//! Tonic adapter for every pinned `OpenFGA` service method.

use std::{
    fmt,
    net::{IpAddr, Ipv4Addr},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use openfga_auth::{Action, AuthenticationService};
use openfga_domain::{Deadline, Principal, RequestTimeout};
use openfga_list::ListObjectsStream;
use openfga_proto::openfga::v1::{
    self as pb,
    open_fga_service_server::{OpenFgaService, OpenFgaServiceServer},
};
use openfga_service::ServiceError;
use prost_reflect::ReflectMessage;
use tokio::sync::OwnedSemaphorePermit;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, codegen::InterceptedService, service::Interceptor};

use crate::{
    ApiError, EndpointClass, OpenFgaApi, admission::AdmissionControl, api::with_request_deadline,
};

/// gRPC object stream retaining its endpoint concurrency permit until termination.
#[non_exhaustive]
pub struct GrpcListObjectsStream {
    inner: ListObjectsStream,
    _permit: OwnedSemaphorePermit,
}

impl Stream for GrpcListObjectsStream {
    type Item = Result<pb::StreamedListObjectsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(context).map(|item| {
            item.map(|result| {
                result
                    .map(|object| pb::StreamedListObjectsResponse {
                        object: object.to_string(),
                    })
                    .map_err(|error| Status::from(ApiError::from(ServiceError::from(error))))
            })
        })
    }
}

impl fmt::Debug for GrpcListObjectsStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrpcListObjectsStream")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// An authenticated bounded Tonic service adapter.
pub type AuthenticatedGrpcService =
    InterceptedService<OpenFgaServiceServer<OpenFgaApi>, GrpcAuthenticationInterceptor>;

/// Request-metadata authenticator that runs before protobuf message decoding.
#[derive(Clone, Debug)]
pub struct GrpcAuthenticationInterceptor {
    authentication: AuthenticationService,
    admission: AdmissionControl,
}

impl Interceptor for GrpcAuthenticationInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let peer_ip = request
            .remote_addr()
            .map_or(IpAddr::V4(Ipv4Addr::LOCALHOST), |peer| peer.ip());
        self.admission
            .admit_authentication(peer_ip)
            .map_err(Status::from)?;
        let values = request.metadata().get_all("authorization");
        let mut values = values.iter();
        let header = values.next().and_then(|value| value.to_str().ok());
        if values.next().is_some() {
            self.admission
                .record_authentication_failure(peer_ip)
                .map_err(Status::from)?;
            return Err(ApiError::unauthenticated().into());
        }
        let principal = match self.authentication.authenticate(header) {
            Ok(principal) => principal,
            Err(error) => {
                self.admission
                    .record_authentication_failure(peer_ip)
                    .map_err(Status::from)?;
                return Err(Status::from(ApiError::from(error)));
            }
        };
        request.extensions_mut().insert(principal);
        Ok(request)
    }
}

/// Creates the authenticated bounded Tonic service adapter.
#[must_use]
pub fn grpc_service(
    api: OpenFgaApi,
    authentication: AuthenticationService,
) -> AuthenticatedGrpcService {
    let maximum = api.config.maximum_message_bytes;
    let admission = api.admission.clone();
    let service = OpenFgaServiceServer::new(api)
        .max_decoding_message_size(maximum)
        .max_encoding_message_size(maximum);
    InterceptedService::new(
        service,
        GrpcAuthenticationInterceptor {
            authentication,
            admission,
        },
    )
}

macro_rules! unary {
    ($self:ident, $request:ident, $method:ident, $class:expr, $action:expr, $store_id:expr) => {{
        let principal = $request
            .extensions()
            .get::<Principal>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("authentication context is missing"))?;
        $self
            .preauthorize(&principal, $action, $store_id)
            .map_err(Status::from)?;
        let request_deadline = grpc_deadline($self, &$request)?;
        $self
            .admission
            .admit_principal(&principal, $class)
            .map_err(Status::from)?;
        let endpoint_permit = $self.acquire_endpoint_permit().map_err(Status::from)?;
        let request_deadline = match request_deadline {
            GrpcDeadline::Elapsed => {
                return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
            }
            GrpcDeadline::At(deadline) => deadline,
        };
        if request_deadline.is_elapsed(Instant::now()) {
            return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
        }
        ApiError::validate_grpc($request.get_ref()).map_err(Status::from)?;
        let response = with_request_deadline(
            request_deadline,
            $self.$method(&principal, $request.into_inner()),
        )
        .await
        .map(Response::new)
        .map_err(Status::from);
        drop(endpoint_permit);
        response
    }};
}

#[derive(Clone, Copy, Debug)]
enum GrpcDeadline {
    Elapsed,
    At(Deadline),
}

fn grpc_deadline<T>(api: &OpenFgaApi, request: &Request<T>) -> Result<GrpcDeadline, Status> {
    let timeout = request
        .metadata()
        .get("grpc-timeout")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_grpc_timeout)
        .map_or(api.config.request_timeout.duration(), |client| {
            client.min(api.config.request_timeout.duration())
        });
    if timeout.is_zero() {
        return Ok(GrpcDeadline::Elapsed);
    }
    let timeout = RequestTimeout::new(timeout)
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout metadata"))?;
    Deadline::from_timeout(Instant::now(), timeout)
        .map(GrpcDeadline::At)
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout metadata"))
}

fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let split = value.len().checked_sub(1)?;
    let (number, unit) = value.split_at(split);
    if number.is_empty() || number.len() > 8 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let number = number.parse::<u64>().ok()?;
    match unit {
        "H" => number.checked_mul(3_600).map(Duration::from_secs),
        "M" => number.checked_mul(60).map(Duration::from_secs),
        "S" => Some(Duration::from_secs(number)),
        "m" => Some(Duration::from_millis(number)),
        "u" => Some(Duration::from_micros(number)),
        "n" => Some(Duration::from_nanos(number)),
        _ => None,
    }
}

#[tonic::async_trait]
impl OpenFgaService for OpenFgaApi {
    async fn read(
        &self,
        request: Request<pb::ReadRequest>,
    ) -> Result<Response<pb::ReadResponse>, Status> {
        unary!(
            self,
            request,
            read,
            EndpointClass::Read,
            Action::Read,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn write(
        &self,
        request: Request<pb::WriteRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        unary!(
            self,
            request,
            write,
            EndpointClass::Write,
            Action::Write,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn check(
        &self,
        request: Request<pb::CheckRequest>,
    ) -> Result<Response<pb::CheckResponse>, Status> {
        unary!(
            self,
            request,
            check,
            EndpointClass::Check,
            Action::Check,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn batch_check(
        &self,
        request: Request<pb::BatchCheckRequest>,
    ) -> Result<Response<pb::BatchCheckResponse>, Status> {
        unary!(
            self,
            request,
            batch_check,
            EndpointClass::Check,
            Action::BatchCheck,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn expand(
        &self,
        request: Request<pb::ExpandRequest>,
    ) -> Result<Response<pb::ExpandResponse>, Status> {
        let _permit = validate_unimplemented(
            self,
            &request,
            Action::Expand,
            &request.get_ref().store_id,
            EndpointClass::Enumeration,
        )?;
        Err(Status::unimplemented("method Expand not implemented"))
    }

    async fn read_authorization_model(
        &self,
        request: Request<pb::ReadAuthorizationModelRequest>,
    ) -> Result<Response<pb::ReadAuthorizationModelResponse>, Status> {
        unary!(
            self,
            request,
            read_authorization_model,
            EndpointClass::Administration,
            Action::ReadAuthorizationModels,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn write_authorization_model(
        &self,
        request: Request<pb::WriteAuthorizationModelRequest>,
    ) -> Result<Response<pb::WriteAuthorizationModelResponse>, Status> {
        unary!(
            self,
            request,
            write_authorization_model,
            EndpointClass::Administration,
            Action::WriteAuthorizationModel,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn read_authorization_models(
        &self,
        request: Request<pb::ReadAuthorizationModelsRequest>,
    ) -> Result<Response<pb::ReadAuthorizationModelsResponse>, Status> {
        unary!(
            self,
            request,
            read_authorization_models,
            EndpointClass::Administration,
            Action::ReadAuthorizationModels,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn write_assertions(
        &self,
        request: Request<pb::WriteAssertionsRequest>,
    ) -> Result<Response<pb::WriteAssertionsResponse>, Status> {
        unary!(
            self,
            request,
            write_assertions,
            EndpointClass::Administration,
            Action::WriteAssertions,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn read_assertions(
        &self,
        request: Request<pb::ReadAssertionsRequest>,
    ) -> Result<Response<pb::ReadAssertionsResponse>, Status> {
        unary!(
            self,
            request,
            read_assertions,
            EndpointClass::Administration,
            Action::ReadAssertions,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn read_changes(
        &self,
        request: Request<pb::ReadChangesRequest>,
    ) -> Result<Response<pb::ReadChangesResponse>, Status> {
        unary!(
            self,
            request,
            read_changes,
            EndpointClass::Read,
            Action::ReadChanges,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn create_store(
        &self,
        request: Request<pb::CreateStoreRequest>,
    ) -> Result<Response<pb::CreateStoreResponse>, Status> {
        unary!(
            self,
            request,
            create_store,
            EndpointClass::Administration,
            Action::CreateStore,
            None
        )
    }

    async fn update_store(
        &self,
        request: Request<pb::UpdateStoreRequest>,
    ) -> Result<Response<pb::UpdateStoreResponse>, Status> {
        let _permit = validate_unimplemented(
            self,
            &request,
            Action::UpdateStore,
            &request.get_ref().store_id,
            EndpointClass::Administration,
        )?;
        Err(Status::unimplemented("method UpdateStore not implemented"))
    }

    async fn delete_store(
        &self,
        request: Request<pb::DeleteStoreRequest>,
    ) -> Result<Response<pb::DeleteStoreResponse>, Status> {
        unary!(
            self,
            request,
            delete_store,
            EndpointClass::Administration,
            Action::DeleteStore,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn get_store(
        &self,
        request: Request<pb::GetStoreRequest>,
    ) -> Result<Response<pb::GetStoreResponse>, Status> {
        unary!(
            self,
            request,
            get_store,
            EndpointClass::Administration,
            Action::GetStore,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn list_stores(
        &self,
        request: Request<pb::ListStoresRequest>,
    ) -> Result<Response<pb::ListStoresResponse>, Status> {
        unary!(
            self,
            request,
            list_stores,
            EndpointClass::Administration,
            Action::ListStores,
            None
        )
    }

    type StreamedListObjectsStream = GrpcListObjectsStream;

    async fn streamed_list_objects(
        &self,
        request: Request<pb::StreamedListObjectsRequest>,
    ) -> Result<Response<Self::StreamedListObjectsStream>, Status> {
        let (principal, deadline, permit) = validate_streaming(
            self,
            &request,
            Action::StreamedListObjects,
            &request.get_ref().store_id,
            EndpointClass::Enumeration,
        )?;
        let inner = with_request_deadline(
            deadline,
            self.streamed_list_objects(&principal, request.into_inner()),
        )
        .await
        .map_err(Status::from)?;
        Ok(Response::new(GrpcListObjectsStream {
            inner,
            _permit: permit,
        }))
    }

    async fn list_objects(
        &self,
        request: Request<pb::ListObjectsRequest>,
    ) -> Result<Response<pb::ListObjectsResponse>, Status> {
        unary!(
            self,
            request,
            list_objects,
            EndpointClass::Enumeration,
            Action::ListObjects,
            Some(request.get_ref().store_id.as_str())
        )
    }

    async fn list_users(
        &self,
        request: Request<pb::ListUsersRequest>,
    ) -> Result<Response<pb::ListUsersResponse>, Status> {
        unary!(
            self,
            request,
            list_users,
            EndpointClass::Enumeration,
            Action::ListUsers,
            Some(request.get_ref().store_id.as_str())
        )
    }
}

fn validate_unimplemented<T: ReflectMessage>(
    api: &OpenFgaApi,
    request: &Request<T>,
    action: Action,
    store_id: &str,
    class: EndpointClass,
) -> Result<OwnedSemaphorePermit, Status> {
    let principal = request
        .extensions()
        .get::<Principal>()
        .ok_or_else(|| Status::unauthenticated("authentication context is missing"))?;
    api.preauthorize(principal, action, Some(store_id))
        .map_err(Status::from)?;
    let deadline = grpc_deadline(api, request)?;
    api.admission
        .admit_principal(principal, class)
        .map_err(Status::from)?;
    let permit = api.acquire_endpoint_permit().map_err(Status::from)?;
    let deadline = match deadline {
        GrpcDeadline::Elapsed => {
            return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
        }
        GrpcDeadline::At(deadline) => deadline,
    };
    if deadline.is_elapsed(Instant::now()) {
        return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
    }
    ApiError::validate_grpc(request.get_ref()).map_err(Status::from)?;
    Ok(permit)
}

fn validate_streaming<T: ReflectMessage>(
    api: &OpenFgaApi,
    request: &Request<T>,
    action: Action,
    store_id: &str,
    class: EndpointClass,
) -> Result<(Principal, Deadline, OwnedSemaphorePermit), Status> {
    let principal = request
        .extensions()
        .get::<Principal>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authentication context is missing"))?;
    api.preauthorize(&principal, action, Some(store_id))
        .map_err(Status::from)?;
    let deadline = grpc_deadline(api, request)?;
    api.admission
        .admit_principal(&principal, class)
        .map_err(Status::from)?;
    let permit = api.acquire_endpoint_permit().map_err(Status::from)?;
    let deadline = match deadline {
        GrpcDeadline::Elapsed => {
            return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
        }
        GrpcDeadline::At(deadline) => deadline,
    };
    if deadline.is_elapsed(Instant::now()) {
        return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
    }
    ApiError::validate_grpc(request.get_ref()).map_err(Status::from)?;
    Ok((principal, deadline, permit))
}

#[cfg(test)]
mod tests {
    use openfga_auth::{AuthenticationService, PresharedKey};
    use secrecy::SecretString;
    use tonic::{Request, service::Interceptor};

    use super::GrpcAuthenticationInterceptor;
    use crate::{AdmissionPolicy, admission::AdmissionControl};

    const KEY: &str = "grpc-test-preshared-key-material-with-32-bytes";

    #[test]
    fn test_should_authenticate_grpc_metadata_before_message_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let authentication = AuthenticationService::preshared(vec![PresharedKey::new(
            "grpc-client".parse()?,
            &SecretString::from(KEY),
        )?])?;
        let mut interceptor = GrpcAuthenticationInterceptor {
            authentication,
            admission: AdmissionControl::new(AdmissionPolicy::builder().build())?,
        };

        let missing = interceptor.call(Request::new(()));
        assert!(matches!(
            missing,
            Err(status) if status.code() == tonic::Code::Unauthenticated
        ));

        let mut valid = Request::new(());
        valid
            .metadata_mut()
            .insert("authorization", format!("Bearer {KEY}").parse()?);
        let valid = interceptor.call(valid)?;
        assert_eq!(
            valid
                .extensions()
                .get::<openfga_domain::Principal>()
                .ok_or("authenticated principal missing")?
                .id()
                .as_str(),
            "grpc-client"
        );

        let mut duplicate = Request::new(());
        duplicate
            .metadata_mut()
            .append("authorization", format!("Bearer {KEY}").parse()?);
        duplicate
            .metadata_mut()
            .append("authorization", format!("Bearer {KEY}").parse()?);
        assert!(matches!(
            interceptor.call(duplicate),
            Err(status) if status.code() == tonic::Code::Unauthenticated
        ));
        Ok(())
    }
}
