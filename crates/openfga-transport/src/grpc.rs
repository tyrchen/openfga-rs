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
use openfga_proto::{
    authzen::v1::{
        self as az,
        auth_zen_service_server::{AuthZenService, AuthZenServiceServer},
    },
    openfga::v1::{
        self as pb,
        open_fga_service_server::{OpenFgaService, OpenFgaServiceServer},
    },
};
use openfga_service::ServiceError;
use prost_reflect::ReflectMessage;
use tokio_stream::Stream;
use tonic::{Code, Request, Response, Status, codegen::InterceptedService, service::Interceptor};

use crate::{
    ApiError, EndpointClass, OpenFgaApi,
    admission::AdmissionControl,
    api::{EndpointPermit, with_request_deadline},
    authzen::{AUTHORIZATION_MODEL_ID_HEADER, authorization_model_id},
};

/// gRPC object stream retaining its endpoint concurrency permit until termination.
#[non_exhaustive]
pub struct GrpcListObjectsStream {
    inner: ListObjectsStream,
    permit: EndpointPermit,
}

impl Stream for GrpcListObjectsStream {
    type Item = Result<pb::StreamedListObjectsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(context).map(|item| {
            item.map(|result| {
                result
                    .map(|object| pb::StreamedListObjectsResponse {
                        object: object.to_string(),
                    })
                    .map_err(|error| Status::from(ApiError::from(ServiceError::from(error))))
            })
        });
        match &result {
            Poll::Ready(None) => self.permit.complete("success"),
            Poll::Ready(Some(Err(status))) => {
                self.permit.complete(grpc_completion(status.code()));
            }
            Poll::Pending | Poll::Ready(Some(Ok(_))) => {}
        }
        result
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

/// An authenticated bounded `AuthZEN` Tonic service adapter.
pub type AuthenticatedAuthZenGrpcService =
    InterceptedService<AuthZenServiceServer<OpenFgaApi>, GrpcAuthenticationInterceptor>;

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

/// Creates the authenticated bounded `AuthZEN` Tonic service adapter.
#[must_use]
pub fn authzen_grpc_service(
    api: OpenFgaApi,
    authentication: AuthenticationService,
) -> AuthenticatedAuthZenGrpcService {
    let maximum = api.config.maximum_message_bytes;
    let admission = api.admission.clone();
    let service = AuthZenServiceServer::new(api)
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
        let mut endpoint_permit = $self
            .acquire_endpoint_permit($class)
            .map_err(Status::from)?;
        let request_deadline = match request_deadline {
            GrpcDeadline::Elapsed => {
                endpoint_permit.complete("timeout");
                return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
            }
            GrpcDeadline::At(deadline) => deadline,
        };
        if request_deadline.is_elapsed(Instant::now()) {
            endpoint_permit.complete("timeout");
            return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
        }
        if let Err(error) = ApiError::validate_grpc($request.get_ref()) {
            endpoint_permit.complete("client_error");
            return Err(Status::from(error));
        }
        let response = with_request_deadline(
            request_deadline,
            $self.$method(&principal, $request.into_inner()),
        )
        .await
        .map(Response::new)
        .map_err(Status::from);
        endpoint_permit.complete(match &response {
            Ok(_) => "success",
            Err(status) => grpc_completion(status.code()),
        });
        drop(endpoint_permit);
        response
    }};
}

macro_rules! authzen_unary {
    ($self:ident, $request:ident, $method:ident, $class:expr, $action:expr) => {{
        $self.ensure_authzen_enabled().map_err(Status::from)?;
        let principal = $request
            .extensions()
            .get::<Principal>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("authentication context is missing"))?;
        let store_id = $request.get_ref().store_id.as_str();
        $self
            .preauthorize(&principal, $action, Some(store_id))
            .map_err(Status::from)?;
        let request_deadline = grpc_deadline($self, &$request)?;
        $self
            .admission
            .admit_principal(&principal, $class)
            .map_err(Status::from)?;
        let mut endpoint_permit = $self
            .acquire_endpoint_permit($class)
            .map_err(Status::from)?;
        let request_deadline = match request_deadline {
            GrpcDeadline::Elapsed => {
                endpoint_permit.complete("timeout");
                return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
            }
            GrpcDeadline::At(deadline) => deadline,
        };
        if let Err(error) = ApiError::validate_grpc($request.get_ref()) {
            endpoint_permit.complete("client_error");
            return Err(Status::from(error));
        }
        let model_id = authorization_model_id(
            $request
                .metadata()
                .get(AUTHORIZATION_MODEL_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
        );
        let response = with_request_deadline(
            request_deadline,
            $self.$method(&principal, $request.into_inner(), &model_id),
        )
        .await
        .map(Response::new)
        .map_err(Status::from);
        endpoint_permit.complete(match &response {
            Ok(_) => "success",
            Err(status) => grpc_completion(status.code()),
        });
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
impl AuthZenService for OpenFgaApi {
    async fn evaluation(
        &self,
        request: Request<az::EvaluationRequest>,
    ) -> Result<Response<az::EvaluationResponse>, Status> {
        authzen_unary!(
            self,
            request,
            authzen_evaluation,
            EndpointClass::Check,
            Action::Check
        )
    }

    async fn evaluations(
        &self,
        request: Request<az::EvaluationsRequest>,
    ) -> Result<Response<az::EvaluationsResponse>, Status> {
        authzen_unary!(
            self,
            request,
            authzen_evaluations,
            EndpointClass::Check,
            Action::BatchCheck
        )
    }

    async fn subject_search(
        &self,
        request: Request<az::SubjectSearchRequest>,
    ) -> Result<Response<az::SubjectSearchResponse>, Status> {
        authzen_unary!(
            self,
            request,
            authzen_subject_search,
            EndpointClass::Enumeration,
            Action::ListUsers
        )
    }

    async fn resource_search(
        &self,
        request: Request<az::ResourceSearchRequest>,
    ) -> Result<Response<az::ResourceSearchResponse>, Status> {
        authzen_unary!(
            self,
            request,
            authzen_resource_search,
            EndpointClass::Enumeration,
            Action::StreamedListObjects
        )
    }

    async fn action_search(
        &self,
        request: Request<az::ActionSearchRequest>,
    ) -> Result<Response<az::ActionSearchResponse>, Status> {
        authzen_unary!(
            self,
            request,
            authzen_action_search,
            EndpointClass::Enumeration,
            Action::BatchCheck
        )
    }

    async fn get_configuration(
        &self,
        request: Request<az::GetConfigurationRequest>,
    ) -> Result<Response<az::GetConfigurationResponse>, Status> {
        authzen_unary!(
            self,
            request,
            authzen_configuration,
            EndpointClass::Administration,
            Action::GetStore
        )
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
        unary!(
            self,
            request,
            expand,
            EndpointClass::Enumeration,
            Action::Expand,
            Some(request.get_ref().store_id.as_str())
        )
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
        let mut permit = validate_unimplemented(
            self,
            &request,
            Action::UpdateStore,
            &request.get_ref().store_id,
            EndpointClass::Administration,
        )?;
        permit.complete("unimplemented");
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
        let (principal, deadline, mut permit) = validate_streaming(
            self,
            &request,
            Action::StreamedListObjects,
            &request.get_ref().store_id,
            EndpointClass::Enumeration,
        )?;
        let inner = match with_request_deadline(
            deadline,
            self.streamed_list_objects(&principal, request.into_inner()),
        )
        .await
        {
            Ok(inner) => inner,
            Err(error) => {
                let status = Status::from(error);
                permit.complete(grpc_completion(status.code()));
                return Err(status);
            }
        };
        Ok(Response::new(GrpcListObjectsStream { inner, permit }))
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
) -> Result<EndpointPermit, Status> {
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
    let mut permit = api.acquire_endpoint_permit(class).map_err(Status::from)?;
    let deadline = match deadline {
        GrpcDeadline::Elapsed => {
            permit.complete("timeout");
            return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
        }
        GrpcDeadline::At(deadline) => deadline,
    };
    if deadline.is_elapsed(Instant::now()) {
        permit.complete("timeout");
        return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
    }
    if let Err(error) = ApiError::validate_grpc(request.get_ref()) {
        permit.complete("client_error");
        return Err(Status::from(error));
    }
    Ok(permit)
}

fn validate_streaming<T: ReflectMessage>(
    api: &OpenFgaApi,
    request: &Request<T>,
    action: Action,
    store_id: &str,
    class: EndpointClass,
) -> Result<(Principal, Deadline, EndpointPermit), Status> {
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
    let mut permit = api.acquire_endpoint_permit(class).map_err(Status::from)?;
    let deadline = match deadline {
        GrpcDeadline::Elapsed => {
            permit.complete("timeout");
            return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
        }
        GrpcDeadline::At(deadline) => deadline,
    };
    if deadline.is_elapsed(Instant::now()) {
        permit.complete("timeout");
        return Err(Status::deadline_exceeded("Request Deadline Exceeded"));
    }
    if let Err(error) = ApiError::validate_grpc(request.get_ref()) {
        permit.complete("client_error");
        return Err(Status::from(error));
    }
    Ok((principal, deadline, permit))
}

const fn grpc_completion(code: Code) -> &'static str {
    match code {
        Code::Ok => "success",
        Code::DeadlineExceeded => "timeout",
        Code::Cancelled => "cancelled",
        Code::ResourceExhausted => "overloaded",
        Code::InvalidArgument
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::FailedPrecondition
        | Code::OutOfRange => "client_error",
        Code::Unimplemented => "unimplemented",
        Code::Unknown | Code::Aborted | Code::Internal | Code::Unavailable | Code::DataLoss => {
            "server_error"
        }
    }
}

#[cfg(test)]
mod tests {
    use openfga_auth::{AuthenticationService, PresharedKey};
    use secrecy::SecretString;
    use tonic::{Code, Request, service::Interceptor};

    use super::{GrpcAuthenticationInterceptor, grpc_completion};
    use crate::{AdmissionPolicy, admission::AdmissionControl};

    const KEY: &str = "grpc-test-preshared-key-material-with-32-bytes";

    #[test]
    fn test_should_classify_grpc_completions_with_bounded_labels() {
        assert_eq!(grpc_completion(Code::Ok), "success");
        assert_eq!(grpc_completion(Code::InvalidArgument), "client_error");
        assert_eq!(grpc_completion(Code::DeadlineExceeded), "timeout");
        assert_eq!(grpc_completion(Code::Cancelled), "cancelled");
        assert_eq!(grpc_completion(Code::ResourceExhausted), "overloaded");
        assert_eq!(grpc_completion(Code::Internal), "server_error");
        assert_eq!(grpc_completion(Code::Unimplemented), "unimplemented");
    }

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
