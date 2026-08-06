//! Tonic adapter for every pinned `OpenFGA` service method.

use openfga_auth::{Action, AuthenticationService};
use openfga_domain::Principal;
use openfga_proto::openfga::v1::{
    self as pb,
    open_fga_service_server::{OpenFgaService, OpenFgaServiceServer},
};
use tonic::{Request, Response, Status, codegen::InterceptedService, service::Interceptor};

use crate::{ApiError, OpenFgaApi};

/// An authenticated bounded Tonic service adapter.
pub type AuthenticatedGrpcService =
    InterceptedService<OpenFgaServiceServer<OpenFgaApi>, GrpcAuthenticationInterceptor>;

/// Request-metadata authenticator that runs before protobuf message decoding.
#[derive(Clone, Debug)]
pub struct GrpcAuthenticationInterceptor {
    authentication: AuthenticationService,
}

impl Interceptor for GrpcAuthenticationInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let values = request.metadata().get_all("authorization");
        let mut values = values.iter();
        let header = values.next().and_then(|value| value.to_str().ok());
        if values.next().is_some() {
            return Err(ApiError::unauthenticated().into());
        }
        let principal = self
            .authentication
            .authenticate(header)
            .map_err(ApiError::from)
            .map_err(Status::from)?;
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
    let service = OpenFgaServiceServer::new(api)
        .max_decoding_message_size(maximum)
        .max_encoding_message_size(maximum);
    InterceptedService::new(service, GrpcAuthenticationInterceptor { authentication })
}

macro_rules! unary {
    ($self:ident, $request:ident, $method:ident) => {{
        let principal = $request
            .extensions()
            .get::<Principal>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("authentication context is missing"))?;
        $self
            .$method(&principal, $request.into_inner())
            .await
            .map(Response::new)
            .map_err(Status::from)
    }};
}

#[tonic::async_trait]
impl OpenFgaService for OpenFgaApi {
    async fn read(
        &self,
        request: Request<pb::ReadRequest>,
    ) -> Result<Response<pb::ReadResponse>, Status> {
        unary!(self, request, read)
    }

    async fn write(
        &self,
        request: Request<pb::WriteRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        unary!(self, request, write)
    }

    async fn check(
        &self,
        request: Request<pb::CheckRequest>,
    ) -> Result<Response<pb::CheckResponse>, Status> {
        unary!(self, request, check)
    }

    async fn batch_check(
        &self,
        request: Request<pb::BatchCheckRequest>,
    ) -> Result<Response<pb::BatchCheckResponse>, Status> {
        unary!(self, request, batch_check)
    }

    async fn expand(
        &self,
        request: Request<pb::ExpandRequest>,
    ) -> Result<Response<pb::ExpandResponse>, Status> {
        authorize_unimplemented(self, &request, Action::Expand, &request.get_ref().store_id)?;
        Err(crate::ApiError::unimplemented().into())
    }

    async fn read_authorization_model(
        &self,
        request: Request<pb::ReadAuthorizationModelRequest>,
    ) -> Result<Response<pb::ReadAuthorizationModelResponse>, Status> {
        unary!(self, request, read_authorization_model)
    }

    async fn write_authorization_model(
        &self,
        request: Request<pb::WriteAuthorizationModelRequest>,
    ) -> Result<Response<pb::WriteAuthorizationModelResponse>, Status> {
        unary!(self, request, write_authorization_model)
    }

    async fn read_authorization_models(
        &self,
        request: Request<pb::ReadAuthorizationModelsRequest>,
    ) -> Result<Response<pb::ReadAuthorizationModelsResponse>, Status> {
        unary!(self, request, read_authorization_models)
    }

    async fn write_assertions(
        &self,
        request: Request<pb::WriteAssertionsRequest>,
    ) -> Result<Response<pb::WriteAssertionsResponse>, Status> {
        unary!(self, request, write_assertions)
    }

    async fn read_assertions(
        &self,
        request: Request<pb::ReadAssertionsRequest>,
    ) -> Result<Response<pb::ReadAssertionsResponse>, Status> {
        unary!(self, request, read_assertions)
    }

    async fn read_changes(
        &self,
        request: Request<pb::ReadChangesRequest>,
    ) -> Result<Response<pb::ReadChangesResponse>, Status> {
        unary!(self, request, read_changes)
    }

    async fn create_store(
        &self,
        request: Request<pb::CreateStoreRequest>,
    ) -> Result<Response<pb::CreateStoreResponse>, Status> {
        unary!(self, request, create_store)
    }

    async fn update_store(
        &self,
        request: Request<pb::UpdateStoreRequest>,
    ) -> Result<Response<pb::UpdateStoreResponse>, Status> {
        unary!(self, request, update_store)
    }

    async fn delete_store(
        &self,
        request: Request<pb::DeleteStoreRequest>,
    ) -> Result<Response<pb::DeleteStoreResponse>, Status> {
        unary!(self, request, delete_store)
    }

    async fn get_store(
        &self,
        request: Request<pb::GetStoreRequest>,
    ) -> Result<Response<pb::GetStoreResponse>, Status> {
        unary!(self, request, get_store)
    }

    async fn list_stores(
        &self,
        request: Request<pb::ListStoresRequest>,
    ) -> Result<Response<pb::ListStoresResponse>, Status> {
        unary!(self, request, list_stores)
    }

    type StreamedListObjectsStream =
        tonic::codegen::tokio_stream::Empty<Result<pb::StreamedListObjectsResponse, Status>>;

    async fn streamed_list_objects(
        &self,
        request: Request<pb::StreamedListObjectsRequest>,
    ) -> Result<Response<Self::StreamedListObjectsStream>, Status> {
        authorize_unimplemented(
            self,
            &request,
            Action::StreamedListObjects,
            &request.get_ref().store_id,
        )?;
        Err(crate::ApiError::unimplemented().into())
    }

    async fn list_objects(
        &self,
        request: Request<pb::ListObjectsRequest>,
    ) -> Result<Response<pb::ListObjectsResponse>, Status> {
        authorize_unimplemented(
            self,
            &request,
            Action::ListObjects,
            &request.get_ref().store_id,
        )?;
        Err(crate::ApiError::unimplemented().into())
    }

    async fn list_users(
        &self,
        request: Request<pb::ListUsersRequest>,
    ) -> Result<Response<pb::ListUsersResponse>, Status> {
        authorize_unimplemented(
            self,
            &request,
            Action::ListUsers,
            &request.get_ref().store_id,
        )?;
        Err(crate::ApiError::unimplemented().into())
    }
}

fn authorize_unimplemented<T>(
    api: &OpenFgaApi,
    request: &Request<T>,
    action: Action,
    store_id: &str,
) -> Result<(), Status> {
    let principal = request
        .extensions()
        .get::<Principal>()
        .ok_or_else(|| Status::unauthenticated("authentication context is missing"))?;
    api.authorize_store(principal, action, store_id)
        .map_err(Status::from)
}

#[cfg(test)]
mod tests {
    use openfga_auth::{AuthenticationService, PresharedKey};
    use secrecy::SecretString;
    use tonic::{Request, service::Interceptor};

    use super::GrpcAuthenticationInterceptor;

    const KEY: &str = "grpc-test-preshared-key-material-with-32-bytes";

    #[test]
    fn test_should_authenticate_grpc_metadata_before_message_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let authentication = AuthenticationService::preshared(vec![PresharedKey::new(
            "grpc-client".parse()?,
            &SecretString::from(KEY),
        )?])?;
        let mut interceptor = GrpcAuthenticationInterceptor { authentication };

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
