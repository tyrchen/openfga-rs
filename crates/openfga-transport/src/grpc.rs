//! Tonic adapter for every pinned `OpenFGA` service method.

use openfga_proto::openfga::v1::{
    self as pb,
    open_fga_service_server::{OpenFgaService, OpenFgaServiceServer},
};
use tonic::{Request, Response, Status};

use crate::OpenFgaApi;

/// Creates the bounded Tonic service adapter.
#[must_use]
pub fn grpc_service(api: OpenFgaApi) -> OpenFgaServiceServer<OpenFgaApi> {
    let maximum = api.config.maximum_message_bytes;
    OpenFgaServiceServer::new(api)
        .max_decoding_message_size(maximum)
        .max_encoding_message_size(maximum)
}

macro_rules! unary {
    ($self:ident, $request:ident, $method:ident) => {{
        $self
            .$method($request.into_inner())
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
        _request: Request<pb::ExpandRequest>,
    ) -> Result<Response<pb::ExpandResponse>, Status> {
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
        _request: Request<pb::StreamedListObjectsRequest>,
    ) -> Result<Response<Self::StreamedListObjectsStream>, Status> {
        Err(crate::ApiError::unimplemented().into())
    }

    async fn list_objects(
        &self,
        _request: Request<pb::ListObjectsRequest>,
    ) -> Result<Response<pb::ListObjectsResponse>, Status> {
        Err(crate::ApiError::unimplemented().into())
    }

    async fn list_users(
        &self,
        _request: Request<pb::ListUsersRequest>,
    ) -> Result<Response<pb::ListUsersResponse>, Status> {
        Err(crate::ApiError::unimplemented().into())
    }
}
