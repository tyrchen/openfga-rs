//! Explicit Axum routes for the pinned `OpenFGA` HTTP/JSON protocol.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{
        DefaultBodyLimit, Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderName, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use openfga_proto::openfga::v1 as pb;
use serde::Deserialize;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{
    catch_panic::CatchPanicLayer,
    propagate_header::PropagateHeaderLayer,
    request_id::{MakeRequestId, RequestId, SetRequestIdLayer},
    sensitive_headers::SetSensitiveRequestHeadersLayer,
    trace::TraceLayer,
};

use crate::{ApiError, OpenFgaApi};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Builds the bounded HTTP/JSON router for all pinned M2 routes.
pub fn http_router(api: OpenFgaApi) -> Router {
    let body_limit = api.config.maximum_message_bytes;
    let concurrency = api.config.maximum_concurrency;
    let timeout = api.config.request_timeout.duration();
    Router::new()
        .route("/stores", get(list_stores).post(create_store))
        .route("/stores/{store_id}", delete(delete_store).get(get_store))
        .route(
            "/stores/{store_id}/assertions/{authorization_model_id}",
            get(read_assertions).put(write_assertions),
        )
        .route(
            "/stores/{store_id}/authorization-models",
            get(read_authorization_models).post(write_authorization_model),
        )
        .route(
            "/stores/{store_id}/authorization-models/{id}",
            get(read_authorization_model),
        )
        .route("/stores/{store_id}/batch-check", post(batch_check))
        .route("/stores/{store_id}/changes", get(read_changes))
        .route("/stores/{store_id}/check", post(check))
        .route("/stores/{store_id}/read", post(read_tuples))
        .route("/stores/{store_id}/write", post(write_tuples))
        .route("/stores/{store_id}/expand", post(unimplemented))
        .route("/stores/{store_id}/list-objects", post(unimplemented))
        .route("/stores/{store_id}/list-users", post(unimplemented))
        .route(
            "/stores/{store_id}/streamed-list-objects",
            post(unimplemented),
        )
        .layer(middleware::from_fn_with_state(body_limit, response_limit))
        .layer(middleware::from_fn_with_state(timeout, request_timeout))
        .layer(ConcurrencyLimitLayer::new(concurrency))
        .layer(CatchPanicLayer::custom(|_| {
            ApiError::internal().into_response()
        }))
        .layer(TraceLayer::new_for_http())
        .layer(PropagateHeaderLayer::new(REQUEST_ID_HEADER))
        .layer(SetRequestIdLayer::new(
            REQUEST_ID_HEADER,
            RequestIdFactory::default(),
        ))
        .layer(SetSensitiveRequestHeadersLayer::new(std::iter::once(
            axum::http::header::AUTHORIZATION,
        )))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(Arc::new(api))
}

async fn response_limit(
    State(maximum_bytes): State<usize>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    match to_bytes(body, maximum_bytes).await {
        Ok(bytes) => Response::from_parts(parts, Body::from(bytes)),
        Err(_) => ApiError::response_too_large().into_response(),
    }
}

async fn request_timeout(
    State(maximum_duration): State<std::time::Duration>,
    request: Request<Body>,
    next: Next,
) -> Response {
    match tokio::time::timeout(maximum_duration, next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::deadline_exceeded().into_response(),
    }
}

#[derive(Clone, Debug, Default)]
struct RequestIdFactory(Arc<AtomicU64>);

impl MakeRequestId for RequestIdFactory {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let value = self.0.fetch_add(1, Ordering::Relaxed);
        HeaderValue::from_str(&format!("openfga-{value:016x}"))
            .ok()
            .map(RequestId::new)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    page_size: Option<i32>,
    continuation_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListStoresQuery {
    page_size: Option<i32>,
    continuation_token: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangesQuery {
    page_size: Option<i32>,
    continuation_token: Option<String>,
    r#type: Option<String>,
    start_time: Option<String>,
}

async fn create_store(
    State(api): State<Arc<OpenFgaApi>>,
    body: Result<Json<pb::CreateStoreRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<pb::CreateStoreResponse>), ApiError> {
    api.create_store(json(body)?)
        .await
        .map(|response| (StatusCode::CREATED, Json(response)))
}

async fn get_store(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
) -> Result<Json<pb::GetStoreResponse>, ApiError> {
    api.get_store(pb::GetStoreRequest { store_id })
        .await
        .map(Json)
}

async fn delete_store(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    api.delete_store(pb::DeleteStoreRequest { store_id })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_stores(
    State(api): State<Arc<OpenFgaApi>>,
    query_value: Result<Query<ListStoresQuery>, QueryRejection>,
) -> Result<Json<pb::ListStoresResponse>, ApiError> {
    let query = query(query_value)?;
    api.list_stores(pb::ListStoresRequest {
        page_size: query
            .page_size
            .map(|value| pbjson_types::Int32Value { value }),
        continuation_token: query.continuation_token.unwrap_or_default(),
        name: query.name.unwrap_or_default(),
    })
    .await
    .map(Json)
}

async fn write_authorization_model(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::WriteAuthorizationModelRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<pb::WriteAuthorizationModelResponse>), ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.write_authorization_model(request)
        .await
        .map(|response| (StatusCode::CREATED, Json(response)))
}

async fn read_authorization_model(
    State(api): State<Arc<OpenFgaApi>>,
    Path((store_id, id)): Path<(String, String)>,
) -> Result<Json<pb::ReadAuthorizationModelResponse>, ApiError> {
    api.read_authorization_model(pb::ReadAuthorizationModelRequest { store_id, id })
        .await
        .map(Json)
}

async fn read_authorization_models(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
    query_value: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<pb::ReadAuthorizationModelsResponse>, ApiError> {
    let query = query(query_value)?;
    api.read_authorization_models(pb::ReadAuthorizationModelsRequest {
        store_id,
        page_size: query
            .page_size
            .map(|value| pbjson_types::Int32Value { value }),
        continuation_token: query.continuation_token.unwrap_or_default(),
    })
    .await
    .map(Json)
}

async fn write_assertions(
    State(api): State<Arc<OpenFgaApi>>,
    Path((store_id, authorization_model_id)): Path<(String, String)>,
    body: Result<Json<pb::WriteAssertionsRequest>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    request.authorization_model_id = authorization_model_id;
    api.write_assertions(request).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn read_assertions(
    State(api): State<Arc<OpenFgaApi>>,
    Path((store_id, authorization_model_id)): Path<(String, String)>,
) -> Result<Json<pb::ReadAssertionsResponse>, ApiError> {
    api.read_assertions(pb::ReadAssertionsRequest {
        store_id,
        authorization_model_id,
    })
    .await
    .map(Json)
}

async fn read_tuples(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::ReadRequest>, JsonRejection>,
) -> Result<Json<pb::ReadResponse>, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.read(request).await.map(Json)
}

async fn write_tuples(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::WriteRequest>, JsonRejection>,
) -> Result<Json<pb::WriteResponse>, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.write(request).await.map(Json)
}

async fn check(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::CheckRequest>, JsonRejection>,
) -> Result<Json<pb::CheckResponse>, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.check(request).await.map(Json)
}

async fn batch_check(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::BatchCheckRequest>, JsonRejection>,
) -> Result<Json<pb::BatchCheckResponse>, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.batch_check(request).await.map(Json)
}

async fn read_changes(
    State(api): State<Arc<OpenFgaApi>>,
    Path(store_id): Path<String>,
    query_value: Result<Query<ChangesQuery>, QueryRejection>,
) -> Result<Json<pb::ReadChangesResponse>, ApiError> {
    let query = query(query_value)?;
    let start_time = query
        .start_time
        .map(|value| {
            serde_json::from_value(serde_json::Value::String(value))
                .map_err(|_| ApiError::invalid_request())
        })
        .transpose()?;
    api.read_changes(pb::ReadChangesRequest {
        store_id,
        r#type: query.r#type.unwrap_or_default(),
        page_size: query
            .page_size
            .map(|value| pbjson_types::Int32Value { value }),
        continuation_token: query.continuation_token.unwrap_or_default(),
        start_time,
    })
    .await
    .map(Json)
}

async fn unimplemented() -> ApiError {
    ApiError::unimplemented()
}

fn json<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    body.map(|Json(value)| value).map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::payload_too_large()
        } else {
            ApiError::invalid_request()
        }
    })
}

fn query<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    query
        .map(|Query(value)| value)
        .map_err(|_| ApiError::invalid_request())
}
