//! Explicit Axum routes for the pinned `OpenFGA` HTTP/JSON protocol.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{
        DefaultBodyLimit, Extension, Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderName, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use openfga_auth::{Action, AuthenticationService};
use openfga_domain::Principal;
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

use crate::{ApiError, EndpointClass, OpenFgaApi, admission::AdmissionControl};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Builds the bounded HTTP/JSON router for all pinned M2 routes.
pub fn http_router(api: OpenFgaApi, authentication: AuthenticationService) -> Router {
    let body_limit = api.config.maximum_message_bytes;
    let concurrency = api.config.maximum_concurrency;
    let timeout = api.config.request_timeout.duration();
    let admission = api.admission.clone();
    let authentication = AuthenticationState {
        authentication,
        admission: admission.clone(),
    };
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
        .route("/stores/{store_id}/expand", post(expand))
        .route("/stores/{store_id}/list-objects", post(list_objects))
        .route("/stores/{store_id}/list-users", post(list_users))
        .route(
            "/stores/{store_id}/streamed-list-objects",
            post(streamed_list_objects),
        )
        .layer(middleware::from_fn_with_state(body_limit, response_limit))
        .layer(middleware::from_fn_with_state(timeout, request_timeout))
        .layer(middleware::from_fn_with_state(admission, admit_principal))
        .layer(middleware::from_fn_with_state(authentication, authenticate))
        .layer(ConcurrencyLimitLayer::new(concurrency))
        .layer(CatchPanicLayer::custom(|_| {
            ApiError::internal().into_response()
        }))
        .layer(TraceLayer::new_for_http().make_span_with(request_span))
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

fn request_span<B>(request: &Request<B>) -> tracing::Span {
    tracing::info_span!(
        "http_request",
        method = %request.method(),
        path = request.uri().path(),
    )
}

#[derive(Clone, Debug)]
struct AuthenticationState {
    authentication: AuthenticationService,
    admission: AdmissionControl,
}

async fn authenticate(
    State(state): State<AuthenticationState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    if let Err(error) = state.admission.admit_authentication() {
        return error.into_response();
    }
    let authorization_values = request.headers().get_all(axum::http::header::AUTHORIZATION);
    let mut authorization_values = authorization_values.iter();
    let header = authorization_values
        .next()
        .and_then(|value| value.to_str().ok());
    if authorization_values.next().is_some() {
        return match state.admission.record_authentication_failure() {
            Ok(()) => ApiError::unauthenticated().into_response(),
            Err(overloaded) => overloaded.into_response(),
        };
    }
    match state.authentication.authenticate(header) {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(error) => match state.admission.record_authentication_failure() {
            Ok(()) => ApiError::from(error).into_response(),
            Err(overloaded) => overloaded.into_response(),
        },
    }
}

async fn admit_principal(
    State(admission): State<AdmissionControl>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(principal) = request.extensions().get::<Principal>() else {
        return ApiError::unauthenticated().into_response();
    };
    let class = endpoint_class(request.method(), request.uri().path());
    match admission.admit_principal(principal, class) {
        Ok(()) => next.run(request).await,
        Err(error) => error.into_response(),
    }
}

fn endpoint_class(_method: &axum::http::Method, path: &str) -> EndpointClass {
    if path.ends_with("/check") || path.ends_with("/batch-check") {
        EndpointClass::Check
    } else if path.ends_with("/write") {
        EndpointClass::Write
    } else if path.ends_with("/read") || path.ends_with("/changes") {
        EndpointClass::Read
    } else if path.ends_with("/expand")
        || path.ends_with("/list-objects")
        || path.ends_with("/streamed-list-objects")
        || path.ends_with("/list-users")
    {
        EndpointClass::Enumeration
    } else {
        EndpointClass::Administration
    }
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
    Extension(principal): Extension<Principal>,
    body: Result<Json<pb::CreateStoreRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<pb::CreateStoreResponse>), ApiError> {
    api.create_store(&principal, json(body)?)
        .await
        .map(|response| (StatusCode::CREATED, Json(response)))
}

async fn get_store(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
) -> Result<Json<pb::GetStoreResponse>, ApiError> {
    api.get_store(&principal, pb::GetStoreRequest { store_id })
        .await
        .map(Json)
}

async fn delete_store(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    api.delete_store(&principal, pb::DeleteStoreRequest { store_id })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_stores(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    query_value: Result<Query<ListStoresQuery>, QueryRejection>,
) -> Result<Json<pb::ListStoresResponse>, ApiError> {
    let query = query(query_value)?;
    api.list_stores(
        &principal,
        pb::ListStoresRequest {
            page_size: query
                .page_size
                .map(|value| pbjson_types::Int32Value { value }),
            continuation_token: query.continuation_token.unwrap_or_default(),
            name: query.name.unwrap_or_default(),
        },
    )
    .await
    .map(Json)
}

async fn write_authorization_model(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::WriteAuthorizationModelRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<pb::WriteAuthorizationModelResponse>), ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.write_authorization_model(&principal, request)
        .await
        .map(|response| (StatusCode::CREATED, Json(response)))
}

async fn read_authorization_model(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path((store_id, id)): Path<(String, String)>,
) -> Result<Json<pb::ReadAuthorizationModelResponse>, ApiError> {
    api.read_authorization_model(
        &principal,
        pb::ReadAuthorizationModelRequest { store_id, id },
    )
    .await
    .map(Json)
}

async fn read_authorization_models(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    query_value: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<pb::ReadAuthorizationModelsResponse>, ApiError> {
    let query = query(query_value)?;
    api.read_authorization_models(
        &principal,
        pb::ReadAuthorizationModelsRequest {
            store_id,
            page_size: query
                .page_size
                .map(|value| pbjson_types::Int32Value { value }),
            continuation_token: query.continuation_token.unwrap_or_default(),
        },
    )
    .await
    .map(Json)
}

async fn write_assertions(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path((store_id, authorization_model_id)): Path<(String, String)>,
    body: Result<Json<pb::WriteAssertionsRequest>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    request.authorization_model_id = authorization_model_id;
    api.write_assertions(&principal, request).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn read_assertions(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path((store_id, authorization_model_id)): Path<(String, String)>,
) -> Result<Json<pb::ReadAssertionsResponse>, ApiError> {
    api.read_assertions(
        &principal,
        pb::ReadAssertionsRequest {
            store_id,
            authorization_model_id,
        },
    )
    .await
    .map(Json)
}

async fn read_tuples(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::ReadRequest>, JsonRejection>,
) -> Result<Json<pb::ReadResponse>, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.read(&principal, request).await.map(Json)
}

async fn write_tuples(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::WriteRequest>, JsonRejection>,
) -> Result<Json<pb::WriteResponse>, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.write(&principal, request).await.map(Json)
}

async fn check(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::CheckRequest>, JsonRejection>,
) -> Result<Json<pb::CheckResponse>, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.check(&principal, request).await.map(Json)
}

async fn batch_check(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::BatchCheckRequest>, JsonRejection>,
) -> Result<Json<pb::BatchCheckResponse>, ApiError> {
    let mut request = json(body)?;
    request.store_id = store_id;
    api.batch_check(&principal, request).await.map(Json)
}

async fn read_changes(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
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
    api.read_changes(
        &principal,
        pb::ReadChangesRequest {
            store_id,
            r#type: query.r#type.unwrap_or_default(),
            page_size: query
                .page_size
                .map(|value| pbjson_types::Int32Value { value }),
            continuation_token: query.continuation_token.unwrap_or_default(),
            start_time,
        },
    )
    .await
    .map(Json)
}

async fn expand(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
) -> Result<(), ApiError> {
    authorize_unimplemented(&api, &principal, Action::Expand, &store_id)
}

async fn list_objects(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
) -> Result<(), ApiError> {
    authorize_unimplemented(&api, &principal, Action::ListObjects, &store_id)
}

async fn streamed_list_objects(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
) -> Result<(), ApiError> {
    authorize_unimplemented(&api, &principal, Action::StreamedListObjects, &store_id)
}

async fn list_users(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
) -> Result<(), ApiError> {
    authorize_unimplemented(&api, &principal, Action::ListUsers, &store_id)
}

fn authorize_unimplemented(
    api: &OpenFgaApi,
    principal: &Principal,
    action: Action,
    store_id: &str,
) -> Result<(), ApiError> {
    api.authorize_store(principal, action, store_id)?;
    Err(ApiError::unimplemented())
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

#[cfg(test)]
mod trace_tests {
    use std::{
        io::{self, Write},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use axum::http::Request;
    use tracing_subscriber::fmt::{self, format::FmtSpan};

    use super::request_span;

    #[derive(Clone, Debug)]
    struct CanaryWriter {
        leaked: Arc<AtomicBool>,
    }

    impl Write for CanaryWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            if buffer
                .windows(b"secret-continuation-token".len())
                .any(|window| window == b"secret-continuation-token")
            {
                self.leaked.store(true, Ordering::SeqCst);
            }
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> fmt::MakeWriter<'writer> for CanaryWriter {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn test_should_never_record_query_tokens_in_request_spans()
    -> Result<(), Box<dyn std::error::Error>> {
        let leaked = Arc::new(AtomicBool::new(false));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(CanaryWriter {
                leaked: Arc::clone(&leaked),
            })
            .with_span_events(FmtSpan::NEW)
            .finish();
        let request =
            Request::get("/stores?continuation_token=secret-continuation-token&page_size=10")
                .body(())?;

        tracing::subscriber::with_default(subscriber, || {
            let span = request_span(&request);
            let _entered = span.enter();
            tracing::info!("request admitted");
        });

        assert!(!leaked.load(Ordering::SeqCst));
        Ok(())
    }
}
