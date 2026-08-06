//! Explicit Axum routes for the pinned `OpenFGA` HTTP/JSON protocol.

use std::{
    collections::HashSet,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{
        ConnectInfo, DefaultBodyLimit, Extension, Path, Query, State,
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
use prost_reflect::{DescriptorPool, Kind, MessageDescriptor};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};
use tonic::Status;
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
    let timeout = api.config.request_timeout.duration();
    let admission = api.admission.clone();
    let api = Arc::new(api);
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
        .layer(middleware::from_fn_with_state(
            Arc::clone(&api),
            normalize_proto_json,
        ))
        .layer(middleware::from_fn_with_state(timeout, request_timeout))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&api),
            limit_endpoint_concurrency,
        ))
        .layer(middleware::from_fn_with_state(admission, admit_principal))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&api),
            authorize_request,
        ))
        .layer(middleware::from_fn_with_state(authentication, authenticate))
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
        .with_state(api)
}

async fn limit_endpoint_concurrency(
    State(api): State<Arc<OpenFgaApi>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    match api.acquire_endpoint_permit() {
        Ok(permit) => {
            let response = next.run(request).await;
            drop(permit);
            response
        }
        Err(error) => error.into_response(),
    }
}

async fn normalize_proto_json(
    State(api): State<Arc<OpenFgaApi>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let descriptor = match request_message_descriptor(request.method(), request.uri().path()) {
        Ok(Some(descriptor)) => descriptor,
        Ok(None) => return next.run(request).await,
        Err(error) => return error.into_response(),
    };
    let (parts, body) = request.into_parts();
    let Ok(bytes) = to_bytes(body, api.config.maximum_message_bytes).await else {
        return ApiError::payload_too_large().into_response();
    };
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let mut value = match JsonSeed::Message(descriptor.clone()).deserialize(&mut deserializer) {
        Ok(value) => value.0,
        Err(error) => {
            return ApiError::protobuf_json(protobuf_json_diagnostic(&error.to_string()))
                .into_response();
        }
    };
    if let Err(error) = deserializer.end() {
        return ApiError::protobuf_json(protobuf_json_diagnostic(&error.to_string()))
            .into_response();
    }
    match normalize_message_json(&mut value, &descriptor, descriptor.name()) {
        Ok(_) => {}
        Err(error) => return error.into_response(),
    }
    let body = match serde_json::to_vec(&value) {
        Ok(value) => Body::from(value),
        Err(_) => return ApiError::internal().into_response(),
    };
    next.run(Request::from_parts(parts, body)).await
}

fn request_message_descriptor(
    method: &axum::http::Method,
    path: &str,
) -> Result<Option<MessageDescriptor>, ApiError> {
    let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
    let name = match (method, segments.as_slice()) {
        (&axum::http::Method::POST, ["stores"]) => Some("CreateStoreRequest"),
        (&axum::http::Method::PUT, ["stores", _, "assertions", _]) => {
            Some("WriteAssertionsRequest")
        }
        (&axum::http::Method::POST, ["stores", _, "authorization-models"]) => {
            Some("WriteAuthorizationModelRequest")
        }
        (&axum::http::Method::POST, ["stores", _, "batch-check"]) => Some("BatchCheckRequest"),
        (&axum::http::Method::POST, ["stores", _, "check"]) => Some("CheckRequest"),
        (&axum::http::Method::POST, ["stores", _, "read"]) => Some("ReadRequest"),
        (&axum::http::Method::POST, ["stores", _, "write"]) => Some("WriteRequest"),
        (&axum::http::Method::POST, ["stores", _, "expand"]) => Some("ExpandRequest"),
        (&axum::http::Method::POST, ["stores", _, "list-objects"]) => Some("ListObjectsRequest"),
        (&axum::http::Method::POST, ["stores", _, "list-users"]) => Some("ListUsersRequest"),
        (&axum::http::Method::POST, ["stores", _, "streamed-list-objects"]) => {
            Some("StreamedListObjectsRequest")
        }
        _ => None,
    };
    name.map(|name| {
        descriptor_pool()?
            .get_message_by_name(&format!("openfga.v1.{name}"))
            .ok_or_else(ApiError::internal)
    })
    .transpose()
}

fn descriptor_pool() -> Result<&'static DescriptorPool, ApiError> {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    if let Some(pool) = POOL.get() {
        return Ok(pool);
    }
    let pool = DescriptorPool::decode(openfga_proto::FILE_DESCRIPTOR_SET)
        .map_err(|_| ApiError::internal())?;
    let _ = POOL.set(pool);
    POOL.get().ok_or_else(ApiError::internal)
}

fn normalize_message_json(
    value: &mut Value,
    descriptor: &MessageDescriptor,
    path: &str,
) -> Result<bool, ApiError> {
    if matches!(
        descriptor.full_name(),
        "google.protobuf.Struct" | "google.protobuf.Value"
    ) {
        return Ok(false);
    }
    let Value::Object(object) = value else {
        return Ok(false);
    };
    let mut changed = false;
    let keys = object.keys().cloned().collect::<Vec<_>>();
    for key in keys {
        let Some(field) = descriptor
            .fields()
            .find(|field| field.json_name() == key || field.name() == key)
        else {
            continue;
        };
        let field_path = format!("{path}.{}", upper_camel(field.name()));
        let Some(field_value) = object.get_mut(&key) else {
            continue;
        };
        if field_value.is_null() {
            object.remove(&key);
            changed = true;
            continue;
        }
        if let Kind::Enum(enumeration) = field.kind()
            && let Some(number) = field_value.as_i64()
            && (i32::try_from(number)
                .ok()
                .and_then(|number| enumeration.get_value(number)))
            .is_none()
        {
            return Err(ApiError::protobuf_json(format!(
                "invalid {field_path}: value must be one of the defined enum values"
            )));
        }
        if field.is_list() {
            if let (Kind::Message(message), Value::Array(values)) = (field.kind(), field_value) {
                for value in values {
                    changed |= normalize_message_json(value, &message, &field_path)?;
                }
            }
        } else if field.is_map() {
            if let (Kind::Message(entry), Value::Object(values)) = (field.kind(), field_value)
                && let Some(value_field) = entry.get_field_by_name("value")
                && let Kind::Message(message) = value_field.kind()
            {
                for value in values.values_mut() {
                    changed |= normalize_message_json(value, &message, &field_path)?;
                }
            }
        } else if let Kind::Message(message) = field.kind() {
            changed |= normalize_message_json(field_value, &message, &field_path)?;
        }
    }
    Ok(changed)
}

fn upper_camel(value: &str) -> String {
    value
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut characters = part.chars();
            characters.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(characters).collect()
            })
        })
        .collect()
}

#[derive(Debug)]
struct DuplicateJson(Value);

#[derive(Clone, Debug)]
enum JsonSeed {
    Message(MessageDescriptor),
    List(Kind),
    Map(Kind),
    Dynamic,
    Scalar,
}

impl JsonSeed {
    fn for_field(field: &prost_reflect::FieldDescriptor) -> Self {
        if field.is_list() {
            Self::List(field.kind())
        } else if field.is_map() {
            match field.kind() {
                Kind::Message(entry) => entry
                    .get_field_by_name("value")
                    .map_or(Self::Dynamic, |value| Self::Map(value.kind())),
                _ => Self::Dynamic,
            }
        } else {
            Self::for_kind(field.kind())
        }
    }

    fn for_kind(kind: Kind) -> Self {
        match kind {
            Kind::Message(message)
                if matches!(
                    message.full_name(),
                    "google.protobuf.Struct" | "google.protobuf.Value"
                ) =>
            {
                Self::Dynamic
            }
            Kind::Message(message) => Self::Message(message),
            _ => Self::Scalar,
        }
    }
}

impl<'de> DeserializeSeed<'de> for JsonSeed {
    type Value = DuplicateJson;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DuplicateJsonVisitor(self))
    }
}

struct DuplicateJsonVisitor(JsonSeed);

impl<'de> Visitor<'de> for DuplicateJsonVisitor {
    type Value = DuplicateJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value with unique object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(DuplicateJson(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(DuplicateJson(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(DuplicateJson(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .map(DuplicateJson)
            .ok_or_else(|| E::custom("invalid non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(DuplicateJson(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(DuplicateJson(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(DuplicateJson(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(DuplicateJson(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.0.deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or_default());
        let item_seed = match self.0 {
            JsonSeed::List(kind) => JsonSeed::for_kind(kind),
            _ => JsonSeed::Dynamic,
        };
        while let Some(value) = sequence.next_element_seed(item_seed.clone())? {
            values.push(value.0);
        }
        Ok(DuplicateJson(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        match self.0 {
            JsonSeed::Message(descriptor) => {
                let mut fields = HashSet::with_capacity(object.size_hint().unwrap_or_default());
                while let Some(key) = object.next_key::<String>()? {
                    let Some(field) = descriptor
                        .fields()
                        .find(|field| field.json_name() == key || field.name() == key)
                    else {
                        object.next_value::<IgnoredAny>()?;
                        continue;
                    };
                    if !fields.insert(field.number()) {
                        return Err(A::Error::custom(format!("duplicate field \"{key}\"")));
                    }
                    let preserve_null = matches!(
                        field.kind(),
                        Kind::Message(message) if message.full_name() == "google.protobuf.Value"
                    );
                    let value = object.next_value_seed(JsonSeed::for_field(&field))?;
                    if !value.0.is_null() || preserve_null {
                        values.insert(key, value.0);
                    }
                }
            }
            JsonSeed::Map(kind) => {
                let mut keys = HashSet::with_capacity(object.size_hint().unwrap_or_default());
                while let Some(key) = object.next_key::<String>()? {
                    if !keys.insert(key.clone()) {
                        return Err(A::Error::custom(format!("duplicate map key \"{key}\"")));
                    }
                    let value = object.next_value_seed(JsonSeed::for_kind(kind.clone()))?;
                    values.insert(key, value.0);
                }
            }
            JsonSeed::Dynamic | JsonSeed::List(_) | JsonSeed::Scalar => {
                let mut keys = HashSet::with_capacity(object.size_hint().unwrap_or_default());
                while let Some(key) = object.next_key::<String>()? {
                    if !keys.insert(key.clone()) {
                        return Err(A::Error::custom(format!("duplicate map key \"{key}\"")));
                    }
                    let value = object.next_value_seed(JsonSeed::Dynamic)?;
                    values.insert(key, value.0);
                }
            }
        }
        Ok(DuplicateJson(Value::Object(values)))
    }
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
    let peer_ip = peer_ip(&request);
    if let Err(error) = state.admission.admit_authentication(peer_ip) {
        return error.into_response();
    }
    let authorization_values = request.headers().get_all(axum::http::header::AUTHORIZATION);
    let mut authorization_values = authorization_values.iter();
    let header = authorization_values
        .next()
        .and_then(|value| value.to_str().ok());
    if authorization_values.next().is_some() {
        return match state.admission.record_authentication_failure(peer_ip) {
            Ok(()) => ApiError::unauthenticated().into_response(),
            Err(overloaded) => overloaded.into_response(),
        };
    }
    match state.authentication.authenticate(header) {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(error) => match state.admission.record_authentication_failure(peer_ip) {
            Ok(()) => ApiError::from(error).into_response(),
            Err(overloaded) => overloaded.into_response(),
        },
    }
}

fn peer_ip<B>(request: &Request<B>) -> IpAddr {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map_or(IpAddr::V4(Ipv4Addr::LOCALHOST), |peer| peer.ip())
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

async fn authorize_request(
    State(api): State<Arc<OpenFgaApi>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(principal) = request.extensions().get::<Principal>() else {
        return ApiError::unauthenticated().into_response();
    };
    let Some((action, store_id)) = route_authorization(request.method(), request.uri().path())
    else {
        return next.run(request).await;
    };
    match api.preauthorize(principal, action, store_id) {
        Ok(()) => next.run(request).await,
        Err(error) => error.into_response(),
    }
}

fn route_authorization<'a>(
    method: &axum::http::Method,
    path: &'a str,
) -> Option<(Action, Option<&'a str>)> {
    let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
    match (method, segments.as_slice()) {
        (&axum::http::Method::POST, ["stores"]) => Some((Action::CreateStore, None)),
        (&axum::http::Method::GET, ["stores"]) => Some((Action::ListStores, None)),
        (&axum::http::Method::GET, ["stores", store]) => Some((Action::GetStore, Some(store))),
        (&axum::http::Method::DELETE, ["stores", store]) => {
            Some((Action::DeleteStore, Some(store)))
        }
        (&axum::http::Method::GET, ["stores", store, "assertions", _]) => {
            Some((Action::ReadAssertions, Some(store)))
        }
        (&axum::http::Method::PUT, ["stores", store, "assertions", _]) => {
            Some((Action::WriteAssertions, Some(store)))
        }
        (
            &axum::http::Method::GET,
            ["stores", store, "authorization-models"]
            | ["stores", store, "authorization-models", _],
        ) => Some((Action::ReadAuthorizationModels, Some(store))),
        (&axum::http::Method::POST, ["stores", store, "authorization-models"]) => {
            Some((Action::WriteAuthorizationModel, Some(store)))
        }
        (&axum::http::Method::POST, ["stores", store, "batch-check"]) => {
            Some((Action::BatchCheck, Some(store)))
        }
        (&axum::http::Method::GET, ["stores", store, "changes"]) => {
            Some((Action::ReadChanges, Some(store)))
        }
        (&axum::http::Method::POST, ["stores", store, "check"]) => {
            Some((Action::Check, Some(store)))
        }
        (&axum::http::Method::POST, ["stores", store, "read"]) => Some((Action::Read, Some(store))),
        (&axum::http::Method::POST, ["stores", store, "write"]) => {
            Some((Action::Write, Some(store)))
        }
        (&axum::http::Method::POST, ["stores", store, "expand"]) => {
            Some((Action::Expand, Some(store)))
        }
        (&axum::http::Method::POST, ["stores", store, "list-objects"]) => {
            Some((Action::ListObjects, Some(store)))
        }
        (&axum::http::Method::POST, ["stores", store, "list-users"]) => {
            Some((Action::ListUsers, Some(store)))
        }
        (&axum::http::Method::POST, ["stores", store, "streamed-list-objects"]) => {
            Some((Action::StreamedListObjects, Some(store)))
        }
        _ => None,
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
    body: Result<Json<pb::ExpandRequest>, JsonRejection>,
) -> Result<(), ApiError> {
    let mut request = json(body)?;
    request.store_id.clone_from(&store_id);
    ApiError::validate(&request)?;
    authorize_unimplemented(&api, &principal, Action::Expand, &store_id)
}

async fn list_objects(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::ListObjectsRequest>, JsonRejection>,
) -> Result<(), ApiError> {
    let mut request = json(body)?;
    request.store_id.clone_from(&store_id);
    ApiError::validate_list_objects(&request)?;
    authorize_unimplemented(&api, &principal, Action::ListObjects, &store_id)
}

async fn streamed_list_objects(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::StreamedListObjectsRequest>, JsonRejection>,
) -> Response {
    let mut request = match json(body) {
        Ok(request) => request,
        Err(error) => return streamed_error_response(error),
    };
    request.store_id.clone_from(&store_id);
    if let Err(error) = ApiError::validate_streamed_list_objects(&request) {
        return streamed_error_response(error);
    }
    match authorize_unimplemented(&api, &principal, Action::StreamedListObjects, &store_id) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => streamed_error_response(error),
    }
}

#[derive(Debug, Serialize)]
struct StreamErrorResponse {
    error: StreamError,
}

#[derive(Debug, Serialize)]
struct StreamError {
    code: i32,
    message: String,
    details: Vec<()>,
}

fn streamed_error_response(error: ApiError) -> Response {
    let status = Status::from(error);
    (
        StatusCode::BAD_REQUEST,
        Json(StreamErrorResponse {
            error: StreamError {
                code: status.code() as i32,
                message: status.message().to_owned(),
                details: Vec::new(),
            },
        }),
    )
        .into_response()
}

async fn list_users(
    State(api): State<Arc<OpenFgaApi>>,
    Extension(principal): Extension<Principal>,
    Path(store_id): Path<String>,
    body: Result<Json<pb::ListUsersRequest>, JsonRejection>,
) -> Result<(), ApiError> {
    let mut request = json(body)?;
    request.store_id.clone_from(&store_id);
    ApiError::validate_list_users(&request)?;
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
            ApiError::protobuf_json(protobuf_json_diagnostic(&rejection.body_text()))
        }
    })
}

fn protobuf_json_diagnostic(rejection: &str) -> String {
    const MAX_DETAIL_BYTES: usize = 256;
    let (without_location, location) = split_json_location(rejection);
    let syntax_error = [
        "expected value",
        "EOF while parsing",
        "trailing characters",
        "key must be a string",
    ]
    .iter()
    .any(|marker| without_location.contains(marker));
    if syntax_error {
        return "malformed JSON".to_owned();
    }
    let detail = [
        "duplicate map key ",
        "duplicate field ",
        "unknown field ",
        "unknown variant ",
        "invalid type: ",
        "invalid value: ",
    ]
    .iter()
    .find_map(|marker| {
        without_location
            .find(marker)
            .map(|index| &without_location[index..])
    })
    .unwrap_or("invalid protobuf JSON");
    let detail = bounded_json_detail(detail, MAX_DETAIL_BYTES).replace('`', "\"");
    let duplicate_key_bytes = detail
        .strip_prefix("duplicate map key \"")
        .or_else(|| detail.strip_prefix("duplicate field \""))
        .and_then(|value| value.find('"'));
    location.map_or_else(
        || detail.clone(),
        |(line, column)| {
            let column = column.parse::<usize>().ok().map_or_else(
                || column.to_owned(),
                |column| {
                    duplicate_key_bytes
                        .map_or(column, |length| {
                            column.saturating_sub(length.saturating_add(1))
                        })
                        .to_string()
                },
            );
            format!("(line {line}:{column}): {detail}")
        },
    )
}

fn split_json_location(value: &str) -> (&str, Option<(&str, &str)>) {
    let Some((message, location)) = value.rsplit_once(" at line ") else {
        return (value, None);
    };
    let Some((line, column)) = location.split_once(" column ") else {
        return (value, None);
    };
    if line.bytes().all(|byte| byte.is_ascii_digit())
        && column.bytes().all(|byte| byte.is_ascii_digit())
    {
        (message, Some((line, column)))
    } else {
        (value, None)
    }
}

fn bounded_json_detail(value: &str, maximum: usize) -> &str {
    if value.len() <= maximum {
        return value;
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    &value[..end]
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
