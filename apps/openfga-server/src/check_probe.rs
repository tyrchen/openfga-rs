//! Loopback-only Phase 1 Check probe and normalized differential comparator.

use std::{
    io::{self, Write},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use openfga_check::CheckBudget;
use openfga_domain::{
    AuthorizationModelId, CheckCommand, ConditionContext, ConsistencyPreference, ContextualTuples,
    Deadline, InputLimits, ModelSelection, ObjectRef, Principal, PrincipalKind, QueryContext,
    RelationName, RelationshipTuple, RequestTimeout, StoreId, SubjectRef, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_service::{CheckService, ServiceError, ServiceErrorKind};
use openfga_storage::{
    ModelReader, ModelWriter, OperationContext, StorageCancellationToken, StoreName, StoreWriter,
    StoredAuthorizationModel, TupleReader, TupleWriteOptions, TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use reqwest::{Client, Url, redirect::Policy};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer};

use super::{health, shutdown_signal, validated_loopback_url};

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const GO_BASELINE_COMMIT: &str = "4e4f79ed841513dfd61746a75ef473f6198299f7";
const MAXIMUM_BODY_BYTES: usize = 32 * 1_024;
const MAXIMUM_RESPONSE_BYTES: usize = 8 * 1_024;
const MAXIMUM_CONCURRENCY: usize = 16;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
struct ProbeState {
    service: CheckService,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireTupleKey {
    user: String,
    relation: String,
    object: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireContextualTuples {
    tuple_keys: Vec<WireTupleKey>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireCheckRequest {
    tuple_key: WireTupleKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contextual_tuples: Option<WireContextualTuples>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context: Option<Value>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    authorization_model_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    consistency: String,
    #[serde(default)]
    trace: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct WireCheckResponse {
    allowed: bool,
}

#[derive(Debug, Serialize)]
struct WireErrorResponse {
    code: &'static str,
    message: &'static str,
}

#[derive(Debug)]
struct ProbeError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ProbeError {
    const fn validation() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "validation_error",
            message: "invalid check input",
        }
    }
}

impl IntoResponse for ProbeError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(WireErrorResponse {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

/// Runs the loopback-only Check compatibility probe.
pub(crate) async fn serve(address: SocketAddr) -> Result<()> {
    if !address.ip().is_loopback() {
        bail!("the Phase 1 Check probe may bind only to a loopback address");
    }
    let (storage, state) = configured_probe().await?;
    let application = Router::new()
        .route("/healthz", get(health))
        .route("/stores/{store_id}/check", post(check))
        .with_state(state)
        .layer(RequestBodyLimitLayer::new(MAXIMUM_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .layer(ConcurrencyLimitLayer::new(MAXIMUM_CONCURRENCY));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind the Phase 1 Check probe to {address}"))?;
    let serve_result = axum::serve(listener, application)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Phase 1 Check probe server failed");
    let mut owner = Arc::try_unwrap(storage)
        .map_err(|_| anyhow::anyhow!("Check probe retained a storage capability after shutdown"))?;
    let stop_result = owner
        .stop()
        .await
        .context("failed to stop Check storage actor");
    serve_result?;
    stop_result
}

async fn check(
    Path(store_id): Path<String>,
    State(state): State<ProbeState>,
    Json(request): Json<WireCheckRequest>,
) -> Result<Json<WireCheckResponse>, ProbeError> {
    let command = convert_request(&store_id, request)?;
    let outcome = state
        .service
        .check(&command, StorageCancellationToken::new())
        .await
        .map_err(|error| map_service_error(&error))?;
    Ok(Json(WireCheckResponse {
        allowed: outcome.allowed(),
    }))
}

fn convert_request(store_id: &str, request: WireCheckRequest) -> Result<CheckCommand, ProbeError> {
    let limits = InputLimits::default();
    let store_id = store_id
        .parse::<StoreId>()
        .map_err(|_| ProbeError::validation())?;
    let model_selection = if request.authorization_model_id.is_empty() {
        ModelSelection::Latest
    } else {
        ModelSelection::Explicit(
            request
                .authorization_model_id
                .parse::<AuthorizationModelId>()
                .map_err(|_| ProbeError::validation())?,
        )
    };
    let tuple = convert_tuple(&request.tuple_key, &limits)?;
    let contextual_tuples = request
        .contextual_tuples
        .map_or_else(Vec::new, |tuples| tuples.tuple_keys)
        .into_iter()
        .map(|tuple| convert_tuple(&tuple, &limits).map(RelationshipTuple::unconditional))
        .collect::<Result<Vec<_>, _>>()?;
    let contextual_tuples =
        ContextualTuples::new(contextual_tuples, &limits).map_err(|_| ProbeError::validation())?;
    let condition_context = ConditionContext::try_from_json(
        request.context.unwrap_or_else(|| Value::Object(Map::new())),
        &limits,
    )
    .map_err(|_| ProbeError::validation())?;
    let consistency = convert_consistency(&request.consistency)?;
    let deadline = Deadline::from_timeout(
        Instant::now(),
        RequestTimeout::new(REQUEST_TIMEOUT).map_err(|_| ProbeError::validation())?,
    )
    .map_err(|_| ProbeError::validation())?;
    let query = QueryContext::builder()
        .store_id(store_id)
        .model_selection(model_selection)
        .consistency(consistency)
        .contextual_tuples(contextual_tuples)
        .condition_context(condition_context)
        .deadline(deadline)
        .principal(Principal::new(
            PrincipalKind::Development,
            "phase1-check-probe"
                .parse()
                .map_err(|_| ProbeError::validation())?,
        ))
        .build();
    Ok(CheckCommand::new(query, tuple))
}

fn convert_tuple(tuple: &WireTupleKey, limits: &InputLimits) -> Result<TupleKey, ProbeError> {
    let object = ObjectRef::parse_with_limits(&tuple.object, limits)
        .map_err(|_| ProbeError::validation())?;
    let relation = RelationName::parse_with_limits(&tuple.relation, limits)
        .map_err(|_| ProbeError::validation())?;
    let subject =
        SubjectRef::parse_with_limits(&tuple.user, limits).map_err(|_| ProbeError::validation())?;
    Ok(TupleKey::new(object, relation, subject))
}

fn convert_consistency(value: &str) -> Result<ConsistencyPreference, ProbeError> {
    match value {
        "" | "UNSPECIFIED" | "MINIMIZE_LATENCY" => Ok(ConsistencyPreference::MinimizeLatency),
        "HIGHER_CONSISTENCY" => Ok(ConsistencyPreference::HigherConsistency),
        _ => Err(ProbeError::validation()),
    }
}

fn map_service_error(error: &ServiceError) -> ProbeError {
    let (status, code, message) = match error.kind() {
        ServiceErrorKind::ModelNotFound => (
            StatusCode::NOT_FOUND,
            "authorization_model_not_found",
            "authorization model not found",
        ),
        ServiceErrorKind::InvalidRequest | ServiceErrorKind::Condition => (
            StatusCode::BAD_REQUEST,
            "validation_error",
            "invalid check input",
        ),
        ServiceErrorKind::ResourceExhausted => (
            StatusCode::TOO_MANY_REQUESTS,
            "resource_exhausted",
            "authorization work limit exceeded",
        ),
        ServiceErrorKind::Timeout | ServiceErrorKind::Cancelled => (
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            "authorization request did not complete",
        ),
        ServiceErrorKind::Storage => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "authorization service unavailable",
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "authorization service failed",
        ),
    };
    ProbeError {
        status,
        code,
        message,
    }
}

async fn configured_probe() -> Result<(Arc<MemoryStorage>, ProbeState)> {
    let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
    let context = operation_context()?;
    let store_id = STORE_ID.parse::<StoreId>()?;
    storage
        .create_store(
            &context,
            store_id,
            StoreName::new("phase1-check-probe".to_owned())?,
        )
        .await?;
    let source = Arc::new(fixture_model_source()?);
    let compiled = ModelCompiler::default().compile(&source)?;
    storage
        .write_model(
            &context,
            Arc::new(StoredAuthorizationModel::new(
                source,
                compiled,
                SystemTime::now(),
            )?),
        )
        .await?;
    storage
        .write_tuples(
            &context,
            store_id,
            Vec::new(),
            fixture_tuples()?,
            TupleWriteOptions::default(),
        )
        .await?;
    let models: Arc<dyn ModelReader> = storage.clone();
    let tuples: Arc<dyn TupleReader> = storage.clone();
    Ok((
        storage,
        ProbeState {
            service: CheckService::direct(models, tuples, CheckBudget::default()),
        },
    ))
}

fn operation_context() -> Result<OperationContext> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(Instant::now(), RequestTimeout::new(REQUEST_TIMEOUT)?)?,
        StorageCancellationToken::new(),
    ))
}

fn fixture_model_source() -> Result<AuthorizationModelSource> {
    let object = |subject_type: &str| -> Result<DirectRestrictionSource> {
        Ok(DirectRestrictionSource::new(
            subject_type.parse()?,
            RestrictionKindSource::Object,
            None,
        ))
    };
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![
            TypeDefinitionSource::new("user".parse()?, Vec::new()),
            TypeDefinitionSource::new(
                "group".parse()?,
                vec![RelationSource::new(
                    "member".parse()?,
                    RewriteSource::Direct,
                    vec![object("user")?],
                )],
            ),
            TypeDefinitionSource::new(
                "document".parse()?,
                vec![RelationSource::new(
                    "viewer".parse()?,
                    RewriteSource::Direct,
                    vec![
                        object("user")?,
                        DirectRestrictionSource::new(
                            "user".parse()?,
                            RestrictionKindSource::Wildcard,
                            None,
                        ),
                        DirectRestrictionSource::new(
                            "group".parse()?,
                            RestrictionKindSource::Userset("member".parse()?),
                            None,
                        ),
                    ],
                )],
            ),
        ],
        Vec::new(),
    ))
}

fn fixture_tuples() -> Result<Vec<RelationshipTuple>> {
    [
        "document:direct#viewer@user:anne",
        "document:wild#viewer@user:*",
        "document:userset#viewer@group:eng#member",
        "group:eng#member@user:bob",
    ]
    .into_iter()
    .map(|tuple| {
        tuple
            .parse::<TupleKey>()
            .map(RelationshipTuple::unconditional)
            .map_err(Into::into)
    })
    .collect()
}

#[derive(Clone, Copy, Debug)]
struct DifferentialCase {
    name: &'static str,
    object: &'static str,
    relation: &'static str,
    user: &'static str,
    contextual_tuple: Option<(&'static str, &'static str, &'static str)>,
}

impl DifferentialCase {
    fn request(self, model_id: &str) -> WireCheckRequest {
        WireCheckRequest {
            tuple_key: WireTupleKey {
                object: self.object.to_owned(),
                relation: self.relation.to_owned(),
                user: self.user.to_owned(),
            },
            contextual_tuples: self.contextual_tuple.map(|(object, relation, user)| {
                WireContextualTuples {
                    tuple_keys: vec![WireTupleKey {
                        object: object.to_owned(),
                        relation: relation.to_owned(),
                        user: user.to_owned(),
                    }],
                }
            }),
            context: None,
            authorization_model_id: model_id.to_owned(),
            consistency: String::new(),
            trace: false,
        }
    }
}

const DIFFERENTIAL_CASES: [DifferentialCase; 6] = [
    DifferentialCase {
        name: "direct_allow",
        object: "document:direct",
        relation: "viewer",
        user: "user:anne",
        contextual_tuple: None,
    },
    DifferentialCase {
        name: "direct_deny",
        object: "document:direct",
        relation: "viewer",
        user: "user:bob",
        contextual_tuple: None,
    },
    DifferentialCase {
        name: "typed_wildcard_allow",
        object: "document:wild",
        relation: "viewer",
        user: "user:carol",
        contextual_tuple: None,
    },
    DifferentialCase {
        name: "userset_allow",
        object: "document:userset",
        relation: "viewer",
        user: "user:bob",
        contextual_tuple: None,
    },
    DifferentialCase {
        name: "contextual_tuple_allow",
        object: "document:contextual",
        relation: "viewer",
        user: "user:carol",
        contextual_tuple: Some(("document:contextual", "viewer", "user:carol")),
    },
    DifferentialCase {
        name: "invalid_object_error",
        object: "",
        relation: "viewer",
        user: "user:anne",
        contextual_tuple: None,
    },
];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalizedCheck {
    http_status: u16,
    allowed: Option<bool>,
    error_class: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckCaseReport {
    name: &'static str,
    go: NormalizedCheck,
    rust: NormalizedCheck,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckMismatch {
    case: &'static str,
    field: &'static str,
    go: String,
    rust: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DifferentialCheckReport {
    baseline_commit: &'static str,
    corpus_source: &'static str,
    cases: Vec<CheckCaseReport>,
    mismatches: Vec<CheckMismatch>,
}

#[derive(Debug, Deserialize)]
struct CreateStoreResponse {
    id: String,
}

#[derive(Debug, Deserialize)]
struct WriteModelResponse {
    authorization_model_id: String,
}

/// Runs the bounded field-level Check differential against both loopback servers.
pub(crate) async fn run_differential(go_url: &str, rust_url: &str) -> Result<()> {
    let go_url = validated_loopback_url(go_url)?;
    let rust_url = validated_loopback_url(rust_url)?;
    let client = differential_client()?;
    let (go_store, go_model) = configure_go(&client, &go_url).await?;
    let mut reports = Vec::with_capacity(DIFFERENTIAL_CASES.len());
    let mut mismatches = Vec::new();
    for case in DIFFERENTIAL_CASES {
        let go = observe_check(&client, &go_url, &go_store, &case.request(&go_model))
            .await
            .with_context(|| format!("failed to run Go Check case {}", case.name))?;
        let rust = observe_check(&client, &rust_url, STORE_ID, &case.request(MODEL_ID))
            .await
            .with_context(|| format!("failed to run Rust Check case {}", case.name))?;
        compare_case(case.name, &go, &rust, &mut mismatches);
        reports.push(CheckCaseReport {
            name: case.name,
            go,
            rust,
        });
    }
    let report = DifferentialCheckReport {
        baseline_commit: GO_BASELINE_COMMIT,
        corpus_source: "vendors/openfga/tests/check",
        cases: reports,
        mismatches,
    };
    write_report(&report)?;
    if !report.mismatches.is_empty() {
        bail!("Check differential found normalized mismatches");
    }
    Ok(())
}

fn differential_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(REQUEST_TIMEOUT)
        .redirect(Policy::none())
        .build()
        .context("failed to build the Check differential client")
}

async fn configure_go(client: &Client, base_url: &Url) -> Result<(String, String)> {
    let store_url = base_url
        .join("stores")
        .context("failed to build store URL")?;
    let response = client
        .post(store_url)
        .json(&json!({"name": "phase1-check-differential"}))
        .send()
        .await
        .context("Go store creation request failed")?;
    let store = read_success_json::<CreateStoreResponse>(response, "Go store creation").await?;
    let store_id = store
        .id
        .parse::<StoreId>()
        .context("Go store creation returned an invalid store ID")?;
    let model_url = base_url
        .join(&format!("stores/{store_id}/authorization-models"))
        .context("failed to build model URL")?;
    let response = client
        .post(model_url)
        .json(&go_model_document())
        .send()
        .await
        .context("Go model write request failed")?;
    let model = read_success_json::<WriteModelResponse>(response, "Go model write").await?;
    let model_id = model
        .authorization_model_id
        .parse::<AuthorizationModelId>()
        .context("Go model write returned an invalid model ID")?;
    let write_url = base_url
        .join(&format!("stores/{store_id}/write"))
        .context("failed to build tuple write URL")?;
    let response = client
        .post(write_url)
        .json(&json!({
            "writes": {"tuple_keys": go_fixture_tuples()},
            "authorization_model_id": model_id.to_string(),
        }))
        .send()
        .await
        .context("Go tuple write request failed")?;
    require_success(response, "Go tuple write").await?;
    Ok((store_id.to_string(), model_id.to_string()))
}

fn go_model_document() -> Value {
    json!({
        "schema_version": "1.1",
        "type_definitions": [
            {"type": "user"},
            {
                "type": "group",
                "relations": {"member": {"this": {}}},
                "metadata": {"relations": {"member": {
                    "directly_related_user_types": [{"type": "user"}]
                }}}
            },
            {
                "type": "document",
                "relations": {"viewer": {"this": {}}},
                "metadata": {"relations": {"viewer": {
                    "directly_related_user_types": [
                        {"type": "user"},
                        {"type": "user", "wildcard": {}},
                        {"type": "group", "relation": "member"}
                    ]
                }}}
            }
        ]
    })
}

fn go_fixture_tuples() -> Vec<Value> {
    vec![
        json!({"object": "document:direct", "relation": "viewer", "user": "user:anne"}),
        json!({"object": "document:wild", "relation": "viewer", "user": "user:*"}),
        json!({"object": "document:userset", "relation": "viewer", "user": "group:eng#member"}),
        json!({"object": "group:eng", "relation": "member", "user": "user:bob"}),
    ]
}

async fn observe_check(
    client: &Client,
    base_url: &Url,
    store_id: &str,
    request: &WireCheckRequest,
) -> Result<NormalizedCheck> {
    let check_url = base_url
        .join(&format!("stores/{store_id}/check"))
        .context("failed to build Check URL")?;
    let response = client
        .post(check_url)
        .json(request)
        .send()
        .await
        .context("Check request failed")?;
    let status = response.status();
    if status.is_success() {
        let body = read_bounded(response).await?;
        let response: WireCheckResponse =
            serde_json::from_slice(&body).context("successful Check body is not valid JSON")?;
        return Ok(NormalizedCheck {
            http_status: status.as_u16(),
            allowed: Some(response.allowed),
            error_class: None,
        });
    }
    let _body = read_bounded(response).await?;
    Ok(NormalizedCheck {
        http_status: status.as_u16(),
        allowed: None,
        error_class: Some(classify_error(status)),
    })
}

const fn classify_error(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() {
        400 | 422 => "validation",
        404 => "not_found",
        408 => "timeout",
        429 => "resource_exhausted",
        500..=599 => "internal",
        _ => "client_error",
    }
}

fn compare_case(
    case: &'static str,
    go: &NormalizedCheck,
    rust: &NormalizedCheck,
    mismatches: &mut Vec<CheckMismatch>,
) {
    if go.http_status != rust.http_status {
        mismatches.push(CheckMismatch {
            case,
            field: "httpStatus",
            go: go.http_status.to_string(),
            rust: rust.http_status.to_string(),
        });
    }
    if go.allowed != rust.allowed {
        mismatches.push(CheckMismatch {
            case,
            field: "allowed",
            go: optional_value(go.allowed),
            rust: optional_value(rust.allowed),
        });
    }
    if go.error_class != rust.error_class {
        mismatches.push(CheckMismatch {
            case,
            field: "errorClass",
            go: go.error_class.unwrap_or("none").to_owned(),
            rust: rust.error_class.unwrap_or("none").to_owned(),
        });
    }
}

fn optional_value(value: Option<bool>) -> String {
    value.map_or_else(|| "none".to_owned(), |value| value.to_string())
}

async fn read_success_json<T>(response: reqwest::Response, operation: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let body = read_bounded(response).await?;
    if !status.is_success() {
        bail!("{operation} failed with HTTP status {status}");
    }
    serde_json::from_slice(&body).with_context(|| format!("{operation} returned invalid JSON"))
}

async fn require_success(response: reqwest::Response, operation: &str) -> Result<()> {
    let status = response.status();
    let _body = read_bounded(response).await?;
    if !status.is_success() {
        bail!("{operation} failed with HTTP status {status}");
    }
    Ok(())
}

async fn read_bounded(mut response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAXIMUM_RESPONSE_BYTES as u64)
    {
        bail!("differential response exceeds the configured byte limit");
    }
    let mut body = Vec::with_capacity(512);
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read differential response")?
    {
        let next = body
            .len()
            .checked_add(chunk.len())
            .context("differential response length overflowed")?;
        if next > MAXIMUM_RESPONSE_BYTES {
            bail!("differential response exceeds the configured byte limit");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn write_report(report: &DifferentialCheckReport) -> Result<()> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, report)
        .context("failed to serialize the Check differential report")?;
    writeln!(output).context("failed to terminate the Check differential report")
}

#[cfg(test)]
mod tests {
    use super::{CheckMismatch, NormalizedCheck, WireCheckRequest, compare_case, convert_request};

    #[test]
    fn test_should_reject_invalid_wire_values_without_echoing_them() {
        let request = WireCheckRequest {
            tuple_key: super::WireTupleKey {
                object: String::new(),
                relation: "viewer".to_owned(),
                user: "user:anne".to_owned(),
            },
            contextual_tuples: None,
            context: None,
            authorization_model_id: super::MODEL_ID.to_owned(),
            consistency: String::new(),
            trace: false,
        };
        let error = convert_request(super::STORE_ID, request)
            .err()
            .ok_or("invalid object unexpectedly converted");
        assert!(error.is_ok());
        assert!(!format!("{:?}", error.ok()).contains("user:anne"));
    }

    #[test]
    fn test_should_report_each_normalized_check_field_mismatch() {
        let go = NormalizedCheck {
            http_status: 200,
            allowed: Some(true),
            error_class: None,
        };
        let rust = NormalizedCheck {
            http_status: 400,
            allowed: None,
            error_class: Some("validation"),
        };
        let mut mismatches = Vec::<CheckMismatch>::new();
        compare_case("fixture", &go, &rust, &mut mismatches);
        assert_eq!(mismatches.len(), 3);
        assert_eq!(
            mismatches.first().map(|mismatch| mismatch.field),
            Some("httpStatus"),
        );
    }
}
