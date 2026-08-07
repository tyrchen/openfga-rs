//! Loopback-only Phase 1 Check probe and normalized differential comparator.

use std::{
    collections::BTreeMap,
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
use openfga_check::{CheckBudget, CheckErrorKind};
use openfga_condition::{ConditionDefinition, ParameterType};
use openfga_domain::{
    AuthorizationModelId, BatchCheckCommand, BatchCheckItem, BatchCheckItems, CheckCommand,
    ConditionBinding, ConditionContext, ConditionReference, ConsistencyPreference,
    ContextualTuples, Deadline, InputLimits, ModelSelection, ObjectRef, Principal, PrincipalKind,
    QueryContext, RelationName, RelationshipTuple, RequestTimeout, StoreId, SubjectRef, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, ConditionSource, DirectRestrictionSource, ModelCompiler,
    RelationSource, RestrictionKindSource, RewriteSource, TypeDefinitionSource,
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
pub(crate) const GO_BASELINE_COMMIT: &str = "4e4f79ed841513dfd61746a75ef473f6198299f7";
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireBatchCheckItem {
    tuple_key: WireTupleKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contextual_tuples: Option<WireContextualTuples>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context: Option<Value>,
    correlation_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireBatchCheckRequest {
    checks: Vec<WireBatchCheckItem>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    authorization_model_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    consistency: String,
}

#[derive(Debug, Serialize)]
struct WireBatchCheckResponse {
    result: BTreeMap<String, WireBatchCheckSingleResult>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum WireBatchCheckSingleResult {
    Allowed { allowed: bool },
    Error { error: WireBatchCheckError },
}

#[derive(Debug, Serialize)]
struct WireBatchCheckError {
    code: &'static str,
    message: &'static str,
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
        .route("/stores/{store_id}/batch-check", post(batch_check))
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
        .with_graceful_shutdown(async {
            let _shutdown_result = shutdown_signal().await;
        })
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

async fn batch_check(
    Path(store_id): Path<String>,
    State(state): State<ProbeState>,
    Json(request): Json<WireBatchCheckRequest>,
) -> Result<Json<WireBatchCheckResponse>, ProbeError> {
    let command = convert_batch_request(&store_id, request)?;
    let outcome = state
        .service
        .batch_check(&command, StorageCancellationToken::new())
        .await
        .map_err(|error| map_service_error(&error))?;
    let result = outcome
        .results()
        .iter()
        .map(|result| {
            let item = match result.outcome() {
                Ok(outcome) => WireBatchCheckSingleResult::Allowed {
                    allowed: outcome.allowed(),
                },
                Err(error) => WireBatchCheckSingleResult::Error {
                    error: map_batch_error(error.kind()),
                },
            };
            (result.correlation_id().as_str().to_owned(), item)
        })
        .collect();
    Ok(Json(WireBatchCheckResponse { result }))
}

const fn map_batch_error(kind: CheckErrorKind) -> WireBatchCheckError {
    let (code, message) = match kind {
        CheckErrorKind::InvalidModel | CheckErrorKind::InvalidTuple | CheckErrorKind::Condition => {
            ("validation_error", "invalid check input")
        }
        CheckErrorKind::Cancelled | CheckErrorKind::Timeout => {
            ("request_timeout", "authorization request did not complete")
        }
        CheckErrorKind::DepthExceeded
        | CheckErrorKind::DispatchExceeded
        | CheckErrorKind::DatastoreQueryExceeded
        | CheckErrorKind::TupleItemExceeded
        | CheckErrorKind::ConditionCostExceeded => {
            ("resource_exhausted", "authorization work limit exceeded")
        }
        CheckErrorKind::StorageUnavailable => {
            ("service_unavailable", "authorization service unavailable")
        }
        CheckErrorKind::Internal => ("internal_error", "authorization service failed"),
    };
    WireBatchCheckError { code, message }
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
    let tuple = convert_tuple(&request.tuple_key, &limits)?;
    let contextual_tuples = convert_contextual_tuples(request.contextual_tuples, &limits)?;
    let condition_context = convert_condition_context(request.context, &limits)?;
    let query = convert_query_context(
        store_id,
        &request.authorization_model_id,
        &request.consistency,
        contextual_tuples,
        condition_context,
    )?;
    Ok(CheckCommand::new(query, tuple))
}

fn convert_batch_request(
    store_id: &str,
    request: WireBatchCheckRequest,
) -> Result<BatchCheckCommand, ProbeError> {
    let limits = InputLimits::default();
    let items = request
        .checks
        .into_iter()
        .map(|item| {
            Ok(BatchCheckItem::new(
                item.correlation_id
                    .parse()
                    .map_err(|_| ProbeError::validation())?,
                convert_tuple(&item.tuple_key, &limits)?,
                convert_contextual_tuples(item.contextual_tuples, &limits)?,
                convert_condition_context(item.context, &limits)?,
            ))
        })
        .collect::<Result<Vec<_>, ProbeError>>()?;
    let items = BatchCheckItems::new(items, &limits).map_err(|_| ProbeError::validation())?;
    let query = convert_query_context(
        store_id,
        &request.authorization_model_id,
        &request.consistency,
        ContextualTuples::empty(),
        ConditionContext::empty(),
    )?;
    Ok(BatchCheckCommand::new(query, items))
}

fn convert_contextual_tuples(
    tuples: Option<WireContextualTuples>,
    limits: &InputLimits,
) -> Result<ContextualTuples, ProbeError> {
    let tuples = tuples
        .map_or_else(Vec::new, |tuples| tuples.tuple_keys)
        .into_iter()
        .map(|tuple| convert_tuple(&tuple, limits).map(RelationshipTuple::unconditional))
        .collect::<Result<Vec<_>, _>>()?;
    ContextualTuples::new(tuples, limits).map_err(|_| ProbeError::validation())
}

fn convert_condition_context(
    context: Option<Value>,
    limits: &InputLimits,
) -> Result<ConditionContext, ProbeError> {
    ConditionContext::try_from_json(context.unwrap_or_else(|| Value::Object(Map::new())), limits)
        .map_err(|_| ProbeError::validation())
}

fn convert_query_context(
    store_id: &str,
    authorization_model_id: &str,
    consistency: &str,
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
) -> Result<QueryContext, ProbeError> {
    let store_id = store_id
        .parse::<StoreId>()
        .map_err(|_| ProbeError::validation())?;
    let model_selection = if authorization_model_id.is_empty() {
        ModelSelection::Latest
    } else {
        ModelSelection::Explicit(
            authorization_model_id
                .parse::<AuthorizationModelId>()
                .map_err(|_| ProbeError::validation())?,
        )
    };
    let consistency = convert_consistency(consistency)?;
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
    Ok(query)
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
        ServiceErrorKind::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "authorization service unavailable",
        ),
        ServiceErrorKind::StoreNotFound
        | ServiceErrorKind::AlreadyExists
        | ServiceErrorKind::Conflict
        | ServiceErrorKind::InvalidContinuation
        | ServiceErrorKind::Internal => (
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

fn object_restriction(
    subject_type: &str,
    condition: Option<&str>,
) -> Result<DirectRestrictionSource> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Object,
        condition.map(str::parse).transpose()?,
    ))
}

fn wildcard_restriction(subject_type: &str) -> Result<DirectRestrictionSource> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Wildcard,
        None,
    ))
}

fn userset_restriction(subject_type: &str, relation: &str) -> Result<DirectRestrictionSource> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Userset(relation.parse()?),
        None,
    ))
}

fn computed_rewrite(relation: &str) -> Result<RewriteSource> {
    Ok(RewriteSource::Computed(relation.parse()?))
}

fn fixture_model_source() -> Result<AuthorizationModelSource> {
    let parameters = BTreeMap::from([("x".parse()?, ParameterType::any())]);
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
                    vec![
                        object_restriction("user", None)?,
                        userset_restriction("group", "member")?,
                    ],
                )],
            ),
            TypeDefinitionSource::new(
                "folder".parse()?,
                vec![RelationSource::new(
                    "viewer".parse()?,
                    RewriteSource::Direct,
                    vec![
                        object_restriction("user", None)?,
                        wildcard_restriction("user")?,
                    ],
                )],
            ),
            fixture_document_type()?,
        ],
        vec![
            ConditionSource::new(
                "under_limit".parse()?,
                ConditionDefinition::new(
                    "under_limit".parse()?,
                    "x < 100 && x == 50".to_owned(),
                    parameters.clone(),
                ),
            ),
            ConditionSource::new(
                "at_precision_boundary".parse()?,
                ConditionDefinition::new(
                    "at_precision_boundary".parse()?,
                    "x == 9223372036854775807".to_owned(),
                    parameters,
                ),
            ),
        ],
    ))
}

fn fixture_document_type() -> Result<TypeDefinitionSource> {
    Ok(TypeDefinitionSource::new(
        "document".parse()?,
        vec![
            RelationSource::new(
                "owner".parse()?,
                RewriteSource::Direct,
                vec![object_restriction("user", None)?],
            ),
            RelationSource::new(
                "editor".parse()?,
                RewriteSource::Direct,
                vec![object_restriction("user", None)?],
            ),
            RelationSource::new(
                "banned".parse()?,
                RewriteSource::Direct,
                vec![object_restriction("user", None)?],
            ),
            RelationSource::new(
                "conditional".parse()?,
                RewriteSource::Direct,
                vec![object_restriction("user", Some("under_limit"))?],
            ),
            RelationSource::new(
                "boundary".parse()?,
                RewriteSource::Direct,
                vec![object_restriction("user", Some("at_precision_boundary"))?],
            ),
            RelationSource::new(
                "parent".parse()?,
                RewriteSource::Direct,
                vec![object_restriction("folder", None)?],
            ),
            RelationSource::new(
                "viewer".parse()?,
                RewriteSource::Union(vec![
                    RewriteSource::Direct,
                    computed_rewrite("owner")?,
                    RewriteSource::TupleToUserset {
                        tupleset: "parent".parse()?,
                        computed: "viewer".parse()?,
                    },
                ]),
                vec![
                    object_restriction("user", None)?,
                    wildcard_restriction("user")?,
                    userset_restriction("group", "member")?,
                ],
            ),
            RelationSource::new(
                "guarded".parse()?,
                RewriteSource::Union(vec![
                    computed_rewrite("conditional")?,
                    computed_rewrite("owner")?,
                ]),
                Vec::new(),
            ),
            RelationSource::new(
                "both".parse()?,
                RewriteSource::Intersection(vec![
                    computed_rewrite("owner")?,
                    computed_rewrite("editor")?,
                ]),
                Vec::new(),
            ),
            RelationSource::new(
                "allowed".parse()?,
                RewriteSource::Difference {
                    base: Box::new(computed_rewrite("viewer")?),
                    subtract: Box::new(computed_rewrite("banned")?),
                },
                Vec::new(),
            ),
        ],
    ))
}

fn fixture_tuples() -> Result<Vec<RelationshipTuple>> {
    let unconditional = [
        "document:direct#viewer@user:anne",
        "document:wild#viewer@user:*",
        "document:userset#viewer@group:eng#member",
        "group:eng#member@user:bob",
        "document:computed#owner@user:anne",
        "document:ttu#parent@folder:roadmap",
        "folder:roadmap#viewer@user:anne",
        "document:both#owner@user:anne",
        "document:both#editor@user:anne",
        "document:both-deny#owner@user:anne",
        "document:included#viewer@user:anne",
        "document:excluded#viewer@user:anne",
        "document:excluded#banned@user:anne",
        "document:cycle-userset-allow#viewer@group:cycle-allow-a#member",
        "group:cycle-allow-a#member@group:cycle-allow-b#member",
        "group:cycle-allow-b#member@group:cycle-allow-a#member",
        "group:cycle-allow-b#member@user:anne",
        "document:cycle-userset-deny#viewer@group:cycle-deny-a#member",
        "group:cycle-deny-a#member@group:cycle-deny-b#member",
        "group:cycle-deny-b#member@group:cycle-deny-a#member",
    ]
    .into_iter()
    .map(|tuple| {
        tuple
            .parse::<TupleKey>()
            .map(RelationshipTuple::unconditional)
            .map_err(Into::into)
    })
    .collect::<Result<Vec<_>>>()?;
    let mut tuples = Vec::with_capacity(unconditional.len() + 3);
    tuples.extend(unconditional);
    for (tuple, x) in [
        ("document:condition#conditional@user:anne", 50),
        ("document:condition-deny#conditional@user:anne", 150),
    ] {
        tuples.push(RelationshipTuple::new(
            tuple.parse()?,
            ConditionReference::Conditional(ConditionBinding::new(
                "under_limit".parse()?,
                ConditionContext::try_from_json(json!({"x": x}), &InputLimits::default())?,
            )),
        ));
    }
    tuples.push(RelationshipTuple::new(
        "document:precision#boundary@user:anne".parse()?,
        ConditionReference::Conditional(ConditionBinding::new(
            "at_precision_boundary".parse()?,
            ConditionContext::try_from_json(
                json!({"x": 9_223_372_036_854_775_808.0}),
                &InputLimits::default(),
            )?,
        )),
    ));
    Ok(tuples)
}

#[derive(Clone, Copy, Debug)]
struct DifferentialCase {
    name: &'static str,
    object: &'static str,
    relation: &'static str,
    user: &'static str,
    contextual_tuple: Option<(&'static str, &'static str, &'static str)>,
    context_x: Option<i64>,
    consistency: &'static str,
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
            context: self.context_x.map(|x| json!({"x": x})),
            authorization_model_id: model_id.to_owned(),
            consistency: self.consistency.to_owned(),
            trace: false,
        }
    }
}

const DIFFERENTIAL_CASES: [DifferentialCase; 17] = [
    DifferentialCase {
        name: "direct_allow",
        object: "document:direct",
        relation: "viewer",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "direct_deny",
        object: "document:direct",
        relation: "viewer",
        user: "user:bob",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "typed_wildcard_allow",
        object: "document:wild",
        relation: "viewer",
        user: "user:carol",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "userset_allow",
        object: "document:userset",
        relation: "viewer",
        user: "user:bob",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "computed_userset_allow",
        object: "document:computed",
        relation: "viewer",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "tuple_to_userset_allow",
        object: "document:ttu",
        relation: "viewer",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "intersection_allow",
        object: "document:both",
        relation: "both",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "intersection_deny",
        object: "document:both-deny",
        relation: "both",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "difference_allow",
        object: "document:included",
        relation: "allowed",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "difference_deny",
        object: "document:excluded",
        relation: "allowed",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "condition_tuple_context_allow",
        object: "document:condition",
        relation: "guarded",
        user: "user:anne",
        contextual_tuple: None,
        context_x: Some(200),
        consistency: "",
    },
    DifferentialCase {
        name: "condition_deny",
        object: "document:condition-deny",
        relation: "guarded",
        user: "user:anne",
        contextual_tuple: None,
        context_x: Some(50),
        consistency: "",
    },
    DifferentialCase {
        name: "condition_dynamic_numeric_boundary_allow",
        object: "document:precision",
        relation: "boundary",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "userset_cycle_with_direct_allow",
        object: "document:cycle-userset-allow",
        relation: "viewer",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "userset_cycle_deny",
        object: "document:cycle-userset-deny",
        relation: "viewer",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
    },
    DifferentialCase {
        name: "contextual_tuple_allow",
        object: "document:contextual",
        relation: "viewer",
        user: "user:carol",
        contextual_tuple: Some(("document:contextual", "viewer", "user:carol")),
        context_x: None,
        consistency: "HIGHER_CONSISTENCY",
    },
    DifferentialCase {
        name: "invalid_object_error",
        object: "",
        relation: "viewer",
        user: "user:anne",
        contextual_tuple: None,
        context_x: None,
        consistency: "",
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalizedBatchItem {
    allowed: Option<bool>,
    error_class: Option<&'static str>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalizedBatch {
    http_status: u16,
    results: BTreeMap<String, NormalizedBatchItem>,
    error_class: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchCaseReport {
    name: &'static str,
    go: NormalizedBatch,
    rust: NormalizedBatch,
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
    batch_case: BatchCaseReport,
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
    let (go_store, go_model) = configure_differential_server(&client, &go_url).await?;
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
    let go_batch = observe_batch(
        &client,
        &go_url,
        &go_store,
        &batch_differential_request(&go_model),
    )
    .await
    .context("failed to run Go BatchCheck case")?;
    let rust_batch = observe_batch(
        &client,
        &rust_url,
        STORE_ID,
        &batch_differential_request(MODEL_ID),
    )
    .await
    .context("failed to run Rust BatchCheck case")?;
    compare_batch(&go_batch, &rust_batch, &mut mismatches)?;
    let report = DifferentialCheckReport {
        baseline_commit: GO_BASELINE_COMMIT,
        corpus_source: "vendors/openfga/tests/check",
        cases: reports,
        batch_case: BatchCaseReport {
            name: "correlated_mixed_batch",
            go: go_batch,
            rust: rust_batch,
        },
        mismatches,
    };
    write_report(&report)?;
    if !report.mismatches.is_empty() {
        bail!("Check differential found normalized mismatches");
    }
    Ok(())
}

fn batch_differential_request(model_id: &str) -> WireBatchCheckRequest {
    let item = |correlation_id: &str,
                object: &str,
                relation: &str,
                user: &str,
                contextual_tuples: Option<WireContextualTuples>,
                context: Option<Value>| {
        WireBatchCheckItem {
            tuple_key: WireTupleKey {
                object: object.to_owned(),
                relation: relation.to_owned(),
                user: user.to_owned(),
            },
            contextual_tuples,
            context,
            correlation_id: correlation_id.to_owned(),
        }
    };
    WireBatchCheckRequest {
        checks: vec![
            item(
                "allow",
                "document:direct",
                "viewer",
                "user:anne",
                None,
                None,
            ),
            item("deny", "document:direct", "viewer", "user:bob", None, None),
            item(
                "condition",
                "document:condition",
                "guarded",
                "user:anne",
                None,
                Some(json!({"x": 200})),
            ),
            item(
                "contextual",
                "document:batch-contextual",
                "viewer",
                "user:carol",
                Some(WireContextualTuples {
                    tuple_keys: vec![WireTupleKey {
                        object: "document:batch-contextual".to_owned(),
                        relation: "viewer".to_owned(),
                        user: "user:carol".to_owned(),
                    }],
                }),
                None,
            ),
            item(
                "item-error",
                "document:direct",
                "missing",
                "user:anne",
                None,
                None,
            ),
        ],
        authorization_model_id: model_id.to_owned(),
        consistency: "HIGHER_CONSISTENCY".to_owned(),
    }
}

fn differential_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(REQUEST_TIMEOUT)
        .redirect(Policy::none())
        .build()
        .context("failed to build the Check differential client")
}

pub(crate) async fn configure_differential_server(
    client: &Client,
    base_url: &Url,
) -> Result<(String, String)> {
    let store_url = base_url
        .join("stores")
        .context("failed to build store URL")?;
    let response = client
        .post(store_url)
        .json(&json!({"name": "phase1-check-differential"}))
        .send()
        .await
        .context("differential store creation request failed")?;
    let store =
        read_success_json::<CreateStoreResponse>(response, "differential store creation").await?;
    let store_id = store
        .id
        .parse::<StoreId>()
        .context("differential store creation returned an invalid store ID")?;
    let model_url = base_url
        .join(&format!("stores/{store_id}/authorization-models"))
        .context("failed to build model URL")?;
    let response = client
        .post(model_url)
        .json(&go_model_document())
        .send()
        .await
        .context("differential model write request failed")?;
    let model =
        read_success_json::<WriteModelResponse>(response, "differential model write").await?;
    let model_id = model
        .authorization_model_id
        .parse::<AuthorizationModelId>()
        .context("differential model write returned an invalid model ID")?;
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
        .context("differential tuple write request failed")?;
    require_success(response, "differential tuple write").await?;
    Ok((store_id.to_string(), model_id.to_string()))
}

pub(crate) fn go_model_document() -> Value {
    json!({
        "schema_version": "1.1",
        "type_definitions": [
            {"type": "user"},
            {
                "type": "group",
                "relations": {"member": {"this": {}}},
                "metadata": {"relations": {"member": {
                    "directly_related_user_types": [
                        {"type": "user"},
                        {"type": "group", "relation": "member"}
                    ]
                }}}
            },
            {
                "type": "folder",
                "relations": {"viewer": {"this": {}}},
                "metadata": {"relations": {"viewer": {
                    "directly_related_user_types": [
                        {"type": "user"},
                        {"type": "user", "wildcard": {}}
                    ]
                }}}
            },
            {
                "type": "document",
                "relations": {
                    "owner": {"this": {}},
                    "editor": {"this": {}},
                    "banned": {"this": {}},
                    "conditional": {"this": {}},
                    "boundary": {"this": {}},
                    "parent": {"this": {}},
                    "viewer": {"union": {"child": [
                        {"this": {}},
                        {"computedUserset": {"relation": "owner"}},
                        {"tupleToUserset": {
                            "tupleset": {"relation": "parent"},
                            "computedUserset": {"relation": "viewer"}
                        }}
                    ]}},
                    "guarded": {"union": {"child": [
                        {"computedUserset": {"relation": "conditional"}},
                        {"computedUserset": {"relation": "owner"}}
                    ]}},
                    "both": {"intersection": {"child": [
                        {"computedUserset": {"relation": "owner"}},
                        {"computedUserset": {"relation": "editor"}}
                    ]}},
                    "allowed": {"difference": {
                        "base": {"computedUserset": {"relation": "viewer"}},
                        "subtract": {"computedUserset": {"relation": "banned"}}
                    }}
                },
                "metadata": {"relations": {
                    "owner": {"directly_related_user_types": [{"type": "user"}]},
                    "editor": {"directly_related_user_types": [{"type": "user"}]},
                    "banned": {"directly_related_user_types": [{"type": "user"}]},
                    "conditional": {"directly_related_user_types": [
                        {"type": "user", "condition": "under_limit"}
                    ]},
                    "boundary": {"directly_related_user_types": [
                        {"type": "user", "condition": "at_precision_boundary"}
                    ]},
                    "parent": {"directly_related_user_types": [{"type": "folder"}]},
                    "viewer": {"directly_related_user_types": [
                        {"type": "user"},
                        {"type": "user", "wildcard": {}},
                        {"type": "group", "relation": "member"}
                    ]},
                    "guarded": {"directly_related_user_types": []},
                    "both": {"directly_related_user_types": []},
                    "allowed": {"directly_related_user_types": []}
                }}
            }
        ],
        "conditions": {
            "under_limit": {
                "name": "under_limit",
                "expression": "x < 100 && x == 50",
                "parameters": {"x": {"type_name": "TYPE_NAME_ANY"}}
            },
            "at_precision_boundary": {
                "name": "at_precision_boundary",
                "expression": "x == 9223372036854775807",
                "parameters": {"x": {"type_name": "TYPE_NAME_ANY"}}
            }
        }
    })
}

pub(crate) fn go_fixture_tuples() -> Vec<Value> {
    vec![
        json!({"object": "document:direct", "relation": "viewer", "user": "user:anne"}),
        json!({"object": "document:wild", "relation": "viewer", "user": "user:*"}),
        json!({"object": "document:wild-plus", "relation": "viewer", "user": "user:*"}),
        json!({"object": "document:wild-plus", "relation": "viewer", "user": "user:will"}),
        json!({"object": "document:userset", "relation": "viewer", "user": "group:eng#member"}),
        json!({"object": "group:eng", "relation": "member", "user": "user:bob"}),
        json!({"object": "document:computed", "relation": "owner", "user": "user:anne"}),
        json!({"object": "document:ttu", "relation": "parent", "user": "folder:roadmap"}),
        json!({"object": "folder:roadmap", "relation": "viewer", "user": "user:anne"}),
        json!({"object": "document:both", "relation": "owner", "user": "user:anne"}),
        json!({"object": "document:both", "relation": "editor", "user": "user:anne"}),
        json!({"object": "document:both-deny", "relation": "owner", "user": "user:anne"}),
        json!({"object": "document:included", "relation": "viewer", "user": "user:anne"}),
        json!({"object": "document:excluded", "relation": "viewer", "user": "user:anne"}),
        json!({"object": "document:excluded", "relation": "banned", "user": "user:anne"}),
        json!({"object": "document:cycle-userset-allow", "relation": "viewer", "user": "group:cycle-allow-a#member"}),
        json!({"object": "group:cycle-allow-a", "relation": "member", "user": "group:cycle-allow-b#member"}),
        json!({"object": "group:cycle-allow-b", "relation": "member", "user": "group:cycle-allow-a#member"}),
        json!({"object": "group:cycle-allow-b", "relation": "member", "user": "user:anne"}),
        json!({"object": "document:cycle-userset-deny", "relation": "viewer", "user": "group:cycle-deny-a#member"}),
        json!({"object": "group:cycle-deny-a", "relation": "member", "user": "group:cycle-deny-b#member"}),
        json!({"object": "group:cycle-deny-b", "relation": "member", "user": "group:cycle-deny-a#member"}),
        json!({
            "object": "document:condition",
            "relation": "conditional",
            "user": "user:anne",
            "condition": {"name": "under_limit", "context": {"x": 50}}
        }),
        json!({
            "object": "document:condition-deny",
            "relation": "conditional",
            "user": "user:anne",
            "condition": {"name": "under_limit", "context": {"x": 150}}
        }),
        json!({
            "object": "document:precision",
            "relation": "boundary",
            "user": "user:anne",
            "condition": {
                "name": "at_precision_boundary",
                "context": {"x": 9_223_372_036_854_775_808.0}
            }
        }),
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

async fn observe_batch(
    client: &Client,
    base_url: &Url,
    store_id: &str,
    request: &WireBatchCheckRequest,
) -> Result<NormalizedBatch> {
    let batch_url = base_url
        .join(&format!("stores/{store_id}/batch-check"))
        .context("failed to build BatchCheck URL")?;
    let response = client
        .post(batch_url)
        .json(request)
        .send()
        .await
        .context("BatchCheck request failed")?;
    let status = response.status();
    let body = read_bounded(response).await?;
    if !status.is_success() {
        return Ok(NormalizedBatch {
            http_status: status.as_u16(),
            results: BTreeMap::new(),
            error_class: Some(classify_error(status)),
        });
    }
    let body: Value =
        serde_json::from_slice(&body).context("successful BatchCheck body is not valid JSON")?;
    let result = body
        .get("result")
        .and_then(Value::as_object)
        .context("successful BatchCheck body has no result object")?;
    let results = result
        .iter()
        .map(|(correlation_id, item)| {
            let allowed = item.get("allowed").and_then(Value::as_bool);
            let error_class = item
                .get("error")
                .map(classify_batch_item_error)
                .transpose()?;
            if allowed.is_none() == error_class.is_none() {
                bail!("BatchCheck item has an invalid result union");
            }
            Ok((
                correlation_id.clone(),
                NormalizedBatchItem {
                    allowed,
                    error_class,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(NormalizedBatch {
        http_status: status.as_u16(),
        results,
        error_class: None,
    })
}

fn classify_batch_item_error(error: &Value) -> Result<&'static str> {
    let field = |snake_case: &str, camel_case: &str| {
        error
            .get(snake_case)
            .or_else(|| error.get(camel_case))
            .and_then(Value::as_str)
    };
    if let Some(code) = field("input_error", "inputError") {
        return match code {
            "authorization_model_resolution_too_complex" => Ok("resource_exhausted"),
            "validation_error" | "invalid_tuple" => Ok("validation"),
            _ => bail!("BatchCheck item returned an unknown input-error category"),
        };
    }
    if let Some(code) = field("internal_error", "internalError") {
        return match code {
            "deadline_exceeded" => Ok("timeout"),
            "resource_exhausted" => Ok("resource_exhausted"),
            "unavailable" => Ok("storage"),
            "internal_error"
            | "already_exists"
            | "failed_precondition"
            | "aborted"
            | "out_of_range"
            | "data_loss" => Ok("internal"),
            _ => bail!("BatchCheck item returned an unknown internal-error category"),
        };
    }
    match error.get("code").and_then(Value::as_str) {
        Some("validation_error") => Ok("validation"),
        Some("request_timeout") => Ok("timeout"),
        Some("resource_exhausted") => Ok("resource_exhausted"),
        Some("service_unavailable") => Ok("storage"),
        Some("internal_error") => Ok("internal"),
        _ => bail!("BatchCheck item returned no recognized error category"),
    }
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

fn compare_batch(
    go: &NormalizedBatch,
    rust: &NormalizedBatch,
    mismatches: &mut Vec<CheckMismatch>,
) -> Result<()> {
    if go.http_status != rust.http_status {
        mismatches.push(CheckMismatch {
            case: "correlated_mixed_batch",
            field: "httpStatus",
            go: go.http_status.to_string(),
            rust: rust.http_status.to_string(),
        });
    }
    if go.results != rust.results {
        mismatches.push(CheckMismatch {
            case: "correlated_mixed_batch",
            field: "results",
            go: serde_json::to_string(&go.results).context("failed to normalize Go batch")?,
            rust: serde_json::to_string(&rust.results).context("failed to normalize Rust batch")?,
        });
    }
    if go.error_class != rust.error_class {
        mismatches.push(CheckMismatch {
            case: "correlated_mixed_batch",
            field: "errorClass",
            go: go.error_class.unwrap_or("none").to_owned(),
            rust: rust.error_class.unwrap_or("none").to_owned(),
        });
    }
    Ok(())
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
    use serde_json::json;

    use super::{
        CheckMismatch, NormalizedCheck, WireCheckRequest, classify_batch_item_error, compare_case,
        convert_request,
    };

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

    #[test]
    fn test_should_classify_explicit_batch_error_categories() {
        let cases = [
            (json!({"input_error": "validation_error"}), "validation"),
            (
                json!({"inputError": "authorization_model_resolution_too_complex"}),
                "resource_exhausted",
            ),
            (json!({"internal_error": "deadline_exceeded"}), "timeout"),
            (json!({"internalError": "unavailable"}), "storage"),
            (json!({"code": "internal_error"}), "internal"),
        ];
        for (wire_error, expected) in cases {
            assert_eq!(classify_batch_item_error(&wire_error).ok(), Some(expected));
        }
        assert!(classify_batch_item_error(&json!({"input_error": "future_code"})).is_err());
        assert!(classify_batch_item_error(&json!({})).is_err());
    }
}
