use std::{
    collections::{BTreeSet, HashMap},
    error::Error,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use openfga_auth::{
    Action, AuthenticationService, AuthorizationPolicy, PolicyBinding, PresharedKey, StoreScope,
};
use openfga_check::CheckBudget;
use openfga_domain::{
    AuthorizationModelId, FingerprintBuilder, InputLimits, Principal, PrincipalId, PrincipalKind,
    RequestTimeout, StoreId, TokenCodec, TokenKey, TokenKeyId, TokenOperation,
};
use openfga_list::ListObjectsBudget;
use openfga_model::ModelCompiler;
use openfga_proto::openfga::{
    v1 as pb,
    v1::{open_fga_service_client::OpenFgaServiceClient, open_fga_service_server::OpenFgaService},
};
use openfga_service::{
    AssertionService, ChangeService, CheckService, IdentifierSource, IdentifierSourceError,
    ListObjectsService, ModelPublication, ModelService, ServiceClock, ServiceError, StoreService,
    TupleService,
};
use openfga_storage::{
    AssertionReader, AssertionWriter, ChangeReader, ModelReader, ModelWriter, OperationContext,
    StorageCursor, StorageError, StorageErrorKind, StoreReader, StoreWriter, TupleReader,
    TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use prost::Message;
use secrecy::SecretString;
use tokio::sync::oneshot;
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::transport::Server;
use tower::ServiceExt;

use crate::{AdmissionPolicy, ApiError, OpenFgaApi, OpenFgaServices, TransportConfig};

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

#[derive(Debug)]
struct FixedIdentifiers {
    store_id: StoreId,
    model_id: AuthorizationModelId,
}

#[async_trait]
impl IdentifierSource for FixedIdentifiers {
    async fn next_store_id(
        &self,
        _context: &OperationContext,
    ) -> Result<StoreId, IdentifierSourceError> {
        Ok(self.store_id)
    }

    async fn next_model_id(
        &self,
        _context: &OperationContext,
    ) -> Result<AuthorizationModelId, IdentifierSourceError> {
        Ok(self.model_id)
    }
}

#[derive(Debug)]
struct FixedClock;

impl ServiceClock for FixedClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }
}

#[derive(Debug)]
struct TestRuntime {
    storage: Arc<MemoryStorage>,
    api: OpenFgaApi,
    principal: Principal,
    authentication: AuthenticationService,
}

fn test_runtime(maximum_message_bytes: usize) -> Result<TestRuntime, Box<dyn Error>> {
    let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
    let stores: Arc<dyn StoreReader> = storage.clone();
    let store_writes: Arc<dyn StoreWriter> = storage.clone();
    let models: Arc<dyn ModelReader> = storage.clone();
    let model_writes: Arc<dyn ModelWriter> = storage.clone();
    let tuples: Arc<dyn TupleReader> = storage.clone();
    let tuple_writes: Arc<dyn TupleWriter> = storage.clone();
    let assertion_reads: Arc<dyn AssertionReader> = storage.clone();
    let assertion_writes: Arc<dyn AssertionWriter> = storage.clone();
    let changes: Arc<dyn ChangeReader> = storage.clone();
    let identifiers: Arc<dyn IdentifierSource> = Arc::new(FixedIdentifiers {
        store_id: STORE_ID.parse()?,
        model_id: MODEL_ID.parse()?,
    });
    let limits = InputLimits::default();
    let principal_id = "transport-test".parse::<PrincipalId>()?;
    let principal = Principal::new(PrincipalKind::Development, principal_id.clone());
    let authentication = AuthenticationService::development(principal_id.clone());
    let api = OpenFgaApi::new(
        OpenFgaServices::builder()
            .stores(StoreService::new(
                stores.clone(),
                store_writes,
                identifiers.clone(),
            ))
            .models(ModelService::new(
                stores.clone(),
                models.clone(),
                model_writes,
                ModelPublication::new(identifiers, Arc::new(FixedClock), ModelCompiler::default()),
            ))
            .assertions(AssertionService::new(
                stores.clone(),
                models.clone(),
                assertion_reads,
                assertion_writes,
                limits.clone(),
            ))
            .tuples(TupleService::new(
                stores.clone(),
                models.clone(),
                tuples.clone(),
                tuple_writes,
                limits.clone(),
            ))
            .changes(ChangeService::new(stores, changes))
            .checks(CheckService::direct(
                Arc::clone(&models),
                Arc::clone(&tuples),
                CheckBudget::default(),
            ))
            .list_objects(ListObjectsService::direct(
                models,
                tuples,
                ListObjectsBudget::default(),
                limits.clone(),
            ))
            .build(),
        TransportConfig::builder()
            .limits(limits)
            .authorization_policy(Arc::new(AuthorizationPolicy::development(principal_id)))
            .token_codec(Arc::new(TokenCodec::new(
                TokenKey::new("active".parse::<TokenKeyId>()?, vec![7; 32])?,
                Vec::new(),
                &InputLimits::default(),
            )?))
            .request_timeout(RequestTimeout::new(Duration::from_secs(5))?)
            .maximum_message_bytes(maximum_message_bytes)
            .build(),
    )?;
    Ok(TestRuntime {
        storage,
        api,
        principal,
        authentication,
    })
}

#[test]
fn test_should_match_protocol_json_golden_and_ignore_unknown_fields() -> Result<(), Box<dyn Error>>
{
    let request = pb::CheckRequest {
        store_id: STORE_ID.to_owned(),
        tuple_key: Some(pb::CheckRequestTupleKey {
            user: "user:anne".to_owned(),
            relation: "viewer".to_owned(),
            object: "document:roadmap".to_owned(),
        }),
        contextual_tuples: None,
        authorization_model_id: MODEL_ID.to_owned(),
        trace: false,
        context: None,
        consistency: pb::ConsistencyPreference::HigherConsistency as i32,
    };
    let encoded = serde_json::to_string(&request)?;
    assert_eq!(
        encoded,
        format!(
            "{{\"store_id\":\"{STORE_ID}\",\"tuple_key\":{{\"user\":\"user:anne\",\"relation\":\"\
             viewer\",\"object\":\"document:roadmap\"}},\"authorization_model_id\":\"{MODEL_ID}\",\
             \"consistency\":\"HIGHER_CONSISTENCY\"}}"
        ),
    );
    assert_eq!(
        request.encode_to_vec(),
        b"\x0a\x1a01ARZ3NDEKTSV4RRFFQ69G5FAV\x12\x25\x0a\x09user:anne\x12\x06viewer\x1a\x10document:roadmap\x22\x1a01ARZ3NDEKTSV4RRFFQ69G5FAW\x38\xc8\x01"
            .to_vec(),
    );
    assert_eq!(
        serde_json::from_str::<pb::CheckRequest>("{\"unknown\":true}")?,
        pb::CheckRequest::default(),
    );
    Ok(())
}

#[test]
fn test_should_map_errors_without_exposing_internal_diagnostics() {
    let error = ApiError::internal();
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "an internal error occurred");

    for (kind, http, grpc) in [
        (
            StorageErrorKind::AlreadyExists,
            StatusCode::INTERNAL_SERVER_ERROR,
            tonic::Code::AlreadyExists,
        ),
        (
            StorageErrorKind::Conflict,
            StatusCode::CONFLICT,
            tonic::Code::Aborted,
        ),
        (
            StorageErrorKind::InvalidContinuation,
            StatusCode::BAD_REQUEST,
            tonic::Code::Unknown,
        ),
        (
            StorageErrorKind::ResourceExhausted,
            StatusCode::INTERNAL_SERVER_ERROR,
            tonic::Code::ResourceExhausted,
        ),
        (
            StorageErrorKind::Unavailable,
            StatusCode::INTERNAL_SERVER_ERROR,
            tonic::Code::Unavailable,
        ),
        (
            StorageErrorKind::Timeout,
            StatusCode::INTERNAL_SERVER_ERROR,
            tonic::Code::DeadlineExceeded,
        ),
    ] {
        let error = ApiError::from(ServiceError::from(StorageError::new(kind, "secret")));
        assert_eq!(error.http_status(), http);
        assert_eq!(tonic::Status::from(error).code(), grpc);
    }
}

#[test]
fn test_should_bind_continuation_tokens_to_the_exact_query_scope() -> Result<(), Box<dyn Error>> {
    let limits = InputLimits::default();
    let codec = TokenCodec::new(
        TokenKey::new("active".parse()?, vec![9; 32])?,
        Vec::new(),
        &limits,
    )?;
    let store_id = STORE_ID.parse()?;
    let scope = crate::pagination::scope(
        TokenOperation::ReadTuples,
        store_id,
        FingerprintBuilder::new("filter-a").finish(),
    );
    let cursor = StorageCursor::new(b"cursor-1".to_vec())?;
    let token = crate::pagination::continuation_token(Some(&cursor), &scope, &codec, 60, &limits)?;
    let options = crate::pagination::page_options(
        Some(10),
        &token,
        &scope,
        &codec,
        &limits,
        NonZeroU32::new(50).ok_or("invalid test page size")?,
    )?;
    assert_eq!(
        options.after().map(StorageCursor::as_bytes),
        Some(&b"cursor-1"[..])
    );

    let replay_scope = crate::pagination::scope(
        TokenOperation::ReadTuples,
        store_id,
        FingerprintBuilder::new("filter-b").finish(),
    );
    let replay = crate::pagination::page_options(
        Some(10),
        &token,
        &replay_scope,
        &codec,
        &limits,
        NonZeroU32::new(50).ok_or("invalid test page size")?,
    );
    assert!(matches!(replay, Err(error) if error.code() == "invalid_continuation_token"));
    Ok(())
}

#[test]
fn test_should_enforce_pinned_page_size_range() -> Result<(), Box<dyn Error>> {
    let limits = InputLimits::default();
    let codec = TokenCodec::new(
        TokenKey::new("active".parse()?, vec![9; 32])?,
        Vec::new(),
        &limits,
    )?;
    let scope = crate::pagination::scope(
        TokenOperation::ListStores,
        crate::pagination::GLOBAL_SCOPE_STORE,
        FingerprintBuilder::new("all-stores").finish(),
    );
    let default_size = NonZeroU32::new(50).ok_or("invalid default page size")?;

    for invalid in [-1, 0, 101] {
        let error = crate::pagination::page_options(
            Some(invalid),
            "",
            &scope,
            &codec,
            &limits,
            default_size,
        )
        .err()
        .ok_or("invalid page size unexpectedly accepted")?;
        assert_eq!(error.code(), "page_size_invalid");
    }
    let maximum =
        crate::pagination::page_options(Some(100), "", &scope, &codec, &limits, default_size)?;
    assert_eq!(maximum.maximum_results(), 100);
    Ok(())
}

#[test]
fn test_should_preserve_field_specific_tuple_validation_reasons() {
    let limits = InputLimits::default();
    let oversized_object = format!("document:{}", "x".repeat(257));
    let object_error =
        super::convert::tuple_key(&oversized_object, "viewer", "user:anne", &limits).err();
    assert!(matches!(object_error, Some(error) if error.code() == "object_too_long"));

    let oversized_relation = "r".repeat(51);
    let relation_error = super::convert::tuple_key(
        "document:roadmap",
        &oversized_relation,
        "user:anne",
        &limits,
    )
    .err();
    assert!(matches!(relation_error, Some(error) if error.code() == "relation_too_long"));

    let user_error =
        super::convert::tuple_key("document:roadmap", "viewer", "invalid-user", &limits).err();
    assert!(matches!(user_error, Some(error) if error.code() == "validation_error"));
}

#[test]
fn test_should_cancel_in_flight_work_when_request_guard_drops() {
    let guard = super::api::RequestCancellation::new();
    let token = guard.token();
    drop(guard);
    assert!(token.is_cancelled());
}

#[tokio::test]
async fn test_should_match_pinned_model_and_write_error_semantics() -> Result<(), Box<dyn Error>> {
    let TestRuntime {
        storage,
        api,
        principal,
        authentication,
    } = test_runtime(300_000)?;

    let mut expired = authenticated_request(
        &principal,
        pb::WriteAuthorizationModelRequest {
            store_id: STORE_ID.to_owned(),
            schema_version: String::new(),
            type_definitions: Vec::new(),
            conditions: HashMap::new(),
        },
    );
    expired.set_timeout(Duration::ZERO);
    let deadline = OpenFgaService::write_authorization_model(&api, expired)
        .await
        .err()
        .ok_or("expired invalid gRPC request unexpectedly succeeded")?;
    assert_eq!(deadline.code(), tonic::Code::DeadlineExceeded);

    assert_missing_model_precedence(&api, &principal).await?;
    assert_invalid_model_diagnostics(&api, &principal).await?;
    assert_model_size_limit(&api, &principal).await?;

    api.write_authorization_model(&principal, conditioned_model_request())
        .await?;
    assert_tuple_context_and_conflicts(&api, &principal).await?;
    assert_assertion_size_limit(&api, &principal).await?;

    drop(authentication);
    drop(api);
    let mut storage = Arc::try_unwrap(storage).map_err(|_| "storage references remain")?;
    storage.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_should_authorize_before_admission_and_validation_on_both_transports()
-> Result<(), Box<dyn Error>> {
    let TestRuntime {
        storage,
        mut api,
        principal,
        authentication,
    } = test_runtime(1_024)?;
    api.config.authorization_policy = Arc::new(AuthorizationPolicy::new(vec![PolicyBinding::new(
        principal.id().clone(),
        BTreeSet::from([Action::WriteAuthorizationModel]),
        StoreScope::Stores(BTreeSet::from([MODEL_ID.parse()?])),
    )]));
    api.admission = crate::admission::AdmissionControl::new(
        AdmissionPolicy::builder()
            .administration(NonZeroU32::MIN)
            .reads(NonZeroU32::MIN)
            .writes(NonZeroU32::MIN)
            .checks(NonZeroU32::MIN)
            .enumeration(NonZeroU32::MIN)
            .build(),
    )?;

    let router = crate::http_router(api.clone(), authentication.clone());
    let mut saturated = Vec::with_capacity(api.config.maximum_concurrency);
    for _ in 0..api.config.maximum_concurrency {
        saturated.push(api.acquire_endpoint_permit()?);
    }
    for _ in 0..2 {
        let response = router
            .clone()
            .oneshot(
                Request::post(format!("/stores/{STORE_ID}/authorization-models"))
                    .header("content-type", "application/json")
                    .body(Body::from("not-json"))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    let malformed_store = router
        .oneshot(
            Request::post("/stores/short/authorization-models")
                .header("content-type", "application/json")
                .body(Body::from("not-json"))?,
        )
        .await?;
    assert_eq!(malformed_store.status(), StatusCode::FORBIDDEN);

    for _ in 0..2 {
        let error = OpenFgaService::write_authorization_model(
            &api,
            authenticated_request(
                &principal,
                pb::WriteAuthorizationModelRequest {
                    store_id: STORE_ID.to_owned(),
                    schema_version: String::new(),
                    type_definitions: Vec::new(),
                    conditions: HashMap::new(),
                },
            ),
        )
        .await
        .err()
        .ok_or("forbidden gRPC request unexpectedly succeeded")?;
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    drop(saturated);
    drop(authentication);
    drop(api);
    let mut storage = Arc::try_unwrap(storage).map_err(|_| "storage references remain")?;
    storage.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_should_load_shed_only_after_authorization_on_both_transports()
-> Result<(), Box<dyn Error>> {
    let TestRuntime {
        storage,
        api,
        principal,
        authentication,
    } = test_runtime(1_024)?;
    let mut saturated = Vec::with_capacity(api.config.maximum_concurrency);
    for _ in 0..api.config.maximum_concurrency {
        saturated.push(api.acquire_endpoint_permit()?);
    }

    let router = crate::http_router(api.clone(), authentication.clone());
    let response = router
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/authorization-models"))
                .header("content-type", "application/json")
                .body(Body::from("not-json"))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let error = OpenFgaService::write_authorization_model(
        &api,
        authenticated_request(
            &principal,
            pb::WriteAuthorizationModelRequest {
                store_id: STORE_ID.to_owned(),
                schema_version: String::new(),
                type_definitions: Vec::new(),
                conditions: HashMap::new(),
            },
        ),
    )
    .await
    .err()
    .ok_or("saturated authorized gRPC request unexpectedly succeeded")?;
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);

    drop(saturated);
    drop(authentication);
    drop(api);
    let mut storage = Arc::try_unwrap(storage).map_err(|_| "storage references remain")?;
    storage.stop().await?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the cross-operation precedence matrix is kept together so its ordered failures \
              remain auditable"
)]
async fn assert_missing_model_precedence(
    api: &OpenFgaApi,
    principal: &Principal,
) -> Result<(), Box<dyn Error>> {
    let latest = api
        .write(
            principal,
            pb::WriteRequest {
                store_id: STORE_ID.to_owned(),
                writes: Some(pb::WriteRequestWrites {
                    tuple_keys: vec![relationship_tuple()],
                    on_duplicate: String::new(),
                }),
                deletes: None,
                authorization_model_id: String::new(),
            },
        )
        .await
        .err()
        .ok_or("write without a model unexpectedly succeeded")?;
    assert_eq!(latest.code(), "latest_authorization_model_not_found");
    assert_eq!(
        latest.to_string(),
        format!("No authorization models found for store '{STORE_ID}'"),
    );

    let explicit = api
        .write(
            principal,
            pb::WriteRequest {
                store_id: STORE_ID.to_owned(),
                writes: None,
                deletes: Some(pb::WriteRequestDeletes {
                    tuple_keys: vec![pb::TupleKeyWithoutCondition {
                        user: "user:anne".to_owned(),
                        relation: "viewer".to_owned(),
                        object: "document:roadmap".to_owned(),
                    }],
                    on_missing: String::new(),
                }),
                authorization_model_id: MODEL_ID.to_owned(),
            },
        )
        .await
        .err()
        .ok_or("delete without a model unexpectedly succeeded")?;
    assert_eq!(explicit.code(), "authorization_model_not_found");
    assert_eq!(
        explicit.to_string(),
        format!("Authorization Model '{MODEL_ID}' not found"),
    );

    for request in [
        pb::WriteRequest {
            store_id: STORE_ID.to_owned(),
            writes: Some(pb::WriteRequestWrites {
                tuple_keys: vec![pb::TupleKey {
                    user: "bad".to_owned(),
                    relation: "viewer".to_owned(),
                    object: "document:roadmap".to_owned(),
                    condition: None,
                }],
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: MODEL_ID.to_owned(),
        },
        pb::WriteRequest {
            store_id: STORE_ID.to_owned(),
            writes: None,
            deletes: None,
            authorization_model_id: MODEL_ID.to_owned(),
        },
    ] {
        let error = api
            .write(principal, request)
            .await
            .err()
            .ok_or("write with a missing model unexpectedly succeeded")?;
        assert_eq!(error.code(), "authorization_model_not_found");
        assert_eq!(
            error.to_string(),
            format!("Authorization Model '{MODEL_ID}' not found"),
        );
    }
    let invalid_tuple = pb::CheckRequestTupleKey {
        user: "xx".to_owned(),
        relation: "viewer".to_owned(),
        object: "xx".to_owned(),
    };
    let check_error = api
        .check(
            principal,
            pb::CheckRequest {
                store_id: STORE_ID.to_owned(),
                tuple_key: Some(invalid_tuple.clone()),
                contextual_tuples: None,
                authorization_model_id: MODEL_ID.to_owned(),
                trace: false,
                context: None,
                consistency: 0,
            },
        )
        .await
        .err()
        .ok_or("invalid Check with a missing model unexpectedly succeeded")?;
    assert_eq!(check_error.code(), "authorization_model_not_found");
    let batch_error = api
        .batch_check(
            principal,
            pb::BatchCheckRequest {
                store_id: STORE_ID.to_owned(),
                checks: vec![pb::BatchCheckItem {
                    tuple_key: Some(invalid_tuple),
                    contextual_tuples: None,
                    context: None,
                    correlation_id: "missing-model".to_owned(),
                }],
                authorization_model_id: MODEL_ID.to_owned(),
                consistency: 0,
            },
        )
        .await
        .err()
        .ok_or("invalid BatchCheck with a missing model unexpectedly succeeded")?;
    assert_eq!(batch_error.code(), "authorization_model_not_found");
    let assertion_error = api
        .write_assertions(
            principal,
            pb::WriteAssertionsRequest {
                store_id: STORE_ID.to_owned(),
                authorization_model_id: MODEL_ID.to_owned(),
                assertions: vec![pb::Assertion {
                    tuple_key: Some(pb::AssertionTupleKey {
                        user: "xx".to_owned(),
                        relation: "viewer".to_owned(),
                        object: "xx".to_owned(),
                    }),
                    expectation: true,
                    contextual_tuples: Vec::new(),
                    context: None,
                }],
            },
        )
        .await
        .err()
        .ok_or("invalid assertions with a missing model unexpectedly succeeded")?;
    assert_eq!(assertion_error.code(), "authorization_model_not_found");
    Ok(())
}

async fn assert_invalid_model_diagnostics(
    api: &OpenFgaApi,
    principal: &Principal,
) -> Result<(), Box<dyn Error>> {
    for (request, expected) in invalid_model_cases() {
        let error = api
            .write_authorization_model(principal, request)
            .await
            .err()
            .ok_or("invalid model unexpectedly succeeded")?;
        assert_eq!(error.code(), "invalid_authorization_model");
        assert_eq!(error.to_string(), expected);
    }
    Ok(())
}

async fn assert_model_size_limit(
    api: &OpenFgaApi,
    principal: &Principal,
) -> Result<(), Box<dyn Error>> {
    let mut request = model_request();
    let relations = (0..20_000)
        .map(|index| {
            (
                format!("relation{index}"),
                pb::Userset {
                    userset: Some(pb::userset::Userset::This(pb::DirectUserset {})),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    request.type_definitions.push(pb::TypeDefinition {
        r#type: "oversized".to_owned(),
        relations,
        metadata: None,
    });
    let actual = request.encoded_len();
    let error = api
        .write_authorization_model(principal, request)
        .await
        .err()
        .ok_or("oversized model unexpectedly succeeded")?;
    assert_eq!(error.code(), "exceeded_entity_limit");
    assert_eq!(
        error.to_string(),
        format!("model exceeds size limit: {actual} bytes vs 262144 bytes"),
    );

    let mut too_many = model_request();
    too_many.type_definitions = (0..101)
        .map(|index| pb::TypeDefinition {
            r#type: format!("type{index}"),
            relations: HashMap::new(),
            metadata: None,
        })
        .collect();
    let error = api
        .write_authorization_model(principal, too_many)
        .await
        .err()
        .ok_or("authorization model type limit unexpectedly succeeded")?;
    assert_eq!(error.code(), "exceeded_entity_limit");
    assert_eq!(
        error.to_string(),
        "number of type definitions in an authorization model exceeds the allowed limit of 100",
    );
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the pinned tuple compatibility scenario preserves request-ordered state across size \
              and conflict cases"
)]
async fn assert_tuple_context_and_conflicts(
    api: &OpenFgaApi,
    principal: &Principal,
) -> Result<(), Box<dyn Error>> {
    let mut large_check = check_request();
    large_check.context = Some(json_struct(serde_json::json!({
        "x": "a".repeat(40_000),
    }))?);
    api.check(principal, large_check).await?;

    let batch = api
        .batch_check(
            principal,
            pb::BatchCheckRequest {
                store_id: STORE_ID.to_owned(),
                checks: vec![
                    pb::BatchCheckItem {
                        tuple_key: check_request().tuple_key,
                        contextual_tuples: None,
                        context: None,
                        correlation_id: "valid".to_owned(),
                    },
                    pb::BatchCheckItem {
                        tuple_key: Some(pb::CheckRequestTupleKey {
                            user: "xx".to_owned(),
                            relation: "viewer".to_owned(),
                            object: "xx".to_owned(),
                        }),
                        contextual_tuples: None,
                        context: None,
                        correlation_id: "invalid".to_owned(),
                    },
                    pb::BatchCheckItem {
                        tuple_key: check_request().tuple_key,
                        contextual_tuples: Some(pb::ContextualTupleKeys {
                            tuple_keys: vec![pb::TupleKey {
                                user: "user:anne".to_owned(),
                                relation: "viewer".to_owned(),
                                object: "document:contextual".to_owned(),
                                condition: Some(pb::RelationshipCondition {
                                    name: "::".to_owned(),
                                    context: None,
                                }),
                            }],
                        }),
                        context: None,
                        correlation_id: "invalid-condition".to_owned(),
                    },
                ],
                authorization_model_id: MODEL_ID.to_owned(),
                consistency: 0,
            },
        )
        .await?;
    assert!(matches!(
        batch
            .result
            .get("valid")
            .and_then(|result| result.check_result.as_ref()),
        Some(pb::batch_check_single_result::CheckResult::Allowed(_)),
    ));
    let invalid = batch
        .result
        .get("invalid")
        .and_then(|result| result.check_result.as_ref())
        .and_then(|result| match result {
            pb::batch_check_single_result::CheckResult::Error(error) => Some(error),
            pb::batch_check_single_result::CheckResult::Allowed(_) => None,
        })
        .ok_or("invalid BatchCheck item did not return an item-local error")?;
    assert_eq!(
        invalid.message,
        "invalid tuple: the 'user' field is malformed"
    );
    assert!(matches!(
        invalid.code,
        Some(pb::check_error::Code::InputError(code))
            if code == pb::ErrorCode::InvalidTuple as i32
    ));
    let invalid_condition = batch
        .result
        .get("invalid-condition")
        .and_then(|result| result.check_result.as_ref())
        .and_then(|result| match result {
            pb::batch_check_single_result::CheckResult::Error(error) => Some(error),
            pb::batch_check_single_result::CheckResult::Allowed(_) => None,
        })
        .ok_or("invalid contextual condition did not return an item-local error")?;
    assert_eq!(
        invalid_condition.message,
        "invalid tuple: Invalid tuple 'document:contextual#viewer@user:anne (condition ::)'. \
         Reason: undefined condition",
    );
    assert!(matches!(
        invalid_condition.code,
        Some(pb::check_error::Code::InputError(code))
            if code == pb::ErrorCode::InvalidTuple as i32
    ));

    let combined_context = json_struct(serde_json::json!({"x": "a".repeat(32_766)}))?;
    let mut semantically_invalid = relationship_tuple();
    semantically_invalid.condition = Some(pb::RelationshipCondition {
        name: "c2".to_owned(),
        context: Some(combined_context),
    });
    let error = api
        .write(
            principal,
            write_request(semantically_invalid, String::new()),
        )
        .await
        .err()
        .ok_or("invalid oversized condition context unexpectedly succeeded")?;
    assert_eq!(error.code(), "validation_error");
    assert_eq!(
        error.to_string(),
        "Invalid tuple 'document:roadmap#viewer@user:anne (condition c2)'. Reason: parameter type \
         error on condition 'c2' - no parameters defined for the condition",
    );

    let mut unknown_parameter = relationship_tuple();
    unknown_parameter.condition = Some(pb::RelationshipCondition {
        name: "c1".to_owned(),
        context: Some(json_struct(serde_json::json!({"unknownparam": "bad"}))?),
    });
    let error = api
        .write(principal, write_request(unknown_parameter, String::new()))
        .await
        .err()
        .ok_or("unknown condition context parameter unexpectedly succeeded")?;
    assert_eq!(error.code(), "validation_error");
    assert_eq!(
        error.to_string(),
        "Invalid tuple 'document:roadmap#viewer@user:anne (condition c1)'. Reason: found invalid \
         context parameter: unknownparam",
    );

    let mut oversized = relationship_tuple();
    let context = json_struct(serde_json::json!({"x": "a".repeat(32_766)}))?;
    let context_size = context.encoded_len();
    oversized.condition = Some(pb::RelationshipCondition {
        name: "c1".to_owned(),
        context: Some(context),
    });
    let error = api
        .write(principal, write_request(oversized, String::new()))
        .await
        .err()
        .ok_or("oversized tuple condition context unexpectedly succeeded")?;
    assert_eq!(error.code(), "validation_error");
    assert_eq!(
        error.to_string(),
        format!(
            "Invalid tuple 'document:roadmap#viewer@user:anne (condition c1)'. Reason: condition \
             context size limit exceeded: {context_size} bytes exceeds 32768 bytes"
        ),
    );

    api.write(principal, write_request(conditioned_tuple(), String::new()))
        .await?;
    let duplicate = api
        .write(principal, write_request(conditioned_tuple(), String::new()))
        .await
        .err()
        .ok_or("duplicate persisted tuple unexpectedly succeeded")?;
    assert_eq!(duplicate.code(), "write_failed_due_to_invalid_input");
    assert_eq!(
        duplicate.to_string(),
        "cannot write a tuple which already exists: user: 'user:anne', relation: 'viewer', \
         object: 'document:roadmap': tuple to be written already existed or the tuple to be \
         deleted did not exist",
    );

    let missing = api
        .write(principal, delete_request("document:missing", String::new()))
        .await
        .err()
        .ok_or("missing tuple delete unexpectedly succeeded")?;
    assert_eq!(missing.code(), "write_failed_due_to_invalid_input");
    assert_eq!(
        missing.to_string(),
        "cannot delete a tuple which does not exist: user: 'user:anne', relation: 'viewer', \
         object: 'document:missing': tuple to be written already existed or the tuple to be \
         deleted did not exist",
    );

    let strict_delete = api
        .write(
            principal,
            pb::WriteRequest {
                store_id: STORE_ID.to_owned(),
                writes: None,
                deletes: Some(pb::WriteRequestDeletes {
                    tuple_keys: vec![pb::TupleKeyWithoutCondition {
                        user: "user:anne".to_owned(),
                        relation: String::new(),
                        object: "bad".to_owned(),
                    }],
                    on_missing: "ignore".to_owned(),
                }),
                authorization_model_id: MODEL_ID.to_owned(),
            },
        )
        .await
        .err()
        .ok_or("security-normalized malformed delete unexpectedly succeeded")?;
    assert_eq!(strict_delete.code(), "object_invalid_pattern");
    assert_eq!(strict_delete.to_string(), "object has an invalid format");

    let ordered_missing = api
        .write(
            principal,
            pb::WriteRequest {
                store_id: STORE_ID.to_owned(),
                writes: None,
                deletes: Some(pb::WriteRequestDeletes {
                    tuple_keys: vec![delete_tuple("document:z"), delete_tuple("document:a")],
                    on_missing: String::new(),
                }),
                authorization_model_id: MODEL_ID.to_owned(),
            },
        )
        .await
        .err()
        .ok_or("ordered missing deletes unexpectedly succeeded")?;
    assert!(ordered_missing.to_string().contains("object: 'document:z'"));

    let ordered_writes = vec![
        conditioned_tuple_for("document:z"),
        conditioned_tuple_for("document:a"),
    ];
    api.write(
        principal,
        pb::WriteRequest {
            store_id: STORE_ID.to_owned(),
            writes: Some(pb::WriteRequestWrites {
                tuple_keys: ordered_writes.clone(),
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: MODEL_ID.to_owned(),
        },
    )
    .await?;
    let ordered_duplicate = api
        .write(
            principal,
            pb::WriteRequest {
                store_id: STORE_ID.to_owned(),
                writes: Some(pb::WriteRequestWrites {
                    tuple_keys: ordered_writes,
                    on_duplicate: String::new(),
                }),
                deletes: None,
                authorization_model_id: MODEL_ID.to_owned(),
            },
        )
        .await
        .err()
        .ok_or("ordered duplicate writes unexpectedly succeeded")?;
    assert!(
        ordered_duplicate
            .to_string()
            .contains("object: 'document:z'"),
    );

    api.write(
        principal,
        write_request(conditioned_tuple(), "ignore".to_owned()),
    )
    .await?;
    let mut conflicting_condition = conditioned_tuple();
    conflicting_condition.condition = Some(pb::RelationshipCondition {
        name: "c2".to_owned(),
        context: None,
    });
    let conflict = api
        .write(
            principal,
            write_request(conflicting_condition, "ignore".to_owned()),
        )
        .await
        .err()
        .ok_or("ignore accepted a different persisted tuple condition")?;
    assert_eq!(conflict.code(), "Aborted");
    assert_eq!(
        conflict.to_string(),
        "transactional write failed due to conflict",
    );
    let mut conflicting_context = conditioned_tuple();
    conflicting_context.condition = Some(pb::RelationshipCondition {
        name: "c1".to_owned(),
        context: Some(json_struct(serde_json::json!({"x": "different"}))?),
    });
    let conflict = api
        .write(
            principal,
            write_request(conflicting_context, "ignore".to_owned()),
        )
        .await
        .err()
        .ok_or("ignore accepted different persisted tuple condition context")?;
    assert_eq!(conflict.code(), "Aborted");
    assert_eq!(
        conflict.to_string(),
        "transactional write failed due to conflict",
    );
    api.write(
        principal,
        delete_request("document:missing", "ignore".to_owned()),
    )
    .await?;

    let hostile_option = "x".repeat(257);
    let normalized = api
        .write(
            principal,
            write_request(conditioned_tuple(), hostile_option.clone()),
        )
        .await
        .err()
        .ok_or("oversized conflict option unexpectedly succeeded")?;
    assert_eq!(normalized.code(), "validation_error");
    assert_eq!(normalized.to_string(), "the request is invalid");
    assert!(!normalized.to_string().contains(&hostile_option));

    let option_order = api
        .write(
            principal,
            pb::WriteRequest {
                store_id: STORE_ID.to_owned(),
                writes: Some(pb::WriteRequestWrites {
                    tuple_keys: vec![conditioned_tuple()],
                    on_duplicate: "invalid-first".to_owned(),
                }),
                deletes: Some(pb::WriteRequestDeletes {
                    tuple_keys: vec![pb::TupleKeyWithoutCondition {
                        user: "user:anne".to_owned(),
                        relation: "viewer".to_owned(),
                        object: "document:missing".to_owned(),
                    }],
                    on_missing: "invalid-second".to_owned(),
                }),
                authorization_model_id: MODEL_ID.to_owned(),
            },
        )
        .await
        .err()
        .ok_or("invalid conflict options unexpectedly succeeded")?;
    assert_eq!(option_order.code(), "validation_error");
    assert_eq!(
        option_order.to_string(),
        "invalid on_duplicate option: invalid-first",
    );
    Ok(())
}

async fn assert_assertion_size_limit(
    api: &OpenFgaApi,
    principal: &Principal,
) -> Result<(), Box<dyn Error>> {
    let large_context = json_struct(serde_json::json!({"x": "a".repeat(40_000)}))?;
    let accepted = pb::Assertion {
        tuple_key: Some(pb::AssertionTupleKey {
            object: "document:roadmap".to_owned(),
            relation: "viewer".to_owned(),
            user: "user:anne".to_owned(),
        }),
        expectation: true,
        contextual_tuples: Vec::new(),
        context: Some(large_context),
    };
    assert!(accepted.encoded_len() > 32_768);
    assert!(accepted.encoded_len() < 64_000);
    api.write_assertions(
        principal,
        pb::WriteAssertionsRequest {
            store_id: STORE_ID.to_owned(),
            authorization_model_id: MODEL_ID.to_owned(),
            assertions: vec![accepted],
        },
    )
    .await?;

    let mut invalid_contextual_tuple = relationship_tuple();
    invalid_contextual_tuple.condition = Some(pb::RelationshipCondition {
        name: "c2".to_owned(),
        context: Some(json_struct(serde_json::json!({"x": "bad"}))?),
    });
    let error = api
        .write_assertions(
            principal,
            pb::WriteAssertionsRequest {
                store_id: STORE_ID.to_owned(),
                authorization_model_id: MODEL_ID.to_owned(),
                assertions: vec![pb::Assertion {
                    tuple_key: Some(pb::AssertionTupleKey {
                        object: "document:roadmap".to_owned(),
                        relation: "viewer".to_owned(),
                        user: "user:anne".to_owned(),
                    }),
                    expectation: true,
                    contextual_tuples: vec![invalid_contextual_tuple],
                    context: None,
                }],
            },
        )
        .await
        .err()
        .ok_or("invalid zero-parameter assertion context unexpectedly succeeded")?;
    assert_eq!(error.code(), "validation_error");
    assert_eq!(
        error.to_string(),
        "Invalid tuple 'document:roadmap#viewer@user:anne (condition c2)'. Reason: parameter type \
         error on condition 'c2' - no parameters defined for the condition",
    );

    let context = json_struct(serde_json::json!({"x": "a".repeat(1_000)}))?;
    let assertion = pb::Assertion {
        tuple_key: Some(pb::AssertionTupleKey {
            object: "document:roadmap".to_owned(),
            relation: "viewer".to_owned(),
            user: "user:anne".to_owned(),
        }),
        expectation: true,
        contextual_tuples: Vec::new(),
        context: Some(context),
    };
    let assertions = vec![assertion; 70];
    assert!(assertions.iter().map(Message::encoded_len).sum::<usize>() > 64_000,);
    let error = api
        .write_assertions(
            principal,
            pb::WriteAssertionsRequest {
                store_id: STORE_ID.to_owned(),
                authorization_model_id: MODEL_ID.to_owned(),
                assertions,
            },
        )
        .await
        .err()
        .ok_or("oversized assertions unexpectedly succeeded")?;
    assert_eq!(error.code(), "exceeded_entity_limit");
    assert_eq!(
        error.to_string(),
        "The number of bytes exceeds the allowed limit of 64000",
    );
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the end-to-end protocol flow intentionally keeps ordered cross-endpoint state \
              visible"
)]
async fn test_should_execute_implemented_use_cases_through_shared_wire_adapter()
-> Result<(), Box<dyn Error>> {
    let TestRuntime {
        storage,
        api,
        principal,
        authentication,
    } = test_runtime(1_024)?;

    let router = crate::http_router(api.clone(), authentication.clone());
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/stores")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"authorization"}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(response.headers().contains_key("x-request-id"));
    let created = serde_json::from_slice::<pb::CreateStoreResponse>(
        &to_bytes(response.into_body(), 1_024).await?,
    )?;
    assert_eq!(created.id, STORE_ID);
    let grpc_store = OpenFgaService::get_store(
        &api,
        authenticated_request(
            &principal,
            pb::GetStoreRequest {
                store_id: STORE_ID.to_owned(),
            },
        ),
    )
    .await?
    .into_inner();
    assert_eq!(grpc_store.name, "authorization");
    let update = OpenFgaService::update_store(
        &api,
        authenticated_request(
            &principal,
            pb::UpdateStoreRequest {
                store_id: STORE_ID.to_owned(),
                name: "authorization".to_owned(),
            },
        ),
    )
    .await;
    assert!(matches!(update, Err(status) if status.code() == tonic::Code::Unimplemented));

    for (uri, expected_code) in [
        ("/stores?page_size=0".to_owned(), "page_size_invalid"),
        ("/stores/short".to_owned(), "validation_error"),
    ] {
        let response = router
            .clone()
            .oneshot(Request::get(uri).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), 1_024).await?,
        )?;
        assert_eq!(
            body.get("code").and_then(serde_json::Value::as_str),
            Some(expected_code)
        );
    }
    let missing_tuple = router
        .clone()
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/check"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(missing_tuple.status(), StatusCode::BAD_REQUEST);
    let body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(missing_tuple.into_body(), 1_024).await?,
    )?;
    assert_eq!(
        body.get("code").and_then(serde_json::Value::as_str),
        Some("tuple_key_value_not_specified"),
    );

    for (payload, expected_message) in [
        (
            r#"{"schema_version":"1.1","schema_version":"1.1","type_definitions":[{"type":"user"}]}"#,
            "(line 1:25): duplicate field \"schema_version\"",
        ),
        (
            r#"{"schema_version":"1.1","type_definitions":[{"type":"user"}],"conditions":{"c":{"name":"c","expression":"true","parameters":{}},"c":{"name":"c","expression":"true","parameters":{}}}}"#,
            "(line 1:129): duplicate map key \"c\"",
        ),
        (
            r#"{"schema_version":"1.1","type_definitions":[{"type":"user"}]"#,
            "malformed JSON",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post(format!("/stores/{STORE_ID}/authorization-models"))
                    .header("content-type", "application/json")
                    .body(Body::from(payload))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), 1_024).await?,
        )?;
        assert_eq!(
            body.get("code"),
            Some(&serde_json::json!("validation_error"))
        );
        assert_eq!(
            body.get("message"),
            Some(&serde_json::json!(expected_message)),
        );
    }
    let alias_duplicate = router
        .clone()
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/authorization-models"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"schema_version":"1.1","type_definitions":[{"type":"document","relations":{"viewer":{"computed_userset":{},"computedUserset":{}}}}]}"#,
                ))?,
        )
        .await?;
    let alias_duplicate_body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(alias_duplicate.into_body(), 1_024).await?,
    )?;
    assert!(
        alias_duplicate_body
            .get("message")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| message.contains("duplicate field \"computedUserset\"")),
    );
    let unknown_enum = router
        .clone()
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/authorization-models"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"schema_version":"1.1","type_definitions":[{"type":"user"}],"conditions":{"c":{"name":"c","expression":"true","parameters":{"p":{"type_name":"TYPE_NAME_BOGUS"}}}}}"#,
                ))?,
        )
        .await?;
    assert_eq!(unknown_enum.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(unknown_enum.into_body(), 1_024).await?,
        )?,
        serde_json::json!({
            "code": "invalid_authorization_model",
            "message": "failed to compile expression on condition 'c' - failed to decode parameter type for parameter 'p': unknown condition parameter type `TYPE_NAME_UNSPECIFIED`"
        }),
    );

    for (payload, expected_message) in [
        (
            format!(
                r#"{{"tuple_key":{{"user":"user:anne","relation":"viewer","object":"document:roadmap"}},"authorization_model_id":"{MODEL_ID}","context":{{"x":1,"x":2}}}}"#,
            ),
            "duplicate map key \"x\"",
        ),
        (
            format!(
                r#"{{"tuple_key":null,"tuple_key":{{"user":"user:anne","relation":"viewer","object":"document:roadmap"}},"authorization_model_id":"{MODEL_ID}"}}"#,
            ),
            "duplicate field \"tuple_key\"",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post(format!("/stores/{STORE_ID}/check"))
                    .header("content-type", "application/json")
                    .body(Body::from(payload))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), 1_024).await?,
        )?;
        assert!(
            body.get("message")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|message| message.contains(expected_message)),
            "unexpected duplicate diagnostic: {body}",
        );
    }

    let numeric_enum = router
        .clone()
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/check"))
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"tuple_key":{{"user":"user:anne","relation":"viewer","object":"document:roadmap"}},"authorization_model_id":"{MODEL_ID}","consistency":99}}"#,
                )))?,
        )
        .await?;
    assert_eq!(numeric_enum.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(numeric_enum.into_body(), 1_024).await?,
        )?,
        serde_json::json!({
            "code": "validation_error",
            "message": "invalid CheckRequest.Consistency: value must be one of the defined enum values",
        }),
    );

    let duplicate_unknown = router
        .clone()
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/check"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"unknown":1,"unknown":2}"#))?,
        )
        .await?;
    let duplicate_unknown_body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(duplicate_unknown.into_body(), 1_024).await?,
    )?;
    assert_eq!(
        duplicate_unknown_body.get("code"),
        Some(&serde_json::json!("tuple_key_value_not_specified")),
    );

    let null_relation = router
        .clone()
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/check"))
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"tuple_key":{{"user":"user:anne","relation":null,"object":"document:roadmap"}},"authorization_model_id":"{MODEL_ID}"}}"#,
                )))?,
        )
        .await?;
    let null_relation_body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(null_relation.into_body(), 1_024).await?,
    )?;
    assert_eq!(
        null_relation_body.get("code"),
        Some(&serde_json::json!("validation_error")),
    );

    let protected_router = crate::http_router(
        api.clone(),
        AuthenticationService::preshared(vec![PresharedKey::new(
            "transport-test".parse()?,
            &SecretString::from("transport-test-key-material-with-at-least-32-bytes"),
        )?])?,
    );
    let missing = protected_router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/stores")
                .header("content-type", "application/json")
                .body(Body::from("not-json"))?,
        )
        .await?;
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing.headers().get("www-authenticate"),
        Some(&axum::http::HeaderValue::from_static("Bearer"))
    );
    let missing_body = to_bytes(missing.into_body(), 1_024).await?;
    let invalid_key = protected_router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/stores/{STORE_ID}"))
                .header(
                    "authorization",
                    "Bearer invalid-key-material-with-at-least-32-bytes",
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(invalid_key.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        to_bytes(invalid_key.into_body(), 1_024).await?,
        missing_body
    );
    let mut duplicate_headers = Request::builder()
        .method("GET")
        .uri(format!("/stores/{STORE_ID}"))
        .body(Body::empty())?;
    duplicate_headers.headers_mut().append(
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_static(
            "Bearer transport-test-key-material-with-at-least-32-bytes",
        ),
    );
    duplicate_headers.headers_mut().append(
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_static(
            "Bearer transport-test-key-material-with-at-least-32-bytes",
        ),
    );
    assert_eq!(
        protected_router
            .clone()
            .oneshot(duplicate_headers)
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let authenticated = protected_router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/stores/{STORE_ID}"))
                .header(
                    "authorization",
                    "Bearer transport-test-key-material-with-at-least-32-bytes",
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(authenticated.status(), StatusCode::OK);
    let oversized = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/stores")
                .header("content-type", "application/json")
                .body(Body::from("x".repeat(1_025)))?,
        )
        .await?;
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let deferred = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/stores/{STORE_ID}/expand"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tuple_key":{"object":"document:roadmap"}}"#))?,
        )
        .await?;
    assert_eq!(deferred.status(), StatusCode::NOT_FOUND);
    let deferred_body = to_bytes(deferred.into_body(), 1_024).await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&deferred_body)?,
        serde_json::json!({
            "code": "unimplemented",
            "message": "the operation is not implemented"
        }),
    );
    let grpc_deferred = OpenFgaService::expand(
        &api,
        authenticated_request(
            &principal,
            pb::ExpandRequest {
                store_id: STORE_ID.to_owned(),
                tuple_key: Some(pb::ExpandRequestTupleKey {
                    relation: String::new(),
                    object: "document:roadmap".to_owned(),
                }),
                contextual_tuples: None,
                authorization_model_id: String::new(),
                consistency: 0,
            },
        ),
    )
    .await;
    assert!(matches!(
        grpc_deferred,
        Err(status) if status.code() == tonic::Code::Unimplemented
    ));
    let stores_page = api
        .list_stores(
            &principal,
            pb::ListStoresRequest {
                page_size: None,
                continuation_token: String::new(),
                name: "authorization".to_owned(),
            },
        )
        .await?;
    assert_eq!(stores_page.stores.len(), 1);

    let outsider = Principal::new(PrincipalKind::PresharedKey, "outsider".parse()?);
    let existing_denial = api
        .get_store(
            &outsider,
            pb::GetStoreRequest {
                store_id: STORE_ID.to_owned(),
            },
        )
        .await;
    let missing_denial = api
        .get_store(
            &outsider,
            pb::GetStoreRequest {
                store_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            },
        )
        .await;
    assert!(matches!(existing_denial, Err(error) if error.code() == "forbidden"));
    assert!(matches!(missing_denial, Err(error) if error.code() == "forbidden"));

    let model = api
        .write_authorization_model(&principal, model_request())
        .await?;
    assert_eq!(model.authorization_model_id, MODEL_ID);
    assert!(
        api.read_authorization_model(
            &principal,
            pb::ReadAuthorizationModelRequest {
                store_id: STORE_ID.to_owned(),
                id: MODEL_ID.to_owned()
            }
        )
        .await?
        .authorization_model
        .is_some()
    );
    assert_eq!(
        api.read_authorization_models(
            &principal,
            pb::ReadAuthorizationModelsRequest {
                store_id: STORE_ID.to_owned(),
                page_size: None,
                continuation_token: String::new()
            }
        )
        .await?
        .authorization_models
        .len(),
        1
    );

    api.write(
        &principal,
        pb::WriteRequest {
            store_id: STORE_ID.to_owned(),
            writes: Some(pb::WriteRequestWrites {
                tuple_keys: vec![relationship_tuple()],
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: MODEL_ID.to_owned(),
        },
    )
    .await?;
    assert_eq!(
        api.read(
            &principal,
            pb::ReadRequest {
                store_id: STORE_ID.to_owned(),
                tuple_key: None,
                page_size: None,
                continuation_token: String::new(),
                consistency: 0
            }
        )
        .await?
        .tuples
        .len(),
        1
    );

    let check = check_request();
    assert!(api.check(&principal, check.clone()).await?.allowed);
    let batch = api
        .batch_check(
            &principal,
            pb::BatchCheckRequest {
                store_id: STORE_ID.to_owned(),
                checks: vec![pb::BatchCheckItem {
                    tuple_key: check.tuple_key.clone(),
                    contextual_tuples: None,
                    context: None,
                    correlation_id: "item-1".to_owned(),
                }],
                authorization_model_id: MODEL_ID.to_owned(),
                consistency: 0,
            },
        )
        .await?;
    assert_eq!(batch.result.len(), 1);
    let duplicate = api
        .batch_check(
            &principal,
            pb::BatchCheckRequest {
                store_id: STORE_ID.to_owned(),
                checks: vec![
                    pb::BatchCheckItem {
                        tuple_key: check.tuple_key.clone(),
                        contextual_tuples: None,
                        context: None,
                        correlation_id: "duplicate".to_owned(),
                    },
                    pb::BatchCheckItem {
                        tuple_key: check.tuple_key,
                        contextual_tuples: None,
                        context: None,
                        correlation_id: "duplicate".to_owned(),
                    },
                ],
                authorization_model_id: MODEL_ID.to_owned(),
                consistency: 0,
            },
        )
        .await;
    assert!(matches!(duplicate, Err(error) if error.code() == "validation_error"));

    let listed = api.list_objects(&principal, list_objects_request()).await?;
    assert_eq!(listed.objects, vec!["document:roadmap"]);
    let mut listed_stream = api
        .streamed_list_objects(&principal, streamed_list_objects_request())
        .await?;
    assert_eq!(
        listed_stream
            .next()
            .await
            .ok_or("direct ListObjects stream ended before its result")??
            .to_string(),
        "document:roadmap",
    );
    assert!(listed_stream.next().await.is_none());

    let response = router
        .clone()
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/list-objects"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&list_objects_request())?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<pb::ListObjectsResponse>(
            &to_bytes(response.into_body(), 1_024).await?,
        )?
        .objects,
        vec!["document:roadmap"],
    );
    let response = router
        .clone()
        .oneshot(
            Request::post(format!("/stores/{STORE_ID}/streamed-list-objects"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(
                    &streamed_list_objects_request(),
                )?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let streamed_body = to_bytes(response.into_body(), 1_024).await?;
    assert_eq!(
        serde_json::from_slice::<pb::StreamedListObjectsResponse>(
            streamed_body
                .strip_suffix(b"\n")
                .ok_or("stream item lacked delimiter")?,
        )?
        .object,
        "document:roadmap",
    );

    let assertion = pb::Assertion {
        tuple_key: Some(pb::AssertionTupleKey {
            object: "document:roadmap".to_owned(),
            relation: "viewer".to_owned(),
            user: "user:anne".to_owned(),
        }),
        expectation: true,
        contextual_tuples: Vec::new(),
        context: None,
    };
    api.write_assertions(
        &principal,
        pb::WriteAssertionsRequest {
            store_id: STORE_ID.to_owned(),
            authorization_model_id: MODEL_ID.to_owned(),
            assertions: vec![assertion],
        },
    )
    .await?;
    assert_eq!(
        api.read_assertions(
            &principal,
            pb::ReadAssertionsRequest {
                store_id: STORE_ID.to_owned(),
                authorization_model_id: MODEL_ID.to_owned()
            }
        )
        .await?
        .assertions
        .len(),
        1
    );
    assert_eq!(
        api.read_changes(
            &principal,
            pb::ReadChangesRequest {
                store_id: STORE_ID.to_owned(),
                r#type: String::new(),
                page_size: None,
                continuation_token: String::new(),
                start_time: None
            }
        )
        .await?
        .changes
        .len(),
        1
    );

    assert_grpc_endpoint_admission(api.clone(), authentication.clone()).await?;

    api.delete_store(
        &principal,
        pb::DeleteStoreRequest {
            store_id: STORE_ID.to_owned(),
        },
    )
    .await?;
    assert!(
        api.read_authorization_model(
            &principal,
            pb::ReadAuthorizationModelRequest {
                store_id: STORE_ID.to_owned(),
                id: MODEL_ID.to_owned(),
            },
        )
        .await?
        .authorization_model
        .is_some(),
    );
    assert_eq!(
        api.read(
            &principal,
            pb::ReadRequest {
                store_id: STORE_ID.to_owned(),
                tuple_key: None,
                page_size: None,
                continuation_token: String::new(),
                consistency: 0,
            },
        )
        .await?
        .tuples
        .len(),
        1,
    );
    api.delete_store(
        &principal,
        pb::DeleteStoreRequest {
            store_id: STORE_ID.to_owned(),
        },
    )
    .await?;
    drop(router);
    drop(api);
    let mut storage = Arc::try_unwrap(storage).map_err(|_| "storage references remain")?;
    storage.stop().await?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the real TCP endpoint-class matrix keeps shared server state and ordered assertions \
              visible"
)]
async fn assert_grpc_endpoint_admission(
    mut api: OpenFgaApi,
    authentication: AuthenticationService,
) -> Result<(), Box<dyn Error>> {
    let one = NonZeroU32::MIN;
    let two = NonZeroU32::new(2).ok_or("enumeration admission limit was zero")?;
    api.admission = crate::admission::AdmissionControl::new(
        AdmissionPolicy::builder()
            .administration(one)
            .reads(one)
            .writes(one)
            .checks(one)
            .enumeration(two)
            .build(),
    )?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(crate::grpc_service(api, authentication))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _shutdown = shutdown_receiver.await;
            })
            .await
    });

    let scenario = async {
        let mut client = OpenFgaServiceClient::connect(format!("http://{address}")).await?;

        let administration = pb::GetStoreRequest {
            store_id: STORE_ID.to_owned(),
        };
        client.get_store(administration.clone()).await?;
        let error = client
            .get_store(administration)
            .await
            .err()
            .ok_or("administration class was not limited")?;
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);

        let read = pb::ReadRequest {
            store_id: STORE_ID.to_owned(),
            tuple_key: None,
            page_size: None,
            continuation_token: String::new(),
            consistency: 0,
        };
        client.read(read.clone()).await?;
        let error = client
            .read(read)
            .await
            .err()
            .ok_or("read class was not limited")?;
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);

        let write = pb::WriteRequest {
            store_id: STORE_ID.to_owned(),
            writes: Some(pb::WriteRequestWrites {
                tuple_keys: vec![relationship_tuple()],
                on_duplicate: "ignore".to_owned(),
            }),
            deletes: None,
            authorization_model_id: MODEL_ID.to_owned(),
        };
        client.write(write.clone()).await?;
        let error = client
            .write(write)
            .await
            .err()
            .ok_or("write class was not limited")?;
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);

        let check = check_request();
        client.check(check.clone()).await?;
        let error = client
            .check(check)
            .await
            .err()
            .ok_or("check class was not limited")?;
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);

        let listed = client
            .list_objects(list_objects_request())
            .await?
            .into_inner();
        assert_eq!(listed.objects, vec!["document:roadmap"]);
        let mut listed_stream = client
            .streamed_list_objects(streamed_list_objects_request())
            .await?
            .into_inner();
        assert_eq!(
            listed_stream
                .message()
                .await?
                .ok_or("gRPC ListObjects stream ended before its result")?
                .object,
            "document:roadmap",
        );
        assert!(listed_stream.message().await?.is_none());
        let error = client
            .list_objects(list_objects_request())
            .await
            .err()
            .ok_or("enumeration class was not limited")?;
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let _shutdown_result = shutdown_sender.send(());
    server.await??;
    scenario
}

fn authenticated_request<T>(principal: &Principal, message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.extensions_mut().insert(principal.clone());
    request
}

fn invalid_model_cases() -> Vec<(pb::WriteAuthorizationModelRequest, &'static str)> {
    vec![
        (
            model_with_viewer(pb::Userset { userset: None }),
            "the definition of relation 'viewer' in object type 'document' is invalid",
        ),
        (
            model_with_viewer(pb::Userset {
                userset: Some(pb::userset::Userset::Union(pb::Usersets {
                    child: Vec::new(),
                })),
            }),
            "invalid relation: 'document#viewer' as union has less than 2 children",
        ),
        (
            model_with_viewer(pb::Userset {
                userset: Some(pb::userset::Userset::Intersection(pb::Usersets {
                    child: vec![direct_userset()],
                })),
            }),
            "invalid relation: 'document#viewer' as intersection has less than 2 children",
        ),
        (
            potential_loop_model(),
            "the definition of relation 'action1' in object type 'document' is invalid: potential \
             loop",
        ),
        (
            potential_loop_set_model(false),
            "the definition of relation 'action1' in object type 'document' is invalid: potential \
             loop",
        ),
        (
            potential_loop_set_model(true),
            "the definition of relation 'action1' in object type 'document' is invalid: potential \
             loop",
        ),
        (
            model_with_condition("1", HashMap::new()),
            "failed to compile expression on condition 'c' - expected a bool condition expression \
             output, but got 'int'",
        ),
        (
            model_with_condition("1 + \"x\"", HashMap::new()),
            "failed to compile expression on condition 'c' - found no matching overload for '_+_' \
             applied to '(int, string)'",
        ),
        (
            model_with_condition("x", HashMap::new()),
            "failed to compile expression on condition 'c' - undeclared reference to 'x'",
        ),
        (
            model_with_condition("ipaddress()", HashMap::new()),
            "failed to compile expression on condition 'c' - found no matching overload for \
             'ipaddress' applied to '()'",
        ),
        (
            model_with_condition(
                "x",
                HashMap::from([(
                    "x".to_owned(),
                    pb::ConditionParamTypeRef {
                        type_name: pb::condition_param_type_ref::TypeName::Map as i32,
                        generic_types: Vec::new(),
                    },
                )]),
            ),
            "failed to compile expression on condition 'c' - failed to decode parameter type for \
             parameter 'x': condition parameter type `TYPE_NAME_MAP` requires 1 generic types; \
             found 0",
        ),
        (
            model_with_condition(
                "x",
                HashMap::from([(
                    "x".to_owned(),
                    pb::ConditionParamTypeRef {
                        type_name: 0,
                        generic_types: Vec::new(),
                    },
                )]),
            ),
            "failed to compile expression on condition 'c' - failed to decode parameter type for \
             parameter 'x': unknown condition parameter type `TYPE_NAME_UNSPECIFIED`",
        ),
    ]
}

fn model_with_viewer(rewrite: pb::Userset) -> pb::WriteAuthorizationModelRequest {
    let mut request = model_request();
    if let Some(document) = request
        .type_definitions
        .iter_mut()
        .find(|definition| definition.r#type == "document")
    {
        document.relations.insert("viewer".to_owned(), rewrite);
    }
    request
}

fn potential_loop_model() -> pb::WriteAuthorizationModelRequest {
    let computed = |relation: &str| pb::Userset {
        userset: Some(pb::userset::Userset::ComputedUserset(pb::ObjectRelation {
            object: String::new(),
            relation: relation.to_owned(),
        })),
    };
    pb::WriteAuthorizationModelRequest {
        store_id: STORE_ID.to_owned(),
        schema_version: "1.1".to_owned(),
        type_definitions: vec![pb::TypeDefinition {
            r#type: "document".to_owned(),
            relations: HashMap::from([
                ("action1".to_owned(), computed("action2")),
                ("action2".to_owned(), computed("action1")),
            ]),
            metadata: Some(pb::Metadata {
                relations: HashMap::new(),
                module: String::new(),
                source_info: None,
            }),
        }],
        conditions: HashMap::new(),
    }
}

fn potential_loop_set_model(exclusion: bool) -> pb::WriteAuthorizationModelRequest {
    let computed = |relation: &str| pb::Userset {
        userset: Some(pb::userset::Userset::ComputedUserset(pb::ObjectRelation {
            object: String::new(),
            relation: relation.to_owned(),
        })),
    };
    let rewrite = |next: &str, third: &str| {
        if exclusion {
            pb::Userset {
                userset: Some(pb::userset::Userset::Difference(Box::new(pb::Difference {
                    base: Some(Box::new(computed("admin"))),
                    subtract: Some(Box::new(computed(next))),
                }))),
            }
        } else {
            pb::Userset {
                userset: Some(pb::userset::Userset::Intersection(pb::Usersets {
                    child: vec![computed("admin"), computed(next), computed(third)],
                })),
            }
        }
    };
    let mut request = model_request();
    let Some(document) = request
        .type_definitions
        .iter_mut()
        .find(|definition| definition.r#type == "document")
    else {
        return request;
    };
    document.relations = HashMap::from([
        ("admin".to_owned(), direct_userset()),
        ("action1".to_owned(), rewrite("action2", "action3")),
        ("action2".to_owned(), rewrite("action3", "action1")),
        ("action3".to_owned(), rewrite("action1", "action2")),
    ]);
    if let Some(metadata) = document.metadata.as_mut()
        && let Some(viewer) = metadata.relations.remove("viewer")
    {
        metadata.relations.insert("admin".to_owned(), viewer);
    }
    request
}

fn model_with_condition(
    expression: &str,
    parameters: HashMap<String, pb::ConditionParamTypeRef>,
) -> pb::WriteAuthorizationModelRequest {
    let mut request = model_request();
    request.conditions.insert(
        "c".to_owned(),
        pb::Condition {
            name: "c".to_owned(),
            expression: expression.to_owned(),
            parameters,
            metadata: None,
        },
    );
    request
}

fn direct_userset() -> pb::Userset {
    pb::Userset {
        userset: Some(pb::userset::Userset::This(pb::DirectUserset {})),
    }
}

fn conditioned_model_request() -> pb::WriteAuthorizationModelRequest {
    let mut request = model_request();
    request.conditions.insert(
        "c1".to_owned(),
        pb::Condition {
            name: "c1".to_owned(),
            expression: "true".to_owned(),
            parameters: HashMap::from([(
                "x".to_owned(),
                pb::ConditionParamTypeRef {
                    type_name: pb::condition_param_type_ref::TypeName::String as i32,
                    generic_types: Vec::new(),
                },
            )]),
            metadata: None,
        },
    );
    request.conditions.insert(
        "c2".to_owned(),
        pb::Condition {
            name: "c2".to_owned(),
            expression: "true".to_owned(),
            parameters: HashMap::new(),
            metadata: None,
        },
    );
    if let Some(document) = request
        .type_definitions
        .iter_mut()
        .find(|definition| definition.r#type == "document")
        && let Some(metadata) = document.metadata.as_mut()
        && let Some(viewer) = metadata.relations.get_mut("viewer")
        && let Some(restriction) = viewer.directly_related_user_types.first_mut()
    {
        restriction.condition = "c1".to_owned();
        let mut alternate = restriction.clone();
        alternate.condition = "c2".to_owned();
        viewer.directly_related_user_types.push(alternate);
    }
    request
}

fn write_request(tuple: pb::TupleKey, on_duplicate: String) -> pb::WriteRequest {
    pb::WriteRequest {
        store_id: STORE_ID.to_owned(),
        writes: Some(pb::WriteRequestWrites {
            tuple_keys: vec![tuple],
            on_duplicate,
        }),
        deletes: None,
        authorization_model_id: MODEL_ID.to_owned(),
    }
}

fn delete_request(object: &str, on_missing: String) -> pb::WriteRequest {
    pb::WriteRequest {
        store_id: STORE_ID.to_owned(),
        writes: None,
        deletes: Some(pb::WriteRequestDeletes {
            tuple_keys: vec![pb::TupleKeyWithoutCondition {
                user: "user:anne".to_owned(),
                relation: "viewer".to_owned(),
                object: object.to_owned(),
            }],
            on_missing,
        }),
        authorization_model_id: MODEL_ID.to_owned(),
    }
}

fn json_struct(value: serde_json::Value) -> Result<pbjson_types::Struct, serde_json::Error> {
    serde_json::from_value(value)
}

fn relationship_tuple() -> pb::TupleKey {
    pb::TupleKey {
        user: "user:anne".to_owned(),
        relation: "viewer".to_owned(),
        object: "document:roadmap".to_owned(),
        condition: None,
    }
}

fn conditioned_tuple() -> pb::TupleKey {
    conditioned_tuple_for("document:roadmap")
}

fn conditioned_tuple_for(object: &str) -> pb::TupleKey {
    let mut tuple = relationship_tuple();
    tuple.object = object.to_owned();
    tuple.condition = Some(pb::RelationshipCondition {
        name: "c1".to_owned(),
        context: None,
    });
    tuple
}

fn delete_tuple(object: &str) -> pb::TupleKeyWithoutCondition {
    pb::TupleKeyWithoutCondition {
        user: "user:anne".to_owned(),
        relation: "viewer".to_owned(),
        object: object.to_owned(),
    }
}

fn check_request() -> pb::CheckRequest {
    pb::CheckRequest {
        store_id: STORE_ID.to_owned(),
        tuple_key: Some(pb::CheckRequestTupleKey {
            user: "user:anne".to_owned(),
            relation: "viewer".to_owned(),
            object: "document:roadmap".to_owned(),
        }),
        contextual_tuples: None,
        authorization_model_id: MODEL_ID.to_owned(),
        trace: false,
        context: None,
        consistency: 0,
    }
}

fn list_objects_request() -> pb::ListObjectsRequest {
    pb::ListObjectsRequest {
        store_id: STORE_ID.to_owned(),
        authorization_model_id: MODEL_ID.to_owned(),
        r#type: "document".to_owned(),
        relation: "viewer".to_owned(),
        user: "user:anne".to_owned(),
        contextual_tuples: None,
        context: None,
        consistency: 0,
    }
}

fn streamed_list_objects_request() -> pb::StreamedListObjectsRequest {
    let request = list_objects_request();
    pb::StreamedListObjectsRequest {
        store_id: request.store_id,
        authorization_model_id: request.authorization_model_id,
        r#type: request.r#type,
        relation: request.relation,
        user: request.user,
        contextual_tuples: request.contextual_tuples,
        context: request.context,
        consistency: request.consistency,
    }
}

fn model_request() -> pb::WriteAuthorizationModelRequest {
    let direct = pb::Userset {
        userset: Some(pb::userset::Userset::This(pb::DirectUserset {})),
    };
    pb::WriteAuthorizationModelRequest {
        store_id: STORE_ID.to_owned(),
        schema_version: "1.1".to_owned(),
        type_definitions: vec![
            pb::TypeDefinition {
                r#type: "user".to_owned(),
                relations: HashMap::new(),
                metadata: Some(pb::Metadata {
                    relations: HashMap::new(),
                    module: String::new(),
                    source_info: None,
                }),
            },
            pb::TypeDefinition {
                r#type: "document".to_owned(),
                relations: HashMap::from([("viewer".to_owned(), direct)]),
                metadata: Some(pb::Metadata {
                    relations: HashMap::from([(
                        "viewer".to_owned(),
                        pb::RelationMetadata {
                            directly_related_user_types: vec![pb::RelationReference {
                                r#type: "user".to_owned(),
                                condition: String::new(),
                                relation_or_wildcard: None,
                            }],
                            module: String::new(),
                            source_info: None,
                        },
                    )]),
                    module: String::new(),
                    source_info: None,
                }),
            },
        ],
        conditions: HashMap::new(),
    }
}
