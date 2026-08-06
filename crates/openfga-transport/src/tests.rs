use std::{
    collections::HashMap,
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
use openfga_auth::{AuthenticationService, AuthorizationPolicy, PresharedKey};
use openfga_check::CheckBudget;
use openfga_domain::{
    AuthorizationModelId, FingerprintBuilder, InputLimits, Principal, PrincipalId, PrincipalKind,
    RequestTimeout, StoreId, TokenCodec, TokenKey, TokenKeyId, TokenOperation,
};
use openfga_model::ModelCompiler;
use openfga_proto::openfga::{v1 as pb, v1::open_fga_service_server::OpenFgaService};
use openfga_service::{
    AssertionService, ChangeService, CheckService, IdentifierSource, IdentifierSourceError,
    ModelPublication, ModelService, ServiceClock, ServiceError, StoreService, TupleService,
};
use openfga_storage::{
    AssertionReader, AssertionWriter, ChangeReader, ModelReader, ModelWriter, OperationContext,
    StorageCursor, StorageError, StorageErrorKind, StoreReader, StoreWriter, TupleReader,
    TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use prost::Message;
use secrecy::SecretString;
use tower::ServiceExt;

use crate::{ApiError, OpenFgaApi, OpenFgaServices, TransportConfig};

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

#[test]
fn test_should_match_protocol_json_golden_and_reject_unknown_fields() -> Result<(), Box<dyn Error>>
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
    assert!(serde_json::from_str::<pb::CheckRequest>("{\"unknown\":true}").is_err());
    Ok(())
}

#[test]
fn test_should_map_errors_without_exposing_internal_diagnostics() {
    let error = ApiError::internal();
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(
        status.message(),
        "internal_error: an internal error occurred"
    );

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
            tonic::Code::InvalidArgument,
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
fn test_should_cancel_in_flight_work_when_request_guard_drops() {
    let guard = super::api::RequestCancellation::new();
    let token = guard.token();
    drop(guard);
    assert!(token.is_cancelled());
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the end-to-end protocol flow intentionally keeps ordered cross-endpoint state \
              visible"
)]
async fn test_should_execute_every_m2_use_case_through_shared_wire_adapter()
-> Result<(), Box<dyn Error>> {
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
                store_writes.clone(),
                identifiers.clone(),
            ))
            .models(ModelService::new(
                stores.clone(),
                models.clone(),
                model_writes.clone(),
                ModelPublication::new(identifiers, Arc::new(FixedClock), ModelCompiler::default()),
            ))
            .assertions(AssertionService::new(
                stores.clone(),
                models.clone(),
                assertion_reads.clone(),
                assertion_writes.clone(),
                limits.clone(),
            ))
            .tuples(TupleService::new(
                stores.clone(),
                models.clone(),
                tuples.clone(),
                tuple_writes.clone(),
                limits.clone(),
            ))
            .changes(ChangeService::new(stores.clone(), changes.clone()))
            .checks(CheckService::direct(
                models.clone(),
                tuples.clone(),
                CheckBudget::default(),
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
            .maximum_message_bytes(1_024)
            .build(),
    )?;

    let router = crate::http_router(api.clone(), authentication);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/stores")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"engineering"}"#))?,
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
    assert_eq!(grpc_store.name, "engineering");
    let updated = OpenFgaService::update_store(
        &api,
        authenticated_request(
            &principal,
            pb::UpdateStoreRequest {
                store_id: STORE_ID.to_owned(),
                name: "authorization".to_owned(),
            },
        ),
    )
    .await?
    .into_inner();
    assert_eq!(updated.name, "authorization");

    let invalid = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/stores")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"valid","unknown":true}"#))?,
        )
        .await?;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

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
                .body(Body::from("{}"))?,
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
                tuple_key: None,
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

    api.delete_store(
        &principal,
        pb::DeleteStoreRequest {
            store_id: STORE_ID.to_owned(),
        },
    )
    .await?;
    drop(router);
    drop(api);
    drop(changes);
    drop(assertion_writes);
    drop(assertion_reads);
    drop(tuple_writes);
    drop(tuples);
    drop(model_writes);
    drop(models);
    drop(store_writes);
    drop(stores);
    let mut storage = Arc::try_unwrap(storage).map_err(|_| "storage references remain")?;
    storage.stop().await?;
    Ok(())
}

fn authenticated_request<T>(principal: &Principal, message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.extensions_mut().insert(principal.clone());
    request
}

fn relationship_tuple() -> pb::TupleKey {
    pb::TupleKey {
        user: "user:anne".to_owned(),
        relation: "viewer".to_owned(),
        object: "document:roadmap".to_owned(),
        condition: None,
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
