//! End-to-end service orchestration over the actor-owned memory backend.

use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use openfga_check::CheckBudget;
use openfga_domain::{
    BatchCheckCommand, BatchCheckItem, BatchCheckItems, CheckCommand, ConditionContext,
    ConsistencyPreference, ContextualTuples, Deadline, InputLimits, ModelSelection, Principal,
    PrincipalKind, QueryContext, RelationshipTuple, RequestTimeout, StoreId, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_service::{CheckService, ServiceErrorKind};
use openfga_storage::{
    ModelReader, ModelWriter, OperationContext, StorageCancellationToken, StoreName, StoreWriter,
    StoredAuthorizationModel, TupleReader, TupleWriteOptions, TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

#[tokio::test]
async fn test_should_resolve_explicit_and_latest_models_for_check_and_batch()
-> Result<(), Box<dyn Error>> {
    let storage = configured_storage().await?;
    let service = service(storage.clone());

    let allow = service
        .check(
            &check_command(
                ModelSelection::Explicit(MODEL_ID.parse()?),
                "document:roadmap#viewer@user:anne",
            )?,
            StorageCancellationToken::new(),
        )
        .await?;
    assert!(allow.allowed());

    let items = BatchCheckItems::new(
        vec![
            BatchCheckItem::new(
                "allow".parse()?,
                "document:roadmap#viewer@user:anne".parse()?,
                ContextualTuples::empty(),
                ConditionContext::empty(),
            ),
            BatchCheckItem::new(
                "deny".parse()?,
                "document:roadmap#viewer@user:bob".parse()?,
                ContextualTuples::empty(),
                ConditionContext::empty(),
            ),
        ],
        &InputLimits::default(),
    )?;
    let batch = service
        .batch_check(
            &BatchCheckCommand::new(query_context(ModelSelection::Latest)?, items),
            StorageCancellationToken::new(),
        )
        .await?;
    let results = batch.results();
    assert_eq!(results.len(), 2);
    assert!(
        results
            .first()
            .and_then(|result| result.outcome().as_ref().ok())
            .is_some_and(|outcome| outcome.allowed()),
    );
    assert!(
        results
            .get(1)
            .and_then(|result| result.outcome().as_ref().ok())
            .is_some_and(|outcome| !outcome.allowed()),
    );

    drop(service);
    shutdown(storage).await
}

#[tokio::test]
async fn test_should_preserve_model_not_found_and_cancellation_categories()
-> Result<(), Box<dyn Error>> {
    let storage = configured_storage().await?;
    let service = service(storage.clone());
    let missing = service
        .check(
            &check_command(
                ModelSelection::Explicit("01ARZ3NDEKTSV4RRFFQ69G5FAX".parse()?),
                "document:roadmap#viewer@user:anne",
            )?,
            StorageCancellationToken::new(),
        )
        .await
        .err()
        .ok_or("missing model unexpectedly evaluated")?;
    assert_eq!(missing.kind(), ServiceErrorKind::ModelNotFound);

    let cancellation = StorageCancellationToken::new();
    cancellation.cancel();
    let cancelled = service
        .check(
            &check_command(
                ModelSelection::Explicit(MODEL_ID.parse()?),
                "document:roadmap#viewer@user:anne",
            )?,
            cancellation,
        )
        .await
        .err()
        .ok_or("cancelled service request unexpectedly evaluated")?;
    assert_eq!(cancelled.kind(), ServiceErrorKind::Cancelled);

    drop(service);
    shutdown(storage).await
}

fn service(storage: Arc<MemoryStorage>) -> CheckService {
    let models: Arc<dyn ModelReader> = storage.clone();
    let tuples: Arc<dyn TupleReader> = storage;
    CheckService::direct(models, tuples, CheckBudget::default())
}

async fn configured_storage() -> Result<Arc<MemoryStorage>, Box<dyn Error>> {
    let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
    let context = operation_context()?;
    let store_id = STORE_ID.parse::<StoreId>()?;
    storage
        .create_store(
            &context,
            store_id,
            StoreName::new("service-check-tests".to_owned())?,
        )
        .await?;
    let source = Arc::new(model_source()?);
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
            vec![RelationshipTuple::unconditional(
                "document:roadmap#viewer@user:anne".parse()?,
            )],
            TupleWriteOptions::default(),
        )
        .await?;
    Ok(storage)
}

fn model_source() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![
            TypeDefinitionSource::new("user".parse()?, Vec::new()),
            TypeDefinitionSource::new(
                "document".parse()?,
                vec![RelationSource::new(
                    "viewer".parse()?,
                    RewriteSource::Direct,
                    vec![DirectRestrictionSource::new(
                        "user".parse()?,
                        RestrictionKindSource::Object,
                        None,
                    )],
                )],
            ),
        ],
        Vec::new(),
    ))
}

fn check_command(selection: ModelSelection, tuple: &str) -> Result<CheckCommand, Box<dyn Error>> {
    Ok(CheckCommand::new(
        query_context(selection)?,
        tuple.parse::<TupleKey>()?,
    ))
}

fn query_context(selection: ModelSelection) -> Result<QueryContext, Box<dyn Error>> {
    Ok(QueryContext::builder()
        .store_id(STORE_ID.parse()?)
        .model_selection(selection)
        .consistency(ConsistencyPreference::HigherConsistency)
        .contextual_tuples(ContextualTuples::empty())
        .condition_context(ConditionContext::empty())
        .deadline(future_deadline()?)
        .principal(Principal::new(
            PrincipalKind::Internal,
            "service-tests".parse()?,
        ))
        .build())
}

fn operation_context() -> Result<OperationContext, Box<dyn Error>> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        future_deadline()?,
        StorageCancellationToken::new(),
    ))
}

fn future_deadline() -> Result<Deadline, Box<dyn Error>> {
    Ok(Deadline::from_timeout(
        Instant::now(),
        RequestTimeout::new(Duration::from_secs(5))?,
    )?)
}

async fn shutdown(storage: Arc<MemoryStorage>) -> Result<(), Box<dyn Error>> {
    let mut owner = Arc::try_unwrap(storage).map_err(|_| "memory storage still shared")?;
    owner.stop().await?;
    Ok(())
}
