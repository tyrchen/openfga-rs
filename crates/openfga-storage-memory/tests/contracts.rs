//! Shared memory-backend capability, atomicity, pagination, and lifecycle contracts.

use std::{
    error::Error,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use openfga_domain::{
    AuthorizationModelId, ConditionBinding, ConditionContext, ConditionName, ConditionReference,
    ConsistencyPreference, ContextualTuples, Deadline, InputLimits, RelationName,
    RelationshipTuple, RequestTimeout, StoreId, TupleKey, TypeName,
};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ChangeFilter, ChangeOperation, ChangeReader,
    ConditionFilter, HealthCheck, ModelReader, ModelWriter, ObjectRelationFilter, OperationContext,
    PageOptions, ReadOptions, ReverseTupleFilter, StorageCancellationToken, StorageError,
    StorageErrorKind, StoreFilter, StoreName, StoreReader, StoreWriter, StoredAuthorizationModel,
    TupleReadFilter, TupleReader, TupleWriteOptions, TupleWriter, UsersetRestrictionFilter,
    UsersetTupleFilter, WriteConflictPolicy,
    contract::{TupleContractFixture, verify_tuple_contract},
};
use openfga_storage_memory::{
    MemoryStorage, MemoryStorageConfig, MutationFaultInjector, MutationFaultStage, StorageClock,
};

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ONE: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const MODEL_TWO: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
const MODEL_THREE: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAY";
const MODEL_IDS: [&str; 3] = [MODEL_ONE, MODEL_TWO, MODEL_THREE];

#[tokio::test]
async fn test_should_persist_namespace_data_without_a_store_record() -> Result<(), Box<dyn Error>> {
    let mut storage = MemoryStorage::start(MemoryStorageConfig::default())?;
    let context = operation_context()?;
    let store_id = STORE_ID.parse()?;
    storage
        .write_model(
            &context,
            stored_model(store_id, MODEL_ONE, SystemTime::UNIX_EPOCH)?,
        )
        .await?;

    let relationship = tuple("document:roadmap#viewer@user:anne")?;
    storage
        .write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![relationship.clone()],
            TupleWriteOptions::default(),
        )
        .await?;
    assert_eq!(
        storage
            .read_tuples(
                &context,
                store_id,
                &TupleReadFilter::all(),
                &page_options(10, None)?,
            )
            .await?
            .items()
            .len(),
        1,
    );
    storage
        .write_tuples(
            &context,
            store_id,
            vec![relationship.key().clone()],
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await?;
    assert!(
        storage
            .read_tuples(
                &context,
                store_id,
                &TupleReadFilter::all(),
                &page_options(10, None)?,
            )
            .await?
            .items()
            .is_empty(),
    );

    storage.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_should_pass_backend_independent_tuple_contract() -> Result<(), Box<dyn Error>> {
    let mut storage = MemoryStorage::start(MemoryStorageConfig::default())?;
    let context = operation_context()?;
    let store_id = create_store(&storage, &context).await?;
    let fixture = TupleContractFixture::new(
        store_id,
        tuple("document:contract#viewer@user:anne")?,
        tuple("document:contract#viewer@user:bob")?,
        ObjectRelationFilter::new(
            "document:contract".parse()?,
            "viewer".parse()?,
            Vec::new(),
            ConditionFilter::any(),
            &InputLimits::default(),
        )?,
        read_options(2)?,
    );

    verify_tuple_contract(&storage, &context, &fixture).await?;
    storage.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_should_update_all_tuple_indexes_and_changelog_atomically()
-> Result<(), Box<dyn Error>> {
    let timestamp = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut storage = storage_with(timestamp, Arc::new(NeverFail))?;
    let context = operation_context()?;
    let store_id = create_store(&storage, &context).await?;
    let direct = tuple("document:roadmap#viewer@user:anne")?;
    let userset = tuple("document:roadmap#viewer@group:eng#member")?;
    let wildcard = tuple("document:roadmap#viewer@user:*")?;

    let outcome = storage
        .write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![direct.clone(), userset.clone(), wildcard.clone()],
            TupleWriteOptions::default(),
        )
        .await?;
    assert_eq!(outcome.change_ids().len(), 3);
    assert!(
        outcome
            .change_ids()
            .windows(2)
            .all(|pair| matches!(pair, [left, right] if left < right)),
    );

    let forward_filter = assert_tuple_indexes(
        &storage, &context, store_id, &direct, &userset, &wildcard, timestamp,
    )
    .await?;
    let changes = storage
        .read_changes(
            &context,
            store_id,
            &ChangeFilter::default(),
            &page_options(10, None)?,
        )
        .await?;
    assert_eq!(changes.items().len(), 3);
    assert!(changes.items().iter().all(|change| {
        change.operation() == ChangeOperation::Write && change.timestamp() == timestamp
    }));

    storage
        .write_tuples(
            &context,
            store_id,
            vec![direct.key().clone()],
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await?;
    assert!(
        !storage
            .tuple_exists(&context, store_id, direct.key())
            .await?
    );
    assert_eq!(
        storage
            .count_object_relation(&context, store_id, &forward_filter)
            .await?,
        2,
    );
    let changes = storage
        .read_changes(
            &context,
            store_id,
            &ChangeFilter::default(),
            &page_options(10, None)?,
        )
        .await?;
    assert_eq!(
        changes
            .items()
            .last()
            .map(openfga_storage::TupleChange::operation),
        Some(ChangeOperation::Delete)
    );
    storage.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_should_page_partial_tuple_reads_without_duplicates_or_omissions()
-> Result<(), Box<dyn Error>> {
    let mut storage = MemoryStorage::start(MemoryStorageConfig::default())?;
    let context = operation_context()?;
    let store_id = create_store(&storage, &context).await?;
    storage
        .write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![
                tuple("document:roadmap#viewer@user:anne")?,
                tuple("document:roadmap#viewer@user:bob")?,
                tuple("document:other#viewer@user:carol")?,
            ],
            TupleWriteOptions::default(),
        )
        .await?;
    let filter = TupleReadFilter::new(
        "document".parse()?,
        Some("roadmap".parse()?),
        Some("viewer".parse()?),
        None,
    )?;
    let first = storage
        .read_tuples(&context, store_id, &filter, &page_options(1, None)?)
        .await?;
    assert_eq!(first.items().len(), 1);
    let cursor = first
        .continuation()
        .cloned()
        .ok_or("missing tuple cursor")?;
    let second = storage
        .read_tuples(&context, store_id, &filter, &page_options(1, Some(cursor))?)
        .await?;
    assert_eq!(second.items().len(), 1);
    assert!(second.continuation().is_none());
    assert_ne!(first.items().first(), second.items().first());

    let invalid_cursor = openfga_storage::StorageCursor::new(b"not-a-tuple".to_vec())?;
    let invalid = storage
        .read_tuples(
            &context,
            store_id,
            &filter,
            &page_options(1, Some(invalid_cursor))?,
        )
        .await
        .err()
        .ok_or("invalid tuple cursor unexpectedly accepted")?;
    assert_eq!(invalid.kind(), StorageErrorKind::InvalidContinuation);
    storage.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_should_roll_back_every_injected_mutation_failure_stage() -> Result<(), Box<dyn Error>>
{
    for stage in [
        MutationFaultStage::Validated,
        MutationFaultStage::DeletesPrepared,
        MutationFaultStage::WritesPrepared,
        MutationFaultStage::ChangesPrepared,
    ] {
        let injector = Arc::new(FailAt::new(stage));
        let mut storage = storage_with(SystemTime::now(), injector)?;
        let context = operation_context()?;
        let store_id = create_store(&storage, &context).await?;
        let relationship = tuple("document:roadmap#viewer@user:anne")?;
        let error = storage
            .write_tuples(
                &context,
                store_id,
                Vec::new(),
                vec![relationship.clone()],
                TupleWriteOptions::default(),
            )
            .await
            .err()
            .ok_or("fault injection unexpectedly committed")?;
        assert_eq!(error.kind(), StorageErrorKind::Internal);
        assert!(
            !storage
                .tuple_exists(&context, store_id, relationship.key())
                .await?,
        );
        assert!(
            storage
                .read_changes(
                    &context,
                    store_id,
                    &ChangeFilter::default(),
                    &page_options(10, None)?,
                )
                .await?
                .items()
                .is_empty(),
        );
        storage.stop().await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_should_roll_back_cancellation_at_every_mutation_stage() -> Result<(), Box<dyn Error>>
{
    for stage in [
        MutationFaultStage::Validated,
        MutationFaultStage::DeletesPrepared,
        MutationFaultStage::WritesPrepared,
        MutationFaultStage::ChangesPrepared,
    ] {
        let cancellation = StorageCancellationToken::new();
        let injector = Arc::new(CancelAt {
            stage,
            cancellation: cancellation.clone(),
        });
        let mut storage = storage_with(SystemTime::now(), injector)?;
        let setup_context = operation_context()?;
        let store_id = create_store(&storage, &setup_context).await?;
        let write_context = OperationContext::new(
            ConsistencyPreference::HigherConsistency,
            future_deadline()?,
            cancellation,
        );
        let relationship = tuple("document:roadmap#viewer@user:anne")?;

        let error = storage
            .write_tuples(
                &write_context,
                store_id,
                Vec::new(),
                vec![relationship.clone()],
                TupleWriteOptions::default(),
            )
            .await
            .err()
            .ok_or("cancelled mutation unexpectedly committed")?;
        assert_eq!(error.kind(), StorageErrorKind::Cancelled);
        assert!(
            !storage
                .tuple_exists(&setup_context, store_id, relationship.key())
                .await?,
        );
        assert!(
            storage
                .read_changes(
                    &setup_context,
                    store_id,
                    &ChangeFilter::default(),
                    &page_options(10, None)?,
                )
                .await?
                .items()
                .is_empty(),
        );
        assert_eq!(storage.diagnostics().active_operations(), 0);
        storage.stop().await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_should_enforce_conflict_ignore_and_concurrent_write_contracts()
-> Result<(), Box<dyn Error>> {
    let mut storage = MemoryStorage::start(MemoryStorageConfig::default())?;
    let context = operation_context()?;
    let store_id = create_store(&storage, &context).await?;
    let relationship = tuple("document:roadmap#viewer@user:anne")?;

    let (first, second) = tokio::join!(
        storage.write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![relationship.clone()],
            TupleWriteOptions::default(),
        ),
        storage.write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![relationship.clone()],
            TupleWriteOptions::default(),
        ),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let conflict = first
        .err()
        .or_else(|| second.err())
        .ok_or("missing conflict")?;
    assert_eq!(conflict.kind(), StorageErrorKind::Conflict);

    let ignored = storage
        .write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![relationship.clone()],
            TupleWriteOptions::new(WriteConflictPolicy::Error, WriteConflictPolicy::Ignore),
        )
        .await?;
    assert!(ignored.change_ids().is_empty());

    let condition_conflict = RelationshipTuple::new(
        relationship.key().clone(),
        ConditionReference::Conditional(ConditionBinding::new(
            ConditionName::parse_with_limits("alternate", &InputLimits::default())?,
            ConditionContext::empty(),
        )),
    );
    let conflict = storage
        .write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![condition_conflict],
            TupleWriteOptions::new(WriteConflictPolicy::Error, WriteConflictPolicy::Ignore),
        )
        .await
        .err()
        .ok_or("ignore accepted a different condition on an existing tuple")?;
    assert_eq!(conflict.kind(), StorageErrorKind::Conflict);
    assert_eq!(conflict.code(), "tuple_condition_conflict");

    let missing_key: TupleKey = "document:missing#viewer@user:anne".parse()?;
    let ignored = storage
        .write_tuples(
            &context,
            store_id,
            vec![missing_key],
            Vec::new(),
            TupleWriteOptions::new(WriteConflictPolicy::Ignore, WriteConflictPolicy::Error),
        )
        .await?;
    assert!(ignored.change_ids().is_empty());

    let both = storage
        .write_tuples(
            &context,
            store_id,
            vec![relationship.key().clone()],
            vec![relationship],
            TupleWriteOptions::default(),
        )
        .await
        .err()
        .ok_or("conflicting delete/write unexpectedly committed")?;
    assert_eq!(both.kind(), StorageErrorKind::Conflict);
    assert_eq!(
        storage
            .read_changes(
                &context,
                store_id,
                &ChangeFilter::default(),
                &page_options(10, None)?
            )
            .await?
            .items()
            .len(),
        1,
    );
    storage.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_should_publish_models_assertions_and_stable_pages() -> Result<(), Box<dyn Error>> {
    let timestamp = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut storage = storage_with(timestamp, Arc::new(NeverFail))?;
    let context = operation_context()?;
    let store_id = create_store(&storage, &context).await?;
    assert_store_pagination(&storage, &context).await?;

    for model_id in MODEL_IDS {
        storage
            .write_model(&context, stored_model(store_id, model_id, timestamp)?)
            .await?;
    }

    assert_eq!(
        storage
            .read_latest_model(&context, store_id)
            .await?
            .model_id()
            .to_string(),
        MODEL_THREE,
    );
    let first = storage
        .list_models(&context, store_id, &page_options(2, None)?)
        .await?;
    assert_eq!(first.items().len(), 2);
    let first_ids: Vec<_> = first
        .items()
        .iter()
        .map(|model| model.model_id().to_string())
        .collect();
    assert_eq!(first_ids, [MODEL_THREE, MODEL_TWO]);
    let second = storage
        .list_models(
            &context,
            store_id,
            &page_options(2, first.continuation().cloned())?,
        )
        .await?;
    assert_eq!(second.items().len(), 1);
    let second_ids: Vec<_> = second
        .items()
        .iter()
        .map(|model| model.model_id().to_string())
        .collect();
    assert_eq!(second_ids, [MODEL_ONE]);
    assert!(second.continuation().is_none());

    let assertion = Assertion::new(
        "document:roadmap#viewer@user:anne".parse()?,
        true,
        ContextualTuples::empty(),
        ConditionContext::empty(),
    );
    let latest_id = MODEL_THREE.parse::<AuthorizationModelId>()?;
    storage
        .write_assertions(&context, store_id, latest_id, vec![assertion.clone()])
        .await?;
    assert_eq!(
        storage
            .read_assertions(&context, store_id, latest_id)
            .await?
            .as_ref(),
        &[assertion],
    );

    let duplicate = storage
        .write_model(&context, stored_model(store_id, MODEL_THREE, timestamp)?)
        .await
        .err()
        .ok_or("duplicate model unexpectedly persisted")?;
    assert_eq!(duplicate.kind(), StorageErrorKind::AlreadyExists);
    storage.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_should_enforce_bounds_cancellation_health_and_restart_lifecycle()
-> Result<(), Box<dyn Error>> {
    let mut storage = MemoryStorage::start(MemoryStorageConfig::default())?;
    let diagnostics = storage.diagnostics();
    let context = operation_context()?;
    let store_id = create_store(&storage, &context).await?;
    assert!(storage.health(&context).await?.is_ready());
    assert!(diagnostics.is_running());

    let tuple_one = tuple("document:one#viewer@user:anne")?;
    let tuple_two = tuple("document:one#viewer@user:bob")?;
    storage
        .write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![tuple_one, tuple_two],
            TupleWriteOptions::default(),
        )
        .await?;
    let filter = ObjectRelationFilter::new(
        "document:one".parse()?,
        "viewer".parse()?,
        Vec::new(),
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    let bounded = storage
        .read_object_relation(&context, store_id, &filter, read_options(1)?)
        .await
        .err()
        .ok_or("oversized snapshot unexpectedly truncated")?;
    assert_eq!(bounded.kind(), StorageErrorKind::ResourceExhausted);

    let cancellation = StorageCancellationToken::new();
    cancellation.cancel();
    let cancelled_context = OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        future_deadline()?,
        cancellation,
    );
    let cancelled = storage
        .health(&cancelled_context)
        .await
        .err()
        .ok_or("cancelled operation unexpectedly ran")?;
    assert_eq!(cancelled.kind(), StorageErrorKind::Cancelled);
    assert_eq!(diagnostics.active_operations(), 0);

    storage.stop().await?;
    assert!(!diagnostics.is_running());
    storage.restart().await?;
    assert!(diagnostics.is_running());
    let missing = storage
        .read_store(&context, store_id)
        .await
        .err()
        .ok_or("restart retained state")?;
    assert_eq!(missing.kind(), StorageErrorKind::NotFound);
    storage.stop().await?;
    Ok(())
}

#[test]
fn test_should_expose_every_capability_as_a_dyn_safe_trait() {
    fn assert_capabilities<T>()
    where
        T: TupleReader
            + TupleWriter
            + ModelReader
            + ModelWriter
            + StoreReader
            + StoreWriter
            + AssertionReader
            + AssertionWriter
            + ChangeReader
            + HealthCheck
            + Send
            + Sync,
    {
    }

    assert_capabilities::<MemoryStorage>();
}

async fn assert_tuple_indexes(
    storage: &MemoryStorage,
    context: &OperationContext,
    store_id: StoreId,
    direct: &RelationshipTuple,
    userset: &RelationshipTuple,
    wildcard: &RelationshipTuple,
    timestamp: SystemTime,
) -> Result<ObjectRelationFilter, Box<dyn Error>> {
    let forward_filter = ObjectRelationFilter::new(
        "document:roadmap".parse()?,
        "viewer".parse()?,
        Vec::new(),
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    let mut forward = storage
        .read_object_relation(context, store_id, &forward_filter, read_options(10)?)
        .await?;
    assert_eq!(forward.by_ref().count(), 3);
    forward.close();
    forward.close();
    assert!(forward.is_closed());

    let userset_filter = UsersetTupleFilter::new(
        "document:roadmap".parse()?,
        "viewer".parse()?,
        vec![UsersetRestrictionFilter::new(
            "group".parse()?,
            "member".parse()?,
        )],
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    let mut usersets = storage
        .read_userset_tuples(context, store_id, &userset_filter, read_options(10)?)
        .await?;
    let first_userset = usersets
        .next_item()
        .transpose()?
        .ok_or("userset index returned no tuple")?;
    assert_eq!(first_userset.key(), userset.key());
    assert!(usersets.next_item().is_none());

    let reverse_filter = ReverseTupleFilter::new(
        "document".parse()?,
        "viewer".parse()?,
        vec!["user:anne".parse()?],
        vec!["roadmap".parse()?],
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    let mut reverse = storage
        .read_reverse_tuples(context, store_id, &reverse_filter, read_options(10)?)
        .await?;
    let first_reverse = reverse
        .next_item()
        .transpose()?
        .ok_or("reverse index returned no tuple")?;
    assert_eq!(first_reverse.key(), direct.key());
    assert!(reverse.next_item().is_none());

    for key in [direct.key(), userset.key(), wildcard.key()] {
        assert_eq!(
            storage
                .read_exact_tuple(context, store_id, key)
                .await?
                .inserted_at(),
            timestamp,
        );
    }
    Ok(forward_filter)
}

async fn assert_store_pagination(
    storage: &MemoryStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    for additional_store in [MODEL_ONE, MODEL_TWO] {
        storage
            .create_store(
                context,
                additional_store.parse()?,
                StoreName::new("additional".to_owned())?,
            )
            .await?;
    }
    let first = storage
        .list_stores(context, &StoreFilter::all(), &page_options(2, None)?)
        .await?;
    let named = storage
        .list_stores(
            context,
            &StoreFilter::named(StoreName::new("additional".to_owned())?),
            &page_options(10, None)?,
        )
        .await?;
    assert_eq!(named.items().len(), 2);
    let first_ids: Vec<_> = first
        .items()
        .iter()
        .map(|store| store.id().to_string())
        .collect();
    assert_eq!(first_ids, [STORE_ID, MODEL_ONE]);
    let second = storage
        .list_stores(
            context,
            &StoreFilter::all(),
            &page_options(2, first.continuation().cloned())?,
        )
        .await?;
    let second_ids: Vec<_> = second
        .items()
        .iter()
        .map(|store| store.id().to_string())
        .collect();
    assert_eq!(second_ids, [MODEL_TWO]);
    assert!(second.continuation().is_none());

    let invalid_cursor = openfga_storage::StorageCursor::new(b"not-a-ulid".to_vec())?;
    let invalid = storage
        .list_stores(
            context,
            &StoreFilter::all(),
            &page_options(2, Some(invalid_cursor))?,
        )
        .await
        .err()
        .ok_or("invalid store cursor unexpectedly accepted")?;
    assert_eq!(invalid.kind(), StorageErrorKind::InvalidContinuation);
    Ok(())
}

async fn create_store(
    storage: &MemoryStorage,
    context: &OperationContext,
) -> Result<StoreId, Box<dyn Error>> {
    let store_id = STORE_ID.parse::<StoreId>()?;
    storage
        .create_store(context, store_id, StoreName::new("engineering".to_owned())?)
        .await?;
    Ok(store_id)
}

fn storage_with(
    timestamp: SystemTime,
    faults: Arc<dyn MutationFaultInjector>,
) -> Result<MemoryStorage, StorageError> {
    MemoryStorage::start_with_components(
        MemoryStorageConfig::default(),
        Arc::new(FixedClock(timestamp)),
        faults,
    )
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

fn read_options(maximum: u32) -> Result<ReadOptions, Box<dyn Error>> {
    let maximum = NonZeroU32::new(maximum).ok_or("zero test read limit")?;
    Ok(ReadOptions::new(maximum, &InputLimits::default())?)
}

fn page_options(
    maximum: u32,
    after: Option<openfga_storage::StorageCursor>,
) -> Result<PageOptions, Box<dyn Error>> {
    let maximum = NonZeroU32::new(maximum).ok_or("zero test page limit")?;
    Ok(PageOptions::new(maximum, after, &InputLimits::default())?)
}

fn tuple(value: &str) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::unconditional(value.parse::<TupleKey>()?))
}

fn stored_model(
    store_id: StoreId,
    model_id: &str,
    timestamp: SystemTime,
) -> Result<Arc<StoredAuthorizationModel>, Box<dyn Error>> {
    let source = Arc::new(AuthorizationModelSource::new(
        store_id,
        model_id.parse()?,
        "1.1".to_owned(),
        vec![
            TypeDefinitionSource::new("user".parse::<TypeName>()?, Vec::new()),
            TypeDefinitionSource::new(
                "document".parse::<TypeName>()?,
                vec![RelationSource::new(
                    "viewer".parse::<RelationName>()?,
                    RewriteSource::Direct,
                    vec![DirectRestrictionSource::new(
                        "user".parse::<TypeName>()?,
                        RestrictionKindSource::Object,
                        None,
                    )],
                )],
            ),
        ],
        Vec::new(),
    ));
    let compiled = ModelCompiler::default().compile(&source)?;
    Ok(Arc::new(StoredAuthorizationModel::new(
        source, compiled, timestamp,
    )?))
}

#[derive(Debug)]
struct FixedClock(SystemTime);

impl StorageClock for FixedClock {
    fn now(&self) -> Result<SystemTime, StorageError> {
        Ok(self.0)
    }
}

#[derive(Debug)]
struct NeverFail;

impl MutationFaultInjector for NeverFail {
    fn check(&self, _stage: MutationFaultStage) -> Result<(), StorageError> {
        Ok(())
    }
}

#[derive(Debug)]
struct FailAt(AtomicU8);

impl FailAt {
    fn new(stage: MutationFaultStage) -> Self {
        Self(AtomicU8::new(stage_code(stage)))
    }
}

impl MutationFaultInjector for FailAt {
    fn check(&self, stage: MutationFaultStage) -> Result<(), StorageError> {
        if self.0.load(Ordering::Acquire) == stage_code(stage) {
            Err(StorageError::new(
                StorageErrorKind::Internal,
                "injected_memory_mutation_failure",
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct CancelAt {
    stage: MutationFaultStage,
    cancellation: StorageCancellationToken,
}

impl MutationFaultInjector for CancelAt {
    fn check(&self, stage: MutationFaultStage) -> Result<(), StorageError> {
        if stage_code(self.stage) == stage_code(stage) {
            self.cancellation.cancel();
        }
        Ok(())
    }
}

const fn stage_code(stage: MutationFaultStage) -> u8 {
    match stage {
        MutationFaultStage::Validated => 1,
        MutationFaultStage::DeletesPrepared => 2,
        MutationFaultStage::WritesPrepared => 3,
        MutationFaultStage::ChangesPrepared => 4,
        _ => u8::MAX,
    }
}
