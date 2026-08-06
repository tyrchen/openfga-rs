//! End-to-end management use cases over the actor-owned memory backend.

use std::{
    error::Error,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use openfga_domain::{
    AuthorizationModelId, ConditionContext, ConsistencyPreference, ContextualTuples, Deadline,
    InputLimits, ModelSelection, RelationshipTuple, RequestTimeout, StoreId, TupleKey,
};
use openfga_model::{
    AuthorizationModelDefinition, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_service::{
    AssertionService, ChangeService, IdentifierSource, IdentifierSourceError, ModelPublication,
    ModelService, ServiceClock, ServiceErrorKind, StoreService, TupleService,
};
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ChangeFilter, ChangeReader, ModelReader,
    ModelWriter, OperationContext, PageOptions, StorageCancellationToken, StoreFilter, StoreName,
    StoreReader, StoreWriter, TupleReadFilter, TupleReader, TupleWriteOptions, TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

#[tokio::test]
async fn test_should_execute_every_management_use_case_with_stable_pages()
-> Result<(), Box<dyn Error>> {
    let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
    let identifiers: Arc<dyn IdentifierSource> = Arc::new(FixedIdentifiers {
        store_id: STORE_ID.parse()?,
        model_id: MODEL_ID.parse()?,
    });
    let stores: Arc<dyn StoreReader> = storage.clone();
    let store_writes: Arc<dyn StoreWriter> = storage.clone();
    let models: Arc<dyn ModelReader> = storage.clone();
    let model_writes: Arc<dyn ModelWriter> = storage.clone();
    let tuples: Arc<dyn TupleReader> = storage.clone();
    let tuple_writes: Arc<dyn TupleWriter> = storage.clone();
    let assertion_reads: Arc<dyn AssertionReader> = storage.clone();
    let assertion_writes: Arc<dyn AssertionWriter> = storage.clone();
    let changes: Arc<dyn ChangeReader> = storage.clone();
    let context = operation_context()?;

    {
        let store_service = StoreService::new(
            Arc::clone(&stores),
            Arc::clone(&store_writes),
            Arc::clone(&identifiers),
        );
        let model_service = ModelService::new(
            Arc::clone(&stores),
            Arc::clone(&models),
            Arc::clone(&model_writes),
            ModelPublication::new(
                Arc::clone(&identifiers),
                Arc::new(FixedClock),
                ModelCompiler::default(),
            ),
        );
        let tuple_service = TupleService::new(
            Arc::clone(&stores),
            Arc::clone(&models),
            Arc::clone(&tuples),
            Arc::clone(&tuple_writes),
            InputLimits::default(),
        );
        let assertion_service = AssertionService::new(
            Arc::clone(&stores),
            Arc::clone(&models),
            Arc::clone(&assertion_reads),
            Arc::clone(&assertion_writes),
            InputLimits::default(),
        );
        let change_service = ChangeService::new(Arc::clone(&stores), Arc::clone(&changes));

        let (store_id, model_id) =
            exercise_store_and_model(&store_service, &model_service, &context).await?;
        exercise_tuples_assertions_and_changes(
            &tuple_service,
            &assertion_service,
            &change_service,
            &context,
            store_id,
            model_id,
        )
        .await?;
        exercise_delete(&store_service, &context, store_id).await?;
    }

    drop(changes);
    drop(assertion_writes);
    drop(assertion_reads);
    drop(tuple_writes);
    drop(tuples);
    drop(model_writes);
    drop(models);
    drop(store_writes);
    drop(stores);
    drop(identifiers);
    shutdown(storage).await
}

async fn exercise_store_and_model(
    stores: &StoreService,
    models: &ModelService,
    context: &OperationContext,
) -> Result<(StoreId, AuthorizationModelId), Box<dyn Error>> {
    let store = stores
        .create(context, StoreName::new("engineering".to_owned())?)
        .await?;
    assert_eq!(store.id().to_string(), STORE_ID);
    assert_eq!(
        stores.get(context, store.id()).await?.name().as_str(),
        "engineering"
    );
    assert_eq!(
        stores
            .update(
                context,
                store.id(),
                StoreName::new("authorization".to_owned())?,
            )
            .await?
            .name()
            .as_str(),
        "authorization",
    );
    assert_eq!(
        stores
            .list(context, &StoreFilter::all(), &page_options(1, None)?)
            .await?
            .items()
            .len(),
        1
    );

    let invalid = models
        .write(context, store.id(), invalid_model_definition()?)
        .await
        .err()
        .ok_or("invalid model unexpectedly published")?;
    assert_eq!(invalid.kind(), ServiceErrorKind::InvalidRequest);
    assert!(
        models
            .list(context, store.id(), &page_options(1, None)?)
            .await?
            .items()
            .is_empty()
    );

    let model = models
        .write(context, store.id(), model_definition()?)
        .await?;
    assert_eq!(model.model_id().to_string(), MODEL_ID);
    assert_eq!(
        models
            .read(context, store.id(), *model.model_id())
            .await?
            .compiled()
            .fingerprint(),
        model.compiled().fingerprint(),
    );
    assert_eq!(
        models
            .list(context, store.id(), &page_options(1, None)?)
            .await?
            .items()
            .len(),
        1,
    );
    Ok((store.id(), *model.model_id()))
}

async fn exercise_tuples_assertions_and_changes(
    tuples: &TupleService,
    assertions: &AssertionService,
    changes: &ChangeService,
    context: &OperationContext,
    store_id: StoreId,
    model_id: AuthorizationModelId,
) -> Result<(), Box<dyn Error>> {
    let anne = tuple("document:roadmap#viewer@user:anne")?;
    let bob = tuple("document:roadmap#viewer@user:bob")?;
    let outcome = tuples
        .write(
            context,
            store_id,
            ModelSelection::Latest,
            Vec::new(),
            vec![anne.clone(), bob],
            TupleWriteOptions::default(),
        )
        .await?;
    assert_eq!(outcome.change_ids().len(), 2);
    let empty = tuples
        .write(
            context,
            store_id,
            ModelSelection::Latest,
            Vec::new(),
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await
        .err()
        .ok_or("empty tuple write unexpectedly succeeded")?;
    assert_eq!(empty.kind(), ServiceErrorKind::InvalidRequest);
    let duplicate = tuples
        .write(
            context,
            store_id,
            ModelSelection::Latest,
            vec![anne.key().clone()],
            vec![anne.clone()],
            TupleWriteOptions::default(),
        )
        .await
        .err()
        .ok_or("duplicate tuple write unexpectedly succeeded")?;
    assert_eq!(duplicate.kind(), ServiceErrorKind::InvalidRequest);
    assert_two_tuple_pages(tuples, context, store_id).await?;

    exercise_assertions(assertions, context, store_id, model_id, &anne).await?;
    assert_two_change_pages(changes, context, store_id).await?;

    let invalid = tuples
        .write(
            context,
            store_id,
            ModelSelection::Latest,
            Vec::new(),
            vec![tuple("document:roadmap#viewer@group:engineering")?],
            TupleWriteOptions::default(),
        )
        .await
        .err()
        .ok_or("model-incompatible tuple unexpectedly persisted")?;
    assert_eq!(invalid.kind(), ServiceErrorKind::InvalidRequest);

    let nonexistent_model = "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse()?;
    let deleted = tuples
        .write(
            context,
            store_id,
            ModelSelection::Explicit(nonexistent_model),
            vec![anne.key().clone()],
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await?;
    assert_eq!(deleted.change_ids().len(), 1);
    Ok(())
}

async fn exercise_assertions(
    assertions: &AssertionService,
    context: &OperationContext,
    store_id: StoreId,
    model_id: AuthorizationModelId,
    anne: &RelationshipTuple,
) -> Result<(), Box<dyn Error>> {
    let assertion = Assertion::new(
        anne.key().clone(),
        true,
        ContextualTuples::empty(),
        ConditionContext::empty(),
    );
    let resolved_id = assertions
        .write(
            context,
            store_id,
            ModelSelection::Latest,
            vec![assertion.clone()],
        )
        .await?;
    assert_eq!(resolved_id, model_id);
    let stored = assertions
        .read(context, store_id, ModelSelection::Explicit(model_id))
        .await?;
    assert_eq!(stored.assertions().as_ref(), &[assertion]);
    let invalid_context = ContextualTuples::new(
        vec![tuple("document:roadmap#viewer@group:engineering")?],
        &InputLimits::default(),
    )?;
    let invalid_assertion = Assertion::new(
        anne.key().clone(),
        true,
        invalid_context,
        ConditionContext::empty(),
    );
    let invalid = assertions
        .write(
            context,
            store_id,
            ModelSelection::Latest,
            vec![invalid_assertion],
        )
        .await
        .err()
        .ok_or("invalid contextual assertion unexpectedly persisted")?;
    assert_eq!(invalid.kind(), ServiceErrorKind::InvalidRequest);
    Ok(())
}

async fn assert_two_tuple_pages(
    service: &TupleService,
    context: &OperationContext,
    store_id: StoreId,
) -> Result<(), Box<dyn Error>> {
    let first = service
        .read(
            context,
            store_id,
            &TupleReadFilter::all(),
            &page_options(1, None)?,
        )
        .await?;
    assert_eq!(first.items().len(), 1);
    let cursor = first
        .continuation()
        .cloned()
        .ok_or("missing tuple continuation")?;
    let second = service
        .read(
            context,
            store_id,
            &TupleReadFilter::all(),
            &page_options(1, Some(cursor))?,
        )
        .await?;
    assert_eq!(second.items().len(), 1);
    assert!(second.continuation().is_none());
    Ok(())
}

async fn assert_two_change_pages(
    service: &ChangeService,
    context: &OperationContext,
    store_id: StoreId,
) -> Result<(), Box<dyn Error>> {
    let first = service
        .read(
            context,
            store_id,
            &ChangeFilter::default(),
            &page_options(1, None)?,
        )
        .await?;
    assert_eq!(first.items().len(), 1);
    let cursor = first
        .continuation()
        .cloned()
        .ok_or("missing change continuation")?;
    let second = service
        .read(
            context,
            store_id,
            &ChangeFilter::default(),
            &page_options(1, Some(cursor))?,
        )
        .await?;
    assert_eq!(second.items().len(), 1);
    assert!(second.continuation().is_none());
    Ok(())
}

async fn exercise_delete(
    service: &StoreService,
    context: &OperationContext,
    store_id: StoreId,
) -> Result<(), Box<dyn Error>> {
    service.delete(context, store_id).await?;
    let missing = service
        .get(context, store_id)
        .await
        .err()
        .ok_or("deleted store unexpectedly remained readable")?;
    assert_eq!(missing.kind(), ServiceErrorKind::StoreNotFound);
    Ok(())
}

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
        UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }
}

fn model_definition() -> Result<AuthorizationModelDefinition, Box<dyn Error>> {
    Ok(AuthorizationModelDefinition::new(
        "1.1".to_owned(),
        vec![
            TypeDefinitionSource::new("user".parse()?, Vec::new()),
            TypeDefinitionSource::new("group".parse()?, Vec::new()),
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

fn invalid_model_definition() -> Result<AuthorizationModelDefinition, Box<dyn Error>> {
    Ok(AuthorizationModelDefinition::new(
        "9.9".to_owned(),
        vec![TypeDefinitionSource::new("user".parse()?, Vec::new())],
        Vec::new(),
    ))
}

fn tuple(value: &str) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::unconditional(value.parse::<TupleKey>()?))
}

fn operation_context() -> Result<OperationContext, Box<dyn Error>> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(Instant::now(), RequestTimeout::new(Duration::from_secs(5))?)?,
        StorageCancellationToken::new(),
    ))
}

fn page_options(
    maximum: u32,
    continuation: Option<openfga_storage::StorageCursor>,
) -> Result<PageOptions, Box<dyn Error>> {
    let maximum = NonZeroU32::new(maximum).ok_or("zero page size")?;
    Ok(PageOptions::new(
        maximum,
        continuation,
        &InputLimits::default(),
    )?)
}

async fn shutdown(storage: Arc<MemoryStorage>) -> Result<(), Box<dyn Error>> {
    let mut owner = Arc::try_unwrap(storage).map_err(|_| "memory storage still shared")?;
    owner.stop().await?;
    Ok(())
}
