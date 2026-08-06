//! End-to-end evaluator semantics over the actor-owned memory backend.

use std::{
    collections::BTreeMap,
    error::Error,
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use openfga_check::{CheckBudget, CheckErrorKind, CheckEvaluator, DirectCheckEvaluator};
use openfga_condition::{ConditionDefinition, ParameterType};
use openfga_domain::{
    AuthorizationModelId, BatchCheckCommand, BatchCheckItem, BatchCheckItems, CheckCommand,
    ConditionBinding, ConditionContext, ConditionReference, ConsistencyPreference,
    ContextualTuples, Deadline, InputLimits, Limit, ModelSelection, Principal, PrincipalKind,
    QueryContext, RelationshipTuple, StoreId, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, ConditionSource, DirectRestrictionSource, ModelCompiler,
    RelationSource, RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    ObjectRelationFilter, OperationContext, ReadOptions, ReverseTupleFilter,
    StorageCancellationToken, StorageError, StorageErrorKind, StoreName, StoreWriter, StoredTuple,
    TupleReader, TupleStream, TupleWriteOptions, TupleWriter, UsersetTupleFilter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use serde_json::json;
use tokio::sync::Barrier;

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

#[tokio::test]
async fn test_should_resolve_all_rewrites_usersets_wildcards_and_cycles()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:direct#viewer@user:alice")?,
            tuple("document:wild#viewer@user:*")?,
            tuple("document:computed#owner@user:alice")?,
            tuple("document:userset#viewer@group:eng#member")?,
            tuple("group:eng#member@user:alice")?,
            tuple("document:ttu#parent@folder:roadmap")?,
            tuple("folder:roadmap#viewer@user:alice")?,
            tuple("document:both#owner@user:alice")?,
            tuple("document:both#editor@user:alice")?,
            tuple("document:excluded#viewer@user:alice")?,
            tuple("document:excluded#banned@user:alice")?,
            tuple("document:included#viewer@user:alice")?,
            tuple("document:cycle#cycle_b@user:alice")?,
        ],
    )
    .await?;
    for query in [
        "document:direct#viewer@user:alice",
        "document:wild#viewer@user:bob",
        "document:computed#viewer@user:alice",
        "document:userset#viewer@user:alice",
        "document:ttu#viewer@user:alice",
        "document:both#both@user:alice",
        "document:included#allowed@user:alice",
        "document:cycle#cycle_a@user:alice",
    ] {
        let outcome = evaluate(
            query,
            ContextualTuples::empty(),
            ConditionContext::empty(),
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
        assert!(outcome.allowed(), "expected allow for {query}");
    }

    for query in [
        "document:direct#viewer@user:bob",
        "document:both#both@user:bob",
        "document:excluded#allowed@user:alice",
        "document:missing#cycle_a@user:alice",
    ] {
        let outcome = evaluate(
            query,
            ContextualTuples::empty(),
            ConditionContext::empty(),
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
        assert!(!outcome.allowed(), "expected deny for {query}");
        if query.contains("cycle_a") {
            assert!(outcome.metadata().cycles() > 0);
        }
    }

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_overlay_conditions_and_suppress_losing_union_errors()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let empty = ConditionContext::empty();
    let tuple_override =
        ConditionContext::try_from_json(json!({"x": 50}), &InputLimits::default())?;
    let request_false =
        ConditionContext::try_from_json(json!({"x": 200}), &InputLimits::default())?;
    write_tuples(
        storage.as_ref(),
        vec![
            conditional_tuple("document:override#conditional@user:alice", tuple_override)?,
            conditional_tuple("document:missing#conditional@user:alice", empty.clone())?,
            conditional_tuple("document:guarded#conditional@user:alice", empty)?,
            tuple("document:guarded#owner@user:alice")?,
        ],
    )
    .await?;
    let override_outcome = evaluate(
        "document:override#conditional@user:alice",
        ContextualTuples::empty(),
        request_false,
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(override_outcome.allowed());
    assert!(override_outcome.metadata().condition_cost() > 0);

    let condition_budget = CheckBudget::builder()
        .condition_cost(Limit::<1_000_000>::new(1)?)
        .build();
    let budget_error = evaluate(
        "document:override#conditional@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        condition_budget,
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("condition cost exhaustion unexpectedly completed")?;
    assert_eq!(budget_error.kind(), CheckErrorKind::ConditionCostExceeded);

    let missing = evaluate(
        "document:missing#conditional@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("missing condition parameters unexpectedly denied")?;
    assert_eq!(missing.kind(), CheckErrorKind::Condition);

    let guarded = evaluate(
        "document:guarded#guarded@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        model,
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(guarded.allowed());

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_merge_contextual_tuples_and_reject_invalid_contextual_shapes()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let contextual = ContextualTuples::new(
        vec![tuple("document:context#viewer@user:alice")?],
        &InputLimits::default(),
    )?;
    let outcome = evaluate(
        "document:context#viewer@user:alice",
        contextual,
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(outcome.allowed());
    assert_eq!(outcome.metadata().tuple_items(), 1);

    let invalid = ContextualTuples::new(
        vec![tuple("document:context#parent@user:alice")?],
        &InputLimits::default(),
    )?;
    let error = evaluate(
        "document:context#viewer@user:alice",
        invalid,
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("invalid contextual tuple unexpectedly accepted")?;
    assert_eq!(error.kind(), CheckErrorKind::InvalidTuple);

    let error = evaluate(
        "document:context#undeclared@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        model,
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("undeclared query relation unexpectedly accepted")?;
    assert_eq!(error.kind(), CheckErrorKind::InvalidModel);

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_enforce_independent_budgets_and_skip_unreachable_reads()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    write_tuples(
        storage.as_ref(),
        vec![
            tuple("document:many#owner@user:alice")?,
            tuple("document:many#owner@user:bob")?,
            tuple("document:deep#viewer@group:g0#member")?,
            tuple("group:g0#member@group:g1#member")?,
            tuple("group:g1#member@user:alice")?,
        ],
    )
    .await?;
    let unreachable = evaluate(
        "document:none#viewer@service:worker",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(!unreachable.allowed());
    assert_eq!(unreachable.metadata().datastore_queries(), 0);

    let dispatch_budget = CheckBudget::builder()
        .dispatches(Limit::<1_000_000>::new(1)?)
        .build();
    assert_budget_error(
        "document:none#owner@user:alice",
        Arc::clone(&model),
        storage.clone(),
        dispatch_budget,
        CheckErrorKind::DispatchExceeded,
    )
    .await?;

    let datastore_budget = CheckBudget::builder()
        .datastore_queries(Limit::<100_000>::new(1)?)
        .build();
    assert_budget_error(
        "document:none#viewer@user:alice",
        Arc::clone(&model),
        storage.clone(),
        datastore_budget,
        CheckErrorKind::DatastoreQueryExceeded,
    )
    .await?;

    let tuple_budget = CheckBudget::builder()
        .tuple_items(Limit::<1_000_000>::new(1)?)
        .build();
    assert_budget_error(
        "document:many#owner@user:nobody",
        Arc::clone(&model),
        storage.clone(),
        tuple_budget,
        CheckErrorKind::TupleItemExceeded,
    )
    .await?;

    let depth_budget = CheckBudget::builder()
        .depth(Limit::<1_000>::new(1)?)
        .build();
    assert_budget_error(
        "document:deep#viewer@user:alice",
        model,
        storage.clone(),
        depth_budget,
        CheckErrorKind::DepthExceeded,
    )
    .await?;

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_enforce_root_cancellation_and_deadline() -> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let evaluator = DirectCheckEvaluator::default();
    let cancellation = StorageCancellationToken::new();
    cancellation.cancel();
    let error = evaluate(
        "document:none#owner@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        Arc::clone(&model),
        storage.clone(),
        CheckBudget::default(),
        cancellation,
    )
    .await
    .err()
    .ok_or("cancelled check unexpectedly completed")?;
    assert_eq!(error.kind(), CheckErrorKind::Cancelled);

    let expired_deadline = Deadline::from_timeout(
        Instant::now(),
        openfga_domain::RequestTimeout::new(Duration::from_millis(1))?,
    )?;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let expired = CheckCommand::new(
        query_context_at(
            ContextualTuples::empty(),
            ConditionContext::empty(),
            expired_deadline,
        )?,
        "document:none#owner@user:alice".parse()?,
    );
    let error = evaluator
        .check(
            &expired,
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::default(),
            StorageCancellationToken::new(),
        )
        .await
        .err()
        .ok_or("expired check unexpectedly completed")?;
    assert_eq!(error.kind(), CheckErrorKind::Timeout);

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_bound_batch_concurrency_order_results_and_isolate_item_errors()
-> Result<(), Box<dyn Error>> {
    let storage = memory_storage().await?;
    let model = ModelCompiler::default().compile(&complete_model()?)?;
    let evaluator = DirectCheckEvaluator::default();

    let first_context = ContextualTuples::new(
        vec![tuple("document:first#owner@user:alice")?],
        &InputLimits::default(),
    )?;
    let items = BatchCheckItems::new(
        vec![
            BatchCheckItem::new(
                "first".parse()?,
                "document:first#owner@user:alice".parse()?,
                first_context,
                ConditionContext::empty(),
            ),
            BatchCheckItem::new(
                "second".parse()?,
                "document:second#owner@user:alice".parse()?,
                ContextualTuples::empty(),
                ConditionContext::empty(),
            ),
        ],
        &InputLimits::default(),
    )?;
    let command = BatchCheckCommand::new(
        query_context(ContextualTuples::empty(), ConditionContext::empty())?,
        items,
    );
    let batch = evaluator
        .batch_check(
            &command,
            Arc::clone(&model),
            storage.clone(),
            CheckBudget::builder()
                .batch_concurrency(Limit::<1_000>::new(1)?)
                .build(),
            StorageCancellationToken::new(),
        )
        .await?;
    assert_eq!(batch.results().len(), 2);
    let first = batch
        .results()
        .first()
        .ok_or("first batch result missing")?;
    assert_eq!(first.correlation_id().as_str(), "first");
    assert!(matches!(first.outcome(), Ok(outcome) if outcome.allowed()));
    assert!(!format!("{first:?}").contains("first"));
    let second = batch
        .results()
        .get(1)
        .ok_or("second batch result missing")?;
    assert_eq!(second.correlation_id().as_str(), "second");
    assert!(matches!(second.outcome(), Ok(outcome) if !outcome.allowed()));

    let low_limits = InputLimits::builder()
        .context_bytes(Limit::<32_768>::new(12)?)
        .build();
    let base_context = ConditionContext::try_from_json(json!({"a": 1}), &low_limits)?;
    let item_context = ConditionContext::try_from_json(json!({"b": 2}), &low_limits)?;
    let item = BatchCheckItem::new(
        "overlay".parse()?,
        "document:none#owner@user:alice".parse()?,
        ContextualTuples::empty(),
        item_context,
    );
    let command = BatchCheckCommand::new(
        query_context(ContextualTuples::empty(), base_context)?,
        BatchCheckItems::new(vec![item], &low_limits)?,
    );
    let overlay = DirectCheckEvaluator::new(low_limits)
        .batch_check(
            &command,
            model,
            storage.clone(),
            CheckBudget::default(),
            StorageCancellationToken::new(),
        )
        .await?;
    let item = overlay
        .results()
        .first()
        .ok_or("overlay batch result missing")?;
    assert!(matches!(item.outcome(), Err(error) if error.kind() == CheckErrorKind::InvalidTuple));

    shutdown(storage).await?;
    Ok(())
}

#[tokio::test]
async fn test_should_abort_and_join_short_circuited_datastore_reads() -> Result<(), Box<dyn Error>>
{
    let model = ModelCompiler::default().compile(&short_circuit_model()?)?;
    let reader = Arc::new(ShortCircuitReader::new(tuple(
        "document:one#viewer@user:alice",
    )?));
    let outcome = evaluate(
        "document:one#viewer@user:alice",
        ContextualTuples::empty(),
        ConditionContext::empty(),
        model,
        reader.clone(),
        CheckBudget::default(),
        StorageCancellationToken::new(),
    )
    .await?;
    assert!(outcome.allowed());
    assert_eq!(reader.calls.load(Ordering::Acquire), 2);
    assert_eq!(reader.active.load(Ordering::Acquire), 0);
    Ok(())
}

async fn assert_budget_error(
    query: &str,
    model: Arc<openfga_model::CompiledModel>,
    storage: Arc<MemoryStorage>,
    budget: CheckBudget,
    expected: CheckErrorKind,
) -> Result<(), Box<dyn Error>> {
    let error = evaluate(
        query,
        ContextualTuples::empty(),
        ConditionContext::empty(),
        model,
        storage,
        budget,
        StorageCancellationToken::new(),
    )
    .await
    .err()
    .ok_or("budgeted check unexpectedly completed")?;
    assert_eq!(error.kind(), expected);
    Ok(())
}

async fn evaluate(
    query: &str,
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
    model: Arc<openfga_model::CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: CheckBudget,
    cancellation: StorageCancellationToken,
) -> Result<openfga_check::CheckOutcome, openfga_check::CheckError> {
    let command = CheckCommand::new(
        query_context(contextual_tuples, condition_context)
            .map_err(|_| openfga_check::CheckErrorKind::Internal)
            .map_err(|_| StorageError::new(StorageErrorKind::Internal, "test_query_context"))
            .map_err(openfga_check::CheckError::from)?,
        query
            .parse()
            .map_err(|_| StorageError::new(StorageErrorKind::Internal, "test_tuple"))
            .map_err(openfga_check::CheckError::from)?,
    );
    DirectCheckEvaluator::default()
        .check(&command, model, tuples, budget, cancellation)
        .await
}

fn query_context(
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
) -> Result<QueryContext, Box<dyn Error>> {
    query_context_at(
        contextual_tuples,
        condition_context,
        Deadline::from_timeout(
            Instant::now(),
            openfga_domain::RequestTimeout::new(Duration::from_secs(5))?,
        )?,
    )
}

fn query_context_at(
    contextual_tuples: ContextualTuples,
    condition_context: ConditionContext,
    deadline: Deadline,
) -> Result<QueryContext, Box<dyn Error>> {
    Ok(QueryContext::builder()
        .store_id(STORE_ID.parse::<StoreId>()?)
        .model_selection(ModelSelection::Explicit(
            MODEL_ID.parse::<AuthorizationModelId>()?,
        ))
        .consistency(ConsistencyPreference::HigherConsistency)
        .contextual_tuples(contextual_tuples)
        .condition_context(condition_context)
        .deadline(deadline)
        .principal(Principal::new(
            PrincipalKind::Internal,
            "phase1-tests".parse()?,
        ))
        .build())
}

async fn memory_storage() -> Result<Arc<MemoryStorage>, Box<dyn Error>> {
    let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
    let context = operation_context()?;
    storage
        .create_store(
            &context,
            STORE_ID.parse()?,
            StoreName::new("check-tests".to_owned())?,
        )
        .await?;
    Ok(storage)
}

async fn shutdown(storage: Arc<MemoryStorage>) -> Result<(), Box<dyn Error>> {
    let mut owner = Arc::try_unwrap(storage).map_err(|_| "memory storage still shared")?;
    owner.stop().await?;
    Ok(())
}

async fn write_tuples(
    storage: &MemoryStorage,
    tuples: Vec<RelationshipTuple>,
) -> Result<(), Box<dyn Error>> {
    let context = operation_context()?;
    storage
        .write_tuples(
            &context,
            STORE_ID.parse()?,
            Vec::new(),
            tuples,
            TupleWriteOptions::default(),
        )
        .await?;
    Ok(())
}

fn operation_context() -> Result<OperationContext, Box<dyn Error>> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(
            Instant::now(),
            openfga_domain::RequestTimeout::new(Duration::from_secs(5))?,
        )?,
        StorageCancellationToken::new(),
    ))
}

fn tuple(value: &str) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::unconditional(value.parse()?))
}

fn conditional_tuple(
    value: &str,
    context: ConditionContext,
) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::new(
        value.parse()?,
        ConditionReference::Conditional(ConditionBinding::new("under_limit".parse()?, context)),
    ))
}

fn complete_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    let user = type_source("user", Vec::new())?;
    let service = type_source("service", Vec::new())?;
    let group = type_source(
        "group",
        vec![relation(
            "member",
            RewriteSource::Direct,
            vec![object("user", None)?, userset("group", "member", None)?],
        )?],
    )?;
    let folder = type_source(
        "folder",
        vec![relation(
            "viewer",
            RewriteSource::Direct,
            vec![object("user", None)?, wildcard("user")?],
        )?],
    )?;
    let document = type_source(
        "document",
        vec![
            relation("owner", RewriteSource::Direct, vec![object("user", None)?])?,
            relation("editor", RewriteSource::Direct, vec![object("user", None)?])?,
            relation("banned", RewriteSource::Direct, vec![object("user", None)?])?,
            relation(
                "conditional",
                RewriteSource::Direct,
                vec![object("user", Some("under_limit"))?],
            )?,
            relation(
                "parent",
                RewriteSource::Direct,
                vec![object("folder", None)?],
            )?,
            relation(
                "viewer",
                RewriteSource::Union(vec![
                    RewriteSource::Direct,
                    computed("owner")?,
                    ttu("parent", "viewer")?,
                ]),
                vec![
                    object("user", None)?,
                    wildcard("user")?,
                    userset("group", "member", None)?,
                ],
            )?,
            relation(
                "guarded",
                RewriteSource::Union(vec![computed("conditional")?, computed("owner")?]),
                Vec::new(),
            )?,
            relation(
                "both",
                RewriteSource::Intersection(vec![computed("owner")?, computed("editor")?]),
                Vec::new(),
            )?,
            relation(
                "allowed",
                RewriteSource::Difference {
                    base: Box::new(computed("viewer")?),
                    subtract: Box::new(computed("banned")?),
                },
                Vec::new(),
            )?,
            relation(
                "cycle_a",
                RewriteSource::Union(vec![RewriteSource::Direct, computed("cycle_b")?]),
                vec![object("user", None)?],
            )?,
            relation(
                "cycle_b",
                RewriteSource::Union(vec![RewriteSource::Direct, computed("cycle_a")?]),
                vec![object("user", None)?],
            )?,
        ],
    )?;
    let parameters = BTreeMap::from([("x".parse()?, ParameterType::int())]);
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![user, service, group, folder, document],
        vec![ConditionSource::new(
            "under_limit".parse()?,
            ConditionDefinition::new("under_limit".parse()?, "x < 100".to_owned(), parameters),
        )],
    ))
}

fn short_circuit_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![
            type_source("user", Vec::new())?,
            type_source(
                "document",
                vec![relation(
                    "viewer",
                    RewriteSource::Union(vec![RewriteSource::Direct, RewriteSource::Direct]),
                    vec![object("user", None)?],
                )?],
            )?,
        ],
        Vec::new(),
    ))
}

fn type_source(
    name: &str,
    relations: Vec<RelationSource>,
) -> Result<TypeDefinitionSource, Box<dyn Error>> {
    Ok(TypeDefinitionSource::new(name.parse()?, relations))
}

fn relation(
    name: &str,
    rewrite: RewriteSource,
    restrictions: Vec<DirectRestrictionSource>,
) -> Result<RelationSource, Box<dyn Error>> {
    Ok(RelationSource::new(name.parse()?, rewrite, restrictions))
}

fn object(
    subject_type: &str,
    condition: Option<&str>,
) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Object,
        condition.map(str::parse).transpose()?,
    ))
}

fn wildcard(subject_type: &str) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Wildcard,
        None,
    ))
}

fn userset(
    subject_type: &str,
    relation: &str,
    condition: Option<&str>,
) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    Ok(DirectRestrictionSource::new(
        subject_type.parse()?,
        RestrictionKindSource::Userset(relation.parse()?),
        condition.map(str::parse).transpose()?,
    ))
}

fn computed(relation: &str) -> Result<RewriteSource, Box<dyn Error>> {
    Ok(RewriteSource::Computed(relation.parse()?))
}

fn ttu(tupleset: &str, computed: &str) -> Result<RewriteSource, Box<dyn Error>> {
    Ok(RewriteSource::TupleToUserset {
        tupleset: tupleset.parse()?,
        computed: computed.parse()?,
    })
}

#[derive(Debug)]
struct ShortCircuitReader {
    allowed: RelationshipTuple,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    barrier: Arc<Barrier>,
}

impl ShortCircuitReader {
    fn new(allowed: RelationshipTuple) -> Self {
        Self {
            allowed,
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            barrier: Arc::new(Barrier::new(2)),
        }
    }
}

struct ActiveRead(Arc<AtomicUsize>);

impl ActiveRead {
    fn new(active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::AcqRel);
        Self(active)
    }
}

impl Drop for ActiveRead {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[async_trait]
impl TupleReader for ShortCircuitReader {
    async fn read_exact_tuple(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _key: &TupleKey,
    ) -> Result<StoredTuple, StorageError> {
        Err(unsupported())
    }

    async fn read_object_relation(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ObjectRelationFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        let _active = ActiveRead::new(Arc::clone(&self.active));
        self.barrier.wait().await;
        if call == 0 {
            Ok(TupleStream::from_tuples(vec![self.allowed.clone()]))
        } else {
            pending::<Result<TupleStream, StorageError>>().await
        }
    }

    async fn read_userset_tuples(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &UsersetTupleFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        Err(unsupported())
    }

    async fn read_reverse_tuples(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ReverseTupleFilter,
        _options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        Err(unsupported())
    }

    async fn tuple_exists(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _key: &TupleKey,
    ) -> Result<bool, StorageError> {
        Err(unsupported())
    }

    async fn count_object_relation(
        &self,
        _context: &OperationContext,
        _store_id: StoreId,
        _filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError> {
        Err(unsupported())
    }
}

const fn unsupported() -> StorageError {
    StorageError::new(StorageErrorKind::Internal, "unsupported_test_read")
}
