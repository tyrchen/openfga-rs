//! Object-safe evaluator contract and explicit work-graph oracle.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use openfga_cache::{DecisionCache, DecisionKey, DecisionKeyHasher};
use openfga_condition::{
    CancellationCheck, EvaluationBudget as ConditionBudget, EvaluationErrorKind,
};
use openfga_domain::{
    BatchCheckCommand, CheckCommand, ConditionContext, ConditionReference, ConsistencyPreference,
    ContextualTuples, Deadline, InputLimits, Limit, ModelSelection, ObjectRef, QueryContext,
    RelationName, RelationshipTuple, StoreId, SubjectRef, TupleKey,
};
use openfga_model::{
    CompiledModel, ConditionRequirement, DirectRestriction, NodeId, RelationId, RestrictionKind,
    RewriteNode,
};
use openfga_storage::{
    ConditionFilter, ObjectRelationFilter, OperationContext, ReadOptions, StorageCancellationToken,
    StorageErrorKind, TupleReader,
};
use tokio::{
    task::JoinSet,
    time::{Instant as TokioInstant, sleep_until},
};

use crate::{
    BatchCheckOutcome, BatchCheckResult, CheckBudget, CheckError, CheckErrorKind, CheckMetadata,
    CheckOutcome, CheckResolution, CheckWorkMeter,
};

const CHECK_CANCELLED_CODE: &str = "check_cancelled";
const CHECK_DEADLINE_ELAPSED_CODE: &str = "check_deadline_elapsed";

type Evaluation = Result<Decision, CheckError>;
type WorkId = usize;

/// Object-safe authorization evaluator shared by service and future strategies.
///
/// `async-trait` is used deliberately because runtime strategy selection needs
/// `Arc<dyn CheckEvaluator>` and native async trait methods are not dyn-safe.
#[async_trait]
pub trait CheckEvaluator: Send + Sync {
    /// Evaluates one fully parsed command against one explicit compiled model.
    ///
    /// # Errors
    ///
    /// Returns typed semantic, condition, storage, cancellation, timeout, budget,
    /// or internal task failures.
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<CheckWorkMeter>,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, CheckError>;

    /// Evaluates request-ordered independent batch items with bounded concurrency.
    ///
    /// Per-item semantic and condition failures remain attached to their
    /// correlation IDs. Root cancellation, timeout, or a task panic fails the
    /// batch after every spawned item has been aborted and joined.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, model-identity, or internal join failures.
    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<BatchCheckOutcome, CheckError>;
}

/// Semantic version of the evaluator behavior encoded in decision cache keys.
const EVALUATOR_SEMANTICS_VERSION: u32 = 1;

/// Decision-caching evaluator decorator preserving the direct oracle contract.
#[derive(Clone)]
#[non_exhaustive]
pub struct CachedCheckEvaluator {
    delegate: Arc<dyn CheckEvaluator>,
    decisions: DecisionCache<bool>,
    key_hasher: DecisionKeyHasher,
    input_limits: InputLimits,
}

impl CachedCheckEvaluator {
    /// Creates a decision-caching decorator around an evaluator strategy.
    #[must_use]
    pub const fn new(
        delegate: Arc<dyn CheckEvaluator>,
        decisions: DecisionCache<bool>,
        key_hasher: DecisionKeyHasher,
        input_limits: InputLimits,
    ) -> Self {
        Self {
            delegate,
            decisions,
            key_hasher,
            input_limits,
        }
    }
}

impl fmt::Debug for CachedCheckEvaluator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedCheckEvaluator")
            .field("delegate", &"dyn CheckEvaluator")
            .field("decisions", &self.decisions)
            .field("key_hasher", &self.key_hasher)
            .field("input_limits", &self.input_limits)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CheckEvaluator for CachedCheckEvaluator {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<CheckWorkMeter>,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, CheckError> {
        if cancellation.is_cancelled() {
            return Err(cancelled(CHECK_CANCELLED_CODE));
        }
        if command.query().deadline().is_elapsed(Instant::now()) {
            return Err(timed_out(CHECK_DEADLINE_ELAPSED_CODE));
        }
        if command.query().consistency() == ConsistencyPreference::HigherConsistency {
            self.decisions.record_bypass();
            return self
                .delegate
                .check(command, model, tuples, budget, work_meter, cancellation)
                .await
                .map_err(canonicalize_check_control_error);
        }
        let key = DecisionKey::for_check(
            command,
            &model,
            &self.key_hasher,
            EVALUATOR_SEMANTICS_VERSION,
        );
        if let Some(allowed) = self.decisions.get(&key).await {
            if cancellation.is_cancelled() {
                return Err(cancelled(CHECK_CANCELLED_CODE));
            }
            if command.query().deadline().is_elapsed(Instant::now()) {
                return Err(timed_out(CHECK_DEADLINE_ELAPSED_CODE));
            }
            return Ok(cached_outcome(allowed));
        }
        let started_at = self.decisions.begin_computation();
        let outcome = self
            .delegate
            .check(command, model, tuples, budget, work_meter, cancellation)
            .await
            .map_err(canonicalize_check_control_error)?;
        self.decisions
            .insert_if_unchanged(started_at, key, outcome.allowed())
            .await;
        Ok(outcome)
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<BatchCheckOutcome, CheckError> {
        if cancellation.is_cancelled() {
            return Err(cancelled("batch_decision_cache_lookup_cancelled"));
        }
        if command.query().deadline().is_elapsed(Instant::now()) {
            return Err(timed_out("batch_decision_cache_lookup_deadline_elapsed"));
        }
        if command.query().consistency() == ConsistencyPreference::HigherConsistency {
            self.decisions.record_bypass();
            return self
                .delegate
                .batch_check(command, model, tuples, budget, cancellation)
                .await;
        }
        let keys = batch_decision_keys(command, &model, &self.key_hasher, &self.input_limits);
        let mut cached = Vec::with_capacity(keys.len());
        for key in &keys {
            let Some(key) = key else {
                cached.clear();
                break;
            };
            let Some(allowed) = self.decisions.get(key).await else {
                cached.clear();
                break;
            };
            cached.push(allowed);
        }
        if cached.len() == keys.len() {
            if cancellation.is_cancelled() {
                return Err(cancelled("batch_decision_cache_hit_cancelled"));
            }
            if command.query().deadline().is_elapsed(Instant::now()) {
                return Err(timed_out("batch_decision_cache_hit_deadline_elapsed"));
            }
            let results = command
                .items()
                .as_slice()
                .iter()
                .zip(cached)
                .map(|(item, allowed)| {
                    BatchCheckResult::new(
                        item.correlation_id().clone(),
                        Ok(cached_outcome(allowed)),
                    )
                })
                .collect();
            return Ok(BatchCheckOutcome::new(results));
        }

        let started_at = self.decisions.begin_computation();
        let outcome = self
            .delegate
            .batch_check(command, model, tuples, budget, cancellation)
            .await?;
        for (key, result) in keys.into_iter().zip(outcome.results()) {
            if let (Some(key), Ok(item_outcome)) = (key, result.outcome()) {
                self.decisions
                    .insert_if_unchanged(started_at, key, item_outcome.allowed())
                    .await;
            }
        }
        Ok(outcome)
    }
}

fn batch_decision_keys(
    command: &BatchCheckCommand,
    model: &CompiledModel,
    hasher: &DecisionKeyHasher,
    input_limits: &InputLimits,
) -> Vec<Option<DecisionKey>> {
    let query = command.query();
    command
        .items()
        .as_slice()
        .iter()
        .map(|item| {
            let condition_context = query
                .condition_context()
                .overlay(item.condition_context(), input_limits)
                .ok()?;
            let mut contextual = BTreeMap::<TupleKey, RelationshipTuple>::new();
            for tuple in query.contextual_tuples().as_slice() {
                contextual.insert(tuple.key().clone(), tuple.clone());
            }
            for tuple in item.contextual_tuples().as_slice() {
                contextual.insert(tuple.key().clone(), tuple.clone());
            }
            let contextual_tuples =
                ContextualTuples::new(contextual.into_values().collect(), input_limits).ok()?;
            let item_query = QueryContext::builder()
                .store_id(query.store_id())
                .model_selection(query.model_selection())
                .consistency(query.consistency())
                .contextual_tuples(contextual_tuples)
                .condition_context(condition_context)
                .deadline(query.deadline())
                .principal(query.principal().clone())
                .build();
            Some(DecisionKey::for_check(
                &CheckCommand::new(item_query, item.tuple().clone()),
                model,
                hasher,
                EVALUATOR_SEMANTICS_VERSION,
            ))
        })
        .collect()
}

fn cached_outcome(allowed: bool) -> CheckOutcome {
    CheckOutcome::new(
        allowed,
        CheckResolution::Cached,
        CheckMetadata::new(0, 0, 0, 0, 0, 0, std::time::Duration::ZERO),
    )
}

/// Stateless correctness-first evaluator configured only with boundary limits.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DirectCheckEvaluator {
    input_limits: InputLimits,
}

impl DirectCheckEvaluator {
    /// Creates an evaluator using the same input limits as transport conversion.
    #[must_use]
    pub const fn new(input_limits: InputLimits) -> Self {
        Self { input_limits }
    }
}

impl Default for DirectCheckEvaluator {
    fn default() -> Self {
        Self::new(InputLimits::default())
    }
}

#[async_trait]
impl CheckEvaluator for DirectCheckEvaluator {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<CheckWorkMeter>,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, CheckError> {
        let query = command.query();
        let input = RootInput {
            store_id: query.store_id(),
            model_selection: query.model_selection(),
            consistency: query.consistency(),
            deadline: query.deadline(),
            tuple: command.tuple().clone(),
            contextual: ContextualIndex::new(query.contextual_tuples(), None),
            condition_context: query.condition_context().clone(),
        };
        evaluate_root(
            input,
            model,
            tuples,
            budget,
            work_meter,
            self.input_limits.clone(),
            cancellation,
        )
        .await
        .map_err(canonicalize_check_control_error)
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<BatchCheckOutcome, CheckError> {
        let prepared = prepare_batch(command, &model, &self.input_limits)?;
        run_batch(
            prepared,
            model,
            tuples,
            budget,
            self.input_limits.clone(),
            cancellation,
        )
        .await
    }
}

struct BatchJob {
    index: usize,
    correlation_id: openfga_domain::CorrelationId,
    input: RootInput,
}

impl fmt::Debug for BatchJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchJob")
            .field("index", &self.index)
            .field("correlation_id_bytes", &self.correlation_id.as_str().len())
            .field("input", &self.input)
            .finish()
    }
}

#[derive(Debug)]
struct PreparedBatch {
    jobs: VecDeque<BatchJob>,
    results: Vec<Option<BatchCheckResult>>,
    deadline: Deadline,
}

fn prepare_batch(
    command: &BatchCheckCommand,
    model: &CompiledModel,
    input_limits: &InputLimits,
) -> Result<PreparedBatch, CheckError> {
    let query = command.query();
    validate_model_identity(query.store_id(), query.model_selection(), model)?;
    let item_count = command.items().as_slice().len();
    let mut results = std::iter::repeat_with(|| None)
        .take(item_count)
        .collect::<Vec<Option<BatchCheckResult>>>();
    let mut jobs = VecDeque::with_capacity(item_count);
    for (index, item) in command.items().as_slice().iter().enumerate() {
        let condition_context = query
            .condition_context()
            .overlay(item.condition_context(), input_limits);
        let Ok(condition_context) = condition_context else {
            let slot = results
                .get_mut(index)
                .ok_or_else(|| internal("batch_result_index_invalid"))?;
            *slot = Some(BatchCheckResult::new(
                item.correlation_id().clone(),
                Err(CheckError::new(
                    CheckErrorKind::InvalidTuple,
                    "batch_condition_context_invalid",
                )),
            ));
            continue;
        };
        jobs.push_back(BatchJob {
            index,
            correlation_id: item.correlation_id().clone(),
            input: RootInput {
                store_id: query.store_id(),
                model_selection: query.model_selection(),
                consistency: query.consistency(),
                deadline: query.deadline(),
                tuple: item.tuple().clone(),
                contextual: ContextualIndex::new(
                    query.contextual_tuples(),
                    Some(item.contextual_tuples()),
                ),
                condition_context,
            },
        });
    }
    Ok(PreparedBatch {
        jobs,
        results,
        deadline: query.deadline(),
    })
}

async fn run_batch(
    mut prepared: PreparedBatch,
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: CheckBudget,
    input_limits: InputLimits,
    cancellation: StorageCancellationToken,
) -> Result<BatchCheckOutcome, CheckError> {
    let mut pending = JoinSet::new();
    while !prepared.jobs.is_empty() || !pending.is_empty() {
        while !prepared.jobs.is_empty() && pending.len() < budget.maximum_batch_concurrency() {
            let Some(job) = prepared.jobs.pop_front() else {
                abort_and_join(&mut pending).await;
                return Err(internal("batch_job_missing"));
            };
            let item_model = Arc::clone(&model);
            let item_tuples = Arc::clone(&tuples);
            let item_limits = input_limits.clone();
            let item_cancellation = cancellation.clone();
            pending.spawn(async move {
                let outcome = evaluate_root(
                    job.input,
                    item_model,
                    item_tuples,
                    budget,
                    None,
                    item_limits,
                    item_cancellation,
                )
                .await;
                (
                    job.index,
                    BatchCheckResult::new(job.correlation_id, outcome),
                )
            });
        }
        collect_batch_result(&mut prepared, &mut pending, &cancellation).await?;
    }
    let ordered = prepared
        .results
        .into_iter()
        .map(|result| result.ok_or_else(|| internal("batch_result_missing")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BatchCheckOutcome::new(ordered))
}

async fn collect_batch_result(
    prepared: &mut PreparedBatch,
    pending: &mut JoinSet<(usize, BatchCheckResult)>,
    cancellation: &StorageCancellationToken,
) -> Result<(), CheckError> {
    if pending.is_empty() {
        return Ok(());
    }
    let deadline = TokioInstant::from_std(prepared.deadline.instant());
    let joined = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            abort_and_join(pending).await;
            return Err(cancelled("batch_cancelled"));
        }
        () = sleep_until(deadline) => {
            abort_and_join(pending).await;
            return Err(timed_out("batch_deadline_elapsed"));
        }
        joined = pending.join_next() => joined,
    };
    let Some(joined) = joined else {
        return Ok(());
    };
    let Ok((index, result)) = joined else {
        abort_and_join(pending).await;
        return Err(internal("batch_item_task_failed"));
    };
    let Some(slot) = prepared.results.get_mut(index) else {
        abort_and_join(pending).await;
        return Err(internal("batch_result_index_invalid"));
    };
    *slot = Some(result);
    Ok(())
}

async fn abort_and_join<T: 'static>(tasks: &mut JoinSet<T>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[derive(Debug)]
struct RootInput {
    store_id: StoreId,
    model_selection: ModelSelection,
    consistency: ConsistencyPreference,
    deadline: Deadline,
    tuple: TupleKey,
    contextual: ContextualIndex,
    condition_context: ConditionContext,
}

#[derive(Debug)]
struct ContextualIndex(BTreeMap<(ObjectRef, RelationName), Vec<RelationshipTuple>>);

impl ContextualIndex {
    fn new(base: &ContextualTuples, overlay: Option<&ContextualTuples>) -> Self {
        let mut tuples = BTreeMap::<TupleKey, RelationshipTuple>::new();
        for tuple in base.as_slice() {
            tuples.insert(tuple.key().clone(), tuple.clone());
        }
        if let Some(overlay) = overlay {
            for tuple in overlay.as_slice() {
                tuples.insert(tuple.key().clone(), tuple.clone());
            }
        }
        let mut index = BTreeMap::<(ObjectRef, RelationName), Vec<RelationshipTuple>>::new();
        for tuple in tuples.into_values() {
            index
                .entry((tuple.key().object().clone(), tuple.key().relation().clone()))
                .or_default()
                .push(tuple);
        }
        Self(index)
    }

    fn get(&self, object: &ObjectRef, relation: &RelationName) -> &[RelationshipTuple] {
        self.0
            .get(&(object.clone(), relation.clone()))
            .map_or(&[], Vec::as_slice)
    }

    fn values(&self) -> impl Iterator<Item = &RelationshipTuple> {
        self.0.values().flat_map(|tuples| tuples.iter())
    }
}

#[tracing::instrument(
    level = "debug",
    name = "openfga.check.evaluate",
    skip_all,
    fields(
        store_id = %input.store_id,
        model_id = %model.model_id(),
        object_type = %input.tuple.object().object_type(),
        subject_type = %input.tuple.subject().subject_type(),
        consistency = ?input.consistency,
        strategy = "direct_oracle",
        allowed = tracing::field::Empty,
        resolution = tracing::field::Empty,
        error_class = tracing::field::Empty,
        dispatches = tracing::field::Empty,
        datastore_queries = tracing::field::Empty,
        tuple_items = tracing::field::Empty,
        condition_cost = tracing::field::Empty,
        cycles = tracing::field::Empty,
        maximum_depth = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    )
)]
async fn evaluate_root(
    input: RootInput,
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: CheckBudget,
    work_meter: Option<CheckWorkMeter>,
    input_limits: InputLimits,
    cancellation: StorageCancellationToken,
) -> Result<CheckOutcome, CheckError> {
    let outcome = evaluate_root_inner(
        input,
        model,
        tuples,
        budget,
        work_meter,
        input_limits,
        cancellation,
    )
    .await;
    record_root_outcome(&outcome);
    outcome
}

async fn evaluate_root_inner(
    input: RootInput,
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: CheckBudget,
    work_meter: Option<CheckWorkMeter>,
    input_limits: InputLimits,
    cancellation: StorageCancellationToken,
) -> Result<CheckOutcome, CheckError> {
    validate_model_identity(input.store_id, input.model_selection, &model)?;
    let root_relation = validate_query_tuple(&input.tuple, &model)?;
    for tuple in input.contextual.values() {
        validate_contextual_tuple(tuple, &model)?;
    }
    let operation = OperationContext::new(input.consistency, input.deadline, cancellation);
    let mut scheduler = Scheduler::new(
        model,
        tuples,
        budget,
        work_meter,
        input_limits,
        operation,
        input.contextual,
        input.condition_context,
    )?;
    scheduler
        .run(SemanticWork {
            object: input.tuple.object().clone(),
            relation: root_relation,
            subject: input.tuple.subject().clone(),
            path: Arc::new(BTreeSet::new()),
            depth: 0,
        })
        .await
}

fn record_root_outcome(outcome: &Result<CheckOutcome, CheckError>) {
    let span = tracing::Span::current();
    match outcome {
        Ok(outcome) => {
            let metadata = outcome.metadata();
            span.record("allowed", outcome.allowed());
            span.record("resolution", tracing::field::debug(outcome.resolution()));
            span.record("dispatches", metadata.dispatches());
            span.record("datastore_queries", metadata.datastore_queries());
            span.record("tuple_items", metadata.tuple_items());
            span.record("condition_cost", metadata.condition_cost());
            span.record("cycles", metadata.cycles());
            span.record("maximum_depth", metadata.maximum_depth());
            let duration_ms = u64::try_from(metadata.duration().as_millis()).unwrap_or(u64::MAX);
            span.record("duration_ms", duration_ms);
        }
        Err(error) => {
            span.record("error_class", tracing::field::debug(error.kind()));
        }
    }
}

fn validate_model_identity(
    store_id: StoreId,
    selection: ModelSelection,
    model: &CompiledModel,
) -> Result<(), CheckError> {
    if &store_id != model.store_id() {
        return Err(CheckError::new(
            CheckErrorKind::InvalidModel,
            "model_store_mismatch",
        ));
    }
    if let ModelSelection::Explicit(model_id) = selection
        && &model_id != model.model_id()
    {
        return Err(CheckError::new(
            CheckErrorKind::InvalidModel,
            "model_id_mismatch",
        ));
    }
    Ok(())
}

fn validate_query_tuple(tuple: &TupleKey, model: &CompiledModel) -> Result<RelationId, CheckError> {
    model.validate_query_tuple(tuple).map_err(Into::into)
}

fn validate_contextual_tuple(
    tuple: &RelationshipTuple,
    model: &CompiledModel,
) -> Result<(), CheckError> {
    model.validate_relationship_tuple(tuple).map_err(Into::into)
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SemanticKey {
    object: ObjectRef,
    relation: RelationId,
    subject: SubjectRef,
}

#[derive(Clone, Debug)]
struct SemanticWork {
    object: ObjectRef,
    relation: RelationId,
    subject: SubjectRef,
    path: Arc<BTreeSet<SemanticKey>>,
    depth: u32,
}

#[derive(Clone, Debug)]
struct RewriteWork {
    object: ObjectRef,
    node: NodeId,
    subject: SubjectRef,
    path: Arc<BTreeSet<SemanticKey>>,
    depth: u32,
}

#[derive(Clone, Debug)]
enum WorkKind {
    Semantic(SemanticWork),
    Rewrite(RewriteWork),
}

#[derive(Clone, Copy, Debug)]
struct ParentLink {
    work: WorkId,
    operand: usize,
}

#[derive(Debug)]
struct WorkSlot {
    parent: Option<ParentLink>,
    memo_key: Option<MemoKey>,
    state: WorkState,
}

#[derive(Debug)]
enum WorkState {
    Queued(WorkKind),
    Processing,
    Waiting(Reducer),
    Reading(ReadContinuation),
    Completed,
}

#[derive(Debug)]
enum ReadContinuation {
    Direct(DirectRead),
    TupleToUserset(TtuRead),
}

#[derive(Debug)]
struct DirectRead {
    relation: RelationId,
    object: ObjectRef,
    subject: SubjectRef,
    path: Arc<BTreeSet<SemanticKey>>,
    depth: u32,
}

#[derive(Debug)]
struct TtuRead {
    tupleset: RelationId,
    targets: Box<[RelationId]>,
    object: ObjectRef,
    subject: SubjectRef,
    path: Arc<BTreeSet<SemanticKey>>,
    depth: u32,
}

#[derive(Clone, Copy, Debug)]
struct Decision {
    allowed: bool,
    resolution: CheckResolution,
}

/// Per-root memo identity. Store, model, contextual tuples, condition context,
/// consistency, and deadline are invariant within one scheduler and therefore
/// form the enclosing namespace rather than repeated key fields.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MemoKey {
    semantic: SemanticKey,
    branch_path: BTreeSet<SemanticKey>,
}

impl Decision {
    const fn allow(resolution: CheckResolution) -> Self {
        Self {
            allowed: true,
            resolution,
        }
    }

    const fn deny(resolution: CheckResolution) -> Self {
        Self {
            allowed: false,
            resolution,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ReducerKind {
    Single,
    Union,
    Intersection,
    Difference,
}

#[derive(Debug)]
struct Reducer {
    kind: ReducerKind,
    resolution: Option<CheckResolution>,
    outcomes: Vec<Option<Evaluation>>,
    remaining: usize,
}

impl Reducer {
    fn new(kind: ReducerKind, operands: usize, resolution: Option<CheckResolution>) -> Self {
        Self {
            kind,
            resolution,
            outcomes: std::iter::repeat_with(|| None).take(operands).collect(),
            remaining: operands,
        }
    }

    fn accept(
        &mut self,
        operand: usize,
        outcome: Evaluation,
    ) -> Result<Option<Evaluation>, CheckError> {
        let slot = self
            .outcomes
            .get_mut(operand)
            .ok_or_else(|| internal("reducer_operand_invalid"))?;
        if slot.is_some() {
            return Err(internal("reducer_operand_completed_twice"));
        }
        *slot = Some(outcome);
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or_else(|| internal("reducer_remaining_underflow"))?;
        self.reduce()
    }

    fn reduce(&mut self) -> Result<Option<Evaluation>, CheckError> {
        match self.kind {
            ReducerKind::Single => {
                if self.remaining != 0 {
                    return Ok(None);
                }
                let outcome = self.take(0)?;
                Ok(Some(map_resolution(outcome, self.resolution)))
            }
            ReducerKind::Union => {
                if self
                    .outcomes
                    .iter()
                    .flatten()
                    .any(|outcome| matches!(outcome, Ok(decision) if decision.allowed))
                {
                    return Ok(Some(Ok(Decision::allow(
                        self.resolution.unwrap_or(CheckResolution::Union),
                    ))));
                }
                if self.remaining != 0 {
                    return Ok(None);
                }
                if let Some(error) = self.take_first_error()? {
                    return Ok(Some(Err(error)));
                }
                Ok(Some(Ok(Decision::deny(CheckResolution::Denied))))
            }
            ReducerKind::Intersection => {
                if self
                    .outcomes
                    .iter()
                    .flatten()
                    .any(|outcome| matches!(outcome, Ok(decision) if !decision.allowed))
                {
                    return Ok(Some(Ok(Decision::deny(CheckResolution::Denied))));
                }
                if self.remaining != 0 {
                    return Ok(None);
                }
                if let Some(error) = self.take_first_error()? {
                    return Ok(Some(Err(error)));
                }
                Ok(Some(Ok(Decision::allow(
                    self.resolution.unwrap_or(CheckResolution::Intersection),
                ))))
            }
            ReducerKind::Difference => self.reduce_difference(),
        }
    }

    fn reduce_difference(&mut self) -> Result<Option<Evaluation>, CheckError> {
        let base = self.outcomes.first().and_then(Option::as_ref);
        let subtract = self.outcomes.get(1).and_then(Option::as_ref);
        if matches!(base, Some(Ok(decision)) if !decision.allowed)
            || matches!(subtract, Some(Ok(decision)) if decision.allowed)
        {
            return Ok(Some(Ok(Decision::deny(CheckResolution::Denied))));
        }
        if self.remaining != 0 {
            return Ok(None);
        }
        if let Some(error) = self.take_first_error()? {
            return Ok(Some(Err(error)));
        }
        let base = self.take(0)?;
        let subtract = self.take(1)?;
        match (base, subtract) {
            (Ok(base), Ok(subtract)) if base.allowed && !subtract.allowed => Ok(Some(Ok(
                Decision::allow(self.resolution.unwrap_or(CheckResolution::Difference)),
            ))),
            (Ok(_), Ok(_)) => Ok(Some(Ok(Decision::deny(CheckResolution::Denied)))),
            _ => Err(internal("difference_outcome_inconsistent")),
        }
    }

    fn take(&mut self, operand: usize) -> Result<Evaluation, CheckError> {
        self.outcomes
            .get_mut(operand)
            .and_then(Option::take)
            .ok_or_else(|| internal("reducer_outcome_missing"))
    }

    fn take_first_error(&mut self) -> Result<Option<CheckError>, CheckError> {
        for outcome in &mut self.outcomes {
            if matches!(outcome, Some(Err(_))) {
                let taken = outcome
                    .take()
                    .ok_or_else(|| internal("reducer_error_missing"))?;
                if let Err(error) = taken {
                    return Ok(Some(error));
                }
            }
        }
        Ok(None)
    }
}

fn map_resolution(outcome: Evaluation, resolution: Option<CheckResolution>) -> Evaluation {
    outcome.map(|decision| {
        if decision.allowed {
            resolution.map_or(decision, Decision::allow)
        } else {
            decision
        }
    })
}

#[derive(Debug, Default)]
struct Counters {
    dispatches: u32,
    datastore_queries: u32,
    tuple_items: u32,
    condition_cost: u32,
    cycles: u32,
    maximum_depth: u32,
}

struct RootConditionCancellation<'a> {
    operation: &'a OperationContext,
    #[cfg(test)]
    poll_observer: Option<&'a (dyn Fn() + Sync)>,
    #[cfg(test)]
    clock: Option<&'a (dyn Fn() -> Instant + Sync)>,
}

impl CancellationCheck for RootConditionCancellation<'_> {
    fn is_cancelled(&self) -> bool {
        #[cfg(test)]
        if let Some(observer) = self.poll_observer {
            observer();
        }
        #[cfg(test)]
        let current_time = self.clock.map_or_else(Instant::now, |clock| clock());
        #[cfg(not(test))]
        let current_time = Instant::now();
        self.operation.cancellation().is_cancelled()
            || self.operation.deadline().is_elapsed(current_time)
    }
}

struct Scheduler {
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: CheckBudget,
    work_meter: Option<CheckWorkMeter>,
    input_limits: InputLimits,
    operation: OperationContext,
    contextual: ContextualIndex,
    condition_context: ConditionContext,
    read_options: ReadOptions,
    counters: Counters,
    memo: BTreeMap<MemoKey, Decision>,
    slots: Vec<WorkSlot>,
    ready: VecDeque<WorkId>,
    reads: JoinSet<(WorkId, Result<Vec<RelationshipTuple>, CheckError>)>,
    root_result: Option<Evaluation>,
    started_at: Instant,
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Scheduler")
            .field("model", &self.model)
            .field("budget", &self.budget)
            .field("counters", &self.counters)
            .field("slots", &self.slots.len())
            .field("ready", &self.ready.len())
            .field("reads", &self.reads.len())
            .finish_non_exhaustive()
    }
}

use std::fmt;

impl Scheduler {
    #[allow(
        clippy::too_many_arguments,
        reason = "evaluator capabilities and request-scoped state remain explicit"
    )]
    fn new(
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<CheckWorkMeter>,
        input_limits: InputLimits,
        operation: OperationContext,
        contextual: ContextualIndex,
        condition_context: ConditionContext,
    ) -> Result<Self, CheckError> {
        let maximum_results = Limit::<100_000>::new(budget.maximum_tuple_items().min(100_000))
            .map_err(|_| internal("tuple_read_limit_invalid"))?;
        let read_options = ReadOptions::from_limit(maximum_results);
        Ok(Self {
            model,
            tuples,
            budget,
            work_meter,
            input_limits,
            operation,
            contextual,
            condition_context,
            read_options,
            counters: Counters::default(),
            memo: BTreeMap::new(),
            slots: Vec::new(),
            ready: VecDeque::new(),
            reads: JoinSet::new(),
            root_result: None,
            started_at: Instant::now(),
        })
    }

    async fn run(&mut self, root: SemanticWork) -> Result<CheckOutcome, CheckError> {
        let decision = self.drive(root).await;
        self.abort_reads().await;
        let outcome = decision?;
        let metadata = CheckMetadata::new(
            self.counters.dispatches,
            self.counters.datastore_queries,
            self.counters.tuple_items,
            self.counters.condition_cost,
            self.counters.cycles,
            self.counters.maximum_depth,
            self.started_at.elapsed(),
        );
        Ok(CheckOutcome::new(
            outcome.allowed,
            outcome.resolution,
            metadata,
        ))
    }

    async fn drive(&mut self, root: SemanticWork) -> Result<Decision, CheckError> {
        let root_id = self.schedule(WorkKind::Semantic(root), None)?;
        if root_id != 0 {
            return Err(internal("root_work_id_invalid"));
        }
        while self.root_result.is_none() {
            self.check_context()?;
            let available = self.ready.len();
            for _ in 0..available {
                let Some(work_id) = self.ready.pop_front() else {
                    break;
                };
                self.process(work_id)?;
                if self.root_result.is_some() {
                    break;
                }
            }
            if self.root_result.is_some() {
                break;
            }
            if self.reads.is_empty() {
                if self.ready.is_empty() {
                    return Err(internal("work_graph_stalled"));
                }
                continue;
            }
            let deadline = TokioInstant::from_std(self.operation.deadline().instant());
            let joined = tokio::select! {
                biased;
                () = self.operation.cancellation().cancelled() => {
                    return Err(cancelled(CHECK_CANCELLED_CODE));
                }
                () = sleep_until(deadline) => {
                    return Err(timed_out(CHECK_DEADLINE_ELAPSED_CODE));
                }
                joined = self.reads.join_next() => joined,
            };
            let Some(joined) = joined else {
                continue;
            };
            if let Ok((work, result)) = joined {
                self.finish_read(work, result)?;
            } else {
                return Err(internal("datastore_read_task_failed"));
            }
        }

        self.root_result
            .take()
            .ok_or_else(|| internal("root_result_missing"))?
    }

    async fn abort_reads(&mut self) {
        self.reads.abort_all();
        while self.reads.join_next().await.is_some() {}
    }

    fn check_context(&self) -> Result<(), CheckError> {
        self.operation.check().map_err(CheckError::from)
    }

    fn schedule(
        &mut self,
        kind: WorkKind,
        parent: Option<ParentLink>,
    ) -> Result<WorkId, CheckError> {
        self.counters.dispatches = self
            .counters
            .dispatches
            .checked_add(1)
            .ok_or_else(dispatch_exceeded)?;
        if self.counters.dispatches > self.budget.maximum_dispatches() {
            return Err(dispatch_exceeded());
        }
        if self
            .work_meter
            .as_ref()
            .is_some_and(|meter| !meter.charge_dispatches(1))
        {
            return Err(dispatch_exceeded());
        }
        let id = self.slots.len();
        self.slots.push(WorkSlot {
            parent,
            memo_key: None,
            state: WorkState::Queued(kind),
        });
        self.ready.push_back(id);
        Ok(id)
    }

    fn process(&mut self, work_id: WorkId) -> Result<(), CheckError> {
        if !self.is_active(work_id)? {
            self.slot_mut(work_id)?.state = WorkState::Completed;
            return Ok(());
        }
        let state = std::mem::replace(&mut self.slot_mut(work_id)?.state, WorkState::Processing);
        let WorkState::Queued(kind) = state else {
            return Err(internal("queued_work_state_invalid"));
        };
        match kind {
            WorkKind::Semantic(work) => self.process_semantic(work_id, work),
            WorkKind::Rewrite(work) => self.process_rewrite(work_id, work),
        }
    }

    fn process_semantic(&mut self, work_id: WorkId, work: SemanticWork) -> Result<(), CheckError> {
        if work.depth > self.budget.maximum_depth() {
            return self.complete(work_id, Err(depth_exceeded()));
        }
        self.counters.maximum_depth = self.counters.maximum_depth.max(work.depth);
        let key = SemanticKey {
            object: work.object.clone(),
            relation: work.relation,
            subject: work.subject.clone(),
        };
        let memo_key = MemoKey {
            semantic: key.clone(),
            branch_path: work.path.as_ref().clone(),
        };
        if let Some(decision) = self.memo.get(&memo_key).copied() {
            return self.complete(work_id, Ok(decision));
        }
        self.slot_mut(work_id)?.memo_key = Some(memo_key);
        if work.path.contains(&key) {
            self.counters.cycles = self.counters.cycles.saturating_add(1);
            return self.complete(work_id, Ok(Decision::deny(CheckResolution::Cycle)));
        }
        let subject_type = match self.model.type_id(work.subject.subject_type()) {
            Ok(subject_type) => subject_type,
            Err(source) => {
                return self.complete(
                    work_id,
                    Err(CheckError::model("semantic_subject_type_missing", source)),
                );
            }
        };
        let reachable = if work.subject.is_typed_wildcard() {
            self.model.can_reach_wildcard(work.relation, subject_type)
        } else {
            self.model
                .can_reach_subject_type(work.relation, subject_type)
        };
        if !reachable {
            return self.complete(work_id, Ok(Decision::deny(CheckResolution::Unreachable)));
        }
        let relation_root = match self.model.relation(work.relation) {
            Ok(relation) => relation.root(),
            Err(source) => {
                return self.complete(
                    work_id,
                    Err(CheckError::model("semantic_relation_invalid", source)),
                );
            }
        };
        let mut path = work.path.as_ref().clone();
        path.insert(key);
        self.slot_mut(work_id)?.state =
            WorkState::Waiting(Reducer::new(ReducerKind::Single, 1, None));
        let child = WorkKind::Rewrite(RewriteWork {
            object: work.object,
            node: relation_root,
            subject: work.subject,
            path: Arc::new(path),
            depth: work.depth,
        });
        self.schedule_child_or_error(work_id, 0, child)
    }

    fn process_rewrite(&mut self, work_id: WorkId, work: RewriteWork) -> Result<(), CheckError> {
        let node = match self.model.node(work.node) {
            Ok(node) => node.clone(),
            Err(source) => {
                return self.complete(
                    work_id,
                    Err(CheckError::model("rewrite_node_invalid", source)),
                );
            }
        };
        match node {
            RewriteNode::Direct(relation) => self.process_direct(work_id, work, relation),
            RewriteNode::Computed(relation) => self.process_computed(work_id, work, relation),
            RewriteNode::TupleToUserset {
                tupleset,
                computed: _,
                targets,
            } => self.process_ttu(work_id, work, tupleset, targets),
            RewriteNode::Union(nodes) => self.start_rewrite_children(
                work_id,
                &work,
                nodes.as_ref(),
                ReducerKind::Union,
                CheckResolution::Union,
            ),
            RewriteNode::Intersection(nodes) => self.start_rewrite_children(
                work_id,
                &work,
                nodes.as_ref(),
                ReducerKind::Intersection,
                CheckResolution::Intersection,
            ),
            RewriteNode::Difference { base, subtract } => {
                self.process_difference(work_id, &work, base, subtract)
            }
            _ => self.complete(work_id, Err(internal("rewrite_node_unsupported"))),
        }
    }

    fn process_direct(
        &mut self,
        work_id: WorkId,
        work: RewriteWork,
        relation: RelationId,
    ) -> Result<(), CheckError> {
        if self.defer_for_read_capacity(work_id, &work)? {
            return Ok(());
        }
        let filter_object = work.object.clone();
        self.start_read(
            work_id,
            relation,
            &filter_object,
            ReadContinuation::Direct(DirectRead {
                relation,
                object: work.object,
                subject: work.subject,
                path: work.path,
                depth: work.depth,
            }),
        )
    }

    fn process_computed(
        &mut self,
        work_id: WorkId,
        work: RewriteWork,
        relation: RelationId,
    ) -> Result<(), CheckError> {
        self.slot_mut(work_id)?.state = WorkState::Waiting(Reducer::new(
            ReducerKind::Single,
            1,
            Some(CheckResolution::Computed),
        ));
        self.schedule_child_or_error(
            work_id,
            0,
            WorkKind::Semantic(SemanticWork {
                object: work.object,
                relation,
                subject: work.subject,
                path: work.path,
                depth: next_depth(work.depth)?,
            }),
        )
    }

    fn process_ttu(
        &mut self,
        work_id: WorkId,
        work: RewriteWork,
        tupleset: RelationId,
        targets: Box<[RelationId]>,
    ) -> Result<(), CheckError> {
        if self.defer_for_read_capacity(work_id, &work)? {
            return Ok(());
        }
        let filter_object = work.object.clone();
        self.start_read(
            work_id,
            tupleset,
            &filter_object,
            ReadContinuation::TupleToUserset(TtuRead {
                tupleset,
                targets,
                object: work.object,
                subject: work.subject,
                path: work.path,
                depth: work.depth,
            }),
        )
    }

    fn defer_for_read_capacity(
        &mut self,
        work_id: WorkId,
        work: &RewriteWork,
    ) -> Result<bool, CheckError> {
        if self.reads.len() < self.budget.maximum_concurrent_reads() {
            return Ok(false);
        }
        self.slot_mut(work_id)?.state = WorkState::Queued(WorkKind::Rewrite(work.clone()));
        self.ready.push_back(work_id);
        Ok(true)
    }

    fn process_difference(
        &mut self,
        work_id: WorkId,
        work: &RewriteWork,
        base: NodeId,
        subtract: NodeId,
    ) -> Result<(), CheckError> {
        self.slot_mut(work_id)?.state = WorkState::Waiting(Reducer::new(
            ReducerKind::Difference,
            2,
            Some(CheckResolution::Difference),
        ));
        for (operand, node) in [base, subtract].into_iter().enumerate() {
            if !self.is_active(work_id)? {
                break;
            }
            self.schedule_child_or_error(
                work_id,
                operand,
                WorkKind::Rewrite(RewriteWork {
                    object: work.object.clone(),
                    node,
                    subject: work.subject.clone(),
                    path: Arc::clone(&work.path),
                    depth: work.depth,
                }),
            )?;
        }
        Ok(())
    }

    fn start_rewrite_children(
        &mut self,
        work_id: WorkId,
        work: &RewriteWork,
        nodes: &[NodeId],
        kind: ReducerKind,
        resolution: CheckResolution,
    ) -> Result<(), CheckError> {
        self.slot_mut(work_id)?.state =
            WorkState::Waiting(Reducer::new(kind, nodes.len(), Some(resolution)));
        for (operand, node) in nodes.iter().copied().enumerate() {
            if !self.is_active(work_id)? {
                break;
            }
            self.schedule_child_or_error(
                work_id,
                operand,
                WorkKind::Rewrite(RewriteWork {
                    object: work.object.clone(),
                    node,
                    subject: work.subject.clone(),
                    path: Arc::clone(&work.path),
                    depth: work.depth,
                }),
            )?;
        }
        Ok(())
    }

    fn start_read(
        &mut self,
        work_id: WorkId,
        relation_id: RelationId,
        object: &ObjectRef,
        continuation: ReadContinuation,
    ) -> Result<(), CheckError> {
        self.counters.datastore_queries = self
            .counters
            .datastore_queries
            .checked_add(1)
            .ok_or_else(datastore_exceeded)?;
        if self.counters.datastore_queries > self.budget.maximum_datastore_queries() {
            return self.complete(work_id, Err(datastore_exceeded()));
        }
        if self
            .work_meter
            .as_ref()
            .is_some_and(|meter| !meter.charge_datastore_queries(1))
        {
            return self.complete(work_id, Err(datastore_exceeded()));
        }
        let relation = match self.model.relation(relation_id) {
            Ok(relation) => relation,
            Err(source) => {
                return self.complete(
                    work_id,
                    Err(CheckError::model("read_relation_invalid", source)),
                );
            }
        };
        let filter = match ObjectRelationFilter::new(
            object.clone(),
            relation.name().clone(),
            Vec::new(),
            ConditionFilter::any(),
            &self.input_limits,
        ) {
            Ok(filter) => filter,
            Err(error) => return self.complete(work_id, Err(CheckError::from(error))),
        };
        self.slot_mut(work_id)?.state = WorkState::Reading(continuation);
        let reader = Arc::clone(&self.tuples);
        let operation = self.operation.clone();
        let store_id = *self.model.store_id();
        let options = self.read_options;
        let work_meter = self.work_meter.clone();
        self.reads.spawn(async move {
            let result = async {
                let mut stream = reader
                    .read_object_relation(&operation, store_id, &filter, options)
                    .await
                    .map_err(map_read_error)?;
                let stored_items =
                    u32::try_from(stream.remaining()).map_err(|_| tuple_items_exceeded())?;
                if work_meter
                    .as_ref()
                    .is_some_and(|meter| !meter.charge_tuple_items(stored_items))
                {
                    return Err(tuple_items_exceeded());
                }
                let mut rows = Vec::with_capacity(stream.remaining());
                for item in &mut stream {
                    rows.push(item.map_err(map_read_error)?);
                }
                Ok(rows)
            }
            .await;
            (work_id, result)
        });
        Ok(())
    }

    fn finish_read(
        &mut self,
        work_id: WorkId,
        result: Result<Vec<RelationshipTuple>, CheckError>,
    ) -> Result<(), CheckError> {
        if !self.is_active(work_id)? {
            self.slot_mut(work_id)?.state = WorkState::Completed;
            return Ok(());
        }
        let state = std::mem::replace(&mut self.slot_mut(work_id)?.state, WorkState::Processing);
        let WorkState::Reading(continuation) = state else {
            return Err(internal("read_completion_state_invalid"));
        };
        let stored = match result {
            Ok(stored) => stored,
            Err(error) => return self.complete(work_id, Err(error)),
        };
        match continuation {
            ReadContinuation::Direct(read) => {
                let relation_name = self
                    .model
                    .relation(read.relation)
                    .map_err(|source| CheckError::model("direct_relation_invalid", source))?
                    .name()
                    .clone();
                let contextual = self.contextual.get(&read.object, &relation_name).to_vec();
                if let Err(error) = self.charge_tuple_items(stored.len(), contextual.len()) {
                    return self.complete(work_id, Err(error));
                }
                let rows = contextual.into_iter().chain(stored).collect::<Vec<_>>();
                self.finish_direct(work_id, &read, rows)
            }
            ReadContinuation::TupleToUserset(read) => {
                let relation_name = self
                    .model
                    .relation(read.tupleset)
                    .map_err(|source| CheckError::model("ttu_relation_invalid", source))?
                    .name()
                    .clone();
                let contextual = self.contextual.get(&read.object, &relation_name).to_vec();
                if let Err(error) = self.charge_tuple_items(stored.len(), contextual.len()) {
                    return self.complete(work_id, Err(error));
                }
                let rows = contextual.into_iter().chain(stored).collect::<Vec<_>>();
                self.finish_ttu(work_id, &read, rows)
            }
        }
    }

    fn charge_tuple_items(&mut self, stored: usize, contextual: usize) -> Result<(), CheckError> {
        let contextual_items = u32::try_from(contextual).map_err(|_| tuple_items_exceeded())?;
        let total = stored
            .checked_add(contextual)
            .and_then(|total| u32::try_from(total).ok())
            .ok_or_else(tuple_items_exceeded)?;
        self.counters.tuple_items = self
            .counters
            .tuple_items
            .checked_add(total)
            .ok_or_else(tuple_items_exceeded)?;
        if self.counters.tuple_items > self.budget.maximum_tuple_items() {
            return Err(tuple_items_exceeded());
        }
        if self
            .work_meter
            .as_ref()
            .is_some_and(|meter| !meter.charge_tuple_items(contextual_items))
        {
            return Err(tuple_items_exceeded());
        }
        Ok(())
    }

    fn finish_direct(
        &mut self,
        work_id: WorkId,
        read: &DirectRead,
        rows: Vec<RelationshipTuple>,
    ) -> Result<(), CheckError> {
        let relation = self
            .model
            .relation(read.relation)
            .map_err(|source| CheckError::model("direct_relation_invalid", source))?;
        let restrictions = relation.restrictions().to_vec();
        let mut candidates = Vec::new();
        for tuple in rows {
            let Some(class) = matching_restriction(&tuple, &restrictions, &self.model)? else {
                continue;
            };
            match class {
                DirectClass::Object if tuple.key().subject() == &read.subject => {
                    if let Some(outcome) = self.evaluate_tuple_condition(&tuple)? {
                        candidates.push(Candidate::Immediate(outcome));
                    }
                }
                DirectClass::Wildcard
                    if tuple.key().subject() == &read.subject
                        || wildcard_matches(tuple.key().subject(), &read.subject) =>
                {
                    if let Some(outcome) = self.evaluate_tuple_condition(&tuple)? {
                        candidates.push(Candidate::Immediate(outcome));
                    }
                }
                DirectClass::Userset(relation) => {
                    if let SubjectRef::Userset(userset) = tuple.key().subject()
                        && let Some(outcome) = self.evaluate_tuple_condition(&tuple)?
                    {
                        match outcome {
                            Ok(decision) if decision.allowed => {
                                if tuple.key().subject() == &read.subject {
                                    candidates.push(Candidate::Immediate(Ok(decision)));
                                } else {
                                    candidates.push(Candidate::Semantic(SemanticWork {
                                        object: userset.object().clone(),
                                        relation,
                                        subject: read.subject.clone(),
                                        path: Arc::clone(&read.path),
                                        depth: next_depth(read.depth)?,
                                    }));
                                }
                            }
                            Ok(_) => {}
                            Err(error) => candidates.push(Candidate::Immediate(Err(error))),
                        }
                    }
                }
                _ => {}
            }
        }
        self.start_candidates(work_id, candidates, CheckResolution::Direct)
    }

    fn finish_ttu(
        &mut self,
        work_id: WorkId,
        read: &TtuRead,
        rows: Vec<RelationshipTuple>,
    ) -> Result<(), CheckError> {
        let relation = self
            .model
            .relation(read.tupleset)
            .map_err(|source| CheckError::model("ttu_tupleset_invalid", source))?;
        let restrictions = relation.restrictions().to_vec();
        let mut candidates = Vec::new();
        for tuple in rows {
            if !matches!(
                matching_restriction(&tuple, &restrictions, &self.model)?,
                Some(DirectClass::Object)
            ) {
                continue;
            }
            let condition = self.evaluate_tuple_condition(&tuple)?;
            match condition {
                Some(Ok(decision)) if decision.allowed => {}
                Some(Ok(_)) | None => continue,
                Some(Err(error)) => {
                    candidates.push(Candidate::Immediate(Err(error)));
                    continue;
                }
            }
            let SubjectRef::Object(target_object) = tuple.key().subject() else {
                continue;
            };
            for target in &read.targets {
                let target_relation = self
                    .model
                    .relation(*target)
                    .map_err(|source| CheckError::model("ttu_target_invalid", source))?;
                let target_type = self
                    .model
                    .type_name(target_relation.object_type())
                    .map_err(|source| CheckError::model("ttu_target_type_invalid", source))?;
                if target_type == target_object.object_type() {
                    candidates.push(Candidate::Semantic(SemanticWork {
                        object: target_object.clone(),
                        relation: *target,
                        subject: read.subject.clone(),
                        path: Arc::clone(&read.path),
                        depth: next_depth(read.depth)?,
                    }));
                }
            }
        }
        self.start_candidates(work_id, candidates, CheckResolution::TupleToUserset)
    }

    fn evaluate_tuple_condition(
        &mut self,
        tuple: &RelationshipTuple,
    ) -> Result<Option<Evaluation>, CheckError> {
        match tuple.condition() {
            ConditionReference::Unconditional => {
                Ok(Some(Ok(Decision::allow(CheckResolution::Direct))))
            }
            ConditionReference::Conditional(binding) => {
                self.check_context()?;
                let condition_id = self
                    .model
                    .condition_id(binding.name())
                    .map_err(|source| CheckError::model("tuple_condition_missing", source))?;
                let condition = self
                    .model
                    .condition(condition_id)
                    .map_err(|source| CheckError::model("tuple_condition_invalid", source))?;
                let remaining = self
                    .budget
                    .maximum_condition_cost()
                    .checked_sub(self.counters.condition_cost);
                let Some(remaining) = remaining else {
                    return Ok(Some(Err(condition_cost_exceeded())));
                };
                if remaining == 0 {
                    return Ok(Some(Err(condition_cost_exceeded())));
                }
                let condition_budget = ConditionBudget::new(u64::from(remaining))
                    .map_err(|_| condition_cost_exceeded())?;
                let cancellation = RootConditionCancellation {
                    operation: &self.operation,
                    #[cfg(test)]
                    poll_observer: None,
                    #[cfg(test)]
                    clock: None,
                };
                let evaluated = condition.evaluate(
                    &self.condition_context,
                    binding.context(),
                    condition_budget,
                    &cancellation,
                );
                self.check_context()?;
                match evaluated {
                    Ok(outcome) => {
                        let cost =
                            u32::try_from(outcome.cost()).map_err(|_| condition_cost_exceeded())?;
                        self.counters.condition_cost = self
                            .counters
                            .condition_cost
                            .checked_add(cost)
                            .ok_or_else(condition_cost_exceeded)?;
                        if self.counters.condition_cost > self.budget.maximum_condition_cost() {
                            return Err(condition_cost_exceeded());
                        }
                        if outcome.condition_met() {
                            Ok(Some(Ok(Decision::allow(CheckResolution::Direct))))
                        } else {
                            Ok(None)
                        }
                    }
                    Err(error) if error.kind() == EvaluationErrorKind::CostExceeded => {
                        Ok(Some(Err(condition_cost_exceeded())))
                    }
                    Err(error) if error.kind() == EvaluationErrorKind::Cancelled => {
                        Ok(Some(Err(cancelled("condition_cancelled"))))
                    }
                    Err(error) => Ok(Some(Err(CheckError::condition(error)))),
                }
            }
            _ => Err(internal("condition_reference_unsupported")),
        }
    }

    fn start_candidates(
        &mut self,
        work_id: WorkId,
        candidates: Vec<Candidate>,
        resolution: CheckResolution,
    ) -> Result<(), CheckError> {
        if candidates.is_empty() {
            return self.complete(work_id, Ok(Decision::deny(CheckResolution::Denied)));
        }
        self.slot_mut(work_id)?.state = WorkState::Waiting(Reducer::new(
            ReducerKind::Union,
            candidates.len(),
            Some(resolution),
        ));
        for (operand, candidate) in candidates.into_iter().enumerate() {
            if !self.is_active(work_id)? {
                break;
            }
            match candidate {
                Candidate::Immediate(outcome) => {
                    self.accept_operand(work_id, operand, outcome)?;
                }
                Candidate::Semantic(work) => {
                    self.schedule_child_or_error(work_id, operand, WorkKind::Semantic(work))?;
                }
            }
        }
        Ok(())
    }

    fn schedule_child_or_error(
        &mut self,
        parent: WorkId,
        operand: usize,
        kind: WorkKind,
    ) -> Result<(), CheckError> {
        match self.schedule(
            kind,
            Some(ParentLink {
                work: parent,
                operand,
            }),
        ) {
            Ok(_) => Ok(()),
            Err(error) => self.accept_operand(parent, operand, Err(error)),
        }
    }

    fn accept_operand(
        &mut self,
        work_id: WorkId,
        operand: usize,
        outcome: Evaluation,
    ) -> Result<(), CheckError> {
        let reduced = match &mut self.slot_mut(work_id)?.state {
            WorkState::Waiting(reducer) => reducer.accept(operand, outcome)?,
            _ => return Err(internal("parent_reducer_state_invalid")),
        };
        if let Some(outcome) = reduced {
            self.complete(work_id, outcome)?;
        }
        Ok(())
    }

    fn complete(&mut self, mut work_id: WorkId, mut outcome: Evaluation) -> Result<(), CheckError> {
        loop {
            let (parent, memo_key) = {
                let slot = self.slot_mut(work_id)?;
                let memo_key = slot.memo_key.take();
                slot.state = WorkState::Completed;
                (slot.parent, memo_key)
            };
            if let (Some(memo_key), Ok(decision)) = (memo_key, &outcome) {
                self.memo.insert(memo_key, *decision);
            }
            let Some(parent) = parent else {
                self.root_result = Some(outcome);
                return Ok(());
            };
            let reduced = match &mut self.slot_mut(parent.work)?.state {
                WorkState::Waiting(reducer) => reducer.accept(parent.operand, outcome)?,
                WorkState::Completed => return Ok(()),
                _ => return Err(internal("completion_parent_state_invalid")),
            };
            let Some(parent_outcome) = reduced else {
                return Ok(());
            };
            work_id = parent.work;
            outcome = parent_outcome;
        }
    }

    fn is_active(&self, work_id: WorkId) -> Result<bool, CheckError> {
        let mut current = Some(work_id);
        while let Some(id) = current {
            let slot = self
                .slots
                .get(id)
                .ok_or_else(|| internal("work_slot_invalid"))?;
            if id != work_id && matches!(slot.state, WorkState::Completed) {
                return Ok(false);
            }
            current = slot.parent.map(|parent| parent.work);
        }
        Ok(true)
    }

    fn slot_mut(&mut self, work_id: WorkId) -> Result<&mut WorkSlot, CheckError> {
        self.slots
            .get_mut(work_id)
            .ok_or_else(|| internal("work_slot_invalid"))
    }
}

#[derive(Debug)]
enum Candidate {
    Immediate(Evaluation),
    Semantic(SemanticWork),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectClass {
    Object,
    Userset(RelationId),
    Wildcard,
}

fn matching_restriction(
    tuple: &RelationshipTuple,
    restrictions: &[DirectRestriction],
    model: &CompiledModel,
) -> Result<Option<DirectClass>, CheckError> {
    let Ok(subject_type) = model.type_id(tuple.key().subject().subject_type()) else {
        return Ok(None);
    };
    let class = match tuple.key().subject() {
        SubjectRef::Object(_) => DirectClass::Object,
        SubjectRef::TypedWildcard(_) => DirectClass::Wildcard,
        SubjectRef::Userset(userset) => {
            let Ok(relation) =
                model.relation_id(userset.object().object_type(), userset.relation())
            else {
                return Ok(None);
            };
            DirectClass::Userset(relation)
        }
        _ => return Err(internal("subject_reference_unsupported")),
    };
    for restriction in restrictions {
        if restriction.subject_type() != subject_type
            || !restriction_kind_matches(restriction.kind(), class)
        {
            continue;
        }
        if condition_requirement_matches(restriction.condition(), tuple.condition(), model)? {
            return Ok(Some(class));
        }
    }
    Ok(None)
}

fn restriction_kind_matches(kind: RestrictionKind, class: DirectClass) -> bool {
    matches!(
        (kind, class),
        (RestrictionKind::Object, DirectClass::Object)
            | (RestrictionKind::Wildcard, DirectClass::Wildcard)
            | (RestrictionKind::Userset(_), DirectClass::Userset(_))
    ) && match (kind, class) {
        (RestrictionKind::Userset(expected), DirectClass::Userset(actual)) => expected == actual,
        _ => true,
    }
}

fn condition_requirement_matches(
    requirement: ConditionRequirement,
    reference: &ConditionReference,
    model: &CompiledModel,
) -> Result<bool, CheckError> {
    match (requirement, reference) {
        (ConditionRequirement::Unconditional, ConditionReference::Unconditional) => Ok(true),
        (
            ConditionRequirement::Required(condition_id),
            ConditionReference::Conditional(binding),
        ) => {
            let condition = model
                .condition(condition_id)
                .map_err(|source| CheckError::model("restriction_condition_invalid", source))?;
            Ok(condition.name() == binding.name())
        }
        _ => Ok(false),
    }
}

fn wildcard_matches(tuple_subject: &SubjectRef, query_subject: &SubjectRef) -> bool {
    matches!(
        (tuple_subject, query_subject),
        (SubjectRef::TypedWildcard(wildcard), SubjectRef::Object(object))
            if wildcard == object.object_type()
    )
}

const fn next_depth(depth: u32) -> Result<u32, CheckError> {
    match depth.checked_add(1) {
        Some(depth) => Ok(depth),
        None => Err(CheckError::new(
            CheckErrorKind::DepthExceeded,
            "check_depth_exceeded",
        )),
    }
}

fn map_read_error(error: openfga_storage::StorageError) -> CheckError {
    if error.kind() == StorageErrorKind::ResourceExhausted {
        tuple_items_exceeded()
    } else {
        CheckError::from(error)
    }
}

const fn depth_exceeded() -> CheckError {
    CheckError::new(CheckErrorKind::DepthExceeded, "check_depth_exceeded")
}

const fn dispatch_exceeded() -> CheckError {
    CheckError::new(CheckErrorKind::DispatchExceeded, "check_dispatch_exceeded")
}

const fn datastore_exceeded() -> CheckError {
    CheckError::new(
        CheckErrorKind::DatastoreQueryExceeded,
        "check_datastore_query_exceeded",
    )
}

const fn tuple_items_exceeded() -> CheckError {
    CheckError::new(
        CheckErrorKind::TupleItemExceeded,
        "check_tuple_items_exceeded",
    )
}

const fn condition_cost_exceeded() -> CheckError {
    CheckError::new(
        CheckErrorKind::ConditionCostExceeded,
        "check_condition_cost_exceeded",
    )
}

const fn cancelled(code: &'static str) -> CheckError {
    CheckError::new(CheckErrorKind::Cancelled, code)
}

const fn timed_out(code: &'static str) -> CheckError {
    CheckError::new(CheckErrorKind::Timeout, code)
}

fn canonicalize_check_control_error(error: CheckError) -> CheckError {
    match error.kind() {
        CheckErrorKind::Cancelled => error.with_code(CHECK_CANCELLED_CODE),
        CheckErrorKind::Timeout => error.with_code(CHECK_DEADLINE_ELAPSED_CODE),
        _ => error,
    }
}

const fn internal(code: &'static str) -> CheckError {
    CheckError::new(CheckErrorKind::Internal, code)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        error::Error,
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant},
    };

    use openfga_condition::{
        ConditionCompiler, ConditionDefinition, ConditionLimits, EvaluationBudget,
        EvaluationErrorKind,
    };
    use openfga_domain::{ConditionContext, ConsistencyPreference, Deadline, RequestTimeout};
    use openfga_storage::{OperationContext, StorageCancellationToken};

    use super::{
        CheckError, CheckErrorKind, CheckResolution, Decision, Evaluation, Reducer, ReducerKind,
        RootConditionCancellation,
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Branch {
        Allow,
        Deny,
        Error,
    }

    #[test]
    fn test_should_exhaustively_apply_reducer_truth_tables_and_error_precedence() {
        let branches = [Branch::Allow, Branch::Deny, Branch::Error];
        for kind in [
            ReducerKind::Union,
            ReducerKind::Intersection,
            ReducerKind::Difference,
        ] {
            for left in branches {
                for right in branches {
                    for order in [[0_usize, 1_usize], [1_usize, 0_usize]] {
                        let actual = reduce(kind, [left, right], order);
                        assert_reduction(kind, left, right, actual);
                    }
                }
            }
        }
    }

    #[test]
    fn test_should_poll_root_cancellation_during_condition_evaluation() -> Result<(), Box<dyn Error>>
    {
        let cancellation = StorageCancellationToken::new();
        let operation = operation_context(Instant::now(), cancellation.clone())?;
        let polls = AtomicUsize::new(0);
        let observer = || {
            if polls.fetch_add(1, Ordering::AcqRel) == 2 {
                cancellation.cancel();
            }
        };
        let signal = RootConditionCancellation {
            operation: &operation,
            poll_observer: Some(&observer),
            clock: None,
        };
        let error = compiled_poll_condition()?
            .evaluate(
                &ConditionContext::empty(),
                &ConditionContext::empty(),
                EvaluationBudget::new(100)?,
                &signal,
            )
            .err()
            .ok_or("condition ignored root cancellation")?;
        assert_eq!(error.kind(), EvaluationErrorKind::Cancelled);
        assert!(polls.load(Ordering::Acquire) >= 3);
        Ok(())
    }

    #[test]
    fn test_should_poll_root_deadline_during_condition_evaluation() -> Result<(), Box<dyn Error>> {
        let start = Instant::now();
        let operation = operation_context(start, StorageCancellationToken::new())?;
        let polls = AtomicUsize::new(0);
        let clock = || {
            if polls.fetch_add(1, Ordering::AcqRel) < 2 {
                start
            } else {
                start + Duration::from_secs(2)
            }
        };
        let signal = RootConditionCancellation {
            operation: &operation,
            poll_observer: None,
            clock: Some(&clock),
        };
        let error = compiled_poll_condition()?
            .evaluate(
                &ConditionContext::empty(),
                &ConditionContext::empty(),
                EvaluationBudget::new(100)?,
                &signal,
            )
            .err()
            .ok_or("condition ignored root deadline")?;
        assert_eq!(error.kind(), EvaluationErrorKind::Cancelled);
        assert!(polls.load(Ordering::Acquire) >= 3);
        Ok(())
    }

    fn operation_context(
        start: Instant,
        cancellation: StorageCancellationToken,
    ) -> Result<OperationContext, Box<dyn Error>> {
        Ok(OperationContext::new(
            ConsistencyPreference::HigherConsistency,
            Deadline::from_timeout(start, RequestTimeout::new(Duration::from_secs(1))?)?,
            cancellation,
        ))
    }

    fn compiled_poll_condition() -> Result<openfga_condition::CompiledCondition, Box<dyn Error>> {
        Ok(ConditionCompiler::default().compile(
            &ConditionDefinition::new(
                "poll_root".parse()?,
                "true && true && true && true".to_owned(),
                BTreeMap::new(),
            ),
            &ConditionLimits::default(),
        )?)
    }

    fn reduce(kind: ReducerKind, branches: [Branch; 2], order: [usize; 2]) -> Evaluation {
        let mut reducer = Reducer::new(kind, 2, None);
        for operand in order {
            let branch = branches.get(operand).copied().unwrap_or(Branch::Error);
            let result = reducer.accept(operand, outcome(branch, operand));
            assert!(result.is_ok());
            if let Ok(Some(outcome)) = result {
                return outcome;
            }
        }
        Err(CheckError::new(
            CheckErrorKind::Internal,
            "test_reducer_incomplete",
        ))
    }

    fn outcome(branch: Branch, operand: usize) -> Evaluation {
        match branch {
            Branch::Allow => Ok(Decision::allow(CheckResolution::Direct)),
            Branch::Deny => Ok(Decision::deny(CheckResolution::Denied)),
            Branch::Error if operand == 0 => Err(CheckError::new(
                CheckErrorKind::StorageUnavailable,
                "operand_zero_error",
            )),
            Branch::Error => Err(CheckError::new(
                CheckErrorKind::Condition,
                "operand_one_error",
            )),
        }
    }

    fn assert_reduction(kind: ReducerKind, left: Branch, right: Branch, actual: Evaluation) {
        let expected = expected(kind, left, right);
        let (actual, error_code) = match actual {
            Ok(decision) if decision.allowed => (Branch::Allow, None),
            Ok(_) => (Branch::Deny, None),
            Err(error) => (Branch::Error, Some(error.code())),
        };
        assert_eq!(actual, expected);
        if expected == Branch::Error {
            let expected_code = if left == Branch::Error {
                "operand_zero_error"
            } else {
                "operand_one_error"
            };
            assert_eq!(error_code, Some(expected_code));
        }
    }

    const fn expected(kind: ReducerKind, left: Branch, right: Branch) -> Branch {
        match kind {
            ReducerKind::Union
                if matches!(left, Branch::Allow) || matches!(right, Branch::Allow) =>
            {
                Branch::Allow
            }
            ReducerKind::Union
                if matches!(left, Branch::Error) || matches!(right, Branch::Error) =>
            {
                Branch::Error
            }
            ReducerKind::Union => Branch::Deny,
            ReducerKind::Intersection
                if matches!(left, Branch::Deny) || matches!(right, Branch::Deny) =>
            {
                Branch::Deny
            }
            ReducerKind::Intersection
                if matches!(left, Branch::Error) || matches!(right, Branch::Error) =>
            {
                Branch::Error
            }
            ReducerKind::Intersection => Branch::Allow,
            ReducerKind::Difference
                if matches!(left, Branch::Deny) || matches!(right, Branch::Allow) =>
            {
                Branch::Deny
            }
            ReducerKind::Difference if matches!((left, right), (Branch::Allow, Branch::Deny)) => {
                Branch::Allow
            }
            ReducerKind::Difference | ReducerKind::Single => Branch::Error,
        }
    }
}
