//! Residual-Check `ListObjects` collection and backpressured streaming.
//!
//! `async-trait` preserves object safety because the service layer owns the
//! engine through `Arc<dyn ListObjectsEngine>`.

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use openfga_check::{CheckEvaluator, CheckOutcome, CheckWorkMeter, DirectCheckEvaluator};
use openfga_domain::{CheckCommand, InputLimits, ListObjectsCommand, ObjectRef, TupleKey};
use openfga_model::CompiledModel;
use openfga_storage::{StorageCancellationToken, TupleReader};
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
    time::{Instant as TokioInstant, sleep_until},
};
use tokio_stream::Stream;

use crate::{
    Candidate, CandidateMetadata, CandidateSet, ListError, ListErrorKind, ListObjectsBudget,
    ReverseCandidateTraversal,
};

/// Resource accounting for one completed `ListObjects` query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ListObjectsMetadata {
    candidate: CandidateMetadata,
    residual_checks: u32,
    results: u32,
}

impl ListObjectsMetadata {
    /// Returns reverse-candidate traversal accounting.
    #[must_use]
    pub const fn candidate(self) -> CandidateMetadata {
        self.candidate
    }

    /// Returns residual oracle evaluations started.
    #[must_use]
    pub const fn residual_checks(self) -> u32 {
        self.residual_checks
    }

    /// Returns emitted result count.
    #[must_use]
    pub const fn results(self) -> u32 {
        self.results
    }
}

/// One bounded unary `ListObjects` result.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ListObjectsOutcome {
    objects: Box<[ObjectRef]>,
    metadata: ListObjectsMetadata,
}

impl ListObjectsOutcome {
    /// Returns canonical deduplicated result objects.
    #[must_use]
    pub const fn objects(&self) -> &[ObjectRef] {
        &self.objects
    }

    /// Returns finite query accounting.
    #[must_use]
    pub const fn metadata(&self) -> ListObjectsMetadata {
        self.metadata
    }
}

/// Backpressured object stream that cancels and joins its producer on drop.
#[non_exhaustive]
pub struct ListObjectsStream {
    receiver: mpsc::Receiver<Result<ObjectRef, ListError>>,
    task: Option<JoinHandle<()>>,
    cancellation: StorageCancellationToken,
}

impl Stream for ListObjectsStream {
    type Item = Result<ObjectRef, ListError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(context) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                let Some(task) = self.task.as_mut() else {
                    return Poll::Ready(None);
                };
                match Pin::new(task).poll(context) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(_)) => {
                        self.task = None;
                        Poll::Ready(Some(Err(ListError::new(
                            ListErrorKind::Internal,
                            "list_stream_task_failed",
                        ))))
                    }
                    Poll::Ready(Ok(())) => {
                        self.task = None;
                        Poll::Ready(None)
                    }
                }
            }
        }
    }
}

impl Drop for ListObjectsStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let Some(task) = self.task.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _joined = task.await;
            });
        }
    }
}

impl fmt::Debug for ListObjectsStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListObjectsStream")
            .field("buffered", &self.receiver.len())
            .field("producer_active", &self.task.is_some())
            .finish_non_exhaustive()
    }
}

/// Object-safe unary and streaming enumeration contract.
#[async_trait]
pub trait ListObjectsEngine: Send + Sync {
    /// Collects one bounded unary result.
    ///
    /// # Errors
    ///
    /// Returns model, tuple, condition, storage, cancellation, deadline, or
    /// independent resource failures without returning partial results.
    async fn list_objects(
        &self,
        command: &ListObjectsCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ListObjectsBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<ListObjectsOutcome, ListError>;

    /// Starts one bounded backpressured result stream.
    ///
    /// # Errors
    ///
    /// Returns failures found during candidate discovery. Residual evaluation
    /// failures are delivered as terminal stream items.
    async fn streamed_list_objects(
        &self,
        command: &ListObjectsCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ListObjectsBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<ListObjectsStream, ListError>;
}

/// Correctness-first reverse traversal plus residual Check implementation.
#[derive(Clone)]
#[non_exhaustive]
pub struct DirectListObjectsEngine {
    traversal: ReverseCandidateTraversal,
    evaluator: Arc<dyn CheckEvaluator>,
}

impl DirectListObjectsEngine {
    /// Creates an engine with an explicit permanent Check oracle.
    #[must_use]
    pub fn new(input_limits: InputLimits, evaluator: Arc<dyn CheckEvaluator>) -> Self {
        Self {
            traversal: ReverseCandidateTraversal::new(input_limits),
            evaluator,
        }
    }
}

impl Default for DirectListObjectsEngine {
    fn default() -> Self {
        Self::new(
            InputLimits::default(),
            Arc::new(DirectCheckEvaluator::default()),
        )
    }
}

impl fmt::Debug for DirectListObjectsEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectListObjectsEngine")
            .field("traversal", &self.traversal)
            .field("evaluator", &"dyn CheckEvaluator")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ListObjectsEngine for DirectListObjectsEngine {
    async fn list_objects(
        &self,
        command: &ListObjectsCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ListObjectsBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<ListObjectsOutcome, ListError> {
        let candidates = self
            .traversal
            .traverse(
                command,
                Arc::clone(&model),
                Arc::clone(&tuples),
                budget.candidate(),
                cancellation.clone(),
            )
            .await?;
        let mut sink = VecSink::default();
        let metadata = process_candidates(
            command,
            candidates,
            model,
            tuples,
            Arc::clone(&self.evaluator),
            budget,
            cancellation,
            &mut sink,
        )
        .await?;
        Ok(ListObjectsOutcome {
            objects: sink.objects.into_boxed_slice(),
            metadata,
        })
    }

    async fn streamed_list_objects(
        &self,
        command: &ListObjectsCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ListObjectsBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<ListObjectsStream, ListError> {
        let candidates = self
            .traversal
            .traverse(
                command,
                Arc::clone(&model),
                Arc::clone(&tuples),
                budget.candidate(),
                cancellation.clone(),
            )
            .await?;
        let (sender, receiver) = mpsc::channel(budget.stream_buffer());
        let producer_cancellation = cancellation.clone();
        let evaluator = Arc::clone(&self.evaluator);
        let command = command.clone();
        let task = tokio::spawn(async move {
            let mut sink = ChannelSink {
                sender: sender.clone(),
                cancellation: producer_cancellation.clone(),
            };
            let result = process_candidates(
                &command,
                candidates,
                model,
                tuples,
                evaluator,
                budget,
                producer_cancellation,
                &mut sink,
            )
            .await;
            if let Err(error) = result {
                let _sent = sender.send(Err(error)).await;
            }
        });
        Ok(ListObjectsStream {
            receiver,
            task: Some(task),
            cancellation,
        })
    }
}

#[async_trait]
trait ObjectSink: Send {
    async fn send(&mut self, object: ObjectRef) -> bool;
}

#[derive(Debug, Default)]
struct VecSink {
    objects: Vec<ObjectRef>,
}

#[async_trait]
impl ObjectSink for VecSink {
    async fn send(&mut self, object: ObjectRef) -> bool {
        self.objects.push(object);
        true
    }
}

#[derive(Debug)]
struct ChannelSink {
    sender: mpsc::Sender<Result<ObjectRef, ListError>>,
    cancellation: StorageCancellationToken,
}

#[async_trait]
impl ObjectSink for ChannelSink {
    async fn send(&mut self, object: ObjectRef) -> bool {
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => false,
            result = self.sender.send(Ok(object)) => result.is_ok(),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "engine capabilities and policy are explicit"
)]
async fn process_candidates<S: ObjectSink>(
    command: &ListObjectsCommand,
    candidates: CandidateSet,
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    evaluator: Arc<dyn CheckEvaluator>,
    budget: ListObjectsBudget,
    cancellation: StorageCancellationToken,
    sink: &mut S,
) -> Result<ListObjectsMetadata, ListError> {
    let candidate_metadata = candidates.metadata();
    let mut candidates = candidates
        .candidates()
        .iter()
        .cloned()
        .collect::<VecDeque<_>>();
    let mut checks = JoinSet::new();
    let runtime = ResidualRuntime {
        model,
        tuples,
        evaluator,
        budget,
        work_meter: budget.residual_work_meter(),
        cancellation: cancellation.clone(),
    };
    let mut residual_checks = 0_u32;
    let mut results = 0_u32;
    let maximum_results = command.control().maximum_results().get();
    while !candidates.is_empty() || !checks.is_empty() {
        while checks.len() < budget.maximum_residual_concurrency() {
            let Some(candidate) = candidates.pop_front() else {
                break;
            };
            if !candidate.requires_check() {
                if !emit(sink, candidate.object().clone(), &mut results).await {
                    abort_and_join(&mut checks).await;
                    return Ok(metadata(candidate_metadata, residual_checks, results));
                }
                if results >= maximum_results {
                    abort_and_join(&mut checks).await;
                    return Ok(metadata(candidate_metadata, residual_checks, results));
                }
                continue;
            }
            let Some(next_residual_checks) = residual_checks.checked_add(1) else {
                abort_and_join(&mut checks).await;
                return Err(internal("list_residual_check_count_overflow"));
            };
            residual_checks = next_residual_checks;
            spawn_check(&mut checks, command, candidate, &runtime);
        }
        if checks.is_empty() {
            continue;
        }
        let deadline = TokioInstant::from_std(command.query().deadline().instant());
        let joined = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                abort_and_join(&mut checks).await;
                return Err(ListError::new(ListErrorKind::Cancelled, "list_objects_cancelled"));
            }
            () = sleep_until(deadline) => {
                abort_and_join(&mut checks).await;
                return Err(ListError::new(ListErrorKind::Timeout, "list_objects_deadline_elapsed"));
            }
            joined = checks.join_next() => joined,
        };
        let Some(joined) = joined else {
            continue;
        };
        let Ok((object, outcome)) = joined else {
            abort_and_join(&mut checks).await;
            return Err(internal("list_residual_check_task_failed"));
        };
        let outcome = abort_on_error(outcome, &mut checks).await?;
        if outcome.allowed() {
            if !emit(sink, object, &mut results).await {
                abort_and_join(&mut checks).await;
                return Ok(metadata(candidate_metadata, residual_checks, results));
            }
            if results >= maximum_results {
                abort_and_join(&mut checks).await;
                return Ok(metadata(candidate_metadata, residual_checks, results));
            }
        }
    }
    Ok(metadata(candidate_metadata, residual_checks, results))
}

#[derive(Clone)]
struct ResidualRuntime {
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    evaluator: Arc<dyn CheckEvaluator>,
    budget: ListObjectsBudget,
    work_meter: CheckWorkMeter,
    cancellation: StorageCancellationToken,
}

fn spawn_check(
    checks: &mut JoinSet<(ObjectRef, Result<CheckOutcome, ListError>)>,
    command: &ListObjectsCommand,
    candidate: Candidate,
    runtime: &ResidualRuntime,
) {
    let query = command.query().clone();
    let relation = command.relation().clone();
    let subject = command.subject().clone();
    let runtime = runtime.clone();
    checks.spawn(async move {
        let object = candidate.object().clone();
        let check = CheckCommand::new(query, TupleKey::new(object.clone(), relation, subject));
        let outcome = runtime
            .evaluator
            .check(
                &check,
                runtime.model,
                runtime.tuples,
                runtime.budget.check(),
                Some(runtime.work_meter),
                runtime.cancellation,
            )
            .await
            .map_err(ListError::from);
        (object, outcome)
    });
}

async fn emit<S: ObjectSink>(sink: &mut S, object: ObjectRef, results: &mut u32) -> bool {
    if !sink.send(object).await {
        return false;
    }
    *results = results.saturating_add(1);
    true
}

async fn abort_and_join<T: 'static>(tasks: &mut JoinSet<T>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

async fn abort_on_error<T, U: 'static>(
    result: Result<T, ListError>,
    tasks: &mut JoinSet<U>,
) -> Result<T, ListError> {
    if result.is_err() {
        abort_and_join(tasks).await;
    }
    result
}

const fn metadata(
    candidate: CandidateMetadata,
    residual_checks: u32,
    results: u32,
) -> ListObjectsMetadata {
    ListObjectsMetadata {
        candidate,
        residual_checks,
        results,
    }
}

const fn internal(code: &'static str) -> ListError {
    ListError::new(ListErrorKind::Internal, code)
}
