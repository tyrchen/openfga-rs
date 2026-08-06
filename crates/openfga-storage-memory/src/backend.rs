//! Public memory backend handle, lifecycle, dispatch, and capability implementations.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use openfga_domain::{
    AuthorizationModelId, ChangeId, RelationshipTuple, StoreId, TupleKey, TypeName,
};
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ChangeReader, HealthCheck, HealthStatus,
    ModelReader, ModelWriter, MutationOutcome, ObjectRelationFilter, OperationContext, Page,
    PageOptions, ReadOptions, ReverseTupleFilter, StorageError, StorageErrorKind, StoreName,
    StoreReader, StoreRecord, StoreWriter, StoredAuthorizationModel, StoredTuple, TupleChange,
    TupleReader, TupleStream, TupleWriteOptions, TupleWriter, UsersetTupleFilter,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};

use crate::{
    MemoryStorageConfig, MutationFaultInjector, NoMutationFaults, StorageClock, SystemStorageClock,
    actor::{ActorMessage, Command, Envelope, Reply, run_actor},
    state::MemoryState,
};

#[derive(Debug)]
struct DiagnosticsState {
    running: Arc<AtomicBool>,
    active_operations: Arc<AtomicUsize>,
}

/// Cloneable non-sensitive memory actor diagnostics.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct MemoryDiagnostics(Arc<DiagnosticsState>);

impl MemoryDiagnostics {
    /// Returns whether the actor task currently owns live state.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.0.running.load(Ordering::Acquire)
    }

    /// Returns the number of dispatched operations awaiting completion.
    #[must_use]
    pub fn active_operations(&self) -> usize {
        self.0.active_operations.load(Ordering::Acquire)
    }
}

/// Supervised handle for one actor-owned in-memory backend.
pub struct MemoryStorage {
    config: MemoryStorageConfig,
    clock: Arc<dyn StorageClock>,
    faults: Arc<dyn MutationFaultInjector>,
    sender: mpsc::Sender<ActorMessage>,
    join: Option<JoinHandle<()>>,
    diagnostics: MemoryDiagnostics,
}

impl MemoryStorage {
    /// Starts an empty memory actor using the system transaction clock.
    ///
    /// # Errors
    ///
    /// Returns unavailable when called outside a Tokio runtime.
    pub fn start(config: MemoryStorageConfig) -> Result<Self, StorageError> {
        Self::start_with_components(
            config,
            Arc::new(SystemStorageClock),
            Arc::new(NoMutationFaults),
        )
    }

    /// Starts an empty actor with injected clock and mutation fault boundary.
    ///
    /// # Errors
    ///
    /// Returns unavailable when called outside a Tokio runtime.
    pub fn start_with_components(
        config: MemoryStorageConfig,
        clock: Arc<dyn StorageClock>,
        faults: Arc<dyn MutationFaultInjector>,
    ) -> Result<Self, StorageError> {
        let running = Arc::new(AtomicBool::new(true));
        let active_operations = Arc::new(AtomicUsize::new(0));
        let diagnostics = MemoryDiagnostics(Arc::new(DiagnosticsState {
            running,
            active_operations,
        }));
        let (sender, join) = spawn_actor(
            &config,
            Arc::clone(&clock),
            Arc::clone(&faults),
            &diagnostics,
        )?;
        Ok(Self {
            config,
            clock,
            faults,
            sender,
            join: Some(join),
            diagnostics,
        })
    }

    /// Returns cloneable non-sensitive actor diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> MemoryDiagnostics {
        self.diagnostics.clone()
    }

    /// Gracefully drains queued commands, stops the actor, and joins its task.
    ///
    /// Repeated calls are harmless.
    ///
    /// # Errors
    ///
    /// Returns timeout, unavailable, or internal join failure. A timed-out actor
    /// is aborted and joined before this method returns.
    pub async fn stop(&mut self) -> Result<(), StorageError> {
        let Some(mut join) = self.join.take() else {
            return Ok(());
        };
        let (reply, received) = oneshot::channel();
        let shutdown = self.sender.send(ActorMessage::Shutdown(reply));
        match timeout(self.config.shutdown_timeout(), shutdown).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let joined = join.await;
                self.diagnostics.0.running.store(false, Ordering::Release);
                return match joined {
                    Ok(()) => Err(StorageError::with_source(
                        StorageErrorKind::Unavailable,
                        "memory_actor_shutdown_channel_closed",
                        error,
                    )),
                    Err(join_error) => Err(StorageError::with_source(
                        StorageErrorKind::Internal,
                        "memory_actor_join_failed",
                        join_error,
                    )),
                };
            }
            Err(_) => return abort_timed_out(&mut join, &self.diagnostics).await,
        }
        if timeout(self.config.shutdown_timeout(), received)
            .await
            .is_err()
        {
            return abort_timed_out(&mut join, &self.diagnostics).await;
        }
        match timeout(self.config.shutdown_timeout(), &mut join).await {
            Ok(Ok(())) => {
                self.diagnostics.0.running.store(false, Ordering::Release);
                Ok(())
            }
            Ok(Err(error)) => {
                self.diagnostics.0.running.store(false, Ordering::Release);
                Err(StorageError::with_source(
                    StorageErrorKind::Internal,
                    "memory_actor_join_failed",
                    error,
                ))
            }
            Err(_) => abort_timed_out(&mut join, &self.diagnostics).await,
        }
    }

    /// Restarts the backend with empty actor-owned state after a graceful stop.
    ///
    /// # Errors
    ///
    /// Returns a spawn failure for the replacement actor. Any previous actor is
    /// first joined or forcibly aborted, so a failed/panicked actor is recoverable.
    pub async fn restart(&mut self) -> Result<(), StorageError> {
        let _previous_status = self.stop().await;
        let (sender, join) = spawn_actor(
            &self.config,
            Arc::clone(&self.clock),
            Arc::clone(&self.faults),
            &self.diagnostics,
        )?;
        self.diagnostics.0.running.store(true, Ordering::Release);
        self.sender = sender;
        self.join = Some(join);
        Ok(())
    }

    async fn dispatch<T>(
        &self,
        context: &OperationContext,
        build: impl FnOnce(Reply<T>) -> Command,
    ) -> Result<T, StorageError> {
        context.check()?;
        let _guard = ActiveOperationGuard::new(Arc::clone(&self.diagnostics.0.active_operations));
        let (reply, received) = oneshot::channel();
        let message = ActorMessage::Operation(Envelope {
            context: context.clone(),
            command: build(reply),
        });
        let deadline = Instant::from_std(context.deadline().instant());
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => return Err(cancelled()),
            () = sleep_until(deadline) => return Err(timed_out()),
            result = self.sender.send(message) => result.map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Unavailable,
                    "memory_actor_command_channel_closed",
                    error,
                )
            })?,
        }
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => Err(cancelled()),
            () = sleep_until(deadline) => Err(timed_out()),
            result = received => result.map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Unavailable,
                    "memory_actor_reply_channel_closed",
                    error,
                )
            })?,
        }
    }
}

impl fmt::Debug for MemoryStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryStorage")
            .field("config", &self.config)
            .field("diagnostics", &self.diagnostics)
            .finish_non_exhaustive()
    }
}

impl Drop for MemoryStorage {
    fn drop(&mut self) {
        if self.join.is_some() {
            let (reply, _received) = oneshot::channel();
            let _ = self.sender.try_send(ActorMessage::Shutdown(reply));
        }
    }
}

#[async_trait]
impl TupleReader for MemoryStorage {
    async fn read_exact_tuple(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<StoredTuple, StorageError> {
        self.dispatch(context, |reply| Command::ReadExact {
            store_id,
            key: key.clone(),
            reply,
        })
        .await
    }

    async fn read_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        self.dispatch(context, |reply| Command::ReadObjectRelation {
            store_id,
            filter: filter.clone(),
            options,
            reply,
        })
        .await
        .map(TupleStream::from_tuples)
    }

    async fn read_userset_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &UsersetTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        self.dispatch(context, |reply| Command::ReadUserset {
            store_id,
            filter: filter.clone(),
            options,
            reply,
        })
        .await
        .map(TupleStream::from_tuples)
    }

    async fn read_reverse_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ReverseTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        self.dispatch(context, |reply| Command::ReadReverse {
            store_id,
            filter: filter.clone(),
            options,
            reply,
        })
        .await
        .map(TupleStream::from_tuples)
    }

    async fn tuple_exists(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<bool, StorageError> {
        self.dispatch(context, |reply| Command::TupleExists {
            store_id,
            key: key.clone(),
            reply,
        })
        .await
    }

    async fn count_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError> {
        self.dispatch(context, |reply| Command::CountObjectRelation {
            store_id,
            filter: filter.clone(),
            reply,
        })
        .await
    }
}

#[async_trait]
impl TupleWriter for MemoryStorage {
    async fn write_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, StorageError> {
        self.dispatch(context, |reply| Command::WriteTuples {
            store_id,
            deletes,
            writes,
            options,
            reply,
        })
        .await
    }
}

#[async_trait]
impl ModelReader for MemoryStorage {
    async fn read_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        self.dispatch(context, |reply| Command::ReadModel {
            store_id,
            model_id,
            reply,
        })
        .await
    }

    async fn read_latest_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        self.dispatch(context, |reply| Command::ReadLatestModel {
            store_id,
            reply,
        })
        .await
    }

    async fn list_models(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        options: &PageOptions,
    ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError> {
        self.dispatch(context, |reply| Command::ListModels {
            store_id,
            options: options.clone(),
            reply,
        })
        .await
    }
}

#[async_trait]
impl ModelWriter for MemoryStorage {
    async fn write_model(
        &self,
        context: &OperationContext,
        model: Arc<StoredAuthorizationModel>,
    ) -> Result<(), StorageError> {
        self.dispatch(context, |reply| Command::WriteModel { model, reply })
            .await
    }
}

#[async_trait]
impl StoreReader for MemoryStorage {
    async fn read_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<StoreRecord, StorageError> {
        self.dispatch(context, |reply| Command::ReadStore { store_id, reply })
            .await
    }

    async fn list_stores(
        &self,
        context: &OperationContext,
        options: &PageOptions,
    ) -> Result<Page<StoreRecord>, StorageError> {
        self.dispatch(context, |reply| Command::ListStores {
            options: options.clone(),
            reply,
        })
        .await
    }
}

#[async_trait]
impl StoreWriter for MemoryStorage {
    async fn create_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError> {
        self.dispatch(context, |reply| Command::CreateStore {
            store_id,
            name,
            reply,
        })
        .await
    }

    async fn rename_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError> {
        self.dispatch(context, |reply| Command::RenameStore {
            store_id,
            name,
            reply,
        })
        .await
    }

    async fn delete_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<(), StorageError> {
        self.dispatch(context, |reply| Command::DeleteStore { store_id, reply })
            .await
    }
}

#[async_trait]
impl AssertionReader for MemoryStorage {
    async fn read_assertions(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<[Assertion]>, StorageError> {
        self.dispatch(context, |reply| Command::ReadAssertions {
            store_id,
            model_id,
            reply,
        })
        .await
    }
}

#[async_trait]
impl AssertionWriter for MemoryStorage {
    async fn write_assertions(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
        assertions: Vec<Assertion>,
    ) -> Result<(), StorageError> {
        self.dispatch(context, |reply| Command::WriteAssertions {
            store_id,
            model_id,
            assertions,
            reply,
        })
        .await
    }
}

#[async_trait]
impl ChangeReader for MemoryStorage {
    async fn read_changes(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        object_type: Option<&TypeName>,
        after: Option<ChangeId>,
        options: ReadOptions,
    ) -> Result<Vec<TupleChange>, StorageError> {
        self.dispatch(context, |reply| Command::ReadChanges {
            store_id,
            object_type: object_type.cloned(),
            after,
            options,
            reply,
        })
        .await
    }
}

#[async_trait]
impl HealthCheck for MemoryStorage {
    async fn health(&self, context: &OperationContext) -> Result<HealthStatus, StorageError> {
        self.dispatch(context, |reply| Command::Health { reply })
            .await
    }
}

fn spawn_actor(
    config: &MemoryStorageConfig,
    clock: Arc<dyn StorageClock>,
    faults: Arc<dyn MutationFaultInjector>,
    diagnostics: &MemoryDiagnostics,
) -> Result<(mpsc::Sender<ActorMessage>, JoinHandle<()>), StorageError> {
    let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Unavailable,
            "memory_actor_requires_tokio_runtime",
            error,
        )
    })?;
    let (sender, receiver) = mpsc::channel(config.channel_capacity());
    let state = MemoryState::new(config.input_limits().clone(), clock, faults);
    let running = Arc::clone(&diagnostics.0.running);
    let join = runtime.spawn(run_actor(receiver, state, running));
    Ok((sender, join))
}

struct ActiveOperationGuard(Arc<AtomicUsize>);

impl ActiveOperationGuard {
    fn new(active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::AcqRel);
        Self(active)
    }
}

impl Drop for ActiveOperationGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn abort_timed_out(
    join: &mut JoinHandle<()>,
    diagnostics: &MemoryDiagnostics,
) -> Result<(), StorageError> {
    join.abort();
    let _ = join.await;
    diagnostics.0.running.store(false, Ordering::Release);
    Err(timed_out())
}

const fn cancelled() -> StorageError {
    StorageError::new(StorageErrorKind::Cancelled, "memory_operation_cancelled")
}

const fn timed_out() -> StorageError {
    StorageError::new(StorageErrorKind::Timeout, "memory_operation_timed_out")
}
