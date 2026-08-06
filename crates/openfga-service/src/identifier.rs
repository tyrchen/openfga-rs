//! Supervised actor-owned monotonic ULID allocation for public resource IDs.

use std::{
    error::Error as StdError,
    fmt,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use openfga_domain::{AuthorizationModelId, StoreId};
use openfga_storage::OperationContext;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};
use ulid::Ulid;

const RANDOMNESS_MASK: u128 = (1_u128 << 80) - 1;
const MAXIMUM_TIMESTAMP_MILLIS: u64 = (1_u64 << 48) - 1;
const MAXIMUM_SHUTDOWN_TIMEOUT: Duration = Duration::from_mins(1);

/// Stable identifier-allocation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierSourceErrorKind {
    /// The caller cancelled allocation.
    Cancelled,
    /// The request deadline elapsed.
    Timeout,
    /// The allocation actor is unavailable.
    Unavailable,
    /// The system clock or ULID space cannot represent another identifier.
    Exhausted,
    /// The operating-system entropy source failed.
    Entropy,
    /// Actor lifecycle or reply state failed internally.
    Internal,
}

/// Redacted identifier-allocation failure.
#[derive(Error)]
#[error("identifier allocation failed: {code}")]
pub struct IdentifierSourceError {
    kind: IdentifierSourceErrorKind,
    code: &'static str,
    #[source]
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl IdentifierSourceError {
    const fn new(kind: IdentifierSourceErrorKind, code: &'static str) -> Self {
        Self {
            kind,
            code,
            source: None,
        }
    }

    fn with_source(
        kind: IdentifierSourceErrorKind,
        code: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            code,
            source: Some(Box::new(source)),
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> IdentifierSourceErrorKind {
        self.kind
    }

    /// Returns the low-cardinality, non-sensitive diagnostic code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Debug for IdentifierSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdentifierSourceError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

/// Monotonic resource-ID source used by store and model publication services.
///
/// `async-trait` is deliberate because runtime assembly selects an implementation
/// behind `Arc<dyn IdentifierSource>`.
#[async_trait]
pub trait IdentifierSource: Send + Sync {
    /// Allocates the next monotonic store identifier.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, entropy, or exhaustion failures.
    async fn next_store_id(
        &self,
        context: &OperationContext,
    ) -> Result<StoreId, IdentifierSourceError>;

    /// Allocates the next monotonic authorization-model identifier.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, entropy, or exhaustion failures.
    async fn next_model_id(
        &self,
        context: &OperationContext,
    ) -> Result<AuthorizationModelId, IdentifierSourceError>;
}

/// Validated actor lifecycle limits for the system identifier source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SystemIdentifierSourceConfig {
    channel_capacity: NonZeroUsize,
    shutdown_timeout: Duration,
}

impl SystemIdentifierSourceConfig {
    /// Creates bounded actor lifecycle configuration.
    ///
    /// # Errors
    ///
    /// Returns exhaustion when shutdown timeout is zero or exceeds one minute.
    pub fn new(
        channel_capacity: NonZeroUsize,
        shutdown_timeout: Duration,
    ) -> Result<Self, IdentifierSourceError> {
        if shutdown_timeout.is_zero() || shutdown_timeout > MAXIMUM_SHUTDOWN_TIMEOUT {
            return Err(IdentifierSourceError::new(
                IdentifierSourceErrorKind::Exhausted,
                "identifier_shutdown_timeout_invalid",
            ));
        }
        Ok(Self {
            channel_capacity,
            shutdown_timeout,
        })
    }
}

impl Default for SystemIdentifierSourceConfig {
    fn default() -> Self {
        Self {
            channel_capacity: NonZeroUsize::new(256).unwrap_or(NonZeroUsize::MIN),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/// Supervised actor owning one process-local monotonic ULID sequence.
pub struct SystemIdentifierSource {
    config: SystemIdentifierSourceConfig,
    sender: mpsc::Sender<ActorMessage>,
    join: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl SystemIdentifierSource {
    /// Starts a monotonic allocator seeded from the operating-system CSPRNG.
    ///
    /// # Errors
    ///
    /// Returns entropy failure or unavailable when no Tokio runtime is active.
    pub fn start(config: SystemIdentifierSourceConfig) -> Result<Self, IdentifierSourceError> {
        let running = Arc::new(AtomicBool::new(true));
        let (sender, join) = spawn_actor(config, Arc::clone(&running))?;
        Ok(Self {
            config,
            sender,
            join: Some(join),
            running,
        })
    }

    /// Returns whether the allocator actor is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Gracefully drains, stops, and joins the allocator actor.
    ///
    /// Repeated calls are harmless.
    ///
    /// # Errors
    ///
    /// Returns timeout, unavailable, or actor join failure.
    pub async fn stop(&mut self) -> Result<(), IdentifierSourceError> {
        let Some(mut join) = self.join.take() else {
            return Ok(());
        };
        let (reply, received) = oneshot::channel();
        match timeout(
            self.config.shutdown_timeout,
            self.sender.send(ActorMessage::Shutdown(reply)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let joined = join.await;
                self.running.store(false, Ordering::Release);
                return match joined {
                    Ok(()) => Err(IdentifierSourceError::with_source(
                        IdentifierSourceErrorKind::Unavailable,
                        "identifier_shutdown_channel_closed",
                        error,
                    )),
                    Err(join_error) => Err(IdentifierSourceError::with_source(
                        IdentifierSourceErrorKind::Internal,
                        "identifier_actor_join_failed",
                        join_error,
                    )),
                };
            }
            Err(_) => return abort_timed_out(&mut join, &self.running).await,
        }
        if timeout(self.config.shutdown_timeout, received)
            .await
            .is_err()
        {
            return abort_timed_out(&mut join, &self.running).await;
        }
        match timeout(self.config.shutdown_timeout, &mut join).await {
            Ok(Ok(())) => {
                self.running.store(false, Ordering::Release);
                Ok(())
            }
            Ok(Err(error)) => {
                self.running.store(false, Ordering::Release);
                Err(IdentifierSourceError::with_source(
                    IdentifierSourceErrorKind::Internal,
                    "identifier_actor_join_failed",
                    error,
                ))
            }
            Err(_) => abort_timed_out(&mut join, &self.running).await,
        }
    }

    /// Restarts a stopped or failed allocator with a fresh random sequence.
    ///
    /// # Errors
    ///
    /// Returns lifecycle, entropy, or runtime availability failure.
    pub async fn restart(&mut self) -> Result<(), IdentifierSourceError> {
        let _previous_status = self.stop().await;
        let (sender, join) = spawn_actor(self.config, Arc::clone(&self.running))?;
        self.sender = sender;
        self.join = Some(join);
        self.running.store(true, Ordering::Release);
        Ok(())
    }

    async fn next(&self, context: &OperationContext) -> Result<Ulid, IdentifierSourceError> {
        context.check().map_err(|error| map_context_error(&error))?;
        let (reply, received) = oneshot::channel();
        let deadline = Instant::from_std(context.deadline().instant());
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => return Err(cancelled()),
            () = sleep_until(deadline) => return Err(timed_out()),
            result = self.sender.send(ActorMessage::Next(reply)) => result.map_err(|error| {
                IdentifierSourceError::with_source(
                    IdentifierSourceErrorKind::Unavailable,
                    "identifier_actor_channel_closed",
                    error,
                )
            })?,
        }
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => Err(cancelled()),
            () = sleep_until(deadline) => Err(timed_out()),
            result = received => result.map_err(|error| {
                IdentifierSourceError::with_source(
                    IdentifierSourceErrorKind::Unavailable,
                    "identifier_actor_reply_closed",
                    error,
                )
            })?,
        }
    }
}

impl fmt::Debug for SystemIdentifierSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemIdentifierSource")
            .field("config", &self.config)
            .field("running", &self.is_running())
            .finish_non_exhaustive()
    }
}

impl Drop for SystemIdentifierSource {
    fn drop(&mut self) {
        if self.join.is_some() {
            let (reply, _received) = oneshot::channel();
            let _ = self.sender.try_send(ActorMessage::Shutdown(reply));
        }
    }
}

#[async_trait]
impl IdentifierSource for SystemIdentifierSource {
    async fn next_store_id(
        &self,
        context: &OperationContext,
    ) -> Result<StoreId, IdentifierSourceError> {
        self.next(context).await.map(StoreId::from_ulid)
    }

    async fn next_model_id(
        &self,
        context: &OperationContext,
    ) -> Result<AuthorizationModelId, IdentifierSourceError> {
        self.next(context)
            .await
            .map(AuthorizationModelId::from_ulid)
    }
}

#[derive(Debug)]
enum ActorMessage {
    Next(oneshot::Sender<Result<Ulid, IdentifierSourceError>>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug)]
struct MonotonicState {
    last_millis: u64,
    randomness: u128,
}

impl MonotonicState {
    const fn new(randomness: u128) -> Self {
        Self {
            last_millis: 0,
            randomness: randomness & RANDOMNESS_MASK,
        }
    }

    fn next(&mut self, now: SystemTime) -> Result<Ulid, IdentifierSourceError> {
        let millis = now
            .duration_since(UNIX_EPOCH)
            .map_err(|source| {
                IdentifierSourceError::with_source(
                    IdentifierSourceErrorKind::Exhausted,
                    "identifier_clock_before_epoch",
                    source,
                )
            })?
            .as_millis();
        let millis = u64::try_from(millis).map_err(|source| {
            IdentifierSourceError::with_source(
                IdentifierSourceErrorKind::Exhausted,
                "identifier_clock_out_of_range",
                source,
            )
        })?;
        if millis > MAXIMUM_TIMESTAMP_MILLIS {
            return Err(IdentifierSourceError::new(
                IdentifierSourceErrorKind::Exhausted,
                "identifier_clock_out_of_range",
            ));
        }
        if millis > self.last_millis {
            self.last_millis = millis;
            self.randomness = self.randomness.wrapping_add(1) & RANDOMNESS_MASK;
            return Ok(Ulid::from_parts(self.last_millis, self.randomness));
        } else if self.randomness == RANDOMNESS_MASK {
            self.last_millis = self.last_millis.checked_add(1).ok_or_else(|| {
                IdentifierSourceError::new(
                    IdentifierSourceErrorKind::Exhausted,
                    "identifier_space_exhausted",
                )
            })?;
            if self.last_millis > MAXIMUM_TIMESTAMP_MILLIS {
                return Err(IdentifierSourceError::new(
                    IdentifierSourceErrorKind::Exhausted,
                    "identifier_space_exhausted",
                ));
            }
            self.randomness = 0;
            return Ok(Ulid::from_parts(self.last_millis, self.randomness));
        }
        self.randomness = self.randomness.checked_add(1).ok_or_else(|| {
            IdentifierSourceError::new(
                IdentifierSourceErrorKind::Exhausted,
                "identifier_space_exhausted",
            )
        })?;
        Ok(Ulid::from_parts(self.last_millis, self.randomness))
    }
}

fn spawn_actor(
    config: SystemIdentifierSourceConfig,
    running: Arc<AtomicBool>,
) -> Result<(mpsc::Sender<ActorMessage>, JoinHandle<()>), IdentifierSourceError> {
    tokio::runtime::Handle::try_current().map_err(|source| {
        IdentifierSourceError::with_source(
            IdentifierSourceErrorKind::Unavailable,
            "identifier_runtime_unavailable",
            source,
        )
    })?;
    let mut seed = [0_u8; 10];
    getrandom::fill(&mut seed).map_err(|source| {
        IdentifierSourceError::with_source(
            IdentifierSourceErrorKind::Entropy,
            "identifier_entropy_failed",
            source,
        )
    })?;
    let randomness = seed
        .into_iter()
        .fold(0_u128, |value, byte| (value << 8) | u128::from(byte));
    let (sender, receiver) = mpsc::channel(config.channel_capacity.get());
    let join = tokio::spawn(run_actor(
        receiver,
        MonotonicState::new(randomness),
        running,
    ));
    Ok((sender, join))
}

async fn run_actor(
    mut receiver: mpsc::Receiver<ActorMessage>,
    mut state: MonotonicState,
    running: Arc<AtomicBool>,
) {
    running.store(true, Ordering::Release);
    while let Some(message) = receiver.recv().await {
        match message {
            ActorMessage::Next(reply) => {
                let _ = reply.send(state.next(SystemTime::now()));
            }
            ActorMessage::Shutdown(reply) => {
                let _ = reply.send(());
                break;
            }
        }
    }
    running.store(false, Ordering::Release);
}

async fn abort_timed_out(
    join: &mut JoinHandle<()>,
    running: &AtomicBool,
) -> Result<(), IdentifierSourceError> {
    join.abort();
    let joined = join.await;
    running.store(false, Ordering::Release);
    match joined {
        Err(error) if error.is_cancelled() => Err(IdentifierSourceError::new(
            IdentifierSourceErrorKind::Timeout,
            "identifier_actor_shutdown_timeout",
        )),
        Err(error) => Err(IdentifierSourceError::with_source(
            IdentifierSourceErrorKind::Internal,
            "identifier_actor_join_failed",
            error,
        )),
        Ok(()) => Err(IdentifierSourceError::new(
            IdentifierSourceErrorKind::Timeout,
            "identifier_actor_shutdown_timeout",
        )),
    }
}

fn map_context_error(error: &openfga_storage::StorageError) -> IdentifierSourceError {
    match error.kind() {
        openfga_storage::StorageErrorKind::Cancelled => cancelled(),
        openfga_storage::StorageErrorKind::Timeout => timed_out(),
        openfga_storage::StorageErrorKind::NotFound
        | openfga_storage::StorageErrorKind::AlreadyExists
        | openfga_storage::StorageErrorKind::Conflict
        | openfga_storage::StorageErrorKind::InvalidContinuation
        | openfga_storage::StorageErrorKind::Unavailable
        | openfga_storage::StorageErrorKind::Integrity
        | openfga_storage::StorageErrorKind::ResourceExhausted
        | openfga_storage::StorageErrorKind::Internal => IdentifierSourceError::new(
            IdentifierSourceErrorKind::Internal,
            "identifier_context_failed",
        ),
    }
}

const fn cancelled() -> IdentifierSourceError {
    IdentifierSourceError::new(
        IdentifierSourceErrorKind::Cancelled,
        "identifier_allocation_cancelled",
    )
}

const fn timed_out() -> IdentifierSourceError {
    IdentifierSourceError::new(
        IdentifierSourceErrorKind::Timeout,
        "identifier_allocation_timeout",
    )
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant, UNIX_EPOCH};

    use openfga_domain::{ConsistencyPreference, Deadline, RequestTimeout};
    use openfga_storage::{OperationContext, StorageCancellationToken};

    use super::{
        IdentifierSource, IdentifierSourceErrorKind, MonotonicState, RANDOMNESS_MASK,
        SystemIdentifierSource, SystemIdentifierSourceConfig,
    };

    #[test]
    fn test_should_remain_monotonic_when_clock_repeats_or_moves_backwards()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = MonotonicState::new(7);
        let first = state.next(UNIX_EPOCH + Duration::from_millis(10))?;
        let second = state.next(UNIX_EPOCH + Duration::from_millis(10))?;
        let third = state.next(UNIX_EPOCH + Duration::from_millis(9))?;
        assert!(first < second && second < third);
        Ok(())
    }

    #[test]
    fn test_should_advance_timestamp_when_randomness_is_exhausted()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = MonotonicState::new(RANDOMNESS_MASK);
        state.last_millis = 10;
        let next = state.next(UNIX_EPOCH + Duration::from_millis(10))?;
        assert_eq!(next.timestamp_ms(), 11);
        assert_eq!(next.random(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_should_allocate_cancel_stop_and_restart_actor()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut source = SystemIdentifierSource::start(SystemIdentifierSourceConfig::default())?;
        let context = operation_context(StorageCancellationToken::new())?;
        let store = source.next_store_id(&context).await?;
        let model = source.next_model_id(&context).await?;
        assert!(store.as_ulid() < model.as_ulid());

        let cancellation = StorageCancellationToken::new();
        cancellation.cancel();
        let cancelled = source
            .next_store_id(&operation_context(cancellation)?)
            .await
            .err()
            .ok_or("cancelled allocation unexpectedly succeeded")?;
        assert_eq!(cancelled.kind(), IdentifierSourceErrorKind::Cancelled);

        source.stop().await?;
        assert!(!source.is_running());
        source.restart().await?;
        assert!(source.is_running());
        source.stop().await?;
        Ok(())
    }

    fn operation_context(
        cancellation: StorageCancellationToken,
    ) -> Result<OperationContext, Box<dyn std::error::Error>> {
        Ok(OperationContext::new(
            ConsistencyPreference::HigherConsistency,
            Deadline::from_timeout(Instant::now(), RequestTimeout::new(Duration::from_secs(5))?)?,
            cancellation,
        ))
    }
}
