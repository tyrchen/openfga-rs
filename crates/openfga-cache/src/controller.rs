//! Supervised bounded changelog invalidation actor.

use std::{
    collections::BTreeMap,
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use openfga_domain::{ChangeId, ConsistencyPreference, Deadline, RequestTimeout, StoreId};
use openfga_storage::{
    ChangeFilter, ChangeReader, OperationContext, PageOptions, StorageCancellationToken,
    StorageError, TupleChange,
};
use opentelemetry::metrics::{ObservableCounter, ObservableGauge};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{MissedTickBehavior, interval, timeout},
};

use crate::InvalidationWatermark;

const MAXIMUM_INTERVAL: Duration = Duration::from_mins(5);
const MAXIMUM_PAGES_PER_CYCLE: u32 = 4;

/// Validated finite policy for the changelog invalidation actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct InvalidationControllerConfig {
    channel_capacity: NonZeroUsize,
    page_size: NonZeroU32,
    poll_interval: Duration,
    read_timeout: Duration,
    maximum_lag: Duration,
}

impl InvalidationControllerConfig {
    /// Creates a bounded polling and supervision policy.
    ///
    /// # Errors
    ///
    /// Returns an error for zero/oversized durations, a page over 1,000
    /// changes, or a maximum lag too small to bound one polling cycle.
    pub fn new(
        channel_capacity: NonZeroUsize,
        page_size: NonZeroU32,
        poll_interval: Duration,
        read_timeout: Duration,
        maximum_lag: Duration,
    ) -> Result<Self, InvalidationControllerConfigError> {
        if page_size.get() > 1_000 {
            return Err(InvalidationControllerConfigError::PageSize);
        }
        if poll_interval.is_zero() || poll_interval > MAXIMUM_INTERVAL {
            return Err(InvalidationControllerConfigError::PollInterval);
        }
        if read_timeout.is_zero() || read_timeout > MAXIMUM_INTERVAL {
            return Err(InvalidationControllerConfigError::ReadTimeout);
        }
        let maximum_cycle = read_timeout.saturating_mul(MAXIMUM_PAGES_PER_CYCLE);
        if maximum_lag < maximum_cycle
            || maximum_lag < poll_interval
            || maximum_lag > MAXIMUM_INTERVAL
        {
            return Err(InvalidationControllerConfigError::MaximumLag);
        }
        Ok(Self {
            channel_capacity,
            page_size,
            poll_interval,
            read_timeout,
            maximum_lag,
        })
    }
}

/// Invalid changelog controller configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum InvalidationControllerConfigError {
    /// A changelog page contains more than 1,000 rows.
    #[error("cache controller page size must be between 1 and 1000")]
    PageSize,
    /// The poll interval is zero or longer than five minutes.
    #[error("cache controller poll interval must be between one nanosecond and five minutes")]
    PollInterval,
    /// The read timeout is zero or longer than five minutes.
    #[error("cache controller read timeout must be between one nanosecond and five minutes")]
    ReadTimeout,
    /// Maximum lag cannot bound a polling cycle or exceeds five minutes.
    #[error("cache controller maximum lag must bound four reads and not exceed five minutes")]
    MaximumLag,
}

#[derive(Debug)]
struct DiagnosticsState {
    running: AtomicBool,
    ready: AtomicBool,
    tracked_stores: AtomicUsize,
    successful_polls: AtomicU64,
    failed_polls: AtomicU64,
    flushes: AtomicU64,
    overflows: AtomicU64,
    restarts: AtomicU64,
    current_poll_freshness_age_millis: AtomicU64,
}

struct ControllerMetrics {
    _running: ObservableGauge<u64>,
    _ready: ObservableGauge<u64>,
    _tracked_stores: ObservableGauge<u64>,
    _poll_freshness_age: ObservableGauge<u64>,
    _successful_polls: ObservableCounter<u64>,
    _failed_polls: ObservableCounter<u64>,
    _flushes: ObservableCounter<u64>,
    _overflows: ObservableCounter<u64>,
    _restarts: ObservableCounter<u64>,
}

impl ControllerMetrics {
    fn new(diagnostics: &InvalidationControllerDiagnostics) -> Self {
        let meter = opentelemetry::global::meter("openfga-cache-controller");
        Self {
            _running: boolean_gauge(
                &meter,
                "openfga.cache.controller.running",
                Arc::clone(&diagnostics.0),
                |state| state.running.load(Ordering::Acquire),
            ),
            _ready: boolean_gauge(
                &meter,
                "openfga.cache.controller.ready",
                Arc::clone(&diagnostics.0),
                |state| state.ready.load(Ordering::Acquire),
            ),
            _tracked_stores: u64_gauge(
                &meter,
                "openfga.cache.controller.tracked_stores",
                Arc::clone(&diagnostics.0),
                |state| {
                    u64::try_from(state.tracked_stores.load(Ordering::Relaxed)).unwrap_or(u64::MAX)
                },
            ),
            _poll_freshness_age: meter
                .u64_observable_gauge("openfga.cache.controller.poll_freshness_age")
                .with_description("Maximum elapsed time since a tracked store's successful poll")
                .with_unit("ms")
                .with_callback({
                    let state = Arc::clone(&diagnostics.0);
                    move |observer| {
                        observer.observe(
                            state
                                .current_poll_freshness_age_millis
                                .load(Ordering::Relaxed),
                            &[],
                        );
                    }
                })
                .build(),
            _successful_polls: u64_counter(
                &meter,
                "openfga.cache.controller.polls.successful",
                Arc::clone(&diagnostics.0),
                |state| state.successful_polls.load(Ordering::Relaxed),
            ),
            _failed_polls: u64_counter(
                &meter,
                "openfga.cache.controller.polls.failed",
                Arc::clone(&diagnostics.0),
                |state| state.failed_polls.load(Ordering::Relaxed),
            ),
            _flushes: u64_counter(
                &meter,
                "openfga.cache.controller.flushes",
                Arc::clone(&diagnostics.0),
                |state| state.flushes.load(Ordering::Relaxed),
            ),
            _overflows: u64_counter(
                &meter,
                "openfga.cache.controller.overflows",
                Arc::clone(&diagnostics.0),
                |state| state.overflows.load(Ordering::Relaxed),
            ),
            _restarts: u64_counter(
                &meter,
                "openfga.cache.controller.restarts",
                Arc::clone(&diagnostics.0),
                |state| state.restarts.load(Ordering::Relaxed),
            ),
        }
    }
}

impl fmt::Debug for ControllerMetrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControllerMetrics")
    }
}

fn boolean_gauge(
    meter: &opentelemetry::metrics::Meter,
    name: &'static str,
    state: Arc<DiagnosticsState>,
    value: fn(&DiagnosticsState) -> bool,
) -> ObservableGauge<u64> {
    meter
        .u64_observable_gauge(name)
        .with_callback(move |observer| observer.observe(u64::from(value(&state)), &[]))
        .build()
}

fn u64_gauge(
    meter: &opentelemetry::metrics::Meter,
    name: &'static str,
    state: Arc<DiagnosticsState>,
    value: fn(&DiagnosticsState) -> u64,
) -> ObservableGauge<u64> {
    meter
        .u64_observable_gauge(name)
        .with_callback(move |observer| observer.observe(value(&state), &[]))
        .build()
}

fn u64_counter(
    meter: &opentelemetry::metrics::Meter,
    name: &'static str,
    state: Arc<DiagnosticsState>,
    value: fn(&DiagnosticsState) -> u64,
) -> ObservableCounter<u64> {
    meter
        .u64_observable_counter(name)
        .with_callback(move |observer| observer.observe(value(&state), &[]))
        .build()
}

struct RunningGuard(InvalidationControllerDiagnostics);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.0.ready.store(false, Ordering::Release);
        self.0.0.running.store(false, Ordering::Release);
    }
}

/// Cloneable low-cardinality invalidation-controller diagnostics.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InvalidationControllerDiagnostics(Arc<DiagnosticsState>);

impl InvalidationControllerDiagnostics {
    /// Returns whether the actor task is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.0.running.load(Ordering::Acquire)
    }

    /// Returns whether every tracked store has completed a successful poll since restart.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.0.ready.load(Ordering::Acquire)
    }

    /// Returns the number of stores currently tracked by the actor.
    #[must_use]
    pub fn tracked_stores(&self) -> usize {
        self.0.tracked_stores.load(Ordering::Relaxed)
    }

    /// Returns completed successful changelog polls.
    #[must_use]
    pub fn successful_polls(&self) -> u64 {
        self.0.successful_polls.load(Ordering::Relaxed)
    }

    /// Returns failed, malformed, timed-out, or lagged changelog polls.
    #[must_use]
    pub fn failed_polls(&self) -> u64 {
        self.0.failed_polls.load(Ordering::Relaxed)
    }

    /// Returns conservative mutable-cache flushes.
    #[must_use]
    pub fn flushes(&self) -> u64 {
        self.0.flushes.load(Ordering::Relaxed)
    }

    /// Returns bounded registration-channel overflows.
    #[must_use]
    pub fn overflows(&self) -> u64 {
        self.0.overflows.load(Ordering::Relaxed)
    }

    /// Returns explicit actor state restarts.
    #[must_use]
    pub fn restarts(&self) -> u64 {
        self.0.restarts.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
enum Command {
    Track(StoreId),
    Restart(oneshot::Sender<()>),
    Stop(oneshot::Sender<()>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistrationState {
    Pending,
    ActiveUntil(Instant),
}

/// Nonblocking store-registration and conservative overflow handle.
#[derive(Clone)]
#[non_exhaustive]
pub struct InvalidationControllerHandle {
    sender: mpsc::Sender<Command>,
    registrations: moka::sync::Cache<StoreId, RegistrationState>,
    invalidation: InvalidationWatermark,
    diagnostics: InvalidationControllerDiagnostics,
}

impl InvalidationControllerHandle {
    /// Registers a store for ordered changelog polling.
    ///
    /// A full or closed bounded channel immediately flushes mutable caches. A
    /// later request retries registration, so overflow cannot silently disable
    /// invalidation for an active store.
    pub fn track(&self, store_id: StoreId) {
        if self.registrations.get(&store_id).is_some() {
            return;
        }
        self.diagnostics.0.ready.store(false, Ordering::Release);
        self.registrations
            .insert(store_id, RegistrationState::Pending);
        match self.sender.try_send(Command::Track(store_id)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.registrations.invalidate(&store_id);
                self.diagnostics.0.overflows.fetch_add(1, Ordering::Relaxed);
                flush(&self.invalidation, &self.diagnostics);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.registrations.invalidate(&store_id);
                flush(&self.invalidation, &self.diagnostics);
            }
        }
    }

    pub(crate) fn permits_caching(&self, store_id: StoreId) -> bool {
        if !self.diagnostics.is_running() {
            return false;
        }
        match self.registrations.get(&store_id) {
            Some(RegistrationState::ActiveUntil(deadline)) if Instant::now() <= deadline => {
                return self.diagnostics.is_ready();
            }
            Some(RegistrationState::ActiveUntil(_)) => {}
            Some(RegistrationState::Pending) | None => return false,
        }
        if self.diagnostics.0.ready.swap(false, Ordering::AcqRel) {
            self.diagnostics
                .0
                .failed_polls
                .fetch_add(1, Ordering::Relaxed);
            flush(&self.invalidation, &self.diagnostics);
        }
        false
    }
}

impl fmt::Debug for InvalidationControllerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvalidationControllerHandle")
            .field("channel_capacity", &self.sender.max_capacity())
            .field("registered_stores", &self.registrations.entry_count())
            .field("diagnostics", &self.diagnostics)
            .finish_non_exhaustive()
    }
}

/// Owned lifecycle supervisor for one invalidation actor task.
#[non_exhaustive]
pub struct InvalidationController {
    handle: InvalidationControllerHandle,
    join: Option<JoinHandle<()>>,
    maximum_lag: Duration,
    metrics: ControllerMetrics,
}

impl InvalidationController {
    /// Starts the bounded invalidation actor and conservatively flushes restart gaps.
    ///
    /// # Errors
    ///
    /// Returns an error when called outside a Tokio runtime.
    pub fn start(
        reader: Arc<dyn ChangeReader>,
        invalidation: InvalidationWatermark,
        config: InvalidationControllerConfig,
    ) -> Result<Self, InvalidationControllerError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| InvalidationControllerError::RuntimeUnavailable)?;
        let (sender, receiver) = mpsc::channel(config.channel_capacity.get());
        let diagnostics = InvalidationControllerDiagnostics(Arc::new(DiagnosticsState {
            running: AtomicBool::new(true),
            ready: AtomicBool::new(true),
            tracked_stores: AtomicUsize::new(0),
            successful_polls: AtomicU64::new(0),
            failed_polls: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            overflows: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
            current_poll_freshness_age_millis: AtomicU64::new(0),
        }));
        let metrics = ControllerMetrics::new(&diagnostics);
        flush(&invalidation, &diagnostics);
        let registrations = moka::sync::Cache::builder()
            .max_capacity(config.channel_capacity.get() as u64)
            .build();
        let actor = Actor {
            reader,
            invalidation: invalidation.clone(),
            config,
            diagnostics: diagnostics.clone(),
            registrations: registrations.clone(),
            stores: BTreeMap::new(),
        };
        let task_diagnostics = diagnostics.clone();
        let join = runtime.spawn(async move {
            let _running_guard = RunningGuard(task_diagnostics);
            actor.run(receiver).await;
        });
        Ok(Self {
            handle: InvalidationControllerHandle {
                sender,
                registrations,
                invalidation,
                diagnostics,
            },
            join: Some(join),
            maximum_lag: config.maximum_lag,
            metrics,
        })
    }

    /// Returns the cloneable registration handle used by mutable caches.
    #[must_use]
    pub fn handle(&self) -> InvalidationControllerHandle {
        self.handle.clone()
    }

    /// Returns cloneable non-sensitive lifecycle and invalidation counters.
    #[must_use]
    pub fn diagnostics(&self) -> InvalidationControllerDiagnostics {
        self.handle.diagnostics.clone()
    }

    /// Clears every cursor and forces a conservative restart flush.
    ///
    /// # Errors
    ///
    /// Returns an unavailable or timeout error if the actor cannot acknowledge.
    pub async fn restart(&self) -> Result<(), InvalidationControllerError> {
        let (sent, received) = oneshot::channel();
        timeout(
            self.maximum_lag,
            self.handle.sender.send(Command::Restart(sent)),
        )
        .await
        .map_err(|_| InvalidationControllerError::CommandTimeout)?
        .map_err(|_| InvalidationControllerError::ChannelClosed)?;
        timeout(self.maximum_lag, received)
            .await
            .map_err(|_| InvalidationControllerError::CommandTimeout)?
            .map_err(|_| InvalidationControllerError::ChannelClosed)
    }

    /// Stops, drains, and joins the actor task. Repeated calls are harmless.
    ///
    /// # Errors
    ///
    /// Returns timeout, channel, or task-join failures after aborting and joining
    /// a task that exceeds the bounded shutdown window.
    pub async fn stop(&mut self) -> Result<(), InvalidationControllerError> {
        let Some(mut join) = self.join.take() else {
            return Ok(());
        };
        let (sent, received) = oneshot::channel();
        match timeout(
            self.maximum_lag,
            self.handle.sender.send(Command::Stop(sent)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return abort_join(&mut join, InvalidationControllerError::ChannelClosed).await;
            }
            Err(_) => {
                return abort_join(&mut join, InvalidationControllerError::CommandTimeout).await;
            }
        }
        match timeout(self.maximum_lag, received).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return abort_join(&mut join, InvalidationControllerError::ChannelClosed).await;
            }
            Err(_) => {
                return abort_join(&mut join, InvalidationControllerError::CommandTimeout).await;
            }
        }
        match timeout(self.maximum_lag, &mut join).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(InvalidationControllerError::TaskFailed(error)),
            Err(_) => abort_join(&mut join, InvalidationControllerError::CommandTimeout).await,
        }
    }
}

impl fmt::Debug for InvalidationController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvalidationController")
            .field("handle", &self.handle)
            .field("running", &self.join.is_some())
            .field("maximum_lag", &self.maximum_lag)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

impl Drop for InvalidationController {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
        self.handle
            .diagnostics
            .0
            .running
            .store(false, Ordering::Release);
    }
}

/// Invalidation actor lifecycle failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InvalidationControllerError {
    /// No Tokio runtime was active during start.
    #[error("cache controller requires an active Tokio runtime")]
    RuntimeUnavailable,
    /// The actor command channel closed unexpectedly.
    #[error("cache controller command channel closed")]
    ChannelClosed,
    /// A lifecycle command exceeded its finite deadline.
    #[error("cache controller lifecycle command timed out")]
    CommandTimeout,
    /// The actor task panicked or was cancelled unexpectedly.
    #[error("cache controller task failed")]
    TaskFailed(#[source] tokio::task::JoinError),
}

#[derive(Debug)]
struct StoreState {
    last_seen: Option<ChangeId>,
    last_success: Instant,
    next_poll: Instant,
    backoff: Duration,
    initialized: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PollOutcome {
    changed: bool,
    caught_up: bool,
}

#[derive(Debug, Error)]
enum PollError {
    #[error("controller policy invariant failed")]
    Policy,
    #[error("changelog read timed out")]
    Timeout,
    #[error("changelog storage read failed")]
    Storage(#[source] StorageError),
    #[error("changelog page violated ordering or cursor invariants")]
    Malformed,
}

impl StoreState {
    fn new(config: InvalidationControllerConfig) -> Self {
        let now = Instant::now();
        Self {
            last_seen: None,
            last_success: now,
            next_poll: now,
            backoff: config.poll_interval,
            initialized: false,
        }
    }
}

struct Actor {
    reader: Arc<dyn ChangeReader>,
    invalidation: InvalidationWatermark,
    config: InvalidationControllerConfig,
    diagnostics: InvalidationControllerDiagnostics,
    registrations: moka::sync::Cache<StoreId, RegistrationState>,
    stores: BTreeMap<StoreId, StoreState>,
}

impl fmt::Debug for Actor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Actor")
            .field("reader", &"dyn ChangeReader")
            .field("invalidation", &self.invalidation)
            .field("config", &self.config)
            .field("diagnostics", &self.diagnostics)
            .field("registrations", &self.registrations.entry_count())
            .field("stores", &self.stores)
            .finish()
    }
}

impl Actor {
    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        let mut ticker = interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                command = commands.recv() => match command {
                    Some(Command::Track(store_id)) => self.track(store_id),
                    Some(Command::Restart(completed)) => {
                        self.restart_state();
                        let _ignored = completed.send(());
                    }
                    Some(Command::Stop(completed)) => {
                        let _ignored = completed.send(());
                        break;
                    }
                    None => break,
                },
                _ = ticker.tick() => self.poll_due().await,
            }
        }
        self.diagnostics.0.ready.store(false, Ordering::Release);
        self.diagnostics.0.running.store(false, Ordering::Release);
    }

    fn track(&mut self, store_id: StoreId) {
        if let Some(state) = self.stores.get(&store_id) {
            let registration = if state.initialized
                && Instant::now().duration_since(state.last_success) <= self.config.maximum_lag
            {
                RegistrationState::ActiveUntil(state.last_success + self.config.maximum_lag)
            } else {
                RegistrationState::Pending
            };
            self.registrations.insert(store_id, registration);
            self.update_readiness();
            return;
        }
        if self.stores.len() >= self.config.channel_capacity.get() {
            self.registrations.invalidate(&store_id);
            self.diagnostics.0.overflows.fetch_add(1, Ordering::Relaxed);
            flush(&self.invalidation, &self.diagnostics);
            self.update_readiness();
            return;
        }
        self.stores.insert(store_id, StoreState::new(self.config));
        self.update_readiness();
    }

    fn restart_state(&mut self) {
        for state in self.stores.values_mut() {
            *state = StoreState::new(self.config);
        }
        for store_id in self.stores.keys() {
            self.registrations
                .insert(*store_id, RegistrationState::Pending);
        }
        self.diagnostics.0.restarts.fetch_add(1, Ordering::Relaxed);
        flush(&self.invalidation, &self.diagnostics);
        self.update_readiness();
    }

    async fn poll_due(&mut self) {
        let now = Instant::now();
        let stores = self
            .stores
            .iter()
            .filter_map(|(store_id, state)| (state.next_poll <= now).then_some(*store_id))
            .collect::<Vec<_>>();
        for store_id in stores {
            let Some(mut state) = self.stores.remove(&store_id) else {
                continue;
            };
            if Instant::now().duration_since(state.last_success) > self.config.maximum_lag {
                self.diagnostics.0.ready.store(false, Ordering::Release);
                self.registrations
                    .insert(store_id, RegistrationState::Pending);
                self.diagnostics
                    .0
                    .failed_polls
                    .fetch_add(1, Ordering::Relaxed);
                flush(&self.invalidation, &self.diagnostics);
            }
            match poll_store(Arc::clone(&self.reader), store_id, &mut state, self.config).await {
                Ok(outcome) if outcome.caught_up => {
                    if outcome.changed {
                        flush(&self.invalidation, &self.diagnostics);
                    }
                    state.last_success = Instant::now();
                    state.next_poll = state.last_success + self.config.poll_interval;
                    state.backoff = self.config.poll_interval;
                    state.initialized = true;
                    self.registrations.insert(
                        store_id,
                        RegistrationState::ActiveUntil(
                            state.last_success + self.config.maximum_lag,
                        ),
                    );
                    self.diagnostics
                        .0
                        .successful_polls
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(outcome) => {
                    self.diagnostics.0.ready.store(false, Ordering::Release);
                    if outcome.changed {
                        flush(&self.invalidation, &self.diagnostics);
                    }
                    state.initialized = false;
                    state.next_poll = Instant::now();
                    state.backoff = self.config.poll_interval;
                    self.registrations
                        .insert(store_id, RegistrationState::Pending);
                    self.diagnostics
                        .0
                        .successful_polls
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    tracing::warn!(
                        store.id = %store_id,
                        error = %error,
                        "cache invalidation poll failed; mutable caches disabled"
                    );
                    self.diagnostics.0.ready.store(false, Ordering::Release);
                    flush(&self.invalidation, &self.diagnostics);
                    state.last_seen = None;
                    state.initialized = false;
                    self.registrations
                        .insert(store_id, RegistrationState::Pending);
                    state.next_poll = Instant::now() + jittered_backoff(store_id, state.backoff);
                    state.backoff = state.backoff.saturating_mul(2).min(self.config.maximum_lag);
                    self.diagnostics
                        .0
                        .failed_polls
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            self.stores.insert(store_id, state);
        }
        self.update_readiness();
    }

    fn update_readiness(&self) {
        self.diagnostics
            .0
            .tracked_stores
            .store(self.stores.len(), Ordering::Relaxed);
        let now = Instant::now();
        let current_poll_freshness_age_millis = self
            .stores
            .values()
            .map(|state| now.duration_since(state.last_success).as_millis())
            .max()
            .map_or(0, |millis| u64::try_from(millis).unwrap_or(u64::MAX));
        self.diagnostics
            .0
            .current_poll_freshness_age_millis
            .store(current_poll_freshness_age_millis, Ordering::Relaxed);
        let ready = self.stores.values().all(|state| {
            state.initialized && now.duration_since(state.last_success) <= self.config.maximum_lag
        });
        self.diagnostics.0.ready.store(ready, Ordering::Release);
    }
}

#[tracing::instrument(name = "cache_invalidation_poll", skip_all, fields(store.id = %store_id))]
async fn poll_store(
    reader: Arc<dyn ChangeReader>,
    store_id: StoreId,
    state: &mut StoreState,
    config: InvalidationControllerConfig,
) -> Result<PollOutcome, PollError> {
    let mut changed = false;
    for _ in 0..MAXIMUM_PAGES_PER_CYCLE {
        let page_limit = openfga_domain::Limit::<100_000>::new(config.page_size.get())
            .map_err(|_| PollError::Policy)?;
        let options = match state.last_seen {
            Some(last_seen) => PageOptions::after_change_id(config.page_size, last_seen),
            None => {
                PageOptions::from_read_options(openfga_storage::ReadOptions::from_limit(page_limit))
            }
        };
        let request_timeout =
            RequestTimeout::new(config.read_timeout).map_err(|_| PollError::Policy)?;
        let deadline = Deadline::from_timeout(Instant::now(), request_timeout)
            .map_err(|_| PollError::Policy)?;
        let context = OperationContext::new(
            ConsistencyPreference::HigherConsistency,
            deadline,
            StorageCancellationToken::new(),
        );
        let page = timeout(
            config.read_timeout,
            reader.read_changes(&context, store_id, &ChangeFilter::default(), &options),
        )
        .await
        .map_err(|_| PollError::Timeout)?
        .map_err(PollError::Storage)?;
        validate_changes(page.items(), store_id, state.last_seen)?;
        let Some(last) = page.items().last() else {
            if page.continuation().is_some() {
                return Err(PollError::Malformed);
            }
            return Ok(PollOutcome {
                changed,
                caught_up: true,
            });
        };
        state.last_seen = Some(last.id());
        changed = true;
        if page.continuation().is_none() {
            return Ok(PollOutcome {
                changed,
                caught_up: true,
            });
        }
    }
    Ok(PollOutcome {
        changed,
        caught_up: false,
    })
}

fn jittered_backoff(store_id: StoreId, base: Duration) -> Duration {
    let mut hasher = DefaultHasher::new();
    store_id.hash(&mut hasher);
    base.hash(&mut hasher);
    let percent = 80_u32.saturating_add(u32::try_from(hasher.finish() % 41).unwrap_or_default());
    base.saturating_mul(percent) / 100
}

fn validate_changes(
    changes: &[TupleChange],
    store_id: StoreId,
    last_seen: Option<ChangeId>,
) -> Result<(), PollError> {
    let mut previous = last_seen;
    for change in changes {
        if change.store_id() != store_id || previous.is_some_and(|id| change.id() <= id) {
            return Err(PollError::Malformed);
        }
        previous = Some(change.id());
    }
    Ok(())
}

fn flush(invalidation: &InvalidationWatermark, diagnostics: &InvalidationControllerDiagnostics) {
    let _generation = invalidation.advance();
    diagnostics.0.flushes.fetch_add(1, Ordering::Relaxed);
}

async fn abort_join(
    join: &mut JoinHandle<()>,
    error: InvalidationControllerError,
) -> Result<(), InvalidationControllerError> {
    join.abort();
    let _joined = join.await;
    Err(error)
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        num::{NonZeroU32, NonZeroUsize},
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    };

    use async_trait::async_trait;
    use openfga_domain::{
        ChangeId, ConsistencyPreference, Deadline, RelationshipTuple, RequestTimeout, StoreId,
        TupleKey,
    };
    use openfga_storage::{
        ChangeFilter, ChangeOperation, ChangeReader, OperationContext, Page, PageOptions,
        StorageCancellationToken, StorageError, TupleChange, TupleWriteOptions, TupleWriter,
    };
    use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};

    use super::{InvalidationController, InvalidationControllerConfig};
    use crate::InvalidationWatermark;

    #[tokio::test]
    async fn test_should_poll_restart_and_stop_controller_actor()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let store_id = store_id()?;
        write_tuple(&storage, "document:one#viewer@user:anne").await?;
        let watermark = InvalidationWatermark::new();
        let mut controller = InvalidationController::start(
            storage.clone(),
            watermark.clone(),
            controller_config()?,
        )?;
        let started_at = watermark.current();
        controller.handle().track(store_id);
        wait_until(Duration::from_secs(1), || {
            controller.diagnostics().successful_polls() >= 1
        })
        .await?;
        assert!(watermark.current() > started_at);
        assert!(controller.diagnostics().is_ready());

        let before_write = watermark.current();
        write_tuple(&storage, "document:two#viewer@user:anne").await?;
        wait_until(Duration::from_secs(1), || {
            watermark.current() > before_write
        })
        .await?;

        let before_restart = watermark.current();
        controller.restart().await?;
        assert!(watermark.current() > before_restart);
        assert_eq!(controller.diagnostics().restarts(), 1);
        assert_eq!(controller.diagnostics().tracked_stores(), 1);
        controller.stop().await?;
        assert!(!controller.diagnostics().is_running());

        drop(controller);
        stop_storage(storage).await
    }

    #[tokio::test]
    async fn test_should_flush_on_registration_overflow_and_duplicated_changelog()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let watermark = InvalidationWatermark::new();
        let config = InvalidationControllerConfig::new(
            NonZeroUsize::MIN,
            NonZeroU32::MIN,
            Duration::from_millis(1),
            Duration::from_millis(10),
            Duration::from_millis(40),
        )?;
        let mut controller = InvalidationController::start(
            Arc::new(MalformedChangeReader),
            watermark.clone(),
            config,
        )?;
        let handle = controller.handle();
        let before = watermark.current();
        for offset in 0..32_u128 {
            handle.track(StoreId::from_ulid(ulid::Ulid::from(offset + 1)));
        }
        assert!(controller.diagnostics().overflows() > 0);
        assert!(watermark.current() > before);
        wait_until(Duration::from_secs(1), || {
            controller.diagnostics().failed_polls() > 0
        })
        .await?;
        assert!(controller.diagnostics().tracked_stores() <= 1);
        assert!(!controller.diagnostics().is_ready());
        controller.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_should_flush_and_disable_caching_on_changelog_timeout()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let watermark = InvalidationWatermark::new();
        let mut controller = InvalidationController::start(
            Arc::new(TimeoutChangeReader),
            watermark.clone(),
            InvalidationControllerConfig::new(
                NonZeroUsize::new(4).ok_or("invalid controller capacity")?,
                NonZeroU32::MIN,
                Duration::from_millis(1),
                Duration::from_millis(5),
                Duration::from_millis(20),
            )?,
        )?;
        let before = watermark.current();
        let handle = controller.handle();
        let tracked_store = store_id()?;
        handle.track(tracked_store);
        wait_until(Duration::from_secs(1), || {
            controller.diagnostics().failed_polls() > 0
        })
        .await?;
        assert!(watermark.current() > before);
        assert!(!handle.permits_caching(tracked_store));
        assert!(!controller.diagnostics().is_ready());
        controller.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_should_keep_cache_disabled_until_bounded_backlog_is_caught_up()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        for offset in 0..5 {
            write_tuple(&storage, &format!("document:{offset}#viewer@user:anne")).await?;
        }
        let watermark = InvalidationWatermark::new();
        let mut controller = InvalidationController::start(
            storage.clone(),
            watermark,
            InvalidationControllerConfig::new(
                NonZeroUsize::new(8).ok_or("invalid controller capacity")?,
                NonZeroU32::MIN,
                Duration::from_millis(50),
                Duration::from_millis(5),
                Duration::from_millis(50),
            )?,
        )?;
        controller.handle().track(store_id()?);
        wait_until(Duration::from_secs(1), || {
            controller.diagnostics().successful_polls() >= 1
        })
        .await?;
        assert!(!controller.diagnostics().is_ready());
        wait_until(Duration::from_secs(1), || {
            controller.diagnostics().is_ready()
        })
        .await?;
        controller.stop().await?;
        drop(controller);
        stop_storage(storage).await
    }

    #[derive(Debug)]
    struct MalformedChangeReader;

    #[async_trait]
    impl ChangeReader for MalformedChangeReader {
        async fn read_changes(
            &self,
            _context: &OperationContext,
            store_id: StoreId,
            _filter: &ChangeFilter,
            _options: &PageOptions,
        ) -> Result<Page<TupleChange>, StorageError> {
            let id: ChangeId = "01ARZ3NDEKTSV4RRFFQ69G5FAZ".parse().map_err(|_| {
                StorageError::new(openfga_storage::StorageErrorKind::Internal, "test_change")
            })?;
            let tuple = RelationshipTuple::unconditional(
                "document:one#viewer@user:anne"
                    .parse::<TupleKey>()
                    .map_err(|_| {
                        StorageError::new(openfga_storage::StorageErrorKind::Internal, "test_tuple")
                    })?,
            );
            let change = TupleChange::new(
                id,
                store_id,
                ChangeOperation::Write,
                tuple,
                SystemTime::now(),
            );
            Ok(Page::new(vec![change.clone(), change], None))
        }
    }

    #[derive(Debug)]
    struct TimeoutChangeReader;

    #[async_trait]
    impl ChangeReader for TimeoutChangeReader {
        async fn read_changes(
            &self,
            _context: &OperationContext,
            _store_id: StoreId,
            _filter: &ChangeFilter,
            _options: &PageOptions,
        ) -> Result<Page<TupleChange>, StorageError> {
            std::future::pending().await
        }
    }

    fn controller_config() -> Result<InvalidationControllerConfig, Box<dyn Error + Send + Sync>> {
        Ok(InvalidationControllerConfig::new(
            NonZeroUsize::new(32).ok_or("invalid test channel capacity")?,
            NonZeroU32::new(10).ok_or("invalid test page size")?,
            Duration::from_millis(5),
            Duration::from_millis(20),
            Duration::from_millis(100),
        )?)
    }

    async fn write_tuple(
        storage: &MemoryStorage,
        tuple: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        storage
            .write_tuples(
                &operation_context()?,
                store_id()?,
                Vec::new(),
                vec![RelationshipTuple::unconditional(tuple.parse()?)],
                TupleWriteOptions::default(),
            )
            .await?;
        Ok(())
    }

    fn operation_context() -> Result<OperationContext, Box<dyn Error + Send + Sync>> {
        Ok(OperationContext::new(
            ConsistencyPreference::HigherConsistency,
            Deadline::from_timeout(Instant::now(), RequestTimeout::new(Duration::from_secs(1))?)?,
            StorageCancellationToken::new(),
        ))
    }

    fn store_id() -> Result<StoreId, Box<dyn Error + Send + Sync>> {
        Ok("01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?)
    }

    async fn wait_until(
        maximum: Duration,
        predicate: impl Fn() -> bool,
    ) -> Result<(), &'static str> {
        let deadline = Instant::now() + maximum;
        while !predicate() {
            if Instant::now() >= deadline {
                return Err("controller condition timed out");
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(())
    }

    async fn stop_storage(storage: Arc<MemoryStorage>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut storage =
            Arc::try_unwrap(storage).map_err(|_| "memory storage references remain")?;
        storage.stop().await?;
        Ok(())
    }
}
