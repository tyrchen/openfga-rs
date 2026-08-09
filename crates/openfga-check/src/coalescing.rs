//! Bounded identical-request coalescing with an oracle shadow mode and kill switch.

use std::{
    fmt,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use moka::future::Cache;
use openfga_cache::{DecisionKey, DecisionKeyHasher, InvalidationWatermark};
use openfga_domain::{BatchCheckCommand, CheckCommand, ConsistencyPreference};
use openfga_model::CompiledModel;
use openfga_storage::{StorageCancellationToken, TupleReader};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, ObservableGauge},
};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    time::{Instant as TokioInstant, sleep_until},
};
use tracing::error;

use crate::{
    BatchCheckOutcome, CheckBudget, CheckError, CheckEvaluator, CheckOutcome, CheckWorkMeter,
};

const COALESCING_SEMANTICS_VERSION: u32 = 1;
const MAXIMUM_IN_FLIGHT_KEYS: u64 = 1_000_000;
const ENABLED_VERIFICATION_SAMPLE_INTERVAL: u64 = 64;

/// Runtime rollout mode for identical `Check` request coalescing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckCoalescingMode {
    /// Use only the permanent oracle evaluator.
    #[default]
    Disabled,
    /// Return the oracle result while comparing a coalesced candidate.
    Shadow,
    /// Return coalesced results unless the mismatch kill switch trips.
    Enabled,
}

/// Validated finite policy for simultaneous identical `Check` requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CheckCoalescingConfig {
    mode: CheckCoalescingMode,
    maximum_in_flight_keys: NonZeroU64,
}

impl CheckCoalescingConfig {
    /// Creates a bounded coalescing policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the key bound exceeds the process safety ceiling.
    pub fn new(
        mode: CheckCoalescingMode,
        maximum_in_flight_keys: NonZeroU64,
    ) -> Result<Self, CheckCoalescingConfigError> {
        if maximum_in_flight_keys.get() > MAXIMUM_IN_FLIGHT_KEYS {
            return Err(CheckCoalescingConfigError::MaximumInFlightKeys);
        }
        Ok(Self {
            mode,
            maximum_in_flight_keys,
        })
    }

    /// Returns the configured rollout mode.
    #[must_use]
    pub const fn mode(self) -> CheckCoalescingMode {
        self.mode
    }
}

/// Invalid request-coalescing policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckCoalescingConfigError {
    /// The number of tracked simultaneous keys exceeds the process ceiling.
    #[error("check coalescing maximum in-flight keys must not exceed 1000000")]
    MaximumInFlightKeys,
}

/// Check evaluator decorator that shares simultaneous identical computations.
///
/// Higher-consistency requests and requests carrying an aggregate work meter
/// always bypass the strategy. Failures are never shared: a waiter retries with
/// its own deadline and cancellation token, preserving request-local behavior.
#[derive(Clone)]
#[non_exhaustive]
pub struct CoalescingCheckEvaluator {
    oracle: Arc<dyn CheckEvaluator>,
    mode: CheckCoalescingMode,
    in_flight: Cache<CoalescingKey, SharedEvaluation>,
    key_hasher: DecisionKeyHasher,
    invalidation: InvalidationWatermark,
    killed: Arc<AtomicBool>,
    verification_sequence: Arc<AtomicU64>,
    metrics: CoalescingMetrics,
}

impl CoalescingCheckEvaluator {
    /// Creates a strategy decorator around the permanent oracle evaluator.
    #[must_use]
    pub fn new(
        oracle: Arc<dyn CheckEvaluator>,
        config: CheckCoalescingConfig,
        key_hasher: DecisionKeyHasher,
    ) -> Self {
        Self::with_invalidation(oracle, config, key_hasher, InvalidationWatermark::new())
    }

    /// Creates a strategy sharing the mutable-cache invalidation generation.
    #[must_use]
    pub fn with_invalidation(
        oracle: Arc<dyn CheckEvaluator>,
        config: CheckCoalescingConfig,
        key_hasher: DecisionKeyHasher,
        invalidation: InvalidationWatermark,
    ) -> Self {
        let killed = Arc::new(AtomicBool::new(false));
        Self {
            oracle,
            mode: config.mode,
            in_flight: Cache::builder()
                .max_capacity(config.maximum_in_flight_keys.get())
                .build(),
            key_hasher,
            invalidation,
            killed: Arc::clone(&killed),
            verification_sequence: Arc::new(AtomicU64::new(0)),
            metrics: CoalescingMetrics::new(killed),
        }
    }

    /// Returns whether a mismatch or runtime control disabled the strategy.
    #[must_use]
    pub fn is_killed(&self) -> bool {
        self.killed.load(Ordering::Acquire)
    }

    /// Immediately and permanently disables coalescing for this evaluator instance.
    ///
    /// This is the runtime rollback control used by mismatch handling and may also
    /// be called by an operator control plane. A process restart is required to
    /// create a fresh instance after the underlying incident is resolved.
    pub fn disable(&self) {
        self.killed.store(true, Ordering::Release);
        self.metrics.record("disabled");
    }

    fn bypasses(&self, command: &CheckCommand, work_meter: Option<&CheckWorkMeter>) -> bool {
        self.mode == CheckCoalescingMode::Disabled
            || self.is_killed()
            || command.query().consistency() == ConsistencyPreference::HigherConsistency
            || work_meter.is_some()
    }

    fn should_verify_enabled(&self) -> bool {
        let sequence = self.verification_sequence.fetch_add(1, Ordering::Relaxed);
        sequence.is_multiple_of(ENABLED_VERIFICATION_SAMPLE_INTERVAL)
    }

    async fn evaluate_shared(&self, computation: SharedComputation<'_>) -> SharedEvaluation {
        let candidate = self
            .oracle
            .check(
                computation.command,
                Arc::clone(&computation.model),
                Arc::clone(&computation.tuples),
                computation.budget,
                None,
                computation.cancellation.clone(),
            )
            .await;
        SharedEvaluation::from_result(candidate, &computation.own_error).await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the evaluator trait contract is forwarded without hiding request-local controls"
    )]
    async fn verified_enabled(
        &self,
        command: &CheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, CheckError> {
        self.metrics.record("verification_sample");
        let generation = self.invalidation.current();
        let oracle = self.oracle.check(
            command,
            Arc::clone(&model),
            Arc::clone(&tuples),
            budget,
            None,
            cancellation.clone(),
        );
        let candidate = self.coalesced(command, model, tuples, budget, cancellation);
        let (oracle, candidate) = tokio::join!(oracle, candidate);
        if self.invalidation.current() == generation {
            self.compare_and_maybe_kill(&oracle, &candidate);
        }
        oracle
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the evaluator trait contract is forwarded without hiding request-local controls"
    )]
    async fn coalesced(
        &self,
        command: &CheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, CheckError> {
        let generation = self.invalidation.current();
        let key = CoalescingKey::new(
            command,
            &model,
            &tuples,
            &self.key_hasher,
            budget,
            generation,
        );
        let own_error = Arc::new(Mutex::new(None));
        let computation = SharedComputation {
            command,
            model: Arc::clone(&model),
            tuples: Arc::clone(&tuples),
            budget,
            cancellation: cancellation.clone(),
            own_error: Arc::clone(&own_error),
        };
        let entry = self
            .in_flight
            .entry(key.clone())
            .or_insert_with(async move { self.evaluate_shared(computation).await });
        tokio::pin!(entry);
        let entry = tokio::select! {
            () = cancellation.cancelled() => {
                return self.oracle
                    .check(command, model, tuples, budget, None, cancellation)
                    .await;
            }
            () = sleep_until(TokioInstant::from_std(command.query().deadline().instant())) => {
                return self.oracle
                    .check(command, model, tuples, budget, None, cancellation)
                    .await;
            }
            entry = &mut entry => entry,
        };
        self.metrics.record(if entry.is_fresh() {
            "leader"
        } else {
            "coalesced"
        });
        let shared = entry.into_value();
        self.in_flight.invalidate(&key).await;

        if cancellation.is_cancelled() || command.query().deadline().is_elapsed(Instant::now()) {
            return self
                .oracle
                .check(command, model, tuples, budget, None, cancellation)
                .await;
        }
        match shared {
            SharedEvaluation::Outcome(outcome) => Ok(outcome),
            SharedEvaluation::Error => {
                if let Some(error) = own_error.lock().await.take() {
                    return Err(error);
                }
                self.metrics.record("error_retry");
                self.oracle
                    .check(command, model, tuples, budget, None, cancellation)
                    .await
            }
        }
    }

    fn compare_and_maybe_kill(
        &self,
        oracle: &Result<CheckOutcome, CheckError>,
        candidate: &Result<CheckOutcome, CheckError>,
    ) {
        if equivalent(oracle, candidate) {
            self.metrics.record("match");
            return;
        }
        self.disable();
        self.metrics.record("mismatch");
        error!(
            strategy = "check_request_coalescing",
            "authorization strategy mismatch; kill switch engaged"
        );
    }
}

impl fmt::Debug for CoalescingCheckEvaluator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoalescingCheckEvaluator")
            .field("oracle", &"dyn CheckEvaluator")
            .field("mode", &self.mode)
            .field("in_flight_keys", &self.in_flight.entry_count())
            .field("key_hasher", &self.key_hasher)
            .field("invalidation", &self.invalidation)
            .field("killed", &self.is_killed())
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CheckEvaluator for CoalescingCheckEvaluator {
    async fn check(
        &self,
        command: &CheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        work_meter: Option<CheckWorkMeter>,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, CheckError> {
        if self.bypasses(command, work_meter.as_ref()) {
            self.metrics.record("bypass");
            return self
                .oracle
                .check(command, model, tuples, budget, work_meter, cancellation)
                .await;
        }
        if self.mode == CheckCoalescingMode::Shadow {
            let generation = self.invalidation.current();
            let oracle = self.oracle.check(
                command,
                Arc::clone(&model),
                Arc::clone(&tuples),
                budget,
                None,
                cancellation.clone(),
            );
            let candidate = self.coalesced(command, model, tuples, budget, cancellation);
            let (oracle, candidate) = tokio::join!(oracle, candidate);
            if self.invalidation.current() == generation {
                self.compare_and_maybe_kill(&oracle, &candidate);
            }
            return oracle;
        }
        if self.mode == CheckCoalescingMode::Enabled && self.should_verify_enabled() {
            return self
                .verified_enabled(command, model, tuples, budget, cancellation)
                .await;
        }
        self.coalesced(command, model, tuples, budget, cancellation)
            .await
    }

    async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<BatchCheckOutcome, CheckError> {
        self.metrics.record("batch_bypass");
        self.oracle
            .batch_check(command, model, tuples, budget, cancellation)
            .await
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct CoalescingKey {
    decision: DecisionKey,
    budget: BudgetKey,
    tuple_source: usize,
    generation: u64,
}

impl CoalescingKey {
    fn new(
        command: &CheckCommand,
        model: &CompiledModel,
        tuples: &Arc<dyn TupleReader>,
        hasher: &DecisionKeyHasher,
        budget: CheckBudget,
        generation: u64,
    ) -> Self {
        Self {
            decision: DecisionKey::for_check(command, model, hasher, COALESCING_SEMANTICS_VERSION),
            budget: BudgetKey::from(budget),
            tuple_source: Arc::as_ptr(tuples).cast::<()>() as usize,
            generation,
        }
    }
}

impl fmt::Debug for CoalescingKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoalescingKey")
            .field("decision", &self.decision)
            .field("budget", &self.budget)
            .field("tuple_source", &"dyn TupleReader")
            .field("generation", &self.generation)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct BudgetKey {
    depth: u32,
    dispatches: u32,
    datastore_queries: u32,
    tuple_items: u32,
    condition_cost: u32,
    concurrent_reads: usize,
}

impl From<CheckBudget> for BudgetKey {
    fn from(value: CheckBudget) -> Self {
        Self {
            depth: value.maximum_depth(),
            dispatches: value.maximum_dispatches(),
            datastore_queries: value.maximum_datastore_queries(),
            tuple_items: value.maximum_tuple_items(),
            condition_cost: value.maximum_condition_cost(),
            concurrent_reads: value.maximum_concurrent_reads(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SharedEvaluation {
    Outcome(CheckOutcome),
    Error,
}

struct SharedComputation<'a> {
    command: &'a CheckCommand,
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: CheckBudget,
    cancellation: StorageCancellationToken,
    own_error: Arc<Mutex<Option<CheckError>>>,
}

impl SharedEvaluation {
    async fn from_result(
        result: Result<CheckOutcome, CheckError>,
        own_error: &Mutex<Option<CheckError>>,
    ) -> Self {
        match result {
            Ok(outcome) => Self::Outcome(outcome),
            Err(error) => {
                *own_error.lock().await = Some(error);
                Self::Error
            }
        }
    }
}

fn equivalent(
    oracle: &Result<CheckOutcome, CheckError>,
    candidate: &Result<CheckOutcome, CheckError>,
) -> bool {
    match (oracle, candidate) {
        (Ok(oracle), Ok(candidate)) => oracle.allowed() == candidate.allowed(),
        (Err(oracle), Err(candidate)) => {
            oracle.kind() == candidate.kind() && oracle.code() == candidate.code()
        }
        (Ok(_), Err(_)) | (Err(_), Ok(_)) => false,
    }
}

#[derive(Clone)]
struct CoalescingMetrics {
    requests: Counter<u64>,
    _killed: ObservableGauge<u64>,
}

impl CoalescingMetrics {
    fn new(killed: Arc<AtomicBool>) -> Self {
        let meter = opentelemetry::global::meter("openfga-check");
        Self {
            requests: meter
                .u64_counter("openfga.check.coalescing.requests")
                .with_description("Check coalescing events by bounded result class")
                .build(),
            _killed: meter
                .u64_observable_gauge("openfga.check.coalescing.killed")
                .with_description("Whether the Check coalescing runtime kill switch is engaged")
                .with_callback(move |observer| {
                    observer.observe(u64::from(killed.load(Ordering::Acquire)), &[]);
                })
                .build(),
        }
    }

    fn record(&self, result: &'static str) {
        self.requests.add(1, &[KeyValue::new("result", result)]);
    }
}

impl fmt::Debug for CoalescingMetrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CoalescingMetrics")
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc, time::Duration};

    use openfga_cache::{DecisionKeyHasher, InvalidationWatermark};

    use super::{CheckCoalescingConfig, CheckCoalescingMode, CoalescingCheckEvaluator};
    use crate::{
        CheckError, CheckErrorKind, CheckMetadata, CheckOutcome, CheckResolution,
        DirectCheckEvaluator,
    };

    #[test]
    fn test_should_engage_kill_switch_on_shadow_mismatch() -> Result<(), Box<dyn std::error::Error>>
    {
        let strategy = CoalescingCheckEvaluator::with_invalidation(
            Arc::new(DirectCheckEvaluator::default()),
            CheckCoalescingConfig::new(CheckCoalescingMode::Shadow, NonZeroU64::MIN)?,
            DecisionKeyHasher::random()?,
            InvalidationWatermark::new(),
        );
        let oracle = Ok(CheckOutcome::new(
            true,
            CheckResolution::Direct,
            CheckMetadata::new(1, 1, 1, 0, 0, 1, Duration::ZERO),
        ));
        let candidate = Err(CheckError::new(
            CheckErrorKind::Internal,
            "injected_shadow_mismatch",
        ));
        strategy.compare_and_maybe_kill(&oracle, &candidate);
        assert!(strategy.is_killed());
        Ok(())
    }
}
