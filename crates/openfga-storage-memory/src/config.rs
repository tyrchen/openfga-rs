//! Memory actor limits and deterministic mutation fault injection.

use std::{fmt, num::NonZeroUsize, time::Duration};

use openfga_domain::InputLimits;
use openfga_storage::StorageError;

const DEFAULT_CHANNEL_CAPACITY: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Runtime policy for one memory-storage actor.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct MemoryStorageConfig {
    input_limits: InputLimits,
    channel_capacity: NonZeroUsize,
    shutdown_timeout: Duration,
}

impl MemoryStorageConfig {
    /// Creates an explicit actor policy.
    ///
    /// # Errors
    ///
    /// Returns resource exhaustion when the shutdown timeout is zero.
    pub fn new(
        input_limits: InputLimits,
        channel_capacity: NonZeroUsize,
        shutdown_timeout: Duration,
    ) -> Result<Self, StorageError> {
        if shutdown_timeout.is_zero() {
            return Err(StorageError::new(
                openfga_storage::StorageErrorKind::ResourceExhausted,
                "memory_shutdown_timeout_zero",
            ));
        }
        Ok(Self {
            input_limits,
            channel_capacity,
            shutdown_timeout,
        })
    }

    /// Returns shared semantic input limits.
    #[must_use]
    pub const fn input_limits(&self) -> &InputLimits {
        &self.input_limits
    }

    /// Returns bounded actor command capacity.
    #[must_use]
    pub const fn channel_capacity(&self) -> usize {
        self.channel_capacity.get()
    }

    /// Returns the graceful actor shutdown timeout.
    #[must_use]
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }
}

impl Default for MemoryStorageConfig {
    fn default() -> Self {
        Self {
            input_limits: InputLimits::default(),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

/// Named pre-commit mutation stage used by deterministic fault tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutationFaultStage {
    /// Input identities and conflict policies have been validated.
    Validated,
    /// Existing deletes have been fully prepared.
    DeletesPrepared,
    /// Non-duplicate writes have been fully prepared.
    WritesPrepared,
    /// Timestamped changelog rows and tuple records have been fully prepared.
    ChangesPrepared,
}

/// Injected fault boundary for atomic mutation contract tests.
pub trait MutationFaultInjector: fmt::Debug + Send + Sync {
    /// Fails at a selected pre-commit stage.
    ///
    /// # Errors
    ///
    /// Returns a backend-neutral injected failure without mutating actor state.
    fn check(&self, stage: MutationFaultStage) -> Result<(), StorageError>;
}

/// Production mutation injector that never fails.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct NoMutationFaults;

impl MutationFaultInjector for NoMutationFaults {
    fn check(&self, _stage: MutationFaultStage) -> Result<(), StorageError> {
        Ok(())
    }
}
