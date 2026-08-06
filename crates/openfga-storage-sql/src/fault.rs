//! Deterministic tuple-mutation fault injection for transaction tests.

use std::fmt;

use openfga_storage::StorageError;

/// Observable atomic tuple-mutation transaction stages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostgresMutationStage {
    /// A transaction started but no locks were acquired.
    BeforeLock,
    /// All affected canonical tuple keys were locked and read.
    AfterLock,
    /// Effective deletes were applied.
    AfterDelete,
    /// Effective writes were applied.
    AfterWrite,
    /// Changelog rows were appended.
    AfterChangelog,
    /// The transaction is ready to commit.
    BeforeCommit,
}

/// Test seam used to prove rollback at every mutation boundary.
pub trait PostgresMutationFaultInjector: fmt::Debug + Send + Sync {
    /// Returns a failure to abort the transaction at `stage`.
    ///
    /// # Errors
    ///
    /// Returns the injected backend-neutral failure.
    fn check(&self, stage: PostgresMutationStage) -> Result<(), StorageError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoPostgresMutationFaults;

impl PostgresMutationFaultInjector for NoPostgresMutationFaults {
    fn check(&self, _stage: PostgresMutationStage) -> Result<(), StorageError> {
        Ok(())
    }
}
