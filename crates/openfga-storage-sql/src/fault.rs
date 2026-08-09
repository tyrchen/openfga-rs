//! Deterministic tuple-mutation fault injection for transaction tests.

use std::fmt;

use openfga_storage::StorageError;

/// Observable atomic tuple-mutation transaction stages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SqlMutationStage {
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
pub trait SqlMutationFaultInjector: fmt::Debug + Send + Sync {
    /// Returns a failure to abort the transaction at `stage`.
    ///
    /// # Errors
    ///
    /// Returns the injected backend-neutral failure.
    fn check(&self, stage: SqlMutationStage) -> Result<(), StorageError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoSqlMutationFaults;

impl SqlMutationFaultInjector for NoSqlMutationFaults {
    fn check(&self, _stage: SqlMutationStage) -> Result<(), StorageError> {
        Ok(())
    }
}
