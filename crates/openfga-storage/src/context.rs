//! Explicit storage deadline, cancellation, and consistency context.

use std::{fmt, sync::Arc, time::Instant};

use openfga_domain::{ConsistencyPreference, Deadline};
use tokio::sync::watch;

use crate::{StorageError, StorageErrorKind};

#[derive(Debug)]
struct CancellationState {
    sender: watch::Sender<bool>,
}

/// Cloneable cancellation signal shared by one storage operation tree.
#[derive(Clone)]
#[non_exhaustive]
pub struct StorageCancellationToken(Arc<CancellationState>);

impl StorageCancellationToken {
    /// Creates a live cancellation token.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(CancellationState {
            sender: watch::channel(false).0,
        }))
    }

    /// Marks the token cancelled and wakes pending operations.
    pub fn cancel(&self) {
        self.0.sender.send_replace(true);
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.0.sender.borrow()
    }

    /// Waits until cancellation is requested without a lost-wakeup race.
    pub async fn cancelled(&self) {
        let mut receiver = self.0.sender.subscribe();
        if *receiver.borrow_and_update() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow_and_update() {
                return;
            }
        }
    }
}

impl Default for StorageCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for StorageCancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageCancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Shared context required by every asynchronous storage operation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct OperationContext {
    consistency: ConsistencyPreference,
    deadline: Deadline,
    cancellation: StorageCancellationToken,
}

impl OperationContext {
    /// Creates an explicit storage operation context.
    #[must_use]
    pub const fn new(
        consistency: ConsistencyPreference,
        deadline: Deadline,
        cancellation: StorageCancellationToken,
    ) -> Self {
        Self {
            consistency,
            deadline,
            cancellation,
        }
    }

    /// Returns the caller-selected consistency preference.
    #[must_use]
    pub const fn consistency(&self) -> ConsistencyPreference {
        self.consistency
    }

    /// Returns the absolute monotonic deadline.
    #[must_use]
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Returns the shared cancellation token.
    #[must_use]
    pub const fn cancellation(&self) -> &StorageCancellationToken {
        &self.cancellation
    }

    /// Fails before dispatch when cancellation or the deadline already applies.
    ///
    /// # Errors
    ///
    /// Returns a stable cancelled or timeout storage error.
    pub fn check(&self) -> Result<(), StorageError> {
        if self.cancellation.is_cancelled() {
            return Err(StorageError::new(
                StorageErrorKind::Cancelled,
                "operation_cancelled",
            ));
        }
        if self.deadline.is_elapsed(Instant::now()) {
            return Err(StorageError::new(
                StorageErrorKind::Timeout,
                "operation_deadline_elapsed",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use openfga_domain::{ConsistencyPreference, Deadline, RequestTimeout};
    use tokio::task::JoinSet;

    use super::{OperationContext, StorageCancellationToken};
    use crate::StorageErrorKind;

    #[test]
    fn test_should_reject_an_elapsed_deadline_before_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let timeout = RequestTimeout::new(Duration::from_millis(1))?;
        let deadline = Deadline::from_timeout(Instant::now(), timeout)?;
        std::thread::sleep(Duration::from_millis(2));
        let context = OperationContext::new(
            ConsistencyPreference::HigherConsistency,
            deadline,
            StorageCancellationToken::new(),
        );

        let error = context.check().err().ok_or("elapsed deadline passed")?;
        assert_eq!(error.kind(), StorageErrorKind::Timeout);
        Ok(())
    }

    #[tokio::test]
    async fn test_should_wake_every_cancellation_waiter_without_lost_notifications() {
        let cancellation = StorageCancellationToken::new();
        let mut waiters = JoinSet::new();
        for _ in 0..32 {
            let waiter = cancellation.clone();
            waiters.spawn(async move { waiter.cancelled().await });
        }
        tokio::task::yield_now().await;
        cancellation.cancel();

        while let Some(result) = waiters.join_next().await {
            assert!(result.is_ok());
        }
        assert!(cancellation.is_cancelled());
    }
}
