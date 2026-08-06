//! Injected transaction wall clock.

use std::{fmt, time::SystemTime};

use openfga_storage::StorageError;

/// Synchronous transaction clock called only inside the memory actor.
pub trait StorageClock: fmt::Debug + Send + Sync {
    /// Returns one transaction timestamp.
    ///
    /// # Errors
    ///
    /// Returns a safe internal storage error when the clock is unavailable.
    fn now(&self) -> Result<SystemTime, StorageError>;
}

/// Production system wall clock.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct SystemStorageClock;

impl StorageClock for SystemStorageClock {
    fn now(&self) -> Result<SystemTime, StorageError> {
        Ok(SystemTime::now())
    }
}
