//! Injectable wall clock for model-publication timestamps.

use std::{fmt, time::SystemTime};

/// Wall-clock source used only for persisted publication metadata.
pub trait ServiceClock: Send + Sync {
    /// Returns the current wall-clock timestamp.
    fn now(&self) -> SystemTime;
}

/// Production service clock backed by [`SystemTime::now`].
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct SystemServiceClock;

impl ServiceClock for SystemServiceClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

impl fmt::Debug for dyn ServiceClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dyn ServiceClock")
    }
}
