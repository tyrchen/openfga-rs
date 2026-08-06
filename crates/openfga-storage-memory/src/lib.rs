//! Actor-owned in-memory storage with atomic indexes and tuple changelog.
//!
//! One Tokio task owns every map. Public methods communicate through a bounded
//! MPSC channel and receive owned snapshots, so callers never hold actor state or
//! a lock. Tuple mutations are fully prepared before one infallible apply step.
//!
//! # Examples
//!
//! ```no_run
//! use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut storage = MemoryStorage::start(MemoryStorageConfig::default())?;
//! // Query components depend on narrow capability-trait references to this owner.
//! storage.stop().await?;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod actor;
mod backend;
mod clock;
mod config;
mod state;

pub use backend::{MemoryDiagnostics, MemoryStorage};
pub use clock::{StorageClock, SystemStorageClock};
pub use config::{
    MemoryStorageConfig, MutationFaultInjector, MutationFaultStage, NoMutationFaults,
};
