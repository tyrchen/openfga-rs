//! `PostgreSQL` migrations and a durable, bounded storage implementation.

#![forbid(unsafe_code)]

mod backend;
mod codec;
mod config;
mod error;
mod fault;
mod migration;

pub use backend::PostgresStorage;
pub use config::{PostgresConfigError, PostgresStorageConfig, PostgresStorageConfigBuilder};
pub use fault::{PostgresMutationFaultInjector, PostgresMutationStage};
pub use migration::{MigrationState, MigrationStatus, apply_migrations, migration_status};
