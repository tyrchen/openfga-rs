//! `PostgreSQL` migrations and a durable, bounded storage implementation.

#![forbid(unsafe_code)]

mod backend;
mod codec;
mod config;
mod error;
mod fault;
mod migration;
mod portable;

pub use backend::PostgresStorage;
pub use config::{
    PortableSqlConfigError, PortableSqlDialect, PortableSqlStorageConfig,
    PortableSqlStorageConfigBuilder, PostgresConfigError, PostgresStorageConfig,
    PostgresStorageConfigBuilder,
};
pub use fault::{
    SqlMutationFaultInjector, SqlMutationFaultInjector as PostgresMutationFaultInjector,
    SqlMutationStage, SqlMutationStage as PostgresMutationStage,
};
pub use migration::{
    MigrationState, MigrationStatus, apply_migrations, apply_portable_migrations, migration_status,
    portable_migration_status,
};
pub use portable::PortableSqlStorage;
