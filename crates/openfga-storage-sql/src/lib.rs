//! `PostgreSQL` migrations and a durable, bounded storage implementation.

#![forbid(unsafe_code)]

mod backend;
mod codec;
mod config;
mod error;
mod fault;

pub use backend::PostgresStorage;
pub use config::{PostgresConfigError, PostgresStorageConfig, PostgresStorageConfigBuilder};
pub use fault::{PostgresMutationFaultInjector, PostgresMutationStage};
