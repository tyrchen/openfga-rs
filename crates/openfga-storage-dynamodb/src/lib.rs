//! Single-table Amazon `DynamoDB` implementation of every storage capability.
//!
//! The AWS SDK generates deliberately large concrete futures. Boxing every SDK call would add a
//! heap allocation to each storage operation without reducing the size of the public futures
//! produced by the storage traits.

#![allow(clippy::large_futures, reason = "AWS SDK operation futures are large")]
#![allow(
    clippy::similar_names,
    reason = "partition-key and sort-key pairs intentionally use parallel names"
)]
#![allow(
    clippy::too_many_arguments,
    reason = "blob persistence keeps all durability inputs explicit"
)]
#![allow(
    clippy::too_many_lines,
    reason = "atomic tuple and blob transactions are kept contiguous for auditability"
)]
//!
//! The physical schema uses only the base table's `pk` string partition key and
//! `sk` binary sort key. Runtime code never issues `Scan` and never owns AWS
//! credentials; authentication is delegated to the AWS SDK credential chain.

#![forbid(unsafe_code)]

mod backend;
mod client;
mod config;
mod item;
mod key;
mod migration;
mod runtime;

pub use backend::DynamoDbStorage;
pub use config::{
    DevelopmentEndpoint, DynamoDbConfigError, DynamoDbGarbageCollectionConfig,
    DynamoDbMutationLimit, DynamoDbProvisioningConfig, DynamoDbStorageConfig,
    DynamoDbStorageConfigBuilder, DynamoDbTableName, KmsKeyIdentifier, RegionName,
};
pub use migration::{DYNAMODB_SCHEMA_VERSION, DynamoDbProvisioningStatus};
pub use runtime::{DynamoDbRuntime, DynamoDbRuntimeDiagnostics};
