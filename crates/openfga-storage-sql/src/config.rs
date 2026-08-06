//! Bounded `PostgreSQL` connection and routing configuration.

use std::{fmt, num::NonZeroU32, time::Duration};

use secrecy::{ExposeSecret, SecretString};
use typed_builder::TypedBuilder;

const MAXIMUM_DATABASE_URL_BYTES: usize = 4_096;

/// `PostgreSQL` primary/replica pool and transaction policy.
#[derive(Clone, TypedBuilder)]
#[non_exhaustive]
pub struct PostgresStorageConfig {
    /// Primary `PostgreSQL` connection URL.
    pub(crate) primary_url: SecretString,
    /// Optional read-replica `PostgreSQL` connection URL.
    #[builder(default)]
    pub(crate) replica_url: Option<SecretString>,
    /// Maximum connections in each pool.
    #[builder(default = NonZeroU32::new(16).unwrap_or(NonZeroU32::MIN))]
    pub(crate) max_connections: NonZeroU32,
    /// Minimum eagerly retained connections in each pool.
    #[builder(default)]
    pub(crate) min_connections: u32,
    /// Pool acquisition timeout.
    #[builder(default = Duration::from_secs(5))]
    pub(crate) acquire_timeout: Duration,
    /// Server-side statement timeout applied to every connection.
    #[builder(default = Duration::from_secs(5))]
    pub(crate) statement_timeout: Duration,
    /// Maximum acceptable replica replay lag for latency-preferring reads.
    #[builder(default = Duration::from_secs(1))]
    pub(crate) replica_max_lag: Duration,
    /// Maximum tuple keys accepted by one atomic mutation.
    #[builder(default = NonZeroU32::new(100).unwrap_or(NonZeroU32::MIN))]
    pub(crate) max_tuple_mutations: NonZeroU32,
    /// Whether connection establishment applies embedded forward migrations.
    #[builder(default = true)]
    pub(crate) migrate_on_connect: bool,
}

impl PostgresStorageConfig {
    /// Validates cross-field pool and timeout invariants.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresConfigError`] for an invalid pool or timeout policy.
    pub fn validate(&self) -> Result<(), PostgresConfigError> {
        if self.primary_url.expose_secret().len() > MAXIMUM_DATABASE_URL_BYTES {
            return Err(PostgresConfigError::PrimaryUrlTooLong);
        }
        if self
            .replica_url
            .as_ref()
            .is_some_and(|url| url.expose_secret().len() > MAXIMUM_DATABASE_URL_BYTES)
        {
            return Err(PostgresConfigError::ReplicaUrlTooLong);
        }
        if self.min_connections > self.max_connections.get() {
            return Err(PostgresConfigError::MinimumExceedsMaximum);
        }
        if self.acquire_timeout.is_zero() {
            return Err(PostgresConfigError::ZeroAcquireTimeout);
        }
        if self.statement_timeout.is_zero() {
            return Err(PostgresConfigError::ZeroStatementTimeout);
        }
        Ok(())
    }
}

impl fmt::Debug for PostgresStorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStorageConfig")
            .field("primary_url", &"[REDACTED]")
            .field(
                "replica_url",
                &self.replica_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field("max_connections", &self.max_connections)
            .field("min_connections", &self.min_connections)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("statement_timeout", &self.statement_timeout)
            .field("replica_max_lag", &self.replica_max_lag)
            .field("max_tuple_mutations", &self.max_tuple_mutations)
            .field("migrate_on_connect", &self.migrate_on_connect)
            .finish_non_exhaustive()
    }
}

/// Invalid `PostgreSQL` storage configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum PostgresConfigError {
    /// Primary `PostgreSQL` connection URL exceeds the accepted byte limit.
    #[error("primary PostgreSQL URL exceeds the byte limit")]
    PrimaryUrlTooLong,
    /// Replica `PostgreSQL` connection URL exceeds the accepted byte limit.
    #[error("replica PostgreSQL URL exceeds the byte limit")]
    ReplicaUrlTooLong,
    /// Minimum connections exceeds the configured maximum.
    #[error("minimum PostgreSQL connections exceeds maximum")]
    MinimumExceedsMaximum,
    /// Pool acquisition timeout is zero.
    #[error("PostgreSQL acquisition timeout must be nonzero")]
    ZeroAcquireTimeout,
    /// Server-side statement timeout is zero.
    #[error("PostgreSQL statement timeout must be nonzero")]
    ZeroStatementTimeout,
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;

    use super::{PostgresConfigError, PostgresStorageConfig};

    #[test]
    fn test_should_redact_connection_urls_from_debug_output() {
        let config = PostgresStorageConfig::builder()
            .primary_url(SecretString::from(
                "postgres://admin:secret@example.test/openfga".to_owned(),
            ))
            .build();
        let debug = format!("{config:?}");

        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("admin"));
    }

    #[test]
    fn test_should_reject_oversized_database_url() {
        let config = PostgresStorageConfig::builder()
            .primary_url(SecretString::from("x".repeat(4_097)))
            .build();

        assert_eq!(
            config.validate(),
            Err(PostgresConfigError::PrimaryUrlTooLong)
        );
    }
}
