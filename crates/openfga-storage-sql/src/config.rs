//! Bounded `PostgreSQL` connection and routing configuration.

use std::{fmt, num::NonZeroU32, time::Duration};

use secrecy::{ExposeSecret, SecretString};
use typed_builder::TypedBuilder;

const MAXIMUM_DATABASE_URL_BYTES: usize = 4_096;
const MAXIMUM_DATABASE_TIMEOUT: Duration = Duration::from_mins(5);

/// SQL dialect selected by a portable backend configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PortableSqlDialect {
    /// `MySQL` 8.4 or a compatible server.
    MySql,
    /// Embedded `SQLite` 3.
    Sqlite,
}

impl PortableSqlDialect {
    /// Returns the stable backend label used by diagnostics and metrics.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::MySql => "mysql",
            Self::Sqlite => "sqlite",
        }
    }
}

/// Bounded `MySQL` or `SQLite` pool and transaction policy.
#[derive(Clone, TypedBuilder)]
#[non_exhaustive]
pub struct PortableSqlStorageConfig {
    /// Backend dialect.
    pub(crate) dialect: PortableSqlDialect,
    /// Primary database URL.
    pub(crate) primary_url: SecretString,
    /// Maximum connections in the pool.
    #[builder(default = NonZeroU32::new(16).unwrap_or(NonZeroU32::MIN))]
    pub(crate) max_connections: NonZeroU32,
    /// Minimum eagerly retained connections.
    #[builder(default)]
    pub(crate) min_connections: u32,
    /// Pool acquisition timeout.
    #[builder(default = Duration::from_secs(5))]
    pub(crate) acquire_timeout: Duration,
    /// Per-operation database timeout used for rollback and connection setup.
    #[builder(default = Duration::from_secs(5))]
    pub(crate) statement_timeout: Duration,
    /// Maximum tuple keys accepted by one atomic mutation.
    #[builder(default = NonZeroU32::new(100).unwrap_or(NonZeroU32::MIN))]
    pub(crate) max_tuple_mutations: NonZeroU32,
    /// Whether connection establishment applies embedded forward migrations.
    #[builder(default = true)]
    pub(crate) migrate_on_connect: bool,
}

impl PortableSqlStorageConfig {
    /// Validates URL, pool, timeout, and `SQLite` serialization invariants.
    ///
    /// # Errors
    ///
    /// Returns [`PortableSqlConfigError`] when the configuration is unsafe or invalid.
    pub fn validate(&self) -> Result<(), PortableSqlConfigError> {
        let url = self.primary_url.expose_secret();
        if url.len() > MAXIMUM_DATABASE_URL_BYTES {
            return Err(PortableSqlConfigError::UrlTooLong);
        }
        let expected = match self.dialect {
            PortableSqlDialect::MySql => "mysql://",
            PortableSqlDialect::Sqlite => "sqlite:",
        };
        if !url.starts_with(expected) {
            return Err(PortableSqlConfigError::DialectUrlMismatch);
        }
        if self.min_connections > self.max_connections.get() {
            return Err(PortableSqlConfigError::MinimumExceedsMaximum);
        }
        if self.acquire_timeout.is_zero() {
            return Err(PortableSqlConfigError::ZeroAcquireTimeout);
        }
        if self.statement_timeout.is_zero() {
            return Err(PortableSqlConfigError::ZeroStatementTimeout);
        }
        if self.acquire_timeout > MAXIMUM_DATABASE_TIMEOUT
            || self.statement_timeout > MAXIMUM_DATABASE_TIMEOUT
        {
            return Err(PortableSqlConfigError::TimeoutTooLong);
        }
        if self.dialect == PortableSqlDialect::Sqlite && self.max_connections.get() != 1 {
            return Err(PortableSqlConfigError::SqliteRequiresSingleConnection);
        }
        Ok(())
    }
}

impl fmt::Debug for PortableSqlStorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortableSqlStorageConfig")
            .field("dialect", &self.dialect)
            .field("primary_url", &"[REDACTED]")
            .field("max_connections", &self.max_connections)
            .field("min_connections", &self.min_connections)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("statement_timeout", &self.statement_timeout)
            .field("max_tuple_mutations", &self.max_tuple_mutations)
            .field("migrate_on_connect", &self.migrate_on_connect)
            .finish_non_exhaustive()
    }
}

/// Invalid `MySQL` or `SQLite` storage configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum PortableSqlConfigError {
    /// Database URL exceeds the accepted byte limit.
    #[error("database URL exceeds the byte limit")]
    UrlTooLong,
    /// Database URL scheme does not match the selected dialect.
    #[error("database URL scheme does not match the selected SQL dialect")]
    DialectUrlMismatch,
    /// Minimum connections exceeds the configured maximum.
    #[error("minimum SQL connections exceeds maximum")]
    MinimumExceedsMaximum,
    /// Pool acquisition timeout is zero.
    #[error("SQL acquisition timeout must be nonzero")]
    ZeroAcquireTimeout,
    /// Statement timeout is zero.
    #[error("SQL statement timeout must be nonzero")]
    ZeroStatementTimeout,
    /// A timeout exceeds the supported operational ceiling.
    #[error("SQL timeout exceeds five minutes")]
    TimeoutTooLong,
    /// `SQLite` requires exactly one connection so writes are serialized.
    #[error("SQLite requires exactly one pooled connection")]
    SqliteRequiresSingleConnection,
}

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
    use std::num::NonZeroU32;

    use secrecy::SecretString;

    use super::{
        PortableSqlConfigError, PortableSqlDialect, PortableSqlStorageConfig, PostgresConfigError,
        PostgresStorageConfig,
    };

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

    #[test]
    fn test_should_redact_and_validate_portable_database_urls() {
        let config = PortableSqlStorageConfig::builder()
            .dialect(PortableSqlDialect::MySql)
            .primary_url(SecretString::from(
                "mysql://admin:secret@example.test/openfga".to_owned(),
            ))
            .build();
        assert_eq!(config.validate(), Ok(()));
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("admin"));

        let mismatch = PortableSqlStorageConfig::builder()
            .dialect(PortableSqlDialect::MySql)
            .primary_url(SecretString::from("sqlite::memory:".to_owned()))
            .build();
        assert_eq!(
            mismatch.validate(),
            Err(PortableSqlConfigError::DialectUrlMismatch),
        );

        let concurrent_sqlite = PortableSqlStorageConfig::builder()
            .dialect(PortableSqlDialect::Sqlite)
            .primary_url(SecretString::from("sqlite::memory:".to_owned()))
            .max_connections(NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN))
            .build();
        assert_eq!(
            concurrent_sqlite.validate(),
            Err(PortableSqlConfigError::SqliteRequiresSingleConnection),
        );
    }
}
