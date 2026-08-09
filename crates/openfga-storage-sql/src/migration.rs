//! Locked forward migration commands and read-only schema status.

use openfga_storage::{StorageError, StorageErrorKind};
use secrecy::ExposeSecret;
use sqlx::Row;

use crate::{
    PortableSqlDialect, PortableSqlStorageConfig, PostgresStorageConfig,
    backend::{MIGRATOR, SCHEMA_VERSION, connect_pool},
    portable::{PORTABLE_SCHEMA_VERSION, connect_pool as connect_portable_pool, migrator},
};

/// Schema relationship between a database and this binary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MigrationState {
    /// No `SQLx` migration table exists yet.
    Fresh,
    /// The database is older than this binary.
    Pending,
    /// The database exactly matches this binary.
    Current,
    /// The database was migrated by a newer binary.
    TooNew,
}

/// Redacted migration status suitable for operational output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MigrationStatus {
    current: Option<i64>,
    target: i64,
    state: MigrationState,
}

impl MigrationStatus {
    /// Returns the newest successfully applied version, if any.
    #[must_use]
    pub const fn current(self) -> Option<i64> {
        self.current
    }

    /// Returns the newest embedded migration version.
    #[must_use]
    pub const fn target(self) -> i64 {
        self.target
    }

    /// Returns the database/binary version relationship.
    #[must_use]
    pub const fn state(self) -> MigrationState {
        self.state
    }
}

/// Applies embedded migrations under `SQLx`'s backend advisory lock.
///
/// `SQLx` verifies the checksum of every previously applied migration before
/// advancing. Migrations are forward-only and transactional unless a migration
/// explicitly declares otherwise.
///
/// # Errors
///
/// Returns a redacted configuration, connection, lock, checksum, migration, or
/// schema compatibility failure.
pub async fn apply_migrations(
    config: &PostgresStorageConfig,
) -> Result<MigrationStatus, StorageError> {
    config.validate().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "postgres_configuration_invalid",
            error,
        )
    })?;
    let pool = connect_pool(config, config.primary_url.expose_secret()).await?;
    let result = async {
        MIGRATOR.run(&pool).await.map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "postgres_migration_failed",
                error,
            )
        })?;
        status_from_pool(&pool).await
    }
    .await;
    pool.close().await;
    result
}

/// Reads schema status without creating tables or applying migrations.
///
/// # Errors
///
/// Returns a redacted configuration, connection, interrupted-migration,
/// checksum, or metadata compatibility failure.
pub async fn migration_status(
    config: &PostgresStorageConfig,
) -> Result<MigrationStatus, StorageError> {
    config.validate().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "postgres_configuration_invalid",
            error,
        )
    })?;
    let pool = connect_pool(config, config.primary_url.expose_secret()).await?;
    let result = status_from_pool(&pool).await;
    pool.close().await;
    result
}

/// Applies the selected `MySQL` or `SQLite` embedded migration set.
///
/// # Errors
///
/// Returns a redacted configuration, connection, checksum, migration, or schema failure.
pub async fn apply_portable_migrations(
    config: &PortableSqlStorageConfig,
) -> Result<MigrationStatus, StorageError> {
    validate_portable_config(config)?;
    let pool = connect_portable_pool(config, config.primary_url.expose_secret()).await?;
    let result = async {
        migrator(config.dialect).run(&pool).await.map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "portable_migration_failed",
                error,
            )
        })?;
        portable_status_from_pool(&pool, config.dialect).await
    }
    .await;
    pool.close().await;
    result
}

/// Reads `MySQL` or `SQLite` schema status without creating or migrating tables.
///
/// # Errors
///
/// Returns a redacted configuration, connection, checksum, interrupted-migration, or schema
/// compatibility failure.
pub async fn portable_migration_status(
    config: &PortableSqlStorageConfig,
) -> Result<MigrationStatus, StorageError> {
    validate_portable_config(config)?;
    let pool = connect_portable_pool(config, config.primary_url.expose_secret()).await?;
    let result = portable_status_from_pool(&pool, config.dialect).await;
    pool.close().await;
    result
}

fn validate_portable_config(config: &PortableSqlStorageConfig) -> Result<(), StorageError> {
    config.validate().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "portable_configuration_invalid",
            error,
        )
    })
}

async fn portable_status_from_pool(
    pool: &sqlx::AnyPool,
    dialect: PortableSqlDialect,
) -> Result<MigrationStatus, StorageError> {
    let existence_query = match dialect {
        PortableSqlDialect::MySql => {
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND \
             table_name = '_sqlx_migrations'"
        }
        PortableSqlDialect::Sqlite => {
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'"
        }
    };
    let exists = sqlx::query_scalar::<_, i64>(existence_query)
        .fetch_one(pool)
        .await
        .map_err(|error| operational(error, "portable_migration_status_failed"))?;
    if exists == 0 {
        return Ok(classify_at(None, PORTABLE_SCHEMA_VERSION));
    }

    let rows = sqlx::query(
        "SELECT version, checksum, CASE WHEN success THEN 1 ELSE 0 END AS succeeded FROM \
         _sqlx_migrations ORDER BY version ASC",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| operational(error, "portable_migration_history_failed"))?;
    let embedded = migrator(dialect);
    let mut current = None;
    for row in rows {
        let version = row
            .try_get::<i64, _>("version")
            .map_err(|error| operational(error, "portable_migration_version_invalid"))?;
        let checksum = row
            .try_get::<Vec<u8>, _>("checksum")
            .map_err(|error| operational(error, "portable_migration_checksum_invalid"))?;
        let succeeded = row
            .try_get::<i64, _>("succeeded")
            .map_err(|error| operational(error, "portable_migration_success_invalid"))?;
        if succeeded != 1 {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "portable_migration_interrupted",
            ));
        }
        let Some(expected) = embedded
            .iter()
            .find(|migration| migration.version == version)
        else {
            if version > PORTABLE_SCHEMA_VERSION {
                current = Some(version);
                continue;
            }
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "portable_migration_missing_from_binary",
            ));
        };
        if expected.checksum.as_ref() != checksum.as_slice() {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "portable_migration_checksum_mismatch",
            ));
        }
        current = Some(version);
    }

    let status = classify_at(current, PORTABLE_SCHEMA_VERSION);
    if status.state == MigrationState::Current {
        let logical = sqlx::query_scalar::<_, i64>(
            "SELECT schema_version FROM openfga_schema_metadata WHERE singleton = TRUE",
        )
        .fetch_one(pool)
        .await
        .map_err(|error| operational(error, "portable_schema_version_read_failed"))?;
        if logical != PORTABLE_SCHEMA_VERSION {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "portable_schema_metadata_mismatch",
            ));
        }
    }
    Ok(status)
}

async fn status_from_pool(pool: &sqlx::PgPool) -> Result<MigrationStatus, StorageError> {
    let exists =
        sqlx::query_scalar::<_, bool>("SELECT to_regclass('public._sqlx_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await
            .map_err(|error| operational(error, "postgres_migration_status_failed"))?;
    if !exists {
        return Ok(classify(None));
    }

    let rows =
        sqlx::query("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version ASC")
            .fetch_all(pool)
            .await
            .map_err(|error| operational(error, "postgres_migration_history_failed"))?;
    let mut current = None;
    for row in rows {
        let version = row
            .try_get::<i64, _>("version")
            .map_err(|error| operational(error, "postgres_migration_version_invalid"))?;
        let checksum = row
            .try_get::<Vec<u8>, _>("checksum")
            .map_err(|error| operational(error, "postgres_migration_checksum_invalid"))?;
        let success = row
            .try_get::<bool, _>("success")
            .map_err(|error| operational(error, "postgres_migration_success_invalid"))?;
        if !success {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "postgres_migration_interrupted",
            ));
        }
        let Some(embedded) = MIGRATOR
            .iter()
            .find(|migration| migration.version == version)
        else {
            if version > SCHEMA_VERSION {
                current = Some(version);
                continue;
            }
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "postgres_migration_missing_from_binary",
            ));
        };
        if embedded.checksum.as_ref() != checksum.as_slice() {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "postgres_migration_checksum_mismatch",
            ));
        }
        current = Some(version);
    }

    let status = classify(current);
    if status.state == MigrationState::Current {
        let logical = sqlx::query_scalar::<_, i64>(
            "SELECT schema_version FROM openfga_schema_metadata WHERE singleton = TRUE",
        )
        .fetch_one(pool)
        .await
        .map_err(|error| operational(error, "postgres_schema_version_read_failed"))?;
        if logical != SCHEMA_VERSION {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "postgres_schema_metadata_mismatch",
            ));
        }
    }
    Ok(status)
}

const fn classify(current: Option<i64>) -> MigrationStatus {
    classify_at(current, SCHEMA_VERSION)
}

const fn classify_at(current: Option<i64>, target: i64) -> MigrationStatus {
    let state = match current {
        None => MigrationState::Fresh,
        Some(version) if version < target => MigrationState::Pending,
        Some(version) if version == target => MigrationState::Current,
        Some(_) => MigrationState::TooNew,
    };
    MigrationStatus {
        current,
        target,
        state,
    }
}

fn operational(error: sqlx::Error, code: &'static str) -> StorageError {
    StorageError::with_source(StorageErrorKind::Unavailable, code, error)
}

#[cfg(test)]
mod tests {
    use super::{MigrationState, SCHEMA_VERSION, classify};

    #[test]
    fn test_should_classify_fresh_pending_current_and_too_new_schema() {
        assert_eq!(classify(None).state(), MigrationState::Fresh);
        assert_eq!(
            classify(Some(SCHEMA_VERSION - 1)).state(),
            MigrationState::Pending
        );
        assert_eq!(
            classify(Some(SCHEMA_VERSION)).state(),
            MigrationState::Current
        );
        assert_eq!(
            classify(Some(SCHEMA_VERSION + 1)).state(),
            MigrationState::TooNew
        );
    }
}
