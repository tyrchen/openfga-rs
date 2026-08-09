//! PostgreSQL-to-storage error classification.

use std::borrow::Cow;

use openfga_storage::{StorageError, StorageErrorKind};

pub(crate) fn map_sqlx(error: sqlx::Error, code: &'static str) -> StorageError {
    let kind = match &error {
        sqlx::Error::RowNotFound => StorageErrorKind::NotFound,
        sqlx::Error::PoolTimedOut => StorageErrorKind::Timeout,
        sqlx::Error::PoolClosed | sqlx::Error::Io(_) | sqlx::Error::Tls(_) => {
            StorageErrorKind::Unavailable
        }
        sqlx::Error::Decode(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::ColumnNotFound(_) => StorageErrorKind::Integrity,
        sqlx::Error::Database(database) => match database.code().as_deref() {
            Some("23505") => StorageErrorKind::AlreadyExists,
            Some("23503") => StorageErrorKind::NotFound,
            Some("23502" | "23514" | "22P02") => StorageErrorKind::Integrity,
            Some("40001" | "40P01") => StorageErrorKind::Conflict,
            Some("57014") => StorageErrorKind::Timeout,
            Some(code) if code.starts_with("08") => StorageErrorKind::Unavailable,
            _ => StorageErrorKind::Internal,
        },
        _ => StorageErrorKind::Internal,
    };
    if matches!(
        kind,
        StorageErrorKind::Internal | StorageErrorKind::Unavailable
    ) {
        let sqlx_error_kind = match &error {
            sqlx::Error::Configuration(_) => "configuration",
            sqlx::Error::Database(_) => "database",
            sqlx::Error::Io(_) => "io",
            sqlx::Error::Tls(_) => "tls",
            sqlx::Error::Protocol(_) => "protocol",
            sqlx::Error::RowNotFound => "row_not_found",
            sqlx::Error::TypeNotFound { .. } => "type_not_found",
            sqlx::Error::ColumnIndexOutOfBounds { .. } => "column_index_out_of_bounds",
            sqlx::Error::ColumnNotFound(_) => "column_not_found",
            sqlx::Error::ColumnDecode { .. } => "column_decode",
            sqlx::Error::Decode(_) => "decode",
            sqlx::Error::AnyDriverError(_) => "any_driver",
            sqlx::Error::PoolTimedOut => "pool_timed_out",
            sqlx::Error::PoolClosed => "pool_closed",
            sqlx::Error::WorkerCrashed => "worker_crashed",
            sqlx::Error::Migrate(_) => "migrate",
            _ => "unknown",
        };
        let database_code = match &error {
            sqlx::Error::Database(database) => database.code().map(Cow::into_owned),
            _ => None,
        };
        tracing::error!(
            storage.error_kind = ?kind,
            storage.error_code = code,
            storage.sqlx_error_kind = sqlx_error_kind,
            storage.database_code = database_code.as_deref().unwrap_or("none"),
            "PostgreSQL operation failed",
        );
    }
    StorageError::with_source(kind, code, error)
}

pub(crate) fn cancelled() -> StorageError {
    StorageError::new(StorageErrorKind::Cancelled, "postgres_operation_cancelled")
}

pub(crate) fn timed_out() -> StorageError {
    StorageError::new(
        StorageErrorKind::Timeout,
        "postgres_operation_deadline_elapsed",
    )
}

#[cfg(test)]
mod tests {
    use openfga_storage::StorageErrorKind;

    use super::map_sqlx;

    #[test]
    fn test_should_map_and_redact_backend_failures() {
        let error = map_sqlx(sqlx::Error::RowNotFound, "postgres_test_failure");

        assert_eq!(error.kind(), StorageErrorKind::NotFound);
        assert_eq!(error.code(), "postgres_test_failure");
        assert!(format!("{error:?}").contains("[REDACTED]"));
    }
}
