//! PostgreSQL-to-storage error classification.

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
