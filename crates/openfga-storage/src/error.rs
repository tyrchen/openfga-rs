//! Backend-neutral, redacted storage failures.

use std::{error::Error as StdError, fmt};

use thiserror::Error;

/// Stable storage failure category used by services and transports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageErrorKind {
    /// The requested record does not exist.
    NotFound,
    /// The requested identity already exists.
    AlreadyExists,
    /// Existing state conflicts with the requested atomic operation.
    Conflict,
    /// A pagination cursor is malformed or incompatible with the operation.
    InvalidContinuation,
    /// The operation exceeded its deadline.
    Timeout,
    /// The caller cancelled the operation.
    Cancelled,
    /// The backend or actor is not available.
    Unavailable,
    /// Persisted state violates a semantic invariant.
    Integrity,
    /// A configured finite resource bound was exceeded.
    ResourceExhausted,
    /// An internal backend operation failed.
    Internal,
}

/// One safe storage error with an optional redacted backend source chain.
#[derive(Error)]
#[error("storage failure {kind:?}: {code}")]
#[non_exhaustive]
pub struct StorageError {
    kind: StorageErrorKind,
    code: &'static str,
    #[source]
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl StorageError {
    /// Creates a safe storage failure without a backend source.
    #[must_use]
    pub const fn new(kind: StorageErrorKind, code: &'static str) -> Self {
        Self {
            kind,
            code,
            source: None,
        }
    }

    /// Creates a storage failure while preserving its source for internal inspection.
    #[must_use]
    pub fn with_source(
        kind: StorageErrorKind,
        code: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            code,
            source: Some(Box::new(source)),
        }
    }

    /// Returns the stable storage failure category.
    #[must_use]
    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    /// Returns the non-sensitive stable incident code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Debug for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("source", &self.source.as_ref().map(|_| "[REDACTED]"))
            .finish_non_exhaustive()
    }
}
