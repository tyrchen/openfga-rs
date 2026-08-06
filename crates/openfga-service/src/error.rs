//! Stable, redacted service-orchestration failures.

use std::fmt;

use openfga_check::{CheckError, CheckErrorKind};
use openfga_storage::{StorageError, StorageErrorKind};
use thiserror::Error;

/// Stable failure category exposed by transport-neutral services.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceErrorKind {
    /// The selected immutable authorization model does not exist.
    ModelNotFound,
    /// A model read or other storage operation failed.
    Storage,
    /// The request or selected model is semantically invalid.
    InvalidRequest,
    /// A finite evaluator or service budget was exhausted.
    ResourceExhausted,
    /// A relationship condition could not be evaluated.
    Condition,
    /// The request deadline elapsed.
    Timeout,
    /// The request was cancelled.
    Cancelled,
    /// An invariant or spawned task failed internally.
    Internal,
}

#[derive(Debug, Error)]
enum ServiceErrorSource {
    #[error(transparent)]
    Check(CheckError),
    #[error(transparent)]
    Storage(StorageError),
}

/// One redacted service failure with a low-cardinality diagnostic code.
#[derive(Error)]
#[error("authorization service failed: {code}")]
pub struct ServiceError {
    kind: ServiceErrorKind,
    code: &'static str,
    #[source]
    source: Option<ServiceErrorSource>,
}

impl ServiceError {
    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> ServiceErrorKind {
        self.kind
    }

    /// Returns the stable, non-sensitive diagnostic code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub(crate) const fn unsupported_model_selection() -> Self {
        Self {
            kind: ServiceErrorKind::Internal,
            code: "unsupported_model_selection",
            source: None,
        }
    }
}

impl From<CheckError> for ServiceError {
    fn from(source: CheckError) -> Self {
        let kind = match source.kind() {
            CheckErrorKind::InvalidModel | CheckErrorKind::InvalidTuple => {
                ServiceErrorKind::InvalidRequest
            }
            CheckErrorKind::DepthExceeded
            | CheckErrorKind::DispatchExceeded
            | CheckErrorKind::DatastoreQueryExceeded
            | CheckErrorKind::TupleItemExceeded
            | CheckErrorKind::ConditionCostExceeded => ServiceErrorKind::ResourceExhausted,
            CheckErrorKind::Condition => ServiceErrorKind::Condition,
            CheckErrorKind::Storage => ServiceErrorKind::Storage,
            CheckErrorKind::Timeout => ServiceErrorKind::Timeout,
            CheckErrorKind::Cancelled => ServiceErrorKind::Cancelled,
            _ => ServiceErrorKind::Internal,
        };
        let code = source.code();
        Self {
            kind,
            code,
            source: Some(ServiceErrorSource::Check(source)),
        }
    }
}

impl From<StorageError> for ServiceError {
    fn from(source: StorageError) -> Self {
        let kind = match source.kind() {
            StorageErrorKind::NotFound => ServiceErrorKind::ModelNotFound,
            StorageErrorKind::Timeout => ServiceErrorKind::Timeout,
            StorageErrorKind::Cancelled => ServiceErrorKind::Cancelled,
            StorageErrorKind::ResourceExhausted => ServiceErrorKind::ResourceExhausted,
            StorageErrorKind::Integrity => ServiceErrorKind::Internal,
            _ => ServiceErrorKind::Storage,
        };
        let code = source.code();
        Self {
            kind,
            code,
            source: Some(ServiceErrorSource::Storage(source)),
        }
    }
}

impl fmt::Debug for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("source", &"[REDACTED]")
            .finish()
    }
}
