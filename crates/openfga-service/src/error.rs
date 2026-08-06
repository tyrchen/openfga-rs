//! Stable, redacted service-orchestration failures.

use std::fmt;

use openfga_check::{CheckError, CheckErrorKind};
use openfga_model::{ModelErrors, TupleValidationError, TupleValidationErrorKind};
use openfga_storage::{StorageError, StorageErrorKind};
use thiserror::Error;

use crate::{IdentifierSourceError, IdentifierSourceErrorKind};

/// Stable failure category exposed by transport-neutral services.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceErrorKind {
    /// The selected store does not exist.
    StoreNotFound,
    /// The selected immutable authorization model does not exist.
    ModelNotFound,
    /// The requested immutable identity already exists.
    AlreadyExists,
    /// Existing state conflicts with the requested atomic operation.
    Conflict,
    /// The supplied continuation state is malformed or out of scope.
    InvalidContinuation,
    /// The request or selected model is semantically invalid.
    InvalidRequest,
    /// A finite evaluator or service budget was exhausted.
    ResourceExhausted,
    /// A relationship condition could not be evaluated.
    Condition,
    /// A required backend or service actor is temporarily unavailable.
    Unavailable,
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
    #[error(transparent)]
    Model(ModelErrors),
    #[error(transparent)]
    TupleValidation(TupleValidationError),
    #[error(transparent)]
    Identifier(IdentifierSourceError),
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

    pub(crate) fn store_storage(source: StorageError) -> Self {
        if source.kind() == StorageErrorKind::NotFound {
            Self::from_source(
                ServiceErrorKind::StoreNotFound,
                "store_not_found",
                ServiceErrorSource::Storage(source),
            )
        } else {
            source.into()
        }
    }

    pub(crate) fn model_storage(source: StorageError) -> Self {
        if source.kind() == StorageErrorKind::NotFound {
            Self::from_source(
                ServiceErrorKind::ModelNotFound,
                "authorization_model_not_found",
                ServiceErrorSource::Storage(source),
            )
        } else {
            source.into()
        }
    }

    pub(crate) const fn invalid_request(code: &'static str) -> Self {
        Self {
            kind: ServiceErrorKind::InvalidRequest,
            code,
            source: None,
        }
    }

    pub(crate) const fn resource_exhausted(code: &'static str) -> Self {
        Self {
            kind: ServiceErrorKind::ResourceExhausted,
            code,
            source: None,
        }
    }

    fn from_source(kind: ServiceErrorKind, code: &'static str, source: ServiceErrorSource) -> Self {
        Self {
            kind,
            code,
            source: Some(source),
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
            CheckErrorKind::StorageUnavailable => ServiceErrorKind::Unavailable,
            CheckErrorKind::Timeout => ServiceErrorKind::Timeout,
            CheckErrorKind::Cancelled => ServiceErrorKind::Cancelled,
            CheckErrorKind::Internal => ServiceErrorKind::Internal,
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
            StorageErrorKind::AlreadyExists => ServiceErrorKind::AlreadyExists,
            StorageErrorKind::Conflict => ServiceErrorKind::Conflict,
            StorageErrorKind::InvalidContinuation => ServiceErrorKind::InvalidContinuation,
            StorageErrorKind::Timeout => ServiceErrorKind::Timeout,
            StorageErrorKind::Cancelled => ServiceErrorKind::Cancelled,
            StorageErrorKind::Unavailable => ServiceErrorKind::Unavailable,
            StorageErrorKind::ResourceExhausted => ServiceErrorKind::ResourceExhausted,
            StorageErrorKind::NotFound
            | StorageErrorKind::Integrity
            | StorageErrorKind::Internal => ServiceErrorKind::Internal,
        };
        let code = source.code();
        Self {
            kind,
            code,
            source: Some(ServiceErrorSource::Storage(source)),
        }
    }
}

impl From<ModelErrors> for ServiceError {
    fn from(source: ModelErrors) -> Self {
        Self::from_source(
            ServiceErrorKind::InvalidRequest,
            "invalid_authorization_model",
            ServiceErrorSource::Model(source),
        )
    }
}

impl From<TupleValidationError> for ServiceError {
    fn from(source: TupleValidationError) -> Self {
        let kind = match source.kind() {
            TupleValidationErrorKind::Query | TupleValidationErrorKind::Relationship => {
                ServiceErrorKind::InvalidRequest
            }
            TupleValidationErrorKind::CompiledModel => ServiceErrorKind::Internal,
        };
        let code = source.code();
        Self::from_source(kind, code, ServiceErrorSource::TupleValidation(source))
    }
}

impl From<IdentifierSourceError> for ServiceError {
    fn from(source: IdentifierSourceError) -> Self {
        let kind = match source.kind() {
            IdentifierSourceErrorKind::Cancelled => ServiceErrorKind::Cancelled,
            IdentifierSourceErrorKind::Timeout => ServiceErrorKind::Timeout,
            IdentifierSourceErrorKind::Unavailable => ServiceErrorKind::Unavailable,
            IdentifierSourceErrorKind::Exhausted => ServiceErrorKind::ResourceExhausted,
            IdentifierSourceErrorKind::Entropy | IdentifierSourceErrorKind::Internal => {
                ServiceErrorKind::Internal
            }
        };
        let code = source.code();
        Self::from_source(kind, code, ServiceErrorSource::Identifier(source))
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

#[cfg(test)]
mod tests {
    use std::io;

    use openfga_storage::{StorageError, StorageErrorKind};

    use super::{ServiceError, ServiceErrorKind};

    #[test]
    fn test_should_map_every_storage_category_without_collapsing_public_conflicts() {
        let cases = [
            (StorageErrorKind::NotFound, ServiceErrorKind::Internal),
            (
                StorageErrorKind::AlreadyExists,
                ServiceErrorKind::AlreadyExists,
            ),
            (StorageErrorKind::Conflict, ServiceErrorKind::Conflict),
            (
                StorageErrorKind::InvalidContinuation,
                ServiceErrorKind::InvalidContinuation,
            ),
            (StorageErrorKind::Timeout, ServiceErrorKind::Timeout),
            (StorageErrorKind::Cancelled, ServiceErrorKind::Cancelled),
            (StorageErrorKind::Unavailable, ServiceErrorKind::Unavailable),
            (StorageErrorKind::Integrity, ServiceErrorKind::Internal),
            (
                StorageErrorKind::ResourceExhausted,
                ServiceErrorKind::ResourceExhausted,
            ),
            (StorageErrorKind::Internal, ServiceErrorKind::Internal),
        ];
        for (storage, expected) in cases {
            let mapped = ServiceError::from(StorageError::new(storage, "test_storage_error"));
            assert_eq!(mapped.kind(), expected);
        }
    }

    #[test]
    fn test_should_contextualize_not_found_and_redact_source_details() {
        let store = ServiceError::store_storage(StorageError::new(
            StorageErrorKind::NotFound,
            "record_not_found",
        ));
        let model = ServiceError::model_storage(StorageError::new(
            StorageErrorKind::NotFound,
            "record_not_found",
        ));
        assert_eq!(store.kind(), ServiceErrorKind::StoreNotFound);
        assert_eq!(model.kind(), ServiceErrorKind::ModelNotFound);

        let error = ServiceError::from(StorageError::with_source(
            StorageErrorKind::Internal,
            "storage_failed",
            io::Error::other("secret backend detail"),
        ));
        let debug = format!("{error:?}");
        assert!(!debug.contains("secret backend detail"));
        assert!(debug.contains("[REDACTED]"));
    }
}
