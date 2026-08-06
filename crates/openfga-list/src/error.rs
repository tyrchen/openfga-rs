//! Stable redacted enumeration failures.

use std::fmt;

use openfga_check::{CheckError, CheckErrorKind};
use openfga_model::{ModelLookupError, TupleValidationError};
use openfga_storage::{StorageError, StorageErrorKind};
use thiserror::Error;

/// Stable reverse-enumeration failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListErrorKind {
    /// The selected model does not own or describe the query.
    InvalidModel,
    /// A contextual relationship tuple is invalid for the model.
    InvalidTuple,
    /// A derived path exceeded its semantic depth ceiling.
    DepthExceeded,
    /// The graph work ceiling was exhausted.
    DispatchExceeded,
    /// The reverse datastore-query ceiling was exhausted.
    DatastoreQueryExceeded,
    /// The tuple-item ceiling was exhausted.
    TupleItemExceeded,
    /// The distinct intermediate-candidate ceiling was exhausted.
    CandidateExceeded,
    /// Tuple storage is temporarily unavailable.
    StorageUnavailable,
    /// The request deadline elapsed.
    Timeout,
    /// The request was cancelled.
    Cancelled,
    /// An invariant or backend integrity contract failed.
    Internal,
}

#[derive(Debug, Error)]
enum ListErrorSource {
    #[error(transparent)]
    Check(CheckError),
    #[error(transparent)]
    Storage(StorageError),
    #[error(transparent)]
    Model(ModelLookupError),
    #[error(transparent)]
    TupleValidation(TupleValidationError),
}

/// One typed reverse-enumeration failure with a stable low-cardinality code.
#[derive(Error)]
#[error("authorization enumeration failed: {code}")]
pub struct ListError {
    kind: ListErrorKind,
    code: &'static str,
    #[source]
    source: Option<ListErrorSource>,
}

impl ListError {
    pub(crate) const fn new(kind: ListErrorKind, code: &'static str) -> Self {
        Self {
            kind,
            code,
            source: None,
        }
    }

    pub(crate) const fn model(code: &'static str, source: ModelLookupError) -> Self {
        Self {
            kind: ListErrorKind::InvalidModel,
            code,
            source: Some(ListErrorSource::Model(source)),
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> ListErrorKind {
        self.kind
    }

    /// Returns the stable low-cardinality diagnostic code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl From<StorageError> for ListError {
    fn from(source: StorageError) -> Self {
        let kind = match source.kind() {
            StorageErrorKind::Cancelled => ListErrorKind::Cancelled,
            StorageErrorKind::Timeout => ListErrorKind::Timeout,
            StorageErrorKind::Unavailable => ListErrorKind::StorageUnavailable,
            StorageErrorKind::ResourceExhausted => ListErrorKind::TupleItemExceeded,
            StorageErrorKind::NotFound
            | StorageErrorKind::AlreadyExists
            | StorageErrorKind::Conflict
            | StorageErrorKind::InvalidContinuation
            | StorageErrorKind::Integrity
            | StorageErrorKind::Internal => ListErrorKind::Internal,
        };
        Self {
            kind,
            code: "tuple_storage_failed",
            source: Some(ListErrorSource::Storage(source)),
        }
    }
}

impl From<TupleValidationError> for ListError {
    fn from(source: TupleValidationError) -> Self {
        Self {
            kind: ListErrorKind::InvalidTuple,
            code: source.code(),
            source: Some(ListErrorSource::TupleValidation(source)),
        }
    }
}

impl From<CheckError> for ListError {
    fn from(source: CheckError) -> Self {
        let kind = match source.kind() {
            CheckErrorKind::InvalidModel => ListErrorKind::InvalidModel,
            CheckErrorKind::InvalidTuple | CheckErrorKind::Condition => ListErrorKind::InvalidTuple,
            CheckErrorKind::DepthExceeded => ListErrorKind::DepthExceeded,
            CheckErrorKind::DispatchExceeded => ListErrorKind::DispatchExceeded,
            CheckErrorKind::DatastoreQueryExceeded => ListErrorKind::DatastoreQueryExceeded,
            CheckErrorKind::TupleItemExceeded | CheckErrorKind::ConditionCostExceeded => {
                ListErrorKind::TupleItemExceeded
            }
            CheckErrorKind::StorageUnavailable => ListErrorKind::StorageUnavailable,
            CheckErrorKind::Timeout => ListErrorKind::Timeout,
            CheckErrorKind::Cancelled => ListErrorKind::Cancelled,
            CheckErrorKind::Internal => ListErrorKind::Internal,
        };
        let code = source.code();
        Self {
            kind,
            code,
            source: Some(ListErrorSource::Check(source)),
        }
    }
}

impl fmt::Debug for ListError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}
