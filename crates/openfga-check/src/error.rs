//! Stable redacted evaluator failures.

use std::fmt;

use openfga_condition::EvaluationError;
use openfga_model::ModelLookupError;
use openfga_storage::{StorageError, StorageErrorKind};
use thiserror::Error;

/// Stable authorization-evaluator error category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckErrorKind {
    /// The supplied model does not own or describe the query.
    InvalidModel,
    /// A root or contextual tuple is not valid for the compiled model.
    InvalidTuple,
    /// The branch-depth ceiling was exhausted.
    DepthExceeded,
    /// The work-dispatch ceiling was exhausted.
    DispatchExceeded,
    /// The datastore-query ceiling was exhausted.
    DatastoreQueryExceeded,
    /// The tuple-item ceiling was exhausted.
    TupleItemExceeded,
    /// The aggregate condition-cost ceiling was exhausted.
    ConditionCostExceeded,
    /// A relationship condition could not be evaluated.
    Condition,
    /// A storage operation failed.
    Storage,
    /// The request deadline elapsed.
    Timeout,
    /// The request was cancelled.
    Cancelled,
    /// An invariant, spawned task, or compiled state failed internally.
    Internal,
}

#[derive(Debug, Error)]
enum CheckErrorSource {
    #[error(transparent)]
    Storage(StorageError),
    #[error(transparent)]
    Condition(EvaluationError),
    #[error(transparent)]
    Model(ModelLookupError),
}

/// One redacted typed evaluator failure with an optional typed source chain.
#[derive(Error)]
#[error("authorization evaluation failed: {code}")]
pub struct CheckError {
    kind: CheckErrorKind,
    code: &'static str,
    #[source]
    source: Option<CheckErrorSource>,
}

impl CheckError {
    pub(crate) const fn new(kind: CheckErrorKind, code: &'static str) -> Self {
        Self {
            kind,
            code,
            source: None,
        }
    }

    pub(crate) const fn model(code: &'static str, source: ModelLookupError) -> Self {
        Self {
            kind: CheckErrorKind::InvalidModel,
            code,
            source: Some(CheckErrorSource::Model(source)),
        }
    }

    pub(crate) fn condition(source: EvaluationError) -> Self {
        Self {
            kind: CheckErrorKind::Condition,
            code: "condition_evaluation_failed",
            source: Some(CheckErrorSource::Condition(source)),
        }
    }

    /// Returns the stable evaluator failure category.
    #[must_use]
    pub const fn kind(&self) -> CheckErrorKind {
        self.kind
    }

    /// Returns a stable low-cardinality diagnostic code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl From<StorageError> for CheckError {
    fn from(source: StorageError) -> Self {
        let kind = match source.kind() {
            StorageErrorKind::Cancelled => CheckErrorKind::Cancelled,
            StorageErrorKind::Timeout => CheckErrorKind::Timeout,
            _ => CheckErrorKind::Storage,
        };
        Self {
            kind,
            code: "tuple_storage_failed",
            source: Some(CheckErrorSource::Storage(source)),
        }
    }
}

impl fmt::Debug for CheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}
