//! Typed, redacted condition compilation and evaluation failures.

use thiserror::Error;

/// Stable category for a rejected condition expression.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompileErrorKind {
    /// The CEL syntax is invalid.
    Syntax,
    /// A configured structural limit was exceeded.
    LimitExceeded,
    /// An identifier was not declared in the current scope.
    UnknownIdentifier,
    /// A function or syntax form is outside the supported `OpenFGA` surface.
    Unsupported,
    /// Static types are incompatible.
    TypeMismatch,
    /// The top-level result is not Boolean.
    NonBooleanResult,
}

/// A source-redacted condition compilation failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("condition compilation failed at byte {offset}: {kind:?}")]
#[non_exhaustive]
pub struct CompileError {
    kind: CompileErrorKind,
    offset: usize,
}

impl CompileError {
    pub(crate) const fn new(kind: CompileErrorKind, offset: usize) -> Self {
        Self { kind, offset }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> CompileErrorKind {
        self.kind
    }

    /// Returns a bounded source byte offset, or zero when unavailable.
    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }
}

/// Stable category for a condition runtime failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EvaluationErrorKind {
    /// One or more declared parameters are absent.
    MissingParameters,
    /// A supplied parameter cannot be converted without loss.
    InvalidParameter,
    /// A runtime operation received incompatible values.
    TypeMismatch,
    /// A checked arithmetic operation overflowed or divided by zero.
    Arithmetic,
    /// A helper argument, such as an IP address or timestamp, is invalid.
    InvalidValue,
    /// A runtime string, byte string, or collection would exceed its configured ceiling.
    ValueLimitExceeded,
    /// The deterministic operation budget was exhausted.
    CostExceeded,
    /// Evaluation was explicitly cancelled.
    Cancelled,
    /// Compiled state was internally inconsistent.
    InvalidCompiledState,
}

/// A context-redacted condition evaluation failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("condition evaluation failed: {kind:?}")]
#[non_exhaustive]
pub struct EvaluationError {
    kind: EvaluationErrorKind,
    missing_parameter_count: usize,
}

impl EvaluationError {
    pub(crate) const fn new(kind: EvaluationErrorKind) -> Self {
        Self {
            kind,
            missing_parameter_count: 0,
        }
    }

    pub(crate) const fn missing(count: usize) -> Self {
        Self {
            kind: EvaluationErrorKind::MissingParameters,
            missing_parameter_count: count,
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> EvaluationErrorKind {
        self.kind
    }

    /// Returns the number of absent parameters without exposing their names.
    #[must_use]
    pub const fn missing_parameter_count(&self) -> usize {
        self.missing_parameter_count
    }
}
