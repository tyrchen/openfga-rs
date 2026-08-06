//! Typed, redacted condition compilation and evaluation failures.

use openfga_domain::ParameterName;
use thiserror::Error;

/// Stable category for a rejected persisted condition-context entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConditionContextErrorKind {
    /// The context key is not declared by the condition.
    UnknownParameter,
    /// The context value cannot be converted to its declared condition type.
    InvalidParameter,
}

/// One safely bounded condition-context diagnostic.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("condition context parameter is invalid: {kind:?}")]
#[non_exhaustive]
pub struct ConditionContextError {
    kind: ConditionContextErrorKind,
    parameter: ParameterName,
    expected_type: Option<&'static str>,
    found_type: Option<&'static str>,
}

impl ConditionContextError {
    pub(crate) const fn unknown(parameter: ParameterName) -> Self {
        Self {
            kind: ConditionContextErrorKind::UnknownParameter,
            parameter,
            expected_type: None,
            found_type: None,
        }
    }

    pub(crate) const fn invalid(
        parameter: ParameterName,
        expected_type: &'static str,
        found_type: &'static str,
    ) -> Self {
        Self {
            kind: ConditionContextErrorKind::InvalidParameter,
            parameter,
            expected_type: Some(expected_type),
            found_type: Some(found_type),
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> ConditionContextErrorKind {
        self.kind
    }

    /// Returns the validated offending parameter name.
    #[must_use]
    pub const fn parameter(&self) -> &ParameterName {
        &self.parameter
    }

    /// Returns the bounded declared type name for an incompatible parameter.
    #[must_use]
    pub const fn expected_type(&self) -> Option<&'static str> {
        self.expected_type
    }

    /// Returns the bounded runtime type name for an incompatible parameter.
    #[must_use]
    pub const fn found_type(&self) -> Option<&'static str> {
        self.found_type
    }
}

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

/// Safely bounded detail for a non-syntax CEL compilation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompileErrorDetail {
    /// One undeclared CEL identifier.
    UnknownIdentifier(Box<str>),
    /// A function or operator has no overload for the static argument types.
    NoMatchingOverload {
        /// Canonical CEL function/operator name.
        function: Box<str>,
        /// Safe canonical static argument types.
        argument_types: Box<[&'static str]>,
    },
}

/// A source-redacted condition compilation failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("condition compilation failed at byte {offset}: {kind:?}")]
#[non_exhaustive]
pub struct CompileError {
    kind: CompileErrorKind,
    offset: usize,
    found_type: Option<&'static str>,
    detail: Option<Box<CompileErrorDetail>>,
}

impl CompileError {
    pub(crate) const fn new(kind: CompileErrorKind, offset: usize) -> Self {
        Self {
            kind,
            offset,
            found_type: None,
            detail: None,
        }
    }

    pub(crate) const fn non_boolean(found_type: &'static str) -> Self {
        Self {
            kind: CompileErrorKind::NonBooleanResult,
            offset: 0,
            found_type: Some(found_type),
            detail: None,
        }
    }

    pub(crate) fn unknown_identifier(identifier: &str) -> Self {
        Self {
            kind: CompileErrorKind::UnknownIdentifier,
            offset: 0,
            found_type: None,
            detail: Some(Box::new(CompileErrorDetail::UnknownIdentifier(
                identifier.into(),
            ))),
        }
    }

    pub(crate) fn no_matching_overload(function: &str, argument_types: Vec<&'static str>) -> Self {
        Self {
            kind: CompileErrorKind::TypeMismatch,
            offset: 0,
            found_type: None,
            detail: Some(Box::new(CompileErrorDetail::NoMatchingOverload {
                function: function.into(),
                argument_types: argument_types.into(),
            })),
        }
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

    /// Returns the safe static result type for a non-Boolean expression.
    #[must_use]
    pub const fn found_type(&self) -> Option<&'static str> {
        self.found_type
    }

    /// Returns bounded structured diagnostic detail when available.
    #[must_use]
    pub fn detail(&self) -> Option<&CompileErrorDetail> {
        self.detail.as_deref()
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
