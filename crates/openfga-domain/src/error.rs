//! Safe, structured errors shared by domain constructors and commands.

use thiserror::Error;

/// The grammar failure category reported by a domain parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ParseKind {
    /// The input was empty.
    Empty,
    /// A required separator was absent.
    MissingSeparator,
    /// A separator appeared in an invalid position or more than once.
    UnexpectedSeparator,
    /// A character is outside the field's allowlist.
    InvalidCharacter,
    /// The input exceeded its byte limit.
    TooLong,
    /// The input did not have its required fixed byte length.
    InvalidLength,
    /// The input is syntactically valid but not in canonical form.
    NonCanonical,
}

/// A bounded parser error that never echoes the hostile input.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid {field} at byte offset {offset}: {kind:?}")]
#[non_exhaustive]
pub struct ParseError {
    field: &'static str,
    offset: usize,
    kind: ParseKind,
}

impl ParseError {
    /// Creates a safe parser error.
    #[must_use]
    pub const fn new(field: &'static str, offset: usize, kind: ParseKind) -> Self {
        Self {
            field,
            offset,
            kind,
        }
    }

    /// Returns the semantic field being parsed.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }

    /// Returns the byte offset at which parsing failed.
    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the grammar failure category.
    #[must_use]
    pub const fn kind(&self) -> ParseKind {
        self.kind
    }
}

/// The invariant violated by an otherwise well-formed domain value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ValidationReason {
    /// A required value was absent.
    Missing,
    /// A value was outside its permitted numeric range.
    OutOfRange,
    /// A string or encoded value exceeded its byte limit.
    TooLarge,
    /// A collection exceeded its item limit.
    TooManyItems,
    /// A nested value exceeded its depth limit.
    TooDeep,
    /// A value appeared more than once where uniqueness is required.
    Duplicate,
    /// Two individually valid values conflict.
    Conflicting,
    /// Cross-field state is inconsistent.
    Inconsistent,
    /// A time-bounded value has expired.
    Expired,
    /// Integrity or authentication validation failed.
    Integrity,
}

/// A safe validation error that identifies only the field and invariant.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid {field}: {reason:?}")]
#[non_exhaustive]
pub struct ValidationError {
    field: &'static str,
    reason: ValidationReason,
}

impl ValidationError {
    /// Creates a validation error without retaining the rejected value.
    #[must_use]
    pub const fn new(field: &'static str, reason: ValidationReason) -> Self {
        Self { field, reason }
    }

    /// Returns the semantic field that failed validation.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }

    /// Returns the failed invariant category.
    #[must_use]
    pub const fn reason(&self) -> ValidationReason {
        self.reason
    }
}

/// A stable, non-sensitive subsystem failure code.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("{code}")]
#[non_exhaustive]
pub struct SubsystemError {
    code: &'static str,
}

impl SubsystemError {
    /// Creates a subsystem error from a stable static code.
    #[must_use]
    pub const fn new(code: &'static str) -> Self {
        Self { code }
    }

    /// Returns the stable subsystem failure code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

/// A finite resource whose configured budget may be exhausted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResourceKind {
    /// Input or response bytes.
    Bytes,
    /// Collection items.
    Items,
    /// Nested input or evaluation depth.
    Depth,
    /// Evaluator dispatches.
    Dispatches,
    /// Datastore queries.
    DatastoreQueries,
    /// Datastore tuple items.
    TupleItems,
    /// Condition evaluation cost.
    ConditionCost,
    /// Concurrent work.
    Concurrency,
    /// Request time.
    Deadline,
}

/// Shared semantic error classes used across transport-independent APIs.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DomainError {
    /// A wire or textual field failed grammar validation.
    #[error(transparent)]
    Parse(#[from] ParseError),
    /// Cross-field or non-grammar validation failed.
    #[error(transparent)]
    Validation(#[from] ValidationError),
    /// Authorization-model compilation or resolution failed.
    #[error("model error: {0}")]
    Model(SubsystemError),
    /// Condition compilation or evaluation failed.
    #[error("condition error: {0}")]
    Condition(SubsystemError),
    /// A storage operation failed.
    #[error("storage error: {0}")]
    Storage(SubsystemError),
    /// A finite request budget was exhausted.
    #[error("resource exhausted: {resource:?} limit {limit}")]
    ResourceExhausted {
        /// The exhausted budget class.
        resource: ResourceKind,
        /// The configured finite limit.
        limit: u64,
    },
    /// The caller or server cancelled the operation.
    #[error("operation cancelled")]
    Cancelled,
    /// An internal invariant failed; the code is safe for diagnostics.
    #[error("internal error: {code}")]
    Internal {
        /// Stable, non-sensitive incident code.
        code: &'static str,
    },
}
