//! Stable, source-ordered, non-sensitive model diagnostics.

use thiserror::Error;

/// A stable declaration location that never copies hostile source text.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum DeclarationPath {
    /// Whole-model declaration.
    Model,
    /// Type definition by source index.
    Type {
        /// Zero-based type declaration index.
        index: u32,
    },
    /// Relation by type and relation source indices.
    Relation {
        /// Zero-based type declaration index.
        type_index: u32,
        /// Zero-based relation declaration index within the type.
        relation_index: u32,
    },
    /// Direct restriction by enclosing declaration indices.
    Restriction {
        /// Zero-based type declaration index.
        type_index: u32,
        /// Zero-based relation declaration index within the type.
        relation_index: u32,
        /// Zero-based restriction index within the relation.
        restriction_index: u32,
    },
    /// Rewrite node by enclosing declaration and preorder node indices.
    Rewrite {
        /// Zero-based type declaration index.
        type_index: u32,
        /// Zero-based relation declaration index within the type.
        relation_index: u32,
        /// Zero-based preorder rewrite-node index.
        node_index: u32,
    },
    /// Condition by source index.
    Condition {
        /// Zero-based condition declaration index.
        index: u32,
    },
    /// Condition parameter by source indices.
    Parameter {
        /// Zero-based condition declaration index.
        condition_index: u32,
        /// Zero-based parameter declaration index within the condition.
        parameter_index: u32,
    },
}

/// Stable authorization-model validation failure category.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ModelErrorCode {
    /// Additional independent diagnostics exceeded the configured error cap.
    TooManyModelErrors,
    /// Schema version is not the pinned supported version.
    InvalidSchemaVersion,
    /// Type-definition count is outside configured bounds.
    InvalidTypeDefinitionCount,
    /// Condition-definition count exceeds its configured bound.
    TooManyConditions,
    /// Total or per-type relation count exceeds its configured bound.
    TooManyRelations,
    /// Rewrite node, depth, or operand count exceeds its configured bound.
    RewriteLimitExceeded,
    /// Graph edge count exceeds its configured bound.
    GraphLimitExceeded,
    /// A type name failed boundary grammar validation.
    InvalidTypeName,
    /// A relation name failed boundary grammar validation.
    InvalidRelationName,
    /// A condition name failed boundary grammar validation.
    InvalidConditionName,
    /// A parameter name failed boundary grammar validation.
    InvalidParameterName,
    /// A reserved model keyword was used as a declaration name.
    ReservedName,
    /// A type definition is duplicated.
    DuplicateType,
    /// A relation is duplicated within one type.
    DuplicateRelation,
    /// A condition key is duplicated.
    DuplicateCondition,
    /// A condition map key differs from its declared name.
    ConditionNameMismatch,
    /// A relation metadata declaration has no relation.
    OrphanRelationMetadata,
    /// A rewrite is absent or structurally malformed.
    InvalidRewrite,
    /// A union or intersection has fewer than two operands.
    InvalidOperatorArity,
    /// A rewrite references an undefined relation.
    UndefinedRelation,
    /// A direct restriction references an undefined type.
    UndefinedType,
    /// A direct restriction references an undefined condition.
    UndefinedCondition,
    /// An assignable relation has no restrictions.
    AssignableWithoutRestrictions,
    /// A non-assignable relation declares restrictions.
    NonAssignableWithRestrictions,
    /// A restriction form is invalid for its relation.
    InvalidRestriction,
    /// A computed rewrite directly references its enclosing relation.
    IllegalSelfReference,
    /// A tupleset relation is not a direct-only relation.
    InvalidTuplesetRelation,
    /// A TTU computed relation is absent from at least one permitted target type.
    InvalidTupleToUsersetTarget,
    /// A relation has no path to a concrete entrypoint.
    NoEntrypoints,
    /// Computed-userset rewrites form a forbidden cycle.
    ForbiddenComputedCycle,
    /// A condition parameter type is malformed or unsupported.
    InvalidConditionParameterType,
    /// A condition expression failed bounded compilation.
    InvalidCondition,
    /// An authorization-model identifier failed boundary validation.
    InvalidModelIdentifier,
}

/// One safe, structured model diagnostic.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("authorization model error {code:?} at {path:?}")]
#[non_exhaustive]
pub struct ModelError {
    code: ModelErrorCode,
    path: DeclarationPath,
}

impl ModelError {
    /// Creates a diagnostic from a stable code and declaration path.
    #[must_use]
    pub const fn new(code: ModelErrorCode, path: DeclarationPath) -> Self {
        Self { code, path }
    }

    /// Returns the stable failure code.
    #[must_use]
    pub const fn code(&self) -> ModelErrorCode {
        self.code
    }

    /// Returns the source declaration path.
    #[must_use]
    pub const fn path(&self) -> DeclarationPath {
        self.path
    }
}

/// Deterministically sorted model diagnostics, optionally truncated at a configured cap.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("authorization model compilation failed with {count} diagnostic(s)", count = .errors.len())]
#[non_exhaustive]
pub struct ModelErrors {
    errors: Vec<ModelError>,
    truncated: bool,
}

impl ModelErrors {
    pub(crate) fn from_unsorted(mut errors: Vec<ModelError>, truncated: bool) -> Self {
        errors.sort_by_key(|error| (error.path(), error.code()));
        errors.dedup();
        Self { errors, truncated }
    }

    /// Returns diagnostics in deterministic declaration/code order.
    #[must_use]
    pub fn errors(&self) -> &[ModelError] {
        &self.errors
    }

    /// Returns whether additional diagnostics were omitted at the configured cap.
    #[must_use]
    pub const fn is_truncated(&self) -> bool {
        self.truncated
    }
}

/// Failure to resolve a dense identifier or declared model symbol.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelLookupError {
    /// No declaration matches the requested symbol.
    #[error("model declaration not found")]
    NotFound,
    /// A dense identifier does not belong to this compiled model.
    #[error("compiled model identifier is out of range")]
    InvalidIdentifier,
}

#[derive(Debug)]
pub(crate) struct ErrorCollector {
    errors: Vec<ModelError>,
    maximum: usize,
    truncated: bool,
}

impl ErrorCollector {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            errors: Vec::with_capacity(maximum.min(32)),
            maximum,
            truncated: false,
        }
    }

    pub(crate) fn push(&mut self, code: ModelErrorCode, path: DeclarationPath) {
        if self.errors.len() < self.maximum {
            self.errors.push(ModelError::new(code, path));
        } else {
            self.truncated = true;
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    pub(crate) fn finish(mut self) -> ModelErrors {
        if self.truncated {
            self.errors.push(ModelError::new(
                ModelErrorCode::TooManyModelErrors,
                DeclarationPath::Model,
            ));
        }
        ModelErrors::from_unsorted(self.errors, self.truncated)
    }
}
