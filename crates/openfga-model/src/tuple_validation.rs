//! Model-aware validation shared by query and relationship-tuple use cases.

use std::fmt;

use openfga_domain::{ConditionReference, RelationshipTuple, SubjectRef, TupleKey};
use thiserror::Error;

use crate::{
    CompiledModel, ConditionRequirement, DirectRestriction, ModelLookupError, RelationId,
    RestrictionKind,
};

/// Stable model-aware tuple validation category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TupleValidationErrorKind {
    /// The authorization question references declarations absent from the model.
    Query,
    /// A persisted or contextual relationship is not directly assignable.
    Relationship,
    /// Compiled model state violates an internal invariant.
    CompiledModel,
}

/// Redacted model-aware tuple validation failure.
#[derive(Error)]
#[error("tuple validation failed: {code}")]
pub struct TupleValidationError {
    kind: TupleValidationErrorKind,
    code: &'static str,
    #[source]
    source: Option<ModelLookupError>,
}

impl TupleValidationError {
    const fn new(kind: TupleValidationErrorKind, code: &'static str) -> Self {
        Self {
            kind,
            code,
            source: None,
        }
    }

    const fn lookup(
        kind: TupleValidationErrorKind,
        code: &'static str,
        source: ModelLookupError,
    ) -> Self {
        Self {
            kind,
            code,
            source: Some(source),
        }
    }

    /// Returns the stable validation category.
    #[must_use]
    pub const fn kind(&self) -> TupleValidationErrorKind {
        self.kind
    }

    /// Returns the low-cardinality, non-sensitive diagnostic code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Debug for TupleValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TupleValidationError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl CompiledModel {
    /// Validates an authorization question and returns its resolved relation.
    ///
    /// # Errors
    ///
    /// Returns a query error when the object relation, subject type, or userset
    /// relation is absent from this model.
    pub fn validate_query_tuple(
        &self,
        tuple: &TupleKey,
    ) -> Result<RelationId, TupleValidationError> {
        let relation = self
            .relation_id(tuple.object().object_type(), tuple.relation())
            .map_err(|source| {
                TupleValidationError::lookup(
                    TupleValidationErrorKind::Query,
                    "query_relation_missing",
                    source,
                )
            })?;
        self.validate_subject(tuple.subject())?;
        Ok(relation)
    }

    /// Validates that a contextual relationship tuple is directly assignable in this model.
    ///
    /// # Errors
    ///
    /// Returns a relationship error for absent declarations, subject-shape
    /// mismatches, or condition-requirement mismatches.
    pub fn validate_relationship_tuple(
        &self,
        tuple: &RelationshipTuple,
    ) -> Result<(), TupleValidationError> {
        let relation_id = self
            .relation_id(tuple.key().object().object_type(), tuple.key().relation())
            .map_err(|source| {
                TupleValidationError::lookup(
                    TupleValidationErrorKind::Relationship,
                    "relationship_relation_missing",
                    source,
                )
            })?;
        let relation = self.relation(relation_id).map_err(|source| {
            TupleValidationError::lookup(
                TupleValidationErrorKind::CompiledModel,
                "relationship_relation_invalid",
                source,
            )
        })?;
        if self.matches_restriction(tuple, relation.restrictions())? {
            Ok(())
        } else {
            Err(TupleValidationError::new(
                TupleValidationErrorKind::Relationship,
                "relationship_tuple_not_permitted",
            ))
        }
    }

    /// Validates a relationship tuple for durable persistence.
    ///
    /// Unlike request-only contextual tuples, a durable tuple cannot encode an
    /// implicit self-referential userset relationship.
    ///
    /// # Errors
    ///
    /// Returns a relationship error for an implicit or non-assignable tuple.
    pub fn validate_persistent_tuple(
        &self,
        tuple: &RelationshipTuple,
    ) -> Result<(), TupleValidationError> {
        if is_implicit(tuple.key()) {
            return Err(TupleValidationError::new(
                TupleValidationErrorKind::Relationship,
                "relationship_tuple_implicit",
            ));
        }
        self.validate_relationship_tuple(tuple)
    }

    fn validate_subject(&self, subject: &SubjectRef) -> Result<(), TupleValidationError> {
        self.type_id(subject.subject_type()).map_err(|source| {
            TupleValidationError::lookup(
                TupleValidationErrorKind::Query,
                "query_subject_type_missing",
                source,
            )
        })?;
        if let SubjectRef::Userset(userset) = subject {
            self.relation_id(userset.object().object_type(), userset.relation())
                .map_err(|source| {
                    TupleValidationError::lookup(
                        TupleValidationErrorKind::Query,
                        "query_userset_relation_missing",
                        source,
                    )
                })?;
        }
        Ok(())
    }

    fn matches_restriction(
        &self,
        tuple: &RelationshipTuple,
        restrictions: &[DirectRestriction],
    ) -> Result<bool, TupleValidationError> {
        let Ok(subject_type) = self.type_id(tuple.key().subject().subject_type()) else {
            return Ok(false);
        };
        let class = match tuple.key().subject() {
            SubjectRef::Object(_) => DirectClass::Object,
            SubjectRef::TypedWildcard(_) => DirectClass::Wildcard,
            SubjectRef::Userset(userset) => {
                let Ok(relation) =
                    self.relation_id(userset.object().object_type(), userset.relation())
                else {
                    return Ok(false);
                };
                DirectClass::Userset(relation)
            }
            _ => {
                return Err(TupleValidationError::new(
                    TupleValidationErrorKind::CompiledModel,
                    "subject_reference_unsupported",
                ));
            }
        };
        for restriction in restrictions {
            if restriction.subject_type() == subject_type
                && restriction_kind_matches(restriction.kind(), class)
                && self.condition_requirement_matches(restriction.condition(), tuple.condition())?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn condition_requirement_matches(
        &self,
        requirement: ConditionRequirement,
        reference: &ConditionReference,
    ) -> Result<bool, TupleValidationError> {
        match (requirement, reference) {
            (ConditionRequirement::Unconditional, ConditionReference::Unconditional) => Ok(true),
            (
                ConditionRequirement::Required(condition_id),
                ConditionReference::Conditional(binding),
            ) => self
                .condition(condition_id)
                .map(|condition| condition.name() == binding.name())
                .map_err(|source| {
                    TupleValidationError::lookup(
                        TupleValidationErrorKind::CompiledModel,
                        "restriction_condition_invalid",
                        source,
                    )
                }),
            _ => Ok(false),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectClass {
    Object,
    Userset(RelationId),
    Wildcard,
}

fn restriction_kind_matches(kind: RestrictionKind, class: DirectClass) -> bool {
    matches!(
        (kind, class),
        (RestrictionKind::Object, DirectClass::Object)
            | (RestrictionKind::Wildcard, DirectClass::Wildcard)
            | (RestrictionKind::Userset(_), DirectClass::Userset(_))
    ) && match (kind, class) {
        (RestrictionKind::Userset(expected), DirectClass::Userset(actual)) => expected == actual,
        _ => true,
    }
}

fn is_implicit(tuple: &TupleKey) -> bool {
    matches!(
        tuple.subject(),
        SubjectRef::Userset(userset)
            if userset.object() == tuple.object() && userset.relation() == tuple.relation()
    )
}
