//! Model-aware validation shared by query and relationship-tuple use cases.

use std::fmt;

use openfga_condition::ConditionContextError;
use openfga_domain::{
    ConditionBinding, ConditionReference, ParameterName, RelationshipTuple, SubjectRef, TupleKey,
};
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
    condition_context: Option<Box<ConditionContextError>>,
}

impl TupleValidationError {
    const fn new(kind: TupleValidationErrorKind, code: &'static str) -> Self {
        Self {
            kind,
            code,
            source: None,
            condition_context: None,
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
            condition_context: None,
        }
    }

    fn condition_context(source: ConditionContextError) -> Self {
        Self {
            kind: TupleValidationErrorKind::Relationship,
            code: "relationship_condition_context_invalid",
            source: None,
            condition_context: Some(Box::new(source)),
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

    /// Returns the safely bounded invalid condition parameter, when applicable.
    #[must_use]
    pub const fn parameter(&self) -> Option<&ParameterName> {
        match &self.condition_context {
            Some(source) => Some(source.parameter()),
            None => None,
        }
    }

    /// Returns the structured condition-context diagnostic, when applicable.
    #[must_use]
    pub fn condition_context_error(&self) -> Option<&ConditionContextError> {
        self.condition_context.as_deref()
    }
}

impl fmt::Debug for TupleValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TupleValidationError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("has_source", &self.source.is_some())
            .field(
                "condition_context",
                &self.condition_context.as_ref().map(|_| "[REDACTED]"),
            )
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
        self.type_id(tuple.object().object_type())
            .map_err(|source| {
                TupleValidationError::lookup(
                    TupleValidationErrorKind::Query,
                    "query_object_type_missing",
                    source,
                )
            })?;
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
        self.type_id(tuple.key().object().object_type())
            .map_err(|source| {
                TupleValidationError::lookup(
                    TupleValidationErrorKind::Relationship,
                    "relationship_object_type_missing",
                    source,
                )
            })?;
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
        self.type_id(tuple.key().subject().subject_type())
            .map_err(|source| {
                TupleValidationError::lookup(
                    TupleValidationErrorKind::Relationship,
                    "relationship_subject_type_missing",
                    source,
                )
            })?;
        if let SubjectRef::Userset(userset) = tuple.key().subject() {
            self.relation_id(userset.object().object_type(), userset.relation())
                .map_err(|source| {
                    TupleValidationError::lookup(
                        TupleValidationErrorKind::Relationship,
                        "relationship_userset_relation_missing",
                        source,
                    )
                })?;
        }
        self.validate_relationship_condition(tuple, relation.restrictions())
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

    fn validate_relationship_condition(
        &self,
        tuple: &RelationshipTuple,
        restrictions: &[DirectRestriction],
    ) -> Result<(), TupleValidationError> {
        let subject_type =
            self.type_id(tuple.key().subject().subject_type())
                .map_err(|source| {
                    TupleValidationError::lookup(
                        TupleValidationErrorKind::CompiledModel,
                        "relationship_subject_type_invalid",
                        source,
                    )
                })?;
        let class = self.relationship_direct_class(tuple.key().subject())?;
        let matching = restrictions
            .iter()
            .filter(|restriction| {
                restriction.subject_type() == subject_type
                    && restriction_kind_matches(restriction.kind(), class)
            })
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Err(TupleValidationError::new(
                TupleValidationErrorKind::Relationship,
                "relationship_tuple_not_permitted",
            ));
        }
        match tuple.condition() {
            ConditionReference::Unconditional => validate_unconditional(&matching),
            ConditionReference::Conditional(binding) => {
                self.validate_conditional(binding, &matching)
            }
            _ => Err(TupleValidationError::new(
                TupleValidationErrorKind::CompiledModel,
                "subject_condition_unsupported",
            )),
        }
    }

    fn relationship_direct_class(
        &self,
        subject: &SubjectRef,
    ) -> Result<DirectClass, TupleValidationError> {
        match subject {
            SubjectRef::Object(_) => Ok(DirectClass::Object),
            SubjectRef::TypedWildcard(_) => Ok(DirectClass::Wildcard),
            SubjectRef::Userset(userset) => self
                .relation_id(userset.object().object_type(), userset.relation())
                .map(DirectClass::Userset)
                .map_err(|source| {
                    TupleValidationError::lookup(
                        TupleValidationErrorKind::CompiledModel,
                        "relationship_userset_relation_invalid",
                        source,
                    )
                }),
            _ => Err(TupleValidationError::new(
                TupleValidationErrorKind::CompiledModel,
                "subject_reference_unsupported",
            )),
        }
    }

    fn validate_conditional(
        &self,
        binding: &ConditionBinding,
        restrictions: &[&DirectRestriction],
    ) -> Result<(), TupleValidationError> {
        self.condition_id(binding.name()).map_err(|source| {
            TupleValidationError::lookup(
                TupleValidationErrorKind::Relationship,
                "relationship_condition_undefined",
                source,
            )
        })?;
        let matched = restrictions
            .iter()
            .try_fold(false, |matched, restriction| {
                let required = match restriction.condition() {
                    ConditionRequirement::Unconditional => return Ok(matched),
                    ConditionRequirement::Required(id) => self.condition(id).map_err(|source| {
                        TupleValidationError::lookup(
                            TupleValidationErrorKind::CompiledModel,
                            "restriction_condition_invalid",
                            source,
                        )
                    })?,
                };
                if required.name() != binding.name() {
                    return Ok(matched);
                }
                required
                    .validate_context(binding.context())
                    .map_err(TupleValidationError::condition_context)?;
                Ok::<bool, TupleValidationError>(true)
            })?;
        if matched {
            Ok(())
        } else {
            Err(TupleValidationError::new(
                TupleValidationErrorKind::Relationship,
                "relationship_condition_invalid",
            ))
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

fn validate_unconditional(restrictions: &[&DirectRestriction]) -> Result<(), TupleValidationError> {
    if restrictions
        .iter()
        .any(|restriction| restriction.condition() == ConditionRequirement::Unconditional)
    {
        Ok(())
    } else {
        Err(TupleValidationError::new(
            TupleValidationErrorKind::Relationship,
            "relationship_condition_missing",
        ))
    }
}

fn is_implicit(tuple: &TupleKey) -> bool {
    matches!(
        tuple.subject(),
        SubjectRef::Userset(userset)
            if userset.object() == tuple.object() && userset.relation() == tuple.relation()
    )
}
