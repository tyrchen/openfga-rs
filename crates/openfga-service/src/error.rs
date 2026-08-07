//! Stable, redacted service-orchestration failures.

use std::fmt;

use openfga_check::{CheckError, CheckErrorKind};
use openfga_domain::{
    AuthorizationModelId, ConditionName, RelationName, RelationshipTuple, TupleKey, TypeName,
};
use openfga_list::{ListError, ListErrorKind};
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

/// Safely bounded source context for exact model-compilation error rendering.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelSemanticContext {
    /// A whole-model diagnostic with no source identifier.
    Model,
    /// A latest-model selection for one validated store namespace.
    LatestSelection {
        /// Store namespace used by the selection.
        store_id: openfga_domain::StoreId,
    },
    /// One type declaration.
    Type {
        /// Declared object type.
        object_type: TypeName,
    },
    /// One relation declaration.
    Relation {
        /// Enclosing object type.
        object_type: TypeName,
        /// Declared relation.
        relation: RelationName,
    },
    /// One direct relation-type restriction.
    Restriction {
        /// Enclosing object type.
        object_type: TypeName,
        /// Enclosing relation.
        relation: RelationName,
        /// Referenced subject type.
        subject_type: TypeName,
        /// Referenced subject userset relation, when present.
        subject_relation: Option<RelationName>,
        /// Referenced condition, when present.
        condition: Option<ConditionName>,
    },
    /// One relation rewrite and its safely bounded referenced declarations.
    Rewrite {
        /// Enclosing object type.
        object_type: TypeName,
        /// Enclosing relation.
        relation: RelationName,
        /// Missing or self-referenced computed relation, when present.
        referenced_relation: Option<RelationName>,
        /// Tuple-to-userset tupleset relation, when present.
        tupleset: Option<RelationName>,
        /// Tuple-to-userset computed relation, when present.
        computed: Option<RelationName>,
        /// Direct target types permitted by the tupleset relation.
        target_types: Box<[ModelRelationType]>,
        /// Set operator at this rewrite node, when applicable.
        operator: Option<ModelSetOperator>,
        /// Number of operands declared on the set operator.
        child_count: Option<usize>,
    },
    /// One condition declaration.
    Condition {
        /// Condition map key.
        key: ConditionName,
        /// Name declared inside the condition.
        name: ConditionName,
    },
}

/// Safely renderable authorization-model set operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelSetOperator {
    /// Set union.
    Union,
    /// Set intersection.
    Intersection,
}

/// Safely bounded source form of one direct relation-type restriction.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelRelationType {
    /// A concrete object restriction.
    Object(TypeName),
    /// A userset restriction.
    Userset(TypeName, RelationName),
    /// A typed-wildcard restriction.
    Wildcard(TypeName),
}

#[derive(Debug, Error)]
enum ServiceErrorSource {
    #[error(transparent)]
    Check(CheckError),
    #[error(transparent)]
    List(ListError),
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
    source: Option<Box<ServiceErrorSource>>,
    tuple: Option<Box<TupleKey>>,
    condition: Option<Box<ConditionName>>,
    condition_parameter_count: Option<usize>,
    model_id: Option<Box<AuthorizationModelId>>,
    model_context: Option<Box<ModelSemanticContext>>,
    actual: Option<usize>,
    limit: Option<usize>,
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

    /// Returns safely bounded tuple context for public semantic error mapping.
    #[must_use]
    pub fn tuple(&self) -> Option<&TupleKey> {
        self.tuple.as_deref()
    }

    /// Returns the validated condition identifier attached to a relationship tuple.
    #[must_use]
    pub fn condition(&self) -> Option<&ConditionName> {
        self.condition.as_deref()
    }

    /// Returns the selected condition's declared parameter count when known.
    #[must_use]
    pub const fn condition_parameter_count(&self) -> Option<usize> {
        self.condition_parameter_count
    }

    /// Returns the validated model identifier associated with a public failure.
    #[must_use]
    pub fn model_id(&self) -> Option<AuthorizationModelId> {
        self.model_id.as_ref().map(|model_id| **model_id)
    }

    /// Returns safely bounded model source context for public error mapping.
    #[must_use]
    pub fn model_context(&self) -> Option<&ModelSemanticContext> {
        self.model_context.as_deref()
    }

    /// Returns structured authorization-model diagnostics when compilation failed.
    #[must_use]
    pub fn model_errors(&self) -> Option<&ModelErrors> {
        match self.source.as_deref() {
            Some(ServiceErrorSource::Model(errors)) => Some(errors),
            _ => None,
        }
    }

    /// Returns the structured tuple diagnostic when semantic validation failed.
    #[must_use]
    pub fn tuple_validation_error(&self) -> Option<&TupleValidationError> {
        match self.source.as_deref() {
            Some(ServiceErrorSource::TupleValidation(error)) => Some(error),
            _ => None,
        }
    }

    /// Returns the configured finite limit associated with a resource failure.
    #[must_use]
    pub const fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Returns the measured resource size associated with a bounded-input failure.
    #[must_use]
    pub const fn actual(&self) -> Option<usize> {
        self.actual
    }

    /// Attaches a validated, byte-bounded tuple for exact public error rendering.
    #[must_use]
    pub fn with_tuple(mut self, tuple: TupleKey) -> Self {
        self.tuple = Some(Box::new(tuple));
        self
    }

    /// Attaches a validated tuple key and its optional condition name without condition values.
    #[must_use]
    pub fn with_relationship_tuple(mut self, tuple: &RelationshipTuple) -> Self {
        self.tuple = Some(Box::new(tuple.key().clone()));
        self.condition = tuple
            .condition()
            .binding()
            .map(|binding| Box::new(binding.name().clone()));
        self
    }

    /// Attaches the selected condition's declared parameter count.
    #[must_use]
    pub const fn with_condition_parameter_count(mut self, count: Option<usize>) -> Self {
        self.condition_parameter_count = count;
        self
    }

    /// Attaches a validated model identifier for exact public error rendering.
    #[must_use]
    pub fn with_model_id(mut self, model_id: AuthorizationModelId) -> Self {
        self.model_id = Some(Box::new(model_id));
        self
    }

    /// Attaches safely bounded model source context for exact public errors.
    #[must_use]
    pub fn with_model_context(mut self, context: ModelSemanticContext) -> Self {
        self.model_context = Some(Box::new(context));
        self
    }

    pub(crate) fn assertion_tuple(source: TupleValidationError, tuple: TupleKey) -> Self {
        let code = match source.code() {
            "query_object_type_missing" => "assertion_object_type_missing",
            "query_relation_missing" => "assertion_relation_missing",
            "query_subject_type_missing" => "assertion_subject_type_missing",
            "query_userset_relation_missing" => "assertion_userset_relation_missing",
            code => code,
        };
        Self::from_source(
            ServiceErrorKind::InvalidRequest,
            code,
            ServiceErrorSource::TupleValidation(source),
        )
        .with_tuple(tuple)
    }

    pub(crate) const fn tuple_write_limit(limit: usize) -> Self {
        Self {
            kind: ServiceErrorKind::ResourceExhausted,
            code: "tuple_write_item_limit",
            source: None,
            tuple: None,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: Some(limit),
        }
    }

    pub(crate) const fn unsupported_model_selection() -> Self {
        Self {
            kind: ServiceErrorKind::Internal,
            code: "unsupported_model_selection",
            source: None,
            tuple: None,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: None,
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
            tuple: None,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: None,
        }
    }

    pub(crate) const fn resource_exhausted(code: &'static str) -> Self {
        Self {
            kind: ServiceErrorKind::ResourceExhausted,
            code,
            source: None,
            tuple: None,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: None,
        }
    }

    pub(crate) const fn resource_exhausted_with_limit(code: &'static str, limit: usize) -> Self {
        Self {
            kind: ServiceErrorKind::ResourceExhausted,
            code,
            source: None,
            tuple: None,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: Some(limit),
        }
    }

    pub(crate) fn condition_context_size(
        tuple: &RelationshipTuple,
        actual: usize,
        limit: usize,
    ) -> Self {
        Self {
            kind: ServiceErrorKind::InvalidRequest,
            code: "relationship_condition_context_size",
            source: None,
            tuple: Some(Box::new(tuple.key().clone())),
            condition: tuple
                .condition()
                .binding()
                .map(|binding| Box::new(binding.name().clone())),
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: Some(actual),
            limit: Some(limit),
        }
    }

    fn from_source(kind: ServiceErrorKind, code: &'static str, source: ServiceErrorSource) -> Self {
        Self {
            kind,
            code,
            source: Some(Box::new(source)),
            tuple: None,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: None,
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
            source: Some(Box::new(ServiceErrorSource::Check(source))),
            tuple: None,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: None,
        }
    }
}

impl From<ListError> for ServiceError {
    fn from(source: ListError) -> Self {
        let kind = match source.kind() {
            ListErrorKind::InvalidModel | ListErrorKind::InvalidTuple => {
                ServiceErrorKind::InvalidRequest
            }
            ListErrorKind::DepthExceeded
            | ListErrorKind::DispatchExceeded
            | ListErrorKind::DatastoreQueryExceeded
            | ListErrorKind::TupleItemExceeded
            | ListErrorKind::CandidateExceeded
            | ListErrorKind::SubjectExceeded
            | ListErrorKind::ConditionCostExceeded => ServiceErrorKind::ResourceExhausted,
            ListErrorKind::StorageUnavailable => ServiceErrorKind::Unavailable,
            ListErrorKind::Timeout => ServiceErrorKind::Timeout,
            ListErrorKind::Cancelled => ServiceErrorKind::Cancelled,
            _ => ServiceErrorKind::Internal,
        };
        let code = source.code();
        Self {
            kind,
            code,
            source: Some(Box::new(ServiceErrorSource::List(source))),
            tuple: None,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: None,
        }
    }
}

impl From<StorageError> for ServiceError {
    fn from(source: StorageError) -> Self {
        let storage_code = source.code();
        let tuple = source.tuple().cloned().map(Box::new);
        let kind = match (source.kind(), storage_code) {
            (
                StorageErrorKind::Conflict,
                "missing_tuple_delete"
                | "tuple_delete_missing"
                | "duplicate_tuple_write"
                | "tuple_write_duplicate",
            ) => ServiceErrorKind::InvalidRequest,
            (StorageErrorKind::AlreadyExists, _) => ServiceErrorKind::AlreadyExists,
            (StorageErrorKind::Conflict, _) => ServiceErrorKind::Conflict,
            (StorageErrorKind::InvalidContinuation, _) => ServiceErrorKind::InvalidContinuation,
            (StorageErrorKind::Timeout, _) => ServiceErrorKind::Timeout,
            (StorageErrorKind::Cancelled, _) => ServiceErrorKind::Cancelled,
            (StorageErrorKind::Unavailable, _) => ServiceErrorKind::Unavailable,
            (StorageErrorKind::ResourceExhausted, _) => ServiceErrorKind::ResourceExhausted,
            (
                StorageErrorKind::NotFound
                | StorageErrorKind::Integrity
                | StorageErrorKind::Internal,
                _,
            ) => ServiceErrorKind::Internal,
        };
        let code = match storage_code {
            "missing_tuple_delete" | "tuple_delete_missing" => "missing_tuple_delete",
            "duplicate_tuple_write" | "tuple_write_duplicate" => "duplicate_tuple_write",
            code => code,
        };
        Self {
            kind,
            code,
            source: Some(Box::new(ServiceErrorSource::Storage(source))),
            tuple,
            condition: None,
            condition_parameter_count: None,
            model_id: None,
            model_context: None,
            actual: None,
            limit: None,
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
            .field("tuple", &self.tuple.as_ref().map(|_| "[REDACTED]"))
            .field("condition", &self.condition.as_ref().map(|_| "[REDACTED]"))
            .field("condition_parameter_count", &self.condition_parameter_count)
            .field("model_id", &self.model_id.as_ref().map(|_| "[REDACTED]"))
            .field(
                "model_context",
                &self.model_context.as_ref().map(|_| "[REDACTED]"),
            )
            .field("limit", &self.limit)
            .field("actual", &self.actual)
            .finish_non_exhaustive()
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
