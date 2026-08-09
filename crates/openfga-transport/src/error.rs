//! Exhaustive, redacted wire-error mapping shared by both transports.

use std::{borrow::Cow, fmt};

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE},
    response::{IntoResponse, Response},
};
use openfga_auth::AuthenticationError;
use openfga_condition::{
    CompileErrorDetail, CompileErrorKind, ConditionContextError, ConditionContextErrorKind,
};
use openfga_domain::SubjectRef;
use openfga_model::{ConditionParameterTypeError, ModelError, ModelErrorCode, ModelErrorDetail};
use openfga_proto::openfga::v1 as pb;
use openfga_service::{
    ModelRelationType, ModelSemanticContext, ModelSetOperator, ServiceError, ServiceErrorKind,
};
use prost_reflect::ReflectMessage;
use prost_validate::{
    Error as ValidationError,
    errors::{self as validation, r#enum, list, map, message, string},
};
use serde::Serialize;
use tonic::Code;

use crate::validation::validate_all;

const INVALID_TIMESTAMP_PREFIX: &str = "openfga_invalid_timestamp:";

/// A safe `OpenFGA` protocol failure.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
pub struct ApiError {
    http_status: StatusCode,
    grpc_code: Code,
    code: &'static str,
    message: Cow<'static, str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: String,
}

/// Bounded HTTP completion label carried out-of-band from compatible wire status codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HttpCompletionClass(&'static str);

impl HttpCompletionClass {
    #[must_use]
    pub(crate) const fn value(self) -> &'static str {
        self.0
    }
}

impl ApiError {
    /// Creates a request validation failure without retaining hostile input.
    #[must_use]
    pub const fn invalid_request() -> Self {
        Self::validation("validation_error", "the request is invalid")
    }

    /// Validates one generated request against every pinned PGV rule.
    ///
    /// # Errors
    ///
    /// Returns the exact public `OpenFGA` structural-validation failure.
    pub(crate) fn validate<T: ReflectMessage>(request: &T) -> Result<(), Self> {
        Self::from_validations(&validate_all(request))
    }

    /// Applies ordered `ValidateAll` with generated gRPC nested-cause formatting.
    pub(crate) fn validate_grpc<T: ReflectMessage>(request: &T) -> Result<(), Self> {
        Self::from_nested_validations(&validate_all(request))
    }

    pub(crate) fn validate_write_authorization_model(
        request: &pb::WriteAuthorizationModelRequest,
    ) -> Result<(), Self> {
        let mut violations = validate_all(request)
            .iter()
            .map(ValidationViolation::from_error)
            .collect::<Vec<_>>();
        let exceeded_type_limit = violations.iter().any(|violation| {
            violation.field.ends_with(".TypeDefinitions")
                && violation.kind == ValidationKind::ListMaximum
        });
        violations.retain(|violation| {
            !violation.field.ends_with(".TypeDefinitions")
                || violation.kind != ValidationKind::ListMaximum
        });
        if !violations.is_empty() {
            return Self::from_violations(&violations);
        }
        if exceeded_type_limit {
            return Err(Self::authorization_model_type_limit());
        }
        Ok(())
    }

    pub(crate) fn validate_list_objects(request: &pb::ListObjectsRequest) -> Result<(), Self> {
        Self::validate(request)
    }

    pub(crate) fn validate_streamed_list_objects(
        request: &pb::StreamedListObjectsRequest,
    ) -> Result<(), Self> {
        Self::validate(request)
    }

    pub(crate) fn validate_list_users(request: &pb::ListUsersRequest) -> Result<(), Self> {
        Self::validate(request)
    }

    fn from_validations(errors: &[ValidationError]) -> Result<(), Self> {
        if errors.is_empty() {
            return Ok(());
        }
        let violations = errors
            .iter()
            .map(ValidationViolation::from_error)
            .collect::<Vec<_>>();
        Self::from_violations(&violations)
    }

    fn from_violations(violations: &[ValidationViolation]) -> Result<(), Self> {
        let first = violations.first().ok_or_else(Self::invalid_request)?;
        let code = validation_code(first);
        let message = violations
            .iter()
            .map(|violation| format!("invalid {}: {}", violation.field, violation.reason))
            .collect::<Vec<_>>()
            .join("; ");
        Err(Self::new_owned(
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            code,
            message,
        ))
    }

    fn from_nested_validations(errors: &[ValidationError]) -> Result<(), Self> {
        let Some(first) = errors.first() else {
            return Ok(());
        };
        let code = validation_code(&ValidationViolation::from_error(first));
        let mut violations = Vec::new();
        for error in errors {
            NestedValidationViolation::merge(
                &mut violations,
                NestedValidationViolation::from_error(error),
            );
        }
        let message = violations
            .iter()
            .map(NestedValidationViolation::render)
            .collect::<Vec<_>>()
            .join("; ");
        Err(Self::new_owned(
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            code,
            message,
        ))
    }

    pub(crate) const fn invalid_store_id() -> Self {
        Self::validation(
            "store_id_invalid_length",
            "store_id must be a canonical 26-character ULID",
        )
    }

    pub(crate) const fn invalid_page_size() -> Self {
        Self::validation("page_size_invalid", "page_size must be between 1 and 100")
    }

    pub(crate) const fn missing_tuple_key() -> Self {
        Self::validation("tuple_key_value_not_specified", "tuple_key is required")
    }

    pub(crate) fn authorization_model_too_large(actual: usize, limit: usize) -> Self {
        Self::semantic_validation_owned(
            "exceeded_entity_limit",
            format!("model exceeds size limit: {actual} bytes vs {limit} bytes"),
        )
    }

    pub(crate) const fn authorization_model_type_limit() -> Self {
        Self::semantic_validation(
            "exceeded_entity_limit",
            "number of type definitions in an authorization model exceeds the allowed limit of 100",
        )
    }

    pub(crate) fn assertion_bytes_too_large(limit: usize) -> Self {
        Self::semantic_validation_owned(
            "exceeded_entity_limit",
            format!("The number of bytes exceeds the allowed limit of {limit}"),
        )
    }

    pub(crate) fn protobuf_json(message: String) -> Self {
        Self::semantic_validation_owned("validation_error", message)
    }

    pub(crate) const fn invalid_object(too_long: bool) -> Self {
        if too_long {
            Self::validation("object_too_long", "object exceeds its byte limit")
        } else {
            Self::validation("object_invalid_pattern", "object has an invalid format")
        }
    }

    pub(crate) const fn invalid_relation(too_long: bool) -> Self {
        if too_long {
            Self::validation("relation_too_long", "relation exceeds its byte limit")
        } else {
            Self::validation("validation_error", "relation has an invalid format")
        }
    }

    pub(crate) const fn invalid_user() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            Code::Unknown,
            "validation_error",
            "invalid relation: the 'user' field must be an object (e.g. document:1) or an \
             'object#relation' or a typed wildcard (e.g. group:*)",
        )
    }

    pub(crate) const fn invalid_model_id(too_long: bool) -> Self {
        if too_long {
            Self::validation(
                "authorization_model_id_too_long",
                "authorization_model_id exceeds its byte limit",
            )
        } else {
            Self::validation("validation_error", "authorization_model_id is invalid")
        }
    }

    const fn validation(code: &'static str, message: &'static str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            code,
            message,
        )
    }

    const fn semantic_validation(code: &'static str, message: &'static str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, Code::Unknown, code, message)
    }

    pub(crate) fn semantic_validation_owned(code: &'static str, message: String) -> Self {
        Self::new_owned(StatusCode::BAD_REQUEST, Code::Unknown, code, message)
    }

    pub(crate) fn from_check_service(
        error: ServiceError,
        tuple: &pb::CheckRequestTupleKey,
    ) -> Self {
        let object_type = tuple
            .object
            .split_once(':')
            .map(|(object_type, _)| object_type);
        match (error.code(), object_type) {
            ("query_object_type_missing", Some(object_type)) => Self::semantic_validation_owned(
                "validation_error",
                format!("invalid relation: type '{object_type}' not found"),
            ),
            ("query_relation_missing", Some(object_type)) => Self::semantic_validation_owned(
                "validation_error",
                format!(
                    "invalid relation: relation '{}#{}' not found",
                    object_type, tuple.relation
                ),
            ),
            ("query_subject_type_missing", _) => {
                let subject_type = tuple
                    .user
                    .split_once(':')
                    .map(|(subject_type, _)| subject_type);
                subject_type.map_or_else(
                    || Self::from(error),
                    |subject_type| {
                        Self::semantic_validation_owned(
                            "validation_error",
                            format!("invalid relation: type '{subject_type}' not found"),
                        )
                    },
                )
            }
            ("query_userset_relation_missing", _) => {
                let userset = tuple
                    .user
                    .split_once(':')
                    .and_then(|(subject_type, suffix)| {
                        suffix
                            .rsplit_once('#')
                            .map(|(_, relation)| (subject_type, relation))
                    });
                userset.map_or_else(
                    || Self::from(error),
                    |(subject_type, relation)| {
                        Self::semantic_validation_owned(
                            "validation_error",
                            format!(
                                "invalid relation: relation '{subject_type}#{relation}' not found"
                            ),
                        )
                    },
                )
            }
            _ => Self::from(error),
        }
    }

    pub(crate) fn from_assertion_service(error: ServiceError) -> Self {
        match error.code() {
            "assertion_object_type_missing" => tuple_type_error(&error, true),
            "assertion_relation_missing" => assertion_relation_error(&error, false),
            "assertion_subject_type_missing" => tuple_type_error(&error, false),
            "assertion_userset_relation_missing" => assertion_relation_error(&error, true),
            _ => Self::from(error),
        }
    }

    /// Creates the stable invalid-continuation response.
    #[must_use]
    pub const fn invalid_continuation() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            "invalid_continuation_token",
            "the continuation token is invalid",
        )
    }

    /// Creates a deliberate response for methods delivered in a later milestone.
    #[must_use]
    pub const fn unimplemented() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            Code::Unimplemented,
            "unimplemented",
            "the operation is not implemented",
        )
    }

    pub(crate) const fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            Code::Internal,
            "internal_error",
            "an internal error occurred",
        )
    }

    pub(crate) const fn payload_too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            Code::ResourceExhausted,
            "resource_exhausted",
            "the request body is too large",
        )
    }

    pub(crate) const fn response_too_large() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            Code::ResourceExhausted,
            "resource_exhausted",
            "the response exceeds its configured size limit",
        )
    }

    pub(crate) const fn deadline_exceeded() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            Code::DeadlineExceeded,
            "deadline_exceeded",
            "the request deadline elapsed",
        )
    }

    pub(crate) const fn cancelled() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            Code::Cancelled,
            "cancelled",
            "Request Cancelled",
        )
    }

    pub(crate) const fn unauthenticated() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            Code::Unauthenticated,
            "unauthenticated",
            "authentication credentials are missing or invalid",
        )
    }

    pub(crate) const fn permission_denied() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            Code::PermissionDenied,
            "forbidden",
            "the principal is not authorized to perform the action",
        )
    }

    pub(crate) const fn authentication_unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            Code::Unavailable,
            "unavailable",
            "authentication service is unavailable",
        )
    }

    pub(crate) const fn overloaded() -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            Code::ResourceExhausted,
            "throttled_timeout_error",
            "the request rate limit was exceeded",
        )
    }

    const fn new(
        http_status: StatusCode,
        grpc_code: Code,
        code: &'static str,
        message: &'static str,
    ) -> Self {
        Self {
            http_status,
            grpc_code,
            code,
            message: Cow::Borrowed(message),
        }
    }

    fn new_owned(
        http_status: StatusCode,
        grpc_code: Code,
        code: &'static str,
        message: String,
    ) -> Self {
        Self {
            http_status,
            grpc_code,
            code,
            message: Cow::Owned(message),
        }
    }

    /// Returns the `OpenFGA` error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Returns the HTTP status.
    #[must_use]
    pub const fn http_status(&self) -> StatusCode {
        self.http_status
    }

    #[must_use]
    pub(crate) fn http_completion_class(&self) -> HttpCompletionClass {
        let class = match self.grpc_code {
            Code::DeadlineExceeded => "timeout",
            Code::Cancelled => "cancelled",
            Code::ResourceExhausted if self.code == "throttled_timeout_error" => "overloaded",
            _ if self.http_status.is_client_error() => "client_error",
            _ => "server_error",
        };
        HttpCompletionClass(class)
    }
}

impl From<AuthenticationError> for ApiError {
    fn from(error: AuthenticationError) -> Self {
        match error {
            AuthenticationError::MissingCredentials | AuthenticationError::InvalidCredentials => {
                Self::unauthenticated()
            }
            AuthenticationError::Unavailable => Self::authentication_unavailable(),
            _ => Self::unauthenticated(),
        }
    }
}

impl From<ServiceError> for ApiError {
    fn from(error: ServiceError) -> Self {
        if matches!(
            error.kind(),
            ServiceErrorKind::Internal | ServiceErrorKind::Unavailable
        ) {
            tracing::error!(
                service.error_kind = ?error.kind(),
                service.error_code = error.code(),
                "authorization service request failed",
            );
        }
        match error.kind() {
            ServiceErrorKind::StoreNotFound => Self::new(
                StatusCode::NOT_FOUND,
                Code::Unknown,
                "store_id_not_found",
                "Store ID not found",
            ),
            ServiceErrorKind::ModelNotFound => error.model_id().map_or_else(
                || match error.model_context() {
                    Some(ModelSemanticContext::LatestSelection { store_id }) => {
                        Self::semantic_validation_owned(
                            "latest_authorization_model_not_found",
                            format!("No authorization models found for store '{store_id}'"),
                        )
                    }
                    _ => Self::new(
                        StatusCode::BAD_REQUEST,
                        Code::Unknown,
                        "authorization_model_not_found",
                        "authorization model not found",
                    ),
                },
                |model_id| {
                    Self::semantic_validation_owned(
                        "authorization_model_not_found",
                        format!("Authorization Model '{model_id}' not found"),
                    )
                },
            ),
            ServiceErrorKind::AlreadyExists => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::AlreadyExists,
                "already_exists",
                "the resource already exists",
            ),
            ServiceErrorKind::Conflict => {
                let message = if error.code() == "tuple_condition_conflict" {
                    "transactional write failed due to conflict"
                } else {
                    "the request conflicts with current state"
                };
                Self::new(StatusCode::CONFLICT, Code::Aborted, "Aborted", message)
            }
            ServiceErrorKind::InvalidContinuation => Self::semantic_validation(
                "invalid_continuation_token",
                "Invalid continuation token",
            ),
            ServiceErrorKind::InvalidRequest => invalid_service_error(&error),
            ServiceErrorKind::Condition => {
                Self::semantic_validation("validation_error", "condition evaluation failed")
            }
            ServiceErrorKind::ResourceExhausted => resource_service_error(&error),
            ServiceErrorKind::Unavailable => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::Unavailable,
                "unavailable",
                "the service is unavailable",
            ),
            ServiceErrorKind::Timeout => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::DeadlineExceeded,
                "deadline_exceeded",
                "Request Deadline Exceeded",
            ),
            ServiceErrorKind::Cancelled => Self::cancelled(),
            ServiceErrorKind::Internal => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::Internal,
                "internal_error",
                "an internal error occurred",
            ),
        }
    }
}

fn invalid_service_error(error: &ServiceError) -> ApiError {
    match error.code() {
        "tuple_write_empty" => ApiError::semantic_validation(
            "invalid_write_input",
            "Invalid input. Make sure you provide at least one write, or at least one delete",
        ),
        "duplicate_tuple_in_write" => duplicate_tuple_error(error),
        "missing_tuple_delete" => persisted_tuple_conflict(error, false),
        "duplicate_tuple_write" => persisted_tuple_conflict(error, true),
        "invalid_authorization_model" => invalid_model_error(error),
        "query_subject_type_missing" | "semantic_subject_type_missing" => {
            query_subject_type_error(error)
        }
        "query_relation_missing" | "query_userset_relation_missing" => query_relation_error(error),
        "relationship_object_type_missing" => relationship_object_type_error(error),
        "relationship_relation_missing" => relationship_relation_error(error),
        "relationship_subject_type_missing" => relationship_subject_type_error(error),
        "relationship_userset_relation_missing" => relationship_userset_relation_error(error),
        "semantic_relation_invalid" => ApiError::semantic_validation(
            "relation_not_found",
            "the referenced relation was not found",
        ),
        "relationship_tuple_implicit" => {
            invalid_tuple_error(error, "cannot write a tuple that is implicit")
        }
        "relationship_tuple_not_permitted" => tuple_not_permitted_error(error),
        "relationship_condition_missing" => invalid_tuple_error(error, "condition is missing"),
        "relationship_condition_undefined" => invalid_tuple_error(error, "undefined condition"),
        "relationship_condition_invalid" => {
            invalid_tuple_error(error, "invalid condition for type restriction")
        }
        "relationship_condition_context_invalid" => condition_context_error(error),
        "relationship_condition_context_size" => invalid_tuple_error(
            error,
            &format!(
                "condition context size limit exceeded: {} bytes exceeds {} bytes",
                error.actual().unwrap_or_default(),
                error.limit().unwrap_or(32_768),
            ),
        ),
        "batch_condition_context_invalid" => {
            ApiError::semantic_validation("validation_error", "condition context is invalid")
        }
        "model_id_mismatch" | "model_store_mismatch" => ApiError::semantic_validation(
            "authorization_model_not_found",
            "authorization model not found",
        ),
        _ => ApiError::invalid_request(),
    }
}

fn persisted_tuple_conflict(error: &ServiceError, write: bool) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "write_failed_due_to_invalid_input",
            "tuple to be written already existed or the tuple to be deleted did not exist",
        );
    };
    let operation = if write {
        "cannot write a tuple which already exists"
    } else {
        "cannot delete a tuple which does not exist"
    };
    ApiError::semantic_validation_owned(
        "write_failed_due_to_invalid_input",
        format!(
            "{operation}: user: '{}', relation: '{}', object: '{}': tuple to be written already \
             existed or the tuple to be deleted did not exist",
            tuple.subject(),
            tuple.relation(),
            tuple.object(),
        ),
    )
}

fn duplicate_tuple_error(error: &ServiceError) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "cannot_allow_duplicate_tuples_in_one_request",
            "duplicate tuple in write",
        );
    };
    ApiError::semantic_validation_owned(
        "cannot_allow_duplicate_tuples_in_one_request",
        format!(
            "duplicate tuple in write: user: '{}', relation: '{}', object: '{}'",
            tuple.subject(),
            tuple.relation(),
            tuple.object()
        ),
    )
}

fn invalid_model_error(error: &ServiceError) -> ApiError {
    let Some(diagnostic) = error
        .model_errors()
        .and_then(|errors| errors.errors().first())
    else {
        return ApiError::semantic_validation(
            "invalid_authorization_model",
            "invalid authorization model encountered",
        );
    };
    let context = error.model_context();
    if diagnostic.code() == ModelErrorCode::InvalidTypeDefinitionCount {
        return ApiError::semantic_validation(
            "exceeded_entity_limit",
            "number of type definitions in an authorization model exceeds the allowed limit of 100",
        );
    }
    ApiError::semantic_validation_owned(
        "invalid_authorization_model",
        model_error_message(diagnostic, context),
    )
}

#[allow(
    clippy::too_many_lines,
    clippy::unnested_or_patterns,
    reason = "the row-oriented compatibility table keeps each context form independently auditable"
)]
fn model_error_message(diagnostic: &ModelError, context: Option<&ModelSemanticContext>) -> String {
    let code = diagnostic.code();
    if let (
        ModelErrorCode::InvalidConditionParameterType,
        Some(ModelSemanticContext::Condition { key, .. }),
        Some(ModelErrorDetail::ConditionParameterType(error)),
    ) = (code, context, diagnostic.detail())
    {
        return condition_parameter_type_message(key, error);
    }
    if let (
        ModelErrorCode::InvalidCondition,
        Some(ModelSemanticContext::Condition { key, .. }),
        Some(ModelErrorDetail::ConditionCompile {
            kind: CompileErrorKind::NonBooleanResult,
            found_type: Some(found_type),
            ..
        }),
    ) = (code, context, diagnostic.detail())
    {
        return format!(
            "failed to compile expression on condition '{key}' - expected a bool condition \
             expression output, but got '{found_type}'"
        );
    }
    if let (
        ModelErrorCode::InvalidCondition,
        Some(ModelSemanticContext::Condition { key, .. }),
        Some(ModelErrorDetail::ConditionCompile {
            diagnostic: Some(diagnostic),
            ..
        }),
    ) = (code, context, diagnostic.detail())
    {
        let detail = match diagnostic {
            CompileErrorDetail::UnknownIdentifier(identifier) => {
                format!("undeclared reference to '{identifier}'")
            }
            CompileErrorDetail::NoMatchingOverload {
                function,
                argument_types,
            } => format!(
                "found no matching overload for '{function}' applied to '({})'",
                argument_types.join(", "),
            ),
            _ => "condition expression is invalid".to_owned(),
        };
        return format!("failed to compile expression on condition '{key}' - {detail}");
    }
    match (code, context) {
        (ModelErrorCode::InvalidSchemaVersion, _) => "invalid schema version".to_owned(),
        (ModelErrorCode::DuplicateType, _) => {
            "an authorization model cannot contain duplicate types".to_owned()
        }
        (ModelErrorCode::ReservedName, Some(ModelSemanticContext::Type { object_type })) => {
            format!("the definition of type '{object_type}' is invalid")
        }
        (
            ModelErrorCode::ReservedName,
            Some(ModelSemanticContext::Relation {
                object_type,
                relation,
            }),
        )
        | (
            ModelErrorCode::ReservedName,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                ..
            }),
        ) => relation_error(object_type, relation, "self and this are reserved keywords"),
        (
            ModelErrorCode::UndefinedType | ModelErrorCode::UndefinedRelation,
            Some(ModelSemanticContext::Restriction {
                object_type,
                relation,
                subject_type,
                subject_relation,
                ..
            }),
        ) => invalid_relation_type(
            object_type,
            relation,
            subject_type,
            subject_relation.as_ref(),
        ),
        (
            ModelErrorCode::UndefinedCondition,
            Some(ModelSemanticContext::Restriction {
                relation,
                condition: Some(condition),
                ..
            }),
        ) => format!("condition {condition} is undefined for relation {relation}"),
        (
            ModelErrorCode::UndefinedRelation,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                referenced_relation: Some(referenced),
                ..
            }),
        ) => format!("'{object_type}#{referenced}' relation is undefined"),
        (
            ModelErrorCode::UndefinedRelation,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                tupleset: Some(tupleset),
                ..
            }),
        ) => format!("'{object_type}#{tupleset}' relation is undefined"),
        (
            ModelErrorCode::AssignableWithoutRestrictions,
            Some(ModelSemanticContext::Relation {
                object_type,
                relation,
            }),
        )
        | (
            ModelErrorCode::AssignableWithoutRestrictions,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                ..
            }),
        ) => format!(
            "the assignable relation '{relation}' in object type '{object_type}' must contain at \
             least one relation type"
        ),
        (
            ModelErrorCode::NonAssignableWithRestrictions,
            Some(ModelSemanticContext::Relation {
                object_type,
                relation,
            }),
        )
        | (
            ModelErrorCode::NonAssignableWithRestrictions,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                ..
            }),
        ) => format!(
            "the non-assignable relation '{relation}' in object type '{object_type}' should not \
             contain a relation type"
        ),
        (
            ModelErrorCode::InvalidOperatorArity,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                operator: Some(operator),
                child_count: Some(_),
                ..
            }),
        ) => {
            let operator = match operator {
                ModelSetOperator::Union => "union",
                ModelSetOperator::Intersection => "intersection",
            };
            format!(
                "invalid relation: '{object_type}#{relation}' as {operator} has less than 2 \
                 children"
            )
        }
        (
            ModelErrorCode::IllegalSelfReference,
            Some(ModelSemanticContext::Relation {
                object_type,
                relation,
            }),
        )
        | (
            ModelErrorCode::IllegalSelfReference,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                ..
            }),
        ) => relation_error(object_type, relation, "invalid userset rewrite definition"),
        (
            ModelErrorCode::InvalidRewrite,
            Some(ModelSemanticContext::Relation {
                object_type,
                relation,
            }),
        )
        | (
            ModelErrorCode::InvalidRewrite,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                ..
            }),
        ) => format!(
            "the definition of relation '{relation}' in object type '{object_type}' is invalid"
        ),
        (
            ModelErrorCode::InvalidTuplesetRelation,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                tupleset: Some(tupleset),
                ..
            }),
        ) => format!(
            "the '{object_type}#{tupleset}' relation is referenced in at least one tupleset and \
             thus must be a direct relation"
        ),
        (
            ModelErrorCode::InvalidRestriction,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                tupleset: Some(tupleset),
                target_types,
                ..
            }),
        ) => target_types
            .iter()
            .find_map(|target| match target {
                ModelRelationType::Userset(subject_type, subject_relation) => {
                    Some(invalid_relation_type(
                        object_type,
                        tupleset,
                        subject_type,
                        Some(subject_relation),
                    ))
                }
                _ => None,
            })
            .unwrap_or_else(|| "invalid relation type on tupleset relation".to_owned()),
        (
            ModelErrorCode::InvalidTupleToUsersetTarget,
            Some(ModelSemanticContext::Rewrite {
                computed: Some(computed),
                target_types,
                ..
            }),
        ) => format!(
            "undefined relation: {computed} does not appear as a relation in any of the directly \
             related user types [{}]",
            render_relation_types(target_types)
        ),
        (
            ModelErrorCode::NoEntrypoints,
            Some(ModelSemanticContext::Relation {
                object_type,
                relation,
            }),
        )
        | (
            ModelErrorCode::NoEntrypoints,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                ..
            }),
        ) => relation_error(object_type, relation, "no entrypoints defined"),
        (
            ModelErrorCode::PotentialLoop,
            Some(ModelSemanticContext::Relation {
                object_type,
                relation,
            }),
        )
        | (
            ModelErrorCode::PotentialLoop,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                ..
            }),
        ) => relation_error(object_type, relation, "potential loop"),
        (
            ModelErrorCode::ForbiddenComputedCycle,
            Some(ModelSemanticContext::Relation {
                object_type,
                relation,
            }),
        )
        | (
            ModelErrorCode::ForbiddenComputedCycle,
            Some(ModelSemanticContext::Rewrite {
                object_type,
                relation,
                ..
            }),
        ) => relation_error(
            object_type,
            relation,
            "an authorization model cannot contain a cycle",
        ),
        (
            ModelErrorCode::ConditionNameMismatch,
            Some(ModelSemanticContext::Condition { key, name }),
        ) => format!("condition key '{key}' does not match condition name '{name}'"),
        (ModelErrorCode::InvalidCondition, Some(ModelSemanticContext::Condition { key, .. })) => {
            format!("failed to compile expression on condition '{key}'")
        }
        (ModelErrorCode::TooManyConditions, _) => {
            "number of conditions exceeds the allowed limit".to_owned()
        }
        (ModelErrorCode::TooManyRelations, _) => {
            "number of relations exceeds the allowed limit".to_owned()
        }
        (ModelErrorCode::RewriteLimitExceeded, _) => {
            "authorization model rewrite exceeds the allowed limit".to_owned()
        }
        (ModelErrorCode::GraphLimitExceeded, _) => {
            "authorization model graph exceeds the allowed limit".to_owned()
        }
        (ModelErrorCode::TooManyModelErrors, _) => {
            "authorization model contains too many errors".to_owned()
        }
        (ModelErrorCode::DuplicateRelation, _) => {
            "an authorization model cannot contain duplicate relations".to_owned()
        }
        (ModelErrorCode::DuplicateCondition, _) => {
            "an authorization model cannot contain duplicate conditions".to_owned()
        }
        (ModelErrorCode::InvalidTypeName, _) => {
            "the type name of a type definition cannot be an empty string".to_owned()
        }
        (
            ModelErrorCode::InvalidRelationName,
            Some(ModelSemanticContext::Relation { object_type, .. }),
        ) => {
            format!("type '{object_type}' defines a relation with an empty string for a name")
        }
        (ModelErrorCode::InvalidConditionName, _) => "condition name is invalid".to_owned(),
        (ModelErrorCode::InvalidParameterName, _) => {
            "condition parameter name is invalid".to_owned()
        }
        (ModelErrorCode::OrphanRelationMetadata, _) => "relation metadata is undefined".to_owned(),
        (
            ModelErrorCode::InvalidConditionParameterType,
            Some(ModelSemanticContext::Condition { key, .. }),
        ) => {
            format!("failed to compile expression on condition '{key}': invalid parameter type")
        }
        (ModelErrorCode::InvalidModelIdentifier, _) => {
            "authorization model identifier is invalid".to_owned()
        }
        (ModelErrorCode::InvalidTypeDefinitionCount, _) => {
            "authorization model contains an invalid number of type definitions".to_owned()
        }
        _ => "invalid authorization model encountered".to_owned(),
    }
}

fn condition_parameter_type_message(
    condition: &openfga_domain::ConditionName,
    error: &ConditionParameterTypeError,
) -> String {
    let (parameter, cause) = match error {
        ConditionParameterTypeError::Unknown {
            parameter,
            type_name,
        } => (
            parameter,
            format!("unknown condition parameter type `{type_name}`"),
        ),
        ConditionParameterTypeError::GenericArity {
            parameter,
            type_name,
            expected,
            found,
        } => (
            parameter,
            format!(
                "condition parameter type `{type_name}` requires {expected} generic types; found \
                 {found}"
            ),
        ),
        _ => {
            return format!(
                "failed to compile expression on condition '{condition}': invalid parameter type"
            );
        }
    };
    format!(
        "failed to compile expression on condition '{condition}' - failed to decode parameter \
         type for parameter '{parameter}': {cause}"
    )
}

fn relation_error(
    object_type: &impl fmt::Display,
    relation: &impl fmt::Display,
    reason: &str,
) -> String {
    format!(
        "the definition of relation '{relation}' in object type '{object_type}' is invalid: \
         {reason}"
    )
}

fn invalid_relation_type(
    object_type: &impl fmt::Display,
    relation: &impl fmt::Display,
    subject_type: &impl fmt::Display,
    subject_relation: Option<&impl fmt::Display>,
) -> String {
    let relation_type = subject_relation.map_or_else(
        || subject_type.to_string(),
        |subject_relation| format!("{subject_type}#{subject_relation}"),
    );
    format!(
        "the relation type '{relation_type}' on '{relation}' in object type '{object_type}' is \
         not valid"
    )
}

fn render_relation_types(types: &[ModelRelationType]) -> String {
    types
        .iter()
        .map(|target| match target {
            ModelRelationType::Object(subject_type) => format!("type:\"{subject_type}\""),
            ModelRelationType::Userset(subject_type, relation) => {
                format!("type:\"{subject_type}\" relation:\"{relation}\"")
            }
            ModelRelationType::Wildcard(subject_type) => {
                format!("type:\"{subject_type}\" wildcard:{{}}")
            }
            _ => "unknown:{}".to_owned(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn query_subject_type_error(error: &ServiceError) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "type_not_found",
            "the referenced type was not found",
        );
    };
    ApiError::semantic_validation_owned(
        "type_not_found",
        format!("type '{}' not found", tuple.subject().subject_type()),
    )
}

fn tuple_type_error(error: &ServiceError, object: bool) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "validation_error",
            "the referenced type was not found",
        );
    };
    let object_type = if object {
        tuple.object().object_type()
    } else {
        tuple.subject().subject_type()
    };
    ApiError::semantic_validation_owned(
        "validation_error",
        format!("type '{object_type}' not found"),
    )
}

fn assertion_relation_error(error: &ServiceError, userset: bool) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "validation_error",
            "the referenced relation was not found",
        );
    };
    let (object_type, relation) = if userset {
        match tuple.subject() {
            SubjectRef::Userset(userset) => (userset.object().object_type(), userset.relation()),
            _ => (tuple.object().object_type(), tuple.relation()),
        }
    } else {
        (tuple.object().object_type(), tuple.relation())
    };
    ApiError::semantic_validation_owned(
        "validation_error",
        format!("relation '{object_type}#{relation}' not found"),
    )
}

fn query_relation_error(error: &ServiceError) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "relation_not_found",
            "the referenced relation was not found",
        );
    };
    let (object_type, relation) = if error.code() == "query_userset_relation_missing" {
        match tuple.subject() {
            SubjectRef::Userset(userset) => (userset.object().object_type(), userset.relation()),
            _ => (tuple.object().object_type(), tuple.relation()),
        }
    } else {
        (tuple.object().object_type(), tuple.relation())
    };
    ApiError::semantic_validation_owned(
        "relation_not_found",
        format!("relation '{object_type}#{relation}' not found"),
    )
}

fn relationship_relation_error(error: &ServiceError) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "validation_error",
            "the referenced relation was not found",
        );
    };
    invalid_tuple_error(
        error,
        &format!(
            "relation '{}#{}' not found",
            tuple.object().object_type(),
            tuple.relation()
        ),
    )
}

fn relationship_object_type_error(error: &ServiceError) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "validation_error",
            "the referenced type was not found",
        );
    };
    invalid_tuple_error(
        error,
        &format!("type '{}' not found", tuple.object().object_type()),
    )
}

fn relationship_subject_type_error(error: &ServiceError) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "validation_error",
            "the referenced type was not found",
        );
    };
    invalid_tuple_error(
        error,
        &format!("type '{}' not found", tuple.subject().subject_type()),
    )
}

fn relationship_userset_relation_error(error: &ServiceError) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation(
            "validation_error",
            "the referenced relation was not found",
        );
    };
    let SubjectRef::Userset(userset) = tuple.subject() else {
        return ApiError::semantic_validation(
            "validation_error",
            "the referenced relation was not found",
        );
    };
    invalid_tuple_error(
        error,
        &format!(
            "relation '{}#{}' not found",
            userset.object().object_type(),
            userset.relation()
        ),
    )
}

fn tuple_not_permitted_error(error: &ServiceError) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation("validation_error", "the tuple is invalid");
    };
    let restriction = format!("{}#{}", tuple.object().object_type(), tuple.relation());
    let reason = match tuple.subject() {
        SubjectRef::TypedWildcard(subject) => format!(
            "the typed wildcard '{subject}:*' is not an allowed type restriction for \
             '{restriction}'"
        ),
        subject => format!(
            "type '{}' is not an allowed type restriction for '{restriction}'",
            subject.subject_type()
        ),
    };
    invalid_tuple_error(error, &reason)
}

fn invalid_tuple_error(error: &ServiceError, reason: &str) -> ApiError {
    let Some(tuple) = error.tuple() else {
        return ApiError::semantic_validation("validation_error", "the tuple is invalid");
    };
    ApiError::semantic_validation_owned(
        "validation_error",
        error.condition().map_or_else(
            || format!("Invalid tuple '{tuple}'. Reason: {reason}"),
            |condition| {
                format!("Invalid tuple '{tuple} (condition {condition})'. Reason: {reason}")
            },
        ),
    )
}

fn condition_context_error(error: &ServiceError) -> ApiError {
    let context_error = error
        .tuple_validation_error()
        .and_then(|source| source.condition_context_error());
    let reason = match context_error {
        Some(source) if source.kind() == ConditionContextErrorKind::UnknownParameter => {
            match error.condition_parameter_count() {
                Some(0) => error.condition().map_or_else(
                    || "no parameters defined for the condition".to_owned(),
                    |condition| {
                        format!(
                            "parameter type error on condition '{condition}' - no parameters \
                             defined for the condition"
                        )
                    },
                ),
                Some(_) | None => {
                    format!("found invalid context parameter: {}", source.parameter())
                }
            }
        }
        Some(source) if source.kind() == ConditionContextErrorKind::InvalidParameter => {
            invalid_condition_parameter(error, source)
        }
        _ => "found invalid context parameter".to_owned(),
    };
    invalid_tuple_error(error, &reason)
}

fn invalid_condition_parameter(error: &ServiceError, source: &ConditionContextError) -> String {
    let (Some(condition), Some(expected), Some(found)) = (
        error.condition(),
        source.expected_type(),
        source.found_type(),
    ) else {
        return format!("found invalid context parameter: {}", source.parameter());
    };
    format!(
        "parameter type error on condition '{condition}' - failed to convert context parameter \
         '{}': expected type value '{expected}', but found '{found}'",
        source.parameter(),
    )
}

fn resource_service_error(error: &ServiceError) -> ApiError {
    match error.code() {
        "check_depth_exceeded"
        | "check_dispatch_exceeded"
        | "check_datastore_query_exceeded"
        | "check_tuple_items_exceeded"
        | "check_condition_cost_exceeded" => ApiError::semantic_validation(
            "authorization_model_resolution_too_complex",
            "Authorization Model resolution required too many rewrite rules to be resolved. Check \
             your authorization model for infinite recursion or too much nesting",
        ),
        "assertion_item_limit" => ApiError::semantic_validation(
            "exceeded_entity_limit",
            "the request exceeds an entity limit",
        ),
        "assertion_byte_limit" => ApiError::semantic_validation_owned(
            "exceeded_entity_limit",
            format!(
                "The number of bytes exceeds the allowed limit of {}",
                error.limit().unwrap_or(64_000),
            ),
        ),
        "tuple_write_item_limit" => error.limit().map_or_else(
            || {
                ApiError::semantic_validation(
                    "exceeded_entity_limit",
                    "the request exceeds an entity limit",
                )
            },
            |limit| {
                ApiError::semantic_validation_owned(
                    "exceeded_entity_limit",
                    format!("The number of write operations exceeds the allowed limit of {limit}"),
                )
            },
        ),
        _ => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            Code::ResourceExhausted,
            "resource_exhausted",
            "a request resource limit was exceeded",
        ),
    }
}

impl From<ApiError> for tonic::Status {
    fn from(error: ApiError) -> Self {
        Self::new(error.grpc_code, error.message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let unauthenticated = self.http_status == StatusCode::UNAUTHORIZED;
        let completion = self.http_completion_class();
        let mut response = (
            self.http_status,
            Json(ErrorBody {
                code: self.code,
                message: self.message.into_owned(),
            }),
        )
            .into_response();
        response.extensions_mut().insert(completion);
        if unauthenticated {
            response
                .headers_mut()
                .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

#[derive(Debug)]
struct NestedValidationViolation {
    field: String,
    content: NestedValidationContent,
}

#[derive(Debug)]
enum NestedValidationContent {
    Leaf(String),
    Children(Vec<NestedValidationViolation>),
}

impl NestedValidationViolation {
    fn from_error(error: &ValidationError) -> Self {
        let field = protobuf_field_path(&error.field);
        match &error.details {
            validation::Error::Message(message::Error::Message(inner))
            | validation::Error::List(list::Error::Item(inner))
            | validation::Error::Map(map::Error::Keys(inner) | map::Error::Values(inner)) => Self {
                field,
                content: NestedValidationContent::Children(vec![Self::from_error(inner)]),
            },
            details => Self {
                field: field.clone(),
                content: NestedValidationContent::Leaf(
                    ValidationViolation::from_details_nested(field, details).reason,
                ),
            },
        }
    }

    fn merge(violations: &mut Vec<Self>, incoming: Self) {
        match incoming.content {
            NestedValidationContent::Children(incoming_children) => {
                if let Some(existing) = violations.iter_mut().find(|existing| {
                    existing.field == incoming.field
                        && matches!(existing.content, NestedValidationContent::Children(_))
                }) && let NestedValidationContent::Children(existing_children) =
                    &mut existing.content
                {
                    for child in incoming_children {
                        Self::merge(existing_children, child);
                    }
                } else {
                    violations.push(Self {
                        field: incoming.field,
                        content: NestedValidationContent::Children(incoming_children),
                    });
                }
            }
            NestedValidationContent::Leaf(reason) => {
                violations.push(Self {
                    field: incoming.field,
                    content: NestedValidationContent::Leaf(reason),
                });
            }
        }
    }

    fn render(&self) -> String {
        match &self.content {
            NestedValidationContent::Leaf(reason) => {
                format!("invalid {}: {reason}", self.field)
            }
            NestedValidationContent::Children(children) => {
                let causes = children
                    .iter()
                    .map(Self::render)
                    .collect::<Vec<_>>()
                    .join("; ");
                format!(
                    "invalid {}: embedded message failed validation | caused by: {causes}",
                    self.field
                )
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum ValidationKind {
    ListMinimum,
    ListMaximum,
    MessageRequired,
    StringMaximumBytes,
    StringPattern,
    Other,
}

#[derive(Debug, Eq, PartialEq)]
struct ValidationViolation {
    field: String,
    reason: String,
    kind: ValidationKind,
}

impl ValidationViolation {
    fn from_error(error: &ValidationError) -> Self {
        let field = protobuf_field_path(&error.field);
        Self::from_details_flat(field, &error.details)
    }

    fn from_details_flat(field: String, details: &validation::Error) -> Self {
        match details {
            validation::Error::Message(message::Error::Message(inner))
            | validation::Error::List(list::Error::Item(inner))
            | validation::Error::Map(map::Error::Keys(inner) | map::Error::Values(inner)) => {
                Self::from_error(inner)
            }
            _ => Self::from_details_nested(field, details),
        }
    }

    fn from_details_nested(field: String, details: &validation::Error) -> Self {
        match details {
            validation::Error::Message(message::Error::Message(inner))
            | validation::Error::List(list::Error::Item(inner))
            | validation::Error::Map(map::Error::Keys(inner) | map::Error::Values(inner)) => {
                Self::from_nested(field, inner)
            }
            validation::Error::Message(message::Error::Required)
            | validation::Error::Any(validation::any::Error::Required) => Self {
                field,
                reason: "value is required".to_owned(),
                kind: ValidationKind::MessageRequired,
            },
            validation::Error::String(error) => Self::from_string(field, error),
            validation::Error::List(list::Error::MinItems(minimum)) => Self {
                reason: if field.ends_with("ListUsersRequest.UserFilters") {
                    format!("value must contain exactly {minimum} item(s)")
                } else {
                    format!("value must contain at least {minimum} item(s)")
                },
                field,
                kind: ValidationKind::ListMinimum,
            },
            validation::Error::List(list::Error::MaxItems(maximum)) => Self {
                field,
                reason: format!("value must contain no more than {maximum} item(s)"),
                kind: ValidationKind::ListMaximum,
            },
            validation::Error::List(list::Error::Unique) => Self {
                field,
                reason: "value must contain unique items".to_owned(),
                kind: ValidationKind::Other,
            },
            validation::Error::Map(map::Error::MinPairs(minimum)) => Self {
                field,
                reason: format!("value must contain at least {minimum} pair(s)"),
                kind: ValidationKind::Other,
            },
            validation::Error::Map(map::Error::MaxPairs(maximum)) => Self {
                field,
                reason: format!("value must contain no more than {maximum} pair(s)"),
                kind: ValidationKind::Other,
            },
            validation::Error::Enum(r#enum::Error::DefinedOnly) => Self {
                field,
                reason: "value must be one of the defined enum values".to_owned(),
                kind: ValidationKind::Other,
            },
            validation::Error::Enum(r#enum::Error::Const(value)) => Self {
                field,
                reason: format!("value must be equal to {value}"),
                kind: ValidationKind::Other,
            },
            validation::Error::Enum(r#enum::Error::In(values)) => Self {
                field,
                reason: format_number_list("value must be in list", values),
                kind: ValidationKind::Other,
            },
            validation::Error::Enum(r#enum::Error::NotIn(values)) => Self {
                field,
                reason: format_number_list("value must not be in list", values),
                kind: ValidationKind::Other,
            },
            validation::Error::Int32(error) => Self {
                field,
                reason: int32_reason(error),
                kind: ValidationKind::Other,
            },
            validation::Error::Timestamp(validation::timestamp::Error::LtNow) => Self {
                field,
                reason: "value must be less than now".to_owned(),
                kind: ValidationKind::Other,
            },
            validation::Error::InvalidRules(reason)
                if reason.starts_with(INVALID_TIMESTAMP_PREFIX) =>
            {
                Self {
                    field,
                    reason: format!(
                        "value is not a valid timestamp | caused by: proto: {}",
                        reason.trim_start_matches(INVALID_TIMESTAMP_PREFIX),
                    ),
                    kind: ValidationKind::Other,
                }
            }
            error => Self {
                field,
                reason: format!("value {error}"),
                kind: ValidationKind::Other,
            },
        }
    }

    fn from_nested(parent: String, inner: &ValidationError) -> Self {
        let child = protobuf_field_path(&inner.field);
        let child = Self::from_details_nested(child, &inner.details);
        Self {
            field: parent,
            reason: format!(
                "embedded message failed validation | caused by: invalid {}: {}",
                child.field, child.reason
            ),
            kind: child.kind,
        }
    }

    fn from_string(field: String, error: &string::Error) -> Self {
        let (reason, kind) = match error {
            string::Error::Const(value) => (
                format!("value must be equal to \"{value}\""),
                ValidationKind::Other,
            ),
            string::Error::Len(length) => (
                format!("value length must be {length} characters"),
                ValidationKind::Other,
            ),
            string::Error::MinLen(minimum) => (
                format!("value length must be at least {minimum} characters"),
                ValidationKind::Other,
            ),
            string::Error::MaxLen(maximum) => (
                format!("value length must be at most {maximum} characters"),
                ValidationKind::Other,
            ),
            string::Error::LenBytes(length) => (
                format!("value length must be {length} bytes"),
                ValidationKind::Other,
            ),
            string::Error::MinLenBytes(minimum) => (
                if field.ends_with("ListObjectsRequest.User")
                    || field.ends_with("StreamedListObjectsRequest.User")
                {
                    format!("value length must be between {minimum} and 512 bytes, inclusive")
                } else {
                    format!("value length must be at least {minimum} bytes")
                },
                ValidationKind::Other,
            ),
            string::Error::MaxLenBytes(maximum) => (
                format!("value length must be at most {maximum} bytes"),
                ValidationKind::StringMaximumBytes,
            ),
            string::Error::Pattern(pattern) => (
                format!(
                    "value does not match regex pattern \"{}\"",
                    pattern.replace('\\', "\\\\")
                ),
                ValidationKind::StringPattern,
            ),
            string::Error::In(values) => (
                format_string_list("value must be in list", values),
                ValidationKind::Other,
            ),
            string::Error::NotIn(values) => (
                format_string_list("value must not be in list", values),
                ValidationKind::Other,
            ),
            error => (format!("value {error}"), ValidationKind::Other),
        };
        Self {
            field,
            reason,
            kind,
        }
    }
}

fn protobuf_field_path(field: &str) -> String {
    let mut parts = field.rsplitn(2, '.');
    let field_name = parts.next().unwrap_or(field);
    let message_name = parts
        .next()
        .and_then(|path| path.rsplit('.').next())
        .unwrap_or_default();
    if message_name.is_empty() {
        pascal_case(field_name)
    } else {
        format!("{message_name}.{}", pascal_case(field_name))
    }
}

fn pascal_case(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut uppercase = true;
    for character in value.chars() {
        if character == '_' {
            uppercase = true;
        } else if uppercase {
            output.extend(character.to_uppercase());
            uppercase = false;
        } else {
            output.push(character);
        }
    }
    output
}

fn validation_code(violation: &ValidationViolation) -> &'static str {
    let caused_field = violation
        .reason
        .rsplit_once("caused by: invalid ")
        .map(|(_, cause)| cause)
        .and_then(|cause| cause.split_once(':').map(|(field, _)| field));
    let field = caused_field.unwrap_or(&violation.field);
    let leaf = field.rsplit('.').next().unwrap_or(field);
    if leaf.starts_with("Relations[") {
        return match &violation.kind {
            ValidationKind::StringMaximumBytes => "relations_too_long",
            ValidationKind::StringPattern => "relations_invalid_pattern",
            _ => "validation_error",
        };
    }
    match (leaf, &violation.kind) {
        ("Assertions", ValidationKind::ListMaximum) => "assertions_too_many_items",
        ("AuthorizationModelId", ValidationKind::StringMaximumBytes) => {
            "authorization_model_id_too_long"
        }
        ("Base", ValidationKind::MessageRequired) => "difference_base_missing_value",
        ("Id", ValidationKind::StringMaximumBytes) => "id_too_long",
        ("Object", ValidationKind::StringMaximumBytes) => "object_too_long",
        ("PageSize", _) if !field.starts_with("ReadChangesRequest.") => "page_size_invalid",
        ("Params", ValidationKind::MessageRequired) => "param_missing_value",
        ("Relation", ValidationKind::StringMaximumBytes) => "relation_too_long",
        ("Relations", ValidationKind::ListMinimum) => "relations_too_few_items",
        ("Subtract", ValidationKind::MessageRequired) => "subtract_base_missing_value",
        ("StoreId", ValidationKind::StringMaximumBytes) => "store_id_invalid_length",
        ("TupleKey", ValidationKind::MessageRequired) => "tuple_key_value_not_specified",
        ("TupleKeys", _) if violation.reason.starts_with("value must contain between") => {
            "tuple_keys_too_many_or_too_few_items"
        }
        ("Type", ValidationKind::StringMaximumBytes) => "type_invalid_length",
        ("Type", ValidationKind::StringPattern) => "type_invalid_pattern",
        ("TypeDefinitions", ValidationKind::ListMinimum) => "type_definitions_too_few_items",
        _ => "validation_error",
    }
}

fn format_string_list(prefix: &str, values: &[String]) -> String {
    format!("{prefix} [{}]", values.join(" "))
}

fn format_number_list<T: fmt::Display>(prefix: &str, values: &[T]) -> String {
    let values = values.iter().map(ToString::to_string).collect::<Vec<_>>();
    format!("{prefix} [{}]", values.join(" "))
}

fn int32_reason(error: &validation::int32::Error) -> String {
    match error {
        validation::int32::Error::Const(value) => format!("value must be equal to {value}"),
        validation::int32::Error::Lt(value) => format!("value must be less than {value}"),
        validation::int32::Error::Lte(value) => {
            format!("value must be less than or equal to {value}")
        }
        validation::int32::Error::Gt(value) => format!("value must be greater than {value}"),
        validation::int32::Error::Gte(value) => {
            format!("value must be greater than or equal to {value}")
        }
        validation::int32::Error::InRange(start, minimum, maximum, end) => {
            format!("value must be inside range {start}{minimum}, {maximum}{end}")
        }
        validation::int32::Error::NotInRange(start, minimum, maximum, end) => {
            format!("value must be outside range {start}{minimum}, {maximum}{end}")
        }
        validation::int32::Error::In(values) => format_number_list("value must be in list", values),
        validation::int32::Error::NotIn(values) => {
            format_number_list("value must not be in list", values)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error};

    use openfga_proto::openfga::v1 as pb;
    use tonic::Status;

    use super::ApiError;

    const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    fn status(error: ApiError) -> Status {
        Status::from(error)
    }

    #[test]
    fn test_should_match_pinned_page_size_validation() -> Result<(), Box<dyn Error>> {
        let Err(error) = ApiError::validate(&pb::ListStoresRequest {
            page_size: Some(pbjson_types::Int32Value { value: 0 }),
            continuation_token: String::new(),
            name: String::new(),
        }) else {
            return Err("zero page size unexpectedly passed validation".into());
        };
        assert_eq!(error.code(), "page_size_invalid");
        assert_eq!(
            status(error).message(),
            "invalid ListStoresRequest.PageSize: value must be inside range [1, 100]"
        );
        Ok(())
    }

    #[test]
    fn test_should_match_pinned_ordered_multi_violation_validation() -> Result<(), Box<dyn Error>> {
        let Err(error) = ApiError::validate(&pb::ListStoresRequest {
            page_size: Some(pbjson_types::Int32Value { value: 0 }),
            continuation_token: "!".to_owned(),
            name: "x".to_owned(),
        }) else {
            return Err("invalid list stores request unexpectedly passed validation".into());
        };
        assert_eq!(error.code(), "page_size_invalid");
        assert_eq!(
            status(error).message(),
            "invalid ListStoresRequest.PageSize: value must be inside range [1, 100]; invalid \
             ListStoresRequest.ContinuationToken: value does not match regex pattern \
             \"^$|^[A-Za-z0-9-_]+={0,2}$\"; invalid ListStoresRequest.Name: value does not match \
             regex pattern \"^[a-zA-Z0-9\\\\s\\\\.\\\\-\\\\/^_&@]{3,64}$\""
        );
        Ok(())
    }

    #[test]
    fn test_should_preserve_multiple_rules_violated_by_one_original_field()
    -> Result<(), Box<dyn Error>> {
        let Err(error) = ApiError::validate(&pb::ListStoresRequest {
            page_size: None,
            continuation_token: "!".repeat(5_121),
            name: String::new(),
        }) else {
            return Err("oversized invalid token unexpectedly passed validation".into());
        };
        assert_eq!(error.code(), "validation_error");
        assert_eq!(
            status(error).message(),
            "invalid ListStoresRequest.ContinuationToken: value length must be at most 5120 \
             bytes; invalid ListStoresRequest.ContinuationToken: value does not match regex \
             pattern \"^$|^[A-Za-z0-9-_]+={0,2}$\""
        );
        Ok(())
    }

    #[test]
    fn test_should_validate_invalid_tail_items_after_an_oversized_list()
    -> Result<(), Box<dyn Error>> {
        let valid = pb::TupleKey {
            user: "user:anne".to_owned(),
            relation: "viewer".to_owned(),
            object: "document:roadmap".to_owned(),
            condition: None,
        };
        let mut contextual_tuples = vec![valid; 20];
        contextual_tuples.push(pb::TupleKey {
            user: "x".repeat(513),
            relation: "#".to_owned(),
            object: "x".to_owned(),
            condition: None,
        });
        let Err(error) = ApiError::validate(&pb::Assertion {
            tuple_key: Some(pb::AssertionTupleKey {
                object: "document:roadmap".to_owned(),
                relation: "viewer".to_owned(),
                user: "user:anne".to_owned(),
            }),
            expectation: true,
            contextual_tuples,
            context: None,
        }) else {
            return Err("oversized list with invalid tail unexpectedly passed validation".into());
        };
        let message = status(error).message().to_owned();
        assert!(message.contains("no more than 20 item(s)"), "{message}");
        assert!(message.contains("invalid TupleKey.User:"), "{message}");
        assert!(message.contains("invalid TupleKey.Relation:"), "{message}");
        assert!(message.contains("invalid TupleKey.Object:"), "{message}");
        Ok(())
    }

    #[test]
    fn test_should_match_pinned_grpc_nested_multi_violation_validation()
    -> Result<(), Box<dyn Error>> {
        let Err(error) = ApiError::validate_grpc(&pb::CheckRequest {
            store_id: STORE_ID.to_owned(),
            tuple_key: Some(pb::CheckRequestTupleKey {
                user: "x".to_owned(),
                relation: "#".to_owned(),
                object: "x".to_owned(),
            }),
            authorization_model_id: "short".to_owned(),
            ..pb::CheckRequest::default()
        }) else {
            return Err("invalid Check request unexpectedly passed validation".into());
        };
        assert_eq!(error.code(), "validation_error");
        assert_eq!(
            status(error).message(),
            "invalid CheckRequest.TupleKey: embedded message failed validation | caused by: \
             invalid CheckRequestTupleKey.User: value does not match regex pattern \
             \"^[^\\\\s]{2,512}$\"; invalid CheckRequestTupleKey.Relation: value does not match \
             regex pattern \"^[^:#@\\\\s]{1,50}$\"; invalid CheckRequestTupleKey.Object: value \
             does not match regex pattern \"^[^\\\\s]{2,256}$\"; invalid \
             CheckRequest.AuthorizationModelId: value does not match regex pattern \
             \"^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$\""
        );
        Ok(())
    }

    #[test]
    fn test_should_match_pinned_missing_tuple_validation() -> Result<(), Box<dyn Error>> {
        let Err(error) = ApiError::validate(&pb::CheckRequest {
            store_id: STORE_ID.to_owned(),
            ..pb::CheckRequest::default()
        }) else {
            return Err("missing tuple unexpectedly passed validation".into());
        };
        assert_eq!(error.code(), "tuple_key_value_not_specified");
        assert_eq!(
            status(error).message(),
            "invalid CheckRequest.TupleKey: value is required"
        );
        Ok(())
    }

    #[test]
    fn test_should_match_pinned_multi_field_model_validation() -> Result<(), Box<dyn Error>> {
        let Err(error) =
            ApiError::validate_write_authorization_model(&pb::WriteAuthorizationModelRequest {
                store_id: STORE_ID.to_owned(),
                ..pb::WriteAuthorizationModelRequest::default()
            })
        else {
            return Err("empty model unexpectedly passed validation".into());
        };
        assert_eq!(error.code(), "type_definitions_too_few_items");
        assert_eq!(
            status(error).message(),
            "invalid WriteAuthorizationModelRequest.TypeDefinitions: value must contain at least \
             1 item(s); invalid WriteAuthorizationModelRequest.SchemaVersion: value must be in \
             list [1.0 1.1 1.2]"
        );
        Ok(())
    }

    #[test]
    fn test_should_match_pinned_multi_field_enumeration_validation() -> Result<(), Box<dyn Error>> {
        let request = pb::ListObjectsRequest {
            store_id: STORE_ID.to_owned(),
            ..pb::ListObjectsRequest::default()
        };
        assert!(ApiError::validate(&request).is_err());
        let Err(error) = ApiError::validate_list_objects(&request) else {
            return Err("empty list objects request unexpectedly passed validation".into());
        };
        assert_eq!(error.code(), "type_invalid_pattern");
        assert_eq!(
            status(error).message(),
            "invalid ListObjectsRequest.Type: value does not match regex pattern \
             \"^[^:#@\\\\s]{1,254}$\"; invalid ListObjectsRequest.Relation: value does not match \
             regex pattern \"^[^:#@\\\\s]{1,50}$\"; invalid ListObjectsRequest.User: value length \
             must be between 1 and 512 bytes, inclusive"
        );
        Ok(())
    }

    #[test]
    fn test_should_render_equal_repeated_bounds_as_exact_length() -> Result<(), Box<dyn Error>> {
        for count in [0, 2] {
            let request = pb::ListUsersRequest {
                store_id: STORE_ID.to_owned(),
                object: Some(pb::Object {
                    r#type: "document".to_owned(),
                    id: "roadmap".to_owned(),
                }),
                relation: "viewer".to_owned(),
                user_filters: vec![
                    pb::UserTypeFilter {
                        r#type: "user".to_owned(),
                        relation: String::new(),
                    };
                    count
                ],
                ..pb::ListUsersRequest::default()
            };
            let Err(error) = ApiError::validate_list_users(&request) else {
                return Err(format!("{count} user filters unexpectedly passed validation").into());
            };
            assert_eq!(
                status(error).message(),
                "invalid ListUsersRequest.UserFilters: value must contain exactly 1 item(s)"
            );
        }
        Ok(())
    }

    #[test]
    fn test_should_render_combined_string_byte_bounds_as_one_range() -> Result<(), Box<dyn Error>> {
        for user in [String::new(), "x".repeat(513)] {
            let request = pb::ListObjectsRequest {
                store_id: STORE_ID.to_owned(),
                r#type: "document".to_owned(),
                relation: "viewer".to_owned(),
                user,
                ..pb::ListObjectsRequest::default()
            };
            let Err(error) = ApiError::validate_list_objects(&request) else {
                return Err("out-of-range user unexpectedly passed validation".into());
            };
            assert_eq!(
                status(error).message(),
                "invalid ListObjectsRequest.User: value length must be between 1 and 512 bytes, \
                 inclusive"
            );
        }
        Ok(())
    }

    #[test]
    fn test_should_recurse_into_message_values_without_container_rules()
    -> Result<(), Box<dyn Error>> {
        let request = pb::WriteAuthorizationModelRequest {
            store_id: STORE_ID.to_owned(),
            type_definitions: vec![pb::TypeDefinition {
                r#type: "document".to_owned(),
                relations: HashMap::from([(
                    "viewer".to_owned(),
                    pb::Userset {
                        userset: Some(pb::userset::Userset::This(pb::DirectUserset {})),
                    },
                )]),
                metadata: Some(pb::Metadata {
                    relations: HashMap::from([(
                        "viewer".to_owned(),
                        pb::RelationMetadata {
                            module: ":".to_owned(),
                            ..pb::RelationMetadata::default()
                        },
                    )]),
                    ..pb::Metadata::default()
                }),
            }],
            schema_version: "1.1".to_owned(),
            conditions: HashMap::new(),
        };
        let Err(error) = ApiError::validate_write_authorization_model(&request) else {
            return Err("invalid nested metadata unexpectedly passed validation".into());
        };
        assert_eq!(error.code(), "validation_error");
        let message = status(error).message().to_owned();
        assert!(
            message.contains("invalid RelationMetadata.Module:"),
            "{message}",
        );
        Ok(())
    }

    #[test]
    fn test_should_sort_and_render_map_key_violations_at_the_parent_path()
    -> Result<(), Box<dyn Error>> {
        let condition = pb::Condition {
            name: "cond1".to_owned(),
            expression: "true".to_owned(),
            parameters: HashMap::new(),
            metadata: None,
        };
        let request = pb::WriteAuthorizationModelRequest {
            store_id: STORE_ID.to_owned(),
            type_definitions: vec![pb::TypeDefinition {
                r#type: "user".to_owned(),
                ..pb::TypeDefinition::default()
            }],
            schema_version: "1.1".to_owned(),
            conditions: HashMap::from([
                ("@bad".to_owned(), condition.clone()),
                ("#bad".to_owned(), condition),
            ]),
        };
        let Err(error) = ApiError::validate(&request) else {
            return Err("invalid condition keys unexpectedly passed validation".into());
        };
        assert_eq!(
            status(error).message(),
            "invalid WriteAuthorizationModelRequest.Conditions[#bad]: value does not match regex \
             pattern \"^[^:#@\\\\s]{1,50}$\"; invalid \
             WriteAuthorizationModelRequest.Conditions[@bad]: value does not match regex pattern \
             \"^[^:#@\\\\s]{1,50}$\""
        );
        Ok(())
    }

    #[test]
    fn test_should_apply_ascii_re2_classes_to_unicode_correlation_ids() -> Result<(), Box<dyn Error>>
    {
        let request = pb::BatchCheckRequest {
            store_id: STORE_ID.to_owned(),
            checks: vec![pb::BatchCheckItem {
                tuple_key: Some(pb::CheckRequestTupleKey {
                    user: "user:anne".to_owned(),
                    relation: "viewer".to_owned(),
                    object: "document:roadmap".to_owned(),
                }),
                correlation_id: "é".to_owned(),
                ..pb::BatchCheckItem::default()
            }],
            authorization_model_id: STORE_ID.to_owned(),
            consistency: pb::ConsistencyPreference::Unspecified as i32,
        };
        let Err(error) = ApiError::validate_grpc(&request) else {
            return Err("Unicode correlation ID unexpectedly passed validation".into());
        };
        assert_eq!(
            status(error).message(),
            "invalid BatchCheckRequest.Checks[0]: embedded message failed validation | caused by: \
             invalid BatchCheckItem.CorrelationId: value does not match regex pattern \
             \"^[\\\\w\\\\d-]{1,36}$\""
        );
        Ok(())
    }

    #[test]
    fn test_should_reject_invalid_protobuf_timestamps_before_lt_now() -> Result<(), Box<dyn Error>>
    {
        for (timestamp, cause) in [
            (
                pbjson_types::Timestamp {
                    seconds: 253_402_300_800,
                    nanos: 0,
                },
                "timestamp (seconds:253402300800) after 9999-12-31",
            ),
            (
                pbjson_types::Timestamp {
                    seconds: 0,
                    nanos: -1,
                },
                "timestamp (nanos:-1) has out-of-range nanos",
            ),
        ] {
            let request = pb::ReadChangesRequest {
                store_id: STORE_ID.to_owned(),
                start_time: Some(timestamp),
                ..pb::ReadChangesRequest::default()
            };
            let Err(error) = ApiError::validate_grpc(&request) else {
                return Err("invalid timestamp unexpectedly passed validation".into());
            };
            assert_eq!(
                status(error).message(),
                format!(
                    "invalid ReadChangesRequest.StartTime: value is not a valid timestamp | \
                     caused by: proto: {cause}"
                )
            );
        }
        Ok(())
    }
}
