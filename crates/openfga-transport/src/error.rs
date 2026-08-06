//! Exhaustive, redacted wire-error mapping shared by both transports.

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE},
    response::{IntoResponse, Response},
};
use openfga_auth::AuthenticationError;
use openfga_service::{ServiceError, ServiceErrorKind};
use serde::Serialize;
use tonic::Code;

/// A safe `OpenFGA` protocol failure.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct ApiError {
    http_status: StatusCode,
    grpc_code: Code,
    code: &'static str,
    message: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

impl ApiError {
    /// Creates a request validation failure without retaining hostile input.
    #[must_use]
    pub const fn invalid_request() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            "validation_error",
            "the request is invalid",
        )
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
            message,
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
        match error.kind() {
            ServiceErrorKind::StoreNotFound => Self::new(
                StatusCode::NOT_FOUND,
                Code::NotFound,
                "store_id_not_found",
                "store not found",
            ),
            ServiceErrorKind::ModelNotFound => Self::new(
                StatusCode::BAD_REQUEST,
                Code::InvalidArgument,
                "authorization_model_not_found",
                "authorization model not found",
            ),
            ServiceErrorKind::AlreadyExists => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::AlreadyExists,
                "already_exists",
                "the resource already exists",
            ),
            ServiceErrorKind::Conflict => Self::new(
                StatusCode::CONFLICT,
                Code::Aborted,
                "Aborted",
                "the request conflicts with current state",
            ),
            ServiceErrorKind::InvalidContinuation => Self::invalid_continuation(),
            ServiceErrorKind::InvalidRequest | ServiceErrorKind::Condition => {
                Self::invalid_request()
            }
            ServiceErrorKind::ResourceExhausted => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::ResourceExhausted,
                "resource_exhausted",
                "a request resource limit was exceeded",
            ),
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
                "the request deadline elapsed",
            ),
            ServiceErrorKind::Cancelled => Self::new(
                StatusCode::BAD_REQUEST,
                Code::Cancelled,
                "cancelled",
                "the request was cancelled",
            ),
            ServiceErrorKind::Internal => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::Internal,
                "internal_error",
                "an internal error occurred",
            ),
        }
    }
}

impl From<ApiError> for tonic::Status {
    fn from(error: ApiError) -> Self {
        Self::new(
            error.grpc_code,
            format!("{}: {}", error.code, error.message),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let unauthenticated = self.http_status == StatusCode::UNAUTHORIZED;
        let mut response = (
            self.http_status,
            Json(ErrorBody {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response();
        if unauthenticated {
            response
                .headers_mut()
                .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}
