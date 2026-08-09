//! Bounded transport policy and service assembly.

use std::{fmt, num::NonZeroU32, sync::Arc, time::Duration};

use axum::http::Uri;
use openfga_auth::AuthorizationPolicy;
use openfga_domain::{InputLimits, RequestTimeout, TokenCodec};
use openfga_service::{
    AssertionService, ChangeService, CheckService, ExpandService, ListObjectsService,
    ListUsersService, ModelService, StoreService, TupleService,
};
use typed_builder::TypedBuilder;

use crate::AdmissionPolicy;

const MAXIMUM_MESSAGE_BYTES: usize = 16 * 1_024 * 1_024;
const MAXIMUM_CONCURRENCY: usize = 65_536;
const MAXIMUM_TOKEN_TTL: Duration = Duration::from_hours(720);
const MAXIMUM_PAGE_SIZE: u32 = 100;
const MAXIMUM_AUTHZEN_BASE_URL_BYTES: usize = 2_048;

/// Validated `AuthZEN` endpoint and discovery policy.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct AuthZenConfig {
    enabled: bool,
    base_url: Option<Arc<str>>,
}

impl AuthZenConfig {
    /// Creates a disabled `AuthZEN` surface.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            base_url: None,
        }
    }

    /// Creates an `AuthZEN` policy and validates the canonical discovery base URL when present.
    ///
    /// The base URL is never inferred from request headers. An enabled API may omit it, in which
    /// case evaluation/search work normally and the discovery endpoint returns a failed
    /// precondition until an operator supplies the URL.
    ///
    /// # Errors
    ///
    /// Returns a stable diagnostic when the URL is not an absolute HTTP(S) origin with an
    /// optional path prefix, or when it contains credentials, a query, or a fragment.
    pub fn new(enabled: bool, base_url: Option<&str>) -> Result<Self, &'static str> {
        let base_url = base_url
            .filter(|value| !value.is_empty())
            .map(validate_authzen_base_url)
            .transpose()?
            .map(Arc::from);
        Ok(Self { enabled, base_url })
    }

    /// Returns whether `AuthZEN` handlers are enabled.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the operator-configured canonical discovery base URL.
    #[must_use]
    pub fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }
}

impl fmt::Debug for AuthZenConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthZenConfig")
            .field("enabled", &self.enabled)
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

fn validate_authzen_base_url(value: &str) -> Result<String, &'static str> {
    if value.len() > MAXIMUM_AUTHZEN_BASE_URL_BYTES || value.trim() != value {
        return Err("authzen_base_url_invalid");
    }
    let uri = value
        .parse::<Uri>()
        .map_err(|_| "authzen_base_url_invalid")?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.authority().is_none()
        || uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        || uri
            .path_and_query()
            .is_some_and(|value| value.query().is_some())
    {
        return Err("authzen_base_url_invalid");
    }
    let canonical = value.trim_end_matches('/');
    if canonical.is_empty() {
        return Err("authzen_base_url_invalid");
    }
    Ok(canonical.to_owned())
}

/// Complete service set consumed by the `OpenFGA` transport adapters.
#[derive(Clone, Debug, TypedBuilder)]
pub struct OpenFgaServices {
    /// Store lifecycle use cases.
    pub(crate) stores: StoreService,
    /// Authorization-model use cases.
    pub(crate) models: ModelService,
    /// Assertion use cases.
    pub(crate) assertions: AssertionService,
    /// Relationship-tuple use cases.
    pub(crate) tuples: TupleService,
    /// Tuple-changelog use cases.
    pub(crate) changes: ChangeService,
    /// Check and `BatchCheck` use cases.
    pub(crate) checks: CheckService,
    /// Unary and streaming `ListObjects` use cases.
    pub(crate) list_objects: ListObjectsService,
    /// `ListUsers` use cases.
    pub(crate) list_users: ListUsersService,
    /// Diagnostic `Expand` use cases.
    pub(crate) expand: ExpandService,
}

/// Validated finite policy shared by HTTP and gRPC adapters.
#[derive(Clone, TypedBuilder)]
pub struct TransportConfig {
    /// Domain input ceilings.
    pub(crate) limits: InputLimits,
    /// Default-deny store/action authorization policy.
    pub(crate) authorization_policy: Arc<AuthorizationPolicy>,
    /// Rotating continuation-token codec.
    pub(crate) token_codec: Arc<TokenCodec>,
    /// Default number of records returned by list methods.
    #[builder(default = trusted_page_size(50))]
    pub(crate) default_page_size: NonZeroU32,
    /// Maximum request duration.
    pub(crate) request_timeout: RequestTimeout,
    /// Continuation-token lifetime.
    #[builder(default = Duration::from_hours(24))]
    pub(crate) token_ttl: Duration,
    /// Maximum accepted HTTP body or gRPC message size.
    #[builder(default = 1_048_576)]
    pub(crate) maximum_message_bytes: usize,
    /// Maximum concurrent requests admitted by the HTTP adapter.
    #[builder(default = 1_024)]
    pub(crate) maximum_concurrency: usize,
    /// Authentication and per-principal endpoint rate policy.
    #[builder(default = AdmissionPolicy::builder().build())]
    pub(crate) admission_policy: AdmissionPolicy,
    /// `AuthZEN` feature and canonical discovery metadata policy.
    #[builder(default = AuthZenConfig::disabled())]
    pub(crate) authzen: AuthZenConfig,
}

const fn trusted_page_size(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => NonZeroU32::MIN,
    }
}

impl TransportConfig {
    /// Validates relationships among individually bounded transport settings.
    ///
    /// # Errors
    ///
    /// Returns a static diagnostic code when a transport ceiling is invalid.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.default_page_size.get() > self.limits.results()
            || self.default_page_size.get() > MAXIMUM_PAGE_SIZE
        {
            return Err("default_page_size_exceeds_endpoint_limit");
        }
        if self.token_ttl.as_secs() == 0 || self.token_ttl > MAXIMUM_TOKEN_TTL {
            return Err("token_ttl_out_of_range");
        }
        if !(1..=MAXIMUM_MESSAGE_BYTES).contains(&self.maximum_message_bytes) {
            return Err("maximum_message_bytes_out_of_range");
        }
        if !(1..=MAXIMUM_CONCURRENCY).contains(&self.maximum_concurrency) {
            return Err("maximum_concurrency_out_of_range");
        }
        self.admission_policy.validate()?;
        Ok(())
    }
}

impl fmt::Debug for TransportConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportConfig")
            .field("limits", &self.limits)
            .field("authorization_policy", &self.authorization_policy)
            .field("token_codec", &"[REDACTED]")
            .field("default_page_size", &self.default_page_size)
            .field("request_timeout", &self.request_timeout)
            .field("token_ttl", &self.token_ttl)
            .field("maximum_message_bytes", &self.maximum_message_bytes)
            .field("maximum_concurrency", &self.maximum_concurrency)
            .field("admission_policy", &self.admission_policy)
            .field("authzen", &self.authzen)
            .finish_non_exhaustive()
    }
}
