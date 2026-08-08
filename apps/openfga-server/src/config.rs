//! Bounded YAML configuration loading and validation.

use std::{
    collections::{BTreeSet, HashSet},
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use config::{Case, Config, Environment, File, FileFormat};
use openfga_auth::{
    Action, AuthorizationPolicy, OidcAlgorithm, OidcConfig, PolicyBinding, StoreScope,
};
use openfga_cache::{
    DecisionCacheConfig, InvalidationControllerConfig, ModelCacheConfig, TupleCacheConfig,
};
use openfga_domain::{Limit, PrincipalId, RequestTimeout, StoreId, TokenKeyId};
use openfga_transport::AdmissionPolicy;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

const MAXIMUM_CONFIG_BYTES: u64 = 1_048_576;
const MAXIMUM_ENVIRONMENT_NAME_BYTES: usize = 128;
const MAXIMUM_OTLP_ENDPOINT_BYTES: usize = 2_048;
const MAXIMUM_SHUTDOWN_DURATION: Duration = Duration::from_mins(5);
const MAXIMUM_HEALTH_INTERVAL: Duration = Duration::from_mins(1);
const MAXIMUM_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);
const MAXIMUM_AUTH_KEYS: usize = 32;
const MAXIMUM_TOKEN_KEYS: usize = 16;
const MAXIMUM_PAGE_SIZE: u32 = 100;
const MAXIMUM_POLICY_BINDINGS: usize = 1_024;
pub(crate) const DEVELOPMENT_PRINCIPAL_ID: &str = "openfga-development-runtime";

/// Fully validated server configuration. No secret values are retained here.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ServerConfig {
    pub(crate) profile: Profile,
    pub(crate) listeners: ListenerConfig,
    pub(crate) tls: TlsConfig,
    pub(crate) storage: StorageConfig,
    pub(crate) cache: CacheConfig,
    pub(crate) auth: AuthConfig,
    pub(crate) transport: TransportPolicy,
    pub(crate) evaluator: EvaluatorPolicy,
    pub(crate) list_objects: ListObjectsPolicy,
    pub(crate) list_users: ListUsersPolicy,
    pub(crate) expand: ExpandPolicy,
    pub(crate) telemetry: TelemetryConfig,
    pub(crate) shutdown: ShutdownConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum Profile {
    Development,
    Production,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ListenerConfig {
    pub(crate) http: SocketAddr,
    pub(crate) grpc: SocketAddr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TlsConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) certificate_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) private_key_path: Option<PathBuf>,
    #[serde(default = "default_tls_reload_interval_seconds")]
    pub(crate) reload_interval_seconds: u64,
}

impl TlsConfig {
    pub(crate) const fn reload_interval(&self) -> Duration {
        Duration::from_secs(self.reload_interval_seconds)
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            certificate_path: None,
            private_key_path: None,
            reload_interval_seconds: default_tls_reload_interval_seconds(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum StorageBackend {
    Memory,
    Postgres,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct StorageConfig {
    pub(crate) backend: StorageBackend,
    #[serde(default)]
    pub(crate) memory: MemoryConfig,
    #[serde(default)]
    pub(crate) postgres: PostgresConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MemoryConfig {
    #[serde(default = "default_actor_capacity")]
    pub(crate) actor_capacity: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            actor_capacity: default_actor_capacity(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PostgresConfig {
    #[serde(default = "default_primary_url_env")]
    pub(crate) primary_url_env: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) replica_url_env: Option<String>,
    #[serde(default = "default_max_connections")]
    pub(crate) max_connections: u32,
    #[serde(default)]
    pub(crate) min_connections: u32,
    #[serde(default = "default_database_timeout_ms")]
    pub(crate) acquire_timeout_ms: u64,
    #[serde(default = "default_database_timeout_ms")]
    pub(crate) statement_timeout_ms: u64,
    #[serde(default = "default_replica_lag_ms")]
    pub(crate) replica_max_lag_ms: u64,
    #[serde(default = "default_tuple_mutations")]
    pub(crate) max_tuple_mutations: u32,
    #[serde(default)]
    pub(crate) migrate_on_start: bool,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            primary_url_env: default_primary_url_env(),
            replica_url_env: None,
            max_connections: default_max_connections(),
            min_connections: 0,
            acquire_timeout_ms: default_database_timeout_ms(),
            statement_timeout_ms: default_database_timeout_ms(),
            replica_max_lag_ms: default_replica_lag_ms(),
            max_tuple_mutations: default_tuple_mutations(),
            migrate_on_start: false,
        }
    }
}

/// Finite in-process cache policy.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CacheConfig {
    #[serde(default)]
    pub(crate) model: ModelCachePolicy,
    #[serde(default)]
    pub(crate) decision: DecisionCachePolicy,
    #[serde(default)]
    pub(crate) tuple: TupleCachePolicy,
    #[serde(default)]
    pub(crate) controller: CacheControllerPolicy,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ModelCachePolicy {
    #[serde(default = "default_model_source_weight")]
    pub(crate) source_weight: u64,
    #[serde(default = "default_model_compiled_weight")]
    pub(crate) compiled_weight: u64,
    #[serde(default = "default_model_aliases")]
    pub(crate) latest_aliases: u64,
    #[serde(default = "default_model_immutable_ttl_seconds")]
    pub(crate) immutable_ttl_seconds: u64,
    #[serde(default = "default_model_alias_ttl_seconds")]
    pub(crate) latest_alias_ttl_seconds: u64,
}

impl Default for ModelCachePolicy {
    fn default() -> Self {
        Self {
            source_weight: default_model_source_weight(),
            compiled_weight: default_model_compiled_weight(),
            latest_aliases: default_model_aliases(),
            immutable_ttl_seconds: default_model_immutable_ttl_seconds(),
            latest_alias_ttl_seconds: default_model_alias_ttl_seconds(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DecisionCachePolicy {
    #[serde(default = "default_decision_weight")]
    pub(crate) weight: u64,
    #[serde(default = "default_mutable_cache_ttl_seconds")]
    pub(crate) ttl_seconds: u64,
}

impl Default for DecisionCachePolicy {
    fn default() -> Self {
        Self {
            weight: default_decision_weight(),
            ttl_seconds: default_mutable_cache_ttl_seconds(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TupleCachePolicy {
    #[serde(default = "default_tuple_weight")]
    pub(crate) weight: u64,
    #[serde(default = "default_tuple_cache_results")]
    pub(crate) maximum_results: usize,
    #[serde(default = "default_mutable_cache_ttl_seconds")]
    pub(crate) ttl_seconds: u64,
}

impl Default for TupleCachePolicy {
    fn default() -> Self {
        Self {
            weight: default_tuple_weight(),
            maximum_results: default_tuple_cache_results(),
            ttl_seconds: default_mutable_cache_ttl_seconds(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CacheControllerPolicy {
    #[serde(default = "default_cache_controller_capacity")]
    pub(crate) channel_capacity: usize,
    #[serde(default = "default_cache_controller_page_size")]
    pub(crate) page_size: u32,
    #[serde(default = "default_cache_controller_poll_ms")]
    pub(crate) poll_interval_ms: u64,
    #[serde(default = "default_cache_controller_read_ms")]
    pub(crate) read_timeout_ms: u64,
    #[serde(default = "default_cache_controller_lag_ms")]
    pub(crate) maximum_lag_ms: u64,
}

impl Default for CacheControllerPolicy {
    fn default() -> Self {
        Self {
            channel_capacity: default_cache_controller_capacity(),
            page_size: default_cache_controller_page_size(),
            poll_interval_ms: default_cache_controller_poll_ms(),
            read_timeout_ms: default_cache_controller_read_ms(),
            maximum_lag_ms: default_cache_controller_lag_ms(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum AuthMode {
    Disabled,
    Preshared,
    Oidc,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AuthConfig {
    pub(crate) mode: AuthMode,
    #[serde(default)]
    pub(crate) preshared: PresharedAuthConfig,
    #[serde(default)]
    pub(crate) oidc: OidcAuthConfig,
    #[serde(default)]
    pub(crate) authorization: AuthorizationConfig,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PresharedAuthConfig {
    #[serde(default)]
    pub(crate) keys: Vec<PresharedKeyConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PresharedKeyConfig {
    pub(crate) id: String,
    pub(crate) key_env: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct OidcAuthConfig {
    #[serde(default)]
    pub(crate) issuer: String,
    #[serde(default)]
    pub(crate) audiences: Vec<String>,
    #[serde(default)]
    pub(crate) authorized_parties: Vec<String>,
    #[serde(default = "default_oidc_algorithms")]
    pub(crate) algorithms: Vec<OidcAlgorithm>,
    #[serde(default)]
    pub(crate) allowed_hosts: Vec<String>,
    #[serde(default = "default_oidc_token_bytes")]
    pub(crate) maximum_token_bytes: usize,
    #[serde(default = "default_oidc_document_bytes")]
    pub(crate) maximum_document_bytes: usize,
    #[serde(default = "default_oidc_fetch_timeout_ms")]
    pub(crate) fetch_timeout_ms: u64,
    #[serde(default = "default_oidc_clock_skew_seconds")]
    pub(crate) clock_skew_seconds: u64,
    #[serde(default = "default_oidc_refresh_seconds")]
    pub(crate) refresh_interval_seconds: u64,
    #[serde(default = "default_oidc_stale_seconds")]
    pub(crate) stale_key_grace_seconds: u64,
}

impl Default for OidcAuthConfig {
    fn default() -> Self {
        Self {
            issuer: String::new(),
            audiences: Vec::new(),
            authorized_parties: Vec::new(),
            algorithms: default_oidc_algorithms(),
            allowed_hosts: Vec::new(),
            maximum_token_bytes: default_oidc_token_bytes(),
            maximum_document_bytes: default_oidc_document_bytes(),
            fetch_timeout_ms: default_oidc_fetch_timeout_ms(),
            clock_skew_seconds: default_oidc_clock_skew_seconds(),
            refresh_interval_seconds: default_oidc_refresh_seconds(),
            stale_key_grace_seconds: default_oidc_stale_seconds(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AuthorizationConfig {
    #[serde(default)]
    pub(crate) bindings: Vec<AuthorizationBindingConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AuthorizationBindingConfig {
    pub(crate) principal: String,
    pub(crate) actions: Vec<Action>,
    pub(crate) stores: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TransportPolicy {
    #[serde(default = "default_page_size")]
    pub(crate) default_page_size: u32,
    #[serde(default = "default_request_timeout_ms")]
    pub(crate) request_timeout_ms: u64,
    #[serde(default = "default_token_ttl_seconds")]
    pub(crate) token_ttl_seconds: u64,
    #[serde(default = "default_message_bytes")]
    pub(crate) maximum_message_bytes: usize,
    #[serde(default = "default_concurrency")]
    pub(crate) maximum_concurrency: usize,
    #[serde(default = "default_token_key_id")]
    pub(crate) token_key_id: String,
    #[serde(default = "default_token_key_env")]
    pub(crate) token_key_env: String,
    #[serde(default)]
    pub(crate) token_verification_keys: Vec<TokenKeyConfig>,
    #[serde(default)]
    pub(crate) admission: AdmissionConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TokenKeyConfig {
    pub(crate) id: String,
    pub(crate) key_env: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AdmissionConfig {
    #[serde(default = "default_rate_window_seconds")]
    pub(crate) window_seconds: u64,
    #[serde(default = "default_authentication_attempts")]
    pub(crate) authentication_attempts: u32,
    #[serde(default = "default_authentication_failures")]
    pub(crate) authentication_failures: u32,
    #[serde(default = "default_global_authentication_attempts")]
    pub(crate) global_authentication_attempts: u32,
    #[serde(default = "default_global_authentication_failures")]
    pub(crate) global_authentication_failures: u32,
    #[serde(default = "default_administration_rate")]
    pub(crate) administration: u32,
    #[serde(default = "default_read_rate")]
    pub(crate) reads: u32,
    #[serde(default = "default_write_rate")]
    pub(crate) writes: u32,
    #[serde(default = "default_check_rate")]
    pub(crate) checks: u32,
    #[serde(default = "default_enumeration_rate")]
    pub(crate) enumeration: u32,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            window_seconds: default_rate_window_seconds(),
            authentication_attempts: default_authentication_attempts(),
            authentication_failures: default_authentication_failures(),
            global_authentication_attempts: default_global_authentication_attempts(),
            global_authentication_failures: default_global_authentication_failures(),
            administration: default_administration_rate(),
            reads: default_read_rate(),
            writes: default_write_rate(),
            checks: default_check_rate(),
            enumeration: default_enumeration_rate(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EvaluatorPolicy {
    #[serde(default = "default_depth")]
    pub(crate) depth: u32,
    #[serde(default = "default_dispatches")]
    pub(crate) dispatches: u32,
    #[serde(default = "default_datastore_queries")]
    pub(crate) datastore_queries: u32,
    #[serde(default = "default_tuple_items")]
    pub(crate) tuple_items: u32,
    #[serde(default = "default_condition_cost")]
    pub(crate) condition_cost: u32,
    #[serde(default = "default_concurrent_reads")]
    pub(crate) concurrent_reads: u32,
    #[serde(default = "default_batch_concurrency")]
    pub(crate) batch_concurrency: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ListObjectsPolicy {
    #[serde(default = "default_depth")]
    pub(crate) candidate_depth: u32,
    #[serde(default = "default_dispatches")]
    pub(crate) candidate_dispatches: u32,
    #[serde(default = "default_candidate_datastore_queries")]
    pub(crate) candidate_datastore_queries: u32,
    #[serde(default = "default_tuple_items")]
    pub(crate) candidate_tuple_items: u32,
    #[serde(default = "default_candidates")]
    pub(crate) candidates: u32,
    #[serde(default = "default_dispatches")]
    pub(crate) residual_dispatches: u32,
    #[serde(default = "default_candidate_datastore_queries")]
    pub(crate) residual_datastore_queries: u32,
    #[serde(default = "default_tuple_items")]
    pub(crate) residual_tuple_items: u32,
    #[serde(default = "default_residual_concurrency")]
    pub(crate) residual_concurrency: u32,
    #[serde(default = "default_stream_buffer")]
    pub(crate) stream_buffer: u32,
}

impl Default for ListObjectsPolicy {
    fn default() -> Self {
        Self {
            candidate_depth: default_depth(),
            candidate_dispatches: default_dispatches(),
            candidate_datastore_queries: default_candidate_datastore_queries(),
            candidate_tuple_items: default_tuple_items(),
            candidates: default_candidates(),
            residual_dispatches: default_dispatches(),
            residual_datastore_queries: default_candidate_datastore_queries(),
            residual_tuple_items: default_tuple_items(),
            residual_concurrency: default_residual_concurrency(),
            stream_buffer: default_stream_buffer(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ListUsersPolicy {
    #[serde(default = "default_depth")]
    pub(crate) depth: u32,
    #[serde(default = "default_dispatches")]
    pub(crate) dispatches: u32,
    #[serde(default = "default_candidate_datastore_queries")]
    pub(crate) datastore_queries: u32,
    #[serde(default = "default_tuple_items")]
    pub(crate) tuple_items: u32,
    #[serde(default = "default_candidates")]
    pub(crate) subjects: u32,
    #[serde(default = "default_condition_cost")]
    pub(crate) condition_cost: u32,
}

impl Default for ListUsersPolicy {
    fn default() -> Self {
        Self {
            depth: default_depth(),
            dispatches: default_dispatches(),
            datastore_queries: default_candidate_datastore_queries(),
            tuple_items: default_tuple_items(),
            subjects: default_candidates(),
            condition_cost: default_condition_cost(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ExpandPolicy {
    #[serde(default = "default_depth")]
    pub(crate) depth: u32,
    #[serde(default = "default_candidates")]
    pub(crate) nodes: u32,
    #[serde(default = "default_candidate_datastore_queries")]
    pub(crate) datastore_queries: u32,
    #[serde(default = "default_tuple_items")]
    pub(crate) tuple_items: u32,
    #[serde(default = "default_expand_response_bytes")]
    pub(crate) response_bytes: u32,
}

impl Default for ExpandPolicy {
    fn default() -> Self {
        Self {
            depth: default_depth(),
            nodes: default_candidates(),
            datastore_queries: default_candidate_datastore_queries(),
            tuple_items: default_tuple_items(),
            response_bytes: default_expand_response_bytes(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum LogFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TelemetryConfig {
    #[serde(default)]
    pub(crate) log_format: LogFormat,
    #[serde(default = "default_log_filter")]
    pub(crate) log_filter: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) otlp_endpoint: Option<String>,
    #[serde(default = "default_telemetry_timeout_ms")]
    pub(crate) export_timeout_ms: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Pretty,
            log_filter: default_log_filter(),
            otlp_endpoint: None,
            export_timeout_ms: default_telemetry_timeout_ms(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ShutdownConfig {
    #[serde(default = "default_drain_timeout_ms")]
    pub(crate) drain_timeout_ms: u64,
    #[serde(default = "default_health_interval_ms")]
    pub(crate) health_interval_ms: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            drain_timeout_ms: default_drain_timeout_ms(),
            health_interval_ms: default_health_interval_ms(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawServerConfig {
    profile: Profile,
    listeners: ListenerConfig,
    #[serde(default)]
    tls: TlsConfig,
    storage: StorageConfig,
    #[serde(default)]
    cache: CacheConfig,
    auth: AuthConfig,
    transport: TransportPolicy,
    evaluator: EvaluatorPolicy,
    #[serde(default)]
    list_objects: ListObjectsPolicy,
    #[serde(default)]
    list_users: ListUsersPolicy,
    #[serde(default)]
    expand: ExpandPolicy,
    #[serde(default)]
    telemetry: TelemetryConfig,
    #[serde(default)]
    shutdown: ShutdownConfig,
}

impl From<RawServerConfig> for ServerConfig {
    fn from(raw: RawServerConfig) -> Self {
        Self {
            profile: raw.profile,
            listeners: raw.listeners,
            tls: raw.tls,
            storage: raw.storage,
            cache: raw.cache,
            auth: raw.auth,
            transport: raw.transport,
            evaluator: raw.evaluator,
            list_objects: raw.list_objects,
            list_users: raw.list_users,
            expand: raw.expand,
            telemetry: raw.telemetry,
            shutdown: raw.shutdown,
        }
    }
}

impl ServerConfig {
    /// Loads a bounded YAML document, applies `OPENFGA__...` overrides, and validates it.
    pub(crate) async fn load(path: &Path) -> Result<Self> {
        let contents = read_bounded(path, MAXIMUM_CONFIG_BYTES)
            .await
            .with_context(|| format!("failed to read configuration from {}", path.display()))?;
        let raw = tokio::task::spawn_blocking(move || parse(&contents))
            .await
            .context("configuration parser task failed")??;
        let config = Self::from(raw);
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.listeners.http == self.listeners.grpc {
            bail!("HTTP and gRPC listeners must be distinct");
        }
        let every_listener_is_loopback =
            self.listeners.http.ip().is_loopback() && self.listeners.grpc.ip().is_loopback();
        if (!every_listener_is_loopback || self.profile == Profile::Production) && !self.tls.enabled
        {
            bail!("public or production listeners require TLS");
        }
        if self.auth.mode == AuthMode::Disabled
            && (self.profile != Profile::Development || !every_listener_is_loopback)
        {
            bail!("disabled authentication requires development profile and loopback listeners");
        }
        self.validate_auth()?;
        self.validate_tls()?;
        self.validate_storage()?;
        self.model_cache_config()?;
        self.decision_cache_config()?;
        self.tuple_cache_config()?;
        self.cache_controller_config()?;
        self.validate_transport()?;
        self.validate_evaluator()?;
        self.validate_list_objects()?;
        self.validate_list_users()?;
        self.validate_expand()?;
        self.validate_database_concurrency()?;
        self.validate_telemetry()?;
        bounded_duration(
            self.shutdown.drain_timeout_ms,
            MAXIMUM_SHUTDOWN_DURATION,
            "shutdown drain timeout",
        )?;
        if self.cache.controller.maximum_lag_ms > self.shutdown.drain_timeout_ms {
            bail!("cache controller maximum lag cannot exceed shutdown drain timeout");
        }
        bounded_duration(
            self.shutdown.health_interval_ms,
            MAXIMUM_HEALTH_INTERVAL,
            "health interval",
        )?;
        Ok(())
    }

    pub(crate) fn request_timeout(&self) -> Result<RequestTimeout> {
        RequestTimeout::new(Duration::from_millis(self.transport.request_timeout_ms))
            .context("request timeout is outside the domain safety range")
    }

    pub(crate) fn admission_policy(&self) -> Result<AdmissionPolicy> {
        let policy = &self.transport.admission;
        Ok(AdmissionPolicy::builder()
            .window(Duration::from_secs(policy.window_seconds))
            .authentication_attempts(nonzero_rate(
                policy.authentication_attempts,
                "authentication attempt",
            )?)
            .authentication_failures(nonzero_rate(
                policy.authentication_failures,
                "authentication failure",
            )?)
            .global_authentication_attempts(nonzero_rate(
                policy.global_authentication_attempts,
                "global authentication attempt",
            )?)
            .global_authentication_failures(nonzero_rate(
                policy.global_authentication_failures,
                "global authentication failure",
            )?)
            .administration(nonzero_rate(policy.administration, "administration")?)
            .reads(nonzero_rate(policy.reads, "read")?)
            .writes(nonzero_rate(policy.writes, "write")?)
            .checks(nonzero_rate(policy.checks, "check")?)
            .enumeration(nonzero_rate(policy.enumeration, "enumeration")?)
            .build())
    }

    pub(crate) const fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown.drain_timeout_ms)
    }

    pub(crate) const fn health_interval(&self) -> Duration {
        Duration::from_millis(self.shutdown.health_interval_ms)
    }

    pub(crate) fn model_cache_config(&self) -> Result<ModelCacheConfig> {
        ModelCacheConfig::new(
            NonZeroU64::new(self.cache.model.source_weight)
                .context("model source cache weight must be nonzero")?,
            NonZeroU64::new(self.cache.model.compiled_weight)
                .context("compiled model cache weight must be nonzero")?,
            NonZeroU64::new(self.cache.model.latest_aliases)
                .context("latest model alias capacity must be nonzero")?,
            Duration::from_secs(self.cache.model.immutable_ttl_seconds),
            Duration::from_secs(self.cache.model.latest_alias_ttl_seconds),
        )
        .context("model cache configuration is invalid")
    }

    pub(crate) fn decision_cache_config(&self) -> Result<DecisionCacheConfig> {
        DecisionCacheConfig::new(
            NonZeroU64::new(self.cache.decision.weight)
                .context("decision cache weight must be nonzero")?,
            Duration::from_secs(self.cache.decision.ttl_seconds),
        )
        .context("decision cache configuration is invalid")
    }

    pub(crate) fn tuple_cache_config(&self) -> Result<TupleCacheConfig> {
        TupleCacheConfig::new(
            NonZeroU64::new(self.cache.tuple.weight)
                .context("tuple cache weight must be nonzero")?,
            self.cache.tuple.maximum_results,
            Duration::from_secs(self.cache.tuple.ttl_seconds),
        )
        .context("tuple cache configuration is invalid")
    }

    pub(crate) fn cache_controller_config(&self) -> Result<InvalidationControllerConfig> {
        InvalidationControllerConfig::new(
            NonZeroUsize::new(self.cache.controller.channel_capacity)
                .context("cache controller channel capacity must be nonzero")?,
            NonZeroU32::new(self.cache.controller.page_size)
                .context("cache controller page size must be nonzero")?,
            Duration::from_millis(self.cache.controller.poll_interval_ms),
            Duration::from_millis(self.cache.controller.read_timeout_ms),
            Duration::from_millis(self.cache.controller.maximum_lag_ms),
        )
        .context("cache controller configuration is invalid")
    }

    pub(crate) fn oidc_config(&self) -> OidcConfig {
        OidcConfig::builder()
            .issuer(self.auth.oidc.issuer.clone())
            .audiences(self.auth.oidc.audiences.clone())
            .authorized_parties(self.auth.oidc.authorized_parties.clone())
            .algorithms(self.auth.oidc.algorithms.clone())
            .allowed_hosts(self.auth.oidc.allowed_hosts.clone())
            .maximum_token_bytes(self.auth.oidc.maximum_token_bytes)
            .maximum_document_bytes(self.auth.oidc.maximum_document_bytes)
            .fetch_timeout(Duration::from_millis(self.auth.oidc.fetch_timeout_ms))
            .clock_skew(Duration::from_secs(self.auth.oidc.clock_skew_seconds))
            .refresh_interval(Duration::from_secs(self.auth.oidc.refresh_interval_seconds))
            .stale_key_grace(Duration::from_secs(self.auth.oidc.stale_key_grace_seconds))
            .build()
    }

    pub(crate) fn authorization_policy(&self) -> Result<AuthorizationPolicy> {
        if self.auth.mode == AuthMode::Disabled {
            return Ok(AuthorizationPolicy::development(
                DEVELOPMENT_PRINCIPAL_ID.parse()?,
            ));
        }
        let bindings = self
            .auth
            .authorization
            .bindings
            .iter()
            .map(|binding| {
                let principal_id = binding.principal.parse::<PrincipalId>()?;
                let actions = binding.actions.iter().copied().collect::<BTreeSet<_>>();
                let stores = if binding.stores.as_slice() == ["*"] {
                    StoreScope::Any
                } else {
                    StoreScope::Stores(
                        binding
                            .stores
                            .iter()
                            .map(|store| store.parse::<StoreId>())
                            .collect::<Result<BTreeSet<_>, _>>()?,
                    )
                };
                Ok(PolicyBinding::new(principal_id, actions, stores))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(AuthorizationPolicy::new(bindings))
    }

    fn validate_auth(&self) -> Result<()> {
        match self.auth.mode {
            AuthMode::Disabled => {
                if !self.auth.preshared.keys.is_empty()
                    || self.auth.oidc != OidcAuthConfig::default()
                    || !self.auth.authorization.bindings.is_empty()
                {
                    bail!("disabled authentication cannot retain ignored credentials or policy");
                }
            }
            AuthMode::Preshared => {
                self.validate_preshared()?;
                if self.auth.oidc != OidcAuthConfig::default() {
                    bail!("preshared authentication cannot retain ignored OIDC configuration");
                }
                self.validate_authorization()?;
            }
            AuthMode::Oidc => {
                if !self.auth.preshared.keys.is_empty() {
                    bail!("OIDC authentication cannot retain ignored preshared keys");
                }
                self.oidc_config()
                    .validate()
                    .context("OIDC configuration is invalid")?;
                self.validate_authorization()?;
            }
        }
        Ok(())
    }

    fn validate_preshared(&self) -> Result<()> {
        if self.auth.preshared.keys.is_empty() || self.auth.preshared.keys.len() > MAXIMUM_AUTH_KEYS
        {
            bail!("preshared authentication requires between 1 and 32 active keys");
        }
        let mut labels = HashSet::with_capacity(self.auth.preshared.keys.len());
        let mut environments = HashSet::with_capacity(self.auth.preshared.keys.len());
        for key in &self.auth.preshared.keys {
            key.id
                .parse::<PrincipalId>()
                .context("preshared key identity is invalid")?;
            validate_environment_name(&key.key_env)?;
            if !labels.insert(&key.id) || !environments.insert(&key.key_env) {
                bail!("preshared key identities and environment references must be unique");
            }
        }
        if self
            .auth
            .authorization
            .bindings
            .iter()
            .any(|binding| !labels.contains(&binding.principal))
        {
            bail!("preshared policy principals must name active preshared key identities");
        }
        Ok(())
    }

    fn validate_authorization(&self) -> Result<()> {
        let bindings = &self.auth.authorization.bindings;
        if bindings.is_empty() || bindings.len() > MAXIMUM_POLICY_BINDINGS {
            bail!("enabled authentication requires between 1 and 1024 policy bindings");
        }
        for binding in bindings {
            binding
                .principal
                .parse::<PrincipalId>()
                .context("policy principal is invalid")?;
            if binding.actions.is_empty()
                || binding
                    .actions
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>()
                    .len()
                    != binding.actions.len()
            {
                bail!("policy actions must be nonempty and unique");
            }
            if binding.stores.is_empty()
                || binding.stores.iter().collect::<HashSet<_>>().len() != binding.stores.len()
            {
                bail!("policy stores must be nonempty and unique");
            }
            let wildcard = binding.stores.as_slice() == ["*"];
            if binding.stores.iter().any(|store| store == "*") && !wildcard {
                bail!("policy wildcard store cannot be combined with explicit stores");
            }
            if !wildcard {
                for store in &binding.stores {
                    store
                        .parse::<StoreId>()
                        .context("policy store ID is invalid")?;
                }
                if binding.actions.iter().copied().any(Action::is_system) {
                    bail!("system actions require the wildcard store scope");
                }
            }
        }
        self.authorization_policy()?;
        Ok(())
    }

    fn validate_tls(&self) -> Result<()> {
        match (
            self.tls.enabled,
            self.tls.certificate_path.as_deref(),
            self.tls.private_key_path.as_deref(),
        ) {
            (false, None, None) => {}
            (false, _, _) => bail!("TLS paths cannot be set while TLS is disabled"),
            (true, Some(certificate), Some(key)) => {
                validate_absolute_path(certificate, "TLS certificate")?;
                validate_absolute_path(key, "TLS private key")?;
                if certificate == key {
                    bail!("TLS certificate and private key paths must differ");
                }
            }
            (true, _, _) => bail!("TLS requires certificatePath and privateKeyPath"),
        }
        if !(1..=3_600).contains(&self.tls.reload_interval_seconds) {
            bail!("TLS reload interval must be between 1 and 3600 seconds");
        }
        Ok(())
    }

    fn validate_storage(&self) -> Result<()> {
        if !(1..=65_536).contains(&self.storage.memory.actor_capacity) {
            bail!("memory actor capacity must be between 1 and 65536");
        }
        validate_environment_name(&self.storage.postgres.primary_url_env)?;
        if let Some(name) = &self.storage.postgres.replica_url_env {
            validate_environment_name(name)?;
        }
        if self.storage.postgres.max_connections == 0
            || self.storage.postgres.max_connections > 65_536
            || self.storage.postgres.min_connections > self.storage.postgres.max_connections
        {
            bail!("PostgreSQL pool limits are invalid");
        }
        bounded_duration(
            self.storage.postgres.acquire_timeout_ms,
            MAXIMUM_REQUEST_TIMEOUT,
            "PostgreSQL acquire timeout",
        )?;
        bounded_duration(
            self.storage.postgres.statement_timeout_ms,
            MAXIMUM_REQUEST_TIMEOUT,
            "PostgreSQL statement timeout",
        )?;
        if self.storage.postgres.max_tuple_mutations == 0
            || self.storage.postgres.max_tuple_mutations > 5_000
        {
            bail!("PostgreSQL tuple mutation limit must be between 1 and 5000");
        }
        Ok(())
    }

    fn validate_transport(&self) -> Result<()> {
        if self.transport.default_page_size == 0
            || self.transport.default_page_size > MAXIMUM_PAGE_SIZE
        {
            bail!("default page size must be between 1 and 100");
        }
        self.request_timeout()?;
        if self.transport.token_ttl_seconds == 0
            || self.transport.token_ttl_seconds > Duration::from_hours(720).as_secs()
        {
            bail!("continuation token TTL must be between one second and 720 hours");
        }
        if !(1..=16 * 1_024 * 1_024).contains(&self.transport.maximum_message_bytes) {
            bail!("maximum message bytes must be between 1 and 16777216");
        }
        if !(1..=65_536).contains(&self.transport.maximum_concurrency) {
            bail!("maximum concurrency must be between 1 and 65536");
        }
        let mut key_ids = BTreeSet::new();
        key_ids.insert(
            self.transport
                .token_key_id
                .parse::<TokenKeyId>()
                .context("continuation token signing key ID is invalid")?,
        );
        validate_environment_name(&self.transport.token_key_env)?;
        if self.transport.token_verification_keys.len() >= MAXIMUM_TOKEN_KEYS {
            bail!("continuation token verification key count must be at most 15");
        }
        for key in &self.transport.token_verification_keys {
            let id = key
                .id
                .parse::<TokenKeyId>()
                .context("continuation token verification key ID is invalid")?;
            if !key_ids.insert(id) {
                bail!("continuation token key IDs must be unique");
            }
            validate_environment_name(&key.key_env)?;
        }
        if !(1..=3_600).contains(&self.transport.admission.window_seconds) {
            bail!("admission rate window must be between 1 and 3600 seconds");
        }
        if [
            self.transport.admission.authentication_attempts,
            self.transport.admission.authentication_failures,
            self.transport.admission.global_authentication_attempts,
            self.transport.admission.global_authentication_failures,
            self.transport.admission.administration,
            self.transport.admission.reads,
            self.transport.admission.writes,
            self.transport.admission.checks,
            self.transport.admission.enumeration,
        ]
        .into_iter()
        .any(|rate| !(1..=1_000_000).contains(&rate))
        {
            bail!("admission rates must be between 1 and 1000000 per window");
        }
        Ok(())
    }

    fn validate_evaluator(&self) -> Result<()> {
        Limit::<1_000>::new(self.evaluator.depth).context("evaluator depth is invalid")?;
        Limit::<1_000_000>::new(self.evaluator.dispatches)
            .context("evaluator dispatch limit is invalid")?;
        Limit::<100_000>::new(self.evaluator.datastore_queries)
            .context("evaluator datastore query limit is invalid")?;
        Limit::<1_000_000>::new(self.evaluator.tuple_items)
            .context("evaluator tuple item limit is invalid")?;
        Limit::<1_000_000>::new(self.evaluator.condition_cost)
            .context("evaluator condition cost is invalid")?;
        Limit::<1_024>::new(self.evaluator.concurrent_reads)
            .context("evaluator concurrent read limit is invalid")?;
        Limit::<1_000>::new(self.evaluator.batch_concurrency)
            .context("evaluator batch concurrency is invalid")?;
        Ok(())
    }

    fn validate_list_objects(&self) -> Result<()> {
        Limit::<1_000>::new(self.list_objects.candidate_depth)
            .context("ListObjects candidate depth is invalid")?;
        Limit::<1_000_000>::new(self.list_objects.candidate_dispatches)
            .context("ListObjects candidate dispatch limit is invalid")?;
        Limit::<100_000>::new(self.list_objects.candidate_datastore_queries)
            .context("ListObjects candidate datastore query limit is invalid")?;
        Limit::<1_000_000>::new(self.list_objects.candidate_tuple_items)
            .context("ListObjects candidate tuple item limit is invalid")?;
        Limit::<100_000>::new(self.list_objects.candidates)
            .context("ListObjects candidate limit is invalid")?;
        Limit::<1_000_000>::new(self.list_objects.residual_dispatches)
            .context("ListObjects residual dispatch limit is invalid")?;
        Limit::<100_000>::new(self.list_objects.residual_datastore_queries)
            .context("ListObjects residual datastore query limit is invalid")?;
        Limit::<1_000_000>::new(self.list_objects.residual_tuple_items)
            .context("ListObjects residual tuple item limit is invalid")?;
        Limit::<1_024>::new(self.list_objects.residual_concurrency)
            .context("ListObjects residual concurrency is invalid")?;
        Limit::<1_024>::new(self.list_objects.stream_buffer)
            .context("ListObjects stream buffer is invalid")?;
        Ok(())
    }

    fn validate_list_users(&self) -> Result<()> {
        Limit::<1_000>::new(self.list_users.depth).context("ListUsers depth is invalid")?;
        Limit::<1_000_000>::new(self.list_users.dispatches)
            .context("ListUsers dispatch limit is invalid")?;
        Limit::<100_000>::new(self.list_users.datastore_queries)
            .context("ListUsers datastore query limit is invalid")?;
        Limit::<1_000_000>::new(self.list_users.tuple_items)
            .context("ListUsers tuple item limit is invalid")?;
        Limit::<100_000>::new(self.list_users.subjects)
            .context("ListUsers subject limit is invalid")?;
        Limit::<1_000_000>::new(self.list_users.condition_cost)
            .context("ListUsers condition cost is invalid")?;
        Ok(())
    }

    fn validate_expand(&self) -> Result<()> {
        Limit::<1_000>::new(self.expand.depth).context("Expand depth is invalid")?;
        Limit::<100_000>::new(self.expand.nodes).context("Expand node limit is invalid")?;
        Limit::<100_000>::new(self.expand.datastore_queries)
            .context("Expand datastore query limit is invalid")?;
        Limit::<1_000_000>::new(self.expand.tuple_items)
            .context("Expand tuple item limit is invalid")?;
        Limit::<16_777_216>::new(self.expand.response_bytes)
            .context("Expand response byte limit is invalid")?;
        Ok(())
    }

    fn validate_database_concurrency(&self) -> Result<()> {
        if self.storage.backend == StorageBackend::Postgres
            && (self.evaluator.concurrent_reads > self.storage.postgres.max_connections
                || self.evaluator.batch_concurrency > self.storage.postgres.max_connections
                || self.list_objects.residual_concurrency > self.storage.postgres.max_connections)
        {
            bail!("evaluator and ListObjects concurrency cannot exceed the PostgreSQL work limit");
        }
        Ok(())
    }

    fn validate_telemetry(&self) -> Result<()> {
        tracing_subscriber::EnvFilter::try_new(&self.telemetry.log_filter)
            .context("telemetry log filter is invalid")?;
        bounded_duration(
            self.telemetry.export_timeout_ms,
            Duration::from_mins(1),
            "telemetry export timeout",
        )?;
        if let Some(endpoint) = &self.telemetry.otlp_endpoint {
            if endpoint.len() > MAXIMUM_OTLP_ENDPOINT_BYTES {
                bail!("OTLP endpoint exceeds its byte limit");
            }
            let endpoint = reqwest::Url::parse(endpoint).context("OTLP endpoint is invalid")?;
            if !matches!(endpoint.scheme(), "http" | "https")
                || !endpoint.username().is_empty()
                || endpoint.password().is_some()
                || endpoint.host_str().is_none()
                || endpoint.path() != "/"
                || endpoint.query().is_some()
                || endpoint.fragment().is_some()
            {
                bail!("OTLP endpoint must be a credential-free HTTP(S) origin");
            }
        }
        Ok(())
    }
}

fn parse(contents: &[u8]) -> Result<RawServerConfig> {
    let text = std::str::from_utf8(contents).context("configuration is not valid UTF-8")?;
    Config::builder()
        .add_source(File::from_str(text, FileFormat::Yaml))
        .add_source(
            Environment::with_prefix("OPENFGA")
                .prefix_separator("__")
                .separator("__")
                .convert_case(Case::Camel)
                .try_parsing(true),
        )
        .build()
        .context("failed to merge configuration sources")?
        .try_deserialize()
        .context("configuration shape is invalid")
}

async fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(path).await?;
    let mut contents = Vec::with_capacity(usize::try_from(maximum.min(64 * 1_024))?);
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut contents)
        .await?;
    if u64::try_from(contents.len())? > maximum {
        bail!("configuration exceeds the {maximum}-byte limit");
    }
    Ok(contents)
}

fn validate_environment_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > MAXIMUM_ENVIRONMENT_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        || !name.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
    {
        bail!("secret environment reference is invalid");
    }
    Ok(())
}

fn nonzero_rate(value: u32, label: &str) -> Result<NonZeroU32> {
    NonZeroU32::new(value).with_context(|| format!("{label} rate must be nonzero"))
}

fn validate_absolute_path(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("{label} path must be absolute without parent traversal");
    }
    Ok(())
}

fn bounded_duration(value_ms: u64, maximum: Duration, label: &str) -> Result<Duration> {
    let value = Duration::from_millis(value_ms);
    if value.is_zero() || value > maximum {
        bail!("{label} must be positive and at most {maximum:?}");
    }
    Ok(value)
}

const fn default_actor_capacity() -> usize {
    256
}

const fn default_tls_reload_interval_seconds() -> u64 {
    30
}

fn default_primary_url_env() -> String {
    "OPENFGA_DATABASE_URL".to_owned()
}

const fn default_max_connections() -> u32 {
    16
}

const fn default_database_timeout_ms() -> u64 {
    5_000
}

const fn default_replica_lag_ms() -> u64 {
    1_000
}

const fn default_tuple_mutations() -> u32 {
    100
}

const fn default_page_size() -> u32 {
    50
}

const fn default_request_timeout_ms() -> u64 {
    5_000
}

const fn default_token_ttl_seconds() -> u64 {
    86_400
}

const fn default_message_bytes() -> usize {
    1_048_576
}

const fn default_concurrency() -> usize {
    1_024
}

fn default_token_key_id() -> String {
    "primary".to_owned()
}

fn default_token_key_env() -> String {
    "OPENFGA_TOKEN_KEY".to_owned()
}

const fn default_rate_window_seconds() -> u64 {
    60
}

const fn default_authentication_attempts() -> u32 {
    20_000
}

const fn default_authentication_failures() -> u32 {
    2_000
}

const fn default_global_authentication_attempts() -> u32 {
    200_000
}

const fn default_global_authentication_failures() -> u32 {
    20_000
}

const fn default_administration_rate() -> u32 {
    1_000
}

const fn default_read_rate() -> u32 {
    10_000
}

const fn default_write_rate() -> u32 {
    2_000
}

const fn default_check_rate() -> u32 {
    20_000
}

const fn default_enumeration_rate() -> u32 {
    1_000
}

fn default_oidc_algorithms() -> Vec<OidcAlgorithm> {
    vec![OidcAlgorithm::RS256]
}

const fn default_oidc_token_bytes() -> usize {
    8_192
}

const fn default_oidc_document_bytes() -> usize {
    256 * 1_024
}

const fn default_oidc_fetch_timeout_ms() -> u64 {
    5_000
}

const fn default_oidc_clock_skew_seconds() -> u64 {
    30
}

const fn default_oidc_refresh_seconds() -> u64 {
    3_600
}

const fn default_oidc_stale_seconds() -> u64 {
    86_400
}

const fn default_depth() -> u32 {
    25
}

const fn default_dispatches() -> u32 {
    10_000
}

const fn default_datastore_queries() -> u32 {
    100
}

const fn default_candidate_datastore_queries() -> u32 {
    1_000
}

const fn default_tuple_items() -> u32 {
    10_000
}

const fn default_condition_cost() -> u32 {
    100_000
}

const fn default_concurrent_reads() -> u32 {
    16
}

const fn default_batch_concurrency() -> u32 {
    16
}

const fn default_candidates() -> u32 {
    10_000
}

const fn default_expand_response_bytes() -> u32 {
    1_048_576
}

const fn default_residual_concurrency() -> u32 {
    16
}

const fn default_stream_buffer() -> u32 {
    16
}

fn default_log_filter() -> String {
    "info".to_owned()
}

const fn default_telemetry_timeout_ms() -> u64 {
    5_000
}

const fn default_drain_timeout_ms() -> u64 {
    10_000
}

const fn default_health_interval_ms() -> u64 {
    1_000
}

const fn default_model_source_weight() -> u64 {
    100_000
}

const fn default_model_compiled_weight() -> u64 {
    200_000
}

const fn default_model_aliases() -> u64 {
    10_000
}

const fn default_model_immutable_ttl_seconds() -> u64 {
    7 * 24 * 60 * 60
}

const fn default_model_alias_ttl_seconds() -> u64 {
    10
}

const fn default_decision_weight() -> u64 {
    100_000
}

const fn default_tuple_weight() -> u64 {
    1_000_000
}

const fn default_tuple_cache_results() -> usize {
    10_000
}

const fn default_mutable_cache_ttl_seconds() -> u64 {
    10
}

const fn default_cache_controller_capacity() -> usize {
    1_024
}

const fn default_cache_controller_page_size() -> u32 {
    100
}

const fn default_cache_controller_poll_ms() -> u64 {
    1_000
}

const fn default_cache_controller_read_ms() -> u64 {
    1_000
}

const fn default_cache_controller_lag_ms() -> u64 {
    10_000
}

#[cfg(test)]
mod tests {
    use openfga_auth::Action;
    use openfga_domain::{Principal, PrincipalKind};

    use super::{Profile, ServerConfig, parse};

    const VALID: &str = r"
profile: development
listeners:
  http: 127.0.0.1:8080
  grpc: 127.0.0.1:8081
storage:
  backend: memory
auth:
  mode: disabled
transport: {}
evaluator: {}
";
    const VALID_PRESHARED: &str = r"
profile: production
listeners:
  http: 127.0.0.1:8080
  grpc: 127.0.0.1:8081
tls:
  enabled: true
  certificatePath: /run/openfga/tls.crt
  privateKeyPath: /run/openfga/tls.key
storage:
  backend: memory
auth:
  mode: preshared
  preshared:
    keys:
      - id: reader
        keyEnv: OPENFGA_READER_KEY
  authorization:
    bindings:
      - principal: reader
        actions: [read]
        stores: [01ARZ3NDEKTSV4RRFFQ69G5FAV]
transport: {}
evaluator: {}
";

    #[test]
    fn test_should_validate_loopback_development_configuration() -> anyhow::Result<()> {
        let config = ServerConfig::from(parse(VALID.as_bytes())?);
        config.validate()?;
        assert_eq!(config.profile, Profile::Development);
        Ok(())
    }

    #[test]
    fn test_should_reject_unknown_and_insecure_public_configuration() -> anyhow::Result<()> {
        assert!(parse(format!("{VALID}\nunknown: true\n").as_bytes()).is_err());
        let public = VALID
            .replace("development", "production")
            .replace("127.0.0.1:8080", "0.0.0.0:8080");
        let config = ServerConfig::from(parse(public.as_bytes())?);
        assert!(config.validate().is_err());

        let mut ignored_oidc = ServerConfig::from(parse(VALID.as_bytes())?);
        ignored_oidc.auth.oidc.audiences = vec!["silently-ignored".to_owned()];
        assert!(ignored_oidc.validate().is_err());
        Ok(())
    }

    #[test]
    fn test_should_never_serialize_secret_values() -> anyhow::Result<()> {
        let config = ServerConfig::from(parse(VALID_PRESHARED.as_bytes())?);
        config.validate()?;
        let rendered = serde_json::to_string(&config)?;
        assert!(rendered.contains("OPENFGA_TOKEN_KEY"));
        assert!(rendered.contains("OPENFGA_READER_KEY"));
        assert!(!rendered.contains("databasePassword"));
        Ok(())
    }

    #[test]
    fn test_should_build_default_deny_store_action_policy() -> anyhow::Result<()> {
        let config = ServerConfig::from(parse(VALID_PRESHARED.as_bytes())?);
        config.validate()?;
        let policy = config.authorization_policy()?;
        let principal = Principal::new(PrincipalKind::PresharedKey, "reader".parse()?);
        assert!(
            policy
                .authorize(
                    &principal,
                    Action::Read,
                    Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?),
                )
                .is_ok()
        );
        assert!(
            policy
                .authorize(
                    &principal,
                    Action::Read,
                    Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".parse()?),
                )
                .is_err()
        );
        assert!(
            policy
                .authorize(&principal, Action::ListStores, None)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn test_should_reject_preshared_policy_for_inactive_identity() -> anyhow::Result<()> {
        let input = VALID_PRESHARED.replace("principal: reader", "principal: inactive-reader");
        let config = ServerConfig::from(parse(input.as_bytes())?);
        assert!(config.validate().is_err());
        Ok(())
    }

    #[test]
    fn test_should_validate_overlapping_continuation_token_keys() -> anyhow::Result<()> {
        let input = VALID_PRESHARED.replace(
            "transport: {}",
            "transport:\n  tokenKeyId: current\n  tokenKeyEnv: OPENFGA_TOKEN_CURRENT\n  \
             tokenVerificationKeys:\n    - id: prior\n      keyEnv: OPENFGA_TOKEN_PRIOR",
        );
        let config = ServerConfig::from(parse(input.as_bytes())?);
        config.validate()?;

        let duplicate = input.replace("- id: prior", "- id: current");
        let duplicate = ServerConfig::from(parse(duplicate.as_bytes())?);
        assert!(duplicate.validate().is_err());
        Ok(())
    }

    #[test]
    fn test_should_reject_transport_limits_outside_public_contract() -> anyhow::Result<()> {
        let mut config = ServerConfig::from(parse(VALID.as_bytes())?);
        config.transport.default_page_size = 101;
        assert!(config.validate().is_err());

        config.transport.default_page_size = 100;
        config.transport.admission.authentication_attempts = 0;
        assert!(config.validate().is_err());

        config.transport.admission.authentication_attempts = 1;
        config.list_objects.stream_buffer = 0;
        assert!(config.validate().is_err());

        config.list_objects.stream_buffer = 16;
        config.list_users.subjects = 0;
        assert!(config.validate().is_err());

        config.list_users.subjects = 10_000;
        config.expand.response_bytes = 0;
        assert!(config.validate().is_err());

        config.expand.response_bytes = 1_048_576;
        config.tls.reload_interval_seconds = 0;
        assert!(config.validate().is_err());
        Ok(())
    }

    #[test]
    fn test_should_reject_unbounded_or_zero_model_cache_policy() -> anyhow::Result<()> {
        let mut config = ServerConfig::from(parse(VALID.as_bytes())?);
        config.cache.model.source_weight = 0;
        assert!(config.validate().is_err());

        config.cache.model.source_weight = 1;
        config.cache.model.latest_alias_ttl_seconds = 301;
        assert!(config.validate().is_err());

        config.cache.model.latest_alias_ttl_seconds = 10;
        config.cache.model.immutable_ttl_seconds = 30 * 24 * 60 * 60;
        config.validate()?;

        config.cache.decision.ttl_seconds = 0;
        assert!(config.validate().is_err());

        config.cache.decision.ttl_seconds = 10;
        config.cache.tuple.maximum_results = 100_001;
        assert!(config.validate().is_err());

        config.cache.tuple.maximum_results = 10_000;
        config.cache.controller.maximum_lag_ms = 3_999;
        assert!(config.validate().is_err());

        config.cache.controller.maximum_lag_ms = 10_001;
        assert!(config.validate().is_err());
        Ok(())
    }

    #[test]
    fn test_should_reject_evaluator_and_list_scheduling_above_postgres_work_limit()
    -> anyhow::Result<()> {
        let mut config = ServerConfig::from(parse(VALID.as_bytes())?);
        config.storage.backend = super::StorageBackend::Postgres;
        config.storage.postgres.max_connections = 8;
        config.evaluator.concurrent_reads = 9;
        assert!(config.validate().is_err());

        config.evaluator.concurrent_reads = 8;
        config.evaluator.batch_concurrency = 9;
        assert!(config.validate().is_err());

        config.evaluator.batch_concurrency = 8;
        config.list_objects.residual_concurrency = 9;
        assert!(config.validate().is_err());

        config.list_objects.residual_concurrency = 8;
        config.validate()?;
        Ok(())
    }

    #[test]
    fn test_should_validate_oidc_without_network_and_reject_downgrade() -> anyhow::Result<()> {
        let oidc = VALID_PRESHARED
            .replace("mode: preshared", "mode: oidc")
            .replace(
                "  preshared:\n    keys:\n      - id: reader\n        keyEnv: OPENFGA_READER_KEY\n",
                "  oidc:\n    issuer: https://issuer.example.com\n    audiences: [openfga]\n    \
                 authorizedParties: [client]\n    algorithms: [RS256]\n",
            );
        let config = ServerConfig::from(parse(oidc.as_bytes())?);
        config.validate()?;

        let insecure = oidc.replace("https://issuer.example.com", "http://issuer.example.com");
        let insecure = ServerConfig::from(parse(insecure.as_bytes())?);
        assert!(insecure.validate().is_err());
        Ok(())
    }
}
