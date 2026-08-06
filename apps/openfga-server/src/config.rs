//! Bounded YAML configuration loading and validation.

use std::{
    collections::{BTreeSet, HashSet},
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use config::{Case, Config, Environment, File, FileFormat};
use openfga_auth::{
    Action, AuthorizationPolicy, OidcAlgorithm, OidcConfig, PolicyBinding, StoreScope,
};
use openfga_domain::{Limit, PrincipalId, RequestTimeout, StoreId, TokenKeyId};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

const MAXIMUM_CONFIG_BYTES: u64 = 1_048_576;
const MAXIMUM_ENVIRONMENT_NAME_BYTES: usize = 128;
const MAXIMUM_OTLP_ENDPOINT_BYTES: usize = 2_048;
const MAXIMUM_SHUTDOWN_DURATION: Duration = Duration::from_mins(5);
const MAXIMUM_HEALTH_INTERVAL: Duration = Duration::from_mins(1);
const MAXIMUM_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);
const MAXIMUM_AUTH_KEYS: usize = 32;
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
    pub(crate) auth: AuthConfig,
    pub(crate) transport: TransportPolicy,
    pub(crate) evaluator: EvaluatorPolicy,
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TlsConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) certificate_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) private_key_path: Option<PathBuf>,
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
    auth: AuthConfig,
    transport: TransportPolicy,
    evaluator: EvaluatorPolicy,
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
            auth: raw.auth,
            transport: raw.transport,
            evaluator: raw.evaluator,
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
        self.validate_transport()?;
        self.validate_evaluator()?;
        self.validate_telemetry()?;
        bounded_duration(
            self.shutdown.drain_timeout_ms,
            MAXIMUM_SHUTDOWN_DURATION,
            "shutdown drain timeout",
        )?;
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

    pub(crate) const fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown.drain_timeout_ms)
    }

    pub(crate) const fn health_interval(&self) -> Duration {
        Duration::from_millis(self.shutdown.health_interval_ms)
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
            (false, None, None) => Ok(()),
            (false, _, _) => bail!("TLS paths cannot be set while TLS is disabled"),
            (true, Some(certificate), Some(key)) => {
                validate_absolute_path(certificate, "TLS certificate")?;
                validate_absolute_path(key, "TLS private key")?;
                if certificate == key {
                    bail!("TLS certificate and private key paths must differ");
                }
                Ok(())
            }
            (true, _, _) => bail!("TLS requires certificatePath and privateKeyPath"),
        }
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
        if self.transport.default_page_size == 0 || self.transport.default_page_size > 100_000 {
            bail!("default page size must be between 1 and 100000");
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
        self.transport
            .token_key_id
            .parse::<TokenKeyId>()
            .context("continuation token key ID is invalid")?;
        validate_environment_name(&self.transport.token_key_env)
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
