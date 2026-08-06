//! Production service assembly, health supervision, listeners, and drain.

use std::{
    env, fmt,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use axum::{Json, Router, http::StatusCode, routing::get};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use openfga_auth::{AuthenticationService, JwksActor, PresharedKey};
use openfga_check::CheckBudget;
use openfga_domain::{
    ConsistencyPreference, Deadline, InputLimits, Limit, RequestTimeout, TokenCodec, TokenKey,
};
use openfga_model::ModelCompiler;
use openfga_service::{
    AssertionService, ChangeService, CheckService, IdentifierSource, ModelPublication,
    ModelService, StoreService, SystemIdentifierSource, SystemIdentifierSourceConfig,
    SystemServiceClock, TupleService,
};
use openfga_storage::{
    AssertionReader, AssertionWriter, ChangeReader, HealthCheck, ModelReader, ModelWriter,
    OperationContext, StorageCancellationToken, StoreReader, StoreWriter, TupleReader, TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use openfga_storage_sql::{PostgresStorage, PostgresStorageConfig};
use openfga_transport::{
    AuthenticatedGrpcService, OpenFgaApi, OpenFgaServices, TransportConfig, grpc_service,
    http_router,
};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use tokio::{
    io::AsyncReadExt,
    sync::{mpsc, watch},
    task::JoinSet,
    time::{MissedTickBehavior, timeout},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic_health::{ServingStatus, server::HealthReporter};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::limit::RequestBodyLimitLayer;

use crate::config::{AuthMode, DEVELOPMENT_PRINCIPAL_ID, ServerConfig, StorageBackend};

const MAXIMUM_SECRET_ENV_BYTES: usize = 8_192;
const MAXIMUM_TLS_FILE_BYTES: u64 = 2 * 1_024 * 1_024;
const MAXIMUM_HEALTH_BODY_BYTES: usize = 1_024;
const MAXIMUM_HEALTH_CONCURRENCY: usize = 64;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug)]
struct HealthState {
    live: AtomicBool,
    ready: AtomicBool,
}

impl HealthState {
    fn new() -> Self {
        Self {
            live: AtomicBool::new(true),
            ready: AtomicBool::new(false),
        }
    }

    fn set_ready(&self, ready: bool) -> bool {
        self.ready.swap(ready, Ordering::AcqRel)
    }
}

#[derive(Clone)]
struct TlsMaterial {
    http_config: axum_server::tls_rustls::RustlsConfig,
    certificate_path: PathBuf,
    private_key_path: PathBuf,
    read_timeout: Duration,
    reload_interval: Duration,
}

impl fmt::Debug for TlsMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsMaterial")
            .field("certificate_path", &self.certificate_path)
            .field("private_key", &"[REDACTED]")
            .field("read_timeout", &self.read_timeout)
            .field("reload_interval", &self.reload_interval)
            .finish_non_exhaustive()
    }
}

enum StorageOwner {
    Memory(Arc<MemoryStorage>),
    Postgres(Arc<PostgresStorage>),
}

impl fmt::Debug for StorageOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory(_) => formatter.write_str("StorageOwner::Memory"),
            Self::Postgres(_) => formatter.write_str("StorageOwner::Postgres"),
        }
    }
}

struct RuntimeAssembly {
    api: OpenFgaApi,
    authentication: AuthenticationService,
    jwks_actor: Option<JwksActor>,
    storage: StorageOwner,
    health: Arc<dyn HealthCheck>,
    identifiers: Arc<SystemIdentifierSource>,
}

#[derive(Clone, Debug)]
struct TransportRuntime {
    api: OpenFgaApi,
    authentication: AuthenticationService,
}

#[derive(Clone)]
struct ReadinessDependencies {
    storage: Arc<dyn HealthCheck>,
    authentication: AuthenticationService,
}

impl fmt::Debug for ReadinessDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadinessDependencies")
            .field("storage", &"dyn HealthCheck")
            .field("authentication", &self.authentication)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for RuntimeAssembly {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeAssembly")
            .field("api", &self.api)
            .field("authentication", &self.authentication)
            .field("jwks_actor", &self.jwks_actor)
            .field("storage", &self.storage)
            .field("health", &"dyn HealthCheck")
            .field("identifiers_running", &self.identifiers.is_running())
            .finish()
    }
}

/// Runs the fully assembled HTTP/gRPC server until signal or supervised failure.
pub(crate) async fn run(config: ServerConfig) -> Result<()> {
    let tls = load_tls(&config).await?;
    let assembly = assemble(&config).await?;
    let RuntimeAssembly {
        api,
        authentication,
        jwks_actor,
        storage,
        health,
        identifiers,
    } = assembly;
    let http_listener = bind(config.listeners.http, "HTTP").await?;
    let grpc_listener = bind(config.listeners.grpc, "gRPC").await?;
    let health_state = Arc::new(HealthState::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<AuthenticatedGrpcService>()
        .await;

    let mut tasks = JoinSet::new();
    if let Some(actor) = jwks_actor {
        let actor_shutdown = shutdown_rx.clone();
        tasks.spawn(async move {
            actor.run(actor_shutdown).await;
            Ok(())
        });
    }
    if let Some(material) = tls.clone() {
        let actor_shutdown = shutdown_rx.clone();
        tasks.spawn(async move { material.run_reloader(actor_shutdown).await });
    }
    spawn_http(
        &mut tasks,
        http_listener,
        http_router(api.clone(), authentication.clone())
            .merge(health_router(Arc::clone(&health_state))),
        tls.as_ref(),
        shutdown_rx.clone(),
        config.drain_timeout() / 2,
    )?;
    spawn_grpc(
        &mut tasks,
        grpc_listener,
        TransportRuntime {
            api,
            authentication: authentication.clone(),
        },
        health_service,
        tls.as_ref(),
        shutdown_rx.clone(),
        &config,
    )?;
    spawn_health_monitor(
        &mut tasks,
        ReadinessDependencies {
            storage: Arc::clone(&health),
            authentication,
        },
        Arc::clone(&health_state),
        health_reporter.clone(),
        shutdown_rx,
        config.health_interval(),
        config.request_timeout()?.duration(),
    );
    let _previous_readiness = health_state.set_ready(true);
    tracing::info!(
        http.address = %config.listeners.http,
        grpc.address = %config.listeners.grpc,
        tls.enabled = config.tls.enabled,
        "OpenFGA listeners are ready"
    );

    let first_failure = supervise_until_shutdown(&mut tasks).await;
    let _previous_readiness = health_state.set_ready(false);
    let _shutdown_receivers = shutdown_tx.send(true);
    health_reporter
        .set_service_status("", ServingStatus::NotServing)
        .await;
    health_reporter
        .set_not_serving::<AuthenticatedGrpcService>()
        .await;
    let drain_result = drain_tasks(&mut tasks, config.drain_timeout()).await;
    health_state.live.store(false, Ordering::Release);
    drop(health);
    shutdown_resources(storage, identifiers).await?;
    first_failure?;
    drain_result
}

pub(crate) fn postgres_config(
    config: &ServerConfig,
    migrate_on_connect: bool,
) -> Result<PostgresStorageConfig> {
    if config.storage.backend != StorageBackend::Postgres {
        bail!("migration commands require storage.backend: postgres");
    }
    let raw = &config.storage.postgres;
    let primary = load_secret(&raw.primary_url_env)?;
    let replica = raw
        .replica_url_env
        .as_deref()
        .map(load_secret)
        .transpose()?;
    let maximum = NonZeroU32::new(raw.max_connections)
        .context("PostgreSQL maximum connections must be nonzero")?;
    let mutations = NonZeroU32::new(raw.max_tuple_mutations)
        .context("PostgreSQL tuple mutation limit must be nonzero")?;
    let built = PostgresStorageConfig::builder()
        .primary_url(primary)
        .replica_url(replica)
        .max_connections(maximum)
        .min_connections(raw.min_connections)
        .acquire_timeout(Duration::from_millis(raw.acquire_timeout_ms))
        .statement_timeout(Duration::from_millis(raw.statement_timeout_ms))
        .replica_max_lag(Duration::from_millis(raw.replica_max_lag_ms))
        .max_tuple_mutations(mutations)
        .migrate_on_connect(migrate_on_connect)
        .build();
    built
        .validate()
        .context("PostgreSQL configuration is invalid")?;
    Ok(built)
}

async fn assemble(config: &ServerConfig) -> Result<RuntimeAssembly> {
    let (authentication, jwks_actor) = authentication(config).await?;
    let limits = InputLimits::default();
    let identifiers = Arc::new(
        SystemIdentifierSource::start(SystemIdentifierSourceConfig::default())
            .context("failed to start identifier actor")?,
    );
    let identifier_service: Arc<dyn IdentifierSource> = identifiers.clone();
    let budget = check_budget(config)?;
    let (services, storage, health) = match config.storage.backend {
        StorageBackend::Memory => {
            let capacity = NonZeroUsize::new(config.storage.memory.actor_capacity)
                .context("memory actor capacity must be nonzero")?;
            let storage = Arc::new(
                MemoryStorage::start(MemoryStorageConfig::new(
                    limits.clone(),
                    capacity,
                    config.drain_timeout(),
                )?)
                .context("failed to start memory storage actor")?,
            );
            let health: Arc<dyn HealthCheck> = storage.clone();
            let services = services(storage.clone(), identifier_service, limits.clone(), budget);
            (services, StorageOwner::Memory(storage), health)
        }
        StorageBackend::Postgres => {
            let postgres = postgres_config(config, config.storage.postgres.migrate_on_start)?;
            let storage = Arc::new(
                PostgresStorage::connect(postgres)
                    .await
                    .context("failed to connect PostgreSQL storage")?,
            );
            let health: Arc<dyn HealthCheck> = storage.clone();
            let services = services(storage.clone(), identifier_service, limits.clone(), budget);
            (services, StorageOwner::Postgres(storage), health)
        }
    };
    let (token_key, verification_keys) = load_token_keys(config)?;
    let token_codec = Arc::new(
        TokenCodec::new(token_key, verification_keys, &limits)
            .context("failed to create continuation token codec")?,
    );
    let authorization_policy = Arc::new(config.authorization_policy()?);
    let page_size = NonZeroU32::new(config.transport.default_page_size)
        .context("default page size must be nonzero")?;
    let transport = TransportConfig::builder()
        .limits(limits)
        .authorization_policy(authorization_policy)
        .token_codec(token_codec)
        .default_page_size(page_size)
        .request_timeout(config.request_timeout()?)
        .token_ttl(Duration::from_secs(config.transport.token_ttl_seconds))
        .maximum_message_bytes(config.transport.maximum_message_bytes)
        .maximum_concurrency(config.transport.maximum_concurrency)
        .admission_policy(config.admission_policy()?)
        .build();
    let api = OpenFgaApi::new(services, transport)
        .map_err(anyhow::Error::msg)
        .context("transport configuration is invalid")?;
    Ok(RuntimeAssembly {
        api,
        authentication,
        jwks_actor,
        storage,
        health,
        identifiers,
    })
}

async fn authentication(
    config: &ServerConfig,
) -> Result<(AuthenticationService, Option<JwksActor>)> {
    match config.auth.mode {
        AuthMode::Disabled => Ok((
            AuthenticationService::development(
                DEVELOPMENT_PRINCIPAL_ID
                    .parse()
                    .context("development principal invariant is invalid")?,
            ),
            None,
        )),
        AuthMode::Preshared => {
            let keys = config
                .auth
                .preshared
                .keys
                .iter()
                .map(|key| {
                    let secret = load_secret(&key.key_env)?;
                    PresharedKey::new(key.id.parse()?, &secret)
                        .context("preshared authentication key is invalid")
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((
                AuthenticationService::preshared(keys)
                    .context("preshared authentication configuration is invalid")?,
                None,
            ))
        }
        AuthMode::Oidc => {
            let (authentication, actor) =
                AuthenticationService::open_id_connect(config.oidc_config())
                    .await
                    .context("OIDC authentication initialization failed")?;
            Ok((authentication, Some(actor)))
        }
    }
}

fn services<B>(
    storage: Arc<B>,
    identifiers: Arc<dyn IdentifierSource>,
    limits: InputLimits,
    budget: CheckBudget,
) -> OpenFgaServices
where
    B: AssertionReader
        + AssertionWriter
        + ChangeReader
        + ModelReader
        + ModelWriter
        + StoreReader
        + StoreWriter
        + TupleReader
        + TupleWriter
        + Send
        + Sync
        + 'static,
{
    let stores: Arc<dyn StoreReader> = storage.clone();
    let store_writes: Arc<dyn StoreWriter> = storage.clone();
    let models: Arc<dyn ModelReader> = storage.clone();
    let model_writes: Arc<dyn ModelWriter> = storage.clone();
    let tuples: Arc<dyn TupleReader> = storage.clone();
    let tuple_writes: Arc<dyn TupleWriter> = storage.clone();
    let assertion_reads: Arc<dyn AssertionReader> = storage.clone();
    let assertion_writes: Arc<dyn AssertionWriter> = storage.clone();
    let changes: Arc<dyn ChangeReader> = storage;
    OpenFgaServices::builder()
        .stores(StoreService::new(
            stores.clone(),
            store_writes,
            identifiers.clone(),
        ))
        .models(ModelService::new(
            stores.clone(),
            models.clone(),
            model_writes,
            ModelPublication::new(
                identifiers,
                Arc::new(SystemServiceClock::default()),
                ModelCompiler::default(),
            ),
        ))
        .assertions(AssertionService::new(
            stores.clone(),
            models.clone(),
            assertion_reads,
            assertion_writes,
            limits.clone(),
        ))
        .tuples(TupleService::new(
            stores.clone(),
            models.clone(),
            tuples.clone(),
            tuple_writes,
            limits,
        ))
        .changes(ChangeService::new(stores, changes))
        .checks(CheckService::direct(models, tuples, budget))
        .build()
}

fn check_budget(config: &ServerConfig) -> Result<CheckBudget> {
    Ok(CheckBudget::builder()
        .depth(Limit::<1_000>::new(config.evaluator.depth)?)
        .dispatches(Limit::<1_000_000>::new(config.evaluator.dispatches)?)
        .datastore_queries(Limit::<100_000>::new(config.evaluator.datastore_queries)?)
        .tuple_items(Limit::<1_000_000>::new(config.evaluator.tuple_items)?)
        .condition_cost(Limit::<1_000_000>::new(config.evaluator.condition_cost)?)
        .concurrent_reads(Limit::<1_024>::new(config.evaluator.concurrent_reads)?)
        .batch_concurrency(Limit::<1_000>::new(config.evaluator.batch_concurrency)?)
        .build())
}

fn load_token_keys(config: &ServerConfig) -> Result<(TokenKey, Vec<TokenKey>)> {
    let encoded = load_secret(&config.transport.token_key_env)?;
    let bytes = BASE64_STANDARD
        .decode(encoded.expose_secret().as_bytes())
        .context("continuation token key must be standard base64")?;
    let signing_key = TokenKey::new(config.transport.token_key_id.parse()?, bytes)
        .context("continuation token signing key must decode to 32 through 64 bytes")?;
    let verification_keys = config
        .transport
        .token_verification_keys
        .iter()
        .map(|key| {
            let encoded = load_secret(&key.key_env)?;
            let bytes = BASE64_STANDARD
                .decode(encoded.expose_secret().as_bytes())
                .context("continuation token verification key must be standard base64")?;
            TokenKey::new(key.id.parse()?, bytes)
                .context("continuation token verification key must decode to 32 through 64 bytes")
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((signing_key, verification_keys))
}

fn load_secret(name: &str) -> Result<SecretString> {
    let value =
        env::var(name).with_context(|| format!("required secret environment {name} is unset"))?;
    if value.is_empty()
        || value.len() > MAXIMUM_SECRET_ENV_BYTES
        || value.chars().any(char::is_control)
    {
        bail!("secret environment {name} is empty, oversized, or contains control characters");
    }
    Ok(SecretString::from(value))
}

pub(crate) fn install_crypto_provider() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("a different rustls crypto provider is already installed"))
}

async fn load_tls(config: &ServerConfig) -> Result<Option<TlsMaterial>> {
    if !config.tls.enabled {
        return Ok(None);
    }
    let certificate_path = config
        .tls
        .certificate_path
        .as_deref()
        .context("TLS certificate path is missing")?;
    let private_key_path = config
        .tls
        .private_key_path
        .as_deref()
        .context("TLS private key path is missing")?;
    let read_timeout = config.request_timeout()?.duration();
    let http_config = load_tls_pair(certificate_path, private_key_path, read_timeout).await?;
    Ok(Some(TlsMaterial {
        http_config,
        certificate_path: certificate_path.to_owned(),
        private_key_path: private_key_path.to_owned(),
        read_timeout,
        reload_interval: config.tls.reload_interval(),
    }))
}

async fn load_tls_pair(
    certificate_path: &Path,
    private_key_path: &Path,
    maximum: Duration,
) -> Result<axum_server::tls_rustls::RustlsConfig> {
    let reads = async {
        tokio::try_join!(
            read_bounded_file(certificate_path),
            read_bounded_file(private_key_path),
        )
    };
    let (certificate, private_key) = timeout(maximum, reads)
        .await
        .context("TLS material read timed out")??;
    axum_server::tls_rustls::RustlsConfig::from_pem(certificate, private_key)
        .await
        .context("TLS certificate and private key pair is invalid")
}

impl TlsMaterial {
    async fn run_reloader(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut ticker = tokio::time::interval(self.reload_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let _initial_tick = ticker.tick().await;
        loop {
            tokio::select! {
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = ticker.tick() => {
                    match self.reload_once().await {
                        Ok(()) => {
                            tracing::info!("TLS certificate and private key reloaded atomically");
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "TLS reload rejected; retaining the active identity");
                        }
                    }
                }
            }
        }
    }

    async fn reload_once(&self) -> Result<()> {
        let candidate = load_tls_pair(
            &self.certificate_path,
            &self.private_key_path,
            self.read_timeout,
        )
        .await?;
        self.publish(&candidate);
        Ok(())
    }

    fn publish(&self, candidate: &axum_server::tls_rustls::RustlsConfig) {
        self.http_config.reload_from_config(candidate.get_inner());
    }
}

async fn read_bounded_file(path: &Path) -> Result<Vec<u8>> {
    let canonical = tokio::fs::canonicalize(path)
        .await
        .with_context(|| format!("failed to canonicalize {}", path.display()))?;
    let file = tokio::fs::File::open(&canonical)
        .await
        .with_context(|| format!("failed to open {}", canonical.display()))?;
    let mut bytes = Vec::with_capacity(16 * 1_024);
    file.take(MAXIMUM_TLS_FILE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .context("failed to read TLS material")?;
    if u64::try_from(bytes.len())? > MAXIMUM_TLS_FILE_BYTES {
        bail!("TLS material exceeds its byte limit");
    }
    Ok(bytes)
}

async fn bind(address: SocketAddr, protocol: &str) -> Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind {protocol} listener at {address}"))
}

fn health_router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/healthz", get(liveness))
        .route("/readyz", get(readiness))
        .layer(RequestBodyLimitLayer::new(MAXIMUM_HEALTH_BODY_BYTES))
        .layer(ConcurrencyLimitLayer::new(MAXIMUM_HEALTH_CONCURRENCY))
        .with_state(state)
}

async fn liveness(
    axum::extract::State(state): axum::extract::State<Arc<HealthState>>,
) -> (StatusCode, Json<HealthResponse>) {
    health_response(state.live.load(Ordering::Acquire))
}

async fn readiness(
    axum::extract::State(state): axum::extract::State<Arc<HealthState>>,
) -> (StatusCode, Json<HealthResponse>) {
    health_response(state.ready.load(Ordering::Acquire))
}

const fn health_response(healthy: bool) -> (StatusCode, Json<HealthResponse>) {
    if healthy {
        (StatusCode::OK, Json(HealthResponse { status: "SERVING" }))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "NOT_SERVING",
            }),
        )
    }
}

fn spawn_http(
    tasks: &mut JoinSet<Result<()>>,
    listener: tokio::net::TcpListener,
    router: Router,
    tls: Option<&TlsMaterial>,
    shutdown: watch::Receiver<bool>,
    graceful_timeout: Duration,
) -> Result<()> {
    let listener = listener
        .into_std()
        .context("failed to transfer the HTTP listener")?;
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    let shutdown = async move {
        wait_for_shutdown(shutdown).await;
        shutdown_handle.graceful_shutdown(Some(graceful_timeout));
    };
    if let Some(tls) = tls {
        let rustls = tls.http_config.clone();
        let server = async move {
            let serve = axum_server::from_tcp_rustls(listener, rustls)
                .context("failed to configure HTTP TLS listener")?
                .handle(handle)
                .serve(router.into_make_service_with_connect_info::<SocketAddr>());
            tokio::pin!(serve);
            tokio::pin!(shutdown);
            tokio::select! {
                result = &mut serve => result.context("HTTP listener failed"),
                () = &mut shutdown => serve.await.context("HTTP listener failed during drain"),
            }
        };
        tasks.spawn(server);
    } else {
        let server = async move {
            let serve = axum_server::from_tcp(listener)
                .context("failed to configure HTTP listener")?
                .handle(handle)
                .serve(router.into_make_service_with_connect_info::<SocketAddr>());
            tokio::pin!(serve);
            tokio::pin!(shutdown);
            tokio::select! {
                result = &mut serve => result.context("HTTP listener failed"),
                () = &mut shutdown => serve.await.context("HTTP listener failed during drain"),
            }
        };
        tasks.spawn(server);
    }
    Ok(())
}

fn spawn_grpc<H>(
    tasks: &mut JoinSet<Result<()>>,
    listener: tokio::net::TcpListener,
    transport: TransportRuntime,
    health_service: H,
    tls: Option<&TlsMaterial>,
    shutdown: watch::Receiver<bool>,
    config: &ServerConfig,
) -> Result<()>
where
    H: tower::Service<
            axum::http::Request<tonic::body::Body>,
            Response = axum::http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + tonic::server::NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    H::Future: Send + 'static,
{
    let mut server = Server::builder().timeout(config.request_timeout()?.duration());
    if let Some(tls) = tls {
        let incoming = spawn_grpc_tls_incoming(
            tasks,
            listener,
            tls.clone(),
            shutdown.clone(),
            config.request_timeout()?.duration(),
            config.transport.maximum_concurrency.min(1_024),
        );
        tasks.spawn(async move {
            server
                .add_service(health_service)
                .add_service(grpc_service(transport.api, transport.authentication))
                .serve_with_incoming_shutdown(incoming, wait_for_shutdown(shutdown))
                .await
                .context("gRPC listener failed")
        });
        return Ok(());
    }
    let incoming = TcpListenerStream::new(listener);
    tasks.spawn(async move {
        server
            .add_service(health_service)
            .add_service(grpc_service(transport.api, transport.authentication))
            .serve_with_incoming_shutdown(incoming, wait_for_shutdown(shutdown))
            .await
            .context("gRPC listener failed")
    });
    Ok(())
}

fn spawn_grpc_tls_incoming(
    tasks: &mut JoinSet<Result<()>>,
    listener: tokio::net::TcpListener,
    tls: TlsMaterial,
    mut shutdown: watch::Receiver<bool>,
    handshake_timeout: Duration,
    maximum_handshakes: usize,
) -> ReceiverStream<Result<TlsStream<tokio::net::TcpStream>, std::io::Error>> {
    let capacity = maximum_handshakes.max(1);
    let (sender, receiver) = mpsc::channel(capacity);
    tasks.spawn(async move {
        let mut handshakes = JoinSet::new();
        loop {
            tokio::select! {
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        handshakes.abort_all();
                        while handshakes.join_next().await.is_some() {}
                        return Ok(());
                    }
                }
                accepted = listener.accept(), if handshakes.len() < capacity => {
                    let (stream, _peer) = accepted.context("gRPC TCP accept failed")?;
                    let acceptor = TlsAcceptor::from(tls.http_config.get_inner());
                    handshakes.spawn(async move {
                        timeout(handshake_timeout, acceptor.accept(stream))
                            .await
                            .context("gRPC TLS handshake timed out")?
                            .context("gRPC TLS handshake failed")
                    });
                }
                Some(result) = handshakes.join_next(), if !handshakes.is_empty() => {
                    match result {
                        Ok(Ok(stream)) => match sender.try_send(Ok(stream)) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!("gRPC TLS admission queue is full");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                        },
                        Ok(Err(error)) => {
                            tracing::debug!(error = %error, "gRPC TLS connection rejected");
                        }
                        Err(error) if error.is_panic() => {
                            return Err(anyhow::Error::new(error)
                                .context("gRPC TLS handshake task panicked"));
                        }
                        Err(error) => {
                            return Err(anyhow::Error::new(error)
                                .context("gRPC TLS handshake task was cancelled"));
                        }
                    }
                }
            }
        }
    });
    ReceiverStream::new(receiver)
}

fn spawn_health_monitor(
    tasks: &mut JoinSet<Result<()>>,
    dependencies: ReadinessDependencies,
    state: Arc<HealthState>,
    reporter: HealthReporter,
    mut shutdown: watch::Receiver<bool>,
    interval: Duration,
    probe_timeout: Duration,
) {
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = ticker.tick() => {
                    let storage_ready = probe_storage(dependencies.storage.as_ref(), probe_timeout).await;
                    let authentication_ready = dependencies.authentication.is_ready();
                    let ready = storage_ready && authentication_ready;
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                    let was_ready = state.set_ready(ready);
                    if ready && !was_ready {
                        tracing::info!(dependency = "runtime", "readiness dependencies recovered");
                    } else if !ready && was_ready {
                        tracing::warn!(
                            dependency = "runtime",
                            storage.ready = storage_ready,
                            authentication.ready = authentication_ready,
                            "readiness dependency failed"
                        );
                    }
                    let status = if ready {
                        ServingStatus::Serving
                    } else {
                        ServingStatus::NotServing
                    };
                    reporter.set_service_status("", status).await;
                }
            }
        }
    });
}

async fn probe_storage(storage: &dyn HealthCheck, probe_timeout: Duration) -> bool {
    let Ok(request_timeout) = RequestTimeout::new(probe_timeout) else {
        return false;
    };
    let Ok(deadline) = Deadline::from_timeout(Instant::now(), request_timeout) else {
        return false;
    };
    let cancellation = StorageCancellationToken::new();
    let context = OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        deadline,
        cancellation.clone(),
    );
    let result = timeout(probe_timeout, storage.health(&context)).await;
    cancellation.cancel();
    matches!(result, Ok(Ok(status)) if status.is_ready())
}

async fn supervise_until_shutdown(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    tokio::select! {
        signal = crate::shutdown_signal() => signal,
        task = tasks.join_next() => match task {
            Some(Ok(Ok(()))) => bail!("a supervised runtime task exited unexpectedly"),
            Some(Ok(Err(error))) => Err(error.context("a supervised runtime task failed")),
            Some(Err(error)) if error.is_panic() => Err(anyhow::Error::new(error).context("a supervised runtime task panicked")),
            Some(Err(error)) => Err(anyhow::Error::new(error).context("a supervised runtime task was cancelled")),
            None => bail!("runtime supervisor has no owned tasks"),
        },
    }
}

async fn drain_tasks(tasks: &mut JoinSet<Result<()>>, maximum: Duration) -> Result<()> {
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error.context("runtime task failed during drain")),
                Err(error) if error.is_panic() => {
                    return Err(
                        anyhow::Error::new(error).context("runtime task panicked during drain")
                    );
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error)
                        .context("runtime task was cancelled during drain"));
                }
            }
        }
        Ok(())
    };
    if let Ok(result) = timeout(maximum, drain).await {
        result
    } else {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        bail!("runtime task drain exceeded its deadline");
    }
}

async fn shutdown_resources(
    storage: StorageOwner,
    identifiers: Arc<SystemIdentifierSource>,
) -> Result<()> {
    let mut identifiers = Arc::try_unwrap(identifiers)
        .map_err(|_| anyhow::anyhow!("identifier actor references remain after listener drain"))?;
    identifiers
        .stop()
        .await
        .context("failed to stop identifier actor")?;
    match storage {
        StorageOwner::Memory(storage) => {
            let mut storage = Arc::try_unwrap(storage)
                .map_err(|_| anyhow::anyhow!("memory storage references remain after drain"))?;
            storage
                .stop()
                .await
                .context("failed to stop memory storage actor")
        }
        StorageOwner::Postgres(storage) => {
            let storage = Arc::try_unwrap(storage)
                .map_err(|_| anyhow::anyhow!("PostgreSQL references remain after drain"))?;
            storage.close().await;
            Ok(())
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime},
    };

    use anyhow::Context as _;
    use async_trait::async_trait;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use openfga_auth::{AuthenticationService, AuthorizationPolicy};
    use openfga_check::CheckBudget;
    use openfga_domain::{
        AuthorizationModelId, InputLimits, PrincipalId, RequestTimeout, StoreId, TokenCodec,
        TokenKey, TokenKeyId,
    };
    use openfga_model::ModelCompiler;
    use openfga_proto::openfga::v1::{self as pb, open_fga_service_client::OpenFgaServiceClient};
    use openfga_service::{
        AssertionService, ChangeService, CheckService, IdentifierSource, IdentifierSourceError,
        ModelPublication, ModelService, ServiceClock, StoreService, TupleService,
    };
    use openfga_storage::{
        AssertionReader, AssertionWriter, ChangeReader, ModelReader, ModelWriter, OperationContext,
        Page, PageOptions, StorageCancellationToken, StorageError, StorageErrorKind, StoreReader,
        StoreWriter, StoredAuthorizationModel, TupleReader, TupleWriter,
    };
    use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
    use openfga_transport::{OpenFgaApi, OpenFgaServices, TransportConfig};
    use rustls::server::ResolvesServerCertUsingSni;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        sync::{mpsc, watch},
        task::JoinSet,
        time::timeout,
    };
    use tower::ServiceExt;

    use super::{
        HealthState, TlsMaterial, TransportRuntime, drain_tasks, health_router, spawn_grpc,
        spawn_http,
    };

    const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

    #[derive(Debug)]
    struct FixedIdentifiers {
        store_id: StoreId,
        model_id: AuthorizationModelId,
    }

    #[async_trait]
    impl IdentifierSource for FixedIdentifiers {
        async fn next_store_id(
            &self,
            _context: &OperationContext,
        ) -> Result<StoreId, IdentifierSourceError> {
            Ok(self.store_id)
        }

        async fn next_model_id(
            &self,
            _context: &OperationContext,
        ) -> Result<AuthorizationModelId, IdentifierSourceError> {
            Ok(self.model_id)
        }
    }

    #[derive(Debug)]
    struct FixedClock;

    impl ServiceClock for FixedClock {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
        }
    }

    #[derive(Debug)]
    struct ActiveWorkGuard(Arc<AtomicUsize>);

    impl Drop for ActiveWorkGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }

    #[derive(Debug)]
    struct BlockingModelReader {
        active: Arc<AtomicUsize>,
        entered: mpsc::Sender<StorageCancellationToken>,
    }

    impl BlockingModelReader {
        async fn block<T>(&self, context: &OperationContext) -> Result<T, StorageError> {
            self.active.fetch_add(1, Ordering::AcqRel);
            let _guard = ActiveWorkGuard(Arc::clone(&self.active));
            self.entered
                .send(context.cancellation().clone())
                .await
                .map_err(|_| {
                    StorageError::new(
                        StorageErrorKind::Unavailable,
                        "runtime_cancellation_probe_unavailable",
                    )
                })?;
            context.cancellation().cancelled().await;
            Err(StorageError::new(
                StorageErrorKind::Cancelled,
                "runtime_cancellation_probe_cancelled",
            ))
        }
    }

    #[async_trait]
    impl ModelReader for BlockingModelReader {
        async fn read_model(
            &self,
            context: &OperationContext,
            _store_id: StoreId,
            _model_id: AuthorizationModelId,
        ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
            self.block(context).await
        }

        async fn read_latest_model(
            &self,
            context: &OperationContext,
            _store_id: StoreId,
        ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
            self.block(context).await
        }

        async fn list_models(
            &self,
            _context: &OperationContext,
            _store_id: StoreId,
            _options: &PageOptions,
        ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError> {
            Err(StorageError::new(
                StorageErrorKind::Unavailable,
                "runtime_cancellation_probe_list_unsupported",
            ))
        }
    }

    fn cancellation_api(
        entered: mpsc::Sender<StorageCancellationToken>,
        active: Arc<AtomicUsize>,
    ) -> anyhow::Result<(OpenFgaApi, AuthenticationService, Arc<MemoryStorage>)> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let stores: Arc<dyn StoreReader> = storage.clone();
        let store_writes: Arc<dyn StoreWriter> = storage.clone();
        let models: Arc<dyn ModelReader> = storage.clone();
        let model_writes: Arc<dyn ModelWriter> = storage.clone();
        let tuples: Arc<dyn TupleReader> = storage.clone();
        let tuple_writes: Arc<dyn TupleWriter> = storage.clone();
        let assertion_reads: Arc<dyn AssertionReader> = storage.clone();
        let assertion_writes: Arc<dyn AssertionWriter> = storage.clone();
        let changes: Arc<dyn ChangeReader> = storage.clone();
        let blocking_models: Arc<dyn ModelReader> =
            Arc::new(BlockingModelReader { active, entered });
        let identifiers: Arc<dyn IdentifierSource> = Arc::new(FixedIdentifiers {
            store_id: STORE_ID.parse()?,
            model_id: MODEL_ID.parse()?,
        });
        let limits = InputLimits::default();
        let principal_id = "runtime-cancellation".parse::<PrincipalId>()?;
        let authentication = AuthenticationService::development(principal_id.clone());
        let services = OpenFgaServices::builder()
            .stores(StoreService::new(
                stores.clone(),
                store_writes,
                identifiers.clone(),
            ))
            .models(ModelService::new(
                stores.clone(),
                models.clone(),
                model_writes,
                ModelPublication::new(identifiers, Arc::new(FixedClock), ModelCompiler::default()),
            ))
            .assertions(AssertionService::new(
                stores.clone(),
                models,
                assertion_reads,
                assertion_writes,
                limits.clone(),
            ))
            .tuples(TupleService::new(
                stores.clone(),
                blocking_models.clone(),
                tuples.clone(),
                tuple_writes,
                limits.clone(),
            ))
            .changes(ChangeService::new(stores, changes))
            .checks(CheckService::direct(
                blocking_models,
                tuples,
                CheckBudget::default(),
            ))
            .build();
        let api = OpenFgaApi::new(
            services,
            TransportConfig::builder()
                .limits(limits.clone())
                .authorization_policy(Arc::new(AuthorizationPolicy::development(principal_id)))
                .token_codec(Arc::new(TokenCodec::new(
                    TokenKey::new("runtime".parse::<TokenKeyId>()?, vec![11; 32])?,
                    Vec::new(),
                    &limits,
                )?))
                .request_timeout(RequestTimeout::new(Duration::from_secs(5))?)
                .build(),
        )
        .map_err(anyhow::Error::msg)?;
        Ok((api, authentication, storage))
    }

    async fn wait_for_request_cancellation(
        cancellation: StorageCancellationToken,
        active: &AtomicUsize,
    ) -> anyhow::Result<()> {
        timeout(Duration::from_secs(1), async {
            while !cancellation.is_cancelled() || active.load(Ordering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_should_report_readiness_transitions() -> anyhow::Result<()> {
        let state = Arc::new(HealthState::new());
        let router = health_router(Arc::clone(&state));
        let unavailable = router
            .clone()
            .oneshot(Request::get("/readyz").body(Body::empty())?)
            .await?;
        assert_eq!(
            unavailable.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        let _previous_readiness = state.set_ready(true);
        let ready = router
            .oneshot(Request::get("/readyz").body(Body::empty())?)
            .await?;
        assert_eq!(ready.status(), axum::http::StatusCode::OK);
        tokio::time::timeout(Duration::from_secs(1), async {}).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_should_publish_tls_atomically_and_retain_identity_on_invalid_reload()
    -> anyhow::Result<()> {
        let active = empty_tls_config()?;
        let material = TlsMaterial {
            http_config: active.clone(),
            certificate_path: PathBuf::from("/dev/null"),
            private_key_path: PathBuf::from("/dev/null"),
            read_timeout: Duration::from_secs(1),
            reload_interval: Duration::from_secs(30),
        };
        let health = HealthState::new();
        let _previous = health.set_ready(true);
        let before = active.get_inner();

        assert!(material.reload_once().await.is_err());
        assert!(Arc::ptr_eq(&before, &active.get_inner()));
        assert!(health.ready.load(Ordering::Acquire));

        let candidate = empty_tls_config()?;
        material.publish(&candidate);
        assert!(!Arc::ptr_eq(&before, &active.get_inner()));
        Ok(())
    }

    #[tokio::test]
    async fn test_should_bound_shutdown_with_an_in_flight_client() -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (entered_tx, mut entered_rx) = mpsc::channel(1);
        let router = axum::Router::new().route(
            "/slow",
            get(move || {
                let entered_tx = entered_tx.clone();
                async move {
                    let _send_result = entered_tx.send(()).await;
                    std::future::pending::<StatusCode>().await
                }
            }),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();
        spawn_http(
            &mut tasks,
            listener,
            router,
            None,
            shutdown_rx,
            Duration::from_millis(25),
        )?;

        let mut client = tokio::net::TcpStream::connect(address).await?;
        client
            .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        entered_rx
            .recv()
            .await
            .context("request was not admitted")?;
        shutdown_tx.send(true)?;

        drain_tasks(&mut tasks, Duration::from_millis(100)).await?;
        assert!(tasks.is_empty());
        let mut byte = [0_u8; 1];
        let closed = timeout(Duration::from_secs(1), client.read(&mut byte)).await?;
        assert!(matches!(closed, Ok(0) | Err(_)));
        Ok(())
    }

    #[tokio::test]
    async fn test_should_cancel_http_and_grpc_storage_work_on_client_disconnect()
    -> anyhow::Result<()> {
        let (entered_tx, mut entered_rx) = mpsc::channel(2);
        let active = Arc::new(AtomicUsize::new(0));
        let (api, authentication, storage) = cancellation_api(entered_tx, Arc::clone(&active))?;
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let http_address = http_listener.local_addr()?;
        let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let grpc_address = grpc_listener.local_addr()?;
        let config_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/openfga-development.yaml");
        let config = super::ServerConfig::load(&config_path).await?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();
        spawn_http(
            &mut tasks,
            http_listener,
            super::http_router(api.clone(), authentication.clone()),
            None,
            shutdown_rx.clone(),
            Duration::from_secs(1),
        )?;
        let (_health_reporter, health_service) = tonic_health::server::health_reporter();
        spawn_grpc(
            &mut tasks,
            grpc_listener,
            TransportRuntime {
                api: api.clone(),
                authentication,
            },
            health_service,
            None,
            shutdown_rx,
            &config,
        )?;
        drop(api);

        let body = serde_json::to_vec(&serde_json::json!({
            "tuple_key": {
                "user": "user:anne",
                "relation": "viewer",
                "object": "document:roadmap"
            },
            "authorization_model_id": MODEL_ID
        }))?;
        let head = format!(
            "POST /stores/{STORE_ID}/check HTTP/1.1\r\nHost: localhost\r\nContent-Type: \
             application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len(),
        );
        let mut http_client = tokio::net::TcpStream::connect(http_address).await?;
        http_client.write_all(head.as_bytes()).await?;
        http_client.write_all(&body).await?;
        let http_cancellation = timeout(Duration::from_secs(1), entered_rx.recv())
            .await?
            .context("HTTP request did not reach storage")?;
        drop(http_client);
        wait_for_request_cancellation(http_cancellation, active.as_ref()).await?;

        let mut grpc_client =
            OpenFgaServiceClient::connect(format!("http://{grpc_address}")).await?;
        let grpc_request = pb::CheckRequest {
            store_id: STORE_ID.to_owned(),
            tuple_key: Some(pb::CheckRequestTupleKey {
                user: "user:anne".to_owned(),
                relation: "viewer".to_owned(),
                object: "document:roadmap".to_owned(),
            }),
            contextual_tuples: None,
            authorization_model_id: MODEL_ID.to_owned(),
            trace: false,
            context: None,
            consistency: 0,
        };
        let grpc_request_task = tokio::spawn(async move { grpc_client.check(grpc_request).await });
        let grpc_cancellation = timeout(Duration::from_secs(1), entered_rx.recv())
            .await?
            .context("gRPC request did not reach storage")?;
        grpc_request_task.abort();
        let request_result = grpc_request_task.await;
        assert!(request_result.is_err_and(|error| error.is_cancelled()));
        wait_for_request_cancellation(grpc_cancellation, active.as_ref()).await?;

        shutdown_tx.send(true)?;
        drain_tasks(&mut tasks, Duration::from_secs(2)).await?;
        assert!(tasks.is_empty());
        assert_eq!(active.load(Ordering::Acquire), 0);
        let mut storage = Arc::try_unwrap(storage)
            .map_err(|_| anyhow::anyhow!("runtime cancellation storage references remain"))?;
        storage.stop().await?;
        Ok(())
    }

    fn empty_tls_config() -> anyhow::Result<axum_server::tls_rustls::RustlsConfig> {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(ResolvesServerCertUsingSni::new()));
        Ok(axum_server::tls_rustls::RustlsConfig::from_config(
            Arc::new(config),
        ))
    }
}
