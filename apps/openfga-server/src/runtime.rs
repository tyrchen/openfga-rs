//! Production service assembly, health supervision, listeners, and drain.

use std::{
    env, fmt,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::Path,
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
use secrecy::SecretString;
use serde::Serialize;
use tokio::{
    io::AsyncReadExt,
    sync::watch,
    task::JoinSet,
    time::{MissedTickBehavior, timeout},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
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

struct TlsMaterial {
    certificate: Vec<u8>,
    private_key: Vec<u8>,
    http_config: axum_server::tls_rustls::RustlsConfig,
}

impl fmt::Debug for TlsMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsMaterial")
            .field("certificate_bytes", &self.certificate.len())
            .field("private_key", &"[REDACTED]")
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
    spawn_http(
        &mut tasks,
        http_listener,
        http_router(api.clone(), authentication.clone())
            .merge(health_router(Arc::clone(&health_state))),
        tls.as_ref(),
        shutdown_rx.clone(),
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
    let token_key = load_token_key(config)?;
    let token_codec = Arc::new(
        TokenCodec::new(token_key, Vec::new(), &limits)
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

fn load_token_key(config: &ServerConfig) -> Result<TokenKey> {
    let encoded = load_secret_string(&config.transport.token_key_env)?;
    let bytes = BASE64_STANDARD
        .decode(encoded.as_bytes())
        .context("continuation token key must be standard base64")?;
    TokenKey::new(config.transport.token_key_id.parse()?, bytes)
        .context("continuation token key must decode to 32 through 64 bytes")
}

fn load_secret(name: &str) -> Result<SecretString> {
    load_secret_string(name).map(SecretString::from)
}

fn load_secret_string(name: &str) -> Result<String> {
    let value =
        env::var(name).with_context(|| format!("required secret environment {name} is unset"))?;
    if value.is_empty()
        || value.len() > MAXIMUM_SECRET_ENV_BYTES
        || value.chars().any(char::is_control)
    {
        bail!("secret environment {name} is empty, oversized, or contains control characters");
    }
    Ok(value)
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
    let duration = config.request_timeout()?.duration();
    let certificate = timeout(duration, read_bounded_file(certificate_path))
        .await
        .context("TLS certificate read timed out")??;
    let private_key = timeout(duration, read_bounded_file(private_key_path))
        .await
        .context("TLS private key read timed out")??;
    let http_config =
        axum_server::tls_rustls::RustlsConfig::from_pem(certificate.clone(), private_key.clone())
            .await
            .context("HTTP TLS material is invalid")?;
    let _grpc_validation = Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(certificate.clone(), private_key.clone())),
        )
        .context("gRPC TLS material is invalid")?;
    Ok(Some(TlsMaterial {
        certificate,
        private_key,
        http_config,
    }))
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
) -> Result<()> {
    let listener = listener
        .into_std()
        .context("failed to transfer the HTTP listener")?;
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    let shutdown = async move {
        wait_for_shutdown(shutdown).await;
        shutdown_handle.graceful_shutdown(None);
    };
    if let Some(tls) = tls {
        let rustls = tls.http_config.clone();
        let server = async move {
            let serve = axum_server::from_tcp_rustls(listener, rustls)
                .context("failed to configure HTTP TLS listener")?
                .handle(handle)
                .serve(router.into_make_service());
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
                .serve(router.into_make_service());
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
    let mut server = Server::builder()
        .timeout(config.request_timeout()?.duration())
        .concurrency_limit_per_connection(config.transport.maximum_concurrency)
        .load_shed(true);
    if let Some(tls) = tls {
        server = server
            .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(
                tls.certificate.clone(),
                tls.private_key.clone(),
            )))
            .context("gRPC TLS material is invalid")?;
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
    use std::{sync::Arc, time::Duration};

    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    use super::{HealthState, health_router};

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
}
