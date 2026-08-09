//! SSRF-hardened OIDC discovery, JWKS refresh, and JWT validation.

use std::{
    collections::{HashMap, HashSet},
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{JwkSet, KeyOperations, PublicKeyUse},
};
use openfga_domain::{Principal, PrincipalKind};
use reqwest::{Client, StatusCode, Url, redirect::Policy};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use typed_builder::TypedBuilder;

use crate::AuthenticationError;

const MAXIMUM_ISSUER_BYTES: usize = 2_048;
const MAXIMUM_AUDIENCES: usize = 16;
const MAXIMUM_AUTHORIZED_PARTIES: usize = 16;
const MAXIMUM_ALLOWED_HOSTS: usize = 16;
const MAXIMUM_CLAIM_BYTES: usize = 256;
const MAXIMUM_KEY_ID_BYTES: usize = 128;
const MAXIMUM_KEYS: usize = 64;
const MAXIMUM_DOCUMENT_BYTES: usize = 1_048_576;
const MAXIMUM_TOKEN_BYTES: usize = 16 * 1_024;
const MAXIMUM_CLOCK_SKEW: Duration = Duration::from_mins(5);
const MAXIMUM_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAXIMUM_REFRESH_INTERVAL: Duration = Duration::from_hours(24);
const MAXIMUM_STALE_GRACE: Duration = Duration::from_hours(168);
const ON_DEMAND_REFRESH_MINIMUM: Duration = Duration::from_mins(1);

/// An explicitly supported asymmetric OIDC signing algorithm.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
pub enum OidcAlgorithm {
    /// ECDSA using P-256 and SHA-256.
    ES256,
    /// ECDSA using P-384 and SHA-384.
    ES384,
    /// RSA PKCS#1 v1.5 using SHA-256.
    RS256,
    /// RSA PKCS#1 v1.5 using SHA-384.
    RS384,
    /// RSA PKCS#1 v1.5 using SHA-512.
    RS512,
    /// RSA-PSS using SHA-256.
    PS256,
    /// RSA-PSS using SHA-384.
    PS384,
    /// RSA-PSS using SHA-512.
    PS512,
    /// Ed25519 signatures.
    EdDSA,
}

impl OidcAlgorithm {
    const fn algorithm(self) -> Algorithm {
        match self {
            Self::ES256 => Algorithm::ES256,
            Self::ES384 => Algorithm::ES384,
            Self::RS256 => Algorithm::RS256,
            Self::RS384 => Algorithm::RS384,
            Self::RS512 => Algorithm::RS512,
            Self::PS256 => Algorithm::PS256,
            Self::PS384 => Algorithm::PS384,
            Self::PS512 => Algorithm::PS512,
            Self::EdDSA => Algorithm::EdDSA,
        }
    }
}

/// Validated OIDC discovery, claims, network, and refresh policy.
#[derive(Clone, TypedBuilder)]
pub struct OidcConfig {
    /// Exact HTTPS issuer expected in discovery and tokens.
    pub issuer: String,
    /// Accepted token audiences.
    pub audiences: Vec<String>,
    /// Accepted `azp` values; an empty collection disables the optional check.
    #[builder(default)]
    pub authorized_parties: Vec<String>,
    /// Allowed asymmetric signing algorithms.
    #[builder(default = vec![OidcAlgorithm::RS256])]
    pub algorithms: Vec<OidcAlgorithm>,
    /// Additional exact DNS hosts allowed for discovery/JWKS fetching.
    #[builder(default)]
    pub allowed_hosts: Vec<String>,
    /// Maximum encoded bearer-token bytes.
    #[builder(default = 8_192)]
    pub maximum_token_bytes: usize,
    /// Maximum discovery or JWKS response bytes.
    #[builder(default = 256 * 1_024)]
    pub maximum_document_bytes: usize,
    /// DNS/connect/request timeout.
    #[builder(default = Duration::from_secs(5))]
    pub fetch_timeout: Duration,
    /// JWT temporal validation leeway.
    #[builder(default = Duration::from_secs(30))]
    pub clock_skew: Duration,
    /// Successful JWKS refresh cadence.
    #[builder(default = Duration::from_hours(1))]
    pub refresh_interval: Duration,
    /// Time after the last successful refresh before keys fail closed.
    #[builder(default = Duration::from_hours(24))]
    pub stale_key_grace: Duration,
}

impl OidcConfig {
    /// Validates local configuration without performing network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`OidcError::InvalidConfiguration`] for an unsafe or unbounded setting.
    pub fn validate(&self) -> Result<(), OidcError> {
        ParsedConfig::try_from(self.clone()).map(|_| ())
    }
}

impl fmt::Debug for OidcConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcConfig")
            .field("issuer", &"[REDACTED]")
            .field("audience_count", &self.audiences.len())
            .field("authorized_party_count", &self.authorized_parties.len())
            .field("algorithms", &self.algorithms)
            .field("allowed_host_count", &self.allowed_hosts.len())
            .field("maximum_token_bytes", &self.maximum_token_bytes)
            .field("maximum_document_bytes", &self.maximum_document_bytes)
            .field("fetch_timeout", &self.fetch_timeout)
            .field("clock_skew", &self.clock_skew)
            .field("refresh_interval", &self.refresh_interval)
            .field("stale_key_grace", &self.stale_key_grace)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct ParsedConfig {
    issuer: String,
    audiences: Vec<String>,
    authorized_parties: HashSet<String>,
    algorithms: Vec<Algorithm>,
    allowed_hosts: HashSet<String>,
    maximum_token_bytes: usize,
    maximum_document_bytes: usize,
    fetch_timeout: Duration,
    clock_skew: Duration,
    refresh_interval: Duration,
    stale_key_grace: Duration,
}

impl TryFrom<OidcConfig> for ParsedConfig {
    type Error = OidcError;

    fn try_from(config: OidcConfig) -> Result<Self, Self::Error> {
        if config.issuer.is_empty()
            || config.issuer.len() > MAXIMUM_ISSUER_BYTES
            || config.issuer.ends_with('/')
        {
            return Err(OidcError::InvalidConfiguration("issuer"));
        }
        let issuer_url =
            Url::parse(&config.issuer).map_err(|_| OidcError::InvalidConfiguration("issuer"))?;
        validate_https_url(&issuer_url)?;
        let issuer_host = issuer_url
            .host_str()
            .ok_or(OidcError::InvalidConfiguration("issuer_host"))?
            .to_ascii_lowercase();
        if issuer_host.parse::<IpAddr>().is_ok() {
            return Err(OidcError::InvalidConfiguration("issuer_host"));
        }
        validate_claim_list(&config.audiences, MAXIMUM_AUDIENCES, false, "audiences")?;
        validate_claim_list(
            &config.authorized_parties,
            MAXIMUM_AUTHORIZED_PARTIES,
            true,
            "authorized_parties",
        )?;
        if config.algorithms.is_empty() || config.algorithms.len() > 9 {
            return Err(OidcError::InvalidConfiguration("algorithms"));
        }
        let mut algorithms = config
            .algorithms
            .into_iter()
            .map(OidcAlgorithm::algorithm)
            .collect::<Vec<_>>();
        algorithms.sort_by_key(|algorithm| *algorithm as u8);
        algorithms.dedup();
        if !(1..=MAXIMUM_TOKEN_BYTES).contains(&config.maximum_token_bytes) {
            return Err(OidcError::InvalidConfiguration("maximum_token_bytes"));
        }
        if !(1..=MAXIMUM_DOCUMENT_BYTES).contains(&config.maximum_document_bytes) {
            return Err(OidcError::InvalidConfiguration("maximum_document_bytes"));
        }
        validate_duration(config.fetch_timeout, MAXIMUM_FETCH_TIMEOUT, "fetch_timeout")?;
        if config.clock_skew > MAXIMUM_CLOCK_SKEW {
            return Err(OidcError::InvalidConfiguration("clock_skew"));
        }
        validate_duration(
            config.refresh_interval,
            MAXIMUM_REFRESH_INTERVAL,
            "refresh_interval",
        )?;
        validate_duration(
            config.stale_key_grace,
            MAXIMUM_STALE_GRACE,
            "stale_key_grace",
        )?;
        if config.stale_key_grace < config.refresh_interval {
            return Err(OidcError::InvalidConfiguration("stale_key_grace"));
        }
        if config.allowed_hosts.len() > MAXIMUM_ALLOWED_HOSTS {
            return Err(OidcError::InvalidConfiguration("allowed_hosts"));
        }
        let mut allowed_hosts =
            HashSet::with_capacity(config.allowed_hosts.len().saturating_add(1));
        allowed_hosts.insert(issuer_host);
        for host in config.allowed_hosts {
            validate_dns_name(&host)?;
            allowed_hosts.insert(host.to_ascii_lowercase());
        }
        Ok(Self {
            issuer: config.issuer,
            audiences: config.audiences,
            authorized_parties: config.authorized_parties.into_iter().collect(),
            algorithms,
            allowed_hosts,
            maximum_token_bytes: config.maximum_token_bytes,
            maximum_document_bytes: config.maximum_document_bytes,
            fetch_timeout: config.fetch_timeout,
            clock_skew: config.clock_skew,
            refresh_interval: config.refresh_interval,
            stale_key_grace: config.stale_key_grace,
        })
    }
}

#[derive(Debug)]
struct VerificationKey {
    algorithm: Option<Algorithm>,
    key: DecodingKey,
}

#[derive(Debug)]
struct KeySnapshot {
    keys: HashMap<String, VerificationKey>,
    refreshed_at: Instant,
}

/// Request-path OIDC verifier backed by atomically published actor state.
#[derive(Clone)]
pub struct OidcAuthenticator {
    config: Arc<ParsedConfig>,
    keys: watch::Receiver<Arc<KeySnapshot>>,
    refresh: mpsc::Sender<()>,
}

impl OidcAuthenticator {
    pub(crate) fn maximum_token_bytes(&self) -> usize {
        self.config.maximum_token_bytes
    }

    pub(crate) fn is_ready(&self) -> bool {
        !snapshot_is_stale(&self.keys.borrow(), self.config.stale_key_grace)
    }

    pub(crate) fn authenticate(&self, token: &str) -> Result<Principal, AuthenticationError> {
        if token.len() > self.config.maximum_token_bytes {
            return Err(AuthenticationError::InvalidCredentials);
        }
        let snapshot = self.keys.borrow().clone();
        if snapshot_is_stale(&snapshot, self.config.stale_key_grace) {
            return Err(AuthenticationError::Unavailable);
        }
        let header = decode_header(token).map_err(|_| AuthenticationError::InvalidCredentials)?;
        if header.jku.is_some()
            || header.jwk.is_some()
            || header.x5u.is_some()
            || header.x5c.is_some()
            || header
                .crit
                .as_ref()
                .is_some_and(|critical| !critical.is_empty())
            || !self.config.algorithms.contains(&header.alg)
        {
            return Err(AuthenticationError::InvalidCredentials);
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|value| valid_key_id(value))
            .ok_or(AuthenticationError::InvalidCredentials)?;
        let Some(key) = snapshot.keys.get(kid) else {
            let _ = self.refresh.try_send(());
            return Err(AuthenticationError::InvalidCredentials);
        };
        if key
            .algorithm
            .is_some_and(|algorithm| algorithm != header.alg)
            || key.key.family() != header.alg.family()
        {
            return Err(AuthenticationError::InvalidCredentials);
        }
        let mut validation = Validation::new(header.alg);
        validation.algorithms = vec![header.alg];
        validation.leeway = self.config.clock_skew.as_secs();
        validation.validate_nbf = true;
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&self.config.audiences);
        let claims = decode::<Claims>(token, &key.key, &validation)
            .map_err(|_| AuthenticationError::InvalidCredentials)?
            .claims;
        validate_claims(&claims, &self.config)?;
        let principal_id = claims
            .sub
            .parse()
            .map_err(|_| AuthenticationError::InvalidCredentials)?;
        Ok(Principal::new(PrincipalKind::OpenIdConnect, principal_id))
    }
}

impl fmt::Debug for OidcAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcAuthenticator")
            .field("configuration", &"[REDACTED]")
            .field("keys", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Actor that refreshes and atomically publishes OIDC verification keys.
pub struct JwksActor {
    config: Arc<ParsedConfig>,
    keys: watch::Sender<Arc<KeySnapshot>>,
    refresh: mpsc::Receiver<()>,
}

impl JwksActor {
    pub(crate) async fn initialize(
        config: OidcConfig,
    ) -> Result<(OidcAuthenticator, Self), OidcError> {
        let config = Arc::new(ParsedConfig::try_from(config)?);
        let snapshot = Arc::new(fetch_snapshot(&config).await?);
        let (keys, receiver) = watch::channel(snapshot);
        let (refresh_sender, refresh) = mpsc::channel(1);
        Ok((
            OidcAuthenticator {
                config: config.clone(),
                keys: receiver,
                refresh: refresh_sender,
            },
            Self {
                config,
                keys,
                refresh,
            },
        ))
    }

    /// Runs refresh supervision until shutdown is requested.
    ///
    /// Fetch failures retain the last verified set only for the configured stale-key grace.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let start = tokio::time::Instant::now() + self.config.refresh_interval;
        let mut interval = tokio::time::interval_at(start, self.config.refresh_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_attempt = Instant::now();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.refresh_once().await;
                    last_attempt = Instant::now();
                },
                message = self.refresh.recv() => {
                    if message.is_none() {
                        return;
                    }
                    if last_attempt.elapsed() >= ON_DEMAND_REFRESH_MINIMUM {
                        self.refresh_once().await;
                        last_attempt = Instant::now();
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    async fn refresh_once(&self) {
        match fetch_snapshot(&self.config).await {
            Ok(snapshot) => {
                self.keys.send_replace(Arc::new(snapshot));
                tracing::info!(dependency = "oidc_jwks", "authentication keys refreshed");
            }
            Err(error) => {
                tracing::warn!(
                    dependency = "oidc_jwks",
                    error_kind = error.kind(),
                    "authentication key refresh failed"
                );
            }
        }
    }
}

impl fmt::Debug for JwksActor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwksActor")
            .field("configuration", &"[REDACTED]")
            .field("keys", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    jwks_uri: String,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    azp: Option<String>,
    #[serde(default)]
    iat: Option<u64>,
}

async fn fetch_snapshot(config: &ParsedConfig) -> Result<KeySnapshot, OidcError> {
    let discovery_url = Url::parse(&format!(
        "{}/.well-known/openid-configuration",
        config.issuer
    ))
    .map_err(|_| OidcError::InvalidConfiguration("issuer"))?;
    let discovery_bytes = fetch_document(config, &discovery_url).await?;
    let discovery: DiscoveryDocument =
        serde_json::from_slice(&discovery_bytes).map_err(OidcError::InvalidDiscoveryDocument)?;
    if discovery.issuer != config.issuer || discovery.jwks_uri.len() > MAXIMUM_ISSUER_BYTES {
        return Err(OidcError::InvalidDiscovery);
    }
    let jwks_url = Url::parse(&discovery.jwks_uri).map_err(|_| OidcError::InvalidDiscovery)?;
    validate_fetch_url(config, &jwks_url)?;
    let jwks_bytes = fetch_document(config, &jwks_url).await?;
    let jwks: JwkSet =
        serde_json::from_slice(&jwks_bytes).map_err(OidcError::InvalidJwksDocument)?;
    Ok(KeySnapshot {
        keys: parse_key_set(jwks, &config.algorithms)?,
        refreshed_at: Instant::now(),
    })
}

async fn fetch_document(config: &ParsedConfig, url: &Url) -> Result<Vec<u8>, OidcError> {
    validate_fetch_url(config, url)?;
    let host = url.host_str().ok_or(OidcError::InvalidFetchUrl)?;
    let port = url
        .port_or_known_default()
        .ok_or(OidcError::InvalidFetchUrl)?;
    let addresses =
        tokio::time::timeout(config.fetch_timeout, tokio::net::lookup_host((host, port)))
            .await
            .map_err(|_| OidcError::DnsTimeout)?
            .map_err(OidcError::Dns)?
            .collect::<Vec<_>>();
    validate_resolved_addresses(&addresses)?;
    let client = pinned_client(host, &addresses, config.fetch_timeout)?;
    let mut response = client
        .get(url.clone())
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|error| OidcError::Request(error.without_url()))?;
    if response.status() != StatusCode::OK {
        return Err(OidcError::ResponseStatus);
    }
    if response
        .content_length()
        .is_some_and(|length| length > config.maximum_document_bytes as u64)
    {
        return Err(OidcError::DocumentTooLarge);
    }
    let mut contents = Vec::with_capacity(config.maximum_document_bytes.min(64 * 1_024));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| OidcError::ResponseBody(error.without_url()))?
    {
        append_document_chunk(&mut contents, &chunk, config.maximum_document_bytes)?;
    }
    Ok(contents)
}

fn validate_resolved_addresses(addresses: &[SocketAddr]) -> Result<(), OidcError> {
    if addresses.is_empty() || addresses.iter().any(|address| !is_public_ip(address.ip())) {
        return Err(OidcError::ForbiddenAddress);
    }
    Ok(())
}

fn append_document_chunk(
    contents: &mut Vec<u8>,
    chunk: &[u8],
    maximum_bytes: usize,
) -> Result<(), OidcError> {
    let next_length = contents
        .len()
        .checked_add(chunk.len())
        .ok_or(OidcError::DocumentTooLarge)?;
    if next_length > maximum_bytes {
        return Err(OidcError::DocumentTooLarge);
    }
    contents.extend_from_slice(chunk);
    Ok(())
}

fn pinned_client(
    host: &str,
    addresses: &[SocketAddr],
    timeout: Duration,
) -> Result<Client, OidcError> {
    Client::builder()
        .https_only(true)
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(timeout)
        .timeout(timeout)
        .resolve_to_addrs(host, addresses)
        .build()
        .map_err(|error| OidcError::Client(error.without_url()))
}

fn parse_key_set(
    jwks: JwkSet,
    allowed_algorithms: &[Algorithm],
) -> Result<HashMap<String, VerificationKey>, OidcError> {
    if jwks.keys.is_empty() || jwks.keys.len() > MAXIMUM_KEYS {
        return Err(OidcError::InvalidKeySet);
    }
    let mut keys = HashMap::with_capacity(jwks.keys.len());
    for jwk in jwks.keys {
        if jwk
            .common
            .public_key_use
            .as_ref()
            .is_some_and(|key_use| !matches!(key_use, PublicKeyUse::Signature))
        {
            continue;
        }
        if jwk
            .common
            .key_operations
            .as_ref()
            .is_some_and(|operations| {
                operations.is_empty()
                    || operations
                        .iter()
                        .any(|operation| !matches!(operation, KeyOperations::Verify))
            })
        {
            return Err(OidcError::InvalidKeySet);
        }
        let algorithm = jwk
            .common
            .key_algorithm
            .and_then(|algorithm| Algorithm::try_from(algorithm).ok());
        if algorithm.is_some_and(|algorithm| !allowed_algorithms.contains(&algorithm)) {
            continue;
        }
        let kid = jwk
            .common
            .key_id
            .as_deref()
            .filter(|value| valid_key_id(value))
            .ok_or(OidcError::InvalidKeySet)?
            .to_owned();
        let key = DecodingKey::from_jwk(&jwk).map_err(OidcError::InvalidKey)?;
        if algorithm.is_some_and(|algorithm| key.family() != algorithm.family())
            || keys
                .insert(kid, VerificationKey { algorithm, key })
                .is_some()
        {
            return Err(OidcError::InvalidKeySet);
        }
    }
    if keys.is_empty() {
        return Err(OidcError::InvalidKeySet);
    }
    Ok(keys)
}

fn validate_claims(claims: &Claims, config: &ParsedConfig) -> Result<(), AuthenticationError> {
    if claims.sub.is_empty()
        || claims.sub.len() > MAXIMUM_CLAIM_BYTES
        || claims.sub.bytes().any(|byte| !byte.is_ascii_graphic())
    {
        return Err(AuthenticationError::InvalidCredentials);
    }
    if !config.authorized_parties.is_empty()
        && !claims
            .azp
            .as_ref()
            .is_some_and(|party| config.authorized_parties.contains(party))
    {
        return Err(AuthenticationError::InvalidCredentials);
    }
    if let Some(issued_at) = claims.iat {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthenticationError::Unavailable)?
            .as_secs();
        if issued_at > now.saturating_add(config.clock_skew.as_secs()) {
            return Err(AuthenticationError::InvalidCredentials);
        }
    }
    Ok(())
}

fn snapshot_is_stale(snapshot: &KeySnapshot, grace: Duration) -> bool {
    snapshot.refreshed_at.elapsed() > grace
}

fn validate_fetch_url(config: &ParsedConfig, url: &Url) -> Result<(), OidcError> {
    validate_https_url(url)?;
    let host = url.host_str().ok_or(OidcError::InvalidFetchUrl)?;
    if host.parse::<IpAddr>().is_ok() || !config.allowed_hosts.contains(&host.to_ascii_lowercase())
    {
        return Err(OidcError::ForbiddenHost);
    }
    Ok(())
}

fn validate_https_url(url: &Url) -> Result<(), OidcError> {
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err(OidcError::InvalidFetchUrl);
    }
    Ok(())
}

fn validate_claim_list(
    values: &[String],
    maximum_count: usize,
    allow_empty: bool,
    field: &'static str,
) -> Result<(), OidcError> {
    if (!allow_empty && values.is_empty()) || values.len() > maximum_count {
        return Err(OidcError::InvalidConfiguration(field));
    }
    let mut unique = HashSet::with_capacity(values.len());
    if values.iter().any(|value| {
        value.is_empty()
            || value.len() > MAXIMUM_CLAIM_BYTES
            || value.bytes().any(|byte| byte.is_ascii_control())
            || !unique.insert(value)
    }) {
        return Err(OidcError::InvalidConfiguration(field));
    }
    Ok(())
}

fn validate_dns_name(host: &str) -> Result<(), OidcError> {
    if host.is_empty()
        || host.len() > 253
        || host.parse::<IpAddr>().is_ok()
        || host.starts_with('.')
        || host.ends_with('.')
        || host.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(OidcError::InvalidConfiguration("allowed_hosts"));
    }
    Ok(())
}

fn validate_duration(
    value: Duration,
    maximum: Duration,
    field: &'static str,
) -> Result<(), OidcError> {
    if value.is_zero() || value > maximum {
        return Err(OidcError::InvalidConfiguration(field));
    }
    Ok(())
}

fn valid_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAXIMUM_KEY_ID_BYTES
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    !address.is_private()
        && !address.is_loopback()
        && !address.is_link_local()
        && !address.is_unspecified()
        && !address.is_multicast()
        && !address.is_broadcast()
        && !address.is_documentation()
        && octets[0] != 0
        && !(octets[0] == 100 && (64..=127).contains(&octets[1]))
        && !(octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        && !(octets[0] == 198 && matches!(octets[1], 18 | 19))
        && octets[0] < 240
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    let forbidden = address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        || address.is_unique_local()
        || address.is_unicast_link_local()
        || address.to_ipv4_mapped().is_some()
        || (segments[0] & 0xe000) != 0x2000
        || (segments[0] == 0x2001 && segments[1] < 0x0200)
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || segments[0] == 0x2002
        || (segments[0] == 0x3fff && segments[1] & 0xf000 == 0);
    !forbidden
}

/// A redacted OIDC configuration, discovery, or refresh failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OidcError {
    /// A local setting is missing, unsafe, or outside its finite range.
    #[error("OIDC configuration field is invalid: {0}")]
    InvalidConfiguration(&'static str),
    /// A configured or discovered URL violates the HTTPS/credential policy.
    #[error("OIDC endpoint URL is invalid")]
    InvalidFetchUrl,
    /// A discovered host is outside the explicit allowlist.
    #[error("OIDC endpoint host is not allowed")]
    ForbiddenHost,
    /// DNS resolved an endpoint to a non-public address.
    #[error("OIDC endpoint resolved to a forbidden address")]
    ForbiddenAddress,
    /// DNS resolution failed.
    #[error("OIDC endpoint DNS resolution failed")]
    Dns(#[source] io::Error),
    /// DNS resolution exceeded its configured timeout.
    #[error("OIDC endpoint DNS resolution timed out")]
    DnsTimeout,
    /// The bounded HTTP client could not be constructed.
    #[error("OIDC HTTP client construction failed")]
    Client(#[source] reqwest::Error),
    /// A discovery or JWKS request failed.
    #[error("OIDC endpoint request failed")]
    Request(#[source] reqwest::Error),
    /// An endpoint returned a non-success status.
    #[error("OIDC endpoint returned an invalid status")]
    ResponseStatus,
    /// Streaming the bounded response failed.
    #[error("OIDC endpoint response body failed")]
    ResponseBody(#[source] reqwest::Error),
    /// A discovery or JWKS response exceeded its configured byte ceiling.
    #[error("OIDC endpoint response exceeded its size limit")]
    DocumentTooLarge,
    /// Discovery JSON was malformed.
    #[error("OIDC discovery document is invalid")]
    InvalidDiscoveryDocument(#[source] serde_json::Error),
    /// Discovery metadata did not bind to the configured issuer.
    #[error("OIDC discovery metadata is invalid")]
    InvalidDiscovery,
    /// JWKS JSON was malformed.
    #[error("OIDC JWKS document is invalid")]
    InvalidJwksDocument(#[source] serde_json::Error),
    /// A JWK could not be converted into a verification key.
    #[error("OIDC verification key is invalid")]
    InvalidKey(#[source] jsonwebtoken::errors::Error),
    /// The JWKS is empty, oversized, ambiguous, or contains a disallowed key.
    #[error("OIDC key set is invalid")]
    InvalidKeySet,
}

impl OidcError {
    const fn kind(&self) -> &'static str {
        match self {
            Self::InvalidConfiguration(_) => "invalid_configuration",
            Self::InvalidFetchUrl => "invalid_url",
            Self::ForbiddenHost => "forbidden_host",
            Self::ForbiddenAddress => "forbidden_address",
            Self::Dns(_) => "dns",
            Self::DnsTimeout => "dns_timeout",
            Self::Client(_) => "client",
            Self::Request(_) => "request",
            Self::ResponseStatus => "status",
            Self::ResponseBody(_) => "body",
            Self::DocumentTooLarge => "oversize",
            Self::InvalidDiscoveryDocument(_) | Self::InvalidDiscovery => "discovery",
            Self::InvalidJwksDocument(_) | Self::InvalidKey(_) | Self::InvalidKeySet => "jwks",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::{IpAddr, SocketAddr},
        sync::Arc,
        time::{Duration, Instant},
    };

    use jsonwebtoken::{Algorithm, DecodingKey, jwk::JwkSet};
    use tokio::sync::{mpsc, watch};

    use super::{
        KeySnapshot, OidcAlgorithm, OidcAuthenticator, OidcConfig, OidcError, VerificationKey,
        append_document_chunk, is_public_ip, parse_key_set, validate_fetch_url,
        validate_resolved_addresses,
    };
    use crate::AuthenticationError;

    const PUBLIC_KEY: &[u8] = br"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2
pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXB
Xd/4LkvcPuUakBoAkfh+eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHR
yIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8Lw
oPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xq
i+yUod+j8MtvIj812dkS4QMiRVN/by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5T
dQIDAQAB
-----END PUBLIC KEY-----";
    const VALID_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InByaW1hcnkifQ.eyJzdWIiOiJhbGljZSIsImlzcyI6Imh0dHBzOi8vaXNzdWVyLmV4YW1wbGUuY29tL3RlbmFudCIsImF1ZCI6Im9wZW5mZ2EiLCJhenAiOiJjbGllbnQiLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6NDAwMDAwMDAwMH0.lxkVIyWac2xU0JedPTt2R6-5gMwzudcWuCvp6uK70toiPSA4nlBYBBoMP2fD_xmZtcyHl7GV-cgIAArMqJA3z_nYWe8_L3F8Re_-WOBF7wXMYi1vNVC_iRvA_xlfYPsGY8r7oSRslMDk_of6m3vsej3XevRDe7yny9odHhx9udrHqfqCu9YxH6pm4KK7-nW2zEViLUFLuMmyNFRmBMFEUwCLR19g0VASitVOqAYADd29wnVN2Gi1egPeM9qWvwh3MIPqTkBy_POzAYM786H6D9jfKj1LVy_wuzXAPpODwm-mEFJS6ygPBDMEr0yueUU1G8knLZkElMxXP8jercknuw"; // gitleaks:allow -- synthetic unit-test token

    fn config() -> OidcConfig {
        OidcConfig::builder()
            .issuer("https://issuer.example.com/tenant".to_owned())
            .audiences(vec!["openfga".to_owned()])
            .authorized_parties(vec!["client".to_owned()])
            .algorithms(vec![OidcAlgorithm::RS256])
            .allowed_hosts(vec!["keys.example.com".to_owned()])
            .maximum_token_bytes(4_096)
            .maximum_document_bytes(8_192)
            .fetch_timeout(Duration::from_secs(2))
            .clock_skew(Duration::from_secs(10))
            .refresh_interval(Duration::from_mins(1))
            .stale_key_grace(Duration::from_mins(2))
            .build()
    }

    #[test]
    fn test_should_reject_ssrf_destinations_and_non_allowlisted_hosts()
    -> Result<(), Box<dyn std::error::Error>> {
        let parsed = super::ParsedConfig::try_from(config())?;
        assert!(validate_fetch_url(&parsed, &"https://keys.example.com/jwks".parse()?).is_ok());
        assert!(matches!(
            validate_fetch_url(&parsed, &"https://other.example.com/jwks".parse()?),
            Err(OidcError::ForbiddenHost)
        ));
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.1.1",
            "100.64.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
            "3fff::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_public_ip(address.parse::<IpAddr>()?));
        }
        assert!(is_public_ip("8.8.8.8".parse()?));
        assert!(is_public_ip("2606:4700:4700::1111".parse()?));

        let mixed_resolution = [
            "8.8.8.8:443".parse::<SocketAddr>()?,
            "127.0.0.1:443".parse::<SocketAddr>()?,
        ];
        assert!(matches!(
            validate_resolved_addresses(&mixed_resolution),
            Err(OidcError::ForbiddenAddress)
        ));
        Ok(())
    }

    #[test]
    fn test_should_reject_oversized_streamed_oidc_documents() {
        let mut document = vec![0_u8; 4];
        assert!(append_document_chunk(&mut document, &[1, 2], 6).is_ok());
        assert!(matches!(
            append_document_chunk(&mut document, &[3], 6),
            Err(OidcError::DocumentTooLarge)
        ));
        assert_eq!(document.len(), 6);
    }

    #[test]
    fn test_should_reject_symmetric_algorithm_confusion_and_duplicate_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        let hmac: JwkSet = serde_json::from_str(
            r#"{"keys":[{"kty":"oct","alg":"HS256","kid":"one","k":"YWJj"}]}"#,
        )?;
        assert!(parse_key_set(hmac, &[Algorithm::RS256]).is_err());

        let duplicate: JwkSet = serde_json::from_str(
            r#"{"keys":[
                {"kty":"RSA","alg":"RS256","kid":"one","n":"sXch","e":"AQAB"},
                {"kty":"RSA","alg":"RS256","kid":"one","n":"sXch","e":"AQAB"}
            ]}"#,
        )?;
        assert!(parse_key_set(duplicate, &[Algorithm::RS256]).is_err());
        Ok(())
    }

    #[test]
    fn test_should_validate_bounded_oidc_configuration() {
        assert!(config().validate().is_ok());
        let mut insecure = config();
        insecure.issuer = "http://issuer.example.com".to_owned();
        assert!(insecure.validate().is_err());
        let mut stale_too_short = config();
        stale_too_short.stale_key_grace = Duration::from_secs(30);
        assert!(stale_too_short.validate().is_err());
    }

    #[test]
    fn test_should_enforce_authorized_party_and_future_issued_at() -> Result<(), OidcError> {
        let parsed = super::ParsedConfig::try_from(config())?;
        let wrong_party = super::Claims {
            sub: "alice".to_owned(),
            azp: Some("other-client".to_owned()),
            iat: None,
        };
        assert_eq!(
            super::validate_claims(&wrong_party, &parsed),
            Err(AuthenticationError::InvalidCredentials)
        );
        let future = super::Claims {
            sub: "alice".to_owned(),
            azp: Some("client".to_owned()),
            iat: Some(u64::MAX),
        };
        assert_eq!(
            super::validate_claims(&future, &parsed),
            Err(AuthenticationError::InvalidCredentials)
        );
        Ok(())
    }

    #[test]
    fn test_should_validate_signed_claims_rotate_keys_and_expire_stale_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let parsed = Arc::new(super::ParsedConfig::try_from(config())?);
        let (sender, receiver) = watch::channel(Arc::new(KeySnapshot {
            keys: HashMap::new(),
            refreshed_at: Instant::now(),
        }));
        let (refresh, mut refresh_requests) = mpsc::channel(1);
        let authenticator = OidcAuthenticator {
            config: Arc::clone(&parsed),
            keys: receiver,
            refresh,
        };

        assert_eq!(
            authenticator.authenticate(VALID_TOKEN),
            Err(AuthenticationError::InvalidCredentials)
        );
        assert!(refresh_requests.try_recv().is_ok());

        sender.send_replace(Arc::new(KeySnapshot {
            keys: HashMap::from([(
                "primary".to_owned(),
                VerificationKey {
                    algorithm: Some(Algorithm::RS256),
                    key: DecodingKey::from_rsa_pem(PUBLIC_KEY)?,
                },
            )]),
            refreshed_at: Instant::now(),
        }));
        let principal = authenticator.authenticate(VALID_TOKEN)?;
        assert_eq!(principal.id().as_str(), "alice");
        assert!(authenticator.is_ready());

        sender.send_replace(Arc::new(KeySnapshot {
            keys: HashMap::new(),
            refreshed_at: Instant::now()
                .checked_sub(Duration::from_secs(121))
                .ok_or("test instant underflow")?,
        }));
        assert!(!authenticator.is_ready());
        assert_eq!(
            authenticator.authenticate(VALID_TOKEN),
            Err(AuthenticationError::Unavailable)
        );
        Ok(())
    }

    #[test]
    fn test_should_redact_oidc_configuration_from_debug() {
        let config = config();
        let debug = format!("{config:?}");
        assert!(!debug.contains("issuer.example.com"));
        assert!(!debug.contains("openfga"));
        assert!(debug.contains("[REDACTED]"));
    }
}
