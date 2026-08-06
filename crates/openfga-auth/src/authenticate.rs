//! Request credential parsing and authentication mechanisms.

use std::{fmt, sync::Arc};

use openfga_domain::{Principal, PrincipalId, PrincipalKind};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::oidc::{JwksActor, OidcAuthenticator, OidcConfig, OidcError};

const MINIMUM_PRESHARED_KEY_BYTES: usize = 32;
const MAXIMUM_PRESHARED_KEY_BYTES: usize = 256;
const MAXIMUM_BEARER_TOKEN_BYTES: usize = 16 * 1_024;
const MAXIMUM_AUTHORIZATION_HEADER_BYTES: usize = MAXIMUM_BEARER_TOKEN_BYTES + "Bearer ".len();

/// One active preshared credential with a non-secret stable identity label.
#[derive(Clone)]
#[non_exhaustive]
pub struct PresharedKey {
    principal_id: PrincipalId,
    digest: [u8; 32],
}

impl PresharedKey {
    /// Validates and hashes a preshared credential for constant-time matching.
    ///
    /// # Errors
    ///
    /// Returns [`AuthenticationConfigurationError`] when the key is too short, too long, or
    /// contains whitespace or control bytes.
    pub fn new(
        principal_id: PrincipalId,
        secret: &SecretString,
    ) -> Result<Self, AuthenticationConfigurationError> {
        let bytes = secret.expose_secret().as_bytes();
        if !(MINIMUM_PRESHARED_KEY_BYTES..=MAXIMUM_PRESHARED_KEY_BYTES).contains(&bytes.len()) {
            return Err(AuthenticationConfigurationError::InvalidPresharedKey);
        }
        if !bytes.iter().all(u8::is_ascii_graphic) {
            return Err(AuthenticationConfigurationError::InvalidPresharedKey);
        }
        Ok(Self {
            principal_id,
            digest: Sha256::digest(bytes).into(),
        })
    }
}

impl fmt::Debug for PresharedKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PresharedKey")
            .field("principal_id", &"[REDACTED]")
            .field("digest", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum Mechanism {
    Development(Principal),
    Preshared(Arc<[PresharedKey]>),
    OpenIdConnect(OidcAuthenticator),
}

/// Cloneable authentication service shared by HTTP and gRPC adapters.
#[derive(Clone)]
pub struct AuthenticationService {
    mechanism: Mechanism,
}

impl AuthenticationService {
    /// Creates an explicit loopback-development authenticator.
    #[must_use]
    pub fn development(principal_id: PrincipalId) -> Self {
        Self {
            mechanism: Mechanism::Development(Principal::new(
                PrincipalKind::Development,
                principal_id,
            )),
        }
    }

    /// Creates a rotating preshared-key authenticator.
    ///
    /// # Errors
    ///
    /// Returns [`AuthenticationConfigurationError`] if no keys are configured or labels repeat.
    pub fn preshared(keys: Vec<PresharedKey>) -> Result<Self, AuthenticationConfigurationError> {
        if keys.is_empty() {
            return Err(AuthenticationConfigurationError::NoPresharedKeys);
        }
        for (index, key) in keys.iter().enumerate() {
            if keys.iter().skip(index.saturating_add(1)).any(|candidate| {
                candidate.principal_id == key.principal_id || candidate.digest == key.digest
            }) {
                return Err(AuthenticationConfigurationError::DuplicatePrincipal);
            }
        }
        Ok(Self {
            mechanism: Mechanism::Preshared(keys.into()),
        })
    }

    /// Fetches initial OIDC discovery/JWKS state before constructing an authenticator.
    ///
    /// # Errors
    ///
    /// Returns [`OidcError`] when configuration, discovery, DNS policy, or JWKS validation fails.
    pub async fn open_id_connect(config: OidcConfig) -> Result<(Self, JwksActor), OidcError> {
        let (authenticator, actor) = JwksActor::initialize(config).await?;
        Ok((
            Self {
                mechanism: Mechanism::OpenIdConnect(authenticator),
            },
            actor,
        ))
    }

    /// Authenticates an optional raw `Authorization` header value.
    ///
    /// # Errors
    ///
    /// Returns a redacted failure for missing, malformed, invalid, or temporarily unavailable
    /// credentials.
    pub fn authenticate(
        &self,
        authorization_header: Option<&str>,
    ) -> Result<Principal, AuthenticationError> {
        match &self.mechanism {
            Mechanism::Development(principal) => Ok(principal.clone()),
            Mechanism::Preshared(keys) => {
                let token = bearer_token(authorization_header, MAXIMUM_BEARER_TOKEN_BYTES)?;
                authenticate_preshared(keys, token)
            }
            Mechanism::OpenIdConnect(authenticator) => {
                let token =
                    bearer_token(authorization_header, authenticator.maximum_token_bytes())?;
                authenticator.authenticate(token)
            }
        }
    }

    /// Returns whether correctness-critical authentication state is currently usable.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        match &self.mechanism {
            Mechanism::Development(_) | Mechanism::Preshared(_) => true,
            Mechanism::OpenIdConnect(authenticator) => authenticator.is_ready(),
        }
    }
}

impl fmt::Debug for AuthenticationService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mechanism = match self.mechanism {
            Mechanism::Development(_) => "development",
            Mechanism::Preshared(_) => "preshared",
            Mechanism::OpenIdConnect(_) => "oidc",
        };
        formatter
            .debug_struct("AuthenticationService")
            .field("mechanism", &mechanism)
            .finish_non_exhaustive()
    }
}

fn bearer_token(
    authorization_header: Option<&str>,
    maximum_token_bytes: usize,
) -> Result<&str, AuthenticationError> {
    let header = authorization_header.ok_or(AuthenticationError::MissingCredentials)?;
    let maximum_header_bytes = maximum_token_bytes.saturating_add("Bearer ".len());
    if header.len() > maximum_header_bytes || header.len() > MAXIMUM_AUTHORIZATION_HEADER_BYTES {
        return Err(AuthenticationError::InvalidCredentials);
    }
    let (scheme, token) = header
        .split_once(' ')
        .ok_or(AuthenticationError::InvalidCredentials)?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(AuthenticationError::InvalidCredentials);
    }
    Ok(token)
}

fn authenticate_preshared(
    keys: &[PresharedKey],
    token: &str,
) -> Result<Principal, AuthenticationError> {
    if !(MINIMUM_PRESHARED_KEY_BYTES..=MAXIMUM_PRESHARED_KEY_BYTES).contains(&token.len()) {
        return Err(AuthenticationError::InvalidCredentials);
    }
    let candidate: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let mut matched_index = 0_usize;
    let mut matched = 0_u8;
    for (index, key) in keys.iter().enumerate() {
        let choice = key.digest.ct_eq(&candidate).unwrap_u8();
        if choice == 1 {
            matched_index = index;
        }
        matched |= choice;
    }
    if matched == 0 {
        return Err(AuthenticationError::InvalidCredentials);
    }
    let key = keys
        .get(matched_index)
        .ok_or(AuthenticationError::InvalidCredentials)?;
    Ok(Principal::new(
        PrincipalKind::PresharedKey,
        key.principal_id.clone(),
    ))
}

/// A redacted request authentication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum AuthenticationError {
    /// No credentials were supplied.
    #[error("authentication credentials are missing")]
    MissingCredentials,
    /// Credentials were malformed, expired, incorrectly signed, or otherwise invalid.
    #[error("authentication credentials are invalid")]
    InvalidCredentials,
    /// Correctness-critical identity-provider state is unavailable.
    #[error("authentication service is unavailable")]
    Unavailable,
}

/// A redacted authentication configuration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum AuthenticationConfigurationError {
    /// No preshared key was configured.
    #[error("at least one preshared key is required")]
    NoPresharedKeys,
    /// A key failed entropy-size or character validation.
    #[error("a preshared key is invalid")]
    InvalidPresharedKey,
    /// More than one key uses the same identity label.
    #[error("preshared key identity labels must be unique")]
    DuplicatePrincipal,
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;

    use super::{
        AuthenticationError, AuthenticationService, MAXIMUM_AUTHORIZATION_HEADER_BYTES,
        PresharedKey,
    };

    const PRIMARY: &str = "primary-key-material-with-32-bytes-minimum";
    const SECONDARY: &str = "secondary-key-material-with-32-bytes-minimum";

    #[test]
    fn test_should_match_every_active_preshared_key_exactly()
    -> Result<(), Box<dyn std::error::Error>> {
        let service = AuthenticationService::preshared(vec![
            PresharedKey::new("primary".parse()?, &SecretString::from(PRIMARY))?,
            PresharedKey::new("secondary".parse()?, &SecretString::from(SECONDARY))?,
        ])?;

        assert_eq!(
            service
                .authenticate(Some(&format!("Bearer {PRIMARY}")))?
                .id()
                .as_str(),
            "primary"
        );
        assert_eq!(
            service
                .authenticate(Some(&format!("bearer {SECONDARY}")))?
                .id()
                .as_str(),
            "secondary"
        );
        assert_eq!(
            service.authenticate(Some(&format!("Bearer {PRIMARY}suffix"))),
            Err(AuthenticationError::InvalidCredentials)
        );
        assert_eq!(
            service.authenticate(Some("Basic secret")),
            Err(AuthenticationError::InvalidCredentials)
        );
        assert_eq!(
            service.authenticate(None),
            Err(AuthenticationError::MissingCredentials)
        );
        Ok(())
    }

    #[test]
    fn test_should_redact_preshared_material_from_debug() -> Result<(), Box<dyn std::error::Error>>
    {
        let key = PresharedKey::new("primary".parse()?, &SecretString::from(PRIMARY))?;
        let debug = format!("{key:?}");
        assert!(!debug.contains(PRIMARY));
        assert!(!debug.contains("primary"));
        assert!(debug.contains("[REDACTED]"));

        let service = AuthenticationService::preshared(vec![key])?;
        let service_debug = format!("{service:?}");
        assert!(!service_debug.contains(PRIMARY));
        assert!(!service_debug.contains("primary"));
        assert!(service_debug.contains("preshared"));
        Ok(())
    }

    #[test]
    fn test_should_reject_oversized_or_ambiguous_authorization_headers()
    -> Result<(), Box<dyn std::error::Error>> {
        let service = AuthenticationService::preshared(vec![PresharedKey::new(
            "primary".parse()?,
            &SecretString::from(PRIMARY),
        )?])?;
        let oversized = "x".repeat(MAXIMUM_AUTHORIZATION_HEADER_BYTES.saturating_add(1));
        assert_eq!(
            service.authenticate(Some(&oversized)),
            Err(AuthenticationError::InvalidCredentials)
        );
        assert_eq!(
            service.authenticate(Some(&format!("Bearer {PRIMARY} extra"))),
            Err(AuthenticationError::InvalidCredentials)
        );
        Ok(())
    }
}
