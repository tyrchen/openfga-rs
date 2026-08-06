//! Versioned, authenticated, scope-bound continuation tokens.

use std::{
    collections::BTreeSet,
    fmt,
    io::{Cursor, Read},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use secrecy::{ExposeSecret, SecretBox};
use sha2::Sha256;

use crate::{
    error::{ValidationError, ValidationReason},
    fingerprint::Fingerprint,
    identifier::{StoreId, TokenKeyId},
    limits::InputLimits,
};

const TOKEN_MAGIC: &[u8; 4] = b"OFGA";
const TOKEN_VERSION: u8 = 1;
const MAC_BYTES: usize = 32;
const MINIMUM_KEY_BYTES: usize = 32;
const MAXIMUM_KEY_BYTES: usize = 64;
const STORE_ID_BYTES: usize = 26;

type HmacSha256 = Hmac<Sha256>;

fn invalid_token(reason: ValidationReason) -> ValidationError {
    ValidationError::new("continuation_token", reason)
}

/// A list/read operation to which a continuation token is cryptographically bound.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum TokenOperation {
    /// List relationship tuples.
    ReadTuples,
    /// Read the store changelog.
    ReadChanges,
    /// List stores.
    ListStores,
    /// Read authorization models.
    ReadAuthorizationModels,
    /// List objects reachable from a subject.
    ListObjects,
    /// List users reachable from an object/relation.
    ListUsers,
}

impl TokenOperation {
    const fn code(self) -> u8 {
        match self {
            Self::ReadTuples => 1,
            Self::ReadChanges => 2,
            Self::ListStores => 3,
            Self::ReadAuthorizationModels => 4,
            Self::ListObjects => 5,
            Self::ListUsers => 6,
        }
    }

    fn from_code(code: u8) -> Result<Self, ValidationError> {
        match code {
            1 => Ok(Self::ReadTuples),
            2 => Ok(Self::ReadChanges),
            3 => Ok(Self::ListStores),
            4 => Ok(Self::ReadAuthorizationModels),
            5 => Ok(Self::ListObjects),
            6 => Ok(Self::ListUsers),
            _ => Err(invalid_token(ValidationReason::Integrity)),
        }
    }
}

/// The stable request identity that prevents continuation-token replay across queries.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ContinuationScope {
    operation: TokenOperation,
    store_id: StoreId,
    filter: Fingerprint,
}

impl ContinuationScope {
    /// Creates a scope from an endpoint, store, and normalized-filter fingerprint.
    #[must_use]
    pub const fn new(operation: TokenOperation, store_id: StoreId, filter: Fingerprint) -> Self {
        Self {
            operation,
            store_id,
            filter,
        }
    }

    /// Returns the endpoint operation.
    #[must_use]
    pub const fn operation(&self) -> TokenOperation {
        self.operation
    }

    /// Returns the store identity.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the normalized-filter fingerprint.
    #[must_use]
    pub const fn filter(&self) -> Fingerprint {
        self.filter
    }
}

/// A verified, bounded backend continuation cursor.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ContinuationCursor {
    bytes: Vec<u8>,
    expires_at_unix_seconds: u64,
}

impl ContinuationCursor {
    /// Creates a bounded cursor and expiry claim.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if the cursor is empty, oversized, or has a
    /// zero expiry timestamp.
    pub fn new(
        bytes: Vec<u8>,
        expires_at_unix_seconds: u64,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        if bytes.is_empty() {
            return Err(ValidationError::new(
                "continuation_cursor",
                ValidationReason::Missing,
            ));
        }
        if bytes.len() > limits.token_cursor_bytes() {
            return Err(ValidationError::new(
                "continuation_cursor",
                ValidationReason::TooLarge,
            ));
        }
        if expires_at_unix_seconds == 0 {
            return Err(ValidationError::new(
                "continuation_expiry",
                ValidationReason::OutOfRange,
            ));
        }
        Ok(Self {
            bytes,
            expires_at_unix_seconds,
        })
    }

    /// Returns the backend-independent cursor bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the absolute Unix expiry time in seconds.
    #[must_use]
    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }
}

impl fmt::Debug for ContinuationCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContinuationCursor")
            .field("bytes", &self.bytes.len())
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// One dedicated HMAC key for continuation tokens.
#[non_exhaustive]
pub struct TokenKey {
    id: TokenKeyId,
    secret: SecretBox<Vec<u8>>,
}

impl TokenKey {
    /// Creates a token key with at least 256 bits of entropy material.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] unless 32 through 64 key bytes are supplied.
    pub fn new(id: TokenKeyId, secret: Vec<u8>) -> Result<Self, ValidationError> {
        if !(MINIMUM_KEY_BYTES..=MAXIMUM_KEY_BYTES).contains(&secret.len()) {
            return Err(ValidationError::new(
                "continuation_token_key",
                ValidationReason::OutOfRange,
            ));
        }
        Ok(Self {
            id,
            secret: SecretBox::new(Box::new(secret)),
        })
    }

    /// Returns the public key selector.
    #[must_use]
    pub const fn id(&self) -> &TokenKeyId {
        &self.id
    }
}

impl fmt::Debug for TokenKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenKey")
            .field("id", &self.id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

/// A rotating HMAC-SHA-256 key set that signs and verifies continuation tokens.
#[non_exhaustive]
pub struct TokenCodec {
    signing_key: TokenKey,
    verification_keys: Vec<TokenKey>,
    token_bytes: usize,
    cursor_bytes: usize,
}

impl TokenCodec {
    /// Creates a codec with one active signing key and optional retired verification keys.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if any public key identifier is duplicated.
    pub fn new(
        signing_key: TokenKey,
        verification_keys: Vec<TokenKey>,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        let mut ids = BTreeSet::new();
        ids.insert(signing_key.id.clone());
        if verification_keys
            .iter()
            .any(|key| !ids.insert(key.id.clone()))
        {
            return Err(ValidationError::new(
                "continuation_token_keys",
                ValidationReason::Duplicate,
            ));
        }
        if ids.len() > limits.token_keys() {
            return Err(ValidationError::new(
                "continuation_token_keys",
                ValidationReason::TooManyItems,
            ));
        }
        Ok(Self {
            signing_key,
            verification_keys,
            token_bytes: limits.token_bytes(),
            cursor_bytes: limits.token_cursor_bytes(),
        })
    }

    /// Authenticates and encodes a bounded cursor for one exact query scope.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if the final token exceeds the configured cap
    /// or the HMAC implementation rejects the dedicated key.
    pub fn encode(
        &self,
        scope: &ContinuationScope,
        cursor: &ContinuationCursor,
    ) -> Result<String, ValidationError> {
        if cursor.bytes.len() > self.cursor_bytes {
            return Err(invalid_token(ValidationReason::TooLarge));
        }
        let unsigned = encode_unsigned(self.signing_key.id(), scope, cursor)?;
        let mut mac = HmacSha256::new_from_slice(self.signing_key.secret.expose_secret())
            .map_err(|_| invalid_token(ValidationReason::Integrity))?;
        mac.update(&unsigned);
        let tag = mac.finalize().into_bytes();
        let mut envelope = unsigned;
        envelope.extend_from_slice(&tag);
        let encoded = URL_SAFE_NO_PAD.encode(envelope);
        if encoded.len() > self.token_bytes {
            return Err(invalid_token(ValidationReason::TooLarge));
        }
        Ok(encoded)
    }

    /// Verifies, decodes, and scope-checks a continuation token.
    ///
    /// `now_unix_seconds` is explicit so callers can use one request clock and
    /// tests can be deterministic.
    ///
    /// # Errors
    ///
    /// Returns a safe [`ValidationError`] for malformed, unknown-key, tampered,
    /// replayed, oversized, or expired tokens.
    pub fn decode(
        &self,
        token: &str,
        expected_scope: &ContinuationScope,
        now_unix_seconds: u64,
    ) -> Result<ContinuationCursor, ValidationError> {
        if token.is_empty() || token.len() > self.token_bytes {
            return Err(invalid_token(ValidationReason::TooLarge));
        }
        let maximum_decoded = base64::decoded_len_estimate(token.len());
        if maximum_decoded > self.token_bytes {
            return Err(invalid_token(ValidationReason::TooLarge));
        }
        let envelope = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| invalid_token(ValidationReason::Integrity))?;
        if URL_SAFE_NO_PAD.encode(&envelope) != token {
            return Err(invalid_token(ValidationReason::Integrity));
        }
        let unsigned_len = envelope
            .len()
            .checked_sub(MAC_BYTES)
            .ok_or_else(|| invalid_token(ValidationReason::Integrity))?;
        let unsigned = envelope
            .get(..unsigned_len)
            .ok_or_else(|| invalid_token(ValidationReason::Integrity))?;
        let tag = envelope
            .get(unsigned_len..)
            .ok_or_else(|| invalid_token(ValidationReason::Integrity))?;
        let key_id = decode_key_id(unsigned)?;
        let key = self
            .find_key(&key_id)
            .ok_or_else(|| invalid_token(ValidationReason::Integrity))?;
        let mut mac = HmacSha256::new_from_slice(key.secret.expose_secret())
            .map_err(|_| invalid_token(ValidationReason::Integrity))?;
        mac.update(unsigned);
        mac.verify_slice(tag)
            .map_err(|_| invalid_token(ValidationReason::Integrity))?;

        let (scope, cursor) = decode_unsigned(unsigned, self.cursor_bytes)?;
        if scope != *expected_scope {
            return Err(invalid_token(ValidationReason::Inconsistent));
        }
        if cursor.expires_at_unix_seconds <= now_unix_seconds {
            return Err(invalid_token(ValidationReason::Expired));
        }
        Ok(cursor)
    }

    fn find_key(&self, id: &TokenKeyId) -> Option<&TokenKey> {
        if self.signing_key.id() == id {
            return Some(&self.signing_key);
        }
        self.verification_keys.iter().find(|key| key.id() == id)
    }
}

impl fmt::Debug for TokenCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenCodec")
            .field("signing_key_id", self.signing_key.id())
            .field("verification_keys", &self.verification_keys.len())
            .finish_non_exhaustive()
    }
}

fn encode_unsigned(
    key_id: &TokenKeyId,
    scope: &ContinuationScope,
    cursor: &ContinuationCursor,
) -> Result<Vec<u8>, ValidationError> {
    let key_id_len = u8::try_from(key_id.as_str().len())
        .map_err(|_| invalid_token(ValidationReason::TooLarge))?;
    let cursor_len =
        u32::try_from(cursor.bytes.len()).map_err(|_| invalid_token(ValidationReason::TooLarge))?;
    let mut bytes = Vec::with_capacity(
        TOKEN_MAGIC
            .len()
            .saturating_add(3)
            .saturating_add(key_id.as_str().len())
            .saturating_add(STORE_ID_BYTES)
            .saturating_add(32)
            .saturating_add(8)
            .saturating_add(4)
            .saturating_add(cursor.bytes.len()),
    );
    bytes.extend_from_slice(TOKEN_MAGIC);
    bytes.push(TOKEN_VERSION);
    bytes.push(scope.operation.code());
    bytes.push(key_id_len);
    bytes.extend_from_slice(key_id.as_str().as_bytes());
    bytes.extend_from_slice(scope.store_id.to_string().as_bytes());
    bytes.extend_from_slice(scope.filter.as_bytes());
    bytes.extend_from_slice(&cursor.expires_at_unix_seconds.to_be_bytes());
    bytes.extend_from_slice(&cursor_len.to_be_bytes());
    bytes.extend_from_slice(&cursor.bytes);
    Ok(bytes)
}

fn read_exact<const N: usize>(cursor: &mut Cursor<&[u8]>) -> Result<[u8; N], ValidationError> {
    let mut value = [0_u8; N];
    cursor
        .read_exact(&mut value)
        .map_err(|_| invalid_token(ValidationReason::Integrity))?;
    Ok(value)
}

fn read_length_prefixed(
    cursor: &mut Cursor<&[u8]>,
    length: usize,
) -> Result<Vec<u8>, ValidationError> {
    let mut value = vec![0_u8; length];
    cursor
        .read_exact(&mut value)
        .map_err(|_| invalid_token(ValidationReason::Integrity))?;
    Ok(value)
}

fn decode_header(
    cursor: &mut Cursor<&[u8]>,
) -> Result<(TokenOperation, TokenKeyId), ValidationError> {
    if read_exact::<4>(cursor)? != *TOKEN_MAGIC || read_exact::<1>(cursor)? != [TOKEN_VERSION] {
        return Err(invalid_token(ValidationReason::Integrity));
    }
    let operation = TokenOperation::from_code(u8::from_be_bytes(read_exact::<1>(cursor)?))?;
    let key_length = usize::from(u8::from_be_bytes(read_exact::<1>(cursor)?));
    let key_bytes = read_length_prefixed(cursor, key_length)?;
    let key_text =
        std::str::from_utf8(&key_bytes).map_err(|_| invalid_token(ValidationReason::Integrity))?;
    let key_id = key_text
        .parse()
        .map_err(|_| invalid_token(ValidationReason::Integrity))?;
    Ok((operation, key_id))
}

fn decode_key_id(unsigned: &[u8]) -> Result<TokenKeyId, ValidationError> {
    let mut reader = Cursor::new(unsigned);
    decode_header(&mut reader).map(|(_, key_id)| key_id)
}

fn decode_unsigned(
    unsigned: &[u8],
    maximum_cursor_bytes: usize,
) -> Result<(ContinuationScope, ContinuationCursor), ValidationError> {
    let mut reader = Cursor::new(unsigned);
    let (operation, _) = decode_header(&mut reader)?;
    let store_bytes = read_exact::<STORE_ID_BYTES>(&mut reader)?;
    let store_text = std::str::from_utf8(&store_bytes)
        .map_err(|_| invalid_token(ValidationReason::Integrity))?;
    let store_id = store_text
        .parse()
        .map_err(|_| invalid_token(ValidationReason::Integrity))?;
    let filter = Fingerprint::from_bytes(read_exact::<32>(&mut reader)?);
    let expires_at_unix_seconds = u64::from_be_bytes(read_exact::<8>(&mut reader)?);
    let cursor_length_u32 = u32::from_be_bytes(read_exact::<4>(&mut reader)?);
    let cursor_length = usize::try_from(cursor_length_u32)
        .map_err(|_| invalid_token(ValidationReason::TooLarge))?;
    if cursor_length == 0 || cursor_length > maximum_cursor_bytes {
        return Err(invalid_token(ValidationReason::TooLarge));
    }
    let cursor = read_length_prefixed(&mut reader, cursor_length)?;
    let consumed = usize::try_from(reader.position())
        .map_err(|_| invalid_token(ValidationReason::TooLarge))?;
    if consumed != unsigned.len() || expires_at_unix_seconds == 0 {
        return Err(invalid_token(ValidationReason::Integrity));
    }
    Ok((
        ContinuationScope::new(operation, store_id, filter),
        ContinuationCursor {
            bytes: cursor,
            expires_at_unix_seconds,
        },
    ))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::{any, proptest};

    use super::{ContinuationCursor, ContinuationScope, TokenCodec, TokenKey, TokenOperation};
    use crate::{Fingerprint, InputLimits, StoreId, TokenKeyId, ValidationReason};

    fn key(id: &str, byte: u8) -> Result<TokenKey, crate::DomainError> {
        Ok(TokenKey::new(id.parse::<TokenKeyId>()?, vec![byte; 32])?)
    }

    fn scope(operation: TokenOperation) -> Result<ContinuationScope, crate::DomainError> {
        Ok(ContinuationScope::new(
            operation,
            "01G5JAVJ41T49E9TT3SKVS7X1J".parse::<StoreId>()?,
            Fingerprint::from_bytes([7; 32]),
        ))
    }

    #[test]
    fn test_should_round_trip_and_redact_authenticated_cursor() -> Result<(), crate::DomainError> {
        let limits = InputLimits::default();
        let codec = TokenCodec::new(key("current", 1)?, Vec::new(), &limits)?;
        let scope = scope(TokenOperation::ReadTuples)?;
        let cursor = ContinuationCursor::new(b"backend-position".to_vec(), 2_000, &limits)?;
        let token = codec.encode(&scope, &cursor)?;

        assert_eq!(codec.decode(&token, &scope, 1_000)?, cursor);
        assert!(!format!("{cursor:?}").contains("backend-position"));
        assert!(!format!("{codec:?}").contains("[1, 1"));
        Ok(())
    }

    #[test]
    fn test_should_reject_tamper_scope_replay_and_expiry() -> Result<(), crate::DomainError> {
        let limits = InputLimits::default();
        let codec = TokenCodec::new(key("current", 2)?, Vec::new(), &limits)?;
        let token_scope = scope(TokenOperation::ReadTuples)?;
        let cursor = ContinuationCursor::new(vec![9], 2_000, &limits)?;
        let token = codec.encode(&token_scope, &cursor)?;
        let mut tampered = token.clone();
        tampered.push('A');

        assert!(codec.decode(&tampered, &token_scope, 1_000).is_err());
        let other_scope = scope(TokenOperation::ReadChanges)?;
        assert_eq!(
            codec
                .decode(&token, &other_scope, 1_000)
                .map_err(|error| error.reason()),
            Err(ValidationReason::Inconsistent)
        );
        assert_eq!(
            codec
                .decode(&token, &token_scope, 2_000)
                .map_err(|error| error.reason()),
            Err(ValidationReason::Expired)
        );
        Ok(())
    }

    #[test]
    fn test_should_verify_tokens_across_key_rotation() -> Result<(), crate::DomainError> {
        let limits = InputLimits::default();
        let old_codec = TokenCodec::new(key("old", 3)?, Vec::new(), &limits)?;
        let scope = scope(TokenOperation::ListObjects)?;
        let cursor = ContinuationCursor::new(vec![4, 5], 3_000, &limits)?;
        let old_token = old_codec.encode(&scope, &cursor)?;
        let rotated = TokenCodec::new(key("new", 4)?, vec![key("old", 3)?], &limits)?;

        assert_eq!(rotated.decode(&old_token, &scope, 2_000)?, cursor);
        Ok(())
    }

    #[test]
    fn test_should_bound_key_material_and_rotating_key_count() -> Result<(), crate::DomainError> {
        let limits = InputLimits::builder()
            .token_keys(
                crate::Limit::<256>::new(2).map_err(|_| crate::DomainError::Internal {
                    code: "invalid_test_token_key_limit",
                })?,
            )
            .build();
        let id = "short".parse::<TokenKeyId>()?;
        assert!(TokenKey::new(id, vec![1; 31]).is_err());
        let id = "long".parse::<TokenKeyId>()?;
        assert!(TokenKey::new(id, vec![1; 65]).is_err());
        assert!(
            TokenCodec::new(
                key("current", 1)?,
                vec![key("old-one", 2)?, key("old-two", 3)?],
                &limits,
            )
            .is_err()
        );
        Ok(())
    }

    proptest! {
        #[test]
        fn test_should_never_panic_decoding_arbitrary_tokens(token in any::<String>()) {
            let limits = InputLimits::default();
            if let Ok(signing_key) = key("current", 8) {
                let codec = TokenCodec::new(signing_key, Vec::new(), &limits);
                if let Ok(codec) = codec {
                    let expected = scope(TokenOperation::ListUsers);
                    if let Ok(expected) = expected {
                        let _ = codec.decode(&token, &expected, 100);
                    }
                }
            }
        }
    }
}
