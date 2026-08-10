//! Validated `DynamoDB` connection, retry, and cleanup configuration.

use std::{collections::BTreeMap, fmt, num::NonZeroU32, str::FromStr, time::Duration};

use typed_builder::TypedBuilder;
use url::{Host, Url};

const MAXIMUM_TABLE_NAME_BYTES: usize = 255;
const MAXIMUM_REGION_NAME_BYTES: usize = 64;
const MAXIMUM_TIMEOUT: Duration = Duration::from_mins(5);
#[allow(
    clippy::duration_suboptimal_units,
    reason = "Duration::from_days is not stable as a const constructor"
)]
const MAXIMUM_GARBAGE_COLLECTION_GRACE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const MAXIMUM_IN_FLIGHT: u32 = 65_536;
const MAXIMUM_ATTEMPTS: u32 = 10;
const MAXIMUM_CONFLICT_RETRIES: u32 = 100;
const MAXIMUM_GARBAGE_COLLECTION_BATCH: u32 = 1_000;
const MAXIMUM_KMS_KEY_IDENTIFIER_BYTES: usize = 2_048;
const MAXIMUM_TAGS: usize = 50;

/// A validated `DynamoDB` table name.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct DynamoDbTableName(String);

impl DynamoDbTableName {
    /// Returns the validated table name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DynamoDbTableName {
    type Err = DynamoDbConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !(3..=MAXIMUM_TABLE_NAME_BYTES).contains(&value.len()) {
            return Err(DynamoDbConfigError::InvalidTableName);
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(DynamoDbConfigError::InvalidTableName);
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Debug for DynamoDbTableName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("DynamoDbTableName")
            .field(&self.0)
            .finish()
    }
}

/// A validated AWS Region identifier.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct RegionName(String);

impl RegionName {
    /// Returns the validated Region name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for RegionName {
    type Err = DynamoDbConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > MAXIMUM_REGION_NAME_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(DynamoDbConfigError::InvalidRegion);
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Debug for RegionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("RegionName").field(&self.0).finish()
    }
}

/// A loopback-only HTTP endpoint accepted exclusively for local emulators.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DevelopmentEndpoint(String);

impl DevelopmentEndpoint {
    /// Returns the validated endpoint URL.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DevelopmentEndpoint {
    type Err = DynamoDbConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = Url::parse(value).map_err(|_| DynamoDbConfigError::InvalidEndpoint)?;
        let host_is_loopback = match parsed.host() {
            Some(Host::Ipv4(address)) => address.is_loopback(),
            Some(Host::Ipv6(address)) => address.is_loopback(),
            Some(Host::Domain(_)) | None => {
                return Err(DynamoDbConfigError::EndpointNotLoopback);
            }
        };
        if parsed.scheme() != "http"
            || !host_is_loopback
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(DynamoDbConfigError::InvalidEndpoint);
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Debug for DevelopmentEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DevelopmentEndpoint")
            .field("loopback", &true)
            .finish()
    }
}

/// Validated KMS key ID, ARN, or alias used for customer-managed table encryption.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct KmsKeyIdentifier(String);

impl KmsKeyIdentifier {
    /// Returns the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for KmsKeyIdentifier {
    type Err = DynamoDbConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > MAXIMUM_KMS_KEY_IDENTIFIER_BYTES
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'/' | b'_' | b'-')
            })
        {
            return Err(DynamoDbConfigError::InvalidKmsKeyIdentifier);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Explicit production table-provisioning controls.
#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct DynamoDbProvisioningConfig {
    /// Optional customer-managed KMS key; absence selects `DynamoDB`'s AWS-owned key.
    #[builder(default)]
    pub(crate) kms_key_identifier: Option<KmsKeyIdentifier>,
    /// Enables point-in-time recovery after table creation.
    #[builder(default = true)]
    pub(crate) point_in_time_recovery: bool,
    /// Enables table deletion protection.
    #[builder(default)]
    pub(crate) deletion_protection: bool,
    /// Required ownership and cost-allocation tags.
    #[builder(default = default_provisioning_tags())]
    pub(crate) tags: BTreeMap<String, String>,
}

impl Default for DynamoDbProvisioningConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl DynamoDbProvisioningConfig {
    fn validate(&self) -> Result<(), DynamoDbConfigError> {
        if self.tags.is_empty()
            || self.tags.len() > MAXIMUM_TAGS
            || self.tags.iter().any(|(key, value)| {
                key.is_empty()
                    || key.len() > 128
                    || value.len() > 256
                    || !key.bytes().all(valid_tag_byte)
                    || !value.bytes().all(valid_tag_value_byte)
            })
        {
            return Err(DynamoDbConfigError::InvalidTags);
        }
        Ok(())
    }
}

fn default_provisioning_tags() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("application".to_owned(), "openfga".to_owned()),
        ("managed-by".to_owned(), "openfga-rs".to_owned()),
    ])
}

fn valid_tag_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/' | b'=' | b'+' | b'@')
}

fn valid_tag_value_byte(byte: u8) -> bool {
    byte == b' ' || valid_tag_byte(byte)
}

/// A validated `DynamoDB` tuple-mutation ceiling in `1..=49`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DynamoDbMutationLimit(NonZeroU32);

impl DynamoDbMutationLimit {
    /// Validates a mutation limit against the 100-action transaction ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`DynamoDbConfigError::InvalidMutationLimit`] for values above 49.
    pub const fn new(value: NonZeroU32) -> Result<Self, DynamoDbConfigError> {
        if value.get() <= 49 {
            Ok(Self(value))
        } else {
            Err(DynamoDbConfigError::InvalidMutationLimit)
        }
    }

    /// Returns the validated limit.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Default for DynamoDbMutationLimit {
    fn default() -> Self {
        Self(NonZeroU32::new(49).unwrap_or(NonZeroU32::MIN))
    }
}

/// Durable garbage-collection actor policy.
#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct DynamoDbGarbageCollectionConfig {
    /// Maximum due generations claimed per pass.
    #[builder(default = NonZeroU32::new(16).unwrap_or(NonZeroU32::MIN))]
    pub(crate) batch_size: NonZeroU32,
    /// Delay between cleanup passes.
    #[builder(default = Duration::from_secs(30))]
    pub(crate) interval: Duration,
    /// Grace period before an unreachable generation can be deleted.
    #[builder(default = Duration::from_mins(5))]
    pub(crate) grace_period: Duration,
    /// Retention for a replaced assertion generation after its former readers can drain.
    #[builder(default = Duration::from_mins(6))]
    pub(crate) assertion_retention: Duration,
    /// Maximum observed overdue-work lag before readiness degrades.
    #[builder(default = Duration::from_mins(15))]
    pub(crate) maximum_work_lag: Duration,
    /// Maximum graceful actor shutdown duration.
    #[builder(default = Duration::from_secs(5))]
    pub(crate) shutdown_timeout: Duration,
}

impl Default for DynamoDbGarbageCollectionConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Validated `DynamoDB` backend policy.
#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct DynamoDbStorageConfig {
    /// Base-table name.
    pub(crate) table_name: DynamoDbTableName,
    /// Explicit AWS Region.
    pub(crate) region: RegionName,
    /// Optional loopback development endpoint.
    #[builder(default)]
    pub(crate) endpoint: Option<DevelopmentEndpoint>,
    /// Maximum concurrent SDK requests.
    #[builder(default = NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN))]
    pub(crate) maximum_in_flight: NonZeroU32,
    /// Per-attempt Smithy timeout.
    #[builder(default = Duration::from_secs(2))]
    pub(crate) attempt_timeout: Duration,
    /// Whole-operation Smithy timeout.
    #[builder(default = Duration::from_secs(5))]
    pub(crate) operation_timeout: Duration,
    /// Maximum caller deadline accepted by the composing server.
    #[builder(default = Duration::from_secs(5))]
    pub(crate) maximum_caller_deadline: Duration,
    /// Maximum SDK attempts including the initial request.
    #[builder(default = NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN))]
    pub(crate) maximum_attempts: NonZeroU32,
    /// Maximum optimistic transaction conflict retries.
    #[builder(default = NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN))]
    pub(crate) maximum_conflict_retries: NonZeroU32,
    /// Maximum tuple keys in one atomic write.
    #[builder(default)]
    pub(crate) maximum_tuple_mutations: DynamoDbMutationLimit,
    /// Cleanup actor policy.
    #[builder(default)]
    pub(crate) garbage_collection: DynamoDbGarbageCollectionConfig,
    /// Production-only table provisioning controls.
    #[builder(default)]
    pub(crate) provisioning: DynamoDbProvisioningConfig,
}

impl DynamoDbStorageConfig {
    /// Validates timeout and garbage-collection relationships.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error for a zero or inconsistent timeout.
    pub fn validate(&self) -> Result<(), DynamoDbConfigError> {
        self.provisioning.validate()?;
        if self.maximum_in_flight.get() > MAXIMUM_IN_FLIGHT
            || self.maximum_attempts.get() > MAXIMUM_ATTEMPTS
            || self.maximum_conflict_retries.get() > MAXIMUM_CONFLICT_RETRIES
        {
            return Err(DynamoDbConfigError::InvalidResourceLimit);
        }
        if self.attempt_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.maximum_caller_deadline.is_zero()
            || self.attempt_timeout > self.operation_timeout
            || self.operation_timeout > self.maximum_caller_deadline
        {
            return Err(DynamoDbConfigError::InvalidTimeout);
        }
        if self.operation_timeout > MAXIMUM_TIMEOUT
            || self.maximum_caller_deadline > MAXIMUM_TIMEOUT
        {
            return Err(DynamoDbConfigError::TimeoutTooLong);
        }
        if self.garbage_collection.interval.is_zero()
            || self.garbage_collection.grace_period.is_zero()
            || self.garbage_collection.assertion_retention.is_zero()
            || self.garbage_collection.maximum_work_lag.is_zero()
            || self.garbage_collection.shutdown_timeout.is_zero()
            || self.garbage_collection.batch_size.get() > MAXIMUM_GARBAGE_COLLECTION_BATCH
            || self.garbage_collection.grace_period <= self.operation_timeout
            || self.garbage_collection.assertion_retention <= self.maximum_caller_deadline
            || self.garbage_collection.maximum_work_lag <= self.garbage_collection.interval
            || self.garbage_collection.interval > MAXIMUM_TIMEOUT
            || self.garbage_collection.shutdown_timeout > MAXIMUM_TIMEOUT
            || self.garbage_collection.grace_period > MAXIMUM_GARBAGE_COLLECTION_GRACE
            || self.garbage_collection.assertion_retention > MAXIMUM_GARBAGE_COLLECTION_GRACE
            || self.garbage_collection.maximum_work_lag > MAXIMUM_GARBAGE_COLLECTION_GRACE
        {
            return Err(DynamoDbConfigError::InvalidGarbageCollection);
        }
        Ok(())
    }

    /// Returns the table name.
    #[must_use]
    pub const fn table_name(&self) -> &DynamoDbTableName {
        &self.table_name
    }

    /// Returns the Region.
    #[must_use]
    pub const fn region(&self) -> &RegionName {
        &self.region
    }

    /// Returns the optional local endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> Option<&DevelopmentEndpoint> {
        self.endpoint.as_ref()
    }
}

/// Invalid `DynamoDB` backend configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum DynamoDbConfigError {
    /// Table name violates `DynamoDB`'s bounded allowlist.
    #[error("invalid DynamoDB table name")]
    InvalidTableName,
    /// Region is empty, oversized, or contains unsupported characters.
    #[error("invalid AWS Region name")]
    InvalidRegion,
    /// Development endpoint is malformed or contains forbidden URL components.
    #[error("invalid DynamoDB development endpoint")]
    InvalidEndpoint,
    /// Development endpoint host is not a literal loopback address.
    #[error("DynamoDB development endpoint must use a loopback IP literal")]
    EndpointNotLoopback,
    /// KMS key identifier violates the bounded allowlist.
    #[error("invalid DynamoDB KMS key identifier")]
    InvalidKmsKeyIdentifier,
    /// Provisioning tags are empty, oversized, or contain unsupported characters.
    #[error("invalid DynamoDB provisioning tags")]
    InvalidTags,
    /// Tuple-mutation limit exceeds `DynamoDB` transaction capacity.
    #[error("DynamoDB tuple-mutation limit must be between 1 and 49")]
    InvalidMutationLimit,
    /// An operation timeout is zero or inconsistent.
    #[error("invalid DynamoDB timeout policy")]
    InvalidTimeout,
    /// Operation timeout exceeds the supported ceiling.
    #[error("DynamoDB timeout exceeds five minutes")]
    TimeoutTooLong,
    /// Garbage-collection timing is invalid.
    #[error("invalid DynamoDB garbage-collection policy")]
    InvalidGarbageCollection,
    /// A concurrency, retry, or batch limit exceeds the supported ceiling.
    #[error("invalid DynamoDB resource limit")]
    InvalidResourceLimit,
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, num::NonZeroU32, str::FromStr, time::Duration};

    use super::{
        DevelopmentEndpoint, DynamoDbConfigError, DynamoDbProvisioningConfig,
        DynamoDbStorageConfig, DynamoDbTableName, KmsKeyIdentifier, RegionName,
    };

    #[test]
    fn test_should_accept_only_loopback_development_endpoints() {
        assert!(DevelopmentEndpoint::from_str("http://127.0.0.1:8000").is_ok());
        assert!(DevelopmentEndpoint::from_str("http://[::1]:8000").is_ok());
        assert!(DevelopmentEndpoint::from_str("http://localhost:8000").is_err());
        assert!(DevelopmentEndpoint::from_str("https://127.0.0.1:8000").is_err());
        assert!(DevelopmentEndpoint::from_str("http://127.0.0.1:8000?q=x").is_err());
    }

    #[test]
    fn test_should_validate_table_and_region_allowlists() {
        assert!(DynamoDbTableName::from_str("openfga.test-1").is_ok());
        assert!(DynamoDbTableName::from_str("bad/table").is_err());
        assert!(RegionName::from_str("us-west-2").is_ok());
        assert!(RegionName::from_str("US West 2").is_err());
    }

    #[test]
    fn test_should_validate_provisioning_identifiers_and_tags() -> Result<(), DynamoDbConfigError> {
        assert!(KmsKeyIdentifier::from_str("alias/openfga-production").is_ok());
        assert!(KmsKeyIdentifier::from_str("alias/openfga production").is_err());
        let config = DynamoDbStorageConfig::builder()
            .table_name(DynamoDbTableName::from_str("openfga-test")?)
            .region(RegionName::from_str("us-west-2")?)
            .provisioning(
                DynamoDbProvisioningConfig::builder()
                    .tags(BTreeMap::from([("bad key".to_owned(), "value".to_owned())]))
                    .build(),
            )
            .build();
        assert_eq!(config.validate(), Err(DynamoDbConfigError::InvalidTags));
        Ok(())
    }

    #[test]
    fn test_should_reject_unbounded_retry_and_timeout_policies() -> Result<(), DynamoDbConfigError>
    {
        let config = DynamoDbStorageConfig::builder()
            .table_name(DynamoDbTableName::from_str("openfga-test")?)
            .region(RegionName::from_str("us-west-2")?)
            .maximum_attempts(NonZeroU32::new(11).ok_or(DynamoDbConfigError::InvalidResourceLimit)?)
            .build();
        assert_eq!(
            config.validate(),
            Err(DynamoDbConfigError::InvalidResourceLimit)
        );

        let config = DynamoDbStorageConfig::builder()
            .table_name(DynamoDbTableName::from_str("openfga-test")?)
            .region(RegionName::from_str("us-west-2")?)
            .operation_timeout(Duration::from_secs(5))
            .garbage_collection(
                super::DynamoDbGarbageCollectionConfig::builder()
                    .grace_period(Duration::from_secs(5))
                    .build(),
            )
            .build();
        assert_eq!(
            config.validate(),
            Err(DynamoDbConfigError::InvalidGarbageCollection)
        );

        let config = DynamoDbStorageConfig::builder()
            .table_name(DynamoDbTableName::from_str("openfga-test")?)
            .region(RegionName::from_str("us-west-2")?)
            .maximum_caller_deadline(Duration::from_secs(30))
            .garbage_collection(
                super::DynamoDbGarbageCollectionConfig::builder()
                    .assertion_retention(Duration::from_secs(30))
                    .build(),
            )
            .build();
        assert_eq!(
            config.validate(),
            Err(DynamoDbConfigError::InvalidGarbageCollection)
        );

        let config = DynamoDbStorageConfig::builder()
            .table_name(DynamoDbTableName::from_str("openfga-test")?)
            .region(RegionName::from_str("us-west-2")?)
            .garbage_collection(
                super::DynamoDbGarbageCollectionConfig::builder()
                    .interval(Duration::from_secs(30))
                    .maximum_work_lag(Duration::from_secs(30))
                    .build(),
            )
            .build();
        assert_eq!(
            config.validate(),
            Err(DynamoDbConfigError::InvalidGarbageCollection)
        );
        Ok(())
    }
}
