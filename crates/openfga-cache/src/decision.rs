//! Complete semantic decision identities and bounded decision storage.

use std::{fmt, mem::size_of, num::NonZeroU64, sync::Arc, time::Duration};

use getrandom::fill;
use hmac::{Hmac, KeyInit, Mac};
use moka::future::Cache;
use openfga_domain::{CheckCommand, Fingerprint, FingerprintBuilder, StoreId};
use openfga_model::CompiledModel;
use sha2::Sha256;
use thiserror::Error;

use crate::{InvalidationControllerHandle, InvalidationWatermark, metrics::CacheMetrics};

const MAXIMUM_DECISION_TTL: Duration = Duration::from_hours(24);
const MAXIMUM_DECISION_CACHE_BYTES: u64 = 512 * 1024 * 1024;
const ESTIMATED_CACHE_ENTRY_OVERHEAD_BYTES: usize = 128;
type HmacSha256 = Hmac<Sha256>;

/// Validated bounded decision-cache policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DecisionCacheConfig {
    maximum_weight: NonZeroU64,
    ttl: Duration,
}

impl DecisionCacheConfig {
    /// Creates a finite fixed-size decision policy measured in estimated bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the byte capacity is too large or the TTL is invalid.
    pub fn new(
        maximum_weight: NonZeroU64,
        ttl: Duration,
    ) -> Result<Self, DecisionCacheConfigError> {
        if maximum_weight.get() > MAXIMUM_DECISION_CACHE_BYTES {
            return Err(DecisionCacheConfigError::MaximumWeight);
        }
        if ttl.is_zero() || ttl > MAXIMUM_DECISION_TTL {
            return Err(DecisionCacheConfigError::Ttl);
        }
        Ok(Self {
            maximum_weight,
            ttl,
        })
    }
}

/// Invalid decision-cache configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DecisionCacheConfigError {
    /// The decision cache exceeds the per-cache process ceiling.
    #[error("decision cache byte capacity must not exceed 536870912")]
    MaximumWeight,
    /// The entry TTL is zero or longer than 24 hours.
    #[error("decision cache TTL must be between one nanosecond and 24 hours")]
    Ttl,
}

/// Process-keyed semantic decision identity generator.
#[derive(Clone)]
#[non_exhaustive]
pub struct DecisionKeyHasher(Arc<[u8; 64]>);

impl DecisionKeyHasher {
    /// Generates a fresh process-local key from the operating system CSPRNG.
    ///
    /// # Errors
    ///
    /// Returns an entropy error when the operating system cannot provide key material.
    pub fn random() -> Result<Self, DecisionKeyHasherError> {
        let mut key = [0_u8; 64];
        fill(&mut key).map_err(|_| DecisionKeyHasherError::Entropy)?;
        Ok(Self(Arc::new(key)))
    }

    #[cfg(test)]
    fn from_key(key: [u8; 64]) -> Self {
        Self(Arc::new(key))
    }

    fn keyed_context(&self, contextual: Fingerprint, condition: Fingerprint) -> Fingerprint {
        let mut mac = <HmacSha256 as KeyInit>::new(self.0.as_ref().into());
        mac.update(contextual.as_bytes());
        mac.update(condition.as_bytes());
        Fingerprint::from_bytes(mac.finalize().into_bytes().into())
    }
}

impl fmt::Debug for DecisionKeyHasher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DecisionKeyHasher([REDACTED])")
    }
}

/// Failure to initialize a process-keyed decision identity generator.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DecisionKeyHasherError {
    /// Operating-system entropy was unavailable.
    #[error("operating-system entropy unavailable for decision cache key")]
    Entropy,
}

/// Opaque, redacted identity of every semantic input to one Check decision.
#[derive(Clone, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct DecisionKey {
    store_id: StoreId,
    fingerprint: Fingerprint,
}

impl DecisionKey {
    /// Computes the complete decision identity after model selection resolves.
    #[must_use]
    pub fn for_check(
        command: &CheckCommand,
        model: &CompiledModel,
        hasher: &DecisionKeyHasher,
        evaluator_semantics_version: u32,
    ) -> Self {
        let query = command.query();
        let keyed_context = hasher.keyed_context(
            query.contextual_tuples().fingerprint(),
            query.condition_context().fingerprint(),
        );
        let mut fingerprint = FingerprintBuilder::new("openfga.check-decision-key.v1");
        fingerprint.write_str(&query.store_id().to_string());
        fingerprint.write_str(&model.model_id().to_string());
        fingerprint.write_bytes(model.fingerprint().as_bytes());
        fingerprint.write_bytes(command.tuple().fingerprint().as_bytes());
        fingerprint.write_bytes(keyed_context.as_bytes());
        fingerprint.write_u32(evaluator_semantics_version);
        Self {
            store_id: query.store_id(),
            fingerprint: fingerprint.finish(),
        }
    }
}

impl fmt::Debug for DecisionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecisionKey")
            .field("store_id", &self.store_id)
            .field("fingerprint", &self.fingerprint)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct DecisionEntry<V> {
    value: V,
    watermark: u64,
}

/// Bounded successful-decision cache guarded by a monotonic watermark.
#[derive(Clone)]
#[non_exhaustive]
pub struct DecisionCache<V>
where
    V: Clone + Send + Sync + 'static,
{
    entries: Cache<DecisionKey, Arc<DecisionEntry<V>>>,
    invalidation: InvalidationWatermark,
    controller: Option<InvalidationControllerHandle>,
    metrics: CacheMetrics,
}

impl<V> DecisionCache<V>
where
    V: Clone + Send + Sync + 'static,
{
    /// Creates a cache using the shared mutable-state watermark.
    #[must_use]
    pub fn new(config: DecisionCacheConfig, invalidation: InvalidationWatermark) -> Self {
        let entries = Cache::builder()
            .max_capacity(config.maximum_weight.get())
            .time_to_live(config.ttl)
            .weigher(|_key: &DecisionKey, _entry: &Arc<DecisionEntry<V>>| {
                u32::try_from(
                    size_of::<DecisionKey>()
                        .saturating_add(size_of::<DecisionEntry<V>>())
                        .saturating_add(ESTIMATED_CACHE_ENTRY_OVERHEAD_BYTES)
                        .max(1),
                )
                .unwrap_or(u32::MAX)
            })
            .build();
        Self {
            entries,
            invalidation,
            controller: None,
            metrics: CacheMetrics::new(),
        }
    }

    /// Creates a cache that registers active stores with the changelog controller.
    #[must_use]
    pub fn with_controller(
        config: DecisionCacheConfig,
        invalidation: InvalidationWatermark,
        controller: InvalidationControllerHandle,
    ) -> Self {
        let mut cache = Self::new(config, invalidation);
        cache.controller = Some(controller);
        cache
    }

    /// Captures the generation before an authoritative computation starts.
    #[must_use]
    pub fn begin_computation(&self) -> u64 {
        self.invalidation.current()
    }

    /// Returns a decision only if no invalidation races with lookup.
    pub async fn get(&self, key: &DecisionKey) -> Option<V> {
        if let Some(controller) = &self.controller {
            controller.track(key.store_id);
            if !controller.permits_caching(key.store_id) {
                self.metrics.record("decision", "bypass_controller");
                return None;
            }
        }
        let before = self.invalidation.current();
        let entry = self.entries.get(key).await;
        let after = self.invalidation.current();
        match entry {
            Some(entry)
                if before == after
                    && entry.watermark == after
                    && self
                        .controller
                        .as_ref()
                        .is_none_or(|controller| controller.permits_caching(key.store_id)) =>
            {
                self.metrics.record("decision", "hit");
                Some(entry.value.clone())
            }
            Some(_) => {
                self.metrics.record("decision", "invalidated");
                None
            }
            None => {
                self.metrics.record("decision", "miss");
                None
            }
        }
    }

    /// Inserts a successful result only if invalidation did not race computation.
    pub async fn insert_if_unchanged(&self, started_at: u64, key: DecisionKey, value: V) -> bool {
        let store_id = key.store_id;
        if self.invalidation.current() != started_at
            || self
                .controller
                .as_ref()
                .is_some_and(|controller| !controller.permits_caching(store_id))
        {
            return false;
        }
        self.entries
            .insert(
                key,
                Arc::new(DecisionEntry {
                    value,
                    watermark: started_at,
                }),
            )
            .await;
        self.invalidation.current() == started_at
            && self
                .controller
                .as_ref()
                .is_none_or(|controller| controller.permits_caching(store_id))
    }

    /// Records an explicit consistency bypass performed before key lookup.
    pub fn record_bypass(&self) {
        self.metrics.record("decision", "bypass_consistency");
    }
}

impl<V> fmt::Debug for DecisionCache<V>
where
    V: Clone + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecisionCache")
            .field("entries", &self.entries.entry_count())
            .field("watermark", &self.invalidation.current())
            .field("controller", &self.controller)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        num::{NonZeroU32, NonZeroU64, NonZeroUsize},
        sync::Arc,
        time::{Duration, Instant},
    };

    use openfga_domain::{
        AuthorizationModelId, CheckCommand, ConditionContext, ConsistencyPreference,
        ContextualTuples, Deadline, InputLimits, ModelSelection, Principal, PrincipalKind,
        QueryContext, RelationName, RelationshipTuple, RequestTimeout, StoreId, TupleKey, TypeName,
    };
    use openfga_model::{
        AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
        RestrictionKindSource, RewriteSource, TypeDefinitionSource,
    };
    use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
    use serde_json::json;

    use super::{DecisionCache, DecisionCacheConfig, DecisionKey, DecisionKeyHasher};
    use crate::{InvalidationController, InvalidationControllerConfig, InvalidationWatermark};

    #[tokio::test]
    async fn test_should_reject_entries_crossing_an_invalidation() -> Result<(), &'static str> {
        let watermark = InvalidationWatermark::new();
        let config = DecisionCacheConfig::new(
            NonZeroU64::new(4_096).ok_or("invalid test capacity")?,
            Duration::from_secs(1),
        )
        .map_err(|_| "invalid test config")?;
        let cache = DecisionCache::new(config, watermark.clone());
        let started = cache.begin_computation();
        let _advanced = watermark.advance();
        let key = test_key()?;
        assert!(!cache.insert_if_unchanged(started, key.clone(), true).await);
        assert_eq!(cache.get(&key).await, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_should_bypass_entries_until_store_controller_is_ready()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let watermark = InvalidationWatermark::new();
        let mut controller = InvalidationController::start(
            storage.clone(),
            watermark.clone(),
            InvalidationControllerConfig::new(
                NonZeroUsize::new(8).ok_or("invalid controller capacity")?,
                NonZeroU32::new(10).ok_or("invalid controller page size")?,
                Duration::from_millis(5),
                Duration::from_millis(20),
                Duration::from_millis(100),
            )?,
        )?;
        let mut cache = DecisionCache::new(
            DecisionCacheConfig::new(
                NonZeroU64::new(4_096).ok_or("invalid test capacity")?,
                Duration::from_secs(1),
            )?,
            watermark,
        );
        let key = test_key().map_err(str::to_owned)?;
        let started = cache.begin_computation();
        assert!(cache.insert_if_unchanged(started, key.clone(), true).await);
        cache.controller = Some(controller.handle());
        assert_eq!(cache.get(&key).await, None);

        let deadline = Instant::now() + Duration::from_secs(1);
        while cache.get(&key).await.is_none() {
            if Instant::now() >= deadline {
                return Err("cache controller bootstrap timed out".into());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        controller.stop().await?;
        drop(cache);
        drop(controller);
        let mut storage = Arc::try_unwrap(storage).map_err(|_| "storage references remain")?;
        storage.stop().await?;
        Ok(())
    }

    #[test]
    fn test_should_key_every_semantic_decision_input() -> Result<(), Box<dyn Error + Send + Sync>> {
        let model = compiled_model()?;
        let hasher = DecisionKeyHasher::from_key([7; 64]);
        let baseline_command = command(
            "document:one#viewer@user:anne",
            ContextualTuples::empty(),
            ConditionContext::empty(),
        )?;
        let baseline = DecisionKey::for_check(&baseline_command, &model, &hasher, 1);
        assert_eq!(
            baseline,
            DecisionKey::for_check(&baseline_command, &model, &hasher, 1)
        );

        let different_tuple = command(
            "document:two#viewer@user:anne",
            ContextualTuples::empty(),
            ConditionContext::empty(),
        )?;
        assert_ne!(
            baseline,
            DecisionKey::for_check(&different_tuple, &model, &hasher, 1)
        );
        let contextual = ContextualTuples::new(
            vec![RelationshipTuple::unconditional(
                "document:one#viewer@user:bob".parse()?,
            )],
            &InputLimits::default(),
        )?;
        let different_contextual = command(
            "document:one#viewer@user:anne",
            contextual,
            ConditionContext::empty(),
        )?;
        assert_ne!(
            baseline,
            DecisionKey::for_check(&different_contextual, &model, &hasher, 1)
        );
        let condition =
            ConditionContext::try_from_json(json!({"secret": "value"}), &InputLimits::default())?;
        let different_condition = command(
            "document:one#viewer@user:anne",
            ContextualTuples::empty(),
            condition,
        )?;
        assert_ne!(
            baseline,
            DecisionKey::for_check(&different_condition, &model, &hasher, 1)
        );
        assert_ne!(
            baseline,
            DecisionKey::for_check(&baseline_command, &model, &hasher, 2)
        );
        assert_ne!(
            baseline,
            DecisionKey::for_check(
                &baseline_command,
                &model,
                &DecisionKeyHasher::from_key([8; 64]),
                1,
            )
        );
        assert!(!format!("{baseline:?}").contains("secret"));
        Ok(())
    }

    fn compiled_model() -> Result<Arc<openfga_model::CompiledModel>, Box<dyn Error + Send + Sync>> {
        let source = AuthorizationModelSource::new(
            store_id()?,
            model_id()?,
            "1.1".to_owned(),
            vec![
                TypeDefinitionSource::new("user".parse::<TypeName>()?, Vec::new()),
                TypeDefinitionSource::new(
                    "document".parse::<TypeName>()?,
                    vec![RelationSource::new(
                        "viewer".parse::<RelationName>()?,
                        RewriteSource::Direct,
                        vec![DirectRestrictionSource::new(
                            "user".parse::<TypeName>()?,
                            RestrictionKindSource::Object,
                            None,
                        )],
                    )],
                ),
            ],
            Vec::new(),
        );
        Ok(ModelCompiler::default().compile(&source)?)
    }

    fn command(
        tuple: &str,
        contextual_tuples: ContextualTuples,
        condition_context: ConditionContext,
    ) -> Result<CheckCommand, Box<dyn Error + Send + Sync>> {
        let query = QueryContext::builder()
            .store_id(store_id()?)
            .model_selection(ModelSelection::Explicit(model_id()?))
            .consistency(ConsistencyPreference::MinimizeLatency)
            .contextual_tuples(contextual_tuples)
            .condition_context(condition_context)
            .deadline(Deadline::from_timeout(
                Instant::now(),
                RequestTimeout::new(Duration::from_secs(5))?,
            )?)
            .principal(Principal::new(
                PrincipalKind::Internal,
                "cache-tests".parse()?,
            ))
            .build();
        Ok(CheckCommand::new(query, tuple.parse::<TupleKey>()?))
    }

    fn store_id() -> Result<StoreId, Box<dyn Error + Send + Sync>> {
        Ok("01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?)
    }

    fn model_id() -> Result<AuthorizationModelId, Box<dyn Error + Send + Sync>> {
        Ok("01ARZ3NDEKTSV4RRFFQ69G5FAW".parse()?)
    }

    fn test_key() -> Result<super::DecisionKey, &'static str> {
        Ok(super::DecisionKey {
            store_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .map_err(|_| "invalid static store ID")?,
            fingerprint: openfga_domain::Fingerprint::from_bytes([1; 32]),
        })
    }
}
