//! Immutable authorization-model source, compilation, and latest-alias caches.

use std::{
    fmt,
    future::{Future, ready},
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use moka::{Entry, future::Cache, ops::compute::Op};
use openfga_domain::{AuthorizationModelId, ConsistencyPreference, StoreId};
use openfga_model::{
    AuthorizationModelSource, CompiledModel, MODEL_COMPILER_FORMAT_VERSION, ModelCompiler,
};
use openfga_storage::{
    ModelReader, ModelWriter, OperationContext, Page, PageOptions, StorageError, StorageErrorKind,
    StoredAuthorizationModel,
};
use thiserror::Error;
use tokio::time::{Instant as TokioInstant, sleep_until};

use crate::metrics::CacheMetrics;

const MAXIMUM_IMMUTABLE_TTL: Duration = Duration::from_hours(720);
const MAXIMUM_ALIAS_TTL: Duration = Duration::from_mins(5);
const MAXIMUM_MODEL_CACHE_BYTES: u64 = 512 * 1024 * 1024;
const ESTIMATED_CACHE_ENTRY_OVERHEAD_BYTES: usize = 256;

/// Validated capacity and lifetime policy for immutable model caches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ModelCacheConfig {
    maximum_source_weight: NonZeroU64,
    maximum_compiled_weight: NonZeroU64,
    maximum_latest_aliases: NonZeroU64,
    immutable_ttl: Duration,
    latest_alias_ttl: Duration,
}

impl ModelCacheConfig {
    /// Creates a finite model-cache policy.
    ///
    /// Source and compiled capacities are conservative owned-byte estimates,
    /// while latest aliases are measured as entries.
    ///
    /// # Errors
    ///
    /// Returns an error when a byte capacity or TTL exceeds its safety ceiling.
    pub fn new(
        maximum_source_weight: NonZeroU64,
        maximum_compiled_weight: NonZeroU64,
        maximum_latest_aliases: NonZeroU64,
        immutable_ttl: Duration,
        latest_alias_ttl: Duration,
    ) -> Result<Self, ModelCacheConfigError> {
        if maximum_source_weight.get() > MAXIMUM_MODEL_CACHE_BYTES
            || maximum_compiled_weight.get() > MAXIMUM_MODEL_CACHE_BYTES
        {
            return Err(ModelCacheConfigError::MaximumWeight);
        }
        if immutable_ttl.is_zero() || immutable_ttl > MAXIMUM_IMMUTABLE_TTL {
            return Err(ModelCacheConfigError::ImmutableTtl);
        }
        if latest_alias_ttl.is_zero() || latest_alias_ttl > MAXIMUM_ALIAS_TTL {
            return Err(ModelCacheConfigError::LatestAliasTtl);
        }
        Ok(Self {
            maximum_source_weight,
            maximum_compiled_weight,
            maximum_latest_aliases,
            immutable_ttl,
            latest_alias_ttl,
        })
    }
}

impl Default for ModelCacheConfig {
    fn default() -> Self {
        Self {
            maximum_source_weight: NonZeroU64::new(64 * 1024 * 1024).unwrap_or(NonZeroU64::MIN),
            maximum_compiled_weight: NonZeroU64::new(128 * 1024 * 1024).unwrap_or(NonZeroU64::MIN),
            maximum_latest_aliases: NonZeroU64::new(10_000).unwrap_or(NonZeroU64::MIN),
            immutable_ttl: Duration::from_hours(168),
            latest_alias_ttl: Duration::from_secs(10),
        }
    }
}

/// Invalid immutable model-cache configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelCacheConfigError {
    /// A source or compiled-model cache exceeds the per-cache process ceiling.
    #[error("model cache byte capacity must not exceed 536870912")]
    MaximumWeight,
    /// The immutable-entry TTL is zero or longer than 30 days.
    #[error("immutable model cache TTL must be between one nanosecond and 30 days")]
    ImmutableTtl,
    /// The mutable latest-alias TTL is zero or longer than five minutes.
    #[error("latest model alias TTL must be between one nanosecond and five minutes")]
    LatestAliasTtl,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ModelKey {
    store_id: StoreId,
    model_id: AuthorizationModelId,
}

impl ModelKey {
    const fn new(store_id: StoreId, model_id: AuthorizationModelId) -> Self {
        Self { store_id, model_id }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CompiledModelKey {
    model: ModelKey,
    compiler_version: u32,
}

impl CompiledModelKey {
    const fn current(model: ModelKey) -> Self {
        Self {
            model,
            compiler_version: MODEL_COMPILER_FORMAT_VERSION,
        }
    }
}

#[derive(Clone)]
struct SourceEntry {
    source: Arc<AuthorizationModelSource>,
    written_at: SystemTime,
}

#[derive(Clone)]
struct CompiledEntry {
    model: Arc<CompiledModel>,
    weight: u32,
}

impl fmt::Debug for CompiledEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledEntry")
            .field("store_id", self.model.store_id())
            .field("model_id", self.model.model_id())
            .field("compiler_version", &self.model.compiler_format_version())
            .field("weight", &self.weight)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for SourceEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceEntry")
            .field("store_id", self.source.store_id())
            .field("model_id", self.source.model_id())
            .field("written_at", &self.written_at)
            .finish_non_exhaustive()
    }
}

/// A cache-fronted immutable model capability with coherent local publication.
///
/// Concurrent cold reads for the same source or compiler-version key are
/// coalesced by Moka. Failed loads and failed compilations are returned to all
/// waiters but are never inserted. The short-lived latest alias is invalidated
/// after every successful local publication; immutable explicit-ID entries are
/// populated immediately.
#[derive(Clone)]
#[non_exhaustive]
pub struct CachedModelStorage {
    reader: Arc<dyn ModelReader>,
    writer: Arc<dyn ModelWriter>,
    compiler: ModelCompiler,
    sources: Cache<ModelKey, Arc<SourceEntry>>,
    compiled: Cache<CompiledModelKey, Arc<CompiledEntry>>,
    latest_aliases: Cache<StoreId, ModelKey>,
    metrics: CacheMetrics,
}

impl CachedModelStorage {
    /// Creates bounded immutable caches around model storage.
    #[must_use]
    pub fn new(
        reader: Arc<dyn ModelReader>,
        writer: Arc<dyn ModelWriter>,
        model_compiler: ModelCompiler,
        config: ModelCacheConfig,
    ) -> Self {
        let sources = Cache::builder()
            .max_capacity(config.maximum_source_weight.get())
            .time_to_live(config.immutable_ttl)
            .weigher(|_key: &ModelKey, entry: &Arc<SourceEntry>| {
                cache_weight(entry.source.estimated_owned_bytes())
            })
            .build();
        let compiled_cache = Cache::builder()
            .max_capacity(config.maximum_compiled_weight.get())
            .time_to_live(config.immutable_ttl)
            .weigher(|_key: &CompiledModelKey, entry: &Arc<CompiledEntry>| entry.weight)
            .build();
        let latest_aliases = Cache::builder()
            .max_capacity(config.maximum_latest_aliases.get())
            .time_to_live(config.latest_alias_ttl)
            .build();
        Self {
            reader,
            writer,
            compiler: model_compiler,
            sources,
            compiled: compiled_cache,
            latest_aliases,
            metrics: CacheMetrics::new(),
        }
    }

    async fn resolve_key(
        &self,
        context: &OperationContext,
        key: ModelKey,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        context.check()?;
        let reader = Arc::clone(&self.reader);
        let load_key = key.clone();
        let source = if let Some(source) = self.sources.get(&key).await {
            self.metrics.record("model_source", "hit");
            source
        } else {
            self.metrics.record("model_source", "miss");
            wait_for_cache(
                context,
                self.sources.try_get_with(key.clone(), async move {
                    let model = reader
                        .read_model(context, load_key.store_id, load_key.model_id)
                        .await?;
                    Ok::<Arc<SourceEntry>, StorageError>(Arc::new(SourceEntry {
                        source: Arc::clone(model.source()),
                        written_at: model.written_at(),
                    }))
                }),
            )
            .await?
        };

        let model_compiler = self.compiler.clone();
        let compile_source = Arc::clone(&source.source);
        let compiled_key = CompiledModelKey::current(key);
        let compiled_entry = if let Some(entry) = self.compiled.get(&compiled_key).await {
            self.metrics.record("model_compiled", "hit");
            entry
        } else {
            self.metrics.record("model_compiled", "miss");
            wait_for_cache(
                context,
                self.compiled.try_get_with(compiled_key, async move {
                    model_compiler
                        .compile(&compile_source)
                        .map(|model| {
                            Arc::new(CompiledEntry {
                                weight: cache_weight(model.estimated_owned_bytes()),
                                model,
                            })
                        })
                        .map_err(|error| {
                            StorageError::with_source(
                                StorageErrorKind::Integrity,
                                "cached_model_compile_failed",
                                error,
                            )
                        })
                }),
            )
            .await?
        };
        context.check()?;
        StoredAuthorizationModel::new(
            Arc::clone(&source.source),
            Arc::clone(&compiled_entry.model),
            source.written_at,
        )
        .map(Arc::new)
    }

    async fn cache_published(&self, model: &Arc<StoredAuthorizationModel>) {
        let key = ModelKey::new(*model.store_id(), *model.model_id());
        self.sources
            .insert(
                key.clone(),
                Arc::new(SourceEntry {
                    source: Arc::clone(model.source()),
                    written_at: model.written_at(),
                }),
            )
            .await;
        self.compiled
            .insert(
                CompiledModelKey {
                    model: key,
                    compiler_version: model.compiled().compiler_format_version(),
                },
                Arc::new(CompiledEntry {
                    model: Arc::clone(model.compiled()),
                    weight: cache_weight(model.compiled().estimated_owned_bytes()),
                }),
            )
            .await;
    }
}

impl fmt::Debug for CachedModelStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedModelStorage")
            .field("reader", &"dyn ModelReader")
            .field("writer", &"dyn ModelWriter")
            .field("compiler", &self.compiler)
            .field("source_entries", &self.sources.entry_count())
            .field("compiled_entries", &self.compiled.entry_count())
            .field("latest_alias_entries", &self.latest_aliases.entry_count())
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ModelReader for CachedModelStorage {
    async fn read_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        self.resolve_key(context, ModelKey::new(store_id, model_id))
            .await
    }

    async fn read_latest_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        context.check()?;
        if context.consistency() == ConsistencyPreference::HigherConsistency {
            self.metrics
                .record("model_latest_alias", "bypass_consistency");
            let model = self.reader.read_latest_model(context, store_id).await?;
            self.cache_published(&model).await;
            context.check()?;
            return Ok(model);
        }
        let reader = Arc::clone(&self.reader);
        let sources = self.sources.clone();
        let compiled = self.compiled.clone();
        let key = if let Some(key) = self.latest_aliases.get(&store_id).await {
            self.metrics.record("model_latest_alias", "hit");
            key
        } else {
            self.metrics.record("model_latest_alias", "miss");
            let computed = wait_for_cache(
                context,
                self.latest_aliases
                    .entry(store_id)
                    .and_try_compute_with(|existing| async move {
                        if existing.is_some() {
                            return Ok::<Op<ModelKey>, Arc<StorageError>>(Op::Nop);
                        }
                        let model = reader.read_latest_model(context, store_id).await?;
                        let key = ModelKey::new(*model.store_id(), *model.model_id());
                        sources
                            .insert(
                                key.clone(),
                                Arc::new(SourceEntry {
                                    source: Arc::clone(model.source()),
                                    written_at: model.written_at(),
                                }),
                            )
                            .await;
                        compiled
                            .insert(
                                CompiledModelKey {
                                    model: key.clone(),
                                    compiler_version: model.compiled().compiler_format_version(),
                                },
                                Arc::new(CompiledEntry {
                                    model: Arc::clone(model.compiled()),
                                    weight: cache_weight(model.compiled().estimated_owned_bytes()),
                                }),
                            )
                            .await;
                        Ok::<Op<ModelKey>, Arc<StorageError>>(Op::Put(key))
                    }),
            )
            .await?;
            computed
                .into_entry()
                .map(Entry::into_value)
                .ok_or_else(|| {
                    StorageError::new(
                        StorageErrorKind::Integrity,
                        "model_latest_alias_compute_missing",
                    )
                })?
        };
        self.resolve_key(context, key).await
    }

    async fn list_models(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        options: &PageOptions,
    ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError> {
        context.check()?;
        let page = self.reader.list_models(context, store_id, options).await?;
        for model in page.items() {
            self.cache_published(model).await;
        }
        context.check()?;
        Ok(page)
    }
}

#[async_trait]
impl ModelWriter for CachedModelStorage {
    async fn write_model(
        &self,
        context: &OperationContext,
        model: Arc<StoredAuthorizationModel>,
    ) -> Result<(), StorageError> {
        context.check()?;
        self.writer.write_model(context, Arc::clone(&model)).await?;
        self.cache_published(&model).await;
        self.latest_aliases
            .entry(*model.store_id())
            .and_compute_with(|_| ready(Op::Remove))
            .await;
        context.check()
    }
}

async fn wait_for_cache<T, F>(context: &OperationContext, future: F) -> Result<T, StorageError>
where
    F: Future<Output = Result<T, Arc<StorageError>>>,
{
    let cancellation = context.cancellation().clone();
    let deadline = TokioInstant::from_std(context.deadline().instant());
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(StorageError::new(
            StorageErrorKind::Cancelled,
            "model_cache_wait_cancelled",
        )),
        () = sleep_until(deadline) => Err(StorageError::new(
            StorageErrorKind::Timeout,
            "model_cache_wait_timeout",
        )),
        result = future => result.map_err(|error| StorageError::new(error.kind(), error.code())),
    }
}

fn cache_weight(estimated_owned_bytes: usize) -> u32 {
    u32::try_from(
        estimated_owned_bytes
            .saturating_add(ESTIMATED_CACHE_ENTRY_OVERHEAD_BYTES)
            .max(1),
    )
    .unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        num::NonZeroU64,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant, SystemTime},
    };

    use async_trait::async_trait;
    use openfga_domain::{
        AuthorizationModelId, ConsistencyPreference, Deadline, RelationName, RequestTimeout,
        StoreId, TypeName,
    };
    use openfga_model::{
        AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
        RestrictionKindSource, RewriteSource, TypeDefinitionSource,
    };
    use openfga_storage::{
        ModelReader, ModelWriter, OperationContext, Page, PageOptions, StorageCancellationToken,
        StorageError, StoredAuthorizationModel,
    };
    use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
    use tokio::{sync::Notify, task::JoinSet};

    use super::{CachedModelStorage, ModelCacheConfig};

    #[derive(Debug)]
    struct CountingModelReader {
        inner: Arc<MemoryStorage>,
        explicit_reads: AtomicUsize,
        latest_reads: AtomicUsize,
    }

    impl CountingModelReader {
        const fn new(inner: Arc<MemoryStorage>) -> Self {
            Self {
                inner,
                explicit_reads: AtomicUsize::new(0),
                latest_reads: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl ModelReader for CountingModelReader {
        async fn read_model(
            &self,
            context: &OperationContext,
            store_id: StoreId,
            model_id: AuthorizationModelId,
        ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
            self.explicit_reads.fetch_add(1, Ordering::Relaxed);
            self.inner.read_model(context, store_id, model_id).await
        }

        async fn read_latest_model(
            &self,
            context: &OperationContext,
            store_id: StoreId,
        ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
            self.latest_reads.fetch_add(1, Ordering::Relaxed);
            self.inner.read_latest_model(context, store_id).await
        }

        async fn list_models(
            &self,
            context: &OperationContext,
            store_id: StoreId,
            options: &PageOptions,
        ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError> {
            self.inner.list_models(context, store_id, options).await
        }
    }

    #[derive(Debug)]
    struct PublicationRaceStorage {
        inner: Arc<MemoryStorage>,
        block_next_latest: AtomicBool,
        latest_snapshot_taken: Notify,
        publication_committed: Notify,
        release_latest_snapshot: Notify,
    }

    impl PublicationRaceStorage {
        const fn new(inner: Arc<MemoryStorage>) -> Self {
            Self {
                inner,
                block_next_latest: AtomicBool::new(true),
                latest_snapshot_taken: Notify::const_new(),
                publication_committed: Notify::const_new(),
                release_latest_snapshot: Notify::const_new(),
            }
        }
    }

    #[async_trait]
    impl ModelReader for PublicationRaceStorage {
        async fn read_model(
            &self,
            context: &OperationContext,
            store_id: StoreId,
            model_id: AuthorizationModelId,
        ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
            self.inner.read_model(context, store_id, model_id).await
        }

        async fn read_latest_model(
            &self,
            context: &OperationContext,
            store_id: StoreId,
        ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
            let model = self.inner.read_latest_model(context, store_id).await?;
            if self.block_next_latest.swap(false, Ordering::AcqRel) {
                self.latest_snapshot_taken.notify_one();
                self.release_latest_snapshot.notified().await;
            }
            Ok(model)
        }

        async fn list_models(
            &self,
            context: &OperationContext,
            store_id: StoreId,
            options: &PageOptions,
        ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError> {
            self.inner.list_models(context, store_id, options).await
        }
    }

    #[async_trait]
    impl ModelWriter for PublicationRaceStorage {
        async fn write_model(
            &self,
            context: &OperationContext,
            model: Arc<StoredAuthorizationModel>,
        ) -> Result<(), StorageError> {
            self.inner.write_model(context, model).await?;
            self.publication_committed.notify_one();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_should_coalesce_concurrent_explicit_model_loads()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let store_id = store_id()?;
        let model = stored_model(store_id, "01ARZ3NDEKTSV4RRFFQ69G5FAW")?;
        storage
            .write_model(
                &context(ConsistencyPreference::HigherConsistency)?,
                Arc::clone(&model),
            )
            .await?;
        let reader = Arc::new(CountingModelReader::new(Arc::clone(&storage)));
        let reader_capability: Arc<dyn ModelReader> = reader.clone();
        let writer_capability: Arc<dyn ModelWriter> = storage.clone();
        let cache = Arc::new(CachedModelStorage::new(
            reader_capability,
            writer_capability,
            ModelCompiler::default(),
            ModelCacheConfig::default(),
        ));
        let mut reads = JoinSet::new();
        for _ in 0..32 {
            let cache = Arc::clone(&cache);
            let model_id = *model.model_id();
            reads.spawn(async move {
                cache
                    .read_model(
                        &context(ConsistencyPreference::MinimizeLatency)?,
                        store_id,
                        model_id,
                    )
                    .await
                    .map_err(Box::<dyn Error + Send + Sync>::from)
            });
        }
        while let Some(result) = reads.join_next().await {
            let loaded = result??;
            assert_eq!(loaded.model_id(), model.model_id());
        }
        assert_eq!(reader.explicit_reads.load(Ordering::Relaxed), 1);

        drop(cache);
        drop(reader);
        stop_storage(storage).await
    }

    #[tokio::test]
    async fn test_should_bypass_latest_alias_for_higher_consistency()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let store_id = store_id()?;
        let model = stored_model(store_id, "01ARZ3NDEKTSV4RRFFQ69G5FAW")?;
        storage
            .write_model(&context(ConsistencyPreference::HigherConsistency)?, model)
            .await?;
        let reader = Arc::new(CountingModelReader::new(Arc::clone(&storage)));
        let cache = CachedModelStorage::new(
            reader.clone(),
            storage.clone(),
            ModelCompiler::default(),
            ModelCacheConfig::default(),
        );

        for _ in 0..2 {
            cache
                .read_latest_model(&context(ConsistencyPreference::MinimizeLatency)?, store_id)
                .await?;
        }
        assert_eq!(reader.latest_reads.load(Ordering::Relaxed), 1);
        for _ in 0..2 {
            cache
                .read_latest_model(
                    &context(ConsistencyPreference::HigherConsistency)?,
                    store_id,
                )
                .await?;
        }
        assert_eq!(reader.latest_reads.load(Ordering::Relaxed), 3);

        drop(cache);
        drop(reader);
        stop_storage(storage).await
    }

    #[tokio::test]
    async fn test_should_invalidate_latest_alias_after_publication()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let store_id = store_id()?;
        let reader = Arc::new(CountingModelReader::new(Arc::clone(&storage)));
        let cache = CachedModelStorage::new(
            reader.clone(),
            storage.clone(),
            ModelCompiler::default(),
            ModelCacheConfig::default(),
        );
        let operation = context(ConsistencyPreference::MinimizeLatency)?;
        let first = stored_model(store_id, "01ARZ3NDEKTSV4RRFFQ69G5FAW")?;
        cache.write_model(&operation, first).await?;
        let first_latest = cache.read_latest_model(&operation, store_id).await?;
        let second = stored_model(store_id, "01ARZ3NDEKTSV4RRFFQ69G5FAX")?;
        cache.write_model(&operation, Arc::clone(&second)).await?;
        let second_latest = cache.read_latest_model(&operation, store_id).await?;

        assert_ne!(first_latest.model_id(), second_latest.model_id());
        assert_eq!(second_latest.model_id(), second.model_id());
        assert_eq!(reader.latest_reads.load(Ordering::Relaxed), 2);

        drop(cache);
        drop(reader);
        stop_storage(storage).await
    }

    #[tokio::test]
    async fn test_should_not_restore_stale_latest_alias_after_concurrent_publication()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let store_id = store_id()?;
        let first = stored_model(store_id, "01ARZ3NDEKTSV4RRFFQ69G5FAW")?;
        storage
            .write_model(
                &context(ConsistencyPreference::HigherConsistency)?,
                Arc::clone(&first),
            )
            .await?;
        let race_storage = Arc::new(PublicationRaceStorage::new(Arc::clone(&storage)));
        let cache = Arc::new(CachedModelStorage::new(
            race_storage.clone(),
            race_storage.clone(),
            ModelCompiler::default(),
            ModelCacheConfig::default(),
        ));

        let read_cache = Arc::clone(&cache);
        let cold_read = tokio::spawn(async move {
            read_cache
                .read_latest_model(&context(ConsistencyPreference::MinimizeLatency)?, store_id)
                .await
                .map_err(Box::<dyn Error + Send + Sync>::from)
        });
        race_storage.latest_snapshot_taken.notified().await;

        let second = stored_model(store_id, "01ARZ3NDEKTSV4RRFFQ69G5FAX")?;
        let write_cache = Arc::clone(&cache);
        let published_model = Arc::clone(&second);
        let publication = tokio::spawn(async move {
            write_cache
                .write_model(
                    &context(ConsistencyPreference::MinimizeLatency)?,
                    published_model,
                )
                .await
                .map_err(Box::<dyn Error + Send + Sync>::from)
        });
        race_storage.publication_committed.notified().await;
        race_storage.release_latest_snapshot.notify_one();

        let overlapping_read = cold_read.await??;
        publication.await??;
        assert_eq!(overlapping_read.model_id(), first.model_id());
        let latest = cache
            .read_latest_model(&context(ConsistencyPreference::MinimizeLatency)?, store_id)
            .await?;
        assert_eq!(latest.model_id(), second.model_id());

        drop(cache);
        drop(race_storage);
        stop_storage(storage).await
    }

    #[tokio::test]
    async fn test_should_evict_source_and_compiled_models_by_owned_byte_weight()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let store_id = store_id()?;
        let first = stored_model(store_id, "01ARZ3NDEKTSV4RRFFQ69G5FAW")?;
        let second = stored_model(store_id, "01ARZ3NDEKTSV4RRFFQ69G5FAX")?;
        let source_capacity =
            u64::from(super::cache_weight(first.source().estimated_owned_bytes()));
        let compiled_capacity = u64::from(super::cache_weight(
            first.compiled().estimated_owned_bytes(),
        ));
        let config = ModelCacheConfig::new(
            NonZeroU64::new(source_capacity).ok_or("source cache capacity was zero")?,
            NonZeroU64::new(compiled_capacity).ok_or("compiled cache capacity was zero")?,
            NonZeroU64::MIN,
            Duration::from_mins(1),
            Duration::from_secs(10),
        )?;
        let cache = CachedModelStorage::new(
            storage.clone(),
            storage.clone(),
            ModelCompiler::default(),
            config,
        );

        let operation = context(ConsistencyPreference::MinimizeLatency)?;
        cache.write_model(&operation, first).await?;
        cache.write_model(&operation, second).await?;
        cache.sources.run_pending_tasks().await;
        cache.compiled.run_pending_tasks().await;

        assert!(cache.sources.weighted_size() <= source_capacity);
        assert!(cache.compiled.weighted_size() <= compiled_capacity);
        assert!(cache.sources.entry_count() <= 1);
        assert!(cache.compiled.entry_count() <= 1);

        drop(cache);
        stop_storage(storage).await
    }

    fn context(
        consistency: ConsistencyPreference,
    ) -> Result<OperationContext, Box<dyn Error + Send + Sync>> {
        let timeout = RequestTimeout::new(Duration::from_secs(5))?;
        let deadline = Deadline::from_timeout(Instant::now(), timeout)?;
        Ok(OperationContext::new(
            consistency,
            deadline,
            StorageCancellationToken::new(),
        ))
    }

    fn store_id() -> Result<StoreId, Box<dyn Error + Send + Sync>> {
        Ok("01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?)
    }

    fn stored_model(
        store_id: StoreId,
        model_id: &str,
    ) -> Result<Arc<StoredAuthorizationModel>, Box<dyn Error + Send + Sync>> {
        let source = Arc::new(AuthorizationModelSource::new(
            store_id,
            model_id.parse()?,
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
        ));
        let compiled = ModelCompiler::default().compile(&source)?;
        Ok(Arc::new(StoredAuthorizationModel::new(
            source,
            compiled,
            SystemTime::now(),
        )?))
    }

    async fn stop_storage(storage: Arc<MemoryStorage>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut storage =
            Arc::try_unwrap(storage).map_err(|_| "memory storage references remain")?;
        storage.stop().await?;
        Ok(())
    }
}
