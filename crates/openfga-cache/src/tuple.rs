//! Consistency-bypassed, watermark-validated tuple read caches.

use std::{fmt, num::NonZeroU64, sync::Arc, time::Duration};

use async_trait::async_trait;
use moka::future::Cache;
use openfga_domain::{
    ConsistencyPreference, Fingerprint, FingerprintBuilder, RelationshipTuple, StoreId, SubjectRef,
    TupleKey,
};
use openfga_storage::{
    ConditionFilter, MutationOutcome, ObjectRelationFilter, OperationContext, Page, PageOptions,
    ReadOptions, ReverseTupleFilter, StorageError, StoredTuple, TupleReadFilter, TupleReader,
    TupleStream, TupleWriteOptions, TupleWriter, UsersetTupleFilter,
};
use thiserror::Error;

use crate::{InvalidationControllerHandle, InvalidationWatermark, metrics::CacheMetrics};

const MAXIMUM_TUPLE_CACHE_TTL: Duration = Duration::from_hours(24);
const MAXIMUM_CACHED_RESULTS: usize = 100_000;

/// Validated bounded tuple-cache policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TupleCacheConfig {
    maximum_weight: NonZeroU64,
    maximum_results: usize,
    ttl: Duration,
}

impl TupleCacheConfig {
    /// Creates a finite tuple-cache policy.
    ///
    /// Weight is approximately the number of owned tuple rows plus cache-entry
    /// overhead. Result sets larger than `maximum_results` are returned without
    /// insertion.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero/oversized result ceiling or invalid TTL.
    pub fn new(
        maximum_weight: NonZeroU64,
        maximum_results: usize,
        ttl: Duration,
    ) -> Result<Self, TupleCacheConfigError> {
        if !(1..=MAXIMUM_CACHED_RESULTS).contains(&maximum_results) {
            return Err(TupleCacheConfigError::MaximumResults);
        }
        if ttl.is_zero() || ttl > MAXIMUM_TUPLE_CACHE_TTL {
            return Err(TupleCacheConfigError::Ttl);
        }
        Ok(Self {
            maximum_weight,
            maximum_results,
            ttl,
        })
    }
}

/// Invalid tuple-cache configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TupleCacheConfigError {
    /// The maximum cached result count is zero or above 100,000.
    #[error("tuple cache result limit must be between 1 and 100000")]
    MaximumResults,
    /// The entry TTL is zero or longer than 24 hours.
    #[error("tuple cache TTL must be between one nanosecond and 24 hours")]
    Ttl,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct TupleReadKey {
    fingerprint: Fingerprint,
}

impl fmt::Debug for TupleReadKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TupleReadKey")
            .field("fingerprint", &self.fingerprint)
            .finish()
    }
}

#[derive(Clone, Debug)]
enum TupleCacheValue {
    Page(Page<StoredTuple>),
    Exact(StoredTuple),
    Tuples(Arc<[RelationshipTuple]>),
    Exists(bool),
    Count(u64),
}

impl TupleCacheValue {
    fn weight(&self) -> u32 {
        let rows = match self {
            Self::Page(page) => page.items().len(),
            Self::Tuples(tuples) => tuples.len(),
            Self::Exact(_) | Self::Exists(_) | Self::Count(_) => 1,
        };
        u32::try_from(rows.saturating_add(1)).unwrap_or(u32::MAX)
    }
}

#[derive(Clone, Debug)]
struct TupleCacheEntry {
    value: TupleCacheValue,
    watermark: u64,
}

/// Tuple storage decorated with finite mutable read caches and local invalidation.
///
/// Higher-consistency operations always call the authoritative reader with the
/// original context and never read or populate this cache. Successful local
/// writes advance the shared watermark after the atomic storage mutation.
#[derive(Clone)]
#[non_exhaustive]
pub struct CachedTupleStorage {
    reader: Arc<dyn TupleReader>,
    writer: Arc<dyn TupleWriter>,
    entries: Cache<TupleReadKey, Arc<TupleCacheEntry>>,
    invalidation: InvalidationWatermark,
    maximum_results: usize,
    controller: Option<InvalidationControllerHandle>,
    metrics: CacheMetrics,
}

impl CachedTupleStorage {
    /// Creates a consistency-aware tuple cache around storage capabilities.
    #[must_use]
    pub fn new(
        reader: Arc<dyn TupleReader>,
        writer: Arc<dyn TupleWriter>,
        invalidation: InvalidationWatermark,
        config: TupleCacheConfig,
    ) -> Self {
        let entries = Cache::builder()
            .max_capacity(config.maximum_weight.get())
            .time_to_live(config.ttl)
            .weigher(|_key: &TupleReadKey, entry: &Arc<TupleCacheEntry>| entry.value.weight())
            .build();
        Self {
            reader,
            writer,
            entries,
            invalidation,
            maximum_results: config.maximum_results,
            controller: None,
            metrics: CacheMetrics::new(),
        }
    }

    /// Creates tuple storage that registers active stores with the changelog controller.
    #[must_use]
    pub fn with_controller(
        reader: Arc<dyn TupleReader>,
        writer: Arc<dyn TupleWriter>,
        invalidation: InvalidationWatermark,
        config: TupleCacheConfig,
        controller: InvalidationControllerHandle,
    ) -> Self {
        let mut storage = Self::new(reader, writer, invalidation, config);
        storage.controller = Some(controller);
        storage
    }

    fn track(&self, store_id: StoreId) {
        if let Some(controller) = &self.controller {
            controller.track(store_id);
        }
    }

    async fn cached(&self, store_id: StoreId, key: &TupleReadKey) -> Option<TupleCacheValue> {
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
                        .is_none_or(|controller| controller.permits_caching(store_id)) =>
            {
                self.metrics.record("tuple", "hit");
                Some(entry.value.clone())
            }
            Some(_) => {
                self.metrics.record("tuple", "invalidated");
                None
            }
            None => {
                self.metrics.record("tuple", "miss");
                None
            }
        }
    }

    async fn insert_if_unchanged(
        &self,
        started_at: u64,
        store_id: StoreId,
        key: TupleReadKey,
        value: TupleCacheValue,
    ) {
        if self.invalidation.current() != started_at
            || self
                .controller
                .as_ref()
                .is_some_and(|controller| !controller.permits_caching(store_id))
        {
            return;
        }
        self.entries
            .insert(
                key,
                Arc::new(TupleCacheEntry {
                    value,
                    watermark: started_at,
                }),
            )
            .await;
    }

    fn eligible(&self, context: &OperationContext, store_id: StoreId) -> bool {
        context.consistency() == ConsistencyPreference::MinimizeLatency
            && self
                .controller
                .as_ref()
                .is_none_or(|controller| controller.permits_caching(store_id))
    }

    async fn lookup(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleReadKey,
    ) -> Option<TupleCacheValue> {
        if !self.eligible(context, store_id) {
            let result = if context.consistency() == ConsistencyPreference::HigherConsistency {
                "bypass_consistency"
            } else {
                "bypass_controller"
            };
            self.metrics.record("tuple", result);
            return None;
        }
        self.cached(store_id, key).await
    }

    async fn read_stream(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: TupleReadKey,
        load: impl std::future::Future<Output = Result<TupleStream, StorageError>>,
    ) -> Result<TupleStream, StorageError> {
        context.check()?;
        if let Some(TupleCacheValue::Tuples(tuples)) = self.lookup(context, store_id, &key).await {
            context.check()?;
            return Ok(TupleStream::from_tuples(tuples.to_vec()));
        }
        let started_at = self.invalidation.current();
        let mut stream = load.await?;
        let mut results = Vec::with_capacity(stream.remaining());
        let mut tuples = Vec::with_capacity(stream.remaining());
        while let Some(item) = stream.next_item() {
            match item {
                Ok(tuple) => {
                    tuples.push(tuple.clone());
                    results.push(Ok(tuple));
                }
                Err(error) => {
                    results.push(Err(error));
                    return Ok(TupleStream::from_results(results));
                }
            }
        }
        if self.eligible(context, store_id) && tuples.len() <= self.maximum_results {
            self.insert_if_unchanged(
                started_at,
                store_id,
                key,
                TupleCacheValue::Tuples(Arc::from(tuples.clone())),
            )
            .await;
        }
        Ok(TupleStream::from_tuples(tuples))
    }
}

impl fmt::Debug for CachedTupleStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedTupleStorage")
            .field("reader", &"dyn TupleReader")
            .field("writer", &"dyn TupleWriter")
            .field("entries", &self.entries.entry_count())
            .field("watermark", &self.invalidation.current())
            .field("maximum_results", &self.maximum_results)
            .field("controller", &self.controller)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TupleReader for CachedTupleStorage {
    async fn read_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &TupleReadFilter,
        options: &PageOptions,
    ) -> Result<Page<StoredTuple>, StorageError> {
        self.track(store_id);
        context.check()?;
        let key = page_key(store_id, filter, options);
        if let Some(TupleCacheValue::Page(page)) = self.lookup(context, store_id, &key).await {
            context.check()?;
            return Ok(page);
        }
        let started_at = self.invalidation.current();
        let page = self
            .reader
            .read_tuples(context, store_id, filter, options)
            .await?;
        if self.eligible(context, store_id) && page.items().len() <= self.maximum_results {
            self.insert_if_unchanged(
                started_at,
                store_id,
                key,
                TupleCacheValue::Page(page.clone()),
            )
            .await;
        }
        Ok(page)
    }

    async fn read_exact_tuple(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<StoredTuple, StorageError> {
        self.track(store_id);
        context.check()?;
        let cache_key = exact_key("exact", store_id, key);
        if let Some(TupleCacheValue::Exact(tuple)) =
            self.lookup(context, store_id, &cache_key).await
        {
            context.check()?;
            return Ok(tuple);
        }
        let started_at = self.invalidation.current();
        let tuple = self.reader.read_exact_tuple(context, store_id, key).await?;
        if self.eligible(context, store_id) {
            self.insert_if_unchanged(
                started_at,
                store_id,
                cache_key,
                TupleCacheValue::Exact(tuple.clone()),
            )
            .await;
        }
        Ok(tuple)
    }

    async fn read_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        self.track(store_id);
        self.read_stream(
            context,
            store_id,
            object_relation_key("object_relation", store_id, filter, options),
            self.reader
                .read_object_relation(context, store_id, filter, options),
        )
        .await
    }

    async fn read_userset_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &UsersetTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        self.track(store_id);
        self.read_stream(
            context,
            store_id,
            userset_key(store_id, filter, options),
            self.reader
                .read_userset_tuples(context, store_id, filter, options),
        )
        .await
    }

    async fn read_reverse_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ReverseTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        self.track(store_id);
        self.read_stream(
            context,
            store_id,
            reverse_key(store_id, filter, options),
            self.reader
                .read_reverse_tuples(context, store_id, filter, options),
        )
        .await
    }

    async fn tuple_exists(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<bool, StorageError> {
        self.track(store_id);
        context.check()?;
        let cache_key = exact_key("exists", store_id, key);
        if let Some(TupleCacheValue::Exists(exists)) =
            self.lookup(context, store_id, &cache_key).await
        {
            context.check()?;
            return Ok(exists);
        }
        let started_at = self.invalidation.current();
        let exists = self.reader.tuple_exists(context, store_id, key).await?;
        if self.eligible(context, store_id) {
            self.insert_if_unchanged(
                started_at,
                store_id,
                cache_key,
                TupleCacheValue::Exists(exists),
            )
            .await;
        }
        Ok(exists)
    }

    async fn count_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError> {
        self.track(store_id);
        context.check()?;
        let cache_key = object_relation_key(
            "count_object_relation",
            store_id,
            filter,
            ReadOptions::from_limit(openfga_domain::Limit::MIN),
        );
        if let Some(TupleCacheValue::Count(count)) =
            self.lookup(context, store_id, &cache_key).await
        {
            context.check()?;
            return Ok(count);
        }
        let started_at = self.invalidation.current();
        let count = self
            .reader
            .count_object_relation(context, store_id, filter)
            .await?;
        if self.eligible(context, store_id) {
            self.insert_if_unchanged(
                started_at,
                store_id,
                cache_key,
                TupleCacheValue::Count(count),
            )
            .await;
        }
        Ok(count)
    }
}

#[async_trait]
impl TupleWriter for CachedTupleStorage {
    async fn write_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, StorageError> {
        self.track(store_id);
        let outcome = self
            .writer
            .write_tuples(context, store_id, deletes, writes, options)
            .await?;
        if !outcome.change_ids().is_empty() {
            let _advanced = self.invalidation.advance();
        }
        Ok(outcome)
    }
}

fn key_builder(operation: &str, store_id: StoreId) -> FingerprintBuilder {
    let mut builder = FingerprintBuilder::new("openfga.tuple-read-key.v1");
    builder.write_str(operation);
    builder.write_str(&store_id.to_string());
    builder
}

fn exact_key(operation: &str, store_id: StoreId, key: &TupleKey) -> TupleReadKey {
    let mut builder = key_builder(operation, store_id);
    builder.write_bytes(key.fingerprint().as_bytes());
    TupleReadKey {
        fingerprint: builder.finish(),
    }
}

fn page_key(store_id: StoreId, filter: &TupleReadFilter, options: &PageOptions) -> TupleReadKey {
    let mut builder = key_builder("page", store_id);
    write_optional(
        &mut builder,
        filter.object_type().map(openfga_domain::TypeName::as_str),
    );
    write_optional(
        &mut builder,
        filter.object_id().map(openfga_domain::ObjectId::as_str),
    );
    write_optional(
        &mut builder,
        filter.relation().map(openfga_domain::RelationName::as_str),
    );
    write_optional(
        &mut builder,
        filter.subject().map(ToString::to_string).as_deref(),
    );
    builder.write_u64(u64::try_from(options.maximum_results()).unwrap_or(u64::MAX));
    match options.after() {
        Some(cursor) => {
            builder.write_tag(1);
            builder.write_bytes(cursor.as_bytes());
        }
        None => builder.write_tag(0),
    }
    TupleReadKey {
        fingerprint: builder.finish(),
    }
}

fn object_relation_key(
    operation: &str,
    store_id: StoreId,
    filter: &ObjectRelationFilter,
    options: ReadOptions,
) -> TupleReadKey {
    let mut builder = key_builder(operation, store_id);
    builder.write_str(&filter.object().to_string());
    builder.write_str(filter.relation().as_str());
    write_subjects(&mut builder, filter.subjects());
    write_conditions(&mut builder, filter.conditions());
    builder.write_u64(u64::try_from(options.maximum_results()).unwrap_or(u64::MAX));
    TupleReadKey {
        fingerprint: builder.finish(),
    }
}

fn userset_key(
    store_id: StoreId,
    filter: &UsersetTupleFilter,
    options: ReadOptions,
) -> TupleReadKey {
    let mut builder = key_builder("userset", store_id);
    builder.write_str(&filter.object().to_string());
    builder.write_str(filter.relation().as_str());
    builder.write_u64(u64::try_from(filter.allowed().len()).unwrap_or(u64::MAX));
    for allowed in filter.allowed() {
        builder.write_str(allowed.subject_type().as_str());
        builder.write_str(allowed.relation().as_str());
    }
    write_conditions(&mut builder, filter.conditions());
    builder.write_u64(u64::try_from(options.maximum_results()).unwrap_or(u64::MAX));
    TupleReadKey {
        fingerprint: builder.finish(),
    }
}

fn reverse_key(
    store_id: StoreId,
    filter: &ReverseTupleFilter,
    options: ReadOptions,
) -> TupleReadKey {
    let mut builder = key_builder("reverse", store_id);
    builder.write_str(filter.object_type().as_str());
    builder.write_str(filter.relation().as_str());
    write_subjects(&mut builder, filter.subjects());
    builder.write_u64(u64::try_from(filter.object_ids().len()).unwrap_or(u64::MAX));
    for object_id in filter.object_ids() {
        builder.write_str(object_id.as_str());
    }
    write_conditions(&mut builder, filter.conditions());
    builder.write_u64(u64::try_from(options.maximum_results()).unwrap_or(u64::MAX));
    TupleReadKey {
        fingerprint: builder.finish(),
    }
}

fn write_subjects(
    builder: &mut FingerprintBuilder,
    subjects: &std::collections::BTreeSet<SubjectRef>,
) {
    builder.write_u64(u64::try_from(subjects.len()).unwrap_or(u64::MAX));
    for subject in subjects {
        builder.write_str(&subject.to_string());
    }
}

fn write_conditions(builder: &mut FingerprintBuilder, conditions: &ConditionFilter) {
    if conditions.is_any() {
        builder.write_tag(0);
    } else if conditions.accepts_unconditional() {
        builder.write_tag(1);
    } else if let Some(names) = conditions.names() {
        builder.write_tag(2);
        builder.write_u64(u64::try_from(names.len()).unwrap_or(u64::MAX));
        for name in names {
            builder.write_str(name.as_str());
        }
    }
}

fn write_optional(builder: &mut FingerprintBuilder, value: Option<&str>) {
    match value {
        Some(value) => {
            builder.write_tag(1);
            builder.write_str(value);
        }
        None => builder.write_tag(0),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        sync::Arc,
        time::{Duration, Instant},
    };

    use openfga_domain::{
        ConsistencyPreference, Deadline, RelationshipTuple, RequestTimeout, StoreId, TupleKey,
    };
    use openfga_storage::{
        OperationContext, StorageCancellationToken, StorageErrorKind, TupleReader,
        TupleWriteOptions, TupleWriter,
    };
    use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};

    use super::{CachedTupleStorage, TupleCacheConfig};
    use crate::InvalidationWatermark;

    #[tokio::test]
    async fn test_should_bypass_tuple_cache_for_higher_consistency()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let watermark = InvalidationWatermark::new();
        let cached = cached_storage(Arc::clone(&storage), watermark.clone())?;
        let store_id = store_id()?;
        let key = tuple_key()?;
        storage
            .write_tuples(
                &context(ConsistencyPreference::HigherConsistency)?,
                store_id,
                Vec::new(),
                vec![RelationshipTuple::unconditional(key.clone())],
                TupleWriteOptions::default(),
            )
            .await?;

        cached
            .read_exact_tuple(
                &context(ConsistencyPreference::MinimizeLatency)?,
                store_id,
                &key,
            )
            .await?;
        storage
            .write_tuples(
                &context(ConsistencyPreference::HigherConsistency)?,
                store_id,
                vec![key.clone()],
                Vec::new(),
                TupleWriteOptions::default(),
            )
            .await?;
        assert!(
            cached
                .read_exact_tuple(
                    &context(ConsistencyPreference::MinimizeLatency)?,
                    store_id,
                    &key,
                )
                .await
                .is_ok()
        );
        let error = cached
            .read_exact_tuple(
                &context(ConsistencyPreference::HigherConsistency)?,
                store_id,
                &key,
            )
            .await
            .err()
            .ok_or("higher-consistency read used stale cache")?;
        assert_eq!(error.kind(), StorageErrorKind::NotFound);

        let _advanced = watermark.advance();
        let error = cached
            .read_exact_tuple(
                &context(ConsistencyPreference::MinimizeLatency)?,
                store_id,
                &key,
            )
            .await
            .err()
            .ok_or("invalidated tuple cache entry remained eligible")?;
        assert_eq!(error.kind(), StorageErrorKind::NotFound);

        drop(cached);
        stop_storage(storage).await
    }

    #[tokio::test]
    async fn test_should_invalidate_after_successful_local_tuple_write()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?);
        let watermark = InvalidationWatermark::new();
        let cached = cached_storage(Arc::clone(&storage), watermark.clone())?;
        let store_id = store_id()?;
        let key = tuple_key()?;
        let operation = context(ConsistencyPreference::MinimizeLatency)?;
        cached
            .write_tuples(
                &operation,
                store_id,
                Vec::new(),
                vec![RelationshipTuple::unconditional(key.clone())],
                TupleWriteOptions::default(),
            )
            .await?;
        cached.read_exact_tuple(&operation, store_id, &key).await?;
        let populated_at = watermark.current();
        cached
            .write_tuples(
                &operation,
                store_id,
                vec![key.clone()],
                Vec::new(),
                TupleWriteOptions::default(),
            )
            .await?;
        assert!(watermark.current() > populated_at);
        let error = cached
            .read_exact_tuple(&operation, store_id, &key)
            .await
            .err()
            .ok_or("local write did not invalidate tuple cache")?;
        assert_eq!(error.kind(), StorageErrorKind::NotFound);

        drop(cached);
        stop_storage(storage).await
    }

    fn cached_storage(
        storage: Arc<MemoryStorage>,
        watermark: InvalidationWatermark,
    ) -> Result<CachedTupleStorage, Box<dyn Error + Send + Sync>> {
        let config = TupleCacheConfig::new(
            std::num::NonZeroU64::new(100).ok_or("invalid test capacity")?,
            100,
            Duration::from_mins(1),
        )?;
        Ok(CachedTupleStorage::new(
            storage.clone(),
            storage,
            watermark,
            config,
        ))
    }

    fn context(
        consistency: ConsistencyPreference,
    ) -> Result<OperationContext, Box<dyn Error + Send + Sync>> {
        Ok(OperationContext::new(
            consistency,
            Deadline::from_timeout(Instant::now(), RequestTimeout::new(Duration::from_secs(5))?)?,
            StorageCancellationToken::new(),
        ))
    }

    fn store_id() -> Result<StoreId, Box<dyn Error + Send + Sync>> {
        Ok("01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?)
    }

    fn tuple_key() -> Result<TupleKey, Box<dyn Error + Send + Sync>> {
        Ok("document:one#viewer@user:anne".parse()?)
    }

    async fn stop_storage(storage: Arc<MemoryStorage>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut storage =
            Arc::try_unwrap(storage).map_err(|_| "memory storage references remain")?;
        storage.stop().await?;
        Ok(())
    }
}
