//! Actor-exclusive maps, indexes, and atomic state transitions.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use openfga_domain::{
    AuthorizationModelId, ChangeId, InputLimits, ObjectRef, RelationName, RelationshipTuple,
    StoreId, SubjectRef, TupleKey, TypeName,
};
use openfga_storage::{
    Assertion, ChangeFilter, ChangeOperation, ConditionFilter, MutationOutcome,
    ObjectRelationFilter, OperationContext, Page, PageOptions, ReadOptions, ReverseTupleFilter,
    StorageCursor, StorageError, StorageErrorKind, StoreFilter, StoreName, StoreRecord,
    StoredAuthorizationModel, StoredTuple, TupleChange, TupleReadFilter, TupleWriteOptions,
    UsersetRestrictionFilter, UsersetTupleFilter, WriteConflictPolicy,
};
use ulid::Ulid;

use crate::{
    StorageClock,
    config::{MutationFaultInjector, MutationFaultStage},
};

type ForwardKey = (StoreId, ObjectRef, RelationName);
type ReverseKey = (StoreId, SubjectRef, TypeName, RelationName);
type ModelKey = (StoreId, AuthorizationModelId);

pub(crate) struct MemoryState {
    limits: InputLimits,
    clock: Arc<dyn StorageClock>,
    faults: Arc<dyn MutationFaultInjector>,
    stores: BTreeMap<StoreId, StoreRecord>,
    tuples: BTreeMap<(StoreId, TupleKey), StoredTuple>,
    forward: BTreeMap<ForwardKey, BTreeSet<TupleKey>>,
    reverse: BTreeMap<ReverseKey, BTreeSet<TupleKey>>,
    usersets: BTreeMap<ForwardKey, BTreeSet<TupleKey>>,
    models: BTreeMap<ModelKey, Arc<StoredAuthorizationModel>>,
    model_ids: BTreeMap<StoreId, BTreeSet<AuthorizationModelId>>,
    assertions: BTreeMap<ModelKey, Arc<[Assertion]>>,
    changes: BTreeMap<(StoreId, ChangeId), TupleChange>,
    change_ids: ChangeIdAllocator,
}

impl MemoryState {
    pub(crate) fn new(
        limits: InputLimits,
        clock: Arc<dyn StorageClock>,
        faults: Arc<dyn MutationFaultInjector>,
    ) -> Self {
        Self {
            limits,
            clock,
            faults,
            stores: BTreeMap::new(),
            tuples: BTreeMap::new(),
            forward: BTreeMap::new(),
            reverse: BTreeMap::new(),
            usersets: BTreeMap::new(),
            models: BTreeMap::new(),
            model_ids: BTreeMap::new(),
            assertions: BTreeMap::new(),
            changes: BTreeMap::new(),
            change_ids: ChangeIdAllocator::default(),
        }
    }

    pub(crate) fn read_exact(
        &self,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<StoredTuple, StorageError> {
        self.tuples
            .get(&(store_id, key.clone()))
            .cloned()
            .ok_or_else(not_found)
    }

    pub(crate) fn read_tuples(
        &self,
        store_id: StoreId,
        filter: &TupleReadFilter,
        options: &PageOptions,
    ) -> Result<Page<StoredTuple>, StorageError> {
        self.require_store(store_id)?;
        let after = options.after().map(parse_tuple_cursor).transpose()?;
        let mut candidates = self
            .tuples
            .iter()
            .filter(|((store, key), _)| {
                *store == store_id
                    && after.as_ref().is_none_or(|cursor| key > cursor)
                    && filter.matches(key)
            })
            .map(|((_, key), stored)| (key.clone(), stored.clone()))
            .take(options.maximum_results().saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = candidates.len() > options.maximum_results();
        if has_more {
            candidates.pop();
        }
        let continuation = if has_more {
            candidates
                .last()
                .map(|(key, _)| StorageCursor::new(key.to_string().into_bytes()))
                .transpose()?
        } else {
            None
        };
        Ok(Page::new(
            candidates.into_iter().map(|(_, stored)| stored).collect(),
            continuation,
        ))
    }

    pub(crate) fn read_object_relation(
        &self,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
        options: ReadOptions,
    ) -> Result<Vec<RelationshipTuple>, StorageError> {
        let key = (store_id, filter.object().clone(), filter.relation().clone());
        let keys = self.forward.get(&key);
        self.collect_tuples(store_id, keys.into_iter().flatten(), options, |tuple| {
            (filter.subjects().is_empty() || filter.subjects().contains(tuple.key().subject()))
                && condition_matches(tuple, filter.conditions())
        })
    }

    pub(crate) fn read_userset(
        &self,
        store_id: StoreId,
        filter: &UsersetTupleFilter,
        options: ReadOptions,
    ) -> Result<Vec<RelationshipTuple>, StorageError> {
        let key = (store_id, filter.object().clone(), filter.relation().clone());
        let keys = self.usersets.get(&key);
        self.collect_tuples(store_id, keys.into_iter().flatten(), options, |tuple| {
            userset_matches(tuple, filter.allowed())
                && condition_matches(tuple, filter.conditions())
        })
    }

    pub(crate) fn read_reverse(
        &self,
        store_id: StoreId,
        filter: &ReverseTupleFilter,
        options: ReadOptions,
    ) -> Result<Vec<RelationshipTuple>, StorageError> {
        let mut keys = BTreeSet::new();
        for subject in filter.subjects() {
            let index = (
                store_id,
                subject.clone(),
                filter.object_type().clone(),
                filter.relation().clone(),
            );
            if let Some(indexed) = self.reverse.get(&index) {
                keys.extend(indexed.iter().cloned());
            }
        }
        self.collect_tuples(store_id, keys.iter(), options, |tuple| {
            (filter.object_ids().is_empty()
                || filter
                    .object_ids()
                    .contains(tuple.key().object().object_id()))
                && condition_matches(tuple, filter.conditions())
        })
    }

    pub(crate) fn tuple_exists(&self, store_id: StoreId, key: &TupleKey) -> bool {
        self.tuples.contains_key(&(store_id, key.clone()))
    }

    pub(crate) fn count_object_relation(
        &self,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError> {
        let key = (store_id, filter.object().clone(), filter.relation().clone());
        let mut count = 0_usize;
        for tuple_key in self.forward.get(&key).into_iter().flatten() {
            let stored = self
                .tuples
                .get(&(store_id, tuple_key.clone()))
                .ok_or_else(|| {
                    StorageError::new(StorageErrorKind::Integrity, "tuple_index_dangling")
                })?;
            let tuple = stored.tuple();
            let matches = (filter.subjects().is_empty()
                || filter.subjects().contains(tuple.key().subject()))
                && condition_matches(tuple, filter.conditions());
            if matches {
                count = count.saturating_add(1);
            }
        }
        u64::try_from(count).map_err(|error| {
            StorageError::with_source(StorageErrorKind::Internal, "tuple_count_overflow", error)
        })
    }

    pub(crate) fn write_tuples(
        &mut self,
        context: &OperationContext,
        store_id: StoreId,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, StorageError> {
        self.require_store(store_id)?;
        let total = deletes.len().saturating_add(writes.len());
        if total > self.limits.write_tuples() {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "tuple_mutation_item_limit",
            ));
        }

        let delete_keys = unique_delete_keys(deletes)?;
        let write_tuples = unique_write_tuples(writes)?;
        if delete_keys.iter().any(|key| write_tuples.contains_key(key)) {
            return Err(StorageError::new(
                StorageErrorKind::Conflict,
                "tuple_in_delete_and_write",
            ));
        }
        self.faults.check(MutationFaultStage::Validated)?;
        context.check()?;

        let mut prepared_deletes = Vec::new();
        for key in delete_keys {
            match self.tuples.get(&(store_id, key.clone())) {
                Some(stored) => prepared_deletes.push(stored.tuple().clone()),
                None if options.on_missing_delete() == WriteConflictPolicy::Ignore => {}
                None => {
                    return Err(StorageError::new(
                        StorageErrorKind::Conflict,
                        "missing_tuple_delete",
                    ));
                }
            }
        }
        self.faults.check(MutationFaultStage::DeletesPrepared)?;
        context.check()?;

        let mut prepared_writes = Vec::new();
        for (key, tuple) in write_tuples {
            if self.tuples.contains_key(&(store_id, key)) {
                if options.on_duplicate_write() == WriteConflictPolicy::Error {
                    return Err(StorageError::new(
                        StorageErrorKind::Conflict,
                        "duplicate_tuple_write",
                    ));
                }
            } else {
                prepared_writes.push(tuple);
            }
        }
        self.faults.check(MutationFaultStage::WritesPrepared)?;
        context.check()?;

        if prepared_deletes.is_empty() && prepared_writes.is_empty() {
            return Ok(MutationOutcome::new(Vec::new()));
        }
        let timestamp = self.clock.now()?;
        let mut allocator = self.change_ids;
        let mut changes =
            Vec::with_capacity(prepared_deletes.len().saturating_add(prepared_writes.len()));
        for tuple in &prepared_deletes {
            changes.push(TupleChange::new(
                allocator.next(timestamp)?,
                store_id,
                ChangeOperation::Delete,
                tuple.clone(),
                timestamp,
            ));
        }
        let stored_writes = prepared_writes
            .into_iter()
            .map(|tuple| {
                let id = allocator.next(timestamp)?;
                let change = TupleChange::new(
                    id,
                    store_id,
                    ChangeOperation::Write,
                    tuple.clone(),
                    timestamp,
                );
                Ok((StoredTuple::new(tuple, timestamp), change))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        changes.extend(stored_writes.iter().map(|(_, change)| change.clone()));
        self.faults.check(MutationFaultStage::ChangesPrepared)?;
        context.check()?;

        for tuple in &prepared_deletes {
            self.remove_tuple(store_id, tuple.key());
        }
        for (stored, _) in stored_writes {
            self.insert_tuple(store_id, stored);
        }
        let change_ids = changes.iter().map(TupleChange::id).collect();
        for change in changes {
            self.changes.insert((store_id, change.id()), change);
        }
        self.change_ids = allocator;
        Ok(MutationOutcome::new(change_ids))
    }

    pub(crate) fn read_model(
        &self,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        self.models
            .get(&(store_id, model_id))
            .cloned()
            .ok_or_else(not_found)
    }

    pub(crate) fn read_latest_model(
        &self,
        store_id: StoreId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        let model_id = self
            .model_ids
            .get(&store_id)
            .and_then(|ids| ids.last())
            .copied()
            .ok_or_else(not_found)?;
        self.models
            .get(&(store_id, model_id))
            .cloned()
            .ok_or_else(|| StorageError::new(StorageErrorKind::Integrity, "model_index_dangling"))
    }

    pub(crate) fn list_models(
        &self,
        store_id: StoreId,
        options: &PageOptions,
    ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError> {
        self.require_store(store_id)?;
        let after = options.after().map(parse_model_cursor).transpose()?;
        let ids = self
            .model_ids
            .get(&store_id)
            .into_iter()
            .flatten()
            .rev()
            .copied()
            .filter(|id| after.is_none_or(|cursor| *id < cursor));
        let (ids, continuation) = bounded_page(ids, options.maximum_results());
        let models = ids
            .into_iter()
            .map(|id| {
                self.models.get(&(store_id, id)).cloned().ok_or_else(|| {
                    StorageError::new(StorageErrorKind::Integrity, "model_index_dangling")
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let continuation = continuation
            .map(|id| StorageCursor::new(id.to_string().into_bytes()))
            .transpose()?;
        Ok(Page::new(models, continuation))
    }

    pub(crate) fn write_model(
        &mut self,
        model: Arc<StoredAuthorizationModel>,
    ) -> Result<(), StorageError> {
        let store_id = *model.store_id();
        let model_id = *model.model_id();
        self.require_store(store_id)?;
        if let Some(existing) = self.models.get(&(store_id, model_id)) {
            let kind = if existing.compiled().fingerprint() == model.compiled().fingerprint() {
                StorageErrorKind::AlreadyExists
            } else {
                StorageErrorKind::Integrity
            };
            return Err(StorageError::new(kind, "authorization_model_id_collision"));
        }
        self.models.insert((store_id, model_id), model);
        self.model_ids.entry(store_id).or_default().insert(model_id);
        Ok(())
    }

    pub(crate) fn read_store(&self, store_id: StoreId) -> Result<StoreRecord, StorageError> {
        self.stores.get(&store_id).cloned().ok_or_else(not_found)
    }

    pub(crate) fn list_stores(
        &self,
        filter: &StoreFilter,
        options: &PageOptions,
    ) -> Result<Page<StoreRecord>, StorageError> {
        let after = options.after().map(parse_store_cursor).transpose()?;
        let ids = self
            .stores
            .keys()
            .copied()
            .filter(|id| after.is_none_or(|cursor| *id > cursor))
            .filter(|id| {
                filter.name().is_none_or(|name| {
                    self.stores
                        .get(id)
                        .is_some_and(|store| store.name() == name)
                })
            });
        let (ids, continuation) = bounded_page(ids, options.maximum_results());
        let stores = ids
            .into_iter()
            .map(|id| {
                self.stores.get(&id).cloned().ok_or_else(|| {
                    StorageError::new(StorageErrorKind::Integrity, "store_index_dangling")
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let continuation = continuation
            .map(|id| StorageCursor::new(id.to_string().into_bytes()))
            .transpose()?;
        Ok(Page::new(stores, continuation))
    }

    pub(crate) fn create_store(
        &mut self,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError> {
        if self.stores.contains_key(&store_id) {
            return Err(StorageError::new(
                StorageErrorKind::AlreadyExists,
                "store_id_collision",
            ));
        }
        let record = StoreRecord::new(store_id, name, self.clock.now()?);
        self.stores.insert(store_id, record.clone());
        Ok(record)
    }

    pub(crate) fn rename_store(
        &mut self,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError> {
        let timestamp = self.clock.now()?;
        let existing = self.stores.get(&store_id).ok_or_else(not_found)?;
        let renamed = existing.renamed(name, timestamp);
        self.stores.insert(store_id, renamed.clone());
        Ok(renamed)
    }

    pub(crate) fn delete_store(&mut self, store_id: StoreId) -> Result<(), StorageError> {
        if self.stores.remove(&store_id).is_none() {
            return Err(not_found());
        }
        self.tuples.retain(|(store, _), _| *store != store_id);
        self.forward.retain(|(store, _, _), _| *store != store_id);
        self.reverse
            .retain(|(store, _, _, _), _| *store != store_id);
        self.usersets.retain(|(store, _, _), _| *store != store_id);
        self.models.retain(|(store, _), _| *store != store_id);
        self.model_ids.remove(&store_id);
        self.assertions.retain(|(store, _), _| *store != store_id);
        self.changes.retain(|(store, _), _| *store != store_id);
        Ok(())
    }

    pub(crate) fn read_assertions(
        &self,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<[Assertion]>, StorageError> {
        self.require_model(store_id, model_id)?;
        Ok(self
            .assertions
            .get(&(store_id, model_id))
            .cloned()
            .unwrap_or_else(|| Arc::from([])))
    }

    pub(crate) fn write_assertions(
        &mut self,
        store_id: StoreId,
        model_id: AuthorizationModelId,
        assertions: Vec<Assertion>,
    ) -> Result<(), StorageError> {
        self.require_model(store_id, model_id)?;
        if assertions.len() > self.limits.assertions() {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "assertion_item_limit",
            ));
        }
        self.assertions
            .insert((store_id, model_id), Arc::from(assertions));
        Ok(())
    }

    pub(crate) fn read_changes(
        &self,
        store_id: StoreId,
        filter: &ChangeFilter,
        options: &PageOptions,
    ) -> Result<Page<TupleChange>, StorageError> {
        self.require_store(store_id)?;
        let after = options.after().map(parse_change_cursor).transpose()?;
        let mut changes =
            self.changes
                .iter()
                .filter(|((store, id), _)| {
                    *store == store_id && after.is_none_or(|cursor| *id > cursor)
                })
                .map(|(_, change)| change)
                .filter(|change| {
                    filter.object_type().is_none_or(|expected| {
                        change.tuple().key().object().object_type() == expected
                    }) && filter
                        .start_time()
                        .is_none_or(|start| change.timestamp() >= start)
                })
                .take(options.maximum_results().saturating_add(1))
                .cloned()
                .collect::<Vec<_>>();
        let has_more = changes.len() > options.maximum_results();
        if has_more {
            changes.pop();
        }
        let continuation = if has_more {
            changes
                .last()
                .map(|change| StorageCursor::new(change.id().to_string().into_bytes()))
                .transpose()?
        } else {
            None
        };
        Ok(Page::new(changes, continuation))
    }

    fn collect_tuples<'a>(
        &self,
        store_id: StoreId,
        keys: impl Iterator<Item = &'a TupleKey>,
        options: ReadOptions,
        predicate: impl Fn(&RelationshipTuple) -> bool,
    ) -> Result<Vec<RelationshipTuple>, StorageError> {
        let maximum = options.maximum_results();
        let mut tuples = Vec::new();
        for key in keys {
            let stored = self.tuples.get(&(store_id, key.clone())).ok_or_else(|| {
                StorageError::new(StorageErrorKind::Integrity, "tuple_index_dangling")
            })?;
            if predicate(stored.tuple()) {
                if tuples.len() >= maximum {
                    return Err(StorageError::new(
                        StorageErrorKind::ResourceExhausted,
                        "tuple_snapshot_result_limit",
                    ));
                }
                tuples.push(stored.tuple().clone());
            }
        }
        Ok(tuples)
    }

    fn insert_tuple(&mut self, store_id: StoreId, stored: StoredTuple) {
        let key = stored.tuple().key().clone();
        let forward_key = (store_id, key.object().clone(), key.relation().clone());
        self.forward
            .entry(forward_key.clone())
            .or_default()
            .insert(key.clone());
        if matches!(key.subject(), SubjectRef::Userset(_)) {
            self.usersets
                .entry(forward_key)
                .or_default()
                .insert(key.clone());
        }
        let reverse_key = (
            store_id,
            key.subject().clone(),
            key.object().object_type().clone(),
            key.relation().clone(),
        );
        self.reverse
            .entry(reverse_key)
            .or_default()
            .insert(key.clone());
        self.tuples.insert((store_id, key), stored);
    }

    fn remove_tuple(&mut self, store_id: StoreId, key: &TupleKey) {
        self.tuples.remove(&(store_id, key.clone()));
        let forward_key = (store_id, key.object().clone(), key.relation().clone());
        remove_index_key(&mut self.forward, &forward_key, key);
        if matches!(key.subject(), SubjectRef::Userset(_)) {
            remove_index_key(&mut self.usersets, &forward_key, key);
        }
        let reverse_key = (
            store_id,
            key.subject().clone(),
            key.object().object_type().clone(),
            key.relation().clone(),
        );
        remove_index_key(&mut self.reverse, &reverse_key, key);
    }

    fn require_store(&self, store_id: StoreId) -> Result<(), StorageError> {
        self.stores
            .contains_key(&store_id)
            .then_some(())
            .ok_or_else(not_found)
    }

    fn require_model(
        &self,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<(), StorageError> {
        self.models
            .contains_key(&(store_id, model_id))
            .then_some(())
            .ok_or_else(not_found)
    }
}

impl std::fmt::Debug for MemoryState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryState")
            .field("stores", &self.stores.len())
            .field("tuples", &self.tuples.len())
            .field("models", &self.models.len())
            .field("assertions", &self.assertions.len())
            .field("changes", &self.changes.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ChangeIdAllocator {
    last_timestamp_ms: u64,
    last_random: u128,
    initialized: bool,
}

impl ChangeIdAllocator {
    fn next(&mut self, timestamp: SystemTime) -> Result<ChangeId, StorageError> {
        const MAXIMUM_ULID_TIMESTAMP: u64 = (1_u64 << 48) - 1;
        const MAXIMUM_ULID_RANDOM: u128 = (1_u128 << 80) - 1;

        let duration = timestamp.duration_since(UNIX_EPOCH).map_err(|error| {
            StorageError::with_source(StorageErrorKind::Internal, "clock_before_epoch", error)
        })?;
        let wall_ms = u64::try_from(duration.as_millis()).map_err(|error| {
            StorageError::with_source(StorageErrorKind::Internal, "clock_millis_overflow", error)
        })?;
        let mut next_ms = wall_ms;
        let next_random;
        if self.initialized && next_ms <= self.last_timestamp_ms {
            next_ms = self.last_timestamp_ms;
            if self.last_random == MAXIMUM_ULID_RANDOM {
                next_ms = next_ms.checked_add(1).ok_or_else(|| {
                    StorageError::new(StorageErrorKind::Internal, "change_id_timestamp_overflow")
                })?;
                next_random = 0;
            } else {
                next_random = self.last_random.saturating_add(1);
            }
        } else {
            next_random = 0;
        }
        if next_ms > MAXIMUM_ULID_TIMESTAMP {
            return Err(StorageError::new(
                StorageErrorKind::Internal,
                "change_id_timestamp_out_of_range",
            ));
        }
        let id = ChangeId::try_from(Ulid::from_parts(next_ms, next_random).to_string()).map_err(
            |error| {
                StorageError::with_source(StorageErrorKind::Internal, "change_id_encoding", error)
            },
        )?;
        self.last_timestamp_ms = next_ms;
        self.last_random = next_random;
        self.initialized = true;
        Ok(id)
    }
}

fn unique_delete_keys(deletes: Vec<TupleKey>) -> Result<BTreeSet<TupleKey>, StorageError> {
    let length = deletes.len();
    let keys: BTreeSet<_> = deletes.into_iter().collect();
    if keys.len() != length {
        return Err(StorageError::new(
            StorageErrorKind::Conflict,
            "duplicate_tuple_delete_input",
        ));
    }
    Ok(keys)
}

fn unique_write_tuples(
    writes: Vec<RelationshipTuple>,
) -> Result<BTreeMap<TupleKey, RelationshipTuple>, StorageError> {
    let length = writes.len();
    let tuples: BTreeMap<_, _> = writes
        .into_iter()
        .map(|tuple| (tuple.key().clone(), tuple))
        .collect();
    if tuples.len() != length {
        return Err(StorageError::new(
            StorageErrorKind::Conflict,
            "duplicate_tuple_write_input",
        ));
    }
    Ok(tuples)
}

fn condition_matches(tuple: &RelationshipTuple, filter: &ConditionFilter) -> bool {
    filter.matches(tuple.condition())
}

fn userset_matches(
    tuple: &RelationshipTuple,
    allowed: &BTreeSet<UsersetRestrictionFilter>,
) -> bool {
    let SubjectRef::Userset(userset) = tuple.key().subject() else {
        return false;
    };
    allowed.is_empty()
        || allowed.iter().any(|restriction| {
            restriction.subject_type() == userset.object().object_type()
                && restriction.relation() == userset.relation()
        })
}

fn remove_index_key<K: Ord>(
    index: &mut BTreeMap<K, BTreeSet<TupleKey>>,
    index_key: &K,
    tuple_key: &TupleKey,
) {
    let remove_entry = index.get_mut(index_key).is_some_and(|keys| {
        keys.remove(tuple_key);
        keys.is_empty()
    });
    if remove_entry {
        index.remove(index_key);
    }
}

fn bounded_page<T: Copy>(items: impl Iterator<Item = T>, maximum: usize) -> (Vec<T>, Option<T>) {
    let mut values: Vec<_> = items.take(maximum.saturating_add(1)).collect();
    let has_more = values.len() > maximum;
    if has_more {
        values.pop();
    }
    let continuation = if has_more {
        values.last().copied()
    } else {
        None
    };
    (values, continuation)
}

fn parse_store_cursor(cursor: &StorageCursor) -> Result<StoreId, StorageError> {
    parse_cursor(cursor, "store_cursor")
}

fn parse_model_cursor(cursor: &StorageCursor) -> Result<AuthorizationModelId, StorageError> {
    parse_cursor(cursor, "model_cursor")
}

fn parse_tuple_cursor(cursor: &StorageCursor) -> Result<TupleKey, StorageError> {
    parse_cursor(cursor, "tuple_cursor")
}

fn parse_change_cursor(cursor: &StorageCursor) -> Result<ChangeId, StorageError> {
    parse_cursor(cursor, "change_cursor")
}

fn parse_cursor<T: std::str::FromStr>(
    cursor: &StorageCursor,
    code: &'static str,
) -> Result<T, StorageError> {
    let text = std::str::from_utf8(cursor.as_bytes()).map_err(|error| {
        StorageError::with_source(StorageErrorKind::InvalidContinuation, code, error)
    })?;
    text.parse()
        .map_err(|_| StorageError::new(StorageErrorKind::InvalidContinuation, code))
}

const fn not_found() -> StorageError {
    StorageError::new(StorageErrorKind::NotFound, "record_not_found")
}
