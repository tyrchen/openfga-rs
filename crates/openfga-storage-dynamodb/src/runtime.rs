//! Supervised durable-generation garbage-collection actor.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, ReturnConsumedCapacity, TransactWriteItem, Update,
};
use openfga_domain::{AuthorizationModelId, StoreId};
use openfga_storage::{StorageError, StorageErrorKind};
use tokio::{sync::watch, task::JoinHandle, time::MissedTickBehavior};
use tracing::warn;
use ulid::Ulid;

use crate::{
    DynamoDbGarbageCollectionConfig,
    client::DynamoClient,
    item::{self, GENERATION, Item, KIND, STATE},
    key::{self, GARBAGE_COLLECTION_SHARDS},
};

/// Cloneable non-sensitive `DynamoDB` runtime diagnostics.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DynamoDbRuntimeDiagnostics {
    running: Arc<AtomicBool>,
    consecutive_failures: Arc<AtomicU64>,
    overdue_work_lag_millis: Arc<AtomicU64>,
}

impl DynamoDbRuntimeDiagnostics {
    /// Returns whether the cleanup actor is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Returns the number of consecutive failed cleanup passes.
    #[must_use]
    pub fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures.load(Ordering::Acquire)
    }

    /// Returns the largest overdue-work lag observed in the latest successful pass.
    #[must_use]
    pub fn overdue_work_lag_millis(&self) -> u64 {
        self.overdue_work_lag_millis.load(Ordering::Acquire)
    }
}

/// Explicit lifecycle handle for the `DynamoDB` cleanup actor.
pub struct DynamoDbRuntime {
    client: DynamoClient,
    config: DynamoDbGarbageCollectionConfig,
    shutdown: watch::Sender<bool>,
    join: Option<JoinHandle<()>>,
    diagnostics: DynamoDbRuntimeDiagnostics,
}

impl DynamoDbRuntime {
    pub(crate) fn start(
        client: DynamoClient,
        config: DynamoDbGarbageCollectionConfig,
    ) -> Result<Self, StorageError> {
        let running = Arc::new(AtomicBool::new(true));
        let diagnostics = DynamoDbRuntimeDiagnostics {
            running,
            consecutive_failures: Arc::new(AtomicU64::new(0)),
            overdue_work_lag_millis: Arc::new(AtomicU64::new(0)),
        };
        let (shutdown, receiver) = watch::channel(false);
        let join = spawn(&client, &config, receiver, &diagnostics)?;
        Ok(Self {
            client,
            config,
            shutdown,
            join: Some(join),
            diagnostics,
        })
    }

    /// Returns cloneable runtime diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> DynamoDbRuntimeDiagnostics {
        self.diagnostics.clone()
    }

    /// Requests shutdown and joins the cleanup actor.
    ///
    /// # Errors
    ///
    /// Returns timeout or internal when the actor cannot be joined cleanly.
    pub async fn stop(&mut self) -> Result<(), StorageError> {
        let Some(mut join) = self.join.take() else {
            return Ok(());
        };
        self.shutdown.send_replace(true);
        match tokio::time::timeout(self.config.shutdown_timeout, &mut join).await {
            Ok(Ok(())) => {
                self.diagnostics.running.store(false, Ordering::Release);
                Ok(())
            }
            Ok(Err(error)) => {
                self.diagnostics.running.store(false, Ordering::Release);
                Err(StorageError::with_source(
                    StorageErrorKind::Internal,
                    "dynamodb_gc_join_failed",
                    error,
                ))
            }
            Err(_) => {
                join.abort();
                let _ = join.await;
                self.diagnostics.running.store(false, Ordering::Release);
                Err(StorageError::new(
                    StorageErrorKind::Timeout,
                    "dynamodb_gc_shutdown_timeout",
                ))
            }
        }
    }

    /// Restarts the cleanup actor after joining the previous task.
    ///
    /// # Errors
    ///
    /// Returns a task-spawn failure when no Tokio runtime is active.
    pub async fn restart(&mut self) -> Result<(), StorageError> {
        let _previous = self.stop().await;
        let (shutdown, receiver) = watch::channel(false);
        self.diagnostics.running.store(true, Ordering::Release);
        self.join = Some(spawn(
            &self.client,
            &self.config,
            receiver,
            &self.diagnostics,
        )?);
        self.shutdown = shutdown;
        Ok(())
    }
}

impl fmt::Debug for DynamoDbRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamoDbRuntime")
            .field("config", &self.config)
            .field("diagnostics", &self.diagnostics)
            .finish_non_exhaustive()
    }
}

impl Drop for DynamoDbRuntime {
    fn drop(&mut self) {
        if self.join.is_some() {
            self.shutdown.send_replace(true);
        }
    }
}

fn spawn(
    client: &DynamoClient,
    config: &DynamoDbGarbageCollectionConfig,
    receiver: watch::Receiver<bool>,
    diagnostics: &DynamoDbRuntimeDiagnostics,
) -> Result<JoinHandle<()>, StorageError> {
    let handle = tokio::runtime::Handle::try_current().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Unavailable,
            "dynamodb_gc_requires_tokio_runtime",
            error,
        )
    })?;
    Ok(handle.spawn(run(
        client.clone(),
        config.clone(),
        receiver,
        diagnostics.clone(),
    )))
}

async fn run(
    client: DynamoClient,
    config: DynamoDbGarbageCollectionConfig,
    mut shutdown: watch::Receiver<bool>,
    diagnostics: DynamoDbRuntimeDiagnostics,
) {
    let _running = RunningGuard(Arc::clone(&diagnostics.running));
    let mut interval = tokio::time::interval(config.interval);
    let mut next_shard = 0_u8;
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() { break; }
            }
            _ = interval.tick() => {
                let first_shard = next_shard;
                next_shard = next_shard.wrapping_add(1) % GARBAGE_COLLECTION_SHARDS;
                match collect_once(&client, config.batch_size.get(), first_shard, &shutdown).await {
                    Ok(lag_millis) => {
                        diagnostics.consecutive_failures.store(0, Ordering::Release);
                        diagnostics.overdue_work_lag_millis.store(lag_millis, Ordering::Release);
                    }
                    Err(error) => {
                        diagnostics.consecutive_failures.fetch_add(1, Ordering::AcqRel);
                        client.record_garbage_collection_failure();
                        warn!(code = error.code(), "DynamoDB generation cleanup pass failed");
                    }
                }
            }
        }
    }
}

struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn collect_once(
    client: &DynamoClient,
    batch_size: u32,
    first_shard: u8,
    shutdown: &watch::Receiver<bool>,
) -> Result<u64, StorageError> {
    let mut remaining = batch_size;
    let mut maximum_lag_millis = 0_u64;
    for offset in 0..GARBAGE_COLLECTION_SHARDS {
        let shard = collection_shard(first_shard, offset);
        if *shutdown.borrow() {
            return Ok(maximum_lag_millis);
        }
        let limit = i32::try_from(remaining.max(1)).map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::ResourceExhausted,
                "dynamodb_gc_batch_limit_overflow",
                error,
            )
        })?;
        let due = item::epoch_millis(std::time::SystemTime::now())?;
        let mut upper = due.to_be_bytes().to_vec();
        upper.extend_from_slice(&[u8::MAX; 26]);
        let values = std::collections::HashMap::from([
            (
                ":pk".to_owned(),
                AttributeValue::S(format!("G#{shard:02x}")),
            ),
            (":due".to_owned(), AttributeValue::B(upper.into())),
        ]);
        let output = client
            .execute_background(
                "dynamodb_gc_query_failed",
                client
                    .sdk()
                    .query()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .table_name(client.table())
                    .key_condition_expression("pk = :pk AND sk <= :due")
                    .set_expression_attribute_values(Some(values))
                    .limit(limit)
                    .send(),
            )
            .await?;
        for work in output.items.unwrap_or_default() {
            if *shutdown.borrow() {
                return Ok(maximum_lag_millis);
            }
            let work_due = item::number_u64(&work, item::DUE_AT)?;
            let generation = item::string(&work, GENERATION)?;
            generation.parse::<Ulid>().map_err(|_| {
                StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_gc_generation_invalid",
                )
            })?;
            let mut expected_sort = work_due.to_be_bytes().to_vec();
            expected_sort.extend_from_slice(generation.as_bytes());
            let work_pk = item::string(&work, item::PK)?;
            let work_sk = item::binary(&work, item::SK)?;
            let encoded_due = work_sk
                .get(..8)
                .and_then(|value| value.try_into().ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| {
                    StorageError::new(StorageErrorKind::Integrity, "dynamodb_gc_due_key_invalid")
                })?;
            if work_due != encoded_due
                || work_due > due
                || work_pk != key::garbage_collection_partition(generation.as_bytes())
                || work_pk != format!("G#{shard:02x}")
                || work_sk != expected_sort
            {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_gc_due_mismatch",
                ));
            }
            let lag_millis = due.saturating_sub(work_due);
            maximum_lag_millis = maximum_lag_millis.max(lag_millis);
            client.record_garbage_collection_lag(lag_millis);
            if remaining == 0 {
                continue;
            }
            collect_work(client, &work).await?;
            client.record_garbage_collection_work();
            remaining = remaining.saturating_sub(1);
        }
    }
    Ok(maximum_lag_millis)
}

const fn collection_shard(first_shard: u8, offset: u8) -> u8 {
    first_shard.wrapping_add(offset) % GARBAGE_COLLECTION_SHARDS
}

async fn collect_work(client: &DynamoClient, work: &Item) -> Result<(), StorageError> {
    if item::string(work, KIND)? != "gc" {
        return Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_gc_kind_invalid",
        ));
    }
    let manifest_pk = item::string(work, item::MANIFEST_PK)?.to_owned();
    let manifest_sk = item::binary(work, item::MANIFEST_SK)?.to_vec();
    let generation = item::string(work, GENERATION)?.to_owned();
    generation.parse::<Ulid>().map_err(|_| {
        StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_gc_generation_invalid",
        )
    })?;
    let persisted_target_pk = item::string(work, item::TARGET_PK)?;
    let target_pk = blob_target_partition(&manifest_pk, &manifest_sk, &generation)?;
    if persisted_target_pk != target_pk {
        return Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_gc_target_mismatch",
        ));
    }
    let manifest = client
        .execute_background(
            "dynamodb_gc_manifest_read_failed",
            client
                .sdk()
                .get_item()
                .return_consumed_capacity(ReturnConsumedCapacity::Total)
                .table_name(client.table())
                .set_key(Some(item::key(manifest_pk.clone(), manifest_sk.clone())))
                .consistent_read(true)
                .send(),
        )
        .await?
        .item;
    let should_delete_manifest = match manifest.as_ref() {
        None => false,
        Some(value) => {
            if item::string(value, GENERATION)? != generation {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_gc_generation_mismatch",
                ));
            }
            let state = item::string(value, STATE)?;
            let kind = item::string(value, KIND)?;
            let assertion_current = if state == "COMMITTED" && kind == "assertion" {
                Some(assertion_is_current(client, &manifest_pk, &generation).await?)
            } else {
                None
            };
            match collection_decision(state, kind, assertion_current)? {
                CollectionDecision::DiscardWork => {
                    delete_work(client, work).await?;
                    return Ok(());
                }
                CollectionDecision::Claim {
                    expected_state,
                    guard_head,
                } => {
                    if !claim_manifest(
                        client,
                        &manifest_pk,
                        &manifest_sk,
                        &generation,
                        expected_state,
                        guard_head,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                    true
                }
                CollectionDecision::ResumeDeletion => true,
            }
        }
    };
    delete_partition(client, &target_pk).await?;
    if should_delete_manifest {
        client
            .execute_background(
                "dynamodb_gc_manifest_delete_failed",
                client
                    .sdk()
                    .delete_item()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .table_name(client.table())
                    .set_key(Some(item::key(manifest_pk, manifest_sk)))
                    .condition_expression("st = :deleting AND g = :generation")
                    .expression_attribute_values(
                        ":deleting",
                        AttributeValue::S("DELETING".to_owned()),
                    )
                    .expression_attribute_values(":generation", AttributeValue::S(generation))
                    .send(),
            )
            .await?;
    }
    delete_work(client, work).await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CollectionDecision {
    DiscardWork,
    Claim {
        expected_state: &'static str,
        guard_head: bool,
    },
    ResumeDeletion,
}

fn collection_decision(
    state: &str,
    kind: &str,
    assertion_current: Option<bool>,
) -> Result<CollectionDecision, StorageError> {
    match (state, kind, assertion_current) {
        ("COMMITTED", "model", None) | ("COMMITTED", "assertion", Some(true)) => {
            Ok(CollectionDecision::DiscardWork)
        }
        ("COMMITTED", "assertion", Some(false)) => Ok(CollectionDecision::Claim {
            expected_state: "COMMITTED",
            guard_head: true,
        }),
        ("STAGING", "model" | "assertion", None) => Ok(CollectionDecision::Claim {
            expected_state: "STAGING",
            guard_head: false,
        }),
        ("RETIRED", "assertion", None) => Ok(CollectionDecision::Claim {
            expected_state: "RETIRED",
            guard_head: true,
        }),
        ("DELETING", "model" | "assertion", None) => Ok(CollectionDecision::ResumeDeletion),
        _ => Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_gc_manifest_invalid",
        )),
    }
}

fn blob_target_partition(
    manifest_partition: &str,
    manifest_sort: &[u8],
    generation: &str,
) -> Result<String, StorageError> {
    let segments = manifest_partition.split('#').collect::<Vec<_>>();
    let kind = match segments.as_slice() {
        ["M", store] if canonical_id::<StoreId>(store) => {
            let model = std::str::from_utf8(manifest_sort).ok();
            if !model.is_some_and(canonical_id::<AuthorizationModelId>) {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_gc_manifest_sort_invalid",
                ));
            }
            "model"
        }
        ["A", store, model]
            if canonical_id::<StoreId>(store)
                && canonical_id::<AuthorizationModelId>(model)
                && manifest_sort == generation.as_bytes() =>
        {
            "assertion"
        }
        _ => {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "dynamodb_gc_manifest_partition_invalid",
            ));
        }
    };
    Ok(format!("B#{kind}#{manifest_partition}#{generation}"))
}

fn canonical_id<T>(value: &str) -> bool
where
    T: std::str::FromStr + ToString,
{
    value
        .parse::<T>()
        .is_ok_and(|parsed| parsed.to_string() == value)
}

async fn assertion_is_current(
    client: &DynamoClient,
    manifest_partition: &str,
    generation: &str,
) -> Result<bool, StorageError> {
    let head = client
        .execute_background(
            "dynamodb_gc_head_read_failed",
            client
                .sdk()
                .get_item()
                .return_consumed_capacity(ReturnConsumedCapacity::Total)
                .table_name(client.table())
                .set_key(Some(item::key(
                    manifest_partition.to_owned(),
                    b"head".to_vec(),
                )))
                .consistent_read(true)
                .send(),
        )
        .await?
        .item;
    match head.as_ref() {
        Some(item) => {
            let current = item::string(item, GENERATION)?;
            current.parse::<Ulid>().map_err(|_| {
                StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_gc_head_generation_invalid",
                )
            })?;
            Ok(current == generation)
        }
        None => Ok(false),
    }
}

async fn claim_manifest(
    client: &DynamoClient,
    manifest_partition: &str,
    manifest_sort: &[u8],
    generation: &str,
    expected_state: &str,
    guard_head: bool,
) -> Result<bool, StorageError> {
    let update = Update::builder()
        .table_name(client.table())
        .set_key(Some(item::key(
            manifest_partition.to_owned(),
            manifest_sort.to_vec(),
        )))
        .update_expression("SET st = :deleting")
        .condition_expression("st = :expected AND g = :generation")
        .expression_attribute_values(":deleting", AttributeValue::S("DELETING".to_owned()))
        .expression_attribute_values(":expected", AttributeValue::S(expected_state.to_owned()))
        .expression_attribute_values(":generation", AttributeValue::S(generation.to_owned()))
        .build()
        .map_err(request_build_error)?;
    let mut actions = vec![TransactWriteItem::builder().update(update).build()];
    if guard_head {
        let check = ConditionCheck::builder()
            .table_name(client.table())
            .set_key(Some(item::key(
                manifest_partition.to_owned(),
                b"head".to_vec(),
            )))
            .condition_expression("attribute_not_exists(g) OR g <> :generation")
            .expression_attribute_values(":generation", AttributeValue::S(generation.to_owned()))
            .build()
            .map_err(request_build_error)?;
        actions.push(TransactWriteItem::builder().condition_check(check).build());
    }
    let result = client
        .transact_write_background("dynamodb_gc_claim_failed", actions, random_token()?)
        .await;
    match result {
        Ok(_) => Ok(true),
        Err(error) => {
            if error.kind() == StorageErrorKind::Conflict {
                Ok(false)
            } else {
                Err(error)
            }
        }
    }
}

async fn delete_work(client: &DynamoClient, work: &Item) -> Result<(), StorageError> {
    let work_pk = item::string(work, item::PK)?.to_owned();
    let work_sk = item::binary(work, item::SK)?.to_vec();
    client
        .execute_background(
            "dynamodb_gc_work_delete_failed",
            client
                .sdk()
                .delete_item()
                .return_consumed_capacity(ReturnConsumedCapacity::Total)
                .table_name(client.table())
                .set_key(Some(item::key(work_pk, work_sk)))
                .send(),
        )
        .await?;
    Ok(())
}

fn random_token() -> Result<String, StorageError> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|error| {
        StorageError::with_source(StorageErrorKind::Internal, "dynamodb_random_failed", error)
    })?;
    Ok(Ulid::from(random).to_string())
}

fn request_build_error(error: impl std::error::Error + Send + Sync + 'static) -> StorageError {
    StorageError::with_source(
        StorageErrorKind::Internal,
        "dynamodb_gc_request_build_failed",
        error,
    )
}

async fn delete_partition(client: &DynamoClient, partition: &str) -> Result<(), StorageError> {
    let mut start = None;
    loop {
        let values = std::collections::HashMap::from([(
            ":pk".to_owned(),
            AttributeValue::S(partition.to_owned()),
        )]);
        let output = client
            .execute_background(
                "dynamodb_gc_blob_query_failed",
                client
                    .sdk()
                    .query()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .table_name(client.table())
                    .key_condition_expression("pk = :pk")
                    .set_expression_attribute_values(Some(values))
                    .set_exclusive_start_key(start)
                    .send(),
            )
            .await?;
        for value in output.items.unwrap_or_default() {
            let pk = item::string(&value, item::PK)?.to_owned();
            let sk = item::binary(&value, item::SK)?.to_vec();
            client
                .execute_background(
                    "dynamodb_gc_blob_delete_failed",
                    client
                        .sdk()
                        .delete_item()
                        .return_consumed_capacity(ReturnConsumedCapacity::Total)
                        .table_name(client.table())
                        .set_key(Some(item::key(pk, sk)))
                        .send(),
                )
                .await?;
        }
        start = output.last_evaluated_key;
        if start
            .as_ref()
            .is_none_or(std::collections::HashMap::is_empty)
        {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_derive_gc_target_from_validated_manifest_identity() -> Result<(), StorageError> {
        let generation = "01K23Y8M5Y2Q6X9QKPF9J3WJ7Z";
        assert_eq!(
            blob_target_partition(
                "M#01K23Y8M5Y2Q6X9QKPF9J3WJ7Y",
                b"01K23Y8M5Y2Q6X9QKPF9J3WJ7X",
                generation,
            )?,
            format!("B#model#M#01K23Y8M5Y2Q6X9QKPF9J3WJ7Y#{generation}")
        );
        assert!(blob_target_partition("F#forged", b"forged", generation).is_err());
        Ok(())
    }

    #[test]
    fn test_should_rotate_gc_shards_without_omission() {
        for first in 0..GARBAGE_COLLECTION_SHARDS {
            let visited = (0..GARBAGE_COLLECTION_SHARDS)
                .map(|offset| collection_shard(first, offset))
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(visited.len(), usize::from(GARBAGE_COLLECTION_SHARDS));
            assert_eq!(collection_shard(first, 0), first);
        }
    }

    #[test]
    fn test_should_classify_every_gc_manifest_transition() -> Result<(), StorageError> {
        assert_eq!(
            collection_decision("COMMITTED", "assertion", Some(true))?,
            CollectionDecision::DiscardWork
        );
        assert_eq!(
            collection_decision("COMMITTED", "assertion", Some(false))?,
            CollectionDecision::Claim {
                expected_state: "COMMITTED",
                guard_head: true,
            }
        );
        assert_eq!(
            collection_decision("STAGING", "model", None)?,
            CollectionDecision::Claim {
                expected_state: "STAGING",
                guard_head: false,
            }
        );
        assert_eq!(
            collection_decision("RETIRED", "assertion", None)?,
            CollectionDecision::Claim {
                expected_state: "RETIRED",
                guard_head: true,
            }
        );
        assert_eq!(
            collection_decision("DELETING", "model", None)?,
            CollectionDecision::ResumeDeletion
        );
        assert!(collection_decision("COMMITTED", "assertion", None).is_err());
        assert!(collection_decision("UNKNOWN", "model", None).is_err());
        Ok(())
    }
}
