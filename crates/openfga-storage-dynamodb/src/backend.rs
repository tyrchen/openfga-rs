//! `DynamoDB` storage capability implementation.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use aws_sdk_dynamodb::{
    primitives::Blob,
    types::{
        AttributeValue, ConditionCheck, Delete, KeysAndAttributes, Put, ReturnConsumedCapacity,
        Select, TransactWriteItem, Update,
    },
};
use openfga_domain::{
    AuthorizationModelId, ChangeId, ConsistencyPreference, RelationshipTuple, StoreId, SubjectRef,
    TupleKey,
};
use openfga_model::ModelCompiler;
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ChangeFilter, ChangeOperation, ChangeReader,
    ConditionFilter, HealthCheck, HealthStatus, ModelReader, ModelWriter, MutationOutcome,
    ObjectRelationFilter, OperationContext, Page, PageOptions, ReadOptions, ReverseTupleFilter,
    StorageCursor, StorageError, StorageErrorKind, StoreFilter, StoreName, StoreReader,
    StoreRecord, StoreWriter, StoredAuthorizationModel, StoredTuple, TupleChange, TupleReadFilter,
    TupleReader, TupleStream, TupleWriteOptions, TupleWriter, UsersetTupleFilter,
    WriteConflictPolicy,
    persistence::{
        MAXIMUM_ASSERTION_PAYLOAD_BYTES, MAXIMUM_MODEL_PAYLOAD_BYTES, decode_assertions,
        decode_model, encode_assertions, encode_model,
    },
};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use ulid::Ulid;

use crate::{
    DynamoDbProvisioningStatus, DynamoDbRuntime, DynamoDbRuntimeDiagnostics, DynamoDbStorageConfig,
    client::DynamoClient,
    item::{
        self, CHUNK_BYTES, CHUNK_COUNT, CREATED_AT, DIGEST, GENERATION, Item, KIND, LAST_CHANGE,
        NAME, PAYLOAD, PAYLOAD_BYTES, STATE, TIMESTAMP, UPDATED_AT,
    },
    key::{
        self, CursorOperation, FORWARD_SHARDS, STORE_SHARDS, assertion_partition,
        change_head_partition, change_partition, decode_cursor, encode_cursor,
        forward_object_partition, forward_object_relation_prefix, forward_partition,
        model_partition, reverse_partition, reverse_prefix, store_partition, tuple_keys,
    },
    migration,
};

const MAXIMUM_QUERY_ITEMS: usize = 100_000;
const MAXIMUM_PAGE_EVALUATIONS: usize = 4_096;
const STATE_ACTIVE: &str = "ACTIVE";
const STATE_STAGING: &str = "STAGING";
const STATE_COMMITTED: &str = "COMMITTED";
const HEAD_SK: &[u8] = b"head";
const TRANSACTION_ITEM_BYTES: usize = 3_500 * 1_024;

/// Durable single-table `DynamoDB` storage backend.
pub struct DynamoDbStorage {
    client: DynamoClient,
    config: DynamoDbStorageConfig,
    compiler: ModelCompiler,
    runtime_diagnostics: DynamoDbRuntimeDiagnostics,
}

impl DynamoDbStorage {
    /// Creates a backend and verifies exact schema compatibility.
    ///
    /// # Errors
    ///
    /// Returns a typed storage failure when SDK configuration or schema validation fails.
    pub async fn connect(
        config: DynamoDbStorageConfig,
        context: &OperationContext,
    ) -> Result<(Self, DynamoDbRuntime), StorageError> {
        let client = DynamoClient::create(&config).await?;
        migration::require_ready(&client, context).await?;
        let runtime = DynamoDbRuntime::start(client.clone(), config.garbage_collection.clone())?;
        let runtime_diagnostics = runtime.diagnostics();
        Ok((
            Self {
                client,
                config,
                compiler: ModelCompiler::default(),
                runtime_diagnostics,
            },
            runtime,
        ))
    }

    /// Creates the table when absent and writes immutable schema metadata.
    ///
    /// # Errors
    ///
    /// Returns a typed storage failure for incompatible tables or control-plane errors.
    pub async fn provision(
        config: &DynamoDbStorageConfig,
        context: &OperationContext,
    ) -> Result<DynamoDbProvisioningStatus, StorageError> {
        let client = DynamoClient::create(config).await?;
        migration::provision(&client, context, config).await
    }

    /// Reads safe table/schema provisioning status.
    ///
    /// # Errors
    ///
    /// Returns a typed storage failure for control-plane or credential errors.
    pub async fn provisioning_status(
        config: &DynamoDbStorageConfig,
        context: &OperationContext,
    ) -> Result<DynamoDbProvisioningStatus, StorageError> {
        let client = DynamoClient::create(config).await?;
        migration::status(&client, context).await
    }

    async fn get(
        &self,
        context: &OperationContext,
        pk: String,
        sk: Vec<u8>,
        force_strong: bool,
    ) -> Result<Option<Item>, StorageError> {
        let expected_pk = pk.clone();
        let expected_sk = sk.clone();
        let output = self
            .client
            .execute(
                context,
                "dynamodb_get_item_failed",
                self.client
                    .sdk()
                    .get_item()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .table_name(self.client.table())
                    .set_key(Some(item::key(pk, sk)))
                    .consistent_read(force_strong || strong(context))
                    .send(),
            )
            .await?;
        output
            .item
            .map(|raw| {
                require_physical_identity(&raw, &expected_pk, &expected_sk)?;
                Ok(raw)
            })
            .transpose()
    }

    async fn query_partition(
        &self,
        context: &OperationContext,
        pk: String,
        prefix: Option<Vec<u8>>,
        evaluation_budget: usize,
    ) -> Result<Vec<Item>, StorageError> {
        let mut start = None;
        let mut items = Vec::new();
        loop {
            let remaining = evaluation_budget.saturating_sub(items.len());
            if remaining == 0 {
                return Err(StorageError::new(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_snapshot_evaluation_limit",
                ));
            }
            let limit = i32::try_from(remaining).map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_query_limit_overflow",
                    error,
                )
            })?;
            let mut values = HashMap::from([(":pk".to_owned(), AttributeValue::S(pk.clone()))]);
            let expression = if let Some(prefix) = &prefix {
                values.insert(
                    ":prefix".to_owned(),
                    AttributeValue::B(Blob::new(prefix.clone())),
                );
                "pk = :pk AND begins_with(sk, :prefix)"
            } else {
                "pk = :pk"
            };
            let output = self
                .client
                .execute(
                    context,
                    "dynamodb_query_failed",
                    self.client
                        .sdk()
                        .query()
                        .return_consumed_capacity(ReturnConsumedCapacity::Total)
                        .table_name(self.client.table())
                        .key_condition_expression(expression)
                        .set_expression_attribute_values(Some(values))
                        .set_exclusive_start_key(start)
                        .consistent_read(strong(context))
                        .limit(limit)
                        .send(),
                )
                .await?;
            let output_items = output.items.unwrap_or_default();
            if items.len().saturating_add(output_items.len()) > evaluation_budget {
                return Err(StorageError::new(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_query_item_limit",
                ));
            }
            for raw in &output_items {
                let actual_pk = item::string(raw, item::PK)?;
                let actual_sk = item::binary(raw, item::SK)?;
                if actual_pk != pk
                    || prefix
                        .as_ref()
                        .is_some_and(|expected| !actual_sk.starts_with(expected))
                {
                    return Err(StorageError::new(
                        StorageErrorKind::Integrity,
                        "dynamodb_query_identity_mismatch",
                    ));
                }
            }
            items.extend(output_items);
            start = output.last_evaluated_key;
            if start.as_ref().is_none_or(HashMap::is_empty) {
                return Ok(items);
            }
        }
    }

    async fn count_partition(
        &self,
        context: &OperationContext,
        pk: String,
        prefix: Vec<u8>,
    ) -> Result<u64, StorageError> {
        let mut start = None;
        let mut evaluated = 0_usize;
        let mut count = 0_u64;
        loop {
            let remaining = MAXIMUM_QUERY_ITEMS.saturating_sub(evaluated);
            if remaining == 0 {
                return Err(StorageError::new(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_count_evaluation_limit",
                ));
            }
            let limit = i32::try_from(remaining).map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_query_limit_overflow",
                    error,
                )
            })?;
            let values = HashMap::from([
                (":pk".to_owned(), AttributeValue::S(pk.clone())),
                (
                    ":prefix".to_owned(),
                    AttributeValue::B(Blob::new(prefix.clone())),
                ),
            ]);
            let output = self
                .client
                .execute(
                    context,
                    "dynamodb_count_query_failed",
                    self.client
                        .sdk()
                        .query()
                        .return_consumed_capacity(ReturnConsumedCapacity::Total)
                        .table_name(self.client.table())
                        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
                        .set_expression_attribute_values(Some(values))
                        .set_exclusive_start_key(start)
                        .consistent_read(strong(context))
                        .select(Select::Count)
                        .limit(limit)
                        .send(),
                )
                .await?;
            let page_evaluated = usize::try_from(output.scanned_count).unwrap_or_default();
            evaluated = evaluated.checked_add(page_evaluated).ok_or_else(|| {
                StorageError::new(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_count_evaluation_overflow",
                )
            })?;
            count = count
                .checked_add(u64::try_from(output.count).unwrap_or_default())
                .ok_or_else(|| {
                    StorageError::new(
                        StorageErrorKind::ResourceExhausted,
                        "dynamodb_tuple_count_overflow",
                    )
                })?;
            self.client.record_query_work("count", page_evaluated, 0, 1);
            start = output.last_evaluated_key;
            if start.as_ref().is_none_or(HashMap::is_empty) {
                return Ok(count);
            }
        }
    }

    async fn put_item(
        &self,
        context: &OperationContext,
        item: Item,
        condition: Option<&str>,
        conflict_code: &'static str,
    ) -> Result<(), StorageError> {
        item::require_item_limit(&item)?;
        let result = self
            .client
            .execute(
                context,
                "dynamodb_put_item_failed",
                self.client
                    .sdk()
                    .put_item()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .table_name(self.client.table())
                    .set_item(Some(item))
                    .set_condition_expression(condition.map(str::to_owned))
                    .send(),
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == StorageErrorKind::Conflict => Err(StorageError::new(
                StorageErrorKind::AlreadyExists,
                conflict_code,
            )),
            Err(error) => Err(error),
        }
    }
}

impl fmt::Debug for DynamoDbStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamoDbStorage")
            .field("client", &self.client)
            .field("config", &self.config)
            .field("runtime_diagnostics", &self.runtime_diagnostics)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ForwardWindow {
    matches: Vec<(Vec<u8>, StoredTuple)>,
    frontier: Option<Vec<u8>>,
    exhausted: bool,
}

#[derive(Debug)]
struct RawWindow {
    items: Vec<Item>,
    frontier: Option<Vec<u8>>,
    exhausted: bool,
}

async fn query_raw_window(
    client: DynamoClient,
    context: OperationContext,
    pk: String,
    boundary: Option<Vec<u8>>,
    ascending: bool,
    evaluation_budget: usize,
) -> Result<RawWindow, StorageError> {
    let limit = i32::try_from(evaluation_budget).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::ResourceExhausted,
            "dynamodb_query_limit_overflow",
            error,
        )
    })?;
    let mut values = HashMap::from([(":pk".to_owned(), AttributeValue::S(pk.clone()))]);
    let expression = if let Some(boundary) = &boundary {
        values.insert(
            ":boundary".to_owned(),
            AttributeValue::B(Blob::new(boundary.clone())),
        );
        if ascending {
            "pk = :pk AND sk > :boundary"
        } else {
            "pk = :pk AND sk < :boundary"
        }
    } else {
        "pk = :pk"
    };
    let output = client
        .execute(
            &context,
            "dynamodb_page_query_failed",
            client
                .sdk()
                .query()
                .return_consumed_capacity(ReturnConsumedCapacity::Total)
                .table_name(client.table())
                .key_condition_expression(expression)
                .set_expression_attribute_values(Some(values))
                .scan_index_forward(ascending)
                .consistent_read(strong(&context))
                .limit(limit)
                .send(),
        )
        .await?;
    let evaluated = usize::try_from(output.scanned_count).unwrap_or_default();
    let items = output.items.unwrap_or_default();
    client.record_query_work("page_query", evaluated, items.len(), 1);
    for raw in &items {
        let sk = item::binary(raw, item::SK)?;
        require_physical_identity(raw, &pk, sk)?;
        if boundary.as_ref().is_some_and(|value| {
            (ascending && sk <= value.as_slice()) || (!ascending && sk >= value.as_slice())
        }) {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "dynamodb_query_boundary_violation",
            ));
        }
    }
    let start = output.last_evaluated_key;
    let frontier = start
        .as_ref()
        .map(|key| {
            require_physical_identity(key, &pk, item::binary(key, item::SK)?)?;
            Ok(item::binary(key, item::SK)?.to_vec())
        })
        .transpose()?
        .or_else(|| {
            items
                .last()
                .and_then(|raw| item::binary(raw, item::SK).ok())
                .map(<[u8]>::to_vec)
        });
    Ok(RawWindow {
        items,
        frontier,
        exhausted: start.as_ref().is_none_or(HashMap::is_empty),
    })
}

async fn query_forward_window(
    client: DynamoClient,
    context: OperationContext,
    store_id: StoreId,
    pk: String,
    boundary: Option<Vec<u8>>,
    filter: TupleReadFilter,
    maximum_matches: usize,
    evaluation_budget: usize,
) -> Result<ForwardWindow, StorageError> {
    let mut start = None;
    let mut evaluated = 0_usize;
    let mut frontier = boundary.clone();
    let mut matches = Vec::new();
    loop {
        let remaining = evaluation_budget.saturating_sub(evaluated);
        if remaining == 0 || matches.len() >= maximum_matches {
            return Ok(ForwardWindow {
                matches,
                frontier,
                exhausted: false,
            });
        }
        let limit = i32::try_from(remaining).map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::ResourceExhausted,
                "dynamodb_query_limit_overflow",
                error,
            )
        })?;
        let mut values = HashMap::from([(":pk".to_owned(), AttributeValue::S(pk.clone()))]);
        let expression = if let Some(boundary) = &boundary {
            values.insert(
                ":after".to_owned(),
                AttributeValue::B(Blob::new(boundary.clone())),
            );
            "pk = :pk AND sk > :after"
        } else {
            "pk = :pk"
        };
        let output = client
            .execute(
                &context,
                "dynamodb_tuple_page_query_failed",
                client
                    .sdk()
                    .query()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .table_name(client.table())
                    .key_condition_expression(expression)
                    .set_expression_attribute_values(Some(values))
                    .set_exclusive_start_key(start)
                    .consistent_read(strong(&context))
                    .limit(limit)
                    .send(),
            )
            .await?;
        let page = output.items.unwrap_or_default();
        evaluated = evaluated.checked_add(page.len()).ok_or_else(|| {
            StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "dynamodb_query_evaluation_overflow",
            )
        })?;
        for raw in page {
            let encoded = item::binary(&raw, item::SK)?.to_vec();
            require_physical_identity(&raw, &pk, &encoded)?;
            if boundary
                .as_ref()
                .is_some_and(|minimum| encoded.as_slice() <= minimum.as_slice())
            {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_query_boundary_violation",
                ));
            }
            let stored = item::decode_stored_tuple(&raw)?;
            if key::decode_forward(&encoded)? != *stored.tuple().key() {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_forward_key_payload_mismatch",
                ));
            }
            if tuple_keys(store_id, stored.tuple().key())?.forward_partition != pk {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_forward_shard_mismatch",
                ));
            }
            frontier = Some(encoded.clone());
            if filter.matches(stored.tuple().key()) {
                matches.push((encoded, stored));
            }
        }
        start = output.last_evaluated_key;
        if let Some(last) = &start {
            require_physical_identity(last, &pk, item::binary(last, item::SK)?)?;
            frontier = Some(item::binary(last, item::SK)?.to_vec());
        } else {
            return Ok(ForwardWindow {
                matches,
                frontier,
                exhausted: true,
            });
        }
    }
}

#[async_trait]
impl TupleReader for DynamoDbStorage {
    async fn read_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &TupleReadFilter,
        options: &PageOptions,
    ) -> Result<Page<StoredTuple>, StorageError> {
        let boundary = options
            .after()
            .map(|cursor| decode_cursor(CursorOperation::Tuple, cursor))
            .transpose()?
            .map(<[u8]>::to_vec);
        let per_shard_budget = MAXIMUM_PAGE_EVALUATIONS.div_ceil(usize::from(FORWARD_SHARDS));
        let mut tasks = JoinSet::new();
        for shard in 0..FORWARD_SHARDS {
            tasks.spawn(query_forward_window(
                self.client.clone(),
                context.clone(),
                store_id,
                forward_partition(store_id, shard),
                boundary.clone(),
                filter.clone(),
                options.maximum_results().saturating_add(1),
                per_shard_budget,
            ));
        }
        let mut windows = Vec::with_capacity(usize::from(FORWARD_SHARDS));
        while let Some(result) = tasks.join_next().await {
            windows.push(result.map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Internal,
                    "dynamodb_query_task_failed",
                    error,
                )
            })??);
        }
        let safe_frontier = windows
            .iter()
            .filter(|window| !window.exhausted)
            .filter_map(|window| window.frontier.as_deref())
            .min()
            .map(<[u8]>::to_vec);
        let mut matches = windows
            .into_iter()
            .flat_map(|window| window.matches)
            .filter(|(encoded, _)| {
                safe_frontier
                    .as_ref()
                    .is_none_or(|frontier| encoded <= frontier)
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| left.0.cmp(&right.0));
        if matches.windows(2).any(|pair| {
            pair.first().map(|(_, tuple)| tuple.tuple().key())
                == pair.get(1).map(|(_, tuple)| tuple.tuple().key())
        }) {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "dynamodb_tuple_duplicate_identity",
            ));
        }
        let full_page = matches.len() > options.maximum_results();
        matches.truncate(options.maximum_results());
        let continuation_key = if full_page {
            matches.last().map(|(encoded, _)| encoded.as_slice())
        } else {
            safe_frontier.as_deref()
        };
        let continuation = continuation_key
            .map(|encoded| encode_cursor(CursorOperation::Tuple, encoded))
            .transpose()?;
        Ok(Page::new(
            matches.into_iter().map(|(_, tuple)| tuple).collect(),
            continuation,
        ))
    }

    async fn read_exact_tuple(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<StoredTuple, StorageError> {
        let keys = tuple_keys(store_id, key)?;
        self.get(context, keys.forward_partition, keys.forward_sort, false)
            .await?
            .ok_or_else(|| StorageError::new(StorageErrorKind::NotFound, "tuple_not_found"))
            .and_then(|raw| item::require_tuple_identity(&raw, key))
    }

    async fn read_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        if !filter.subjects().is_empty() {
            let keys = filter
                .subjects()
                .iter()
                .map(|subject| {
                    tuple_key_from_parts(
                        &filter.object().to_string(),
                        filter.relation().as_str(),
                        &subject.to_string(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            return self
                .read_exact_tuple_keys(context, store_id, keys, filter.conditions(), options)
                .await;
        }
        let mut tuples = self
            .query_partition(
                context,
                forward_object_partition(store_id, filter.object()),
                Some(forward_object_relation_prefix(
                    filter.object(),
                    filter.relation(),
                )?),
                snapshot_evaluation_budget(options),
            )
            .await?
            .into_iter()
            .map(|raw| decode_forward_item(&raw))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|stored| {
                let tuple = stored.tuple();
                tuple.key().object() == filter.object()
                    && tuple.key().relation() == filter.relation()
                    && (filter.subjects().is_empty()
                        || filter.subjects().contains(tuple.key().subject()))
                    && filter.conditions().matches(tuple.condition())
            })
            .map(StoredTuple::into_tuple)
            .collect::<Vec<_>>();
        require_snapshot_limit(&mut tuples, options)?;
        Ok(TupleStream::from_tuples(tuples))
    }

    async fn read_userset_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &UsersetTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        let mut tuples = self
            .query_partition(
                context,
                forward_object_partition(store_id, filter.object()),
                Some(forward_object_relation_prefix(
                    filter.object(),
                    filter.relation(),
                )?),
                snapshot_evaluation_budget(options),
            )
            .await?
            .into_iter()
            .map(|raw| decode_forward_item(&raw))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|stored| {
                let tuple = stored.tuple();
                let allowed = match tuple.key().subject() {
                    SubjectRef::Userset(userset) => {
                        filter.allowed().is_empty()
                            || filter.allowed().iter().any(|candidate| {
                                candidate.subject_type() == userset.object().object_type()
                                    && candidate.relation() == userset.relation()
                            })
                    }
                    _ => false,
                };
                tuple.key().object() == filter.object()
                    && tuple.key().relation() == filter.relation()
                    && allowed
                    && filter.conditions().matches(tuple.condition())
            })
            .map(StoredTuple::into_tuple)
            .collect::<Vec<_>>();
        require_snapshot_limit(&mut tuples, options)?;
        Ok(TupleStream::from_tuples(tuples))
    }

    async fn read_reverse_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ReverseTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        if !filter.object_ids().is_empty() {
            let key_count = filter
                .subjects()
                .len()
                .checked_mul(filter.object_ids().len())
                .ok_or_else(|| {
                    StorageError::new(
                        StorageErrorKind::ResourceExhausted,
                        "dynamodb_exact_key_count_overflow",
                    )
                })?;
            if key_count > snapshot_evaluation_budget(options) {
                return Err(StorageError::new(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_snapshot_evaluation_limit",
                ));
            }
            let mut keys = Vec::with_capacity(key_count);
            for subject in filter.subjects() {
                for object_id in filter.object_ids() {
                    keys.push(tuple_key_from_parts(
                        &format!("{}:{object_id}", filter.object_type()),
                        filter.relation().as_str(),
                        &subject.to_string(),
                    )?);
                }
            }
            return self
                .read_exact_tuple_keys(context, store_id, keys, filter.conditions(), options)
                .await;
        }
        let mut found = BTreeMap::new();
        let total_budget = snapshot_evaluation_budget(options);
        let per_subject_budget = total_budget.div_ceil(filter.subjects().len().max(1));
        for subject in filter.subjects() {
            let pk = reverse_partition(store_id, subject);
            let prefix = reverse_prefix(subject, filter.object_type(), filter.relation())?;
            for raw in self
                .query_partition(context, pk, Some(prefix), per_subject_budget)
                .await?
            {
                let stored = item::decode_stored_tuple(&raw)?;
                let physical_key = key::decode_reverse(item::binary(&raw, item::SK)?)?;
                if &physical_key != stored.tuple().key() {
                    return Err(StorageError::new(
                        StorageErrorKind::Integrity,
                        "dynamodb_reverse_key_payload_mismatch",
                    ));
                }
                let tuple = stored.into_tuple();
                if tuple.key().subject() == subject
                    && tuple.key().object().object_type() == filter.object_type()
                    && tuple.key().relation() == filter.relation()
                    && (filter.object_ids().is_empty()
                        || filter
                            .object_ids()
                            .contains(tuple.key().object().object_id()))
                    && filter.conditions().matches(tuple.condition())
                {
                    found.insert(tuple.key().clone(), tuple);
                }
            }
        }
        let mut tuples = found.into_values().collect::<Vec<_>>();
        require_snapshot_limit(&mut tuples, options)?;
        Ok(TupleStream::from_tuples(tuples))
    }

    async fn tuple_exists(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<bool, StorageError> {
        match self.read_exact_tuple(context, store_id, key).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn count_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError> {
        let partition = forward_object_partition(store_id, filter.object());
        let prefix = forward_object_relation_prefix(filter.object(), filter.relation())?;
        if filter.subjects().is_empty() && filter.conditions().is_any() {
            return self.count_partition(context, partition, prefix).await;
        }
        let count = self
            .query_partition(context, partition, Some(prefix), MAXIMUM_PAGE_EVALUATIONS)
            .await?
            .into_iter()
            .map(|raw| decode_forward_item(&raw))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|stored| {
                let tuple = stored.tuple();
                tuple.key().object() == filter.object()
                    && tuple.key().relation() == filter.relation()
                    && (filter.subjects().is_empty()
                        || filter.subjects().contains(tuple.key().subject()))
                    && filter.conditions().matches(tuple.condition())
            })
            .count();
        u64::try_from(count).map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::ResourceExhausted,
                "dynamodb_tuple_count_overflow",
                error,
            )
        })
    }
}

#[async_trait]
impl TupleWriter for DynamoDbStorage {
    async fn write_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, StorageError> {
        validate_mutation(&deletes, &writes, self.config.maximum_tuple_mutations.get())?;
        let mut deletes = deletes;
        deletes.sort();
        let mut writes = writes;
        writes.sort_by(|left, right| left.key().cmp(right.key()));
        let mut jitter = [0_u8; 1];
        getrandom::fill(&mut jitter).map_err(|error| {
            StorageError::with_source(StorageErrorKind::Internal, "dynamodb_random_failed", error)
        })?;
        for attempt in 0..self.config.maximum_conflict_retries.get() {
            match self
                .write_tuples_once(context, store_id, &deletes, &writes, options)
                .await
            {
                Err(error)
                    if error.kind() == StorageErrorKind::Conflict
                        && error.code() == "dynamodb_transaction_failed" =>
                {
                    self.client.record_head_retry("change");
                    operation_backoff(
                        context,
                        attempt,
                        jitter[0],
                        "dynamodb_conflict_retry_cancelled",
                        "dynamodb_conflict_retry_timed_out",
                    )
                    .await?;
                }
                result => return result,
            }
        }
        Err(StorageError::new(
            StorageErrorKind::Conflict,
            "dynamodb_mutation_retry_exhausted",
        ))
    }
}

async fn operation_backoff(
    context: &OperationContext,
    attempt: u32,
    jitter: u8,
    cancelled_code: &'static str,
    timeout_code: &'static str,
) -> Result<(), StorageError> {
    let exponent = attempt.min(6);
    let base_millis = 1_u64.checked_shl(exponent).unwrap_or(64);
    let jitter_millis = u64::from(jitter) % base_millis.saturating_add(1);
    let delay = Duration::from_millis(base_millis.saturating_add(jitter_millis));
    let deadline = tokio::time::Instant::from_std(context.deadline().instant());
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => Err(StorageError::new(
            StorageErrorKind::Cancelled,
            cancelled_code,
        )),
        () = tokio::time::sleep_until(deadline) => Err(StorageError::new(
            StorageErrorKind::Timeout,
            timeout_code,
        )),
        () = tokio::time::sleep(delay) => Ok(()),
    }
}

impl DynamoDbStorage {
    async fn batch_get_tuple_items<'a>(
        &self,
        context: &OperationContext,
        keys: impl IntoIterator<Item = &'a key::TupleKeys>,
        consistent: bool,
    ) -> Result<HashMap<(String, Vec<u8>), Item>, StorageError> {
        let keys = keys.into_iter().collect::<Vec<_>>();
        let mut requested = Vec::with_capacity(keys.len().saturating_mul(2));
        for tuple_keys in keys {
            requested.push(item::key(
                tuple_keys.forward_partition.clone(),
                tuple_keys.forward_sort.clone(),
            ));
            requested.push(item::key(
                tuple_keys.reverse_partition.clone(),
                tuple_keys.reverse_sort.clone(),
            ));
        }
        let expected = requested
            .iter()
            .map(|raw| {
                Ok((
                    item::string(raw, item::PK)?.to_owned(),
                    item::binary(raw, item::SK)?.to_vec(),
                ))
            })
            .collect::<Result<HashSet<_>, StorageError>>()?;
        let mut found = HashMap::with_capacity(requested.len());
        let mut jitter = [0_u8; 1];
        getrandom::fill(&mut jitter).map_err(|error| {
            StorageError::with_source(StorageErrorKind::Internal, "dynamodb_random_failed", error)
        })?;
        for batch in requested.chunks(100) {
            let mut pending = batch.to_vec();
            for attempt in 0..self.config.maximum_attempts.get() {
                if pending.is_empty() {
                    break;
                }
                let request = KeysAndAttributes::builder()
                    .set_keys(Some(pending))
                    .consistent_read(consistent)
                    .build()
                    .map_err(request_build_error)?;
                let output = self
                    .client
                    .execute(
                        context,
                        "dynamodb_tuple_batch_get_failed",
                        self.client
                            .sdk()
                            .batch_get_item()
                            .return_consumed_capacity(ReturnConsumedCapacity::Total)
                            .request_items(self.client.table(), request)
                            .send(),
                    )
                    .await?;
                let mut responses = output.responses.unwrap_or_default();
                for raw in responses.remove(self.client.table()).unwrap_or_default() {
                    let identity = (
                        item::string(&raw, item::PK)?.to_owned(),
                        item::binary(&raw, item::SK)?.to_vec(),
                    );
                    if !expected.contains(&identity) {
                        return Err(StorageError::new(
                            StorageErrorKind::Integrity,
                            "dynamodb_tuple_batch_get_unrequested_item",
                        ));
                    }
                    if found.insert(identity, raw).is_some() {
                        return Err(StorageError::new(
                            StorageErrorKind::Integrity,
                            "dynamodb_tuple_batch_get_duplicate",
                        ));
                    }
                }
                let mut unprocessed = output.unprocessed_keys.unwrap_or_default();
                pending = unprocessed
                    .remove(self.client.table())
                    .map_or_else(Vec::new, |request| request.keys);
                if !pending.is_empty() {
                    self.client.record_unprocessed_keys(pending.len());
                    operation_backoff(
                        context,
                        attempt,
                        jitter[0],
                        "dynamodb_batch_get_retry_cancelled",
                        "dynamodb_batch_get_retry_timed_out",
                    )
                    .await?;
                }
            }
            if !pending.is_empty() {
                return Err(StorageError::new(
                    StorageErrorKind::Unavailable,
                    "dynamodb_tuple_batch_get_retry_exhausted",
                ));
            }
        }
        Ok(found)
    }

    async fn read_exact_tuple_keys(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        keys: Vec<TupleKey>,
        conditions: &ConditionFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        if keys.len() > snapshot_evaluation_budget(options) {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "dynamodb_snapshot_evaluation_limit",
            ));
        }
        let physical = keys
            .iter()
            .map(|key| tuple_keys(store_id, key))
            .collect::<Result<Vec<_>, _>>()?;
        let states = self
            .batch_get_tuple_items(context, physical.iter(), strong(context))
            .await?;
        let mut tuples = Vec::with_capacity(keys.len().min(options.maximum_results()));
        for (key, physical) in keys.iter().zip(physical) {
            let forward = states.get(&(
                physical.forward_partition.clone(),
                physical.forward_sort.clone(),
            ));
            let reverse = states.get(&(
                physical.reverse_partition.clone(),
                physical.reverse_sort.clone(),
            ));
            if let PairState::Present { tuple, .. } = validate_pair(forward, reverse, key)?
                && conditions.matches(tuple.condition())
            {
                tuples.push(*tuple);
            }
        }
        require_snapshot_limit(&mut tuples, options)?;
        Ok(TupleStream::from_tuples(tuples))
    }

    async fn write_tuples_once(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        deletes: &[TupleKey],
        writes: &[RelationshipTuple],
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, StorageError> {
        let timestamp = SystemTime::now();
        let mut actions = Vec::with_capacity(
            deletes
                .len()
                .saturating_add(writes.len())
                .saturating_mul(2)
                .saturating_add(2),
        );
        let mut changes = Vec::new();
        let delete_keys = deletes
            .iter()
            .map(|tuple| tuple_keys(store_id, tuple))
            .collect::<Result<Vec<_>, _>>()?;
        let write_keys = writes
            .iter()
            .map(|tuple| tuple_keys(store_id, tuple.key()))
            .collect::<Result<Vec<_>, _>>()?;
        let states = self
            .batch_get_tuple_items(context, delete_keys.iter().chain(write_keys.iter()), true)
            .await?;
        for (key, keys) in deletes.iter().zip(delete_keys) {
            let forward = states.get(&(keys.forward_partition.clone(), keys.forward_sort.clone()));
            let reverse = states.get(&(keys.reverse_partition.clone(), keys.reverse_sort.clone()));
            let state = validate_pair(forward, reverse, key)?;
            match state {
                PairState::Missing
                    if options.on_missing_delete() == WriteConflictPolicy::Ignore =>
                {
                    actions.push(condition_absent(
                        self.client.table(),
                        keys.forward_partition,
                        keys.forward_sort,
                    )?);
                    actions.push(condition_absent(
                        self.client.table(),
                        keys.reverse_partition,
                        keys.reverse_sort,
                    )?);
                }
                PairState::Missing => {
                    return Err(StorageError::new(
                        StorageErrorKind::Conflict,
                        "tuple_delete_missing",
                    )
                    .with_tuple(key.clone()));
                }
                PairState::Present { digest, tuple } => {
                    actions.push(delete_action(
                        self.client.table(),
                        keys.forward_partition,
                        keys.forward_sort,
                        digest.clone(),
                    )?);
                    actions.push(delete_action(
                        self.client.table(),
                        keys.reverse_partition,
                        keys.reverse_sort,
                        digest,
                    )?);
                    changes.push((ChangeOperation::Delete, *tuple));
                }
            }
        }
        for (tuple, keys) in writes.iter().zip(write_keys) {
            let key = tuple.key();
            let forward = states.get(&(keys.forward_partition.clone(), keys.forward_sort.clone()));
            let reverse = states.get(&(keys.reverse_partition.clone(), keys.reverse_sort.clone()));
            let state = validate_pair(forward, reverse, key)?;
            let new_forward = item::tuple_item(
                keys.forward_partition.clone(),
                keys.forward_sort.clone(),
                tuple,
                timestamp,
            )?;
            let new_reverse = item::tuple_item(
                keys.reverse_partition.clone(),
                keys.reverse_sort.clone(),
                tuple,
                timestamp,
            )?;
            let new_digest = item::binary(&new_forward, DIGEST)?.to_vec();
            match state {
                PairState::Missing => {
                    actions.push(put_action(
                        self.client.table(),
                        new_forward,
                        Some("attribute_not_exists(pk)"),
                    )?);
                    actions.push(put_action(
                        self.client.table(),
                        new_reverse,
                        Some("attribute_not_exists(pk)"),
                    )?);
                    changes.push((ChangeOperation::Write, tuple.clone()));
                }
                PairState::Present { digest, .. }
                    if options.on_duplicate_write() == WriteConflictPolicy::Ignore
                        && digest == new_digest =>
                {
                    actions.push(condition_digest(
                        self.client.table(),
                        keys.forward_partition,
                        keys.forward_sort,
                        digest.clone(),
                    )?);
                    actions.push(condition_digest(
                        self.client.table(),
                        keys.reverse_partition,
                        keys.reverse_sort,
                        digest,
                    )?);
                }
                PairState::Present { .. } => {
                    return Err(StorageError::new(
                        StorageErrorKind::Conflict,
                        "tuple_write_duplicate",
                    )
                    .with_tuple(key.clone()));
                }
            }
        }
        if changes.is_empty() {
            if !actions.is_empty() {
                require_transaction_size(&actions, states.values())?;
                execute_transaction(&self.client, context, actions, random_token()?).await?;
            }
            return Ok(MutationOutcome::new(Vec::new()));
        }
        let head_pk = change_head_partition(store_id);
        let head = self
            .get(context, head_pk.clone(), HEAD_SK.to_vec(), true)
            .await?;
        let previous = head
            .as_ref()
            .map(|value| item::string(value, LAST_CHANGE))
            .transpose()?;
        let ids = allocate_change_ids(previous, changes.len())?;
        let committed = changes
            .into_iter()
            .zip(ids.iter().copied())
            .map(|((operation, tuple), id)| {
                TupleChange::new(id, store_id, operation, tuple, timestamp)
            })
            .collect::<Vec<_>>();
        let last = ids.last().ok_or_else(|| {
            StorageError::new(StorageErrorKind::Internal, "dynamodb_change_id_missing")
        })?;
        actions.push(update_head_action(
            self.client.table(),
            head_pk,
            previous,
            *last,
        )?);
        let payload = item::encode_changes(&committed)?;
        let mut change_item = item::key(
            change_partition(store_id, last.to_string().as_bytes()),
            last.to_string().into_bytes(),
        );
        change_item.insert(KIND.to_owned(), AttributeValue::S("change".to_owned()));
        change_item.insert(PAYLOAD.to_owned(), AttributeValue::B(Blob::new(payload)));
        change_item.insert(
            TIMESTAMP.to_owned(),
            AttributeValue::N(item::epoch_millis(timestamp)?.to_string()),
        );
        actions.push(put_action(
            self.client.table(),
            change_item,
            Some("attribute_not_exists(pk)"),
        )?);
        require_transaction_size(&actions, states.values())?;
        execute_transaction(&self.client, context, actions, random_token()?).await?;
        Ok(MutationOutcome::new(ids))
    }
}

#[async_trait]
impl StoreReader for DynamoDbStorage {
    async fn read_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<StoreRecord, StorageError> {
        let raw = self
            .get(
                context,
                store_partition(store_id),
                store_id.to_string().into_bytes(),
                false,
            )
            .await?
            .ok_or_else(|| StorageError::new(StorageErrorKind::NotFound, "store_not_found"))?;
        decode_store(&raw, store_id)
    }

    async fn list_stores(
        &self,
        context: &OperationContext,
        filter: &StoreFilter,
        options: &PageOptions,
    ) -> Result<Page<StoreRecord>, StorageError> {
        let boundary = options
            .after()
            .map(|cursor| decode_cursor(CursorOperation::Store, cursor))
            .transpose()?
            .map(<[u8]>::to_vec);
        let per_shard_budget = MAXIMUM_PAGE_EVALUATIONS.div_ceil(usize::from(STORE_SHARDS));
        let mut tasks = JoinSet::new();
        for shard in 0..STORE_SHARDS {
            tasks.spawn(query_raw_window(
                self.client.clone(),
                context.clone(),
                format!("S#{shard:02x}"),
                boundary.clone(),
                true,
                per_shard_budget,
            ));
        }
        let mut windows = Vec::with_capacity(usize::from(STORE_SHARDS));
        while let Some(result) = tasks.join_next().await {
            windows.push(result.map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Internal,
                    "dynamodb_store_query_task_failed",
                    error,
                )
            })??);
        }
        let safe_frontier = windows
            .iter()
            .filter(|window| !window.exhausted)
            .filter_map(|window| window.frontier.as_deref())
            .min()
            .map(<[u8]>::to_vec);
        let mut stores = Vec::new();
        for window in windows {
            for raw in window.items {
                let sk = item::binary(&raw, item::SK)?.to_vec();
                if safe_frontier
                    .as_ref()
                    .is_some_and(|frontier| sk.as_slice() > frontier.as_slice())
                {
                    continue;
                }
                let id = std::str::from_utf8(item::binary(&raw, item::SK)?)
                    .map_err(|error| {
                        StorageError::with_source(
                            StorageErrorKind::Integrity,
                            "dynamodb_store_id_utf8",
                            error,
                        )
                    })?
                    .parse::<StoreId>()
                    .map_err(|error| {
                        StorageError::with_source(
                            StorageErrorKind::Integrity,
                            "dynamodb_store_id_invalid",
                            error,
                        )
                    })?;
                if id.to_string().as_bytes() != sk.as_slice() {
                    return Err(StorageError::new(
                        StorageErrorKind::Integrity,
                        "dynamodb_store_id_noncanonical",
                    ));
                }
                if item::string(&raw, item::PK)? != store_partition(id) {
                    return Err(StorageError::new(
                        StorageErrorKind::Integrity,
                        "dynamodb_store_shard_mismatch",
                    ));
                }
                let store = decode_store(&raw, id)?;
                if filter.name().is_none_or(|name| name == store.name()) {
                    stores.push((sk, store));
                }
            }
        }
        stores.sort_by(|left, right| left.0.cmp(&right.0));
        if stores.windows(2).any(|pair| {
            pair.first().map(|(_, store)| store.id()) == pair.get(1).map(|(_, store)| store.id())
        }) {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "dynamodb_store_duplicate_identity",
            ));
        }
        let full_page = stores.len() > options.maximum_results();
        stores.truncate(options.maximum_results());
        let continuation_key = if full_page {
            stores.last().map(|(key, _)| key.as_slice())
        } else {
            safe_frontier.as_deref()
        };
        let continuation = continuation_key
            .map(|key| encode_cursor(CursorOperation::Store, key))
            .transpose()?;
        Ok(Page::new(
            stores.into_iter().map(|(_, store)| store).collect(),
            continuation,
        ))
    }
}

#[async_trait]
impl StoreWriter for DynamoDbStorage {
    async fn create_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError> {
        let timestamp = SystemTime::now();
        let record = StoreRecord::new(store_id, name, timestamp);
        let mut raw = item::key(store_partition(store_id), store_id.to_string().into_bytes());
        raw.insert(KIND.to_owned(), AttributeValue::S("store".to_owned()));
        raw.insert(
            NAME.to_owned(),
            AttributeValue::S(record.name().as_str().to_owned()),
        );
        raw.insert(
            CREATED_AT.to_owned(),
            AttributeValue::N(item::epoch_millis(timestamp)?.to_string()),
        );
        raw.insert(
            UPDATED_AT.to_owned(),
            AttributeValue::N(item::epoch_millis(timestamp)?.to_string()),
        );
        raw.insert(STATE.to_owned(), AttributeValue::S(STATE_ACTIVE.to_owned()));
        self.put_item(
            context,
            raw,
            Some("attribute_not_exists(pk)"),
            "store_already_exists",
        )
        .await?;
        Ok(record)
    }

    async fn rename_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError> {
        let current = self.read_store(context, store_id).await?;
        let timestamp = SystemTime::now();
        let values = HashMap::from([
            (
                ":name".to_owned(),
                AttributeValue::S(name.as_str().to_owned()),
            ),
            (
                ":updated".to_owned(),
                AttributeValue::N(item::epoch_millis(timestamp)?.to_string()),
            ),
            (
                ":active".to_owned(),
                AttributeValue::S(STATE_ACTIVE.to_owned()),
            ),
        ]);
        let result = self
            .client
            .execute(
                context,
                "dynamodb_store_rename_failed",
                self.client
                    .sdk()
                    .update_item()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .table_name(self.client.table())
                    .set_key(Some(item::key(
                        store_partition(store_id),
                        store_id.to_string().into_bytes(),
                    )))
                    .update_expression("SET n = :name, ua = :updated")
                    .condition_expression("st = :active")
                    .set_expression_attribute_values(Some(values))
                    .send(),
            )
            .await;
        match result {
            Ok(_) => Ok(current.renamed(name, timestamp)),
            Err(error) if error.kind() == StorageErrorKind::Conflict => Err(StorageError::new(
                StorageErrorKind::NotFound,
                "store_not_found",
            )),
            Err(error) => Err(error),
        }
    }

    async fn delete_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<(), StorageError> {
        self.client
            .execute(
                context,
                "dynamodb_store_delete_failed",
                self.client
                    .sdk()
                    .delete_item()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .table_name(self.client.table())
                    .set_key(Some(item::key(
                        store_partition(store_id),
                        store_id.to_string().into_bytes(),
                    )))
                    .send(),
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ModelWriter for DynamoDbStorage {
    async fn write_model(
        &self,
        context: &OperationContext,
        model: Arc<StoredAuthorizationModel>,
    ) -> Result<(), StorageError> {
        let payload = encode_model(&model)?;
        if payload.len() > MAXIMUM_MODEL_PAYLOAD_BYTES {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "model_payload_limit",
            ));
        }
        let identity = model.model_id().to_string();
        write_immutable_blob(
            &self.client,
            context,
            model_partition(*model.store_id()),
            identity.as_bytes().to_vec(),
            "model",
            &payload,
            model.written_at(),
            SystemTime::now(),
            self.config.garbage_collection.grace_period,
        )
        .await
    }
}

#[async_trait]
impl ModelReader for DynamoDbStorage {
    async fn read_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        let (payload, written_at) = read_blob(
            self,
            context,
            model_partition(store_id),
            model_id.to_string().into_bytes(),
            "model",
        )
        .await?
        .ok_or_else(|| StorageError::new(StorageErrorKind::NotFound, "model_not_found"))?;
        decode_model(&payload, store_id, model_id, written_at, &self.compiler)
    }

    async fn read_latest_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        let page = self
            .list_models(
                context,
                store_id,
                &PageOptions::new(
                    NonZeroU32::MIN,
                    None,
                    &openfga_domain::InputLimits::default(),
                )?,
            )
            .await?;
        page.into_items()
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::new(StorageErrorKind::NotFound, "model_not_found"))
    }

    async fn list_models(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        options: &PageOptions,
    ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError> {
        let boundary = options
            .after()
            .map(|cursor| decode_cursor(CursorOperation::Model, cursor))
            .transpose()?
            .map(<[u8]>::to_vec);
        let window = query_raw_window(
            self.client.clone(),
            context.clone(),
            model_partition(store_id),
            boundary,
            false,
            MAXIMUM_PAGE_EVALUATIONS,
        )
        .await?;
        let safe_frontier = (!window.exhausted)
            .then(|| window.frontier.clone())
            .flatten();
        let mut ids = Vec::new();
        for raw in window.items {
            let sk = item::binary(&raw, item::SK)?;
            if safe_frontier
                .as_ref()
                .is_some_and(|frontier| sk < frontier.as_slice())
            {
                continue;
            }
            if item::string(&raw, KIND)? != "model" {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_model_manifest_kind_invalid",
                ));
            }
            let state = item::string(&raw, STATE)?;
            if !matches!(state, STATE_COMMITTED | STATE_STAGING | "DELETING") {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_model_manifest_state_invalid",
                ));
            }
            if state != STATE_COMMITTED {
                continue;
            }
            let id = std::str::from_utf8(sk)
                .map_err(|error| {
                    StorageError::with_source(
                        StorageErrorKind::Integrity,
                        "dynamodb_model_id_utf8",
                        error,
                    )
                })?
                .parse::<AuthorizationModelId>()
                .map_err(|error| {
                    StorageError::with_source(
                        StorageErrorKind::Integrity,
                        "dynamodb_model_id_invalid",
                        error,
                    )
                })?;
            if id.to_string().as_bytes() != sk {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_model_id_noncanonical",
                ));
            }
            ids.push(id);
        }
        ids.sort_by(|left, right| right.cmp(left));
        let full_page = ids.len() > options.maximum_results();
        ids.truncate(options.maximum_results());
        let mut models = Vec::with_capacity(ids.len());
        for id in &ids {
            models.push(self.read_model(context, store_id, *id).await?);
        }
        let emitted_key = ids.last().map(ToString::to_string);
        let continuation_key = if full_page {
            emitted_key.as_deref().map(str::as_bytes)
        } else {
            safe_frontier.as_deref()
        };
        let continuation = continuation_key
            .map(|key| encode_cursor(CursorOperation::Model, key))
            .transpose()?;
        Ok(Page::new(models, continuation))
    }
}

#[async_trait]
impl AssertionWriter for DynamoDbStorage {
    async fn write_assertions(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
        assertions: Vec<Assertion>,
    ) -> Result<(), StorageError> {
        self.read_model(context, store_id, model_id).await?;
        let payload = encode_assertions(&assertions)?;
        if payload.len() > MAXIMUM_ASSERTION_PAYLOAD_BYTES {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "assertion_payload_limit",
            ));
        }
        write_replaceable_blob(
            &self.client,
            context,
            assertion_partition(store_id, model_id),
            "assertion",
            &payload,
            SystemTime::now(),
            self.config.garbage_collection.grace_period,
            self.config.garbage_collection.assertion_retention,
            self.config.maximum_conflict_retries,
        )
        .await
    }
}

#[async_trait]
impl AssertionReader for DynamoDbStorage {
    async fn read_assertions(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<[Assertion]>, StorageError> {
        self.read_model(context, store_id, model_id).await?;
        let pk = assertion_partition(store_id, model_id);
        let Some(head) = self
            .get(context, pk.clone(), HEAD_SK.to_vec(), true)
            .await?
        else {
            return Ok(Arc::from([]));
        };
        let generation = item::string(&head, GENERATION)?.as_bytes().to_vec();
        let (payload, _) = read_blob(self, context, pk, generation, "assertion")
            .await?
            .ok_or_else(|| {
                StorageError::new(StorageErrorKind::Integrity, "assertion_generation_missing")
            })?;
        decode_assertions(&payload)
    }
}

#[async_trait]
impl ChangeReader for DynamoDbStorage {
    async fn read_changes(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ChangeFilter,
        options: &PageOptions,
    ) -> Result<Page<TupleChange>, StorageError> {
        let after = options
            .after()
            .map(|cursor| {
                std::str::from_utf8(cursor.as_bytes())
                    .map_err(|error| {
                        StorageError::with_source(
                            StorageErrorKind::InvalidContinuation,
                            "change_cursor_utf8",
                            error,
                        )
                    })?
                    .parse::<ChangeId>()
                    .map_err(|error| {
                        StorageError::with_source(
                            StorageErrorKind::InvalidContinuation,
                            "change_cursor_invalid",
                            error,
                        )
                    })
            })
            .transpose()?;
        let boundary = after.map(|id| id.to_string().into_bytes());
        let per_shard_budget = MAXIMUM_PAGE_EVALUATIONS.div_ceil(usize::from(key::CHANGE_SHARDS));
        let mut tasks = JoinSet::new();
        for shard in 0..key::CHANGE_SHARDS {
            tasks.spawn(query_raw_window(
                self.client.clone(),
                context.clone(),
                format!("C#{store_id}#{shard:02x}"),
                boundary.clone(),
                true,
                per_shard_budget,
            ));
        }
        let mut windows = Vec::with_capacity(usize::from(key::CHANGE_SHARDS));
        while let Some(result) = tasks.join_next().await {
            windows.push(result.map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Internal,
                    "dynamodb_change_query_task_failed",
                    error,
                )
            })??);
        }
        let safe_frontier = windows
            .iter()
            .filter(|window| !window.exhausted)
            .filter_map(|window| window.frontier.as_deref())
            .min()
            .map(<[u8]>::to_vec);
        let mut changes = Vec::new();
        for window in windows {
            for raw in window.items {
                let sk = item::binary(&raw, item::SK)?;
                if safe_frontier
                    .as_ref()
                    .is_some_and(|frontier| sk > frontier.as_slice())
                {
                    continue;
                }
                if item::string(&raw, KIND)? != "change" {
                    return Err(StorageError::new(
                        StorageErrorKind::Integrity,
                        "dynamodb_change_kind_invalid",
                    ));
                }
                let decoded = item::decode_changes(item::binary(&raw, PAYLOAD)?, store_id)?;
                if decoded
                    .last()
                    .is_none_or(|change| change.id().to_string().as_bytes() != sk)
                {
                    return Err(StorageError::new(
                        StorageErrorKind::Integrity,
                        "dynamodb_change_batch_identity_mismatch",
                    ));
                }
                if item::string(&raw, item::PK)? != change_partition(store_id, sk) {
                    return Err(StorageError::new(
                        StorageErrorKind::Integrity,
                        "dynamodb_change_shard_mismatch",
                    ));
                }
                changes.extend(decoded);
            }
        }
        changes.retain(|change| {
            after.is_none_or(|id| change.id() > id)
                && filter
                    .object_type()
                    .is_none_or(|kind| change.tuple().key().object().object_type() == kind)
                && filter
                    .start_time()
                    .is_none_or(|time| change.timestamp() >= time)
        });
        changes.sort_by_key(TupleChange::id);
        if changes
            .windows(2)
            .any(|pair| pair.first().map(TupleChange::id) == pair.get(1).map(TupleChange::id))
        {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "dynamodb_change_duplicate_identity",
            ));
        }
        if let Some(frontier) = &safe_frontier {
            let frontier = std::str::from_utf8(frontier)
                .ok()
                .and_then(|value| value.parse::<ChangeId>().ok())
                .ok_or_else(|| {
                    StorageError::new(
                        StorageErrorKind::Integrity,
                        "dynamodb_change_frontier_invalid",
                    )
                })?;
            changes.retain(|change| change.id() <= frontier);
        }
        let full_page = changes.len() > options.maximum_results();
        changes.truncate(options.maximum_results());
        let continuation = if full_page {
            changes
                .last()
                .map(|change| StorageCursor::new(change.id().to_string().into_bytes()))
                .transpose()?
        } else {
            safe_frontier.map(StorageCursor::new).transpose()?
        };
        Ok(Page::new(changes, continuation))
    }
}

#[async_trait]
impl HealthCheck for DynamoDbStorage {
    async fn health(&self, context: &OperationContext) -> Result<HealthStatus, StorageError> {
        if !self.runtime_diagnostics.is_running() {
            self.client.record_readiness("gc_not_running");
            return Ok(HealthStatus::new(false, "dynamodb_gc_not_running"));
        }
        if self.runtime_diagnostics.consecutive_failures() >= 3 {
            self.client.record_readiness("gc_repeated_failure");
            return Ok(HealthStatus::new(false, "dynamodb_gc_repeated_failure"));
        }
        let maximum_lag_millis =
            u64::try_from(self.config.garbage_collection.maximum_work_lag.as_millis())
                .unwrap_or(u64::MAX);
        if self.runtime_diagnostics.overdue_work_lag_millis() > maximum_lag_millis {
            self.client.record_readiness("gc_lag_exceeded");
            return Ok(HealthStatus::new(false, "dynamodb_gc_lag_exceeded"));
        }
        match migration::status(&self.client, context).await? {
            DynamoDbProvisioningStatus::Ready => {
                self.client.record_readiness("ready");
                Ok(HealthStatus::new(true, "dynamodb_ready"))
            }
            DynamoDbProvisioningStatus::Missing => {
                self.client.record_readiness("table_missing");
                Ok(HealthStatus::new(false, "dynamodb_table_missing"))
            }
            DynamoDbProvisioningStatus::Transitioning => {
                self.client.record_readiness("table_transitioning");
                Ok(HealthStatus::new(false, "dynamodb_table_transitioning"))
            }
            DynamoDbProvisioningStatus::Incompatible => {
                self.client.record_readiness("schema_incompatible");
                Ok(HealthStatus::new(false, "dynamodb_schema_incompatible"))
            }
        }
    }
}

enum PairState {
    Missing,
    Present {
        digest: Vec<u8>,
        tuple: Box<RelationshipTuple>,
    },
}

fn validate_pair(
    forward: Option<&Item>,
    reverse: Option<&Item>,
    key: &TupleKey,
) -> Result<PairState, StorageError> {
    match (forward, reverse) {
        (None, None) => Ok(PairState::Missing),
        (Some(left), Some(right)) => {
            let stored = item::require_tuple_identity(left, key)?;
            item::require_tuple_identity(right, key)?;
            let left_digest = item::binary(left, DIGEST)?;
            let right_digest = item::binary(right, DIGEST)?;
            if left_digest != right_digest
                || item::binary(left, PAYLOAD)? != item::binary(right, PAYLOAD)?
            {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_tuple_peer_mismatch",
                ));
            }
            Ok(PairState::Present {
                digest: left_digest.to_vec(),
                tuple: Box::new(stored.into_tuple()),
            })
        }
        _ => Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_tuple_peer_missing",
        )),
    }
}

fn validate_mutation(
    deletes: &[TupleKey],
    writes: &[RelationshipTuple],
    maximum: u32,
) -> Result<(), StorageError> {
    let total = deletes.len().checked_add(writes.len()).ok_or_else(|| {
        StorageError::new(
            StorageErrorKind::ResourceExhausted,
            "dynamodb_mutation_count_overflow",
        )
    })?;
    if total > maximum as usize {
        return Err(StorageError::new(
            StorageErrorKind::ResourceExhausted,
            "dynamodb_mutation_limit",
        ));
    }
    let delete_keys = deletes.iter().collect::<BTreeSet<_>>();
    let write_keys = writes
        .iter()
        .map(RelationshipTuple::key)
        .collect::<BTreeSet<_>>();
    if delete_keys.len() != deletes.len()
        || write_keys.len() != writes.len()
        || !delete_keys.is_disjoint(&write_keys)
    {
        return Err(StorageError::new(
            StorageErrorKind::Conflict,
            "dynamodb_mutation_duplicate_key",
        ));
    }
    Ok(())
}

fn put_action(
    table: &str,
    item: Item,
    condition: Option<&str>,
) -> Result<TransactWriteItem, StorageError> {
    item::require_item_limit(&item)?;
    let put = Put::builder()
        .table_name(table)
        .set_item(Some(item))
        .set_condition_expression(condition.map(str::to_owned))
        .build()
        .map_err(request_build_error)?;
    Ok(TransactWriteItem::builder().put(put).build())
}

fn delete_action(
    table: &str,
    pk: String,
    sk: Vec<u8>,
    digest: Vec<u8>,
) -> Result<TransactWriteItem, StorageError> {
    let delete = Delete::builder()
        .table_name(table)
        .set_key(Some(item::key(pk, sk)))
        .condition_expression("d = :digest")
        .expression_attribute_values(":digest", AttributeValue::B(Blob::new(digest)))
        .build()
        .map_err(request_build_error)?;
    Ok(TransactWriteItem::builder().delete(delete).build())
}

fn delete_key_action(
    table: &str,
    pk: String,
    sk: Vec<u8>,
) -> Result<TransactWriteItem, StorageError> {
    let delete = Delete::builder()
        .table_name(table)
        .set_key(Some(item::key(pk, sk)))
        .build()
        .map_err(request_build_error)?;
    Ok(TransactWriteItem::builder().delete(delete).build())
}

fn condition_absent(
    table: &str,
    pk: String,
    sk: Vec<u8>,
) -> Result<TransactWriteItem, StorageError> {
    let check = ConditionCheck::builder()
        .table_name(table)
        .set_key(Some(item::key(pk, sk)))
        .condition_expression("attribute_not_exists(pk)")
        .build()
        .map_err(request_build_error)?;
    Ok(TransactWriteItem::builder().condition_check(check).build())
}

fn condition_digest(
    table: &str,
    pk: String,
    sk: Vec<u8>,
    digest: Vec<u8>,
) -> Result<TransactWriteItem, StorageError> {
    let check = ConditionCheck::builder()
        .table_name(table)
        .set_key(Some(item::key(pk, sk)))
        .condition_expression("d = :digest")
        .expression_attribute_values(":digest", AttributeValue::B(Blob::new(digest)))
        .build()
        .map_err(request_build_error)?;
    Ok(TransactWriteItem::builder().condition_check(check).build())
}

fn update_head_action(
    table: &str,
    pk: String,
    previous: Option<&str>,
    next: ChangeId,
) -> Result<TransactWriteItem, StorageError> {
    let mut builder = Update::builder()
        .table_name(table)
        .set_key(Some(item::key(pk, HEAD_SK.to_vec())))
        .update_expression("SET lc = :next")
        .expression_attribute_values(":next", AttributeValue::S(next.to_string()));
    builder = if let Some(previous) = previous {
        builder
            .condition_expression("lc = :previous")
            .expression_attribute_values(":previous", AttributeValue::S(previous.to_owned()))
    } else {
        builder.condition_expression("attribute_not_exists(lc)")
    };
    let update = builder.build().map_err(request_build_error)?;
    Ok(TransactWriteItem::builder().update(update).build())
}

async fn execute_transaction(
    client: &DynamoClient,
    context: &OperationContext,
    actions: Vec<TransactWriteItem>,
    token: String,
) -> Result<(), StorageError> {
    require_transaction_size(&actions, std::iter::empty())?;
    let mut jitter = [0_u8; 1];
    getrandom::fill(&mut jitter).map_err(|error| {
        StorageError::with_source(StorageErrorKind::Internal, "dynamodb_random_failed", error)
    })?;
    let mut attempt = 0_u32;
    loop {
        match client
            .execute_transaction(
                context,
                "dynamodb_transaction_failed",
                client
                    .sdk()
                    .transact_write_items()
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .set_transact_items(Some(actions.clone()))
                    .client_request_token(token.clone())
                    .send(),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == StorageErrorKind::Timeout => {
                context.check()?;
                operation_backoff(
                    context,
                    attempt,
                    jitter[0],
                    "dynamodb_idempotent_retry_cancelled",
                    "dynamodb_idempotent_retry_timed_out",
                )
                .await?;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

fn require_transaction_size<'a>(
    actions: &[TransactWriteItem],
    mut current_items: impl Iterator<Item = &'a Item>,
) -> Result<(), StorageError> {
    let current_size = current_items.try_fold(0_usize, |total, value| {
        checked_size_add(total, item::encoded_item_size(value)?)
    })?;
    let total = actions.iter().try_fold(current_size, |size, action| {
        checked_size_add(size, transaction_action_size(action)?)
    })?;
    if total > TRANSACTION_ITEM_BYTES {
        return Err(StorageError::new(
            StorageErrorKind::ResourceExhausted,
            "dynamodb_transaction_too_large",
        ));
    }
    Ok(())
}

fn transaction_action_size(action: &TransactWriteItem) -> Result<usize, StorageError> {
    if let Some(put) = action.put() {
        return operation_size(
            put.table_name(),
            put.item(),
            put.condition_expression().into_iter(),
            put.expression_attribute_names(),
            put.expression_attribute_values(),
        );
    }
    if let Some(delete) = action.delete() {
        return operation_size(
            delete.table_name(),
            delete.key(),
            delete.condition_expression().into_iter(),
            delete.expression_attribute_names(),
            delete.expression_attribute_values(),
        );
    }
    if let Some(update) = action.update() {
        return operation_size(
            update.table_name(),
            update.key(),
            update
                .condition_expression()
                .into_iter()
                .chain(Some(update.update_expression())),
            update.expression_attribute_names(),
            update.expression_attribute_values(),
        );
    }
    if let Some(check) = action.condition_check() {
        return operation_size(
            check.table_name(),
            check.key(),
            Some(check.condition_expression()).into_iter(),
            check.expression_attribute_names(),
            check.expression_attribute_values(),
        );
    }
    Err(StorageError::new(
        StorageErrorKind::Integrity,
        "dynamodb_transaction_action_invalid",
    ))
}

fn operation_size<'a>(
    table: &str,
    values: &Item,
    expressions: impl Iterator<Item = &'a str>,
    names: Option<&HashMap<String, String>>,
    expression_values: Option<&HashMap<String, AttributeValue>>,
) -> Result<usize, StorageError> {
    let mut total = checked_size_add(table.len(), item::encoded_item_size(values)?)?;
    for expression in expressions {
        total = checked_size_add(total, expression.len())?;
    }
    if let Some(names) = names {
        for (name, value) in names {
            total = checked_size_add(total, name.len())?;
            total = checked_size_add(total, value.len())?;
        }
    }
    if let Some(values) = expression_values {
        for (name, value) in values {
            total = checked_size_add(total, name.len())?;
            total = checked_size_add(total, item::attribute_value_size(value))?;
        }
    }
    checked_size_add(total, 64)
}

fn checked_size_add(left: usize, right: usize) -> Result<usize, StorageError> {
    left.checked_add(right).ok_or_else(|| {
        StorageError::new(
            StorageErrorKind::ResourceExhausted,
            "dynamodb_transaction_size_overflow",
        )
    })
}

fn allocate_change_ids(
    previous: Option<&str>,
    count: usize,
) -> Result<Vec<ChangeId>, StorageError> {
    let now = item::epoch_millis(SystemTime::now())?;
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|error| {
        StorageError::with_source(StorageErrorKind::Internal, "dynamodb_random_failed", error)
    })?;
    let mut candidate = Ulid::from_parts(now, u128::from_be_bytes(random));
    if let Some(previous) = previous {
        let previous = previous.parse::<Ulid>().map_err(|_| {
            StorageError::new(StorageErrorKind::Integrity, "dynamodb_change_head_invalid")
        })?;
        if candidate <= previous {
            candidate = previous.increment().map_err(|_| {
                StorageError::new(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_change_id_exhausted",
                )
            })?;
        }
    }
    let mut ids = Vec::with_capacity(count);
    for offset in 0..count {
        if offset > 0 {
            candidate = candidate.increment().map_err(|_| {
                StorageError::new(
                    StorageErrorKind::ResourceExhausted,
                    "dynamodb_change_id_exhausted",
                )
            })?;
        }
        ids.push(ChangeId::from_ulid(candidate));
    }
    Ok(ids)
}

fn random_token() -> Result<String, StorageError> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|error| {
        StorageError::with_source(StorageErrorKind::Internal, "dynamodb_random_failed", error)
    })?;
    Ok(Ulid::from(random).to_string())
}

fn decode_store(raw: &Item, expected_id: StoreId) -> Result<StoreRecord, StorageError> {
    if item::string(raw, KIND)? != "store" || item::string(raw, STATE)? != STATE_ACTIVE {
        return Err(StorageError::new(
            StorageErrorKind::NotFound,
            "store_not_found",
        ));
    }
    let name = StoreName::new(item::string(raw, NAME)?.to_owned())?;
    let created = item::system_time(item::number_u64(raw, CREATED_AT)?)?;
    let updated = item::system_time(item::number_u64(raw, UPDATED_AT)?)?;
    let record = StoreRecord::new(expected_id, name, created);
    Ok(if updated == created {
        record
    } else {
        record.renamed(record.name().clone(), updated)
    })
}

fn require_physical_identity(
    raw: &Item,
    expected_pk: &str,
    expected_sk: &[u8],
) -> Result<(), StorageError> {
    if item::string(raw, item::PK)? != expected_pk || item::binary(raw, item::SK)? != expected_sk {
        return Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_item_identity_mismatch",
        ));
    }
    Ok(())
}

fn decode_forward_item(raw: &Item) -> Result<StoredTuple, StorageError> {
    let stored = item::decode_stored_tuple(raw)?;
    if key::decode_forward(item::binary(raw, item::SK)?)? != *stored.tuple().key() {
        return Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_forward_key_payload_mismatch",
        ));
    }
    Ok(stored)
}

async fn write_immutable_blob(
    client: &DynamoClient,
    context: &OperationContext,
    manifest_pk: String,
    manifest_sk: Vec<u8>,
    kind: &str,
    payload: &[u8],
    written_at: SystemTime,
    publication_started: SystemTime,
    grace_period: std::time::Duration,
) -> Result<(), StorageError> {
    let generation = random_token()?;
    write_blob_generation(
        client,
        context,
        &manifest_pk,
        &manifest_sk,
        &generation,
        kind,
        payload,
        written_at,
        publication_started,
        grace_period,
        grace_period,
        true,
        false,
        NonZeroU32::MIN,
    )
    .await
}

async fn write_replaceable_blob(
    client: &DynamoClient,
    context: &OperationContext,
    pk: String,
    kind: &str,
    payload: &[u8],
    written_at: SystemTime,
    grace_period: std::time::Duration,
    assertion_retention: std::time::Duration,
    conflict_retries: NonZeroU32,
) -> Result<(), StorageError> {
    let generation = random_token()?;
    write_blob_generation(
        client,
        context,
        &pk,
        generation.as_bytes(),
        &generation,
        kind,
        payload,
        written_at,
        written_at,
        grace_period,
        assertion_retention,
        false,
        true,
        conflict_retries,
    )
    .await
}

async fn write_blob_generation(
    client: &DynamoClient,
    context: &OperationContext,
    manifest_pk: &str,
    manifest_sk: &[u8],
    generation: &str,
    kind: &str,
    payload: &[u8],
    written_at: SystemTime,
    publication_started: SystemTime,
    grace_period: std::time::Duration,
    assertion_retention: std::time::Duration,
    immutable: bool,
    replace_head: bool,
    conflict_retries: NonZeroU32,
) -> Result<(), StorageError> {
    let chunks = payload.chunks(CHUNK_BYTES).collect::<Vec<_>>();
    let digest = Sha256::digest(payload);
    let mut manifest = item::key(manifest_pk.to_owned(), manifest_sk.to_vec());
    manifest.insert(KIND.to_owned(), AttributeValue::S(kind.to_owned()));
    manifest.insert(
        STATE.to_owned(),
        AttributeValue::S(STATE_STAGING.to_owned()),
    );
    manifest.insert(
        GENERATION.to_owned(),
        AttributeValue::S(generation.to_owned()),
    );
    manifest.insert(
        CHUNK_COUNT.to_owned(),
        AttributeValue::N(chunks.len().to_string()),
    );
    manifest.insert(
        PAYLOAD_BYTES.to_owned(),
        AttributeValue::N(payload.len().to_string()),
    );
    manifest.insert(
        DIGEST.to_owned(),
        AttributeValue::B(Blob::new(digest.to_vec())),
    );
    manifest.insert(
        TIMESTAMP.to_owned(),
        AttributeValue::N(item::epoch_millis(written_at)?.to_string()),
    );
    let blob_pk = format!("B#{kind}#{manifest_pk}#{generation}");
    let garbage_collection = garbage_collection_work(
        manifest_pk,
        manifest_sk,
        generation,
        &blob_pk,
        publication_started,
        grace_period,
    )?;
    let garbage_partition = item::string(&garbage_collection, item::PK)?.to_owned();
    let garbage_sort = item::binary(&garbage_collection, item::SK)?.to_vec();
    let result = execute_transaction(
        client,
        context,
        vec![
            put_action(
                client.table(),
                manifest,
                immutable.then_some("attribute_not_exists(pk)"),
            )?,
            put_action(
                client.table(),
                garbage_collection,
                Some("attribute_not_exists(pk)"),
            )?,
        ],
        random_token()?,
    )
    .await;
    if let Err(error) = result {
        return if error.kind() == StorageErrorKind::Conflict {
            Err(StorageError::new(
                StorageErrorKind::AlreadyExists,
                "blob_already_exists",
            ))
        } else {
            Err(error)
        };
    }
    for (index, chunk) in chunks.iter().enumerate() {
        let index = u32::try_from(index).map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::ResourceExhausted,
                "dynamodb_chunk_index",
                error,
            )
        })?;
        let mut chunk_item = item::key(blob_pk.clone(), index.to_be_bytes().to_vec());
        chunk_item.insert(KIND.to_owned(), AttributeValue::S("chunk".to_owned()));
        chunk_item.insert(PAYLOAD.to_owned(), AttributeValue::B(Blob::new(*chunk)));
        let check = ConditionCheck::builder()
            .table_name(client.table())
            .set_key(Some(item::key(
                manifest_pk.to_owned(),
                manifest_sk.to_vec(),
            )))
            .condition_expression("st = :staging AND g = :generation")
            .expression_attribute_values(":staging", AttributeValue::S(STATE_STAGING.to_owned()))
            .expression_attribute_values(":generation", AttributeValue::S(generation.to_owned()))
            .build()
            .map_err(request_build_error)?;
        execute_transaction(
            client,
            context,
            vec![
                TransactWriteItem::builder().condition_check(check).build(),
                put_action(client.table(), chunk_item, Some("attribute_not_exists(pk)"))?,
            ],
            random_token()?,
        )
        .await?;
    }
    if replace_head {
        commit_replaceable_blob(
            client,
            context,
            manifest_pk,
            manifest_sk,
            generation,
            kind,
            garbage_partition,
            garbage_sort,
            assertion_retention,
            conflict_retries,
        )
        .await?;
    } else {
        commit_immutable_blob(
            client,
            context,
            manifest_pk,
            manifest_sk,
            generation,
            garbage_partition,
            garbage_sort,
        )
        .await?;
    }
    client.record_blob_work("write", payload.len(), chunks.len());
    Ok(())
}

async fn commit_immutable_blob(
    client: &DynamoClient,
    context: &OperationContext,
    manifest_partition: &str,
    manifest_sort: &[u8],
    generation: &str,
    garbage_partition: String,
    garbage_sort: Vec<u8>,
) -> Result<(), StorageError> {
    let update = commit_manifest_update(client, manifest_partition, manifest_sort, generation)?;
    execute_transaction(
        client,
        context,
        vec![
            TransactWriteItem::builder().update(update).build(),
            delete_key_action(client.table(), garbage_partition, garbage_sort)?,
        ],
        random_token()?,
    )
    .await
}

async fn commit_replaceable_blob(
    client: &DynamoClient,
    context: &OperationContext,
    manifest_partition: &str,
    manifest_sort: &[u8],
    generation: &str,
    kind: &str,
    garbage_partition: String,
    garbage_sort: Vec<u8>,
    grace_period: Duration,
    conflict_retries: NonZeroU32,
) -> Result<(), StorageError> {
    let mut jitter = [0_u8; 1];
    getrandom::fill(&mut jitter).map_err(|error| {
        StorageError::with_source(StorageErrorKind::Internal, "dynamodb_random_failed", error)
    })?;
    for attempt in 0..conflict_retries.get() {
        let previous = read_generation_head(client, context, manifest_partition).await?;
        let manifest_update =
            commit_manifest_update(client, manifest_partition, manifest_sort, generation)?;
        let head_update =
            generation_head_update(client, manifest_partition, previous.as_deref(), generation)?;
        let mut actions = vec![
            TransactWriteItem::builder().update(manifest_update).build(),
            TransactWriteItem::builder().update(head_update).build(),
            delete_key_action(
                client.table(),
                garbage_partition.clone(),
                garbage_sort.clone(),
            )?,
        ];
        if let Some(previous) = &previous {
            let retired_update =
                retire_manifest_update(client, manifest_partition, previous.as_bytes(), previous)?;
            let old_blob_partition = format!("B#{kind}#{manifest_partition}#{previous}");
            let old_work = garbage_collection_work(
                manifest_partition,
                previous.as_bytes(),
                previous,
                &old_blob_partition,
                SystemTime::now(),
                grace_period,
            )?;
            actions.push(TransactWriteItem::builder().update(retired_update).build());
            actions.push(put_action(
                client.table(),
                old_work,
                Some("attribute_not_exists(pk)"),
            )?);
        }
        match execute_transaction(client, context, actions, random_token()?).await {
            Ok(()) => return Ok(()),
            Err(error)
                if error.kind() == StorageErrorKind::Conflict
                    && error.code() == "dynamodb_transaction_failed" =>
            {
                client.record_head_retry("assertion");
                operation_backoff(
                    context,
                    attempt,
                    jitter[0],
                    "dynamodb_assertion_retry_cancelled",
                    "dynamodb_assertion_retry_timed_out",
                )
                .await?;
            }
            Err(error) => return Err(error),
        }
    }
    Err(StorageError::new(
        StorageErrorKind::Conflict,
        "dynamodb_assertion_head_retry_exhausted",
    ))
}

fn commit_manifest_update(
    client: &DynamoClient,
    manifest_partition: &str,
    manifest_sort: &[u8],
    generation: &str,
) -> Result<Update, StorageError> {
    Update::builder()
        .table_name(client.table())
        .set_key(Some(item::key(
            manifest_partition.to_owned(),
            manifest_sort.to_vec(),
        )))
        .update_expression("SET st = :committed")
        .condition_expression("st = :staging AND g = :generation")
        .expression_attribute_values(":committed", AttributeValue::S(STATE_COMMITTED.to_owned()))
        .expression_attribute_values(":staging", AttributeValue::S(STATE_STAGING.to_owned()))
        .expression_attribute_values(":generation", AttributeValue::S(generation.to_owned()))
        .build()
        .map_err(request_build_error)
}

fn generation_head_update(
    client: &DynamoClient,
    manifest_partition: &str,
    previous: Option<&str>,
    generation: &str,
) -> Result<Update, StorageError> {
    let mut update = Update::builder()
        .table_name(client.table())
        .set_key(Some(item::key(
            manifest_partition.to_owned(),
            HEAD_SK.to_vec(),
        )))
        .update_expression("SET g = :generation")
        .expression_attribute_values(":generation", AttributeValue::S(generation.to_owned()));
    update = if let Some(previous) = previous {
        update
            .condition_expression("g = :previous")
            .expression_attribute_values(":previous", AttributeValue::S(previous.to_owned()))
    } else {
        update.condition_expression("attribute_not_exists(g)")
    };
    update.build().map_err(request_build_error)
}

fn retire_manifest_update(
    client: &DynamoClient,
    manifest_partition: &str,
    manifest_sort: &[u8],
    generation: &str,
) -> Result<Update, StorageError> {
    Update::builder()
        .table_name(client.table())
        .set_key(Some(item::key(
            manifest_partition.to_owned(),
            manifest_sort.to_vec(),
        )))
        .update_expression("SET st = :retired")
        .condition_expression("st = :committed AND g = :generation")
        .expression_attribute_values(":retired", AttributeValue::S("RETIRED".to_owned()))
        .expression_attribute_values(":committed", AttributeValue::S(STATE_COMMITTED.to_owned()))
        .expression_attribute_values(":generation", AttributeValue::S(generation.to_owned()))
        .build()
        .map_err(request_build_error)
}

async fn read_generation_head(
    client: &DynamoClient,
    context: &OperationContext,
    manifest_partition: &str,
) -> Result<Option<String>, StorageError> {
    let output = client
        .execute(
            context,
            "dynamodb_assertion_head_read_failed",
            client
                .sdk()
                .get_item()
                .return_consumed_capacity(ReturnConsumedCapacity::Total)
                .table_name(client.table())
                .set_key(Some(item::key(
                    manifest_partition.to_owned(),
                    HEAD_SK.to_vec(),
                )))
                .consistent_read(true)
                .send(),
        )
        .await?;
    output
        .item
        .as_ref()
        .map(|head| item::string(head, GENERATION).map(str::to_owned))
        .transpose()
}

fn garbage_collection_work(
    manifest_pk: &str,
    manifest_sk: &[u8],
    generation: &str,
    target_pk: &str,
    written_at: SystemTime,
    grace_period: std::time::Duration,
) -> Result<Item, StorageError> {
    let due = written_at.checked_add(grace_period).ok_or_else(|| {
        StorageError::new(
            StorageErrorKind::ResourceExhausted,
            "dynamodb_gc_due_overflow",
        )
    })?;
    let due_millis = item::epoch_millis(due)?;
    let mut sort = due_millis.to_be_bytes().to_vec();
    sort.extend_from_slice(generation.as_bytes());
    let mut work = item::key(
        key::garbage_collection_partition(generation.as_bytes()),
        sort,
    );
    work.insert(KIND.to_owned(), AttributeValue::S("gc".to_owned()));
    work.insert(
        item::DUE_AT.to_owned(),
        AttributeValue::N(due_millis.to_string()),
    );
    work.insert(
        item::MANIFEST_PK.to_owned(),
        AttributeValue::S(manifest_pk.to_owned()),
    );
    work.insert(
        item::MANIFEST_SK.to_owned(),
        AttributeValue::B(Blob::new(manifest_sk)),
    );
    work.insert(
        GENERATION.to_owned(),
        AttributeValue::S(generation.to_owned()),
    );
    work.insert(
        item::TARGET_PK.to_owned(),
        AttributeValue::S(target_pk.to_owned()),
    );
    Ok(work)
}

async fn read_blob(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    pk: String,
    sk: Vec<u8>,
    kind: &str,
) -> Result<Option<(Vec<u8>, SystemTime)>, StorageError> {
    let Some(manifest) = storage.get(context, pk.clone(), sk, true).await? else {
        return Ok(None);
    };
    if item::string(&manifest, KIND)? != kind || item::string(&manifest, STATE)? != STATE_COMMITTED
    {
        return Ok(None);
    }
    let generation = item::string(&manifest, GENERATION)?;
    let count = item::number_u32(&manifest, CHUNK_COUNT)?;
    let total_bytes = blob_layout(&manifest, kind, count)?;
    let blob_pk = format!("B#{kind}#{pk}#{generation}");
    let mut payload = Vec::new();
    payload.try_reserve_exact(total_bytes).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::ResourceExhausted,
            "dynamodb_blob_allocation_failed",
            error,
        )
    })?;
    for index in 0..count {
        let chunk = storage
            .get(context, blob_pk.clone(), index.to_be_bytes().to_vec(), true)
            .await?
            .ok_or_else(|| {
                StorageError::new(StorageErrorKind::Integrity, "dynamodb_blob_chunk_missing")
            })?;
        if item::string(&chunk, KIND)? != "chunk" {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "dynamodb_blob_chunk_invalid",
            ));
        }
        let bytes = item::binary(&chunk, PAYLOAD)?;
        if bytes.len() > CHUNK_BYTES
            || payload
                .len()
                .checked_add(bytes.len())
                .is_none_or(|length| length > total_bytes)
        {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "dynamodb_blob_chunk_size_invalid",
            ));
        }
        payload.extend_from_slice(bytes);
    }
    if payload.len() != total_bytes {
        return Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_blob_length_mismatch",
        ));
    }
    if Sha256::digest(&payload).as_slice() != item::binary(&manifest, DIGEST)? {
        return Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_blob_digest_mismatch",
        ));
    }
    storage.client.record_blob_work(
        "read",
        payload.len(),
        usize::try_from(count).unwrap_or(usize::MAX),
    );
    let timestamp = item::system_time(item::number_u64(&manifest, TIMESTAMP)?)?;
    Ok(Some((payload, timestamp)))
}

fn blob_layout(manifest: &Item, kind: &str, count: u32) -> Result<usize, StorageError> {
    let maximum_bytes = match kind {
        "model" => MAXIMUM_MODEL_PAYLOAD_BYTES,
        "assertion" => MAXIMUM_ASSERTION_PAYLOAD_BYTES,
        _ => {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "dynamodb_blob_kind_invalid",
            ));
        }
    };
    let total_bytes =
        usize::try_from(item::number_u64(manifest, PAYLOAD_BYTES)?).map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "dynamodb_blob_length_invalid",
                error,
            )
        })?;
    let expected_count = if total_bytes == 0 {
        0
    } else {
        total_bytes
            .saturating_sub(1)
            .checked_div(CHUNK_BYTES)
            .and_then(|chunks| chunks.checked_add(1))
            .ok_or_else(|| {
                StorageError::new(
                    StorageErrorKind::Integrity,
                    "dynamodb_blob_chunk_count_invalid",
                )
            })?
    };
    if total_bytes > maximum_bytes || usize::try_from(count).ok() != Some(expected_count) {
        return Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_blob_layout_invalid",
        ));
    }
    Ok(total_bytes)
}

fn require_snapshot_limit(
    tuples: &mut [RelationshipTuple],
    options: ReadOptions,
) -> Result<(), StorageError> {
    if tuples.len() > options.maximum_results() {
        return Err(StorageError::new(
            StorageErrorKind::ResourceExhausted,
            "tuple_read_result_limit",
        ));
    }
    tuples.sort_by(|left, right| left.key().cmp(right.key()));
    Ok(())
}

fn snapshot_evaluation_budget(options: ReadOptions) -> usize {
    options
        .maximum_results()
        .saturating_mul(16)
        .max(options.maximum_results().saturating_add(1))
        .min(MAXIMUM_QUERY_ITEMS)
}

fn tuple_key_from_parts(
    object: &str,
    relation: &str,
    subject: &str,
) -> Result<TupleKey, StorageError> {
    format!("{object}#{relation}@{subject}")
        .parse::<TupleKey>()
        .map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "dynamodb_exact_key_invalid",
                error,
            )
        })
}

fn strong(context: &OperationContext) -> bool {
    matches!(
        context.consistency(),
        ConsistencyPreference::HigherConsistency
    )
}

fn request_build_error(error: impl std::error::Error + Send + Sync + 'static) -> StorageError {
    StorageError::with_source(
        StorageErrorKind::Internal,
        "dynamodb_request_build_failed",
        error,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_reject_blob_chunk_count_that_disagrees_with_bounded_length() {
        let manifest = blob_manifest(CHUNK_BYTES, u32::MAX);

        let result = blob_layout(&manifest, "model", u32::MAX);

        assert!(matches!(
            result,
            Err(ref error)
                if error.kind() == StorageErrorKind::Integrity
                    && error.code() == "dynamodb_blob_layout_invalid"
        ));
    }

    #[test]
    fn test_should_reject_blob_length_above_domain_limit() {
        let length = MAXIMUM_ASSERTION_PAYLOAD_BYTES.saturating_add(1);
        let count = 33_u32;
        let manifest = blob_manifest(length, count);

        let result = blob_layout(&manifest, "assertion", count);

        assert!(matches!(
            result,
            Err(ref error)
                if error.kind() == StorageErrorKind::Integrity
                    && error.code() == "dynamodb_blob_layout_invalid"
        ));
    }

    #[test]
    fn test_should_accept_zero_length_blob_layout() -> Result<(), StorageError> {
        let manifest = blob_manifest(0, 0);

        assert_eq!(blob_layout(&manifest, "assertion", 0)?, 0);
        Ok(())
    }

    #[test]
    fn test_should_reject_large_current_items_before_transaction_dispatch() {
        let current = (0..11)
            .map(|index| {
                Item::from([
                    (
                        item::PK.to_owned(),
                        AttributeValue::S(format!("F#store#{index}")),
                    ),
                    (
                        PAYLOAD.to_owned(),
                        AttributeValue::B(Blob::new(vec![0_u8; 349 * 1_024])),
                    ),
                ])
            })
            .collect::<Vec<_>>();

        let result = require_transaction_size(&[], current.iter());

        assert!(matches!(
            result,
            Err(ref error)
                if error.kind() == StorageErrorKind::ResourceExhausted
                    && error.code() == "dynamodb_transaction_too_large"
        ));
    }

    #[test]
    fn test_should_enforce_item_and_aggregate_transaction_byte_boundaries()
    -> Result<(), StorageError> {
        let item_limit = item_with_encoded_size(item::MAXIMUM_ITEM_BYTES)?;
        assert!(item::require_item_limit(&item_limit).is_ok());
        let over_item_limit = item_with_encoded_size(item::MAXIMUM_ITEM_BYTES.saturating_add(1))?;
        assert!(item::require_item_limit(&over_item_limit).is_err());
        let aws_hard_limit = item_with_encoded_size(400 * 1_024)?;
        assert!(item::require_item_limit(&aws_hard_limit).is_err());

        let at_transaction_limit = (0..10)
            .map(|_| item_with_encoded_size(item::MAXIMUM_ITEM_BYTES))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(require_transaction_size(&[], at_transaction_limit.iter()).is_ok());
        let one_byte_over = at_transaction_limit
            .iter()
            .cloned()
            .chain(std::iter::once(item_with_encoded_size(1)?))
            .collect::<Vec<_>>();
        assert!(require_transaction_size(&[], one_byte_over.iter()).is_err());
        let four_mebibytes = (0..11)
            .map(|_| item_with_encoded_size(350 * 1_024))
            .chain(std::iter::once(item_with_encoded_size(246 * 1_024)))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(require_transaction_size(&[], four_mebibytes.iter()).is_err());
        Ok(())
    }

    fn item_with_encoded_size(size: usize) -> Result<Item, StorageError> {
        let name = "p".to_owned();
        let overhead = name.len();
        let payload = size.checked_sub(overhead).ok_or_else(|| {
            StorageError::new(StorageErrorKind::Internal, "test_item_size_too_small")
        })?;
        let value = Item::from([(name, AttributeValue::B(Blob::new(vec![0_u8; payload])))]);
        if item::encoded_item_size(&value)? != size {
            return Err(StorageError::new(
                StorageErrorKind::Internal,
                "test_item_size_mismatch",
            ));
        }
        Ok(value)
    }

    fn blob_manifest(length: usize, count: u32) -> Item {
        Item::from([
            (
                PAYLOAD_BYTES.to_owned(),
                AttributeValue::N(length.to_string()),
            ),
            (CHUNK_COUNT.to_owned(), AttributeValue::N(count.to_string())),
        ])
    }
}
