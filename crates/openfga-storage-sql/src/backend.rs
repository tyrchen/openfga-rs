//! `PostgreSQL` implementation of every narrow storage capability.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use openfga_domain::{
    AuthorizationModelId, ChangeId, ConsistencyPreference, RelationshipTuple, StoreId, SubjectKind,
    SubjectRef, TupleKey,
};
use openfga_model::{MODEL_COMPILER_FORMAT_VERSION, ModelCompiler};
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ChangeFilter, ChangeOperation, ChangeReader,
    HealthCheck, HealthStatus, ModelReader, ModelWriter, MutationOutcome, ObjectRelationFilter,
    OperationContext, Page, PageOptions, ReadOptions, ReverseTupleFilter, StorageCursor,
    StorageError, StorageErrorKind, StoreFilter, StoreName, StoreReader, StoreRecord, StoreWriter,
    StoredAuthorizationModel, StoredTuple, TupleChange, TupleReadFilter, TupleReader, TupleStream,
    TupleWriteOptions, TupleWriter, UsersetTupleFilter, WriteConflictPolicy,
};
use secrecy::ExposeSecret;
use sqlx::{
    FromRow, PgPool, Postgres, QueryBuilder, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, sleep_until},
};
use tracing::{instrument, warn};
use ulid::Ulid;

use crate::{
    PostgresMutationFaultInjector, PostgresMutationStage, PostgresStorageConfig,
    codec::{
        decode_assertions, decode_model, decode_tuple, encode_assertions, encode_condition_context,
        encode_model, encode_tuple,
    },
    error::{cancelled, map_sqlx, timed_out},
    fault::NoPostgresMutationFaults,
};

pub(crate) const SCHEMA_VERSION: i64 = 202_608_050_001;
const SUBJECT_OBJECT: i16 = 0;
const SUBJECT_USERSET: i16 = 1;
const SUBJECT_WILDCARD: i16 = 2;
const CHANGE_WRITE: i16 = 0;
const CHANGE_DELETE: i16 = 1;
const ULID_MAX_TIMESTAMP_MS: u64 = (1_u64 << 48) - 1;

pub(crate) static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Durable `PostgreSQL` backend with strict primary/replica routing.
pub struct PostgresStorage {
    primary: PgPool,
    replica: Option<PgPool>,
    work_permits: Arc<Semaphore>,
    config: PostgresStorageConfig,
    compiler: ModelCompiler,
    faults: Arc<dyn PostgresMutationFaultInjector>,
}

#[derive(Clone, Copy, Debug)]
struct PreparedTupleMutation<'a> {
    deletes: &'a BTreeSet<TupleKey>,
    writes: &'a BTreeMap<TupleKey, RelationshipTuple>,
    delete_order: &'a [TupleKey],
    write_order: &'a [TupleKey],
    options: TupleWriteOptions,
}

impl PostgresStorage {
    /// Connects bounded pools and optionally applies embedded forward migrations.
    ///
    /// # Errors
    ///
    /// Returns a redacted configuration, availability, migration, or schema-version failure.
    pub async fn connect(config: PostgresStorageConfig) -> Result<Self, StorageError> {
        Self::connect_with_faults(config, Arc::new(NoPostgresMutationFaults)).await
    }

    /// Connects with an explicit transaction fault injector for integration testing.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::connect`].
    #[doc(hidden)]
    pub async fn connect_with_faults(
        config: PostgresStorageConfig,
        faults: Arc<dyn PostgresMutationFaultInjector>,
    ) -> Result<Self, StorageError> {
        config.validate().map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "postgres_configuration_invalid",
                error,
            )
        })?;
        let primary = connect_pool(&config, config.primary_url.expose_secret()).await?;
        if config.migrate_on_connect {
            MIGRATOR.run(&primary).await.map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Integrity,
                    "postgres_migration_failed",
                    error,
                )
            })?;
        }
        verify_schema(&primary).await?;
        let replica = match &config.replica_url {
            Some(url) => {
                let pool = connect_pool(&config, url.expose_secret()).await?;
                verify_schema(&pool).await?;
                Some(pool)
            }
            None => None,
        };
        let work_permits = Arc::new(Semaphore::new(
            usize::try_from(config.max_connections.get()).map_err(|_| {
                StorageError::new(
                    StorageErrorKind::Integrity,
                    "postgres_work_limit_out_of_range",
                )
            })?,
        ));
        Ok(Self {
            primary,
            replica,
            work_permits,
            config,
            compiler: ModelCompiler::default(),
            faults,
        })
    }

    /// Applies every embedded forward migration and verifies the exact schema version.
    ///
    /// # Errors
    ///
    /// Returns an integrity failure for a failed, older, or newer schema.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        MIGRATOR.run(&self.primary).await.map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "postgres_migration_failed",
                error,
            )
        })?;
        verify_schema(&self.primary).await
    }

    /// Closes primary and replica pools, waiting for checked-out connections to return.
    pub async fn close(&self) {
        if let Some(replica) = &self.replica {
            replica.close().await;
        }
        self.primary.close().await;
    }

    /// Returns the primary pool for bounded operational diagnostics.
    #[doc(hidden)]
    #[must_use]
    pub const fn primary_pool(&self) -> &PgPool {
        &self.primary
    }

    /// Returns the configured replica pool for bounded operational diagnostics.
    #[doc(hidden)]
    #[must_use]
    pub fn replica_pool(&self) -> Option<&PgPool> {
        self.replica.as_ref()
    }

    async fn acquire_work(
        &self,
        context: &OperationContext,
    ) -> Result<OwnedSemaphorePermit, StorageError> {
        context.check()?;
        let acquire = Arc::clone(&self.work_permits).acquire_owned();
        tokio::pin!(acquire);
        let deadline = Instant::from_std(context.deadline().instant());
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => Err(cancelled()),
            () = sleep_until(deadline) => Err(timed_out()),
            result = &mut acquire => result.map_err(|_| StorageError::new(
                StorageErrorKind::Unavailable,
                "postgres_work_admission_closed",
            )),
        }
    }

    async fn read_pool<'a>(&'a self, context: &OperationContext) -> &'a PgPool {
        if matches!(
            context.consistency(),
            ConsistencyPreference::HigherConsistency
        ) {
            return &self.primary;
        }
        let Some(replica) = &self.replica else {
            return &self.primary;
        };
        let maximum_lag_seconds = self.config.replica_max_lag.as_secs_f64();
        let healthy = match execute(
            context,
            sqlx::query_scalar::<_, bool>(
                "SELECT NOT pg_is_in_recovery() OR COALESCE(EXTRACT(EPOCH FROM clock_timestamp() \
                 - pg_last_xact_replay_timestamp()) <= $1, FALSE)",
            )
            .bind(maximum_lag_seconds)
            .fetch_one(replica),
            "postgres_replica_lag_check_failed",
        )
        .await
        {
            Ok(healthy) => healthy,
            Err(error) => {
                warn!(
                    error_kind = ?error.kind(),
                    error_code = error.code(),
                    "PostgreSQL replica lag check failed; routing read to primary"
                );
                false
            }
        };
        if healthy { replica } else { &self.primary }
    }

    async fn write_tuples_in_transaction(
        &self,
        context: &OperationContext,
        transaction: &mut Transaction<'_, Postgres>,
        store_id: StoreId,
        mutation: PreparedTupleMutation<'_>,
    ) -> Result<Vec<ChangeId>, StorageError> {
        let PreparedTupleMutation {
            deletes,
            writes,
            delete_order,
            write_order,
            options,
        } = mutation;
        configure_transaction_deadline(context, transaction).await?;
        self.faults.check(PostgresMutationStage::BeforeLock)?;
        let mut affected = deletes.clone();
        affected.extend(writes.keys().cloned());
        let mut existing = BTreeMap::new();
        for key in &affected {
            execute(
                context,
                sqlx::query(
                    "SELECT pg_advisory_xact_lock(hashtextextended($2, hashtextextended($1, 0)))",
                )
                .bind(store_id.to_string())
                .bind(key.to_string())
                .execute(&mut **transaction),
                "postgres_tuple_lock_failed",
            )
            .await?;
            if let Some(tuple) =
                read_exact_in_transaction(context, transaction, store_id, key).await?
            {
                existing.insert(key.clone(), tuple);
            }
        }
        self.faults.check(PostgresMutationStage::AfterLock)?;

        validate_conflict_policy(delete_order, write_order, writes, &existing, options)?;
        let timestamp_ms = transaction_timestamp(context, transaction).await?;
        let mut changes = Vec::with_capacity(deletes.len().saturating_add(writes.len()));

        for key in deletes {
            let Some(tuple) = existing.get(key) else {
                continue;
            };
            delete_tuple(context, transaction, store_id, key).await?;
            changes.push((ChangeOperation::Delete, tuple.clone()));
        }
        self.faults.check(PostgresMutationStage::AfterDelete)?;

        for (key, tuple) in writes {
            if existing.contains_key(key) {
                continue;
            }
            insert_tuple(context, transaction, store_id, tuple, timestamp_ms).await?;
            changes.push((ChangeOperation::Write, tuple.clone()));
        }
        self.faults.check(PostgresMutationStage::AfterWrite)?;

        let mut change_ids = Vec::with_capacity(changes.len());
        let mut previous_change_id = if changes.is_empty() {
            None
        } else {
            lock_change_allocator(context, transaction).await?
        };
        for (operation, tuple) in changes {
            let change_id =
                next_change_id(context, transaction, timestamp_ms, previous_change_id).await?;
            insert_change(
                context,
                transaction,
                store_id,
                change_id,
                operation,
                &tuple,
                timestamp_ms,
            )
            .await?;
            change_ids.push(change_id);
            previous_change_id = Some(change_id);
        }
        if let Some(change_id) = previous_change_id {
            update_change_allocator(context, transaction, change_id).await?;
        }
        self.faults.check(PostgresMutationStage::AfterChangelog)?;
        self.faults.check(PostgresMutationStage::BeforeCommit)?;
        Ok(change_ids)
    }
}

impl fmt::Debug for PostgresStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStorage")
            .field("config", &self.config)
            .field("replica_configured", &self.replica.is_some())
            .field("faults", &self.faults)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TupleReader for PostgresStorage {
    #[instrument(skip_all, fields(store_id = %store_id))]
    async fn read_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &TupleReadFilter,
        options: &PageOptions,
    ) -> Result<Page<StoredTuple>, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        context.check()?;
        let pool = self.read_pool(context).await;
        let mut query = QueryBuilder::<Postgres>::new(tuple_select_prefix());
        query
            .push(" WHERE store_id = ")
            .push_bind(store_id.to_string());
        push_tuple_read_filter(&mut query, filter);
        if let Some(cursor) = options.after() {
            let key = tuple_cursor(cursor)?;
            push_after_tuple(&mut query, &key);
        }
        query
            .push(
                " ORDER BY object_type, object_id, relation, subject_kind, subject_type, \
                 subject_id, subject_relation LIMIT ",
            )
            .push_bind(page_fetch_limit(options.maximum_results())?);
        let rows = execute(
            context,
            query.build_query_as::<TupleRow>().fetch_all(pool),
            "postgres_read_tuples_failed",
        )
        .await?;
        tuple_page(rows, options.maximum_results())
    }

    #[instrument(skip_all, fields(store_id = %store_id))]
    async fn read_exact_tuple(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<StoredTuple, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let parts = TupleParts::from_key(key);
        let store_id_text = store_id.to_string();
        let row = execute(
            context,
            sqlx::query_as!(
                TupleRow,
                r#"SELECT tuple_payload,
                    (EXTRACT(EPOCH FROM inserted_at) * 1000)::BIGINT AS "inserted_at_ms!"
                   FROM tuples
                   WHERE store_id = $1 AND object_type = $2 AND object_id = $3
                     AND relation = $4 AND subject_kind = $5 AND subject_type = $6
                     AND subject_id = $7 AND subject_relation = $8"#,
                store_id_text,
                parts.object_type,
                parts.object_id,
                parts.relation,
                parts.subject_kind,
                parts.subject_type,
                parts.subject_id,
                parts.subject_relation,
            )
            .fetch_one(pool),
            "postgres_read_exact_tuple_failed",
        )
        .await?;
        row.into_stored_tuple()
    }

    async fn read_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let mut query = QueryBuilder::<Postgres>::new(tuple_select_prefix());
        query
            .push(" WHERE store_id = ")
            .push_bind(store_id.to_string())
            .push(" AND object_type = ")
            .push_bind(filter.object().object_type().as_str())
            .push(" AND object_id = ")
            .push_bind(filter.object().object_id().as_str())
            .push(" AND relation = ")
            .push_bind(filter.relation().as_str());
        push_subject_allowlist(&mut query, filter.subjects());
        push_condition_filter(&mut query, filter.conditions());
        query
            .push(" ORDER BY subject_kind, subject_type, subject_id, subject_relation LIMIT ")
            .push_bind(result_limit(options.maximum_results())?);
        let rows = execute(
            context,
            query.build_query_as::<TupleRow>().fetch_all(pool),
            "postgres_read_object_relation_failed",
        )
        .await?;
        rows_to_stream(rows, |tuple| filter.conditions().matches(tuple.condition()))
    }

    async fn read_userset_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &UsersetTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let mut query = QueryBuilder::<Postgres>::new(tuple_select_prefix());
        query
            .push(" WHERE store_id = ")
            .push_bind(store_id.to_string())
            .push(" AND object_type = ")
            .push_bind(filter.object().object_type().as_str())
            .push(" AND object_id = ")
            .push_bind(filter.object().object_id().as_str())
            .push(" AND relation = ")
            .push_bind(filter.relation().as_str())
            .push(" AND subject_kind = ")
            .push_bind(SUBJECT_USERSET);
        if !filter.allowed().is_empty() {
            query.push(" AND (");
            let mut separated = query.separated(" OR ");
            for allowed in filter.allowed() {
                separated
                    .push("(subject_type = ")
                    .push_bind(allowed.subject_type().as_str())
                    .push(" AND subject_relation = ")
                    .push_bind(allowed.relation().as_str())
                    .push_unseparated(")");
            }
            query.push(")");
        }
        push_condition_filter(&mut query, filter.conditions());
        query
            .push(" ORDER BY subject_type, subject_relation, subject_id LIMIT ")
            .push_bind(result_limit(options.maximum_results())?);
        let rows = execute(
            context,
            query.build_query_as::<TupleRow>().fetch_all(pool),
            "postgres_read_userset_tuples_failed",
        )
        .await?;
        rows_to_stream(rows, |tuple| filter.conditions().matches(tuple.condition()))
    }

    async fn read_reverse_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ReverseTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let mut query = QueryBuilder::<Postgres>::new(tuple_select_prefix());
        query
            .push(" WHERE store_id = ")
            .push_bind(store_id.to_string())
            .push(" AND object_type = ")
            .push_bind(filter.object_type().as_str())
            .push(" AND relation = ")
            .push_bind(filter.relation().as_str());
        push_subject_allowlist(&mut query, filter.subjects());
        if !filter.object_ids().is_empty() {
            query.push(" AND object_id IN (");
            let mut separated = query.separated(", ");
            for object_id in filter.object_ids() {
                separated.push_bind(object_id.as_str());
            }
            query.push(")");
        }
        push_condition_filter(&mut query, filter.conditions());
        query
            .push(
                " ORDER BY subject_kind, subject_type, subject_id, subject_relation, object_id \
                 LIMIT ",
            )
            .push_bind(result_limit(options.maximum_results())?);
        let rows = execute(
            context,
            query.build_query_as::<TupleRow>().fetch_all(pool),
            "postgres_read_reverse_tuples_failed",
        )
        .await?;
        rows_to_stream(rows, |tuple| filter.conditions().matches(tuple.condition()))
    }

    async fn tuple_exists(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<bool, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let parts = TupleParts::from_key(key);
        let store_id_text = store_id.to_string();
        execute(
            context,
            sqlx::query_scalar!(
                r#"SELECT EXISTS(
                    SELECT 1 FROM tuples
                    WHERE store_id = $1 AND object_type = $2 AND object_id = $3
                      AND relation = $4 AND subject_kind = $5 AND subject_type = $6
                      AND subject_id = $7 AND subject_relation = $8
                ) AS "exists!""#,
                store_id_text,
                parts.object_type,
                parts.object_id,
                parts.relation,
                parts.subject_kind,
                parts.subject_type,
                parts.subject_id,
                parts.subject_relation,
            )
            .fetch_one(pool),
            "postgres_tuple_exists_failed",
        )
        .await
    }

    async fn count_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let mut query = QueryBuilder::<Postgres>::new("SELECT COUNT(*) FROM tuples");
        query
            .push(" WHERE store_id = ")
            .push_bind(store_id.to_string())
            .push(" AND object_type = ")
            .push_bind(filter.object().object_type().as_str())
            .push(" AND object_id = ")
            .push_bind(filter.object().object_id().as_str())
            .push(" AND relation = ")
            .push_bind(filter.relation().as_str());
        push_subject_allowlist(&mut query, filter.subjects());
        push_condition_filter(&mut query, filter.conditions());
        let count = execute(
            context,
            query.build_query_scalar::<i64>().fetch_one(pool),
            "postgres_count_object_relation_failed",
        )
        .await?;
        u64::try_from(count).map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "postgres_tuple_count_invalid",
                error,
            )
        })
    }
}

#[async_trait]
impl TupleWriter for PostgresStorage {
    #[instrument(skip_all, fields(store_id = %store_id, deletes = deletes.len(), writes = writes.len()))]
    async fn write_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        context.check()?;
        let delete_order = deletes.clone();
        let write_order = writes
            .iter()
            .map(|tuple| tuple.key().clone())
            .collect::<Vec<_>>();
        let deletes = unique_deletes(deletes)?;
        let writes = unique_writes(writes)?;
        if deletes.len().saturating_add(writes.len())
            > self.config.max_tuple_mutations.get() as usize
        {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "postgres_tuple_mutation_limit",
            ));
        }
        if deletes.iter().any(|key| writes.contains_key(key)) {
            return Err(StorageError::new(
                StorageErrorKind::Conflict,
                "tuple_key_in_delete_and_write",
            ));
        }
        if deletes.is_empty() && writes.is_empty() {
            return Ok(MutationOutcome::new(Vec::new()));
        }
        let mut transaction = execute(
            context,
            self.primary.begin(),
            "postgres_tuple_transaction_begin_failed",
        )
        .await?;
        let result = self
            .write_tuples_in_transaction(
                context,
                &mut transaction,
                store_id,
                PreparedTupleMutation {
                    deletes: &deletes,
                    writes: &writes,
                    delete_order: &delete_order,
                    write_order: &write_order,
                    options,
                },
            )
            .await;
        let change_ids = match result {
            Ok(change_ids) => change_ids,
            Err(error) => {
                let _ = tokio::time::timeout(self.config.statement_timeout, transaction.rollback())
                    .await;
                return Err(error);
            }
        };
        execute(
            context,
            transaction.commit(),
            "postgres_tuple_transaction_commit_failed",
        )
        .await?;
        Ok(MutationOutcome::new(change_ids))
    }
}

#[async_trait]
impl ModelReader for PostgresStorage {
    async fn read_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let store_id_text = store_id.to_string();
        let model_id_text = model_id.to_string();
        let row = execute(
            context,
            sqlx::query_as!(
                ModelRow,
                r#"SELECT model_id, schema_version, compiler_format_version,
                    source_fingerprint, source_payload,
                    (EXTRACT(EPOCH FROM written_at) * 1000)::BIGINT AS "written_at_ms!"
                   FROM authorization_models WHERE store_id = $1 AND model_id = $2"#,
                store_id_text,
                model_id_text,
            )
            .fetch_one(pool),
            "postgres_read_model_failed",
        )
        .await?;
        row.into_model(store_id, model_id, &self.compiler)
    }

    async fn read_latest_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let store_id_text = store_id.to_string();
        let row = execute(
            context,
            sqlx::query_as!(
                ModelRow,
                r#"SELECT model_id, schema_version, compiler_format_version,
                    source_fingerprint, source_payload,
                    (EXTRACT(EPOCH FROM written_at) * 1000)::BIGINT AS "written_at_ms!"
                   FROM authorization_models
                   WHERE store_id = $1 ORDER BY model_id DESC LIMIT 1"#,
                store_id_text,
            )
            .fetch_one(pool),
            "postgres_read_latest_model_failed",
        )
        .await?;
        let model_id = parse_id(&row.model_id, "postgres_model_id_invalid")?;
        row.into_model(store_id, model_id, &self.compiler)
    }

    async fn list_models(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        options: &PageOptions,
    ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let mut query = QueryBuilder::<Postgres>::new(model_select_columns());
        query
            .push(" WHERE store_id = ")
            .push_bind(store_id.to_string());
        if let Some(cursor) = options.after() {
            let after = cursor_id::<AuthorizationModelId>(cursor, "postgres_model_cursor_invalid")?;
            query.push(" AND model_id < ").push_bind(after.to_string());
        }
        query
            .push(" ORDER BY model_id DESC LIMIT ")
            .push_bind(page_fetch_limit(options.maximum_results())?);
        let rows = execute(
            context,
            query.build_query_as::<ModelRow>().fetch_all(pool),
            "postgres_list_models_failed",
        )
        .await?;
        let has_more = rows.len() > options.maximum_results();
        let mut models = Vec::with_capacity(rows.len().min(options.maximum_results()));
        for row in rows.into_iter().take(options.maximum_results()) {
            let model_id = parse_id(&row.model_id, "postgres_model_id_invalid")?;
            models.push(row.into_model(store_id, model_id, &self.compiler)?);
        }
        let continuation = if has_more {
            models
                .last()
                .map(|model| cursor(model.model_id().to_string()))
                .transpose()?
        } else {
            None
        };
        Ok(Page::new(models, continuation))
    }
}

#[async_trait]
impl ModelWriter for PostgresStorage {
    async fn write_model(
        &self,
        context: &OperationContext,
        model: Arc<StoredAuthorizationModel>,
    ) -> Result<(), StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let payload = encode_model(&model)?;
        let written_at_ms = system_time_to_millis(model.written_at())?;
        execute(
            context,
            sqlx::query(
                "INSERT INTO authorization_models (store_id, model_id, schema_version, \
                 compiler_format_version, source_fingerprint, source_payload, written_at) VALUES \
                 ($1, $2, $3, $4, $5, $6, to_timestamp($7::double precision / 1000.0))",
            )
            .bind(model.store_id().to_string())
            .bind(model.model_id().to_string())
            .bind(model.compiled().schema_version())
            .bind(
                i32::try_from(MODEL_COMPILER_FORMAT_VERSION)
                    .map_err(internal_conversion("compiler_format_version"))?,
            )
            .bind(model.compiled().source_fingerprint().as_bytes().to_vec())
            .bind(payload)
            .bind(written_at_ms)
            .execute(&self.primary),
            "postgres_write_model_failed",
        )
        .await
        .map(|_| ())
    }
}

#[async_trait]
impl StoreReader for PostgresStorage {
    async fn read_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<StoreRecord, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let store_id_text = store_id.to_string();
        let row = execute(
            context,
            sqlx::query_as!(
                StoreRow,
                r#"SELECT id, name,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS "created_at_ms!",
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS "updated_at_ms!"
                   FROM stores WHERE id = $1 AND deleted_at IS NULL"#,
                store_id_text,
            )
            .fetch_one(pool),
            "postgres_read_store_failed",
        )
        .await?;
        row.into_record()
    }

    async fn list_stores(
        &self,
        context: &OperationContext,
        filter: &StoreFilter,
        options: &PageOptions,
    ) -> Result<Page<StoreRecord>, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let mut query = QueryBuilder::<Postgres>::new(store_select_columns());
        query.push(" WHERE deleted_at IS NULL");
        if let Some(name) = filter.name() {
            query
                .push(" AND name = ")
                .push_bind(name.as_str().to_owned());
        }
        if let Some(after) = options.after() {
            let id = cursor_id::<StoreId>(after, "postgres_store_cursor_invalid")?;
            query.push(" AND id > ").push_bind(id.to_string());
        }
        query
            .push(" ORDER BY id LIMIT ")
            .push_bind(page_fetch_limit(options.maximum_results())?);
        let rows = execute(
            context,
            query.build_query_as::<StoreRow>().fetch_all(pool),
            "postgres_list_stores_failed",
        )
        .await?;
        let has_more = rows.len() > options.maximum_results();
        let records = rows
            .into_iter()
            .take(options.maximum_results())
            .map(StoreRow::into_record)
            .collect::<Result<Vec<_>, _>>()?;
        let continuation = if has_more {
            records
                .last()
                .map(|record| cursor(record.id().to_string()))
                .transpose()?
        } else {
            None
        };
        Ok(Page::new(records, continuation))
    }
}

#[async_trait]
impl StoreWriter for PostgresStorage {
    async fn create_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let row = execute(
            context,
            sqlx::query_as::<_, StoreRow>(
                "INSERT INTO stores (id, name, created_at, updated_at) VALUES ($1, $2, \
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id, name, (EXTRACT(EPOCH FROM \
                 created_at) * 1000)::BIGINT AS created_at_ms, (EXTRACT(EPOCH FROM updated_at) * \
                 1000)::BIGINT AS updated_at_ms",
            )
            .bind(store_id.to_string())
            .bind(name.as_str())
            .fetch_one(&self.primary),
            "postgres_create_store_failed",
        )
        .await?;
        row.into_record()
    }

    async fn rename_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let row = execute(
            context,
            sqlx::query_as::<_, StoreRow>(
                "UPDATE stores SET name = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND \
                 deleted_at IS NULL RETURNING id, name, (EXTRACT(EPOCH FROM created_at) * \
                 1000)::BIGINT AS created_at_ms, (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT \
                 AS updated_at_ms",
            )
            .bind(store_id.to_string())
            .bind(name.as_str())
            .fetch_one(&self.primary),
            "postgres_rename_store_failed",
        )
        .await?;
        row.into_record()
    }

    async fn delete_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<(), StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        execute(
            context,
            sqlx::query("DELETE FROM stores WHERE id = $1")
                .bind(store_id.to_string())
                .execute(&self.primary),
            "postgres_delete_store_failed",
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl AssertionReader for PostgresStorage {
    async fn read_assertions(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<[Assertion]>, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        match execute(
            context,
            sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT assertions_payload FROM assertions WHERE store_id = $1 AND model_id = $2",
            )
            .bind(store_id.to_string())
            .bind(model_id.to_string())
            .fetch_optional(pool),
            "postgres_read_assertions_failed",
        )
        .await?
        {
            Some(payload) => decode_assertions(&payload),
            None => Ok(Arc::from(Vec::<Assertion>::new())),
        }
    }
}

#[async_trait]
impl AssertionWriter for PostgresStorage {
    async fn write_assertions(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
        assertions: Vec<Assertion>,
    ) -> Result<(), StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let payload = encode_assertions(&assertions)?;
        execute(
            context,
            sqlx::query(
                "INSERT INTO assertions (store_id, model_id, assertions_payload, written_at) \
                 VALUES ($1, $2, $3, CURRENT_TIMESTAMP) ON CONFLICT (store_id, model_id) DO \
                 UPDATE SET assertions_payload = EXCLUDED.assertions_payload, written_at = \
                 EXCLUDED.written_at",
            )
            .bind(store_id.to_string())
            .bind(model_id.to_string())
            .bind(payload)
            .execute(&self.primary),
            "postgres_write_assertions_failed",
        )
        .await
        .map(|_| ())
    }
}

#[async_trait]
impl ChangeReader for PostgresStorage {
    async fn read_changes(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ChangeFilter,
        options: &PageOptions,
    ) -> Result<Page<TupleChange>, StorageError> {
        let _work_permit = self.acquire_work(context).await?;
        let pool = self.read_pool(context).await;
        let mut query = QueryBuilder::<Postgres>::new(change_select_columns());
        query
            .push(" WHERE store_id = ")
            .push_bind(store_id.to_string());
        if let Some(object_type) = filter.object_type() {
            query
                .push(" AND object_type = ")
                .push_bind(object_type.as_str());
        }
        if let Some(start_time) = filter.start_time() {
            query
                .push(" AND changed_at >= to_timestamp(")
                .push_bind(system_time_to_millis(start_time)?)
                .push("::double precision / 1000.0)");
        }
        if let Some(after) = options.after() {
            let id = cursor_id::<ChangeId>(after, "postgres_change_cursor_invalid")?;
            query.push(" AND change_id > ").push_bind(id.to_string());
        }
        query
            .push(" ORDER BY change_id LIMIT ")
            .push_bind(page_fetch_limit(options.maximum_results())?);
        let rows = execute(
            context,
            query.build_query_as::<ChangeRow>().fetch_all(pool),
            "postgres_read_changes_failed",
        )
        .await?;
        let has_more = rows.len() > options.maximum_results();
        let changes = rows
            .into_iter()
            .take(options.maximum_results())
            .map(|row| row.into_change(store_id))
            .collect::<Result<Vec<_>, _>>()?;
        let continuation = if has_more {
            changes
                .last()
                .map(|change| cursor(change.id().to_string()))
                .transpose()?
        } else {
            None
        };
        Ok(Page::new(changes, continuation))
    }
}

#[async_trait]
impl HealthCheck for PostgresStorage {
    async fn health(&self, context: &OperationContext) -> Result<HealthStatus, StorageError> {
        let version = execute(
            context,
            sqlx::query_scalar::<_, i64>(
                "SELECT schema_version FROM openfga_schema_metadata WHERE singleton = TRUE",
            )
            .fetch_one(&self.primary),
            "postgres_health_failed",
        )
        .await?;
        if version != SCHEMA_VERSION {
            return Err(schema_version_error(version));
        }
        Ok(HealthStatus::new(true, "ready"))
    }
}

#[derive(Debug, FromRow)]
struct TupleRow {
    tuple_payload: Vec<u8>,
    inserted_at_ms: i64,
}

impl TupleRow {
    fn into_stored_tuple(self) -> Result<StoredTuple, StorageError> {
        Ok(StoredTuple::new(
            decode_tuple(&self.tuple_payload)?,
            millis_to_system_time(self.inserted_at_ms)?,
        ))
    }
}

#[derive(Debug, FromRow)]
struct StoreRow {
    id: String,
    name: String,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl StoreRow {
    fn into_record(self) -> Result<StoreRecord, StorageError> {
        let id = parse_id(&self.id, "postgres_store_id_invalid")?;
        let name = StoreName::new(self.name).map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "postgres_store_name_invalid",
                error,
            )
        })?;
        let created = millis_to_system_time(self.created_at_ms)?;
        let updated = millis_to_system_time(self.updated_at_ms)?;
        Ok(StoreRecord::new(id, name.clone(), created).renamed(name, updated))
    }
}

#[derive(Debug, FromRow)]
struct ModelRow {
    model_id: String,
    schema_version: String,
    compiler_format_version: i32,
    source_fingerprint: Vec<u8>,
    source_payload: Vec<u8>,
    written_at_ms: i64,
}

impl ModelRow {
    fn into_model(
        self,
        store_id: StoreId,
        model_id: AuthorizationModelId,
        compiler: &ModelCompiler,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
        let model = decode_model(
            &self.source_payload,
            store_id,
            model_id,
            millis_to_system_time(self.written_at_ms)?,
            compiler,
        )?;
        let expected_format = i32::try_from(MODEL_COMPILER_FORMAT_VERSION)
            .map_err(internal_conversion("compiler_format_version"))?;
        if self.schema_version != model.compiled().schema_version()
            || self.compiler_format_version != expected_format
            || self.source_fingerprint.as_slice()
                != model.compiled().source_fingerprint().as_bytes()
        {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "postgres_model_metadata_mismatch",
            ));
        }
        Ok(model)
    }
}

#[derive(Debug, FromRow)]
struct ChangeRow {
    change_id: String,
    operation: i16,
    tuple_payload: Vec<u8>,
    changed_at_ms: i64,
}

impl ChangeRow {
    fn into_change(self, store_id: StoreId) -> Result<TupleChange, StorageError> {
        let operation = match self.operation {
            CHANGE_WRITE => ChangeOperation::Write,
            CHANGE_DELETE => ChangeOperation::Delete,
            _ => {
                return Err(StorageError::new(
                    StorageErrorKind::Integrity,
                    "postgres_change_operation_invalid",
                ));
            }
        };
        Ok(TupleChange::new(
            parse_id(&self.change_id, "postgres_change_id_invalid")?,
            store_id,
            operation,
            decode_tuple(&self.tuple_payload)?,
            millis_to_system_time(self.changed_at_ms)?,
        ))
    }
}

struct TupleParts<'a> {
    object_type: &'a str,
    object_id: &'a str,
    relation: &'a str,
    subject_kind: i16,
    subject_type: &'a str,
    subject_id: &'a str,
    subject_relation: &'a str,
}

impl<'a> TupleParts<'a> {
    fn from_key(key: &'a TupleKey) -> Self {
        let subject = key.subject();
        let kind = match subject.kind() {
            SubjectKind::Object => SUBJECT_OBJECT,
            SubjectKind::Userset => SUBJECT_USERSET,
            SubjectKind::TypedWildcard => SUBJECT_WILDCARD,
        };
        Self {
            object_type: key.object().object_type().as_str(),
            object_id: key.object().object_id().as_str(),
            relation: key.relation().as_str(),
            subject_kind: kind,
            subject_type: subject.subject_type().as_str(),
            subject_id: subject.object_id(),
            subject_relation: subject.relation().map_or("", |relation| relation.as_str()),
        }
    }
}

pub(crate) async fn connect_pool(
    config: &PostgresStorageConfig,
    url: &str,
) -> Result<PgPool, StorageError> {
    validate_url_query_keys(url)?;
    let options =
        PgConnectOptions::from_str(url).map_err(|error| map_sqlx(error, "postgres_url_invalid"))?;
    let statement_timeout = format!("{}ms", config.statement_timeout.as_millis().max(1));
    PgPoolOptions::new()
        .max_connections(config.max_connections.get())
        .min_connections(config.min_connections)
        .acquire_timeout(config.acquire_timeout)
        .after_connect(move |connection, _metadata| {
            let timeout = statement_timeout.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                    .bind(timeout)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(|error| map_sqlx(error, "postgres_connect_failed"))
}

fn validate_url_query_keys(url: &str) -> Result<(), StorageError> {
    let Some((_, query_and_fragment)) = url.split_once('?') else {
        return Ok(());
    };
    let query = query_and_fragment
        .split_once('#')
        .map_or(query_and_fragment, |(query, _)| query);
    for parameter in query.split('&').filter(|parameter| !parameter.is_empty()) {
        let key = parameter.split_once('=').map_or(parameter, |(key, _)| key);
        let accepted = matches!(
            key,
            "sslmode"
                | "ssl-mode"
                | "sslrootcert"
                | "ssl-root-cert"
                | "ssl-ca"
                | "sslcert"
                | "ssl-cert"
                | "sslkey"
                | "ssl-key"
                | "statement-cache-capacity"
                | "host"
                | "hostaddr"
                | "port"
                | "dbname"
                | "user"
                | "password"
                | "application_name"
                | "options"
        ) || valid_scoped_option(key);
        if !accepted {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "postgres_url_parameter_not_allowed",
            ));
        }
    }
    Ok(())
}

fn valid_scoped_option(key: &str) -> bool {
    let Some(option) = key
        .strip_prefix("options[")
        .and_then(|value| value.strip_suffix(']'))
    else {
        return false;
    };
    !option.is_empty()
        && option.len() <= 128
        && option
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

async fn verify_schema(pool: &PgPool) -> Result<(), StorageError> {
    let version = sqlx::query_scalar::<_, i64>(
        "SELECT schema_version FROM openfga_schema_metadata WHERE singleton = TRUE",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| map_sqlx(error, "postgres_schema_version_read_failed"))?;
    if version == SCHEMA_VERSION {
        Ok(())
    } else {
        Err(schema_version_error(version))
    }
}

fn schema_version_error(version: i64) -> StorageError {
    if version > SCHEMA_VERSION {
        StorageError::new(
            StorageErrorKind::Integrity,
            "postgres_schema_newer_than_binary",
        )
    } else {
        StorageError::new(
            StorageErrorKind::Unavailable,
            "postgres_schema_migration_required",
        )
    }
}

async fn execute<T>(
    context: &OperationContext,
    future: impl Future<Output = Result<T, sqlx::Error>>,
    code: &'static str,
) -> Result<T, StorageError> {
    context.check()?;
    tokio::pin!(future);
    let deadline = Instant::from_std(context.deadline().instant());
    tokio::select! { biased; () = context.cancellation().cancelled() => Err(cancelled()), () = sleep_until(deadline) => Err(timed_out()), result = &mut future => result.map_err(|error| map_sqlx(error, code)) }
}

fn tuple_select_prefix() -> &'static str {
    "SELECT tuple_payload, (EXTRACT(EPOCH FROM inserted_at) * 1000)::BIGINT AS inserted_at_ms FROM \
     tuples"
}
fn store_select_columns() -> &'static str {
    "SELECT id, name, (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms, \
     (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms FROM stores"
}
fn model_select_columns() -> &'static str {
    "SELECT model_id, schema_version, compiler_format_version, source_fingerprint, source_payload, \
     (EXTRACT(EPOCH FROM written_at) * 1000)::BIGINT AS written_at_ms FROM authorization_models"
}
fn change_select_columns() -> &'static str {
    "SELECT change_id, operation, tuple_payload, (EXTRACT(EPOCH FROM changed_at) * 1000)::BIGINT \
     AS changed_at_ms FROM tuple_changes"
}

fn unique_deletes(deletes: Vec<TupleKey>) -> Result<BTreeSet<TupleKey>, StorageError> {
    let length = deletes.len();
    let keys = deletes.into_iter().collect::<BTreeSet<_>>();
    if keys.len() == length {
        Ok(keys)
    } else {
        Err(StorageError::new(
            StorageErrorKind::Conflict,
            "duplicate_tuple_delete_input",
        ))
    }
}
fn unique_writes(
    writes: Vec<RelationshipTuple>,
) -> Result<BTreeMap<TupleKey, RelationshipTuple>, StorageError> {
    let length = writes.len();
    let tuples = writes
        .into_iter()
        .map(|tuple| (tuple.key().clone(), tuple))
        .collect::<BTreeMap<_, _>>();
    if tuples.len() == length {
        Ok(tuples)
    } else {
        Err(StorageError::new(
            StorageErrorKind::Conflict,
            "duplicate_tuple_write_input",
        ))
    }
}

fn validate_conflict_policy(
    deletes: &[TupleKey],
    writes: &[TupleKey],
    requested: &BTreeMap<TupleKey, RelationshipTuple>,
    existing: &BTreeMap<TupleKey, RelationshipTuple>,
    options: TupleWriteOptions,
) -> Result<(), StorageError> {
    if matches!(options.on_missing_delete(), WriteConflictPolicy::Error)
        && let Some(key) = deletes.iter().find(|key| !existing.contains_key(*key))
    {
        return Err(
            StorageError::new(StorageErrorKind::Conflict, "tuple_delete_missing")
                .with_tuple(key.clone()),
        );
    }
    if matches!(options.on_duplicate_write(), WriteConflictPolicy::Error)
        && let Some(key) = writes.iter().find(|key| existing.contains_key(*key))
    {
        return Err(
            StorageError::new(StorageErrorKind::Conflict, "tuple_write_duplicate")
                .with_tuple(key.clone()),
        );
    }
    if matches!(options.on_duplicate_write(), WriteConflictPolicy::Ignore)
        && let Some(key) = writes.iter().find(|key| {
            existing
                .get(*key)
                .zip(requested.get(*key))
                .is_some_and(|(stored, requested)| stored != requested)
        })
    {
        return Err(
            StorageError::new(StorageErrorKind::Conflict, "tuple_condition_conflict")
                .with_tuple(key.clone()),
        );
    }
    Ok(())
}

async fn read_exact_in_transaction(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
    store_id: StoreId,
    key: &TupleKey,
) -> Result<Option<RelationshipTuple>, StorageError> {
    let parts = TupleParts::from_key(key);
    let payload = execute(
        context,
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT tuple_payload FROM tuples WHERE store_id = $1 AND object_type = $2 AND \
             object_id = $3 AND relation = $4 AND subject_kind = $5 AND subject_type = $6 AND \
             subject_id = $7 AND subject_relation = $8 FOR UPDATE",
        )
        .bind(store_id.to_string())
        .bind(parts.object_type)
        .bind(parts.object_id)
        .bind(parts.relation)
        .bind(parts.subject_kind)
        .bind(parts.subject_type)
        .bind(parts.subject_id)
        .bind(parts.subject_relation)
        .fetch_optional(&mut **transaction),
        "postgres_tuple_lock_read_failed",
    )
    .await?;
    payload.map(|payload| decode_tuple(&payload)).transpose()
}

async fn transaction_timestamp(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i64, StorageError> {
    execute(
        context,
        sqlx::query_scalar::<_, i64>(
            "SELECT (EXTRACT(EPOCH FROM transaction_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&mut **transaction),
        "postgres_transaction_timestamp_failed",
    )
    .await
}

async fn configure_transaction_deadline(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), StorageError> {
    let remaining = context
        .deadline()
        .instant()
        .saturating_duration_since(std::time::Instant::now());
    let milliseconds = remaining.as_millis().max(1).to_string();
    execute(
        context,
        sqlx::query("SELECT set_config('statement_timeout', $1, TRUE)")
            .bind(milliseconds)
            .execute(&mut **transaction),
        "postgres_transaction_deadline_configuration_failed",
    )
    .await
    .map(|_| ())
}

async fn next_change_id(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
    timestamp_ms: i64,
    previous: Option<ChangeId>,
) -> Result<ChangeId, StorageError> {
    let sequence = execute(
        context,
        sqlx::query_scalar::<_, i64>("SELECT nextval('openfga_change_sequence')")
            .fetch_one(&mut **transaction),
        "postgres_change_sequence_failed",
    )
    .await?;
    let mut timestamp = u64::try_from(timestamp_ms).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Internal,
            "postgres_change_timestamp_invalid",
            error,
        )
    })?;
    let mut random = u128::try_from(sequence).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Internal,
            "postgres_change_sequence_invalid",
            error,
        )
    })?;
    if let Some(previous) = previous {
        let previous = previous.as_ulid();
        if timestamp < previous.timestamp_ms()
            || (timestamp == previous.timestamp_ms() && random <= previous.random())
        {
            timestamp = previous.timestamp_ms();
            if previous.random() == (1_u128 << 80) - 1 {
                timestamp = timestamp.checked_add(1).ok_or_else(|| {
                    StorageError::new(
                        StorageErrorKind::Internal,
                        "postgres_change_timestamp_overflow",
                    )
                })?;
                random = 0;
            } else {
                random = previous.random().saturating_add(1);
            }
        }
    }
    if timestamp > ULID_MAX_TIMESTAMP_MS {
        return Err(StorageError::new(
            StorageErrorKind::Internal,
            "postgres_change_timestamp_overflow",
        ));
    }
    ChangeId::try_from(Ulid::from_parts(timestamp, random).to_string()).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Internal,
            "postgres_change_id_invalid",
            error,
        )
    })
}

async fn lock_change_allocator(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Option<ChangeId>, StorageError> {
    let value = execute(
        context,
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT last_change_id FROM openfga_change_allocator WHERE singleton = TRUE FOR UPDATE",
        )
        .fetch_one(&mut **transaction),
        "postgres_change_allocator_lock_failed",
    )
    .await?;
    value
        .map(|value| parse_id(&value, "postgres_change_allocator_invalid"))
        .transpose()
}

async fn update_change_allocator(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
    change_id: ChangeId,
) -> Result<(), StorageError> {
    execute(
        context,
        sqlx::query(
            "UPDATE openfga_change_allocator SET last_change_id = $1 WHERE singleton = TRUE",
        )
        .bind(change_id.to_string())
        .execute(&mut **transaction),
        "postgres_change_allocator_update_failed",
    )
    .await
    .map(|_| ())
}

async fn insert_tuple(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
    store_id: StoreId,
    tuple: &RelationshipTuple,
    timestamp_ms: i64,
) -> Result<(), StorageError> {
    let parts = TupleParts::from_key(tuple.key());
    let payload = encode_tuple(tuple)?;
    let (condition_name, condition_context) = condition_columns(tuple)?;
    execute(
        context,
        sqlx::query(
            "INSERT INTO tuples (store_id, object_type, object_id, relation, subject_kind, \
             subject_type, subject_id, subject_relation, condition_name, condition_context, \
             tuple_payload, inserted_at) VALUES \
             ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,to_timestamp($12::double precision / 1000.0))",
        )
        .bind(store_id.to_string())
        .bind(parts.object_type)
        .bind(parts.object_id)
        .bind(parts.relation)
        .bind(parts.subject_kind)
        .bind(parts.subject_type)
        .bind(parts.subject_id)
        .bind(parts.subject_relation)
        .bind(condition_name)
        .bind(condition_context)
        .bind(payload)
        .bind(timestamp_ms)
        .execute(&mut **transaction),
        "postgres_tuple_insert_failed",
    )
    .await
    .map(|_| ())
}

async fn delete_tuple(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
    store_id: StoreId,
    key: &TupleKey,
) -> Result<(), StorageError> {
    let parts = TupleParts::from_key(key);
    execute(
        context,
        sqlx::query(
            "DELETE FROM tuples WHERE store_id=$1 AND object_type=$2 AND object_id=$3 AND \
             relation=$4 AND subject_kind=$5 AND subject_type=$6 AND subject_id=$7 AND \
             subject_relation=$8",
        )
        .bind(store_id.to_string())
        .bind(parts.object_type)
        .bind(parts.object_id)
        .bind(parts.relation)
        .bind(parts.subject_kind)
        .bind(parts.subject_type)
        .bind(parts.subject_id)
        .bind(parts.subject_relation)
        .execute(&mut **transaction),
        "postgres_tuple_delete_failed",
    )
    .await
    .map(|_| ())
}

async fn insert_change(
    context: &OperationContext,
    transaction: &mut Transaction<'_, Postgres>,
    store_id: StoreId,
    change_id: ChangeId,
    operation: ChangeOperation,
    tuple: &RelationshipTuple,
    timestamp_ms: i64,
) -> Result<(), StorageError> {
    let parts = TupleParts::from_key(tuple.key());
    let payload = encode_tuple(tuple)?;
    let (condition_name, condition_context) = condition_columns(tuple)?;
    let operation = match operation {
        ChangeOperation::Write => CHANGE_WRITE,
        ChangeOperation::Delete => CHANGE_DELETE,
        _ => {
            return Err(StorageError::new(
                StorageErrorKind::Internal,
                "change_operation_unknown",
            ));
        }
    };
    execute(
        context,
        sqlx::query(
            "INSERT INTO tuple_changes (store_id, change_id, object_type, object_id, relation, \
             subject_kind, subject_type, subject_id, subject_relation, condition_name, \
             condition_context, tuple_payload, operation, changed_at) VALUES \
             ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,to_timestamp($14::double precision / \
             1000.0))",
        )
        .bind(store_id.to_string())
        .bind(change_id.to_string())
        .bind(parts.object_type)
        .bind(parts.object_id)
        .bind(parts.relation)
        .bind(parts.subject_kind)
        .bind(parts.subject_type)
        .bind(parts.subject_id)
        .bind(parts.subject_relation)
        .bind(condition_name)
        .bind(condition_context)
        .bind(payload)
        .bind(operation)
        .bind(timestamp_ms)
        .execute(&mut **transaction),
        "postgres_changelog_insert_failed",
    )
    .await
    .map(|_| ())
}

fn condition_columns(
    tuple: &RelationshipTuple,
) -> Result<(Option<String>, Option<Vec<u8>>), StorageError> {
    tuple
        .condition()
        .binding()
        .map_or(Ok((None, None)), |binding| {
            Ok((
                Some(binding.name().to_string()),
                Some(encode_condition_context(binding.context())?),
            ))
        })
}

fn push_tuple_read_filter(query: &mut QueryBuilder<Postgres>, filter: &TupleReadFilter) {
    if let Some(value) = filter.object_type() {
        query.push(" AND object_type = ").push_bind(value.as_str());
    }
    if let Some(value) = filter.object_id() {
        query.push(" AND object_id = ").push_bind(value.as_str());
    }
    if let Some(value) = filter.relation() {
        query.push(" AND relation = ").push_bind(value.as_str());
    }
    if let Some(value) = filter.subject() {
        push_exact_subject(query, value);
    }
}
fn push_exact_subject(query: &mut QueryBuilder<Postgres>, subject: &SubjectRef) {
    let (kind, ty, id, relation) = subject_parts(subject);
    query
        .push(" AND subject_kind = ")
        .push_bind(kind)
        .push(" AND subject_type = ")
        .push_bind(ty.to_owned())
        .push(" AND subject_id = ")
        .push_bind(id.to_owned())
        .push(" AND subject_relation = ")
        .push_bind(relation.to_owned());
}

fn push_subject_allowlist(query: &mut QueryBuilder<Postgres>, subjects: &BTreeSet<SubjectRef>) {
    if subjects.is_empty() {
        return;
    }
    query.push(" AND (");
    let mut separated = query.separated(" OR ");
    for subject in subjects {
        let (kind, ty, id, relation) = subject_parts(subject);
        separated
            .push("(subject_kind = ")
            .push_bind(kind)
            .push(" AND subject_type = ")
            .push_bind(ty.to_owned())
            .push(" AND subject_id = ")
            .push_bind(id.to_owned())
            .push(" AND subject_relation = ")
            .push_bind(relation.to_owned())
            .push_unseparated(")");
    }
    query.push(")");
}

fn push_condition_filter(
    query: &mut QueryBuilder<Postgres>,
    filter: &openfga_storage::ConditionFilter,
) {
    if filter.is_any() {
        return;
    }
    if filter.accepts_unconditional() {
        query.push(" AND condition_name IS NULL");
        return;
    }
    if let Some(names) = filter.names() {
        query.push(" AND condition_name IN (");
        let mut separated = query.separated(", ");
        for name in names {
            separated.push_bind(name.as_str());
        }
        query.push(")");
    }
}

fn subject_parts(subject: &SubjectRef) -> (i16, &str, &str, &str) {
    let kind = match subject.kind() {
        SubjectKind::Object => SUBJECT_OBJECT,
        SubjectKind::Userset => SUBJECT_USERSET,
        SubjectKind::TypedWildcard => SUBJECT_WILDCARD,
    };
    (
        kind,
        subject.subject_type().as_str(),
        subject.object_id(),
        subject.relation().map_or("", |relation| relation.as_str()),
    )
}

fn push_after_tuple(query: &mut QueryBuilder<Postgres>, key: &TupleKey) {
    let parts = TupleParts::from_key(key);
    query
        .push(
            " AND (object_type, object_id, relation, subject_kind, subject_type, subject_id, \
             subject_relation) > (",
        )
        .push_bind(parts.object_type.to_owned())
        .push(",")
        .push_bind(parts.object_id.to_owned())
        .push(",")
        .push_bind(parts.relation.to_owned())
        .push(",")
        .push_bind(parts.subject_kind)
        .push(",")
        .push_bind(parts.subject_type.to_owned())
        .push(",")
        .push_bind(parts.subject_id.to_owned())
        .push(",")
        .push_bind(parts.subject_relation.to_owned())
        .push(")");
}
fn tuple_cursor(cursor: &StorageCursor) -> Result<TupleKey, StorageError> {
    let value = std::str::from_utf8(cursor.as_bytes()).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::InvalidContinuation,
            "postgres_tuple_cursor_utf8",
            error,
        )
    })?;
    value.parse().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::InvalidContinuation,
            "postgres_tuple_cursor_invalid",
            error,
        )
    })
}

fn tuple_page(rows: Vec<TupleRow>, maximum: usize) -> Result<Page<StoredTuple>, StorageError> {
    let has_more = rows.len() > maximum;
    let tuples = rows
        .into_iter()
        .take(maximum)
        .map(TupleRow::into_stored_tuple)
        .collect::<Result<Vec<_>, _>>()?;
    let continuation = if has_more {
        tuples
            .last()
            .map(|tuple| cursor(tuple.tuple().key().to_string()))
            .transpose()?
    } else {
        None
    };
    Ok(Page::new(tuples, continuation))
}
fn rows_to_stream(
    rows: Vec<TupleRow>,
    predicate: impl Fn(&RelationshipTuple) -> bool,
) -> Result<TupleStream, StorageError> {
    rows.into_iter()
        .map(TupleRow::into_stored_tuple)
        .collect::<Result<Vec<_>, _>>()
        .map(|rows| {
            TupleStream::from_tuples(
                rows.into_iter()
                    .map(StoredTuple::into_tuple)
                    .filter(predicate)
                    .collect(),
            )
        })
}

fn cursor(value: String) -> Result<StorageCursor, StorageError> {
    StorageCursor::new(value.into_bytes())
}
fn cursor_id<T>(cursor: &StorageCursor, code: &'static str) -> Result<T, StorageError>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = std::str::from_utf8(cursor.as_bytes()).map_err(|error| {
        StorageError::with_source(StorageErrorKind::InvalidContinuation, code, error)
    })?;
    value.parse().map_err(|error| {
        StorageError::with_source(StorageErrorKind::InvalidContinuation, code, error)
    })
}
fn parse_id<T>(value: &str, code: &'static str) -> Result<T, StorageError>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value
        .parse()
        .map_err(|error| StorageError::with_source(StorageErrorKind::Integrity, code, error))
}
fn page_fetch_limit(maximum: usize) -> Result<i64, StorageError> {
    i64::try_from(maximum.checked_add(1).ok_or_else(|| {
        StorageError::new(
            StorageErrorKind::ResourceExhausted,
            "postgres_page_limit_overflow",
        )
    })?)
    .map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::ResourceExhausted,
            "postgres_page_limit_invalid",
            error,
        )
    })
}
fn result_limit(maximum: usize) -> Result<i64, StorageError> {
    i64::try_from(maximum).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::ResourceExhausted,
            "postgres_result_limit_invalid",
            error,
        )
    })
}
fn system_time_to_millis(value: SystemTime) -> Result<i64, StorageError> {
    let millis = value
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "postgres_timestamp_before_epoch",
                error,
            )
        })?
        .as_millis();
    i64::try_from(millis).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "postgres_timestamp_overflow",
            error,
        )
    })
}
fn millis_to_system_time(value: i64) -> Result<SystemTime, StorageError> {
    let millis = u64::try_from(value).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "postgres_timestamp_negative",
            error,
        )
    })?;
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .ok_or_else(|| {
            StorageError::new(StorageErrorKind::Integrity, "postgres_timestamp_overflow")
        })
}
fn internal_conversion(
    code: &'static str,
) -> impl FnOnce(std::num::TryFromIntError) -> StorageError {
    move |error| StorageError::with_source(StorageErrorKind::Internal, code, error)
}

#[cfg(test)]
mod tests {
    use super::validate_url_query_keys;

    #[test]
    fn test_should_reject_unknown_dsn_parameters_without_retaining_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let error = validate_url_query_keys(
            "postgresql://user@localhost/openfga?unknown=database-secret-canary",
        )
        .err()
        .ok_or("unknown PostgreSQL URL parameter was accepted")?;
        let diagnostic = format!("{error:?} {error}");

        assert_eq!(error.code(), "postgres_url_parameter_not_allowed");
        assert!(!diagnostic.contains("database-secret-canary"));
        Ok(())
    }

    #[test]
    fn test_should_accept_only_sqlx_supported_dsn_parameters() {
        assert!(
            validate_url_query_keys(
                "postgresql://user@localhost/openfga?sslmode=require&application_name=openfga&\
                 options[search_path]=public",
            )
            .is_ok()
        );
        assert!(
            validate_url_query_keys(
                "postgresql://user@localhost/openfga?unkn%6fwn=database-secret-canary",
            )
            .is_err()
        );
    }
}
