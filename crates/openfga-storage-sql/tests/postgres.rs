//! Live `PostgreSQL` contract, fault, concurrency, migration, and plan gates.

use std::{
    error::Error,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use openfga_domain::{
    AuthorizationModelId, ConditionBinding, ConditionContext, ConditionName, ConditionReference,
    ConsistencyPreference, ContextualTuples, Deadline, InputLimits, ObjectRef, RelationshipTuple,
    RequestTimeout, StoreId, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ChangeFilter, ChangeReader, ConditionFilter,
    HealthCheck, ModelReader, ModelWriter, ObjectRelationFilter, OperationContext, PageOptions,
    ReadOptions, StorageCancellationToken, StorageError, StorageErrorKind, StoreFilter, StoreName,
    StoreReader, StoreWriter, StoredAuthorizationModel, TupleReadFilter, TupleReader,
    TupleWriteOptions, TupleWriter, WriteConflictPolicy,
    contract::{TupleContractFixture, verify_tuple_contract},
};
use openfga_storage_sql::{
    MigrationState, PostgresMutationFaultInjector, PostgresMutationStage, PostgresStorage,
    PostgresStorageConfig, apply_migrations, migration_status,
};
use secrecy::SecretString;
use sqlx::{PgPool, Row};
use tokio::task::JoinSet;
use ulid::Ulid;

const TEST_URL_ENV: &str = "OPENFGA_POSTGRES_TEST_URL";

#[tokio::test]
#[ignore = "requires OPENFGA_POSTGRES_TEST_URL"]
async fn test_should_satisfy_postgres_contract_fault_and_plan_gates() -> Result<(), Box<dyn Error>>
{
    let url = std::env::var(TEST_URL_ENV)?;
    let storage = Arc::new(PostgresStorage::connect(config(&url)).await?);
    sqlx::query("TRUNCATE assertions, authorization_models, tuple_changes, tuples, stores CASCADE")
        .execute(storage.primary_pool())
        .await?;
    let context = operation_context(ConsistencyPreference::HigherConsistency)?;

    verify_management_and_shared_contract(&storage, &context).await?;
    verify_request_ordered_conflicts(&storage, &context).await?;
    verify_primary_replica_policy(&url, &context).await?;
    verify_atomic_faults(&url, &context).await?;
    verify_concurrent_mutations(&storage, &context).await?;
    verify_cancellation_releases_pool(&storage).await?;
    verify_hot_query_plans(&storage).await?;
    verify_schema_health(&storage, &context).await?;

    storage.close().await;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential integration flow preserves dependencies across every storage \
              capability"
)]
async fn verify_management_and_shared_contract(
    storage: &PostgresStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    verify_namespace_data_without_store_record(storage, context).await?;
    let store_id = store_id(1);
    let created = storage
        .create_store(
            context,
            store_id,
            StoreName::new("Postgres Store".to_owned())?,
        )
        .await?;
    assert_eq!(created.id(), store_id);
    assert_eq!(
        storage.read_store(context, store_id).await?.name().as_str(),
        "Postgres Store"
    );
    assert_eq!(
        storage
            .list_stores(context, &StoreFilter::all(), &page_options(1)?)
            .await?
            .items()
            .len(),
        1
    );
    assert_eq!(
        storage
            .list_stores(
                context,
                &StoreFilter::named(StoreName::new("Postgres Store".to_owned())?),
                &page_options(1)?,
            )
            .await?
            .items()
            .len(),
        1,
    );
    assert_eq!(
        storage
            .rename_store(
                context,
                store_id,
                StoreName::new("Renamed Store".to_owned())?
            )
            .await?
            .name()
            .as_str(),
        "Renamed Store",
    );

    let first = tuple("document:roadmap#viewer@user:anne")?;
    let second = tuple("document:roadmap#viewer@user:beth")?;
    let filter = ObjectRelationFilter::new(
        "document:roadmap".parse::<ObjectRef>()?,
        "viewer".parse()?,
        Vec::new(),
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    verify_tuple_contract(
        storage,
        context,
        &TupleContractFixture::new(store_id, first.clone(), second, filter, read_options(100)?),
    )
    .await?;
    storage
        .write_tuples(
            context,
            store_id,
            Vec::new(),
            vec![first.clone()],
            TupleWriteOptions::default(),
        )
        .await?;

    let first_page = storage
        .read_tuples(
            context,
            store_id,
            &TupleReadFilter::all(),
            &page_options(1)?,
        )
        .await?;
    assert_eq!(first_page.items().len(), 1);
    let second_page = storage
        .read_tuples(
            context,
            store_id,
            &TupleReadFilter::all(),
            &PageOptions::new(
                NonZeroU32::MIN,
                first_page.continuation().cloned(),
                &InputLimits::default(),
            )?,
        )
        .await?;
    assert_eq!(second_page.items().len(), 1);
    let first_tuple = first_page
        .items()
        .first()
        .ok_or("first page empty")?
        .tuple();
    let second_tuple = second_page
        .items()
        .first()
        .ok_or("second page empty")?
        .tuple();
    assert_ne!(first_tuple, second_tuple);

    let model_id = model_id(10);
    let stored_model = stored_model(store_id, model_id)?;
    storage
        .write_model(context, Arc::clone(&stored_model))
        .await?;
    assert_eq!(
        storage
            .read_model(context, store_id, model_id)
            .await?
            .model_id(),
        &model_id
    );
    assert_eq!(
        storage
            .read_latest_model(context, store_id)
            .await?
            .model_id(),
        &model_id
    );
    assert_eq!(
        storage
            .list_models(context, store_id, &page_options(10)?)
            .await?
            .items()
            .len(),
        1
    );

    let assertion = Assertion::new(
        first.key().clone(),
        true,
        ContextualTuples::new(
            vec![tuple("document:roadmap#viewer@user:carol")?],
            &InputLimits::default(),
        )?,
        ConditionContext::empty(),
    );
    storage
        .write_assertions(context, store_id, model_id, vec![assertion.clone()])
        .await?;
    assert_eq!(
        storage
            .read_assertions(context, store_id, model_id)
            .await?
            .as_ref(),
        &[assertion]
    );
    Ok(())
}

async fn verify_namespace_data_without_store_record(
    storage: &PostgresStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = store_id(99);
    let model_id = model_id(99);
    storage
        .write_model(context, stored_model(store_id, model_id)?)
        .await?;

    let relationship = tuple("document:orphan#viewer@user:anne")?;
    storage
        .write_tuples(
            context,
            store_id,
            Vec::new(),
            vec![relationship.clone()],
            TupleWriteOptions::default(),
        )
        .await?;
    assert_eq!(
        storage
            .read_tuples(
                context,
                store_id,
                &TupleReadFilter::all(),
                &page_options(10)?,
            )
            .await?
            .items()
            .len(),
        1,
    );
    storage
        .write_tuples(
            context,
            store_id,
            vec![relationship.key().clone()],
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await?;
    assert!(
        storage
            .read_tuples(
                context,
                store_id,
                &TupleReadFilter::all(),
                &page_options(10)?,
            )
            .await?
            .items()
            .is_empty(),
    );
    Ok(())
}

async fn verify_primary_replica_policy(
    url: &str,
    primary_context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let primary_url = with_application_name(url, "openfga-primary-test");
    let replica_url = with_application_name(url, "openfga-replica-test");
    let replica_storage = PostgresStorage::connect(
        PostgresStorageConfig::builder()
            .primary_url(SecretString::from(primary_url))
            .replica_url(Some(SecretString::from(replica_url)))
            .max_connections(NonZeroU32::MIN)
            .build(),
    )
    .await?;
    let observer = PgPool::connect(url).await?;
    let store = store_id(1);
    assert!(
        replica_storage
            .read_store(primary_context, store)
            .await
            .is_ok()
    );
    let primary_query = last_backend_query(&observer, "openfga-primary-test").await?;
    assert!(primary_query.contains("FROM stores"), "{primary_query}");
    let latency_context = operation_context(ConsistencyPreference::MinimizeLatency)?;
    assert!(
        replica_storage
            .read_store(&latency_context, store)
            .await
            .is_ok()
    );
    let replica_query = last_backend_query(&observer, "openfga-replica-test").await?;
    assert!(replica_query.contains("FROM stores"), "{replica_query}");
    replica_storage
        .replica_pool()
        .ok_or("replica pool was not configured")?
        .close()
        .await;
    assert!(
        replica_storage
            .read_store(&latency_context, store)
            .await
            .is_ok()
    );
    replica_storage.close().await;
    observer.close().await;
    Ok(())
}

async fn last_backend_query(
    observer: &PgPool,
    application_name: &str,
) -> Result<String, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT query FROM pg_stat_activity WHERE application_name = $1 ORDER BY backend_start \
         DESC LIMIT 1",
    )
    .bind(application_name)
    .fetch_one(observer)
    .await
}

async fn verify_atomic_faults(url: &str, context: &OperationContext) -> Result<(), Box<dyn Error>> {
    let stages = [
        PostgresMutationStage::BeforeLock,
        PostgresMutationStage::AfterLock,
        PostgresMutationStage::AfterDelete,
        PostgresMutationStage::AfterWrite,
        PostgresMutationStage::AfterChangelog,
        PostgresMutationStage::BeforeCommit,
    ];
    for (index, stage) in stages.into_iter().enumerate() {
        let id = store_id(u128::try_from(index)?.saturating_add(100));
        let setup = PostgresStorage::connect(config(url)).await?;
        setup
            .create_store(context, id, StoreName::new(format!("Fault Store {index}"))?)
            .await?;
        let existing = tuple(&format!("document:fault{index}#viewer@user:anne"))?;
        setup
            .write_tuples(
                context,
                id,
                Vec::new(),
                vec![existing.clone()],
                TupleWriteOptions::default(),
            )
            .await?;
        setup.close().await;

        let storage =
            PostgresStorage::connect_with_faults(config(url), Arc::new(FailAt(stage))).await?;
        let replacement = tuple(&format!("document:fault{index}#viewer@user:beth"))?;
        let error = storage
            .write_tuples(
                context,
                id,
                vec![existing.key().clone()],
                vec![replacement.clone()],
                TupleWriteOptions::default(),
            )
            .await
            .err()
            .ok_or("fault did not abort mutation")?;
        assert_eq!(error.kind(), StorageErrorKind::Internal);
        assert!(storage.tuple_exists(context, id, existing.key()).await?);
        assert!(!storage.tuple_exists(context, id, replacement.key()).await?);
        assert_eq!(
            storage
                .read_changes(context, id, &ChangeFilter::default(), &page_options(10)?)
                .await?
                .items()
                .len(),
            1,
        );
        storage.close().await;
    }
    Ok(())
}

async fn verify_concurrent_mutations(
    storage: &Arc<PostgresStorage>,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = store_id(500);
    storage
        .create_store(
            context,
            store_id,
            StoreName::new("Concurrent Store".to_owned())?,
        )
        .await?;
    let relationship = tuple("document:concurrent#viewer@user:anne")?;
    let mut writes = JoinSet::new();
    for _ in 0..32 {
        let storage = Arc::clone(storage);
        let context = context.clone();
        let relationship = relationship.clone();
        writes.spawn(async move {
            storage
                .write_tuples(
                    &context,
                    store_id,
                    Vec::new(),
                    vec![relationship],
                    TupleWriteOptions::new(WriteConflictPolicy::Error, WriteConflictPolicy::Ignore),
                )
                .await
        });
    }
    while let Some(result) = writes.join_next().await {
        result??;
    }
    let changes = storage
        .read_changes(
            context,
            store_id,
            &ChangeFilter::default(),
            &page_options(100)?,
        )
        .await?;
    assert_eq!(changes.items().len(), 1);
    Ok(())
}

async fn verify_request_ordered_conflicts(
    storage: &PostgresStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = store_id(501);
    let z = tuple("document:z#viewer@user:anne")?;
    let a = tuple("document:a#viewer@user:anne")?;
    let missing = storage
        .write_tuples(
            context,
            store_id,
            vec![z.key().clone(), a.key().clone()],
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await
        .err()
        .ok_or("missing ordered deletes unexpectedly succeeded")?;
    assert_eq!(
        missing.tuple().map(ToString::to_string).as_deref(),
        Some("document:z#viewer@user:anne"),
    );

    storage
        .write_tuples(
            context,
            store_id,
            Vec::new(),
            vec![z.clone(), a.clone()],
            TupleWriteOptions::default(),
        )
        .await?;
    let duplicate = storage
        .write_tuples(
            context,
            store_id,
            Vec::new(),
            vec![z, a],
            TupleWriteOptions::default(),
        )
        .await
        .err()
        .ok_or("ordered duplicate writes unexpectedly succeeded")?;
    assert_eq!(
        duplicate.tuple().map(ToString::to_string).as_deref(),
        Some("document:z#viewer@user:anne"),
    );
    let condition_conflict = RelationshipTuple::new(
        duplicate
            .tuple()
            .cloned()
            .ok_or("ordered conflict did not retain its tuple")?,
        ConditionReference::Conditional(ConditionBinding::new(
            ConditionName::parse_with_limits("alternate", &InputLimits::default())?,
            ConditionContext::empty(),
        )),
    );
    let conflict = storage
        .write_tuples(
            context,
            store_id,
            Vec::new(),
            vec![condition_conflict],
            TupleWriteOptions::new(WriteConflictPolicy::Error, WriteConflictPolicy::Ignore),
        )
        .await
        .err()
        .ok_or("ignore accepted a different condition on an existing tuple")?;
    assert_eq!(conflict.code(), "tuple_condition_conflict");
    Ok(())
}

async fn verify_cancellation_releases_pool(
    storage: &PostgresStorage,
) -> Result<(), Box<dyn Error>> {
    let context = operation_context(ConsistencyPreference::HigherConsistency)?;
    let other_store_id = store_id(701);
    let store_id = store_id(700);
    storage
        .create_store(
            &context,
            store_id,
            StoreName::new("Cancellation Store".to_owned())?,
        )
        .await?;
    let relationship = tuple("document:blocked#viewer@user:anne")?;
    let mut blocker = storage.primary_pool().begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($2, hashtextextended($1, 0)))")
        .bind(store_id.to_string())
        .bind(relationship.key().to_string())
        .execute(&mut *blocker)
        .await?;

    storage
        .write_tuples(
            &context,
            other_store_id,
            Vec::new(),
            vec![relationship.clone()],
            TupleWriteOptions::default(),
        )
        .await?;

    let timed_context = OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(
            Instant::now(),
            RequestTimeout::new(Duration::from_millis(20))?,
        )?,
        StorageCancellationToken::new(),
    );
    let error = storage
        .write_tuples(
            &timed_context,
            store_id,
            Vec::new(),
            vec![relationship.clone()],
            TupleWriteOptions::default(),
        )
        .await
        .err()
        .ok_or("blocked mutation ignored deadline")?;
    assert_eq!(error.kind(), StorageErrorKind::Timeout);
    blocker.rollback().await?;

    storage
        .write_tuples(
            &context,
            store_id,
            Vec::new(),
            vec![relationship],
            TupleWriteOptions::default(),
        )
        .await?;
    wait_for_idle_pool(storage).await
}

async fn wait_for_idle_pool(storage: &PostgresStorage) -> Result<(), Box<dyn Error>> {
    for _ in 0..100 {
        if usize::try_from(storage.primary_pool().size())? == storage.primary_pool().num_idle() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("PostgreSQL pool did not release every connection after cancellation".into())
}

async fn verify_hot_query_plans(storage: &PostgresStorage) -> Result<(), Box<dyn Error>> {
    let store = store_id(900);
    let store_text = store.to_string();
    sqlx::query(
        "INSERT INTO stores (id, name, created_at, updated_at) VALUES ($1, 'Plan Store', \
         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(&store_text)
    .execute(storage.primary_pool())
    .await?;
    sqlx::query(
        "INSERT INTO tuples (store_id, object_type, object_id, relation, subject_kind, \
         subject_type, subject_id, subject_relation, tuple_payload, inserted_at) SELECT $1, CASE \
         WHEN value % 100 = 0 THEN 'document' ELSE 'folder' END, value::text, 'viewer', 0, \
         'user', ('user-' || value::text), '', '{}'::bytea, CURRENT_TIMESTAMP FROM \
         generate_series(1, 10000) AS value",
    )
    .bind(&store_text)
    .execute(storage.primary_pool())
    .await?;
    sqlx::query(
        "INSERT INTO tuple_changes (store_id, change_id, object_type, object_id, relation, \
         subject_kind, subject_type, subject_id, subject_relation, tuple_payload, operation, \
         changed_at) SELECT $1, lpad(value::text, 26, '0'), CASE WHEN value % 100 = 0 THEN \
         'document' ELSE 'folder' END, value::text, 'viewer', 0, 'user', ('user-' || \
         value::text), '', '{}'::bytea, 0, CURRENT_TIMESTAMP FROM generate_series(1, 10000) AS \
         value",
    )
    .bind(&store_text)
    .execute(storage.primary_pool())
    .await?;
    sqlx::query("ANALYZE tuples")
        .execute(storage.primary_pool())
        .await?;
    sqlx::query("ANALYZE tuple_changes")
        .execute(storage.primary_pool())
        .await?;

    let forward = explain(
        storage,
        "EXPLAIN (COSTS OFF) SELECT tuple_payload FROM tuples WHERE store_id = $1 AND object_type \
         = 'document' AND object_id = '5000' AND relation = 'viewer' ORDER BY subject_kind, \
         subject_type, subject_id, subject_relation LIMIT 100",
        &store_text,
    )
    .await?;
    assert!(
        forward.contains("tuples_pkey") || forward.contains("tuples_forward_idx"),
        "{forward}"
    );
    let reverse = explain(
        storage,
        "EXPLAIN (COSTS OFF) SELECT tuple_payload FROM tuples WHERE store_id = $1 AND \
         subject_kind = 0 AND subject_type = 'user' AND subject_id = 'user-5000' AND \
         subject_relation = '' AND object_type = 'document' AND relation = 'viewer' ORDER BY \
         object_id LIMIT 100",
        &store_text,
    )
    .await?;
    assert!(reverse.contains("tuples_reverse_idx"), "{reverse}");
    let changes = explain(
        storage,
        "EXPLAIN (COSTS OFF) SELECT change_id FROM tuple_changes WHERE store_id = $1 AND \
         object_type = 'document' ORDER BY change_id LIMIT 100",
        &store_text,
    )
    .await?;
    assert!(
        changes.contains("tuple_changes_object_type_idx"),
        "{changes}"
    );
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()",
    )
    .fetch_all(storage.primary_pool())
    .await?
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    for expected in [
        "assertions_pkey",
        "authorization_models_latest_idx",
        "authorization_models_pkey",
        "stores_active_id_idx",
        "stores_pkey",
        "tuple_changes_object_type_idx",
        "tuple_changes_pkey",
        "tuple_changes_time_idx",
        "tuples_forward_idx",
        "tuples_pkey",
        "tuples_reverse_idx",
        "tuples_userset_idx",
    ] {
        assert!(indexes.contains(expected), "missing index {expected}");
    }
    Ok(())
}

async fn explain(
    storage: &PostgresStorage,
    sql: &'static str,
    store_id: &str,
) -> Result<String, Box<dyn Error>> {
    let rows = sqlx::query(sql)
        .bind(store_id)
        .fetch_all(storage.primary_pool())
        .await?;
    rows.into_iter()
        .map(|row| row.try_get::<String, _>(0))
        .collect::<Result<Vec<_>, _>>()
        .map(|lines| lines.join("\n"))
        .map_err(Into::into)
}

async fn verify_schema_health(
    storage: &PostgresStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    assert!(storage.health(context).await?.is_ready());
    assert_eq!(
        migration_status(&config_without_migration(&std::env::var(TEST_URL_ENV)?))
            .await?
            .state(),
        MigrationState::Current,
    );
    let url = std::env::var(TEST_URL_ENV)?;
    let first_config = config_without_migration(&url);
    let second_config = config_without_migration(&url);
    let (first_migration, second_migration) = tokio::join!(
        apply_migrations(&first_config),
        apply_migrations(&second_config),
    );
    assert_eq!(first_migration?.state(), MigrationState::Current);
    assert_eq!(second_migration?.state(), MigrationState::Current);
    let cancelled = StorageCancellationToken::new();
    cancelled.cancel();
    let cancelled_context = OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        context.deadline(),
        cancelled,
    );
    let error = storage
        .health(&cancelled_context)
        .await
        .err()
        .ok_or("cancelled health succeeded")?;
    assert_eq!(error.kind(), StorageErrorKind::Cancelled);

    sqlx::query("UPDATE openfga_schema_metadata SET schema_version = 202608050002")
        .execute(storage.primary_pool())
        .await?;
    let newer = PostgresStorage::connect(config_without_migration(&url))
        .await
        .err()
        .ok_or("newer schema was accepted")?;
    assert_eq!(newer.kind(), StorageErrorKind::Integrity);

    sqlx::query("UPDATE openfga_schema_metadata SET schema_version = 202608050000")
        .execute(storage.primary_pool())
        .await?;
    let older = PostgresStorage::connect(config_without_migration(&url))
        .await
        .err()
        .ok_or("older schema was accepted")?;
    assert_eq!(older.kind(), StorageErrorKind::Unavailable);
    sqlx::query("UPDATE openfga_schema_metadata SET schema_version = 202608050001")
        .execute(storage.primary_pool())
        .await?;

    let checksum = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT checksum FROM _sqlx_migrations WHERE version = 202608050001",
    )
    .fetch_one(storage.primary_pool())
    .await?;
    sqlx::query(
        "UPDATE _sqlx_migrations SET checksum = decode('00', 'hex') WHERE version = 202608050001",
    )
    .execute(storage.primary_pool())
    .await?;
    let checksum_error = PostgresStorage::connect(config(&url))
        .await
        .err()
        .ok_or("migration checksum mismatch was accepted")?;
    assert_eq!(checksum_error.kind(), StorageErrorKind::Integrity);
    let checksum_status = migration_status(&config_without_migration(&url))
        .await
        .err()
        .ok_or("status accepted a migration checksum mismatch")?;
    assert_eq!(checksum_status.kind(), StorageErrorKind::Integrity);
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 202608050001")
        .bind(checksum)
        .execute(storage.primary_pool())
        .await?;

    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (202608050002, 'future', TRUE, decode('00', 'hex'), 0)",
    )
    .execute(storage.primary_pool())
    .await?;
    assert_eq!(
        migration_status(&config_without_migration(&url))
            .await?
            .state(),
        MigrationState::TooNew,
    );
    sqlx::query("UPDATE _sqlx_migrations SET success = FALSE WHERE version = 202608050002")
        .execute(storage.primary_pool())
        .await?;
    let interrupted = migration_status(&config_without_migration(&url))
        .await
        .err()
        .ok_or("status accepted an interrupted migration")?;
    assert_eq!(interrupted.kind(), StorageErrorKind::Integrity);
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 202608050002")
        .execute(storage.primary_pool())
        .await?;
    Ok(())
}

fn config(url: &str) -> PostgresStorageConfig {
    PostgresStorageConfig::builder()
        .primary_url(SecretString::from(url.to_owned()))
        .build()
}

fn config_without_migration(url: &str) -> PostgresStorageConfig {
    PostgresStorageConfig::builder()
        .primary_url(SecretString::from(url.to_owned()))
        .migrate_on_connect(false)
        .build()
}

fn with_application_name(url: &str, application_name: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}application_name={application_name}")
}

fn operation_context(
    consistency: ConsistencyPreference,
) -> Result<OperationContext, Box<dyn Error>> {
    Ok(OperationContext::new(
        consistency,
        Deadline::from_timeout(
            Instant::now(),
            RequestTimeout::new(Duration::from_secs(20))?,
        )?,
        StorageCancellationToken::new(),
    ))
}

fn page_options(maximum: u32) -> Result<PageOptions, Box<dyn Error>> {
    Ok(PageOptions::new(
        NonZeroU32::new(maximum).ok_or("zero page")?,
        None,
        &InputLimits::default(),
    )?)
}

fn read_options(maximum: u32) -> Result<ReadOptions, Box<dyn Error>> {
    Ok(ReadOptions::new(
        NonZeroU32::new(maximum).ok_or("zero read")?,
        &InputLimits::default(),
    )?)
}

fn tuple(value: &str) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::unconditional(value.parse::<TupleKey>()?))
}

fn store_id(random: u128) -> StoreId {
    StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, random))
}

fn model_id(random: u128) -> AuthorizationModelId {
    AuthorizationModelId::from_ulid(Ulid::from_parts(1_700_000_000_100, random))
}

fn stored_model(
    store_id: StoreId,
    model_id: AuthorizationModelId,
) -> Result<Arc<StoredAuthorizationModel>, Box<dyn Error>> {
    let source = Arc::new(AuthorizationModelSource::new(
        store_id,
        model_id,
        "1.1".to_owned(),
        vec![
            TypeDefinitionSource::new("user".parse()?, Vec::new()),
            TypeDefinitionSource::new(
                "document".parse()?,
                vec![RelationSource::new(
                    "viewer".parse()?,
                    RewriteSource::Direct,
                    vec![DirectRestrictionSource::new(
                        "user".parse()?,
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

#[derive(Debug)]
struct FailAt(PostgresMutationStage);

impl PostgresMutationFaultInjector for FailAt {
    fn check(&self, stage: PostgresMutationStage) -> Result<(), StorageError> {
        if stage == self.0 {
            Err(StorageError::new(
                StorageErrorKind::Internal,
                "injected_postgres_mutation_fault",
            ))
        } else {
            Ok(())
        }
    }
}
