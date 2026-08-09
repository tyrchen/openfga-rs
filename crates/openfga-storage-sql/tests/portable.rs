//! Shared `MySQL` and `SQLite` storage contract and query-plan gates.

use std::{
    error::Error,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use openfga_domain::{
    AuthorizationModelId, ConsistencyPreference, Deadline, InputLimits, ObjectRef,
    RelationshipTuple, RequestTimeout, StoreId, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    ChangeFilter, ChangeReader, ConditionFilter, HealthCheck, ModelReader, ModelWriter,
    ObjectRelationFilter, OperationContext, PageOptions, ReadOptions, StorageCancellationToken,
    StorageError, StorageErrorKind, StoreFilter, StoreName, StoreReader, StoreWriter,
    StoredAuthorizationModel, TupleReader, TupleWriteOptions, TupleWriter, WriteConflictPolicy,
    contract::{TupleContractFixture, verify_tuple_contract},
};
use openfga_storage_sql::{
    MigrationState, PortableSqlDialect, PortableSqlStorage, PortableSqlStorageConfig,
    SqlMutationFaultInjector, SqlMutationStage, apply_portable_migrations,
    portable_migration_status,
};
use secrecy::SecretString;
use sqlx::Row;
use ulid::Ulid;

const MYSQL_TEST_URL_ENV: &str = "OPENFGA_MYSQL_TEST_URL";
const PORTABLE_SCHEMA_VERSION: i64 = 202_608_080_001;

#[tokio::test]
async fn test_should_satisfy_sqlite_contract_and_plan_gates() -> Result<(), Box<dyn Error>> {
    let storage = Arc::new(
        PortableSqlStorage::connect(config(PortableSqlDialect::Sqlite, "sqlite::memory:")?).await?,
    );
    verify_backend(&storage).await?;
    verify_all_fault_stages(PortableSqlDialect::Sqlite, "sqlite::memory:").await?;

    let plan_rows = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT tuple_payload FROM tuples WHERE store_id = ? AND object_type = \
         ? AND object_id = ? AND relation = ? ORDER BY subject_kind, subject_type, subject_id, \
         subject_relation LIMIT 100",
    )
    .bind(store_id(1).to_string())
    .bind("document")
    .bind("roadmap")
    .bind("viewer")
    .fetch_all(storage.primary_pool())
    .await?;
    let plans = plan_rows
        .into_iter()
        .map(|row| row.try_get::<String, _>(3))
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    assert!(
        plans.contains("tuples_forward_idx") || plans.contains("sqlite_autoindex_tuples"),
        "{plans}",
    );
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name IS NOT NULL",
    )
    .fetch_all(storage.primary_pool())
    .await?
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    verify_index_inventory(&indexes)?;
    storage.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires OPENFGA_MYSQL_TEST_URL"]
async fn test_should_satisfy_mysql_contract_and_plan_gates() -> Result<(), Box<dyn Error>> {
    let url = std::env::var(MYSQL_TEST_URL_ENV)?;
    let storage =
        Arc::new(PortableSqlStorage::connect(config(PortableSqlDialect::MySql, &url)?).await?);
    sqlx::query("SET FOREIGN_KEY_CHECKS = 0")
        .execute(storage.primary_pool())
        .await?;
    for statement in [
        "TRUNCATE TABLE assertions",
        "TRUNCATE TABLE authorization_models",
        "TRUNCATE TABLE tuple_changes",
        "TRUNCATE TABLE tuples",
        "TRUNCATE TABLE stores",
    ] {
        sqlx::query(statement)
            .execute(storage.primary_pool())
            .await?;
    }
    sqlx::query("UPDATE openfga_change_allocator SET last_change_id = NULL WHERE singleton = TRUE")
        .execute(storage.primary_pool())
        .await?;
    sqlx::query("SET FOREIGN_KEY_CHECKS = 1")
        .execute(storage.primary_pool())
        .await?;
    verify_backend(&storage).await?;
    verify_mysql_query_plans(&storage).await?;
    verify_migration_history_barriers(&storage, PortableSqlDialect::MySql, &url).await?;
    verify_too_new_rollback_barrier(&storage, PortableSqlDialect::MySql, &url).await?;
    storage.close().await;
    verify_all_fault_stages(PortableSqlDialect::MySql, &url).await?;
    verify_concurrent_migrations(PortableSqlDialect::MySql, &url).await?;
    Ok(())
}

async fn verify_mysql_query_plans(storage: &PortableSqlStorage) -> Result<(), Box<dyn Error>> {
    let rows = sqlx::query(
        "EXPLAIN SELECT tuple_payload FROM tuples WHERE store_id = ? AND object_type = ? AND \
         object_id = ? AND relation = ? ORDER BY subject_kind, subject_type, subject_id, \
         subject_relation LIMIT 100",
    )
    .bind(store_id(1).to_string())
    .bind("document")
    .bind("roadmap")
    .bind("viewer")
    .fetch_all(storage.primary_pool())
    .await?;
    let keys = rows
        .into_iter()
        .filter_map(|row| row.try_get::<Option<String>, _>("key").ok().flatten())
        .collect::<Vec<_>>();
    assert!(
        keys.iter()
            .any(|key| key == "PRIMARY" || key == "tuples_forward_idx"),
        "MySQL forward query did not use an expected index: {keys:?}",
    );
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT index_name FROM information_schema.statistics WHERE table_schema = \
         DATABASE()",
    )
    .fetch_all(storage.primary_pool())
    .await?
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    verify_index_inventory(&indexes)
}

fn verify_index_inventory(
    indexes: &std::collections::BTreeSet<String>,
) -> Result<(), Box<dyn Error>> {
    for expected in [
        "authorization_models_latest_idx",
        "stores_active_id_idx",
        "tuple_changes_object_type_idx",
        "tuple_changes_time_idx",
        "tuples_forward_idx",
        "tuples_reverse_idx",
        "tuples_userset_idx",
    ] {
        if !indexes.contains(expected) {
            return Err(format!("missing SQL index {expected}").into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_should_backup_restore_and_rollback_sqlite() -> Result<(), Box<dyn Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!("openfga-phase5-{}-{nonce}", std::process::id()));
    tokio::fs::create_dir(&root).await?;
    let source = root.join("source.db");
    let backup = root.join("backup.db");
    let restore = root.join("restore.db");
    let source_url = sqlite_url(&source, "rwc")?;
    let storage =
        PortableSqlStorage::connect(config(PortableSqlDialect::Sqlite, &source_url)?).await?;
    let context = operation_context()?;
    let store_id = store_id(900);
    storage
        .create_store(
            &context,
            store_id,
            StoreName::new("Restore Drill".to_owned())?,
        )
        .await?;
    storage.close().await;

    tokio::fs::copy(&source, &backup).await?;
    tokio::fs::copy(&backup, &restore).await?;
    let restore_url = sqlite_url(&restore, "rw")?;
    let restored = PortableSqlStorage::connect(config_without_migration(
        PortableSqlDialect::Sqlite,
        &restore_url,
    )?)
    .await?;
    assert_eq!(
        restored
            .read_store(&context, store_id)
            .await?
            .name()
            .as_str(),
        "Restore Drill",
    );
    assert_eq!(
        portable_migration_status(&config_without_migration(
            PortableSqlDialect::Sqlite,
            &restore_url,
        )?)
        .await?
        .state(),
        MigrationState::Current,
    );
    verify_migration_history_barriers(&restored, PortableSqlDialect::Sqlite, &restore_url).await?;
    restored.close().await;

    let concurrent = root.join("concurrent.db");
    let concurrent_url = sqlite_url(&concurrent, "rwc")?;
    verify_concurrent_migrations(PortableSqlDialect::Sqlite, &concurrent_url).await?;

    let source_storage = PortableSqlStorage::connect(config_without_migration(
        PortableSqlDialect::Sqlite,
        &source_url,
    )?)
    .await?;
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, \
         execution_time) SELECT ?, 'future schema drill', installed_on, success, checksum, \
         execution_time FROM _sqlx_migrations WHERE version = ?",
    )
    .bind(PORTABLE_SCHEMA_VERSION.saturating_add(1))
    .bind(PORTABLE_SCHEMA_VERSION)
    .execute(source_storage.primary_pool())
    .await?;
    sqlx::query("UPDATE openfga_schema_metadata SET schema_version = ?")
        .bind(PORTABLE_SCHEMA_VERSION.saturating_add(1))
        .execute(source_storage.primary_pool())
        .await?;
    source_storage.close().await;
    assert_eq!(
        portable_migration_status(&config_without_migration(
            PortableSqlDialect::Sqlite,
            &source_url,
        )?)
        .await?
        .state(),
        MigrationState::TooNew,
    );
    let newer = PortableSqlStorage::connect(config_without_migration(
        PortableSqlDialect::Sqlite,
        &source_url,
    )?)
    .await
    .err()
    .ok_or("newer SQLite schema was accepted")?;
    assert_eq!(newer.kind(), StorageErrorKind::Integrity);

    tokio::fs::remove_file(source).await?;
    tokio::fs::remove_file(backup).await?;
    tokio::fs::remove_file(restore).await?;
    tokio::fs::remove_file(concurrent).await?;
    tokio::fs::remove_dir(root).await?;
    Ok(())
}

async fn verify_migration_history_barriers(
    storage: &PortableSqlStorage,
    dialect: PortableSqlDialect,
    url: &str,
) -> Result<(), Box<dyn Error>> {
    let checksum =
        sqlx::query_scalar::<_, Vec<u8>>("SELECT checksum FROM _sqlx_migrations WHERE version = ?")
            .bind(PORTABLE_SCHEMA_VERSION)
            .fetch_one(storage.primary_pool())
            .await?;
    let mut corrupted = checksum.clone();
    let first = corrupted
        .first_mut()
        .ok_or("embedded migration checksum was empty")?;
    *first ^= 0xff;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = ?")
        .bind(corrupted)
        .bind(PORTABLE_SCHEMA_VERSION)
        .execute(storage.primary_pool())
        .await?;
    let checksum_result = portable_migration_status(&config_without_migration(dialect, url)?).await;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = ?")
        .bind(checksum)
        .bind(PORTABLE_SCHEMA_VERSION)
        .execute(storage.primary_pool())
        .await?;
    assert_eq!(
        checksum_result
            .err()
            .ok_or("corrupt portable migration checksum was accepted")?
            .kind(),
        StorageErrorKind::Integrity,
    );

    sqlx::query("UPDATE _sqlx_migrations SET success = FALSE WHERE version = ?")
        .bind(PORTABLE_SCHEMA_VERSION)
        .execute(storage.primary_pool())
        .await?;
    let interrupted_result =
        portable_migration_status(&config_without_migration(dialect, url)?).await;
    sqlx::query("UPDATE _sqlx_migrations SET success = TRUE WHERE version = ?")
        .bind(PORTABLE_SCHEMA_VERSION)
        .execute(storage.primary_pool())
        .await?;
    assert_eq!(
        interrupted_result
            .err()
            .ok_or("interrupted portable migration history was accepted")?
            .kind(),
        StorageErrorKind::Integrity,
    );
    Ok(())
}

async fn verify_concurrent_migrations(
    dialect: PortableSqlDialect,
    url: &str,
) -> Result<(), Box<dyn Error>> {
    if dialect == PortableSqlDialect::MySql {
        let pool = sqlx::AnyPool::connect(url).await?;
        sqlx::query("SET FOREIGN_KEY_CHECKS = 0")
            .execute(&pool)
            .await?;
        for statement in [
            "DROP TABLE IF EXISTS assertions",
            "DROP TABLE IF EXISTS authorization_models",
            "DROP TABLE IF EXISTS tuple_changes",
            "DROP TABLE IF EXISTS tuples",
            "DROP TABLE IF EXISTS stores",
            "DROP TABLE IF EXISTS openfga_change_allocator",
            "DROP TABLE IF EXISTS openfga_schema_metadata",
            "DROP TABLE IF EXISTS _sqlx_migrations",
        ] {
            sqlx::query(statement).execute(&pool).await?;
        }
        sqlx::query("SET FOREIGN_KEY_CHECKS = 1")
            .execute(&pool)
            .await?;
        pool.close().await;
    }
    let left_config = config(dialect, url)?;
    let right_config = left_config.clone();
    let (left, right) = tokio::join!(
        apply_portable_migrations(&left_config),
        apply_portable_migrations(&right_config),
    );
    assert_eq!(left?.state(), MigrationState::Current);
    assert_eq!(right?.state(), MigrationState::Current);
    assert_eq!(
        portable_migration_status(&config_without_migration(dialect, url)?)
            .await?
            .state(),
        MigrationState::Current,
    );
    Ok(())
}

async fn verify_too_new_rollback_barrier(
    storage: &PortableSqlStorage,
    dialect: PortableSqlDialect,
    url: &str,
) -> Result<(), Box<dyn Error>> {
    let future_version = PORTABLE_SCHEMA_VERSION.saturating_add(1);
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, \
         execution_time) SELECT ?, 'future schema drill', installed_on, success, checksum, \
         execution_time FROM _sqlx_migrations WHERE version = ?",
    )
    .bind(future_version)
    .bind(PORTABLE_SCHEMA_VERSION)
    .execute(storage.primary_pool())
    .await?;
    sqlx::query("UPDATE openfga_schema_metadata SET schema_version = ?")
        .bind(future_version)
        .execute(storage.primary_pool())
        .await?;
    let status = portable_migration_status(&config_without_migration(dialect, url)?).await;
    sqlx::query("UPDATE openfga_schema_metadata SET schema_version = ?")
        .bind(PORTABLE_SCHEMA_VERSION)
        .execute(storage.primary_pool())
        .await?;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = ?")
        .bind(future_version)
        .execute(storage.primary_pool())
        .await?;
    assert_eq!(status?.state(), MigrationState::TooNew);
    Ok(())
}

async fn verify_all_fault_stages(
    dialect: PortableSqlDialect,
    url: &str,
) -> Result<(), Box<dyn Error>> {
    let stages = [
        SqlMutationStage::BeforeLock,
        SqlMutationStage::AfterLock,
        SqlMutationStage::AfterDelete,
        SqlMutationStage::AfterWrite,
        SqlMutationStage::AfterChangelog,
        SqlMutationStage::BeforeCommit,
    ];
    for (index, stage) in stages.into_iter().enumerate() {
        let injector = Arc::new(FailWhenArmed::new(stage));
        let storage =
            PortableSqlStorage::connect_with_faults(config(dialect, url)?, injector.clone())
                .await?;
        let context = operation_context()?;
        let store_id = store_id(u128::try_from(index)?.saturating_add(100));
        let existing = tuple(&format!("document:fault{index}#viewer@user:anne"))?;
        let replacement = tuple(&format!("document:fault{index}#viewer@user:beth"))?;
        storage
            .write_tuples(
                &context,
                store_id,
                Vec::new(),
                vec![existing.clone()],
                TupleWriteOptions::default(),
            )
            .await?;
        injector.arm();
        let error = storage
            .write_tuples(
                &context,
                store_id,
                vec![existing.key().clone()],
                vec![replacement.clone()],
                TupleWriteOptions::default(),
            )
            .await
            .err()
            .ok_or("fault did not abort portable SQL mutation")?;
        assert_eq!(error.kind(), StorageErrorKind::Internal);
        assert!(
            storage
                .tuple_exists(&context, store_id, existing.key())
                .await?
        );
        assert!(
            !storage
                .tuple_exists(&context, store_id, replacement.key())
                .await?
        );
        assert_eq!(
            storage
                .read_changes(
                    &context,
                    store_id,
                    &ChangeFilter::default(),
                    &PageOptions::from_read_options(read_options(10)?),
                )
                .await?
                .items()
                .len(),
            1,
        );
        storage.close().await;
    }
    Ok(())
}

async fn verify_backend(storage: &Arc<PortableSqlStorage>) -> Result<(), Box<dyn Error>> {
    let context = operation_context()?;
    assert!(storage.health(&context).await?.is_ready());
    let store_id = store_id(1);
    storage
        .create_store(
            &context,
            store_id,
            StoreName::new("Portable Store".to_owned())?,
        )
        .await?;
    assert_eq!(
        storage
            .read_store(&context, store_id)
            .await?
            .name()
            .as_str(),
        "Portable Store",
    );
    let renamed = storage
        .rename_store(
            &context,
            store_id,
            StoreName::new("Renamed Portable Store".to_owned())?,
        )
        .await?;
    assert_eq!(renamed.name().as_str(), "Renamed Portable Store");
    assert!(
        storage
            .list_stores(
                &context,
                &StoreFilter::named(StoreName::new("renamed Portable Store".to_owned())?),
                &PageOptions::from_read_options(read_options(10)?),
            )
            .await?
            .items()
            .is_empty(),
    );
    let stores = storage
        .list_stores(
            &context,
            &StoreFilter::all(),
            &PageOptions::from_read_options(read_options(10)?),
        )
        .await?;
    assert!(stores.items().iter().any(|record| record.id() == store_id));

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
        storage.as_ref(),
        &context,
        &TupleContractFixture::new(store_id, first, second, filter, read_options(2)?),
    )
    .await?;

    let model_id = model_id(1);
    let model = stored_model(store_id, model_id)?;
    storage.write_model(&context, Arc::clone(&model)).await?;
    assert_eq!(
        storage
            .read_model(&context, store_id, model_id)
            .await?
            .model_id(),
        &model_id,
    );
    assert_eq!(
        storage
            .read_latest_model(&context, store_id)
            .await?
            .model_id(),
        &model_id,
    );
    assert_eq!(
        storage
            .list_models(
                &context,
                store_id,
                &PageOptions::from_read_options(read_options(10)?),
            )
            .await?
            .items()
            .len(),
        1,
    );
    verify_concurrent_mutations(storage, &context).await?;
    Ok(())
}

async fn verify_concurrent_mutations(
    storage: &Arc<PortableSqlStorage>,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = store_id(700);
    let relationship = tuple("document:concurrent#viewer@user:anne")?;
    let start = Arc::new(tokio::sync::Barrier::new(32));
    let mut writes = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let storage = Arc::clone(storage);
        let context = context.clone();
        let relationship = relationship.clone();
        let start = Arc::clone(&start);
        writes.spawn(async move {
            start.wait().await;
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
    assert_eq!(
        storage
            .read_changes(
                context,
                store_id,
                &ChangeFilter::default(),
                &PageOptions::from_read_options(read_options(100)?),
            )
            .await?
            .items()
            .len(),
        1,
    );
    Ok(())
}

fn config(
    dialect: PortableSqlDialect,
    url: &str,
) -> Result<PortableSqlStorageConfig, Box<dyn Error>> {
    let maximum_connections = match dialect {
        PortableSqlDialect::MySql => NonZeroU32::new(8).ok_or("zero MySQL pool")?,
        PortableSqlDialect::Sqlite => NonZeroU32::MIN,
        _ => return Err("unsupported portable SQL dialect".into()),
    };
    Ok(PortableSqlStorageConfig::builder()
        .dialect(dialect)
        .primary_url(SecretString::from(url.to_owned()))
        .max_connections(maximum_connections)
        .build())
}

fn config_without_migration(
    dialect: PortableSqlDialect,
    url: &str,
) -> Result<PortableSqlStorageConfig, Box<dyn Error>> {
    Ok(PortableSqlStorageConfig::builder()
        .dialect(dialect)
        .primary_url(SecretString::from(url.to_owned()))
        .max_connections(match dialect {
            PortableSqlDialect::MySql => NonZeroU32::new(8).ok_or("zero MySQL pool")?,
            PortableSqlDialect::Sqlite => NonZeroU32::MIN,
            _ => return Err("unsupported portable SQL dialect".into()),
        })
        .min_connections(0)
        .acquire_timeout(Duration::from_secs(5))
        .statement_timeout(Duration::from_secs(5))
        .max_tuple_mutations(NonZeroU32::new(100).ok_or("zero tuple limit")?)
        .migrate_on_connect(false)
        .build())
}

fn sqlite_url(path: &std::path::Path, mode: &str) -> Result<String, Box<dyn Error>> {
    let path = path.to_str().ok_or("SQLite drill path is not UTF-8")?;
    if !matches!(mode, "rw" | "rwc") {
        return Err("invalid SQLite drill mode".into());
    }
    Ok(format!("sqlite://{path}?mode={mode}"))
}

fn operation_context() -> Result<OperationContext, Box<dyn Error>> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(
            Instant::now(),
            RequestTimeout::new(Duration::from_secs(20))?,
        )?,
        StorageCancellationToken::new(),
    ))
}

fn read_options(maximum: u32) -> Result<ReadOptions, Box<dyn Error>> {
    Ok(ReadOptions::new(
        NonZeroU32::new(maximum).ok_or("zero read limit")?,
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
struct FailWhenArmed {
    stage: SqlMutationStage,
    armed: AtomicBool,
}

impl FailWhenArmed {
    const fn new(stage: SqlMutationStage) -> Self {
        Self {
            stage,
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

impl SqlMutationFaultInjector for FailWhenArmed {
    fn check(&self, stage: SqlMutationStage) -> Result<(), StorageError> {
        if stage == self.stage && self.armed.swap(false, Ordering::AcqRel) {
            Err(StorageError::new(
                StorageErrorKind::Internal,
                "injected_portable_sql_mutation_fault",
            ))
        } else {
            Ok(())
        }
    }
}
