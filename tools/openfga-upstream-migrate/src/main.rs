//! Bounded offline import from the pinned `OpenFGA` `SQLite` schema.

#![forbid(unsafe_code)]

use std::{
    collections::BTreeSet,
    io::Write,
    num::NonZeroU32,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use openfga_domain::{
    AuthorizationModelId, ConsistencyPreference, Deadline, InputLimits, RelationshipTuple,
    RequestTimeout, StoreId,
};
use openfga_model::ModelCompiler;
use openfga_proto::openfga::v1 as pb;
use openfga_storage::{
    AssertionWriter, ModelWriter, OperationContext, StorageCancellationToken, StoreName,
    StoreWriter, StoredAuthorizationModel, TupleWriteOptions, TupleWriter,
};
use openfga_storage_sql::{PortableSqlDialect, PortableSqlStorage, PortableSqlStorageConfig};
use openfga_transport::{
    assertion_from_wire, model_definition_from_wire, relationship_tuple_from_wire,
};
use prost::Message;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use sqlx::{
    FromRow, Row, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

const UPSTREAM_COMMIT: &str = "4e4f79ed841513dfd61746a75ef473f6198299f7";
const UPSTREAM_SQLITE_SCHEMA_VERSION: i64 = 6;
const MAXIMUM_URL_BYTES: usize = 4_096;
const MAXIMUM_MODEL_BYTES: usize = 16 * 1_024 * 1_024;
const MAXIMUM_ASSERTION_BYTES: usize = 8 * 1_024 * 1_024;
const MAXIMUM_CONTEXT_BYTES: usize = 2 * 1_024 * 1_024;
const MAXIMUM_BATCH_SIZE: u32 = 1_000;
const MAXIMUM_STORES: usize = 100_000;
const MAXIMUM_MODELS_PER_STORE: usize = 10_000;

#[derive(Debug, Parser)]
#[command(about = "Migrate a quiesced pinned OpenFGA SQLite datastore into openfga-rs")]
struct Cli {
    #[arg(long, default_value = "OPENFGA_UPSTREAM_SQLITE_URL")]
    source_url_env: String,
    #[arg(long, default_value = "OPENFGA_SQLITE_URL")]
    destination_url_env: String,
    #[arg(long, default_value_t = 100)]
    batch_size: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MigrationReport {
    upstream_commit: &'static str,
    stores: u64,
    models: u64,
    assertions: u64,
    tuples: u64,
    changelog_policy: &'static str,
}

#[derive(Debug, FromRow)]
struct UpstreamStoreRow {
    id: String,
    name: String,
}

#[derive(Debug, FromRow)]
struct UpstreamModelRow {
    authorization_model_id: String,
    schema_version: String,
    serialized_protobuf: Vec<u8>,
}

#[derive(Debug, FromRow)]
struct UpstreamAssertionRow {
    authorization_model_id: String,
    assertions: Vec<u8>,
}

#[derive(Debug, FromRow)]
struct UpstreamTupleRow {
    object_type: String,
    object_id: String,
    relation: String,
    user_object_type: String,
    user_object_id: String,
    user_relation: String,
    user_type: String,
    condition_name: Option<String>,
    condition_context: Option<Vec<u8>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    validate_environment_name(&cli.source_url_env)?;
    validate_environment_name(&cli.destination_url_env)?;
    if !(1..=MAXIMUM_BATCH_SIZE).contains(&cli.batch_size) {
        bail!("batch size must be between 1 and {MAXIMUM_BATCH_SIZE}");
    }
    let source_url = load_url(&cli.source_url_env)?;
    let destination_url = load_url(&cli.destination_url_env)?;
    if source_url.expose_secret() == destination_url.expose_secret() {
        bail!("source and destination SQLite URLs must differ");
    }

    let source_options = SqliteConnectOptions::from_str(source_url.expose_secret())?
        .create_if_missing(false)
        .foreign_keys(true);
    let source_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(source_options)
        .await
        .context("failed to connect upstream SQLite datastore")?;
    let mut source = source_pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *source)
        .await
        .context("failed to acquire the upstream SQLite write-blocking snapshot")?;
    verify_upstream_schema(&mut source).await?;

    let destination = PortableSqlStorage::connect(
        PortableSqlStorageConfig::builder()
            .dialect(PortableSqlDialect::Sqlite)
            .primary_url(destination_url)
            .max_connections(NonZeroU32::MIN)
            .migrate_on_connect(true)
            .build(),
    )
    .await
    .context("failed to connect destination SQLite datastore")?;
    require_empty_destination(&destination).await?;
    let limits = InputLimits::default();
    let report = migrate_snapshot(
        &mut source,
        &destination,
        &limits,
        usize::try_from(cli.batch_size)?,
    )
    .await?;
    sqlx::query("ROLLBACK").execute(&mut *source).await?;
    drop(source);
    destination.close().await;
    source_pool.close().await;
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, &report)?;
    writeln!(output)?;
    Ok(())
}

async fn migrate_snapshot(
    source: &mut SqliteConnection,
    destination: &PortableSqlStorage,
    limits: &InputLimits,
    batch_size: usize,
) -> Result<MigrationReport> {
    let stores = migrate_stores(source, destination).await?;
    let namespaces = upstream_namespaces(source).await?;
    let mut models = 0_u64;
    let mut assertions = 0_u64;
    let mut tuples = 0_u64;
    for store_id in namespaces {
        models =
            models.saturating_add(migrate_models(source, destination, limits, store_id).await?);
        assertions = assertions
            .saturating_add(migrate_assertions(source, destination, limits, store_id).await?);
        tuples = tuples.saturating_add(
            migrate_tuples(source, destination, limits, store_id, batch_size).await?,
        );
    }
    Ok(MigrationReport {
        upstream_commit: UPSTREAM_COMMIT,
        stores,
        models,
        assertions,
        tuples,
        changelog_policy: "reset-as-cutover-writes",
    })
}

async fn migrate_stores(
    source: &mut SqliteConnection,
    destination: &PortableSqlStorage,
) -> Result<u64> {
    let rows = sqlx::query_as::<_, UpstreamStoreRow>(
        "SELECT id, name FROM store WHERE deleted_at IS NULL ORDER BY id LIMIT ?",
    )
    .bind(i64::try_from(MAXIMUM_STORES.saturating_add(1))?)
    .fetch_all(&mut *source)
    .await?;
    if rows.len() > MAXIMUM_STORES {
        bail!("upstream active store count exceeds the migration limit");
    }
    let count = u64::try_from(rows.len())?;
    for row in rows {
        let store_id = row.id.parse::<StoreId>()?;
        destination
            .create_store(&operation_context()?, store_id, StoreName::new(row.name)?)
            .await?;
    }
    Ok(count)
}

async fn upstream_namespaces(source: &mut SqliteConnection) -> Result<Vec<StoreId>> {
    let values = sqlx::query_scalar::<_, String>(
        "SELECT namespace_id FROM (SELECT id AS namespace_id FROM store WHERE deleted_at IS NULL \
         UNION SELECT store AS namespace_id FROM authorization_model UNION SELECT store AS \
         namespace_id FROM assertion UNION SELECT store AS namespace_id FROM tuple) AS namespaces \
         ORDER BY namespace_id LIMIT ?",
    )
    .bind(i64::try_from(MAXIMUM_STORES.saturating_add(1))?)
    .fetch_all(&mut *source)
    .await?;
    if values.len() > MAXIMUM_STORES {
        bail!("upstream namespace count exceeds the migration limit");
    }
    values
        .into_iter()
        .map(|value| value.parse::<StoreId>().map_err(Into::into))
        .collect()
}

async fn migrate_models(
    source: &mut SqliteConnection,
    destination: &PortableSqlStorage,
    limits: &InputLimits,
    store_id: StoreId,
) -> Result<u64> {
    let rows = sqlx::query_as::<_, UpstreamModelRow>(
        "SELECT authorization_model_id, schema_version, serialized_protobuf FROM \
         authorization_model WHERE store = ? ORDER BY authorization_model_id LIMIT ?",
    )
    .bind(store_id.to_string())
    .bind(i64::try_from(MAXIMUM_MODELS_PER_STORE.saturating_add(1))?)
    .fetch_all(&mut *source)
    .await?;
    if rows.len() > MAXIMUM_MODELS_PER_STORE {
        bail!("upstream model count exceeds the bounded per-store limit");
    }
    for row in &rows {
        if row.serialized_protobuf.len() > MAXIMUM_MODEL_BYTES {
            bail!("upstream authorization model exceeds the byte limit");
        }
        let model_id = row.authorization_model_id.parse::<AuthorizationModelId>()?;
        let mut wire = pb::AuthorizationModel::decode(row.serialized_protobuf.as_slice())?;
        wire.id.clone_from(&row.authorization_model_id);
        if wire.schema_version != row.schema_version {
            bail!("upstream authorization model schema metadata mismatch");
        }
        let source_model =
            Arc::new(model_definition_from_wire(&wire, limits)?.with_identity(store_id, model_id));
        let compiled = ModelCompiler::default().compile(&source_model)?;
        let written_at = UNIX_EPOCH
            .checked_add(Duration::from_millis(model_id.as_ulid().timestamp_ms()))
            .context("authorization model timestamp overflow")?;
        destination
            .write_model(
                &operation_context()?,
                Arc::new(StoredAuthorizationModel::new(
                    source_model,
                    compiled,
                    written_at,
                )?),
            )
            .await?;
    }
    Ok(u64::try_from(rows.len())?)
}

async fn migrate_assertions(
    source: &mut SqliteConnection,
    destination: &PortableSqlStorage,
    limits: &InputLimits,
    store_id: StoreId,
) -> Result<u64> {
    let rows = sqlx::query_as::<_, UpstreamAssertionRow>(
        "SELECT authorization_model_id, assertions FROM assertion WHERE store = ? ORDER BY \
         authorization_model_id LIMIT ?",
    )
    .bind(store_id.to_string())
    .bind(i64::try_from(MAXIMUM_MODELS_PER_STORE.saturating_add(1))?)
    .fetch_all(&mut *source)
    .await?;
    if rows.len() > MAXIMUM_MODELS_PER_STORE {
        bail!("upstream assertion model count exceeds the bounded per-store limit");
    }
    let mut total = 0_u64;
    for row in rows {
        if row.assertions.len() > MAXIMUM_ASSERTION_BYTES {
            bail!("upstream assertions payload exceeds the byte limit");
        }
        let wire = pb::Assertions::decode(row.assertions.as_slice())?;
        let assertions = wire
            .assertions
            .into_iter()
            .map(|assertion| assertion_from_wire(assertion, limits, row.assertions.len()))
            .collect::<Result<Vec<_>, _>>()?;
        total = total.saturating_add(u64::try_from(assertions.len())?);
        destination
            .write_assertions(
                &operation_context()?,
                store_id,
                row.authorization_model_id.parse::<AuthorizationModelId>()?,
                assertions,
            )
            .await?;
    }
    Ok(total)
}

async fn migrate_tuples(
    source: &mut SqliteConnection,
    destination: &PortableSqlStorage,
    limits: &InputLimits,
    store_id: StoreId,
    batch_size: usize,
) -> Result<u64> {
    let mut offset = 0_i64;
    let mut total = 0_u64;
    loop {
        let rows = sqlx::query_as::<_, UpstreamTupleRow>(
            "SELECT object_type, object_id, relation, user_object_type, user_object_id, \
             user_relation, user_type, condition_name, condition_context FROM tuple WHERE store = \
             ? ORDER BY object_type, object_id, relation, user_object_type, user_object_id, \
             user_relation LIMIT ? OFFSET ?",
        )
        .bind(store_id.to_string())
        .bind(i64::try_from(batch_size)?)
        .bind(offset)
        .fetch_all(&mut *source)
        .await?;
        if rows.is_empty() {
            break;
        }
        let row_count = rows.len();
        let tuples = rows
            .into_iter()
            .map(|row| upstream_tuple(row, limits))
            .collect::<Result<Vec<_>>>()?;
        destination
            .write_tuples(
                &operation_context()?,
                store_id,
                Vec::new(),
                tuples,
                TupleWriteOptions::default(),
            )
            .await?;
        let increment = i64::try_from(row_count)?;
        offset = offset
            .checked_add(increment)
            .context("tuple offset overflow")?;
        total = total.saturating_add(u64::try_from(row_count)?);
        if row_count < batch_size {
            break;
        }
    }
    Ok(total)
}

fn upstream_tuple(row: UpstreamTupleRow, limits: &InputLimits) -> Result<RelationshipTuple> {
    let object = format!("{}:{}", row.object_type, row.object_id);
    let user = match row.user_type.as_str() {
        "user" if row.user_relation.is_empty() => {
            format!("{}:{}", row.user_object_type, row.user_object_id)
        }
        "userset" if row.user_relation.is_empty() && row.user_object_id == "*" => {
            format!("{}:*", row.user_object_type)
        }
        "userset" if !row.user_relation.is_empty() => format!(
            "{}:{}#{}",
            row.user_object_type, row.user_object_id, row.user_relation,
        ),
        _ => bail!("upstream tuple contains an invalid user shape"),
    };
    let condition = match (row.condition_name, row.condition_context) {
        (None, None) => None,
        (Some(name), Some(payload)) => {
            if payload.len() > MAXIMUM_CONTEXT_BYTES {
                bail!("upstream condition context exceeds the byte limit");
            }
            Some(pb::RelationshipCondition {
                name,
                context: Some(pbjson_types::Struct::decode(payload.as_slice())?),
            })
        }
        _ => bail!("upstream tuple has inconsistent condition columns"),
    };
    Ok(relationship_tuple_from_wire(
        pb::TupleKey {
            user,
            relation: row.relation,
            object,
            condition,
        },
        limits,
        MAXIMUM_CONTEXT_BYTES,
    )?)
}

async fn verify_upstream_schema(source: &mut SqliteConnection) -> Result<()> {
    let required = [
        ("tuple", "user_object_type"),
        ("tuple", "condition_context"),
        ("authorization_model", "serialized_protobuf"),
        ("assertion", "assertions"),
        ("store", "deleted_at"),
        ("changelog", "ulid"),
        ("goose_db_version", "version_id"),
    ];
    for (table, column) in required {
        let statement = match table {
            "tuple" => "PRAGMA table_info(tuple)",
            "authorization_model" => "PRAGMA table_info(authorization_model)",
            "assertion" => "PRAGMA table_info(assertion)",
            "store" => "PRAGMA table_info(store)",
            "changelog" => "PRAGMA table_info(changelog)",
            "goose_db_version" => "PRAGMA table_info(goose_db_version)",
            _ => bail!("unsupported upstream schema table"),
        };
        let columns = sqlx::query(statement)
            .fetch_all(&mut *source)
            .await?
            .into_iter()
            .map(|row| row.try_get::<String, _>("name"))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if !columns.contains(column) {
            bail!("source does not match the pinned OpenFGA SQLite schema");
        }
    }
    let version = sqlx::query_scalar::<_, i64>(
        "SELECT version_id FROM goose_db_version WHERE is_applied = 1 ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&mut *source)
    .await?;
    if version != UPSTREAM_SQLITE_SCHEMA_VERSION {
        bail!("source does not match the pinned OpenFGA SQLite migration version");
    }
    let has_store_ulid_index = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_store_ulid'",
    )
    .fetch_one(&mut *source)
    .await?;
    if has_store_ulid_index != 1 {
        bail!("source does not contain the pinned OpenFGA SQLite index set");
    }
    Ok(())
}

async fn require_empty_destination(destination: &PortableSqlStorage) -> Result<()> {
    let populated = sqlx::query_scalar::<_, i64>(
        "SELECT (SELECT COUNT(*) FROM stores) + (SELECT COUNT(*) FROM authorization_models) + \
         (SELECT COUNT(*) FROM tuples) + (SELECT COUNT(*) FROM tuple_changes)",
    )
    .fetch_one(destination.primary_pool())
    .await?;
    if populated != 0 {
        bail!("destination datastore must be empty");
    }
    Ok(())
}

fn operation_context() -> Result<OperationContext> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(Instant::now(), RequestTimeout::new(Duration::from_mins(5))?)?,
        StorageCancellationToken::new(),
    ))
}

fn load_url(name: &str) -> Result<SecretString> {
    let value =
        std::env::var(name).with_context(|| format!("environment variable {name} is not set"))?;
    if value.is_empty() || value.len() > MAXIMUM_URL_BYTES || !value.starts_with("sqlite:") {
        bail!("environment variable {name} must contain a bounded SQLite URL");
    }
    Ok(SecretString::from(value))
}

fn validate_environment_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        bail!("environment variable name is invalid");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use openfga_storage::{
        Assertion as StoredAssertion, AssertionReader, ChangeFilter, ChangeReader, ModelReader,
        PageOptions, StoreReader, TupleReader,
    };

    use super::*;

    #[tokio::test]
    async fn test_should_migrate_pinned_upstream_sqlite_snapshot() -> Result<()> {
        let source_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        create_upstream_fixture(&source_pool).await?;
        let mut source = source_pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *source).await?;
        verify_upstream_schema(&mut source).await?;

        let destination = PortableSqlStorage::connect(
            PortableSqlStorageConfig::builder()
                .dialect(PortableSqlDialect::Sqlite)
                .primary_url(SecretString::from("sqlite::memory:".to_owned()))
                .max_connections(NonZeroU32::MIN)
                .build(),
        )
        .await?;
        let context = operation_context()?;
        let report =
            migrate_snapshot(&mut source, &destination, &InputLimits::default(), 1).await?;
        verify_migrated_snapshot(&destination, &context, &report).await?;
        assert!(require_empty_destination(&destination).await.is_err());
        sqlx::query("UPDATE goose_db_version SET version_id = ? WHERE is_applied = 1")
            .bind(UPSTREAM_SQLITE_SCHEMA_VERSION.saturating_add(1))
            .execute(&mut *source)
            .await?;
        assert!(verify_upstream_schema(&mut source).await.is_err());
        sqlx::query("UPDATE goose_db_version SET version_id = ? WHERE is_applied = 1")
            .bind(UPSTREAM_SQLITE_SCHEMA_VERSION)
            .execute(&mut *source)
            .await?;
        sqlx::query("ROLLBACK").execute(&mut *source).await?;
        drop(source);
        destination.close().await;
        source_pool.close().await;
        Ok(())
    }

    async fn verify_migrated_snapshot(
        destination: &PortableSqlStorage,
        context: &OperationContext,
        report: &MigrationReport,
    ) -> Result<()> {
        assert_eq!(report.stores, 1);
        assert_eq!(report.models, 3);
        assert_eq!(report.assertions, 1);
        assert_eq!(report.tuples, 6);

        let store_id = fixture_store_id()?;
        let model_id = fixture_model_id()?;
        assert_eq!(
            destination
                .read_store(context, store_id)
                .await?
                .name()
                .as_str(),
            "Migrated Store",
        );
        assert_eq!(
            destination
                .read_model(context, store_id, model_id)
                .await?
                .model_id(),
            &model_id,
        );
        let assertions = destination
            .read_assertions(context, store_id, model_id)
            .await?;
        assert_eq!(assertions.len(), 1);
        assert!(assertions.first().is_some_and(StoredAssertion::expectation));
        let tuple = "document:roadmap#viewer@user:anne".parse()?;
        assert!(destination.tuple_exists(context, store_id, &tuple).await?);
        for tuple in [
            "document:wildcard#viewer@user:*",
            "document:userset#viewer@group:eng#member",
            "document:conditioned#viewer@user:beth",
        ] {
            assert!(
                destination
                    .tuple_exists(context, store_id, &tuple.parse()?)
                    .await?,
            );
        }
        for namespace in [fixture_deleted_store_id()?, fixture_orphan_store_id()?] {
            assert!(destination.read_store(context, namespace).await.is_err());
            assert_eq!(
                destination
                    .read_model(context, namespace, model_id)
                    .await?
                    .model_id(),
                &model_id,
            );
            let tuple = format!("document:{namespace}#viewer@user:anne").parse()?;
            assert!(destination.tuple_exists(context, namespace, &tuple).await?);
        }
        assert_eq!(
            destination
                .read_changes(
                    context,
                    store_id,
                    &ChangeFilter::default(),
                    &PageOptions::from_read_options(openfga_storage::ReadOptions::new(
                        NonZeroU32::new(10).context("nonzero read limit")?,
                        &InputLimits::default(),
                    )?),
                )
                .await?
                .items()
                .len(),
            4,
        );
        Ok(())
    }

    async fn create_upstream_fixture(pool: &sqlx::SqlitePool) -> Result<()> {
        create_upstream_schema(pool).await?;
        insert_upstream_fixture(pool).await
    }

    async fn create_upstream_schema(pool: &sqlx::SqlitePool) -> Result<()> {
        for statement in [
            "CREATE TABLE store (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT \
             NULL, updated_at TEXT, deleted_at TEXT)",
            "CREATE TABLE authorization_model (store TEXT NOT NULL, authorization_model_id TEXT \
             NOT NULL, schema_version TEXT NOT NULL, serialized_protobuf BLOB NOT NULL, PRIMARY \
             KEY (store, authorization_model_id))",
            "CREATE TABLE assertion (store TEXT NOT NULL, authorization_model_id TEXT NOT NULL, \
             assertions BLOB NOT NULL, PRIMARY KEY (store, authorization_model_id))",
            "CREATE TABLE tuple (store TEXT NOT NULL, object_type TEXT NOT NULL, object_id TEXT \
             NOT NULL, relation TEXT NOT NULL, user_object_type TEXT NOT NULL, user_object_id \
             TEXT NOT NULL, user_relation TEXT NOT NULL, user_type TEXT NOT NULL, ulid TEXT NOT \
             NULL, inserted_at TEXT NOT NULL, condition_name TEXT, condition_context BLOB, \
             PRIMARY KEY (store, object_type, object_id, relation, user_object_type, \
             user_object_id, user_relation))",
            "CREATE TABLE changelog (store TEXT NOT NULL, object_type TEXT NOT NULL, object_id \
             TEXT NOT NULL, relation TEXT NOT NULL, user_object_type TEXT NOT NULL, \
             user_object_id TEXT NOT NULL, user_relation TEXT NOT NULL, operation INTEGER NOT \
             NULL, ulid TEXT NOT NULL, inserted_at TEXT NOT NULL, condition_name TEXT, \
             condition_context BLOB, PRIMARY KEY (store, ulid, object_type))",
            "CREATE TABLE goose_db_version (id INTEGER PRIMARY KEY AUTOINCREMENT, version_id \
             INTEGER NOT NULL, is_applied INTEGER NOT NULL, tstamp TEXT DEFAULT CURRENT_TIMESTAMP)",
            "CREATE INDEX idx_store_ulid ON tuple (store, ulid)",
        ] {
            sqlx::query(statement).execute(pool).await?;
        }
        sqlx::query("INSERT INTO goose_db_version (version_id, is_applied) VALUES (?, 1)")
            .bind(UPSTREAM_SQLITE_SCHEMA_VERSION)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn insert_upstream_fixture(pool: &sqlx::SqlitePool) -> Result<()> {
        let store_id = fixture_store_id()?.to_string();
        let deleted_store_id = fixture_deleted_store_id()?.to_string();
        let orphan_store_id = fixture_orphan_store_id()?.to_string();
        insert_fixture_stores(pool, &store_id, &deleted_store_id).await?;
        let model = fixture_model(fixture_model_id()?.to_string());
        insert_fixture_models(
            pool,
            [&store_id, &deleted_store_id, &orphan_store_id],
            &model,
        )
        .await?;
        insert_fixture_assertion(pool, &store_id, &model.id).await?;
        insert_fixture_tuples(pool, &store_id, &deleted_store_id, &orphan_store_id).await
    }

    async fn insert_fixture_stores(
        pool: &sqlx::SqlitePool,
        store_id: &str,
        deleted_store_id: &str,
    ) -> Result<()> {
        sqlx::query("INSERT INTO store (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)")
            .bind(store_id)
            .bind("Migrated Store")
            .bind("2026-08-08T00:00:00Z")
            .bind("2026-08-08T00:00:00Z")
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO store (id, name, created_at, updated_at, deleted_at) VALUES (?, ?, ?, ?, \
             ?)",
        )
        .bind(deleted_store_id)
        .bind("Deleted Store")
        .bind("2026-08-08T00:00:00Z")
        .bind("2026-08-08T00:00:00Z")
        .bind("2026-08-08T01:00:00Z")
        .execute(pool)
        .await?;
        Ok(())
    }

    fn fixture_model(model_id: String) -> pb::AuthorizationModel {
        pb::AuthorizationModel {
            id: model_id,
            schema_version: "1.1".to_owned(),
            type_definitions: vec![
                pb::TypeDefinition {
                    r#type: "user".to_owned(),
                    relations: std::collections::HashMap::new(),
                    metadata: None,
                },
                pb::TypeDefinition {
                    r#type: "document".to_owned(),
                    relations: std::collections::HashMap::from([(
                        "viewer".to_owned(),
                        pb::Userset {
                            userset: Some(pb::userset::Userset::This(pb::DirectUserset {})),
                        },
                    )]),
                    metadata: Some(pb::Metadata {
                        relations: std::collections::HashMap::from([(
                            "viewer".to_owned(),
                            pb::RelationMetadata {
                                directly_related_user_types: vec![pb::RelationReference {
                                    r#type: "user".to_owned(),
                                    condition: String::new(),
                                    relation_or_wildcard: None,
                                }],
                                module: String::new(),
                                source_info: None,
                            },
                        )]),
                        module: String::new(),
                        source_info: None,
                    }),
                },
            ],
            conditions: std::collections::HashMap::new(),
        }
    }

    async fn insert_fixture_models(
        pool: &sqlx::SqlitePool,
        namespaces: [&str; 3],
        model: &pb::AuthorizationModel,
    ) -> Result<()> {
        for namespace in namespaces {
            sqlx::query(
                "INSERT INTO authorization_model (store, authorization_model_id, schema_version, \
                 serialized_protobuf) VALUES (?, ?, ?, ?)",
            )
            .bind(namespace)
            .bind(&model.id)
            .bind(&model.schema_version)
            .bind(model.encode_to_vec())
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    async fn insert_fixture_assertion(
        pool: &sqlx::SqlitePool,
        store_id: &str,
        model_id: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO assertion (store, authorization_model_id, assertions) VALUES (?, ?, ?)",
        )
        .bind(store_id)
        .bind(model_id)
        .bind(
            pb::Assertions {
                assertions: vec![pb::Assertion {
                    tuple_key: Some(pb::AssertionTupleKey {
                        object: "document:roadmap".to_owned(),
                        relation: "viewer".to_owned(),
                        user: "user:anne".to_owned(),
                    }),
                    expectation: true,
                    contextual_tuples: Vec::new(),
                    context: None,
                }],
            }
            .encode_to_vec(),
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_fixture_tuples(
        pool: &sqlx::SqlitePool,
        store_id: &str,
        deleted_store_id: &str,
        orphan_store_id: &str,
    ) -> Result<()> {
        let empty_context = pbjson_types::Struct::default().encode_to_vec();
        for (
            namespace,
            object_id,
            user_object_type,
            user_object_id,
            user_relation,
            user_type,
            condition_name,
            condition_context,
        ) in [
            (store_id, "roadmap", "user", "anne", "", "user", None, None),
            (store_id, "wildcard", "user", "*", "", "userset", None, None),
            (
                store_id, "userset", "group", "eng", "member", "userset", None, None,
            ),
            (
                store_id,
                "conditioned",
                "user",
                "beth",
                "",
                "user",
                Some("cond"),
                Some(empty_context.clone()),
            ),
            (
                deleted_store_id,
                deleted_store_id,
                "user",
                "anne",
                "",
                "user",
                None,
                None,
            ),
            (
                orphan_store_id,
                orphan_store_id,
                "user",
                "anne",
                "",
                "user",
                None,
                None,
            ),
        ] {
            sqlx::query(
                "INSERT INTO tuple (store, object_type, object_id, relation, user_object_type, \
                 user_object_id, user_relation, user_type, ulid, inserted_at, condition_name, \
                 condition_context) VALUES (?, 'document', ?, 'viewer', ?, ?, ?, ?, ?, \
                 '2026-08-08T00:00:00Z', ?, ?)",
            )
            .bind(namespace)
            .bind(object_id)
            .bind(user_object_type)
            .bind(user_object_id)
            .bind(user_relation)
            .bind(user_type)
            .bind(format!("01J{:023}", object_id.len()))
            .bind(condition_name)
            .bind(condition_context)
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    fn fixture_store_id() -> Result<StoreId> {
        Ok("01HF7YAT000000000000000001".parse()?)
    }

    fn fixture_model_id() -> Result<AuthorizationModelId> {
        Ok("01HF7YAT000000000000000002".parse()?)
    }

    fn fixture_deleted_store_id() -> Result<StoreId> {
        Ok("01HF7YAT000000000000000003".parse()?)
    }

    fn fixture_orphan_store_id() -> Result<StoreId> {
        Ok("01HF7YAT000000000000000004".parse()?)
    }
}
