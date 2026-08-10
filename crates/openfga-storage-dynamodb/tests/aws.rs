//! Explicit opt-in authoritative AWS `DynamoDB` contract.

#![allow(clippy::large_futures, reason = "AWS SDK operation futures are large")]

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::Write,
    num::NonZeroU32,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::Region;
use aws_smithy_http_client::{
    Builder as HttpClientBuilder,
    tls::{self, rustls_provider::CryptoMode},
};
use openfga_domain::{
    AuthorizationModelId, ConditionContext, ConsistencyPreference, ContextualTuples, Deadline,
    InputLimits, RelationshipTuple, RequestTimeout, StoreId, TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ChangeFilter, ChangeReader, ConditionFilter,
    ModelReader, ModelWriter, ObjectRelationFilter, OperationContext, PageOptions, ReadOptions,
    ReverseTupleFilter, StorageCancellationToken, StorageErrorKind, StoreFilter, StoreName,
    StoreReader, StoreWriter, StoredAuthorizationModel, TupleReadFilter, TupleReader,
    TupleWriteOptions, TupleWriter, UsersetRestrictionFilter, UsersetTupleFilter,
    contract::{TupleContractFixture, verify_tuple_contract},
};
use openfga_storage_dynamodb::{
    DynamoDbProvisioningConfig, DynamoDbProvisioningStatus, DynamoDbStorage, DynamoDbStorageConfig,
    DynamoDbTableName, RegionName,
};
use ulid::Ulid;

#[tokio::test]
#[ignore = "requires explicit real-AWS opt-in, workload identity, Region, and isolated table"]
async fn test_should_satisfy_authoritative_aws_contract() -> Result<(), Box<dyn Error>> {
    if std::env::var("OPENFGA_DYNAMODB_AWS_TEST")?.as_str() != "1" {
        return Err("real AWS test was not explicitly enabled".into());
    }
    let table_prefix = std::env::var("OPENFGA_DYNAMODB_TEST_TABLE")?;
    if !table_prefix.starts_with("openfga-aws-test-") {
        return Err("real AWS test table prefix must start with openfga-aws-test-".into());
    }
    let table = unique_table_name(&table_prefix)?;
    let region = std::env::var("AWS_REGION")?;
    let config = DynamoDbStorageConfig::builder()
        .table_name(DynamoDbTableName::from_str(&table)?)
        .region(RegionName::from_str(&region)?)
        .provisioning(
            DynamoDbProvisioningConfig::builder()
                .deletion_protection(false)
                .tags(BTreeMap::from([
                    ("application".to_owned(), "openfga".to_owned()),
                    ("managed-by".to_owned(), "openfga-rs-test".to_owned()),
                    ("run-id".to_owned(), table.clone()),
                ]))
                .build(),
        )
        .build();
    let context = operation_context()?;
    if DynamoDbStorage::provisioning_status(&config, &context).await?
        != DynamoDbProvisioningStatus::Missing
    {
        return Err("generated real AWS test table unexpectedly already exists".into());
    }
    let provision = DynamoDbStorage::provision(&config, &context).await;
    let result = match provision {
        Ok(_) => run_contract(config, &context).await,
        Err(error) => Err(Box::<dyn Error>::from(error)),
    };
    let cleanup = delete_table(&region, &table).await;
    result?;
    cleanup?;
    Ok(())
}

async fn run_contract(
    config: DynamoDbStorageConfig,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let (storage, mut runtime) = DynamoDbStorage::connect(config, context).await?;
    let store_id = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 801));
    storage
        .create_store(
            context,
            store_id,
            StoreName::new("AWS Contract".to_owned())?,
        )
        .await?;
    let fixture = TupleContractFixture::new(
        store_id,
        tuple("document:contract#viewer@user:anne")?,
        tuple("document:contract#viewer@user:bob")?,
        ObjectRelationFilter::new(
            "document:contract".parse()?,
            "viewer".parse()?,
            Vec::new(),
            ConditionFilter::any(),
            &InputLimits::default(),
        )?,
        ReadOptions::new(
            NonZeroU32::new(2).ok_or("nonzero")?,
            &InputLimits::default(),
        )?,
    );
    verify_tuple_contract(&storage, context, &fixture).await?;
    verify_all_capabilities(&storage, context, store_id).await?;
    runtime.stop().await?;
    Ok(())
}

async fn verify_all_capabilities(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    store_id: StoreId,
) -> Result<(), Box<dyn Error>> {
    let model_id = AuthorizationModelId::from_ulid(Ulid::from_parts(1_700_000_000_100, 802));
    verify_model_and_assertions(storage, context, store_id, model_id).await?;
    verify_snapshot_capabilities(storage, context).await?;
    verify_large_mutation_and_changes(storage, context).await?;
    verify_store_lifecycle(storage, context, store_id, model_id).await
}

async fn verify_snapshot_capabilities(
    storage: &DynamoDbStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 892));
    let direct = tuple("document:aws-capabilities#viewer@user:anne")?;
    let userset = tuple("document:aws-capabilities#viewer@group:engineering#member")?;
    storage
        .write_tuples(
            context,
            store_id,
            Vec::new(),
            vec![direct.clone(), userset.clone()],
            TupleWriteOptions::default(),
        )
        .await?;
    let forward = ObjectRelationFilter::new(
        "document:aws-capabilities".parse()?,
        "viewer".parse()?,
        Vec::new(),
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    assert_eq!(
        storage
            .count_object_relation(context, store_id, &forward)
            .await?,
        2
    );
    let exact_subject = ObjectRelationFilter::new(
        "document:aws-capabilities".parse()?,
        "viewer".parse()?,
        vec![direct.key().subject().clone()],
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    assert_eq!(
        storage
            .read_object_relation(context, store_id, &exact_subject, read_options(10)?)
            .await?
            .count(),
        1
    );
    assert_eq!(
        storage
            .read_object_relation(context, store_id, &forward, read_options(10)?)
            .await?
            .count(),
        2
    );
    let userset_filter = UsersetTupleFilter::new(
        "document:aws-capabilities".parse()?,
        "viewer".parse()?,
        vec![UsersetRestrictionFilter::new(
            "group".parse()?,
            "member".parse()?,
        )],
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    let mut usersets = storage
        .read_userset_tuples(context, store_id, &userset_filter, read_options(10)?)
        .await?;
    assert_eq!(
        usersets
            .next_item()
            .transpose()?
            .ok_or("missing userset")?
            .key(),
        userset.key()
    );
    let reverse_filter = ReverseTupleFilter::new(
        "document".parse()?,
        "viewer".parse()?,
        vec![direct.key().subject().clone()],
        vec!["aws-capabilities".parse()?],
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    let mut reverse = storage
        .read_reverse_tuples(context, store_id, &reverse_filter, read_options(10)?)
        .await?;
    assert_eq!(
        reverse
            .next_item()
            .transpose()?
            .ok_or("missing reverse tuple")?
            .key(),
        direct.key()
    );
    Ok(())
}

async fn verify_model_and_assertions(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    store_id: StoreId,
    model_id: AuthorizationModelId,
) -> Result<(), Box<dyn Error>> {
    storage
        .write_model(context, stored_model(store_id, model_id)?)
        .await?;
    assert_eq!(
        storage
            .read_latest_model(context, store_id)
            .await?
            .model_id(),
        &model_id
    );
    assert_eq!(
        storage
            .list_models(context, store_id, &page_options(1, None)?)
            .await?
            .items()
            .len(),
        1
    );

    let assertion = Assertion::new(
        "document:contract#viewer@user:anne".parse()?,
        true,
        ContextualTuples::new(Vec::new(), &InputLimits::default())?,
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

async fn verify_large_mutation_and_changes(
    storage: &DynamoDbStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let mutation_store = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 849));
    let writes = (0..49)
        .map(|index| tuple(&format!("document:aws-{index}#viewer@user:anne")))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        storage
            .write_tuples(
                context,
                mutation_store,
                Vec::new(),
                writes,
                TupleWriteOptions::default(),
            )
            .await?
            .change_ids()
            .len(),
        49
    );
    let mut cursor = None;
    let mut observed = Vec::new();
    loop {
        let page = storage
            .read_tuples(
                context,
                mutation_store,
                &TupleReadFilter::all(),
                &page_options(7, cursor)?,
            )
            .await?;
        cursor = page.continuation().cloned();
        observed.extend(page.into_items());
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        observed
            .iter()
            .map(|tuple| tuple.tuple().key().to_string())
            .collect::<BTreeSet<_>>()
            .len(),
        49
    );
    assert_eq!(
        storage
            .read_changes(
                context,
                mutation_store,
                &ChangeFilter::default(),
                &page_options(49, None)?,
            )
            .await?
            .items()
            .len(),
        49
    );

    Ok(())
}

async fn verify_store_lifecycle(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    store_id: StoreId,
    model_id: AuthorizationModelId,
) -> Result<(), Box<dyn Error>> {
    storage
        .rename_store(
            context,
            store_id,
            StoreName::new("AWS Contract Renamed".to_owned())?,
        )
        .await?;
    assert!(
        storage
            .list_stores(context, &StoreFilter::all(), &page_options(10, None)?)
            .await?
            .items()
            .iter()
            .any(|store| store.id() == store_id)
    );
    storage.delete_store(context, store_id).await?;
    storage.delete_store(context, store_id).await?;
    let Err(error) = storage.read_store(context, store_id).await else {
        return Err("deleted store directory record must be absent".into());
    };
    assert_eq!(error.kind(), StorageErrorKind::NotFound);
    assert_eq!(
        storage
            .read_model(context, store_id, model_id)
            .await?
            .model_id(),
        &model_id,
        "directory deletion must preserve namespace data"
    );
    Ok(())
}

fn page_options(
    size: u32,
    continuation: Option<openfga_storage::StorageCursor>,
) -> Result<PageOptions, Box<dyn Error>> {
    Ok(PageOptions::new(
        NonZeroU32::new(size).ok_or("page size must be nonzero")?,
        continuation,
        &InputLimits::default(),
    )?)
}

fn read_options(maximum: u32) -> Result<ReadOptions, Box<dyn Error>> {
    Ok(ReadOptions::new(
        NonZeroU32::new(maximum).ok_or("read limit must be nonzero")?,
        &InputLimits::default(),
    )?)
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

fn unique_table_name(prefix: &str) -> Result<String, Box<dyn Error>> {
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random)?;
    let suffix = random.iter().fold(
        String::with_capacity(random.len().saturating_mul(2)),
        |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        },
    );
    let name = format!("{prefix}-{suffix}");
    let _ = DynamoDbTableName::from_str(&name)?;
    Ok(name)
}

async fn delete_table(region: &str, table: &str) -> Result<(), Box<dyn Error>> {
    let http_client = HttpClientBuilder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
        .build_https();
    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .http_client(http_client)
        .load()
        .await;
    aws_sdk_dynamodb::Client::new(&shared)
        .delete_table()
        .table_name(table)
        .send()
        .await?;
    Ok(())
}

fn operation_context() -> Result<OperationContext, Box<dyn Error>> {
    let timeout = RequestTimeout::new(Duration::from_mins(5))?;
    let deadline = Deadline::from_timeout(Instant::now(), timeout)?;
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        deadline,
        StorageCancellationToken::new(),
    ))
}

fn tuple(value: &str) -> Result<RelationshipTuple, Box<dyn Error>> {
    Ok(RelationshipTuple::unconditional(value.parse::<TupleKey>()?))
}
