//! Rustack-backed `DynamoDB` storage contracts through the official AWS SDK.

#![allow(clippy::large_futures, reason = "AWS SDK operation futures are large")]

use std::{
    collections::BTreeSet,
    error::Error,
    num::NonZeroU32,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{Client, config::Region, types::AttributeValue};
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
    DevelopmentEndpoint, DynamoDbGarbageCollectionConfig, DynamoDbStorage, DynamoDbStorageConfig,
    DynamoDbTableName, RegionName,
};
use sha2::{Digest, Sha256};
use ulid::Ulid;

const ENDPOINT_ENV: &str = "OPENFGA_DYNAMODB_TEST_ENDPOINT";
const TABLE_ENV: &str = "OPENFGA_DYNAMODB_TEST_TABLE";

#[tokio::test]
#[ignore = "requires a Makefile-managed Rustack process"]
async fn test_should_satisfy_rustack_storage_contract() -> Result<(), Box<dyn Error>> {
    let endpoint = std::env::var(ENDPOINT_ENV)?;
    let table = std::env::var(TABLE_ENV)?;
    let config = DynamoDbStorageConfig::builder()
        .table_name(DynamoDbTableName::from_str(&table)?)
        .region(RegionName::from_str("us-west-2")?)
        .endpoint(Some(DevelopmentEndpoint::from_str(&endpoint)?))
        .attempt_timeout(Duration::from_millis(50))
        .operation_timeout(Duration::from_millis(100))
        .maximum_caller_deadline(Duration::from_millis(100))
        .garbage_collection(
            DynamoDbGarbageCollectionConfig::builder()
                .interval(Duration::from_millis(20))
                .grace_period(Duration::from_millis(300))
                .assertion_retention(Duration::from_millis(300))
                .build(),
        )
        .build();
    let context = operation_context()?;
    DynamoDbStorage::provision(&config, &context).await?;
    let (storage, mut runtime) = DynamoDbStorage::connect(config, &context).await?;
    verify_namespace_without_directory_record(&storage, &context).await?;
    let store_id = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 1));
    storage
        .create_store(
            &context,
            store_id,
            StoreName::new("Rustack Contract".to_owned())?,
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
    verify_tuple_contract(&storage, &context, &fixture).await?;
    verify_snapshot_capabilities(&storage, &context).await?;
    storage
        .rename_store(
            &context,
            store_id,
            StoreName::new("Rustack Contract Renamed".to_owned())?,
        )
        .await?;
    let model_id =
        verify_model_and_assertion_lifecycle(&storage, &context, store_id, &endpoint, &table)
            .await?;
    verify_store_deletion_lifecycle(&storage, &context, store_id, model_id, &endpoint, &table)
        .await?;
    verify_transaction_limit(&storage, &context).await?;
    verify_empty_filtered_page_and_canonical_changes(&storage, &context).await?;
    verify_wrong_shard_corruption_is_rejected(&storage, &context, &endpoint, &table).await?;
    runtime.stop().await?;
    Ok(())
}

async fn verify_wrong_shard_corruption_is_rejected(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    endpoint: &str,
    table: &str,
) -> Result<(), Box<dyn Error>> {
    let client = test_client(endpoint).await;
    let store_id = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 777));
    storage
        .create_store(
            context,
            store_id,
            StoreName::new("Shard Integrity".to_owned())?,
        )
        .await?;
    let tuple = tuple("document:corrupt#viewer@user:anne")?;
    storage
        .write_tuples(
            context,
            store_id,
            Vec::new(),
            vec![tuple],
            TupleWriteOptions::default(),
        )
        .await?;
    verify_wrong_tuple_shard(storage, context, &client, table, store_id).await?;
    verify_wrong_change_shard(storage, context, &client, table, store_id).await?;
    verify_wrong_store_shard(storage, context, &client, table, store_id).await
}

async fn verify_wrong_tuple_shard(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    client: &Client,
    table: &str,
    store_id: StoreId,
) -> Result<(), Box<dyn Error>> {
    let forward = format!("F#{store_id}#{:02x}", forward_shard("corrupt"));
    let mut forward_item = query_partition(client, table, &forward)
        .await?
        .into_iter()
        .next()
        .ok_or("missing forward corruption fixture")?;
    forward_item.insert(
        "pk".to_owned(),
        AttributeValue::S(format!(
            "F#{store_id}#{:02x}",
            forward_shard("corrupt").wrapping_add(1) & 31
        )),
    );
    put_raw(client, table, forward_item).await?;
    let Err(tuple_error) = storage
        .read_tuples(
            context,
            store_id,
            &TupleReadFilter::all(),
            &PageOptions::new(
                NonZeroU32::new(100).ok_or("nonzero")?,
                None,
                &InputLimits::default(),
            )?,
        )
        .await
    else {
        return Err("wrong-shard tuple was not rejected".into());
    };
    assert_eq!(tuple_error.kind(), StorageErrorKind::Integrity);
    Ok(())
}

async fn verify_wrong_change_shard(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    client: &Client,
    table: &str,
    store_id: StoreId,
) -> Result<(), Box<dyn Error>> {
    let mut change_item = None;
    for shard in 0..4 {
        let partition = format!("C#{store_id}#{shard:02x}");
        if let Some(item) = query_partition(client, table, &partition)
            .await?
            .into_iter()
            .next()
        {
            change_item = Some((partition, item));
            break;
        }
    }
    let (change_partition, mut change_item) = change_item.ok_or("missing change fixture")?;
    let wrong_change_partition = if change_partition.ends_with("#00") {
        format!("C#{store_id}#01")
    } else {
        format!("C#{store_id}#00")
    };
    change_item.insert("pk".to_owned(), AttributeValue::S(wrong_change_partition));
    put_raw(client, table, change_item).await?;
    let Err(change_error) = storage
        .read_changes(
            context,
            store_id,
            &ChangeFilter::default(),
            &PageOptions::new(
                NonZeroU32::new(100).ok_or("nonzero")?,
                None,
                &InputLimits::default(),
            )?,
        )
        .await
    else {
        return Err("wrong-shard change was not rejected".into());
    };
    assert_eq!(change_error.kind(), StorageErrorKind::Integrity);
    Ok(())
}

async fn verify_wrong_store_shard(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    client: &Client,
    table: &str,
    store_id: StoreId,
) -> Result<(), Box<dyn Error>> {
    let correct_store_partition = format!(
        "S#{:02x}",
        stable_shard(store_id.to_string().as_bytes(), 16)
    );
    let mut store_item = query_partition(client, table, &correct_store_partition)
        .await?
        .into_iter()
        .find(|item| {
            item.get("sk")
                .and_then(|value| value.as_b().ok())
                .is_some_and(|value| value.as_ref() == store_id.to_string().as_bytes())
        })
        .ok_or("missing store fixture")?;
    let wrong_store_partition = if correct_store_partition == "S#00" {
        "S#01".to_owned()
    } else {
        "S#00".to_owned()
    };
    store_item.insert("pk".to_owned(), AttributeValue::S(wrong_store_partition));
    put_raw(client, table, store_item).await?;
    let Err(store_error) = storage
        .list_stores(
            context,
            &StoreFilter::all(),
            &PageOptions::new(
                NonZeroU32::new(100).ok_or("nonzero")?,
                None,
                &InputLimits::default(),
            )?,
        )
        .await
    else {
        return Err("wrong-shard store was not rejected".into());
    };
    assert_eq!(store_error.kind(), StorageErrorKind::Integrity);
    Ok(())
}

async fn put_raw(
    client: &Client,
    table: &str,
    item: std::collections::HashMap<String, AttributeValue>,
) -> Result<(), Box<dyn Error>> {
    client
        .put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await?;
    Ok(())
}

fn stable_shard(identity: &[u8], shards: u8) -> u8 {
    Sha256::digest(identity)
        .first()
        .copied()
        .unwrap_or_default()
        % shards
}

async fn verify_snapshot_capabilities(
    storage: &DynamoDbStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 92));
    let direct = tuple("document:roadmap#viewer@user:anne")?;
    let userset = tuple("document:roadmap#viewer@group:engineering#member")?;
    storage
        .write_tuples(
            context,
            store_id,
            Vec::new(),
            vec![direct.clone(), userset.clone()],
            TupleWriteOptions::default(),
        )
        .await?;
    let object_filter = ObjectRelationFilter::new(
        "document:roadmap".parse()?,
        "viewer".parse()?,
        Vec::new(),
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    assert_eq!(
        storage
            .count_object_relation(context, store_id, &object_filter)
            .await?,
        2
    );
    let mut forward = storage
        .read_object_relation(context, store_id, &object_filter, read_options(10)?)
        .await?;
    assert_eq!(forward.by_ref().count(), 2);
    let exact_subject_filter = ObjectRelationFilter::new(
        "document:roadmap".parse()?,
        "viewer".parse()?,
        vec![direct.key().subject().clone()],
        ConditionFilter::any(),
        &InputLimits::default(),
    )?;
    assert_eq!(
        storage
            .read_object_relation(context, store_id, &exact_subject_filter, read_options(10)?,)
            .await?
            .count(),
        1
    );
    let userset_filter = UsersetTupleFilter::new(
        "document:roadmap".parse()?,
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
        vec!["roadmap".parse()?],
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

fn read_options(maximum: u32) -> Result<ReadOptions, Box<dyn Error>> {
    Ok(ReadOptions::new(
        NonZeroU32::new(maximum).ok_or("read limit must be nonzero")?,
        &InputLimits::default(),
    )?)
}

async fn verify_namespace_without_directory_record(
    storage: &DynamoDbStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 91));
    let relationship = tuple("document:namespace#viewer@user:anne")?;
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
            .read_exact_tuple(context, store_id, relationship.key())
            .await?
            .tuple(),
        &relationship
    );
    Ok(())
}

async fn verify_store_deletion_lifecycle(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    store_id: StoreId,
    model_id: AuthorizationModelId,
    endpoint: &str,
    table: &str,
) -> Result<(), Box<dyn Error>> {
    storage.delete_store(context, store_id).await?;
    let Err(error) = storage.read_store(context, store_id).await else {
        return Err("deleted store must not remain visible".into());
    };
    assert_eq!(error.kind(), StorageErrorKind::NotFound);
    storage.delete_store(context, store_id).await?;
    assert_eq!(
        storage
            .read_model(context, store_id, model_id)
            .await?
            .model_id(),
        &model_id
    );

    let client = test_client(endpoint).await;
    assert!(
        !query_partition(&client, table, &format!("M#{store_id}"))
            .await?
            .is_empty(),
        "deleting a store must preserve its immutable namespace data"
    );
    Ok(())
}

async fn verify_model_and_assertion_lifecycle(
    storage: &DynamoDbStorage,
    context: &OperationContext,
    store_id: StoreId,
    endpoint: &str,
    table: &str,
) -> Result<AuthorizationModelId, Box<dyn Error>> {
    let model_id = AuthorizationModelId::from_ulid(Ulid::from_parts(1_700_000_000_100, 2));
    let model = stored_model(store_id, model_id)?;
    storage.write_model(context, Arc::clone(&model)).await?;
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
    let client = test_client(endpoint).await;
    let assertion_partition = format!("A#{store_id}#{model_id}");
    let first_generation = read_head(&client, table, &assertion_partition).await?;
    let replacement = Assertion::new(
        "document:contract#viewer@user:bob".parse()?,
        false,
        ContextualTuples::new(Vec::new(), &InputLimits::default())?,
        ConditionContext::empty(),
    );
    storage
        .write_assertions(context, store_id, model_id, vec![replacement.clone()])
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        storage
            .read_assertions(context, store_id, model_id)
            .await?
            .as_ref(),
        &[replacement]
    );
    let manifests = query_partition(&client, table, &assertion_partition).await?;
    assert_eq!(
        manifests.len(),
        2,
        "HEAD plus the active assertion manifest"
    );
    let retired_blob_partition = format!("B#assertion#{assertion_partition}#{first_generation}");
    assert!(
        query_partition(&client, table, &retired_blob_partition)
            .await?
            .is_empty(),
        "retired assertion chunks must be reclaimed"
    );
    Ok(model_id)
}

async fn test_client(endpoint: &str) -> Client {
    let http_client = HttpClientBuilder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
        .build_https();
    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-west-2"))
        .http_client(http_client)
        .load()
        .await;
    let config = aws_sdk_dynamodb::config::Builder::from(&shared)
        .endpoint_url(endpoint)
        .build();
    Client::from_conf(config)
}

async fn read_head(
    client: &Client,
    table: &str,
    partition: &str,
) -> Result<String, Box<dyn Error>> {
    let item = client
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(partition.to_owned()))
        .key("sk", AttributeValue::B(b"head".as_slice().into()))
        .consistent_read(true)
        .send()
        .await?
        .item
        .ok_or("assertion HEAD is missing")?;
    Ok(item
        .get("g")
        .and_then(|value| value.as_s().ok())
        .ok_or("assertion HEAD generation is missing")?
        .to_owned())
}

async fn query_partition(
    client: &Client,
    table: &str,
    partition: &str,
) -> Result<Vec<std::collections::HashMap<String, AttributeValue>>, Box<dyn Error>> {
    Ok(client
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", AttributeValue::S(partition.to_owned()))
        .consistent_read(true)
        .send()
        .await?
        .items
        .unwrap_or_default())
}

async fn verify_transaction_limit(
    storage: &DynamoDbStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 49));
    storage
        .create_store(
            context,
            store_id,
            StoreName::new("Mutation Limit".to_owned())?,
        )
        .await?;
    let writes = (0..49)
        .map(|index| tuple(&format!("document:item-{index}#viewer@user:anne")))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        storage
            .write_tuples(
                context,
                store_id,
                Vec::new(),
                writes,
                TupleWriteOptions::default()
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
                store_id,
                &TupleReadFilter::all(),
                &PageOptions::new(
                    NonZeroU32::new(7).ok_or("nonzero")?,
                    cursor,
                    &InputLimits::default(),
                )?,
            )
            .await?;
        cursor = page.continuation().cloned();
        observed.extend(page.into_items());
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(observed.len(), 49);
    assert_eq!(
        observed
            .iter()
            .map(|tuple| tuple.tuple().key().to_string())
            .collect::<BTreeSet<_>>()
            .len(),
        49,
        "logical pagination must not duplicate or omit tuples"
    );
    let mut change_cursor = None;
    let mut change_ids = BTreeSet::new();
    loop {
        let page = storage
            .read_changes(
                context,
                store_id,
                &ChangeFilter::default(),
                &PageOptions::new(
                    NonZeroU32::new(7).ok_or("nonzero")?,
                    change_cursor,
                    &InputLimits::default(),
                )?,
            )
            .await?;
        change_cursor = page.continuation().cloned();
        change_ids.extend(page.into_items().into_iter().map(|change| change.id()));
        if change_cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        change_ids.len(),
        49,
        "mid-batch pages must retain every change"
    );
    let too_many = (0..50)
        .map(|index| tuple(&format!("document:overflow-{index}#viewer@user:anne")))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        storage
            .write_tuples(
                context,
                store_id,
                Vec::new(),
                too_many,
                TupleWriteOptions::default()
            )
            .await
            .is_err()
    );
    Ok(())
}

async fn verify_empty_filtered_page_and_canonical_changes(
    storage: &DynamoDbStorage,
    context: &OperationContext,
) -> Result<(), Box<dyn Error>> {
    let store_id = StoreId::from_ulid(Ulid::from_parts(1_700_000_000_000, 140));
    let mut object_ids = (0..10_000)
        .map(|index| format!("page-{index:05}"))
        .filter(|object_id| forward_shard(object_id) == 0)
        .take(140)
        .collect::<Vec<_>>();
    if object_ids.len() != 140 {
        return Err("failed to generate an asymmetric shard fixture".into());
    }
    let last = object_ids.pop().ok_or("missing final object ID")?;
    let mut writes = object_ids
        .into_iter()
        .map(|object_id| tuple(&format!("document:{object_id}#viewer@user:bob")))
        .collect::<Result<Vec<_>, _>>()?;
    writes.push(tuple(&format!("document:{last}#viewer@user:anne"))?);
    for batch in writes.chunks(49) {
        storage
            .write_tuples(
                context,
                store_id,
                Vec::new(),
                batch.to_vec(),
                TupleWriteOptions::default(),
            )
            .await?;
    }
    let filter = TupleReadFilter::new("document".parse()?, None, None, Some("user:anne".parse()?))?;
    let first = storage
        .read_tuples(
            context,
            store_id,
            &filter,
            &PageOptions::new(
                NonZeroU32::new(10).ok_or("nonzero")?,
                None,
                &InputLimits::default(),
            )?,
        )
        .await?;
    assert!(first.items().is_empty());
    let second = storage
        .read_tuples(
            context,
            store_id,
            &filter,
            &PageOptions::new(
                NonZeroU32::new(10).ok_or("nonzero")?,
                first.continuation().cloned(),
                &InputLimits::default(),
            )?,
        )
        .await?;
    assert_eq!(second.items().len(), 1);

    let changes = storage
        .read_changes(
            context,
            store_id,
            &ChangeFilter::default(),
            &PageOptions::new(
                NonZeroU32::new(200).ok_or("nonzero")?,
                None,
                &InputLimits::default(),
            )?,
        )
        .await?
        .into_items();
    for transaction in changes.chunks(49) {
        assert!(transaction.windows(2).all(|pair| {
            pair.first().map(|change| change.tuple().key())
                <= pair.get(1).map(|change| change.tuple().key())
        }));
    }
    Ok(())
}

fn forward_shard(object_id: &str) -> u8 {
    let mut identity = b"document\0".to_vec();
    identity.extend_from_slice(object_id.as_bytes());
    Sha256::digest(identity)
        .first()
        .copied()
        .unwrap_or_default()
        & 31
}

fn operation_context() -> Result<OperationContext, Box<dyn Error>> {
    let timeout = RequestTimeout::new(Duration::from_secs(30))?;
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
