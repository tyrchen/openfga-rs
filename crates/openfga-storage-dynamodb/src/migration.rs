//! Explicit single-table provisioning and schema compatibility checks.

use aws_sdk_dynamodb::{
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        PointInTimeRecoverySpecification, ReturnConsumedCapacity, ScalarAttributeType,
        SseSpecification, SseType, TableDescription, TableStatus, Tag,
    },
};
use openfga_storage::{OperationContext, StorageError, StorageErrorKind};

use crate::{
    DynamoDbStorageConfig,
    client::DynamoClient,
    item::{self, Item},
    key::{CHANGE_SHARDS, FORWARD_SHARDS, GARBAGE_COLLECTION_SHARDS, REVERSE_SHARDS, STORE_SHARDS},
};

/// Current immutable `DynamoDB` physical-schema version.
pub const DYNAMODB_SCHEMA_VERSION: u32 = 1;
const METADATA_PK: &str = "D#schema";
const METADATA_SK: &[u8] = b"v1";

/// Safe `DynamoDB` provisioning status.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DynamoDbProvisioningStatus {
    /// The table does not exist.
    Missing,
    /// The table is still transitioning.
    Transitioning,
    /// The table is active and the schema metadata matches this binary.
    Ready,
    /// The table exists but its key schema or metadata is incompatible.
    Incompatible,
}

pub(crate) async fn provision(
    client: &DynamoClient,
    context: &OperationContext,
    config: &DynamoDbStorageConfig,
) -> Result<DynamoDbProvisioningStatus, StorageError> {
    match status(client, context).await? {
        DynamoDbProvisioningStatus::Missing => {
            let pk = AttributeDefinition::builder()
                .attribute_name(item::PK)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .map_err(builder_error)?;
            let sk = AttributeDefinition::builder()
                .attribute_name(item::SK)
                .attribute_type(ScalarAttributeType::B)
                .build()
                .map_err(builder_error)?;
            let pk_key = KeySchemaElement::builder()
                .attribute_name(item::PK)
                .key_type(KeyType::Hash)
                .build()
                .map_err(builder_error)?;
            let sk_key = KeySchemaElement::builder()
                .attribute_name(item::SK)
                .key_type(KeyType::Range)
                .build()
                .map_err(builder_error)?;
            let mut create = client
                .sdk()
                .create_table()
                .table_name(client.table())
                .attribute_definitions(pk)
                .attribute_definitions(sk)
                .key_schema(pk_key)
                .key_schema(sk_key)
                .billing_mode(BillingMode::PayPerRequest);
            if config.endpoint.is_none() {
                let tags = config
                    .provisioning
                    .tags
                    .iter()
                    .map(|(key, value)| {
                        Tag::builder()
                            .key(key)
                            .value(value)
                            .build()
                            .map_err(builder_error)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                create = create
                    .deletion_protection_enabled(config.provisioning.deletion_protection)
                    .set_tags(Some(tags));
                if let Some(key) = &config.provisioning.kms_key_identifier {
                    create = create.sse_specification(
                        SseSpecification::builder()
                            .enabled(true)
                            .sse_type(SseType::Kms)
                            .kms_master_key_id(key.as_str())
                            .build(),
                    );
                }
            }
            client
                .execute(context, "dynamodb_create_table_failed", create.send())
                .await?;
            wait_until_active(client, context).await?;
            configure_point_in_time_recovery(client, context, config).await?;
            put_metadata(client, context).await?;
            Ok(DynamoDbProvisioningStatus::Ready)
        }
        DynamoDbProvisioningStatus::Ready => Ok(DynamoDbProvisioningStatus::Ready),
        DynamoDbProvisioningStatus::Transitioning => {
            wait_until_active(client, context).await?;
            ensure_compatible_layout(client, context).await?;
            configure_point_in_time_recovery(client, context, config).await?;
            put_metadata(client, context).await?;
            status(client, context).await
        }
        DynamoDbProvisioningStatus::Incompatible => Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_schema_incompatible",
        )),
    }
}

async fn configure_point_in_time_recovery(
    client: &DynamoClient,
    context: &OperationContext,
    config: &DynamoDbStorageConfig,
) -> Result<(), StorageError> {
    if config.endpoint.is_some() || !config.provisioning.point_in_time_recovery {
        return Ok(());
    }
    let specification = PointInTimeRecoverySpecification::builder()
        .point_in_time_recovery_enabled(true)
        .build()
        .map_err(builder_error)?;
    client
        .execute(
            context,
            "dynamodb_pitr_enable_failed",
            client
                .sdk()
                .update_continuous_backups()
                .table_name(client.table())
                .point_in_time_recovery_specification(specification)
                .send(),
        )
        .await?;
    Ok(())
}

pub(crate) async fn status(
    client: &DynamoClient,
    context: &OperationContext,
) -> Result<DynamoDbProvisioningStatus, StorageError> {
    let described = client
        .execute(
            context,
            "dynamodb_describe_table_failed",
            client
                .sdk()
                .describe_table()
                .table_name(client.table())
                .send(),
        )
        .await;
    let described = match described {
        Ok(value) => value,
        Err(error) if error.kind() == StorageErrorKind::NotFound => {
            return Ok(DynamoDbProvisioningStatus::Missing);
        }
        Err(error) => return Err(error),
    };
    let table = described.table().ok_or_else(|| {
        StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_table_description_missing",
        )
    })?;
    if table.table_status() != Some(&TableStatus::Active) {
        return Ok(DynamoDbProvisioningStatus::Transitioning);
    }
    if !table_layout_compatible(table) {
        return Ok(DynamoDbProvisioningStatus::Incompatible);
    }
    let output = client
        .execute(
            context,
            "dynamodb_schema_read_failed",
            client
                .sdk()
                .get_item()
                .return_consumed_capacity(ReturnConsumedCapacity::Total)
                .table_name(client.table())
                .set_key(Some(metadata_key()))
                .consistent_read(true)
                .send(),
        )
        .await?;
    match output.item() {
        Some(metadata) if metadata_matches(metadata) => Ok(DynamoDbProvisioningStatus::Ready),
        None | Some(_) => Ok(DynamoDbProvisioningStatus::Incompatible),
    }
}

fn table_layout_compatible(table: &TableDescription) -> bool {
    let key_schema = table.key_schema();
    let definitions = table.attribute_definitions();
    key_schema.len() == 2
        && key_schema
            .iter()
            .any(|key| key.attribute_name() == item::PK && key.key_type() == &KeyType::Hash)
        && key_schema
            .iter()
            .any(|key| key.attribute_name() == item::SK && key.key_type() == &KeyType::Range)
        && definitions.len() == 2
        && definitions.iter().any(|definition| {
            definition.attribute_name() == item::PK
                && definition.attribute_type() == &ScalarAttributeType::S
        })
        && definitions.iter().any(|definition| {
            definition.attribute_name() == item::SK
                && definition.attribute_type() == &ScalarAttributeType::B
        })
}

async fn ensure_compatible_layout(
    client: &DynamoClient,
    context: &OperationContext,
) -> Result<(), StorageError> {
    let output = client
        .execute(
            context,
            "dynamodb_describe_table_failed",
            client
                .sdk()
                .describe_table()
                .table_name(client.table())
                .send(),
        )
        .await?;
    if output.table().is_some_and(table_layout_compatible) {
        Ok(())
    } else {
        Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_schema_incompatible",
        ))
    }
}

pub(crate) async fn require_ready(
    client: &DynamoClient,
    context: &OperationContext,
) -> Result<(), StorageError> {
    match status(client, context).await? {
        DynamoDbProvisioningStatus::Ready => Ok(()),
        DynamoDbProvisioningStatus::Missing | DynamoDbProvisioningStatus::Transitioning => Err(
            StorageError::new(StorageErrorKind::Unavailable, "dynamodb_schema_not_ready"),
        ),
        DynamoDbProvisioningStatus::Incompatible => Err(StorageError::new(
            StorageErrorKind::Integrity,
            "dynamodb_schema_incompatible",
        )),
    }
}

async fn put_metadata(
    client: &DynamoClient,
    context: &OperationContext,
) -> Result<(), StorageError> {
    let mut metadata = metadata_key();
    metadata.insert(
        item::SCHEMA_VERSION.to_owned(),
        AttributeValue::N(DYNAMODB_SCHEMA_VERSION.to_string()),
    );
    for (name, value) in metadata_counts() {
        metadata.insert(name.to_owned(), AttributeValue::N(value.to_string()));
    }
    let result = client
        .execute(
            context,
            "dynamodb_schema_write_failed",
            client
                .sdk()
                .put_item()
                .return_consumed_capacity(ReturnConsumedCapacity::Total)
                .table_name(client.table())
                .set_item(Some(metadata))
                .condition_expression("attribute_not_exists(pk)")
                .send(),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == StorageErrorKind::Conflict => {
            require_ready(client, context).await
        }
        Err(error) => Err(error),
    }
}

async fn wait_until_active(
    client: &DynamoClient,
    context: &OperationContext,
) -> Result<(), StorageError> {
    loop {
        context.check()?;
        let output = client
            .execute(
                context,
                "dynamodb_describe_table_failed",
                client
                    .sdk()
                    .describe_table()
                    .table_name(client.table())
                    .send(),
            )
            .await?;
        if output.table().and_then(|table| table.table_status()) == Some(&TableStatus::Active) {
            return Ok(());
        }
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => return Err(StorageError::new(StorageErrorKind::Cancelled, "dynamodb_operation_cancelled")),
            () = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
        }
    }
}

fn metadata_key() -> Item {
    Item::from([
        (
            item::PK.to_owned(),
            AttributeValue::S(METADATA_PK.to_owned()),
        ),
        (
            item::SK.to_owned(),
            AttributeValue::B(Blob::new(METADATA_SK)),
        ),
    ])
}

fn metadata_matches(metadata: &Item) -> bool {
    item::number_u32(metadata, item::SCHEMA_VERSION).ok() == Some(DYNAMODB_SCHEMA_VERSION)
        && metadata_counts().iter().all(|(name, expected)| {
            item::number_u32(metadata, name).ok() == Some(u32::from(*expected))
        })
}

const fn metadata_counts() -> [(&'static str, u8); 5] {
    [
        ("fs", FORWARD_SHARDS),
        ("rs", REVERSE_SHARDS),
        ("cs", CHANGE_SHARDS),
        ("ss", STORE_SHARDS),
        ("gs", GARBAGE_COLLECTION_SHARDS),
    ]
}

fn builder_error(error: impl std::error::Error + Send + Sync + 'static) -> StorageError {
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
    fn test_should_require_exact_key_scalar_types() -> Result<(), StorageError> {
        assert!(table_layout_compatible(&table_description(
            ScalarAttributeType::S,
            ScalarAttributeType::B,
        )?));
        assert!(!table_layout_compatible(&table_description(
            ScalarAttributeType::B,
            ScalarAttributeType::S,
        )?));
        Ok(())
    }

    fn table_description(
        pk_type: ScalarAttributeType,
        sk_type: ScalarAttributeType,
    ) -> Result<TableDescription, StorageError> {
        let pk = AttributeDefinition::builder()
            .attribute_name(item::PK)
            .attribute_type(pk_type)
            .build()
            .map_err(builder_error)?;
        let sk = AttributeDefinition::builder()
            .attribute_name(item::SK)
            .attribute_type(sk_type)
            .build()
            .map_err(builder_error)?;
        let pk_key = KeySchemaElement::builder()
            .attribute_name(item::PK)
            .key_type(KeyType::Hash)
            .build()
            .map_err(builder_error)?;
        let sk_key = KeySchemaElement::builder()
            .attribute_name(item::SK)
            .key_type(KeyType::Range)
            .build()
            .map_err(builder_error)?;
        Ok(TableDescription::builder()
            .set_attribute_definitions(Some(vec![pk, sk]))
            .set_key_schema(Some(vec![pk_key, sk_key]))
            .build())
    }
}
