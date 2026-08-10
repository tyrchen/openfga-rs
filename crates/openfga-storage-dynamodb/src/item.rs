//! `DynamoDB` attribute maps and bounded payload codecs.

use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};
use openfga_domain::{ChangeId, RelationshipTuple, StoreId, TupleKey};
use openfga_storage::{
    ChangeOperation, StorageError, StorageErrorKind, StoredTuple, TupleChange,
    persistence::{decode_tuple, encode_tuple},
};
use sha2::{Digest, Sha256};

pub(crate) type Item = HashMap<String, AttributeValue>;

pub(crate) const PK: &str = "pk";
pub(crate) const SK: &str = "sk";
pub(crate) const KIND: &str = "k";
pub(crate) const PAYLOAD: &str = "p";
pub(crate) const DIGEST: &str = "d";
pub(crate) const TIMESTAMP: &str = "t";
pub(crate) const STATE: &str = "st";
pub(crate) const GENERATION: &str = "g";
pub(crate) const CHUNK_COUNT: &str = "cc";
pub(crate) const PAYLOAD_BYTES: &str = "pb";
pub(crate) const NAME: &str = "n";
pub(crate) const CREATED_AT: &str = "ca";
pub(crate) const UPDATED_AT: &str = "ua";
pub(crate) const LAST_CHANGE: &str = "lc";
pub(crate) const SCHEMA_VERSION: &str = "sv";
pub(crate) const DUE_AT: &str = "due";
pub(crate) const MANIFEST_PK: &str = "mpk";
pub(crate) const MANIFEST_SK: &str = "msk";
pub(crate) const TARGET_PK: &str = "tpk";
pub(crate) const MAXIMUM_ITEM_BYTES: usize = 350 * 1_024;
pub(crate) const CHUNK_BYTES: usize = 256 * 1_024;

pub(crate) fn key(pk: String, sk: Vec<u8>) -> Item {
    HashMap::from([
        (PK.to_owned(), AttributeValue::S(pk)),
        (SK.to_owned(), AttributeValue::B(Blob::new(sk))),
    ])
}

pub(crate) fn tuple_item(
    pk: String,
    sk: Vec<u8>,
    tuple: &RelationshipTuple,
    timestamp: SystemTime,
) -> Result<Item, StorageError> {
    let payload = encode_tuple(tuple)?;
    let digest = Sha256::digest(&payload);
    let mut item = key(pk, sk);
    item.insert(KIND.to_owned(), AttributeValue::S("tuple".to_owned()));
    item.insert(PAYLOAD.to_owned(), AttributeValue::B(Blob::new(payload)));
    item.insert(
        DIGEST.to_owned(),
        AttributeValue::B(Blob::new(digest.to_vec())),
    );
    item.insert(
        TIMESTAMP.to_owned(),
        AttributeValue::N(epoch_millis(timestamp)?.to_string()),
    );
    require_item_limit(&item)?;
    Ok(item)
}

pub(crate) fn decode_stored_tuple(item: &Item) -> Result<StoredTuple, StorageError> {
    require_kind(item, "tuple")?;
    let payload = binary(item, PAYLOAD)?;
    let expected_digest = binary(item, DIGEST)?;
    if Sha256::digest(payload).as_slice() != expected_digest {
        return Err(integrity("dynamodb_tuple_digest_mismatch"));
    }
    let tuple = decode_tuple(payload)?;
    let timestamp = system_time(number_u64(item, TIMESTAMP)?)?;
    Ok(StoredTuple::new(tuple, timestamp))
}

pub(crate) fn encode_changes(changes: &[TupleChange]) -> Result<Vec<u8>, StorageError> {
    if changes.is_empty()
        || changes
            .windows(2)
            .any(|pair| pair.first().map(TupleChange::id) >= pair.get(1).map(TupleChange::id))
    {
        return Err(integrity("dynamodb_change_batch_order_invalid"));
    }
    let mut output = Vec::new();
    output.extend_from_slice(b"CB\x01");
    let count = u16::try_from(changes.len()).map_err(|_| exhausted("dynamodb_change_count"))?;
    output.extend_from_slice(&count.to_be_bytes());
    for change in changes {
        output.push(match change.operation() {
            ChangeOperation::Write => 0,
            ChangeOperation::Delete => 1,
            _ => return Err(integrity("dynamodb_change_operation_unknown")),
        });
        output.extend_from_slice(change.id().to_string().as_bytes());
        output.extend_from_slice(&epoch_millis(change.timestamp())?.to_be_bytes());
        let payload = encode_tuple(change.tuple())?;
        let length = u32::try_from(payload.len())
            .map_err(|_| exhausted("dynamodb_change_payload_length"))?;
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&payload);
    }
    if output.len() > MAXIMUM_ITEM_BYTES {
        return Err(exhausted("dynamodb_change_batch_too_large"));
    }
    Ok(output)
}

pub(crate) fn decode_changes(
    bytes: &[u8],
    store_id: StoreId,
) -> Result<Vec<TupleChange>, StorageError> {
    if bytes.len() > MAXIMUM_ITEM_BYTES || bytes.get(..3) != Some(b"CB\x01") {
        return Err(integrity("dynamodb_change_batch_invalid"));
    }
    let mut offset = 3_usize;
    let count = read_u16(bytes, &mut offset)?;
    let mut changes = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        let operation = match take(bytes, &mut offset, 1)?.first() {
            Some(0) => ChangeOperation::Write,
            Some(1) => ChangeOperation::Delete,
            _ => return Err(integrity("dynamodb_change_operation_invalid")),
        };
        let id = std::str::from_utf8(take(bytes, &mut offset, 26)?)
            .map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Integrity,
                    "dynamodb_change_id_utf8",
                    error,
                )
            })?
            .parse::<ChangeId>()
            .map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Integrity,
                    "dynamodb_change_id_invalid",
                    error,
                )
            })?;
        let timestamp_bytes: [u8; 8] = take(bytes, &mut offset, 8)?
            .try_into()
            .map_err(|_| integrity("dynamodb_change_timestamp_invalid"))?;
        let timestamp = system_time(u64::from_be_bytes(timestamp_bytes))?;
        let length_bytes: [u8; 4] = take(bytes, &mut offset, 4)?
            .try_into()
            .map_err(|_| integrity("dynamodb_change_length_invalid"))?;
        let payload = take(
            bytes,
            &mut offset,
            u32::from_be_bytes(length_bytes) as usize,
        )?;
        changes.push(TupleChange::new(
            id,
            store_id,
            operation,
            decode_tuple(payload)?,
            timestamp,
        ));
    }
    if offset != bytes.len() {
        return Err(integrity("dynamodb_change_batch_trailing_bytes"));
    }
    if changes.is_empty()
        || changes
            .windows(2)
            .any(|pair| pair.first().map(TupleChange::id) >= pair.get(1).map(TupleChange::id))
    {
        return Err(integrity("dynamodb_change_batch_order_invalid"));
    }
    Ok(changes)
}

pub(crate) fn string<'a>(item: &'a Item, name: &str) -> Result<&'a str, StorageError> {
    item.get(name)
        .and_then(|value| value.as_s().ok())
        .map(String::as_str)
        .ok_or_else(|| integrity("dynamodb_string_attribute_invalid"))
}

pub(crate) fn binary<'a>(item: &'a Item, name: &str) -> Result<&'a [u8], StorageError> {
    item.get(name)
        .and_then(|value| value.as_b().ok())
        .map(Blob::as_ref)
        .ok_or_else(|| integrity("dynamodb_binary_attribute_invalid"))
}

pub(crate) fn number_u64(item: &Item, name: &str) -> Result<u64, StorageError> {
    string_number(item, name)?.parse().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "dynamodb_number_attribute_invalid",
            error,
        )
    })
}

pub(crate) fn number_u32(item: &Item, name: &str) -> Result<u32, StorageError> {
    string_number(item, name)?.parse().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "dynamodb_number_attribute_invalid",
            error,
        )
    })
}

pub(crate) fn system_time(milliseconds: u64) -> Result<SystemTime, StorageError> {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(milliseconds))
        .ok_or_else(|| integrity("dynamodb_timestamp_out_of_range"))
}

pub(crate) fn epoch_millis(time: SystemTime) -> Result<u64, StorageError> {
    let value = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "dynamodb_timestamp_before_epoch",
                error,
            )
        })?
        .as_millis();
    u64::try_from(value).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "dynamodb_timestamp_out_of_range",
            error,
        )
    })
}

pub(crate) fn require_item_limit(item: &Item) -> Result<(), StorageError> {
    if encoded_item_size(item).is_ok_and(|value| value <= MAXIMUM_ITEM_BYTES) {
        Ok(())
    } else {
        Err(exhausted("dynamodb_item_too_large"))
    }
}

pub(crate) fn encoded_item_size(item: &Item) -> Result<usize, StorageError> {
    item.iter().try_fold(0_usize, |total, (name, value)| {
        total
            .checked_add(name.len())
            .and_then(|size| size.checked_add(attribute_value_size(value)))
            .ok_or_else(|| exhausted("dynamodb_item_size_overflow"))
    })
}

pub(crate) fn attribute_value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) => value.len(),
        AttributeValue::B(value) => value.as_ref().len(),
        AttributeValue::Bool(_) | AttributeValue::Null(_) => 1,
        _ => MAXIMUM_ITEM_BYTES,
    }
}

pub(crate) fn require_tuple_identity(
    item: &Item,
    expected: &TupleKey,
) -> Result<StoredTuple, StorageError> {
    let tuple = decode_stored_tuple(item)?;
    if tuple.tuple().key() == expected {
        Ok(tuple)
    } else {
        Err(integrity("dynamodb_tuple_identity_mismatch"))
    }
}

fn require_kind(item: &Item, expected: &str) -> Result<(), StorageError> {
    if string(item, KIND)? == expected {
        Ok(())
    } else {
        Err(integrity("dynamodb_item_kind_mismatch"))
    }
}

fn string_number<'a>(item: &'a Item, name: &str) -> Result<&'a str, StorageError> {
    item.get(name)
        .and_then(|value| value.as_n().ok())
        .map(String::as_str)
        .ok_or_else(|| integrity("dynamodb_number_attribute_invalid"))
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> Result<u16, StorageError> {
    let value: [u8; 2] = take(bytes, offset, 2)?
        .try_into()
        .map_err(|_| integrity("dynamodb_change_count_invalid"))?;
    Ok(u16::from_be_bytes(value))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> Result<&'a [u8], StorageError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| integrity("dynamodb_payload_overflow"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| integrity("dynamodb_payload_truncated"))?;
    *offset = end;
    Ok(value)
}

const fn integrity(code: &'static str) -> StorageError {
    StorageError::new(StorageErrorKind::Integrity, code)
}

const fn exhausted(code: &'static str) -> StorageError {
    StorageError::new(StorageErrorKind::ResourceExhausted, code)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use openfga_domain::{ChangeId, RelationshipTuple, StoreId, TupleKey};
    use openfga_storage::{ChangeOperation, TupleChange};
    use ulid::Ulid;

    use super::{decode_changes, encode_changes};

    #[test]
    fn test_should_round_trip_packed_change_batches() -> Result<(), Box<dyn std::error::Error>> {
        let store_id = StoreId::from_ulid(Ulid::from_parts(1, 1));
        let tuple =
            RelationshipTuple::unconditional("document:a#viewer@user:b".parse::<TupleKey>()?);
        let change = TupleChange::new(
            ChangeId::from_ulid(Ulid::from_parts(2, 2)),
            store_id,
            ChangeOperation::Write,
            tuple,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(
            decode_changes(&encode_changes(std::slice::from_ref(&change))?, store_id)?,
            vec![change]
        );
        Ok(())
    }

    #[test]
    fn test_should_reject_reordered_and_duplicate_packed_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let store_id = StoreId::from_ulid(Ulid::from_parts(1, 1));
        let first = test_change(store_id, 2)?;
        let second = test_change(store_id, 3)?;
        assert!(encode_changes(&[second.clone(), first.clone()]).is_err());
        assert!(encode_changes(&[first.clone(), first.clone()]).is_err());

        let first_record = encode_changes(std::slice::from_ref(&first))?;
        let second_record = encode_changes(std::slice::from_ref(&second))?;
        let mut corrupt = b"CB\x01\x00\x02".to_vec();
        corrupt.extend_from_slice(second_record.get(5..).ok_or("missing second record")?);
        corrupt.extend_from_slice(first_record.get(5..).ok_or("missing first record")?);
        assert!(decode_changes(&corrupt, store_id).is_err());
        Ok(())
    }

    fn test_change(
        store_id: StoreId,
        randomness: u128,
    ) -> Result<TupleChange, Box<dyn std::error::Error>> {
        Ok(TupleChange::new(
            ChangeId::from_ulid(Ulid::from_parts(2, randomness)),
            store_id,
            ChangeOperation::Write,
            RelationshipTuple::unconditional(
                format!("document:{randomness}#viewer@user:b").parse::<TupleKey>()?,
            ),
            SystemTime::UNIX_EPOCH,
        ))
    }
}
