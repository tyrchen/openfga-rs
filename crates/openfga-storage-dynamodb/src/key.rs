//! Versioned physical-key, shard, and logical-cursor codecs.

use std::fmt::Write;

use openfga_domain::{
    AuthorizationModelId, ObjectRef, RelationName, StoreId, SubjectKind, SubjectRef, TupleKey,
    TypeName,
};
use openfga_storage::{StorageCursor, StorageError, StorageErrorKind};
use sha2::{Digest, Sha256};

pub(crate) const FORWARD_SHARDS: u8 = 32;
pub(crate) const REVERSE_SHARDS: u8 = 32;
pub(crate) const CHANGE_SHARDS: u8 = 4;
pub(crate) const STORE_SHARDS: u8 = 16;
pub(crate) const GARBAGE_COLLECTION_SHARDS: u8 = 16;
pub(crate) const MAXIMUM_KEY_BYTES: usize = 896;
const KEY_VERSION: u8 = 1;
const CURSOR_VERSION: u8 = 1;
const CURSOR_MAGIC: &[u8; 2] = b"DC";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CursorOperation {
    Tuple = 1,
    Store = 2,
    Model = 3,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TupleKeys {
    pub(crate) forward_partition: String,
    pub(crate) forward_sort: Vec<u8>,
    pub(crate) reverse_partition: String,
    pub(crate) reverse_sort: Vec<u8>,
}

pub(crate) fn tuple_keys(store_id: StoreId, key: &TupleKey) -> Result<TupleKeys, StorageError> {
    let forward_identity = encode_object(key);
    let reverse_identity = encode_subject(key.subject());
    let forward_sort = encode_forward(key)?;
    let reverse_sort = encode_reverse(key)?;
    Ok(TupleKeys {
        forward_partition: sharded_partition("F", store_id, &forward_identity, FORWARD_SHARDS),
        forward_sort,
        reverse_partition: sharded_partition("R", store_id, &reverse_identity, REVERSE_SHARDS),
        reverse_sort,
    })
}

pub(crate) fn forward_partition(store_id: StoreId, shard: u8) -> String {
    explicit_sharded_partition("F", store_id, shard)
}

pub(crate) fn forward_object_partition(store_id: StoreId, object: &ObjectRef) -> String {
    let mut identity = Vec::with_capacity(
        object
            .object_type()
            .as_str()
            .len()
            .saturating_add(object.object_id().as_str().len())
            .saturating_add(1),
    );
    identity.extend_from_slice(object.object_type().as_str().as_bytes());
    identity.push(0);
    identity.extend_from_slice(object.object_id().as_str().as_bytes());
    sharded_partition("F", store_id, &identity, FORWARD_SHARDS)
}

pub(crate) fn forward_object_relation_prefix(
    object: &ObjectRef,
    relation: &RelationName,
) -> Result<Vec<u8>, StorageError> {
    encode_segments(&[
        object.object_type().as_str(),
        object.object_id().as_str(),
        relation.as_str(),
    ])
}

pub(crate) fn reverse_partition(store_id: StoreId, subject: &SubjectRef) -> String {
    sharded_partition("R", store_id, &encode_subject(subject), REVERSE_SHARDS)
}

pub(crate) fn reverse_prefix(
    subject: &SubjectRef,
    object_type: &TypeName,
    relation: &RelationName,
) -> Result<Vec<u8>, StorageError> {
    encode_segments(&[
        subject_kind_name(subject.kind()),
        subject.subject_type().as_str(),
        subject.object_id(),
        subject.relation().map_or("", |value| value.as_str()),
        object_type.as_str(),
        relation.as_str(),
    ])
}

pub(crate) fn change_partition(store_id: StoreId, identity: &[u8]) -> String {
    sharded_partition("C", store_id, identity, CHANGE_SHARDS)
}

pub(crate) fn store_partition(store_id: StoreId) -> String {
    let identity = store_id.to_string();
    let shard = stable_shard(identity.as_bytes(), STORE_SHARDS);
    format!("S#{shard:02x}")
}

pub(crate) fn model_partition(store_id: StoreId) -> String {
    format!("M#{store_id}")
}

pub(crate) fn assertion_partition(store_id: StoreId, model_id: impl std::fmt::Display) -> String {
    format!("A#{store_id}#{model_id}")
}

pub(crate) fn change_head_partition(store_id: StoreId) -> String {
    format!("H#{store_id}")
}

pub(crate) fn garbage_collection_partition(identity: &[u8]) -> String {
    format!(
        "G#{:02x}",
        stable_shard(identity, GARBAGE_COLLECTION_SHARDS)
    )
}

pub(crate) fn encode_forward(key: &TupleKey) -> Result<Vec<u8>, StorageError> {
    let subject = key.subject();
    encode_segments(&[
        key.object().object_type().as_str(),
        key.object().object_id().as_str(),
        key.relation().as_str(),
        subject_kind_name(subject.kind()),
        subject.subject_type().as_str(),
        subject.object_id(),
        subject.relation().map_or("", |relation| relation.as_str()),
    ])
}

pub(crate) fn encode_reverse(key: &TupleKey) -> Result<Vec<u8>, StorageError> {
    let subject = key.subject();
    encode_segments(&[
        subject_kind_name(subject.kind()),
        subject.subject_type().as_str(),
        subject.object_id(),
        subject.relation().map_or("", |relation| relation.as_str()),
        key.object().object_type().as_str(),
        key.relation().as_str(),
        key.object().object_id().as_str(),
    ])
}

pub(crate) fn decode_forward(bytes: &[u8]) -> Result<TupleKey, StorageError> {
    let segments = decode_segments(bytes, 7)?;
    let object_type = segments.first().copied().ok_or_else(invalid_key)?;
    let object_id = segments.get(1).copied().ok_or_else(invalid_key)?;
    let relation = segments.get(2).copied().ok_or_else(invalid_key)?;
    let subject_segments = segments.get(3..7).ok_or_else(invalid_key)?;
    let subject = subject_from_segments(subject_segments)?;
    let canonical = format!("{object_type}:{object_id}#{relation}@{subject}");
    let key: TupleKey = canonical.parse().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "dynamodb_tuple_key_invalid",
            error,
        )
    })?;
    if encode_forward(&key)?.as_slice() != bytes {
        return Err(invalid_key());
    }
    Ok(key)
}

pub(crate) fn decode_reverse(bytes: &[u8]) -> Result<TupleKey, StorageError> {
    let segments = decode_segments(bytes, 7)?;
    let subject = subject_from_segments(segments.get(..4).ok_or_else(invalid_key)?)?;
    let object_type = segments.get(4).copied().ok_or_else(invalid_key)?;
    let relation = segments.get(5).copied().ok_or_else(invalid_key)?;
    let object_id = segments.get(6).copied().ok_or_else(invalid_key)?;
    let canonical = format!("{object_type}:{object_id}#{relation}@{subject}");
    let key: TupleKey = canonical.parse().map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "dynamodb_tuple_key_invalid",
            error,
        )
    })?;
    if encode_reverse(&key)?.as_slice() != bytes {
        return Err(invalid_key());
    }
    Ok(key)
}

pub(crate) fn encode_cursor(
    operation: CursorOperation,
    logical_key: &[u8],
) -> Result<StorageCursor, StorageError> {
    let length = u16::try_from(logical_key.len()).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::ResourceExhausted,
            "dynamodb_cursor_key_too_large",
            error,
        )
    })?;
    let mut bytes = Vec::with_capacity(logical_key.len().saturating_add(6));
    bytes.extend_from_slice(CURSOR_MAGIC);
    bytes.push(CURSOR_VERSION);
    bytes.push(operation as u8);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(logical_key);
    StorageCursor::new(bytes)
}

pub(crate) fn decode_cursor(
    operation: CursorOperation,
    cursor: &StorageCursor,
) -> Result<&[u8], StorageError> {
    let bytes = cursor.as_bytes();
    if bytes.len() < 6
        || bytes.get(..2) != Some(CURSOR_MAGIC.as_slice())
        || bytes.get(2) != Some(&CURSOR_VERSION)
        || bytes.get(3) != Some(&(operation as u8))
    {
        return Err(invalid_cursor());
    }
    let length_bytes: [u8; 2] = bytes
        .get(4..6)
        .and_then(|value| value.try_into().ok())
        .ok_or_else(invalid_cursor)?;
    let length = usize::from(u16::from_be_bytes(length_bytes));
    let value = bytes.get(6..).ok_or_else(invalid_cursor)?;
    if value.len() != length || value.is_empty() || !cursor_key_is_canonical(operation, value) {
        return Err(invalid_cursor());
    }
    Ok(value)
}

fn cursor_key_is_canonical(operation: CursorOperation, value: &[u8]) -> bool {
    match operation {
        CursorOperation::Tuple => decode_forward(value).is_ok(),
        CursorOperation::Store => std::str::from_utf8(value)
            .ok()
            .and_then(|text| text.parse::<StoreId>().ok())
            .is_some_and(|id| id.to_string().as_bytes() == value),
        CursorOperation::Model => std::str::from_utf8(value)
            .ok()
            .and_then(|text| text.parse::<AuthorizationModelId>().ok())
            .is_some_and(|id| id.to_string().as_bytes() == value),
    }
}

fn encode_segments(segments: &[&str]) -> Result<Vec<u8>, StorageError> {
    let capacity = segments.iter().try_fold(1_usize, |total, segment| {
        total.checked_add(1)?.checked_add(segment.len())
    });
    let capacity = capacity.ok_or_else(key_too_large)?;
    if capacity > MAXIMUM_KEY_BYTES {
        return Err(key_too_large());
    }
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(KEY_VERSION);
    for segment in segments {
        if segment.as_bytes().contains(&0) {
            return Err(invalid_key());
        }
        bytes.extend_from_slice(segment.as_bytes());
        bytes.push(0);
    }
    Ok(bytes)
}

fn decode_segments(bytes: &[u8], expected: usize) -> Result<Vec<&str>, StorageError> {
    if bytes.len() > MAXIMUM_KEY_BYTES || bytes.first() != Some(&KEY_VERSION) {
        return Err(invalid_key());
    }
    let remaining = bytes.get(1..).ok_or_else(invalid_key)?;
    if remaining.last() != Some(&0) {
        return Err(invalid_key());
    }
    let mut values = Vec::with_capacity(expected);
    for value in remaining
        .get(..remaining.len().saturating_sub(1))
        .ok_or_else(invalid_key)?
        .split(|byte| *byte == 0)
    {
        let value = std::str::from_utf8(value).map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "dynamodb_key_utf8_invalid",
                error,
            )
        })?;
        values.push(value);
    }
    if values.len() != expected {
        return Err(invalid_key());
    }
    Ok(values)
}

fn subject_from_segments(segments: &[&str]) -> Result<String, StorageError> {
    let [kind, subject_type, subject_id, relation] = segments else {
        return Err(invalid_key());
    };
    match *kind {
        "o" if relation.is_empty() && *subject_id != "*" => {
            Ok(format!("{subject_type}:{subject_id}"))
        }
        "u" if !relation.is_empty() && *subject_id != "*" => {
            Ok(format!("{subject_type}:{subject_id}#{relation}"))
        }
        "w" if relation.is_empty() && *subject_id == "*" => Ok(format!("{subject_type}:*")),
        _ => Err(invalid_key()),
    }
}

fn encode_object(key: &TupleKey) -> Vec<u8> {
    let mut value = Vec::with_capacity(
        key.object()
            .object_type()
            .as_str()
            .len()
            .saturating_add(key.object().object_id().as_str().len())
            .saturating_add(1),
    );
    value.extend_from_slice(key.object().object_type().as_str().as_bytes());
    value.push(0);
    value.extend_from_slice(key.object().object_id().as_str().as_bytes());
    value
}

fn encode_subject(subject: &SubjectRef) -> Vec<u8> {
    let mut value = Vec::with_capacity(subject.to_string().len().saturating_add(1));
    value.push(match subject.kind() {
        SubjectKind::Object => 0,
        SubjectKind::Userset => 1,
        SubjectKind::TypedWildcard => 2,
    });
    value.extend_from_slice(subject.to_string().as_bytes());
    value
}

fn sharded_partition(prefix: &str, store_id: StoreId, identity: &[u8], shards: u8) -> String {
    explicit_sharded_partition(prefix, store_id, stable_shard(identity, shards))
}

fn explicit_sharded_partition(prefix: &str, store_id: StoreId, shard: u8) -> String {
    let mut value = String::with_capacity(prefix.len().saturating_add(31));
    let _ = write!(value, "{prefix}#{store_id}#{shard:02x}");
    value
}

fn stable_shard(identity: &[u8], shards: u8) -> u8 {
    let digest = Sha256::digest(identity);
    digest.first().copied().unwrap_or_default() & shards.saturating_sub(1)
}

const fn subject_kind_name(kind: SubjectKind) -> &'static str {
    match kind {
        SubjectKind::Object => "o",
        SubjectKind::Userset => "u",
        SubjectKind::TypedWildcard => "w",
    }
}

const fn key_too_large() -> StorageError {
    StorageError::new(
        StorageErrorKind::ResourceExhausted,
        "dynamodb_key_too_large",
    )
}

const fn invalid_key() -> StorageError {
    StorageError::new(StorageErrorKind::Integrity, "dynamodb_key_invalid")
}

const fn invalid_cursor() -> StorageError {
    StorageError::new(
        StorageErrorKind::InvalidContinuation,
        "dynamodb_cursor_invalid",
    )
}

#[cfg(test)]
mod tests {
    use openfga_domain::TupleKey;
    use proptest::prelude::*;

    use super::{decode_forward, decode_reverse, encode_forward, encode_reverse};

    #[test]
    fn test_should_preserve_forward_and_reverse_field_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let first: TupleKey = "document:a#viewer@user:z".parse()?;
        let second: TupleKey = "document:b#viewer@user:a".parse()?;
        assert!(encode_forward(&first)? < encode_forward(&second)?);
        assert!(encode_reverse(&second)? < encode_reverse(&first)?);
        Ok(())
    }

    #[test]
    fn test_should_preserve_variable_length_canonical_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let first: TupleKey = "document:aa#viewer@user:a".parse()?;
        let second: TupleKey = "document:z#viewer@user:a".parse()?;
        assert!(first < second);
        assert!(encode_forward(&first)? < encode_forward(&second)?);
        assert_eq!(decode_reverse(&encode_reverse(&first)?)?, first);
        Ok(())
    }

    proptest! {
        #[test]
        fn test_should_round_trip_every_generated_canonical_tuple(
            object_id in "[a-zA-Z0-9_-]{1,64}",
            subject_id in "[a-zA-Z0-9_-]{1,64}",
        ) {
            let canonical = format!("document:{object_id}#viewer@user:{subject_id}");
            let key: TupleKey = canonical.parse().map_err(|error| TestCaseError::fail(format!("{error}")))?;
            let encoded = encode_forward(&key).map_err(|error| TestCaseError::fail(format!("{error}")))?;
            let decoded = decode_forward(&encoded).map_err(|error| TestCaseError::fail(format!("{error}")))?;
            prop_assert_eq!(decoded, key);
        }

        #[test]
        fn test_should_match_tuple_order_for_variable_length_fields(
            first_id in "[a-zA-Z0-9_-]{1,64}",
            second_id in "[a-zA-Z0-9_-]{1,64}",
        ) {
            let first: TupleKey = format!("document:{first_id}#viewer@user:anne")
                .parse()
                .map_err(|error| TestCaseError::fail(format!("{error}")))?;
            let second: TupleKey = format!("document:{second_id}#viewer@user:anne")
                .parse()
                .map_err(|error| TestCaseError::fail(format!("{error}")))?;
            let encoded_first = encode_forward(&first)
                .map_err(|error| TestCaseError::fail(format!("{error}")))?;
            let encoded_second = encode_forward(&second)
                .map_err(|error| TestCaseError::fail(format!("{error}")))?;
            prop_assert_eq!(first.cmp(&second), encoded_first.cmp(&encoded_second));
        }
    }
}
