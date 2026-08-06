//! Validated filters, records, policies, and pagination values.

use std::{collections::BTreeSet, fmt, num::NonZeroU32, sync::Arc, time::SystemTime};

use openfga_domain::{
    AuthorizationModelId, ChangeId, ConditionName, ContextualTuples, InputLimits, ObjectId,
    ObjectRef, RelationName, RelationshipTuple, StoreId, SubjectRef, TupleKey, TypeName,
};
use openfga_model::{AuthorizationModelSource, CompiledModel};

use crate::{StorageError, StorageErrorKind};

const MINIMUM_STORE_NAME_BYTES: usize = 3;
const MAXIMUM_STORE_NAME_BYTES: usize = 64;
const MAXIMUM_CURSOR_BYTES: usize = 512;

/// A bounded store display name using the pinned API character allowlist.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct StoreName(String);

impl StoreName {
    /// Validates one externally supplied store display name.
    ///
    /// # Errors
    ///
    /// Returns a resource or integrity error for empty, oversized, or control-character input.
    pub fn new(value: String) -> Result<Self, StorageError> {
        if !(MINIMUM_STORE_NAME_BYTES..=MAXIMUM_STORE_NAME_BYTES).contains(&value.len()) {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "invalid_store_name_length",
            ));
        }
        if !value.bytes().all(is_store_name_byte) {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "invalid_store_name_character",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated display name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

const fn is_store_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b' ' | b'.' | b'-' | b'/' | b'^' | b'_' | b'&' | b'@')
}

impl fmt::Debug for StoreName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreName")
            .field("bytes", &self.0.len())
            .finish()
    }
}

/// One persisted store record.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StoreRecord {
    id: StoreId,
    name: StoreName,
    created_at: SystemTime,
    updated_at: SystemTime,
}

impl StoreRecord {
    /// Creates a store record using one transaction timestamp.
    #[must_use]
    pub const fn new(id: StoreId, name: StoreName, timestamp: SystemTime) -> Self {
        Self {
            id,
            name,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    /// Returns the immutable store identifier.
    #[must_use]
    pub const fn id(&self) -> StoreId {
        self.id
    }

    /// Returns the validated store display name.
    #[must_use]
    pub const fn name(&self) -> &StoreName {
        &self.name
    }

    /// Returns the creation timestamp.
    #[must_use]
    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns the last update timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    /// Returns a renamed record while retaining its creation timestamp.
    #[must_use]
    pub const fn renamed(&self, name: StoreName, timestamp: SystemTime) -> Self {
        Self {
            id: self.id,
            name,
            created_at: self.created_at,
            updated_at: timestamp,
        }
    }
}

/// One immutable, semantically compiled authorization model ready for persistence.
#[non_exhaustive]
pub struct StoredAuthorizationModel {
    source: Arc<AuthorizationModelSource>,
    compiled: Arc<CompiledModel>,
    written_at: SystemTime,
}

impl StoredAuthorizationModel {
    /// Validates source/compiled identity and semantics, then creates a persistable record.
    ///
    /// # Errors
    ///
    /// Returns an integrity error when the source and compiled handles differ
    /// in identity or compiler-produced source fingerprint.
    pub fn new(
        source: Arc<AuthorizationModelSource>,
        compiled: Arc<CompiledModel>,
        written_at: SystemTime,
    ) -> Result<Self, StorageError> {
        if source.store_id() != compiled.store_id() || source.model_id() != compiled.model_id() {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "model_source_compiled_identity_mismatch",
            ));
        }
        if source.fingerprint() != compiled.source_fingerprint() {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "model_source_compiled_semantics_mismatch",
            ));
        }
        Ok(Self {
            source,
            compiled,
            written_at,
        })
    }

    /// Returns the owning store identifier.
    #[must_use]
    pub fn store_id(&self) -> &StoreId {
        self.compiled.store_id()
    }

    /// Returns the immutable model identifier.
    #[must_use]
    pub fn model_id(&self) -> &AuthorizationModelId {
        self.compiled.model_id()
    }

    /// Returns the validated project-owned model source.
    #[must_use]
    pub const fn source(&self) -> &Arc<AuthorizationModelSource> {
        &self.source
    }

    /// Returns the identical compiled model published with the source.
    #[must_use]
    pub const fn compiled(&self) -> &Arc<CompiledModel> {
        &self.compiled
    }

    /// Returns the persistence timestamp.
    #[must_use]
    pub const fn written_at(&self) -> SystemTime {
        self.written_at
    }
}

impl fmt::Debug for StoredAuthorizationModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredAuthorizationModel")
            .field("store_id", self.store_id())
            .field("model_id", self.model_id())
            .field("fingerprint", &self.compiled.fingerprint())
            .field("written_at", &self.written_at)
            .finish_non_exhaustive()
    }
}

/// One stored tuple plus its transaction timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StoredTuple {
    tuple: RelationshipTuple,
    inserted_at: SystemTime,
}

impl StoredTuple {
    /// Creates a timestamped stored tuple.
    #[must_use]
    pub const fn new(tuple: RelationshipTuple, inserted_at: SystemTime) -> Self {
        Self { tuple, inserted_at }
    }

    /// Returns the relationship tuple.
    #[must_use]
    pub const fn tuple(&self) -> &RelationshipTuple {
        &self.tuple
    }

    /// Returns the insertion transaction timestamp.
    #[must_use]
    pub const fn inserted_at(&self) -> SystemTime {
        self.inserted_at
    }
}

/// One persisted assertion used to verify an immutable model.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Assertion {
    tuple: TupleKey,
    expectation: bool,
    contextual_tuples: ContextualTuples,
}

impl Assertion {
    /// Creates a fully validated assertion.
    #[must_use]
    pub const fn new(
        tuple: TupleKey,
        expectation: bool,
        contextual_tuples: ContextualTuples,
    ) -> Self {
        Self {
            tuple,
            expectation,
            contextual_tuples,
        }
    }

    /// Returns the asserted authorization question.
    #[must_use]
    pub const fn tuple(&self) -> &TupleKey {
        &self.tuple
    }

    /// Returns the expected authorization decision.
    #[must_use]
    pub const fn expectation(&self) -> bool {
        self.expectation
    }

    /// Returns the bounded assertion-only tuples.
    #[must_use]
    pub const fn contextual_tuples(&self) -> &ContextualTuples {
        &self.contextual_tuples
    }
}

/// Tuple changelog operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChangeOperation {
    /// A relationship tuple was written.
    Write,
    /// A relationship tuple was deleted.
    Delete,
}

/// One ordered tuple change committed atomically with the tuple mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TupleChange {
    id: ChangeId,
    store_id: StoreId,
    operation: ChangeOperation,
    tuple: RelationshipTuple,
    timestamp: SystemTime,
}

impl TupleChange {
    /// Creates a committed tuple change.
    #[must_use]
    pub const fn new(
        id: ChangeId,
        store_id: StoreId,
        operation: ChangeOperation,
        tuple: RelationshipTuple,
        timestamp: SystemTime,
    ) -> Self {
        Self {
            id,
            store_id,
            operation,
            tuple,
            timestamp,
        }
    }

    /// Returns the monotonic change identifier.
    #[must_use]
    pub const fn id(&self) -> ChangeId {
        self.id
    }

    /// Returns the owning store.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the change operation.
    #[must_use]
    pub const fn operation(&self) -> ChangeOperation {
        self.operation
    }

    /// Returns the tuple state written or removed.
    #[must_use]
    pub const fn tuple(&self) -> &RelationshipTuple {
        &self.tuple
    }

    /// Returns the shared mutation transaction timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
}

/// Duplicate-write or missing-delete behavior.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum WriteConflictPolicy {
    /// Reject the entire atomic mutation.
    #[default]
    Error,
    /// Treat the conflicting operation as a no-op.
    Ignore,
}

/// Atomic tuple mutation policies.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct TupleWriteOptions {
    on_missing_delete: WriteConflictPolicy,
    on_duplicate_write: WriteConflictPolicy,
}

impl TupleWriteOptions {
    /// Creates explicit delete/write conflict policies.
    #[must_use]
    pub const fn new(
        on_missing_delete: WriteConflictPolicy,
        on_duplicate_write: WriteConflictPolicy,
    ) -> Self {
        Self {
            on_missing_delete,
            on_duplicate_write,
        }
    }

    /// Returns missing-delete behavior.
    #[must_use]
    pub const fn on_missing_delete(self) -> WriteConflictPolicy {
        self.on_missing_delete
    }

    /// Returns duplicate-write behavior.
    #[must_use]
    pub const fn on_duplicate_write(self) -> WriteConflictPolicy {
        self.on_duplicate_write
    }
}

/// Summary of one committed tuple mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MutationOutcome {
    change_ids: Box<[ChangeId]>,
}

impl MutationOutcome {
    /// Creates a mutation outcome from ordered committed change IDs.
    #[must_use]
    pub fn new(change_ids: Vec<ChangeId>) -> Self {
        Self {
            change_ids: change_ids.into_boxed_slice(),
        }
    }

    /// Returns ordered IDs for non-no-op changes.
    #[must_use]
    pub const fn change_ids(&self) -> &[ChangeId] {
        &self.change_ids
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum ConditionSelection {
    #[default]
    Any,
    Unconditional,
    Named(BTreeSet<ConditionName>),
}

/// Bounded condition-selection policy for tuple reads.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConditionFilter(ConditionSelection);

impl ConditionFilter {
    /// Returns a filter accepting conditional and unconditional tuples.
    #[must_use]
    pub const fn any() -> Self {
        Self(ConditionSelection::Any)
    }

    /// Returns a filter accepting only unconditional tuples.
    #[must_use]
    pub const fn unconditional() -> Self {
        Self(ConditionSelection::Unconditional)
    }

    /// Validates a filter accepting only named conditions.
    ///
    /// # Errors
    ///
    /// Returns integrity or resource exhaustion for an empty or oversized name set.
    pub fn named(names: Vec<ConditionName>, limits: &InputLimits) -> Result<Self, StorageError> {
        if names.is_empty() {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "condition_filter_empty",
            ));
        }
        if names.len() > limits.user_filters() {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "condition_filter_limit",
            ));
        }
        Ok(Self(ConditionSelection::Named(names.into_iter().collect())))
    }

    /// Returns whether one tuple condition matches this filter.
    #[must_use]
    pub fn matches(&self, condition: &openfga_domain::ConditionReference) -> bool {
        match (condition, &self.0) {
            (_, ConditionSelection::Any)
            | (
                openfga_domain::ConditionReference::Unconditional,
                ConditionSelection::Unconditional,
            ) => true,
            (
                openfga_domain::ConditionReference::Conditional(binding),
                ConditionSelection::Named(names),
            ) => names.contains(binding.name()),
            _ => false,
        }
    }
}

/// A bounded owned tuple-read result ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReadOptions {
    maximum_results: NonZeroU32,
}

impl ReadOptions {
    /// Validates an owned tuple-read result ceiling.
    ///
    /// # Errors
    ///
    /// Returns resource exhaustion when the value exceeds the configured result bound.
    pub fn new(maximum_results: NonZeroU32, limits: &InputLimits) -> Result<Self, StorageError> {
        if maximum_results.get() > limits.results() {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "tuple_read_result_limit",
            ));
        }
        Ok(Self { maximum_results })
    }

    /// Returns the maximum number of owned tuples.
    #[must_use]
    pub const fn maximum_results(self) -> usize {
        self.maximum_results.get() as usize
    }
}

/// Bounded object/relation forward-read filter.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ObjectRelationFilter {
    object: ObjectRef,
    relation: RelationName,
    subjects: BTreeSet<SubjectRef>,
    conditions: ConditionFilter,
}

impl ObjectRelationFilter {
    /// Validates a forward-read filter.
    ///
    /// # Errors
    ///
    /// Returns resource exhaustion for too many subject restrictions.
    pub fn new(
        object: ObjectRef,
        relation: RelationName,
        subjects: Vec<SubjectRef>,
        conditions: ConditionFilter,
        limits: &InputLimits,
    ) -> Result<Self, StorageError> {
        if subjects.len() > limits.user_filters() {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "forward_subject_filter_limit",
            ));
        }
        Ok(Self {
            object,
            relation,
            subjects: subjects.into_iter().collect(),
            conditions,
        })
    }

    /// Returns the exact target object.
    #[must_use]
    pub const fn object(&self) -> &ObjectRef {
        &self.object
    }

    /// Returns the exact target relation.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }

    /// Returns an optional exact-subject allowlist.
    #[must_use]
    pub const fn subjects(&self) -> &BTreeSet<SubjectRef> {
        &self.subjects
    }

    /// Returns the condition-selection policy.
    #[must_use]
    pub const fn conditions(&self) -> &ConditionFilter {
        &self.conditions
    }
}

/// One allowed userset subject type/relation pair.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct UsersetRestrictionFilter {
    subject_type: TypeName,
    relation: RelationName,
}

impl UsersetRestrictionFilter {
    /// Creates one validated userset restriction.
    #[must_use]
    pub const fn new(subject_type: TypeName, relation: RelationName) -> Self {
        Self {
            subject_type,
            relation,
        }
    }

    /// Returns the userset object type.
    #[must_use]
    pub const fn subject_type(&self) -> &TypeName {
        &self.subject_type
    }

    /// Returns the userset relation.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }
}

/// Bounded userset-only tuple-read filter.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct UsersetTupleFilter {
    object: ObjectRef,
    relation: RelationName,
    allowed: BTreeSet<UsersetRestrictionFilter>,
    conditions: ConditionFilter,
}

impl UsersetTupleFilter {
    /// Validates one userset tuple filter.
    ///
    /// # Errors
    ///
    /// Returns resource exhaustion for too many allowed userset restrictions.
    pub fn new(
        object: ObjectRef,
        relation: RelationName,
        allowed: Vec<UsersetRestrictionFilter>,
        conditions: ConditionFilter,
        limits: &InputLimits,
    ) -> Result<Self, StorageError> {
        if allowed.len() > limits.user_filters() {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "userset_filter_limit",
            ));
        }
        Ok(Self {
            object,
            relation,
            allowed: allowed.into_iter().collect(),
            conditions,
        })
    }

    /// Returns the exact target object.
    #[must_use]
    pub const fn object(&self) -> &ObjectRef {
        &self.object
    }

    /// Returns the exact target relation.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }

    /// Returns an optional allowed userset type/relation set.
    #[must_use]
    pub const fn allowed(&self) -> &BTreeSet<UsersetRestrictionFilter> {
        &self.allowed
    }

    /// Returns the condition-selection policy.
    #[must_use]
    pub const fn conditions(&self) -> &ConditionFilter {
        &self.conditions
    }
}

/// Bounded reverse-read filter from exact subjects to target objects.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReverseTupleFilter {
    object_type: TypeName,
    relation: RelationName,
    subjects: BTreeSet<SubjectRef>,
    object_ids: BTreeSet<ObjectId>,
    conditions: ConditionFilter,
}

impl ReverseTupleFilter {
    /// Validates a reverse-read filter.
    ///
    /// # Errors
    ///
    /// Returns an integrity or resource error for an empty/oversized subject set
    /// or an oversized object-ID restriction.
    pub fn new(
        object_type: TypeName,
        relation: RelationName,
        subjects: Vec<SubjectRef>,
        object_ids: Vec<ObjectId>,
        conditions: ConditionFilter,
        limits: &InputLimits,
    ) -> Result<Self, StorageError> {
        if subjects.is_empty() {
            return Err(StorageError::new(
                StorageErrorKind::Integrity,
                "reverse_subject_filter_empty",
            ));
        }
        if subjects.len() > limits.user_filters() || object_ids.len() > limits.results() as usize {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "reverse_filter_limit",
            ));
        }
        Ok(Self {
            object_type,
            relation,
            subjects: subjects.into_iter().collect(),
            object_ids: object_ids.into_iter().collect(),
            conditions,
        })
    }

    /// Returns the target object type.
    #[must_use]
    pub const fn object_type(&self) -> &TypeName {
        &self.object_type
    }

    /// Returns the target relation.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }

    /// Returns the exact starting subjects.
    #[must_use]
    pub const fn subjects(&self) -> &BTreeSet<SubjectRef> {
        &self.subjects
    }

    /// Returns an optional target object-ID allowlist.
    #[must_use]
    pub const fn object_ids(&self) -> &BTreeSet<ObjectId> {
        &self.object_ids
    }

    /// Returns the condition-selection policy.
    #[must_use]
    pub const fn conditions(&self) -> &ConditionFilter {
        &self.conditions
    }
}

/// One bounded opaque backend cursor.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct StorageCursor(Vec<u8>);

impl StorageCursor {
    /// Validates bounded nonempty cursor bytes.
    ///
    /// # Errors
    ///
    /// Returns invalid continuation for empty or oversized bytes.
    pub fn new(bytes: Vec<u8>) -> Result<Self, StorageError> {
        if bytes.is_empty() || bytes.len() > MAXIMUM_CURSOR_BYTES {
            return Err(StorageError::new(
                StorageErrorKind::InvalidContinuation,
                "storage_cursor_length",
            ));
        }
        Ok(Self(bytes))
    }

    /// Returns the opaque cursor bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for StorageCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageCursor")
            .field("bytes", &self.0.len())
            .finish()
    }
}

/// Stable backend-page request.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PageOptions {
    maximum_results: NonZeroU32,
    after: Option<StorageCursor>,
}

impl PageOptions {
    /// Validates a page-size ceiling and optional verified cursor.
    ///
    /// # Errors
    ///
    /// Returns resource exhaustion when the page exceeds configured bounds.
    pub fn new(
        maximum_results: NonZeroU32,
        after: Option<StorageCursor>,
        limits: &InputLimits,
    ) -> Result<Self, StorageError> {
        if maximum_results.get() > limits.results() {
            return Err(StorageError::new(
                StorageErrorKind::ResourceExhausted,
                "page_result_limit",
            ));
        }
        Ok(Self {
            maximum_results,
            after,
        })
    }

    /// Returns the maximum number of page items.
    #[must_use]
    pub const fn maximum_results(&self) -> usize {
        self.maximum_results.get() as usize
    }

    /// Returns the exclusive stable cursor.
    #[must_use]
    pub const fn after(&self) -> Option<&StorageCursor> {
        self.after.as_ref()
    }
}

/// One bounded stable page and its exclusive next cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Page<T> {
    items: Vec<T>,
    continuation: Option<StorageCursor>,
}

impl<T> Page<T> {
    /// Creates a page from bounded owned items and its next cursor.
    #[must_use]
    pub const fn new(items: Vec<T>, continuation: Option<StorageCursor>) -> Self {
        Self {
            items,
            continuation,
        }
    }

    /// Returns page items in stable canonical order.
    #[must_use]
    pub fn items(&self) -> &[T] {
        &self.items
    }

    /// Consumes the page and returns owned items.
    #[must_use]
    pub fn into_items(self) -> Vec<T> {
        self.items
    }

    /// Returns the next exclusive cursor, if more items exist.
    #[must_use]
    pub const fn continuation(&self) -> Option<&StorageCursor> {
        self.continuation.as_ref()
    }
}

/// Backend readiness without backend-specific details.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct HealthStatus {
    ready: bool,
    code: &'static str,
}

impl HealthStatus {
    /// Creates a stable health result.
    #[must_use]
    pub const fn new(ready: bool, code: &'static str) -> Self {
        Self { ready, code }
    }

    /// Returns whether the backend can accept traffic.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Returns the safe readiness code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::SystemTime};

    use openfga_domain::{InputLimits, Limit};
    use openfga_model::{
        AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, ModelLimits,
        RelationSource, RestrictionKindSource, RewriteSource, TypeDefinitionSource,
    };

    use super::{
        ConditionFilter, StorageCursor, StorageErrorKind, StoreName, StoredAuthorizationModel,
    };

    #[test]
    fn test_should_enforce_pinned_store_name_allowlist_and_redaction() -> Result<(), Box<dyn Error>>
    {
        let name = StoreName::new("Engineering / R&D @ US".to_owned())?;
        assert_eq!(name.as_str(), "Engineering / R&D @ US");
        assert!(!format!("{name:?}").contains("Engineering"));
        for invalid in [
            "ab".to_owned(),
            "a".repeat(65),
            "line\nbreak".to_owned(),
            "unicode-雪".to_owned(),
        ] {
            assert!(StoreName::new(invalid).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_should_reject_empty_condition_filters_and_redact_cursors() -> Result<(), Box<dyn Error>>
    {
        assert!(ConditionFilter::named(Vec::new(), &InputLimits::default()).is_err());
        let cursor = StorageCursor::new(b"sensitive-cursor".to_vec())?;
        assert!(!format!("{cursor:?}").contains("sensitive"));
        Ok(())
    }

    #[test]
    fn test_should_reject_same_id_source_and_compiled_semantic_mismatch()
    -> Result<(), Box<dyn Error>> {
        let source = Arc::new(model_source("viewer")?);
        let compiled = ModelCompiler::default().compile(&source)?;
        let different_source = Arc::new(model_source("editor")?);
        let error = StoredAuthorizationModel::new(different_source, compiled, SystemTime::now())
            .err()
            .ok_or("same-ID semantic mismatch unexpectedly accepted")?;
        assert_eq!(error.kind(), StorageErrorKind::Integrity);
        assert_eq!(error.code(), "model_source_compiled_semantics_mismatch");
        Ok(())
    }

    #[test]
    fn test_should_accept_source_proof_from_custom_compiler_limits() -> Result<(), Box<dyn Error>> {
        let relations = (0..101)
            .map(|index| {
                Ok(RelationSource::new(
                    format!("relation_{index}").parse()?,
                    RewriteSource::Direct,
                    vec![DirectRestrictionSource::new(
                        "user".parse()?,
                        RestrictionKindSource::Object,
                        None,
                    )],
                ))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        let source = Arc::new(AuthorizationModelSource::new(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?,
            "01ARZ3NDEKTSV4RRFFQ69G5FAW".parse()?,
            "1.1".to_owned(),
            vec![
                TypeDefinitionSource::new("user".parse()?, Vec::new()),
                TypeDefinitionSource::new("document".parse()?, relations),
            ],
            Vec::new(),
        ));
        let input = InputLimits::builder()
            .relations(Limit::<2_000>::new(101)?)
            .build();
        let model_compiler = ModelCompiler::new(ModelLimits::builder().input(input).build());
        let compiled_model = model_compiler.compile(&source)?;
        let stored = StoredAuthorizationModel::new(source, compiled_model, SystemTime::now())?;
        assert_eq!(stored.model_id().to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        Ok(())
    }

    fn model_source(relation: &str) -> Result<AuthorizationModelSource, Box<dyn Error>> {
        Ok(AuthorizationModelSource::new(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?,
            "01ARZ3NDEKTSV4RRFFQ69G5FAW".parse()?,
            "1.1".to_owned(),
            vec![
                TypeDefinitionSource::new("user".parse()?, Vec::new()),
                TypeDefinitionSource::new(
                    "document".parse()?,
                    vec![RelationSource::new(
                        relation.parse()?,
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
        ))
    }
}
