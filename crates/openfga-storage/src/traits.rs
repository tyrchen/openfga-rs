//! Minimal object-safe asynchronous storage capabilities.

use std::sync::Arc;

use async_trait::async_trait;
use openfga_domain::{AuthorizationModelId, RelationshipTuple, StoreId, TupleKey};

use crate::{
    Assertion, ChangeFilter, HealthStatus, MutationOutcome, ObjectRelationFilter, OperationContext,
    Page, PageOptions, ReadOptions, ReverseTupleFilter, StorageError, StoreName, StoreRecord,
    StoredAuthorizationModel, StoredTuple, TupleChange, TupleReadFilter, TupleStream,
    TupleWriteOptions, UsersetTupleFilter,
};

/// Exact, forward, userset, and reverse tuple-read capability.
#[async_trait]
pub trait TupleReader: Send + Sync {
    /// Reads a stable bounded page matching a public partial tuple filter.
    ///
    /// # Errors
    ///
    /// Returns invalid continuation, cancellation, timeout, availability, or backend failures.
    async fn read_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &TupleReadFilter,
        options: &PageOptions,
    ) -> Result<Page<StoredTuple>, StorageError>;

    /// Reads one exact relationship tuple.
    ///
    /// # Errors
    ///
    /// Returns not found, cancellation, timeout, availability, or backend failures.
    async fn read_exact_tuple(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<StoredTuple, StorageError>;

    /// Reads a bounded owned object/relation snapshot.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, resource, or backend failures.
    async fn read_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError>;

    /// Reads a bounded owned userset-subject snapshot.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, resource, or backend failures.
    async fn read_userset_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &UsersetTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError>;

    /// Reads a bounded owned reverse-index snapshot.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, resource, or backend failures.
    async fn read_reverse_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ReverseTupleFilter,
        options: ReadOptions,
    ) -> Result<TupleStream, StorageError>;

    /// Returns whether an exact tuple exists.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, or backend failures.
    async fn tuple_exists(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        key: &TupleKey,
    ) -> Result<bool, StorageError>;

    /// Counts a bounded object/relation snapshot without returning rows.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, or backend failures.
    async fn count_object_relation(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ObjectRelationFilter,
    ) -> Result<u64, StorageError>;
}

/// Atomic relationship-tuple mutation capability.
#[async_trait]
pub trait TupleWriter: Send + Sync {
    /// Applies sorted deletes and writes with changelog rows in one transaction.
    ///
    /// # Errors
    ///
    /// Returns conflicts, resource exhaustion, cancellation, timeout, availability,
    /// integrity, or backend failures without committing a partial mutation.
    async fn write_tuples(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, StorageError>;
}

/// Immutable authorization-model read capability.
#[async_trait]
pub trait ModelReader: Send + Sync {
    /// Reads one immutable model by ID.
    ///
    /// # Errors
    ///
    /// Returns not found, cancellation, timeout, availability, or integrity failures.
    async fn read_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError>;

    /// Resolves the latest immutable model by monotonic ID.
    ///
    /// # Errors
    ///
    /// Returns not found, cancellation, timeout, availability, or integrity failures.
    async fn read_latest_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<Arc<StoredAuthorizationModel>, StorageError>;

    /// Lists immutable models newest first using a stable cursor.
    ///
    /// # Errors
    ///
    /// Returns invalid continuation, cancellation, timeout, availability, or backend failures.
    async fn list_models(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        options: &PageOptions,
    ) -> Result<Page<Arc<StoredAuthorizationModel>>, StorageError>;
}

/// Immutable authorization-model publication capability.
#[async_trait]
pub trait ModelWriter: Send + Sync {
    /// Persists an already compiled model atomically.
    ///
    /// # Errors
    ///
    /// Returns already exists, not found, cancellation, timeout, integrity, or backend failures.
    async fn write_model(
        &self,
        context: &OperationContext,
        model: Arc<StoredAuthorizationModel>,
    ) -> Result<(), StorageError>;
}

/// Store read and stable listing capability.
#[async_trait]
pub trait StoreReader: Send + Sync {
    /// Reads one active store.
    ///
    /// # Errors
    ///
    /// Returns not found, cancellation, timeout, availability, or backend failures.
    async fn read_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<StoreRecord, StorageError>;

    /// Lists active stores by ascending ID.
    ///
    /// # Errors
    ///
    /// Returns invalid continuation, cancellation, timeout, availability, or backend failures.
    async fn list_stores(
        &self,
        context: &OperationContext,
        options: &PageOptions,
    ) -> Result<Page<StoreRecord>, StorageError>;
}

/// Store creation, rename, and deletion capability.
#[async_trait]
pub trait StoreWriter: Send + Sync {
    /// Creates one store with an immutable caller-provided ID.
    ///
    /// # Errors
    ///
    /// Returns already exists, cancellation, timeout, availability, or backend failures.
    async fn create_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError>;

    /// Renames one active store.
    ///
    /// # Errors
    ///
    /// Returns not found, cancellation, timeout, availability, or backend failures.
    async fn rename_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, StorageError>;

    /// Deletes a store and all memory-backend state owned by it.
    ///
    /// # Errors
    ///
    /// Returns not found, cancellation, timeout, availability, or backend failures.
    async fn delete_store(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<(), StorageError>;
}

/// Model-scoped assertion read capability.
#[async_trait]
pub trait AssertionReader: Send + Sync {
    /// Reads assertions, returning an empty set when none were stored.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, or backend failures.
    async fn read_assertions(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<[Assertion]>, StorageError>;
}

/// Atomic model-scoped assertion replacement capability.
#[async_trait]
pub trait AssertionWriter: Send + Sync {
    /// Replaces every assertion for one immutable model.
    ///
    /// # Errors
    ///
    /// Returns not found, resource exhaustion, cancellation, timeout, or backend failures.
    async fn write_assertions(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
        assertions: Vec<Assertion>,
    ) -> Result<(), StorageError>;
}

/// Ordered tuple-changelog read capability.
#[async_trait]
pub trait ChangeReader: Send + Sync {
    /// Reads changes after an optional exclusive ID, ordered oldest first.
    ///
    /// # Errors
    ///
    /// Returns invalid continuation, cancellation, timeout, availability, or backend failures.
    async fn read_changes(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ChangeFilter,
        options: &PageOptions,
    ) -> Result<Page<TupleChange>, StorageError>;
}

/// Backend readiness capability.
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Returns safe backend readiness.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, availability, or backend failures.
    async fn health(&self, context: &OperationContext) -> Result<HealthStatus, StorageError>;
}
