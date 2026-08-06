//! Relationship-tuple read and atomic mutation use cases.

use std::{collections::BTreeSet, fmt, sync::Arc};

use openfga_domain::{InputLimits, ModelSelection, RelationshipTuple, StoreId, TupleKey};
use openfga_storage::{
    ModelReader, MutationOutcome, OperationContext, Page, PageOptions, StoreReader, StoredTuple,
    TupleReadFilter, TupleReader, TupleWriteOptions, TupleWriter,
};

use crate::ServiceError;

/// Transport-neutral paginated tuple read and atomic write service.
#[derive(Clone)]
#[non_exhaustive]
pub struct TupleService {
    stores: Arc<dyn StoreReader>,
    models: Arc<dyn ModelReader>,
    reader: Arc<dyn TupleReader>,
    writer: Arc<dyn TupleWriter>,
    limits: InputLimits,
}

impl TupleService {
    /// Creates a service with explicit semantic and finite-input capabilities.
    #[must_use]
    pub const fn new(
        stores: Arc<dyn StoreReader>,
        models: Arc<dyn ModelReader>,
        reader: Arc<dyn TupleReader>,
        writer: Arc<dyn TupleWriter>,
        limits: InputLimits,
    ) -> Self {
        Self {
            stores,
            models,
            reader,
            writer,
            limits,
        }
    }

    /// Reads one stable page matching a validated public tuple filter.
    ///
    /// # Errors
    ///
    /// Returns store-not-found, invalid continuation, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "read_tuples", store_id = %store_id))]
    pub async fn read(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &TupleReadFilter,
        options: &PageOptions,
    ) -> Result<Page<StoredTuple>, ServiceError> {
        crate::common::require_store(self.stores.as_ref(), context, store_id).await?;
        let page = self
            .reader
            .read_tuples(context, store_id, filter, options)
            .await
            .map_err(ServiceError::store_storage)?;
        context.check()?;
        Ok(page)
    }

    /// Semantically validates and atomically applies tuple deletes and writes.
    ///
    /// # Errors
    ///
    /// Returns invalid/duplicate/empty input, store/model-not-found, conflict,
    /// cancellation, timeout, resource, or backend failure.
    #[tracing::instrument(
        skip_all,
        fields(
            operation = "write_tuples",
            store_id = %store_id,
            delete_count = deletes.len(),
            write_count = writes.len(),
        )
    )]
    pub async fn write(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        selection: ModelSelection,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, ServiceError> {
        validate_mutation_shape(&deletes, &writes, &self.limits)?;
        crate::common::require_store(self.stores.as_ref(), context, store_id).await?;
        let model =
            crate::common::resolve_model(self.models.as_ref(), context, store_id, selection)
                .await?;
        for tuple in &writes {
            model.compiled().validate_persistent_tuple(tuple)?;
        }
        context.check()?;
        let outcome = self
            .writer
            .write_tuples(context, store_id, deletes, writes, options)
            .await
            .map_err(ServiceError::store_storage)?;
        context.check()?;
        Ok(outcome)
    }
}

impl fmt::Debug for TupleService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TupleService")
            .field("stores", &"dyn StoreReader")
            .field("models", &"dyn ModelReader")
            .field("reader", &"dyn TupleReader")
            .field("writer", &"dyn TupleWriter")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

fn validate_mutation_shape(
    deletes: &[TupleKey],
    writes: &[RelationshipTuple],
    limits: &InputLimits,
) -> Result<(), ServiceError> {
    let total = deletes
        .len()
        .checked_add(writes.len())
        .ok_or_else(|| ServiceError::resource_exhausted("tuple_write_item_limit"))?;
    if total == 0 {
        return Err(ServiceError::invalid_request("tuple_write_empty"));
    }
    if total > limits.write_tuples() {
        return Err(ServiceError::resource_exhausted("tuple_write_item_limit"));
    }
    let mut keys = BTreeSet::new();
    if deletes.iter().any(|key| !keys.insert(key))
        || writes.iter().any(|tuple| !keys.insert(tuple.key()))
    {
        return Err(ServiceError::invalid_request("duplicate_tuple_in_write"));
    }
    Ok(())
}
