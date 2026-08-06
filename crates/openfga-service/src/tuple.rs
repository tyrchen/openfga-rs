//! Relationship-tuple read and atomic mutation use cases.

use std::{collections::BTreeSet, fmt, sync::Arc};

use openfga_domain::{InputLimits, ModelSelection, RelationshipTuple, StoreId, TupleKey};
use openfga_storage::{
    ModelReader, MutationOutcome, OperationContext, Page, PageOptions, StoreReader,
    StoredAuthorizationModel, StoredTuple, TupleReadFilter, TupleReader, TupleWriteOptions,
    TupleWriter,
};

use crate::ServiceError;

/// A semantically validated tuple mutation awaiting conflict-policy selection.
#[derive(Debug)]
pub struct ValidatedTupleWrite {
    store_id: StoreId,
    deletes: Vec<TupleKey>,
    writes: Vec<RelationshipTuple>,
}

/// An immutable model selected before hostile tuple payload conversion.
#[derive(Debug)]
pub struct ResolvedTupleWriteModel {
    store_id: StoreId,
    model: Arc<StoredAuthorizationModel>,
}

/// Raw protobuf condition-context sizes and their finite wire limit.
#[derive(Clone, Copy, Debug)]
pub struct TupleContextSizePolicy<'a> {
    sizes: &'a [usize],
    limit: usize,
}

impl<'a> TupleContextSizePolicy<'a> {
    /// Creates a policy for sizes in matching tuple-write request order.
    #[must_use]
    pub const fn new(sizes: &'a [usize], limit: usize) -> Self {
        Self { sizes, limit }
    }
}

/// Transport-neutral paginated tuple read and atomic write service.
#[derive(Clone)]
#[non_exhaustive]
pub struct TupleService {
    models: Arc<dyn ModelReader>,
    reader: Arc<dyn TupleReader>,
    writer: Arc<dyn TupleWriter>,
    limits: InputLimits,
}

impl TupleService {
    /// Creates a service with explicit semantic and finite-input capabilities.
    #[must_use]
    pub fn new(
        _stores: Arc<dyn StoreReader>,
        models: Arc<dyn ModelReader>,
        reader: Arc<dyn TupleReader>,
        writer: Arc<dyn TupleWriter>,
        limits: InputLimits,
    ) -> Self {
        Self {
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
        let encoded_sizes = vec![0; writes.len()];
        let model = self
            .resolve_write_model(context, store_id, selection)
            .await?;
        let validated = self.prepare_write(
            &model,
            deletes,
            writes,
            TupleContextSizePolicy::new(&encoded_sizes, usize::MAX),
        )?;
        let outcome = self.apply_write(context, validated, options).await;
        drop(model);
        outcome
    }

    /// Resolves an explicit or latest model before tuple payload conversion.
    ///
    /// # Errors
    ///
    /// Returns model-not-found, cancellation, timeout, or backend failures.
    pub async fn resolve_write_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        selection: ModelSelection,
    ) -> Result<ResolvedTupleWriteModel, ServiceError> {
        let model =
            crate::common::resolve_model(self.models.as_ref(), context, store_id, selection)
                .await?;
        Ok(ResolvedTupleWriteModel { store_id, model })
    }

    /// Validates tuple semantics and finite limits before conflict options are parsed.
    ///
    /// `context_sizes` must contain the protobuf encoded size of each write tuple's condition
    /// context in matching request order.
    ///
    /// # Errors
    ///
    /// Returns model-not-found, invalid tuple, duplicate tuple, or resource-limit failures.
    #[tracing::instrument(
        skip_all,
        fields(operation = "prepare_write_tuples", store_id = %resolved.store_id)
    )]
    pub fn prepare_write(
        &self,
        resolved: &ResolvedTupleWriteModel,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        context_sizes: TupleContextSizePolicy<'_>,
    ) -> Result<ValidatedTupleWrite, ServiceError> {
        if deletes.is_empty() && writes.is_empty() {
            return Err(ServiceError::invalid_request("tuple_write_empty"));
        }
        if context_sizes.sizes.len() != writes.len() {
            return Err(ServiceError::invalid_request(
                "condition_context_size_count_mismatch",
            ));
        }
        for (tuple, encoded_size) in writes.iter().zip(context_sizes.sizes) {
            let condition_parameter_count =
                crate::common::condition_parameter_count(resolved.model.as_ref(), tuple);
            resolved
                .model
                .compiled()
                .validate_persistent_tuple(tuple)
                .map_err(ServiceError::from)
                .map_err(|error| {
                    error
                        .with_relationship_tuple(tuple)
                        .with_condition_parameter_count(condition_parameter_count)
                })?;
            if *encoded_size > context_sizes.limit {
                return Err(ServiceError::condition_context_size(
                    tuple,
                    *encoded_size,
                    context_sizes.limit,
                ));
            }
        }
        if let Some(error) = validate_mutation_shape(&deletes, &writes, &self.limits) {
            return Err(error);
        }
        Ok(ValidatedTupleWrite {
            store_id: resolved.store_id,
            deletes,
            writes,
        })
    }

    /// Atomically applies an already validated mutation using selected conflict policies.
    ///
    /// # Errors
    ///
    /// Returns conflict, cancellation, timeout, resource, or backend failures.
    #[tracing::instrument(skip_all, fields(operation = "apply_write_tuples"))]
    pub async fn apply_write(
        &self,
        context: &OperationContext,
        mutation: ValidatedTupleWrite,
        options: TupleWriteOptions,
    ) -> Result<MutationOutcome, ServiceError> {
        context.check()?;
        let outcome = self
            .writer
            .write_tuples(
                context,
                mutation.store_id,
                mutation.deletes,
                mutation.writes,
                options,
            )
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
) -> Option<ServiceError> {
    let mut keys = BTreeSet::new();
    for tuple in deletes
        .iter()
        .chain(writes.iter().map(RelationshipTuple::key))
    {
        if !keys.insert(tuple) {
            return Some(
                ServiceError::invalid_request("duplicate_tuple_in_write").with_tuple(tuple.clone()),
            );
        }
    }
    let Some(total) = deletes.len().checked_add(writes.len()) else {
        return Some(ServiceError::tuple_write_limit(limits.write_tuples()));
    };
    if total > limits.write_tuples() {
        return Some(ServiceError::tuple_write_limit(limits.write_tuples()));
    }
    None
}
