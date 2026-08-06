//! Model-scoped assertion read and replacement use cases.

use std::{fmt, sync::Arc};

use openfga_domain::{AuthorizationModelId, InputLimits, ModelSelection, StoreId};
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ModelReader, OperationContext, StoreReader,
};

use crate::ServiceError;

/// Assertions resolved against one exact immutable model.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct AssertionSet {
    model_id: AuthorizationModelId,
    assertions: Arc<[Assertion]>,
}

impl AssertionSet {
    /// Creates a resolved assertion set.
    #[must_use]
    pub const fn new(model_id: AuthorizationModelId, assertions: Arc<[Assertion]>) -> Self {
        Self {
            model_id,
            assertions,
        }
    }

    /// Returns the exact model identity.
    #[must_use]
    pub const fn model_id(&self) -> AuthorizationModelId {
        self.model_id
    }

    /// Returns assertions in persisted request order.
    #[must_use]
    pub const fn assertions(&self) -> &Arc<[Assertion]> {
        &self.assertions
    }
}

/// Transport-neutral assertion service.
#[derive(Clone)]
#[non_exhaustive]
pub struct AssertionService {
    stores: Arc<dyn StoreReader>,
    models: Arc<dyn ModelReader>,
    reader: Arc<dyn AssertionReader>,
    writer: Arc<dyn AssertionWriter>,
    limits: InputLimits,
}

impl AssertionService {
    /// Creates a service with an explicit boundary limit policy.
    #[must_use]
    pub const fn new(
        stores: Arc<dyn StoreReader>,
        models: Arc<dyn ModelReader>,
        reader: Arc<dyn AssertionReader>,
        writer: Arc<dyn AssertionWriter>,
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

    /// Reads assertions for an explicit or latest model.
    ///
    /// # Errors
    ///
    /// Returns store/model-not-found, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "read_assertions", store_id = %store_id))]
    pub async fn read(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        selection: ModelSelection,
    ) -> Result<AssertionSet, ServiceError> {
        crate::common::require_store(self.stores.as_ref(), context, store_id).await?;
        let model =
            crate::common::resolve_model(self.models.as_ref(), context, store_id, selection)
                .await?;
        let model_id = *model.model_id();
        let assertions = self
            .reader
            .read_assertions(context, store_id, model_id)
            .await
            .map_err(ServiceError::model_storage)?;
        context.check()?;
        Ok(AssertionSet::new(model_id, assertions))
    }

    /// Validates and atomically replaces assertions for an explicit or latest model.
    ///
    /// # Errors
    ///
    /// Returns store/model-not-found, invalid tuple, resource, cancellation,
    /// timeout, or backend failure.
    #[tracing::instrument(
        skip_all,
        fields(operation = "write_assertions", store_id = %store_id, assertion_count = assertions.len())
    )]
    pub async fn write(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        selection: ModelSelection,
        assertions: Vec<Assertion>,
    ) -> Result<AuthorizationModelId, ServiceError> {
        if assertions.len() > self.limits.assertions() {
            return Err(ServiceError::resource_exhausted("assertion_item_limit"));
        }
        crate::common::require_store(self.stores.as_ref(), context, store_id).await?;
        let model =
            crate::common::resolve_model(self.models.as_ref(), context, store_id, selection)
                .await?;
        for assertion in &assertions {
            model.compiled().validate_query_tuple(assertion.tuple())?;
            for tuple in assertion.contextual_tuples().as_slice() {
                model.compiled().validate_relationship_tuple(tuple)?;
            }
        }
        let model_id = *model.model_id();
        context.check()?;
        self.writer
            .write_assertions(context, store_id, model_id, assertions)
            .await
            .map_err(ServiceError::model_storage)?;
        context.check()?;
        Ok(model_id)
    }
}

impl fmt::Debug for AssertionService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssertionService")
            .field("stores", &"dyn StoreReader")
            .field("models", &"dyn ModelReader")
            .field("reader", &"dyn AssertionReader")
            .field("writer", &"dyn AssertionWriter")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
