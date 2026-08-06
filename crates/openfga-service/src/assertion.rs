//! Model-scoped assertion read and replacement use cases.

use std::{fmt, sync::Arc};

use openfga_domain::{AuthorizationModelId, InputLimits, ModelSelection, StoreId};
use openfga_storage::{
    Assertion, AssertionReader, AssertionWriter, ModelReader, OperationContext, StoreReader,
    StoredAuthorizationModel,
};

use crate::ServiceError;

/// Assertions resolved against one exact immutable model.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct AssertionSet {
    model_id: AuthorizationModelId,
    assertions: Arc<[Assertion]>,
}

/// An immutable authorization model selected before assertion payload conversion.
#[derive(Clone, Debug)]
pub struct ResolvedAssertionModel(Arc<StoredAuthorizationModel>);

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
    models: Arc<dyn ModelReader>,
    reader: Arc<dyn AssertionReader>,
    writer: Arc<dyn AssertionWriter>,
    limits: InputLimits,
}

impl AssertionService {
    /// Creates a service with an explicit boundary limit policy.
    #[must_use]
    pub fn new(
        _stores: Arc<dyn StoreReader>,
        models: Arc<dyn ModelReader>,
        reader: Arc<dyn AssertionReader>,
        writer: Arc<dyn AssertionWriter>,
        limits: InputLimits,
    ) -> Self {
        Self {
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
        encoded_size: usize,
        encoded_size_limit: usize,
    ) -> Result<AuthorizationModelId, ServiceError> {
        let model = self
            .resolve_write_model(context, store_id, selection)
            .await?;
        self.write_resolved(
            context,
            store_id,
            model,
            assertions,
            encoded_size,
            encoded_size_limit,
        )
        .await
    }

    /// Selects the immutable model before assertion payload conversion.
    ///
    /// # Errors
    ///
    /// Returns model-not-found, cancellation, timeout, or backend failure.
    pub async fn resolve_write_model(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        selection: ModelSelection,
    ) -> Result<ResolvedAssertionModel, ServiceError> {
        crate::common::resolve_model(self.models.as_ref(), context, store_id, selection)
            .await
            .map(ResolvedAssertionModel)
    }

    /// Validates and atomically replaces assertions using a preselected model.
    ///
    /// # Errors
    ///
    /// Returns invalid tuple, resource, cancellation, timeout, or backend failure.
    pub async fn write_resolved(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model: ResolvedAssertionModel,
        assertions: Vec<Assertion>,
        encoded_size: usize,
        encoded_size_limit: usize,
    ) -> Result<AuthorizationModelId, ServiceError> {
        let model = model.0;
        if encoded_size > encoded_size_limit {
            return Err(ServiceError::resource_exhausted_with_limit(
                "assertion_byte_limit",
                encoded_size_limit,
            ));
        }
        if assertions.len() > self.limits.assertions() {
            return Err(ServiceError::resource_exhausted("assertion_item_limit"));
        }
        for assertion in &assertions {
            model
                .compiled()
                .validate_query_tuple(assertion.tuple())
                .map_err(|error| ServiceError::assertion_tuple(error, assertion.tuple().clone()))?;
            for tuple in assertion.contextual_tuples().as_slice() {
                let condition_parameter_count =
                    crate::common::condition_parameter_count(model.as_ref(), tuple);
                model
                    .compiled()
                    .validate_relationship_tuple(tuple)
                    .map_err(ServiceError::from)
                    .map_err(|error| {
                        error
                            .with_relationship_tuple(tuple)
                            .with_condition_parameter_count(condition_parameter_count)
                    })?;
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
            .field("models", &"dyn ModelReader")
            .field("reader", &"dyn AssertionReader")
            .field("writer", &"dyn AssertionWriter")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
