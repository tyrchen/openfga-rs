//! Immutable authorization-model publication and read use cases.

use std::{fmt, sync::Arc};

use openfga_domain::{AuthorizationModelId, StoreId};
use openfga_model::{AuthorizationModelDefinition, ModelCompiler};
use openfga_storage::{
    ModelReader, ModelWriter, OperationContext, Page, PageOptions, StoreReader,
    StoredAuthorizationModel,
};

use crate::{IdentifierSource, ServiceClock, ServiceError};

/// Injected, deterministic dependencies for one model-publication flow.
#[derive(Clone)]
#[non_exhaustive]
pub struct ModelPublication {
    identifiers: Arc<dyn IdentifierSource>,
    clock: Arc<dyn ServiceClock>,
    compiler: ModelCompiler,
}

impl ModelPublication {
    /// Creates model-publication dependencies.
    #[must_use]
    pub const fn new(
        identifiers: Arc<dyn IdentifierSource>,
        clock: Arc<dyn ServiceClock>,
        compiler: ModelCompiler,
    ) -> Self {
        Self {
            identifiers,
            clock,
            compiler,
        }
    }
}

impl fmt::Debug for ModelPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelPublication")
            .field("identifiers", &"dyn IdentifierSource")
            .field("clock", &self.clock)
            .field("compiler", &self.compiler)
            .finish_non_exhaustive()
    }
}

/// Transport-neutral immutable authorization-model service.
#[derive(Clone)]
#[non_exhaustive]
pub struct ModelService {
    stores: Arc<dyn StoreReader>,
    reader: Arc<dyn ModelReader>,
    writer: Arc<dyn ModelWriter>,
    publication: ModelPublication,
}

impl ModelService {
    /// Creates a service from narrow storage and publication capabilities.
    #[must_use]
    pub const fn new(
        stores: Arc<dyn StoreReader>,
        reader: Arc<dyn ModelReader>,
        writer: Arc<dyn ModelWriter>,
        publication: ModelPublication,
    ) -> Self {
        Self {
            stores,
            reader,
            writer,
            publication,
        }
    }

    /// Validates the store, allocates identity, compiles, and atomically publishes a model.
    ///
    /// # Errors
    ///
    /// Returns store/model validation, identity allocation, cancellation, timeout,
    /// conflict, integrity, or backend failure. Invalid models are never persisted.
    #[tracing::instrument(skip_all, fields(operation = "write_authorization_model", store_id = %store_id))]
    pub async fn write(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        definition: AuthorizationModelDefinition,
    ) -> Result<Arc<StoredAuthorizationModel>, ServiceError> {
        crate::common::require_store(self.stores.as_ref(), context, store_id).await?;
        let model_id = self.publication.identifiers.next_model_id(context).await?;
        let source = Arc::new(definition.with_identity(store_id, model_id));
        let compiled = self.publication.compiler.compile(source.as_ref())?;
        let stored = Arc::new(StoredAuthorizationModel::new(
            source,
            compiled,
            self.publication.clock.now(),
        )?);
        context.check()?;
        self.writer
            .write_model(context, Arc::clone(&stored))
            .await
            .map_err(ServiceError::store_storage)?;
        context.check()?;
        Ok(stored)
    }

    /// Reads one immutable model by ID after validating its store.
    ///
    /// # Errors
    ///
    /// Returns store/model-not-found, cancellation, timeout, or backend failure.
    #[tracing::instrument(
        skip_all,
        fields(operation = "read_authorization_model", store_id = %store_id, model_id = %model_id)
    )]
    pub async fn read(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<StoredAuthorizationModel>, ServiceError> {
        crate::common::require_store(self.stores.as_ref(), context, store_id).await?;
        let model = self
            .reader
            .read_model(context, store_id, model_id)
            .await
            .map_err(ServiceError::model_storage)?;
        context.check()?;
        Ok(model)
    }

    /// Lists immutable models newest first after validating their store.
    ///
    /// # Errors
    ///
    /// Returns store-not-found, invalid continuation, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "read_authorization_models", store_id = %store_id))]
    pub async fn list(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        options: &PageOptions,
    ) -> Result<Page<Arc<StoredAuthorizationModel>>, ServiceError> {
        crate::common::require_store(self.stores.as_ref(), context, store_id).await?;
        let page = self
            .reader
            .list_models(context, store_id, options)
            .await
            .map_err(ServiceError::store_storage)?;
        context.check()?;
        Ok(page)
    }
}

impl fmt::Debug for ModelService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelService")
            .field("stores", &"dyn StoreReader")
            .field("reader", &"dyn ModelReader")
            .field("writer", &"dyn ModelWriter")
            .field("publication", &self.publication)
            .finish_non_exhaustive()
    }
}
