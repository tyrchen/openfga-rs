//! Store lifecycle service use cases.

use std::{fmt, sync::Arc};

use openfga_domain::StoreId;
use openfga_storage::{
    OperationContext, Page, PageOptions, StoreFilter, StoreName, StoreReader, StoreRecord,
    StoreWriter,
};

use crate::{IdentifierSource, ServiceError};

/// Transport-neutral create/get/update/delete/list store orchestration.
#[derive(Clone)]
#[non_exhaustive]
pub struct StoreService {
    reader: Arc<dyn StoreReader>,
    writer: Arc<dyn StoreWriter>,
    identifiers: Arc<dyn IdentifierSource>,
}

impl StoreService {
    /// Creates a service from narrow storage and identifier capabilities.
    #[must_use]
    pub const fn new(
        reader: Arc<dyn StoreReader>,
        writer: Arc<dyn StoreWriter>,
        identifiers: Arc<dyn IdentifierSource>,
    ) -> Self {
        Self {
            reader,
            writer,
            identifiers,
        }
    }

    /// Allocates and creates one store.
    ///
    /// # Errors
    ///
    /// Returns allocation, duplicate identity, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "create_store"))]
    pub async fn create(
        &self,
        context: &OperationContext,
        name: StoreName,
    ) -> Result<StoreRecord, ServiceError> {
        context.check()?;
        let store_id = self.identifiers.next_store_id(context).await?;
        let record = self.writer.create_store(context, store_id, name).await?;
        context.check()?;
        Ok(record)
    }

    /// Gets one active store.
    ///
    /// # Errors
    ///
    /// Returns store-not-found, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "get_store", store_id = %store_id))]
    pub async fn get(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<StoreRecord, ServiceError> {
        crate::common::require_store(self.reader.as_ref(), context, store_id).await
    }

    /// Renames one active store.
    ///
    /// # Errors
    ///
    /// Returns store-not-found, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "update_store", store_id = %store_id))]
    pub async fn update(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        name: StoreName,
    ) -> Result<StoreRecord, ServiceError> {
        context.check()?;
        let record = self
            .writer
            .rename_store(context, store_id, name)
            .await
            .map_err(ServiceError::store_storage)?;
        context.check()?;
        Ok(record)
    }

    /// Idempotently removes one store record without deleting namespace data.
    ///
    /// # Errors
    ///
    /// Returns cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "delete_store", store_id = %store_id))]
    pub async fn delete(
        &self,
        context: &OperationContext,
        store_id: StoreId,
    ) -> Result<(), ServiceError> {
        context.check()?;
        self.writer
            .delete_store(context, store_id)
            .await
            .map_err(ServiceError::store_storage)?;
        context.check()?;
        Ok(())
    }

    /// Lists stores in stable ascending-ID order.
    ///
    /// # Errors
    ///
    /// Returns invalid continuation, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "list_stores"))]
    pub async fn list(
        &self,
        context: &OperationContext,
        filter: &StoreFilter,
        options: &PageOptions,
    ) -> Result<Page<StoreRecord>, ServiceError> {
        context.check()?;
        let page = self.reader.list_stores(context, filter, options).await?;
        context.check()?;
        Ok(page)
    }
}

impl fmt::Debug for StoreService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreService")
            .field("reader", &"dyn StoreReader")
            .field("writer", &"dyn StoreWriter")
            .field("identifiers", &"dyn IdentifierSource")
            .finish_non_exhaustive()
    }
}
