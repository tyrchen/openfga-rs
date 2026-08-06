//! Ordered tuple-changelog read use case.

use std::{fmt, sync::Arc};

use openfga_domain::StoreId;
use openfga_storage::{
    ChangeFilter, ChangeReader, OperationContext, Page, PageOptions, StoreReader, TupleChange,
};

use crate::ServiceError;

/// Transport-neutral ordered change reader.
#[derive(Clone)]
#[non_exhaustive]
pub struct ChangeService {
    stores: Arc<dyn StoreReader>,
    changes: Arc<dyn ChangeReader>,
}

impl ChangeService {
    /// Creates a service from narrow store and changelog capabilities.
    #[must_use]
    pub const fn new(stores: Arc<dyn StoreReader>, changes: Arc<dyn ChangeReader>) -> Self {
        Self { stores, changes }
    }

    /// Reads a stable oldest-first changelog page.
    ///
    /// # Errors
    ///
    /// Returns store-not-found, invalid continuation, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "read_changes", store_id = %store_id))]
    pub async fn read(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        filter: &ChangeFilter,
        options: &PageOptions,
    ) -> Result<Page<TupleChange>, ServiceError> {
        crate::common::require_store(self.stores.as_ref(), context, store_id).await?;
        let page = self
            .changes
            .read_changes(context, store_id, filter, options)
            .await
            .map_err(ServiceError::store_storage)?;
        context.check()?;
        Ok(page)
    }
}

impl fmt::Debug for ChangeService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangeService")
            .field("stores", &"dyn StoreReader")
            .field("changes", &"dyn ChangeReader")
            .finish_non_exhaustive()
    }
}
