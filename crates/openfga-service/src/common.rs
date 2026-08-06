//! Shared store validation and immutable-model resolution.

use std::sync::Arc;

use openfga_domain::{ModelSelection, StoreId};
use openfga_storage::{
    ModelReader, OperationContext, StoreReader, StoreRecord, StoredAuthorizationModel,
};

use crate::ServiceError;

pub(crate) async fn require_store(
    stores: &dyn StoreReader,
    context: &OperationContext,
    store_id: StoreId,
) -> Result<StoreRecord, ServiceError> {
    context.check()?;
    let store = stores
        .read_store(context, store_id)
        .await
        .map_err(ServiceError::store_storage)?;
    context.check()?;
    Ok(store)
}

pub(crate) async fn resolve_model(
    models: &dyn ModelReader,
    context: &OperationContext,
    store_id: StoreId,
    selection: ModelSelection,
) -> Result<Arc<StoredAuthorizationModel>, ServiceError> {
    context.check()?;
    let model = match selection {
        ModelSelection::Explicit(model_id) => models.read_model(context, store_id, model_id).await,
        ModelSelection::Latest => models.read_latest_model(context, store_id).await,
        _ => return Err(ServiceError::unsupported_model_selection()),
    }
    .map_err(ServiceError::model_storage)?;
    context.check()?;
    Ok(model)
}
