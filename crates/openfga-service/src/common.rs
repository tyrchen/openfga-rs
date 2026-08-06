//! Shared store validation and immutable-model resolution.

use std::sync::Arc;

use openfga_domain::{ModelSelection, RelationshipTuple, StoreId};
use openfga_storage::{
    ModelReader, OperationContext, StoreReader, StoreRecord, StoredAuthorizationModel,
};

use crate::{ModelSemanticContext, ServiceError};

pub(crate) fn condition_parameter_count(
    model: &StoredAuthorizationModel,
    tuple: &RelationshipTuple,
) -> Option<usize> {
    tuple
        .condition()
        .binding()
        .and_then(|binding| model.compiled().condition_id(binding.name()).ok())
        .and_then(|id| model.compiled().condition(id).ok())
        .map(|condition| condition.parameters().len())
}

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
    let explicit_model_id = match selection {
        ModelSelection::Explicit(model_id) => Some(model_id),
        _ => None,
    };
    let model = match selection {
        ModelSelection::Explicit(model_id) => models.read_model(context, store_id, model_id).await,
        ModelSelection::Latest => models.read_latest_model(context, store_id).await,
        _ => return Err(ServiceError::unsupported_model_selection()),
    }
    .map_err(ServiceError::model_storage)
    .map_err(|error| match explicit_model_id {
        Some(model_id) => error.with_model_id(model_id),
        None => error.with_model_context(ModelSemanticContext::LatestSelection { store_id }),
    })?;
    context.check()?;
    Ok(model)
}
