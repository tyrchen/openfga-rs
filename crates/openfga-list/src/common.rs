//! Shared enumeration invariants.

use openfga_domain::{ModelSelection, QueryContext};
use openfga_model::CompiledModel;

use crate::{ListError, ListErrorKind};

pub(crate) fn validate_query_model(
    query: &QueryContext,
    model: &CompiledModel,
) -> Result<(), ListError> {
    if model.store_id() != &query.store_id() {
        return Err(ListError::new(
            ListErrorKind::InvalidModel,
            "list_model_store_mismatch",
        ));
    }
    match query.model_selection() {
        ModelSelection::Explicit(model_id) if model.model_id() != &model_id => Err(ListError::new(
            ListErrorKind::InvalidModel,
            "list_model_id_mismatch",
        )),
        ModelSelection::Explicit(_) | ModelSelection::Latest => Ok(()),
        _ => Err(ListError::new(
            ListErrorKind::InvalidModel,
            "list_model_selection_unsupported",
        )),
    }
}
