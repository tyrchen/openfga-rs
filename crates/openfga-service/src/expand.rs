//! Model resolution and bounded diagnostic `Expand` orchestration.

use std::{fmt, sync::Arc};

use openfga_domain::{
    ConsistencyPreference, Deadline, ExpandCommand, InputLimits, ModelSelection, StoreId,
};
use openfga_list::{DirectExpandEngine, ExpandBudget, ExpandEngine, ExpandOutcome};
use openfga_storage::{
    ModelReader, OperationContext, StorageCancellationToken, StoredAuthorizationModel, TupleReader,
};

use crate::ServiceError;

/// An immutable model selected before hostile `Expand` payload conversion.
#[derive(Debug)]
pub struct ResolvedExpandModel(Arc<StoredAuthorizationModel>);

/// Transport-neutral diagnostic expansion orchestration.
#[derive(Clone)]
#[non_exhaustive]
pub struct ExpandService {
    models: Arc<dyn ModelReader>,
    tuples: Arc<dyn TupleReader>,
    engine: Arc<dyn ExpandEngine>,
    budget: ExpandBudget,
}

impl ExpandService {
    /// Creates a service from narrow storage capabilities and an expansion engine.
    #[must_use]
    pub const fn new(
        models: Arc<dyn ModelReader>,
        tuples: Arc<dyn TupleReader>,
        engine: Arc<dyn ExpandEngine>,
        budget: ExpandBudget,
    ) -> Self {
        Self {
            models,
            tuples,
            engine,
            budget,
        }
    }

    /// Creates the correctness-first diagnostic expansion service.
    #[must_use]
    pub fn direct(
        models: Arc<dyn ModelReader>,
        tuples: Arc<dyn TupleReader>,
        budget: ExpandBudget,
        input_limits: InputLimits,
    ) -> Self {
        Self::new(
            models,
            tuples,
            Arc::new(DirectExpandEngine::new(input_limits)),
            budget,
        )
    }

    /// Resolves the selected model and constructs a diagnostic userset tree.
    ///
    /// # Errors
    ///
    /// Returns model-resolution, expansion, storage, cancellation, timeout, or
    /// finite-resource failures.
    pub async fn expand(
        &self,
        command: &ExpandCommand,
        cancellation: StorageCancellationToken,
    ) -> Result<ExpandOutcome, ServiceError> {
        let model = self
            .resolve_transport_model(
                command.query().store_id(),
                command.query().model_selection(),
                command.query().consistency(),
                command.query().deadline(),
                cancellation.clone(),
            )
            .await?;
        self.expand_resolved(command, model, cancellation).await
    }

    /// Constructs a diagnostic userset tree with a preselected immutable model.
    ///
    /// # Errors
    ///
    /// Returns expansion, storage, cancellation, timeout, or finite-resource
    /// failures.
    pub async fn expand_resolved(
        &self,
        command: &ExpandCommand,
        model: ResolvedExpandModel,
        cancellation: StorageCancellationToken,
    ) -> Result<ExpandOutcome, ServiceError> {
        self.engine
            .expand(
                command,
                Arc::clone(model.0.compiled()),
                Arc::clone(&self.tuples),
                self.budget,
                cancellation,
            )
            .await
            .map_err(Into::into)
    }

    /// Resolves an explicit or latest model before wire payload conversion.
    ///
    /// # Errors
    ///
    /// Returns model-not-found, cancellation, timeout, or backend failures.
    pub async fn resolve_transport_model(
        &self,
        store_id: StoreId,
        selection: ModelSelection,
        consistency: ConsistencyPreference,
        deadline: Deadline,
        cancellation: StorageCancellationToken,
    ) -> Result<ResolvedExpandModel, ServiceError> {
        let context = OperationContext::new(consistency, deadline, cancellation);
        crate::common::resolve_model(self.models.as_ref(), &context, store_id, selection)
            .await
            .map(ResolvedExpandModel)
    }
}

impl fmt::Debug for ExpandService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExpandService")
            .field("models", &"dyn ModelReader")
            .field("tuples", &"dyn TupleReader")
            .field("engine", &"dyn ExpandEngine")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}
