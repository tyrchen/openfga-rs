//! Model resolution and bounded `ListUsers` orchestration.

use std::{fmt, sync::Arc};

use openfga_domain::{
    ConsistencyPreference, Deadline, InputLimits, ListUsersCommand, ModelSelection, StoreId,
};
use openfga_list::{DirectListUsersEngine, ListUsersBudget, ListUsersEngine, ListUsersOutcome};
use openfga_storage::{
    ModelReader, OperationContext, StorageCancellationToken, StoredAuthorizationModel, TupleReader,
};

use crate::ServiceError;

/// An immutable model selected before hostile `ListUsers` payload conversion.
#[derive(Debug)]
pub struct ResolvedListUsersModel(Arc<StoredAuthorizationModel>);

/// Transport-neutral `ListUsers` orchestration.
#[derive(Clone)]
#[non_exhaustive]
pub struct ListUsersService {
    models: Arc<dyn ModelReader>,
    tuples: Arc<dyn TupleReader>,
    engine: Arc<dyn ListUsersEngine>,
    budget: ListUsersBudget,
}

impl ListUsersService {
    /// Creates a service from narrow storage capabilities and an enumeration engine.
    #[must_use]
    pub const fn new(
        models: Arc<dyn ModelReader>,
        tuples: Arc<dyn TupleReader>,
        engine: Arc<dyn ListUsersEngine>,
        budget: ListUsersBudget,
    ) -> Self {
        Self {
            models,
            tuples,
            engine,
            budget,
        }
    }

    /// Creates the correctness-first forward expansion service.
    #[must_use]
    pub fn direct(
        models: Arc<dyn ModelReader>,
        tuples: Arc<dyn TupleReader>,
        budget: ListUsersBudget,
        input_limits: InputLimits,
    ) -> Self {
        Self::new(
            models,
            tuples,
            Arc::new(DirectListUsersEngine::new(input_limits)),
            budget,
        )
    }

    /// Resolves the selected model and expands filtered subjects.
    ///
    /// # Errors
    ///
    /// Returns model-resolution, enumeration, storage, cancellation, timeout,
    /// condition, or finite-resource failures.
    pub async fn list_users(
        &self,
        command: &ListUsersCommand,
        cancellation: StorageCancellationToken,
    ) -> Result<ListUsersOutcome, ServiceError> {
        let model = self
            .resolve_transport_model(
                command.query().store_id(),
                command.query().model_selection(),
                command.query().consistency(),
                command.query().deadline(),
                cancellation.clone(),
            )
            .await?;
        self.list_users_resolved(command, model, cancellation).await
    }

    /// Expands filtered subjects with a preselected immutable model.
    ///
    /// # Errors
    ///
    /// Returns enumeration, storage, cancellation, timeout, condition, or
    /// finite-resource failures.
    pub async fn list_users_resolved(
        &self,
        command: &ListUsersCommand,
        model: ResolvedListUsersModel,
        cancellation: StorageCancellationToken,
    ) -> Result<ListUsersOutcome, ServiceError> {
        self.engine
            .list_users(
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
    ) -> Result<ResolvedListUsersModel, ServiceError> {
        let context = OperationContext::new(consistency, deadline, cancellation);
        crate::common::resolve_model(self.models.as_ref(), &context, store_id, selection)
            .await
            .map(ResolvedListUsersModel)
    }
}

impl fmt::Debug for ListUsersService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListUsersService")
            .field("models", &"dyn ModelReader")
            .field("tuples", &"dyn TupleReader")
            .field("engine", &"dyn ListUsersEngine")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}
