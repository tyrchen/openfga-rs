//! Model resolution and bounded `ListObjects` orchestration.

use std::{fmt, sync::Arc};

use openfga_check::DirectCheckEvaluator;
use openfga_domain::{
    ConsistencyPreference, Deadline, InputLimits, ListObjectsCommand, ModelSelection, StoreId,
};
use openfga_list::{
    DirectListObjectsEngine, ListObjectsBudget, ListObjectsEngine, ListObjectsOutcome,
    ListObjectsStream,
};
use openfga_storage::{
    ModelReader, OperationContext, StorageCancellationToken, StoredAuthorizationModel, TupleReader,
};

use crate::ServiceError;

/// An immutable model selected before hostile `ListObjects` payload conversion.
#[derive(Debug)]
pub struct ResolvedListObjectsModel(Arc<StoredAuthorizationModel>);

/// Transport-neutral unary and streaming `ListObjects` orchestration.
#[derive(Clone)]
#[non_exhaustive]
pub struct ListObjectsService {
    models: Arc<dyn ModelReader>,
    tuples: Arc<dyn TupleReader>,
    engine: Arc<dyn ListObjectsEngine>,
    budget: ListObjectsBudget,
}

impl ListObjectsService {
    /// Creates a service from narrow storage capabilities and an enumeration engine.
    #[must_use]
    pub const fn new(
        models: Arc<dyn ModelReader>,
        tuples: Arc<dyn TupleReader>,
        engine: Arc<dyn ListObjectsEngine>,
        budget: ListObjectsBudget,
    ) -> Self {
        Self {
            models,
            tuples,
            engine,
            budget,
        }
    }

    /// Creates the conservative reverse-plus-Check service.
    #[must_use]
    pub fn direct(
        models: Arc<dyn ModelReader>,
        tuples: Arc<dyn TupleReader>,
        budget: ListObjectsBudget,
        input_limits: InputLimits,
    ) -> Self {
        Self::new(
            models,
            tuples,
            Arc::new(DirectListObjectsEngine::new(
                input_limits,
                Arc::new(DirectCheckEvaluator::default()),
            )),
            budget,
        )
    }

    /// Resolves the selected model and collects one unary result.
    ///
    /// # Errors
    ///
    /// Returns model-resolution, enumeration, storage, cancellation, timeout,
    /// or finite-resource failures.
    pub async fn list_objects(
        &self,
        command: &ListObjectsCommand,
        cancellation: StorageCancellationToken,
    ) -> Result<ListObjectsOutcome, ServiceError> {
        let model = self
            .resolve_transport_model(
                command.query().store_id(),
                command.query().model_selection(),
                command.query().consistency(),
                command.query().deadline(),
                cancellation.clone(),
            )
            .await?;
        self.list_objects_resolved(command, model, cancellation)
            .await
    }

    /// Collects one unary result with a preselected immutable model.
    ///
    /// # Errors
    ///
    /// Returns enumeration, storage, cancellation, timeout, or resource failures.
    pub async fn list_objects_resolved(
        &self,
        command: &ListObjectsCommand,
        model: ResolvedListObjectsModel,
        cancellation: StorageCancellationToken,
    ) -> Result<ListObjectsOutcome, ServiceError> {
        self.engine
            .list_objects(
                command,
                Arc::clone(model.0.compiled()),
                Arc::clone(&self.tuples),
                self.budget,
                cancellation,
            )
            .await
            .map_err(Into::into)
    }

    /// Resolves the selected model and starts one backpressured stream.
    ///
    /// # Errors
    ///
    /// Returns model-resolution or candidate-discovery failures before the
    /// stream is returned. Residual failures are terminal stream items.
    pub async fn streamed_list_objects(
        &self,
        command: &ListObjectsCommand,
        cancellation: StorageCancellationToken,
    ) -> Result<ListObjectsStream, ServiceError> {
        let model = self
            .resolve_transport_model(
                command.query().store_id(),
                command.query().model_selection(),
                command.query().consistency(),
                command.query().deadline(),
                cancellation.clone(),
            )
            .await?;
        self.streamed_list_objects_resolved(command, model, cancellation)
            .await
    }

    /// Starts one backpressured stream with a preselected immutable model.
    ///
    /// # Errors
    ///
    /// Returns candidate-discovery, storage, cancellation, timeout, or resource failures.
    pub async fn streamed_list_objects_resolved(
        &self,
        command: &ListObjectsCommand,
        model: ResolvedListObjectsModel,
        cancellation: StorageCancellationToken,
    ) -> Result<ListObjectsStream, ServiceError> {
        self.engine
            .streamed_list_objects(
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
    ) -> Result<ResolvedListObjectsModel, ServiceError> {
        let context = OperationContext::new(consistency, deadline, cancellation);
        crate::common::resolve_model(self.models.as_ref(), &context, store_id, selection)
            .await
            .map(ResolvedListObjectsModel)
    }
}

impl fmt::Debug for ListObjectsService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListObjectsService")
            .field("models", &"dyn ModelReader")
            .field("tuples", &"dyn TupleReader")
            .field("engine", &"dyn ListObjectsEngine")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}
