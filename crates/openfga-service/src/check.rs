//! Model resolution and evaluator orchestration for Check operations.

use std::{fmt, sync::Arc};

use openfga_check::{
    BatchCheckOutcome, CheckBudget, CheckEvaluator, CheckOutcome, DirectCheckEvaluator,
};
use openfga_domain::{
    BatchCheckCommand, CheckCommand, ConsistencyPreference, Deadline, ModelSelection, QueryContext,
    StoreId,
};
use openfga_storage::{
    ModelReader, OperationContext, StorageCancellationToken, StoredAuthorizationModel, TupleReader,
};

use crate::ServiceError;

/// An immutable model selected before hostile Check payload conversion.
#[derive(Debug)]
pub struct ResolvedCheckModel(Arc<StoredAuthorizationModel>);

/// Transport-neutral Check and `BatchCheck` orchestration.
#[derive(Clone)]
#[non_exhaustive]
pub struct CheckService {
    models: Arc<dyn ModelReader>,
    tuples: Arc<dyn TupleReader>,
    evaluator: Arc<dyn CheckEvaluator>,
    budget: CheckBudget,
}

impl CheckService {
    /// Creates a service from narrow storage capabilities and an evaluator.
    #[must_use]
    pub const fn new(
        models: Arc<dyn ModelReader>,
        tuples: Arc<dyn TupleReader>,
        evaluator: Arc<dyn CheckEvaluator>,
        budget: CheckBudget,
    ) -> Self {
        Self {
            models,
            tuples,
            evaluator,
            budget,
        }
    }

    /// Creates the correctness-first service with default evaluator limits.
    #[must_use]
    pub fn direct(
        models: Arc<dyn ModelReader>,
        tuples: Arc<dyn TupleReader>,
        budget: CheckBudget,
    ) -> Self {
        Self::new(
            models,
            tuples,
            Arc::new(DirectCheckEvaluator::default()),
            budget,
        )
    }

    /// Resolves the selected model once and evaluates one authorization check.
    ///
    /// # Errors
    ///
    /// Returns typed model-resolution, evaluator, cancellation, timeout, or
    /// finite-resource failures.
    #[tracing::instrument(
        skip_all,
        fields(
            operation = "check",
            store_id = %command.query().store_id(),
            model_selection = ?command.query().model_selection(),
        )
    )]
    pub async fn check(
        &self,
        command: &CheckCommand,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, ServiceError> {
        let model = self
            .resolve_model(command.query(), cancellation.clone())
            .await?;
        self.check_resolved(command, model, cancellation).await
    }

    /// Evaluates one already-converted command with its preselected immutable model.
    ///
    /// # Errors
    ///
    /// Returns typed evaluator, cancellation, timeout, or finite-resource failures.
    pub async fn check_resolved(
        &self,
        command: &CheckCommand,
        model: ResolvedCheckModel,
        cancellation: StorageCancellationToken,
    ) -> Result<CheckOutcome, ServiceError> {
        let outcome = self
            .evaluator
            .check(
                command,
                Arc::clone(model.0.compiled()),
                Arc::clone(&self.tuples),
                self.budget,
                None,
                cancellation,
            )
            .await
            .map_err(Into::into);
        drop(model);
        outcome
    }

    /// Resolves the selected model once and evaluates a bounded Check batch.
    ///
    /// # Errors
    ///
    /// Returns typed model-resolution, evaluator, root cancellation, timeout,
    /// or finite-resource failures. Item-local failures remain in the outcome.
    #[tracing::instrument(
        skip_all,
        fields(
            operation = "batch_check",
            store_id = %command.query().store_id(),
            model_selection = ?command.query().model_selection(),
            item_count = command.items().as_slice().len(),
        )
    )]
    pub async fn batch_check(
        &self,
        command: &BatchCheckCommand,
        cancellation: StorageCancellationToken,
    ) -> Result<BatchCheckOutcome, ServiceError> {
        let model = self
            .resolve_model(command.query(), cancellation.clone())
            .await?;
        self.batch_check_resolved(command, model, cancellation)
            .await
    }

    /// Evaluates one converted batch with its preselected immutable model.
    ///
    /// # Errors
    ///
    /// Returns typed evaluator, root cancellation, timeout, or finite-resource failures.
    pub async fn batch_check_resolved(
        &self,
        command: &BatchCheckCommand,
        model: ResolvedCheckModel,
        cancellation: StorageCancellationToken,
    ) -> Result<BatchCheckOutcome, ServiceError> {
        let outcome = self
            .evaluator
            .batch_check(
                command,
                Arc::clone(model.0.compiled()),
                Arc::clone(&self.tuples),
                self.budget,
                cancellation,
            )
            .await
            .map_err(Into::into);
        drop(model);
        outcome
    }

    /// Resolves an explicit or latest model before Check payload conversion.
    ///
    /// # Errors
    ///
    /// Returns typed model-resolution, cancellation, timeout, or backend failures.
    pub async fn resolve_transport_model(
        &self,
        store_id: StoreId,
        selection: ModelSelection,
        consistency: ConsistencyPreference,
        deadline: Deadline,
        cancellation: StorageCancellationToken,
    ) -> Result<ResolvedCheckModel, ServiceError> {
        let context = OperationContext::new(consistency, deadline, cancellation);
        crate::common::resolve_model(self.models.as_ref(), &context, store_id, selection)
            .await
            .map(ResolvedCheckModel)
    }

    async fn resolve_model(
        &self,
        query: &QueryContext,
        cancellation: StorageCancellationToken,
    ) -> Result<ResolvedCheckModel, ServiceError> {
        self.resolve_transport_model(
            query.store_id(),
            query.model_selection(),
            query.consistency(),
            query.deadline(),
            cancellation,
        )
        .await
    }
}

impl fmt::Debug for CheckService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckService")
            .field("models", &"dyn ModelReader")
            .field("tuples", &"dyn TupleReader")
            .field("evaluator", &"dyn CheckEvaluator")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}
