//! Model resolution and evaluator orchestration for Check operations.

use std::{fmt, sync::Arc};

use openfga_check::{
    BatchCheckOutcome, CheckBudget, CheckEvaluator, CheckOutcome, DirectCheckEvaluator,
};
use openfga_domain::{BatchCheckCommand, CheckCommand, ModelSelection, QueryContext};
use openfga_storage::{
    ModelReader, OperationContext, StorageCancellationToken, StoredAuthorizationModel, TupleReader,
};

use crate::ServiceError;

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
        let outcome = self
            .evaluator
            .check(
                command,
                Arc::clone(model.compiled()),
                Arc::clone(&self.tuples),
                self.budget,
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
        let outcome = self
            .evaluator
            .batch_check(
                command,
                Arc::clone(model.compiled()),
                Arc::clone(&self.tuples),
                self.budget,
                cancellation,
            )
            .await
            .map_err(Into::into);
        drop(model);
        outcome
    }

    async fn resolve_model(
        &self,
        query: &QueryContext,
        cancellation: StorageCancellationToken,
    ) -> Result<Arc<StoredAuthorizationModel>, ServiceError> {
        let context = OperationContext::new(query.consistency(), query.deadline(), cancellation);
        context.check()?;
        let model = match query.model_selection() {
            ModelSelection::Explicit(model_id) => self
                .models
                .read_model(&context, query.store_id(), model_id)
                .await
                .map_err(ServiceError::model_storage)?,
            ModelSelection::Latest => self
                .models
                .read_latest_model(&context, query.store_id())
                .await
                .map_err(ServiceError::model_storage)?,
            _ => return Err(ServiceError::unsupported_model_selection()),
        };
        context.check()?;
        Ok(model)
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
