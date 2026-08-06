//! Transport-independent execution of generated `OpenFGA` wire requests.

use std::{collections::HashMap, fmt, ops::Deref, time::Instant};

use openfga_auth::Action;
use openfga_check::{CheckError, CheckErrorKind, CheckResolution};
use openfga_domain::{
    BatchCheckCommand, BatchCheckItem, BatchCheckItems, CheckCommand, ConsistencyPreference,
    CorrelationId, Deadline, Principal, QueryContext, StoreId, TokenOperation, TypeName,
};
use openfga_proto::openfga::v1 as pb;
use openfga_storage::{
    ChangeFilter, OperationContext, PageOptions, StorageCancellationToken, StoreFilter, StoreName,
    TupleWriteOptions, WriteConflictPolicy,
};

use crate::{
    ApiError, OpenFgaServices, TransportConfig, convert,
    pagination::{self, GLOBAL_SCOPE_STORE},
};

/// Shared `OpenFGA` application adapter used by Tonic and Axum.
#[derive(Clone)]
pub struct OpenFgaApi {
    pub(crate) services: OpenFgaServices,
    pub(crate) config: TransportConfig,
}

impl OpenFgaApi {
    /// Creates an adapter after validating finite transport policy.
    ///
    /// # Errors
    ///
    /// Returns a static configuration diagnostic when policy is inconsistent.
    pub fn new(services: OpenFgaServices, config: TransportConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self { services, config })
    }

    #[tracing::instrument(skip_all, fields(operation = "create_store"))]
    pub(crate) async fn create_store(
        &self,
        principal: &Principal,
        request: pb::CreateStoreRequest,
    ) -> Result<pb::CreateStoreResponse, ApiError> {
        self.authorize_system(principal, Action::CreateStore)?;
        let record = self
            .services
            .stores
            .create(
                &*self.operation_context(ConsistencyPreference::HigherConsistency)?,
                StoreName::new(request.name).map_err(|_| ApiError::invalid_request())?,
            )
            .await
            .map_err(ApiError::from)?;
        convert::create_store_response(&record)
    }

    #[tracing::instrument(skip_all, fields(operation = "update_store"))]
    pub(crate) async fn update_store(
        &self,
        principal: &Principal,
        request: pb::UpdateStoreRequest,
    ) -> Result<pb::UpdateStoreResponse, ApiError> {
        self.authorize_store(principal, Action::UpdateStore, &request.store_id)?;
        let record = self
            .services
            .stores
            .update(
                &*self.operation_context(ConsistencyPreference::HigherConsistency)?,
                convert::store_id(&request.store_id)?,
                StoreName::new(request.name).map_err(|_| ApiError::invalid_request())?,
            )
            .await
            .map_err(ApiError::from)?;
        convert::update_store_response(&record)
    }

    #[tracing::instrument(skip_all, fields(operation = "get_store"))]
    pub(crate) async fn get_store(
        &self,
        principal: &Principal,
        request: pb::GetStoreRequest,
    ) -> Result<pb::GetStoreResponse, ApiError> {
        self.authorize_store(principal, Action::GetStore, &request.store_id)?;
        let record = self
            .services
            .stores
            .get(
                &*self.operation_context(ConsistencyPreference::MinimizeLatency)?,
                convert::store_id(&request.store_id)?,
            )
            .await
            .map_err(ApiError::from)?;
        convert::get_store_response(&record)
    }

    #[tracing::instrument(skip_all, fields(operation = "delete_store"))]
    pub(crate) async fn delete_store(
        &self,
        principal: &Principal,
        request: pb::DeleteStoreRequest,
    ) -> Result<pb::DeleteStoreResponse, ApiError> {
        self.authorize_store(principal, Action::DeleteStore, &request.store_id)?;
        self.services
            .stores
            .delete(
                &*self.operation_context(ConsistencyPreference::HigherConsistency)?,
                convert::store_id(&request.store_id)?,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::DeleteStoreResponse {})
    }

    #[tracing::instrument(skip_all, fields(operation = "list_stores"))]
    pub(crate) async fn list_stores(
        &self,
        principal: &Principal,
        request: pb::ListStoresRequest,
    ) -> Result<pb::ListStoresResponse, ApiError> {
        self.authorize_system(principal, Action::ListStores)?;
        let filter = if request.name.is_empty() {
            StoreFilter::all()
        } else {
            StoreFilter::named(
                StoreName::new(request.name.clone()).map_err(|_| ApiError::invalid_request())?,
            )
        };
        let scope = pagination::scope(
            TokenOperation::ListStores,
            GLOBAL_SCOPE_STORE,
            convert::store_filter_fingerprint(
                (!request.name.is_empty()).then_some(request.name.as_str()),
            ),
        );
        let options = self.page_options(
            request.page_size.map(|value| value.value),
            &request.continuation_token,
            &scope,
        )?;
        let page = self
            .services
            .stores
            .list(
                &*self.operation_context(ConsistencyPreference::MinimizeLatency)?,
                &filter,
                &options,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::ListStoresResponse {
            stores: page
                .items()
                .iter()
                .map(convert::store)
                .collect::<Result<_, _>>()?,
            continuation_token: self.continuation(page.continuation(), &scope)?,
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "write_authorization_model"))]
    pub(crate) async fn write_authorization_model(
        &self,
        principal: &Principal,
        request: pb::WriteAuthorizationModelRequest,
    ) -> Result<pb::WriteAuthorizationModelResponse, ApiError> {
        self.authorize_store(
            principal,
            Action::WriteAuthorizationModel,
            &request.store_id,
        )?;
        let store_id = convert::store_id(&request.store_id)?;
        let definition = convert::model_definition(&request, &self.config.limits)?;
        let model = self
            .services
            .models
            .write(
                &*self.operation_context(ConsistencyPreference::HigherConsistency)?,
                store_id,
                definition,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::WriteAuthorizationModelResponse {
            authorization_model_id: model.model_id().to_string(),
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "read_authorization_model"))]
    pub(crate) async fn read_authorization_model(
        &self,
        principal: &Principal,
        request: pb::ReadAuthorizationModelRequest,
    ) -> Result<pb::ReadAuthorizationModelResponse, ApiError> {
        self.authorize_store(
            principal,
            Action::ReadAuthorizationModels,
            &request.store_id,
        )?;
        let model = self
            .services
            .models
            .read(
                &*self.operation_context(ConsistencyPreference::MinimizeLatency)?,
                convert::store_id(&request.store_id)?,
                convert::model_id(&request.id)?,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::ReadAuthorizationModelResponse {
            authorization_model: Some(convert::authorization_model(&model)?),
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "read_authorization_models"))]
    pub(crate) async fn read_authorization_models(
        &self,
        principal: &Principal,
        request: pb::ReadAuthorizationModelsRequest,
    ) -> Result<pb::ReadAuthorizationModelsResponse, ApiError> {
        self.authorize_store(
            principal,
            Action::ReadAuthorizationModels,
            &request.store_id,
        )?;
        let store_id = convert::store_id(&request.store_id)?;
        let scope = pagination::scope(
            TokenOperation::ReadAuthorizationModels,
            store_id,
            convert::model_filter_fingerprint(),
        );
        let options = self.page_options(
            request.page_size.map(|value| value.value),
            &request.continuation_token,
            &scope,
        )?;
        let page = self
            .services
            .models
            .list(
                &*self.operation_context(ConsistencyPreference::MinimizeLatency)?,
                store_id,
                &options,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::ReadAuthorizationModelsResponse {
            authorization_models: page
                .items()
                .iter()
                .map(|model| convert::authorization_model(model))
                .collect::<Result<_, _>>()?,
            continuation_token: self.continuation(page.continuation(), &scope)?,
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "write_assertions"))]
    pub(crate) async fn write_assertions(
        &self,
        principal: &Principal,
        request: pb::WriteAssertionsRequest,
    ) -> Result<pb::WriteAssertionsResponse, ApiError> {
        self.authorize_store(principal, Action::WriteAssertions, &request.store_id)?;
        let assertions = request
            .assertions
            .into_iter()
            .map(|assertion| convert::domain_assertion(assertion, &self.config.limits))
            .collect::<Result<Vec<_>, _>>()?;
        self.services
            .assertions
            .write(
                &*self.operation_context(ConsistencyPreference::HigherConsistency)?,
                convert::store_id(&request.store_id)?,
                convert::model_selection(&request.authorization_model_id)?,
                assertions,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::WriteAssertionsResponse {})
    }

    #[tracing::instrument(skip_all, fields(operation = "read_assertions"))]
    pub(crate) async fn read_assertions(
        &self,
        principal: &Principal,
        request: pb::ReadAssertionsRequest,
    ) -> Result<pb::ReadAssertionsResponse, ApiError> {
        self.authorize_store(principal, Action::ReadAssertions, &request.store_id)?;
        let assertions = self
            .services
            .assertions
            .read(
                &*self.operation_context(ConsistencyPreference::MinimizeLatency)?,
                convert::store_id(&request.store_id)?,
                convert::model_selection(&request.authorization_model_id)?,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::ReadAssertionsResponse {
            authorization_model_id: assertions.model_id().to_string(),
            assertions: assertions
                .assertions()
                .iter()
                .map(convert::assertion)
                .collect::<Result<_, _>>()?,
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "read"))]
    pub(crate) async fn read(
        &self,
        principal: &Principal,
        request: pb::ReadRequest,
    ) -> Result<pb::ReadResponse, ApiError> {
        self.authorize_store(principal, Action::Read, &request.store_id)?;
        let store_id = convert::store_id(&request.store_id)?;
        let filter = convert::tuple_read_filter(request.tuple_key.as_ref(), &self.config.limits)?;
        let scope = pagination::scope(
            TokenOperation::ReadTuples,
            store_id,
            convert::tuple_filter_fingerprint(&filter),
        );
        let options = self.page_options(
            request.page_size.map(|value| value.value),
            &request.continuation_token,
            &scope,
        )?;
        let page = self
            .services
            .tuples
            .read(
                &*self.operation_context(consistency(request.consistency)?)?,
                store_id,
                &filter,
                &options,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::ReadResponse {
            tuples: page
                .items()
                .iter()
                .map(convert::stored_tuple)
                .collect::<Result<_, _>>()?,
            continuation_token: self.continuation(page.continuation(), &scope)?,
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "write"))]
    pub(crate) async fn write(
        &self,
        principal: &Principal,
        request: pb::WriteRequest,
    ) -> Result<pb::WriteResponse, ApiError> {
        self.authorize_store(principal, Action::Write, &request.store_id)?;
        let writes = request
            .writes
            .as_ref()
            .map_or(&[][..], |writes| writes.tuple_keys.as_slice())
            .iter()
            .cloned()
            .map(|tuple| convert::relationship_tuple(tuple, &self.config.limits))
            .collect::<Result<Vec<_>, _>>()?;
        let deletes = request
            .deletes
            .as_ref()
            .map_or(&[][..], |deletes| deletes.tuple_keys.as_slice())
            .iter()
            .map(|tuple| {
                convert::tuple_key(
                    &tuple.object,
                    &tuple.relation,
                    &tuple.user,
                    &self.config.limits,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = TupleWriteOptions::new(
            conflict_policy(
                request
                    .deletes
                    .as_ref()
                    .map_or("", |value| value.on_missing.as_str()),
            )?,
            conflict_policy(
                request
                    .writes
                    .as_ref()
                    .map_or("", |value| value.on_duplicate.as_str()),
            )?,
        );
        self.services
            .tuples
            .write(
                &*self.operation_context(ConsistencyPreference::HigherConsistency)?,
                convert::store_id(&request.store_id)?,
                convert::model_selection(&request.authorization_model_id)?,
                deletes,
                writes,
                options,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::WriteResponse {})
    }

    #[tracing::instrument(skip_all, fields(operation = "read_changes"))]
    pub(crate) async fn read_changes(
        &self,
        principal: &Principal,
        request: pb::ReadChangesRequest,
    ) -> Result<pb::ReadChangesResponse, ApiError> {
        self.authorize_store(principal, Action::ReadChanges, &request.store_id)?;
        let store_id = convert::store_id(&request.store_id)?;
        let object_type = (!request.r#type.is_empty())
            .then(|| TypeName::parse_with_limits(&request.r#type, &self.config.limits))
            .transpose()
            .map_err(|_| ApiError::invalid_request())?;
        let start_time = request
            .start_time
            .as_ref()
            .map(convert::system_time)
            .transpose()?;
        let filter = ChangeFilter::new(object_type.clone(), start_time);
        let scope = pagination::scope(
            TokenOperation::ReadChanges,
            store_id,
            convert::change_filter_fingerprint(object_type.as_ref(), start_time)?,
        );
        let options = self.page_options(
            request.page_size.map(|value| value.value),
            &request.continuation_token,
            &scope,
        )?;
        let page = self
            .services
            .changes
            .read(
                &*self.operation_context(ConsistencyPreference::MinimizeLatency)?,
                store_id,
                &filter,
                &options,
            )
            .await
            .map_err(ApiError::from)?;
        Ok(pb::ReadChangesResponse {
            changes: page
                .items()
                .iter()
                .map(convert::tuple_change)
                .collect::<Result<_, _>>()?,
            continuation_token: self.continuation(page.continuation(), &scope)?,
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "check"))]
    pub(crate) async fn check(
        &self,
        principal: &Principal,
        request: pb::CheckRequest,
    ) -> Result<pb::CheckResponse, ApiError> {
        self.authorize_store(principal, Action::Check, &request.store_id)?;
        let tuple = request
            .tuple_key
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let query = self.query_context(
            principal,
            &request.store_id,
            &request.authorization_model_id,
            request.consistency,
            convert::contextual_tuples(request.contextual_tuples, &self.config.limits)?,
            convert::condition_context(request.context, &self.config.limits)?,
        )?;
        let command = CheckCommand::new(
            query,
            convert::tuple_key(
                &tuple.object,
                &tuple.relation,
                &tuple.user,
                &self.config.limits,
            )?,
        );
        let cancellation = RequestCancellation::new();
        let outcome = self
            .services
            .checks
            .check(&command, cancellation.token())
            .await
            .map_err(ApiError::from)?;
        Ok(pb::CheckResponse {
            allowed: outcome.allowed(),
            resolution: if request.trace {
                resolution_name(outcome.resolution()).to_owned()
            } else {
                String::new()
            },
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "batch_check"))]
    pub(crate) async fn batch_check(
        &self,
        principal: &Principal,
        request: pb::BatchCheckRequest,
    ) -> Result<pb::BatchCheckResponse, ApiError> {
        self.authorize_store(principal, Action::BatchCheck, &request.store_id)?;
        let query = self.query_context(
            principal,
            &request.store_id,
            &request.authorization_model_id,
            request.consistency,
            openfga_domain::ContextualTuples::empty(),
            openfga_domain::ConditionContext::empty(),
        )?;
        let items = request
            .checks
            .into_iter()
            .map(|item| {
                let tuple = item.tuple_key.ok_or_else(ApiError::invalid_request)?;
                Ok(BatchCheckItem::new(
                    item.correlation_id
                        .parse::<CorrelationId>()
                        .map_err(|_| ApiError::invalid_request())?,
                    convert::tuple_key(
                        &tuple.object,
                        &tuple.relation,
                        &tuple.user,
                        &self.config.limits,
                    )?,
                    convert::contextual_tuples(item.contextual_tuples, &self.config.limits)?,
                    convert::condition_context(item.context, &self.config.limits)?,
                ))
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        let command = BatchCheckCommand::new(
            query,
            BatchCheckItems::new(items, &self.config.limits)
                .map_err(|_| ApiError::invalid_request())?,
        );
        let cancellation = RequestCancellation::new();
        let outcome = self
            .services
            .checks
            .batch_check(&command, cancellation.token())
            .await
            .map_err(ApiError::from)?;
        let result = outcome
            .results()
            .iter()
            .map(|item| {
                let check_result = match item.outcome() {
                    Ok(outcome) => {
                        pb::batch_check_single_result::CheckResult::Allowed(outcome.allowed())
                    }
                    Err(error) => {
                        pb::batch_check_single_result::CheckResult::Error(batch_item_error(error))
                    }
                };
                (
                    item.correlation_id().to_string(),
                    pb::BatchCheckSingleResult {
                        check_result: Some(check_result),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        Ok(pb::BatchCheckResponse { result })
    }

    fn query_context(
        &self,
        principal: &Principal,
        store_id: &str,
        model_id: &str,
        consistency_value: i32,
        contextual_tuples: openfga_domain::ContextualTuples,
        condition_context: openfga_domain::ConditionContext,
    ) -> Result<QueryContext, ApiError> {
        Ok(QueryContext::builder()
            .store_id(convert::store_id(store_id)?)
            .model_selection(convert::model_selection(model_id)?)
            .consistency(consistency(consistency_value)?)
            .contextual_tuples(contextual_tuples)
            .condition_context(condition_context)
            .deadline(self.deadline()?)
            .principal(principal.clone())
            .build())
    }

    pub(crate) fn authorize_store(
        &self,
        principal: &Principal,
        action: Action,
        store_id: &str,
    ) -> Result<(), ApiError> {
        let store_id = convert::store_id(store_id)?;
        self.authorize(principal, action, Some(store_id))
    }

    fn authorize_system(&self, principal: &Principal, action: Action) -> Result<(), ApiError> {
        self.authorize(principal, action, None)
    }

    fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        store_id: Option<StoreId>,
    ) -> Result<(), ApiError> {
        let result = self
            .config
            .authorization_policy
            .authorize(principal, action, store_id);
        if result.is_err() {
            tracing::warn!(
                principal_kind = ?principal.kind(),
                action = ?action,
                resource = if store_id.is_some() { "store" } else { "system" },
                outcome = "denied",
                "service authorization denied"
            );
        }
        result.map_err(|_| ApiError::permission_denied())
    }

    fn operation_context(
        &self,
        consistency: ConsistencyPreference,
    ) -> Result<CancellableOperationContext, ApiError> {
        let cancellation = StorageCancellationToken::new();
        Ok(CancellableOperationContext {
            context: OperationContext::new(consistency, self.deadline()?, cancellation.clone()),
            cancellation,
        })
    }

    fn deadline(&self) -> Result<Deadline, ApiError> {
        Deadline::from_timeout(Instant::now(), self.config.request_timeout)
            .map_err(|_| ApiError::invalid_request())
    }

    fn page_options(
        &self,
        size: Option<i32>,
        token: &str,
        scope: &openfga_domain::ContinuationScope,
    ) -> Result<PageOptions, ApiError> {
        pagination::page_options(
            size,
            token,
            scope,
            self.config.token_codec.as_ref(),
            &self.config.limits,
            self.config.default_page_size,
        )
    }

    fn continuation(
        &self,
        cursor: Option<&openfga_storage::StorageCursor>,
        scope: &openfga_domain::ContinuationScope,
    ) -> Result<String, ApiError> {
        pagination::continuation_token(
            cursor,
            scope,
            self.config.token_codec.as_ref(),
            self.config.token_ttl.as_secs(),
            &self.config.limits,
        )
    }
}

struct CancellableOperationContext {
    context: OperationContext,
    cancellation: StorageCancellationToken,
}

impl Deref for CancellableOperationContext {
    type Target = OperationContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

impl Drop for CancellableOperationContext {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

pub(crate) struct RequestCancellation(StorageCancellationToken);

impl RequestCancellation {
    pub(crate) fn new() -> Self {
        Self(StorageCancellationToken::new())
    }

    pub(crate) fn token(&self) -> StorageCancellationToken {
        self.0.clone()
    }
}

impl Drop for RequestCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl fmt::Debug for OpenFgaApi {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenFgaApi")
            .field("services", &self.services)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

fn consistency(value: i32) -> Result<ConsistencyPreference, ApiError> {
    match pb::ConsistencyPreference::try_from(value).map_err(|_| ApiError::invalid_request())? {
        pb::ConsistencyPreference::Unspecified | pb::ConsistencyPreference::MinimizeLatency => {
            Ok(ConsistencyPreference::MinimizeLatency)
        }
        pb::ConsistencyPreference::HigherConsistency => {
            Ok(ConsistencyPreference::HigherConsistency)
        }
    }
}

fn conflict_policy(value: &str) -> Result<WriteConflictPolicy, ApiError> {
    match value {
        "" | "error" => Ok(WriteConflictPolicy::Error),
        "ignore" => Ok(WriteConflictPolicy::Ignore),
        _ => Err(ApiError::invalid_request()),
    }
}

const fn resolution_name(value: CheckResolution) -> &'static str {
    match value {
        CheckResolution::Denied => "denied",
        CheckResolution::Unreachable => "unreachable",
        CheckResolution::Cycle => "cycle",
        CheckResolution::Direct => "direct",
        CheckResolution::Computed => "computed",
        CheckResolution::TupleToUserset => "tuple_to_userset",
        CheckResolution::Union => "union",
        CheckResolution::Intersection => "intersection",
        CheckResolution::Difference => "difference",
        _ => "unknown",
    }
}

fn batch_item_error(error: &CheckError) -> pb::CheckError {
    use pb::check_error::Code;
    let (code, message) = match error.kind() {
        CheckErrorKind::InvalidModel | CheckErrorKind::InvalidTuple | CheckErrorKind::Condition => {
            (
                Code::InputError(pb::ErrorCode::InvalidCheckInput as i32),
                "the check item is invalid",
            )
        }
        CheckErrorKind::Timeout => (
            Code::InternalError(pb::InternalErrorCode::DeadlineExceeded as i32),
            "the check item deadline elapsed",
        ),
        CheckErrorKind::StorageUnavailable => (
            Code::InternalError(pb::InternalErrorCode::Unavailable as i32),
            "the check service is unavailable",
        ),
        CheckErrorKind::DepthExceeded
        | CheckErrorKind::DispatchExceeded
        | CheckErrorKind::DatastoreQueryExceeded
        | CheckErrorKind::TupleItemExceeded
        | CheckErrorKind::ConditionCostExceeded => (
            Code::InternalError(pb::InternalErrorCode::ResourceExhausted as i32),
            "a check item resource limit was exceeded",
        ),
        CheckErrorKind::Cancelled | CheckErrorKind::Internal => (
            Code::InternalError(pb::InternalErrorCode::InternalError as i32),
            "the check item failed",
        ),
    };
    pb::CheckError {
        message: message.to_owned(),
        code: Some(code),
    }
}
