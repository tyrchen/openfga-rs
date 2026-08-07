//! Transport-independent execution of generated `OpenFGA` wire requests.

use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    future::Future,
    num::NonZeroU32,
    ops::Deref,
    sync::Arc,
    time::Instant,
};

use openfga_auth::Action;
use openfga_check::{CheckError, CheckErrorKind, CheckResolution};
use openfga_domain::{
    BatchCheckCommand, BatchCheckItem, BatchCheckItems, CheckCommand, ConditionContext,
    ConsistencyPreference, CorrelationId, Deadline, ExpandCommand, ListControl, ListObjectsCommand,
    ListUsersCommand, ModelSelection, Principal, QueryContext, StoreId, TokenOperation, TypeName,
    UserTypeFilters,
};
use openfga_list::ListObjectsStream;
use openfga_proto::openfga::v1 as pb;
use openfga_service::TupleContextSizePolicy;
use openfga_storage::{
    ChangeFilter, OperationContext, PageOptions, StorageCancellationToken, StoreFilter, StoreName,
    TupleWriteOptions, WriteConflictPolicy,
};
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    ApiError, OpenFgaServices, TransportConfig,
    admission::AdmissionControl,
    convert,
    pagination::{self, GLOBAL_SCOPE_STORE},
};

const MAX_AUTHORIZATION_MODEL_BYTES: usize = 262_144;
const MAX_ASSERTION_BYTES: usize = 64_000;
const MAX_CONDITION_CONTEXT_BYTES: usize = 32_768;

tokio::task_local! {
    static REQUEST_DEADLINE: Deadline;
}

pub(crate) async fn with_request_deadline<F>(deadline: Deadline, future: F) -> F::Output
where
    F: Future,
{
    REQUEST_DEADLINE.scope(deadline, future).await
}

/// Shared `OpenFGA` application adapter used by Tonic and Axum.
#[derive(Clone)]
pub struct OpenFgaApi {
    pub(crate) services: OpenFgaServices,
    pub(crate) config: TransportConfig,
    pub(crate) admission: AdmissionControl,
    endpoint_permits: Arc<Semaphore>,
}

impl OpenFgaApi {
    /// Creates an adapter after validating finite transport policy.
    ///
    /// # Errors
    ///
    /// Returns a static configuration diagnostic when policy is inconsistent.
    pub fn new(services: OpenFgaServices, config: TransportConfig) -> Result<Self, &'static str> {
        config.validate()?;
        let endpoint_permits = Arc::new(Semaphore::new(config.maximum_concurrency));
        let admission = AdmissionControl::new(config.admission_policy)?;
        Ok(Self {
            services,
            config,
            admission,
            endpoint_permits,
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "create_store"))]
    pub(crate) async fn create_store(
        &self,
        principal: &Principal,
        request: pb::CreateStoreRequest,
    ) -> Result<pb::CreateStoreResponse, ApiError> {
        self.preauthorize(principal, Action::CreateStore, None)?;
        ApiError::validate(&request)?;
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

    #[tracing::instrument(skip_all, fields(operation = "get_store"))]
    pub(crate) async fn get_store(
        &self,
        principal: &Principal,
        request: pb::GetStoreRequest,
    ) -> Result<pb::GetStoreResponse, ApiError> {
        self.preauthorize(principal, Action::GetStore, Some(&request.store_id))?;
        ApiError::validate(&request)?;
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
        self.preauthorize(principal, Action::DeleteStore, Some(&request.store_id))?;
        ApiError::validate(&request)?;
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
        self.preauthorize(principal, Action::ListStores, None)?;
        ApiError::validate(&request)?;
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
        self.preauthorize(
            principal,
            Action::WriteAuthorizationModel,
            Some(&request.store_id),
        )?;
        ApiError::validate_write_authorization_model(&request)?;
        if request.type_definitions.len() > 100 {
            return Err(ApiError::authorization_model_type_limit());
        }
        let model_size = request.encoded_len();
        if model_size > MAX_AUTHORIZATION_MODEL_BYTES {
            return Err(ApiError::authorization_model_too_large(
                model_size,
                MAX_AUTHORIZATION_MODEL_BYTES,
            ));
        }
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
        self.preauthorize(
            principal,
            Action::ReadAuthorizationModels,
            Some(&request.store_id),
        )?;
        ApiError::validate(&request)?;
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
        self.preauthorize(
            principal,
            Action::ReadAuthorizationModels,
            Some(&request.store_id),
        )?;
        ApiError::validate(&request)?;
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
        self.preauthorize(principal, Action::WriteAssertions, Some(&request.store_id))?;
        ApiError::validate(&request)?;
        self.authorize_store(principal, Action::WriteAssertions, &request.store_id)?;
        let assertion_bytes = request
            .assertions
            .iter()
            .try_fold(0_usize, |total, assertion| {
                total.checked_add(assertion.encoded_len())
            })
            .unwrap_or(usize::MAX);
        let operation = self.operation_context(ConsistencyPreference::HigherConsistency)?;
        let store_id = convert::store_id(&request.store_id)?;
        let model = self
            .services
            .assertions
            .resolve_write_model(
                &operation,
                store_id,
                convert::model_selection(&request.authorization_model_id)?,
            )
            .await
            .map_err(ApiError::from)?;
        if assertion_bytes > MAX_ASSERTION_BYTES {
            return Err(ApiError::assertion_bytes_too_large(MAX_ASSERTION_BYTES));
        }
        let assertions = request
            .assertions
            .into_iter()
            .map(|assertion| {
                convert::domain_assertion_for_wire_semantics(
                    assertion,
                    &self.config.limits,
                    MAX_ASSERTION_BYTES,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.services
            .assertions
            .write_resolved(
                &operation,
                store_id,
                model,
                assertions,
                assertion_bytes,
                MAX_ASSERTION_BYTES,
            )
            .await
            .map_err(ApiError::from_assertion_service)?;
        Ok(pb::WriteAssertionsResponse {})
    }

    #[tracing::instrument(skip_all, fields(operation = "read_assertions"))]
    pub(crate) async fn read_assertions(
        &self,
        principal: &Principal,
        request: pb::ReadAssertionsRequest,
    ) -> Result<pb::ReadAssertionsResponse, ApiError> {
        self.preauthorize(principal, Action::ReadAssertions, Some(&request.store_id))?;
        ApiError::validate(&request)?;
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
        self.preauthorize(principal, Action::Read, Some(&request.store_id))?;
        ApiError::validate(&request)?;
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
        self.preauthorize(principal, Action::Write, Some(&request.store_id))?;
        ApiError::validate(&request)?;
        self.authorize_store(principal, Action::Write, &request.store_id)?;
        let condition_context_sizes = request
            .writes
            .as_ref()
            .map_or(&[][..], |writes| writes.tuple_keys.as_slice())
            .iter()
            .map(|tuple| {
                tuple
                    .condition
                    .as_ref()
                    .and_then(|condition| condition.context.as_ref())
                    .map_or(0, Message::encoded_len)
            })
            .collect::<Vec<_>>();
        let operation = self.operation_context(ConsistencyPreference::HigherConsistency)?;
        let resolved_model = self
            .services
            .tuples
            .resolve_write_model(
                &operation,
                convert::store_id(&request.store_id)?,
                convert::model_selection(&request.authorization_model_id)?,
            )
            .await
            .map_err(ApiError::from)?;
        let writes = request
            .writes
            .as_ref()
            .map_or(&[][..], |writes| writes.tuple_keys.as_slice())
            .iter()
            .cloned()
            .zip(&condition_context_sizes)
            .map(|(tuple, encoded_size)| {
                convert::relationship_tuple_for_write(tuple, &self.config.limits, *encoded_size)
            })
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
        let mutation = self
            .services
            .tuples
            .prepare_write(
                &resolved_model,
                deletes,
                writes,
                TupleContextSizePolicy::new(&condition_context_sizes, MAX_CONDITION_CONTEXT_BYTES),
            )
            .map_err(ApiError::from)?;
        let on_duplicate = conflict_policy(
            request
                .writes
                .as_ref()
                .map_or("", |value| value.on_duplicate.as_str()),
            "on_duplicate",
        )?;
        let on_missing = conflict_policy(
            request
                .deletes
                .as_ref()
                .map_or("", |value| value.on_missing.as_str()),
            "on_missing",
        )?;
        let options = TupleWriteOptions::new(on_missing, on_duplicate);
        self.services
            .tuples
            .apply_write(&operation, mutation, options)
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
        self.preauthorize(principal, Action::ReadChanges, Some(&request.store_id))?;
        ApiError::validate(&request)?;
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
        self.preauthorize(principal, Action::Check, Some(&request.store_id))?;
        ApiError::validate(&request)?;
        self.authorize_store(principal, Action::Check, &request.store_id)?;
        let store_id = convert::store_id(&request.store_id)?;
        let model_selection = convert::model_selection(&request.authorization_model_id)?;
        let consistency = consistency(request.consistency)?;
        let deadline = self.deadline()?;
        let cancellation = RequestCancellation::new();
        let model = self
            .services
            .checks
            .resolve_transport_model(
                store_id,
                model_selection,
                consistency,
                deadline,
                cancellation.token(),
            )
            .await
            .map_err(ApiError::from)?;
        let tuple = request
            .tuple_key
            .as_ref()
            .ok_or_else(ApiError::missing_tuple_key)?;
        let query = QueryContext::builder()
            .store_id(store_id)
            .model_selection(model_selection)
            .consistency(consistency)
            .contextual_tuples(convert::contextual_tuples_for_wire_semantics(
                request.contextual_tuples,
                &self.config.limits,
                self.config.maximum_message_bytes,
            )?)
            .condition_context(convert::condition_context_for_wire_semantics(
                request.context,
                &self.config.limits,
                self.config.maximum_message_bytes,
            )?)
            .deadline(deadline)
            .principal(principal.clone())
            .build();
        let command = CheckCommand::new(
            query,
            convert::tuple_key(
                &tuple.object,
                &tuple.relation,
                &tuple.user,
                &self.config.limits,
            )?,
        );
        let outcome = self
            .services
            .checks
            .check_resolved(&command, model, cancellation.token())
            .await
            .map_err(|error| ApiError::from_check_service(error, tuple))?;
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
        self.preauthorize(principal, Action::BatchCheck, Some(&request.store_id))?;
        ApiError::validate(&request)?;
        self.authorize_store(principal, Action::BatchCheck, &request.store_id)?;
        let store_id = convert::store_id(&request.store_id)?;
        let model_selection = convert::model_selection(&request.authorization_model_id)?;
        let consistency = consistency(request.consistency)?;
        let deadline = self.deadline()?;
        let cancellation = RequestCancellation::new();
        let model = self
            .services
            .checks
            .resolve_transport_model(
                store_id,
                model_selection,
                consistency,
                deadline,
                cancellation.token(),
            )
            .await
            .map_err(ApiError::from)?;
        let query = QueryContext::builder()
            .store_id(store_id)
            .model_selection(model_selection)
            .consistency(consistency)
            .contextual_tuples(openfga_domain::ContextualTuples::empty())
            .condition_context(openfga_domain::ConditionContext::empty())
            .deadline(deadline)
            .principal(principal.clone())
            .build();
        let (items, local_errors) = convert_batch_items(
            request.checks,
            &self.config.limits,
            self.config.maximum_message_bytes,
        )?;
        if items.is_empty() {
            drop(model);
            return Ok(pb::BatchCheckResponse {
                result: local_errors,
            });
        }
        let command = BatchCheckCommand::new(
            query,
            BatchCheckItems::new(items, &self.config.limits)
                .map_err(|_| ApiError::invalid_request())?,
        );
        let outcome = self
            .services
            .checks
            .batch_check_resolved(&command, model, cancellation.token())
            .await
            .map_err(ApiError::from)?;
        let mut result = outcome
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
        result.extend(local_errors);
        Ok(pb::BatchCheckResponse { result })
    }

    #[tracing::instrument(skip_all, fields(operation = "list_objects"))]
    pub(crate) async fn list_objects(
        &self,
        principal: &Principal,
        request: pb::ListObjectsRequest,
    ) -> Result<pb::ListObjectsResponse, ApiError> {
        self.preauthorize(principal, Action::ListObjects, Some(&request.store_id))?;
        ApiError::validate_list_objects(&request)?;
        self.authorize_store(principal, Action::ListObjects, &request.store_id)?;
        let store_id = convert::store_id(&request.store_id)?;
        let model_selection = convert::model_selection(&request.authorization_model_id)?;
        let consistency = consistency(request.consistency)?;
        let deadline = self.deadline()?;
        let cancellation = RequestCancellation::new();
        let model = self
            .services
            .list_objects
            .resolve_transport_model(
                store_id,
                model_selection,
                consistency,
                deadline,
                cancellation.token(),
            )
            .await
            .map_err(ApiError::from)?;
        let command = self.list_objects_command(
            principal,
            store_id,
            model_selection,
            consistency,
            deadline,
            &request.r#type,
            &request.relation,
            &request.user,
            request.contextual_tuples,
            request.context,
        )?;
        let outcome = self
            .services
            .list_objects
            .list_objects_resolved(&command, model, cancellation.token())
            .await
            .map_err(ApiError::from)?;
        Ok(pb::ListObjectsResponse {
            objects: outcome.objects().iter().map(ToString::to_string).collect(),
        })
    }

    #[tracing::instrument(skip_all, fields(operation = "streamed_list_objects"))]
    pub(crate) async fn streamed_list_objects(
        &self,
        principal: &Principal,
        request: pb::StreamedListObjectsRequest,
    ) -> Result<ListObjectsStream, ApiError> {
        self.preauthorize(
            principal,
            Action::StreamedListObjects,
            Some(&request.store_id),
        )?;
        ApiError::validate_streamed_list_objects(&request)?;
        self.authorize_store(principal, Action::StreamedListObjects, &request.store_id)?;
        let store_id = convert::store_id(&request.store_id)?;
        let model_selection = convert::model_selection(&request.authorization_model_id)?;
        let consistency = consistency(request.consistency)?;
        let deadline = self.deadline()?;
        let cancellation = RequestCancellation::new();
        let model = self
            .services
            .list_objects
            .resolve_transport_model(
                store_id,
                model_selection,
                consistency,
                deadline,
                cancellation.token(),
            )
            .await
            .map_err(ApiError::from)?;
        let command = self.list_objects_command(
            principal,
            store_id,
            model_selection,
            consistency,
            deadline,
            &request.r#type,
            &request.relation,
            &request.user,
            request.contextual_tuples,
            request.context,
        )?;
        let stream = self
            .services
            .list_objects
            .streamed_list_objects_resolved(&command, model, cancellation.token())
            .await
            .map_err(ApiError::from)?;
        cancellation.disarm();
        Ok(stream)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "shared generated request fields remain explicit"
    )]
    fn list_objects_command(
        &self,
        principal: &Principal,
        store_id: StoreId,
        model_selection: ModelSelection,
        consistency: ConsistencyPreference,
        deadline: Deadline,
        object_type: &str,
        relation: &str,
        user: &str,
        contextual_tuples: Option<pb::ContextualTupleKeys>,
        context: Option<pbjson_types::Struct>,
    ) -> Result<ListObjectsCommand, ApiError> {
        let query = QueryContext::builder()
            .store_id(store_id)
            .model_selection(model_selection)
            .consistency(consistency)
            .contextual_tuples(convert::contextual_tuples_for_wire_semantics(
                contextual_tuples,
                &self.config.limits,
                self.config.maximum_message_bytes,
            )?)
            .condition_context(convert::condition_context_for_wire_semantics(
                context,
                &self.config.limits,
                self.config.maximum_message_bytes,
            )?)
            .deadline(deadline)
            .principal(principal.clone())
            .build();
        let maximum_results =
            NonZeroU32::new(self.config.limits.results()).ok_or_else(ApiError::invalid_request)?;
        Ok(ListObjectsCommand::new(
            query,
            convert::type_name(object_type, &self.config.limits)?,
            convert::relation_name(relation, &self.config.limits)?,
            convert::subject_ref(user, &self.config.limits)?,
            ListControl::new(maximum_results, None, &self.config.limits)
                .map_err(|_| ApiError::invalid_request())?,
        ))
    }

    #[tracing::instrument(skip_all, fields(operation = "expand"))]
    pub(crate) async fn expand(
        &self,
        principal: &Principal,
        request: pb::ExpandRequest,
    ) -> Result<pb::ExpandResponse, ApiError> {
        self.preauthorize(principal, Action::Expand, Some(&request.store_id))?;
        ApiError::validate(&request)?;
        self.authorize_store(principal, Action::Expand, &request.store_id)?;
        let store_id = convert::store_id(&request.store_id)?;
        let model_selection = convert::model_selection(&request.authorization_model_id)?;
        let consistency = consistency(request.consistency)?;
        let deadline = self.deadline()?;
        let cancellation = RequestCancellation::new();
        let model = self
            .services
            .expand
            .resolve_transport_model(
                store_id,
                model_selection,
                consistency,
                deadline,
                cancellation.token(),
            )
            .await
            .map_err(ApiError::from)?;
        let tuple_key = request.tuple_key.ok_or_else(ApiError::missing_tuple_key)?;
        let query = QueryContext::builder()
            .store_id(store_id)
            .model_selection(model_selection)
            .consistency(consistency)
            .contextual_tuples(convert::contextual_tuples_for_wire_semantics(
                request.contextual_tuples,
                &self.config.limits,
                self.config.maximum_message_bytes,
            )?)
            .condition_context(ConditionContext::empty())
            .deadline(deadline)
            .principal(principal.clone())
            .build();
        let command = ExpandCommand::new(
            query,
            convert::object_ref_string(&tuple_key.object, &self.config.limits)?,
            convert::relation_name(&tuple_key.relation, &self.config.limits)?,
        );
        let outcome = self
            .services
            .expand
            .expand_resolved(&command, model, cancellation.token())
            .await
            .map_err(ApiError::from)?;
        let response = pb::ExpandResponse {
            tree: Some(pb::UsersetTree {
                root: Some(convert::expand_node(outcome.root())?),
            }),
        };
        if response.encoded_len() > self.config.maximum_message_bytes {
            return Err(ApiError::response_too_large());
        }
        Ok(response)
    }

    #[tracing::instrument(skip_all, fields(operation = "list_users"))]
    pub(crate) async fn list_users(
        &self,
        principal: &Principal,
        request: pb::ListUsersRequest,
    ) -> Result<pb::ListUsersResponse, ApiError> {
        self.preauthorize(principal, Action::ListUsers, Some(&request.store_id))?;
        ApiError::validate_list_users(&request)?;
        self.authorize_store(principal, Action::ListUsers, &request.store_id)?;
        let store_id = convert::store_id(&request.store_id)?;
        let model_selection = convert::model_selection(&request.authorization_model_id)?;
        let consistency = consistency(request.consistency)?;
        let deadline = self.deadline()?;
        let cancellation = RequestCancellation::new();
        let model = self
            .services
            .list_users
            .resolve_transport_model(
                store_id,
                model_selection,
                consistency,
                deadline,
                cancellation.token(),
            )
            .await
            .map_err(ApiError::from)?;
        let Some(object) = request.object else {
            return Err(ApiError::invalid_request());
        };
        let query = QueryContext::builder()
            .store_id(store_id)
            .model_selection(model_selection)
            .consistency(consistency)
            .contextual_tuples(convert::contextual_tuples_for_wire_semantics(
                Some(pb::ContextualTupleKeys {
                    tuple_keys: request.contextual_tuples,
                }),
                &self.config.limits,
                self.config.maximum_message_bytes,
            )?)
            .condition_context(convert::condition_context_for_wire_semantics(
                request.context,
                &self.config.limits,
                self.config.maximum_message_bytes,
            )?)
            .deadline(deadline)
            .principal(principal.clone())
            .build();
        let filters = request
            .user_filters
            .into_iter()
            .map(|filter| convert::user_type_filter(&filter, &self.config.limits))
            .collect::<Result<Vec<_>, _>>()?;
        let maximum_results =
            NonZeroU32::new(self.config.limits.results()).ok_or_else(ApiError::invalid_request)?;
        let command = ListUsersCommand::new(
            query,
            convert::object_ref(&object.r#type, &object.id, &self.config.limits)?,
            convert::relation_name(&request.relation, &self.config.limits)?,
            UserTypeFilters::new(filters, &self.config.limits)
                .map_err(|_| ApiError::invalid_request())?,
            ListControl::new(maximum_results, None, &self.config.limits)
                .map_err(|_| ApiError::invalid_request())?,
        );
        let outcome = self
            .services
            .list_users
            .list_users_resolved(&command, model, cancellation.token())
            .await
            .map_err(ApiError::from)?;
        Ok(pb::ListUsersResponse {
            users: outcome
                .users()
                .iter()
                .map(convert::user)
                .collect::<Result<Vec<_>, _>>()?,
        })
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

    pub(crate) fn acquire_endpoint_permit(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        Arc::clone(&self.endpoint_permits)
            .try_acquire_owned()
            .map_err(|_| ApiError::overloaded())
    }

    pub(crate) fn preauthorize(
        &self,
        principal: &Principal,
        action: Action,
        store_id: Option<&str>,
    ) -> Result<(), ApiError> {
        self.config
            .authorization_policy
            .authorize_action(principal, action)
            .map_err(|_| ApiError::permission_denied())?;
        match store_id {
            Some(store_id) => match store_id.parse::<StoreId>() {
                Ok(store_id) => self.authorize(principal, action, Some(store_id)),
                Err(_) => self
                    .config
                    .authorization_policy
                    .authorize_unparsed_store(principal, action)
                    .map_err(|_| ApiError::permission_denied()),
            },
            None => self.authorize(principal, action, None),
        }
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
        if let Ok(deadline) = REQUEST_DEADLINE.try_with(|deadline| *deadline) {
            return Ok(deadline);
        }
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

pub(crate) struct RequestCancellation {
    token: StorageCancellationToken,
    armed: bool,
}

impl RequestCancellation {
    pub(crate) fn new() -> Self {
        Self {
            token: StorageCancellationToken::new(),
            armed: true,
        }
    }

    pub(crate) fn token(&self) -> StorageCancellationToken {
        self.token.clone()
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for RequestCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

impl fmt::Debug for OpenFgaApi {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenFgaApi")
            .field("services", &self.services)
            .field("config", &self.config)
            .field("admission", &self.admission)
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

fn conflict_policy(value: &str, option: &'static str) -> Result<WriteConflictPolicy, ApiError> {
    const MAX_OPTION_BYTES: usize = 256;
    match value {
        "" | "error" => Ok(WriteConflictPolicy::Error),
        "ignore" => Ok(WriteConflictPolicy::Ignore),
        _ if value.len() > MAX_OPTION_BYTES => Err(ApiError::invalid_request()),
        _ => Err(ApiError::semantic_validation_owned(
            "validation_error",
            format!("invalid {option} option: {value}"),
        )),
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

fn batch_conversion_error(error: &ApiError) -> pb::CheckError {
    pb::CheckError {
        message: error.to_string(),
        code: Some(pb::check_error::Code::InputError(
            pb::ErrorCode::ValidationError as i32,
        )),
    }
}

fn convert_batch_items(
    checks: Vec<pb::BatchCheckItem>,
    limits: &openfga_domain::InputLimits,
    maximum_message_bytes: usize,
) -> Result<
    (
        Vec<BatchCheckItem>,
        HashMap<String, pb::BatchCheckSingleResult>,
    ),
    ApiError,
> {
    if checks.len() > limits.batch_items() {
        return Err(ApiError::invalid_request());
    }
    let mut correlation_ids = BTreeSet::new();
    let identified = checks
        .into_iter()
        .map(|item| {
            let correlation_id = item
                .correlation_id
                .parse::<CorrelationId>()
                .map_err(|_| ApiError::invalid_request())?;
            if !correlation_ids.insert(correlation_id.clone()) {
                return Err(ApiError::invalid_request());
            }
            Ok((correlation_id, item))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let mut local_errors = HashMap::new();
    let mut items = Vec::with_capacity(identified.len());
    for (correlation_id, item) in identified {
        let wire_error = batch_wire_error(&item, limits);
        let converted = item
            .tuple_key
            .ok_or_else(ApiError::missing_tuple_key)
            .and_then(|tuple| {
                Ok(BatchCheckItem::new(
                    correlation_id.clone(),
                    convert::tuple_key(&tuple.object, &tuple.relation, &tuple.user, limits)?,
                    convert::contextual_tuples_for_wire_semantics(
                        item.contextual_tuples,
                        limits,
                        maximum_message_bytes,
                    )?,
                    convert::condition_context_for_wire_semantics(
                        item.context,
                        limits,
                        maximum_message_bytes,
                    )?,
                ))
            });
        match converted {
            Ok(item) => items.push(item),
            Err(error) => {
                local_errors.insert(
                    correlation_id.to_string(),
                    pb::BatchCheckSingleResult {
                        check_result: Some(pb::batch_check_single_result::CheckResult::Error(
                            wire_error.unwrap_or_else(|| batch_conversion_error(&error)),
                        )),
                    },
                );
            }
        }
    }
    Ok((items, local_errors))
}

fn batch_wire_error(
    item: &pb::BatchCheckItem,
    limits: &openfga_domain::InputLimits,
) -> Option<pb::CheckError> {
    let tuple = item.tuple_key.as_ref()?;
    if tuple.user.parse::<openfga_domain::SubjectRef>().is_err() {
        return Some(batch_input_error(
            pb::ErrorCode::InvalidTuple,
            "invalid tuple: the 'user' field is malformed".to_owned(),
        ));
    }
    if openfga_domain::ObjectRef::parse_with_limits(&tuple.object, limits).is_err() {
        return Some(batch_input_error(
            pb::ErrorCode::ValidationError,
            "invalid relation: invalid 'object' field format".to_owned(),
        ));
    }
    if openfga_domain::RelationName::parse_with_limits(&tuple.relation, limits).is_err() {
        return Some(batch_input_error(
            pb::ErrorCode::ValidationError,
            "invalid relation: the 'relation' field is malformed".to_owned(),
        ));
    }
    for contextual in item
        .contextual_tuples
        .as_ref()
        .map_or(&[][..], |tuples| tuples.tuple_keys.as_slice())
    {
        let reason = if contextual
            .user
            .parse::<openfga_domain::SubjectRef>()
            .is_err()
        {
            Some("the 'user' field is malformed")
        } else if openfga_domain::ObjectRef::parse_with_limits(&contextual.object, limits).is_err()
        {
            Some("invalid 'object' field format")
        } else if openfga_domain::RelationName::parse_with_limits(&contextual.relation, limits)
            .is_err()
        {
            Some("the 'relation' field is malformed")
        } else {
            None
        };
        if let Some(reason) = reason {
            return Some(batch_input_error(
                pb::ErrorCode::InvalidTuple,
                format!(
                    "invalid tuple: Invalid tuple '{}#{}@{}'. Reason: {reason}",
                    contextual.object, contextual.relation, contextual.user,
                ),
            ));
        }
        if let Some(condition) = &contextual.condition
            && openfga_domain::ConditionName::parse_with_limits(&condition.name, limits).is_err()
        {
            if condition.name.chars().any(char::is_control) {
                return Some(batch_input_error(
                    pb::ErrorCode::InvalidTuple,
                    "invalid tuple: condition name contains forbidden characters".to_owned(),
                ));
            }
            return Some(batch_input_error(
                pb::ErrorCode::InvalidTuple,
                format!(
                    "invalid tuple: Invalid tuple '{}#{}@{} (condition {})'. Reason: undefined \
                     condition",
                    contextual.object, contextual.relation, contextual.user, condition.name,
                ),
            ));
        }
    }
    None
}

fn batch_input_error(code: pb::ErrorCode, message: String) -> pb::CheckError {
    pb::CheckError {
        message,
        code: Some(pb::check_error::Code::InputError(code as i32)),
    }
}
