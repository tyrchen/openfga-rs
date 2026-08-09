//! `AuthZEN` Authorization API 1.0 mappings over the native `OpenFGA` service core.

use std::collections::HashMap;

use openfga_auth::Action as PolicyAction;
use openfga_domain::{ConsistencyPreference, Principal};
use openfga_proto::{authzen::v1 as az, openfga::v1 as pb};
use pbjson_types::{Struct, Value, value::Kind};
use tokio_stream::StreamExt;

use crate::{ApiError, OpenFgaApi, api::RequestCancellation, convert};

/// HTTP/gRPC metadata name used to pin an `AuthZEN` request to one `OpenFGA` model.
pub(crate) const AUTHORIZATION_MODEL_ID_HEADER: &str = "openfga-authorization-model-id";

impl OpenFgaApi {
    pub(crate) async fn authzen_evaluation(
        &self,
        principal: &Principal,
        request: az::EvaluationRequest,
        authorization_model_id: &str,
    ) -> Result<az::EvaluationResponse, ApiError> {
        self.ensure_authzen_enabled()?;
        ApiError::validate(&request)?;
        let check = build_check_request(
            request.store_id,
            authorization_model_id,
            request.subject.as_ref(),
            request.resource.as_ref(),
            request.action.as_ref(),
            request.context.as_ref(),
        )?;
        let response = self.check(principal, check).await?;
        Ok(az::EvaluationResponse {
            decision: response.allowed,
            context: None,
        })
    }

    pub(crate) async fn authzen_evaluations(
        &self,
        principal: &Principal,
        request: az::EvaluationsRequest,
        authorization_model_id: &str,
    ) -> Result<az::EvaluationsResponse, ApiError> {
        self.ensure_authzen_enabled()?;
        ApiError::validate(&request)?;
        if request.evaluations.is_empty() {
            let response = self
                .authzen_evaluation(
                    principal,
                    az::EvaluationRequest {
                        store_id: request.store_id,
                        subject: request.subject,
                        resource: request.resource,
                        action: request.action,
                        context: request.context,
                    },
                    authorization_model_id,
                )
                .await?;
            return Ok(az::EvaluationsResponse {
                evaluations: vec![response],
            });
        }

        let semantic = request
            .options
            .map_or(az::EvaluationsSemantic::ExecuteAll, |options| {
                az::EvaluationsSemantic::try_from(options.evaluations_semantic)
                    .unwrap_or(az::EvaluationsSemantic::ExecuteAll)
            });
        match semantic {
            az::EvaluationsSemantic::ExecuteAll => {
                self.authzen_evaluate_all(principal, request, authorization_model_id)
                    .await
            }
            az::EvaluationsSemantic::DenyOnFirstDeny
            | az::EvaluationsSemantic::PermitOnFirstPermit => {
                self.authzen_evaluate_short_circuit(
                    principal,
                    request,
                    authorization_model_id,
                    semantic,
                )
                .await
            }
        }
    }

    async fn authzen_evaluate_all(
        &self,
        principal: &Principal,
        request: az::EvaluationsRequest,
        authorization_model_id: &str,
    ) -> Result<az::EvaluationsResponse, ApiError> {
        let mut checks = Vec::with_capacity(request.evaluations.len());
        for (index, item) in request.evaluations.iter().enumerate() {
            let (subject, resource, action, context) = effective_evaluation(&request, item);
            let check = build_check_request(
                request.store_id.clone(),
                authorization_model_id,
                subject,
                resource,
                action,
                context,
            )?;
            checks.push(pb::BatchCheckItem {
                tuple_key: check.tuple_key,
                contextual_tuples: None,
                context: check.context,
                correlation_id: index.to_string(),
            });
        }
        let expected = checks.len();
        let batch = self
            .batch_check(
                principal,
                pb::BatchCheckRequest {
                    store_id: request.store_id,
                    checks,
                    authorization_model_id: authorization_model_id.to_owned(),
                    consistency: pb::ConsistencyPreference::Unspecified as i32,
                },
            )
            .await?;
        let mut evaluations = Vec::with_capacity(expected);
        for index in 0..expected {
            let response = batch.result.get(&index.to_string()).map_or_else(
                || evaluation_error_response(500, "missing batch evaluation result"),
                batch_evaluation_response,
            );
            evaluations.push(response);
        }
        Ok(az::EvaluationsResponse { evaluations })
    }

    async fn authzen_evaluate_short_circuit(
        &self,
        principal: &Principal,
        request: az::EvaluationsRequest,
        authorization_model_id: &str,
        semantic: az::EvaluationsSemantic,
    ) -> Result<az::EvaluationsResponse, ApiError> {
        let mut evaluations = Vec::with_capacity(request.evaluations.len());
        for item in &request.evaluations {
            let (subject, resource, action, context) = effective_evaluation(&request, item);
            let response = match build_check_request(
                request.store_id.clone(),
                authorization_model_id,
                subject,
                resource,
                action,
                context,
            ) {
                Ok(check) => match self.check(principal, check).await {
                    Ok(response) => az::EvaluationResponse {
                        decision: response.allowed,
                        context: None,
                    },
                    Err(error) => evaluation_error_response(
                        u32::from(error.http_status().as_u16()),
                        &error.to_string(),
                    ),
                },
                Err(error) => evaluation_error_response(
                    u32::from(error.http_status().as_u16()),
                    &error.to_string(),
                ),
            };
            let decision = response.decision;
            evaluations.push(response);
            if (semantic == az::EvaluationsSemantic::DenyOnFirstDeny && !decision)
                || (semantic == az::EvaluationsSemantic::PermitOnFirstPermit && decision)
            {
                break;
            }
        }
        Ok(az::EvaluationsResponse { evaluations })
    }

    pub(crate) async fn authzen_subject_search(
        &self,
        principal: &Principal,
        request: az::SubjectSearchRequest,
        authorization_model_id: &str,
    ) -> Result<az::SubjectSearchResponse, ApiError> {
        self.ensure_authzen_enabled()?;
        ApiError::validate(&request)?;
        let resource = request
            .resource
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let action = request
            .action
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let subject = request
            .subject
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let context = merge_properties(
            subject.properties.as_ref(),
            resource.properties.as_ref(),
            action.properties.as_ref(),
            request.context.as_ref(),
        );
        let response = self
            .list_users(
                principal,
                pb::ListUsersRequest {
                    store_id: request.store_id,
                    authorization_model_id: authorization_model_id.to_owned(),
                    object: Some(pb::Object {
                        r#type: resource.r#type.clone(),
                        id: resource.id.clone(),
                    }),
                    relation: action.name.clone(),
                    user_filters: vec![pb::UserTypeFilter {
                        r#type: subject.r#type.clone(),
                        relation: String::new(),
                    }],
                    contextual_tuples: Vec::new(),
                    context,
                    consistency: pb::ConsistencyPreference::Unspecified as i32,
                },
            )
            .await?;
        let results = response
            .users
            .into_iter()
            .filter_map(|user| match user.user {
                Some(pb::user::User::Object(object)) => Some(az::Subject {
                    r#type: object.r#type,
                    id: object.id,
                    properties: None,
                }),
                Some(pb::user::User::Wildcard(wildcard)) => Some(az::Subject {
                    r#type: wildcard.r#type,
                    id: "*".to_owned(),
                    properties: None,
                }),
                Some(pb::user::User::Userset(_)) | None => None,
            })
            .collect();
        Ok(az::SubjectSearchResponse {
            results,
            page: None,
        })
    }

    pub(crate) async fn authzen_resource_search(
        &self,
        principal: &Principal,
        request: az::ResourceSearchRequest,
        authorization_model_id: &str,
    ) -> Result<az::ResourceSearchResponse, ApiError> {
        self.ensure_authzen_enabled()?;
        ApiError::validate(&request)?;
        let subject = request
            .subject
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let action = request
            .action
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let resource = request
            .resource
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let context = merge_properties(
            subject.properties.as_ref(),
            resource.properties.as_ref(),
            action.properties.as_ref(),
            request.context.as_ref(),
        );
        let mut stream = self
            .streamed_list_objects(
                principal,
                pb::StreamedListObjectsRequest {
                    store_id: request.store_id,
                    authorization_model_id: authorization_model_id.to_owned(),
                    r#type: resource.r#type.clone(),
                    relation: action.name.clone(),
                    user: entity(&subject.r#type, &subject.id),
                    contextual_tuples: None,
                    context,
                    consistency: pb::ConsistencyPreference::Unspecified as i32,
                },
            )
            .await?;
        let mut results = Vec::new();
        while let Some(object) = stream.next().await {
            let object = object
                .map_err(|error| ApiError::from(openfga_service::ServiceError::from(error)))?;
            results.push(az::Resource {
                r#type: object.object_type().as_str().to_owned(),
                id: object.object_id().as_str().to_owned(),
                properties: None,
            });
        }
        Ok(az::ResourceSearchResponse {
            results,
            page: None,
        })
    }

    pub(crate) async fn authzen_action_search(
        &self,
        principal: &Principal,
        request: az::ActionSearchRequest,
        authorization_model_id: &str,
    ) -> Result<az::ActionSearchResponse, ApiError> {
        self.ensure_authzen_enabled()?;
        self.preauthorize(principal, PolicyAction::BatchCheck, Some(&request.store_id))?;
        ApiError::validate(&request)?;
        self.authorize_store(principal, PolicyAction::BatchCheck, &request.store_id)?;
        let subject = request
            .subject
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let resource = request
            .resource
            .as_ref()
            .ok_or_else(ApiError::invalid_request)?;
        let store_id = convert::store_id(&request.store_id)?;
        let selection = convert::model_selection(authorization_model_id)?;
        let cancellation = RequestCancellation::new();
        let model = self
            .services
            .checks
            .resolve_transport_model(
                store_id,
                selection,
                ConsistencyPreference::MinimizeLatency,
                self.deadline()?,
                cancellation.token(),
            )
            .await
            .map_err(ApiError::from)?;
        let object_type = convert::type_name(&resource.r#type, &self.config.limits)?;
        let relations = model
            .relation_names(&object_type)
            .map_err(|_| ApiError::invalid_request())?;
        let resolved_model_id = model.authorization_model_id().to_string();
        let context = merge_properties(
            subject.properties.as_ref(),
            resource.properties.as_ref(),
            None,
            request.context.as_ref(),
        );
        let checks = relations
            .iter()
            .enumerate()
            .map(|(index, relation)| pb::BatchCheckItem {
                tuple_key: Some(pb::CheckRequestTupleKey {
                    user: entity(&subject.r#type, &subject.id),
                    relation: relation.as_str().to_owned(),
                    object: entity(&resource.r#type, &resource.id),
                }),
                contextual_tuples: None,
                context: context.clone(),
                correlation_id: index.to_string(),
            })
            .collect();
        let batch = self
            .batch_check(
                principal,
                pb::BatchCheckRequest {
                    store_id: request.store_id,
                    checks,
                    authorization_model_id: resolved_model_id,
                    consistency: pb::ConsistencyPreference::Unspecified as i32,
                },
            )
            .await?;
        let mut results = batch
            .result
            .iter()
            .filter_map(|(correlation, result)| {
                let Some(pb::batch_check_single_result::CheckResult::Allowed(true)) =
                    result.check_result
                else {
                    return None;
                };
                let index = correlation.parse::<usize>().ok()?;
                relations.get(index).map(|relation| az::Action {
                    name: relation.as_str().to_owned(),
                    properties: None,
                })
            })
            .collect::<Vec<_>>();
        results.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(az::ActionSearchResponse {
            results,
            page: None,
        })
    }

    #[allow(
        clippy::unused_async,
        reason = "all transport operation adapters share one async dispatch contract"
    )]
    pub(crate) async fn authzen_configuration(
        &self,
        principal: &Principal,
        request: az::GetConfigurationRequest,
        _authorization_model_id: &str,
    ) -> Result<az::GetConfigurationResponse, ApiError> {
        self.ensure_authzen_enabled()?;
        self.preauthorize(principal, PolicyAction::GetStore, Some(&request.store_id))?;
        ApiError::validate(&request)?;
        self.authorize_store(principal, PolicyAction::GetStore, &request.store_id)?;
        let base = self
            .config
            .authzen
            .base_url()
            .ok_or_else(ApiError::authzen_discovery_unconfigured)?;
        let store_base = format!("{base}/stores/{}", request.store_id);
        Ok(az::GetConfigurationResponse {
            policy_decision_point: store_base.clone(),
            access_evaluation_endpoint: format!("{store_base}/access/v1/evaluation"),
            access_evaluations_endpoint: format!("{store_base}/access/v1/evaluations"),
            search_subject_endpoint: format!("{store_base}/access/v1/search/subject"),
            search_resource_endpoint: format!("{store_base}/access/v1/search/resource"),
            search_action_endpoint: format!("{store_base}/access/v1/search/action"),
            capabilities: Vec::new(),
            signed_metadata: None,
        })
    }

    pub(crate) fn ensure_authzen_enabled(&self) -> Result<(), ApiError> {
        if self.config.authzen.enabled() {
            Ok(())
        } else {
            Err(ApiError::authzen_disabled())
        }
    }
}

fn build_check_request(
    store_id: String,
    authorization_model_id: &str,
    subject: Option<&az::Subject>,
    resource: Option<&az::Resource>,
    action: Option<&az::Action>,
    context: Option<&Struct>,
) -> Result<pb::CheckRequest, ApiError> {
    let subject = subject.ok_or_else(ApiError::invalid_request)?;
    let resource = resource.ok_or_else(ApiError::invalid_request)?;
    let action = action.ok_or_else(ApiError::invalid_request)?;
    Ok(pb::CheckRequest {
        store_id,
        tuple_key: Some(pb::CheckRequestTupleKey {
            user: entity(&subject.r#type, &subject.id),
            relation: action.name.clone(),
            object: entity(&resource.r#type, &resource.id),
        }),
        contextual_tuples: None,
        authorization_model_id: authorization_model_id.to_owned(),
        trace: false,
        context: merge_properties(
            subject.properties.as_ref(),
            resource.properties.as_ref(),
            action.properties.as_ref(),
            context,
        ),
        consistency: pb::ConsistencyPreference::Unspecified as i32,
    })
}

fn effective_evaluation<'a>(
    request: &'a az::EvaluationsRequest,
    item: &'a az::EvaluationsItemRequest,
) -> (
    Option<&'a az::Subject>,
    Option<&'a az::Resource>,
    Option<&'a az::Action>,
    Option<&'a Struct>,
) {
    (
        item.subject.as_ref().or(request.subject.as_ref()),
        item.resource.as_ref().or(request.resource.as_ref()),
        item.action.as_ref().or(request.action.as_ref()),
        item.context.as_ref().or(request.context.as_ref()),
    )
}

fn merge_properties(
    subject: Option<&Struct>,
    resource: Option<&Struct>,
    action: Option<&Struct>,
    context: Option<&Struct>,
) -> Option<Struct> {
    let capacity = [subject, resource, action, context]
        .into_iter()
        .flatten()
        .fold(0_usize, |total, value| {
            total.saturating_add(value.fields.len())
        });
    let mut fields = HashMap::with_capacity(capacity);
    merge_prefixed(&mut fields, "subject_", subject);
    merge_prefixed(&mut fields, "resource_", resource);
    merge_prefixed(&mut fields, "action_", action);
    if let Some(context) = context {
        fields.extend(
            context
                .fields
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }
    (!fields.is_empty()).then_some(Struct { fields })
}

fn merge_prefixed(fields: &mut HashMap<String, Value>, prefix: &str, source: Option<&Struct>) {
    let Some(source) = source else {
        return;
    };
    fields.extend(
        source
            .fields
            .iter()
            .map(|(key, value)| (format!("{prefix}{key}"), value.clone())),
    );
}

fn batch_evaluation_response(result: &pb::BatchCheckSingleResult) -> az::EvaluationResponse {
    match result.check_result.as_ref() {
        Some(pb::batch_check_single_result::CheckResult::Allowed(decision)) => {
            az::EvaluationResponse {
                decision: *decision,
                context: None,
            }
        }
        Some(pb::batch_check_single_result::CheckResult::Error(error)) => {
            let status = match error.code {
                Some(pb::check_error::Code::InputError(_)) => 400,
                Some(pb::check_error::Code::InternalError(_)) | None => 500,
            };
            evaluation_error_response(status, &error.message)
        }
        None => evaluation_error_response(500, "missing batch evaluation result"),
    }
}

fn evaluation_error_response(status: u32, message: &str) -> az::EvaluationResponse {
    let error = Struct::from(HashMap::from([
        (
            "status".to_owned(),
            Value {
                kind: Some(Kind::NumberValue(f64::from(status))),
            },
        ),
        (
            "message".to_owned(),
            Value {
                kind: Some(Kind::StringValue(message.to_owned())),
            },
        ),
    ]));
    az::EvaluationResponse {
        decision: false,
        context: Some(Struct::from(HashMap::from([(
            "error".to_owned(),
            Value {
                kind: Some(Kind::StructValue(error)),
            },
        )]))),
    }
}

fn entity(entity_type: &str, id: &str) -> String {
    format!("{entity_type}:{id}")
}

/// Extracts and validates the optional model pin without retaining malformed header contents.
#[must_use]
pub(crate) fn authorization_model_id(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| {
            value.len() == 26
                && value.bytes().all(|byte| {
                    byte.is_ascii_digit()
                        || matches!(byte, b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z')
                })
        })
        .unwrap_or_default()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pbjson_types::{Struct, Value, value::Kind};

    use super::{authorization_model_id, merge_properties};

    #[test]
    fn test_should_merge_authzen_properties_with_documented_precedence() {
        let property = |value: &str| {
            Struct::from(HashMap::from([(
                "name".to_owned(),
                Value {
                    kind: Some(Kind::StringValue(value.to_owned())),
                },
            )]))
        };
        let context = property("request");
        let merged = merge_properties(
            Some(&property("subject")),
            Some(&property("resource")),
            Some(&property("action")),
            Some(&context),
        );
        assert!(merged.is_some());
        let Some(merged) = merged else {
            return;
        };
        assert_eq!(merged.fields.len(), 4);
        assert_eq!(merged.fields.get("name"), context.fields.get("name"));
    }

    #[test]
    fn test_should_ignore_invalid_authzen_model_header() {
        assert_eq!(
            authorization_model_id(Some(" 01ARZ3NDEKTSV4RRFFQ69G5FAV ")),
            "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        );
        assert!(authorization_model_id(Some("invalid-model-id")).is_empty());
    }
}
