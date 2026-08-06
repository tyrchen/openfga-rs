//! Immutable authorization-model publication and read use cases.

use std::{fmt, sync::Arc};

use openfga_domain::{AuthorizationModelId, RelationName, StoreId};
use openfga_model::{
    AuthorizationModelDefinition, AuthorizationModelSource, DeclarationPath, ModelCompiler,
    ModelErrorCode, ModelErrors, RelationSource, RestrictionKindSourceRef, RewriteSource,
    RewriteSourceRef, TypeDefinitionSource,
};
use openfga_storage::{
    ModelReader, ModelWriter, OperationContext, Page, PageOptions, StoreReader,
    StoredAuthorizationModel,
};

use crate::{
    IdentifierSource, ModelRelationType, ModelSemanticContext, ModelSetOperator, ServiceClock,
    ServiceError,
};

/// Injected, deterministic dependencies for one model-publication flow.
#[derive(Clone)]
#[non_exhaustive]
pub struct ModelPublication {
    identifiers: Arc<dyn IdentifierSource>,
    clock: Arc<dyn ServiceClock>,
    compiler: ModelCompiler,
}

impl ModelPublication {
    /// Creates model-publication dependencies.
    #[must_use]
    pub const fn new(
        identifiers: Arc<dyn IdentifierSource>,
        clock: Arc<dyn ServiceClock>,
        compiler: ModelCompiler,
    ) -> Self {
        Self {
            identifiers,
            clock,
            compiler,
        }
    }
}

impl fmt::Debug for ModelPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelPublication")
            .field("identifiers", &"dyn IdentifierSource")
            .field("clock", &self.clock)
            .field("compiler", &self.compiler)
            .finish_non_exhaustive()
    }
}

/// Transport-neutral immutable authorization-model service.
#[derive(Clone)]
#[non_exhaustive]
pub struct ModelService {
    reader: Arc<dyn ModelReader>,
    writer: Arc<dyn ModelWriter>,
    publication: ModelPublication,
}

impl ModelService {
    /// Creates a service from narrow storage and publication capabilities.
    #[must_use]
    pub fn new(
        _stores: Arc<dyn StoreReader>,
        reader: Arc<dyn ModelReader>,
        writer: Arc<dyn ModelWriter>,
        publication: ModelPublication,
    ) -> Self {
        Self {
            reader,
            writer,
            publication,
        }
    }

    /// Allocates identity, compiles, and atomically publishes a model.
    ///
    /// # Errors
    ///
    /// Returns store/model validation, identity allocation, cancellation, timeout,
    /// conflict, integrity, or backend failure. Invalid models are never persisted.
    #[tracing::instrument(skip_all, fields(operation = "write_authorization_model", store_id = %store_id))]
    pub async fn write(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        definition: AuthorizationModelDefinition,
    ) -> Result<Arc<StoredAuthorizationModel>, ServiceError> {
        let model_id = self.publication.identifiers.next_model_id(context).await?;
        let source = Arc::new(definition.with_identity(store_id, model_id));
        let compiled = self
            .publication
            .compiler
            .compile(source.as_ref())
            .map_err(|errors| invalid_model_error(errors, source.as_ref()))?;
        let stored = Arc::new(StoredAuthorizationModel::new(
            source,
            compiled,
            self.publication.clock.now(),
        )?);
        context.check()?;
        self.writer
            .write_model(context, Arc::clone(&stored))
            .await
            .map_err(ServiceError::store_storage)?;
        context.check()?;
        Ok(stored)
    }

    /// Reads one immutable model by ID.
    ///
    /// # Errors
    ///
    /// Returns store/model-not-found, cancellation, timeout, or backend failure.
    #[tracing::instrument(
        skip_all,
        fields(operation = "read_authorization_model", store_id = %store_id, model_id = %model_id)
    )]
    pub async fn read(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> Result<Arc<StoredAuthorizationModel>, ServiceError> {
        let model = self
            .reader
            .read_model(context, store_id, model_id)
            .await
            .map_err(ServiceError::model_storage)
            .map_err(|error| error.with_model_id(model_id))?;
        context.check()?;
        Ok(model)
    }

    /// Lists immutable models newest first.
    ///
    /// # Errors
    ///
    /// Returns store-not-found, invalid continuation, cancellation, timeout, or backend failure.
    #[tracing::instrument(skip_all, fields(operation = "read_authorization_models", store_id = %store_id))]
    pub async fn list(
        &self,
        context: &OperationContext,
        store_id: StoreId,
        options: &PageOptions,
    ) -> Result<Page<Arc<StoredAuthorizationModel>>, ServiceError> {
        let page = self
            .reader
            .list_models(context, store_id, options)
            .await
            .map_err(ServiceError::model_storage)?;
        context.check()?;
        Ok(page)
    }
}

fn invalid_model_error(errors: ModelErrors, source: &AuthorizationModelSource) -> ServiceError {
    let context = errors
        .errors()
        .first()
        .and_then(|error| model_error_context(error.code(), error.path(), source));
    let error = ServiceError::from(errors);
    match context {
        Some(context) => error.with_model_context(context),
        None => error,
    }
}

fn model_error_context(
    code: ModelErrorCode,
    path: DeclarationPath,
    source: &AuthorizationModelSource,
) -> Option<ModelSemanticContext> {
    match path {
        DeclarationPath::Model => Some(ModelSemanticContext::Model),
        DeclarationPath::Type { index } => {
            let r#type = type_at(source, index)?;
            Some(ModelSemanticContext::Type {
                object_type: r#type.name().clone(),
            })
        }
        DeclarationPath::Relation {
            type_index,
            relation_index,
        } => relation_context(code, source, type_index, relation_index, None),
        DeclarationPath::Rewrite {
            type_index,
            relation_index,
            node_index,
        } => relation_context(code, source, type_index, relation_index, Some(node_index)),
        DeclarationPath::Restriction {
            type_index,
            relation_index,
            restriction_index,
        } => restriction_context(source, type_index, relation_index, restriction_index),
        DeclarationPath::Condition { index } => condition_context(source, index),
        DeclarationPath::Parameter {
            condition_index, ..
        } => condition_context(source, condition_index),
        _ => None,
    }
}

fn type_at(source: &AuthorizationModelSource, index: u32) -> Option<&TypeDefinitionSource> {
    source.type_definitions().get(usize::try_from(index).ok()?)
}

fn relation_at(
    source: &AuthorizationModelSource,
    type_index: u32,
    relation_index: u32,
) -> Option<(&TypeDefinitionSource, &RelationSource)> {
    let r#type = type_at(source, type_index)?;
    let relation = r#type
        .relations()
        .get(usize::try_from(relation_index).ok()?)?;
    Some((r#type, relation))
}

fn restriction_context(
    source: &AuthorizationModelSource,
    type_index: u32,
    relation_index: u32,
    restriction_index: u32,
) -> Option<ModelSemanticContext> {
    let (r#type, relation) = relation_at(source, type_index, relation_index)?;
    let restriction = relation
        .restrictions()
        .get(usize::try_from(restriction_index).ok()?)?;
    let subject_relation = match restriction.kind().as_ref() {
        RestrictionKindSourceRef::Userset(relation) => Some(relation.clone()),
        RestrictionKindSourceRef::Object | RestrictionKindSourceRef::Wildcard => None,
    };
    Some(ModelSemanticContext::Restriction {
        object_type: r#type.name().clone(),
        relation: relation.name().clone(),
        subject_type: restriction.subject_type().clone(),
        subject_relation,
        condition: restriction.condition().cloned(),
    })
}

fn condition_context(
    source: &AuthorizationModelSource,
    index: u32,
) -> Option<ModelSemanticContext> {
    let condition = source.conditions().get(usize::try_from(index).ok()?)?;
    Some(ModelSemanticContext::Condition {
        key: condition.key().clone(),
        name: condition.definition().name().clone(),
    })
}

fn relation_context(
    code: ModelErrorCode,
    source: &AuthorizationModelSource,
    type_index: u32,
    relation_index: u32,
    node_index: Option<u32>,
) -> Option<ModelSemanticContext> {
    let (r#type, relation) = relation_at(source, type_index, relation_index)?;
    let selected_rewrite = node_index
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| rewrite_at(relation.rewrite(), index));
    let (operator, child_count) = match selected_rewrite.map(RewriteSource::as_ref) {
        Some(RewriteSourceRef::Union(children)) => {
            (Some(ModelSetOperator::Union), Some(children.len()))
        }
        Some(RewriteSourceRef::Intersection(children)) => {
            (Some(ModelSetOperator::Intersection), Some(children.len()))
        }
        _ => (None, None),
    };
    let (referenced_relation, tupleset, computed) = rewrite_references(
        code,
        r#type,
        relation.name(),
        relation.rewrite(),
        node_index,
    );
    let target_types = tupleset
        .as_ref()
        .and_then(|name| {
            r#type
                .relations()
                .iter()
                .find(|candidate| candidate.name() == name)
        })
        .map(|tupleset_relation| {
            tupleset_relation
                .restrictions()
                .iter()
                .map(|restriction| match restriction.kind().as_ref() {
                    RestrictionKindSourceRef::Object => {
                        ModelRelationType::Object(restriction.subject_type().clone())
                    }
                    RestrictionKindSourceRef::Userset(relation) => ModelRelationType::Userset(
                        restriction.subject_type().clone(),
                        relation.clone(),
                    ),
                    RestrictionKindSourceRef::Wildcard => {
                        ModelRelationType::Wildcard(restriction.subject_type().clone())
                    }
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
        .unwrap_or_default();
    if referenced_relation.is_none()
        && tupleset.is_none()
        && computed.is_none()
        && operator.is_none()
    {
        return Some(ModelSemanticContext::Relation {
            object_type: r#type.name().clone(),
            relation: relation.name().clone(),
        });
    }
    Some(ModelSemanticContext::Rewrite {
        object_type: r#type.name().clone(),
        relation: relation.name().clone(),
        referenced_relation,
        tupleset,
        computed,
        target_types,
        operator,
        child_count,
    })
}

fn rewrite_references(
    code: ModelErrorCode,
    r#type: &TypeDefinitionSource,
    relation: &RelationName,
    root: &RewriteSource,
    node_index: Option<u32>,
) -> (
    Option<RelationName>,
    Option<RelationName>,
    Option<RelationName>,
) {
    let selected = node_index
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| rewrite_at(root, index));
    let mut pending = vec![selected.unwrap_or(root)];
    while let Some(rewrite) = pending.pop() {
        match rewrite.as_ref() {
            RewriteSourceRef::Computed(referenced)
                if (code == ModelErrorCode::IllegalSelfReference && referenced == relation)
                    || (code == ModelErrorCode::UndefinedRelation
                        && !relation_declared(r#type, referenced)) =>
            {
                return (Some(referenced.clone()), None, None);
            }
            RewriteSourceRef::TupleToUserset { tupleset, computed }
                if matches!(
                    code,
                    ModelErrorCode::UndefinedRelation
                        | ModelErrorCode::InvalidTuplesetRelation
                        | ModelErrorCode::InvalidTupleToUsersetTarget
                        | ModelErrorCode::InvalidRestriction
                ) =>
            {
                return (None, Some(tupleset.clone()), Some(computed.clone()));
            }
            RewriteSourceRef::Union(children) | RewriteSourceRef::Intersection(children) => {
                pending.extend(children.iter().rev());
            }
            RewriteSourceRef::Difference { base, subtract } => {
                pending.push(subtract);
                pending.push(base);
            }
            RewriteSourceRef::Direct
            | RewriteSourceRef::Computed(_)
            | RewriteSourceRef::TupleToUserset { .. } => {}
        }
    }
    (None, None, None)
}

fn rewrite_at(root: &RewriteSource, target_index: usize) -> Option<&RewriteSource> {
    let mut pending = vec![root];
    let mut index = 0_usize;
    while let Some(rewrite) = pending.pop() {
        if index == target_index {
            return Some(rewrite);
        }
        index = index.saturating_add(1);
        match rewrite.as_ref() {
            RewriteSourceRef::Union(children) | RewriteSourceRef::Intersection(children) => {
                pending.extend(children.iter().rev());
            }
            RewriteSourceRef::Difference { base, subtract } => {
                pending.push(subtract);
                pending.push(base);
            }
            RewriteSourceRef::Direct
            | RewriteSourceRef::Computed(_)
            | RewriteSourceRef::TupleToUserset { .. } => {}
        }
    }
    None
}

fn relation_declared(r#type: &TypeDefinitionSource, relation: &RelationName) -> bool {
    r#type
        .relations()
        .iter()
        .any(|candidate| candidate.name() == relation)
}

impl fmt::Debug for ModelService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelService")
            .field("reader", &"dyn ModelReader")
            .field("writer", &"dyn ModelWriter")
            .field("publication", &self.publication)
            .finish_non_exhaustive()
    }
}
