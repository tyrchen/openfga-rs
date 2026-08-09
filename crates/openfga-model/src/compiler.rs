//! Deterministic bounded authorization-model compilation pipeline.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    mem::size_of,
    sync::Arc,
};

use openfga_condition::{CompiledCondition, ConditionCompiler};
use openfga_domain::{
    AuthorizationModelId, ConditionName, Fingerprint, FingerprintBuilder, RelationName, StoreId,
    TypeName,
};

use crate::{
    error::{
        DeclarationPath, ErrorCollector, ModelErrorCode, ModelErrorDetail, ModelErrors,
        ModelLookupError,
    },
    graph::{GraphMetadata, build_graph_metadata, computed_cycle_relations},
    ir::{
        CompiledConditionEntry, CompiledRelation, CompiledType, ConditionId, ConditionRequirement,
        DirectRestriction, NodeId, RelationId, RestrictionKind, RewriteNode, TypeId,
    },
    limits::ModelLimits,
    source::{
        AuthorizationModelSource, DirectRestrictionSource, RelationSource, RestrictionKindSource,
        RewriteSource,
    },
};

const SUPPORTED_SCHEMA_VERSION: &str = "1.1";

/// Version of the immutable compiler representation and fingerprint encoding.
pub const MODEL_COMPILER_FORMAT_VERSION: u32 = 1;

/// Deterministic compiler configured with finite model and condition limits.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ModelCompiler {
    limits: ModelLimits,
}

impl ModelCompiler {
    /// Creates a compiler with an explicit bounded policy.
    #[must_use]
    pub const fn new(limits: ModelLimits) -> Self {
        Self { limits }
    }

    /// Validates and compiles one model into immutable cacheable state.
    ///
    /// # Errors
    ///
    /// Returns deterministic structured diagnostics for every independent
    /// failure found before the configured diagnostic cap.
    pub fn compile(
        &self,
        source: &AuthorizationModelSource,
    ) -> Result<Arc<CompiledModel>, ModelErrors> {
        Compiler::new(source, &self.limits).compile()
    }
}

/// Immutable compiled authorization model shared by query engines and caches.
#[non_exhaustive]
pub struct CompiledModel {
    store_id: StoreId,
    model_id: AuthorizationModelId,
    schema_version: Box<str>,
    compiler_format_version: u32,
    source_fingerprint: Fingerprint,
    fingerprint: Fingerprint,
    types: Box<[CompiledType]>,
    type_lookup: BTreeMap<TypeName, TypeId>,
    relations: Box<[CompiledRelation]>,
    relation_lookup: BTreeMap<(TypeName, RelationName), RelationId>,
    nodes: Box<[RewriteNode]>,
    conditions: Box<[CompiledConditionEntry]>,
    condition_lookup: BTreeMap<ConditionName, ConditionId>,
    graph: GraphMetadata,
}

impl CompiledModel {
    /// Returns the owning store identifier.
    #[must_use]
    pub const fn store_id(&self) -> &StoreId {
        &self.store_id
    }

    /// Returns the immutable authorization-model identifier.
    #[must_use]
    pub const fn model_id(&self) -> &AuthorizationModelId {
        &self.model_id
    }

    /// Returns the validated authorization-model schema version.
    #[must_use]
    pub const fn schema_version(&self) -> &str {
        &self.schema_version
    }

    /// Returns the compiler representation version used for cache invalidation.
    #[must_use]
    pub const fn compiler_format_version(&self) -> u32 {
        self.compiler_format_version
    }

    /// Returns the compiler-produced proof of the exact source model.
    #[must_use]
    pub const fn source_fingerprint(&self) -> Fingerprint {
        self.source_fingerprint
    }

    /// Returns the canonical semantic fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    /// Returns a conservative estimate of heap and inline bytes owned by this compiled model.
    #[must_use]
    pub fn estimated_owned_bytes(&self) -> usize {
        let map_node_overhead = 4_usize.saturating_mul(size_of::<usize>());
        let mut bytes =
            size_of::<Self>()
                .saturating_add(self.schema_version.len())
                .saturating_add(self.types.len().saturating_mul(size_of::<CompiledType>()))
                .saturating_add(
                    self.relations
                        .len()
                        .saturating_mul(size_of::<CompiledRelation>()),
                )
                .saturating_add(self.nodes.len().saturating_mul(size_of::<RewriteNode>()))
                .saturating_add(
                    self.conditions
                        .len()
                        .saturating_mul(size_of::<CompiledConditionEntry>()),
                )
                .saturating_add(self.type_lookup.len().saturating_mul(
                    size_of::<(TypeName, TypeId)>().saturating_add(map_node_overhead),
                ))
                .saturating_add(
                    self.relation_lookup.len().saturating_mul(
                        size_of::<((TypeName, RelationName), RelationId)>()
                            .saturating_add(map_node_overhead),
                    ),
                )
                .saturating_add(self.condition_lookup.len().saturating_mul(
                    size_of::<(ConditionName, ConditionId)>().saturating_add(map_node_overhead),
                ));
        for compiled_type in &self.types {
            bytes = bytes
                .saturating_add(compiled_type.name.as_str().len())
                .saturating_add(
                    compiled_type
                        .relations
                        .len()
                        .saturating_mul(size_of::<RelationId>()),
                );
        }
        for relation in &self.relations {
            bytes = bytes
                .saturating_add(relation.name.as_str().len())
                .saturating_add(
                    relation
                        .restrictions
                        .len()
                        .saturating_mul(size_of::<DirectRestriction>()),
                );
        }
        for node in &self.nodes {
            bytes = bytes.saturating_add(match node {
                RewriteNode::TupleToUserset {
                    computed, targets, ..
                } => computed
                    .as_str()
                    .len()
                    .saturating_add(targets.len().saturating_mul(size_of::<RelationId>())),
                RewriteNode::Union(children) | RewriteNode::Intersection(children) => {
                    children.len().saturating_mul(size_of::<NodeId>())
                }
                RewriteNode::Direct(_)
                | RewriteNode::Computed(_)
                | RewriteNode::Difference { .. } => 0,
            });
        }
        for condition in &self.conditions {
            bytes = bytes
                .saturating_add(condition.name.as_str().len())
                .saturating_add(condition.condition.estimated_owned_bytes());
        }
        for name in self.type_lookup.keys() {
            bytes = bytes.saturating_add(name.as_str().len());
        }
        for (object_type, relation) in self.relation_lookup.keys() {
            bytes = bytes
                .saturating_add(object_type.as_str().len())
                .saturating_add(relation.as_str().len());
        }
        for name in self.condition_lookup.keys() {
            bytes = bytes.saturating_add(name.as_str().len());
        }
        bytes.saturating_add(graph_owned_bytes(&self.graph, map_node_overhead))
    }

    /// Resolves a declared object type.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::NotFound`] when the type is undeclared.
    pub fn type_id(&self, name: &TypeName) -> Result<TypeId, ModelLookupError> {
        self.type_lookup
            .get(name)
            .copied()
            .ok_or(ModelLookupError::NotFound)
    }

    /// Returns the declared name for a dense object-type ID.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::InvalidIdentifier`] for a foreign/out-of-range ID.
    pub fn type_name(&self, id: TypeId) -> Result<&TypeName, ModelLookupError> {
        self.types
            .get(id.index())
            .map(|compiled_type| &compiled_type.name)
            .ok_or(ModelLookupError::InvalidIdentifier)
    }

    /// Resolves a declared relation on an object type.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::NotFound`] when the declaration is absent.
    pub fn relation_id(
        &self,
        object_type: &TypeName,
        relation: &RelationName,
    ) -> Result<RelationId, ModelLookupError> {
        self.relation_lookup
            .get(&(object_type.clone(), relation.clone()))
            .copied()
            .ok_or(ModelLookupError::NotFound)
    }

    /// Returns one compiled relation by dense ID.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::InvalidIdentifier`] for a foreign/out-of-range ID.
    pub fn relation(&self, id: RelationId) -> Result<&CompiledRelation, ModelLookupError> {
        self.relations
            .get(id.index())
            .ok_or(ModelLookupError::InvalidIdentifier)
    }

    /// Returns one rewrite node by dense ID.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::InvalidIdentifier`] for a foreign/out-of-range ID.
    pub fn node(&self, id: NodeId) -> Result<&RewriteNode, ModelLookupError> {
        self.nodes
            .get(id.index())
            .ok_or(ModelLookupError::InvalidIdentifier)
    }

    /// Returns one compiled condition by dense ID.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::InvalidIdentifier`] for a foreign/out-of-range ID.
    pub fn condition(&self, id: ConditionId) -> Result<&Arc<CompiledCondition>, ModelLookupError> {
        self.conditions
            .get(id.index())
            .map(|entry| &entry.condition)
            .ok_or(ModelLookupError::InvalidIdentifier)
    }

    /// Resolves a compiled condition by public name.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::NotFound`] when no condition has that name.
    pub fn condition_id(&self, name: &ConditionName) -> Result<ConditionId, ModelLookupError> {
        self.condition_lookup
            .get(name)
            .copied()
            .ok_or(ModelLookupError::NotFound)
    }

    /// Returns relations declared on one type in deterministic source order.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::InvalidIdentifier`] for a foreign/out-of-range type ID.
    pub fn relations_for_type(&self, id: TypeId) -> Result<&[RelationId], ModelLookupError> {
        self.types
            .get(id.index())
            .map(|compiled_type| compiled_type.relations.as_ref())
            .ok_or(ModelLookupError::InvalidIdentifier)
    }

    /// Returns whether conservative metadata says a subject type can reach a relation.
    #[must_use]
    pub fn can_reach_subject_type(&self, relation: RelationId, subject_type: TypeId) -> bool {
        self.graph
            .reachable_types
            .get(relation.index())
            .is_some_and(|types| types.contains(&subject_type))
    }

    /// Returns whether a typed wildcard can conservatively reach a relation.
    #[must_use]
    pub fn can_reach_wildcard(&self, relation: RelationId, subject_type: TypeId) -> bool {
        self.graph
            .reachable_wildcards
            .get(relation.index())
            .is_some_and(|types| types.contains(&subject_type))
    }

    /// Returns relations whose results or tuple dependencies consume this relation.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::InvalidIdentifier`] for a foreign/out-of-range ID.
    pub fn reverse_relations(
        &self,
        relation: RelationId,
    ) -> Result<&[RelationId], ModelLookupError> {
        self.graph
            .reverse
            .get(relation.index())
            .map(Box::as_ref)
            .ok_or(ModelLookupError::InvalidIdentifier)
    }

    /// Returns forward semantic relation dependencies.
    ///
    /// # Errors
    ///
    /// Returns [`ModelLookupError::InvalidIdentifier`] for a foreign/out-of-range ID.
    pub fn forward_relations(
        &self,
        relation: RelationId,
    ) -> Result<&[RelationId], ModelLookupError> {
        self.graph
            .forward
            .get(relation.index())
            .map(Box::as_ref)
            .ok_or(ModelLookupError::InvalidIdentifier)
    }

    /// Returns a stable recursion-group ID when the relation lies on a legal query cycle.
    #[must_use]
    pub fn recursion_group(&self, relation: RelationId) -> Option<u32> {
        self.graph
            .recursion_groups
            .get(relation.index())
            .copied()
            .flatten()
    }
}

fn graph_owned_bytes(graph: &GraphMetadata, map_node_overhead: usize) -> usize {
    let relation_id_bytes = |relations: &[Box<[RelationId]>]| {
        relations.iter().fold(
            relations
                .len()
                .saturating_mul(size_of::<Box<[RelationId]>>()),
            |total, row| total.saturating_add(row.len().saturating_mul(size_of::<RelationId>())),
        )
    };
    let set_bytes = |sets: &[BTreeSet<TypeId>]| {
        sets.iter().fold(
            sets.len().saturating_mul(size_of::<BTreeSet<TypeId>>()),
            |total, set| {
                total.saturating_add(
                    set.len()
                        .saturating_mul(size_of::<TypeId>().saturating_add(map_node_overhead)),
                )
            },
        )
    };
    size_of::<GraphMetadata>()
        .saturating_add(relation_id_bytes(&graph.forward))
        .saturating_add(relation_id_bytes(&graph.reverse))
        .saturating_add(set_bytes(&graph.reachable_types))
        .saturating_add(set_bytes(&graph.reachable_wildcards))
        .saturating_add(
            graph
                .recursion_groups
                .capacity()
                .saturating_mul(size_of::<Option<u32>>()),
        )
}

impl fmt::Debug for CompiledModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledModel")
            .field("store_id", &self.store_id)
            .field("model_id", &self.model_id)
            .field("schema_version", &self.schema_version)
            .field("compiler_format_version", &self.compiler_format_version)
            .field("fingerprint", &self.fingerprint)
            .field("types", &self.types.len())
            .field("relations", &self.relations.len())
            .field("nodes", &self.nodes.len())
            .field("conditions", &self.conditions.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct RelationOrigin<'a> {
    id: RelationId,
    object_type: TypeId,
    type_index: usize,
    relation_index: usize,
    source: &'a RelationSource,
}

#[derive(Debug)]
struct Symbols<'a> {
    types: Vec<CompiledType>,
    type_lookup: BTreeMap<TypeName, TypeId>,
    relation_origins: Vec<RelationOrigin<'a>>,
    relation_lookup: BTreeMap<(TypeName, RelationName), RelationId>,
}

#[derive(Debug)]
struct Compiler<'a> {
    source: &'a AuthorizationModelSource,
    limits: &'a ModelLimits,
    errors: ErrorCollector,
}

impl<'a> Compiler<'a> {
    fn new(source: &'a AuthorizationModelSource, limits: &'a ModelLimits) -> Self {
        Self {
            source,
            limits,
            errors: ErrorCollector::new(limits.model_errors()),
        }
    }

    fn compile(mut self) -> Result<Arc<CompiledModel>, ModelErrors> {
        self.validate_declarations();
        if !self.errors.is_empty() {
            return Err(self.errors.finish());
        }
        let Some(symbols) = self.build_symbols() else {
            return Err(self.errors.finish());
        };
        let (conditions, condition_lookup) = self.compile_conditions();
        if !self.errors.is_empty() {
            return Err(self.errors.finish());
        }
        let restrictions = self.resolve_restrictions(&symbols, &condition_lookup);
        if !self.errors.is_empty() {
            return Err(self.errors.finish());
        }
        let (relations, nodes) = self.lower_relations(&symbols, &restrictions);
        if !self.errors.is_empty() {
            return Err(self.errors.finish());
        }
        let cycle_relations = computed_cycle_relations(&relations, &nodes);
        let _ = self.validate_entrypoints(&symbols, &relations, &nodes, &cycle_relations);
        for relation in cycle_relations {
            if let Some(origin) = symbols.relation_origins.get(relation.index()) {
                self.errors.push(
                    ModelErrorCode::ForbiddenComputedCycle,
                    relation_path(origin),
                );
            }
        }
        if !self.errors.is_empty() {
            return Err(self.errors.finish());
        }
        let Some(graph) = build_graph_metadata(&relations, &nodes, self.limits, &mut self.errors)
        else {
            return Err(self.errors.finish());
        };
        if !self.errors.is_empty() {
            return Err(self.errors.finish());
        }
        let fingerprint = model_fingerprint(
            self.source,
            MODEL_COMPILER_FORMAT_VERSION,
            &symbols.types,
            &relations,
            &nodes,
            &conditions,
        );
        Ok(Arc::new(CompiledModel {
            store_id: self.source.store_id,
            model_id: self.source.model_id,
            schema_version: self.source.schema_version.clone().into_boxed_str(),
            compiler_format_version: MODEL_COMPILER_FORMAT_VERSION,
            source_fingerprint: self.source.fingerprint(),
            fingerprint,
            types: symbols.types.into_boxed_slice(),
            type_lookup: symbols.type_lookup,
            relations: relations.into_boxed_slice(),
            relation_lookup: symbols.relation_lookup,
            nodes: nodes.into_boxed_slice(),
            conditions: conditions.into_boxed_slice(),
            condition_lookup,
            graph,
        }))
    }

    fn validate_declarations(&mut self) {
        if self.source.schema_version != SUPPORTED_SCHEMA_VERSION {
            self.errors
                .push(ModelErrorCode::InvalidSchemaVersion, DeclarationPath::Model);
        }
        if self.source.type_definitions.is_empty()
            || self.source.type_definitions.len() > self.limits.input().type_definitions()
        {
            self.errors.push(
                ModelErrorCode::InvalidTypeDefinitionCount,
                DeclarationPath::Model,
            );
        }
        if self.source.conditions.len() > self.limits.condition_definitions() {
            self.errors
                .push(ModelErrorCode::TooManyConditions, DeclarationPath::Model);
        }

        let mut seen_types = BTreeSet::new();
        let mut total_relations = 0_usize;
        for (type_index, definition) in self.source.type_definitions.iter().enumerate() {
            let type_path = DeclarationPath::Type {
                index: to_u32(type_index),
            };
            if is_reserved(definition.name.as_str()) {
                self.errors.push(ModelErrorCode::ReservedName, type_path);
            }
            if !seen_types.insert(definition.name.clone()) {
                self.errors.push(ModelErrorCode::DuplicateType, type_path);
            }
            if definition.relations.len() > self.limits.input().relations() {
                self.errors
                    .push(ModelErrorCode::TooManyRelations, type_path);
            }
            total_relations = total_relations.saturating_add(definition.relations.len());
            let mut seen_relations = BTreeSet::new();
            for (relation_index, relation) in definition.relations.iter().enumerate() {
                let path = DeclarationPath::Relation {
                    type_index: to_u32(type_index),
                    relation_index: to_u32(relation_index),
                };
                if is_reserved(relation.name.as_str()) {
                    self.errors.push(ModelErrorCode::ReservedName, path);
                }
                if !seen_relations.insert(relation.name.clone()) {
                    self.errors.push(ModelErrorCode::DuplicateRelation, path);
                }
                if relation.rewrite_valid {
                    validate_rewrite_shape(
                        &relation.rewrite,
                        type_index,
                        relation_index,
                        self.limits,
                        &mut self.errors,
                    );
                } else {
                    self.errors.push(ModelErrorCode::InvalidRewrite, path);
                }
            }
        }
        if total_relations > self.limits.input().relations() {
            self.errors
                .push(ModelErrorCode::TooManyRelations, DeclarationPath::Model);
        }

        let mut seen_conditions = BTreeSet::new();
        for (index, condition) in self.source.conditions.iter().enumerate() {
            let path = DeclarationPath::Condition {
                index: to_u32(index),
            };
            if !seen_conditions.insert(condition.key.clone()) {
                self.errors.push(ModelErrorCode::DuplicateCondition, path);
            }
            if condition.key != *condition.definition.name() {
                self.errors
                    .push(ModelErrorCode::ConditionNameMismatch, path);
            }
            for (parameter_index, error) in &condition.parameter_type_errors {
                self.errors.push_detail(
                    ModelErrorCode::InvalidConditionParameterType,
                    DeclarationPath::Parameter {
                        condition_index: to_u32(index),
                        parameter_index: *parameter_index,
                    },
                    ModelErrorDetail::ConditionParameterType(error.clone()),
                );
            }
        }
    }

    fn build_symbols(&mut self) -> Option<Symbols<'a>> {
        let mut types = Vec::with_capacity(self.source.type_definitions.len());
        let mut type_lookup = BTreeMap::new();
        let mut relation_origins = Vec::new();
        let mut relation_lookup = BTreeMap::new();
        for (type_index, definition) in self.source.type_definitions.iter().enumerate() {
            let type_id = TypeId::from_index(types.len())?;
            type_lookup.insert(definition.name.clone(), type_id);
            let mut type_relations = Vec::with_capacity(definition.relations.len());
            for (relation_index, relation) in definition.relations.iter().enumerate() {
                let relation_id = RelationId::from_index(relation_origins.len())?;
                relation_lookup.insert(
                    (definition.name.clone(), relation.name.clone()),
                    relation_id,
                );
                type_relations.push(relation_id);
                relation_origins.push(RelationOrigin {
                    id: relation_id,
                    object_type: type_id,
                    type_index,
                    relation_index,
                    source: relation,
                });
            }
            types.push(CompiledType {
                id: type_id,
                name: definition.name.clone(),
                relations: type_relations.into_boxed_slice(),
            });
        }
        Some(Symbols {
            types,
            type_lookup,
            relation_origins,
            relation_lookup,
        })
    }

    fn compile_conditions(
        &mut self,
    ) -> (
        Vec<CompiledConditionEntry>,
        BTreeMap<ConditionName, ConditionId>,
    ) {
        let compiler = ConditionCompiler::default();
        let mut conditions = Vec::with_capacity(self.source.conditions.len());
        let mut lookup = BTreeMap::new();
        for (index, source) in self.source.conditions.iter().enumerate() {
            let Some(id) = ConditionId::from_index(index) else {
                self.errors.push(
                    ModelErrorCode::TooManyConditions,
                    DeclarationPath::Condition {
                        index: to_u32(index),
                    },
                );
                continue;
            };
            match compiler.compile(&source.definition, self.limits.conditions()) {
                Ok(condition) => {
                    lookup.insert(source.key.clone(), id);
                    conditions.push(CompiledConditionEntry {
                        id,
                        name: source.key.clone(),
                        condition: Arc::new(condition),
                    });
                }
                Err(error) => self.errors.push_detail(
                    ModelErrorCode::InvalidCondition,
                    DeclarationPath::Condition {
                        index: to_u32(index),
                    },
                    ModelErrorDetail::ConditionCompile {
                        kind: error.kind(),
                        found_type: error.found_type(),
                        diagnostic: error.detail().cloned(),
                    },
                ),
            }
        }
        (conditions, lookup)
    }

    fn resolve_restrictions(
        &mut self,
        symbols: &Symbols<'_>,
        conditions: &BTreeMap<ConditionName, ConditionId>,
    ) -> Vec<Vec<DirectRestriction>> {
        let mut all = Vec::with_capacity(symbols.relation_origins.len());
        for origin in &symbols.relation_origins {
            let mut restrictions = Vec::with_capacity(origin.source.restrictions.len());
            for (restriction_index, source) in origin.source.restrictions.iter().enumerate() {
                if let Some(resolved) =
                    self.resolve_restriction(source, symbols, conditions, origin, restriction_index)
                {
                    restrictions.push(resolved);
                }
            }
            restrictions.sort_unstable();
            restrictions.dedup();
            all.push(restrictions);
        }
        all
    }

    fn resolve_restriction(
        &mut self,
        source: &DirectRestrictionSource,
        symbols: &Symbols<'_>,
        conditions: &BTreeMap<ConditionName, ConditionId>,
        origin: &RelationOrigin<'_>,
        restriction_index: usize,
    ) -> Option<DirectRestriction> {
        let path = DeclarationPath::Restriction {
            type_index: to_u32(origin.type_index),
            relation_index: to_u32(origin.relation_index),
            restriction_index: to_u32(restriction_index),
        };
        let Some(subject_type) = symbols.type_lookup.get(&source.subject_type).copied() else {
            self.errors.push(ModelErrorCode::UndefinedType, path);
            return None;
        };
        let kind = match &source.kind {
            RestrictionKindSource::Object => RestrictionKind::Object,
            RestrictionKindSource::Wildcard => RestrictionKind::Wildcard,
            RestrictionKindSource::Userset(relation) => {
                let Some(target) = symbols
                    .relation_lookup
                    .get(&(source.subject_type.clone(), relation.clone()))
                    .copied()
                else {
                    self.errors.push(ModelErrorCode::UndefinedRelation, path);
                    return None;
                };
                RestrictionKind::Userset(target)
            }
        };
        let condition = match &source.condition {
            None => ConditionRequirement::Unconditional,
            Some(name) => {
                let Some(condition) = conditions.get(name).copied() else {
                    self.errors.push(ModelErrorCode::UndefinedCondition, path);
                    return None;
                };
                ConditionRequirement::Required(condition)
            }
        };
        Some(DirectRestriction {
            subject_type,
            kind,
            condition,
        })
    }

    fn lower_relations(
        &mut self,
        symbols: &Symbols<'_>,
        restrictions: &[Vec<DirectRestriction>],
    ) -> (Vec<CompiledRelation>, Vec<RewriteNode>) {
        let mut nodes = Vec::new();
        let mut node_lookup = BTreeMap::new();
        let mut relations = Vec::with_capacity(symbols.relation_origins.len());
        for origin in &symbols.relation_origins {
            let relation_restrictions = restrictions
                .get(origin.id.index())
                .cloned()
                .unwrap_or_default();
            let context = LowerContext {
                origin,
                symbols,
                restrictions,
            };
            let root = lower_rewrite(
                &origin.source.rewrite,
                context,
                &mut nodes,
                &mut node_lookup,
                self.limits,
                &mut self.errors,
            );
            let Some(root) = root else {
                continue;
            };
            let assignable = contains_direct(root, &nodes);
            if assignable && relation_restrictions.is_empty() {
                self.errors.push(
                    ModelErrorCode::AssignableWithoutRestrictions,
                    relation_path(origin),
                );
            }
            if !assignable && !relation_restrictions.is_empty() {
                self.errors.push(
                    ModelErrorCode::NonAssignableWithRestrictions,
                    relation_path(origin),
                );
            }
            relations.push(CompiledRelation {
                id: origin.id,
                object_type: origin.object_type,
                name: origin.source.name.clone(),
                root,
                restrictions: relation_restrictions.into_boxed_slice(),
            });
        }
        (relations, nodes)
    }

    fn validate_entrypoints(
        &mut self,
        symbols: &Symbols<'_>,
        relations: &[CompiledRelation],
        nodes: &[RewriteNode],
        cycle_relations: &BTreeSet<RelationId>,
    ) -> Vec<bool> {
        let mut entrypoints = vec![false; relations.len()];
        let mut remaining = relations.len().saturating_add(1);
        let mut changed = true;
        while changed && remaining > 0 {
            changed = false;
            remaining = remaining.saturating_sub(1);
            let node_values = entrypoint_nodes(relations, nodes, &entrypoints);
            for relation in relations {
                let value = node_values
                    .get(relation.root().index())
                    .copied()
                    .unwrap_or(false);
                if value
                    && !entrypoints
                        .get(relation.id().index())
                        .copied()
                        .unwrap_or(false)
                    && let Some(slot) = entrypoints.get_mut(relation.id().index())
                {
                    *slot = true;
                    changed = true;
                }
            }
        }
        for relation in relations {
            if !entrypoints
                .get(relation.id().index())
                .copied()
                .unwrap_or(false)
                && let Some(origin) = symbols.relation_origins.get(relation.id().index())
            {
                let code = if cycle_relations.contains(&relation.id()) {
                    ModelErrorCode::PotentialLoop
                } else {
                    ModelErrorCode::NoEntrypoints
                };
                self.errors.push(code, relation_path(origin));
            }
        }
        entrypoints
    }
}

#[derive(Clone, Copy, Debug)]
struct LowerContext<'a> {
    origin: &'a RelationOrigin<'a>,
    symbols: &'a Symbols<'a>,
    restrictions: &'a [Vec<DirectRestriction>],
}

#[derive(Debug)]
enum LowerFrame<'a> {
    Visit(&'a RewriteSource, usize),
    FinishUnion(usize),
    FinishIntersection(usize),
    FinishDifference,
}

#[allow(
    clippy::too_many_lines,
    reason = "the iterative frame machine keeps rewrite lowering nonrecursive and explicit"
)]
fn lower_rewrite(
    root: &RewriteSource,
    context: LowerContext<'_>,
    nodes: &mut Vec<RewriteNode>,
    node_lookup: &mut BTreeMap<RewriteNode, NodeId>,
    limits: &ModelLimits,
    errors: &mut ErrorCollector,
) -> Option<NodeId> {
    let mut frames = vec![LowerFrame::Visit(root, 1)];
    let mut values = Vec::new();
    let mut source_nodes = 0_usize;
    while let Some(frame) = frames.pop() {
        match frame {
            LowerFrame::Visit(source, depth) => {
                let path = rewrite_path(context.origin, source_nodes);
                source_nodes = source_nodes.saturating_add(1);
                match source {
                    RewriteSource::Direct => values.push(intern_node(
                        RewriteNode::Direct(context.origin.id),
                        nodes,
                        node_lookup,
                        limits,
                        errors,
                    )?),
                    RewriteSource::Computed(name) => {
                        let type_name = context
                            .symbols
                            .types
                            .get(context.origin.object_type.index())
                            .map(|compiled_type| compiled_type.name.clone())?;
                        let Some(target) = context
                            .symbols
                            .relation_lookup
                            .get(&(type_name, name.clone()))
                            .copied()
                        else {
                            errors.push(ModelErrorCode::UndefinedRelation, path);
                            return None;
                        };
                        if target == context.origin.id {
                            errors.push(ModelErrorCode::IllegalSelfReference, path);
                            return None;
                        }
                        values.push(intern_node(
                            RewriteNode::Computed(target),
                            nodes,
                            node_lookup,
                            limits,
                            errors,
                        )?);
                    }
                    RewriteSource::TupleToUserset { tupleset, computed } => {
                        let (tupleset_id, targets) =
                            resolve_ttu(tupleset, computed, context, path, errors)?;
                        values.push(intern_node(
                            RewriteNode::TupleToUserset {
                                tupleset: tupleset_id,
                                computed: computed.clone(),
                                targets: targets.into_boxed_slice(),
                            },
                            nodes,
                            node_lookup,
                            limits,
                            errors,
                        )?);
                    }
                    RewriteSource::Union(children) => {
                        push_lower_children(&mut frames, children, depth, LowerFrame::FinishUnion);
                    }
                    RewriteSource::Intersection(children) => {
                        push_lower_children(
                            &mut frames,
                            children,
                            depth,
                            LowerFrame::FinishIntersection,
                        );
                    }
                    RewriteSource::Difference { base, subtract } => {
                        let next_depth = depth.checked_add(1)?;
                        frames.push(LowerFrame::FinishDifference);
                        frames.push(LowerFrame::Visit(subtract, next_depth));
                        frames.push(LowerFrame::Visit(base, next_depth));
                    }
                }
            }
            LowerFrame::FinishUnion(count) => {
                let children = take_node_tail(&mut values, count)?;
                values.push(intern_node(
                    RewriteNode::Union(children.into_boxed_slice()),
                    nodes,
                    node_lookup,
                    limits,
                    errors,
                )?);
            }
            LowerFrame::FinishIntersection(count) => {
                let children = take_node_tail(&mut values, count)?;
                values.push(intern_node(
                    RewriteNode::Intersection(children.into_boxed_slice()),
                    nodes,
                    node_lookup,
                    limits,
                    errors,
                )?);
            }
            LowerFrame::FinishDifference => {
                let subtract = values.pop()?;
                let base = values.pop()?;
                values.push(intern_node(
                    RewriteNode::Difference { base, subtract },
                    nodes,
                    node_lookup,
                    limits,
                    errors,
                )?);
            }
        }
    }
    if values.len() == 1 {
        values.pop()
    } else {
        errors.push(
            ModelErrorCode::InvalidRewrite,
            relation_path(context.origin),
        );
        None
    }
}

fn resolve_ttu(
    tupleset: &RelationName,
    computed: &RelationName,
    context: LowerContext<'_>,
    path: DeclarationPath,
    errors: &mut ErrorCollector,
) -> Option<(RelationId, Vec<RelationId>)> {
    let type_name = context
        .symbols
        .types
        .get(context.origin.object_type.index())?
        .name
        .clone();
    let Some(tupleset_id) = context
        .symbols
        .relation_lookup
        .get(&(type_name, tupleset.clone()))
        .copied()
    else {
        errors.push(ModelErrorCode::UndefinedRelation, path);
        return None;
    };
    let Some(tupleset_origin) = context.symbols.relation_origins.get(tupleset_id.index()) else {
        errors.push(ModelErrorCode::InvalidTuplesetRelation, path);
        return None;
    };
    if !matches!(tupleset_origin.source.rewrite, RewriteSource::Direct) {
        errors.push(ModelErrorCode::InvalidTuplesetRelation, path);
        return None;
    }
    let restrictions = context.restrictions.get(tupleset_id.index())?;
    let mut targets = Vec::new();
    for restriction in restrictions {
        if restriction.kind() != RestrictionKind::Object {
            errors.push(ModelErrorCode::InvalidRestriction, path);
            return None;
        }
        let Some(target_type) = context
            .symbols
            .types
            .get(restriction.subject_type().index())
        else {
            continue;
        };
        let Some(target) = context
            .symbols
            .relation_lookup
            .get(&(target_type.name.clone(), computed.clone()))
            .copied()
        else {
            errors.push(ModelErrorCode::InvalidTupleToUsersetTarget, path);
            return None;
        };
        targets.push(target);
    }
    targets.sort_unstable();
    targets.dedup();
    if targets.is_empty() {
        errors.push(ModelErrorCode::InvalidTupleToUsersetTarget, path);
        None
    } else {
        Some((tupleset_id, targets))
    }
}

fn intern_node(
    node: RewriteNode,
    nodes: &mut Vec<RewriteNode>,
    lookup: &mut BTreeMap<RewriteNode, NodeId>,
    limits: &ModelLimits,
    errors: &mut ErrorCollector,
) -> Option<NodeId> {
    if let Some(existing) = lookup.get(&node) {
        return Some(*existing);
    }
    if nodes.len() >= limits.rewrite_nodes() {
        errors.push(ModelErrorCode::RewriteLimitExceeded, DeclarationPath::Model);
        return None;
    }
    let id = NodeId::from_index(nodes.len())?;
    lookup.insert(node.clone(), id);
    nodes.push(node);
    Some(id)
}

fn push_lower_children<'a>(
    frames: &mut Vec<LowerFrame<'a>>,
    children: &'a [RewriteSource],
    depth: usize,
    finish: fn(usize) -> LowerFrame<'a>,
) {
    frames.push(finish(children.len()));
    if let Some(next_depth) = depth.checked_add(1) {
        for child in children.iter().rev() {
            frames.push(LowerFrame::Visit(child, next_depth));
        }
    }
}

fn take_node_tail(values: &mut Vec<NodeId>, count: usize) -> Option<Vec<NodeId>> {
    let start = values.len().checked_sub(count)?;
    Some(values.split_off(start))
}

fn contains_direct(root: NodeId, nodes: &[RewriteNode]) -> bool {
    let mut stack = vec![root];
    let mut seen = BTreeSet::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some(node) = nodes.get(id.index()) else {
            continue;
        };
        match node {
            RewriteNode::Direct(_) => return true,
            RewriteNode::Union(children) | RewriteNode::Intersection(children) => {
                stack.extend(children.iter().copied());
            }
            RewriteNode::Difference { base, subtract } => {
                stack.push(*base);
                stack.push(*subtract);
            }
            RewriteNode::Computed(_) | RewriteNode::TupleToUserset { .. } => {}
        }
    }
    false
}

fn entrypoint_nodes(
    relations: &[CompiledRelation],
    nodes: &[RewriteNode],
    relation_entrypoints: &[bool],
) -> Vec<bool> {
    let mut values = Vec::with_capacity(nodes.len());
    for node in nodes {
        let value = match node {
            RewriteNode::Direct(owner) => relations.get(owner.index()).is_some_and(|relation| {
                relation
                    .restrictions()
                    .iter()
                    .any(|restriction| match restriction.kind() {
                        RestrictionKind::Object | RestrictionKind::Wildcard => true,
                        RestrictionKind::Userset(target) => relation_entrypoints
                            .get(target.index())
                            .copied()
                            .unwrap_or(false),
                    })
            }),
            RewriteNode::Computed(target) => relation_entrypoints
                .get(target.index())
                .copied()
                .unwrap_or(false),
            RewriteNode::TupleToUserset { targets, .. } => targets.iter().any(|target| {
                relation_entrypoints
                    .get(target.index())
                    .copied()
                    .unwrap_or(false)
            }),
            RewriteNode::Union(children) => children
                .iter()
                .any(|child| values.get(child.index()).copied().unwrap_or(false)),
            RewriteNode::Intersection(children) => children
                .iter()
                .all(|child| values.get(child.index()).copied().unwrap_or(false)),
            RewriteNode::Difference { base, subtract } => {
                values.get(base.index()).copied().unwrap_or(false)
                    && values.get(subtract.index()).copied().unwrap_or(false)
            }
        };
        values.push(value);
    }
    values
}

fn validate_rewrite_shape(
    root: &RewriteSource,
    type_index: usize,
    relation_index: usize,
    limits: &ModelLimits,
    errors: &mut ErrorCollector,
) {
    let mut stack = vec![(root, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((rewrite, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        let path = DeclarationPath::Rewrite {
            type_index: to_u32(type_index),
            relation_index: to_u32(relation_index),
            node_index: to_u32(nodes.saturating_sub(1)),
        };
        if nodes > limits.rewrite_nodes() || depth > limits.rewrite_depth() {
            errors.push(ModelErrorCode::RewriteLimitExceeded, path);
            return;
        }
        let next_depth = depth.saturating_add(1);
        match rewrite {
            RewriteSource::Union(children) | RewriteSource::Intersection(children) => {
                if children.len() < 2 {
                    errors.push(ModelErrorCode::InvalidOperatorArity, path);
                }
                if children.len() > limits.input().operands() {
                    errors.push(ModelErrorCode::RewriteLimitExceeded, path);
                }
                stack.extend(children.iter().rev().map(|child| (child, next_depth)));
            }
            RewriteSource::Difference { base, subtract } => {
                stack.push((subtract, next_depth));
                stack.push((base, next_depth));
            }
            RewriteSource::Direct
            | RewriteSource::Computed(_)
            | RewriteSource::TupleToUserset { .. } => {}
        }
    }
}

fn model_fingerprint(
    source: &AuthorizationModelSource,
    format_version: u32,
    types: &[CompiledType],
    relations: &[CompiledRelation],
    nodes: &[RewriteNode],
    conditions: &[CompiledConditionEntry],
) -> Fingerprint {
    let mut builder = FingerprintBuilder::new("openfga.compiled-model.v1");
    builder.write_bytes(source.store_id.to_string().as_bytes());
    builder.write_bytes(source.model_id.to_string().as_bytes());
    builder.write_str(&source.schema_version);
    builder.write_u32(format_version);
    builder.write_u64(to_u64(types.len()));
    for compiled_type in types {
        builder.write_u32(compiled_type.id.as_u32());
        builder.write_str(compiled_type.name.as_str());
        builder.write_u64(to_u64(compiled_type.relations.len()));
        for relation in &compiled_type.relations {
            builder.write_u32(relation.as_u32());
        }
    }
    builder.write_u64(to_u64(relations.len()));
    for relation in relations {
        builder.write_u32(relation.id().as_u32());
        builder.write_u32(relation.object_type().as_u32());
        builder.write_str(relation.name().as_str());
        builder.write_u32(relation.root().as_u32());
        builder.write_u64(to_u64(relation.restrictions().len()));
        for restriction in relation.restrictions() {
            builder.write_u32(restriction.subject_type().as_u32());
            match restriction.kind() {
                RestrictionKind::Object => builder.write_tag(0),
                RestrictionKind::Userset(target) => {
                    builder.write_tag(1);
                    builder.write_u32(target.as_u32());
                }
                RestrictionKind::Wildcard => builder.write_tag(2),
            }
            match restriction.condition() {
                ConditionRequirement::Unconditional => builder.write_tag(0),
                ConditionRequirement::Required(condition) => {
                    builder.write_tag(1);
                    builder.write_u32(condition.as_u32());
                }
            }
        }
    }
    builder.write_u64(to_u64(nodes.len()));
    for node in nodes {
        write_node_fingerprint(&mut builder, node);
    }
    builder.write_u64(to_u64(conditions.len()));
    for condition in conditions {
        builder.write_u32(condition.id.as_u32());
        builder.write_str(condition.name.as_str());
        builder.write_bytes(condition.condition.fingerprint().as_bytes());
    }
    builder.finish()
}

fn write_node_fingerprint(builder: &mut FingerprintBuilder, node: &RewriteNode) {
    match node {
        RewriteNode::Direct(owner) => {
            builder.write_tag(0);
            builder.write_u32(owner.as_u32());
        }
        RewriteNode::Computed(target) => {
            builder.write_tag(1);
            builder.write_u32(target.as_u32());
        }
        RewriteNode::TupleToUserset {
            tupleset,
            computed,
            targets,
        } => {
            builder.write_tag(2);
            builder.write_u32(tupleset.as_u32());
            builder.write_str(computed.as_str());
            builder.write_u64(to_u64(targets.len()));
            for target in targets {
                builder.write_u32(target.as_u32());
            }
        }
        RewriteNode::Union(children) | RewriteNode::Intersection(children) => {
            builder.write_tag(if matches!(node, RewriteNode::Union(_)) {
                3
            } else {
                4
            });
            builder.write_u64(to_u64(children.len()));
            for child in children {
                builder.write_u32(child.as_u32());
            }
        }
        RewriteNode::Difference { base, subtract } => {
            builder.write_tag(5);
            builder.write_u32(base.as_u32());
            builder.write_u32(subtract.as_u32());
        }
    }
}

const fn is_reserved(name: &str) -> bool {
    matches!(name.as_bytes(), b"self" | b"this")
}

fn to_u32(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

fn to_u64(index: usize) -> u64 {
    u64::try_from(index).unwrap_or(u64::MAX)
}

fn relation_path(origin: &RelationOrigin<'_>) -> DeclarationPath {
    DeclarationPath::Relation {
        type_index: to_u32(origin.type_index),
        relation_index: to_u32(origin.relation_index),
    }
}

fn rewrite_path(origin: &RelationOrigin<'_>, node_index: usize) -> DeclarationPath {
    DeclarationPath::Rewrite {
        type_index: to_u32(origin.type_index),
        relation_index: to_u32(origin.relation_index),
        node_index: to_u32(node_index),
    }
}
