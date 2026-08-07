//! Bounded diagnostic userset-tree expansion.
//!
//! `async-trait` preserves object safety because the service layer stores the
//! engine behind `Arc<dyn ExpandEngine>`.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use async_trait::async_trait;
use openfga_domain::{
    ExpandCommand, InputLimits, Limit, ObjectRef, RelationshipTuple, SubjectRef, UsersetRef,
};
use openfga_model::{CompiledModel, NodeId, RelationId, RewriteNode};
use openfga_storage::{
    ConditionFilter, ObjectRelationFilter, OperationContext, ReadOptions, StorageCancellationToken,
    TupleReader,
};

use crate::{ExpandBudget, ListError, ListErrorKind, common::validate_query_model};

type NodeFuture<'a> = Pin<Box<dyn Future<Output = Result<ExpandNode, ListError>> + Send + 'a>>;
type ChildrenFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<[ExpandNode]>, ListError>> + Send + 'a>>;

/// Resource accounting for one completed `Expand` query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExpandMetadata {
    nodes: u32,
    datastore_queries: u32,
    tuple_items: u32,
    maximum_depth: u32,
    estimated_response_bytes: u32,
}

impl ExpandMetadata {
    /// Returns the number of materialized tree nodes.
    #[must_use]
    pub const fn nodes(self) -> u32 {
        self.nodes
    }

    /// Returns the number of forward datastore reads.
    #[must_use]
    pub const fn datastore_queries(self) -> u32 {
        self.datastore_queries
    }

    /// Returns the number of stored and contextual tuples inspected.
    #[must_use]
    pub const fn tuple_items(self) -> u32 {
        self.tuple_items
    }

    /// Returns the deepest materialized rewrite-tree path.
    #[must_use]
    pub const fn maximum_depth(self) -> u32 {
        self.maximum_depth
    }

    /// Returns the conservative protobuf response-size estimate.
    #[must_use]
    pub const fn estimated_response_bytes(self) -> u32 {
        self.estimated_response_bytes
    }
}

/// One typed diagnostic userset-tree node.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExpandNode {
    name: UsersetRef,
    value: ExpandNodeValue,
}

impl ExpandNode {
    /// Returns the object/relation described by this node.
    #[must_use]
    pub const fn name(&self) -> &UsersetRef {
        &self.name
    }

    /// Returns the typed node payload.
    #[must_use]
    pub const fn value(&self) -> &ExpandNodeValue {
        &self.value
    }
}

/// Baseline-compatible diagnostic expansion payload.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExpandNodeValue {
    /// Canonical direct subjects read for this object/relation.
    Users(Box<[SubjectRef]>),
    /// A same-object computed userset reference.
    Computed(UsersetRef),
    /// A tupleset and its canonical computed target references.
    TupleToUserset {
        /// The tupleset relation read from storage.
        tupleset: UsersetRef,
        /// Computed usersets derived from tupleset subjects.
        computed: Box<[UsersetRef]>,
    },
    /// Ordered union operands.
    Union(Box<[ExpandNode]>),
    /// Ordered intersection operands.
    Intersection(Box<[ExpandNode]>),
    /// Positive and subtractive operands.
    Difference {
        /// Positive operand.
        base: Box<ExpandNode>,
        /// Subtractive operand.
        subtract: Box<ExpandNode>,
    },
}

/// One bounded diagnostic expansion result.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExpandOutcome {
    root: ExpandNode,
    metadata: ExpandMetadata,
}

impl ExpandOutcome {
    /// Returns the root diagnostic node.
    #[must_use]
    pub const fn root(&self) -> &ExpandNode {
        &self.root
    }

    /// Returns finite query accounting.
    #[must_use]
    pub const fn metadata(&self) -> ExpandMetadata {
        self.metadata
    }
}

/// Object-safe diagnostic expansion contract.
#[async_trait]
pub trait ExpandEngine: Send + Sync {
    /// Constructs one bounded userset tree.
    ///
    /// # Errors
    ///
    /// Returns typed model, tuple, storage, cancellation, timeout, or finite
    /// resource failures without returning a partial tree.
    async fn expand(
        &self,
        command: &ExpandCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ExpandBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<ExpandOutcome, ListError>;
}

/// Correctness-first diagnostic tree engine.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DirectExpandEngine {
    input_limits: InputLimits,
}

impl DirectExpandEngine {
    /// Creates an engine using the transport's validated boundary limits.
    #[must_use]
    pub const fn new(input_limits: InputLimits) -> Self {
        Self { input_limits }
    }
}

impl Default for DirectExpandEngine {
    fn default() -> Self {
        Self::new(InputLimits::default())
    }
}

#[async_trait]
impl ExpandEngine for DirectExpandEngine {
    async fn expand(
        &self,
        command: &ExpandCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ExpandBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<ExpandOutcome, ListError> {
        validate_query_model(command.query(), &model)?;
        for tuple in command.query().contextual_tuples().as_slice() {
            model.validate_relationship_tuple(tuple)?;
        }
        let relation = model
            .relation_id(command.object().object_type(), command.relation())
            .map_err(|source| ListError::model("expand_relation_not_found", source))?;
        let root = model
            .relation(relation)
            .map_err(|source| ListError::model("expand_relation_invalid", source))?
            .root();
        let operation = OperationContext::new(
            command.query().consistency(),
            command.query().deadline(),
            cancellation,
        );
        let mut evaluator = Evaluator::new(
            command,
            model,
            tuples,
            budget,
            self.input_limits.clone(),
            operation,
        )?;
        evaluator.charge_response_bytes(8)?;
        let root = evaluator
            .expand_node(command.object().clone(), relation, root, BTreeSet::new(), 0)
            .await?;
        Ok(ExpandOutcome {
            root,
            metadata: ExpandMetadata {
                nodes: evaluator.counters.nodes,
                datastore_queries: evaluator.counters.datastore_queries,
                tuple_items: evaluator.counters.tuple_items,
                maximum_depth: evaluator.counters.maximum_depth,
                estimated_response_bytes: evaluator.counters.response_bytes,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RelationKey {
    object: ObjectRef,
    relation: RelationId,
}

#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    nodes: u32,
    datastore_queries: u32,
    tuple_items: u32,
    maximum_depth: u32,
    response_bytes: u32,
}

struct Evaluator<'a> {
    command: &'a ExpandCommand,
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: ExpandBudget,
    input_limits: InputLimits,
    operation: OperationContext,
    read_options: ReadOptions,
    tuple_cache: BTreeMap<RelationKey, Arc<[RelationshipTuple]>>,
    counters: Counters,
}

impl<'a> Evaluator<'a> {
    fn new(
        command: &'a ExpandCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ExpandBudget,
        input_limits: InputLimits,
        operation: OperationContext,
    ) -> Result<Self, ListError> {
        let maximum_per_read = Limit::<100_000>::new(budget.maximum_tuple_items().min(100_000))
            .map_err(|_| internal("expand_forward_read_limit_invalid"))?;
        let read_options = ReadOptions::from_limit(maximum_per_read);
        Ok(Self {
            command,
            model,
            tuples,
            budget,
            input_limits,
            operation,
            read_options,
            tuple_cache: BTreeMap::new(),
            counters: Counters::default(),
        })
    }

    fn expand_node(
        &mut self,
        object: ObjectRef,
        owning_relation: RelationId,
        node: NodeId,
        mut visited: BTreeSet<NodeId>,
        depth: u32,
    ) -> NodeFuture<'_> {
        Box::pin(async move {
            self.operation.check().map_err(ListError::from)?;
            let depth = depth.checked_add(1).ok_or_else(depth_exceeded)?;
            self.consume_node(depth)?;
            let name = self.userset_ref(object.clone(), owning_relation)?;
            self.charge_node(&name)?;
            if !visited.insert(node) {
                self.charge_reference(&name)?;
                return Ok(ExpandNode {
                    name: name.clone(),
                    value: ExpandNodeValue::Computed(name),
                });
            }
            let rewrite = self
                .model
                .node(node)
                .map_err(|source| ListError::model("expand_node_invalid", source))?
                .clone();
            let value = match rewrite {
                RewriteNode::Direct(relation) => {
                    let rows = self.read_relation(object, relation).await?;
                    let users = rows
                        .iter()
                        .map(|tuple| tuple.key().subject().clone())
                        .collect::<BTreeSet<_>>();
                    for user in &users {
                        self.charge_subject(user)?;
                    }
                    let mut users = users.into_iter().collect::<Vec<_>>();
                    users.sort_by(subject_canonical_cmp);
                    ExpandNodeValue::Users(users.into_boxed_slice())
                }
                RewriteNode::Computed(relation) => {
                    let computed = self.userset_ref(object, relation)?;
                    self.charge_reference(&computed)?;
                    ExpandNodeValue::Computed(computed)
                }
                RewriteNode::TupleToUserset {
                    tupleset, targets, ..
                } => self.expand_ttu(object, tupleset, &targets).await?,
                RewriteNode::Union(children) => ExpandNodeValue::Union(
                    self.expand_children(object, owning_relation, &children, visited, depth)
                        .await?,
                ),
                RewriteNode::Intersection(children) => ExpandNodeValue::Intersection(
                    self.expand_children(object, owning_relation, &children, visited, depth)
                        .await?,
                ),
                RewriteNode::Difference { base, subtract } => {
                    let base = self
                        .expand_node(
                            object.clone(),
                            owning_relation,
                            base,
                            visited.clone(),
                            depth,
                        )
                        .await?;
                    let subtract = self
                        .expand_node(object, owning_relation, subtract, visited, depth)
                        .await?;
                    ExpandNodeValue::Difference {
                        base: Box::new(base),
                        subtract: Box::new(subtract),
                    }
                }
                _ => return Err(internal("expand_rewrite_unsupported")),
            };
            Ok(ExpandNode { name, value })
        })
    }

    fn expand_children(
        &mut self,
        object: ObjectRef,
        owning_relation: RelationId,
        children: &[NodeId],
        visited: BTreeSet<NodeId>,
        depth: u32,
    ) -> ChildrenFuture<'_> {
        let children = children.to_vec();
        Box::pin(async move {
            let mut expanded = Vec::with_capacity(children.len());
            for child in children {
                expanded.push(
                    self.expand_node(
                        object.clone(),
                        owning_relation,
                        child,
                        visited.clone(),
                        depth,
                    )
                    .await?,
                );
            }
            Ok(expanded.into_boxed_slice())
        })
    }

    async fn expand_ttu(
        &mut self,
        object: ObjectRef,
        tupleset: RelationId,
        targets: &[RelationId],
    ) -> Result<ExpandNodeValue, ListError> {
        let tupleset_ref = self.userset_ref(object.clone(), tupleset)?;
        self.charge_reference(&tupleset_ref)?;
        let rows = self.read_relation(object, tupleset).await?;
        let mut computed = BTreeSet::new();
        for tuple in rows.iter() {
            let reference = match tuple.key().subject() {
                SubjectRef::Object(target) => self
                    .target_reference(target, targets)?
                    .ok_or_else(|| internal("expand_ttu_target_relation_missing"))?,
                SubjectRef::Userset(userset) => userset.clone(),
                SubjectRef::TypedWildcard(_) => {
                    return Err(internal("expand_ttu_wildcard_subject"));
                }
                _ => return Err(internal("expand_ttu_subject_unsupported")),
            };
            computed.insert(reference);
        }
        for reference in &computed {
            self.charge_reference(reference)?;
        }
        Ok(ExpandNodeValue::TupleToUserset {
            tupleset: tupleset_ref,
            computed: computed.into_iter().collect(),
        })
    }

    fn target_reference(
        &self,
        object: &ObjectRef,
        targets: &[RelationId],
    ) -> Result<Option<UsersetRef>, ListError> {
        for target in targets {
            let relation = self
                .model
                .relation(*target)
                .map_err(|source| ListError::model("expand_ttu_target_invalid", source))?;
            let target_type = self
                .model
                .type_name(relation.object_type())
                .map_err(|source| ListError::model("expand_ttu_target_type_invalid", source))?;
            if target_type == object.object_type() {
                return self.userset_ref(object.clone(), *target).map(Some);
            }
        }
        Ok(None)
    }

    async fn read_relation(
        &mut self,
        object: ObjectRef,
        relation: RelationId,
    ) -> Result<Arc<[RelationshipTuple]>, ListError> {
        let key = RelationKey {
            object: object.clone(),
            relation,
        };
        if let Some(cached) = self.tuple_cache.get(&key) {
            return Ok(Arc::clone(cached));
        }
        self.operation.check().map_err(ListError::from)?;
        self.counters.datastore_queries = self
            .counters
            .datastore_queries
            .checked_add(1)
            .ok_or_else(datastore_query_exceeded)?;
        if self.counters.datastore_queries > self.budget.maximum_datastore_queries() {
            return Err(datastore_query_exceeded());
        }
        let relation_name = self
            .model
            .relation(relation)
            .map_err(|source| ListError::model("expand_read_relation_invalid", source))?
            .name()
            .clone();
        let filter = ObjectRelationFilter::new(
            object.clone(),
            relation_name.clone(),
            Vec::new(),
            ConditionFilter::any(),
            &self.input_limits,
        )?;
        let mut stream = self
            .tuples
            .read_object_relation(
                &self.operation,
                self.command.query().store_id(),
                &filter,
                self.read_options,
            )
            .await?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next_item() {
            self.operation.check().map_err(ListError::from)?;
            rows.push(row?);
        }
        let contextual = self
            .command
            .query()
            .contextual_tuples()
            .as_slice()
            .iter()
            .filter(|tuple| {
                tuple.key().object() == &object && tuple.key().relation() == &relation_name
            })
            .cloned()
            .collect::<Vec<_>>();
        self.charge_tuple_items(
            rows.len()
                .checked_add(contextual.len())
                .ok_or_else(tuple_items_exceeded)?,
        )?;
        rows.retain(|tuple| self.model.validate_relationship_tuple(tuple).is_ok());
        rows.extend(contextual);
        let rows = Arc::<[RelationshipTuple]>::from(rows);
        self.tuple_cache.insert(key, Arc::clone(&rows));
        Ok(rows)
    }

    fn userset_ref(
        &self,
        object: ObjectRef,
        relation: RelationId,
    ) -> Result<UsersetRef, ListError> {
        let relation = self
            .model
            .relation(relation)
            .map_err(|source| ListError::model("expand_userset_relation_invalid", source))?
            .name()
            .clone();
        UsersetRef::new(object, relation, &self.input_limits)
            .map_err(|_| internal("expand_userset_render_invalid"))
    }

    fn consume_node(&mut self, depth: u32) -> Result<(), ListError> {
        if depth > self.budget.maximum_depth() {
            return Err(depth_exceeded());
        }
        self.counters.maximum_depth = self.counters.maximum_depth.max(depth);
        self.counters.nodes = self
            .counters
            .nodes
            .checked_add(1)
            .ok_or_else(node_exceeded)?;
        if self.counters.nodes > self.budget.maximum_nodes() {
            return Err(node_exceeded());
        }
        Ok(())
    }

    fn charge_tuple_items(&mut self, count: usize) -> Result<(), ListError> {
        let count = u32::try_from(count).map_err(|_| tuple_items_exceeded())?;
        self.counters.tuple_items = self
            .counters
            .tuple_items
            .checked_add(count)
            .ok_or_else(tuple_items_exceeded)?;
        if self.counters.tuple_items > self.budget.maximum_tuple_items() {
            return Err(tuple_items_exceeded());
        }
        Ok(())
    }

    fn charge_node(&mut self, name: &UsersetRef) -> Result<(), ListError> {
        self.charge_rendered_bytes(16, userset_rendered_bytes(name)?)
    }

    fn charge_subject(&mut self, subject: &SubjectRef) -> Result<(), ListError> {
        let rendered_bytes = match subject {
            SubjectRef::Object(object) => object_rendered_bytes(object)?,
            SubjectRef::Userset(userset) => userset_rendered_bytes(userset)?,
            SubjectRef::TypedWildcard(object_type) => object_type
                .as_str()
                .len()
                .checked_add(2)
                .ok_or_else(response_size_exceeded)?,
            _ => return Err(internal("expand_subject_size_unsupported")),
        };
        self.charge_rendered_bytes(8, rendered_bytes)
    }

    fn charge_reference(&mut self, userset: &UsersetRef) -> Result<(), ListError> {
        self.charge_rendered_bytes(8, userset_rendered_bytes(userset)?)
    }

    fn charge_rendered_bytes(
        &mut self,
        envelope_bytes: u32,
        rendered_bytes: usize,
    ) -> Result<(), ListError> {
        let rendered_bytes = u32::try_from(rendered_bytes).map_err(|_| response_size_exceeded())?;
        let bytes = envelope_bytes
            .checked_add(rendered_bytes)
            .ok_or_else(response_size_exceeded)?;
        self.charge_response_bytes(bytes)
    }

    fn charge_response_bytes(&mut self, bytes: u32) -> Result<(), ListError> {
        self.counters.response_bytes = self
            .counters
            .response_bytes
            .checked_add(bytes)
            .ok_or_else(response_size_exceeded)?;
        if self.counters.response_bytes > self.budget.maximum_response_bytes() {
            return Err(response_size_exceeded());
        }
        Ok(())
    }
}

fn object_rendered_bytes(object: &ObjectRef) -> Result<usize, ListError> {
    object
        .object_type()
        .as_str()
        .len()
        .checked_add(1)
        .and_then(|bytes| bytes.checked_add(object.object_id().as_str().len()))
        .ok_or_else(response_size_exceeded)
}

fn userset_rendered_bytes(userset: &UsersetRef) -> Result<usize, ListError> {
    object_rendered_bytes(userset.object())?
        .checked_add(1)
        .and_then(|bytes| bytes.checked_add(userset.relation().as_str().len()))
        .ok_or_else(response_size_exceeded)
}

fn subject_canonical_cmp(left: &SubjectRef, right: &SubjectRef) -> Ordering {
    left.subject_type()
        .cmp(right.subject_type())
        .then_with(|| left.object_id().cmp(right.object_id()))
        .then_with(|| left.relation().cmp(&right.relation()))
}

const fn internal(code: &'static str) -> ListError {
    ListError::new(ListErrorKind::Internal, code)
}

const fn depth_exceeded() -> ListError {
    ListError::new(ListErrorKind::DepthExceeded, "expand_depth_exceeded")
}

const fn node_exceeded() -> ListError {
    ListError::new(ListErrorKind::NodeExceeded, "expand_nodes_exceeded")
}

const fn datastore_query_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::DatastoreQueryExceeded,
        "expand_datastore_query_exceeded",
    )
}

const fn tuple_items_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::TupleItemExceeded,
        "expand_tuple_items_exceeded",
    )
}

const fn response_size_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::ResponseSizeExceeded,
        "expand_response_size_exceeded",
    )
}
