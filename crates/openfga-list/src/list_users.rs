//! Bounded forward `ListUsers` expansion with symbolic wildcard set algebra.
//!
//! `async-trait` preserves object safety because the service layer owns the
//! engine through `Arc<dyn ListUsersEngine>`.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
};

use async_trait::async_trait;
use openfga_condition::{CancellationCheck, EvaluationBudget, EvaluationErrorKind};
use openfga_domain::{
    ConditionReference, InputLimits, ListUsersCommand, ObjectRef, RelationshipTuple, SubjectRef,
    UserTypeFilter, UsersetRef,
};
use openfga_model::{CompiledModel, NodeId, RelationId, RewriteNode};
use openfga_storage::{
    ConditionFilter, ObjectRelationFilter, OperationContext, ReadOptions, StorageCancellationToken,
    TupleReader,
};

use crate::{ListError, ListErrorKind, ListUsersBudget, common::validate_query_model};

type ExpansionFuture<'a> = Pin<Box<dyn Future<Output = Result<Expansion, ListError>> + Send + 'a>>;

/// Resource accounting for one completed `ListUsers` query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ListUsersMetadata {
    dispatches: u32,
    datastore_queries: u32,
    tuple_items: u32,
    condition_cost: u32,
    maximum_depth: u32,
    results: u32,
    truncated: bool,
}

impl ListUsersMetadata {
    /// Returns rewrite and recursive userset dispatches.
    #[must_use]
    pub const fn dispatches(self) -> u32 {
        self.dispatches
    }

    /// Returns forward datastore reads.
    #[must_use]
    pub const fn datastore_queries(self) -> u32 {
        self.datastore_queries
    }

    /// Returns stored and contextual tuple rows inspected.
    #[must_use]
    pub const fn tuple_items(self) -> u32 {
        self.tuple_items
    }

    /// Returns cumulative CEL evaluation cost.
    #[must_use]
    pub const fn condition_cost(self) -> u32 {
        self.condition_cost
    }

    /// Returns the deepest recursively expanded userset path.
    #[must_use]
    pub const fn maximum_depth(self) -> u32 {
        self.maximum_depth
    }

    /// Returns the number of subjects in the public result.
    #[must_use]
    pub const fn results(self) -> u32 {
        self.results
    }

    /// Returns whether the public result ceiling truncated the final set.
    #[must_use]
    pub const fn truncated(self) -> bool {
        self.truncated
    }
}

/// One canonical bounded `ListUsers` result.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ListUsersOutcome {
    users: Box<[SubjectRef]>,
    metadata: ListUsersMetadata,
}

impl ListUsersOutcome {
    /// Returns canonical deduplicated subjects.
    #[must_use]
    pub const fn users(&self) -> &[SubjectRef] {
        &self.users
    }

    /// Returns finite query accounting.
    #[must_use]
    pub const fn metadata(&self) -> ListUsersMetadata {
        self.metadata
    }
}

/// Object-safe forward enumeration contract.
#[async_trait]
pub trait ListUsersEngine: Send + Sync {
    /// Expands one validated object/relation into filtered subjects.
    ///
    /// # Errors
    ///
    /// Returns typed model, tuple, condition, storage, cancellation, timeout,
    /// or independent resource failures without partial results.
    async fn list_users(
        &self,
        command: &ListUsersCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ListUsersBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<ListUsersOutcome, ListError>;
}

/// Correctness-first forward expansion engine.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DirectListUsersEngine {
    input_limits: InputLimits,
}

impl DirectListUsersEngine {
    /// Creates an engine using the transport's validated boundary limits.
    #[must_use]
    pub const fn new(input_limits: InputLimits) -> Self {
        Self { input_limits }
    }
}

impl Default for DirectListUsersEngine {
    fn default() -> Self {
        Self::new(InputLimits::default())
    }
}

#[async_trait]
impl ListUsersEngine for DirectListUsersEngine {
    async fn list_users(
        &self,
        command: &ListUsersCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ListUsersBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<ListUsersOutcome, ListError> {
        validate_query_model(command.query(), &model)?;
        for tuple in command.query().contextual_tuples().as_slice() {
            model.validate_relationship_tuple(tuple)?;
        }
        let root = model
            .relation_id(command.object().object_type(), command.relation())
            .map_err(|source| ListError::model("list_users_relation_not_found", source))?;
        validate_filters(command, &model)?;
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
        let mut users = BTreeSet::new();
        for filter in command.filters().as_slice() {
            let expansion = evaluator
                .expand_relation(
                    command.object().clone(),
                    root,
                    filter.clone(),
                    BTreeSet::new(),
                    0,
                )
                .await?;
            users.extend(expansion.set.into_subjects(filter));
            evaluator.check_subject_count(users.len())?;
        }
        let maximum_results = command.control().maximum_results().get() as usize;
        let truncated = users.len() > maximum_results;
        let users = users
            .into_iter()
            .take(maximum_results)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let results = u32::try_from(users.len()).map_err(|_| subject_exceeded())?;
        Ok(ListUsersOutcome {
            users,
            metadata: ListUsersMetadata {
                dispatches: evaluator.counters.dispatches,
                datastore_queries: evaluator.counters.datastore_queries,
                tuple_items: evaluator.counters.tuple_items,
                condition_cost: evaluator.counters.condition_cost,
                maximum_depth: evaluator.counters.maximum_depth,
                results,
                truncated,
            },
        })
    }
}

fn validate_filters(command: &ListUsersCommand, model: &CompiledModel) -> Result<(), ListError> {
    for filter in command.filters().as_slice() {
        model
            .type_id(filter.user_type())
            .map_err(|source| ListError::model("list_users_filter_type_not_found", source))?;
        if let Some(relation) = filter.relation() {
            model
                .relation_id(filter.user_type(), relation)
                .map_err(|source| {
                    ListError::model("list_users_filter_relation_not_found", source)
                })?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RelationKey {
    object: ObjectRef,
    relation: RelationId,
}

#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    dispatches: u32,
    datastore_queries: u32,
    tuple_items: u32,
    condition_cost: u32,
    maximum_depth: u32,
}

struct Evaluator<'a> {
    command: &'a ListUsersCommand,
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: ListUsersBudget,
    input_limits: InputLimits,
    operation: OperationContext,
    read_options: ReadOptions,
    tuple_cache: BTreeMap<RelationKey, Arc<[RelationshipTuple]>>,
    counters: Counters,
}

impl<'a> Evaluator<'a> {
    fn new(
        command: &'a ListUsersCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: ListUsersBudget,
        input_limits: InputLimits,
        operation: OperationContext,
    ) -> Result<Self, ListError> {
        let maximum_per_read = budget.maximum_tuple_items().min(input_limits.results());
        let maximum_per_read = NonZeroU32::new(maximum_per_read)
            .ok_or_else(|| internal("list_users_forward_read_limit_zero"))?;
        let read_options = ReadOptions::new(maximum_per_read, &input_limits)?;
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

    fn expand_relation(
        &mut self,
        object: ObjectRef,
        relation: RelationId,
        filter: UserTypeFilter,
        mut visited: BTreeSet<RelationKey>,
        depth: u32,
    ) -> ExpansionFuture<'_> {
        Box::pin(async move {
            self.operation.check().map_err(ListError::from)?;
            self.consume_dispatch()?;
            let depth = depth.checked_add(1).ok_or_else(depth_exceeded)?;
            self.check_depth(depth)?;
            let key = RelationKey {
                object: object.clone(),
                relation,
            };
            if !visited.insert(key) {
                return Ok(Expansion::cycle());
            }
            let compiled = self
                .model
                .relation(relation)
                .map_err(|source| ListError::model("list_users_relation_invalid", source))?;
            let relation_name = compiled.name().clone();
            let root = compiled.root();
            let mut set = if filter.user_type() == object.object_type()
                && filter.relation() == Some(&relation_name)
            {
                let userset = UsersetRef::new(object.clone(), relation_name, &self.input_limits)
                    .map_err(|_| internal("list_users_userset_render_invalid"))?;
                SymbolicSet::singleton(SubjectRef::Userset(userset))
            } else {
                SymbolicSet::empty()
            };
            let rewrite = self
                .expand_node(object, root, filter, visited, depth)
                .await?;
            set = set.union(rewrite.set);
            self.check_set(&set)?;
            Ok(Expansion {
                set,
                has_cycle: rewrite.has_cycle,
            })
        })
    }

    fn expand_node(
        &mut self,
        object: ObjectRef,
        node: NodeId,
        filter: UserTypeFilter,
        visited: BTreeSet<RelationKey>,
        depth: u32,
    ) -> ExpansionFuture<'_> {
        Box::pin(async move {
            self.operation.check().map_err(ListError::from)?;
            self.consume_dispatch()?;
            let rewrite = self
                .model
                .node(node)
                .map_err(|source| ListError::model("list_users_node_invalid", source))?
                .clone();
            match rewrite {
                RewriteNode::Direct(relation) => {
                    self.expand_direct(object, relation, filter, visited, depth)
                        .await
                }
                RewriteNode::Computed(relation) => {
                    self.expand_relation(object, relation, filter, visited, depth)
                        .await
                }
                RewriteNode::TupleToUserset {
                    tupleset, targets, ..
                } => {
                    self.expand_ttu(object, tupleset, &targets, filter, visited, depth)
                        .await
                }
                RewriteNode::Union(children) => {
                    let mut set = SymbolicSet::empty();
                    for child in children {
                        let child = self
                            .expand_node(
                                object.clone(),
                                child,
                                filter.clone(),
                                visited.clone(),
                                depth,
                            )
                            .await?;
                        set = set.union(child.set);
                        self.check_set(&set)?;
                    }
                    Ok(Expansion::set(set))
                }
                RewriteNode::Intersection(children) => {
                    self.expand_intersection(object, &children, filter, visited, depth)
                        .await
                }
                RewriteNode::Difference { base, subtract } => {
                    let base = self
                        .expand_node(object.clone(), base, filter.clone(), visited.clone(), depth)
                        .await?;
                    let subtract = self
                        .expand_node(object, subtract, filter, visited, depth)
                        .await?;
                    if subtract.has_cycle {
                        return Ok(Expansion::set(SymbolicSet::empty()));
                    }
                    let set = base.set.difference(subtract.set);
                    self.check_set(&set)?;
                    Ok(Expansion::set(set))
                }
                _ => Err(internal("list_users_rewrite_unsupported")),
            }
        })
    }

    fn expand_direct(
        &mut self,
        object: ObjectRef,
        relation: RelationId,
        filter: UserTypeFilter,
        visited: BTreeSet<RelationKey>,
        depth: u32,
    ) -> ExpansionFuture<'_> {
        Box::pin(async move {
            let rows = self.read_relation(object, relation).await?;
            let mut set = SymbolicSet::empty();
            let mut has_cycle = false;
            for tuple in rows.iter() {
                if !self.condition_met(tuple)? {
                    continue;
                }
                match tuple.key().subject() {
                    SubjectRef::Userset(userset) => {
                        let relation = self
                            .model
                            .relation_id(userset.object().object_type(), userset.relation())
                            .map_err(|source| {
                                ListError::model("list_users_direct_userset_invalid", source)
                            })?;
                        let expanded = self
                            .expand_relation(
                                userset.object().clone(),
                                relation,
                                filter.clone(),
                                visited.clone(),
                                depth,
                            )
                            .await?;
                        set = set.union(expanded.set);
                        has_cycle |= expanded.has_cycle;
                    }
                    SubjectRef::TypedWildcard(_)
                        if filter_matches(&filter, tuple.key().subject()) =>
                    {
                        set = set.union(SymbolicSet::Cofinite(BTreeSet::new()));
                    }
                    subject if filter_matches(&filter, subject) => {
                        set = set.union(SymbolicSet::singleton(subject.clone()));
                    }
                    SubjectRef::Object(_) | SubjectRef::TypedWildcard(_) => {}
                    _ => return Err(internal("list_users_subject_unsupported")),
                }
                self.check_set(&set)?;
            }
            Ok(Expansion { set, has_cycle })
        })
    }

    fn expand_ttu(
        &mut self,
        object: ObjectRef,
        tupleset: RelationId,
        targets: &[RelationId],
        filter: UserTypeFilter,
        visited: BTreeSet<RelationKey>,
        depth: u32,
    ) -> ExpansionFuture<'_> {
        let targets = targets.to_vec();
        Box::pin(async move {
            let rows = self.read_relation(object, tupleset).await?;
            let mut set = SymbolicSet::empty();
            for tuple in rows.iter() {
                if !self.condition_met(tuple)? {
                    continue;
                }
                let SubjectRef::Object(target_object) = tuple.key().subject() else {
                    continue;
                };
                for target in &targets {
                    let relation = self.model.relation(*target).map_err(|source| {
                        ListError::model("list_users_ttu_target_invalid", source)
                    })?;
                    let target_type =
                        self.model
                            .type_name(relation.object_type())
                            .map_err(|source| {
                                ListError::model("list_users_ttu_target_type_invalid", source)
                            })?;
                    if target_type != target_object.object_type() {
                        continue;
                    }
                    let expanded = self
                        .expand_relation(
                            target_object.clone(),
                            *target,
                            filter.clone(),
                            visited.clone(),
                            depth,
                        )
                        .await?;
                    set = set.union(expanded.set);
                    self.check_set(&set)?;
                }
            }
            Ok(Expansion::set(set))
        })
    }

    fn expand_intersection(
        &mut self,
        object: ObjectRef,
        children: &[NodeId],
        filter: UserTypeFilter,
        visited: BTreeSet<RelationKey>,
        depth: u32,
    ) -> ExpansionFuture<'_> {
        let children = children.to_vec();
        Box::pin(async move {
            let mut children = children.into_iter();
            let Some(first) = children.next() else {
                return Err(internal("list_users_intersection_empty"));
            };
            let mut set = self
                .expand_node(
                    object.clone(),
                    first,
                    filter.clone(),
                    visited.clone(),
                    depth,
                )
                .await?
                .set;
            for child in children {
                let child = self
                    .expand_node(
                        object.clone(),
                        child,
                        filter.clone(),
                        visited.clone(),
                        depth,
                    )
                    .await?;
                set = set.intersection(child.set);
                self.check_set(&set)?;
            }
            Ok(Expansion::set(set))
        })
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
            .map_err(|source| ListError::model("list_users_read_relation_invalid", source))?
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
            rows.push(row?);
        }
        if rows.len() == self.read_options.maximum_results() {
            return Err(ListError::new(
                ListErrorKind::TupleItemExceeded,
                "list_users_forward_read_may_be_truncated",
            ));
        }
        rows.extend(
            self.command
                .query()
                .contextual_tuples()
                .as_slice()
                .iter()
                .filter(|tuple| {
                    tuple.key().object() == &object && tuple.key().relation() == &relation_name
                })
                .cloned(),
        );
        self.charge_tuple_items(rows.len())?;
        for tuple in &rows {
            self.model.validate_relationship_tuple(tuple)?;
        }
        let rows = Arc::<[RelationshipTuple]>::from(rows);
        self.tuple_cache.insert(key, Arc::clone(&rows));
        Ok(rows)
    }

    fn condition_met(&mut self, tuple: &RelationshipTuple) -> Result<bool, ListError> {
        let ConditionReference::Conditional(binding) = tuple.condition() else {
            return Ok(true);
        };
        self.operation.check().map_err(ListError::from)?;
        let condition_id = self
            .model
            .condition_id(binding.name())
            .map_err(|source| ListError::model("list_users_condition_missing", source))?;
        let condition = self
            .model
            .condition(condition_id)
            .map_err(|source| ListError::model("list_users_condition_invalid", source))?;
        let remaining = self
            .budget
            .maximum_condition_cost()
            .checked_sub(self.counters.condition_cost)
            .filter(|remaining| *remaining > 0)
            .ok_or_else(condition_cost_exceeded)?;
        let condition_budget =
            EvaluationBudget::new(u64::from(remaining)).map_err(|_| condition_cost_exceeded())?;
        let cancellation = OperationCancellation(&self.operation);
        let evaluated = condition.evaluate(
            self.command.query().condition_context(),
            binding.context(),
            condition_budget,
            &cancellation,
        );
        self.operation.check().map_err(ListError::from)?;
        match evaluated {
            Ok(outcome) => {
                let cost = u32::try_from(outcome.cost()).map_err(|_| condition_cost_exceeded())?;
                self.counters.condition_cost = self
                    .counters
                    .condition_cost
                    .checked_add(cost)
                    .ok_or_else(condition_cost_exceeded)?;
                if self.counters.condition_cost > self.budget.maximum_condition_cost() {
                    return Err(condition_cost_exceeded());
                }
                Ok(outcome.condition_met())
            }
            Err(error) if error.kind() == EvaluationErrorKind::CostExceeded => {
                Err(condition_cost_exceeded())
            }
            Err(error) if error.kind() == EvaluationErrorKind::Cancelled => {
                self.operation.check().map_err(ListError::from)?;
                Err(ListError::condition(
                    "list_users_condition_cancelled",
                    error,
                ))
            }
            Err(error) => Err(ListError::condition(
                "list_users_condition_evaluation_failed",
                error,
            )),
        }
    }

    fn consume_dispatch(&mut self) -> Result<(), ListError> {
        self.counters.dispatches = self
            .counters
            .dispatches
            .checked_add(1)
            .ok_or_else(dispatch_exceeded)?;
        if self.counters.dispatches > self.budget.maximum_dispatches() {
            return Err(dispatch_exceeded());
        }
        Ok(())
    }

    fn check_depth(&mut self, depth: u32) -> Result<(), ListError> {
        if depth > self.budget.maximum_depth() {
            return Err(depth_exceeded());
        }
        self.counters.maximum_depth = self.counters.maximum_depth.max(depth);
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

    fn check_set(&self, set: &SymbolicSet<SubjectRef>) -> Result<(), ListError> {
        self.check_subject_count(set.tracked_len())
    }

    fn check_subject_count(&self, count: usize) -> Result<(), ListError> {
        let count = u32::try_from(count).map_err(|_| subject_exceeded())?;
        if count > self.budget.maximum_subjects() {
            return Err(subject_exceeded());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct OperationCancellation<'a>(&'a OperationContext);

impl CancellationCheck for OperationCancellation<'_> {
    fn is_cancelled(&self) -> bool {
        self.0.check().is_err()
    }
}

#[derive(Debug)]
struct Expansion {
    set: SymbolicSet<SubjectRef>,
    has_cycle: bool,
}

impl Expansion {
    const fn set(set: SymbolicSet<SubjectRef>) -> Self {
        Self {
            set,
            has_cycle: false,
        }
    }

    fn cycle() -> Self {
        Self {
            set: SymbolicSet::empty(),
            has_cycle: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SymbolicSet<T: Ord> {
    Finite(BTreeSet<T>),
    Cofinite(BTreeSet<T>),
}

impl<T: Ord> SymbolicSet<T> {
    const fn empty() -> Self {
        Self::Finite(BTreeSet::new())
    }

    fn singleton(value: T) -> Self {
        Self::Finite(BTreeSet::from([value]))
    }

    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Finite(mut left), Self::Finite(mut right)) => {
                left.append(&mut right);
                Self::Finite(left)
            }
            (Self::Cofinite(mut excluded), Self::Finite(included))
            | (Self::Finite(included), Self::Cofinite(mut excluded)) => {
                excluded.retain(|value| !included.contains(value));
                Self::Cofinite(excluded)
            }
            (Self::Cofinite(mut left), Self::Cofinite(right)) => {
                left.retain(|value| right.contains(value));
                Self::Cofinite(left)
            }
        }
    }

    fn intersection(self, other: Self) -> Self {
        match (self, other) {
            (Self::Finite(mut left), Self::Finite(right)) => {
                left.retain(|value| right.contains(value));
                Self::Finite(left)
            }
            (Self::Cofinite(excluded), Self::Finite(mut included))
            | (Self::Finite(mut included), Self::Cofinite(excluded)) => {
                included.retain(|value| !excluded.contains(value));
                Self::Finite(included)
            }
            (Self::Cofinite(mut left), Self::Cofinite(mut right)) => {
                left.append(&mut right);
                Self::Cofinite(left)
            }
        }
    }

    fn difference(self, other: Self) -> Self {
        match (self, other) {
            (Self::Finite(mut left), Self::Finite(right)) => {
                left.retain(|value| !right.contains(value));
                Self::Finite(left)
            }
            (Self::Finite(mut included), Self::Cofinite(excluded)) => {
                included.retain(|value| excluded.contains(value));
                Self::Finite(included)
            }
            (Self::Cofinite(mut excluded), Self::Finite(mut removed)) => {
                excluded.append(&mut removed);
                Self::Cofinite(excluded)
            }
            (Self::Cofinite(left_excluded), Self::Cofinite(mut right_excluded)) => {
                right_excluded.retain(|value| !left_excluded.contains(value));
                Self::Finite(right_excluded)
            }
        }
    }

    fn tracked_len(&self) -> usize {
        match self {
            Self::Finite(values) | Self::Cofinite(values) => values.len(),
        }
    }
}

impl SymbolicSet<SubjectRef> {
    fn into_subjects(self, filter: &UserTypeFilter) -> BTreeSet<SubjectRef> {
        match self {
            Self::Finite(subjects) => subjects,
            Self::Cofinite(_) => {
                BTreeSet::from([SubjectRef::TypedWildcard(filter.user_type().clone())])
            }
        }
    }
}

fn filter_matches(filter: &UserTypeFilter, subject: &SubjectRef) -> bool {
    if filter.user_type() != subject.subject_type() {
        return false;
    }
    match (filter.relation(), subject) {
        (None, SubjectRef::Object(_) | SubjectRef::TypedWildcard(_)) => true,
        (Some(expected), SubjectRef::Userset(userset)) => expected == userset.relation(),
        _ => false,
    }
}

const fn internal(code: &'static str) -> ListError {
    ListError::new(ListErrorKind::Internal, code)
}

const fn depth_exceeded() -> ListError {
    ListError::new(ListErrorKind::DepthExceeded, "list_users_depth_exceeded")
}

const fn dispatch_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::DispatchExceeded,
        "list_users_dispatch_exceeded",
    )
}

const fn datastore_query_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::DatastoreQueryExceeded,
        "list_users_datastore_query_exceeded",
    )
}

const fn tuple_items_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::TupleItemExceeded,
        "list_users_tuple_items_exceeded",
    )
}

const fn subject_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::SubjectExceeded,
        "list_users_subjects_exceeded",
    )
}

const fn condition_cost_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::ConditionCostExceeded,
        "list_users_condition_cost_exceeded",
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::SymbolicSet;

    proptest! {
        #[test]
        fn test_should_match_finite_universe_set_algebra(
            left_values in proptest::collection::btree_set(0_u8..8, 0..8),
            right_values in proptest::collection::btree_set(0_u8..8, 0..8),
            left_wildcard in any::<bool>(),
            right_wildcard in any::<bool>(),
        ) {
            let universe = (0_u8..8).collect::<BTreeSet<_>>();
            let left = symbolic(&universe, &left_values, left_wildcard);
            let right = symbolic(&universe, &right_values, right_wildcard);
            let left_concrete = concrete(&universe, &left);
            let right_concrete = concrete(&universe, &right);

            prop_assert_eq!(
                concrete(&universe, &left.clone().union(right.clone())),
                left_concrete.union(&right_concrete).copied().collect(),
            );
            prop_assert_eq!(
                concrete(&universe, &left.clone().intersection(right.clone())),
                left_concrete.intersection(&right_concrete).copied().collect(),
            );
            prop_assert_eq!(
                concrete(&universe, &left.difference(right)),
                left_concrete.difference(&right_concrete).copied().collect(),
            );
        }
    }

    fn symbolic(universe: &BTreeSet<u8>, values: &BTreeSet<u8>, wildcard: bool) -> SymbolicSet<u8> {
        if wildcard {
            SymbolicSet::Cofinite(universe.difference(values).copied().collect())
        } else {
            SymbolicSet::Finite(values.clone())
        }
    }

    fn concrete(universe: &BTreeSet<u8>, set: &SymbolicSet<u8>) -> BTreeSet<u8> {
        match set {
            SymbolicSet::Finite(values) => values.clone(),
            SymbolicSet::Cofinite(excluded) => universe.difference(excluded).copied().collect(),
        }
    }
}
