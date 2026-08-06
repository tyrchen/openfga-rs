//! Conservative reverse candidate fixpoint traversal.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroU32,
    sync::Arc,
};

use openfga_domain::{
    ConditionReference, InputLimits, ListObjectsCommand, ModelSelection, ObjectRef, SubjectRef,
    UsersetRef,
};
use openfga_model::{CompiledModel, NodeId, RelationId, RestrictionKind, RewriteNode};
use openfga_storage::{
    ConditionFilter, OperationContext, ReadOptions, ReverseTupleFilter, StorageCancellationToken,
    TupleReader,
};

use crate::{CandidateBudget, ListError, ListErrorKind};

/// One canonical object discovered by conservative reverse traversal.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Candidate {
    object: ObjectRef,
    requires_check: bool,
}

impl Candidate {
    /// Returns the candidate object.
    #[must_use]
    pub const fn object(&self) -> &ObjectRef {
        &self.object
    }

    /// Returns whether ambiguous semantics require residual oracle evaluation.
    #[must_use]
    pub const fn requires_check(&self) -> bool {
        self.requires_check
    }
}

/// Resource accounting for one completed reverse traversal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CandidateMetadata {
    dispatches: u32,
    datastore_queries: u32,
    tuple_items: u32,
    intermediate_candidates: u32,
    maximum_depth: u32,
}

impl CandidateMetadata {
    /// Returns queued graph and propagation work.
    #[must_use]
    pub const fn dispatches(self) -> u32 {
        self.dispatches
    }

    /// Returns reverse datastore calls.
    #[must_use]
    pub const fn datastore_queries(self) -> u32 {
        self.datastore_queries
    }

    /// Returns stored and contextual tuple rows inspected.
    #[must_use]
    pub const fn tuple_items(self) -> u32 {
        self.tuple_items
    }

    /// Returns distinct candidates inserted across all relation states.
    #[must_use]
    pub const fn intermediate_candidates(self) -> u32 {
        self.intermediate_candidates
    }

    /// Returns the deepest derived candidate path.
    #[must_use]
    pub const fn maximum_depth(self) -> u32 {
        self.maximum_depth
    }
}

/// Canonical deduplicated candidates and traversal accounting.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CandidateSet {
    candidates: Box<[Candidate]>,
    metadata: CandidateMetadata,
}

impl CandidateSet {
    /// Returns candidates in canonical object order.
    #[must_use]
    pub const fn candidates(&self) -> &[Candidate] {
        &self.candidates
    }

    /// Returns finite traversal accounting.
    #[must_use]
    pub const fn metadata(&self) -> CandidateMetadata {
        self.metadata
    }
}

/// Stateless conservative reverse candidate traversal.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReverseCandidateTraversal {
    input_limits: InputLimits,
}

impl ReverseCandidateTraversal {
    /// Creates a traversal using the transport's validated input limits.
    #[must_use]
    pub const fn new(input_limits: InputLimits) -> Self {
        Self { input_limits }
    }

    /// Discovers every reachable candidate using only exact indexed reverse reads.
    ///
    /// Intersection, difference, and conditional paths are marked for residual
    /// Check. Internal safety limits return typed errors instead of truncated
    /// candidate sets.
    ///
    /// # Errors
    ///
    /// Returns model/tuple validation, storage, cancellation, deadline, or
    /// independent traversal-budget failures.
    pub async fn traverse(
        &self,
        command: &ListObjectsCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CandidateBudget,
        cancellation: StorageCancellationToken,
    ) -> Result<CandidateSet, ListError> {
        validate_model(command, &model)?;
        for tuple in command.query().contextual_tuples().as_slice() {
            model.validate_relationship_tuple(tuple)?;
        }
        let relation = model
            .relation_id(command.object_type(), command.relation())
            .map_err(|source| ListError::model("list_relation_not_found", source))?;
        let operation = OperationContext::new(
            command.query().consistency(),
            command.query().deadline(),
            cancellation,
        );
        Traversal::new(
            command,
            model,
            tuples,
            budget,
            self.input_limits.clone(),
            operation,
            relation,
        )?
        .run()
        .await
    }
}

impl Default for ReverseCandidateTraversal {
    fn default() -> Self {
        Self::new(InputLimits::default())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StateKey {
    relation: RelationId,
    subject: SubjectRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CandidateInfo {
    requires_check: bool,
    depth: u32,
}

impl CandidateInfo {
    fn merge(&mut self, other: Self) -> bool {
        let changed = (!self.requires_check && other.requires_check) || other.depth < self.depth;
        self.requires_check |= other.requires_check;
        self.depth = self.depth.min(other.depth);
        changed
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Transform {
    SameObject,
    ReverseObject(RelationId),
    ReverseUserset {
        owner: RelationId,
        subject_relation: RelationId,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Subscriber {
    parent: StateKey,
    transform: Transform,
    ambiguous: bool,
}

#[derive(Debug, Default)]
struct RelationState {
    expanded: bool,
    objects: BTreeMap<ObjectRef, CandidateInfo>,
    subscribers: BTreeSet<Subscriber>,
}

#[derive(Clone, Debug)]
enum Event {
    Expand(StateKey),
    Publish {
        state: StateKey,
        object: ObjectRef,
        info: CandidateInfo,
    },
    Apply {
        subscriber: Subscriber,
        object: ObjectRef,
        info: CandidateInfo,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PendingKey {
    parent: StateKey,
    relation: RelationId,
}

#[derive(Debug, Default)]
struct Counters {
    dispatches: u32,
    datastore_queries: u32,
    tuple_items: u32,
    intermediate_candidates: u32,
    maximum_depth: u32,
}

struct Traversal<'a> {
    command: &'a ListObjectsCommand,
    model: Arc<CompiledModel>,
    tuples: Arc<dyn TupleReader>,
    budget: CandidateBudget,
    input_limits: InputLimits,
    operation: OperationContext,
    root: StateKey,
    states: BTreeMap<StateKey, RelationState>,
    events: VecDeque<Event>,
    pending: BTreeMap<PendingKey, BTreeMap<SubjectRef, CandidateInfo>>,
    counters: Counters,
    read_options: ReadOptions,
}

impl fmt::Debug for Traversal<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Traversal")
            .field("model", &self.model)
            .field("budget", &self.budget)
            .field("root", &self.root)
            .field("states", &self.states.len())
            .field("events", &self.events.len())
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

use std::fmt;

impl<'a> Traversal<'a> {
    #[allow(
        clippy::too_many_arguments,
        reason = "all traversal capabilities are explicit"
    )]
    fn new(
        command: &'a ListObjectsCommand,
        model: Arc<CompiledModel>,
        tuples: Arc<dyn TupleReader>,
        budget: CandidateBudget,
        input_limits: InputLimits,
        operation: OperationContext,
        relation: RelationId,
    ) -> Result<Self, ListError> {
        let maximum_per_read = budget.maximum_tuple_items().min(input_limits.results());
        let maximum_per_read = NonZeroU32::new(maximum_per_read)
            .ok_or_else(|| internal("list_reverse_read_limit_zero"))?;
        let read_options = ReadOptions::new(maximum_per_read, &input_limits)?;
        let root = StateKey {
            relation,
            subject: command.subject().clone(),
        };
        let mut traversal = Self {
            command,
            model,
            tuples,
            budget,
            input_limits,
            operation,
            root: root.clone(),
            states: BTreeMap::new(),
            events: VecDeque::new(),
            pending: BTreeMap::new(),
            counters: Counters::default(),
            read_options,
        };
        traversal.ensure_state(root)?;
        Ok(traversal)
    }

    async fn run(mut self) -> Result<CandidateSet, ListError> {
        while !self.events.is_empty() || !self.pending.is_empty() {
            self.operation.check().map_err(ListError::from)?;
            while let Some(event) = self.events.pop_front() {
                self.process_event(event)?;
                self.operation.check().map_err(ListError::from)?;
            }
            if let Some((key, subjects)) = self.pending.pop_first() {
                self.process_reverse_read(key, subjects).await?;
            }
        }
        let state = self
            .states
            .remove(&self.root)
            .ok_or_else(|| internal("list_root_state_missing"))?;
        let candidates = state
            .objects
            .into_iter()
            .map(|(object, info)| Candidate {
                object,
                requires_check: info.requires_check,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(CandidateSet {
            candidates,
            metadata: CandidateMetadata {
                dispatches: self.counters.dispatches,
                datastore_queries: self.counters.datastore_queries,
                tuple_items: self.counters.tuple_items,
                intermediate_candidates: self.counters.intermediate_candidates,
                maximum_depth: self.counters.maximum_depth,
            },
        })
    }

    fn process_event(&mut self, event: Event) -> Result<(), ListError> {
        match event {
            Event::Expand(state) => self.expand_state(&state),
            Event::Publish {
                state,
                object,
                info,
            } => self.publish(&state, &object, info),
            Event::Apply {
                subscriber,
                object,
                info,
            } => self.apply(subscriber, object, info),
        }
    }

    fn ensure_state(&mut self, key: StateKey) -> Result<(), ListError> {
        if self.states.contains_key(&key) {
            return Ok(());
        }
        self.states.insert(key.clone(), RelationState::default());
        self.push_event(Event::Expand(key))
    }

    fn expand_state(&mut self, key: &StateKey) -> Result<(), ListError> {
        let state = self
            .states
            .get_mut(key)
            .ok_or_else(|| internal("list_relation_state_missing"))?;
        if state.expanded {
            return Ok(());
        }
        state.expanded = true;
        let relation = self
            .model
            .relation(key.relation)
            .map_err(|source| ListError::model("list_relation_invalid", source))?;
        let mut nodes = vec![(relation.root(), false, 0_u32)];
        let mut seen = BTreeSet::new();
        while let Some((node_id, ambiguous, node_depth)) = nodes.pop() {
            self.check_depth(node_depth)?;
            if !seen.insert((node_id, ambiguous)) {
                continue;
            }
            let node = self
                .model
                .node(node_id)
                .map_err(|source| ListError::model("list_rewrite_node_invalid", source))?
                .clone();
            self.expand_node(key, node, ambiguous, node_depth, &mut nodes)?;
        }
        Ok(())
    }

    fn expand_node(
        &mut self,
        key: &StateKey,
        node: RewriteNode,
        ambiguous: bool,
        node_depth: u32,
        nodes: &mut Vec<(NodeId, bool, u32)>,
    ) -> Result<(), ListError> {
        match node {
            RewriteNode::Direct(owner) => {
                self.enqueue_direct_read(key, owner, ambiguous, node_depth)?;
                let restrictions = self
                    .model
                    .relation(owner)
                    .map_err(|source| ListError::model("list_direct_relation_invalid", source))?
                    .restrictions()
                    .to_vec();
                for restriction in restrictions {
                    if let RestrictionKind::Userset(target) = restriction.kind() {
                        let child = StateKey {
                            relation: target,
                            subject: key.subject.clone(),
                        };
                        let subscriber = Subscriber {
                            parent: key.clone(),
                            transform: Transform::ReverseUserset {
                                owner,
                                subject_relation: target,
                            },
                            ambiguous,
                        };
                        self.subscribe(&child, &subscriber)?;
                    }
                }
            }
            RewriteNode::Computed(target) => {
                let child = StateKey {
                    relation: target,
                    subject: key.subject.clone(),
                };
                let subscriber = Subscriber {
                    parent: key.clone(),
                    transform: Transform::SameObject,
                    ambiguous,
                };
                self.subscribe(&child, &subscriber)?;
            }
            RewriteNode::TupleToUserset {
                tupleset, targets, ..
            } => {
                for target in targets {
                    let child = StateKey {
                        relation: target,
                        subject: key.subject.clone(),
                    };
                    let subscriber = Subscriber {
                        parent: key.clone(),
                        transform: Transform::ReverseObject(tupleset),
                        ambiguous,
                    };
                    self.subscribe(&child, &subscriber)?;
                }
            }
            RewriteNode::Union(children) => {
                let depth = next_depth(node_depth)?;
                nodes.extend(
                    children
                        .iter()
                        .rev()
                        .map(|child| (*child, ambiguous, depth)),
                );
            }
            RewriteNode::Intersection(children) => {
                let depth = next_depth(node_depth)?;
                nodes.extend(children.iter().rev().map(|child| (*child, true, depth)));
            }
            RewriteNode::Difference { base, .. } => {
                nodes.push((base, true, next_depth(node_depth)?));
            }
            _ => return Err(internal("list_rewrite_node_unsupported")),
        }
        Ok(())
    }

    fn subscribe(&mut self, child: &StateKey, subscriber: &Subscriber) -> Result<(), ListError> {
        self.ensure_state(child.clone())?;
        let state = self
            .states
            .get_mut(child)
            .ok_or_else(|| internal("list_child_state_missing"))?;
        if !state.subscribers.insert(subscriber.clone()) {
            return Ok(());
        }
        let existing = state
            .objects
            .iter()
            .map(|(object, info)| (object.clone(), *info))
            .collect::<Vec<_>>();
        for (object, info) in existing {
            self.push_event(Event::Apply {
                subscriber: subscriber.clone(),
                object,
                info,
            })?;
        }
        Ok(())
    }

    fn enqueue_direct_read(
        &mut self,
        state: &StateKey,
        relation: RelationId,
        ambiguous: bool,
        depth: u32,
    ) -> Result<(), ListError> {
        let mut subjects = vec![state.subject.clone()];
        if let SubjectRef::Object(object) = &state.subject {
            subjects.push(SubjectRef::TypedWildcard(object.object_type().clone()));
        }
        for subject in subjects {
            self.enqueue_reverse(
                state.clone(),
                relation,
                subject,
                CandidateInfo {
                    requires_check: ambiguous,
                    depth,
                },
            )?;
        }
        Ok(())
    }

    fn apply(
        &mut self,
        subscriber: Subscriber,
        object: ObjectRef,
        mut info: CandidateInfo,
    ) -> Result<(), ListError> {
        info.requires_check |= subscriber.ambiguous;
        info.depth = next_depth(info.depth)?;
        self.check_depth(info.depth)?;
        match subscriber.transform {
            Transform::SameObject => self.push_event(Event::Publish {
                state: subscriber.parent,
                object,
                info,
            }),
            Transform::ReverseObject(relation) => self.enqueue_reverse(
                subscriber.parent,
                relation,
                SubjectRef::Object(object),
                info,
            ),
            Transform::ReverseUserset {
                owner,
                subject_relation,
            } => {
                let relation = self
                    .model
                    .relation(subject_relation)
                    .map_err(|source| ListError::model("list_userset_relation_invalid", source))?
                    .name()
                    .clone();
                let userset = UsersetRef::new(object, relation, &self.input_limits)
                    .map_err(|_| internal("list_userset_candidate_invalid"))?;
                self.enqueue_reverse(subscriber.parent, owner, SubjectRef::Userset(userset), info)
            }
        }
    }

    fn enqueue_reverse(
        &mut self,
        parent: StateKey,
        relation: RelationId,
        subject: SubjectRef,
        info: CandidateInfo,
    ) -> Result<(), ListError> {
        self.check_depth(info.depth)?;
        let subjects = self
            .pending
            .entry(PendingKey { parent, relation })
            .or_default();
        match subjects.entry(subject) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(info);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().merge(info);
            }
        }
        Ok(())
    }

    async fn process_reverse_read(
        &mut self,
        key: PendingKey,
        subjects: BTreeMap<SubjectRef, CandidateInfo>,
    ) -> Result<(), ListError> {
        let chunk_size = self.input_limits.user_filters().max(1);
        let entries = subjects.into_iter().collect::<Vec<_>>();
        for chunk in entries.chunks(chunk_size) {
            self.charge_query()?;
            let relation = self
                .model
                .relation(key.relation)
                .map_err(|source| ListError::model("list_reverse_relation_invalid", source))?;
            let object_type = self
                .model
                .type_name(relation.object_type())
                .map_err(|source| ListError::model("list_reverse_type_invalid", source))?
                .clone();
            let subject_info = chunk.iter().cloned().collect::<BTreeMap<_, _>>();
            let filter = ReverseTupleFilter::new(
                object_type,
                relation.name().clone(),
                subject_info.keys().cloned().collect(),
                Vec::new(),
                ConditionFilter::any(),
                &self.input_limits,
            )?;
            let mut stream = self
                .tuples
                .read_reverse_tuples(
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
                    "list_reverse_read_may_be_truncated",
                ));
            }
            let contextual = self
                .command
                .query()
                .contextual_tuples()
                .as_slice()
                .iter()
                .filter(|tuple| {
                    tuple.key().object().object_type() == filter.object_type()
                        && tuple.key().relation() == filter.relation()
                        && subject_info.contains_key(tuple.key().subject())
                })
                .cloned()
                .collect::<Vec<_>>();
            self.charge_tuple_items(rows.len(), contextual.len())?;
            for tuple in rows.into_iter().chain(contextual) {
                let source = subject_info
                    .get(tuple.key().subject())
                    .copied()
                    .ok_or_else(|| internal("list_reverse_subject_unexpected"))?;
                let info = CandidateInfo {
                    requires_check: source.requires_check
                        || matches!(tuple.condition(), ConditionReference::Conditional(_)),
                    depth: source.depth,
                };
                self.push_event(Event::Publish {
                    state: key.parent.clone(),
                    object: tuple.key().object().clone(),
                    info,
                })?;
            }
        }
        Ok(())
    }

    fn publish(
        &mut self,
        state_key: &StateKey,
        object: &ObjectRef,
        info: CandidateInfo,
    ) -> Result<(), ListError> {
        self.check_depth(info.depth)?;
        let state = self
            .states
            .get_mut(state_key)
            .ok_or_else(|| internal("list_publish_state_missing"))?;
        let changed = match state.objects.entry(object.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(info);
                self.counters.intermediate_candidates = self
                    .counters
                    .intermediate_candidates
                    .checked_add(1)
                    .ok_or_else(candidate_exceeded)?;
                if self.counters.intermediate_candidates > self.budget.maximum_candidates() {
                    return Err(candidate_exceeded());
                }
                true
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => entry.get_mut().merge(info),
        };
        if !changed {
            return Ok(());
        }
        let current = state
            .objects
            .get(object)
            .copied()
            .ok_or_else(|| internal("list_published_candidate_missing"))?;
        let subscribers = state.subscribers.iter().cloned().collect::<Vec<_>>();
        for subscriber in subscribers {
            self.push_event(Event::Apply {
                subscriber,
                object: object.clone(),
                info: current,
            })?;
        }
        Ok(())
    }

    fn push_event(&mut self, event: Event) -> Result<(), ListError> {
        self.counters.dispatches = self
            .counters
            .dispatches
            .checked_add(1)
            .ok_or_else(dispatch_exceeded)?;
        if self.counters.dispatches > self.budget.maximum_dispatches() {
            return Err(dispatch_exceeded());
        }
        self.events.push_back(event);
        Ok(())
    }

    fn charge_query(&mut self) -> Result<(), ListError> {
        self.counters.datastore_queries = self
            .counters
            .datastore_queries
            .checked_add(1)
            .ok_or_else(datastore_query_exceeded)?;
        if self.counters.datastore_queries > self.budget.maximum_datastore_queries() {
            return Err(datastore_query_exceeded());
        }
        Ok(())
    }

    fn charge_tuple_items(&mut self, stored: usize, contextual: usize) -> Result<(), ListError> {
        let count = stored
            .checked_add(contextual)
            .and_then(|count| u32::try_from(count).ok())
            .ok_or_else(tuple_item_exceeded)?;
        self.counters.tuple_items = self
            .counters
            .tuple_items
            .checked_add(count)
            .ok_or_else(tuple_item_exceeded)?;
        if self.counters.tuple_items > self.budget.maximum_tuple_items() {
            return Err(tuple_item_exceeded());
        }
        Ok(())
    }

    fn check_depth(&mut self, depth: u32) -> Result<(), ListError> {
        self.counters.maximum_depth = self.counters.maximum_depth.max(depth);
        if depth > self.budget.maximum_depth() {
            return Err(ListError::new(
                ListErrorKind::DepthExceeded,
                "list_candidate_depth_exceeded",
            ));
        }
        Ok(())
    }
}

fn validate_model(command: &ListObjectsCommand, model: &CompiledModel) -> Result<(), ListError> {
    if model.store_id() != &command.query().store_id() {
        return Err(ListError::new(
            ListErrorKind::InvalidModel,
            "list_model_store_mismatch",
        ));
    }
    match command.query().model_selection() {
        ModelSelection::Explicit(model_id) if model.model_id() != &model_id => Err(ListError::new(
            ListErrorKind::InvalidModel,
            "list_model_id_mismatch",
        )),
        ModelSelection::Explicit(_) | ModelSelection::Latest => Ok(()),
        _ => Err(ListError::new(
            ListErrorKind::InvalidModel,
            "list_model_selection_unsupported",
        )),
    }
}

const fn next_depth(depth: u32) -> Result<u32, ListError> {
    match depth.checked_add(1) {
        Some(depth) => Ok(depth),
        None => Err(ListError::new(
            ListErrorKind::DepthExceeded,
            "list_candidate_depth_exceeded",
        )),
    }
}

const fn internal(code: &'static str) -> ListError {
    ListError::new(ListErrorKind::Internal, code)
}

const fn dispatch_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::DispatchExceeded,
        "list_candidate_dispatch_exceeded",
    )
}

const fn datastore_query_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::DatastoreQueryExceeded,
        "list_candidate_datastore_query_exceeded",
    )
}

const fn tuple_item_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::TupleItemExceeded,
        "list_candidate_tuple_items_exceeded",
    )
}

const fn candidate_exceeded() -> ListError {
    ListError::new(
        ListErrorKind::CandidateExceeded,
        "list_candidate_count_exceeded",
    )
}
