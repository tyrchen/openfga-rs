//! Iterative relationship-graph metadata construction.

use std::collections::BTreeSet;

use crate::{
    error::{DeclarationPath, ErrorCollector, ModelErrorCode},
    ir::{CompiledRelation, NodeId, RelationId, RestrictionKind, RewriteNode, TypeId},
    limits::ModelLimits,
};

#[derive(Clone, Debug)]
pub(crate) struct GraphMetadata {
    pub(crate) forward: Vec<Box<[RelationId]>>,
    pub(crate) reverse: Vec<Box<[RelationId]>>,
    pub(crate) reachable_types: Vec<BTreeSet<TypeId>>,
    pub(crate) reachable_wildcards: Vec<BTreeSet<TypeId>>,
    pub(crate) recursion_groups: Vec<Option<u32>>,
}

pub(crate) fn build_graph_metadata(
    relations: &[CompiledRelation],
    nodes: &[RewriteNode],
    limits: &ModelLimits,
    errors: &mut ErrorCollector,
) -> Option<GraphMetadata> {
    let mut forward_sets = vec![BTreeSet::new(); relations.len()];
    let mut reverse_sets = vec![BTreeSet::new(); relations.len()];
    let mut reachable_types = vec![BTreeSet::new(); relations.len()];
    let mut reachable_wildcards = vec![BTreeSet::new(); relations.len()];
    let mut edge_count = 0_usize;

    for relation in relations {
        let mut stack = vec![relation.root()];
        let mut seen_nodes = BTreeSet::new();
        while let Some(node_id) = stack.pop() {
            if !seen_nodes.insert(node_id) {
                continue;
            }
            let Some(node) = nodes.get(node_id.index()) else {
                errors.push(ModelErrorCode::InvalidRewrite, DeclarationPath::Model);
                return None;
            };
            match node {
                RewriteNode::Direct(owner) => {
                    let Some(owner_relation) = relations.get(owner.index()) else {
                        errors.push(ModelErrorCode::InvalidRewrite, DeclarationPath::Model);
                        return None;
                    };
                    for restriction in owner_relation.restrictions() {
                        if let Some(types) = reachable_types.get_mut(relation.id().index()) {
                            types.insert(restriction.subject_type());
                        }
                        match restriction.kind() {
                            RestrictionKind::Userset(target) => {
                                insert_edge(
                                    relation.id(),
                                    target,
                                    &mut forward_sets,
                                    &mut reverse_sets,
                                    &mut edge_count,
                                );
                            }
                            RestrictionKind::Wildcard => {
                                if let Some(wildcards) =
                                    reachable_wildcards.get_mut(relation.id().index())
                                {
                                    wildcards.insert(restriction.subject_type());
                                }
                            }
                            RestrictionKind::Object => {}
                        }
                    }
                }
                RewriteNode::Computed(target) => insert_edge(
                    relation.id(),
                    *target,
                    &mut forward_sets,
                    &mut reverse_sets,
                    &mut edge_count,
                ),
                RewriteNode::TupleToUserset {
                    tupleset, targets, ..
                } => {
                    insert_reverse_only(
                        relation.id(),
                        *tupleset,
                        &mut reverse_sets,
                        &mut edge_count,
                    );
                    for target in targets {
                        insert_edge(
                            relation.id(),
                            *target,
                            &mut forward_sets,
                            &mut reverse_sets,
                            &mut edge_count,
                        );
                    }
                }
                RewriteNode::Union(children) | RewriteNode::Intersection(children) => {
                    stack.extend(children.iter().copied());
                }
                RewriteNode::Difference { base, subtract } => {
                    stack.push(*base);
                    stack.push(*subtract);
                }
            }
            if edge_count > limits.graph_edges() {
                errors.push(ModelErrorCode::GraphLimitExceeded, DeclarationPath::Model);
                return None;
            }
        }
    }

    propagate_reachability(
        &forward_sets,
        &mut reachable_types,
        &mut reachable_wildcards,
    );
    let recursion_groups = recursion_groups(&forward_sets);
    Some(GraphMetadata {
        forward: freeze_sets(forward_sets),
        reverse: freeze_sets(reverse_sets),
        reachable_types,
        reachable_wildcards,
        recursion_groups,
    })
}

pub(crate) fn computed_cycle_relations(
    relations: &[CompiledRelation],
    nodes: &[RewriteNode],
) -> BTreeSet<RelationId> {
    let mut edges = vec![BTreeSet::new(); relations.len()];
    for relation in relations {
        let mut stack = vec![relation.root()];
        let mut seen = BTreeSet::<NodeId>::new();
        while let Some(node_id) = stack.pop() {
            if !seen.insert(node_id) {
                continue;
            }
            let Some(node) = nodes.get(node_id.index()) else {
                continue;
            };
            match node {
                RewriteNode::Computed(target) => {
                    if let Some(outgoing) = edges.get_mut(relation.id().index()) {
                        outgoing.insert(*target);
                    }
                }
                RewriteNode::Union(children) | RewriteNode::Intersection(children) => {
                    stack.extend(children.iter().copied());
                }
                RewriteNode::Difference { base, subtract } => {
                    stack.push(*base);
                    stack.push(*subtract);
                }
                RewriteNode::Direct(_) | RewriteNode::TupleToUserset { .. } => {}
            }
        }
    }
    let groups = strongly_connected_components(&edges);
    let mut cyclic = BTreeSet::new();
    for component in groups {
        let self_cycle = component.first().is_some_and(|relation| {
            edges
                .get(relation.index())
                .is_some_and(|targets| targets.contains(relation))
        });
        if component.len() > 1 || self_cycle {
            cyclic.extend(component);
        }
    }
    cyclic
}

fn insert_edge(
    source: RelationId,
    target: RelationId,
    forward: &mut [BTreeSet<RelationId>],
    reverse: &mut [BTreeSet<RelationId>],
    count: &mut usize,
) {
    let inserted = forward
        .get_mut(source.index())
        .is_some_and(|edges| edges.insert(target));
    if inserted {
        *count = count.saturating_add(1);
        if let Some(edges) = reverse.get_mut(target.index()) {
            edges.insert(source);
        }
    }
}

fn insert_reverse_only(
    source: RelationId,
    target: RelationId,
    reverse: &mut [BTreeSet<RelationId>],
    count: &mut usize,
) {
    if reverse
        .get_mut(target.index())
        .is_some_and(|edges| edges.insert(source))
    {
        *count = count.saturating_add(1);
    }
}

fn propagate_reachability(
    forward: &[BTreeSet<RelationId>],
    types: &mut [BTreeSet<TypeId>],
    wildcards: &mut [BTreeSet<TypeId>],
) {
    let mut changed = true;
    let mut remaining = forward.len().saturating_add(1);
    while changed && remaining > 0 {
        changed = false;
        remaining = remaining.saturating_sub(1);
        for source_index in 0..forward.len() {
            let Some(targets) = forward.get(source_index) else {
                continue;
            };
            let mut added_types = BTreeSet::new();
            let mut added_wildcards = BTreeSet::new();
            for target in targets {
                if let Some(target_types) = types.get(target.index()) {
                    added_types.extend(target_types.iter().copied());
                }
                if let Some(target_wildcards) = wildcards.get(target.index()) {
                    added_wildcards.extend(target_wildcards.iter().copied());
                }
            }
            if let Some(source_types) = types.get_mut(source_index) {
                let before = source_types.len();
                source_types.extend(added_types);
                changed |= source_types.len() != before;
            }
            if let Some(source_wildcards) = wildcards.get_mut(source_index) {
                let before = source_wildcards.len();
                source_wildcards.extend(added_wildcards);
                changed |= source_wildcards.len() != before;
            }
        }
    }
}

fn recursion_groups(edges: &[BTreeSet<RelationId>]) -> Vec<Option<u32>> {
    let mut assignments = vec![None; edges.len()];
    let mut next_group = 0_u32;
    for component in strongly_connected_components(edges) {
        let self_cycle = component.first().is_some_and(|relation| {
            edges
                .get(relation.index())
                .is_some_and(|targets| targets.contains(relation))
        });
        if component.len() > 1 || self_cycle {
            for relation in component {
                if let Some(slot) = assignments.get_mut(relation.index()) {
                    *slot = Some(next_group);
                }
            }
            next_group = next_group.saturating_add(1);
        }
    }
    assignments
}

fn strongly_connected_components(edges: &[BTreeSet<RelationId>]) -> Vec<Vec<RelationId>> {
    let mut reverse = vec![BTreeSet::new(); edges.len()];
    for (source_index, targets) in edges.iter().enumerate() {
        let Some(source) = RelationId::from_index(source_index) else {
            continue;
        };
        for target in targets {
            if let Some(incoming) = reverse.get_mut(target.index()) {
                incoming.insert(source);
            }
        }
    }

    let mut seen = vec![false; edges.len()];
    let mut finish_order = Vec::with_capacity(edges.len());
    for index in 0..edges.len() {
        if seen.get(index).copied().unwrap_or(true) {
            continue;
        }
        let Some(start) = RelationId::from_index(index) else {
            continue;
        };
        let mut stack = vec![(start, false)];
        while let Some((node, expanded)) = stack.pop() {
            if expanded {
                finish_order.push(node);
                continue;
            }
            let Some(node_seen) = seen.get_mut(node.index()) else {
                continue;
            };
            if *node_seen {
                continue;
            }
            *node_seen = true;
            stack.push((node, true));
            if let Some(targets) = edges.get(node.index()) {
                for target in targets.iter().rev() {
                    if !seen.get(target.index()).copied().unwrap_or(true) {
                        stack.push((*target, false));
                    }
                }
            }
        }
    }

    let mut assigned = vec![false; edges.len()];
    let mut components = Vec::new();
    while let Some(start) = finish_order.pop() {
        if assigned.get(start.index()).copied().unwrap_or(true) {
            continue;
        }
        let mut component = Vec::new();
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            let Some(node_assigned) = assigned.get_mut(node.index()) else {
                continue;
            };
            if *node_assigned {
                continue;
            }
            *node_assigned = true;
            component.push(node);
            if let Some(sources) = reverse.get(node.index()) {
                stack.extend(sources.iter().rev().copied());
            }
        }
        component.sort_unstable();
        components.push(component);
    }
    components
}

fn freeze_sets(sets: Vec<BTreeSet<RelationId>>) -> Vec<Box<[RelationId]>> {
    sets.into_iter()
        .map(|set| set.into_iter().collect::<Vec<_>>().into_boxed_slice())
        .collect()
}
