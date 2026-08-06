//! Dense immutable authorization-model representation.

use openfga_domain::{ConditionName, RelationName, TypeName};

macro_rules! dense_id {
    ($name:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub struct $name(u32);

        impl $name {
            pub(crate) fn from_index(index: usize) -> Option<Self> {
                u32::try_from(index).ok().map(Self)
            }

            pub(crate) const fn index(self) -> usize {
                self.0 as usize
            }

            /// Returns the model-local dense numeric identifier.
            #[must_use]
            pub const fn as_u32(self) -> u32 {
                self.0
            }
        }
    };
}

dense_id!(TypeId, "A model-local dense object-type identifier.");
dense_id!(RelationId, "A model-local dense relation identifier.");
dense_id!(ConditionId, "A model-local dense condition identifier.");
dense_id!(NodeId, "A model-local dense rewrite-node identifier.");

/// Condition requirement on a direct subject restriction.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ConditionRequirement {
    /// No condition is required.
    Unconditional,
    /// The named compiled condition is required.
    Required(ConditionId),
}

/// Resolved directly-related subject shape.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RestrictionKind {
    /// Concrete object.
    Object,
    /// Userset referencing the resolved relation.
    Userset(RelationId),
    /// Typed wildcard.
    Wildcard,
}

/// One resolved, canonical direct type restriction.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct DirectRestriction {
    pub(crate) subject_type: TypeId,
    pub(crate) kind: RestrictionKind,
    pub(crate) condition: ConditionRequirement,
}

impl DirectRestriction {
    /// Returns the permitted subject type.
    #[must_use]
    pub const fn subject_type(&self) -> TypeId {
        self.subject_type
    }

    /// Returns the permitted direct subject shape.
    #[must_use]
    pub const fn kind(&self) -> RestrictionKind {
        self.kind
    }

    /// Returns the condition requirement.
    #[must_use]
    pub const fn condition(&self) -> ConditionRequirement {
        self.condition
    }
}

/// One normalized rewrite operation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RewriteNode {
    /// Direct tuple membership for the owning relation.
    Direct(RelationId),
    /// Same-object computed relation.
    Computed(RelationId),
    /// Tuple-to-userset with all valid computed targets resolved.
    TupleToUserset {
        /// Direct tupleset relation.
        tupleset: RelationId,
        /// Computed relation name retained for query planning and diagnostics.
        computed: RelationName,
        /// Resolved computed relations on permitted target types.
        targets: Box<[RelationId]>,
    },
    /// Nonempty set union.
    Union(Box<[NodeId]>),
    /// Nonempty set intersection.
    Intersection(Box<[NodeId]>),
    /// Set difference.
    Difference {
        /// Positive operand.
        base: NodeId,
        /// Subtracted operand.
        subtract: NodeId,
    },
}

/// One immutable compiled relation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CompiledRelation {
    pub(crate) id: RelationId,
    pub(crate) object_type: TypeId,
    pub(crate) name: RelationName,
    pub(crate) root: NodeId,
    pub(crate) restrictions: Box<[DirectRestriction]>,
}

impl CompiledRelation {
    /// Returns the dense relation identifier.
    #[must_use]
    pub const fn id(&self) -> RelationId {
        self.id
    }

    /// Returns the declaring object type.
    #[must_use]
    pub const fn object_type(&self) -> TypeId {
        self.object_type
    }

    /// Returns the relation name.
    #[must_use]
    pub const fn name(&self) -> &RelationName {
        &self.name
    }

    /// Returns the root rewrite node.
    #[must_use]
    pub const fn root(&self) -> NodeId {
        self.root
    }

    /// Returns canonical direct restrictions.
    #[must_use]
    pub const fn restrictions(&self) -> &[DirectRestriction] {
        &self.restrictions
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledType {
    pub(crate) id: TypeId,
    pub(crate) name: TypeName,
    pub(crate) relations: Box<[RelationId]>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledConditionEntry {
    pub(crate) id: ConditionId,
    pub(crate) name: ConditionName,
    pub(crate) condition: std::sync::Arc<openfga_condition::CompiledCondition>,
}
