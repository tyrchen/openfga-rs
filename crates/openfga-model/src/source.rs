//! Project-owned authorization-model source values.

use std::{fmt, mem};

use openfga_condition::{ConditionDefinition, ParameterTypeRef};
use openfga_domain::{
    AuthorizationModelId, ConditionName, Fingerprint, FingerprintBuilder, RelationName, StoreId,
    TypeName,
};

use crate::ConditionParameterTypeError;

/// Authorization-model declarations before the service assigns store/model identity.
#[non_exhaustive]
pub struct AuthorizationModelDefinition {
    schema_version: String,
    type_definitions: Vec<TypeDefinitionSource>,
    conditions: Vec<ConditionSource>,
}

impl AuthorizationModelDefinition {
    /// Creates a bounded, project-owned model definition for semantic compilation.
    #[must_use]
    pub const fn new(
        schema_version: String,
        type_definitions: Vec<TypeDefinitionSource>,
        conditions: Vec<ConditionSource>,
    ) -> Self {
        Self {
            schema_version,
            type_definitions,
            conditions,
        }
    }

    /// Assigns immutable identity and consumes the definition into compiler source.
    #[must_use]
    pub fn with_identity(
        self,
        store_id: StoreId,
        model_id: AuthorizationModelId,
    ) -> AuthorizationModelSource {
        AuthorizationModelSource::new(
            store_id,
            model_id,
            self.schema_version,
            self.type_definitions,
            self.conditions,
        )
    }
}

impl fmt::Debug for AuthorizationModelDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationModelDefinition")
            .field("schema_version_bytes", &self.schema_version.len())
            .field("type_definitions", &self.type_definitions.len())
            .field("conditions", &self.conditions.len())
            .finish_non_exhaustive()
    }
}

/// One authorization model after bounded wire conversion and before semantic compilation.
#[non_exhaustive]
pub struct AuthorizationModelSource {
    pub(crate) store_id: StoreId,
    pub(crate) model_id: AuthorizationModelId,
    pub(crate) schema_version: String,
    pub(crate) type_definitions: Vec<TypeDefinitionSource>,
    pub(crate) conditions: Vec<ConditionSource>,
}

impl AuthorizationModelSource {
    /// Creates project-owned model source. Semantic validation occurs during compilation.
    #[must_use]
    pub fn new(
        store_id: StoreId,
        model_id: AuthorizationModelId,
        schema_version: String,
        type_definitions: Vec<TypeDefinitionSource>,
        conditions: Vec<ConditionSource>,
    ) -> Self {
        Self {
            store_id,
            model_id,
            schema_version,
            type_definitions,
            conditions,
        }
    }

    /// Returns the store owning the immutable model.
    #[must_use]
    pub const fn store_id(&self) -> &StoreId {
        &self.store_id
    }

    /// Returns the immutable authorization-model identifier.
    #[must_use]
    pub const fn model_id(&self) -> &AuthorizationModelId {
        &self.model_id
    }

    /// Returns the declared schema version.
    #[must_use]
    pub fn schema_version(&self) -> &str {
        &self.schema_version
    }

    /// Returns type declarations in source order.
    #[must_use]
    pub fn type_definitions(&self) -> &[TypeDefinitionSource] {
        &self.type_definitions
    }

    /// Returns condition declarations in source order.
    #[must_use]
    pub fn conditions(&self) -> &[ConditionSource] {
        &self.conditions
    }

    /// Returns a deterministic proof of this exact ordered source model.
    #[must_use]
    pub fn fingerprint(&self) -> Fingerprint {
        source_fingerprint(self)
    }

    /// Returns a conservative estimate of heap and inline bytes owned by this source model.
    #[must_use]
    pub fn estimated_owned_bytes(&self) -> usize {
        let map_node_overhead = 4_usize.saturating_mul(mem::size_of::<usize>());
        let mut bytes = mem::size_of::<Self>()
            .saturating_add(self.schema_version.capacity())
            .saturating_add(
                self.type_definitions
                    .capacity()
                    .saturating_mul(mem::size_of::<TypeDefinitionSource>()),
            )
            .saturating_add(
                self.conditions
                    .capacity()
                    .saturating_mul(mem::size_of::<ConditionSource>()),
            );
        for definition in &self.type_definitions {
            bytes = bytes
                .saturating_add(definition.name.as_str().len())
                .saturating_add(
                    definition
                        .relations
                        .capacity()
                        .saturating_mul(mem::size_of::<RelationSource>()),
                );
            for relation in &definition.relations {
                bytes = bytes
                    .saturating_add(relation.name.as_str().len())
                    .saturating_add(rewrite_owned_bytes(&relation.rewrite))
                    .saturating_add(
                        relation
                            .restrictions
                            .capacity()
                            .saturating_mul(mem::size_of::<DirectRestrictionSource>()),
                    );
                for restriction in &relation.restrictions {
                    bytes = bytes
                        .saturating_add(restriction.subject_type.as_str().len())
                        .saturating_add(match &restriction.kind {
                            RestrictionKindSource::Userset(relation) => relation.as_str().len(),
                            RestrictionKindSource::Object | RestrictionKindSource::Wildcard => 0,
                        })
                        .saturating_add(
                            restriction
                                .condition
                                .as_ref()
                                .map_or(0, |condition| condition.as_str().len()),
                        );
                }
            }
        }
        for condition in &self.conditions {
            bytes = bytes
                .saturating_add(condition.key.as_str().len())
                .saturating_add(condition.definition.name().as_str().len())
                .saturating_add(condition.definition.expression().len())
                .saturating_add(
                    condition
                        .parameter_type_errors
                        .capacity()
                        .saturating_mul(mem::size_of::<(u32, ConditionParameterTypeError)>()),
                );
            for (name, parameter_type) in condition.definition.parameters() {
                bytes = bytes
                    .saturating_add(
                        mem::size_of_val(name)
                            .saturating_add(mem::size_of_val(parameter_type))
                            .saturating_add(map_node_overhead),
                    )
                    .saturating_add(name.as_str().len())
                    .saturating_add(parameter_type_owned_bytes(parameter_type.as_ref()));
            }
        }
        bytes
    }
}

fn rewrite_owned_bytes(root: &RewriteSource) -> usize {
    let mut bytes = 0_usize;
    let mut pending = vec![root];
    while let Some(rewrite) = pending.pop() {
        match rewrite {
            RewriteSource::Direct => {}
            RewriteSource::Computed(relation) => {
                bytes = bytes.saturating_add(relation.as_str().len());
            }
            RewriteSource::TupleToUserset { tupleset, computed } => {
                bytes = bytes
                    .saturating_add(tupleset.as_str().len())
                    .saturating_add(computed.as_str().len());
            }
            RewriteSource::Union(children) | RewriteSource::Intersection(children) => {
                bytes = bytes.saturating_add(
                    children
                        .capacity()
                        .saturating_mul(mem::size_of::<RewriteSource>()),
                );
                pending.extend(children);
            }
            RewriteSource::Difference { base, subtract } => {
                bytes =
                    bytes.saturating_add(2_usize.saturating_mul(mem::size_of::<RewriteSource>()));
                pending.extend([base.as_ref(), subtract.as_ref()]);
            }
        }
    }
    bytes
}

fn parameter_type_owned_bytes(root: ParameterTypeRef<'_>) -> usize {
    let mut bytes = mem::size_of_val(&root);
    let mut current = root;
    while let ParameterTypeRef::List(child) | ParameterTypeRef::Map(child) = current {
        bytes = bytes.saturating_add(mem::size_of_val(child));
        current = child.as_ref();
    }
    bytes
}

impl fmt::Debug for AuthorizationModelSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationModelSource")
            .field("store_id", &self.store_id)
            .field("model_id", &self.model_id)
            .field("schema_version_bytes", &self.schema_version.len())
            .field("type_definitions", &self.type_definitions.len())
            .field("conditions", &self.conditions.len())
            .finish_non_exhaustive()
    }
}

fn source_fingerprint(source: &AuthorizationModelSource) -> Fingerprint {
    let mut builder = FingerprintBuilder::new("openfga.authorization-model-source.v1");
    builder.write_str(&source.store_id.to_string());
    builder.write_str(&source.model_id.to_string());
    builder.write_str(&source.schema_version);
    builder.write_u64(source_length(source.type_definitions.len()));
    for type_definition in &source.type_definitions {
        builder.write_str(type_definition.name.as_str());
        builder.write_u64(source_length(type_definition.relations.len()));
        for relation in &type_definition.relations {
            builder.write_str(relation.name.as_str());
            builder.write_tag(u8::from(relation.rewrite_valid));
            write_rewrite_fingerprint(&relation.rewrite, &mut builder);
            builder.write_u64(source_length(relation.restrictions.len()));
            for restriction in &relation.restrictions {
                builder.write_str(restriction.subject_type.as_str());
                match &restriction.kind {
                    RestrictionKindSource::Object => builder.write_tag(0),
                    RestrictionKindSource::Userset(relation) => {
                        builder.write_tag(1);
                        builder.write_str(relation.as_str());
                    }
                    RestrictionKindSource::Wildcard => builder.write_tag(2),
                }
                match &restriction.condition {
                    Some(condition) => {
                        builder.write_tag(1);
                        builder.write_str(condition.as_str());
                    }
                    None => builder.write_tag(0),
                }
            }
        }
    }
    builder.write_u64(source_length(source.conditions.len()));
    for condition in &source.conditions {
        builder.write_str(condition.key.as_str());
        builder.write_bytes(condition.definition.fingerprint().as_bytes());
    }
    builder.finish()
}

fn write_rewrite_fingerprint(root: &RewriteSource, builder: &mut FingerprintBuilder) {
    let mut pending = vec![root];
    while let Some(rewrite) = pending.pop() {
        match rewrite {
            RewriteSource::Direct => builder.write_tag(0),
            RewriteSource::Computed(relation) => {
                builder.write_tag(1);
                builder.write_str(relation.as_str());
            }
            RewriteSource::TupleToUserset { tupleset, computed } => {
                builder.write_tag(2);
                builder.write_str(tupleset.as_str());
                builder.write_str(computed.as_str());
            }
            RewriteSource::Union(children) => {
                builder.write_tag(3);
                builder.write_u64(source_length(children.len()));
                pending.extend(children.iter().rev());
            }
            RewriteSource::Intersection(children) => {
                builder.write_tag(4);
                builder.write_u64(source_length(children.len()));
                pending.extend(children.iter().rev());
            }
            RewriteSource::Difference { base, subtract } => {
                builder.write_tag(5);
                pending.push(subtract);
                pending.push(base);
            }
        }
    }
}

fn source_length(length: usize) -> u64 {
    u64::try_from(length).unwrap_or(u64::MAX)
}

/// One ordered object-type declaration.
#[non_exhaustive]
pub struct TypeDefinitionSource {
    pub(crate) name: TypeName,
    pub(crate) relations: Vec<RelationSource>,
}

impl TypeDefinitionSource {
    /// Creates a type declaration with ordered relations.
    #[must_use]
    pub const fn new(name: TypeName, relations: Vec<RelationSource>) -> Self {
        Self { name, relations }
    }

    /// Returns the declared type name.
    #[must_use]
    pub const fn name(&self) -> &TypeName {
        &self.name
    }

    /// Returns relation declarations in source order.
    #[must_use]
    pub fn relations(&self) -> &[RelationSource] {
        &self.relations
    }
}

impl fmt::Debug for TypeDefinitionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypeDefinitionSource")
            .field("name", &self.name)
            .field("relations", &self.relations.len())
            .finish_non_exhaustive()
    }
}

/// One ordered relation declaration and its direct type restrictions.
#[non_exhaustive]
pub struct RelationSource {
    pub(crate) name: RelationName,
    pub(crate) rewrite: RewriteSource,
    pub(crate) rewrite_valid: bool,
    pub(crate) restrictions: Vec<DirectRestrictionSource>,
}

impl RelationSource {
    /// Creates a relation declaration.
    #[must_use]
    pub const fn new(
        name: RelationName,
        rewrite: RewriteSource,
        restrictions: Vec<DirectRestrictionSource>,
    ) -> Self {
        Self {
            name,
            rewrite,
            rewrite_valid: true,
            restrictions,
        }
    }

    /// Creates a relation whose wire rewrite was absent.
    ///
    /// The compiler deterministically rejects this source as
    /// [`InvalidRewrite`](crate::ModelErrorCode::InvalidRewrite).
    #[must_use]
    pub fn with_invalid_rewrite(
        name: RelationName,
        restrictions: Vec<DirectRestrictionSource>,
    ) -> Self {
        Self {
            name,
            rewrite: RewriteSource::Direct,
            rewrite_valid: false,
            restrictions,
        }
    }

    /// Returns whether the wire source contained a rewrite alternative.
    #[must_use]
    pub const fn has_valid_rewrite(&self) -> bool {
        self.rewrite_valid
    }

    /// Returns the declared relation name.
    #[must_use]
    pub const fn name(&self) -> &RelationName {
        &self.name
    }

    /// Returns the unresolved relation rewrite.
    #[must_use]
    pub const fn rewrite(&self) -> &RewriteSource {
        &self.rewrite
    }

    /// Returns direct subject restrictions in source order.
    #[must_use]
    pub fn restrictions(&self) -> &[DirectRestrictionSource] {
        &self.restrictions
    }
}

impl fmt::Debug for RelationSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationSource")
            .field("name", &self.name)
            .field("rewrite", &self.rewrite)
            .field("rewrite_valid", &self.rewrite_valid)
            .field("restrictions", &self.restrictions.len())
            .finish_non_exhaustive()
    }
}

/// Unresolved rewrite syntax. Compilation lowers this tree iteratively into dense IR.
#[non_exhaustive]
pub enum RewriteSource {
    /// Direct tuple membership (`this`).
    Direct,
    /// Same-object computed relation.
    Computed(RelationName),
    /// Tuple-to-userset (`computed from tupleset`).
    TupleToUserset {
        /// Direct tupleset relation on the enclosing type.
        tupleset: RelationName,
        /// Computed relation name on permitted tupleset target types.
        computed: RelationName,
    },
    /// Set union.
    Union(Vec<Self>),
    /// Set intersection.
    Intersection(Vec<Self>),
    /// Set difference.
    Difference {
        /// Positive operand.
        base: Box<Self>,
        /// Subtracted operand.
        subtract: Box<Self>,
    },
}

/// Borrowed structural view of an unresolved relation rewrite.
#[derive(Clone, Copy, Debug)]
pub enum RewriteSourceRef<'a> {
    /// Direct tuple membership.
    Direct,
    /// Same-object computed relation.
    Computed(&'a RelationName),
    /// Tuple-to-userset relation pair.
    TupleToUserset {
        /// Tupleset relation.
        tupleset: &'a RelationName,
        /// Computed relation.
        computed: &'a RelationName,
    },
    /// Set union.
    Union(&'a [RewriteSource]),
    /// Set intersection.
    Intersection(&'a [RewriteSource]),
    /// Set difference.
    Difference {
        /// Positive operand.
        base: &'a RewriteSource,
        /// Subtracted operand.
        subtract: &'a RewriteSource,
    },
}

impl RewriteSource {
    /// Returns a structural view suitable for validated persistence conversion.
    #[must_use]
    pub fn as_ref(&self) -> RewriteSourceRef<'_> {
        match self {
            Self::Direct => RewriteSourceRef::Direct,
            Self::Computed(relation) => RewriteSourceRef::Computed(relation),
            Self::TupleToUserset { tupleset, computed } => {
                RewriteSourceRef::TupleToUserset { tupleset, computed }
            }
            Self::Union(children) => RewriteSourceRef::Union(children),
            Self::Intersection(children) => RewriteSourceRef::Intersection(children),
            Self::Difference { base, subtract } => RewriteSourceRef::Difference { base, subtract },
        }
    }
}

impl Drop for RewriteSource {
    fn drop(&mut self) {
        let mut pending = Vec::new();
        take_children(self, &mut pending);
        while let Some(mut child) = pending.pop() {
            take_children(&mut child, &mut pending);
        }
    }
}

fn take_children(rewrite: &mut RewriteSource, pending: &mut Vec<RewriteSource>) {
    match rewrite {
        RewriteSource::Union(children) | RewriteSource::Intersection(children) => {
            pending.extend(mem::take(children));
        }
        RewriteSource::Difference { base, subtract } => {
            pending.push(mem::replace(base.as_mut(), RewriteSource::Direct));
            pending.push(mem::replace(subtract.as_mut(), RewriteSource::Direct));
        }
        RewriteSource::Direct
        | RewriteSource::Computed(_)
        | RewriteSource::TupleToUserset { .. } => {}
    }
}

impl fmt::Debug for RewriteSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct => formatter.write_str("Direct"),
            Self::Computed(relation) => formatter.debug_tuple("Computed").field(relation).finish(),
            Self::TupleToUserset { tupleset, computed } => formatter
                .debug_struct("TupleToUserset")
                .field("tupleset", tupleset)
                .field("computed", computed)
                .finish(),
            Self::Union(children) => formatter
                .debug_struct("Union")
                .field("children", &children.len())
                .finish(),
            Self::Intersection(children) => formatter
                .debug_struct("Intersection")
                .field("children", &children.len())
                .finish(),
            Self::Difference { .. } => formatter.write_str("Difference { .. }"),
        }
    }
}

/// One unresolved directly-related subject restriction.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DirectRestrictionSource {
    pub(crate) subject_type: TypeName,
    pub(crate) kind: RestrictionKindSource,
    pub(crate) condition: Option<ConditionName>,
}

impl DirectRestrictionSource {
    /// Creates a direct restriction.
    #[must_use]
    pub const fn new(
        subject_type: TypeName,
        kind: RestrictionKindSource,
        condition: Option<ConditionName>,
    ) -> Self {
        Self {
            subject_type,
            kind,
            condition,
        }
    }

    /// Returns the permitted subject type.
    #[must_use]
    pub const fn subject_type(&self) -> &TypeName {
        &self.subject_type
    }

    /// Returns the permitted direct subject shape.
    #[must_use]
    pub const fn kind(&self) -> &RestrictionKindSource {
        &self.kind
    }

    /// Returns the optional required condition name.
    #[must_use]
    pub const fn condition(&self) -> Option<&ConditionName> {
        self.condition.as_ref()
    }
}

/// Unresolved direct restriction shape.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RestrictionKindSource {
    /// Concrete object of the declared subject type.
    Object,
    /// Userset of the declared subject type and relation.
    Userset(RelationName),
    /// Typed wildcard of the declared subject type.
    Wildcard,
}

/// Borrowed structural view of a direct subject restriction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestrictionKindSourceRef<'a> {
    /// Concrete object.
    Object,
    /// Userset with its relation.
    Userset(&'a RelationName),
    /// Typed wildcard.
    Wildcard,
}

impl RestrictionKindSource {
    /// Returns the stable restriction structure.
    #[must_use]
    pub const fn as_ref(&self) -> RestrictionKindSourceRef<'_> {
        match self {
            Self::Object => RestrictionKindSourceRef::Object,
            Self::Userset(relation) => RestrictionKindSourceRef::Userset(relation),
            Self::Wildcard => RestrictionKindSourceRef::Wildcard,
        }
    }
}

/// One condition map entry, retaining both key and declared name for validation.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConditionSource {
    pub(crate) key: ConditionName,
    pub(crate) definition: ConditionDefinition,
    pub(crate) parameter_type_errors: Vec<(u32, ConditionParameterTypeError)>,
}

impl ConditionSource {
    /// Creates a condition source entry.
    #[must_use]
    pub const fn new(key: ConditionName, definition: ConditionDefinition) -> Self {
        Self {
            key,
            definition,
            parameter_type_errors: Vec::new(),
        }
    }

    /// Creates a condition source retaining malformed wire parameter-type diagnostics.
    #[must_use]
    pub const fn with_parameter_type_errors(
        key: ConditionName,
        definition: ConditionDefinition,
        parameter_type_errors: Vec<(u32, ConditionParameterTypeError)>,
    ) -> Self {
        Self {
            key,
            definition,
            parameter_type_errors,
        }
    }

    /// Returns the condition map key.
    #[must_use]
    pub const fn key(&self) -> &ConditionName {
        &self.key
    }

    /// Returns the validated condition definition.
    #[must_use]
    pub const fn definition(&self) -> &ConditionDefinition {
        &self.definition
    }
}

impl fmt::Debug for ConditionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConditionSource")
            .field("key", &self.key)
            .field("definition", &self.definition)
            .field("parameter_type_errors", &self.parameter_type_errors.len())
            .finish_non_exhaustive()
    }
}
