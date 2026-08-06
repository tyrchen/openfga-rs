//! Deterministic authorization-model validation, compilation, and graph metadata.
//!
//! Generated protobufs are converted into bounded project-owned source values
//! before compilation. Successful compilation returns one immutable model handle
//! containing dense rewrite IR, compiled conditions, reachability, reverse edges,
//! recursion groups, and a canonical semantic fingerprint.
//!
//! # Examples
//!
//! ```
//! use openfga_domain::{AuthorizationModelId, RelationName, StoreId, TypeName};
//! use openfga_model::{
//!     AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
//!     RestrictionKindSource, RewriteSource, TypeDefinitionSource,
//! };
//!
//! let source = AuthorizationModelSource::new(
//!     "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse::<StoreId>()?,
//!     "01ARZ3NDEKTSV4RRFFQ69G5FAW".parse::<AuthorizationModelId>()?,
//!     "1.1".to_owned(),
//!     vec![
//!         TypeDefinitionSource::new("user".parse::<TypeName>()?, Vec::new()),
//!         TypeDefinitionSource::new(
//!             "document".parse::<TypeName>()?,
//!             vec![RelationSource::new(
//!                 "viewer".parse::<RelationName>()?,
//!                 RewriteSource::Direct,
//!                 vec![DirectRestrictionSource::new(
//!                     "user".parse::<TypeName>()?,
//!                     RestrictionKindSource::Object,
//!                     None,
//!                 )],
//!             )],
//!         ),
//!     ],
//!     Vec::new(),
//! );
//! let model = ModelCompiler::default().compile(&source)?;
//! let viewer = model.relation_id(
//!     &"document".parse::<TypeName>()?,
//!     &"viewer".parse::<RelationName>()?,
//! )?;
//!
//! assert_eq!(model.relation(viewer)?.name().as_str(), "viewer");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![forbid(unsafe_code)]

mod compiler;
mod error;
mod graph;
mod ir;
mod limits;
mod source;
mod tuple_validation;

pub use compiler::{CompiledModel, MODEL_COMPILER_FORMAT_VERSION, ModelCompiler};
pub use error::{DeclarationPath, ModelError, ModelErrorCode, ModelErrors, ModelLookupError};
pub use ir::{
    CompiledRelation, ConditionId, ConditionRequirement, DirectRestriction, NodeId, RelationId,
    RestrictionKind, RewriteNode, TypeId,
};
pub use limits::{ModelLimits, ModelLimitsBuilder};
pub use source::{
    AuthorizationModelDefinition, AuthorizationModelSource, ConditionSource,
    DirectRestrictionSource, RelationSource, RestrictionKindSource, RewriteSource,
    TypeDefinitionSource,
};
pub use tuple_validation::{TupleValidationError, TupleValidationErrorKind};
