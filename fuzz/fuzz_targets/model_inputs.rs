#![no_main]
// `fuzz_target!` owns its artifact file; production model code performs no file I/O here.
#![allow(clippy::disallowed_types)]

use libfuzzer_sys::fuzz_target;
use openfga_domain::{AuthorizationModelId, RelationName, StoreId, TypeName};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};

const MAX_GENERATED_RELATIONS: usize = 96;

fuzz_target!(|data: &[u8]| {
    let (Ok(store_id), Ok(model_id), Ok(user), Ok(document)) = (
        "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse::<StoreId>(),
        "01ARZ3NDEKTSV4RRFFQ69G5FAW".parse::<AuthorizationModelId>(),
        "user".parse::<TypeName>(),
        "document".parse::<TypeName>(),
    ) else {
        return;
    };
    let count = data.len().min(MAX_GENERATED_RELATIONS);
    let mut relations = Vec::with_capacity(count.max(1));
    let Ok(first_name) = "r0".parse::<RelationName>() else {
        return;
    };
    relations.push(RelationSource::new(
        first_name,
        RewriteSource::Direct,
        vec![DirectRestrictionSource::new(
            user.clone(),
            RestrictionKindSource::Object,
            None,
        )],
    ));
    for (index, byte) in data.iter().take(count.saturating_sub(1)).enumerate() {
        let current = index.saturating_add(1);
        let previous = index;
        let (Ok(name), Ok(previous_name)) = (
            format!("r{current}").parse::<RelationName>(),
            format!("r{previous}").parse::<RelationName>(),
        ) else {
            return;
        };
        let (rewrite, restrictions) = match byte % 5 {
            0 => (RewriteSource::Computed(previous_name), Vec::new()),
            1 => (
                RewriteSource::Union(vec![
                    RewriteSource::Direct,
                    RewriteSource::Computed(previous_name),
                ]),
                vec![DirectRestrictionSource::new(
                    user.clone(),
                    RestrictionKindSource::Object,
                    None,
                )],
            ),
            2 => (
                RewriteSource::Intersection(vec![
                    RewriteSource::Direct,
                    RewriteSource::Computed(previous_name),
                ]),
                vec![DirectRestrictionSource::new(
                    user.clone(),
                    RestrictionKindSource::Object,
                    None,
                )],
            ),
            3 => (
                RewriteSource::Difference {
                    base: Box::new(RewriteSource::Computed(previous_name)),
                    subtract: Box::new(RewriteSource::Direct),
                },
                vec![DirectRestrictionSource::new(
                    user.clone(),
                    RestrictionKindSource::Object,
                    None,
                )],
            ),
            _ => (
                RewriteSource::Direct,
                vec![DirectRestrictionSource::new(
                    document.clone(),
                    RestrictionKindSource::Userset(previous_name),
                    None,
                )],
            ),
        };
        relations.push(RelationSource::new(name, rewrite, restrictions));
    }
    let source = AuthorizationModelSource::new(
        store_id,
        model_id,
        "1.1".to_owned(),
        vec![
            TypeDefinitionSource::new(user, Vec::new()),
            TypeDefinitionSource::new(document, relations),
        ],
        Vec::new(),
    );
    let _ = ModelCompiler::default().compile(&source);
});
