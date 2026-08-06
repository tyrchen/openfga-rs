#![no_main]
// `fuzz_target!` owns its artifact file; production OpenFGA code performs no file I/O here.
#![allow(clippy::disallowed_types)]

use libfuzzer_sys::fuzz_target;
use openfga_domain::{
    AuthorizationModelId, ChangeId, ConditionContext, ConditionName, ContextValue,
    ContinuationCursor, ContinuationScope, CorrelationId, Fingerprint, InputLimits, ObjectId,
    ObjectRef, ParameterName, PrincipalId, RelationName, StoreId, SubjectRef, TokenCodec, TokenKey,
    TokenKeyId, TokenOperation, TupleKey, TypeName,
};

fuzz_target!(|data: &[u8]| {
    let limits = InputLimits::default();
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = text.parse::<StoreId>();
        let _ = text.parse::<AuthorizationModelId>();
        let _ = text.parse::<ChangeId>();
        let _ = text.parse::<TypeName>();
        let _ = text.parse::<RelationName>();
        let _ = text.parse::<ConditionName>();
        let _ = text.parse::<ParameterName>();
        let _ = text.parse::<ObjectId>();
        let _ = text.parse::<CorrelationId>();
        let _ = text.parse::<PrincipalId>();
        let _ = ObjectRef::parse_with_limits(text, &limits);
        let _ = SubjectRef::parse_with_limits(text, &limits);
        let _ = TupleKey::parse_with_limits(text, &limits);
    }

    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) {
        let _ = ContextValue::try_from_json(value.clone(), &limits);
        let _ = ConditionContext::try_from_json(value, &limits);
    }

    let signing_key_id = "fuzz-key".parse::<TokenKeyId>();
    let store_id = "01G5JAVJ41T49E9TT3SKVS7X1J".parse::<StoreId>();
    if let (Ok(signing_key_id), Ok(store_id)) = (signing_key_id, store_id) {
        let signing_key = TokenKey::new(signing_key_id, vec![0xA5; 32]);
        if let Ok(signing_key) = signing_key {
            let codec = TokenCodec::new(signing_key, Vec::new(), &limits);
            if let Ok(codec) = codec {
                let scope = ContinuationScope::new(
                    TokenOperation::ReadTuples,
                    store_id,
                    Fingerprint::from_bytes([0x5A; 32]),
                );
                if let Ok(text) = std::str::from_utf8(data) {
                    let _ = codec.decode(text, &scope, 1_000);
                }
                let bounded_cursor = data.iter().take(1_024).copied().collect::<Vec<_>>();
                if let Ok(cursor) = ContinuationCursor::new(bounded_cursor, 2_000, &limits)
                    && let Ok(token) = codec.encode(&scope, &cursor)
                {
                    let _ = codec.decode(&token, &scope, 1_000);
                }
            }
        }
    }
});
