//! Authenticated, scope-bound conversion between protocol tokens and storage cursors.

use std::{
    num::NonZeroU32,
    time::{SystemTime, UNIX_EPOCH},
};

use openfga_domain::{
    ContinuationCursor, ContinuationScope, Fingerprint, InputLimits, StoreId, TokenCodec,
    TokenOperation,
};
use openfga_storage::{PageOptions, StorageCursor};

use crate::ApiError;

/// Global sentinel used only in the separately tagged `ListStores` token scope.
pub(crate) const GLOBAL_SCOPE_STORE: StoreId = StoreId::from_ulid(ulid::Ulid::nil());
const MAXIMUM_PAGE_SIZE: u32 = 100;

pub(crate) fn scope(
    operation: TokenOperation,
    store_id: StoreId,
    filter: Fingerprint,
) -> ContinuationScope {
    ContinuationScope::new(operation, store_id, filter)
}

pub(crate) fn page_options(
    requested: Option<i32>,
    token: &str,
    scope: &ContinuationScope,
    codec: &TokenCodec,
    limits: &InputLimits,
    default_size: NonZeroU32,
) -> Result<PageOptions, ApiError> {
    let size = match requested {
        None => default_size,
        Some(value) => {
            let value = u32::try_from(value).map_err(|_| ApiError::invalid_page_size())?;
            if value > MAXIMUM_PAGE_SIZE {
                return Err(ApiError::invalid_page_size());
            }
            NonZeroU32::new(value).ok_or_else(ApiError::invalid_page_size)?
        }
    };
    let after = if token.is_empty() {
        None
    } else {
        let cursor = codec
            .decode(token, scope, now_unix_seconds()?)
            .map_err(|_| ApiError::invalid_continuation())?;
        Some(
            StorageCursor::new(cursor.as_bytes().to_vec())
                .map_err(|_| ApiError::invalid_continuation())?,
        )
    };
    PageOptions::new(size, after, limits).map_err(|_| ApiError::invalid_request())
}

pub(crate) fn continuation_token(
    cursor: Option<&StorageCursor>,
    scope: &ContinuationScope,
    codec: &TokenCodec,
    ttl_seconds: u64,
    limits: &InputLimits,
) -> Result<String, ApiError> {
    let Some(cursor) = cursor else {
        return Ok(String::new());
    };
    let expires_at = now_unix_seconds()?
        .checked_add(ttl_seconds)
        .ok_or_else(ApiError::invalid_request)?;
    let continuation = ContinuationCursor::new(cursor.as_bytes().to_vec(), expires_at, limits)
        .map_err(|_| ApiError::invalid_continuation())?;
    codec
        .encode(scope, &continuation)
        .map_err(|_| ApiError::invalid_continuation())
}

fn now_unix_seconds() -> Result<u64, ApiError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| ApiError::invalid_request())
}
