//! Backend-independent storage contract runner for backend test suites.

use openfga_domain::{RelationshipTuple, StoreId};
use thiserror::Error;

use crate::{
    ChangeFilter, ChangeOperation, ChangeReader, ObjectRelationFilter, OperationContext,
    PageOptions, ReadOptions, StorageError, StorageErrorKind, TupleReader, TupleWriteOptions,
    TupleWriter,
};

/// Canonical inputs for the backend-independent tuple contract.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TupleContractFixture {
    store_id: StoreId,
    first: RelationshipTuple,
    second: RelationshipTuple,
    filter: ObjectRelationFilter,
    read_options: ReadOptions,
}

impl TupleContractFixture {
    /// Creates one canonical tuple contract fixture.
    #[must_use]
    pub const fn new(
        store_id: StoreId,
        first: RelationshipTuple,
        second: RelationshipTuple,
        filter: ObjectRelationFilter,
        read_options: ReadOptions,
    ) -> Self {
        Self {
            store_id,
            first,
            second,
            filter,
            read_options,
        }
    }
}

/// Shared contract runner failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ContractError {
    /// The backend returned a storage failure.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A backend result violated a shared semantic invariant.
    #[error("storage contract invariant failed: {0}")]
    Invariant(&'static str),
}

/// Verifies exact/forward reads, atomic writes, conflicts, deletes, and changelog order.
///
/// The caller creates an isolated store before invoking this function. The same
/// function is intended to run unchanged against memory and every SQL backend.
///
/// # Errors
///
/// Returns the backend failure or a stable contract invariant code.
pub async fn verify_tuple_contract<B>(
    backend: &B,
    context: &OperationContext,
    fixture: &TupleContractFixture,
) -> Result<(), ContractError>
where
    B: TupleReader + TupleWriter + ChangeReader + Sync,
{
    let outcome = backend
        .write_tuples(
            context,
            fixture.store_id,
            Vec::new(),
            vec![fixture.first.clone(), fixture.second.clone()],
            TupleWriteOptions::default(),
        )
        .await?;
    if outcome.change_ids().len() != 2 {
        return Err(ContractError::Invariant("initial_change_count"));
    }
    if backend
        .read_exact_tuple(context, fixture.store_id, fixture.first.key())
        .await?
        .tuple()
        != &fixture.first
    {
        return Err(ContractError::Invariant("exact_tuple_round_trip"));
    }
    let tuples = backend
        .read_object_relation(
            context,
            fixture.store_id,
            &fixture.filter,
            fixture.read_options,
        )
        .await?
        .collect::<Result<Vec<_>, StorageError>>()?;
    if tuples.len() != 2 {
        return Err(ContractError::Invariant("forward_tuple_count"));
    }
    let duplicate = backend
        .write_tuples(
            context,
            fixture.store_id,
            Vec::new(),
            vec![fixture.first.clone()],
            TupleWriteOptions::default(),
        )
        .await;
    if !matches!(duplicate, Err(error) if error.kind() == StorageErrorKind::Conflict) {
        return Err(ContractError::Invariant("duplicate_write_policy"));
    }
    backend
        .write_tuples(
            context,
            fixture.store_id,
            vec![fixture.first.key().clone()],
            Vec::new(),
            TupleWriteOptions::default(),
        )
        .await?;
    if backend
        .tuple_exists(context, fixture.store_id, fixture.first.key())
        .await?
    {
        return Err(ContractError::Invariant("delete_visibility"));
    }
    let page_options = PageOptions::from_read_options(fixture.read_options);
    let changes = backend
        .read_changes(
            context,
            fixture.store_id,
            &ChangeFilter::default(),
            &page_options,
        )
        .await?;
    if changes.items().len() != 3
        || changes
            .items()
            .last()
            .is_none_or(|change| change.operation() != ChangeOperation::Delete)
        || !changes
            .items()
            .windows(2)
            .all(|pair| matches!(pair, [left, right] if left.id() < right.id()))
    {
        return Err(ContractError::Invariant("ordered_atomic_changelog"));
    }
    Ok(())
}
