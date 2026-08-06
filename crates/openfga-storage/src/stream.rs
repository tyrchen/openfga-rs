//! Owned bounded tuple snapshots with idempotent close semantics.

use std::{collections::VecDeque, fmt};

use openfga_domain::RelationshipTuple;

use crate::StorageError;

/// Project-owned fallible tuple stream.
///
/// Backends may release database resources before returning an owned batch. A
/// stream can still contain a terminal item error, and closing or dropping it
/// immediately discards every remaining owned row.
#[non_exhaustive]
pub struct TupleStream {
    items: VecDeque<Result<RelationshipTuple, StorageError>>,
    closed: bool,
}

impl TupleStream {
    /// Creates a bounded owned stream from successful tuple rows.
    #[must_use]
    pub fn from_tuples(tuples: Vec<RelationshipTuple>) -> Self {
        let closed = tuples.is_empty();
        Self {
            items: tuples.into_iter().map(Ok).collect(),
            closed,
        }
    }

    /// Creates a bounded owned stream that may carry a terminal item failure.
    #[must_use]
    pub fn from_results(items: Vec<Result<RelationshipTuple, StorageError>>) -> Self {
        let closed = items.is_empty();
        Self {
            items: items.into(),
            closed,
        }
    }

    /// Returns the next tuple or item failure.
    pub fn next_item(&mut self) -> Option<Result<RelationshipTuple, StorageError>> {
        if self.closed {
            None
        } else {
            let next = self.items.pop_front();
            if self.items.is_empty() {
                self.closed = true;
            }
            next
        }
    }

    /// Closes the stream. Repeated calls are harmless.
    pub fn close(&mut self) {
        self.items.clear();
        self.closed = true;
    }

    /// Returns whether the stream has been explicitly closed.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Returns the number of currently owned unread items.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.items.len()
    }
}

impl Iterator for TupleStream {
    type Item = Result<RelationshipTuple, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item()
    }
}

impl Drop for TupleStream {
    fn drop(&mut self) {
        self.close();
    }
}

impl fmt::Debug for TupleStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TupleStream")
            .field("remaining", &self.items.len())
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
