//! Monotonic invalidation generation shared by all mutable caches.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Process-local monotonic generation for mutable cache entries.
///
/// The initial implementation advances one global generation for every
/// observed store change. This deliberately over-invalidates entries for other
/// stores while making missed scope relationships impossible. Store-scoped
/// markers may replace it only after their invalidation coverage is proven.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct InvalidationWatermark(Arc<AtomicU64>);

impl InvalidationWatermark {
    /// Creates a zero-generation watermark.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current generation with acquire ordering.
    #[must_use]
    pub fn current(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// Conservatively invalidates all mutable entries and returns the new generation.
    #[must_use]
    pub fn advance(&self) -> u64 {
        self.0.fetch_add(1, Ordering::AcqRel).saturating_add(1)
    }
}
