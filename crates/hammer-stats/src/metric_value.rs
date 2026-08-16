//! Cache-line-aligned value record shared by `Counter` and `Gauge`.
//!
//! This record is the sole indirection between metric handles and the
//! directory: handles outlive directory relocation, so they hold the value
//! record's mapping offset rather than any pointer derived from the
//! directory. All fields are atomics so a shared `&MetricValue` is sound.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::StatsError;

/// Size of one value record in bytes (the record is 64-byte aligned).
pub(crate) const VALUE_RECORD_BYTES: u64 = std::mem::size_of::<MetricValue>() as u64;

/// One 64-byte value record: generation, live handle count, value.
///
/// The generation starts at 1 per block and is bumped by the structural
/// writer on removal, which invalidates every outstanding handle. The
/// reference count tracks live `Counter`/`Gauge` handles (including
/// clones); a removed block is released only when it reaches zero.
#[repr(C)]
#[repr(align(64))]
pub(crate) struct MetricValue {
    generation: AtomicU64,
    refs: AtomicU64,
    value: AtomicU64,
    reserved: [AtomicU64; 5],
}

impl MetricValue {
    pub(crate) fn new(generation: u64, refs: u64) -> MetricValue {
        MetricValue {
            generation: AtomicU64::new(generation),
            refs: AtomicU64::new(refs),
            value: AtomicU64::new(0),
            reserved: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub(crate) fn refs(&self) -> u64 {
        self.refs.load(Ordering::Relaxed)
    }

    /// Computes the next generation without storing it; the store happens
    /// only inside a publication.
    pub(crate) fn next_generation(&self) -> Result<u64, StatsError> {
        self.generation
            .load(Ordering::Relaxed)
            .checked_add(1)
            .ok_or(StatsError::GenerationOverflow)
    }

    /// Stores a generation previously computed by `next_generation`.
    /// Called only by the structural writer during a publication.
    pub(crate) fn store_generation(&self, generation: u64) {
        self.generation.store(generation, Ordering::Relaxed);
    }

    /// Adds one live handle; fails instead of wrapping `u64::MAX`.
    pub(crate) fn try_add_ref(&self) -> Result<(), StatsError> {
        if self.refs.load(Ordering::Relaxed) == u64::MAX {
            return Err(StatsError::RefCountOverflow);
        }
        self.refs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Releases one live handle; infallible so `Drop` never panics.
    pub(crate) fn release_ref(&self) {
        self.refs.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn load_value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub(crate) fn add_value(&self, delta: u64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
    }

    pub(crate) fn store_value(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }
}
