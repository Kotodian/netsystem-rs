//! Cache-line-aligned value record shared by direct metric handles.
//!
//! The directory slot owns identity and generation. This record stores the
//! generation copied into the metric block plus the value cells. All fields
//! are atomics so a shared `&MetricValue` is sound.

use std::sync::atomic::{AtomicU64, Ordering};

/// Size of one value record in bytes (the record is 64-byte aligned).
pub(crate) const VALUE_RECORD_BYTES: u64 = std::mem::size_of::<MetricValue>() as u64;

/// One 64-byte value record: generation and value, with reserved padding.
#[repr(C)]
#[repr(align(64))]
pub(crate) struct MetricValue {
    generation: AtomicU64,
    value: AtomicU64,
    reserved: [AtomicU64; 6],
}

impl MetricValue {
    pub(crate) fn new(generation: u64) -> MetricValue {
        MetricValue {
            generation: AtomicU64::new(generation),
            value: AtomicU64::new(0),
            reserved: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
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
