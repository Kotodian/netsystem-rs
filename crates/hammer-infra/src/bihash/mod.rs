//! VPP-style bounded-index extensible hash table (bihash).
//!
//! See `third_party/vpp/src/vppinfra/bihash_template_inlines.h` for the
//! reference algorithm. This Rust implementation matches the C semantics
//! bucket-for-bucket but uses const generics for template instantiation
//! instead of the C preprocessor.

use std::sync::Arc;

pub mod alloc;
pub mod bucket;
pub mod iter;
pub mod key;
pub mod ops;
pub mod split;
pub mod template;
pub mod value;

pub use alloc::{PageAlloc, PageId};
pub use bucket::Bucket;
pub use iter::BihashIter;
pub use key::BihashKey;
pub use template::{Bihash16x8, Bihash24x8, Bihash48x8, Bihash8x8};
pub use value::{Kv, ValuePage, FREE_U64};

use crate::boxed::Slice;
use crate::heap::Heap;

/// A bounded-index extensible hash table.
///
/// `K`:      the key type (must implement `BihashKey`, `Copy`, `Eq`).
/// `KVP`:    the number of KV pairs that fit in one value page — chosen so the page
///           fits in a single cache line for performance. See `template.rs` for
///           recommended values per key shape.
pub struct Bihash<K: BihashKey, const KVP: usize> {
    buckets: Slice<Bucket>,
    pages: PageAlloc<K, KVP>,
    heap: Arc<Heap>,
    len: usize,
    nbuckets: u32,
    log2_nbuckets: u8,
}

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    /// Create a bihash with at least `nbuckets` buckets. `nbuckets` is
    /// rounded up to the next power of two; `log2_nbuckets` is stored and
    /// used to select bucket indices from hash bits.
    pub fn new(nbuckets: u32) -> Self {
        Self::with_capacity_in(nbuckets, Arc::new(Heap::local()))
    }

    pub fn with_capacity_in(mut nbuckets: u32, heap: Arc<Heap>) -> Self {
        if nbuckets == 0 {
            nbuckets = 1;
        }
        let actual_buckets = nbuckets.next_power_of_two();
        let log2 = actual_buckets.trailing_zeros() as u8;
        Self {
            buckets: Slice::from_elem_in(actual_buckets as usize, Bucket::empty(), heap.clone()),
            pages: PageAlloc::new_in(heap.clone()),
            heap,
            len: 0,
            nbuckets: actual_buckets,
            log2_nbuckets: log2,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn nbuckets(&self) -> u32 {
        self.nbuckets
    }

    #[inline]
    pub(crate) fn heap(&self) -> Arc<Heap> {
        self.heap.clone()
    }

    /// Internal: shared access to the bucket array. Used by the iterator
    /// (`crate::bihash::iter`).
    #[inline]
    pub(crate) fn buckets(&self) -> &[Bucket] {
        self.buckets.as_slice()
    }

    /// Internal: shared access to the page allocator. Used by the iterator
    /// (`crate::bihash::iter`).
    #[inline]
    pub(crate) fn pages(&self) -> &PageAlloc<K, KVP> {
        &self.pages
    }

    /// Snapshot-style iterator over `(&K, &V)` pairs. Iteration order is
    /// bucket index order, then page index order within a bucket, then slot
    /// index order within a page — i.e. deterministic but not insertion
    /// order. Free slots are skipped.
    #[inline]
    pub fn iter(&self) -> BihashIter<'_, K, KVP> {
        BihashIter::new(self)
    }
}
