//! VPP-style bounded-index extensible hash table (bihash).
//!
//! See `third_party/vpp/src/vppinfra/bihash_template_inlines.h` for the
//! reference algorithm. This Rust implementation matches the C semantics
//! bucket-for-bucket but uses const generics for template instantiation
//! instead of the C preprocessor.
//!
//! Concurrency (VPP-shaped):
//! - Lookups load `AtomicBucket` and never take a mutex.
//! - Writers CAS the per-bucket lock bit, mutate pages, then publish an
//!   unlocked bucket word (generation bump).
//! - Page-arena push/free uses a spin bit (allocator sync only).

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
pub use bucket::{AtomicBucket, Bucket};
pub use iter::BihashIter;
pub use key::{BihashKey, hash_words, splitmix64};
pub use template::{Bihash8x8, Bihash16x8, Bihash24x8, Bihash48x8};
pub use value::{FREE_U64, Kv, ValuePage};

use crate::boxed::Slice;
use crate::heap::Heap;

/// A bounded-index extensible hash table.
///
/// `K`:      the key type (must implement `BihashKey`, `Copy`, `Eq`).
/// `KVP`:    the number of KV pairs that fit in one value page — chosen so the page
///           fits in a single cache line for performance. See `template.rs` for
///           recommended values per key shape.
pub struct Bihash<K: BihashKey, const KVP: usize> {
    buckets: Slice<AtomicBucket>,
    pages: UnsafeCell<PageAlloc<K, KVP>>,
    /// Spin bit for PageAlloc push/free only — not a table reader lock.
    alloc_busy: AtomicBool,
    len: AtomicUsize,
    nbuckets: u32,
    log2_nbuckets: u8,
    heap: Arc<Heap>,
}

// SAFETY: bucket lock bit + alloc_busy establish exclusive mutation of pages;
// readers only observe unlocked buckets via Acquire loads.
unsafe impl<K: BihashKey + Send, const KVP: usize> Sync for Bihash<K, KVP> {}

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    /// Create a bihash with at least `nbuckets` buckets. `nbuckets` is
    /// rounded up to the next power of two; `log2_nbuckets` is stored and
    /// used to select bucket indices from hash bits.
    #[inline]
    pub fn new(nbuckets: u32) -> Self {
        Self::with_capacity_in(nbuckets, Arc::new(Heap::local()))
    }

    pub fn with_capacity_in(mut nbuckets: u32, heap: Arc<Heap>) -> Self {
        if nbuckets == 0 {
            nbuckets = 1;
        }
        let actual_buckets = nbuckets.next_power_of_two();
        let log2 = actual_buckets.trailing_zeros() as u8;
        let buckets = Slice::from_fn(actual_buckets as usize, |_| {
            AtomicBucket::new(Bucket::empty())
        });
        Self {
            buckets,
            pages: UnsafeCell::new(PageAlloc::new_in(heap.clone())),
            alloc_busy: AtomicBool::new(false),
            len: AtomicUsize::new(0),
            nbuckets: actual_buckets,
            log2_nbuckets: log2,
            heap,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn nbuckets(&self) -> u32 {
        self.nbuckets
    }

    #[inline]
    pub(crate) fn log2_nbuckets(&self) -> u8 {
        self.log2_nbuckets
    }

    #[inline]
    pub(crate) fn heap(&self) -> Arc<Heap> {
        self.heap.clone()
    }

    #[inline]
    pub(crate) fn atomic_bucket(&self, idx: usize) -> &AtomicBucket {
        &self.buckets[idx]
    }

    #[inline]
    pub(crate) fn load_bucket(&self, idx: usize) -> Bucket {
        self.buckets[idx].load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn store_bucket(&self, idx: usize, bucket: Bucket) {
        self.buckets[idx].store(bucket, Ordering::Release);
    }

    #[inline]
    pub(crate) fn cas_bucket(
        &self,
        idx: usize,
        current: Bucket,
        new: Bucket,
    ) -> Result<Bucket, Bucket> {
        self.buckets[idx].compare_exchange(
            current,
            new,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
    }

    /// Lock the bucket word (VPP lock bit). Returns the locked snapshot.
    pub(crate) fn lock_bucket(&self, idx: usize) -> Bucket {
        loop {
            let cur = self.load_bucket(idx);
            if cur.is_locked() {
                core::hint::spin_loop();
                continue;
            }
            let locked = Bucket::pack(
                cur.offset(),
                cur.log2_pages(),
                cur.refcnt(),
                cur.generation(),
                cur.is_linear_search(),
                true,
            );
            match self.cas_bucket(idx, cur, locked) {
                Ok(_) => return locked,
                Err(_) => core::hint::spin_loop(),
            }
        }
    }

    #[inline]
    pub(crate) fn pages(&self) -> &PageAlloc<K, KVP> {
        unsafe { &*self.pages.get() }
    }

    /// SAFETY: caller holds the bucket lock bit for every page it mutates, and
    /// holds `alloc_busy` across any PageAlloc push/free.
    #[inline]
    pub(crate) unsafe fn pages_mut(&self) -> &mut PageAlloc<K, KVP> {
        unsafe { &mut *self.pages.get() }
    }

    pub(crate) fn with_alloc_mut<R>(&self, f: impl FnOnce(&mut PageAlloc<K, KVP>) -> R) -> R {
        while self
            .alloc_busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        let result = f(unsafe { &mut *self.pages.get() });
        self.alloc_busy.store(false, Ordering::Release);
        result
    }

    #[inline]
    pub(crate) fn add_len(&self, delta: isize) {
        if delta >= 0 {
            self.len.fetch_add(delta as usize, Ordering::Relaxed);
        } else {
            self.len.fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn set_len(&self, len: usize) {
        self.len.store(len, Ordering::Relaxed);
    }

    /// Snapshot-style iterator over `(&K, &V)` pairs.
    #[inline]
    pub fn iter(&self) -> BihashIter<'_, K, KVP> {
        BihashIter::new(self)
    }
}
