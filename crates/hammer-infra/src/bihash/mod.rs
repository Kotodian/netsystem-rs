//! VPP-style bounded-index extensible hash table (bihash).
//!
//! See `third_party/vpp/src/vppinfra/bihash_template_inlines.h` for the
//! reference algorithm. This Rust implementation matches the C semantics
//! bucket-for-bucket but uses const generics for template instantiation
//! instead of the C preprocessor.

pub mod key;
pub mod bucket;
pub mod value;
pub mod alloc;

pub use key::BihashKey;
pub use bucket::Bucket;
pub use value::{Kv, ValuePage, FREE_U64};
pub use alloc::{PageAlloc, PageId};

/// A bounded-index extensible hash table.
///
/// `K`:      the key type (must implement `BihashKey`, `Copy`, `Eq`).
/// `V`:      the value type (must be `Copy`; `Eq` enables sentinel-based free-slot detection).
/// `KVP`:    the number of KV pairs that fit in one value page — chosen so the page
///           fits in a single cache line for performance. See `template.rs` for
///           recommended values per `(K, V)` pair.
pub struct Bihash<K: BihashKey, V: Copy + Eq, const KVP: usize> {
    buckets: Vec<Bucket>,
    pages: PageAlloc<K, V, KVP>,
    len: usize,
    nbuckets: u32,
    log2_nbuckets: u8,
}

impl<K: BihashKey + Default, V: Copy + Eq + Default, const KVP: usize> Bihash<K, V, KVP> {
    /// Create a bihash with at least `nbuckets` buckets. `nbuckets` is
    /// rounded up to the next power of two; `log2_nbuckets` is stored and
    /// used to select bucket indices from hash bits.
    pub fn new(mut nbuckets: u32) -> Self {
        if nbuckets == 0 {
            nbuckets = 1;
        }
        let actual_buckets = nbuckets.next_power_of_two();
        let log2 = actual_buckets.trailing_zeros() as u8;
        Self {
            buckets: vec![Bucket::empty(); actual_buckets as usize],
            pages: PageAlloc::new(),
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
}
