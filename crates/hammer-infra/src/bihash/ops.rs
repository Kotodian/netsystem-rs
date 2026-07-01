//! Hot-path operations: lookup.

use crate::bihash::{Bihash, BihashKey, PageId};

impl<K: BihashKey + Default, V: Copy + Eq + Default, const KVP: usize> Bihash<K, V, KVP> {
    /// Read-only lookup. Phase 1 is `&self`-bound (exclusive-borrow model,
    /// same as `FlatHashTable::lookup` today).
    #[inline(always)]
    pub fn lookup(&self, key: &K) -> Option<V> {
        self.lookup_with_hash(*key, key.hash())
    }

    /// Lookup with a precomputed hash. Useful when the same hash is reused
    /// across a prefetch + lookup pair on the hot path.
    #[inline(always)]
    pub fn lookup_with_hash(&self, key: K, hash: u64) -> Option<V>
    where
        K: Copy,
    {
        if self.buckets.is_empty() {
            return None;
        }
        let bucket_idx = (hash as u32) & (self.nbuckets - 1);
        let bucket = self.buckets[bucket_idx as usize];
        if bucket.is_empty() {
            return None;
        }
        let page_id = PageId(bucket.offset() as u32);
        if page_id.is_none() {
            return None;
        }
        let log2_pages = bucket.log2_pages();
        let linear = bucket.is_linear_search();
        let page_offset = if linear {
            0u32
        } else {
            // Use bits above `log2_nbuckets` to select a page within this
            // bucket's page run.
            ((hash >> (self.log2_nbuckets as u32)) as u32) & ((1u32 << log2_pages) - 1)
        };
        let first_page = PageId(page_id.0 + page_offset);
        let limit = if linear { 1u32 << log2_pages } else { 1 };
        for rel in 0..limit {
            let cur = PageId(first_page.0 + rel);
            let page = self.pages.get(cur);
            for kv in page.slots() {
                // Skip free slots.
                if kv.value == V::default() {
                    continue;
                }
                if K::key_eq(kv.key, key) {
                    return Some(kv.value);
                }
            }
        }
        None
    }
}
