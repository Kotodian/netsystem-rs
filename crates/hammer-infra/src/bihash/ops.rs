//! Hot-path operations: lookup.

use crate::bihash::{Bihash, BihashKey, Bucket, Kv, PageId};

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

/// Helper for generic-V free detection — checks if a KV slot is free.
/// Phase 1 uses `V::default()`. Phase 2 will use a sentinel trait.
#[inline(always)]
fn kv_slot_is_free<K, V>(kv: &Kv<K, V>) -> bool
where
    V: Copy + Eq + Default,
{
    kv.value == V::default()
}

impl<K: BihashKey + Default, V: Copy + Eq + Default, const KVP: usize> Bihash<K, V, KVP> {
    /// Insert or overwrite a key. VPP semantics: `is_add=1` (always overwrite).
    pub fn insert(&mut self, key: K, value: V) {
        let hash = key.hash();
        let bucket_idx = (hash as u32) & (self.nbuckets - 1);
        let bucket = self.buckets[bucket_idx as usize];

        if bucket.is_empty() {
            let page_id = self.pages.alloc_single(0);
            let page = self.pages.get_mut(page_id);
            page.slots_mut()[0] = Kv { key, value };
            self.buckets[bucket_idx as usize] = Bucket::pack(
                page_id.0 as u64, 0, 1, 0, false, false,
            );
            self.len += 1;
            return;
        }

        let page_id = PageId(bucket.offset() as u32);
        let log2_pages = bucket.log2_pages();
        let page_offset = ((hash >> (self.log2_nbuckets as u32)) as u32)
            & ((1u32 << log2_pages) - 1);
        let target = PageId(page_id.0 + page_offset);
        let page = self.pages.get_mut(target);

        let mut free_idx: Option<usize> = None;
        for (i, kv) in page.slots().iter().enumerate() {
            if kv_slot_is_free(kv) {
                if free_idx.is_none() {
                    free_idx = Some(i);
                }
                continue;
            }
            if K::key_eq(kv.key, key) {
                page.slots_mut()[i].value = value;
                return;
            }
        }
        if let Some(i) = free_idx {
            page.slots_mut()[i] = Kv { key, value };
            self.buckets[bucket_idx as usize] = Bucket::pack(
                page_id.0 as u64,
                log2_pages,
                bucket.refcnt() + 1,
                bucket.generation().wrapping_add(1) & 0x1F,
                bucket.is_linear_search(),
                false,
            );
            self.len += 1;
            return;
        }

        self.split_and_rehash(bucket_idx, key, value);
    }
}

impl<K: BihashKey + Default, V: Copy + Eq + Default, const KVP: usize> Bihash<K, V, KVP> {
    #[allow(unused_variables)]
    fn split_and_rehash(&mut self, bucket_idx: u32, key: K, value: V) {
        unimplemented!("Task 6: bucket split on overflow");
    }
}
