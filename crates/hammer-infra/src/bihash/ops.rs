//! Hot-path operations: lookup, insert, remove, clear.

use crate::bihash::{Bihash, BihashFree, BihashKey, Bucket, Kv, PageAlloc, PageId};

impl<K: BihashKey + Default, V: Copy + Eq + Default + BihashFree, const KVP: usize>
    Bihash<K, V, KVP>
{
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
                if kv_slot_is_free(kv) {
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

/// Helper for generic-V free detection — checks if a KV slot is free via
/// the `BihashFree` trait's sentinel (e.g. `FREE_U64` for `u64`).
#[inline(always)]
pub(super) fn kv_slot_is_free<K, V: BihashFree>(kv: &Kv<K, V>) -> bool {
    kv.value.is_free_value()
}

impl<K: BihashKey + Default, V: Copy + Eq + Default + BihashFree, const KVP: usize>
    Bihash<K, V, KVP>
{
    /// Insert or overwrite a key. VPP semantics: `is_add=1` (always overwrite).
    pub fn insert(&mut self, key: K, value: V) {
        let hash = key.hash();
        let bucket_idx = (hash as u32) & (self.nbuckets - 1);
        let bucket = self.buckets[bucket_idx as usize];

        if bucket.is_empty() {
            let page_id = self.pages.alloc_single(0);
            let page = self.pages.get_mut(page_id);
            page.slots_mut()[0] = Kv { key, value };
            self.buckets[bucket_idx as usize] =
                Bucket::pack(page_id.0 as u64, 0, 1, 0, false, false);
            self.len += 1;
            return;
        }

        let page_id = PageId(bucket.offset() as u32);
        let log2_pages = bucket.log2_pages();
        let page_offset =
            ((hash >> (self.log2_nbuckets as u32)) as u32) & ((1u32 << log2_pages) - 1);
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

    /// Remove a key from the table. Returns `true` if the key was found and
    /// removed, `false` if it was absent.
    pub fn remove(&mut self, key: &K) -> bool {
        let hash = key.hash();
        let bucket_idx = (hash as u32) & (self.nbuckets - 1);
        let bucket = self.buckets[bucket_idx as usize];
        if bucket.is_empty() {
            return false;
        }
        let page_id = PageId(bucket.offset() as u32);
        let log2_pages = bucket.log2_pages();
        let linear = bucket.is_linear_search();
        let page_offset = if linear {
            0u32
        } else {
            ((hash >> (self.log2_nbuckets as u32)) as u32) & ((1u32 << log2_pages) - 1)
        };
        let limit = if linear { 1u32 << log2_pages } else { 1 };

        for rel in 0..limit {
            let cur = PageId(page_id.0 + page_offset + rel);
            let page = self.pages.get_mut(cur);
            for slot in page.slots_mut() {
                if slot.value.is_free_value() {
                    continue;
                }
                if K::key_eq(slot.key, *key) {
                    slot.value = V::free_sentinel();
                    if !linear {
                        self.buckets[bucket_idx as usize] = Bucket::pack(
                            page_id.0 as u64,
                            log2_pages,
                            bucket.refcnt() - 1,
                            bucket.generation().wrapping_add(1) & 0x1F,
                            false,
                            false,
                        );
                    }
                    self.len -= 1;
                    return true;
                }
            }
        }
        false
    }

    /// Remove all entries from the table. Resets every bucket to empty and
    /// drops every page back to the allocator by replacing `PageAlloc` with a
    /// fresh instance. The table is left usable for further inserts.
    pub fn clear(&mut self) {
        for b in self.buckets.iter_mut() {
            *b = Bucket::empty();
        }
        self.pages = PageAlloc::new();
        self.len = 0;
    }
}
