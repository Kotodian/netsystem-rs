//! Hot-path operations: lookup, insert, remove, clear.

use crate::bihash::{Bihash, BihashKey, Bucket, FREE_U64, Kv, PageAlloc, PageId};
use crate::prefetch::prefetch_read_l1;

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    #[inline(always)]
    pub fn prefetch(&self, key: &K) {
        self.prefetch_with_hash(key.hash());
    }

    #[inline(always)]
    pub fn prefetch_with_hash(&self, hash: u64) {
        if self.buckets.is_empty() {
            return;
        }
        let bucket_idx = (hash as u32) & (self.nbuckets - 1);
        let bucket_ptr = self.buckets.as_ptr().wrapping_add(bucket_idx as usize);
        prefetch_read_l1(bucket_ptr);
    }

    /// Read-only lookup. Phase 1 is `&self`-bound (exclusive-borrow model,
    /// same as `FlatHashTable::lookup` today).
    #[inline(always)]
    pub fn lookup(&self, key: &K) -> Option<u64> {
        self.lookup_with_hash(*key, key.hash())
    }

    /// Lookup with a precomputed hash. Useful when the same hash is reused
    /// across a prefetch + lookup pair on the hot path.
    #[inline(always)]
    pub fn lookup_with_hash(&self, key: K, hash: u64) -> Option<u64>
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
                if kv.key == key {
                    return Some(kv.value);
                }
            }
        }
        None
    }
}

#[inline(always)]
pub(super) fn kv_slot_is_free<K>(kv: &Kv<K>) -> bool {
    kv.is_free()
}

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    /// Insert or overwrite a key. VPP semantics: `is_add=1` (always overwrite).
    pub fn insert(&mut self, key: K, value: u64) {
        debug_assert_ne!(value, FREE_U64);
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
        let linear = bucket.is_linear_search();
        let page_offset = if linear {
            0u32
        } else {
            ((hash >> (self.log2_nbuckets as u32)) as u32) & ((1u32 << log2_pages) - 1)
        };
        let first_page = PageId(page_id.0 + page_offset);
        let limit = if linear { 1u32 << log2_pages } else { 1 };

        let mut free_slot: Option<(PageId, usize)> = None;
        for rel in 0..limit {
            let cur = PageId(first_page.0 + rel);
            let page = self.pages.get(cur);
            for (i, kv) in page.slots().iter().enumerate() {
                if kv_slot_is_free(kv) {
                    if free_slot.is_none() {
                        free_slot = Some((cur, i));
                    }
                    continue;
                }
                if kv.key == key {
                    self.pages.get_mut(cur).slots_mut()[i].value = value;
                    return;
                }
            }
        }
        if let Some((pid, i)) = free_slot {
            self.pages.get_mut(pid).slots_mut()[i] = Kv { key, value };
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
                if slot.is_free() {
                    continue;
                }
                if slot.key == *key {
                    slot.mark_free();
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
        for b in self.buckets.as_mut_slice() {
            *b = Bucket::empty();
        }
        let heap = self.pages.heap();
        self.pages = PageAlloc::new_in(heap);
        self.len = 0;
    }
}
