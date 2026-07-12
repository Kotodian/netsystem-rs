//! Hot-path operations: lookup, insert, remove, clear.
//!
//! Lookup is lock-free aside from retrying while a bucket lock bit is set.
//! Insert/remove CAS the bucket lock bit, mutate pages, then publish.

use crate::bihash::{
    AtomicBucket, Bihash, BihashKey, Bucket, FREE_U64, Kv, PageAlloc, PageId,
};
use crate::prefetch::prefetch_read_l1;

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    #[inline(always)]
    pub fn prefetch(&self, key: &K) {
        self.prefetch_with_hash(key.hash());
    }

    #[inline(always)]
    pub fn prefetch_with_hash(&self, hash: u64) {
        if self.nbuckets() == 0 {
            return;
        }
        let bucket_idx = (hash as u32) & (self.nbuckets() - 1);
        let bucket_ptr = self.atomic_bucket(bucket_idx as usize) as *const AtomicBucket;
        prefetch_read_l1(bucket_ptr);
    }

    /// Lock-free lookup with retry while the target bucket is writer-locked.
    #[inline(always)]
    pub fn lookup(&self, key: &K) -> Option<u64> {
        self.lookup_with_hash(*key, key.hash())
    }

    #[inline(always)]
    pub fn lookup_with_hash(&self, key: K, hash: u64) -> Option<u64>
    where
        K: Copy,
    {
        if self.nbuckets() == 0 {
            return None;
        }
        let bucket_idx = ((hash as u32) & (self.nbuckets() - 1)) as usize;
        loop {
            let bucket = self.load_bucket(bucket_idx);
            if bucket.is_locked() {
                core::hint::spin_loop();
                continue;
            }
            if bucket.is_empty() {
                return None;
            }
            let generation = bucket.generation();
            let found = self.scan_bucket(bucket, key, hash);
            let after = self.load_bucket(bucket_idx);
            if after.is_locked() || after.generation() != generation {
                continue;
            }
            return found;
        }
    }

    #[inline(always)]
    fn scan_bucket(&self, bucket: Bucket, key: K, hash: u64) -> Option<u64> {
        let page_id = PageId(bucket.offset() as u32);
        if page_id.is_none() {
            return None;
        }
        let log2_pages = bucket.log2_pages();
        let linear = bucket.is_linear_search();
        let page_offset = if linear {
            0u32
        } else {
            ((hash >> (self.log2_nbuckets() as u32)) as u32) & ((1u32 << log2_pages) - 1)
        };
        let first_page = PageId(page_id.0 + page_offset);
        let limit = if linear { 1u32 << log2_pages } else { 1 };
        for rel in 0..limit {
            let cur = PageId(first_page.0 + rel);
            let page = self.pages().get(cur);
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

    /// Insert only when absent. On conflict returns the existing value.
    pub fn insert_if_absent(&self, key: K, value: u64) -> Result<(), u64> {
        debug_assert_ne!(value, FREE_U64);
        let hash = key.hash();
        let bucket_idx = ((hash as u32) & (self.nbuckets() - 1)) as usize;
        let bucket = self.lock_bucket(bucket_idx);

        if !bucket.is_empty() {
            if let Some(existing) = self.scan_bucket(bucket, key, hash) {
                self.store_bucket(
                    bucket_idx,
                    Bucket::pack(
                        bucket.offset(),
                        bucket.log2_pages(),
                        bucket.refcnt(),
                        bucket.generation(),
                        bucket.is_linear_search(),
                        false,
                    ),
                );
                return Err(existing);
            }
        }

        // Reuse insert body while already holding the lock by unlocking and
        // calling insert — but that races. Inline the empty/free-slot paths.
        if bucket.is_empty() {
            let page_id = self.with_alloc_mut(|pages| pages.alloc_single(0));
            unsafe {
                self.pages_mut().get_mut(page_id).slots_mut()[0] = Kv { key, value };
            }
            self.store_bucket(
                bucket_idx,
                Bucket::pack(page_id.0 as u64, 0, 1, 0, false, false),
            );
            self.add_len(1);
            return Ok(());
        }

        let page_id = PageId(bucket.offset() as u32);
        let log2_pages = bucket.log2_pages();
        let linear = bucket.is_linear_search();
        let page_offset = if linear {
            0u32
        } else {
            ((hash >> (self.log2_nbuckets() as u32)) as u32) & ((1u32 << log2_pages) - 1)
        };
        let first_page = PageId(page_id.0 + page_offset);
        let limit = if linear { 1u32 << log2_pages } else { 1 };
        let mut free_slot: Option<(PageId, usize)> = None;
        for rel in 0..limit {
            let cur = PageId(first_page.0 + rel);
            let page = self.pages().get(cur);
            for (i, kv) in page.slots().iter().enumerate() {
                if kv_slot_is_free(kv) {
                    if free_slot.is_none() {
                        free_slot = Some((cur, i));
                    }
                }
            }
        }
        if let Some((pid, i)) = free_slot {
            unsafe {
                self.pages_mut().get_mut(pid).slots_mut()[i] = Kv { key, value };
            }
            self.store_bucket(
                bucket_idx,
                Bucket::pack(
                    page_id.0 as u64,
                    log2_pages,
                    bucket.refcnt() + 1,
                    bucket.generation().wrapping_add(1) & 0x1F,
                    linear,
                    false,
                ),
            );
            self.add_len(1);
            return Ok(());
        }

        self.split_and_rehash(bucket_idx, bucket, key, value);
        Ok(())
    }

    /// Insert or overwrite a key. Shared (`&self`) for multi-worker tables.
    pub fn insert(&self, key: K, value: u64) {
        debug_assert_ne!(value, FREE_U64);
        let hash = key.hash();
        let bucket_idx = ((hash as u32) & (self.nbuckets() - 1)) as usize;
        let bucket = self.lock_bucket(bucket_idx);

        if bucket.is_empty() {
            let page_id = self.with_alloc_mut(|pages| pages.alloc_single(0));
            unsafe {
                self.pages_mut().get_mut(page_id).slots_mut()[0] = Kv { key, value };
            }
            self.store_bucket(
                bucket_idx,
                Bucket::pack(page_id.0 as u64, 0, 1, 0, false, false),
            );
            self.add_len(1);
            return;
        }

        let page_id = PageId(bucket.offset() as u32);
        let log2_pages = bucket.log2_pages();
        let linear = bucket.is_linear_search();
        let page_offset = if linear {
            0u32
        } else {
            ((hash >> (self.log2_nbuckets() as u32)) as u32) & ((1u32 << log2_pages) - 1)
        };
        let first_page = PageId(page_id.0 + page_offset);
        let limit = if linear { 1u32 << log2_pages } else { 1 };

        let mut free_slot: Option<(PageId, usize)> = None;
        for rel in 0..limit {
            let cur = PageId(first_page.0 + rel);
            let page = self.pages().get(cur);
            for (i, kv) in page.slots().iter().enumerate() {
                if kv_slot_is_free(kv) {
                    if free_slot.is_none() {
                        free_slot = Some((cur, i));
                    }
                    continue;
                }
                if kv.key == key {
                    unsafe {
                        self.pages_mut().get_mut(cur).slots_mut()[i].value = value;
                    }
                    let unlocked = Bucket::pack(
                        page_id.0 as u64,
                        log2_pages,
                        bucket.refcnt(),
                        bucket.generation().wrapping_add(1) & 0x1F,
                        bucket.is_linear_search(),
                        false,
                    );
                    self.store_bucket(bucket_idx, unlocked);
                    return;
                }
            }
        }
        if let Some((pid, i)) = free_slot {
            unsafe {
                self.pages_mut().get_mut(pid).slots_mut()[i] = Kv { key, value };
            }
            let unlocked = Bucket::pack(
                page_id.0 as u64,
                log2_pages,
                bucket.refcnt() + 1,
                bucket.generation().wrapping_add(1) & 0x1F,
                bucket.is_linear_search(),
                false,
            );
            self.store_bucket(bucket_idx, unlocked);
            self.add_len(1);
            return;
        }

        self.split_and_rehash(bucket_idx, bucket, key, value);
    }

    /// Remove a key from the table. Returns `true` if the key was found.
    pub fn remove(&self, key: &K) -> bool {
        let hash = key.hash();
        let bucket_idx = ((hash as u32) & (self.nbuckets() - 1)) as usize;
        let bucket = self.lock_bucket(bucket_idx);
        if bucket.is_empty() {
            self.store_bucket(bucket_idx, Bucket::empty());
            return false;
        }
        let page_id = PageId(bucket.offset() as u32);
        let log2_pages = bucket.log2_pages();
        let linear = bucket.is_linear_search();
        let page_offset = if linear {
            0u32
        } else {
            ((hash >> (self.log2_nbuckets() as u32)) as u32) & ((1u32 << log2_pages) - 1)
        };
        let limit = if linear { 1u32 << log2_pages } else { 1 };

        for rel in 0..limit {
            let cur = PageId(page_id.0 + page_offset + rel);
            let page = unsafe { self.pages_mut().get_mut(cur) };
            for slot in page.slots_mut() {
                if slot.is_free() {
                    continue;
                }
                if slot.key == *key {
                    slot.mark_free();
                    let unlocked = if !linear {
                        Bucket::pack(
                            page_id.0 as u64,
                            log2_pages,
                            bucket.refcnt() - 1,
                            bucket.generation().wrapping_add(1) & 0x1F,
                            false,
                            false,
                        )
                    } else {
                        Bucket::pack(
                            page_id.0 as u64,
                            log2_pages,
                            bucket.refcnt(),
                            bucket.generation().wrapping_add(1) & 0x1F,
                            true,
                            false,
                        )
                    };
                    self.store_bucket(bucket_idx, unlocked);
                    self.add_len(-1);
                    return true;
                }
            }
        }
        self.store_bucket(
            bucket_idx,
            Bucket::pack(
                page_id.0 as u64,
                log2_pages,
                bucket.refcnt(),
                bucket.generation(),
                linear,
                false,
            ),
        );
        false
    }

    /// Remove all entries from the table.
    pub fn clear(&self) {
        // Lock every bucket so lookups retry, then rebuild the arena.
        for idx in 0..self.nbuckets() as usize {
            let _ = self.lock_bucket(idx);
        }
        self.with_alloc_mut(|pages| {
            let heap = pages.heap();
            *pages = PageAlloc::new_in(heap);
        });
        for idx in 0..self.nbuckets() as usize {
            self.store_bucket(idx, Bucket::empty());
        }
        self.set_len(0);
    }
}

#[inline(always)]
pub(super) fn kv_slot_is_free<K>(kv: &Kv<K>) -> bool {
    kv.is_free()
}
