//! Hot-path operations: lookup, insert, remove, clear.
//!
//! Lookup scans published pages. Writers atomically own VPP's `bucket.lock`
//! writer bit and modify the live value page in place; readers wait on the
//! lock bit. Split/rehash allocates through `PageAlloc` under alloc_lock.

use crate::bihash::{AtomicBucket, Bihash, BihashKey, Bucket, FREE_U64, Kv, ValuePage};
use crate::prefetch::prefetch_read_l1;
use std::sync::atomic::{fence, Ordering};

type Slot = (usize, usize);

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

    /// Lock-free lookup with retry while the target bucket changes.
    #[inline(always)]
    pub fn lookup(&self, key: &K) -> Option<u64> {
        self.lookup_with_hash(*key, key.hash())
    }

    #[inline(always)]
    pub fn lookup_with_hash(&self, key: K, hash: u64) -> Option<u64> {
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
            let found = self.pages.read(
                bucket.offset(),
                bucket.log2_pages(),
                || self.load_bucket(bucket_idx) == bucket,
                |pages| {
                    scan_pages(
                        pages,
                        bucket.log2_pages(),
                        bucket.is_linear_search(),
                        key,
                        hash,
                        self.log2_nbuckets(),
                    )
                    .0
                    .map(|(_, value)| value)
                },
            );
            if let Some(found) = found {
                if self.load_bucket(bucket_idx) == bucket {
                    return found;
                }
            }
        }
    }

    /// Insert only when absent. On conflict returns the existing value.
    pub fn insert_if_absent(&self, key: K, value: u64) -> Result<(), u64> {
        debug_assert_ne!(value, FREE_U64);
        let hash = key.hash();
        let bucket_idx = ((hash as u32) & (self.nbuckets() - 1)) as usize;
        let bucket = self.lock_bucket(bucket_idx);

        if bucket.is_empty() {
            let (offset, _) = self.pages.allocate(0, |pages| {
                pages[0].slots_mut()[0] = Kv { key, value };
            });
            self.store_bucket(bucket_idx, Bucket::pack(offset, 0, 1, 0, false, false));
            self.add_len(1);
            return Ok(());
        }

        let (found, free) = self.inspect_locked(bucket, key, hash);
        if let Some((_, existing)) = found {
            self.unlock_unchanged(bucket_idx, bucket);
            return Err(existing);
        }
        if let Some(slot) = free {
            let pages = self
                .pages
                .live_pages_mut(bucket.offset(), bucket.log2_pages())
                .expect("locked bucket retains its live value pages");
            write_live_slot(&mut pages[slot.0].slots_mut()[slot.1], key, value);
            self.publish_unlocked(bucket_idx, bucket, bucket.refcnt() + 1);
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
            let (offset, _) = self.pages.allocate(0, |pages| {
                pages[0].slots_mut()[0] = Kv { key, value };
            });
            self.store_bucket(bucket_idx, Bucket::pack(offset, 0, 1, 0, false, false));
            self.add_len(1);
            return;
        }

        let (found, free) = self.inspect_locked(bucket, key, hash);
        if let Some((slot, _)) = found {
            let pages = self
                .pages
                .live_pages_mut(bucket.offset(), bucket.log2_pages())
                .expect("locked bucket retains its live value pages");
            pages[slot.0].slots_mut()[slot.1].value = value;
            self.unlock_unchanged(bucket_idx, bucket);
            return;
        }
        if let Some(slot) = free {
            let pages = self
                .pages
                .live_pages_mut(bucket.offset(), bucket.log2_pages())
                .expect("locked bucket retains its live value pages");
            write_live_slot(&mut pages[slot.0].slots_mut()[slot.1], key, value);
            self.publish_unlocked(bucket_idx, bucket, bucket.refcnt() + 1);
            self.add_len(1);
            return;
        }

        self.split_and_rehash(bucket_idx, bucket, key, value);
    }

    /// Replace a key only when its current value is exactly `old_value`.
    ///
    /// This is the compare-current-value publication operation used by an
    /// owner that must not overwrite a newer generation or another owner.
    pub fn replace_if_current(&self, key: &K, old_value: u64, new_value: u64) -> bool {
        debug_assert_ne!(new_value, FREE_U64);
        let hash = key.hash();
        let bucket_idx = ((hash as u32) & (self.nbuckets() - 1)) as usize;
        let bucket = self.lock_bucket(bucket_idx);
        if bucket.is_empty() {
            self.unlock_unchanged(bucket_idx, bucket);
            return false;
        }

        let (found, _) = self.inspect_locked(bucket, *key, hash);
        let Some((slot, current)) = found else {
            self.unlock_unchanged(bucket_idx, bucket);
            return false;
        };
        if current != old_value {
            self.unlock_unchanged(bucket_idx, bucket);
            return false;
        }

        let pages = self
            .pages
            .live_pages_mut(bucket.offset(), bucket.log2_pages())
            .expect("locked bucket retains its live value pages");
        pages[slot.0].slots_mut()[slot.1].value = new_value;
        self.unlock_unchanged(bucket_idx, bucket);
        true
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

        let (found, _) = self.inspect_locked(bucket, *key, hash);
        let Some((slot, _)) = found else {
            self.unlock_unchanged(bucket_idx, bucket);
            return false;
        };

        let pages = self
            .pages
            .live_pages_mut(bucket.offset(), bucket.log2_pages())
            .expect("locked bucket retains its live value pages");
        pages[slot.0].slots_mut()[slot.1].mark_free();
        if bucket.refcnt() == 1 {
            self.store_bucket(bucket_idx, Bucket::empty());
            self.pages.retire(bucket.offset(), bucket.log2_pages());
        } else {
            self.publish_unlocked(bucket_idx, bucket, bucket.refcnt() - 1);
        }
        self.add_len(-1);
        true
    }

    /// Remove a key only when its current value is exactly `value`.
    ///
    /// Stale cleanup uses this so an old owner cannot delete a route already
    /// replaced by a newer Session handle or generation.
    pub fn remove_if_current(&self, key: &K, value: u64) -> bool {
        let hash = key.hash();
        let bucket_idx = ((hash as u32) & (self.nbuckets() - 1)) as usize;
        let bucket = self.lock_bucket(bucket_idx);
        if bucket.is_empty() {
            self.unlock_unchanged(bucket_idx, bucket);
            return false;
        }

        let (found, _) = self.inspect_locked(bucket, *key, hash);
        let Some((slot, current)) = found else {
            self.unlock_unchanged(bucket_idx, bucket);
            return false;
        };
        if current != value {
            self.unlock_unchanged(bucket_idx, bucket);
            return false;
        }

        let pages = self
            .pages
            .live_pages_mut(bucket.offset(), bucket.log2_pages())
            .expect("locked bucket retains its live value pages");
        pages[slot.0].slots_mut()[slot.1].mark_free();
        if bucket.refcnt() == 1 {
            self.store_bucket(bucket_idx, Bucket::empty());
            self.pages.retire(bucket.offset(), bucket.log2_pages());
        } else {
            self.publish_unlocked(bucket_idx, bucket, bucket.refcnt() - 1);
        }
        self.add_len(-1);
        true
    }

    /// Remove all entries from the table.
    pub fn clear(&self) {
        let mut retired = std::vec::Vec::new();
        for idx in 0..self.nbuckets() as usize {
            let bucket = self.lock_bucket(idx);
            if !bucket.is_empty() {
                retired.push((bucket.offset(), bucket.log2_pages()));
            }
        }
        for idx in 0..self.nbuckets() as usize {
            self.store_bucket(idx, Bucket::empty());
        }
        for (offset, log2_pages) in retired {
            self.pages.retire(offset, log2_pages);
        }
        self.set_len(0);
    }

    fn inspect_locked(
        &self,
        bucket: Bucket,
        key: K,
        hash: u64,
    ) -> (Option<(Slot, u64)>, Option<Slot>) {
        let pages = self
            .pages
            .live_pages_mut(bucket.offset(), bucket.log2_pages())
            .expect("locked bucket retains its live value pages");
        scan_pages(
            &*pages,
            bucket.log2_pages(),
            bucket.is_linear_search(),
            key,
            hash,
            self.log2_nbuckets(),
        )
    }

    fn publish_unlocked(&self, bucket_idx: usize, bucket: Bucket, refcnt: u16) {
        self.store_bucket(
            bucket_idx,
            Bucket::pack(
                bucket.offset(),
                bucket.log2_pages(),
                refcnt,
                bucket.generation(),
                bucket.is_linear_search(),
                false,
            ),
        );
    }

    #[inline]
    fn unlock_unchanged(&self, bucket_idx: usize, bucket: Bucket) {
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
    }
}

#[inline(always)]
fn write_live_slot<K: Copy>(slot: &mut Kv<K>, key: K, value: u64) {
    slot.value = value;
    fence(Ordering::Release);
    slot.key = key;
}

#[inline(always)]
fn scan_pages<K: BihashKey + Default, const KVP: usize>(
    pages: &[ValuePage<K, KVP>],
    log2_pages: u8,
    linear: bool,
    key: K,
    hash: u64,
    log2_nbuckets: u8,
) -> (Option<(Slot, u64)>, Option<Slot>) {
    let first = if linear {
        0
    } else {
        ((hash >> u32::from(log2_nbuckets)) as usize) & ((1usize << log2_pages) - 1)
    };
    let limit = if linear { 1usize << log2_pages } else { 1 };
    let mut free = None;
    for (page_index, page) in pages.iter().enumerate().skip(first).take(limit) {
        for (slot_index, kv) in page.slots().iter().enumerate() {
            if kv_slot_is_free(kv) {
                free.get_or_insert((page_index, slot_index));
                continue;
            }
            if kv.key == key {
                return (Some(((page_index, slot_index), kv.value)), free);
            }
        }
    }
    (None, free)
}

#[inline(always)]
pub(super) fn kv_slot_is_free<K>(kv: &Kv<K>) -> bool {
    kv.is_free()
}
