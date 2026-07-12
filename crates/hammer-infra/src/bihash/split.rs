//! Slow path: bucket split + rehash. Called when the target page for a new
//! `insert` is full. Caller already holds the bucket lock bit.

use crate::bihash::ops::kv_slot_is_free;
use crate::bihash::{Bihash, BihashKey, Bucket, Kv, PageId};
use crate::vec::Vec;

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    /// Split this bucket's page run. `bucket` is the locked snapshot.
    pub(super) fn split_and_rehash(
        &self,
        bucket_idx: usize,
        bucket: Bucket,
        new_key: K,
        new_value: u64,
    ) {
        let old_log2 = bucket.log2_pages();
        let new_log2 = old_log2 + 1;
        let page_id = PageId(bucket.offset() as u32);

        let heap = self.heap();
        let mut working: Vec<Kv<K>> = Vec::with_capacity_in(0, heap.clone());
        let old_pages = 1u32 << old_log2;
        for rel in 0..old_pages {
            let cur = PageId(page_id.0 + rel);
            for kv in self.pages().get(cur).slots() {
                if !kv_slot_is_free(kv) {
                    working.push(*kv);
                }
            }
        }

        let (first_id, page_ids, actual_log2, cap_overflow) = self.with_alloc_mut(|pages| {
            for rel in 0..old_pages {
                pages.free(PageId(page_id.0 + rel), 0);
            }

            let cap_overflow = new_log2 > 8;
            let actual_log2 = if cap_overflow { 8u8 } else { new_log2 };
            let new_count = 1u32 << actual_log2;
            let first_id = pages.alloc_fresh();
            let mut page_ids: Vec<PageId> = Vec::with_capacity_in(new_count as usize, heap.clone());
            page_ids.push(first_id);
            for _ in 1..new_count {
                page_ids.push(pages.alloc_fresh());
            }
            (first_id, page_ids, actual_log2, cap_overflow)
        });

        let mut refcnt: u16 = 0;
        let mut overflow = false;
        for kv in &working {
            let on_target =
                self.place_in_run(&page_ids, actual_log2, kv.key, kv.value, &mut refcnt);
            if !on_target {
                overflow = true;
            }
        }

        let on_target = self.place_in_run(&page_ids, actual_log2, new_key, new_value, &mut refcnt);
        if !on_target {
            overflow = true;
        }

        let linear = overflow || cap_overflow;
        self.store_bucket(
            bucket_idx,
            Bucket::pack(
                first_id.0 as u64,
                actual_log2,
                refcnt,
                bucket.generation().wrapping_add(1) & 0x1F,
                linear,
                false,
            ),
        );
        self.add_len(1);
    }

    fn place_in_run(
        &self,
        page_ids: &[PageId],
        log2_pages: u8,
        key: K,
        value: u64,
        refcnt: &mut u16,
    ) -> bool {
        let hash = key.hash();
        let page_offset =
            ((hash >> (self.log2_nbuckets() as u32)) as u32) & ((1u32 << log2_pages) - 1);
        let target = page_ids[page_offset as usize];

        {
            let page = unsafe { self.pages_mut().get_mut(target) };
            for slot in page.slots_mut() {
                if kv_slot_is_free(slot) {
                    *slot = Kv { key, value };
                    *refcnt += 1;
                    return true;
                }
            }
        }

        for &pid in page_ids {
            if pid == target {
                continue;
            }
            let page = unsafe { self.pages_mut().get_mut(pid) };
            for slot in page.slots_mut() {
                if kv_slot_is_free(slot) {
                    *slot = Kv { key, value };
                    *refcnt += 1;
                    return false;
                }
            }
        }

        panic!("bihash page run is full");
    }
}
