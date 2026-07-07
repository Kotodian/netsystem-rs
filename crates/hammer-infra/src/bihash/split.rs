//! Slow path: bucket split + rehash. Called when the target page for a new
//! `insert` is full.

use crate::bihash::ops::kv_slot_is_free;
use crate::bihash::{Bihash, BihashKey, Bucket, Kv, PageId};
use crate::vec::Vec;

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    /// Split this bucket's page run: double the page count, rehash all existing
    /// entries using one extra hash bit, and insert the new (key, value).
    ///
    /// `split_and_rehash` is the slow path of `insert` and is called only when
    /// the target page for the new key is already full. It snapshots the
    /// bucket's current entries, releases the old page run, allocates a new
    /// page run with one more page, rehashes every entry using one extra
    /// hash bit, and finally places the new entry.
    pub(super) fn split_and_rehash(&mut self, bucket_idx: u32, new_key: K, new_value: u64) {
        let bucket = self.buckets[bucket_idx as usize];
        let old_log2 = bucket.log2_pages();
        let new_log2 = old_log2 + 1;
        let page_id = PageId(bucket.offset() as u32);

        // Snapshot all current entries from this bucket before reallocating.
        let heap = self.heap();
        let mut working: Vec<Kv<K>> = Vec::with_capacity_in(0, heap.clone());
        let old_pages = 1u32 << old_log2;
        for rel in 0..old_pages {
            let cur = PageId(page_id.0 + rel);
            for kv in self.pages.get(cur).slots() {
                if !kv_slot_is_free(kv) {
                    working.push(*kv);
                }
            }
        }

        // Release old page run back to the allocator.
        //
        // Phase 1 limitation: `PageAlloc::free` only supports `log2_pages = 0`,
        // so we free each page individually even when the old run had more
        // than one page. For `old_log2 == 0` this is a single free; for larger
        // runs we still free every page that backs the old bucket.
        for rel in 0..old_pages {
            self.pages.free(PageId(page_id.0 + rel), 0);
        }

        // VPP overflow protection: cap `log2_pages` at the field width
        // (8 bits → 256 pages) and switch to linear search. The cap must
        // be computed BEFORE allocating pages and rehashing — the
        // placement uses `actual_log2` to pick a page offset, and the
        // bucket stores `actual_log2` so the lookup scans the same number
        // of pages.
        let cap_overflow = new_log2 > 8;
        let actual_log2 = if cap_overflow { 8u8 } else { new_log2 };

        // Phase 1 limitation: allocate `2^actual_log2` individual pages.
        //
        // VPP guarantees physically contiguous page runs, but `PageAlloc` in
        // Phase 1 only supports `log2_pages = 0`. We use `alloc_fresh` (which
        // pushes directly to `pages` and bypasses the LIFO freelist) so the
        // new IDs are consecutive — this lets the lookup continue to use
        // `first_id + rel` addressing without tracking a per-bucket page-id
        // array.
        let new_count = 1u32 << actual_log2;
        let first_id = self.pages.alloc_fresh();
        let mut page_ids: Vec<PageId> = Vec::with_capacity_in(new_count as usize, heap);
        page_ids.push(first_id);
        for _ in 1..new_count {
            page_ids.push(self.pages.alloc_fresh());
        }

        // Rehash all working entries into the new page run using one extra
        // hash bit. Track whether any entry was forced onto a non-target
        // page (i.e. the run overflowed) — if so, the bucket must be marked
        // `linear_search` so future lookups scan every page in the run.
        let mut refcnt: u16 = 0;
        let mut overflow = false;
        for kv in &working {
            let on_target =
                self.place_in_run(&page_ids, actual_log2, kv.key, kv.value, &mut refcnt);
            if !on_target {
                overflow = true;
            }
        }

        // Place the new entry that triggered the split.
        let on_target = self.place_in_run(&page_ids, actual_log2, new_key, new_value, &mut refcnt);
        if !on_target {
            overflow = true;
        }

        // Linear search if overflow happened or cap overflow.
        let linear = overflow || cap_overflow;

        self.buckets[bucket_idx as usize] = Bucket::pack(
            first_id.0 as u64,
            actual_log2,
            refcnt,
            bucket.generation().wrapping_add(1) & 0x1F,
            linear,
            false,
        );
        self.len += 1;
    }

    /// Place a single `(key, value)` into the appropriate page within a page
    /// run, scanning for a free slot.
    ///
    /// Returns `true` when the entry landed on its target page (selected by
    /// the lower `log2_pages` bits of the hash), and `false` when the target
    /// page was full and the entry was placed on a fallback page. The caller
    /// uses this signal to set the bucket's `linear_search` flag.
    fn place_in_run(
        &mut self,
        page_ids: &[PageId],
        log2_pages: u8,
        key: K,
        value: u64,
        refcnt: &mut u16,
    ) -> bool {
        let hash = key.hash();
        let page_offset =
            ((hash >> (self.log2_nbuckets as u32)) as u32) & ((1u32 << log2_pages) - 1);
        let target = page_ids[page_offset as usize];

        // Try the target page first.
        {
            let page = self.pages.get_mut(target);
            for slot in page.slots_mut() {
                if kv_slot_is_free(slot) {
                    *slot = Kv { key, value };
                    *refcnt += 1;
                    return true;
                }
            }
        }

        // Target page is full. Scan every other page in the run for a free
        // slot. Phase 1 accepts overflow within the run; the bucket's
        // `linear_search` flag is set by the caller so subsequent lookups
        // also scan the whole run.
        for &pid in page_ids {
            if pid == target {
                continue;
            }
            let page = self.pages.get_mut(pid);
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
