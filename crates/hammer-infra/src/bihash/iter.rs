//! Snapshot-style iterator for `Bihash`.

use crate::bihash::ops::kv_slot_is_free;
use crate::bihash::value::BihashFree;
use crate::bihash::{Bihash, BihashKey, PageId};

/// A snapshot-style iterator that yields `(&K, &V)` pairs.
///
/// Created by `Bihash::iter()`. The iterator traverses buckets and their
/// page runs in index order. It returns references to live entries.
pub struct BihashIter<'a, K: BihashKey, V: Copy + Eq + BihashFree, const KVP: usize> {
    bihash: &'a Bihash<K, V, KVP>,
    bucket_idx: usize,
    page_rel: u32,
    slot_idx: usize,
}

impl<'a, K: BihashKey + Default, V: Copy + Eq + Default + BihashFree, const KVP: usize>
    BihashIter<'a, K, V, KVP>
{
    pub(crate) fn new(bihash: &'a Bihash<K, V, KVP>) -> Self {
        Self {
            bihash,
            bucket_idx: 0,
            page_rel: 0,
            slot_idx: 0,
        }
    }
}

impl<'a, K: BihashKey + Default, V: Copy + Eq + Default + BihashFree, const KVP: usize> Iterator
    for BihashIter<'a, K, V, KVP>
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.bucket_idx < self.bihash.nbuckets() as usize {
            let bucket = self.bihash.buckets()[self.bucket_idx];
            if bucket.is_empty() {
                self.bucket_idx += 1;
                self.page_rel = 0;
                self.slot_idx = 0;
                continue;
            }
            let page_id = PageId(bucket.offset() as u32);
            let log2_pages = bucket.log2_pages();
            let num_pages = 1u32 << log2_pages;

            while self.page_rel < num_pages {
                let cur = PageId(page_id.0 + self.page_rel);
                let page = self.bihash.pages().get(cur);
                let slots = page.slots();
                while self.slot_idx < slots.len() {
                    let kv = &slots[self.slot_idx];
                    self.slot_idx += 1;
                    if !kv_slot_is_free(kv) {
                        return Some((&kv.key, &kv.value));
                    }
                }
                self.slot_idx = 0;
                self.page_rel += 1;
            }
            self.page_rel = 0;
            self.slot_idx = 0;
            self.bucket_idx += 1;
        }
        None
    }
}
