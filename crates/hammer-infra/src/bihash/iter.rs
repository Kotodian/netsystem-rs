//! Weakly consistent iterator for `Bihash`.

use crate::bihash::ops::kv_slot_is_free;
use crate::bihash::{Bihash, BihashKey};

/// Traverses copied key/value pairs without extending a page borrow beyond
/// allocator protection.
pub struct BihashIter<'a, K: BihashKey, const KVP: usize> {
    bihash: &'a Bihash<K, KVP>,
    bucket_idx: usize,
    page_rel: usize,
    slot_idx: usize,
}

impl<'a, K: BihashKey + Default, const KVP: usize> BihashIter<'a, K, KVP> {
    pub(crate) fn new(bihash: &'a Bihash<K, KVP>) -> Self {
        Self {
            bihash,
            bucket_idx: 0,
            page_rel: 0,
            slot_idx: 0,
        }
    }

    fn advance_bucket(&mut self) {
        self.bucket_idx += 1;
        self.page_rel = 0;
        self.slot_idx = 0;
    }
}

impl<K: BihashKey + Default, const KVP: usize> Iterator for BihashIter<'_, K, KVP> {
    type Item = (K, u64);

    fn next(&mut self) -> Option<Self::Item> {
        'bucket: while self.bucket_idx < self.bihash.nbuckets() as usize {
            let bucket = self.bihash.load_bucket(self.bucket_idx);
            if bucket.is_locked() {
                core::hint::spin_loop();
                continue;
            }
            if bucket.is_empty() {
                self.advance_bucket();
                continue;
            }

            let page_count = 1usize << bucket.log2_pages();
            while self.page_rel < page_count {
                while self.slot_idx < KVP {
                    let page_rel = self.page_rel;
                    let slot_idx = self.slot_idx;
                    self.slot_idx += 1;
                    let item = self.bihash.pages.read(
                        bucket.offset(),
                        bucket.log2_pages(),
                        || self.bihash.load_bucket(self.bucket_idx) == bucket,
                        |pages| {
                            let kv = pages[page_rel].slots()[slot_idx];
                            (!kv_slot_is_free(&kv)).then_some((kv.key, kv.value))
                        },
                    );
                    match item {
                        Some(Some(item)) => return Some(item),
                        Some(None) => {}
                        None => {
                            self.page_rel = 0;
                            self.slot_idx = 0;
                            continue 'bucket;
                        }
                    }
                }
                self.page_rel += 1;
                self.slot_idx = 0;
            }
            self.advance_bucket();
        }
        None
    }
}
