//! Slow path: allocate a larger value array, rehash, and publish its offset.

use crate::bihash::ops::kv_slot_is_free;
use crate::bihash::{Bihash, BihashKey, Bucket, Kv, ValuePage};

const MAX_LOG2_PAGES: u8 = 8;

impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    /// Split this bucket's page array. `bucket` is the locked snapshot.
    pub(super) fn split_and_rehash(
        &self,
        bucket_idx: usize,
        bucket: Bucket,
        new_key: K,
        new_value: u64,
    ) {
        let requested_log2 = bucket.log2_pages().saturating_add(1);
        let first_log2 = requested_log2.min(MAX_LOG2_PAGES);
        let cap_overflow = requested_log2 > MAX_LOG2_PAGES;
        let rehash = |new_log2, linear_mode| {
            self.pages.replace(
                bucket.offset(),
                bucket.log2_pages(),
                new_log2,
                |old_pages, new_pages| {
                    let mut refcnt = 0;
                    let mut displaced = false;
                    for page in old_pages {
                        for kv in page.slots() {
                            if kv_slot_is_free(kv) {
                                continue;
                            }
                            displaced |= if linear_mode {
                                !place_linear(new_pages, kv.key, kv.value)
                            } else {
                                !place(new_pages, new_log2, kv.key, kv.value, self.log2_nbuckets())
                            };
                            refcnt += 1;
                        }
                    }
                    displaced |= if linear_mode {
                        !place_linear(new_pages, new_key, new_value)
                    } else {
                        !place(
                            new_pages,
                            new_log2,
                            new_key,
                            new_value,
                            self.log2_nbuckets(),
                        )
                    };
                    refcnt += 1;
                    (refcnt, displaced)
                },
            )
        };

        let (offset, (refcnt, _), new_log2, linear) = if !cap_overflow {
            let (offset, (refcnt, displaced)) = rehash(first_log2, false);
            if !displaced {
                (offset, (refcnt, false), first_log2, false)
            } else {
                self.pages.retire(offset, first_log2);
                let retry_log2 = (first_log2 + 1).min(MAX_LOG2_PAGES);
                let (retry_offset, (retry_refcnt, retry_displaced)) = rehash(retry_log2, false);
                if !retry_displaced {
                    (retry_offset, (retry_refcnt, false), retry_log2, false)
                } else {
                    self.pages.retire(retry_offset, retry_log2);
                    let (linear_offset, (linear_refcnt, _)) = rehash(first_log2, true);
                    (linear_offset, (linear_refcnt, true), first_log2, true)
                }
            }
        } else {
            let (offset, (refcnt, _)) = rehash(first_log2, true);
            (offset, (refcnt, true), first_log2, true)
        };

        self.store_bucket(
            bucket_idx,
            Bucket::pack(
                offset,
                new_log2,
                refcnt,
                bucket.generation().wrapping_add(1) & 0x1F,
                linear || cap_overflow,
                false,
            ),
        );
        self.pages.retire(bucket.offset(), bucket.log2_pages());
        self.add_len(1);
    }
}

fn place<K: BihashKey + Default, const KVP: usize>(
    pages: &mut [ValuePage<K, KVP>],
    log2_pages: u8,
    key: K,
    value: u64,
    log2_nbuckets: u8,
) -> bool {
    let hash = key.hash();
    let target = ((hash >> u32::from(log2_nbuckets)) as usize) & ((1usize << log2_pages) - 1);

    for slot in pages[target].slots_mut() {
        if kv_slot_is_free(slot) {
            *slot = Kv { key, value };
            return true;
        }
    }

    for (page_index, page) in pages.iter_mut().enumerate() {
        if page_index == target {
            continue;
        }
        for slot in page.slots_mut() {
            if kv_slot_is_free(slot) {
                *slot = Kv { key, value };
                return false;
            }
        }
    }

    panic!("bihash value array is full");
}

fn place_linear<K: BihashKey + Default, const KVP: usize>(
    pages: &mut [ValuePage<K, KVP>],
    key: K,
    value: u64,
) -> bool {
    for page in pages {
        for slot in page.slots_mut() {
            if kv_slot_is_free(slot) {
                *slot = Kv { key, value };
                return true;
            }
        }
    }
    false
}
