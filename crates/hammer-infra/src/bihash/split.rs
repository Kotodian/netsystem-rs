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
        let new_log2 = requested_log2.min(MAX_LOG2_PAGES);
        let cap_overflow = requested_log2 > MAX_LOG2_PAGES;
        let (offset, (refcnt, displaced)) = self.pages.replace(
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
                        displaced |=
                            !place(new_pages, new_log2, kv.key, kv.value, self.log2_nbuckets());
                        refcnt += 1;
                    }
                }
                displaced |= !place(
                    new_pages,
                    new_log2,
                    new_key,
                    new_value,
                    self.log2_nbuckets(),
                );
                refcnt += 1;
                (refcnt, displaced)
            },
        );

        self.store_bucket(
            bucket_idx,
            Bucket::pack(
                offset,
                new_log2,
                refcnt,
                bucket.generation().wrapping_add(1) & 0x1F,
                displaced || cap_overflow,
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
