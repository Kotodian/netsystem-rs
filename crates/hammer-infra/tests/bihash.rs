use std::sync::Arc;

use hammer_infra::bihash::bucket::Bucket;
use hammer_infra::bihash::{Bihash, Bihash8x8, BihashKey};
use hammer_infra::heap::Heap;

#[test]
fn bihash_key_u64_hashes_deterministically() {
    let a: u64 = 0xdead_beef_cafe_f00d;
    let b: u64 = 0xdead_beef_cafe_f00e;
    assert_ne!(a.hash(), b.hash(), "distinct keys must hash differently");
    assert_eq!(a.hash(), a.hash(), "hashing must be deterministic");
}

#[test]
fn bihash_skeleton_constructs_with_zero_entries() {
    let t: Bihash<u64, 7> = Bihash::new(64);
    assert_eq!(t.len(), 0);
    assert!(t.is_empty());
    assert_eq!(t.nbuckets(), 64);
}

#[test]
fn bucket_empty_sentinel_is_all_zero() {
    let b = Bucket::empty();
    assert!(b.is_empty());
    assert_eq!(b.offset(), 0);
    assert_eq!(b.log2_pages(), 0);
    assert_eq!(b.refcnt(), 0);
    assert_eq!(b.generation(), 0);
    assert!(!b.is_locked());
    assert!(!b.is_linear_search());
}

#[test]
fn bucket_pack_round_trip() {
    let b = Bucket::pack(
        0x1234_5678, // offset — fits in 36 bits
        3,           // log2_pages
        7,           // refcnt
        17,          // generation
        false,       // linear_search
        false,       // lock
    );
    assert_eq!(b.offset(), 0x1234_5678);
    assert_eq!(b.log2_pages(), 3);
    assert_eq!(b.refcnt(), 7);
    assert_eq!(b.generation(), 17);
    assert!(!b.is_linear_search());
    assert!(!b.is_locked());
    assert!(!b.is_empty());
}

#[test]
fn bucket_size_is_exactly_eight_bytes() {
    assert_eq!(core::mem::size_of::<Bucket>(), 8);
}

#[test]
fn bucket_generation_increments_modulo_32() {
    let b = Bucket::pack(1, 2, 3, 31, false, false);
    assert_eq!(b.generation(), 31);
    let b2 = b.bump_generation();
    assert_eq!(b2.generation(), 0);
}

#[test]
fn bucket_refcnt_max_is_8191() {
    let b = Bucket::pack(0, 0, 8191, 0, false, false);
    assert_eq!(b.refcnt(), 8191);
}

#[test]
fn bihash_with_capacity_in_uses_supplied_heap_surface() {
    let heap = Arc::new(Heap::main());
    let mut table: Bihash<u64, 7> = Bihash::with_capacity_in(8, heap);

    table.insert(10, 100);
    table.insert(11, 110);

    assert_eq!(table.nbuckets(), 8);
    assert_eq!(table.lookup(&10), Some(100));
    assert_eq!(table.lookup(&11), Some(110));
}

#[test]
fn bihash_prefetch_accepts_empty_and_present_keys() {
    let mut table: Bihash<u64, 7> = Bihash::new(8);

    table.prefetch(&42);
    table.insert(42, 420);
    table.prefetch(&42);

    assert_eq!(table.lookup(&42), Some(420));
}

// ── ValuePage / Kv / FREE_U64 ──────────────────────────────────────────

use hammer_infra::bihash::alloc::PageAlloc;
use hammer_infra::bihash::value::{FREE_U64, Kv, ValuePage};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FixedHashKey {
    id: u64,
    hash: u64,
}

impl FixedHashKey {
    fn new(id: u64, hash: u64) -> Self {
        Self { id, hash }
    }
}

impl BihashKey for FixedHashKey {
    #[inline(always)]
    fn hash(self) -> u64 {
        self.hash
    }
}

#[test]
fn kv_u64_mark_free_sets_sentinel_in_value() {
    let mut kv = Kv {
        key: 1u64,
        value: 2u64,
    };
    kv.mark_free();
    assert!(kv.is_free());
    assert_eq!(kv.key, 1);
    assert_eq!(kv.value, FREE_U64);
}

#[test]
fn kv_u64_is_free_ignores_non_sentinel_value() {
    let kv = Kv {
        key: 99u64,
        value: 0xDEAD_BEEF,
    };
    assert!(!kv.is_free());
}

#[test]
fn kv_u64_new_is_free_by_default() {
    let kv = Kv::<u64>::default();
    assert!(kv.is_free());
}

#[test]
fn value_page_4_fresh_new_has_all_free_count() {
    let page = ValuePage::<u64, 4>::new();
    // new() initializes all slots with the free sentinel (FREE_U64).
    assert_eq!(page.free_count(), 4);
    assert!(page.is_all_free());
}

#[test]
fn value_page_7_capacity_matches_kvp_const() {
    let page = ValuePage::<u64, 7>::new();
    assert_eq!(page.capacity(), 7);
}

#[test]
fn page_alloc_returns_distinct_page_ids() {
    let mut a = PageAlloc::<u64, 7>::new();
    let p1 = a.alloc_single(0);
    let p2 = a.alloc_single(0);
    assert_ne!(p1, p2);
    assert_eq!(a.live_pages(), 2);
}

#[test]
fn page_alloc_free_reuses_page_id_lifo() {
    let mut a = PageAlloc::<u64, 7>::new();
    let p1 = a.alloc_single(0);
    a.free(p1, 0);
    let p2 = a.alloc_single(0);
    assert_eq!(p1, p2);
    assert_eq!(a.live_pages(), 1);
}

#[test]
fn bihash_lookup_miss_on_empty_table() {
    let t: Bihash<u64, 7> = Bihash::new(16);
    assert_eq!(t.lookup(&42), None);
}

#[test]
fn bihash_insert_then_lookup_returns_value() {
    let mut t: Bihash<u64, 7> = Bihash::new(16);
    t.insert(1, 100);
    t.insert(2, 200);
    t.insert(3, 300);
    assert_eq!(t.lookup(&1), Some(100));
    assert_eq!(t.lookup(&2), Some(200));
    assert_eq!(t.lookup(&3), Some(300));
    assert_eq!(t.lookup(&4), None);
    assert_eq!(t.len(), 3);
}

#[test]
fn bihash_insert_overwrite_replaces_value_without_growing_len() {
    let mut t: Bihash<u64, 7> = Bihash::new(8);
    t.insert(7, 1);
    t.insert(7, 2);
    assert_eq!(t.lookup(&7), Some(2));
    assert_eq!(t.len(), 1);
}

#[test]
fn bihash_insert_overwrites_key_on_linear_search_fallback_page() {
    let mut t: Bihash<FixedHashKey, 1> = Bihash::new(1);
    let first = FixedHashKey::new(1, 0);
    let fallback = FixedHashKey::new(2, 0);

    t.insert(first, 10);
    t.insert(fallback, 20);
    t.insert(fallback, 30);

    assert_eq!(t.lookup(&fallback), Some(30));
    assert_eq!(t.len(), 2);
}

#[test]
fn bihash_insert_distinct_keys_that_hash_to_same_bucket() {
    // 1000 distinct keys in an 8-bucket table — collisions guaranteed.
    // The bihash must split the overfull bucket and rehash all entries.
    // Use KVP=4 so each page holds 4 entries, forcing overflow quickly.
    let mut t: Bihash<u64, 4> = Bihash::new(8);
    for k in 0..1000u64 {
        t.insert(k, k * 3);
    }
    for k in 0..1000u64 {
        assert_eq!(t.lookup(&k), Some(k * 3), "key {k} missing after insert");
    }
    assert_eq!(t.len(), 1000);
}

#[test]
fn bihash_split_preserves_lookup_for_many_keys() {
    let mut t: Bihash<u64, 2> = Bihash::new(8);
    for k in 0..500u64 {
        t.insert(k, k ^ 0xabcd);
    }
    for k in 0..500u64 {
        assert_eq!(t.lookup(&k), Some(k ^ 0xabcd), "key {k} missing post-split");
    }
    assert_eq!(t.len(), 500);
}

#[test]
fn bihash_split_handles_many_collisions() {
    // With KVP=2 and nbuckets=4, each bucket holds only 2 entries per page.
    // Inserting 100 keys forces multiple splits per bucket.
    let mut t: Bihash<u64, 2> = Bihash::new(4);
    for k in 0..100u64 {
        t.insert(k, k * 10);
    }
    for k in 0..100u64 {
        assert_eq!(t.lookup(&k), Some(k * 10), "key {k} missing");
    }
    assert_eq!(t.len(), 100);
}

#[test]
fn bihash_remove_existing_key() {
    let mut t: Bihash<u64, 7> = Bihash::new(16);
    t.insert(1, 100);
    t.insert(2, 200);
    assert!(t.remove(&1));
    assert_eq!(t.lookup(&1), None);
    assert_eq!(t.lookup(&2), Some(200));
    assert_eq!(t.len(), 1);
}

#[test]
fn bihash_remove_missing_key_returns_false() {
    let mut t: Bihash<u64, 7> = Bihash::new(16);
    t.insert(1, 100);
    assert!(!t.remove(&999));
    assert_eq!(t.len(), 1);
}

#[test]
fn bihash_remove_all_entries_returns_bucket_to_empty() {
    let mut t: Bihash<u64, 7> = Bihash::new(16);
    t.insert(1, 100);
    t.insert(2, 200);
    t.remove(&1);
    t.remove(&2);
    assert!(t.is_empty());
    assert_eq!(t.lookup(&1), None);
    assert_eq!(t.lookup(&2), None);
}

#[test]
fn bihash_clear_drops_len_to_zero() {
    let mut t: Bihash<u64, 4> = Bihash::new(4);
    for k in 0..100u64 {
        t.insert(k, k);
    }
    t.clear();
    assert!(t.is_empty());
    for k in 0..100u64 {
        assert_eq!(t.lookup(&k), None);
    }
    t.insert(7, 77);
    assert_eq!(t.lookup(&7), Some(77));
}

#[test]
fn bihash_8x8_alias_compiles() {
    let t: Bihash8x8 = Bihash::new(16);
    assert_eq!(t.nbuckets(), 16);
}

#[test]
fn bihash_iter_empty_table_yields_nothing() {
    let t: Bihash<u64, 7> = Bihash::new(16);
    assert_eq!(t.iter().count(), 0);
}

#[test]
fn bihash_iter_after_inserts_yields_correct_count() {
    let mut t: Bihash<u64, 7> = Bihash::new(16);
    t.insert(1, 10);
    t.insert(2, 20);
    t.insert(3, 30);
    let pairs: Vec<(u64, u64)> = t.iter().map(|(&k, &v)| (k, v)).collect();
    assert_eq!(pairs.len(), 3);
    assert!(pairs.contains(&(1, 10)));
    assert!(pairs.contains(&(2, 20)));
    assert!(pairs.contains(&(3, 30)));
}
