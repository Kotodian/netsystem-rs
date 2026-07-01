use hammer_infra::bihash::bucket::Bucket;
use hammer_infra::bihash::{Bihash, BihashKey};

#[test]
fn bihash_key_u64_hashes_deterministically() {
    let a: u64 = 0xdead_beef_cafe_f00d;
    let b: u64 = 0xdead_beef_cafe_f00e;
    assert_ne!(a.hash(), b.hash(), "distinct keys must hash differently");
    assert_eq!(a.hash(), a.hash(), "hashing must be deterministic");
}

#[test]
fn bihash_key_u64_eq_symmetric() {
    let a: u64 = 42;
    let b: u64 = 42;
    let c: u64 = 43;
    assert!(a.key_eq(b));
    assert!(a.key_eq(a));
    assert!(!a.key_eq(c));
}

#[test]
fn bihash_skeleton_constructs_with_zero_entries() {
    let t: Bihash<u64, u64, 7> = Bihash::new(64);
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

// ── ValuePage / Kv / FREE_U64 ──────────────────────────────────────────

use hammer_infra::bihash::value::{Kv, ValuePage, FREE_U64};
use hammer_infra::bihash::alloc::PageAlloc;

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
    let kv = Kv::<u64, u64>::empty();
    assert!(kv.is_free());
}

#[test]
fn value_page_4_fresh_new_has_zero_free_count_until_marked() {
    let mut page = ValuePage::<u64, u64, 4>::new();
    // Generic new() uses V::default() (0 for u64), not FREE_U64.
    assert_eq!(page.free_count(), 0);
    // After marking all slots free, the sentinel is present.
    page.slots_mut().iter_mut().for_each(|kv| kv.mark_free());
    assert_eq!(page.free_count(), 4);
    assert!(page.is_all_free());
}

#[test]
fn value_page_7_capacity_matches_kvp_const() {
    let page = ValuePage::<u64, u64, 7>::new();
    assert_eq!(page.capacity(), 7);
}

#[test]
fn page_alloc_returns_distinct_page_ids() {
    let mut a = PageAlloc::<u64, u64, 7>::new();
    let p1 = a.alloc_single(0);
    let p2 = a.alloc_single(0);
    assert_ne!(p1, p2);
    assert_eq!(a.live_pages(), 2);
}

#[test]
fn page_alloc_free_reuses_page_id_lifo() {
    let mut a = PageAlloc::<u64, u64, 7>::new();
    let p1 = a.alloc_single(0);
    a.free(p1, 0);
    let p2 = a.alloc_single(0);
    assert_eq!(p1, p2);
    assert_eq!(a.live_pages(), 1);
}

#[test]
fn bihash_lookup_miss_on_empty_table() {
    let t: Bihash<u64, u64, 7> = Bihash::new(16);
    assert_eq!(t.lookup(&42), None);
}
