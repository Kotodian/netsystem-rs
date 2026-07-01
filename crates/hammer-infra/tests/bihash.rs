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
