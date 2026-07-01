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
