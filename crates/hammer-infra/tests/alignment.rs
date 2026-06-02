use hammer_infra::align::{CACHE_LINE, CacheLine, is_aligned};
use hammer_infra::{boxed, map, pool, vec};

#[test]
fn boxed_slice_allocates_cache_line_aligned_storage() {
    let slice = boxed::Slice::<u8>::from_elem(1500, 0xaa);

    assert_eq!(slice.len(), 1500);
    assert!(is_aligned(slice.as_ptr(), CACHE_LINE));
    assert_eq!(slice.as_slice()[0], 0xaa);
    assert_eq!(slice.as_slice()[1499], 0xaa);
}

#[test]
fn boxed_slice_drops_elements_once() {
    use std::rc::Rc;

    #[derive(Clone)]
    struct Counted(#[allow(dead_code)] Rc<()>);

    let marker = Rc::new(());
    let slice = boxed::Slice::<Counted>::from_elem(4, Counted(Rc::clone(&marker)));

    assert_eq!(Rc::strong_count(&marker), 5);
    drop(slice);
    assert_eq!(Rc::strong_count(&marker), 1);
}

#[test]
fn vec_preserves_cache_line_alignment_after_growth_and_clone() {
    let mut values = vec::Vec::<u64>::with_capacity(1);
    assert!(is_aligned(values.as_ptr(), CACHE_LINE));

    for value in 0..256 {
        values.push(value);
    }

    assert_eq!(values.len(), 256);
    assert_eq!(values.as_slice()[128], 128);
    assert!(is_aligned(values.as_ptr(), CACHE_LINE));

    let cloned = values.clone();
    assert_eq!(cloned.as_slice(), values.as_slice());
    assert!(is_aligned(cloned.as_ptr(), CACHE_LINE));
}

#[test]
fn cache_line_wrapper_aligns_each_vec_element() {
    let mut values = vec::Vec::<CacheLine<u64>>::new();
    for value in 0..8 {
        values.push(CacheLine::new(value));
    }

    for item in values.as_slice() {
        assert!(is_aligned(item as *const CacheLine<u64>, CACHE_LINE));
    }
}

#[test]
fn pool_aligns_each_slot_and_rejects_stale_indices() {
    let mut pool = pool::Pool::<u64>::with_capacity(2);
    let first = pool.insert(10).expect("first slot");
    let second = pool.insert(20).expect("second slot");

    assert!(is_aligned(
        pool.slot_ptr(first).expect("first ptr"),
        CACHE_LINE
    ));
    assert!(is_aligned(
        pool.slot_ptr(second).expect("second ptr"),
        CACHE_LINE
    ));
    assert_eq!(pool.get(first), Some(&10));
    assert_eq!(pool.get(second), Some(&20));

    assert_eq!(pool.remove(first), Some(10));
    assert_eq!(pool.get(first), None);

    let reused = pool.insert(30).expect("reused slot");
    assert_eq!(pool.get(reused), Some(&30));
    assert_ne!(first.generation(), reused.generation());
    assert_eq!(pool.get(first), None);
}

#[test]
fn flat_hash_map_keeps_bucket_storage_aligned_after_rehash() {
    let mut table = map::FlatHashTable::<u64, u32>::with_capacity(1);
    assert!(is_aligned(table.bucket_ptr(), CACHE_LINE));

    for key in 0..128 {
        table.insert(key, key as u32 + 10);
    }

    assert_eq!(table.len(), 128);
    assert!(table.bucket_count() >= 256);
    assert!(is_aligned(table.bucket_ptr(), CACHE_LINE));
    assert_eq!(table.get(&64).copied(), Some(74));
    assert_eq!(table.lookup(&64), Some(74));

    table.insert(64, 999);
    assert_eq!(table.get(&64).copied(), Some(999));
    assert_eq!(table.len(), 128);
}
