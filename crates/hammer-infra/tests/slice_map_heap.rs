#![allow(deprecated)]

use std::sync::Arc;

use hammer_infra::boxed::Slice;
use hammer_infra::heap::Heap;
use hammer_infra::map::FlatHashTable;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn slice_and_flat_hash_table_use_the_passed_heap() {
    let region = SvmRegion::with_size(128 * 1024);
    let heap = Arc::new(Heap::svm(region.clone(), 0));
    let start = region.base() as usize;
    let end = start + region.size();

    let slice: Slice<u8> = Slice::from_elem_in(2048, 0u8, heap.clone());
    let mut table: FlatHashTable<u64, u64> = FlatHashTable::with_capacity_in(4, heap);
    for entry in 0..16u64 {
        table.insert(entry, entry * 10);
    }

    assert_eq!(slice.len(), 2048);
    assert_eq!(table.get(&1), Some(&10));
    assert_eq!(table.get(&15), Some(&150));
    assert!(
        table.bucket_count() >= 32,
        "table should grow in-place on the same heap"
    );

    let slice_ptr = slice.as_ptr() as usize;
    assert!(
        slice_ptr >= start && slice_ptr < end,
        "slice backing must live inside the SVM region"
    );

    let bucket_ptr = table.bucket_ptr() as usize;
    assert!(
        bucket_ptr >= start && bucket_ptr < end,
        "table buckets must live inside the SVM region"
    );
}

#[test]
fn cloned_slice_stays_in_the_same_svm_region() {
    let region = SvmRegion::with_size(128 * 1024);
    let heap = Arc::new(Heap::svm(region.clone(), 0));
    let start = region.base() as usize;
    let end = start + region.size();

    let slice: Slice<u64> = Slice::from_elem_in(256, 9u64, heap);
    let cloned = slice.clone();

    assert_eq!(cloned.len(), slice.len());
    assert!(cloned.iter().all(|value| *value == 9));
    assert_ne!(cloned.as_ptr(), slice.as_ptr());

    let cloned_ptr = cloned.as_ptr() as usize;
    assert!(
        cloned_ptr >= start && cloned_ptr < end,
        "cloned slice backing must remain inside the SVM region"
    );
}

#[test]
fn cloned_flat_hash_table_buckets_stay_in_the_same_svm_region() {
    let region = SvmRegion::with_size(128 * 1024);
    let heap = Arc::new(Heap::svm(region.clone(), 0));
    let start = region.base() as usize;
    let end = start + region.size();

    let mut table: FlatHashTable<u64, u64> = FlatHashTable::with_capacity_in(4, heap);
    for entry in 0..16u64 {
        table.insert(entry, entry * 10);
    }

    let cloned = table.clone();

    assert_eq!(cloned.len(), table.len());
    assert_eq!(cloned.bucket_count(), table.bucket_count());
    assert_eq!(cloned.get(&1), Some(&10));
    assert_eq!(cloned.get(&15), Some(&150));
    assert_ne!(cloned.bucket_ptr(), table.bucket_ptr());

    let cloned_ptr = cloned.bucket_ptr() as usize;
    assert!(
        cloned_ptr >= start && cloned_ptr < end,
        "cloned table buckets must remain inside the SVM region"
    );
}

#[test]
fn slice_and_flat_hash_table_default_constructors_still_work() {
    let slice: Slice<u64> = Slice::from_elem(64, 7);
    let mut table: FlatHashTable<u64, u64> = FlatHashTable::with_capacity(8);
    table.insert(3, 9);

    assert_eq!(slice.len(), 64);
    assert_eq!(slice[0], 7);
    assert_eq!(table.get(&3), Some(&9));
}

#[test]
fn infra_container_sources_do_not_reintroduce_heap_traits() {
    for src in [
        include_str!("../src/pool.rs"),
        include_str!("../src/boxed.rs"),
        include_str!("../src/map.rs"),
    ] {
        assert!(!src.contains("H: Heap"));
        assert!(!src.contains("pub trait Heap"));
        assert!(src.contains("Arc<Heap>") || src.contains("&Heap"));
    }
}
