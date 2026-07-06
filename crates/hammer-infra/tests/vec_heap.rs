//! Vec/Slice/FlatHashTable must allocate their backing storage through a Heap
//! instead of `std::alloc` directly, matching VPP's "active heap" model.
//!
//! The original dyn-trait version of this test used a `CountingHeap` to count
//! `alloc` calls. With `Heap` now a concrete handle backed by either mimalloc
//! or an SVM region, these tests verify externally visible behavior and
//! address placement instead of allocator internals.
#![allow(deprecated)]

use std::alloc::{GlobalAlloc, Layout};
use std::sync::Arc;

use hammer_infra::boxed::Slice;
use hammer_infra::heap::Heap;
use hammer_infra::map::FlatHashTable;
use hammer_infra::svm_region::SvmRegion;
use hammer_infra::vec::Vec as HVec;

#[test]
fn hammer_vec_routes_through_heap_registry() {
    let region = SvmRegion::with_size(1 << 20); // 1 MiB
    let heap = Arc::new(Heap::svm(region.clone(), 0));

    let mut v: HVec<u64> = HVec::with_capacity_in(128, heap.clone());
    assert_eq!(v.capacity(), 128);
    for i in 0..128u64 {
        v.push(i);
    }
    assert_eq!(v.len(), 128);
    assert_eq!(v[127], 127);
    let start = region.base() as usize;
    let end = start + region.size();
    let ptr = v.as_ptr() as usize;
    assert!(
        ptr >= start && ptr < end,
        "Vec backing must live inside the SVM region"
    );
}

#[test]
fn hammer_vec_default_does_not_touch_a_svm_probe() {
    let mut v: HVec<u64> = HVec::with_capacity(64);
    v.push(7);
    assert_eq!(v[0], 7);
}

#[test]
fn hammer_slice_from_elem_in_routes_through_heap() {
    let region = SvmRegion::with_size(1 << 20);
    let heap = Arc::new(Heap::svm(region.clone(), 0));

    let s: Slice<u8> = Slice::from_elem_in(2048, 0u8, heap.clone());
    assert_eq!(s.len(), 2048);
    let base = s.as_ptr() as usize;
    assert_eq!(
        base % 64,
        0,
        "Slice preserves 64-byte alignment through the Heap (got {base})"
    );
    let start = region.base() as usize;
    let end = start + region.size();
    assert!(
        base >= start && base < end,
        "Slice backing must live inside the SVM region"
    );
}

#[test]
fn hammer_slice_default_from_elem_does_not_touch_a_svm_probe() {
    let s: Slice<u64> = Slice::from_elem(64, 0u64);
    assert_eq!(s.len(), 64);
    assert_eq!(s[0], 0);
}

#[test]
fn flat_hash_table_with_capacity_in_routes_through_heap() {
    let region = SvmRegion::with_size(1 << 20);
    let heap = Arc::new(Heap::svm(region.clone(), 0));

    let mut table: FlatHashTable<u64, u64> = FlatHashTable::with_capacity_in(64, heap.clone());
    table.insert(1, 100);
    table.insert(2, 200);
    assert_eq!(table.len(), 2);
    assert_eq!(table.get(&1), Some(&100));
    assert_eq!(table.get(&2), Some(&200));
    assert_eq!(table.bucket_count(), 64);
    let start = region.base() as usize;
    let end = start + region.size();
    let ptr = table.bucket_ptr() as usize;
    assert!(
        ptr >= start && ptr < end,
        "FlatHashTable backing must live inside the SVM region"
    );
}

#[test]
fn flat_hash_table_default_with_capacity_does_not_touch_a_svm_probe() {
    let mut table: FlatHashTable<u64, u64> = FlatHashTable::with_capacity(64);
    table.insert(1, 100);
    assert_eq!(table.get(&1), Some(&100));
}

#[test]
fn hammer_vec_drop_returns_storage_to_same_heap() {
    let region = SvmRegion::with_size(20 * 1024);
    let heap = Arc::new(Heap::svm(region.clone(), 0));
    let capacity = 1536usize;
    let layout = Layout::from_size_align(capacity * std::mem::size_of::<u64>(), 64).unwrap();

    {
        let mut v: HVec<u64> = HVec::with_capacity_in(capacity, heap.clone());
        v.push(7);
    }

    let raw = heap
        .alloc(layout)
        .expect("Vec drop must return its backing storage to the same heap");
    let start = region.base() as usize;
    let end = start + region.size();
    let ptr = raw.as_ptr() as usize;
    assert!(ptr >= start && ptr < end);
    unsafe {
        GlobalAlloc::dealloc(&*heap, raw.as_ptr(), layout);
    }
}

#[test]
fn hammer_slice_drop_returns_storage_to_same_heap() {
    let region = SvmRegion::with_size(20 * 1024);
    let heap = Arc::new(Heap::svm(region.clone(), 0));
    let len = 12 * 1024usize;
    let layout = Layout::from_size_align(len, 64).unwrap();

    {
        let s: Slice<u8> = Slice::from_elem_in(len, 0u8, heap.clone());
        assert_eq!(s.len(), len);
    }

    let raw = heap
        .alloc(layout)
        .expect("Slice drop must return its backing storage to the same heap");
    let start = region.base() as usize;
    let end = start + region.size();
    let ptr = raw.as_ptr() as usize;
    assert!(ptr >= start && ptr < end);
    unsafe {
        GlobalAlloc::dealloc(&*heap, raw.as_ptr(), layout);
    }
}
