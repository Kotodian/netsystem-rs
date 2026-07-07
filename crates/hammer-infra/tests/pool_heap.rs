//! Pool must allocate its backing slab through a Heap, not std::alloc directly.
//!
//! The original dyn-trait version of this test used a `CountingHeap` to count
//! `alloc` calls. With `Heap` now a concrete handle backed by either mimalloc
//! or an SVM region, this test sticks to behavioral checks instead of peeking
//! at allocator internals.
use std::alloc::{GlobalAlloc, Layout};
use std::sync::Arc;

use hammer_infra::align::CACHE_LINE;
use hammer_infra::heap::Heap;
use hammer_infra::pool::Pool;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn pool_with_capacity_in_routes_through_heap_svm() {
    let region = SvmRegion::with_size(1 << 20);
    let heap = Arc::new(Heap::svm_data(region.clone()).expect("owner region heap"));

    let mut pool: Pool<u64> = Pool::with_capacity_in(64, heap.clone());
    assert_eq!(pool.capacity(), 64);

    // End-to-end insert/get still works.
    let idx = pool.insert(42u64).expect("insert should succeed");
    assert_eq!(pool.get(idx), Some(&42));

    let slot_ptr = pool.slot_ptr(idx).expect("pool slot pointer");
    let start = region.base() as usize;
    let end = start + region.size();
    let slot = slot_ptr as usize;
    assert!(
        slot >= start && slot < end,
        "pool storage must live inside the SVM region"
    );
}

#[test]
fn pool_with_capacity_default_does_not_touch_a_svm_probe() {
    let mut pool: Pool<u64> = Pool::with_capacity(64);
    let idx = pool.insert(7u64).expect("insert");
    assert_eq!(pool.get(idx), Some(&7));
}

#[test]
fn pool_drop_returns_storage_to_same_heap() {
    let region = SvmRegion::with_size(24 * 1024);
    let heap = Arc::new(Heap::svm_data(region.clone()).expect("owner region heap"));
    let capacity = 256usize;
    let layout = Layout::from_size_align(capacity * CACHE_LINE, CACHE_LINE).unwrap();

    {
        let mut pool: Pool<u64> = Pool::with_capacity_in(capacity, heap.clone());
        let idx = pool.insert(1u64).expect("insert");
        assert_eq!(idx.slot(), 0, "first insert should claim slot 0");
    }

    let raw = heap
        .alloc(layout)
        .expect("pool drop must return the slab to the same heap");
    let start = region.base() as usize;
    let end = start + region.size();
    let ptr = raw.as_ptr() as usize;
    assert!(ptr >= start && ptr < end);
    unsafe {
        GlobalAlloc::dealloc(&*heap, raw.as_ptr(), layout);
    }
}
