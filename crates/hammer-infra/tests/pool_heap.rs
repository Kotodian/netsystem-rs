//! Pool must allocate its backing slab through a Heap, not std::alloc directly.
use std::alloc::Layout;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_infra::heap::Heap;
use hammer_infra::pool::Pool;

#[derive(Debug)]
struct CountingHeap {
    count: AtomicUsize,
    numa: u32,
}

impl CountingHeap {
    fn new(numa: u32) -> Self {
        CountingHeap {
            count: AtomicUsize::new(0),
            numa,
        }
    }
}

impl Heap for CountingHeap {
    fn alloc(&self, layout: Layout) -> Option<std::ptr::NonNull<u8>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        let p = unsafe { std::alloc::alloc(layout) };
        std::ptr::NonNull::new(p)
    }

    unsafe fn dealloc(&self, ptr: std::ptr::NonNull<u8>, layout: Layout) {
        unsafe {
            std::alloc::dealloc(ptr.as_ptr(), layout);
        }
    }

    fn numa_node(&self) -> u32 {
        self.numa
    }
}

#[test]
fn pool_with_capacity_in_routes_through_heap() {
    let heap = Arc::new(CountingHeap::new(2));
    let before = heap.count.load(Ordering::SeqCst);
    let mut pool: Pool<u64> = Pool::with_capacity_in(64, heap.clone());
    let after = heap.count.load(Ordering::SeqCst);
    assert_eq!(
        after - before,
        1,
        "Pool slab must come from the provided Heap"
    );
    assert_eq!(pool.capacity(), 64);
    // Insert/remove still work end-to-end
    let idx = pool.insert(42u64).expect("insert should succeed");
    assert_eq!(pool.get(idx), Some(&42));
}
