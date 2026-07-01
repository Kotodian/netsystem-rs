//! Pool must allocate its backing slab through a Heap, not std::alloc directly.
//!
//! The original dyn-trait version of this test used a `CountingHeap` to count
//! `alloc` calls. With `Heap` now a hand-written vtable struct (no `dyn`,
//! no public `Heap` trait), the test probes `HeapSvm` via the
//! `SvmRegion`'s bump pointer and free list: building a
//! `Pool::with_capacity_in(...)` must advance the bump by at least the slab
//! size, and dropping the pool must push the freed slab back to the LIFO
//! freelist. Default `Pool::with_capacity` must use `Heap::local(0)` and
//! leave the Svm region untouched.
use std::sync::Arc;

use hammer_infra::heap::Heap;
use hammer_infra::pool::Pool;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn pool_with_capacity_in_routes_through_heap_svm() {
    let region = SvmRegion::with_size(1 << 20);
    let heap = Arc::new(Heap::svm(region.clone(), 0));

    let bump_before = region.bump_depth();
    let freelist_before = region.freelist_len();

    let mut pool: Pool<u64> = Pool::with_capacity_in(64, heap.clone());
    assert_eq!(pool.capacity(), 64);

    // 64 slots * 64-byte (cache-line) stride = 4096 byte slab. The bump may
    // land at a higher aligned offset, but the *minimum* delta is 4096.
    let bump_after = region.bump_depth();
    let slab_bytes = bump_after - bump_before;
    assert!(
        slab_bytes >= 4096,
        "Pool slab must come from the provided Heap (bump advanced by {slab_bytes} bytes, expected >= 4096)"
    );

    // End-to-end insert/get still works.
    let idx = pool.insert(42u64).expect("insert should succeed");
    assert_eq!(pool.get(idx), Some(&42));
    drop(pool);

    // After `Drop`, the slab layout is freed back to the SvmRegion's freelist.
    assert_eq!(
        region.freelist_len(),
        freelist_before + 1,
        "Pool::drop must route the slab back through the same Heap"
    );
}

#[test]
fn pool_with_capacity_default_does_not_touch_a_svm_probe() {
    // Default `with_capacity` must use `Heap::local`, not a Svm region.
    let region = SvmRegion::with_size(1 << 16);
    let bump_before = region.bump_depth();
    let mut pool: Pool<u64> = Pool::with_capacity(64);
    let _ = pool.insert(7u64).expect("insert");
    assert_eq!(
        region.bump_depth(),
        bump_before,
        "default `Pool::with_capacity` must not allocate from a Svm region"
    );
}
