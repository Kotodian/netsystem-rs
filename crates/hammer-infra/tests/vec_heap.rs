//! Vec/Slice/FlatHashTable must allocate their backing storage through a Heap
//! instead of `std::alloc` directly, matching VPP's "active heap" model.
//!
//! The original dyn-trait version of this test used a `CountingHeap` to count
//! `alloc` calls. With `Heap` now a hand-written vtable struct (no `dyn`,
//! no public `Heap` trait), the test probes `HeapSvm` via the
//! `SvmRegion`'s bump pointer and free list: building a
//! `Vec::with_capacity_in(...)` / `Slice::from_elem_in(...)` /
//! `FlatHashTable::with_capacity_in(...)` must advance the bump by at least
//! the layout's byte count, and dropping the collection must push the freed
//! region back to the LIFO freelist. Default back-compat constructors must
//! use `Heap::local(0)` and leave the Svm region untouched.
#![allow(deprecated)]

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

    let bump_before = region.bump_depth();
    let freelist_before = region.freelist_len();

    let mut v: HVec<u64> = HVec::with_capacity_in(128, heap.clone());
    assert_eq!(v.capacity(), 128);
    for i in 0..128u64 {
        v.push(i);
    }
    assert_eq!(v.len(), 128);
    assert_eq!(v[127], 127);

    // 128 * 8 = 1024 bytes; with 64-byte alignment the bump advances by at
    // least 1024 bytes (and likely a full cache-line multiple, but >= 1024
    // is the contract).
    let bump_after = region.bump_depth();
    let bumped = bump_after - bump_before;
    assert!(
        bumped >= 1024,
        "Vec backing must be heap-allocated (bump advanced by {bumped} bytes, expected >= 1024)"
    );

    drop(v);
    assert_eq!(
        region.freelist_len(),
        freelist_before + 1,
        "Vec::drop must route the backing region back through the same Heap"
    );
}

#[test]
fn hammer_vec_default_does_not_touch_a_svm_probe() {
    // Default `with_capacity` must use `Heap::local`, not a Svm region.
    let region = SvmRegion::with_size(1 << 16);
    let bump_before = region.bump_depth();
    let mut v: HVec<u64> = HVec::with_capacity(64);
    v.push(7);
    assert_eq!(
        region.bump_depth(),
        bump_before,
        "default `Vec::with_capacity` must not allocate from a Svm region"
    );
}

#[test]
fn hammer_slice_from_elem_in_routes_through_heap() {
    let region = SvmRegion::with_size(1 << 20);
    let heap = Arc::new(Heap::svm(region.clone(), 0));

    let bump_before = region.bump_depth();
    let freelist_before = region.freelist_len();

    let s: Slice<u8> = Slice::from_elem_in(2048, 0u8, heap.clone());
    assert_eq!(s.len(), 2048);
    let base = s.as_ptr() as usize;
    assert_eq!(
        base % 64,
        0,
        "Slice preserves 64-byte alignment through the Heap (got {base})"
    );

    // 2048 * 1 = 2048 bytes minimum; bump advances by at least that.
    let bump_after = region.bump_depth();
    let bumped = bump_after - bump_before;
    assert!(
        bumped >= 2048,
        "Slice backing must be heap-allocated (bump advanced by {bumped} bytes, expected >= 2048)"
    );

    drop(s);
    assert_eq!(
        region.freelist_len(),
        freelist_before + 1,
        "Slice::drop must route the backing region back through the same Heap"
    );
}

#[test]
fn hammer_slice_default_from_elem_does_not_touch_a_svm_probe() {
    let region = SvmRegion::with_size(1 << 16);
    let bump_before = region.bump_depth();
    let s: Slice<u64> = Slice::from_elem(64, 0u64);
    assert_eq!(s.len(), 64);
    assert_eq!(
        region.bump_depth(),
        bump_before,
        "default `Slice::from_elem` must not allocate from a Svm region"
    );
}

#[test]
fn flat_hash_table_with_capacity_in_routes_through_heap() {
    let region = SvmRegion::with_size(1 << 20);
    let heap = Arc::new(Heap::svm(region.clone(), 0));

    let bump_before = region.bump_depth();
    let freelist_before = region.freelist_len();

    let mut table: FlatHashTable<u64, u64> = FlatHashTable::with_capacity_in(64, heap.clone());
    table.insert(1, 100);
    table.insert(2, 200);
    assert_eq!(table.len(), 2);
    assert_eq!(table.get(&1), Some(&100));
    assert_eq!(table.get(&2), Some(&200));
    assert_eq!(table.bucket_count(), 64);

    // Bucket storage comes from the heap. 64 buckets * sizeof(Bucket) — the
    // exact byte count depends on the bucket struct size, but the bump
    // must have advanced by a positive amount >= one cache line.
    let bump_after = region.bump_depth();
    let bumped = bump_after - bump_before;
    assert!(
        bumped >= 64,
        "FlatHashTable bucket storage must be heap-allocated (bump advanced by {bumped} bytes)"
    );

    drop(table);
    assert_eq!(
        region.freelist_len(),
        freelist_before + 1,
        "FlatHashTable::drop must route the bucket storage back through the same Heap"
    );
}

#[test]
fn flat_hash_table_default_with_capacity_does_not_touch_a_svm_probe() {
    let region = SvmRegion::with_size(1 << 16);
    let bump_before = region.bump_depth();
    let mut table: FlatHashTable<u64, u64> = FlatHashTable::with_capacity(64);
    table.insert(1, 100);
    assert_eq!(
        region.bump_depth(),
        bump_before,
        "default `FlatHashTable::with_capacity` must not allocate from a Svm region"
    );
}
