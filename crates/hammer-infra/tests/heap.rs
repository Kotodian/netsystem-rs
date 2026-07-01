//! Tests for hammer-infra Heap abstraction + SvmRegion primitive.
use std::alloc::Layout;
use std::sync::Arc;

use hammer_infra::heap::{Heap, HeapRegistry};
use hammer_infra::svm_region::SvmRegion;

#[test]
fn heap_local_alloc_dealloc_round_trip() {
    let heap = Heap::local(0);
    assert_eq!(heap.numa_node(), 0);
    let layout = Layout::from_size_align(128, 64).unwrap();
    let ptr = heap.alloc(layout).expect("alloc");
    unsafe {
        std::ptr::write_bytes(ptr.as_ptr(), 0xAB, layout.size());
        heap.dealloc(ptr, layout);
    }
    assert_eq!(heap.numa_node(), 0);
}

#[test]
fn heap_svm_alloc_reuse_and_free() {
    let region = SvmRegion::with_size(1 << 20); // 1 MiB
    let heap = Heap::svm(region, 1);
    assert_eq!(heap.numa_node(), 1);
    let layout = Layout::from_size_align(256, 64).unwrap();
    let p1 = heap.alloc(layout).expect("first alloc");
    unsafe {
        heap.dealloc(p1, layout);
    }
    let p2 = heap.alloc(layout).expect("second alloc reuses free-list");
    assert_eq!(p2, p1, "freed slot must be reused on next alloc");
    unsafe {
        heap.dealloc(p2, layout);
    }
}

#[test]
fn heap_svm_region_accessor_returns_some() {
    // Type-erased region accessor: walks the vtable pointer, returns
    // `Some(&SvmRegion)` only when the vtable is the SVM vtable.
    let region = SvmRegion::with_size(1 << 16);
    let expected_size = region.size();
    let heap = Heap::svm(region, 0);
    let region_ref = heap.region().expect("svm heap exposes its region");
    assert_eq!(region_ref.size(), expected_size);
    let local = Heap::local(0);
    assert!(
        local.region().is_none(),
        "local heap must not expose a region"
    );
}

#[test]
fn heap_clone_shares_svm_region() {
    // Cloning a Heap must keep the underlying SvmRegion alive (Arc
    // bump). Drop the original first, then the clone should still
    // see a valid region (no use-after-drop).
    let region = SvmRegion::with_size(1 << 16);
    let heap = Arc::new(Heap::svm(region, 0));
    let clone = heap.clone();
    drop(heap);
    let region_ref = clone.region().expect("clone still owns its region");
    assert!(region_ref.size() > 0);
}

#[test]
fn heap_registry_lookup_by_numa() {
    let mut reg = HeapRegistry::new();
    let l0 = Arc::new(Heap::local(0));
    let svm = Arc::new(Heap::svm(SvmRegion::with_size(1 << 16), 1));
    assert!(reg.for_numa(5).is_none());
    reg.register(l0); // numa 0
    reg.register(svm); // numa 1
    assert_eq!(reg.for_numa(0).unwrap().numa_node(), 0);
    assert_eq!(reg.for_numa(1).unwrap().numa_node(), 1);
    assert!(reg.for_numa(2).is_none());
}

#[test]
fn svm_region_default_is_nonzero_and_aligned() {
    let r = SvmRegion::default();
    assert!(r.size() > 0);
    let base = r.base();
    assert_eq!(base as usize % 64, 0, "base must be 64-byte aligned");
    let off = r.alloc(128, 64);
    assert!(off != u64::MAX);
}
