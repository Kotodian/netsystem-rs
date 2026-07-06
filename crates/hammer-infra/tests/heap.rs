use std::alloc::{GlobalAlloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;

use hammer_infra::heap::{Heap, HeapRegistry};
use hammer_infra::segment::{Segment, Svm};
use hammer_infra::svm_region::SvmRegion;

struct RawAllocation<'heap> {
    heap: &'heap Heap,
    ptr: NonNull<u8>,
    layout: Layout,
}

impl<'heap> RawAllocation<'heap> {
    fn new(heap: &'heap Heap, layout: Layout) -> Self {
        let ptr = heap.alloc(layout).expect("alloc");
        Self { heap, ptr, layout }
    }

    fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }
}

impl Drop for RawAllocation<'_> {
    fn drop(&mut self) {
        unsafe {
            GlobalAlloc::dealloc(self.heap, self.ptr.as_ptr(), self.layout);
        }
    }
}

fn assert_allocation_inside_region(region: &SvmRegion, allocation: &RawAllocation<'_>) {
    let start = region.base() as usize;
    let end = start + region.size();
    let ptr = allocation.ptr().as_ptr() as usize;
    assert!(ptr >= start, "allocation must start inside SVM region");
    assert!(
        ptr + allocation.layout.size() <= end,
        "allocation must end inside SVM region"
    );
}

#[test]
fn heap_api_is_concrete_handle_only() {
    let src = include_str!("../src/heap.rs");
    for forbidden in [
        "pub trait Heap",
        "HeapLocal",
        "HeapSvm",
        "H: Heap",
        "dyn GlobalAlloc",
        "#[global_allocator]",
        "std::alloc::Allocator",
        "std::alloc::System",
        "pub fn dealloc",
        "pub unsafe fn dealloc",
        "BumpMainHeap",
        "MainHeapFreeList",
        "Vec<Arc<Heap>>",
        "pub fn free",
        "pub fn freelist_len",
        "fn local_alloc",
        "fn local_dealloc",
        "fn local_numa",
        "fn svm_alloc",
        "fn svm_dealloc",
        "fn svm_numa",
        "fn alloc_region",
        "fn page_size",
    ] {
        assert!(
            !src.contains(forbidden),
            "heap.rs must not expose {forbidden}"
        );
    }
    let trait_object_spell = format!("{} {}", "dyn", "Heap");
    assert!(
        !src.contains(&trait_object_spell),
        "Heap must stay a concrete handle, not a trait object"
    );
}

#[test]
fn svm_region_uses_talc_not_private_span_lists() {
    let src = include_str!("../src/svm_region.rs");
    assert!(src.contains("talc::TalcLock"));
    assert!(src.contains("source::Manual"));
    assert!(
        !src.contains("Layout::from_size_align(bytes, SVM_OFFSET_ALIGN)"),
        "offset free must not reconstruct a fixed 64-byte layout"
    );
    for forbidden in [
        format!("{}{}", "Returned", "Span"),
        format!("{}{}", "returned", "_spans"),
        format!("{}{}", "bump", "_depth"),
        format!("SVM_{}{}", "RETURNED_SPAN", "_NODES"),
        "std::vec::Vec".to_string(),
        "hammer_infra::vec::Vec".to_string(),
    ] {
        assert!(
            !src.contains(&forbidden),
            "svm_region.rs must not hand-roll allocator metadata: {forbidden}"
        );
    }
}

#[test]
fn heap_main_mimalloc_alloc_round_trip_uses_drop_guard() {
    let src = include_str!("../src/heap.rs");
    assert!(src.contains("mimalloc::MiMalloc"));
    assert!(!src.contains("#[global_allocator]"));

    let heap = Heap::main(0);
    assert_eq!(heap.numa_node(), 0);
    assert!(heap.is_main_heap());
    let layout = Layout::from_size_align(128, 64).unwrap();
    let allocation = RawAllocation::new(&heap, layout);
    unsafe {
        std::ptr::write_bytes(allocation.ptr().as_ptr(), 0xAB, layout.size());
    }
}

#[test]
fn heap_local_is_main_heap_alias() {
    let heap = Heap::local(3);
    assert_eq!(heap.numa_node(), 3);
    assert!(heap.is_main_heap());
}

#[test]
fn heap_svm_allocation_is_returned_by_drop() {
    let region = SvmRegion::with_size(48 * 1024);
    let heap = Arc::new(Heap::svm(region.clone(), 1));
    let layout = Layout::from_size_align(32 * 1024, 64).unwrap();

    {
        let allocation = RawAllocation::new(&heap, layout);
        assert_allocation_inside_region(&region, &allocation);
    }

    {
        let allocation = RawAllocation::new(&heap, layout);
        assert_allocation_inside_region(&region, &allocation);
        unsafe {
            std::ptr::write_bytes(allocation.ptr().as_ptr(), 0xCD, layout.size());
        }
    }
}

#[test]
fn heap_clone_shares_svm_region() {
    let region = SvmRegion::with_size(1 << 16);
    let heap = Arc::new(Heap::svm(region, 0));
    let clone = heap.clone();
    drop(heap);
    assert!(clone.region().expect("clone keeps region alive").size() > 0);
}

#[test]
fn svm_region_maps_fd_backed_shared_memory() {
    let region = SvmRegion::with_size(4096);
    assert!(region.size() >= 4096);
    assert!(region.fd() >= 0);

    let offset = region.alloc(16, 8);
    assert_ne!(offset, u64::MAX);
    unsafe {
        std::ptr::write_bytes(region.base().add(offset as usize), 0x5A, 16);
    }

    let attached = SvmRegion::from_fd(region.fd(), region.size()).expect("attach fd");
    unsafe {
        assert_eq!(*attached.base().add(offset as usize), 0x5A);
    }
}

#[test]
fn svm_offset_free_round_trip_preserves_alignment_over_cache_line() {
    let name = format!("heap-align-round-trip-{}", std::process::id());
    let segment = Svm::create(&name, 16 * 1024).expect("create svm segment");
    let bytes = 12 * 1024;
    let align = 256usize;

    let first = segment.alloc(bytes, align);
    assert_ne!(first, u64::MAX);
    assert_eq!(first % align as u64, 0);

    segment.free(first, bytes);

    let second = segment.alloc(bytes, align);
    assert_ne!(
        second,
        u64::MAX,
        "aligned offset allocation must be reusable after free"
    );
    assert_eq!(second % align as u64, 0);
}

#[test]
fn attached_svm_region_is_not_an_allocator_owner() {
    let region = SvmRegion::with_size(4096);
    let attached = SvmRegion::from_fd(region.fd(), region.size()).expect("attach fd");

    assert_eq!(
        attached.alloc(16, 8),
        u64::MAX,
        "attached mappings can read/write existing offsets but cannot allocate"
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _heap = Heap::svm(attached, 0);
    }));
    assert!(
        result.is_err(),
        "Heap::svm must reject an attached mapping because this plan is not full VPP ssvm_mem_alloc"
    );
}

#[test]
fn heap_registry_lookup_by_numa() {
    let mut registry = HeapRegistry::new();
    registry.register(Arc::new(Heap::main(0)));
    registry.register(Arc::new(Heap::svm(SvmRegion::with_size(1 << 16), 1)));
    assert_eq!(registry.for_numa(0).unwrap().numa_node(), 0);
    assert_eq!(registry.for_numa(1).unwrap().numa_node(), 1);
    assert!(registry.for_numa(2).is_none());
}
