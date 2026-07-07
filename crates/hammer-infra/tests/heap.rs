use std::alloc::{GlobalAlloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;

use hammer_infra::heap::{Heap, HeapError};
use hammer_infra::svm_region::SvmRegion;

struct HeapAllocationGuard<'heap> {
    heap: &'heap Heap,
    ptr: NonNull<u8>,
    layout: Layout,
}

impl<'heap> HeapAllocationGuard<'heap> {
    fn new(heap: &'heap Heap, layout: Layout) -> Self {
        let ptr = heap.alloc(layout).expect("alloc");
        Self { heap, ptr, layout }
    }

    fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }
}

impl Drop for HeapAllocationGuard<'_> {
    fn drop(&mut self) {
        unsafe {
            GlobalAlloc::dealloc(self.heap, self.ptr.as_ptr(), self.layout);
        }
    }
}

fn assert_allocation_inside_region(region: &SvmRegion, allocation: &HeapAllocationGuard<'_>) {
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
        concat!("pub struct Heap", "Registry"),
        concat!("pub fn main", "(numa"),
        concat!("pub fn local", "(numa"),
        concat!("pub fn numa", "_node"),
        "pub fn dealloc",
        "pub unsafe fn dealloc",
        "BumpMainHeap",
        "MainHeapFreeList",
        "Vec<Arc<Heap>>",
        "pub fn free",
        "pub fn freelist_len",
        "fn local_alloc",
        "fn local_dealloc",
        concat!("fn local", "_numa"),
        concat!("fn svm_", "alloc"),
        concat!("fn svm_", "dealloc"),
        concat!("fn svm", "_numa"),
        concat!("fn alloc", "_region"),
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

    let heap = Heap::main();
    assert!(heap.is_main_heap());
    let layout = Layout::from_size_align(128, 64).unwrap();
    let allocation = HeapAllocationGuard::new(&heap, layout);
    unsafe {
        std::ptr::write_bytes(allocation.ptr().as_ptr(), 0xAB, layout.size());
    }
}

#[test]
fn heap_local_is_main_heap_alias() {
    let heap = Heap::local();
    assert!(heap.is_main_heap());
}

#[test]
fn heap_shared_region_bytes_are_returned_by_drop() {
    let region = SvmRegion::with_size(48 * 1024);
    let heap = Arc::new(Heap::svm_data(region.clone()).expect("owner region heap"));
    let layout = Layout::from_size_align(32 * 1024, 64).unwrap();

    {
        let allocation = HeapAllocationGuard::new(&heap, layout);
        assert_allocation_inside_region(&region, &allocation);
    }

    {
        let allocation = HeapAllocationGuard::new(&heap, layout);
        assert_allocation_inside_region(&region, &allocation);
        unsafe {
            std::ptr::write_bytes(allocation.ptr().as_ptr(), 0xCD, layout.size());
        }
    }
}

#[test]
fn heap_clone_shares_svm_region() {
    let region = SvmRegion::with_size(1 << 16);
    let heap = Arc::new(Heap::svm_data(region).expect("owner region heap"));
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
fn svm_heap_drop_round_trip_preserves_alignment_over_cache_line() {
    let region = SvmRegion::with_size(16 * 1024);
    let heap = Heap::svm_data(region.clone()).expect("owner region heap");
    let bytes = 12 * 1024;
    let align = 256usize;
    let layout = Layout::from_size_align(bytes, align).expect("aligned layout");

    let first = {
        let allocation = HeapAllocationGuard::new(&heap, layout);
        assert_allocation_inside_region(&region, &allocation);
        assert_eq!((allocation.ptr().as_ptr() as usize) % align, 0);
        allocation.ptr().as_ptr() as usize
    };

    let second = {
        let allocation = HeapAllocationGuard::new(&heap, layout);
        assert_allocation_inside_region(&region, &allocation);
        assert_eq!((allocation.ptr().as_ptr() as usize) % align, 0);
        allocation.ptr().as_ptr() as usize
    };
    assert_eq!(first, second);
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
    assert!(
        matches!(Heap::svm_data(attached), Err(HeapError::AttachedSvmRegion)),
        "Heap::svm_data must reject attached mappings because this plan is not full VPP ssvm_mem_alloc"
    );
}
