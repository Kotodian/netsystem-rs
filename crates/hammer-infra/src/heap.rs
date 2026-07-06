//! Concrete VPP-style heap handle.
//!
//! Hammer follows VPP's "active heap" model with a concrete handle instead of
//! a public heap trait. The handle carries a private vtable plus backend state
//! and routes allocation through either mimalloc or an owner SVM region.

use std::alloc::{GlobalAlloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::svm_region::SvmRegion;

static MAIN_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
const MAX_HEAP_REGISTRY_SLOTS: usize = 64;

struct HeapVTable {
    alloc: unsafe fn(*const (), Layout) -> *mut u8,
    dealloc: unsafe fn(*const (), *mut u8, Layout),
    numa_node: unsafe fn(*const ()) -> u32,
    is_main_heap: unsafe fn(*const ()) -> bool,
}

enum HeapData {
    Main { numa: u32 },
    Svm { region: SvmRegion, numa: u32 },
}

#[derive(Clone)]
pub struct Heap {
    vtable: &'static HeapVTable,
    data: Arc<HeapData>,
}

impl Heap {
    #[inline]
    pub fn main(numa: u32) -> Heap {
        Heap {
            vtable: &MAIN_VTABLE,
            data: Arc::new(HeapData::Main { numa }),
        }
    }

    #[inline]
    pub fn local(numa: u32) -> Heap {
        Heap::main(numa)
    }

    #[inline]
    pub fn svm(region: SvmRegion, numa: u32) -> Heap {
        assert!(
            region.is_allocation_owner(),
            "attached SVM regions are read/write mappings, not allocation-owner heaps"
        );
        Heap {
            vtable: &SVM_VTABLE,
            data: Arc::new(HeapData::Svm { region, numa }),
        }
    }

    #[inline]
    pub fn alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        let ptr = unsafe { GlobalAlloc::alloc(self, layout) };
        NonNull::new(ptr)
    }

    #[inline]
    pub fn numa_node(&self) -> u32 {
        unsafe { (self.vtable.numa_node)(self.data_ptr()) }
    }

    #[inline]
    pub fn is_main_heap(&self) -> bool {
        unsafe { (self.vtable.is_main_heap)(self.data_ptr()) }
    }

    #[inline]
    pub fn region(&self) -> Option<&SvmRegion> {
        match self.data.as_ref() {
            HeapData::Svm { region, .. } => Some(region),
            HeapData::Main { .. } => None,
        }
    }

    #[inline]
    fn data_ptr(&self) -> *const () {
        Arc::as_ptr(&self.data).cast::<()>()
    }
}

unsafe impl GlobalAlloc for Heap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { (self.vtable.alloc)(self.data_ptr(), layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { (self.vtable.dealloc)(self.data_ptr(), ptr, layout) }
    }
}

unsafe fn main_alloc_callback(_: *const (), layout: Layout) -> *mut u8 {
    unsafe { GlobalAlloc::alloc(&MAIN_ALLOCATOR, layout) }
}

unsafe fn main_dealloc_callback(_: *const (), ptr: *mut u8, layout: Layout) {
    unsafe { GlobalAlloc::dealloc(&MAIN_ALLOCATOR, ptr, layout) }
}

unsafe fn main_numa_callback(data: *const ()) -> u32 {
    match unsafe { &*(data.cast::<HeapData>()) } {
        HeapData::Main { numa } => *numa,
        HeapData::Svm { .. } => 0,
    }
}

unsafe fn main_kind_callback(_: *const ()) -> bool {
    true
}

static MAIN_VTABLE: HeapVTable = HeapVTable {
    alloc: main_alloc_callback,
    dealloc: main_dealloc_callback,
    numa_node: main_numa_callback,
    is_main_heap: main_kind_callback,
};

unsafe fn shared_owner_alloc_callback(data: *const (), layout: Layout) -> *mut u8 {
    let HeapData::Svm { region, .. } = (unsafe { &*(data.cast::<HeapData>()) }) else {
        return std::ptr::null_mut();
    };
    region
        .alloc_layout(layout)
        .map_or(std::ptr::null_mut(), NonNull::as_ptr)
}

unsafe fn shared_owner_dealloc_callback(data: *const (), ptr: *mut u8, layout: Layout) {
    let HeapData::Svm { region, .. } = (unsafe { &*(data.cast::<HeapData>()) }) else {
        return;
    };
    if let Some(ptr) = NonNull::new(ptr) {
        unsafe { region.dealloc_layout(ptr, layout) };
    }
}

unsafe fn shared_owner_numa_callback(data: *const ()) -> u32 {
    match unsafe { &*(data.cast::<HeapData>()) } {
        HeapData::Svm { numa, .. } => *numa,
        HeapData::Main { .. } => 0,
    }
}

unsafe fn shared_owner_kind_callback(_: *const ()) -> bool {
    false
}

static SVM_VTABLE: HeapVTable = HeapVTable {
    alloc: shared_owner_alloc_callback,
    dealloc: shared_owner_dealloc_callback,
    numa_node: shared_owner_numa_callback,
    is_main_heap: shared_owner_kind_callback,
};

pub struct HeapRegistry {
    heaps: [Option<Arc<Heap>>; MAX_HEAP_REGISTRY_SLOTS],
}

impl HeapRegistry {
    pub fn new() -> HeapRegistry {
        HeapRegistry {
            heaps: std::array::from_fn(|_| None),
        }
    }

    pub fn register(&mut self, heap: Arc<Heap>) -> &mut Self {
        let index = heap.numa_node() as usize;
        assert!(
            index < MAX_HEAP_REGISTRY_SLOTS,
            "NUMA heap index {index} exceeds fixed registry capacity {MAX_HEAP_REGISTRY_SLOTS}"
        );
        self.heaps[index] = Some(heap);
        self
    }

    pub fn for_numa(&self, numa: u32) -> Option<Arc<Heap>> {
        self.heaps.get(numa as usize).and_then(|heap| heap.clone())
    }
}

impl Default for HeapRegistry {
    fn default() -> Self {
        HeapRegistry::new()
    }
}
