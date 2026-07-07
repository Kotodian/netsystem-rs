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

struct HeapVTable {
    alloc: unsafe fn(*const (), Layout) -> *mut u8,
    dealloc: unsafe fn(*const (), *mut u8, Layout),
}

enum HeapData {
    Main,
    SvmData { region: SvmRegion },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapError {
    AttachedSvmRegion,
}

#[derive(Clone)]
pub struct Heap {
    vtable: &'static HeapVTable,
    data: Arc<HeapData>,
}

impl Heap {
    #[inline]
    pub fn main() -> Heap {
        Heap {
            vtable: &MAIN_VTABLE,
            data: Arc::new(HeapData::Main),
        }
    }

    #[inline]
    pub fn local() -> Heap {
        Heap::main()
    }

    #[inline]
    pub fn svm_data(region: SvmRegion) -> Result<Heap, HeapError> {
        if !region.is_allocation_owner() {
            return Err(HeapError::AttachedSvmRegion);
        }
        Ok(Heap {
            vtable: &SVM_VTABLE,
            data: Arc::new(HeapData::SvmData { region }),
        })
    }

    #[inline]
    pub fn alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        let ptr = unsafe { GlobalAlloc::alloc(self, layout) };
        NonNull::new(ptr)
    }

    #[inline]
    pub fn is_main_heap(&self) -> bool {
        matches!(self.data.as_ref(), HeapData::Main)
    }

    #[inline]
    pub fn region(&self) -> Option<&SvmRegion> {
        match self.data.as_ref() {
            HeapData::SvmData { region } => Some(region),
            HeapData::Main => None,
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

static MAIN_VTABLE: HeapVTable = HeapVTable {
    alloc: main_alloc_callback,
    dealloc: main_dealloc_callback,
};

unsafe fn shared_owner_alloc_callback(data: *const (), layout: Layout) -> *mut u8 {
    let HeapData::SvmData { region } = (unsafe { &*(data.cast::<HeapData>()) }) else {
        return std::ptr::null_mut();
    };
    region
        .alloc_layout(layout)
        .map_or(std::ptr::null_mut(), NonNull::as_ptr)
}

unsafe fn shared_owner_dealloc_callback(data: *const (), ptr: *mut u8, layout: Layout) {
    let HeapData::SvmData { region } = (unsafe { &*(data.cast::<HeapData>()) }) else {
        return;
    };
    if let Some(ptr) = NonNull::new(ptr) {
        unsafe { region.dealloc_layout(ptr, layout) };
    }
}

static SVM_VTABLE: HeapVTable = HeapVTable {
    alloc: shared_owner_alloc_callback,
    dealloc: shared_owner_dealloc_callback,
};
