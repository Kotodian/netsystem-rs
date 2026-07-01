//! VPP "main heap" abstraction: a replaceable, per-NUMA allocation backend.
//! Pool/Vec/Slice draw from a Heap instead of calling std::alloc directly,
//! matching VPP's "active heap" model (vppinfra/mem.h) without porting the
//! slab/bin internals — that belongs to the global allocator, not the framework.
use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::svm_region::SvmRegion;

pub trait Heap: Send + Sync + 'static {
    fn alloc(&self, layout: Layout) -> Option<NonNull<u8>>;
    /// # Safety
    /// `ptr` must have been returned by `self.alloc(layout)` and not previously
    /// deallocated; `layout` must match the alloc call exactly.
    unsafe fn dealloc(&self, ptr: NonNull<u8>, layout: Layout);
    fn numa_node(&self) -> u32;
}

pub struct HeapLocal {
    numa: u32,
}

impl HeapLocal {
    pub fn new(numa: u32) -> HeapLocal {
        HeapLocal { numa }
    }
}

impl Heap for HeapLocal {
    fn alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        unsafe { NonNull::new(alloc(layout)) }
    }

    unsafe fn dealloc(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            dealloc(ptr.as_ptr(), layout);
        }
    }

    fn numa_node(&self) -> u32 {
        self.numa
    }
}

pub struct HeapSvm {
    region: SvmRegion,
    numa: u32,
}

impl HeapSvm {
    pub fn new(region: SvmRegion, numa: u32) -> HeapSvm {
        HeapSvm { region, numa }
    }

    pub fn region(&self) -> &SvmRegion {
        &self.region
    }
}

impl Heap for HeapSvm {
    fn alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        let off = self.region.alloc(layout.size(), layout.align());
        if off == u64::MAX {
            return None;
        }
        let ptr = unsafe { self.region.base().add(off as usize) };
        NonNull::new(ptr)
    }

    unsafe fn dealloc(&self, ptr: NonNull<u8>, layout: Layout) {
        let base = self.region.base();
        let off = (ptr.as_ptr() as usize - base as usize) as u64;
        self.region.free(off, layout.size());
    }

    fn numa_node(&self) -> u32 {
        self.numa
    }
}

pub struct HeapRegistry {
    heaps: Vec<Arc<dyn Heap>>,
}

impl HeapRegistry {
    pub fn new() -> HeapRegistry {
        HeapRegistry { heaps: Vec::new() }
    }

    pub fn register(&mut self, heap: Arc<dyn Heap>) -> &mut Self {
        let n = heap.numa_node() as usize;
        if self.heaps.len() <= n {
            self.heaps.resize(n + 1, Arc::new(HeapLocal::new(0)));
        }
        self.heaps[n] = heap;
        self
    }

    pub fn for_numa(&self, numa: u32) -> Option<Arc<dyn Heap>> {
        self.heaps.get(numa as usize).cloned()
    }
}

impl Default for HeapRegistry {
    fn default() -> Self {
        HeapRegistry::new()
    }
}
