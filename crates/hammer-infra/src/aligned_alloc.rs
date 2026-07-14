//! Allocators that raise allocation alignment above the element natural align.

use std::alloc::Layout;
use std::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator};

use crate::main_alloc::{main_allocate, main_allocate_zeroed, main_deallocate};

/// Raise `layout.align()` to at least `ALIGN` before forwarding to mimalloc.
#[derive(Copy, Clone, Debug, Default)]
pub struct AlignTo<const ALIGN: usize>;

unsafe impl<const ALIGN: usize> Allocator for AlignTo<ALIGN> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        main_allocate(layout, ALIGN)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        main_allocate_zeroed(layout, ALIGN)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: forwarded from `Allocator::deallocate` with its allocation
        // provenance and layout requirements unchanged.
        unsafe { main_deallocate(ptr, layout, ALIGN) };
    }
}
