//! Allocators that raise allocation alignment above the element natural align.

use std::alloc::{GlobalAlloc, Layout};
use std::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator};

use crate::main_alloc::MIMALLOC;

/// Raise `layout.align()` to at least `ALIGN` before forwarding to mimalloc.
#[derive(Copy, Clone, Debug, Default)]
pub struct AlignTo<const ALIGN: usize>;

fn bump_align(layout: Layout, min_align: usize) -> Result<Layout, AllocError> {
    debug_assert!(min_align.is_power_of_two() && min_align != 0);
    let align = layout.align().max(min_align);
    Layout::from_size_align(layout.size(), align).map_err(|_| AllocError)
}

unsafe impl<const ALIGN: usize> Allocator for AlignTo<ALIGN> {
    #[inline]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let layout = bump_align(layout, ALIGN)?;
        match layout.size() {
            0 => Ok(NonNull::slice_from_raw_parts(NonNull::dangling(), 0)),
            size => {
                let raw = unsafe { GlobalAlloc::alloc(&MIMALLOC, layout) };
                let ptr = NonNull::new(raw).ok_or(AllocError)?;
                Ok(NonNull::slice_from_raw_parts(ptr, size))
            }
        }
    }

    #[inline]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let layout = bump_align(layout, ALIGN)?;
        match layout.size() {
            0 => Ok(NonNull::slice_from_raw_parts(NonNull::dangling(), 0)),
            size => {
                let raw = unsafe { GlobalAlloc::alloc_zeroed(&MIMALLOC, layout) };
                let ptr = NonNull::new(raw).ok_or(AllocError)?;
                Ok(NonNull::slice_from_raw_parts(ptr, size))
            }
        }
    }

    #[inline]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        let layout = match bump_align(layout, ALIGN) {
            Ok(layout) => layout,
            Err(_) => return,
        };
        if layout.size() != 0 {
            unsafe { GlobalAlloc::dealloc(&MIMALLOC, ptr.as_ptr(), layout) };
        }
    }
}
