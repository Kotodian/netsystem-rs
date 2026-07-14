//! Local main-heap allocator (mimalloc). ZST so public `Vec<T>` stays three words.

use std::alloc::{GlobalAlloc, Layout};
use std::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator};

static MIMALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Hammer's process-local main heap backend.
///
/// Public collections default to this allocator. It must remain a ZST so
/// `Vec<T>` / `Box<T>` stay the same size as the Rust std shapes.
#[derive(Copy, Clone, Debug, Default)]
pub struct MainAllocator;

unsafe impl Allocator for MainAllocator {
    #[inline]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
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
        if layout.size() != 0 {
            unsafe { GlobalAlloc::dealloc(&MIMALLOC, ptr.as_ptr(), layout) };
        }
    }
}
