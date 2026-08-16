use std::alloc::Layout;
use std::fmt;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;

use crate::align::align_up;
use crate::svm_region::SvmRegion;

/// Memory domain backing application FIFOs and message queues.
///
/// Local and cross-process applications use the same Segment semantics. The
/// application attach path decides whether the mapping may be exported; users
/// of FIFO and message-queue storage do not carry that choice in their types.
#[derive(Clone)]
pub struct Segment {
    region: SvmRegion,
    shareable: bool,
}

impl Segment {
    /// Create a process-local Segment.
    pub fn local(size: usize) -> Self {
        Self {
            region: SvmRegion::with_size(size),
            shareable: false,
        }
    }

    /// Create a Segment whose mapping can be attached by another process.
    ///
    /// The shared mapping and its backing file are rounded up to a page
    /// multiple, so `size` is a minimum rather than an exact size.
    pub fn shared(name: &str, size: usize) -> Result<Self, io::Error> {
        Self::shared_impl(name, size, None)
    }

    /// Create a shared Segment whose first `reserved_prefix` bytes stay
    /// outside the allocator's ownership.
    ///
    /// The shared mapping and its backing file are rounded up to a page
    /// multiple. The prefix must be page-aligned and smaller than the
    /// page-rounded mapping size. Talc claims only
    /// `[base + reserved_prefix, end)`, while allocation offsets remain
    /// relative to the mapping base, so a fixed shared header can live at
    /// offset zero without changing `alloc`/`free` semantics.
    pub fn shared_with_reserved_prefix(
        name: &str,
        size: usize,
        reserved_prefix: usize,
    ) -> Result<Self, io::Error> {
        Self::shared_impl(name, size, Some(reserved_prefix))
    }

    fn shared_impl(
        name: &str,
        size: usize,
        reserved_prefix: Option<usize>,
    ) -> Result<Self, io::Error> {
        let name = std::ffi::CString::new(if cfg!(target_os = "linux") {
            name.to_owned()
        } else {
            format!("/{name}")
        })
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains nul"))?;

        // Round the file and mapping to the same page multiple so the sizes
        // always agree and the mapping never extends past the file.
        let page = crate::page_size()?;
        let total = align_up(size, page);
        if let Some(prefix) = reserved_prefix
            && (prefix % page != 0 || prefix >= total)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "reserved prefix must be page-aligned and smaller than the mapping",
            ));
        }

        #[cfg(target_os = "linux")]
        let fd = unsafe { libc::syscall(libc::SYS_memfd_create, name.as_ptr(), libc::MFD_CLOEXEC) };
        #[cfg(not(target_os = "linux"))]
        let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = fd as RawFd;
        #[cfg(not(target_os = "linux"))]
        unsafe {
            libc::shm_unlink(name.as_ptr());
        }
        if unsafe { libc::ftruncate(fd, total as libc::off_t) } != 0 {
            let source = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(source);
        }
        let region = match reserved_prefix {
            None => SvmRegion::from_created_fd_owned(fd, total),
            Some(prefix) => SvmRegion::from_created_fd_owned_with_prefix(fd, total, prefix),
        }?;
        Ok(Self {
            region,
            shareable: true,
        })
    }

    /// Attach to a shared mapping through a borrowed descriptor.
    ///
    /// The Segment owns a close-on-exec duplicate, so the caller may close the
    /// supplied descriptor immediately after this operation returns.
    pub fn from_fd(fd: RawFd, size: usize) -> Result<Self, io::Error> {
        let region = SvmRegion::from_fd(fd, size).ok_or_else(io::Error::last_os_error)?;
        Ok(Self {
            region,
            shareable: true,
        })
    }

    #[inline]
    pub fn base(&self) -> *mut u8 {
        self.region.base()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.region.size()
    }

    #[inline]
    pub fn alloc(&self, bytes: usize, align: usize) -> Option<u64> {
        self.region.alloc(bytes, align)
    }

    #[inline]
    pub fn free(&self, offset: u64, bytes: usize) {
        self.region.release_offset(offset, bytes);
    }

    /// Allocate an owned block of `layout` from this segment's storage.
    ///
    /// The returned [`SegmentAllocation`] owns the block: dropping it returns
    /// the storage, `bytes_mut` borrows the raw bytes for safe initialization,
    /// and `into_raw_offset` hands the block to the segment's allocator for
    /// later checked reconstruction. The legacy [`Segment::alloc`] offset API
    /// remains available.
    pub fn allocate(&self, layout: Layout) -> Result<SegmentAllocation, SegmentAllocationError> {
        if layout.size() == 0 {
            return Err(SegmentAllocationError::EmptyLayout);
        }
        let offset = self
            .alloc(layout.size(), layout.align())
            .ok_or(SegmentAllocationError::Exhausted)?;
        Ok(SegmentAllocation {
            segment: self.clone(),
            offset,
            layout,
            live: true,
        })
    }

    /// Backing descriptor for cross-process attach.
    #[inline]
    pub fn shared_fd(&self) -> Option<RawFd> {
        self.shareable.then(|| self.region.fd())
    }

    pub fn shared_default() -> Self {
        Self {
            region: SvmRegion::default(),
            shareable: true,
        }
    }
}

impl Default for Segment {
    fn default() -> Self {
        Self::local(65_536)
    }
}

unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

/// An owned raw block of a [`Segment`]'s storage.
///
/// The allocation owns a clone of its [`Segment`], the exact
/// mapping-relative offset, and the exact [`Layout`] that produced it.
/// Dropping it returns exactly that block to the segment's allocator. It
/// represents raw bytes: it deallocates storage but invents no typed
/// drop behavior.
pub struct SegmentAllocation {
    segment: Segment,
    offset: u64,
    layout: Layout,
    /// Whether `drop` must return the block to the allocator. Cleared by
    /// `into_raw_offset`, which transfers that obligation to the segment's
    /// allocator while still dropping the owned `Segment` clone.
    live: bool,
}

impl SegmentAllocation {
    /// Mapping-relative offset of the block, as returned by
    /// [`Segment::alloc`].
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The exact layout this block was allocated with.
    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Number of bytes in the block.
    pub fn len(&self) -> usize {
        self.layout.size()
    }

    /// Borrow the block's raw bytes as uninitialized storage.
    ///
    /// The borrow is tied to `&mut self`, so the storage cannot be read as
    /// initialized until the caller initializes every element, for example
    /// with `MaybeUninit::write` followed by `assume_init_ref`.
    pub fn bytes_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let start = self.segment.base() as usize + self.offset as usize;
        // SAFETY: the span was checked to lie inside the mapping at
        // allocation or reconstruction time, and `&mut self` is the only
        // way to reach the storage while this allocation is owned.
        unsafe { std::slice::from_raw_parts_mut(start as *mut MaybeUninit<u8>, self.len()) }
    }

    /// Hand the block to the segment's allocator, returning its
    /// mapping-relative offset.
    ///
    /// The deallocation obligation transfers to the allocator: the block
    /// stays live until a matching [`SegmentAllocation::from_raw_offset`]
    /// reconstructs and drops it. The owned `Segment` clone is dropped here,
    /// never leaked. The caller must eventually reconstruct exactly this
    /// offset and layout, or the block leaks.
    pub fn into_raw_offset(mut self) -> u64 {
        self.live = false;
        self.offset
    }

    /// Reconstruct an allocation from a raw offset previously returned by
    /// [`SegmentAllocation::into_raw_offset`].
    ///
    /// Checks that the offset span lies inside the mapping and is aligned
    /// for `layout`, and that the layout is non-empty; violating checks
    /// returns a typed error without touching the allocator.
    ///
    /// # Safety
    ///
    /// `offset` and `layout` must uniquely identify a live block in
    /// `segment`'s allocator that no other owner refers to: the exact pair
    /// produced by `into_raw_offset` of an allocation that has not been
    /// reconstructed yet. Reconstructing a block another owner may free, or
    /// with a different layout than the original, is undefined behavior.
    pub unsafe fn from_raw_offset(
        segment: Segment,
        offset: u64,
        layout: Layout,
    ) -> Result<Self, SegmentAllocationError> {
        if layout.size() == 0 {
            return Err(SegmentAllocationError::EmptyLayout);
        }
        let Some(end) = offset.checked_add(layout.size() as u64) else {
            return Err(SegmentAllocationError::OutOfBounds);
        };
        if end > segment.size() as u64 {
            return Err(SegmentAllocationError::OutOfBounds);
        }
        let base = segment.base() as usize;
        if (base + offset as usize) % layout.align() != 0 {
            return Err(SegmentAllocationError::Misaligned);
        }
        Ok(SegmentAllocation {
            segment,
            offset,
            layout,
            live: true,
        })
    }
}

impl Drop for SegmentAllocation {
    fn drop(&mut self) {
        if self.live {
            self.segment.free(self.offset, self.layout.size());
        }
    }
}

/// Errors from owning a block of [`Segment`] storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentAllocationError {
    /// The layout has zero size; empty blocks are not tracked.
    EmptyLayout,
    /// The allocator has no room for the requested block.
    Exhausted,
    /// The offset lies outside the segment's mapping.
    OutOfBounds,
    /// The mapping base plus offset is not aligned to the layout.
    Misaligned,
}

impl fmt::Display for SegmentAllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyLayout => "segment allocation layout has zero size",
            Self::Exhausted => "segment allocator is exhausted",
            Self::OutOfBounds => "segment allocation offset is outside the mapping",
            Self::Misaligned => "segment allocation offset is misaligned for the layout",
        };
        f.write_str(message)
    }
}

impl std::error::Error for SegmentAllocationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_allocation_returns_aligned_offsets() {
        let segment = Segment::local(4096);
        let first = segment.alloc(128, 64).expect("first allocation");
        let second = segment.alloc(128, 64).expect("second allocation");
        assert_eq!(first % 64, 0);
        assert_eq!(second % 64, 0);
        assert!(segment.shared_fd().is_none());
    }

    #[test]
    fn shared_segment_exposes_backing_descriptor() {
        let segment = Segment::shared("hammer-test-segment", 4096).expect("shared segment");
        assert!(segment.shared_fd().is_some());
    }

    #[test]
    fn attached_segment_observes_shared_bytes() {
        let owner = Segment::shared("hammer-test-attach", 4096).expect("shared segment");
        let offset = owner.alloc(8, 8).expect("shared allocation");
        unsafe {
            owner
                .base()
                .add(offset as usize)
                .cast::<u64>()
                .write(0x1234);
        }
        let attached = Segment::from_fd(owner.shared_fd().expect("shared fd"), owner.size())
            .expect("attach segment");
        let value = unsafe { attached.base().add(offset as usize).cast::<u64>().read() };
        assert_eq!(value, 0x1234);
    }

    fn page_size() -> usize {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(page > 0, "sysconf(_SC_PAGESIZE) must be positive");
        page as usize
    }

    #[test]
    fn page_reserved_shared_segment_keeps_offset_zero_untouched() {
        let page = page_size();
        let segment = Segment::shared_with_reserved_prefix("hammer-test-reserved", 2 * page, page)
            .expect("page-reserved shared segment");
        unsafe { segment.base().write(0xAA) };

        let offset = segment
            .alloc(64, 64)
            .expect("allocation after the reserved page");
        assert!(
            offset as usize >= page,
            "allocation must start after the reserved page"
        );
        assert_eq!(offset % 64, 0, "allocation must be 64-byte aligned");
        unsafe { segment.base().add(offset as usize).write(0x11) };

        assert_eq!(
            unsafe { segment.base().read() },
            0xAA,
            "the reserved first page must remain untouched by allocation"
        );
        segment.free(offset, 64);
        assert_eq!(
            unsafe { segment.base().read() },
            0xAA,
            "freeing must not touch the reserved first page"
        );
    }

    #[test]
    fn reserved_shared_segment_rounds_requested_and_file_sizes_to_pages() {
        let page = page_size();
        let requested = page + 1000;
        let segment = Segment::shared_with_reserved_prefix("hammer-test-sizes", requested, page)
            .expect("page-reserved shared segment");
        let rounded = page * 2;
        assert_eq!(
            segment.size(),
            rounded,
            "mapping size must round the requested size up to a page multiple"
        );
        let fd = segment.shared_fd().expect("shared descriptor");
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
        assert_eq!(result, 0, "fstat must succeed");
        let stat = unsafe { stat.assume_init() };
        assert_eq!(
            stat.st_size as usize, rounded,
            "file size must match the page-rounded mapping size"
        );
    }

    #[test]
    fn reserved_shared_segment_exhaustion_fails_without_touching_the_prefix() {
        let page = page_size();
        // One reserved page leaves exactly one page for the allocator.
        let segment = Segment::shared_with_reserved_prefix("hammer-test-exhaust", 2 * page, page)
            .expect("page-reserved shared segment");
        unsafe { segment.base().write(0x5A) };

        let mut allocations = 0usize;
        while let Some(offset) = segment.alloc(256, 64) {
            assert!(
                offset as usize >= page,
                "every allocation must start after the reserved page"
            );
            unsafe { segment.base().add(offset as usize).write(0x11) };
            allocations += 1;
        }
        assert!(
            allocations >= 1,
            "one page of allocator space must hold at least one allocation"
        );
        assert_eq!(
            unsafe { segment.base().read() },
            0x5A,
            "allocation exhaustion must leave the reserved prefix untouched"
        );
    }

    #[test]
    fn reserved_shared_segment_rejects_unaligned_or_oversized_prefixes() {
        let page = page_size();
        let unaligned =
            Segment::shared_with_reserved_prefix("hammer-test-prefix-align", 2 * page, 100);
        assert!(
            unaligned.is_err(),
            "a non-page-aligned prefix must be rejected"
        );
        let oversized =
            Segment::shared_with_reserved_prefix("hammer-test-prefix-size", 2 * page, 3 * page);
        assert!(
            oversized.is_err(),
            "a prefix beyond the mapping must be rejected"
        );
    }
}
