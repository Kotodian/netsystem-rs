use std::alloc::{GlobalAlloc, Layout};
use std::ffi::CString;
use std::fmt;
use std::io;
use std::mem;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::align::align_up;
use crate::page_size;

use talc::source::Manual;

/// Mapping plus bump allocator owned by one [`Segment`].
pub(crate) struct SegmentMapping {
    inner: Arc<SegmentMappingInner>,
}

/// Memory domain backing application FIFOs and message queues.
///
/// Local and cross-process applications use the same Segment semantics. The
/// application attach path decides whether the mapping may be exported; users
/// of FIFO and message-queue storage do not carry that choice in their types.
#[derive(Clone)]
pub struct Segment {
    mapping: SegmentMapping,
    shareable: bool,
}

impl Segment {
    /// Create a process-local Segment.
    pub fn local(size: usize) -> Self {
        Self {
            mapping: SegmentMapping::with_size(size),
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
        let mapping = match reserved_prefix {
            None => SegmentMapping::from_created_fd_owned(fd, total),
            Some(prefix) => SegmentMapping::from_created_fd_owned_with_prefix(fd, total, prefix),
        }?;
        Ok(Self {
            mapping,
            shareable: true,
        })
    }

    /// Attach to a shared mapping through a borrowed descriptor.
    ///
    /// The Segment owns a close-on-exec duplicate, so the caller may close the
    /// supplied descriptor immediately after this operation returns.
    pub fn from_fd(fd: RawFd, size: usize) -> Result<Self, io::Error> {
        let mapping = SegmentMapping::from_fd(fd, size).ok_or_else(io::Error::last_os_error)?;
        Ok(Self {
            mapping,
            shareable: true,
        })
    }

    #[inline]
    pub fn base(&self) -> *mut u8 {
        self.mapping.base()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.mapping.size()
    }

    #[inline]
    pub fn alloc(&self, bytes: usize, align: usize) -> Option<u64> {
        self.mapping.alloc(bytes, align)
    }

    #[inline]
    pub fn free(&self, offset: u64, bytes: usize) {
        self.mapping.release_offset(offset, bytes);
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
        self.shareable.then(|| self.mapping.fd())
    }

    pub fn shared_default() -> Self {
        Self {
            mapping: SegmentMapping::default(),
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

/// Shared mapping plus bump allocator behind [`Segment`].
///
/// This is the legacy mapping/allocator pairing that [`Segment`] still exposes;
/// it is registered for deletion in ADR-0011 section 12.10 and stays here,
/// beside its only owner, until the region and FIFO segment owners replace it.
/// A future region allocator is offset based and shareable, so an attached
/// process never re-initializes the creator's allocator.
const SVM_OFFSET_ALIGN: usize = 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct OffsetAllocHeader {
    raw_offset: u64,
    layout_align: usize,
}

struct SegmentMappingInner {
    base: *mut u8,
    size: usize,
    fd: RawFd,
    allocator: Option<talc::TalcLock<spinning_top::RawSpinlock, talc::source::Manual>>,
}

unsafe impl Send for SegmentMappingInner {}
unsafe impl Sync for SegmentMappingInner {}

impl Clone for SegmentMapping {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl SegmentMapping {
    pub fn with_size(size: usize) -> SegmentMapping {
        Self::with_size_and_prefix(size, 0)
            .expect("SegmentMapping::with_size: shared mapping creation failed")
    }

    /// Creates a shared mapping whose first `reserved_prefix` bytes stay
    /// outside the allocator's ownership.
    ///
    /// The mapping and file sizes are rounded up to a page multiple, and the
    /// reserved prefix must itself be page-aligned and smaller than the mapped
    /// size. Talc claims only `[base + reserved_prefix, end)`, so the prefix
    /// can host a fixed shared header while allocation offsets remain relative
    /// to the mapping base.
    pub fn with_size_and_prefix(size: usize, reserved_prefix: usize) -> io::Result<SegmentMapping> {
        let page = page_size()?;
        let total = align_up(size, page);
        let reserved = validate_prefix(reserved_prefix, total, page)?;
        let fd = create_region_fd(total)?;
        let base = Self::map_shared(fd, total)?;
        Self::claim_region(base, total, reserved, fd)
            .ok_or_else(|| io::Error::other("SegmentMapping: Talc failed to claim mapped memory"))
    }

    /// Maps `total` bytes of `fd` as one shared mapping, closing the descriptor
    /// on failure.
    fn map_shared(fd: RawFd, total: usize) -> io::Result<*mut u8> {
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            let error = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(error);
        }
        Ok(base.cast::<u8>())
    }

    pub fn from_fd(fd: RawFd, size: usize) -> Option<SegmentMapping> {
        // SAFETY: F_DUPFD_CLOEXEC duplicates the borrowed live descriptor and
        // returns a fresh descriptor owned by the attached mapping.
        let owned_fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        if owned_fd < 0 {
            return None;
        }
        Self::from_fd_owned(owned_fd, size)
    }

    pub(crate) fn from_fd_owned(fd: RawFd, size: usize) -> Option<SegmentMapping> {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(
            page > 0,
            "sysconf(_SC_PAGESIZE) must return a positive page size"
        );
        let total = align_up(size, page as usize);
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            unsafe { libc::close(fd) };
            return None;
        }

        Some(SegmentMapping {
            inner: Arc::new(SegmentMappingInner {
                base: base.cast::<u8>(),
                size: total,
                fd,
                allocator: None,
            }),
        })
    }

    pub(crate) fn from_created_fd_owned(fd: RawFd, size: usize) -> io::Result<SegmentMapping> {
        Self::from_created_fd_owned_with_prefix(fd, size, 0)
    }

    /// Claims an existing created descriptor as a shared, page-rounded
    /// mapping with a page-aligned `reserved_prefix` kept outside the
    /// allocator's ownership, taking ownership of `fd` and closing it on
    /// any failure.
    pub(crate) fn from_created_fd_owned_with_prefix(
        fd: RawFd,
        size: usize,
        reserved_prefix: usize,
    ) -> io::Result<SegmentMapping> {
        let page = page_size().map_err(|error| {
            unsafe { libc::close(fd) };
            error
        })?;
        let total = align_up(size, page);
        let reserved = validate_prefix(reserved_prefix, total, page).map_err(|error| {
            unsafe { libc::close(fd) };
            error
        })?;
        let base = Self::map_shared(fd, total)?;
        Self::claim_region(base, total, reserved, fd)
            .ok_or_else(|| io::Error::other("SegmentMapping: Talc failed to claim mapped memory"))
    }

    /// Claims the allocator over `[base + reserved, end)`. On failure, unmaps
    /// the mapping and closes `fd`, which this function always takes ownership
    /// of.
    fn claim_region(
        base: *mut u8,
        total: usize,
        reserved: usize,
        fd: RawFd,
    ) -> Option<SegmentMapping> {
        // SAFETY: `reserved` is a validated page multiple smaller than
        // `total`, so the offset stays inside the mapping and keeps the
        // claimed span's required alignment.
        let allocator = match Self::claim_allocator(unsafe { base.add(reserved) }, total - reserved)
        {
            Some(allocator) => Some(allocator),
            None => {
                unsafe {
                    libc::munmap(base.cast::<libc::c_void>(), total);
                    libc::close(fd);
                }
                return None;
            }
        };

        Some(SegmentMapping {
            inner: Arc::new(SegmentMappingInner {
                base,
                size: total,
                fd,
                allocator,
            }),
        })
    }

    #[inline]
    pub fn base(&self) -> *mut u8 {
        self.inner.base
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.inner.size
    }

    #[inline]
    pub fn fd(&self) -> RawFd {
        self.inner.fd
    }

    #[inline]
    pub(crate) fn is_allocation_owner(&self) -> bool {
        self.inner.allocator.is_some()
    }

    pub fn alloc(&self, bytes: usize, align: usize) -> Option<u64> {
        if !self.is_allocation_owner() {
            return None;
        }
        if bytes == 0 || !align.is_power_of_two() {
            return None;
        }
        let Some((layout, layout_align)) = offset_alloc_layout(bytes, align) else {
            return None;
        };
        self.alloc_layout(layout)
            .and_then(|raw_ptr| {
                let user_ptr = user_ptr_from_raw(raw_ptr, layout_align)?;
                let raw_offset = self.ptr_to_offset(raw_ptr)?;
                unsafe {
                    write_offset_alloc_header(user_ptr, raw_offset, layout_align);
                }
                Some(user_ptr)
            })
            .and_then(|ptr| self.ptr_to_offset(ptr))
    }

    pub(crate) fn alloc_layout(&self, layout: Layout) -> Option<NonNull<u8>> {
        let allocator = self.inner.allocator.as_ref()?;
        let ptr = unsafe { GlobalAlloc::alloc(allocator, layout) };
        NonNull::new(ptr)
    }

    pub(crate) unsafe fn dealloc_layout(&self, ptr: NonNull<u8>, layout: Layout) {
        let Some(allocator) = self.inner.allocator.as_ref() else {
            panic!("attached SVM mapping does not own allocator state");
        };
        unsafe {
            GlobalAlloc::dealloc(allocator, ptr.as_ptr(), layout);
        }
    }

    pub(crate) fn release_offset(&self, offset: u64, bytes: usize) {
        if bytes == 0 || !self.is_allocation_owner() {
            return;
        }
        let Some(user_ptr) = self.offset_to_ptr(offset) else {
            return;
        };
        let header = unsafe { read_offset_alloc_header(user_ptr) };
        let Some((layout, _)) = offset_alloc_layout(bytes, header.layout_align) else {
            return;
        };
        let Some(ptr) = self.offset_to_ptr(header.raw_offset) else {
            return;
        };
        unsafe {
            self.dealloc_layout(ptr, layout);
        }
    }

    fn ptr_to_offset(&self, ptr: NonNull<u8>) -> Option<u64> {
        let base = self.inner.base as usize;
        let end = base.checked_add(self.inner.size)?;
        let ptr = ptr.as_ptr() as usize;
        if ptr < base || ptr > end {
            return None;
        }
        Some((ptr - base) as u64)
    }

    fn offset_to_ptr(&self, offset: u64) -> Option<NonNull<u8>> {
        let offset = usize::try_from(offset).ok()?;
        if offset >= self.inner.size {
            return None;
        }
        NonNull::new(unsafe { self.inner.base.add(offset) })
    }

    fn claim_allocator(
        base: *mut u8,
        total: usize,
    ) -> Option<talc::TalcLock<spinning_top::RawSpinlock, talc::source::Manual>> {
        let allocator =
            talc::TalcLock::<spinning_top::RawSpinlock, talc::source::Manual>::new(Manual);
        unsafe {
            allocator.lock().claim(base, total)?;
        }
        Some(allocator)
    }
}

impl Default for SegmentMapping {
    fn default() -> SegmentMapping {
        SegmentMapping::with_size(256 * 1024 * 1024)
    }
}

impl Drop for SegmentMappingInner {
    fn drop(&mut self) {
        unsafe {
            if !self.base.is_null() {
                libc::munmap(self.base.cast::<libc::c_void>(), self.size);
            }
            libc::close(self.fd);
        }
    }
}

static SVM_REGION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Creates a unique, unnamed shared-memory descriptor truncated to `total`
/// bytes, closing the descriptor on any failure.
#[cfg(target_os = "linux")]
fn create_region_fd(total: usize) -> io::Result<RawFd> {
    let counter = SVM_REGION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = CString::new(format!("hammer-region-{}-{counter}", std::process::id()))
        .expect("generated memfd name contains no nul");
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let result = unsafe { libc::ftruncate(fd, total as libc::off_t) };
    if result != 0 {
        let error = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(error);
    }
    Ok(fd)
}

/// Creates a unique, unnamed shared-memory descriptor truncated to `total`
/// bytes, closing the descriptor on any failure.
#[cfg(not(target_os = "linux"))]
fn create_region_fd(total: usize) -> io::Result<RawFd> {
    let counter = SVM_REGION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = CString::new(format!("/hammer-region-{}-{counter}", std::process::id()))
        .expect("generated shm name contains no nul");
    let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    unsafe { libc::shm_unlink(name.as_ptr()) };
    let result = unsafe { libc::ftruncate(fd, total as libc::off_t) };
    if result != 0 {
        let error = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(error);
    }
    Ok(fd)
}

fn validate_prefix(reserved_prefix: usize, total: usize, page: usize) -> io::Result<usize> {
    if reserved_prefix % page != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reserved prefix must be page-aligned",
        ));
    }
    if reserved_prefix >= total {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reserved prefix must be smaller than the mapping",
        ));
    }
    Ok(reserved_prefix)
}

#[inline]
fn offset_alloc_layout(bytes: usize, align: usize) -> Option<(Layout, usize)> {
    let layout_align = SVM_OFFSET_ALIGN
        .max(align)
        .max(mem::align_of::<OffsetAllocHeader>());
    let size = bytes
        .checked_add(mem::size_of::<OffsetAllocHeader>())?
        .checked_add(layout_align.checked_sub(1)?)?;
    Layout::from_size_align(size, layout_align)
        .ok()
        .map(|layout| (layout, layout_align))
}

#[inline]
fn user_ptr_from_raw(raw_ptr: NonNull<u8>, layout_align: usize) -> Option<NonNull<u8>> {
    let base = raw_ptr.as_ptr() as usize;
    let user = align_up(
        base.checked_add(mem::size_of::<OffsetAllocHeader>())?,
        layout_align,
    );
    NonNull::new(user as *mut u8)
}

#[inline]
unsafe fn write_offset_alloc_header(user_ptr: NonNull<u8>, raw_offset: u64, layout_align: usize) {
    let header_ptr = unsafe {
        user_ptr
            .as_ptr()
            .sub(mem::size_of::<OffsetAllocHeader>())
            .cast::<OffsetAllocHeader>()
    };
    unsafe {
        header_ptr.write(OffsetAllocHeader {
            raw_offset,
            layout_align,
        });
    }
}

#[inline]
unsafe fn read_offset_alloc_header(user_ptr: NonNull<u8>) -> OffsetAllocHeader {
    let header_ptr = unsafe {
        user_ptr
            .as_ptr()
            .sub(mem::size_of::<OffsetAllocHeader>())
            .cast::<OffsetAllocHeader>()
    };
    unsafe { header_ptr.read() }
}
