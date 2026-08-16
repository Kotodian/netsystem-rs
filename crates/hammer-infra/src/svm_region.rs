//! Shared-memory region backed by `memfd_create`/`shm_open` and optionally
//! claimed as an owner allocator with Talc.

use std::alloc::{GlobalAlloc, Layout};
use std::ffi::CString;
use std::io;
use std::mem;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use talc::source::Manual;

use crate::align::align_up;

const SVM_OFFSET_ALIGN: usize = 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct OffsetAllocHeader {
    raw_offset: u64,
    layout_align: usize,
}

pub struct SvmRegion {
    inner: Arc<SvmRegionInner>,
}

struct SvmRegionInner {
    base: *mut u8,
    size: usize,
    fd: RawFd,
    allocator: Option<talc::TalcLock<spinning_top::RawSpinlock, talc::source::Manual>>,
}

unsafe impl Send for SvmRegionInner {}
unsafe impl Sync for SvmRegionInner {}

impl Clone for SvmRegion {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl SvmRegion {
    pub fn with_size(size: usize) -> SvmRegion {
        Self::with_size_and_prefix(size, 0)
            .expect("SvmRegion::with_size: shared mapping creation failed")
    }

    /// Creates a shared mapping whose first `reserved_prefix` bytes stay
    /// outside the allocator's ownership.
    ///
    /// The mapping and file sizes are rounded up to a page multiple, and the
    /// reserved prefix must itself be page-aligned and smaller than the mapped
    /// size. Talc claims only `[base + reserved_prefix, end)`, so the prefix
    /// can host a fixed shared header while allocation offsets remain relative
    /// to the mapping base.
    pub fn with_size_and_prefix(size: usize, reserved_prefix: usize) -> io::Result<SvmRegion> {
        let page = page_size()?;
        let total = align_up(size, page);
        let reserved = validate_prefix(reserved_prefix, total, page)?;
        let fd = create_region_fd(total)?;
        let base = Self::map_shared(fd, total)?;
        Self::claim_region(base, total, reserved, fd).ok_or_else(|| {
            io::Error::other("SvmRegion: Talc failed to claim mapped memory")
        })
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

    pub fn from_fd(fd: RawFd, size: usize) -> Option<SvmRegion> {
        // SAFETY: F_DUPFD_CLOEXEC duplicates the borrowed live descriptor and
        // returns a fresh descriptor owned by the attached mapping.
        let owned_fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        if owned_fd < 0 {
            return None;
        }
        Self::from_fd_owned(owned_fd, size)
    }

    pub(crate) fn from_fd_owned(fd: RawFd, size: usize) -> Option<SvmRegion> {
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

        Some(SvmRegion {
            inner: Arc::new(SvmRegionInner {
                base: base.cast::<u8>(),
                size: total,
                fd,
                allocator: None,
            }),
        })
    }

    pub(crate) fn from_created_fd_owned(fd: RawFd, size: usize) -> Option<SvmRegion> {
        Self::from_created_fd_owned_with_prefix(fd, size, 0)
    }

    pub(crate) fn from_created_fd_owned_with_prefix(
        fd: RawFd,
        size: usize,
        reserved_prefix: usize,
    ) -> Option<SvmRegion> {
        let Some(page) = page_size().ok() else {
            unsafe { libc::close(fd) };
            return None;
        };
        let total = align_up(size, page);
        let Some(reserved) = validate_prefix(reserved_prefix, total, page).ok() else {
            unsafe { libc::close(fd) };
            return None;
        };
        let base = Self::map_shared(fd, total).ok()?;
        Self::claim_region(base, total, reserved, fd)
    }

    /// Claims the allocator over `[base + reserved, end)`. On failure, unmaps
    /// the mapping and closes `fd`, which this function always takes ownership
    /// of.
    fn claim_region(base: *mut u8, total: usize, reserved: usize, fd: RawFd) -> Option<SvmRegion> {
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

        Some(SvmRegion {
            inner: Arc::new(SvmRegionInner {
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

impl Default for SvmRegion {
    fn default() -> SvmRegion {
        SvmRegion::with_size(256 * 1024 * 1024)
    }
}

impl Drop for SvmRegionInner {
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

fn page_size() -> io::Result<usize> {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page <= 0 {
        return Err(io::Error::other(
            "sysconf(_SC_PAGESIZE) must return a positive page size",
        ));
    }
    Ok(page as usize)
}

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
