//! Shared-memory region backed by `memfd_create`/`shm_open` and optionally
//! claimed as an owner allocator with Talc.

use std::alloc::{GlobalAlloc, Layout};
use std::ffi::CString;
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
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(
            page > 0,
            "sysconf(_SC_PAGESIZE) must return a positive page size"
        );
        let total = align_up(size, page as usize);
        let counter = SVM_REGION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();

        #[cfg(target_os = "linux")]
        let (base, fd) = {
            let name = CString::new(format!("hammer-region-{pid}-{counter}"))
                .expect("generated memfd name contains no nul");
            let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
            if fd < 0 {
                panic!(
                    "SvmRegion::with_size: memfd_create failed: {}",
                    std::io::Error::last_os_error()
                );
            }

            let result = unsafe { libc::ftruncate(fd, total as libc::off_t) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                panic!("SvmRegion::with_size: ftruncate failed: {error}");
            }

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
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                panic!("SvmRegion::with_size: mmap failed: {error}");
            }

            (base.cast::<u8>(), fd)
        };

        #[cfg(not(target_os = "linux"))]
        let (base, fd) = {
            let name = CString::new(format!("/hammer-region-{pid}-{counter}"))
                .expect("generated shm name contains no nul");
            let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
            if fd < 0 {
                panic!(
                    "SvmRegion::with_size: shm_open failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            unsafe { libc::shm_unlink(name.as_ptr()) };

            let result = unsafe { libc::ftruncate(fd, total as libc::off_t) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                panic!("SvmRegion::with_size: ftruncate failed: {error}");
            }

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
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                panic!("SvmRegion::with_size: mmap failed: {error}");
            }

            (base.cast::<u8>(), fd)
        };

        let allocator = match Self::claim_allocator(base, total) {
            Some(allocator) => Some(allocator),
            None => {
                unsafe {
                    libc::munmap(base.cast::<libc::c_void>(), total);
                    libc::close(fd);
                }
                panic!("SvmRegion::with_size: Talc failed to claim mapped memory");
            }
        };

        SvmRegion {
            inner: Arc::new(SvmRegionInner {
                base,
                size: total,
                fd,
                allocator,
            }),
        }
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
        let base = base.cast::<u8>();
        let allocator = match Self::claim_allocator(base, total) {
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

    pub fn alloc(&self, bytes: usize, align: usize) -> u64 {
        if !self.is_allocation_owner() {
            return u64::MAX;
        }
        if bytes == 0 || !align.is_power_of_two() {
            return u64::MAX;
        }
        let Some((layout, layout_align)) = offset_alloc_layout(bytes, align) else {
            return u64::MAX;
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
            .unwrap_or(u64::MAX)
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
