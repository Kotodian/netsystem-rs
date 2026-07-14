//! VPP-style physmem shared map for buffer packet regions.
//!
//! Packet slots are carved from this mapped span. Freelist metadata stays on
//! the main heap (see Buffer Arena). This is independent of [`SvmRegion`].

use std::ffi::CString;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::align::align_up;

static PHYSMEM_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysmemError {
    CreateFailed,
    TruncateFailed,
    MapFailed,
    InvalidSize,
}

/// NUMA-aware shared mmap arena used as the Buffer Arena packet region.
pub struct PhysmemMap {
    base: *mut u8,
    size: usize,
    numa_node: u32,
    log2_page_size: u32,
    fd: RawFd,
    fd_owned: bool,
}

unsafe impl Send for PhysmemMap {}
unsafe impl Sync for PhysmemMap {}

impl PhysmemMap {
    /// Create a shared map sized for buffer packet storage.
    ///
    /// `log2_page_size` of `0` selects the OS default page size. Hugepage and
    /// IOVA page-table fidelity are best-effort; this is a semantic adaptation
    /// of VPP `vlib_physmem_shared_map_create`, not a full `clib_pmalloc` port.
    /// `name` is retained for VPP-shaped call sites; the OS object uses a short
    /// unique token because macOS `shm_open` names are length-limited.
    pub fn create(
        _name: &str,
        size: usize,
        log2_page_size: u32,
        numa_node: u32,
    ) -> Result<Self, PhysmemError> {
        if size == 0 {
            return Err(PhysmemError::InvalidSize);
        }
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 {
            return Err(PhysmemError::CreateFailed);
        }
        let page_bytes = if log2_page_size == 0 {
            page as usize
        } else {
            1usize << log2_page_size
        };
        let total = align_up(size, page_bytes);
        let counter = PHYSMEM_COUNTER.fetch_add(1, Ordering::Relaxed);
        let label = format!("hpm{counter}");

        #[cfg(target_os = "linux")]
        let (base, fd, fd_owned) = {
            let cname = CString::new(label).map_err(|_| PhysmemError::CreateFailed)?;
            let fd = unsafe { libc::memfd_create(cname.as_ptr(), libc::MFD_CLOEXEC) };
            if fd < 0 {
                return Err(PhysmemError::CreateFailed);
            }
            if unsafe { libc::ftruncate(fd, total as libc::off_t) } != 0 {
                unsafe { libc::close(fd) };
                return Err(PhysmemError::TruncateFailed);
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
                unsafe { libc::close(fd) };
                return Err(PhysmemError::MapFailed);
            }
            (base.cast::<u8>(), fd, true)
        };

        #[cfg(not(target_os = "linux"))]
        let (base, fd, fd_owned) = {
            let cname =
                CString::new(format!("/{label}")).map_err(|_| PhysmemError::CreateFailed)?;
            let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
            if fd < 0 {
                return Err(PhysmemError::CreateFailed);
            }
            unsafe { libc::shm_unlink(cname.as_ptr()) };
            if unsafe { libc::ftruncate(fd, total as libc::off_t) } != 0 {
                unsafe { libc::close(fd) };
                return Err(PhysmemError::TruncateFailed);
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
                unsafe { libc::close(fd) };
                return Err(PhysmemError::MapFailed);
            }
            (base.cast::<u8>(), fd, true)
        };

        let log2 = if log2_page_size == 0 {
            page_bytes.trailing_zeros()
        } else {
            log2_page_size
        };

        Ok(Self {
            base,
            size: total,
            numa_node,
            log2_page_size: log2,
            fd,
            fd_owned,
        })
    }

    #[inline]
    pub fn base(&self) -> *mut u8 {
        self.base
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn numa_node(&self) -> u32 {
        self.numa_node
    }

    #[inline]
    pub fn log2_page_size(&self) -> u32 {
        self.log2_page_size
    }

    #[inline]
    pub fn fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for PhysmemMap {
    fn drop(&mut self) {
        if !self.base.is_null() && self.size != 0 {
            unsafe {
                libc::munmap(self.base.cast(), self.size);
            }
        }
        if self.fd_owned && self.fd >= 0 {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}
