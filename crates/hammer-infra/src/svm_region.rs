//! Reusable shared-memory region: memfd/shm_open mmap backing + bump allocator
//! with a LIFO best-fit free list. Owned by `Svm` segments and `HeapSvm` heaps.
use std::ffi::CString;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::align::align_up;

pub struct SvmRegion {
    inner: Arc<SvmRegionInner>,
}

struct SvmRegionInner {
    base: *mut u8,
    size: usize,
    fd: RawFd,
    bump: AtomicU64,
    free_list: Mutex<Vec<(u64, usize)>>,
    owned: bool,
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
    /// Create a new owned shared-memory region of at least `size` bytes.
    /// On Linux this uses `memfd_create`; elsewhere it uses `shm_open`.
    pub fn with_size(size: usize) -> SvmRegion {
        let page = page_size();
        let total = align_up(size, page);
        let (base, fd, owned) = unsafe { alloc_region(total) };
        SvmRegion {
            inner: Arc::new(SvmRegionInner {
                base,
                size: total,
                fd,
                bump: AtomicU64::new(0),
                free_list: Mutex::new(Vec::new()),
                owned,
            }),
        }
    }

    /// Attach to an existing shared-memory region by fd (does not own the fd).
    pub fn from_fd(fd: RawFd, size: usize) -> Option<SvmRegion> {
        let page = page_size();
        let total = align_up(size, page);
        unsafe {
            let base = libc::mmap(
                std::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            if base == libc::MAP_FAILED {
                return None;
            }
            Some(SvmRegion {
                inner: Arc::new(SvmRegionInner {
                    base: base as *mut u8,
                    size: total,
                    fd,
                    bump: AtomicU64::new(0),
                    free_list: Mutex::new(Vec::new()),
                    owned: false,
                }),
            })
        }
    }

    pub fn base(&self) -> *mut u8 {
        self.inner.base
    }

    pub fn size(&self) -> usize {
        self.inner.size
    }

    pub fn fd(&self) -> RawFd {
        self.inner.fd
    }

    /// Best-fit search of the LIFO free list; on miss, bump-allocates.
    /// Returns `u64::MAX` on OOM. The returned offset is `align`-aligned
    /// and satisfies `offset + bytes <= base + size`.
    pub fn alloc(&self, bytes: usize, align: usize) -> u64 {
        // 1. Try free list (best-fit by smallest remaining sliver).
        let mut fl = self.inner.free_list.lock().expect("svm_region free_list");
        let mut best: Option<(usize, u64)> = None;
        for (i, &(off, sz)) in fl.iter().enumerate() {
            if sz < bytes {
                continue;
            }
            let aligned = align_up(off as usize, align);
            let pad = aligned - off as usize;
            let end = match aligned.checked_add(bytes) {
                Some(v) => v,
                None => continue,
            };
            if end > off as usize + sz {
                continue;
            }
            let tail = (sz - pad - bytes) as u64;
            match best {
                None => best = Some((i, tail)),
                Some((_, t)) if tail < t => best = Some((i, tail)),
                _ => {}
            }
        }
        if let Some((i, _)) = best {
            let (off, sz) = fl.swap_remove(i);
            let aligned = align_up(off as usize, align);
            let pad = aligned - off as usize;
            if pad + bytes < sz {
                fl.push(((aligned + bytes) as u64, sz - pad - bytes));
            }
            return aligned as u64;
        }
        drop(fl);
        // 2. Bump.
        let size = self.inner.size;
        loop {
            let current = self.inner.bump.load(Ordering::Relaxed);
            let aligned = align_up(current as usize, align) as u64;
            let next = aligned + bytes as u64;
            if next > size as u64 {
                return u64::MAX;
            }
            if self
                .inner
                .bump
                .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return aligned;
            }
        }
    }

    /// Push `(offset, bytes)` to the LIFO free list. No coalescing — VPP/hammer
    /// semantics match existing `SvmInner` behavior.
    pub fn free(&self, offset: u64, bytes: usize) {
        self.inner
            .free_list
            .lock()
            .expect("svm_region free_list")
            .push((offset, bytes));
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
                libc::munmap(self.base as *mut libc::c_void, self.size);
            }
            if self.owned {
                libc::close(self.fd);
            }
        }
    }
}

fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

static SVM_REGION_COUNTER: AtomicU64 = AtomicU64::new(0);

unsafe fn alloc_region(total: usize) -> (*mut u8, RawFd, bool) {
    let counter = SVM_REGION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    #[cfg(target_os = "linux")]
    {
        let name = CString::new(format!("hammer-region-{pid}-{counter}")).unwrap();
        let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        if fd < 0 {
            panic!(
                "SvmRegion alloc_region: memfd_create failed: {}",
                std::io::Error::last_os_error()
            );
        }
        let ret = unsafe { libc::ftruncate(fd, total as libc::off_t) };
        if ret != 0 {
            let e = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            panic!("SvmRegion alloc_region: ftruncate failed: {e}");
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
            let e = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            panic!("SvmRegion alloc_region: mmap failed: {e}");
        }
        (base as *mut u8, fd, true)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let name = CString::new(format!("/hammer-region-{pid}-{counter}")).unwrap();
        let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
        if fd < 0 {
            panic!(
                "SvmRegion alloc_region: shm_open failed: {}",
                std::io::Error::last_os_error()
            );
        }
        unsafe { libc::shm_unlink(name.as_ptr()) };
        let ret = unsafe { libc::ftruncate(fd, total as libc::off_t) };
        if ret != 0 {
            let e = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            panic!("SvmRegion alloc_region: ftruncate failed: {e}");
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
            let e = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            panic!("SvmRegion alloc_region: mmap failed: {e}");
        }
        (base as *mut u8, fd, true)
    }
}
