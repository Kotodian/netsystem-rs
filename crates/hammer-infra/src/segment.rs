use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::align::align_up;
use crate::svm_region::SvmRegion;

pub trait Segment: Send + Sync + Clone + 'static {
    fn base(&self) -> *mut u8;
    fn alloc(&self, bytes: usize, align: usize) -> u64;
    fn free(&self, offset: u64, bytes: usize);
    fn fd(&self) -> Option<RawFd>;
}

pub struct Local {
    inner: Arc<LocalInner>,
}

struct LocalInner {
    buf: Box<[u8]>,
    bump: AtomicU64,
    base_offset: u64,
}

impl Local {
    /// Create a heap-backed segment with at least `size` usable bytes.
    /// The base address is aligned to 64 bytes (cache line).
    pub fn new(size: usize) -> Self {
        let extra = 64usize;
        let total = size
            .checked_add(extra)
            .expect("Local segment size overflow");
        let mut buf = vec![0u8; total].into_boxed_slice();
        let base_raw = buf.as_mut_ptr() as usize;
        let base_aligned = align_up(base_raw, 64);
        let base_offset = (base_aligned - base_raw) as u64;
        Self {
            inner: Arc::new(LocalInner {
                buf,
                bump: AtomicU64::new(0),
                base_offset,
            }),
        }
    }
}

impl Default for Local {
    fn default() -> Self {
        Self::new(65536)
    }
}

impl Clone for Local {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Segment for Local {
    fn base(&self) -> *mut u8 {
        unsafe { self.inner.buf.as_ptr().add(self.inner.base_offset as usize) as *mut u8 }
    }

    fn alloc(&self, bytes: usize, align: usize) -> u64 {
        let total = self.inner.buf.len();
        let bo = self.inner.base_offset;
        loop {
            let current = self.inner.bump.load(Ordering::Relaxed);
            let aligned = align_up(current as usize, align) as u64;
            let next = aligned + bytes as u64;
            if bo + next > total as u64 {
                panic!(
                    "Local segment exhausted: requested {bytes} at {aligned}, size {}",
                    total - bo as usize
                );
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

    fn free(&self, _: u64, _: usize) {
        // Local uses bump allocator; free is a no-op.
    }

    fn fd(&self) -> Option<RawFd> {
        None
    }
}

unsafe impl Send for Local {}
unsafe impl Sync for Local {}

static SVM_DEFAULT_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct Svm {
    region: SvmRegion,
}

impl Svm {
    #[cfg(target_os = "linux")]
    pub fn create(name: &str, size: usize) -> Result<Self, io::Error> {
        let c_name = std::ffi::CString::new(name)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains nul"))?;
        let fd = unsafe { libc::syscall(libc::SYS_memfd_create, c_name.as_ptr(), 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = fd as RawFd;
        let ret = unsafe { libc::ftruncate(fd, size as libc::off_t) };
        if ret != 0 {
            unsafe {
                libc::close(fd);
            }
            return Err(io::Error::last_os_error());
        }
        let region =
            SvmRegion::from_fd_owned(fd, size, true).ok_or_else(|| io::Error::last_os_error())?;
        Ok(Self { region })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn create(name: &str, size: usize) -> Result<Self, io::Error> {
        let c_name = std::ffi::CString::new(format!("/{name}"))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains nul"))?;
        let fd = unsafe { libc::shm_open(c_name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe {
            libc::shm_unlink(c_name.as_ptr());
        }
        let ret = unsafe { libc::ftruncate(fd, size as libc::off_t) };
        if ret != 0 {
            unsafe {
                libc::close(fd);
            }
            return Err(io::Error::last_os_error());
        }
        let region =
            SvmRegion::from_fd_owned(fd, size, true).ok_or_else(|| io::Error::last_os_error())?;
        Ok(Self { region })
    }

    pub fn from_fd(fd: RawFd, size: usize) -> Result<Self, io::Error> {
        SvmRegion::from_fd(fd, size)
            .map(|region| Self { region })
            .ok_or_else(io::Error::last_os_error)
    }
}

impl Default for Svm {
    fn default() -> Self {
        let pid = std::process::id();
        let counter = SVM_DEFAULT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("hammer-{pid}-{counter}");
        let size = 256 * 1024 * 1024;
        Self::create(&name, size)
            .unwrap_or_else(|e| panic!("Svm::default: failed to create shared memory segment: {e}"))
    }
}

impl Svm {
    pub fn size(&self) -> usize {
        self.region.size()
    }
}

impl Clone for Svm {
    fn clone(&self) -> Self {
        Self {
            region: self.region.clone(),
        }
    }
}

impl Segment for Svm {
    fn base(&self) -> *mut u8 {
        self.region.base()
    }

    fn alloc(&self, bytes: usize, align: usize) -> u64 {
        self.region.alloc(bytes, align)
    }

    fn free(&self, offset: u64, bytes: usize) {
        self.region.free(offset, bytes);
    }

    fn fd(&self) -> Option<RawFd> {
        Some(self.region.fd())
    }
}

unsafe impl Send for Svm {}
unsafe impl Sync for Svm {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_alloc_returns_aligned_offsets() {
        let seg = Local::new(4096);
        let off1 = seg.alloc(128, 64);
        assert_eq!(off1, 0);
        assert_eq!(off1 % 64, 0);
        let off2 = seg.alloc(128, 64);
        assert_eq!(off2, 128);
        assert_eq!(off2 % 64, 0);
    }

    #[test]
    fn local_base_writable() {
        let seg = Local::new(256);
        let off = seg.alloc(8, 1);
        unsafe {
            std::ptr::write_bytes(seg.base().add(off as usize), 0xAB, 8);
            assert_eq!(*seg.base().add(off as usize), 0xAB);
        }
    }

    #[test]
    fn local_fd_is_none() {
        let seg = Local::new(64);
        assert!(seg.fd().is_none());
    }

    #[test]
    fn local_clone_shares_base() {
        let seg = Local::new(256);
        let clone = seg.clone();
        assert_eq!(seg.base(), clone.base());
    }

    #[test]
    fn svm_create_and_write_read() {
        let seg = Svm::create("hammer_test_rw", 4096).expect("create");
        let off = seg.alloc(64, 8);
        unsafe {
            std::ptr::write_bytes(seg.base().add(off as usize), 0xCD, 64);
            assert_eq!(*seg.base().add(off as usize), 0xCD);
        }
        assert!(seg.fd().is_some());
    }

    #[test]
    fn svm_alloc_aligned() {
        let seg = Svm::create("hammer_test_align", 4096).expect("create");
        let off = seg.alloc(128, 64);
        assert_eq!(off % 64, 0);
    }

    #[test]
    fn svm_free_then_reuse() {
        let seg = Svm::create("hammer_test_free", 4096).expect("create");
        let off1 = seg.alloc(4096, 64);
        seg.free(off1, 4096);
        let off2 = seg.alloc(4096, 64);
        assert_eq!(off1, off2);
    }

    #[test]
    fn svm_default_creates_valid_segment() {
        let seg = Svm::default();
        assert!(seg.size() > 0);
        assert!(seg.fd().is_some());
        let off = seg.alloc(128, 64);
        assert_eq!(off % 64, 0);
        assert!((off as usize) < seg.size());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn svm_cross_process_via_fd() {
        let seg = Svm::create("hammer_test_fork", 4096).expect("create");
        let off = seg.alloc(8, 1);
        unsafe {
            std::ptr::write_bytes(seg.base().add(off as usize), 0x42, 8);
        }
        let fd = seg.fd().unwrap();
        let pid = unsafe { libc::fork() };
        if pid == 0 {
            let child_seg = Svm::from_fd(fd, 4096).expect("attach");
            let val = unsafe { *child_seg.base().add(off as usize) };
            std::process::exit(if val == 0x42 { 0 } else { 1 });
        }
        let mut status = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
        }
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
    }

    #[test]
    fn svm_create_closes_its_fd_on_drop() {
        let svm = Svm::default();
        let fd = svm.fd().expect("Svm exposes its fd");
        drop(svm);
        unsafe {
            let r = libc::fcntl(fd, libc::F_GETFL);
            assert!(r < 0, "fd must be closed after Svm drop");
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::EBADF)
            );
        }
    }
}
