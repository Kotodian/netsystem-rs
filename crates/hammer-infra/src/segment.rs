use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::align::align_up;

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
}

impl Local {
    pub fn new(size: usize) -> Self {
        let buf = vec![0u8; size].into_boxed_slice();
        Self {
            inner: Arc::new(LocalInner {
                buf,
                bump: AtomicU64::new(0),
            }),
        }
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
        self.inner.buf.as_ptr() as *mut u8
    }

    fn alloc(&self, bytes: usize, align: usize) -> u64 {
        let size = self.inner.buf.len();
        loop {
            let current = self.inner.bump.load(Ordering::Relaxed);
            let aligned = align_up(current as usize, align) as u64;
            let next = aligned + bytes as u64;
            if next > size as u64 {
                panic!(
                    "Local segment exhausted: requested {bytes} at {aligned}, size {size}"
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

pub struct Svm {
    inner: Arc<SvmInner>,
}

struct SvmInner {
    base: *mut u8,
    size: usize,
    fd: RawFd,
    bump: AtomicU64,
    free_list: Mutex<Vec<(u64, usize)>>,
    owned: bool,
}

impl Svm {
    #[cfg(target_os = "linux")]
    pub fn create(name: &str, size: usize) -> Result<Self, io::Error> {
        let c_name =
            std::ffi::CString::new(name).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains nul"))?;
        let fd = unsafe { libc::syscall(libc::SYS_memfd_create, c_name.as_ptr(), 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = fd as RawFd;
        let ret = unsafe { libc::ftruncate(fd, size as libc::off_t) };
        if ret != 0 {
            unsafe { libc::close(fd); }
            return Err(io::Error::last_os_error());
        }
        Self::mmap_shared(fd, size, true)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn create(name: &str, size: usize) -> Result<Self, io::Error> {
        let c_name = std::ffi::CString::new(format!("/{name}"))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains nul"))?;
        let fd = unsafe { libc::shm_open(c_name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe { libc::shm_unlink(c_name.as_ptr()); }
        let ret = unsafe { libc::ftruncate(fd, size as libc::off_t) };
        if ret != 0 {
            unsafe { libc::close(fd); }
            return Err(io::Error::last_os_error());
        }
        Self::mmap_shared(fd, size, true)
    }

    pub fn from_fd(fd: RawFd, size: usize) -> Result<Self, io::Error> {
        Self::mmap_shared(fd, size, false)
    }

    fn mmap_shared(fd: RawFd, size: usize, owned: bool) -> Result<Self, io::Error> {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            inner: Arc::new(SvmInner {
                base: ptr as *mut u8,
                size,
                fd,
                bump: AtomicU64::new(0),
                free_list: Mutex::new(Vec::new()),
                owned,
            }),
        })
    }
}

impl Clone for Svm {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Segment for Svm {
    fn base(&self) -> *mut u8 {
        self.inner.base
    }

    fn alloc(&self, bytes: usize, align: usize) -> u64 {
        let mut free_list = self.inner.free_list.lock().expect("free_list mutex");
        let best_idx = free_list
            .iter()
            .enumerate()
            .filter(|(_, (off, _))| (*off as usize) % align == 0)
            .filter(|(_, (_, sz))| *sz >= bytes)
            .min_by_key(|(_, (_, sz))| *sz)
            .map(|(idx, _)| idx);
        if let Some(idx) = best_idx {
            let (off, _) = free_list.swap_remove(idx);
            drop(free_list);
            return off;
        }
        drop(free_list);
        let size = self.inner.size;
        loop {
            let current = self.inner.bump.load(Ordering::Relaxed);
            let aligned = align_up(current as usize, align) as u64;
            let next = aligned + bytes as u64;
            if next > size as u64 {
                panic!("Svm segment exhausted");
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

    fn free(&self, offset: u64, bytes: usize) {
        self.inner
            .free_list
            .lock()
            .expect("free_list mutex")
            .push((offset, bytes));
    }

    fn fd(&self) -> Option<RawFd> {
        Some(self.inner.fd)
    }
}

impl Drop for SvmInner {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base as *mut libc::c_void, self.size);
            if self.owned {
                libc::close(self.fd);
            }
        }
    }
}

unsafe impl Send for SvmInner {}
unsafe impl Sync for SvmInner {}

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
    #[cfg(target_os = "linux")]
    fn svm_cross_process_via_fd() {
        let seg = Svm::create("hammer_test_fork", 4096).expect("create");
        let off = seg.alloc(8, 1);
        unsafe { std::ptr::write_bytes(seg.base().add(off as usize), 0x42, 8); }
        let fd = seg.fd().unwrap();
        let pid = unsafe { libc::fork() };
        if pid == 0 {
            let child_seg = Svm::from_fd(fd, 4096).expect("attach");
            let val = unsafe { *child_seg.base().add(off as usize) };
            std::process::exit(if val == 0x42 { 0 } else { 1 });
        }
        let mut status = 0;
        unsafe { libc::waitpid(pid, &mut status, 0); }
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
    }
}
