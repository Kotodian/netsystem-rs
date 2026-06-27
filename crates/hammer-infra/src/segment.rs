use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

    fn free(&self, _offset: u64, _bytes: usize) {
        // Local uses bump allocator; free is a no-op.
    }

    fn fd(&self) -> Option<RawFd> {
        None
    }
}

unsafe impl Send for Local {}
unsafe impl Sync for Local {}

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
}
