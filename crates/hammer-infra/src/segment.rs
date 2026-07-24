use std::io;
use std::os::fd::RawFd;

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
    pub fn shared(name: &str, size: usize) -> Result<Self, io::Error> {
        let name = std::ffi::CString::new(if cfg!(target_os = "linux") {
            name.to_owned()
        } else {
            format!("/{name}")
        })
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains nul"))?;

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
        if unsafe { libc::ftruncate(fd, size as libc::off_t) } != 0 {
            let source = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(source);
        }
        let region =
            SvmRegion::from_created_fd_owned(fd, size).ok_or_else(io::Error::last_os_error)?;
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
    pub fn alloc(&self, bytes: usize, align: usize) -> u64 {
        self.region.alloc(bytes, align)
    }

    #[inline]
    pub fn free(&self, offset: u64, bytes: usize) {
        self.region.release_offset(offset, bytes);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_allocation_returns_aligned_offsets() {
        let segment = Segment::local(4096);
        let first = segment.alloc(128, 64);
        let second = segment.alloc(128, 64);
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
        let offset = owner.alloc(8, 8);
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
}
