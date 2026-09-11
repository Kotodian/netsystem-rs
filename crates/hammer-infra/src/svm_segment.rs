//! Linux shared virtual-memory mapping ownership.
//!
//! `SvmSegment` owns only the OS mapping and backing descriptor. Allocation
//! metadata belongs to [`crate::svm_region`], so an attached process never
//! reconstructs the creator's allocator from process-local pointers.

use std::ffi::CString;
use std::io;
use std::mem::size_of;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::align::align_up;

const SEGMENT_MAGIC: u64 = 0x4841_4d4d_4552_5356;
const SEGMENT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentBackend {
    Private,
    Shm,
    Memfd,
}

#[derive(Debug, Clone)]
pub struct SvmSegmentConfig {
    pub backend: SegmentBackend,
    pub name: Option<String>,
    pub size: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum SvmSegmentError {
    #[error("segment size {requested} is too small")]
    SizeTooSmall { requested: usize },
    #[error("segment name contains NUL")]
    InvalidName,
    #[error("segment backend is unavailable on this platform")]
    BackendUnavailable,
    #[error("segment system call failed during {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("segment header is not ready")]
    NotReady,
    #[error("segment header version {version} is unsupported")]
    UnsupportedVersion { version: u32 },
    #[error("segment header magic is invalid")]
    InvalidMagic,
    #[error("segment header size {declared} does not match mapping size {mapped}")]
    SizeMismatch { declared: u64, mapped: usize },
    #[error("segment offset {offset} with length {length} is outside {size} bytes")]
    OutOfBounds {
        offset: u64,
        length: usize,
        size: usize,
    },
    #[error("segment offset {offset} is not aligned to {alignment}")]
    Misaligned { offset: u64, alignment: usize },
}

#[repr(C, align(64))]
struct SegmentHeader {
    magic: u64,
    version: u32,
    _reserved: u32,
    size: u64,
    ready: AtomicU32,
}

pub struct SvmSegment {
    base: *mut u8,
    size: usize,
    fd: Option<RawFd>,
    backend: SegmentBackend,
    creator: bool,
}

unsafe impl Send for SvmSegment {}
unsafe impl Sync for SvmSegment {}

impl SvmSegment {
    pub fn create(config: &SvmSegmentConfig) -> Result<Self, SvmSegmentError> {
        match config.backend {
            SegmentBackend::Private => Self::private(config.size),
            SegmentBackend::Memfd => Self::memfd(
                config.name.as_deref().ok_or(SvmSegmentError::InvalidName)?,
                config.size,
            ),
            SegmentBackend::Shm => Self::shm(
                config.name.as_deref().ok_or(SvmSegmentError::InvalidName)?,
                config.size,
            ),
        }
    }

    pub fn private(size: usize) -> Result<Self, SvmSegmentError> {
        Self::map_private(size)
    }

    pub fn memfd(name: &str, size: usize) -> Result<Self, SvmSegmentError> {
        Self::create_shared(SegmentBackend::Memfd, name, size)
    }

    pub fn shm(name: &str, size: usize) -> Result<Self, SvmSegmentError> {
        Self::create_shared(SegmentBackend::Shm, name, size)
    }

    pub fn attach(fd: RawFd, size: usize) -> Result<Self, SvmSegmentError> {
        let page = page_size()?;
        let mapped_size = align_up(size, page);
        if mapped_size < size_of::<SegmentHeader>() {
            return Err(SvmSegmentError::SizeTooSmall { requested: size });
        }
        let owned_fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        if owned_fd < 0 {
            return Err(io_error("duplicate segment descriptor"));
        }
        let base = match map(owned_fd, mapped_size) {
            Ok(base) => base,
            Err(error) => {
                unsafe { libc::close(owned_fd) };
                return Err(error);
            }
        };
        let segment = Self {
            base,
            size: mapped_size,
            fd: Some(owned_fd),
            backend: SegmentBackend::Memfd,
            creator: false,
        };
        if let Err(error) = segment.validate_header() {
            return Err(error);
        }
        Ok(segment)
    }

    pub fn backend(&self) -> SegmentBackend {
        self.backend
    }

    pub fn base(&self) -> *mut u8 {
        self.base
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn fd(&self) -> Option<RawFd> {
        self.fd
    }

    pub fn is_creator(&self) -> bool {
        self.creator
    }

    pub fn is_ready(&self) -> bool {
        let header = unsafe { &*(self.base.cast::<SegmentHeader>()) };
        header.ready.load(Ordering::Acquire) != 0
    }

    pub fn publish_ready(&self) {
        assert!(self.creator, "only the segment creator may publish ready");
        let header = unsafe { &*(self.base.cast::<SegmentHeader>()) };
        header.ready.store(1, Ordering::Release);
    }

    pub fn offset_ptr(
        &self,
        offset: u64,
        length: usize,
        alignment: usize,
    ) -> Result<*mut u8, SvmSegmentError> {
        if alignment == 0 || !alignment.is_power_of_two() || (offset as usize) % alignment != 0 {
            return Err(SvmSegmentError::Misaligned { offset, alignment });
        }
        let end = offset
            .checked_add(length as u64)
            .ok_or(SvmSegmentError::OutOfBounds {
                offset,
                length,
                size: self.size,
            })?;
        if end > self.size as u64 {
            return Err(SvmSegmentError::OutOfBounds {
                offset,
                length,
                size: self.size,
            });
        }
        Ok(unsafe { self.base.add(offset as usize) })
    }

    fn map_private(size: usize) -> Result<Self, SvmSegmentError> {
        let page = page_size()?;
        let mapped_size = align_up(size, page);
        if mapped_size < size_of::<SegmentHeader>() {
            return Err(SvmSegmentError::SizeTooSmall { requested: size });
        }
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapped_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io_error("private mmap"));
        }
        let segment = Self {
            base: base.cast(),
            size: mapped_size,
            fd: None,
            backend: SegmentBackend::Private,
            creator: true,
        };
        segment.initialize_header();
        Ok(segment)
    }

    fn create_shared(
        backend: SegmentBackend,
        name: &str,
        size: usize,
    ) -> Result<Self, SvmSegmentError> {
        let page = page_size()?;
        let mapped_size = align_up(size, page);
        if mapped_size < size_of::<SegmentHeader>() {
            return Err(SvmSegmentError::SizeTooSmall { requested: size });
        }
        let name = CString::new(name).map_err(|_| SvmSegmentError::InvalidName)?;
        let fd = match backend {
            SegmentBackend::Memfd => {
                #[cfg(target_os = "linux")]
                {
                    let fd = unsafe {
                        libc::syscall(libc::SYS_memfd_create, name.as_ptr(), libc::MFD_CLOEXEC)
                    };
                    if fd < 0 {
                        return Err(io_error("memfd_create"));
                    }
                    fd as RawFd
                }
                #[cfg(not(target_os = "linux"))]
                {
                    return Err(SvmSegmentError::BackendUnavailable);
                }
            }
            SegmentBackend::Shm => {
                #[cfg(unix)]
                {
                    let fd = unsafe {
                        libc::shm_open(
                            name.as_ptr(),
                            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                            0o600,
                        )
                    };
                    if fd < 0 {
                        return Err(io_error("shm_open"));
                    }
                    unsafe { libc::shm_unlink(name.as_ptr()) };
                    fd
                }
                #[cfg(not(unix))]
                {
                    return Err(SvmSegmentError::BackendUnavailable);
                }
            }
            SegmentBackend::Private => return Self::private(size),
        };
        if unsafe { libc::ftruncate(fd, mapped_size as libc::off_t) } != 0 {
            let error = io_error("ftruncate");
            unsafe { libc::close(fd) };
            return Err(error);
        }
        let base = match map(fd, mapped_size) {
            Ok(base) => base,
            Err(error) => {
                unsafe { libc::close(fd) };
                return Err(error);
            }
        };
        let segment = Self {
            base,
            size: mapped_size,
            fd: Some(fd),
            backend,
            creator: true,
        };
        segment.initialize_header();
        Ok(segment)
    }

    fn initialize_header(&self) {
        unsafe {
            std::ptr::write(
                self.base.cast::<SegmentHeader>(),
                SegmentHeader {
                    magic: SEGMENT_MAGIC,
                    version: SEGMENT_VERSION,
                    _reserved: 0,
                    size: self.size as u64,
                    ready: AtomicU32::new(0),
                },
            );
        }
    }

    fn validate_header(&self) -> Result<(), SvmSegmentError> {
        let header = unsafe { &*(self.base.cast::<SegmentHeader>()) };
        if header.magic != SEGMENT_MAGIC {
            return Err(SvmSegmentError::InvalidMagic);
        }
        if header.version != SEGMENT_VERSION {
            return Err(SvmSegmentError::UnsupportedVersion {
                version: header.version,
            });
        }
        if header.size != self.size as u64 {
            return Err(SvmSegmentError::SizeMismatch {
                declared: header.size,
                mapped: self.size,
            });
        }
        if !self.is_ready() {
            return Err(SvmSegmentError::NotReady);
        }
        Ok(())
    }
}

impl Drop for SvmSegment {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base.cast(), self.size);
            if let Some(fd) = self.fd {
                libc::close(fd);
            }
        }
    }
}

fn page_size() -> Result<usize, SvmSegmentError> {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page <= 0 {
        return Err(io_error("page size"));
    }
    Ok(page as usize)
}

fn map(fd: RawFd, size: usize) -> Result<*mut u8, SvmSegmentError> {
    let base = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if base == libc::MAP_FAILED {
        return Err(io_error("shared mmap"));
    }
    Ok(base.cast())
}

fn io_error(operation: &'static str) -> SvmSegmentError {
    SvmSegmentError::Io {
        operation,
        source: io::Error::last_os_error(),
    }
}
