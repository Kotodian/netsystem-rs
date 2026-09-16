//! Linux shared virtual-memory mappings (`ssvm`).
//!
//! `SsvmPrivate` owns the OS mapping, its backing descriptor, the mapping's
//! [`SsvmSharedHeader`], and the generic shared heap. Region allocation metadata
//! belongs to [`crate::svm::region`], so an attached process never rebuilds the
//! creator's allocator from process-local pointers.
//!
//! Field and lifecycle semantics follow VPP `src/svm/ssvm.h` and `ssvm.c`.
//! ADR-0011 section 13 records the intentional layout divergences: the ported
//! mapping carries no VPP recursive lock or opaque pointer slots, while the
//! generic SSVM heap pointer is retained; FIFO map-only segments explicitly
//! omit that heap and publish a zero VA for relative-offset storage.

use std::ffi::CString;
use std::io;
use std::mem::size_of;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::mem::MemHeap;

const SSVM_MAGIC: u64 = 0x4841_4d4d_4552_5353;
const SSVM_VERSION: u32 = 1;
const SSVM_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Longest shared segment name stored inline in [`SsvmSharedHeader`].
pub const SSVM_NAME_MAX: usize = 64;

/// Shared header at offset 0 of every segment mapping.
///
/// Corresponds to the used VPP `ssvm_shared_header_t` fields. The generic heap
/// pointer is retained because attached processes reuse the creator's mspace;
/// the unused recursive lock and untyped opaque pointer slots are not part of
/// Hammer's layout.
#[repr(C, align(64))]
pub struct SsvmSharedHeader {
    magic: u64,
    version: u32,
    segment_type: u8,
    padding: [u8; 3],
    ssvm_size: u64,
    ssvm_va: u64,
    server_pid: u32,
    client_pid: u32,
    heap: *mut MemHeap,
    name_len: u32,
    ready: AtomicU32,
    fifo_segment_offset: AtomicU64,
    name: [u8; SSVM_NAME_MAX],
}

/// Offset of the caller-owned payload inside every segment mapping.
pub const SSVM_PAYLOAD_OFFSET: u64 = size_of::<SsvmSharedHeader>() as u64;

/// Segment backend (`ssvm_segment_type_t`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SsvmSegmentBackend {
    Shm,
    Memfd,
    Private,
}

/// Server-side segment request.
#[derive(Debug, Clone)]
pub struct SsvmConfig {
    pub backend: SsvmSegmentBackend,
    pub name: String,
    /// Requested segment size. Generic mappings are page-rounded; PRIVATE
    /// mappings round this value for the heap and add one header page to the
    /// actual mapping.
    pub size: usize,
    /// Address hint, and the address an attacher must use when non-zero.
    pub requested_va: u64,
    pub huge_page: bool,
    /// How long an attacher waits for the segment and for `ready`.
    pub attach_timeout: Duration,
}

/// Backend-specific private parameter (`ssvm_private_t`'s anonymous union).
///
/// Exactly one member is meaningful: `fd` for [`SsvmSegmentBackend::Memfd`],
/// `attach_timeout` for [`SsvmSegmentBackend::Shm`], neither for
/// [`SsvmSegmentBackend::Private`].
#[repr(C)]
pub union SsvmBackendParameter {
    pub fd: RawFd,
    pub attach_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum SsvmError {
    #[error("segment name must be 1..{max} bytes and must not contain NUL")]
    NoName { max: usize },
    #[error("segment size is not set")]
    NoSize,
    #[error("segment size {requested} is too small for the segment header")]
    SizeTooSmall { requested: usize },
    #[error("segment backend is unavailable on this platform")]
    BackendUnavailable,
    #[error("segment attach needs a backing descriptor for this backend")]
    MissingBackingDescriptor,
    #[error("shared segment backend {value} is invalid")]
    InvalidBackend { value: u8 },
    #[error("segment system call failed during {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("client did not map segment {name} within {seconds} s")]
    ClientTimeout { name: String, seconds: u64 },
    #[error("segment header magic is invalid")]
    InvalidMagic,
    #[error("segment header version {version} is unsupported")]
    UnsupportedVersion { version: u32 },
    #[error("segment header size {declared} does not match mapping size {mapped}")]
    SizeMismatch { declared: u64, mapped: usize },
    #[error("segment header is not ready")]
    NotReady,
    #[error("segment offset {offset} with length {length} is outside {size} bytes")]
    OutOfBounds {
        offset: u64,
        length: usize,
        size: usize,
    },
    #[error("segment offset {offset} is not aligned to {alignment}")]
    Misaligned { offset: u64, alignment: usize },
    #[error("failed to create heap at {base:#x}+{size}: {source}")]
    Heap {
        base: usize,
        size: usize,
        #[source]
        source: crate::mem::MemError,
    },
}

/// One mapped segment (`ssvm_private_t`).
pub struct SsvmPrivate {
    base: *mut u8,
    ssvm_size: usize,
    mapping_size: usize,
    backend: SsvmSegmentBackend,
    is_server: bool,
    requested_va: u64,
    my_pid: u32,
    name: CString,
    numa: u8,
    huge_page: bool,
    backing: SsvmBackendParameter,
}

unsafe impl Send for SsvmPrivate {}
unsafe impl Sync for SsvmPrivate {}

impl SsvmPrivate {
    /// Creates a segment with the backend selected by `config`.
    pub fn server_init(config: &SsvmConfig) -> Result<Self, SsvmError> {
        match config.backend {
            SsvmSegmentBackend::Shm => Self::server_init_shm(config),
            SsvmSegmentBackend::Memfd => Self::server_init_memfd(config),
            SsvmSegmentBackend::Private => Self::server_init_private_mode(config, false),
        }
    }

    /// Creates a FIFO segment with the owner-private map-only layout.
    pub fn server_init_fifo_segment(config: &SsvmConfig) -> Result<Self, SsvmError> {
        match config.backend {
            SsvmSegmentBackend::Shm => {
                let name = validated_name(&config.name)?;
                let descriptor = create_shm(&name)?;
                let result =
                    Self::create_shared(SsvmSegmentBackend::Shm, config, &name, descriptor, true);
                close_descriptor(descriptor);
                result
            }
            SsvmSegmentBackend::Memfd => {
                let name = validated_name(&config.name)?;
                let descriptor = create_memfd(&name)?;
                match Self::create_shared(
                    SsvmSegmentBackend::Memfd,
                    config,
                    &name,
                    descriptor,
                    true,
                ) {
                    Ok(segment) => Ok(segment),
                    Err(error) => {
                        close_descriptor(descriptor);
                        Err(error)
                    }
                }
            }
            SsvmSegmentBackend::Private => Self::server_init_private_mode(config, true),
        }
    }

    /// Creates an anonymous private mapping.
    pub fn server_init_private(config: &SsvmConfig) -> Result<Self, SsvmError> {
        Self::server_init_private_mode(config, false)
    }

    fn server_init_private_mode(config: &SsvmConfig, map_only: bool) -> Result<Self, SsvmError> {
        if config.size == 0 {
            return Err(SsvmError::NoSize);
        }
        let name = validated_name(&config.name)?;
        let page = page_size()?;
        let mut rnd_size = rounded_size_for(config.size, page)?;
        let mapping_size = rnd_size.checked_add(page).ok_or(SsvmError::SizeTooSmall {
            requested: config.size,
        })?;
        let base = map_private(mapping_size)?;
        let mut segment = Self {
            base,
            ssvm_size: if map_only { mapping_size } else { rnd_size },
            mapping_size,
            backend: SsvmSegmentBackend::Private,
            is_server: true,
            requested_va: u64::MAX,
            my_pid: std::process::id(),
            name,
            numa: 0,
            huge_page: config.huge_page,
            backing: SsvmBackendParameter { fd: -1 },
        };
        segment.initialize_header(if map_only { 0 } else { base as u64 });
        if !map_only {
            segment.initialize_heap(rnd_size)?;
            let usage = segment
                .heap()
                .expect("private SSVM heap is initialized")
                .usage();
            // `free_bytes` is bounded by the heap size, which already fits the
            // mapping size, so this conversion is a local invariant.
            rnd_size =
                usize::try_from(usage.free_bytes).expect("heap free bytes fit the mapping size");
            segment.ssvm_size = rnd_size;
            unsafe { (*shared_header_at(segment.base)).ssvm_size = rnd_size as u64 };
        }
        Ok(segment)
    }

    /// Creates a POSIX shared-memory segment named by `config.name`.
    pub fn server_init_shm(config: &SsvmConfig) -> Result<Self, SsvmError> {
        let name = validated_name(&config.name)?;
        let descriptor = create_shm(&name)?;
        let result = Self::create_shared(SsvmSegmentBackend::Shm, config, &name, descriptor, false);
        close_descriptor(descriptor);
        result
    }

    /// Creates a memfd-backed segment named by `config.name`.
    pub fn server_init_memfd(config: &SsvmConfig) -> Result<Self, SsvmError> {
        let name = validated_name(&config.name)?;
        let descriptor = create_memfd(&name)?;
        match Self::create_shared(SsvmSegmentBackend::Memfd, config, &name, descriptor, false) {
            Ok(segment) => Ok(segment),
            Err(error) => {
                close_descriptor(descriptor);
                Err(error)
            }
        }
    }

    /// Attaches to the backend selected by `config`.
    ///
    /// `descriptor` is required for [`SsvmSegmentBackend::Memfd`]; the shm
    /// backend looks the segment up by `config.name`.
    pub fn client_init(config: &SsvmConfig, descriptor: Option<RawFd>) -> Result<Self, SsvmError> {
        match config.backend {
            SsvmSegmentBackend::Shm => Self::client_init_shm(&config.name, config.attach_timeout),
            SsvmSegmentBackend::Memfd => {
                let descriptor = descriptor.ok_or(SsvmError::MissingBackingDescriptor)?;
                Self::client_init_memfd(descriptor)
            }
            SsvmSegmentBackend::Private => Err(SsvmError::BackendUnavailable),
        }
    }

    /// Attaches to a POSIX shared-memory segment and waits for `ready`.
    pub fn client_init_shm(name: &str, attach_timeout: Duration) -> Result<Self, SsvmError> {
        let requested_name = validated_name(name)?;
        let deadline = Instant::now() + attach_timeout;
        let descriptor = loop {
            let descriptor =
                unsafe { libc::shm_open(requested_name.as_ptr(), libc::O_RDWR, 0o600) };
            if descriptor >= 0 {
                let mut status: libc::stat = unsafe { std::mem::zeroed() };
                if unsafe { libc::fstat(descriptor, &mut status) } == 0 && status.st_size > 0 {
                    break descriptor;
                }
                close_descriptor(descriptor);
            }
            if Instant::now() >= deadline {
                return Err(timeout_error(name, attach_timeout));
            }
            sleep(SSVM_POLL_INTERVAL);
        };
        let segment = match Self::attach(descriptor, attach_timeout, true) {
            Ok(segment) => segment,
            Err(error) => {
                close_descriptor(descriptor);
                return Err(error);
            }
        };
        close_descriptor(descriptor);
        Ok(segment)
    }

    /// Attaches to a memfd-backed segment.
    ///
    /// The mapping owns a duplicate of `descriptor`; the caller keeps its own.
    pub fn client_init_memfd(descriptor: RawFd) -> Result<Self, SsvmError> {
        let owned = duplicate_descriptor(descriptor)?;
        match Self::attach(owned, Duration::ZERO, false) {
            Ok(segment) => Ok(segment),
            Err(error) => {
                close_descriptor(owned);
                Err(error)
            }
        }
    }

    /// Releases the mapping and, for shm-backed segments, its backing name.
    pub fn delete(self) {
        if self.backend == SsvmSegmentBackend::Shm {
            unsafe { libc::shm_unlink(self.name.as_ptr()) };
        }
        drop(self);
    }

    /// Backend recorded in the shared header (`ssvm_type`).
    pub fn segment_type(&self) -> SsvmSegmentBackend {
        self.backend
    }

    /// Segment name recorded in the shared header (`ssvm_name`).
    pub fn name(&self) -> &str {
        std::str::from_utf8(self.name.to_bytes())
            .expect("segment name was validated as UTF-8 at construction")
    }

    /// Fixed address recorded by the creator; 0 means any address is allowed.
    pub fn ssvm_va(&self) -> u64 {
        self.shared_header().ssvm_va
    }

    pub fn published_va(&self) -> Option<std::num::NonZeroUsize> {
        std::num::NonZeroUsize::new(self.ssvm_va() as usize)
    }

    /// Advertised `ssvm_size` in bytes.
    ///
    /// For PRIVATE SSVM this is the dlmalloc free space after heap creation;
    /// the actual mmap length is kept separately by the mapping owner.
    pub fn ssvm_size(&self) -> usize {
        self.ssvm_size
    }

    /// Process that created the segment.
    pub fn server_pid(&self) -> u32 {
        self.shared_header().server_pid
    }

    /// Process that most recently attached; 0 while unattached.
    pub fn client_pid(&self) -> u32 {
        self.shared_header().client_pid
    }

    /// Mapping base used for payload offset arithmetic.
    pub fn base(&self) -> *mut u8 {
        self.base
    }

    #[inline(always)]
    pub(crate) fn page_size(&self) -> Result<usize, SsvmError> {
        page_size()
    }

    /// Descriptor owned by this mapping, when the backend keeps one open.
    pub fn fd(&self) -> Option<RawFd> {
        match self.backend {
            SsvmSegmentBackend::Memfd => {
                let descriptor = unsafe { self.backing.fd };
                (descriptor >= 0).then_some(descriptor)
            }
            SsvmSegmentBackend::Shm | SsvmSegmentBackend::Private => None,
        }
    }

    pub fn heap(&self) -> Option<&MemHeap> {
        let pointer = self.shared_header().heap;
        if pointer.is_null() {
            return None;
        }
        let start = self.base as usize;
        let end = start.checked_add(self.mapping_size)?;
        let address = pointer as usize;
        if address < start || address >= end {
            return None;
        }
        Some(unsafe { &*pointer })
    }

    /// Whether this mapping created the segment.
    pub fn is_server(&self) -> bool {
        self.is_server
    }

    /// Requested address, or 0 when the mapping may land anywhere.
    pub fn requested_va(&self) -> u64 {
        self.requested_va
    }

    /// NUMA node request; unused, kept to match `ssvm_private_t`.
    pub fn numa(&self) -> u8 {
        self.numa
    }

    /// Huge page request; unused, kept to match `ssvm_private_t`.
    pub fn huge_page(&self) -> bool {
        self.huge_page
    }

    /// Offset at which the caller-owned payload starts inside this mapping.
    pub fn payload_offset(&self) -> u64 {
        SSVM_PAYLOAD_OFFSET
    }

    /// Bytes from [`Self::payload_offset`] to the advertised segment end.
    pub fn payload_len(&self) -> u64 {
        self.ssvm_size as u64 - self.payload_offset()
    }

    /// Whether the application owner finished initializing the payload.
    pub fn is_ready(&self) -> bool {
        self.shared_header().ready.load(Ordering::Acquire) != 0
    }

    /// Publishes a fully initialized payload to attachers.
    pub fn publish_ready(&self) {
        assert!(self.is_server, "only the segment creator may publish ready");
        self.shared_header().ready.store(1, Ordering::Release);
    }

    /// Offset of the VPP-style FIFO segment header relative to this mapping.
    pub fn fifo_segment_offset(&self) -> Option<u64> {
        let offset = self
            .shared_header()
            .fifo_segment_offset
            .load(Ordering::Acquire);
        (offset != 0).then_some(offset)
    }

    /// Publishes the FIFO segment header location before the segment ready bit.
    pub fn set_fifo_segment_offset(&self, offset: u64) {
        assert!(
            self.is_server,
            "only the segment creator may publish metadata"
        );
        self.shared_header()
            .fifo_segment_offset
            .store(offset, Ordering::Release);
    }

    /// Waits for the creator to publish `ready`.
    pub fn wait_ready(&self, timeout: Duration) -> Result<(), SsvmError> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.is_ready() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(timeout_error(self.name(), timeout));
            }
            sleep(SSVM_POLL_INTERVAL);
        }
    }

    /// Resolves a payload-relative offset to a pointer in this mapping.
    pub fn offset_ptr(
        &self,
        offset: u64,
        length: usize,
        alignment: usize,
    ) -> Result<*mut u8, SsvmError> {
        if alignment == 0
            || !alignment.is_power_of_two()
            || !(offset as usize).is_multiple_of(alignment)
        {
            return Err(SsvmError::Misaligned { offset, alignment });
        }
        let end = offset
            .checked_add(length as u64)
            .ok_or(SsvmError::OutOfBounds {
                offset,
                length,
                size: self.ssvm_size,
            })?;
        if end > self.ssvm_size as u64 {
            return Err(SsvmError::OutOfBounds {
                offset,
                length,
                size: self.ssvm_size,
            });
        }
        Ok(unsafe { self.base.add(offset as usize) })
    }

    fn create_shared(
        backend: SsvmSegmentBackend,
        config: &SsvmConfig,
        name: &CString,
        descriptor: RawFd,
        map_only: bool,
    ) -> Result<Self, SsvmError> {
        if config.size == 0 {
            return Err(SsvmError::NoSize);
        }
        let mapped_size = mapped_size_for(config.size)?;
        if unsafe { libc::ftruncate(descriptor, mapped_size as libc::off_t) } != 0 {
            return Err(io_error("ftruncate"));
        }
        let base = map_raw(
            config.requested_va as *mut libc::c_void,
            mapped_size,
            libc::MAP_SHARED,
            descriptor,
        )?;
        let recorded_va = if map_only { 0 } else { base as u64 };
        let mut segment = Self {
            base,
            ssvm_size: mapped_size,
            mapping_size: mapped_size,
            backend,
            is_server: true,
            requested_va: config.requested_va,
            my_pid: std::process::id(),
            name: name.clone(),
            numa: 0,
            huge_page: config.huge_page,
            backing: backend_parameter(backend, descriptor, config.attach_timeout),
        };
        segment.initialize_header(recorded_va);
        if !map_only {
            let page = page_size()?;
            let heap_size = mapped_size
                .checked_sub(page)
                .ok_or(SsvmError::SizeTooSmall {
                    requested: config.size,
                })?;
            segment.initialize_heap(heap_size)?;
        }
        Ok(segment)
    }

    /// Maps a segment another process created.
    ///
    /// Every error path returns before the mapping owns `descriptor`, so the
    /// caller still closes it; on success the mapping owns it.
    fn attach(
        descriptor: RawFd,
        shm_timeout: Duration,
        wait_for_ready: bool,
    ) -> Result<Self, SsvmError> {
        let page = page_size()?;
        let probe = map_raw(std::ptr::null_mut(), page, libc::MAP_SHARED, descriptor)?;
        let header = unsafe { &*shared_header_at(probe) };
        if let Err(error) = validate_header(header) {
            unsafe { libc::munmap(probe.cast(), page) };
            return Err(error);
        }
        if wait_for_ready {
            let deadline = Instant::now() + shm_timeout;
            while header.ready.load(Ordering::Acquire) == 0 {
                if Instant::now() >= deadline {
                    let error = match name_from_header(header) {
                        Ok(name) => timeout_error(
                            name.to_str().expect("validated segment name"),
                            shm_timeout,
                        ),
                        Err(error) => error,
                    };
                    unsafe { libc::munmap(probe.cast(), page) };
                    return Err(error);
                }
                sleep(SSVM_POLL_INTERVAL);
            }
        }
        let name = match name_from_header(header) {
            Ok(name) => name,
            Err(error) => {
                unsafe { libc::munmap(probe.cast(), page) };
                return Err(error);
            }
        };
        let backend = match backend_from_byte(header.segment_type) {
            Ok(backend) => backend,
            Err(error) => {
                unsafe { libc::munmap(probe.cast(), page) };
                return Err(error);
            }
        };
        let probed = (header.ssvm_size, header.ssvm_va, backend, name, header.heap);
        unsafe { libc::munmap(probe.cast(), page) };
        let (declared_size, declared_va, backend, name, heap) = probed;
        if backend == SsvmSegmentBackend::Private {
            return Err(SsvmError::BackendUnavailable);
        }
        if declared_size < page as u64 || !declared_size.is_multiple_of(page as u64) {
            return Err(SsvmError::SizeMismatch {
                declared: declared_size,
                mapped: page,
            });
        }
        let mapped_size = declared_size as usize;
        let flags = if declared_va == 0 {
            libc::MAP_SHARED
        } else {
            libc::MAP_SHARED | libc::MAP_FIXED
        };
        let base = map_raw(
            declared_va as *mut libc::c_void,
            mapped_size,
            flags,
            descriptor,
        )?;
        let segment = Self {
            base,
            ssvm_size: mapped_size,
            mapping_size: mapped_size,
            backend,
            is_server: false,
            requested_va: declared_va,
            my_pid: std::process::id(),
            name,
            numa: 0,
            huge_page: false,
            backing: backend_parameter(backend, descriptor, shm_timeout),
        };
        let header = unsafe { &mut *shared_header_at(segment.base) };
        debug_assert_eq!(header.ssvm_size, mapped_size as u64);
        header.client_pid = std::process::id();
        if !heap.is_null() {
            let heap_address = heap as usize;
            let mapping_end = segment.base.addr().checked_add(segment.ssvm_size).ok_or(
                SsvmError::SizeMismatch {
                    declared: declared_size,
                    mapped: mapped_size,
                },
            )?;
            if heap_address < segment.base.addr() || heap_address >= mapping_end {
                return Err(SsvmError::InvalidMagic);
            }
        }
        Ok(segment)
    }

    fn initialize_header(&mut self, recorded_va: u64) {
        let bytes = self.name.to_bytes();
        debug_assert!(!bytes.is_empty() && bytes.len() < SSVM_NAME_MAX);
        let header = unsafe { &mut *shared_header_at(self.base) };
        header.magic = SSVM_MAGIC;
        header.version = SSVM_VERSION;
        header.segment_type = backend_byte(self.backend);
        header.ssvm_size = self.ssvm_size as u64;
        header.ssvm_va = recorded_va;
        header.server_pid = self.my_pid;
        header.client_pid = 0;
        header.heap = std::ptr::null_mut();
        header.name_len = bytes.len() as u32;
        header.fifo_segment_offset.store(0, Ordering::Relaxed);
        header.name[..bytes.len()].copy_from_slice(bytes);
        header.ready.store(0, Ordering::Release);
    }

    fn initialize_heap(&mut self, heap_size: usize) -> Result<(), SsvmError> {
        let page = page_size()?;
        let heap_base =
            NonNull::new(unsafe { self.base.add(page) }).expect("heap base is non-null");
        if heap_size == 0
            || page
                .checked_add(heap_size)
                .is_none_or(|end| end > self.mapping_size)
        {
            return Err(SsvmError::SizeTooSmall {
                requested: heap_size,
            });
        }
        let heap = unsafe {
            MemHeap::create_at(heap_base, heap_size, true, "ssvm heap").map_err(|source| {
                SsvmError::Heap {
                    base: heap_base.as_ptr() as usize,
                    size: heap_size,
                    source,
                }
            })?
        };
        unsafe { (*shared_header_at(self.base)).heap = heap.as_ptr() };
        Ok(())
    }

    fn shared_header(&self) -> &SsvmSharedHeader {
        unsafe { &*shared_header_at(self.base) }
    }
}

impl Drop for SsvmPrivate {
    fn drop(&mut self) {
        if self.is_server {
            if let Some(heap) = self.heap() {
                unsafe { heap.destroy() };
            }
        }
        unsafe {
            libc::munmap(self.base.cast(), self.mapping_size);
        }
        if let Some(descriptor) = self.fd() {
            close_descriptor(descriptor);
        }
    }
}

impl std::fmt::Debug for SsvmPrivate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SsvmPrivate")
            .field("base", &self.base)
            .field("ssvm_size", &self.ssvm_size)
            .field("segment_type", &self.backend)
            .field("is_server", &self.is_server)
            .field("requested_va", &self.requested_va)
            .field("name", &self.name)
            .field("ready", &self.is_ready())
            .finish_non_exhaustive()
    }
}

fn shared_header_at(base: *mut u8) -> *mut SsvmSharedHeader {
    base.cast::<SsvmSharedHeader>()
}

fn validate_header(header: &SsvmSharedHeader) -> Result<(), SsvmError> {
    if header.magic != SSVM_MAGIC {
        return Err(SsvmError::InvalidMagic);
    }
    if header.version != SSVM_VERSION {
        return Err(SsvmError::UnsupportedVersion {
            version: header.version,
        });
    }
    if header.ssvm_size == 0 {
        return Err(SsvmError::NotReady);
    }
    Ok(())
}

fn name_from_header(header: &SsvmSharedHeader) -> Result<CString, SsvmError> {
    let length = (header.name_len as usize).min(SSVM_NAME_MAX);
    CString::new(&header.name[..length]).map_err(|_| SsvmError::NoName { max: SSVM_NAME_MAX })
}

fn backend_byte(backend: SsvmSegmentBackend) -> u8 {
    match backend {
        SsvmSegmentBackend::Shm => 0,
        SsvmSegmentBackend::Memfd => 1,
        SsvmSegmentBackend::Private => 2,
    }
}

fn backend_parameter(
    backend: SsvmSegmentBackend,
    descriptor: RawFd,
    attach_timeout: Duration,
) -> SsvmBackendParameter {
    match backend {
        SsvmSegmentBackend::Memfd => SsvmBackendParameter { fd: descriptor },
        SsvmSegmentBackend::Shm => SsvmBackendParameter { attach_timeout },
        SsvmSegmentBackend::Private => SsvmBackendParameter { fd: -1 },
    }
}

fn backend_from_byte(value: u8) -> Result<SsvmSegmentBackend, SsvmError> {
    match value {
        0 => Ok(SsvmSegmentBackend::Shm),
        1 => Ok(SsvmSegmentBackend::Memfd),
        2 => Ok(SsvmSegmentBackend::Private),
        other => Err(SsvmError::InvalidBackend { value: other }),
    }
}

fn validated_name(name: &str) -> Result<CString, SsvmError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() >= SSVM_NAME_MAX {
        return Err(SsvmError::NoName { max: SSVM_NAME_MAX });
    }
    CString::new(name).map_err(|_| SsvmError::NoName { max: SSVM_NAME_MAX })
}

fn mapped_size_for(size: usize) -> Result<usize, SsvmError> {
    if size == 0 {
        return Err(SsvmError::NoSize);
    }
    let page = page_size()?;
    let mapped = rounded_size_for(size, page)?;
    if mapped < size_of::<SsvmSharedHeader>() {
        return Err(SsvmError::SizeTooSmall { requested: size });
    }
    Ok(mapped)
}

fn rounded_size_for(size: usize, page: usize) -> Result<usize, SsvmError> {
    let rounded = size
        .checked_add(page - 1)
        .ok_or(SsvmError::SizeTooSmall { requested: size })?
        & !(page - 1);
    if rounded == 0 {
        return Err(SsvmError::SizeTooSmall { requested: size });
    }
    Ok(rounded)
}

fn timeout_error(name: &str, timeout: Duration) -> SsvmError {
    SsvmError::ClientTimeout {
        name: name.to_string(),
        seconds: timeout.as_secs(),
    }
}

fn create_shm(name: &CString) -> Result<RawFd, SsvmError> {
    unsafe { libc::shm_unlink(name.as_ptr()) };
    let descriptor = unsafe {
        libc::shm_open(
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
            0o600,
        )
    };
    if descriptor < 0 {
        return Err(io_error("shm_open"));
    }
    Ok(descriptor)
}

fn create_memfd(name: &CString) -> Result<RawFd, SsvmError> {
    #[cfg(target_os = "linux")]
    {
        let descriptor =
            unsafe { libc::syscall(libc::SYS_memfd_create, name.as_ptr(), libc::MFD_CLOEXEC) };
        if descriptor < 0 {
            return Err(io_error("memfd_create"));
        }
        Ok(descriptor as RawFd)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = name;
        Err(SsvmError::BackendUnavailable)
    }
}

fn duplicate_descriptor(descriptor: RawFd) -> Result<RawFd, SsvmError> {
    let owned = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 0) };
    if owned < 0 {
        return Err(io_error("duplicate segment descriptor"));
    }
    Ok(owned)
}

fn close_descriptor(descriptor: RawFd) {
    unsafe { libc::close(descriptor) };
}

fn map_private(size: usize) -> Result<*mut u8, SsvmError> {
    map_raw(
        std::ptr::null_mut(),
        size,
        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
        -1,
    )
}

fn map_raw(
    address: *mut libc::c_void,
    size: usize,
    flags: libc::c_int,
    descriptor: RawFd,
) -> Result<*mut u8, SsvmError> {
    let base = unsafe {
        libc::mmap(
            address,
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            descriptor,
            0,
        )
    };
    if base == libc::MAP_FAILED {
        return Err(io_error("shared mmap"));
    }
    Ok(base.cast())
}

fn page_size() -> Result<usize, SsvmError> {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page <= 0 {
        return Err(io_error("page size"));
    }
    Ok(page as usize)
}

fn io_error(operation: &'static str) -> SsvmError {
    SsvmError::Io {
        operation,
        source: io::Error::last_os_error(),
    }
}
