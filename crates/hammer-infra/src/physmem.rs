//! VPP-style physmem shared map for buffer packet regions.
//!
//! Packet slots are carved from this mapped span. Freelist metadata stays on
//! the main heap (see Buffer Arena). This is independent of [`SvmRegion`].

use std::ffi::CString;
use std::fmt;
use std::io;
use std::os::fd::RawFd;
#[cfg(target_os = "linux")]
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::main_heap::PageSize;

static PHYSMEM_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub enum PhysmemError {
    InvalidSize {
        requested: usize,
    },
    PageSizeOverflow {
        requested: PageSize,
    },
    UnsupportedPageSize {
        requested: PageSize,
    },
    PageSizeQuery {
        source: io::Error,
    },
    Create {
        source: io::Error,
    },
    Truncate {
        source: io::Error,
    },
    Map {
        source: io::Error,
    },
    HugePageDiscovery {
        requested: PageSize,
        source: io::Error,
    },
    HugePageUnsupported {
        requested: PageSize,
        page_size: usize,
        path: PathBuf,
    },
    HugePagePool {
        operation: &'static str,
        path: PathBuf,
        requested: PageSize,
        page_size: usize,
        numa_node: u32,
        required: usize,
        free: usize,
        current: usize,
        attempted: Option<usize>,
        source: io::Error,
    },
    NumaPolicy {
        operation: &'static str,
        numa_node: u32,
        source: io::Error,
    },
    NumaPolicyRestore {
        primary: Box<Self>,
        source: io::Error,
    },
    BackingVerification {
        requested: usize,
        actual: usize,
    },
    PlacementVerification {
        requested: u32,
        actual: i32,
    },
}

impl fmt::Display for PhysmemError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSize { requested } => {
                write!(formatter, "physmem size must be non-zero, got {requested}")
            }
            Self::PageSizeOverflow { requested } => {
                write!(
                    formatter,
                    "physmem page size `{requested}` does not fit usize"
                )
            }
            Self::UnsupportedPageSize { requested } => write!(
                formatter,
                "physmem page size `{requested}` is unsupported on this platform"
            ),
            Self::PageSizeQuery { source } => {
                write!(formatter, "failed to query the OS page size: {source}")
            }
            Self::Create { source } => {
                write!(formatter, "failed to create physmem backing: {source}")
            }
            Self::Truncate { source } => {
                write!(formatter, "failed to size physmem backing: {source}")
            }
            Self::Map { source } => write!(formatter, "failed to map physmem backing: {source}"),
            Self::HugePageDiscovery { requested, source } => write!(
                formatter,
                "failed to resolve HugeTLB size for `{requested}`: {source}"
            ),
            Self::HugePageUnsupported {
                requested,
                page_size,
                path,
            } => write!(
                formatter,
                "HugeTLB size {page_size} for `{requested}` is unavailable at {}",
                path.display()
            ),
            Self::HugePagePool {
                operation,
                path,
                page_size,
                numa_node,
                required,
                free,
                current,
                attempted,
                source,
                ..
            } => {
                write!(
                    formatter,
                    "HugeTLB pool operation `{operation}` at {} failed for {required} pages of {page_size} bytes on NUMA node {numa_node} (free {free}, current {current}",
                    path.display()
                )?;
                if let Some(attempted) = attempted {
                    write!(formatter, ", attempted {attempted}")?;
                }
                write!(formatter, "): {source}")
            }
            Self::NumaPolicy {
                operation,
                numa_node,
                source,
            } => write!(
                formatter,
                "NUMA policy operation `{operation}` for node {numa_node} failed: {source}"
            ),
            Self::NumaPolicyRestore { primary, source } => write!(
                formatter,
                "{primary}; restoring the previous NUMA policy also failed: {source}"
            ),
            Self::BackingVerification { requested, actual } => write!(
                formatter,
                "HugeTLB mapping reports kernel page size {actual}, requested {requested}"
            ),
            Self::PlacementVerification { requested, actual } => write!(
                formatter,
                "HugeTLB mapping landed on NUMA node {actual}, requested node {requested}"
            ),
        }
    }
}

impl std::error::Error for PhysmemError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PageSizeQuery { source }
            | Self::Create { source }
            | Self::Truncate { source }
            | Self::Map { source }
            | Self::HugePageDiscovery { source, .. }
            | Self::HugePagePool { source, .. }
            | Self::NumaPolicy { source, .. } => Some(source),
            Self::NumaPolicyRestore { primary, .. } => Some(primary),
            Self::InvalidSize { .. }
            | Self::PageSizeOverflow { .. }
            | Self::UnsupportedPageSize { .. }
            | Self::HugePageUnsupported { .. }
            | Self::BackingVerification { .. }
            | Self::PlacementVerification { .. } => None,
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) struct MappedRegion {
    base: *mut u8,
    size: usize,
    fd: RawFd,
}

#[cfg(target_os = "linux")]
impl MappedRegion {
    #[inline]
    pub(crate) fn base(&self) -> *mut u8 {
        self.base
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    #[inline]
    fn into_parts(mut self) -> (*mut u8, usize, RawFd) {
        let parts = (self.base, self.size, self.fd);
        self.base = std::ptr::null_mut();
        self.size = 0;
        self.fd = -1;
        parts
    }

    #[inline]
    pub(crate) fn retain_for_process_lifetime(mut self) {
        self.base = std::ptr::null_mut();
        self.size = 0;
        self.fd = -1;
    }
}

#[cfg(target_os = "linux")]
impl Drop for MappedRegion {
    fn drop(&mut self) {
        if !self.base.is_null() && self.size != 0 {
            // SAFETY: this object exclusively owns the mapping for its entire
            // recorded length until ownership is transferred to mimalloc.
            unsafe { libc::munmap(self.base.cast(), self.size) };
        }
        if self.fd >= 0 {
            // SAFETY: this object owns the descriptor and closes it once.
            unsafe { libc::close(self.fd) };
        }
    }
}

/// NUMA-aware shared mmap arena used as the Buffer Arena packet region.
pub struct PhysmemMap {
    base: *mut u8,
    size: usize,
    numa_node: u32,
    page_size: usize,
    hugetlb: bool,
    fd: RawFd,
    fd_owned: bool,
}

unsafe impl Send for PhysmemMap {}
unsafe impl Sync for PhysmemMap {}

impl PhysmemMap {
    /// Create a shared map sized for buffer packet storage.
    ///
    /// `name` is retained for VPP-shaped call sites; the OS object uses a short
    /// unique token because macOS `shm_open` names are length-limited.
    pub fn create(
        _name: &str,
        size: usize,
        requested_page_size: PageSize,
        numa_node: u32,
    ) -> Result<Self, PhysmemError> {
        if size == 0 {
            return Err(PhysmemError::InvalidSize { requested: size });
        }
        #[cfg(not(target_os = "linux"))]
        if !requested_page_size.is_supported_on_current_platform() {
            return Err(PhysmemError::UnsupportedPageSize {
                requested: requested_page_size,
            });
        }
        let ordinary_page_size = PageSize::Default
            .bytes()
            .map_err(|source| PhysmemError::PageSizeQuery { source })?;
        let page_bytes =
            requested_page_size
                .bytes()
                .map_err(|source| PhysmemError::HugePageDiscovery {
                    requested: requested_page_size,
                    source,
                })?;
        let hugetlb = page_bytes != ordinary_page_size;
        #[cfg(not(target_os = "linux"))]
        if hugetlb {
            return Err(PhysmemError::UnsupportedPageSize {
                requested: requested_page_size,
            });
        }
        let total = checked_align_up(size, page_bytes).ok_or(PhysmemError::PageSizeOverflow {
            requested: requested_page_size,
        })?;
        #[cfg(target_os = "linux")]
        let (base, fd, fd_owned) = {
            if hugetlb {
                let (base, _, fd) = map_hugetlb(
                    total,
                    requested_page_size,
                    page_bytes,
                    numa_node,
                    true,
                    page_bytes,
                )?
                .into_parts();
                return Ok(Self {
                    base,
                    size: total,
                    numa_node,
                    page_size: page_bytes,
                    hugetlb: true,
                    fd,
                    fd_owned: true,
                });
            }
            let counter = PHYSMEM_COUNTER.fetch_add(1, Ordering::Relaxed);
            let label = format!("hpm{}-{counter}", std::process::id());
            let cname = CString::new(label).expect("generated memfd name contains no NUL");
            let fd = unsafe { libc::memfd_create(cname.as_ptr(), libc::MFD_CLOEXEC) };
            if fd < 0 {
                return Err(PhysmemError::Create {
                    source: io::Error::last_os_error(),
                });
            }
            if unsafe { libc::ftruncate(fd, total as libc::off_t) } != 0 {
                let source = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(PhysmemError::Truncate { source });
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
                let source = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(PhysmemError::Map { source });
            }
            (base.cast::<u8>(), fd, true)
        };

        #[cfg(not(target_os = "linux"))]
        let (base, fd, fd_owned) = {
            let (cname, fd) = loop {
                let counter = PHYSMEM_COUNTER.fetch_add(1, Ordering::Relaxed);
                let label = format!("/hpm{}-{counter}", std::process::id());
                let cname = CString::new(label).expect("generated shm name contains no NUL");
                let fd = unsafe {
                    libc::shm_open(
                        cname.as_ptr(),
                        libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                        0o600,
                    )
                };
                if fd >= 0 {
                    break (cname, fd);
                }
                if std::io::Error::last_os_error().raw_os_error() != Some(libc::EEXIST) {
                    return Err(PhysmemError::Create {
                        source: io::Error::last_os_error(),
                    });
                }
            };
            unsafe { libc::shm_unlink(cname.as_ptr()) };
            if unsafe { libc::ftruncate(fd, total as libc::off_t) } != 0 {
                let source = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(PhysmemError::Truncate { source });
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
                let source = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(PhysmemError::Map { source });
            }
            (base.cast::<u8>(), fd, true)
        };

        Ok(Self {
            base,
            size: total,
            numa_node,
            page_size: page_bytes,
            hugetlb,
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
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    #[inline]
    pub fn is_hugetlb(&self) -> bool {
        self.hugetlb
    }

    #[inline]
    pub fn fd(&self) -> RawFd {
        self.fd
    }
}

fn checked_align_up(value: usize, alignment: usize) -> Option<usize> {
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|rounded| rounded / alignment * alignment)
}

#[cfg(target_os = "linux")]
fn provision_hugepages(
    directory: &Path,
    requested: PageSize,
    page_size: usize,
    numa_node: u32,
    required: usize,
) -> Result<(), PhysmemError> {
    let free_path = directory.join("free_hugepages");
    let current_path = directory.join("nr_hugepages");
    let free = std::fs::read_to_string(&free_path)
        .and_then(|value| {
            value
                .trim()
                .parse::<usize>()
                .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))
        })
        .map_err(|source| PhysmemError::HugePagePool {
            operation: "read free",
            path: free_path.clone(),
            requested,
            page_size,
            numa_node,
            required,
            free: 0,
            current: 0,
            attempted: None,
            source,
        })?;
    let current = std::fs::read_to_string(&current_path)
        .and_then(|value| {
            value
                .trim()
                .parse::<usize>()
                .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))
        })
        .map_err(|source| PhysmemError::HugePagePool {
            operation: "read current",
            path: current_path.clone(),
            requested,
            page_size,
            numa_node,
            required,
            free,
            current: 0,
            attempted: None,
            source,
        })?;
    if free >= required {
        return Ok(());
    }

    let attempted = current
        .checked_add(required - free)
        .ok_or(PhysmemError::PageSizeOverflow { requested })?;
    std::fs::write(&current_path, attempted.to_string()).map_err(|source| {
        PhysmemError::HugePagePool {
            operation: "grow",
            path: current_path.clone(),
            requested,
            page_size,
            numa_node,
            required,
            free,
            current,
            attempted: Some(attempted),
            source,
        }
    })?;
    let available = std::fs::read_to_string(&free_path)
        .and_then(|value| {
            value
                .trim()
                .parse::<usize>()
                .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))
        })
        .map_err(|source| PhysmemError::HugePagePool {
            operation: "re-read free",
            path: free_path,
            requested,
            page_size,
            numa_node,
            required,
            free,
            current,
            attempted: Some(attempted),
            source,
        })?;
    let provisioned = std::fs::read_to_string(&current_path)
        .and_then(|value| {
            value
                .trim()
                .parse::<usize>()
                .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))
        })
        .map_err(|source| PhysmemError::HugePagePool {
            operation: "re-read current",
            path: current_path.clone(),
            requested,
            page_size,
            numa_node,
            required,
            free: available,
            current,
            attempted: Some(attempted),
            source,
        })?;
    if available < required {
        return Err(PhysmemError::HugePagePool {
            operation: "confirm",
            path: current_path,
            requested,
            page_size,
            numa_node,
            required,
            free: available,
            current: provisioned,
            attempted: Some(attempted),
            source: io::Error::other("kernel did not make the requested pages available"),
        });
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn map_hugetlb(
    size: usize,
    requested: PageSize,
    page_size: usize,
    numa_node: u32,
    shared: bool,
    alignment: usize,
) -> Result<MappedRegion, PhysmemError> {
    use procfs::process::Process;

    const MFD_HUGETLB: libc::c_uint = 0x0004;
    const MFD_HUGE_SHIFT: u32 = 26;
    const MAX_NUMA_NODES: usize = 1024;

    let alignment = alignment.max(page_size);
    if page_size == 0
        || !page_size.is_power_of_two()
        || !alignment.is_power_of_two()
        || alignment % page_size != 0
    {
        return Err(PhysmemError::UnsupportedPageSize { requested });
    }
    let total =
        checked_align_up(size, alignment).ok_or(PhysmemError::PageSizeOverflow { requested })?;
    let mut directory = PathBuf::from(format!(
        "/sys/devices/system/node/node{numa_node}/hugepages/hugepages-{}kB",
        page_size / 1024
    ));
    if numa_node == 0 && !directory.is_dir() {
        directory = PathBuf::from(format!(
            "/sys/kernel/mm/hugepages/hugepages-{}kB",
            page_size / 1024
        ));
    }
    if !directory.is_dir() {
        return Err(PhysmemError::HugePageUnsupported {
            requested,
            page_size,
            path: directory,
        });
    }

    let required = total / page_size;
    provision_hugepages(&directory, requested, page_size, numa_node, required)?;

    let mut previous_mode: libc::c_int = 0;
    let mut previous_mask = [0 as libc::c_ulong; MAX_NUMA_NODES / libc::c_ulong::BITS as usize];
    // SAFETY: all pointers refer to writable storage sized for `maxnode` bits.
    if unsafe {
        libc::syscall(
            libc::SYS_get_mempolicy,
            &mut previous_mode,
            previous_mask.as_mut_ptr(),
            MAX_NUMA_NODES as libc::c_ulong,
            std::ptr::null_mut::<libc::c_void>(),
            0,
        )
    } != 0
    {
        return Err(PhysmemError::NumaPolicy {
            operation: "snapshot",
            numa_node,
            source: io::Error::last_os_error(),
        });
    }

    let node = usize::try_from(numa_node).map_err(|_| PhysmemError::NumaPolicy {
        operation: "bind",
        numa_node,
        source: io::Error::other("NUMA node does not fit usize"),
    })?;
    if node >= MAX_NUMA_NODES {
        return Err(PhysmemError::NumaPolicy {
            operation: "bind",
            numa_node,
            source: io::Error::other("NUMA node exceeds the Linux nodemask limit"),
        });
    }
    let mut target_mask = [0 as libc::c_ulong; MAX_NUMA_NODES / libc::c_ulong::BITS as usize];
    target_mask[node / libc::c_ulong::BITS as usize] = 1 << (node % libc::c_ulong::BITS as usize);
    // SAFETY: `target_mask` contains exactly the requested node and remains
    // live for the syscall.
    if unsafe {
        libc::syscall(
            libc::SYS_set_mempolicy,
            libc::MPOL_BIND,
            target_mask.as_ptr(),
            MAX_NUMA_NODES as libc::c_ulong,
        )
    } != 0
    {
        return Err(PhysmemError::NumaPolicy {
            operation: "bind",
            numa_node,
            source: io::Error::last_os_error(),
        });
    }

    let mapping = (|| {
        let fd = if shared {
            let counter = PHYSMEM_COUNTER.fetch_add(1, Ordering::Relaxed);
            let label = format!("hpm{}-{counter}", std::process::id());
            let cname = CString::new(label).expect("generated memfd name contains no NUL");
            let log2_page_size = page_size.trailing_zeros();
            // SAFETY: the generated name is NUL terminated and flags follow
            // the Linux memfd HugeTLB ABI.
            let fd = unsafe {
                libc::memfd_create(
                    cname.as_ptr(),
                    libc::MFD_CLOEXEC | MFD_HUGETLB | (log2_page_size << MFD_HUGE_SHIFT),
                )
            };
            if fd < 0 {
                return Err(PhysmemError::Create {
                    source: io::Error::last_os_error(),
                });
            }
            // SAFETY: `fd` is owned here and `total` was checked to fit usize.
            if unsafe { libc::ftruncate(fd, total as libc::off_t) } != 0 {
                let source = io::Error::last_os_error();
                // SAFETY: this branch still owns the descriptor.
                unsafe { libc::close(fd) };
                return Err(PhysmemError::Truncate { source });
            }
            fd
        } else {
            -1
        };
        let reserve_size = total
            .checked_add(alignment)
            .ok_or(PhysmemError::PageSizeOverflow { requested })?;
        // SAFETY: this anonymous inaccessible reservation only selects an
        // address range; MAP_FIXED below replaces its aligned middle.
        let reservation = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                reserve_size,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if reservation == libc::MAP_FAILED {
            if fd >= 0 {
                // SAFETY: this branch still owns the descriptor.
                unsafe { libc::close(fd) };
            }
            return Err(PhysmemError::Map {
                source: io::Error::last_os_error(),
            });
        }
        let reservation_start = reservation as usize;
        let Some(aligned_start) = checked_align_up(reservation_start, alignment) else {
            // SAFETY: this branch still owns the complete reservation.
            unsafe { libc::munmap(reservation, reserve_size) };
            if fd >= 0 {
                // SAFETY: this branch still owns the descriptor.
                unsafe { libc::close(fd) };
            }
            return Err(PhysmemError::PageSizeOverflow { requested });
        };
        let prefix = aligned_start - reservation_start;
        let suffix = reserve_size - prefix - total;
        let flags = if shared {
            libc::MAP_SHARED | libc::MAP_FIXED
        } else {
            libc::MAP_PRIVATE
                | libc::MAP_ANONYMOUS
                | libc::MAP_HUGETLB
                | libc::MAP_FIXED
                | ((page_size.trailing_zeros() as libc::c_int) << 26)
        };
        // SAFETY: the length and flags describe a new read/write mapping; the
        // descriptor is valid for shared mappings and ignored for anonymous.
        let base = unsafe {
            libc::mmap(
                aligned_start as *mut libc::c_void,
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                fd,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            let source = io::Error::last_os_error();
            // SAFETY: the failed replacement leaves this reservation owned.
            unsafe { libc::munmap(reservation, reserve_size) };
            if fd >= 0 {
                // SAFETY: mapping failed, so this branch retains fd ownership.
                unsafe { libc::close(fd) };
            }
            return Err(PhysmemError::Map { source });
        }
        if prefix != 0 {
            // SAFETY: this is the untouched prefix of the owned reservation.
            unsafe { libc::munmap(reservation, prefix) };
        }
        if suffix != 0 {
            // SAFETY: this is the untouched suffix after the new mapping.
            unsafe { libc::munmap((aligned_start + total) as *mut libc::c_void, suffix) };
        }
        let region = MappedRegion {
            base: base.cast(),
            size: total,
            fd,
        };
        for offset in (0..total).step_by(page_size) {
            // SAFETY: each offset is inside the writable mapping and touching
            // one byte faults in the selected HugeTLB page under MPOL_BIND.
            unsafe { region.base.add(offset).write_volatile(0) };
        }
        Ok(region)
    })();

    // SAFETY: these are the exact mode and nodemask returned by
    // get_mempolicy before the temporary bind.
    let restore = unsafe {
        libc::syscall(
            libc::SYS_set_mempolicy,
            previous_mode,
            previous_mask.as_ptr(),
            MAX_NUMA_NODES as libc::c_ulong,
        )
    };
    let region = match (mapping, restore) {
        (Err(primary), value) if value != 0 => {
            return Err(PhysmemError::NumaPolicyRestore {
                primary: Box::new(primary),
                source: io::Error::last_os_error(),
            });
        }
        (Err(primary), _) => return Err(primary),
        (Ok(_), value) if value != 0 => {
            return Err(PhysmemError::NumaPolicy {
                operation: "restore",
                numa_node,
                source: io::Error::last_os_error(),
            });
        }
        (Ok(region), _) => region,
    };

    let start = region.base() as u64;
    let end = start + region.size() as u64;
    let actual_page_size = Process::myself()
        .and_then(|process| process.smaps())
        .ok()
        .and_then(|maps| {
            maps.0
                .into_iter()
                .find(|mapping| mapping.address.0 <= start && mapping.address.1 >= end)
                .and_then(|mapping| mapping.extension.map.get("KernelPageSize").copied())
        })
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);
    if actual_page_size != page_size {
        return Err(PhysmemError::BackingVerification {
            requested: page_size,
            actual: actual_page_size,
        });
    }

    let pages = (0..region.size())
        .step_by(page_size)
        .map(|offset| unsafe { region.base().add(offset).cast::<libc::c_void>() })
        .collect::<Vec<_>>();
    let mut status = vec![-1 as libc::c_int; pages.len()];
    // SAFETY: `pages` and `status` have the same count and remain live for the
    // query. A null nodes array requests placement without moving pages.
    if unsafe {
        libc::syscall(
            libc::SYS_move_pages,
            0,
            pages.len(),
            pages.as_ptr(),
            std::ptr::null::<libc::c_int>(),
            status.as_mut_ptr(),
            0,
        )
    } != 0
    {
        return Err(PhysmemError::NumaPolicy {
            operation: "verify placement",
            numa_node,
            source: io::Error::last_os_error(),
        });
    }
    if let Some(actual) = status
        .into_iter()
        .find(|actual| *actual != numa_node as i32)
    {
        return Err(PhysmemError::PlacementVerification {
            requested: numa_node,
            actual,
        });
    }
    Ok(region)
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::fs;

    #[cfg(target_os = "linux")]
    use byte_unit::Byte;

    use super::*;

    #[test]
    fn ordinary_mapping_reports_actual_page_size() {
        let mapping = PhysmemMap::create("ordinary", 1, PageSize::Default, 0)
            .expect("create ordinary physmem mapping");

        assert_eq!(
            mapping.page_size(),
            PageSize::Default.bytes().expect("OS page size")
        );
        assert!(!mapping.is_hugetlb());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn hugepage_pool_growth_writes_only_the_shortfall_and_confirms_free_pages() {
        let directory = fixture_pool("growth", 1, 2);

        let error = provision_hugepages(
            &directory,
            PageSize::Bytes(Byte::from_u64(2 << 20)),
            2 << 20,
            3,
            3,
        )
        .expect_err("fixture cannot make written pages free");

        assert_eq!(
            fs::read_to_string(directory.join("nr_hugepages")).expect("read attempted pool size"),
            "4"
        );
        assert!(matches!(
            error,
            PhysmemError::HugePagePool {
                operation: "confirm",
                required: 3,
                free: 1,
                current: 4,
                attempted: Some(4),
                ..
            }
        ));
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn hugepage_pool_with_enough_free_pages_is_not_modified() {
        let directory = fixture_pool("no-growth", 4, 7);

        provision_hugepages(
            &directory,
            PageSize::Bytes(Byte::from_u64(2 << 20)),
            2 << 20,
            0,
            3,
        )
        .expect("existing free pages satisfy request");

        assert_eq!(
            fs::read_to_string(directory.join("nr_hugepages")).expect("read unchanged pool size"),
            "7"
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(target_os = "linux")]
    fn fixture_pool(name: &str, free: usize, current: usize) -> PathBuf {
        let counter = PHYSMEM_COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "hammer-hugetlb-{name}-{}-{counter}",
            std::process::id()
        ));
        fs::create_dir(&directory).expect("create fixture pool");
        fs::write(directory.join("free_hugepages"), free.to_string())
            .expect("write fixture free pages");
        fs::write(directory.join("nr_hugepages"), current.to_string())
            .expect("write fixture pool size");
        directory
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn explicit_hugepage_is_rejected_on_unsupported_platform() {
        let error = match PhysmemMap::create("huge", 1, PageSize::DefaultHuge, 0) {
            Err(error) => error,
            Ok(_) => panic!("explicit HugeTLB must fail"),
        };

        assert!(matches!(
            error,
            PhysmemError::UnsupportedPageSize {
                requested: PageSize::DefaultHuge
            }
        ));
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
