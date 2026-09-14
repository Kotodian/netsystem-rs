//! Fixed-address shared regions with VPP-style PVT and Data Heaps.
//!
//! A region owns one shared mapping whose first page contains
//! [`SvmRegionHeader`]. The next range contains a locked [`MemHeap`] used for
//! region metadata. Ordinary subregions may also contain a locked Data Heap.
//! Every attached process maps the region at the creator's address, so shared
//! Rust collections and dlmalloc mspace pointers retain the same value.

use std::collections::HashMap;
use std::ffi::c_void;
use std::fmt;
use std::io;
use std::mem::{MaybeUninit, size_of};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, OwnedFd};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU64, Ordering};

use posix_sync::condvar::{CondvarBuilder, CondvarClock, CondvarSharing, RawCondvarAlloc};
use posix_sync::mutex::guards::{RobustGuardContainer, StandardGuard};
use posix_sync::mutex::{
    BorrowedMutex, MutexBuilder, MutexLockError, MutexSharing, RawMutexAlloc,
    robustness_markers::Robust,
};

use crate::bitmap::Bitmap;
use crate::mem::{MemError, MemHeap, MemMain};
use crate::pool::Pool;

/// Shared layout version. 2.1 held the former process-local SVM queue handle.
pub const SVM_REGION_VERSION: u64 = (2 << 16) | 2;

/// Default size of the locked PVT Heap following the region header page.
pub const SVM_PVT_HEAP_SIZE: usize = 128 << 10;

/// Longest accepted region name.
pub const SVM_REGION_NAME_MAX_LENGTH: usize = 256;

const DATA_HEAP_FLAG: u64 = 1 << 0;
const NODATA_FLAG: u64 = 1 << 2;
const NEED_DATA_INIT_FLAG: u64 = 1 << 3;
const PUBLIC_FLAGS: u64 = DATA_HEAP_FLAG | NODATA_FLAG;

const LOCK_TAG_INIT: i32 = 1;
const LOCK_TAG_ATTACH: i32 = 2;
const LOCK_TAG_ROOT_INIT: i32 = 3;
const LOCK_TAG_SUBREGION: i32 = 4;
const LOCK_TAG_UNMAP: i32 = 5;
const LOCK_TAG_SCAN: i32 = 7;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmRegionFlags(u64);

impl SvmRegionFlags {
    /// An ordinary region with caller-owned data bytes and no Data Heap.
    pub const NONE: Self = Self(0);
    /// The ordinary region's data range contains a locked Data Heap.
    pub const DATA_HEAP: Self = Self(DATA_HEAP_FLAG);
    /// The region is a root whose remaining virtual range is subdivided.
    pub const NODATA: Self = Self(NODATA_FLAG);

    pub fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }

    fn bits(self) -> u64 {
        self.0
    }
}

#[derive(Debug)]
pub struct SvmRegionConfig {
    pub name: String,
    pub size: usize,
    pub pvt_heap_size: usize,
    pub flags: SvmRegionFlags,
}

impl SvmRegionConfig {
    fn map_size(
        &self,
        page_size: usize,
        include_region_overhead: bool,
    ) -> Result<(usize, usize), SvmRegionError> {
        let name_length = self.name.len();
        if name_length == 0
            || name_length > SVM_REGION_NAME_MAX_LENGTH
            || self.name.as_bytes().contains(&0)
        {
            return Err(SvmRegionError::InvalidName {
                length: name_length,
                maximum: SVM_REGION_NAME_MAX_LENGTH,
            });
        }

        let flag_bits = self.flags.bits();
        if flag_bits & !PUBLIC_FLAGS != 0
            || self.flags.contains(SvmRegionFlags::DATA_HEAP)
                && self.flags.contains(SvmRegionFlags::NODATA)
        {
            return Err(SvmRegionError::InvalidFlags { bits: flag_bits });
        }

        let pvt_heap_size = if self.pvt_heap_size == 0 {
            SVM_PVT_HEAP_SIZE
        } else {
            self.pvt_heap_size
        };
        if pvt_heap_size < page_size || !pvt_heap_size.is_multiple_of(page_size) {
            return Err(SvmRegionError::InvalidSize {
                requested: pvt_heap_size,
                minimum: page_size,
            });
        }

        let overhead =
            page_size
                .checked_add(pvt_heap_size)
                .ok_or(SvmRegionError::SizeOverflow {
                    requested: self.size,
                    alignment: page_size,
                })?;
        let requested = if include_region_overhead {
            self.size
                .checked_add(overhead)
                .ok_or(SvmRegionError::SizeOverflow {
                    requested: self.size,
                    alignment: page_size,
                })?
        } else {
            self.size
        };
        let minimum = overhead
            .checked_add(if self.flags.contains(SvmRegionFlags::DATA_HEAP) {
                page_size
            } else {
                0
            })
            .ok_or(SvmRegionError::SizeOverflow {
                requested: self.size,
                alignment: page_size,
            })?;
        if requested < minimum {
            return Err(SvmRegionError::InvalidSize {
                requested: self.size,
                minimum: if include_region_overhead {
                    minimum - overhead
                } else {
                    minimum
                },
            });
        }
        let size = requested
            .checked_add(page_size - 1)
            .map(|size| size & !(page_size - 1))
            .ok_or(SvmRegionError::SizeOverflow {
                requested: self.size,
                alignment: page_size,
            })?;
        Ok((size, pvt_heap_size))
    }
}

#[repr(C, align(64))]
pub(crate) struct SvmRegionHeader {
    version: AtomicU64,
    mutex: MaybeUninit<RawMutexAlloc>,
    condvar: MaybeUninit<RawCondvarAlloc>,
    mutex_owner_pid: AtomicI32,
    mutex_owner_tag: AtomicI32,
    flags: SvmRegionFlags,
    virtual_base: *mut u8,
    virtual_size: usize,
    pvt_heap: *mut MemHeap,
    data_base: *mut c_void,
    data_heap: *mut MemHeap,
    user_ctx: AtomicPtr<c_void>,
    bitmap_size: usize,
    bitmap: *mut Bitmap,
    region_name: *mut String,
    backing_file: *mut String,
    filenames: *mut Vec<String>,
    client_pids: *mut Vec<i32>,
}

#[derive(Debug)]
pub struct SvmSubregion {
    subregion_name: String,
}

#[derive(Debug)]
pub struct SvmMainRegion {
    subregions: Pool<SvmSubregion>,
    name_hash: HashMap<String, u32>,
}

impl SvmMainRegion {
    pub fn subregion_index(&self, name: &str) -> Option<u32> {
        self.name_hash.get(name).copied()
    }

    pub fn subregion(&self, index: u32) -> Option<&SvmSubregion> {
        self.subregions.get(index)
    }

    pub fn subregion_count(&self) -> usize {
        self.subregions.len()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SvmRegionError {
    #[error("region name has {length} bytes; expected 1..={maximum}")]
    InvalidName { length: usize, maximum: usize },
    #[error("region flags {bits:#x} are invalid")]
    InvalidFlags { bits: u64 },
    #[error("region size {requested} is smaller than {minimum}")]
    InvalidSize { requested: usize, minimum: usize },
    #[error("region size {requested} cannot be aligned to {alignment}")]
    SizeOverflow { requested: usize, alignment: usize },
    #[error("region base {base:#x} is not aligned to {alignment}")]
    MisalignedBase { base: usize, alignment: usize },
    #[error("region backing descriptor metadata is unavailable: {source}")]
    BackingMetadata {
        #[source]
        source: io::Error,
    },
    #[error("region backing descriptor cannot be resized to {size} bytes: {source}")]
    BackingResize {
        size: usize,
        #[source]
        source: io::Error,
    },
    #[error("region backing descriptor has {available} bytes; {required} are required")]
    BackingTooSmall { available: usize, required: usize },
    #[error("failed to probe the first {size} bytes of a region: {source}")]
    ProbeMapping {
        size: usize,
        #[source]
        source: io::Error,
    },
    #[error("region version {found:#x} is unsupported; expected {expected:#x}")]
    UnsupportedVersion { found: u64, expected: u64 },
    #[error("region version has not been published")]
    NotReady,
    #[error("region address range {base:#x}+{size} is occupied: {source}")]
    AddressRangeOccupied {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    #[error("failed to map region at {base:#x}+{size}: {source}")]
    FixedMapping {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    #[error("mapped region base {found:#x} differs from published base {expected:#x}")]
    VirtualBaseMismatch { found: usize, expected: usize },
    #[error("mapped region size {mapped} differs from published size {declared}")]
    VirtualSizeMismatch { mapped: usize, declared: usize },
    #[error(
        "region PVT Heap describes {found_base:#x}+{found_size}; expected base {expected_base:#x}"
    )]
    PvtHeapLayout {
        found_base: usize,
        found_size: usize,
        expected_base: usize,
    },
    #[error("region PVT Heap object pointer {pointer:#x} is invalid")]
    InvalidPvtObject { pointer: usize },
    #[error("region data base {pointer:#x} is outside {base:#x}+{size}")]
    InvalidDataBase {
        pointer: usize,
        base: usize,
        size: usize,
    },
    #[error("DATA_HEAP region has no Data Heap")]
    MissingDataHeap,
    #[error("region without DATA_HEAP published Data Heap {pointer:#x}")]
    UnexpectedDataHeap { pointer: usize },
    #[error(
        "region Data Heap describes {found_base:#x}+{found_size}; expected {expected_base:#x}+{expected_size}"
    )]
    DataHeapLayout {
        found_base: usize,
        found_size: usize,
        expected_base: usize,
        expected_size: usize,
    },
    #[error("failed to create the region PVT Heap: {source}")]
    PvtHeapCreation {
        #[source]
        source: MemError,
    },
    #[error("failed to create the region Data Heap: {source}")]
    DataHeapCreation {
        #[source]
        source: MemError,
    },
    #[error("operation requires a NODATA root region")]
    RootRequired,
    #[error("root region has no contiguous range of {requested} bytes in {available} bytes")]
    RootAddressSpaceExhausted { requested: usize, available: usize },
    #[error("subregion publishes size {found}; expected {expected}")]
    SubregionSizeMismatch { found: usize, expected: usize },
    #[error("subregion publishes flags {found:#x}; expected {expected:#x}")]
    SubregionFlagsMismatch { found: u64, expected: u64 },
    #[error("subregion backing publishes a different name")]
    SubregionNameMismatch,
    #[error("subregion range {base:#x}+{size} is outside root {root_base:#x}+{root_size}")]
    SubregionOutsideRoot {
        base: usize,
        size: usize,
        root_base: usize,
        root_size: usize,
    },
    #[error("subregion range {base:#x}+{size} is not reserved in the root bitmap")]
    SubregionNotReserved { base: usize, size: usize },
    #[error("pid {pid} is not registered in the region")]
    ClientNotRegistered { pid: i32 },
    #[error("failed to probe region client pid {pid}: {source}")]
    ClientProbe {
        pid: i32,
        #[source]
        source: io::Error,
    },
    #[error("region mutex owner pid {owner_pid} tag {owner_tag} died")]
    OwnerDied { owner_pid: i32, owner_tag: i32 },
    #[error("region mutex lock failed: {source}")]
    Lock {
        #[source]
        source: MutexLockError,
    },
    #[error("failed to release local region mapping {base:#x}+{size}: {source}")]
    Unmapping {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
}

pub struct SvmRegion {
    backing: OwnedFd,
    base: NonNull<u8>,
    size: usize,
    header: NonNull<SvmRegionHeader>,
    root_header: Option<NonNull<SvmRegionHeader>>,
    is_client: bool,
}

unsafe impl Send for SvmRegion {}
unsafe impl Sync for SvmRegion {}

impl fmt::Debug for SvmRegion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SvmRegion")
            .field("base", &self.base)
            .field("size", &self.size)
            .field("is_client", &self.is_client)
            .finish_non_exhaustive()
    }
}

pub struct RegionLock<'region> {
    header: NonNull<SvmRegionHeader>,
    guard: StandardGuard<'region>,
}

impl RegionLock<'_> {
    /// Makes initialized region-owned state visible to attaching clients.
    pub fn set_user_context(&mut self, value: NonNull<u8>) {
        unsafe { self.header.as_ref() }
            .user_ctx
            .store(value.as_ptr().cast(), Ordering::Release);
    }

    pub fn remove_exited_clients(&mut self) -> Result<usize, SvmRegionError> {
        let active_heap = self.pvt_heap().activate();
        let clients = unsafe { &mut *self.header.as_ref().client_pids };
        let current_pid = std::process::id() as i32;
        let mut exited = Vec::new();
        for (index, pid) in clients.iter().copied().enumerate() {
            if pid != current_pid && process_is_dead(pid)? {
                exited.push(index);
            }
        }
        for index in exited.iter().rev().copied() {
            clients.remove(index);
        }
        let removed = exited.len();
        drop(exited);
        drop(active_heap);
        Ok(removed)
    }

    pub fn pvt_heap(&self) -> &MemHeap {
        let pointer = unsafe { self.header.as_ref().pvt_heap };
        assert!(!pointer.is_null(), "validated region has a PVT Heap");
        unsafe { &*pointer }
    }

    pub fn data_heap(&self) -> Option<&MemHeap> {
        let pointer = unsafe { self.header.as_ref().data_heap };
        unsafe { pointer.as_ref() }
    }

    pub fn main_region(&self) -> Option<&SvmMainRegion> {
        let header = unsafe { self.header.as_ref() };
        if !header.flags.contains(SvmRegionFlags::NODATA) {
            return None;
        }
        unsafe { header.data_base.cast::<SvmMainRegion>().as_ref() }
    }

    pub fn main_region_mut(&mut self) -> Option<&mut SvmMainRegion> {
        let header = unsafe { self.header.as_mut() };
        if !header.flags.contains(SvmRegionFlags::NODATA) {
            return None;
        }
        unsafe { header.data_base.cast::<SvmMainRegion>().as_mut() }
    }
}

impl Drop for RegionLock<'_> {
    fn drop(&mut self) {
        let header = unsafe { self.header.as_ref() };
        header.mutex_owner_tag.store(0, Ordering::Relaxed);
        header.mutex_owner_pid.store(0, Ordering::Release);
        let _ = &self.guard;
    }
}

impl SvmRegion {
    pub fn create(
        base: NonZeroUsize,
        config: &SvmRegionConfig,
        backing: OwnedFd,
    ) -> Result<Self, SvmRegionError> {
        let page_size = MemMain::system_page_size();
        let (size, pvt_heap_size) = config.map_size(page_size, false)?;
        if !base.get().is_multiple_of(page_size) {
            return Err(SvmRegionError::MisalignedBase {
                base: base.get(),
                alignment: page_size,
            });
        }
        resize_backing(&backing, size)?;
        let mapped = map_fixed_backing(&backing, base.get(), size, 0)?;
        let header = mapped.cast::<SvmRegionHeader>();

        unsafe {
            ptr::write(
                header.as_ptr(),
                SvmRegionHeader {
                    version: AtomicU64::new(0),
                    mutex: MaybeUninit::uninit(),
                    condvar: MaybeUninit::uninit(),
                    mutex_owner_pid: AtomicI32::new(0),
                    mutex_owner_tag: AtomicI32::new(0),
                    flags: SvmRegionFlags(
                        config.flags.bits()
                            | if config.flags.contains(SvmRegionFlags::NODATA) {
                                NEED_DATA_INIT_FLAG
                            } else {
                                0
                            },
                    ),
                    virtual_base: mapped.as_ptr(),
                    virtual_size: size,
                    pvt_heap: ptr::null_mut(),
                    data_base: ptr::null_mut(),
                    data_heap: ptr::null_mut(),
                    user_ctx: AtomicPtr::new(ptr::null_mut()),
                    bitmap_size: 0,
                    bitmap: ptr::null_mut(),
                    region_name: ptr::null_mut(),
                    backing_file: ptr::null_mut(),
                    filenames: ptr::null_mut(),
                    client_pids: ptr::null_mut(),
                },
            );

            MutexBuilder::<Robust>::new()
                .with_sharing(MutexSharing::Shared)
                .build_borrowed(ptr::addr_of_mut!((*header.as_ptr()).mutex).cast(), &backing);
            CondvarBuilder::new()
                .with_sharing(CondvarSharing::Shared)
                .with_clock(CondvarClock::Monotonic)
                .build_borrowed(
                    ptr::addr_of_mut!((*header.as_ptr()).condvar).cast(),
                    &backing,
                );
        }

        let mut region = Self {
            backing,
            base: mapped,
            size,
            header,
            root_header: None,
            is_client: false,
        };
        if let Err(error) = region.initialize_mapped_region(config, pvt_heap_size, page_size) {
            let header = unsafe { region.header.as_ref() };
            if let Some(data_heap) = unsafe { header.data_heap.as_ref() } {
                unsafe { data_heap.destroy() };
            }
            if let Some(pvt_heap) = unsafe { header.pvt_heap.as_ref() } {
                unsafe { pvt_heap.destroy() };
            }
            if unsafe { libc::munmap(region.base.as_ptr().cast(), region.size) } != 0 {
                std::process::abort();
            }
            region.size = 0;
            return Err(error);
        }
        Ok(region)
    }

    pub fn attach(backing: OwnedFd) -> Result<Self, SvmRegionError> {
        Self::map_region(backing, None)
    }

    pub fn base(&self) -> NonNull<u8> {
        self.base
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn user_context(&self) -> Option<NonNull<u8>> {
        NonNull::new(
            unsafe { self.header.as_ref() }
                .user_ctx
                .load(Ordering::Acquire)
                .cast(),
        )
    }

    pub fn contains_range(&self, start: NonNull<u8>, bytes: usize) -> bool {
        let base = self.base.as_ptr().addr();
        let Some(end) = base.checked_add(self.size) else {
            return false;
        };
        let start = start.as_ptr().addr();
        start >= base && start.checked_add(bytes).is_some_and(|last| last <= end)
    }

    pub fn flags(&self) -> SvmRegionFlags {
        unsafe { self.header.as_ref().flags }
    }

    pub fn lock(&self) -> Result<RegionLock<'_>, SvmRegionError> {
        self.lock_header(self.header, LOCK_TAG_SCAN)
    }

    pub fn client_count(&self) -> Result<usize, SvmRegionError> {
        let lock = self.lock()?;
        let pvt_heap = lock.pvt_heap() as *const MemHeap;
        let active_heap = unsafe { &*pvt_heap }.activate();
        let count = unsafe { (&*self.header.as_ref().client_pids).len() };
        drop(active_heap);
        drop(lock);
        Ok(count)
    }

    pub fn remove_exited_clients(&self) -> Result<usize, SvmRegionError> {
        self.lock()?.remove_exited_clients()
    }

    pub fn find_or_create_subregion(
        &mut self,
        config: &SvmRegionConfig,
        backing: OwnedFd,
    ) -> Result<Self, SvmRegionError> {
        if config.flags.contains(SvmRegionFlags::NODATA) {
            return Err(SvmRegionError::InvalidFlags {
                bits: config.flags.bits(),
            });
        }

        let page_size = MemMain::system_page_size();
        let (mapped_size, pvt_heap_size) = config.map_size(page_size, true)?;

        let mut root_lock = self.lock_header(self.header, LOCK_TAG_SUBREGION)?;
        if !unsafe { root_lock.header.as_ref().flags }.contains(SvmRegionFlags::NODATA) {
            return Err(SvmRegionError::RootRequired);
        }
        let root_pvt_heap = root_lock.pvt_heap() as *const MemHeap;
        let root_pvt_heap_size = unsafe { &*root_pvt_heap }.size();
        let active_heap = unsafe { &*root_pvt_heap }.activate();
        let root_main = root_lock
            .main_region_mut()
            .expect("validated NODATA region has SvmMainRegion");

        if root_main.name_hash.contains_key(config.name.as_str()) {
            let (base, size) = probe_backing(&backing, page_size)?;
            let root_base = self.base.as_ptr().addr();
            let root_end = root_base + self.size;
            let end = base
                .checked_add(size)
                .ok_or(SvmRegionError::SubregionOutsideRoot {
                    base,
                    size,
                    root_base,
                    root_size: self.size,
                })?;
            if base < root_base || end > root_end || !base.is_multiple_of(page_size) {
                return Err(SvmRegionError::SubregionOutsideRoot {
                    base,
                    size,
                    root_base,
                    root_size: self.size,
                });
            }
            let first_page = (base - root_base) / page_size;
            let page_count = size / page_size;
            let bitmap = unsafe { &*self.header.as_ref().bitmap };
            if !(first_page..first_page + page_count).all(|page| bitmap.is_set(page)) {
                return Err(SvmRegionError::SubregionNotReserved { base, size });
            }
            if unsafe { libc::munmap(base as *mut c_void, size) } != 0 {
                return Err(SvmRegionError::Unmapping {
                    base,
                    size,
                    source: io::Error::last_os_error(),
                });
            }

            let mut region = match Self::map_region(backing, Some(config)) {
                Ok(region) => region,
                Err(error) => {
                    if map_fixed_backing(&self.backing, base, size, (base - root_base) as u64)
                        .is_err()
                    {
                        std::process::abort();
                    }
                    return Err(error);
                }
            };
            region.root_header = Some(self.header);
            drop(active_heap);
            drop(root_lock);
            return Ok(region);
        }

        resize_backing(&backing, mapped_size)?;
        let bitmap = unsafe { &mut *self.header.as_ref().bitmap };
        let overhead_pages = (page_size + root_pvt_heap_size) / page_size;
        let required_pages = mapped_size / page_size;
        let bitmap_size = unsafe { self.header.as_ref().bitmap_size };
        let mut page_index = overhead_pages;
        let mut available_page = None;
        while page_index + required_pages <= bitmap_size {
            match (page_index..page_index + required_pages).find(|page| bitmap.is_set(*page)) {
                Some(occupied_page) => page_index = occupied_page + 1,
                None => {
                    available_page = Some(page_index);
                    break;
                }
            }
        }
        let Some(page_index) = available_page else {
            return Err(SvmRegionError::RootAddressSpaceExhausted {
                requested: mapped_size,
                available: self.size,
            });
        };
        for page in page_index..page_index + required_pages {
            assert!(bitmap.set(page), "root bitmap reserves clear pages");
        }
        let subregion_base = self
            .base
            .as_ptr()
            .addr()
            .checked_add(page_index * page_size)
            .expect("validated root range address fits usize");
        if unsafe { libc::munmap(subregion_base as *mut c_void, mapped_size) } != 0 {
            for page in page_index..page_index + required_pages {
                assert!(bitmap.clear(page), "root bitmap releases reserved pages");
            }
            return Err(SvmRegionError::Unmapping {
                base: subregion_base,
                size: mapped_size,
                source: io::Error::last_os_error(),
            });
        }

        let mapped_config = SvmRegionConfig {
            name: config.name.clone(),
            size: mapped_size,
            pvt_heap_size,
            flags: config.flags,
        };
        let base = NonZeroUsize::new(subregion_base).expect("root base is nonzero");
        let mut region = match Self::create(base, &mapped_config, backing) {
            Ok(region) => region,
            Err(error) => {
                for page in page_index..page_index + required_pages {
                    assert!(bitmap.clear(page), "root bitmap releases reserved pages");
                }
                if map_fixed_backing(
                    &self.backing,
                    subregion_base,
                    mapped_size,
                    (subregion_base - self.base.as_ptr().addr()) as u64,
                )
                .is_err()
                {
                    std::process::abort();
                }
                return Err(error);
            }
        };

        let subregion_name = config.name.clone();
        let index = root_main.subregions.insert(SvmSubregion {
            subregion_name: subregion_name.clone(),
        });
        assert!(
            root_main.name_hash.insert(subregion_name, index).is_none(),
            "new subregion name inserts once"
        );
        region.root_header = Some(self.header);
        drop(mapped_config);
        drop(active_heap);
        drop(root_lock);
        Ok(region)
    }

    pub fn unmap(mut self) -> Result<(), SvmRegionError> {
        if let Err(error) = self.remove_client() {
            if self.unmap_local().is_err() {
                std::process::abort();
            }
            return Err(error);
        }

        self.unmap_local()
    }

    fn initialize_mapped_region(
        &mut self,
        config: &SvmRegionConfig,
        pvt_heap_size: usize,
        page_size: usize,
    ) -> Result<(), SvmRegionError> {
        let mut header = self.header;
        let lock = self.lock_header(header, LOCK_TAG_INIT)?;
        let pvt_base = NonNull::new(unsafe { self.base.as_ptr().add(page_size) })
            .expect("mapped region base plus one page is non-null");
        let pvt_heap = unsafe {
            MemHeap::create_at(pvt_base, pvt_heap_size, true, "svm region")
                .map_err(|source| SvmRegionError::PvtHeapCreation { source })?
        };
        unsafe { header.as_mut().pvt_heap = pvt_heap.as_ptr() };

        let active_heap = unsafe { pvt_heap.as_ref() }.activate();
        let data_base = unsafe { pvt_base.as_ptr().add(pvt_heap_size) };
        unsafe { header.as_mut().data_base = data_base.cast() };
        if config.flags.contains(SvmRegionFlags::DATA_HEAP) {
            let data_size = self.size - page_size - pvt_heap_size;
            let data_heap = unsafe {
                MemHeap::create_at(
                    NonNull::new(data_base).expect("validated data range is non-null"),
                    data_size,
                    true,
                    "svm data",
                )
                .map_err(|source| SvmRegionError::DataHeapCreation { source })?
            };
            unsafe { header.as_mut().data_heap = data_heap.as_ptr() };
        }

        let region_pages = self.size / page_size;
        let overhead_pages = (page_size + pvt_heap_size) / page_size;
        let mut bitmap = Box::new(Bitmap::with_capacity(region_pages));
        for page in 0..overhead_pages {
            assert!(bitmap.set(page), "region overhead pages begin clear");
        }
        let region_name = Box::new(config.name.clone());
        let filenames = Box::new(Vec::new());
        let client_pids = Box::new(vec![std::process::id() as i32]);

        unsafe {
            let shared = header.as_mut();
            shared.bitmap_size = region_pages;
            shared.bitmap = Box::into_raw(bitmap);
            shared.region_name = Box::into_raw(region_name);
            shared.filenames = Box::into_raw(filenames);
            shared.client_pids = Box::into_raw(client_pids);
        }

        if config.flags.contains(SvmRegionFlags::NODATA) {
            let main_region = Box::new(SvmMainRegion {
                subregions: Pool::new(),
                name_hash: HashMap::new(),
            });
            unsafe {
                let shared = header.as_mut();
                shared.data_base = Box::into_raw(main_region).cast();
                shared.flags = SvmRegionFlags(shared.flags.bits() & !NEED_DATA_INIT_FLAG);
            }
            unsafe {
                header
                    .as_ref()
                    .mutex_owner_tag
                    .store(LOCK_TAG_ROOT_INIT, Ordering::Relaxed);
            }
        }

        unsafe {
            header
                .as_ref()
                .version
                .store(SVM_REGION_VERSION, Ordering::Release);
        }
        drop(active_heap);
        drop(lock);
        Ok(())
    }

    fn map_region(
        backing: OwnedFd,
        expected: Option<&SvmRegionConfig>,
    ) -> Result<Self, SvmRegionError> {
        let page_size = MemMain::system_page_size();
        let (base, size) = probe_backing(&backing, page_size)?;
        let mapped = map_fixed_backing(&backing, base, size, 0)?;
        let header = mapped.cast::<SvmRegionHeader>();
        let mut region = Self {
            backing,
            base: mapped,
            size,
            header,
            root_header: None,
            is_client: true,
        };

        let result = (|| {
            let header = unsafe { region.header.as_ref() };
            let version = header.version.load(Ordering::Acquire);
            if version == 0 {
                return Err(SvmRegionError::NotReady);
            }
            if version != SVM_REGION_VERSION {
                return Err(SvmRegionError::UnsupportedVersion {
                    found: version,
                    expected: SVM_REGION_VERSION,
                });
            }
            if header.virtual_base != region.base.as_ptr() {
                return Err(SvmRegionError::VirtualBaseMismatch {
                    found: region.base.as_ptr().addr(),
                    expected: header.virtual_base.addr(),
                });
            }
            if header.virtual_size != region.size {
                return Err(SvmRegionError::VirtualSizeMismatch {
                    mapped: region.size,
                    declared: header.virtual_size,
                });
            }
            let flag_bits = header.flags.bits();
            if flag_bits & !PUBLIC_FLAGS != 0
                || header.flags.contains(SvmRegionFlags::DATA_HEAP)
                    && header.flags.contains(SvmRegionFlags::NODATA)
            {
                return Err(SvmRegionError::InvalidFlags { bits: flag_bits });
            }

            let lock = region.lock_header(region.header, LOCK_TAG_ATTACH)?;
            let expected_pvt_base = region.base.as_ptr().addr() + page_size;
            let region_end = region.base.as_ptr().addr() + region.size;
            let pvt_heap = header.pvt_heap;
            let pvt_heap_address = pvt_heap.addr();
            if pvt_heap_address < expected_pvt_base
                || pvt_heap_address
                    .checked_add(size_of::<MemHeap>())
                    .is_none_or(|control_end| control_end > region_end)
            {
                return Err(SvmRegionError::PvtHeapLayout {
                    found_base: pvt_heap_address,
                    found_size: 0,
                    expected_base: expected_pvt_base,
                });
            }
            let pvt_heap_ref = unsafe { &*pvt_heap };
            if pvt_heap_ref.base().as_ptr().addr() != expected_pvt_base
                || pvt_heap_ref.size() < page_size
                || !pvt_heap_ref.size().is_multiple_of(page_size)
                || expected_pvt_base
                    .checked_add(pvt_heap_ref.size())
                    .is_none_or(|pvt_end| pvt_end > region_end)
            {
                return Err(SvmRegionError::PvtHeapLayout {
                    found_base: pvt_heap_ref.base().as_ptr().addr(),
                    found_size: pvt_heap_ref.size(),
                    expected_base: expected_pvt_base,
                });
            }
            for pointer in [
                header.pvt_heap.cast::<u8>(),
                header.bitmap.cast::<u8>(),
                header.region_name.cast::<u8>(),
                header.filenames.cast::<u8>(),
                header.client_pids.cast::<u8>(),
            ] {
                let Some(pointer) = NonNull::new(pointer) else {
                    return Err(SvmRegionError::InvalidPvtObject { pointer: 0 });
                };
                if !pvt_heap_ref.is_heap_object(pointer) {
                    return Err(SvmRegionError::InvalidPvtObject {
                        pointer: pointer.as_ptr().addr(),
                    });
                }
            }
            if let Some(backing_file) = NonNull::new(header.backing_file.cast::<u8>())
                && !pvt_heap_ref.is_heap_object(backing_file)
            {
                return Err(SvmRegionError::InvalidPvtObject {
                    pointer: backing_file.as_ptr().addr(),
                });
            }

            let expected_data_base = expected_pvt_base + pvt_heap_ref.size();
            if header.flags.contains(SvmRegionFlags::NODATA) {
                if !header.data_heap.is_null() {
                    return Err(SvmRegionError::UnexpectedDataHeap {
                        pointer: header.data_heap.addr(),
                    });
                }
                let Some(data_base) = NonNull::new(header.data_base.cast()) else {
                    return Err(SvmRegionError::InvalidDataBase {
                        pointer: 0,
                        base: expected_pvt_base,
                        size: pvt_heap_ref.size(),
                    });
                };
                if !pvt_heap_ref.is_heap_object(data_base) {
                    return Err(SvmRegionError::InvalidDataBase {
                        pointer: data_base.as_ptr().addr(),
                        base: expected_pvt_base,
                        size: pvt_heap_ref.size(),
                    });
                }
            } else if header.data_base.addr() != expected_data_base {
                return Err(SvmRegionError::InvalidDataBase {
                    pointer: header.data_base.addr(),
                    base: expected_data_base,
                    size: region_end - expected_data_base,
                });
            } else if header.flags.contains(SvmRegionFlags::DATA_HEAP) {
                let data_heap_address = header.data_heap.addr();
                if data_heap_address < expected_data_base
                    || data_heap_address
                        .checked_add(size_of::<MemHeap>())
                        .is_none_or(|control_end| control_end > region_end)
                {
                    return Err(SvmRegionError::DataHeapLayout {
                        found_base: data_heap_address,
                        found_size: 0,
                        expected_base: expected_data_base,
                        expected_size: region_end - expected_data_base,
                    });
                }
                let Some(data_heap) = (unsafe { header.data_heap.as_ref() }) else {
                    return Err(SvmRegionError::MissingDataHeap);
                };
                if data_heap.base().as_ptr().addr() != expected_data_base
                    || data_heap.size() != region_end - expected_data_base
                {
                    return Err(SvmRegionError::DataHeapLayout {
                        found_base: data_heap.base().as_ptr().addr(),
                        found_size: data_heap.size(),
                        expected_base: expected_data_base,
                        expected_size: region_end - expected_data_base,
                    });
                }
            } else if !header.data_heap.is_null() {
                return Err(SvmRegionError::UnexpectedDataHeap {
                    pointer: header.data_heap.addr(),
                });
            }

            let active_heap = unsafe { &*pvt_heap }.activate();
            if let Some(config) = expected {
                let (expected_size, _) = config.map_size(page_size, true)?;
                let name = unsafe { &*header.region_name };
                if name != &config.name {
                    return Err(SvmRegionError::SubregionNameMismatch);
                }
                if region.size != expected_size {
                    return Err(SvmRegionError::SubregionSizeMismatch {
                        found: region.size,
                        expected: expected_size,
                    });
                }
                if header.flags != config.flags {
                    return Err(SvmRegionError::SubregionFlagsMismatch {
                        found: header.flags.bits(),
                        expected: config.flags.bits(),
                    });
                }
            }
            unsafe {
                (&mut *region.header.as_ref().client_pids).push(std::process::id() as i32);
            }
            drop(active_heap);
            drop(lock);
            Ok(())
        })();
        if let Err(error) = result {
            if unsafe { libc::munmap(region.base.as_ptr().cast(), region.size) } != 0 {
                std::process::abort();
            }
            region.size = 0;
            return Err(error);
        }
        Ok(region)
    }

    fn lock_header<'region>(
        &'region self,
        header: NonNull<SvmRegionHeader>,
        tag: i32,
    ) -> Result<RegionLock<'region>, SvmRegionError> {
        let shared = unsafe { header.as_ref() };
        let owner_pid = shared.mutex_owner_pid.load(Ordering::Acquire);
        let owner_tag = shared.mutex_owner_tag.load(Ordering::Relaxed);
        if owner_pid != 0 && process_is_dead(owner_pid)? {
            return Err(SvmRegionError::OwnerDied {
                owner_pid,
                owner_tag,
            });
        }

        let mutex = unsafe {
            BorrowedMutex::<Robust>::from_raw(
                ptr::addr_of!((*header.as_ptr()).mutex).cast_mut().cast(),
                self,
            )
        };
        match unsafe { mutex.lock() } {
            Ok(RobustGuardContainer::Standard(guard)) => {
                shared.mutex_owner_tag.store(tag, Ordering::Relaxed);
                shared
                    .mutex_owner_pid
                    .store(std::process::id() as i32, Ordering::Release);
                // The guard borrows a Copy BorrowedMutex local, but that value
                // never owns the pthread mutex. The mapped header outlives the
                // returned borrow of `self`, which is the real guard lifetime.
                let guard = unsafe {
                    std::mem::transmute::<StandardGuard<'_>, StandardGuard<'region>>(guard)
                };
                Ok(RegionLock { header, guard })
            }
            Ok(RobustGuardContainer::Indeterminate(guard)) => {
                let owner_pid = shared.mutex_owner_pid.load(Ordering::Acquire);
                let owner_tag = shared.mutex_owner_tag.load(Ordering::Relaxed);
                drop(guard);
                Err(SvmRegionError::OwnerDied {
                    owner_pid,
                    owner_tag,
                })
            }
            Err(MutexLockError::NotRecoverable) => Err(SvmRegionError::OwnerDied {
                owner_pid: shared.mutex_owner_pid.load(Ordering::Acquire),
                owner_tag: shared.mutex_owner_tag.load(Ordering::Relaxed),
            }),
            Err(source) => Err(SvmRegionError::Lock { source }),
        }
    }

    fn remove_client(&self) -> Result<(), SvmRegionError> {
        let mut root_lock = match self.root_header {
            Some(root_header) => {
                let lock = self.lock_header(root_header, LOCK_TAG_UNMAP)?;
                if !unsafe { lock.header.as_ref().flags }.contains(SvmRegionFlags::NODATA) {
                    return Err(SvmRegionError::RootRequired);
                }
                Some(lock)
            }
            None => None,
        };
        let region_lock = self.lock_header(self.header, LOCK_TAG_UNMAP)?;
        let pvt_heap = region_lock.pvt_heap() as *const MemHeap;
        let active_heap = unsafe { &*pvt_heap }.activate();
        let clients = unsafe { &mut *self.header.as_ref().client_pids };
        let pid = std::process::id() as i32;
        let Some(client_index) = clients.iter().position(|client| *client == pid) else {
            return Err(SvmRegionError::ClientNotRegistered { pid });
        };
        clients.remove(client_index);
        let last_client = clients.is_empty();
        drop(active_heap);

        if last_client && let Some(root_lock) = root_lock.as_mut() {
            let root_pvt_heap = root_lock.pvt_heap() as *const MemHeap;
            let root_active_heap = unsafe { &*root_pvt_heap }.activate();
            let name = unsafe { (&*self.header.as_ref().region_name).clone() };
            let root_main = root_lock
                .main_region_mut()
                .expect("NODATA root has SvmMainRegion");
            let index = root_main
                .name_hash
                .remove(name.as_str())
                .expect("mapped subregion name is registered in root");
            let subregion = root_main
                .subregions
                .remove(index)
                .expect("mapped subregion pool index is occupied");
            assert_eq!(subregion.subregion_name, name);
            drop(subregion);

            let root_header = unsafe { root_lock.header.as_ref() };
            let page_size = MemMain::system_page_size();
            let first_page =
                (self.base.as_ptr().addr() - root_header.virtual_base.addr()) / page_size;
            let page_count = self.size / page_size;
            let bitmap = unsafe { &mut *root_header.bitmap };
            for page in first_page..first_page + page_count {
                assert!(bitmap.clear(page), "root bitmap releases reserved pages");
            }
            drop(name);
            drop(root_active_heap);
        }

        drop(region_lock);
        drop(root_lock);
        Ok(())
    }

    fn unmap_local(&mut self) -> Result<(), SvmRegionError> {
        if self.size == 0 {
            return Ok(());
        }
        let base = self.base.as_ptr().addr();
        let size = self.size;
        if unsafe { libc::munmap(self.base.as_ptr().cast(), size) } != 0 {
            return Err(SvmRegionError::Unmapping {
                base,
                size,
                source: io::Error::last_os_error(),
            });
        }
        self.size = 0;
        Ok(())
    }
}

impl Drop for SvmRegion {
    fn drop(&mut self) {
        if self.size != 0 && self.unmap_local().is_err() {
            std::process::abort();
        }
    }
}

fn resize_backing(backing: &OwnedFd, size: usize) -> Result<(), SvmRegionError> {
    let length = libc::off_t::try_from(size).map_err(|_| SvmRegionError::SizeOverflow {
        requested: size,
        alignment: MemMain::system_page_size(),
    })?;
    if unsafe { libc::ftruncate(backing.as_raw_fd(), length) } != 0 {
        return Err(SvmRegionError::BackingResize {
            size,
            source: io::Error::last_os_error(),
        });
    }
    Ok(())
}

fn probe_backing(backing: &OwnedFd, page_size: usize) -> Result<(usize, usize), SvmRegionError> {
    let mut status = MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(backing.as_raw_fd(), status.as_mut_ptr()) } != 0 {
        return Err(SvmRegionError::BackingMetadata {
            source: io::Error::last_os_error(),
        });
    }
    let available = usize::try_from(unsafe { status.assume_init() }.st_size).map_err(|_| {
        SvmRegionError::BackingMetadata {
            source: io::Error::new(io::ErrorKind::InvalidData, "negative backing size"),
        }
    })?;
    if available < page_size {
        return Err(SvmRegionError::BackingTooSmall {
            available,
            required: page_size,
        });
    }
    let probe = unsafe {
        libc::mmap(
            ptr::null_mut(),
            page_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            backing.as_raw_fd(),
            0,
        )
    };
    if probe == libc::MAP_FAILED {
        return Err(SvmRegionError::ProbeMapping {
            size: page_size,
            source: io::Error::last_os_error(),
        });
    }
    let header = probe.cast::<SvmRegionHeader>();
    let result = (|| {
        let version = unsafe { (*header).version.load(Ordering::Acquire) };
        if version == 0 {
            return Err(SvmRegionError::NotReady);
        }
        if version != SVM_REGION_VERSION {
            return Err(SvmRegionError::UnsupportedVersion {
                found: version,
                expected: SVM_REGION_VERSION,
            });
        }
        let base = unsafe { (*header).virtual_base.addr() };
        let size = unsafe { (*header).virtual_size };
        if !base.is_multiple_of(page_size) {
            return Err(SvmRegionError::MisalignedBase {
                base,
                alignment: page_size,
            });
        }
        if size < page_size || !size.is_multiple_of(page_size) {
            return Err(SvmRegionError::InvalidSize {
                requested: size,
                minimum: page_size,
            });
        }
        if available < size {
            return Err(SvmRegionError::BackingTooSmall {
                available,
                required: size,
            });
        }
        let owner_pid = unsafe { (*header).mutex_owner_pid.load(Ordering::Acquire) };
        let owner_tag = unsafe { (*header).mutex_owner_tag.load(Ordering::Relaxed) };
        if owner_pid != 0 && process_is_dead(owner_pid)? {
            return Err(SvmRegionError::OwnerDied {
                owner_pid,
                owner_tag,
            });
        }
        Ok((base, size))
    })();
    if unsafe { libc::munmap(probe, page_size) } != 0 {
        std::process::abort();
    }
    result
}

fn map_fixed_backing(
    backing: &OwnedFd,
    base: usize,
    size: usize,
    offset: u64,
) -> Result<NonNull<u8>, SvmRegionError> {
    #[cfg(target_os = "linux")]
    let reservation_flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED_NOREPLACE;
    #[cfg(not(target_os = "linux"))]
    let reservation_flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
    let reservation = unsafe {
        libc::mmap(
            base as *mut c_void,
            size,
            libc::PROT_NONE,
            reservation_flags,
            -1,
            0,
        )
    };
    if reservation == libc::MAP_FAILED {
        let source = io::Error::last_os_error();
        if source.raw_os_error() == Some(libc::EEXIST) {
            return Err(SvmRegionError::AddressRangeOccupied { base, size, source });
        }
        return Err(SvmRegionError::FixedMapping { base, size, source });
    }
    if reservation.addr() != base {
        if unsafe { libc::munmap(reservation, size) } != 0 {
            std::process::abort();
        }
        return Err(SvmRegionError::AddressRangeOccupied {
            base,
            size,
            source: io::Error::new(io::ErrorKind::AddrInUse, "fixed range is unavailable"),
        });
    }
    let offset = libc::off_t::try_from(offset).map_err(|_| SvmRegionError::FixedMapping {
        base,
        size,
        source: io::Error::new(
            io::ErrorKind::InvalidInput,
            "mapping offset does not fit off_t",
        ),
    })?;
    let mapped = unsafe {
        libc::mmap(
            reservation,
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_FIXED,
            backing.as_raw_fd(),
            offset,
        )
    };
    if mapped == libc::MAP_FAILED {
        let source = io::Error::last_os_error();
        if unsafe { libc::munmap(reservation, size) } != 0 {
            std::process::abort();
        }
        return Err(SvmRegionError::FixedMapping { base, size, source });
    }
    Ok(NonNull::new(mapped.cast()).expect("successful fixed mmap is non-null"))
}

fn process_is_dead(pid: i32) -> Result<bool, SvmRegionError> {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(false);
    }
    let source = io::Error::last_os_error();
    match source.raw_os_error() {
        Some(libc::ESRCH) => Ok(true),
        Some(libc::EPERM) => Ok(false),
        _ => Err(SvmRegionError::ClientProbe { pid, source }),
    }
}
