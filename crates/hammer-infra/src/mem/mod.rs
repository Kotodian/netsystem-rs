//! VPP-shaped process Main Heap and allocator-owned thread state.
//!
//! `MemMain` is the process allocation authority. Rust's global allocator uses
//! `System` until the Main Heap is published, then uses the current
//! thread's active `MemHeap`. The selector is a const-initialized Rust
//! `thread_local!` containing only the four fields required by ADR-0012.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::UnsafeCell;
use std::ffi::{CStr, c_int, c_void};
use std::io;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

use byte_unit::Byte;

pub const DEFAULT_MAIN_HEAP_SIZE: usize = 1 << 30;

const PAGE_SIZE_UNKNOWN: u8 = 0;
const MIN_HEAP_ALIGNMENT: usize = 1 << 3;
const RESERVED_THREAD_INDEX: u32 = u32::MAX;
const MAP_HUGE_SHIFT: u32 = 26;
const MFD_CLOEXEC: c_int = 0x0001;
const MFD_ALLOW_SEALING: c_int = 0x0002;
const MFD_HUGETLB: c_int = 0x0004;
const F_ADD_SEALS: c_int = 1024 + 9;
const F_SEAL_SHRINK: c_int = 0x0002;
const MPOL_DEFAULT: c_int = 0;
const MPOL_PREFERRED: c_int = 1;
const MPOL_BIND: c_int = 2;
const MPOL_F_MEMS_ALLOWED: usize = 0x4;

type Mspace = *mut c_void;

unsafe extern "C" {
    fn create_mspace_with_base(base: *mut c_void, capacity: usize, locked: i32) -> Mspace;
    fn destroy_mspace(mspace: Mspace) -> usize;
    fn mspace_disable_expand(mspace: Mspace);
    fn mspace_memalign(mspace: Mspace, alignment: usize, size: usize) -> *mut c_void;
    fn mspace_realloc_in_place(mspace: Mspace, pointer: *mut c_void, size: usize) -> *mut c_void;
    fn mspace_free(mspace: Mspace, pointer: *mut c_void);
    fn mspace_usable_size(pointer: *const c_void) -> usize;
    fn mspace_is_heap_object(mspace: Mspace, pointer: *mut c_void) -> c_int;
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    parse_display::Display,
    parse_display::FromStr,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
pub enum PageSize {
    #[display("default")]
    Default,
    #[display("default-hugepage")]
    DefaultHuge,
    #[display("{0}")]
    Bytes(Byte),
}

impl PageSize {
    pub fn bytes(self) -> io::Result<usize> {
        match self {
            Self::Default => {
                let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
                if value <= 0 {
                    return Err(io::Error::last_os_error());
                }
                usize::try_from(value)
                    .map_err(|_| io::Error::other("OS page size does not fit usize"))
            }
            Self::Bytes(bytes) => usize::try_from(bytes.as_u64())
                .map_err(|_| io::Error::other("configured page size does not fit usize")),
            Self::DefaultHuge => {
                #[cfg(target_os = "linux")]
                {
                    let fd = unsafe {
                        libc::syscall(
                            libc::SYS_memfd_create,
                            c"hammer-hugepage-probe".as_ptr(),
                            MFD_CLOEXEC | MFD_HUGETLB,
                        )
                    };
                    if fd >= 0 {
                        let fd = RawFd::try_from(fd).expect("memfd fits RawFd");
                        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                        let page_size = if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } == 0 {
                            usize::try_from(unsafe { stat.assume_init() }.st_blksize).ok()
                        } else {
                            None
                        };
                        unsafe { libc::close(fd) };
                        if let Some(page_size) = page_size {
                            return Ok(page_size);
                        }
                    }
                    use procfs::Current;
                    procfs::Meminfo::current()
                        .ok()
                        .and_then(|meminfo| meminfo.hugepagesize)
                        .and_then(|value| usize::try_from(value).ok())
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::Unsupported,
                                "default HugeTLB pages are unavailable",
                            )
                        })
                }
                #[cfg(not(target_os = "linux"))]
                {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "default HugeTLB pages are unavailable",
                    ))
                }
            }
        }
    }

    pub fn is_supported_on_current_platform(self) -> bool {
        if matches!(self, Self::Bytes(bytes) if bytes.as_u64() == 0 || !bytes.as_u64().is_power_of_two())
        {
            return false;
        }

        #[cfg(target_os = "linux")]
        {
            match self {
                Self::Default => true,
                Self::DefaultHuge => self.bytes().is_ok(),
                Self::Bytes(_) => true,
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let ordinary_page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            match self {
                Self::Default => true,
                Self::Bytes(bytes) => {
                    ordinary_page_size > 0 && bytes.as_u64() == ordinary_page_size as u64
                }
                Self::DefaultHuge => false,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MainHeapConfig {
    #[serde(rename = "main_heap_size")]
    pub size: Byte,
    #[serde(rename = "main_heap_page_size")]
    pub page_size: PageSize,
    #[serde(rename = "default_hugepage_size")]
    pub default_hugepage_size: Option<PageSize>,
}

impl Default for MainHeapConfig {
    fn default() -> Self {
        Self {
            size: Byte::from_u64(DEFAULT_MAIN_HEAP_SIZE as u64),
            page_size: PageSize::Default,
            default_hugepage_size: None,
        }
    }
}

impl MainHeapConfig {
    pub fn validate(&self) -> Result<(), MemError> {
        let requested = self.size_bytes()?;
        let page_size = self
            .page_size
            .bytes()
            .map_err(|_| MemError::PageSizeUnavailable {
                requested: self.page_size,
            })?;
        if !page_size.is_power_of_two() {
            return Err(MemError::PageSizeUnavailable {
                requested: self.page_size,
            });
        }
        if requested < page_size {
            return Err(MemError::MainHeapTooSmall {
                requested,
                minimum: page_size,
            });
        }
        requested
            .checked_add(page_size - 1)
            .map(|value| value & !(page_size - 1))
            .ok_or(MemError::MainHeapSizeOverflow {
                requested,
                page_size,
            })?;
        if let Some(default_hugepage_size) = self.default_hugepage_size {
            let page_size =
                default_hugepage_size
                    .bytes()
                    .map_err(|_| MemError::PageSizeUnavailable {
                        requested: default_hugepage_size,
                    })?;
            if !page_size.is_power_of_two() {
                return Err(MemError::PageSizeUnavailable {
                    requested: default_hugepage_size,
                });
            }
        }
        Ok(())
    }

    pub fn size_bytes(&self) -> Result<usize, MemError> {
        usize::try_from(self.size.as_u64()).map_err(|_| MemError::MainHeapSizeOverflow {
            requested: usize::MAX,
            page_size: 0,
        })
    }

    pub fn initialize(&self) -> Result<usize, MemError> {
        self.validate()?;
        let page_size = self
            .page_size
            .bytes()
            .map_err(|_| MemError::PageSizeUnavailable {
                requested: self.page_size,
            })?;
        let requested = self.size_bytes()?;
        let size = requested
            .checked_add(page_size - 1)
            .map(|value| value & !(page_size - 1))
            .ok_or(MemError::MainHeapSizeOverflow {
                requested,
                page_size,
            })?;
        let state = ptr::addr_of_mut!(MEM_MAIN);
        if !unsafe { (*state).main_heap }.is_null() {
            assert_eq!(
                unsafe { (*(*state).main_heap).size },
                size,
                "main heap initializes once"
            );
            return Ok(size);
        }

        if unsafe { (*state).log2_page_size } == PAGE_SIZE_UNKNOWN {
            let system_page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if system_page_size <= 0 {
                return Err(MemError::PageSizeUnavailable {
                    requested: PageSize::Default,
                });
            }
            let system_page_size = system_page_size as usize;
            if !system_page_size.is_power_of_two() {
                return Err(MemError::PageSizeUnavailable {
                    requested: PageSize::Default,
                });
            }
            let system_hugepage_size = PageSize::DefaultHuge
                .bytes()
                .ok()
                .filter(|bytes| bytes.is_power_of_two());

            #[cfg(target_os = "linux")]
            let numa_node_bitmap = {
                let mut mode: c_int = 0;
                let mut mask = [0usize; 16];
                let result = unsafe {
                    libc::syscall(
                        libc::SYS_get_mempolicy,
                        ptr::from_mut(&mut mode),
                        mask.as_mut_ptr(),
                        mask.len() * usize::BITS as usize,
                        ptr::null::<c_void>(),
                        MPOL_F_MEMS_ALLOWED,
                    )
                };
                if result == 0 { mask[0] as u64 } else { 0 }
            };
            #[cfg(not(target_os = "linux"))]
            let numa_node_bitmap = 0u64;

            unsafe {
                (*state).log2_page_size = system_page_size.trailing_zeros() as u8;
                let system_hugepage_log2 = system_hugepage_size
                    .map(|bytes| bytes.trailing_zeros() as u8)
                    .unwrap_or(PAGE_SIZE_UNKNOWN);
                (*state).log2_default_hugepage_size = system_hugepage_log2;
                (*state).log2_system_default_hugepage_size = system_hugepage_log2;
                (*state).numa_node_bitmap = numa_node_bitmap;
            }
        }

        let default_hugepage_log2 = self
            .default_hugepage_size
            .map(|page_size| {
                page_size
                    .bytes()
                    .map_err(|_| MemError::PageSizeUnavailable {
                        requested: page_size,
                    })
                    .and_then(|bytes| {
                        bytes
                            .is_power_of_two()
                            .then(|| bytes.trailing_zeros() as u8)
                            .ok_or(MemError::PageSizeUnavailable {
                                requested: page_size,
                            })
                    })
            })
            .transpose()?;

        let mapping_page_size = if page_size == MemMain::system_page_size() {
            PageSize::Default
        } else if MemMain::default_hugepage_size() == Some(page_size) {
            PageSize::DefaultHuge
        } else {
            PageSize::Bytes(Byte::from_u64(page_size as u64))
        };
        let base = MemMain::vm_map(
            None,
            size,
            mapping_page_size,
            None,
            0,
            page_size,
            "main heap",
        )?;
        let heap = match unsafe { MemHeap::create_at(base, size, true, "main heap") } {
            Ok(heap) => heap,
            Err(error) => {
                if unsafe { MemMain::vm_unmap(base) }.is_err() {
                    std::process::abort();
                }
                return Err(error);
            }
        };

        unsafe {
            (*heap.as_ptr()).unmap_on_destroy = 1;
            (*state).main_heap = heap.as_ptr();
            let current = MemThreadMain::current();
            (*current).active_heap = heap.as_ptr();
            MemThreadMain::register_current(0);

            // Build MemMain.heaps in the Main Heap before enabling interception.
            let Some(heaps) = (*heap.as_ptr()).allocate(Layout::new::<*mut MemHeap>()) else {
                std::process::abort();
            };
            (*state).heaps = Vec::from_raw_parts(heaps.cast().as_ptr(), 0, 1);
            (*state).heaps.push(heap.as_ptr());

            if let Some(default_hugepage_log2) = default_hugepage_log2 {
                (*state).log2_default_hugepage_size = default_hugepage_log2;
            }
            AtomicU8::from_ptr(ptr::addr_of_mut!((*state).alloc_free_intercept))
                .store(1, Ordering::Release);
        }
        Ok(size)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemError {
    #[error("main heap size {requested} is smaller than the {minimum}-byte minimum")]
    MainHeapTooSmall { requested: usize, minimum: usize },
    #[error("main heap size {requested} cannot be aligned to {page_size}-byte pages")]
    MainHeapSizeOverflow { requested: usize, page_size: usize },
    #[error("page size `{requested}` is unavailable")]
    PageSizeUnavailable { requested: PageSize },
    #[error(
        "failed to reserve address range at {requested_base:?} size {size} alignment {alignment}: {source}"
    )]
    AddressReservation {
        requested_base: Option<usize>,
        size: usize,
        alignment: usize,
        #[source]
        source: io::Error,
    },
    #[error("address range {base:#x}+{size} is occupied: {source}")]
    AddressRangeOccupied {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    #[error(
        "failed to map {size} bytes at {requested_base:?} with log2 page size {page_size_log2}: {source}"
    )]
    VirtualMemoryMap {
        requested_base: Option<usize>,
        size: usize,
        page_size_log2: u8,
        backing_fd: Option<RawFd>,
        backing_offset: u64,
        #[source]
        source: io::Error,
    },
    #[error("failed to lock mapped range {base:#x}+{size}: {source}")]
    VirtualMemoryLock {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    #[error("failed to map mapping header at {address:#x}+{size}: {source}")]
    MappingHeaderMap {
        address: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    #[error(
        "failed to change mapping header protection at {address:#x}, writable {writable}: {source}"
    )]
    MappingHeaderProtect {
        address: usize,
        writable: bool,
        #[source]
        source: io::Error,
    },
    #[error("failed to unmap {size} bytes at {base:#x}: {source}")]
    VirtualMemoryUnmap {
        base: usize,
        size: usize,
        #[source]
        source: io::Error,
    },
    #[error("failed to create backing file for log2 page size {page_size_log2}: {source}")]
    BackingFileCreate {
        page_size_log2: u8,
        #[source]
        source: io::Error,
    },
    #[error("failed to seal backing file {fd}: {source}")]
    BackingFileSeal {
        fd: RawFd,
        #[source]
        source: io::Error,
    },
    #[error("failed to query backing file {fd} page size: {source}")]
    BackingFilePageSize {
        fd: RawFd,
        #[source]
        source: io::Error,
    },
    #[error("NUMA node {requested} is unavailable")]
    NumaUnavailable { requested: u32 },
    #[error("NUMA node {requested} is not in available-node bitmap {available:#x}")]
    NumaNodeUnavailable { requested: u32, available: u64 },
    #[error("failed to set NUMA policy for node {requested:?} with force={force}: {source}")]
    NumaPolicy {
        requested: Option<u32>,
        force: bool,
        #[source]
        source: io::Error,
    },
    #[error("failed to create locked heap at {base:#x}+{size}")]
    HeapCreation {
        base: usize,
        size: usize,
        locked: bool,
    },
}

#[repr(C)]
struct MemVmMapHeader {
    base_address: usize,
    page_count: usize,
    page_size_log2: u8,
    backing_fd: RawFd,
    name: [u8; 64],
    previous: *mut MemVmMapHeader,
    next: *mut MemVmMapHeader,
}

#[repr(C)]
pub struct MemThreadMain {
    active_heap: *mut MemHeap,
    thread_index: u32,
    next: *mut MemThreadMain,
    trace_thread_disable: i32,
}

thread_local! {
    static MEM_THREAD_MAIN: UnsafeCell<MemThreadMain> = const {
        UnsafeCell::new(MemThreadMain {
            active_heap: ptr::null_mut(),
            thread_index: RESERVED_THREAD_INDEX,
            next: ptr::null_mut(),
            trace_thread_disable: 0,
        })
    };
}

impl MemThreadMain {
    unsafe fn current() -> *mut Self {
        MEM_THREAD_MAIN.with(UnsafeCell::get)
    }

    fn active_heap() -> *mut MemHeap {
        unsafe {
            let current = Self::current();
            let active = (*current).active_heap;
            if !active.is_null() {
                return active;
            }
            let main_heap = (*ptr::addr_of_mut!(MEM_MAIN)).main_heap;
            if !main_heap.is_null() {
                (*current).active_heap = main_heap;
            }
            main_heap
        }
    }

    /// Registers the current runtime thread in `MemMain.threads`.
    ///
    /// # Safety
    ///
    /// The current OS thread must remain alive until process exit, and this
    /// function must be called exactly once on that thread.
    pub unsafe fn register_current(thread_index: u32) {
        assert_ne!(
            thread_index, RESERVED_THREAD_INDEX,
            "thread index is reserved"
        );
        let state = ptr::addr_of_mut!(MEM_MAIN);
        let main_heap = unsafe { (*state).main_heap };
        assert!(
            !main_heap.is_null(),
            "main heap publishes before registration"
        );

        unsafe {
            let current = Self::current();
            assert_eq!(
                (*current).thread_index,
                RESERVED_THREAD_INDEX,
                "allocator thread registers once"
            );
            (*current).active_heap = main_heap;
            (*current).thread_index = thread_index;

            let head = AtomicPtr::from_ptr(ptr::addr_of_mut!((*state).threads));
            loop {
                let previous = head.load(Ordering::Acquire);
                (*current).next = previous;
                if head
                    .compare_exchange_weak(previous, current, Ordering::Release, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    pub fn thread_index() -> Option<u32> {
        let index = unsafe { (*Self::current()).thread_index };
        (index != RESERVED_THREAD_INDEX).then_some(index)
    }
}

#[repr(C)]
pub struct MemHeap {
    base: *mut c_void,
    mspace: Mspace,
    size: usize,
    page_size_log2: u8,
    locked: u8,
    traced: u8,
    unmap_on_destroy: u8,
    name: [u8; 0],
}

impl MemHeap {
    pub(crate) unsafe fn create_at(
        base: NonNull<u8>,
        size: usize,
        locked: bool,
        name: &str,
    ) -> Result<NonNull<Self>, MemError> {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        let system_page_size = unsafe { 1usize << (*state).log2_page_size };
        let map_lock = unsafe { AtomicU8::from_ptr(ptr::addr_of_mut!((*state).map_lock)) };
        while map_lock
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }

        let requested_start = base.as_ptr().addr();
        let requested_end = requested_start
            .checked_add(size)
            .expect("heap range end fits usize");
        let mut mapping = unsafe { (*state).first_map };
        let mut mapping_page_size_log2 = None;
        let mut mapping_error = None;
        while !mapping.is_null() {
            if unsafe {
                libc::mprotect(
                    mapping.cast(),
                    system_page_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                )
            } != 0
            {
                mapping_error = Some(MemError::MappingHeaderProtect {
                    address: mapping.addr(),
                    writable: true,
                    source: io::Error::last_os_error(),
                });
                break;
            }
            let (base_address, page_count, page_size_log2, next) = unsafe {
                (
                    (*mapping).base_address,
                    (*mapping).page_count,
                    (*mapping).page_size_log2,
                    (*mapping).next,
                )
            };
            if unsafe { libc::mprotect(mapping.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                mapping_error = Some(MemError::MappingHeaderProtect {
                    address: mapping.addr(),
                    writable: false,
                    source: io::Error::last_os_error(),
                });
                break;
            }

            let mapping_end = page_count
                .checked_shl(page_size_log2.into())
                .and_then(|mapping_size| base_address.checked_add(mapping_size));
            if requested_start >= base_address
                && mapping_end.is_some_and(|mapping_end| requested_end <= mapping_end)
            {
                mapping_page_size_log2 = Some(page_size_log2);
                break;
            }
            mapping = next;
        }
        map_lock.store(0, Ordering::Release);
        if let Some(error) = mapping_error {
            return Err(error);
        }
        let page_size_log2 = mapping_page_size_log2.unwrap_or_else(|| {
            assert!(
                system_page_size.is_power_of_two(),
                "system page size is a power of two"
            );
            system_page_size.trailing_zeros() as u8
        });
        let mspace =
            unsafe { create_mspace_with_base(base.as_ptr().cast(), size, i32::from(locked)) };
        if mspace.is_null() {
            return Err(MemError::HeapCreation {
                base: base.as_ptr() as usize,
                size,
                locked,
            });
        }
        unsafe { mspace_disable_expand(mspace) };

        let control_size = size_of::<Self>()
            .checked_add(name.len() + 1)
            .and_then(|size| {
                size.checked_add(align_of::<Self>() - 1)
                    .map(|size| size & !(align_of::<Self>() - 1))
            })
            .expect("heap control block size fits usize");
        let control =
            unsafe { mspace_memalign(mspace, align_of::<Self>(), control_size).cast::<Self>() };
        if control.is_null() {
            unsafe { destroy_mspace(mspace) };
            return Err(MemError::HeapCreation {
                base: base.as_ptr() as usize,
                size,
                locked,
            });
        }

        unsafe {
            ptr::write_bytes(control.cast::<u8>(), 0, control_size);
            ptr::addr_of_mut!((*control).base).write(base.as_ptr().cast());
            ptr::addr_of_mut!((*control).mspace).write(mspace);
            ptr::addr_of_mut!((*control).size).write(size);
            ptr::addr_of_mut!((*control).page_size_log2).write(page_size_log2);
            ptr::addr_of_mut!((*control).locked).write(u8::from(locked));
            let name_pointer = ptr::addr_of_mut!((*control).name).cast::<u8>();
            ptr::copy_nonoverlapping(name.as_ptr(), name_pointer, name.len());
            name_pointer.add(name.len()).write(0);
        }

        let main_heap = unsafe { (*state).main_heap };
        if !main_heap.is_null() {
            let previous_heap = unsafe { (*main_heap).activate() };
            unsafe { (*state).heaps.push(control) };
            drop(previous_heap);
        }

        Ok(unsafe { NonNull::new_unchecked(control) })
    }

    pub fn activate(&self) -> ActiveHeap<'_> {
        let previous_heap = unsafe {
            let current = MemThreadMain::current();
            let previous = (*current).active_heap;
            (*current).active_heap = (self as *const Self).cast_mut();
            previous
        };
        ActiveHeap {
            active_heap: self,
            previous_heap,
        }
    }

    pub fn allocate(&self, layout: Layout) -> Option<NonNull<u8>> {
        let layout = Layout::from_size_align(layout.size(), layout.align().max(MIN_HEAP_ALIGNMENT))
            .expect("valid minimum allocation layout");
        let pointer = unsafe { mspace_memalign(self.mspace, layout.align(), layout.size()) };
        NonNull::new(pointer.cast::<u8>())
    }

    pub fn allocate_zeroed(&self, layout: Layout) -> Option<NonNull<u8>> {
        let pointer = self.allocate(layout)?;
        unsafe { ptr::write_bytes(pointer.as_ptr(), 0, layout.size()) };
        Some(pointer)
    }

    /// Reallocates a block in this heap.
    ///
    /// # Safety
    ///
    /// `pointer` must name a live block allocated by this `MemHeap`, and
    /// `old_layout` must match the layout used for that allocation.
    pub unsafe fn reallocate(
        &self,
        pointer: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Option<NonNull<u8>> {
        let new_layout = Layout::from_size_align(
            new_layout.size(),
            new_layout.align().max(MIN_HEAP_ALIGNMENT),
        )
        .expect("valid minimum allocation layout");
        if !self.is_heap_object(pointer) {
            std::process::abort();
        }
        let old_size = unsafe { mspace_usable_size(pointer.as_ptr().cast()) };
        debug_assert!(old_layout.size() <= old_size);
        if new_layout.size() == old_size {
            return Some(pointer);
        }
        if pointer.as_ptr().addr().is_multiple_of(new_layout.align())
            && !unsafe {
                mspace_realloc_in_place(self.mspace, pointer.as_ptr().cast(), new_layout.size())
            }
            .is_null()
        {
            return Some(pointer);
        }

        let replacement = self.allocate(new_layout)?;
        unsafe {
            ptr::copy_nonoverlapping(
                pointer.as_ptr(),
                replacement.as_ptr(),
                old_size.min(new_layout.size()),
            );
            if !self.is_heap_object(pointer) {
                std::process::abort();
            }
            mspace_free(self.mspace, pointer.as_ptr().cast());
        }
        Some(replacement)
    }

    /// Releases a block from this heap.
    ///
    /// # Safety
    ///
    /// `pointer` must name a live block allocated by this `MemHeap`, and the
    /// supplied layout must match the layout used for that allocation.
    pub unsafe fn deallocate(&self, pointer: NonNull<u8>, _: Layout) {
        if !self.is_heap_object(pointer) {
            unsafe {
                let mut message = [0_u8; 128];
                let length = libc::snprintf(
                    message.as_mut_ptr().cast(),
                    message.len(),
                    c"hammer-infra: active heap %p cannot free %p\n".as_ptr(),
                    (self as *const Self).cast::<c_void>(),
                    pointer.as_ptr().cast::<c_void>(),
                );
                if length > 0 {
                    libc::write(
                        libc::STDERR_FILENO,
                        message.as_ptr().cast(),
                        (length as usize).min(message.len()),
                    );
                }
            };
            std::process::abort();
        }
        unsafe { mspace_free(self.mspace, pointer.as_ptr().cast()) };
    }

    pub fn is_heap_object(&self, pointer: NonNull<u8>) -> bool {
        unsafe { mspace_is_heap_object(self.mspace, pointer.as_ptr().cast()) != 0 }
    }

    pub fn base(&self) -> NonNull<u8> {
        unsafe { NonNull::new_unchecked(self.base.cast::<u8>()) }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn name(&self) -> &str {
        let pointer = ptr::addr_of!(self.name).cast::<u8>();
        unsafe { CStr::from_ptr(pointer.cast()) }
            .to_str()
            .expect("heap names are constructed from UTF-8")
    }

    /// Destroys a heap that was not published to its owning subsystem.
    ///
    /// # Safety
    ///
    /// No allocation from this heap may remain live or be accessed again.
    pub(crate) unsafe fn destroy(&self) {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        let main_heap = unsafe { (*state).main_heap };
        assert_ne!(
            main_heap,
            (self as *const Self).cast_mut(),
            "main heap remains live for process lifetime"
        );

        if !main_heap.is_null() {
            let active_heap = unsafe { (*main_heap).activate() };
            let pointer = (self as *const Self).cast_mut();
            if let Some(index) = unsafe { (*state).heaps.iter().position(|heap| *heap == pointer) }
            {
                unsafe { (*state).heaps.remove(index) };
            }
            drop(active_heap);
        }
        unsafe { destroy_mspace(self.mspace) };
    }
}

#[must_use = "dropping ActiveHeap restores the previous heap"]
pub struct ActiveHeap<'heap> {
    active_heap: &'heap MemHeap,
    previous_heap: *mut MemHeap,
}

impl Drop for ActiveHeap<'_> {
    fn drop(&mut self) {
        let restored = unsafe {
            let current = MemThreadMain::current();
            let restored = (*current).active_heap;
            (*current).active_heap = self.previous_heap;
            restored
        };
        debug_assert_eq!(restored, (self.active_heap as *const MemHeap).cast_mut());
    }
}

pub struct MemMain {
    main_heap: *mut MemHeap,
    heaps: Vec<*mut MemHeap>,
    threads: *mut MemThreadMain,
    log2_page_size: u8,
    log2_default_hugepage_size: u8,
    log2_system_default_hugepage_size: u8,
    alloc_free_intercept: u8,
    numa_node_bitmap: u64,
    first_map: *mut MemVmMapHeader,
    last_map: *mut MemVmMapHeader,
    map_lock: u8,
}

impl MemMain {
    const fn new() -> Self {
        Self {
            main_heap: ptr::null_mut(),
            heaps: Vec::new(),
            threads: ptr::null_mut(),
            log2_page_size: PAGE_SIZE_UNKNOWN,
            log2_default_hugepage_size: PAGE_SIZE_UNKNOWN,
            log2_system_default_hugepage_size: PAGE_SIZE_UNKNOWN,
            alloc_free_intercept: 0,
            numa_node_bitmap: 0,
            first_map: ptr::null_mut(),
            last_map: ptr::null_mut(),
            map_lock: 0,
        }
    }

    pub fn main_heap() -> &'static MemHeap {
        let pointer = unsafe { (*ptr::addr_of_mut!(MEM_MAIN)).main_heap };
        assert!(!pointer.is_null(), "main heap is initialized");
        unsafe { &*pointer }
    }

    pub fn heap_count() -> usize {
        unsafe { (*ptr::addr_of_mut!(MEM_MAIN)).heaps.len() }
    }

    pub fn registered_thread_count() -> usize {
        unsafe {
            let mut count = 0;
            let head =
                AtomicPtr::from_ptr(ptr::addr_of_mut!((*ptr::addr_of_mut!(MEM_MAIN)).threads));
            let mut thread = head.load(Ordering::Acquire);
            while !thread.is_null() {
                count += 1;
                thread = (*thread).next;
            }
            count
        }
    }

    pub fn mapping_count() -> usize {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        let map_lock = unsafe { AtomicU8::from_ptr(ptr::addr_of_mut!((*state).map_lock)) };
        while map_lock
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }

        let system_page_size = unsafe { 1usize << (*state).log2_page_size };
        let mut header = unsafe { (*state).first_map };
        let mut count = 0;
        while !header.is_null() {
            if unsafe {
                libc::mprotect(
                    header.cast(),
                    system_page_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                )
            } != 0
            {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            let next = unsafe { (*header).next };
            if unsafe { libc::mprotect(header.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            count += 1;
            header = next;
        }
        map_lock.store(0, Ordering::Release);
        count
    }

    pub fn system_page_size() -> usize {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        unsafe { 1usize << (*state).log2_page_size }
    }

    pub fn default_hugepage_size() -> Option<usize> {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        let log2 = unsafe { (*state).log2_default_hugepage_size };
        (log2 != PAGE_SIZE_UNKNOWN).then(|| 1usize << log2)
    }

    pub fn system_default_hugepage_size() -> Option<usize> {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        let log2 = unsafe { (*state).log2_system_default_hugepage_size };
        (log2 != PAGE_SIZE_UNKNOWN).then(|| 1usize << log2)
    }

    pub fn set_default_hugepage_size(page_size: PageSize) -> Result<(), MemError> {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        let bytes = page_size
            .bytes()
            .map_err(|_| MemError::PageSizeUnavailable {
                requested: page_size,
            })?;
        let log2 = bytes
            .is_power_of_two()
            .then(|| bytes.trailing_zeros() as u8)
            .ok_or(MemError::PageSizeUnavailable {
                requested: page_size,
            })?;
        unsafe { (*state).log2_default_hugepage_size = log2 };
        Ok(())
    }

    pub fn next_numa_node(previous: Option<u32>) -> Option<u32> {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        let bitmap = unsafe { (*state).numa_node_bitmap };
        let allow_lower = previous.map_or(u64::MAX, |node| {
            if node >= 63 {
                0
            } else {
                u64::MAX << (node + 1)
            }
        });
        let available = bitmap & allow_lower;
        (available != 0).then(|| available.trailing_zeros())
    }

    pub fn set_numa_affinity(numa_node: u32, force: bool) -> Result<(), MemError> {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        let available = unsafe { (*state).numa_node_bitmap };
        if available == 0 {
            return if numa_node == 0 {
                Ok(())
            } else {
                Err(MemError::NumaUnavailable {
                    requested: numa_node,
                })
            };
        }
        if numa_node >= u64::BITS || available & (1u64 << numa_node) == 0 {
            return Err(MemError::NumaNodeUnavailable {
                requested: numa_node,
                available,
            });
        }

        #[cfg(target_os = "linux")]
        {
            let mode = if force { MPOL_BIND } else { MPOL_PREFERRED };
            let mask = 1usize << numa_node;
            let result = unsafe {
                libc::syscall(
                    libc::SYS_set_mempolicy,
                    mode,
                    ptr::from_ref(&mask),
                    u64::BITS,
                )
            };
            if result != 0 {
                return Err(MemError::NumaPolicy {
                    requested: Some(numa_node),
                    force,
                    source: io::Error::last_os_error(),
                });
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(MemError::NumaUnavailable {
                requested: numa_node,
            })
        }
    }

    pub fn set_default_numa_affinity() -> Result<(), MemError> {
        #[cfg(target_os = "linux")]
        {
            let result = unsafe {
                libc::syscall(
                    libc::SYS_set_mempolicy,
                    MPOL_DEFAULT,
                    ptr::null::<c_void>(),
                    0,
                )
            };
            if result != 0 {
                return Err(MemError::NumaPolicy {
                    requested: None,
                    force: false,
                    source: io::Error::last_os_error(),
                });
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(())
        }
    }

    pub(crate) fn vm_create_backing(page_size: PageSize, name: &str) -> Result<OwnedFd, MemError> {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        let page_bytes = match page_size {
            PageSize::Default => unsafe { 1usize << (*state).log2_page_size },
            PageSize::DefaultHuge => {
                let log2 = unsafe { (*state).log2_default_hugepage_size };
                if log2 == PAGE_SIZE_UNKNOWN {
                    return Err(MemError::PageSizeUnavailable {
                        requested: page_size,
                    });
                }
                1usize << log2
            }
            PageSize::Bytes(bytes) => {
                usize::try_from(bytes.as_u64()).map_err(|_| MemError::PageSizeUnavailable {
                    requested: page_size,
                })?
            }
        };
        let page_size_log2 = if page_bytes != 0 && page_bytes.is_power_of_two() {
            page_bytes.trailing_zeros() as u8
        } else {
            return Err(MemError::PageSizeUnavailable {
                requested: page_size,
            });
        };

        #[cfg(target_os = "linux")]
        {
            let name = name.as_bytes();
            let mut fixed_name = [0u8; 250];
            let copied = name.len().min(fixed_name.len() - 1);
            fixed_name[..copied].copy_from_slice(&name[..copied]);
            let system_page = MemMain::system_page_size();
            let system_hugepage = MemMain::system_default_hugepage_size();
            let flags = if page_bytes == system_page {
                MFD_ALLOW_SEALING | MFD_CLOEXEC
            } else if system_hugepage == Some(page_bytes) {
                MFD_HUGETLB | MFD_CLOEXEC
            } else {
                MFD_CLOEXEC | MFD_HUGETLB | ((page_size_log2 as c_int) << MAP_HUGE_SHIFT)
            };
            let fd = unsafe {
                libc::syscall(
                    libc::SYS_memfd_create,
                    fixed_name.as_ptr().cast::<libc::c_char>(),
                    flags,
                )
            };
            if fd < 0 {
                return Err(MemError::BackingFileCreate {
                    page_size_log2,
                    source: io::Error::last_os_error(),
                });
            }
            let fd = RawFd::try_from(fd).expect("memfd fits RawFd");
            if flags & MFD_ALLOW_SEALING != 0
                && unsafe { libc::fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK) } != 0
            {
                let source = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(MemError::BackingFileSeal { fd, source });
            }
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = name;
            Err(MemError::BackingFileCreate {
                page_size_log2,
                source: io::Error::new(io::ErrorKind::Unsupported, "memfd is Linux-only"),
            })
        }
    }

    pub(crate) fn vm_map(
        base: Option<NonZeroUsize>,
        size: usize,
        page_size: PageSize,
        backing: Option<BorrowedFd<'_>>,
        backing_offset: u64,
        alignment: usize,
        name: &str,
    ) -> Result<NonNull<u8>, MemError> {
        let state = ptr::addr_of_mut!(MEM_MAIN);
        assert_ne!(
            unsafe { (*state).log2_page_size },
            PAGE_SIZE_UNKNOWN,
            "memory platform initializes before heap access"
        );
        let backing_fd = backing.map(|backing| backing.as_raw_fd());
        let page_bytes = if let Some(fd) = backing_fd {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
                return Err(MemError::BackingFilePageSize {
                    fd,
                    source: io::Error::last_os_error(),
                });
            }
            let bytes = unsafe { stat.assume_init() }.st_blksize;
            usize::try_from(bytes)
                .ok()
                .filter(|bytes| *bytes != 0 && bytes.is_power_of_two())
                .ok_or_else(|| MemError::BackingFilePageSize {
                    fd,
                    source: io::Error::new(
                        io::ErrorKind::InvalidData,
                        "backing file reports an invalid page size",
                    ),
                })?
        } else {
            match page_size {
                PageSize::Default => unsafe { 1usize << (*state).log2_page_size },
                PageSize::DefaultHuge => {
                    let log2 = unsafe { (*state).log2_default_hugepage_size };
                    if log2 == PAGE_SIZE_UNKNOWN {
                        return Err(MemError::PageSizeUnavailable {
                            requested: page_size,
                        });
                    }
                    1usize << log2
                }
                PageSize::Bytes(bytes) => {
                    usize::try_from(bytes.as_u64()).map_err(|_| MemError::PageSizeUnavailable {
                        requested: page_size,
                    })?
                }
            }
        };
        let page_size_log2 = if page_bytes != 0 && page_bytes.is_power_of_two() {
            page_bytes.trailing_zeros() as u8
        } else {
            return Err(MemError::PageSizeUnavailable {
                requested: page_size,
            });
        };
        let size = size
            .checked_add(page_bytes - 1)
            .map(|value| value & !(page_bytes - 1))
            .ok_or(MemError::MainHeapSizeOverflow {
                requested: size,
                page_size: page_bytes,
            })?;
        let alignment = alignment.max(page_bytes);
        let system_page_size = unsafe { 1usize << (*state).log2_page_size };
        assert!(alignment.is_power_of_two() && alignment >= system_page_size);

        let (reservation_base, reservation_size, payload) = if let Some(base) = base {
            let payload = base.get();
            if !payload.is_multiple_of(alignment) {
                return Err(MemError::AddressReservation {
                    requested_base: Some(payload),
                    size,
                    alignment,
                    source: io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "requested base is misaligned",
                    ),
                });
            }
            let header =
                payload
                    .checked_sub(system_page_size)
                    .ok_or(MemError::AddressReservation {
                        requested_base: Some(payload),
                        size,
                        alignment,
                        source: io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "requested base has no header page",
                        ),
                    })?;
            let reservation_size =
                size.checked_add(system_page_size)
                    .ok_or(MemError::AddressReservation {
                        requested_base: Some(payload),
                        size,
                        alignment,
                        source: io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "mapping size overflow",
                        ),
                    })?;
            #[cfg(target_os = "linux")]
            let reservation_flags =
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED_NOREPLACE;
            #[cfg(not(target_os = "linux"))]
            let reservation_flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
            let reservation = unsafe {
                libc::mmap(
                    header as *mut c_void,
                    reservation_size,
                    libc::PROT_NONE,
                    reservation_flags,
                    -1,
                    0,
                )
            };
            if reservation == libc::MAP_FAILED {
                return Err(MemError::AddressRangeOccupied {
                    base: payload,
                    size,
                    source: io::Error::last_os_error(),
                });
            }
            if reservation.addr() != header {
                if unsafe { libc::munmap(reservation, reservation_size) } != 0 {
                    std::process::abort();
                }
                return Err(MemError::AddressRangeOccupied {
                    base: payload,
                    size,
                    source: io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "requested address range is unavailable",
                    ),
                });
            }
            (header as *mut u8, reservation_size, payload as *mut u8)
        } else {
            let total = size
                .checked_add(alignment)
                .and_then(|value| value.checked_add(system_page_size))
                .ok_or(MemError::AddressReservation {
                    requested_base: None,
                    size,
                    alignment,
                    source: io::Error::new(io::ErrorKind::InvalidInput, "mapping size overflow"),
                })?;
            let raw = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    total,
                    libc::PROT_NONE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };
            if raw == libc::MAP_FAILED {
                return Err(MemError::AddressReservation {
                    requested_base: None,
                    size,
                    alignment,
                    source: io::Error::last_os_error(),
                });
            }
            let raw_address = raw.addr();
            let payload = raw_address
                .checked_add(system_page_size)
                .and_then(|value| value.checked_add(alignment - 1))
                .map(|value| value & !(alignment - 1))
                .expect("alignment overflow");
            let header = payload - system_page_size;
            let prefix = header - raw_address;
            if prefix != 0 && unsafe { libc::munmap(raw, prefix) } != 0 {
                let source = io::Error::last_os_error();
                if unsafe { libc::munmap(raw, total) } != 0 {
                    std::process::abort();
                }
                return Err(MemError::AddressReservation {
                    requested_base: None,
                    size,
                    alignment,
                    source,
                });
            }
            let mapped_end = raw_address + total;
            let reservation_end = payload + size;
            if mapped_end != reservation_end
                && unsafe {
                    libc::munmap(reservation_end as *mut c_void, mapped_end - reservation_end)
                } != 0
            {
                let source = io::Error::last_os_error();
                if unsafe { libc::munmap(header as *mut c_void, mapped_end - header) } != 0 {
                    std::process::abort();
                }
                return Err(MemError::AddressReservation {
                    requested_base: None,
                    size,
                    alignment,
                    source,
                });
            }
            (
                header as *mut u8,
                size + system_page_size,
                payload as *mut u8,
            )
        };

        let mut flags = match backing_fd {
            Some(_) => libc::MAP_SHARED,
            None => libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
        };
        if backing_fd.is_none() && page_bytes != MemMain::system_page_size() {
            #[cfg(target_os = "linux")]
            {
                flags |= libc::MAP_HUGETLB;
                if page_bytes != MemMain::system_default_hugepage_size().unwrap_or(0) {
                    flags |= (page_size_log2 as c_int) << MAP_HUGE_SHIFT;
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                return Err(MemError::PageSizeUnavailable {
                    requested: page_size,
                });
            }
        }
        let fd = backing_fd.unwrap_or(-1);
        let mapped = unsafe {
            libc::mmap(
                payload.cast(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                flags | libc::MAP_FIXED,
                fd,
                backing_offset as libc::off_t,
            )
        };
        if mapped == libc::MAP_FAILED {
            unsafe { libc::munmap(reservation_base.cast(), reservation_size) };
            return Err(MemError::VirtualMemoryMap {
                requested_base: base.map(NonZeroUsize::get),
                size,
                page_size_log2,
                backing_fd,
                backing_offset,
                source: io::Error::last_os_error(),
            });
        }
        debug_assert_eq!(mapped, payload.cast());

        if page_bytes != MemMain::system_page_size()
            && unsafe { libc::mlock(payload.cast(), size) } != 0
        {
            let source = io::Error::last_os_error();
            unsafe { libc::munmap(reservation_base.cast(), reservation_size) };
            return Err(MemError::VirtualMemoryLock {
                base: payload.addr(),
                size,
                source,
            });
        }

        let header_address = payload.addr() - system_page_size;
        let header = unsafe {
            libc::mmap(
                header_address as *mut c_void,
                system_page_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED,
                -1,
                0,
            )
        };
        if header == libc::MAP_FAILED {
            unsafe { libc::munmap(reservation_base.cast(), reservation_size) };
            return Err(MemError::MappingHeaderMap {
                address: header_address,
                size: system_page_size,
                source: io::Error::last_os_error(),
            });
        }

        let mut fixed_name = [0u8; 64];
        let name_bytes = name.as_bytes();
        let copied = name_bytes.len().min(fixed_name.len() - 1);
        fixed_name[..copied].copy_from_slice(&name_bytes[..copied]);
        let header = header.cast::<MemVmMapHeader>();
        let map_lock = unsafe { AtomicU8::from_ptr(ptr::addr_of_mut!((*state).map_lock)) };
        while map_lock
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        let previous = unsafe { (*state).last_map };
        unsafe {
            ptr::write(
                header,
                MemVmMapHeader {
                    base_address: payload.addr(),
                    page_count: size >> page_size_log2,
                    page_size_log2,
                    backing_fd: fd,
                    name: fixed_name,
                    previous,
                    next: ptr::null_mut(),
                },
            );
        }
        if unsafe { libc::mprotect(header.cast(), system_page_size, libc::PROT_NONE) } != 0 {
            let source = io::Error::last_os_error();
            map_lock.store(0, Ordering::Release);
            unsafe {
                libc::munmap(header.cast(), system_page_size);
                libc::munmap(reservation_base.cast(), reservation_size);
            }
            return Err(MemError::MappingHeaderProtect {
                address: header.addr(),
                writable: false,
                source,
            });
        }
        if !previous.is_null() {
            if unsafe {
                libc::mprotect(
                    previous.cast(),
                    system_page_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                )
            } != 0
            {
                let source = io::Error::last_os_error();
                map_lock.store(0, Ordering::Release);
                unsafe {
                    libc::munmap(header.cast(), system_page_size);
                    libc::munmap(reservation_base.cast(), reservation_size);
                }
                return Err(MemError::MappingHeaderProtect {
                    address: previous.addr(),
                    writable: true,
                    source,
                });
            }
            unsafe { (*previous).next = header };
            if unsafe { libc::mprotect(previous.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                let source = io::Error::last_os_error();
                unsafe { (*previous).next = ptr::null_mut() };
                map_lock.store(0, Ordering::Release);
                unsafe {
                    libc::munmap(header.cast(), system_page_size);
                    libc::munmap(reservation_base.cast(), reservation_size);
                }
                return Err(MemError::MappingHeaderProtect {
                    address: previous.addr(),
                    writable: false,
                    source,
                });
            }
        } else {
            unsafe { (*state).first_map = header };
        }
        unsafe { (*state).last_map = header };
        map_lock.store(0, Ordering::Release);
        Ok(unsafe { NonNull::new_unchecked(payload) })
    }

    pub(crate) unsafe fn vm_unmap(base: NonNull<u8>) -> Result<(), MemError> {
        let system_page_size = MemMain::system_page_size();
        let header = base
            .as_ptr()
            .wrapping_sub(system_page_size)
            .cast::<MemVmMapHeader>();
        let state = ptr::addr_of_mut!(MEM_MAIN);
        let map_lock = unsafe { AtomicU8::from_ptr(ptr::addr_of_mut!((*state).map_lock)) };
        while map_lock
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        if unsafe {
            libc::mprotect(
                header.cast(),
                system_page_size,
                libc::PROT_READ | libc::PROT_WRITE,
            )
        } != 0
        {
            let source = io::Error::last_os_error();
            map_lock.store(0, Ordering::Release);
            return Err(MemError::MappingHeaderProtect {
                address: header.addr(),
                writable: true,
                source,
            });
        }
        let size = unsafe {
            (*header)
                .page_count
                .checked_shl((*header).page_size_log2.into())
        };
        let Some(size) = size else {
            if unsafe { libc::mprotect(header.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            map_lock.store(0, Ordering::Release);
            return Err(MemError::VirtualMemoryUnmap {
                base: base.as_ptr().addr(),
                size: 0,
                source: io::Error::new(io::ErrorKind::InvalidData, "mapping size overflow"),
            });
        };
        let previous = unsafe { (*header).previous };
        let next = unsafe { (*header).next };

        if !previous.is_null()
            && unsafe {
                libc::mprotect(
                    previous.cast(),
                    system_page_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                )
            } != 0
        {
            let source = io::Error::last_os_error();
            if unsafe { libc::mprotect(header.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            map_lock.store(0, Ordering::Release);
            return Err(MemError::MappingHeaderProtect {
                address: previous.addr(),
                writable: true,
                source,
            });
        }
        if !next.is_null()
            && unsafe {
                libc::mprotect(
                    next.cast(),
                    system_page_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                )
            } != 0
        {
            let source = io::Error::last_os_error();
            if !previous.is_null()
                && unsafe { libc::mprotect(previous.cast(), system_page_size, libc::PROT_NONE) }
                    != 0
            {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            if unsafe { libc::mprotect(header.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            map_lock.store(0, Ordering::Release);
            return Err(MemError::MappingHeaderProtect {
                address: next.addr(),
                writable: true,
                source,
            });
        }

        if unsafe { libc::munmap(base.as_ptr().cast(), size) } != 0 {
            let source = io::Error::last_os_error();
            if !previous.is_null()
                && unsafe { libc::mprotect(previous.cast(), system_page_size, libc::PROT_NONE) }
                    != 0
            {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            if !next.is_null()
                && unsafe { libc::mprotect(next.cast(), system_page_size, libc::PROT_NONE) } != 0
            {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            if unsafe { libc::mprotect(header.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
            map_lock.store(0, Ordering::Release);
            return Err(MemError::VirtualMemoryUnmap {
                base: base.as_ptr().addr(),
                size,
                source,
            });
        }

        if !previous.is_null() {
            unsafe { (*previous).next = next };
            if unsafe { libc::mprotect(previous.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
        } else {
            unsafe { (*state).first_map = next };
        }
        if !next.is_null() {
            unsafe { (*next).previous = previous };
            if unsafe { libc::mprotect(next.cast(), system_page_size, libc::PROT_NONE) } != 0 {
                map_lock.store(0, Ordering::Release);
                std::process::abort();
            }
        } else {
            unsafe { (*state).last_map = previous };
        }
        map_lock.store(0, Ordering::Release);

        if unsafe { libc::munmap(header.cast(), system_page_size) } != 0 {
            return Err(MemError::VirtualMemoryUnmap {
                base: header.addr(),
                size: system_page_size,
                source: io::Error::last_os_error(),
            });
        }
        Ok(())
    }
}

#[global_allocator]
static mut MEM_MAIN: MemMain = MemMain::new();

unsafe impl GlobalAlloc for MemMain {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if unsafe {
            AtomicU8::from_ptr(ptr::addr_of_mut!(
                (*ptr::addr_of_mut!(MEM_MAIN)).alloc_free_intercept
            ))
            .load(Ordering::Acquire)
                == 0
        } {
            return unsafe { System.alloc(layout) };
        }
        let heap = MemThreadMain::active_heap();
        assert!(!heap.is_null(), "allocation intercept has an active heap");
        unsafe {
            (*heap)
                .allocate(layout)
                .map_or(ptr::null_mut(), NonNull::as_ptr)
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if unsafe {
            AtomicU8::from_ptr(ptr::addr_of_mut!(
                (*ptr::addr_of_mut!(MEM_MAIN)).alloc_free_intercept
            ))
            .load(Ordering::Acquire)
                == 0
        } {
            return unsafe { System.alloc_zeroed(layout) };
        }
        let heap = MemThreadMain::active_heap();
        assert!(!heap.is_null(), "allocation intercept has an active heap");
        unsafe {
            (*heap)
                .allocate_zeroed(layout)
                .map_or(ptr::null_mut(), NonNull::as_ptr)
        }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if pointer.is_null() {
            return;
        }
        if unsafe {
            AtomicU8::from_ptr(ptr::addr_of_mut!(
                (*ptr::addr_of_mut!(MEM_MAIN)).alloc_free_intercept
            ))
            .load(Ordering::Acquire)
                == 0
        } {
            unsafe { System.dealloc(pointer, layout) };
            return;
        }
        let heap = MemThreadMain::active_heap();
        assert!(!heap.is_null(), "allocation intercept has an active heap");
        let pointer = unsafe { NonNull::new_unchecked(pointer) };
        unsafe { (*heap).deallocate(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if pointer.is_null() {
            let Ok(layout) = Layout::from_size_align(new_size, layout.align()) else {
                return ptr::null_mut();
            };
            return unsafe { self.alloc(layout) };
        }
        if new_size == 0 {
            unsafe { self.dealloc(pointer, layout) };
            return ptr::null_mut();
        }
        if unsafe {
            AtomicU8::from_ptr(ptr::addr_of_mut!(
                (*ptr::addr_of_mut!(MEM_MAIN)).alloc_free_intercept
            ))
            .load(Ordering::Acquire)
                == 0
        } {
            return unsafe { System.realloc(pointer, layout, new_size) };
        }
        let heap = MemThreadMain::active_heap();
        assert!(!heap.is_null(), "allocation intercept has an active heap");
        let Ok(new_layout) = Layout::from_size_align(new_size, layout.align()) else {
            return ptr::null_mut();
        };
        let pointer = unsafe { NonNull::new_unchecked(pointer) };
        unsafe { (*heap).reallocate(pointer, layout, new_layout) }
            .map_or(ptr::null_mut(), NonNull::as_ptr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main_heap_config_uses_adr_field_names() {
        let config: MainHeapConfig =
            toml::from_str("main_heap_size = '64 MiB'\nmain_heap_page_size = 'default'\n").unwrap();
        assert_eq!(config.size.as_u64(), 64 * 1024 * 1024);
        assert_eq!(config.page_size, PageSize::Default);
        assert_eq!(config.default_hugepage_size, None);
    }
}
