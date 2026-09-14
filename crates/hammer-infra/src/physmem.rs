//! Process-wide physical-memory authority for packet Buffer mappings.

use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::os::fd::RawFd;
use std::sync::OnceLock;

use crate::align::CACHE_LINE;
use crate::mem::PageSize;
use crate::pmalloc::PmallocMain;
use crate::pool::Pool;

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
        path: std::path::PathBuf,
    },
    HugePagePool {
        operation: &'static str,
        path: std::path::PathBuf,
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
            Self::UnsupportedPageSize { requested } => {
                write!(formatter, "physmem page size `{requested}` is unsupported")
            }
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
            Self::HugePageDiscovery { requested, source } => {
                write!(
                    formatter,
                    "failed to resolve HugeTLB size for `{requested}`: {source}"
                )
            }
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
                "physmem mapping reports kernel page size {actual}, requested {requested}"
            ),
            Self::PlacementVerification { requested, actual } => write!(
                formatter,
                "physmem mapping landed on NUMA node {actual}, requested node {requested}"
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

#[repr(align(64))]
pub struct PhysmemMain {
    flags: u32,
    base_addr: usize,
    max_size: usize,
    maps: Pool<PhysmemMap>,
    pmalloc_main: PmallocMain,
}

pub struct PhysmemMap {
    index: u32,
    fd: RawFd,
    base: *mut u8,
    size: usize,
    n_pages: u32,
    page_table: Vec<usize>,
    log2_page_size: u32,
    numa_node: u32,
}

unsafe impl Send for PhysmemMap {}
unsafe impl Sync for PhysmemMap {}

pub static PHYSMEM_MAIN: OnceLock<PhysmemMain> = OnceLock::new();

const INVALID_MAP_INDEX: u32 = u32::MAX;

impl PhysmemMain {
    pub fn init(
        base_addr: Option<NonZeroUsize>,
        max_size: usize,
        map_size: usize,
        page_size: PageSize,
        numa_nodes: &[u32],
    ) -> Result<&'static Self, PhysmemError> {
        assert!(
            PHYSMEM_MAIN.get().is_none(),
            "Physmem Main initializes once"
        );
        assert!(!numa_nodes.is_empty(), "Physmem Main requires a NUMA node");
        if map_size == 0 {
            return Err(PhysmemError::InvalidSize {
                requested: map_size,
            });
        }
        let page_bytes = page_size
            .bytes()
            .map_err(|source| PhysmemError::HugePageDiscovery {
                requested: page_size,
                source,
            })?;
        if !page_bytes.is_power_of_two() {
            return Err(PhysmemError::UnsupportedPageSize {
                requested: page_size,
            });
        }
        let log2_page_size = page_bytes.trailing_zeros();
        let mut pmalloc_main = PmallocMain::new();
        pmalloc_main
            .initialize(base_addr, max_size)
            .map_err(|source| PhysmemError::Map {
                source: io::Error::other(source),
            })?;
        let mut main = Self {
            flags: pmalloc_main.flags,
            base_addr: pmalloc_main.base,
            max_size: (pmalloc_main.max_pages as usize)
                .saturating_mul(pmalloc_main.page_size_bytes()),
            maps: Pool::with_capacity(numa_nodes.len()),
            pmalloc_main,
        };
        for &numa_node in numa_nodes {
            main.shared_map_create("buffers", map_size, log2_page_size, numa_node)?;
        }
        assert!(
            PHYSMEM_MAIN.set(main).is_ok(),
            "Physmem Main initializes once"
        );
        Ok(Self::global())
    }

    pub fn global() -> &'static Self {
        PHYSMEM_MAIN
            .get()
            .expect("Physmem Main is published before Buffer Main")
    }

    #[inline]
    pub fn base_addr(&self) -> usize {
        self.base_addr
    }

    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    #[inline]
    pub fn flags(&self) -> u32 {
        self.flags
    }

    #[inline]
    pub fn get_map(&self, index: u32) -> &PhysmemMap {
        self.maps
            .get(index)
            .expect("physmem map index names a live map")
    }

    #[inline]
    pub fn get_page_index(&self, address: usize) -> u32 {
        self.pmalloc_main.get_page_index(address)
    }

    #[inline]
    pub fn get_pa(&self, address: usize) -> usize {
        self.pmalloc_main.get_pa(address)
    }

    #[inline]
    pub fn convert_to_phys_addrs_with_offset(&self, addresses: &mut [usize], offset: i32) {
        self.pmalloc_main
            .convert_to_phys_addrs_with_offset(addresses, offset);
    }

    #[inline]
    pub fn convert_to_phys_addrs(&self, addresses: &mut [usize]) {
        self.pmalloc_main.convert_to_phys_addrs(addresses);
    }

    fn shared_map_create(
        &mut self,
        name: &str,
        size: usize,
        log2_page_size: u32,
        numa_node: u32,
    ) -> Result<u32, PhysmemError> {
        let base = self
            .pmalloc_main
            .create_shared_arena(name, size, log2_page_size, numa_node)
            .map_err(|source| PhysmemError::Map {
                source: io::Error::other(source),
            })?;
        let Some(base) = NonZeroUsize::new(base.addr()) else {
            return Err(PhysmemError::Create {
                source: io::Error::from_raw_os_error(libc::ENOMEM),
            });
        };
        let arena_index = self.pmalloc_main.get_arena(base.get()).index;
        let arena = self.pmalloc_main.arena(arena_index);
        let page_size = 1usize
            .checked_shl(log2_page_size)
            .expect("physmem page size fits usize");
        let n_pages = (arena.n_pages as usize)
            .checked_mul(arena.subpages_per_page as usize)
            .and_then(|count| u32::try_from(count).ok())
            .ok_or(PhysmemError::PageSizeOverflow {
                requested: PageSize::Bytes(byte_unit::Byte::from_u64(page_size as u64)),
            })?;
        let mapping_size =
            (n_pages as usize)
                .checked_mul(page_size)
                .ok_or(PhysmemError::PageSizeOverflow {
                    requested: PageSize::Bytes(byte_unit::Byte::from_u64(page_size as u64)),
                })?;
        let first_page = arena.first_page_index;
        let page_table = (0..arena.n_pages)
            .map(|offset| {
                let address = self.pmalloc_main.page_address_for_index(first_page)
                    + (offset as usize * page_size);
                self.pmalloc_main.get_pa(address)
            })
            .collect();
        let index = self.maps.insert(PhysmemMap {
            index: INVALID_MAP_INDEX,
            fd: arena.fd,
            base: base.get() as *mut u8,
            size: mapping_size,
            n_pages,
            page_table,
            log2_page_size,
            numa_node: arena.numa_node,
        });
        self.maps
            .get_mut(index)
            .expect("new physmem map is installed")
            .index = index;
        Ok(index)
    }
}

impl PhysmemMap {
    #[inline]
    pub fn base(&self) -> *mut u8 {
        self.base
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn page_size(&self) -> usize {
        1usize << self.log2_page_size
    }

    #[inline]
    pub fn numa_node(&self) -> u32 {
        self.numa_node
    }

    #[inline]
    pub fn fd(&self) -> RawFd {
        self.fd
    }

    #[inline]
    pub fn page_physical_address(&self, page_index: u32) -> usize {
        self.page_table
            .get(page_index as usize)
            .copied()
            .expect("physmem page index names a map page")
    }

    #[inline]
    pub fn page_count(&self) -> u32 {
        self.n_pages
    }
}

const _: () = {
    assert!(core::mem::align_of::<PhysmemMain>() == CACHE_LINE);
};
