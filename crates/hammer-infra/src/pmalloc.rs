use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, AsRawFd, IntoRawFd, RawFd};
use std::ptr::NonNull;

#[cfg(target_os = "linux")]
use std::os::unix::fs::FileExt;

use byte_unit::Byte;

use crate::align::CACHE_LINE;
use crate::aligned_vec::AlignedVec;
use crate::mem::{MemError, MemMain, PageSize};
use crate::pool::Pool;

pub const PMALLOC_LOG2_BLOCK_SIZE: u8 = 6;
pub const PMALLOC_BLOCK_SIZE: usize = 1 << PMALLOC_LOG2_BLOCK_SIZE;
pub const NUMA_LOCAL: u32 = u32::MAX;

const INVALID_INDEX: u32 = u32::MAX;
const ARENA_SHARED: u32 = 1 << 0;
const FLAG_NO_PAGEMAP: u32 = 1 << 0;

#[derive(Debug)]
pub struct PmallocChunk {
    pub start: u32,
    pub prev: u32,
    pub next: u32,
    pub size: u32,
    pub used: bool,
}

#[derive(Debug)]
pub struct PmallocPage {
    pub index: u32,
    pub arena_index: u32,
    pub chunks: Pool<PmallocChunk>,
    pub first_chunk_index: u32,
    pub n_free_chunks: u32,
    pub n_free_blocks: u32,
}

#[derive(Debug)]
pub struct PmallocArena {
    pub index: u32,
    pub flags: u32,
    pub fd: RawFd,
    pub numa_node: u32,
    pub first_page_index: u32,
    pub log2_subpage_size: u32,
    pub subpages_per_page: u32,
    pub n_pages: u32,
    pub name: String,
    pub page_indices: Vec<u32>,
}

#[repr(align(64))]
pub struct PmallocMain {
    pub flags: u32,
    pub base: usize,
    pub default_log2_page_size: u8,
    pub max_pages: u32,
    pub pages: Vec<PmallocPage>,
    pub chunk_index_by_va: HashMap<usize, u32>,
    pub arenas: Pool<PmallocArena>,
    pub default_arena_for_numa_node: Vec<u32>,
    pub lookup_table: AlignedVec<usize, CACHE_LINE>,
    pub linear_pa_offset: usize,
    pub linear_pa: bool,
    pub lookup_log2_page_size: u8,
}

impl PmallocMain {
    pub fn new() -> Self {
        Self {
            flags: 0,
            base: 0,
            default_log2_page_size: 0,
            max_pages: 0,
            pages: Vec::new(),
            chunk_index_by_va: HashMap::new(),
            arenas: Pool::new(),
            default_arena_for_numa_node: Vec::new(),
            lookup_table: AlignedVec::new(),
            linear_pa_offset: 0,
            linear_pa: false,
            lookup_log2_page_size: 0,
        }
    }

    pub fn initialize(
        &mut self,
        base_addr: Option<NonZeroUsize>,
        requested_size: usize,
    ) -> Result<(), MemError> {
        assert_eq!(self.base, 0, "pmalloc initializes once");
        let page_size = MemMain::default_hugepage_size().unwrap_or_else(MemMain::system_page_size);
        let page_log2 = Self::log2_page_size(page_size);
        let size = if requested_size == 0 {
            if usize::BITS >= 64 {
                16usize << 30
            } else {
                256usize << 20
            }
        } else {
            requested_size
        };
        let size = Self::round_up(size, page_size).ok_or(MemError::MainHeapSizeOverflow {
            requested: size,
            page_size,
        })?;
        let base = MemMain::vm_reserve(base_addr, size, page_size)?;

        self.flags = if Self::pagemap_available() {
            0
        } else {
            FLAG_NO_PAGEMAP
        };
        self.base = base.as_ptr().addr();
        self.default_log2_page_size = page_log2;
        self.lookup_log2_page_size = page_log2;
        self.max_pages = u32::try_from(size >> page_log2).expect("pmalloc page count fits u32");
        Ok(())
    }

    #[inline]
    pub fn get_page_index(&self, va: usize) -> u32 {
        assert!(self.base != 0 && va >= self.base, "VA belongs to pmalloc");
        let index = (va - self.base) >> self.default_log2_page_size;
        assert!(
            index < self.pages.len(),
            "VA belongs to a mapped pmalloc page"
        );
        index as u32
    }

    #[inline]
    pub fn get_arena(&self, va: usize) -> &PmallocArena {
        let page_index = self.get_page_index(va);
        let arena_index = self.pages[page_index as usize].arena_index;
        self.arenas
            .get(arena_index)
            .expect("pmalloc page has a live arena")
    }

    #[inline]
    pub fn arena(&self, index: u32) -> &PmallocArena {
        self.arenas
            .get(index)
            .expect("pmalloc arena index names a live arena")
    }

    #[inline]
    pub fn page_size_bytes(&self) -> usize {
        Self::page_size(self.default_log2_page_size)
    }

    #[inline]
    pub fn page_address_for_index(&self, index: u32) -> usize {
        assert!(
            (index as usize) < self.pages.len(),
            "pmalloc page index is live"
        );
        self.base + ((index as usize) << self.default_log2_page_size)
    }

    #[inline]
    pub fn get_pa(&self, va: usize) -> usize {
        assert!(self.base != 0 && va >= self.base, "VA belongs to pmalloc");
        let index = (va - self.base) >> self.lookup_log2_page_size;
        va - self.lookup_table[index]
    }

    #[inline]
    pub fn convert_to_phys_addrs_with_offset(&self, addresses: &mut [usize], offset: i32) {
        if self.linear_pa {
            for address in addresses {
                *address = address
                    .wrapping_sub(self.linear_pa_offset)
                    .wrapping_add_signed(offset as isize);
            }
            return;
        }

        let base = self.base;
        let shift = self.lookup_log2_page_size;
        for address in addresses {
            assert!(*address >= base, "VA belongs to pmalloc");
            let index = (*address - base) >> shift;
            *address = address
                .wrapping_sub(self.lookup_table[index])
                .wrapping_add_signed(offset as isize);
        }
    }

    #[inline]
    pub fn convert_to_phys_addrs(&self, addresses: &mut [usize]) {
        self.convert_to_phys_addrs_with_offset(addresses, 0);
    }

    pub fn alloc_aligned_on_numa(
        &mut self,
        size: usize,
        alignment: usize,
        numa_node: u32,
    ) -> Result<*mut u8, MemError> {
        self.alloc_from_arena_index(None, size, alignment, numa_node)
    }

    pub fn alloc_aligned(&mut self, size: usize, alignment: usize) -> Result<*mut u8, MemError> {
        self.alloc_aligned_on_numa(size, alignment, NUMA_LOCAL)
    }

    pub fn create_shared_arena(
        &mut self,
        name: &str,
        size: usize,
        log2_page_size: u32,
        numa_node: u32,
    ) -> Result<*mut u8, MemError> {
        self.assert_initialized();
        let log2_page_size = if log2_page_size == 0 {
            u32::from(self.default_log2_page_size)
        } else {
            log2_page_size
        };
        let system_log2 = Self::log2_page_size(MemMain::system_page_size());
        if log2_page_size != u32::from(self.default_log2_page_size)
            && log2_page_size != u32::from(system_log2)
        {
            return Err(MemError::PageSizeUnavailable {
                requested: PageSize::Bytes(Byte::from_u64(
                    1u64.checked_shl(log2_page_size).unwrap_or(0),
                )),
            });
        }
        let subpage_size =
            Self::page_size(u8::try_from(log2_page_size).expect("page size log2 fits u8"));
        let page_bytes = Self::page_size(self.default_log2_page_size);
        let n_pages = size
            .checked_add(page_bytes - 1)
            .map(|value| value / page_bytes)
            .unwrap_or(0);
        let n_pages = u32::try_from(n_pages).unwrap_or(u32::MAX);
        if n_pages == 0
            || self.pages.len().saturating_add(n_pages as usize) > self.max_pages as usize
        {
            return Ok(std::ptr::null_mut());
        }
        let numa_node = if numa_node == NUMA_LOCAL {
            0
        } else {
            numa_node
        };
        let arena_index = self.arenas.insert(PmallocArena {
            index: INVALID_INDEX,
            flags: ARENA_SHARED,
            fd: -1,
            numa_node,
            first_page_index: self.pages.len() as u32,
            log2_subpage_size: log2_page_size,
            subpages_per_page: 1u32 << (self.default_log2_page_size - log2_page_size as u8),
            n_pages: 0,
            name: name.to_owned(),
            page_indices: Vec::new(),
        });
        self.arenas.get_mut(arena_index).unwrap().index = arena_index;
        match self.map_pages(arena_index, n_pages, subpage_size) {
            Ok(true) => {}
            Ok(false) => {
                assert!(self.arenas.remove(arena_index).is_some());
                return Ok(std::ptr::null_mut());
            }
            Err(error) => {
                let arena = self.arenas.remove(arena_index).unwrap();
                if arena.fd >= 0 {
                    unsafe { libc::close(arena.fd) };
                }
                return Err(error);
            }
        }
        let page_index = self.arenas.get(arena_index).unwrap().first_page_index;
        Ok(self.page_address(page_index))
    }

    pub fn alloc_from_arena(
        &mut self,
        arena_va: *mut u8,
        size: usize,
        alignment: usize,
    ) -> Result<*mut u8, MemError> {
        let arena_index = self.get_arena(arena_va.addr()).index;
        self.alloc_from_arena_index(Some(arena_index), size, alignment, 0)
    }

    pub fn free(&mut self, va: *mut u8) {
        let va = va.addr();
        let chunk_index = self
            .chunk_index_by_va
            .remove(&va)
            .unwrap_or_else(|| panic!("pmalloc free of unknown VA {va:#x}"));
        let page_index = self.get_page_index(va) as usize;
        let arena_index = self.pages[page_index].arena_index;
        let subpage_shift = self.arenas.get(arena_index).unwrap().log2_subpage_size;
        let subpage_blocks = 1u32 << (subpage_shift - u32::from(PMALLOC_LOG2_BLOCK_SIZE));
        let page = &mut self.pages[page_index];
        let chunk = page
            .chunks
            .get_mut(chunk_index)
            .expect("pmalloc VA index points to a live chunk");
        assert!(chunk.used, "pmalloc double free");
        chunk.used = false;
        page.n_free_blocks += chunk.size;
        page.n_free_chunks += 1;

        let next_index = page.chunks.get(chunk_index).unwrap().next;
        if next_index != INVALID_INDEX
            && !page.chunks.get(next_index).unwrap().used
            && page.chunks.get(next_index).unwrap().start / subpage_blocks
                == page.chunks.get(chunk_index).unwrap().start / subpage_blocks
        {
            let next_next = page.chunks.get(next_index).unwrap().next;
            let next_size = page.chunks.get(next_index).unwrap().size;
            page.chunks.get_mut(chunk_index).unwrap().size += next_size;
            page.chunks.get_mut(chunk_index).unwrap().next = next_next;
            if next_next != INVALID_INDEX {
                page.chunks.get_mut(next_next).unwrap().prev = chunk_index;
            }
            page.chunks.put_index(next_index);
            page.n_free_chunks -= 1;
        }

        let prev_index = page.chunks.get(chunk_index).unwrap().prev;
        if prev_index != INVALID_INDEX
            && !page.chunks.get(prev_index).unwrap().used
            && page.chunks.get(prev_index).unwrap().start / subpage_blocks
                == page.chunks.get(chunk_index).unwrap().start / subpage_blocks
        {
            let next_index = page.chunks.get(chunk_index).unwrap().next;
            let chunk_size = page.chunks.get(chunk_index).unwrap().size;
            page.chunks.get_mut(prev_index).unwrap().size += chunk_size;
            page.chunks.get_mut(prev_index).unwrap().next = next_index;
            if next_index != INVALID_INDEX {
                page.chunks.get_mut(next_index).unwrap().prev = prev_index;
            }
            page.chunks.put_index(chunk_index);
            page.n_free_chunks -= 1;
        }
    }

    fn alloc_from_arena_index(
        &mut self,
        arena_index: Option<u32>,
        size: usize,
        alignment: usize,
        numa_node: u32,
    ) -> Result<*mut u8, MemError> {
        self.assert_initialized();
        assert!(
            alignment.is_power_of_two(),
            "pmalloc alignment must be a power of two"
        );
        assert!(
            alignment >= PMALLOC_BLOCK_SIZE,
            "pmalloc alignment is cache-line based"
        );
        if size == 0 || size > Self::page_size(self.default_log2_page_size) {
            return Ok(std::ptr::null_mut());
        }
        let numa_node = if numa_node == NUMA_LOCAL {
            0
        } else {
            numa_node
        };
        let (arena_index, created_default_arena) = match arena_index {
            Some(index) => (index, false),
            None => {
                if self.default_arena_for_numa_node.len() <= numa_node as usize {
                    self.default_arena_for_numa_node
                        .resize(numa_node as usize + 1, INVALID_INDEX);
                }
                let index = self.default_arena_for_numa_node[numa_node as usize];
                if index != INVALID_INDEX {
                    (index, false)
                } else {
                    let index = self.arenas.insert(PmallocArena {
                        index: INVALID_INDEX,
                        flags: 0,
                        fd: -1,
                        numa_node,
                        first_page_index: self.pages.len() as u32,
                        log2_subpage_size: u32::from(self.default_log2_page_size),
                        subpages_per_page: 1,
                        n_pages: 0,
                        name: format!("default-numa-{numa_node}"),
                        page_indices: Vec::new(),
                    });
                    self.arenas.get_mut(index).unwrap().index = index;
                    self.default_arena_for_numa_node[numa_node as usize] = index;
                    (index, true)
                }
            }
        };
        let subpage_size = Self::page_size(
            u8::try_from(self.arenas.get(arena_index).unwrap().log2_subpage_size)
                .expect("page size log2 fits u8"),
        );
        if size > subpage_size {
            return Ok(std::ptr::null_mut());
        }
        let page_indices = self.arenas.get(arena_index).unwrap().page_indices.clone();
        let Some(rounded_size) = Self::round_up(size, PMALLOC_BLOCK_SIZE) else {
            return Ok(std::ptr::null_mut());
        };
        let n_blocks = rounded_size / PMALLOC_BLOCK_SIZE;
        let block_align = alignment / PMALLOC_BLOCK_SIZE;
        for page_index in page_indices {
            if let Some(pointer) =
                self.alloc_chunk_from_page(page_index, n_blocks as u32, block_align as u32)
            {
                return Ok(pointer);
            }
        }
        if self.arenas.get(arena_index).unwrap().flags & ARENA_SHARED != 0 {
            return Ok(std::ptr::null_mut());
        }
        let mapped = match self.map_pages(arena_index, 1, subpage_size) {
            Ok(mapped) => mapped,
            Err(error) => {
                if created_default_arena {
                    assert!(self.arenas.remove(arena_index).is_some());
                    self.default_arena_for_numa_node[numa_node as usize] = INVALID_INDEX;
                }
                return Err(error);
            }
        };
        if !mapped {
            if created_default_arena {
                assert!(self.arenas.remove(arena_index).is_some());
                self.default_arena_for_numa_node[numa_node as usize] = INVALID_INDEX;
            }
            return Ok(std::ptr::null_mut());
        }
        let page_index = self
            .arenas
            .get(arena_index)
            .unwrap()
            .page_indices
            .last()
            .copied()
            .unwrap();
        Ok(self
            .alloc_chunk_from_page(page_index, n_blocks as u32, block_align as u32)
            .unwrap_or(std::ptr::null_mut()))
    }

    fn alloc_chunk_from_page(
        &mut self,
        page_index: u32,
        n_blocks: u32,
        block_align: u32,
    ) -> Option<*mut u8> {
        let page_position = page_index as usize;
        let arena_index = self.pages[page_position].arena_index;
        let subpages = self.arenas.get(arena_index).unwrap().subpages_per_page;
        if self.pages[page_position].chunks.is_empty() {
            let total_blocks = self.pages[page_position].n_free_blocks;
            let blocks_per_chunk = total_blocks / subpages;
            let mut previous = INVALID_INDEX;
            for chunk_number in 0..subpages {
                let index = self.pages[page_position].chunks.insert(PmallocChunk {
                    start: chunk_number * blocks_per_chunk,
                    prev: previous,
                    next: INVALID_INDEX,
                    size: blocks_per_chunk,
                    used: false,
                });
                if previous == INVALID_INDEX {
                    self.pages[page_position].first_chunk_index = index;
                } else {
                    self.pages[page_position]
                        .chunks
                        .get_mut(previous)
                        .unwrap()
                        .next = index;
                }
                previous = index;
            }
            self.pages[page_position].n_free_chunks = subpages;
        }
        if self.pages[page_position].n_free_blocks < n_blocks {
            return None;
        }

        let mut chunk_index = self.pages[page_position].first_chunk_index;
        loop {
            let chunk = self.pages[page_position].chunks.get(chunk_index).unwrap();
            let offset = (block_align - (chunk.start & (block_align - 1))) & (block_align - 1);
            if !chunk.used && n_blocks + offset <= chunk.size {
                break;
            }
            if chunk.next == INVALID_INDEX {
                return None;
            }
            chunk_index = chunk.next;
        }

        let offset = {
            let chunk = self.pages[page_position].chunks.get(chunk_index).unwrap();
            (block_align - (chunk.start & (block_align - 1))) & (block_align - 1)
        };
        if offset != 0 {
            let chunk = self.pages[page_position].chunks.get(chunk_index).unwrap();
            let old_next = chunk.next;
            let old_start = chunk.start;
            let old_size = chunk.size;
            let split_index = self.pages[page_position].chunks.insert(PmallocChunk {
                start: old_start + offset,
                prev: chunk_index,
                next: old_next,
                size: old_size - offset,
                used: false,
            });
            self.pages[page_position]
                .chunks
                .get_mut(chunk_index)
                .unwrap()
                .size = offset;
            self.pages[page_position]
                .chunks
                .get_mut(chunk_index)
                .unwrap()
                .next = split_index;
            if old_next != INVALID_INDEX {
                self.pages[page_position]
                    .chunks
                    .get_mut(old_next)
                    .unwrap()
                    .prev = split_index;
            }
            self.pages[page_position].n_free_chunks += 1;
            chunk_index = split_index;
        }

        let current_size = self.pages[page_position]
            .chunks
            .get(chunk_index)
            .unwrap()
            .size;
        if current_size > n_blocks {
            let old_next = self.pages[page_position]
                .chunks
                .get(chunk_index)
                .unwrap()
                .next;
            let tail_start = self.pages[page_position]
                .chunks
                .get(chunk_index)
                .unwrap()
                .start
                + n_blocks;
            let tail_index = self.pages[page_position].chunks.insert(PmallocChunk {
                start: tail_start,
                prev: chunk_index,
                next: old_next,
                size: current_size - n_blocks,
                used: false,
            });
            self.pages[page_position]
                .chunks
                .get_mut(chunk_index)
                .unwrap()
                .size = n_blocks;
            self.pages[page_position]
                .chunks
                .get_mut(chunk_index)
                .unwrap()
                .next = tail_index;
            if old_next != INVALID_INDEX {
                self.pages[page_position]
                    .chunks
                    .get_mut(old_next)
                    .unwrap()
                    .prev = tail_index;
            }
            self.pages[page_position].n_free_chunks += 1;
        }
        self.pages[page_position]
            .chunks
            .get_mut(chunk_index)
            .unwrap()
            .used = true;
        self.pages[page_position].n_free_blocks -= n_blocks;
        self.pages[page_position].n_free_chunks -= 1;
        let va = self.page_address(page_index).addr()
            + self.pages[page_position]
                .chunks
                .get(chunk_index)
                .unwrap()
                .start as usize
                * PMALLOC_BLOCK_SIZE;
        assert!(self.chunk_index_by_va.insert(va, chunk_index).is_none());
        Some(va as *mut u8)
    }

    fn map_pages(
        &mut self,
        arena_index: u32,
        n_pages: u32,
        subpage_size: usize,
    ) -> Result<bool, MemError> {
        if n_pages == 0
            || self.pages.len().saturating_add(n_pages as usize) > self.max_pages as usize
        {
            return Ok(false);
        }
        let (numa_node, flags, name) = {
            let arena = self.arenas.get(arena_index).unwrap();
            (arena.numa_node, arena.flags, arena.name.clone())
        };
        let numa_node = if numa_node == NUMA_LOCAL {
            0
        } else {
            numa_node
        };
        MemMain::set_numa_affinity(numa_node, true)?;
        let map_size = (n_pages as usize)
            .checked_shl(self.default_log2_page_size.into())
            .expect("pmalloc mapping size overflow");
        let page_offset = self
            .pages
            .len()
            .checked_shl(self.default_log2_page_size.into())
            .expect("pmalloc page offset overflow");
        let map_base = NonNull::new((self.base + page_offset) as *mut u8).unwrap();
        let mut backing = if flags & ARENA_SHARED != 0 {
            let page_size = PageSize::Bytes(Byte::from_u64(subpage_size as u64));
            let fd = match MemMain::vm_create_backing(page_size, &name) {
                Ok(fd) => fd,
                Err(error) => {
                    if MemMain::set_default_numa_affinity().is_err() {
                        std::process::abort();
                    }
                    return Err(error);
                }
            };
            if unsafe { libc::ftruncate(fd.as_raw_fd(), map_size as libc::off_t) } != 0 {
                let error = MemError::VirtualMemoryMap {
                    requested_base: Some(map_base.as_ptr().addr()),
                    size: map_size,
                    page_size_log2: subpage_size.trailing_zeros() as u8,
                    backing_fd: Some(fd.as_raw_fd()),
                    backing_offset: 0,
                    source: io::Error::last_os_error(),
                };
                if MemMain::set_default_numa_affinity().is_err() {
                    std::process::abort();
                }
                return Err(error);
            }
            Some(fd)
        } else {
            None
        };
        let page_size = PageSize::Bytes(Byte::from_u64(subpage_size as u64));
        if let Err(error) = MemMain::vm_map_reserved(
            map_base,
            map_size,
            page_size,
            backing.as_ref().map(AsFd::as_fd),
            0,
            false,
        ) {
            if MemMain::set_default_numa_affinity().is_err() {
                std::process::abort();
            }
            return Err(error);
        }
        if let Err(error) = MemMain::set_default_numa_affinity() {
            if MemMain::vm_restore_reserved(map_base, map_size).is_err() {
                std::process::abort();
            }
            return Err(error);
        }
        if let Some(fd) = backing.take() {
            self.arenas.get_mut(arena_index).unwrap().fd = fd.into_raw_fd();
        }
        let blocks_per_page = 1u32 << (self.default_log2_page_size - PMALLOC_LOG2_BLOCK_SIZE);
        let first_page = self.pages.len() as u32;
        for index in 0..n_pages {
            let page_index = first_page + index;
            self.pages.push(PmallocPage {
                index: page_index,
                arena_index,
                chunks: Pool::new(),
                first_chunk_index: INVALID_INDEX,
                n_free_chunks: 0,
                n_free_blocks: blocks_per_page,
            });
            let arena = self.arenas.get_mut(arena_index).unwrap();
            arena.page_indices.push(page_index);
            arena.n_pages += 1;
        }
        self.update_lookup_table();
        Ok(true)
    }

    fn update_lookup_table(&mut self) {
        let elements_per_page =
            1usize << (self.default_log2_page_size - self.lookup_log2_page_size);
        let count = self.pages.len() * elements_per_page;
        self.lookup_table.resize(count, 0);
        let mut linear_offset = None;
        let mut linear = true;
        for index in 0..count {
            let va = self.base + (index << self.lookup_log2_page_size);
            let pa = if self.flags & FLAG_NO_PAGEMAP != 0 {
                0
            } else {
                Self::read_physical_address(va).unwrap_or(0)
            };
            let offset = va.wrapping_sub(pa);
            self.lookup_table[index] = offset;
            if let Some(first) = linear_offset {
                if first != offset {
                    linear = false;
                }
            } else {
                linear_offset = Some(offset);
            }
        }
        self.linear_pa = linear && linear_offset.is_some();
        self.linear_pa_offset = linear_offset.unwrap_or(0);
    }

    fn page_address(&self, page_index: u32) -> *mut u8 {
        (self.base + ((page_index as usize) << self.default_log2_page_size)) as *mut u8
    }

    fn assert_initialized(&self) {
        assert!(
            self.base != 0 && self.max_pages != 0,
            "pmalloc is initialized"
        );
    }

    fn page_size(log2: u8) -> usize {
        1usize
            .checked_shl(u32::from(log2))
            .expect("page size fits usize")
    }

    fn log2_page_size(size: usize) -> u8 {
        assert!(size.is_power_of_two(), "page size must be a power of two");
        size.trailing_zeros() as u8
    }

    fn round_up(value: usize, alignment: usize) -> Option<usize> {
        value
            .checked_add(alignment - 1)
            .map(|rounded| rounded & !(alignment - 1))
    }

    fn pagemap_available() -> bool {
        #[cfg(target_os = "linux")]
        {
            File::open("/proc/self/pagemap").is_ok()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    #[cfg(target_os = "linux")]
    fn read_physical_address(va: usize) -> Result<usize, io::Error> {
        let file = File::open("/proc/self/pagemap")?;
        let page_size = MemMain::system_page_size();
        let offset = (va / page_size).checked_mul(8).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "pagemap offset overflow")
        })?;
        let mut entry = [0u8; 8];
        file.read_at(&mut entry, offset as u64)?;
        let value = u64::from_ne_bytes(entry);
        if value & (1u64 << 63) == 0 {
            return Ok(0);
        }
        Ok(((value & ((1u64 << 55) - 1)) as usize) * page_size)
    }

    #[cfg(not(target_os = "linux"))]
    fn read_physical_address(_: usize) -> Result<usize, io::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "physical address lookup is unsupported on this platform",
        ))
    }
}

impl Default for PmallocMain {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PmallocMain {
    fn drop(&mut self) {
        for arena_index in self.arenas.indices().collect::<Vec<_>>() {
            let arena = self.arenas.get(arena_index).unwrap();
            if arena.fd >= 0 {
                unsafe { libc::close(arena.fd) };
            }
        }
        if self.base != 0 {
            let base = NonNull::new(self.base as *mut u8).unwrap();
            let size = (self.max_pages as usize) << self.default_log2_page_size;
            if unsafe { MemMain::vm_unmap_reserved(base, size) }.is_err() {
                std::process::abort();
            }
        }
    }
}
