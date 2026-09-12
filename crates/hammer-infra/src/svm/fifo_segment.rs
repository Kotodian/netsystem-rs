//! VPP-style FIFO segment ownership and shared freelists.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::svm::fifo::{
    FS_CHUNK_VEC_LEN, FS_MIN_LOG2_CHUNK_SIZE, Fifo, FifoError, FifoSegmentHeader, FifoSegmentSlice,
    SvmFifoChunk,
};
use crate::svm::ssvm::{SSVM_PAYLOAD_OFFSET, SsvmPrivate};

const FIFO_SEGMENT_ALIGN: u64 = 64;
const CHUNK_HEADER_SIZE: usize = std::mem::size_of::<SvmFifoChunk>();

#[derive(Debug, Clone, Copy)]
pub struct SvmFifoSegmentConfig {
    pub slices: u32,
}

/// Owner-local FIFO handles backed by a shared `fifo_segment_header_t`.
/// Shared chunk/header freelists are never reconstructed by an attacher.
pub struct SvmFifoSegment {
    segment: Arc<SsvmPrivate>,
    header_offset: u64,
    header: *mut FifoSegmentHeader,
    slices: Vec<Vec<Option<Fifo>>>,
}

unsafe impl Send for SvmFifoSegment {}
unsafe impl Sync for SvmFifoSegment {}

#[derive(Debug, thiserror::Error)]
pub enum FifoSegmentError {
    #[error("FIFO segment header is not published")]
    HeaderMissing,
    #[error("FIFO segment slice count {count} is invalid")]
    InvalidSliceCount { count: u32 },
    #[error("FIFO segment header is outside the mapping")]
    HeaderOutOfBounds,
    #[error("FIFO segment is not ready")]
    NotReady,
}

pub struct SvmFifoSegmentMain {
    segments: Vec<Option<SvmFifoSegment>>,
}

#[inline]
const fn align_offset(offset: u64) -> u64 {
    (offset + FIFO_SEGMENT_ALIGN - 1) & !(FIFO_SEGMENT_ALIGN - 1)
}

#[inline]
fn chunk_class(size: usize) -> usize {
    let size = size.max(1 << FS_MIN_LOG2_CHUNK_SIZE);
    let log2 = usize::BITS - size.saturating_sub(1).leading_zeros();
    (log2.saturating_sub(FS_MIN_LOG2_CHUNK_SIZE) as usize).min(FS_CHUNK_VEC_LEN - 1)
}

#[inline]
fn chunk_size(size: usize) -> usize {
    1usize << (FS_MIN_LOG2_CHUNK_SIZE + chunk_class(size) as u32)
}

impl SvmFifoSegment {
    pub fn new(segment: Arc<SsvmPrivate>, config: SvmFifoSegmentConfig) -> Result<Self, FifoError> {
        if config.slices == 0 {
            return Err(FifoError::InvalidCapacity);
        }
        if !segment.is_server() {
            return Self::attach(segment, config).map_err(|_| FifoError::SegmentExhausted);
        }
        let header_offset = align_offset(SSVM_PAYLOAD_OFFSET);
        let header_bytes = FifoSegmentHeader::layout_bytes(config.slices as usize);
        let header_end = header_offset
            .checked_add(header_bytes as u64)
            .ok_or(FifoError::SegmentExhausted)?;
        if header_end >= segment.ssvm_size() as u64 {
            return Err(FifoError::SegmentExhausted);
        }
        let header = unsafe {
            segment
                .base()
                .add(header_offset as usize)
                .cast::<FifoSegmentHeader>()
        };
        unsafe {
            std::ptr::write(
                header,
                FifoSegmentHeader {
                    n_cached_bytes: std::sync::atomic::AtomicU64::new(0),
                    n_active_fifos: std::sync::atomic::AtomicU32::new(0),
                    n_reserved_bytes: std::sync::atomic::AtomicU32::new(header_bytes as u32),
                    max_log2_fifo_size: 31,
                    n_slices: config.slices as u8,
                    pct_first_alloc: 100,
                    n_mqs: 0,
                    _header_pad: [0; 36],
                    byte_index: std::sync::atomic::AtomicU64::new(header_bytes as u64),
                    max_byte_index: segment.ssvm_size() as u64 - header_offset,
                    start_byte_index: header_bytes as u64,
                    _slice_pad: [0; 40],
                },
            );
            for slice in (*header).slices_mut() {
                std::ptr::write(slice, FifoSegmentSlice::new());
            }
        }
        segment.set_fifo_segment_offset(header_offset);
        segment.publish_ready();
        let mut slices = Vec::with_capacity(config.slices as usize);
        slices.resize_with(config.slices as usize, Vec::new);
        Ok(Self {
            segment,
            header_offset,
            header,
            slices,
        })
    }

    pub fn attach(
        segment: Arc<SsvmPrivate>,
        config: SvmFifoSegmentConfig,
    ) -> Result<Self, FifoSegmentError> {
        let header_offset = segment
            .fifo_segment_offset()
            .ok_or(FifoSegmentError::HeaderMissing)?;
        let end = header_offset
            .checked_add(std::mem::size_of::<FifoSegmentHeader>() as u64)
            .ok_or(FifoSegmentError::HeaderOutOfBounds)?;
        if end > segment.ssvm_size() as u64 {
            return Err(FifoSegmentError::HeaderOutOfBounds);
        }
        if !segment.is_ready() {
            return Err(FifoSegmentError::NotReady);
        }
        let header = unsafe {
            segment
                .base()
                .add(header_offset as usize)
                .cast::<FifoSegmentHeader>()
        };
        let shared_slices = unsafe { (*header).n_slices as u32 };
        if shared_slices == 0 || (config.slices != 0 && config.slices != shared_slices) {
            return Err(FifoSegmentError::InvalidSliceCount {
                count: shared_slices,
            });
        }
        let layout = FifoSegmentHeader::layout_bytes(shared_slices as usize) as u64;
        if header_offset
            .checked_add(layout)
            .is_none_or(|end| end > segment.ssvm_size() as u64)
        {
            return Err(FifoSegmentError::HeaderOutOfBounds);
        }
        let mut slices = Vec::with_capacity(shared_slices as usize);
        slices.resize_with(shared_slices as usize, Vec::new);
        Ok(Self {
            segment,
            header_offset,
            header,
            slices,
        })
    }

    #[inline]
    fn header(&self) -> &FifoSegmentHeader {
        unsafe { &*self.header }
    }

    #[inline]
    fn slice(&self, index: u32) -> Option<&FifoSegmentSlice> {
        unsafe { self.header().slices().get(index as usize) }
    }

    fn allocate_block(&self, bytes: usize, align: usize) -> Result<u64, FifoError> {
        let header = self.header();
        let mut current = header.byte_index.load(Ordering::Relaxed);
        loop {
            let aligned = (current + align as u64 - 1) & !(align as u64 - 1);
            let end = aligned
                .checked_add(bytes as u64)
                .ok_or(FifoError::SegmentExhausted)?;
            if end >= header.max_byte_index {
                return Err(FifoError::SegmentExhausted);
            }
            match header.byte_index.compare_exchange_weak(
                current,
                end,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(aligned),
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    fn shared_slice(&self, index: u32) -> Option<&FifoSegmentSlice> {
        self.slice(index)
    }

    #[inline]
    fn chunk_ptr(&self, offset: u64) -> *mut SvmFifoChunk {
        unsafe {
            self.segment
                .base()
                .add(self.header_offset as usize + offset as usize)
                .cast::<SvmFifoChunk>()
        }
    }

    #[inline]
    fn fifo_header_ptr(&self, offset: u64) -> *mut crate::svm::fifo::SvmFifoShared {
        unsafe {
            self.segment
                .base()
                .add(self.header_offset as usize + offset as usize)
                .cast::<crate::svm::fifo::SvmFifoShared>()
        }
    }

    fn pop_chunk(&self, slice: u32, class: usize) -> Option<u64> {
        let shared = self.shared_slice(slice)?;
        let mut head = shared.free_chunks[class].load(Ordering::Acquire);
        while head != 0 {
            let chunk = self.chunk_ptr(head);
            let next = unsafe { (*chunk).next.load(Ordering::Relaxed) };
            match shared.free_chunks[class].compare_exchange_weak(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let bytes = unsafe { (*chunk).length.load(Ordering::Relaxed) } as u64;
                    shared.n_fl_chunk_bytes.fetch_sub(bytes, Ordering::Relaxed);
                    self.header()
                        .n_cached_bytes
                        .fetch_sub(bytes, Ordering::Relaxed);
                    unsafe { (*chunk).next.store(0, Ordering::Relaxed) };
                    return Some(head);
                }
                Err(observed) => head = observed,
            }
        }
        None
    }

    fn push_chunk(&self, slice: u32, class: usize, offset: u64) {
        let shared = self
            .shared_slice(slice)
            .expect("FIFO chunk returned to an invalid slice");
        let chunk = self.chunk_ptr(offset);
        let bytes = unsafe { (*chunk).length.load(Ordering::Relaxed) } as u64;
        let mut head = shared.free_chunks[class].load(Ordering::Acquire);
        loop {
            unsafe { (*chunk).next.store(head, Ordering::Relaxed) };
            match shared.free_chunks[class].compare_exchange_weak(
                head,
                offset,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => head = observed,
            }
        }
        shared.n_fl_chunk_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.header()
            .n_cached_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn pop_fifo_header(&self, slice: u32) -> Option<u64> {
        let shared = self.shared_slice(slice)?;
        let head = shared.free_fifos.swap(0, Ordering::AcqRel);
        if head == 0 {
            return None;
        }
        let next = unsafe { (*self.fifo_header_ptr(head)).next.load(Ordering::Relaxed) };
        shared.free_fifos.store(next, Ordering::Release);
        unsafe {
            (*self.fifo_header_ptr(head))
                .next
                .store(0, Ordering::Relaxed)
        };
        Some(head)
    }

    fn push_fifo_header(&self, slice: u32, offset: u64) {
        let shared = self
            .shared_slice(slice)
            .expect("FIFO header returned to an invalid slice");
        let header = self.fifo_header_ptr(offset);
        let old = shared.free_fifos.load(Ordering::Acquire);
        unsafe { (*header).next.store(old, Ordering::Relaxed) };
        shared.free_fifos.store(offset, Ordering::Release);
    }

    fn allocate_chunk(&self, start_byte: u32) -> Result<u64, FifoError> {
        let offset = self.allocate_block(CHUNK_HEADER_SIZE + (1 << FS_MIN_LOG2_CHUNK_SIZE), 8)?;
        let chunk = self.chunk_ptr(offset);
        unsafe {
            std::ptr::write(
                chunk,
                SvmFifoChunk {
                    start_byte,
                    length: std::sync::atomic::AtomicU32::new(1 << FS_MIN_LOG2_CHUNK_SIZE),
                    next: std::sync::atomic::AtomicU64::new(0),
                    enq_rb_index: u32::MAX,
                    deq_rb_index: u32::MAX,
                },
            );
        }
        Ok(offset)
    }

    pub fn allocate_fifo(&mut self, slice: u32, capacity: usize) -> Result<u32, FifoError> {
        if !self.segment.is_server() {
            return Err(FifoError::SegmentExhausted);
        }
        if self.slices.get(slice as usize).is_none() {
            return Err(FifoError::InvalidCapacity);
        }
        if capacity == 0 || capacity > u32::MAX as usize {
            return Err(FifoError::CapacityOutOfRange { capacity });
        }

        let header_offset = self.pop_fifo_header(slice).map_or_else(
            || {
                self.allocate_block(
                    std::mem::size_of::<crate::svm::fifo::SvmFifoShared>(),
                    FIFO_SEGMENT_ALIGN as usize,
                )
            },
            Ok,
        )?;
        let needed_chunks = capacity.div_ceil(1 << FS_MIN_LOG2_CHUNK_SIZE);
        let mut chunks = Vec::with_capacity(needed_chunks);
        for index in 0..needed_chunks {
            let offset = self.pop_chunk(slice, 0).map_or_else(
                || self.allocate_chunk((index << FS_MIN_LOG2_CHUNK_SIZE) as u32),
                Ok,
            );
            match offset {
                Ok(offset) => chunks.push(offset),
                Err(error) => {
                    for chunk in chunks {
                        self.push_chunk(slice, 0, chunk);
                    }
                    self.push_fifo_header(slice, header_offset);
                    return Err(error);
                }
            }
        }
        for (index, &offset) in chunks.iter().enumerate() {
            let next = chunks.get(index + 1).copied().unwrap_or(0);
            let chunk = self.chunk_ptr(offset);
            unsafe {
                (*chunk).start_byte = (index << FS_MIN_LOG2_CHUNK_SIZE) as u32;
                (*chunk)
                    .length
                    .store(1 << FS_MIN_LOG2_CHUNK_SIZE, Ordering::Relaxed);
                (*chunk).next.store(next, Ordering::Release);
            }
        }
        let fifo = unsafe {
            Fifo::init_at_svm_shared(
                Arc::clone(&self.segment),
                header_offset,
                capacity,
                chunks[0],
                *chunks.last().expect("FIFO always has one chunk"),
            )?
        };
        let target = self
            .slices
            .get_mut(slice as usize)
            .ok_or(FifoError::InvalidCapacity)?;
        let index = target.len() as u32;
        target.push(Some(fifo));
        self.header().n_active_fifos.fetch_add(1, Ordering::Relaxed);
        self.slice(slice)
            .expect("validated FIFO segment slice")
            .virtual_mem
            .fetch_add(capacity as u64, Ordering::Relaxed);
        Ok(index)
    }

    pub fn fifo(&self, slice: u32, index: u32) -> Option<&Fifo> {
        self.slices
            .get(slice as usize)?
            .get(index as usize)?
            .as_ref()
    }

    pub fn fifo_mut(&mut self, slice: u32, index: u32) -> Option<&mut Fifo> {
        self.slices
            .get_mut(slice as usize)?
            .get_mut(index as usize)?
            .as_mut()
    }

    /// Attaches a process-local FIFO object to a shared FIFO header offset.
    /// The shared header and chunk chain are owned by the creator; this method
    /// only reconstructs the caller's private lookup and OOO state.
    pub fn attach_fifo(&mut self, slice: u32, header_offset: u64) -> Result<u32, FifoError> {
        if self.slices.get(slice as usize).is_none() {
            return Err(FifoError::InvalidCapacity);
        }
        let fifo = unsafe { Fifo::attach_at_svm_shared(Arc::clone(&self.segment), header_offset)? };
        let capacity = fifo.size();
        let target = self
            .slices
            .get_mut(slice as usize)
            .ok_or(FifoError::InvalidCapacity)?;
        let index = target.len() as u32;
        target.push(Some(fifo));
        self.slice(slice)
            .expect("validated FIFO segment slice")
            .virtual_mem
            .fetch_add(capacity as u64, Ordering::Relaxed);
        Ok(index)
    }

    /// Drops only the local private FIFO object for an attached FIFO. The
    /// shared header/chunks remain available to the creator until it frees the
    /// owning FIFO.
    pub fn detach_fifo(&mut self, slice: u32, index: u32) -> Result<(), FifoError> {
        let target = self
            .slices
            .get_mut(slice as usize)
            .ok_or(FifoError::InvalidCapacity)?;
        let fifo = target
            .get_mut(index as usize)
            .ok_or(FifoError::InvalidCapacity)?
            .take()
            .ok_or(FifoError::InvalidCapacity)?;
        self.slice(slice)
            .expect("validated FIFO segment slice")
            .virtual_mem
            .fetch_sub(fifo.size() as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn free_fifo(&mut self, slice: u32, index: u32) -> Result<(), FifoError> {
        let target = self
            .slices
            .get_mut(slice as usize)
            .ok_or(FifoError::InvalidCapacity)?;
        let slot = target
            .get_mut(index as usize)
            .ok_or(FifoError::InvalidCapacity)?;
        let fifo = slot.take().ok_or(FifoError::InvalidCapacity)?;
        let mut chunk_offset = unsafe { (*fifo.shr).start_chunk.load(Ordering::Acquire) };
        while chunk_offset != 0 {
            let chunk = self.chunk_ptr(chunk_offset);
            let next = unsafe { (*chunk).next.load(Ordering::Acquire) };
            let class = chunk_class(unsafe { (*chunk).length.load(Ordering::Relaxed) as usize });
            self.push_chunk(slice, class, chunk_offset);
            chunk_offset = next;
        }
        unsafe {
            (*fifo.shr).start_chunk.store(0, Ordering::Relaxed);
            (*fifo.shr).end_chunk.store(0, Ordering::Relaxed);
            (*fifo.shr).head_chunk.store(0, Ordering::Relaxed);
            (*fifo.shr).tail_chunk.store(0, Ordering::Relaxed);
        }
        self.push_fifo_header(slice, fifo.hdr_offset());
        self.header().n_active_fifos.fetch_sub(1, Ordering::Relaxed);
        self.slice(slice)
            .expect("validated FIFO segment slice")
            .virtual_mem
            .fetch_sub(fifo.size() as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn preallocate_chunks(
        &mut self,
        slice: u32,
        size: u32,
        count: u32,
    ) -> Result<(), FifoError> {
        let shared = self.slice(slice).ok_or(FifoError::InvalidCapacity)?;
        if size == 0 {
            return Err(FifoError::InvalidCapacity);
        }
        if count == 0 {
            return Ok(());
        }
        let physical = chunk_size(size as usize);
        let stride = CHUNK_HEADER_SIZE + physical;
        let first = self.allocate_block(stride.saturating_mul(count as usize), 8)?;
        let class = chunk_class(size as usize);
        let mut head = shared.free_chunks[class].load(Ordering::Acquire);
        for index in (0..count).rev() {
            let offset = first + index as u64 * stride as u64;
            let chunk = unsafe {
                self.segment
                    .base()
                    .add(self.header_offset as usize + offset as usize)
                    .cast::<SvmFifoChunk>()
            };
            unsafe {
                std::ptr::write(
                    chunk,
                    SvmFifoChunk {
                        start_byte: 0,
                        length: std::sync::atomic::AtomicU32::new(physical as u32),
                        next: std::sync::atomic::AtomicU64::new(head),
                        enq_rb_index: u32::MAX,
                        deq_rb_index: u32::MAX,
                    },
                );
            }
            head = offset;
        }
        shared.free_chunks[class].store(head, Ordering::Release);
        shared.num_chunks[class].fetch_add(count, Ordering::Relaxed);
        shared
            .n_fl_chunk_bytes
            .fetch_add((physical * count as usize) as u64, Ordering::Relaxed);
        self.header()
            .n_cached_bytes
            .fetch_add((physical * count as usize) as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn preallocate_fifo_headers(&mut self, slice: u32, count: u32) -> Result<(), FifoError> {
        let shared = self.slice(slice).ok_or(FifoError::InvalidCapacity)?;
        if count == 0 {
            return Ok(());
        }
        let stride = std::mem::size_of::<crate::svm::fifo::SvmFifoShared>();
        let first = self.allocate_block(
            stride.saturating_mul(count as usize),
            FIFO_SEGMENT_ALIGN as usize,
        )?;
        let mut head = shared.free_fifos.load(Ordering::Acquire);
        for index in (0..count).rev() {
            let offset = first + index as u64 * stride as u64;
            let header = unsafe {
                self.segment
                    .base()
                    .add(self.header_offset as usize + offset as usize)
                    .cast::<crate::svm::fifo::SvmFifoShared>()
            };
            unsafe {
                std::ptr::write_bytes(header, 0, 1);
                (*header).next.store(head, Ordering::Relaxed);
            }
            head = offset;
        }
        shared.free_fifos.store(head, Ordering::Release);
        Ok(())
    }

    pub fn preallocate_fifo_pairs(
        &mut self,
        slice: u32,
        fifo_size: u32,
        chunk_size_bytes: u32,
        pairs: &mut u32,
    ) {
        let requested = *pairs;
        if requested == 0 || fifo_size == 0 || chunk_size_bytes == 0 {
            return;
        }
        let pair_bytes = (std::mem::size_of::<crate::svm::fifo::SvmFifoShared>() * 2)
            .saturating_add(
                (CHUNK_HEADER_SIZE + chunk_size(chunk_size_bytes as usize)).saturating_mul(2),
            );
        let count = requested.min((self.free_bytes() / pair_bytes.max(1)) as u32);
        if self
            .preallocate_chunks(slice, chunk_size_bytes, count.saturating_mul(2))
            .is_err()
            || self
                .preallocate_fifo_headers(slice, count.saturating_mul(2))
                .is_err()
        {
            *pairs = requested;
        } else {
            *pairs = requested.saturating_sub(count);
        }
    }

    pub fn num_free_chunks(&self, size: usize) -> u32 {
        let class = chunk_class(size);
        unsafe { self.header().slices() }
            .iter()
            .map(|slice| {
                let mut count = 0;
                let mut offset = slice.free_chunks[class].load(Ordering::Acquire);
                while offset != 0 {
                    count += 1;
                    offset = unsafe { (*self.chunk_ptr(offset)).next.load(Ordering::Relaxed) };
                }
                count
            })
            .sum()
    }

    pub fn num_free_fifos(&self) -> u32 {
        unsafe { self.header().slices() }
            .iter()
            .map(|slice| {
                let mut count = 0;
                let mut offset = slice.free_fifos.load(Ordering::Acquire);
                while offset != 0 {
                    count += 1;
                    offset =
                        unsafe { (*self.fifo_header_ptr(offset)).next.load(Ordering::Relaxed) };
                }
                count
            })
            .sum()
    }

    pub fn free_chunk_bytes(&self) -> usize {
        unsafe { self.header().slices() }
            .iter()
            .map(|slice| slice.n_fl_chunk_bytes.load(Ordering::Relaxed) as usize)
            .sum()
    }

    pub fn free_bytes(&self) -> usize {
        let current = self.header().byte_index.load(Ordering::Relaxed);
        self.header().max_byte_index.saturating_sub(current) as usize
    }

    pub fn available_bytes(&self) -> usize {
        self.free_bytes().saturating_add(self.cached_bytes())
    }

    pub fn cached_bytes(&self) -> usize {
        self.header().n_cached_bytes.load(Ordering::Relaxed) as usize
    }

    pub fn ssvm(&self) -> &Arc<SsvmPrivate> {
        &self.segment
    }

    pub fn header_offset(&self) -> u64 {
        self.header_offset
    }
}

impl SvmFifoSegmentMain {
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub fn add(&mut self, segment: SvmFifoSegment) -> u32 {
        if let Some((index, slot)) = self
            .segments
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| slot.is_none())
        {
            *slot = Some(segment);
            return index as u32;
        }
        self.segments.push(Some(segment));
        (self.segments.len() - 1) as u32
    }

    pub fn segment(&self, index: u32) -> Option<&SvmFifoSegment> {
        self.segments.get(index as usize)?.as_ref()
    }

    pub fn segment_mut(&mut self, index: u32) -> Option<&mut SvmFifoSegment> {
        self.segments.get_mut(index as usize)?.as_mut()
    }
}

impl Default for SvmFifoSegmentMain {
    fn default() -> Self {
        Self::new()
    }
}

pub use crate::svm::fifo::Fifo as SvmFifo;
