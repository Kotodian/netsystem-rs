//! VPP-style FIFO segment ownership and shared freelists.

use std::alloc::Layout;
use std::os::fd::{AsRawFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::pool::Pool;
use crate::svm::fifo::{
    FS_CHUNK_OFFSET_MASK, FS_CHUNK_VEC_LEN, FS_MIN_LOG2_CHUNK_SIZE, Fifo, FifoError,
    FifoSegmentHeader, FifoSegmentSlice, SvmFifoChunk, fs_chunk_class, fs_head_offset,
    fs_head_with_next, fs_offset_is_valid,
};
use crate::svm::msg_queue::{SvmMsgQ, SvmMsgQConfig, SvmMsgQError};
use crate::svm::ssvm::{SsvmConfig, SsvmError, SsvmPrivate};

const FIFO_SEGMENT_ALIGN: u64 = 64;
const FIFO_SEGMENT_MIN_FIFO_SIZE: usize = 1 << FS_MIN_LOG2_CHUNK_SIZE;
const CHUNK_HEADER_SIZE: usize = std::mem::size_of::<SvmFifoChunk>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FifoSegmentFtype {
    None,
    RxFifo,
    TxFifo,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FifoSegmentFlags: u8 {
        const PREALLOCATED = 1 << 0;
        const WILL_DELETE = 1 << 1;
        const MEMORY_LIMIT = 1 << 2;
        const CUSTOM_USE = 1 << 3;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FifoSegmentMemoryStatus {
    NoPressure,
    LowPressure,
    HighPressure,
    NoMemory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmFifoSegmentConfig {
    pub slices: u32,
    pub max_fifo_size: usize,
    pub first_allocation_percent: u8,
    pub low_watermark: u8,
    pub high_watermark: u8,
}

impl Default for SvmFifoSegmentConfig {
    fn default() -> Self {
        Self {
            slices: 1,
            max_fifo_size: 1 << 31,
            first_allocation_percent: 100,
            low_watermark: 0,
            high_watermark: 0,
        }
    }
}

pub struct FifoSlicePrivate {
    slice_index: u32,
    segment_header: usize,
    pub fifos: Pool<Fifo>,
    pub active_fifos: Vec<u32>,
}

impl FifoSlicePrivate {
    #[inline]
    fn new(slice_index: u32, segment_header: *mut FifoSegmentHeader) -> Self {
        Self {
            slice_index,
            segment_header: segment_header as usize,
            fifos: Pool::new(),
            active_fifos: Vec::new(),
        }
    }
}

/// Process-local FIFO state together with the SSVM mapping lifecycle.
///
/// This mirrors VPP's `fifo_segment_t`: `ssvm` owns the mapping, `h` points at
/// the shared FIFO header, and `slices`/`mqs` own process-local objects. The
/// FIFO map-only path never creates or exposes an SSVM heap.
pub struct SvmFifoSegment {
    pub ssvm: Arc<SsvmPrivate>,
    pub h: *mut FifoSegmentHeader,
    slices: Vec<FifoSlicePrivate>,
    pub mqs: Vec<SvmMsgQ>,
    mq_offsets: Vec<u64>,
    pub max_byte_index: u64,
    pub sm_index: u32,
    pub fs_index: u32,
    pub n_slices: u8,
    pub flags: FifoSegmentFlags,
    pub high_watermark: u8,
    pub low_watermark: u8,
    memory_limit: AtomicBool,
}

unsafe impl Send for SvmFifoSegment {}
// SAFETY: all shared-borrow mutation is confined to atomic shared metadata.
// Process-local FIFO pools are accessible only through &mut self or unique
// FifoSlicePrivate values moved out before sharing the segment. MQs provide
// their own synchronization; segment topology changes require &mut self.
unsafe impl Sync for SvmFifoSegment {}

#[derive(Debug, thiserror::Error)]
pub enum FifoSegmentError {
    #[error("FIFO segment header is not published")]
    HeaderMissing,
    #[error("FIFO segment slice count {count} is invalid")]
    InvalidSliceCount { count: u32 },
    #[error("FIFO segment header is outside the mapping")]
    HeaderOutOfBounds,
    #[error("FIFO segment mapping already owns a generic SSVM heap")]
    HeapPresent,
    #[error("FIFO segment is not ready")]
    NotReady,
    #[error("FIFO segment slice {slice} is invalid")]
    InvalidSlice { slice: u32 },
    #[error("FIFO segment FIFO {fifo} is invalid")]
    InvalidFifo { fifo: u32 },
    #[error("FIFO segment offset {offset} is invalid")]
    InvalidOffset { offset: u64 },
    #[error("FIFO segment memory status watermarks are invalid")]
    InvalidWatermark,
    #[error("FIFO segment allocation failed: {source}")]
    Fifo {
        #[source]
        source: FifoError,
    },
    #[error("FIFO segment mapping failed: {source}")]
    Ssvm {
        #[source]
        source: SsvmError,
    },
    #[error("FIFO segment message queue failed: {source}")]
    MessageQueue {
        #[source]
        source: SvmMsgQError,
    },
}

impl From<FifoError> for FifoSegmentError {
    fn from(source: FifoError) -> Self {
        Self::Fifo { source }
    }
}

impl From<SsvmError> for FifoSegmentError {
    fn from(source: SsvmError) -> Self {
        Self::Ssvm { source }
    }
}

impl From<SvmMsgQError> for FifoSegmentError {
    fn from(source: SvmMsgQError) -> Self {
        Self::MessageQueue { source }
    }
}

#[inline(always)]
fn fifo_segment_alloc_overhead(ssvm: &SsvmPrivate) -> Result<u64, FifoSegmentError> {
    let page = u64::try_from(ssvm.page_size()?).map_err(|_| FifoSegmentError::HeaderOutOfBounds)?;
    page.checked_mul(2)
        .ok_or(FifoSegmentError::HeaderOutOfBounds)
}

#[inline(always)]
const fn chunk_size(size: usize) -> usize {
    1usize << (FS_MIN_LOG2_CHUNK_SIZE + fs_chunk_class(size) as u32)
}

fn max_log2(size: usize) -> u32 {
    usize::BITS - size.max(1).leading_zeros() - 1
}

impl SvmFifoSegment {
    pub fn new(
        ssvm: Arc<SsvmPrivate>,
        config: SvmFifoSegmentConfig,
    ) -> Result<Self, FifoSegmentError> {
        validate_config(config)?;
        if !ssvm.is_server() {
            return Self::attach(ssvm, config);
        }
        if ssvm.heap().is_some() {
            return Err(FifoSegmentError::HeapPresent);
        }
        let header_offset = fifo_segment_alloc_overhead(&ssvm)?;
        let header_bytes = FifoSegmentHeader::layout_bytes(config.slices as usize);
        let header_end = header_offset
            .checked_add(header_bytes as u64)
            .ok_or(FifoSegmentError::HeaderOutOfBounds)?;
        if header_end >= ssvm.ssvm_size() as u64 || header_end > FS_CHUNK_OFFSET_MASK {
            return Err(FifoSegmentError::HeaderOutOfBounds);
        }
        let header: *mut FifoSegmentHeader =
            unsafe { ssvm.base().add(header_offset as usize).cast() };
        let max_byte_index = ssvm.ssvm_size() as u64 - header_offset;
        unsafe {
            std::ptr::write(
                header,
                FifoSegmentHeader {
                    n_cached_bytes: std::sync::atomic::AtomicU64::new(0),
                    n_active_fifos: std::sync::atomic::AtomicU32::new(0),
                    n_reserved_bytes: std::sync::atomic::AtomicU32::new(header_bytes as u32),
                    max_log2_fifo_size: max_log2(config.max_fifo_size),
                    n_slices: config.slices as u8,
                    pct_first_alloc: config.first_allocation_percent,
                    n_mqs: 0,
                    _header_pad: [0; 36],
                    byte_index: std::sync::atomic::AtomicU64::new(header_bytes as u64),
                    max_byte_index,
                    start_byte_index: header_bytes as u64,
                    slices_offset: FifoSegmentHeader::slices_offset() as u64,
                    _slice_pad: [0; 32],
                },
            );
            for slice in (*header).slices_mut() {
                std::ptr::write(slice, FifoSegmentSlice::new());
            }
        }
        ssvm.set_fifo_segment_offset(header_offset);
        let mut segment = Self {
            ssvm,
            h: header,
            slices: Vec::new(),
            mqs: Vec::new(),
            mq_offsets: Vec::new(),
            max_byte_index,
            sm_index: u32::MAX,
            fs_index: u32::MAX,
            n_slices: config.slices as u8,
            flags: FifoSegmentFlags::empty(),
            high_watermark: config.high_watermark,
            low_watermark: config.low_watermark,
            memory_limit: AtomicBool::new(false),
        };
        segment.initialize_slices(config.slices as usize);
        segment.ssvm.publish_ready();
        Ok(segment)
    }

    pub fn attach(
        ssvm: Arc<SsvmPrivate>,
        config: SvmFifoSegmentConfig,
    ) -> Result<Self, FifoSegmentError> {
        let header_offset = ssvm
            .fifo_segment_offset()
            .ok_or(FifoSegmentError::HeaderMissing)?;
        if !ssvm.is_ready() {
            return Err(FifoSegmentError::NotReady);
        }
        let header_end = header_offset
            .checked_add(std::mem::size_of::<FifoSegmentHeader>() as u64)
            .ok_or(FifoSegmentError::HeaderOutOfBounds)?;
        if !header_offset.is_multiple_of(std::mem::align_of::<FifoSegmentHeader>() as u64)
            || header_end > ssvm.ssvm_size() as u64
            || header_end > FS_CHUNK_OFFSET_MASK
        {
            return Err(FifoSegmentError::HeaderOutOfBounds);
        }
        let header: *mut FifoSegmentHeader =
            unsafe { ssvm.base().add(header_offset as usize).cast() };
        let shared_slices = unsafe { (*header).n_slices as u32 };
        if shared_slices == 0 || (config.slices != 0 && config.slices != shared_slices) {
            return Err(FifoSegmentError::InvalidSliceCount {
                count: shared_slices,
            });
        }
        let max_byte_index = unsafe { (*header).max_byte_index };
        let slices_offset = unsafe { (*header).slices_offset };
        let slices_bytes = shared_slices as u64 * std::mem::size_of::<FifoSegmentSlice>() as u64;
        let start_byte_index = unsafe { (*header).start_byte_index };
        if slices_offset < std::mem::size_of::<FifoSegmentHeader>() as u64
            || !slices_offset.is_multiple_of(std::mem::align_of::<FifoSegmentSlice>() as u64)
            || slices_offset
                .checked_add(slices_bytes)
                .is_none_or(|end| end > start_byte_index)
            || start_byte_index > max_byte_index
            || header_offset
                .checked_add(max_byte_index)
                .is_none_or(|end| end > ssvm.ssvm_size() as u64)
            || max_byte_index > FS_CHUNK_OFFSET_MASK
        {
            return Err(FifoSegmentError::HeaderOutOfBounds);
        }
        let mut segment = Self {
            ssvm,
            h: header,
            slices: Vec::new(),
            mqs: Vec::new(),
            mq_offsets: Vec::new(),
            max_byte_index,
            sm_index: u32::MAX,
            fs_index: u32::MAX,
            n_slices: shared_slices as u8,
            flags: FifoSegmentFlags::empty(),
            high_watermark: 0,
            low_watermark: 0,
            memory_limit: AtomicBool::new(false),
        };
        segment.initialize_slices(shared_slices as usize);
        Ok(segment)
    }

    fn initialize_slices(&mut self, count: usize) {
        self.slices.reserve(count);
        for index in 0..count {
            self.slices
                .push(FifoSlicePrivate::new(index as u32, self.h));
        }
    }

    /// Moves the unique process-local FIFO pools out before sharing the segment
    /// with workers. Each worker borrows only its own entry from the returned
    /// vector's native Rust slice.
    pub fn take_private_slices(&mut self) -> Vec<FifoSlicePrivate> {
        std::mem::take(&mut self.slices)
    }

    #[inline(always)]
    fn private_slice_index(&self, private: &FifoSlicePrivate) -> Result<u32, FifoSegmentError> {
        if private.segment_header != self.h as usize || private.slice_index >= self.n_slices as u32
        {
            return Err(FifoSegmentError::InvalidSlice {
                slice: private.slice_index,
            });
        }
        Ok(private.slice_index)
    }

    #[inline(always)]
    fn header(&self) -> &FifoSegmentHeader {
        unsafe { &*self.h }
    }

    #[inline(always)]
    fn header_mut(&mut self) -> &mut FifoSegmentHeader {
        unsafe { &mut *self.h }
    }

    #[inline(always)]
    fn shared_slice(&self, slice: u32) -> Result<&FifoSegmentSlice, FifoSegmentError> {
        if slice >= self.n_slices as u32 {
            return Err(FifoSegmentError::InvalidSlice { slice });
        }
        unsafe { self.header().slices() }
            .get(slice as usize)
            .ok_or(FifoSegmentError::InvalidSlice { slice })
    }

    #[inline(always)]
    fn slice(&self, slice: u32) -> Result<&FifoSlicePrivate, FifoSegmentError> {
        self.slices
            .get(slice as usize)
            .ok_or(FifoSegmentError::InvalidSlice { slice })
    }

    #[inline(always)]
    fn slice_mut(&mut self, slice: u32) -> Result<&mut FifoSlicePrivate, FifoSegmentError> {
        self.slices
            .get_mut(slice as usize)
            .ok_or(FifoSegmentError::InvalidSlice { slice })
    }

    #[inline(always)]
    fn chunk_ptr(&self, offset: u64) -> *mut SvmFifoChunk {
        unsafe { self.h.cast::<u8>().add(offset as usize).cast() }
    }

    #[inline(always)]
    fn fifo_header_ptr(&self, offset: u64) -> *mut crate::svm::fifo::SvmFifoShared {
        unsafe { self.h.cast::<u8>().add(offset as usize).cast() }
    }

    fn allocate_block(&self, bytes: usize, align: usize) -> Result<u64, FifoSegmentError> {
        if align == 0 || !align.is_power_of_two() {
            return Err(FifoSegmentError::HeaderOutOfBounds);
        }
        let header = self.header();
        let mut current = header.byte_index.load(Ordering::Relaxed);
        loop {
            let aligned = current
                .checked_add(align as u64 - 1)
                .map(|value| value & !(align as u64 - 1))
                .ok_or(FifoSegmentError::HeaderOutOfBounds)?;
            let end = aligned
                .checked_add(bytes as u64)
                .ok_or(FifoSegmentError::HeaderOutOfBounds)?;
            if end >= self.max_byte_index || end > FS_CHUNK_OFFSET_MASK {
                self.memory_limit.store(true, Ordering::Relaxed);
                return Err(FifoError::SegmentExhausted.into());
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

    #[inline(always)]
    fn pop_chunk(&self, slice: u32, class: usize) -> Result<Option<u64>, FifoSegmentError> {
        let shared = self.shared_slice(slice)?;
        if class >= FS_CHUNK_VEC_LEN {
            return Ok(None);
        }
        let mut head = shared.free_chunks[class].load(Ordering::Acquire);
        while head != 0 {
            let offset = fs_head_offset(head);
            if !fs_offset_is_valid(offset, self.max_byte_index) {
                panic!("FIFO chunk freelist contains invalid offset {offset:#x}");
            }
            let chunk = self.chunk_ptr(offset);
            let next = unsafe { (*chunk).next.load(Ordering::Relaxed) } & FS_CHUNK_OFFSET_MASK;
            let new_head = fs_head_with_next(head, next);
            match shared.free_chunks[class].compare_exchange_weak(
                head,
                new_head,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let bytes = unsafe { (*chunk).length.load(Ordering::Relaxed) } as u64;
                    shared.n_fl_chunk_bytes.fetch_sub(bytes, Ordering::Relaxed);
                    self.header()
                        .n_cached_bytes
                        .fetch_sub(bytes, Ordering::Relaxed);
                    unsafe { (*chunk).next.store(0, Ordering::Relaxed) };
                    return Ok(Some(offset));
                }
                Err(observed) => head = observed,
            }
        }
        Ok(None)
    }

    fn push_chunk_list(
        &self,
        slice: u32,
        class: usize,
        first: u64,
        tail: u64,
        bytes: u64,
        count: u32,
    ) -> Result<(), FifoSegmentError> {
        if !fs_offset_is_valid(first, self.max_byte_index)
            || !fs_offset_is_valid(tail, self.max_byte_index)
            || class >= FS_CHUNK_VEC_LEN
        {
            panic!("FIFO chunk push-list received an invalid offset");
        }
        let shared = self.shared_slice(slice)?;
        let mut head = shared.free_chunks[class].load(Ordering::Acquire);
        loop {
            unsafe {
                (*self.chunk_ptr(tail))
                    .next
                    .store(fs_head_offset(head), Ordering::Relaxed)
            };
            let new_head = fs_head_with_next(head, first);
            match shared.free_chunks[class].compare_exchange_weak(
                head,
                new_head,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => head = observed,
            }
        }
        shared.num_chunks[class].fetch_add(count, Ordering::Relaxed);
        shared.n_fl_chunk_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.header()
            .n_cached_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    fn push_chunk(&self, slice: u32, class: usize, offset: u64) -> Result<(), FifoSegmentError> {
        if !fs_offset_is_valid(offset, self.max_byte_index) {
            return Err(FifoSegmentError::InvalidOffset { offset });
        }
        let bytes = unsafe { (*self.chunk_ptr(offset)).length.load(Ordering::Relaxed) as u64 };
        self.push_chunk_list(slice, class, offset, offset, bytes, 1)
    }

    fn pop_fifo_header(&self, slice: u32) -> Result<Option<u64>, FifoSegmentError> {
        let shared = self.shared_slice(slice)?;
        let mut head = shared.free_fifos.load(Ordering::Acquire);
        while head != 0 {
            if !fs_offset_is_valid(head, self.max_byte_index) {
                panic!("FIFO header freelist contains invalid offset {head:#x}");
            }
            let next = unsafe { (*self.fifo_header_ptr(head)).next.load(Ordering::Relaxed) };
            match shared.free_fifos.compare_exchange_weak(
                head,
                next,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    unsafe {
                        (*self.fifo_header_ptr(head))
                            .next
                            .store(0, Ordering::Relaxed)
                    };
                    return Ok(Some(head));
                }
                Err(observed) => head = observed,
            }
        }
        Ok(None)
    }

    fn push_fifo_header(&self, slice: u32, offset: u64) -> Result<(), FifoSegmentError> {
        if !fs_offset_is_valid(offset, self.max_byte_index) {
            panic!("FIFO header push received an invalid offset");
        }
        let shared = self.shared_slice(slice)?;
        let header = self.fifo_header_ptr(offset);
        let mut old = shared.free_fifos.load(Ordering::Acquire);
        loop {
            unsafe { (*header).next.store(old, Ordering::Relaxed) };
            match shared.free_fifos.compare_exchange_weak(
                old,
                offset,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => old = observed,
            }
        }
    }

    fn allocate_chunk(
        &self,
        slice: u32,
        size: usize,
        start_byte: u32,
    ) -> Result<u64, FifoSegmentError> {
        let class = fs_chunk_class(size);
        if let Some(offset) = self.pop_chunk(slice, class)? {
            let chunk = self.chunk_ptr(offset);
            unsafe {
                (*chunk).start_byte = start_byte;
                (*chunk)
                    .length
                    .store(chunk_size(size) as u32, Ordering::Relaxed);
                (*chunk).next.store(0, Ordering::Relaxed);
            }
            return Ok(offset);
        }
        let physical = chunk_size(size);
        let offset = self.allocate_block(CHUNK_HEADER_SIZE + physical, 8)?;
        unsafe {
            std::ptr::write(
                self.chunk_ptr(offset),
                SvmFifoChunk {
                    start_byte,
                    length: std::sync::atomic::AtomicU32::new(physical as u32),
                    next: std::sync::atomic::AtomicU64::new(0),
                    enq_rb_index: u32::MAX,
                    deq_rb_index: u32::MAX,
                },
            );
        }
        Ok(offset)
    }

    fn allocate_fifo_header(&self, slice: u32) -> Result<u64, FifoSegmentError> {
        if let Some(offset) = self.pop_fifo_header(slice)? {
            return Ok(offset);
        }
        self.allocate_block(
            std::mem::size_of::<crate::svm::fifo::SvmFifoShared>(),
            FIFO_SEGMENT_ALIGN as usize,
        )
    }

    fn allocate_fifo_value(
        &self,
        slice: u32,
        capacity: usize,
        ftype: FifoSegmentFtype,
    ) -> Result<Fifo, FifoSegmentError> {
        self.validate_fifo_capacity(capacity)?;
        if matches!(ftype, FifoSegmentFtype::None) {
            return Err(FifoSegmentError::InvalidFifo { fifo: u32::MAX });
        }
        self.shared_slice(slice)?;
        let header_offset = self.allocate_fifo_header(slice)?;
        let min_alloc = (capacity * self.header().pct_first_alloc as usize / 100)
            .max(FIFO_SEGMENT_MIN_FIFO_SIZE);
        let physical = chunk_size(min_alloc);
        let chunk_count = capacity.div_ceil(physical);
        let mut chunks = Vec::with_capacity(chunk_count);
        for index in 0..chunk_count {
            match self.allocate_chunk(slice, physical, (index * physical) as u32) {
                Ok(offset) => chunks.push(offset),
                Err(error) => {
                    self.release_chunks(slice, &chunks)?;
                    self.push_fifo_header(slice, header_offset)?;
                    return Err(error);
                }
            }
        }
        for index in 0..chunks.len() {
            let next = chunks.get(index + 1).copied().unwrap_or(0);
            unsafe {
                (*self.chunk_ptr(chunks[index]))
                    .next
                    .store(next, Ordering::Release)
            };
        }
        let fifo = unsafe {
            Fifo::init_at_svm_shared(
                Arc::clone(&self.ssvm),
                header_offset,
                capacity,
                chunks[0],
                *chunks.last().expect("FIFO allocation has a chunk"),
            )
        };
        let mut fifo = match fifo {
            Ok(fifo) => fifo,
            Err(error) => {
                self.release_chunks(slice, &chunks)?;
                self.push_fifo_header(slice, header_offset)?;
                return Err(error.into());
            }
        };
        fifo.set_slice_index(slice);
        Ok(fifo)
    }

    fn record_fifo_allocation(&self, slice: u32, capacity: usize) {
        self.header().n_active_fifos.fetch_add(1, Ordering::Relaxed);
        self.shared_slice(slice)
            .expect("allocated FIFO slice exists")
            .virtual_mem
            .fetch_add(capacity as u64, Ordering::Relaxed);
    }

    pub fn allocate_fifo(
        &mut self,
        slice: u32,
        capacity: usize,
        ftype: FifoSegmentFtype,
    ) -> Result<u32, FifoSegmentError> {
        self.slice(slice)?;
        let fifo = self.allocate_fifo_value(slice, capacity, ftype)?;
        let fifo_index = self.slice_mut(slice)?.fifos.insert(fifo);
        if matches!(ftype, FifoSegmentFtype::RxFifo) {
            self.slice_mut(slice)?.active_fifos.push(fifo_index);
        }
        self.record_fifo_allocation(slice, capacity);
        Ok(fifo_index)
    }

    /// Allocates the shared FIFO while the caller exclusively owns the matching
    /// process-local slice and its `Pool<Fifo>`.
    pub fn allocate_fifo_in(
        &self,
        private: &mut FifoSlicePrivate,
        capacity: usize,
        ftype: FifoSegmentFtype,
    ) -> Result<u32, FifoSegmentError> {
        let slice = self.private_slice_index(private)?;
        let fifo = self.allocate_fifo_value(slice, capacity, ftype)?;
        let fifo_index = private.fifos.insert(fifo);
        if matches!(ftype, FifoSegmentFtype::RxFifo) {
            private.active_fifos.push(fifo_index);
        }
        self.record_fifo_allocation(slice, capacity);
        Ok(fifo_index)
    }

    pub fn attach_fifo(
        &mut self,
        slice: u32,
        header_offset: usize,
    ) -> Result<u32, FifoSegmentError> {
        self.slice(slice)?;
        let offset = header_offset as u64;
        if !fs_offset_is_valid(offset, self.max_byte_index) {
            return Err(FifoSegmentError::InvalidOffset { offset });
        }
        let fifo = unsafe { Fifo::attach_at_svm_shared(Arc::clone(&self.ssvm), offset) }?;
        let fifo_index = self.slice_mut(slice)?.fifos.insert(fifo);
        Ok(fifo_index)
    }

    pub fn attach_fifo_in(
        &self,
        private: &mut FifoSlicePrivate,
        header_offset: usize,
    ) -> Result<u32, FifoSegmentError> {
        self.private_slice_index(private)?;
        let offset = header_offset as u64;
        if !fs_offset_is_valid(offset, self.max_byte_index) {
            return Err(FifoSegmentError::InvalidOffset { offset });
        }
        let fifo = unsafe { Fifo::attach_at_svm_shared(Arc::clone(&self.ssvm), offset) }?;
        Ok(private.fifos.insert(fifo))
    }

    pub fn duplicate_fifo(&mut self, slice: u32, fifo: u32) -> Result<u32, FifoSegmentError> {
        self.slice(slice)?;
        // The slice owner controls both the original and its private duplicate.
        let duplicate = unsafe {
            self.fifo(slice, fifo)
                .ok_or(FifoSegmentError::InvalidFifo { fifo })?
                .duplicate()
        };
        let fifo_index = self.slice_mut(slice)?.fifos.insert(duplicate);
        Ok(fifo_index)
    }

    pub fn migrate_fifo(
        &mut self,
        source_slice: u32,
        fifo: u32,
        destination_slice: u32,
    ) -> Result<u32, FifoSegmentError> {
        self.slice(source_slice)?;
        self.slice(destination_slice)?;
        if source_slice == destination_slice {
            return Ok(fifo);
        }
        // Migration replaces the source entry before the destination executes.
        let replacement = unsafe {
            self.fifo(source_slice, fifo)
                .ok_or(FifoSegmentError::InvalidFifo { fifo })?
                .duplicate()
        };
        let capacity = replacement.size();
        let was_active = self.slice(source_slice)?.active_fifos.contains(&fifo);
        let new_index = self.slice_mut(destination_slice)?.fifos.insert(replacement);
        self.slice_mut(destination_slice)?
            .fifos
            .get_mut(new_index)
            .expect("inserted FIFO")
            .set_slice_index(destination_slice);
        if was_active {
            self.slice_mut(destination_slice)?
                .active_fifos
                .push(new_index);
        }
        self.slice_mut(source_slice)?.fifos.remove(fifo);
        self.remove_active(source_slice, fifo);
        self.shared_slice(source_slice)?
            .virtual_mem
            .fetch_sub(capacity as u64, Ordering::Relaxed);
        self.shared_slice(destination_slice)?
            .virtual_mem
            .fetch_add(capacity as u64, Ordering::Relaxed);
        Ok(new_index)
    }

    fn validate_fifo_chunks(&self, fifo: &Fifo) -> Result<(), FifoSegmentError> {
        let mut chunk_offset = unsafe { (*fifo.shr).start_chunk.load(Ordering::Acquire) };
        while chunk_offset != 0 {
            if !fs_offset_is_valid(chunk_offset, self.max_byte_index) {
                return Err(FifoSegmentError::InvalidOffset {
                    offset: chunk_offset,
                });
            }
            chunk_offset = fs_head_offset(unsafe {
                (*self.chunk_ptr(chunk_offset)).next.load(Ordering::Acquire)
            });
        }
        Ok(())
    }

    fn release_fifo_value(&self, slice: u32, fifo: Fifo) {
        let capacity = fifo.size();
        let mut chunk_offset = unsafe { (*fifo.shr).start_chunk.load(Ordering::Acquire) };
        while chunk_offset != 0 {
            let next = unsafe { (*self.chunk_ptr(chunk_offset)).next.load(Ordering::Acquire) };
            let class = fs_chunk_class(unsafe {
                (*self.chunk_ptr(chunk_offset))
                    .length
                    .load(Ordering::Relaxed) as usize
            });
            self.push_chunk(slice, class, chunk_offset)
                .expect("validated FIFO chunk returns to its slice");
            chunk_offset = fs_head_offset(next);
        }
        unsafe {
            (*fifo.shr).start_chunk.store(0, Ordering::Relaxed);
            (*fifo.shr).end_chunk.store(0, Ordering::Relaxed);
            (*fifo.shr).head_chunk.store(0, Ordering::Relaxed);
            (*fifo.shr).tail_chunk.store(0, Ordering::Relaxed);
        }
        self.push_fifo_header(slice, fifo.hdr_offset())
            .expect("validated FIFO header returns to its slice");
        self.header().n_active_fifos.fetch_sub(1, Ordering::Relaxed);
        self.shared_slice(slice)
            .expect("validated FIFO slice exists")
            .virtual_mem
            .fetch_sub(capacity as u64, Ordering::Relaxed);
    }

    pub fn free_server_fifo(&mut self, slice: u32, fifo: u32) -> Result<(), FifoSegmentError> {
        self.slice(slice)?;
        let fifo_value = self
            .fifo(slice, fifo)
            .ok_or(FifoSegmentError::InvalidFifo { fifo })?;
        self.validate_fifo_chunks(fifo_value)?;
        let fifo_value = self
            .slice_mut(slice)?
            .fifos
            .remove(fifo)
            .ok_or(FifoSegmentError::InvalidFifo { fifo })?;
        self.remove_active(slice, fifo);
        self.release_fifo_value(slice, fifo_value);
        Ok(())
    }

    pub fn free_server_fifo_in(
        &self,
        private: &mut FifoSlicePrivate,
        fifo: u32,
    ) -> Result<(), FifoSegmentError> {
        let slice = self.private_slice_index(private)?;
        let fifo_value = private
            .fifos
            .get(fifo)
            .ok_or(FifoSegmentError::InvalidFifo { fifo })?;
        self.validate_fifo_chunks(fifo_value)?;
        let fifo_value = private
            .fifos
            .remove(fifo)
            .expect("validated FIFO remains in its private pool");
        if let Some(position) = private.active_fifos.iter().position(|&index| index == fifo) {
            private.active_fifos.swap_remove(position);
        }
        self.release_fifo_value(slice, fifo_value);
        Ok(())
    }

    pub fn free_client_fifo(&mut self, slice: u32, fifo: u32) -> Result<(), FifoSegmentError> {
        self.slice(slice)?;
        self.slice_mut(slice)?
            .fifos
            .remove(fifo)
            .ok_or(FifoSegmentError::InvalidFifo { fifo })?;
        Ok(())
    }

    pub fn free_client_fifo_in(
        &self,
        private: &mut FifoSlicePrivate,
        fifo: u32,
    ) -> Result<(), FifoSegmentError> {
        self.private_slice_index(private)?;
        private
            .fifos
            .remove(fifo)
            .ok_or(FifoSegmentError::InvalidFifo { fifo })?;
        Ok(())
    }

    fn release_chunks(&self, slice: u32, chunks: &[u64]) -> Result<(), FifoSegmentError> {
        for &offset in chunks {
            if !fs_offset_is_valid(offset, self.max_byte_index) {
                return Err(FifoSegmentError::InvalidOffset { offset });
            }
            let class = fs_chunk_class(unsafe {
                (*self.chunk_ptr(offset)).length.load(Ordering::Relaxed) as usize
            });
            self.push_chunk(slice, class, offset)?;
        }
        Ok(())
    }

    fn remove_active(&mut self, slice: u32, fifo: u32) {
        if let Some(private) = self.slices.get_mut(slice as usize) {
            if let Some(position) = private.active_fifos.iter().position(|&index| index == fifo) {
                private.active_fifos.swap_remove(position);
            }
        }
    }

    pub fn fifo(&mut self, slice: u32, fifo: u32) -> Option<&Fifo> {
        self.slices.get(slice as usize)?.fifos.get(fifo)
    }

    pub fn fifo_mut(&mut self, slice: u32, fifo: u32) -> Option<&mut Fifo> {
        self.slices.get_mut(slice as usize)?.fifos.get_mut(fifo)
    }

    #[inline(always)]
    pub fn fifo_offset(&mut self, slice: u32, fifo: u32) -> Result<usize, FifoSegmentError> {
        let fifo = self
            .fifo(slice, fifo)
            .ok_or(FifoSegmentError::InvalidFifo { fifo })?;
        if !fs_offset_is_valid(fifo.hdr_offset(), self.max_byte_index) {
            return Err(FifoSegmentError::InvalidOffset {
                offset: fifo.hdr_offset(),
            });
        }
        usize::try_from(fifo.hdr_offset()).map_err(|_| FifoSegmentError::InvalidOffset {
            offset: fifo.hdr_offset(),
        })
    }

    pub fn allocate_chunk_at(&mut self, slice: u32, size: usize) -> Result<u64, FifoSegmentError> {
        self.validate_chunk_size(size)?;
        self.allocate_chunk(slice, size, 0)
    }

    pub fn collect_chunk(&mut self, slice: u32, first: u64) -> Result<(), FifoSegmentError> {
        let mut offset = first;
        while offset != 0 {
            if !fs_offset_is_valid(offset, self.max_byte_index) {
                return Err(FifoSegmentError::InvalidOffset { offset });
            }
            let next = unsafe { (*self.chunk_ptr(offset)).next.load(Ordering::Acquire) }
                & FS_CHUNK_OFFSET_MASK;
            let class = fs_chunk_class(unsafe {
                (*self.chunk_ptr(offset)).length.load(Ordering::Relaxed) as usize
            });
            self.push_chunk(slice, class, offset)?;
            offset = next;
        }
        Ok(())
    }

    #[inline(always)]
    pub fn chunk_offset(&self, chunk: u64) -> Result<usize, FifoSegmentError> {
        if !fs_offset_is_valid(chunk, self.max_byte_index) {
            return Err(FifoSegmentError::InvalidOffset { offset: chunk });
        }
        usize::try_from(chunk).map_err(|_| FifoSegmentError::InvalidOffset { offset: chunk })
    }

    pub fn allocate_message_queue(
        &mut self,
        index: u32,
        config: &SvmMsgQConfig<'_>,
    ) -> Result<&mut SvmMsgQ, FifoSegmentError> {
        let index = index as usize;
        if index != self.mqs.len() {
            return Err(FifoSegmentError::InvalidFifo { fifo: index as u32 });
        }
        let size = SvmMsgQ::size_to_alloc(config)?;
        let offset = self.allocate_block(size, 8)?;
        let base = unsafe { NonNull::new_unchecked(self.h.cast::<u8>().add(offset as usize)) };
        let queue = unsafe { SvmMsgQ::init(base, config) }?;
        self.mqs.push(queue);
        self.mq_offsets.push(offset);
        self.header_mut().n_mqs = self.mqs.len() as u8;
        self.header_mut()
            .n_reserved_bytes
            .fetch_add(size as u32, Ordering::Relaxed);
        self.mqs
            .get_mut(index)
            .ok_or(FifoSegmentError::InvalidFifo { fifo: index as u32 })
    }

    pub fn attach_message_queue(
        &mut self,
        index: u32,
        offset: usize,
        eventfd: Option<OwnedFd>,
    ) -> Result<&mut SvmMsgQ, FifoSegmentError> {
        let index = index as usize;
        if index != self.mqs.len() {
            return Err(FifoSegmentError::InvalidFifo { fifo: index as u32 });
        }
        let offset = offset as u64;
        if !fs_offset_is_valid(offset, self.max_byte_index) {
            return Err(FifoSegmentError::InvalidOffset { offset });
        }
        let base = unsafe { NonNull::new_unchecked(self.h.cast::<u8>().add(offset as usize)) };
        let mut queue = unsafe { SvmMsgQ::attach(base) }?;
        if let Some(eventfd) = eventfd {
            queue.install_eventfd(eventfd);
        }
        self.mqs.push(queue);
        self.mq_offsets.push(offset);
        self.mqs
            .get_mut(index)
            .ok_or(FifoSegmentError::InvalidFifo { fifo: index as u32 })
    }

    pub fn discover_message_queues(
        &mut self,
        eventfds: Vec<OwnedFd>,
    ) -> Result<(), FifoSegmentError> {
        let count = self.header().n_mqs as usize;
        if count == 0 {
            return Ok(());
        }
        if !eventfds.is_empty() && eventfds.len() != count {
            return Err(FifoSegmentError::InvalidSliceCount {
                count: eventfds.len() as u32,
            });
        }
        let reserved = self.header().n_reserved_bytes.load(Ordering::Acquire) as u64;
        let start = self.header().start_byte_index;
        let total = reserved.saturating_sub(start);
        if total == 0 || !total.is_multiple_of(count as u64) {
            return Err(FifoSegmentError::HeaderOutOfBounds);
        }
        let size = total / count as u64;
        let mut eventfds = eventfds.into_iter();
        for index in 0..count {
            let offset = start
                .checked_add(index as u64 * size)
                .ok_or(FifoSegmentError::HeaderOutOfBounds)?;
            self.attach_message_queue(index as u32, offset as usize, eventfds.next())?;
        }
        Ok(())
    }

    pub fn message_queue(&self, index: u32) -> Option<&SvmMsgQ> {
        self.mqs.get(index as usize)
    }

    pub fn message_queue_mut(&mut self, index: u32) -> Option<&mut SvmMsgQ> {
        self.mqs.get_mut(index as usize)
    }

    #[inline(always)]
    pub fn message_queue_offset(&self, index: u32) -> Result<usize, FifoSegmentError> {
        let offset = *self
            .mq_offsets
            .get(index as usize)
            .ok_or(FifoSegmentError::InvalidFifo { fifo: index })?;
        usize::try_from(offset).map_err(|_| FifoSegmentError::InvalidOffset { offset })
    }

    pub fn preallocate_fifo_headers(
        &mut self,
        slice: u32,
        count: u32,
    ) -> Result<(), FifoSegmentError> {
        self.slice(slice)?;
        if count == 0 {
            return Ok(());
        }
        let stride = std::mem::size_of::<crate::svm::fifo::SvmFifoShared>();
        let first_offset = self.allocate_block(
            stride.saturating_mul(count as usize),
            FIFO_SEGMENT_ALIGN as usize,
        )?;
        let mut first = 0;
        let mut tail = 0;
        for index in (0..count).rev() {
            let offset = first_offset + index as u64 * stride as u64;
            if tail == 0 {
                tail = offset;
            }
            let header = self.fifo_header_ptr(offset);
            unsafe {
                std::ptr::write_bytes(header, 0, 1);
                (*header).next.store(first, Ordering::Relaxed);
            }
            first = offset;
        }
        let shared = self.shared_slice(slice)?;
        let mut old = shared.free_fifos.load(Ordering::Acquire);
        loop {
            unsafe {
                (*self.fifo_header_ptr(tail))
                    .next
                    .store(old, Ordering::Relaxed)
            };
            match shared.free_fifos.compare_exchange_weak(
                old,
                first,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => old = observed,
            }
        }
        Ok(())
    }

    pub fn preallocate_chunks(
        &mut self,
        slice: u32,
        size: usize,
        count: u32,
    ) -> Result<(), FifoSegmentError> {
        self.validate_chunk_size(size)?;
        self.slice(slice)?;
        if count == 0 {
            return Ok(());
        }
        let physical = chunk_size(size);
        let stride = CHUNK_HEADER_SIZE + physical;
        let first_offset = self.allocate_block(stride.saturating_mul(count as usize), 8)?;
        let class = fs_chunk_class(size);
        let mut first = 0;
        let mut tail = 0;
        for index in (0..count).rev() {
            let offset = first_offset + index as u64 * stride as u64;
            if tail == 0 {
                tail = offset;
            }
            unsafe {
                std::ptr::write(
                    self.chunk_ptr(offset),
                    SvmFifoChunk {
                        start_byte: 0,
                        length: std::sync::atomic::AtomicU32::new(physical as u32),
                        next: std::sync::atomic::AtomicU64::new(first),
                        enq_rb_index: u32::MAX,
                        deq_rb_index: u32::MAX,
                    },
                );
            }
            first = offset;
        }
        self.push_chunk_list(
            slice,
            class,
            first,
            tail,
            (physical * count as usize) as u64,
            count,
        )
    }

    pub fn preallocate_fifo_pairs(
        &mut self,
        slice: u32,
        rx_capacity: usize,
        tx_capacity: usize,
        pairs: u32,
    ) -> Result<u32, FifoSegmentError> {
        self.validate_fifo_capacity(rx_capacity)?;
        self.validate_fifo_capacity(tx_capacity)?;
        let pair_bytes = 2usize
            .saturating_mul(
                std::mem::size_of::<crate::svm::fifo::SvmFifoShared>() + CHUNK_HEADER_SIZE,
            )
            .saturating_add(chunk_size(rx_capacity))
            .saturating_add(chunk_size(tx_capacity));
        let count = pairs.min((self.new_free_bytes() / pair_bytes.max(1)) as u32);
        if count == 0 {
            return Ok(pairs);
        }
        self.preallocate_fifo_headers(slice, count.saturating_mul(2))?;
        self.preallocate_chunks(slice, rx_capacity, count)?;
        self.preallocate_chunks(slice, tx_capacity, count)?;
        self.flags.insert(FifoSegmentFlags::PREALLOCATED);
        Ok(pairs - count)
    }

    pub fn allocate_reserved(&mut self, layout: Layout) -> Result<NonNull<u8>, FifoSegmentError> {
        let offset = self.allocate_block(layout.size(), layout.align())?;
        self.header_mut()
            .n_reserved_bytes
            .fetch_add(layout.size() as u32, Ordering::Relaxed);
        Ok(unsafe { NonNull::new_unchecked(self.h.cast::<u8>().add(offset as usize)) })
    }

    #[inline(always)]
    pub fn allocated_bytes(&self) -> usize {
        self.header()
            .byte_index
            .load(Ordering::Relaxed)
            .saturating_sub(self.header().start_byte_index) as usize
    }

    #[inline(always)]
    pub fn new_free_bytes(&self) -> usize {
        self.max_byte_index
            .saturating_sub(self.header().byte_index.load(Ordering::Relaxed)) as usize
    }

    #[inline(always)]
    pub fn cached_bytes(&self) -> usize {
        self.header().n_cached_bytes.load(Ordering::Relaxed) as usize
    }

    #[inline(always)]
    pub fn available_bytes(&self) -> usize {
        self.new_free_bytes().saturating_add(self.cached_bytes())
    }

    #[inline(always)]
    pub fn freelist_bytes(&self) -> usize {
        self.free_chunk_bytes()
    }

    pub fn free_chunk_bytes(&self) -> usize {
        unsafe { self.header().slices() }
            .iter()
            .map(|slice| slice.n_fl_chunk_bytes.load(Ordering::Relaxed) as usize)
            .sum()
    }

    pub fn active_fifo_count(&self) -> u32 {
        self.header().n_active_fifos.load(Ordering::Relaxed)
    }

    pub fn free_fifo_count(&mut self) -> u32 {
        let mut count = 0;
        for slice in unsafe { self.header().slices() } {
            let mut offset = slice.free_fifos.load(Ordering::Acquire);
            while offset != 0 {
                if !fs_offset_is_valid(offset, self.max_byte_index) {
                    panic!("FIFO header freelist contains invalid offset {offset:#x}");
                }
                count += 1;
                offset = unsafe { (*self.fifo_header_ptr(offset)).next.load(Ordering::Relaxed) };
            }
        }
        count
    }

    pub fn free_chunk_count(&self, size: usize) -> u32 {
        let class = fs_chunk_class(size);
        let mut count = 0;
        for slice in unsafe { self.header().slices() } {
            let mut head = slice.free_chunks[class].load(Ordering::Acquire);
            while head != 0 {
                let offset = fs_head_offset(head);
                if !fs_offset_is_valid(offset, self.max_byte_index) {
                    panic!("FIFO chunk freelist contains invalid offset {offset:#x}");
                }
                count += 1;
                head = fs_head_offset(unsafe {
                    (*self.chunk_ptr(offset)).next.load(Ordering::Relaxed)
                });
            }
        }
        count
    }

    #[inline]
    pub fn flags(&self) -> FifoSegmentFlags {
        self.flags
    }

    pub fn usage_percent(&self) -> u8 {
        let size = self
            .max_byte_index
            .saturating_sub(self.header().n_reserved_bytes.load(Ordering::Relaxed) as u64);
        if size == 0 {
            return 0;
        }
        let in_use = size
            .saturating_sub(self.new_free_bytes() as u64)
            .saturating_sub(self.cached_bytes() as u64);
        (in_use
            .saturating_mul(100)
            .checked_div(size)
            .unwrap_or(0)
            .min(100)) as u8
    }

    pub fn memory_status(&mut self) -> FifoSegmentMemoryStatus {
        if self.high_watermark == 0 || self.low_watermark == 0 {
            return FifoSegmentMemoryStatus::NoPressure;
        }
        let usage = self.usage_percent();
        if self.memory_limit.load(Ordering::Relaxed) {
            if usage >= self.high_watermark {
                return FifoSegmentMemoryStatus::NoMemory;
            }
            self.memory_limit.store(false, Ordering::Relaxed);
        }
        if usage >= self.high_watermark {
            FifoSegmentMemoryStatus::HighPressure
        } else if usage >= self.low_watermark {
            FifoSegmentMemoryStatus::LowPressure
        } else {
            FifoSegmentMemoryStatus::NoPressure
        }
    }

    pub fn active_fifos(&mut self, slice: u32) -> Result<&[u32], FifoSegmentError> {
        Ok(&self.slice(slice)?.active_fifos)
    }

    pub fn base(&self) -> NonNull<u8> {
        NonNull::new(self.ssvm.base()).expect("mapped FIFO segment base is non-null")
    }

    pub fn size(&self) -> usize {
        self.ssvm.ssvm_size()
    }

    pub fn cleanup(&mut self) -> Result<(), FifoSegmentError> {
        for private in &mut self.slices {
            while let Some(index) = private.fifos.first_index() {
                private.fifos.remove(index);
            }
            private.active_fifos.clear();
        }
        self.mqs.clear();
        self.mq_offsets.clear();
        Ok(())
    }

    fn validate_fifo_capacity(&self, capacity: usize) -> Result<(), FifoSegmentError> {
        let max = 1usize << self.header().max_log2_fifo_size.min(usize::BITS - 1);
        if capacity < FIFO_SEGMENT_MIN_FIFO_SIZE || capacity > max {
            return Err(FifoError::CapacityOutOfRange { capacity }.into());
        }
        Ok(())
    }

    fn validate_chunk_size(&self, size: usize) -> Result<(), FifoSegmentError> {
        let max = 1usize << self.header().max_log2_fifo_size.min(usize::BITS - 1);
        if size < FIFO_SEGMENT_MIN_FIFO_SIZE || size > max {
            return Err(FifoError::CapacityOutOfRange { capacity: size }.into());
        }
        Ok(())
    }
}

pub struct SvmFifoSegmentMain {
    pub segments: Pool<SvmFifoSegment>,
    pub next_baseva: u64,
    pub timeout_in_seconds: u32,
}

impl SvmFifoSegmentMain {
    pub fn new() -> Self {
        Self {
            segments: Pool::new(),
            next_baseva: 0,
            timeout_in_seconds: 0,
        }
    }

    pub fn init(&mut self, baseva: u64, timeout_in_seconds: u32) {
        self.next_baseva = baseva;
        self.timeout_in_seconds = timeout_in_seconds;
    }

    pub fn create(
        &mut self,
        config: SsvmConfig,
        segment_config: SvmFifoSegmentConfig,
    ) -> Result<u32, FifoSegmentError> {
        let ssvm = Arc::new(SsvmPrivate::server_init_fifo_segment(&config)?);
        let segment = SvmFifoSegment::new(ssvm, segment_config)?;
        let index = self.segments.insert(segment);
        let segment = self.segments.get_mut(index).expect("inserted FIFO segment");
        segment.fs_index = index;
        segment.sm_index = 0;
        Ok(index)
    }

    pub fn attach(
        &mut self,
        config: SsvmConfig,
        backing: Option<OwnedFd>,
    ) -> Result<u32, FifoSegmentError> {
        let descriptor = backing.as_ref().map(AsRawFd::as_raw_fd);
        let ssvm = Arc::new(SsvmPrivate::client_init(&config, descriptor)?);
        let segment = SvmFifoSegment::attach(ssvm, SvmFifoSegmentConfig::default())?;
        let index = self.segments.insert(segment);
        let segment = self.segments.get_mut(index).expect("inserted FIFO segment");
        segment.fs_index = index;
        segment.sm_index = 0;
        Ok(index)
    }

    pub fn segment(&self, index: u32) -> Option<&SvmFifoSegment> {
        self.segments.get(index)
    }

    pub fn segment_mut(&mut self, index: u32) -> Option<&mut SvmFifoSegment> {
        self.segments.get_mut(index)
    }

    pub fn segment_index(&self, name: &str) -> Option<u32> {
        self.segments
            .iter()
            .find_map(|(index, segment)| (segment.ssvm.name() == name).then_some(index))
    }

    pub fn delete(&mut self, index: u32) -> Result<(), FifoSegmentError> {
        self.segments
            .get_mut(index)
            .ok_or(FifoSegmentError::InvalidFifo { fifo: index })?
            .cleanup()?;
        self.segments
            .remove(index)
            .ok_or(FifoSegmentError::InvalidFifo { fifo: index })?;
        Ok(())
    }
}

impl Default for SvmFifoSegmentMain {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_config(config: SvmFifoSegmentConfig) -> Result<(), FifoSegmentError> {
    if config.slices == 0 || config.slices > u8::MAX as u32 {
        return Err(FifoSegmentError::InvalidSliceCount {
            count: config.slices,
        });
    }
    if config.first_allocation_percent == 0 || config.first_allocation_percent > 100 {
        return Err(FifoSegmentError::InvalidWatermark);
    }
    if config.max_fifo_size < FIFO_SEGMENT_MIN_FIFO_SIZE || config.max_fifo_size > (1usize << 31) {
        return Err(FifoSegmentError::InvalidWatermark);
    }
    if config.high_watermark != 0
        && (config.low_watermark == 0 || config.low_watermark > config.high_watermark)
    {
        return Err(FifoSegmentError::InvalidWatermark);
    }
    Ok(())
}

pub use crate::svm::fifo::Fifo as SvmFifo;
