use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::march_fn;
use crate::pool::Pool;
use crate::rbtree::RbTree;
use crate::svm::ssvm::{SSVM_PAYLOAD_OFFSET, SsvmPrivate};

pub type FsSptr = u64;

pub const OOO_SEGMENT_INVALID_INDEX: u32 = u32::MAX;
pub const FS_MIN_LOG2_CHUNK_SIZE: u32 = 12;
pub const FS_MAX_LOG2_CHUNK_SIZE: u32 = 22;
pub const FS_CHUNK_VEC_LEN: usize = (FS_MAX_LOG2_CHUNK_SIZE - FS_MIN_LOG2_CHUNK_SIZE + 1) as usize;
pub const FS_CHUNK_OFFSET_MASK: u64 = 0x0000_ffff_ffff_ffff;
pub const FS_CHUNK_TAG_MASK: u64 = 0xffff_0000_0000_0000;
pub const FS_CHUNK_TAG_INCREMENT: u64 = 1_u64 << 48;

/// FIFO chunk stored in a FIFO segment.
///
/// The field order is the order of VPP's `svm_fifo_chunk_t`: offsets are
/// relative to the segment header, and the two rb-tree indexes are private
/// process-local bookkeeping. `length` and `next` use atomics in Hammer so a
/// consumer can observe a published chunk without inventing a second shared
/// header; their representation remains the same size as the VPP fields.
#[repr(C)]
pub struct SvmFifoChunk {
    pub start_byte: u32,
    pub length: AtomicU32,
    pub next: AtomicU64,
    pub enq_rb_index: u32,
    pub deq_rb_index: u32,
}

pub type Chunk = SvmFifoChunk;
const CHUNK_HEADER_SIZE: usize = std::mem::size_of::<SvmFifoChunk>();

#[repr(C)]
pub struct SvmFifoSignals {
    pub has_event: AtomicU32,
    pub want_deq_ntf: AtomicU32,
    pub has_deq_ntf: AtomicU32,
    pub n_subscribers: u8,
    pub subscribers: [u8; 7],
    pub deq_thresh: AtomicU32,
}

impl SvmFifoSignals {
    const fn new() -> Self {
        Self {
            has_event: AtomicU32::new(0),
            want_deq_ntf: AtomicU32::new(0),
            has_deq_ntf: AtomicU32::new(0),
            n_subscribers: 0,
            subscribers: [0; 7],
            deq_thresh: AtomicU32::new(0),
        }
    }
}

#[repr(C, align(64))]
pub struct SvmFifoShared {
    pub start_chunk: AtomicU64,
    pub end_chunk: AtomicU64,
    pub min_alloc: u32,
    pub size: u32,
    pub slice_index: u8,
    pub _pad0: [u8; 7],
    pub next: AtomicU64,
    pub signals: SvmFifoSignals,
    pub _shared_pad: [u8; 128 - (8 + 8 + 4 + 4 + 1 + 7 + 8 + 28)],

    pub head_chunk: AtomicU64,
    pub head: AtomicU32,
    pub _consumer_pad: [u8; 64 - (8 + 4)],

    pub tail_chunk: AtomicU64,
    pub tail: AtomicU32,
    pub _producer_pad: [u8; 64 - (8 + 4)],
}

pub type FifoHeader = SvmFifoShared;

#[repr(C)]
#[derive(Clone, Copy)]
pub union SvmFifoSession {
    pub session_index: SvmFifoSessionIndex,
    pub session_handle: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SvmFifoSessionIndex {
    pub session_index: u32,
    pub thread_index: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union SvmFifoAttachment {
    pub client_fifo: *mut Fifo,
    pub segment: SvmFifoSegmentIndex,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SvmFifoSegmentIndex {
    pub context_index: u32,
    pub client_segment_index: u32,
}

#[repr(C, align(64))]
pub struct FifoSegmentSlice {
    pub free_chunks: [AtomicU64; FS_CHUNK_VEC_LEN],
    pub free_fifos: AtomicU64,
    pub n_fl_chunk_bytes: AtomicU64,
    pub virtual_mem: AtomicU64,
    pub num_chunks: [AtomicU32; FS_CHUNK_VEC_LEN],
}

impl FifoSegmentSlice {
    pub fn new() -> Self {
        Self {
            free_chunks: std::array::from_fn(|_| AtomicU64::new(0)),
            free_fifos: AtomicU64::new(0),
            n_fl_chunk_bytes: AtomicU64::new(0),
            virtual_mem: AtomicU64::new(0),
            num_chunks: std::array::from_fn(|_| AtomicU32::new(0)),
        }
    }
}

#[repr(C, align(64))]
pub struct FifoSegmentHeader {
    pub n_cached_bytes: AtomicU64,
    pub n_active_fifos: AtomicU32,
    pub n_reserved_bytes: AtomicU32,
    pub max_log2_fifo_size: u32,
    pub n_slices: u8,
    pub pct_first_alloc: u8,
    pub n_mqs: u8,
    pub _header_pad: [u8; 36],
    pub byte_index: AtomicU64,
    pub max_byte_index: u64,
    pub start_byte_index: u64,
    pub slices_offset: u64,
    pub _slice_pad: [u8; 32],
}

impl FifoSegmentHeader {
    pub const MAGIC: u64 = 0x4841_4d4d_4552_4653;

    pub const fn slices_offset() -> usize {
        std::mem::size_of::<Self>()
    }

    pub const fn layout_bytes(n_slices: usize) -> usize {
        Self::slices_offset() + n_slices * std::mem::size_of::<FifoSegmentSlice>()
    }

    /// The segment owner must validate the explicit offset, length, and mapping
    /// bounds before accessing this shared-memory slice.
    pub unsafe fn slices(&self) -> &[FifoSegmentSlice] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self)
                    .cast::<u8>()
                    .add(self.slices_offset as usize)
                    .cast::<FifoSegmentSlice>(),
                self.n_slices as usize,
            )
        }
    }

    pub unsafe fn slices_mut(&mut self) -> &mut [FifoSegmentSlice] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self as *mut Self)
                    .cast::<u8>()
                    .add(self.slices_offset as usize)
                    .cast::<FifoSegmentSlice>(),
                self.n_slices as usize,
            )
        }
    }
}

#[inline(always)]
pub fn f_chunk_end(c: &SvmFifoChunk) -> u32 {
    c.start_byte.wrapping_add(c.length.load(Ordering::Relaxed))
}

#[inline(always)]
pub fn f_pos_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

#[inline(always)]
pub fn f_pos_leq(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) <= 0
}

#[inline(always)]
pub fn f_pos_gt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) > 0
}

#[inline(always)]
pub fn f_pos_geq(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) >= 0
}

#[inline(always)]
pub fn f_chunk_includes_pos(c: &SvmFifoChunk, pos: u32) -> bool {
    f_pos_geq(pos, c.start_byte) && f_pos_lt(pos, f_chunk_end(c))
}

#[inline(always)]
pub(crate) fn fs_head_offset(head: u64) -> u64 {
    head & FS_CHUNK_OFFSET_MASK
}

#[inline(always)]
pub(crate) fn fs_head_with_next(old_head: u64, next_offset: u64) -> u64 {
    assert!(
        next_offset <= FS_CHUNK_OFFSET_MASK,
        "FIFO chunk offset exceeds 48 bits"
    );
    next_offset | (old_head.wrapping_add(FS_CHUNK_TAG_INCREMENT) & FS_CHUNK_TAG_MASK)
}

#[inline(always)]
pub(crate) fn fs_offset_is_valid(offset: u64, max_byte_index: u64) -> bool {
    offset != 0 && offset <= FS_CHUNK_OFFSET_MASK && offset < max_byte_index
}

#[inline(always)]
pub(crate) const fn fs_chunk_class(size: usize) -> usize {
    let size = if size < 1usize << FS_MIN_LOG2_CHUNK_SIZE {
        1usize << FS_MIN_LOG2_CHUNK_SIZE
    } else {
        size
    };
    let log2 = usize::BITS - size.saturating_sub(1).leading_zeros();
    let class = log2.saturating_sub(FS_MIN_LOG2_CHUNK_SIZE) as usize;
    if class < FS_CHUNK_VEC_LEN {
        class
    } else {
        FS_CHUNK_VEC_LEN - 1
    }
}

#[inline(always)]
pub unsafe fn fs_ptr(fsh: *mut FifoSegmentHeader, sp: FsSptr) -> *mut u8 {
    assert!(
        sp <= FS_CHUNK_OFFSET_MASK,
        "FIFO segment offset exceeds 48 bits"
    );
    if sp == 0 {
        std::ptr::null_mut()
    } else {
        unsafe { (fsh.cast::<u8>()).add(sp as usize) }
    }
}

#[inline(always)]
pub unsafe fn fs_sptr(fsh: *mut FifoSegmentHeader, ptr: *mut u8) -> FsSptr {
    if ptr.is_null() {
        0
    } else {
        let offset = (ptr as usize).wrapping_sub(fsh as usize) as FsSptr;
        assert!(
            offset <= FS_CHUNK_OFFSET_MASK,
            "FIFO segment offset exceeds 48 bits"
        );
        offset
    }
}

#[inline(always)]
pub unsafe fn fs_chunk_ptr(fsh: *mut FifoSegmentHeader, cp: FsSptr) -> *mut SvmFifoChunk {
    unsafe { fs_ptr(fsh, cp).cast::<SvmFifoChunk>() }
}

#[inline(always)]
pub unsafe fn fs_chunk_sptr(fsh: *mut FifoSegmentHeader, chunk: *mut SvmFifoChunk) -> FsSptr {
    unsafe { fs_sptr(fsh, chunk.cast::<u8>()) }
}

#[inline(always)]
pub fn f_start_cptr(f: &Fifo) -> *mut SvmFifoChunk {
    unsafe { fs_chunk_ptr(f.fs_hdr, (*f.hdr).start_chunk.load(Ordering::Relaxed)) }
}

#[inline(always)]
pub fn f_end_cptr(f: &Fifo) -> *mut SvmFifoChunk {
    unsafe { fs_chunk_ptr(f.fs_hdr, (*f.hdr).end_chunk.load(Ordering::Relaxed)) }
}

#[inline(always)]
pub fn f_head_cptr(f: &Fifo) -> *mut SvmFifoChunk {
    unsafe { fs_chunk_ptr(f.fs_hdr, (*f.hdr).head_chunk.load(Ordering::Relaxed)) }
}

#[inline(always)]
pub fn f_tail_cptr(f: &Fifo) -> *mut SvmFifoChunk {
    unsafe { fs_chunk_ptr(f.fs_hdr, (*f.hdr).tail_chunk.load(Ordering::Relaxed)) }
}

#[inline(always)]
pub fn f_cptr(f: &Fifo, cp: FsSptr) -> *mut SvmFifoChunk {
    if cp == 0 {
        std::ptr::null_mut()
    } else {
        unsafe { fs_chunk_ptr(f.fs_hdr, cp) }
    }
}

#[inline(always)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn f_csptr(f: &Fifo, chunk: *mut SvmFifoChunk) -> FsSptr {
    if chunk.is_null() {
        0
    } else {
        unsafe { fs_chunk_sptr(f.fs_hdr, chunk) }
    }
}

#[inline(always)]
pub unsafe fn f_csptr_link(f: &Fifo, cp: FsSptr, chunk: *mut SvmFifoChunk) {
    let target = f_cptr(f, cp);
    assert!(!target.is_null(), "FIFO chunk link offset is invalid");
    unsafe { (*target).next.store(f_csptr(f, chunk), Ordering::Release) };
}

#[inline(always)]
pub fn f_cursize(head: u32, tail: u32) -> u32 {
    tail.wrapping_sub(head)
}

#[inline(always)]
pub fn f_free_count(f: &Fifo, head: u32, tail: u32) -> u32 {
    unsafe { (*f.hdr).size.saturating_sub(f_cursize(head, tail)) }
}

unsafe fn svm_fifo_copy_to_chunk_scalar(
    fifo: &Fifo,
    mut chunk: *mut SvmFifoChunk,
    mut tail_idx: u32,
    mut src: *const u8,
    mut len: u32,
    last: &mut FsSptr,
) {
    assert!(!chunk.is_null() && f_chunk_includes_pos(unsafe { &*chunk }, tail_idx));
    while len != 0 {
        let current = unsafe { &*chunk };
        let offset = tail_idx.wrapping_sub(current.start_byte) as usize;
        let available = current.length.load(Ordering::Relaxed) as usize - offset;
        let copied = available.min(len as usize);
        unsafe {
            std::ptr::copy_nonoverlapping(
                src,
                fifo.base
                    .add(chunk as usize - fifo.base as usize + CHUNK_HEADER_SIZE + offset),
                copied,
            );
        }
        len -= copied as u32;
        src = unsafe { src.add(copied) };
        if len == 0 {
            break;
        }
        let next = current.next.load(Ordering::Acquire);
        assert_ne!(next, 0, "FIFO copy crossed the end of the chunk list");
        chunk = unsafe { fifo.base.add(next as usize).cast::<SvmFifoChunk>() };
        tail_idx = unsafe { (*chunk).start_byte };
    }
    if *last != 0 {
        *last = f_csptr(fifo, chunk);
    }
}

unsafe fn svm_fifo_copy_from_chunk_scalar(
    fifo: &Fifo,
    mut chunk: *mut SvmFifoChunk,
    mut head_idx: u32,
    mut dst: *mut u8,
    mut len: u32,
    last: &mut FsSptr,
) {
    assert!(!chunk.is_null() && f_chunk_includes_pos(unsafe { &*chunk }, head_idx));
    while len != 0 {
        let current = unsafe { &*chunk };
        let offset = head_idx.wrapping_sub(current.start_byte) as usize;
        let available = current.length.load(Ordering::Acquire) as usize - offset;
        let copied = available.min(len as usize);
        unsafe {
            std::ptr::copy_nonoverlapping(
                fifo.base
                    .add(chunk as usize - fifo.base as usize + CHUNK_HEADER_SIZE + offset),
                dst,
                copied,
            );
        }
        len -= copied as u32;
        dst = unsafe { dst.add(copied) };
        if len == 0 {
            break;
        }
        let next = current.next.load(Ordering::Acquire);
        assert_ne!(next, 0, "FIFO copy crossed the end of the chunk list");
        chunk = unsafe { fifo.base.add(next as usize).cast::<SvmFifoChunk>() };
        head_idx = unsafe { (*chunk).start_byte };
    }
    if *last != 0 {
        *last = f_csptr(fifo, chunk);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn svm_fifo_copy_to_chunk_avx2(
    fifo: &Fifo,
    chunk: *mut SvmFifoChunk,
    tail_idx: u32,
    src: *const u8,
    len: u32,
    last: &mut FsSptr,
) {
    use core::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_storeu_si256};
    assert!(!chunk.is_null() && f_chunk_includes_pos(unsafe { &*chunk }, tail_idx));
    let mut current_chunk = chunk;
    let mut position = tail_idx;
    let mut source = src;
    let mut remaining = len as usize;
    while remaining != 0 {
        let current = unsafe { &*current_chunk };
        let offset = position.wrapping_sub(current.start_byte) as usize;
        let available = current.length.load(Ordering::Relaxed) as usize - offset;
        let count = available.min(remaining);
        let destination = unsafe {
            fifo.base
                .add(current_chunk as usize - fifo.base as usize + CHUNK_HEADER_SIZE + offset)
        };
        let mut copied = 0;
        while copied + 32 <= count {
            let value = unsafe { _mm256_loadu_si256(source.add(copied).cast::<__m256i>()) };
            unsafe { _mm256_storeu_si256(destination.add(copied).cast::<__m256i>(), value) };
            copied += 32;
        }
        if copied < count {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    source.add(copied),
                    destination.add(copied),
                    count - copied,
                )
            };
        }
        source = unsafe { source.add(count) };
        position = position.wrapping_add(count as u32);
        remaining -= count;
        if remaining != 0 {
            let next = current.next.load(Ordering::Acquire);
            assert_ne!(next, 0, "FIFO copy crossed the end of the chunk list");
            current_chunk = unsafe { fifo.base.add(next as usize).cast::<SvmFifoChunk>() };
            position = unsafe { (*current_chunk).start_byte };
        }
    }
    if *last != 0 {
        *last = f_csptr(fifo, current_chunk);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn svm_fifo_copy_from_chunk_avx2(
    fifo: &Fifo,
    chunk: *mut SvmFifoChunk,
    head_idx: u32,
    dst: *mut u8,
    len: u32,
    last: &mut FsSptr,
) {
    use core::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_storeu_si256};
    assert!(!chunk.is_null() && f_chunk_includes_pos(unsafe { &*chunk }, head_idx));
    let mut current_chunk = chunk;
    let mut position = head_idx;
    let mut destination = dst;
    let mut remaining = len as usize;
    while remaining != 0 {
        let current = unsafe { &*current_chunk };
        let offset = position.wrapping_sub(current.start_byte) as usize;
        let available = current.length.load(Ordering::Acquire) as usize - offset;
        let count = available.min(remaining);
        let source = unsafe {
            fifo.base
                .add(current_chunk as usize - fifo.base as usize + CHUNK_HEADER_SIZE + offset)
        };
        let mut copied = 0;
        while copied + 32 <= count {
            let value = unsafe { _mm256_loadu_si256(source.add(copied).cast::<__m256i>()) };
            unsafe { _mm256_storeu_si256(destination.add(copied).cast::<__m256i>(), value) };
            copied += 32;
        }
        if copied < count {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    source.add(copied),
                    destination.add(copied),
                    count - copied,
                )
            };
        }
        destination = unsafe { destination.add(count) };
        position = position.wrapping_add(count as u32);
        remaining -= count;
        if remaining != 0 {
            let next = current.next.load(Ordering::Acquire);
            assert_ne!(next, 0, "FIFO copy crossed the end of the chunk list");
            current_chunk = unsafe { fifo.base.add(next as usize).cast::<SvmFifoChunk>() };
            position = unsafe { (*current_chunk).start_byte };
        }
    }
    if *last != 0 {
        *last = f_csptr(fifo, current_chunk);
    }
}

// `CLIB_MARCH_FN`-style selected FIFO chunk copy entry point.
march_fn! {
    pub fn svm_fifo_copy_to_chunk = svm_fifo_copy_to_chunk_scalar;
    variants {
        x86_64(std::is_x86_feature_detected!("avx2"), priority = 20 => svm_fifo_copy_to_chunk_avx2),
    }
    ;
    (fifo: &Fifo, chunk: *mut SvmFifoChunk, tail_idx: u32, src: *const u8, len: u32, last: &mut FsSptr) -> ()
}

// `CLIB_MARCH_FN`-style selected FIFO chunk copy entry point.
march_fn! {
    pub fn svm_fifo_copy_from_chunk = svm_fifo_copy_from_chunk_scalar;
    variants {
        x86_64(std::is_x86_feature_detected!("avx2"), priority = 20 => svm_fifo_copy_from_chunk_avx2),
    }
    ;
    (fifo: &Fifo, chunk: *mut SvmFifoChunk, head_idx: u32, dst: *mut u8, len: u32, last: &mut FsSptr) -> ()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FifoError {
    #[error("FIFO capacity must be nonzero and fit the shared layout")]
    InvalidCapacity,
    #[error("FIFO capacity {capacity} exceeds the shared layout range")]
    CapacityOutOfRange { capacity: usize },
    #[error("segment has insufficient space for FIFO storage")]
    SegmentExhausted,
    #[error("FIFO has {available} writable bytes for {requested} requested bytes")]
    InsufficientCapacity { requested: usize, available: usize },
    #[error("out-of-order FIFO delivery is disabled")]
    OutOfOrderDisabled,
    #[error("out-of-order FIFO length {length} exceeds u32")]
    OutOfOrderLengthOutOfRange { length: usize },
    #[error("out-of-order FIFO offset {offset} plus length {length} overflows u32")]
    OutOfOrderOffsetOverflow { offset: u32, length: u32 },
    #[error("out-of-order FIFO end offset {end_offset} exceeds available capacity {available}")]
    OutOfOrderCapacityExceeded { end_offset: u32, available: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OooResult {
    pub accepted: u32,
    pub delivered: u32,
    pub start: Option<u32>,
    pub len: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OooSegment {
    pub next: u32,
    pub prev: u32,
    pub start: u32,
    pub length: u32,
}

#[repr(C, align(64))]
pub struct Fifo {
    // These fields mirror the process-local VPP `svm_fifo_t` prefix. The
    // SSVM mapping owner below keeps the shared header alive.
    pub shr: *mut SvmFifoShared,
    pub fs_hdr: *mut FifoSegmentHeader,
    pub ooo_enq_lookup: UnsafeCell<RbTree<u32, u32>>,
    pub ooo_deq_lookup: UnsafeCell<RbTree<u32, u32>>,
    pub ooo_deq: *mut SvmFifoChunk,
    pub ooo_enq: *mut SvmFifoChunk,
    pub ooo_segments: UnsafeCell<Pool<OooSegment>>,
    pub ooos_list_head: UnsafeCell<u32>,
    pub ooos_newest: UnsafeCell<u32>,
    pub flags: u8,
    pub refcnt: i8,
    pub client_thread_index: u8,
    pub app_session_index: u32,
    pub session: SvmFifoSession,
    pub segment_manager: u32,
    pub segment_index: u32,
    pub signals: *mut SvmFifoSignals,
    pub next: *mut Fifo,
    pub prev: *mut Fifo,
    pub attachment: SvmFifoAttachment,

    segment: Arc<SsvmPrivate>,
    base: *mut u8,
    hdr: *mut FifoHeader,
    hdr_off: u64,
    ooo_base: UnsafeCell<u32>,
}

unsafe impl Send for Fifo {}

impl Fifo {
    const fn chunk_data_size(_capacity: usize) -> usize {
        4096
    }

    pub const fn layout_bytes(capacity: usize) -> Result<usize, FifoError> {
        if capacity == 0 {
            return Err(FifoError::InvalidCapacity);
        }
        if capacity > u32::MAX as usize {
            return Err(FifoError::CapacityOutOfRange { capacity });
        }
        let chunk_data_size = Self::chunk_data_size(capacity);
        let chunk_count = capacity.div_ceil(chunk_data_size);
        let Some(chunks_bytes) = chunk_count.checked_mul(CHUNK_HEADER_SIZE + chunk_data_size)
        else {
            return Err(FifoError::CapacityOutOfRange { capacity });
        };
        let Some(bytes) = std::mem::size_of::<FifoHeader>().checked_add(chunks_bytes) else {
            return Err(FifoError::CapacityOutOfRange { capacity });
        };
        Ok(bytes)
    }

    /// Initialise a FIFO directly in an `ssvm` mapping owned by a FIFO
    /// segment. The offset is mapping-relative, so an attacher can reconstruct
    /// the same shared header without rebuilding a process-local allocator.
    pub unsafe fn init_at_svm(
        segment: Arc<SsvmPrivate>,
        hdr_offset: u64,
        capacity: usize,
    ) -> Result<Self, FifoError> {
        let layout = Self::layout_bytes(capacity)?;
        let fs_offset = segment.fifo_segment_offset().unwrap_or(SSVM_PAYLOAD_OFFSET);
        let offset =
            usize::try_from(hdr_offset).map_err(|_| FifoError::CapacityOutOfRange { capacity })?;
        let end = (fs_offset as usize)
            .checked_add(offset)
            .and_then(|value| value.checked_add(layout))
            .ok_or(FifoError::CapacityOutOfRange { capacity })?;
        if end > segment.ssvm_size() {
            return Err(FifoError::SegmentExhausted);
        }
        let base = unsafe { segment.base().add(fs_offset as usize) };
        let hdr = unsafe { base.add(offset).cast::<FifoHeader>() };
        let chunk_size = Self::chunk_data_size(capacity);
        let chunk_count = capacity.div_ceil(chunk_size);
        let first_chunk_off = hdr_offset + std::mem::size_of::<FifoHeader>() as u64;
        let chunk_stride = (CHUNK_HEADER_SIZE + chunk_size) as u64;
        unsafe {
            std::ptr::write(
                hdr,
                FifoHeader {
                    start_chunk: AtomicU64::new(first_chunk_off),
                    end_chunk: AtomicU64::new(first_chunk_off),
                    min_alloc: chunk_size as u32,
                    size: capacity as u32,
                    slice_index: 0,
                    _pad0: [0; 7],
                    next: AtomicU64::new(0),
                    signals: SvmFifoSignals::new(),
                    _shared_pad: [0; 128 - (8 + 8 + 4 + 4 + 1 + 7 + 8 + 28)],
                    head_chunk: AtomicU64::new(first_chunk_off),
                    head: AtomicU32::new(0),
                    _consumer_pad: [0; 64 - (8 + 4)],
                    tail_chunk: AtomicU64::new(first_chunk_off),
                    tail: AtomicU32::new(0),
                    _producer_pad: [0; 64 - (8 + 4)],
                },
            );
            for index in 0..chunk_count {
                let chunk_off = first_chunk_off + index as u64 * chunk_stride;
                let next = if index + 1 < chunk_count {
                    chunk_off + chunk_stride
                } else {
                    0
                };
                std::ptr::write(
                    base.add(chunk_off as usize).cast::<Chunk>(),
                    Chunk {
                        start_byte: index as u32 * chunk_size as u32,
                        length: AtomicU32::new(chunk_size as u32),
                        next: AtomicU64::new(next),
                        enq_rb_index: OOO_SEGMENT_INVALID_INDEX,
                        deq_rb_index: OOO_SEGMENT_INVALID_INDEX,
                    },
                );
            }
        }
        let fifo = Self {
            shr: hdr,
            fs_hdr: base.cast::<FifoSegmentHeader>(),
            ooo_enq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq: std::ptr::null_mut(),
            ooo_enq: std::ptr::null_mut(),
            ooo_segments: UnsafeCell::new(Pool::with_capacity(4)),
            ooos_list_head: UnsafeCell::new(OOO_SEGMENT_INVALID_INDEX),
            ooos_newest: UnsafeCell::new(OOO_SEGMENT_INVALID_INDEX),
            flags: 0,
            refcnt: 1,
            client_thread_index: 0,
            app_session_index: u32::MAX,
            session: SvmFifoSession {
                session_handle: u64::MAX,
            },
            segment_manager: u32::MAX,
            segment_index: u32::MAX,
            signals: unsafe { std::ptr::addr_of_mut!((*hdr).signals) },
            next: std::ptr::null_mut(),
            prev: std::ptr::null_mut(),
            attachment: SvmFifoAttachment {
                segment: SvmFifoSegmentIndex {
                    context_index: u32::MAX,
                    client_segment_index: u32::MAX,
                },
            },
            segment,
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo_base: UnsafeCell::new(0),
        };
        fifo.initialize_chunk_lookups();
        Ok(fifo)
    }

    /// Initializes a process-local FIFO around a shared header and a chunk
    /// chain allocated by [`SvmFifoSegment`]. The offsets are relative to the
    /// FIFO segment header, matching VPP's `fs_sptr_t` contract.
    pub unsafe fn init_at_svm_shared(
        segment: Arc<SsvmPrivate>,
        hdr_offset: u64,
        capacity: usize,
        start_chunk: u64,
        end_chunk: u64,
    ) -> Result<Self, FifoError> {
        if capacity == 0 || capacity > u32::MAX as usize {
            return Err(FifoError::InvalidCapacity);
        }
        let fs_offset = segment.fifo_segment_offset().unwrap_or(SSVM_PAYLOAD_OFFSET);
        let base = unsafe { segment.base().add(fs_offset as usize) };
        let segment_header = unsafe { &*base.cast::<FifoSegmentHeader>() };
        if !fs_offset_is_valid(hdr_offset, segment_header.max_byte_index)
            || !fs_offset_is_valid(start_chunk, segment_header.max_byte_index)
            || !fs_offset_is_valid(end_chunk, segment_header.max_byte_index)
        {
            return Err(FifoError::SegmentExhausted);
        }
        if hdr_offset
            .checked_add(std::mem::size_of::<FifoHeader>() as u64)
            .is_none_or(|end| end > segment_header.max_byte_index)
        {
            return Err(FifoError::SegmentExhausted);
        }
        let hdr = unsafe { base.add(hdr_offset as usize).cast::<FifoHeader>() };
        let first_chunk = unsafe { base.add(start_chunk as usize).cast::<Chunk>() };
        let min_alloc = unsafe { (*first_chunk).length.load(Ordering::Acquire) };
        if min_alloc == 0 {
            return Err(FifoError::InvalidCapacity);
        }
        let layout_end = (fs_offset as usize)
            .checked_add(end_chunk as usize)
            .and_then(|value| value.checked_add(CHUNK_HEADER_SIZE))
            .ok_or(FifoError::SegmentExhausted)?;
        if layout_end > segment.ssvm_size() {
            return Err(FifoError::SegmentExhausted);
        }
        unsafe {
            std::ptr::write(
                hdr,
                FifoHeader {
                    start_chunk: AtomicU64::new(start_chunk),
                    end_chunk: AtomicU64::new(end_chunk),
                    min_alloc,
                    size: capacity as u32,
                    slice_index: 0,
                    _pad0: [0; 7],
                    next: AtomicU64::new(0),
                    signals: SvmFifoSignals::new(),
                    _shared_pad: [0; 128 - (8 + 8 + 4 + 4 + 1 + 7 + 8 + 28)],
                    head_chunk: AtomicU64::new(start_chunk),
                    head: AtomicU32::new(0),
                    _consumer_pad: [0; 64 - (8 + 4)],
                    tail_chunk: AtomicU64::new(start_chunk),
                    tail: AtomicU32::new(0),
                    _producer_pad: [0; 64 - (8 + 4)],
                },
            );
        }
        let fifo = Self {
            shr: hdr,
            fs_hdr: base.cast::<FifoSegmentHeader>(),
            ooo_enq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq: std::ptr::null_mut(),
            ooo_enq: std::ptr::null_mut(),
            ooo_segments: UnsafeCell::new(Pool::with_capacity(4)),
            ooos_list_head: UnsafeCell::new(OOO_SEGMENT_INVALID_INDEX),
            ooos_newest: UnsafeCell::new(OOO_SEGMENT_INVALID_INDEX),
            flags: 0,
            refcnt: 1,
            client_thread_index: 0,
            app_session_index: u32::MAX,
            session: SvmFifoSession {
                session_handle: u64::MAX,
            },
            segment_manager: u32::MAX,
            segment_index: u32::MAX,
            signals: unsafe { std::ptr::addr_of_mut!((*hdr).signals) },
            next: std::ptr::null_mut(),
            prev: std::ptr::null_mut(),
            attachment: SvmFifoAttachment {
                segment: SvmFifoSegmentIndex {
                    context_index: u32::MAX,
                    client_segment_index: u32::MAX,
                },
            },
            segment,
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo_base: UnsafeCell::new(0),
        };
        fifo.initialize_chunk_lookups();
        Ok(fifo)
    }

    /// Reconstructs the process-local FIFO state for an already initialized
    /// shared FIFO. Shared head/tail/chunk state is left untouched.
    pub unsafe fn attach_at_svm_shared(
        segment: Arc<SsvmPrivate>,
        hdr_offset: u64,
    ) -> Result<Self, FifoError> {
        let fs_offset = segment
            .fifo_segment_offset()
            .ok_or(FifoError::SegmentExhausted)?;
        let base = unsafe { segment.base().add(fs_offset as usize) };
        let segment_header = unsafe { &*base.cast::<FifoSegmentHeader>() };
        if !fs_offset_is_valid(hdr_offset, segment_header.max_byte_index) {
            return Err(FifoError::SegmentExhausted);
        }
        if hdr_offset
            .checked_add(std::mem::size_of::<FifoHeader>() as u64)
            .is_none_or(|end| end > segment_header.max_byte_index)
        {
            return Err(FifoError::SegmentExhausted);
        }
        let hdr = unsafe { base.add(hdr_offset as usize).cast::<FifoHeader>() };
        let (capacity, start_chunk, end_chunk) = unsafe {
            (
                (*hdr).size as usize,
                (*hdr).start_chunk.load(Ordering::Acquire),
                (*hdr).end_chunk.load(Ordering::Acquire),
            )
        };
        if capacity == 0
            || !fs_offset_is_valid(start_chunk, segment_header.max_byte_index)
            || !fs_offset_is_valid(end_chunk, segment_header.max_byte_index)
        {
            return Err(FifoError::SegmentExhausted);
        }
        let fifo = Self {
            shr: hdr,
            fs_hdr: base.cast::<FifoSegmentHeader>(),
            ooo_enq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq: std::ptr::null_mut(),
            ooo_enq: std::ptr::null_mut(),
            ooo_segments: UnsafeCell::new(Pool::with_capacity(4)),
            ooos_list_head: UnsafeCell::new(OOO_SEGMENT_INVALID_INDEX),
            ooos_newest: UnsafeCell::new(OOO_SEGMENT_INVALID_INDEX),
            flags: 0,
            refcnt: 1,
            client_thread_index: 0,
            app_session_index: u32::MAX,
            session: SvmFifoSession {
                session_handle: u64::MAX,
            },
            segment_manager: u32::MAX,
            segment_index: u32::MAX,
            signals: unsafe { std::ptr::addr_of_mut!((*hdr).signals) },
            next: std::ptr::null_mut(),
            prev: std::ptr::null_mut(),
            attachment: SvmFifoAttachment {
                segment: SvmFifoSegmentIndex {
                    context_index: u32::MAX,
                    client_segment_index: u32::MAX,
                },
            },
            segment,
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo_base: UnsafeCell::new(0),
        };
        fifo.initialize_chunk_lookups();
        Ok(fifo)
    }

    /// Offset of the [`FifoHeader`] within the FIFO Segment mapping.
    #[inline]
    pub fn hdr_offset(&self) -> u64 {
        self.hdr_off
    }

    #[inline(always)]
    pub(crate) fn set_slice_index(&mut self, slice: u32) {
        assert!(
            slice <= u8::MAX as u32,
            "FIFO slice index exceeds shared field"
        );
        unsafe { (*self.hdr).slice_index = slice as u8 };
    }

    /// Copies the process-local FIFO state while retaining the shared FIFO
    /// header and chunk chain, matching `fifo_segment_duplicate_fifo`.
    ///
    /// # Safety
    /// The caller must preserve the shared FIFO's single producer and single
    /// consumer ownership. Duplicating private lookup state does not authorize
    /// either side to run concurrently with another owner of that same side.
    pub unsafe fn duplicate(&self) -> Self {
        Self {
            shr: self.shr,
            fs_hdr: self.fs_hdr,
            ooo_enq_lookup: UnsafeCell::new(unsafe { &*self.ooo_enq_lookup.get() }.clone()),
            ooo_deq_lookup: UnsafeCell::new(unsafe { &*self.ooo_deq_lookup.get() }.clone()),
            ooo_deq: self.ooo_deq,
            ooo_enq: self.ooo_enq,
            ooo_segments: UnsafeCell::new(unsafe { &*self.ooo_segments.get() }.clone()),
            ooos_list_head: UnsafeCell::new(unsafe { *self.ooos_list_head.get() }),
            ooos_newest: UnsafeCell::new(unsafe { *self.ooos_newest.get() }),
            flags: self.flags,
            refcnt: self.refcnt,
            client_thread_index: self.client_thread_index,
            app_session_index: self.app_session_index,
            session: self.session,
            segment_manager: self.segment_manager,
            segment_index: self.segment_index,
            signals: self.signals,
            next: std::ptr::null_mut(),
            prev: std::ptr::null_mut(),
            attachment: self.attachment,
            segment: Arc::clone(&self.segment),
            base: self.base,
            hdr: self.hdr,
            hdr_off: self.hdr_off,
            ooo_base: UnsafeCell::new(unsafe { *self.ooo_base.get() }),
        }
    }

    fn initialize_chunk_lookups(&self) {
        // Construction, reset and pointer initialization exclude producer and
        // consumer access to both process-local lookup trees.
        let enqueue = unsafe { &mut *self.ooo_enq_lookup.get() };
        let dequeue = unsafe { &mut *self.ooo_deq_lookup.get() };
        while let Some((key, _)) = enqueue.first() {
            let key = *key;
            enqueue.remove(&key);
        }
        while let Some((key, _)) = dequeue.first() {
            let key = *key;
            dequeue.remove(&key);
        }
        let mut chunk_off = unsafe { (*self.hdr).start_chunk.load(Ordering::Relaxed) };
        while chunk_off != 0 {
            let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
            enqueue.insert(chunk.start_byte, chunk_off as u32);
            dequeue.insert(chunk.start_byte, chunk_off as u32);
            chunk_off = chunk.next.load(Ordering::Acquire);
        }
    }

    unsafe fn release_chunk(&self, chunk_off: u64) {
        let header = unsafe { &*self.fs_hdr };
        let slice_index = unsafe { (*self.hdr).slice_index as usize };
        let slices = unsafe { header.slices() };
        let Some(slice) = slices.get(slice_index) else {
            return;
        };
        let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
        let length = chunk.length.load(Ordering::Relaxed) as usize;
        assert!(
            fs_offset_is_valid(chunk_off, header.max_byte_index),
            "FIFO chunk offset is outside the segment"
        );
        let class = fs_chunk_class(length);
        let mut head = slice.free_chunks[class].load(Ordering::Acquire);
        loop {
            chunk.next.store(fs_head_offset(head), Ordering::Relaxed);
            let new_head = fs_head_with_next(head, chunk_off);
            match slice.free_chunks[class].compare_exchange_weak(
                head,
                new_head,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => head = observed,
            }
        }
        slice
            .n_fl_chunk_bytes
            .fetch_add(length as u64, Ordering::Relaxed);
        header
            .n_cached_bytes
            .fetch_add(length as u64, Ordering::Relaxed);
    }

    #[inline]
    pub fn enqueue(&self, src: &[u8]) -> usize {
        let (copied, promoted) = self.enqueue_in_order(src);
        copied + promoted
    }

    fn enqueue_in_order(&self, src: &[u8]) -> (usize, usize) {
        unsafe { *self.ooos_newest.get() = OOO_SEGMENT_INVALID_INDEX };
        if src.is_empty() {
            return (0, 0);
        }
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Acquire);
            let tail = (*hdr).tail.load(Ordering::Relaxed);
            if head == tail {
                self.prepare_empty_tail_chunk(tail);
            }
            let used = tail.wrapping_sub(head);
            let free = ((*hdr).size - used) as usize;
            let mut to_write = src.len().min(free);
            if to_write == 0 {
                return (0, 0);
            }
            if self.ensure_chunk_capacity(head, tail, to_write).is_err() {
                // VPP svm_fifo_enqueue keeps the physical prefix when a new
                // chunk cannot be allocated; the caller observes a short write.
                let end = f_chunk_end(&*f_end_cptr(self));
                to_write = to_write.min(end.wrapping_sub(tail) as usize);
                if to_write == 0 {
                    return (0, 0);
                }
            }
            let old_tail_chunk = (*hdr).tail_chunk.load(Ordering::Relaxed);
            let written = self.append_at_tail_without_tail_store(tail, &src[..to_write]);
            if written == 0 {
                return (0, 0);
            }
            let new_tail = tail.wrapping_add(written as u32);
            let collected = self.promote_contiguous_from(new_tail);
            let final_tail = new_tail.wrapping_add(collected);
            self.clear_enq_chunks(old_tail_chunk, final_tail);
            (*hdr).tail.store(final_tail, Ordering::Release);
            (written, collected as usize)
        }
    }

    #[inline]
    pub fn peek(&self, offset: usize, len: usize, dst: &mut [u8]) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let available = tail.wrapping_sub(head) as usize;
            if offset >= available {
                return 0;
            }
            let to_copy = len.min(dst.len()).min(available - offset);
            if to_copy == 0 {
                return 0;
            }
            let logical_pos = head.wrapping_add(offset as u32);
            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            let mut copied = 0usize;
            let mut pos = logical_pos;
            while copied < to_copy && chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                if f_chunk_includes_pos(chunk, pos) {
                    let chunk_avail = pos.wrapping_sub(chunk.start_byte);
                    let chunk_avail =
                        chunk.length.load(Ordering::Relaxed) as usize - chunk_avail as usize;
                    let to_read = (to_copy - copied).min(chunk_avail);
                    let mut last = 0;
                    svm_fifo_copy_from_chunk(
                        self,
                        chunk as *const SvmFifoChunk as *mut SvmFifoChunk,
                        pos,
                        dst.as_mut_ptr().add(copied),
                        to_read as u32,
                        &mut last,
                    );
                    copied += to_read;
                    pos = pos.wrapping_add(to_read as u32);
                }
                chunk_off = chunk.next.load(Ordering::Acquire);
            }
            copied
        }
    }

    #[inline]
    pub fn readable_segments<'fifo>(
        &'fifo self,
        offset: usize,
        output: &mut [std::mem::MaybeUninit<&'fifo [u8]>],
    ) -> Result<usize, FifoError> {
        if output.is_empty() {
            return Err(FifoError::InvalidCapacity);
        }
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let available = tail.wrapping_sub(head) as usize;
            if offset >= available {
                return Ok(0);
            }
            let to_read = available - offset;
            let logical_pos = head + offset as u32;
            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            let mut written = 0;
            let mut remaining = to_read;
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                if f_chunk_includes_pos(chunk, logical_pos) {
                    let data_off = logical_pos.wrapping_sub(chunk.start_byte) as usize;
                    let chunk_avail = chunk.length.load(Ordering::Relaxed) as usize - data_off;
                    let first_len = remaining.min(chunk_avail);
                    let first_slice: &'fifo [u8] = std::slice::from_raw_parts(
                        self.base
                            .add(chunk_off as usize + CHUNK_HEADER_SIZE + data_off),
                        first_len,
                    );
                    output[written].write(first_slice);
                    written += 1;
                    remaining -= first_len;
                    if remaining == 0 {
                        return Ok(written);
                    }
                    chunk_off = chunk.next.load(Ordering::Acquire);
                    while chunk_off != 0 && remaining != 0 {
                        if written == output.len() {
                            // VPP svm_fifo_segments returns the prefix that fits
                            // in the caller's segment array. Continue with another
                            // offset when more chunks are needed.
                            return Ok(written);
                        }
                        let next = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                        let next_len = remaining.min(next.length.load(Ordering::Relaxed) as usize);
                        let next_slice: &'fifo [u8] = std::slice::from_raw_parts(
                            self.base.add(chunk_off as usize + CHUNK_HEADER_SIZE),
                            next_len,
                        );
                        output[written].write(next_slice);
                        written += 1;
                        remaining -= next_len;
                        chunk_off = next.next.load(Ordering::Acquire);
                    }
                    return (remaining == 0)
                        .then_some(written)
                        .ok_or(FifoError::SegmentExhausted);
                }
                chunk_off = chunk.next.load(Ordering::Acquire);
            }
            Err(FifoError::SegmentExhausted)
        }
    }

    #[inline]
    pub fn drop_dequeue(&self, len: usize) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let available = tail.wrapping_sub(head) as usize;
            let to_drop = len.min(available);
            if to_drop == 0 {
                return 0;
            }
            let new_head = head.wrapping_add(to_drop as u32);
            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = f_chunk_end(chunk);
                if f_pos_geq(new_head, chunk_end) {
                    // The producer may already have advanced tail_chunk while
                    // still clearing this chunk before publishing the new tail.
                    if !f_pos_gt(tail, chunk_end) {
                        break;
                    }
                    if chunk_off == (*hdr).tail_chunk.load(Ordering::Acquire) {
                        break;
                    }
                    let next_off = chunk.next.load(Ordering::Acquire);
                    if next_off == 0 {
                        break;
                    }
                    (*hdr).head_chunk.store(next_off, Ordering::Release);
                    (*hdr).start_chunk.store(next_off, Ordering::Release);
                    (&mut *self.ooo_deq_lookup.get()).remove(&chunk.start_byte);
                    self.release_chunk(chunk_off);
                    chunk_off = next_off;
                } else {
                    break;
                }
            }
            (*hdr).head.store(new_head, Ordering::Release);
            to_drop
        }
    }

    // Rebase after observing the released head without discarding future OOO bytes.
    unsafe fn prepare_empty_tail_chunk(&self, tail: u32) {
        let chunk_off = unsafe { (*self.hdr).tail_chunk.load(Ordering::Relaxed) };
        if chunk_off == 0 {
            return;
        }
        let chunk = unsafe { &mut *self.base.add(chunk_off as usize).cast::<Chunk>() };
        if chunk.next.load(Ordering::Acquire) != 0 {
            return;
        }
        let visible_len = tail.wrapping_sub(chunk.start_byte);
        if chunk.start_byte != tail && chunk.length.load(Ordering::Relaxed) <= visible_len {
            chunk.start_byte = tail;
        }
    }

    // VPP svm_fifo.c:f_try_chunk_alloc. Only the FIFO producer extends the
    // chain; the consumer releases old chunks to the segment's shared slice.
    fn ensure_chunk_capacity(&self, head: u32, tail: u32, len: usize) -> Result<(), FifoError> {
        if len == 0 {
            return Ok(());
        }
        let header = unsafe { &*self.hdr };
        let old_end_offset = header.end_chunk.load(Ordering::Relaxed);
        let old_end = unsafe { &*self.base.add(old_end_offset as usize).cast::<Chunk>() };
        let mut end_byte = f_chunk_end(old_end);
        let target = tail.wrapping_add(len as u32);
        if !f_pos_gt(target, end_byte) {
            return Ok(());
        }

        let free = header.size - tail.wrapping_sub(head);
        let mut first = 0;
        let mut last = 0;
        while f_pos_gt(target, end_byte) {
            let missing = target.wrapping_sub(end_byte) as usize;
            let requested = (header.min_alloc.min(free) as usize)
                .max(missing)
                .min(1usize << FS_MAX_LOG2_CHUNK_SIZE);
            let chunk_offset = match crate::svm::fifo_segment::allocate_chunk(
                unsafe { &*self.fs_hdr },
                u32::from(header.slice_index),
                requested,
                end_byte,
            ) {
                Ok(offset) => offset,
                Err(source) => {
                    while first != 0 {
                        let chunk = unsafe { &*self.base.add(first as usize).cast::<Chunk>() };
                        let next = chunk.next.load(Ordering::Relaxed);
                        unsafe { self.release_chunk(first) };
                        first = next;
                    }
                    return Err(source);
                }
            };
            if last == 0 {
                first = chunk_offset;
            } else {
                unsafe { &*self.base.add(last as usize).cast::<Chunk>() }
                    .next
                    .store(chunk_offset, Ordering::Relaxed);
            }
            last = chunk_offset;
            let chunk = unsafe { &*self.base.add(chunk_offset as usize).cast::<Chunk>() };
            end_byte = f_chunk_end(chunk);
        }

        old_end.next.store(first, Ordering::Release);
        header.end_chunk.store(last, Ordering::Release);
        Ok(())
    }

    fn append_at_tail_without_tail_store(&self, offset: u32, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let mut written = 0usize;
            let mut remaining_offset = offset;
            let mut remaining_src = src;
            let mut chunk_off = (*hdr).tail_chunk.load(Ordering::Relaxed);

            while !remaining_src.is_empty() {
                if chunk_off == 0 {
                    return written;
                }

                let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = f_chunk_end(chunk);
                if f_pos_geq(remaining_offset, chunk_end) {
                    let next_off = chunk.next.load(Ordering::Acquire);
                    if next_off != 0 {
                        let next = &*(self.base.add(next_off as usize) as *mut Chunk);
                        if f_pos_geq(remaining_offset, next.start_byte) {
                            (*hdr).tail_chunk.store(next_off, Ordering::Release);
                            chunk_off = next_off;
                            continue;
                        }
                    }
                    if f_pos_gt(remaining_offset, chunk_end) {
                        return written;
                    }

                    return written;
                }

                if !f_chunk_includes_pos(chunk, remaining_offset) && remaining_offset != chunk_end {
                    return written;
                }

                let data_off = remaining_offset.wrapping_sub(chunk.start_byte) as usize;
                let chunk_avail = chunk.length.load(Ordering::Relaxed) as usize - data_off;
                let to_write = remaining_src.len().min(chunk_avail);
                let mut last = 0;
                svm_fifo_copy_to_chunk(
                    self,
                    chunk,
                    remaining_offset,
                    remaining_src.as_ptr(),
                    to_write as u32,
                    &mut last,
                );
                written += to_write;
                remaining_offset = remaining_offset.wrapping_add(to_write as u32);
                remaining_src = &remaining_src[to_write..];
            }
            written
        }
    }

    fn clear_enq_chunks(&self, start_chunk: u64, end_pos: u32) {
        // VPP f_lookup_clear_enq_chunks: the producer removes lookup entries
        // before publishing tail, so the consumer may then release old chunks.
        let lookup = unsafe { &mut *self.ooo_enq_lookup.get() };
        let mut chunk_off = start_chunk;
        while chunk_off != 0 {
            let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
            if f_chunk_includes_pos(chunk, end_pos) {
                break;
            }
            let next = chunk.next.load(Ordering::Acquire);
            if next == 0 {
                break;
            }
            lookup.remove(&chunk.start_byte);
            chunk_off = next;
        }
        if chunk_off != 0 {
            let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
            if unsafe { *self.ooos_list_head.get() } == OOO_SEGMENT_INVALID_INDEX {
                lookup.remove(&chunk.start_byte);
            }
            unsafe { (*self.hdr).tail_chunk.store(chunk_off, Ordering::Release) };
        }
    }

    fn write_at_without_tail_store(&self, offset: u32, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let mut written = 0usize;
            let mut remaining_offset = offset;
            let mut remaining_src = src;

            let lookup = &*self.ooo_enq_lookup.get();
            let mut chunk_off = lookup
                .get(&remaining_offset)
                .or_else(|| lookup.predecessor(&remaining_offset).map(|(_, value)| value))
                .map_or(0, |value| u64::from(*value));
            if chunk_off != 0 {
                let chunk = &*self.base.add(chunk_off as usize).cast::<Chunk>();
                if !f_chunk_includes_pos(chunk, remaining_offset) {
                    chunk_off = 0;
                }
            }
            if chunk_off == 0 {
                chunk_off = (*hdr).tail_chunk.load(Ordering::Acquire);
            }
            // Seek to the chunk covering remaining_offset
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = f_chunk_end(chunk);
                if f_chunk_includes_pos(chunk, remaining_offset) {
                    break;
                }
                if remaining_offset == chunk_end {
                    chunk_off = chunk.next.load(Ordering::Acquire);
                    continue;
                }
                chunk_off = chunk.next.load(Ordering::Acquire);
            }

            // Write across chunks, taking preallocated chunks at the end.
            while !remaining_src.is_empty() {
                if chunk_off == 0 {
                    return written;
                }

                let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                if f_chunk_includes_pos(chunk, remaining_offset) {
                    let data_off = remaining_offset.wrapping_sub(chunk.start_byte) as usize;
                    let chunk_avail = chunk.length.load(Ordering::Relaxed) as usize - data_off;
                    let to_write = remaining_src.len().min(chunk_avail);
                    let mut last = 0;
                    svm_fifo_copy_to_chunk(
                        self,
                        chunk,
                        remaining_offset,
                        remaining_src.as_ptr(),
                        to_write as u32,
                        &mut last,
                    );
                    written += to_write;
                    remaining_offset = remaining_offset.wrapping_add(to_write as u32);
                    remaining_src = &remaining_src[to_write..];
                }

                chunk_off = chunk.next.load(Ordering::Acquire);
            }
            written
        }
    }

    pub fn max_dequeue(&self) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            tail.wrapping_sub(head) as usize
        }
    }

    pub fn max_enqueue(&self) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Acquire);
            let tail = (*hdr).tail.load(Ordering::Relaxed);
            let used = tail.wrapping_sub(head);
            ((*hdr).size - used) as usize
        }
    }

    /// Copy borrowed segments into the FIFO and publish the producer tail.
    /// The caller supplies the total length so the capacity check happens
    /// before any bytes become visible, matching `svm_fifo_enqueue_segments`.
    pub fn enqueue_segments<I, S>(&self, total: usize, segments: I) -> Result<usize, FifoError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        if total > self.max_enqueue() {
            return Err(FifoError::InsufficientCapacity {
                requested: total,
                available: self.max_enqueue(),
            });
        }
        let head = unsafe { (*self.hdr).head.load(Ordering::Acquire) };
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        if head == tail {
            unsafe { self.prepare_empty_tail_chunk(tail) };
        }
        self.ensure_chunk_capacity(head, tail, total)?;
        let mut copied = 0usize;
        for segment in segments {
            let source = segment.as_ref();
            let next = copied
                .checked_add(source.len())
                .ok_or(FifoError::CapacityOutOfRange {
                    capacity: usize::MAX,
                })?;
            if next > total {
                return Err(FifoError::SegmentExhausted);
            }
            let written = self.append_at_tail_without_tail_store(
                unsafe { (*self.hdr).tail.load(Ordering::Relaxed) }.wrapping_add(copied as u32),
                source,
            );
            copied += written;
            if written != source.len() {
                return Err(FifoError::SegmentExhausted);
            }
        }
        if copied != total {
            return Err(FifoError::SegmentExhausted);
        }
        self.enqueue_nocopy(total)?;
        Ok(copied)
    }

    /// Publish bytes already copied into the producer chunks.
    pub fn enqueue_nocopy(&self, len: usize) -> Result<(), FifoError> {
        unsafe { *self.ooos_newest.get() = OOO_SEGMENT_INVALID_INDEX };
        if len > self.max_enqueue() {
            return Err(FifoError::InsufficientCapacity {
                requested: len,
                available: self.max_enqueue(),
            });
        }
        unsafe {
            let tail = (*self.hdr).tail.load(Ordering::Relaxed);
            let old_tail_chunk = (*self.hdr).tail_chunk.load(Ordering::Relaxed);
            let new_tail = tail.wrapping_add(len as u32);
            let collected = self.promote_contiguous_from(new_tail);
            let final_tail = new_tail.wrapping_add(collected);
            self.clear_enq_chunks(old_tail_chunk, final_tail);
            (*self.hdr).tail.store(final_tail, Ordering::Release);
        }
        Ok(())
    }

    #[inline]
    pub fn needs_deq_notification(&self, dropped: usize) -> bool {
        if dropped == 0 {
            return false;
        }
        let hdr = self.hdr;
        unsafe {
            (*hdr)
                .signals
                .want_deq_ntf
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        }
    }

    #[inline]
    pub fn has_event(&self) -> bool {
        unsafe { (*self.hdr).signals.has_event.load(Ordering::Acquire) != 0 }
    }

    #[inline]
    pub fn set_event(&self) -> bool {
        unsafe { (*self.hdr).signals.has_event.swap(1, Ordering::Release) == 0 }
    }

    #[inline]
    pub fn unset_event(&self) {
        unsafe {
            (*self.hdr).signals.has_event.swap(0, Ordering::Acquire);
        }
    }

    #[inline]
    pub fn want_deq_notification(&self) {
        unsafe {
            (*self.hdr).signals.want_deq_ntf.store(1, Ordering::Release);
        }
    }

    #[inline]
    pub fn clear_deq_notification(&self) {
        unsafe {
            (*self.hdr).signals.want_deq_ntf.store(0, Ordering::Release);
        }
    }

    #[inline]
    pub fn deq_threshold(&self) -> u32 {
        unsafe { (*self.hdr).signals.deq_thresh.load(Ordering::Relaxed) }
    }

    #[inline]
    pub fn has_deq_notification(&self) -> bool {
        unsafe { (*self.hdr).signals.has_deq_ntf.load(Ordering::Acquire) != 0 }
    }

    #[inline]
    pub fn clear_deq_notification_flag(&self) {
        unsafe {
            (*self.hdr).signals.has_deq_ntf.store(0, Ordering::Release);
        }
    }

    /// VPP: svm_fifo.h:868-875. Subscriber slots are changed by the owner
    /// while workers are stopped; the returned slice borrows this FIFO.
    #[inline(always)]
    pub fn subscribers(&self) -> &[u8] {
        let signals = unsafe { &(*self.hdr).signals };
        &signals.subscribers[..usize::from(signals.n_subscribers)]
    }

    pub fn clear(&mut self) {
        let hdr = self.hdr;
        unsafe {
            let first_chunk = (*hdr).start_chunk.load(Ordering::Relaxed);
            assert_ne!(first_chunk, 0, "a valid FIFO always retains one chunk");
            let first = &mut *self.base.add(first_chunk as usize).cast::<Chunk>();
            let mut chunk_off = first.next.load(Ordering::Relaxed);
            first.next.store(0, Ordering::Relaxed);
            while chunk_off != 0 {
                let chunk = &*self.base.add(chunk_off as usize).cast::<Chunk>();
                let next = chunk.next.load(Ordering::Relaxed);
                self.release_chunk(chunk_off);
                chunk_off = next;
            }
            first.start_byte = 0;
            (*hdr).head.store(0, Ordering::Relaxed);
            (*hdr).tail.store(0, Ordering::Relaxed);
            (*hdr).head_chunk.store(first_chunk, Ordering::Relaxed);
            (*hdr).tail_chunk.store(first_chunk, Ordering::Relaxed);
            (*hdr).start_chunk.store(first_chunk, Ordering::Relaxed);
            (*hdr).end_chunk.store(first_chunk, Ordering::Relaxed);
            (*hdr).signals.has_event.store(0, Ordering::Relaxed);
            (*hdr).signals.want_deq_ntf.store(0, Ordering::Relaxed);
            (*hdr).signals.has_deq_ntf.store(0, Ordering::Relaxed);
        }
        let segments = self.ooo_segments.get_mut();
        while let Some((index, _)) = segments.iter().next() {
            segments.remove(index);
        }
        *self.ooos_list_head.get_mut() = OOO_SEGMENT_INVALID_INDEX;
        *self.ooos_newest.get_mut() = OOO_SEGMENT_INVALID_INDEX;
        self.initialize_chunk_lookups();
    }

    pub fn is_empty(&self) -> bool {
        self.max_dequeue() == 0
    }

    pub fn is_full(&self) -> bool {
        self.max_enqueue() == 0
    }
}

impl Fifo {
    /// Set producer and consumer positions, matching `svm_fifo_init_pointers`.
    pub fn init_pointers(&self, head: u32, tail: u32) {
        unsafe {
            (*self.hdr).head.store(head, Ordering::Relaxed);
            (*self.hdr).tail.store(tail, Ordering::Relaxed);
            let chunk = (*self.hdr).head_chunk.load(Ordering::Relaxed);
            let mut chunk_off = chunk;
            let mut start = head;
            while chunk_off != 0 {
                let current = &mut *self.base.add(chunk_off as usize).cast::<Chunk>();
                current.start_byte = start;
                start = start.wrapping_add(current.length.load(Ordering::Relaxed));
                chunk_off = current.next.load(Ordering::Acquire);
            }
        }
        self.initialize_chunk_lookups();
    }

    #[inline]
    pub fn size(&self) -> usize {
        unsafe { (*self.hdr).size as usize }
    }

    /// Change the advertised capacity. Chunks are provisioned lazily by the
    /// segment allocator when the producer first reaches the new range.
    pub fn set_size(&self, size: usize) -> Result<(), FifoError> {
        if size == 0 || size > u32::MAX as usize {
            return Err(FifoError::CapacityOutOfRange { capacity: size });
        }
        let used = self.max_dequeue();
        if size < used {
            return Err(FifoError::InsufficientCapacity {
                requested: used,
                available: size,
            });
        }
        unsafe { (*self.hdr).size = size as u32 };
        Ok(())
    }

    #[inline]
    pub fn dequeue(&self, len: usize, dst: &mut [u8]) -> usize {
        let copied = self.peek(0, len, dst);
        self.drop_dequeue(copied)
    }

    #[inline]
    pub fn dequeue_drop_all(&self) -> usize {
        self.drop_dequeue(self.max_dequeue())
    }

    #[inline]
    pub fn enqueue_with_offset(
        &self,
        offset: u32,
        len: usize,
        src: &[u8],
    ) -> Result<u32, FifoError> {
        let requested = len.min(src.len());
        let result = self.enqueue_ooo(offset, &src[..requested])?;
        Ok(result.accepted)
    }

    #[inline]
    pub fn overwrite_head(&self, src: &[u8]) -> usize {
        let head = unsafe { (*self.hdr).head.load(Ordering::Relaxed) };
        self.write_at_without_tail_store(head, src)
    }

    #[inline]
    pub fn n_ooo_segments(&self) -> usize {
        self.ooo_enqueued()
    }

    #[inline]
    pub fn has_ooo_data(&self) -> bool {
        self.n_ooo_segments() != 0
    }

    pub fn first_ooo_segment(&self) -> Option<OooSegment> {
        let entries = unsafe { &*self.ooo_segments.get() };
        entries.get(unsafe { *self.ooos_list_head.get() }).copied()
    }

    pub fn n_chunks(&self) -> usize {
        let mut count = 0;
        let mut chunk_off = unsafe { (*self.hdr).start_chunk.load(Ordering::Relaxed) };
        while chunk_off != 0 {
            count += 1;
            chunk_off = unsafe {
                (*self.base.add(chunk_off as usize).cast::<Chunk>())
                    .next
                    .load(Ordering::Acquire)
            };
            if count > self.size().saturating_add(1) {
                break;
            }
        }
        count
    }

    pub fn is_sane(&self) -> bool {
        let head = unsafe { (*self.hdr).head.load(Ordering::Acquire) };
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
        f_cursize(head, tail) <= self.size() as u32 && self.n_chunks() != 0
    }

    pub fn enable_ooo(&mut self) {
        // VPP allocates the OOO pool lazily but keeps it on svm_fifo_t. The
        // pool and lookup already exist in this FIFO; enabling is retained as
        // a compatibility operation and only resets the sequence base.
        *self.ooo_base.get_mut() = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
    }

    pub fn enqueue_ooo(&self, offset: u32, src: &[u8]) -> Result<OooResult, FifoError> {
        let length = u32::try_from(src.len())
            .map_err(|_| FifoError::OutOfOrderLengthOutOfRange { length: src.len() })?;
        if offset == 0 {
            let (accepted, delivered) = self.enqueue_in_order(src);
            return Ok(OooResult {
                accepted: accepted as u32,
                delivered: delivered as u32,
                start: None,
                len: 0,
            });
        }
        let available = self.max_enqueue();
        if offset as usize > available || src.len() > available.saturating_sub(offset as usize) {
            return Err(FifoError::OutOfOrderCapacityExceeded {
                end_offset: offset.wrapping_add(length),
                available,
            });
        }
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        let head = unsafe { (*self.hdr).head.load(Ordering::Acquire) };
        self.ensure_chunk_capacity(head, tail, offset as usize + src.len())?;
        let abs_start = tail.wrapping_add(offset);
        let written = self.write_at_without_tail_store(abs_start, src);
        if written != src.len() {
            return Err(FifoError::SegmentExhausted);
        }
        let newest = self.add_ooo_segment(tail, offset, length);
        Ok(OooResult {
            accepted: length,
            delivered: 0,
            start: newest.map(|(start, _)| start),
            len: newest.map_or(0, |(_, len)| len),
        })
    }

    fn add_ooo_segment(&self, tail: u32, offset: u32, length: u32) -> Option<(u32, u32)> {
        let start = tail.wrapping_add(offset);
        let end = start.wrapping_add(length);
        // SAFETY: the FIFO producer alone mutates OOO metadata. The consumer
        // cannot observe future bytes until a release store advances tail.
        let segments = unsafe { &mut *self.ooo_segments.get() };
        let head = unsafe { &mut *self.ooos_list_head.get() };
        let newest = unsafe { &mut *self.ooos_newest.get() };
        *newest = OOO_SEGMENT_INVALID_INDEX;
        if length == 0 {
            return None;
        }

        let mut previous = OOO_SEGMENT_INVALID_INDEX;
        let mut current = *head;
        while current != OOO_SEGMENT_INVALID_INDEX {
            let segment = segments.get(current).expect("OOO link remains live");
            if !f_pos_lt(segment.start, start) {
                break;
            }
            previous = current;
            current = segment.next;
        }

        let target = if previous != OOO_SEGMENT_INVALID_INDEX {
            let segment = segments.get(previous).expect("OOO predecessor remains live");
            if f_pos_leq(start, segment.start.wrapping_add(segment.length)) {
                Some(previous)
            } else {
                None
            }
        } else {
            None
        };
        let target = target.or_else(|| {
            let segment = segments.get(current)?;
            f_pos_leq(segment.start, end).then_some(current)
        });

        let index = if let Some(index) = target {
            let segment = segments.get_mut(index).expect("OOO target remains live");
            let old_start = segment.start;
            let old_end = old_start.wrapping_add(segment.length);
            if f_pos_lt(start, old_start) {
                segment.start = start;
            }
            if f_pos_gt(end, old_end) {
                segment.length = end.wrapping_sub(segment.start);
            } else {
                segment.length = old_end.wrapping_sub(segment.start);
            }
            if segment.start == old_start && segment.length == old_end.wrapping_sub(old_start) {
                return None;
            }
            index
        } else {
            let index = segments.insert(OooSegment {
                next: current,
                prev: previous,
                start,
                length,
            });
            if previous == OOO_SEGMENT_INVALID_INDEX {
                *head = index;
            } else {
                segments.get_mut(previous).expect("OOO predecessor remains live").next = index;
            }
            if current != OOO_SEGMENT_INVALID_INDEX {
                segments.get_mut(current).expect("OOO successor remains live").prev = index;
            }
            index
        };

        loop {
            let segment = *segments.get(index).expect("merged OOO segment remains live");
            let next = segment.next;
            if next == OOO_SEGMENT_INVALID_INDEX {
                break;
            }
            let following = *segments.get(next).expect("OOO successor remains live");
            let segment_end = segment.start.wrapping_add(segment.length);
            if f_pos_gt(following.start, segment_end) {
                break;
            }
            let following_end = following.start.wrapping_add(following.length);
            let after = following.next;
            let merged = segments.get_mut(index).expect("merged OOO segment remains live");
            if f_pos_gt(following_end, segment_end) {
                merged.length = following_end.wrapping_sub(merged.start);
            }
            merged.next = after;
            if after != OOO_SEGMENT_INVALID_INDEX {
                segments.get_mut(after).expect("OOO successor remains live").prev = index;
            }
            segments.remove(next).expect("merged OOO successor remains live");
        }
        let segment = segments.get(index).expect("merged OOO segment remains live");
        *newest = index;
        Some((segment.start.wrapping_sub(tail), segment.length))
    }

    fn promote_contiguous_from(&self, base: u32) -> u32 {
        let mut tail = base;
        let segments = unsafe { &mut *self.ooo_segments.get() };
        let head = unsafe { &mut *self.ooos_list_head.get() };
        let newest = unsafe { &mut *self.ooos_newest.get() };
        loop {
            let Some(segment) = segments.get(*head).copied() else {
                break;
            };
            if f_pos_gt(segment.start, tail) {
                break;
            }
            let finish = segment.start.wrapping_add(segment.length);
            if f_pos_gt(finish, tail) {
                tail = finish;
            }
            let old_head = *head;
            *head = segment.next;
            if *head != OOO_SEGMENT_INVALID_INDEX {
                segments.get_mut(*head).expect("OOO successor remains live").prev =
                    OOO_SEGMENT_INVALID_INDEX;
            }
            if *newest == old_head {
                *newest = OOO_SEGMENT_INVALID_INDEX;
            }
            segments.remove(old_head).expect("collected OOO segment remains live");
        }
        tail.wrapping_sub(base)
    }

    pub fn promote_contiguous(&self) -> u32 {
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        let old_tail_chunk = unsafe { (*self.hdr).tail_chunk.load(Ordering::Relaxed) };
        let collected = self.promote_contiguous_from(tail);
        if collected != 0 {
            let final_tail = tail.wrapping_add(collected);
            self.clear_enq_chunks(old_tail_chunk, final_tail);
            unsafe { (*self.hdr).tail.store(final_tail, Ordering::Release) };
        }
        collected
    }

    pub fn ooo_head(&self) -> Option<(u32, u32)> {
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        let entries = unsafe { &*self.ooo_segments.get() };
        let segment = entries.get(unsafe { *self.ooos_list_head.get() })?;
        Some((segment.start.wrapping_sub(tail), segment.length))
    }

    pub fn ooo_enqueued(&self) -> usize {
        unsafe { (&*self.ooo_segments.get()).len() }
    }
}
