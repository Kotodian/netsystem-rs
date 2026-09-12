use std::cell::UnsafeCell;
use std::io::{self, BufRead, Read, Write};
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::march_fn;
use crate::pool::Pool;
use crate::rbtree::RbTree;
use crate::segment::Segment;
use crate::svm::ssvm::{SSVM_PAYLOAD_OFFSET, SsvmPrivate};

pub type FsSptr = u64;

pub const OOO_SEGMENT_INVALID_INDEX: u32 = u32::MAX;
pub const FS_MIN_LOG2_CHUNK_SIZE: u32 = 12;
pub const FS_MAX_LOG2_CHUNK_SIZE: u32 = 22;
pub const FS_CHUNK_VEC_LEN: usize = (FS_MAX_LOG2_CHUNK_SIZE - FS_MIN_LOG2_CHUNK_SIZE + 1) as usize;

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
    pub _slice_pad: [u8; 40],
}

impl FifoSegmentHeader {
    pub const MAGIC: u64 = 0x4841_4d4d_4552_4653;

    pub const fn layout_bytes(n_slices: usize) -> usize {
        std::mem::size_of::<Self>() + n_slices * std::mem::size_of::<FifoSegmentSlice>()
    }

    pub unsafe fn slices(&self) -> &[FifoSegmentSlice] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self).add(1).cast::<FifoSegmentSlice>(),
                self.n_slices as usize,
            )
        }
    }

    pub unsafe fn slices_mut(&mut self) -> &mut [FifoSegmentSlice] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self as *mut Self).add(1).cast::<FifoSegmentSlice>(),
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
pub unsafe fn fs_ptr(fsh: *mut FifoSegmentHeader, sp: FsSptr) -> *mut u8 {
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
        (ptr as usize).wrapping_sub(fsh as usize) as FsSptr
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
    // standalone Rust implementation keeps the actual segment owner below,
    // but callers can inspect the same identity and union state.
    pub shr: *mut SvmFifoShared,
    pub fs_hdr: *mut FifoSegmentHeader,
    pub ooo_enq_lookup: UnsafeCell<RbTree<u32, u32>>,
    pub ooo_deq_lookup: UnsafeCell<RbTree<u32, u32>>,
    pub ooo_deq: *mut SvmFifoChunk,
    pub ooo_enq: *mut SvmFifoChunk,
    pub ooo_segments: UnsafeCell<Pool<OooSegment>>,
    pub ooos_list_head: u32,
    pub ooos_newest: u32,
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

    storage: FifoStorage,
    base: *mut u8,
    hdr: *mut FifoHeader,
    hdr_off: u64,
    ooo_base: UnsafeCell<u32>,
}

enum FifoStorage {
    Segment(Segment),
    Ssvm(Arc<SsvmPrivate>),
}

unsafe impl Send for Fifo {}
unsafe impl Sync for Fifo {}

impl Read for &Fifo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.max_dequeue() == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        let read = self.peek(0, buf.len(), buf);
        assert_eq!(
            self.dequeue_drop(read),
            read,
            "FIFO readable bytes changed while held by its consumer"
        );
        Ok(read)
    }
}

impl BufRead for &Fifo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.readable_segment()
            .ok_or_else(|| io::ErrorKind::WouldBlock.into())
    }

    fn consume(&mut self, amount: usize) {
        let readable = self.readable_segment().map_or(0, <[u8]>::len);
        assert!(
            amount <= readable,
            "cannot consume {amount} bytes from a FIFO segment containing {readable} bytes"
        );
        assert_eq!(
            self.dequeue_drop(amount),
            amount,
            "FIFO readable bytes changed while held by its consumer"
        );
    }
}

impl Write for &Fifo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let available = self.max_enqueue();
        if available == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let requested = buf.len().min(available);
        match self.enqueue_segments(requested, [buf]) {
            Ok(written) => Ok(written),
            Err(error) => Err(io::Error::other(error)),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

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

    pub fn new(seg: Segment, capacity: usize) -> Result<Self, FifoError> {
        let bytes = Self::layout_bytes(capacity)?;
        let hdr_off = seg.alloc(bytes, 64).ok_or(FifoError::SegmentExhausted)?;
        unsafe { Self::init_at(seg, hdr_off, capacity) }
    }

    /// Initialise a [`Fifo`] header at a pre-allocated offset in `seg`.
    /// The caller must guarantee that `seg` has [`Self::layout_bytes`] bytes
    /// available at `hdr_offset` and that no other [`Fifo`] uses the same
    /// region.
    pub unsafe fn init_at(
        seg: Segment,
        hdr_offset: u64,
        capacity: usize,
    ) -> Result<Self, FifoError> {
        let layout = Self::layout_bytes(capacity)?;
        let offset = usize::try_from(hdr_offset).expect("FIFO offset exceeds usize");
        let end = offset
            .checked_add(layout)
            .expect("FIFO layout end overflows usize");
        assert!(end <= seg.size(), "FIFO layout exceeds segment bounds");
        let base = seg.base();
        let hdr = unsafe { base.add(offset) as *mut FifoHeader };
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
                    base.add(chunk_off as usize) as *mut Chunk,
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
        Ok(Self {
            shr: hdr,
            fs_hdr: base.cast::<FifoSegmentHeader>(),
            ooo_enq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq: std::ptr::null_mut(),
            ooo_enq: std::ptr::null_mut(),
            ooo_segments: UnsafeCell::new(Pool::with_capacity(4)),
            ooos_list_head: OOO_SEGMENT_INVALID_INDEX,
            ooos_newest: OOO_SEGMENT_INVALID_INDEX,
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
            storage: FifoStorage::Segment(seg),
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo_base: UnsafeCell::new(0),
        })
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
        Ok(Self {
            shr: hdr,
            fs_hdr: base.cast::<FifoSegmentHeader>(),
            ooo_enq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq: std::ptr::null_mut(),
            ooo_enq: std::ptr::null_mut(),
            ooo_segments: UnsafeCell::new(Pool::with_capacity(4)),
            ooos_list_head: OOO_SEGMENT_INVALID_INDEX,
            ooos_newest: OOO_SEGMENT_INVALID_INDEX,
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
            storage: FifoStorage::Ssvm(segment),
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo_base: UnsafeCell::new(0),
        })
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
        if capacity == 0 || capacity > u32::MAX as usize || start_chunk == 0 {
            return Err(FifoError::InvalidCapacity);
        }
        let fs_offset = segment.fifo_segment_offset().unwrap_or(SSVM_PAYLOAD_OFFSET);
        let base = unsafe { segment.base().add(fs_offset as usize) };
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
        Ok(Self {
            shr: hdr,
            fs_hdr: base.cast::<FifoSegmentHeader>(),
            ooo_enq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq: std::ptr::null_mut(),
            ooo_enq: std::ptr::null_mut(),
            ooo_segments: UnsafeCell::new(Pool::with_capacity(4)),
            ooos_list_head: OOO_SEGMENT_INVALID_INDEX,
            ooos_newest: OOO_SEGMENT_INVALID_INDEX,
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
            storage: FifoStorage::Ssvm(segment),
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo_base: UnsafeCell::new(0),
        })
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
        let hdr = unsafe { base.add(hdr_offset as usize).cast::<FifoHeader>() };
        let (capacity, start_chunk, end_chunk) = unsafe {
            (
                (*hdr).size as usize,
                (*hdr).start_chunk.load(Ordering::Acquire),
                (*hdr).end_chunk.load(Ordering::Acquire),
            )
        };
        if capacity == 0 || start_chunk == 0 || end_chunk == 0 {
            return Err(FifoError::SegmentExhausted);
        }
        Ok(Self {
            shr: hdr,
            fs_hdr: base.cast::<FifoSegmentHeader>(),
            ooo_enq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq: std::ptr::null_mut(),
            ooo_enq: std::ptr::null_mut(),
            ooo_segments: UnsafeCell::new(Pool::with_capacity(4)),
            ooos_list_head: OOO_SEGMENT_INVALID_INDEX,
            ooos_newest: OOO_SEGMENT_INVALID_INDEX,
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
            storage: FifoStorage::Ssvm(segment),
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo_base: UnsafeCell::new(0),
        })
    }

    /// Offset of the [`FifoHeader`] within the backing [`Segment`].
    /// Used by `from_shared` to reconstruct the same FIFO in another
    /// process that shares the segment.
    #[inline]
    pub fn hdr_offset(&self) -> u64 {
        self.hdr_off
    }

    unsafe fn acquire_chunk(&self, start_byte: u32) -> Option<u64> {
        let bytes = CHUNK_HEADER_SIZE + unsafe { (*self.hdr).min_alloc as usize };
        let chunk_off = match &self.storage {
            FifoStorage::Segment(segment) => segment.alloc(bytes, 8)?,
            FifoStorage::Ssvm(_) => return None,
        };
        unsafe {
            std::ptr::write(
                self.base.add(chunk_off as usize).cast::<Chunk>(),
                Chunk {
                    start_byte,
                    length: AtomicU32::new((*self.hdr).min_alloc),
                    next: AtomicU64::new(0),
                    enq_rb_index: OOO_SEGMENT_INVALID_INDEX,
                    deq_rb_index: OOO_SEGMENT_INVALID_INDEX,
                },
            );
        }
        Some(chunk_off)
    }

    unsafe fn release_chunk(&self, chunk_off: u64) {
        let FifoStorage::Ssvm(_) = &self.storage else {
            return;
        };
        let header = unsafe { &*self.fs_hdr };
        let slice_index = unsafe { (*self.hdr).slice_index as usize };
        let slices = unsafe { header.slices() };
        let Some(slice) = slices.get(slice_index) else {
            return;
        };
        let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
        let length = chunk.length.load(Ordering::Relaxed) as usize;
        let class = if length <= 1 << FS_MIN_LOG2_CHUNK_SIZE {
            0
        } else {
            ((usize::BITS - (length - 1).leading_zeros()).saturating_sub(FS_MIN_LOG2_CHUNK_SIZE)
                as usize)
                .min(FS_CHUNK_VEC_LEN - 1)
        };
        let mut head = slice.free_chunks[class].load(Ordering::Acquire);
        loop {
            chunk.next.store(head, Ordering::Relaxed);
            match slice.free_chunks[class].compare_exchange_weak(
                head,
                chunk_off,
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
        if src.is_empty() {
            return 0;
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
            let to_write = src.len().min(free);
            if to_write == 0 {
                return 0;
            }
            let written = self.append_at_tail_without_tail_store(tail, &src[..to_write]);
            if written == 0 {
                return 0;
            }
            let new_tail = tail.wrapping_add(written as u32);
            (*hdr).tail.store(new_tail, Ordering::Release);
            let collected = self.promote_contiguous_from(new_tail);
            (*hdr)
                .tail
                .store(new_tail.wrapping_add(collected), Ordering::Release);
            written + collected as usize
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
    pub fn segments(&self, offset: usize, len: usize) -> Option<(&[u8], &[u8])> {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let available = tail.wrapping_sub(head) as usize;
            if offset >= available {
                return None;
            }
            let to_read = len.min(available - offset);
            if to_read == 0 {
                return Some((&[], &[]));
            }
            let logical_pos = head + offset as u32;
            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                if f_chunk_includes_pos(chunk, logical_pos) {
                    let data_off = logical_pos.wrapping_sub(chunk.start_byte) as usize;
                    let chunk_avail = chunk.length.load(Ordering::Relaxed) as usize - data_off;
                    let first_len = to_read.min(chunk_avail);
                    let first_slice = std::slice::from_raw_parts(
                        self.base
                            .add(chunk_off as usize + CHUNK_HEADER_SIZE + data_off),
                        first_len,
                    );
                    if first_len == to_read {
                        return Some((first_slice, &[]));
                    }
                    let second_len = to_read - first_len;
                    let next_off = chunk.next.load(Ordering::Acquire);
                    if next_off != 0 {
                        let next_chunk = &*(self.base.add(next_off as usize) as *mut Chunk);
                        let second_avail = next_chunk.length.load(Ordering::Relaxed) as usize;
                        let second_actual = second_len.min(second_avail);
                        let second_slice = std::slice::from_raw_parts(
                            self.base.add(next_off as usize + CHUNK_HEADER_SIZE),
                            second_actual,
                        );
                        return Some((first_slice, second_slice));
                    }
                    return Some((first_slice, &[]));
                }
                chunk_off = chunk.next.load(Ordering::Acquire);
            }
            None
        }
    }

    fn readable_segment(&self) -> Option<&[u8]> {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            if head == tail {
                return None;
            }

            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            while chunk_off != 0 {
                let chunk = &*self.base.add(chunk_off as usize).cast::<Chunk>();
                if f_chunk_includes_pos(chunk, head) {
                    let data_offset = head.wrapping_sub(chunk.start_byte) as usize;
                    let available = (chunk.length.load(Ordering::Relaxed) as u32)
                        .wrapping_sub(data_offset as u32)
                        .min(tail.wrapping_sub(head)) as usize;
                    return Some(std::slice::from_raw_parts(
                        self.base
                            .add(chunk_off as usize + CHUNK_HEADER_SIZE + data_offset),
                        available,
                    ));
                }
                chunk_off = chunk.next.load(Ordering::Acquire);
            }
            None
        }
    }

    #[inline]
    pub fn dequeue_drop(&self, len: usize) -> usize {
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
                if f_pos_geq(new_head, f_chunk_end(chunk)) {
                    if chunk_off == (*hdr).tail_chunk.load(Ordering::Acquire) {
                        break;
                    }
                    let next_off = chunk.next.load(Ordering::Acquire);
                    if next_off == 0 {
                        break;
                    }
                    (*hdr).head_chunk.store(next_off, Ordering::Release);
                    (*hdr).start_chunk.store(next_off, Ordering::Release);
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
        let visible_len = tail.wrapping_sub(chunk.start_byte);
        if chunk.start_byte != tail && chunk.length.load(Ordering::Relaxed) <= visible_len {
            chunk.start_byte = tail;
            chunk.length.store(0, Ordering::Relaxed);
        }
    }

    fn append_at_tail_without_tail_store(&self, offset: u32, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let chunk_data_size = (*hdr).min_alloc as usize;
            let mut written = 0usize;
            let mut remaining_offset = offset;
            let mut remaining_src = src;
            let mut chunk_off = (*hdr).tail_chunk.load(Ordering::Relaxed);

            while !remaining_src.is_empty() {
                if chunk_off == 0 {
                    let Some(new_off) = self.acquire_chunk(remaining_offset) else {
                        return written;
                    };
                    (*hdr).head_chunk.store(new_off, Ordering::Release);
                    (*hdr).tail_chunk.store(new_off, Ordering::Release);
                    (*hdr).start_chunk.store(new_off, Ordering::Release);
                    (*hdr).end_chunk.store(new_off, Ordering::Release);
                    chunk_off = new_off;
                }

                let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte.wrapping_add(chunk_data_size as u32);
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

                    let Some(new_off) = self.acquire_chunk(remaining_offset) else {
                        return written;
                    };
                    let new_chunk = &*self.base.add(new_off as usize).cast::<Chunk>();
                    new_chunk.next.store(next_off, Ordering::Relaxed);
                    chunk.next.store(new_off, Ordering::Release);
                    (*hdr).tail_chunk.store(new_off, Ordering::Release);
                    (*hdr).end_chunk.store(new_off, Ordering::Release);
                    chunk_off = new_off;
                    continue;
                }

                if !f_chunk_includes_pos(chunk, remaining_offset) && remaining_offset != chunk_end {
                    return written;
                }

                let data_off = remaining_offset.wrapping_sub(chunk.start_byte) as usize;
                let chunk_avail = chunk_data_size - data_off;
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

    fn write_at_without_tail_store(&self, offset: u32, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let chunk_data_size = (*hdr).min_alloc as usize;
            let mut written = 0usize;
            let mut remaining_offset = offset;
            let mut remaining_src = src;

            let mut chunk_off = (*hdr).tail_chunk.load(Ordering::Acquire);
            let mut prev_off = 0u64;

            // Seek to the chunk covering remaining_offset
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte.wrapping_add(chunk_data_size as u32);
                if f_chunk_includes_pos(chunk, remaining_offset) {
                    break;
                }
                if remaining_offset == chunk_end {
                    prev_off = chunk_off;
                    chunk_off = chunk.next.load(Ordering::Acquire);
                    continue;
                }
                prev_off = chunk_off;
                chunk_off = chunk.next.load(Ordering::Acquire);
            }

            // Write across chunks, taking preallocated chunks at the end.
            while !remaining_src.is_empty() {
                if chunk_off == 0 {
                    let Some(new_off) = self.acquire_chunk(remaining_offset) else {
                        return written;
                    };
                    if prev_off != 0 {
                        let prev = &mut *(self.base.add(prev_off as usize) as *mut Chunk);
                        if prev.next.load(Ordering::Acquire) == 0 {
                            prev.next.store(new_off, Ordering::Release);
                            (*hdr).end_chunk.store(new_off, Ordering::Release);
                        }
                    } else {
                        (*hdr).head_chunk.store(new_off, Ordering::Release);
                        (*hdr).tail_chunk.store(new_off, Ordering::Release);
                        (*hdr).start_chunk.store(new_off, Ordering::Release);
                        (*hdr).end_chunk.store(new_off, Ordering::Release);
                    }
                    chunk_off = new_off;
                }

                let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                if f_chunk_includes_pos(chunk, remaining_offset) {
                    let data_off = remaining_offset.wrapping_sub(chunk.start_byte) as usize;
                    let chunk_avail = chunk_data_size - data_off;
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

                prev_off = chunk_off;
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
        if len > self.max_enqueue() {
            return Err(FifoError::InsufficientCapacity {
                requested: len,
                available: self.max_enqueue(),
            });
        }
        unsafe {
            let tail = (*self.hdr).tail.load(Ordering::Relaxed);
            let new_tail = tail.wrapping_add(len as u32);
            (*self.hdr).tail.store(new_tail, Ordering::Release);
            let collected = self.promote_contiguous_from(new_tail);
            (*self.hdr)
                .tail
                .store(new_tail.wrapping_add(collected), Ordering::Release);
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
            (*self.hdr).signals.has_event.store(0, Ordering::Release);
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

    pub fn clear(&self) {
        let hdr = self.hdr;
        unsafe {
            let first_chunk = (*hdr).start_chunk.load(Ordering::Relaxed);
            assert_ne!(first_chunk, 0, "a valid FIFO always retains one chunk");
            let first = &mut *self.base.add(first_chunk as usize).cast::<Chunk>();
            first.next.store(0, Ordering::Relaxed);
            first.start_byte = 0;
            first.length.store((*hdr).min_alloc, Ordering::Relaxed);
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
    }

    pub fn is_empty(&self) -> bool {
        self.max_dequeue() == 0
    }

    pub fn is_full(&self) -> bool {
        self.max_enqueue() == 0
    }

    pub fn segment_fd(&self) -> Option<RawFd> {
        match &self.storage {
            FifoStorage::Segment(segment) => segment.shared_fd(),
            FifoStorage::Ssvm(segment) => segment.fd(),
        }
    }

    /// Reconstruct a [`Fifo`] from a shared-memory segment at the given
    /// header offset. The caller must guarantee that the segment contains a
    /// valid, initialised `FifoHeader` at `hdr_offset`.
    pub unsafe fn from_shared(seg: Segment, hdr_offset: u64) -> Self {
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_offset as usize) as *mut FifoHeader };
        Self {
            shr: hdr,
            fs_hdr: base.cast::<FifoSegmentHeader>(),
            ooo_enq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq_lookup: UnsafeCell::new(RbTree::with_capacity(4)),
            ooo_deq: std::ptr::null_mut(),
            ooo_enq: std::ptr::null_mut(),
            ooo_segments: UnsafeCell::new(Pool::with_capacity(4)),
            ooos_list_head: OOO_SEGMENT_INVALID_INDEX,
            ooos_newest: OOO_SEGMENT_INVALID_INDEX,
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
            storage: FifoStorage::Segment(seg),
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo_base: UnsafeCell::new(0),
        }
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
                current
                    .length
                    .store((*self.hdr).min_alloc, Ordering::Relaxed);
                start = start.wrapping_add((*self.hdr).min_alloc);
                chunk_off = current.next.load(Ordering::Acquire);
            }
        }
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
        self.dequeue_drop(copied)
    }

    #[inline]
    pub fn dequeue_drop_all(&self) -> usize {
        self.dequeue_drop(self.max_dequeue())
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

    pub fn first_ooo_segment(&self) -> Option<&OooSegment> {
        let entries = unsafe { &*self.ooo_segments.get() };
        entries.get(self.ooos_list_head)
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
        let available = self.max_enqueue();
        if offset as usize > available || src.len() > available.saturating_sub(offset as usize) {
            return Err(FifoError::OutOfOrderCapacityExceeded {
                end_offset: offset.wrapping_add(length),
                available,
            });
        }
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        let abs_start = tail.wrapping_add(offset);
        let written = self.write_at_without_tail_store(abs_start, src);
        if written != src.len() {
            return Err(FifoError::SegmentExhausted);
        }
        if offset == 0 {
            unsafe {
                (*self.hdr)
                    .tail
                    .store(tail.wrapping_add(length), Ordering::Release)
            };
            let delivered = self.promote_contiguous();
            return Ok(OooResult {
                accepted: length,
                delivered,
                start: Some(0),
                len: length.wrapping_add(delivered),
            });
        }

        let end = abs_start.wrapping_add(length);
        let old_segments = self.collect_ooo_segments();
        let mut intervals = old_segments;
        intervals.push((abs_start, end));
        intervals.sort_by(|left, right| {
            if left.0 == right.0 {
                core::cmp::Ordering::Equal
            } else if f_pos_lt(left.0, right.0) {
                core::cmp::Ordering::Less
            } else {
                core::cmp::Ordering::Greater
            }
        });
        let mut merged: Vec<(u32, u32)> = Vec::with_capacity(intervals.len());
        for (start, finish) in intervals {
            if let Some((_, last_finish)) = merged.last_mut() {
                if f_pos_leq(start, *last_finish) {
                    if f_pos_gt(finish, *last_finish) {
                        *last_finish = finish;
                    }
                    continue;
                }
            }
            merged.push((start, finish));
        }
        self.replace_ooo_segments(&merged);
        let accepted = length;
        Ok(OooResult {
            accepted,
            delivered: 0,
            start: Some(offset),
            len: length,
        })
    }

    fn collect_ooo_segments(&self) -> Vec<(u32, u32)> {
        let entries = unsafe { &*self.ooo_segments.get() };
        let mut segments = Vec::new();
        let mut index = self.ooos_list_head;
        while index != OOO_SEGMENT_INVALID_INDEX {
            if let Some(segment) = entries.get(index) {
                segments.push((segment.start, segment.start.wrapping_add(segment.length)));
                index = segment.next;
            } else {
                break;
            }
        }
        segments
    }

    fn replace_ooo_segments(&self, intervals: &[(u32, u32)]) {
        let entries = unsafe { &mut *self.ooo_segments.get() };
        let lookup = unsafe { &mut *self.ooo_enq_lookup.get() };
        while let Some(index) = entries.iter().next().map(|(index, _)| index) {
            let _ = entries.remove(index);
        }
        while let Some((key, _)) = lookup.first().map(|(key, value)| (*key, *value)) {
            let _ = lookup.remove(&key);
        }
        let fifo = self as *const Fifo as *mut Fifo;
        unsafe {
            (*fifo).ooos_list_head = OOO_SEGMENT_INVALID_INDEX;
            (*fifo).ooos_newest = OOO_SEGMENT_INVALID_INDEX;
        }
        let mut previous = OOO_SEGMENT_INVALID_INDEX;
        for &(start, finish) in intervals {
            let index = entries.insert(OooSegment {
                next: OOO_SEGMENT_INVALID_INDEX,
                prev: previous,
                start,
                length: finish.wrapping_sub(start),
            });
            if previous == OOO_SEGMENT_INVALID_INDEX {
                unsafe { (*fifo).ooos_list_head = index };
            } else if let Some(segment) = entries.get_mut(previous) {
                segment.next = index;
            }
            lookup.insert(start, index);
            previous = index;
            unsafe { (*fifo).ooos_newest = index };
        }
    }

    fn promote_contiguous_from(&self, base: u32) -> u32 {
        let mut tail = base;
        let mut delivered: u32 = 0;
        loop {
            let Some((start, finish)) = self.collect_ooo_segments().first().copied() else {
                break;
            };
            if start != tail && !f_pos_lt(start, tail) {
                break;
            }
            let advance = finish.wrapping_sub(tail);
            if advance == 0 {
                break;
            }
            tail = finish;
            delivered = delivered.wrapping_add(advance);
            let mut remaining = self.collect_ooo_segments();
            remaining.remove(0);
            self.replace_ooo_segments(&remaining);
        }
        unsafe { (*self.hdr).tail.store(tail, Ordering::Release) };
        delivered
    }

    pub fn promote_contiguous(&self) -> u32 {
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        self.promote_contiguous_from(tail)
    }

    pub fn ooo_head(&self) -> Option<(u32, u32)> {
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        let entries = unsafe { &*self.ooo_segments.get() };
        let segment = entries.get(self.ooos_list_head)?;
        Some((segment.start.wrapping_sub(tail), segment.length))
    }

    pub fn ooo_enqueued(&self) -> usize {
        unsafe { (&*self.ooo_segments.get()).len() }
    }
}

impl Fifo {
    /// Convenience constructor backed by a process-local Segment.
    pub fn with_capacity(capacity: usize) -> Result<Self, FifoError> {
        let bytes = Self::layout_bytes(capacity)?;
        let seg = Segment::local(bytes.saturating_add(256));
        Self::new(seg, capacity)
    }
}
