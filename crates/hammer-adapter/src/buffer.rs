use core::alloc::{GlobalAlloc, Layout};
use core::mem;
use core::ptr::{self, NonNull};
use core::slice;
use core::sync::atomic::{AtomicU64, Ordering};
use std::cell::{Cell, Ref, RefCell, RefMut};
use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::{
    align::align_up,
    heap::Heap,
    prefetch::{prefetch_read_l1, prefetch_read_l2, prefetch_write_l1},
    simd::movemask_4,
    vec::Vec,
};
use spinning_top::{
    RawRwSpinlock, RwSpinlock,
    lock_api::{MappedRwLockReadGuard, MappedRwLockWriteGuard},
    relax::Spin,
};

use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HandoffIndices};
use crate::instruction_set::{DataPlaneInstructionSet, FrameBatchWidth};
use crate::memory::{HAMMER_MAX_NUMA_NODES, StaticNumaTable};
use crate::node::{NodeEntry, NodeHandle, NodeId, NodeNext, NodeRuntime};
use crate::trace::{DataPlaneTrace, PacketTrace, TraceControlHandle};

pub const DEFAULT_BUFFER_FRAME_CAPACITY: usize = 256;
pub const DEFAULT_BUFFER_FRAME_POOL_SIZE: usize = 64;
pub const BUFFER_CACHE_LINE_SIZE: usize = 64;
pub const DEFAULT_PACKET_HEADROOM: usize = 256;
pub const DEFAULT_PRE_DATA_SIZE: usize = 128;
pub const BUFFER_INVALID_INDEX: u32 = u32::MAX;

/// Number of free slots moved between the per-thread cache and the arena free
/// list in a single batch. Batching amortises the `Rc<RefCell>` borrow across
/// this many alloc/free operations.
pub const BUFFER_THREAD_CACHE_BATCH: usize = 32;
/// High-water mark at which the thread cache returns a batch back to the
/// arena free list, preventing unbounded cache growth and keeping arena free
/// list non-empty for other consumers.
pub const BUFFER_THREAD_CACHE_HIGH_WATER: usize = 512;
/// `in_use` is folded from the lazy `in_use_delta` counter once its absolute
/// value exceeds this threshold or when the count is read.
pub const BUFFER_IN_USE_FOLD_THRESHOLD: i32 = 64;

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub union PrimaryOpaque {
    words64: [u64; 5],
    words32: [u32; 10],
    bytes: [u8; 40],
}

pub const PRIMARY_OPAQUE_BYTES: usize = mem::size_of::<PrimaryOpaque>();
pub const PRIMARY_OPAQUE_ALIGN: usize = mem::align_of::<PrimaryOpaque>();

impl PrimaryOpaque {
    #[inline]
    pub fn clear(&mut self) {
        *self = Self { words64: [0; 5] };
    }
}

impl Default for PrimaryOpaque {
    fn default() -> Self {
        Self { words64: [0; 5] }
    }
}

impl fmt::Debug for PrimaryOpaque {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let words64 = unsafe { self.words64 };
        f.debug_struct("PrimaryOpaque")
            .field("words64", &words64)
            .finish()
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub union SecondaryOpaque {
    words64: [u64; 7],
    words32: [u32; 14],
    bytes: [u8; 56],
}

impl SecondaryOpaque {
    #[inline]
    pub fn clear(&mut self) {
        *self = Self { words64: [0; 7] };
    }
}

impl Default for SecondaryOpaque {
    fn default() -> Self {
        Self { words64: [0; 7] }
    }
}

impl fmt::Debug for SecondaryOpaque {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let words64 = unsafe { self.words64 };
        f.debug_struct("SecondaryOpaque")
            .field("words64", &words64)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct BufferFlags(u32);

impl BufferFlags {
    const PUBLIC_MASK: u32 = (1 << 4) - 1;
    const PRIVATE_CAPACITY_SHIFT: u32 = 4;
    const PRIVATE_CAPACITY_MASK: u32 = !Self::PUBLIC_MASK;

    pub const NEXT_PRESENT: Self = Self(1 << 0);
    pub const TOTAL_LENGTH_VALID: Self = Self(1 << 1);
    pub const TRACED: Self = Self(1 << 2);
    /// Cacheline1 (trace_handle / total_length_not_including_first / opaque2)
    /// is known to be zeroed. Set by the free fast path and by the full reset
    /// routines; cleared by any mutator that dirties cacheline1. Lets the
    /// alloc fast path skip the second cacheline write when the slot was
    /// cleanly freed.
    pub const SLOT_CLEAN: Self = Self(1 << 3);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0 & Self::PUBLIC_MASK
    }

    #[inline]
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits & Self::PUBLIC_MASK)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.bits() & other.bits() == other.bits()
    }

    #[inline]
    pub fn insert(&mut self, other: Self) {
        self.0 = (self.0 & Self::PRIVATE_CAPACITY_MASK) | (self.bits() | other.bits());
    }

    #[inline]
    pub fn remove(&mut self, other: Self) {
        self.0 = (self.0 & Self::PRIVATE_CAPACITY_MASK) | (self.bits() & !other.bits());
    }

    #[inline]
    const fn with_private_data_capacity(self, data_capacity: usize) -> Self {
        let max_capacity = Self::max_private_data_capacity();
        let capped = if data_capacity > max_capacity {
            max_capacity
        } else {
            data_capacity
        };
        Self(self.bits() | ((capped as u32) << Self::PRIVATE_CAPACITY_SHIFT))
    }

    #[inline]
    const fn private_data_capacity(self) -> usize {
        ((self.0 & Self::PRIVATE_CAPACITY_MASK) >> Self::PRIVATE_CAPACITY_SHIFT) as usize
    }

    #[inline]
    const fn max_private_data_capacity() -> usize {
        (u32::MAX >> Self::PRIVATE_CAPACITY_SHIFT) as usize
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct BufferHeaderCacheline0 {
    pub current_data: i16,
    pub current_length: u16,
    pub flags: BufferFlags,
    pub flow_id: u32,
    pub ref_count: u8,
    pub buffer_pool_index: u8,
    pub error: u16,
    pub next_buffer: u32,
    pub current_config_or_punt: u32,
    pub opaque: PrimaryOpaque,
}

const _: () = assert!(core::mem::size_of::<BufferHeaderCacheline0>() == 64);
const _: () = assert!(core::mem::align_of::<BufferHeaderCacheline0>() == 64);

impl Default for BufferHeaderCacheline0 {
    fn default() -> Self {
        Self {
            current_data: 0,
            current_length: 0,
            flags: BufferFlags::empty(),
            flow_id: 0,
            ref_count: 0,
            buffer_pool_index: 0,
            error: 0,
            next_buffer: BUFFER_INVALID_INDEX,
            current_config_or_punt: 0,
            opaque: PrimaryOpaque::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct BufferHeaderCacheline1 {
    pub trace_handle: u32,
    pub total_length_not_including_first: u32,
    pub opaque2: SecondaryOpaque,
}

const _: () = assert!(core::mem::size_of::<BufferHeaderCacheline1>() == 64);
const _: () = assert!(core::mem::align_of::<BufferHeaderCacheline1>() == 64);

impl Default for BufferHeaderCacheline1 {
    fn default() -> Self {
        Self {
            trace_handle: 0,
            total_length_not_including_first: 0,
            opaque2: SecondaryOpaque::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BufferIndex {
    pool_id: u64,
    slot: u32,
    generation: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameIndex {
    pool_id: u64,
    slot: u32,
    generation: u32,
}

impl FrameIndex {
    pub fn pool_id(self) -> u64 {
        self.pool_id
    }

    pub fn slot(self) -> u32 {
        self.slot
    }

    pub fn generation(self) -> u32 {
        self.generation
    }
}

impl BufferIndex {
    pub fn pool_id(self) -> u64 {
        self.pool_id
    }

    pub fn slot(self) -> u32 {
        self.slot
    }

    pub fn generation(self) -> u32 {
        self.generation
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPacketCursor {
    pub(crate) packet_len: u32,
    pub(crate) network_header_offset: u16,
    pub(crate) network_header_len: u16,
    pub(crate) transport_header_offset: u16,
    pub(crate) transport_header_len: u16,
    pub(crate) transport_payload_offset: u16,
}

impl BufferPacketCursor {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn packet_len(self) -> usize {
        self.packet_len as usize
    }

    #[inline]
    pub fn network_header_offset(self) -> usize {
        self.network_header_offset as usize
    }

    #[inline]
    pub fn network_header_len(self) -> usize {
        usize::from(self.network_header_len)
    }

    #[inline]
    pub fn transport_header_offset(self) -> usize {
        usize::from(self.transport_header_offset)
    }

    #[inline]
    pub fn transport_header_len(self) -> usize {
        usize::from(self.transport_header_len)
    }

    #[inline]
    pub fn transport_payload_offset(self) -> usize {
        usize::from(self.transport_payload_offset)
    }

    #[inline]
    pub fn with_packet_len(mut self, packet_len: usize) -> Self {
        self.packet_len = u32::try_from(packet_len).expect("packet length exceeds u32");
        self
    }

    #[inline]
    pub fn with_network_header(mut self, offset: usize, len: usize) -> Self {
        self.network_header_offset =
            u16::try_from(offset).expect("network header offset exceeds u16");
        self.network_header_len = u16::try_from(len).expect("network header length exceeds u16");
        self
    }

    #[inline]
    pub fn with_transport_header(mut self, offset: usize, len: usize) -> Self {
        self.transport_header_offset =
            u16::try_from(offset).expect("transport header offset exceeds u16");
        self.transport_header_len =
            u16::try_from(len).expect("transport header length exceeds u16");
        self
    }

    #[inline]
    pub fn with_transport_payload_offset(mut self, offset: usize) -> Self {
        self.transport_payload_offset =
            u16::try_from(offset).expect("transport payload offset exceeds u16");
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferNodeError {
    node: NodeId,
    code: u16,
}

impl BufferNodeError {
    #[inline(always)]
    pub const fn new(node: NodeId, code: u16) -> Self {
        Self { node, code }
    }

    #[inline(always)]
    pub const fn node(self) -> NodeId {
        self.node
    }

    #[inline(always)]
    pub const fn code(self) -> u16 {
        self.code
    }
}

#[derive(Debug)]
#[repr(C, align(64))]
pub struct Buffer {
    cacheline0: BufferHeaderCacheline0,
    cacheline1: BufferHeaderCacheline1,
}

const _: () = assert!(mem::align_of::<Buffer>() == BUFFER_CACHE_LINE_SIZE);
const _: () = assert!(mem::size_of::<Buffer>() == BUFFER_CACHE_LINE_SIZE * 2);

#[inline]
pub const fn buffer_data_offset() -> usize {
    mem::size_of::<Buffer>() + DEFAULT_PRE_DATA_SIZE
}

impl Buffer {
    #[inline]
    fn reset(&mut self, data_size: usize, bytes: &[u8]) -> CoreResult<()> {
        if bytes.len() > data_size {
            return Err(CoreError::internal(format!(
                "buffer bytes exceed slot capacity: {} > {}",
                bytes.len(),
                data_size
            )));
        }
        let current_len = u16::try_from(bytes.len())
            .map_err(|_| CoreError::internal("buffer length exceeds u16"))?;
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.cacheline0.current_data = 0;
        self.cacheline0.current_length = current_len;
        self.cacheline0.ref_count = 1;
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
        self.cacheline1 = BufferHeaderCacheline1::default();
        self.data_region_mut(data_size)[..bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    #[inline]
    fn reset_for_free(&mut self, data_size: usize) {
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
        self.cacheline1 = BufferHeaderCacheline1::default();
    }

    #[inline]
    fn reset_empty(&mut self, data_size: usize, headroom: usize) -> CoreResult<()> {
        if headroom > data_size {
            return Err(CoreError::internal("buffer headroom exceeds slot capacity"));
        }
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.set_current_data_offset(isize::try_from(headroom).expect("headroom fits isize"))?;
        self.cacheline0.ref_count = 1;
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
        self.cacheline1 = BufferHeaderCacheline1::default();
        Ok(())
    }

    /// Clear only cacheline0 and mark the slot clean, leaving cacheline1 alone.
    /// Caller must have verified `flags.contains(SLOT_CLEAN)` so cacheline1 is
    /// already zeroed from the previous free. Returns the headroom/length pair
    /// the alloc fast path needs.
    #[inline]
    fn reset_empty_fast(&mut self, data_size: usize, headroom: usize) -> CoreResult<()> {
        if headroom > data_size {
            return Err(CoreError::internal("buffer headroom exceeds slot capacity"));
        }
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.set_current_data_offset(isize::try_from(headroom).expect("headroom fits isize"))?;
        self.cacheline0.ref_count = 1;
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
        Ok(())
    }

    /// Free fast path: only cacheline0 is rewritten (clean-default with
    /// SLOT_CLEAN set); cacheline1 is left untouched because it is already
    /// zeroed when SLOT_CLEAN was set on the slot.
    #[inline]
    fn reset_for_free_fast(&mut self, data_size: usize) {
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
    }

    #[inline]
    pub fn opaque(&self) -> &PrimaryOpaque {
        &self.cacheline0.opaque
    }

    #[inline]
    pub fn opaque_mut(&mut self) -> &mut PrimaryOpaque {
        &mut self.cacheline0.opaque
    }

    #[inline]
    pub fn opaque2(&self) -> &SecondaryOpaque {
        &self.cacheline1.opaque2
    }

    #[inline]
    pub fn opaque2_mut(&mut self) -> &mut SecondaryOpaque {
        self.cacheline0.flags.remove(BufferFlags::SLOT_CLEAN);
        &mut self.cacheline1.opaque2
    }

    #[inline]
    pub fn current_config(&self) -> crate::NodeId {
        crate::NodeId::new(self.cacheline0.current_config_or_punt)
    }

    #[inline]
    pub fn set_current_config(&mut self, next: crate::NodeId) {
        self.cacheline0.current_config_or_punt = next.slot();
    }

    #[inline]
    pub fn node_error_code(&self) -> Option<u16> {
        (self.cacheline0.error != 0).then_some(self.cacheline0.error)
    }

    #[inline]
    pub fn trace_handle(&self) -> Option<u32> {
        (self.cacheline1.trace_handle != 0).then_some(self.cacheline1.trace_handle)
    }

    #[inline]
    pub fn set_trace_handle(&mut self, handle: u32) {
        self.cacheline0.flags.remove(BufferFlags::SLOT_CLEAN);
        self.cacheline1.trace_handle = handle;
    }

    #[inline]
    pub fn take_trace_handle(&mut self) -> Option<u32> {
        let handle = self.trace_handle();
        self.cacheline1.trace_handle = 0;
        if handle.is_none() {
            // trace_handle was already 0; if the rest of cacheline1 is also
            // zeroed we keep CLEAN, otherwise it was already cleared.
        } else {
            // We cannot prove cacheline1 is fully zeroed anymore without a
            // scan; conservatively drop the clean invariant. A subsequent
            // free will rebuild it via the slow path.
            self.cacheline0.flags.remove(BufferFlags::SLOT_CLEAN);
        }
        handle
    }

    #[inline]
    pub fn set_node_error(&mut self, error: BufferNodeError) {
        self.cacheline0.error = error.code();
    }

    #[inline]
    pub fn clear_node_error(&mut self) {
        self.cacheline0.error = 0;
    }

    #[inline]
    pub fn flags(&self) -> BufferFlags {
        BufferFlags::from_bits(self.cacheline0.flags.bits())
    }

    #[inline]
    pub fn current_data_offset(&self) -> i16 {
        self.cacheline0.current_data
    }

    #[inline]
    pub fn current_data(&self) -> usize {
        usize::try_from(self.current_data_offset()).unwrap_or(0)
    }

    #[inline]
    pub fn ref_count(&self) -> u8 {
        self.cacheline0.ref_count
    }

    #[inline]
    pub fn current_len(&self) -> usize {
        usize::from(self.cacheline0.current_length)
    }

    #[inline]
    pub fn next_buffer_slot(&self) -> Option<u32> {
        self.flags()
            .contains(BufferFlags::NEXT_PRESENT)
            .then_some(self.cacheline0.next_buffer)
    }

    #[inline]
    pub fn total_len_not_including_first(&self) -> usize {
        self.cacheline1.total_length_not_including_first as usize
    }

    #[inline]
    pub fn current(&self) -> &[u8] {
        let len = self.current_len();
        // SAFETY: `current_ptr` is computed from the inline slot backing owned
        // by the arena, and `current_len` is maintained within slot bounds by
        // the pool mutation paths.
        unsafe { slice::from_raw_parts(self.current_ptr(), len) }
    }

    #[inline]
    pub fn current_ptr(&self) -> *const u8 {
        // SAFETY: the slot layout is `[Buffer][pre_data][data]`; the current
        // window is always kept within that inline backing.
        unsafe {
            self.as_bytes_ptr()
                .add(self.current_start_offset_from_header())
        }
    }

    #[inline]
    pub fn current_mut(&mut self) -> &mut [u8] {
        let len = self.current_len();
        // SAFETY: see `current`; the mutable borrow of `self` guarantees unique
        // access to the current window.
        unsafe {
            slice::from_raw_parts_mut(
                self.as_mut_bytes_ptr()
                    .add(self.current_start_offset_from_header()),
                len,
            )
        }
    }

    #[inline]
    pub fn writable_tail_mut(&mut self) -> &mut [u8] {
        let data_size = self.data_capacity();
        let start = self.current_end_offset_from_header();
        let len = self.available_tail_with_data_size(data_size);
        let writable_start = mem::size_of::<Buffer>();
        let offset = start - writable_start;
        &mut self.slot_writable_region_mut(data_size)[offset..offset + len]
    }

    #[inline]
    pub fn commit_writable_tail(&mut self, len: usize) -> CoreResult<()> {
        if len > self.available_tail_with_data_size(self.data_capacity()) {
            return Err(CoreError::internal("buffer commit exceeds writable tail"));
        }
        self.set_current_len(self.current_len() + len)?;
        Ok(())
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) -> CoreResult<()> {
        if len > self.current_len() {
            return Err(CoreError::internal(
                "buffer truncate extends current length",
            ));
        }
        self.set_current_len(len)
    }

    #[inline]
    pub fn advance(&mut self, displacement: isize) -> CoreResult<()> {
        if displacement == 0 {
            return Ok(());
        }
        if displacement < 0 {
            let rewind = displacement.unsigned_abs();
            if rewind > self.available_headroom() {
                return Err(CoreError::internal("buffer rewind exceeds headroom"));
            }
            let new_offset = isize::from(self.current_data_offset())
                - isize::try_from(rewind).expect("rewind fits isize");
            self.set_current_data_offset(new_offset)?;
            self.set_current_len(self.current_len() + rewind)?;
            return Ok(());
        }

        let len = usize::try_from(displacement)
            .map_err(|_| CoreError::internal("buffer advance displacement overflow"))?;
        if len > self.current_len() {
            return Err(CoreError::internal("buffer advance exceeds current length"));
        }
        let new_offset =
            isize::from(self.current_data_offset()) + isize::try_from(len).expect("len fits isize");
        self.set_current_data_offset(new_offset)?;
        self.set_current_len(self.current_len() - len)
    }

    #[inline]
    pub fn current_mut_ptr(&mut self) -> *mut u8 {
        // SAFETY: the slot layout is `[Buffer][pre_data][data]`; the current
        // window is always kept within that inline backing.
        unsafe {
            self.as_mut_bytes_ptr()
                .add(self.current_start_offset_from_header())
        }
    }

    #[inline]
    pub fn prepend(&mut self, bytes: &[u8]) -> CoreResult<()> {
        self.prepend_mut(bytes.len())?.copy_from_slice(bytes);
        Ok(())
    }

    #[inline]
    pub fn prepend_mut(&mut self, len: usize) -> CoreResult<&mut [u8]> {
        if len > self.available_headroom() {
            return Err(CoreError::internal("buffer prepend exceeds headroom"));
        }
        let current_start = self.current_start_offset_from_header();
        let start = current_start - len;
        let offset_from_data = isize::try_from(start).expect("slot offset fits isize")
            - isize::try_from(buffer_data_offset()).expect("buffer data offset fits isize");
        self.set_current_data_offset(offset_from_data)?;
        self.set_current_len(self.current_len() + len)?;
        // SAFETY: `start..current_start` lies within the inline pre_data/data
        // range and the mutable borrow of `self` guarantees uniqueness.
        unsafe {
            Ok(slice::from_raw_parts_mut(
                self.as_mut_bytes_ptr().add(start),
                len,
            ))
        }
    }

    #[inline]
    fn available_tail_with_data_size(&self, data_size: usize) -> usize {
        self.data_end_offset_from_header(data_size)
            .saturating_sub(self.current_end_offset_from_header())
    }

    #[inline]
    fn append_in_place(&mut self, data_size: usize, bytes: &[u8]) -> usize {
        let take = bytes
            .len()
            .min(self.available_tail_with_data_size(data_size));
        if take == 0 {
            return 0;
        }
        let start = self.current_end_offset_from_header();
        let end = start + take;
        let writable_start = mem::size_of::<Buffer>();
        self.slot_writable_region_mut(data_size)[start - writable_start..end - writable_start]
            .copy_from_slice(&bytes[..take]);
        self.set_current_len(self.current_len() + take)
            .expect("buffer append keeps current length within u16");
        take
    }

    #[inline]
    fn set_current_data_offset(&mut self, offset: isize) -> CoreResult<()> {
        let lower_bound =
            -isize::try_from(DEFAULT_PRE_DATA_SIZE).expect("default pre-data size fits isize");
        if offset < lower_bound {
            return Err(CoreError::internal(
                "buffer current_data exceeds pre-data headroom",
            ));
        }
        self.cacheline0.current_data = i16::try_from(offset)
            .map_err(|_| CoreError::internal("buffer current_data exceeds i16"))?;
        Ok(())
    }

    #[inline]
    fn set_current_len(&mut self, len: usize) -> CoreResult<()> {
        self.cacheline0.current_length = u16::try_from(len)
            .map_err(|_| CoreError::internal("buffer current_length exceeds u16"))?;
        Ok(())
    }

    #[inline]
    fn set_data_capacity(&mut self, data_size: usize) {
        self.cacheline0.flags = self.cacheline0.flags.with_private_data_capacity(data_size);
    }

    #[inline]
    fn data_capacity(&self) -> usize {
        self.cacheline0.flags.private_data_capacity()
    }

    #[inline]
    fn set_next_buffer(&mut self, next: Option<BufferIndex>) {
        self.cacheline0.next_buffer = next.map_or(BUFFER_INVALID_INDEX, BufferIndex::slot);
        if next.is_some() {
            self.cacheline0.flags.insert(BufferFlags::NEXT_PRESENT);
        } else {
            self.cacheline0.flags.remove(BufferFlags::NEXT_PRESENT);
        }
    }

    #[inline]
    fn set_total_len_not_including_first(&mut self, len: usize) -> CoreResult<()> {
        self.cacheline0.flags.remove(BufferFlags::SLOT_CLEAN);
        self.cacheline1.total_length_not_including_first = u32::try_from(len)
            .map_err(|_| CoreError::internal("buffer chain tail length exceeds u32"))?;
        Ok(())
    }

    #[inline]
    fn as_bytes_ptr(&self) -> *const u8 {
        ptr::from_ref(self).cast::<u8>()
    }

    #[inline]
    fn as_mut_bytes_ptr(&mut self) -> *mut u8 {
        ptr::from_mut(self).cast::<u8>()
    }

    #[inline]
    fn current_start_offset_from_header(&self) -> usize {
        let offset = isize::try_from(buffer_data_offset()).expect("buffer data offset fits isize")
            + isize::from(self.current_data_offset());
        usize::try_from(offset).expect("buffer current start underflowed header")
    }

    #[inline]
    fn current_end_offset_from_header(&self) -> usize {
        self.current_start_offset_from_header() + self.current_len()
    }

    #[inline]
    fn data_end_offset_from_header(&self, data_size: usize) -> usize {
        buffer_data_offset() + data_size
    }

    #[inline]
    fn slot_writable_end_offset_from_header(&self, data_size: usize) -> usize {
        mem::size_of::<Buffer>() + DEFAULT_PRE_DATA_SIZE + data_size
    }

    #[inline]
    fn available_headroom(&self) -> usize {
        self.current_start_offset_from_header()
            .saturating_sub(mem::size_of::<Buffer>())
    }

    #[inline]
    fn data_region_mut(&mut self, data_size: usize) -> &mut [u8] {
        // SAFETY: the inline data region begins at `buffer_data_offset()` and
        // spans exactly `data_size` bytes in the owning arena slot.
        unsafe {
            slice::from_raw_parts_mut(self.as_mut_bytes_ptr().add(buffer_data_offset()), data_size)
        }
    }

    #[inline]
    fn slot_writable_region_mut(&mut self, data_size: usize) -> &mut [u8] {
        // SAFETY: the writable inline slot backing begins immediately after the
        // header and spans the full `[pre_data][data]` capacity for the slot.
        unsafe {
            slice::from_raw_parts_mut(
                self.as_mut_bytes_ptr().add(mem::size_of::<Buffer>()),
                self.slot_writable_end_offset_from_header(data_size) - mem::size_of::<Buffer>(),
            )
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct BufferSlot {
    generation: u32,
    allocated: bool,
}

struct BufferPoolInner {
    pool_id: u64,
    slot_capacity: usize,
    slot_stride: usize,
    region: Arc<Heap>,
    region_base: usize,
    region_layout: Layout,
    region_size: usize,
    metadata_heap: Arc<Heap>,
    metadata_base: usize,
    metadata_layout: Layout,
    slot_states: usize,
    available_stack: usize,
    available_len: usize,
    total_slots: usize,
    in_use: usize,
    in_use_delta: i32,
}

impl fmt::Debug for BufferPoolInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPoolInner")
            .field("pool_id", &self.pool_id)
            .field("slot_capacity", &self.slot_capacity)
            .field("slot_stride", &self.slot_stride)
            .field("region_base", &self.region_base)
            .field("region_size", &self.region_size)
            .field("metadata_base", &self.metadata_base)
            .field("available_len", &self.available_len)
            .field("total_slots", &self.total_slots)
            .field("in_use", &self.in_use)
            .field("in_use_delta", &self.in_use_delta)
            .finish()
    }
}

impl Drop for BufferPoolInner {
    fn drop(&mut self) {
        // SAFETY: `region_base` came from `self.region.alloc(self.region_layout)`
        // during arena construction and has not been deallocated yet.
        unsafe {
            GlobalAlloc::dealloc(
                &*self.region,
                self.region_base as *mut u8,
                self.region_layout,
            )
        };
        // SAFETY: `metadata_base` came from `self.metadata_heap.alloc` during
        // arena construction and is paired with `metadata_layout`.
        unsafe {
            GlobalAlloc::dealloc(
                &*self.metadata_heap,
                self.metadata_base as *mut u8,
                self.metadata_layout,
            )
        };
    }
}

#[derive(Debug, Clone)]
pub struct BufferPoolArena {
    inner: Arc<RwSpinlock<BufferPoolInner>>,
}

type BufferMappedReadGuard<'a, T> = MappedRwLockReadGuard<'a, RawRwSpinlock<Spin>, T>;
type BufferMappedWriteGuard<'a, T> = MappedRwLockWriteGuard<'a, RawRwSpinlock<Spin>, T>;

#[derive(Debug)]
pub struct BufferRef<'a> {
    guard: BufferMappedReadGuard<'a, Buffer>,
}

impl Deref for BufferRef<'_> {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

#[derive(Debug)]
pub struct BufferRefMut<'a> {
    guard: BufferMappedWriteGuard<'a, Buffer>,
}

impl Deref for BufferRefMut<'_> {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for BufferRefMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

#[derive(Debug, Clone)]
pub struct BufferThreadCache {
    cached_slots: [u32; BUFFER_THREAD_CACHE_HIGH_WATER],
    len: usize,
}

impl BufferThreadCache {
    #[inline]
    fn new() -> Self {
        Self {
            cached_slots: [0; BUFFER_THREAD_CACHE_HIGH_WATER],
            len: 0,
        }
    }

    #[inline]
    pub fn cached_free_len(&self) -> usize {
        self.len
    }

    #[inline]
    fn push(&mut self, slot: u32) {
        debug_assert!(self.len < BUFFER_THREAD_CACHE_HIGH_WATER);
        self.cached_slots[self.len] = slot;
        self.len += 1;
    }

    #[inline]
    fn pop(&mut self) -> Option<u32> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(self.cached_slots[self.len])
    }

    #[inline]
    fn last(&self) -> Option<u32> {
        if self.len == 0 {
            return None;
        }
        Some(self.cached_slots[self.len - 1])
    }
}

#[derive(Debug)]
pub struct BufferPool {
    arena: BufferPoolArena,
    thread_cache: Rc<RefCell<BufferThreadCache>>,
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self {
            arena: self.arena.clone(),
            thread_cache: Rc::clone(&self.thread_cache),
        }
    }
}

#[derive(Debug)]
struct FrameSlot {
    generation: u32,
    allocated: bool,
    frame: Option<BufferFrame>,
}

#[derive(Debug)]
struct FramePoolInner {
    pool_id: u64,
    slots: Vec<FrameSlot>,
    available: Vec<u32>,
    in_use: usize,
}

#[derive(Debug, Clone)]
pub struct FramePool {
    inner: Rc<RefCell<FramePoolInner>>,
}

#[derive(Debug)]
pub struct PooledBufferFrame {
    index: FrameIndex,
    frame: BufferFrame,
}

#[derive(Clone)]
pub struct DataPlaneBuffers {
    buffer_pools: StaticNumaTable<BufferPool, HAMMER_MAX_NUMA_NODES>,
    active_numa_node: u32,
    thread_index: u32,
    frames: FramePool,
    frame_capacity: usize,
    frame_slots: usize,
    instruction_set: DataPlaneInstructionSet,
    trace: DataPlaneTrace,
}

#[derive(Debug)]
pub struct DataPlaneRuntime {
    buffers: DataPlaneBuffers,
    nodes: NodeRuntime,
    current_node: Rc<Cell<Option<NodeId>>>,
    handoff: Option<DataPlaneHandoffWorker>,
    handoff_node_handle: Option<NodeHandle>,
    active_numa_node: u32,
}

#[derive(Clone)]
struct DataPlaneRuntimeWorkerSeed {
    buffer_arenas: StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
    frame_capacity: usize,
    frame_slots: usize,
    instruction_set: DataPlaneInstructionSet,
    handoff: Option<DataPlaneHandoffWorker>,
    handoff_node_handle: Option<NodeHandle>,
}

impl fmt::Debug for DataPlaneBuffers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneBuffers")
            .field("active_numa_node", &self.active_numa_node)
            .field("thread_index", &self.thread_index)
            .field("frame_capacity", &self.frame_capacity)
            .field("frame_slots", &self.frame_slots)
            .field("instruction_set", &self.instruction_set)
            .finish()
    }
}

impl fmt::Debug for DataPlaneRuntimeWorkerSeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneRuntimeWorkerSeed")
            .field("frame_capacity", &self.frame_capacity)
            .field("frame_slots", &self.frame_slots)
            .field("instruction_set", &self.instruction_set)
            .field("handoff_node_handle", &self.handoff_node_handle)
            .finish()
    }
}

impl Clone for DataPlaneRuntime {
    fn clone(&self) -> Self {
        Self {
            buffers: self.buffers.clone(),
            nodes: self.nodes.clone(),
            current_node: Rc::clone(&self.current_node),
            handoff: self.handoff.clone(),
            handoff_node_handle: self.handoff_node_handle,
            active_numa_node: self.active_numa_node,
        }
    }
}

impl DataPlaneRuntimeWorkerSeed {
    #[inline]
    fn clone_for_worker(&self, thread_index: u32, numa_node: u32) -> DataPlaneRuntime {
        let mut runtime = DataPlaneRuntime::with_worker_buffer_arenas_and_frame_capacity(
            self.buffer_arenas.clone(),
            self.frame_capacity,
            self.frame_slots,
            self.instruction_set,
            thread_index,
            numa_node,
        );
        runtime.handoff = self.handoff.clone();
        runtime.handoff_node_handle = self.handoff_node_handle;
        if let Some(handoff) = runtime.handoff.clone() {
            runtime.buffers = runtime.buffers.with_handoff(handoff);
            runtime.active_numa_node = runtime.buffers.active_numa_node();
        }
        runtime
    }
}

static NEXT_BUFFER_POOL_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_FRAME_POOL_ID: AtomicU64 = AtomicU64::new(1);

#[inline]
fn next_buffer_pool_id() -> u64 {
    NEXT_BUFFER_POOL_ID.fetch_add(1, Ordering::Relaxed)
}

#[inline]
fn next_frame_pool_id() -> u64 {
    NEXT_FRAME_POOL_ID.fetch_add(1, Ordering::Relaxed)
}

impl DataPlaneBuffers {
    #[inline]
    pub fn with_numa_buffer_capacity(
        slot_capacity: usize,
        slots_per_numa: usize,
        numa_nodes: &[u32],
    ) -> Self {
        Self::with_numa_capacities_and_instruction_set(
            slot_capacity,
            slots_per_numa,
            DEFAULT_BUFFER_FRAME_CAPACITY,
            DEFAULT_BUFFER_FRAME_POOL_SIZE,
            numa_nodes,
            DataPlaneInstructionSet::native(),
        )
    }

    #[inline]
    pub fn with_buffer_capacity(slot_capacity: usize, slots: usize) -> Self {
        Self::with_capacities_and_instruction_set(
            slot_capacity,
            slots,
            DEFAULT_BUFFER_FRAME_CAPACITY,
            DEFAULT_BUFFER_FRAME_POOL_SIZE,
            DataPlaneInstructionSet::native(),
        )
    }

    #[inline]
    pub fn with_capacities(
        buffer_slot_capacity: usize,
        buffer_slots: usize,
        frame_capacity: usize,
        frame_slots: usize,
    ) -> Self {
        Self::with_capacities_and_instruction_set(
            buffer_slot_capacity,
            buffer_slots,
            frame_capacity,
            frame_slots,
            DataPlaneInstructionSet::native(),
        )
    }

    #[inline]
    pub fn with_buffer_capacity_and_instruction_set(
        slot_capacity: usize,
        slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        Self::with_capacities_and_instruction_set(
            slot_capacity,
            slots,
            DEFAULT_BUFFER_FRAME_CAPACITY,
            DEFAULT_BUFFER_FRAME_POOL_SIZE,
            instruction_set,
        )
    }

    #[inline]
    pub fn with_capacities_and_instruction_set(
        buffer_slot_capacity: usize,
        buffer_slots: usize,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        Self::with_buffer_arenas_and_frame_capacity(
            Self::single_numa_buffer_arenas(BufferPoolArena::with_capacity(
                buffer_slot_capacity,
                buffer_slots,
            )),
            frame_capacity,
            frame_slots,
            instruction_set,
            0,
            0,
        )
    }

    #[inline]
    pub fn with_buffer_arena_and_frame_capacity(
        buffer_arena: BufferPoolArena,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        Self::with_buffer_arenas_and_frame_capacity(
            Self::single_numa_buffer_arenas(buffer_arena),
            frame_capacity,
            frame_slots,
            instruction_set,
            0,
            0,
        )
    }

    #[inline]
    fn with_numa_capacities_and_instruction_set(
        buffer_slot_capacity: usize,
        buffer_slots_per_numa: usize,
        frame_capacity: usize,
        frame_slots: usize,
        numa_nodes: &[u32],
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        let mut buffer_arenas = StaticNumaTable::new();
        let configured_numa_nodes = if numa_nodes.is_empty() {
            &[0][..]
        } else {
            numa_nodes
        };
        for &numa_node in configured_numa_nodes {
            let heap = Arc::new(Heap::local(numa_node));
            buffer_arenas
                .insert(
                    numa_node,
                    BufferPoolArena::with_capacity_in(
                        buffer_slot_capacity,
                        buffer_slots_per_numa,
                        heap,
                    ),
                )
                .expect("NUMA node must fit static buffer arena table");
        }
        let active_numa_node = if let Some(&first_numa_node) = configured_numa_nodes.first() {
            first_numa_node
        } else {
            0
        };
        Self::with_buffer_arenas_and_frame_capacity(
            buffer_arenas,
            frame_capacity,
            frame_slots,
            instruction_set,
            0,
            active_numa_node,
        )
    }

    #[inline]
    fn with_buffer_arenas_and_frame_capacity(
        buffer_arenas: StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
        thread_index: u32,
        requested_numa_node: u32,
    ) -> Self {
        let active_numa_node = Self::resolve_numa_node(&buffer_arenas, requested_numa_node);
        Self {
            buffer_pools: Self::buffer_pools_from_arenas(&buffer_arenas),
            active_numa_node,
            thread_index,
            frames: FramePool::with_capacity(frame_capacity, frame_slots),
            frame_capacity,
            frame_slots,
            instruction_set,
            trace: DataPlaneTrace::default(),
        }
    }

    #[inline]
    fn with_worker_buffer_arenas_and_frame_capacity(
        buffer_arenas: StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
        thread_index: u32,
        requested_numa_node: u32,
    ) -> Self {
        let active_numa_node = Self::resolve_numa_node(&buffer_arenas, requested_numa_node);
        Self {
            buffer_pools: Self::worker_buffer_pools_from_arenas(&buffer_arenas),
            active_numa_node,
            thread_index,
            frames: FramePool::with_capacity(frame_capacity, frame_slots),
            frame_capacity,
            frame_slots,
            instruction_set,
            trace: DataPlaneTrace::default(),
        }
    }

    #[inline]
    pub fn clone_for_worker(&self, thread_index: u32, numa_node: u32) -> Self {
        Self::with_worker_buffer_arenas_and_frame_capacity(
            self.buffer_arenas(),
            self.frame_capacity,
            self.frame_slots,
            self.instruction_set,
            thread_index,
            numa_node,
        )
    }

    #[inline]
    fn with_handoff(mut self, handoff: DataPlaneHandoffWorker) -> Self {
        let Some(configured_arena) = handoff.configured_buffer_arena() else {
            return self;
        };
        let active_numa_node = self.active_numa_node;
        let mut buffer_pools = StaticNumaTable::new();
        for index in 0..HAMMER_MAX_NUMA_NODES {
            let numa_node = u32::try_from(index).expect("static NUMA node index fits in u32");
            let Some(pool) = self.buffer_pools.get(numa_node).cloned() else {
                continue;
            };
            let pool = if numa_node == active_numa_node {
                BufferPool::with_arena(configured_arena.clone())
            } else {
                pool
            };
            buffer_pools
                .insert(numa_node, pool)
                .expect("buffer pool NUMA node must fit static table");
        }
        self.buffer_pools = buffer_pools;
        self
    }

    #[inline]
    pub fn buffers(&self) -> &BufferPool {
        self.buffer_pools
            .get(self.active_numa_node)
            .expect("active NUMA buffer pool must exist")
    }

    #[inline]
    pub fn active_numa_node(&self) -> u32 {
        self.active_numa_node
    }

    #[inline]
    pub fn frames(&self) -> &FramePool {
        &self.frames
    }

    #[inline]
    pub fn instruction_set(&self) -> DataPlaneInstructionSet {
        self.instruction_set
    }

    #[inline]
    pub fn set_trace_control(&self, control: Option<TraceControlHandle>, packet_capacity: usize) {
        self.trace.set_control(control, packet_capacity);
    }

    #[inline]
    pub fn preferred_frame_batch_width(&self) -> FrameBatchWidth {
        self.instruction_set.preferred_frame_batch_width()
    }

    #[inline]
    pub fn in_use_buffers(&self) -> usize {
        self.buffers().in_use()
    }

    #[inline]
    pub fn cached_free_buffers(&self) -> usize {
        self.buffers().cached_free_len()
    }

    #[inline]
    pub fn frames_in_use(&self) -> usize {
        self.frames.in_use()
    }

    #[inline]
    pub fn alloc_index(&self) -> CoreResult<BufferIndex> {
        self.buffers().alloc_index()
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> CoreResult<BufferIndex> {
        self.buffers().alloc_index_with_bytes(bytes)
    }

    #[inline]
    pub fn free_index(&self, index: BufferIndex) {
        let buffers = self.buffers();
        let mut cache = buffers.thread_cache.borrow_mut();
        buffers
            .arena
            .inner
            .write()
            .free_chain_trace(&mut cache, index, |handle| self.trace.finalize(handle));
    }

    #[inline]
    pub fn attach_clone(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.buffers().attach_clone(head, tail)
    }

    #[inline]
    pub fn chain_buffer(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.buffers().chain_buffer(head, tail)
    }

    #[inline]
    pub fn prefetch_header(&self, index: BufferIndex) {
        self.buffers().prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: BufferIndex) {
        self.buffers().prefetch_read(index);
    }

    #[inline]
    pub fn prefetch_write(&self, index: BufferIndex) {
        self.buffers().prefetch_write(index);
    }

    #[inline]
    pub fn free_frame(&self, frame: &mut BufferFrame) {
        for index in frame.drain_indices() {
            self.free_index(index);
        }
    }

    #[inline]
    pub fn alloc_frame_index(&self) -> CoreResult<FrameIndex> {
        self.frames.alloc_index()
    }

    #[inline]
    pub fn with_frame<R>(
        &self,
        index: FrameIndex,
        f: impl FnOnce(&BufferFrame) -> R,
    ) -> CoreResult<R> {
        self.frames.with_frame(index, f)
    }

    #[inline]
    pub fn get_frame(&self, index: FrameIndex) -> CoreResult<FrameRef<'_>> {
        self.frames.get(index)
    }

    #[inline]
    pub fn get_frame_mut(&self, index: FrameIndex) -> CoreResult<FrameRefMut<'_>> {
        self.frames.get_mut(index)
    }

    #[inline]
    pub fn with_frame_mut<R>(
        &self,
        index: FrameIndex,
        f: impl FnOnce(&mut BufferFrame) -> R,
    ) -> CoreResult<R> {
        self.frames.with_frame_mut(index, f)
    }

    #[inline]
    pub fn free_frame_index(&self, index: FrameIndex) -> CoreResult<()> {
        let mut frame = self.frames.take_index(index)?;
        self.free_frame(&mut frame);
        self.frames.free_taken_index(self.buffers(), index, frame)
    }

    #[inline]
    pub fn alloc_pooled_frame(&self) -> CoreResult<PooledBufferFrame> {
        let index = self.frames.alloc_index()?;
        match self.frames.take_index(index) {
            Ok(frame) => Ok(PooledBufferFrame { index, frame }),
            Err(err) => {
                let _ = self.frames.free_index(self.buffers(), index);
                Err(err)
            }
        }
    }

    #[inline]
    pub fn release_pooled_frame(&self, frame: PooledBufferFrame) -> CoreResult<()> {
        let PooledBufferFrame { index, frame } = frame;
        let mut frame = frame;
        self.free_frame(&mut frame);
        self.frames.free_taken_index(self.buffers(), index, frame)
    }

    #[inline]
    pub(crate) fn return_pooled_frame_for_schedule(
        &self,
        frame: PooledBufferFrame,
    ) -> CoreResult<FrameIndex> {
        let PooledBufferFrame { index, frame } = frame;
        self.frames.return_taken_index(index, frame)?;
        Ok(index)
    }

    #[inline]
    pub(crate) fn take_frame_index(&self, index: FrameIndex) -> CoreResult<BufferFrame> {
        self.frames.take_index(index)
    }

    #[inline]
    pub(crate) fn return_taken_frame_index(
        &self,
        index: FrameIndex,
        frame: BufferFrame,
    ) -> CoreResult<()> {
        self.frames.return_taken_index(index, frame)
    }

    #[inline]
    pub(crate) fn release_taken_frame_index(
        &self,
        index: FrameIndex,
        frame: BufferFrame,
    ) -> CoreResult<()> {
        let mut frame = frame;
        self.free_frame(&mut frame);
        self.frames.free_taken_index(self.buffers(), index, frame)
    }

    #[inline]
    pub fn get_buffer(&self, index: BufferIndex) -> CoreResult<BufferRef<'_>> {
        self.buffers().get(index)
    }

    #[inline]
    pub fn get_buffer_mut(&self, index: BufferIndex) -> CoreResult<BufferRefMut<'_>> {
        self.buffers().get_mut(index)
    }

    #[inline]
    pub fn chain(
        &self,
        index: BufferIndex,
    ) -> impl Iterator<Item = CoreResult<BufferRef<'_>>> + '_ {
        self.buffers().chain(index)
    }

    #[inline]
    pub fn node_error_code(&self, index: BufferIndex) -> CoreResult<Option<u16>> {
        self.buffers().node_error_code(index)
    }

    #[inline]
    pub fn current_config(&self, index: BufferIndex) -> CoreResult<crate::NodeId> {
        self.buffers().current_config(index)
    }

    #[inline]
    pub fn set_current_config(&self, index: BufferIndex, next: crate::NodeId) -> CoreResult<()> {
        self.buffers().set_current_config(index, next)
    }

    #[inline]
    pub fn advance(&self, index: BufferIndex, displacement: isize) -> CoreResult<()> {
        self.buffers().advance(index, displacement)
    }

    #[inline]
    pub fn append(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        self.buffers().append(index, bytes)
    }

    #[inline]
    fn single_numa_buffer_arenas(
        buffer_arena: BufferPoolArena,
    ) -> StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES> {
        Self::single_numa_buffer_arenas_for_node(buffer_arena, 0)
    }

    #[inline]
    fn single_numa_buffer_arenas_for_node(
        buffer_arena: BufferPoolArena,
        numa_node: u32,
    ) -> StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES> {
        let mut buffer_arenas = StaticNumaTable::new();
        buffer_arenas
            .insert(numa_node, buffer_arena)
            .expect("NUMA node must fit static buffer arena table");
        buffer_arenas
    }

    #[inline]
    fn buffer_pools_from_arenas(
        buffer_arenas: &StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
    ) -> StaticNumaTable<BufferPool, HAMMER_MAX_NUMA_NODES> {
        let mut buffer_pools = StaticNumaTable::new();
        for index in 0..HAMMER_MAX_NUMA_NODES {
            let numa_node = u32::try_from(index).expect("static NUMA node index fits in u32");
            let Some(arena) = buffer_arenas.get(numa_node).cloned() else {
                continue;
            };
            buffer_pools
                .insert(numa_node, BufferPool::with_arena(arena))
                .expect("buffer pool NUMA node must fit static table");
        }
        buffer_pools
    }

    #[inline]
    fn worker_buffer_pools_from_arenas(
        buffer_arenas: &StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
    ) -> StaticNumaTable<BufferPool, HAMMER_MAX_NUMA_NODES> {
        let mut buffer_pools = StaticNumaTable::new();
        for index in 0..HAMMER_MAX_NUMA_NODES {
            let numa_node = u32::try_from(index).expect("static NUMA node index fits in u32");
            let Some(arena) = buffer_arenas.get(numa_node).cloned() else {
                continue;
            };
            buffer_pools
                .insert(numa_node, BufferPool::with_worker_arena(arena))
                .expect("buffer pool NUMA node must fit static table");
        }
        buffer_pools
    }

    #[inline]
    fn buffer_arenas(&self) -> StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES> {
        let mut buffer_arenas = StaticNumaTable::new();
        for index in 0..HAMMER_MAX_NUMA_NODES {
            let numa_node = u32::try_from(index).expect("static NUMA node index fits in u32");
            let Some(pool) = self.buffer_pools.get(numa_node) else {
                continue;
            };
            buffer_arenas
                .insert(numa_node, pool.arena())
                .expect("buffer arena NUMA node must fit static table");
        }
        buffer_arenas
    }

    #[inline]
    fn resolve_numa_node(
        buffer_arenas: &StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
        requested_numa_node: u32,
    ) -> u32 {
        if buffer_arenas.get(requested_numa_node).is_some() {
            return requested_numa_node;
        }
        if buffer_arenas.get(0).is_some() {
            return 0;
        }
        for index in 0..HAMMER_MAX_NUMA_NODES {
            let numa_node = u32::try_from(index).expect("static NUMA node index fits in u32");
            if buffer_arenas.get(numa_node).is_some() {
                return numa_node;
            }
        }
        0
    }
}

impl DataPlaneRuntime {
    #[inline]
    pub fn node_error(&self, index: BufferIndex) -> CoreResult<Option<BufferNodeError>> {
        let code = self.buffers.node_error_code(index)?;
        match code {
            Some(code) => self.nodes.decode_node_error(code),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn with_buffer_capacity(slot_capacity: usize, slots: usize) -> Self {
        Self::with_capacities_and_instruction_set(
            slot_capacity,
            slots,
            DEFAULT_BUFFER_FRAME_CAPACITY,
            DEFAULT_BUFFER_FRAME_POOL_SIZE,
            DataPlaneInstructionSet::native(),
        )
    }

    #[inline]
    pub fn with_numa_buffer_capacity(
        slot_capacity: usize,
        slots_per_numa: usize,
        numa_nodes: &[u32],
    ) -> Self {
        Self::with_numa_capacities_and_instruction_set(
            slot_capacity,
            slots_per_numa,
            DEFAULT_BUFFER_FRAME_CAPACITY,
            DEFAULT_BUFFER_FRAME_POOL_SIZE,
            numa_nodes,
            DataPlaneInstructionSet::native(),
        )
    }

    #[inline]
    pub fn with_capacities(
        buffer_slot_capacity: usize,
        buffer_slots: usize,
        frame_capacity: usize,
        frame_slots: usize,
    ) -> Self {
        Self::with_capacities_and_instruction_set(
            buffer_slot_capacity,
            buffer_slots,
            frame_capacity,
            frame_slots,
            DataPlaneInstructionSet::native(),
        )
    }

    #[inline]
    pub fn with_buffer_capacity_and_instruction_set(
        slot_capacity: usize,
        slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        Self::with_capacities_and_instruction_set(
            slot_capacity,
            slots,
            DEFAULT_BUFFER_FRAME_CAPACITY,
            DEFAULT_BUFFER_FRAME_POOL_SIZE,
            instruction_set,
        )
    }

    #[inline]
    pub fn with_capacities_and_instruction_set(
        buffer_slot_capacity: usize,
        buffer_slots: usize,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        Self::with_buffer_arenas_and_frame_capacity(
            DataPlaneBuffers::single_numa_buffer_arenas(BufferPoolArena::with_capacity(
                buffer_slot_capacity,
                buffer_slots,
            )),
            frame_capacity,
            frame_slots,
            instruction_set,
            0,
            0,
        )
    }

    #[inline]
    pub fn with_buffer_arena_and_frame_capacity(
        buffer_arena: BufferPoolArena,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        Self::with_buffer_arenas_and_frame_capacity(
            DataPlaneBuffers::single_numa_buffer_arenas(buffer_arena),
            frame_capacity,
            frame_slots,
            instruction_set,
            0,
            0,
        )
    }

    #[inline]
    fn with_numa_capacities_and_instruction_set(
        buffer_slot_capacity: usize,
        buffer_slots_per_numa: usize,
        frame_capacity: usize,
        frame_slots: usize,
        numa_nodes: &[u32],
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        let buffers = DataPlaneBuffers::with_numa_capacities_and_instruction_set(
            buffer_slot_capacity,
            buffer_slots_per_numa,
            frame_capacity,
            frame_slots,
            numa_nodes,
            instruction_set,
        );
        Self {
            active_numa_node: buffers.active_numa_node(),
            buffers,
            nodes: NodeRuntime::default(),
            current_node: Rc::new(Cell::new(None)),
            handoff: None,
            handoff_node_handle: None,
        }
    }

    #[inline]
    fn with_buffer_arenas_and_frame_capacity(
        buffer_arenas: StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
        thread_index: u32,
        requested_numa_node: u32,
    ) -> Self {
        let buffers = DataPlaneBuffers::with_buffer_arenas_and_frame_capacity(
            buffer_arenas,
            frame_capacity,
            frame_slots,
            instruction_set,
            thread_index,
            requested_numa_node,
        );
        Self {
            active_numa_node: buffers.active_numa_node(),
            buffers,
            nodes: NodeRuntime::default(),
            current_node: Rc::new(Cell::new(None)),
            handoff: None,
            handoff_node_handle: None,
        }
    }

    #[inline]
    fn with_worker_buffer_arenas_and_frame_capacity(
        buffer_arenas: StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
        thread_index: u32,
        requested_numa_node: u32,
    ) -> Self {
        let buffers = DataPlaneBuffers::with_worker_buffer_arenas_and_frame_capacity(
            buffer_arenas,
            frame_capacity,
            frame_slots,
            instruction_set,
            thread_index,
            requested_numa_node,
        );
        Self {
            active_numa_node: buffers.active_numa_node(),
            buffers,
            nodes: NodeRuntime::default(),
            current_node: Rc::new(Cell::new(None)),
            handoff: None,
            handoff_node_handle: None,
        }
    }

    #[inline]
    pub(crate) fn with_static_buffer_arena(
        arena: BufferPoolArena,
        frame_capacity: usize,
        frame_slots: usize,
        numa_node: u32,
    ) -> Self {
        Self::with_buffer_arenas_and_frame_capacity(
            DataPlaneBuffers::single_numa_buffer_arenas_for_node(arena, numa_node),
            frame_capacity,
            frame_slots,
            DataPlaneInstructionSet::native(),
            0,
            numa_node,
        )
    }

    #[inline]
    fn seed_for_worker(&self) -> DataPlaneRuntimeWorkerSeed {
        DataPlaneRuntimeWorkerSeed {
            buffer_arenas: self.buffers.buffer_arenas(),
            frame_capacity: self.buffers.frame_capacity,
            frame_slots: self.buffers.frame_slots,
            instruction_set: self.buffers.instruction_set,
            handoff: self.handoff.clone(),
            handoff_node_handle: self.handoff_node_handle,
        }
    }

    #[inline]
    pub fn clone_for_worker(&self, thread_index: u32, numa_node: u32) -> Self {
        self.seed_for_worker()
            .clone_for_worker(thread_index, numa_node)
    }

    #[inline]
    pub fn worker_seed(&self) -> impl Fn(u32, u32) -> DataPlaneRuntime + Send + 'static {
        let seed = self.seed_for_worker();
        move |thread_index, numa_node| seed.clone_for_worker(thread_index, numa_node)
    }

    #[inline]
    pub fn with_handoff(
        mut runtime: Self,
        worker: DataWorkerId,
        handoff: DataPlaneHandoffWorker,
    ) -> Self {
        debug_assert_eq!(worker, handoff.worker());
        runtime.buffers = runtime.buffers.with_handoff(handoff.clone());
        runtime.handoff = Some(handoff);
        runtime
    }

    #[inline]
    pub fn with_handoff_node_handle(mut self, handle: NodeHandle) -> Self {
        self.handoff_node_handle = Some(handle);
        self
    }

    #[inline]
    pub fn handoff_node_handle(&self) -> CoreResult<NodeHandle> {
        self.handoff_node_handle
            .ok_or_else(|| CoreError::internal("data plane handoff node handle is not configured"))
    }

    #[inline]
    pub fn active_numa_node(&self) -> u32 {
        self.active_numa_node
    }

    #[inline]
    pub fn with_handoff_capacities(
        worker: DataWorkerId,
        handoff: DataPlaneHandoffWorker,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        debug_assert_eq!(worker, handoff.worker());
        let arena = handoff.buffer_arena();
        let numa_node = arena.heap_numa_node();
        let runtime = Self::with_buffer_arenas_and_frame_capacity(
            DataPlaneBuffers::single_numa_buffer_arenas_for_node(arena, numa_node),
            frame_capacity,
            frame_slots,
            instruction_set,
            0,
            numa_node,
        );
        Self::with_handoff(runtime, worker, handoff)
    }

    #[inline]
    pub fn buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    #[inline]
    pub fn in_use_buffers(&self) -> usize {
        self.buffers.in_use_buffers()
    }

    #[inline]
    pub fn cached_free_buffers(&self) -> usize {
        self.buffers.cached_free_buffers()
    }

    #[inline]
    pub fn frames_in_use(&self) -> usize {
        self.buffers.frames_in_use()
    }

    #[inline]
    pub fn alloc_index(&self) -> CoreResult<BufferIndex> {
        self.buffers.alloc_index()
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> CoreResult<BufferIndex> {
        self.buffers.alloc_index_with_bytes(bytes)
    }

    #[inline]
    pub fn free_index(&self, index: BufferIndex) {
        self.buffers.free_index(index);
    }

    #[inline]
    pub fn prefetch_header(&self, index: BufferIndex) {
        self.buffers.prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: BufferIndex) {
        self.buffers.prefetch_read(index);
    }

    #[inline]
    pub fn prefetch_write(&self, index: BufferIndex) {
        self.buffers.prefetch_write(index);
    }

    #[inline]
    pub fn chain(
        &self,
        index: BufferIndex,
    ) -> impl Iterator<Item = CoreResult<BufferRef<'_>>> + '_ {
        self.buffers.chain(index)
    }

    #[inline]
    pub fn current_config(&self, index: BufferIndex) -> CoreResult<crate::NodeId> {
        self.buffers.current_config(index)
    }

    #[inline]
    pub fn free_frame(&self, frame: &mut BufferFrame) {
        self.buffers.free_frame(frame);
    }

    #[inline]
    pub fn alloc_frame_index(&self) -> CoreResult<FrameIndex> {
        self.buffers.alloc_frame_index()
    }

    #[inline]
    pub fn with_frame<R>(
        &self,
        index: FrameIndex,
        f: impl FnOnce(&BufferFrame) -> R,
    ) -> CoreResult<R> {
        self.buffers.with_frame(index, f)
    }

    #[inline]
    pub fn get_frame(&self, index: FrameIndex) -> CoreResult<FrameRef<'_>> {
        self.buffers.get_frame(index)
    }

    #[inline]
    pub fn get_frame_mut(&self, index: FrameIndex) -> CoreResult<FrameRefMut<'_>> {
        self.buffers.get_frame_mut(index)
    }

    #[inline]
    pub fn with_frame_mut<R>(
        &self,
        index: FrameIndex,
        f: impl FnOnce(&mut BufferFrame) -> R,
    ) -> CoreResult<R> {
        self.buffers.with_frame_mut(index, f)
    }

    #[inline]
    pub fn free_frame_index(&self, index: FrameIndex) -> CoreResult<()> {
        self.buffers.free_frame_index(index)
    }

    #[inline]
    pub fn alloc_pooled_frame(&self) -> CoreResult<PooledBufferFrame> {
        self.buffers.alloc_pooled_frame()
    }

    #[inline]
    pub fn release_pooled_frame(&self, frame: PooledBufferFrame) -> CoreResult<()> {
        self.buffers.release_pooled_frame(frame)
    }

    #[inline]
    pub fn get_buffer(&self, index: BufferIndex) -> CoreResult<BufferRef<'_>> {
        self.buffers.get_buffer(index)
    }

    #[inline]
    pub fn get_buffer_mut(&self, index: BufferIndex) -> CoreResult<BufferRefMut<'_>> {
        self.buffers.get_buffer_mut(index)
    }

    #[inline]
    pub fn nodes(&self) -> &NodeRuntime {
        &self.nodes
    }

    /// Initialize a packet graph: walk `entries`, call each `init`, then resolve
    /// named next-node edges. VPP `vlib_register_all_static_nodes` +
    /// `vlib_node_main_init`. Per-worker `worker` index is forwarded to each node.
    pub fn init_graph(&self, worker: usize, entries: &[NodeEntry]) -> CoreResult<()> {
        for entry in entries {
            (entry.init)(self, worker).map_err(|err| {
                CoreError::internal(format!(
                    "init graph node `{}`: {err}",
                    entry.registration.name().unwrap_or("?")
                ))
            })?;
        }
        self.nodes.resolve_named_next_nodes()
    }

    #[inline]
    pub fn set_trace_control(&self, control: Option<TraceControlHandle>, packet_capacity: usize) {
        self.buffers.set_trace_control(control, packet_capacity);
    }

    #[inline]
    pub fn node_by_name(&self, name: &str) -> Option<NodeId> {
        self.nodes.node_by_name(name)
    }

    #[inline]
    pub(crate) fn set_current_node(&self, node: Option<NodeId>) {
        self.current_node.set(node);
    }

    #[inline]
    pub fn current_node(&self) -> Option<NodeId> {
        self.current_node.get()
    }

    #[inline]
    pub fn may_mark_trace(&self, node: NodeId) -> bool {
        self.buffers.trace.may_mark(node)
    }

    #[inline]
    pub fn try_mark_trace(&self, node: NodeId, index: BufferIndex) -> CoreResult<()> {
        if !self.buffers.trace.may_mark(node) {
            return Ok(());
        }
        if self.get_buffer(index)?.trace_handle().is_some() {
            return Ok(());
        }
        let node_name = self.nodes.node_name(node)?;
        if let Some(handle) = self.buffers.trace.try_mark(node, node_name) {
            self.get_buffer_mut(index)?.set_trace_handle(handle);
        }
        Ok(())
    }

    #[inline]
    pub fn add_trace<T: PacketTrace>(&self, index: BufferIndex, trace: T) -> CoreResult<()> {
        let Some(node) = self.current_node() else {
            return Ok(());
        };
        let Some(handle) = self.get_buffer(index)?.trace_handle() else {
            return Ok(());
        };
        let node_name = self.nodes.node_name(node)?;
        let formatter = self.nodes.node_trace_formatter(node)?;
        let mut payload_bytes = hammer_infra::vec::Vec::new();
        trace.encode_trace(&mut payload_bytes);
        self.buffers
            .trace
            .add_entry(handle, node, node_name, formatter, payload_bytes);
        Ok(())
    }

    #[inline(always)]
    pub fn should_trace_packet(&self, index: BufferIndex) -> CoreResult<bool> {
        Ok(crate::unlikely(
            self.get_buffer(index)?.trace_handle().is_some(),
        ))
    }

    #[inline]
    pub fn current_node_next<K: NodeNext>(&self, key: K) -> CoreResult<NodeId> {
        let node = self
            .current_node()
            .ok_or_else(|| CoreError::internal("node next read outside node processing"))?;
        self.nodes.node_next(node, key)
    }

    #[inline]
    pub fn current_node_nexts<const COUNT: usize>(&self) -> CoreResult<[NodeId; COUNT]> {
        let node = self
            .current_node()
            .ok_or_else(|| CoreError::internal("node next read outside node processing"))?;
        self.nodes.node_nexts(node)
    }

    #[inline]
    pub fn record_current_node_error(&self, code: u16) -> CoreResult<u16> {
        let node = self
            .current_node()
            .ok_or_else(|| CoreError::internal("node error set outside node processing"))?;
        self.nodes.increment_node_error(node, code)
    }

    #[inline]
    pub fn node_error_count(&self, node: NodeId, code: u16) -> CoreResult<u64> {
        self.nodes.node_error_count(node, code)
    }

    pub fn snapshot_node_errors(&self, node: NodeId) -> Vec<(u16, u64)> {
        let mut out = Vec::new();
        for code in 1..256u16 {
            if let Ok(count) = self.node_error_count(node, code) {
                if count > 0 {
                    out.push((code, count));
                }
            }
        }
        out
    }

    #[inline]
    pub fn instruction_set(&self) -> DataPlaneInstructionSet {
        self.buffers.instruction_set()
    }

    #[inline]
    pub fn preferred_frame_batch_width(&self) -> FrameBatchWidth {
        self.buffers.preferred_frame_batch_width()
    }

    #[inline]
    pub fn schedule_frame(&self, node: NodeId, frame: FrameIndex) -> CoreResult<bool> {
        if !self.get_frame(frame)?.has_pending() {
            return Ok(false);
        }
        self.get_frame_mut(frame)?.set_next_node(node);
        self.nodes.schedule_frame(node, frame, false)?;
        Ok(true)
    }

    #[inline]
    pub(crate) fn schedule_pooled_frame(
        &self,
        node: NodeId,
        frame: PooledBufferFrame,
    ) -> CoreResult<()> {
        let frame_index = self.buffers.return_pooled_frame_for_schedule(frame)?;
        match self.schedule_frame(node, frame_index) {
            Ok(true) => Ok(()),
            Ok(false) => self.free_frame_index(frame_index),
            Err(err) => {
                let _ = self.free_frame_index(frame_index);
                Err(err)
            }
        }
    }

    #[inline]
    pub fn schedule_driver_frame(&self, node: NodeId, frame: FrameIndex) -> CoreResult<()> {
        self.get_frame_mut(frame)?.set_next_node(node);
        self.nodes.schedule_frame(node, frame, true)
    }

    #[inline]
    pub fn schedule_empty_frame(&self, node: NodeId) -> CoreResult<()> {
        let frame = self.alloc_frame_index()?;
        if let Err(err) = self
            .get_frame_mut(frame)
            .map(|mut frame_ref| frame_ref.set_next_node(node))
        {
            let _ = self.free_frame_index(frame);
            return Err(err);
        }
        if let Err(err) = self.nodes.schedule_frame(node, frame, true) {
            let _ = self.free_frame_index(frame);
            return Err(err);
        }
        Ok(())
    }

    #[inline]
    pub fn schedule_polling_driver_nodes(&self) -> CoreResult<usize> {
        let nodes = self.nodes.polling_driver_nodes()?;
        let scheduled = nodes.len();
        for node in nodes {
            self.schedule_empty_frame(node)?;
        }
        Ok(scheduled)
    }

    #[inline]
    pub fn set_node_interrupt_pending(&self, node: NodeId) -> CoreResult<bool> {
        if !self.nodes.mark_interrupt_pending(node)? {
            return Ok(false);
        }
        if let Err(err) = self.schedule_empty_frame(node) {
            let _ = self.nodes.clear_interrupt_pending(node);
            return Err(err);
        }
        Ok(true)
    }

    #[inline]
    pub fn run_ready_nodes(&self) -> CoreResult<usize> {
        self.drain_handoff_frames()?;
        self.nodes.run_ready_function_nodes(self)
    }

    #[inline]
    fn drain_handoff_frames(&self) -> CoreResult<()> {
        let Some(handoff) = &self.handoff else {
            return Ok(());
        };
        while let Some(handoff_frame) = handoff.pop() {
            let node = match self.nodes.node_for_handle(handoff_frame.target) {
                Ok(node) => node,
                Err(err) => {
                    self.free_handoff_indices(handoff_frame.indices);
                    return Err(err);
                }
            };
            let frame = match self.alloc_frame_index() {
                Ok(frame) => frame,
                Err(err) => {
                    self.free_handoff_indices(handoff_frame.indices);
                    return Err(err);
                }
            };
            {
                let mut frame_ref = self.get_frame_mut(frame)?;
                if let Err(err) = self.push_handoff_indices(&mut frame_ref, handoff_frame.indices) {
                    drop(frame_ref);
                    let _ = self.free_frame_index(frame);
                    return Err(err);
                }
            }
            if !self.schedule_frame(node, frame)? {
                self.free_frame_index(frame)?;
            }
        }
        Ok(())
    }

    #[inline]
    fn push_handoff_indices(
        &self,
        frame: &mut FrameRefMut<'_>,
        indices: HandoffIndices,
    ) -> CoreResult<()> {
        match indices {
            HandoffIndices::Single(index) => {
                if let Err(err) = frame.push_index(index) {
                    self.free_index(index);
                    return Err(err);
                }
            }
            HandoffIndices::Batch(indices) => {
                if let Err(err) = frame.push_indices(indices.iter().copied()) {
                    for index in indices {
                        self.free_index(index);
                    }
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn free_handoff_indices(&self, indices: HandoffIndices) {
        match indices {
            HandoffIndices::Single(index) => self.free_index(index),
            HandoffIndices::Batch(indices) => {
                for index in indices {
                    self.free_index(index);
                }
            }
        }
    }

    #[inline]
    pub fn handoff_frame(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        frame: &mut BufferFrame,
    ) -> CoreResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(CoreError::internal("data plane handoff is not configured"));
        };
        handoff.ensure_enqueue_capacity(worker)?;
        let indices = frame.drain_pending().collect::<Vec<_>>();
        if indices.is_empty() {
            return Ok(());
        }
        handoff.enqueue(worker, target, indices)
    }

    #[inline]
    pub fn handoff_indices(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        indices: impl IntoIterator<Item = BufferIndex>,
    ) -> CoreResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(CoreError::internal("data plane handoff is not configured"));
        };
        handoff.ensure_enqueue_capacity(worker)?;
        let indices = indices.into_iter().collect::<Vec<_>>();
        if indices.is_empty() {
            return Ok(());
        }
        handoff.enqueue(worker, target, indices)
    }

    #[inline]
    pub fn handoff_index(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(CoreError::internal("data plane handoff is not configured"));
        };
        handoff.ensure_enqueue_capacity(worker)?;
        handoff.enqueue_index(worker, target, index)
    }

    #[inline]
    pub(crate) fn take_frame_index(&self, index: FrameIndex) -> CoreResult<BufferFrame> {
        self.buffers.take_frame_index(index)
    }

    #[inline]
    pub(crate) fn return_taken_frame_index(
        &self,
        index: FrameIndex,
        frame: BufferFrame,
    ) -> CoreResult<()> {
        self.buffers.return_taken_frame_index(index, frame)
    }

    #[inline]
    pub(crate) fn release_taken_frame_index(
        &self,
        index: FrameIndex,
        frame: BufferFrame,
    ) -> CoreResult<()> {
        self.buffers.release_taken_frame_index(index, frame)
    }
}

impl BufferPoolArena {
    #[inline]
    pub fn with_capacity(slot_capacity: usize, slots: usize) -> Self {
        Self::with_capacity_in(slot_capacity, slots, Arc::new(Heap::local(0)))
    }

    #[inline]
    pub fn with_capacity_in(slot_capacity: usize, slots: usize, heap: Arc<Heap>) -> Self {
        assert!(slot_capacity > 0, "buffer slot capacity must be non-zero");
        assert!(
            slots > 0,
            "buffer pool must contain at least one usable slot"
        );

        let total_slots = slots
            .checked_add(1)
            .expect("buffer arena slot count overflow");
        let slot_stride = align_up(
            buffer_data_offset()
                .checked_add(slot_capacity)
                .expect("buffer slot size overflow"),
            BUFFER_CACHE_LINE_SIZE,
        );
        let region_size = slot_stride
            .checked_mul(total_slots)
            .expect("buffer arena size overflow");
        let region_layout = Layout::from_size_align(region_size, BUFFER_CACHE_LINE_SIZE)
            .expect("buffer arena layout");
        let region_base = heap
            .alloc(region_layout)
            .expect("buffer arena heap allocation");
        unsafe {
            ptr::write_bytes(region_base.as_ptr(), 0, region_layout.size());
        }

        let slot_state_bytes = mem::size_of::<BufferSlot>()
            .checked_mul(total_slots)
            .expect("buffer metadata slot state overflow");
        let available_bytes = mem::size_of::<u32>()
            .checked_mul(slots)
            .expect("buffer metadata availability overflow");
        let available_offset = align_up(slot_state_bytes, mem::align_of::<u32>());
        let metadata_size = available_offset
            .checked_add(available_bytes)
            .expect("buffer metadata size overflow");
        let metadata_layout = Layout::from_size_align(metadata_size, BUFFER_CACHE_LINE_SIZE)
            .expect("buffer metadata layout");
        let metadata_base = heap
            .alloc(metadata_layout)
            .expect("buffer metadata heap allocation");
        unsafe {
            ptr::write_bytes(metadata_base.as_ptr(), 0, metadata_layout.size());
        }
        let slot_states = metadata_base.cast::<BufferSlot>();
        let available_stack = unsafe {
            NonNull::new_unchecked(metadata_base.as_ptr().add(available_offset).cast::<u32>())
        };
        for i in 0..slots {
            let slot = u32::try_from(total_slots - i - 1).expect("buffer slot fits u32");
            unsafe { available_stack.as_ptr().add(i).write(slot) };
        }

        Self {
            inner: Arc::new(RwSpinlock::new(BufferPoolInner {
                pool_id: next_buffer_pool_id(),
                slot_capacity,
                slot_stride,
                region: Arc::clone(&heap),
                region_base: region_base.as_ptr() as usize,
                region_layout,
                region_size,
                metadata_heap: heap,
                metadata_base: metadata_base.as_ptr() as usize,
                metadata_layout,
                slot_states: slot_states.as_ptr() as usize,
                available_stack: available_stack.as_ptr() as usize,
                available_len: slots,
                total_slots,
                in_use: 0,
                in_use_delta: 0,
            })),
        }
    }

    #[inline]
    pub fn pool_id(&self) -> u64 {
        self.inner.read().pool_id
    }

    #[inline]
    pub fn heap_numa_node(&self) -> u32 {
        self.inner.read().region.numa_node()
    }
}

impl BufferPool {
    #[inline]
    pub fn with_capacity(slot_capacity: usize, slots: usize) -> Self {
        Self::with_arena(BufferPoolArena::with_capacity(slot_capacity, slots))
    }

    #[inline]
    pub fn with_arena(arena: BufferPoolArena) -> Self {
        Self {
            arena,
            thread_cache: Rc::new(RefCell::new(BufferThreadCache::new())),
        }
    }

    #[inline]
    fn with_worker_arena(arena: BufferPoolArena) -> Self {
        Self {
            arena,
            thread_cache: Rc::new(RefCell::new(BufferThreadCache::new())),
        }
    }

    #[inline]
    pub fn arena(&self) -> BufferPoolArena {
        self.arena.clone()
    }

    #[inline]
    pub fn pool_id(&self) -> u64 {
        self.arena.pool_id()
    }

    #[inline]
    pub fn heap_numa_node(&self) -> u32 {
        self.arena.heap_numa_node()
    }

    #[inline]
    pub fn slot_stride(&self) -> usize {
        self.arena.inner.read().slot_stride
    }

    #[inline]
    pub fn base_ptr(&self) -> *const u8 {
        self.arena.inner.read().region_base as *const u8
    }

    #[inline]
    pub fn buffer_raw_ptr(&self, slot: u32) -> *const Buffer {
        self.arena
            .inner
            .read()
            .buffer_raw_ptr(slot)
            .expect("buffer slot must be in bounds")
            .cast_const()
    }

    #[inline]
    pub fn data_raw_ptr(&self, slot: u32) -> *const u8 {
        self.arena
            .inner
            .read()
            .data_raw_ptr(slot)
            .expect("buffer slot must be in bounds")
            .cast_const()
    }

    #[inline]
    pub fn cached_free_len(&self) -> usize {
        self.thread_cache.borrow().cached_free_len()
    }

    #[inline]
    pub fn in_use(&self) -> usize {
        let mut arena = self.arena.inner.write();
        arena.fold_in_use();
        arena.in_use
    }

    #[inline]
    pub fn alloc_index(&self) -> CoreResult<BufferIndex> {
        let mut cache = self.thread_cache.borrow_mut();
        let mut arena = self.arena.inner.write();
        arena.alloc_empty_chain(&mut cache)
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> CoreResult<BufferIndex> {
        let mut cache = self.thread_cache.borrow_mut();
        let mut arena = self.arena.inner.write();
        arena.alloc_chain(&mut cache, bytes)
    }

    #[inline]
    pub fn free_index(&self, index: BufferIndex) {
        let mut cache = self.thread_cache.borrow_mut();
        self.arena.inner.write().free_chain(&mut cache, index);
    }

    #[inline]
    pub fn attach_clone(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.arena.inner.write().attach_clone(head, tail)
    }

    #[inline]
    pub fn chain_buffer(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.arena.inner.write().chain_buffer(head, tail)
    }

    #[inline]
    pub fn prefetch_header(&self, index: BufferIndex) {
        self.arena.inner.read().prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: BufferIndex) {
        self.arena.inner.read().prefetch_read(index);
    }

    #[inline]
    pub fn prefetch_write(&self, index: BufferIndex) {
        self.arena.inner.read().prefetch_write(index);
    }

    #[inline]
    pub fn free_frame(&self, frame: &mut BufferFrame) {
        let mut cache = self.thread_cache.borrow_mut();
        let mut pool = self.arena.inner.write();
        for index in frame.drain_indices() {
            pool.free_chain(&mut cache, index);
        }
        pool.fold_in_use();
    }

    #[inline]
    pub fn get(&self, index: BufferIndex) -> CoreResult<BufferRef<'_>> {
        let guard = self.arena.inner.read();
        guard.buffer(index)?;
        Ok(BufferRef {
            guard: spinning_top::guard::RwSpinlockReadGuard::map(guard, |pool| {
                pool.buffer(index)
                    .expect("buffer index was validated before mapping")
            }),
        })
    }

    #[inline]
    pub fn get_mut(&self, index: BufferIndex) -> CoreResult<BufferRefMut<'_>> {
        let mut guard = self.arena.inner.write();
        guard.ensure_writable(index)?;
        guard.buffer_mut(index)?;
        Ok(BufferRefMut {
            guard: spinning_top::guard::RwSpinlockWriteGuard::map(guard, |pool| {
                pool.buffer_mut(index)
                    .expect("buffer index was validated before mapping")
            }),
        })
    }

    #[inline]
    fn next_buffer(&self, index: BufferIndex) -> CoreResult<Option<BufferIndex>> {
        self.arena.inner.read().next_buffer(index)
    }

    #[inline]
    pub fn chain(
        &self,
        index: BufferIndex,
    ) -> impl Iterator<Item = CoreResult<BufferRef<'_>>> + '_ {
        let mut next = Some(index);
        let mut failed = false;
        std::iter::from_fn(move || {
            if failed {
                return None;
            }
            let current = next?;
            let guard = self.arena.inner.read();
            next = match guard.next_buffer(current) {
                Ok(next) => next,
                Err(err) => {
                    failed = true;
                    return Some(Err(err));
                }
            };
            Some(Ok(BufferRef {
                guard: spinning_top::guard::RwSpinlockReadGuard::map(guard, |pool| {
                    pool.buffer(current)
                        .expect("buffer index was validated before mapping")
                }),
            }))
        })
    }

    #[inline]
    pub fn current_data(&self, index: BufferIndex) -> CoreResult<usize> {
        Ok(self.arena.inner.read().buffer(index)?.current_data())
    }

    #[inline]
    pub fn current_len(&self, index: BufferIndex) -> CoreResult<usize> {
        Ok(self.arena.inner.read().buffer(index)?.current_len())
    }

    #[inline]
    pub fn current_ptr(&self, index: BufferIndex) -> CoreResult<*const u8> {
        Ok(self.arena.inner.read().buffer(index)?.current_ptr())
    }

    #[inline]
    pub fn current_mut_ptr(&self, index: BufferIndex) -> CoreResult<*mut u8> {
        let mut guard = self.arena.inner.write();
        guard.ensure_writable(index)?;
        Ok(guard.buffer_mut(index)?.current_mut_ptr())
    }

    #[inline]
    pub fn current_config(&self, index: BufferIndex) -> CoreResult<crate::NodeId> {
        Ok(self.arena.inner.read().buffer(index)?.current_config())
    }

    #[inline]
    pub fn set_current_config(&self, index: BufferIndex, next: crate::NodeId) -> CoreResult<()> {
        let mut guard = self.arena.inner.write();
        guard.ensure_header_exclusive(index)?;
        guard.buffer_mut(index)?.set_current_config(next);
        Ok(())
    }

    #[inline]
    pub fn node_error_code(&self, index: BufferIndex) -> CoreResult<Option<u16>> {
        Ok(self.arena.inner.read().buffer(index)?.node_error_code())
    }

    #[inline]
    pub fn advance(&self, index: BufferIndex, displacement: isize) -> CoreResult<()> {
        let mut pool = self.arena.inner.write();
        pool.advance(index, displacement)
    }

    #[inline]
    pub fn truncate_current(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        let mut pool = self.arena.inner.write();
        pool.ensure_writable(index)?;

        let mut walked = 0usize;
        let mut current = Some(index);
        let mut cut_buffer: Option<BufferIndex> = None;
        let mut cut_remainder = 0usize;
        while let Some(current_index) = current {
            let current_len = pool.buffer(current_index)?.current_len();
            if walked + current_len >= len {
                cut_buffer = Some(current_index);
                cut_remainder = len - walked;
                break;
            }
            walked += current_len;
            current = pool.next_buffer(current_index)?;
        }

        let cut_buffer = cut_buffer
            .ok_or_else(|| CoreError::internal("buffer truncate extends current length"))?;
        if cut_buffer != index {
            pool.ensure_header_exclusive(cut_buffer)?;
        }

        let head_current_len = pool.buffer(index)?.current_len();
        let head_had_next = pool.next_buffer(index)?.is_some();

        pool.buffer_mut(cut_buffer)?
            .set_current_len(cut_remainder)?;

        if cut_buffer == index {
            if head_had_next {
                pool.buffer_mut(index)?.set_next_buffer(None);
                pool.buffer_mut(index)?
                    .set_total_len_not_including_first(0)?;
            }
        } else {
            pool.buffer_mut(cut_buffer)?.set_next_buffer(None);
            let new_total_tail = len - head_current_len;
            pool.buffer_mut(index)?
                .set_total_len_not_including_first(new_total_tail)?;
        }
        Ok(())
    }

    #[inline]
    pub fn prepend(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        let mut pool = self.arena.inner.write();
        pool.ensure_writable(index)?;
        pool.buffer_mut(index)?.prepend(bytes)
    }

    #[inline]
    pub fn append(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        let mut cache = self.thread_cache.borrow_mut();
        self.arena
            .inner
            .write()
            .append_chain(&mut cache, index, bytes)
    }
}

impl FramePool {
    #[inline]
    pub fn with_capacity(frame_capacity: usize, slots: usize) -> Self {
        let available = (0..slots)
            .rev()
            .map(|slot| u32::try_from(slot).expect("frame slot index fits u32"))
            .collect();
        let slots = (0..slots)
            .map(|_| FrameSlot {
                generation: 0,
                allocated: false,
                frame: Some(BufferFrame::with_capacity(frame_capacity)),
            })
            .collect();
        Self {
            inner: Rc::new(RefCell::new(FramePoolInner {
                pool_id: next_frame_pool_id(),
                slots,
                available,
                in_use: 0,
            })),
        }
    }

    #[inline]
    pub fn in_use(&self) -> usize {
        self.inner.borrow().in_use
    }

    #[inline]
    pub fn alloc_index(&self) -> CoreResult<FrameIndex> {
        self.inner.borrow_mut().alloc_index()
    }

    #[inline]
    pub fn with_frame<R>(
        &self,
        index: FrameIndex,
        f: impl FnOnce(&BufferFrame) -> R,
    ) -> CoreResult<R> {
        let pool = self.inner.borrow();
        let frame = pool.frame(index)?;
        Ok(f(frame))
    }

    #[inline]
    pub fn get(&self, index: FrameIndex) -> CoreResult<FrameRef<'_>> {
        let guard = self.inner.borrow();
        guard.frame(index)?;
        Ok(FrameRef { guard, index })
    }

    #[inline]
    pub fn get_mut(&self, index: FrameIndex) -> CoreResult<FrameRefMut<'_>> {
        let mut guard = self.inner.borrow_mut();
        guard.frame_mut(index)?;
        Ok(FrameRefMut { guard, index })
    }

    #[inline]
    pub fn with_frame_mut<R>(
        &self,
        index: FrameIndex,
        f: impl FnOnce(&mut BufferFrame) -> R,
    ) -> CoreResult<R> {
        let mut pool = self.inner.borrow_mut();
        let frame = pool.frame_mut(index)?;
        Ok(f(frame))
    }

    #[inline]
    pub fn free_index(&self, buffers: &BufferPool, index: FrameIndex) -> CoreResult<()> {
        let mut pool = self.inner.borrow_mut();
        let frame = pool.frame_mut(index)?;
        buffers.free_frame(frame);
        frame.reset_for_pool_reuse();
        pool.release_index(index)
    }

    #[inline]
    pub fn take_index(&self, index: FrameIndex) -> CoreResult<BufferFrame> {
        self.inner.borrow_mut().take_frame(index)
    }

    #[inline]
    pub fn free_taken_index(
        &self,
        buffers: &BufferPool,
        index: FrameIndex,
        mut frame: BufferFrame,
    ) -> CoreResult<()> {
        buffers.free_frame(&mut frame);
        frame.reset_for_pool_reuse();
        self.inner
            .borrow_mut()
            .return_frame_and_release(index, frame)
    }

    #[inline]
    pub fn return_taken_index(&self, index: FrameIndex, frame: BufferFrame) -> CoreResult<()> {
        self.inner.borrow_mut().return_frame(index, frame)
    }
}

impl FramePoolInner {
    #[inline]
    fn alloc_index(&mut self) -> CoreResult<FrameIndex> {
        let slot = self
            .available
            .pop()
            .ok_or_else(|| CoreError::internal("frame pool exhausted"))?;
        let entry = self
            .slots
            .get_mut(slot as usize)
            .ok_or_else(|| CoreError::internal("frame slot out of bounds"))?;
        entry.generation = entry.generation.wrapping_add(1).max(1);
        entry.allocated = true;
        let frame = entry
            .frame
            .as_mut()
            .ok_or_else(|| CoreError::internal("frame slot is checked out"))?;
        frame.reset_for_pool_reuse();
        self.in_use += 1;
        Ok(FrameIndex {
            pool_id: self.pool_id,
            slot,
            generation: entry.generation,
        })
    }

    #[inline]
    fn validate_index(&self, index: FrameIndex) -> CoreResult<()> {
        if index.pool_id != self.pool_id {
            return Err(CoreError::internal("frame index belongs to another pool"));
        }
        Ok(())
    }

    #[inline]
    fn frame(&self, index: FrameIndex) -> CoreResult<&BufferFrame> {
        self.validate_index(index)?;
        let entry = self
            .slots
            .get(index.slot as usize)
            .ok_or_else(|| CoreError::internal("frame slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale frame index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("frame slot is free"));
        }
        entry
            .frame
            .as_ref()
            .ok_or_else(|| CoreError::internal("frame slot is checked out"))
    }

    #[inline]
    fn frame_mut(&mut self, index: FrameIndex) -> CoreResult<&mut BufferFrame> {
        self.validate_index(index)?;
        let entry = self
            .slots
            .get_mut(index.slot as usize)
            .ok_or_else(|| CoreError::internal("frame slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale frame index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("frame slot is free"));
        }
        entry
            .frame
            .as_mut()
            .ok_or_else(|| CoreError::internal("frame slot is checked out"))
    }

    #[inline]
    fn take_frame(&mut self, index: FrameIndex) -> CoreResult<BufferFrame> {
        self.validate_index(index)?;
        let entry = self
            .slots
            .get_mut(index.slot as usize)
            .ok_or_else(|| CoreError::internal("frame slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale frame index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("frame slot is free"));
        }
        entry
            .frame
            .take()
            .ok_or_else(|| CoreError::internal("frame slot is checked out"))
    }

    #[inline]
    fn release_index(&mut self, index: FrameIndex) -> CoreResult<()> {
        let entry = self
            .slots
            .get_mut(index.slot as usize)
            .ok_or_else(|| CoreError::internal("frame slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale frame index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("frame slot is free"));
        }
        if entry.frame.is_none() {
            return Err(CoreError::internal("frame slot is checked out"));
        }
        entry.allocated = false;
        self.in_use = self.in_use.saturating_sub(1);
        self.available.push(index.slot);
        Ok(())
    }

    #[inline]
    fn return_frame_and_release(
        &mut self,
        index: FrameIndex,
        frame: BufferFrame,
    ) -> CoreResult<()> {
        self.validate_index(index)?;
        let entry = self
            .slots
            .get_mut(index.slot as usize)
            .ok_or_else(|| CoreError::internal("frame slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale frame index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("frame slot is free"));
        }
        if entry.frame.is_some() {
            return Err(CoreError::internal("frame slot already has a frame"));
        }
        entry.frame = Some(frame);
        entry.allocated = false;
        self.in_use = self.in_use.saturating_sub(1);
        self.available.push(index.slot);
        Ok(())
    }

    #[inline]
    fn return_frame(&mut self, index: FrameIndex, frame: BufferFrame) -> CoreResult<()> {
        self.validate_index(index)?;
        let entry = self
            .slots
            .get_mut(index.slot as usize)
            .ok_or_else(|| CoreError::internal("frame slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale frame index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("frame slot is free"));
        }
        if entry.frame.is_some() {
            return Err(CoreError::internal("frame slot already has a frame"));
        }
        entry.frame = Some(frame);
        Ok(())
    }
}

impl PooledBufferFrame {
    #[inline]
    pub fn index(&self) -> FrameIndex {
        self.index
    }
}

impl Deref for PooledBufferFrame {
    type Target = BufferFrame;

    fn deref(&self) -> &Self::Target {
        &self.frame
    }
}

impl DerefMut for PooledBufferFrame {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frame
    }
}

impl BufferPoolInner {
    #[inline]
    fn slot_index(&self, slot: u32) -> CoreResult<usize> {
        let slot = usize::try_from(slot).expect("buffer slot index fits usize");
        if slot >= self.total_slots {
            return Err(CoreError::internal("buffer slot out of bounds"));
        }
        Ok(slot)
    }

    #[inline]
    fn slot_offset(&self, slot: u32) -> CoreResult<usize> {
        let slot = self.slot_index(slot)?;
        slot.checked_mul(self.slot_stride)
            .ok_or_else(|| CoreError::internal("buffer slot offset overflow"))
    }

    #[inline]
    fn slot_state(&self, slot: u32) -> CoreResult<&BufferSlot> {
        let slot = self.slot_index(slot)?;
        // SAFETY: `slot_states` points to `total_slots` contiguous `BufferSlot`
        // records allocated during arena construction.
        Ok(unsafe { &*((self.slot_states as *const BufferSlot).add(slot)) })
    }

    #[inline]
    fn slot_state_mut(&mut self, slot: u32) -> CoreResult<&mut BufferSlot> {
        let slot = self.slot_index(slot)?;
        // SAFETY: the mutable borrow of `self` guarantees unique access to the
        // raw metadata block.
        Ok(unsafe { &mut *((self.slot_states as *mut BufferSlot).add(slot)) })
    }

    #[inline]
    fn pop_available_slot(&mut self) -> Option<u32> {
        if self.available_len == 0 {
            return None;
        }
        self.available_len -= 1;
        // SAFETY: `available_len` always tracks initialized stack entries in
        // `available_stack`.
        Some(unsafe {
            (self.available_stack as *const u32)
                .add(self.available_len)
                .read()
        })
    }

    #[inline]
    fn push_available_slot(&mut self, slot: u32) {
        debug_assert_ne!(slot, 0);
        debug_assert!(self.available_len < self.total_slots - 1);
        // SAFETY: `available_len < total_slots - 1` and slot 0 is never
        // inserted, so the raw availability stack has spare initialized
        // capacity.
        unsafe {
            (self.available_stack as *mut u32)
                .add(self.available_len)
                .write(slot);
        }
        self.available_len += 1;
    }

    #[inline]
    fn buffer_raw_ptr(&self, slot: u32) -> CoreResult<*mut Buffer> {
        let offset = self.slot_offset(slot)?;
        // SAFETY: `offset` is validated to land within the arena region and
        // each slot begins with an inline `Buffer` header.
        Ok(unsafe { (self.region_base as *mut u8).add(offset).cast::<Buffer>() })
    }

    #[inline]
    fn data_raw_ptr(&self, slot: u32) -> CoreResult<*mut u8> {
        let offset = self
            .slot_offset(slot)?
            .checked_add(buffer_data_offset())
            .ok_or_else(|| CoreError::internal("buffer data pointer overflow"))?;
        // SAFETY: `offset` points at the inline data block within the validated
        // arena slot.
        Ok(unsafe { (self.region_base as *mut u8).add(offset) })
    }

    #[inline]
    fn buffer_at_slot(&self, slot: u32) -> CoreResult<&Buffer> {
        let ptr = self.buffer_raw_ptr(slot)?;
        // SAFETY: the slot layout guarantees that `ptr` addresses a live inline
        // `Buffer` header for the lifetime of `&self`.
        Ok(unsafe { &*ptr })
    }

    #[inline]
    fn buffer_at_slot_mut(&mut self, slot: u32) -> CoreResult<&mut Buffer> {
        let ptr = self.buffer_raw_ptr(slot)?;
        // SAFETY: the mutable borrow of `self` guarantees unique access to the
        // slot's inline `Buffer` header.
        Ok(unsafe { &mut *ptr })
    }

    #[inline]
    fn index_from_slot(&self, slot: u32) -> Option<BufferIndex> {
        Some(BufferIndex {
            pool_id: self.pool_id,
            slot,
            generation: self.next_buffer_generation(slot)?,
        })
    }

    #[inline]
    fn next_buffer_generation(&self, slot: u32) -> Option<u32> {
        let entry = self.slot_state(slot).ok()?;
        entry.allocated.then_some(entry.generation)
    }

    #[inline]
    fn next_buffer(&self, index: BufferIndex) -> CoreResult<Option<BufferIndex>> {
        Ok(self
            .buffer(index)?
            .next_buffer_slot()
            .and_then(|slot| self.index_from_slot(slot)))
    }

    #[inline]
    fn advance(&mut self, index: BufferIndex, displacement: isize) -> CoreResult<()> {
        if displacement == 0 {
            return Ok(());
        }

        if displacement < 0 {
            let rewind = displacement.unsigned_abs();
            let buffer = self.buffer(index)?;
            if rewind > buffer.available_headroom() {
                return Err(CoreError::internal("buffer rewind exceeds headroom"));
            }
            self.ensure_header_exclusive(index)?;
            let buffer = self.buffer_mut(index)?;
            let new_offset = isize::from(buffer.current_data_offset())
                - isize::try_from(rewind).expect("rewind fits isize");
            buffer.set_current_data_offset(new_offset)?;
            buffer.set_current_len(buffer.current_len() + rewind)?;
            return Ok(());
        }

        let len = usize::try_from(displacement)
            .map_err(|_| CoreError::internal("buffer advance displacement overflow"))?;

        let first = self.buffer(index)?;
        if self.next_buffer(index)?.is_none() {
            if len > first.current_len() {
                return Err(CoreError::internal("buffer advance exceeds current length"));
            }
            self.ensure_header_exclusive(index)?;
            let buffer = self.buffer_mut(index)?;
            let new_offset = isize::from(buffer.current_data_offset())
                + isize::try_from(len).expect("len fits isize");
            buffer.set_current_data_offset(new_offset)?;
            buffer.set_current_len(buffer.current_len() - len)?;
            return Ok(());
        }

        let original_total_len = first
            .current_len()
            .checked_add(first.total_len_not_including_first())
            .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?;
        if len > original_total_len {
            return Err(CoreError::internal("buffer advance exceeds current length"));
        }

        let mut touched = Vec::new();
        let mut remaining = len;
        let mut current = Some(index);
        while remaining != 0 {
            let current_index = current
                .ok_or_else(|| CoreError::internal("buffer chain advance lost current segment"))?;
            let buffer = self.buffer(current_index)?;
            touched.push(current_index);
            if remaining <= buffer.current_len() {
                break;
            }
            remaining -= buffer.current_len();
            current = self.next_buffer(current_index)?;
        }

        for current_index in touched.iter().copied() {
            self.ensure_header_exclusive(current_index)?;
        }

        let mut remaining = len;
        for current_index in touched.iter().copied() {
            let buffer = self.buffer_mut(current_index)?;
            let consume = remaining.min(buffer.current_len());
            let new_offset = isize::from(buffer.current_data_offset())
                + isize::try_from(consume).expect("consume fits isize");
            buffer.set_current_data_offset(new_offset)?;
            buffer.set_current_len(buffer.current_len() - consume)?;
            remaining -= consume;
            if remaining == 0 {
                break;
            }
        }

        let remaining_total_len = original_total_len
            .checked_sub(len)
            .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?;
        let first_current_len = self.buffer(index)?.current_len();
        let tail_len = remaining_total_len
            .checked_sub(first_current_len)
            .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?;
        self.buffer_mut(index)?
            .set_total_len_not_including_first(tail_len)
    }

    #[inline]
    fn alloc_chain(
        &mut self,
        cache: &mut BufferThreadCache,
        bytes: &[u8],
    ) -> CoreResult<BufferIndex> {
        if self.slot_capacity == 0 {
            return Err(CoreError::internal("buffer slot capacity must be nonzero"));
        }
        if bytes.len() <= self.slot_capacity {
            return self.alloc_slot(cache, bytes);
        }

        let first_len = self.slot_capacity;
        let first = self.alloc_slot(cache, &bytes[..first_len])?;
        let mut tail = first;
        let mut offset = first_len;
        let mut total_tail_len = 0usize;

        while offset < bytes.len() {
            let end = (offset + self.slot_capacity).min(bytes.len());
            let next = self.alloc_slot(cache, &bytes[offset..end])?;
            {
                let tail_buffer = self.buffer_mut(tail)?;
                tail_buffer.set_next_buffer(Some(next));
            }
            total_tail_len += end - offset;
            tail = next;
            offset = end;
        }
        self.buffer_mut(first)?
            .set_total_len_not_including_first(total_tail_len)?;
        Ok(first)
    }

    #[inline]
    fn alloc_empty_chain(&mut self, cache: &mut BufferThreadCache) -> CoreResult<BufferIndex> {
        if self.slot_capacity == 0 {
            return Err(CoreError::internal("buffer slot capacity must be nonzero"));
        }
        self.alloc_slot_empty_fast(cache, 0)
    }

    #[inline]
    fn alloc_slot(
        &mut self,
        cache: &mut BufferThreadCache,
        bytes: &[u8],
    ) -> CoreResult<BufferIndex> {
        if bytes.len() > self.slot_capacity {
            return Err(CoreError::internal(format!(
                "buffer bytes exceed slot capacity: {} > {}",
                bytes.len(),
                self.slot_capacity
            )));
        }
        self.alloc_slot_with(cache, |buffer, data_size| buffer.reset(data_size, bytes))
    }

    #[inline]
    fn alloc_slot_with(
        &mut self,
        cache: &mut BufferThreadCache,
        reset: impl FnOnce(&mut Buffer, usize) -> CoreResult<()>,
    ) -> CoreResult<BufferIndex> {
        let slot = match cache.pop() {
            Some(slot) => slot,
            None => {
                self.refill_cache_batch(cache);
                cache
                    .pop()
                    .ok_or_else(|| CoreError::internal("buffer pool exhausted"))?
            }
        };
        let generation = {
            let entry = self.slot_state_mut(slot)?;
            entry.generation = entry.generation.wrapping_add(1).max(1);
            entry.generation
        };
        let reset_result = {
            let data_size = self.slot_capacity;
            let buffer = self.buffer_at_slot_mut(slot)?;
            reset(buffer, data_size)
        };
        if let Err(error) = reset_result {
            self.slot_state_mut(slot)
                .expect("buffer slot metadata remains valid")
                .allocated = false;
            cache.push(slot);
            return Err(error);
        }
        self.slot_state_mut(slot)
            .expect("buffer slot metadata remains valid")
            .allocated = true;
        self.bump_in_use();
        self.prefetch_next_cached_slot(cache);
        Ok(BufferIndex {
            pool_id: self.pool_id,
            slot,
            generation,
        })
    }

    /// Empty-buffer alloc fast path: only cacheline0 is rewritten (clean-default
    /// with SLOT_CLEAN set); cacheline1 is left alone when the slot was cleanly
    /// freed, otherwise the slow path zeros it.
    #[inline(always)]
    fn alloc_slot_empty_fast(
        &mut self,
        cache: &mut BufferThreadCache,
        headroom: usize,
    ) -> CoreResult<BufferIndex> {
        let slot = match cache.pop() {
            Some(slot) => slot,
            None => {
                self.refill_cache_batch(cache);
                cache
                    .pop()
                    .ok_or_else(|| CoreError::internal("buffer pool exhausted"))?
            }
        };
        let generation = {
            let entry = self.slot_state_mut(slot)?;
            entry.generation = entry.generation.wrapping_add(1).max(1);
            entry.generation
        };
        let clean = self
            .buffer_at_slot(slot)?
            .cacheline0
            .flags
            .contains(BufferFlags::SLOT_CLEAN);
        let reset_result = {
            let data_size = self.slot_capacity;
            let buffer = self.buffer_at_slot_mut(slot)?;
            if clean {
                buffer.reset_empty_fast(data_size, headroom)
            } else {
                buffer.reset_empty(data_size, headroom)
            }
        };
        if let Err(error) = reset_result {
            self.slot_state_mut(slot)
                .expect("buffer slot metadata remains valid")
                .allocated = false;
            cache.push(slot);
            return Err(error);
        }
        self.slot_state_mut(slot)
            .expect("buffer slot metadata remains valid")
            .allocated = true;
        self.bump_in_use();
        self.prefetch_next_cached_slot(cache);
        Ok(BufferIndex {
            pool_id: self.pool_id,
            slot,
            generation,
        })
    }

    #[inline]
    fn validate_pool_index(&self, index: BufferIndex) -> CoreResult<()> {
        if index.pool_id != self.pool_id {
            return Err(CoreError::internal("buffer index belongs to another pool"));
        }
        Ok(())
    }

    #[inline]
    fn buffer(&self, index: BufferIndex) -> CoreResult<&Buffer> {
        self.validate_pool_index(index)?;
        let entry = self.slot_state(index.slot)?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale buffer index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("buffer slot is free"));
        }
        self.buffer_at_slot(index.slot)
    }

    #[inline]
    fn buffer_mut(&mut self, index: BufferIndex) -> CoreResult<&mut Buffer> {
        self.validate_pool_index(index)?;
        {
            let entry = self.slot_state(index.slot)?;
            if entry.generation != index.generation {
                return Err(CoreError::internal("stale buffer index"));
            }
            if !entry.allocated {
                return Err(CoreError::internal("buffer slot is free"));
            }
        }
        self.buffer_at_slot_mut(index.slot)
    }

    #[inline]
    fn ensure_header_exclusive(&self, index: BufferIndex) -> CoreResult<()> {
        let buffer = self.buffer(index)?;
        if buffer.ref_count() == 1 {
            return Ok(());
        }
        Err(CoreError::internal(
            "shared buffer requires exclusive header ownership",
        ))
    }

    #[inline]
    fn ensure_writable(&self, index: BufferIndex) -> CoreResult<()> {
        self.ensure_header_exclusive(index)
    }

    #[inline]
    fn prefetch_header(&self, index: BufferIndex) {
        let Ok(buffer) = self.buffer(index) else {
            return;
        };
        prefetch_buffer_header(buffer);
    }

    #[inline]
    fn prefetch_read(&self, index: BufferIndex) {
        let Ok(buffer) = self.buffer(index) else {
            return;
        };
        prefetch_buffer_header(buffer);
        prefetch_buffer_cacheline1(buffer);
        prefetch_buffer_data(buffer);
    }

    #[inline]
    fn prefetch_write(&self, index: BufferIndex) {
        let Ok(buffer) = self.buffer(index) else {
            return;
        };
        prefetch_buffer_header_write(buffer);
        prefetch_buffer_cacheline1_write(buffer);
        prefetch_buffer_data_write(buffer);
    }

    #[inline]
    fn free_chain(&mut self, cache: &mut BufferThreadCache, index: BufferIndex) {
        self.free_chain_trace(cache, index, |_| {});
    }

    #[inline]
    fn free_chain_trace(
        &mut self,
        cache: &mut BufferThreadCache,
        index: BufferIndex,
        mut release_trace: impl FnMut(u32),
    ) {
        let mut next = Some(index);
        while let Some(index) = next {
            if index.pool_id != self.pool_id {
                return;
            }
            let slot = index.slot;
            let (next_slot, ref_count, clean, trace_handle) = {
                let Ok(entry) = self.slot_state(slot) else {
                    return;
                };
                if entry.generation != index.generation {
                    return;
                }
                if !entry.allocated {
                    next = None;
                    continue;
                }
                let Ok(buffer) = self.buffer_at_slot(index.slot) else {
                    return;
                };
                (
                    buffer.next_buffer_slot(),
                    buffer.ref_count(),
                    buffer.cacheline0.flags.contains(BufferFlags::SLOT_CLEAN),
                    buffer.cacheline1.trace_handle,
                )
            };

            if ref_count > 1 {
                if let Ok(buffer) = self.buffer_at_slot_mut(index.slot) {
                    buffer.cacheline0.ref_count = buffer.cacheline0.ref_count.saturating_sub(1);
                }
                next = next_slot.and_then(|slot| self.index_from_slot(slot));
                continue;
            }

            // Fast path: trace_handle is provably zero when SLOT_CLEAN holds,
            // so no trace finalisation is needed and cacheline1 is already
            // zeroed, letting us skip the second cacheline write on free.
            if clean {
                let slot_capacity = self.slot_capacity;
                {
                    let buffer = self
                        .buffer_at_slot_mut(index.slot)
                        .expect("buffer slot remains valid");
                    buffer.reset_for_free_fast(slot_capacity);
                }
                {
                    let entry = self
                        .slot_state_mut(slot)
                        .expect("buffer slot metadata remains valid");
                    entry.allocated = false;
                }
                self.dec_in_use();
                self.push_cache_slot(cache, index.slot);
                if let Some(next_slot) = next_slot {
                    self.prefetch_chain_next(next_slot);
                }
                next = next_slot.and_then(|slot| self.index_from_slot(slot));
                continue;
            }

            // Slow path: trace finalisation + full cacheline reset.
            if trace_handle != 0 {
                release_trace(trace_handle);
            }
            let slot_capacity = self.slot_capacity;
            {
                let buffer = self
                    .buffer_at_slot_mut(index.slot)
                    .expect("buffer slot remains valid");
                buffer.cacheline1.trace_handle = 0;
                buffer.reset_for_free(slot_capacity);
            }
            {
                let entry = self
                    .slot_state_mut(slot)
                    .expect("buffer slot metadata remains valid");
                entry.allocated = false;
            }
            self.dec_in_use();
            self.push_cache_slot(cache, index.slot);
            next = next_slot.and_then(|slot| self.index_from_slot(slot));
        }
    }

    /// Push a freed slot onto the thread cache, returning a batch to the arena
    /// free list when the cache exceeds the high-water mark so it never grows
    /// past its preallocated capacity.
    #[inline]
    fn push_cache_slot(&mut self, cache: &mut BufferThreadCache, slot: u32) {
        if cache.len >= BUFFER_THREAD_CACHE_HIGH_WATER {
            self.return_cache_batch(cache);
        }
        cache.push(slot);
    }

    /// Move up to `BUFFER_THREAD_CACHE_BATCH` slots from the arena free list
    /// into the thread cache. Cold because it only runs when the cache is
    /// empty, amortising the arena `RefCell` borrow across a batch. The grab
    /// is capped at half of the arena's currently-free slots so concurrent
    /// consumers sharing the arena (handoff workers) are not starved.
    #[cold]
    #[inline(never)]
    fn refill_cache_batch(&mut self, cache: &mut BufferThreadCache) {
        let arena_free = self.available_len;
        if arena_free == 0 {
            return;
        }
        // Leave at least one slot for any other arena consumer, and never grab
        // more than half of what is currently free.
        let max_grab = BUFFER_THREAD_CACHE_BATCH.min(arena_free / 2 + arena_free % 2);
        let mut moved = 0usize;
        while moved < max_grab && cache.len < BUFFER_THREAD_CACHE_HIGH_WATER {
            let Some(slot) = self.pop_available_slot() else {
                break;
            };
            cache.push(slot);
            moved += 1;
        }
    }

    /// Move up to `BUFFER_THREAD_CACHE_BATCH` slots from the thread cache back
    /// to the arena free list when the cache is at/over the high-water mark.
    #[cold]
    #[inline(never)]
    fn return_cache_batch(&mut self, cache: &mut BufferThreadCache) {
        let mut moved = 0usize;
        while moved < BUFFER_THREAD_CACHE_BATCH && cache.len > BUFFER_THREAD_CACHE_BATCH {
            let Some(slot) = cache.pop() else {
                break;
            };
            self.push_available_slot(slot);
            moved += 1;
        }
    }

    #[inline(always)]
    fn bump_in_use(&mut self) {
        self.in_use_delta += 1;
        if self.in_use_delta >= BUFFER_IN_USE_FOLD_THRESHOLD {
            self.fold_in_use();
        }
    }

    #[inline(always)]
    fn dec_in_use(&mut self) {
        self.in_use_delta -= 1;
        if self.in_use_delta <= -BUFFER_IN_USE_FOLD_THRESHOLD {
            self.fold_in_use();
        }
    }

    #[inline]
    fn fold_in_use(&mut self) {
        if self.in_use_delta == 0 {
            return;
        }
        if self.in_use_delta > 0 {
            self.in_use = self.in_use.saturating_add(self.in_use_delta as usize);
        } else {
            let dec = self.in_use_delta.unsigned_abs() as usize;
            self.in_use = self.in_use.saturating_sub(dec);
        }
        self.in_use_delta = 0;
    }

    /// Prefetch the next slot the caller is about to pop from the cache so its
    /// header lands in L2 (and is promoted to L1 by the time it is touched).
    #[inline]
    fn prefetch_next_cached_slot(&self, cache: &BufferThreadCache) {
        if let Some(next_slot) = cache.last()
            && let Ok(buffer) = self.buffer_at_slot(next_slot)
        {
            prefetch_read_l2(ptr::from_ref(&buffer.cacheline0).cast::<u8>());
        }
    }

    /// Prefetch the next buffer header along a chain being freed so the
    /// generation/ref_count reads hit a warm line.
    #[inline]
    fn prefetch_chain_next(&self, slot: u32) {
        if let Ok(buffer) = self.buffer_at_slot(slot) {
            prefetch_read_l2(ptr::from_ref(&buffer.cacheline0).cast::<u8>());
        }
    }

    #[inline]
    fn attach_clone(&mut self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        if head == tail {
            return Err(CoreError::internal(
                "buffer attach clone requires distinct head and tail",
            ));
        }
        self.ensure_header_exclusive(head)?;
        if self.next_buffer(head)?.is_some() {
            return Err(CoreError::internal(
                "buffer attach clone requires head without next buffer",
            ));
        }
        let tail_len = {
            let tail_buffer = self.buffer(tail)?;
            tail_buffer
                .current_len()
                .checked_add(tail_buffer.total_len_not_including_first())
                .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?
        };
        {
            let head_buffer = self.buffer_mut(head)?;
            head_buffer.set_next_buffer(Some(tail));
        }
        let mut current = Some(tail);
        while let Some(current_index) = current {
            let next = self.next_buffer(current_index)?;
            let buffer = self.buffer_mut(current_index)?;
            buffer.cacheline0.ref_count = buffer
                .cacheline0
                .ref_count
                .checked_add(1)
                .ok_or_else(|| CoreError::internal("buffer refcount overflow"))?;
            current = next;
        }
        self.buffer_mut(head)?
            .set_total_len_not_including_first(tail_len)
    }

    #[inline]
    fn append_chain(
        &mut self,
        cache: &mut BufferThreadCache,
        index: BufferIndex,
        bytes: &[u8],
    ) -> CoreResult<()> {
        self.ensure_writable(index)?;
        let mut tail = index;
        while let Some(next) = self.next_buffer(tail)? {
            self.ensure_writable(next)?;
            tail = next;
        }
        let appended_after_first = tail != index;
        let original_tail_len = self.buffer(index)?.total_len_not_including_first();
        let slot_capacity = self.slot_capacity;

        let taken = self.buffer_mut(tail)?.append_in_place(slot_capacity, bytes);
        let mut added_tail_len = if appended_after_first { taken } else { 0 };
        let mut remaining = &bytes[taken..];
        while !remaining.is_empty() {
            let take = remaining.len().min(slot_capacity);
            let next = self.alloc_slot(cache, &remaining[..take])?;
            {
                let tail_buffer = self.buffer_mut(tail)?;
                tail_buffer.set_next_buffer(Some(next));
            }
            added_tail_len = added_tail_len
                .checked_add(take)
                .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?;
            tail = next;
            remaining = &remaining[take..];
        }
        let first_tail_len = original_tail_len
            .checked_add(added_tail_len)
            .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?;
        self.buffer_mut(index)?
            .set_total_len_not_including_first(first_tail_len)
    }

    #[inline]
    fn chain_buffer(&mut self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.ensure_writable(head)?;
        self.buffer(tail)?;
        let tail_len = {
            let tail_buffer = self.buffer(tail)?;
            tail_buffer
                .current_len()
                .checked_add(tail_buffer.total_len_not_including_first())
                .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?
        };
        let mut last = head;
        while let Some(next) = self.next_buffer(last)? {
            self.ensure_writable(next)?;
            last = next;
        }
        self.buffer_mut(last)?.set_next_buffer(Some(tail));
        let total_tail_len = self
            .buffer(head)?
            .total_len_not_including_first()
            .checked_add(tail_len)
            .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?;
        self.buffer_mut(head)?
            .set_total_len_not_including_first(total_tail_len)
    }
}

#[inline(always)]
fn prefetch_buffer_header(buffer: &Buffer) {
    prefetch_read_l1(ptr::from_ref(&buffer.cacheline0).cast::<u8>());
}

#[inline(always)]
fn prefetch_buffer_header_write(buffer: &Buffer) {
    prefetch_write_l1(ptr::from_ref(&buffer.cacheline0).cast::<u8>());
}

#[inline(always)]
fn prefetch_buffer_cacheline1(buffer: &Buffer) {
    prefetch_read_l1(ptr::from_ref(&buffer.cacheline1).cast::<u8>());
}

#[inline(always)]
fn prefetch_buffer_cacheline1_write(buffer: &Buffer) {
    prefetch_write_l1(ptr::from_ref(&buffer.cacheline1).cast::<u8>());
}

#[inline(always)]
fn prefetch_buffer_data(buffer: &Buffer) {
    if !buffer.current().is_empty() {
        prefetch_read_l1(buffer.current().as_ptr());
    }
}

#[inline(always)]
fn prefetch_buffer_data_write(buffer: &Buffer) {
    if !buffer.current().is_empty() {
        prefetch_write_l1(buffer.current().as_ptr());
    }
}

#[derive(Debug)]
pub struct BufferFrame {
    indices: Vec<BufferIndex>,
    next_node: Option<NodeId>,
    readiness: Rc<FrameReadiness>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferFramePairBatch {
    Pair([BufferIndex; 2]),
    Single(BufferIndex),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferFrameQuadBatch {
    Quad([BufferIndex; 4]),
    Pair([BufferIndex; 2]),
    Single(BufferIndex),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferFrameBatch {
    Quad([BufferIndex; 4]),
    Pair([BufferIndex; 2]),
    Single(BufferIndex),
}

impl BufferFramePairBatch {
    #[inline]
    pub fn indices(self) -> BufferFrameBatchIndices {
        match self {
            Self::Pair(indices) => BufferFrameBatchIndices::new(&indices),
            Self::Single(index) => BufferFrameBatchIndices::new(&[index]),
        }
    }
}

impl BufferFrameQuadBatch {
    #[inline]
    pub fn indices(self) -> BufferFrameBatchIndices {
        match self {
            Self::Quad(indices) => BufferFrameBatchIndices::new(&indices),
            Self::Pair(indices) => BufferFrameBatchIndices::new(&indices),
            Self::Single(index) => BufferFrameBatchIndices::new(&[index]),
        }
    }
}

impl BufferFrameBatch {
    #[inline]
    pub fn indices(self) -> BufferFrameBatchIndices {
        match self {
            Self::Quad(indices) => BufferFrameBatchIndices::new(&indices),
            Self::Pair(indices) => BufferFrameBatchIndices::new(&indices),
            Self::Single(index) => BufferFrameBatchIndices::new(&[index]),
        }
    }
}

pub struct BufferFrameBatchIndices {
    indices: [Option<BufferIndex>; 4],
    len: usize,
    offset: usize,
}

impl BufferFrameBatchIndices {
    #[inline]
    fn new(indices: &[BufferIndex]) -> Self {
        let mut values = [None; 4];
        for (offset, index) in indices.iter().copied().enumerate() {
            values[offset] = Some(index);
        }
        Self {
            indices: values,
            len: indices.len(),
            offset: 0,
        }
    }
}

impl Iterator for BufferFrameBatchIndices {
    type Item = BufferIndex;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.len {
            return None;
        }
        let index = self.indices[self.offset];
        self.offset += 1;
        index
    }
}

#[derive(Debug, Clone)]
pub struct BufferFramePairBatchCursor<'frame> {
    indices: &'frame [BufferIndex],
    offset: usize,
}

#[derive(Debug, Clone)]
pub struct BufferFrameQuadBatchCursor<'frame> {
    indices: &'frame [BufferIndex],
    offset: usize,
}

#[derive(Debug, Clone)]
pub struct BufferFrameBatchCursor<'frame> {
    indices: &'frame [BufferIndex],
    offset: usize,
    width: FrameBatchWidth,
}

#[derive(Default)]
struct FrameReadiness {
    pending: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl fmt::Debug for FrameReadiness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameReadiness")
            .field("pending", &self.pending.get())
            .field("has_waker", &self.waker.borrow().is_some())
            .finish()
    }
}

impl FrameReadiness {
    #[inline]
    fn mark_pending(&self) {
        self.pending.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    #[inline]
    fn clear_pending(&self) {
        self.pending.set(false);
    }

    #[inline]
    fn reset_for_pool_reuse(&self) {
        self.pending.set(false);
        self.waker.borrow_mut().take();
    }

    #[inline]
    fn poll_pending(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.pending.get() {
            return Poll::Ready(());
        }
        let mut waker = self.waker.borrow_mut();
        let replace_waker = match waker.as_ref() {
            Some(waker) => !waker.will_wake(cx.waker()),
            None => true,
        };
        if replace_waker {
            *waker = Some(cx.waker().clone());
        }
        if self.pending.get() {
            waker.take();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

macro_rules! retain_ladder {
    ($read:ident, $write:ident, $len:ident, 2, $step:expr) => {
        while $read + 2 <= $len {
            $step(0)?;
            $step(1)?;
            $read += 2;
        }
        if $read < $len {
            $step(0)?;
            $read += 1;
        }
    };
    ($read:ident, $write:ident, $len:ident, 4, $step:expr) => {
        while $read + 4 <= $len {
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $read += 4;
        }
        retain_ladder!($read, $write, $len, 2, $step);
    };
}

macro_rules! retain_ladder_prefetch {
    ($self:expr, $read:ident, $write:ident, $len:ident, $prefetch:ident, 2, $step:expr) => {
        while $read + 2 <= $len {
            $self.prefetch_indices($read + 2, 2, $prefetch);
            $step(0)?;
            $step(1)?;
            $read += 2;
        }
        if $read < $len {
            $step(0)?;
            $read += 1;
        }
    };
    ($self:expr, $read:ident, $write:ident, $len:ident, $prefetch:ident, 4, $step:expr) => {
        while $read + 4 <= $len {
            $self.prefetch_indices($read + 4, 4, $prefetch);
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $read += 4;
        }
        retain_ladder_prefetch!($self, $read, $write, $len, $prefetch, 2, $step);
    };
}

macro_rules! retain_ladder_state_prefetch {
    ($self:expr, $read:ident, $write:ident, $len:ident, $state:ident, $prefetch:ident, 2, $step:expr) => {
        while $read + 2 <= $len {
            $self.prefetch_indices_state($read + 2, 2, $state, $prefetch);
            $step(0)?;
            $step(1)?;
            $read += 2;
        }
        if $read < $len {
            $step(0)?;
            $read += 1;
        }
    };
    ($self:expr, $read:ident, $write:ident, $len:ident, $state:ident, $prefetch:ident, 4, $step:expr) => {
        while $read + 4 <= $len {
            $self.prefetch_indices_state($read + 4, 4, $state, $prefetch);
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $read += 4;
        }
        retain_ladder_state_prefetch!($self, $read, $write, $len, $state, $prefetch, 2, $step);
    };
}

macro_rules! rewrite_ladder {
    ($read:ident, $write:ident, $len:ident, 2, $step:expr) => {
        while $read + 2 <= $len {
            $step(0)?;
            $step(1)?;
            $read += 2;
        }
        if $read < $len {
            $step(0)?;
            $read += 1;
        }
    };
    ($read:ident, $write:ident, $len:ident, 4, $step:expr) => {
        while $read + 4 <= $len {
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $read += 4;
        }
        rewrite_ladder!($read, $write, $len, 2, $step);
    };
    ($read:ident, $write:ident, $len:ident, 8, $step:expr) => {
        while $read + 8 <= $len {
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $step(4)?;
            $step(5)?;
            $step(6)?;
            $step(7)?;
            $read += 8;
        }
        rewrite_ladder!($read, $write, $len, 4, $step);
    };
}

impl BufferFrame {
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            indices: Vec::with_capacity(capacity),
            next_node: None,
            readiness: Rc::new(FrameReadiness::default()),
        }
    }

    #[inline]
    pub fn push_index(&mut self, index: BufferIndex) -> CoreResult<()> {
        if self.indices.len() == self.indices.capacity() {
            return Err(CoreError::internal("buffer frame capacity exceeded"));
        }
        self.indices.push(index);
        self.readiness.mark_pending();
        Ok(())
    }

    #[inline]
    pub fn push_indices(
        &mut self,
        indices: impl IntoIterator<Item = BufferIndex>,
    ) -> CoreResult<()> {
        let indices = indices.into_iter();
        let (lower, upper) = indices.size_hint();
        if let Some(upper) = upper {
            if self.indices.len() + upper > self.indices.capacity() {
                return Err(CoreError::internal("buffer frame capacity exceeded"));
            }
        } else if self.indices.len() + lower > self.indices.capacity() {
            return Err(CoreError::internal("buffer frame capacity exceeded"));
        }

        let original_len = self.indices.len();
        for index in indices {
            if self.indices.len() == self.indices.capacity() {
                self.indices.truncate(original_len);
                return Err(CoreError::internal("buffer frame capacity exceeded"));
            }
            self.indices.push(index);
        }
        if self.indices.len() != original_len {
            self.readiness.mark_pending();
        }
        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    #[inline]
    pub fn has_pending(&self) -> bool {
        !self.indices.is_empty()
    }

    #[inline]
    pub fn pending_len(&self) -> usize {
        self.indices.len()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.indices.capacity()
    }

    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        self.capacity().saturating_sub(self.len())
    }

    #[inline]
    pub fn reset(&mut self) {
        self.indices.clear();
        self.next_node = None;
        self.readiness.clear_pending();
    }

    #[inline]
    fn reset_for_pool_reuse(&mut self) {
        self.indices.clear();
        self.next_node = None;
        self.readiness.reset_for_pool_reuse();
    }

    #[inline]
    pub fn clear(&mut self) {
        self.reset();
    }

    #[inline]
    pub fn indices(&self) -> &[BufferIndex] {
        &self.indices
    }

    #[inline]
    pub fn pending_indices(&self) -> &[BufferIndex] {
        &self.indices
    }

    #[inline]
    pub fn pair_batch_cursor(&self) -> BufferFramePairBatchCursor<'_> {
        BufferFramePairBatchCursor {
            indices: self.pending_indices(),
            offset: 0,
        }
    }

    #[inline]
    pub fn quad_batch_cursor(&self) -> BufferFrameQuadBatchCursor<'_> {
        BufferFrameQuadBatchCursor {
            indices: self.pending_indices(),
            offset: 0,
        }
    }

    #[inline]
    pub fn batch_cursor(&self, width: FrameBatchWidth) -> BufferFrameBatchCursor<'_> {
        BufferFrameBatchCursor {
            indices: self.pending_indices(),
            offset: 0,
            width,
        }
    }

    #[inline]
    pub fn iter_indices(&self) -> slice::Iter<'_, BufferIndex> {
        self.indices.iter()
    }

    #[inline]
    pub fn drain_indices(&mut self) -> hammer_infra::vec::Drain<'_, BufferIndex> {
        self.readiness.clear_pending();
        self.indices.drain(..)
    }

    #[inline]
    pub fn discard_prefix(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        let count = count.min(self.indices.len());
        drop(self.indices.drain(..count));
        if self.indices.is_empty() {
            self.readiness.clear_pending();
        } else {
            self.readiness.mark_pending();
        }
    }

    #[inline]
    pub fn drain_pending(&mut self) -> hammer_infra::vec::Drain<'_, BufferIndex> {
        self.readiness.clear_pending();
        self.indices.drain(..)
    }

    #[inline]
    pub fn retain_indices(
        &mut self,
        mut keep: impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let mut write = 0usize;
        for read in 0..self.indices.len() {
            let index = self.indices[read];
            if keep(index)? {
                if write != read {
                    self.indices[write] = index;
                }
                write += 1;
            }
        }
        self.indices.truncate(write);
        if self.indices.is_empty() {
            self.readiness.clear_pending();
        } else {
            self.readiness.mark_pending();
        }
        Ok(())
    }

    #[inline(always)]
    pub fn retain_indices_batched(
        &mut self,
        width: FrameBatchWidth,
        mut keep: impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        match width {
            FrameBatchWidth::Octo => self.retain_indices_octo(&mut keep),
            FrameBatchWidth::Quad => self.retain_indices_quad(&mut keep),
            FrameBatchWidth::Pair => self.retain_indices_pair(&mut keep),
        }
    }

    #[inline(always)]
    pub fn retain_indices_batched_with_prefetch(
        &mut self,
        width: FrameBatchWidth,
        mut prefetch: impl FnMut(BufferIndex),
        mut keep: impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        match width {
            FrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch(&mut prefetch, &mut keep)
            }
            FrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch(&mut prefetch, &mut keep)
            }
            FrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch(&mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub fn retain_indices_batched_with_prefetch_state<S>(
        &mut self,
        width: FrameBatchWidth,
        state: &mut S,
        mut prefetch: impl FnMut(&mut S, BufferIndex),
        mut keep: impl FnMut(&mut S, BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        match width {
            FrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
            FrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
            FrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub fn buffer_node_inline<S>(
        &mut self,
        width: FrameBatchWidth,
        state: &mut S,
        mut prefetch: impl FnMut(&mut S, BufferIndex),
        mut keep: impl FnMut(&mut S, BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        match width {
            FrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
            FrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
            FrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub(crate) fn buffer_node_inline_chunks<S>(
        &mut self,
        width: FrameBatchWidth,
        state: &mut S,
        mut prefetch: impl FnMut(&mut S, &[BufferIndex]),
        mut keep_chunk: impl FnMut(&mut S, &[BufferIndex], &mut [bool; 4]) -> CoreResult<()>,
    ) -> CoreResult<()> {
        match width {
            FrameBatchWidth::Quad => self.retain_indices_quad_with_prefetch_state_lazy_chunks(
                state,
                &mut prefetch,
                &mut keep_chunk,
            ),
            FrameBatchWidth::Pair => self.retain_indices_pair_with_prefetch_state_lazy_chunks(
                state,
                &mut prefetch,
                &mut keep_chunk,
            ),
            FrameBatchWidth::Octo => self.retain_indices_quad_with_prefetch_state_lazy_chunks(
                state,
                &mut prefetch,
                &mut keep_chunk,
            ),
        }
    }

    #[inline(always)]
    pub fn rewrite_indices_batched(
        &mut self,
        width: FrameBatchWidth,
        mut rewrite: impl FnMut(BufferIndex) -> CoreResult<Option<BufferIndex>>,
    ) -> CoreResult<()> {
        match width {
            FrameBatchWidth::Quad => self.rewrite_indices_quad(&mut rewrite),
            FrameBatchWidth::Pair => self.rewrite_indices_pair(&mut rewrite),
            FrameBatchWidth::Octo => self.rewrite_indices_octo(&mut rewrite),
        }
    }

    #[inline(always)]
    fn retain_indices_quad(
        &mut self,
        keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        while read + 4 <= len {
            let chunk = [
                self.indices[read],
                self.indices[read + 1],
                self.indices[read + 2],
                self.indices[read + 3],
            ];
            let mask = movemask_4([
                keep(chunk[0])?,
                keep(chunk[1])?,
                keep(chunk[2])?,
                keep(chunk[3])?,
            ]);
            if mask == 0b1111 && write == read {
                write += 4;
            } else {
                let mut m = mask;
                while m != 0 {
                    let lsb = m.trailing_zeros();
                    self.indices[write] = chunk[lsb as usize];
                    write += 1;
                    m &= m - 1;
                }
            }
            read += 4;
        }
        if read + 2 <= len {
            self.retain_one(read, &mut write, keep)?;
            self.retain_one(read + 1, &mut write, keep)?;
            read += 2;
        }
        while read < len {
            self.retain_one(read, &mut write, keep)?;
            read += 1;
        }
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_quad_with_prefetch(
        &mut self,
        prefetch: &mut impl FnMut(BufferIndex),
        keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder_prefetch!(self, read, write, len, prefetch, 4, |offset| {
            self.retain_one(read + offset, &mut write, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair(
        &mut self,
        keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder!(read, write, len, 2, |offset| {
            self.retain_one(read + offset, &mut write, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_octo(
        &mut self,
        keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        while read + 8 <= len {
            let chunk = [
                self.indices[read],
                self.indices[read + 1],
                self.indices[read + 2],
                self.indices[read + 3],
                self.indices[read + 4],
                self.indices[read + 5],
                self.indices[read + 6],
                self.indices[read + 7],
            ];
            let k0 = keep(chunk[0])?;
            let k1 = keep(chunk[1])?;
            let k2 = keep(chunk[2])?;
            let k3 = keep(chunk[3])?;
            let k4 = keep(chunk[4])?;
            let k5 = keep(chunk[5])?;
            let k6 = keep(chunk[6])?;
            let k7 = keep(chunk[7])?;
            let mask = (k0 as u8)
                | ((k1 as u8) << 1)
                | ((k2 as u8) << 2)
                | ((k3 as u8) << 3)
                | ((k4 as u8) << 4)
                | ((k5 as u8) << 5)
                | ((k6 as u8) << 6)
                | ((k7 as u8) << 7);
            if mask == 0xff && write == read {
                write += 8;
            } else {
                let mut m = mask;
                while m != 0 {
                    let lsb = m.trailing_zeros();
                    self.indices[write] = chunk[lsb as usize];
                    write += 1;
                    m &= m - 1;
                }
            }
            read += 8;
        }
        if read + 4 <= len {
            let chunk = [
                self.indices[read],
                self.indices[read + 1],
                self.indices[read + 2],
                self.indices[read + 3],
            ];
            let mask = movemask_4([
                keep(chunk[0])?,
                keep(chunk[1])?,
                keep(chunk[2])?,
                keep(chunk[3])?,
            ]);
            if mask == 0b1111 && write == read {
                write += 4;
            } else {
                let mut m = mask;
                while m != 0 {
                    let lsb = m.trailing_zeros();
                    self.indices[write] = chunk[lsb as usize];
                    write += 1;
                    m &= m - 1;
                }
            }
            read += 4;
        }
        if read + 2 <= len {
            self.retain_one(read, &mut write, keep)?;
            self.retain_one(read + 1, &mut write, keep)?;
            read += 2;
        }
        while read < len {
            self.retain_one(read, &mut write, keep)?;
            read += 1;
        }
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair_with_prefetch(
        &mut self,
        prefetch: &mut impl FnMut(BufferIndex),
        keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder_prefetch!(self, read, write, len, prefetch, 2, |offset| {
            self.retain_one(read + offset, &mut write, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_quad_with_prefetch_state<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, BufferIndex),
        keep: &mut impl FnMut(&mut S, BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder_state_prefetch!(self, read, write, len, state, prefetch, 4, |offset| {
            self.retain_one_state(read + offset, &mut write, state, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair_with_prefetch_state<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, BufferIndex),
        keep: &mut impl FnMut(&mut S, BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder_state_prefetch!(self, read, write, len, state, prefetch, 2, |offset| {
            self.retain_one_state(read + offset, &mut write, state, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_quad_with_prefetch_state_lazy<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, BufferIndex),
        keep: &mut impl FnMut(&mut S, BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = None;
        retain_ladder_state_prefetch!(self, read, write, len, state, prefetch, 4, |offset| {
            self.retain_one_state_lazy(read + offset, &mut write, state, keep)
        });
        self.finish_retain_lazy(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair_with_prefetch_state_lazy<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, BufferIndex),
        keep: &mut impl FnMut(&mut S, BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = None;
        retain_ladder_state_prefetch!(self, read, write, len, state, prefetch, 2, |offset| {
            self.retain_one_state_lazy(read + offset, &mut write, state, keep)
        });
        self.finish_retain_lazy(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_quad_with_prefetch_state_lazy_chunks<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, &[BufferIndex]),
        keep_chunk: &mut impl FnMut(&mut S, &[BufferIndex], &mut [bool; 4]) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = None;
        while read + 4 <= len {
            self.prefetch_indices_state_chunk(read + 4, 4, state, prefetch);
            let chunk = [
                self.indices[read],
                self.indices[read + 1],
                self.indices[read + 2],
                self.indices[read + 3],
            ];
            self.retain_chunk_state_lazy(read, chunk, &mut write, state, keep_chunk)?;
            read += 4;
        }
        if read + 2 <= len {
            self.prefetch_indices_state_chunk(read + 2, 2, state, prefetch);
            let chunk = [self.indices[read], self.indices[read + 1]];
            self.retain_chunk_state_lazy(read, chunk, &mut write, state, keep_chunk)?;
            read += 2;
        }
        while read < len {
            let chunk = [self.indices[read]];
            self.retain_chunk_state_lazy(read, chunk, &mut write, state, keep_chunk)?;
            read += 1;
        }
        self.finish_retain_lazy(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair_with_prefetch_state_lazy_chunks<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, &[BufferIndex]),
        keep_chunk: &mut impl FnMut(&mut S, &[BufferIndex], &mut [bool; 4]) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = None;
        while read + 2 <= len {
            self.prefetch_indices_state_chunk(read + 2, 2, state, prefetch);
            let chunk = [self.indices[read], self.indices[read + 1]];
            self.retain_chunk_state_lazy(read, chunk, &mut write, state, keep_chunk)?;
            read += 2;
        }
        if read < len {
            let chunk = [self.indices[read]];
            self.retain_chunk_state_lazy(read, chunk, &mut write, state, keep_chunk)?;
        }
        self.finish_retain_lazy(write);
        Ok(())
    }

    #[inline(always)]
    fn rewrite_indices_quad(
        &mut self,
        rewrite: &mut impl FnMut(BufferIndex) -> CoreResult<Option<BufferIndex>>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        rewrite_ladder!(read, write, len, 4, |offset| {
            self.rewrite_one(read + offset, &mut write, rewrite)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn rewrite_indices_octo(
        &mut self,
        rewrite: &mut impl FnMut(BufferIndex) -> CoreResult<Option<BufferIndex>>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        rewrite_ladder!(read, write, len, 8, |offset| {
            self.rewrite_one(read + offset, &mut write, rewrite)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn rewrite_indices_pair(
        &mut self,
        rewrite: &mut impl FnMut(BufferIndex) -> CoreResult<Option<BufferIndex>>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        rewrite_ladder!(read, write, len, 2, |offset| {
            self.rewrite_one(read + offset, &mut write, rewrite)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_one(
        &mut self,
        read: usize,
        write: &mut usize,
        keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let index = self.indices[read];
        if keep(index)? {
            self.indices[*write] = index;
            *write += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn retain_one_state<S>(
        &mut self,
        read: usize,
        write: &mut usize,
        state: &mut S,
        keep: &mut impl FnMut(&mut S, BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let index = self.indices[read];
        if keep(state, index)? {
            self.indices[*write] = index;
            *write += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn retain_one_state_lazy<S>(
        &mut self,
        read: usize,
        write: &mut Option<usize>,
        state: &mut S,
        keep: &mut impl FnMut(&mut S, BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let index = self.indices[read];
        if keep(state, index)? {
            if let Some(write) = write {
                self.indices[*write] = index;
                *write += 1;
            }
        } else if write.is_none() {
            *write = Some(read);
        }
        Ok(())
    }

    #[inline(always)]
    fn retain_chunk_state_lazy<S, const N: usize>(
        &mut self,
        read: usize,
        chunk: [BufferIndex; N],
        write: &mut Option<usize>,
        state: &mut S,
        keep_chunk: &mut impl FnMut(&mut S, &[BufferIndex], &mut [bool; 4]) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let mut keep = [true; 4];
        keep_chunk(state, &chunk, &mut keep)?;
        for offset in 0..N {
            let index = chunk[offset];
            if keep[offset] {
                if let Some(write) = write {
                    self.indices[*write] = index;
                    *write += 1;
                }
            } else if write.is_none() {
                *write = Some(read + offset);
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn rewrite_one(
        &mut self,
        read: usize,
        write: &mut usize,
        rewrite: &mut impl FnMut(BufferIndex) -> CoreResult<Option<BufferIndex>>,
    ) -> CoreResult<()> {
        let index = self.indices[read];
        if let Some(index) = rewrite(index)? {
            self.indices[*write] = index;
            *write += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn prefetch_indices(
        &self,
        offset: usize,
        width: usize,
        prefetch: &mut impl FnMut(BufferIndex),
    ) {
        let end = (offset + width).min(self.indices.len());
        for index in self.indices[offset..end].iter().copied() {
            prefetch(index);
        }
    }

    #[inline(always)]
    fn prefetch_indices_state<S>(
        &self,
        offset: usize,
        width: usize,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, BufferIndex),
    ) {
        let end = (offset + width).min(self.indices.len());
        for index in self.indices[offset..end].iter().copied() {
            prefetch(state, index);
        }
    }

    #[inline(always)]
    fn prefetch_indices_state_chunk<S>(
        &self,
        offset: usize,
        width: usize,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, &[BufferIndex]),
    ) {
        let end = (offset + width).min(self.indices.len());
        if offset < end {
            prefetch(state, &self.indices[offset..end]);
        }
    }

    #[inline(always)]
    fn finish_retain(&mut self, len: usize) {
        self.indices.truncate(len);
        if self.indices.is_empty() {
            self.readiness.clear_pending();
        } else {
            self.readiness.mark_pending();
        }
    }

    #[inline(always)]
    fn finish_retain_lazy(&mut self, len: Option<usize>) {
        if let Some(len) = len {
            self.finish_retain(len);
        }
    }

    #[inline]
    pub fn pending(&self) -> BufferFramePending {
        BufferFramePending {
            readiness: Rc::clone(&self.readiness),
        }
    }

    #[inline]
    pub fn next_node(&self) -> Option<NodeId> {
        self.next_node
    }

    #[inline]
    pub fn set_next_node(&mut self, node: NodeId) {
        self.next_node = Some(node);
    }

    #[inline]
    pub fn clear_next_node(&mut self) {
        self.next_node = None;
    }
}

impl BufferFramePairBatchCursor<'_> {
    #[inline]
    pub fn prefetch_next_pair(&self, runtime: &DataPlaneRuntime) {
        for index in self.indices[self.offset..].iter().take(2).copied() {
            runtime.prefetch_header(index);
        }
    }
}

impl Iterator for BufferFramePairBatchCursor<'_> {
    type Item = BufferFramePairBatch;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.indices.len().saturating_sub(self.offset);
        if remaining >= 2 {
            let batch = BufferFramePairBatch::Pair([
                self.indices[self.offset],
                self.indices[self.offset + 1],
            ]);
            self.offset += 2;
            Some(batch)
        } else if remaining == 1 {
            let batch = BufferFramePairBatch::Single(self.indices[self.offset]);
            self.offset += 1;
            Some(batch)
        } else {
            None
        }
    }
}

impl BufferFrameQuadBatchCursor<'_> {
    #[inline]
    pub fn prefetch_next_quad(&self, runtime: &DataPlaneRuntime) {
        for index in self.indices[self.offset..].iter().take(4).copied() {
            runtime.prefetch_header(index);
        }
    }
}

impl Iterator for BufferFrameQuadBatchCursor<'_> {
    type Item = BufferFrameQuadBatch;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.indices.len().saturating_sub(self.offset);
        if remaining >= 4 {
            let batch = BufferFrameQuadBatch::Quad([
                self.indices[self.offset],
                self.indices[self.offset + 1],
                self.indices[self.offset + 2],
                self.indices[self.offset + 3],
            ]);
            self.offset += 4;
            Some(batch)
        } else if remaining >= 2 {
            let batch = BufferFrameQuadBatch::Pair([
                self.indices[self.offset],
                self.indices[self.offset + 1],
            ]);
            self.offset += 2;
            Some(batch)
        } else if remaining == 1 {
            let batch = BufferFrameQuadBatch::Single(self.indices[self.offset]);
            self.offset += 1;
            Some(batch)
        } else {
            None
        }
    }
}

impl BufferFrameBatchCursor<'_> {
    #[inline]
    pub fn prefetch_next(&self, runtime: &DataPlaneRuntime) {
        let width = match self.width {
            FrameBatchWidth::Octo => 8,
            FrameBatchWidth::Quad => 4,
            FrameBatchWidth::Pair => 2,
        };
        for index in self.indices[self.offset..].iter().take(width).copied() {
            runtime.prefetch_header(index);
        }
    }
}

impl Iterator for BufferFrameBatchCursor<'_> {
    type Item = BufferFrameBatch;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.indices.len().saturating_sub(self.offset);
        match self.width {
            FrameBatchWidth::Octo | FrameBatchWidth::Quad if remaining >= 4 => {
                let batch = BufferFrameBatch::Quad([
                    self.indices[self.offset],
                    self.indices[self.offset + 1],
                    self.indices[self.offset + 2],
                    self.indices[self.offset + 3],
                ]);
                self.offset += 4;
                Some(batch)
            }
            _ if remaining >= 2 => {
                let batch = BufferFrameBatch::Pair([
                    self.indices[self.offset],
                    self.indices[self.offset + 1],
                ]);
                self.offset += 2;
                Some(batch)
            }
            _ if remaining == 1 => {
                let batch = BufferFrameBatch::Single(self.indices[self.offset]);
                self.offset += 1;
                Some(batch)
            }
            _ => None,
        }
    }
}

pub struct BufferFramePending {
    readiness: Rc<FrameReadiness>,
}

impl Future for BufferFramePending {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.readiness.poll_pending(cx)
    }
}

pub struct FrameRef<'pool> {
    guard: Ref<'pool, FramePoolInner>,
    index: FrameIndex,
}

impl FrameRef<'_> {
    #[inline]
    pub fn has_pending(&self) -> bool {
        self.guard
            .frame(self.index)
            .expect("frame ref points to valid frame")
            .has_pending()
    }

    #[inline]
    pub fn pending_len(&self) -> usize {
        self.guard
            .frame(self.index)
            .expect("frame ref points to valid frame")
            .pending_len()
    }
}

pub struct FrameRefMut<'pool> {
    guard: RefMut<'pool, FramePoolInner>,
    index: FrameIndex,
}

impl FrameRefMut<'_> {
    #[inline]
    pub fn push_index(&mut self, index: BufferIndex) -> CoreResult<()> {
        self.guard
            .frame_mut(self.index)
            .expect("frame ref points to valid frame")
            .push_index(index)
    }

    #[inline]
    pub fn push_indices(
        &mut self,
        indices: impl IntoIterator<Item = BufferIndex>,
    ) -> CoreResult<()> {
        self.guard
            .frame_mut(self.index)
            .expect("frame ref points to valid frame")
            .push_indices(indices)
    }

    #[inline]
    pub fn has_pending(&self) -> bool {
        self.guard
            .frame(self.index)
            .expect("frame ref points to valid frame")
            .has_pending()
    }

    #[inline]
    pub fn set_next_node(&mut self, node: NodeId) {
        self.guard
            .frame_mut(self.index)
            .expect("frame ref points to valid frame")
            .set_next_node(node);
    }
}

#[cfg(test)]
mod tests {
    use super::{BufferIndex, BufferPool, DataPlaneRuntime};
    use hammer_core::error::CoreResult;
    use hammer_infra::vec::Vec;

    fn chain_bytes(pool: &BufferPool, index: BufferIndex) -> CoreResult<Vec<u8>> {
        let mut out = Vec::new();
        let mut next = Some(index);
        while let Some(current) = next {
            {
                let buffer = pool.get(current)?;
                out.extend_from_slice(buffer.current());
            }
            next = pool.next_buffer(current)?;
        }
        Ok(out)
    }

    #[test]
    fn advance_with_negative_displacement_rewinds_into_headroom() {
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let index = runtime.alloc_index().expect("alloc buffer");
        runtime
            .buffers()
            .append(index, b"hello")
            .expect("append payload");

        runtime
            .buffers()
            .advance(index, -4)
            .expect("rewind into headroom");

        let buffer = runtime.get_buffer(index).expect("buffer");
        assert_eq!(buffer.current_data_offset(), -4);
        assert_eq!(buffer.current_len(), 9);
        assert_eq!(buffer.total_len_not_including_first(), 0);
        let packet = chain_bytes(runtime.buffers().buffers(), index).expect("packet");
        assert_eq!(packet.len(), 9);
        assert_eq!(&packet[4..], b"hello");
    }

    use super::{Buffer, BufferFlags, BufferPoolInner, BufferThreadCache};

    fn fresh_pool(slot_capacity: usize, slots: usize) -> BufferPool {
        BufferPool::with_capacity(slot_capacity, slots)
    }

    #[test]
    fn slot_clean_set_after_alloc_and_free_cycle() {
        let pool = fresh_pool(64, 4);
        let index = pool.alloc_index().expect("alloc");
        // After a clean alloc the slot must be marked clean (cacheline1 zeroed).
        {
            let inner = pool.arena.inner.read();
            let buffer = inner.buffer_at_slot(index.slot()).unwrap();
            assert!(
                buffer.cacheline0.flags.contains(BufferFlags::SLOT_CLEAN),
                "alloc must mark slot clean"
            );
        }
        pool.free_index(index);
        // After a clean free the slot must still be marked clean so the next
        // alloc fast path can skip the cacheline1 reset.
        {
            let inner = pool.arena.inner.read();
            let buffer = inner.buffer_at_slot(index.slot()).unwrap();
            assert!(
                buffer.cacheline0.flags.contains(BufferFlags::SLOT_CLEAN),
                "free fast path must keep slot clean"
            );
        }
    }

    #[test]
    fn dirtying_cacheline1_clears_slot_clean() {
        let pool = fresh_pool(64, 4);
        let index = pool.alloc_index().expect("alloc");
        // Mutating a cacheline1 field must drop the clean invariant.
        {
            let mut guard = pool.arena.inner.write();
            let buffer = guard.buffer_at_slot_mut(index.slot()).unwrap();
            buffer.set_total_len_not_including_first(7).unwrap();
            assert!(
                !buffer.cacheline0.flags.contains(BufferFlags::SLOT_CLEAN),
                "cacheline1 mutator must clear SLOT_CLEAN"
            );
        }
        pool.free_index(index);
        // The free slow path must rebuild the clean invariant.
        let inner = pool.arena.inner.read();
        let buffer = inner.buffer_at_slot(index.slot()).unwrap();
        assert!(
            buffer.cacheline0.flags.contains(BufferFlags::SLOT_CLEAN),
            "free slow path must restore SLOT_CLEAN"
        );
        assert_eq!(buffer.cacheline1.total_length_not_including_first, 0);
    }

    #[test]
    fn set_trace_handle_clears_slot_clean() {
        let pool = fresh_pool(64, 4);
        let index = pool.alloc_index().expect("alloc");
        {
            let mut guard = pool.arena.inner.write();
            let buffer = guard.buffer_at_slot_mut(index.slot()).unwrap();
            buffer.set_trace_handle(42);
            assert!(!buffer.cacheline0.flags.contains(BufferFlags::SLOT_CLEAN));
        }
        pool.free_index(index);
    }

    #[test]
    fn generation_advances_across_alloc_free_cycles() {
        let pool = fresh_pool(64, 4);
        let first = pool.alloc_index().expect("alloc");
        let first_gen = first.generation();
        pool.free_index(first);
        let second = pool.alloc_index().expect("realloc same slot");
        assert_eq!(second.slot(), first.slot());
        assert_ne!(second.generation(), first_gen);
        // Stale index must be rejected.
        assert!(pool.get(first).is_err());
        assert!(pool.get(second).is_ok());
        pool.free_index(second);
    }

    #[test]
    fn batch_refill_does_not_starve_shared_arena() {
        // Two pools share one arena; each worker must be able to alloc even
        // though the per-thread cache grabs a batch on first use.
        use super::BufferPoolArena;
        let arena = BufferPoolArena::with_capacity(64, 4);
        let pool_a = BufferPool::with_arena(arena.clone());
        let pool_b = BufferPool::with_arena(arena);
        let a = pool_a.alloc_index().expect("pool a alloc");
        let b = pool_b.alloc_index().expect("pool b alloc");
        assert_eq!(a.pool_id(), b.pool_id());
        assert_ne!(a.slot(), b.slot());
        pool_a.free_index(a);
        pool_b.free_index(b);
    }

    #[test]
    fn lazy_in_use_count_matches_synchronous_count() {
        let pool = fresh_pool(64, 8);
        let mut indices = std::vec::Vec::new();
        for _ in 0..5 {
            indices.push(pool.alloc_index().expect("alloc"));
        }
        // in_use() folds the lazy delta and returns the accurate count.
        assert_eq!(pool.in_use(), 5);
        for index in indices.drain(..) {
            pool.free_index(index);
        }
        pool.arena.inner.write().fold_in_use();
        assert_eq!(pool.arena.inner.read().in_use, 0);
    }

    #[test]
    fn refcount_gt_one_free_takes_slow_path_and_stays_allocated() {
        let pool = fresh_pool(64, 8);
        let head = pool.alloc_index().expect("alloc head");
        let tail = pool.alloc_index().expect("alloc tail");
        pool.attach_clone(head, tail).expect("attach clone");
        // tail now has ref_count == 2; freeing the chain drops one ref but
        // keeps the slot allocated (slow path).
        pool.free_index(head);
        let inner = pool.arena.inner.read();
        let tail_entry = inner.slot_state(tail.slot()).expect("tail slot state");
        assert!(tail_entry.allocated, "shared tail must remain allocated");
    }

    #[test]
    fn current_data_reports_negative_offsets_without_panicking() {
        let pool = fresh_pool(64, 4);
        let index = pool.alloc_index().expect("alloc");
        pool.append(index, b"hello").expect("append payload");
        pool.advance(index, -4).expect("rewind into headroom");

        let buffer = pool.get(index).expect("buffer");
        assert_eq!(buffer.current_data_offset(), -4);
        assert_eq!(buffer.current_data(), 0);
        assert_eq!(pool.current_data(index).expect("pool current_data"), 0);
    }

    #[test]
    fn chain_alloc_free_roundtrips() {
        let pool = fresh_pool(32, 16);
        let payload = vec![0xABu8; 96]; // spans 3 slots of 32
        let head = pool.alloc_index_with_bytes(&payload).expect("chain alloc");
        assert!(pool.next_buffer(head).unwrap().is_some());
        let copied = chain_bytes(&pool, head).expect("read chain");
        assert_eq!(copied, payload);
        pool.free_index(head);
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn alloc_index_with_bytes_resets_slot_clean() {
        let pool = fresh_pool(64, 4);
        let index = pool
            .alloc_index_with_bytes(b"payload")
            .expect("alloc with bytes");
        let inner = pool.arena.inner.read();
        let buffer = inner.buffer_at_slot(index.slot()).unwrap();
        assert!(
            buffer.cacheline0.flags.contains(BufferFlags::SLOT_CLEAN),
            "reset(bytes) must set SLOT_CLEAN"
        );
        assert_eq!(buffer.current_len(), 7);
    }

    #[test]
    fn buffer_remains_64_byte_aligned() {
        assert_eq!(core::mem::align_of::<Buffer>(), 64);
    }

    #[test]
    fn thread_cache_capacity_preallocated() {
        let cache = BufferThreadCache::new();
        let _: &[u32; super::BUFFER_THREAD_CACHE_HIGH_WATER] = &cache.cached_slots;
        assert_eq!(cache.cached_free_len(), 0);
        assert_eq!(cache.len, 0);
    }

    #[test]
    fn pool_inner_has_in_use_delta_field() {
        // Compile-time sanity that the lazy counter field exists.
        fn _assert_field(inner: &BufferPoolInner) {
            let _ = inner.in_use_delta;
        }
    }

    #[test]
    fn alloc_then_free_many_keeps_in_use_consistent() -> CoreResult<()> {
        let pool = fresh_pool(64, 32);
        let mut indices = std::vec::Vec::new();
        for _ in 0..32 {
            indices.push(pool.alloc_index()?);
        }
        assert_eq!(pool.in_use(), 32);
        for index in indices {
            pool.free_index(index);
        }
        assert_eq!(pool.in_use(), 0);
        Ok(())
    }
}
