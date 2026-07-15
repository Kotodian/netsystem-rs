use core::mem;
use core::ptr;
use core::slice;
use core::sync::atomic::{AtomicU64, Ordering};
use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::data_plane::NodeId;
use crate::error::{CoreError, CoreResult, DataPlaneError};
use hammer_infra::{
    align::align_up,
    boxed::Box,
    physmem::PhysmemMap,
    prefetch::{prefetch_read_l1, prefetch_read_l2, prefetch_write_l1},
    simd::movemask_4,
    vec::Vec,
};
use spinning_top::{
    RawRwSpinlock, RwSpinlock,
    lock_api::{MappedRwLockReadGuard, MappedRwLockWriteGuard},
    relax::Spin,
};

use super::memory::{HAMMER_MAX_NUMA_NODES, StaticNumaTable};

/// Production graph Frame logical maximum. Insertion enforces this limit even
/// though the underlying infrastructure vector remains growable.
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

/// Copyable data-plane pool identity. Pools construct it; Frame or another
/// domain owner retains release responsibility. Copying does not alter
/// reference counts.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Index {
    pool_id: u64,
    slot: u32,
    generation: u32,
}

const _: () = assert!(core::mem::size_of::<Index>() == 16);

impl Index {
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
    pub fn current_config(&self) -> NodeId {
        NodeId::new(self.cacheline0.current_config_or_punt)
    }

    #[inline]
    pub fn set_current_config(&mut self, next: NodeId) {
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
    fn set_next_buffer(&mut self, next: Option<Index>) {
        self.cacheline0.next_buffer = next.map_or(BUFFER_INVALID_INDEX, Index::slot);
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
    numa_node: u32,
    slot_capacity: usize,
    slot_stride: usize,
    region: PhysmemMap,
    region_size: usize,
    slot_states: Box<[BufferSlot]>,
    available_stack: Vec<u32>,
    total_slots: usize,
    in_use: usize,
    in_use_delta: i32,
}

impl fmt::Debug for BufferPoolInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPoolInner")
            .field("pool_id", &self.pool_id)
            .field("numa_node", &self.numa_node)
            .field("slot_capacity", &self.slot_capacity)
            .field("slot_stride", &self.slot_stride)
            .field("region_base", &self.region.base())
            .field("region_size", &self.region_size)
            .field("available_len", &self.available_stack.len())
            .field("total_slots", &self.total_slots)
            .field("in_use", &self.in_use)
            .field("in_use_delta", &self.in_use_delta)
            .finish()
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
    slots: Box<[FrameSlot]>,
    available: Box<[u32]>,
    available_len: usize,
    in_use: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct FramePool {
    inner: Rc<RefCell<FramePoolInner>>,
}

pub struct Next {
    owner: DataPlaneBuffers,
    index: Index,
    next: NodeId,
    frame: Option<BufferFrame>,
}

pub struct Pending {
    owner: DataPlaneBuffers,
    index: Index,
    frame: Option<BufferFrame>,
}

pub struct Frame<State> {
    state: State,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BufferFrameBatchWidth {
    Pair,
    Quad,
    Octo,
}

pub trait BufferFrameBatchWidthPolicy: Copy {
    fn buffer_frame_batch_width(self) -> BufferFrameBatchWidth;
}

impl BufferFrameBatchWidthPolicy for BufferFrameBatchWidth {
    #[inline]
    fn buffer_frame_batch_width(self) -> BufferFrameBatchWidth {
        self
    }
}

#[derive(Debug, Clone)]
pub struct DataPlaneBufferConfig {
    pub buffer_slot_capacity: usize,
    pub buffer_slots: usize,
    pub frame_slots: usize,
    pub numa_nodes: &'static [u32],
    pub thread_index: u32,
    pub active_numa_node: u32,
}

impl Default for DataPlaneBufferConfig {
    #[inline]
    fn default() -> Self {
        Self {
            buffer_slot_capacity: BUFFER_CACHE_LINE_SIZE,
            buffer_slots: 1024,
            frame_slots: DEFAULT_BUFFER_FRAME_POOL_SIZE,
            numa_nodes: &[0],
            thread_index: 0,
            active_numa_node: 0,
        }
    }
}

#[derive(Clone)]
pub struct DataPlaneBuffers {
    buffer_pools: StaticNumaTable<BufferPool, HAMMER_MAX_NUMA_NODES>,
    active_numa_node: u32,
    thread_index: u32,
    frames: FramePool,
    frame_slots: usize,
}

#[derive(Debug)]
pub struct DataPlaneBufferWorkerSeed {
    buffer_arenas: Vec<BufferPoolArena>,
    frame_slots: usize,
}

pub struct DataPlaneBufferWorkerConfig {
    pub seed: DataPlaneBufferWorkerSeed,
    pub thread_index: u32,
    pub numa_node: u32,
}

impl fmt::Debug for DataPlaneBuffers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneBuffers")
            .field("active_numa_node", &self.active_numa_node)
            .field("thread_index", &self.thread_index)
            .field("frame_capacity", &DEFAULT_BUFFER_FRAME_CAPACITY)
            .field("frame_slots", &self.frame_slots)
            .finish()
    }
}

impl From<&DataPlaneBuffers> for DataPlaneBufferWorkerSeed {
    fn from(buffers: &DataPlaneBuffers) -> Self {
        Self {
            buffer_arenas: buffers
                .buffer_arenas()
                .iter()
                .map(|(_, arena)| arena.clone())
                .collect(),
            frame_slots: buffers.frame_slots,
        }
    }
}

impl From<DataPlaneBufferWorkerConfig> for DataPlaneBuffers {
    fn from(config: DataPlaneBufferWorkerConfig) -> Self {
        let DataPlaneBufferWorkerConfig {
            seed,
            thread_index,
            numa_node,
        } = config;
        let DataPlaneBufferWorkerSeed {
            buffer_arenas: shared_arenas,
            frame_slots,
        } = seed;
        let mut arena_table = StaticNumaTable::new();
        for arena in shared_arenas {
            let inserted = arena_table.insert(arena.numa_node(), arena);
            debug_assert!(inserted.is_ok());
        }
        Self::with_arena_table(arena_table, frame_slots, thread_index, numa_node)
    }
}

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

#[inline]
fn next_pool_id() -> u64 {
    let id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
    if id == 0 || id == u64::MAX {
        // Nonzero namespace; never wrap to a previously used ID.
        abort_pool_id_namespace_exhausted();
    }
    id
}

#[inline(never)]
#[cold]
fn abort_pool_id_namespace_exhausted() -> ! {
    panic!("data-plane pool ID namespace exhausted");
}

/// Advance a slot generation. Retires the slot when the generation would wrap.
#[inline]
fn advance_generation(current: u32) -> Option<u32> {
    if current == u32::MAX {
        None
    } else {
        Some(current.wrapping_add(1).max(1))
    }
}

impl Frame<Next> {
    #[inline]
    fn frame(&self) -> &BufferFrame {
        match self.state.frame.as_ref() {
            Some(frame) => frame,
            None => abort_checked_out_frame(),
        }
    }

    #[inline]
    fn frame_mut(&mut self) -> &mut BufferFrame {
        match self.state.frame.as_mut() {
            Some(frame) => frame,
            None => abort_checked_out_frame(),
        }
    }

    #[inline]
    pub fn next(&self) -> NodeId {
        self.state.next
    }

    #[inline]
    pub fn into_pending(mut self) -> CoreResult<Frame<Pending>> {
        let frame = self
            .state
            .frame
            .take()
            .ok_or(DataPlaneError::FrameSlotCheckedOut)?;
        Ok(Frame {
            state: Pending {
                owner: self.state.owner.clone(),
                index: self.state.index,
                frame: Some(frame),
            },
        })
    }
}

impl Frame<Pending> {
    #[inline]
    fn frame(&self) -> &BufferFrame {
        match self.state.frame.as_ref() {
            Some(frame) => frame,
            None => abort_checked_out_frame(),
        }
    }

    #[inline]
    fn frame_mut(&mut self) -> &mut BufferFrame {
        match self.state.frame.as_mut() {
            Some(frame) => frame,
            None => abort_checked_out_frame(),
        }
    }

    #[inline]
    pub fn return_with_trace_release(mut self, release_trace: impl FnMut(u32)) {
        if let Some(frame) = self.state.frame.take() {
            self.state
                .owner
                .drop_owned_frame_with_trace(self.state.index, frame, release_trace);
        }
    }
}

#[cold]
fn abort_checked_out_frame() -> ! {
    std::process::abort()
}

impl Drop for Next {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.owner.drop_owned_frame(self.index, frame);
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.owner.drop_owned_frame(self.index, frame);
        }
    }
}

impl DataPlaneBuffers {
    /// VPP-style runtime thread index: zero for main, one-based for workers.
    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.thread_index
    }

    #[inline]
    pub fn new(config: DataPlaneBufferConfig) -> Self {
        Self::from_numa_config(config)
    }

    #[inline]
    fn from_numa_config(config: DataPlaneBufferConfig) -> Self {
        let mut buffer_arenas = StaticNumaTable::new();
        let configured_numa_nodes = if config.numa_nodes.is_empty() {
            &[0][..]
        } else {
            config.numa_nodes
        };
        for &numa_node in configured_numa_nodes {
            let inserted = buffer_arenas.insert(
                numa_node,
                BufferPoolArena::with_capacity_on_numa(
                    config.buffer_slot_capacity,
                    config.buffer_slots,
                    numa_node,
                ),
            );
            debug_assert!(inserted.is_ok());
        }
        let active_numa_node = Self::resolve_numa_node(&buffer_arenas, config.active_numa_node);
        Self::with_arena_table(
            buffer_arenas,
            config.frame_slots,
            config.thread_index,
            active_numa_node,
        )
    }

    #[inline]
    fn with_arena_table(
        buffer_arenas: StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
        frame_slots: usize,
        thread_index: u32,
        requested_numa_node: u32,
    ) -> Self {
        let active_numa_node = Self::resolve_numa_node(&buffer_arenas, requested_numa_node);
        Self {
            buffer_pools: Self::buffer_pools_from_arenas(&buffer_arenas),
            active_numa_node,
            thread_index,
            frames: FramePool::with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY, frame_slots),
            frame_slots,
        }
    }

    #[inline]
    pub fn with_active_buffer_arena(mut self, arena: BufferPoolArena) -> Self {
        let active_numa_node = arena.numa_node();
        let mut buffer_pools = StaticNumaTable::new();
        for index in 0..HAMMER_MAX_NUMA_NODES {
            let numa_node = index as u32;
            if numa_node == active_numa_node {
                continue;
            }
            let Some(pool) = self.buffer_pools.get(numa_node).cloned() else {
                continue;
            };
            let inserted = buffer_pools.insert(numa_node, pool);
            debug_assert!(inserted.is_ok());
        }
        let inserted = buffer_pools.insert(active_numa_node, BufferPool::with_arena(arena));
        debug_assert!(inserted.is_ok());
        self.buffer_pools = buffer_pools;
        self.active_numa_node = active_numa_node;
        self
    }

    #[inline]
    pub fn try_buffers(&self) -> CoreResult<&BufferPool> {
        self.buffer_pools
            .get(self.active_numa_node)
            .ok_or(DataPlaneError::ActiveNumaBufferPoolMissing.into())
    }

    #[inline]
    pub fn active_numa_node(&self) -> u32 {
        self.active_numa_node
    }

    #[inline]
    pub fn in_use_buffers(&self) -> usize {
        self.try_buffers().map(BufferPool::in_use).unwrap_or(0)
    }

    #[inline]
    pub fn cached_free_buffers(&self) -> usize {
        self.try_buffers()
            .map(BufferPool::cached_free_len)
            .unwrap_or(0)
    }

    #[inline]
    pub fn frames_in_use(&self) -> usize {
        self.frames.in_use()
    }

    #[inline]
    pub fn frame_capacity(&self) -> usize {
        DEFAULT_BUFFER_FRAME_CAPACITY
    }

    #[inline]
    pub fn frame_slots(&self) -> usize {
        self.frame_slots
    }

    #[inline]
    pub fn alloc_index(&self) -> CoreResult<Index> {
        self.try_buffers()?.alloc_index()
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> CoreResult<Index> {
        self.try_buffers()?.alloc_index_with_bytes(bytes)
    }

    #[inline]
    fn drop_index_owned(&self, index: Index) {
        let Ok(buffers) = self.try_buffers() else {
            return;
        };
        let mut cache = buffers.thread_cache.borrow_mut();
        buffers.arena.inner.write().free_chain(&mut cache, index);
    }

    #[inline]
    pub fn drop_index_owned_with_trace(&self, index: Index, release_trace: impl FnMut(u32)) {
        let Ok(buffers) = self.try_buffers() else {
            return;
        };
        let mut cache = buffers.thread_cache.borrow_mut();
        buffers
            .arena
            .inner
            .write()
            .free_chain_trace(&mut cache, index, release_trace);
    }

    #[inline]
    fn drop_frame_indices_with_trace(
        &self,
        frame: &mut BufferFrame,
        mut release_trace: impl FnMut(u32),
    ) {
        for index in frame.drain_indices() {
            self.drop_index_owned_with_trace(index, &mut release_trace);
        }
    }

    #[inline]
    pub fn attach_clone(&self, head: Index, tail: Index) -> CoreResult<()> {
        self.try_buffers()?.attach_clone(head, tail)
    }

    #[inline]
    pub fn chain_buffer(&self, head: Index, tail: Index) -> CoreResult<()> {
        self.try_buffers()?.chain_buffer(head, tail)
    }

    #[inline]
    pub fn prefetch_header(&self, index: Index) {
        if let Ok(buffers) = self.try_buffers() {
            buffers.prefetch_header(index);
        }
    }

    #[inline]
    pub fn prefetch_read(&self, index: Index) {
        if let Ok(buffers) = self.try_buffers() {
            buffers.prefetch_read(index);
        }
    }

    #[inline]
    pub fn prefetch_write(&self, index: Index) {
        if let Ok(buffers) = self.try_buffers() {
            buffers.prefetch_write(index);
        }
    }

    #[inline]
    fn drop_frame_indices(&self, frame: &mut BufferFrame) {
        for index in frame.drain_indices() {
            self.drop_index_owned(index);
        }
    }

    #[inline]
    fn drop_owned_frame(&self, index: Index, frame: BufferFrame) {
        let mut frame = frame;
        self.drop_frame_indices(&mut frame);
        frame.reset_for_pool_reuse();
        let _ = self.frames.return_taken_index(index, frame);
    }

    #[inline]
    fn drop_owned_frame_with_trace(
        &self,
        index: Index,
        frame: BufferFrame,
        release_trace: impl FnMut(u32),
    ) {
        let mut frame = frame;
        self.drop_frame_indices_with_trace(&mut frame, release_trace);
        frame.reset_for_pool_reuse();
        let _ = self.frames.return_taken_index(index, frame);
    }

    #[inline]
    fn alloc_frame(&self) -> CoreResult<(Index, BufferFrame)> {
        let index = self.frames.alloc_index()?;
        match self.frames.take_index(index) {
            Ok(frame) => Ok((index, frame)),
            Err(err) => {
                let buffers = self.try_buffers()?;
                let _ = self.frames.return_index(buffers, index);
                Err(err)
            }
        }
    }

    #[inline]
    pub fn get_next_frame(&self, next: NodeId) -> CoreResult<Frame<Next>> {
        let (index, frame) = self.alloc_frame()?;
        Ok(Frame {
            state: Next {
                owner: self.clone(),
                index,
                next,
                frame: Some(frame),
            },
        })
    }

    #[inline]
    pub fn get_buffer(&self, index: Index) -> CoreResult<BufferRef<'_>> {
        self.try_buffers()?.get(index)
    }

    #[inline]
    pub fn get_buffer_mut(&self, index: Index) -> CoreResult<BufferRefMut<'_>> {
        self.try_buffers()?.get_mut(index)
    }

    #[inline]
    pub fn chain(&self, index: Index) -> DataPlaneBufferChain<'_> {
        DataPlaneBufferChain::new(self.try_buffers(), index)
    }

    #[inline]
    pub fn node_error_code(&self, index: Index) -> CoreResult<Option<u16>> {
        self.try_buffers()?.node_error_code(index)
    }

    #[inline]
    pub fn current_config(&self, index: Index) -> CoreResult<NodeId> {
        self.try_buffers()?.current_config(index)
    }

    #[inline]
    pub fn set_current_config(&self, index: Index, next: NodeId) -> CoreResult<()> {
        self.try_buffers()?.set_current_config(index, next)
    }

    #[inline]
    pub fn advance(&self, index: Index, displacement: isize) -> CoreResult<()> {
        self.try_buffers()?.advance(index, displacement)
    }

    #[inline]
    pub fn append(&self, index: Index, bytes: &[u8]) -> CoreResult<()> {
        self.try_buffers()?.append(index, bytes)
    }

    #[inline]
    fn buffer_pools_from_arenas(
        buffer_arenas: &StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
    ) -> StaticNumaTable<BufferPool, HAMMER_MAX_NUMA_NODES> {
        let mut buffer_pools = StaticNumaTable::new();
        for index in 0..HAMMER_MAX_NUMA_NODES {
            let numa_node = index as u32;
            let Some(arena) = buffer_arenas.get(numa_node).cloned() else {
                continue;
            };
            let inserted = buffer_pools.insert(numa_node, BufferPool::with_arena(arena));
            debug_assert!(inserted.is_ok());
        }
        buffer_pools
    }

    #[inline]
    fn buffer_arenas(&self) -> StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES> {
        let mut buffer_arenas = StaticNumaTable::new();
        for index in 0..HAMMER_MAX_NUMA_NODES {
            let numa_node = index as u32;
            let Some(pool) = self.buffer_pools.get(numa_node) else {
                continue;
            };
            let inserted = buffer_arenas.insert(numa_node, pool.arena());
            debug_assert!(inserted.is_ok());
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
            let numa_node = index as u32;
            if buffer_arenas.get(numa_node).is_some() {
                return numa_node;
            }
        }
        0
    }
}

impl BufferPoolArena {
    #[inline]
    pub fn with_capacity(slot_capacity: usize, slots: usize) -> Self {
        Self::with_capacity_on_numa(slot_capacity, slots, 0)
    }

    #[inline]
    pub fn with_capacity_on_numa(slot_capacity: usize, slots: usize, numa_node: u32) -> Self {
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
        let region = PhysmemMap::create("buffers", region_size, 0, numa_node)
            .expect("buffer arena physmem map");
        unsafe {
            ptr::write_bytes(region.base(), 0, region.size());
        }

        let slot_states = Box::from_elem(
            total_slots,
            BufferSlot {
                generation: 0,
                allocated: false,
            },
        );
        let mut available_stack = Vec::with_capacity(slots);
        for i in 0..slots {
            let slot = u32::try_from(total_slots - i - 1).expect("buffer slot fits u32");
            available_stack.push(slot);
        }

        Self {
            inner: Arc::new(RwSpinlock::new(BufferPoolInner {
                pool_id: next_pool_id(),
                numa_node,
                slot_capacity,
                slot_stride,
                region_size: region.size(),
                region,
                slot_states,
                available_stack,
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
    pub fn numa_node(&self) -> u32 {
        self.inner.read().numa_node
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
    pub fn arena(&self) -> BufferPoolArena {
        self.arena.clone()
    }

    #[inline]
    pub fn pool_id(&self) -> u64 {
        self.arena.pool_id()
    }

    #[inline]
    pub fn numa_node(&self) -> u32 {
        self.arena.numa_node()
    }

    #[inline]
    pub fn slot_stride(&self) -> usize {
        self.arena.inner.read().slot_stride
    }

    #[inline]
    pub fn base_ptr(&self) -> *const u8 {
        self.arena.inner.read().region.base() as *const u8
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
    pub fn alloc_index(&self) -> CoreResult<Index> {
        let mut cache = self.thread_cache.borrow_mut();
        let mut arena = self.arena.inner.write();
        arena.alloc_empty_chain(&mut cache)
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> CoreResult<Index> {
        let mut cache = self.thread_cache.borrow_mut();
        let mut arena = self.arena.inner.write();
        arena.alloc_chain(&mut cache, bytes)
    }

    #[inline]
    pub fn attach_clone(&self, head: Index, tail: Index) -> CoreResult<()> {
        self.arena.inner.write().attach_clone(head, tail)
    }

    #[inline]
    pub fn chain_buffer(&self, head: Index, tail: Index) -> CoreResult<()> {
        self.arena.inner.write().chain_buffer(head, tail)
    }

    #[inline]
    pub fn prefetch_header(&self, index: Index) {
        self.arena.inner.read().prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: Index) {
        self.arena.inner.read().prefetch_read(index);
    }

    #[inline]
    pub fn prefetch_write(&self, index: Index) {
        self.arena.inner.read().prefetch_write(index);
    }

    #[inline]
    fn drop_frame_indices(&self, frame: &mut BufferFrame) {
        let mut cache = self.thread_cache.borrow_mut();
        let mut pool = self.arena.inner.write();
        for index in frame.drain_indices() {
            pool.free_chain(&mut cache, index);
        }
        pool.fold_in_use();
    }

    #[inline]
    pub fn get(&self, index: Index) -> CoreResult<BufferRef<'_>> {
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
    pub fn get_mut(&self, index: Index) -> CoreResult<BufferRefMut<'_>> {
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
    pub fn chain(&self, index: Index) -> impl Iterator<Item = CoreResult<BufferRef<'_>>> + '_ {
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
    pub fn current_data(&self, index: Index) -> CoreResult<usize> {
        Ok(self.arena.inner.read().buffer(index)?.current_data())
    }

    #[inline]
    pub fn current_len(&self, index: Index) -> CoreResult<usize> {
        Ok(self.arena.inner.read().buffer(index)?.current_len())
    }

    #[inline]
    pub fn current_ptr(&self, index: Index) -> CoreResult<*const u8> {
        Ok(self.arena.inner.read().buffer(index)?.current_ptr())
    }

    #[inline]
    pub fn current_mut_ptr(&self, index: Index) -> CoreResult<*mut u8> {
        let mut guard = self.arena.inner.write();
        guard.ensure_writable(index)?;
        Ok(guard.buffer_mut(index)?.current_mut_ptr())
    }

    #[inline]
    pub fn current_config(&self, index: Index) -> CoreResult<NodeId> {
        Ok(self.arena.inner.read().buffer(index)?.current_config())
    }

    #[inline]
    pub fn set_current_config(&self, index: Index, next: NodeId) -> CoreResult<()> {
        let mut guard = self.arena.inner.write();
        guard.ensure_header_exclusive(index)?;
        guard.buffer_mut(index)?.set_current_config(next);
        Ok(())
    }

    #[inline]
    pub fn node_error_code(&self, index: Index) -> CoreResult<Option<u16>> {
        Ok(self.arena.inner.read().buffer(index)?.node_error_code())
    }

    #[inline]
    pub fn advance(&self, index: Index, displacement: isize) -> CoreResult<()> {
        let mut pool = self.arena.inner.write();
        pool.advance(index, displacement)
    }

    #[inline]
    pub fn truncate_current(&self, index: Index, len: usize) -> CoreResult<()> {
        let mut pool = self.arena.inner.write();
        pool.ensure_writable(index)?;

        let mut walked = 0usize;
        let mut current = Some(index);
        let mut cut_buffer: Option<Index> = None;
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
    pub fn prepend(&self, index: Index, bytes: &[u8]) -> CoreResult<()> {
        let mut pool = self.arena.inner.write();
        pool.ensure_writable(index)?;
        pool.buffer_mut(index)?.prepend(bytes)
    }

    #[inline]
    pub fn append(&self, index: Index, bytes: &[u8]) -> CoreResult<()> {
        let mut cache = self.thread_cache.borrow_mut();
        self.arena
            .inner
            .write()
            .append_chain(&mut cache, index, bytes)
    }
}

impl FramePool {
    #[inline]
    fn with_capacity(frame_capacity: usize, slots: usize) -> Self {
        let mut available = Box::from_elem(slots, 0u32);
        for offset in 0..slots {
            available[offset] =
                u32::try_from(slots - offset - 1).expect("frame slot index fits u32");
        }
        let frame_slots = Box::from_fn(slots, |_| FrameSlot {
            generation: 0,
            allocated: false,
            frame: Some(BufferFrame::with_capacity(frame_capacity)),
        });
        let available_len = frame_slots.len();
        Self {
            inner: Rc::new(RefCell::new(FramePoolInner {
                pool_id: next_pool_id(),
                slots: frame_slots,
                available,
                available_len,
                in_use: 0,
            })),
        }
    }

    #[inline]
    fn in_use(&self) -> usize {
        self.inner.borrow().in_use
    }

    #[inline]
    fn alloc_index(&self) -> CoreResult<Index> {
        self.inner.borrow_mut().alloc_index()
    }

    #[inline]
    fn return_index(&self, buffers: &BufferPool, index: Index) -> CoreResult<()> {
        let mut pool = self.inner.borrow_mut();
        let frame = pool.frame_mut(index)?;
        buffers.drop_frame_indices(frame);
        frame.reset_for_pool_reuse();
        pool.release_index(index)
    }

    #[inline]
    fn take_index(&self, index: Index) -> CoreResult<BufferFrame> {
        self.inner.borrow_mut().take_frame(index)
    }

    #[inline]
    fn return_taken_index(&self, index: Index, frame: BufferFrame) -> CoreResult<()> {
        self.inner
            .borrow_mut()
            .return_frame_and_release(index, frame)
    }
}

impl FramePoolInner {
    #[inline]
    fn alloc_index(&mut self) -> CoreResult<Index> {
        loop {
            if self.available_len == 0 {
                return Err(DataPlaneError::FramePoolExhausted.into());
            }
            self.available_len -= 1;
            let slot = self.available[self.available_len];
            let pool_id = self.pool_id;
            let entry = self
                .slots
                .get_mut(slot as usize)
                .ok_or(DataPlaneError::IndexSlotOutOfBounds { pool_id, slot })?;
            let Some(generation) = advance_generation(entry.generation) else {
                // Slot retired at max generation; leave it out of available.
                continue;
            };
            entry.generation = generation;
            entry.allocated = true;
            let frame = entry
                .frame
                .as_mut()
                .ok_or(DataPlaneError::FrameSlotCheckedOut)?;
            frame.reset_for_pool_reuse();
            self.in_use += 1;
            return Ok(Index {
                pool_id,
                slot,
                generation,
            });
        }
    }

    #[inline]
    fn validate_index(&self, index: Index) -> CoreResult<()> {
        if index.pool_id != self.pool_id {
            return Err(DataPlaneError::ForeignIndex {
                expected_pool_id: self.pool_id,
                actual_pool_id: index.pool_id,
            }
            .into());
        }
        Ok(())
    }

    #[inline]
    fn entry_mut(&mut self, index: Index) -> CoreResult<&mut FrameSlot> {
        self.validate_index(index)?;
        let pool_id = self.pool_id;
        let entry = self.slots.get_mut(index.slot as usize).ok_or(
            DataPlaneError::IndexSlotOutOfBounds {
                pool_id,
                slot: index.slot,
            },
        )?;
        if entry.generation != index.generation {
            return Err(DataPlaneError::StaleIndex {
                slot: index.slot,
                index_generation: index.generation,
                current_generation: entry.generation,
            }
            .into());
        }
        if !entry.allocated {
            return Err(DataPlaneError::IndexSlotFree {
                pool_id,
                slot: index.slot,
            }
            .into());
        }
        Ok(entry)
    }

    #[inline]
    fn frame_mut(&mut self, index: Index) -> CoreResult<&mut BufferFrame> {
        self.entry_mut(index)?
            .frame
            .as_mut()
            .ok_or(DataPlaneError::FrameSlotCheckedOut.into())
    }

    #[inline]
    fn take_frame(&mut self, index: Index) -> CoreResult<BufferFrame> {
        self.entry_mut(index)?
            .frame
            .take()
            .ok_or(DataPlaneError::FrameSlotCheckedOut.into())
    }

    #[inline]
    fn release_index(&mut self, index: Index) -> CoreResult<()> {
        let entry = self.entry_mut(index)?;
        if entry.frame.is_none() {
            return Err(DataPlaneError::FrameSlotCheckedOut.into());
        }
        entry.allocated = false;
        self.in_use = self.in_use.saturating_sub(1);
        if self.available_len == self.available.len() {
            return Err(DataPlaneError::FramePoolAvailableOverflow.into());
        }
        self.available[self.available_len] = index.slot;
        self.available_len += 1;
        Ok(())
    }

    #[inline]
    fn return_frame_and_release(&mut self, index: Index, frame: BufferFrame) -> CoreResult<()> {
        let entry = self.entry_mut(index)?;
        if entry.frame.is_some() {
            return Err(DataPlaneError::FrameSlotAlreadyHasFrame.into());
        }
        entry.frame = Some(frame);
        entry.allocated = false;
        self.in_use = self.in_use.saturating_sub(1);
        if self.available_len == self.available.len() {
            return Err(DataPlaneError::FramePoolAvailableOverflow.into());
        }
        self.available[self.available_len] = index.slot;
        self.available_len += 1;
        Ok(())
    }
}

impl Deref for Frame<Next> {
    type Target = BufferFrame;

    fn deref(&self) -> &Self::Target {
        self.frame()
    }
}

impl DerefMut for Frame<Next> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.frame_mut()
    }
}

impl Deref for Frame<Pending> {
    type Target = BufferFrame;

    fn deref(&self) -> &Self::Target {
        self.frame()
    }
}

impl DerefMut for Frame<Pending> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.frame_mut()
    }
}

impl BufferPoolInner {
    #[inline]
    fn slot_index(&self, slot: u32) -> CoreResult<usize> {
        let slot_usize = usize::try_from(slot).expect("buffer slot index fits usize");
        if slot_usize >= self.total_slots {
            return Err(DataPlaneError::IndexSlotOutOfBounds {
                pool_id: self.pool_id,
                slot,
            }
            .into());
        }
        Ok(slot_usize)
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
        Ok(&self.slot_states[slot])
    }

    #[inline]
    fn slot_state_mut(&mut self, slot: u32) -> CoreResult<&mut BufferSlot> {
        let slot = self.slot_index(slot)?;
        Ok(&mut self.slot_states[slot])
    }

    #[inline]
    fn pop_available_slot(&mut self) -> Option<u32> {
        self.available_stack.pop()
    }

    #[inline]
    fn push_available_slot(&mut self, slot: u32) {
        debug_assert_ne!(slot, 0);
        debug_assert!(self.available_stack.len() < self.total_slots - 1);
        self.available_stack.push(slot);
    }

    #[inline]
    fn buffer_raw_ptr(&self, slot: u32) -> CoreResult<*mut Buffer> {
        let offset = self.slot_offset(slot)?;
        // SAFETY: `offset` is validated to land within the arena region and
        // each slot begins with an inline `Buffer` header.
        Ok(unsafe { self.region.base().add(offset).cast::<Buffer>() })
    }

    #[inline]
    fn data_raw_ptr(&self, slot: u32) -> CoreResult<*mut u8> {
        let offset = self
            .slot_offset(slot)?
            .checked_add(buffer_data_offset())
            .ok_or_else(|| CoreError::internal("buffer data pointer overflow"))?;
        // SAFETY: `offset` points at the inline data block within the validated
        // arena slot.
        Ok(unsafe { self.region.base().add(offset) })
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
    fn index_from_slot(&self, slot: u32) -> Option<Index> {
        Some(Index {
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
    fn next_buffer(&self, index: Index) -> CoreResult<Option<Index>> {
        Ok(self
            .buffer(index)?
            .next_buffer_slot()
            .and_then(|slot| self.index_from_slot(slot)))
    }

    #[inline]
    fn advance(&mut self, index: Index, displacement: isize) -> CoreResult<()> {
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

        let mut remaining = len;
        let mut current = Some(index);
        while remaining != 0 {
            let current_index = current
                .ok_or_else(|| CoreError::internal("buffer chain advance lost current segment"))?;
            let buffer = self.buffer(current_index)?;
            if remaining <= buffer.current_len() {
                break;
            }
            remaining -= buffer.current_len();
            current = self.next_buffer(current_index)?;
        }

        let mut remaining = len;
        let mut current = Some(index);
        while remaining != 0 {
            let current_index = current
                .ok_or_else(|| CoreError::internal("buffer chain advance lost current segment"))?;
            self.ensure_header_exclusive(current_index)?;
            let buffer = self.buffer(current_index)?;
            if remaining <= buffer.current_len() {
                break;
            }
            remaining -= buffer.current_len();
            current = self.next_buffer(current_index)?;
        }

        let mut remaining = len;
        let mut current = Some(index);
        while remaining != 0 {
            let current_index = current
                .ok_or_else(|| CoreError::internal("buffer chain advance lost current segment"))?;
            let next = self.next_buffer(current_index)?;
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
            current = next;
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
    fn alloc_chain(&mut self, cache: &mut BufferThreadCache, bytes: &[u8]) -> CoreResult<Index> {
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
    fn alloc_empty_chain(&mut self, cache: &mut BufferThreadCache) -> CoreResult<Index> {
        if self.slot_capacity == 0 {
            return Err(CoreError::internal("buffer slot capacity must be nonzero"));
        }
        self.alloc_slot_empty_fast(cache, 0)
    }

    #[inline]
    fn alloc_slot(&mut self, cache: &mut BufferThreadCache, bytes: &[u8]) -> CoreResult<Index> {
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
    ) -> CoreResult<Index> {
        let (slot, generation) = loop {
            let slot = match cache.pop() {
                Some(slot) => slot,
                None => {
                    self.refill_cache_batch(cache);
                    cache
                        .pop()
                        .ok_or_else(|| CoreError::internal("buffer pool exhausted"))?
                }
            };
            let entry = self.slot_state_mut(slot)?;
            match advance_generation(entry.generation) {
                Some(generation) => {
                    entry.generation = generation;
                    break (slot, generation);
                }
                None => {
                    // Slot retired at max generation; leave it out of the cache.
                }
            }
        };
        let reset_result = {
            let data_size = self.slot_capacity;
            let buffer = self.buffer_at_slot_mut(slot)?;
            reset(buffer, data_size)
        };
        if let Err(error) = reset_result {
            self.slot_state_mut(slot)?.allocated = false;
            cache.push(slot);
            return Err(error);
        }
        self.slot_state_mut(slot)?.allocated = true;
        self.bump_in_use();
        self.prefetch_next_cached_slot(cache);
        Ok(Index {
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
    ) -> CoreResult<Index> {
        let (slot, generation) = loop {
            let slot = match cache.pop() {
                Some(slot) => slot,
                None => {
                    self.refill_cache_batch(cache);
                    cache
                        .pop()
                        .ok_or_else(|| CoreError::internal("buffer pool exhausted"))?
                }
            };
            let entry = self.slot_state_mut(slot)?;
            match advance_generation(entry.generation) {
                Some(generation) => {
                    entry.generation = generation;
                    break (slot, generation);
                }
                None => {
                    // Slot retired at max generation; leave it out of the cache.
                }
            }
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
            self.slot_state_mut(slot)?.allocated = false;
            cache.push(slot);
            return Err(error);
        }
        self.slot_state_mut(slot)?.allocated = true;
        self.bump_in_use();
        self.prefetch_next_cached_slot(cache);
        Ok(Index {
            pool_id: self.pool_id,
            slot,
            generation,
        })
    }

    #[inline]
    fn validate_pool_index(&self, index: Index) -> CoreResult<()> {
        if index.pool_id != self.pool_id {
            return Err(DataPlaneError::ForeignIndex {
                expected_pool_id: self.pool_id,
                actual_pool_id: index.pool_id,
            }
            .into());
        }
        Ok(())
    }

    #[inline]
    fn validate_allocated_index(&self, index: Index) -> CoreResult<()> {
        self.validate_pool_index(index)?;
        let entry = self.slot_state(index.slot)?;
        if entry.generation != index.generation {
            return Err(DataPlaneError::StaleIndex {
                slot: index.slot,
                index_generation: index.generation,
                current_generation: entry.generation,
            }
            .into());
        }
        if !entry.allocated {
            return Err(DataPlaneError::IndexSlotFree {
                pool_id: self.pool_id,
                slot: index.slot,
            }
            .into());
        }
        Ok(())
    }

    #[inline]
    fn buffer(&self, index: Index) -> CoreResult<&Buffer> {
        self.validate_allocated_index(index)?;
        self.buffer_at_slot(index.slot)
    }

    #[inline]
    fn buffer_mut(&mut self, index: Index) -> CoreResult<&mut Buffer> {
        self.validate_allocated_index(index)?;
        self.buffer_at_slot_mut(index.slot)
    }

    #[inline]
    fn ensure_header_exclusive(&self, index: Index) -> CoreResult<()> {
        let buffer = self.buffer(index)?;
        if buffer.ref_count() == 1 {
            return Ok(());
        }
        Err(CoreError::internal(
            "shared buffer requires exclusive header ownership",
        ))
    }

    #[inline]
    fn ensure_writable(&self, index: Index) -> CoreResult<()> {
        self.ensure_header_exclusive(index)
    }

    #[inline]
    fn prefetch_header(&self, index: Index) {
        let Ok(buffer) = self.buffer(index) else {
            return;
        };
        prefetch_buffer_header(buffer);
    }

    #[inline]
    fn prefetch_read(&self, index: Index) {
        let Ok(buffer) = self.buffer(index) else {
            return;
        };
        prefetch_buffer_header(buffer);
        prefetch_buffer_cacheline1(buffer);
        prefetch_buffer_data(buffer);
    }

    #[inline]
    fn prefetch_write(&self, index: Index) {
        let Ok(buffer) = self.buffer(index) else {
            return;
        };
        prefetch_buffer_header_write(buffer);
        prefetch_buffer_cacheline1_write(buffer);
        prefetch_buffer_data_write(buffer);
    }

    #[inline]
    fn free_chain(&mut self, cache: &mut BufferThreadCache, index: Index) {
        self.free_chain_trace(cache, index, |_| {});
    }

    #[inline]
    fn free_chain_trace(
        &mut self,
        cache: &mut BufferThreadCache,
        index: Index,
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
                    let Ok(buffer) = self.buffer_at_slot_mut(index.slot) else {
                        return;
                    };
                    buffer.reset_for_free_fast(slot_capacity);
                }
                {
                    let Ok(entry) = self.slot_state_mut(slot) else {
                        return;
                    };
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
                let Ok(buffer) = self.buffer_at_slot_mut(index.slot) else {
                    return;
                };
                buffer.cacheline1.trace_handle = 0;
                buffer.reset_for_free(slot_capacity);
            }
            {
                let Ok(entry) = self.slot_state_mut(slot) else {
                    return;
                };
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
        let arena_free = self.available_stack.len();
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
    fn attach_clone(&mut self, head: Index, tail: Index) -> CoreResult<()> {
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
        index: Index,
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
    fn chain_buffer(&mut self, head: Index, tail: Index) -> CoreResult<()> {
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

pub struct DataPlaneBufferChain<'pool> {
    pool: Option<&'pool BufferPool>,
    next: Option<Index>,
    failed: bool,
    error: Option<CoreError>,
}

impl<'pool> DataPlaneBufferChain<'pool> {
    #[inline]
    fn new(pool: CoreResult<&'pool BufferPool>, index: Index) -> Self {
        match pool {
            Ok(pool) => Self {
                pool: Some(pool),
                next: Some(index),
                failed: false,
                error: None,
            },
            Err(error) => Self {
                pool: None,
                next: None,
                failed: false,
                error: Some(error),
            },
        }
    }
}

impl<'pool> Iterator for DataPlaneBufferChain<'pool> {
    type Item = CoreResult<BufferRef<'pool>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.error.take() {
            return Some(Err(error));
        }
        if self.failed {
            return None;
        }
        let current = self.next?;
        let pool = self.pool?;
        let guard = pool.arena.inner.read();
        self.next = match guard.next_buffer(current) {
            Ok(next) => next,
            Err(err) => {
                self.failed = true;
                return Some(Err(err));
            }
        };
        Some(Ok(BufferRef {
            guard: spinning_top::guard::RwSpinlockReadGuard::map(guard, |pool| {
                pool.buffer(current)
                    .expect("buffer index was validated before mapping")
            }),
        }))
    }
}

#[derive(Debug)]
pub struct BufferFrame {
    indices: Vec<Index>,
    /// Logical graph Frame maximum. Independent of the growable vector's
    /// reserved capacity.
    limit: usize,
    readiness: Rc<FrameReadiness>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferFramePairBatch {
    Pair([Index; 2]),
    Single(Index),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferFrameQuadBatch {
    Quad([Index; 4]),
    Pair([Index; 2]),
    Single(Index),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferFrameBatch {
    Quad([Index; 4]),
    Pair([Index; 2]),
    Single(Index),
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
    indices: [Option<Index>; 4],
    len: usize,
    offset: usize,
}

impl BufferFrameBatchIndices {
    #[inline]
    fn new(indices: &[Index]) -> Self {
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
    type Item = Index;

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
    indices: &'frame [Index],
    offset: usize,
}

#[derive(Debug, Clone)]
pub struct BufferFrameQuadBatchCursor<'frame> {
    indices: &'frame [Index],
    offset: usize,
}

#[derive(Debug, Clone)]
pub struct BufferFrameBatchCursor<'frame> {
    indices: &'frame [Index],
    offset: usize,
    width: BufferFrameBatchWidth,
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
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "frame capacity must be non-zero");
        Self {
            indices: Vec::with_capacity(capacity),
            limit: capacity,
            readiness: Rc::new(FrameReadiness::default()),
        }
    }

    #[inline]
    pub fn push_index(&mut self, index: Index) -> CoreResult<()> {
        if self.indices.len() == self.limit {
            return Err(DataPlaneError::BufferFrameCapacityExceeded.into());
        }
        self.indices.push(index);
        self.readiness.mark_pending();
        Ok(())
    }

    #[inline]
    pub fn push_indices(&mut self, indices: impl IntoIterator<Item = Index>) -> CoreResult<()> {
        let indices = indices.into_iter();
        let (lower, upper) = indices.size_hint();
        if let Some(upper) = upper {
            if self.indices.len() + upper > self.limit {
                return Err(DataPlaneError::BufferFrameCapacityExceeded.into());
            }
        } else if self.indices.len() + lower > self.limit {
            return Err(DataPlaneError::BufferFrameCapacityExceeded.into());
        }

        let original_len = self.indices.len();
        for index in indices {
            if self.indices.len() == self.limit {
                self.indices.truncate(original_len);
                return Err(DataPlaneError::BufferFrameCapacityExceeded.into());
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
        self.limit
    }

    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        self.capacity().saturating_sub(self.len())
    }

    #[inline]
    fn reset_for_pool_reuse(&mut self) {
        self.indices.clear();
        self.readiness.reset_for_pool_reuse();
    }

    #[inline]
    pub fn indices(&self) -> &[Index] {
        &self.indices
    }

    #[inline]
    pub fn pending_indices(&self) -> &[Index] {
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
    pub fn batch_cursor(
        &self,
        width: impl BufferFrameBatchWidthPolicy,
    ) -> BufferFrameBatchCursor<'_> {
        BufferFrameBatchCursor {
            indices: self.pending_indices(),
            offset: 0,
            width: width.buffer_frame_batch_width(),
        }
    }

    #[inline]
    pub fn iter_indices(&self) -> slice::Iter<'_, Index> {
        self.indices.iter()
    }

    #[inline]
    fn drain_indices(&mut self) -> hammer_infra::vec::Drain<'_, Index> {
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
    pub fn retain_indices(
        &mut self,
        mut keep: impl FnMut(Index) -> CoreResult<bool>,
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
        width: impl BufferFrameBatchWidthPolicy,
        mut keep: impl FnMut(Index) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        match width.buffer_frame_batch_width() {
            BufferFrameBatchWidth::Octo => self.retain_indices_octo(&mut keep),
            BufferFrameBatchWidth::Quad => self.retain_indices_quad(&mut keep),
            BufferFrameBatchWidth::Pair => self.retain_indices_pair(&mut keep),
        }
    }

    #[inline(always)]
    pub fn retain_indices_batched_with_prefetch(
        &mut self,
        width: impl BufferFrameBatchWidthPolicy,
        mut prefetch: impl FnMut(Index),
        mut keep: impl FnMut(Index) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        match width.buffer_frame_batch_width() {
            BufferFrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch(&mut prefetch, &mut keep)
            }
            BufferFrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch(&mut prefetch, &mut keep)
            }
            BufferFrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch(&mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub fn retain_indices_batched_with_prefetch_state<S>(
        &mut self,
        width: impl BufferFrameBatchWidthPolicy,
        state: &mut S,
        mut prefetch: impl FnMut(&mut S, Index),
        mut keep: impl FnMut(&mut S, Index) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        match width.buffer_frame_batch_width() {
            BufferFrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
            BufferFrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
            BufferFrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub fn buffer_node_inline<S>(
        &mut self,
        width: impl BufferFrameBatchWidthPolicy,
        state: &mut S,
        mut prefetch: impl FnMut(&mut S, Index),
        mut keep: impl FnMut(&mut S, Index) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        match width.buffer_frame_batch_width() {
            BufferFrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
            BufferFrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
            BufferFrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub fn rewrite_indices_batched(
        &mut self,
        width: impl BufferFrameBatchWidthPolicy,
        mut rewrite: impl FnMut(Index) -> CoreResult<Option<Index>>,
    ) -> CoreResult<()> {
        match width.buffer_frame_batch_width() {
            BufferFrameBatchWidth::Quad => self.rewrite_indices_quad(&mut rewrite),
            BufferFrameBatchWidth::Pair => self.rewrite_indices_pair(&mut rewrite),
            BufferFrameBatchWidth::Octo => self.rewrite_indices_octo(&mut rewrite),
        }
    }

    #[inline(always)]
    fn retain_indices_quad(
        &mut self,
        keep: &mut impl FnMut(Index) -> CoreResult<bool>,
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
        prefetch: &mut impl FnMut(Index),
        keep: &mut impl FnMut(Index) -> CoreResult<bool>,
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
        keep: &mut impl FnMut(Index) -> CoreResult<bool>,
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
        keep: &mut impl FnMut(Index) -> CoreResult<bool>,
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
        prefetch: &mut impl FnMut(Index),
        keep: &mut impl FnMut(Index) -> CoreResult<bool>,
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
        prefetch: &mut impl FnMut(&mut S, Index),
        keep: &mut impl FnMut(&mut S, Index) -> CoreResult<bool>,
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
        prefetch: &mut impl FnMut(&mut S, Index),
        keep: &mut impl FnMut(&mut S, Index) -> CoreResult<bool>,
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
        prefetch: &mut impl FnMut(&mut S, Index),
        keep: &mut impl FnMut(&mut S, Index) -> CoreResult<bool>,
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
        prefetch: &mut impl FnMut(&mut S, Index),
        keep: &mut impl FnMut(&mut S, Index) -> CoreResult<bool>,
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
    fn rewrite_indices_quad(
        &mut self,
        rewrite: &mut impl FnMut(Index) -> CoreResult<Option<Index>>,
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
        rewrite: &mut impl FnMut(Index) -> CoreResult<Option<Index>>,
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
        rewrite: &mut impl FnMut(Index) -> CoreResult<Option<Index>>,
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
        keep: &mut impl FnMut(Index) -> CoreResult<bool>,
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
        keep: &mut impl FnMut(&mut S, Index) -> CoreResult<bool>,
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
        keep: &mut impl FnMut(&mut S, Index) -> CoreResult<bool>,
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
    fn rewrite_one(
        &mut self,
        read: usize,
        write: &mut usize,
        rewrite: &mut impl FnMut(Index) -> CoreResult<Option<Index>>,
    ) -> CoreResult<()> {
        let index = self.indices[read];
        if let Some(index) = rewrite(index)? {
            self.indices[*write] = index;
            *write += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn prefetch_indices(&self, offset: usize, width: usize, prefetch: &mut impl FnMut(Index)) {
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
        prefetch: &mut impl FnMut(&mut S, Index),
    ) {
        let end = (offset + width).min(self.indices.len());
        for index in self.indices[offset..end].iter().copied() {
            prefetch(state, index);
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
}

impl BufferFramePairBatchCursor<'_> {
    #[inline]
    pub fn prefetch_next_pair_with(&self, mut prefetch: impl FnMut(Index)) {
        for index in self.indices[self.offset..].iter().take(2).copied() {
            prefetch(index);
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
    pub fn prefetch_next_quad_with(&self, mut prefetch: impl FnMut(Index)) {
        for index in self.indices[self.offset..].iter().take(4).copied() {
            prefetch(index);
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
    pub fn prefetch_next_with(&self, mut prefetch: impl FnMut(Index)) {
        let width = match self.width {
            BufferFrameBatchWidth::Octo => 8,
            BufferFrameBatchWidth::Quad => 4,
            BufferFrameBatchWidth::Pair => 2,
        };
        for index in self.indices[self.offset..].iter().take(width).copied() {
            prefetch(index);
        }
    }
}

impl Iterator for BufferFrameBatchCursor<'_> {
    type Item = BufferFrameBatch;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.indices.len().saturating_sub(self.offset);
        match self.width {
            BufferFrameBatchWidth::Octo | BufferFrameBatchWidth::Quad if remaining >= 4 => {
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

#[cfg(test)]
mod index_identity_tests {
    use super::*;
    use crate::error::CoreError;

    #[test]
    fn advance_generation_retires_at_max() {
        assert_eq!(advance_generation(0), Some(1));
        assert_eq!(advance_generation(u32::MAX - 1), Some(u32::MAX));
        assert_eq!(advance_generation(u32::MAX), None);
    }

    #[test]
    fn buffer_pool_reports_out_of_bounds_slot_facts() {
        let pool = BufferPool::with_capacity(64, 1);
        let foreign = Index {
            pool_id: pool.pool_id(),
            slot: 99,
            generation: 1,
        };
        match pool.get(foreign).map(|_| ()).unwrap_err() {
            CoreError::DataPlane(DataPlaneError::IndexSlotOutOfBounds { pool_id, slot }) => {
                assert_eq!(pool_id, pool.pool_id());
                assert_eq!(slot, 99);
            }
            other => panic!("expected IndexSlotOutOfBounds, got {other:?}"),
        }
    }

    #[test]
    fn buffer_pool_retires_slot_at_max_generation() {
        let pool = BufferPool::with_capacity(64, 2);
        let first = pool.alloc_index().expect("first");
        let second = pool.alloc_index().expect("second");
        let retired_slot = first.slot;
        {
            let mut cache = pool.thread_cache.borrow_mut();
            let mut inner = pool.arena.inner.write();
            inner.free_chain(&mut cache, first);
            inner.free_chain(&mut cache, second);
            // Force the next pop of `retired_slot` to hit max-generation retirement.
            let entry = inner.slot_state_mut(retired_slot).expect("slot");
            entry.generation = u32::MAX;
        }
        let reused = pool.alloc_index().expect("alloc after retirement");
        assert_ne!(reused.slot(), retired_slot);
        assert!(
            pool.get(Index {
                pool_id: pool.pool_id(),
                slot: retired_slot,
                generation: u32::MAX,
            })
            .is_err()
        );
        {
            let mut cache = pool.thread_cache.borrow_mut();
            pool.arena.inner.write().free_chain(&mut cache, reused);
        }
    }

    #[test]
    fn next_pool_id_never_returns_zero() {
        let a = next_pool_id();
        let b = next_pool_id();
        assert_ne!(a, 0);
        assert_ne!(b, 0);
        assert_ne!(a, b);
    }
}

#[cfg(test)]
mod frame_capacity_tests {
    use super::*;

    fn test_index(slot: u32) -> Index {
        Index {
            pool_id: 1,
            slot,
            generation: 1,
        }
    }

    #[test]
    fn buffer_frame_test_capacity_is_crate_private_logical_limit() {
        let mut frame = BufferFrame::with_capacity(2);
        assert_eq!(frame.capacity(), 2);
        frame.push_index(test_index(0)).expect("first");
        frame.push_index(test_index(1)).expect("second");
        assert!(frame.push_index(test_index(2)).is_err());
        assert_eq!(frame.indices(), &[test_index(0), test_index(1)]);
    }

    #[test]
    fn production_frame_accepts_256_and_rejects_257th() {
        let mut frame = BufferFrame::with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY);
        for slot in 0..DEFAULT_BUFFER_FRAME_CAPACITY as u32 {
            frame
                .push_index(test_index(slot))
                .expect("push within production limit");
        }
        assert_eq!(frame.len(), DEFAULT_BUFFER_FRAME_CAPACITY);
        assert!(frame.push_index(test_index(u32::MAX)).is_err());
        assert_eq!(frame.len(), DEFAULT_BUFFER_FRAME_CAPACITY);
    }

    #[test]
    fn production_bulk_push_is_atomic_against_logical_limit() {
        let mut frame = BufferFrame::with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY);
        for slot in 0..(DEFAULT_BUFFER_FRAME_CAPACITY as u32 - 1) {
            frame.push_index(test_index(slot)).expect("seed");
        }
        let before = frame.indices().to_vec();
        let batch = [test_index(1000), test_index(1001)];
        assert!(frame.push_indices(batch).is_err());
        assert_eq!(frame.indices(), before.as_slice());
    }
}
