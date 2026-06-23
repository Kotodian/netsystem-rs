use std::cell::{Cell, Ref, RefCell, RefMut};
use std::fmt;
use std::future::Future;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::{align::CacheLine, boxed::Slice, prefetch::prefetch_read_l1, vec::Vec};

use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HandoffIndices};
use crate::instruction_set::{DataPlaneInstructionSet, FrameBatchWidth};
use crate::node::{NodeHandle, NodeId, NodeNext, NodeRuntime};
use crate::packet_buffer::NetworkOpaque;
use crate::packet_buffer::{
    PACKET_BUFFER_INVALID_INDEX, PacketBufferCacheline0, PacketBufferCacheline1, PacketBufferFlags,
    PrimaryOpaque, SecondaryOpaque,
};
use crate::trace::{DataPlaneTrace, PacketTrace, TraceControlHandle};

pub const DEFAULT_BUFFER_FRAME_CAPACITY: usize = 256;
pub const DEFAULT_BUFFER_FRAME_POOL_SIZE: usize = 64;
pub const BUFFER_CACHE_LINE_SIZE: usize = 64;
pub const CURRENT_CHAIN_IO_SEGMENT_CAPACITY: usize = 64;
pub const DEFAULT_PACKET_HEADROOM: usize = 256;

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

pub type BufferFlags = PacketBufferFlags;

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
    cacheline0: PacketBufferCacheline0,
    cacheline1: PacketBufferCacheline1,
    storage: Slice<u8>,
    storage_view: NonNull<u8>,
    storage_capacity: usize,
    storage_owner_slot: u32,
}

const _: () = assert!(std::mem::align_of::<Buffer>() == BUFFER_CACHE_LINE_SIZE);

impl Buffer {
    #[inline]
    fn with_slot_capacity(slot_capacity: usize) -> Self {
        let mut storage = Slice::from_elem(slot_capacity, 0);
        Self {
            cacheline0: PacketBufferCacheline0::default(),
            cacheline1: PacketBufferCacheline1::default(),
            storage_view: NonNull::new(storage.as_mut_ptr()).unwrap_or_else(NonNull::dangling),
            storage_capacity: storage.len(),
            storage_owner_slot: PACKET_BUFFER_INVALID_INDEX,
            storage,
        }
    }

    #[inline]
    fn reset(&mut self, slot: u32, bytes: &[u8]) -> CoreResult<()> {
        self.reset_storage_view(slot);
        if bytes.len() > self.storage_capacity {
            return Err(CoreError::internal(format!(
                "buffer bytes exceed slot capacity: {} > {}",
                bytes.len(),
                self.storage_capacity
            )));
        }
        let headroom = 0usize;
        let current_len = u16::try_from(bytes.len())
            .map_err(|_| CoreError::internal("buffer length exceeds u16"))?;
        self.cacheline0 = PacketBufferCacheline0::default();
        self.cacheline0.current_data = i16::try_from(headroom)
            .map_err(|_| CoreError::internal("buffer current_data exceeds i16"))?;
        self.cacheline0.current_length = current_len;
        self.cacheline0.ref_count = 1;
        self.cacheline1 = PacketBufferCacheline1::default();
        let start = headroom;
        let end = start + bytes.len();
        self.storage_view_mut()[start..end].copy_from_slice(bytes);
        Ok(())
    }

    #[inline]
    fn reset_for_free(&mut self) {
        self.cacheline0 = PacketBufferCacheline0::default();
        self.cacheline1 = PacketBufferCacheline1::default();
        self.storage_owner_slot = PACKET_BUFFER_INVALID_INDEX;
    }

    #[inline]
    fn reset_storage_view(&mut self, slot: u32) {
        self.storage_view = NonNull::new(self.storage.as_mut_ptr()).unwrap_or_else(NonNull::dangling);
        self.storage_capacity = self.storage.len();
        self.storage_owner_slot = slot;
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
        &mut self.cacheline1.opaque2
    }

    #[inline]
    pub fn packet_cursor(&self) -> BufferPacketCursor {
        unsafe { transmute::<&PrimaryOpaque, &NetworkOpaque>(&self.cacheline0.opaque) }
            .packet_cursor()
    }

    #[inline]
    pub fn set_packet_cursor(&mut self, cursor: BufferPacketCursor) {
        unsafe { transmute::<&mut PrimaryOpaque, &mut NetworkOpaque>(&mut self.cacheline0.opaque) }
            .set_packet_cursor(cursor)
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
    pub fn handoff_source_worker(&self) -> Option<DataWorkerId> {
        unsafe { transmute::<&PrimaryOpaque, &NetworkOpaque>(&self.cacheline0.opaque) }
            .handoff_source_worker()
            .map(|worker| DataWorkerId::new(u32::from(worker)))
    }

    #[inline]
    pub fn trace_handle(&self) -> Option<u32> {
        (self.cacheline1.trace_handle != 0).then_some(self.cacheline1.trace_handle)
    }

    #[inline]
    pub fn set_handoff_source_worker(&mut self, worker: DataWorkerId) {
        unsafe { transmute::<&mut PrimaryOpaque, &mut NetworkOpaque>(&mut self.cacheline0.opaque) }
            .set_handoff_source_worker(Some(worker.slot() as u16));
    }

    #[inline]
    pub fn set_trace_handle(&mut self, handle: u32) {
        self.cacheline1.trace_handle = handle;
    }

    #[inline]
    pub fn take_trace_handle(&mut self) -> Option<u32> {
        let handle = self.trace_handle();
        self.cacheline1.trace_handle = 0;
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
        self.cacheline0.flags
    }

    #[inline]
    pub fn current_data(&self) -> usize {
        usize::try_from(self.cacheline0.current_data)
            .expect("buffer current_data must remain non-negative")
    }

    #[inline]
    fn storage_owner_slot(&self) -> u32 {
        self.storage_owner_slot
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
        let start = self.current_data();
        let end = start + self.current_len();
        &self.storage_view()[start..end]
    }

    #[inline]
    pub fn current_ptr(&self) -> *const u8 {
        self.current().as_ptr()
    }

    #[inline]
    pub fn current_mut(&mut self) -> &mut [u8] {
        let start = self.current_data();
        let end = start + self.current_len();
        &mut self.storage_view_mut()[start..end]
    }

    #[inline]
    pub fn writable_tail_mut(&mut self) -> &mut [u8] {
        let start = self.current_data() + self.current_len();
        &mut self.storage_view_mut()[start..]
    }

    #[inline]
    pub fn commit_writable_tail(&mut self, len: usize) -> CoreResult<()> {
        if len > self.available_tail() {
            return Err(CoreError::internal("buffer commit exceeds writable tail"));
        }
        self.set_current_len(self.current_len() + len)?;
        Ok(())
    }

    #[inline]
    pub fn current_mut_ptr(&mut self) -> *mut u8 {
        self.current_mut().as_mut_ptr()
    }

    #[inline]
    fn available_tail(&self) -> usize {
        self.storage_capacity
            .saturating_sub(self.current_data() + self.current_len())
    }

    #[inline]
    fn append_in_place(&mut self, bytes: &[u8]) -> usize {
        let take = bytes.len().min(self.available_tail());
        if take == 0 {
            return 0;
        }
        let start = self.current_data() + self.current_len();
        let end = start + take;
        self.storage_view_mut()[start..end].copy_from_slice(&bytes[..take]);
        self.set_current_len(self.current_len() + take)
            .expect("buffer append keeps current length within u16");
        take
    }

    #[inline]
    fn set_current_data(&mut self, len: usize) -> CoreResult<()> {
        self.cacheline0.current_data = i16::try_from(len)
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
    fn storage_view(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.storage_view.as_ptr(), self.storage_capacity) }
    }

    #[inline]
    fn storage_view_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.storage_view.as_ptr(), self.storage_capacity) }
    }

    #[inline]
    fn set_next_buffer(&mut self, next: Option<BufferIndex>) {
        self.cacheline0.next_buffer = next.map_or(PACKET_BUFFER_INVALID_INDEX, BufferIndex::slot);
        if next.is_some() {
            self.cacheline0.flags.insert(BufferFlags::NEXT_PRESENT);
        } else {
            self.cacheline0.flags.remove(BufferFlags::NEXT_PRESENT);
        }
    }

    #[inline]
    fn take_next_buffer_slot(&mut self) -> Option<u32> {
        let next = self.next_buffer_slot();
        self.set_next_buffer(None);
        next
    }

    #[inline]
    fn set_total_len_not_including_first(&mut self, len: usize) -> CoreResult<()> {
        self.cacheline1.total_length_not_including_first = u32::try_from(len)
            .map_err(|_| CoreError::internal("buffer chain tail length exceeds u32"))?;
        Ok(())
    }
}

#[derive(Debug)]
#[repr(C, align(64))]
struct BufferSlot {
    generation: u32,
    allocated: bool,
    header_live: bool,
    storage_ref_count: u8,
    buffer: Buffer,
}

#[derive(Debug)]
struct BufferPoolInner {
    pool_id: u64,
    slot_capacity: usize,
    slots: Vec<CacheLine<BufferSlot>>,
    free: Vec<u32>,
    in_use: usize,
}

#[derive(Debug, Clone)]
pub struct BufferPoolArena {
    inner: Rc<RefCell<BufferPoolInner>>,
}

#[derive(Debug, Clone)]
pub struct BufferThreadCache {
    free: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct BufferPool {
    arena: BufferPoolArena,
    thread_cache: Rc<RefCell<BufferThreadCache>>,
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
    free: Vec<u32>,
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

#[derive(Debug, Clone)]
pub struct DataPlaneBuffers {
    buffers: BufferPool,
    frames: FramePool,
    instruction_set: DataPlaneInstructionSet,
    trace: DataPlaneTrace,
}

#[derive(Debug)]
pub struct DataPlaneRuntime {
    buffers: DataPlaneBuffers,
    nodes: NodeRuntime,
    current_node: Rc<Cell<Option<NodeId>>>,
    handoff: Option<DataPlaneHandoffWorker>,
}

impl Clone for DataPlaneRuntime {
    fn clone(&self) -> Self {
        Self {
            buffers: self.buffers.clone(),
            nodes: self.nodes.clone(),
            current_node: Rc::clone(&self.current_node),
            handoff: self.handoff.clone(),
        }
    }
}

impl Deref for DataPlaneRuntime {
    type Target = DataPlaneBuffers;

    fn deref(&self) -> &Self::Target {
        &self.buffers
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
        Self::with_buffer_arena_and_frame_capacity(
            BufferPoolArena::with_capacity(buffer_slot_capacity, buffer_slots),
            frame_capacity,
            frame_slots,
            instruction_set,
        )
    }

    #[inline]
    pub fn with_buffer_arena_and_frame_capacity(
        buffer_arena: BufferPoolArena,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        Self {
            buffers: BufferPool::with_arena(buffer_arena),
            frames: FramePool::with_capacity(frame_capacity, frame_slots),
            instruction_set,
            trace: DataPlaneTrace::default(),
        }
    }

    #[inline]
    fn with_handoff(mut self, handoff: DataPlaneHandoffWorker) -> Self {
        self.buffers =
            BufferPool::with_arena(handoff.set_or_get_buffer_arena(self.buffers.arena()));
        self
    }

    #[inline]
    pub fn buffers(&self) -> &BufferPool {
        &self.buffers
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
        self.buffers.in_use()
    }

    #[inline]
    pub fn cached_free_buffers(&self) -> usize {
        self.buffers.cached_free_len()
    }

    #[inline]
    pub fn frames_in_use(&self) -> usize {
        self.frames.in_use()
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
        let mut cache = self.buffers.thread_cache.borrow_mut();
        let handles = self
            .buffers
            .arena
            .inner
            .borrow_mut()
            .free_chain_collect_trace_handles(&mut cache, index);
        self.finalize_trace_handles(handles);
    }

    #[inline]
    pub fn attach_clone(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.buffers.attach_clone(head, tail)
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
        self.frames.free_taken_index(&self.buffers, index, frame)
    }

    #[inline]
    pub fn alloc_pooled_frame(&self) -> CoreResult<PooledBufferFrame> {
        let index = self.frames.alloc_index()?;
        match self.frames.take_index(index) {
            Ok(frame) => Ok(PooledBufferFrame { index, frame }),
            Err(err) => {
                let _ = self.frames.free_index(&self.buffers, index);
                Err(err)
            }
        }
    }

    #[inline]
    pub fn release_pooled_frame(&self, frame: PooledBufferFrame) -> CoreResult<()> {
        let PooledBufferFrame { index, frame } = frame;
        let mut frame = frame;
        self.free_frame(&mut frame);
        self.frames.free_taken_index(&self.buffers, index, frame)
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
        self.frames.free_taken_index(&self.buffers, index, frame)
    }

    #[inline]
    pub fn get_buffer(&self, index: BufferIndex) -> CoreResult<BufferRef<'_>> {
        self.buffers.get(index)
    }

    #[inline]
    pub fn get_buffer_mut(&self, index: BufferIndex) -> CoreResult<BufferRefMut<'_>> {
        self.buffers.get_mut(index)
    }

    #[inline]
    pub fn buffer_batch_mut(&self) -> BufferBatchMut<'_> {
        self.buffers.batch_mut()
    }

    #[inline]
    pub fn packet_cursor(&self, index: BufferIndex) -> CoreResult<BufferPacketCursor> {
        self.buffers.packet_cursor(index)
    }

    #[inline]
    pub fn advance(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        self.buffers.advance(index, len)
    }

    #[inline]
    pub fn current_data(&self, index: BufferIndex) -> CoreResult<usize> {
        self.buffers.current_data(index)
    }

    #[inline]
    pub fn current_len(&self, index: BufferIndex) -> CoreResult<usize> {
        self.buffers.current_len(index)
    }

    #[inline]
    pub fn total_len_not_including_first(&self, index: BufferIndex) -> CoreResult<usize> {
        self.buffers.total_len_not_including_first(index)
    }

    #[inline]
    pub fn current_ptr(&self, index: BufferIndex) -> CoreResult<*const u8> {
        self.buffers.current_ptr(index)
    }

    #[inline]
    pub fn current_mut_ptr(&self, index: BufferIndex) -> CoreResult<*mut u8> {
        self.buffers.current_mut_ptr(index)
    }

    #[inline]
    pub fn node_error_code(&self, index: BufferIndex) -> CoreResult<Option<u16>> {
        self.buffers.node_error_code(index)
    }

    #[inline]
    pub fn handoff_source_worker(&self, index: BufferIndex) -> CoreResult<Option<DataWorkerId>> {
        self.buffers.handoff_source_worker(index)
    }

    #[inline]
    pub fn mark_handoff_source_worker(
        &self,
        index: BufferIndex,
        worker: DataWorkerId,
    ) -> CoreResult<()> {
        self.buffers.mark_handoff_source_worker(index, worker)
    }

    #[inline]
    pub fn current_config(&self, index: BufferIndex) -> CoreResult<crate::NodeId> {
        self.buffers.current_config(index)
    }

    #[inline]
    pub fn set_current_config(&self, index: BufferIndex, next: crate::NodeId) -> CoreResult<()> {
        self.buffers.set_current_config(index, next)
    }

    #[inline]
    pub fn truncate_current(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        self.buffers.truncate_current(index, len)
    }

    #[inline]
    pub fn prepend(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        self.buffers.prepend(index, bytes)
    }

    #[inline]
    pub fn append(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        self.buffers.append(index, bytes)
    }

    #[inline]
    pub fn detach_next(&self, index: BufferIndex) -> CoreResult<Option<BufferIndex>> {
        self.buffers.detach_next(index)
    }

    #[inline]
    pub fn append_existing_chain(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.buffers.append_existing_chain(head, tail)
    }

    #[inline]
    pub fn truncate_chain(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        self.buffers.truncate_chain(index, len)
    }

    #[inline]
    pub fn current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.buffers.current(index)
    }

    #[inline]
    pub fn copy_current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.buffers.copy_current(index)
    }

    #[inline]
    pub fn is_chained(&self, index: BufferIndex) -> CoreResult<bool> {
        self.buffers.is_chained(index)
    }

    #[inline]
    pub fn copy_packet(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.buffers.copy_packet(index)
    }

    #[inline]
    pub fn copy_current_chain(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.buffers.copy_current_chain(index)
    }

    #[inline]
    pub fn with_current_chain_io_segments<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&[&[u8]], usize) -> CoreResult<R>,
    ) -> CoreResult<R> {
        self.buffers.with_current_chain_io_segments(index, f)
    }

    fn finalize_trace_handles(&self, handles: Vec<u32>) {
        for handle in handles {
            self.trace.finalize(handle);
        }
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
        Self::with_buffer_arena_and_frame_capacity(
            BufferPoolArena::with_capacity(buffer_slot_capacity, buffer_slots),
            frame_capacity,
            frame_slots,
            instruction_set,
        )
    }

    #[inline]
    pub fn with_buffer_arena_and_frame_capacity(
        buffer_arena: BufferPoolArena,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        Self {
            buffers: DataPlaneBuffers::with_buffer_arena_and_frame_capacity(
                buffer_arena,
                frame_capacity,
                frame_slots,
                instruction_set,
            ),
            nodes: NodeRuntime::default(),
            current_node: Rc::new(Cell::new(None)),
            handoff: None,
        }
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
    pub fn with_handoff_capacities(
        worker: DataWorkerId,
        handoff: DataPlaneHandoffWorker,
        frame_capacity: usize,
        frame_slots: usize,
        instruction_set: DataPlaneInstructionSet,
    ) -> Self {
        debug_assert_eq!(worker, handoff.worker());
        let runtime = Self::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            frame_capacity,
            frame_slots,
            instruction_set,
        );
        Self::with_handoff(runtime, worker, handoff)
    }

    #[inline]
    pub fn packet_buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    #[inline]
    pub fn nodes(&self) -> &NodeRuntime {
        &self.nodes
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
        let mut payload_bytes = std::vec::Vec::new();
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
        for index in frame.pending_indices().iter().copied() {
            self.mark_handoff_source_worker(index, handoff.worker())?;
        }
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
        for index in indices.iter().copied() {
            self.mark_handoff_source_worker(index, handoff.worker())?;
        }
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
        self.mark_handoff_source_worker(index, handoff.worker())?;
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
        let free = (0..slots)
            .rev()
            .map(|slot| u32::try_from(slot).expect("buffer slot index fits u32"))
            .collect();
        let slots = (0..slots)
            .map(|_| {
                CacheLine::new(BufferSlot {
                    generation: 0,
                    allocated: false,
                    header_live: false,
                    storage_ref_count: 0,
                    buffer: Buffer::with_slot_capacity(slot_capacity),
                })
            })
            .collect();
        Self {
            inner: Rc::new(RefCell::new(BufferPoolInner {
                pool_id: next_buffer_pool_id(),
                slot_capacity,
                slots,
                free,
                in_use: 0,
            })),
        }
    }

    #[inline]
    pub fn pool_id(&self) -> u64 {
        self.inner.borrow().pool_id
    }
}

impl BufferThreadCache {
    #[inline]
    fn new() -> Self {
        Self { free: Vec::new() }
    }

    #[inline]
    pub fn cached_free_len(&self) -> usize {
        self.free.len()
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
    pub fn cached_free_len(&self) -> usize {
        self.thread_cache.borrow().cached_free_len()
    }

    #[inline]
    pub fn in_use(&self) -> usize {
        self.arena.inner.borrow().in_use
    }

    #[inline]
    pub fn alloc_index(&self) -> CoreResult<BufferIndex> {
        let mut cache = self.thread_cache.borrow_mut();
        let mut arena = self.arena.inner.borrow_mut();
        arena.alloc_empty_chain(&mut cache)
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> CoreResult<BufferIndex> {
        let mut cache = self.thread_cache.borrow_mut();
        let mut arena = self.arena.inner.borrow_mut();
        arena.alloc_chain(&mut cache, bytes)
    }

    #[inline]
    pub fn free_index(&self, index: BufferIndex) {
        let mut cache = self.thread_cache.borrow_mut();
        let _ = self
            .arena
            .inner
            .borrow_mut()
            .free_chain_collect_trace_handles(&mut cache, index);
    }

    #[inline]
    pub fn attach_clone(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        let mut cache = self.thread_cache.borrow_mut();
        self.arena.inner.borrow_mut().attach_clone(&mut cache, head, tail)
    }

    #[inline]
    pub fn prefetch_header(&self, index: BufferIndex) {
        self.arena.inner.borrow().prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: BufferIndex) {
        self.arena.inner.borrow().prefetch_read(index);
    }

    #[inline]
    pub fn free_frame(&self, frame: &mut BufferFrame) {
        let mut cache = self.thread_cache.borrow_mut();
        let mut pool = self.arena.inner.borrow_mut();
        for index in frame.drain_indices() {
            let _ = pool.free_chain_collect_trace_handles(&mut cache, index);
        }
    }

    #[inline]
    pub fn get(&self, index: BufferIndex) -> CoreResult<BufferRef<'_>> {
        let guard = self.arena.inner.borrow();
        guard.buffer(index)?;
        Ok(BufferRef { guard, index })
    }

    #[inline]
    pub fn get_mut(&self, index: BufferIndex) -> CoreResult<BufferRefMut<'_>> {
        let mut guard = self.arena.inner.borrow_mut();
        guard.ensure_storage_exclusive(index)?;
        guard.buffer_mut(index)?;
        let thread_cache = self.thread_cache.borrow_mut();
        Ok(BufferRefMut {
            guard,
            thread_cache,
            index,
        })
    }

    #[inline]
    pub fn batch_mut(&self) -> BufferBatchMut<'_> {
        BufferBatchMut {
            guard: self.arena.inner.borrow_mut(),
        }
    }

    #[inline]
    pub fn packet_cursor(&self, index: BufferIndex) -> CoreResult<BufferPacketCursor> {
        Ok(self.arena.inner.borrow().buffer(index)?.packet_cursor())
    }

    #[inline]
    pub fn current_data(&self, index: BufferIndex) -> CoreResult<usize> {
        Ok(self.arena.inner.borrow().buffer(index)?.current_data())
    }

    #[inline]
    pub fn current_len(&self, index: BufferIndex) -> CoreResult<usize> {
        Ok(self.arena.inner.borrow().buffer(index)?.current_len())
    }

    #[inline]
    pub fn total_len_not_including_first(&self, index: BufferIndex) -> CoreResult<usize> {
        Ok(self
            .arena
            .inner
            .borrow()
            .buffer(index)?
            .total_len_not_including_first())
    }

    #[inline]
    pub fn current_ptr(&self, index: BufferIndex) -> CoreResult<*const u8> {
        Ok(self.arena.inner.borrow().buffer(index)?.current_ptr())
    }

    #[inline]
    pub fn current_mut_ptr(&self, index: BufferIndex) -> CoreResult<*mut u8> {
        let mut guard = self.arena.inner.borrow_mut();
        guard.ensure_storage_exclusive(index)?;
        Ok(guard.buffer_mut(index)?.current_mut_ptr())
    }

    #[inline]
    pub fn handoff_source_worker(&self, index: BufferIndex) -> CoreResult<Option<DataWorkerId>> {
        Ok(self
            .arena
            .inner
            .borrow()
            .buffer(index)?
            .handoff_source_worker())
    }

    #[inline]
    pub fn mark_handoff_source_worker(
        &self,
        index: BufferIndex,
        worker: DataWorkerId,
    ) -> CoreResult<()> {
        let mut guard = self.arena.inner.borrow_mut();
        guard.ensure_header_exclusive(index)?;
        guard.buffer_mut(index)?.set_handoff_source_worker(worker);
        Ok(())
    }

    #[inline]
    pub fn current_config(&self, index: BufferIndex) -> CoreResult<crate::NodeId> {
        Ok(self.arena.inner.borrow().buffer(index)?.current_config())
    }

    #[inline]
    pub fn set_current_config(&self, index: BufferIndex, next: crate::NodeId) -> CoreResult<()> {
        let mut guard = self.arena.inner.borrow_mut();
        guard.ensure_header_exclusive(index)?;
        guard.buffer_mut(index)?.set_current_config(next);
        Ok(())
    }

    #[inline]
    pub fn node_error_code(&self, index: BufferIndex) -> CoreResult<Option<u16>> {
        Ok(self.arena.inner.borrow().buffer(index)?.node_error_code())
    }

    #[inline]
    pub fn advance(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        let mut pool = self.arena.inner.borrow_mut();
        pool.advance(index, len)
    }

    #[inline]
    pub fn truncate_current(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        let mut pool = self.arena.inner.borrow_mut();
        pool.ensure_storage_exclusive(index)?;
        let buffer = pool.buffer_mut(index)?;
        if len > buffer.current_len() {
            return Err(CoreError::internal(
                "buffer truncate extends current length",
            ));
        }
        buffer.set_current_len(len)?;
        Ok(())
    }

    #[inline]
    pub fn prepend(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        let mut pool = self.arena.inner.borrow_mut();
        pool.ensure_storage_exclusive(index)?;
        let buffer = pool.buffer_mut(index)?;
        if bytes.len() > buffer.current_data() {
            return Err(CoreError::internal("buffer prepend exceeds headroom"));
        }
        let start = buffer.current_data() - bytes.len();
        buffer.set_current_data(start)?;
        let end = start + bytes.len();
        buffer.storage.as_mut_slice()[start..end].copy_from_slice(bytes);
        buffer.set_current_len(buffer.current_len() + bytes.len())?;
        Ok(())
    }

    #[inline]
    pub fn append(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        let mut cache = self.thread_cache.borrow_mut();
        self.arena
            .inner
            .borrow_mut()
            .append_chain(&mut cache, index, bytes)
    }

    #[inline]
    pub fn detach_next(&self, index: BufferIndex) -> CoreResult<Option<BufferIndex>> {
        self.arena.inner.borrow_mut().detach_next(index)
    }

    #[inline]
    pub fn append_existing_chain(&self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.arena
            .inner
            .borrow_mut()
            .append_existing_chain(head, tail)
    }

    #[inline]
    pub fn truncate_chain(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        let mut cache = self.thread_cache.borrow_mut();
        self.arena
            .inner
            .borrow_mut()
            .truncate_chain(&mut cache, index, len)
    }

    #[inline]
    pub fn current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        Ok(self
            .arena
            .inner
            .borrow()
            .buffer(index)?
            .current()
            .iter()
            .copied()
            .collect())
    }

    #[inline]
    pub fn copy_current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.current(index)
    }

    #[inline]
    pub fn is_chained(&self, index: BufferIndex) -> CoreResult<bool> {
        Ok(self.arena.inner.borrow().next_buffer(index)?.is_some())
    }

    #[inline]
    pub fn copy_packet(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        let inner = self.arena.inner.borrow();
        let buffer = inner.buffer(index)?;
        if inner.next_buffer(index)?.is_none() {
            return Ok(buffer.current().iter().copied().collect());
        }
        inner.copy_current_chain(index)
    }

    #[inline]
    pub fn copy_current_chain(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.arena.inner.borrow().copy_current_chain(index)
    }

    #[inline]
    pub fn with_current_chain_io_segments<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&[&[u8]], usize) -> CoreResult<R>,
    ) -> CoreResult<R> {
        self.arena
            .inner
            .borrow()
            .with_current_chain_io_segments(index, f)
    }
}

impl FramePool {
    #[inline]
    pub fn with_capacity(frame_capacity: usize, slots: usize) -> Self {
        let free = (0..slots)
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
                free,
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
            .free
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
        self.free.push(index.slot);
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
        self.free.push(index.slot);
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
    fn index_from_slot(&self, slot: u32) -> Option<BufferIndex> {
        Some(BufferIndex {
            pool_id: self.pool_id,
            slot,
            generation: self.next_buffer_generation(slot)?,
        })
    }

    #[inline]
    fn next_buffer_generation(&self, slot: u32) -> Option<u32> {
        let slot = slot as usize;
        if slot >= self.slots.len() {
            return None;
        }
        let entry = &self.slots[slot];
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
    fn advance(&mut self, index: BufferIndex, len: usize) -> CoreResult<()> {
        if len == 0 {
            return Ok(());
        }

        let first = self.buffer(index)?;
        if self.next_buffer(index)?.is_none() {
            if len > first.current_len() {
                return Err(CoreError::internal("buffer advance exceeds current length"));
            }
            self.ensure_header_exclusive(index)?;
            let buffer = self.buffer_mut(index)?;
            buffer.set_current_data(buffer.current_data() + len)?;
            buffer.set_current_len(buffer.current_len() - len)?;
            return Ok(());
        }

        let total_len = first
            .current_len()
            .checked_add(first.total_len_not_including_first())
            .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?;
        if len > total_len {
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
            buffer.set_current_data(buffer.current_data() + consume)?;
            buffer.set_current_len(buffer.current_len() - consume)?;
            remaining -= consume;
            if remaining == 0 {
                break;
            }
        }

        self.refresh_chain_lengths(index)
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
        let index = self.alloc_slot(cache, &[])?;
        let headroom = DEFAULT_PACKET_HEADROOM.min(self.slot_capacity);
        {
            let buffer = self.buffer_mut(index)?;
            buffer.set_current_data(headroom)?;
        }
        Ok(index)
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
        let slot = cache
            .free
            .pop()
            .or_else(|| self.free.pop())
            .ok_or_else(|| CoreError::internal("buffer pool exhausted"))?;
        let entry = self
            .slots
            .get_mut(slot as usize)
            .ok_or_else(|| CoreError::internal("buffer slot out of bounds"))?;
        entry.generation = entry.generation.wrapping_add(1).max(1);
        entry.allocated = true;
        entry.header_live = true;
        entry.storage_ref_count = 1;
        entry.buffer.reset(slot, bytes)?;
        self.in_use += 1;
        Ok(BufferIndex {
            pool_id: self.pool_id,
            slot,
            generation: entry.generation,
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
        let entry = self
            .slots
            .get(index.slot as usize)
            .ok_or_else(|| CoreError::internal("buffer slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale buffer index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("buffer slot is free"));
        }
        Ok(&entry.buffer)
    }

    #[inline]
    fn buffer_mut(&mut self, index: BufferIndex) -> CoreResult<&mut Buffer> {
        self.validate_pool_index(index)?;
        let entry = self
            .slots
            .get_mut(index.slot as usize)
            .ok_or_else(|| CoreError::internal("buffer slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale buffer index"));
        }
        if !entry.allocated {
            return Err(CoreError::internal("buffer slot is free"));
        }
        Ok(&mut entry.buffer)
    }

    #[inline]
    fn ensure_header_exclusive(&self, index: BufferIndex) -> CoreResult<()> {
        self.buffer(index)?;
        if self
            .slots
            .get(index.slot as usize)
            .ok_or_else(|| CoreError::internal("buffer slot out of bounds"))?
            .header_live
        {
            return Ok(());
        }
        Err(CoreError::internal("buffer header is not live"))
    }

    #[inline]
    fn ensure_storage_exclusive(&self, index: BufferIndex) -> CoreResult<()> {
        self.ensure_header_exclusive(index)?;
        let entry = self
            .slots
            .get(index.slot as usize)
            .ok_or_else(|| CoreError::internal("buffer slot out of bounds"))?;
        let owner_slot = entry.buffer.storage_owner_slot() as usize;
        let owner = self
            .slots
            .get(owner_slot)
            .ok_or_else(|| CoreError::internal("buffer storage owner is invalid"))?;
        if owner.storage_ref_count == 1 {
            return Ok(());
        }
        Err(CoreError::internal(
            "shared buffer segment requires exclusive ownership",
        ))
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
    fn free_chain(&mut self, cache: &mut BufferThreadCache, index: BufferIndex) {
        let _ = self.free_chain_collect_trace_handles(cache, index);
    }

    #[inline]
    fn free_chain_collect_trace_handles(
        &mut self,
        cache: &mut BufferThreadCache,
        index: BufferIndex,
    ) -> Vec<u32> {
        let mut released_trace_handles = Vec::new();
        let mut next = Some(index);
        while let Some(index) = next {
            if index.pool_id != self.pool_id {
                return released_trace_handles;
            }
            let slot = index.slot as usize;
            let Some(entry) = self.slots.get(slot) else {
                return released_trace_handles;
            };
            if entry.generation != index.generation {
                return released_trace_handles;
            }
            if entry.allocated {
                let next_slot = entry.buffer.next_buffer_slot();
                let owner_slot = entry.buffer.storage_owner_slot() as usize;
                let handle = (entry.buffer.cacheline1.trace_handle != 0)
                    .then_some(entry.buffer.cacheline1.trace_handle);
                {
                    let entry = self
                        .slots
                        .get_mut(slot)
                        .expect("buffer slot remains valid");
                    entry.header_live = false;
                    entry.buffer.cacheline1.trace_handle = 0;
                }
                if let Some(handle) = handle {
                    released_trace_handles.push(handle);
                }
                let release_storage = {
                    let owner = self
                        .slots
                        .get_mut(owner_slot)
                        .and_then(|owner| owner.allocated.then_some(owner))
                        .expect("buffer storage owner must be live");
                    owner.storage_ref_count = owner.storage_ref_count.saturating_sub(1);
                    owner.storage_ref_count == 0
                };
                if release_storage {
                    let owner = self
                        .slots
                        .get_mut(owner_slot)
                        .expect("buffer storage owner slot");
                    owner.storage_ref_count = 0;
                    owner.buffer.reset_storage_view(owner_slot as u32);
                }
                let can_free_header = owner_slot != slot || release_storage;
                if can_free_header {
                    let entry = self
                        .slots
                        .get_mut(slot)
                        .expect("buffer slot remains valid");
                    entry.allocated = false;
                    entry.buffer.reset_for_free();
                    self.in_use = self.in_use.saturating_sub(1);
                    cache.free.push(index.slot);
                }
                next = next_slot.and_then(|slot| self.index_from_slot(slot));
            } else {
                next = None;
            }
        }
        released_trace_handles
    }

    #[inline]
    fn attach_clone(
        &mut self,
        cache: &mut BufferThreadCache,
        head: BufferIndex,
        tail: BufferIndex,
    ) -> CoreResult<()> {
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
        let mut source = Some(tail);
        let mut clone_tail = head;
        while let Some(source_index) = source {
            let source_next = self.next_buffer(source_index)?;
            let (
                current_data,
                current_length,
                flags,
                flow_id,
                error,
                current_config_or_punt,
                opaque,
                total_length_not_including_first,
                opaque2,
                storage_owner_slot,
                storage_view,
                storage_capacity,
            ) = {
                let source_buffer = self.buffer(source_index)?;
                (
                    source_buffer.cacheline0.current_data,
                    source_buffer.cacheline0.current_length,
                    source_buffer.cacheline0.flags,
                    source_buffer.cacheline0.flow_id,
                    source_buffer.cacheline0.error,
                    source_buffer.cacheline0.current_config_or_punt,
                    source_buffer.cacheline0.opaque,
                    source_buffer.cacheline1.total_length_not_including_first,
                    source_buffer.cacheline1.opaque2,
                    source_buffer.storage_owner_slot(),
                    source_buffer.storage_view,
                    source_buffer.storage_capacity,
                )
            };
            let next_clone = self.alloc_empty_chain(cache)?;
            self.buffer_mut(clone_tail)?
                .set_next_buffer(Some(next_clone));
            let clone_buffer = self.buffer_mut(next_clone)?;
            clone_buffer.cacheline0.current_data = current_data;
            clone_buffer.cacheline0.current_length = current_length;
            clone_buffer.cacheline0.flags = flags;
            clone_buffer.cacheline0.flow_id = flow_id;
            clone_buffer.cacheline0.error = error;
            clone_buffer.cacheline0.current_config_or_punt = current_config_or_punt;
            clone_buffer.cacheline0.opaque = opaque;
            clone_buffer.cacheline1.total_length_not_including_first =
                total_length_not_including_first;
            clone_buffer.cacheline1.opaque2 = opaque2;
            clone_buffer.storage_owner_slot = storage_owner_slot;
            clone_buffer.storage_view = storage_view;
            clone_buffer.storage_capacity = storage_capacity;
            let owner = self
                .slots
                .get_mut(storage_owner_slot as usize)
                .ok_or_else(|| CoreError::internal("buffer storage owner is invalid"))?;
            owner.storage_ref_count = owner
                .storage_ref_count
                .checked_add(1)
                .ok_or_else(|| CoreError::internal("buffer refcount overflow"))?;
            if let Some(next_source) = source_next {
                clone_tail = next_clone;
                source = Some(next_source);
            } else {
                source = None;
            }
        }
        self.refresh_chain_lengths(head)
    }

    #[inline]
    fn copy_current_chain(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut next = Some(index);
        while let Some(index) = next {
            let buffer = self.buffer(index)?;
            bytes.extend_from_copy_slice(buffer.current());
            next = self.next_buffer(index)?;
        }
        Ok(bytes)
    }

    #[inline]
    fn with_current_chain_io_segments<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&[&[u8]], usize) -> CoreResult<R>,
    ) -> CoreResult<R> {
        let first = self.buffer(index)?;
        let total_len = first
            .current_len()
            .checked_add(first.total_len_not_including_first())
            .ok_or_else(|| CoreError::internal("buffer chain length overflow"))?;
        let empty: &[u8] = &[];
        let mut segments = [empty; CURRENT_CHAIN_IO_SEGMENT_CAPACITY];
        let mut segment_count = 0usize;
        let mut next = Some(index);
        while let Some(index) = next {
            if segment_count == CURRENT_CHAIN_IO_SEGMENT_CAPACITY {
                return Err(CoreError::internal(
                    "buffer chain exceeds single TUN writev segment capacity",
                ));
            }
            let buffer = self.buffer(index)?;
            segments[segment_count] = buffer.current();
            segment_count += 1;
            next = self.next_buffer(index)?;
        }
        f(&segments[..segment_count], total_len)
    }

    #[inline]
    fn append_chain(
        &mut self,
        cache: &mut BufferThreadCache,
        index: BufferIndex,
        bytes: &[u8],
    ) -> CoreResult<()> {
        self.ensure_storage_exclusive(index)?;
        let mut tail = index;
        while let Some(next) = self.next_buffer(tail)? {
            tail = next;
        }

        let taken = self.buffer_mut(tail)?.append_in_place(bytes);
        let mut remaining = &bytes[taken..];
        while !remaining.is_empty() {
            let take = remaining.len().min(self.slot_capacity);
            let next = self.alloc_slot(cache, &remaining[..take])?;
            {
                let tail_buffer = self.buffer_mut(tail)?;
                tail_buffer.set_next_buffer(Some(next));
            }
            tail = next;
            remaining = &remaining[take..];
        }
        self.refresh_chain_lengths(index)
    }

    #[inline]
    fn detach_next(&mut self, index: BufferIndex) -> CoreResult<Option<BufferIndex>> {
        let next = {
            self.ensure_header_exclusive(index)?;
            let buffer = self.buffer_mut(index)?;
            let next_slot = buffer.take_next_buffer_slot();
            buffer.set_total_len_not_including_first(0)?;
            next_slot
        };
        let next = next.and_then(|slot| self.index_from_slot(slot));
        self.refresh_chain_lengths(index)?;
        Ok(next)
    }

    #[inline]
    fn append_existing_chain(&mut self, head: BufferIndex, tail: BufferIndex) -> CoreResult<()> {
        self.ensure_header_exclusive(head)?;
        self.buffer(tail)?;
        let mut current = head;
        while let Some(next) = self.next_buffer(current)? {
            current = next;
        }
        {
            self.ensure_header_exclusive(current)?;
            let current_buffer = self.buffer_mut(current)?;
            current_buffer.set_next_buffer(Some(tail));
        }
        self.refresh_chain_lengths(head)
    }

    #[inline]
    fn truncate_chain(
        &mut self,
        cache: &mut BufferThreadCache,
        index: BufferIndex,
        len: usize,
    ) -> CoreResult<()> {
        let mut remaining = len;
        let mut current = Some(index);
        while let Some(current_index) = current {
            self.ensure_header_exclusive(current_index)?;
            let current_len = self.buffer(current_index)?.current_len();
            if remaining > current_len {
                remaining -= current_len;
                current = self.next_buffer(current_index)?;
                continue;
            }

            let tail = {
                let buffer = self.buffer_mut(current_index)?;
                buffer.set_current_len(remaining)?;
                let tail_slot = buffer.take_next_buffer_slot();
                buffer.set_total_len_not_including_first(0)?;
                tail_slot
            };
            let tail = tail.and_then(|slot| self.index_from_slot(slot));
            if let Some(tail) = tail {
                self.free_chain(cache, tail);
            }
            return self.refresh_chain_lengths(index);
        }
        Err(CoreError::internal("buffer chain truncate exceeds length"))
    }

    #[inline]
    fn refresh_chain_lengths(&mut self, index: BufferIndex) -> CoreResult<()> {
        let mut total = 0usize;
        let mut next = self.next_buffer(index)?;
        while let Some(index) = next {
            let buffer = self.buffer(index)?;
            total += buffer.current_len();
            next = self.next_buffer(index)?;
        }
        let buffer = self.buffer_mut(index)?;
        buffer.set_total_len_not_including_first(total)?;
        Ok(())
    }
}

#[inline(always)]
fn prefetch_buffer_header(buffer: &Buffer) {
    prefetch_read_l1(std::ptr::from_ref(&buffer.cacheline0).cast::<u8>());
}

#[inline(always)]
fn prefetch_buffer_cacheline1(buffer: &Buffer) {
    prefetch_read_l1(std::ptr::from_ref(&buffer.cacheline1).cast::<u8>());
}

#[inline(always)]
fn prefetch_buffer_data(buffer: &Buffer) {
    if !buffer.current().is_empty() {
        prefetch_read_l1(buffer.current().as_ptr());
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
    pub fn iter_indices(&self) -> std::slice::Iter<'_, BufferIndex> {
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
            self.retain_one(read, &mut write, keep)?;
            self.retain_one(read + 1, &mut write, keep)?;
            self.retain_one(read + 2, &mut write, keep)?;
            self.retain_one(read + 3, &mut write, keep)?;
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
        while read + 4 <= len {
            self.prefetch_indices(read + 4, 4, prefetch);
            self.retain_one(read, &mut write, keep)?;
            self.retain_one(read + 1, &mut write, keep)?;
            self.retain_one(read + 2, &mut write, keep)?;
            self.retain_one(read + 3, &mut write, keep)?;
            read += 4;
        }
        if read + 2 <= len {
            self.prefetch_indices(read + 2, 2, prefetch);
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
    fn retain_indices_pair(
        &mut self,
        keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>,
    ) -> CoreResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        while read + 2 <= len {
            self.retain_one(read, &mut write, keep)?;
            self.retain_one(read + 1, &mut write, keep)?;
            read += 2;
        }
        if read < len {
            self.retain_one(read, &mut write, keep)?;
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
        while read + 2 <= len {
            self.prefetch_indices(read + 2, 2, prefetch);
            self.retain_one(read, &mut write, keep)?;
            self.retain_one(read + 1, &mut write, keep)?;
            read += 2;
        }
        if read < len {
            self.retain_one(read, &mut write, keep)?;
        }
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
        while read + 4 <= len {
            self.prefetch_indices_state(read + 4, 4, state, prefetch);
            self.retain_one_state(read, &mut write, state, keep)?;
            self.retain_one_state(read + 1, &mut write, state, keep)?;
            self.retain_one_state(read + 2, &mut write, state, keep)?;
            self.retain_one_state(read + 3, &mut write, state, keep)?;
            read += 4;
        }
        if read + 2 <= len {
            self.prefetch_indices_state(read + 2, 2, state, prefetch);
            self.retain_one_state(read, &mut write, state, keep)?;
            self.retain_one_state(read + 1, &mut write, state, keep)?;
            read += 2;
        }
        while read < len {
            self.retain_one_state(read, &mut write, state, keep)?;
            read += 1;
        }
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
        while read + 2 <= len {
            self.prefetch_indices_state(read + 2, 2, state, prefetch);
            self.retain_one_state(read, &mut write, state, keep)?;
            self.retain_one_state(read + 1, &mut write, state, keep)?;
            read += 2;
        }
        if read < len {
            self.retain_one_state(read, &mut write, state, keep)?;
        }
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
        while read + 4 <= len {
            self.prefetch_indices_state(read + 4, 4, state, prefetch);
            self.retain_one_state_lazy(read, &mut write, state, keep)?;
            self.retain_one_state_lazy(read + 1, &mut write, state, keep)?;
            self.retain_one_state_lazy(read + 2, &mut write, state, keep)?;
            self.retain_one_state_lazy(read + 3, &mut write, state, keep)?;
            read += 4;
        }
        if read + 2 <= len {
            self.prefetch_indices_state(read + 2, 2, state, prefetch);
            self.retain_one_state_lazy(read, &mut write, state, keep)?;
            self.retain_one_state_lazy(read + 1, &mut write, state, keep)?;
            read += 2;
        }
        while read < len {
            self.retain_one_state_lazy(read, &mut write, state, keep)?;
            read += 1;
        }
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
        while read + 2 <= len {
            self.prefetch_indices_state(read + 2, 2, state, prefetch);
            self.retain_one_state_lazy(read, &mut write, state, keep)?;
            self.retain_one_state_lazy(read + 1, &mut write, state, keep)?;
            read += 2;
        }
        if read < len {
            self.retain_one_state_lazy(read, &mut write, state, keep)?;
        }
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
        while read + 4 <= len {
            self.rewrite_one(read, &mut write, rewrite)?;
            self.rewrite_one(read + 1, &mut write, rewrite)?;
            self.rewrite_one(read + 2, &mut write, rewrite)?;
            self.rewrite_one(read + 3, &mut write, rewrite)?;
            read += 4;
        }
        if read + 2 <= len {
            self.rewrite_one(read, &mut write, rewrite)?;
            self.rewrite_one(read + 1, &mut write, rewrite)?;
            read += 2;
        }
        while read < len {
            self.rewrite_one(read, &mut write, rewrite)?;
            read += 1;
        }
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
        while read + 2 <= len {
            self.rewrite_one(read, &mut write, rewrite)?;
            self.rewrite_one(read + 1, &mut write, rewrite)?;
            read += 2;
        }
        if read < len {
            self.rewrite_one(read, &mut write, rewrite)?;
        }
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
            FrameBatchWidth::Quad if remaining >= 4 => {
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

pub struct BufferBatchMut<'pool> {
    guard: RefMut<'pool, BufferPoolInner>,
}

impl BufferBatchMut<'_> {
    #[inline]
    pub fn prefetch_header(&self, index: BufferIndex) {
        self.guard.prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: BufferIndex) {
        self.guard.prefetch_read(index);
    }

    #[inline]
    pub fn buffer(&self, index: BufferIndex) -> CoreResult<&Buffer> {
        self.guard.buffer(index)
    }

    #[inline]
    pub fn buffer_mut(&mut self, index: BufferIndex) -> CoreResult<&mut Buffer> {
        self.guard.ensure_storage_exclusive(index)?;
        self.guard.buffer_mut(index)
    }

    #[inline]
    pub fn with_buffer<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&Buffer) -> R,
    ) -> CoreResult<R> {
        let buffer = self.guard.buffer(index)?;
        Ok(f(buffer))
    }

    #[inline]
    pub fn with_buffer_mut<R>(
        &mut self,
        index: BufferIndex,
        f: impl FnOnce(&mut Buffer) -> R,
    ) -> CoreResult<R> {
        self.guard.ensure_storage_exclusive(index)?;
        let buffer = self.guard.buffer_mut(index)?;
        Ok(f(buffer))
    }
}

pub struct BufferRef<'pool> {
    guard: Ref<'pool, BufferPoolInner>,
    index: BufferIndex,
}

impl BufferRef<'_> {
    #[inline]
    pub fn prefetch_header(&self) {
        let buffer = self
            .guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer");
        prefetch_buffer_header(buffer);
    }

    #[inline]
    pub fn prefetch_read(&self) {
        let buffer = self
            .guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer");
        prefetch_buffer_header(buffer);
        prefetch_buffer_cacheline1(buffer);
        prefetch_buffer_data(buffer);
    }

    #[inline]
    pub fn buffer_ptr(&self) -> *const Buffer {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer") as *const Buffer
    }

    #[inline]
    pub fn packet_cursor(&self) -> BufferPacketCursor {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .packet_cursor()
    }

    #[inline]
    pub fn opaque(&self) -> &crate::PrimaryOpaque {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .opaque()
    }

    #[inline]
    pub fn opaque2(&self) -> &crate::SecondaryOpaque {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .opaque2()
    }

    #[inline]
    pub fn current_config(&self) -> crate::NodeId {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current_config()
    }

    #[inline]
    pub fn node_error(&self) -> Option<BufferNodeError> {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .node_error_code()
            .map(|code| BufferNodeError::new(crate::NodeId::new(0), code))
    }

    #[inline]
    pub fn trace_handle(&self) -> Option<u32> {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .trace_handle()
    }

    #[inline]
    pub fn current(&self) -> &[u8] {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current()
    }

    #[inline]
    pub fn current_ptr(&self) -> *const u8 {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current_ptr()
    }

    #[inline]
    pub fn current_data(&self) -> usize {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current_data()
    }

    #[inline]
    pub fn current_len(&self) -> usize {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current_len()
    }

    #[inline]
    pub fn total_len_not_including_first(&self) -> usize {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .total_len_not_including_first()
    }
}

pub struct BufferRefMut<'pool> {
    guard: RefMut<'pool, BufferPoolInner>,
    thread_cache: RefMut<'pool, BufferThreadCache>,
    index: BufferIndex,
}

impl BufferRefMut<'_> {
    #[inline]
    pub fn attach_clone(&mut self, tail: BufferIndex) -> CoreResult<()> {
        self.guard.attach_clone(&mut self.thread_cache, self.index, tail)
    }

    #[inline]
    pub fn advance(&mut self, len: usize) -> CoreResult<()> {
        self.guard.advance(self.index, len)
    }

    #[inline]
    pub fn truncate_chain(&mut self, len: usize) -> CoreResult<()> {
        self.guard
            .truncate_chain(&mut self.thread_cache, self.index, len)
    }

    #[inline]
    pub fn packet_cursor(&self) -> BufferPacketCursor {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .packet_cursor()
    }

    #[inline]
    pub fn node_error(&self) -> Option<BufferNodeError> {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .node_error_code()
            .map(|code| BufferNodeError::new(crate::NodeId::new(0), code))
    }

    #[inline]
    pub fn set_node_error(&mut self, error: BufferNodeError) {
        self.guard
            .ensure_header_exclusive(self.index)
            .expect("buffer ref mut requires live header");
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .set_node_error(error);
    }

    #[inline]
    pub fn next_buffer(&self) -> Option<BufferIndex> {
        self.guard
            .next_buffer(self.index)
            .expect("buffer ref points to valid buffer")
    }

    #[inline]
    pub fn set_trace_handle(&mut self, handle: u32) {
        self.guard
            .ensure_header_exclusive(self.index)
            .expect("buffer ref mut requires live header");
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .set_trace_handle(handle);
    }

    #[inline]
    pub fn take_trace_handle(&mut self) -> Option<u32> {
        self.guard
            .ensure_header_exclusive(self.index)
            .expect("buffer ref mut requires live header");
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .take_trace_handle()
    }

    #[inline]
    pub fn clear_node_error(&mut self) {
        self.guard
            .ensure_header_exclusive(self.index)
            .expect("buffer ref mut requires live header");
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .clear_node_error();
    }

    #[inline]
    pub fn set_packet_cursor(&mut self, cursor: BufferPacketCursor) {
        self.guard
            .ensure_header_exclusive(self.index)
            .expect("buffer ref mut requires live header");
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .set_packet_cursor(cursor);
    }

    #[inline]
    pub fn opaque(&self) -> &crate::PrimaryOpaque {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .opaque()
    }

    #[inline]
    pub fn opaque_mut(&mut self) -> &mut crate::PrimaryOpaque {
        self.guard
            .ensure_header_exclusive(self.index)
            .expect("buffer ref mut requires live header");
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .opaque_mut()
    }

    #[inline]
    pub fn opaque2(&self) -> &crate::SecondaryOpaque {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .opaque2()
    }

    #[inline]
    pub fn current_config(&self) -> crate::NodeId {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current_config()
    }

    #[inline]
    pub fn set_current_config(&mut self, next: crate::NodeId) {
        self.guard
            .ensure_header_exclusive(self.index)
            .expect("buffer ref mut requires live header");
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .set_current_config(next);
    }

    #[inline]
    pub fn opaque2_mut(&mut self) -> &mut crate::SecondaryOpaque {
        self.guard
            .ensure_header_exclusive(self.index)
            .expect("buffer ref mut requires live header");
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .opaque2_mut()
    }

    #[inline]
    pub fn current(&self) -> &[u8] {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current()
    }

    #[inline]
    pub fn current_mut(&mut self) -> &mut [u8] {
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .current_mut()
    }

    #[inline]
    pub fn writable_tail_mut(&mut self) -> &mut [u8] {
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .writable_tail_mut()
    }

    #[inline]
    pub fn commit_writable_tail(&mut self, len: usize) -> CoreResult<()> {
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .commit_writable_tail(len)
    }

    #[inline]
    pub fn current_ptr(&self) -> *const u8 {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current_ptr()
    }

    #[inline]
    pub fn current_mut_ptr(&mut self) -> *mut u8 {
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .current_mut_ptr()
    }

    #[inline]
    pub fn current_data(&self) -> usize {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current_data()
    }

    #[inline]
    pub fn current_len(&self) -> usize {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current_len()
    }

    #[inline]
    pub fn total_len_not_including_first(&self) -> usize {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .total_len_not_including_first()
    }
}
