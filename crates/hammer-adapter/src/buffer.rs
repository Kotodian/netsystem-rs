use std::cell::{Cell, Ref, RefCell, RefMut};
use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use hammer_core::error::{CoreError, CoreResult};

use crate::RouteMetadata;
use crate::instruction_set::{DataPlaneInstructionSet, FrameBatchWidth};
use crate::node::{Node, NodeId, NodeRuntime, NoopNode};

pub const DEFAULT_BUFFER_FRAME_CAPACITY: usize = 256;
pub const DEFAULT_BUFFER_FRAME_POOL_SIZE: usize = 64;
pub const BUFFER_CACHE_LINE_SIZE: usize = 64;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BufferFlags(u32);

impl BufferFlags {
    pub const NEXT_PRESENT: Self = Self(1 << 0);

    #[inline]
    pub fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPacketCursor {
    packet_len: usize,
    network_header_offset: usize,
    network_header_len: usize,
    transport_header_offset: usize,
    transport_header_len: usize,
    transport_payload_offset: usize,
}

impl BufferPacketCursor {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn packet_len(self) -> usize {
        self.packet_len
    }

    #[inline]
    pub fn network_header_offset(self) -> usize {
        self.network_header_offset
    }

    #[inline]
    pub fn network_header_len(self) -> usize {
        self.network_header_len
    }

    #[inline]
    pub fn transport_header_offset(self) -> usize {
        self.transport_header_offset
    }

    #[inline]
    pub fn transport_header_len(self) -> usize {
        self.transport_header_len
    }

    #[inline]
    pub fn transport_payload_offset(self) -> usize {
        self.transport_payload_offset
    }

    #[inline]
    pub fn with_packet_len(mut self, packet_len: usize) -> Self {
        self.packet_len = packet_len;
        self
    }

    #[inline]
    pub fn with_network_header(mut self, offset: usize, len: usize) -> Self {
        self.network_header_offset = offset;
        self.network_header_len = len;
        self
    }

    #[inline]
    pub fn with_transport_header(mut self, offset: usize, len: usize) -> Self {
        self.transport_header_offset = offset;
        self.transport_header_len = len;
        self
    }

    #[inline]
    pub fn with_transport_payload_offset(mut self, offset: usize) -> Self {
        self.transport_payload_offset = offset;
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

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct CacheLine([u8; BUFFER_CACHE_LINE_SIZE]);

impl Default for CacheLine {
    fn default() -> Self {
        Self([0; BUFFER_CACHE_LINE_SIZE])
    }
}

struct CacheAlignedStorage {
    lines: Box<[CacheLine]>,
    len: usize,
}

impl fmt::Debug for CacheAlignedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheAlignedStorage")
            .field("len", &self.len)
            .finish()
    }
}

impl CacheAlignedStorage {
    #[inline]
    fn with_len(len: usize) -> Self {
        let line_count = len.div_ceil(BUFFER_CACHE_LINE_SIZE).max(1);
        Self {
            lines: vec![CacheLine::default(); line_count].into_boxed_slice(),
            len,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        let ptr = self.lines.as_ptr().cast::<u8>();
        // SAFETY: CacheLine is a repr(C, align(64)) wrapper around contiguous
        // bytes. `len` never exceeds the allocated line byte capacity.
        unsafe { std::slice::from_raw_parts(ptr, self.len) }
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self.lines.as_mut_ptr().cast::<u8>();
        // SAFETY: Same layout guarantee as `as_slice`, with unique access via
        // `&mut self`.
        unsafe { std::slice::from_raw_parts_mut(ptr, self.len) }
    }
}

#[derive(Debug)]
#[repr(C, align(64))]
pub struct Buffer {
    metadata: RouteMetadata,
    packet_cursor: BufferPacketCursor,
    node_error: Option<BufferNodeError>,
    flags: BufferFlags,
    current_data: usize,
    current_len: usize,
    data_len: usize,
    next_buffer: Option<BufferIndex>,
    total_len_not_including_first: usize,
    storage: CacheAlignedStorage,
}

impl Buffer {
    #[inline]
    fn with_slot_capacity(slot_capacity: usize) -> Self {
        Self {
            metadata: RouteMetadata::default(),
            packet_cursor: BufferPacketCursor::default(),
            node_error: None,
            flags: BufferFlags::empty(),
            current_data: 0,
            current_len: 0,
            data_len: 0,
            next_buffer: None,
            total_len_not_including_first: 0,
            storage: CacheAlignedStorage::with_len(slot_capacity),
        }
    }

    #[inline]
    fn reset(&mut self, metadata: RouteMetadata, bytes: &[u8]) -> CoreResult<()> {
        if bytes.len() > self.storage.len() {
            return Err(CoreError::internal(format!(
                "buffer bytes exceed slot capacity: {} > {}",
                bytes.len(),
                self.storage.len()
            )));
        }
        self.metadata = metadata;
        self.packet_cursor = BufferPacketCursor::default();
        self.node_error = None;
        self.flags = BufferFlags::empty();
        self.current_data = 0;
        self.current_len = bytes.len();
        self.data_len = bytes.len();
        self.next_buffer = None;
        self.total_len_not_including_first = 0;
        self.storage.as_mut_slice()[..bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    #[inline]
    fn reset_for_free(&mut self) {
        self.metadata = RouteMetadata::default();
        self.packet_cursor = BufferPacketCursor::default();
        self.node_error = None;
        self.flags = BufferFlags::empty();
        self.current_data = 0;
        self.current_len = 0;
        self.data_len = 0;
        self.next_buffer = None;
        self.total_len_not_including_first = 0;
    }

    #[inline]
    pub fn metadata(&self) -> &RouteMetadata {
        &self.metadata
    }

    #[inline]
    pub fn metadata_mut(&mut self) -> &mut RouteMetadata {
        &mut self.metadata
    }

    #[inline]
    pub fn packet_cursor(&self) -> BufferPacketCursor {
        self.packet_cursor
    }

    #[inline]
    pub fn packet_cursor_mut(&mut self) -> &mut BufferPacketCursor {
        &mut self.packet_cursor
    }

    #[inline]
    pub fn node_error(&self) -> Option<BufferNodeError> {
        self.node_error
    }

    #[inline]
    pub fn set_node_error(&mut self, error: BufferNodeError) {
        self.node_error = Some(error);
    }

    #[inline]
    pub fn clear_node_error(&mut self) {
        self.node_error = None;
    }

    #[inline]
    pub fn flags(&self) -> BufferFlags {
        self.flags
    }

    #[inline]
    pub fn current_data(&self) -> usize {
        self.current_data
    }

    #[inline]
    pub fn current_len(&self) -> usize {
        self.current_len
    }

    #[inline]
    pub fn data_len(&self) -> usize {
        self.data_len
    }

    #[inline]
    pub fn next_buffer(&self) -> Option<BufferIndex> {
        self.next_buffer
    }

    #[inline]
    pub fn total_len_not_including_first(&self) -> usize {
        self.total_len_not_including_first
    }

    #[inline]
    pub fn current(&self) -> &[u8] {
        &self.storage.as_slice()[self.current_data..self.current_data + self.current_len]
    }

    #[inline]
    pub fn current_mut(&mut self) -> &mut [u8] {
        let start = self.current_data;
        let end = start + self.current_len;
        &mut self.storage.as_mut_slice()[start..end]
    }

    #[inline]
    fn available_tail(&self) -> usize {
        self.storage
            .len()
            .saturating_sub(self.current_data + self.current_len)
    }

    #[inline]
    fn append_in_place(&mut self, bytes: &[u8]) -> usize {
        let take = bytes.len().min(self.available_tail());
        if take == 0 {
            return 0;
        }
        let start = self.current_data + self.current_len;
        let end = start + take;
        self.storage.as_mut_slice()[start..end].copy_from_slice(&bytes[..take]);
        self.current_len += take;
        self.data_len = self.data_len.max(end);
        take
    }
}

#[derive(Debug)]
struct BufferSlot {
    generation: u32,
    allocated: bool,
    buffer: Buffer,
}

#[derive(Debug)]
struct BufferPoolInner {
    pool_id: u64,
    slot_capacity: usize,
    slots: Vec<BufferSlot>,
    free: Vec<u32>,
    in_use: usize,
}

#[derive(Debug, Clone)]
pub struct BufferPool {
    inner: Rc<RefCell<BufferPoolInner>>,
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
}

#[derive(Debug)]
pub struct DataPlaneRuntime<N = NoopNode> {
    buffers: DataPlaneBuffers,
    nodes: NodeRuntime<N>,
    current_node: Rc<Cell<Option<NodeId>>>,
}

impl<N> Clone for DataPlaneRuntime<N> {
    fn clone(&self) -> Self {
        Self {
            buffers: self.buffers.clone(),
            nodes: self.nodes.clone(),
            current_node: Rc::clone(&self.current_node),
        }
    }
}

impl<N> Deref for DataPlaneRuntime<N> {
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

#[cfg(target_arch = "x86")]
#[inline]
fn prefetch_read_l1(ptr: *const u8) {
    unsafe {
        core::arch::x86::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86::_MM_HINT_T0);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn prefetch_read_l1(ptr: *const u8) {
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86_64::_MM_HINT_T0);
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn prefetch_read_l1(ptr: *const u8) {
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags, readonly)
        );
    }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn prefetch_read_l1(_ptr: *const u8) {}

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
        Self {
            buffers: BufferPool::with_capacity(buffer_slot_capacity, buffer_slots),
            frames: FramePool::with_capacity(frame_capacity, frame_slots),
            instruction_set,
        }
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
    pub fn preferred_frame_batch_width(&self) -> FrameBatchWidth {
        self.instruction_set.preferred_frame_batch_width()
    }

    #[inline]
    pub fn in_use_buffers(&self) -> usize {
        self.buffers.in_use()
    }

    #[inline]
    pub fn frames_in_use(&self) -> usize {
        self.frames.in_use()
    }

    #[inline]
    pub fn alloc_index(&self, metadata: RouteMetadata) -> CoreResult<BufferIndex> {
        self.buffers.alloc_index(metadata)
    }

    #[inline]
    pub fn alloc_index_with_bytes(
        &self,
        metadata: RouteMetadata,
        bytes: &[u8],
    ) -> CoreResult<BufferIndex> {
        self.buffers.alloc_index_with_bytes(metadata, bytes)
    }

    #[inline]
    pub fn free_index(&self, index: BufferIndex) {
        self.buffers.free_index(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: BufferIndex) {
        self.buffers.prefetch_read(index);
    }

    #[inline]
    pub fn free_frame(&self, frame: &mut BufferFrame) {
        self.buffers.free_frame(frame);
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
        self.frames.free_index(&self.buffers, index)
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
        self.frames.free_taken_index(&self.buffers, index, frame)
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
        self.frames.free_taken_index(&self.buffers, index, frame)
    }

    #[inline]
    pub fn metadata(&self, index: BufferIndex) -> CoreResult<RouteMetadata> {
        self.buffers.metadata(index)
    }

    #[inline]
    pub fn with_buffer<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&Buffer) -> R,
    ) -> CoreResult<R> {
        self.buffers.with_buffer(index, f)
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
    pub fn with_buffer_mut<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&mut Buffer) -> R,
    ) -> CoreResult<R> {
        self.buffers.with_buffer_mut(index, f)
    }

    #[inline]
    pub fn packet_cursor(&self, index: BufferIndex) -> CoreResult<BufferPacketCursor> {
        self.buffers.packet_cursor(index)
    }

    #[inline]
    pub fn node_error(&self, index: BufferIndex) -> CoreResult<Option<BufferNodeError>> {
        self.buffers.node_error(index)
    }

    #[inline]
    pub fn with_metadata<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&RouteMetadata) -> R,
    ) -> CoreResult<R> {
        self.buffers.with_metadata(index, f)
    }

    #[inline]
    pub fn with_metadata_mut<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&mut RouteMetadata) -> R,
    ) -> CoreResult<R> {
        self.buffers.with_metadata_mut(index, f)
    }

    #[inline]
    pub fn advance(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        self.buffers.advance(index, len)
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
}

impl<N> DataPlaneRuntime<N> {
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
        Self {
            buffers: DataPlaneBuffers::with_capacities_and_instruction_set(
                buffer_slot_capacity,
                buffer_slots,
                frame_capacity,
                frame_slots,
                instruction_set,
            ),
            nodes: NodeRuntime::default(),
            current_node: Rc::new(Cell::new(None)),
        }
    }

    #[inline]
    pub fn packet_buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    #[inline]
    pub fn nodes(&self) -> &NodeRuntime<N> {
        &self.nodes
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
    pub fn record_current_node_error(&self, code: u16) -> CoreResult<BufferNodeError> {
        let node = self
            .current_node()
            .ok_or_else(|| CoreError::internal("node error set outside node processing"))?;
        self.nodes.increment_node_error(node, code)?;
        Ok(BufferNodeError::new(node, code))
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
    pub fn schedule_driver_frame(&self, node: NodeId, frame: FrameIndex) -> CoreResult<()> {
        self.get_frame_mut(frame)?.set_next_node(node);
        self.nodes.schedule_frame(node, frame, true)
    }

    #[inline]
    pub fn run_ready_nodes(&self) -> CoreResult<usize>
    where
        N: Node<N>,
    {
        self.nodes.run_ready(self)
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

impl BufferPool {
    #[inline]
    pub fn with_capacity(slot_capacity: usize, slots: usize) -> Self {
        let free = (0..slots)
            .rev()
            .map(|slot| u32::try_from(slot).expect("buffer slot index fits u32"))
            .collect();
        let slots = (0..slots)
            .map(|_| BufferSlot {
                generation: 0,
                allocated: false,
                buffer: Buffer::with_slot_capacity(slot_capacity),
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
    pub fn in_use(&self) -> usize {
        self.inner.borrow().in_use
    }

    #[inline]
    pub fn alloc_index(&self, metadata: RouteMetadata) -> CoreResult<BufferIndex> {
        self.alloc_index_with_bytes(metadata, &[])
    }

    #[inline]
    pub fn alloc_index_with_bytes(
        &self,
        metadata: RouteMetadata,
        bytes: &[u8],
    ) -> CoreResult<BufferIndex> {
        self.inner.borrow_mut().alloc_chain(metadata, bytes)
    }

    #[inline]
    pub fn free_index(&self, index: BufferIndex) {
        self.inner.borrow_mut().free_chain(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: BufferIndex) {
        self.inner.borrow().prefetch_read(index);
    }

    #[inline]
    pub fn free_frame(&self, frame: &mut BufferFrame) {
        let mut pool = self.inner.borrow_mut();
        for index in frame.drain_indices() {
            pool.free_chain(index);
        }
    }

    #[inline]
    pub fn get(&self, index: BufferIndex) -> CoreResult<BufferRef<'_>> {
        let guard = self.inner.borrow();
        guard.buffer(index)?;
        Ok(BufferRef { guard, index })
    }

    #[inline]
    pub fn get_mut(&self, index: BufferIndex) -> CoreResult<BufferRefMut<'_>> {
        let mut guard = self.inner.borrow_mut();
        guard.buffer_mut(index)?;
        Ok(BufferRefMut { guard, index })
    }

    #[inline]
    pub fn with_buffer<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&Buffer) -> R,
    ) -> CoreResult<R> {
        let pool = self.inner.borrow();
        let buffer = pool.buffer(index)?;
        Ok(f(buffer))
    }

    #[inline]
    pub fn with_buffer_mut<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&mut Buffer) -> R,
    ) -> CoreResult<R> {
        let mut pool = self.inner.borrow_mut();
        let buffer = pool.buffer_mut(index)?;
        Ok(f(buffer))
    }

    #[inline]
    pub fn packet_cursor(&self, index: BufferIndex) -> CoreResult<BufferPacketCursor> {
        self.with_buffer(index, Buffer::packet_cursor)
    }

    #[inline]
    pub fn node_error(&self, index: BufferIndex) -> CoreResult<Option<BufferNodeError>> {
        self.with_buffer(index, Buffer::node_error)
    }

    #[inline]
    pub fn metadata(&self, index: BufferIndex) -> CoreResult<RouteMetadata> {
        Ok(self.inner.borrow().buffer(index)?.metadata().clone())
    }

    #[inline]
    pub fn with_metadata<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&RouteMetadata) -> R,
    ) -> CoreResult<R> {
        let pool = self.inner.borrow();
        let metadata = pool.buffer(index)?.metadata();
        Ok(f(metadata))
    }

    #[inline]
    pub fn with_metadata_mut<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&mut RouteMetadata) -> R,
    ) -> CoreResult<R> {
        let mut pool = self.inner.borrow_mut();
        let metadata = pool.buffer_mut(index)?.metadata_mut();
        Ok(f(metadata))
    }

    #[inline]
    pub fn advance(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        let mut pool = self.inner.borrow_mut();
        let buffer = pool.buffer_mut(index)?;
        if len > buffer.current_len {
            return Err(CoreError::internal("buffer advance exceeds current length"));
        }
        buffer.current_data += len;
        buffer.current_len -= len;
        Ok(())
    }

    #[inline]
    pub fn truncate_current(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        let mut pool = self.inner.borrow_mut();
        let buffer = pool.buffer_mut(index)?;
        if len > buffer.current_len {
            return Err(CoreError::internal(
                "buffer truncate extends current length",
            ));
        }
        buffer.current_len = len;
        Ok(())
    }

    #[inline]
    pub fn prepend(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        let mut pool = self.inner.borrow_mut();
        let buffer = pool.buffer_mut(index)?;
        if bytes.len() > buffer.current_data {
            return Err(CoreError::internal("buffer prepend exceeds headroom"));
        }
        buffer.current_data -= bytes.len();
        let start = buffer.current_data;
        let end = start + bytes.len();
        buffer.storage.as_mut_slice()[start..end].copy_from_slice(bytes);
        buffer.current_len += bytes.len();
        Ok(())
    }

    #[inline]
    pub fn append(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        self.inner.borrow_mut().append_chain(index, bytes)
    }

    #[inline]
    pub fn current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        Ok(self.inner.borrow().buffer(index)?.current().to_vec())
    }

    #[inline]
    pub fn copy_current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.current(index)
    }

    #[inline]
    pub fn is_chained(&self, index: BufferIndex) -> CoreResult<bool> {
        Ok(self.inner.borrow().buffer(index)?.next_buffer().is_some())
    }

    #[inline]
    pub fn copy_packet(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        let inner = self.inner.borrow();
        let buffer = inner.buffer(index)?;
        if buffer.next_buffer().is_none() {
            return Ok(buffer.current().to_vec());
        }
        inner.copy_current_chain(index)
    }

    #[inline]
    pub fn copy_current_chain(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.inner.borrow().copy_current_chain(index)
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
    fn alloc_chain(&mut self, metadata: RouteMetadata, bytes: &[u8]) -> CoreResult<BufferIndex> {
        if self.slot_capacity == 0 {
            return Err(CoreError::internal("buffer slot capacity must be nonzero"));
        }
        if bytes.len() <= self.slot_capacity {
            return self.alloc_slot(metadata, bytes);
        }

        let first_len = self.slot_capacity;
        let first = self.alloc_slot(metadata, &bytes[..first_len])?;
        let mut tail = first;
        let mut offset = first_len;
        let mut total_tail_len = 0usize;

        while offset < bytes.len() {
            let end = (offset + self.slot_capacity).min(bytes.len());
            let next = self.alloc_slot(RouteMetadata::default(), &bytes[offset..end])?;
            {
                let tail_buffer = self.buffer_mut(tail)?;
                tail_buffer.next_buffer = Some(next);
                tail_buffer.flags.insert(BufferFlags::NEXT_PRESENT);
            }
            total_tail_len += end - offset;
            tail = next;
            offset = end;
        }
        self.buffer_mut(first)?.total_len_not_including_first = total_tail_len;
        Ok(first)
    }

    #[inline]
    fn alloc_slot(&mut self, metadata: RouteMetadata, bytes: &[u8]) -> CoreResult<BufferIndex> {
        if bytes.len() > self.slot_capacity {
            return Err(CoreError::internal(format!(
                "buffer bytes exceed slot capacity: {} > {}",
                bytes.len(),
                self.slot_capacity
            )));
        }
        let slot = self
            .free
            .pop()
            .ok_or_else(|| CoreError::internal("buffer pool exhausted"))?;
        let entry = self
            .slots
            .get_mut(slot as usize)
            .ok_or_else(|| CoreError::internal("buffer slot out of bounds"))?;
        entry.generation = entry.generation.wrapping_add(1).max(1);
        entry.allocated = true;
        entry.buffer.reset(metadata, bytes)?;
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
    fn prefetch_read(&self, index: BufferIndex) {
        let Ok(buffer) = self.buffer(index) else {
            return;
        };
        prefetch_read_l1(std::ptr::from_ref(buffer).cast::<u8>());
        prefetch_read_l1(std::ptr::from_ref(buffer.metadata()).cast::<u8>());
        if !buffer.current().is_empty() {
            prefetch_read_l1(buffer.current().as_ptr());
            if buffer.current().len() > 64 {
                prefetch_read_l1(unsafe { buffer.current().as_ptr().add(64) });
            }
        }
        if let Some(next) = buffer.next_buffer()
            && let Ok(next_buffer) = self.buffer(next)
        {
            prefetch_read_l1(std::ptr::from_ref(next_buffer).cast::<u8>());
        }
    }

    #[inline]
    fn free_chain(&mut self, index: BufferIndex) {
        let mut next = Some(index);
        while let Some(index) = next {
            if index.pool_id != self.pool_id {
                return;
            }
            let Some(entry) = self.slots.get_mut(index.slot as usize) else {
                return;
            };
            if entry.generation != index.generation {
                return;
            }
            if entry.allocated {
                next = entry.buffer.next_buffer();
                entry.allocated = false;
                entry.buffer.reset_for_free();
                self.in_use = self.in_use.saturating_sub(1);
                self.free.push(index.slot);
            } else {
                next = None;
            }
        }
    }

    #[inline]
    fn copy_current_chain(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut next = Some(index);
        while let Some(index) = next {
            let buffer = self.buffer(index)?;
            bytes.extend_from_slice(buffer.current());
            next = buffer.next_buffer;
        }
        Ok(bytes)
    }

    #[inline]
    fn append_chain(&mut self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        let mut tail = index;
        while let Some(next) = self.buffer(tail)?.next_buffer {
            tail = next;
        }

        let taken = self.buffer_mut(tail)?.append_in_place(bytes);
        let mut remaining = &bytes[taken..];
        while !remaining.is_empty() {
            let take = remaining.len().min(self.slot_capacity);
            let next = self.alloc_slot(RouteMetadata::default(), &remaining[..take])?;
            {
                let tail_buffer = self.buffer_mut(tail)?;
                tail_buffer.next_buffer = Some(next);
                tail_buffer.flags.insert(BufferFlags::NEXT_PRESENT);
            }
            tail = next;
            remaining = &remaining[take..];
        }
        self.refresh_chain_lengths(index)
    }

    #[inline]
    fn refresh_chain_lengths(&mut self, index: BufferIndex) -> CoreResult<()> {
        let mut total = 0usize;
        let mut next = self.buffer(index)?.next_buffer;
        while let Some(index) = next {
            let buffer = self.buffer(index)?;
            total += buffer.current_len;
            next = buffer.next_buffer;
        }
        self.buffer_mut(index)?.total_len_not_including_first = total;
        Ok(())
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

#[macro_export]
macro_rules! for_each_buffer_frame_index {
    ($runtime:expr, $frame:expr, |$index:ident| $body:block) => {{
        use $crate::{BufferFramePairBatch, BufferFrameQuadBatch, FrameBatchWidth};

        (|| -> hammer_core::error::CoreResult<()> {
            match $runtime.preferred_frame_batch_width() {
                FrameBatchWidth::Quad => {
                    let mut cursor = $frame.quad_batch_cursor();
                    cursor.prefetch_next_quad($runtime);
                    while let Some(batch) = cursor.next() {
                        cursor.prefetch_next_quad($runtime);
                        match batch {
                            BufferFrameQuadBatch::Quad(indices) => {
                                let $index = indices[0];
                                $body?;
                                let $index = indices[1];
                                $body?;
                                let $index = indices[2];
                                $body?;
                                let $index = indices[3];
                                $body?;
                            }
                            BufferFrameQuadBatch::Pair(indices) => {
                                let $index = indices[0];
                                $body?;
                                let $index = indices[1];
                                $body?;
                            }
                            BufferFrameQuadBatch::Single(value) => {
                                let $index = value;
                                $body?;
                            }
                        }
                    }
                }
                FrameBatchWidth::Pair => {
                    let mut cursor = $frame.pair_batch_cursor();
                    cursor.prefetch_next_pair($runtime);
                    while let Some(batch) = cursor.next() {
                        cursor.prefetch_next_pair($runtime);
                        match batch {
                            BufferFramePairBatch::Pair(indices) => {
                                let $index = indices[0];
                                $body?;
                                let $index = indices[1];
                                $body?;
                            }
                            BufferFramePairBatch::Single(value) => {
                                let $index = value;
                                $body?;
                            }
                        }
                    }
                }
            }
            Ok(())
        })()
    }};
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
    pub fn iter_indices(&self) -> std::slice::Iter<'_, BufferIndex> {
        self.indices.iter()
    }

    #[inline]
    pub fn drain_indices(&mut self) -> std::vec::Drain<'_, BufferIndex> {
        self.readiness.clear_pending();
        self.indices.drain(..)
    }

    #[inline]
    pub fn drain_pending(&mut self) -> std::vec::Drain<'_, BufferIndex> {
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
    pub fn prefetch_next_pair<G>(&self, runtime: &DataPlaneRuntime<G>) {
        for index in self.indices[self.offset..].iter().take(2).copied() {
            runtime.prefetch_read(index);
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
    pub fn prefetch_next_quad<G>(&self, runtime: &DataPlaneRuntime<G>) {
        for index in self.indices[self.offset..].iter().take(4).copied() {
            runtime.prefetch_read(index);
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

pub struct BufferRef<'pool> {
    guard: Ref<'pool, BufferPoolInner>,
    index: BufferIndex,
}

impl BufferRef<'_> {
    #[inline]
    pub fn metadata(&self) -> &RouteMetadata {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .metadata()
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
            .node_error()
    }

    #[inline]
    pub fn current(&self) -> &[u8] {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current()
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
    index: BufferIndex,
}

impl BufferRefMut<'_> {
    #[inline]
    pub fn metadata(&self) -> &RouteMetadata {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .metadata()
    }

    #[inline]
    pub fn metadata_mut(&mut self) -> &mut RouteMetadata {
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .metadata_mut()
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
            .node_error()
    }

    #[inline]
    pub fn set_node_error(&mut self, error: BufferNodeError) {
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .set_node_error(error);
    }

    #[inline]
    pub fn clear_node_error(&mut self) {
        self.guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .clear_node_error();
    }

    #[inline]
    pub fn set_packet_cursor(&mut self, cursor: BufferPacketCursor) {
        *self
            .guard
            .buffer_mut(self.index)
            .expect("buffer ref points to valid buffer")
            .packet_cursor_mut() = cursor;
    }

    #[inline]
    pub fn current(&self) -> &[u8] {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current()
    }

    #[inline]
    pub fn total_len_not_including_first(&self) -> usize {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .total_len_not_including_first()
    }
}
