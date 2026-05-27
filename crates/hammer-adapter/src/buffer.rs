use std::cell::{Cell, Ref, RefCell};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use hammer_core::error::{CoreError, CoreResult};

use crate::RouteMetadata;

pub const DEFAULT_BUFFER_FRAME_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BufferIndex {
    pool_id: u64,
    slot: u32,
    generation: u32,
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

    pub fn empty() -> Self {
        Self(0)
    }

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

#[derive(Debug)]
pub struct Buffer {
    metadata: RouteMetadata,
    flags: BufferFlags,
    current_data: usize,
    current_len: usize,
    data_len: usize,
    next_buffer: Option<BufferIndex>,
    total_len_not_including_first: usize,
    storage: Vec<u8>,
}

impl Buffer {
    fn new(slot_capacity: usize, metadata: RouteMetadata, bytes: &[u8]) -> CoreResult<Self> {
        if bytes.len() > slot_capacity {
            return Err(CoreError::internal(format!(
                "buffer bytes exceed slot capacity: {} > {}",
                bytes.len(),
                slot_capacity
            )));
        }
        let mut storage = vec![0; slot_capacity];
        storage[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            metadata,
            flags: BufferFlags::empty(),
            current_data: 0,
            current_len: bytes.len(),
            data_len: bytes.len(),
            next_buffer: None,
            total_len_not_including_first: 0,
            storage,
        })
    }

    pub fn metadata(&self) -> &RouteMetadata {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut RouteMetadata {
        &mut self.metadata
    }

    pub fn flags(&self) -> BufferFlags {
        self.flags
    }

    pub fn current_data(&self) -> usize {
        self.current_data
    }

    pub fn current_len(&self) -> usize {
        self.current_len
    }

    pub fn data_len(&self) -> usize {
        self.data_len
    }

    pub fn next_buffer(&self) -> Option<BufferIndex> {
        self.next_buffer
    }

    pub fn total_len_not_including_first(&self) -> usize {
        self.total_len_not_including_first
    }

    pub fn current(&self) -> &[u8] {
        &self.storage[self.current_data..self.current_data + self.current_len]
    }

    pub fn current_mut(&mut self) -> &mut [u8] {
        let start = self.current_data;
        let end = start + self.current_len;
        &mut self.storage[start..end]
    }

    fn available_tail(&self) -> usize {
        self.storage
            .len()
            .saturating_sub(self.current_data + self.current_len)
    }

    fn append_in_place(&mut self, bytes: &[u8]) -> usize {
        let take = bytes.len().min(self.available_tail());
        if take == 0 {
            return 0;
        }
        let start = self.current_data + self.current_len;
        let end = start + take;
        self.storage[start..end].copy_from_slice(&bytes[..take]);
        self.current_len += take;
        self.data_len = self.data_len.max(end);
        take
    }
}

#[derive(Debug)]
struct BufferSlot {
    generation: u32,
    buffer: Option<Buffer>,
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

#[derive(Debug, Clone)]
pub struct DataPlaneRuntime {
    buffers: BufferPool,
}

static NEXT_BUFFER_POOL_ID: AtomicU64 = AtomicU64::new(1);

fn next_buffer_pool_id() -> u64 {
    NEXT_BUFFER_POOL_ID.fetch_add(1, Ordering::Relaxed)
}

impl DataPlaneRuntime {
    pub fn with_buffer_capacity(slot_capacity: usize, slots: usize) -> Self {
        Self {
            buffers: BufferPool::with_capacity(slot_capacity, slots),
        }
    }

    pub fn buffers(&self) -> &BufferPool {
        &self.buffers
    }

    pub fn in_use_buffers(&self) -> usize {
        self.buffers.in_use()
    }

    pub fn frame(&self) -> BufferFrame {
        self.buffers.frame()
    }

    pub fn frame_with_capacity(&self, capacity: usize) -> BufferFrame {
        self.buffers.frame_with_capacity(capacity)
    }

    pub fn alloc_index(&self, metadata: RouteMetadata) -> CoreResult<BufferIndex> {
        self.buffers.alloc_index(metadata)
    }

    pub fn alloc_index_with_bytes(
        &self,
        metadata: RouteMetadata,
        bytes: &[u8],
    ) -> CoreResult<BufferIndex> {
        self.buffers.alloc_index_with_bytes(metadata, bytes)
    }

    pub fn free_index(&self, index: BufferIndex) {
        self.buffers.free_index(index);
    }

    pub fn free_frame(&self, frame: &mut BufferFrame) {
        self.buffers.free_frame(frame);
    }

    pub fn metadata(&self, index: BufferIndex) -> CoreResult<RouteMetadata> {
        self.buffers.metadata(index)
    }

    pub fn with_metadata_mut<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&mut RouteMetadata) -> R,
    ) -> CoreResult<R> {
        self.buffers.with_metadata_mut(index, f)
    }

    pub fn advance(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        self.buffers.advance(index, len)
    }

    pub fn truncate_current(&self, index: BufferIndex, len: usize) -> CoreResult<()> {
        self.buffers.truncate_current(index, len)
    }

    pub fn prepend(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        self.buffers.prepend(index, bytes)
    }

    pub fn append(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        self.buffers.append(index, bytes)
    }

    pub fn current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.buffers.current(index)
    }

    pub fn copy_current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.buffers.copy_current(index)
    }

    pub fn is_chained(&self, index: BufferIndex) -> CoreResult<bool> {
        self.buffers.is_chained(index)
    }

    pub fn copy_packet(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.buffers.copy_packet(index)
    }

    pub fn copy_current_chain(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.buffers.copy_current_chain(index)
    }
}

impl BufferPool {
    pub fn with_capacity(slot_capacity: usize, slots: usize) -> Self {
        let free = (0..slots)
            .rev()
            .map(|slot| u32::try_from(slot).expect("buffer slot index fits u32"))
            .collect();
        let slots = (0..slots)
            .map(|_| BufferSlot {
                generation: 0,
                buffer: None,
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

    pub fn in_use(&self) -> usize {
        self.inner.borrow().in_use
    }

    pub fn alloc_index(&self, metadata: RouteMetadata) -> CoreResult<BufferIndex> {
        self.alloc_index_with_bytes(metadata, &[])
    }

    pub fn alloc_index_with_bytes(
        &self,
        metadata: RouteMetadata,
        bytes: &[u8],
    ) -> CoreResult<BufferIndex> {
        self.inner.borrow_mut().alloc_chain(metadata, bytes)
    }

    pub fn free_index(&self, index: BufferIndex) {
        self.inner.borrow_mut().free_chain(index);
    }

    pub fn free_frame(&self, frame: &mut BufferFrame) {
        let mut pool = self.inner.borrow_mut();
        for index in frame.drain_indices() {
            pool.free_chain(index);
        }
    }

    pub fn get(&self, index: BufferIndex) -> CoreResult<BufferRef<'_>> {
        let guard = self.inner.borrow();
        guard.buffer(index)?;
        Ok(BufferRef { guard, index })
    }

    pub fn metadata(&self, index: BufferIndex) -> CoreResult<RouteMetadata> {
        Ok(self.inner.borrow().buffer(index)?.metadata().clone())
    }

    pub fn with_metadata_mut<R>(
        &self,
        index: BufferIndex,
        f: impl FnOnce(&mut RouteMetadata) -> R,
    ) -> CoreResult<R> {
        let mut pool = self.inner.borrow_mut();
        let metadata = pool.buffer_mut(index)?.metadata_mut();
        Ok(f(metadata))
    }

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

    pub fn prepend(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        let mut pool = self.inner.borrow_mut();
        let buffer = pool.buffer_mut(index)?;
        if bytes.len() > buffer.current_data {
            return Err(CoreError::internal("buffer prepend exceeds headroom"));
        }
        buffer.current_data -= bytes.len();
        let start = buffer.current_data;
        let end = start + bytes.len();
        buffer.storage[start..end].copy_from_slice(bytes);
        buffer.current_len += bytes.len();
        Ok(())
    }

    pub fn append(&self, index: BufferIndex, bytes: &[u8]) -> CoreResult<()> {
        self.inner.borrow_mut().append_chain(index, bytes)
    }

    pub fn current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        Ok(self.inner.borrow().buffer(index)?.current().to_vec())
    }

    pub fn copy_current(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.current(index)
    }

    pub fn is_chained(&self, index: BufferIndex) -> CoreResult<bool> {
        Ok(self.inner.borrow().buffer(index)?.next_buffer().is_some())
    }

    pub fn copy_packet(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        let inner = self.inner.borrow();
        let buffer = inner.buffer(index)?;
        if buffer.next_buffer().is_none() {
            return Ok(buffer.current().to_vec());
        }
        inner.copy_current_chain(index)
    }

    pub fn copy_current_chain(&self, index: BufferIndex) -> CoreResult<Vec<u8>> {
        self.inner.borrow().copy_current_chain(index)
    }

    pub fn frame(&self) -> BufferFrame {
        self.frame_with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY)
    }

    pub fn frame_with_capacity(&self, capacity: usize) -> BufferFrame {
        BufferFrame::with_capacity(capacity)
    }
}

impl BufferPoolInner {
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
        entry.buffer = Some(Buffer::new(self.slot_capacity, metadata, bytes)?);
        self.in_use += 1;
        Ok(BufferIndex {
            pool_id: self.pool_id,
            slot,
            generation: entry.generation,
        })
    }

    fn validate_pool_index(&self, index: BufferIndex) -> CoreResult<()> {
        if index.pool_id != self.pool_id {
            return Err(CoreError::internal("buffer index belongs to another pool"));
        }
        Ok(())
    }

    fn buffer(&self, index: BufferIndex) -> CoreResult<&Buffer> {
        self.validate_pool_index(index)?;
        let entry = self
            .slots
            .get(index.slot as usize)
            .ok_or_else(|| CoreError::internal("buffer slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale buffer index"));
        }
        entry
            .buffer
            .as_ref()
            .ok_or_else(|| CoreError::internal("buffer slot is free"))
    }

    fn buffer_mut(&mut self, index: BufferIndex) -> CoreResult<&mut Buffer> {
        self.validate_pool_index(index)?;
        let entry = self
            .slots
            .get_mut(index.slot as usize)
            .ok_or_else(|| CoreError::internal("buffer slot out of bounds"))?;
        if entry.generation != index.generation {
            return Err(CoreError::internal("stale buffer index"));
        }
        entry
            .buffer
            .as_mut()
            .ok_or_else(|| CoreError::internal("buffer slot is free"))
    }

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
            next = entry.buffer.as_ref().and_then(Buffer::next_buffer);
            if entry.buffer.take().is_some() {
                self.in_use = self.in_use.saturating_sub(1);
                self.free.push(index.slot);
            }
        }
    }

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
    readiness: Rc<FrameReadiness>,
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
    fn mark_pending(&self) {
        self.pending.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    fn clear_pending(&self) {
        self.pending.set(false);
    }

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
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            indices: Vec::with_capacity(capacity),
            readiness: Rc::new(FrameReadiness::default()),
        }
    }

    pub fn push_index(&mut self, index: BufferIndex) -> CoreResult<()> {
        if self.indices.len() == self.indices.capacity() {
            return Err(CoreError::internal("buffer frame capacity exceeded"));
        }
        self.indices.push(index);
        self.readiness.mark_pending();
        Ok(())
    }

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

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub fn has_pending(&self) -> bool {
        !self.indices.is_empty()
    }

    pub fn pending_len(&self) -> usize {
        self.indices.len()
    }

    pub fn capacity(&self) -> usize {
        self.indices.capacity()
    }

    pub fn reset(&mut self) {
        self.indices.clear();
        self.readiness.clear_pending();
    }

    pub fn clear(&mut self) {
        self.reset();
    }

    pub fn indices(&self) -> &[BufferIndex] {
        &self.indices
    }

    pub fn pending_indices(&self) -> &[BufferIndex] {
        &self.indices
    }

    pub fn iter_indices(&self) -> std::slice::Iter<'_, BufferIndex> {
        self.indices.iter()
    }

    pub fn drain_indices(&mut self) -> std::vec::Drain<'_, BufferIndex> {
        self.readiness.clear_pending();
        self.indices.drain(..)
    }

    pub fn drain_pending(&mut self) -> std::vec::Drain<'_, BufferIndex> {
        self.readiness.clear_pending();
        self.indices.drain(..)
    }

    pub fn pending(&self) -> BufferFramePending {
        BufferFramePending {
            readiness: Rc::clone(&self.readiness),
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

pub struct BufferRef<'pool> {
    guard: Ref<'pool, BufferPoolInner>,
    index: BufferIndex,
}

impl BufferRef<'_> {
    pub fn metadata(&self) -> &RouteMetadata {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .metadata()
    }

    pub fn current(&self) -> &[u8] {
        self.guard
            .buffer(self.index)
            .expect("buffer ref points to valid buffer")
            .current()
    }
}
