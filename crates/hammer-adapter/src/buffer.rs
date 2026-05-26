use std::cell::{Ref, RefCell};
use std::rc::Rc;

use hammer_core::error::{CoreError, CoreResult};

use crate::RouteMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BufferIndex {
    slot: u32,
    generation: u32,
}

impl BufferIndex {
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
    slot_capacity: usize,
    slots: Vec<BufferSlot>,
    free: Vec<u32>,
    in_use: usize,
}

#[derive(Debug, Clone)]
pub struct BufferPool {
    inner: Rc<RefCell<BufferPoolInner>>,
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

    pub fn alloc(&self, metadata: RouteMetadata) -> CoreResult<BufferHandle> {
        self.alloc_with_bytes(metadata, &[])
    }

    pub fn alloc_with_bytes(
        &self,
        metadata: RouteMetadata,
        bytes: &[u8],
    ) -> CoreResult<BufferHandle> {
        let index = self.inner.borrow_mut().alloc_slot(metadata, bytes)?;
        Ok(BufferHandle {
            pool: Rc::clone(&self.inner),
            index,
            armed: true,
        })
    }

    pub fn import(&self, handoff: BufferHandoff) -> CoreResult<BufferHandle> {
        let mut inner = self.inner.borrow_mut();
        let mut segments = handoff.segments.into_iter();
        let first = segments
            .next()
            .ok_or_else(|| CoreError::internal("empty buffer handoff"))?;
        let first_index = inner.alloc_slot(first.metadata, &first.bytes)?;
        {
            let buffer = inner.buffer_mut(first_index)?;
            buffer.flags = first.flags;
            buffer.current_data = 0;
            buffer.current_len = first.bytes.len();
            buffer.data_len = first.bytes.len();
        }

        let mut tail = first_index;
        let mut total_tail_len = 0usize;
        for segment in segments {
            let next = inner.alloc_slot(RouteMetadata::default(), &segment.bytes)?;
            {
                let tail_buffer = inner.buffer_mut(tail)?;
                tail_buffer.next_buffer = Some(next);
                tail_buffer.flags.insert(BufferFlags::NEXT_PRESENT);
            }
            total_tail_len += segment.bytes.len();
            tail = next;
        }
        inner.buffer_mut(first_index)?.total_len_not_including_first = total_tail_len;
        drop(inner);

        Ok(BufferHandle {
            pool: Rc::clone(&self.inner),
            index: first_index,
            armed: true,
        })
    }

    pub fn get(&self, index: BufferIndex) -> CoreResult<BufferRef<'_>> {
        let guard = self.inner.borrow();
        guard.buffer(index)?;
        Ok(BufferRef { guard, index })
    }
}

impl BufferPoolInner {
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
            slot,
            generation: entry.generation,
        })
    }

    fn buffer(&self, index: BufferIndex) -> CoreResult<&Buffer> {
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

    fn export_chain(&mut self, index: BufferIndex) -> CoreResult<BufferHandoff> {
        let mut segments = Vec::new();
        let mut next = Some(index);
        while let Some(index) = next {
            let buffer = self.buffer(index)?;
            segments.push(BufferHandoffSegment {
                metadata: buffer.metadata.clone(),
                flags: buffer.flags,
                bytes: buffer.current().to_vec(),
            });
            next = buffer.next_buffer;
        }
        self.free_chain(index);
        Ok(BufferHandoff { segments })
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

pub struct BufferHandle {
    pool: Rc<RefCell<BufferPoolInner>>,
    index: BufferIndex,
    armed: bool,
}

impl BufferHandle {
    pub fn index(&self) -> BufferIndex {
        self.index
    }

    pub fn metadata(&self) -> RouteMetadata {
        self.pool
            .borrow()
            .buffer(self.index)
            .expect("live buffer handle points to valid buffer")
            .metadata()
            .clone()
    }

    pub fn with_metadata_mut<R>(&self, f: impl FnOnce(&mut RouteMetadata) -> R) -> R {
        let mut pool = self.pool.borrow_mut();
        let metadata = pool
            .buffer_mut(self.index)
            .expect("live buffer handle points to valid buffer")
            .metadata_mut();
        f(metadata)
    }

    pub fn current(&self) -> Vec<u8> {
        self.pool
            .borrow()
            .buffer(self.index)
            .expect("live buffer handle points to valid buffer")
            .current()
            .to_vec()
    }

    pub fn with_current_mut<R>(&self, f: impl FnOnce(&mut [u8]) -> R) -> R {
        let mut pool = self.pool.borrow_mut();
        let current = pool
            .buffer_mut(self.index)
            .expect("live buffer handle points to valid buffer")
            .current_mut();
        f(current)
    }

    pub fn next_buffer(&self) -> Option<BufferIndex> {
        self.pool
            .borrow()
            .buffer(self.index)
            .expect("live buffer handle points to valid buffer")
            .next_buffer()
    }

    pub fn advance(&mut self, len: usize) -> CoreResult<()> {
        let mut pool = self.pool.borrow_mut();
        let buffer = pool.buffer_mut(self.index)?;
        if len > buffer.current_len {
            return Err(CoreError::internal("buffer advance exceeds current length"));
        }
        buffer.current_data += len;
        buffer.current_len -= len;
        Ok(())
    }

    pub fn truncate_current(&mut self, len: usize) -> CoreResult<()> {
        let mut pool = self.pool.borrow_mut();
        let buffer = pool.buffer_mut(self.index)?;
        if len > buffer.current_len {
            return Err(CoreError::internal(
                "buffer truncate extends current length",
            ));
        }
        buffer.current_len = len;
        Ok(())
    }

    pub fn prepend(&mut self, bytes: &[u8]) -> CoreResult<()> {
        let mut pool = self.pool.borrow_mut();
        let buffer = pool.buffer_mut(self.index)?;
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

    pub fn append(&mut self, bytes: &[u8]) -> CoreResult<()> {
        self.pool.borrow_mut().append_chain(self.index, bytes)
    }

    pub fn copy_current_chain(&self) -> Vec<u8> {
        self.pool
            .borrow()
            .copy_current_chain(self.index)
            .expect("live buffer handle points to valid chain")
    }

    pub fn into_handoff(mut self) -> BufferHandoff {
        self.armed = false;
        self.pool
            .borrow_mut()
            .export_chain(self.index)
            .expect("live buffer handle exports valid chain")
    }
}

impl Drop for BufferHandle {
    fn drop(&mut self) {
        if self.armed {
            self.pool.borrow_mut().free_chain(self.index);
        }
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

#[derive(Debug, Clone)]
pub struct BufferHandoff {
    segments: Vec<BufferHandoffSegment>,
}

impl BufferHandoff {
    pub fn current_bytes(&self) -> Vec<u8> {
        let total_len = self
            .segments
            .iter()
            .map(|segment| segment.bytes.len())
            .sum();
        let mut bytes = Vec::with_capacity(total_len);
        for segment in &self.segments {
            bytes.extend_from_slice(&segment.bytes);
        }
        bytes
    }

    pub fn into_current_bytes(self) -> Vec<u8> {
        let total_len = self
            .segments
            .iter()
            .map(|segment| segment.bytes.len())
            .sum();
        let mut bytes = Vec::with_capacity(total_len);
        for segment in self.segments {
            bytes.extend_from_slice(&segment.bytes);
        }
        bytes
    }
}

#[derive(Debug, Clone)]
struct BufferHandoffSegment {
    metadata: RouteMetadata,
    flags: BufferFlags,
    bytes: Vec<u8>,
}
