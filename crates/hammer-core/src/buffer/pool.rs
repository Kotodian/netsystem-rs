use super::chain::BufferChain;
use super::prefetch::*;
use super::*;

impl DataPlaneBuffers {
    /// VPP-style runtime thread index: zero for main, one-based for workers.
    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.thread_index
    }

    #[inline]
    pub fn from_arenas(
        arenas: impl IntoIterator<Item = BufferPoolArena>,
        frame_slots: usize,
        thread_index: u32,
        requested_numa_node: u32,
    ) -> Self {
        let mut buffer_arenas = StaticNumaTable::new();
        for arena in arenas {
            let inserted = buffer_arenas.insert(arena.numa_node(), arena);
            debug_assert!(inserted.is_ok());
        }
        Self::with_arena_table(
            buffer_arenas,
            frame_slots,
            thread_index,
            requested_numa_node,
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
    pub(super) fn try_buffers(&self) -> DataPlaneResult<&BufferPool> {
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
    pub fn alloc_index(&self) -> DataPlaneResult<Index> {
        self.try_buffers()?.alloc_index()
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> DataPlaneResult<Index> {
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
    pub(super) fn drop_frame_indices(&self, frame: &mut BufferFrame) {
        for index in frame.drain_indices() {
            self.drop_index_owned(index);
        }
    }

    #[inline]
    pub(super) fn drop_owned_frame(&self, index: Index, frame: BufferFrame) {
        let mut frame = frame;
        self.drop_frame_indices(&mut frame);
        frame.reset_for_pool_reuse();
        let _ = self.frames.return_taken_index(index, frame);
    }

    #[inline]
    pub(super) fn drop_owned_frame_with_trace(
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
    fn alloc_frame(&self) -> DataPlaneResult<(Index, BufferFrame)> {
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
    pub fn get_next_frame(&self, next: NodeId) -> DataPlaneResult<Frame<Next>> {
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
    pub fn get_buffer(&self, index: Index) -> DataPlaneResult<BufferRef<'_>> {
        self.try_buffers()?.get(index)
    }

    #[inline]
    pub fn get_buffer_mut(&self, index: Index) -> DataPlaneResult<BufferRefMut<'_>> {
        self.try_buffers()?.get_mut(index)
    }

    #[inline]
    pub fn chain(&self, index: Index) -> impl Iterator<Item = DataPlaneResult<BufferRef<'_>>> + '_ {
        BufferChain::new(self.try_buffers(), index)
    }

    #[inline]
    pub fn node_error_index(&self, index: Index) -> DataPlaneResult<Option<NodeErrorIndex>> {
        self.try_buffers()?.node_error_index(index)
    }

    #[inline]
    pub fn current_config(&self, index: Index) -> DataPlaneResult<NodeId> {
        self.try_buffers()?.current_config(index)
    }

    #[inline]
    pub fn set_current_config(&self, index: Index, next: NodeId) -> DataPlaneResult<()> {
        self.try_buffers()?.set_current_config(index, next)
    }

    #[inline]
    pub fn advance(&self, index: Index, displacement: isize) -> DataPlaneResult<()> {
        self.try_buffers()?.advance(index, displacement)
    }

    #[inline]
    pub fn append(&self, index: Index, bytes: &[u8]) -> DataPlaneResult<()> {
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
    pub fn buffer_arenas(&self) -> impl Iterator<Item = BufferPoolArena> + '_ {
        self.buffer_pools.iter().map(|(_, pool)| pool.arena.clone())
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
        Self::with_capacity_on_numa(slot_capacity, slots, PageSize::Default, 0)
            .expect("create ordinary-page buffer arena")
    }

    #[inline]
    pub fn with_capacity_on_numa(
        slot_capacity: usize,
        slots: usize,
        page_size: PageSize,
        numa_node: u32,
    ) -> DataPlaneResult<Self> {
        if slot_capacity == 0 {
            return Err(BufferInvariant::SlotCapacityZero.into());
        }
        if slots == 0 {
            return Err(DataPlaneError::BufferArenaSlotsZero);
        }

        let total_slots = slots
            .checked_add(1)
            .ok_or(DataPlaneError::BufferArenaSizeOverflow)?;
        let slot_stride = align_up(
            buffer_data_offset()
                .checked_add(slot_capacity)
                .ok_or(DataPlaneError::BufferArenaSizeOverflow)?,
            BUFFER_CACHE_LINE_SIZE,
        );
        let region_size = slot_stride
            .checked_mul(total_slots)
            .ok_or(DataPlaneError::BufferArenaSizeOverflow)?;
        let region = PhysmemMap::create("buffers", region_size, page_size, numa_node)
            .map_err(|source| DataPlaneError::BufferArenaMapping { source })?;
        unsafe {
            ptr::write_bytes(region.base(), 0, region.size());
        }

        let slot_states = vec![
            BufferSlot {
                generation: 0,
                allocated: false,
            };
            total_slots
        ]
        .into_boxed_slice();
        let mut available_stack = Vec::with_capacity(slots);
        for i in 0..slots {
            let slot = u32::try_from(total_slots - i - 1).expect("buffer slot fits u32");
            available_stack.push(slot);
        }

        Ok(Self {
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
        })
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
    fn with_arena(arena: BufferPoolArena) -> Self {
        Self {
            arena,
            thread_cache: Rc::new(RefCell::new(BufferThreadCache::new())),
        }
    }

    #[inline]
    fn cached_free_len(&self) -> usize {
        self.thread_cache.borrow().cached_free_len()
    }

    #[inline]
    fn in_use(&self) -> usize {
        let mut arena = self.arena.inner.write();
        arena.fold_in_use();
        arena.in_use
    }

    #[inline]
    fn alloc_index(&self) -> DataPlaneResult<Index> {
        let mut cache = self.thread_cache.borrow_mut();
        let mut arena = self.arena.inner.write();
        arena.alloc_empty_chain(&mut cache)
    }

    #[inline]
    fn alloc_index_with_bytes(&self, bytes: &[u8]) -> DataPlaneResult<Index> {
        let mut cache = self.thread_cache.borrow_mut();
        let mut arena = self.arena.inner.write();
        arena.alloc_chain(&mut cache, bytes)
    }

    #[inline]
    fn prefetch_header(&self, index: Index) {
        self.arena.inner.read().prefetch_header(index);
    }

    #[inline]
    fn prefetch_read(&self, index: Index) {
        self.arena.inner.read().prefetch_read(index);
    }

    #[inline]
    fn prefetch_write(&self, index: Index) {
        self.arena.inner.read().prefetch_write(index);
    }

    #[inline]
    pub(super) fn drop_frame_indices(&self, frame: &mut BufferFrame) {
        let mut cache = self.thread_cache.borrow_mut();
        let mut pool = self.arena.inner.write();
        for index in frame.drain_indices() {
            pool.free_chain(&mut cache, index);
        }
        pool.fold_in_use();
    }

    #[inline]
    fn get(&self, index: Index) -> DataPlaneResult<BufferRef<'_>> {
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
    fn get_mut(&self, index: Index) -> DataPlaneResult<BufferRefMut<'_>> {
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
    fn current_config(&self, index: Index) -> DataPlaneResult<NodeId> {
        Ok(self.arena.inner.read().buffer(index)?.current_config())
    }

    #[inline]
    fn set_current_config(&self, index: Index, next: NodeId) -> DataPlaneResult<()> {
        let mut guard = self.arena.inner.write();
        guard.ensure_header_exclusive(index)?;
        guard.buffer_mut(index)?.set_current_config(next);
        Ok(())
    }

    #[inline]
    fn node_error_index(&self, index: Index) -> DataPlaneResult<Option<NodeErrorIndex>> {
        Ok(self.arena.inner.read().buffer(index)?.node_error_index())
    }

    #[inline]
    fn advance(&self, index: Index, displacement: isize) -> DataPlaneResult<()> {
        let mut pool = self.arena.inner.write();
        pool.advance(index, displacement)
    }

    #[inline]
    fn append(&self, index: Index, bytes: &[u8]) -> DataPlaneResult<()> {
        let mut cache = self.thread_cache.borrow_mut();
        self.arena
            .inner
            .write()
            .append_chain(&mut cache, index, bytes)
    }
}

impl BufferPoolInner {
    #[inline]
    fn slot_index(&self, slot: u32) -> DataPlaneResult<usize> {
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
    fn slot_offset(&self, slot: u32) -> DataPlaneResult<usize> {
        let slot = self.slot_index(slot)?;
        slot.checked_mul(self.slot_stride)
            .ok_or(BufferInvariant::SlotOffsetOverflow.into())
    }

    #[inline]
    fn slot_state(&self, slot: u32) -> DataPlaneResult<&BufferSlot> {
        let slot = self.slot_index(slot)?;
        Ok(&self.slot_states[slot])
    }

    #[inline]
    fn slot_state_mut(&mut self, slot: u32) -> DataPlaneResult<&mut BufferSlot> {
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
    fn buffer_raw_ptr(&self, slot: u32) -> DataPlaneResult<*mut Buffer> {
        let offset = self.slot_offset(slot)?;
        // SAFETY: `offset` is validated to land within the arena region and
        // each slot begins with an inline `Buffer` header.
        Ok(unsafe { self.region.base().add(offset).cast::<Buffer>() })
    }

    #[inline]
    fn buffer_at_slot(&self, slot: u32) -> DataPlaneResult<&Buffer> {
        let ptr = self.buffer_raw_ptr(slot)?;
        // SAFETY: the slot layout guarantees that `ptr` addresses a live inline
        // `Buffer` header for the lifetime of `&self`.
        Ok(unsafe { &*ptr })
    }

    #[inline]
    fn buffer_at_slot_mut(&mut self, slot: u32) -> DataPlaneResult<&mut Buffer> {
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
    pub(crate) fn next_buffer(&self, index: Index) -> DataPlaneResult<Option<Index>> {
        Ok(self
            .buffer(index)?
            .next_buffer_slot()
            .and_then(|slot| self.index_from_slot(slot)))
    }

    #[inline]
    fn advance(&mut self, index: Index, displacement: isize) -> DataPlaneResult<()> {
        if displacement == 0 {
            return Ok(());
        }

        if displacement < 0 {
            let rewind = displacement.unsigned_abs();
            let buffer = self.buffer(index)?;
            if rewind > buffer.available_headroom() {
                return Err(BufferInvariant::RewindExceedsHeadroom.into());
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
            .map_err(|_| BufferInvariant::AdvanceDisplacementOutOfRange)?;

        let first = self.buffer(index)?;
        if self.next_buffer(index)?.is_none() {
            if len > first.current_len() {
                return Err(BufferInvariant::AdvanceExceedsCurrentLength.into());
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
            .ok_or(BufferInvariant::ChainLengthOverflow)?;
        if len > original_total_len {
            return Err(BufferInvariant::AdvanceExceedsCurrentLength.into());
        }

        let mut remaining = len;
        let mut current = Some(index);
        while remaining != 0 {
            let current_index = current.ok_or(BufferInvariant::ChainAdvanceLostSegment)?;
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
            let current_index = current.ok_or(BufferInvariant::ChainAdvanceLostSegment)?;
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
            let current_index = current.ok_or(BufferInvariant::ChainAdvanceLostSegment)?;
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
            .ok_or(BufferInvariant::ChainLengthOverflow)?;
        let first_current_len = self.buffer(index)?.current_len();
        let tail_len = remaining_total_len
            .checked_sub(first_current_len)
            .ok_or(BufferInvariant::ChainLengthOverflow)?;
        self.buffer_mut(index)?
            .set_total_len_not_including_first(tail_len)
    }

    #[inline]
    fn alloc_chain(
        &mut self,
        cache: &mut BufferThreadCache,
        bytes: &[u8],
    ) -> DataPlaneResult<Index> {
        if self.slot_capacity == 0 {
            return Err(BufferInvariant::SlotCapacityZero.into());
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
    fn alloc_empty_chain(&mut self, cache: &mut BufferThreadCache) -> DataPlaneResult<Index> {
        if self.slot_capacity == 0 {
            return Err(BufferInvariant::SlotCapacityZero.into());
        }
        self.alloc_slot_empty_fast(cache, 0)
    }

    #[inline]
    fn alloc_slot(
        &mut self,
        cache: &mut BufferThreadCache,
        bytes: &[u8],
    ) -> DataPlaneResult<Index> {
        if bytes.len() > self.slot_capacity {
            return Err(BufferInvariant::BytesExceedCapacity {
                length: bytes.len(),
                capacity: self.slot_capacity,
            }
            .into());
        }
        self.alloc_slot_with(cache, |buffer, data_size| buffer.reset(data_size, bytes))
    }

    #[inline]
    fn alloc_slot_with(
        &mut self,
        cache: &mut BufferThreadCache,
        reset: impl FnOnce(&mut Buffer, usize) -> DataPlaneResult<()>,
    ) -> DataPlaneResult<Index> {
        let (slot, generation) = loop {
            let slot = match cache.pop() {
                Some(slot) => slot,
                None => {
                    self.refill_cache_batch(cache);
                    cache.pop().ok_or(BufferInvariant::PoolExhausted)?
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
    ) -> DataPlaneResult<Index> {
        let (slot, generation) = loop {
            let slot = match cache.pop() {
                Some(slot) => slot,
                None => {
                    self.refill_cache_batch(cache);
                    cache.pop().ok_or(BufferInvariant::PoolExhausted)?
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
    fn validate_pool_index(&self, index: Index) -> DataPlaneResult<()> {
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
    fn validate_allocated_index(&self, index: Index) -> DataPlaneResult<()> {
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
    pub(crate) fn buffer(&self, index: Index) -> DataPlaneResult<&Buffer> {
        self.validate_allocated_index(index)?;
        self.buffer_at_slot(index.slot)
    }

    #[inline]
    pub(super) fn buffer_mut(&mut self, index: Index) -> DataPlaneResult<&mut Buffer> {
        self.validate_allocated_index(index)?;
        self.buffer_at_slot_mut(index.slot)
    }

    #[inline]
    pub(super) fn ensure_header_exclusive(&self, index: Index) -> DataPlaneResult<()> {
        let buffer = self.buffer(index)?;
        if buffer.ref_count() == 1 {
            return Ok(());
        }
        Err(BufferInvariant::HeaderNotExclusive.into())
    }

    #[inline]
    pub(super) fn ensure_writable(&self, index: Index) -> DataPlaneResult<()> {
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
    fn append_chain(
        &mut self,
        cache: &mut BufferThreadCache,
        index: Index,
        bytes: &[u8],
    ) -> DataPlaneResult<()> {
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
                .ok_or(BufferInvariant::ChainLengthOverflow)?;
            tail = next;
            remaining = &remaining[take..];
        }
        let first_tail_len = original_tail_len
            .checked_add(added_tail_len)
            .ok_or(BufferInvariant::ChainLengthOverflow)?;
        self.buffer_mut(index)?
            .set_total_len_not_including_first(first_tail_len)
    }
}
