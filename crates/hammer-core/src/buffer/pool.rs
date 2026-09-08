use super::chain::BufferChain;
use super::prefetch::*;
use super::*;
use hammer_infra::thread_owned::ThreadOwnedError;

impl DataPlaneBuffers {
    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.thread_index
    }

    pub fn from_arenas(
        arenas: impl IntoIterator<Item = BufferPoolArena>,
        frame_slots: usize,
        thread_index: u32,
        requested_numa_node: u32,
    ) -> Self {
        let mut buffer_pools = StaticNumaTable::new();
        for arena in arenas {
            buffer_pools
                .insert(arena.numa_node(), arena)
                .expect("Buffer NUMA node fits the static table");
        }
        let active_numa_node = Self::resolve_numa_node(&buffer_pools, requested_numa_node);
        for pool in &BufferMain::global().pools {
            assert!(
                (thread_index as usize) < pool.workers.len(),
                "configured Buffer Worker index"
            );
        }
        Self {
            buffer_pools,
            active_numa_node,
            thread_index,
            frames: FramePool::with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY, frame_slots),
            frame_slots,
        }
    }

    #[inline]
    pub(super) fn try_buffers(&self) -> DataPlaneResult<&BufferPool> {
        let arena = self
            .buffer_pools
            .get(self.active_numa_node)
            .ok_or(DataPlaneError::ActiveNumaBufferPoolMissing)?;
        Ok(&BufferMain::global().pools[usize::from(arena.pool_index)])
    }

    pub fn active_numa_node(&self) -> u32 {
        self.active_numa_node
    }
    pub fn in_use_buffers(&self) -> usize {
        self.buffer_pools
            .iter()
            .map(|(_, arena)| {
                BufferMain::global().pools[usize::from(arena.pool_index)]
                    .free
                    .read()
                    .1
                    .iter()
                    .filter(|allocated| **allocated)
                    .count()
            })
            .sum()
    }
    pub fn cached_free_buffers(&self) -> usize {
        let pool = self.try_buffers().expect("active Buffer Pool exists");
        pool.bind_worker(self.thread_index);
        pool.workers[self.thread_index as usize]
            .borrow_mut()
            .expect("Buffer cache belongs to this Worker")
            .len
    }
    pub fn frames_in_use(&self) -> usize {
        self.frames.in_use()
    }
    pub fn frame_capacity(&self) -> usize {
        DEFAULT_BUFFER_FRAME_CAPACITY
    }
    pub fn frame_slots(&self) -> usize {
        self.frame_slots
    }

    #[inline]
    pub fn alloc_index(&self) -> DataPlaneResult<u32> {
        self.try_buffers()?.alloc_index(self.thread_index)
    }

    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> DataPlaneResult<u32> {
        let capacity = self.try_buffers()?.data_size.min(u16::MAX as usize);
        let head = self.alloc_index()?;
        let mut last = head;
        let mut remaining = bytes;
        loop {
            let count = remaining.len().min(capacity);
            self.get_buffer_mut(last)?
                .put_uninit(count as u16)
                .copy_from_slice(&remaining[..count]);
            remaining = &remaining[count..];
            if remaining.is_empty() {
                break;
            }
            let next = match self.alloc_index() {
                Ok(next) => next,
                Err(error) => {
                    self.drop_index_owned(head);
                    return Err(error);
                }
            };
            self.get_buffer_mut(last)?.set_next_buffer(Some(next));
            last = next;
        }
        let tail_length = bytes.len().saturating_sub(capacity);
        let set_length = self
            .get_buffer_mut(head)?
            .set_total_len_not_including_first(tail_length);
        if let Err(error) = set_length {
            self.drop_index_owned(head);
            return Err(error);
        }
        Ok(head)
    }

    /// Copy the current segment and opaque metadata into an independent slot.
    /// Chain links, trace ownership and errors are not inherited.
    pub fn alloc_index_from(&self, source: u32) -> DataPlaneResult<u32> {
        let source_pool = BufferMain::global().pool(source);
        let destination = source_pool.alloc_index(self.thread_index)?;
        let mut state = source_pool.free.write();
        let source_buffer = source_pool.buffer(source, &state.1);
        let offset = source_buffer.current_data_offset();
        let length = source_buffer.current_len();
        let bytes = source_buffer.current_ptr();
        let opaque = source_buffer.cacheline0.opaque;
        let opaque2 = source_buffer.opaque2;
        let buffer = source_pool.buffer_mut(destination, &mut state.1);
        // Source and destination use the same Pool layout; these values have
        // already been validated on the source and cannot fail here.
        buffer.set_current_window(isize::from(offset), length);
        buffer.cacheline0.opaque = opaque;
        buffer.opaque2 = opaque2;
        // SAFETY: the Pool write guard excludes any source mutation/release.
        // Allocation selected a distinct slot with the same valid window.
        unsafe {
            buffer
                .current_mut_ptr()
                .copy_from_nonoverlapping(bytes, length);
        }
        Ok(destination)
    }

    fn drop_index_owned(&self, index: u32) {
        self.drop_index_owned_with_trace(index, |_| {});
    }

    pub fn drop_index_owned_with_trace(&self, index: u32, mut release_trace: impl FnMut(u32)) {
        let mut current = Some(index);
        while let Some(index) = current {
            let pool = BufferMain::global().pool(index);
            pool.bind_worker(self.thread_index);
            let trace = {
                let mut cache = pool.workers[self.thread_index as usize]
                    .borrow_mut()
                    .expect("Buffer cache belongs to this Worker");
                let mut state = pool.free.write();
                let buffer = pool.buffer_mut(index, &mut state.1);
                current = buffer.next_buffer_slot();
                assert_ne!(buffer.ref_count(), 0, "live Buffer has a reference");
                buffer.cacheline0.ref_count -= 1;
                if buffer.ref_count() != 0 {
                    None
                } else {
                    let trace = buffer.take_trace_handle();
                    buffer.cacheline0 = pool.template;
                    state.1[pool.slot(index)] = false;
                    if cache.len == BUFFER_THREAD_CACHE_HIGH_WATER {
                        let start = cache.len - BUFFER_THREAD_CACHE_BATCH;
                        state.0.extend_from_slice(&cache.indices[start..cache.len]);
                        cache.len = start;
                    }
                    let offset = cache.len;
                    cache.indices[offset] = index;
                    cache.len += 1;
                    trace
                }
            };
            // Trace finalization may call into runtime state; never invoke it
            // while holding the Pool lock or borrowing its Worker cache.
            if let Some(trace) = trace {
                release_trace(trace);
            }
        }
    }

    pub fn prefetch_header(&self, index: u32) {
        prefetch_buffer_header(&self.get_buffer(index).expect("live Buffer index"));
    }
    pub fn prefetch_read(&self, index: u32) {
        let buffer = self.get_buffer(index).expect("live Buffer index");
        prefetch_buffer_header(&buffer);
        prefetch_buffer_cacheline1(&buffer);
        prefetch_buffer_data(&buffer);
    }
    pub fn prefetch_write(&self, index: u32) {
        let buffer = self.get_buffer(index).expect("live Buffer index");
        prefetch_buffer_header_write(&buffer);
        prefetch_buffer_cacheline1_write(&buffer);
        prefetch_buffer_data_write(&buffer);
    }

    pub(super) fn drop_frame_indices(&self, frame: &mut BufferFrame) {
        for index in frame.drain_indices() {
            self.drop_index_owned(index);
        }
    }
    pub(super) fn drop_owned_frame(&self, index: (u64, u32, u32), frame: BufferFrame) {
        self.drop_owned_frame_with_trace(index, frame, |_| {});
    }
    pub(super) fn drop_owned_frame_with_trace(
        &self,
        index: (u64, u32, u32),
        mut frame: BufferFrame,
        mut release_trace: impl FnMut(u32),
    ) {
        for buffer in frame.drain_indices() {
            self.drop_index_owned_with_trace(buffer, &mut release_trace);
        }
        frame.reset_for_pool_reuse();
        self.frames
            .return_taken_index(index, frame)
            .expect("return checked-out Frame");
    }
    fn alloc_frame(&self) -> DataPlaneResult<((u64, u32, u32), BufferFrame)> {
        let index = self.frames.alloc_index()?;
        match self.frames.take_index(index) {
            Ok(frame) => Ok((index, frame)),
            Err(error) => {
                self.frames
                    .return_index(self, index)
                    .expect("return reserved Frame");
                Err(error)
            }
        }
    }
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

    pub fn get_buffer(&self, index: u32) -> DataPlaneResult<BufferRef<'_>> {
        let pool = BufferMain::global().pool(index);
        let guard = pool.free.read();
        Ok(BufferRef {
            guard: spinning_top::guard::RwSpinlockReadGuard::map(guard, |state| {
                pool.buffer(index, &state.1)
            }),
        })
    }
    pub fn get_buffer_mut(&self, index: u32) -> DataPlaneResult<BufferRefMut<'_>> {
        let pool = BufferMain::global().pool(index);
        let guard = pool.free.write();
        assert_eq!(
            pool.buffer(index, &guard.1).ref_count(),
            1,
            "shared Buffer tails are immutable"
        );
        Ok(BufferRefMut {
            guard: spinning_top::guard::RwSpinlockWriteGuard::map(guard, |state| {
                pool.buffer_mut(index, &mut state.1)
            }),
        })
    }
    pub fn chain(&self, index: u32) -> impl Iterator<Item = DataPlaneResult<BufferRef<'_>>> + '_ {
        BufferChain::new(self, index)
    }
    pub fn node_error_index(&self, index: u32) -> DataPlaneResult<Option<NodeErrorIndex>> {
        Ok(self.get_buffer(index)?.node_error_index())
    }
    pub fn current_config_index(&self, index: u32) -> DataPlaneResult<u32> {
        Ok(self.get_buffer(index)?.current_config_index())
    }
    pub fn set_current_config_index(&self, index: u32, config_index: u32) -> DataPlaneResult<()> {
        self.get_buffer_mut(index)?
            .set_current_config_index(config_index);
        Ok(())
    }

    pub fn append(&self, index: u32, bytes: &[u8]) -> DataPlaneResult<()> {
        let mut last = index;
        loop {
            let buffer = self.get_buffer(last)?;
            assert_eq!(
                buffer.ref_count(),
                1,
                "chain append requires exclusive segments"
            );
            match buffer.next_buffer_slot() {
                Some(next) => last = next,
                None => break,
            }
        }
        let available = {
            let buffer = self.get_buffer(last)?;
            buffer
                .space_left_at_end()
                .min(u16::MAX as usize - buffer.current_len())
        };
        let count = available.min(bytes.len());
        // Reserve a complete suffix before publishing mutation through the
        // existing fallible append surface. Task 4 replaces this operation.
        let suffix = if count < bytes.len() {
            Some(self.alloc_index_with_bytes(&bytes[count..])?)
        } else {
            None
        };
        let tail_len = self.get_buffer(index)?.total_len_not_including_first();
        let added_tail = bytes.len() - if last == index { count } else { 0 };
        let total = tail_len
            .checked_add(added_tail)
            .filter(|length| u32::try_from(*length).is_ok());
        let Some(total) = total else {
            if let Some(suffix) = suffix {
                self.drop_index_owned(suffix);
            }
            return Err(BufferInvariant::ChainLengthOverflow.into());
        };
        {
            let mut buffer = self.get_buffer_mut(last)?;
            buffer
                .put_uninit(count as u16)
                .copy_from_slice(&bytes[..count]);
            if let Some(suffix) = suffix {
                buffer.set_next_buffer(Some(suffix));
            }
        }
        self.get_buffer_mut(index)?
            .set_total_len_not_including_first(total)
    }

    pub fn buffer_arenas(&self) -> impl Iterator<Item = BufferPoolArena> + '_ {
        self.buffer_pools.iter().map(|(_, arena)| arena.clone())
    }
    fn resolve_numa_node(
        pools: &StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
        requested: u32,
    ) -> u32 {
        if pools.get(requested).is_some() {
            return requested;
        }
        if pools.get(0).is_some() {
            return 0;
        }
        pools.iter().next().expect("at least one Buffer Pool").0
    }
}

impl BufferPoolArena {
    pub fn with_capacity(slot_capacity: usize, slots: usize) -> Self {
        Self::with_capacity_on_numa(slot_capacity, slots, PageSize::Default, 0)
            .expect("select published Buffer Pool")
    }
    pub fn with_capacity_on_numa(
        slot_capacity: usize,
        slots: usize,
        page_size: PageSize,
        numa_node: u32,
    ) -> DataPlaneResult<Self> {
        let main = BufferMain::global();
        let pool_index = *main.default_pool_by_numa.get(numa_node as usize).ok_or(
            DataPlaneError::NumaNodeExceedsStaticMemoryTable {
                numa_node,
                capacity: HAMMER_MAX_NUMA_NODES,
            },
        )?;
        if pool_index == u8::MAX {
            return Err(DataPlaneError::ActiveNumaBufferPoolMissing);
        }
        let pool = &main.pools[usize::from(pool_index)];
        assert_eq!(
            slot_capacity, pool.data_size,
            "runtime uses the published Buffer data size"
        );
        assert_ne!(slots, 0, "Buffer allocation policy is nonempty");
        assert_eq!(
            page_size.bytes().expect("published page size is available"),
            pool.mapping.page_size(),
            "runtime uses the published Buffer page size"
        );
        Ok(Self { pool_index })
    }
    pub fn numa_node(&self) -> u32 {
        BufferMain::global().pools[usize::from(self.pool_index)]
            .mapping
            .numa_node()
    }
}

impl BufferMain {
    /// Runtime implementation boundary for a live, Worker-owned Buffer.
    ///
    /// Packet Nodes use `DataPlaneMain::buffer`; this entry point exists only
    /// because Buffer storage and the Worker runtime live in different crates.
    ///
    /// # Safety
    /// The caller must retain a readable ownership obligation for `index`
    /// throughout the returned borrow. No owner may mutate or release this
    /// segment until the borrow ends. The runtime must bound the reference by
    /// its own Worker borrow, not by the process lifetime of Buffer Main.
    #[doc(hidden)]
    #[inline]
    pub unsafe fn buffer(&self, index: u32) -> &Buffer {
        let pool = self.pool(index);
        let state = pool.free.read();
        let buffer = std::ptr::from_ref(pool.buffer(index, &state.1));
        // SAFETY: Pool lookup validates mapping, alignment and allocation.
        // The caller, rather than this temporary allocation-state guard,
        // retains the readable Buffer obligation for the returned lifetime.
        unsafe { &*buffer }
    }

    /// Runtime implementation boundary for an exclusively owned Buffer.
    ///
    /// # Safety
    /// The caller must exclusively own `index` and retain that obligation for
    /// the complete returned borrow, excluding all other Buffer references,
    /// release, clone attachment and Worker Handoff. The runtime must bound the
    /// reference by its exclusive Worker borrow. A shared tail is immutable.
    #[doc(hidden)]
    #[inline]
    pub unsafe fn buffer_mut(&self, index: u32) -> &mut Buffer {
        let pool = self.pool(index);
        let mut state = pool.free.write();
        let buffer = pool.buffer_mut(index, &mut state.1);
        assert_eq!(buffer.ref_count(), 1, "shared Buffer tails are immutable");
        let buffer = std::ptr::from_mut(buffer);
        // SAFETY: the Pool validates this allocated slot and the reference
        // count excludes shared tails. The caller maintains exclusive Buffer
        // ownership after the allocation-state guard is released.
        unsafe { &mut *buffer }
    }

    pub(super) fn pool(&self, index: u32) -> &BufferPool {
        assert_ne!(index, 0, "Buffer Index zero is invalid");
        let offset = (index as usize) << 6;
        assert!(
            offset < self.buffer_mem_size,
            "Buffer index {index} exceeds the process span"
        );
        let address = self.buffer_mem_start + offset;
        let mapping_pool = self
            .pools
            .iter()
            .find(|pool| {
                let base = pool.mapping.base() as usize;
                address >= base && address - base < pool.mapping.size()
            })
            .expect("Buffer index belongs to a registered Pool");
        // Validate the address before reading its header. The header owns
        // Pool provenance; the address-range search only bounds this borrow.
        let state = mapping_pool.free.read();
        let pool_index = mapping_pool
            .buffer(index, &state.1)
            .cacheline0
            .buffer_pool_index;
        assert_eq!(
            pool_index, mapping_pool.index,
            "Buffer header names its backing Pool"
        );
        &self.pools[usize::from(pool_index)]
    }
}

impl BufferPool {
    fn bind_worker(&self, thread_index: u32) {
        let worker = self
            .workers
            .get(thread_index as usize)
            .expect("configured Buffer Worker index");
        match worker.borrow_mut() {
            Ok(_) => {}
            Err(ThreadOwnedError::NotInstalled) => {
                worker
                    .install(BufferThreadCache {
                        indices: [0; BUFFER_THREAD_CACHE_HIGH_WATER],
                        len: 0,
                    })
                    .expect("bind Buffer cache once on its Worker");
            }
            Err(error) => panic!("Buffer cache {thread_index} owner violation: {error}"),
        }
    }

    fn slot(&self, index: u32) -> usize {
        assert_ne!(index, 0, "Buffer Index zero is invalid");
        let address = BufferMain::global().buffer_mem_start + ((index as usize) << 6);
        assert!(
            address >= self.first_buffer,
            "Buffer index precedes Pool slots"
        );
        let offset = address - self.first_buffer;
        assert_eq!(
            offset % self.allocation_size,
            0,
            "Buffer index is a slot boundary"
        );
        let end = self.mapping.base() as usize + self.mapping.size();
        assert!(
            address < end - self.allocation_size,
            "Buffer index exceeds Pool slots"
        );
        assert_eq!(
            address / self.mapping.page_size(),
            (address + self.allocation_size) / self.mapping.page_size(),
            "Buffer index identifies a non-crossing page slot"
        );
        offset / self.allocation_size
    }

    // These private borrows are tied to the allocation-state lock guard. The
    // same guard protects all bytes in the mapping until direct Worker borrows
    // replace the existing shared-receiver API.
    pub(super) fn buffer<'a>(&'a self, index: u32, allocated: &'a [bool]) -> &'a Buffer {
        let slot = self.slot(index);
        assert!(allocated[slot], "Buffer index {index} is allocated");
        let offset = self.first_buffer - self.mapping.base() as usize + slot * self.allocation_size;
        // SAFETY: slot() proves alignment, mapping bounds and page containment.
        // Physmem supplies initialized zero-filled memory. The first cacheline
        // was written with a valid template; the read guard excludes mutation.
        unsafe { &*self.mapping.base().add(offset).cast::<Buffer>() }
    }
    pub(super) fn buffer_mut<'a>(
        &'a self,
        index: u32,
        allocated: &'a mut [bool],
    ) -> &'a mut Buffer {
        let slot = self.slot(index);
        assert!(allocated[slot], "Buffer index {index} is allocated");
        let offset = self.first_buffer - self.mapping.base() as usize + slot * self.allocation_size;
        // SAFETY: same slot validation as buffer(); the exclusive allocation-
        // state guard excludes every other borrow of this mapping.
        unsafe { &mut *self.mapping.base().add(offset).cast::<Buffer>() }
    }

    fn alloc_index(&self, thread_index: u32) -> DataPlaneResult<u32> {
        self.bind_worker(thread_index);
        let mut cache = self.workers[thread_index as usize]
            .borrow_mut()
            .expect("Buffer cache belongs to this Worker");
        let mut state = self.free.write();
        if cache.len == 0 {
            let count = state.0.len().min(BUFFER_THREAD_CACHE_BATCH);
            let start = state.0.len() - count;
            cache.indices[..count].copy_from_slice(&state.0[start..]);
            state.0.truncate(start);
            cache.len = count;
        }
        if cache.len == 0 {
            return Err(BufferInvariant::PoolExhausted.into());
        }
        cache.len -= 1;
        let index = cache.indices[cache.len];
        let slot = self.slot(index);
        assert!(
            !state.1[slot],
            "free Buffer cache cannot contain a live slot"
        );
        state.1[slot] = true;
        self.buffer_mut(index, &mut state.1).cacheline0 = self.template;
        Ok(index)
    }
}
