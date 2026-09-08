use super::*;
use hammer_infra::thread_owned::ThreadOwnedError;

impl BufferMain {
    #[doc(hidden)]
    pub fn cached_free_buffers(&self, thread_index: u32, numa_node: u32) -> usize {
        let pool = &self.pools[usize::from(self.default_pool(numa_node))];
        pool.bind_worker(thread_index);
        pool.workers[thread_index as usize]
            .borrow_mut()
            .expect("Buffer cache belongs to this Worker")
            .len
    }

    /// Copy one segment and both opaque regions, without chain or trace ownership.
    #[doc(hidden)]
    pub fn copy_no_chain(&self, thread_index: u32, source: u32) -> Option<u32> {
        let pool = self.pool(source);
        let mut indices = [0];
        if pool.alloc_indices(thread_index, &mut indices) == 0 {
            return None;
        }
        let destination = indices[0];
        // SAFETY: allocation owns a distinct destination with the source Pool's
        // layout; the source remains borrowed until its bytes and metadata copy.
        unsafe {
            let source = self.buffer(source);
            let buffer = self.buffer_mut(destination);
            buffer.set_current_window(
                isize::from(source.current_data_offset()),
                source.current_len(),
            );
            buffer.cacheline0.opaque = source.cacheline0.opaque;
            buffer.opaque2 = source.opaque2;
            buffer.current_mut().copy_from_slice(source.current());
        }
        Some(destination)
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
        // SAFETY: the caller retains the readable slot obligation; Pool
        // validation checks mapping bounds and slot alignment.
        unsafe { pool.buffer(index) }
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
        // SAFETY: the caller retains a live obligation. Inspect shared state
        // before creating any mutable reference to a possibly shared segment.
        assert_eq!(
            unsafe { pool.buffer(index) }.ref_count(),
            1,
            "shared Buffer tails are immutable"
        );
        // SAFETY: exclusivity was checked before constructing the reference.
        unsafe { pool.buffer_mut(index) }
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
        // SAFETY: the index is retained by the caller; slot validation precedes
        // reading immutable Pool provenance from the initialized header.
        let pool_index = unsafe { mapping_pool.buffer(index) }
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

    // These borrows require the caller's live slot ownership, not a Pool lock.
    unsafe fn buffer(&self, index: u32) -> &Buffer {
        #[cfg(debug_assertions)]
        assert!(
            self.known_allocated.lock().contains(&index),
            "Buffer {index} is allocated"
        );
        let slot = self.slot(index);
        let offset = self.first_buffer - self.mapping.base() as usize + slot * self.allocation_size;
        // SAFETY: slot validates alignment, mapping bounds and page containment;
        // the caller retains readable ownership of the initialized segment.
        unsafe { &*self.mapping.base().add(offset).cast::<Buffer>() }
    }

    unsafe fn buffer_mut(&self, index: u32) -> &mut Buffer {
        #[cfg(debug_assertions)]
        assert!(
            self.known_allocated.lock().contains(&index),
            "Buffer {index} is allocated"
        );
        let slot = self.slot(index);
        let offset = self.first_buffer - self.mapping.base() as usize + slot * self.allocation_size;
        // SAFETY: the same slot validation applies; the caller owns this segment
        // exclusively, including when restoring a slot removed from its cache.
        unsafe { &mut *self.mapping.base().add(offset).cast::<Buffer>() }
    }

    pub(super) fn alloc_indices(&self, thread_index: u32, indices: &mut [u32]) -> usize {
        self.bind_worker(thread_index);
        let mut cache = self.workers[thread_index as usize]
            .borrow_mut()
            .expect("Buffer cache belongs to this Worker");
        for (allocated, destination) in indices.iter_mut().enumerate() {
            if cache.len == 0 {
                let mut free = self.free.lock();
                let count = free.len().min(BUFFER_THREAD_CACHE_BATCH);
                let start = free.len() - count;
                cache.indices[..count].copy_from_slice(&free[start..]);
                free.truncate(start);
                cache.len = count;
            }
            if cache.len == 0 {
                return allocated;
            }
            cache.len -= 1;
            let index = cache.indices[cache.len];
            #[cfg(debug_assertions)]
            assert!(
                self.known_allocated.lock().insert(index),
                "Buffer {index} was free before allocation"
            );
            // SAFETY: removing this index from the Worker cache transfers
            // exclusive ownership; restore only the first-cache-line template.
            unsafe { self.buffer_mut(index) }.cacheline0 = self.template.clone();
            *destination = index;
        }
        indices.len()
    }
}

impl BufferMain {
    #[doc(hidden)]
    pub fn free_buffers(
        &self,
        thread_index: u32,
        indices: &[u32],
        follow_next: bool,
        mut release_trace: impl FnMut(u32),
    ) {
        for &index in indices {
            let mut current = Some(index);
            while let Some(index) = current {
                let pool = self.pool(index);
                pool.bind_worker(thread_index);
                let trace = {
                    let mut cache = pool.workers[thread_index as usize]
                        .borrow_mut()
                        .expect("Buffer cache belongs to this Worker");
                    // SAFETY: the caller retains this live segment until the
                    // reference decrement transfers its last release obligation.
                    let buffer = unsafe { pool.buffer(index) };
                    current = if follow_next {
                        buffer.next_buffer_slot()
                    } else {
                        None
                    };
                    assert_ne!(buffer.ref_count(), 0, "live Buffer has a reference");
                    // AcqRel pairs with clone retention and other releases; only
                    // the final owner may restore the template and publish the free slot.
                    let references = buffer
                        .cacheline0
                        .ref_count
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    if references != 1 {
                        None
                    } else {
                        // SAFETY: this decrement observed the final reference.
                        let buffer = unsafe { pool.buffer_mut(index) };
                        let trace = buffer.take_trace_handle();
                        buffer.cacheline0 = pool.template.clone();
                        #[cfg(debug_assertions)]
                        assert!(
                            pool.known_allocated.lock().remove(&index),
                            "Buffer {index} is released once"
                        );
                        if cache.len == BUFFER_THREAD_CACHE_HIGH_WATER {
                            let start = cache.len - BUFFER_THREAD_CACHE_BATCH;
                            pool.free
                                .lock()
                                .extend_from_slice(&cache.indices[start..cache.len]);
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
    }
}
