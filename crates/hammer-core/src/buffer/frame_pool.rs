use super::*;

impl FramePool {
    #[inline]
    pub(super) fn with_capacity(frame_capacity: usize, slots: usize) -> Self {
        let mut available = vec![0u32; slots].into_boxed_slice();
        for offset in 0..slots {
            available[offset] =
                u32::try_from(slots - offset - 1).expect("frame slot index fits u32");
        }
        let frame_slots = (0..slots)
            .map(|_| FrameSlot {
                generation: 0,
                allocated: false,
                frame: Some(BufferFrame::with_capacity(frame_capacity)),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
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
    pub(super) fn in_use(&self) -> usize {
        self.inner.borrow().in_use
    }

    #[inline]
    pub(super) fn alloc_index(&self) -> DataPlaneResult<Index> {
        self.inner.borrow_mut().alloc_index()
    }

    #[inline]
    pub(super) fn return_index(&self, buffers: &BufferPool, index: Index) -> DataPlaneResult<()> {
        let mut pool = self.inner.borrow_mut();
        let frame = pool.frame_mut(index)?;
        buffers.drop_frame_indices(frame);
        frame.reset_for_pool_reuse();
        pool.release_index(index)
    }

    #[inline]
    pub(super) fn take_index(&self, index: Index) -> DataPlaneResult<BufferFrame> {
        self.inner.borrow_mut().take_frame(index)
    }

    #[inline]
    pub(super) fn return_taken_index(
        &self,
        index: Index,
        frame: BufferFrame,
    ) -> DataPlaneResult<()> {
        self.inner
            .borrow_mut()
            .return_frame_and_release(index, frame)
    }
}

impl FramePoolInner {
    #[inline]
    fn alloc_index(&mut self) -> DataPlaneResult<Index> {
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
    fn validate_index(&self, index: Index) -> DataPlaneResult<()> {
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
    fn entry_mut(&mut self, index: Index) -> DataPlaneResult<&mut FrameSlot> {
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
    fn frame_mut(&mut self, index: Index) -> DataPlaneResult<&mut BufferFrame> {
        self.entry_mut(index)?
            .frame
            .as_mut()
            .ok_or(DataPlaneError::FrameSlotCheckedOut.into())
    }

    #[inline]
    fn take_frame(&mut self, index: Index) -> DataPlaneResult<BufferFrame> {
        self.entry_mut(index)?
            .frame
            .take()
            .ok_or(DataPlaneError::FrameSlotCheckedOut.into())
    }

    #[inline]
    fn release_index(&mut self, index: Index) -> DataPlaneResult<()> {
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
    fn return_frame_and_release(
        &mut self,
        index: Index,
        frame: BufferFrame,
    ) -> DataPlaneResult<()> {
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
