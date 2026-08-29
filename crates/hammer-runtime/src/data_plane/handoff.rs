use super::*;

impl DataPlaneMain {
    pub fn run_ready_nodes(&self) -> RuntimeResult<usize> {
        self.drain_handoff_frames()?;
        self.nodes.run_ready_function_nodes(self)
    }

    #[inline]
    fn drain_handoff_frames(&self) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Ok(());
        };
        while let Some(handoff_frame) = handoff.pop() {
            let mut slot = HandoffSlotGuard::new(self, handoff_frame.slot);
            let node = self.nodes.node_for_handle(handoff_frame.target)?;
            let mut frame = self.buffers.get_next_frame(node)?;
            slot.push_into_frame(&mut frame)?;
            self.put_next_frame(frame)?;
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn drop_handoff_slot_owned(&self, slot: HandoffSlot) {
        for index in slot.iter() {
            self.drop_index_owned(index);
        }
    }

    #[inline]
    pub fn handoff_frame(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        frame: &mut BufferFrame,
    ) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(DataPlaneError::HandoffNotConfigured.into());
        };
        let pending = frame.len();
        if pending == 0 {
            return Ok(());
        }
        let target_node = self.nodes.node_for_handle(target)?;
        let slots = pending.div_ceil(HANDOFF_SLOT_CAPACITY);
        handoff.ensure_enqueue_slots(worker, slots)?;
        while !frame.is_empty() {
            let slot = HandoffSlot::from_prefix(frame.indices());
            let slot_len = slot.len();
            match handoff.enqueue_slot(worker, target, slot) {
                Ok(()) => {
                    frame.discard_prefix(slot_len);
                    self.set_worker_node_interrupt_pending(worker, target_node);
                }
                Err(err) => {
                    let (error, _) = err.into_parts();
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn handoff_indices(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        frame: &mut BufferFrame,
    ) -> RuntimeResult<()> {
        self.handoff_frame(worker, target, frame)
    }

    #[inline]
    pub fn handoff_index<N: NodeNext>(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        index: Index,
        continuation: Option<N>,
    ) -> RuntimeResult<()> {
        if let Some(next) = continuation {
            let node = self
                .current_node()
                .ok_or(RuntimeError::HandoffDispatchContextMissing)?;
            let resolved = self.nodes.node_next(node, next)?;
            self.get_buffer_mut(index)?.set_current_config(resolved);
        }
        let Some(handoff) = &self.handoff else {
            return Err(DataPlaneError::HandoffNotConfigured.into());
        };
        let target_node = self.nodes.node_for_handle(target)?;
        handoff.ensure_enqueue_slots(worker, 1)?;
        match handoff.enqueue_index(worker, target, index) {
            Ok(()) => {
                self.set_worker_node_interrupt_pending(worker, target_node);
                Ok(())
            }
            Err(err) => {
                let (error, _) = err.into_parts();
                Err(error.into())
            }
        }
    }
}
