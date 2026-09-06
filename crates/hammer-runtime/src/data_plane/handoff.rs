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
            let mut frame = self.buffers.get_next_frame(handoff_frame.target)?;
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
        target: NodeId,
        frame: &mut BufferFrame,
    ) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(DataPlaneError::HandoffNotConfigured.into());
        };
        let pending = frame.len();
        if pending == 0 {
            return Ok(());
        }
        self.nodes.node_kind(target)?;
        let slots = pending.div_ceil(HANDOFF_SLOT_CAPACITY);
        handoff.ensure_enqueue_slots(worker, slots)?;
        while !frame.is_empty() {
            let slot = HandoffSlot::from_prefix(frame.indices());
            let slot_len = slot.len();
            match handoff.enqueue_slot(worker, target, slot) {
                Ok(()) => {
                    frame.discard_prefix(slot_len);
                    self.set_worker_node_interrupt_pending(worker, target);
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
    pub fn handoff_index(
        &self,
        worker: DataWorkerId,
        target: NodeId,
        index: Index,
    ) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(DataPlaneError::HandoffNotConfigured.into());
        };
        self.nodes.node_kind(target)?;
        handoff.ensure_enqueue_slots(worker, 1)?;
        match handoff.enqueue_index(worker, target, index) {
            Ok(()) => {
                self.set_worker_node_interrupt_pending(worker, target);
                Ok(())
            }
            Err(err) => {
                let (error, _) = err.into_parts();
                Err(error.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::DataPlaneHandoff;
    use crate::node::NodeDescriptor;

    fn local_input(runtime: &DataPlaneMain, data: NodeRuntimeData, frame: &mut BufferFrame) {
        assert_eq!(runtime.thread_index(), 2);
        assert_eq!(data.usize_word(0).unwrap(), 1);
        for &index in frame.indices() {
            let buffer = runtime.get_buffer(index).unwrap();
            assert_eq!(buffer.current_config_index(), 0x1234_5678);
            assert_eq!(buffer.current(), &[0x45; 20]);
            assert_eq!(buffer.ref_count(), 1);
        }
    }

    // vlib/handoff.c delivers queued indices directly to hqm->node_index.
    // Exercise that contract across OS threads without a NodeHandle or cursor rewrite.
    #[test]
    fn handoff_preserves_feature_cursor_and_transfers_buffer_ownership() {
        hammer_infra::main_heap::init_default().unwrap();
        let arena = BufferPoolArena::with_capacity(64, 64);
        let buffers = DataPlaneBuffers::from_arenas([arena.clone()], 8, 1, 0);
        let source = DataPlaneMain::from_buffers(buffers, native_simd_bytes()).unwrap();
        source
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(local_input, NodeRuntimeData::empty(), None, &[], None),
            )
            .unwrap();
        let target = source
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    local_input,
                    NodeRuntimeData::from_words([1, 0, 0, 0]),
                    None,
                    &[],
                    None,
                ),
            )
            .unwrap();
        let handoff = DataPlaneHandoff::new_shared_buffer_arena_with_node_capacity(2, 2, 2, arena);
        let source =
            DataPlaneMain::attach_handoff_worker(source, handoff.worker(DataWorkerId::new(0)));
        let receiver = handoff.worker(DataWorkerId::new(1));
        let (arenas, frame_slots, nodes, simd_bytes, _, trace_control) = source.worker_parts();
        let (send, receive) = std::sync::mpsc::channel();
        let (acknowledge, acknowledged) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let runtime = DataPlaneMain::from_worker_parts(
                arenas,
                frame_slots,
                nodes,
                simd_bytes,
                Some(receiver),
                trace_control,
                2,
                0,
            )
            .unwrap();
            for expected_frames in receive {
                assert_eq!(runtime.run_ready_nodes().unwrap(), expected_frames);
                assert_eq!(runtime.buffers().frames_in_use(), 0);
                acknowledge.send(()).unwrap();
            }
            assert_eq!(runtime.buffers().in_use_buffers(), 0);
        });

        let destination = DataWorkerId::new(1);
        let mut frame = BufferFrame::with_capacity(33);
        for _ in 0..33 {
            let index = source.alloc_index_with_bytes(&[0x45; 20]).unwrap();
            source
                .get_buffer_mut(index)
                .unwrap()
                .set_current_config_index(0x1234_5678);
            frame.push_index(index).unwrap();
        }
        let index = frame.indices()[0];
        let absent = NodeId::new(u32::MAX);
        assert!(matches!(
            source.handoff_index(destination, absent, index),
            Err(RuntimeError::NodeNotRegistered { node }) if node == absent
        ));
        source.handoff_index(destination, target, index).unwrap();
        frame.discard_prefix(1);
        let index = frame.indices()[0];
        source.handoff_index(destination, target, index).unwrap();
        frame.discard_prefix(1);

        let index = frame.indices()[0];
        assert!(matches!(
            source.handoff_index(destination, target, index),
            Err(RuntimeError::DataPlane(
                DataPlaneError::HandoffQueueExhausted
            ))
        ));
        assert!(matches!(
            source.handoff_frame(destination, target, &mut frame),
            Err(RuntimeError::DataPlane(
                DataPlaneError::HandoffQueueExhausted
            ))
        ));
        assert_eq!(frame.len(), 31);
        assert_eq!(source.current_config_index(index).unwrap(), 0x1234_5678);
        assert_eq!(source.buffers().in_use_buffers(), 33);
        send.send(2).unwrap();
        acknowledged.recv().unwrap();
        assert_eq!(source.buffers().in_use_buffers(), 31);

        // Retry the unchanged source frame, spanning two queue slots.
        for _ in 0..2 {
            let index = source.alloc_index_with_bytes(&[0x45; 20]).unwrap();
            source
                .get_buffer_mut(index)
                .unwrap()
                .set_current_config_index(0x1234_5678);
            frame.push_index(index).unwrap();
        }
        source
            .handoff_frame(destination, target, &mut frame)
            .unwrap();
        assert!(frame.is_empty());
        send.send(2).unwrap();
        acknowledged.recv().unwrap();
        assert_eq!(source.buffers().in_use_buffers(), 0);
        drop(send);
        worker.join().unwrap();
    }
}
