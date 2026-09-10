use super::*;

impl DataPlaneMain {
    pub fn run_ready_nodes(&mut self) -> RuntimeResult<usize> {
        self.drain_handoff_frames()?;
        self.run_ready_function_nodes()
    }

    #[inline]
    fn drain_handoff_frames(&self) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Ok(());
        };
        while let Some(handoff_frame) = handoff.pop() {
            // vlib/handoff.c copies raw indices into the destination Frame.
            // Enqueue validated the target; graph refork preserves its identity.
            let mut frame = self
                .get_frame_to_node(handoff_frame.target)
                .expect("queued handoff target remains registered");
            frame.set_vector_count(handoff_frame.slot.len());
            for (destination, index) in frame
                .vector_args_mut()
                .iter_mut()
                .zip(handoff_frame.slot.iter())
            {
                *destination = index;
            }
            self.put_frame_to_node(handoff_frame.target, frame)
                .expect("queued handoff target accepts its Frame");
        }
        Ok(())
    }

    /// Transfer packet release obligations to the destination Worker.
    /// On success the input vectors remain unchanged but are no longer owned
    /// by the source. On error the Frame contains only the untransferred suffix.
    #[inline]
    pub fn handoff_frame(
        &mut self,
        worker: DataWorkerId,
        target: NodeId,
        frame: &mut Frame,
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
        let mut transferred = 0;
        for indices in frame.vector_args().chunks(HANDOFF_SLOT_CAPACITY) {
            let slot = HandoffSlot::from_prefix(indices);
            match handoff.enqueue_slot(worker, target, slot) {
                Ok(()) => {
                    transferred += indices.len();
                    self.set_worker_node_interrupt_pending(worker, target);
                }
                Err(err) => {
                    let (error, _) = err.into_parts();
                    // Another producer can fill the queue after the capacity
                    // check. Already published indices belong to the receiver.
                    frame.vector_args_mut().copy_within(transferred.., 0);
                    frame.set_vector_count(pending - transferred);
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn handoff_index(
        &mut self,
        worker: DataWorkerId,
        target: NodeId,
        index: u32,
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

    fn local_input(
        runtime: &mut DataPlaneMain,
        data: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let processed_vectors = frame.len();
        (|| {
            assert_eq!(runtime.thread_index(), 2);
            assert_eq!(data.usize_word(0).unwrap(), 1);
            for &index in frame.vector_args() {
                let buffer = runtime.buffer(index);
                assert_eq!(buffer.current_config_index(), 0x1234_5678);
                assert_eq!(buffer.current(), &[0x45; 20]);
                assert_eq!(buffer.ref_count(), 1);
            }
        })();
        runtime.buffer_free(frame.vector_args());
        processed_vectors
    }

    // vlib/handoff.c delivers queued indices directly to hqm->node_index.
    // Drive source enqueue and destination dispatch directly as a unit test.
    #[test]
    fn handoff_preserves_feature_cursor_and_transfers_buffer_ownership() {
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_infra::main_heap::init_default().unwrap();
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                3,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        hammer_infra::main_heap::init_default().unwrap();
        let source = DataPlaneMain::new(DataPlaneBufferConfig {
            thread_index: 1,
            ..Default::default()
        });
        source
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(local_input, NodeRuntime::empty(), None, &[], None),
            )
            .unwrap();
        let target = source
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    local_input,
                    NodeRuntime::from_words([1, 0, 0, 0]),
                    None,
                    &[],
                    None,
                ),
            )
            .unwrap();
        let handoff = DataPlaneHandoff::with_node_capacity(2, 2, 2);
        let mut source =
            DataPlaneMain::attach_handoff_worker(source, handoff.worker(DataWorkerId::new(0)));
        let receiver = handoff.worker(DataWorkerId::new(1));
        let (nodes, simd_bytes, _, trace_control) = source.worker_parts();
        let mut receiver =
            DataPlaneMain::new_worker(nodes, simd_bytes, Some(receiver), trace_control, 2, 0)
                .unwrap();

        let destination = DataWorkerId::new(1);
        let mut frame = Frame::<(), u32, ()>::new(0);
        for _ in 0..33 {
            let mut index = u32::MAX;
            assert_eq!(
                source.buffer_add_data(&mut index, &[0x45; 20]),
                (&[0x45; 20]).len()
            );
            source
                .buffer_mut(index)
                .set_current_config_index(0x1234_5678);
            {
                let count = frame.len();
                frame.set_vector_count(count + 1);
                frame.vector_args_mut()[count] = index;
            }
        }
        let index = frame.vector_args()[0];
        let absent = NodeId::new(u32::MAX);
        assert!(matches!(
            source.handoff_index(destination, absent, index),
            Err(RuntimeError::NodeNotRegistered { node }) if node == absent
        ));
        source.handoff_index(destination, target, index).unwrap();
        let count = frame.len();
        frame.vector_args_mut().copy_within(1..count, 0);
        frame.set_vector_count(count - 1);
        let index = frame.vector_args()[0];
        source.handoff_index(destination, target, index).unwrap();
        let count = frame.len();
        frame.vector_args_mut().copy_within(1..count, 0);
        frame.set_vector_count(count - 1);

        let index = frame.vector_args()[0];
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
        assert_eq!(source.buffer(index).current_config_index(), 0x1234_5678);
        assert_eq!(source.buffer(index).ref_count(), 1);
        let cached_free = receiver.cached_free_buffers();
        assert_eq!(receiver.run_ready_nodes().unwrap(), 2);
        assert_eq!(receiver.nodes().frames_in_use(), 0);
        assert_eq!(receiver.cached_free_buffers(), cached_free + 2);

        // Retry the unchanged source frame, spanning two queue slots.
        for _ in 0..2 {
            let mut index = u32::MAX;
            assert_eq!(
                source.buffer_add_data(&mut index, &[0x45; 20]),
                (&[0x45; 20]).len()
            );
            source
                .buffer_mut(index)
                .set_current_config_index(0x1234_5678);
            {
                let count = frame.len();
                frame.set_vector_count(count + 1);
                frame.vector_args_mut()[count] = index;
            }
        }
        source
            .handoff_frame(destination, target, &mut frame)
            .unwrap();
        assert_eq!(frame.len(), 33);
        assert_eq!(receiver.run_ready_nodes().unwrap(), 2);
        assert_eq!(receiver.nodes().frames_in_use(), 0);
        assert_eq!(receiver.cached_free_buffers(), cached_free + 35);
        assert_eq!(receiver.run_ready_nodes().unwrap(), 0);
    }
}
