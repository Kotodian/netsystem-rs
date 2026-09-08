use super::*;
use hammer_core::graph::frame::FRAME_VECTOR_CAPACITY;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

const IS_ALLOCATED: u16 = 1 << 1;
const IS_PENDING: u16 = 1 << 2;
const FREE_AFTER_DISPATCH: u16 = 1 << 3;
const NO_APPEND: u16 = 1 << 14;
const ENQUEUE_OWNER: u16 = 1 << 15;

/// VPP vlib_next_frame_t. An enqueued allocation resides in Pending storage;
/// its index keeps it appendable until dispatch takes the allocation.
pub(super) struct NextFrame {
    pub(super) frame: Option<Box<Frame>>,
    pub(super) pending_index: Option<usize>,
    node: NodeId,
    pub(super) flags: u16,
    vectors_since_last_overflow: u32,
}

impl NodeMain {
    pub(super) fn next_frames_for_graph(
        graph: &NodeRuntimeInner,
    ) -> (Vec<NextFrame>, Vec<Vec<usize>>) {
        let mut frames = Vec::new();
        let mut indices = Vec::with_capacity(graph.next_nodes.len());
        for next_nodes in &graph.next_nodes {
            let mut slots = Vec::with_capacity(next_nodes.len());
            for destination in next_nodes {
                let Some(destination) = destination else {
                    slots.push(usize::MAX);
                    continue;
                };
                let flags = graph.nodes[destination.slot() as usize]
                    .runtime_data
                    .as_ref()
                    .expect("published node runtime is available")
                    .flags
                    & 1;
                slots.push(frames.len());
                frames.push(NextFrame {
                    frame: None,
                    pending_index: None,
                    node: *destination,
                    flags,
                    vectors_since_last_overflow: 0,
                });
            }
            indices.push(slots);
        }
        (frames, indices)
    }

    pub(super) fn refork_next_frames(&mut self, graph: &NodeRuntimeInner) {
        // vlib_worker_thread_node_refork detaches before recycling, then
        // replaces Next storage. Pending storage is not part of this operation.
        for next in &mut self.next_frames {
            if next.flags & IS_ALLOCATED != 0
                && let Some(frame) = next.frame.take()
            {
                self.frames.get_mut().recycle(frame);
            }
        }
        self.next_frames = Vec::new();
        let (frames, indices) = Self::next_frames_for_graph(graph);
        self.next_frames = frames;
        self.next_frame_indices = indices;
        self.enqueue_owners = vec![None; graph.nodes.len()];
    }

    /// Select the indexed destination enqueue owner, as in
    /// vlib_next_frame_change_ownership and vlib_get_next_frame_internal.
    pub(crate) fn prepare_next_frame(&mut self, source: NodeId, slot: u32) -> usize {
        let destination = self
            .node_next_slot(source, slot as usize)
            .expect("Next Frame names a registered destination");
        let source_index = source.slot() as usize;
        if self.next_frame_indices.len() <= source_index {
            self.next_frame_indices
                .resize_with(source_index + 1, Vec::new);
        }
        let indices = &mut self.next_frame_indices[source_index];
        if indices.len() <= slot as usize {
            indices.resize(slot as usize + 1, usize::MAX);
        }
        let index = if indices[slot as usize] == usize::MAX {
            let index = self.next_frames.len();
            indices[slot as usize] = index;
            self.next_frames.push(NextFrame {
                frame: None,
                pending_index: None,
                node: destination,
                flags: 0,
                vectors_since_last_overflow: 0,
            });
            index
        } else {
            indices[slot as usize]
        };
        assert_eq!(self.next_frames[index].node, destination);
        let destination_index = destination.slot() as usize;
        if self.enqueue_owners.len() <= destination_index {
            self.enqueue_owners.resize(destination_index + 1, None);
        }
        if self.enqueue_owners[destination_index] != Some(index) {
            if let Some(owner) = self.enqueue_owners[destination_index] {
                self.next_frames.swap(owner, index);
                if let Some(pending) = self.next_frames[index].pending_index {
                    self.pending_frames.get_mut()[pending].next_frame_index = Some(index);
                }
            } else {
                self.next_frames[index].flags |= ENQUEUE_OWNER;
            }
            self.enqueue_owners[destination_index] = Some(index);
        }
        assert_ne!(self.next_frames[index].flags & ENQUEUE_OWNER, 0);

        if self.next_frames[index].pending_index.is_none()
            && self.next_frames[index].flags & IS_PENDING != 0
            && let Some(frame) = self.next_frames[index].frame.as_mut()
            && frame.frame_flags & IS_PENDING == 0
        {
            frame.set_vector_count(0);
            frame.flags = 0;
            self.next_frames[index].flags &= !IS_PENDING;
        }

        let replace = if self.next_frames[index].pending_index.is_some()
            || self.next_frames[index].frame.is_some()
        {
            let frame = self.next_frame_mut(index);
            frame.len() == FRAME_VECTOR_CAPACITY || frame.frame_flags & NO_APPEND != 0
        } else {
            true
        };
        if replace {
            if let Some(pending) = self.next_frames[index].pending_index.take() {
                self.pending_frames.get_mut()[pending]
                    .frame
                    .as_mut()
                    .expect("Pending Frame owns its allocation")
                    .frame_flags |= FREE_AFTER_DISPATCH;
            }
            assert!(
                self.next_frames[index].frame.is_none(),
                "nonpending Next Frame is empty"
            );
            let (scalar, vector, aux) = self.frame_args_size(destination).unwrap();
            self.next_frames[index].frame =
                Some(self.frames.get_mut().allocate(scalar, vector, aux));
            self.next_frames[index].flags |= IS_ALLOCATED;
        }
        index
    }

    pub(crate) fn next_frame_mut(&mut self, index: usize) -> &mut Frame {
        if let Some(pending) = self.next_frames[index].pending_index {
            self.pending_frames.get_mut()[pending]
                .frame
                .as_mut()
                .expect("appendable Pending Frame has not entered dispatch")
        } else {
            self.next_frames[index]
                .frame
                .as_mut()
                .expect("Next Frame is allocated")
        }
    }

    pub(crate) fn put_next_frame_index(&mut self, index: usize, vectors_left: usize) {
        assert!(vectors_left <= FRAME_VECTOR_CAPACITY);
        let count = FRAME_VECTOR_CAPACITY - vectors_left;
        let frame = self.next_frame_mut(index);
        assert!(
            count >= frame.len(),
            "put retains previously enqueued vectors"
        );
        frame.set_vector_count(count);
        if count == 0 {
            return;
        }
        frame.frame_flags |= IS_PENDING;
        let next = &mut self.next_frames[index];
        if next.pending_index.is_none() {
            let pending = self.pending_frames.get_mut();
            next.pending_index = Some(pending.len());
            pending.push(PendingFrame {
                node: next.node,
                frame: next.frame.take(),
                next_frame_index: Some(index),
            });
        }
        next.flags |= IS_PENDING;
        next.vectors_since_last_overflow =
            next.vectors_since_last_overflow.wrapping_add(count as u32);
        self.readiness.mark_pending();
    }
}

impl DataPlaneMain {
    pub fn get_next_frame<V, A>(
        &mut self,
        _: &mut NodeRuntime,
        next_index: u32,
    ) -> (&mut [V], Option<&mut [A]>)
    where
        V: KnownLayout + FromBytes + Immutable + IntoBytes,
        A: KnownLayout + FromBytes + Immutable + IntoBytes,
    {
        let source = self
            .current_node()
            .expect("Next Frame requires an executing node");
        let target = self
            .nodes
            .node_next_slot(source, next_index as usize)
            .expect("registered next slot");
        let (scalar, vector, aux) = self.nodes.frame_args_size(target).unwrap();
        assert_eq!(usize::from(vector), core::mem::size_of::<V>());
        assert_eq!(usize::from(aux), core::mem::size_of::<A>());
        let index = self.nodes.prepare_next_frame(source, next_index);
        self.nodes
            .next_frame_mut(index)
            .next_args_mut::<V, A>(scalar)
    }

    pub fn put_next_frame(
        &mut self,
        runtime: &mut NodeRuntime,
        next_index: u32,
        vectors_left: usize,
    ) {
        let source = self
            .current_node()
            .expect("Next Frame requires an executing node");
        let index = self.nodes.next_frame_indices[source.slot() as usize][next_index as usize];
        if vectors_left < FRAME_VECTOR_CAPACITY {
            runtime.cached_next_index = next_index;
        }
        self.nodes.next_frames[index].flags |= runtime.flags & (1 << 5);
        self.nodes.put_next_frame_index(index, vectors_left);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet_graph() -> (NodeMain, NodeId, NodeId) {
        hammer_infra::main_heap::init_default().unwrap();
        let graph = NodeMain::default();
        let output = graph
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-output", 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        let input = graph
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-input", 1)),
                    &[output],
                    None,
                ),
            )
            .unwrap();
        (graph, input, output)
    }

    #[test]
    fn refork_recycles_allocated_next_frames() {
        let (main, input, output) = packet_graph();
        let auxiliary = main
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-auxiliary", 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        main.inner.borrow_mut().nodes[auxiliary.slot() as usize].frame_args_size = (8, 4, 2);
        main.add_node_next_slot(input, auxiliary).unwrap();
        let idle = main
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-idle", 1)),
                    &[output],
                    None,
                ),
            )
            .unwrap();
        let mut worker = NodeMain::from(main.snapshot());
        let packet_next = worker.prepare_next_frame(input, 0);
        let auxiliary_next = worker.prepare_next_frame(input, 1);
        let packet_address = core::ptr::from_ref(worker.next_frame_mut(packet_next)).addr();
        let auxiliary_address = core::ptr::from_ref(worker.next_frame_mut(auxiliary_next)).addr();
        let packet_class = worker.next_frame_mut(packet_next).frame_size_index;
        let auxiliary_class = worker.next_frame_mut(auxiliary_next).frame_size_index;
        assert_ne!(packet_class, auxiliary_class);
        assert!(
            worker.next_frames[worker.next_frame_indices[idle.slot() as usize][0]]
                .frame
                .is_none()
        );
        assert_eq!(worker.frames_in_use(), 2);
        worker.refork(main.snapshot());
        assert_eq!(worker.frames_in_use(), 0);
        assert!(worker.next_frames.iter().all(|next| next.frame.is_none()));
        // test_vlib.py::test_vlib_mw_refork_frame_leak repeats the same
        // traffic after refork and compares allocations per Worker/size class.
        // Reacquire through the same Next arcs, rather than the Pool directly.
        let packet_next = worker.prepare_next_frame(input, 0);
        let auxiliary_next = worker.prepare_next_frame(input, 1);
        assert_eq!(worker.frames_in_use(), 2);
        let packet = worker.next_frame_mut(packet_next);
        assert_eq!(core::ptr::from_ref(&*packet).addr(), packet_address);
        assert_eq!(packet.frame_size_index, packet_class);
        let auxiliary = worker.next_frame_mut(auxiliary_next);
        assert_eq!(core::ptr::from_ref(&*auxiliary).addr(), auxiliary_address);
        assert_eq!(auxiliary.frame_size_index, auxiliary_class);
    }

    #[test]
    fn refork_initializes_published_next_frames() {
        let (main, input, output) = packet_graph();
        let punt = main
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-punt", 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        main.inner.borrow_mut().nodes[punt.slot() as usize]
            .runtime_data
            .as_mut()
            .unwrap()
            .flags = 1 | (1 << 5);
        main.add_node_next_slot(input, punt).unwrap();
        let mut worker = NodeMain::from(main.snapshot());
        let index = worker.prepare_next_frame(input, 0);
        worker.next_frames[index].flags |= ENQUEUE_OWNER | IS_PENDING | (1 << 5);
        worker.next_frames[index].vectors_since_last_overflow = 123;
        worker.refork(main.snapshot());
        let ordinary = &worker.next_frames[worker.next_frame_indices[input.slot() as usize][0]];
        let retained = &worker.next_frames[worker.next_frame_indices[input.slot() as usize][1]];
        assert_eq!(ordinary.node, output);
        assert_eq!(ordinary.flags, 0);
        assert_eq!(retained.node, punt);
        assert_eq!(retained.flags, 1);
        for next in &worker.next_frames {
            assert!(next.frame.is_none());
            assert!(next.pending_index.is_none());
            assert_eq!(next.vectors_since_last_overflow, 0);
        }
        assert!(worker.enqueue_owners.iter().all(Option::is_none));
    }

    #[test]
    fn refork_preserves_pending_frame_storage() {
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
        let (main, _, output) = packet_graph();
        let mut worker = DataPlaneMain::new(crate::DataPlaneBufferConfig::default());
        worker.nodes = NodeMain::from(main.snapshot());
        for index in 1..=40 {
            let mut frame = worker.get_frame_to_node(output).unwrap();
            frame.set_vector_count(1);
            frame.vector_args_mut()[0] = index;
            worker.put_frame_to_node(output, frame).unwrap();
        }
        assert_eq!(worker.run_ready_nodes().unwrap(), 40);
        let pending = worker.nodes.pending_frames.get_mut();
        assert!(pending.is_empty());
        let address = pending.as_ptr().addr();
        let capacity = pending.capacity();
        worker.nodes.refork(main.snapshot());
        let pending = worker.nodes.pending_frames.get_mut();
        assert!(pending.is_empty());
        assert_eq!(pending.as_ptr().addr(), address);
        assert_eq!(pending.capacity(), capacity);
    }

    #[test]
    fn refork_does_not_release_packet_buffers() {
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
        let (main, input, _) = packet_graph();
        // Pool caches have one OS-thread owner: handoff uses Workers 1/2,
        // while this independent refork case owns Worker 3 for its lifetime.
        let mut worker = DataPlaneMain::new(crate::DataPlaneBufferConfig {
            thread_index: 3,
            ..Default::default()
        });
        worker.nodes = NodeMain::from(main.snapshot());
        let mut indices = [0];
        assert_eq!(worker.buffer_alloc(&mut indices), 1);
        let index = indices[0];
        let cached_free = worker.cached_free_buffers();
        let references = worker.buffer(index).ref_count();
        let next = worker.nodes.prepare_next_frame(input, 0);
        let frame = worker.nodes.next_frame_mut(next);
        frame.next_args_mut::<u32, ()>(0).0[17] = index;
        assert!(frame.is_empty());
        worker.nodes.refork(main.snapshot());
        assert_eq!(worker.buffer(index).ref_count(), references);
        assert_eq!(worker.cached_free_buffers(), cached_free);
        assert_eq!(worker.nodes.frames_in_use(), 0);
        worker.buffer_free(&indices);
    }

    fn packet_output(_: &mut DataPlaneMain, state: &mut NodeRuntime, frame: &mut Frame) -> usize {
        *state = NodeRuntime::from_words([
            state.word(0) + 1,
            state.word(1) + frame.len() as u64,
            state.word(2)
                + frame
                    .vector_args()
                    .iter()
                    .map(|index| u64::from(*index))
                    .sum::<u64>(),
            0,
        ]);
        frame.len()
    }

    fn packet_input(
        runtime: &mut DataPlaneMain,
        state: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let destination = NodeId::new(state.word(0) as u32);
        for index in 1..=40 {
            let mut output = runtime.get_frame_to_node(destination).unwrap();
            output.set_vector_count(1);
            output.vector_args_mut()[0] = index;
            runtime.put_frame_to_node(destination, output).unwrap();
        }
        frame.len()
    }

    // vlib/main.c: next_frame_change_ownership, get_next_frame_internal,
    // put_next_frame and dispatch_pending_node. Values remain graph vector
    // arguments here; these nodes perform no packet-memory lookup or release.
    #[test]
    fn next_frames_append_swap_owners_and_dispatch_overflow() {
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
        let mut runtime = DataPlaneMain::new(crate::DataPlaneBufferConfig::default());
        let output = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-output", 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        let input = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_input,
                    NodeRuntime::from_words([u64::from(output.slot()), 0, 0, 0]),
                    Some(NodeRegistration::next("packet-input", 1)),
                    &[output],
                    None,
                ),
            )
            .unwrap();
        let receive = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-receive", 1)),
                    &[output],
                    None,
                ),
            )
            .unwrap();
        let mut state = NodeRuntime::empty();
        runtime.with_current_node(input, |runtime| {
            let (vectors, _) = runtime.get_next_frame::<u32, ()>(&mut state, 0);
            assert_eq!(vectors.len(), 256);
            runtime.put_next_frame(&mut state, 0, 256);
        });
        assert!(runtime.nodes.pending_frames.borrow().is_empty());
        runtime.with_current_node(input, |runtime| {
            let (vectors, auxiliary) = runtime.get_next_frame::<u32, ()>(&mut state, 0);
            assert!(auxiliary.is_none());
            assert_eq!(vectors.len(), 256);
            vectors[..2].copy_from_slice(&[11, 13]);
            runtime.put_next_frame(&mut state, 0, 254);
        });
        let previous_owner = runtime.nodes.next_frame_indices[input.slot() as usize][0];
        assert_eq!(runtime.nodes.pending_frames.borrow().len(), 1);
        runtime.with_current_node(receive, |runtime| {
            let (vectors, _) = runtime.get_next_frame::<u32, ()>(&mut state, 0);
            assert_eq!(vectors.len(), 254);
            vectors.fill(17);
            runtime.put_next_frame(&mut state, 0, 0);
        });
        let owner = runtime.nodes.next_frame_indices[receive.slot() as usize][0];
        assert_eq!(
            runtime.nodes.next_frames[previous_owner].flags & ENQUEUE_OWNER,
            0
        );
        assert_eq!(
            runtime.nodes.pending_frames.borrow()[0].next_frame_index,
            Some(owner)
        );
        runtime.with_current_node(receive, |runtime| {
            let (vectors, _) = runtime.get_next_frame::<u32, ()>(&mut state, 0);
            assert_eq!(vectors.len(), 256);
            vectors[0] = 19;
            runtime.put_next_frame(&mut state, 0, 255);
        });
        assert_eq!(runtime.nodes.pending_frames.borrow().len(), 2);
        assert_eq!(runtime.run_ready_nodes().unwrap(), 2);
        assert!(runtime.nodes.pending_frames.borrow().is_empty());
        assert_eq!(
            runtime.nodes().node_runtime_data(output).unwrap(),
            NodeRuntime::from_words([2, 257, 11 + 13 + 254 * 17 + 19, 0])
        );
        assert_eq!(runtime.nodes().frames_in_use(), 1);
        assert!(
            runtime.nodes.next_frames[owner]
                .frame
                .as_ref()
                .unwrap()
                .is_empty()
        );

        let mut input_frame = Frame::<(), u32, ()>::new(0);
        input_frame.set_vector_count(3);
        input_frame.vector_args_mut().copy_from_slice(&[29, 31, 37]);
        runtime.with_current_node(input, |runtime| {
            runtime.enqueue_to_next(&mut input_frame, &[0u16; 3]);
        });
        assert_eq!(input_frame.vector_args(), &[29, 31, 37]);
        assert_eq!(runtime.run_ready_nodes().unwrap(), 1);

        // Growth beyond the initial 32 entries happens inside a callback.
        let capacity = runtime.nodes.pending_frames.borrow().capacity();
        let mut frame = runtime.get_frame_to_node(input).unwrap();
        frame.set_vector_count(1);
        frame.vector_args_mut()[0] = 23;
        runtime.put_frame_to_node(input, frame).unwrap();
        assert_eq!(runtime.run_ready_nodes().unwrap(), 41);
        assert!(runtime.nodes.pending_frames.borrow().capacity() > capacity);
        assert!(runtime.nodes.pending_frames.borrow().is_empty());
        assert_eq!(
            runtime.nodes().node_runtime_data(output).unwrap().word(1),
            300
        );
    }
}
