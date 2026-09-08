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
                2,
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
