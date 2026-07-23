//! VPP `vlib_buffer_enqueue_to_next` / `enqueue_one`.

use crate::DataPlaneRuntime;
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Frame, Index, Next, NodeId, NodeNext,
};
use hammer_infra::mask_compare::{mask_compare_u16, mask_compare_u16_words};

const MASK_WORDS: usize = mask_compare_u16_words(DEFAULT_BUFFER_FRAME_CAPACITY);

#[cold]
#[inline(never)]
fn abort_fanout(message: &str) -> ! {
    panic!("graph fanout invariant breached: {message}");
}

#[inline]
fn mask_bit(masks: &[u64], index: usize) -> bool {
    masks[index / 64] & (1u64 << (index % 64)) != 0
}

#[inline]
fn first_unhandled(nexts: &[u16], used: &[u64]) -> u16 {
    for (offset, &next) in nexts.iter().enumerate() {
        if !mask_bit(used, offset) {
            return next;
        }
    }
    abort_fanout("used bitmap covered every next before n_left reached zero");
}

impl DataPlaneRuntime {
    /// Enqueue every Index in `frame` to its parallel current-node-local next.
    ///
    /// Shape matches VPP `vlib_buffer_enqueue_to_next`: walk first-unhandled
    /// next groups via a used bitmap, and for each group run `enqueue_one`.
    pub fn enqueue_to_next<N: NodeNext>(&self, frame: &mut BufferFrame, nexts: &[N]) {
        if frame.len() != nexts.len() {
            abort_fanout("nexts length must equal frame length");
        }
        if frame.is_empty() {
            return;
        }
        let Some(current) = self.current_node() else {
            abort_fanout("current graph node is required");
        };
        let count = frame.len();
        if count > DEFAULT_BUFFER_FRAME_CAPACITY {
            abort_fanout("frame length exceeds production frame capacity");
        }

        let mut next_slots = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
        for (offset, next) in nexts.iter().enumerate() {
            next_slots[offset] = next.slot();
        }

        let mut used = [0u64; MASK_WORDS];
        let mut n_left = count;
        while n_left > 0 {
            let next_index = first_unhandled(&next_slots[..count], &used);
            n_left = self.enqueue_one(
                current,
                next_index,
                frame.indices(),
                &next_slots[..count],
                &mut used,
                n_left,
            );
        }
        frame.discard_prefix(count);
    }

    /// VPP `enqueue_one`: mask-compare, copy matches into the appendable next
    /// frame, put when full, rotate once if the group still spills.
    fn enqueue_one(
        &self,
        current: NodeId,
        next_index: u16,
        buffers: &[Index],
        nexts: &[u16],
        used: &mut [u64; MASK_WORDS],
        n_left: usize,
    ) -> usize {
        let target = match self
            .nodes()
            .node_next_slot(current, usize::from(next_index))
        {
            Ok(node) => node,
            Err(_) => abort_fanout("local next slot is not registered"),
        };

        let mut match_bmp = [0u64; MASK_WORDS];
        let n_extracted = mask_compare_u16(next_index, nexts, &mut match_bmp) as usize;
        for (word, bits) in match_bmp.iter().enumerate() {
            used[word] |= bits;
        }

        let mut out = self.take_appendable_next_frame(current, next_index, target);
        let mut copied = 0usize;
        for offset in 0..nexts.len() {
            if !mask_bit(&match_bmp, offset) {
                continue;
            }
            if out.remaining_capacity() == 0 {
                if self.put_next_frame(out).is_err() {
                    abort_fanout("failed to put full next frame");
                }
                out = match self.buffers().get_next_frame(target) {
                    Ok(frame) => frame,
                    Err(_) => abort_fanout("failed to acquire next frame"),
                };
            }
            if out.push_index(buffers[offset]).is_err() {
                abort_fanout("next frame rejected an index within remaining capacity");
            }
            copied += 1;
        }
        if copied != n_extracted {
            abort_fanout("extracted count mismatch");
        }

        if out.is_empty() {
            drop(out);
        } else if out.remaining_capacity() == 0 {
            if self.put_next_frame(out).is_err() {
                abort_fanout("failed to put next frame");
            }
        } else {
            // Hammer put takes ownership; keep partial frames worker-local until
            // flush (VPP leaves them in next_frames and still appendable after put).
            self.appendable_next_frames
                .borrow_mut()
                .push((current, next_index, out));
        }

        n_left - n_extracted
    }

    pub(crate) fn flush_fanout_appendable(&self) {
        let mut appendable = self.appendable_next_frames.borrow_mut();
        while let Some((_, _, frame)) = appendable.pop() {
            if frame.is_empty() {
                drop(frame);
                continue;
            }
            if self.put_next_frame(frame).is_err() {
                abort_fanout("failed to put appendable next frame");
            }
        }
    }

    fn take_appendable_next_frame(
        &self,
        current: NodeId,
        slot: u16,
        target: NodeId,
    ) -> Frame<Next> {
        let mut appendable = self.appendable_next_frames.borrow_mut();
        if let Some(position) = appendable
            .iter()
            .position(|&(node, next_slot, _)| node == current && next_slot == slot)
        {
            let (_, _, frame) = appendable.swap_remove(position);
            if frame.next() != target {
                abort_fanout("appendable next frame target mismatch");
            }
            return frame;
        }
        drop(appendable);
        match self.buffers().get_next_frame(target) {
            Ok(frame) => frame,
            Err(_) => abort_fanout("failed to acquire next frame"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use hammer_core::data_plane::{
        DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeKind, NodeNext, NodeRegistration,
    };
    use hammer_runtime::RuntimeResult;

    use super::*;
    use crate::{
        DataPlaneBufferConfig, DataPlaneRuntimeConfig, NodeDescriptor, NodeProcessFn, NodeResult,
        NodeRuntimeData,
    };

    static TEST_LOCK: Mutex<()> = Mutex::new(());
    static SINK_A: Mutex<Vec<Index>> = Mutex::new(Vec::new());
    static SINK_B: Mutex<Vec<Index>> = Mutex::new(Vec::new());

    fn clear_sinks() {
        SINK_A.lock().expect("sink a").clear();
        SINK_B.lock().expect("sink b").clear();
    }

    fn collect_a(_: &DataPlaneRuntime, _: NodeRuntimeData, frame: &mut BufferFrame) -> NodeResult {
        SINK_A
            .lock()
            .expect("sink a")
            .extend(frame.pending_indices().iter().copied());
        NodeResult::drop()
    }

    fn collect_b(_: &DataPlaneRuntime, _: NodeRuntimeData, frame: &mut BufferFrame) -> NodeResult {
        SINK_B
            .lock()
            .expect("sink b")
            .extend(frame.pending_indices().iter().copied());
        NodeResult::drop()
    }

    fn noop(_: &DataPlaneRuntime, _: NodeRuntimeData, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    fn register_sink(
        runtime: &DataPlaneRuntime,
        name: &'static str,
        process: NodeProcessFn,
    ) -> RuntimeResult<NodeId> {
        runtime.nodes().try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                process,
                NodeRuntimeData::empty(),
                NodeRegistration::next(name, 0),
                &[],
                None,
            ),
        )
    }

    fn register_plain(runtime: &DataPlaneRuntime, process: NodeProcessFn) -> RuntimeResult<NodeId> {
        runtime.nodes().try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                process,
                NodeRuntimeData::empty(),
                NodeRegistration::Plain,
                &[],
                None,
            ),
        )
    }

    fn register_owner(runtime: &DataPlaneRuntime, nexts: &[NodeId]) -> RuntimeResult<NodeId> {
        runtime.nodes().try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                noop,
                NodeRuntimeData::empty(),
                NodeRegistration::next("fanout-owner", nexts.len()),
                nexts,
                None,
            ),
        )
    }

    fn test_runtime(frame_slots: usize, buffer_slots: usize) -> DataPlaneRuntime {
        DataPlaneRuntime::new(DataPlaneRuntimeConfig {
            buffers: DataPlaneBufferConfig {
                buffer_slot_capacity: 64,
                buffer_slots,
                frame_slots,
                ..DataPlaneBufferConfig::default()
            },
        })
    }

    fn run_fanout(
        runtime: &DataPlaneRuntime,
        owner: NodeId,
        frame: &mut BufferFrame,
        nexts: &[u16],
    ) {
        runtime.set_current_node(Some(owner));
        runtime.enqueue_to_next(frame, nexts);
        runtime.flush_fanout_appendable();
        runtime.set_current_node(None);
    }

    #[test]
    fn single_next_in_order() -> RuntimeResult<()> {
        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(8, 8);
        let a = register_sink(&runtime, "a", collect_a)?;
        let b = register_sink(&runtime, "b", collect_b)?;
        let owner = register_owner(&runtime, &[a, b])?;

        let first = runtime.alloc_index_with_bytes(b"a")?;
        let second = runtime.alloc_index_with_bytes(b"b")?;
        let mut frame = runtime.buffers().get_next_frame(owner)?;
        frame.push_index(first)?;
        frame.push_index(second)?;
        run_fanout(&runtime, owner, &mut frame, &[0, 0]);
        drop(frame);

        assert_eq!(runtime.run_ready_nodes()?, 1);
        assert_eq!(SINK_A.lock().expect("a").as_slice(), &[first, second]);
        assert!(SINK_B.lock().expect("b").is_empty());
        assert_eq!(runtime.in_use_buffers(), 0);
        Ok(())
    }

    #[test]
    fn empty_is_noop() -> RuntimeResult<()> {
        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(4, 4);
        let a = register_sink(&runtime, "a", collect_a)?;
        let b = register_sink(&runtime, "b", collect_b)?;
        let owner = register_owner(&runtime, &[a, b])?;

        let mut frame = runtime.buffers().get_next_frame(owner)?;
        run_fanout(&runtime, owner, &mut frame, &[]);
        drop(frame);

        assert_eq!(runtime.run_ready_nodes()?, 0);
        Ok(())
    }

    #[test]
    fn alternating_stable_per_next() -> RuntimeResult<()> {
        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(8, 8);
        let a = register_sink(&runtime, "a", collect_a)?;
        let b = register_sink(&runtime, "b", collect_b)?;
        let owner = register_owner(&runtime, &[a, b])?;

        let indices = [
            runtime.alloc_index_with_bytes(b"0")?,
            runtime.alloc_index_with_bytes(b"1")?,
            runtime.alloc_index_with_bytes(b"2")?,
            runtime.alloc_index_with_bytes(b"3")?,
        ];
        let mut frame = runtime.buffers().get_next_frame(owner)?;
        frame.push_indices(indices)?;
        run_fanout(&runtime, owner, &mut frame, &[0, 1, 0, 1]);
        drop(frame);

        assert_eq!(runtime.run_ready_nodes()?, 2);
        assert_eq!(
            SINK_A.lock().expect("a").as_slice(),
            &[indices[0], indices[2]]
        );
        assert_eq!(
            SINK_B.lock().expect("b").as_slice(),
            &[indices[1], indices[3]]
        );
        Ok(())
    }

    #[test]
    fn append_then_rotate() -> RuntimeResult<()> {
        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(8, DEFAULT_BUFFER_FRAME_CAPACITY + 8);
        let a = register_sink(&runtime, "a", collect_a)?;
        let b = register_sink(&runtime, "b", collect_b)?;
        let owner = register_owner(&runtime, &[a, b])?;

        let mut owned = Vec::new();
        let seed = runtime.alloc_index_with_bytes(b"seed")?;
        owned.push(seed);
        let mut first = runtime.buffers().get_next_frame(owner)?;
        first.push_index(seed)?;
        runtime.set_current_node(Some(owner));
        runtime.enqueue_to_next(&mut first, &[0u16]);
        drop(first);

        let mut second = runtime.buffers().get_next_frame(owner)?;
        for offset in 0..DEFAULT_BUFFER_FRAME_CAPACITY {
            let index = runtime.alloc_index_with_bytes(&[(offset % 256) as u8])?;
            second.push_index(index)?;
            owned.push(index);
        }
        let nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
        runtime.enqueue_to_next(&mut second, &nexts);
        runtime.flush_fanout_appendable();
        runtime.set_current_node(None);
        drop(second);

        assert_eq!(runtime.run_ready_nodes()?, 2);
        assert_eq!(SINK_A.lock().expect("a").as_slice(), owned.as_slice());
        assert!(SINK_B.lock().expect("b").is_empty());
        assert_eq!(runtime.in_use_buffers(), 0);
        Ok(())
    }

    #[test]
    fn sparse_local_slots() -> RuntimeResult<()> {
        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(8, 8);
        let a = register_sink(&runtime, "a", collect_a)?;
        let b = register_sink(&runtime, "b", collect_b)?;
        let pads = [
            register_sink(&runtime, "p0", collect_b)?,
            register_sink(&runtime, "p1", collect_b)?,
            register_sink(&runtime, "p2", collect_b)?,
            register_sink(&runtime, "p3", collect_b)?,
        ];
        let owner = register_owner(&runtime, &[a, pads[0], pads[1], pads[2], pads[3], b])?;

        let first = runtime.alloc_index_with_bytes(b"s0")?;
        let second = runtime.alloc_index_with_bytes(b"s5")?;
        let mut frame = runtime.buffers().get_next_frame(owner)?;
        frame.push_index(first)?;
        frame.push_index(second)?;
        run_fanout(&runtime, owner, &mut frame, &[0, 5]);
        drop(frame);

        assert_eq!(runtime.run_ready_nodes()?, 2);
        assert_eq!(SINK_A.lock().expect("a").as_slice(), &[first]);
        assert_eq!(SINK_B.lock().expect("b").as_slice(), &[second]);
        Ok(())
    }

    #[test]
    fn typed_node_next() -> RuntimeResult<()> {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Arc {
            B = 1,
        }
        impl NodeNext for Arc {
            fn slot(self) -> u16 {
                self as u16
            }
        }

        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(8, 8);
        let a = register_sink(&runtime, "a", collect_a)?;
        let b = register_sink(&runtime, "b", collect_b)?;
        let owner = register_owner(&runtime, &[a, b])?;

        let index = runtime.alloc_index_with_bytes(b"t")?;
        let mut frame = runtime.buffers().get_next_frame(owner)?;
        frame.push_index(index)?;
        runtime.set_current_node(Some(owner));
        runtime.enqueue_to_next(&mut frame, &[Arc::B]);
        runtime.flush_fanout_appendable();
        runtime.set_current_node(None);
        drop(frame);

        assert_eq!(runtime.run_ready_nodes()?, 1);
        assert!(SINK_A.lock().expect("a").is_empty());
        assert_eq!(SINK_B.lock().expect("b").as_slice(), &[index]);
        Ok(())
    }

    #[test]
    fn drop_is_ordinary_local_next() -> RuntimeResult<()> {
        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(8, 8);
        let drop_node = register_sink(&runtime, "drop", noop)?;
        let keep = register_sink(&runtime, "keep", collect_a)?;
        let owner = register_owner(&runtime, &[drop_node, keep])?;

        let dropped = runtime.alloc_index_with_bytes(b"d")?;
        let kept = runtime.alloc_index_with_bytes(b"k")?;
        let mut frame = runtime.buffers().get_next_frame(owner)?;
        frame.push_index(dropped)?;
        frame.push_index(kept)?;
        run_fanout(&runtime, owner, &mut frame, &[0, 1]);
        drop(frame);

        assert_eq!(runtime.run_ready_nodes()?, 2);
        assert_eq!(SINK_A.lock().expect("a").as_slice(), &[kept]);
        assert_eq!(runtime.in_use_buffers(), 0);
        Ok(())
    }

    #[test]
    fn full_frame_single_next() -> RuntimeResult<()> {
        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(8, DEFAULT_BUFFER_FRAME_CAPACITY + 4);
        let a = register_sink(&runtime, "a", collect_a)?;
        let b = register_sink(&runtime, "b", collect_b)?;
        let owner = register_owner(&runtime, &[a, b])?;

        let mut owned = Vec::with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY);
        let mut frame = runtime.buffers().get_next_frame(owner)?;
        for offset in 0..DEFAULT_BUFFER_FRAME_CAPACITY {
            let index = runtime.alloc_index_with_bytes(&[(offset % 256) as u8])?;
            frame.push_index(index)?;
            owned.push(index);
        }
        let nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
        run_fanout(&runtime, owner, &mut frame, &nexts);
        drop(frame);

        assert_eq!(runtime.run_ready_nodes()?, 1);
        assert_eq!(SINK_A.lock().expect("a").as_slice(), owned.as_slice());
        assert!(SINK_B.lock().expect("b").is_empty());
        assert_eq!(runtime.in_use_buffers(), 0);
        Ok(())
    }

    #[test]
    fn local_slot_above_old_sixteen_limit() -> RuntimeResult<()> {
        let _guard = TEST_LOCK.lock().expect("test lock");
        clear_sinks();
        let runtime = test_runtime(8, 32);
        let a = register_sink(&runtime, "a", collect_a)?;
        let b = register_sink(&runtime, "b", collect_b)?;
        let owner = register_owner(&runtime, &[a, b])?;

        let mut slot = 1u16;
        for _ in 2..20 {
            let pad = register_plain(&runtime, noop)?;
            slot = runtime.nodes().add_node_next_slot(owner, pad)?;
        }
        let high = register_plain(&runtime, collect_b)?;
        let high_slot = runtime.nodes().add_node_next_slot(owner, high)?;
        assert!(
            high_slot > 16,
            "expected slot above old limit, got {high_slot}"
        );
        assert_eq!(high_slot, slot + 1);

        clear_sinks();
        let index = runtime.alloc_index_with_bytes(b"hi")?;
        let mut frame = runtime.buffers().get_next_frame(owner)?;
        frame.push_index(index)?;
        run_fanout(&runtime, owner, &mut frame, &[high_slot]);
        drop(frame);

        assert_eq!(runtime.run_ready_nodes()?, 1);
        assert!(SINK_A.lock().expect("a").is_empty());
        assert_eq!(SINK_B.lock().expect("b").as_slice(), &[index]);
        Ok(())
    }
}
