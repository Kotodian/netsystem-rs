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
