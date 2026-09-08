//! VPP `vlib_buffer_enqueue_to_next` / `enqueue_one`.

use crate::DataPlaneMain;
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
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

impl DataPlaneMain {
    /// Enqueue every u32 in `frame` to its parallel current-node-local next.
    ///
    /// Shape matches VPP `vlib_buffer_enqueue_to_next`: walk first-unhandled
    /// next groups via a used bitmap, and for each group run `enqueue_one`.
    pub fn enqueue_to_next<N: NodeNext>(&mut self, frame: &mut Frame, nexts: &[N]) {
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
                frame.vector_args(),
                &next_slots[..count],
                &mut used,
                n_left,
            );
        }
    }

    /// VPP `enqueue_one`: mask-compare, copy matches into the appendable next
    /// frame, put when full, rotate once if the group still spills.
    fn enqueue_one(
        &mut self,
        current: NodeId,
        next_index: u16,
        buffers: &[u32],
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

        let scalar_size = self
            .nodes()
            .frame_args_size(target)
            .expect("registered next layout")
            .0;
        let mut source_offset = 0;
        let mut copied = 0;
        while copied < n_extracted {
            let frame_index = self
                .nodes
                .prepare_next_frame(current, u32::from(next_index));
            let vectors_left = {
                let (vectors, _) = self
                    .nodes
                    .next_frame_mut(frame_index)
                    .next_args_mut::<u32, ()>(scalar_size);
                let mut written = 0;
                while source_offset < nexts.len() && written < vectors.len() {
                    if mask_bit(&match_bmp, source_offset) {
                        vectors[written] = buffers[source_offset];
                        written += 1;
                    }
                    source_offset += 1;
                }
                copied += written;
                vectors.len() - written
            };
            self.nodes.put_next_frame_index(frame_index, vectors_left);
        }
        n_left - n_extracted
    }
}
