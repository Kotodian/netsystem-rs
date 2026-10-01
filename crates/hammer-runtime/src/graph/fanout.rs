//! VPP `vlib_buffer_enqueue_to_next` / `enqueue_one`.

use crate::{DataPlaneMain, NodeRuntime};
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_infra::mask_compare::{compress_u32, mask_compare_u16, mask_compare_u16_words};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

const MASK_WORDS: usize = mask_compare_u16_words(DEFAULT_BUFFER_FRAME_CAPACITY);

pub(crate) type EnqueueNextFn = fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame, &[u16]);

#[inline]
pub(crate) fn select_enqueue_next(cpu_pinned: bool) -> EnqueueNextFn {
    if cpu_pinned {
        #[cfg(target_arch = "x86_64")]
        {
            use crate::node::NodeVariant;
            if NodeVariant::X86_64V4.priority_on_current_cpu().is_some() {
                return enqueue_next_x86_64_v4;
            }
            if NodeVariant::X86_64V3.priority_on_current_cpu().is_some() {
                return enqueue_next_x86_64_v3;
            }
        }
    }
    enqueue_next_base
}

#[cfg(target_arch = "x86_64")]
fn enqueue_next_x86_64_v3(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
    nexts: &[u16],
) {
    // SAFETY: selected on the executing, CPU-pinned worker.
    unsafe { enqueue_next_x86_64_v3_body(runtime, node_runtime, frame, nexts) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,fma,f16c,lzcnt,movbe")]
unsafe fn enqueue_next_x86_64_v3_body(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
    nexts: &[u16],
) {
    enqueue_next_base(runtime, node_runtime, frame, nexts);
}

#[cfg(target_arch = "x86_64")]
fn enqueue_next_x86_64_v4(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
    nexts: &[u16],
) {
    // SAFETY: selected on the executing, CPU-pinned worker.
    unsafe { enqueue_next_x86_64_v4_body(runtime, node_runtime, frame, nexts) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512dq,avx512vl,avx512cd")]
unsafe fn enqueue_next_x86_64_v4_body(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
    nexts: &[u16],
) {
    enqueue_next_base(runtime, node_runtime, frame, nexts);
}

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
    /// Enqueue by next arc and scalar value. A destination without scalar
    /// arguments ignores the scalar (for example the output drop arc).
    /// VPP interface_output.c: enqueue_to_tx_node/enqueue_one_to_tx_node.
    pub fn enqueue_to_next_with_scalar<N, S>(
        &mut self,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
        nexts: &[N],
        scalars: &[S],
    ) where
        N: NodeNext,
        S: Copy + Eq + KnownLayout + FromBytes + Immutable + IntoBytes,
    {
        assert_eq!(frame.len(), nexts.len(), "next count matches Frame");
        assert_eq!(frame.len(), scalars.len(), "scalar count matches Frame");
        assert!(frame.len() <= DEFAULT_BUFFER_FRAME_CAPACITY);
        let current = self
            .current_node()
            .expect("fanout requires an executing node");
        let mut handled = [false; DEFAULT_BUFFER_FRAME_CAPACITY];
        let mut group = [0u32; DEFAULT_BUFFER_FRAME_CAPACITY];
        for first in 0..frame.len() {
            if handled[first] {
                continue;
            }
            let next_index = nexts[first].slot();
            let target = self
                .nodes()
                .node_next_slot(current, usize::from(next_index))
                .expect("registered next slot");
            let scalar_size = self
                .nodes()
                .frame_args_size(target)
                .expect("registered next layout")
                .0;
            assert!(scalar_size == 0 || usize::from(scalar_size) == core::mem::size_of::<S>());
            let mut count = 0;
            for index in first..frame.len() {
                if !handled[index]
                    && nexts[index].slot() == next_index
                    && (scalar_size == 0 || scalars[index] == scalars[first])
                {
                    group[count] = frame.vector_args()[index];
                    handled[index] = true;
                    count += 1;
                }
            }
            let mut copied = 0;
            while copied < count {
                let (written, remaining) = if scalar_size == 0 {
                    let (vectors, _) =
                        self.get_next_frame::<u32, ()>(node_runtime, u32::from(next_index));
                    let written = vectors.len().min(count - copied);
                    vectors[..written].copy_from_slice(&group[copied..copied + written]);
                    (written, vectors.len() - written)
                } else {
                    let vectors =
                        self.get_next_frame_with_scalar(u32::from(next_index), scalars[first]);
                    let written = vectors.len().min(count - copied);
                    vectors[..written].copy_from_slice(&group[copied..copied + written]);
                    (written, vectors.len() - written)
                };
                self.put_next_frame(node_runtime, u32::from(next_index), remaining);
                copied += written;
            }
        }
    }

    /// Enqueue every u32 in `frame` to its parallel current-node-local next.
    ///
    /// Shape matches VPP `vlib_buffer_enqueue_to_next`: walk first-unhandled
    /// next groups via a used bitmap, and for each group run `enqueue_one`.
    #[inline(always)]
    pub fn enqueue_to_next<N: NodeNext>(
        &mut self,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
        nexts: &[N],
    ) {
        if frame.len() != nexts.len() {
            abort_fanout("nexts length must equal frame length");
        }
        if frame.is_empty() {
            return;
        }
        let count = frame.len();
        if count > DEFAULT_BUFFER_FRAME_CAPACITY {
            abort_fanout("frame length exceeds production frame capacity");
        }

        let mut next_slots = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
        for (offset, next) in nexts.iter().enumerate() {
            next_slots[offset] = next.slot();
        }

        let enqueue = self.enqueue_next;
        enqueue(self, node_runtime, frame, &next_slots[..count]);
    }

    /// VPP `enqueue_one`: mask-compare, copy matches into the appendable next
    /// frame, put when full, rotate once if the group still spills.
    #[inline(always)]
    fn enqueue_one(
        &mut self,
        node_runtime: &mut NodeRuntime,
        current: NodeId,
        next_index: u16,
        buffers: &[u32],
        nexts: &[u16],
        used: &mut [u64; MASK_WORDS],
        n_left: usize,
        extracted: &mut [u32; DEFAULT_BUFFER_FRAME_CAPACITY],
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
        let frame_index = self
            .nodes
            .prepare_next_frame(current, u32::from(next_index));
        if let Some(vectors_left) = {
            let (vectors, _) = self
                .nodes
                .next_frame_mut(frame_index)
                .next_args_mut::<u32, ()>(scalar_size);
            (vectors.len() >= n_left).then(|| {
                assert_eq!(compress_u32(vectors, buffers, &match_bmp), n_extracted);
                vectors.len() - n_extracted
            })
        } {
            self.put_next_frame(node_runtime, u32::from(next_index), vectors_left);
            return n_left - n_extracted;
        }

        assert_eq!(compress_u32(extracted, buffers, &match_bmp), n_extracted);
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
                let written = vectors.len().min(n_extracted - copied);
                vectors[..written].copy_from_slice(&extracted[copied..copied + written]);
                copied += written;
                vectors.len() - written
            };
            self.put_next_frame(node_runtime, u32::from(next_index), vectors_left);
        }
        n_left - n_extracted
    }
}

/// VPP buffer_funcs.c:94-173: one selected ordinary architecture function.
#[inline(always)]
pub(crate) fn enqueue_next_base(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
    nexts: &[u16],
) {
    let Some(current) = runtime.current_node() else {
        abort_fanout("current graph node is required");
    };
    let mut used = [0u64; MASK_WORDS];
    let mut extracted = [0u32; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut n_left = nexts.len();
    while n_left > 0 {
        let next_index = first_unhandled(nexts, &used);
        n_left = runtime.enqueue_one(
            node_runtime,
            current,
            next_index,
            frame.vector_args(),
            nexts,
            &mut used,
            n_left,
            &mut extracted,
        );
    }
}
