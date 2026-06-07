use hammer_core::error::{CoreError, CoreResult};

use crate::buffer::{
    BufferBatchMut, BufferFrame, BufferIndex, DataPlaneRuntime, FrameIndex, PooledBufferFrame,
};
use crate::instruction_set::FrameBatchWidth;

use super::{MAX_NODE_NEXT_FRAMES, NodeId, NodeResult};

const NODE_VECTOR_DISPATCH_UNSET: NodeId = NodeId::new(u32::MAX);

pub trait NodeNext: Copy + Eq {
    const COUNT: usize;

    fn slot(self) -> usize;
}

pub trait NodeNextStorage<K> {
    fn next(&self, key: K) -> NodeId;
}

impl<K, const N: usize> NodeNextStorage<K> for [NodeId; N]
where
    K: NodeNext,
{
    #[inline(always)]
    fn next(&self, key: K) -> NodeId {
        self[key.slot()]
    }
}

impl NodeNextStorage<()> for NodeId {
    #[inline(always)]
    fn next(&self, _key: ()) -> NodeId {
        *self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NodeNextFrames {
    nodes: [Option<NodeId>; MAX_NODE_NEXT_FRAMES],
    frames: [Option<FrameIndex>; MAX_NODE_NEXT_FRAMES],
    len: usize,
}

#[derive(Debug)]
struct NodeNextOwnedFrames {
    nodes: [Option<NodeId>; MAX_NODE_NEXT_FRAMES],
    frames: [Option<PooledBufferFrame>; MAX_NODE_NEXT_FRAMES],
    len: usize,
}

#[derive(Debug)]
pub struct NodeNextEnqueue {
    speculative_node: NodeId,
    split: NodeNextOwnedFrames,
}

#[derive(Debug)]
pub struct NodeNextVectorEnqueue {
    cached_next: NodeId,
    frames: NodeNextOwnedFrames,
}

#[derive(Debug)]
pub struct NodeVectorDispatch {
    cached_next: Option<NodeId>,
    frames: NodeNextOwnedFrames,
}

impl Default for NodeNextFrames {
    #[inline]
    fn default() -> Self {
        Self {
            nodes: [None; MAX_NODE_NEXT_FRAMES],
            frames: [None; MAX_NODE_NEXT_FRAMES],
            len: 0,
        }
    }
}

impl Default for NodeNextOwnedFrames {
    #[inline]
    fn default() -> Self {
        Self {
            nodes: [None; MAX_NODE_NEXT_FRAMES],
            frames: std::array::from_fn(|_| None),
            len: 0,
        }
    }
}

impl NodeNextEnqueue {
    #[inline]
    pub fn new(speculative_node: NodeId) -> Self {
        Self {
            speculative_node,
            split: NodeNextOwnedFrames::default(),
        }
    }

    #[inline(always)]
    pub fn validate_frame(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mut next_for_index: impl FnMut(BufferIndex) -> CoreResult<NodeId>,
    ) -> CoreResult<NodeResult> {
        let speculative_node = self.speculative_node;
        let width = runtime.preferred_frame_batch_width();
        self.validate_frame_with_width(runtime, frame, width, |index| {
            let node = next_for_index(index)?;
            Ok(node)
        })?;
        self.finish(runtime, frame, speculative_node)
    }

    #[inline(always)]
    pub fn validate_frame_with_first_next(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        first_index: BufferIndex,
        first_next: NodeId,
        mut next_for_index: impl FnMut(BufferIndex) -> CoreResult<NodeId>,
    ) -> CoreResult<NodeResult> {
        let speculative_node = self.speculative_node;
        let width = runtime.preferred_frame_batch_width();
        let mut first_seen = false;
        self.validate_frame_with_width(runtime, frame, width, |index| {
            if !first_seen && index == first_index {
                first_seen = true;
                return Ok(first_next);
            }
            next_for_index(index)
        })?;
        self.finish(runtime, frame, speculative_node)
    }

    #[inline(always)]
    pub fn validate_frame_with_first_next_and_prefetch(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        first_index: BufferIndex,
        first_next: NodeId,
        mut prefetch_index: impl FnMut(BufferIndex),
        mut next_for_index: impl FnMut(BufferIndex) -> CoreResult<NodeId>,
    ) -> CoreResult<NodeResult> {
        let speculative_node = self.speculative_node;
        let width = runtime.preferred_frame_batch_width();
        let mut first_seen = false;
        self.validate_frame_with_width_and_prefetch(
            runtime,
            frame,
            width,
            |index| prefetch_index(index),
            |index| {
                if !first_seen && index == first_index {
                    first_seen = true;
                    return Ok(first_next);
                }
                next_for_index(index)
            },
        )?;
        self.finish(runtime, frame, speculative_node)
    }

    #[inline(always)]
    pub fn validate_frame_with_first_next_and_buffer_batch_prefetch(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        first_index: BufferIndex,
        first_next: NodeId,
        mut prefetch_index: impl FnMut(&mut BufferBatchMut<'_>, BufferIndex),
        mut next_for_index: impl FnMut(&mut BufferBatchMut<'_>, BufferIndex) -> CoreResult<NodeId>,
    ) -> CoreResult<NodeResult> {
        let speculative_node = self.speculative_node;
        let width = runtime.preferred_frame_batch_width();
        let mut first_seen = false;
        self.validate_frame_with_width_and_buffer_batch_prefetch(
            runtime,
            frame,
            width,
            |batch, index| prefetch_index(batch, index),
            |batch, index| {
                if !first_seen && index == first_index {
                    first_seen = true;
                    return Ok(first_next);
                }
                next_for_index(batch, index)
            },
        )?;
        self.finish(runtime, frame, speculative_node)
    }

    #[inline(always)]
    pub fn validate_frame_with_width(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        width: FrameBatchWidth,
        mut next_for_index: impl FnMut(BufferIndex) -> CoreResult<NodeId>,
    ) -> CoreResult<()> {
        let speculative_node = self.speculative_node;
        let result = frame.retain_indices_batched(width, |index| {
            let node = next_for_index(index)?;
            if node == speculative_node {
                Ok(true)
            } else {
                self.split.enqueue(runtime, node, index)?;
                Ok(false)
            }
        });
        if crate::unlikely(result.is_err()) {
            self.split.free(runtime);
        }
        result
    }

    #[inline(always)]
    pub fn validate_frame_with_nexts(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        nexts: &[NodeId],
    ) -> CoreResult<NodeResult> {
        let speculative_node = self.speculative_node;
        let width = runtime.preferred_frame_batch_width();
        self.validate_frame_with_nexts_and_width(runtime, frame, width, nexts)?;
        self.finish(runtime, frame, speculative_node)
    }

    #[inline(always)]
    pub fn validate_frame_with_nexts_and_width(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        width: FrameBatchWidth,
        nexts: &[NodeId],
    ) -> CoreResult<()> {
        let speculative_node = self.speculative_node;
        if nexts.len() != frame.pending_len() {
            return Err(CoreError::internal("node next decision count mismatch"));
        }
        let mut offset = 0usize;
        let result = frame.retain_indices_batched(width, |index| {
            let node = nexts[offset];
            offset += 1;
            if node == speculative_node {
                Ok(true)
            } else {
                self.split.enqueue(runtime, node, index)?;
                Ok(false)
            }
        });
        if crate::unlikely(result.is_err()) {
            self.split.free(runtime);
        }
        result
    }

    #[inline(always)]
    pub fn validate_frame_with_width_and_prefetch(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        width: FrameBatchWidth,
        mut prefetch_index: impl FnMut(BufferIndex),
        mut next_for_index: impl FnMut(BufferIndex) -> CoreResult<NodeId>,
    ) -> CoreResult<()> {
        let speculative_node = self.speculative_node;
        let result = frame.retain_indices_batched_with_prefetch(
            width,
            |index| prefetch_index(index),
            |index| {
                let node = next_for_index(index)?;
                if node == speculative_node {
                    Ok(true)
                } else {
                    self.split.enqueue(runtime, node, index)?;
                    Ok(false)
                }
            },
        );
        if crate::unlikely(result.is_err()) {
            self.split.free(runtime);
        }
        result
    }

    #[inline(always)]
    pub fn validate_frame_with_width_and_buffer_batch_prefetch(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        width: FrameBatchWidth,
        mut prefetch_index: impl FnMut(&mut BufferBatchMut<'_>, BufferIndex),
        mut next_for_index: impl FnMut(&mut BufferBatchMut<'_>, BufferIndex) -> CoreResult<NodeId>,
    ) -> CoreResult<()> {
        let speculative_node = self.speculative_node;
        let mut batch = runtime.buffer_batch_mut();
        let result = frame.retain_indices_batched_with_prefetch_state_lazy(
            width,
            &mut batch,
            |batch, index| prefetch_index(batch, index),
            |batch, index| {
                let node = next_for_index(batch, index)?;
                if node == speculative_node {
                    Ok(true)
                } else {
                    self.split.enqueue(runtime, node, index)?;
                    Ok(false)
                }
            },
        );
        drop(batch);
        if crate::unlikely(result.is_err()) {
            self.split.free(runtime);
        }
        result
    }

    #[inline(always)]
    pub fn validate_frame_with_buffer_batch_chunks(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mut prefetch_indices: impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
        mut nexts_for_indices: impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [NodeId; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<NodeResult> {
        let speculative_node = self.speculative_node;
        let width = runtime.preferred_frame_batch_width();
        let mut batch = runtime.buffer_batch_mut();
        let result = frame.retain_indices_batched_with_prefetch_state_lazy_chunks(
            width,
            &mut batch,
            |batch, indices| prefetch_indices(batch, indices),
            |batch, indices, keep| {
                let mut nexts = [speculative_node; 4];
                nexts_for_indices(batch, indices, &mut nexts)?;
                for offset in 0..indices.len() {
                    let node = nexts[offset];
                    if node != speculative_node {
                        keep[offset] = false;
                        self.split.enqueue(runtime, node, indices[offset])?;
                    }
                }
                Ok(())
            },
        );
        drop(batch);
        if crate::unlikely(result.is_err()) {
            self.split.free(runtime);
        }
        result?;
        self.finish(runtime, frame, speculative_node)
    }

    #[inline(always)]
    fn finish(
        self,
        runtime: &DataPlaneRuntime,
        frame: &BufferFrame,
        speculative_node: NodeId,
    ) -> CoreResult<NodeResult> {
        self.split.schedule(runtime)?;
        if frame.has_pending() {
            Ok(NodeResult::next_current(speculative_node))
        } else {
            Ok(NodeResult::drop())
        }
    }
}

impl NodeNextVectorEnqueue {
    #[inline]
    pub fn new(cached_next: NodeId) -> Self {
        Self {
            cached_next,
            frames: NodeNextOwnedFrames::default(),
        }
    }

    #[inline(always)]
    pub fn enqueue_frame_with_buffer_batch_chunks(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mut prefetch_indices: impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
        mut nexts_for_indices: impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [NodeId; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<(NodeResult, NodeId)> {
        let mut current_next = self.cached_next;
        let width = runtime.preferred_frame_batch_width();
        let mut batch = runtime.buffer_batch_mut();
        let result = match width {
            FrameBatchWidth::Quad => self.enqueue_frame_quad_chunks(
                runtime,
                frame,
                &mut batch,
                &mut current_next,
                &mut prefetch_indices,
                &mut nexts_for_indices,
            ),
            FrameBatchWidth::Pair => self.enqueue_frame_pair_chunks(
                runtime,
                frame,
                &mut batch,
                &mut current_next,
                &mut prefetch_indices,
                &mut nexts_for_indices,
            ),
        };
        drop(batch);

        if crate::unlikely(result.is_err()) {
            self.frames.free(runtime);
        }
        result?;

        frame.clear();
        self.frames.schedule(runtime)?;
        Ok((NodeResult::drop(), current_next))
    }

    #[inline(always)]
    fn enqueue_frame_quad_chunks(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &BufferFrame,
        batch: &mut BufferBatchMut<'_>,
        current_next: &mut NodeId,
        prefetch_indices: &mut impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
        nexts_for_indices: &mut impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [NodeId; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read + 4 <= len {
            Self::prefetch_range(batch, indices, read + 4, 4, prefetch_indices);
            let chunk = [
                indices[read],
                indices[read + 1],
                indices[read + 2],
                indices[read + 3],
            ];
            self.enqueue_chunk(runtime, &chunk, batch, current_next, nexts_for_indices)?;
            read += 4;
        }
        if read + 2 <= len {
            Self::prefetch_range(batch, indices, read + 2, 2, prefetch_indices);
            let chunk = [indices[read], indices[read + 1]];
            self.enqueue_chunk(runtime, &chunk, batch, current_next, nexts_for_indices)?;
            read += 2;
        }
        while read < len {
            let chunk = [indices[read]];
            self.enqueue_chunk(runtime, &chunk, batch, current_next, nexts_for_indices)?;
            read += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn enqueue_frame_pair_chunks(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &BufferFrame,
        batch: &mut BufferBatchMut<'_>,
        current_next: &mut NodeId,
        prefetch_indices: &mut impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
        nexts_for_indices: &mut impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [NodeId; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read + 2 <= len {
            Self::prefetch_range(batch, indices, read + 2, 2, prefetch_indices);
            let chunk = [indices[read], indices[read + 1]];
            self.enqueue_chunk(runtime, &chunk, batch, current_next, nexts_for_indices)?;
            read += 2;
        }
        if read < len {
            let chunk = [indices[read]];
            self.enqueue_chunk(runtime, &chunk, batch, current_next, nexts_for_indices)?;
        }
        Ok(())
    }

    #[inline(always)]
    fn enqueue_chunk<const N: usize>(
        &mut self,
        runtime: &DataPlaneRuntime,
        indices: &[BufferIndex; N],
        batch: &mut BufferBatchMut<'_>,
        current_next: &mut NodeId,
        nexts_for_indices: &mut impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [NodeId; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let mut nexts = [*current_next; 4];
        nexts_for_indices(batch, indices, &mut nexts)?;

        let first = nexts[0];
        let all_same = (1..N).all(|offset| nexts[offset] == first);
        if all_same {
            self.frames
                .enqueue_indices(runtime, first, indices.iter().copied())?;
            *current_next = first;
            return Ok(());
        }

        for offset in 0..N {
            self.frames
                .enqueue(runtime, nexts[offset], indices[offset])?;
        }
        if N == 1 || nexts[N - 2] == nexts[N - 1] {
            *current_next = nexts[N - 1];
        }
        Ok(())
    }

    #[inline(always)]
    fn prefetch_range(
        batch: &mut BufferBatchMut<'_>,
        indices: &[BufferIndex],
        offset: usize,
        width: usize,
        prefetch_indices: &mut impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
    ) {
        if crate::unlikely(offset >= indices.len()) {
            return;
        }
        let end = (offset + width).min(indices.len());
        prefetch_indices(batch, &indices[offset..end]);
    }
}

impl NodeVectorDispatch {
    #[inline]
    pub fn new(cached_next: Option<NodeId>) -> Self {
        Self {
            cached_next,
            frames: NodeNextOwnedFrames::default(),
        }
    }

    #[inline(always)]
    pub fn route_frame(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mut prefetch_indices: impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
        mut route_chunk: impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [Option<NodeId>; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<(NodeResult, Option<NodeId>)> {
        // `route_chunk` may inspect or mutate packet contents/metadata while filling decisions,
        // but ownership stays with this dispatcher until the callback returns. On error we only
        // commit the written prefix and roll back any staged next frames for the remaining tail.
        let mut cached_next = self.cached_next;
        let mut processed = 0usize;
        let width = runtime.preferred_frame_batch_width();
        let result = match width {
            FrameBatchWidth::Quad => self.route_frame_quad_chunks(
                runtime,
                frame,
                &mut cached_next,
                &mut processed,
                &mut prefetch_indices,
                &mut route_chunk,
            ),
            FrameBatchWidth::Pair => self.route_frame_pair_chunks(
                runtime,
                frame,
                &mut cached_next,
                &mut processed,
                &mut prefetch_indices,
                &mut route_chunk,
            ),
        };

        if crate::unlikely(result.is_err()) {
            frame.discard_prefix(processed);
            self.frames.free(runtime);
        }
        result?;

        frame.discard_prefix(processed);
        self.frames.schedule(runtime)?;
        Ok((NodeResult::drop(), cached_next))
    }

    #[inline(always)]
    pub fn route_frame_prefetch(
        self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        route_chunk: impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [Option<NodeId>; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<(NodeResult, Option<NodeId>)> {
        self.route_frame(runtime, frame, default_prefetch_indices, route_chunk)
    }

    #[inline(always)]
    pub fn route_frame_map(
        self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mut route_index: impl FnMut(&mut BufferBatchMut<'_>, BufferIndex) -> CoreResult<Option<NodeId>>,
    ) -> CoreResult<(NodeResult, Option<NodeId>)> {
        self.route_frame_prefetch(runtime, frame, |batch, indices, nexts| {
            for (offset, index) in indices.iter().copied().enumerate() {
                nexts[offset] = route_index(batch, index)?;
            }
            Ok(())
        })
    }

    #[inline(always)]
    pub fn route_frame_index(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mut route_index: impl FnMut(BufferIndex) -> CoreResult<Option<NodeId>>,
    ) -> CoreResult<(NodeResult, Option<NodeId>)> {
        let mut cached_next = self.cached_next;
        let mut processed = 0usize;
        let width = runtime.preferred_frame_batch_width();
        let result = match width {
            FrameBatchWidth::Quad => self.route_frame_index_quad_chunks(
                runtime,
                frame,
                &mut cached_next,
                &mut processed,
                &mut route_index,
            ),
            FrameBatchWidth::Pair => self.route_frame_index_pair_chunks(
                runtime,
                frame,
                &mut cached_next,
                &mut processed,
                &mut route_index,
            ),
        };

        if crate::unlikely(result.is_err()) {
            frame.discard_prefix(processed);
            self.frames.free(runtime);
        }
        result?;

        frame.discard_prefix(processed);
        self.frames.schedule(runtime)?;
        Ok((NodeResult::drop(), cached_next))
    }

    #[inline(always)]
    fn route_frame_quad_chunks(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &BufferFrame,
        cached_next: &mut Option<NodeId>,
        processed: &mut usize,
        prefetch_indices: &mut impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
        route_chunk: &mut impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [Option<NodeId>; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read + 4 <= len {
            Self::prefetch_range(runtime, indices, read + 4, 4, prefetch_indices);
            let chunk = [
                indices[read],
                indices[read + 1],
                indices[read + 2],
                indices[read + 3],
            ];
            self.route_indices(runtime, &chunk, cached_next, processed, route_chunk)?;
            read += 4;
        }
        if read + 2 <= len {
            Self::prefetch_range(runtime, indices, read + 2, 2, prefetch_indices);
            let chunk = [indices[read], indices[read + 1]];
            self.route_indices(runtime, &chunk, cached_next, processed, route_chunk)?;
            read += 2;
        }
        if read < len {
            let chunk = [indices[read]];
            self.route_indices(runtime, &chunk, cached_next, processed, route_chunk)?;
        }
        Ok(())
    }

    #[inline(always)]
    fn route_frame_index_quad_chunks(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &BufferFrame,
        cached_next: &mut Option<NodeId>,
        processed: &mut usize,
        route_index: &mut impl FnMut(BufferIndex) -> CoreResult<Option<NodeId>>,
    ) -> CoreResult<()> {
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read + 4 <= len {
            Self::prefetch_range(runtime, indices, read + 4, 4, &mut default_prefetch_indices);
            let chunk = [
                indices[read],
                indices[read + 1],
                indices[read + 2],
                indices[read + 3],
            ];
            self.route_index_chunk(runtime, &chunk, cached_next, processed, route_index)?;
            read += 4;
        }
        if read + 2 <= len {
            Self::prefetch_range(runtime, indices, read + 2, 2, &mut default_prefetch_indices);
            let chunk = [indices[read], indices[read + 1]];
            self.route_index_chunk(runtime, &chunk, cached_next, processed, route_index)?;
            read += 2;
        }
        if read < len {
            let chunk = [indices[read]];
            self.route_index_chunk(runtime, &chunk, cached_next, processed, route_index)?;
        }
        Ok(())
    }

    #[inline(always)]
    fn route_frame_index_pair_chunks(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &BufferFrame,
        cached_next: &mut Option<NodeId>,
        processed: &mut usize,
        route_index: &mut impl FnMut(BufferIndex) -> CoreResult<Option<NodeId>>,
    ) -> CoreResult<()> {
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read + 2 <= len {
            Self::prefetch_range(runtime, indices, read + 2, 2, &mut default_prefetch_indices);
            let chunk = [indices[read], indices[read + 1]];
            self.route_index_chunk(runtime, &chunk, cached_next, processed, route_index)?;
            read += 2;
        }
        if read < len {
            let chunk = [indices[read]];
            self.route_index_chunk(runtime, &chunk, cached_next, processed, route_index)?;
        }
        Ok(())
    }

    #[inline(always)]
    fn route_frame_pair_chunks(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &BufferFrame,
        cached_next: &mut Option<NodeId>,
        processed: &mut usize,
        prefetch_indices: &mut impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
        route_chunk: &mut impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [Option<NodeId>; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read + 2 <= len {
            Self::prefetch_range(runtime, indices, read + 2, 2, prefetch_indices);
            let chunk = [indices[read], indices[read + 1]];
            self.route_indices(runtime, &chunk, cached_next, processed, route_chunk)?;
            read += 2;
        }
        if read < len {
            let chunk = [indices[read]];
            self.route_indices(runtime, &chunk, cached_next, processed, route_chunk)?;
        }
        Ok(())
    }

    #[inline(always)]
    fn route_indices<const N: usize>(
        &mut self,
        runtime: &DataPlaneRuntime,
        indices: &[BufferIndex; N],
        cached_next: &mut Option<NodeId>,
        processed: &mut usize,
        route_chunk: &mut impl FnMut(
            &mut BufferBatchMut<'_>,
            &[BufferIndex],
            &mut [Option<NodeId>; 4],
        ) -> CoreResult<()>,
    ) -> CoreResult<()> {
        let mut nexts = [Some(NODE_VECTOR_DISPATCH_UNSET); 4];
        let mut batch = runtime.buffer_batch_mut();
        let route = route_chunk(&mut batch, indices, &mut nexts);
        drop(batch);
        if let Err(err) = route {
            let committed = Self::committed_prefix_len(&nexts, N);
            self.flush_next_runs(
                runtime,
                &indices[..committed],
                &nexts[..committed],
                cached_next,
                processed,
            )?;
            return Err(err);
        }
        Self::validate_active_nexts(&nexts, N)?;

        self.flush_next_runs(runtime, indices, &nexts[..N], cached_next, processed)
    }

    #[inline(always)]
    fn route_index_chunk<const N: usize>(
        &mut self,
        runtime: &DataPlaneRuntime,
        indices: &[BufferIndex; N],
        cached_next: &mut Option<NodeId>,
        processed: &mut usize,
        route_index: &mut impl FnMut(BufferIndex) -> CoreResult<Option<NodeId>>,
    ) -> CoreResult<()> {
        let mut nexts = [None; 4];
        for offset in 0..N {
            match route_index(indices[offset]) {
                Ok(next) => nexts[offset] = next,
                Err(err) => {
                    self.flush_next_runs(
                        runtime,
                        &indices[..offset],
                        &nexts[..offset],
                        cached_next,
                        processed,
                    )?;
                    return Err(err);
                }
            }
        }
        self.flush_next_runs(runtime, indices, &nexts[..N], cached_next, processed)
    }

    #[inline(always)]
    fn flush_next_runs(
        &mut self,
        runtime: &DataPlaneRuntime,
        indices: &[BufferIndex],
        nexts: &[Option<NodeId>],
        cached_next: &mut Option<NodeId>,
        processed: &mut usize,
    ) -> CoreResult<()> {
        if indices.is_empty() {
            return Ok(());
        }
        let mut run_node = nexts[0];
        let mut run_start = 0usize;
        for offset in 1..=indices.len() {
            let next = if offset < indices.len() {
                nexts[offset]
            } else {
                None
            };
            if offset == indices.len() || next != run_node {
                self.flush_run(runtime, &indices[run_start..offset], run_node, cached_next)?;
                *processed += offset - run_start;
                run_start = offset;
                run_node = next;
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn flush_run(
        &mut self,
        runtime: &DataPlaneRuntime,
        indices: &[BufferIndex],
        node: Option<NodeId>,
        cached_next: &mut Option<NodeId>,
    ) -> CoreResult<()> {
        if crate::unlikely(node.is_none()) {
            return Ok(());
        }
        let node = node.expect("checked node");
        self.frames
            .enqueue_indices(runtime, node, indices.iter().copied())?;
        *cached_next = Some(node);
        Ok(())
    }

    #[inline(always)]
    fn prefetch_range(
        runtime: &DataPlaneRuntime,
        indices: &[BufferIndex],
        offset: usize,
        width: usize,
        prefetch_indices: &mut impl FnMut(&mut BufferBatchMut<'_>, &[BufferIndex]),
    ) {
        if crate::unlikely(offset >= indices.len()) {
            return;
        }
        let end = (offset + width).min(indices.len());
        let mut batch = runtime.buffer_batch_mut();
        prefetch_indices(&mut batch, &indices[offset..end]);
    }

    #[inline(always)]
    fn validate_active_nexts(nexts: &[Option<NodeId>; 4], len: usize) -> CoreResult<()> {
        for next in nexts.iter().take(len) {
            if crate::unlikely(*next == Some(NODE_VECTOR_DISPATCH_UNSET)) {
                return Err(CoreError::internal("node route decision is missing"));
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn committed_prefix_len(nexts: &[Option<NodeId>; 4], len: usize) -> usize {
        nexts[..len]
            .iter()
            .position(|next| *next == Some(NODE_VECTOR_DISPATCH_UNSET))
            .unwrap_or(len)
    }
}

#[inline(always)]
pub fn default_prefetch_indices(batch: &mut BufferBatchMut<'_>, indices: &[BufferIndex]) {
    for index in indices {
        batch.prefetch_read(*index);
    }
}

impl NodeNextFrames {
    #[inline]
    pub fn enqueue(
        &mut self,
        runtime: &DataPlaneRuntime,
        node: NodeId,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let (frame_index, offset, created) = self.frame_for_enqueue(runtime, node)?;
        let result = runtime.get_frame_mut(frame_index)?.push_index(index);
        if crate::unlikely(result.is_err()) && created {
            self.free_from(runtime, offset);
        }
        result
    }

    #[inline]
    pub fn enqueue_indices(
        &mut self,
        runtime: &DataPlaneRuntime,
        node: NodeId,
        indices: impl IntoIterator<Item = BufferIndex>,
    ) -> CoreResult<()> {
        let (frame_index, offset, created) = self.frame_for_enqueue(runtime, node)?;
        let result = runtime.get_frame_mut(frame_index)?.push_indices(indices);
        if crate::unlikely(result.is_err()) && created {
            self.free_from(runtime, offset);
        }
        result
    }

    #[inline]
    pub fn enqueue_optional(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        node: Option<NodeId>,
    ) -> CoreResult<()> {
        let Some(node) = node else {
            runtime.free_index(index);
            return Ok(());
        };
        self.enqueue(runtime, node, index)
    }

    #[inline]
    pub fn schedule(mut self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        for offset in 0..self.len {
            let node = match self.node(offset) {
                Ok(node) => node,
                Err(err) => {
                    self.free_from(runtime, offset);
                    return Err(err);
                }
            };
            let frame_index = match self.take_frame(offset) {
                Ok(frame) => frame,
                Err(err) => {
                    self.free_from(runtime, offset + 1);
                    return Err(err);
                }
            };
            match runtime.schedule_frame(node, frame_index) {
                Ok(true) => {}
                Ok(false) => {
                    runtime.free_frame_index(frame_index)?;
                }
                Err(err) => {
                    let _ = runtime.free_frame_index(frame_index);
                    self.free_from(runtime, offset + 1);
                    return Err(err);
                }
            }
        }
        self.len = 0;
        Ok(())
    }

    #[inline]
    pub fn free(&mut self, runtime: &DataPlaneRuntime) {
        self.free_from(runtime, 0);
    }

    #[inline]
    fn frame_for_enqueue(
        &mut self,
        runtime: &DataPlaneRuntime,
        node: NodeId,
    ) -> CoreResult<(FrameIndex, usize, bool)> {
        if let Some(offset) = self.position(node) {
            return self.frame(offset).map(|frame| (frame, offset, false));
        }
        if self.len == MAX_NODE_NEXT_FRAMES {
            return Err(CoreError::internal("node next frame capacity exceeded"));
        }
        let offset = self.len;
        self.nodes[offset] = Some(node);
        self.frames[offset] = Some(runtime.alloc_frame_index()?);
        self.len += 1;
        self.frame(offset).map(|frame| (frame, offset, true))
    }

    #[inline]
    fn node(&self, offset: usize) -> CoreResult<NodeId> {
        self.nodes
            .get(offset)
            .and_then(|node| *node)
            .ok_or_else(|| CoreError::internal("node next entry is missing"))
    }

    #[inline]
    fn frame(&self, offset: usize) -> CoreResult<FrameIndex> {
        self.frames
            .get(offset)
            .and_then(|frame| *frame)
            .ok_or_else(|| CoreError::internal("node next frame is missing"))
    }

    #[inline]
    fn take_frame(&mut self, offset: usize) -> CoreResult<FrameIndex> {
        self.nodes[offset] = None;
        self.frames
            .get_mut(offset)
            .and_then(|frame| frame.take())
            .ok_or_else(|| CoreError::internal("node next frame is missing"))
    }

    #[inline]
    fn position(&self, node: NodeId) -> Option<usize> {
        self.nodes[..self.len]
            .iter()
            .position(|candidate| *candidate == Some(node))
    }

    #[inline]
    fn free_from(&mut self, runtime: &DataPlaneRuntime, start: usize) {
        for offset in start..self.len {
            self.nodes[offset] = None;
            if let Some(frame) = self.frames[offset].take() {
                let _ = runtime.free_frame_index(frame);
            }
        }
        self.len = start.min(self.len);
    }
}

impl NodeNextOwnedFrames {
    #[inline]
    fn enqueue(
        &mut self,
        runtime: &DataPlaneRuntime,
        node: NodeId,
        index: BufferIndex,
    ) -> CoreResult<()> {
        self.frame_for_mut(runtime, node)?.push_index(index)
    }

    #[inline]
    fn enqueue_indices(
        &mut self,
        runtime: &DataPlaneRuntime,
        node: NodeId,
        indices: impl IntoIterator<Item = BufferIndex>,
    ) -> CoreResult<()> {
        self.frame_for_mut(runtime, node)?.push_indices(indices)
    }

    #[inline]
    fn schedule(mut self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        for offset in 0..self.len {
            let node = match self.node(offset) {
                Ok(node) => node,
                Err(err) => {
                    self.free_from(runtime, offset);
                    return Err(err);
                }
            };
            let frame = match self.take_frame(offset) {
                Ok(frame) => frame,
                Err(err) => {
                    self.free_from(runtime, offset + 1);
                    return Err(err);
                }
            };
            if let Err(err) = runtime.schedule_pooled_frame(node, frame) {
                self.free_from(runtime, offset + 1);
                return Err(err);
            }
        }
        self.len = 0;
        Ok(())
    }

    #[inline]
    fn free(&mut self, runtime: &DataPlaneRuntime) {
        self.free_from(runtime, 0);
    }

    #[inline]
    fn frame_for_mut(
        &mut self,
        runtime: &DataPlaneRuntime,
        node: NodeId,
    ) -> CoreResult<&mut BufferFrame> {
        if let Some(offset) = self.position(node) {
            return self.frame_mut(offset);
        }
        if self.len == MAX_NODE_NEXT_FRAMES {
            return Err(CoreError::internal("node next frame capacity exceeded"));
        }
        let offset = self.len;
        self.nodes[offset] = Some(node);
        self.frames[offset] = Some(runtime.alloc_pooled_frame()?);
        self.len += 1;
        self.frame_mut(offset)
    }

    #[inline]
    fn node(&self, offset: usize) -> CoreResult<NodeId> {
        self.nodes
            .get(offset)
            .and_then(|node| *node)
            .ok_or_else(|| CoreError::internal("node next entry is missing"))
    }

    #[inline]
    fn frame_mut(&mut self, offset: usize) -> CoreResult<&mut BufferFrame> {
        self.frames
            .get_mut(offset)
            .and_then(Option::as_mut)
            .map(|frame| &mut **frame)
            .ok_or_else(|| CoreError::internal("node next frame is missing"))
    }

    #[inline]
    fn take_frame(&mut self, offset: usize) -> CoreResult<PooledBufferFrame> {
        self.nodes[offset] = None;
        self.frames
            .get_mut(offset)
            .and_then(|frame| frame.take())
            .ok_or_else(|| CoreError::internal("node next frame is missing"))
    }

    #[inline]
    fn position(&self, node: NodeId) -> Option<usize> {
        self.nodes[..self.len]
            .iter()
            .position(|candidate| *candidate == Some(node))
    }

    #[inline]
    fn free_from(&mut self, runtime: &DataPlaneRuntime, start: usize) {
        for offset in start..self.len {
            self.nodes[offset] = None;
            if let Some(frame) = self.frames[offset].take() {
                let _ = runtime.release_pooled_frame(frame);
            }
        }
        self.len = start.min(self.len);
    }
}
