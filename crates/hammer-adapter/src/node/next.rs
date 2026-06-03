use hammer_core::error::{CoreError, CoreResult};

use crate::buffer::{
    BufferBatchMut, BufferFrame, BufferIndex, DataPlaneRuntime, FrameIndex, PooledBufferFrame,
};
use crate::instruction_set::FrameBatchWidth;

use super::{MAX_NODE_NEXT_FRAMES, NodeId, NodeResult};

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

pub trait PacketNextResolver<G> {
    fn next_for_index(
        &self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
    ) -> CoreResult<NodeId>;
}

#[inline(always)]
pub fn process_cached_speculative_next<G, R>(
    runtime: &DataPlaneRuntime<G>,
    frame: &mut BufferFrame,
    cached_next: &mut Option<NodeId>,
    resolver: &R,
) -> CoreResult<NodeResult>
where
    R: PacketNextResolver<G> + ?Sized,
{
    let Some(first_index) = frame.pending_indices().first().copied() else {
        return Ok(NodeResult::drop());
    };
    let mut last_next = None;
    let result = match *cached_next {
        Some(speculative) => {
            NodeNextEnqueue::new(speculative).validate_frame(runtime, frame, |index| {
                let node = resolver.next_for_index(runtime, index)?;
                last_next = Some(node);
                Ok(node)
            })
        }
        None => {
            let first_next = resolver.next_for_index(runtime, first_index)?;
            last_next = Some(first_next);
            NodeNextEnqueue::new(first_next).validate_frame_with_first_next(
                runtime,
                frame,
                first_index,
                first_next,
                |index| {
                    let node = resolver.next_for_index(runtime, index)?;
                    last_next = Some(node);
                    Ok(node)
                },
            )
        }
    };
    if result.is_ok()
        && let Some(node) = last_next
    {
        *cached_next = Some(node);
    }
    result
}

#[inline(always)]
pub fn process_cached_rewrite_next<G, R>(
    runtime: &DataPlaneRuntime<G>,
    frame: &mut BufferFrame,
    cached_next: &mut Option<NodeId>,
    resolver: &R,
) -> CoreResult<NodeResult>
where
    R: PacketNextResolver<G> + ?Sized,
{
    let mut next_frames = NodeNextFrames::default();
    let mut current_next = *cached_next;
    let mut last_next = None;
    let result = frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        let node = resolver.next_for_index(runtime, index)?;
        last_next = Some(node);
        match current_next {
            Some(current) if current == node => Ok(Some(index)),
            Some(_) => {
                next_frames.enqueue(runtime, node, index)?;
                Ok(None)
            }
            None => {
                current_next = Some(node);
                Ok(Some(index))
            }
        }
    });
    if let Err(err) = result {
        next_frames.free(runtime);
        return Err(err);
    }

    next_frames.schedule(runtime)?;
    if let Some(node) = last_next {
        *cached_next = Some(node);
    }
    if frame.has_pending()
        && let Some(node) = current_next
    {
        Ok(NodeResult::next_current(node))
    } else {
        Ok(NodeResult::drop())
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
    pub fn validate_frame<G>(
        mut self,
        runtime: &DataPlaneRuntime<G>,
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
    pub fn validate_frame_with_first_next<G>(
        mut self,
        runtime: &DataPlaneRuntime<G>,
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
    pub fn validate_frame_with_first_next_and_prefetch<G>(
        mut self,
        runtime: &DataPlaneRuntime<G>,
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
    pub fn validate_frame_with_first_next_and_buffer_batch_prefetch<G>(
        mut self,
        runtime: &DataPlaneRuntime<G>,
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
    pub fn validate_frame_with_width<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
        if result.is_err() {
            self.split.free(runtime);
        }
        result
    }

    #[inline(always)]
    pub fn validate_frame_with_nexts<G>(
        mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
        nexts: &[NodeId],
    ) -> CoreResult<NodeResult> {
        let speculative_node = self.speculative_node;
        let width = runtime.preferred_frame_batch_width();
        self.validate_frame_with_nexts_and_width(runtime, frame, width, nexts)?;
        self.finish(runtime, frame, speculative_node)
    }

    #[inline(always)]
    pub fn validate_frame_with_nexts_and_width<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
        if result.is_err() {
            self.split.free(runtime);
        }
        result
    }

    #[inline(always)]
    pub fn validate_frame_with_width_and_prefetch<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
        if result.is_err() {
            self.split.free(runtime);
        }
        result
    }

    #[inline(always)]
    pub fn validate_frame_with_width_and_buffer_batch_prefetch<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
        if result.is_err() {
            self.split.free(runtime);
        }
        result
    }

    #[inline(always)]
    pub fn validate_frame_with_buffer_batch_chunks<G>(
        mut self,
        runtime: &DataPlaneRuntime<G>,
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
        if result.is_err() {
            self.split.free(runtime);
        }
        result?;
        self.finish(runtime, frame, speculative_node)
    }

    #[inline(always)]
    fn finish<G>(
        self,
        runtime: &DataPlaneRuntime<G>,
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
    pub fn enqueue_frame_with_buffer_batch_chunks<G>(
        mut self,
        runtime: &DataPlaneRuntime<G>,
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

        if result.is_err() {
            self.frames.free(runtime);
        }
        result?;

        frame.clear();
        self.frames.schedule(runtime)?;
        Ok((NodeResult::drop(), current_next))
    }

    #[inline(always)]
    fn enqueue_frame_quad_chunks<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
    fn enqueue_frame_pair_chunks<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
    fn enqueue_chunk<G, const N: usize>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
        if offset >= indices.len() {
            return;
        }
        let end = (offset + width).min(indices.len());
        prefetch_indices(batch, &indices[offset..end]);
    }
}

impl NodeNextFrames {
    #[inline]
    pub fn enqueue<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        node: NodeId,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let (frame_index, offset, created) = self.frame_for_enqueue(runtime, node)?;
        let result = runtime.get_frame_mut(frame_index)?.push_index(index);
        if result.is_err() && created {
            self.free_from(runtime, offset);
        }
        result
    }

    #[inline]
    pub fn enqueue_indices<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        node: NodeId,
        indices: impl IntoIterator<Item = BufferIndex>,
    ) -> CoreResult<()> {
        let (frame_index, offset, created) = self.frame_for_enqueue(runtime, node)?;
        let result = runtime.get_frame_mut(frame_index)?.push_indices(indices);
        if result.is_err() && created {
            self.free_from(runtime, offset);
        }
        result
    }

    #[inline]
    pub fn enqueue_optional<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
    pub fn schedule<G>(mut self, runtime: &DataPlaneRuntime<G>) -> CoreResult<()> {
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
    pub fn free<G>(&mut self, runtime: &DataPlaneRuntime<G>) {
        self.free_from(runtime, 0);
    }

    #[inline]
    fn frame_for_enqueue<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
    fn free_from<G>(&mut self, runtime: &DataPlaneRuntime<G>, start: usize) {
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
    fn enqueue<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        node: NodeId,
        index: BufferIndex,
    ) -> CoreResult<()> {
        self.frame_for_mut(runtime, node)?.push_index(index)
    }

    #[inline]
    fn enqueue_indices<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        node: NodeId,
        indices: impl IntoIterator<Item = BufferIndex>,
    ) -> CoreResult<()> {
        self.frame_for_mut(runtime, node)?.push_indices(indices)
    }

    #[inline]
    fn schedule<G>(mut self, runtime: &DataPlaneRuntime<G>) -> CoreResult<()> {
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
    fn free<G>(&mut self, runtime: &DataPlaneRuntime<G>) {
        self.free_from(runtime, 0);
    }

    #[inline]
    fn frame_for_mut<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
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
    fn free_from<G>(&mut self, runtime: &DataPlaneRuntime<G>, start: usize) {
        for offset in start..self.len {
            self.nodes[offset] = None;
            if let Some(frame) = self.frames[offset].take() {
                let _ = runtime.release_pooled_frame(frame);
            }
        }
        self.len = start.min(self.len);
    }
}
