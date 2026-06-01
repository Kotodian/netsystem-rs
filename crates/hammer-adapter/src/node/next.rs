use hammer_core::error::{CoreError, CoreResult};

use crate::buffer::{BufferFrame, BufferIndex, DataPlaneRuntime, FrameIndex};
use crate::instruction_set::FrameBatchWidth;

use super::{MAX_NODE_NEXT_FRAMES, NodeId, NodeResult};

pub trait NodeNext: Copy + Eq {
    const COUNT: usize;

    fn slot(self) -> usize;
}

#[derive(Debug, Clone, Copy)]
pub struct NodeNextFrames {
    nodes: [Option<NodeId>; MAX_NODE_NEXT_FRAMES],
    frames: [Option<FrameIndex>; MAX_NODE_NEXT_FRAMES],
    len: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct NodeNextEnqueue {
    speculative_node: NodeId,
    split: NodeNextFrames,
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

impl NodeNextEnqueue {
    #[inline]
    pub fn new(speculative_node: NodeId) -> Self {
        Self {
            speculative_node,
            split: NodeNextFrames::default(),
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

impl NodeNextFrames {
    #[inline]
    pub fn enqueue<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        node: NodeId,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let frame_index = self.frame_for(runtime, node)?;
        runtime.get_frame_mut(frame_index)?.push_index(index)
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
    fn frame_for<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        node: NodeId,
    ) -> CoreResult<FrameIndex> {
        if let Some(offset) = self.position(node) {
            return self.frame(offset);
        }
        if self.len == MAX_NODE_NEXT_FRAMES {
            return Err(CoreError::internal("node next frame capacity exceeded"));
        }
        let offset = self.len;
        self.nodes[offset] = Some(node);
        self.frames[offset] = Some(runtime.alloc_frame_index()?);
        self.len += 1;
        self.frame(offset)
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
