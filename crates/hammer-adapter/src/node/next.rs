use hammer_core::error::{CoreError, CoreResult};

use crate::buffer::{BufferIndex, DataPlaneRuntime, FrameIndex};

use super::{MAX_NODE_NEXT_FRAMES, NodeId};

pub trait NodeNext: Copy + Eq {
    const COUNT: usize;

    fn slot(self) -> usize;
}

#[derive(Debug, Clone, Copy)]
pub struct NodeNextGroups {
    nodes: [Option<NodeId>; MAX_NODE_NEXT_FRAMES],
    len: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct NodeNextFrames {
    nodes: [Option<NodeId>; MAX_NODE_NEXT_FRAMES],
    frames: [Option<FrameIndex>; MAX_NODE_NEXT_FRAMES],
    len: usize,
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
    pub fn schedule<G>(self, runtime: &DataPlaneRuntime<G>) -> CoreResult<()> {
        for offset in 0..self.len {
            let node = self.node(offset)?;
            let frame_index = self.frame(offset)?;
            if !runtime.schedule_frame(node, frame_index)? {
                runtime.free_frame_index(frame_index)?;
            }
        }
        Ok(())
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
    fn position(&self, node: NodeId) -> Option<usize> {
        self.nodes[..self.len]
            .iter()
            .position(|candidate| *candidate == Some(node))
    }
}

impl Default for NodeNextGroups {
    fn default() -> Self {
        Self {
            nodes: [None; MAX_NODE_NEXT_FRAMES],
            len: 0,
        }
    }
}

impl NodeNextGroups {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn push(&mut self, node: NodeId) -> CoreResult<()> {
        if self.position(node).is_some() {
            return Ok(());
        }
        if self.len == MAX_NODE_NEXT_FRAMES {
            return Err(CoreError::internal("node next frame capacity exceeded"));
        }
        self.nodes[self.len] = Some(node);
        self.len += 1;
        Ok(())
    }

    #[inline]
    pub fn node(&self, index: usize) -> CoreResult<NodeId> {
        self.nodes
            .get(index)
            .and_then(|node| *node)
            .ok_or_else(|| CoreError::internal("node next group index out of bounds"))
    }

    #[inline]
    pub fn position(&self, node: NodeId) -> Option<usize> {
        self.nodes[..self.len]
            .iter()
            .position(|candidate| *candidate == Some(node))
    }
}
