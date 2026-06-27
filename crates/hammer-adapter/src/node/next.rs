use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;

use crate::buffer::{BufferFrame, BufferIndex, DataPlaneRuntime, FrameIndex};

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

/// Per-dispatch next-frame accumulator. Mirrors VPP `vlib_frame_queue` coupling
/// between a node's current frame and the frames it hands to successor nodes.
///
/// The first distinct next node enqueued becomes the **current-frame reuse
/// target**: its buffer indices stay in the node's current frame (recorded in
/// `current_indices`) instead of allocating a fresh frame. Enqueues to any
/// other next node allocate a frame from the runtime frame pool as before.
///
/// Two terminal methods consume the accumulator:
///
/// - [`Self::finish`] is the node-process path. It clears the current frame,
///   writes `current_indices` back into it, schedules the freshly allocated
///   frames, and returns a [`NodeResult`] that forwards the current frame to
///   the reuse target via [`NextFrame::Current`]. This is the VPP
///   `vlib_put_next_frame` same-next reuse: with a single next, no frame is
///   allocated and the current frame is handed directly to the successor.
///
/// - [`Self::schedule`] is the path used when there is no current frame to
///   reuse (session-queue output, `node_rewrite_frame!` / `node_rewrite_frame_current!`
///   macros). It allocates a frame for `current_indices` and schedules every
///   staged frame itself, returning `()`.
///
/// # Synchronization
///
/// Single-threaded per node dispatch: each `NodeNextFrames` is constructed
/// inside a node process call and consumed before the call returns.
#[derive(Debug)]
pub struct NodeNextFrames {
    nodes: [Option<NodeId>; MAX_NODE_NEXT_FRAMES],
    frames: [Option<FrameIndex>; MAX_NODE_NEXT_FRAMES],
    len: usize,
    cached_next_node: Option<NodeId>,
    cached_next_offset: usize,
    current_node: Option<NodeId>,
    current_indices: Vec<BufferIndex>,
}

impl Default for NodeNextFrames {
    #[inline]
    fn default() -> Self {
        Self {
            nodes: [None; MAX_NODE_NEXT_FRAMES],
            frames: [None; MAX_NODE_NEXT_FRAMES],
            len: 0,
            cached_next_node: None,
            cached_next_offset: 0,
            current_node: None,
            current_indices: Vec::new(),
        }
    }
}

#[inline(always)]
pub fn default_prefetch_indices(runtime: &DataPlaneRuntime, indices: &[BufferIndex]) {
    let mut read = 0usize;
    let len = indices.len();
    while read < len {
        runtime.prefetch_header(indices[read]);
        read += 1;
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
        if let Some(current) = self.current_node
            && current == node
        {
            self.current_indices.push(index);
            return Ok(());
        }
        if self.current_node.is_none() {
            self.current_node = Some(node);
            self.current_indices.push(index);
            return Ok(());
        }
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
        if let Some(current) = self.current_node
            && current == node
        {
            for index in indices {
                self.current_indices.push(index);
            }
            return Ok(());
        }
        if self.current_node.is_none() {
            self.current_node = Some(node);
            for index in indices {
                self.current_indices.push(index);
            }
            return Ok(());
        }
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

    /// Node-process terminal: write the current-frame reuse indices back into
    /// `frame`, schedule every other staged frame, and return a [`NodeResult`]
    /// that forwards the current frame to the reuse target.
    ///
    /// On error the staged frames are released and `frame` is freed via
    /// `runtime.free_frame` so the caller does not need to clean up.
    #[inline]
    pub fn finish(
        mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        frame.clear();
        if !self.current_indices.is_empty() {
            // The current frame's capacity is at least `pending_len()` at
            // dispatch start and `current_indices` is a subset of those
            // indices, so the push cannot overflow.
            frame.push_indices(self.current_indices.iter().copied())?;
        }

        if let Err(err) = self.schedule_staged(runtime) {
            runtime.free_frame(frame);
            return Err(err);
        }

        let result = match (self.current_node, frame.has_pending()) {
            (Some(node), true) => NodeResult::next_current(node),
            _ => NodeResult::drop(),
        };
        Ok(result)
    }

    /// Driver / macro terminal: allocate a frame for the current-frame reuse
    /// indices (there is no current frame to hand off), schedule every staged
    /// frame, and return `()`.
    #[inline]
    pub fn schedule(mut self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        if let Some(node) = self.current_node
            && !self.current_indices.is_empty()
        {
            let frame_index = runtime.alloc_frame_index()?;
            if let Err(err) = runtime
                .get_frame_mut(frame_index)?
                .push_indices(self.current_indices.iter().copied())
            {
                let _ = runtime.free_frame_index(frame_index);
                self.free(runtime);
                return Err(err);
            }
            if self.len == MAX_NODE_NEXT_FRAMES {
                let _ = runtime.free_frame_index(frame_index);
                self.free(runtime);
                return Err(CoreError::internal("node next frame capacity exceeded"));
            }
            let offset = self.len;
            self.nodes[offset] = Some(node);
            self.frames[offset] = Some(frame_index);
            self.len += 1;
            self.cached_next_node = Some(node);
            self.cached_next_offset = offset;
        }
        self.schedule_staged(runtime)?;
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
        if self.cached_next_node == Some(node)
            && let Some(frame) = self.frame(self.cached_next_offset).ok()
        {
            return Ok((frame, self.cached_next_offset, false));
        }
        if let Some(offset) = self.position(node) {
            self.cached_next_node = Some(node);
            self.cached_next_offset = offset;
            return self.frame(offset).map(|frame| (frame, offset, false));
        }
        if self.len == MAX_NODE_NEXT_FRAMES {
            return Err(CoreError::internal("node next frame capacity exceeded"));
        }
        let offset = self.len;
        self.nodes[offset] = Some(node);
        self.frames[offset] = Some(runtime.alloc_frame_index()?);
        self.len += 1;
        self.cached_next_node = Some(node);
        self.cached_next_offset = offset;
        self.frame(offset).map(|frame| (frame, offset, true))
    }

    /// Schedule every freshly allocated frame in `self.nodes` / `self.frames`.
    /// Used by both [`Self::finish`] and [`Self::schedule`]; neither keeps a
    /// borrow on the runtime's frame pool past this call.
    #[inline]
    fn schedule_staged(&mut self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        let mut offset = 0usize;
        while offset < self.len {
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
            offset += 1;
        }
        self.len = 0;
        self.cached_next_node = None;
        self.cached_next_offset = 0;
        Ok(())
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
        let mut offset = 0usize;
        while offset < self.len {
            if self.nodes[offset] == Some(node) {
                return Some(offset);
            }
            offset += 1;
        }
        None
    }

    #[inline]
    fn free_from(&mut self, runtime: &DataPlaneRuntime, start: usize) {
        let mut offset = start;
        while offset < self.len {
            self.nodes[offset] = None;
            if let Some(frame) = self.frames[offset].take() {
                let _ = runtime.free_frame_index(frame);
            }
            offset += 1;
        }
        self.len = start.min(self.len);
        self.cached_next_node = None;
        self.cached_next_offset = 0;
    }
}
