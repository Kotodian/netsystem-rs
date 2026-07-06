use hammer_infra::vec::Vec;

use crate::buffer::{BufferFrame, BufferIndex, DataPlaneRuntime, Frame, Next};

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

pub struct NodeNextFrames {
    nodes: [Option<NodeId>; MAX_NODE_NEXT_FRAMES],
    frames: [Option<Frame<Next>>; MAX_NODE_NEXT_FRAMES],
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
            frames: std::array::from_fn(|_| None),
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
    pub fn enqueue(&mut self, runtime: &DataPlaneRuntime, node: NodeId, index: BufferIndex) {
        if let Some(current) = self.current_node
            && current == node
        {
            self.current_indices.push(index);
            return;
        }
        if self.current_node.is_none() {
            self.current_node = Some(node);
            self.current_indices.push(index);
            return;
        }
        let (offset, created) = self.frame_for_enqueue(runtime, node);
        let frame = self.frames[offset]
            .as_mut()
            .expect("next enqueue frame mut");
        let result = frame.push_index(index);
        if crate::unlikely(result.is_err()) && created {
            self.free_from(offset);
        }
    }

    #[inline]
    pub fn enqueue_indices(
        &mut self,
        runtime: &DataPlaneRuntime,
        node: NodeId,
        indices: impl IntoIterator<Item = BufferIndex>,
    ) {
        if let Some(current) = self.current_node
            && current == node
        {
            for index in indices {
                self.current_indices.push(index);
            }
            return;
        }
        if self.current_node.is_none() {
            self.current_node = Some(node);
            for index in indices {
                self.current_indices.push(index);
            }
            return;
        }
        let (offset, created) = self.frame_for_enqueue(runtime, node);
        let frame = self.frames[offset]
            .as_mut()
            .expect("next enqueue indices frame mut");
        let result = frame.push_indices(indices);
        if crate::unlikely(result.is_err()) && created {
            self.free_from(offset);
        }
    }

    #[inline]
    pub fn enqueue_optional(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        node: Option<NodeId>,
    ) {
        let Some(node) = node else {
            runtime.free_index(index);
            return;
        };
        self.enqueue(runtime, node, index)
    }

    /// Node-process terminal. Clears the current frame, writes
    /// `current_indices` back, schedules every other staged frame, and returns
    /// a [`NodeResult`] that forwards the current frame to the reuse target.
    ///
    /// On schedule error the current frame is freed and `NodeResult::drop()` is
    /// returned.
    #[inline]
    pub fn finish(mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        frame.clear();
        if !self.current_indices.is_empty() {
            let _ = frame.push_indices(self.current_indices.iter().copied());
        }

        if !self.schedule_staged(runtime) {
            runtime.free_frame(frame);
            return NodeResult::drop();
        }

        match (self.current_node, frame.has_pending()) {
            (Some(node), true) => NodeResult::next_current(node),
            _ => NodeResult::drop(),
        }
    }

    /// Driver / macro terminal. Allocates a frame for the current-frame reuse
    /// indices (there is no current frame to hand off), schedules every staged
    /// frame, and returns `()`.
    #[inline]
    pub fn schedule(mut self, runtime: &DataPlaneRuntime) {
        if let Some(node) = self.current_node
            && !self.current_indices.is_empty()
        {
            let mut frame = runtime.alloc_frame().expect("schedule alloc frame");
            if frame
                .push_indices(self.current_indices.iter().copied())
                .is_err()
            {
                self.free(runtime);
                return;
            }
            if self.len == MAX_NODE_NEXT_FRAMES {
                self.free(runtime);
                return;
            }
            let offset = self.len;
            self.nodes[offset] = Some(node);
            self.frames[offset] = Some(frame);
            self.len += 1;
            self.cached_next_node = Some(node);
            self.cached_next_offset = offset;
        }
        self.schedule_staged(runtime);
    }

    #[inline]
    pub fn free(&mut self, runtime: &DataPlaneRuntime) {
        let _ = runtime;
        self.free_from(0);
    }

    #[inline]
    fn frame_for_enqueue(&mut self, runtime: &DataPlaneRuntime, node: NodeId) -> (usize, bool) {
        if self.cached_next_node == Some(node) && self.frames[self.cached_next_offset].is_some() {
            return (self.cached_next_offset, false);
        }
        if let Some(offset) = self.position(node) {
            self.cached_next_node = Some(node);
            self.cached_next_offset = offset;
            return (offset, false);
        }
        if self.len == MAX_NODE_NEXT_FRAMES {
            panic!("node next frame capacity exceeded");
        }
        let offset = self.len;
        self.nodes[offset] = Some(node);
        self.frames[offset] = Some(runtime.alloc_frame().expect("frame for enqueue alloc"));
        self.len += 1;
        self.cached_next_node = Some(node);
        self.cached_next_offset = offset;
        (offset, true)
    }

    /// Schedule every freshly allocated frame in `self.nodes` / `self.frames`.
    /// Returns `true` if all frames were scheduled, `false` if any error
    /// occurred (partially scheduled frames are left in the runtime queue).
    #[inline]
    fn schedule_staged(&mut self, runtime: &DataPlaneRuntime) -> bool {
        let mut offset = 0usize;
        while offset < self.len {
            let node = self.node(offset);
            let frame = self.take_frame(offset);
            if runtime.submit_frame(frame, node).is_err() {
                self.free_from(offset + 1);
                return false;
            }
            offset += 1;
        }
        self.len = 0;
        self.cached_next_node = None;
        self.cached_next_offset = 0;
        true
    }

    #[inline]
    fn node(&self, offset: usize) -> NodeId {
        self.nodes
            .get(offset)
            .and_then(|node| *node)
            .expect("node next entry is missing")
    }

    #[inline]
    fn take_frame(&mut self, offset: usize) -> Frame<Next> {
        self.nodes[offset] = None;
        self.frames
            .get_mut(offset)
            .and_then(|frame| frame.take())
            .expect("node next frame is missing")
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
    fn free_from(&mut self, start: usize) {
        let mut offset = start;
        while offset < self.len {
            self.nodes[offset] = None;
            let _ = self.frames[offset].take();
            offset += 1;
        }
        self.len = start.min(self.len);
        self.cached_next_node = None;
        self.cached_next_offset = 0;
    }
}
