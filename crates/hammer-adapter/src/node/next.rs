use std::marker::PhantomData;

use hammer_core::error::{CoreError, CoreResult};

use crate::buffer::{BufferIndex, DataPlaneRuntime, FrameIndex};

use super::{MAX_NODE_NEXT_FRAMES, NodeId};

#[macro_export]
macro_rules! define_node_next {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($variant:ident),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[repr(usize)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        $vis enum $name {
            $($variant),+
        }

        impl $crate::node::NodeNext for $name {
            const COUNT: usize = <[()]>::len(&[$($crate::define_node_next!(@unit $variant)),+]);

            #[inline]
            fn slot(self) -> usize {
                self as usize
            }
        }

        const _: () = {
            assert!(
                <$name as $crate::node::NodeNext>::COUNT
                    <= $crate::node::MAX_NODE_NEXT_FRAMES
            );
        };
    };
    (@unit $variant:ident) => {
        {
            let _ = stringify!($variant);
            ()
        }
    };
}

pub trait NodeNext: Copy + Eq {
    const COUNT: usize;

    fn slot(self) -> usize;
}

#[derive(Debug, Clone, Copy)]
pub struct NodeNextTable<N>
where
    N: NodeNext,
{
    nodes: [NodeId; MAX_NODE_NEXT_FRAMES],
    _marker: PhantomData<fn() -> N>,
}

impl<N> NodeNextTable<N>
where
    N: NodeNext,
{
    #[inline]
    pub fn new(default: NodeId) -> Self {
        debug_assert!(N::COUNT <= MAX_NODE_NEXT_FRAMES);
        Self {
            nodes: [default; MAX_NODE_NEXT_FRAMES],
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn with(mut self, next: N, node: NodeId) -> Self {
        debug_assert!(next.slot() < N::COUNT);
        self.nodes[next.slot()] = node;
        self
    }

    #[inline]
    pub fn node(self, next: N) -> NodeId {
        debug_assert!(next.slot() < N::COUNT);
        self.nodes[next.slot()]
    }
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
