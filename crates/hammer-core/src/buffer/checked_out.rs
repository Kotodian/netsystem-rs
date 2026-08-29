use std::ops::{Deref, DerefMut};

use crate::error::{DataPlaneError, DataPlaneResult};
use crate::graph::NodeId;

use super::{BufferFrame, DataPlaneBuffers};

pub struct Next {
    pub(super) owner: DataPlaneBuffers,
    pub(super) index: super::Index,
    pub(super) next: NodeId,
    pub(super) frame: Option<BufferFrame>,
}

pub struct Pending {
    pub(super) owner: DataPlaneBuffers,
    pub(super) index: super::Index,
    pub(super) frame: Option<BufferFrame>,
}

pub struct Frame<State> {
    pub(super) state: State,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameBatchWidth {
    Pair,
    Quad,
    Octo,
}

impl Frame<Next> {
    #[inline]
    fn frame(&self) -> &BufferFrame {
        match self.state.frame.as_ref() {
            Some(frame) => frame,
            None => super::abort_checked_out_frame(),
        }
    }

    #[inline]
    fn frame_mut(&mut self) -> &mut BufferFrame {
        match self.state.frame.as_mut() {
            Some(frame) => frame,
            None => super::abort_checked_out_frame(),
        }
    }

    #[inline]
    pub fn next(&self) -> NodeId {
        self.state.next
    }

    #[inline]
    pub fn into_pending(mut self) -> DataPlaneResult<Frame<Pending>> {
        let frame = self
            .state
            .frame
            .take()
            .ok_or(DataPlaneError::FrameSlotCheckedOut)?;
        Ok(Frame {
            state: Pending {
                owner: self.state.owner.clone(),
                index: self.state.index,
                frame: Some(frame),
            },
        })
    }
}

impl Frame<Pending> {
    #[inline]
    fn frame(&self) -> &BufferFrame {
        match self.state.frame.as_ref() {
            Some(frame) => frame,
            None => super::abort_checked_out_frame(),
        }
    }

    #[inline]
    fn frame_mut(&mut self) -> &mut BufferFrame {
        match self.state.frame.as_mut() {
            Some(frame) => frame,
            None => super::abort_checked_out_frame(),
        }
    }

    #[inline]
    pub fn return_with_trace_release(mut self, release_trace: impl FnMut(u32)) {
        if let Some(frame) = self.state.frame.take() {
            self.state
                .owner
                .drop_owned_frame_with_trace(self.state.index, frame, release_trace);
        }
    }
}

impl Drop for Next {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.owner.drop_owned_frame(self.index, frame);
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.owner.drop_owned_frame(self.index, frame);
        }
    }
}

impl Deref for Frame<Next> {
    type Target = BufferFrame;

    fn deref(&self) -> &Self::Target {
        self.frame()
    }
}

impl DerefMut for Frame<Next> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.frame_mut()
    }
}

impl Deref for Frame<Pending> {
    type Target = BufferFrame;

    fn deref(&self) -> &Self::Target {
        self.frame()
    }
}

impl DerefMut for Frame<Pending> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.frame_mut()
    }
}
