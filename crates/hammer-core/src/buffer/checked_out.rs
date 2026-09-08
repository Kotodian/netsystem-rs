use std::ops::{Deref, DerefMut};

use crate::error::{DataPlaneError, DataPlaneResult};
use crate::graph::NodeId;

use super::DataPlaneBuffers;
use crate::graph::frame;

pub struct Next {
    pub(super) owner: DataPlaneBuffers,
    pub(super) next: NodeId,
    pub(super) frame: Option<Box<frame::Frame>>,
}

pub struct Pending {
    pub(super) owner: DataPlaneBuffers,
    pub(super) frame: Option<Box<frame::Frame>>,
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
    fn frame(&self) -> &frame::Frame {
        match self.state.frame.as_ref() {
            Some(frame) => frame,
            None => super::abort_checked_out_frame(),
        }
    }

    #[inline]
    fn frame_mut(&mut self) -> &mut frame::Frame {
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
                frame: Some(frame),
            },
        })
    }
}

impl Frame<Pending> {
    #[inline]
    pub fn return_with_trace_release(mut self, release_trace: impl FnMut(u32)) {
        if let Some(frame) = self.state.frame.take() {
            self.state
                .owner
                .drop_owned_frame_with_trace(frame, release_trace);
        }
    }
    #[inline]
    fn frame(&self) -> &frame::Frame {
        match self.state.frame.as_ref() {
            Some(frame) => frame,
            None => super::abort_checked_out_frame(),
        }
    }

    #[inline]
    fn frame_mut(&mut self) -> &mut frame::Frame {
        match self.state.frame.as_mut() {
            Some(frame) => frame,
            None => super::abort_checked_out_frame(),
        }
    }
}

impl Drop for Next {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.owner.drop_owned_frame(frame);
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.owner.drop_owned_frame(frame);
        }
    }
}

impl Deref for Frame<Next> {
    type Target = frame::Frame;

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
    type Target = frame::Frame;

    fn deref(&self) -> &Self::Target {
        self.frame()
    }
}

impl DerefMut for Frame<Pending> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.frame_mut()
    }
}
