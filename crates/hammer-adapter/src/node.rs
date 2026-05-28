use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use hammer_core::error::{CoreError, CoreResult};

use crate::buffer::{BufferFrame, DataPlaneRuntime, FrameIndex};

pub const MAX_NODE_NEXT_FRAMES: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u32);

impl NodeId {
    pub fn slot(self) -> u32 {
        self.0
    }
}

pub trait Node {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextFrame {
    Current(NodeId),
    Frame { node: NodeId, frame: FrameIndex },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeResult {
    next: [Option<NextFrame>; MAX_NODE_NEXT_FRAMES],
    len: usize,
}

impl Default for NodeResult {
    fn default() -> Self {
        Self::drop()
    }
}

impl NodeResult {
    pub fn drop() -> Self {
        Self {
            next: [None; MAX_NODE_NEXT_FRAMES],
            len: 0,
        }
    }

    pub fn next_current(node: NodeId) -> Self {
        let mut result = Self::drop();
        result.next[0] = Some(NextFrame::Current(node));
        result.len = 1;
        result
    }

    pub fn next_frame(node: NodeId, frame: FrameIndex) -> Self {
        let mut result = Self::drop();
        result.next[0] = Some(NextFrame::Frame { node, frame });
        result.len = 1;
        result
    }

    pub fn try_next_frames(next: impl IntoIterator<Item = NextFrame>) -> CoreResult<Self> {
        let mut result = Self::drop();
        for next in next {
            result.push(next)?;
        }
        Ok(result)
    }

    pub fn push(&mut self, next: NextFrame) -> CoreResult<()> {
        if self.len == MAX_NODE_NEXT_FRAMES {
            return Err(CoreError::internal("node next frame capacity exceeded"));
        }
        self.next[self.len] = Some(next);
        self.len += 1;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = NextFrame> + '_ {
        self.next[..self.len].iter().filter_map(|next| *next)
    }
}

#[derive(Debug, Clone)]
pub struct NodeRuntime {
    inner: Rc<RefCell<NodeRuntimeInner>>,
    readiness: Rc<NodeReadiness>,
}

#[derive(Default)]
struct NodeReadiness {
    pending: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl std::fmt::Debug for NodeReadiness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeReadiness")
            .field("pending", &self.pending.get())
            .field("has_waker", &self.waker.borrow().is_some())
            .finish()
    }
}

impl NodeReadiness {
    fn mark_pending(&self) {
        self.pending.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    fn clear_pending(&self) {
        self.pending.set(false);
    }

    fn poll_pending(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.pending.get() {
            return Poll::Ready(());
        }
        let mut waker = self.waker.borrow_mut();
        let replace_waker = match waker.as_ref() {
            Some(waker) => !waker.will_wake(cx.waker()),
            None => true,
        };
        if replace_waker {
            *waker = Some(cx.waker().clone());
        }
        if self.pending.get() {
            waker.take();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug)]
struct NodeRuntimeInner {
    nodes: Vec<Rc<RefCell<Box<dyn Node>>>>,
    queue: VecDeque<ScheduledFrame>,
}

impl std::fmt::Debug for dyn Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Node")
    }
}

#[derive(Debug, Clone, Copy)]
struct ScheduledFrame {
    node: NodeId,
    frame: FrameIndex,
}

impl Default for NodeRuntime {
    fn default() -> Self {
        Self {
            inner: Rc::new(RefCell::new(NodeRuntimeInner {
                nodes: Vec::new(),
                queue: VecDeque::new(),
            })),
            readiness: Rc::new(NodeReadiness::default()),
        }
    }
}

impl NodeRuntime {
    pub fn register<N>(&self, node: N) -> NodeId
    where
        N: Node + 'static,
    {
        let mut inner = self.inner.borrow_mut();
        let id = NodeId(u32::try_from(inner.nodes.len()).expect("node index fits u32"));
        inner.nodes.push(Rc::new(RefCell::new(Box::new(node))));
        id
    }

    pub fn pending_len(&self) -> usize {
        self.inner.borrow().queue.len()
    }

    pub fn has_pending(&self) -> bool {
        !self.inner.borrow().queue.is_empty()
    }

    pub fn ready(&self) -> NodeRuntimeReady {
        NodeRuntimeReady {
            readiness: Rc::clone(&self.readiness),
        }
    }

    pub(crate) fn schedule_frame(&self, node: NodeId, frame: FrameIndex) -> CoreResult<()> {
        self.node(node)?;
        self.inner
            .borrow_mut()
            .queue
            .push_back(ScheduledFrame { node, frame });
        self.readiness.mark_pending();
        Ok(())
    }

    pub(crate) fn run_ready(&self, runtime: &DataPlaneRuntime) -> CoreResult<usize> {
        let mut processed = 0usize;
        while let Some(scheduled) = self.pop_scheduled() {
            let node = self.node(scheduled.node)?;
            let mut frame = runtime.take_frame_index(scheduled.frame)?;
            if !frame.has_pending() {
                runtime.release_taken_frame_index(scheduled.frame, frame)?;
                continue;
            }

            frame.clear_next_node();
            let result = match node.borrow_mut().process(runtime, &mut frame) {
                Ok(result) => result,
                Err(err) => {
                    let _ = runtime.release_taken_frame_index(scheduled.frame, frame);
                    return Err(err);
                }
            };
            processed += 1;

            self.dispatch_result(runtime, scheduled.frame, frame, result)?;
        }
        Ok(processed)
    }

    fn node(&self, node: NodeId) -> CoreResult<Rc<RefCell<Box<dyn Node>>>> {
        self.inner
            .borrow()
            .nodes
            .get(node.0 as usize)
            .cloned()
            .ok_or_else(|| CoreError::internal("node id out of bounds"))
    }

    fn pop_scheduled(&self) -> Option<ScheduledFrame> {
        let mut inner = self.inner.borrow_mut();
        let scheduled = inner.queue.pop_front();
        if inner.queue.is_empty() {
            self.readiness.clear_pending();
        }
        scheduled
    }

    fn dispatch_result(
        &self,
        runtime: &DataPlaneRuntime,
        current_index: FrameIndex,
        current_frame: BufferFrame,
        result: NodeResult,
    ) -> CoreResult<()> {
        let mut current_frame = Some(current_frame);
        for next in result.iter() {
            match next {
                NextFrame::Current(node) => {
                    let Some(mut frame) = current_frame.take() else {
                        return Err(CoreError::internal(
                            "current frame cannot be forwarded more than once",
                        ));
                    };
                    if frame.has_pending() {
                        frame.set_next_node(node);
                        runtime.return_taken_frame_index(current_index, frame)?;
                        self.schedule_frame(node, current_index)?;
                    } else {
                        runtime.release_taken_frame_index(current_index, frame)?;
                    }
                }
                NextFrame::Frame { node, frame } => {
                    if runtime.with_frame(frame, BufferFrame::has_pending)? {
                        runtime.with_frame_mut(frame, |frame| frame.set_next_node(node))?;
                        self.schedule_frame(node, frame)?;
                    }
                }
            }
        }

        if let Some(frame) = current_frame {
            runtime.release_taken_frame_index(current_index, frame)?;
        }
        Ok(())
    }
}

pub struct NodeRuntimeReady {
    readiness: Rc<NodeReadiness>,
}

impl Future for NodeRuntimeReady {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.readiness.poll_pending(cx)
    }
}
