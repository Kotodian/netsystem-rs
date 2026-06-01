use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use hammer_core::error::{CoreError, CoreResult};

use crate::buffer::{BufferFrame, DataPlaneRuntime, FrameIndex};

pub mod next;

pub const MAX_NODE_NEXT_FRAMES: usize = 8;

pub use next::{NodeNext, NodeNextEnqueue, NodeNextFrames};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u32);

impl NodeId {
    #[inline]
    pub fn slot(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeHandle(u32);

impl NodeHandle {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
}

pub trait Node<G = Self> {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult>;
}

/// Packet graph node that drives an external input boundary.
///
/// Driver nodes are responsible for bringing packets into the data plane from
/// the operating system or protocol I/O. They are runtime roles, not business
/// protocol roles.
pub trait DriverNode<G = Self>: Node<G> {}

/// Packet graph node that performs dataplane-internal work.
///
/// Internal nodes are not external I/O drivers. They transform frame metadata,
/// split frames, or select the next graph edge while keeping packet ownership on
/// the current data worker.
pub trait InternalNode<G = Self>: Node<G> {}

#[derive(Debug, Clone, Copy)]
pub enum NoopNode {}

impl Node<NoopNode> for NoopNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime<NoopNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match *self {}
    }
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

pub struct NodeRuntime<N = NoopNode> {
    inner: Rc<RefCell<NodeRuntimeInner<N>>>,
    readiness: Rc<NodeReadiness>,
}

impl<N> Clone for NodeRuntime<N> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
            readiness: Rc::clone(&self.readiness),
        }
    }
}

impl<N> std::fmt::Debug for NodeRuntime<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.borrow();
        f.debug_struct("NodeRuntime")
            .field("nodes_len", &inner.nodes.len())
            .field("queue_len", &inner.queue.len())
            .field("readiness", &self.readiness)
            .finish()
    }
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

struct NodeRuntimeInner<N> {
    nodes: Vec<Option<N>>,
    error_counters: Vec<NodeErrorCounters>,
    queue: VecDeque<ScheduledFrame>,
    handles: HashMap<NodeHandle, NodeId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeErrorCounters {
    counters: Vec<u64>,
}

impl NodeErrorCounters {
    #[inline]
    pub fn increment(&mut self, code: u16) {
        let slot = code as usize;
        if self.counters.len() <= slot {
            self.counters.resize(slot + 1, 0);
        }
        self.counters[slot] += 1;
    }

    #[inline]
    pub fn get(&self, code: u16) -> u64 {
        self.counters.get(code as usize).copied().unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy)]
struct ScheduledFrame {
    node: NodeId,
    frame: FrameIndex,
    allow_empty: bool,
}

impl<N> Default for NodeRuntime<N> {
    fn default() -> Self {
        Self {
            inner: Rc::new(RefCell::new(NodeRuntimeInner {
                nodes: Vec::new(),
                error_counters: Vec::new(),
                queue: VecDeque::new(),
                handles: HashMap::new(),
            })),
            readiness: Rc::new(NodeReadiness::default()),
        }
    }
}

impl<N> NodeRuntime<N> {
    pub fn register(&self, node: N) -> NodeId {
        let mut inner = self.inner.borrow_mut();
        let id = NodeId(u32::try_from(inner.nodes.len()).expect("node index fits u32"));
        inner.nodes.push(Some(node));
        inner.error_counters.push(NodeErrorCounters::default());
        id
    }

    pub fn register_driver<I>(&self, node: I) -> NodeId
    where
        I: DriverNode<N> + Into<N>,
    {
        self.register(node.into())
    }

    pub fn register_internal<I>(&self, node: I) -> NodeId
    where
        I: InternalNode<N> + Into<N>,
    {
        self.register(node.into())
    }

    pub fn register_internal_with_handle<I>(
        &self,
        handle: NodeHandle,
        node: I,
    ) -> CoreResult<NodeId>
    where
        I: InternalNode<N> + Into<N>,
    {
        self.register_with_handle(handle, node.into())
    }

    pub fn register_with_handle(&self, handle: NodeHandle, node: N) -> CoreResult<NodeId> {
        let mut inner = self.inner.borrow_mut();
        if inner.handles.contains_key(&handle) {
            return Err(CoreError::internal("node handle already registered"));
        }
        let id = NodeId(u32::try_from(inner.nodes.len()).expect("node index fits u32"));
        inner.nodes.push(Some(node));
        inner.error_counters.push(NodeErrorCounters::default());
        inner.handles.insert(handle, id);
        Ok(id)
    }

    #[inline]
    pub fn node_for_handle(&self, handle: NodeHandle) -> CoreResult<NodeId> {
        self.inner
            .borrow()
            .handles
            .get(&handle)
            .copied()
            .ok_or_else(|| CoreError::internal("node handle is not registered"))
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

    #[inline]
    pub fn increment_node_error(&self, node: NodeId, code: u16) -> CoreResult<()> {
        self.inner
            .borrow_mut()
            .error_counters
            .get_mut(node.0 as usize)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?
            .increment(code);
        Ok(())
    }

    #[inline]
    pub fn node_error_count(&self, node: NodeId, code: u16) -> CoreResult<u64> {
        Ok(self
            .inner
            .borrow()
            .error_counters
            .get(node.0 as usize)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?
            .get(code))
    }

    pub(crate) fn schedule_frame(
        &self,
        node: NodeId,
        frame: FrameIndex,
        allow_empty: bool,
    ) -> CoreResult<()> {
        self.validate_node(node)?;
        self.inner.borrow_mut().queue.push_back(ScheduledFrame {
            node,
            frame,
            allow_empty,
        });
        self.readiness.mark_pending();
        Ok(())
    }

    pub(crate) fn run_ready(&self, runtime: &DataPlaneRuntime<N>) -> CoreResult<usize>
    where
        N: Node<N>,
    {
        let mut processed = 0usize;
        while let Some(scheduled) = self.pop_scheduled() {
            let mut node = self.take_node(scheduled.node)?;
            let mut frame = runtime.take_frame_index(scheduled.frame)?;
            if !scheduled.allow_empty && !frame.has_pending() {
                self.return_node(scheduled.node, node)?;
                runtime.release_taken_frame_index(scheduled.frame, frame)?;
                continue;
            }

            frame.clear_next_node();
            runtime.set_current_node(Some(scheduled.node));
            let result = node.process(runtime, &mut frame);
            runtime.set_current_node(None);
            self.return_node(scheduled.node, node)?;
            let result = match result {
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

    fn validate_node(&self, node: NodeId) -> CoreResult<()> {
        if self.inner.borrow().nodes.get(node.0 as usize).is_none() {
            return Err(CoreError::internal("node id out of bounds"));
        }
        Ok(())
    }

    fn take_node(&self, node: NodeId) -> CoreResult<N> {
        self.inner
            .borrow_mut()
            .nodes
            .get_mut(node.0 as usize)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?
            .take()
            .ok_or_else(|| CoreError::internal("node is already running"))
    }

    fn return_node(&self, node: NodeId, value: N) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        let slot = inner
            .nodes
            .get_mut(node.0 as usize)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
        if slot.is_some() {
            return Err(CoreError::internal("node slot is already occupied"));
        }
        *slot = Some(value);
        Ok(())
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
        runtime: &DataPlaneRuntime<N>,
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
                        self.schedule_frame(node, current_index, false)?;
                    } else {
                        runtime.release_taken_frame_index(current_index, frame)?;
                    }
                }
                NextFrame::Frame { node, frame } => {
                    if runtime.get_frame(frame)?.has_pending() {
                        runtime.get_frame_mut(frame)?.set_next_node(node);
                        self.schedule_frame(node, frame, false)?;
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
