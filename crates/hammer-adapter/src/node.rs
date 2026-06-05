use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use hammer_core::error::{CoreError, CoreResult};

use crate::buffer::{BufferFrame, DataPlaneRuntime, FrameIndex};
use crate::trace::TraceFormatter;

pub mod next;

pub const MAX_NODE_NEXT_FRAMES: usize = 8;

pub use next::{
    NodeNext, NodeNextEnqueue, NodeNextFrames, NodeNextStorage, NodeNextVectorEnqueue,
    PacketNextResolver, process_cached_rewrite_next, process_cached_speculative_next,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u32);

impl NodeId {
    #[inline(always)]
    pub const fn new(slot: u32) -> Self {
        Self(slot)
    }

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

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        None
    }
}

/// Packet graph node that drives an external input boundary.
///
/// Driver nodes are responsible for bringing packets into the data plane from
/// the operating system or protocol I/O. They are runtime roles, not business
/// protocol roles.
pub trait DriverNode<G = Self>: Node<G> {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::Plain
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &[]
    }
}

/// Packet graph node that performs dataplane-internal work.
///
/// Internal nodes are not external I/O drivers. They transform frame metadata,
/// split frames, or select the next graph edge while keeping packet ownership on
/// the current data worker.
pub trait InternalNode<G = Self>: Node<G> {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::Plain
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &[]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRegistration {
    Plain,
    Next {
        name: &'static str,
        next_count: usize,
    },
    Sibling {
        name: &'static str,
        sibling_of: &'static str,
    },
}

impl NodeRegistration {
    #[inline]
    pub fn next(name: &'static str, next_count: usize) -> Self {
        Self::Next { name, next_count }
    }

    #[inline]
    pub fn sibling_of(name: &'static str, sibling_of: &'static str) -> Self {
        Self::Sibling { name, sibling_of }
    }

    #[inline]
    fn name(self) -> Option<&'static str> {
        match self {
            Self::Plain => None,
            Self::Next { name, .. } | Self::Sibling { name, .. } => Some(name),
        }
    }
}

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
    runtime_stats: Vec<NodeRuntimeStats>,
    queue: VecDeque<ScheduledFrame>,
    handles: HashMap<NodeHandle, NodeId>,
    declared_nodes: HashMap<&'static str, NodeId>,
    node_names: Vec<Option<&'static str>>,
    node_trace_formatters: Vec<Option<TraceFormatter>>,
    next_nodes: Vec<Vec<Option<NodeId>>>,
    siblings: Vec<Vec<NodeId>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct NodeRuntimeStats {
    calls: u64,
    vectors: u64,
    total_elapsed_ns: u64,
    max_elapsed_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRuntimeStatsRow {
    pub node_id: NodeId,
    pub node_name: Option<&'static str>,
    pub error_counters: NodeErrorCounters,
    pub calls: u64,
    pub vectors: u64,
    pub suspends: u64,
    pub total_elapsed_ns: u64,
    pub max_elapsed_ns: u64,
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

impl<N> NodeRuntimeInner<N> {
    fn push_node(&mut self, node: N) -> NodeId {
        let id = NodeId(u32::try_from(self.nodes.len()).expect("node index fits u32"));
        self.nodes.push(Some(node));
        self.error_counters.push(NodeErrorCounters::default());
        self.runtime_stats.push(NodeRuntimeStats::default());
        self.node_names.push(None);
        self.node_trace_formatters.push(None);
        self.next_nodes.push(Vec::new());
        self.siblings.push(Vec::new());
        id
    }

    fn register_declared(
        &mut self,
        node: N,
        registration: NodeRegistration,
        initial_nexts: &[NodeId],
        trace_formatter: Option<TraceFormatter>,
        _handle: Option<NodeHandle>,
    ) -> CoreResult<NodeId> {
        if initial_nexts.len() > MAX_NODE_NEXT_FRAMES {
            return Err(CoreError::internal("node next frame capacity exceeded"));
        }
        for next in initial_nexts.iter().copied() {
            self.validate_node(next)?;
        }
        if matches!(registration, NodeRegistration::Plain) && !initial_nexts.is_empty() {
            return Err(CoreError::internal(
                "plain node cannot declare initial next nodes",
            ));
        }
        if matches!(registration, NodeRegistration::Sibling { .. }) && !initial_nexts.is_empty() {
            return Err(CoreError::internal(
                "node sibling cannot declare initial next nodes",
            ));
        }
        if let NodeRegistration::Next { next_count, .. } = registration
            && next_count != initial_nexts.len()
        {
            return Err(CoreError::internal("node initial next count mismatch"));
        }
        if let Some(name) = registration.name()
            && self.declared_nodes.contains_key(name)
        {
            return Err(CoreError::internal("node name is already registered"));
        }

        match registration {
            NodeRegistration::Plain => {
                let id = self.push_node(node);
                self.node_trace_formatters[id.0 as usize] = trace_formatter;
                Ok(id)
            }
            NodeRegistration::Next { name, next_count } => {
                let id = self.push_node(node);
                self.node_names[id.0 as usize] = Some(name);
                self.node_trace_formatters[id.0 as usize] = trace_formatter;
                self.next_nodes[id.0 as usize] = initial_nexts
                    .iter()
                    .copied()
                    .map(Some)
                    .take(next_count)
                    .collect();
                self.declared_nodes.insert(name, id);
                Ok(id)
            }
            NodeRegistration::Sibling { name, sibling_of } => {
                let owner = self
                    .declared_nodes
                    .get(sibling_of)
                    .copied()
                    .ok_or_else(|| CoreError::internal("node sibling owner is not registered"))?;
                let owner_nexts = self
                    .next_nodes
                    .get(owner.0 as usize)
                    .cloned()
                    .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
                let id = self.push_node(node);
                self.node_names[id.0 as usize] = Some(name);
                self.node_trace_formatters[id.0 as usize] = trace_formatter;
                self.next_nodes[id.0 as usize] = owner_nexts;
                let mut group = self.siblings[owner.0 as usize].clone();
                group.push(owner);
                for sibling in group.iter().copied() {
                    self.siblings[sibling.0 as usize].push(id);
                    self.siblings[id.0 as usize].push(sibling);
                }
                self.declared_nodes.insert(name, id);
                Ok(id)
            }
        }
    }

    fn validate_node(&self, node: NodeId) -> CoreResult<()> {
        if self.nodes.get(node.0 as usize).is_none() {
            return Err(CoreError::internal("node id out of bounds"));
        }
        Ok(())
    }

    fn node_next_slot(&self, node: NodeId, slot: usize) -> CoreResult<NodeId> {
        self.validate_node(node)?;
        self.next_nodes
            .get(node.0 as usize)
            .and_then(|nexts| nexts.get(slot))
            .copied()
            .flatten()
            .ok_or_else(|| CoreError::internal("node next slot is not registered"))
    }

    fn set_node_next_slot(&mut self, node: NodeId, slot: usize, next: NodeId) -> CoreResult<()> {
        let next_count = self
            .next_nodes
            .get(node.0 as usize)
            .map(Vec::len)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
        if slot >= next_count {
            return Err(CoreError::internal("node next slot out of range"));
        }
        let mut group = self.siblings[node.0 as usize].clone();
        group.push(node);
        for sibling in group {
            self.next_nodes[sibling.0 as usize][slot] = Some(next);
        }
        Ok(())
    }
}

impl<N> Default for NodeRuntime<N> {
    fn default() -> Self {
        Self {
            inner: Rc::new(RefCell::new(NodeRuntimeInner {
                nodes: Vec::new(),
                error_counters: Vec::new(),
                runtime_stats: Vec::new(),
                queue: VecDeque::new(),
                handles: HashMap::new(),
                declared_nodes: HashMap::new(),
                node_names: Vec::new(),
                node_trace_formatters: Vec::new(),
                next_nodes: Vec::new(),
                siblings: Vec::new(),
            })),
            readiness: Rc::new(NodeReadiness::default()),
        }
    }
}

impl<N> NodeRuntime<N> {
    pub fn register(&self, node: N) -> NodeId {
        let mut inner = self.inner.borrow_mut();
        inner.push_node(node)
    }

    pub fn register_driver<I>(&self, node: I) -> NodeId
    where
        I: DriverNode<N> + Into<N> + 'static,
    {
        self.try_register_driver(node)
            .expect("driver node registration")
    }

    pub fn try_register_driver<I>(&self, node: I) -> CoreResult<NodeId>
    where
        I: DriverNode<N> + Into<N> + 'static,
    {
        let registration = DriverNode::<N>::node_registration(&node);
        let initial_nexts = DriverNode::<N>::node_initial_nexts(&node).to_vec();
        let trace_formatter = Node::<N>::node_trace_formatter(&node);
        self.register_declared(node.into(), registration, &initial_nexts, trace_formatter)
    }

    pub fn register_internal<I>(&self, node: I) -> NodeId
    where
        I: InternalNode<N> + Into<N> + 'static,
    {
        self.try_register_internal(node)
            .expect("internal node registration")
    }

    pub fn try_register_internal<I>(&self, node: I) -> CoreResult<NodeId>
    where
        I: InternalNode<N> + Into<N> + 'static,
    {
        let registration = InternalNode::<N>::node_registration(&node);
        let initial_nexts = InternalNode::<N>::node_initial_nexts(&node).to_vec();
        let trace_formatter = Node::<N>::node_trace_formatter(&node);
        self.register_declared(node.into(), registration, &initial_nexts, trace_formatter)
    }

    pub fn register_internal_with_handle<I>(
        &self,
        handle: NodeHandle,
        node: I,
    ) -> CoreResult<NodeId>
    where
        I: InternalNode<N> + Into<N> + 'static,
    {
        let registration = InternalNode::<N>::node_registration(&node);
        let initial_nexts = InternalNode::<N>::node_initial_nexts(&node).to_vec();
        let trace_formatter = Node::<N>::node_trace_formatter(&node);
        self.register_declared_with_handle(
            handle,
            node.into(),
            registration,
            &initial_nexts,
            trace_formatter,
        )
    }

    pub fn register_with_handle(&self, handle: NodeHandle, node: N) -> CoreResult<NodeId> {
        self.register_declared_with_handle(handle, node, NodeRegistration::Plain, &[], None)
    }

    fn register_declared(
        &self,
        node: N,
        registration: NodeRegistration,
        initial_nexts: &[NodeId],
        trace_formatter: Option<TraceFormatter>,
    ) -> CoreResult<NodeId> {
        let mut inner = self.inner.borrow_mut();
        inner.register_declared(node, registration, initial_nexts, trace_formatter, None)
    }

    fn register_declared_with_handle(
        &self,
        handle: NodeHandle,
        node: N,
        registration: NodeRegistration,
        initial_nexts: &[NodeId],
        trace_formatter: Option<TraceFormatter>,
    ) -> CoreResult<NodeId> {
        let mut inner = self.inner.borrow_mut();
        if inner.handles.contains_key(&handle) {
            return Err(CoreError::internal("node handle already registered"));
        }
        let id = inner.register_declared(
            node,
            registration,
            initial_nexts,
            trace_formatter,
            Some(handle),
        )?;
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

    pub fn node_runtime_stats_snapshot(&self) -> Vec<NodeRuntimeStatsRow> {
        let inner = self.inner.borrow();
        inner
            .nodes
            .iter()
            .enumerate()
            .map(|(slot, _)| {
                let stats = inner.runtime_stats.get(slot).copied().unwrap_or_default();
                NodeRuntimeStatsRow {
                    node_id: NodeId::new(u32::try_from(slot).expect("node index fits u32")),
                    node_name: inner.node_names.get(slot).copied().flatten(),
                    error_counters: inner.error_counters.get(slot).cloned().unwrap_or_default(),
                    calls: stats.calls,
                    vectors: stats.vectors,
                    suspends: 0,
                    total_elapsed_ns: stats.total_elapsed_ns,
                    max_elapsed_ns: stats.max_elapsed_ns,
                }
            })
            .collect()
    }

    #[inline]
    pub fn node_by_name(&self, name: &str) -> Option<NodeId> {
        self.inner.borrow().declared_nodes.get(name).copied()
    }

    #[inline]
    pub fn node_name(&self, node: NodeId) -> CoreResult<Option<&'static str>> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.node_names.get(node.0 as usize).copied().flatten())
    }

    #[inline]
    pub fn node_trace_formatter(&self, node: NodeId) -> CoreResult<Option<TraceFormatter>> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner
            .node_trace_formatters
            .get(node.0 as usize)
            .copied()
            .flatten())
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

    #[inline]
    pub fn node_next<K: NodeNext>(&self, node: NodeId, key: K) -> CoreResult<NodeId> {
        self.node_next_slot(node, key.slot())
    }

    pub fn node_nexts<const COUNT: usize>(&self, node: NodeId) -> CoreResult<[NodeId; COUNT]> {
        let mut nexts = [NodeId(0); COUNT];
        let inner = self.inner.borrow();
        let node_nexts = inner
            .next_nodes
            .get(node.0 as usize)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
        if node_nexts.len() != COUNT {
            return Err(CoreError::internal("node next count mismatch"));
        }
        for (slot, next) in node_nexts.iter().enumerate() {
            nexts[slot] =
                next.ok_or_else(|| CoreError::internal("node next slot is not registered"))?;
        }
        Ok(nexts)
    }

    pub fn node_next_slot(&self, node: NodeId, slot: usize) -> CoreResult<NodeId> {
        let inner = self.inner.borrow();
        inner.node_next_slot(node, slot)
    }

    #[inline]
    pub fn set_node_next<K: NodeNext>(&self, node: NodeId, key: K, next: NodeId) -> CoreResult<()> {
        self.set_node_next_slot(node, key.slot(), next)
    }

    pub fn set_node_next_slot(&self, node: NodeId, slot: usize, next: NodeId) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.validate_node(next)?;
        inner.set_node_next_slot(node, slot, next)
    }

    pub fn node_siblings(&self, node: NodeId) -> CoreResult<Vec<NodeId>> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        inner
            .siblings
            .get(node.0 as usize)
            .cloned()
            .ok_or_else(|| CoreError::internal("node id out of bounds"))
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
            let vectors = frame.pending_len();
            let start = Instant::now();
            let result = node.process(runtime, &mut frame);
            let elapsed_ns = elapsed_ns(start);
            runtime.set_current_node(None);
            self.return_node(scheduled.node, node)?;
            self.record_runtime_stats(scheduled.node, vectors, elapsed_ns)?;
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

    fn record_runtime_stats(
        &self,
        node: NodeId,
        vectors: usize,
        elapsed_ns: u64,
    ) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        let stats = inner
            .runtime_stats
            .get_mut(node.0 as usize)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
        stats.calls = stats.calls.saturating_add(1);
        stats.vectors = stats.vectors.saturating_add(vectors as u64);
        stats.total_elapsed_ns = stats.total_elapsed_ns.saturating_add(elapsed_ns);
        stats.max_elapsed_ns = stats.max_elapsed_ns.max(elapsed_ns);
        Ok(())
    }

    fn validate_node(&self, node: NodeId) -> CoreResult<()> {
        self.inner.borrow().validate_node(node)
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

    pub fn with_node_mut<R>(
        &self,
        node: NodeId,
        operation: impl FnOnce(&mut N) -> CoreResult<R>,
    ) -> CoreResult<R> {
        self.validate_node(node)?;
        let mut node_value = self.take_node(node)?;
        let result = operation(&mut node_value);
        self.return_node(node, node_value)?;
        result
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
        for offset in 0..result.len {
            let Some(next) = result.next[offset] else {
                continue;
            };
            match next {
                NextFrame::Current(node) => {
                    let Some(mut frame) = current_frame.take() else {
                        Self::release_remaining_next_frames_on_dispatch_error(
                            runtime,
                            &result,
                            offset + 1,
                        );
                        return Err(CoreError::internal(
                            "current frame cannot be forwarded more than once",
                        ));
                    };
                    if frame.has_pending() {
                        if let Err(err) = self.validate_node(node) {
                            let _ = runtime.release_taken_frame_index(current_index, frame);
                            Self::release_remaining_next_frames_on_dispatch_error(
                                runtime,
                                &result,
                                offset + 1,
                            );
                            return Err(err);
                        }
                        frame.set_next_node(node);
                        runtime.return_taken_frame_index(current_index, frame)?;
                        self.schedule_frame(node, current_index, false)?;
                    } else {
                        runtime.release_taken_frame_index(current_index, frame)?;
                    }
                }
                NextFrame::Frame { node, frame } => match runtime.schedule_frame(node, frame) {
                    Ok(true) => {}
                    Ok(false) => {
                        if let Err(err) = runtime.free_frame_index(frame) {
                            Self::release_current_frame_on_dispatch_error(
                                runtime,
                                current_index,
                                &mut current_frame,
                            );
                            Self::release_remaining_next_frames_on_dispatch_error(
                                runtime,
                                &result,
                                offset + 1,
                            );
                            return Err(err);
                        }
                    }
                    Err(err) => {
                        Self::release_next_frame_and_current_on_dispatch_error(
                            runtime,
                            current_index,
                            &mut current_frame,
                            frame,
                        );
                        Self::release_remaining_next_frames_on_dispatch_error(
                            runtime,
                            &result,
                            offset + 1,
                        );
                        return Err(err);
                    }
                },
            }
        }

        if let Some(frame) = current_frame {
            runtime.release_taken_frame_index(current_index, frame)?;
        }
        Ok(())
    }

    fn release_remaining_next_frames_on_dispatch_error(
        runtime: &DataPlaneRuntime<N>,
        result: &NodeResult,
        start: usize,
    ) {
        for next in result.next[start..result.len]
            .iter()
            .filter_map(|next| *next)
        {
            if let NextFrame::Frame { frame, .. } = next {
                let _ = runtime.free_frame_index(frame);
            }
        }
    }

    fn release_current_frame_on_dispatch_error(
        runtime: &DataPlaneRuntime<N>,
        current_index: FrameIndex,
        current_frame: &mut Option<BufferFrame>,
    ) {
        if let Some(frame) = current_frame.take() {
            let _ = runtime.release_taken_frame_index(current_index, frame);
        }
    }

    fn release_next_frame_and_current_on_dispatch_error(
        runtime: &DataPlaneRuntime<N>,
        current_index: FrameIndex,
        current_frame: &mut Option<BufferFrame>,
        next_frame: FrameIndex,
    ) {
        let _ = runtime.free_frame_index(next_frame);
        Self::release_current_frame_on_dispatch_error(runtime, current_index, current_frame);
    }
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct StatsNode;

    impl Node<StatsNode> for StatsNode {
        fn process(
            &mut self,
            runtime: &DataPlaneRuntime<StatsNode>,
            frame: &mut BufferFrame,
        ) -> CoreResult<NodeResult> {
            for buffer in frame.drain_pending() {
                runtime.free_index(buffer);
            }
            Ok(NodeResult::drop())
        }
    }

    impl InternalNode<StatsNode> for StatsNode {
        fn node_registration(&self) -> NodeRegistration {
            NodeRegistration::next("stats-node", 0)
        }
    }

    #[test]
    fn node_runtime_stats_snapshot_counts_named_node_frames() {
        let runtime = DataPlaneRuntime::<StatsNode>::with_capacities(16, 8, 4, 2);
        let node = runtime.nodes().register_internal(StatsNode);

        let first = runtime.alloc_frame_index().expect("alloc first frame");
        push_packet(&runtime, first, b"one");
        push_packet(&runtime, first, b"two");
        let second = runtime.alloc_frame_index().expect("alloc second frame");
        push_packet(&runtime, second, b"three");

        assert!(runtime.schedule_frame(node, first).expect("schedule first"));
        assert!(
            runtime
                .schedule_frame(node, second)
                .expect("schedule second")
        );
        runtime
            .nodes()
            .increment_node_error(node, 7)
            .expect("increment error counter");
        assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 2);

        let rows = runtime.nodes().node_runtime_stats_snapshot();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.node_id, node);
        assert_eq!(row.node_name, Some("stats-node"));
        assert_eq!(row.error_counters.get(7), 1);
        assert_eq!(row.calls, 2);
        assert_eq!(row.vectors, 3);
        assert_eq!(row.suspends, 0);
        assert!(row.total_elapsed_ns >= row.max_elapsed_ns);
    }

    fn push_packet(runtime: &DataPlaneRuntime<StatsNode>, frame: FrameIndex, payload: &[u8]) {
        let buffer = runtime
            .alloc_index_with_bytes(crate::RouteMetadata::default(), payload)
            .expect("alloc packet");
        runtime
            .get_frame_mut(frame)
            .expect("get frame")
            .push_index(buffer)
            .expect("push packet");
    }
}
