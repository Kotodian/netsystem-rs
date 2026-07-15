use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use hammer_core::data_plane::{
    BufferFrame, BufferNodeError, Frame, MAX_NODE_NEXT_SLOTS, NodeHandle, NodeId, NodeKind,
    NodeNext, NodeRegistration, NodeState, Pending,
};
use hammer_core::error::{CoreError, CoreResult, DataPlaneError};
use hammer_infra::boxed::Box;
use hammer_infra::vec::Vec;

use crate::trace::TraceFormatter;
use crate::{DataPlaneInstructionSet, DataPlaneRuntime};

pub mod next;

const DEFAULT_SCHEDULED_FRAME_QUEUE_CAPACITY: usize = 4096;

pub use next::default_prefetch_indices;

/// Run packet logic for every Index in `frame`, record one typed local next
/// decision per Index, then invoke Graph Fanout once.
///
/// The body must yield a value implementing [`NodeNext`] (typically a
/// `node_next` enum variant or a current-node-local `u16` slot). It must not
/// remove Indexes from `frame`; Fanout transfers ownership.
///
/// Next decisions are written into a fixed stack scratch of production frame
/// capacity (256). No heap allocation on this path.
#[macro_export]
macro_rules! process_frame {
    (
        $runtime:expr,
        $frame:expr,
        |$index:pat_param| $body:expr
        $(,)?
    ) => {{
        let mut next_slots = [0u16; ::hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY];
        debug_assert!(
            $frame.len() <= ::hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY,
            "process_frame! input exceeds production frame capacity"
        );
        let mut n = 0usize;
        for &packet_index in $frame.indices() {
            let next = {
                let $index = packet_index;
                $body
            };
            next_slots[n] = ::hammer_core::data_plane::NodeNext::slot(next);
            n += 1;
        }
        $runtime.enqueue_to_next($frame, &next_slots[..n]);
        $crate::node::NodeResult::drop()
    }};
}

pub trait Node {
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult;

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        missing_node_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::empty())
    }

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

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        None
    }

    #[inline]
    fn node_descriptor(&self) -> CoreResult<NodeDescriptor<'_>>
    where
        Self: Sized,
    {
        Ok(NodeDescriptor {
            process: self.node_process(),
            runtime_data: self.node_runtime_data()?,
            registration: Node::node_registration(self),
            initial_nexts: self.node_initial_nexts(),
            trace_formatter: self.node_trace_formatter(),
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeRuntimeData {
    words: [u64; 4],
}

impl NodeRuntimeData {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self { words: [0; 4] }
    }

    #[inline(always)]
    pub const fn from_words(words: [u64; 4]) -> Self {
        Self { words }
    }

    #[inline]
    pub fn from_usize(value: usize) -> CoreResult<Self> {
        Ok(Self::from_words([
            u64::try_from(value).map_err(|_| CoreError::internal("node runtime data overflow"))?,
            0,
            0,
            0,
        ]))
    }

    #[inline(always)]
    pub const fn word(self, index: usize) -> u64 {
        self.words[index]
    }

    #[inline]
    pub fn usize_word(self, index: usize) -> CoreResult<usize> {
        usize::try_from(self.word(index))
            .map_err(|_| CoreError::internal("node runtime data word does not fit usize"))
    }
}

pub type NodeProcessFn = fn(&DataPlaneRuntime, NodeRuntimeData, &mut BufferFrame) -> NodeResult;

type NodeFunction = unsafe fn(&DataPlaneRuntime, NodeRuntimeData, &mut BufferFrame) -> NodeResult;

/// One platform-compiled process-function candidate for an existing Graph Node.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct NodeFunctionRegistration {
    node_name: &'static str,
    instruction_set: DataPlaneInstructionSet,
    function: NodeFunction,
}

impl NodeFunctionRegistration {
    /// Creates a Node Function registration used by `#[node_function]`.
    ///
    /// # Safety
    /// `function` must have no CPU requirements beyond `instruction_set` and
    /// must obey the [`NodeFunction`] calling contract.
    #[doc(hidden)]
    pub const unsafe fn new(
        node_name: &'static str,
        instruction_set: DataPlaneInstructionSet,
        function: unsafe fn(&DataPlaneRuntime, NodeRuntimeData, &mut BufferFrame) -> NodeResult,
    ) -> Self {
        Self {
            node_name,
            instruction_set,
            function,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NodeDescriptor<'a> {
    process: NodeProcessFn,
    runtime_data: NodeRuntimeData,
    registration: NodeRegistration,
    initial_nexts: &'a [NodeId],
    trace_formatter: Option<TraceFormatter>,
}

impl<'a> NodeDescriptor<'a> {
    #[inline]
    pub fn new(
        process: NodeProcessFn,
        runtime_data: NodeRuntimeData,
        registration: NodeRegistration,
        initial_nexts: &'a [NodeId],
        trace_formatter: Option<TraceFormatter>,
    ) -> Self {
        Self {
            process,
            runtime_data,
            registration,
            initial_nexts,
            trace_formatter,
        }
    }

    #[inline]
    pub fn process(self) -> NodeProcessFn {
        self.process
    }

    #[inline]
    pub fn runtime_data(self) -> NodeRuntimeData {
        self.runtime_data
    }

    #[inline]
    pub fn registration(self) -> NodeRegistration {
        self.registration
    }

    #[inline]
    pub fn initial_nexts(self) -> &'a [NodeId] {
        self.initial_nexts
    }

    #[inline]
    pub fn trace_formatter(self) -> Option<TraceFormatter> {
        self.trace_formatter
    }
}

fn missing_node_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    _frame: &mut BufferFrame,
) -> NodeResult {
    let current = runtime
        .current_node()
        .map(|node| match runtime.nodes().node_name(node) {
            Ok(Some(name)) => format!("node {name} ({:?})", node),
            _ => format!("node {:?}", node),
        })
        .unwrap_or_else(|| "current node".to_owned());
    // Programming error: node descriptor missing a process function. Drop
    // the frame silently; the error is logged in the runtime stats.
    _ = current;
    NodeResult::drop()
}

/// Packet graph node that drives an external input boundary.
///
/// Driver nodes are responsible for bringing packets into the data plane from
/// the operating system or protocol I/O. They are runtime roles, not business
/// protocol roles.
pub trait DriverNode {
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
pub trait InternalNode {
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

/// A statically-registered graph node (VPP `vlib_node_registration_t`).
///
/// `Copy` so linkme `distributed_slice` `[..]` catch-all can collect struct
/// literals emitted by `#[graph_node]` across crates.
///
#[derive(Clone, Copy)]
pub struct NodeEntry {
    pub registration: NodeRegistration,
    pub kind: NodeKind,
    pub init: fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId>,
}

#[derive(Debug, Clone, Copy)]
pub enum NoopNode {}

impl Node for NoopNode {
    #[inline(always)]
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        match *self {}
    }
}

pub struct NodeResult;

impl Default for NodeResult {
    fn default() -> Self {
        Self::drop()
    }
}

impl NodeResult {
    pub fn drop() -> Self {
        Self
    }
}

pub struct NodeRuntime {
    inner: Rc<RefCell<NodeRuntimeInner>>,
    queue: Rc<RefCell<ScheduledFrameQueue>>,
    readiness: Rc<NodeReadiness>,
}

impl Clone for NodeRuntime {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
            queue: Rc::clone(&self.queue),
            readiness: Rc::clone(&self.readiness),
        }
    }
}

impl std::fmt::Debug for NodeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.borrow();
        let queue = self.queue.borrow();
        f.debug_struct("NodeRuntime")
            .field("nodes_len", &inner.nodes.len())
            .field("queue_len", &queue.len())
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

#[derive(Clone)]
pub(crate) struct NodeRuntimeInner {
    nodes: Vec<NodeRuntimeSlot>,
    node_states: Vec<NodeState>,
    interrupt_pending: Vec<bool>,
    error_counters: Vec<NodeErrorCounters>,
    error_ids: HashMap<u64, u16>,
    error_slots: Vec<BufferNodeError>,
    runtime_stats: Vec<NodeRuntimeStats>,
    scheduled_frame_queue_capacity: usize,
    handles: HashMap<NodeHandle, NodeId>,
    declared_nodes: HashMap<&'static str, NodeId>,
    node_names: Vec<Option<&'static str>>,
    node_trace_formatters: Vec<Option<TraceFormatter>>,
    next_nodes: Vec<Vec<Option<NodeId>>>,
    pending_next_names: Vec<Vec<Option<&'static str>>>,
    sibling_owners: Vec<Option<NodeId>>,
    siblings: Vec<Vec<NodeId>>,
}

struct NodeRuntimeSlot {
    kind: NodeKind,
    process: NodeFunction,
    runtime_data: NodeRuntimeData,
}

impl Copy for NodeRuntimeSlot {}

impl Clone for NodeRuntimeSlot {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl std::fmt::Debug for NodeRuntimeSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeRuntimeSlot")
            .field("kind", &self.kind)
            .field("runtime_data", &self.runtime_data)
            .finish_non_exhaustive()
    }
}

impl NodeRuntimeSlot {
    #[inline]
    fn dispatch(self, runtime: &DataPlaneRuntime, frame: Frame<Pending>) -> Frame<Pending> {
        let mut frame = frame;
        // SAFETY: graph initialization installs specialized functions only
        // after validating their instruction set against the current CPU.
        // Default safe functions coerce to this pointer type.
        unsafe { (self.process)(runtime, self.runtime_data, &mut frame) };
        frame
    }
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

struct ScheduledFrame {
    node: NodeId,
    frame: Frame<Pending>,
    allow_empty: bool,
}

struct ScheduledFrameQueue {
    slots: Box<[Option<ScheduledFrame>]>,
    head: usize,
    len: usize,
}

impl ScheduledFrameQueue {
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Box::from_fn(capacity, |_| None),
            head: 0,
            len: 0,
        }
    }

    #[inline(always)]
    fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        self.len == self.capacity()
    }

    #[inline]
    fn push_back(&mut self, frame: ScheduledFrame) -> Result<(), ScheduledFrame> {
        if self.is_full() {
            return Err(frame);
        }
        let slot = if self.capacity() == 0 {
            0
        } else {
            (self.head + self.len) % self.capacity()
        };
        self.slots[slot] = Some(frame);
        self.len += 1;
        Ok(())
    }

    #[inline]
    fn pop_front(&mut self) -> Option<ScheduledFrame> {
        if self.len == 0 {
            return None;
        }
        let frame = self.slots[self.head].take();
        self.len -= 1;
        if self.len == 0 {
            self.head = 0;
        } else {
            self.head = (self.head + 1) % self.capacity();
        }
        frame
    }

    #[inline]
    fn drain_all(&mut self) {
        while self.pop_front().is_some() {}
    }
}

impl NodeRuntimeInner {
    #[inline]
    fn error_key(node: NodeId, code: u16) -> u64 {
        (u64::from(node.slot()) << 16) | u64::from(code)
    }

    fn push_node_slot(&mut self, slot: NodeRuntimeSlot) -> NodeId {
        let id = NodeId::new(u32::try_from(self.nodes.len()).expect("node index fits u32"));
        self.nodes.push(slot);
        self.node_states.push(NodeState::Polling);
        self.interrupt_pending.push(false);
        self.error_counters.push(NodeErrorCounters::default());
        self.runtime_stats.push(NodeRuntimeStats::default());
        self.node_names.push(None);
        self.node_trace_formatters.push(None);
        self.next_nodes.push(Vec::new());
        self.pending_next_names.push(Vec::new());
        self.sibling_owners.push(None);
        self.siblings.push(Vec::new());
        id
    }

    fn push_function_node(
        &mut self,
        kind: NodeKind,
        process: NodeProcessFn,
        runtime_data: NodeRuntimeData,
    ) -> NodeId {
        self.push_node_slot(NodeRuntimeSlot {
            kind,
            process,
            runtime_data,
        })
    }

    fn clone_for_worker(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            node_states: self.node_states.clone(),
            interrupt_pending: hammer_infra::vec![false; self.nodes.len()],
            error_counters: hammer_infra::vec![NodeErrorCounters::default(); self.nodes.len()],
            error_ids: self.error_ids.clone(),
            error_slots: self.error_slots.clone(),
            runtime_stats: hammer_infra::vec![NodeRuntimeStats::default(); self.nodes.len()],
            scheduled_frame_queue_capacity: self.scheduled_frame_queue_capacity,
            handles: self.handles.clone(),
            declared_nodes: self.declared_nodes.clone(),
            node_names: self.node_names.clone(),
            node_trace_formatters: self.node_trace_formatters.clone(),
            next_nodes: self.next_nodes.clone(),
            pending_next_names: hammer_infra::vec![Vec::new(); self.nodes.len()],
            sibling_owners: self.sibling_owners.clone(),
            siblings: self.siblings.clone(),
        }
    }

    fn register_function_declared(
        &mut self,
        kind: NodeKind,
        process: NodeProcessFn,
        runtime_data: NodeRuntimeData,
        registration: NodeRegistration,
        initial_nexts: &[NodeId],
        trace_formatter: Option<TraceFormatter>,
        handle: Option<NodeHandle>,
        next_names: Option<&[&'static str]>,
    ) -> CoreResult<NodeId> {
        if initial_nexts.len() > usize::from(u16::MAX) + 1 {
            return Err(CoreError::internal(
                "node next slot cannot be represented as u16",
            ));
        }
        if let Some(next_names) = next_names {
            if !initial_nexts.is_empty() {
                return Err(CoreError::internal(
                    "named next registration cannot also supply resolved initial next nodes",
                ));
            }
            let NodeRegistration::Next { next_count, .. } = registration else {
                return Err(CoreError::internal(
                    "named next registration requires a declared next node",
                ));
            };
            if next_names.len() != next_count {
                return Err(CoreError::internal("node named next count mismatch"));
            }
        } else {
            let mut index = 0usize;
            while index < initial_nexts.len() {
                self.validate_node(initial_nexts[index])?;
                index += 1;
            }
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
        if next_names.is_none()
            && let NodeRegistration::Next { next_count, .. } = registration
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
                let id = self.push_function_node(kind, process, runtime_data);
                self.node_trace_formatters[id.slot() as usize] = trace_formatter;
                if let Some(handle) = handle {
                    self.handles.insert(handle, id);
                }
                Ok(id)
            }
            NodeRegistration::Next { name, next_count } => {
                let id = self.push_function_node(kind, process, runtime_data);
                self.node_names[id.slot() as usize] = Some(name);
                self.node_trace_formatters[id.slot() as usize] = trace_formatter;
                if let Some(next_names) = next_names {
                    self.next_nodes[id.slot() as usize] = hammer_infra::vec![None; next_count];
                    self.pending_next_names[id.slot() as usize] =
                        next_names.iter().copied().map(Some).collect();
                } else {
                    self.next_nodes[id.slot() as usize] = initial_nexts
                        .iter()
                        .copied()
                        .map(Some)
                        .take(next_count)
                        .collect();
                }
                self.declared_nodes.insert(name, id);
                if let Some(handle) = handle {
                    self.handles.insert(handle, id);
                }
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
                    .get(owner.slot() as usize)
                    .cloned()
                    .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
                let id = self.push_function_node(kind, process, runtime_data);
                self.node_names[id.slot() as usize] = Some(name);
                self.node_trace_formatters[id.slot() as usize] = trace_formatter;
                self.next_nodes[id.slot() as usize] = owner_nexts;
                self.sibling_owners[id.slot() as usize] = Some(owner);
                let mut group = self.siblings[owner.slot() as usize].clone();
                group.push(owner);
                let mut sibling_index = 0usize;
                while sibling_index < group.len() {
                    let sibling = group[sibling_index];
                    self.siblings[sibling.slot() as usize].push(id);
                    self.siblings[id.slot() as usize].push(sibling);
                    sibling_index += 1;
                }
                self.declared_nodes.insert(name, id);
                if let Some(handle) = handle {
                    self.handles.insert(handle, id);
                }
                Ok(id)
            }
        }
    }

    fn resolve_named_next_nodes(&mut self) -> CoreResult<()> {
        let mut slot = 0usize;
        while slot < self.nodes.len() {
            if self.pending_next_names[slot].is_empty() {
                slot += 1;
                continue;
            }
            if self.pending_next_names[slot].len() != self.next_nodes[slot].len() {
                return Err(CoreError::internal("pending next name slot mismatch"));
            }
            let mut index = 0usize;
            while index < self.pending_next_names[slot].len() {
                let Some(name) = self.pending_next_names[slot][index] else {
                    index += 1;
                    continue;
                };
                let target =
                    self.declared_nodes.get(name).copied().ok_or_else(|| {
                        CoreError::internal(format!("unknown next node `{name}`"))
                    })?;
                self.validate_node(target)?;
                self.next_nodes[slot][index] = Some(target);
                index += 1;
            }
            self.pending_next_names[slot].clear();
            slot += 1;
        }

        let mut slot = 0usize;
        while slot < self.nodes.len() {
            let Some(owner) = self.sibling_owners[slot] else {
                slot += 1;
                continue;
            };
            self.next_nodes[slot] = self.next_nodes[owner.slot() as usize].clone();
            slot += 1;
        }

        Ok(())
    }

    fn validate_node(&self, node: NodeId) -> CoreResult<()> {
        if self.nodes.get(node.slot() as usize).is_none() {
            return Err(CoreError::internal("node id out of bounds"));
        }
        Ok(())
    }

    fn node_next_slot(&self, node: NodeId, slot: usize) -> CoreResult<NodeId> {
        self.validate_node(node)?;
        self.next_nodes
            .get(node.slot() as usize)
            .and_then(|nexts| nexts.get(slot))
            .copied()
            .flatten()
            .ok_or_else(|| CoreError::internal("node next slot is not registered"))
    }

    fn set_node_next_slot(&mut self, node: NodeId, slot: usize, next: NodeId) -> CoreResult<()> {
        let next_count = self
            .next_nodes
            .get(node.slot() as usize)
            .map(Vec::len)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
        if slot >= next_count {
            return Err(CoreError::internal("node next slot out of range"));
        }
        let mut group = self.siblings[node.slot() as usize].clone();
        group.push(node);
        for sibling in group {
            self.next_nodes[sibling.slot() as usize][slot] = Some(next);
        }
        Ok(())
    }

    fn add_node_next_slot(&mut self, node: NodeId, next: NodeId) -> CoreResult<u16> {
        self.validate_node(node)?;
        self.validate_node(next)?;
        if let Some(slot) = self.next_nodes[node.slot() as usize]
            .iter()
            .position(|target| *target == Some(next))
        {
            return u16::try_from(slot)
                .map_err(|_| CoreError::internal("node next slot cannot be represented as u16"));
        }
        let slot = self
            .next_nodes
            .get(node.slot() as usize)
            .map(Vec::len)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
        let slot = u16::try_from(slot)
            .map_err(|_| CoreError::internal("node next slot cannot be represented as u16"))?;
        let mut group = self.siblings[node.slot() as usize].clone();
        group.push(node);
        for sibling in group {
            let sibling_nexts = self
                .next_nodes
                .get_mut(sibling.slot() as usize)
                .ok_or_else(|| CoreError::internal("node id out of bounds"))?;
            if sibling_nexts.len() != usize::from(slot) {
                return Err(CoreError::internal("node sibling next count mismatch"));
            }
            sibling_nexts.push(Some(next));
        }
        Ok(slot)
    }
}

#[cfg(test)]
mod local_next_slot_tests {
    use super::*;

    #[test]
    fn add_node_next_slot_rejects_non_u16_before_mutating_siblings() {
        let process: NodeProcessFn = |_, _, _| NodeResult::drop();
        let mut inner = NodeRuntimeInner {
            nodes: hammer_infra::vec![
                NodeRuntimeSlot {
                    kind: NodeKind::Internal,
                    process,
                    runtime_data: NodeRuntimeData::empty(),
                },
                NodeRuntimeSlot {
                    kind: NodeKind::Internal,
                    process,
                    runtime_data: NodeRuntimeData::empty(),
                },
                NodeRuntimeSlot {
                    kind: NodeKind::Internal,
                    process,
                    runtime_data: NodeRuntimeData::empty(),
                },
            ],
            node_states: hammer_infra::vec![NodeState::Polling; 3],
            interrupt_pending: hammer_infra::vec![false; 3],
            error_counters: hammer_infra::vec![NodeErrorCounters::default(); 3],
            error_ids: HashMap::new(),
            error_slots: Vec::new(),
            runtime_stats: hammer_infra::vec![NodeRuntimeStats::default(); 3],
            scheduled_frame_queue_capacity: 4,
            handles: HashMap::new(),
            declared_nodes: HashMap::new(),
            node_names: hammer_infra::vec![None; 3],
            node_trace_formatters: hammer_infra::vec![None; 3],
            next_nodes: hammer_infra::vec![
                hammer_infra::vec![Some(NodeId::new(1)); usize::from(u16::MAX) + 1],
                Vec::new(),
                Vec::new(),
            ],
            pending_next_names: hammer_infra::vec![Vec::new(), Vec::new(), Vec::new()],
            sibling_owners: hammer_infra::vec![None; 3],
            siblings: hammer_infra::vec![Vec::new(), Vec::new(), Vec::new()],
        };

        let before = inner.next_nodes[0].len();
        let err = inner
            .add_node_next_slot(NodeId::new(0), NodeId::new(2))
            .expect_err("slot past u16 must fail");
        assert!(err.to_string().contains("cannot be represented as u16"));
        assert_eq!(inner.next_nodes[0].len(), before);
    }
}

impl Default for NodeRuntime {
    fn default() -> Self {
        Self {
            inner: Rc::new(RefCell::new(NodeRuntimeInner {
                nodes: Vec::new(),
                node_states: Vec::new(),
                interrupt_pending: Vec::new(),
                error_counters: Vec::new(),
                error_ids: HashMap::new(),
                error_slots: Vec::new(),
                runtime_stats: Vec::new(),
                scheduled_frame_queue_capacity: DEFAULT_SCHEDULED_FRAME_QUEUE_CAPACITY,
                handles: HashMap::new(),
                declared_nodes: HashMap::new(),
                node_names: Vec::new(),
                node_trace_formatters: Vec::new(),
                next_nodes: Vec::new(),
                pending_next_names: Vec::new(),
                sibling_owners: Vec::new(),
                siblings: Vec::new(),
            })),
            queue: Rc::new(RefCell::new(ScheduledFrameQueue::with_capacity(
                DEFAULT_SCHEDULED_FRAME_QUEUE_CAPACITY,
            ))),
            readiness: Rc::new(NodeReadiness::default()),
        }
    }
}

fn preferred_node_function<'registration>(
    node_name: &str,
    instruction_set: DataPlaneInstructionSet,
    registrations: &'registration [NodeFunctionRegistration],
) -> CoreResult<Option<&'registration NodeFunctionRegistration>> {
    let mut seen = [false; DataPlaneInstructionSet::VARIANT_COUNT];
    let mut selected = None;
    let mut selected_priority = 0;

    for registration in registrations
        .iter()
        .filter(|registration| registration.node_name == node_name)
    {
        let slot = registration.instruction_set.slot();
        if seen[slot] {
            return Err(CoreError::internal(format!(
                "duplicate Node Function for `{node_name}` and {:?}",
                registration.instruction_set
            )));
        }
        seen[slot] = true;

        if !registration.instruction_set.is_supported() {
            continue;
        }
        let Some(priority) = instruction_set.candidate_priority(registration.instruction_set)
        else {
            continue;
        };
        if selected.is_none() || priority > selected_priority {
            selected = Some(registration);
            selected_priority = priority;
        }
    }

    Ok(selected)
}

#[cfg(test)]
mod node_function_tests {
    use super::*;

    #[test]
    fn duplicate_instruction_set_is_rejected() {
        let duplicate = NodeFunctionRegistration {
            node_name: "fixture",
            instruction_set: DataPlaneInstructionSet::Scalar,
            function: missing_node_process,
        };

        let error = match preferred_node_function(
            "fixture",
            DataPlaneInstructionSet::Scalar,
            &[duplicate, duplicate],
        ) {
            Err(error) => error,
            Ok(_) => panic!("duplicate Node Function must fail"),
        };

        assert!(error.to_string().contains("duplicate Node Function"));
    }
}

impl NodeRuntime {
    /// Drain scheduled frames and clear topology so `init_graph` can renumber.
    ///
    /// VPP analogue: barrier-held main-thread graph mutation before worker refork.
    /// Old `NodeId` values become unreachable after this returns.
    pub fn detach_graph_for_rebuild(&self) {
        {
            let mut queue = self.queue.borrow_mut();
            queue.drain_all();
        }
        self.readiness.clear_pending();
        let capacity = {
            let inner = self.inner.borrow();
            inner.scheduled_frame_queue_capacity
        };
        *self.inner.borrow_mut() = NodeRuntimeInner {
            nodes: Vec::new(),
            node_states: Vec::new(),
            interrupt_pending: Vec::new(),
            error_counters: Vec::new(),
            error_ids: HashMap::new(),
            error_slots: Vec::new(),
            runtime_stats: Vec::new(),
            scheduled_frame_queue_capacity: capacity,
            handles: HashMap::new(),
            declared_nodes: HashMap::new(),
            node_names: Vec::new(),
            node_trace_formatters: Vec::new(),
            next_nodes: Vec::new(),
            pending_next_names: Vec::new(),
            sibling_owners: Vec::new(),
            siblings: Vec::new(),
        };
    }

    pub(crate) fn clone_inner_for_worker(&self) -> CoreResult<NodeRuntimeInner> {
        let inner = self.inner.borrow();
        if inner
            .pending_next_names
            .iter()
            .any(|names| !names.is_empty())
        {
            return Err(CoreError::internal(
                "main graph has unresolved next names before worker clone",
            ));
        }
        Ok(inner.clone_for_worker())
    }

    pub(crate) fn from_worker_inner(inner: NodeRuntimeInner) -> Self {
        debug_assert!(
            inner
                .pending_next_names
                .iter()
                .all(|names| names.is_empty()),
            "worker graph must be resolved before cloning"
        );
        let queue_capacity = inner.scheduled_frame_queue_capacity;
        Self {
            inner: Rc::new(RefCell::new(inner)),
            queue: Rc::new(RefCell::new(ScheduledFrameQueue::with_capacity(
                queue_capacity,
            ))),
            readiness: Rc::new(NodeReadiness::default()),
        }
    }

    pub(crate) fn install_node_function(
        &self,
        node: NodeId,
        instruction_set: DataPlaneInstructionSet,
        registrations: &[NodeFunctionRegistration],
    ) -> CoreResult<()> {
        let Some(node_name) = self.node_name(node)? else {
            return Ok(());
        };
        let Some(registration) =
            preferred_node_function(node_name, instruction_set, registrations)?
        else {
            return Ok(());
        };
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.nodes[node.slot() as usize].process = registration.function;
        Ok(())
    }

    pub fn register_driver<N>(&self, node: N) -> NodeId
    where
        N: DriverNode + Node,
    {
        self.try_register_driver(node)
            .expect("register driver node descriptor")
    }

    pub fn try_register_driver<N>(&self, node: N) -> CoreResult<NodeId>
    where
        N: DriverNode + Node,
    {
        self.register_descriptor(
            NodeKind::Driver,
            NodeDescriptor::new(
                node.node_process(),
                node.node_runtime_data()?,
                DriverNode::node_registration(&node),
                DriverNode::node_initial_nexts(&node),
                node.node_trace_formatter(),
            ),
        )
    }

    pub fn register_internal<N>(&self, node: N) -> NodeId
    where
        N: InternalNode + Node,
    {
        self.try_register_internal(node)
            .expect("register internal node descriptor")
    }

    pub fn try_register_internal<N>(&self, node: N) -> CoreResult<NodeId>
    where
        N: InternalNode + Node,
    {
        self.register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                node.node_process(),
                node.node_runtime_data()?,
                InternalNode::node_registration(&node),
                InternalNode::node_initial_nexts(&node),
                node.node_trace_formatter(),
            ),
        )
    }

    pub fn register_internal_with_handle<N>(
        &self,
        handle: NodeHandle,
        node: N,
    ) -> CoreResult<NodeId>
    where
        N: InternalNode + Node,
    {
        self.register_descriptor_with_handle(
            NodeKind::Internal,
            handle,
            NodeDescriptor::new(
                node.node_process(),
                node.node_runtime_data()?,
                InternalNode::node_registration(&node),
                InternalNode::node_initial_nexts(&node),
                node.node_trace_formatter(),
            ),
        )
    }

    /// Type-erased node registration entry point.
    ///
    /// Registers an already-constructed `NodeDescriptor` under the given
    /// `NodeKind`. This is the erased counterpart to `try_register_internal<N>`
    /// / `register_driver<N>`: it lets a `NodeFactory` (see `hammer-runtime`)
    /// register a node without the concrete `N: InternalNode`/`DriverNode`
    /// type being known at the call site, mirroring VPP's `vlib_node_t`
    /// registration where every node is stored as a type-erased record.
    ///
    /// The descriptor's `initial_nexts` must satisfy the contract enforced by
    /// `register_descriptor`: for `NodeRegistration::Next { next_count, .. }`
    /// its length must equal `next_count` (use placeholder `NodeId`s when edges
    /// are resolved later by name via `set_node_next_slot`).
    #[inline]
    pub fn try_register_descriptor(
        &self,
        kind: NodeKind,
        descriptor: NodeDescriptor<'_>,
    ) -> CoreResult<NodeId> {
        self.register_descriptor(kind, descriptor)
    }

    fn register_descriptor(
        &self,
        kind: NodeKind,
        descriptor: NodeDescriptor<'_>,
    ) -> CoreResult<NodeId> {
        self.register_function_declared(
            kind,
            descriptor.process,
            descriptor.runtime_data,
            descriptor.registration,
            descriptor.initial_nexts,
            descriptor.trace_formatter,
        )
    }

    fn register_descriptor_with_handle(
        &self,
        kind: NodeKind,
        handle: NodeHandle,
        descriptor: NodeDescriptor<'_>,
    ) -> CoreResult<NodeId> {
        let mut inner = self.inner.borrow_mut();
        if inner.handles.contains_key(&handle) {
            return Err(CoreError::internal("node handle already registered"));
        }
        let id = inner.register_function_declared(
            kind,
            descriptor.process,
            descriptor.runtime_data,
            descriptor.registration,
            descriptor.initial_nexts,
            descriptor.trace_formatter,
            Some(handle),
            None,
        )?;
        Ok(id)
    }

    fn register_function_declared(
        &self,
        kind: NodeKind,
        process: NodeProcessFn,
        runtime_data: NodeRuntimeData,
        registration: NodeRegistration,
        initial_nexts: &[NodeId],
        trace_formatter: Option<TraceFormatter>,
    ) -> CoreResult<NodeId> {
        let mut inner = self.inner.borrow_mut();
        inner.register_function_declared(
            kind,
            process,
            runtime_data,
            registration,
            initial_nexts,
            trace_formatter,
            None,
            None,
        )
    }

    /// Register an internal node whose next edges are resolved by name in
    /// [`Self::resolve_named_next_nodes`] (VPP `vlib_node_main_init` analogue).
    pub fn try_register_internal_with_next_names<N>(
        &self,
        node: N,
        next_names: &[&'static str],
    ) -> CoreResult<NodeId>
    where
        N: InternalNode + Node,
    {
        let mut inner = self.inner.borrow_mut();
        inner.register_function_declared(
            NodeKind::Internal,
            node.node_process(),
            node.node_runtime_data()?,
            InternalNode::node_registration(&node),
            &[],
            node.node_trace_formatter(),
            None,
            Some(next_names),
        )
    }

    /// Register a driver node whose next edges are resolved by name in
    /// [`Self::resolve_named_next_nodes`].
    pub fn try_register_driver_with_next_names<N>(
        &self,
        node: N,
        next_names: &[&'static str],
    ) -> CoreResult<NodeId>
    where
        N: DriverNode + Node,
    {
        let mut inner = self.inner.borrow_mut();
        inner.register_function_declared(
            NodeKind::Driver,
            node.node_process(),
            node.node_runtime_data()?,
            DriverNode::node_registration(&node),
            &[],
            node.node_trace_formatter(),
            None,
            Some(next_names),
        )
    }

    /// Resolve pending next-node names into `NodeId`s after all graph nodes
    /// are registered (VPP `vlib_node_main_init` name-resolution pass).
    pub fn resolve_named_next_nodes(&self) -> CoreResult<()> {
        self.inner.borrow_mut().resolve_named_next_nodes()
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
    pub fn node_kind(&self, node: NodeId) -> CoreResult<NodeKind> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize].kind)
    }

    #[inline]
    pub fn node_state(&self, node: NodeId) -> CoreResult<NodeState> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.node_states[node.slot() as usize])
    }

    pub fn polling_driver_nodes(&self) -> CoreResult<Vec<NodeId>> {
        let inner = self.inner.borrow();
        let mut nodes = Vec::new();
        let mut slot = 0usize;
        while slot < inner.nodes.len() {
            let node = &inner.nodes[slot];
            if node.kind == NodeKind::Driver && inner.node_states[slot] == NodeState::Polling {
                let id = u32::try_from(slot)
                    .map(NodeId::new)
                    .map_err(|_| CoreError::internal("node id overflow"))?;
                nodes.push(id);
            }
            slot += 1;
        }
        Ok(nodes)
    }

    #[inline]
    pub fn set_node_state(&self, node: NodeId, state: NodeState) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.node_states[node.slot() as usize] = state;
        Ok(())
    }

    pub(crate) fn mark_interrupt_pending(&self, node: NodeId) -> CoreResult<bool> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        let slot = node.slot() as usize;
        if inner.nodes[slot].kind != NodeKind::Driver {
            return Err(CoreError::internal("node is not a driver node"));
        }
        match inner.node_states[slot] {
            NodeState::Disabled | NodeState::Polling => Ok(false),
            NodeState::Interrupt => {
                if inner.interrupt_pending[slot] {
                    Ok(false)
                } else {
                    inner.interrupt_pending[slot] = true;
                    Ok(true)
                }
            }
        }
    }

    pub(crate) fn clear_interrupt_pending(&self, node: NodeId) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.interrupt_pending[node.slot() as usize] = false;
        Ok(())
    }

    #[inline]
    pub fn node_name(&self, node: NodeId) -> CoreResult<Option<&'static str>> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner
            .node_names
            .get(node.slot() as usize)
            .copied()
            .flatten())
    }

    #[inline]
    pub fn node_trace_formatter(&self, node: NodeId) -> CoreResult<Option<TraceFormatter>> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner
            .node_trace_formatters
            .get(node.slot() as usize)
            .copied()
            .flatten())
    }

    pub fn has_pending(&self) -> bool {
        !self.queue.borrow().is_empty()
    }

    pub fn ready(&self) -> NodeRuntimeReady {
        NodeRuntimeReady {
            readiness: Rc::clone(&self.readiness),
        }
    }

    #[inline]
    pub fn increment_node_error(&self, node: NodeId, code: u16) -> CoreResult<u16> {
        let mut inner = self.inner.borrow_mut();
        inner
            .error_counters
            .get_mut(node.slot() as usize)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?
            .increment(code);
        let key = NodeRuntimeInner::error_key(node, code);
        if let Some(encoded) = inner.error_ids.get(&key).copied() {
            return Ok(encoded);
        }
        let next = inner
            .error_slots
            .len()
            .checked_add(1)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| CoreError::internal("node error slot overflow"))?;
        inner.error_slots.push(BufferNodeError::new(node, code));
        inner.error_ids.insert(key, next);
        Ok(next)
    }

    #[inline]
    pub fn node_error_count(&self, node: NodeId, code: u16) -> CoreResult<u64> {
        Ok(self
            .inner
            .borrow()
            .error_counters
            .get(node.slot() as usize)
            .ok_or_else(|| CoreError::internal("node id out of bounds"))?
            .get(code))
    }

    #[inline]
    pub fn decode_node_error(&self, encoded: u16) -> CoreResult<Option<BufferNodeError>> {
        if encoded == 0 {
            return Ok(None);
        }
        Ok(self
            .inner
            .borrow()
            .error_slots
            .get(encoded as usize - 1)
            .copied())
    }

    #[inline]
    pub fn node_next<K: NodeNext>(&self, node: NodeId, key: K) -> CoreResult<NodeId> {
        self.node_next_slot(node, usize::from(key.slot()))
    }

    pub fn node_next_slot(&self, node: NodeId, slot: usize) -> CoreResult<NodeId> {
        let inner = self.inner.borrow();
        inner.node_next_slot(node, slot)
    }

    #[inline]
    pub fn set_node_next<K: NodeNext>(&self, node: NodeId, key: K, next: NodeId) -> CoreResult<()> {
        self.set_node_next_slot(node, usize::from(key.slot()), next)
    }

    pub fn set_node_next_slot(&self, node: NodeId, slot: usize, next: NodeId) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.validate_node(next)?;
        inner.set_node_next_slot(node, slot, next)
    }

    pub fn add_node_next_slot(&self, node: NodeId, next: NodeId) -> CoreResult<u16> {
        let mut inner = self.inner.borrow_mut();
        inner.add_node_next_slot(node, next)
    }

    pub fn node_siblings(&self, node: NodeId) -> CoreResult<Vec<NodeId>> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        inner
            .siblings
            .get(node.slot() as usize)
            .cloned()
            .ok_or_else(|| CoreError::internal("node id out of bounds"))
    }

    pub(crate) fn schedule_frame(
        &self,
        node: NodeId,
        frame: Frame<Pending>,
        allow_empty: bool,
    ) -> CoreResult<()> {
        self.validate_node(node)?;
        self.queue
            .borrow_mut()
            .push_back(ScheduledFrame {
                node,
                frame,
                allow_empty,
            })
            .map_err(|_| DataPlaneError::ScheduledFrameQueueExhausted)?;
        self.readiness.mark_pending();
        Ok(())
    }

    pub(crate) fn run_ready_function_nodes(&self, runtime: &DataPlaneRuntime) -> CoreResult<usize> {
        let mut processed = 0usize;
        while let Some(scheduled) = self.pop_scheduled() {
            let ScheduledFrame {
                node,
                frame,
                allow_empty,
            } = scheduled;
            let slot = self.runtime_slot(node)?;
            if self.node_state(node)? == NodeState::Disabled {
                self.clear_interrupt_pending(node)?;
                continue;
            }
            if !allow_empty && !frame.has_pending() {
                continue;
            }
            self.clear_interrupt_pending(node)?;

            runtime.set_current_node(Some(node));
            let vectors = frame.pending_len();
            let start = Instant::now();
            let frame = slot.dispatch(runtime, frame);
            let elapsed_ns = elapsed_ns(start);
            runtime.flush_fanout_appendable();
            runtime.set_current_node(None);
            let _ = self.record_runtime_stats(node, vectors, elapsed_ns);
            processed += 1;
            runtime.drop_pending_frame_owned(frame);
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
            .get_mut(node.slot() as usize)
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

    fn runtime_slot(&self, node: NodeId) -> CoreResult<NodeRuntimeSlot> {
        let inner = self.inner.borrow();
        inner
            .nodes
            .get(node.slot() as usize)
            .copied()
            .ok_or_else(|| CoreError::internal("node id out of bounds"))
    }

    fn pop_scheduled(&self) -> Option<ScheduledFrame> {
        let mut queue = self.queue.borrow_mut();
        let scheduled = queue.pop_front();
        if queue.is_empty() {
            self.readiness.clear_pending();
        }
        scheduled
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
    use hammer_core::data_plane::DataPlaneBufferConfig;

    struct StatsNode;

    impl Node for StatsNode {
        fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }

        fn node_process(&self) -> NodeProcessFn {
            stats_function_node
        }

        fn node_registration(&self) -> NodeRegistration {
            NodeRegistration::next("function-stats-node", 0)
        }
    }

    impl InternalNode for StatsNode {
        fn node_registration(&self) -> NodeRegistration {
            NodeRegistration::next("function-stats-node", 0)
        }
    }

    fn stats_function_node(
        _: &DataPlaneRuntime,
        _data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> NodeResult {
        let _ = frame.pending_indices();
        NodeResult::drop()
    }

    #[test]
    fn function_node_runtime_stats_count_named_node_frames() {
        let runtime = DataPlaneRuntime::new(crate::DataPlaneRuntimeConfig {
            buffers: DataPlaneBufferConfig {
                buffer_slot_capacity: 16,
                buffer_slots: 8,
                frame_slots: 2,
                ..DataPlaneBufferConfig::default()
            },
        });
        let node = runtime.nodes().register_internal(StatsNode);

        let mut frame = runtime.buffers().get_next_frame(node).expect("alloc frame");
        push_packet(&runtime, &mut frame, b"one");
        push_packet(&runtime, &mut frame, b"two");

        runtime.put_next_frame(frame).expect("put next frame");
        runtime
            .nodes()
            .increment_node_error(node, 7)
            .expect("increment error counter");
        assert_eq!(runtime.run_ready_nodes().expect("run function node"), 1);

        let rows = runtime.nodes().node_runtime_stats_snapshot();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.node_id, node);
        assert_eq!(row.node_name, Some("function-stats-node"));
        assert_eq!(row.error_counters.get(7), 1);
        assert_eq!(row.calls, 1);
        assert_eq!(row.vectors, 2);
        assert_eq!(row.suspends, 0);
        assert!(row.total_elapsed_ns >= row.max_elapsed_ns);
    }

    fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, payload: &[u8]) {
        let buffer = runtime
            .alloc_index_with_bytes(payload)
            .expect("alloc packet");
        frame.push_index(buffer).expect("push packet");
    }
}
