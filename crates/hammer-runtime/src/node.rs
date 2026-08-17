use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use crate::error::{RuntimeError, RuntimeResult};
use hammer_core::data_plane::{
    BufferFrame, Frame, NodeErrorIndex, NodeErrorIndexError, NodeHandle, NodeId, NodeKind,
    NodeNext, NodeRegistration, NodeState, Pending,
};
use hammer_core::error::DataPlaneError;
use hammer_stats::CounterVectorSimple;

use crate::engine::NodeStatsRoot;
use crate::trace::TraceFormatter;
use crate::{DataPlaneRuntime, Simd};

pub mod next;

const DEFAULT_SCHEDULED_FRAME_QUEUE_CAPACITY: usize = 4096;

pub use next::default_prefetch_indices;

/// A generated node-local error code that maps to a preinstalled global
/// [`NodeErrorIndex`].
pub trait NodeErrorCode {
    fn local_code(self) -> u16;
}

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
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
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
    fn node_descriptor(&self) -> RuntimeResult<NodeDescriptor<'_>>
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
    pub fn from_usize(value: usize) -> RuntimeResult<Self> {
        Ok(Self::from_words([
            u64::try_from(value).map_err(|_| RuntimeError::NodeRuntimeDataOverflow { value })?,
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
    pub fn usize_word(self, index: usize) -> RuntimeResult<usize> {
        let value = self.word(index);
        usize::try_from(value)
            .map_err(|_| RuntimeError::NodeRuntimeDataWordOutOfRange { word: index, value })
    }
}

pub type NodeProcessFn = fn(&DataPlaneRuntime, NodeRuntimeData, &mut BufferFrame) -> NodeResult;

type NodeFunction = unsafe fn(&DataPlaneRuntime, NodeRuntimeData, &mut BufferFrame) -> NodeResult;

/// One platform-compiled process-function candidate for an existing Graph Node.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct NodeFunctionRegistration {
    node_name: &'static str,
    simd_bytes: usize,
    function: NodeFunction,
}

impl NodeFunctionRegistration {
    /// Creates a Node Function registration used by `#[node_function]`.
    ///
    /// # Safety
    /// `function` must have no CPU requirements beyond `simd` and must obey
    /// the [`NodeFunction`] calling contract.
    #[doc(hidden)]
    pub const unsafe fn new<const LANES: usize>(
        node_name: &'static str,
        _: Simd<u8, LANES>,
        function: unsafe fn(&DataPlaneRuntime, NodeRuntimeData, &mut BufferFrame) -> NodeResult,
    ) -> Self {
        assert!(matches!(LANES, 1 | 16 | 32 | 64));
        Self {
            node_name,
            simd_bytes: core::mem::size_of::<Simd<u8, LANES>>(),
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

/// Severity attached to a node business-error counter (VPP
/// `vl_counter_severity_e`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeErrorSeverity {
    Error = 0,
    Warn = 1,
    Info = 2,
}

/// Immutable metadata for one ordered node business-error counter.
///
/// Runtime statistics handles are installed separately during graph
/// materialization; this declaration carries only the source-order metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeErrorDescriptor {
    pub name: &'static str,
    pub severity: NodeErrorSeverity,
    pub description: &'static str,
}

impl NodeErrorDescriptor {
    #[inline]
    pub const fn new(
        name: &'static str,
        severity: NodeErrorSeverity,
        description: &'static str,
    ) -> Self {
        Self {
            name,
            severity,
            description,
        }
    }
}

/// A statically-registered graph node (VPP `vlib_node_registration_t`).
///
/// `Copy` so linkme `distributed_slice` `[..]` catch-all can collect struct
/// literals emitted by `#[graph_node]` across crates. Error descriptors stay
/// immutable and ordered exactly as declared by the node macro.
///
#[derive(Clone, Copy)]
pub struct NodeEntry {
    pub registration: NodeRegistration,
    pub kind: NodeKind,
    pub init: fn(&DataPlaneRuntime) -> RuntimeResult<NodeId>,
    pub error_counters: &'static [NodeErrorDescriptor],
}

impl NodeEntry {
    #[inline]
    pub const fn new(
        registration: NodeRegistration,
        kind: NodeKind,
        init: fn(&DataPlaneRuntime) -> RuntimeResult<NodeId>,
    ) -> Self {
        Self {
            registration,
            kind,
            init,
            error_counters: &[],
        }
    }
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
    topology_owner: bool,
}

impl Clone for NodeRuntime {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
            queue: Rc::clone(&self.queue),
            readiness: Rc::clone(&self.readiness),
            topology_owner: self.topology_owner,
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

/// Direct StatsMain handles installed with a graph snapshot. The bundle is
/// immutable after installation; each worker graph clone shares the handles'
/// atomic cells without a lock or a fallible clone operation.
#[derive(Clone)]
pub(crate) struct NodeStatsHandles {
    pub(crate) roots: [Arc<CounterVectorSimple>; NodeStatsRoot::COUNT],
}

pub(crate) struct NodeRuntimeInner {
    nodes: Vec<NodeRuntimeSlot>,
    node_states: Vec<NodeState>,
    interrupt_pending: Vec<bool>,
    input_main_loops_per_call: Vec<u32>,
    error_indices: Vec<Box<[NodeErrorIndex]>>,
    next_error_index: u32,
    error_tables_installed: Vec<bool>,
    stats_handles: Option<Arc<NodeStatsHandles>>,
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

impl Clone for NodeRuntimeInner {
    fn clone(&self) -> Self {
        let node_count = self.nodes.len();
        Self {
            nodes: self.nodes.clone(),
            node_states: self.node_states.clone(),
            interrupt_pending: vec![false; node_count],
            input_main_loops_per_call: self.input_main_loops_per_call.clone(),
            error_indices: self.error_indices.clone(),
            next_error_index: self.next_error_index,
            error_tables_installed: self.error_tables_installed.clone(),
            stats_handles: self.stats_handles.as_ref().map(Arc::clone),
            scheduled_frame_queue_capacity: self.scheduled_frame_queue_capacity,
            handles: self.handles.clone(),
            declared_nodes: self.declared_nodes.clone(),
            node_names: self.node_names.clone(),
            node_trace_formatters: self.node_trace_formatters.clone(),
            next_nodes: self.next_nodes.clone(),
            pending_next_names: vec![Vec::new(); node_count],
            sibling_owners: self.sibling_owners.clone(),
            siblings: self.siblings.clone(),
        }
    }
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
            slots: (0..capacity)
                .map(|_| None)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
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
    fn inherit_worker_state(&mut self, current: &Self) -> RuntimeResult<()> {
        if self.nodes.len() < current.nodes.len() {
            return Err(RuntimeError::WorkerGraphUpdateNotAdditive);
        }

        for slot in 0..current.nodes.len() {
            if self.node_names[slot] != current.node_names[slot]
                || self.nodes[slot].kind != current.nodes[slot].kind
                || self.error_indices[slot] != current.error_indices[slot]
            {
                return Err(RuntimeError::WorkerGraphUpdateNotAdditive);
            }

            // The main thread publishes topology and process functions. The
            // worker retains only state owned by its established node instance.
            self.nodes[slot].runtime_data = current.nodes[slot].runtime_data;
            self.node_states[slot] = current.node_states[slot];
            self.input_main_loops_per_call[slot] = current.input_main_loops_per_call[slot];
        }
        Ok(())
    }

    fn materialize_node_errors(
        &mut self,
        node: NodeId,
        descriptors: &[NodeErrorDescriptor],
    ) -> RuntimeResult<()> {
        self.validate_node(node)?;
        let slot = node.slot() as usize;
        if self.error_tables_installed[slot] {
            return Ok(());
        }

        let count = descriptors.len();
        if count > usize::from(u16::MAX) {
            return Err(RuntimeError::NodeErrorSlotOverflow);
        }

        // VPP `vlib_register_errors` reserves one contiguous global range per
        // node. Index zero remains the packet-buffer "no error" sentinel.
        let first = self.next_error_index;
        let end = first
            .checked_add(u32::try_from(count).map_err(|_| RuntimeError::NodeErrorSlotOverflow)?)
            .ok_or(RuntimeError::NodeErrorSlotOverflow)?;
        if end > u32::from(u16::MAX) + 1 {
            return Err(RuntimeError::NodeErrorSlotOverflow);
        }

        let mut indices = Vec::with_capacity(count);
        for local_code in 0..count {
            let encoded = u16::try_from(first + local_code as u32)
                .map_err(|_| RuntimeError::NodeErrorSlotOverflow)?;
            let index = NodeErrorIndex::try_from(encoded)
                .map_err(|_: NodeErrorIndexError| RuntimeError::NodeErrorSlotOverflow)?;
            indices.push(index);
        }

        self.error_indices[slot] = indices.into_boxed_slice();
        self.next_error_index = end;
        self.error_tables_installed[slot] = true;
        Ok(())
    }

    fn install_stats_handles(
        &mut self,
        handles: Arc<NodeStatsHandles>,
        expected_rows: u32,
    ) -> RuntimeResult<()> {
        let minimum_node_columns = u32::try_from(self.nodes.len())
            .map_err(|_| RuntimeError::NodeIdOverflow {
                slot: self.nodes.len(),
            })?
            .max(1);
        let minimum_error_columns = self.next_error_index.max(1);
        for root in NodeStatsRoot::ALL {
            let handle = &handles.roots[root.index()];
            let minimum_columns = if root.is_error() {
                minimum_error_columns
            } else {
                minimum_node_columns
            };
            if handle.rows() != expected_rows || handle.columns() < minimum_columns {
                return Err(RuntimeError::NodeStatsLayout {
                    metric: root.name(),
                    rows: handle.rows(),
                    columns: handle.columns(),
                    expected_rows,
                    minimum_columns,
                });
            }
        }
        self.stats_handles = Some(handles);
        Ok(())
    }

    #[inline]
    fn error_column_count(&self) -> u32 {
        self.next_error_index.max(1)
    }

    fn validate_node_error_batch(
        &self,
        nodes: &[(NodeId, &'static [NodeErrorDescriptor])],
    ) -> RuntimeResult<()> {
        let mut next = self.next_error_index;
        for &(node, descriptors) in nodes {
            self.validate_node(node)?;
            let slot = node.slot() as usize;
            if self.error_tables_installed[slot] {
                continue;
            }
            let count = descriptors.len();
            if count > usize::from(u16::MAX) {
                return Err(RuntimeError::NodeErrorSlotOverflow);
            }
            next = next
                .checked_add(u32::try_from(count).map_err(|_| RuntimeError::NodeErrorSlotOverflow)?)
                .ok_or(RuntimeError::NodeErrorSlotOverflow)?;
            if next > u32::from(u16::MAX) + 1 {
                return Err(RuntimeError::NodeErrorSlotOverflow);
            }
        }
        Ok(())
    }

    #[inline]
    fn node_error_index(&self, node: NodeId, code: u16) -> RuntimeResult<NodeErrorIndex> {
        self.validate_node(node)?;
        self.error_indices[node.slot() as usize]
            .get(code as usize)
            .copied()
            .ok_or(RuntimeError::NodeErrorSlotOverflow)
    }

    fn push_node_slot(&mut self, slot: NodeRuntimeSlot) -> NodeId {
        let id = NodeId::new(u32::try_from(self.nodes.len()).expect("node index fits u32"));
        self.nodes.push(slot);
        self.node_states.push(NodeState::Polling);
        self.interrupt_pending.push(false);
        self.input_main_loops_per_call.push(0);
        self.error_indices.push(Box::default());
        self.error_tables_installed.push(false);
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
    ) -> RuntimeResult<NodeId> {
        if initial_nexts.len() > usize::from(u16::MAX) + 1 {
            return Err(RuntimeError::NodeNextCountOverflow {
                count: initial_nexts.len(),
            });
        }
        if let Some(next_names) = next_names {
            if !initial_nexts.is_empty() {
                return Err(RuntimeError::NamedNextWithResolvedTargets);
            }
            let NodeRegistration::Next { next_count, .. } = registration else {
                return Err(RuntimeError::NamedNextRegistrationKindInvalid);
            };
            if next_names.len() != next_count {
                return Err(RuntimeError::NamedNextCountMismatch {
                    declared: next_count,
                    actual: next_names.len(),
                });
            }
        } else {
            let mut index = 0usize;
            while index < initial_nexts.len() {
                self.validate_node(initial_nexts[index])?;
                index += 1;
            }
        }
        if matches!(registration, NodeRegistration::Plain) && !initial_nexts.is_empty() {
            return Err(RuntimeError::PlainNodeHasInitialNexts {
                count: initial_nexts.len(),
            });
        }
        if matches!(registration, NodeRegistration::Sibling { .. }) && !initial_nexts.is_empty() {
            return Err(RuntimeError::SiblingNodeHasInitialNexts {
                count: initial_nexts.len(),
            });
        }
        if next_names.is_none()
            && !initial_nexts.is_empty()
            && let NodeRegistration::Next { next_count, .. } = registration
            && next_count != initial_nexts.len()
        {
            return Err(RuntimeError::InitialNextCountMismatch {
                declared: next_count,
                actual: initial_nexts.len(),
            });
        }
        if let Some(name) = registration.name()
            && self.declared_nodes.contains_key(name)
        {
            return Err(RuntimeError::NodeNameAlreadyRegistered { name });
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
                    self.next_nodes[id.slot() as usize] = vec![None; next_count];
                    self.pending_next_names[id.slot() as usize] =
                        next_names.iter().copied().map(Some).collect();
                } else if initial_nexts.is_empty() {
                    self.next_nodes[id.slot() as usize] = vec![None; next_count];
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
                let owner = self.declared_nodes.get(sibling_of).copied().ok_or(
                    RuntimeError::SiblingOwnerNotRegistered {
                        node: name,
                        owner: sibling_of,
                    },
                )?;
                let owner_nexts = self
                    .next_nodes
                    .get(owner.slot() as usize)
                    .cloned()
                    .ok_or(RuntimeError::NodeNotRegistered { node: owner })?;
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

    fn resolve_named_next_nodes(&mut self) -> RuntimeResult<()> {
        let mut slot = 0usize;
        while slot < self.nodes.len() {
            if self.pending_next_names[slot].is_empty() {
                slot += 1;
                continue;
            }
            assert_eq!(
                self.pending_next_names[slot].len(),
                self.next_nodes[slot].len(),
                "pending named-next table must stay aligned with graph next slots"
            );
            let mut index = 0usize;
            while index < self.pending_next_names[slot].len() {
                let Some(name) = self.pending_next_names[slot][index] else {
                    index += 1;
                    continue;
                };
                let target = if let Some(target) = self.declared_nodes.get(name).copied() {
                    self.pending_next_names[slot][index] = None;
                    target
                } else {
                    self.declared_nodes
                        .get("drop")
                        .copied()
                        .ok_or(DataPlaneError::NamedNextFallbackMissing)?
                };
                self.validate_node(target)?;
                self.next_nodes[slot][index] = Some(target);
                index += 1;
            }
            if self.pending_next_names[slot].iter().all(Option::is_none) {
                self.pending_next_names[slot].clear();
            }
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

    fn validate_node(&self, node: NodeId) -> RuntimeResult<()> {
        if self.nodes.get(node.slot() as usize).is_none() {
            return Err(RuntimeError::NodeNotRegistered { node });
        }
        Ok(())
    }

    fn node_next_slot(&self, node: NodeId, slot: usize) -> RuntimeResult<NodeId> {
        self.validate_node(node)?;
        self.next_nodes
            .get(node.slot() as usize)
            .and_then(|nexts| nexts.get(slot))
            .copied()
            .flatten()
            .ok_or(RuntimeError::NodeNextSlotNotRegistered { node, slot })
    }

    fn set_node_next_slot(&mut self, node: NodeId, slot: usize, next: NodeId) -> RuntimeResult<()> {
        let next_count = self
            .next_nodes
            .get(node.slot() as usize)
            .map(Vec::len)
            .ok_or(RuntimeError::NodeNotRegistered { node })?;
        if slot >= next_count {
            return Err(RuntimeError::NodeNextSlotOutOfRange {
                node,
                slot,
                next_count,
            });
        }
        let mut group = self.siblings[node.slot() as usize].clone();
        group.push(node);
        for &sibling in &group {
            let sibling_slot = sibling.slot() as usize;
            assert_eq!(
                self.next_nodes[sibling_slot].len(),
                next_count,
                "graph siblings must have identical next counts"
            );
            let pending = &self.pending_next_names[sibling_slot];
            if !pending.is_empty() {
                assert_eq!(
                    pending.len(),
                    next_count,
                    "pending named-next table must stay aligned with graph next slots"
                );
            }
        }
        for sibling in group {
            let sibling_slot = sibling.slot() as usize;
            if !self.pending_next_names[sibling_slot].is_empty() {
                self.pending_next_names[sibling_slot][slot] = None;
            }
            self.next_nodes[sibling_slot][slot] = Some(next);
        }
        Ok(())
    }

    fn add_node_next_slot(&mut self, node: NodeId, next: NodeId) -> RuntimeResult<u16> {
        self.validate_node(node)?;
        self.validate_node(next)?;
        if let Some(slot) = self.next_nodes[node.slot() as usize]
            .iter()
            .position(|target| *target == Some(next))
        {
            return Ok(u16::try_from(slot)
                .expect("registered graph next slot must fit its u16 representation"));
        }
        let slot = self
            .next_nodes
            .get(node.slot() as usize)
            .map(Vec::len)
            .ok_or(RuntimeError::NodeNotRegistered { node })?;
        let slot =
            u16::try_from(slot).map_err(|_| RuntimeError::NodeNextCountOverflow { count: slot })?;
        let mut group = self.siblings[node.slot() as usize].clone();
        group.push(node);
        for &sibling in &group {
            let sibling_slot = sibling.slot() as usize;
            let sibling_nexts = &self.next_nodes[sibling_slot];
            assert_eq!(
                sibling_nexts.len(),
                usize::from(slot),
                "graph siblings must have identical next counts"
            );
            let pending = &self.pending_next_names[sibling_slot];
            if !pending.is_empty() {
                assert_eq!(
                    pending.len(),
                    usize::from(slot),
                    "pending named-next table must stay aligned with graph next slots"
                );
            }
        }
        for sibling in group {
            let sibling_slot = sibling.slot() as usize;
            if !self.pending_next_names[sibling_slot].is_empty() {
                self.pending_next_names[sibling_slot].push(None);
            }
            self.next_nodes[sibling_slot].push(Some(next));
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
            nodes: vec![
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
            node_states: vec![NodeState::Polling; 3],
            interrupt_pending: vec![false; 3],
            input_main_loops_per_call: vec![0; 3],
            error_indices: vec![Box::default(); 3],
            next_error_index: 1,
            error_tables_installed: vec![false; 3],
            stats_handles: None,
            scheduled_frame_queue_capacity: 4,
            handles: HashMap::new(),
            declared_nodes: HashMap::new(),
            node_names: vec![None; 3],
            node_trace_formatters: vec![None; 3],
            next_nodes: vec![
                vec![Some(NodeId::new(1)); usize::from(u16::MAX) + 1],
                Vec::new(),
                Vec::new(),
            ],
            pending_next_names: vec![Vec::new(), Vec::new(), Vec::new()],
            sibling_owners: vec![None; 3],
            siblings: vec![Vec::new(), Vec::new(), Vec::new()],
        };

        let before = inner.next_nodes[0].len();
        let err = inner
            .add_node_next_slot(NodeId::new(0), NodeId::new(2))
            .expect_err("slot past u16 must fail");
        assert!(matches!(
            err,
            RuntimeError::NodeNextCountOverflow { count }
                if count == usize::from(u16::MAX) + 1
        ));
        assert_eq!(inner.next_nodes[0].len(), before);
    }

    #[test]
    fn dynamic_next_added_before_named_resolution_keeps_slots_aligned() {
        let process: NodeProcessFn = |_, _, _| NodeResult::drop();
        let mut inner = Rc::try_unwrap(NodeRuntime::default().inner)
            .ok()
            .expect("default node runtime has one owner")
            .into_inner();
        let source = inner
            .register_function_declared(
                NodeKind::Internal,
                process,
                NodeRuntimeData::empty(),
                NodeRegistration::next("source", 1),
                &[],
                None,
                None,
                Some(&["target"]),
            )
            .expect("register named-next source");
        let target = inner
            .register_function_declared(
                NodeKind::Internal,
                process,
                NodeRuntimeData::empty(),
                NodeRegistration::next("target", 0),
                &[],
                None,
                None,
                Some(&[]),
            )
            .expect("register named target");
        let dynamic = inner
            .register_function_declared(
                NodeKind::Internal,
                process,
                NodeRuntimeData::empty(),
                NodeRegistration::next("dynamic", 0),
                &[],
                None,
                None,
                Some(&[]),
            )
            .expect("register dynamic target");

        assert_eq!(
            inner
                .add_node_next_slot(source, dynamic)
                .expect("append dynamic next"),
            1
        );
        assert_eq!(inner.pending_next_names[source.slot() as usize].len(), 2);
        inner
            .resolve_named_next_nodes()
            .expect("resolve named nexts");
        assert_eq!(
            inner.node_next_slot(source, 0).expect("resolved target"),
            target
        );
        assert_eq!(
            inner.node_next_slot(source, 1).expect("dynamic target"),
            dynamic
        );
    }
}

impl Default for NodeRuntime {
    fn default() -> Self {
        Self {
            inner: Rc::new(RefCell::new(NodeRuntimeInner {
                nodes: Vec::new(),
                node_states: Vec::new(),
                interrupt_pending: Vec::new(),
                input_main_loops_per_call: Vec::new(),
                error_indices: Vec::new(),
                next_error_index: 1,
                error_tables_installed: Vec::new(),
                    stats_handles: None,
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
            topology_owner: true,
        }
    }
}

impl From<NodeRuntimeInner> for NodeRuntime {
    fn from(inner: NodeRuntimeInner) -> Self {
        debug_assert!(
            inner
                .pending_next_names
                .iter()
                .all(|names| names.is_empty()),
            "worker graph must be resolved before installation"
        );
        let queue_capacity = inner.scheduled_frame_queue_capacity;
        Self {
            inner: Rc::new(RefCell::new(inner)),
            queue: Rc::new(RefCell::new(ScheduledFrameQueue::with_capacity(
                queue_capacity,
            ))),
            readiness: Rc::new(NodeReadiness::default()),
            topology_owner: false,
        }
    }
}

fn preferred_node_function<'registration>(
    node_name: &str,
    max_simd_bytes: usize,
    registrations: &'registration [NodeFunctionRegistration],
) -> RuntimeResult<Option<&'registration NodeFunctionRegistration>> {
    let mut selected = None;

    for (offset, registration) in registrations.iter().enumerate() {
        if registration.node_name != node_name {
            continue;
        }
        if registrations[..offset].iter().any(|previous| {
            previous.node_name == node_name && previous.simd_bytes == registration.simd_bytes
        }) {
            return Err(DataPlaneError::DuplicateNodeFunction {
                node: registration.node_name,
                simd_bytes: registration.simd_bytes,
            }
            .into());
        }
        if registration.simd_bytes > max_simd_bytes {
            continue;
        }
        if selected.is_none_or(|current: &NodeFunctionRegistration| {
            registration.simd_bytes > current.simd_bytes
        }) {
            selected = Some(registration);
        }
    }

    Ok(selected)
}

#[cfg(test)]
mod node_function_tests {
    use super::*;

    fn graph_update_probe_process(
        _: &DataPlaneRuntime,
        _: NodeRuntimeData,
        _: &mut BufferFrame,
    ) -> NodeResult {
        NodeResult::drop()
    }

    fn register_graph_update_probe(runtime: &NodeRuntime, name: &'static str) -> NodeId {
        runtime
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    graph_update_probe_process,
                    NodeRuntimeData::empty(),
                    NodeRegistration::next(name, 0),
                    &[],
                    None,
                ),
            )
            .expect("register graph update probe")
    }

    struct NamedNextProbe {
        name: &'static str,
        next_count: usize,
    }

    impl Node for NamedNextProbe {
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }

        fn node_process(&self) -> NodeProcessFn {
            graph_update_probe_process
        }
    }

    impl InternalNode for NamedNextProbe {
        fn node_registration(&self) -> NodeRegistration {
            NodeRegistration::next(self.name, self.next_count)
        }
    }

    #[test]
    fn duplicate_simd_width_is_rejected() {
        let duplicate = NodeFunctionRegistration {
            node_name: "fixture",
            simd_bytes: 1,
            function: missing_node_process,
        };

        let error = match preferred_node_function("fixture", 1, &[duplicate, duplicate]) {
            Err(error) => error,
            Ok(_) => panic!("duplicate Node Function must fail"),
        };

        assert!(error.to_string().contains("duplicate node function"));
    }

    #[test]
    fn node_function_selects_the_widest_candidate_within_runtime_capacity() {
        let candidates = [1, 16, 32, 64].map(|simd_bytes| NodeFunctionRegistration {
            node_name: "fixture",
            simd_bytes,
            function: missing_node_process,
        });

        let selected = preferred_node_function("fixture", 32, &candidates)
            .expect("select Node Function")
            .expect("matching Node Function");

        assert_eq!(selected.simd_bytes, 32);
    }

    #[test]
    fn worker_graph_replacement_uses_the_updated_main_topology() {
        let main = NodeRuntime::default();
        let alpha = register_graph_update_probe(&main, "alpha");
        let worker: NodeRuntime = main.snapshot().into();
        let worker_data = NodeRuntimeData::from_usize(41).expect("worker runtime data");
        worker
            .set_node_runtime_data(alpha, worker_data)
            .expect("bind worker data");
        worker
            .set_node_state(alpha, NodeState::Disabled)
            .expect("disable worker alpha");
        worker.inner.borrow_mut().interrupt_pending[alpha.slot() as usize] = true;

        let beta = register_graph_update_probe(&main, "beta");
        worker.replace_graph(main.snapshot());

        assert_eq!(worker.node_by_name("alpha"), Some(alpha));
        assert_eq!(worker.node_by_name("beta"), Some(beta));
        assert_eq!(
            worker.node_state(alpha).expect("alpha state"),
            NodeState::Polling
        );
        assert!(!worker.inner.borrow().interrupt_pending[alpha.slot() as usize]);
        assert_eq!(
            worker
                .inner
                .borrow()
                .nodes
                .get(alpha.slot() as usize)
                .map(|node| node.runtime_data),
            Some(NodeRuntimeData::empty())
        );
    }

    #[test]
    fn missing_named_next_uses_drop_until_late_target_is_registered() {
        let runtime = NodeRuntime::default();
        let drop_node = runtime
            .try_register_internal_with_next_names(
                NamedNextProbe {
                    name: "drop",
                    next_count: 0,
                },
                &[],
            )
            .expect("register drop");
        let source = runtime
            .try_register_internal_with_next_names(
                NamedNextProbe {
                    name: "source",
                    next_count: 1,
                },
                &["late-target"],
            )
            .expect("register source");

        runtime
            .resolve_named_next_nodes()
            .expect("defer missing next to drop");
        assert_eq!(
            runtime
                .inner
                .borrow()
                .node_next_slot(source, 0)
                .expect("source fallback"),
            drop_node
        );

        let target = runtime
            .try_register_internal_with_next_names(
                NamedNextProbe {
                    name: "late-target",
                    next_count: 0,
                },
                &[],
            )
            .expect("register late target");
        runtime
            .resolve_named_next_nodes()
            .expect("resolve late target");

        assert_eq!(
            runtime
                .inner
                .borrow()
                .node_next_slot(source, 0)
                .expect("resolved source next"),
            target
        );
    }

    #[test]
    fn declared_next_slots_can_be_wired_after_registration() {
        let runtime = NodeRuntime::default();
        let target = runtime
            .try_register_internal_with_next_names(
                NamedNextProbe {
                    name: "target",
                    next_count: 0,
                },
                &[],
            )
            .expect("register target");
        let source = runtime
            .try_register_internal(NamedNextProbe {
                name: "source",
                next_count: 1,
            })
            .expect("register source");

        assert!(runtime.node_next_slot(source, 0).is_err());
        runtime
            .set_node_next_slot(source, 0, target)
            .expect("wire declared next slot");
        assert_eq!(
            runtime.node_next_slot(source, 0).expect("resolved next"),
            target
        );
    }
}

impl NodeRuntime {
    #[inline]
    fn ensure_topology_owner(&self) -> RuntimeResult<()> {
        if self.topology_owner {
            Ok(())
        } else {
            Err(RuntimeError::GraphTopologyMutationFromWorker)
        }
    }

    pub(crate) fn validate_node_error_batch(
        &self,
        nodes: &[(NodeId, &'static [NodeErrorDescriptor])],
    ) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        self.inner.borrow().validate_node_error_batch(nodes)
    }

    /// Install a node's ordered error descriptors on the topology owner.
    ///
    /// Normal graph construction calls this from [`DataPlaneRuntime::init_graph`]
    /// using [`NodeEntry::error_counters`]. The explicit method is retained as
    /// a structural hook for callers that register nodes directly.
    pub fn materialize_node_errors(
        &self,
        node: NodeId,
        descriptors: &[NodeErrorDescriptor],
    ) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        self.inner
            .borrow_mut()
            .materialize_node_errors(node, descriptors)
    }

    pub(crate) fn install_stats_handles(
        &self,
        handles: Arc<NodeStatsHandles>,
        expected_rows: u32,
    ) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        self.inner
            .borrow_mut()
            .install_stats_handles(handles, expected_rows)
    }

    #[inline]
    pub(crate) fn stats_error_column_count(&self) -> u32 {
        self.inner.borrow().error_column_count()
    }

    /// Drain scheduled frames and clear topology so `init_graph` can renumber.
    ///
    /// VPP analogue: barrier-held main-thread graph mutation before workers
    /// install a clone of the updated graph.
    /// Old `NodeId` values become unreachable after this returns.
    pub(crate) fn detach_graph_for_rebuild(&self) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
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
            input_main_loops_per_call: Vec::new(),
            error_indices: Vec::new(),
            next_error_index: 1,
            error_tables_installed: Vec::new(),
            stats_handles: None,
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
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> NodeRuntimeInner {
        self.inner.borrow().clone()
    }

    pub(crate) fn replace_graph(&self, graph: NodeRuntimeInner) {
        self.queue.borrow_mut().drain_all();
        self.readiness.clear_pending();
        *self.inner.borrow_mut() = graph;
    }

    pub(crate) fn replace_graph_preserving_worker_state(
        &self,
        mut graph: NodeRuntimeInner,
    ) -> RuntimeResult<()> {
        graph.inherit_worker_state(&self.inner.borrow())?;
        self.replace_graph(graph);
        Ok(())
    }

    pub(crate) fn install_node_function(
        &self,
        node: NodeId,
        simd_bytes: usize,
        registrations: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        let Some(node_name) = self.node_name(node)? else {
            return Ok(());
        };
        let Some(registration) = preferred_node_function(node_name, simd_bytes, registrations)?
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

    pub fn try_register_driver<N>(&self, node: N) -> RuntimeResult<NodeId>
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

    /// Registers a node that runs before all input nodes in the main loop.
    ///
    /// The registration role reuses `DriverNode` metadata; the node kind is
    /// authoritative for VPP-style `PRE_INPUT` ordering.
    pub fn try_register_pre_input<N>(&self, node: N) -> RuntimeResult<NodeId>
    where
        N: DriverNode + Node,
    {
        self.register_descriptor(
            NodeKind::PreInput,
            NodeDescriptor::new(
                node.node_process(),
                node.node_runtime_data()?,
                DriverNode::node_registration(&node),
                DriverNode::node_initial_nexts(&node),
                node.node_trace_formatter(),
            ),
        )
    }

    pub fn register_pre_input<N>(&self, node: N) -> NodeId
    where
        N: DriverNode + Node,
    {
        self.try_register_pre_input(node)
            .expect("register pre-input node descriptor")
    }

    pub fn register_internal<N>(&self, node: N) -> NodeId
    where
        N: InternalNode + Node,
    {
        self.try_register_internal(node)
            .expect("register internal node descriptor")
    }

    pub fn try_register_internal<N>(&self, node: N) -> RuntimeResult<NodeId>
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
    ) -> RuntimeResult<NodeId>
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
    /// For `NodeRegistration::Next`, `initial_nexts` may either contain every
    /// resolved target or be empty while the topology owner wires the declared
    /// slots through `set_node_next_slot` before dispatch.
    #[inline]
    pub fn try_register_descriptor(
        &self,
        kind: NodeKind,
        descriptor: NodeDescriptor<'_>,
    ) -> RuntimeResult<NodeId> {
        self.register_descriptor(kind, descriptor)
    }

    fn register_descriptor(
        &self,
        kind: NodeKind,
        descriptor: NodeDescriptor<'_>,
    ) -> RuntimeResult<NodeId> {
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
    ) -> RuntimeResult<NodeId> {
        self.ensure_topology_owner()?;
        let mut inner = self.inner.borrow_mut();
        if inner.handles.contains_key(&handle) {
            return Err(RuntimeError::NodeHandleAlreadyRegistered { handle });
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
    ) -> RuntimeResult<NodeId> {
        self.ensure_topology_owner()?;
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
    ) -> RuntimeResult<NodeId>
    where
        N: InternalNode + Node,
    {
        self.ensure_topology_owner()?;
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
    ) -> RuntimeResult<NodeId>
    where
        N: DriverNode + Node,
    {
        self.ensure_topology_owner()?;
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

    /// Register a pre-input node whose next edges are resolved by name.
    pub fn try_register_pre_input_with_next_names<N>(
        &self,
        node: N,
        next_names: &[&'static str],
    ) -> RuntimeResult<NodeId>
    where
        N: DriverNode + Node,
    {
        self.ensure_topology_owner()?;
        let mut inner = self.inner.borrow_mut();
        inner.register_function_declared(
            NodeKind::PreInput,
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
    pub fn resolve_named_next_nodes(&self) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        self.inner.borrow_mut().resolve_named_next_nodes()
    }

    #[inline]
    pub fn node_for_handle(&self, handle: NodeHandle) -> RuntimeResult<NodeId> {
        self.inner
            .borrow()
            .handles
            .get(&handle)
            .copied()
            .ok_or(RuntimeError::NodeHandleNotRegistered { handle })
    }

    #[inline]
    pub(crate) fn node_count(&self) -> usize {
        self.inner.borrow().nodes.len()
    }

    #[inline]
    pub fn node_by_name(&self, name: &str) -> Option<NodeId> {
        self.inner.borrow().declared_nodes.get(name).copied()
    }

    #[inline]
    pub fn node_kind(&self, node: NodeId) -> RuntimeResult<NodeKind> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize].kind)
    }

    #[inline]
    pub fn node_state(&self, node: NodeId) -> RuntimeResult<NodeState> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.node_states[node.slot() as usize])
    }

    #[inline]
    pub fn node_runtime_data(&self, node: NodeId) -> RuntimeResult<NodeRuntimeData> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize].runtime_data)
    }

    pub fn polling_driver_nodes(&self) -> RuntimeResult<Vec<NodeId>> {
        self.polling_nodes(NodeKind::Driver)
    }

    pub fn polling_pre_input_nodes(&self) -> RuntimeResult<Vec<NodeId>> {
        self.polling_nodes(NodeKind::PreInput)
    }

    fn polling_nodes(&self, kind: NodeKind) -> RuntimeResult<Vec<NodeId>> {
        let inner = self.inner.borrow();
        let mut nodes = Vec::new();
        let mut slot = 0usize;
        while slot < inner.nodes.len() {
            let node = &inner.nodes[slot];
            if node.kind == kind && inner.node_states[slot] == NodeState::Polling {
                let id = u32::try_from(slot)
                    .map(NodeId::new)
                    .map_err(|_| RuntimeError::NodeIdOverflow { slot })?;
                nodes.push(id);
            }
            slot += 1;
        }
        Ok(nodes)
    }

    pub(crate) fn polling_nodes_to_schedule(&self, kind: NodeKind) -> RuntimeResult<Vec<NodeId>> {
        let mut inner = self.inner.borrow_mut();
        let mut nodes = Vec::new();
        let mut slot = 0usize;
        while slot < inner.nodes.len() {
            if inner.nodes[slot].kind == kind && inner.node_states[slot] == NodeState::Polling {
                let loops = &mut inner.input_main_loops_per_call[slot];
                if *loops > 0 {
                    *loops -= 1;
                } else {
                    let id = u32::try_from(slot)
                        .map(NodeId::new)
                        .map_err(|_| RuntimeError::NodeIdOverflow { slot })?;
                    nodes.push(id);
                }
            }
            slot += 1;
        }
        Ok(nodes)
    }

    pub fn set_input_main_loops_per_call(&self, node: NodeId, count: u32) -> RuntimeResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.input_main_loops_per_call[node.slot() as usize] = count;
        Ok(())
    }

    #[inline]
    pub fn set_node_state(&self, node: NodeId, state: NodeState) -> RuntimeResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.node_states[node.slot() as usize] = state;
        Ok(())
    }

    #[inline]
    pub(crate) fn set_node_runtime_data(
        &self,
        node: NodeId,
        runtime_data: NodeRuntimeData,
    ) -> RuntimeResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.nodes[node.slot() as usize].runtime_data = runtime_data;
        Ok(())
    }

    pub fn mark_interrupt_pending(&self, node: NodeId) -> RuntimeResult<bool> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        let slot = node.slot() as usize;
        if !matches!(
            inner.nodes[slot].kind,
            NodeKind::Driver | NodeKind::PreInput
        ) {
            return Err(RuntimeError::NodeNotDriver { node });
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

    pub(crate) fn next_interrupt_pending_for_kind(
        &self,
        start: usize,
        kind: NodeKind,
    ) -> Option<NodeId> {
        let inner = self.inner.borrow();
        let mut slot = start;
        while slot < inner.interrupt_pending.len() {
            if inner.interrupt_pending[slot] && inner.nodes[slot].kind == kind {
                return Some(NodeId::new(slot as u32));
            }
            slot += 1;
        }
        None
    }

    pub(crate) fn clear_interrupt_pending(&self, node: NodeId) -> RuntimeResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.interrupt_pending[node.slot() as usize] = false;
        Ok(())
    }

    #[inline]
    pub fn node_name(&self, node: NodeId) -> RuntimeResult<Option<&'static str>> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner
            .node_names
            .get(node.slot() as usize)
            .copied()
            .flatten())
    }

    #[inline]
    pub fn node_trace_formatter(&self, node: NodeId) -> RuntimeResult<Option<TraceFormatter>> {
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

    /// Return the preinstalled global index for a node-local error code.
    #[inline]
    pub(crate) fn node_error_index(
        &self,
        node: NodeId,
        code: u16,
    ) -> RuntimeResult<NodeErrorIndex> {
        self.inner.borrow().node_error_index(node, code)
    }

    /// Increment the preinstalled global error counter for a node-local code.
    #[inline]
    pub(crate) fn record_node_error(
        &self,
        thread_index: u32,
        node: NodeId,
        code: u16,
    ) -> RuntimeResult<NodeErrorIndex> {
        let inner = self.inner.borrow();
        let index = inner.node_error_index(node, code)?;
        if let Some(handles) = inner.stats_handles.as_ref() {
            handles.roots[NodeStatsRoot::Errors.index()]
                .increment(thread_index, u32::from(index.get()))
                .map_err(RuntimeError::from)?;
        }
        Ok(index)
    }

    #[inline]
    pub fn node_next<K: NodeNext>(&self, node: NodeId, key: K) -> RuntimeResult<NodeId> {
        self.node_next_slot(node, usize::from(key.slot()))
    }

    pub fn node_next_slot(&self, node: NodeId, slot: usize) -> RuntimeResult<NodeId> {
        let inner = self.inner.borrow();
        inner.node_next_slot(node, slot)
    }

    #[inline]
    pub fn set_node_next<K: NodeNext>(
        &self,
        node: NodeId,
        key: K,
        next: NodeId,
    ) -> RuntimeResult<()> {
        self.set_node_next_slot(node, usize::from(key.slot()), next)
    }

    pub fn set_node_next_slot(&self, node: NodeId, slot: usize, next: NodeId) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.validate_node(next)?;
        inner.set_node_next_slot(node, slot, next)
    }

    pub fn add_node_next_slot(&self, node: NodeId, next: NodeId) -> RuntimeResult<u16> {
        self.ensure_topology_owner()?;
        let mut inner = self.inner.borrow_mut();
        inner.add_node_next_slot(node, next)
    }

    pub fn node_siblings(&self, node: NodeId) -> RuntimeResult<Vec<NodeId>> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        inner
            .siblings
            .get(node.slot() as usize)
            .cloned()
            .ok_or(RuntimeError::NodeNotRegistered { node })
    }

    pub(crate) fn schedule_frame(
        &self,
        node: NodeId,
        frame: Frame<Pending>,
        allow_empty: bool,
    ) -> RuntimeResult<()> {
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

    pub(crate) fn run_ready_function_nodes(
        &self,
        runtime: &DataPlaneRuntime,
    ) -> RuntimeResult<usize> {
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
            let _ = self.record_runtime_stats(node, runtime.thread_index(), vectors, elapsed_ns);
            processed += 1;
            runtime.drop_pending_frame_owned(frame);
        }
        Ok(processed)
    }

    fn record_runtime_stats(
        &self,
        node: NodeId,
        thread_index: u32,
        vectors: usize,
        elapsed_ns: u64,
    ) -> RuntimeResult<()> {
        let inner = self.inner.borrow();
        let node_slot = node.slot();
        let vector_count = u64::try_from(vectors).unwrap_or(u64::MAX);
        if let Some(handles) = inner.stats_handles.as_ref() {
            handles.roots[NodeStatsRoot::Calls.index()]
                .increment(thread_index, node_slot)
                .map_err(RuntimeError::from)?;
            handles.roots[NodeStatsRoot::Vectors.index()]
                .increment_by(thread_index, node_slot, vector_count)
                .map_err(RuntimeError::from)?;
            handles.roots[NodeStatsRoot::Clocks.index()]
                .increment_by(thread_index, node_slot, elapsed_ns)
                .map_err(RuntimeError::from)?;
            // Suspends remain zero until an explicit process-suspend transition
            // exists, matching VPP's process-sync accounting boundary.
        }
        Ok(())
    }

    fn validate_node(&self, node: NodeId) -> RuntimeResult<()> {
        self.inner.borrow().validate_node(node)
    }

    fn runtime_slot(&self, node: NodeId) -> RuntimeResult<NodeRuntimeSlot> {
        let inner = self.inner.borrow();
        inner
            .nodes
            .get(node.slot() as usize)
            .copied()
            .ok_or(RuntimeError::NodeNotRegistered { node })
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
    use crate::DataPlaneBufferConfig;

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

    fn register_error_test_node(runtime: &NodeRuntime, name: &'static str) -> NodeId {
        runtime
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    stats_function_node,
                    NodeRuntimeData::empty(),
                    NodeRegistration::next(name, 0),
                    &[],
                    None,
                ),
            )
            .expect("register error test node")
    }

    #[test]
    fn materialized_node_errors_allocate_contiguous_global_indices() {
        let runtime = NodeRuntime::default();
        let first = register_error_test_node(&runtime, "error-first");
        let second = register_error_test_node(&runtime, "error-second");
        let first_errors = [
            NodeErrorDescriptor::new("first-0", NodeErrorSeverity::Error, "first"),
            NodeErrorDescriptor::new("first-1", NodeErrorSeverity::Warn, "second"),
        ];
        let second_errors = [NodeErrorDescriptor::new(
            "second-0",
            NodeErrorSeverity::Info,
            "third",
        )];

        runtime
            .materialize_node_errors(first, &first_errors)
            .expect("materialize first node errors");
        runtime
            .materialize_node_errors(second, &second_errors)
            .expect("materialize second node errors");

        assert_eq!(runtime.node_error_index(first, 0).unwrap().get(), 1);
        assert_eq!(runtime.node_error_index(first, 1).unwrap().get(), 2);
        let second_index = runtime.node_error_index(second, 0).unwrap();
        assert_eq!(second_index.get(), 3);
    }

    #[test]
    fn node_error_index_zero_is_rejected() {
        assert!(matches!(
            NodeErrorIndex::try_from(0),
            Err(NodeErrorIndexError)
        ));
    }

    #[test]
    fn materialized_node_error_updates_are_bounded() {
        let runtime = NodeRuntime::default();
        let node = register_error_test_node(&runtime, "bounded-errors");
        let descriptors = [NodeErrorDescriptor::new(
            "only-error",
            NodeErrorSeverity::Error,
            "bounded",
        )];
        runtime
            .materialize_node_errors(node, &descriptors)
            .expect("materialize bounded error table");

        let index = runtime
            .record_node_error(0, node, 0)
            .expect("record valid error");
        assert_eq!(index.get(), 1);
        assert!(matches!(
            runtime.record_node_error(0, node, 1),
            Err(RuntimeError::NodeErrorSlotOverflow)
        ));
    }

    #[test]
    fn additive_graph_inheritance_preserves_indices_and_worker_counts() {
        let main = NodeRuntime::default();
        let existing = register_error_test_node(&main, "existing-errors");
        let descriptors = [NodeErrorDescriptor::new(
            "existing-0",
            NodeErrorSeverity::Error,
            "existing",
        )];
        main.materialize_node_errors(existing, &descriptors)
            .expect("materialize existing node errors");
        let worker = NodeRuntime::from(main.snapshot());
        worker
            .record_node_error(0, existing, 0)
            .expect("record worker error");

        let added = register_error_test_node(&main, "added-errors");
        main.materialize_node_errors(added, &descriptors)
            .expect("materialize added node errors");
        worker
            .replace_graph_preserving_worker_state(main.snapshot())
            .expect("inherit additive graph state");

        assert_eq!(worker.node_error_index(existing, 0).unwrap().get(), 1);
        assert_eq!(worker.node_error_index(added, 0).unwrap().get(), 2);
    }

    fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, payload: &[u8]) {
        let buffer = runtime
            .alloc_index_with_bytes(payload)
            .expect("alloc packet");
        frame.push_index(buffer).expect("push packet");
    }
}
