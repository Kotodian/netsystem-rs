use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::error::{RuntimeError, RuntimeResult};
use crate::trace::TraceFormatter;
use crate::{DataPlaneMain, Simd};
use hammer_core::data_plane::{
    Frame, NodeErrorIndex, NodeErrorIndexError, NodeHandle, NodeId, NodeKind, NodeNext,
    NodeRegistration, NodeState,
};
use hammer_core::error::DataPlaneError;

mod frame;
pub mod next;

use frame::NextFrame;

pub use next::default_prefetch_indices;

/// A generated node-local error code that maps to a preinstalled global
/// [`NodeErrorIndex`].
pub trait NodeErrorCode {
    fn local_code(self) -> u16;
}

/// Run packet logic for every u32 in `frame`, record one typed local next
/// decision per u32, then invoke Graph Fanout once.
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
        $node_runtime:expr,
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
        for &packet_index in $frame.vector_args() {
            let next = {
                let $index = packet_index;
                $body
            };
            next_slots[n] = ::hammer_core::data_plane::NodeNext::slot(next);
            n += 1;
        }
        $runtime.enqueue_to_next($node_runtime, $frame, &next_slots[..n]);
        ()
    }};
}

pub trait Node {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(NodeRuntime::empty())
    }

    #[inline]
    fn node_registration(&self) -> Option<NodeRegistration>
    where
        Self: Sized,
    {
        None
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
            process: Self::process,
            runtime_data: self.node_runtime_data()?,
            registration: Node::node_registration(self),
            initial_nexts: self.node_initial_nexts(),
            trace_formatter: self.node_trace_formatter(),
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeRuntime {
    words: [u64; 4],
    cached_next_index: u32,
    flags: u16,
}

impl NodeRuntime {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self::from_words([0; 4])
    }

    #[inline(always)]
    pub const fn from_words(words: [u64; 4]) -> Self {
        Self {
            words,
            cached_next_index: 0,
            flags: 0,
        }
    }

    #[inline]
    pub fn from_usize(value: usize) -> RuntimeResult<Self> {
        Ok(Self::from_words([
            u64::try_from(value).map_err(|_| RuntimeError::NodeRuntimeValueOverflow { value })?,
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
            .map_err(|_| RuntimeError::NodeRuntimeWordOutOfRange { word: index, value })
    }
}

pub type NodeProcessFn = fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame) -> usize;

type NodeFunction = fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame) -> usize;

/// One platform-compiled process-function candidate for an existing Graph Node.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct NodeFunctionRegistration {
    node_name: &'static str,
    simd_bytes: usize,
    function: NodeFunction,
    frame_args_size: (u16, u16, u16),
}

impl NodeFunctionRegistration {
    /// Creates a Node Function registration used by `#[node_function]`.
    ///
    /// The generated private trampoline contains CPU specialization and
    /// terminates Node panics inside the owning plugin artifact.
    #[doc(hidden)]
    pub const fn new<const LANES: usize>(
        node_name: &'static str,
        _: Simd<u8, LANES>,
        function: fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame) -> usize,
        frame_args_size: (u16, u16, u16),
    ) -> Self {
        assert!(matches!(LANES, 1 | 16 | 32 | 64));
        Self {
            node_name,
            simd_bytes: core::mem::size_of::<Simd<u8, LANES>>(),
            function,
            frame_args_size,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NodeDescriptor<'a> {
    process: NodeProcessFn,
    runtime_data: NodeRuntime,
    registration: Option<NodeRegistration>,
    initial_nexts: &'a [NodeId],
    trace_formatter: Option<TraceFormatter>,
}

impl<'a> NodeDescriptor<'a> {
    #[inline]
    pub fn new(
        process: NodeProcessFn,
        runtime_data: NodeRuntime,
        registration: Option<NodeRegistration>,
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
    pub fn runtime_data(self) -> NodeRuntime {
        self.runtime_data
    }

    #[inline]
    pub fn registration(self) -> Option<NodeRegistration> {
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

/// Packet graph node that drives an external input boundary.
///
/// Driver nodes are responsible for bringing packets into the data plane from
/// the operating system or protocol I/O. They are runtime roles, not business
/// protocol roles.
pub trait DriverNode {
    #[inline]
    fn node_registration(&self) -> Option<NodeRegistration>
    where
        Self: Sized,
    {
        None
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
    fn node_registration(&self) -> Option<NodeRegistration>
    where
        Self: Sized,
    {
        None
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
    pub registration: Option<NodeRegistration>,
    pub kind: NodeKind,
    pub init: fn(&DataPlaneMain) -> RuntimeResult<NodeId>,
    pub process: NodeProcessFn,
    pub error_counters: &'static [NodeErrorDescriptor],
}

pub struct NodeMain {
    inner: RefCell<NodeRuntimeInner>,
    pending_frames: RefCell<Vec<PendingFrame>>,
    scheduled_nodes: RefCell<Vec<NodeId>>,
    frames: RefCell<hammer_core::buffer::frame_pool::FramePool>,
    next_frames: Vec<NextFrame>,
    next_frame_indices: Vec<Vec<usize>>,
    enqueue_owners: Vec<Option<usize>>,
    readiness: NodeReadiness,
    topology_owner: bool,
}

impl std::fmt::Debug for NodeMain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.borrow();
        let queue = self.pending_frames.borrow();
        f.debug_struct("NodeMain")
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

pub(crate) struct NodeRuntimeInner {
    nodes: Vec<NodeRuntimeSlot>,
    node_states: Vec<NodeState>,
    interrupt_pending: Vec<bool>,
    input_main_loops_per_call: Vec<u32>,
    error_indices: Vec<Box<[NodeErrorIndex]>>,
    next_error_index: u32,
    error_tables_installed: Vec<bool>,
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
    frame_args_size: (u16, u16, u16),
    runtime_data: Option<NodeRuntime>,
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
    fn dispatch(&mut self, runtime: &mut DataPlaneMain, frame: &mut Frame) -> usize {
        (self.process)(
            runtime,
            self.runtime_data
                .as_mut()
                .expect("Node owns its executing runtime"),
            frame,
        )
    }
}

struct PendingFrame {
    node: NodeId,
    frame: Option<Box<Frame>>,
    next_frame_index: Option<usize>,
}

impl NodeRuntimeInner {
    fn inherit_worker_state(&mut self, current: &Self) {
        assert!(
            self.nodes.len() >= current.nodes.len(),
            "published worker graph must be additive"
        );

        for slot in 0..current.nodes.len() {
            assert_eq!(
                self.node_names[slot], current.node_names[slot],
                "published worker graph changed node identity"
            );
            assert_eq!(
                self.nodes[slot].kind, current.nodes[slot].kind,
                "published worker graph changed node role"
            );
            assert_eq!(
                self.error_indices[slot], current.error_indices[slot],
                "published worker graph changed node error layout"
            );

            // The main thread publishes topology and process functions. The
            // worker retains only state owned by its established node instance.
            self.nodes[slot].runtime_data = current.nodes[slot].runtime_data;
            self.node_states[slot] = current.node_states[slot];
            self.input_main_loops_per_call[slot] = current.input_main_loops_per_call[slot];
        }
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
        // node. u32 zero remains the packet-buffer "no error" sentinel.
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
        runtime_data: NodeRuntime,
    ) -> NodeId {
        self.push_node_slot(NodeRuntimeSlot {
            kind,
            process,
            frame_args_size: (0, 4, 0),
            runtime_data: Some(runtime_data),
        })
    }

    fn register_function_declared(
        &mut self,
        kind: NodeKind,
        process: NodeProcessFn,
        runtime_data: NodeRuntime,
        registration: Option<NodeRegistration>,
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
            let Some(NodeRegistration::Next { next_count, .. }) = registration else {
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
        if registration.is_none() && !initial_nexts.is_empty() {
            return Err(RuntimeError::UnregisteredNodeHasInitialNexts {
                count: initial_nexts.len(),
            });
        }
        if matches!(registration, Some(NodeRegistration::Sibling { .. }))
            && !initial_nexts.is_empty()
        {
            return Err(RuntimeError::SiblingNodeHasInitialNexts {
                count: initial_nexts.len(),
            });
        }
        if next_names.is_none()
            && !initial_nexts.is_empty()
            && let Some(NodeRegistration::Next { next_count, .. }) = registration
            && next_count != initial_nexts.len()
        {
            return Err(RuntimeError::InitialNextCountMismatch {
                declared: next_count,
                actual: initial_nexts.len(),
            });
        }
        if let Some(name) = registration.map(NodeRegistration::name)
            && self.declared_nodes.contains_key(name)
        {
            return Err(RuntimeError::NodeNameAlreadyRegistered { name });
        }

        match registration {
            None => {
                let id = self.push_function_node(kind, process, runtime_data);
                self.node_trace_formatters[id.slot() as usize] = trace_formatter;
                if let Some(handle) = handle {
                    self.handles.insert(handle, id);
                }
                Ok(id)
            }
            Some(NodeRegistration::Next { name, next_count }) => {
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
            Some(NodeRegistration::Sibling { name, sibling_of }) => {
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

    fn add_node_next_slots(
        &mut self,
        edges: &[(NodeId, NodeId)],
        shared_slots: &[std::ops::Range<usize>],
    ) -> RuntimeResult<(Vec<u16>, bool)> {
        for group in shared_slots {
            assert!(
                group.start < group.end && group.end <= edges.len(),
                "shared-slot constraints must name nonempty ranges in the edge batch"
            );
        }
        let validate_slots = |slots: &[u16]| -> RuntimeResult<()> {
            for group in shared_slots {
                let expected = slots[group.start];
                for index in group.clone() {
                    if slots[index] != expected {
                        let (node, next) = edges[index];
                        return Err(RuntimeError::NodeNextSlotMismatch {
                            node,
                            next,
                            actual: slots[index],
                            expected,
                        });
                    }
                }
            }
            Ok(())
        };
        let mut existing_slots = Vec::with_capacity(edges.len());
        for &(node, next) in edges {
            self.validate_node(node)?;
            self.validate_node(next)?;
            existing_slots.push(
                self.next_nodes[node.slot() as usize]
                    .iter()
                    .position(|target| *target == Some(next))
                    .map(|slot| {
                        u16::try_from(slot)
                            .expect("registered graph next slot must fit its u16 representation")
                    }),
            );
        }
        if existing_slots.iter().all(Option::is_some) {
            let slots = existing_slots
                .into_iter()
                .map(|slot| slot.expect("all graph edges were present"))
                .collect::<Vec<_>>();
            validate_slots(&slots)?;
            return Ok((slots, false));
        }

        let original_next_nodes = self.next_nodes.clone();
        let original_pending_next_names = self.pending_next_names.clone();
        let mut slots = Vec::with_capacity(edges.len());
        for &(node, next) in edges {
            let slot = if let Some(slot) = self.next_nodes[node.slot() as usize]
                .iter()
                .position(|target| *target == Some(next))
            {
                u16::try_from(slot)
                    .expect("registered graph next slot must fit its u16 representation")
            } else {
                let slot = self.next_nodes[node.slot() as usize].len();
                let slot = match u16::try_from(slot) {
                    Ok(slot) => slot,
                    Err(_) => {
                        self.next_nodes = original_next_nodes;
                        self.pending_next_names = original_pending_next_names;
                        return Err(RuntimeError::NodeNextCountOverflow { count: slot });
                    }
                };
                let mut group = self.siblings[node.slot() as usize].clone();
                group.push(node);
                for &sibling in &group {
                    let sibling_slot = sibling.slot() as usize;
                    assert_eq!(
                        self.next_nodes[sibling_slot].len(),
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
                slot
            };
            slots.push(slot);
        }
        if let Err(error) = validate_slots(&slots) {
            self.next_nodes = original_next_nodes;
            self.pending_next_names = original_pending_next_names;
            return Err(error);
        }
        let changed = self.next_nodes != original_next_nodes;
        Ok((slots, changed))
    }

    fn node_next_slot_for_target(&self, node: NodeId, next: NodeId) -> RuntimeResult<Option<u16>> {
        self.validate_node(node)?;
        self.validate_node(next)?;
        match self.next_nodes[node.slot() as usize]
            .iter()
            .position(|target| *target == Some(next))
        {
            Some(slot) => Ok(Some(
                u16::try_from(slot)
                    .expect("registered graph next slot must fit its u16 representation"),
            )),
            None => Ok(None),
        }
    }
}

impl Default for NodeMain {
    fn default() -> Self {
        Self {
            inner: RefCell::new(NodeRuntimeInner {
                nodes: Vec::new(),
                node_states: Vec::new(),
                interrupt_pending: Vec::new(),
                input_main_loops_per_call: Vec::new(),
                error_indices: Vec::new(),
                next_error_index: 1,
                error_tables_installed: Vec::new(),
                handles: HashMap::new(),
                declared_nodes: HashMap::new(),
                node_names: Vec::new(),
                node_trace_formatters: Vec::new(),
                next_nodes: Vec::new(),
                pending_next_names: Vec::new(),
                sibling_owners: Vec::new(),
                siblings: Vec::new(),
            }),
            pending_frames: RefCell::new(Vec::with_capacity(32)),
            scheduled_nodes: RefCell::new(Vec::new()),
            frames: RefCell::new(hammer_core::buffer::frame_pool::FramePool::default()),
            next_frames: Vec::new(),
            next_frame_indices: Vec::new(),
            enqueue_owners: Vec::new(),
            readiness: NodeReadiness::default(),
            topology_owner: true,
        }
    }
}

impl From<NodeRuntimeInner> for NodeMain {
    fn from(inner: NodeRuntimeInner) -> Self {
        debug_assert!(
            inner
                .pending_next_names
                .iter()
                .all(|names| names.is_empty()),
            "worker graph must be resolved before installation"
        );
        let (next_frames, next_frame_indices) = Self::next_frames_for_graph(&inner);
        let enqueue_owners = vec![None; inner.nodes.len()];
        Self {
            inner: RefCell::new(inner),
            pending_frames: RefCell::new(Vec::with_capacity(32)),
            scheduled_nodes: RefCell::new(Vec::new()),
            frames: RefCell::new(hammer_core::buffer::frame_pool::FramePool::default()),
            next_frames,
            next_frame_indices,
            enqueue_owners,
            readiness: NodeReadiness::default(),
            topology_owner: false,
        }
    }
}

fn preferred_node_function<'registration>(
    node_name: &str,
    max_simd_bytes: usize,
    registrations: impl Iterator<Item = &'registration NodeFunctionRegistration>,
) -> RuntimeResult<Option<&'registration NodeFunctionRegistration>> {
    let mut selected = None;
    let mut seen_simd_widths = [false; 4];

    for registration in registrations {
        if registration.node_name != node_name {
            continue;
        }
        let width_index = match registration.simd_bytes {
            1 => 0,
            16 => 1,
            32 => 2,
            64 => 3,
            _ => unreachable!("Node Function SIMD width is validated at construction"),
        };
        if std::mem::replace(&mut seen_simd_widths[width_index], true) {
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

impl NodeMain {
    #[inline]
    pub(crate) fn ensure_topology_owner(&self) -> RuntimeResult<()> {
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
    /// Normal graph construction calls this from [`DataPlaneMain::init_graph`]
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

    /// Clear a fully dispatched topology so `init_graph` can renumber it.
    ///
    /// VPP analogue: barrier-held main-thread graph mutation before workers
    /// install a clone of the updated graph.
    /// Old `NodeId` values become unreachable after this returns.
    pub(crate) fn detach_graph_for_rebuild(&mut self) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        assert!(
            self.pending_frames.get_mut().is_empty(),
            "old graph finished Pending dispatch"
        );
        assert!(
            self.scheduled_nodes.get_mut().is_empty(),
            "old graph finished scheduled dispatch"
        );
        for next in &mut self.next_frames {
            if let Some(frame) = next.frame.take() {
                self.frames.get_mut().recycle(frame);
            }
        }
        self.next_frames.clear();
        self.next_frame_indices.clear();
        self.enqueue_owners.clear();
        self.readiness.clear_pending();
        *self.inner.borrow_mut() = NodeRuntimeInner {
            nodes: Vec::new(),
            node_states: Vec::new(),
            interrupt_pending: Vec::new(),
            input_main_loops_per_call: Vec::new(),
            error_indices: Vec::new(),
            next_error_index: 1,
            error_tables_installed: Vec::new(),
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

    pub(crate) fn refork(&mut self, mut graph: NodeRuntimeInner) {
        self.refork_next_frames(&graph);
        graph.inherit_worker_state(self.inner.get_mut());
        *self.inner.get_mut() = graph;
    }

    pub(crate) fn install_node_function<'registration>(
        &self,
        node: NodeId,
        simd_bytes: usize,
        registrations: impl Iterator<Item = &'registration NodeFunctionRegistration>,
        process: NodeProcessFn,
    ) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        {
            let mut inner = self.inner.borrow_mut();
            inner.validate_node(node)?;
            inner.nodes[node.slot() as usize].process = process;
        }
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
        inner.nodes[node.slot() as usize].frame_args_size = registration.frame_args_size;
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
                N::process,
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
                N::process,
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
                N::process,
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
                N::process,
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
        runtime_data: NodeRuntime,
        registration: Option<NodeRegistration>,
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
            N::process,
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
            N::process,
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
            N::process,
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
    pub fn node_runtime_data(&self, node: NodeId) -> RuntimeResult<NodeRuntime> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize]
            .runtime_data
            .expect("Node runtime is borrowed by its invocation"))
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
        runtime_data: NodeRuntime,
    ) -> RuntimeResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        *inner.nodes[node.slot() as usize]
            .runtime_data
            .as_mut()
            .expect("Node runtime is borrowed by its invocation") = runtime_data;
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

    pub fn ready(&self) -> NodeRuntimeReady<'_> {
        NodeRuntimeReady {
            readiness: &self.readiness,
        }
    }

    /// Return the preinstalled global index for a node-local error code.
    #[inline]
    pub(crate) fn record_node_error(
        &self,
        node: NodeId,
        code: u16,
    ) -> RuntimeResult<NodeErrorIndex> {
        let inner = self.inner.borrow();
        inner.node_error_index(node, code)
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
        self.add_node_next_slots(&[(node, next)], &[])
            .map(|mut slots| slots.pop().expect("one edge produces one slot"))
    }

    /// Adds a batch atomically. Each nonempty range in `shared_slots` names
    /// edges that must resolve to the same next slot; rejection changes no
    /// topology and requests no worker refork.
    pub fn add_node_next_slots(
        &self,
        edges: &[(NodeId, NodeId)],
        shared_slots: &[std::ops::Range<usize>],
    ) -> RuntimeResult<Vec<u16>> {
        self.ensure_topology_owner()?;
        let workers_running =
            crate::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running {
            let inner = self.inner.borrow();
            for &(node, next) in edges {
                inner.validate_node(node)?;
                inner.validate_node(next)?;
            }
            let missing_edge = edges.iter().any(|&(node, next)| {
                !inner.next_nodes[node.slot() as usize]
                    .iter()
                    .any(|target| *target == Some(next))
            });
            if missing_edge {
                crate::barrier::__assert_held();
            }
        }
        let mut inner = self.inner.borrow_mut();
        let (slots, _) = inner.add_node_next_slots(edges, shared_slots)?;
        drop(inner);
        Ok(slots)
    }

    /// Returns the existing next slot for a target node without changing graph
    /// topology. Callers use this read-only probe before deciding whether a
    /// worker barrier is needed for a graph-edge insertion.
    pub fn node_next_slot_for_target(
        &self,
        node: NodeId,
        next: NodeId,
    ) -> RuntimeResult<Option<u16>> {
        let inner = self.inner.borrow();
        inner.node_next_slot_for_target(node, next)
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

    pub fn frames_in_use(&self) -> usize {
        self.frames.borrow().in_use()
    }

    pub(crate) fn schedule_node(&self, node: NodeId) -> RuntimeResult<()> {
        self.validate_node(node)?;
        self.scheduled_nodes.borrow_mut().push(node);
        self.readiness.mark_pending();
        Ok(())
    }

    pub(crate) fn schedule_frame(&self, node: NodeId, mut frame: Box<Frame>) -> RuntimeResult<()> {
        self.validate_node(node)?;
        frame.frame_flags |= 1 << 2;
        self.pending_frames.borrow_mut().push(PendingFrame {
            node,
            frame: Some(frame),
            next_frame_index: None,
        });
        self.readiness.mark_pending();
        Ok(())
    }

    fn validate_node(&self, node: NodeId) -> RuntimeResult<()> {
        self.inner.borrow().validate_node(node)
    }

    fn runtime_slot(&self, node: NodeId) -> RuntimeResult<NodeRuntimeSlot> {
        let mut inner = self.inner.borrow_mut();
        let slot = inner
            .nodes
            .get_mut(node.slot() as usize)
            .ok_or(RuntimeError::NodeNotRegistered { node })?;
        Ok(NodeRuntimeSlot {
            kind: slot.kind,
            process: slot.process,
            frame_args_size: slot.frame_args_size,
            runtime_data: Some(
                slot.runtime_data
                    .take()
                    .expect("Node runtime has one active invocation"),
            ),
        })
    }

    pub fn frame_args_size(&self, node: NodeId) -> RuntimeResult<(u16, u16, u16)> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize].frame_args_size)
    }
}

pub struct NodeRuntimeReady<'a> {
    readiness: &'a NodeReadiness,
}

impl Future for NodeRuntimeReady<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.readiness.poll_pending(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PacketInputNode;

    impl PacketInputNode {
        const NODE_NAME: &'static str = "packet-input";
    }

    #[hammer_component_macros::node_function(node = PacketInputNode)]
    fn packet_input(
        _: &mut DataPlaneMain,
        state: &mut NodeRuntime,
        frame: &mut Frame<u64, u32, u16>,
    ) -> usize {
        let calls = state.word(0) + 1;
        *frame.scalar_args_mut().unwrap() = calls;
        frame.set_vector_count(2);
        frame.vector_args_mut().copy_from_slice(&[11, 17]);
        frame.aux_args_mut().unwrap().copy_from_slice(&[3, 5]);
        *state = NodeRuntime::from_words([
            *frame.scalar_args().unwrap(),
            frame
                .vector_args()
                .iter()
                .map(|index| u64::from(*index))
                .sum(),
            frame
                .aux_args()
                .unwrap()
                .iter()
                .map(|next| u64::from(*next))
                .sum(),
            frame.len() as u64,
        ]);
        frame.len()
    }

    // Derived from vlib/main.c frame_alloc_to_node and dispatch_node: the
    // destination layout selects storage, and each invocation mutates its own
    // persistent node runtime. This exercises the actual generated trampoline.
    #[test]
    fn registered_frame_arguments_and_node_state_survive_invocation() {
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_infra::main_heap::init_default().unwrap();
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                3,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut runtime = DataPlaneMain::new(crate::DataPlaneBufferConfig::default());
        let node = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    process,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next(PacketInputNode::NODE_NAME, 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        runtime
            .nodes()
            .install_node_function(
                node,
                1,
                [&__NODE_FUNCTION_PACKET_INPUT_SCALAR].into_iter(),
                process,
            )
            .unwrap();
        assert_eq!(runtime.nodes().frame_args_size(node).unwrap(), (8, 4, 2));
        for calls in 1..=2 {
            runtime.schedule_empty_frame(node).unwrap();
            assert_eq!(runtime.run_ready_nodes().unwrap(), 1);
            assert_eq!(
                runtime.nodes().node_runtime_data(node).unwrap(),
                NodeRuntime::from_words([calls, 28, 8, 2])
            );
            assert_eq!(runtime.nodes().frames_in_use(), 0);
        }
    }

    fn process(_: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        let processed_vectors = frame.len();
        processed_vectors
    }

    #[test]
    fn refork_preserves_existing_worker_state_and_initializes_new_nodes() {
        let main = NodeMain::default();
        let existing = main
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    process,
                    NodeRuntime::from_words([1, 0, 0, 0]),
                    None,
                    &[],
                    None,
                ),
            )
            .expect("register existing node");
        let mut worker = NodeMain::from(main.snapshot());
        let worker_data = NodeRuntime::from_words([9, 8, 7, 6]);
        worker
            .set_node_runtime_data(existing, worker_data)
            .expect("set worker runtime data");
        worker
            .set_node_state(existing, NodeState::Interrupt)
            .expect("set worker node state");
        worker
            .set_input_main_loops_per_call(existing, 4)
            .expect("set worker input cadence");

        let added = main
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    process,
                    NodeRuntime::from_words([2, 3, 4, 5]),
                    None,
                    &[],
                    None,
                ),
            )
            .expect("register added node");
        worker.refork(main.snapshot());

        assert_eq!(worker.node_runtime_data(existing).unwrap(), worker_data);
        assert_eq!(worker.node_state(existing).unwrap(), NodeState::Interrupt);
        assert_eq!(
            worker.inner.borrow().input_main_loops_per_call[existing.slot() as usize],
            4
        );
        assert_eq!(
            worker.node_runtime_data(added).unwrap(),
            NodeRuntime::from_words([2, 3, 4, 5])
        );
        assert_eq!(worker.node_state(added).unwrap(), NodeState::Polling);
    }
}

impl DataPlaneMain {
    /// VPP vlib_get_frame_to_node: allocate this destination's Frame layout.
    pub fn get_frame_to_node(&self, node: NodeId) -> RuntimeResult<Box<Frame>> {
        let (scalar, vector, aux) = self.nodes.frame_args_size(node)?;
        let mut frame = self.nodes.frames.borrow_mut().allocate(scalar, vector, aux);
        frame.frame_flags |= 1 << 3;
        Ok(frame)
    }

    /// VPP vlib_put_frame_to_node: schedule a nonempty directly allocated Frame.
    pub fn put_frame_to_node(&self, node: NodeId, frame: Box<Frame>) -> RuntimeResult<()> {
        self.nodes.validate_node(node)?;
        if frame.is_empty() {
            self.nodes.frames.borrow_mut().recycle(frame);
            return Ok(());
        }
        self.nodes.schedule_frame(node, frame)
    }
}

impl DataPlaneMain {
    pub(crate) fn run_ready_function_nodes(&mut self) -> RuntimeResult<usize> {
        let mut processed = 0usize;
        let mut scheduled_index = 0;
        loop {
            let node = {
                let scheduled = self.nodes.scheduled_nodes.get_mut();
                if scheduled_index == scheduled.len() {
                    scheduled.clear();
                    break;
                }
                scheduled[scheduled_index]
            };
            scheduled_index += 1;
            self.nodes.clear_interrupt_pending(node)?;
            if self.nodes.node_state(node)? == NodeState::Disabled {
                continue;
            }
            let mut frame = self.get_frame_to_node(node)?;
            self.dispatch_node(node, &mut frame)?;
            self.nodes.frames.get_mut().recycle(frame);
            processed += 1;
        }
        let mut pending_index = 0;
        loop {
            let (node, mut frame, next_frame_index) = {
                let pending = self.nodes.pending_frames.get_mut();
                if pending_index == pending.len() {
                    pending.clear();
                    if self.nodes.scheduled_nodes.get_mut().is_empty() {
                        self.nodes.readiness.clear_pending();
                    }
                    break;
                }
                let pending = &mut pending[pending_index];
                (
                    pending.node,
                    pending
                        .frame
                        .take()
                        .expect("Pending Frame is dispatched once"),
                    pending.next_frame_index,
                )
            };
            let restore = next_frame_index.is_some_and(|index| {
                let next = &mut self.nodes.next_frames[index];
                if next.pending_index == Some(pending_index) {
                    next.pending_index = None;
                    next.flags &= !(1 << 1);
                    true
                } else {
                    false
                }
            });
            if self.nodes.node_state(node)? == NodeState::Disabled {
                self.nodes.clear_interrupt_pending(node)?;
                self.nodes.frames.borrow_mut().recycle(frame);
                pending_index += 1;
                continue;
            }
            assert!(!frame.is_empty(), "Pending Frame contains vectors");
            self.nodes.clear_interrupt_pending(node)?;

            // dispatch_pending_node transfers the enqueue trace bit once and
            // clears it for the next frame, including an untraced dispatch.
            let trace = next_frame_index.map_or(frame.frame_flags & (1 << 5), |index| {
                let next = &mut self.nodes.next_frames[index];
                let trace = next.flags & (1 << 5);
                next.flags &= !(1 << 5);
                trace
            });
            {
                let mut graph = self.nodes.inner.borrow_mut();
                let runtime = graph.nodes[node.slot() as usize]
                    .runtime_data
                    .as_mut()
                    .expect("pending node runtime is available before dispatch");
                runtime.flags = (runtime.flags & !(1 << 5)) | trace;
            }
            self.dispatch_node(node, &mut frame)?;
            processed += 1;
            frame.frame_flags &= !((1 << 2) | (1 << 14));
            // The callback may have grown Pending storage or moved the owner.
            let next_index = self.nodes.pending_frames.get_mut()[pending_index].next_frame_index;
            if restore && let Some(index) = next_index {
                let next = &mut self.nodes.next_frames[index];
                if next.frame.is_none() && next.pending_index.is_none() {
                    frame.set_vector_count(0);
                    frame.flags = 0;
                    next.frame = Some(frame);
                    next.flags |= 1 << 1;
                } else {
                    self.nodes.frames.borrow_mut().recycle(frame);
                }
            } else {
                self.nodes.frames.borrow_mut().recycle(frame);
            }
            pending_index += 1;
        }
        Ok(processed)
    }
}

impl DataPlaneMain {
    fn dispatch_node(&mut self, node: NodeId, frame: &mut Frame) -> RuntimeResult<usize> {
        let mut slot = self.nodes.runtime_slot(node)?;
        self.set_current_node(Some(node));
        let count = slot.dispatch(self, frame);
        let state = slot.runtime_data.take().expect("Node returns its runtime");
        let mut inner = self.nodes.inner.borrow_mut();
        assert!(
            inner.nodes[node.slot() as usize]
                .runtime_data
                .replace(state)
                .is_none(),
            "Node runtime has one active invocation"
        );
        drop(inner);
        self.set_current_node(None);
        Ok(count)
    }
}
