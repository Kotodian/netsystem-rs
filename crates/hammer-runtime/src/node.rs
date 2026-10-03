use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use crate::DataPlaneMain;
use crate::error::{RuntimeError, RuntimeResult};
use crate::trace::TraceFormatter;
use hammer_core::data_plane::{
    Frame, NodeErrorIndex, NodeHandle, NodeId, NodeKind, NodeNext, NodeRegistration, NodeState,
};
use hammer_core::error::DataPlaneError;
use hammer_infra::heap::Heap;

mod frame;
pub mod next;

use frame::NextFrame;

pub use next::default_prefetch_indices;

/// A generated node-local error code that maps to a preinstalled global
/// [`NodeErrorIndex`].
pub trait NodeErrorCode {
    fn local_code(self) -> u16;
}

bitflags::bitflags! {
    /// Node accounting flags corresponding to VPP `vlib_node_t::flags`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct NodeFlags: u16 {
        const IS_OUTPUT = 1 << 1;
        const IS_DROP = 1 << 2;
        const IS_PUNT = 1 << 3;
        const IS_HANDOFF = 1 << 4;
    }
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
    fn trace_supported(&self) -> bool {
        false
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
            trace_supported: self.trace_supported(),
            frame_args_size: (0, 4, 0),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeRuntime {
    words: [u64; 4],
    cached_next_index: u32,
    flags: u16,
    node_index: NodeId,
}

impl Default for NodeRuntime {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
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
            node_index: NodeId::new(0),
        }
    }

    #[inline(always)]
    pub const fn node_index(&self) -> NodeId {
        self.node_index
    }

    /// VPP `VLIB_NODE_FLAG_TRACE`: this Frame may contain traced Buffers.
    #[inline(always)]
    pub const fn trace_enabled(&self) -> bool {
        self.flags & (1 << 5) != 0
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

/// Architecture identity of a compiled Node Function candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NodeVariant {
    Base,
    X86_64V3,
    X86_64V4,
}

impl NodeVariant {
    #[inline]
    pub(crate) fn priority_on_current_cpu(self) -> Option<u8> {
        match self {
            Self::Base => Some(0),
            Self::X86_64V3 if x86_64_variant_supported(false) => Some(45),
            Self::X86_64V4 if x86_64_variant_supported(true) => Some(95),
            _ => None,
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn x86_64_variant_supported(v4: bool) -> bool {
    use core::arch::x86_64::{__cpuid, __cpuid_count, _xgetbv};

    // CPUID is read on the executing worker after affinity, not from the
    // process-wide cached feature detector. XGETBV verifies OS vector state.
    let max_leaf = unsafe { __cpuid(0) }.eax;
    let leaf1 = unsafe { __cpuid(1) };
    if max_leaf < 7
        || leaf1.ecx & ((1 << 12) | (1 << 22) | (1 << 26) | (1 << 27) | (1 << 28) | (1 << 29))
            != ((1 << 12) | (1 << 22) | (1 << 26) | (1 << 27) | (1 << 28) | (1 << 29))
    {
        return false;
    }
    let leaf7 = unsafe { __cpuid_count(7, 0) };
    let v3_bits = (1 << 3) | (1 << 5) | (1 << 8);
    if leaf7.ebx & v3_bits != v3_bits {
        return false;
    }
    let extended_max = unsafe { __cpuid(0x8000_0000) }.eax;
    if extended_max < 0x8000_0001 || unsafe { __cpuid(0x8000_0001) }.ecx & (1 << 5) == 0 {
        return false;
    }
    let xcr0 = unsafe { _xgetbv(0) };
    if xcr0 & 0b110 != 0b110 {
        return false;
    }
    if !v4 {
        return true;
    }
    let v4_bits = (1 << 16) | (1 << 17) | (1 << 28) | (1 << 30) | (1 << 31);
    leaf7.ebx & v4_bits == v4_bits && xcr0 & 0b1110_0000 == 0b1110_0000
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn x86_64_variant_supported(_: bool) -> bool {
    false
}

/// One platform-compiled process-function candidate for an existing Graph Node.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct NodeFunctionRegistration {
    node_name: &'static str,
    variant: NodeVariant,
    function: NodeFunction,
    frame_args_size: (u16, u16, u16),
}

impl NodeFunctionRegistration {
    /// Creates a Node Function registration used by `#[node_function]`.
    ///
    /// The generated private trampoline contains CPU specialization and
    /// terminates Node panics inside the owning plugin artifact.
    #[doc(hidden)]
    pub const fn new(
        node_name: &'static str,
        variant: NodeVariant,
        function: fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame) -> usize,
        frame_args_size: (u16, u16, u16),
    ) -> Self {
        Self {
            node_name,
            variant,
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
    trace_supported: bool,
    frame_args_size: (u16, u16, u16),
}

impl<'a> NodeDescriptor<'a> {
    #[inline]
    pub fn new(
        process: NodeProcessFn,
        runtime_data: NodeRuntime,
        registration: Option<NodeRegistration>,
        initial_nexts: &'a [NodeId],
        trace_formatter: Option<TraceFormatter>,
        trace_supported: bool,
    ) -> Self {
        Self {
            process,
            runtime_data,
            registration,
            initial_nexts,
            trace_formatter,
            trace_supported,
            frame_args_size: (0, 4, 0),
        }
    }

    #[inline]
    pub fn with_frame_args<S, V, A>(mut self) -> Self
    where
        S: zerocopy::KnownLayout + zerocopy::FromBytes + zerocopy::Immutable + zerocopy::IntoBytes,
        V: zerocopy::KnownLayout + zerocopy::FromBytes + zerocopy::Immutable + zerocopy::IntoBytes,
        A: zerocopy::KnownLayout + zerocopy::FromBytes + zerocopy::Immutable + zerocopy::IntoBytes,
    {
        const {
            assert!(core::mem::size_of::<V>() != 0);
            assert!(core::mem::align_of::<S>() <= 16);
            assert!(core::mem::align_of::<V>() <= 16);
            assert!(core::mem::align_of::<A>() <= 16);
            assert!(Frame::<S, V, A>::ALLOCATION_SIZE <= u16::MAX as usize);
        }
        self.frame_args_size = (
            u16::try_from(core::mem::size_of::<S>()).expect("Frame scalar size fits u16"),
            u16::try_from(core::mem::size_of::<V>()).expect("Frame vector size fits u16"),
            u16::try_from(core::mem::size_of::<A>()).expect("Frame auxiliary size fits u16"),
        );
        self
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
    #[doc(hidden)]
    pub process_start: Option<ProcessStartFn>,
    #[doc(hidden)]
    pub process_node_index: Option<&'static OnceLock<NodeId>>,
    pub error_counters: &'static [NodeErrorDescriptor],
}

#[doc(hidden)]
pub type ProcessStartFn = fn(&mut DataPlaneMain, NodeId) -> RuntimeResult<()>;

impl NodeEntry {
    #[inline]
    pub fn process_node_index(&self) -> Option<NodeId> {
        self.process_node_index.and_then(OnceLock::get).copied()
    }
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
    pub(crate) process_node_indices: Vec<NodeId>,
    pub(crate) process_restore_current: Vec<(NodeId, u64)>,
    pub(crate) process_restore_next: Vec<(NodeId, u64)>,
    pub(crate) suspended_processes: Vec<NodeId>,
    pub(crate) process_event_state: Vec<Vec<(u64, u64)>>,
    pub(crate) process_timer_state: Vec<(NodeId, Instant)>,
    pub(crate) current_process_index: Option<NodeId>,
    pub(crate) time_next_process_ready: Option<Instant>,
    pub(crate) process_event_senders: Vec<Option<tokio::sync::mpsc::UnboundedSender<(u64, u64)>>>,
    pub(crate) process_runtime: Option<tokio::runtime::Runtime>,
    pub(crate) running_processes: Vec<crate::process::RunningProcess>,
    pub(crate) processes_started: bool,
}

impl std::fmt::Debug for NodeMain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.borrow();
        let queue = self.pending_frames.borrow();
        f.debug_struct("NodeMain")
            .field("nodes_len", &inner.nodes.len())
            .field("queue_len", &queue.len())
            .field("readiness", &self.readiness)
            .field("process_nodes", &self.process_node_indices)
            .field("running_processes", &self.running_processes.len())
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

/// The contiguous global error column range one node owns.
///
/// VPP analogue: `n->error_heap_index` plus `n->n_errors`
/// (`third_party/vpp/src/vlib/node.h:333,342`). `first` is already the global
/// column number (heap offset + 1, skipping the reserved no-error column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeErrorColumnRange {
    first: NodeErrorIndex,
    count: u16,
}

pub(crate) struct NodeRuntimeInner {
    nodes: Arc<Vec<NodeRuntimeSlot>>,
    node_states: Vec<NodeState>,
    interrupt_pending: Vec<bool>,
    input_main_loops_per_call: Vec<u32>,
    /// The node error column space: VPP's `em->counters_heap`
    /// (`third_party/vpp/src/vlib/error.h:38-39`), where a node error column
    /// number is the element's offset plus the reserved column 0. Only thread
    /// zero allocates; a Worker holds the read-only clone it never grows.
    error_column_heap: Heap<NodeId>,
    /// Per node slot: the column range that node owns, `None` before it
    /// registers errors.
    error_columns: Vec<Option<NodeErrorColumnRange>>,
    error_descriptors: Vec<&'static [NodeErrorDescriptor]>,
    handles: HashMap<NodeHandle, NodeId>,
    declared_nodes: HashMap<&'static str, NodeId>,
    node_names: Vec<Option<&'static str>>,
    node_trace_formatters: Vec<Option<TraceFormatter>>,
    next_nodes: Vec<Vec<Option<NodeId>>>,
    n_vectors_by_next_node: Vec<Vec<u64>>,
    pending_next_names: Vec<Vec<Option<&'static str>>>,
    sibling_owners: Vec<Option<NodeId>>,
    siblings: Vec<Vec<NodeId>>,
}

impl Clone for NodeRuntimeInner {
    fn clone(&self) -> Self {
        let node_count = self.nodes.len();
        let mut nodes: Vec<_> = self.nodes.iter().cloned().collect();
        for node in &mut nodes {
            node.calls_since_last_overflow = AtomicU32::new(0);
            node.vectors_since_last_overflow = AtomicU32::new(0);
            node.clocks_since_last_overflow = AtomicU32::new(0);
            node.max_clock = AtomicU32::new(0);
            node.max_clock_n = AtomicU32::new(0);
            node.total_calls = AtomicU64::new(0);
            node.total_vectors = AtomicU64::new(0);
            node.total_clocks = AtomicU64::new(0);
            node.last_clear_calls = AtomicU64::new(0);
            node.last_clear_vectors = AtomicU64::new(0);
            node.last_clear_clocks = AtomicU64::new(0);
        }
        Self {
            nodes: Arc::new(nodes),
            node_states: self.node_states.clone(),
            interrupt_pending: vec![false; node_count],
            input_main_loops_per_call: self.input_main_loops_per_call.clone(),
            error_column_heap: self.error_column_heap.clone(),
            error_columns: self.error_columns.clone(),
            error_descriptors: self.error_descriptors.clone(),
            handles: self.handles.clone(),
            declared_nodes: self.declared_nodes.clone(),
            node_names: self.node_names.clone(),
            node_trace_formatters: self.node_trace_formatters.clone(),
            next_nodes: self.next_nodes.clone(),
            n_vectors_by_next_node: self
                .next_nodes
                .iter()
                .map(|nexts| vec![0; nexts.len()])
                .collect(),
            pending_next_names: vec![Vec::new(); node_count],
            sibling_owners: self.sibling_owners.clone(),
            siblings: self.siblings.clone(),
        }
    }
}

#[repr(C, align(64))]
pub(crate) struct NodeRuntimeSlot {
    kind: NodeKind,
    flags: NodeFlags,
    process: NodeFunction,
    declared_process: NodeFunction,
    frame_args_size: (u16, u16, u16),
    runtime_data: Cell<Option<NodeRuntime>>,
    trace_supported: bool,
    calls_since_last_overflow: AtomicU32,
    vectors_since_last_overflow: AtomicU32,
    clocks_since_last_overflow: AtomicU32,
    max_clock: AtomicU32,
    max_clock_n: AtomicU32,
    total_calls: AtomicU64,
    total_vectors: AtomicU64,
    total_clocks: AtomicU64,
    last_clear_calls: AtomicU64,
    last_clear_vectors: AtomicU64,
    last_clear_clocks: AtomicU64,
}

// SAFETY: only the NodeMain owner accesses runtime_data. Collectors receive
// shared ownership of these same slots and read only atomic counters and
// immutable metadata. This crate-private type is cloned/debugged only by its
// owner or during graph publication, which excludes runtime_data writes.
// Metadata changes replace the array; collectors never clone its elements.
unsafe impl Sync for NodeRuntimeSlot {}

impl Clone for NodeRuntimeSlot {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            flags: self.flags,
            process: self.process,
            declared_process: self.declared_process,
            frame_args_size: self.frame_args_size,
            runtime_data: Cell::new(self.runtime_data.get()),
            trace_supported: self.trace_supported,
            calls_since_last_overflow: AtomicU32::new(
                self.calls_since_last_overflow.load(Ordering::Relaxed),
            ),
            vectors_since_last_overflow: AtomicU32::new(
                self.vectors_since_last_overflow.load(Ordering::Relaxed),
            ),
            clocks_since_last_overflow: AtomicU32::new(
                self.clocks_since_last_overflow.load(Ordering::Relaxed),
            ),
            max_clock: AtomicU32::new(self.max_clock.load(Ordering::Relaxed)),
            max_clock_n: AtomicU32::new(self.max_clock_n.load(Ordering::Relaxed)),
            total_calls: AtomicU64::new(self.total_calls.load(Ordering::Relaxed)),
            total_vectors: AtomicU64::new(self.total_vectors.load(Ordering::Relaxed)),
            total_clocks: AtomicU64::new(self.total_clocks.load(Ordering::Relaxed)),
            last_clear_calls: AtomicU64::new(self.last_clear_calls.load(Ordering::Relaxed)),
            last_clear_vectors: AtomicU64::new(self.last_clear_vectors.load(Ordering::Relaxed)),
            last_clear_clocks: AtomicU64::new(self.last_clear_clocks.load(Ordering::Relaxed)),
        }
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
    fn sync_stats(&self, calls: u64, vectors: u64, clocks: u64) {
        self.total_calls.store(
            self.total_calls
                .load(Ordering::Relaxed)
                .wrapping_add(calls.wrapping_add(u64::from(
                    self.calls_since_last_overflow.load(Ordering::Relaxed),
                ))),
            Ordering::Relaxed,
        );
        self.total_vectors.store(
            self.total_vectors
                .load(Ordering::Relaxed)
                .wrapping_add(vectors.wrapping_add(u64::from(
                    self.vectors_since_last_overflow.load(Ordering::Relaxed),
                ))),
            Ordering::Relaxed,
        );
        self.total_clocks.store(
            self.total_clocks
                .load(Ordering::Relaxed)
                .wrapping_add(clocks.wrapping_add(u64::from(
                    self.clocks_since_last_overflow.load(Ordering::Relaxed),
                ))),
            Ordering::Relaxed,
        );
        self.calls_since_last_overflow.store(0, Ordering::Relaxed);
        self.vectors_since_last_overflow.store(0, Ordering::Relaxed);
        self.clocks_since_last_overflow.store(0, Ordering::Relaxed);
    }

    #[inline(always)]
    fn update_dispatch(&self, vectors: u64, clocks: u64) {
        let old_calls = self.calls_since_last_overflow.load(Ordering::Relaxed);
        let old_vectors = self.vectors_since_last_overflow.load(Ordering::Relaxed);
        let old_clocks = self.clocks_since_last_overflow.load(Ordering::Relaxed);
        let calls = old_calls.wrapping_add(1);
        let next_vectors = old_vectors.wrapping_add(vectors as u32);
        let next_clocks = old_clocks.wrapping_add(clocks as u32);
        self.calls_since_last_overflow
            .store(calls, Ordering::Relaxed);
        self.vectors_since_last_overflow
            .store(next_vectors, Ordering::Relaxed);
        self.clocks_since_last_overflow
            .store(next_clocks, Ordering::Relaxed);
        if self.max_clock.load(Ordering::Relaxed) <= clocks as u32 {
            self.max_clock.store(clocks as u32, Ordering::Relaxed);
            self.max_clock_n.store(vectors as u32, Ordering::Relaxed);
        }
        if crate::unlikely(
            calls < old_calls || next_vectors < old_vectors || next_clocks < old_clocks,
        ) {
            self.calls_since_last_overflow
                .store(old_calls, Ordering::Relaxed);
            self.vectors_since_last_overflow
                .store(old_vectors, Ordering::Relaxed);
            self.clocks_since_last_overflow
                .store(old_clocks, Ordering::Relaxed);
            self.sync_stats(1, vectors, clocks);
        }
    }

    #[inline]
    pub(crate) fn counter_values(&self) -> (u64, u64, u64) {
        (
            self.total_calls
                .load(Ordering::Relaxed)
                .wrapping_add(u64::from(
                    self.calls_since_last_overflow.load(Ordering::Relaxed),
                ))
                .wrapping_sub(self.last_clear_calls.load(Ordering::Relaxed)),
            self.total_vectors
                .load(Ordering::Relaxed)
                .wrapping_add(u64::from(
                    self.vectors_since_last_overflow.load(Ordering::Relaxed),
                ))
                .wrapping_sub(self.last_clear_vectors.load(Ordering::Relaxed)),
            self.total_clocks
                .load(Ordering::Relaxed)
                .wrapping_add(u64::from(
                    self.clocks_since_last_overflow.load(Ordering::Relaxed),
                ))
                .wrapping_sub(self.last_clear_clocks.load(Ordering::Relaxed)),
        )
    }

    pub(crate) fn is_process(&self) -> bool {
        self.kind == NodeKind::Process
    }
}

struct PendingFrame {
    node: NodeId,
    frame: Option<Box<Frame>>,
    next_frame_index: Option<usize>,
}

impl NodeRuntimeInner {
    pub(crate) fn node_slots(&self) -> Arc<Vec<NodeRuntimeSlot>> {
        Arc::clone(&self.nodes)
    }
    pub(crate) fn node_names(&self) -> &[Option<&'static str>] {
        &self.node_names
    }

    fn inherit_worker_state(&mut self, current: &Self) {
        assert!(
            self.nodes.len() >= current.nodes.len(),
            "published worker graph must be additive"
        );

        for slot in 0..current.nodes.len() {
            let recycled = self.node_names[slot] != current.node_names[slot];
            assert_eq!(
                self.nodes[slot].kind, current.nodes[slot].kind,
                "published worker graph changed node role"
            );
            assert_eq!(
                self.error_columns[slot], current.error_columns[slot],
                "published worker graph changed node error layout"
            );
            assert_eq!(
                self.error_descriptors[slot], current.error_descriptors[slot],
                "published worker graph changed node error descriptors"
            );

            // A renamed node is a deliberately recycled identity and keeps
            // the runtime published by the topology owner. An unchanged node
            // retains state owned by its established worker-local instance.
            if !recycled {
                let target = &mut Arc::make_mut(&mut self.nodes)[slot];
                let source = &current.nodes[slot];
                let process = target.process;
                let declared_process = target.declared_process;
                let flags = target.flags;
                let frame_args_size = target.frame_args_size;
                let trace_supported = target.trace_supported;
                *target = source.clone();
                target.process = process;
                target.declared_process = declared_process;
                target.flags = flags;
                target.frame_args_size = frame_args_size;
                target.trace_supported = trace_supported;
                self.n_vectors_by_next_node[slot] = current.n_vectors_by_next_node[slot].clone();
                self.n_vectors_by_next_node[slot].resize(self.next_nodes[slot].len(), 0);
            }
            self.node_states[slot] = current.node_states[slot];
            self.input_main_loops_per_call[slot] = current.input_main_loops_per_call[slot];
        }
    }

    /// Reserves this node's contiguous global error column range.
    ///
    /// VPP analogue: `vlib_register_errors`
    /// (`third_party/vpp/src/vlib/error.c:138-139`), which allocates the range
    /// from `em->counters_heap` and stores its offset in `n->error_heap_index`.
    /// Column 0 stays reserved for the packet-buffer "no error" sentinel, so
    /// the first column of the range is its heap offset plus one.
    fn register_node_errors(
        &mut self,
        node: NodeId,
        descriptors: &'static [NodeErrorDescriptor],
    ) -> RuntimeResult<()> {
        self.validate_node(node)?;
        if descriptors.is_empty() {
            return Ok(());
        }
        let slot = node.slot() as usize;
        if self.error_columns[slot].is_some() {
            return Ok(());
        }

        let count =
            u16::try_from(descriptors.len()).map_err(|_| RuntimeError::NodeErrorSlotOverflow)?;
        let end = self
            .error_column_heap
            .len()
            .checked_add(u32::from(count))
            .ok_or(RuntimeError::NodeErrorSlotOverflow)?;
        if end > u32::from(u16::MAX) {
            return Err(RuntimeError::NodeErrorSlotOverflow);
        }

        let offset = self.error_column_heap.alloc(u32::from(count), node);
        let first_column = u16::try_from(offset + 1)
            .expect("the checked column capacity keeps every column in the u16 space");
        let first =
            NodeErrorIndex::new(first_column).expect("column 0 is the reserved no-error slot");
        self.error_columns[slot] = Some(NodeErrorColumnRange { first, count });
        self.error_descriptors[slot] = descriptors;
        Ok(())
    }

    fn validate_node_error_batch(
        &self,
        nodes: &[(NodeId, &'static [NodeErrorDescriptor])],
    ) -> RuntimeResult<()> {
        let mut end = self.error_column_heap.len();
        for &(node, descriptors) in nodes {
            self.validate_node(node)?;
            let slot = node.slot() as usize;
            if descriptors.is_empty() || self.error_columns[slot].is_some() {
                continue;
            }
            let count = u32::try_from(descriptors.len())
                .map_err(|_| RuntimeError::NodeErrorSlotOverflow)?;
            end = end
                .checked_add(count)
                .ok_or(RuntimeError::NodeErrorSlotOverflow)?;
            if end > u32::from(u16::MAX) {
                return Err(RuntimeError::NodeErrorSlotOverflow);
            }
        }
        Ok(())
    }

    #[inline]
    fn node_error_index(&self, node: NodeId, code: u16) -> RuntimeResult<NodeErrorIndex> {
        self.validate_node(node)?;
        let range =
            self.error_columns[node.slot() as usize].ok_or(RuntimeError::NodeErrorSlotOverflow)?;
        if code >= range.count {
            return Err(RuntimeError::NodeErrorSlotOverflow);
        }
        let column = range
            .first
            .get()
            .checked_add(code)
            .ok_or(RuntimeError::NodeErrorSlotOverflow)?;
        Ok(NodeErrorIndex::new(column).expect("a registered column is non-zero"))
    }

    fn push_node_slot(&mut self, mut slot: NodeRuntimeSlot) -> NodeId {
        let id = NodeId::new(u32::try_from(self.nodes.len()).expect("node index fits u32"));
        if let Some(runtime_data) = slot.runtime_data.get_mut() {
            runtime_data.node_index = id;
        }
        Arc::make_mut(&mut self.nodes).push(slot);
        self.node_states.push(NodeState::Polling);
        self.interrupt_pending.push(false);
        self.input_main_loops_per_call.push(0);
        self.error_columns.push(None);
        self.error_descriptors.push(&[]);
        self.node_names.push(None);
        self.node_trace_formatters.push(None);
        self.next_nodes.push(Vec::new());
        self.n_vectors_by_next_node.push(Vec::new());
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
        trace_supported: bool,
    ) -> NodeId {
        self.push_node_slot(NodeRuntimeSlot {
            kind,
            flags: NodeFlags::empty(),
            process,
            declared_process: process,
            frame_args_size: (0, 4, 0),
            runtime_data: Cell::new(Some(runtime_data)),
            trace_supported,
            calls_since_last_overflow: AtomicU32::new(0),
            vectors_since_last_overflow: AtomicU32::new(0),
            clocks_since_last_overflow: AtomicU32::new(0),
            max_clock: AtomicU32::new(0),
            max_clock_n: AtomicU32::new(0),
            total_calls: AtomicU64::new(0),
            total_vectors: AtomicU64::new(0),
            total_clocks: AtomicU64::new(0),
            last_clear_calls: AtomicU64::new(0),
            last_clear_vectors: AtomicU64::new(0),
            last_clear_clocks: AtomicU64::new(0),
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
        trace_supported: bool,
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
                let id = self.push_function_node(kind, process, runtime_data, trace_supported);
                self.node_trace_formatters[id.slot() as usize] = trace_formatter;
                if let Some(handle) = handle {
                    self.handles.insert(handle, id);
                }
                Ok(id)
            }
            Some(NodeRegistration::Next { name, next_count }) => {
                let id = self.push_function_node(kind, process, runtime_data, trace_supported);
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
                self.n_vectors_by_next_node[id.slot() as usize] = vec![0; next_count];
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
                let id = self.push_function_node(kind, process, runtime_data, trace_supported);
                self.node_names[id.slot() as usize] = Some(name);
                self.node_trace_formatters[id.slot() as usize] = trace_formatter;
                self.next_nodes[id.slot() as usize] = owner_nexts;
                self.n_vectors_by_next_node[id.slot() as usize] =
                    vec![0; self.next_nodes[id.slot() as usize].len()];
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

    fn set_node_next_slot(
        &mut self,
        node: NodeId,
        slot: usize,
        next: NodeId,
    ) -> RuntimeResult<bool> {
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
        let changed = group
            .iter()
            .any(|sibling| self.next_nodes[sibling.slot() as usize][slot] != Some(next));
        for sibling in group {
            let sibling_slot = sibling.slot() as usize;
            if !self.pending_next_names[sibling_slot].is_empty() {
                self.pending_next_names[sibling_slot][slot] = None;
            }
            self.next_nodes[sibling_slot][slot] = Some(next);
        }
        Ok(changed)
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
        if changed {
            for (nexts, totals) in self.next_nodes.iter().zip(&mut self.n_vectors_by_next_node) {
                totals.resize(nexts.len(), 0);
            }
        }
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
                nodes: Arc::new(Vec::new()),
                node_states: Vec::new(),
                interrupt_pending: Vec::new(),
                input_main_loops_per_call: Vec::new(),
                error_column_heap: Heap::new(),
                error_columns: Vec::new(),
                error_descriptors: Vec::new(),
                handles: HashMap::new(),
                declared_nodes: HashMap::new(),
                node_names: Vec::new(),
                node_trace_formatters: Vec::new(),
                next_nodes: Vec::new(),
                n_vectors_by_next_node: Vec::new(),
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
            process_node_indices: Vec::new(),
            process_restore_current: Vec::new(),
            process_restore_next: Vec::new(),
            suspended_processes: Vec::new(),
            process_event_state: Vec::new(),
            process_timer_state: Vec::new(),
            current_process_index: None,
            time_next_process_ready: None,
            process_event_senders: Vec::new(),
            process_runtime: None,
            running_processes: Vec::new(),
            processes_started: false,
        }
    }
}

impl Clone for NodeMain {
    fn clone(&self) -> Self {
        Self::from(self.inner.borrow().clone())
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
            process_node_indices: Vec::new(),
            process_restore_current: Vec::new(),
            process_restore_next: Vec::new(),
            suspended_processes: Vec::new(),
            process_event_state: Vec::new(),
            process_timer_state: Vec::new(),
            current_process_index: None,
            time_next_process_ready: None,
            process_event_senders: Vec::new(),
            process_runtime: None,
            running_processes: Vec::new(),
            processes_started: false,
        }
    }
}

pub(crate) fn preferred_node_function<'registration>(
    node_name: &str,
    allow_specialized: bool,
    registrations: impl Iterator<Item = &'registration NodeFunctionRegistration>,
) -> RuntimeResult<Option<&'registration NodeFunctionRegistration>> {
    let mut selected = None;
    let mut seen_variants = [false; 3];
    let mut layout = None;

    for registration in registrations {
        if registration.node_name != node_name {
            continue;
        }
        let variant_index = match registration.variant {
            NodeVariant::Base => 0,
            NodeVariant::X86_64V3 => 1,
            NodeVariant::X86_64V4 => 2,
        };
        if std::mem::replace(&mut seen_variants[variant_index], true) {
            return Err(RuntimeError::DuplicateNodeFunction {
                node: registration.node_name,
                variant: registration.variant,
            });
        }
        if let Some(existing) = layout {
            assert_eq!(
                existing, registration.frame_args_size,
                "Node Function variants for {node_name} disagree on Frame layout"
            );
        } else {
            layout = Some(registration.frame_args_size);
        }
        if !allow_specialized && registration.variant != NodeVariant::Base {
            continue;
        }
        let Some(priority) = registration.variant.priority_on_current_cpu() else {
            continue;
        };
        if selected.is_none_or(|current: &NodeFunctionRegistration| {
            priority
                > current
                    .variant
                    .priority_on_current_cpu()
                    .expect("selected candidate is supported")
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

    /// Reserve a node's ordered error column range on the topology owner.
    ///
    /// VPP analogue: the `heap_alloc (em->counters_heap, n_errors, …)` half of
    /// `vlib_register_errors`; the caller that also publishes the stats family
    /// is [`DataPlaneMain::register_node_errors`].
    pub(crate) fn register_node_errors(
        &self,
        node: NodeId,
        descriptors: &'static [NodeErrorDescriptor],
    ) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        self.inner
            .borrow_mut()
            .register_node_errors(node, descriptors)
    }

    /// The published global error column width: zero while no node declared
    /// errors, otherwise the element space length plus the reserved column 0
    /// (VPP `l = vec_len (em->counters_heap)`, error.c:140).
    pub(crate) fn node_error_columns(&self) -> u32 {
        let inner = self.inner.borrow();
        if inner.error_column_heap.is_empty() {
            0
        } else {
            inner.error_column_heap.len() + 1
        }
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
            nodes: Arc::new(Vec::new()),
            node_states: Vec::new(),
            interrupt_pending: Vec::new(),
            input_main_loops_per_call: Vec::new(),
            error_column_heap: Heap::new(),
            error_columns: Vec::new(),
            error_descriptors: Vec::new(),
            handles: HashMap::new(),
            declared_nodes: HashMap::new(),
            node_names: Vec::new(),
            node_trace_formatters: Vec::new(),
            next_nodes: Vec::new(),
            n_vectors_by_next_node: Vec::new(),
            pending_next_names: Vec::new(),
            sibling_owners: Vec::new(),
            siblings: Vec::new(),
        };
        Ok(())
    }

    pub(crate) fn refork(&mut self, mut graph: NodeRuntimeInner) {
        for slot in 0..self.inner.get_mut().nodes.len() {
            self.sync_node_stats(NodeId::new(slot as u32))
                .expect("refork synchronizes an existing Node");
        }
        self.refork_next_frames(&graph);
        graph.inherit_worker_state(self.inner.get_mut());
        *self.inner.get_mut() = graph;
    }

    pub(crate) fn install_node_function<'registration>(
        &self,
        node: NodeId,
        registrations: impl Iterator<Item = &'registration NodeFunctionRegistration>,
        process: NodeProcessFn,
    ) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        let registration = match self.node_name(node)? {
            Some(node_name) => preferred_node_function(node_name, false, registrations)?,
            None => None,
        };
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        let slot = &mut Arc::make_mut(&mut inner.nodes)[node.slot() as usize];
        slot.declared_process = process;
        slot.process = registration.map_or(process, |candidate| candidate.function);
        if let Some(candidate) = registration {
            slot.frame_args_size = candidate.frame_args_size;
        }
        Ok(())
    }

    pub(crate) fn select_node_functions<'registration>(
        &self,
        registrations: impl Clone + Iterator<Item = &'registration NodeFunctionRegistration>,
        cpu_pinned: bool,
    ) -> RuntimeResult<()> {
        let mut inner = self.inner.borrow_mut();
        let selected = inner
            .node_names
            .iter()
            .map(|name| {
                name.map(|name| preferred_node_function(name, cpu_pinned, registrations.clone()))
                    .transpose()
                    .map(Option::flatten)
            })
            .collect::<RuntimeResult<Vec<_>>>()?;
        for (slot, registration) in Arc::make_mut(&mut inner.nodes).iter_mut().zip(selected) {
            slot.process =
                registration.map_or(slot.declared_process, |candidate| candidate.function);
            if let Some(candidate) = registration {
                slot.frame_args_size = candidate.frame_args_size;
            }
        }
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
                node.trace_supported(),
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
                node.trace_supported(),
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
                node.trace_supported(),
            ),
        )
    }

    #[doc(hidden)]
    pub fn try_register_process_node(
        &self,
        name: &'static str,
        process: NodeProcessFn,
    ) -> RuntimeResult<NodeId> {
        let node = self.register_descriptor(
            NodeKind::Process,
            NodeDescriptor::new(
                process,
                NodeRuntime::empty(),
                Some(NodeRegistration::next(name, 0)),
                &[],
                None,
                false,
            ),
        )?;
        self.set_node_state(node, NodeState::Disabled)?;
        Ok(node)
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
                node.trace_supported(),
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

    pub fn recycle_node_descriptor(
        &self,
        node: NodeId,
        descriptor: NodeDescriptor<'_>,
    ) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        let workers_running =
            crate::WorkerThread::main().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running {
            crate::WorkerThread::__assert_held();
        }
        let Some(NodeRegistration::Next { name, next_count }) = descriptor.registration else {
            return Err(RuntimeError::NamedNextRegistrationKindInvalid);
        };
        if !descriptor.initial_nexts.is_empty() {
            return Err(RuntimeError::InitialNextCountMismatch {
                declared: 0,
                actual: descriptor.initial_nexts.len(),
            });
        }

        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        let slot = node.slot() as usize;
        if inner.next_nodes[slot].len() != next_count {
            return Err(RuntimeError::InitialNextCountMismatch {
                declared: inner.next_nodes[slot].len(),
                actual: next_count,
            });
        }
        if let Some(existing) = inner.declared_nodes.get(name)
            && *existing != node
        {
            return Err(RuntimeError::NodeNameAlreadyRegistered { name });
        }
        if let Some(old_name) = inner.node_names[slot]
            && old_name != name
        {
            inner.declared_nodes.remove(old_name);
        }
        inner.declared_nodes.insert(name, node);
        inner.node_names[slot] = Some(name);
        inner.node_trace_formatters[slot] = descriptor.trace_formatter;
        let runtime = &mut Arc::make_mut(&mut inner.nodes)[slot];
        runtime.process = descriptor.process;
        runtime.declared_process = descriptor.process;
        let mut runtime_data = descriptor.runtime_data;
        runtime_data.node_index = node;
        runtime.runtime_data.set(Some(runtime_data));
        runtime.trace_supported = descriptor.trace_supported;
        runtime.flags = NodeFlags::empty();
        runtime.frame_args_size = descriptor.frame_args_size;
        runtime
            .calls_since_last_overflow
            .store(0, Ordering::Relaxed);
        runtime
            .vectors_since_last_overflow
            .store(0, Ordering::Relaxed);
        runtime
            .clocks_since_last_overflow
            .store(0, Ordering::Relaxed);
        runtime.total_calls.store(0, Ordering::Relaxed);
        runtime.total_vectors.store(0, Ordering::Relaxed);
        runtime.total_clocks.store(0, Ordering::Relaxed);
        runtime.last_clear_calls.store(0, Ordering::Relaxed);
        runtime.last_clear_vectors.store(0, Ordering::Relaxed);
        runtime.last_clear_clocks.store(0, Ordering::Relaxed);
        runtime.max_clock.store(0, Ordering::Relaxed);
        runtime.max_clock_n.store(0, Ordering::Relaxed);
        drop(inner);

        if let Some(barrier) =
            crate::WorkerThread::main().filter(|barrier| barrier.worker_count() != 0)
        {
            barrier.request_node_refork(&self.inner.borrow());
        }
        Ok(())
    }

    fn register_descriptor(
        &self,
        kind: NodeKind,
        descriptor: NodeDescriptor<'_>,
    ) -> RuntimeResult<NodeId> {
        let barrier = crate::WorkerThread::main().filter(|barrier| barrier.worker_count() != 0);
        if barrier.is_some() {
            crate::WorkerThread::__assert_held();
        }
        let node = self.register_function_declared(
            kind,
            descriptor.process,
            descriptor.runtime_data,
            descriptor.registration,
            descriptor.initial_nexts,
            descriptor.trace_formatter,
            descriptor.trace_supported,
        )?;
        Arc::make_mut(&mut self.inner.borrow_mut().nodes)[node.slot() as usize].frame_args_size =
            descriptor.frame_args_size;
        if let Some(barrier) = barrier {
            barrier.request_node_refork(&self.inner.borrow());
        }
        Ok(node)
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
            descriptor.trace_supported,
            Some(handle),
            None,
        )?;
        Arc::make_mut(&mut inner.nodes)[id.slot() as usize].frame_args_size =
            descriptor.frame_args_size;
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
        trace_supported: bool,
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
            trace_supported,
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
            node.trace_supported(),
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
            node.trace_supported(),
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
            node.trace_supported(),
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

    /// Synchronizes this thread's Node Runtime and next-frame pending counts.
    pub fn sync_node_stats(&mut self, node: NodeId) -> RuntimeResult<()> {
        let slot = node.slot() as usize;
        let graph = self.inner.get_mut();
        graph.validate_node(node)?;
        graph.nodes[slot].sync_stats(0, 0, 0);
        let totals = &mut graph.n_vectors_by_next_node[slot];
        totals.resize(graph.next_nodes[slot].len(), 0);
        if let Some(indices) = self.next_frame_indices.get(slot) {
            for (arc, &next_frame_index) in indices.iter().enumerate() {
                if next_frame_index == usize::MAX {
                    continue;
                }
                let pending = &mut self.next_frames[next_frame_index].vectors_since_last_overflow;
                totals[arc] = totals[arc].wrapping_add(u64::from(*pending));
                *pending = 0;
            }
        }
        Ok(())
    }

    /// VPP `clear_node_runtime`: synchronize every Node before moving its
    /// clear baseline, including pending next-frame vectors.
    pub(crate) fn clear_runtime_stats(&mut self) {
        let node_count = self.inner.get_mut().nodes.len();
        for slot in 0..node_count {
            let node = NodeId::new(slot as u32);
            self.sync_node_stats(node)
                .expect("clear runtime visits registered Node slots");
            let graph = self.inner.get_mut();
            let runtime = &graph.nodes[slot];
            runtime.last_clear_calls.store(
                runtime.total_calls.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            runtime.last_clear_vectors.store(
                runtime.total_vectors.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            runtime.last_clear_clocks.store(
                runtime.total_clocks.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            runtime.max_clock.store(0, Ordering::Relaxed);

            if runtime.kind == NodeKind::Process && self.topology_owner {
                let stats = hammer_stats::StatsMain::global()
                    .expect("Process runtime has initialized stats");
                if stats.segment.node_counters_enabled() {
                    let counters = crate::node_stats::NodeStats::global();
                    let column = node.slot();
                    stats
                        .segment
                        .set_simple_counter(counters.calls.index, 0, column, 0);
                    stats
                        .segment
                        .set_simple_counter(counters.vectors.index, 0, column, 0);
                    stats
                        .segment
                        .set_simple_counter(counters.clocks.index, 0, column, 0);
                    stats
                        .segment
                        .set_simple_counter(counters.suspends.index, 0, column, 0);
                }
            }
        }
    }

    pub fn node_stats(&self, node: NodeId) -> RuntimeResult<(u64, u64, u64, u64, u32, u32)> {
        let graph = self.inner.borrow();
        graph.validate_node(node)?;
        let slot = &graph.nodes[node.slot() as usize];
        if slot.kind == NodeKind::Process {
            let stats =
                hammer_stats::StatsMain::global().expect("Process runtime has initialized stats");
            assert!(
                stats.segment.node_counters_enabled(),
                "Process counters are read only when per-node counters are enabled"
            );
            let counters = crate::node_stats::NodeStats::global();
            let column = node.slot();
            return Ok((
                stats
                    .segment
                    .simple_counter(counters.calls.index, 0, column),
                0,
                stats
                    .segment
                    .simple_counter(counters.clocks.index, 0, column),
                stats
                    .segment
                    .simple_counter(counters.suspends.index, 0, column),
                0,
                0,
            ));
        }
        let (calls, vectors, clocks) = slot.counter_values();
        Ok((
            calls,
            vectors,
            clocks,
            0,
            slot.max_clock.load(Ordering::Relaxed),
            slot.max_clock_n.load(Ordering::Relaxed),
        ))
    }

    /// VPP `input_node_counts_by_state[VLIB_NODE_STATE_POLLING]`: polling
    /// input work keeps File polling nonblocking.
    pub(crate) fn has_polling_input_nodes(&self) -> bool {
        let inner = self.inner.borrow();
        inner.nodes.iter().enumerate().any(|(slot, node)| {
            matches!(node.kind, NodeKind::Driver | NodeKind::PreInput)
                && inner.node_states[slot] == NodeState::Polling
        })
    }

    pub(crate) fn has_pending_work(&self) -> bool {
        !self.pending_frames.borrow().is_empty()
            || !self.scheduled_nodes.borrow().is_empty()
            || self.readiness.pending.get()
            || self
                .inner
                .borrow()
                .interrupt_pending
                .iter()
                .any(|pending| *pending)
    }

    #[inline]
    pub fn node_count(&self) -> usize {
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
    pub fn node_flags(&self, node: NodeId) -> RuntimeResult<NodeFlags> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize].flags)
    }

    /// Add VPP-style Node statistics flags during graph publication.
    pub fn set_node_flags(&self, node: NodeId, flags: NodeFlags) -> RuntimeResult<()> {
        self.ensure_topology_owner()?;
        let barrier = crate::WorkerThread::main().filter(|barrier| barrier.worker_count() != 0);
        if barrier.is_some() {
            crate::WorkerThread::__assert_held();
        }
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        Arc::make_mut(&mut inner.nodes)[node.slot() as usize].flags |= flags;
        drop(inner);
        if let Some(barrier) = barrier {
            barrier.request_node_refork(&self.inner.borrow());
        }
        Ok(())
    }

    #[inline]
    pub fn node_state(&self, node: NodeId) -> RuntimeResult<NodeState> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.node_states[node.slot() as usize])
    }

    /// VPP `format_vlib_node_state` for the states Hammer's scheduler owns.
    pub fn node_display_state(&self, node: NodeId) -> RuntimeResult<&'static str> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        let slot = node.slot() as usize;
        match inner.nodes[slot].kind {
            NodeKind::Internal => Ok("active"),
            NodeKind::Process => Ok("async"),
            NodeKind::Driver | NodeKind::PreInput => match inner.node_states[slot] {
                NodeState::Disabled => Ok("disabled"),
                NodeState::Polling => Ok("polling"),
                NodeState::Interrupt => Ok("interrupt wait"),
            },
        }
    }

    #[inline]
    pub fn node_runtime_data(&self, node: NodeId) -> RuntimeResult<NodeRuntime> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize]
            .runtime_data
            .get()
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
        mut runtime_data: NodeRuntime,
    ) -> RuntimeResult<()> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        runtime_data.node_index = node;
        let slot = &inner.nodes[node.slot() as usize];
        assert!(
            slot.runtime_data.get().is_some(),
            "Node runtime is borrowed by its invocation"
        );
        slot.runtime_data.set(Some(runtime_data));
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

    #[inline]
    pub fn node_trace_supported(&self, node: NodeId) -> RuntimeResult<bool> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize].trace_supported)
    }

    pub fn ready(&self) -> NodeRuntimeReady<'_> {
        NodeRuntimeReady {
            readiness: &self.readiness,
        }
    }

    /// Resolve the global column for a node-local error code.
    #[inline]
    pub fn node_error_index(&self, node: NodeId, code: u16) -> RuntimeResult<NodeErrorIndex> {
        let inner = self.inner.borrow();
        inner.node_error_index(node, code)
    }

    #[inline]
    pub fn node_error_descriptors(
        &self,
        node: NodeId,
    ) -> RuntimeResult<&'static [NodeErrorDescriptor]> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.error_descriptors[node.slot() as usize])
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
        let barrier = crate::WorkerThread::main().filter(|barrier| barrier.worker_count() != 0);
        if barrier.is_some() {
            crate::WorkerThread::__assert_held();
        }
        let mut inner = self.inner.borrow_mut();
        inner.validate_node(node)?;
        inner.validate_node(next)?;
        let changed = inner.set_node_next_slot(node, slot, next)?;
        drop(inner);
        if changed && let Some(barrier) = barrier {
            barrier.request_node_refork(&self.inner.borrow());
        }
        Ok(())
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
            crate::WorkerThread::main().is_some_and(|barrier| barrier.worker_count() != 0);
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
                crate::WorkerThread::__assert_held();
            }
        }
        let mut inner = self.inner.borrow_mut();
        let (slots, changed) = inner.add_node_next_slots(edges, shared_slots)?;
        drop(inner);
        if changed
            && let Some(barrier) =
                crate::WorkerThread::main().filter(|barrier| barrier.worker_count() != 0)
        {
            barrier.request_node_refork(&self.inner.borrow());
        }
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

    pub(crate) fn node_slots(&self) -> Arc<Vec<NodeRuntimeSlot>> {
        self.inner.borrow().node_slots()
    }

    fn runtime_slot(&self, node: NodeId) -> RuntimeResult<(NodeFunction, NodeRuntime)> {
        let inner = self.inner.borrow();
        let slot = inner
            .nodes
            .get(node.slot() as usize)
            .ok_or(RuntimeError::NodeNotRegistered { node })?;
        let runtime_data = slot
            .runtime_data
            .take()
            .expect("Node runtime has one active invocation");
        Ok((slot.process, runtime_data))
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

    #[test]
    fn duplicate_architecture_candidate_is_a_runtime_error() {
        let candidate =
            NodeFunctionRegistration::new("packet-input", NodeVariant::Base, process, (0, 4, 0));
        let error =
            preferred_node_function("packet-input", false, [&candidate, &candidate].into_iter())
                .err()
                .expect("duplicate variant is rejected");
        assert!(matches!(
            error,
            RuntimeError::DuplicateNodeFunction {
                node: "packet-input",
                variant: NodeVariant::Base,
            }
        ));
    }

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
        state.words = [
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
        ];
        frame.len()
    }

    // Derived from vlib/main.c frame_alloc_to_node and dispatch_node: the
    // destination layout selects storage, and each invocation mutates its own
    // persistent node runtime. This exercises the actual generated trampoline.
    #[test]
    fn registered_frame_arguments_and_node_state_survive_invocation() {
        crate::BUFFER_MAIN_INIT.call_once(|| {
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
                    false,
                ),
            )
            .unwrap();
        runtime
            .nodes()
            .install_node_function(
                node,
                __NODE_FUNCTION_PACKET_INPUT_VARIANTS.iter().copied(),
                process,
            )
            .unwrap();
        assert_eq!(runtime.nodes().frame_args_size(node).unwrap(), (8, 4, 2));
        for calls in 1..=2 {
            runtime.schedule_empty_frame(node).unwrap();
            assert_eq!(runtime.run_ready_nodes().unwrap(), 1);
            let data = runtime.nodes().node_runtime_data(node).unwrap();
            assert_eq!(data.words, [calls, 28, 8, 2]);
            assert_eq!(data.node_index(), node);
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
                    false,
                ),
            )
            .expect("register existing node");
        let mut worker = main.clone();
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
                    false,
                ),
            )
            .expect("register added node");
        worker.refork(main.inner.borrow().clone());

        let existing_data = worker.node_runtime_data(existing).unwrap();
        assert_eq!(existing_data.words, worker_data.words);
        assert_eq!(existing_data.node_index(), existing);
        assert_eq!(worker.node_state(existing).unwrap(), NodeState::Interrupt);
        assert_eq!(
            worker.inner.borrow().input_main_loops_per_call[existing.slot() as usize],
            4
        );
        let added_data = worker.node_runtime_data(added).unwrap();
        assert_eq!(added_data.words, [2, 3, 4, 5]);
        assert_eq!(added_data.node_index(), added);
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
            let input_vectors = frame.len();
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
                let graph = self.nodes.inner.borrow();
                let slot = &graph.nodes[node.slot() as usize];
                let mut runtime = slot
                    .runtime_data
                    .get()
                    .expect("pending node runtime is available before dispatch");
                runtime.flags = (runtime.flags & !(1 << 5)) | trace;
                slot.runtime_data.set(Some(runtime));
            }
            self.dispatch_node(node, &mut frame)?;
            self.internal_node_vectors = self
                .internal_node_vectors
                .wrapping_add(input_vectors as u64);
            self.internal_node_calls = self.internal_node_calls.wrapping_add(1);
            // VPP dispatch_pending_node, vlib/main.c:1064-1066. The input
            // count is captured before dispatch because Hammer may reuse
            // this Frame after the Node returns.
            self.max_internal_frame_vectors = self.max_internal_frame_vectors.max(input_vectors);
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
        let (process, mut state) = self.nodes.runtime_slot(node)?;
        self.set_current_node(Some(node));
        let count = process(self, &mut state, frame);
        // VPP `dispatch_node`: take one timestamp after the node returns, hand
        // the delta from the start of this dispatch to the counters, and leave
        // the value as the start of the next dispatch.
        let dispatch_end = hammer_infra::time::cpu_time_now();
        let dispatch_clocks = dispatch_end - self.last_time_stamp;
        self.last_time_stamp = dispatch_end;
        let inner = self.nodes.inner.borrow();
        let owner = &inner.nodes[node.slot() as usize];
        assert!(
            owner.runtime_data.replace(Some(state)).is_none(),
            "Node runtime has one active invocation"
        );
        owner.update_dispatch(count as u64, dispatch_clocks);
        drop(inner);
        self.set_current_node(None);
        Ok(count)
    }
}
