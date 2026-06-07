use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    Buffer, BufferFrame, BufferIndex, DataPlaneRuntime, FeaturePathEntry, InternalNode, Node,
    NodeId, NodeProcessFn, NodeRegistration, NodeResult, NodeVectorDispatch, PacketTrace,
    RouteMetadata, TraceFormatter, add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::DataPlaneBarrierHandle;

use crate::trace::codec::put_usize;

#[derive(Debug, Clone, Copy, Default)]
pub struct DropNode;

impl DropNode {
    pub const NODE_NAME: &'static str = "drop-node";

    #[inline]
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropTrace {
    pub dropped: usize,
}

impl DropTrace {
    pub const ENCODED_LEN: usize = 8;

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        Some(Self {
            dropped: usize::try_from(u64::from_le_bytes(bytes.try_into().ok()?)).ok()?,
        })
    }
}

impl PacketTrace for DropTrace {
    #[inline]
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_usize(out, self.dropped);
    }
}

fn format_drop_trace(bytes: &[u8]) -> String {
    match DropTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("DropTrace invalid={bytes:?}"),
    }
}

impl Node for DropNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "drop node must run through descriptor process",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        drop_node_process
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_drop_trace)
    }
}

fn drop_node_process(
    runtime: &DataPlaneRuntime,
    _data: hammer_adapter::node::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let dropped = frame.pending_len();
    {
        let mut cursor = frame.batch_cursor(runtime.preferred_frame_batch_width());
        cursor.prefetch_next(runtime);
        while let Some(batch) = cursor.next() {
            cursor.prefetch_next(runtime);
            for index in batch.indices() {
                add_packet_trace!(runtime, index, DropTrace { dropped })?;
                runtime.free_index(index);
            }
        }
    }
    frame.clear();
    Ok(NodeResult::drop())
}

impl InternalNode for DropNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, 0)
    }
}

pub struct FeatureArc<A: FeatureArcSpec> {
    inner: Arc<FeatureArcInner<A>>,
    _arc: PhantomData<fn() -> A>,
}

#[derive(Clone)]
pub struct FeatureArcStartHandle {
    inner: Arc<ArcSwap<FeatureArcStartState>>,
}

pub struct FeatureArcStartSlot<A: FeatureArcSpec> {
    arc: Option<FeatureArc<A>>,
}

pub struct FeatureArcControl<A: FeatureArcSpec> {
    inner: Arc<FeatureArcInner<A>>,
    state: FeatureArcState<A>,
    barrier: Option<DataPlaneBarrierHandle>,
    _arc: PhantomData<fn() -> A>,
    _control_thread_only: PhantomData<Rc<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureArcStart {
    pub node: NodeId,
    pub started: bool,
}

#[derive(Debug)]
struct FeatureArcInner<A: FeatureArcSpec> {
    state: UnsafeCell<FeatureArcState<A>>,
    start_state: Arc<ArcSwap<FeatureArcStartState>>,
}

pub trait FeatureArcSpec: Copy + Eq + Hash + fmt::Debug + 'static {}

pub trait Feature<A: FeatureArcSpec>: 'static {
    fn id() -> A;

    #[inline]
    fn runs_before() -> Vec<A> {
        Vec::new()
    }

    #[inline]
    fn runs_after() -> Vec<A> {
        Vec::new()
    }
}

pub trait FeatureArcStartNode<A: FeatureArcSpec>: 'static {
    fn set_feature_arc(&mut self, arc: FeatureArc<A>);

    fn clear_feature_arc(&mut self);
}

impl<A: FeatureArcSpec> Clone for FeatureArc<A> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _arc: PhantomData,
        }
    }
}

impl<A: FeatureArcSpec> Default for FeatureArcStartSlot<A> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<A: FeatureArcSpec> FeatureArcStartSlot<A> {
    #[inline]
    pub fn new() -> Self {
        Self { arc: None }
    }

    #[inline]
    pub fn as_ref(&self) -> Option<&FeatureArc<A>> {
        self.arc.as_ref()
    }

    #[inline]
    pub fn set(&mut self, arc: FeatureArc<A>) {
        self.arc = Some(arc);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.arc = None;
    }
}

impl<A: FeatureArcSpec> fmt::Debug for FeatureArc<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("FeatureArc").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
struct FeatureArcRegistration<A: FeatureArcSpec> {
    node: NodeId,
    runs_before: Vec<A>,
    runs_after: Vec<A>,
    ordinal: usize,
}

#[derive(Debug, Clone)]
struct FeatureArcState<A: FeatureArcSpec> {
    registered: HashMap<A, FeatureArcRegistration<A>>,
    feature_order: Vec<A>,
    enabled: HashMap<u32, Vec<FeatureArcEnabled<A>>>,
    end_nodes: HashMap<u32, NodeId>,
    chains: HashMap<u32, FeatureArcChain>,
}

#[derive(Debug, Clone, Default)]
struct FeatureArcStartState {
    end_nodes: HashMap<u32, NodeId>,
    chains: HashMap<u32, FeatureArcChain>,
}

impl<A: FeatureArcSpec> Default for FeatureArcState<A> {
    fn default() -> Self {
        Self {
            registered: HashMap::new(),
            feature_order: Vec::new(),
            enabled: HashMap::new(),
            end_nodes: HashMap::new(),
            chains: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FeatureArcChain {
    steps: Vec<FeatureArcStep>,
}

#[derive(Debug, Clone)]
struct FeatureArcEnabled<A: FeatureArcSpec> {
    id: A,
    config: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct FeatureArcStep {
    node: NodeId,
    config: Option<Vec<u8>>,
}

impl<A: FeatureArcSpec> Default for FeatureArc<A> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<A: FeatureArcSpec> FeatureArc<A> {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(FeatureArcInner::new()),
            _arc: PhantomData,
        }
    }

    #[inline]
    pub fn start_handle(&self) -> FeatureArcStartHandle {
        FeatureArcStartHandle {
            inner: Arc::clone(&self.inner.start_state),
        }
    }

    #[inline]
    pub fn start_or(&self, metadata: &mut RouteMetadata, default_next: NodeId) -> NodeId {
        self.start_or_with_end(metadata, default_next, default_next)
            .node
    }

    #[inline]
    pub fn start_or_with_end(
        &self,
        metadata: &mut RouteMetadata,
        default_next: NodeId,
        end_next: NodeId,
    ) -> FeatureArcStart {
        metadata.clear_feature_path();
        let Some(interface_index) = metadata.ingress_interface else {
            return FeatureArcStart {
                node: default_next,
                started: false,
            };
        };
        self.start_for_interface_or_with_end(interface_index, metadata, default_next, end_next)
    }

    #[inline]
    pub fn start_for_interface_or(
        &self,
        interface_index: u32,
        metadata: &mut RouteMetadata,
        default_next: NodeId,
    ) -> NodeId {
        self.start_for_interface_or_with_end(interface_index, metadata, default_next, default_next)
            .node
    }

    #[inline]
    pub fn start_for_interface_or_with_end(
        &self,
        interface_index: u32,
        metadata: &mut RouteMetadata,
        default_next: NodeId,
        end_next: NodeId,
    ) -> FeatureArcStart {
        let state = self.inner.state();
        let override_end = state.end_nodes.get(&interface_index).copied();
        let default_next = override_end.unwrap_or(default_next);
        let end_next = override_end.unwrap_or(end_next);
        let Some(chain) = state.chains.get(&interface_index) else {
            return FeatureArcStart {
                node: default_next,
                started: false,
            };
        };
        let Some(first) = chain.steps.first() else {
            return FeatureArcStart {
                node: default_next,
                started: false,
            };
        };
        let mut next_entries = chain
            .steps
            .iter()
            .skip(1)
            .map(|step| FeaturePathEntry::new(step.node, step.config.clone()))
            .collect::<Vec<_>>();
        next_entries.push(FeaturePathEntry::new(end_next, None));
        metadata.set_current_feature_config(first.config.clone());
        metadata.set_feature_path(next_entries);
        FeatureArcStart {
            node: first.node,
            started: true,
        }
    }
}

impl FeatureArcStartHandle {
    #[inline]
    pub fn start_or(&self, metadata: &mut RouteMetadata, default_next: NodeId) -> NodeId {
        self.start_or_with_end(metadata, default_next, default_next)
            .node
    }

    #[inline]
    pub fn start_or_with_end(
        &self,
        metadata: &mut RouteMetadata,
        default_next: NodeId,
        end_next: NodeId,
    ) -> FeatureArcStart {
        metadata.clear_feature_path();
        let Some(interface_index) = metadata.ingress_interface else {
            return FeatureArcStart {
                node: default_next,
                started: false,
            };
        };
        self.start_for_interface_or_with_end(interface_index, metadata, default_next, end_next)
    }

    #[inline]
    pub fn start_for_interface_or_with_end(
        &self,
        interface_index: u32,
        metadata: &mut RouteMetadata,
        default_next: NodeId,
        end_next: NodeId,
    ) -> FeatureArcStart {
        let state = self.inner.load();
        let override_end = state.end_nodes.get(&interface_index).copied();
        let default_next = override_end.unwrap_or(default_next);
        let end_next = override_end.unwrap_or(end_next);
        let Some(chain) = state.chains.get(&interface_index) else {
            return FeatureArcStart {
                node: default_next,
                started: false,
            };
        };
        let Some(first) = chain.steps.first() else {
            return FeatureArcStart {
                node: default_next,
                started: false,
            };
        };
        let mut next_entries = chain
            .steps
            .iter()
            .skip(1)
            .map(|step| FeaturePathEntry::new(step.node, step.config.clone()))
            .collect::<Vec<_>>();
        next_entries.push(FeaturePathEntry::new(end_next, None));
        metadata.set_current_feature_config(first.config.clone());
        metadata.set_feature_path(next_entries);
        FeatureArcStart {
            node: first.node,
            started: true,
        }
    }
}

#[inline(always)]
pub fn next_feature_node_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<NodeId> {
    let next = runtime.with_metadata_mut(index, RouteMetadata::pop_feature_next)?;
    next.ok_or_else(|| CoreError::internal("missing feature next node metadata"))
}

#[inline(always)]
pub fn next_feature_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        Ok(Some(next_feature_node_for_index(runtime, index)?))
    })?;
    Ok(result)
}

#[inline(always)]
pub fn set_buffer_node_error_code(
    runtime: &DataPlaneRuntime,
    buffer: &mut Buffer,
    code: u16,
) -> CoreResult<()> {
    let error = runtime.record_current_node_error(code)?;
    buffer.set_node_error(error);
    Ok(())
}

#[inline(always)]
pub fn set_index_node_error_code(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    code: u16,
) -> CoreResult<()> {
    let error = runtime.record_current_node_error(code)?;
    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.set_node_error(error);
    Ok(())
}

impl<A: FeatureArcSpec> Default for FeatureArcControl<A> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<A: FeatureArcSpec> FeatureArcControl<A> {
    #[inline]
    pub fn new() -> Self {
        let inner = Arc::new(FeatureArcInner::new());
        Self {
            inner,
            state: FeatureArcState::default(),
            barrier: None,
            _arc: PhantomData,
            _control_thread_only: PhantomData,
        }
    }

    #[inline]
    pub fn with_data_plane_barrier(mut self, barrier: DataPlaneBarrierHandle) -> Self {
        self.barrier = Some(barrier);
        self
    }

    #[inline]
    pub fn arc(&self) -> FeatureArc<A> {
        FeatureArc {
            inner: Arc::clone(&self.inner),
            _arc: PhantomData,
        }
    }

    pub fn register_feature<F: Feature<A>>(&mut self, node: NodeId) -> CoreResult<()> {
        let id = F::id();
        let ordinal = self
            .state
            .registered
            .get(&id)
            .map(|registration| registration.ordinal)
            .unwrap_or(self.state.feature_order.len());
        if !self.state.registered.contains_key(&id) {
            self.state.feature_order.push(id);
        }
        self.state.registered.insert(
            id,
            FeatureArcRegistration {
                node,
                runs_before: F::runs_before(),
                runs_after: F::runs_after(),
                ordinal,
            },
        );
        self.publish()?;
        Ok(())
    }

    #[inline]
    pub fn attach_start<S: FeatureArcStartNode<A>>(&self, start: &mut S) {
        start.set_feature_arc(self.arc());
    }

    #[inline]
    pub fn detach_start<S: FeatureArcStartNode<A>>(&self, start: &mut S) {
        start.clear_feature_arc();
    }

    pub fn enable_feature<F: Feature<A>>(&mut self, interface_index: u32) -> CoreResult<()> {
        self.enable_feature_config::<F>(interface_index, None)
    }

    pub fn enable_feature_with_config<F: Feature<A>>(
        &mut self,
        interface_index: u32,
        config: impl Into<Vec<u8>>,
    ) -> CoreResult<()> {
        self.enable_feature_config::<F>(interface_index, Some(config.into()))
    }

    fn enable_feature_config<F: Feature<A>>(
        &mut self,
        interface_index: u32,
        config: Option<Vec<u8>>,
    ) -> CoreResult<()> {
        let id = F::id();
        if !self.state.registered.contains_key(&id) {
            return Err(CoreError::internal(format!(
                "feature is not registered: {:?}",
                id
            )));
        }
        let enabled = self.state.enabled.entry(interface_index).or_default();
        if let Some(enabled) = enabled.iter_mut().find(|enabled| enabled.id == id) {
            enabled.config = config;
        } else {
            enabled.push(FeatureArcEnabled { id, config });
        }
        self.publish()?;
        Ok(())
    }

    pub fn disable_feature<F: Feature<A>>(&mut self, interface_index: u32) -> CoreResult<()> {
        let id = F::id();
        if !self.state.registered.contains_key(&id) {
            return Err(CoreError::internal(format!(
                "feature is not registered: {:?}",
                id
            )));
        }
        if let Some(enabled) = self.state.enabled.get_mut(&interface_index) {
            enabled.retain(|enabled| enabled.id != id);
            if enabled.is_empty() {
                self.state.enabled.remove(&interface_index);
            }
        }
        self.publish()?;
        Ok(())
    }

    #[inline]
    pub fn is_feature_enabled<F: Feature<A>>(&self, interface_index: u32) -> bool {
        let id = F::id();
        self.state
            .enabled
            .get(&interface_index)
            .is_some_and(|enabled| enabled.iter().any(|enabled| enabled.id == id))
    }

    pub fn set_end_node_for_interface(
        &mut self,
        interface_index: u32,
        node: NodeId,
    ) -> CoreResult<()> {
        self.state.end_nodes.insert(interface_index, node);
        self.publish()
    }

    pub fn clear_end_node_for_interface(&mut self, interface_index: u32) -> CoreResult<()> {
        self.state.end_nodes.remove(&interface_index);
        self.publish()
    }

    #[inline]
    fn publish(&mut self) -> CoreResult<()> {
        let inner = Arc::clone(&self.inner);
        let mut state = self.state.clone();
        let barrier = self.barrier.as_ref().ok_or_else(|| {
            CoreError::internal("feature arc publish requires data-plane barrier")
        })?;
        barrier.synchronize(|| {
            state.rebuild()?;
            inner.replace_after_barrier(state);
            Ok(())
        })?;
        Ok(())
    }
}

impl<A: FeatureArcSpec> FeatureArcInner<A> {
    #[inline]
    fn new() -> Self {
        Self {
            state: UnsafeCell::new(FeatureArcState::<A>::default()),
            start_state: Arc::new(ArcSwap::from_pointee(FeatureArcStartState::default())),
        }
    }

    #[inline]
    fn state(&self) -> &FeatureArcState<A> {
        // SAFETY: Feature arc writes are serialized by the runtime data-plane
        // barrier before publication. Data-plane nodes only read immutable
        // state while workers are running.
        unsafe { &*self.state.get() }
    }

    #[inline]
    fn replace_after_barrier(&self, state: FeatureArcState<A>) {
        self.start_state.store(Arc::new(state.start_state()));
        // SAFETY: callers replace state either while the runtime data-plane
        // barrier is held, or during single-threaded graph setup in tests.
        unsafe {
            *self.state.get() = state;
        }
    }
}

impl<A: FeatureArcSpec> FeatureArcState<A> {
    fn rebuild(&mut self) -> CoreResult<()> {
        self.feature_order = self.sorted_feature_order()?;
        let feature_order = self.feature_order.clone();
        let mut chains = HashMap::with_capacity(self.enabled.len());
        for (interface_index, enabled) in &self.enabled {
            let enabled = enabled
                .iter()
                .map(|enabled| (enabled.id, enabled.config.clone()))
                .collect::<HashMap<_, _>>();
            let chain_features = feature_order
                .iter()
                .filter_map(|id| enabled.get(id).map(|config| (*id, config.clone())))
                .collect::<Vec<_>>();
            let mut steps = Vec::with_capacity(chain_features.len());
            for (id, config) in chain_features {
                let node = self
                    .registered
                    .get(&id)
                    .ok_or_else(|| CoreError::internal("feature chain references missing node"))?
                    .node;
                steps.push(FeatureArcStep { node, config });
            }
            chains.insert(*interface_index, FeatureArcChain { steps });
        }
        self.chains = chains;
        Ok(())
    }

    fn start_state(&self) -> FeatureArcStartState {
        FeatureArcStartState {
            end_nodes: self.end_nodes.clone(),
            chains: self.chains.clone(),
        }
    }

    fn sorted_feature_order(&self) -> CoreResult<Vec<A>> {
        let mut edges = self
            .registered
            .keys()
            .map(|id| (*id, Vec::<A>::new()))
            .collect::<HashMap<_, _>>();
        let mut indegree = self
            .registered
            .keys()
            .map(|id| (*id, 0usize))
            .collect::<HashMap<_, _>>();
        let mut seen_edges = HashSet::<(A, A)>::new();

        for (id, registration) in &self.registered {
            for runs_before in &registration.runs_before {
                if self.registered.contains_key(runs_before) {
                    add_feature_order_edge(
                        &mut edges,
                        &mut indegree,
                        &mut seen_edges,
                        id,
                        runs_before,
                    );
                }
            }
            for runs_after in &registration.runs_after {
                if self.registered.contains_key(runs_after) {
                    add_feature_order_edge(
                        &mut edges,
                        &mut indegree,
                        &mut seen_edges,
                        runs_after,
                        id,
                    );
                }
            }
        }

        let mut selected = HashSet::<A>::new();
        let mut sorted = Vec::with_capacity(self.registered.len());
        while sorted.len() < self.registered.len() {
            let Some(next) = self
                .feature_order
                .iter()
                .copied()
                .filter(|id| self.registered.contains_key(id))
                .filter(|id| !selected.contains(id))
                .filter(|id| indegree.get(id).copied().unwrap_or_default() == 0)
                .min_by_key(|id| {
                    self.registered
                        .get(id)
                        .map(|registration| registration.ordinal)
                        .unwrap_or(usize::MAX)
                })
            else {
                return Err(CoreError::internal(
                    "feature order constraints contain a cycle",
                ));
            };

            selected.insert(next);
            sorted.push(next);
            if let Some(targets) = edges.get(&next) {
                for target in targets {
                    if let Some(count) = indegree.get_mut(target) {
                        *count = count.saturating_sub(1);
                    }
                }
            }
        }

        Ok(sorted)
    }
}

fn add_feature_order_edge<A: FeatureArcSpec>(
    edges: &mut HashMap<A, Vec<A>>,
    indegree: &mut HashMap<A, usize>,
    seen_edges: &mut HashSet<(A, A)>,
    from: &A,
    to: &A,
) {
    if from == to || !seen_edges.insert((*from, *to)) {
        return;
    }
    edges.entry(*from).or_default().push(*to);
    *indegree.entry(*to).or_default() += 1;
}
