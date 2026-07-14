//! Feature Arc: control retains NodeId while compiling; packet path uses local u16.

use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::transmute;
use std::rc::Rc;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_core::data_plane::{BufferFrame, Index, NodeId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::node::NodeRuntime;
use hammer_runtime::{DataPlaneBarrierHandle, DataPlaneRuntime, NodeResult};

use crate::net::NetworkOpaque;
use hammer_infra::vec::Vec;

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
    nodes: Option<NodeRuntime>,
    start_nodes: Vec<NodeId>,
    default_end: Option<NodeId>,
    _arc: PhantomData<fn() -> A>,
    _control_thread_only: PhantomData<Rc<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureArcStart {
    pub next: u16,
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
    default_end: Option<NodeId>,
    start_nodes: Vec<NodeId>,
    /// Per-interface compiled chains (config heap slices).
    chains: HashMap<u32, FeatureArcChain>,
    /// (start_node, interface) → first local next into the chain.
    start_nexts: HashMap<(u32, u32), u16>,
    /// Flat config heap shared by all interfaces; chain entries index into it.
    configs: Vec<FeatureArcConfigEntry>,
}

#[derive(Debug, Clone, Default)]
struct FeatureArcStartState {
    start_nexts: HashMap<(u32, u32), u16>,
    /// interface → first config index in `configs`.
    chain_heads: HashMap<u32, u32>,
    configs: Vec<FeatureArcConfigEntry>,
}

#[derive(Debug, Clone, Copy)]
struct FeatureArcConfigEntry {
    next: u16,
    next_config_index: u32,
}

impl<A: FeatureArcSpec> Default for FeatureArcState<A> {
    fn default() -> Self {
        Self {
            registered: HashMap::new(),
            feature_order: Vec::new(),
            enabled: HashMap::new(),
            end_nodes: HashMap::new(),
            default_end: None,
            start_nodes: Vec::new(),
            chains: HashMap::new(),
            start_nexts: HashMap::new(),
            configs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FeatureArcChain {
    /// Index of the first feature's config entry; `u32::MAX` if empty.
    head_config: u32,
    steps: Vec<FeatureArcStep>,
}

#[derive(Debug, Clone)]
struct FeatureArcEnabled<A: FeatureArcSpec> {
    id: A,
    config: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct FeatureArcStep {
    next: u16,
    next_config_index: u32,
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
}

impl FeatureArcStartHandle {
    /// Start Feature Arc for `interface_index` from the current Graph Node.
    /// Empty/missing chains return `default_next` without touching config progress.
    #[inline]
    pub fn start_for_interface_or(
        &self,
        runtime: &DataPlaneRuntime,
        index: Index,
        interface_index: u32,
        default_next: u16,
    ) -> u16 {
        self.start_for_interface_or_with_result(runtime, index, interface_index, default_next)
            .next
    }

    #[inline]
    pub fn start_for_interface_or_with_result(
        &self,
        runtime: &DataPlaneRuntime,
        index: Index,
        interface_index: u32,
        default_next: u16,
    ) -> FeatureArcStart {
        let state = self.inner.load();
        let start = runtime.current_node().expect("current node").slot();
        let Some(&first_next) = state.start_nexts.get(&(start, interface_index)) else {
            return FeatureArcStart {
                next: default_next,
                started: false,
            };
        };
        let Some(&head) = state.chain_heads.get(&interface_index) else {
            return FeatureArcStart {
                next: default_next,
                started: false,
            };
        };
        set_feature_config_index(runtime, index, head);
        FeatureArcStart {
            next: first_next,
            started: true,
        }
    }

    /// Advance Feature configuration progress exactly once for this Index.
    #[inline]
    pub fn next_feature_slot(&self, runtime: &DataPlaneRuntime, index: Index) -> u16 {
        let state = self.inner.load();
        let config_index = feature_config_index(runtime, index);
        let entry = state
            .configs
            .get(config_index as usize)
            .expect("feature config index must be valid");
        set_feature_config_index(runtime, index, entry.next_config_index);
        entry.next
    }
}

#[inline(always)]
fn feature_config_index(runtime: &DataPlaneRuntime, index: Index) -> u32 {
    let buffer = runtime.get_buffer(index).expect("buffer");
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    network.feature_config_index()
}

#[inline(always)]
fn set_feature_config_index(runtime: &DataPlaneRuntime, index: Index, config: u32) {
    let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
    let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
    network.set_feature_config_index(config);
}

#[inline(always)]
pub fn next_feature_slot_for_index(
    handle: &FeatureArcStartHandle,
    runtime: &DataPlaneRuntime,
    index: Index,
) -> u16 {
    handle.next_feature_slot(runtime, index)
}

#[inline(always)]
pub fn next_feature_frame(
    handle: &FeatureArcStartHandle,
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
) -> NodeResult {
    let mut nexts = Vec::with_capacity(frame.len());
    for index in frame.iter_indices() {
        nexts.push(handle.next_feature_slot(runtime, *index));
    }
    runtime.enqueue_to_next(frame, nexts.as_slice());
    NodeResult::drop()
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
            nodes: None,
            start_nodes: Vec::new(),
            default_end: None,
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
    pub fn with_nodes(mut self, nodes: NodeRuntime) -> Self {
        self.nodes = Some(nodes);
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
        self.publish()
    }

    /// Attach the published Feature Arc handle to a start Graph Node instance.
    #[inline]
    pub fn attach_start<S: FeatureArcStartNode<A>>(&self, start: &mut S) {
        start.set_feature_arc(self.arc());
    }

    #[inline]
    pub fn detach_start<S: FeatureArcStartNode<A>>(&self, start: &mut S) {
        start.clear_feature_arc();
    }

    /// Record a start Graph Node so publication compiles its predecessor-local first transition.
    pub fn add_start_node(&mut self, node: NodeId) -> CoreResult<()> {
        if !self.start_nodes.contains(&node) {
            self.start_nodes.push(node);
            self.state.start_nodes = self.start_nodes.clone();
            self.publish()?;
        }
        Ok(())
    }

    pub fn remove_start_node(&mut self, node: NodeId) -> CoreResult<()> {
        self.start_nodes.retain(|n| *n != node);
        self.state.start_nodes = self.start_nodes.clone();
        self.publish()
    }

    /// Attach and record a start node, then publish. Prefer registering the node
    /// after [`attach_start`] so the graph owns the arc-bearing instance, then
    /// call [`add_start_node`].
    pub fn attach_start_at<S: FeatureArcStartNode<A>>(
        &mut self,
        start: &mut S,
        node: NodeId,
    ) -> CoreResult<()> {
        self.attach_start(start);
        self.add_start_node(node)
    }

    pub fn set_default_end_node(&mut self, node: NodeId) -> CoreResult<()> {
        self.default_end = Some(node);
        self.state.default_end = Some(node);
        self.publish()
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
        self.publish()
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
        self.publish()
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
        state.start_nodes = self.start_nodes.clone();
        state.default_end = self.default_end;
        let nodes = self
            .nodes
            .clone()
            .ok_or_else(|| CoreError::internal("feature arc publish requires node runtime"))?;
        let barrier = self.barrier.as_ref().ok_or_else(|| {
            CoreError::internal("feature arc publish requires data-plane barrier")
        })?;
        let mut published = None;
        barrier.synchronize(|| {
            state.rebuild(&nodes)?;
            published = Some(state.clone());
            inner.replace_after_barrier(state);
            Ok(())
        })?;
        self.state = published.expect("feature arc publish produced state");
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
    fn rebuild(&mut self, nodes: &NodeRuntime) -> CoreResult<()> {
        self.feature_order = self.sorted_feature_order()?;
        let feature_order = self.feature_order.clone();
        let mut chains = HashMap::with_capacity(self.enabled.len());
        let mut start_nexts = HashMap::new();
        let mut configs = Vec::new();

        for (interface_index, enabled) in &self.enabled {
            let enabled = enabled
                .iter()
                .map(|enabled| enabled.id)
                .collect::<HashSet<_>>();
            let chain_features = feature_order
                .iter()
                .copied()
                .filter(|id| enabled.contains(id))
                .collect::<Vec<_>>();
            if chain_features.is_empty() {
                continue;
            }

            let end_node = self
                .end_nodes
                .get(interface_index)
                .copied()
                .or(self.default_end)
                .ok_or_else(|| {
                    CoreError::internal("feature arc chain requires a compiled end node")
                })?;

            let feature_nodes = chain_features
                .iter()
                .map(|id| {
                    self.registered
                        .get(id)
                        .map(|registration| registration.node)
                        .ok_or_else(|| CoreError::internal("feature chain references missing node"))
                })
                .collect::<CoreResult<Vec<_>>>()?;

            let head_config = configs.len() as u32;
            let mut steps = Vec::with_capacity(feature_nodes.len());

            for (offset, feature_node) in feature_nodes.iter().copied().enumerate() {
                let successor = if offset + 1 < feature_nodes.len() {
                    feature_nodes[offset + 1]
                } else {
                    end_node
                };
                let next = nodes.add_node_next_slot(feature_node, successor)?;
                let next_config_index = if offset + 1 < feature_nodes.len() {
                    head_config + offset as u32 + 1
                } else {
                    // Terminal: leave index past the heap; advance must not be called again.
                    u32::MAX
                };
                steps.push(FeatureArcStep {
                    next,
                    next_config_index,
                });
                configs.push(FeatureArcConfigEntry {
                    next,
                    next_config_index,
                });
            }

            for start in &self.start_nodes {
                let first = feature_nodes[0];
                let start_next = nodes.add_node_next_slot(*start, first)?;
                start_nexts.insert((start.slot(), *interface_index), start_next);
            }

            chains.insert(*interface_index, FeatureArcChain { head_config, steps });
        }

        self.chains = chains;
        self.start_nexts = start_nexts;
        self.configs = configs;
        Ok(())
    }

    fn start_state(&self) -> FeatureArcStartState {
        let mut chain_heads = HashMap::with_capacity(self.chains.len());
        for (interface, chain) in &self.chains {
            chain_heads.insert(*interface, chain.head_config);
        }
        FeatureArcStartState {
            start_nexts: self.start_nexts.clone(),
            chain_heads,
            configs: self.configs.clone(),
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

        let mut sorted = Vec::with_capacity(self.registered.len());
        let mut selected = HashSet::new();
        while sorted.len() < self.registered.len() {
            let Some(next) = self
                .feature_order
                .iter()
                .copied()
                .chain(self.registered.keys().copied())
                .find(|id| !selected.contains(id) && indegree.get(id).copied().unwrap_or(0) == 0)
                .or_else(|| {
                    // Stable fallback: lowest ordinal among zero-indegree.
                    self.registered
                        .keys()
                        .copied()
                        .filter(|id| {
                            !selected.contains(id) && indegree.get(id).copied().unwrap_or(0) == 0
                        })
                        .min_by_key(|id| {
                            self.registered
                                .get(id)
                                .map(|registration| registration.ordinal)
                                .unwrap_or(usize::MAX)
                        })
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
