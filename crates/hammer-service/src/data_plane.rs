use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, FeaturePathEntry, InternalNode, Node, NodeId,
    NodeNextEnqueue, NodeResult, RouteMetadata, Router,
};
use hammer_core::error::{CoreError, CoreResult};

#[derive(Debug, Clone, Copy, Default)]
pub struct DropNode;

impl DropNode {
    #[inline(always)]
    pub const fn new() -> Self {
        Self
    }
}

impl<G> Node<G> for DropNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.drain_pending() {
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }
}

impl<G> InternalNode<G> for DropNode {}

pub struct FeatureArc<A: FeatureArcSpec> {
    inner: Arc<FeatureArcInner>,
    _arc: PhantomData<fn() -> A>,
}

pub struct FeatureArcControl<A: FeatureArcSpec> {
    inner: Arc<FeatureArcInner>,
    state: FeatureArcSnapshot,
    _arc: PhantomData<fn() -> A>,
    _control_thread_only: PhantomData<Rc<()>>,
}

#[derive(Debug)]
struct FeatureArcInner {
    snapshot: ArcSwap<FeatureArcSnapshot>,
}

pub trait FeatureArcSpec {
    const NAME: &'static str;
}

pub trait Feature<A: FeatureArcSpec> {
    const NAME: &'static str;

    #[inline]
    fn order() -> FeatureArcOrder {
        FeatureArcOrder::default()
    }
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

impl<A: FeatureArcSpec> fmt::Debug for FeatureArc<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FeatureArc")
            .field("name", &A::NAME)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeatureArcOrder {
    before: Vec<String>,
    after: Vec<String>,
}

#[derive(Debug, Clone)]
struct FeatureArcRegistration {
    node: NodeId,
    order: FeatureArcOrder,
    ordinal: usize,
}

#[derive(Debug, Clone, Default)]
struct FeatureArcSnapshot {
    registered: HashMap<String, FeatureArcRegistration>,
    feature_order: Vec<String>,
    enabled: HashMap<u32, Vec<FeatureArcEnabled>>,
    end_nodes: HashMap<u32, NodeId>,
    chains: HashMap<u32, FeatureArcChain>,
}

#[derive(Debug, Clone, Default)]
struct FeatureArcChain {
    steps: Vec<FeatureArcStep>,
}

#[derive(Debug, Clone)]
struct FeatureArcEnabled {
    name: String,
    config: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct FeatureArcStep {
    node: NodeId,
    config: Option<Vec<u8>>,
}

impl FeatureArcOrder {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn runs_before<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            before: names.into_iter().map(Into::into).collect(),
            after: Vec::new(),
        }
    }

    #[inline]
    pub fn runs_after<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            before: Vec::new(),
            after: names.into_iter().map(Into::into).collect(),
        }
    }

    #[inline]
    pub fn with_runs_before<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.before.extend(names.into_iter().map(Into::into));
        self
    }

    #[inline]
    pub fn with_runs_after<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.after.extend(names.into_iter().map(Into::into));
        self
    }
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
    pub fn start_or(&self, metadata: &mut RouteMetadata, default_next: NodeId) -> NodeId {
        metadata.clear_feature_path();
        let Some(interface_index) = metadata.ingress_interface else {
            return default_next;
        };
        self.start_for_interface_or(interface_index, metadata, default_next)
    }

    #[inline]
    fn start_for_interface_or(
        &self,
        interface_index: u32,
        metadata: &mut RouteMetadata,
        default_next: NodeId,
    ) -> NodeId {
        let snapshot = self.inner.snapshot.load();
        let default_next = snapshot
            .end_nodes
            .get(&interface_index)
            .copied()
            .unwrap_or(default_next);
        let Some(chain) = snapshot.chains.get(&interface_index) else {
            return default_next;
        };
        let Some(first) = chain.steps.first() else {
            return default_next;
        };
        let mut next_entries = chain
            .steps
            .iter()
            .skip(1)
            .map(|step| FeaturePathEntry::new(step.node, step.config.clone()))
            .collect::<Vec<_>>();
        next_entries.push(FeaturePathEntry::new(default_next, None));
        metadata.set_current_feature_config(first.config.clone());
        metadata.set_feature_path(next_entries);
        first.node
    }
}

pub struct FeatureArcStartNode<A: FeatureArcSpec> {
    arc: FeatureArc<A>,
    default_next: NodeId,
}

impl<A: FeatureArcSpec> FeatureArcStartNode<A> {
    #[inline]
    pub fn new(arc: FeatureArc<A>, default_next: NodeId) -> Self {
        Self { arc, default_next }
    }
}

impl<A, G> Node<G> for FeatureArcStartNode<A>
where
    A: FeatureArcSpec,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let Some(first) = frame.pending_indices().first().copied() else {
            return Ok(NodeResult::drop());
        };
        let speculative =
            feature_arc_start_for_index(runtime, first, &self.arc, self.default_next)?;
        NodeNextEnqueue::new(speculative).validate_frame_with_first_next(
            runtime,
            frame,
            first,
            speculative,
            |index| feature_arc_start_for_index(runtime, index, &self.arc, self.default_next),
        )
    }
}

impl<A, G> InternalNode<G> for FeatureArcStartNode<A> where A: FeatureArcSpec {}

#[inline(always)]
fn feature_arc_start_for_index<A, G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    arc: &FeatureArc<A>,
    default_next: NodeId,
) -> CoreResult<NodeId>
where
    A: FeatureArcSpec,
{
    runtime.with_metadata_mut(index, |metadata| arc.start_or(metadata, default_next))
}

#[inline(always)]
pub fn next_feature_node_for_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
) -> CoreResult<NodeId> {
    let next = runtime.with_metadata_mut(index, RouteMetadata::pop_feature_next)?;
    next.ok_or_else(|| CoreError::internal("missing feature next node metadata"))
}

#[inline(always)]
pub fn next_feature_frame<G>(
    runtime: &DataPlaneRuntime<G>,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let Some(first) = frame.pending_indices().first().copied() else {
        return Ok(NodeResult::drop());
    };
    let speculative = next_feature_node_for_index(runtime, first)?;
    NodeNextEnqueue::new(speculative).validate_frame_with_first_next(
        runtime,
        frame,
        first,
        speculative,
        |index| next_feature_node_for_index(runtime, index),
    )
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
            state: FeatureArcSnapshot::default(),
            _arc: PhantomData,
            _control_thread_only: PhantomData,
        }
    }

    #[inline]
    pub fn arc(&self) -> FeatureArc<A> {
        FeatureArc {
            inner: Arc::clone(&self.inner),
            _arc: PhantomData,
        }
    }

    pub fn register_feature<F: Feature<A>>(&mut self, node: NodeId) -> CoreResult<()> {
        let name = F::NAME;
        if name.is_empty() {
            return Err(CoreError::internal("feature name must not be empty"));
        }
        let ordinal = self
            .state
            .registered
            .get(name)
            .map(|registration| registration.ordinal)
            .unwrap_or(self.state.feature_order.len());
        if !self.state.registered.contains_key(name) {
            self.state.feature_order.push(name.to_owned());
        }
        self.state.registered.insert(
            name.to_owned(),
            FeatureArcRegistration {
                node,
                order: F::order(),
                ordinal,
            },
        );
        self.publish()?;
        Ok(())
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
        if !self.state.registered.contains_key(F::NAME) {
            return Err(CoreError::internal(format!(
                "feature is not registered: {}",
                F::NAME
            )));
        }
        let enabled = self.state.enabled.entry(interface_index).or_default();
        if let Some(enabled) = enabled.iter_mut().find(|enabled| enabled.name == F::NAME) {
            enabled.config = config;
        } else {
            enabled.push(FeatureArcEnabled {
                name: F::NAME.to_owned(),
                config,
            });
        }
        self.publish()?;
        Ok(())
    }

    pub fn disable_feature<F: Feature<A>>(&mut self, interface_index: u32) -> CoreResult<()> {
        if !self.state.registered.contains_key(F::NAME) {
            return Err(CoreError::internal(format!(
                "feature is not registered: {}",
                F::NAME
            )));
        }
        if let Some(enabled) = self.state.enabled.get_mut(&interface_index) {
            enabled.retain(|enabled| enabled.name != F::NAME);
            if enabled.is_empty() {
                self.state.enabled.remove(&interface_index);
            }
        }
        self.publish()?;
        Ok(())
    }

    #[inline]
    pub fn is_feature_enabled<F: Feature<A>>(&self, interface_index: u32) -> bool {
        self.state
            .enabled
            .get(&interface_index)
            .is_some_and(|enabled| enabled.iter().any(|enabled| enabled.name == F::NAME))
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
        self.state.rebuild()?;
        self.inner.snapshot.store(Arc::new(self.state.clone()));
        Ok(())
    }
}

impl FeatureArcInner {
    #[inline]
    fn new() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(FeatureArcSnapshot::default()),
        }
    }
}

impl FeatureArcSnapshot {
    fn rebuild(&mut self) -> CoreResult<()> {
        self.feature_order = self.sorted_feature_order()?;
        let feature_order = self.feature_order.clone();
        let mut chains = HashMap::with_capacity(self.enabled.len());
        for (interface_index, enabled) in &self.enabled {
            let enabled = enabled
                .iter()
                .map(|enabled| (enabled.name.as_str(), enabled.config.clone()))
                .collect::<HashMap<_, _>>();
            let chain_features = feature_order
                .iter()
                .filter_map(|name| {
                    enabled
                        .get(name.as_str())
                        .map(|config| (name.clone(), config.clone()))
                })
                .collect::<Vec<_>>();
            let mut steps = Vec::with_capacity(chain_features.len());
            for (name, config) in chain_features {
                let node = self
                    .registered
                    .get(&name)
                    .ok_or_else(|| CoreError::internal("feature chain references missing node"))?
                    .node;
                steps.push(FeatureArcStep { node, config });
            }
            chains.insert(*interface_index, FeatureArcChain { steps });
        }
        self.chains = chains;
        Ok(())
    }

    fn sorted_feature_order(&self) -> CoreResult<Vec<String>> {
        let mut edges = self
            .registered
            .keys()
            .map(|name| (name.clone(), Vec::<String>::new()))
            .collect::<HashMap<_, _>>();
        let mut indegree = self
            .registered
            .keys()
            .map(|name| (name.clone(), 0usize))
            .collect::<HashMap<_, _>>();
        let mut seen_edges = HashSet::<(String, String)>::new();

        for (name, registration) in &self.registered {
            for before in &registration.order.before {
                if self.registered.contains_key(before) {
                    add_feature_order_edge(
                        &mut edges,
                        &mut indegree,
                        &mut seen_edges,
                        name,
                        before,
                    );
                }
            }
            for after in &registration.order.after {
                if self.registered.contains_key(after) {
                    add_feature_order_edge(&mut edges, &mut indegree, &mut seen_edges, after, name);
                }
            }
        }

        let mut selected = HashSet::<String>::new();
        let mut sorted = Vec::with_capacity(self.registered.len());
        while sorted.len() < self.registered.len() {
            let Some(next) = self
                .feature_order
                .iter()
                .filter(|name| self.registered.contains_key(*name))
                .filter(|name| !selected.contains(*name))
                .filter(|name| indegree.get(*name).copied().unwrap_or_default() == 0)
                .min_by_key(|name| {
                    self.registered
                        .get(*name)
                        .map(|registration| registration.ordinal)
                        .unwrap_or(usize::MAX)
                })
                .cloned()
            else {
                return Err(CoreError::internal(
                    "feature order constraints contain a cycle",
                ));
            };

            selected.insert(next.clone());
            sorted.push(next.clone());
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

fn add_feature_order_edge(
    edges: &mut HashMap<String, Vec<String>>,
    indegree: &mut HashMap<String, usize>,
    seen_edges: &mut HashSet<(String, String)>,
    from: &str,
    to: &str,
) {
    if from == to || !seen_edges.insert((from.to_owned(), to.to_owned())) {
        return;
    }
    edges
        .entry(from.to_owned())
        .or_default()
        .push(to.to_owned());
    *indegree.entry(to.to_owned()).or_default() += 1;
}

#[hammer_component_macros::node_next]
pub enum RouteMatchNext {
    Lookup,
}

pub struct RouteMatchNode<R> {
    router: R,
    next: [NodeId; RouteMatchNext::COUNT],
}

impl<R> RouteMatchNode<R> {
    pub fn new(router: R, next: [NodeId; RouteMatchNext::COUNT]) -> Self {
        Self { router, next }
    }
}

impl<R, T, G> Node<G> for RouteMatchNode<R>
where
    R: Deref<Target = T>,
    T: Router + ?Sized,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut cursor = frame.batch_cursor(runtime.preferred_frame_batch_width());
        cursor.prefetch_next(runtime);
        while let Some(batch) = cursor.next() {
            cursor.prefetch_next(runtime);
            for index in batch.indices() {
                let mut buffer = runtime.get_buffer_mut(index)?;
                let metadata = buffer.metadata_mut();
                self.router.prepare_route_metadata(metadata)?;
                let decision = self.router.match_route(metadata)?;
                metadata.route_decision = Some(decision);
            }
        }
        Ok(NodeResult::next_current(
            self.next[RouteMatchNext::Lookup.slot()],
        ))
    }
}

impl<R, T, G> InternalNode<G> for RouteMatchNode<R>
where
    R: Deref<Target = T>,
    T: Router + ?Sized,
{
}
