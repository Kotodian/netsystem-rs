use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Network, Node, NodeId, NodeResult, RouteDecision,
    RouteMetadata, RouteTarget, Router,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_runtime::spawn::DataRuntime;
use hammer_service::data_plane::{
    DropNode, Feature, FeatureArcControl, FeatureArcSpec, RouteMatchNext, RouteMatchNode,
};

struct StaticRouter {
    decision: RouteDecision,
    prepare_count: AtomicUsize,
    match_count: AtomicUsize,
}

impl StaticRouter {
    fn new(decision: RouteDecision) -> Self {
        Self {
            decision,
            prepare_count: AtomicUsize::new(0),
            match_count: AtomicUsize::new(0),
        }
    }
}

impl Lifecycle for StaticRouter {
    fn name(&self) -> &str {
        "static-router"
    }

    fn start(&self, _stage: StartStage) -> CoreResult<()> {
        Ok(())
    }

    fn close(&self) -> CoreResult<()> {
        Ok(())
    }
}

impl Router for StaticRouter {
    fn reset_network(&self) {}

    fn match_route(&self, _metadata: &mut RouteMetadata) -> CoreResult<RouteDecision> {
        self.match_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.decision.clone())
    }

    fn prepare_route_metadata(&self, metadata: &mut RouteMetadata) -> CoreResult<()> {
        self.prepare_count.fetch_add(1, Ordering::SeqCst);
        metadata.protocol = "prepared".to_owned();
        Ok(())
    }

    fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<Duration> {
        None
    }

    fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
        false
    }
}

struct DecisionSinkNode {
    decisions: Rc<RefCell<Vec<RouteDecision>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TestFeatureArc {
    Probe,
}

impl FeatureArcSpec for TestFeatureArc {}

struct ProbeFeatureNode;

impl Feature<TestFeatureArc> for ProbeFeatureNode {
    #[inline]
    fn id() -> TestFeatureArc {
        TestFeatureArc::Probe
    }
}

impl Node<TestNode> for ProbeFeatureNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for ProbeFeatureNode {}

impl Node<TestNode> for DecisionSinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for buffer in frame.drain_pending() {
            let metadata = runtime.metadata(buffer)?;
            let decision = metadata
                .route_decision
                .ok_or_else(|| CoreError::internal("missing route decision"))?;
            self.decisions.borrow_mut().push(decision);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

enum TestNode {
    DecisionSink(DecisionSinkNode),
    Drop(DropNode),
    ProbeFeature(ProbeFeatureNode),
    RouteMatch(RouteMatchNode<Arc<StaticRouter>>),
}

impl From<DropNode> for TestNode {
    fn from(node: DropNode) -> Self {
        Self::Drop(node)
    }
}

impl From<RouteMatchNode<Arc<StaticRouter>>> for TestNode {
    fn from(node: RouteMatchNode<Arc<StaticRouter>>) -> Self {
        Self::RouteMatch(node)
    }
}

impl From<ProbeFeatureNode> for TestNode {
    fn from(node: ProbeFeatureNode) -> Self {
        Self::ProbeFeature(node)
    }
}

impl Node<TestNode> for TestNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::DecisionSink(node) => node.process(runtime, frame),
            Self::Drop(node) => node.process(runtime, frame),
            Self::ProbeFeature(node) => node.process(runtime, frame),
            Self::RouteMatch(node) => node.process(runtime, frame),
        }
    }
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode<TestNode>,
{
    let _ = node;
}

#[test]
fn route_match_node_is_service_internal_node() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 1, 2);
    let decisions = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime
        .nodes()
        .register(TestNode::DecisionSink(DecisionSinkNode {
            decisions: Rc::clone(&decisions),
        }));
    let router = Arc::new(StaticRouter::new(RouteDecision::Route {
        target: RouteTarget::Outbound("direct".to_owned()),
    }));
    let route_match = RouteMatchNode::new(Arc::clone(&router), RouteMatchNext::nodes(sink));
    assert_internal_node(&route_match);
    let route_match = runtime.nodes().register_internal(route_match);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(
            RouteMetadata {
                inbound: "tun".to_owned(),
                network: Network::Udp,
                ..Default::default()
            },
            b"packet",
        )
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(route_match, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(router.prepare_count.load(Ordering::SeqCst), 1);
    assert_eq!(router.match_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        &*decisions.borrow(),
        &[RouteDecision::Route {
            target: RouteTarget::Outbound("direct".to_owned())
        }]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn drop_node_frees_frame_buffers() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(64, 4, 2, 4);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first");
    let second = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(first)
        .expect("push first");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(second)
        .expect("push second");

    assert_eq!(runtime.in_use_buffers(), 2);
    assert!(runtime.schedule_frame(drop, frame).expect("schedule drop"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn feature_arc_updates_run_through_configured_runtime_data_plane_barrier() {
    let data_runtime =
        DataRuntime::new(1, "feature-arc-barrier-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 1, 2);
    let feature: NodeId = runtime.nodes().register_internal(ProbeFeatureNode);
    let mut control =
        FeatureArcControl::<TestFeatureArc>::new().with_data_plane_barrier(barrier.clone());

    control
        .register_feature::<ProbeFeatureNode>(feature)
        .expect("register feature");
    control
        .enable_feature::<ProbeFeatureNode>(7)
        .expect("enable feature");
    control
        .disable_feature::<ProbeFeatureNode>(7)
        .expect("disable feature");

    assert_eq!(barrier.sync_count(), 3);
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn feature_arc_publish_requires_configured_runtime_data_plane_barrier() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 1, 2);
    let feature: NodeId = runtime.nodes().register_internal(ProbeFeatureNode);
    let mut control = FeatureArcControl::<TestFeatureArc>::new();

    let err = control
        .register_feature::<ProbeFeatureNode>(feature)
        .expect_err("feature arc publish should require barrier");

    assert!(
        err.to_string()
            .contains("feature arc publish requires data-plane barrier")
    );
}
