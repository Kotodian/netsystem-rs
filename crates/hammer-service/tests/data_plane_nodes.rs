use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Network, Node, NodeResult, RouteDecision,
    RouteMetadata, RouteTarget, Router,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_service::data_plane::{DropNode, RouteMatchNext, RouteMatchNode};

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
