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
use hammer_service::data_plane::{RouteDispatchNode, RouteMatchNode};

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

struct SinkNode {
    payloads: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Node<TestNode> for SinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for buffer in frame.drain_pending() {
            self.payloads
                .borrow_mut()
                .push(runtime.copy_current_chain(buffer)?);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

enum TestNode {
    DecisionSink(DecisionSinkNode),
    Sink(SinkNode),
    RouteMatch(RouteMatchNode<Arc<StaticRouter>>),
    RouteDispatch(RouteDispatchNode),
}

impl From<RouteMatchNode<Arc<StaticRouter>>> for TestNode {
    fn from(node: RouteMatchNode<Arc<StaticRouter>>) -> Self {
        Self::RouteMatch(node)
    }
}

impl From<RouteDispatchNode> for TestNode {
    fn from(node: RouteDispatchNode) -> Self {
        Self::RouteDispatch(node)
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
            Self::Sink(node) => node.process(runtime, frame),
            Self::RouteMatch(node) => node.process(runtime, frame),
            Self::RouteDispatch(node) => node.process(runtime, frame),
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
    let route_match = RouteMatchNode::new(Arc::clone(&router), sink);
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
        .with_frame_mut(frame, |frame| frame.push_index(buffer))
        .expect("mutate frame")
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
fn route_dispatch_node_uses_explicit_reject_output() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(16, 8, 4, 4);
    let direct_payloads = Rc::new(RefCell::new(Vec::new()));
    let block_payloads = Rc::new(RefCell::new(Vec::new()));
    let drop_payloads = Rc::new(RefCell::new(Vec::new()));
    let direct = runtime.nodes().register(TestNode::Sink(SinkNode {
        payloads: Rc::clone(&direct_payloads),
    }));
    let block = runtime.nodes().register(TestNode::Sink(SinkNode {
        payloads: Rc::clone(&block_payloads),
    }));
    let drop = runtime.nodes().register(TestNode::Sink(SinkNode {
        payloads: Rc::clone(&drop_payloads),
    }));
    let dispatch = RouteDispatchNode::new()
        .with_outbound("direct", direct)
        .with_outbound("block", block)
        .with_reject(drop);
    assert_internal_node(&dispatch);
    let dispatch = runtime.nodes().register_internal(dispatch);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    for (decision, payload) in [
        (
            RouteDecision::Route {
                target: RouteTarget::Outbound("direct".to_owned()),
            },
            b"first".as_slice(),
        ),
        (
            RouteDecision::Route {
                target: RouteTarget::Outbound("block".to_owned()),
            },
            b"second".as_slice(),
        ),
        (
            RouteDecision::Reject {
                method: "drop".to_owned(),
            },
            b"reject".as_slice(),
        ),
    ] {
        let buffer = runtime
            .alloc_index_with_bytes(
                RouteMetadata {
                    route_decision: Some(decision),
                    ..Default::default()
                },
                payload,
            )
            .expect("alloc packet");
        runtime
            .with_frame_mut(frame, |frame| frame.push_index(buffer))
            .expect("mutate frame")
            .expect("push packet");
    }

    assert!(runtime.schedule_frame(dispatch, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(&*direct_payloads.borrow(), &[b"first".to_vec()]);
    assert_eq!(&*block_payloads.borrow(), &[b"second".to_vec()]);
    assert_eq!(&*drop_payloads.borrow(), &[b"reject".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}
