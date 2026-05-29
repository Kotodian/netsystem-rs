use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Network, Node, NodeId, NodeResult, RouteDecision,
    RouteDispatchNode, RouteMetadata, RouteTarget, Router, RouterNode,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::lifecycle::{Lifecycle, StartStage};

#[derive(Default)]
struct WakeCounter {
    wakes: AtomicUsize,
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

struct ForwardNode {
    next: NodeId,
    trace: Rc<RefCell<Vec<&'static str>>>,
}

impl Node<TestNode> for ForwardNode {
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.trace.borrow_mut().push("forward");
        assert_eq!(frame.next_node(), None);
        Ok(NodeResult::next_current(self.next))
    }
}

struct SinkNode {
    trace: Rc<RefCell<Vec<&'static str>>>,
    payloads: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Node<TestNode> for SinkNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.trace.borrow_mut().push("sink");
        for buffer in frame.drain_pending() {
            self.payloads
                .borrow_mut()
                .push(runtime.copy_current_chain(buffer)?);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

struct DecisionSinkNode {
    decisions: Rc<RefCell<Vec<RouteDecision>>>,
}

impl Node<TestNode> for DecisionSinkNode {
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

struct CountNode {
    count: Rc<Cell<usize>>,
}

impl Node<TestNode> for CountNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.count.set(self.count.get() + 1);
        for buffer in frame.drain_pending() {
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

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

enum TestNode {
    Forward(ForwardNode),
    Sink(SinkNode),
    DecisionSink(DecisionSinkNode),
    Count(CountNode),
    Router(RouterNode<Arc<StaticRouter>>),
    RouteDispatch(RouteDispatchNode),
}

impl Node<TestNode> for TestNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::Forward(node) => node.process(runtime, frame),
            Self::Sink(node) => node.process(runtime, frame),
            Self::DecisionSink(node) => node.process(runtime, frame),
            Self::Count(node) => node.process(runtime, frame),
            Self::Router(node) => node.process(runtime, frame),
            Self::RouteDispatch(node) => node.process(runtime, frame),
        }
    }
}

#[test]
fn node_runtime_dispatches_pending_frame_to_next_node() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let trace = Rc::new(RefCell::new(Vec::new()));
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&payloads),
    }));
    let forward = runtime.nodes().register(TestNode::Forward(ForwardNode {
        next: sink,
        trace: Rc::clone(&trace),
    }));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    runtime
        .with_frame_mut(frame, |frame| frame.push_index(buffer))
        .expect("mutate frame")
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(forward, frame)
            .expect("schedule frame")
    );

    assert_eq!(runtime.nodes().pending_len(), 1);
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(&*trace.borrow(), &["forward", "sink"]);
    assert_eq!(&*payloads.borrow(), &[b"packet".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_does_not_schedule_empty_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let count = Rc::new(Cell::new(0));
    let node = runtime.nodes().register(TestNode::Count(CountNode {
        count: Rc::clone(&count),
    }));
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    assert!(!runtime.schedule_frame(node, frame).expect("schedule empty"));
    assert_eq!(runtime.nodes().pending_len(), 0);
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 0);
    assert_eq!(count.get(), 0);

    runtime.free_frame_index(frame).expect("free empty frame");
}

#[test]
fn node_runtime_ready_future_wakes_on_local_schedule() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let count = Rc::new(Cell::new(0));
    let node = runtime.nodes().register(TestNode::Count(CountNode {
        count: Rc::clone(&count),
    }));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    runtime
        .with_frame_mut(frame, |frame| frame.push_index(buffer))
        .expect("mutate frame")
        .expect("push packet");
    let wake_counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&wake_counter));
    let mut context = Context::from_waker(&waker);
    let mut ready = runtime.nodes().ready();

    assert!(matches!(
        Pin::new(&mut ready).poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 0);

    assert!(runtime.schedule_frame(node, frame).expect("schedule frame"));

    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        Pin::new(&mut ready).poll(&mut context),
        Poll::Ready(())
    ));
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(count.get(), 1);
}

#[test]
fn router_node_stores_decision_in_metadata_and_forwards_frame() {
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
    let router_node = runtime
        .nodes()
        .register(TestNode::Router(RouterNode::new(Arc::clone(&router), sink)));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(
            RouteMetadata {
                inbound: "in".to_owned(),
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
            .schedule_frame(router_node, frame)
            .expect("schedule router")
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
fn route_dispatch_node_splits_frame_by_route_decision() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(16, 8, 4, 4);
    let trace = Rc::new(RefCell::new(Vec::new()));
    let direct_payloads = Rc::new(RefCell::new(Vec::new()));
    let block_payloads = Rc::new(RefCell::new(Vec::new()));
    let direct = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&direct_payloads),
    }));
    let block = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&block_payloads),
    }));
    let dispatch = runtime.nodes().register(TestNode::RouteDispatch(
        RouteDispatchNode::new()
            .with_outbound("direct", direct)
            .with_outbound("block", block),
    ));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    for (target, payload) in [
        ("direct", b"first".as_slice()),
        ("block", b"second".as_slice()),
        ("direct", b"third".as_slice()),
    ] {
        let buffer = runtime
            .alloc_index_with_bytes(
                RouteMetadata {
                    route_decision: Some(RouteDecision::Route {
                        target: RouteTarget::Outbound(target.to_owned()),
                    }),
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
    let rejected = runtime
        .alloc_index_with_bytes(
            RouteMetadata {
                route_decision: Some(RouteDecision::Reject {
                    method: "drop".to_owned(),
                }),
                ..Default::default()
            },
            b"reject",
        )
        .expect("alloc rejected packet");
    runtime
        .with_frame_mut(frame, |frame| frame.push_index(rejected))
        .expect("mutate frame")
        .expect("push rejected packet");

    assert!(runtime.schedule_frame(dispatch, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(
        &*direct_payloads.borrow(),
        &[b"first".to_vec(), b"third".to_vec()]
    );
    assert_eq!(&*block_payloads.borrow(), &[b"second".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}
