use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use hammer_adapter::{
    BufferFrame, BufferNodeError, BufferPoolArena, DataPlaneHandoff, DataPlaneRuntime,
    DataWorkerId, DriverNode, InternalNode, Node, NodeHandle, NodeId, NodeResult, RouteMetadata,
};
use hammer_core::error::CoreResult;

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
    #[inline(always)]
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

impl DriverNode<TestNode> for ForwardNode {}

struct SourceDriverNode {
    next: NodeId,
    payload: Vec<u8>,
}

impl Node<TestNode> for SourceDriverNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        assert!(!frame.has_pending());
        let buffer = runtime.alloc_index_with_bytes(RouteMetadata::default(), &self.payload)?;
        frame.push_index(buffer)?;
        Ok(NodeResult::next_current(self.next))
    }
}

impl DriverNode<TestNode> for SourceDriverNode {}

struct SinkNode {
    trace: Rc<RefCell<Vec<&'static str>>>,
    payloads: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Node<TestNode> for SinkNode {
    #[inline(always)]
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

impl DriverNode<TestNode> for SinkNode {}

struct CountNode {
    count: Rc<Cell<usize>>,
}

impl Node<TestNode> for CountNode {
    #[inline(always)]
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

impl InternalNode<TestNode> for CountNode {}

struct ErrorNode {
    code: u16,
    errors: Rc<RefCell<Vec<Option<BufferNodeError>>>>,
}

impl Node<TestNode> for ErrorNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for buffer in frame.drain_pending() {
            let mut buffer_ref = runtime.get_buffer_mut(buffer)?;
            let error = runtime.record_current_node_error(self.code)?;
            buffer_ref.set_node_error(error);
            self.errors.borrow_mut().push(buffer_ref.node_error());
            drop(buffer_ref);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for ErrorNode {}

struct HandoffNode {
    target_worker: DataWorkerId,
    target: NodeHandle,
}

impl Node<TestNode> for HandoffNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        runtime.handoff_frame(self.target_worker, self.target, frame)?;
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for HandoffNode {}

struct PointerSinkNode {
    payloads: Rc<RefCell<Vec<Vec<u8>>>>,
    current_ptrs: Rc<RefCell<Vec<usize>>>,
}

impl Node<TestNode> for PointerSinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for buffer in frame.drain_pending() {
            let current_ptr = runtime.current_ptr(buffer)? as usize;
            self.current_ptrs.borrow_mut().push(current_ptr);
            self.payloads
                .borrow_mut()
                .push(runtime.copy_current_chain(buffer)?);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for PointerSinkNode {}

enum TestNode {
    Forward(ForwardNode),
    SourceDriver(SourceDriverNode),
    Sink(SinkNode),
    Count(CountNode),
    Error(ErrorNode),
    Handoff(HandoffNode),
    PointerSink(PointerSinkNode),
}

impl From<ForwardNode> for TestNode {
    fn from(node: ForwardNode) -> Self {
        Self::Forward(node)
    }
}

impl From<SinkNode> for TestNode {
    fn from(node: SinkNode) -> Self {
        Self::Sink(node)
    }
}

impl From<SourceDriverNode> for TestNode {
    fn from(node: SourceDriverNode) -> Self {
        Self::SourceDriver(node)
    }
}

impl From<CountNode> for TestNode {
    fn from(node: CountNode) -> Self {
        Self::Count(node)
    }
}

impl From<ErrorNode> for TestNode {
    fn from(node: ErrorNode) -> Self {
        Self::Error(node)
    }
}

impl From<HandoffNode> for TestNode {
    fn from(node: HandoffNode) -> Self {
        Self::Handoff(node)
    }
}

impl From<PointerSinkNode> for TestNode {
    fn from(node: PointerSinkNode) -> Self {
        Self::PointerSink(node)
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
            Self::Forward(node) => node.process(runtime, frame),
            Self::SourceDriver(node) => node.process(runtime, frame),
            Self::Sink(node) => node.process(runtime, frame),
            Self::Count(node) => node.process(runtime, frame),
            Self::Error(node) => node.process(runtime, frame),
            Self::Handoff(node) => node.process(runtime, frame),
            Self::PointerSink(node) => node.process(runtime, frame),
        }
    }
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode<TestNode>,
{
    let _ = node;
}

fn assert_driver_node<I>(node: &I)
where
    I: DriverNode<TestNode>,
{
    let _ = node;
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
fn node_runtime_registers_internal_role_node() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let count = Rc::new(Cell::new(0));
    let node = CountNode {
        count: Rc::clone(&count),
    };
    assert_internal_node(&node);
    let node = runtime.nodes().register_internal(node);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    runtime
        .with_frame_mut(frame, |frame| frame.push_index(buffer))
        .expect("mutate frame")
        .expect("push packet");

    assert!(runtime.schedule_frame(node, frame).expect("schedule frame"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(count.get(), 1);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_counts_errors_per_runtime_node() {
    let first_runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let second_runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let first_errors = Rc::new(RefCell::new(Vec::new()));
    let second_errors = Rc::new(RefCell::new(Vec::new()));
    let first_node = first_runtime.nodes().register_internal(ErrorNode {
        code: 7,
        errors: Rc::clone(&first_errors),
    });
    let second_node = second_runtime.nodes().register_internal(ErrorNode {
        code: 7,
        errors: Rc::clone(&second_errors),
    });
    let first_frame = first_runtime.alloc_frame_index().expect("alloc frame");
    let first_buffer = first_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    first_runtime
        .get_frame_mut(first_frame)
        .expect("mutate frame")
        .push_index(first_buffer)
        .expect("push packet");

    assert!(
        first_runtime
            .schedule_frame(first_node, first_frame)
            .expect("schedule frame")
    );

    assert_eq!(first_runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(
        first_runtime
            .node_error_count(first_node, 7)
            .expect("first counter"),
        1
    );
    assert_eq!(
        second_runtime
            .node_error_count(second_node, 7)
            .expect("second counter"),
        0
    );
    assert_eq!(
        &*first_errors.borrow(),
        &[Some(BufferNodeError::new(first_node, 7))]
    );
    assert!(second_errors.borrow().is_empty());
}

#[test]
fn data_plane_handoff_moves_frame_between_workers_without_copying_payload_storage() {
    const SINK_HANDLE: NodeHandle = NodeHandle::new(1);
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 8, BufferPoolArena::with_capacity(64, 8));
    let first_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let current_ptrs = Rc::new(RefCell::new(Vec::new()));
    let sink = second_runtime
        .nodes()
        .register_internal_with_handle(
            SINK_HANDLE,
            PointerSinkNode {
                payloads: Rc::clone(&payloads),
                current_ptrs: Rc::clone(&current_ptrs),
            },
        )
        .expect("register sink handle");
    let handoff_node = first_runtime.nodes().register_internal(HandoffNode {
        target_worker: DataWorkerId::new(1),
        target: SINK_HANDLE,
    });
    let frame = first_runtime.alloc_frame_index().expect("alloc frame");
    let buffer = first_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    let original_ptr = first_runtime
        .get_buffer(buffer)
        .expect("buffer ref")
        .current_ptr() as usize;
    first_runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(
        first_runtime
            .schedule_frame(handoff_node, frame)
            .expect("schedule handoff")
    );

    assert_eq!(first_runtime.run_ready_nodes().expect("run source"), 1);
    assert_eq!(first_runtime.in_use_buffers(), 1);
    assert_eq!(second_runtime.run_ready_nodes().expect("run target"), 1);
    assert_eq!(
        sink,
        second_runtime.nodes().node_for_handle(SINK_HANDLE).unwrap()
    );
    assert_eq!(&*payloads.borrow(), &[b"packet".to_vec()]);
    assert_eq!(&*current_ptrs.borrow(), &[original_ptr]);
    assert_eq!(first_runtime.in_use_buffers(), 0);
    assert_eq!(second_runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_registers_driver_role_node() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let trace = Rc::new(RefCell::new(Vec::new()));
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&payloads),
    });
    let driver = ForwardNode {
        next: sink,
        trace: Rc::clone(&trace),
    };
    assert_driver_node(&driver);
    let driver = runtime.nodes().register_driver(driver);
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
            .schedule_frame(driver, frame)
            .expect("schedule frame")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(&*trace.borrow(), &["forward", "sink"]);
    assert_eq!(&*payloads.borrow(), &[b"packet".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_schedules_empty_driver_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let trace = Rc::new(RefCell::new(Vec::new()));
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&payloads),
    });
    let driver = runtime.nodes().register_driver(SourceDriverNode {
        next: sink,
        payload: b"packet".to_vec(),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(&*trace.borrow(), &["sink"]);
    assert_eq!(&*payloads.borrow(), &[b"packet".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
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
