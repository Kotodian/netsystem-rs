use std::sync::atomic::{AtomicU64, Ordering};

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DriverNode, InternalNode, Node, NodeDescriptor, NodeHandle,
    NodeId, NodeKind, NodeNext, NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData,
    NodeState, TraceFormatter,
};
use hammer_core::error::{CoreError, CoreResult};

static NODE_CALLS_BY_WORD: [AtomicU64; 128] = [const { AtomicU64::new(0) }; 128];

fn reset_calls(word: u64) {
    NODE_CALLS_BY_WORD[word as usize].store(0, Ordering::SeqCst);
}

fn calls_for(word: u64) -> u64 {
    NODE_CALLS_BY_WORD[word as usize].load(Ordering::SeqCst)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TestNext {
    Default,
    Alternate,
}

impl NodeNext for TestNext {
    const COUNT: usize = 2;

    #[inline(always)]
    fn slot(self) -> usize {
        match self {
            Self::Default => 0,
            Self::Alternate => 1,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DescriptorNode {
    process: NodeProcessFn,
    runtime_data: NodeRuntimeData,
    registration: NodeRegistration,
    next: [NodeId; TestNext::COUNT],
    initial_next_count: usize,
    trace: Option<TraceFormatter>,
}

impl DescriptorNode {
    fn plain(process: NodeProcessFn, runtime_data: NodeRuntimeData) -> Self {
        Self {
            process,
            runtime_data,
            registration: NodeRegistration::Plain,
            next: [NodeId::new(0), NodeId::new(0)],
            initial_next_count: 0,
            trace: None,
        }
    }

    fn next(
        name: &'static str,
        process: NodeProcessFn,
        runtime_data: NodeRuntimeData,
        next: [NodeId; TestNext::COUNT],
    ) -> Self {
        Self {
            process,
            runtime_data,
            registration: NodeRegistration::next(name, TestNext::COUNT),
            next,
            initial_next_count: TestNext::COUNT,
            trace: None,
        }
    }

    fn sibling(
        name: &'static str,
        sibling_of: &'static str,
        process: NodeProcessFn,
        runtime_data: NodeRuntimeData,
    ) -> Self {
        Self {
            process,
            runtime_data,
            registration: NodeRegistration::sibling_of(name, sibling_of),
            next: [NodeId::new(0), NodeId::new(0)],
            initial_next_count: 0,
            trace: None,
        }
    }

    fn with_trace(mut self, trace: TraceFormatter) -> Self {
        self.trace = Some(trace);
        self
    }

    fn with_initial_next_count(mut self, initial_next_count: usize) -> Self {
        self.initial_next_count = initial_next_count;
        self
    }
}

impl Node for DescriptorNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "descriptor test nodes must run through their function slot",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        self.process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }

    #[inline]
    fn node_registration(&self) -> NodeRegistration {
        self.registration
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next[..self.initial_next_count]
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        self.trace
    }
}

impl InternalNode for DescriptorNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration {
        self.registration
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next[..self.initial_next_count]
    }
}

impl DriverNode for DescriptorNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration {
        self.registration
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next[..self.initial_next_count]
    }
}

struct ProcessOnlyNode;

impl Node for ProcessOnlyNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Ok(NodeResult::drop())
    }
}

impl InternalNode for ProcessOnlyNode {}

fn count_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    NODE_CALLS_BY_WORD[data.usize_word(0)?].fetch_add(1, Ordering::SeqCst);
    for buffer in frame.drain_pending() {
        runtime.free_index(buffer);
    }
    Ok(NodeResult::drop())
}

fn forward_default_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    _frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    NODE_CALLS_BY_WORD[data.usize_word(0)?].fetch_add(1, Ordering::SeqCst);
    Ok(NodeResult::next_current(
        runtime.current_node_next(TestNext::Default)?,
    ))
}

fn trace_formatter(bytes: &[u8]) -> String {
    format!("descriptor trace bytes={}", bytes.len())
}

#[test]
fn register_internal_uses_descriptor_function_and_runtime_data() {
    reset_calls(42);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let node = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([42, 0, 0, 0]),
    ));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(node, frame).expect("schedule"));
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(42), 1);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn register_driver_preserves_old_spelling_for_descriptor_nodes() {
    reset_calls(7);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let node = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([7, 0, 0, 0]),
    ));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(node, frame).expect("schedule"));
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(7), 1);
}

#[test]
fn runtime_exposes_node_kind_and_state() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let internal = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));

    assert_eq!(
        runtime.nodes().node_kind(internal).unwrap(),
        NodeKind::Internal
    );
    assert_eq!(runtime.nodes().node_kind(driver).unwrap(), NodeKind::Driver);
    assert_eq!(
        runtime.nodes().node_state(driver).unwrap(),
        NodeState::Polling
    );

    runtime
        .nodes()
        .set_node_state(driver, NodeState::Interrupt)
        .expect("set driver interrupt state");
    assert_eq!(
        runtime.nodes().node_state(driver).unwrap(),
        NodeState::Interrupt
    );
}

#[test]
fn schedule_empty_frame_runs_driver_without_packet_vectors() {
    reset_calls(21);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([21, 0, 0, 0]),
    ));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule empty frame");
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(21), 1);
    assert_eq!(runtime.packet_buffers().frames_in_use(), 0);
}

#[test]
fn schedule_polling_driver_nodes_schedules_only_polling_drivers() {
    reset_calls(31);
    reset_calls(32);
    reset_calls(33);

    let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
    let polling_driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([31, 0, 0, 0]),
    ));
    let interrupt_driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([32, 0, 0, 0]),
    ));
    let internal = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([33, 0, 0, 0]),
    ));

    runtime
        .nodes()
        .set_node_state(interrupt_driver, NodeState::Interrupt)
        .expect("set interrupt driver state");
    runtime
        .nodes()
        .set_node_state(internal, NodeState::Polling)
        .expect("set internal polling state");

    assert_eq!(
        runtime
            .schedule_polling_driver_nodes()
            .expect("schedule polling drivers"),
        1
    );
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(31), 1);
    assert_eq!(calls_for(32), 0);
    assert_eq!(calls_for(33), 0);
    assert_eq!(
        runtime.nodes().node_state(polling_driver).unwrap(),
        NodeState::Polling
    );
}

#[test]
fn interrupt_pending_coalesces_empty_driver_dispatch() {
    reset_calls(31);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([31, 0, 0, 0]),
    ));
    runtime
        .nodes()
        .set_node_state(driver, NodeState::Interrupt)
        .expect("set interrupt state");

    assert!(
        runtime
            .set_node_interrupt_pending(driver)
            .expect("first interrupt schedules")
    );
    assert!(
        !runtime
            .set_node_interrupt_pending(driver)
            .expect("second interrupt coalesces")
    );
    assert_eq!(runtime.nodes().pending_len(), 1);

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
    assert_eq!(calls_for(31), 1);
    assert_eq!(runtime.nodes().pending_len(), 0);
    assert_eq!(runtime.packet_buffers().frames_in_use(), 0);
}

#[test]
fn disabled_driver_interrupt_does_not_schedule() {
    reset_calls(37);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([37, 0, 0, 0]),
    ));
    runtime
        .nodes()
        .set_node_state(driver, NodeState::Disabled)
        .expect("disable driver");

    assert!(
        !runtime
            .set_node_interrupt_pending(driver)
            .expect("disabled interrupt is ignored")
    );
    assert_eq!(runtime.nodes().pending_len(), 0);
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 0);
    assert_eq!(calls_for(37), 0);
}

#[test]
fn disabled_node_skips_already_queued_empty_frame() {
    reset_calls(41);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([41, 0, 0, 0]),
    ));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule empty frame before disable");
    runtime
        .nodes()
        .set_node_state(driver, NodeState::Disabled)
        .expect("disable queued node");

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 0);
    assert_eq!(calls_for(41), 0);
    assert_eq!(runtime.packet_buffers().frames_in_use(), 0);
}

#[test]
fn descriptor_registration_keeps_name_next_slots_trace_and_siblings() {
    let runtime = DataPlaneRuntime::with_capacities(64, 8, 8, 4);
    let default = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let alternate = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let owner = runtime.nodes().register_internal(
        DescriptorNode::next(
            "descriptor-owner",
            forward_default_process,
            NodeRuntimeData::from_words([3, 0, 0, 0]),
            [default, alternate],
        )
        .with_trace(trace_formatter),
    );
    let sibling = runtime.nodes().register_internal(DescriptorNode::sibling(
        "descriptor-sibling",
        "descriptor-owner",
        forward_default_process,
        NodeRuntimeData::from_words([5, 0, 0, 0]),
    ));

    assert_eq!(runtime.node_by_name("descriptor-owner"), Some(owner));
    assert_eq!(runtime.node_by_name("descriptor-sibling"), Some(sibling));
    let formatter = runtime
        .nodes()
        .node_trace_formatter(owner)
        .unwrap()
        .expect("trace formatter");
    assert_eq!(formatter(b"abc"), "descriptor trace bytes=3");
    assert_eq!(
        runtime.nodes().node_next(owner, TestNext::Default).unwrap(),
        default
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next(sibling, TestNext::Alternate)
            .unwrap(),
        alternate
    );
    assert_eq!(runtime.nodes().node_siblings(owner).unwrap(), vec![sibling]);
    assert_eq!(runtime.nodes().node_siblings(sibling).unwrap(), vec![owner]);
}

#[test]
fn default_node_process_path_registers_but_fails_explicitly_at_dispatch() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let node = runtime.nodes().register_internal(ProcessOnlyNode);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(node, frame).expect("schedule"));
    let err = runtime
        .run_ready_nodes()
        .expect_err("default node process must fail explicitly");

    assert!(
        err.to_string()
            .contains("descriptor is missing a packet process function")
    );
}

#[test]
fn descriptor_next_node_runs_with_runtime_resolved_next_slot() {
    reset_calls(11);
    reset_calls(13);
    reset_calls(17);
    let runtime = DataPlaneRuntime::with_capacities(64, 8, 8, 4);
    let default = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([11, 0, 0, 0]),
    ));
    let alternate = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([13, 0, 0, 0]),
    ));
    let owner = runtime.nodes().register_internal(DescriptorNode::next(
        "descriptor-runtime-next",
        forward_default_process,
        NodeRuntimeData::from_words([17, 0, 0, 0]),
        [default, alternate],
    ));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(owner, frame).expect("schedule"));
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 2);

    assert_eq!(calls_for(17), 1);
    assert_eq!(calls_for(11), 1);
    assert_eq!(calls_for(13), 0);
}

#[test]
fn descriptor_registration_validates_declared_next_shape() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let next = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));

    let err = runtime
        .nodes()
        .try_register_internal(
            DescriptorNode::next(
                "bad-next-count",
                count_process,
                NodeRuntimeData::empty(),
                [next, next],
            )
            .with_initial_next_count(1),
        )
        .expect_err("next count mismatch must fail");

    assert!(err.to_string().contains("node initial next count mismatch"));
}

#[test]
fn descriptor_registration_with_handle_registers_handle_once() {
    const HANDLE: NodeHandle = NodeHandle::new(9);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let node = runtime
        .nodes()
        .register_internal_with_handle(
            HANDLE,
            DescriptorNode::plain(count_process, NodeRuntimeData::empty()),
        )
        .expect("register handle");

    assert_eq!(runtime.nodes().node_for_handle(HANDLE).unwrap(), node);
    let err = runtime
        .nodes()
        .register_internal_with_handle(
            HANDLE,
            DescriptorNode::plain(count_process, NodeRuntimeData::empty()),
        )
        .expect_err("duplicate handle must fail");
    assert!(err.to_string().contains("node handle already registered"));
}

#[test]
fn node_descriptor_exposes_public_snapshot_accessors() {
    reset_calls(99);
    let next = [NodeId::new(1), NodeId::new(2)];
    let descriptor = NodeDescriptor::new(
        count_process,
        NodeRuntimeData::from_words([99, 0, 0, 0]),
        NodeRegistration::next("manual-descriptor", TestNext::COUNT),
        &next,
        Some(trace_formatter),
    );

    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    runtime
        .with_frame_mut(frame, |frame| {
            descriptor.process()(&runtime, descriptor.runtime_data(), frame)
        })
        .expect("get frame")
        .expect("process through descriptor function");

    assert_eq!(calls_for(99), 1);
    assert_eq!(descriptor.runtime_data().word(0), 99);
    assert_eq!(
        descriptor.registration(),
        NodeRegistration::next("manual-descriptor", TestNext::COUNT)
    );
    assert_eq!(descriptor.initial_nexts(), &next);
    assert_eq!(
        descriptor.trace_formatter().expect("trace formatter")(b"abc"),
        "descriptor trace bytes=3"
    );
}

fn push_packet(runtime: &DataPlaneRuntime, frame: hammer_adapter::FrameIndex, payload: &[u8]) {
    let buffer = runtime
        .alloc_index_with_bytes(payload)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("get frame")
        .push_index(buffer)
        .expect("push packet");
}
