use std::sync::atomic::{AtomicU64, Ordering};

use hammer_core::data_plane::{
    BufferFrame, NodeHandle, NodeId, NodeKind, NodeNext, NodeRegistration, NodeState,
};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, DriverNode, InternalNode,
    Node, NodeDescriptor, NodeEntry, NodeProcessFn, NodeResult, NodeRuntimeData, TraceFormatter,
    process_frame,
};

static NODE_CALLS_BY_WORD: [AtomicU64; 128] = [const { AtomicU64::new(0) }; 128];

fn reset_calls(word: u64) {
    NODE_CALLS_BY_WORD[word as usize].store(0, Ordering::SeqCst);
}

fn calls_for(word: u64) -> u64 {
    NODE_CALLS_BY_WORD[word as usize].load(Ordering::SeqCst)
}

fn test_runtime(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_pool_size: usize,
) -> DataPlaneRuntime {
    let buffers = DataPlaneBufferConfig {
        buffer_slot_capacity,
        buffer_slots,
        frame_slots: frame_pool_size,
        ..DataPlaneBufferConfig::default()
    };
    DataPlaneRuntime::new(DataPlaneRuntimeConfig { buffers })
}

fn init_graph_owner(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            count_process,
            NodeRuntimeData::empty(),
            NodeRegistration::next("init-graph-owner", 0),
            &[],
            None,
        ),
    )
}

fn init_graph_sibling(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            count_process,
            NodeRuntimeData::empty(),
            NodeRegistration::sibling_of("init-graph-sibling", "init-graph-owner"),
            &[],
            None,
        ),
    )
}

#[test]
fn init_graph_registers_sibling_owner_before_sibling() {
    let runtime = test_runtime(64, 4, 2);
    let entries = [
        NodeEntry {
            registration: NodeRegistration::sibling_of("init-graph-sibling", "init-graph-owner"),
            kind: NodeKind::Internal,
            init: init_graph_sibling,
        },
        NodeEntry {
            registration: NodeRegistration::next("init-graph-owner", 0),
            kind: NodeKind::Internal,
            init: init_graph_owner,
        },
    ];

    runtime
        .init_graph(&entries)
        .expect("owner must be registered before sibling regardless of linkme order");

    let owner = runtime
        .node_by_name("init-graph-owner")
        .expect("owner registered");
    let sibling = runtime
        .node_by_name("init-graph-sibling")
        .expect("sibling registered");
    assert_eq!(runtime.nodes().node_siblings(owner).unwrap(), vec![sibling]);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TestNext {
    Default,
    Alternate,
}

impl NodeNext for TestNext {
    #[inline(always)]
    fn slot(self) -> u16 {
        match self {
            Self::Default => 0,
            Self::Alternate => 1,
        }
    }
}

impl TestNext {
    const COUNT: usize = 2;
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
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        self.process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
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
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }
}

impl InternalNode for ProcessOnlyNode {}

fn count_process(_: &DataPlaneRuntime, data: NodeRuntimeData, _: &mut BufferFrame) -> NodeResult {
    let word = match data.usize_word(0) {
        Ok(w) => w,
        Err(_) => return NodeResult::drop(),
    };
    NODE_CALLS_BY_WORD[word].fetch_add(1, Ordering::SeqCst);
    NodeResult::drop()
}

fn forward_default_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let word = match data.usize_word(0) {
        Ok(w) => w,
        Err(_) => return NodeResult::drop(),
    };
    NODE_CALLS_BY_WORD[word].fetch_add(1, Ordering::SeqCst);
    process_frame!(runtime, frame, |_| TestNext::Default)
}

fn trace_formatter(bytes: &[u8]) -> String {
    format!("descriptor trace bytes={}", bytes.len())
}

#[test]
fn register_internal_uses_descriptor_function_and_runtime_data() {
    reset_calls(42);
    let runtime = test_runtime(64, 4, 2);
    let node = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([42, 0, 0, 0]),
    ));
    let mut frame = runtime.buffers().get_next_frame(node).expect("alloc frame");
    push_packet(&runtime, &mut frame, b"packet");

    runtime.put_next_frame(frame).expect("put next frame");
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(42), 1);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn register_driver_preserves_old_spelling_for_descriptor_nodes() {
    reset_calls(7);
    let runtime = test_runtime(64, 4, 2);
    let node = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([7, 0, 0, 0]),
    ));
    let mut frame = runtime.buffers().get_next_frame(node).expect("alloc frame");
    push_packet(&runtime, &mut frame, b"packet");

    runtime.put_next_frame(frame).expect("put next frame");
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(7), 1);
}

#[test]
fn runtime_exposes_node_kind_and_state() {
    let runtime = test_runtime(64, 4, 2);
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
    let runtime = test_runtime(64, 4, 2);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([21, 0, 0, 0]),
    ));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule empty frame");
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(21), 1);
    assert_eq!(runtime.buffers().frames_in_use(), 0);
}

#[test]
fn schedule_polling_driver_nodes_schedules_only_polling_drivers() {
    reset_calls(31);
    reset_calls(32);
    reset_calls(33);

    let runtime = test_runtime(64, 8, 8);
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
    reset_calls(34);
    let runtime = test_runtime(64, 4, 4);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([34, 0, 0, 0]),
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
    assert!(runtime.nodes().has_pending());

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
    assert_eq!(calls_for(34), 1);
    assert!(!runtime.nodes().has_pending());
    assert_eq!(runtime.buffers().frames_in_use(), 0);
}

#[test]
fn disabled_driver_interrupt_does_not_schedule() {
    reset_calls(37);
    let runtime = test_runtime(64, 4, 4);
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
    assert!(!runtime.nodes().has_pending());
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 0);
    assert_eq!(calls_for(37), 0);
}

#[test]
fn disabled_node_skips_already_queued_empty_frame() {
    reset_calls(41);
    let runtime = test_runtime(64, 4, 4);
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
    assert_eq!(runtime.buffers().frames_in_use(), 0);
}

#[test]
fn descriptor_registration_keeps_name_next_slots_trace_and_siblings() {
    let runtime = test_runtime(64, 8, 4);
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
fn default_node_process_path_registers_and_drops_gracefully() {
    let runtime = test_runtime(64, 4, 2);
    let node = runtime.nodes().register_internal(ProcessOnlyNode);
    let mut frame = runtime.buffers().get_next_frame(node).expect("alloc frame");
    push_packet(&runtime, &mut frame, b"packet");

    runtime.put_next_frame(frame).expect("put next frame");
    let result = runtime
        .run_ready_nodes()
        .expect("default node process must succeed");

    assert_eq!(result, 1);
}

#[test]
fn descriptor_next_node_runs_with_runtime_resolved_next_slot() {
    reset_calls(11);
    reset_calls(13);
    reset_calls(17);
    let runtime = test_runtime(64, 8, 4);
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
    let mut frame = runtime
        .buffers()
        .get_next_frame(owner)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, b"packet");

    runtime.put_next_frame(frame).expect("put next frame");
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 2);

    assert_eq!(calls_for(17), 1);
    assert_eq!(calls_for(11), 1);
    assert_eq!(calls_for(13), 0);
}

#[test]
fn descriptor_registration_validates_declared_next_shape() {
    let runtime = test_runtime(64, 4, 2);
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
    let runtime = test_runtime(64, 4, 2);
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

    let runtime = test_runtime(64, 4, 2);
    let mut frame = runtime
        .buffers()
        .get_next_frame(NodeId::new(0))
        .expect("alloc frame");
    let _ = descriptor.process()(&runtime, descriptor.runtime_data(), &mut frame);

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

#[test]
fn node_by_name_returns_registered_id_and_none_for_unknown() {
    let runtime = test_runtime(64, 4, 2);
    let drop = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let named = runtime.nodes().register_internal(DescriptorNode::next(
        "contract-drop",
        count_process,
        NodeRuntimeData::empty(),
        [drop, drop],
    ));

    assert_eq!(runtime.node_by_name("contract-drop"), Some(named));
    assert_eq!(runtime.node_by_name("does-not-exist"), None);
}

#[test]
fn set_node_next_slot_redirects_existing_next_slot() {
    let runtime = test_runtime(64, 4, 2);
    let initial = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let redirect = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let owner = runtime.nodes().register_internal(DescriptorNode::next(
        "contract-owner",
        count_process,
        NodeRuntimeData::empty(),
        [initial, initial],
    ));

    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(owner, TestNext::Default as usize)
            .unwrap(),
        initial
    );

    runtime
        .nodes()
        .set_node_next_slot(owner, TestNext::Default as usize, redirect)
        .expect("redirect next slot");

    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(owner, TestNext::Default as usize)
            .unwrap(),
        redirect
    );
}

#[test]
fn try_register_descriptor_registers_erased_descriptor() {
    let runtime = test_runtime(64, 4, 2);
    let drop = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let nexts = [drop, drop];
    let descriptor = NodeDescriptor::new(
        count_process,
        NodeRuntimeData::from_words([7, 0, 0, 0]),
        NodeRegistration::next("erased-owner", TestNext::COUNT),
        &nexts,
        None,
    );

    let id = runtime
        .nodes()
        .try_register_descriptor(NodeKind::Internal, descriptor)
        .expect("register erased descriptor");

    assert_eq!(runtime.node_by_name("erased-owner"), Some(id));
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(id, TestNext::Default as usize)
            .unwrap(),
        drop
    );
}

#[test]
fn try_register_descriptor_rejects_next_count_mismatch() {
    let runtime = test_runtime(64, 4, 2);
    let empty_nexts: &[NodeId] = &[];
    let descriptor = NodeDescriptor::new(
        count_process,
        NodeRuntimeData::empty(),
        NodeRegistration::next("erased-bad", TestNext::COUNT),
        empty_nexts,
        None,
    );

    let err = runtime
        .nodes()
        .try_register_descriptor(NodeKind::Internal, descriptor)
        .expect_err("initial next count mismatch must fail");
    assert!(err.to_string().contains("node initial next count mismatch"));
}

#[test]
fn add_node_next_slot_returns_u16_and_supports_slots_above_sixteen() {
    let runtime = test_runtime(64, 4, 2);
    let drop = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([1, 0, 0, 0]),
    ));
    let owner = runtime.nodes().register_internal(DescriptorNode::next(
        "dynamic-owner",
        count_process,
        NodeRuntimeData::empty(),
        [drop, drop],
    ));

    let mut last_slot = 1u16;
    for word in 2u64..20 {
        let target = runtime.nodes().register_internal(DescriptorNode::plain(
            count_process,
            NodeRuntimeData::from_words([word, 0, 0, 0]),
        ));
        let slot = runtime
            .nodes()
            .add_node_next_slot(owner, target)
            .expect("append dynamic next");
        assert_eq!(slot, last_slot + 1);
        last_slot = slot;
        assert_eq!(
            runtime
                .nodes()
                .node_next_slot(owner, usize::from(slot))
                .unwrap(),
            target
        );
        assert_eq!(runtime.nodes().node_next(owner, slot).unwrap(), target);
    }
    assert!(last_slot >= 16);
}

#[test]
fn sparse_local_u16_slots_resolve_through_runtime() {
    let runtime = test_runtime(64, 4, 2);
    let mut initial = Vec::with_capacity(32);
    for word in 0..32u64 {
        initial.push(runtime.nodes().register_internal(DescriptorNode::plain(
            count_process,
            NodeRuntimeData::from_words([word, 0, 0, 0]),
        )));
    }
    let owner = runtime
        .nodes()
        .try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                count_process,
                NodeRuntimeData::empty(),
                NodeRegistration::next("sparse-owner", 32),
                &initial,
                None,
            ),
        )
        .expect("register sparse owner");
    let redirected = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([99, 0, 0, 0]),
    ));

    runtime
        .nodes()
        .set_node_next(owner, 0u16, initial[0])
        .expect("keep slot 0");
    runtime
        .nodes()
        .set_node_next(owner, 31u16, redirected)
        .expect("redirect sparse high slot");

    assert_eq!(runtime.nodes().node_next(owner, 0u16).unwrap(), initial[0]);
    assert_eq!(runtime.nodes().node_next(owner, 31u16).unwrap(), redirected);
    assert_eq!(
        runtime.nodes().node_next_slot(owner, 15).unwrap(),
        initial[15]
    );
}

#[test]
fn add_node_next_slot_keeps_sibling_tables_consistent() {
    let runtime = test_runtime(64, 4, 2);
    let seed = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([1, 0, 0, 0]),
    ));
    let target = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([9, 0, 0, 0]),
    ));
    let owner = runtime.nodes().register_internal(DescriptorNode::next(
        "sibling-dynamic-owner",
        count_process,
        NodeRuntimeData::empty(),
        [seed, seed],
    ));
    let sibling = runtime.nodes().register_internal(DescriptorNode::sibling(
        "sibling-dynamic-child",
        "sibling-dynamic-owner",
        count_process,
        NodeRuntimeData::empty(),
    ));

    let slot = runtime
        .nodes()
        .add_node_next_slot(owner, target)
        .expect("append on owner");
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(owner, usize::from(slot))
            .unwrap(),
        target
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(sibling, usize::from(slot))
            .unwrap(),
        target
    );
}

#[test]
fn add_node_next_slot_reuses_existing_target_across_sibling_family() {
    let runtime = test_runtime(64, 4, 2);
    let first = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([1, 0, 0, 0]),
    ));
    let second = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([2, 0, 0, 0]),
    ));
    let appended = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([3, 0, 0, 0]),
    ));
    let owner = runtime.nodes().register_internal(DescriptorNode::next(
        "sibling-existing-owner",
        count_process,
        NodeRuntimeData::empty(),
        [first, second],
    ));
    let sibling = runtime.nodes().register_internal(DescriptorNode::sibling(
        "sibling-existing-child",
        "sibling-existing-owner",
        count_process,
        NodeRuntimeData::empty(),
    ));

    let existing_slot = runtime
        .nodes()
        .add_node_next_slot(sibling, second)
        .expect("reuse existing target from sibling");
    assert_eq!(existing_slot, 1);

    let appended_slot = runtime
        .nodes()
        .add_node_next_slot(sibling, appended)
        .expect("append target from sibling after reused slot");
    assert_eq!(appended_slot, 2);
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(owner, usize::from(appended_slot))
            .unwrap(),
        appended
    );
}

#[test]
fn worker_runtime_materializes_the_registered_graph_without_rebinding_nodes() {
    const HANDLE: NodeHandle = NodeHandle::new(41);

    reset_calls(73);
    let runtime = test_runtime(64, 8, 4);
    let drop = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let owner = runtime
        .nodes()
        .register_internal_with_handle(
            HANDLE,
            DescriptorNode::next(
                "worker-owner",
                count_process,
                NodeRuntimeData::from_words([73, 0, 0, 0]),
                [drop, drop],
            )
            .with_trace(trace_formatter),
        )
        .expect("register canonical owner");
    let sibling = runtime.nodes().register_internal(DescriptorNode::sibling(
        "worker-sibling",
        "worker-owner",
        count_process,
        NodeRuntimeData::from_words([74, 0, 0, 0]),
    ));

    let worker = runtime.for_worker(1, 0).expect("worker runtime fork");

    assert_eq!(worker.node_by_name("worker-owner"), Some(owner));
    assert_eq!(worker.node_by_name("worker-sibling"), Some(sibling));
    assert_eq!(worker.nodes().node_for_handle(HANDLE).unwrap(), owner);
    assert_eq!(worker.nodes().node_kind(owner).unwrap(), NodeKind::Internal);
    assert_eq!(worker.nodes().node_siblings(owner).unwrap(), vec![sibling]);
    assert_eq!(
        worker
            .nodes()
            .node_next_slot(sibling, TestNext::Alternate as usize)
            .unwrap(),
        drop
    );
    assert_eq!(
        worker
            .nodes()
            .node_trace_formatter(owner)
            .unwrap()
            .expect("canonical trace formatter")(b"abc"),
        "descriptor trace bytes=3"
    );

    let mut frame = worker.buffers().get_next_frame(owner).expect("alloc frame");
    push_packet(&worker, &mut frame, b"packet");
    worker.put_next_frame(frame).expect("put next frame");
    assert_eq!(worker.run_ready_nodes().expect("run worker node"), 1);
    assert_eq!(calls_for(73), 1);

    let registration = worker.nodes().try_register_internal(DescriptorNode::next(
        "worker-added-node",
        count_process,
        NodeRuntimeData::from_words([99, 0, 0, 0]),
        [drop, drop],
    ));
    assert!(
        registration
            .expect_err("worker registration must not mutate inherited graph")
            .to_string()
            .contains("cannot mutate graph topology")
    );
    assert!(
        worker
            .rebuild_graph(&[])
            .expect_err("worker rebuild must not replace inherited graph")
            .to_string()
            .contains("cannot mutate graph topology")
    );
}

#[test]
fn worker_runtime_resets_execution_state_but_keeps_configured_node_state() {
    reset_calls(81);
    let runtime = test_runtime(64, 8, 4);
    let drop = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let node = runtime.nodes().register_internal(DescriptorNode::next(
        "worker-state",
        count_process,
        NodeRuntimeData::from_words([81, 0, 0, 0]),
        [drop, drop],
    ));
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));

    let mut frame = runtime.buffers().get_next_frame(node).expect("alloc frame");
    push_packet(&runtime, &mut frame, b"packet");
    runtime.put_next_frame(frame).expect("put next frame");
    assert_eq!(runtime.run_ready_nodes().expect("run canonical node"), 1);

    let encoded = runtime
        .nodes()
        .increment_node_error(node, 7)
        .expect("define and increment node error");
    runtime
        .nodes()
        .set_node_state(driver, NodeState::Interrupt)
        .expect("configure interrupt driver");
    assert!(
        runtime
            .set_node_interrupt_pending(driver)
            .expect("schedule canonical interrupt")
    );
    assert!(runtime.nodes().has_pending());

    let worker = runtime.for_worker(1, 0).expect("worker runtime fork");

    assert!(!worker.nodes().has_pending());
    assert_eq!(worker.current_node(), None);
    assert_eq!(
        worker.nodes().node_state(driver).unwrap(),
        NodeState::Interrupt
    );
    assert_eq!(worker.nodes().node_error_count(node, 7).unwrap(), 0);
    assert_eq!(
        worker.nodes().decode_node_error(encoded).unwrap(),
        runtime.nodes().decode_node_error(encoded).unwrap()
    );
    let row = worker
        .nodes()
        .node_runtime_stats_snapshot()
        .into_iter()
        .find(|row| row.node_id == node)
        .expect("worker node stats row");
    assert_eq!(row.calls, 0);
    assert_eq!(row.vectors, 0);
    assert!(
        worker
            .set_node_interrupt_pending(driver)
            .expect("worker interrupt pending starts clear")
    );
}

#[test]
fn worker_graph_rejects_worker_local_next_slot_mutation() {
    let runtime = test_runtime(64, 8, 4);
    let initial = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([1, 0, 0, 0]),
    ));
    let dynamic = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([2, 0, 0, 0]),
    ));
    let owner = runtime.nodes().register_internal(DescriptorNode::next(
        "worker-sibling-owner",
        count_process,
        NodeRuntimeData::empty(),
        [initial, initial],
    ));
    let sibling = runtime.nodes().register_internal(DescriptorNode::sibling(
        "worker-sibling-child",
        "worker-sibling-owner",
        count_process,
        NodeRuntimeData::empty(),
    ));
    let first = runtime.for_worker(1, 0).expect("first worker runtime fork");
    let second = runtime.for_worker(2, 0).expect("second worker runtime fork");

    let error = first
        .nodes()
        .add_node_next_slot(sibling, dynamic)
        .expect_err("worker must not add a graph next slot");

    assert!(error.to_string().contains("cannot mutate graph topology"));
    assert!(first.nodes().node_next_slot(owner, 2).is_err());
    assert!(first.nodes().node_next_slot(sibling, 2).is_err());
    assert!(runtime.nodes().node_next_slot(owner, 2).is_err());
    assert!(second.nodes().node_next_slot(owner, 2).is_err());
}

fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, payload: &[u8]) {
    let buffer = runtime
        .alloc_index_with_bytes(payload)
        .expect("alloc packet");
    frame.push_index(buffer).expect("push packet");
}
