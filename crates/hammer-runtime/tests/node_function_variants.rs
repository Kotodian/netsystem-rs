use std::sync::atomic::{AtomicU64, Ordering};

use hammer_core::data_plane::BufferFrame;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::config::Worker;
use hammer_runtime::graph::install_packet_graph;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneInstructionSet, DataPlaneRuntime, DataPlaneRuntimeConfig,
    Engine, Node, NodeResult, NodeRuntimeData, spawn::DataRuntime,
};

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    early_config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    graph_nodes = [__MULTIARCH_GRAPH_NODE_MULTIARCH_NODE];
    node_functions = [
        __NODE_FUNCTION_MULTIARCH_PROCESS_SCALAR,
        __NODE_FUNCTION_MULTIARCH_PROCESS_SSE2,
        __NODE_FUNCTION_MULTIARCH_PROCESS_AVX2,
        __NODE_FUNCTION_MULTIARCH_PROCESS_AVX512,
    ];
    process_nodes = [];
);

#[cfg(any(target_arch = "arm", target_arch = "aarch64"))]
hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    early_config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    graph_nodes = [__MULTIARCH_GRAPH_NODE_MULTIARCH_NODE];
    node_functions = [
        __NODE_FUNCTION_MULTIARCH_PROCESS_SCALAR,
        __NODE_FUNCTION_MULTIARCH_PROCESS_NEON,
    ];
    process_nodes = [];
);

static DISPATCH_COUNT: AtomicU64 = AtomicU64::new(0);

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
const UNSUPPORTED_INSTRUCTION_SET: &str = "neon";
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
const UNSUPPORTED_INSTRUCTION_SET: &str = "avx2";

#[hammer_component_macros::graph_node(
    graph = multiarch,
    name = "multiarch-fixture",
    kind = internal,
)]
#[derive(Debug, Clone, Copy)]
struct MultiarchNode;

impl Node for MultiarchNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }
}

#[hammer_component_macros::node_function(node = MultiarchNode)]
fn multiarch_process(_: &DataPlaneRuntime, _: NodeRuntimeData, _: &mut BufferFrame) -> NodeResult {
    DISPATCH_COUNT.fetch_add(1, Ordering::Relaxed);
    NodeResult::drop()
}

#[test]
fn node_function_selection_changes_dispatch_without_changing_topology() {
    let runtime_config = DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots: 4,
            frame_slots: 4,
            ..DataPlaneBufferConfig::default()
        },
    };
    let mut scalar = Engine::new(
        DataPlaneRuntime::new_with_instruction_set(
            runtime_config.clone(),
            DataPlaneInstructionSet::Scalar,
        ),
        RuntimeRegistry::new(),
    );
    scalar
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    install_packet_graph(&mut scalar).expect("initialize scalar graph");
    let scalar_node = scalar
        .runtime
        .node_by_name(MultiarchNode::NODE_NAME)
        .expect("scalar graph node");
    let scalar_topology = (
        scalar_node,
        scalar
            .runtime
            .nodes()
            .node_name(scalar_node)
            .expect("scalar name"),
        scalar
            .runtime
            .nodes()
            .node_kind(scalar_node)
            .expect("scalar kind"),
        scalar
            .runtime
            .nodes()
            .node_state(scalar_node)
            .expect("scalar state"),
        scalar
            .runtime
            .nodes()
            .node_siblings(scalar_node)
            .expect("scalar siblings")
            .is_empty(),
    );

    DISPATCH_COUNT.store(0, Ordering::Relaxed);
    scalar
        .runtime
        .schedule_empty_frame(scalar_node)
        .expect("schedule scalar fixture");
    scalar
        .runtime
        .run_ready_nodes()
        .expect("dispatch scalar fixture");
    assert_eq!(DISPATCH_COUNT.load(Ordering::Relaxed), 1);

    let mut native = Engine::new(
        DataPlaneRuntime::new_with_instruction_set(
            runtime_config,
            DataPlaneInstructionSet::native(),
        ),
        RuntimeRegistry::new(),
    );
    native
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    install_packet_graph(&mut native).expect("initialize native graph");
    let native_node = native
        .runtime
        .node_by_name(MultiarchNode::NODE_NAME)
        .expect("native graph node");
    let native_topology = (
        native_node,
        native
            .runtime
            .nodes()
            .node_name(native_node)
            .expect("native name"),
        native
            .runtime
            .nodes()
            .node_kind(native_node)
            .expect("native kind"),
        native
            .runtime
            .nodes()
            .node_state(native_node)
            .expect("native state"),
        native
            .runtime
            .nodes()
            .node_siblings(native_node)
            .expect("native siblings")
            .is_empty(),
    );

    DISPATCH_COUNT.store(0, Ordering::Relaxed);
    for _ in 0..2 {
        native
            .runtime
            .schedule_empty_frame(native_node)
            .expect("schedule native fixture");
    }
    native
        .runtime
        .run_ready_nodes()
        .expect("dispatch native fixture");
    assert_eq!(DISPATCH_COUNT.load(Ordering::Relaxed), 2);

    assert_eq!(native_topology, scalar_topology);
}

#[test]
fn worker_runtime_rejects_unknown_and_unsupported_instruction_sets() {
    let mut worker = Worker::default();
    worker.instruction_set = "unknown".to_owned();
    let Err(error) = DataRuntime::from_config(&worker, "unknown-isa-worker") else {
        panic!("unknown instruction set must fail");
    };
    assert!(error.to_string().contains("unknown instruction set"));

    worker.instruction_set = UNSUPPORTED_INSTRUCTION_SET.to_owned();
    let Err(error) = DataRuntime::from_config(&worker, "unsupported-isa-worker") else {
        panic!("unsupported instruction set must fail");
    };
    assert!(error.to_string().contains("not supported by this CPU"));
}
