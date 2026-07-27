use std::sync::atomic::{AtomicU64, Ordering};

use hammer_core::data_plane::BufferFrame;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::graph::install_packet_graph;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, Node, NodeResult,
    NodeRuntimeData,
};

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
        __NODE_FUNCTION_MULTIARCH_PROCESS_SIMD128,
        __NODE_FUNCTION_MULTIARCH_PROCESS_SIMD256,
        __NODE_FUNCTION_MULTIARCH_PROCESS_SIMD512,
    ];
    process_nodes = [];
);

static DISPATCH_COUNT: AtomicU64 = AtomicU64::new(0);

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
    let mut engine = Engine::new(
        DataPlaneRuntime::new(runtime_config),
        RuntimeRegistry::new(),
    );
    engine
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    install_packet_graph(&mut engine).expect("initialize graph");
    let node = engine
        .runtime
        .node_by_name(MultiarchNode::NODE_NAME)
        .expect("multiarch graph node");
    let topology = (
        node,
        engine.runtime.nodes().node_name(node).expect("node name"),
        engine.runtime.nodes().node_kind(node).expect("node kind"),
        engine.runtime.nodes().node_state(node).expect("node state"),
        engine
            .runtime
            .nodes()
            .node_siblings(node)
            .expect("node siblings")
            .is_empty(),
    );

    DISPATCH_COUNT.store(0, Ordering::Relaxed);
    for _ in 0..2 {
        engine
            .runtime
            .schedule_empty_frame(node)
            .expect("schedule fixture");
    }
    engine.runtime.run_ready_nodes().expect("dispatch fixture");
    assert_eq!(DISPATCH_COUNT.load(Ordering::Relaxed), 2);

    assert_eq!(
        topology,
        (
            node,
            engine.runtime.nodes().node_name(node).expect("node name"),
            engine.runtime.nodes().node_kind(node).expect("node kind"),
            engine.runtime.nodes().node_state(node).expect("node state"),
            engine
                .runtime
                .nodes()
                .node_siblings(node)
                .expect("node siblings")
                .is_empty(),
        )
    );
}
