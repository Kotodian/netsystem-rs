use std::sync::atomic::{AtomicU64, Ordering};

use hammer_core::config::Config;
use hammer_core::data_plane::{BufferFrame, DataPlaneBufferConfig};
use hammer_infra::vec::Vec;
use hammer_runtime::{
    DataPlaneInstructionSet, DataPlaneRuntime, DataPlaneRuntimeConfig, GRAPH_NODES, Node,
    NodeResult, NodeRuntimeData, filter_by_plugin, spawn::DataRuntime,
};

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

fn multiarch_entries() -> Vec<hammer_runtime::NodeEntry> {
    filter_by_plugin(&GRAPH_NODES[..], &[], |_| None)
        .into_iter()
        .filter(|entry| entry.registration.name() == Some("multiarch-fixture"))
        .copied()
        .collect()
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
    let entries = multiarch_entries();
    let scalar = DataPlaneRuntime::new_with_instruction_set(
        runtime_config.clone(),
        DataPlaneInstructionSet::Scalar,
    );
    scalar
        .init_graph(0, &entries)
        .expect("initialize scalar graph");
    let scalar_node = scalar
        .node_by_name(MultiarchNode::NODE_NAME)
        .expect("scalar graph node");
    let scalar_topology = (
        scalar_node,
        scalar.nodes().node_name(scalar_node).expect("scalar name"),
        scalar.nodes().node_kind(scalar_node).expect("scalar kind"),
        scalar
            .nodes()
            .node_state(scalar_node)
            .expect("scalar state"),
        scalar
            .nodes()
            .node_siblings(scalar_node)
            .expect("scalar siblings")
            .is_empty(),
    );

    DISPATCH_COUNT.store(0, Ordering::Relaxed);
    scalar
        .schedule_empty_frame(scalar_node)
        .expect("schedule scalar fixture");
    scalar.run_ready_nodes().expect("dispatch scalar fixture");
    assert_eq!(DISPATCH_COUNT.load(Ordering::Relaxed), 1);

    let native = DataPlaneRuntime::new_with_instruction_set(
        runtime_config,
        DataPlaneInstructionSet::native(),
    );
    native
        .init_graph(0, &entries)
        .expect("initialize native graph");
    let native_node = native
        .node_by_name(MultiarchNode::NODE_NAME)
        .expect("native graph node");
    let native_topology = (
        native_node,
        native.nodes().node_name(native_node).expect("native name"),
        native.nodes().node_kind(native_node).expect("native kind"),
        native
            .nodes()
            .node_state(native_node)
            .expect("native state"),
        native
            .nodes()
            .node_siblings(native_node)
            .expect("native siblings")
            .is_empty(),
    );

    DISPATCH_COUNT.store(0, Ordering::Relaxed);
    for _ in 0..2 {
        native
            .schedule_empty_frame(native_node)
            .expect("schedule native fixture");
    }
    native.run_ready_nodes().expect("dispatch native fixture");
    assert_eq!(DISPATCH_COUNT.load(Ordering::Relaxed), 2);

    assert_eq!(native_topology, scalar_topology);
}

#[test]
fn worker_runtime_rejects_unknown_and_unsupported_instruction_sets() {
    let mut config = Config::default();
    config.worker.instruction_set = "unknown".to_owned();
    let Err(error) = DataRuntime::from_config(&config.worker, "unknown-isa-worker") else {
        panic!("unknown instruction set must fail");
    };
    assert!(error.to_string().contains("unknown instruction set"));

    config.worker.instruction_set = UNSUPPORTED_INSTRUCTION_SET.to_owned();
    let Err(error) = DataRuntime::from_config(&config.worker, "unsupported-isa-worker") else {
        panic!("unsupported instruction set must fail");
    };
    assert!(error.to_string().contains("not supported by this CPU"));
}
