//! Graph rebuild transaction prototype (#100).

use hammer_core::data_plane::{DataPlaneBufferConfig, NodeId, NodeKind, NodeRegistration};
use hammer_core::error::CoreResult;
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, NodeDescriptor, NodeEntry, NodeProcessFn, NodeResult,
    NodeRuntimeData,
};

fn test_runtime() -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots: 4,
            frame_slots: 4,
            ..DataPlaneBufferConfig::default()
        },
    })
}

fn noop_process(
    _runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    _frame: &mut hammer_core::data_plane::BufferFrame,
) -> NodeResult {
    NodeResult::drop()
}

fn init_named(name: &'static str) -> fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId> {
    match name {
        "alpha" => init_alpha,
        "beta" => init_beta,
        "gamma" => init_gamma,
        _ => panic!("unknown node {name}"),
    }
}

fn init_alpha(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            noop_process as NodeProcessFn,
            NodeRuntimeData::empty(),
            NodeRegistration::next("alpha", 0),
            &[],
            None,
        ),
    )
}

fn init_beta(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            noop_process as NodeProcessFn,
            NodeRuntimeData::empty(),
            NodeRegistration::next("beta", 0),
            &[],
            None,
        ),
    )
}

fn init_gamma(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            noop_process as NodeProcessFn,
            NodeRuntimeData::empty(),
            NodeRegistration::next("gamma", 0),
            &[],
            None,
        ),
    )
}

fn entry(name: &'static str) -> NodeEntry {
    NodeEntry {
        registration: NodeRegistration::next(name, 0),
        kind: NodeKind::Internal,
        init: init_named(name),
    }
}

#[test]
fn rebuild_graph_renumbers_nodes_and_invalidates_old_node_ids() {
    let runtime = test_runtime();
    runtime
        .init_graph(0, &[entry("alpha"), entry("beta")])
        .expect("init");
    let old_alpha = runtime.node_by_name("alpha").expect("alpha");
    let old_beta = runtime.node_by_name("beta").expect("beta");
    assert_eq!(old_alpha.slot(), 0);
    assert_eq!(old_beta.slot(), 1);

    // Shrink the graph so the previous high slot becomes out of range.
    runtime
        .rebuild_graph(0, &[entry("gamma")])
        .expect("rebuild");

    assert!(
        runtime.nodes().node_name(old_beta).is_err(),
        "old beta NodeId slot must be unreachable after shrink rebuild"
    );
    assert!(runtime.node_by_name("alpha").is_none());
    assert!(runtime.node_by_name("beta").is_none());

    let new_gamma = runtime
        .node_by_name("gamma")
        .expect("gamma rebound by name");
    assert_eq!(new_gamma.slot(), 0);
    // Slot reuse is VPP-shaped: callers must rebind by name, never cache NodeId.
    assert_eq!(old_alpha.slot(), new_gamma.slot());
    assert_ne!(
        runtime.nodes().node_name(old_alpha).ok().flatten(),
        Some("alpha"),
        "reused slot must not still name the detached node"
    );
}

#[test]
fn rebuild_graph_rebinds_surviving_nodes_by_name() {
    let runtime = test_runtime();
    runtime
        .init_graph(0, &[entry("alpha"), entry("beta")])
        .expect("init");
    let old_beta = runtime.node_by_name("beta").expect("beta");

    runtime
        .rebuild_graph(0, &[entry("beta"), entry("gamma")])
        .expect("rebuild");

    let new_beta = runtime.node_by_name("beta").expect("beta rebound by name");
    let new_gamma = runtime.node_by_name("gamma").expect("gamma");
    assert_eq!(new_beta.slot(), 0);
    assert_eq!(new_gamma.slot(), 1);
    assert_ne!(new_beta, old_beta);
}

#[test]
fn worker_runtime_after_rebuild_matches_main_topology() {
    let runtime = test_runtime();
    runtime
        .init_graph(0, &[entry("alpha"), entry("beta")])
        .expect("init");
    runtime
        .rebuild_graph(0, &[entry("gamma")])
        .expect("rebuild");

    let worker = runtime.for_worker(1, 0);
    assert_eq!(
        worker.node_by_name("gamma").map(|id| id.slot()),
        runtime.node_by_name("gamma").map(|id| id.slot())
    );
    assert!(worker.node_by_name("alpha").is_none());
    assert!(worker.node_by_name("beta").is_none());
}
