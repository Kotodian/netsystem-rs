const BANNED_RUNTIME_OWNER_PATHS: &[&str] = &[
    "hammer_adapter::DataPlaneRuntime",
    "hammer_adapter::DataPlaneRuntimeConfig",
    "hammer_adapter::DataPlaneInstructionSet",
    "hammer_adapter::FrameBatchWidth",
    "hammer_adapter::DataWorkerId",
    "hammer_adapter::DataPlaneHandoff",
    "hammer_adapter::DataPlaneHandoffWorker",
    "hammer_adapter::Node",
    "hammer_adapter::DriverNode",
    "hammer_adapter::InternalNode",
    "hammer_adapter::NodeDescriptor",
    "hammer_adapter::NodeEntry",
    "hammer_adapter::NodeProcessFn",
    "hammer_adapter::NodeResult",
    "hammer_adapter::NodeRuntime",
    "hammer_adapter::NodeRuntimeData",
    "hammer_adapter::NodeRuntimeReady",
    "hammer_adapter::NoopNode",
    "hammer_adapter::process_frame",
    "hammer_adapter::add_packet_trace",
    "hammer_adapter::node::",
    "hammer_adapter::handoff::",
    "hammer_adapter::instruction_set::",
    "hammer_adapter::trace::",
];

const SCANNED_SOURCES: &[(&str, &str)] = &[
    ("hammer-runtime/src/lib.rs", include_str!("../src/lib.rs")),
    (
        "hammer-runtime/src/data_plane.rs",
        include_str!("../src/data_plane.rs"),
    ),
    (
        "hammer-runtime/src/engine.rs",
        include_str!("../src/engine.rs"),
    ),
    (
        "hammer-runtime/src/spawn.rs",
        include_str!("../src/spawn.rs"),
    ),
    (
        "hammer-runtime/src/graph/mod.rs",
        include_str!("../src/graph/mod.rs"),
    ),
    (
        "hammer-component-macros/src/lib.rs",
        include_str!("../../hammer-component-macros/src/lib.rs"),
    ),
    (
        "hammer-service/src/lib.rs",
        include_str!("../../hammer-service/src/lib.rs"),
    ),
    (
        "hammer-service/src/data_plane.rs",
        include_str!("../../hammer-service/src/data_plane.rs"),
    ),
    (
        "hammer-service/src/packet_graph.rs",
        include_str!("../../hammer-service/src/packet_graph.rs"),
    ),
    (
        "hammer-service/src/session/runtime.rs",
        include_str!("../../hammer-service/src/session/runtime.rs"),
    ),
    (
        "hammer-service/src/transport/tcp/mod.rs",
        include_str!("../../hammer-service/src/transport/tcp/mod.rs"),
    ),
];

#[test]
fn graph_runtime_owner_paths_do_not_point_at_adapter() {
    let mut violations = std::vec::Vec::new();
    for (path, source) in SCANNED_SOURCES {
        for banned in BANNED_RUNTIME_OWNER_PATHS {
            if source.contains(banned) {
                violations.push(format!("{path} contains {banned}"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "runtime graph contracts must be owned by hammer-runtime:\n{}",
        violations.join("\n")
    );
}
