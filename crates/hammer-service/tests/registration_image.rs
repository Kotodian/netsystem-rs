use std::sync::Arc;

use hammer_core::config::Config;
use hammer_core::data_plane::DataPlaneBufferConfig;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::graph::install_packet_graph;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

#[test]
fn service_builtin_image_installs_through_runtime_authority() {
    hammer_service::reset_subsystem_mains_for_plugin_test();
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 256,
            buffer_slots: 16,
            frame_slots: 16,
            ..DataPlaneBufferConfig::default()
        },
    });
    let mut engine = Engine::new(runtime, RuntimeRegistry::new());

    // The service graph references protocol-plugin nodes, so resolving all
    // named next arcs requires those plugins. Registration and node creation
    // happen before that final resolution step.
    _ = install_packet_graph(&mut engine, Arc::new(Config::default()));

    assert!(engine.runtime.node_by_name("drop").is_some());
    assert!(engine.runtime.node_by_name("device-input").is_some());
    assert!(engine.runtime.node_by_name("session-queue").is_some());
}
