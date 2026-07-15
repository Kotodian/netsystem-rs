use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

#[test]
fn reassembly_expiry_is_a_main_process_node() {
    _ = hammer_plugin_ip::registration();
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut engine = Engine::new(runtime, RuntimeRegistry::new());
    engine.start_process_nodes().expect("start Process Nodes");

    assert!(engine.process_handle("ip-reassembly-expire-walk").is_some());
}
