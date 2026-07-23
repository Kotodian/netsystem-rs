use hammer_runtime::RuntimeRegistry;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

#[test]
fn reassembly_expiry_is_a_main_process_node() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut engine = Engine::new(runtime, RuntimeRegistry::new());
    let plugin = hammer_plugin_ip::plugin_module();
    engine
        .plugin_main_mut()
        .register_builtin_image(plugin.registration_image().get());
    engine.start_process_nodes().expect("start Process Nodes");

    assert!(engine.process_handle("ip-reassembly-expire-walk").is_some());
}
