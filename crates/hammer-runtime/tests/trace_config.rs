use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, RuntimeRegistry, TraceControlPlane,
};

#[test]
fn trace_config_uses_its_serde_schema_and_installs_trace_control() {
    let document = r#"
[trace]
enabled = false
record_capacity = 32
packet_capacity = 4
"#;
    let mut engine = Engine::new(
        DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
        RuntimeRegistry::new(),
    );

    hammer_runtime::init::run_config_functions(&mut engine, false, document)
        .expect("deserialize and install trace configuration");

    assert!(engine.registry.get::<TraceControlPlane>().is_some());
}
