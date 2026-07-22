use hammer_core::data_plane::NodeKind;
use hammer_plugin_tun::TunInputDriverNode;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::graph::install_packet_graph;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};
use hammer_service::device::{DeviceInputNext, DeviceInputNode};

#[test]
fn tun_input_matches_device_input_next_layout() {
    assert_eq!(TunInputDriverNode::NODE_NEXT_COUNT, DeviceInputNext::COUNT);
    assert_eq!(DeviceInputNode::NODE_NEXT_COUNT, DeviceInputNext::COUNT);
}

#[test]
fn tun_graph_nodes_install_from_the_link_image() {
    _ = hammer_plugin_tun::plugin_module();
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut engine = Engine::new(runtime, RuntimeRegistry::new());

    // The service image references protocol-plugin nodes that are not linked
    // into this focused test. Node creation precedes named-next resolution.
    _ = install_packet_graph(&mut engine);

    let device_input = engine
        .runtime
        .node_by_name("device-input")
        .expect("device-input");
    let tun_input = engine.runtime.node_by_name("tun-input").expect("tun-input");
    let tun_output = engine
        .runtime
        .node_by_name("tun-output")
        .expect("tun-output");
    assert_eq!(
        engine.runtime.nodes().node_kind(tun_input).unwrap(),
        NodeKind::Driver
    );
    assert_eq!(
        engine.runtime.nodes().node_kind(tun_output).unwrap(),
        NodeKind::Internal
    );
    assert!(
        engine
            .runtime
            .nodes()
            .node_siblings(tun_input)
            .unwrap()
            .contains(&device_input)
    );
}
