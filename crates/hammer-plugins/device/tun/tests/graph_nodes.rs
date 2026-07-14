use hammer_core::data_plane::{NodeKind, NodeRegistration};
use hammer_plugin_tun::TunInputDriverNode;
use hammer_runtime::GRAPH_NODES;
use hammer_service::device::{DeviceInputNext, DeviceInputNode};

#[test]
fn tun_input_matches_device_input_next_layout() {
    assert_eq!(TunInputDriverNode::NODE_NEXT_COUNT, DeviceInputNext::COUNT);
    assert_eq!(DeviceInputNode::NODE_NEXT_COUNT, DeviceInputNext::COUNT);
}

#[test]
fn tun_graph_nodes_owned_by_tun_plugin() {
    let tun_input = GRAPH_NODES
        .iter()
        .find(|entry| entry.registration.name() == Some("tun-input"))
        .expect("tun-input");
    let tun_output = GRAPH_NODES
        .iter()
        .find(|entry| entry.registration.name() == Some("tun-output"))
        .expect("tun-output");
    assert_eq!(tun_input.kind, NodeKind::Driver);
    assert_eq!(
        tun_input.registration,
        NodeRegistration::sibling_of("tun-input", "device-input")
    );
    assert_eq!(tun_input.plugin, Some("tun"));
    assert_eq!(tun_output.kind, NodeKind::Internal);
    assert_eq!(
        tun_output.registration,
        NodeRegistration::next("tun-output", 0)
    );
    assert_eq!(tun_output.plugin, Some("tun"));
}
