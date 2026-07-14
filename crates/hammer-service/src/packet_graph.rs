//! Service packet graph: one global `GRAPH_NODES` catalog filtered by loaded plugins.

use std::sync::Arc;

use hammer_component_macros::init_function;
use hammer_core::config::Config;
use hammer_core::data_plane::NodeHandle;
use hammer_core::error::HammerResult;
use hammer_infra::vec::Vec;
use hammer_runtime::{Engine, GRAPH_NODES, NodeEntry, filter_by_plugin};

#[init_function(
    name = "install_packet_graph",
    runs_after = ["memory_init"]
)]
pub fn install_packet_graph(engine: &mut Engine, config: Arc<Config>) -> HammerResult<()> {
    let handle = NodeHandle::new(config.worker.handoff.node_handle);
    engine.runtime.set_handoff_node_handle(handle);

    let loaded = engine.loaded_plugins();
    let filtered: Vec<NodeEntry> = filter_by_plugin(&GRAPH_NODES[..], loaded, |entry| entry.plugin)
        .into_iter()
        .copied()
        .collect();
    engine.runtime.init_graph(0, &filtered)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{DeviceInputNext, DeviceInputNode};
    use crate::tun::TunInputDriverNode;
    use hammer_core::data_plane::{NodeKind, NodeRegistration};

    fn entry_by_name(name: &str) -> Option<(NodeKind, NodeRegistration, Option<&'static str>)> {
        GRAPH_NODES
            .iter()
            .find(|entry| entry.registration.name() == Some(name))
            .map(|entry| (entry.kind, entry.registration, entry.plugin))
    }

    fn graph_names() -> Vec<&'static str> {
        GRAPH_NODES
            .iter()
            .filter_map(|entry| entry.registration.name())
            .collect()
    }

    #[test]
    fn device_input_family_uses_macro_generated_next_layout() {
        assert_eq!(DeviceInputNode::NODE_NEXT_COUNT, DeviceInputNext::COUNT);
        assert_eq!(TunInputDriverNode::NODE_NEXT_COUNT, DeviceInputNext::COUNT);
    }

    #[test]
    fn global_graph_declares_vpp_device_input_family() {
        assert_eq!(
            (
                entry_by_name("device-input").map(|(kind, registration, _)| (kind, registration)),
                entry_by_name("tun-input").map(|(kind, registration, _)| (kind, registration)),
                entry_by_name("tun-output").map(|(kind, registration, _)| (kind, registration)),
            ),
            (
                Some((NodeKind::Driver, NodeRegistration::next("device-input", 3),)),
                Some((
                    NodeKind::Driver,
                    NodeRegistration::sibling_of("tun-input", "device-input"),
                )),
                Some((NodeKind::Internal, NodeRegistration::next("tun-output", 0),)),
            )
        );
    }

    #[test]
    fn global_graph_contains_service_tun_and_tcp_nodes() {
        let names = graph_names();
        for want in [
            "device-input",
            "tun-input",
            "tun-output",
            "drop",
            "handoff",
            "ip-lookup",
            "tcp-input",
            "tcp-listen",
            "tcp-established",
            "tcp-rcv-process",
            "tcp-syn-sent",
            "tcp-output",
            "session-queue",
        ] {
            assert!(
                names.iter().any(|name| *name == want),
                "missing {want} in GRAPH_NODES"
            );
        }
    }

    #[test]
    fn graph_node_entries_carry_plugin_owner_field() {
        let owners = [
            ("device-input", Some("device")),
            ("tun-input", Some("tun")),
            ("tun-output", Some("tun")),
            ("ip-lookup", Some("ip")),
            ("tcp-input", Some("tcp")),
            ("session-queue", Some("session")),
            ("drop", None),
            ("handoff", None),
        ];
        for (name, plugin) in owners {
            assert_eq!(
                entry_by_name(name).map(|(_, _, owner)| owner),
                Some(plugin),
                "{name} plugin owner"
            );
        }
    }
}
