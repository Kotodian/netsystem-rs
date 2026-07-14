//! Service packet graph: linkme `SERVICE_GRAPH_NODES` for graph registration.
//! Control-plane init migrated to `#[init_function]` in the init system.

use std::sync::Arc;

use hammer_component_macros::init_function;
use hammer_core::config::Config;
use hammer_core::data_plane::NodeHandle;
use hammer_core::error::HammerResult;
use hammer_runtime::Engine;
use hammer_runtime::NodeEntry;
#[cfg(test)]
use hammer_infra::vec::Vec;

#[linkme::distributed_slice]
pub static SERVICE_GRAPH_NODES: [NodeEntry] = [..];

#[linkme::distributed_slice]
pub static TUN_GRAPH_NODES: [NodeEntry] = [..];

#[linkme::distributed_slice]
pub static TCP_WORKER_GRAPH_NODES: [NodeEntry] = [..];

#[init_function(
    name = "install_packet_graph",
    runs_after = ["memory_init", "device_init", "ip_init"]
)]
pub fn install_packet_graph(engine: &mut Engine, config: Arc<Config>) -> HammerResult<()> {
    let handle = NodeHandle::new(config.worker.handoff.node_handle);
    engine.runtime.set_handoff_node_handle(handle);

    engine.runtime.init_graph(0, &SERVICE_GRAPH_NODES)?;
    crate::net::wire_ip_lookup_drop(&engine.runtime)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{DeviceInputNext, DeviceInputNode};
    use crate::tun::TunInputDriverNode;
    use hammer_core::data_plane::{NodeKind, NodeRegistration};

    #[test]
    fn device_input_family_uses_macro_generated_next_layout() {
        assert_eq!(DeviceInputNode::NODE_NEXT_COUNT, DeviceInputNext::COUNT);
        assert_eq!(TunInputDriverNode::NODE_NEXT_COUNT, DeviceInputNext::COUNT);
    }

    #[test]
    fn service_graph_declares_vpp_device_input_family() {
        let entry = |entries: &[NodeEntry], name| {
            entries
                .iter()
                .find(|entry| entry.registration.name() == Some(name))
                .map(|entry| (entry.kind, entry.registration))
        };

        assert_eq!(
            (
                entry(&SERVICE_GRAPH_NODES, "device-input"),
                entry(&TUN_GRAPH_NODES, "tun-input"),
                entry(&TUN_GRAPH_NODES, "tun-output"),
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
    fn tun_driver_nodes_are_loaded_only_with_the_tun_module() {
        let service_names: Vec<&'static str> = SERVICE_GRAPH_NODES
            .iter()
            .filter_map(|entry| entry.registration.name())
            .collect();
        let tun_names: Vec<&'static str> = TUN_GRAPH_NODES
            .iter()
            .filter_map(|entry| entry.registration.name())
            .collect();

        assert!(service_names.contains(&"device-input"));
        assert!(!service_names.contains(&"tun-input"));
        assert!(!service_names.contains(&"tun-output"));
        assert!(tun_names.contains(&"tun-input"));
        assert!(tun_names.contains(&"tun-output"));
    }

    #[test]
    fn service_graph_contains_tcp_nodes() {
        let service_names: Vec<&'static str> = SERVICE_GRAPH_NODES
            .iter()
            .filter_map(|e| e.registration.name())
            .collect();
        for want in ["drop", "handoff", "ip-lookup"] {
            assert!(
                service_names.iter().any(|name| *name == want),
                "missing {want}"
            );
        }

        let tcp_names: Vec<&'static str> = TCP_WORKER_GRAPH_NODES
            .iter()
            .filter_map(|entry| entry.registration.name())
            .collect();
        for want in [
            "tcp-input",
            "tcp-listen",
            "tcp-established",
            "tcp-rcv-process",
            "tcp-syn-sent",
            "tcp-output",
            "session-queue",
        ] {
            assert!(tcp_names.iter().any(|name| *name == want), "missing {want}");
        }
    }

    #[test]
    fn subsystem_graph_slices_do_not_duplicate_service_registrations() {
        for entry in TUN_GRAPH_NODES.iter().chain(TCP_WORKER_GRAPH_NODES.iter()) {
            let name = entry.registration.name().expect("declared node name");
            assert!(
                SERVICE_GRAPH_NODES
                    .iter()
                    .all(|service| service.registration.name() != Some(name)),
                "subsystem graph node {name} must not be in the service slice"
            );
        }
    }
}
