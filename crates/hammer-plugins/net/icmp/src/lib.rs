use std::cell::UnsafeCell;
use std::sync::OnceLock;

use hammer_core::data_plane::{NodeId, NodeNext};
use hammer_runtime::{RuntimeError, RuntimeResult};

mod icmp;
mod protocol;

#[derive(Debug)]
pub struct IcmpMain {
    ip4: UnsafeCell<icmp::IcmpInputTable>,
    ip6: UnsafeCell<icmp::IcmpInputTable>,
    ip4_input_node: OnceLock<NodeId>,
    ip6_input_node: OnceLock<NodeId>,
}

// SAFETY: tables are initialized before graph publication. Subsequent writes
// require the main-thread worker barrier; packet readers copy one entry and
// do not return references into either table.
unsafe impl Sync for IcmpMain {}

static ICMP_MAIN: OnceLock<IcmpMain> = OnceLock::new();

impl IcmpMain {
    pub fn init() -> RuntimeResult<()> {
        let main = Self {
            ip4: UnsafeCell::new(icmp::IcmpInputTable::new(NodeNext::slot(
                icmp::Icmp4InputNext::Punt,
            ))),
            ip6: UnsafeCell::new(icmp::IcmpInputTable::new(NodeNext::slot(
                icmp::Icmp6InputNext::Punt,
            ))),
            ip4_input_node: OnceLock::new(),
            ip6_input_node: OnceLock::new(),
        };
        ICMP_MAIN
            .set(main)
            .map_err(|_| RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_plugin_icmp::IcmpMain",
            })?;
        Ok(())
    }

    pub fn global() -> RuntimeResult<&'static Self> {
        ICMP_MAIN
            .get()
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_plugin_icmp::IcmpMain",
            })
    }
}

#[hammer_component_macros::init_function(
    name = "icmp_main_init", runs_after = ["ip_lookup_init"],
    runs_before = ["install_packet_graph"],
)]
fn init_icmp_main() -> RuntimeResult<()> {
    IcmpMain::init()
}

hammer_component_macros::declare_plugin!(
    name = "icmp",
    load_after = ["ip"],
    init_functions = [__INIT_FN_ICMP_MAIN_INIT],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [
        icmp::__IP_GRAPH_NODE_ICMP4_INPUT_NODE,
        icmp::__IP_GRAPH_NODE_ICMP6_INPUT_NODE,
        icmp::__IP_GRAPH_NODE_ICMP4_ECHO_REQUEST_NODE,
        icmp::__IP_GRAPH_NODE_ICMP6_ECHO_REQUEST_NODE,
    ],
    node_functions = [],
    process_nodes = [],
    binary_api_methods = []
);

pub fn register_ip4_local(
    nodes: &hammer_runtime::node::NodeRuntime,
    node: NodeId,
) -> RuntimeResult<()> {
    hammer_plugin_ip::register_ip4_protocol(nodes, 1, node)
}

pub fn register_ip6_local(
    nodes: &hammer_runtime::node::NodeRuntime,
    node: NodeId,
) -> RuntimeResult<()> {
    hammer_plugin_ip::register_ip6_protocol(nodes, 58, node)
}
