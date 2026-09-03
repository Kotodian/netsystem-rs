use std::sync::OnceLock;

use hammer_core::data_plane::NodeId;
use hammer_runtime::{RuntimeError, RuntimeResult};

#[derive(Debug, Clone, Copy, Default)]
pub struct IcmpMain;

static ICMP_MAIN: OnceLock<IcmpMain> = OnceLock::new();

impl IcmpMain {
    pub fn init() -> RuntimeResult<Self> {
        let main = Self;
        ICMP_MAIN
            .set(main)
            .map_err(|_| RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_plugin_icmp::IcmpMain",
            })?;
        Ok(main)
    }

    pub fn global() -> RuntimeResult<&'static Self> {
        ICMP_MAIN
            .get()
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_plugin_icmp::IcmpMain",
            })
    }
}

#[hammer_component_macros::init_function(name = "icmp_main_init", runs_after = ["ip_init"])]
fn init_icmp_main() -> RuntimeResult<std::sync::Arc<IcmpMain>> {
    Ok(std::sync::Arc::new(IcmpMain::init()?))
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
        hammer_plugin_ip::ip::icmp::__IP_GRAPH_NODE_ICMP_INPUT_NODE,
        hammer_plugin_ip::ip::icmp::__IP_GRAPH_NODE_ICMP_ECHO_REQUEST_NODE,
        hammer_plugin_ip::ip::icmp::__IP_GRAPH_NODE_ICMP_PATH_MTU_NODE,
        hammer_plugin_ip::ip::icmp::__IP_GRAPH_NODE_ICMP_ERROR_NODE,
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

pub fn register_ip4_error(
    nodes: &hammer_runtime::node::NodeRuntime,
    node: NodeId,
) -> RuntimeResult<()> {
    hammer_plugin_ip::register_ip4_protocol(nodes, 1, node)
}

pub fn register_ip6_error(
    nodes: &hammer_runtime::node::NodeRuntime,
    node: NodeId,
) -> RuntimeResult<()> {
    hammer_plugin_ip::register_ip6_protocol(nodes, 58, node)
}
