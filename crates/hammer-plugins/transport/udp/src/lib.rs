//! Dynamic `udp` plugin (`libhammer_plugin_udp`).

use abi_stable::StableAbi;
use hammer_core::data_plane::NodeId;
use hammer_runtime::RuntimeResult;

pub mod input;
mod wire;

#[derive(Debug, Clone, Copy, PartialEq, Eq, StableAbi, serde::Deserialize, serde::Serialize)]
#[repr(u8)]
pub enum UdpIpVersion {
    V4 = 0,
    V6 = 1,
}

pub fn register_dst_port(version: UdpIpVersion, port: u16, node: NodeId) -> RuntimeResult<()> {
    input::register_dst_port(version, port, node)
}

pub fn unregister_dst_port(version: UdpIpVersion, port: u16, node: NodeId) -> RuntimeResult<()> {
    input::unregister_dst_port(version, port, node)
}

hammer_component_macros::declare_plugin!(
    name = "udp",
    load_after = ["ip"],
    init_functions = [worker::__INIT_FN_UDP_INIT],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [worker::__INIT_FN_UDP_WORKER_INIT],
    graph_nodes = [
        input::__UDP_GRAPH_NODE_UDP_INPUT_NODE,
        output::__UDP_WORKER_GRAPH_NODE_UDP_OUTPUT_NODE,
    ],
    node_functions = [],
    process_nodes = [],
);

pub use input::{
    UdpControlError, UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace,
};
pub use output::{UdpOutputNext, UdpOutputNode};
pub use worker::{UdpWorker, protocol};

mod connection;
pub(crate) mod lookup;
pub mod output;
pub(crate) mod worker;
