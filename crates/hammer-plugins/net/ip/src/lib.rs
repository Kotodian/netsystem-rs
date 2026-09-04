//! Dynamic `ip` plugin (`libhammer_plugin_ip`).

use hammer_core::data_plane::NodeId;
use hammer_runtime::RuntimeResult;
use hammer_service::net::{DpoId, DpoProto};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpNullAction {
    Drop,
    IcmpUnreachable,
    IcmpProhibit,
}

#[derive(Debug, Clone, Copy, hammer_component_macros::DpoClass)]
#[dpo_class(nodes = [(DpoProto::IP4, ip4_null_node), (DpoProto::IP6, ip6_null_node)])]
pub struct IpNullDpo {
    pub action: IpNullAction,
}

#[derive(Debug, Clone, Copy, hammer_component_macros::DpoClass)]
#[dpo_class(nodes = [(DpoProto::IP4, ip4_pmtu_node), (DpoProto::IP6, ip6_pmtu_node)])]
pub struct IpPmtuDpo {
    pub proto: DpoProto,
    pub pmtu: u16,
    pub published_roots: u16,
    pub stacked: DpoId,
}

hammer_component_macros::declare_plugin!(
    name = "ip",
    load_after = [],
    init_functions = [
        ip::reassembly::__INIT_FN_IP_REASSEMBLY_INIT,
        lookup::__INIT_FN_IP_LOOKUP_INIT,
    ],
    config_functions = [],
    early_config_functions = [ip::reassembly::__CONFIG_FN_IP_REASSEMBLY_CONFIG,],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [
        ip::input::__IP_GRAPH_NODE_IP_INPUT_NODE,
        ip::reassembly::__IP_GRAPH_NODE_IP_REASSEMBLY_NODE,
        ip::local::__IP_GRAPH_NODE_IP_LOCAL_NODE,
        ip::local::__IP_GRAPH_NODE_IP_RECEIVE_NODE,
        lookup::__IP_GRAPH_NODE_IP4_LOOKUP_NODE,
        lookup::__IP_GRAPH_NODE_IP6_LOOKUP_NODE,
        lookup::__IP_GRAPH_NODE_IP4_LOAD_BALANCE_NODE,
        lookup::__IP_GRAPH_NODE_IP6_LOAD_BALANCE_NODE,
    ],
    node_functions = [],
    process_nodes = [ip::reassembly::__PROCESS_NODE_IP_REASSEMBLY_EXPIRE_WALK],
);

mod config;
mod fib;
pub mod ip;
mod lookup;
pub mod pmtu;
pub mod protocol;

pub fn register_ip4_protocol(
    nodes: &hammer_runtime::node::NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    ip::local::register_ip4_protocol(nodes, protocol, node)
}

pub fn register_ip6_protocol(
    nodes: &hammer_runtime::node::NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    ip::local::register_ip6_protocol(nodes, protocol, node)
}

pub fn path_mtu() -> Option<&'static pmtu::IpPathMtu> {
    pmtu::path_mtu()
}

pub use ip::{
    IpInputNext, IpInputNode, IpInputTrace, IpLocalArc, IpLocalControlPlane, IpLocalError,
    IpLocalNext, IpLocalNode, IpLocalTrace, IpLocalTraceStage, IpReassemblyDirectory,
    IpReassemblyHandoff, IpReassemblyNext, IpReassemblyNode, IpReassemblyTrace,
    IpReassemblyTraceAction, IpReceiveNode, IpUnicastArc, pack_fragment_owner_value,
    unpack_fragment_owner_value,
};
pub use ip::{IpPathFlags, IpRoutePathBehavior};
pub use protocol::ip::{write_ipv4_push_header, write_ipv6_push_header};
