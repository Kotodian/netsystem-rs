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
        lookup::__INIT_FN_IP_INIT,
    ],
    config_functions = [],
    early_config_functions = [
        ip::reassembly::__CONFIG_FN_IP_REASSEMBLY_CONFIG,
        lookup::__CONFIG_FN_IP_CONFIG,
    ],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [
        ip::input::__IP_GRAPH_NODE_IP_INPUT_NODE,
        ip::icmp::__IP_GRAPH_NODE_ICMP_INPUT_NODE,
        ip::icmp::__IP_GRAPH_NODE_ICMP_ECHO_REQUEST_NODE,
        ip::icmp::__IP_GRAPH_NODE_ICMP_PATH_MTU_NODE,
        ip::icmp::__IP_GRAPH_NODE_ICMP_ERROR_NODE,
        ip::reassembly::__IP_GRAPH_NODE_IP_REASSEMBLY_NODE,
        ip::local::__IP_GRAPH_NODE_IP_LOCAL_NODE,
        ip::local::__IP_GRAPH_NODE_IP_RECEIVE_NODE,
        lookup::__SERVICE_GRAPH_NODE_IP_LOOKUP_NODE,
        lookup::__SERVICE_GRAPH_NODE_ADJACENCY_REWRITE_NODE,
    ],
    node_functions = [],
    process_nodes = [ip::reassembly::__PROCESS_NODE_IP_REASSEMBLY_EXPIRE_WALK],
);

mod config;
pub mod forwarding;
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
    IcmpEchoRequestNext, IcmpEchoRequestNode, IcmpEchoRequestTrace, IcmpErrorNext, IcmpErrorNode,
    IcmpErrorSourceTable, IcmpErrorSourceTableHandle, IcmpErrorTrace, IcmpInputControlPlane,
    IcmpInputError, IcmpInputNext, IcmpInputNode, IcmpInputTrace, IcmpNodeError, IcmpPathMtuNode,
    IpInputNext, IpInputNode, IpInputTrace, IpLocalArc, IpLocalControlPlane, IpLocalError,
    IpLocalNext, IpLocalNode, IpLocalSourceCheck, IpLocalTrace, IpLocalTraceStage,
    IpReassemblyDirectory, IpReassemblyHandoff, IpReassemblyNext, IpReassemblyNode,
    IpReassemblyTrace, IpReassemblyTraceAction, IpReceiveNode, IpUnicastArc,
    pack_fragment_owner_value, unpack_fragment_owner_value,
};
pub use ip::{IpPathFlags, IpRoutePathBehavior};
pub use lookup::{
    AdjacencyRewriteNext, AdjacencyRewriteNode, AdjacencyRewriteNodeError, AdjacencyRewriteTrace,
    IpLookupControlPlane, IpLookupNext, IpLookupNode, IpLookupTrace,
};
pub use protocol::ip::{write_ipv4_push_header, write_ipv6_push_header};
