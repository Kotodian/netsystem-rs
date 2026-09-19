//! Dynamic `ip` plugin (`libhammer_plugin_ip`).

use hammer_core::data_plane::NodeId;
use hammer_runtime::RuntimeResult;
use hammer_service::net::{DpoId, DpoProto};

hammer_service::declare_interface_registration_image!(
    hw_callbacks = [],
    sw_callbacks = [
        interface::IP_SW_INTERFACE_CALLBACKS[0],
        interface::IP_SW_INTERFACE_CALLBACKS[1],
        interface::IP_SW_INTERFACE_CALLBACKS[2],
    ]
);

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
        interface::__INIT_FN_IP6_LINK_INIT,
    ],
    config_functions = [ip::reassembly::__CONFIG_FN_IP_REASSEMBLY_CONFIG,],
    main_loop_enter_functions = [
        punt::__INIT_FN_IP_FEATURE_INIT,
        interface::__INIT_FN_IP_INTERFACE_FEATURE_INIT,
    ],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [
        icmp_error::__IP_GRAPH_NODE_IP4_ICMP_ERROR_NODE,
        icmp_error::__IP_GRAPH_NODE_IP6_ICMP_ERROR_NODE,
        ip::input::__IP_GRAPH_NODE_IP4_INPUT_NODE,
        ip::input::__IP_GRAPH_NODE_IP6_INPUT_NODE,
        ip::reassembly::__IP_GRAPH_NODE_IP4_REASSEMBLY_NODE,
        ip::reassembly::__IP_GRAPH_NODE_IP6_REASSEMBLY_NODE,
        ip::local::__IP_GRAPH_NODE_IP4_LOCAL_END_OF_ARC_NODE,
        ip::local::__IP_GRAPH_NODE_IP6_LOCAL_END_OF_ARC_NODE,
        punt::__IP_GRAPH_NODE_IP4_PUNT_NODE,
        punt::__IP_GRAPH_NODE_IP4_DROP_NODE,
        punt::__IP_GRAPH_NODE_IP4_NOT_ENABLED_NODE,
        punt::__IP_GRAPH_NODE_IP6_PUNT_NODE,
        punt::__IP_GRAPH_NODE_IP6_DROP_NODE,
        punt::__IP_GRAPH_NODE_IP6_NOT_ENABLED_NODE,
        ip::local::__IP_GRAPH_NODE_IP4_LOCAL_NODE,
        ip::local::__IP_GRAPH_NODE_IP6_LOCAL_NODE,
        ip::local::__IP_GRAPH_NODE_IP4_RECEIVE_NODE,
        ip::local::__IP_GRAPH_NODE_IP6_RECEIVE_NODE,
        lookup::__IP_GRAPH_NODE_IP4_LOOKUP_NODE,
        lookup::__IP_GRAPH_NODE_IP6_LOOKUP_NODE,
        lookup::__IP_GRAPH_NODE_IP4_LOAD_BALANCE_NODE,
        lookup::__IP_GRAPH_NODE_IP6_LOAD_BALANCE_NODE,
        lookup::__IP_GRAPH_NODE_IP4_INTERFACE_RX_NODE,
        lookup::__IP_GRAPH_NODE_IP6_INTERFACE_RX_NODE,
    ],
    node_functions = [],
    process_nodes = [ip::reassembly::__PROCESS_NODE_IP_REASSEMBLY_EXPIRE_WALK],
);

mod config;
mod fib;
mod icmp_error;
mod interface;
pub mod ip;
mod lookup;
mod punt;
use ip::local;
pub use ip::local::{unregister_ip4_protocol, unregister_ip6_protocol};
pub mod pmtu;
pub mod protocol;

pub fn register_ip4_protocol(
    nodes: &hammer_runtime::node::NodeMain,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    ip::local::register_ip4_protocol(nodes, protocol, node)
}

pub fn register_ip6_protocol(
    nodes: &hammer_runtime::node::NodeMain,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    ip::local::register_ip6_protocol(nodes, protocol, node)
}

pub fn path_mtu() -> Option<&'static pmtu::IpPathMtu> {
    pmtu::path_mtu()
}

pub use interface::{
    IpInterfaceAddressCallback, IpInterfaceAddressError, ip4_add_del_interface_address,
    ip4_sw_interface_enable_disable, ip6_add_del_interface_address,
    ip6_sw_interface_enable_disable, register_ip4_add_del_interface_address_callback,
    register_ip6_add_del_interface_address_callback,
};
pub use ip::{
    Ip4InputNext, Ip4InputNode, Ip4LocalNext, Ip4LocalNode, Ip4ReassemblyNext, Ip4ReassemblyNode,
    Ip4ReceiveNode, Ip6InputNext, Ip6InputNode, Ip6LocalNext, Ip6LocalNode, Ip6ReassemblyNext,
    Ip6ReassemblyNode, Ip6ReceiveNode, IpInputTrace, IpLocalError, IpLocalTrace, IpLocalTraceStage,
    IpReassemblyDirectory, IpReassemblyHandoff, IpReassemblyTrace, IpReassemblyTraceAction,
    pack_fragment_owner_value, unpack_fragment_owner_value,
};
pub use ip::{IpPathFlags, IpRoutePathBehavior};
pub use lookup::fib_table_get_index_for_sw_if_index;
pub use protocol::ip::{write_ipv4_push_header, write_ipv6_push_header};

#[cfg(test)]
static BUFFER_MAIN_INIT: std::sync::Once = std::sync::Once::new();

pub use lookup::IpSecondaryOpaque;
