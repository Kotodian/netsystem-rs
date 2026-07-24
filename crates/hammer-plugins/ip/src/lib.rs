//! Dynamic `ip` plugin (`libhammer_plugin_ip`).

use abi_stable::{
    sabi_trait::TD_Opaque,
    std_types::{RSlice, RSliceMut},
};
use hammer_runtime::IpOutput;
use std::net::{Ipv4Addr, Ipv6Addr};

hammer_component_macros::declare_plugin!(
    name = "ip",
    load_after = [],
    ip_output = &IP_OUTPUT,
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
pub mod protocol;

struct IpOutputService;

impl IpOutput for IpOutputService {
    fn write_ipv4_header(
        &self,
        mut output: RSliceMut<'_, u8>,
        source: RSlice<'_, u8>,
        destination: RSlice<'_, u8>,
        protocol: u8,
        total_len: u16,
    ) -> bool {
        let Ok(source) = <[u8; 4]>::try_from(source.as_slice()) else {
            return false;
        };
        let Ok(destination) = <[u8; 4]>::try_from(destination.as_slice()) else {
            return false;
        };
        protocol::ip::write_ipv4_push_header(
            output.as_mut_slice(),
            Ipv4Addr::from(source),
            Ipv4Addr::from(destination),
            protocol,
            total_len,
        )
        .is_ok()
    }

    fn write_ipv6_header(
        &self,
        mut output: RSliceMut<'_, u8>,
        source: RSlice<'_, u8>,
        destination: RSlice<'_, u8>,
        next_header: u8,
        payload_len: u16,
    ) -> bool {
        let Ok(source) = <[u8; 16]>::try_from(source.as_slice()) else {
            return false;
        };
        let Ok(destination) = <[u8; 16]>::try_from(destination.as_slice()) else {
            return false;
        };
        protocol::ip::write_ipv6_push_header(
            output.as_mut_slice(),
            Ipv6Addr::from(source),
            Ipv6Addr::from(destination),
            next_header,
            payload_len,
        )
        .is_ok()
    }
}

static IP_OUTPUT: hammer_runtime::IpOutput_CTO<'static, 'static> =
    hammer_runtime::IpOutput_CTO::from_const(&IpOutputService, TD_Opaque);

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
pub use lookup::{
    AdjacencyRewriteNext, AdjacencyRewriteNode, AdjacencyRewriteNodeError, AdjacencyRewriteTrace,
    IpLookupControlPlane, IpLookupNext, IpLookupNode, IpLookupTrace,
};
pub fn reset_ip_main_for_test() {
    lookup::reset_for_test();
    hammer_service::net::pmtu::reset_path_mtu_cache_for_test();
}
