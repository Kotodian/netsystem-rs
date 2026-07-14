//! Dynamic `ip` plugin (`libhammer_plugin_ip`).

hammer_component_macros::declare_plugin!(name = "ip", load_after = []);

pub mod ip;
mod lookup;

pub use ip::{
    IcmpEchoRequestNext, IcmpEchoRequestNode, IcmpEchoRequestTrace, IcmpErrorNext, IcmpErrorNode,
    IcmpErrorSourceTable, IcmpErrorSourceTableHandle, IcmpErrorTrace, IcmpInputControlPlane,
    IcmpInputError, IcmpInputNode, IcmpInputTrace, IcmpNodeError, IcmpPathMtuNode, IpInputNext,
    IpInputNode, IpInputTrace, IpLocalArc, IpLocalControlPlane, IpLocalError, IpLocalNext,
    IpLocalNode, IpLocalSourceCheck, IpLocalTrace, IpLocalTraceStage, IpReassemblyDirectory,
    IpReassemblyHandoff, IpReassemblyNext, IpReassemblyNode, IpReassemblyTrace,
    IpReassemblyTraceAction, IpReceiveNode, IpUnicastArc, pack_fragment_owner_value,
    unpack_fragment_owner_value,
};
pub use lookup::{
    AdjacencyRewriteNode, AdjacencyRewriteNodeError, AdjacencyRewriteTrace, IpLookupControlPlane,
    IpLookupNode, IpLookupTrace,
};
pub fn reset_ip_main_for_test() {
    lookup::reset_for_test();
    hammer_service::net::pmtu::reset_path_mtu_cache_for_test();
}
