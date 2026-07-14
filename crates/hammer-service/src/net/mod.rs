pub mod ip;
mod lookup;

hammer_component_macros::declare_plugin!(name = "ip", load_after = []);

pub use crate::opaque::{ForwardingMetadata, IpEcnCodepoint, NetworkOpaque, TapEthernetMetadata};
pub use ip::{
    IcmpEchoRequestNext, IcmpEchoRequestNode, IcmpEchoRequestTrace, IcmpErrorNext, IcmpErrorNode,
    IcmpErrorSourceTable, IcmpErrorSourceTableHandle, IcmpErrorTrace, IcmpInputControlPlane,
    IcmpInputError, IcmpInputNode, IcmpInputTrace, IcmpNodeError, IcmpPathMtuNode, IpInputError,
    IpInputNext, IpInputNode, IpInputTarget, IpInputTrace, IpLocalArc, IpLocalControlPlane,
    IpLocalError, IpLocalNext, IpLocalNode, IpLocalSourceCheck, IpLocalTrace, IpLocalTraceStage,
    IpProtocol, IpReassemblyDirectory, IpReassemblyExpireWalk, IpReassemblyHandoff,
    IpReassemblyNext, IpReassemblyNode, IpReassemblyTrace, IpReassemblyTraceAction, IpReceiveNode,
    IpUnicastArc, IpVersion, PATH_MTU_CACHE, PathMtuCache, pack_fragment_owner_value,
    path_mtu_cache, process_ipv4_icmp_path_mtu_packet, publish_path_mtu_cache,
    reset_path_mtu_cache_for_test, unpack_fragment_owner_value,
};
pub use lookup::{
    Adjacency, AdjacencyIndex, AdjacencyRewriteNode, AdjacencyRewriteNodeError,
    AdjacencyRewriteTrace, Dpo, DpoClass, DpoId, DpoProto, DpoStackRegistry, DpoType,
    DpoTypeRegistry, FibEntry, FibLookupResult, FibRouteDpoError, FibTable, FibTableBuilder,
    FibTableHandle, IpLookupControlPlane, IpLookupNode, IpLookupTrace, LoadBalance,
    LoadBalanceError, LoadBalanceIndex,
};

pub(crate) use lookup::wire_ip_lookup_drop;

pub fn reset_ip_main_for_test() {
    lookup::reset_for_test();
    reset_path_mtu_cache_for_test();
}
