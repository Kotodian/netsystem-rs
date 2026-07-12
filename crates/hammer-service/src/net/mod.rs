pub mod ip;
mod lookup;
mod opaque;

pub use ip::{
    IcmpEchoRequestNext, IcmpEchoRequestNode, IcmpEchoRequestTrace, IcmpErrorNext, IcmpErrorNode,
    IcmpErrorSourceTable, IcmpErrorSourceTableHandle, IcmpErrorTrace, IcmpInputControlPlane,
    IcmpInputError, IcmpInputNode, IcmpInputTrace, IcmpNodeError, IpInputError, IpInputNext,
    IpInputNode, IpInputTarget, IpInputTrace, IpLocalArc, IpLocalControlPlane, IpLocalError,
    IpLocalNext, IpLocalNode, IpLocalSourceCheck, IpLocalTrace, IpLocalTraceStage, IpProtocol,
    IpReassemblyDirectory, IpReassemblyExpireWalk, IpReassemblyHandoff, IpReassemblyNext,
    IpReassemblyNode, IpReassemblyTrace, IpReassemblyTraceAction, IpReceiveNode, IpUnicastArc,
    IpVersion, pack_fragment_owner_value, unpack_fragment_owner_value,
};
pub use lookup::{
    Adjacency, AdjacencyIndex, AdjacencyRewriteNode, AdjacencyRewriteNodeError,
    AdjacencyRewriteTrace, Dpo, DpoClass, DpoId, DpoProto, DpoStackRegistry, DpoType,
    DpoTypeRegistry, FibEntry, FibLookupResult, FibRouteDpoError, FibTable, FibTableBuilder,
    FibTableHandle, IpLookupControlPlane, IpLookupNode, IpLookupTrace, LoadBalance,
    LoadBalanceError, LoadBalanceIndex,
};

pub(crate) use lookup::wire_ip_lookup_drop;
pub use opaque::{ForwardingMetadata, IpEcnCodepoint, NetworkOpaque, TapEthernetMetadata};

#[cfg(test)]
pub(crate) fn reset_ip_main_for_test() {
    lookup::reset_for_test();
}
