pub mod icmp;
pub mod input;
pub mod local;
pub mod reassembly;

use hammer_adapter::Network;
pub use hammer_core::protocol::ip::{
    IpFragmentKey, IpInputError, IpInputTarget, IpProtocol, IpVersion, ParsedIpFragment,
    ParsedIpPacket, parse_ip_fragment, parse_ip_fragment_with_chain_len, parse_ip_packet,
    parse_ip_packet_with_chain_len,
};
pub use icmp::{IcmpInputControlPlane, IcmpInputError, IcmpInputNode};
pub use input::{IpInputNext, IpInputNode, IpUnicastArc};
pub use local::{
    IpLocalArc, IpLocalControlPlane, IpLocalError, IpLocalNext, IpLocalNode, IpLocalSourceCheck,
    IpReceiveNode,
};
pub use reassembly::{
    IpReassemblyDirectory, IpReassemblyHandoff, IpReassemblyNext, IpReassemblyNode,
};

#[inline(always)]
pub(crate) fn network_for_protocol(protocol: IpProtocol) -> Option<Network> {
    match protocol {
        IpProtocol::Tcp => Some(Network::Tcp),
        IpProtocol::Udp => Some(Network::Udp),
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => Some(Network::Icmp),
        IpProtocol::Other(_) => None,
    }
}
