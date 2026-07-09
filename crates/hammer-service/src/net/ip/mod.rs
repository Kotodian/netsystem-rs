pub mod icmp;
pub mod input;
pub mod local;
pub mod reassembly;

use std::net::IpAddr;

use hammer_core::data_plane::BufferPacketCursor;
use hammer_core::error::{CoreError, CoreResult};
pub use hammer_core::protocol::ip::{
    IpFragmentKey, IpInputError, IpInputTarget, IpProtocol, IpVersion, Ipv4Header, Ipv6Header,
    ParsedIpFragment, ParsedIpPacket, parse_ip_fragment, parse_ip_fragment_with_chain_len,
    parse_ip_header,
};
use hammer_core::protocol::wire::read_header;
use hammer_runtime::Network;

pub use icmp::{
    IcmpEchoRequestNext, IcmpEchoRequestNode, IcmpEchoRequestTrace, IcmpErrorNext, IcmpErrorNode,
    IcmpErrorSourceTable, IcmpErrorSourceTableHandle, IcmpErrorTrace, IcmpInputControlPlane,
    IcmpInputError, IcmpInputNode, IcmpInputTrace, IcmpNodeError,
};
pub use input::{IpInputNext, IpInputNode, IpInputTrace, IpUnicastArc};
pub use local::{
    IpLocalArc, IpLocalControlPlane, IpLocalError, IpLocalNext, IpLocalNode, IpLocalSourceCheck,
    IpLocalTrace, IpLocalTraceStage, IpReceiveNode,
};
pub use reassembly::{
    IpReassemblyDirectory, IpReassemblyHandoff, IpReassemblyNext, IpReassemblyNode,
    IpReassemblyTrace, IpReassemblyTraceAction,
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

#[inline(always)]
pub(crate) fn ip_header(packet: &[u8], cursor: BufferPacketCursor) -> CoreResult<ParsedIpPacket> {
    if cursor.packet_len() == 0 {
        return Err(CoreError::internal("missing cached IP packet cursor"));
    }
    let Some(version_byte) = packet.get(cursor.network_header_offset()).copied() else {
        return Err(CoreError::internal("missing cached IP header"));
    };
    let (version, protocol, source, destination) = match version_byte >> 4 {
        4 => {
            let header = read_header::<Ipv4Header>(packet, cursor.network_header_offset())?;
            (
                IpVersion::V4,
                IpProtocol::from(header.protocol()),
                IpAddr::V4(header.source()),
                IpAddr::V4(header.destination()),
            )
        }
        6 => {
            let header = read_header::<Ipv6Header>(packet, cursor.network_header_offset())?;
            let protocol = if cursor.transport_header_offset()
                == cursor.network_header_offset().saturating_add(40)
            {
                header.next_protocol()
            } else {
                read_header::<hammer_core::protocol::ip::Ipv6FragmentHeader>(
                    packet,
                    cursor.transport_header_offset().saturating_sub(8),
                )
                .map(|fragment| fragment.next_protocol())
                .unwrap_or_else(|_| header.next_protocol())
            };
            (
                IpVersion::V6,
                IpProtocol::from(protocol),
                IpAddr::V6(header.source()),
                IpAddr::V6(header.destination()),
            )
        }
        _ => return Err(CoreError::internal("unsupported cached IP version")),
    };
    Ok(ParsedIpPacket {
        version,
        protocol,
        input_target: IpInputTarget::Lookup,
        input_error: IpInputError::None,
        source,
        destination,
        packet_len: cursor.packet_len(),
        network_header_offset: cursor.network_header_offset(),
        network_header_len: cursor.network_header_len(),
        transport_header_offset: cursor.transport_header_offset(),
        transport_header_len: cursor.transport_header_len(),
    })
}
