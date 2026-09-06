#[path = "input.rs"]
pub mod input;
#[path = "local.rs"]
pub mod local;
#[path = "reassembly.rs"]
pub mod reassembly;

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum IpRoutePathBehavior {
    Normal = 0,
    Local = 1,
    Drop = 2,
    UdpEncap = 3,
    IcmpUnreachable = 4,
    IcmpProhibit = 5,
    SourceLookup = 6,
    Dvr = 7,
    InterfaceRx = 8,
    Classify = 9,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct IpPathFlags: u32 {
        const RESOLVE_VIA_HOST = 1 << 0;
        const RESOLVE_VIA_ATTACHED = 1 << 1;
        const LOCAL = 1 << 2;
        const ATTACHED = 1 << 3;
        const DROP = 1 << 4;
        const EXCLUSIVE = 1 << 5;
        const INTF_RX = 1 << 6;
        const RPF_ID = 1 << 7;
        const SOURCE_LOOKUP = 1 << 8;
        const UDP_ENCAP = 1 << 9;
        const DEAG = 1 << 13;
        const DVR = 1 << 14;
        const ICMP_UNREACH = 1 << 15;
        const ICMP_PROHIBIT = 1 << 16;
        const CLASSIFY = 1 << 17;
        const GLEAN = 1 << 19;
    }
}

use std::net::IpAddr;

use crate::protocol::ip::{
    IPV6_NEXT_HEADER_AH, IPV6_NEXT_HEADER_DESTINATION, IPV6_NEXT_HEADER_FRAGMENT,
    IPV6_NEXT_HEADER_HOP_BY_HOP, IPV6_NEXT_HEADER_ROUTING, IpFragmentKey, IpInputError,
    IpInputTarget, IpProtocol, IpVersion, Ipv4Header, Ipv6FragmentHeader, Ipv6Header,
    ParsedIpFragment, ParsedIpPacket, parse_ip_fragment_with_chain_len, parse_ip_header,
};
use crate::protocol::wire::read_header;
use hammer_core::data_plane::BufferPacketCursor;

/// Runtime registries owned by the IP plugin. Mirrors VPP's per-node error
/// enumeration style: the registry identity is a typed discriminant, not a
/// string payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpRuntimeRegistry {
    IpInput,
    IpLocal,
}

impl std::fmt::Display for IpRuntimeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::IpInput => "ip-input",
            Self::IpLocal => "ip-local",
        })
    }
}

/// Control-plane operations that require IP plugin runtime state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpControlOperation {
    IpProtocolRegistration,
}

impl std::fmt::Display for IpControlOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::IpProtocolRegistration => "ip protocol registration",
        })
    }
}

/// Recoverable control-plane failures shared by IP graph-node registration,
/// worker sync, and per-node runtime registry access.
#[hammer_component_macros::runtime_error(subsystem = "ip")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum IpControlError {
    #[error("{registry} runtime registry is poisoned")]
    RuntimeRegistryPoisoned { registry: IpRuntimeRegistry },
    #[error("{registry} runtime slot {slot} is not registered")]
    RuntimeSlotInvalid {
        registry: IpRuntimeRegistry,
        slot: usize,
    },
    #[error("{operation} requires a node runtime")]
    NodeRuntimeUnavailable { operation: IpControlOperation },
}

pub use input::{Ip4InputNext, Ip4InputNode, Ip6InputNext, Ip6InputNode, IpInputTrace};
pub use local::{
    Ip4LocalNext, Ip4LocalNode, Ip4ReceiveNode, Ip6LocalNext, Ip6LocalNode, Ip6ReceiveNode,
    IpLocalError, IpLocalTrace, IpLocalTraceStage,
};
pub use reassembly::{
    Ip4ReassemblyNext, Ip4ReassemblyNode, Ip6ReassemblyNext, Ip6ReassemblyNode,
    IpReassemblyDirectory, IpReassemblyHandoff, IpReassemblyTrace, IpReassemblyTraceAction,
    pack_fragment_owner_value, unpack_fragment_owner_value,
};

#[inline(always)]
pub fn ip_header(
    packet: &[u8],
    cursor: BufferPacketCursor,
) -> Result<ParsedIpPacket, IpInputError> {
    if cursor.packet_len() == 0 {
        return Err(IpInputError::BadLength);
    }
    let Some(version_byte) = packet.get(cursor.network_header_offset()).copied() else {
        return Err(IpInputError::HeaderTooShort);
    };
    let mut transport_header_offset = cursor.transport_header_offset();
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
            let mut protocol = header.next_protocol();
            let mut transport_offset = cursor.network_header_offset().saturating_add(40);
            let mut non_initial_fragment = false;
            loop {
                let extension_length = match protocol {
                    IPV6_NEXT_HEADER_FRAGMENT => {
                        let fragment = read_header::<Ipv6FragmentHeader>(packet, transport_offset)?;
                        non_initial_fragment |= fragment.offset_more() & 0xfff8 != 0;
                        protocol = fragment.next_protocol();
                        8
                    }
                    IPV6_NEXT_HEADER_HOP_BY_HOP
                    | IPV6_NEXT_HEADER_ROUTING
                    | IPV6_NEXT_HEADER_DESTINATION => {
                        let extension_end = transport_offset
                            .checked_add(2)
                            .ok_or(IpInputError::BadLength)?;
                        let Some(extension) = packet.get(transport_offset..extension_end) else {
                            return Err(IpInputError::HeaderTooShort);
                        };
                        protocol = extension[0];
                        (usize::from(extension[1]) + 1)
                            .checked_mul(8)
                            .ok_or(IpInputError::BadLength)?
                    }
                    IPV6_NEXT_HEADER_AH => {
                        let extension_end = transport_offset
                            .checked_add(2)
                            .ok_or(IpInputError::BadLength)?;
                        let Some(extension) = packet.get(transport_offset..extension_end) else {
                            return Err(IpInputError::HeaderTooShort);
                        };
                        protocol = extension[0];
                        (usize::from(extension[1]) + 2)
                            .checked_mul(4)
                            .ok_or(IpInputError::BadLength)?
                    }
                    _ => break,
                };
                let next_offset = transport_offset
                    .checked_add(extension_length)
                    .ok_or(IpInputError::BadLength)?;
                if packet.get(transport_offset..next_offset).is_none() {
                    return Err(IpInputError::HeaderTooShort);
                }
                transport_offset = next_offset;
            }
            transport_header_offset = if non_initial_fragment {
                packet.len()
            } else {
                transport_offset
            };
            (
                IpVersion::V6,
                IpProtocol::from(protocol),
                IpAddr::V6(header.source()),
                IpAddr::V6(header.destination()),
            )
        }
        _ => return Err(IpInputError::Version),
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
        transport_header_offset,
        transport_header_len: cursor.transport_header_len(),
    })
}
