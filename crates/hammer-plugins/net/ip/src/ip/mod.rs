pub mod icmp;
pub mod input;
pub mod local;
pub mod reassembly;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpRoutePathBehavior {
    ResolveViaHost,
    ResolveViaAttached,
    Local,
    Drop,
    UdpEncap,
    IcmpUnreachable,
    IcmpProhibit,
    SourceLookup,
    Dvr,
    InterfaceRx,
    Classify,
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
    IpFragmentKey, IpInputError, IpInputTarget, IpProtocol, IpVersion, Ipv4Header, Ipv6Header,
    ParsedIpFragment, ParsedIpPacket, parse_ip_fragment_with_chain_len, parse_ip_header,
};
use crate::protocol::wire::read_header;
use hammer_core::data_plane::BufferPacketCursor;
use hammer_runtime::Network;

/// Runtime registries owned by the IP plugin. Mirrors VPP's per-node error
/// enumeration style: the registry identity is a typed discriminant, not a
/// string payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpRuntimeRegistry {
    IpInput,
    IpLocal,
    IpLookup,
    IcmpInput,
    IcmpError,
    AdjacencyRewrite,
}

impl std::fmt::Display for IpRuntimeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::IpInput => "ip-input",
            Self::IpLocal => "ip-local",
            Self::IpLookup => "ip-lookup",
            Self::IcmpInput => "icmp-input",
            Self::IcmpError => "icmp-error",
            Self::AdjacencyRewrite => "adjacency-rewrite",
        })
    }
}

/// Control-plane operations that require IP plugin runtime state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpControlOperation {
    IcmpConsumerAttach,
    IcmpTypeRegistration,
    IpReceiveRegistration,
    IpProtocolRegistration,
}

impl std::fmt::Display for IpControlOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::IcmpConsumerAttach => "icmp consumer attach",
            Self::IcmpTypeRegistration => "icmp type registration",
            Self::IpReceiveRegistration => "ip-receive registration",
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
    #[error("ICMP type registration requires an attached input consumer")]
    ConsumerNotAttached,
}

pub use icmp::{
    IcmpEchoRequestNext, IcmpEchoRequestNode, IcmpEchoRequestTrace, IcmpErrorNext, IcmpErrorNode,
    IcmpErrorSourceTable, IcmpErrorSourceTableHandle, IcmpErrorTrace, IcmpInputControlPlane,
    IcmpInputError, IcmpInputNext, IcmpInputNode, IcmpInputTrace, IcmpNodeError, IcmpPathMtuNode,
};
pub use input::{IpInputNext, IpInputNode, IpInputTrace, IpUnicastArc};
pub use local::{
    IpLocalArc, IpLocalControlPlane, IpLocalError, IpLocalNext, IpLocalNode, IpLocalSourceCheck,
    IpLocalTrace, IpLocalTraceStage, IpReceiveNode,
};
pub use reassembly::{
    IpReassemblyDirectory, IpReassemblyHandoff, IpReassemblyNext, IpReassemblyNode,
    IpReassemblyTrace, IpReassemblyTraceAction, pack_fragment_owner_value,
    unpack_fragment_owner_value,
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
pub(crate) fn ip_header(
    packet: &[u8],
    cursor: BufferPacketCursor,
) -> Result<ParsedIpPacket, IpInputError> {
    if cursor.packet_len() == 0 {
        return Err(IpInputError::BadLength);
    }
    let Some(version_byte) = packet.get(cursor.network_header_offset()).copied() else {
        return Err(IpInputError::HeaderTooShort);
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
                read_header::<crate::protocol::ip::Ipv6FragmentHeader>(
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
        transport_header_offset: cursor.transport_header_offset(),
        transport_header_len: cursor.transport_header_len(),
    })
}
