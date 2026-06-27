use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::error::{CoreError, CoreResult};
use crate::protocol::wire::read_header;
use hammer_infra::checksum::internet_checksum;

const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const IPV4_FLAG_MORE_FRAGMENTS: u16 = 0x2000;
const IPV4_FRAGMENT_OFFSET_MASK: u16 = 0x1fff;
const IPV6_NEXT_HEADER_FRAGMENT: u8 = 44;
const IPV6_FRAGMENT_HEADER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpProtocol {
    Icmpv4,
    Tcp,
    Udp,
    Icmpv6,
    Other(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpInputTarget {
    Drop,
    Punt,
    Options,
    Lookup,
    LookupMulticast,
    IcmpError,
    Reassembly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpInputError {
    None,
    Version,
    HeaderTooShort,
    Options,
    BadChecksum,
    TimeExpired,
    FragmentOffsetOne,
    TooShort,
    BadLength,
}

impl IpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpVersion {
    V4,
    V6,
}

impl From<u8> for IpProtocol {
    #[inline(always)]
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Icmpv4,
            6 => Self::Tcp,
            17 => Self::Udp,
            58 => Self::Icmpv6,
            other => Self::Other(other),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParsedIpPacket {
    pub version: IpVersion,
    pub protocol: IpProtocol,
    pub input_target: IpInputTarget,
    pub input_error: IpInputError,
    pub source: IpAddr,
    pub destination: IpAddr,
    pub packet_len: usize,
    pub network_header_offset: usize,
    pub network_header_len: usize,
    pub transport_header_offset: usize,
    pub transport_header_len: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct ParsedIpFragment {
    pub version: IpVersion,
    pub key: IpFragmentKey,
    pub payload_offset: usize,
    pub payload_len: usize,
    pub more_fragments: bool,
    pub header_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IpFragmentKey {
    V4 {
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        identification: u16,
    },
    V6 {
        source: Ipv6Addr,
        destination: Ipv6Addr,
        next_header: u8,
        identification: u32,
    },
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct Ipv4Header {
    version_ihl: u8,
    dscp_ecn: u8,
    total_len: [u8; 2],
    identification: [u8; 2],
    flags_fragment: [u8; 2],
    ttl: u8,
    protocol: u8,
    checksum: [u8; 2],
    source: [u8; 4],
    destination: [u8; 4],
}

impl Ipv4Header {
    #[inline(always)]
    pub fn version(self) -> u8 {
        self.version_ihl >> 4
    }

    #[inline(always)]
    pub fn header_len(self) -> usize {
        usize::from(self.version_ihl & 0x0f) * 4
    }

    #[inline(always)]
    pub fn total_len(self) -> usize {
        usize::from(u16::from_be_bytes(self.total_len))
    }

    #[inline(always)]
    pub fn identification(self) -> u16 {
        u16::from_be_bytes(self.identification)
    }

    #[inline(always)]
    pub fn flags_fragment(self) -> u16 {
        u16::from_be_bytes(self.flags_fragment)
    }

    #[inline(always)]
    pub fn protocol(self) -> u8 {
        self.protocol
    }

    #[inline(always)]
    pub fn source(self) -> Ipv4Addr {
        Ipv4Addr::from(self.source)
    }

    #[inline(always)]
    pub fn destination(self) -> Ipv4Addr {
        Ipv4Addr::from(self.destination)
    }
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct Ipv6Header {
    version_traffic_flow: [u8; 4],
    payload_len: [u8; 2],
    next_header: u8,
    hop_limit: u8,
    source: [u8; 16],
    destination: [u8; 16],
}

impl Ipv6Header {
    #[inline(always)]
    pub fn version(self) -> u8 {
        self.version_traffic_flow[0] >> 4
    }

    #[inline(always)]
    pub fn payload_len(self) -> usize {
        usize::from(u16::from_be_bytes(self.payload_len))
    }

    #[inline(always)]
    pub fn next_protocol(self) -> u8 {
        self.next_header
    }

    #[inline(always)]
    pub fn source(self) -> Ipv6Addr {
        Ipv6Addr::from(self.source)
    }

    #[inline(always)]
    pub fn destination(self) -> Ipv6Addr {
        Ipv6Addr::from(self.destination)
    }
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct Ipv6FragmentHeader {
    next_header: u8,
    reserved: u8,
    offset_more: [u8; 2],
    identification: [u8; 4],
}

impl Ipv6FragmentHeader {
    #[inline(always)]
    pub fn next_protocol(self) -> u8 {
        self.next_header
    }

    #[inline(always)]
    pub fn offset_more(self) -> u16 {
        u16::from_be_bytes(self.offset_more)
    }

    #[inline(always)]
    pub fn identification(self) -> u32 {
        u32::from_be_bytes(self.identification)
    }
}

pub fn parse_ip_header(packet: &[u8]) -> CoreResult<ParsedIpPacket> {
    let Some(first) = packet.first().copied() else {
        return Err(CoreError::internal("empty IP packet"));
    };
    match first >> 4 {
        4 => parse_ipv4_packet_header(packet),
        6 => parse_ipv6_packet_header(packet),
        other => Err(CoreError::internal(format!(
            "unsupported IP version: {other}"
        ))),
    }
}

pub fn parse_ip_fragment(packet: &[u8]) -> CoreResult<ParsedIpFragment> {
    parse_ip_fragment_with_chain_len(packet, 0)
}

pub fn parse_ip_fragment_with_chain_len(
    packet: &[u8],
    tail_len: usize,
) -> CoreResult<ParsedIpFragment> {
    let Some(first) = packet.first().copied() else {
        return Err(CoreError::internal("empty IP packet"));
    };
    let chain_len = packet.len().saturating_add(tail_len);
    match first >> 4 {
        4 => parse_ipv4_fragment(packet, chain_len),
        6 => parse_ipv6_fragment(packet, chain_len),
        other => Err(CoreError::internal(format!(
            "unsupported IP version: {other}"
        ))),
    }
}

#[inline(always)]
fn parse_ipv4_packet_header(packet: &[u8]) -> CoreResult<ParsedIpPacket> {
    let header = read_header::<Ipv4Header>(packet, 0)
        .map_err(|_| CoreError::internal("ipv4 header is too short"))?;
    if header.version() != 4 {
        return Err(CoreError::internal("invalid ipv4 version"));
    }
    let ihl = header.header_len();
    if ihl < IPV4_HEADER_MIN_LEN || packet.len() < ihl {
        return Err(CoreError::internal("invalid ipv4 header length"));
    }

    let total_len = header.total_len();
    if total_len < ihl {
        return Err(CoreError::internal("invalid ipv4 packet length"));
    }

    let fragment = header.flags_fragment();
    let fragment_offset = fragment & IPV4_FRAGMENT_OFFSET_MASK;
    let checksum_bad = internet_checksum(&packet[..ihl]) != 0;
    let destination = header.destination();
    let (input_target, input_error) =
        if fragment_offset == 1 || checksum_bad || total_len < IPV4_HEADER_MIN_LEN {
            (
                IpInputTarget::Drop,
                if fragment_offset == 1 {
                    IpInputError::FragmentOffsetOne
                } else if checksum_bad {
                    IpInputError::BadChecksum
                } else {
                    IpInputError::TooShort
                },
            )
        } else if header.ttl < 1 {
            (IpInputTarget::IcmpError, IpInputError::TimeExpired)
        } else if ihl != IPV4_HEADER_MIN_LEN {
            (IpInputTarget::Options, IpInputError::Options)
        } else if fragment & (IPV4_FLAG_MORE_FRAGMENTS | IPV4_FRAGMENT_OFFSET_MASK) != 0 {
            (IpInputTarget::Reassembly, IpInputError::None)
        } else if destination.is_multicast() {
            (IpInputTarget::LookupMulticast, IpInputError::None)
        } else {
            (IpInputTarget::Lookup, IpInputError::None)
        };

    Ok(ParsedIpPacket {
        version: IpVersion::V4,
        protocol: IpProtocol::from(header.protocol),
        input_target,
        input_error,
        source: IpAddr::V4(header.source()),
        destination: IpAddr::V4(destination),
        packet_len: total_len,
        network_header_offset: 0,
        network_header_len: ihl,
        transport_header_offset: ihl,
        transport_header_len: 0,
    })
}

#[inline(always)]
fn parse_ipv4_fragment(packet: &[u8], chain_len: usize) -> CoreResult<ParsedIpFragment> {
    let header = read_header::<Ipv4Header>(packet, 0)
        .map_err(|_| CoreError::internal("ipv4 fragment header is too short"))?;
    if header.version() != 4 {
        return Err(CoreError::internal("invalid ipv4 version"));
    }
    let ihl = header.header_len();
    if ihl < IPV4_HEADER_MIN_LEN || packet.len() < ihl {
        return Err(CoreError::internal("invalid ipv4 header length"));
    }

    let total_len = header.total_len();
    if total_len < ihl || total_len > chain_len {
        return Err(CoreError::internal("invalid ipv4 fragment length"));
    }

    let fragment = header.flags_fragment();
    let payload_len = total_len - ihl;
    let payload_offset = usize::from(fragment & IPV4_FRAGMENT_OFFSET_MASK) * 8;
    let more_fragments = fragment & IPV4_FLAG_MORE_FRAGMENTS != 0;
    if payload_offset == 0 && !more_fragments {
        return Err(CoreError::internal("ipv4 packet is not fragmented"));
    }

    Ok(ParsedIpFragment {
        version: IpVersion::V4,
        key: IpFragmentKey::V4 {
            source: header.source(),
            destination: header.destination(),
            protocol: header.protocol,
            identification: header.identification(),
        },
        payload_offset,
        payload_len,
        more_fragments,
        header_len: ihl,
    })
}

#[inline(always)]
fn parse_ipv6_packet_header(packet: &[u8]) -> CoreResult<ParsedIpPacket> {
    let header = read_header::<Ipv6Header>(packet, 0)
        .map_err(|_| CoreError::internal("ipv6 header is too short"))?;
    if header.version() != 6 {
        return Err(CoreError::internal("invalid ipv6 version"));
    }
    if packet.len() < IPV6_HEADER_LEN {
        return Err(CoreError::internal("ipv6 header is truncated"));
    }

    let payload_len = header.payload_len();
    let total_len = IPV6_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| CoreError::internal("ipv6 payload length overflow"))?;

    let source = header.source();
    let destination = header.destination();
    let (protocol, input_target, input_error, transport_offset) =
        if header.next_header == IPV6_NEXT_HEADER_FRAGMENT {
            if payload_len < IPV6_FRAGMENT_HEADER_LEN {
                return Err(CoreError::internal("ipv6 fragment header is truncated"));
            }
            let fragment = read_header::<Ipv6FragmentHeader>(packet, IPV6_HEADER_LEN)
                .map_err(|_| CoreError::internal("ipv6 fragment header is missing"))?;
            (
                fragment.next_header,
                if header.hop_limit < 1 {
                    IpInputTarget::IcmpError
                } else {
                    IpInputTarget::Reassembly
                },
                if header.hop_limit < 1 {
                    IpInputError::TimeExpired
                } else {
                    IpInputError::None
                },
                IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN,
            )
        } else if header.hop_limit < 1 {
            (
                header.next_header,
                IpInputTarget::IcmpError,
                IpInputError::TimeExpired,
                IPV6_HEADER_LEN,
            )
        } else if destination.is_multicast() {
            (
                header.next_header,
                IpInputTarget::LookupMulticast,
                IpInputError::None,
                IPV6_HEADER_LEN,
            )
        } else {
            (
                header.next_header,
                IpInputTarget::Lookup,
                IpInputError::None,
                IPV6_HEADER_LEN,
            )
        };

    Ok(ParsedIpPacket {
        version: IpVersion::V6,
        protocol: IpProtocol::from(protocol),
        input_target,
        input_error,
        source: IpAddr::V6(source),
        destination: IpAddr::V6(destination),
        packet_len: total_len,
        network_header_offset: 0,
        network_header_len: IPV6_HEADER_LEN,
        transport_header_offset: transport_offset,
        transport_header_len: 0,
    })
}

#[inline(always)]
fn parse_ipv6_fragment(packet: &[u8], chain_len: usize) -> CoreResult<ParsedIpFragment> {
    let header = read_header::<Ipv6Header>(packet, 0)
        .map_err(|_| CoreError::internal("ipv6 header is too short"))?;
    if header.version() != 6 {
        return Err(CoreError::internal("invalid ipv6 version"));
    }
    if packet.len() < IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN {
        return Err(CoreError::internal("ipv6 fragment header is truncated"));
    }

    let payload_len = header.payload_len();
    let total_len = IPV6_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| CoreError::internal("ipv6 payload length overflow"))?;
    if total_len > chain_len {
        return Err(CoreError::internal("invalid ipv6 fragment length"));
    }
    if payload_len < IPV6_FRAGMENT_HEADER_LEN {
        return Err(CoreError::internal("ipv6 fragment payload is too short"));
    }
    if header.next_header != IPV6_NEXT_HEADER_FRAGMENT {
        return Err(CoreError::internal("ipv6 packet is not fragmented"));
    }

    let fragment = read_header::<Ipv6FragmentHeader>(packet, IPV6_HEADER_LEN)
        .map_err(|_| CoreError::internal("ipv6 fragment header is missing"))?;
    let offset_more = fragment.offset_more();
    let payload_len = payload_len - IPV6_FRAGMENT_HEADER_LEN;
    let payload_offset = usize::from(offset_more >> 3) * 8;
    let more_fragments = offset_more & 1 != 0;
    if payload_offset == 0 && !more_fragments {
        return Err(CoreError::internal("ipv6 packet is not fragmented"));
    }

    Ok(ParsedIpFragment {
        version: IpVersion::V6,
        key: IpFragmentKey::V6 {
            source: header.source(),
            destination: header.destination(),
            next_header: fragment.next_header,
            identification: fragment.identification(),
        },
        payload_offset,
        payload_len,
        more_fragments,
        header_len: IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN,
    })
}
