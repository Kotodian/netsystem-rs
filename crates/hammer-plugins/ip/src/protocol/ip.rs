use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::wire::{header_mut_ptr, read_header};
use hammer_infra::bihash::{BihashKey, hash_words, splitmix64};
use hammer_infra::checksum::internet_checksum;

const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const IPV4_FLAG_MORE_FRAGMENTS: u16 = 0x2000;
/// IPv4 Don't Fragment flag (RFC 791).
pub const IPV4_FLAG_DONT_FRAGMENT: u16 = 0x4000;
const IPV4_FRAGMENT_OFFSET_MASK: u16 = 0x1fff;
const IPV6_NEXT_HEADER_FRAGMENT: u8 = 44;
const IPV6_FRAGMENT_HEADER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IpProtocol {
    Icmpv4,
    Tcp,
    Udp,
    Icmpv6,
    Other(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IpInputTarget {
    Drop,
    Punt,
    Options,
    Lookup,
    LookupMulticast,
    IcmpError,
    Reassembly,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, thiserror::Error,
)]
pub enum IpInputError {
    #[error("no IP input error")]
    None,
    #[error("unsupported IP version")]
    Version,
    #[error("IP header is too short")]
    HeaderTooShort,
    #[error("invalid IP options")]
    Options,
    #[error("bad IP checksum")]
    BadChecksum,
    #[error("IP time to live expired")]
    TimeExpired,
    #[error("IP fragment offset one")]
    FragmentOffsetOne,
    #[error("IP packet is too short")]
    TooShort,
    #[error("inconsistent IP packet length")]
    BadLength,
}

impl IpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

impl From<IpInputError> for hammer_runtime::RuntimeError {
    #[inline]
    fn from(error: IpInputError) -> Self {
        Self::subsystem("ip", error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
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

impl Default for IpFragmentKey {
    #[inline]
    fn default() -> Self {
        Self::V4 {
            source: Ipv4Addr::UNSPECIFIED,
            destination: Ipv4Addr::UNSPECIFIED,
            protocol: 0,
            identification: 0,
        }
    }
}

impl BihashKey for IpFragmentKey {
    #[inline(always)]
    fn hash(self) -> u64 {
        match self {
            Self::V4 {
                source,
                destination,
                protocol,
                identification,
            } => {
                let packed = (u128::from(u32::from(source)) << 96)
                    | (u128::from(u32::from(destination)) << 64)
                    | (u128::from(protocol) << 48)
                    | u128::from(identification);
                splitmix64((packed ^ (packed >> 64)) as u64)
            }
            Self::V6 {
                source,
                destination,
                next_header,
                identification,
            } => hash_words(&[
                fold_u128(u128::from(source)),
                fold_u128(u128::from(destination)),
                u64::from(next_header),
                u64::from(identification),
            ]),
        }
    }
}

#[inline(always)]
fn fold_u128(value: u128) -> u64 {
    value as u64 ^ (value >> 64) as u64
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
    pub fn dont_fragment(self) -> bool {
        self.flags_fragment() & IPV4_FLAG_DONT_FRAGMENT != 0
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

pub fn parse_ip_header(packet: &[u8]) -> Result<ParsedIpPacket, IpInputError> {
    let Some(first) = packet.first().copied() else {
        return Err(IpInputError::HeaderTooShort);
    };
    match first >> 4 {
        4 => parse_ipv4_packet_header(packet),
        6 => parse_ipv6_packet_header(packet),
        _ => Err(IpInputError::Version),
    }
}

pub fn parse_ip_fragment(packet: &[u8]) -> Result<ParsedIpFragment, IpInputError> {
    parse_ip_fragment_with_chain_len(packet, 0)
}

pub fn parse_ip_fragment_with_chain_len(
    packet: &[u8],
    tail_len: usize,
) -> Result<ParsedIpFragment, IpInputError> {
    let Some(first) = packet.first().copied() else {
        return Err(IpInputError::HeaderTooShort);
    };
    let chain_len = packet.len().saturating_add(tail_len);
    match first >> 4 {
        4 => parse_ipv4_fragment(packet, chain_len),
        6 => parse_ipv6_fragment(packet, chain_len),
        _ => Err(IpInputError::Version),
    }
}

#[inline(always)]
fn parse_ipv4_packet_header(packet: &[u8]) -> Result<ParsedIpPacket, IpInputError> {
    let header = read_header::<Ipv4Header>(packet, 0)?;
    if header.version() != 4 {
        return Err(IpInputError::Version);
    }
    let ihl = header.header_len();
    if ihl < IPV4_HEADER_MIN_LEN || packet.len() < ihl {
        return Err(IpInputError::HeaderTooShort);
    }

    let total_len = header.total_len();
    if total_len < ihl {
        return Err(IpInputError::BadLength);
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
fn parse_ipv4_fragment(packet: &[u8], chain_len: usize) -> Result<ParsedIpFragment, IpInputError> {
    let header = read_header::<Ipv4Header>(packet, 0)?;
    if header.version() != 4 {
        return Err(IpInputError::Version);
    }
    let ihl = header.header_len();
    if ihl < IPV4_HEADER_MIN_LEN || packet.len() < ihl {
        return Err(IpInputError::HeaderTooShort);
    }

    let total_len = header.total_len();
    if total_len < ihl || total_len > chain_len {
        return Err(IpInputError::BadLength);
    }

    let fragment = header.flags_fragment();
    let payload_len = total_len - ihl;
    let payload_offset = usize::from(fragment & IPV4_FRAGMENT_OFFSET_MASK) * 8;
    let more_fragments = fragment & IPV4_FLAG_MORE_FRAGMENTS != 0;
    if payload_offset == 0 && !more_fragments {
        return Err(IpInputError::BadLength);
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
fn parse_ipv6_packet_header(packet: &[u8]) -> Result<ParsedIpPacket, IpInputError> {
    let header = read_header::<Ipv6Header>(packet, 0)?;
    if header.version() != 6 {
        return Err(IpInputError::Version);
    }
    if packet.len() < IPV6_HEADER_LEN {
        return Err(IpInputError::HeaderTooShort);
    }

    let payload_len = header.payload_len();
    let total_len = IPV6_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(IpInputError::BadLength)?;

    let source = header.source();
    let destination = header.destination();
    let (protocol, input_target, input_error, transport_offset) =
        if header.next_header == IPV6_NEXT_HEADER_FRAGMENT {
            if payload_len < IPV6_FRAGMENT_HEADER_LEN {
                return Err(IpInputError::HeaderTooShort);
            }
            let fragment = read_header::<Ipv6FragmentHeader>(packet, IPV6_HEADER_LEN)?;
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
fn parse_ipv6_fragment(packet: &[u8], chain_len: usize) -> Result<ParsedIpFragment, IpInputError> {
    let header = read_header::<Ipv6Header>(packet, 0)?;
    if header.version() != 6 {
        return Err(IpInputError::Version);
    }
    if packet.len() < IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN {
        return Err(IpInputError::HeaderTooShort);
    }

    let payload_len = header.payload_len();
    let total_len = IPV6_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(IpInputError::BadLength)?;
    if total_len > chain_len {
        return Err(IpInputError::BadLength);
    }
    if payload_len < IPV6_FRAGMENT_HEADER_LEN {
        return Err(IpInputError::HeaderTooShort);
    }
    if header.next_header != IPV6_NEXT_HEADER_FRAGMENT {
        return Err(IpInputError::BadLength);
    }

    let fragment = read_header::<Ipv6FragmentHeader>(packet, IPV6_HEADER_LEN)?;
    let offset_more = fragment.offset_more();
    let payload_len = payload_len - IPV6_FRAGMENT_HEADER_LEN;
    let payload_offset = usize::from(offset_more >> 3) * 8;
    let more_fragments = offset_more & 1 != 0;
    if payload_offset == 0 && !more_fragments {
        return Err(IpInputError::BadLength);
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

/// Read the IPv4 flags/fragment field from a raw header.
#[inline]
pub fn read_ipv4_flags_fragment(header: &[u8]) -> Option<u16> {
    let bytes = header.get(6..8)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

/// Set or clear the IPv4 Don't Fragment flag on a raw header in place.
#[inline]
pub fn apply_ipv4_dont_fragment(output: &mut [u8], enabled: bool) {
    let Ok(ptr) = header_mut_ptr::<Ipv4Header>(output, 0) else {
        return;
    };
    // SAFETY: `header_mut_ptr` checked the range; fields are byte arrays only.
    let header = unsafe { &mut *ptr };
    let mut flags = u16::from_be_bytes(header.flags_fragment);
    if enabled {
        flags |= IPV4_FLAG_DONT_FRAGMENT;
    } else {
        flags &= !IPV4_FLAG_DONT_FRAGMENT;
    }
    header.flags_fragment = flags.to_be_bytes();
}

/// Write a locally originated IPv4 header like VPP `vlib_buffer_push_ip4`.
///
/// Always sets DF (`is_df=1`) and TTL 255. Does not support fragmentation.
/// `output` must hold at least an [`Ipv4Header`]; `total_len` is the full L3
/// packet length including this header.
#[inline]
pub fn write_ipv4_push_header(
    output: &mut [u8],
    src: Ipv4Addr,
    dst: Ipv4Addr,
    protocol: u8,
    total_len: u16,
) -> Result<(), IpInputError> {
    let ptr = header_mut_ptr::<Ipv4Header>(output, 0)?;
    // SAFETY: `header_mut_ptr` checked the range; `Ipv4Header` fields are only
    // byte arrays so mutable field access cannot create unaligned multi-byte refs.
    unsafe {
        ptr.write(Ipv4Header {
            version_ihl: 0x45,
            dscp_ecn: 0,
            total_len: total_len.to_be_bytes(),
            identification: [0; 2],
            flags_fragment: IPV4_FLAG_DONT_FRAGMENT.to_be_bytes(),
            ttl: 255,
            protocol,
            checksum: [0; 2],
            source: src.octets(),
            destination: dst.octets(),
        });
    }
    let checksum = internet_checksum(&output[..IPV4_HEADER_MIN_LEN]);
    // SAFETY: range already validated above.
    unsafe {
        (*ptr).checksum = checksum.to_be_bytes();
    }
    Ok(())
}

/// Write a locally originated IPv6 header like VPP `vlib_buffer_push_ip6`.
#[inline]
pub fn write_ipv6_push_header(
    output: &mut [u8],
    src: Ipv6Addr,
    dst: Ipv6Addr,
    next_header: u8,
    payload_len: u16,
) -> RuntimeResult<()> {
    let ptr = header_mut_ptr::<Ipv6Header>(output, 0)?;
    // SAFETY: same as `write_ipv4_push_header` — packed wire layout, byte fields only.
    let header = unsafe { &mut *ptr };
    *header = Ipv6Header {
        version_traffic_flow: [0x60, 0, 0, 0],
        payload_len: payload_len.to_be_bytes(),
        next_header,
        hop_limit: 255,
        source: src.octets(),
        destination: dst.octets(),
    };
    Ok(())
}

/// Result of VPP-style `ip4_mtu_check` at adjacency rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ipv4MtuAction {
    Ok,
    /// Packet exceeds adj MTU and DF is clear → fragment.
    Fragment {
        mtu: u16,
    },
    /// Packet exceeds adj MTU and DF is set → ICMP Frag-Needed.
    IcmpFragNeeded {
        mtu: u16,
    },
}

/// VPP `ip4_mtu_check`: compare L3 packet length to adjacency `max_l3_packet_bytes`.
#[inline]
pub fn ipv4_mtu_check(
    packet_len: u16,
    adj_packet_bytes: u16,
    dont_fragment: bool,
) -> Ipv4MtuAction {
    if packet_len <= adj_packet_bytes {
        Ipv4MtuAction::Ok
    } else if dont_fragment {
        Ipv4MtuAction::IcmpFragNeeded {
            mtu: adj_packet_bytes,
        }
    } else {
        Ipv4MtuAction::Fragment {
            mtu: adj_packet_bytes,
        }
    }
}
