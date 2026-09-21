use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_infra::bihash::{BihashKey, hash_words, splitmix64};
use hammer_infra::checksum::internet_checksum;
use zerocopy::{FromBytes, IntoBytes};

pub(crate) const IPV4_HEADER_MIN_LEN: usize = 20;
pub(crate) const IPV6_HEADER_LEN: usize = 40;
pub(crate) const IPV4_FLAG_MORE_FRAGMENTS: u16 = 0x2000;
/// IPv4 Don't Fragment flag (RFC 791).
pub const IPV4_FLAG_DONT_FRAGMENT: u16 = 0x4000;
pub(crate) const IPV4_FRAGMENT_OFFSET_MASK: u16 = 0x1fff;
pub(crate) const IPV6_NEXT_HEADER_HOP_BY_HOP: u8 = 0;
pub(crate) const IPV6_NEXT_HEADER_ROUTING: u8 = 43;
pub(crate) const IPV6_NEXT_HEADER_FRAGMENT: u8 = 44;
pub(crate) const IPV6_NEXT_HEADER_AH: u8 = 51;
pub(crate) const IPV6_NEXT_HEADER_DESTINATION: u8 = 60;
pub(crate) const IPV6_FRAGMENT_HEADER_LEN: usize = 8;

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

#[hammer_component_macros::runtime_error(subsystem = "ip")]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, thiserror::Error,
)]
#[repr(u16)]
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

impl hammer_runtime::node::NodeErrorCode for IpInputError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl IpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
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

impl From<IpProtocol> for u8 {
    #[inline(always)]
    fn from(protocol: IpProtocol) -> Self {
        match protocol {
            IpProtocol::Icmpv4 => 1,
            IpProtocol::Tcp => 6,
            IpProtocol::Udp => 17,
            IpProtocol::Icmpv6 => 58,
            IpProtocol::Other(value) => value,
        }
    }
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

#[derive(zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
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
    pub fn version(&self) -> u8 {
        self.version_ihl >> 4
    }

    #[inline(always)]
    pub fn header_len(&self) -> usize {
        usize::from(self.version_ihl & 0x0f) * 4
    }

    #[inline(always)]
    pub fn total_len(&self) -> usize {
        usize::from(u16::from_be_bytes(self.total_len))
    }

    #[inline(always)]
    pub fn identification(&self) -> u16 {
        u16::from_be_bytes(self.identification)
    }

    #[inline(always)]
    pub fn flags_fragment(&self) -> u16 {
        u16::from_be_bytes(self.flags_fragment)
    }

    #[inline(always)]
    pub fn dont_fragment(&self) -> bool {
        self.flags_fragment() & IPV4_FLAG_DONT_FRAGMENT != 0
    }

    #[inline(always)]
    pub fn protocol(&self) -> u8 {
        self.protocol
    }

    #[inline(always)]
    pub fn ttl(&self) -> u8 {
        self.ttl
    }

    #[inline(always)]
    pub fn set_ttl(&mut self, ttl: u8) {
        self.ttl = ttl;
    }

    #[inline(always)]
    pub fn source(&self) -> Ipv4Addr {
        Ipv4Addr::from(self.source)
    }

    #[inline(always)]
    pub fn destination(&self) -> Ipv4Addr {
        Ipv4Addr::from(self.destination)
    }

    #[inline(always)]
    pub fn set_total_len(&mut self, total_len: u16) {
        self.total_len = total_len.to_be_bytes();
    }

    #[inline(always)]
    pub fn set_flags_fragment(&mut self, flags_fragment: u16) {
        self.flags_fragment = flags_fragment.to_be_bytes();
    }

    #[inline(always)]
    pub fn set_checksum(&mut self, checksum: u16) {
        self.checksum = checksum.to_be_bytes();
    }
}

#[derive(zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
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
    pub fn version(&self) -> u8 {
        self.version_traffic_flow[0] >> 4
    }

    #[inline(always)]
    pub fn payload_len(&self) -> usize {
        usize::from(u16::from_be_bytes(self.payload_len))
    }

    #[inline(always)]
    pub fn next_protocol(&self) -> u8 {
        self.next_header
    }

    #[inline(always)]
    pub fn hop_limit(&self) -> u8 {
        self.hop_limit
    }

    #[inline(always)]
    pub fn set_hop_limit(&mut self, hop_limit: u8) {
        self.hop_limit = hop_limit;
    }

    #[inline(always)]
    pub fn flow_label(&self) -> u32 {
        u32::from_be_bytes(self.version_traffic_flow) & 0x000f_ffff
    }

    #[inline(always)]
    pub fn source(&self) -> Ipv6Addr {
        Ipv6Addr::from(self.source)
    }

    #[inline(always)]
    pub fn destination(&self) -> Ipv6Addr {
        Ipv6Addr::from(self.destination)
    }

    #[inline(always)]
    pub fn set_payload_len(&mut self, payload_len: u16) {
        self.payload_len = payload_len.to_be_bytes();
    }

    #[inline(always)]
    pub fn set_next_protocol(&mut self, protocol: u8) {
        self.next_header = protocol;
    }
}

#[derive(zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
pub struct Ipv6FragmentHeader {
    next_header: u8,
    reserved: u8,
    offset_more: [u8; 2],
    identification: [u8; 4],
}

impl Ipv6FragmentHeader {
    #[inline(always)]
    pub fn next_protocol(&self) -> u8 {
        self.next_header
    }

    #[inline(always)]
    pub fn offset_more(&self) -> u16 {
        u16::from_be_bytes(self.offset_more)
    }

    #[inline(always)]
    pub fn identification(&self) -> u32 {
        u32::from_be_bytes(self.identification)
    }
}

/// Read the IPv4 flags/fragment field from a raw header.
#[inline]
pub fn read_ipv4_flags_fragment(header: &[u8]) -> Option<u16> {
    let (header, _) = Ipv4Header::ref_from_prefix(header).ok()?;
    Some(header.flags_fragment())
}

/// Set or clear the IPv4 Don't Fragment flag on a raw header in place.
#[inline]
pub fn apply_ipv4_dont_fragment(output: &mut [u8], enabled: bool) {
    let Ok((header, _)) = Ipv4Header::mut_from_prefix(output) else {
        return;
    };
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
/// Sets TTL 255; the caller selects DF, including clear DF for ICMP errors.
/// `output` must hold at least an [`Ipv4Header`]; `total_len` is the full L3
/// packet length including this header.
#[inline]
pub fn write_ipv4_push_header(
    output: &mut [u8],
    src: Ipv4Addr,
    dst: Ipv4Addr,
    protocol: u8,
    total_len: u16,
    dont_fragment: bool,
) -> Result<(), IpInputError> {
    let (header, _) =
        Ipv4Header::mut_from_prefix(output).map_err(|_| IpInputError::HeaderTooShort)?;
    header.version_ihl = 0x45;
    header.dscp_ecn = 0;
    header.total_len = total_len.to_be_bytes();
    header.identification = [0; 2];
    header.flags_fragment = if dont_fragment {
        IPV4_FLAG_DONT_FRAGMENT.to_be_bytes()
    } else {
        [0; 2]
    };
    header.ttl = 255;
    header.protocol = protocol;
    header.checksum = [0; 2];
    header.source = src.octets();
    header.destination = dst.octets();
    let checksum = internet_checksum(header.as_bytes());
    header.checksum = checksum.to_be_bytes();
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
) -> Result<(), IpInputError> {
    let (header, _) =
        Ipv6Header::mut_from_prefix(output).map_err(|_| IpInputError::HeaderTooShort)?;
    header.version_traffic_flow = [0x60, 0, 0, 0];
    header.payload_len = payload_len.to_be_bytes();
    header.next_header = next_header;
    header.hop_limit = 255;
    header.source = src.octets();
    header.destination = dst.octets();
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
