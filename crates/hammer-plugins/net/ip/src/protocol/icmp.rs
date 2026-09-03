use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU64;

use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};

use super::ip::{IpProtocol, IpVersion, apply_ipv4_dont_fragment, parse_ip_header};
use super::wire::read_header;

#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct IcmpHeader {
    icmp_type: u8,
    code: u8,
    checksum: [u8; 2],
}

impl IcmpHeader {
    #[inline(always)]
    pub const fn icmp_type(self) -> u8 {
        self.icmp_type
    }

    #[inline(always)]
    pub const fn code(self) -> u8 {
        self.code
    }

    #[inline(always)]
    pub fn checksum(self) -> u16 {
        u16::from_be_bytes(self.checksum)
    }
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Ipv4FragmentWord {
    flags_fragment: [u8; 2],
}

impl Ipv4FragmentWord {
    #[inline(always)]
    fn fragment_offset(self) -> u16 {
        u16::from_be_bytes(self.flags_fragment) & 0x1fff
    }
}

const ICMP_ECHO_HEADER_LEN: usize = 8;
const ICMP4_ECHO_REPLY: u8 = 0;
const ICMP4_ECHO_REQUEST: u8 = 8;
const ICMP6_ECHO_REPLY: u8 = 129;
const ICMP6_ECHO_REQUEST: u8 = 128;
const ICMP_PROTOCOL_V4: u8 = 1;
const ICMP_PROTOCOL_V6: u8 = 58;
const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const IPV4_MIN_MTU: usize = 576;
const IPV6_MIN_MTU: usize = 1280;
const LOCAL_ORIGINATED_TTL: u8 = 64;
const IPV4_TTL_OFFSET: usize = 8;
const IPV4_PROTOCOL_OFFSET: usize = 9;
const IPV4_CHECKSUM_OFFSET: usize = 10;
const IPV4_SOURCE_OFFSET: usize = 12;
const IPV4_DESTINATION_OFFSET: usize = 16;
const IPV6_NEXT_HEADER_OFFSET: usize = 6;
const IPV6_HOP_LIMIT_OFFSET: usize = 7;
const IPV6_SOURCE_OFFSET: usize = 8;
const IPV6_DESTINATION_OFFSET: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcmpBuildError {
    BadLength,
    WrongProtocol,
    WrongType,
    BadCode,
    BadChecksum,
    Suppressed,
    UnsupportedFamily,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IcmpErrorFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[repr(transparent)]
pub struct IcmpErrorMetadata(NonZeroU64);

impl IcmpErrorMetadata {
    #[inline]
    fn new(family: IcmpErrorFamily, icmp_type: u8, code: u8, data: u32) -> Self {
        let family = match family {
            IcmpErrorFamily::Ipv4 => 4u64,
            IcmpErrorFamily::Ipv6 => 6u64,
        };
        let packed = (1u64 << 63)
            | (family << 48)
            | ((icmp_type as u64) << 40)
            | ((code as u64) << 32)
            | data as u64;
        Self(NonZeroU64::new(packed).expect("ICMP error metadata is presence-tagged"))
    }

    #[inline]
    pub fn ipv4_time_exceeded() -> Self {
        Self::new(IcmpErrorFamily::Ipv4, 11, 0, 0)
    }

    #[inline]
    pub fn ipv4_destination_unreachable(code: u8, data: u32) -> Self {
        Self::new(IcmpErrorFamily::Ipv4, 3, code, data)
    }

    #[inline]
    pub fn ipv6_time_exceeded() -> Self {
        Self::new(IcmpErrorFamily::Ipv6, 3, 0, 0)
    }

    #[inline]
    pub fn ipv6_packet_too_big(mtu: u32) -> Self {
        Self::new(IcmpErrorFamily::Ipv6, 2, 0, mtu)
    }

    #[inline]
    pub fn ipv6_port_unreachable() -> Self {
        Self::new(IcmpErrorFamily::Ipv6, 1, 4, 0)
    }

    #[inline]
    pub const fn family(self) -> IcmpErrorFamily {
        match (self.0.get() >> 48) & 0xff {
            4 => IcmpErrorFamily::Ipv4,
            6 => IcmpErrorFamily::Ipv6,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub const fn icmp_type(self) -> u8 {
        ((self.0.get() >> 40) & 0xff) as u8
    }

    #[inline]
    pub const fn code(self) -> u8 {
        ((self.0.get() >> 32) & 0xff) as u8
    }

    #[inline]
    pub const fn data(self) -> u32 {
        self.0.get() as u32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcmpGeneratedPacket {
    pub packet: Vec<u8>,
    pub source: IpAddr,
    pub destination: IpAddr,
    pub network_header_len: usize,
    pub transport_header_offset: usize,
}

pub fn build_echo_reply(packet: &[u8]) -> Result<IcmpGeneratedPacket, IcmpBuildError> {
    let parsed = parse_ip_header(packet).map_err(|_| IcmpBuildError::BadLength)?;
    let packet = packet
        .get(..parsed.packet_len)
        .ok_or(IcmpBuildError::BadLength)?;
    let icmp_offset = parsed.transport_header_offset;
    let icmp_len = parsed.packet_len.saturating_sub(icmp_offset);
    if icmp_len < ICMP_ECHO_HEADER_LEN || icmp_offset > parsed.packet_len {
        return Err(IcmpBuildError::BadLength);
    }
    let icmp = &packet[icmp_offset..parsed.packet_len];
    let icmp_header =
        read_header::<IcmpHeader>(packet, icmp_offset).map_err(|_| IcmpBuildError::BadLength)?;
    match (parsed.version, parsed.protocol) {
        (IpVersion::V4, IpProtocol::Icmpv4) => {
            if icmp_header.icmp_type() != ICMP4_ECHO_REQUEST {
                return Err(IcmpBuildError::WrongType);
            }
            if icmp_header.code() != 0 {
                return Err(IcmpBuildError::BadCode);
            }
            if internet_checksum(icmp) != 0 {
                return Err(IcmpBuildError::BadChecksum);
            }
            let mut reply = Vec::from(packet);
            let icmp = &mut reply[icmp_offset..parsed.packet_len];
            icmp[0] = ICMP4_ECHO_REPLY;
            zero_checksum(icmp);
            let checksum = internet_checksum(icmp);
            icmp[2..4].copy_from_slice(&checksum.to_be_bytes());
            swap_ranges::<4>(&mut reply, IPV4_SOURCE_OFFSET, IPV4_DESTINATION_OFFSET);
            reply[IPV4_TTL_OFFSET] = LOCAL_ORIGINATED_TTL;
            apply_ipv4_dont_fragment(&mut reply, true);
            update_ipv4_header_checksum(&mut reply, parsed.network_header_len);
            Ok(IcmpGeneratedPacket {
                packet: reply,
                source: parsed.destination,
                destination: parsed.source,
                network_header_len: parsed.network_header_len,
                transport_header_offset: icmp_offset,
            })
        }
        (IpVersion::V6, IpProtocol::Icmpv6) => {
            if icmp_header.icmp_type() != ICMP6_ECHO_REQUEST {
                return Err(IcmpBuildError::WrongType);
            }
            if icmp_header.code() != 0 {
                return Err(IcmpBuildError::BadCode);
            }
            if icmpv6_checksum(packet, icmp_offset, parsed.packet_len)? != 0 {
                return Err(IcmpBuildError::BadChecksum);
            }
            let mut reply = Vec::from(packet);
            let icmp = &mut reply[icmp_offset..parsed.packet_len];
            icmp[0] = ICMP6_ECHO_REPLY;
            zero_checksum(icmp);
            swap_ranges::<16>(&mut reply, IPV6_SOURCE_OFFSET, IPV6_DESTINATION_OFFSET);
            reply[IPV6_HOP_LIMIT_OFFSET] = LOCAL_ORIGINATED_TTL;
            let checksum = icmpv6_checksum(&reply, icmp_offset, parsed.packet_len)?;
            reply[icmp_offset + 2..icmp_offset + 4].copy_from_slice(&checksum.to_be_bytes());
            Ok(IcmpGeneratedPacket {
                packet: reply,
                source: parsed.destination,
                destination: parsed.source,
                network_header_len: parsed.network_header_len,
                transport_header_offset: icmp_offset,
            })
        }
        _ => Err(IcmpBuildError::WrongProtocol),
    }
}

pub fn build_icmp_error_packet(
    original: &[u8],
    metadata: IcmpErrorMetadata,
    local_source: IpAddr,
) -> Result<IcmpGeneratedPacket, IcmpBuildError> {
    let parsed = parse_ip_header(original).map_err(|_| IcmpBuildError::BadLength)?;
    let original = original
        .get(..parsed.packet_len)
        .ok_or(IcmpBuildError::BadLength)?;
    if should_suppress_icmp_error(original, &parsed) {
        return Err(IcmpBuildError::Suppressed);
    }
    match (
        metadata.family(),
        parsed.version,
        parsed.source,
        local_source,
    ) {
        (IcmpErrorFamily::Ipv4, IpVersion::V4, IpAddr::V4(source), IpAddr::V4(local_source)) => {
            build_ipv4_icmp_error(original, source, local_source, metadata)
        }
        (IcmpErrorFamily::Ipv6, IpVersion::V6, IpAddr::V6(source), IpAddr::V6(local_source)) => {
            build_ipv6_icmp_error(original, source, local_source, metadata)
        }
        _ => Err(IcmpBuildError::UnsupportedFamily),
    }
}

fn build_ipv4_icmp_error(
    original: &[u8],
    source: Ipv4Addr,
    local_source: Ipv4Addr,
    metadata: IcmpErrorMetadata,
) -> Result<IcmpGeneratedPacket, IcmpBuildError> {
    let quote_len = original
        .len()
        .min(IPV4_MIN_MTU - IPV4_HEADER_MIN_LEN - ICMP_ECHO_HEADER_LEN);
    let total_len = IPV4_HEADER_MIN_LEN + ICMP_ECHO_HEADER_LEN + quote_len;
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[IPV4_TTL_OFFSET] = LOCAL_ORIGINATED_TTL;
    packet[IPV4_PROTOCOL_OFFSET] = ICMP_PROTOCOL_V4;
    packet[IPV4_SOURCE_OFFSET..IPV4_SOURCE_OFFSET + 4].copy_from_slice(&local_source.octets());
    packet[IPV4_DESTINATION_OFFSET..IPV4_DESTINATION_OFFSET + 4].copy_from_slice(&source.octets());
    apply_ipv4_dont_fragment(&mut packet, true);
    let icmp_offset = IPV4_HEADER_MIN_LEN;
    packet[icmp_offset] = metadata.icmp_type();
    packet[icmp_offset + 1] = metadata.code();
    packet[icmp_offset + 4..icmp_offset + 8].copy_from_slice(&metadata.data().to_be_bytes());
    packet[icmp_offset + ICMP_ECHO_HEADER_LEN..].copy_from_slice(&original[..quote_len]);
    update_ipv4_header_checksum(&mut packet, IPV4_HEADER_MIN_LEN);
    let checksum = internet_checksum(&packet[icmp_offset..]);
    packet[icmp_offset + 2..icmp_offset + 4].copy_from_slice(&checksum.to_be_bytes());
    Ok(IcmpGeneratedPacket {
        packet,
        source: IpAddr::V4(local_source),
        destination: IpAddr::V4(source),
        network_header_len: IPV4_HEADER_MIN_LEN,
        transport_header_offset: icmp_offset,
    })
}

fn build_ipv6_icmp_error(
    original: &[u8],
    source: Ipv6Addr,
    local_source: Ipv6Addr,
    metadata: IcmpErrorMetadata,
) -> Result<IcmpGeneratedPacket, IcmpBuildError> {
    let quote_len = original
        .len()
        .min(IPV6_MIN_MTU - IPV6_HEADER_LEN - ICMP_ECHO_HEADER_LEN);
    let payload_len = ICMP_ECHO_HEADER_LEN + quote_len;
    let total_len = IPV6_HEADER_LEN + payload_len;
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[IPV6_NEXT_HEADER_OFFSET] = ICMP_PROTOCOL_V6;
    packet[IPV6_HOP_LIMIT_OFFSET] = LOCAL_ORIGINATED_TTL;
    packet[IPV6_SOURCE_OFFSET..IPV6_SOURCE_OFFSET + 16].copy_from_slice(&local_source.octets());
    packet[IPV6_DESTINATION_OFFSET..IPV6_DESTINATION_OFFSET + 16].copy_from_slice(&source.octets());
    let icmp_offset = IPV6_HEADER_LEN;
    packet[icmp_offset] = metadata.icmp_type();
    packet[icmp_offset + 1] = metadata.code();
    packet[icmp_offset + 4..icmp_offset + 8].copy_from_slice(&metadata.data().to_be_bytes());
    packet[icmp_offset + ICMP_ECHO_HEADER_LEN..].copy_from_slice(&original[..quote_len]);
    let checksum = icmpv6_checksum(&packet, icmp_offset, total_len)?;
    packet[icmp_offset + 2..icmp_offset + 4].copy_from_slice(&checksum.to_be_bytes());
    Ok(IcmpGeneratedPacket {
        packet,
        source: IpAddr::V6(local_source),
        destination: IpAddr::V6(source),
        network_header_len: IPV6_HEADER_LEN,
        transport_header_offset: icmp_offset,
    })
}

fn should_suppress_icmp_error(original: &[u8], parsed: &super::ip::ParsedIpPacket) -> bool {
    match (parsed.source, parsed.destination) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => {
            source.is_unspecified()
                || source.is_broadcast()
                || source.is_multicast()
                || destination.is_broadcast()
                || destination.is_multicast()
                || ipv4_fragment_offset(original) > 0
                || icmp_error_type_should_suppress(original, parsed)
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) => {
            source.is_unspecified()
                || source.is_multicast()
                || destination.is_multicast()
                || icmp_error_type_should_suppress(original, parsed)
        }
        _ => true,
    }
}

#[inline(always)]
fn icmp_error_type_should_suppress(original: &[u8], parsed: &super::ip::ParsedIpPacket) -> bool {
    let Ok(header) = read_header::<IcmpHeader>(original, parsed.transport_header_offset) else {
        return true;
    };
    match parsed.protocol {
        IpProtocol::Icmpv4 => matches!(header.icmp_type(), 3 | 4 | 5 | 11 | 12),
        IpProtocol::Icmpv6 => header.icmp_type() < 128,
        _ => false,
    }
}

#[inline(always)]
fn ipv4_fragment_offset(packet: &[u8]) -> u16 {
    read_header::<Ipv4FragmentWord>(packet, 6).map_or(0, Ipv4FragmentWord::fragment_offset)
}

#[inline(always)]
fn zero_checksum(segment: &mut [u8]) {
    segment[2] = 0;
    segment[3] = 0;
}

#[inline(always)]
fn swap_ranges<const N: usize>(packet: &mut [u8], first: usize, second: usize) {
    let mut left = [0u8; N];
    let mut right = [0u8; N];
    left.copy_from_slice(&packet[first..first + N]);
    right.copy_from_slice(&packet[second..second + N]);
    packet[first..first + N].copy_from_slice(&right);
    packet[second..second + N].copy_from_slice(&left);
}

#[inline(always)]
fn update_ipv4_header_checksum(packet: &mut [u8], header_len: usize) {
    packet[IPV4_CHECKSUM_OFFSET] = 0;
    packet[IPV4_CHECKSUM_OFFSET + 1] = 0;
    let checksum = internet_checksum(&packet[..header_len]);
    packet[IPV4_CHECKSUM_OFFSET..IPV4_CHECKSUM_OFFSET + 2].copy_from_slice(&checksum.to_be_bytes());
}

fn icmpv6_checksum(
    packet: &[u8],
    icmp_offset: usize,
    packet_len: usize,
) -> Result<u16, IcmpBuildError> {
    let icmp = packet
        .get(icmp_offset..packet_len)
        .ok_or(IcmpBuildError::BadLength)?;
    let source = packet
        .get(IPV6_SOURCE_OFFSET..IPV6_SOURCE_OFFSET + 16)
        .ok_or(IcmpBuildError::BadLength)?;
    let destination = packet
        .get(IPV6_DESTINATION_OFFSET..IPV6_DESTINATION_OFFSET + 16)
        .ok_or(IcmpBuildError::BadLength)?;
    Ok(internet_checksum_parts(&[
        source,
        destination,
        &(icmp.len() as u32).to_be_bytes(),
        &[0, 0, 0, ICMP_PROTOCOL_V6],
        icmp,
    ]))
}
