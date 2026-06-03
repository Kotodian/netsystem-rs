use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU64;

use super::ip::{IpProtocol, IpVersion, parse_ip_packet_with_chain_len};

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
    UnsupportedFamily,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct IcmpErrorMetadata(NonZeroU64);

impl IcmpErrorMetadata {
    #[inline]
    pub fn new(icmp_type: u8, code: u8, data: u32) -> Self {
        let packed =
            (1u64 << 63) | ((icmp_type as u64) << 40) | ((code as u64) << 32) | data as u64;
        Self(NonZeroU64::new(packed).expect("ICMP error metadata is presence-tagged"))
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
    let parsed =
        parse_ip_packet_with_chain_len(packet, 0).map_err(|_| IcmpBuildError::BadLength)?;
    let packet = packet
        .get(..parsed.packet_len)
        .ok_or(IcmpBuildError::BadLength)?;
    let mut reply = packet.to_vec();
    let icmp_offset = parsed.transport_header_offset;
    let icmp_len = parsed.packet_len.saturating_sub(icmp_offset);
    if icmp_len < ICMP_ECHO_HEADER_LEN || icmp_offset > parsed.packet_len {
        return Err(IcmpBuildError::BadLength);
    }
    let icmp = &mut reply[icmp_offset..parsed.packet_len];
    match (parsed.version, parsed.protocol) {
        (IpVersion::V4, IpProtocol::Icmpv4) => {
            if icmp[0] != ICMP4_ECHO_REQUEST {
                return Err(IcmpBuildError::WrongType);
            }
            if icmp[1] != 0 {
                return Err(IcmpBuildError::BadCode);
            }
            icmp[0] = ICMP4_ECHO_REPLY;
            zero_checksum(icmp);
            let checksum = internet_checksum(icmp);
            icmp[2..4].copy_from_slice(&checksum.to_be_bytes());
            swap_ranges::<4>(&mut reply, IPV4_SOURCE_OFFSET, IPV4_DESTINATION_OFFSET);
            reply[IPV4_TTL_OFFSET] = LOCAL_ORIGINATED_TTL;
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
            if icmp[0] != ICMP6_ECHO_REQUEST {
                return Err(IcmpBuildError::WrongType);
            }
            if icmp[1] != 0 {
                return Err(IcmpBuildError::BadCode);
            }
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
) -> Result<IcmpGeneratedPacket, IcmpBuildError> {
    let parsed =
        parse_ip_packet_with_chain_len(original, 0).map_err(|_| IcmpBuildError::BadLength)?;
    let original = original
        .get(..parsed.packet_len)
        .ok_or(IcmpBuildError::BadLength)?;
    match (parsed.version, parsed.source, parsed.destination) {
        (IpVersion::V4, IpAddr::V4(source), IpAddr::V4(destination)) => {
            build_ipv4_icmp_error(original, source, destination, metadata)
        }
        (IpVersion::V6, IpAddr::V6(source), IpAddr::V6(destination)) => {
            build_ipv6_icmp_error(original, source, destination, metadata)
        }
        _ => Err(IcmpBuildError::UnsupportedFamily),
    }
}

fn build_ipv4_icmp_error(
    original: &[u8],
    source: Ipv4Addr,
    destination: Ipv4Addr,
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
    packet[IPV4_SOURCE_OFFSET..IPV4_SOURCE_OFFSET + 4].copy_from_slice(&destination.octets());
    packet[IPV4_DESTINATION_OFFSET..IPV4_DESTINATION_OFFSET + 4].copy_from_slice(&source.octets());
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
        source: IpAddr::V4(destination),
        destination: IpAddr::V4(source),
        network_header_len: IPV4_HEADER_MIN_LEN,
        transport_header_offset: icmp_offset,
    })
}

fn build_ipv6_icmp_error(
    original: &[u8],
    source: Ipv6Addr,
    destination: Ipv6Addr,
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
    packet[IPV6_SOURCE_OFFSET..IPV6_SOURCE_OFFSET + 16].copy_from_slice(&destination.octets());
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
        source: IpAddr::V6(destination),
        destination: IpAddr::V6(source),
        network_header_len: IPV6_HEADER_LEN,
        transport_header_offset: icmp_offset,
    })
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
    let mut pseudo = Vec::with_capacity(40 + icmp.len());
    pseudo.extend_from_slice(source);
    pseudo.extend_from_slice(destination);
    pseudo.extend_from_slice(&(icmp.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, ICMP_PROTOCOL_V6]);
    pseudo.extend_from_slice(icmp);
    Ok(internet_checksum(&pseudo))
}

#[inline(always)]
fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::{IcmpErrorMetadata, build_echo_reply, build_icmp_error_packet};

    #[test]
    fn build_echo_reply_rewrites_ipv4_echo_request() {
        let request = ipv4_icmp_echo_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 0, 2, 1),
            b"echo4",
        );

        let reply = build_echo_reply(&request).expect("echo reply");

        assert_eq!(&reply.packet[12..16], &[192, 0, 2, 1]);
        assert_eq!(&reply.packet[16..20], &[10, 0, 0, 1]);
        assert_eq!(reply.packet[20], 0);
        assert_eq!(&reply.packet[24..], &request[24..]);
        assert_eq!(internet_checksum(&reply.packet[..20]), 0);
        assert_eq!(internet_checksum(&reply.packet[20..]), 0);
    }

    #[test]
    fn build_icmp_error_packet_synthesizes_ipv6_error() {
        let source = "2001:db8::1".parse().expect("source");
        let destination = "2001:db8::2".parse().expect("destination");
        let original = ipv6_packet(source, destination, 17, b"expired");

        let error =
            build_icmp_error_packet(&original, IcmpErrorMetadata::new(3, 0, 0)).expect("error");

        assert_eq!(&error.packet[8..24], &destination.octets());
        assert_eq!(&error.packet[24..40], &source.octets());
        assert_eq!(error.packet[40], 3);
        assert_eq!(error.packet[41], 0);
        assert_eq!(&error.packet[48..], &original[..]);
        assert_eq!(
            ipv6_l4_checksum(destination, source, 58, &error.packet[40..]),
            0
        );
    }

    fn ipv4_icmp_echo_packet(source: Ipv4Addr, destination: Ipv4Addr, payload: &[u8]) -> Vec<u8> {
        let mut packet = ipv4_packet(source, destination, 1, 8 + payload.len());
        let icmp = 20;
        packet[icmp] = 8;
        packet[icmp + 4..icmp + 6].copy_from_slice(&0x1234u16.to_be_bytes());
        packet[icmp + 6..icmp + 8].copy_from_slice(&1u16.to_be_bytes());
        packet[icmp + 8..].copy_from_slice(payload);
        let checksum = internet_checksum(&packet[icmp..]);
        packet[icmp + 2..icmp + 4].copy_from_slice(&checksum.to_be_bytes());
        update_ipv4_header_checksum(&mut packet);
        packet
    }

    fn ipv4_packet(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        payload_len: usize,
    ) -> Vec<u8> {
        let total_len = 20 + payload_len;
        let mut packet = vec![0u8; total_len];
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
        packet[8] = 64;
        packet[9] = protocol;
        packet[12..16].copy_from_slice(&source.octets());
        packet[16..20].copy_from_slice(&destination.octets());
        packet
    }

    fn ipv6_packet(
        source: Ipv6Addr,
        destination: Ipv6Addr,
        protocol: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut packet = vec![0u8; 40 + payload.len()];
        packet[0] = 0x60;
        packet[4..6].copy_from_slice(&(payload.len() as u16).to_be_bytes());
        packet[6] = protocol;
        packet[7] = 64;
        packet[8..24].copy_from_slice(&source.octets());
        packet[24..40].copy_from_slice(&destination.octets());
        packet[40..].copy_from_slice(payload);
        packet
    }

    fn update_ipv4_header_checksum(packet: &mut [u8]) {
        packet[10] = 0;
        packet[11] = 0;
        let checksum = internet_checksum(&packet[..20]);
        packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    }

    fn ipv6_l4_checksum(
        source: Ipv6Addr,
        destination: Ipv6Addr,
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        let mut pseudo = Vec::new();
        pseudo.extend_from_slice(&source.octets());
        pseudo.extend_from_slice(&destination.octets());
        pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
        pseudo.extend_from_slice(&[0, 0, 0, protocol]);
        pseudo.extend_from_slice(segment);
        internet_checksum(&pseudo)
    }

    fn internet_checksum(bytes: &[u8]) -> u16 {
        let mut sum = 0u32;
        for chunk in bytes.chunks(2) {
            let word = if chunk.len() == 2 {
                u16::from_be_bytes([chunk[0], chunk[1]]) as u32
            } else {
                (chunk[0] as u32) << 8
            };
            sum += word;
            while sum > 0xffff {
                sum = (sum & 0xffff) + (sum >> 16);
            }
        }
        !(sum as u16)
    }
}
