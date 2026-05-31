use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::error::{CoreError, CoreResult};
use nom::IResult;
use nom::Parser;
use nom::bytes::complete::take;
use nom::number::complete::{be_u8, be_u16, be_u32};

const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const IPV4_FLAG_MORE_FRAGMENTS: u16 = 0x2000;
const IPV4_FRAGMENT_OFFSET_MASK: u16 = 0x1fff;
const IPV6_NEXT_HEADER_FRAGMENT: u8 = 44;
const IPV6_FRAGMENT_HEADER_LEN: usize = 8;

type ParseResult<'a, T> = IResult<&'a [u8], T>;

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
struct ParsedIpPacketHeader {
    parsed: ParsedIpPacket,
    required_len: usize,
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

#[inline(always)]
pub fn parse_ip_packet(packet: &[u8]) -> CoreResult<ParsedIpPacket> {
    parse_ip_packet_with_chain_len(packet, 0)
}

#[inline(always)]
pub fn parse_ip_packet_with_chain_len(
    packet: &[u8],
    tail_len: usize,
) -> CoreResult<ParsedIpPacket> {
    let first = *packet
        .first()
        .ok_or_else(|| CoreError::internal("empty IP packet"))?;
    let parsed = match first >> 4 {
        4 => parse_ipv4_packet_header(packet),
        6 => parse_ipv6_packet_header(packet),
        other => {
            return Err(CoreError::internal(format!(
                "unsupported IP version: {other}"
            )));
        }
    };
    let (_, parsed) =
        parsed.map_err(|err| CoreError::internal(format!("invalid IP packet: {err}")))?;
    let chain_len = packet.len().saturating_add(tail_len);
    if chain_len < parsed.required_len {
        return Err(CoreError::internal("invalid IP packet length"));
    }
    Ok(parsed.parsed)
}

#[inline(always)]
pub fn parse_ip_fragment(packet: &[u8]) -> CoreResult<ParsedIpFragment> {
    parse_ip_fragment_with_chain_len(packet, 0)
}

#[inline(always)]
pub fn parse_ip_fragment_with_chain_len(
    packet: &[u8],
    tail_len: usize,
) -> CoreResult<ParsedIpFragment> {
    let first = *packet
        .first()
        .ok_or_else(|| CoreError::internal("empty IP packet"))?;
    let chain_len = packet.len().saturating_add(tail_len);
    let parsed = match first >> 4 {
        4 => parse_ipv4_fragment(packet, chain_len),
        6 => parse_ipv6_fragment(packet, chain_len),
        other => {
            return Err(CoreError::internal(format!(
                "unsupported IP version: {other}"
            )));
        }
    };
    let (_, parsed) =
        parsed.map_err(|err| CoreError::internal(format!("invalid IP fragment: {err}")))?;
    Ok(parsed)
}

#[inline(always)]
fn parse_ipv4_packet_header(input: &[u8]) -> ParseResult<'_, ParsedIpPacketHeader> {
    let packet = input;
    let (input, version_ihl) = be_u8(input)?;
    if version_ihl >> 4 != 4 {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let ihl = ((version_ihl & 0x0f) as usize) * 4;
    if ihl < IPV4_HEADER_MIN_LEN {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    let (input, _) = be_u8(input)?;
    let (input, total_len) = be_u16(input)?;
    let total_len = total_len as usize;
    if total_len < ihl {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    let (input, _) = be_u16(input)?;
    let (input, fragment) = be_u16(input)?;
    let (input, ttl) = be_u8(input)?;
    let (input, protocol) = be_u8(input)?;
    let (input, _) = be_u16(input)?;
    let (input, source) = parse_ipv4_addr(input)?;
    let (input, destination) = parse_ipv4_addr(input)?;
    let options_len = ihl - IPV4_HEADER_MIN_LEN;
    let (input, _) = take(options_len).parse(input)?;
    let fragment_offset = fragment & IPV4_FRAGMENT_OFFSET_MASK;
    let checksum_bad = internet_checksum(&packet[..ihl]) != 0;
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
        } else if ttl < 1 {
            (IpInputTarget::IcmpError, IpInputError::TimeExpired)
        } else if options_len != 0 {
            (IpInputTarget::Options, IpInputError::Options)
        } else if fragment & (IPV4_FLAG_MORE_FRAGMENTS | IPV4_FRAGMENT_OFFSET_MASK) != 0 {
            (IpInputTarget::Reassembly, IpInputError::None)
        } else if destination.is_multicast() {
            (IpInputTarget::LookupMulticast, IpInputError::None)
        } else {
            (IpInputTarget::Lookup, IpInputError::None)
        };

    Ok((
        input,
        ParsedIpPacketHeader {
            parsed: ParsedIpPacket {
                version: IpVersion::V4,
                protocol: IpProtocol::from(protocol),
                input_target,
                input_error,
                source: IpAddr::V4(source),
                destination: IpAddr::V4(destination),
                packet_len: total_len,
                network_header_offset: 0,
                network_header_len: ihl,
                transport_header_offset: ihl,
                transport_header_len: 0,
            },
            required_len: total_len,
        },
    ))
}

#[inline(always)]
fn parse_ipv4_fragment(input: &[u8], packet_len: usize) -> ParseResult<'_, ParsedIpFragment> {
    let (input, version_ihl) = be_u8(input)?;
    if version_ihl >> 4 != 4 {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let ihl = ((version_ihl & 0x0f) as usize) * 4;
    if ihl < IPV4_HEADER_MIN_LEN {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, _) = be_u8(input)?;
    let (input, total_len) = be_u16(input)?;
    let total_len = total_len as usize;
    if total_len < ihl || total_len > packet_len {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, identification) = be_u16(input)?;
    let (input, fragment) = be_u16(input)?;
    let (input, _) = be_u8(input)?;
    let (input, protocol) = be_u8(input)?;
    let (input, _) = be_u16(input)?;
    let (input, source) = parse_ipv4_addr(input)?;
    let (input, destination) = parse_ipv4_addr(input)?;
    let options_len = ihl - IPV4_HEADER_MIN_LEN;
    let (input, _) = take(options_len).parse(input)?;
    let payload_len = total_len - ihl;
    let payload_offset = ((fragment & IPV4_FRAGMENT_OFFSET_MASK) as usize) * 8;
    let more_fragments = fragment & IPV4_FLAG_MORE_FRAGMENTS != 0;
    if payload_offset == 0 && !more_fragments {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok((
        &[],
        ParsedIpFragment {
            version: IpVersion::V4,
            key: IpFragmentKey::V4 {
                source,
                destination,
                protocol,
                identification,
            },
            payload_offset,
            payload_len,
            more_fragments,
            header_len: ihl,
        },
    ))
}

#[inline(always)]
fn parse_ipv6_packet_header(input: &[u8]) -> ParseResult<'_, ParsedIpPacketHeader> {
    let (input, version_traffic) = be_u8(input)?;
    if version_traffic >> 4 != 6 {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, _) = take(3usize).parse(input)?;
    let (input, payload_len) = be_u16(input)?;
    let payload_len = payload_len as usize;
    let total_len = IPV6_HEADER_LEN.checked_add(payload_len).ok_or_else(|| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    let (input, next_header) = be_u8(input)?;
    let (input, hop_limit) = be_u8(input)?;
    let (input, source) = parse_ipv6_addr(input)?;
    let (input, destination) = parse_ipv6_addr(input)?;
    let (protocol, input_target, input_error, transport_offset) =
        if next_header == IPV6_NEXT_HEADER_FRAGMENT {
            if payload_len < IPV6_FRAGMENT_HEADER_LEN {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
            let (_, fragment_next_header) = be_u8(input)?;
            (
                fragment_next_header,
                if hop_limit < 1 {
                    IpInputTarget::IcmpError
                } else {
                    IpInputTarget::Reassembly
                },
                if hop_limit < 1 {
                    IpInputError::TimeExpired
                } else {
                    IpInputError::None
                },
                IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN,
            )
        } else if hop_limit < 1 {
            (
                next_header,
                IpInputTarget::IcmpError,
                IpInputError::TimeExpired,
                IPV6_HEADER_LEN,
            )
        } else if destination.is_multicast() {
            (
                next_header,
                IpInputTarget::LookupMulticast,
                IpInputError::None,
                IPV6_HEADER_LEN,
            )
        } else {
            (
                next_header,
                IpInputTarget::Lookup,
                IpInputError::None,
                IPV6_HEADER_LEN,
            )
        };

    Ok((
        input,
        ParsedIpPacketHeader {
            parsed: ParsedIpPacket {
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
            },
            required_len: total_len,
        },
    ))
}

#[inline(always)]
fn parse_ipv6_fragment(input: &[u8], packet_len: usize) -> ParseResult<'_, ParsedIpFragment> {
    let (input, version_traffic) = be_u8(input)?;
    if version_traffic >> 4 != 6 {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, _) = take(3usize).parse(input)?;
    let (input, payload_len) = be_u16(input)?;
    let payload_len = payload_len as usize;
    let total_len = IPV6_HEADER_LEN.checked_add(payload_len).ok_or_else(|| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    if total_len > packet_len {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    if payload_len < IPV6_FRAGMENT_HEADER_LEN {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, next_header) = be_u8(input)?;
    let (input, _) = be_u8(input)?;
    let (input, source) = parse_ipv6_addr(input)?;
    let (input, destination) = parse_ipv6_addr(input)?;
    if next_header != IPV6_NEXT_HEADER_FRAGMENT {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, fragment_next_header) = be_u8(input)?;
    let (input, _) = be_u8(input)?;
    let (input, offset_more) = be_u16(input)?;
    let (input, identification) = be_u32(input)?;
    let payload_len = payload_len - IPV6_FRAGMENT_HEADER_LEN;
    let payload_offset = ((offset_more >> 3) as usize) * 8;
    let more_fragments = offset_more & 1 != 0;
    if payload_offset == 0 && !more_fragments {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok((
        &[],
        ParsedIpFragment {
            version: IpVersion::V6,
            key: IpFragmentKey::V6 {
                source,
                destination,
                next_header: fragment_next_header,
                identification,
            },
            payload_offset,
            payload_len,
            more_fragments,
            header_len: IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN,
        },
    ))
}

#[inline(always)]
fn parse_ipv4_addr(input: &[u8]) -> ParseResult<'_, Ipv4Addr> {
    let (input, bytes) = take(4usize).parse(input)?;
    let bytes = <[u8; 4]>::try_from(bytes).expect("nom take returned four bytes");
    Ok((input, Ipv4Addr::from(bytes)))
}

#[inline(always)]
fn parse_ipv6_addr(input: &[u8]) -> ParseResult<'_, Ipv6Addr> {
    let (input, bytes) = take(16usize).parse(input)?;
    let bytes = <[u8; 16]>::try_from(bytes).expect("nom take returned sixteen bytes");
    Ok((input, Ipv6Addr::from(bytes)))
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
