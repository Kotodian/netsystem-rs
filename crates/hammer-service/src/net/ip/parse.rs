use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use hammer_adapter::BufferPacketCursor;
use hammer_core::error::{CoreError, CoreResult};
use nom::IResult;
use nom::Parser;
use nom::bytes::complete::take;
use nom::number::complete::{be_u8, be_u16};

const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const IPV4_FLAG_MORE_FRAGMENTS: u16 = 0x2000;
const IPV4_FRAGMENT_OFFSET_MASK: u16 = 0x1fff;

type ParseResult<'a, T> = IResult<&'a [u8], T>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpProtocol {
    Icmpv4,
    Tcp,
    Udp,
    Icmpv6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpInputTarget {
    Lookup,
    Options,
    Reassembly,
}

impl TryFrom<u8> for IpProtocol {
    type Error = CoreError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Icmpv4),
            6 => Ok(Self::Tcp),
            17 => Ok(Self::Udp),
            58 => Ok(Self::Icmpv6),
            other => Err(CoreError::internal(format!(
                "unsupported transport protocol: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParsedIpPacket {
    pub protocol: IpProtocol,
    pub input_target: IpInputTarget,
    pub source: IpAddr,
    pub destination: IpAddr,
    pub cursor: BufferPacketCursor,
}

#[inline]
pub fn parse_ip_packet(packet: &[u8]) -> CoreResult<ParsedIpPacket> {
    let first = *packet
        .first()
        .ok_or_else(|| CoreError::internal("empty IP packet"))?;
    let parsed = match first >> 4 {
        4 => parse_ipv4_packet(packet),
        6 => parse_ipv6_packet(packet),
        other => {
            return Err(CoreError::internal(format!(
                "unsupported IP version: {other}"
            )));
        }
    };
    let (_, parsed) =
        parsed.map_err(|err| CoreError::internal(format!("invalid IP packet: {err}")))?;
    Ok(parsed)
}

#[inline]
fn parse_ipv4_packet(input: &[u8]) -> ParseResult<'_, ParsedIpPacket> {
    let packet_len = input.len();
    let (input, version_ihl) = be_u8(input)?;
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

    let (input, fragment) = be_u16(input)?;
    let (input, _) = be_u16(input)?;
    let (input, _) = be_u8(input)?;
    let (input, protocol) = be_u8(input)?;
    let (input, _) = be_u16(input)?;
    let (input, source) = parse_ipv4_addr(input)?;
    let (input, destination) = parse_ipv4_addr(input)?;
    let options_len = ihl - IPV4_HEADER_MIN_LEN;
    let (input, _) = take(options_len).parse(input)?;
    let transport_len = total_len - ihl;
    let (input, _) = take(transport_len).parse(input)?;
    let input_target = if options_len != 0 {
        IpInputTarget::Options
    } else if fragment & (IPV4_FLAG_MORE_FRAGMENTS | IPV4_FRAGMENT_OFFSET_MASK) != 0 {
        IpInputTarget::Reassembly
    } else {
        IpInputTarget::Lookup
    };

    Ok((
        input,
        ParsedIpPacket {
            protocol: IpProtocol::try_from(protocol).map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?,
            input_target,
            source: IpAddr::V4(source),
            destination: IpAddr::V4(destination),
            cursor: BufferPacketCursor::new()
                .with_packet_len(total_len)
                .with_network_header(0, ihl)
                .with_transport_header(ihl, 0),
        },
    ))
}

#[inline]
fn parse_ipv6_packet(input: &[u8]) -> ParseResult<'_, ParsedIpPacket> {
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
    let (input, _) = be_u8(input)?;
    let (input, source) = parse_ipv6_addr(input)?;
    let (input, destination) = parse_ipv6_addr(input)?;
    let (input, _) = take(payload_len).parse(input)?;

    Ok((
        input,
        ParsedIpPacket {
            protocol: IpProtocol::try_from(next_header).map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?,
            input_target: if next_header == 44 {
                IpInputTarget::Reassembly
            } else {
                IpInputTarget::Lookup
            },
            source: IpAddr::V6(source),
            destination: IpAddr::V6(destination),
            cursor: BufferPacketCursor::new()
                .with_packet_len(total_len)
                .with_network_header(0, IPV6_HEADER_LEN)
                .with_transport_header(IPV6_HEADER_LEN, 0),
        },
    ))
}

#[inline]
fn parse_ipv4_addr(input: &[u8]) -> ParseResult<'_, Ipv4Addr> {
    let (input, bytes) = take(4usize).parse(input)?;
    let bytes = <[u8; 4]>::try_from(bytes).expect("nom take returned four bytes");
    Ok((input, Ipv4Addr::from(bytes)))
}

#[inline]
fn parse_ipv6_addr(input: &[u8]) -> ParseResult<'_, Ipv6Addr> {
    let (input, bytes) = take(16usize).parse(input)?;
    let bytes = <[u8; 16]>::try_from(bytes).expect("nom take returned sixteen bytes");
    Ok((input, Ipv6Addr::from(bytes)))
}
