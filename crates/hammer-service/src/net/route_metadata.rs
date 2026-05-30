use hammer_adapter::{Network, RouteMetadata, SocksAddr};
use hammer_core::error::{CoreError, CoreResult};
use nom::IResult;
use nom::Parser;
use nom::bytes::complete::take;
use nom::number::complete::{be_u8, be_u16};

use crate::net::ip::{IpProtocol, parse_ip_packet};

const TCP_HEADER_MIN_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;

type ParseResult<'a, T> = IResult<&'a [u8], T>;

struct ParsedPacket {
    network: Network,
    source: SocksAddr,
    destination: SocksAddr,
}

#[inline]
pub fn packet_route_metadata(interface_id: &str, packet: &[u8]) -> CoreResult<RouteMetadata> {
    let parsed = parse_packet(packet)?;
    Ok(RouteMetadata {
        inbound: interface_id.to_owned(),
        network: parsed.network,
        source: Some(parsed.source),
        destination: Some(parsed.destination),
        ..Default::default()
    })
}

#[inline]
fn parse_packet(packet: &[u8]) -> CoreResult<ParsedPacket> {
    let parsed = parse_ip_packet(packet)?;
    let transport = packet
        .get(parsed.cursor.transport_header_offset()..parsed.cursor.packet_len())
        .ok_or_else(|| {
            hammer_core::error::CoreError::internal("invalid transport packet cursor")
        })?;
    match parsed.protocol {
        IpProtocol::Tcp => {
            let header = parse_tcp_header(transport)?;
            Ok(ParsedPacket {
                network: Network::Tcp,
                source: SocksAddr::ip(parsed.source, header.source_port),
                destination: SocksAddr::ip(parsed.destination, header.destination_port),
            })
        }
        IpProtocol::Udp => {
            let header = parse_udp_header(transport)?;
            Ok(ParsedPacket {
                network: Network::Udp,
                source: SocksAddr::ip(parsed.source, header.source_port),
                destination: SocksAddr::ip(parsed.destination, header.destination_port),
            })
        }
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => Ok(ParsedPacket {
            network: Network::Icmp,
            source: SocksAddr::ip(parsed.source, 0),
            destination: SocksAddr::ip(parsed.destination, 0),
        }),
    }
}

#[derive(Debug, Clone, Copy)]
struct TransportHeader {
    source_port: u16,
    destination_port: u16,
}

#[inline]
fn parse_tcp_header(input: &[u8]) -> CoreResult<TransportHeader> {
    let (_, header) = parse_tcp_header_nom(input)
        .map_err(|err| CoreError::internal(format!("invalid TCP segment: {err}")))?;
    Ok(header)
}

#[inline]
fn parse_tcp_header_nom(input: &[u8]) -> ParseResult<'_, TransportHeader> {
    let segment_len = input.len();
    let (input, source_port) = be_u16(input)?;
    let (input, destination_port) = be_u16(input)?;
    let (input, _) = take(8usize).parse(input)?;
    let (input, data_offset_and_flags) = be_u8(input)?;
    let data_offset = ((data_offset_and_flags >> 4) as usize) * 4;
    if data_offset < TCP_HEADER_MIN_LEN || segment_len < data_offset {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, _) = take(data_offset - 13).parse(input)?;
    Ok((
        input,
        TransportHeader {
            source_port,
            destination_port,
        },
    ))
}

#[inline]
fn parse_udp_header(input: &[u8]) -> CoreResult<TransportHeader> {
    let (_, header) = parse_udp_header_nom(input)
        .map_err(|err| CoreError::internal(format!("invalid UDP datagram: {err}")))?;
    Ok(header)
}

#[inline]
fn parse_udp_header_nom(input: &[u8]) -> ParseResult<'_, TransportHeader> {
    let datagram_len = input.len();
    let (input, source_port) = be_u16(input)?;
    let (input, destination_port) = be_u16(input)?;
    let (input, length) = be_u16(input)?;
    let length = length as usize;
    if length < UDP_HEADER_LEN || datagram_len < length {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (input, _) = be_u16(input)?;
    let (input, _) = take(length - UDP_HEADER_LEN).parse(input)?;
    Ok((
        input,
        TransportHeader {
            source_port,
            destination_port,
        },
    ))
}
