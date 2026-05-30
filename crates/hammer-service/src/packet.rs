use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use hammer_adapter::{
    BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, InternalNode, Network, Node,
    NodeId, NodeResult, RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use nom::IResult;
use nom::Parser;
use nom::bytes::complete::take;
use nom::number::complete::{be_u8, be_u16};

const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const TCP_HEADER_MIN_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;

type ParseResult<'a, T> = IResult<&'a [u8], T>;

pub struct IpInputNode {
    udp_next: NodeId,
}

impl IpInputNode {
    pub fn new(udp_next: NodeId) -> Self {
        Self { udp_next }
    }
}

impl<G> Node<G> for IpInputNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        process_frame(runtime, frame, process_ip_input_index)?;
        if frame.has_pending() {
            Ok(NodeResult::next_current(self.udp_next))
        } else {
            Ok(NodeResult::drop())
        }
    }
}

impl<G> InternalNode<G> for IpInputNode {}

pub struct UdpInputNode {
    next: NodeId,
}

impl UdpInputNode {
    pub fn new(next: NodeId) -> Self {
        Self { next }
    }
}

impl<G> Node<G> for UdpInputNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        process_frame(runtime, frame, process_udp_input_index)?;
        if frame.has_pending() {
            Ok(NodeResult::next_current(self.next))
        } else {
            Ok(NodeResult::drop())
        }
    }
}

impl<G> InternalNode<G> for UdpInputNode {}

pub fn packet_route_metadata(interface_id: &str, packet: &[u8]) -> CoreResult<RouteMetadata> {
    let parsed = parse_packet(packet)?;
    Ok(RouteMetadata {
        inbound: interface_id.to_owned(),
        network: parsed.network,
        protocol: parsed.protocol,
        source: Some(parsed.source),
        destination: Some(parsed.destination),
        ..Default::default()
    })
}

fn process_frame<G>(
    runtime: &DataPlaneRuntime<G>,
    frame: &mut BufferFrame,
    process: fn(&DataPlaneRuntime<G>, BufferIndex) -> CoreResult<bool>,
) -> CoreResult<()> {
    let mut cursor = frame.pair_batch_cursor();
    cursor.prefetch_next_pair(runtime);
    while let Some(batch) = cursor.next() {
        cursor.prefetch_next_pair(runtime);
        for index in batch.indices() {
            if !process(runtime, index)? {
                runtime.free_index(index);
            }
        }
    }
    frame.retain_indices(|index| Ok(runtime.with_buffer(index, |_| ()).is_ok()))
}

fn process_ip_input_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
) -> CoreResult<bool> {
    runtime.with_buffer_mut(index, |buffer| {
        let parsed = match parse_ip_packet(buffer.current()) {
            Ok(parsed) => parsed,
            Err(_) => return Ok(false),
        };
        if parsed.protocol != IpProtocol::Udp {
            return Ok(false);
        }

        *buffer.packet_cursor_mut() = parsed.cursor;
        let metadata = buffer.metadata_mut();
        metadata.source = Some(SocksAddr::ip(parsed.source, 0));
        metadata.destination = Some(SocksAddr::ip(parsed.destination, 0));
        metadata.protocol.clear();
        Ok(true)
    })?
}

fn process_udp_input_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
) -> CoreResult<bool> {
    runtime.with_buffer_mut(index, |buffer| {
        let cursor = buffer.packet_cursor();
        let parsed = match parse_udp_packet(buffer.current(), cursor) {
            Ok(parsed) => parsed,
            Err(_) => return Ok(false),
        };
        let source = buffer.metadata().source.as_ref().map(|addr| addr.host);
        let destination = buffer.metadata().destination.as_ref().map(|addr| addr.host);
        let (Some(source), Some(destination)) = (source, destination) else {
            return Ok(false);
        };

        *buffer.packet_cursor_mut() = cursor
            .with_transport_header(cursor.transport_header_offset(), UDP_HEADER_LEN)
            .with_transport_payload_offset(parsed.payload_offset);
        let metadata = buffer.metadata_mut();
        metadata.network = Network::Udp;
        metadata.protocol.clear();
        metadata.source = Some(SocksAddr::ip(source, parsed.source_port));
        metadata.destination = Some(SocksAddr::ip(destination, parsed.destination_port));
        Ok(true)
    })?
}

struct ParsedPacket {
    network: Network,
    protocol: String,
    source: SocksAddr,
    destination: SocksAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpProtocol {
    Icmpv4,
    Tcp,
    Udp,
    Icmpv6,
}

impl TryFrom<u8> for IpProtocol {
    type Error = CoreError;

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
struct ParsedIpPacket {
    protocol: IpProtocol,
    source: IpAddr,
    destination: IpAddr,
    cursor: BufferPacketCursor,
}

#[derive(Debug, Clone, Copy)]
struct ParsedUdpPacket {
    source_port: u16,
    destination_port: u16,
    payload_offset: usize,
}

fn parse_ip_packet(packet: &[u8]) -> CoreResult<ParsedIpPacket> {
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

    let (input, _) = be_u16(input)?;
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

    Ok((
        input,
        ParsedIpPacket {
            protocol: IpProtocol::try_from(protocol).map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?,
            source: IpAddr::V4(source),
            destination: IpAddr::V4(destination),
            cursor: BufferPacketCursor::new()
                .with_packet_len(total_len)
                .with_network_header(0, ihl)
                .with_transport_header(ihl, 0),
        },
    ))
}

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
            source: IpAddr::V6(source),
            destination: IpAddr::V6(destination),
            cursor: BufferPacketCursor::new()
                .with_packet_len(total_len)
                .with_network_header(0, IPV6_HEADER_LEN)
                .with_transport_header(IPV6_HEADER_LEN, 0),
        },
    ))
}

fn parse_packet(packet: &[u8]) -> CoreResult<ParsedPacket> {
    let parsed = parse_ip_packet(packet)?;
    let transport = packet
        .get(parsed.cursor.transport_header_offset()..parsed.cursor.packet_len())
        .ok_or_else(|| CoreError::internal("invalid transport packet cursor"))?;
    match parsed.protocol {
        IpProtocol::Tcp => parse_tcp_packet(parsed.source, parsed.destination, transport),
        IpProtocol::Udp => parse_udp_route_packet(parsed.source, parsed.destination, transport),
        IpProtocol::Icmpv4 => Ok(ParsedPacket {
            network: Network::Icmp,
            protocol: "icmp".to_owned(),
            source: SocksAddr::ip(parsed.source, 0),
            destination: SocksAddr::ip(parsed.destination, 0),
        }),
        IpProtocol::Icmpv6 => Ok(ParsedPacket {
            network: Network::Icmp,
            protocol: "ipv6-icmp".to_owned(),
            source: SocksAddr::ip(parsed.source, 0),
            destination: SocksAddr::ip(parsed.destination, 0),
        }),
    }
}

fn parse_tcp_packet(
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
) -> CoreResult<ParsedPacket> {
    let (_, header) = parse_tcp_header(transport)
        .map_err(|err| CoreError::internal(format!("invalid TCP segment: {err}")))?;
    Ok(ParsedPacket {
        network: Network::Tcp,
        protocol: String::new(),
        source: SocksAddr::ip(source, header.source_port),
        destination: SocksAddr::ip(destination, header.destination_port),
    })
}

fn parse_udp_route_packet(
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
) -> CoreResult<ParsedPacket> {
    let (_, header) = parse_udp_header(transport)
        .map_err(|err| CoreError::internal(format!("invalid UDP datagram: {err}")))?;
    Ok(ParsedPacket {
        network: Network::Udp,
        protocol: String::new(),
        source: SocksAddr::ip(source, header.source_port),
        destination: SocksAddr::ip(destination, header.destination_port),
    })
}

fn parse_udp_packet(packet: &[u8], cursor: BufferPacketCursor) -> CoreResult<ParsedUdpPacket> {
    let transport_offset = cursor.transport_header_offset();
    let packet_len = cursor.packet_len();
    let transport = packet
        .get(transport_offset..packet_len)
        .ok_or_else(|| CoreError::internal("invalid packet cursor"))?;
    let (_, header) = parse_udp_header(transport)
        .map_err(|err| CoreError::internal(format!("invalid UDP datagram: {err}")))?;
    Ok(ParsedUdpPacket {
        source_port: header.source_port,
        destination_port: header.destination_port,
        payload_offset: transport_offset + UDP_HEADER_LEN,
    })
}

#[derive(Debug, Clone, Copy)]
struct TcpHeader {
    source_port: u16,
    destination_port: u16,
}

fn parse_tcp_header(input: &[u8]) -> ParseResult<'_, TcpHeader> {
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
        TcpHeader {
            source_port,
            destination_port,
        },
    ))
}

#[derive(Debug, Clone, Copy)]
struct UdpHeader {
    source_port: u16,
    destination_port: u16,
}

fn parse_udp_header(input: &[u8]) -> ParseResult<'_, UdpHeader> {
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
        UdpHeader {
            source_port,
            destination_port,
        },
    ))
}

fn parse_ipv4_addr(input: &[u8]) -> ParseResult<'_, Ipv4Addr> {
    let (input, bytes) = take(4usize).parse(input)?;
    let bytes = <[u8; 4]>::try_from(bytes).expect("nom take returned four bytes");
    Ok((input, Ipv4Addr::from(bytes)))
}

fn parse_ipv6_addr(input: &[u8]) -> ParseResult<'_, Ipv6Addr> {
    let (input, bytes) = take(16usize).parse(input)?;
    let bytes = <[u8; 16]>::try_from(bytes).expect("nom take returned sixteen bytes");
    Ok((input, Ipv6Addr::from(bytes)))
}
