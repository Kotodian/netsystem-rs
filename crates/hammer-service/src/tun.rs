use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DriverNode, Network, Node, NodeId, NodeResult, OutputNode,
    RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};

const DEFAULT_TUN_RECV_BATCH: usize = 256;
const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const TCP_HEADER_MIN_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;
const IPV4_TOTAL_LENGTH_OFFSET: usize = 2;
const IPV4_PROTOCOL_OFFSET: usize = 9;
const IPV4_SOURCE_OFFSET: usize = 12;
const IPV4_DESTINATION_OFFSET: usize = 16;
const IPV6_PAYLOAD_LEN_OFFSET: usize = 4;
const IPV6_PROTOCOL_OFFSET: usize = 6;
const IPV6_SOURCE_OFFSET: usize = 8;
const IPV6_DESTINATION_OFFSET: usize = 24;
const TCP_SOURCE_PORT_OFFSET: usize = 0;
const TCP_DESTINATION_PORT_OFFSET: usize = 2;
const TCP_DATA_OFFSET_OFFSET: usize = 12;
const UDP_SOURCE_PORT_OFFSET: usize = 0;
const UDP_DESTINATION_PORT_OFFSET: usize = 2;
const UDP_LENGTH_OFFSET: usize = 4;

pub trait TunPacketSource {
    fn recv_batch(&mut self, packets: &mut Vec<Vec<u8>>, max: usize) -> CoreResult<usize>;
}

pub trait TunPacketSink {
    fn send_batch(&mut self, packets: &mut Vec<Vec<u8>>) -> CoreResult<()>;
}

pub struct TunInputDriverNode<I> {
    input: I,
    interface_id: String,
    next: NodeId,
    max_batch: usize,
}

impl<I> TunInputDriverNode<I> {
    pub fn new(input: I, interface_id: impl Into<String>, next: NodeId) -> Self {
        Self {
            input,
            interface_id: interface_id.into(),
            next,
            max_batch: DEFAULT_TUN_RECV_BATCH,
        }
    }

    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch;
        self
    }
}

impl<I, G> Node<G> for TunInputDriverNode<I>
where
    I: TunPacketSource,
{
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let max_batch = self.max_batch.min(frame.remaining_capacity());
        let mut packets = Vec::with_capacity(max_batch);
        self.input.recv_batch(&mut packets, max_batch)?;
        for packet in packets {
            let metadata = packet_route_metadata(&self.interface_id, &packet)?;
            let index = runtime.alloc_index_with_bytes(metadata, &packet)?;
            if let Err(err) = frame.push_index(index) {
                runtime.free_index(index);
                return Err(err);
            }
        }
        if frame.has_pending() {
            Ok(NodeResult::next_current(self.next))
        } else {
            Ok(NodeResult::drop())
        }
    }
}

impl<I, G> DriverNode<G> for TunInputDriverNode<I> where I: TunPacketSource {}

pub struct TunOutputNode<O> {
    output: O,
}

impl<O> TunOutputNode<O> {
    pub fn new(output: O) -> Self {
        Self { output }
    }
}

impl<O, G> Node<G> for TunOutputNode<O>
where
    O: TunPacketSink,
{
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut packets = Vec::with_capacity(frame.pending_len());
        for index in frame.drain_pending() {
            let packet = runtime.copy_current_chain(index);
            runtime.free_index(index);
            packets.push(packet?);
        }
        if !packets.is_empty() {
            self.output.send_batch(&mut packets)?;
        }
        Ok(NodeResult::drop())
    }
}

impl<O, G> OutputNode<G> for TunOutputNode<O> where O: TunPacketSink {}

#[derive(Clone, Default)]
pub struct MemoryTunDevice {
    inner: Rc<RefCell<MemoryTunInner>>,
}

#[derive(Default)]
struct MemoryTunInner {
    input: VecDeque<Vec<u8>>,
    output: VecDeque<Vec<u8>>,
    output_batch_sizes: Vec<usize>,
    closed: bool,
}

#[derive(Clone)]
pub struct MemoryTunInput {
    inner: Rc<RefCell<MemoryTunInner>>,
}

#[derive(Clone)]
pub struct MemoryTunOutput {
    inner: Rc<RefCell<MemoryTunInner>>,
}

impl MemoryTunDevice {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn input(&self) -> MemoryTunInput {
        MemoryTunInput {
            inner: Rc::clone(&self.inner),
        }
    }

    pub fn output(&self) -> MemoryTunOutput {
        MemoryTunOutput {
            inner: Rc::clone(&self.inner),
        }
    }

    pub fn inject(&self, packet: Vec<u8>) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        inner.input.push_back(packet);
        Ok(())
    }

    pub fn drain_output(&self) -> Vec<Vec<u8>> {
        self.inner.borrow_mut().output.drain(..).collect()
    }

    pub fn drain_output_batch_sizes(&self) -> Vec<usize> {
        self.inner
            .borrow_mut()
            .output_batch_sizes
            .drain(..)
            .collect()
    }

    pub fn close(&self) {
        self.inner.borrow_mut().closed = true;
    }
}

impl TunPacketSource for MemoryTunInput {
    fn recv_batch(&mut self, packets: &mut Vec<Vec<u8>>, max: usize) -> CoreResult<usize> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        let start_len = packets.len();
        while packets.len() - start_len < max {
            let Some(packet) = inner.input.pop_front() else {
                break;
            };
            packets.push(packet);
        }
        Ok(packets.len() - start_len)
    }
}

impl TunPacketSink for MemoryTunOutput {
    fn send_batch(&mut self, packets: &mut Vec<Vec<u8>>) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        if !packets.is_empty() {
            inner.output_batch_sizes.push(packets.len());
        }
        inner.output.extend(packets.drain(..));
        Ok(())
    }
}

pub fn packet_route_metadata(interface_id: &str, packet: &[u8]) -> CoreResult<RouteMetadata> {
    let parsed = parse_ip_packet(packet)?;
    Ok(RouteMetadata {
        inbound: interface_id.to_owned(),
        network: parsed.network,
        protocol: parsed.protocol,
        source: Some(parsed.source),
        destination: Some(parsed.destination),
        ..Default::default()
    })
}

struct ParsedTunPacket {
    network: Network,
    protocol: String,
    source: SocksAddr,
    destination: SocksAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpVersion {
    V4,
    V6,
}

impl IpVersion {
    fn from_packet(packet: &[u8]) -> CoreResult<Self> {
        let Some(first) = packet.first() else {
            return Err(CoreError::internal("empty IP packet"));
        };
        match first >> 4 {
            4 => Ok(Self::V4),
            6 => Ok(Self::V6),
            other => Err(CoreError::internal(format!(
                "unsupported IP version: {other}"
            ))),
        }
    }
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

fn parse_ip_packet(packet: &[u8]) -> CoreResult<ParsedTunPacket> {
    match IpVersion::from_packet(packet)? {
        IpVersion::V4 => parse_ipv4_packet(packet),
        IpVersion::V6 => parse_ipv6_packet(packet),
    }
}

fn parse_ipv4_packet(packet: &[u8]) -> CoreResult<ParsedTunPacket> {
    if packet.len() < IPV4_HEADER_MIN_LEN {
        return Err(CoreError::internal("short IPv4 packet"));
    }
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    let total_len = read_u16(packet, IPV4_TOTAL_LENGTH_OFFSET) as usize;
    if ihl < IPV4_HEADER_MIN_LEN || total_len < ihl || packet.len() < total_len {
        return Err(CoreError::internal("invalid IPv4 packet length"));
    }
    let source = IpAddr::V4(Ipv4Addr::new(
        packet[IPV4_SOURCE_OFFSET],
        packet[IPV4_SOURCE_OFFSET + 1],
        packet[IPV4_SOURCE_OFFSET + 2],
        packet[IPV4_SOURCE_OFFSET + 3],
    ));
    let destination = IpAddr::V4(Ipv4Addr::new(
        packet[IPV4_DESTINATION_OFFSET],
        packet[IPV4_DESTINATION_OFFSET + 1],
        packet[IPV4_DESTINATION_OFFSET + 2],
        packet[IPV4_DESTINATION_OFFSET + 3],
    ));
    parse_transport(
        IpProtocol::try_from(packet[IPV4_PROTOCOL_OFFSET])?,
        source,
        destination,
        &packet[ihl..total_len],
    )
}

fn parse_ipv6_packet(packet: &[u8]) -> CoreResult<ParsedTunPacket> {
    if packet.len() < IPV6_HEADER_LEN {
        return Err(CoreError::internal("short IPv6 packet"));
    }
    let payload_len = read_u16(packet, IPV6_PAYLOAD_LEN_OFFSET) as usize;
    let total_len = IPV6_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| CoreError::internal("IPv6 packet length overflow"))?;
    if packet.len() < total_len {
        return Err(CoreError::internal("short IPv6 packet"));
    }
    let source = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET])
            .map_err(|_| CoreError::internal("invalid IPv6 source"))?,
    ));
    let destination = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN])
            .map_err(|_| CoreError::internal("invalid IPv6 destination"))?,
    ));
    parse_transport(
        IpProtocol::try_from(packet[IPV6_PROTOCOL_OFFSET])?,
        source,
        destination,
        &packet[IPV6_HEADER_LEN..total_len],
    )
}

fn parse_transport(
    protocol: IpProtocol,
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
) -> CoreResult<ParsedTunPacket> {
    match protocol {
        IpProtocol::Tcp => parse_tcp(source, destination, transport),
        IpProtocol::Udp => parse_udp(source, destination, transport),
        IpProtocol::Icmpv4 => Ok(ParsedTunPacket {
            network: Network::Icmp,
            protocol: "icmp".to_owned(),
            source: SocksAddr::ip(source, 0),
            destination: SocksAddr::ip(destination, 0),
        }),
        IpProtocol::Icmpv6 => Ok(ParsedTunPacket {
            network: Network::Icmp,
            protocol: "ipv6-icmp".to_owned(),
            source: SocksAddr::ip(source, 0),
            destination: SocksAddr::ip(destination, 0),
        }),
    }
}

fn parse_tcp(source: IpAddr, destination: IpAddr, transport: &[u8]) -> CoreResult<ParsedTunPacket> {
    if transport.len() < TCP_HEADER_MIN_LEN {
        return Err(CoreError::internal("short TCP segment"));
    }
    let data_offset = ((transport[TCP_DATA_OFFSET_OFFSET] >> 4) as usize) * 4;
    if data_offset < TCP_HEADER_MIN_LEN || transport.len() < data_offset {
        return Err(CoreError::internal("invalid TCP data offset"));
    }
    Ok(ParsedTunPacket {
        network: Network::Tcp,
        protocol: String::new(),
        source: SocksAddr::ip(source, read_u16(transport, TCP_SOURCE_PORT_OFFSET)),
        destination: SocksAddr::ip(
            destination,
            read_u16(transport, TCP_DESTINATION_PORT_OFFSET),
        ),
    })
}

fn parse_udp(source: IpAddr, destination: IpAddr, transport: &[u8]) -> CoreResult<ParsedTunPacket> {
    if transport.len() < UDP_HEADER_LEN {
        return Err(CoreError::internal("short UDP datagram"));
    }
    let length = read_u16(transport, UDP_LENGTH_OFFSET) as usize;
    if length < UDP_HEADER_LEN || transport.len() < length {
        return Err(CoreError::internal("invalid UDP length"));
    }
    Ok(ParsedTunPacket {
        network: Network::Udp,
        protocol: String::new(),
        source: SocksAddr::ip(source, read_u16(transport, UDP_SOURCE_PORT_OFFSET)),
        destination: SocksAddr::ip(
            destination,
            read_u16(transport, UDP_DESTINATION_PORT_OFFSET),
        ),
    })
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
}
