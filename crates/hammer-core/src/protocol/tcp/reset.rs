use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
use std::mem::{size_of, transmute};
use std::net::Ipv6Addr;
use thiserror::Error;

use crate::protocol::ip::parse_ip_packet;

use super::{TcpError, TcpSegmentFlags};

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Ipv4WireHeader {
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

impl Ipv4WireHeader {
    #[inline(always)]
    fn version(self) -> u8 {
        self.version_ihl >> 4
    }

    #[inline(always)]
    fn header_len(self) -> usize {
        usize::from(self.version_ihl & 0x0f) * 4
    }
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Ipv6WireHeader {
    version_traffic_flow: [u8; 4],
    payload_len: [u8; 2],
    next_header: u8,
    hop_limit: u8,
    source: [u8; 16],
    destination: [u8; 16],
}

impl Ipv6WireHeader {
    #[inline(always)]
    fn version(self) -> u8 {
        self.version_traffic_flow[0] >> 4
    }
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct TcpWireHeader {
    source_port: [u8; 2],
    destination_port: [u8; 2],
    sequence_number: [u8; 4],
    acknowledgment_number: [u8; 4],
    data_offset_reserved_flags: [u8; 2],
    advertised_window: [u8; 2],
    checksum: [u8; 2],
    urgent_pointer: [u8; 2],
}

impl TcpWireHeader {
    #[inline(always)]
    fn header_len(self) -> usize {
        usize::from(self.data_offset_reserved_flags[0] >> 4) * 4
    }

    #[inline(always)]
    fn source_port(self) -> u16 {
        u16::from_be_bytes(self.source_port)
    }

    #[inline(always)]
    fn destination_port(self) -> u16 {
        u16::from_be_bytes(self.destination_port)
    }

    #[inline(always)]
    fn sequence_number(self) -> u32 {
        u32::from_be_bytes(self.sequence_number)
    }

    #[inline(always)]
    fn acknowledgment_number(self) -> u32 {
        u32::from_be_bytes(self.acknowledgment_number)
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum TcpResetError {
    #[error("tcp reset reply uses unsupported IP version")]
    UnsupportedIpVersion,
}

impl From<TcpResetError> for TcpError {
    #[inline]
    fn from(_: TcpResetError) -> Self {
        TcpError::SegmentInvalid
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpResetPacketCursor {
    pub packet_len: usize,
    pub network_header_offset: usize,
    pub network_header_len: usize,
    pub transport_header_offset: usize,
    pub transport_header_len: usize,
    pub transport_payload_offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResetSourceSegment {
    source_port: u16,
    destination_port: u16,
    sequence_number: u32,
    acknowledgment_number: Option<u32>,
    flags: TcpSegmentFlags,
    sequence_len: u32,
}

pub fn tcp_reset_reply_from_current_packet(
    output: &mut [u8],
    packet: &[u8],
    cursor: TcpResetPacketCursor,
) -> Option<usize> {
    let source = parse_reset_source_segment(packet, cursor)?;
    if source.flags.contains(TcpSegmentFlags::RST) {
        return None;
    }
    match packet
        .get(cursor.network_header_offset)
        .copied()
        .map(|byte| byte >> 4)
    {
        Some(4) => synthesize_ipv4_tcp_reset(output, packet, cursor, source),
        Some(6) => synthesize_ipv6_tcp_reset(output, packet, cursor, source),
        _ => None,
    }
}

pub fn tcp_reset_network_header_len(packet: &[u8]) -> Option<usize> {
    match packet.first().copied().map(|byte| byte >> 4) {
        Some(4) => Some(20),
        Some(6) => Some(40),
        _ => None,
    }
}

pub fn tcp_reset_remote_reply_addrs(
    packet: &[u8],
    cursor: TcpResetPacketCursor,
) -> Option<(std::net::SocketAddr, std::net::SocketAddr)> {
    let parsed = parse_ip_packet(packet).ok()?;
    if cursor.transport_header_offset != parsed.transport_header_offset {
        return None;
    }
    let transport = packet.get(parsed.transport_header_offset..parsed.packet_len)?;
    let segment = read_packed::<TcpWireHeader>(transport, 0)?;
    Some((
        std::net::SocketAddr::new(parsed.destination, segment.destination_port()),
        std::net::SocketAddr::new(parsed.source, segment.source_port()),
    ))
}

fn parse_reset_source_segment(
    packet: &[u8],
    cursor: TcpResetPacketCursor,
) -> Option<ResetSourceSegment> {
    let available_len = packet.len().min(cursor.packet_len);
    if cursor.transport_header_offset > available_len
        || cursor.transport_payload_offset > available_len
    {
        return None;
    }

    let segment = read_packed::<TcpWireHeader>(packet, cursor.transport_header_offset)?;
    if cursor.transport_payload_offset
        != cursor
            .transport_header_offset
            .checked_add(segment.header_len())?
    {
        return None;
    }

    let payload_len = available_len.checked_sub(cursor.transport_payload_offset)?;
    let sequence_len = payload_len
        .checked_add(usize::from(
            tcp_flags_from_header(segment).contains(TcpSegmentFlags::SYN),
        ))?
        .checked_add(usize::from(
            tcp_flags_from_header(segment).contains(TcpSegmentFlags::FIN),
        ))?;
    let flags = tcp_flags_from_header(segment);
    Some(ResetSourceSegment {
        source_port: segment.source_port(),
        destination_port: segment.destination_port(),
        sequence_number: segment.sequence_number(),
        acknowledgment_number: flags
            .contains(TcpSegmentFlags::ACK)
            .then(|| segment.acknowledgment_number()),
        flags,
        sequence_len: u32::try_from(sequence_len).ok()?,
    })
}

fn tcp_flags_from_header(segment: TcpWireHeader) -> TcpSegmentFlags {
    let mut flags = TcpSegmentFlags::empty();
    let raw = u16::from_be_bytes(segment.data_offset_reserved_flags);
    flags.set(TcpSegmentFlags::NS, raw & 0x0100 != 0);
    flags.set(TcpSegmentFlags::FIN, raw & TcpSegmentFlags::FIN.bits() != 0);
    flags.set(TcpSegmentFlags::SYN, raw & TcpSegmentFlags::SYN.bits() != 0);
    flags.set(TcpSegmentFlags::RST, raw & TcpSegmentFlags::RST.bits() != 0);
    flags.set(TcpSegmentFlags::PSH, raw & TcpSegmentFlags::PSH.bits() != 0);
    flags.set(TcpSegmentFlags::ACK, raw & TcpSegmentFlags::ACK.bits() != 0);
    flags.set(TcpSegmentFlags::URG, raw & TcpSegmentFlags::URG.bits() != 0);
    flags.set(TcpSegmentFlags::ECE, raw & TcpSegmentFlags::ECE.bits() != 0);
    flags.set(TcpSegmentFlags::CWR, raw & TcpSegmentFlags::CWR.bits() != 0);
    flags
}

fn synthesize_ipv4_tcp_reset(
    output: &mut [u8],
    packet: &[u8],
    cursor: TcpResetPacketCursor,
    source: ResetSourceSegment,
) -> Option<usize> {
    const IPV4_HEADER_LEN: usize = 20;
    const TCP_HEADER_LEN: usize = 20;

    if cursor.network_header_len < IPV4_HEADER_LEN || cursor.transport_header_len < TCP_HEADER_LEN {
        return None;
    }

    let available_len = packet.len().min(cursor.packet_len);
    let network_end = cursor
        .network_header_offset
        .checked_add(cursor.network_header_len)?;
    let transport_end = cursor
        .transport_header_offset
        .checked_add(cursor.transport_header_len)?;
    if network_end > available_len || transport_end > available_len {
        return None;
    }
    let header = read_packed::<Ipv4WireHeader>(packet, cursor.network_header_offset)?;
    if header.version() != 4 || header.header_len() < IPV4_HEADER_LEN {
        return None;
    }

    let (response_sequence, response_acknowledgment, response_flags) =
        tcp_reset_response_fields(source)?;
    let total_len = IPV4_HEADER_LEN + TCP_HEADER_LEN;
    let reset = output.get_mut(..total_len)?;
    reset.fill(0);
    let mut reply_header = header;
    reply_header.version_ihl = 0x45;
    reply_header.total_len = (total_len as u16).to_be_bytes();
    reply_header.checksum = [0, 0];
    reply_header.source = header.destination;
    reply_header.destination = header.source;
    write_packed(reset, 0, reply_header)?;

    let mut reply_segment = TcpWireHeader {
        source_port: source.destination_port.to_be_bytes(),
        destination_port: source.source_port.to_be_bytes(),
        sequence_number: response_sequence.to_be_bytes(),
        acknowledgment_number: response_acknowledgment.to_be_bytes(),
        data_offset_reserved_flags: [0x50, response_flags],
        advertised_window: [0, 0],
        checksum: [0, 0],
        urgent_pointer: [0, 0],
    };
    write_packed(reset, IPV4_HEADER_LEN, reply_segment)?;
    let checksum = ipv4_l4_checksum(
        reply_header.source,
        reply_header.destination,
        6,
        &reset[20..],
    );
    reply_segment.checksum = checksum.to_be_bytes();
    write_packed(reset, IPV4_HEADER_LEN, reply_segment)?;
    reply_header.checksum = internet_checksum(&reset[..IPV4_HEADER_LEN]).to_be_bytes();
    write_packed(reset, 0, reply_header)?;

    Some(total_len)
}

fn synthesize_ipv6_tcp_reset(
    output: &mut [u8],
    packet: &[u8],
    cursor: TcpResetPacketCursor,
    source: ResetSourceSegment,
) -> Option<usize> {
    const IPV6_HEADER_LEN: usize = 40;
    const TCP_HEADER_LEN: usize = 20;

    if cursor.network_header_len < IPV6_HEADER_LEN || cursor.transport_header_len < TCP_HEADER_LEN {
        return None;
    }

    let available_len = packet.len().min(cursor.packet_len);
    let network_end = cursor
        .network_header_offset
        .checked_add(cursor.network_header_len)?;
    let transport_end = cursor
        .transport_header_offset
        .checked_add(cursor.transport_header_len)?;
    if network_end > available_len || transport_end > available_len {
        return None;
    }
    let header = read_packed::<Ipv6WireHeader>(packet, cursor.network_header_offset)?;
    if header.version() != 6 {
        return None;
    }

    let (response_sequence, response_acknowledgment, response_flags) =
        tcp_reset_response_fields(source)?;
    let total_len = IPV6_HEADER_LEN + TCP_HEADER_LEN;
    let reset = output.get_mut(..total_len)?;
    reset.fill(0);
    let mut reply_header = header;
    reply_header.version_traffic_flow[0] = 0x60;
    reply_header.payload_len = (TCP_HEADER_LEN as u16).to_be_bytes();
    reply_header.source = header.destination;
    reply_header.destination = header.source;
    write_packed(reset, 0, reply_header)?;

    let mut reply_segment = TcpWireHeader {
        source_port: source.destination_port.to_be_bytes(),
        destination_port: source.source_port.to_be_bytes(),
        sequence_number: response_sequence.to_be_bytes(),
        acknowledgment_number: response_acknowledgment.to_be_bytes(),
        data_offset_reserved_flags: [0x50, response_flags],
        advertised_window: [0, 0],
        checksum: [0, 0],
        urgent_pointer: [0, 0],
    };
    write_packed(reset, IPV6_HEADER_LEN, reply_segment)?;
    let checksum = ipv6_l4_checksum(
        Ipv6Addr::from(reply_header.source),
        Ipv6Addr::from(reply_header.destination),
        6,
        &reset[40..],
    );
    reply_segment.checksum = checksum.to_be_bytes();
    write_packed(reset, IPV6_HEADER_LEN, reply_segment)?;

    Some(total_len)
}

fn tcp_reset_response_fields(source: ResetSourceSegment) -> Option<(u32, u32, u8)> {
    const TCP_FLAG_RST: u8 = 0x04;
    const TCP_FLAG_ACK: u8 = 0x10;

    if source.flags.contains(TcpSegmentFlags::ACK) {
        Some((source.acknowledgment_number?, 0, TCP_FLAG_RST))
    } else {
        Some((
            0,
            source.sequence_number.wrapping_add(source.sequence_len),
            TCP_FLAG_RST | TCP_FLAG_ACK,
        ))
    }
}

fn ipv4_l4_checksum(source: [u8; 4], destination: [u8; 4], protocol: u8, segment: &[u8]) -> u16 {
    internet_checksum_parts(&[
        &source,
        &destination,
        &[0, protocol],
        &(segment.len() as u16).to_be_bytes(),
        segment,
    ])
}

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    internet_checksum_parts(&[
        &source.octets(),
        &destination.octets(),
        &(segment.len() as u32).to_be_bytes(),
        &[0, 0, 0, protocol],
        segment,
    ])
}

#[inline(always)]
fn read_packed<T>(packet: &[u8], offset: usize) -> Option<T>
where
    T: Copy,
{
    let end = offset.checked_add(size_of::<T>())?;
    let _ = packet.get(offset..end)?;
    let ptr = unsafe { transmute::<_, *const T>(packet.as_ptr().add(offset)) };
    Some(unsafe { ptr.read_unaligned() })
}

#[inline(always)]
fn write_packed<T>(packet: &mut [u8], offset: usize, value: T) -> Option<()>
where
    T: Copy,
{
    let end = offset.checked_add(size_of::<T>())?;
    let _ = packet.get_mut(offset..end)?;
    let ptr = unsafe { transmute::<_, *mut T>(packet.as_mut_ptr().add(offset)) };
    unsafe { ptr.write_unaligned(value) };
    Some(())
}
