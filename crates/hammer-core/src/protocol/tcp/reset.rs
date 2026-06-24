use std::net::Ipv6Addr;

use etherparse::TcpSlice;
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};

use crate::protocol::ip::parse_ip_packet;

use super::TcpSegmentFlags;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpResetReply {
    pub packet: std::vec::Vec<u8>,
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
    packet: &[u8],
    cursor: TcpResetPacketCursor,
) -> Option<TcpResetReply> {
    let source = parse_reset_source_segment(packet, cursor)?;
    if source.flags.contains(TcpSegmentFlags::RST) {
        return None;
    }
    match packet
        .get(cursor.network_header_offset)
        .copied()
        .map(|byte| byte >> 4)
    {
        Some(4) => synthesize_ipv4_tcp_reset(packet, cursor, source),
        Some(6) => synthesize_ipv6_tcp_reset(packet, cursor, source),
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
    let segment = TcpSlice::from_slice(transport).ok()?;
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

    let segment =
        TcpSlice::from_slice(packet.get(cursor.transport_header_offset..available_len)?).ok()?;
    if cursor.transport_payload_offset
        != cursor
            .transport_header_offset
            .checked_add(segment.header_len())?
    {
        return None;
    }

    let payload_len = available_len.checked_sub(cursor.transport_payload_offset)?;
    let sequence_len = payload_len
        .checked_add(usize::from(segment.syn()))?
        .checked_add(usize::from(segment.fin()))?;
    Some(ResetSourceSegment {
        source_port: segment.source_port(),
        destination_port: segment.destination_port(),
        sequence_number: segment.sequence_number(),
        acknowledgment_number: segment.ack().then(|| segment.acknowledgment_number()),
        flags: tcp_flags_from_slice(&segment),
        sequence_len: u32::try_from(sequence_len).ok()?,
    })
}

fn tcp_flags_from_slice(segment: &TcpSlice<'_>) -> TcpSegmentFlags {
    let mut flags = TcpSegmentFlags::empty();
    flags.set(TcpSegmentFlags::NS, segment.ns());
    flags.set(TcpSegmentFlags::FIN, segment.fin());
    flags.set(TcpSegmentFlags::SYN, segment.syn());
    flags.set(TcpSegmentFlags::RST, segment.rst());
    flags.set(TcpSegmentFlags::PSH, segment.psh());
    flags.set(TcpSegmentFlags::ACK, segment.ack());
    flags.set(TcpSegmentFlags::URG, segment.urg());
    flags.set(TcpSegmentFlags::ECE, segment.ece());
    flags.set(TcpSegmentFlags::CWR, segment.cwr());
    flags
}

fn synthesize_ipv4_tcp_reset(
    packet: &[u8],
    cursor: TcpResetPacketCursor,
    source: ResetSourceSegment,
) -> Option<TcpResetReply> {
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
    if packet.get(cursor.network_header_offset).copied()? >> 4 != 4 {
        return None;
    }

    let (response_sequence, response_acknowledgment, response_flags) =
        tcp_reset_response_fields(source)?;
    let total_len = IPV4_HEADER_LEN + TCP_HEADER_LEN;
    let mut reset = vec![0u8; total_len];

    reset[..IPV4_HEADER_LEN].copy_from_slice(
        &packet[cursor.network_header_offset..cursor.network_header_offset + IPV4_HEADER_LEN],
    );
    reset[0] = 0x45;
    reset[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    reset[10] = 0;
    reset[11] = 0;
    reset[12..16].copy_from_slice(
        packet.get(cursor.network_header_offset + 16..cursor.network_header_offset + 20)?,
    );
    reset[16..20].copy_from_slice(
        packet.get(cursor.network_header_offset + 12..cursor.network_header_offset + 16)?,
    );

    reset[20..22].copy_from_slice(&source.destination_port.to_be_bytes());
    reset[22..24].copy_from_slice(&source.source_port.to_be_bytes());
    reset[24..28].copy_from_slice(&response_sequence.to_be_bytes());
    reset[28..32].copy_from_slice(&response_acknowledgment.to_be_bytes());
    reset[32] = 0x50;
    reset[33] = response_flags;
    reset[36] = 0;
    reset[37] = 0;

    let pseudo_source = reset.get(12..16)?.try_into().ok()?;
    let pseudo_destination = reset.get(16..20)?.try_into().ok()?;
    let checksum = ipv4_l4_checksum(pseudo_source, pseudo_destination, 6, &reset[20..]);
    reset[36..38].copy_from_slice(&checksum.to_be_bytes());
    let header_checksum = internet_checksum(&reset[..IPV4_HEADER_LEN]);
    reset[10..12].copy_from_slice(&header_checksum.to_be_bytes());

    Some(TcpResetReply { packet: reset })
}

fn synthesize_ipv6_tcp_reset(
    packet: &[u8],
    cursor: TcpResetPacketCursor,
    source: ResetSourceSegment,
) -> Option<TcpResetReply> {
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
    if packet.get(cursor.network_header_offset).copied()? >> 4 != 6 {
        return None;
    }

    let (response_sequence, response_acknowledgment, response_flags) =
        tcp_reset_response_fields(source)?;
    let total_len = IPV6_HEADER_LEN + TCP_HEADER_LEN;
    let mut reset = vec![0u8; total_len];

    reset[..IPV6_HEADER_LEN].copy_from_slice(
        &packet[cursor.network_header_offset..cursor.network_header_offset + IPV6_HEADER_LEN],
    );
    reset[0] = 0x60;
    reset[4..6].copy_from_slice(&(TCP_HEADER_LEN as u16).to_be_bytes());
    reset[8..24].copy_from_slice(
        packet.get(cursor.network_header_offset + 24..cursor.network_header_offset + 40)?,
    );
    reset[24..40].copy_from_slice(
        packet.get(cursor.network_header_offset + 8..cursor.network_header_offset + 24)?,
    );

    reset[40..42].copy_from_slice(&source.destination_port.to_be_bytes());
    reset[42..44].copy_from_slice(&source.source_port.to_be_bytes());
    reset[44..48].copy_from_slice(&response_sequence.to_be_bytes());
    reset[48..52].copy_from_slice(&response_acknowledgment.to_be_bytes());
    reset[52] = 0x50;
    reset[53] = response_flags;
    reset[56] = 0;
    reset[57] = 0;

    let pseudo_source = Ipv6Addr::from(<[u8; 16]>::try_from(reset.get(8..24)?).ok()?);
    let pseudo_destination = Ipv6Addr::from(<[u8; 16]>::try_from(reset.get(24..40)?).ok()?);
    let checksum = ipv6_l4_checksum(pseudo_source, pseudo_destination, 6, &reset[40..]);
    reset[56..58].copy_from_slice(&checksum.to_be_bytes());

    Some(TcpResetReply { packet: reset })
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
