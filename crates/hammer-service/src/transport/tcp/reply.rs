use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use hammer_adapter::{
    BufferIndex, BufferPacketCursor, DataPlaneBuffers, Network, RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};

use super::output::TcpOutputBackend;

pub const TCP_FLAG_FIN: u8 = 0x01;
pub const TCP_FLAG_SYN: u8 = 0x02;
pub const TCP_FLAG_RST: u8 = 0x04;
pub const TCP_FLAG_PSH: u8 = 0x08;
pub const TCP_FLAG_ACK: u8 = 0x10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpControlFlags(u8);

impl TcpControlFlags {
    #[inline]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

pub fn synthesize_ipv4_tcp_control(
    local: SocketAddr,
    remote: SocketAddr,
    send_sequence: u32,
    receive_acknowledgment: u32,
    window: u16,
    flags: TcpControlFlags,
    options: &[u8],
) -> CoreResult<Vec<u8>> {
    let (local_ip, remote_ip) = match (local.ip(), remote.ip()) {
        (IpAddr::V4(local_ip), IpAddr::V4(remote_ip)) => (local_ip, remote_ip),
        _ => return Err(CoreError::internal("ipv4 tcp control requires IPv4 addrs")),
    };
    if options.len() % 4 != 0 {
        return Err(CoreError::internal("tcp control options must be aligned"));
    }

    let tcp_header_len = 20usize
        .checked_add(options.len())
        .ok_or_else(|| CoreError::internal("tcp control header length overflow"))?;
    if tcp_header_len > 60 {
        return Err(CoreError::internal("tcp control header too large"));
    }
    let packet_len = 20usize
        .checked_add(tcp_header_len)
        .ok_or_else(|| CoreError::internal("tcp control packet length overflow"))?;
    let total_len = u16::try_from(packet_len)
        .map_err(|_| CoreError::internal("tcp control packet too large"))?;

    let mut packet = vec![0u8; packet_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&total_len.to_be_bytes());
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&local_ip.octets());
    packet[16..20].copy_from_slice(&remote_ip.octets());

    let tcp = &mut packet[20..];
    tcp[0..2].copy_from_slice(&local.port().to_be_bytes());
    tcp[2..4].copy_from_slice(&remote.port().to_be_bytes());
    tcp[4..8].copy_from_slice(&send_sequence.to_be_bytes());
    tcp[8..12].copy_from_slice(&receive_acknowledgment.to_be_bytes());
    tcp[12] = ((tcp_header_len / 4) as u8) << 4;
    tcp[13] = flags.bits();
    tcp[14..16].copy_from_slice(&window.to_be_bytes());
    if !options.is_empty() {
        tcp[20..20 + options.len()].copy_from_slice(options);
    }

    let checksum = ipv4_l4_checksum(local_ip, remote_ip, 6, &packet[20..]);
    packet[36..38].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    Ok(packet)
}

pub fn tcp_control_metadata(local: SocketAddr, remote: SocketAddr) -> RouteMetadata {
    RouteMetadata {
        network: Network::Tcp,
        source: Some(SocksAddr::ip(local.ip(), local.port())),
        destination: Some(SocksAddr::ip(remote.ip(), remote.port())),
        ..RouteMetadata::default()
    }
}

pub fn emit_tcp_control_packet(
    buffers: &DataPlaneBuffers,
    output: &dyn TcpOutputBackend,
    packet: &[u8],
    metadata: RouteMetadata,
) -> CoreResult<()> {
    let index = buffers.alloc_index_with_bytes(metadata, packet)?;
    let result = (|| {
        set_ipv4_tcp_cursor(buffers, index, packet)?;
        output.emit_buffer(buffers, index)
    })();
    buffers.free_index(index);
    result
}

fn set_ipv4_tcp_cursor(
    buffers: &DataPlaneBuffers,
    index: BufferIndex,
    packet: &[u8],
) -> CoreResult<()> {
    let Some(version_ihl) = packet.first().copied() else {
        return Err(CoreError::internal("tcp control packet is empty"));
    };
    if version_ihl >> 4 != 4 {
        return Err(CoreError::internal("tcp control packet must be IPv4"));
    }
    if packet.len() < 40 {
        return Err(CoreError::internal("tcp control packet is too short"));
    }
    let network_header_len = usize::from(version_ihl & 0x0f) * 4;
    let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    if packet_len > packet.len() || network_header_len < 20 || network_header_len >= packet_len {
        return Err(CoreError::internal("tcp control packet cursor is invalid"));
    }
    let tcp_offset = network_header_len;
    let tcp_header_len = usize::from(packet[tcp_offset + 12] >> 4) * 4;
    if tcp_header_len < 20 || tcp_offset + tcp_header_len > packet_len {
        return Err(CoreError::internal("tcp control header length is invalid"));
    }
    buffers.get_buffer_mut(index)?.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, network_header_len)
            .with_transport_header(tcp_offset, tcp_header_len)
            .with_transport_payload_offset(tcp_offset + tcp_header_len),
    );
    Ok(())
}

fn ipv4_l4_checksum(
    source: Ipv4Addr,
    destination: Ipv4Addr,
    protocol: u8,
    segment: &[u8],
) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.push(0);
    pseudo.push(protocol);
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn update_ipv4_header_checksum(packet: &mut [u8]) {
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = match chunk {
            [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
            [hi] => u16::from_be_bytes([*hi, 0]) as u32,
            _ => unreachable!(),
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}
