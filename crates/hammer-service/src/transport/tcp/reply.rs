use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use hammer_adapter::BufferPacketCursor;
use hammer_core::error::{CoreError, CoreResult};
pub use hammer_core::protocol::tcp::synthesize_ipv4_tcp_control;

pub fn tcp_control_cursor(packet: &[u8]) -> CoreResult<BufferPacketCursor> {
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
    Ok(BufferPacketCursor::new()
        .with_packet_len(packet_len)
        .with_network_header(0, network_header_len)
        .with_transport_header(tcp_offset, tcp_header_len)
        .with_transport_payload_offset(tcp_offset + tcp_header_len))
}
