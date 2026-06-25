use hammer_adapter::BufferPacketCursor;
use hammer_core::protocol::tcp::TcpControlPacketParseError;

pub fn tcp_control_cursor(packet: &[u8]) -> Result<BufferPacketCursor, TcpControlPacketParseError> {
    let Some(version_ihl) = packet.first().copied() else {
        return Err(TcpControlPacketParseError::EmptyPacket);
    };
    let (network_header_len, packet_len) = match version_ihl >> 4 {
        4 => {
            if packet.len() < 40 {
                return Err(TcpControlPacketParseError::PacketTooShort);
            }
            (
                usize::from(version_ihl & 0x0f) * 4,
                u16::from_be_bytes([packet[2], packet[3]]) as usize,
            )
        }
        6 => {
            if packet.len() < 60 {
                return Err(TcpControlPacketParseError::PacketTooShort);
            }
            let payload_len = u16::from_be_bytes([packet[4], packet[5]]) as usize;
            (40, 40 + payload_len)
        }
        _ => return Err(TcpControlPacketParseError::UnsupportedIpVersion),
    };
    if packet_len > packet.len() || network_header_len < 20 || network_header_len >= packet_len {
        return Err(TcpControlPacketParseError::InvalidCursor);
    }
    let tcp_offset = network_header_len;
    let tcp_header_len = usize::from(packet[tcp_offset + 12] >> 4) * 4;
    if tcp_header_len < 20 || tcp_offset + tcp_header_len > packet_len {
        return Err(TcpControlPacketParseError::InvalidHeaderLength);
    }
    Ok(BufferPacketCursor::new()
        .with_packet_len(packet_len)
        .with_network_header(0, network_header_len)
        .with_transport_header(tcp_offset, tcp_header_len)
        .with_transport_payload_offset(tcp_offset + tcp_header_len))
}
