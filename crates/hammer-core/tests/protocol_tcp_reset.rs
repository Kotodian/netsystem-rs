use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use hammer_core::protocol::tcp::{
    TcpResetPacketCursor, tcp_reset_network_header_len, tcp_reset_remote_reply_addrs,
    tcp_reset_reply_from_current_packet,
};

#[test]
fn core_tcp_reset_ack_segment_replies_with_rst_only() {
    let packet = ipv4_tcp_packet(0x10, 1_000, 9_000, &[]);
    let mut reply = [0u8; 60];
    let reply_len =
        tcp_reset_reply_from_current_packet(&mut reply, &packet, ipv4_tcp_cursor(packet.len()))
            .expect("reply for ack segment");
    let reply_tcp = etherparse::TcpSlice::from_slice(&reply[20..reply_len]).expect("parse reply");

    assert!(reply_tcp.rst());
    assert!(!reply_tcp.ack());
    assert_eq!(reply_tcp.sequence_number(), 9_000);
    assert_eq!(reply_tcp.acknowledgment_number(), 0);
    assert_eq!(reply_tcp.source_port(), 80);
    assert_eq!(reply_tcp.destination_port(), 50_000);
}

#[test]
fn core_tcp_reset_non_ack_segment_replies_with_rst_ack_using_sequence_space() {
    let packet = ipv4_tcp_packet(0x02, 1_000, 0, b"hello");
    let mut reply = [0u8; 60];
    let reply_len =
        tcp_reset_reply_from_current_packet(&mut reply, &packet, ipv4_tcp_cursor(packet.len()))
            .expect("reply for syn");
    let reply_tcp = etherparse::TcpSlice::from_slice(&reply[20..reply_len]).expect("parse reply");

    assert!(reply_tcp.rst());
    assert!(reply_tcp.ack());
    assert_eq!(reply_tcp.sequence_number(), 0);
    assert_eq!(reply_tcp.acknowledgment_number(), 1_006);
}

#[test]
fn core_tcp_reset_fin_consumes_sequence_space() {
    let packet = ipv4_tcp_packet(0x01, 4_000, 0, b"abc");
    let mut reply = [0u8; 60];
    let reply_len =
        tcp_reset_reply_from_current_packet(&mut reply, &packet, ipv4_tcp_cursor(packet.len()))
            .expect("reply for fin");
    let reply_tcp = etherparse::TcpSlice::from_slice(&reply[20..reply_len]).expect("parse reply");

    assert_eq!(reply_tcp.acknowledgment_number(), 4_004);
}

#[test]
fn core_tcp_reset_drops_existing_rst_segment() {
    let packet = ipv4_tcp_packet(0x14, 1_000, 9_000, &[]);

    assert!(
        tcp_reset_reply_from_current_packet(&mut [0u8; 60], &packet, ipv4_tcp_cursor(packet.len()))
            .is_none()
    );
}

#[test]
fn core_tcp_reset_reports_reply_header_len_and_addrs() {
    let packet = ipv4_tcp_packet(0x10, 1_000, 9_000, &[]);
    let cursor = ipv4_tcp_cursor(packet.len());
    let (local, remote) = tcp_reset_remote_reply_addrs(&packet, cursor).expect("reply addrs");

    assert_eq!(tcp_reset_network_header_len(&packet), Some(20));
    assert_eq!(
        local,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 2)), 80)
    );
    assert_eq!(
        remote,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 50_000)
    );
}

fn ipv4_tcp_cursor(packet_len: usize) -> TcpResetPacketCursor {
    TcpResetPacketCursor {
        packet_len,
        network_header_offset: 0,
        network_header_len: 20,
        transport_header_offset: 20,
        transport_header_len: 20,
        transport_payload_offset: 40,
    }
}

fn ipv4_tcp_packet(flags: u8, sequence: u32, acknowledgment: u32, payload: &[u8]) -> Vec<u8> {
    let packet_len = 20 + 20 + payload.len();
    let total_len = u16::try_from(packet_len).expect("packet length fits");
    let mut packet = vec![0u8; packet_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&total_len.to_be_bytes());
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&[192, 0, 2, 1]);
    packet[16..20].copy_from_slice(&[198, 51, 100, 2]);
    packet[20..22].copy_from_slice(&50_000u16.to_be_bytes());
    packet[22..24].copy_from_slice(&80u16.to_be_bytes());
    packet[24..28].copy_from_slice(&sequence.to_be_bytes());
    packet[28..32].copy_from_slice(&acknowledgment.to_be_bytes());
    packet[32] = 0x50;
    packet[33] = flags;
    packet[34..36].copy_from_slice(&4096u16.to_be_bytes());
    if !payload.is_empty() {
        packet[40..40 + payload.len()].copy_from_slice(payload);
    }
    packet
}
