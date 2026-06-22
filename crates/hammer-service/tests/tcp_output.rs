use std::net::SocketAddr;

use hammer_core::protocol::tcp::{TcpCapabilities, TcpSegmentFlags};
use hammer_service::transport::tcp::output::{
    tcp_available_send_window, tcp_output_next_sequence, tcp_output_sequence_len,
    tcp_payload_len_in_send_window,
};
use hammer_service::transport::tcp::segment::TcpSegment;
use hammer_service::transport::tcp::{TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_SYN};

#[test]
fn tcp_segment_writes_tcp_header_bytes() {
    let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let segment = TcpSegment::new(
        local,
        remote,
        100,
        200,
        4096,
        TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
        TcpCapabilities::default(),
        None,
        None,
        None,
        5,
    );
    let mut header = [0u8; 64];

    let written = segment.write_header(&mut header).expect("write header");

    assert_eq!(written, 20);
    assert_eq!(&header[0..2], &50000u16.to_be_bytes());
    assert_eq!(&header[2..4], &443u16.to_be_bytes());
    assert_eq!(&header[4..8], &100u32.to_be_bytes());
    assert_eq!(&header[8..12], &200u32.to_be_bytes());
    assert_eq!(header[12] >> 4, 5);
    assert_eq!(
        u16::from(header[13]) & TcpSegmentFlags::ACK.bits(),
        TcpSegmentFlags::ACK.bits()
    );
    assert_eq!(
        u16::from(header[13]) & TcpSegmentFlags::PSH.bits(),
        TcpSegmentFlags::PSH.bits()
    );
    assert_eq!(&header[14..16], &4096u16.to_be_bytes());
    assert_eq!(segment.payload_len(), 5);
}

#[test]
fn tcp_output_sequence_space_counts_control_bits_and_wraps() {
    let sequence = u32::MAX - 2;
    let sequence_len = tcp_output_sequence_len(TCP_FLAG_ACK | TCP_FLAG_SYN | TCP_FLAG_FIN, 3);

    assert_eq!(sequence_len, 5);
    assert_eq!(tcp_output_next_sequence(sequence, sequence_len), 2);
}

#[test]
fn tcp_output_send_window_helpers_account_for_inflight_bytes_and_control_len() {
    assert_eq!(tcp_available_send_window(10_000, 10_020, 40, 40), 20);
    assert_eq!(
        tcp_payload_len_in_send_window(10_000, 10_020, 40, 40, 32, 0),
        20
    );
    assert_eq!(
        tcp_payload_len_in_send_window(10_000, 10_020, 40, 40, 32, 1),
        19
    );

    assert_eq!(tcp_available_send_window(10_000, 10_020, 20, 20), 0);
    assert_eq!(
        tcp_payload_len_in_send_window(10_000, 10_020, 20, 20, 32, 0),
        0
    );

    assert_eq!(tcp_available_send_window(u32::MAX - 4, 7, 20, 20), 8);
    assert_eq!(
        tcp_payload_len_in_send_window(u32::MAX - 4, 7, 20, 20, 32, 1),
        7
    );
}

#[test]
fn tcp_output_send_window_uses_min_of_peer_window_and_congestion_window() {
    assert_eq!(tcp_available_send_window(1000, 1200, 8000, 1000), 800);
    assert_eq!(tcp_available_send_window(1000, 1200, 700, 1000), 500);
}

#[test]
fn tcp_output_payload_len_is_zero_when_congestion_window_is_full() {
    assert_eq!(
        tcp_payload_len_in_send_window(1000, 2000, 8000, 1000, 512, 0),
        0
    );
}
