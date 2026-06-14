use hammer_core::protocol::tcp::{
    TcpSegmentFlags, TcpSegmentParseError, TcpSegmentView, tcp_options_from_bytes,
};

#[test]
fn core_tcp_segment_parses_header_ports_sequence_ack_flags_window_options_and_payload() {
    let bytes = tcp_segment(&[2, 4, 0x05, 0xb4], b"hello");

    let segment = TcpSegmentView::parse(&bytes).expect("parse tcp segment");

    assert_eq!(segment.source_port(), 49_152);
    assert_eq!(segment.destination_port(), 443);
    assert_eq!(segment.sequence_number(), 0x0102_0304);
    assert_eq!(segment.acknowledgment_number(), Some(0x1112_1314));
    assert_eq!(segment.advertised_window(), 32_768);
    assert!(segment.flags().contains(TcpSegmentFlags::SYN));
    assert!(segment.flags().contains(TcpSegmentFlags::ACK));
    assert_eq!(segment.header_len(), 24);
    assert_eq!(segment.options(), &[2, 4, 0x05, 0xb4]);
    assert_eq!(segment.payload(), b"hello");
}

#[test]
fn core_tcp_segment_rejects_short_header_and_bad_data_offset() {
    assert_eq!(
        TcpSegmentView::parse(&[0; 19]),
        Err(TcpSegmentParseError::ShortHeader)
    );

    let mut bytes = tcp_segment(&[], &[]);
    bytes[12] = 4 << 4;

    assert_eq!(
        TcpSegmentView::parse(&bytes),
        Err(TcpSegmentParseError::BadDataOffset)
    );
}

#[test]
fn core_tcp_options_parse_mss_window_scale_sack_timestamp_ecn() {
    let parsed = tcp_options_from_bytes(&[
        2, 4, 0x05, 0xb4, 1, 3, 3, 15, 4, 2, 8, 10, 0x01, 0x02, 0x03, 0x04, 0xa1, 0xa2, 0xa3, 0xa4,
        172, 2,
    ]);

    assert_eq!(parsed.capabilities.max_segment_size, Some(1_460));
    assert_eq!(parsed.capabilities.window_scale, Some(14));
    assert!(parsed.capabilities.sack);
    assert!(parsed.capabilities.timestamps);
    assert!(parsed.capabilities.ecn);
    assert_eq!(parsed.timestamp.expect("timestamp").tsval, 0x0102_0304);
}

fn tcp_segment(options: &[u8], payload: &[u8]) -> Vec<u8> {
    assert_eq!(options.len() % 4, 0);
    let header_len = 20 + options.len();
    let mut bytes = vec![0; header_len + payload.len()];
    bytes[0..2].copy_from_slice(&49_152u16.to_be_bytes());
    bytes[2..4].copy_from_slice(&443u16.to_be_bytes());
    bytes[4..8].copy_from_slice(&0x0102_0304u32.to_be_bytes());
    bytes[8..12].copy_from_slice(&0x1112_1314u32.to_be_bytes());
    bytes[12] = ((header_len / 4) as u8) << 4;
    bytes[13] = TcpSegmentFlags::SYN.bits() | TcpSegmentFlags::ACK.bits();
    bytes[14..16].copy_from_slice(&32_768u16.to_be_bytes());
    bytes[20..header_len].copy_from_slice(options);
    bytes[header_len..].copy_from_slice(payload);
    bytes
}
