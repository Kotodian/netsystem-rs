use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpSackBlock, TcpSegmentFlags, TcpSegmentHeader, TcpSegmentParseError,
    TcpSegmentView, tcp_options_from_bytes, write_tcp_segment_header,
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

#[test]
fn core_tcp_write_ack_with_sack_blocks() {
    let mut output = [0u8; 64];

    let written = write_ack_for_test(
        &mut output,
        &[TcpSackBlock {
            left_edge: 30,
            right_edge: 40,
        }],
    )
    .expect("write ack with sack");
    let parsed = tcp_options_from_bytes(&output[20..written]);

    assert_eq!(
        parsed.sack_blocks,
        vec![TcpSackBlock {
            left_edge: 30,
            right_edge: 40,
        }]
    );
}

#[test]
fn core_tcp_non_ack_does_not_write_sack_blocks() {
    let mut output = [0u8; 64];

    let written = write_header_for_test(
        &mut output,
        TcpSegmentFlags::PSH,
        &[TcpSackBlock {
            left_edge: 30,
            right_edge: 40,
        }],
    )
    .expect("write non-ack without sack option");
    let parsed = tcp_options_from_bytes(&output[20..written]);

    assert!(parsed.sack_blocks.is_empty());
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

fn write_ack_for_test(output: &mut [u8], sack_blocks: &[TcpSackBlock]) -> Result<usize, hammer_core::error::CoreError> {
    write_header_for_test(output, TcpSegmentFlags::ACK, sack_blocks)
}

fn write_header_for_test(
    output: &mut [u8],
    flags: TcpSegmentFlags,
    sack_blocks: &[TcpSackBlock],
) -> Result<usize, hammer_core::error::CoreError> {
    write_tcp_segment_header(
        output,
        TcpSegmentHeader {
            source_port: 49_152,
            destination_port: 443,
            sequence_number: 0x0102_0304,
            acknowledgment_number: 0x1112_1314,
            flags,
            advertised_window: 32_768,
            capabilities: TcpCapabilities::default(),
        },
        Some(sack_blocks),
    )
}
