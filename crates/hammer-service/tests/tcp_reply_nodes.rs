use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use hammer_core::protocol::tcp::{TcpCapabilities, TcpError, TcpSegmentFlags, TcpSegmentHeader};
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};

fn v4(a: u8, b: u8, c: u8, d: u8, port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), port)
}

#[test]
fn synthesize_control_ack_has_reversed_tuple_and_fields() {
    let local = v4(192, 0, 2, 10, 443);
    let remote = v4(10, 0, 0, 1, 50_001);
    let packet = tcp_control_packet(
        local,
        remote,
        TcpSegmentHeader {
            source_port: local.port(),
            destination_port: remote.port(),
            sequence_number: 1001,
            acknowledgment_number: 7001,
            flags: TcpSegmentFlags::ACK,
            advertised_window: 65_535,
            urgent_pointer: 0,
            capabilities: TcpCapabilities::default(),
            timestamp: None,
            fast_open_cookie: None,
        },
        &[],
    )
    .expect("synthesize control ack");

    assert_eq!(&packet[12..16], &Ipv4Addr::new(192, 0, 2, 10).octets());
    assert_eq!(&packet[16..20], &Ipv4Addr::new(10, 0, 0, 1).octets());
    assert_eq!(u16::from_be_bytes([packet[20], packet[21]]), 443);
    assert_eq!(u16::from_be_bytes([packet[22], packet[23]]), 50_001);
    assert_eq!(
        u32::from_be_bytes([packet[24], packet[25], packet[26], packet[27]]),
        1001
    );
    assert_eq!(
        u32::from_be_bytes([packet[28], packet[29], packet[30], packet[31]]),
        7001
    );
    assert_eq!(
        packet[33] & (TcpSegmentFlags::ACK.bits() as u8),
        TcpSegmentFlags::ACK.bits() as u8
    );
    assert_eq!(u16::from_be_bytes([packet[34], packet[35]]), 65_535);
    assert_ne!(u16::from_be_bytes([packet[36], packet[37]]), 0);
}

fn tcp_control_packet(
    local: SocketAddr,
    remote: SocketAddr,
    header: TcpSegmentHeader<'_>,
    payload: &[u8],
) -> Result<std::vec::Vec<u8>, TcpError> {
    let mut tcp = [0u8; 60];
    let tcp_header_len = header.write_to_buffer(&mut tcp, None)?;
    let tcp_len = tcp_header_len
        .checked_add(payload.len())
        .ok_or(TcpError::Dispatch)?;
    match (local.ip(), remote.ip()) {
        (IpAddr::V4(local_ip), IpAddr::V4(remote_ip)) => {
            let packet_len = 20usize.checked_add(tcp_len).ok_or(TcpError::Dispatch)?;
            let total_len = u16::try_from(packet_len).map_err(|_| TcpError::Length)?;
            let mut packet = std::vec![0u8; packet_len];
            packet[0] = 0x45;
            packet[2..4].copy_from_slice(&total_len.to_be_bytes());
            packet[8] = 64;
            packet[9] = 6;
            packet[12..16].copy_from_slice(&local_ip.octets());
            packet[16..20].copy_from_slice(&remote_ip.octets());
            packet[20..20 + tcp_header_len].copy_from_slice(&tcp[..tcp_header_len]);
            if !payload.is_empty() {
                packet[20 + tcp_header_len..20 + tcp_header_len + payload.len()]
                    .copy_from_slice(payload);
            }
            let checksum = internet_checksum_parts(&[
                &local_ip.octets(),
                &remote_ip.octets(),
                &[0, 6],
                &(packet_len as u16 - 20).to_be_bytes(),
                &packet[20..],
            ]);
            packet[36..38].copy_from_slice(&checksum.to_be_bytes());
            let checksum = internet_checksum(&packet[..20]);
            packet[10..12].copy_from_slice(&checksum.to_be_bytes());
            Ok(packet)
        }
        (IpAddr::V6(local_ip), IpAddr::V6(remote_ip)) => {
            let packet_len = 40usize.checked_add(tcp_len).ok_or(TcpError::Dispatch)?;
            let payload_len = u16::try_from(tcp_len).map_err(|_| TcpError::Length)?;
            let mut packet = std::vec![0u8; packet_len];
            packet[0] = 0x60;
            packet[4..6].copy_from_slice(&payload_len.to_be_bytes());
            packet[6] = 6;
            packet[7] = 64;
            packet[8..24].copy_from_slice(&local_ip.octets());
            packet[24..40].copy_from_slice(&remote_ip.octets());
            packet[40..40 + tcp_header_len].copy_from_slice(&tcp[..tcp_header_len]);
            if !payload.is_empty() {
                packet[40 + tcp_header_len..40 + tcp_header_len + payload.len()]
                    .copy_from_slice(payload);
            }
            let checksum = internet_checksum_parts(&[
                &local_ip.octets(),
                &remote_ip.octets(),
                &(tcp_len as u32).to_be_bytes(),
                &[0, 0, 0, 6],
                &packet[40..],
            ]);
            packet[56..58].copy_from_slice(&checksum.to_be_bytes());
            Ok(packet)
        }
        _ => Err(TcpError::SegmentInvalid),
    }
}
