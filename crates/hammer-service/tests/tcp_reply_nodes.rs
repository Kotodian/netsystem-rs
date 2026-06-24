use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use hammer_core::protocol::tcp::TcpControlFlags;
use hammer_service::transport::tcp::synthesize_ipv4_tcp_control;

const ACK: u8 = 0x10;

fn v4(a: u8, b: u8, c: u8, d: u8, port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), port)
}

#[test]
fn synthesize_control_ack_has_reversed_tuple_and_fields() {
    let local = v4(192, 0, 2, 10, 443);
    let remote = v4(10, 0, 0, 1, 50_001);
    let packet = synthesize_ipv4_tcp_control(
        local,
        remote,
        1001,
        7001,
        65_535,
        TcpControlFlags::from_bits(ACK),
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
    assert_eq!(packet[33] & ACK, ACK);
    assert_eq!(u16::from_be_bytes([packet[34], packet[35]]), 65_535);
    assert_ne!(u16::from_be_bytes([packet[36], packet[37]]), 0);
}
