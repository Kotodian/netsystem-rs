#![cfg(feature = "vless")]

use std::net::{IpAddr, Ipv4Addr};

use hammer_core::SocksAddr;
use hammer_core::protocol::vless::{VlessCommand, VlessStream, encode_request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn uuid() -> [u8; 16] {
    [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]
}

#[test]
fn encodes_tcp_request_header_and_initial_payload() {
    let destination = SocksAddr::domain(
        "example.com",
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
        443,
    );

    let request =
        encode_request(&uuid(), VlessCommand::Tcp, &destination, b"hello").expect("request");

    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&uuid());
    expected.push(0x00);
    expected.push(0x01);
    expected.extend_from_slice(&443_u16.to_be_bytes());
    expected.push(0x02);
    expected.push("example.com".len() as u8);
    expected.extend_from_slice(b"example.com");
    expected.extend_from_slice(b"hello");
    assert_eq!(request, expected);
}

#[tokio::test]
async fn stream_strips_response_header_before_payload() {
    let (mut server, client) = tokio::io::duplex(64);
    tokio::spawn(async move {
        server.write_all(&[0x00, 0x00]).await.unwrap();
        server.write_all(b"reply").await.unwrap();
    });

    let mut stream = VlessStream::new(client);
    let mut payload = Vec::new();
    stream.read_to_end(&mut payload).await.unwrap();

    assert_eq!(payload, b"reply");
}
