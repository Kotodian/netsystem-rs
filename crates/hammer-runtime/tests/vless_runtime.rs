#![cfg(feature = "outbound-vless")]

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Instant;

use hammer_adapter::{Network, OutboundManager as _, SocksAddr};
use hammer_core::config::{Outbound, OutboundKind, OutboundTlsOptions, VlessOutboundOptions};
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::OutboundManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

fn vless_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        server: "127.0.0.1".to_owned(),
        server_port: port,
        uuid: [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ],
        flow: None,
        tls: OutboundTlsOptions::default(),
    }
}

#[test]
fn outbound_manager_registers_vless_outbound() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_options(443)),
        }],
    )
    .expect("outbound manager");

    let outbound = manager.get("vl").expect("vless outbound");
    assert_eq!(outbound.type_name(), "vless");
    assert_eq!(manager.default().unwrap().id(), "vl");
}

#[tokio::test]
async fn vless_outbound_dials_tcp_and_strips_response_header() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let expected_request_len = 1 + 16 + 1 + 1 + 2 + 1 + 1 + "example.com".len() + "hello".len();
    let captured = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![0_u8; expected_request_len];
        stream.read_exact(&mut request).await.unwrap();
        stream.write_all(&[0x00, 0x00]).await.unwrap();
        stream.write_all(b"reply").await.unwrap();
        request
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_options(server_addr.port())),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::domain(
        "example.com",
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
        443,
    );

    let mut stream = outbound
        .dial(Network::Tcp, destination, b"hello")
        .await
        .expect("dial vless tcp");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    assert_eq!(response, b"reply");

    let request = captured.await.unwrap();
    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&[
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]);
    expected.push(0x00);
    expected.push(0x01);
    expected.extend_from_slice(&443_u16.to_be_bytes());
    expected.push(0x02);
    expected.push("example.com".len() as u8);
    expected.extend_from_slice(b"example.com");
    expected.extend_from_slice(b"hello");
    assert_eq!(request, expected);
}
