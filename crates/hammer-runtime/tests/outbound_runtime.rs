use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Instant;

use hammer_adapter::{Network, OutboundManager as _, SocksAddr};
use hammer_core::config::{
    DirectOutboundOptions, Hysteria2OutboundOptions, Outbound, OutboundKind,
};
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::OutboundManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

fn logger(tag: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(tag)
}

fn destination(addr: std::net::SocketAddr) -> SocksAddr {
    SocksAddr {
        host: addr.ip(),
        port: addr.port(),
    }
}

#[tokio::test]
async fn direct_outbound_dials_tcp_with_initial_payload() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"ping");
        stream.write_all(b"echo:ping").await.unwrap();
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "direct",
        &[Outbound {
            tag: "direct".to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions::default()),
        }],
    );
    let outbound = manager.get("direct").expect("direct outbound");
    assert_eq!(outbound.type_name(), "direct");

    let mut stream = outbound
        .dial(Network::Tcp, destination(addr), b"ping")
        .await
        .expect("dial direct tcp");
    assert_eq!(stream.read_to_end().await.unwrap(), b"echo:ping");
}

#[tokio::test]
async fn direct_outbound_sends_and_receives_udp_datagrams() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0_u8; 64];
        let (len, peer) = socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], b"dns");
        socket.send_to(b"echo:dns", peer).await.unwrap();
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "direct",
        &[Outbound {
            tag: "direct".to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions::default()),
        }],
    );
    let outbound = manager.get("direct").expect("direct outbound");
    let mut packet = outbound.listen_packet().await.expect("listen direct udp");
    packet
        .send_to(destination(addr), b"dns")
        .await
        .expect("send direct udp");

    let got = packet.recv_from().await.expect("recv direct udp");
    assert_eq!(got.destination, destination(addr));
    assert_eq!(got.payload, b"echo:dns");
}

#[tokio::test]
async fn block_and_dns_outbounds_return_protocol_errors() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "block",
        &[
            Outbound {
                tag: "block".to_owned(),
                kind: OutboundKind::Block,
            },
            Outbound {
                tag: "dns-out".to_owned(),
                kind: OutboundKind::Dns,
            },
        ],
    );
    let destination = SocksAddr {
        host: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 9,
    };

    let block = manager.get("block").expect("block outbound");
    assert_eq!(block.type_name(), "block");
    let err = match block.dial(Network::Tcp, destination.clone(), b"").await {
        Ok(_) => panic!("block accepted tcp"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("blocked"));
    let err = match block.listen_packet().await {
        Ok(_) => panic!("block accepted udp"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("blocked"));

    let dns = manager.get("dns-out").expect("dns outbound");
    assert_eq!(dns.type_name(), "dns");
    let err = match dns.dial(Network::Tcp, destination, b"").await {
        Ok(_) => panic!("dns accepted dial"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("invalid"));
    let err = match dns.listen_packet().await {
        Ok(_) => panic!("dns accepted udp"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("invalid"));
}

#[test]
fn outbound_manager_registers_concrete_m7_outbounds() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "direct",
        &[
            Outbound {
                tag: "hysteria2".to_owned(),
                kind: OutboundKind::Hysteria2(Hysteria2OutboundOptions::default()),
            },
            Outbound {
                tag: "direct".to_owned(),
                kind: OutboundKind::Direct(DirectOutboundOptions::default()),
            },
            Outbound {
                tag: "block".to_owned(),
                kind: OutboundKind::Block,
            },
            Outbound {
                tag: "dns-out".to_owned(),
                kind: OutboundKind::Dns,
            },
        ],
    );

    assert_eq!(manager.get("hysteria2").unwrap().type_name(), "hysteria2");
    assert_eq!(manager.get("direct").unwrap().type_name(), "direct");
    assert_eq!(manager.get("block").unwrap().type_name(), "block");
    assert_eq!(manager.get("dns-out").unwrap().type_name(), "dns");
    assert_eq!(manager.default().unwrap().tag(), "direct");
}
