use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use hammer_adapter::{
    DefaultInterfaceUpdateListener, Network, NetworkInterface, OutboundManager as _,
    PlatformInterface, SocksAddr, TunOptions, WifiState,
};
use hammer_core::config::{
    DirectOutboundOptions, Hysteria2OutboundOptions, Outbound, OutboundKind,
};
use hammer_core::error::HammerError;
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::{OutboundManager, outbounds::BlockOutbound};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

fn destination(addr: std::net::SocketAddr) -> SocksAddr {
    SocksAddr {
        host: addr.ip(),
        port: addr.port(),
    }
}

#[derive(Default)]
struct ProtectPlatform {
    calls: AtomicUsize,
}

impl ProtectPlatform {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl PlatformInterface for ProtectPlatform {
    fn open_tun(&self, _options: TunOptions) -> Result<i32, HammerError> {
        Ok(42)
    }

    fn use_platform_auto_detect_interface_control(&self) -> bool {
        true
    }

    fn auto_detect_interface_control(&self, _fd: i32) -> Result<(), HammerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn start_default_interface_monitor(
        &self,
        _listener: Arc<dyn DefaultInterfaceUpdateListener>,
    ) -> Result<(), HammerError> {
        Ok(())
    }

    fn close_default_interface_monitor(
        &self,
        _listener: Arc<dyn DefaultInterfaceUpdateListener>,
    ) -> Result<(), HammerError> {
        Ok(())
    }

    fn get_interfaces(&self) -> Result<Vec<NetworkInterface>, HammerError> {
        Ok(Vec::new())
    }

    fn read_wifi_state(&self) -> Option<WifiState> {
        None
    }
}

#[tokio::test]
async fn direct_outbound_dials_tcp_with_initial_payload() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
        stream.write_all(b"echo:ping").await.unwrap();
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "direct",
        &[Outbound {
            id: "direct".to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions::default()),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("direct").expect("direct outbound");
    assert_eq!(outbound.type_name(), "direct");

    let mut stream = outbound
        .dial(Network::Tcp, destination(addr), b"ping")
        .await
        .expect("dial direct tcp");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"echo:ping");
}

#[tokio::test]
async fn direct_outbound_keeps_tcp_stream_full_duplex_after_dial() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut first = [0_u8; 5];
        stream.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"hello");
        stream.write_all(b"ack").await.unwrap();

        let mut second = [0_u8; 5];
        stream.read_exact(&mut second).await.unwrap();
        assert_eq!(&second, b"again");
        stream.write_all(b"done").await.unwrap();
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "direct",
        &[Outbound {
            id: "direct".to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions::default()),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("direct").expect("direct outbound");
    let mut stream = outbound
        .dial(Network::Tcp, destination(addr), b"")
        .await
        .expect("dial direct tcp");

    stream.write_all(b"hello").await.unwrap();
    let mut ack = [0_u8; 3];
    stream.read_exact(&mut ack).await.unwrap();
    assert_eq!(&ack, b"ack");

    stream.write_all(b"again").await.unwrap();
    let mut done = Vec::new();
    stream.read_to_end(&mut done).await.unwrap();
    assert_eq!(done, b"done");
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
            id: "direct".to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions::default()),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("direct").expect("direct outbound");
    let mut packet = outbound.listen_packet().await.expect("listen direct udp");
    packet
        .send_to(destination(addr), b"dns")
        .await
        .expect("send direct udp");

    let got = packet.recv_from().await.expect("recv direct udp");
    assert_eq!(got.destination, destination(addr));
    assert_eq!(got.payload.as_ref(), b"echo:dns");
}

#[tokio::test]
async fn direct_outbound_sends_and_receives_ipv6_udp_datagrams() {
    let socket = UdpSocket::bind("[::1]:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0_u8; 16];
        let (len, peer) = socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], b"dns6");
        socket.send_to(b"echo:dns6", peer).await.unwrap();
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "direct",
        &[Outbound {
            id: "direct".to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions::default()),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("direct").expect("direct outbound");
    let mut packet = outbound.listen_packet().await.expect("listen direct udp");
    packet
        .send_to(destination(addr), b"dns6")
        .await
        .expect("send direct ipv6 udp");

    let got = packet.recv_from().await.expect("recv direct ipv6 udp");
    assert_eq!(got.destination, destination(addr));
    assert_eq!(got.payload.as_ref(), b"echo:dns6");
}

#[tokio::test]
async fn direct_outbound_protects_tcp_and_udp_sockets() {
    let platform = Arc::new(ProtectPlatform::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.write_all(b"ok").await.unwrap();
    });
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_addr = udp.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0_u8; 8];
        let (len, peer) = udp.recv_from(&mut buf).await.unwrap();
        udp.send_to(&buf[..len], peer).await.unwrap();
    });

    let manager = OutboundManager::from_options_with_platform(
        logger("outbound"),
        "direct",
        &[Outbound {
            id: "direct".to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions::default()),
        }],
        Arc::clone(&platform) as Arc<dyn PlatformInterface>,
    )
    .expect("outbound manager");
    let outbound = manager.get("direct").expect("direct outbound");
    let mut stream = outbound
        .dial(Network::Tcp, destination(tcp_addr), b"")
        .await
        .expect("dial protected tcp");
    let mut tcp_payload = Vec::new();
    stream.read_to_end(&mut tcp_payload).await.unwrap();
    assert_eq!(tcp_payload, b"ok");

    let mut packet = outbound
        .listen_packet()
        .await
        .expect("listen protected udp");
    packet
        .send_to(destination(udp_addr), b"x")
        .await
        .expect("send protected udp");
    assert_eq!(packet.recv_from().await.unwrap().payload.as_ref(), b"x");
    assert_eq!(platform.calls(), 2);
}

#[tokio::test]
async fn block_outbound_returns_protocol_errors() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "block",
        &[Outbound {
            id: "block".to_owned(),
            kind: OutboundKind::Block,
        }],
    )
    .expect("outbound manager");
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
}

#[test]
fn outbound_manager_registers_concrete_m7_outbounds() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "direct",
        &[
            Outbound {
                id: "hysteria2".to_owned(),
                kind: OutboundKind::Hysteria2(Hysteria2OutboundOptions::default()),
            },
            Outbound {
                id: "direct".to_owned(),
                kind: OutboundKind::Direct(DirectOutboundOptions::default()),
            },
            Outbound {
                id: "block".to_owned(),
                kind: OutboundKind::Block,
            },
        ],
    )
    .expect("outbound manager");

    assert_eq!(manager.get("hysteria2").unwrap().type_name(), "hysteria2");
    assert_eq!(manager.get("direct").unwrap().type_name(), "direct");
    assert_eq!(manager.get("block").unwrap().type_name(), "block");
    assert_eq!(manager.default().unwrap().id(), "direct");
}

#[test]
fn outbound_manager_rejects_duplicate_registered_endpoint_view_id() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "direct",
        &[Outbound {
            id: "direct".to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions::default()),
        }],
    )
    .expect("outbound manager");
    let duplicate = Arc::new(BlockOutbound::new(logger("block"), "direct"));

    let err = manager
        .register_outbound("direct".to_owned(), duplicate)
        .expect_err("duplicate endpoint outbound id must be rejected");
    assert!(
        err.to_string().contains("duplicate outbound id: direct"),
        "error = {err:?}"
    );
    assert_eq!(manager.get("direct").unwrap().type_name(), "direct");
}
