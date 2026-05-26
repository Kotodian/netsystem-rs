#![cfg(feature = "inbound-mixed")]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use hammer_adapter::{
    ComponentMeta, DefaultInterfaceUpdateListener, Network, NetworkInterface,
    Outbound as AdapterOutbound, OutboundComponent, PlatformInterface, ProxyDatagram,
    ProxyPacketConn, ProxyStream, RuntimeComponent, SocksAddr, TunOptions, WifiState,
};
use hammer_core::config::{self, Options};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::{
    DnsRouter, MetricsRegistry, OutboundManager, Router, inbounds::InboundManager,
    spawn::DataRuntime,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

fn proxy_options(kind: &str, port: u16) -> Options {
    config::parse_config(&format!(
        r#"
[[inbounds]]
type = "{kind}"
id = "{kind}-in"
listen = "127.0.0.1"
listen_port = {port}
udp_timeout = "1s"

[[inbounds.users]]
username = "alice"
password = "secret"

[[outbounds]]
type = "direct"
id = "direct"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "direct"
"#
    ))
    .expect("parse proxy config")
}

#[derive(Clone, Debug)]
struct DialRecord {
    network: Network,
    destination: SocksAddr,
    initial_payload: Vec<u8>,
}

#[derive(Default)]
struct RecordingOutbound {
    dials: Mutex<Vec<DialRecord>>,
    sent_packets: Arc<Mutex<Vec<ProxyDatagram>>>,
    received_packets: Arc<Mutex<Vec<ProxyDatagram>>>,
}

impl RecordingOutbound {
    fn component(outbound: Arc<Self>) -> OutboundComponent {
        let runtime: Arc<dyn AdapterOutbound> = outbound;
        RuntimeComponent::new(
            ComponentMeta::new(
                "outbound",
                "recording",
                "direct",
                vec![Network::Tcp, Network::Udp],
                Vec::new(),
                None,
            ),
            runtime,
        )
    }

    fn take_dial(&self) -> DialRecord {
        self.dials.lock().expect("dials poisoned").remove(0)
    }

    fn take_packet(&self) -> ProxyDatagram {
        self.sent_packets
            .lock()
            .expect("sent packets poisoned")
            .remove(0)
    }

    fn push_packet_response(&self, packet: ProxyDatagram) {
        self.received_packets
            .lock()
            .expect("received packets poisoned")
            .push(packet);
    }
}

#[tokio::test]
async fn socks5_inbound_proxies_udp_associate_with_password_auth() {
    let port = 18083;
    let outbound = Arc::new(RecordingOutbound::default());
    let _manager = start_proxy("socks", port, Arc::clone(&outbound)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    stream.write_all(&[0x05, 0x01, 0x02]).await.unwrap();
    let mut buf = [0; 2];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, [0x05, 0x02]);
    stream
        .write_all(&[
            0x01, 0x05, b'a', b'l', b'i', b'c', b'e', 0x06, b's', b'e', b'c', b'r', b'e', b't',
        ])
        .await
        .unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, [0x01, 0x00]);

    stream
        .write_all(&[0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut response = [0; 10];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(&response[..4], &[0x05, 0x00, 0x00, 0x01]);
    let relay_port = u16::from_be_bytes([response[8], response[9]]);

    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut packet = BytesMut::new();
    packet.extend_from_slice(&[0, 0, 0, 0x03, 11]);
    packet.extend_from_slice(b"example.com");
    packet.put_u16(53);
    packet.extend_from_slice(b"hello");
    udp.send_to(&packet, ("127.0.0.1", relay_port))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !outbound
                .sent_packets
                .lock()
                .expect("sent packets poisoned")
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let packet = outbound.take_packet();
    assert_eq!(packet.destination.domain.as_deref(), Some("example.com"));
    assert_eq!(packet.destination.port, 53);
    assert_eq!(packet.payload.as_ref(), b"hello");

    outbound.push_packet_response(ProxyDatagram {
        destination: SocksAddr::domain("example.com", "0.0.0.0".parse().unwrap(), 53),
        payload: Bytes::from_static(b"world"),
    });
    let mut response = vec![0; 128];
    let (n, _) = udp.recv_from(&mut response).await.unwrap();
    assert_eq!(&response[..4], &[0, 0, 0, 0x03]);
    assert_eq!(response[4], 11);
    assert_eq!(&response[5..16], b"example.com");
    assert_eq!(u16::from_be_bytes([response[16], response[17]]), 53);
    assert_eq!(&response[18..n], b"world");
}

#[async_trait]
impl AdapterOutbound for RecordingOutbound {
    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        self.dials.lock().expect("dials poisoned").push(DialRecord {
            network,
            destination,
            initial_payload: initial_payload.to_vec(),
        });
        let (client, mut remote) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let _ = remote.write_all(b"proxied").await;
        });
        Ok(Box::new(client))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        Ok(Box::new(RecordingPacketConn {
            received: Arc::clone(&self.received_packets),
            sent: Arc::clone(&self.sent_packets),
        }))
    }
}

struct RecordingPacketConn {
    received: Arc<Mutex<Vec<ProxyDatagram>>>,
    sent: Arc<Mutex<Vec<ProxyDatagram>>>,
}

#[async_trait]
impl ProxyPacketConn for RecordingPacketConn {
    async fn send_to(&mut self, destination: SocksAddr, payload: Bytes) -> Result<(), HammerError> {
        self.sent
            .lock()
            .expect("sent packets poisoned")
            .push(ProxyDatagram {
                destination,
                payload,
            });
        Ok(())
    }

    async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        loop {
            if let Some(packet) = self
                .received
                .lock()
                .expect("received packets poisoned")
                .pop()
            {
                return Ok(packet);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

struct TestPlatform;

impl PlatformInterface for TestPlatform {
    fn open_tun(&self, _options: TunOptions) -> Result<i32, HammerError> {
        Ok(-1)
    }

    fn use_platform_auto_detect_interface_control(&self) -> bool {
        false
    }

    fn auto_detect_interface_control(&self, _fd: i32) -> Result<(), HammerError> {
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

struct ProxyHarness {
    manager: InboundManager,
    data_runtime: Option<DataRuntime>,
}

impl Drop for ProxyHarness {
    fn drop(&mut self) {
        let _ = self.manager.close();
        if let Some(data_runtime) = self.data_runtime.take() {
            data_runtime.shutdown_timeout(Duration::from_secs(1));
        }
    }
}

async fn start_proxy(kind: &str, port: u16, outbound: Arc<RecordingOutbound>) -> ProxyHarness {
    let options = proxy_options(kind, port);
    let outbound_manager = OutboundManager::new(logger("outbound"), "direct");
    outbound_manager
        .register_outbound(RecordingOutbound::component(outbound))
        .expect("register outbound");
    let outbound_manager = Arc::new(outbound_manager);
    let router = Arc::new(
        Router::from_options_with_metrics(
            logger("router"),
            options.route.clone(),
            Arc::clone(&outbound_manager),
            MetricsRegistry::new(),
        )
        .expect("router"),
    );
    let dns_router = Arc::new(DnsRouter::new(logger("dns")));
    let manager = InboundManager::from_options_with_runtime_and_metrics(
        logger("inbound"),
        &options.inbounds,
        router,
        dns_router,
        outbound_manager,
        Arc::new(TestPlatform),
        MetricsRegistry::new(),
    )
    .expect("inbound manager");
    let data_runtime = DataRuntime::new(2, "proxy-test-data", 512 * 1024, 2).expect("data runtime");
    let data_context = data_runtime.context();
    data_context
        .enter(|| manager.start(StartStage::Start))
        .expect("start inbounds");
    ProxyHarness {
        manager,
        data_runtime: Some(data_runtime),
    }
}

#[tokio::test]
async fn http_inbound_proxies_connect_with_basic_auth() {
    let port = 18080;
    let outbound = Arc::new(RecordingOutbound::default());
    let _manager = start_proxy("http", port, Arc::clone(&outbound)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    stream
        .write_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: Basic YWxpY2U6c2VjcmV0\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = vec![0; 128];
    let n = stream.read(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response[..n]).starts_with("HTTP/1.1 200"));

    let dial = outbound.take_dial();
    assert_eq!(dial.network, Network::Tcp);
    assert_eq!(dial.destination.domain.as_deref(), Some("example.com"));
    assert_eq!(dial.destination.port, 443);
    assert!(dial.initial_payload.is_empty());
}

#[tokio::test]
async fn http_inbound_proxies_absolute_url_requests() {
    let port = 18084;
    let outbound = Arc::new(RecordingOutbound::default());
    let _manager = start_proxy("http", port, Arc::clone(&outbound)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    stream
        .write_all(
            b"GET http://example.com/path?q=1 HTTP/1.1\r\nHost: example.com\r\nProxy-Authorization: Basic YWxpY2U6c2VjcmV0\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = vec![0; 16];
    let n = stream.read(&mut response).await.unwrap();
    assert_eq!(&response[..n], b"proxied");

    let dial = outbound.take_dial();
    assert_eq!(dial.destination.domain.as_deref(), Some("example.com"));
    assert_eq!(dial.destination.port, 80);
    let request = String::from_utf8(dial.initial_payload).unwrap();
    assert!(request.starts_with("GET /path?q=1 HTTP/1.1\r\n"));
    assert!(!request.to_ascii_lowercase().contains("proxy-authorization"));
}

#[tokio::test]
async fn socks5_inbound_proxies_tcp_connect_with_password_auth() {
    let port = 18081;
    let outbound = Arc::new(RecordingOutbound::default());
    let _manager = start_proxy("socks", port, Arc::clone(&outbound)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    stream.write_all(&[0x05, 0x01, 0x02]).await.unwrap();
    let mut buf = [0; 2];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, [0x05, 0x02]);

    stream
        .write_all(&[
            0x01, 0x05, b'a', b'l', b'i', b'c', b'e', 0x06, b's', b'e', b'c', b'r', b'e', b't',
        ])
        .await
        .unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, [0x01, 0x00]);

    let mut request = BytesMut::new();
    request.extend_from_slice(&[0x05, 0x01, 0x00, 0x03, 11]);
    request.extend_from_slice(b"example.com");
    request.put_u16(443);
    stream.write_all(&request).await.unwrap();
    let mut response = [0; 10];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(&response[..4], &[0x05, 0x00, 0x00, 0x01]);

    let dial = outbound.take_dial();
    assert_eq!(dial.network, Network::Tcp);
    assert_eq!(dial.destination.domain.as_deref(), Some("example.com"));
    assert_eq!(dial.destination.port, 443);
}

#[tokio::test]
async fn socks4a_inbound_proxies_tcp_connect_with_userid_auth() {
    let port = 18085;
    let outbound = Arc::new(RecordingOutbound::default());
    let _manager = start_proxy("socks", port, Arc::clone(&outbound)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    let mut request = BytesMut::new();
    request.extend_from_slice(&[0x04, 0x01]);
    request.put_u16(443);
    request.extend_from_slice(&[0, 0, 0, 1]);
    request.extend_from_slice(b"alice\0example.com\0");
    stream.write_all(&request).await.unwrap();
    let mut response = [0; 8];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(&response[..2], &[0x00, 0x5a]);

    let dial = outbound.take_dial();
    assert_eq!(dial.destination.domain.as_deref(), Some("example.com"));
    assert_eq!(dial.destination.port, 443);
}

#[tokio::test]
async fn mixed_inbound_accepts_http_and_socks_on_one_port() {
    let port = 18082;
    let outbound = Arc::new(RecordingOutbound::default());
    let _manager = start_proxy("mixed", port, Arc::clone(&outbound)).await;

    let mut http = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    http.write_all(
        b"CONNECT one.example:443 HTTP/1.1\r\nHost: one.example:443\r\nProxy-Authorization: Basic YWxpY2U6c2VjcmV0\r\n\r\n",
    )
    .await
    .unwrap();
    let mut response = vec![0; 128];
    let n = http.read(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response[..n]).starts_with("HTTP/1.1 200"));

    let mut socks = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    socks.write_all(&[0x05, 0x01, 0x02]).await.unwrap();
    let mut buf = [0; 2];
    socks.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, [0x05, 0x02]);
    socks
        .write_all(&[
            0x01, 0x05, b'a', b'l', b'i', b'c', b'e', 0x06, b's', b'e', b'c', b'r', b'e', b't',
        ])
        .await
        .unwrap();
    socks.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, [0x01, 0x00]);
    let mut request = BytesMut::new();
    request.extend_from_slice(&[0x05, 0x01, 0x00, 0x03, 11]);
    request.extend_from_slice(b"two.example");
    request.put_u16(443);
    socks.write_all(&request).await.unwrap();
    let mut response = [0; 10];
    socks.read_exact(&mut response).await.unwrap();
    assert_eq!(&response[..4], &[0x05, 0x00, 0x00, 0x01]);

    let first = outbound.take_dial();
    assert_eq!(first.destination.domain.as_deref(), Some("one.example"));
    let second = outbound.take_dial();
    assert_eq!(second.destination.domain.as_deref(), Some("two.example"));
}
