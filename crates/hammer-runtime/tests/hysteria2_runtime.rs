use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

mod support;

use bytes::Bytes;
use hammer_adapter::{
    DefaultInterfaceUpdateListener, Network, NetworkInterface, OutboundManager as _,
    PlatformInterface, SocksAddr, TunOptions, WifiState,
};
use hammer_core::config::{self, Options, OutboundTlsOptions};
use hammer_core::error::HammerError;
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_core::protocol::congestion::BbrProfile;
use hammer_runtime::OutboundManager;
use hammer_runtime::congestion::{
    CongestionControlHandle, DynamicCongestionController, HysteriaBbrConfig,
};
use hammer_runtime::hysteria2::{ClientOptions, Hysteria2Client, obfs::Salamander, protocol};
use support::hysteria2_echo::EchoServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
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

fn options(server: String, port: u16) -> Options {
    config::parse_config(&format!(
        r#"
[tun]
address = ["172.19.0.1/30"]

[hysteria2]
server = "{server}"
server_port = {port}
password = "secret"
sni = "localhost"
insecure = true
network = ["tcp", "udp"]

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "hysteria2"
"#
    ))
    .expect("parse config")
}

#[test]
fn protocol_encodes_tcp_and_udp_wire_frames() {
    let tcp = protocol::encode_tcp_request("example.com:443", b"hello");
    let decoded = protocol::decode_tcp_request(tcp).expect("decode tcp request");
    assert_eq!(decoded.destination, "example.com:443");
    assert_eq!(decoded.payload.as_ref(), b"hello");

    let response = protocol::encode_tcp_response(true, "", b"world");
    let decoded = protocol::decode_tcp_response(response).expect("decode tcp response");
    assert!(decoded.ok);
    assert_eq!(decoded.message, "");
    assert_eq!(decoded.payload.as_ref(), b"world");

    let udp = protocol::UdpMessage {
        session_id: 7,
        packet_id: 9,
        fragment_id: 0,
        fragment_total: 1,
        destination: "1.1.1.1:53".to_owned(),
        payload: Bytes::from_static(b"dns"),
    };
    let decoded = protocol::UdpMessage::decode(udp.encode()).expect("decode udp message");
    assert_eq!(decoded, udp);
}

#[test]
fn protocol_udp_decode_keeps_payload_as_bytes_slice() {
    let udp = protocol::UdpMessage {
        session_id: 7,
        packet_id: 9,
        fragment_id: 0,
        fragment_total: 1,
        destination: "1.1.1.1:53".to_owned(),
        payload: Bytes::from_static(b"dns"),
    };
    let encoded = udp.encode();
    let decoded = protocol::UdpMessage::decode(encoded.clone()).expect("decode udp message");
    assert_eq!(decoded.payload, Bytes::from_static(b"dns"));
    assert_eq!(
        decoded.payload.as_ptr(),
        &encoded[encoded.len() - 3] as *const u8
    );
}

#[test]
fn salamander_obfs_round_trips_packet_payloads() {
    let salamander = Salamander::new(b"secret".to_vec());
    let sealed = salamander.seal(b"quic-packet");

    assert_ne!(sealed, b"quic-packet");
    assert_eq!(sealed.len(), b"quic-packet".len() + 8);
    assert_eq!(
        salamander.open(&sealed).expect("open salamander packet"),
        b"quic-packet"
    );
}

#[tokio::test]
async fn hysteria2_client_authenticates_and_proxies_tcp_and_udp() {
    let server = EchoServer::start("secret")
        .await
        .expect("start echo server");
    let client = Hysteria2Client::connect(ClientOptions {
        server: "127.0.0.1".to_owned(),
        server_port: server.port(),
        password: "secret".to_owned(),
        server_name: "localhost".to_owned(),
        insecure: true,
        udp_enabled: true,
        bbr_profile: BbrProfile::Standard,
        disable_path_mtu_discovery: false,
        initial_packet_size: 1200,
        idle_timeout: None,
        keep_alive_period: None,
        send_bps: 0,
        receive_bps: 0,
        brutal_debug: false,
        tls: OutboundTlsOptions {
            enabled: true,
            server_name: "localhost".to_owned(),
            insecure: true,
            ..Default::default()
        },
        obfs: None,
        platform: None,
    })
    .await
    .expect("connect client");

    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 443);

    let mut stream = client
        .dial_tcp(destination.clone(), b"ping")
        .await
        .expect("dial tcp");
    stream.shutdown().await.expect("finish tcp request");
    assert_eq!(stream.read_to_end().await.expect("read tcp"), b"echo:ping");

    let mut packet = client.listen_udp().await.expect("listen udp");
    packet
        .send_to(destination, Bytes::from_static(b"dns-query"))
        .await
        .expect("send udp");
    let received = packet.recv_from().await.expect("recv udp");
    assert_eq!(received.payload.as_ref(), b"echo:dns-query");
}

#[tokio::test]
async fn hysteria2_client_protects_quic_socket_before_connecting() {
    let server = EchoServer::start("secret")
        .await
        .expect("start echo server");
    let platform = Arc::new(ProtectPlatform::default());
    let _client = Hysteria2Client::connect(ClientOptions {
        server: "127.0.0.1".to_owned(),
        server_port: server.port(),
        password: "secret".to_owned(),
        server_name: "localhost".to_owned(),
        insecure: true,
        udp_enabled: true,
        bbr_profile: BbrProfile::Standard,
        disable_path_mtu_discovery: false,
        initial_packet_size: 1200,
        idle_timeout: None,
        keep_alive_period: None,
        send_bps: 0,
        receive_bps: 0,
        brutal_debug: false,
        tls: OutboundTlsOptions {
            enabled: true,
            server_name: "localhost".to_owned(),
            insecure: true,
            ..Default::default()
        },
        obfs: None,
        platform: Some(Arc::clone(&platform) as Arc<dyn PlatformInterface>),
    })
    .await
    .expect("connect client");

    assert_eq!(platform.calls(), 1);
}

#[tokio::test]
async fn outbound_manager_registers_real_hysteria2_outbound() {
    let server = EchoServer::start("secret")
        .await
        .expect("start echo server");
    let options = options("127.0.0.1".to_owned(), server.port());
    let manager = OutboundManager::from_options(
        logger("outbound"),
        options.route.final_.clone(),
        &options.outbounds,
    )
    .expect("outbound manager");

    let outbound = manager.get("hysteria2").expect("hysteria2 outbound");
    assert_eq!(outbound.type_name(), "hysteria2");
    assert_eq!(outbound.networks(), &[Network::Tcp, Network::Udp]);

    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)), 80);
    let mut stream = outbound
        .dial(Network::Tcp, destination, b"hello")
        .await
        .expect("dial through outbound");
    stream.shutdown().await.expect("finish outbound request");
    let mut payload = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut payload)
        .await
        .expect("read outbound");
    assert_eq!(payload, b"echo:hello");
}

#[tokio::test]
async fn outbound_reset_during_initial_connect_discards_stale_client() {
    let server = EchoServer::start_with_auth_delay("secret", Duration::from_millis(200))
        .await
        .expect("start echo server");
    let options = options("127.0.0.1".to_owned(), server.port());
    let manager = OutboundManager::from_options(
        logger("outbound"),
        options.route.final_.clone(),
        &options.outbounds,
    )
    .expect("outbound manager");

    let outbound = manager.get("hysteria2").expect("hysteria2 outbound");
    let dial_outbound = outbound.clone();
    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 11)), 80);
    let dial = tokio::spawn(async move {
        dial_outbound
            .dial(Network::Tcp, destination, b"after-reset")
            .await
    });

    server.wait_for_auth_count(1).await;
    outbound.reset();

    let mut stream = tokio::time::timeout(Duration::from_secs(5), dial)
        .await
        .expect("dial should complete")
        .expect("dial task")
        .expect("dial through outbound");
    stream.shutdown().await.expect("finish outbound request");
    let mut payload = Vec::new();
    stream
        .read_to_end(&mut payload)
        .await
        .expect("read outbound");

    assert_eq!(payload, b"echo:after-reset");
    assert_eq!(
        server.auth_count(),
        2,
        "client created before reset must be discarded and reconnected"
    );
}

#[test]
fn hysteria2_bbr_factory_builds_hysteria_controller() {
    let factory = Arc::new(HysteriaBbrConfig::new(BbrProfile::Aggressive, 1200));
    let controller = quinn::congestion::ControllerFactory::build(factory, Instant::now(), 1200);
    // Without a CongestionControlHandle we hand the connection straight to
    // quinn's stock BBR controller — never the dynamic wrapper that would
    // otherwise fall through to the brutal CC path.
    assert!(!controller.into_any().is::<DynamicCongestionController>());
}

#[test]
fn hysteria2_congestion_handle_switches_to_brutal_after_auth() {
    let handle = CongestionControlHandle::default();
    let factory = Arc::new(HysteriaBbrConfig::new_with_handle(
        BbrProfile::Standard,
        1200,
        handle.clone(),
        false,
    ));
    let before = quinn::congestion::ControllerFactory::build(factory.clone(), Instant::now(), 1200);
    assert!(before.into_any().is::<DynamicCongestionController>());

    handle.use_brutal(2_000_000);
    let after = quinn::congestion::ControllerFactory::build(factory, Instant::now(), 1200);
    assert_eq!(after.metrics().pacing_rate, Some(16_000_000));
}
