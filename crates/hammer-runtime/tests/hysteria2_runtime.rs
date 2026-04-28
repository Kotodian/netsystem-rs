use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Instant;

use hammer_adapter::{Network, OutboundManager as _, SocksAddr};
use hammer_core::config::{self, Options};
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::OutboundManager;
use hammer_runtime::hysteria2::bbr::{BbrProfile, HysteriaBbrConfig};
use hammer_runtime::hysteria2::{
    ClientOptions, Hysteria2Client, obfs::Salamander, protocol, testing::EchoServer,
};

fn logger(tag: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(tag)
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
    let decoded = protocol::decode_tcp_request(&tcp).expect("decode tcp request");
    assert_eq!(decoded.destination, "example.com:443");
    assert_eq!(decoded.payload, b"hello");

    let response = protocol::encode_tcp_response(true, "", b"world");
    let decoded = protocol::decode_tcp_response(&response).expect("decode tcp response");
    assert!(decoded.ok);
    assert_eq!(decoded.message, "");
    assert_eq!(decoded.payload, b"world");

    let udp = protocol::UdpMessage {
        session_id: 7,
        packet_id: 9,
        fragment_id: 0,
        fragment_total: 1,
        destination: "1.1.1.1:53".to_owned(),
        payload: b"dns".to_vec(),
    };
    let decoded = protocol::UdpMessage::decode(&udp.encode()).expect("decode udp message");
    assert_eq!(decoded, udp);
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
    let client = Hysteria2Client::connect(
        ClientOptions {
            server: "127.0.0.1".to_owned(),
            server_port: server.port(),
            password: "secret".to_owned(),
            server_name: "localhost".to_owned(),
            insecure: true,
            udp_enabled: true,
            bbr_profile: BbrProfile::Standard,
            disable_path_mtu_discovery: false,
            initial_packet_size: 1200,
        },
        logger("outbound/hysteria2"),
    )
    .await
    .expect("connect client");

    let destination = SocksAddr {
        host: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
        port: 443,
    };

    let mut stream = client
        .dial_tcp(destination.clone(), b"ping")
        .await
        .expect("dial tcp");
    assert_eq!(stream.read_to_end().await.expect("read tcp"), b"echo:ping");

    let mut packet = client.listen_udp().await.expect("listen udp");
    packet
        .send_to(destination, b"dns-query")
        .await
        .expect("send udp");
    let received = packet.recv_from().await.expect("recv udp");
    assert_eq!(received.payload, b"echo:dns-query");
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
    );

    let outbound = manager.get("hysteria2").expect("hysteria2 outbound");
    assert_eq!(outbound.type_name(), "hysteria2");
    assert_eq!(outbound.networks(), &[Network::Tcp, Network::Udp]);

    let destination = SocksAddr {
        host: IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)),
        port: 80,
    };
    let mut stream = outbound
        .dial(Network::Tcp, destination, b"hello")
        .await
        .expect("dial through outbound");
    assert_eq!(
        stream.read_to_end().await.expect("read outbound"),
        b"echo:hello"
    );
}

#[test]
fn hysteria2_bbr_profiles_match_hysteria_go_gains() {
    let standard = BbrProfile::Standard.parameters();
    assert_eq!(standard.high_gain_milli, 2885);
    assert_eq!(standard.high_cwnd_gain_milli, 2885);
    assert_eq!(standard.cwnd_gain_milli, 2000);
    assert_eq!(standard.startup_growth_rounds, 3);
    assert_eq!(standard.bytes_lost_multiplier, 2);

    let conservative = BbrProfile::Conservative.parameters();
    assert_eq!(conservative.high_gain_milli, 2250);
    assert_eq!(conservative.high_cwnd_gain_milli, 1750);
    assert_eq!(conservative.cwnd_gain_milli, 1750);
    assert_eq!(conservative.startup_growth_rounds, 2);
    assert_eq!(conservative.bytes_lost_multiplier, 1);

    let aggressive = BbrProfile::Aggressive.parameters();
    assert_eq!(aggressive.high_gain_milli, 3000);
    assert_eq!(aggressive.high_cwnd_gain_milli, 2250);
    assert_eq!(aggressive.cwnd_gain_milli, 2500);
    assert_eq!(aggressive.startup_growth_rounds, 4);
    assert_eq!(aggressive.bytes_lost_multiplier, 2);
}

#[test]
fn hysteria2_bbr_factory_builds_hysteria_controller() {
    let factory = Arc::new(HysteriaBbrConfig::new(BbrProfile::Aggressive, 1200));
    let controller = quinn::congestion::ControllerFactory::build(factory, Instant::now(), 1200);
    assert!(
        controller
            .into_any()
            .is::<hammer_runtime::hysteria2::bbr::HysteriaBbr>()
    );
}
