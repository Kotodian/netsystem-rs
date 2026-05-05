use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use hammer_adapter::{
    DnsTransport, InboundManager as _, Lifecycle, Network, RouteDecision, SocksAddr,
};
use hammer_core::config::{self, Options};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::StartStage;
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::{
    DnsRouter, DnsTransportManager, InboundManager, OutboundManager, Router,
    dns::{FixedResponseCode, MessageExt},
    tun::{
        MemoryTunDevice, SmoltcpTunStack, SystemTcpNat, TunDispatch, TunInbound, TunPacket,
        icmp_echo_reply_packet, icmp_unreachable_packet, parse_ip_packet,
        process_system_tcp_packet, sniff_packet, sniff_stream, tcp_reset_packet,
        udp_response_packet, udp_unreachable_packet,
    },
};
use hickory_proto::op::Message;
use hickory_proto::rr::{RData, Record};
use tokio::net::UdpSocket;

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

fn options() -> Options {
    config::parse_config(
        r#"
[tun]
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
sniff = true
hijack_dns = true
block_quic = true
domain_strategy = "prefer_ipv4"
udp_disable_domain_unmapping = true

[hysteria2]
server = "example.com"
password = "secret"
sni = "example.com"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "hysteria2"
"#,
    )
    .expect("parse config")
}

fn router_from_options(options: &Options) -> Arc<Router> {
    let outbound = Arc::new(
        OutboundManager::from_options(
            logger("outbound"),
            options.route.final_.clone(),
            &options.outbounds,
        )
        .expect("outbound manager"),
    );
    Arc::new(
        Router::from_options(logger("router"), options.route.clone(), outbound).expect("router"),
    )
}

fn runtime_stack(options: &Options, final_outbound: &str) -> SmoltcpTunStack {
    let outbound = Arc::new(
        OutboundManager::from_options(logger("outbound"), final_outbound, &options.outbounds)
            .expect("outbound manager"),
    );
    let route_options = hammer_core::config::RouteOptions {
        final_: final_outbound.to_owned(),
        ..options.route.clone()
    };
    let router = Arc::new(
        Router::from_options(logger("router"), route_options, Arc::clone(&outbound))
            .expect("router"),
    );
    let dns_transport = Arc::new(DnsTransportManager::new(logger("dns-transport"), "mock"));
    dns_transport.insert(Arc::new(FixedDnsTransport));
    let dns_router = Arc::new(DnsRouter::new_with_manager(
        logger("dns-router"),
        dns_transport,
        hammer_core::config::DomainStrategy::AsIs,
    ));
    SmoltcpTunStack::new_with_runtime(
        logger("tun"),
        router,
        dns_router,
        outbound,
        "tun".to_owned(),
    )
}

#[tokio::test]
async fn tun_stack_matches_udp_routes_using_reverse_dns_before_default_route() {
    let options = config::parse_config(
        r#"
[tun]
address = ["172.19.0.1/30"]
sniff = true
hijack_dns = true

[hysteria2]
server = "example.com"
password = "secret"
sni = "example.com"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "direct"

[[route.rules]]
domain_suffix = ["example.com"]
outbound = "hysteria2"
"#,
    )
    .expect("parse config");
    let stack = runtime_stack(&options, "direct");
    let dns_packet = ipv4_udp_packet(
        [10, 0, 0, 2],
        [1, 1, 1, 1],
        5353,
        53,
        dns_query("www.example.com"),
    );

    stack
        .dispatch_packet(&dns_packet)
        .await
        .expect("populate reverse DNS");

    let routed_packet = ipv4_udp_packet(
        [10, 0, 0, 2],
        [203, 0, 113, 53],
        5353,
        443,
        b"payload".to_vec(),
    );
    let flow = stack.handle_packet(&routed_packet).expect("handle packet");

    assert_eq!(flow.metadata.domain.as_deref(), Some("www.example.com"));
    assert_eq!(
        flow.decision,
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );
}

#[tokio::test]
async fn memory_tun_device_round_trips_packets_and_close_wakes_recv() {
    let device = MemoryTunDevice::new();

    device.inject(vec![1, 2, 3]).await.expect("inject packet");
    assert_eq!(device.recv().await.expect("recv packet"), vec![1, 2, 3]);

    device.send(vec![4, 5, 6]).await.expect("send packet");
    assert_eq!(device.take_output().await, Some(vec![4, 5, 6]));

    device.close();
    assert!(device.recv().await.is_err());
}

#[test]
fn parse_ip_packet_extracts_tcp_and_udp_metadata() {
    let tcp = ipv4_tcp_packet(
        [10, 0, 0, 2],
        [93, 184, 216, 34],
        49152,
        443,
        b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
    );
    let parsed = parse_ip_packet(&tcp).expect("parse tcp");
    assert_eq!(parsed.network, Network::Tcp);
    assert_eq!(parsed.source.port, 49152);
    assert_eq!(parsed.destination.port, 443);

    let udp = ipv4_udp_packet(
        [10, 0, 0, 2],
        [1, 1, 1, 1],
        5353,
        53,
        dns_query("example.com"),
    );
    let parsed = parse_ip_packet(&udp).expect("parse udp");
    assert_eq!(parsed.network, Network::Udp);
    assert_eq!(parsed.destination.port, 53);
}

#[test]
fn sniffers_detect_http_dns_quic_and_stun() {
    let mut tcp = TunPacket::for_test(
        Network::Tcp,
        443,
        b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
    );
    sniff_stream(&mut tcp);
    assert_eq!(tcp.metadata.protocol, "http");
    assert_eq!(tcp.metadata.domain.as_deref(), Some("example.com"));

    let mut dns = TunPacket::for_test(Network::Udp, 53, dns_query("example.com"));
    sniff_packet(&mut dns);
    assert_eq!(dns.metadata.protocol, "dns");

    let mut quic = TunPacket::for_test(Network::Udp, 443, quic_initial());
    sniff_packet(&mut quic);
    assert_eq!(quic.metadata.protocol, "quic");

    let mut stun = TunPacket::for_test(Network::Udp, 3478, stun_binding());
    sniff_packet(&mut stun);
    assert_eq!(stun.metadata.protocol, "stun");
}

#[test]
fn sniff_stream_extracts_tls_sni() {
    let mut tcp = TunPacket::for_test(Network::Tcp, 443, tls_client_hello("Example.COM"));

    sniff_stream(&mut tcp);

    assert_eq!(tcp.metadata.protocol, "tls");
    assert_eq!(tcp.metadata.domain.as_deref(), Some("example.com"));
}

#[test]
fn tun_stack_does_not_sniff_when_sniff_rule_is_disabled() {
    let options = config::parse_config(
        r#"
[tun]
address = ["172.19.0.1/30"]

[hysteria2]
server = "example.com"
password = "secret"
sni = "example.com"

[dns]
server = "hosts"

[route]
final = "direct"

[[route.rules]]
protocol = ["dns"]
outbound = "hysteria2"
"#,
    )
    .expect("parse config");
    let router = router_from_options(&options);
    let stack = SmoltcpTunStack::new(logger("tun"), router, "tun".to_owned());
    let packet = ipv4_udp_packet(
        [10, 0, 0, 2],
        [1, 1, 1, 1],
        5353,
        53,
        dns_query("example.com"),
    );

    let flow = stack.handle_packet(&packet).expect("handle packet");

    assert_eq!(flow.metadata.protocol, "");
    assert_eq!(
        flow.decision,
        RouteDecision::Route {
            outbound: "direct".to_owned()
        }
    );
}

#[test]
fn smoltcp_stack_facade_routes_packets_through_router() {
    let options = options();
    let router = router_from_options(&options);
    let stack = SmoltcpTunStack::new(logger("tun"), router, "tun".to_owned());
    let packet = ipv4_udp_packet(
        [10, 0, 0, 2],
        [1, 1, 1, 1],
        5353,
        53,
        dns_query("example.com"),
    );

    let flow = stack.handle_packet(&packet).expect("handle packet");

    assert_eq!(flow.decision, RouteDecision::HijackDns);
    assert_eq!(flow.metadata.network, Network::Udp);
    assert_eq!(flow.metadata.protocol, "dns");
}

#[test]
fn smoltcp_stack_facade_sets_icmp_protocol_metadata_for_routing() {
    let options = config::parse_config(
        r#"
[tun]
address = ["172.19.0.1/30"]

[hysteria2]
server = "example.com"
password = "secret"
sni = "example.com"

[dns]
server = "hosts"

[route]
final = "direct"

[[route.rules]]
protocol = ["icmp"]
outbound = "hysteria2"

[[route.rules]]
protocol = ["icmpv6"]
outbound = "hysteria2"
"#,
    )
    .expect("parse config");
    let router = router_from_options(&options);
    let stack = SmoltcpTunStack::new(logger("tun"), router, "tun".to_owned());

    let ipv4 = ipv4_icmp_echo_request([10, 0, 0, 2], [8, 8, 8, 8], 0xbeef, 1, b"ping");
    let flow = stack.handle_packet(&ipv4).expect("handle ipv4 icmp");
    assert_eq!(flow.metadata.protocol, "icmp");
    assert_eq!(
        flow.decision,
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );

    let ipv6 = ipv6_icmp_echo_request(
        Ipv6Addr::LOCALHOST.octets(),
        Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111).octets(),
        0xcafe,
        2,
        b"ping6",
    );
    let flow = stack.handle_packet(&ipv6).expect("handle ipv6 icmp");
    assert_eq!(flow.metadata.protocol, "icmpv6");
    assert_eq!(
        flow.decision,
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );
}

#[tokio::test]
async fn tun_dispatch_hijacks_dns_queries() {
    let options = options();
    let stack = runtime_stack(&options, "direct");
    let packet = ipv4_udp_packet(
        [10, 0, 0, 2],
        [1, 1, 1, 1],
        5353,
        53,
        dns_query("example.com"),
    );

    let dispatch = stack.dispatch_packet(&packet).await.expect("dispatch DNS");

    let TunDispatch::DnsResponse { payload, metadata } = dispatch else {
        panic!("unexpected dispatch result");
    };
    assert_eq!(metadata.protocol, "dns");
    let response = <Message as MessageExt>::from_bytes(&payload).expect("decode response");
    assert_eq!(
        response.addresses(),
        vec![std::net::IpAddr::V4(std::net::Ipv4Addr::new(
            203, 0, 113, 53
        ))]
    );
}

#[tokio::test]
async fn tun_dispatch_rejects_blocked_quic() {
    let options = options();
    let stack = runtime_stack(&options, "direct");
    let packet = ipv4_udp_packet([10, 0, 0, 2], [1, 1, 1, 1], 5353, 443, quic_initial());

    let dispatch = stack.dispatch_packet(&packet).await.expect("dispatch QUIC");

    let TunDispatch::Dropped { metadata, reason } = dispatch else {
        panic!("unexpected dispatch result");
    };
    assert_eq!(metadata.protocol, "quic");
    assert!(reason.contains("reject"));
}

#[tokio::test]
async fn tun_dispatch_routes_udp_to_direct_outbound() {
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = udp.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0_u8; 64];
        let (len, peer) = udp.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], b"payload");
        udp.send_to(b"echo:payload", peer).await.unwrap();
    });

    let mut options = options();
    options.route.rules.clear();
    let stack = runtime_stack(&options, "direct");
    let destination = match addr.ip() {
        std::net::IpAddr::V4(ip) => ip.octets(),
        std::net::IpAddr::V6(_) => panic!("test server must be ipv4"),
    };
    let packet = ipv4_udp_packet(
        [10, 0, 0, 2],
        destination,
        5353,
        addr.port(),
        b"payload".to_vec(),
    );

    let dispatch = stack
        .dispatch_packet(&packet)
        .await
        .expect("dispatch direct UDP");

    let TunDispatch::RoutedResponse { payload, metadata } = dispatch else {
        panic!("unexpected dispatch result");
    };
    assert_eq!(metadata.network, Network::Udp);
    assert_eq!(payload.as_ref(), b"echo:payload");
}

#[test]
fn system_stack_rewrites_tcp_packets_to_local_listener_and_back() {
    let mut nat = SystemTcpNat::new();
    let mut packet = ipv4_tcp_packet([10, 0, 0, 2], [93, 184, 216, 34], 49152, 443, b"");

    process_system_tcp_packet(
        &mut packet,
        &mut nat,
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 2)),
        23456,
    )
    .expect("rewrite forward packet");

    let forward = parse_ip_packet(&packet).expect("parse rewritten forward");
    assert_eq!(
        forward.source.host,
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 2))
    );
    assert_eq!(
        forward.destination.host,
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1))
    );
    assert_eq!(forward.destination.port, 23456);
    let nat_port = forward.source.port;
    assert_ne!(nat_port, 49152);

    let mut reply = ipv4_tcp_packet([172, 19, 0, 1], [172, 19, 0, 2], 23456, nat_port, b"");
    process_system_tcp_packet(
        &mut reply,
        &mut nat,
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 2)),
        23456,
    )
    .expect("rewrite reverse packet");

    let reverse = parse_ip_packet(&reply).expect("parse rewritten reverse");
    assert_eq!(
        reverse.source.host,
        IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))
    );
    assert_eq!(reverse.source.port, 443);
    assert_eq!(
        reverse.destination.host,
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))
    );
    assert_eq!(reverse.destination.port, 49152);
}

#[test]
fn system_tcp_nat_refreshes_forward_flow_activity() {
    let mut nat = SystemTcpNat::new_with_timeout(std::time::Duration::from_millis(100));
    let mut first = ipv4_tcp_packet([10, 0, 0, 2], [93, 184, 216, 34], 49152, 443, b"");

    process_system_tcp_packet(
        &mut first,
        &mut nat,
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 2)),
        23456,
    )
    .expect("rewrite first packet");
    let first_port = parse_ip_packet(&first).expect("parse first").source.port;

    std::thread::sleep(std::time::Duration::from_millis(60));
    let mut second = ipv4_tcp_packet([10, 0, 0, 2], [93, 184, 216, 34], 49152, 443, b"");
    process_system_tcp_packet(
        &mut second,
        &mut nat,
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 2)),
        23456,
    )
    .expect("rewrite second packet");
    let second_port = parse_ip_packet(&second).expect("parse second").source.port;

    std::thread::sleep(std::time::Duration::from_millis(60));
    let mut third = ipv4_tcp_packet([10, 0, 0, 2], [93, 184, 216, 34], 49152, 443, b"");
    process_system_tcp_packet(
        &mut third,
        &mut nat,
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(172, 19, 0, 2)),
        23456,
    )
    .expect("rewrite third packet");
    let third_port = parse_ip_packet(&third).expect("parse third").source.port;

    assert_eq!(first_port, second_port);
    assert_eq!(
        second_port, third_port,
        "forward traffic should keep NAT session alive"
    );
}

#[test]
fn system_stack_builds_full_udp_response_packet() {
    let request = ipv4_udp_packet([10, 0, 0, 2], [1, 1, 1, 1], 5353, 53, b"query".to_vec());
    let response = udp_response_packet(
        &request,
        SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53),
        b"answer",
    )
    .expect("build udp response packet");

    let parsed = parse_ip_packet(&response).expect("parse response");
    assert_eq!(parsed.network, Network::Udp);
    assert_eq!(parsed.source.host, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
    assert_eq!(parsed.source.port, 53);
    assert_eq!(
        parsed.destination.host,
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))
    );
    assert_eq!(parsed.destination.port, 5353);
    assert_eq!(parsed.payload, b"answer");
    assert_eq!(
        u16::from_be_bytes([response[2], response[3]]) as usize,
        response.len()
    );
    assert_ne!(u16::from_be_bytes([response[26], response[27]]), 0);
}

#[test]
fn system_stack_builds_udp_reject_unreachable_packet() {
    let request = ipv4_udp_packet([10, 0, 0, 2], [1, 1, 1, 1], 5353, 443, quic_initial());
    let response = udp_unreachable_packet(&request).expect("build unreachable");

    assert_eq!(response[0] >> 4, 4);
    assert_eq!(response[9], 1, "IPv4 reject must be ICMP");
    assert_eq!(response[20], 3, "ICMP destination unreachable");
    assert_eq!(response[21], 3, "ICMP port unreachable");
    assert_eq!(&response[12..16], &[1, 1, 1, 1]);
    assert_eq!(&response[16..20], &[10, 0, 0, 2]);
}

#[test]
fn system_stack_builds_tcp_reject_reset_packet() {
    let request = ipv4_tcp_packet([10, 0, 0, 2], [93, 184, 216, 34], 49152, 443, b"");
    let response = tcp_reset_packet(&request).expect("build tcp reset");
    let parsed = parse_ip_packet(&response).expect("parse tcp reset");

    assert_eq!(parsed.network, Network::Tcp);
    assert_eq!(
        parsed.source.host,
        IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))
    );
    assert_eq!(parsed.source.port, 443);
    assert_eq!(
        parsed.destination.host,
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))
    );
    assert_eq!(parsed.destination.port, 49152);
    assert_eq!(response[33] & 0x14, 0x14, "RST+ACK flags expected");
}

#[test]
fn inbound_manager_registers_tun_inbound_from_options() {
    let options = options();
    let router = router_from_options(&options);
    let manager = InboundManager::from_options(logger("inbound"), &options.inbounds, router)
        .expect("inbound manager");

    let inbound = manager.get("tun").expect("tun inbound registered");
    assert_eq!(inbound.type_name(), "tun");
    assert!(inbound.as_any().is::<TunInbound>());
}

struct FixedDnsTransport;

impl Lifecycle for FixedDnsTransport {
    fn name(&self) -> &str {
        "fixed-dns"
    }

    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for FixedDnsTransport {
    fn type_name(&self) -> &str {
        "mock"
    }

    fn id(&self) -> &str {
        "mock"
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
        let query = message.queries[0].clone();
        let mut response = message.fixed_response(FixedResponseCode::NoError);
        response.add_answer(Record::from_rdata(
            query.name().clone(),
            60,
            RData::A(std::net::Ipv4Addr::new(203, 0, 113, 53).into()),
        ));
        Ok(response)
    }
}

fn ipv4_tcp_packet(
    source: [u8; 4],
    destination: [u8; 4],
    source_port: u16,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = 20 + 20 + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
    packet[32] = 0x50;
    packet[40..].copy_from_slice(payload);
    packet
}

fn ipv4_udp_packet(
    source: [u8; 4],
    destination: [u8; 4],
    source_port: u16,
    destination_port: u16,
    payload: Vec<u8>,
) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[28..].copy_from_slice(&payload);
    packet
}

fn dns_query(name: &str) -> Vec<u8> {
    let mut packet = vec![0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0];
    for label in name.split('.') {
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.extend_from_slice(&[0, 0, 1, 0, 1]);
    packet
}

fn tls_client_hello(server_name: &str) -> Vec<u8> {
    let name = server_name.as_bytes();
    let mut sni = Vec::new();
    sni.extend_from_slice(&((1 + 2 + name.len()) as u16).to_be_bytes());
    sni.push(0);
    sni.extend_from_slice(&(name.len() as u16).to_be_bytes());
    sni.extend_from_slice(name);

    let mut extensions = Vec::new();
    extensions.extend_from_slice(&0u16.to_be_bytes());
    extensions.extend_from_slice(&(sni.len() as u16).to_be_bytes());
    extensions.extend_from_slice(&sni);

    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0u8; 32]);
    body.push(0);
    body.extend_from_slice(&2u16.to_be_bytes());
    body.extend_from_slice(&[0x13, 0x01]);
    body.push(1);
    body.push(0);
    body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    body.extend_from_slice(&extensions);

    let mut handshake = Vec::new();
    handshake.push(1);
    handshake.extend_from_slice(&[
        ((body.len() >> 16) & 0xff) as u8,
        ((body.len() >> 8) & 0xff) as u8,
        (body.len() & 0xff) as u8,
    ]);
    handshake.extend_from_slice(&body);

    let mut record = Vec::new();
    record.push(22);
    record.extend_from_slice(&[0x03, 0x01]);
    record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
    record.extend_from_slice(&handshake);
    record
}

fn quic_initial() -> Vec<u8> {
    vec![
        0xc0, 0x00, 0x00, 0x00, 0x01, 8, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
    ]
}

fn stun_binding() -> Vec<u8> {
    vec![
        0x00, 0x01, 0x00, 0x00, 0x21, 0x12, 0xa4, 0x42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]
}

// ---------------------------------------------------------------------------
// ICMP echo coverage
// ---------------------------------------------------------------------------

#[test]
fn parse_icmpv4_echo_request_yields_icmp_metadata() {
    let packet = ipv4_icmp_echo_request([10, 0, 0, 2], [8, 8, 8, 8], 0xbeef, 1, b"ping body");
    let parsed = parse_ip_packet(&packet).expect("parse icmp echo");
    assert_eq!(parsed.network, Network::Icmp);
    assert_eq!(parsed.source.host, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
    assert_eq!(
        parsed.destination.host,
        IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))
    );
    // ICMP has no port concept; both ends carry the conventional 0.
    assert_eq!(parsed.source.port, 0);
    assert_eq!(parsed.destination.port, 0);
    // Payload is the raw ICMP body starting at the type byte so the
    // outbound conduit can hand it straight to the kernel.
    assert_eq!(parsed.payload[0], 8);
    assert_eq!(&parsed.payload[4..6], &0xbeef_u16.to_be_bytes());
}

#[test]
fn parse_icmpv4_drops_non_echo_types() {
    // Type 13 = Timestamp Request. Anything that isn't an echo request
    // is ours to drop on the floor.
    let mut packet = ipv4_icmp_echo_request([10, 0, 0, 2], [8, 8, 8, 8], 0, 0, b"");
    packet[20] = 13;
    let result = parse_ip_packet(&packet);
    assert!(result.is_err(), "non-echo ICMPv4 must be rejected");
}

#[test]
fn icmp_unreachable_packet_swaps_addresses_and_marks_host_unreachable() {
    let request = ipv4_icmp_echo_request([10, 0, 0, 2], [8, 8, 8, 8], 0x1234, 1, b"data");
    let response = icmp_unreachable_packet(&request).expect("build unreachable");
    // src = original dst, dst = original src — so the unreachable flows
    // back to whoever issued the echo.
    assert_eq!(&response[12..16], &[8, 8, 8, 8]);
    assert_eq!(&response[16..20], &[10, 0, 0, 2]);
    assert_eq!(response[9], 1, "IPv4 reject must be ICMP");
    assert_eq!(response[20], 3, "ICMP destination unreachable type");
    assert_eq!(response[21], 1, "ICMP host unreachable code");
}

#[test]
fn icmp_echo_reply_packet_swaps_addresses_and_normalises_type() {
    let request = ipv4_icmp_echo_request([10, 0, 0, 2], [8, 8, 8, 8], 0xbeef, 1, b"hello");
    // Stub kernel-delivered reply body: type=0 echo reply, with the
    // kernel-owned ping-socket id/seq that must not leak back to the app.
    let mut reply_body = request[20..].to_vec();
    reply_body[0] = 0; // echo reply
    reply_body[4..6].copy_from_slice(&0x4444_u16.to_be_bytes());
    reply_body[6..8].copy_from_slice(&9_u16.to_be_bytes());
    reply_body[2] = 0; // zero checksum so we can recompute below
    reply_body[3] = 0;
    let csum = icmp_checksum(&reply_body);
    reply_body[2..4].copy_from_slice(&csum.to_be_bytes());

    let response = icmp_echo_reply_packet(&request, &reply_body).expect("build reply");
    // Address swap and type=0 normalisation.
    assert_eq!(&response[12..16], &[8, 8, 8, 8]);
    assert_eq!(&response[16..20], &[10, 0, 0, 2]);
    assert_eq!(response[9], 1, "IPv4 reply must be ICMP");
    assert_eq!(response[20], 0, "echo reply type");
    // Identifier + sequence + payload preserved end-to-end.
    assert_eq!(&response[24..26], &0xbeef_u16.to_be_bytes());
    assert_eq!(&response[26..28], &1_u16.to_be_bytes());
    assert_eq!(&response[28..], b"hello");
    assert_eq!(icmp_checksum(&response[20..]), 0);
}

#[test]
fn icmp_echo_reply_packet_restores_ipv6_identifier_and_sequence() {
    let request = ipv6_icmp_echo_request(
        Ipv6Addr::LOCALHOST.octets(),
        Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111).octets(),
        0xcafe,
        2,
        b"hello6",
    );
    let mut reply_body = request[40..].to_vec();
    reply_body[0] = 129; // echo reply
    reply_body[4..6].copy_from_slice(&0x5555_u16.to_be_bytes());
    reply_body[6..8].copy_from_slice(&7_u16.to_be_bytes());
    reply_body[2..4].copy_from_slice(&0_u16.to_be_bytes());

    let response = icmp_echo_reply_packet(&request, &reply_body).expect("build reply");

    assert_eq!(&response[8..24], &request[24..40]);
    assert_eq!(&response[24..40], &request[8..24]);
    assert_eq!(response[6], 58, "IPv6 reply must be ICMPv6");
    assert_eq!(response[40], 129, "echo reply type");
    assert_eq!(&response[44..46], &0xcafe_u16.to_be_bytes());
    assert_eq!(&response[46..48], &2_u16.to_be_bytes());
    assert_eq!(&response[48..], b"hello6");
    assert_eq!(icmpv6_checksum(&response), 0);
}

#[tokio::test]
async fn tun_dispatch_falls_back_to_dest_unreachable_when_outbound_rejects_icmp() {
    // Hysteria2 outbound's default `listen_icmp` impl returns Err, so
    // routing ICMP to it exercises the fallback path.
    let stack = runtime_stack(&options(), "hysteria2");
    let request = ipv4_icmp_echo_request([10, 0, 0, 2], [8, 8, 8, 8], 0xbeef, 1, b"ping");

    let dispatch = stack
        .dispatch_packet(&request)
        .await
        .expect("dispatch icmp");

    let TunDispatch::RoutedResponse { payload, metadata } = dispatch else {
        panic!("unexpected dispatch result for ICMP fallback");
    };
    assert_eq!(metadata.network, Network::Icmp);
    // payload must be a complete IPv4 ICMP Destination Unreachable packet.
    assert_eq!(payload[9], 1);
    assert_eq!(payload[20], 3);
    assert_eq!(payload[21], 1);
}

fn ipv4_icmp_echo_request(
    source: [u8; 4],
    destination: [u8; 4],
    identifier: u16,
    sequence: u16,
    payload: &[u8],
) -> Vec<u8> {
    let icmp_len = 8 + payload.len();
    let total_len = 20 + icmp_len;
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 1; // ICMP
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    packet[20] = 8; // echo request
    packet[21] = 0;
    packet[24..26].copy_from_slice(&identifier.to_be_bytes());
    packet[26..28].copy_from_slice(&sequence.to_be_bytes());
    packet[28..].copy_from_slice(payload);
    let csum = icmp_checksum(&packet[20..]);
    packet[22..24].copy_from_slice(&csum.to_be_bytes());
    packet
}

fn ipv6_icmp_echo_request(
    source: [u8; 16],
    destination: [u8; 16],
    identifier: u16,
    sequence: u16,
    payload: &[u8],
) -> Vec<u8> {
    let icmp_len = 8 + payload.len();
    let mut packet = vec![0u8; 40 + icmp_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(icmp_len as u16).to_be_bytes());
    packet[6] = 58; // ICMPv6
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source);
    packet[24..40].copy_from_slice(&destination);
    packet[40] = 128; // echo request
    packet[41] = 0;
    packet[44..46].copy_from_slice(&identifier.to_be_bytes());
    packet[46..48].copy_from_slice(&sequence.to_be_bytes());
    packet[48..].copy_from_slice(payload);
    let csum = icmpv6_checksum(&packet);
    packet[42..44].copy_from_slice(&csum.to_be_bytes());
    packet
}

fn icmp_checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let mut i = 0;
    while i + 1 < data.len() {
        sum += u32::from(u16::from_be_bytes([data[i], data[i + 1]]));
        i += 2;
    }
    if i < data.len() {
        sum += u32::from(data[i]) << 8;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn icmpv6_checksum(packet: &[u8]) -> u16 {
    let icmp_len = packet.len() - 40;
    let mut pseudo = Vec::with_capacity(40 + icmp_len);
    pseudo.extend_from_slice(&packet[8..40]);
    pseudo.extend_from_slice(&(icmp_len as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, 58]);
    pseudo.extend_from_slice(&packet[40..]);
    icmp_checksum(&pseudo)
}
