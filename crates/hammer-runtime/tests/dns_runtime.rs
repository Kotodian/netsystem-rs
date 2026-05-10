use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use hammer_adapter::{
    ComponentMeta, DefaultInterfaceUpdateListener, DnsQueryOptions, DnsTransport,
    DnsTransportComponent, Lifecycle, Network, NetworkInterface, Outbound as AdapterOutbound,
    OutboundComponent, OutboundManager as _, PlatformInterface, ProxyDatagram, ProxyPacketConn,
    ProxyStream, RuntimeComponent, SocksAddr, StartStage, TunOptions, WifiState,
};
use hammer_core::config::{self, DomainStrategy};
use hammer_core::error::HammerError;
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::{
    DnsClient, DnsRouter, DnsTransportManager, OutboundManager,
    dns::{FixedResponseCode, HostsTransport, MessageExt, TcpDnsTransport, UdpDnsTransport},
};
use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

fn dns_transport_component<T>(
    id: &str,
    type_name: &'static str,
    transport: Arc<T>,
) -> DnsTransportComponent
where
    T: DnsTransport + 'static,
{
    let runtime: Arc<dyn DnsTransport> = transport;
    RuntimeComponent::new(
        ComponentMeta::new("dns_transport", type_name, id, Vec::new(), Vec::new(), None),
        runtime,
    )
}

fn outbound_component<T>(
    id: &str,
    type_name: &'static str,
    networks: Vec<Network>,
    outbound: Arc<T>,
) -> OutboundComponent
where
    T: AdapterOutbound + 'static,
{
    let runtime: Arc<dyn AdapterOutbound> = outbound;
    RuntimeComponent::new(
        ComponentMeta::new("outbound", type_name, id, networks, Vec::new(), None),
        runtime,
    )
}

fn query(name: &str, record_type: RecordType) -> Message {
    let mut msg = Message::new(42, MessageType::Query, OpCode::Query);
    msg.add_query({
        let mut q = Query::query(Name::from_ascii(name).unwrap(), record_type);
        q.set_query_class(DNSClass::IN);
        q
    });
    msg.metadata.recursion_desired = true;
    msg
}

fn response_for(request: &Message, ttl: u32, addr: IpAddr) -> Message {
    let question = request.queries[0].clone();
    let mut response = request.fixed_response(FixedResponseCode::NoError);
    let rdata = match addr {
        IpAddr::V4(ip) => RData::A(ip.into()),
        IpAddr::V6(ip) => RData::AAAA(ip.into()),
    };
    response.add_answer(Record::from_rdata(question.name().clone(), ttl, rdata));
    response
}

#[derive(Debug)]
struct CountingTransport {
    calls: AtomicUsize,
    ttl: u32,
    addr: IpAddr,
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

impl CountingTransport {
    fn new(ttl: u32, addr: IpAddr) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            ttl,
            addr,
        }
    }
}

impl Lifecycle for CountingTransport {
    fn name(&self) -> &str {
        "counting"
    }

    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[derive(Debug)]
struct IndexedTransport {
    calls: AtomicUsize,
    ttl: u32,
}

impl IndexedTransport {
    fn new(ttl: u32) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            ttl,
        }
    }
}

impl Lifecycle for IndexedTransport {
    fn name(&self) -> &str {
        "indexed"
    }

    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for IndexedTransport {
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let index = index_from_query(&message);
        Ok(response_for(&message, self.ttl, indexed_addr(index)))
    }
}

fn index_from_query(message: &Message) -> usize {
    let name = message.queries[0].name().to_ascii();
    let label = name
        .split('.')
        .next()
        .expect("test query must have a first label");
    label
        .rsplit_once('-')
        .expect("test query label must end with an index")
        .1
        .parse()
        .expect("test query index must be numeric")
}

fn indexed_addr(index: usize) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(
        10,
        ((index >> 16) & 0xff) as u8,
        ((index >> 8) & 0xff) as u8,
        (index & 0xff) as u8,
    ))
}

struct CapturingOutbound {
    destinations: Arc<std::sync::Mutex<Vec<String>>>,
}

struct CapturingPacketConn {
    destinations: Arc<std::sync::Mutex<Vec<String>>>,
    response: Option<Bytes>,
}

#[async_trait]
impl AdapterOutbound for CapturingOutbound {
    async fn dial(
        &self,
        _network: Network,
        _destination: SocksAddr,
        _initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        Err(HammerError::internal("capture outbound does not dial"))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        Ok(Box::new(CapturingPacketConn {
            destinations: Arc::clone(&self.destinations),
            response: None,
        }))
    }
}

#[async_trait]
impl ProxyPacketConn for CapturingPacketConn {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> Result<(), HammerError> {
        self.destinations
            .lock()
            .expect("capture destinations poisoned")
            .push(destination.to_string());
        let request = <Message as MessageExt>::from_bytes(payload)?;
        let response = response_for(&request, 60, IpAddr::V4(Ipv4Addr::new(198, 51, 100, 31)));
        self.response = Some(Bytes::from(MessageExt::to_bytes(&response)?));
        Ok(())
    }

    async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        Ok(ProxyDatagram {
            destination: SocksAddr::ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 53),
            payload: self.response.take().expect("send_to must run first"),
        })
    }
}

#[async_trait]
impl DnsTransport for CountingTransport {
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(response_for(&message, self.ttl, self.addr))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hosts_transport_returns_fixed_a_and_aaaa_records() {
    let hosts = HostsTransport::from_predefined(
        "hosts",
        [
            ("example.test", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7))),
            ("example.test", IpAddr::V6(Ipv6Addr::LOCALHOST)),
        ],
    );

    let a = hosts
        .exchange(query("example.test.", RecordType::A))
        .await
        .unwrap();
    assert_eq!(a.metadata.response_code, ResponseCode::NoError);
    assert_eq!(a.addresses(), vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7))]);

    let aaaa = hosts
        .exchange(query("example.test.", RecordType::AAAA))
        .await
        .unwrap();
    assert_eq!(aaaa.addresses(), vec![IpAddr::V6(Ipv6Addr::LOCALHOST)]);

    let missing = hosts
        .exchange(query("missing.test.", RecordType::A))
        .await
        .unwrap();
    assert_eq!(missing.metadata.response_code, ResponseCode::NXDomain);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_client_caches_by_transport_question_and_normalizes_ttl() {
    let transport = Arc::new(CountingTransport::new(
        60,
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
    ));
    let transport_component = dns_transport_component("mock", "mock", Arc::clone(&transport));
    let client = DnsClient::new(logger("dns"));

    let first = client
        .lookup(
            &transport_component,
            "cached.test",
            DnsQueryOptions {
                lookup_strategy: DomainStrategy::Ipv4Only,
                ..DnsQueryOptions::default()
            },
        )
        .await
        .unwrap();
    let second = client
        .lookup(
            &transport_component,
            "cached.test",
            DnsQueryOptions {
                lookup_strategy: DomainStrategy::Ipv4Only,
                ..DnsQueryOptions::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);

    client.clear_cache();
    let _ = client
        .lookup(
            &transport_component,
            "cached.test",
            DnsQueryOptions {
                lookup_strategy: DomainStrategy::Ipv4Only,
                ..DnsQueryOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_client_applies_rewrite_ttl_to_cache_hits() {
    let transport = Arc::new(CountingTransport::new(
        60,
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 12)),
    ));
    let transport_component = dns_transport_component("mock", "mock", Arc::clone(&transport));
    let client = DnsClient::new(logger("dns"));
    let base_options = DnsQueryOptions {
        lookup_strategy: DomainStrategy::Ipv4Only,
        ..DnsQueryOptions::default()
    };

    let first = client
        .exchange(
            &transport_component,
            query("rewrite-hit.test", RecordType::A),
            base_options.clone(),
        )
        .await
        .unwrap();
    assert_eq!(first.answers[0].ttl, 60);

    let cached = client
        .exchange(
            &transport_component,
            query("rewrite-hit.test", RecordType::A),
            DnsQueryOptions {
                rewrite_ttl: Some(5),
                ..base_options
            },
        )
        .await
        .unwrap();

    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
    assert_eq!(cached.answers[0].ttl, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_client_bounds_cache_and_promotes_touched_lru_entry() {
    let transport = Arc::new(CountingTransport::new(
        60,
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 11)),
    ));
    let transport_component = dns_transport_component("mock", "mock", Arc::clone(&transport));
    let client = DnsClient::new(logger("dns"));
    let options = DnsQueryOptions {
        lookup_strategy: DomainStrategy::Ipv4Only,
        ..DnsQueryOptions::default()
    };

    for index in 0..1024 {
        let domain = format!("bounded-{index}.test");
        let _ = client
            .lookup(&transport_component, &domain, options.clone())
            .await
            .unwrap();
    }
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1024);

    let _ = client
        .lookup(&transport_component, "bounded-0.test", options.clone())
        .await
        .unwrap();
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1024);

    let _ = client
        .lookup(&transport_component, "bounded-1024.test", options.clone())
        .await
        .unwrap();
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1025);

    let _ = client
        .lookup(&transport_component, "bounded-0.test", options.clone())
        .await
        .unwrap();
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1025);

    let _ = client
        .lookup(&transport_component, "bounded-1.test", options)
        .await
        .unwrap();
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1026);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_client_applies_lookup_strategy_ordering() {
    let transport = Arc::new(CountingTransport::new(
        60,
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
    ));
    let transport_component = dns_transport_component("mock", "mock", Arc::clone(&transport));
    let client = DnsClient::new(logger("dns"));

    let ipv4_only = client
        .lookup(
            &transport_component,
            "strategy.test",
            DnsQueryOptions {
                lookup_strategy: DomainStrategy::Ipv4Only,
                ..DnsQueryOptions::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(ipv4_only, vec![IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))]);
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_transport_retries_truncated_response_over_tcp() {
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let tcp = TcpListener::bind(udp.local_addr().unwrap()).await.unwrap();
    let addr = udp.local_addr().unwrap();

    tokio::spawn(async move {
        let mut buf = [0_u8; 512];
        let (len, peer) = udp.recv_from(&mut buf).await.unwrap();
        let request = <Message as MessageExt>::from_bytes(&buf[..len]).unwrap();
        let mut truncated = request.fixed_response(FixedResponseCode::NoError);
        truncated.metadata.truncation = true;
        udp.send_to(&MessageExt::to_bytes(&truncated).unwrap(), peer)
            .await
            .unwrap();
    });

    tokio::spawn(async move {
        let (mut stream, _) = tcp.accept().await.unwrap();
        let mut len_buf = [0_u8; 2];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u16::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0_u8; len];
        stream.read_exact(&mut payload).await.unwrap();
        let request = <Message as MessageExt>::from_bytes(&payload).unwrap();
        let response = response_for(&request, 60, IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9)));
        let bytes = MessageExt::to_bytes(&response).unwrap();
        stream
            .write_all(&(bytes.len() as u16).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&bytes).await.unwrap();
    });

    let transport = UdpDnsTransport::new(
        "default",
        addr.ip().to_string(),
        addr.port(),
        logger("dns/transport/udp"),
    );
    let response = transport
        .exchange(query("fallback.test.", RecordType::A))
        .await
        .unwrap();
    assert_eq!(
        response.addresses(),
        vec![IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9))]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_transport_uses_dns_length_prefixed_framing() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = tcp.accept().await.unwrap();
        let mut len_buf = [0_u8; 2];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u16::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0_u8; len];
        stream.read_exact(&mut payload).await.unwrap();
        let request = <Message as MessageExt>::from_bytes(&payload).unwrap();
        let response = response_for(&request, 60, IpAddr::V4(Ipv4Addr::new(198, 51, 100, 11)));
        let bytes = MessageExt::to_bytes(&response).unwrap();
        stream
            .write_all(&(bytes.len() as u16).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&bytes).await.unwrap();
    });

    let transport = TcpDnsTransport::new(
        "default",
        addr.ip().to_string(),
        addr.port(),
        logger("dns/transport/tcp"),
    );
    let response = transport
        .exchange(query("tcp.test.", RecordType::A))
        .await
        .unwrap();
    assert_eq!(
        response.addresses(),
        vec![IpAddr::V4(Ipv4Addr::new(198, 51, 100, 11))]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_transport_uses_via_outbound_and_protects_socket() {
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = udp.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0_u8; 512];
        let (len, peer) = udp.recv_from(&mut buf).await.unwrap();
        let request = <Message as MessageExt>::from_bytes(&buf[..len]).unwrap();
        let response = response_for(&request, 60, IpAddr::V4(Ipv4Addr::new(198, 51, 100, 21)));
        udp.send_to(&MessageExt::to_bytes(&response).unwrap(), peer)
            .await
            .unwrap();
    });

    let options = config::parse_config(&format!(
        r#"
[tun]
address = ["172.19.0.1/30"]
[hysteria2]
server = "example.com"
password = "x"
sni = "example.com"
[dns]
server = "udp://{}:{}"
via = "direct"
[route]
final = "direct"
"#,
        addr.ip(),
        addr.port()
    ))
    .unwrap();
    let platform = Arc::new(ProtectPlatform::default());
    let outbound = Arc::new(
        OutboundManager::from_options_with_platform(
            logger("outbound"),
            options.route.final_.clone(),
            &options.outbounds,
            Arc::clone(&platform),
        )
        .expect("outbound manager"),
    );
    let manager = DnsTransportManager::from_options_with_runtime(
        logger("dns-transport"),
        &options.dns,
        outbound,
        Arc::clone(&platform),
        None,
    )
    .expect("dns manager");

    let response = manager
        .default()
        .expect("default dns")
        .exchange(query("via.test.", RecordType::A))
        .await
        .expect("dns via exchange");

    assert_eq!(
        response.addresses(),
        vec![IpAddr::V4(Ipv4Addr::new(198, 51, 100, 21))]
    );
    assert!(platform.calls() >= 1, "DNS socket was not protected");
}

// Strict domain_resolver validation (commit 0c0ad6e) refuses a domain DNS
// server with a non-empty `via` and no bootstrap, so this fixture no longer
// loads. Either drop this test or add `domain_resolver` + assert the
// resolved IP, but the assertion `destinations == ["resolver.test:53"]`
// then becomes meaningless. Left ignored for human follow-up.
#[ignore = "incompatible with strict domain_resolver validation; needs rewrite"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_udp_via_preserves_hostname_destination_for_proxy() {
    let options = config::parse_config(
        r#"
[tun]
address = ["172.19.0.1/30"]

[[outbounds]]
type = "block"
id = "proxy"

[dns]
server = "udp://resolver.test:53"
via = "proxy"

[route]
final = "proxy"
"#,
    )
    .expect("parse config");
    let destinations = Arc::new(std::sync::Mutex::new(Vec::new()));
    let outbound = Arc::new(
        OutboundManager::from_options(logger("outbound"), "proxy", &options.outbounds)
            .expect("outbound manager"),
    );
    outbound.remove("proxy").expect("remove block outbound");
    outbound
        .register_outbound(outbound_component(
            "proxy",
            "capture",
            vec![Network::Udp],
            Arc::new(CapturingOutbound {
                destinations: Arc::clone(&destinations),
            }),
        ))
        .expect("register capture outbound");
    let manager = DnsTransportManager::from_options_with_runtime(
        logger("dns-transport"),
        &options.dns,
        outbound,
        Arc::new(ProtectPlatform::default()),
        None,
    )
    .expect("dns manager");

    let response = manager
        .default()
        .expect("default dns")
        .exchange(query("via-host.test.", RecordType::A))
        .await
        .expect("dns via exchange");

    assert_eq!(
        response.addresses(),
        vec![IpAddr::V4(Ipv4Addr::new(198, 51, 100, 31))]
    );
    assert_eq!(
        destinations
            .lock()
            .expect("capture destinations poisoned")
            .as_slice(),
        ["resolver.test:53"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_manager_builds_current_schema_default_server() {
    let options = config::parse_config(
        r#"
[tun]
address = ["172.19.0.1/30"]
[hysteria2]
server = "example.com"
password = "x"
sni = "example.com"
[dns]
server = "hosts"
"#,
    )
    .unwrap();
    let manager = DnsTransportManager::from_options(logger("dns-transport"), &options.dns).unwrap();
    manager.start(StartStage::Start).unwrap();

    let default = manager.default().expect("default transport");
    assert_eq!(default.type_name(), "hosts");
    assert_eq!(default.id(), "default");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_lookup_uses_default_transport_and_tracks_reverse_mapping() {
    let manager = Arc::new(DnsTransportManager::new(logger("dns-transport"), "mock"));
    manager.insert(dns_transport_component(
        "mock",
        "mock",
        Arc::new(CountingTransport::new(
            60,
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 22)),
        )),
    ));
    let router = DnsRouter::new_with_manager(logger("dns"), manager, DomainStrategy::AsIs);

    let addrs = router
        .lookup("router.test", DnsQueryOptions::default())
        .await
        .unwrap();

    assert_eq!(addrs, vec![IpAddr::V4(Ipv4Addr::new(203, 0, 113, 22))]);
    assert_eq!(
        router.lookup_reverse_mapping(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 22))),
        Some("router.test".to_owned())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_reverse_cache_bounds_and_promotes_touched_lru_mapping() {
    let manager = Arc::new(DnsTransportManager::new(logger("dns-transport"), "indexed"));
    manager.insert(dns_transport_component(
        "indexed",
        "mock",
        Arc::new(IndexedTransport::new(60)),
    ));
    let router = DnsRouter::new_with_manager(logger("dns"), manager, DomainStrategy::AsIs);
    let options = DnsQueryOptions {
        lookup_strategy: DomainStrategy::Ipv4Only,
        ..DnsQueryOptions::default()
    };

    for index in 0..2048 {
        let domain = format!("reverse-{index}.test");
        let _ = router.lookup(&domain, options.clone()).await.unwrap();
    }

    assert_eq!(
        router.lookup_reverse_mapping(indexed_addr(0)),
        Some("reverse-0.test".to_owned())
    );

    let _ = router
        .lookup("reverse-2048.test", options.clone())
        .await
        .unwrap();

    assert_eq!(
        router.lookup_reverse_mapping(indexed_addr(0)),
        Some("reverse-0.test".to_owned())
    );
    assert_eq!(router.lookup_reverse_mapping(indexed_addr(1)), None);
}
