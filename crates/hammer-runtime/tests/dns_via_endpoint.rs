//! End-to-end integration: `via = "<endpoint-id>"` resolves through the
//! auto-registered `EndpointOutboundAdapter` and a DNS query round-trips
//! through the synthetic endpoint's encrypt / local-recv channels.
//!
//! These tests live separately from `dns_runtime.rs` because they wire up
//! a `FakeEndpoint` + adapter rather than testing the DNS transport
//! against a real outbound. Only UDP DNS is covered today; TCP / DoH via
//! endpoint adapter rely on the same adapter plumbing but require a much
//! larger fake server (smoltcp peer + TLS), so they sit as TODO.

#![cfg(all(feature = "endpoint-wireguard", feature = "dns-udp"))]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hammer_adapter::{Endpoint, Lifecycle, Outbound, StartStage};
use hammer_core::config;
use hammer_core::error::CoreResult;
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::dns::MessageExt;
use hammer_runtime::{DnsTransportManager, OutboundManager, endpoints::EndpointOutboundAdapter};
use hickory_proto::op::{Message, MessageType, OpCode, Query};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use ipnet::{IpNet, Ipv4Net};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

fn dns_query(name: &str) -> Message {
    let mut msg = Message::new(0x4242, MessageType::Query, OpCode::Query);
    msg.add_query({
        let mut q = Query::query(Name::from_ascii(name).unwrap(), RecordType::A);
        q.set_query_class(DNSClass::IN);
        q
    });
    msg.metadata.recursion_desired = true;
    msg
}

fn fixed_a_response(request: &Message, addr: Ipv4Addr) -> Message {
    use hammer_runtime::dns::{FixedResponseCode, MessageExt as _};
    let q = request.queries[0].clone();
    let mut response = request.fixed_response(FixedResponseCode::NoError);
    response.add_answer(Record::from_rdata(
        q.name().clone(),
        60,
        RData::A(addr.into()),
    ));
    response
}

/// Synthetic endpoint that wires the adapter's encrypt channel to a real
/// UDP DNS server and reflects responses back through ip_local_recv. The
/// test creates the channels itself (rather than relying on real wg state
/// machine) so the adapter sees a deterministic, instant L3 fan-out.
struct FakeEndpoint {
    id: String,
    encrypt_tx: mpsc::Sender<Bytes>,
    default_rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
    local_rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
    interface: IpNet,
}

impl Lifecycle for FakeEndpoint {
    fn name(&self) -> &str {
        "fake-endpoint"
    }
    fn start(&self, _stage: StartStage) -> CoreResult<()> {
        Ok(())
    }
    fn close(&self) -> CoreResult<()> {
        Ok(())
    }
}

impl Endpoint for FakeEndpoint {
    fn id(&self) -> &str {
        &self.id
    }
    fn ip_send_clone(&self) -> mpsc::Sender<Bytes> {
        self.encrypt_tx.clone()
    }
    fn ip_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
        self.default_rx.lock().expect("default_rx mutex").take()
    }
    fn ip_local_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
        self.local_rx.lock().expect("local_rx mutex").take()
    }
    fn interface_addresses(&self) -> Vec<IpNet> {
        vec![self.interface]
    }
}

fn parse_ipv4_udp(pkt: &[u8]) -> Option<(Ipv4Addr, u16, Ipv4Addr, u16, Vec<u8>)> {
    if pkt.len() < 28 || (pkt[0] >> 4) != 4 || pkt[9] != 17 {
        return None;
    }
    let ihl = ((pkt[0] & 0x0f) as usize) * 4;
    if ihl < 20 || pkt.len() < ihl + 8 {
        return None;
    }
    let src_ip = Ipv4Addr::new(pkt[12], pkt[13], pkt[14], pkt[15]);
    let dst_ip = Ipv4Addr::new(pkt[16], pkt[17], pkt[18], pkt[19]);
    let src_port = u16::from_be_bytes([pkt[ihl], pkt[ihl + 1]]);
    let dst_port = u16::from_be_bytes([pkt[ihl + 2], pkt[ihl + 3]]);
    let payload = pkt[ihl + 8..].to_vec();
    Some((src_ip, src_port, dst_ip, dst_port, payload))
}

fn build_ipv4_udp(
    src: Ipv4Addr,
    src_port: u16,
    dst: Ipv4Addr,
    dst_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    // Reuse the adapter's wire format by going through a tiny inline
    // builder — IP-id zero, TTL 64, IHL 5. Both checksums are computed
    // here so the demux fast-path checks line up.
    let total_len = 20 + 8 + payload.len();
    let mut pkt = Vec::with_capacity(total_len);
    pkt.push(0x45);
    pkt.push(0x00);
    pkt.extend_from_slice(&(total_len as u16).to_be_bytes());
    pkt.extend_from_slice(&[0, 0]);
    pkt.extend_from_slice(&[0, 0]);
    pkt.push(64);
    pkt.push(17);
    pkt.extend_from_slice(&[0, 0]);
    pkt.extend_from_slice(&src.octets());
    pkt.extend_from_slice(&dst.octets());
    let ipc = internet_checksum(&pkt[..20]);
    pkt[10..12].copy_from_slice(&ipc.to_be_bytes());

    let udp_len = 8 + payload.len();
    pkt.extend_from_slice(&src_port.to_be_bytes());
    pkt.extend_from_slice(&dst_port.to_be_bytes());
    pkt.extend_from_slice(&(udp_len as u16).to_be_bytes());
    pkt.extend_from_slice(&[0, 0]);
    pkt.extend_from_slice(payload);
    let udpc = udp_checksum_ipv4(src, dst, &pkt[20..]);
    let udpc = if udpc == 0 { 0xffff } else { udpc };
    pkt[26..28].copy_from_slice(&udpc.to_be_bytes());
    pkt
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let mut i = 0;
    while i + 1 < bytes.len() {
        sum = sum.wrapping_add(u16::from_be_bytes([bytes[i], bytes[i + 1]]) as u32);
        i += 2;
    }
    if i < bytes.len() {
        sum = sum.wrapping_add((bytes[i] as u32) << 8);
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn udp_checksum_ipv4(src: Ipv4Addr, dst: Ipv4Addr, udp_and_payload: &[u8]) -> u16 {
    let udp_len = udp_and_payload.len();
    let mut pseudo = Vec::with_capacity(12 + udp_len);
    pseudo.extend_from_slice(&src.octets());
    pseudo.extend_from_slice(&dst.octets());
    pseudo.push(0);
    pseudo.push(17);
    pseudo.extend_from_slice(&(udp_len as u16).to_be_bytes());
    pseudo.extend_from_slice(udp_and_payload);
    internet_checksum(&pseudo)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_via_endpoint_udp_round_trip() {
    // 1. Start a real fake UDP DNS server (loopback) — adapter packets
    //    will be forwarded here by the test's endpoint glue.
    let dns_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dns_addr = dns_socket.local_addr().unwrap();
    let answer_ip = Ipv4Addr::new(198, 51, 100, 21);
    tokio::spawn(async move {
        loop {
            let mut buf = [0u8; 1500];
            let (n, peer) = match dns_socket.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(_) => return,
            };
            let Ok(request) = <Message as MessageExt>::from_bytes(&buf[..n]) else {
                continue;
            };
            let response = fixed_a_response(&request, answer_ip);
            let bytes = <Message as MessageExt>::to_bytes(&response).unwrap();
            let _ = dns_socket.send_to(&bytes, peer).await;
        }
    });

    // 2. Wire FakeEndpoint channels manually. The "encrypt" side reads
    //    adapter-generated IP packets and forwards their UDP payload to
    //    the fake DNS server through a real UDP socket; responses are
    //    re-wrapped into IP packets and pushed into the local channel
    //    so the adapter's demux task delivers them to the right flow.
    let (encrypt_tx, mut encrypt_rx) = mpsc::channel::<Bytes>(32);
    let (_default_tx, default_rx) = mpsc::channel::<Bytes>(1);
    let (local_tx, local_rx) = mpsc::channel::<Bytes>(32);
    let ep_interface = Ipv4Addr::new(10, 66, 0, 2);
    let endpoint = Arc::new(FakeEndpoint {
        id: "wg-out".into(),
        encrypt_tx,
        default_rx: Mutex::new(Some(default_rx)),
        local_rx: Mutex::new(Some(local_rx)),
        interface: IpNet::V4(Ipv4Net::new(ep_interface, 32).unwrap()),
    });

    // Glue task: bridge adapter encrypt channel <-> fake DNS server.
    let bridge_local_tx = local_tx.clone();
    tokio::spawn(async move {
        let bridge_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        loop {
            let Some(pkt) = encrypt_rx.recv().await else {
                return;
            };
            let Some((src_ip, src_port, _dst_ip, dst_port, payload)) = parse_ipv4_udp(&pkt) else {
                continue;
            };
            // Forward DNS query to the fake server.
            let server_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), dst_port);
            if bridge_socket
                .send_to(&payload, server_target)
                .await
                .is_err()
            {
                continue;
            }
            let mut buf = [0u8; 1500];
            let Ok(Ok((n, _))) =
                tokio::time::timeout(Duration::from_secs(2), bridge_socket.recv_from(&mut buf))
                    .await
            else {
                continue;
            };
            // Wrap response into an IP+UDP packet aimed at the adapter
            // (dst_ip = endpoint interface, dst_port = adapter's src_port).
            let reply = build_ipv4_udp(
                Ipv4Addr::new(8, 8, 8, 8), // synthetic remote IP for the response
                dst_port,
                src_ip,
                src_port,
                &buf[..n],
            );
            let _ = bridge_local_tx.send(Bytes::from(reply)).await;
        }
    });

    // 3. Build OutboundManager and register the endpoint-adapter under
    //    the endpoint id `wg-out`. Bypass the full RuntimeService path
    //    — we just need outbound.get("wg-out") to return our adapter.
    let outbound = Arc::new(OutboundManager::new(logger("outbound-test"), String::new()));
    let adapter = EndpointOutboundAdapter::arc(
        logger("endpoint-outbound/wg-out"),
        "wg-out".into(),
        endpoint.clone() as Arc<dyn Endpoint>,
    );
    outbound
        .register_outbound(adapter)
        .expect("register adapter");

    // 4. Construct a UdpDnsTransport with `via = "wg-out"` pointing at
    //    the fake DNS server's address (the bridge replays the dst port
    //    onto 127.0.0.1, so any IP works for `server`).
    let dns_toml = format!(
        r#"
[tun]
address = ["172.19.0.1/30"]
[[outbounds]]
type = "direct"
id = "direct"
[dns]
server = "udp://{}:{}"
via = "wg-out"
[[endpoints]]
type = "wireguard"
id = "wg-out"
private_key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
address = ["10.66.0.2/32"]
[[endpoints.peers]]
public_key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
address = "1.2.3.4"
port = 51820
allowed_ips = ["0.0.0.0/0"]
[route]
final = "direct"
"#,
        dns_addr.ip(),
        dns_addr.port(),
    );
    // Parse merely validates the wire format we want to flow. The
    // DnsTransportManager built below talks to our OutboundManager
    // directly, not the EndpointManager parsed from this toml.
    let options = config::parse_config(&dns_toml).expect("parse via=endpoint config");
    let manager = DnsTransportManager::from_options_with_runtime(
        logger("dns-transport-test"),
        &options.dns,
        Arc::clone(&outbound),
        Arc::new(NoopPlatform::default()),
        None,
    )
    .expect("dns transport manager");

    // Drive the lifecycle so DNS transport's warm-up wires through.
    let transport = manager.default().expect("default dns transport");
    let _ = transport.start(StartStage::Initialize);
    let _ = transport.start(StartStage::Start);
    let _ = transport.start(StartStage::Started);

    // 5. Run a query end-to-end.
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        transport.exchange(dns_query("via-endpoint.test.")),
    )
    .await
    .expect("dns exchange timed out")
    .expect("dns exchange failed");

    let answers: Vec<IpAddr> = response
        .answers
        .iter()
        .filter_map(|r| match &r.data {
            RData::A(a) => Some(IpAddr::V4(a.0)),
            _ => None,
        })
        .collect();
    assert_eq!(answers, vec![IpAddr::V4(answer_ip)]);
}

// PlatformInterface stub.
#[derive(Default)]
struct NoopPlatform;

impl hammer_adapter::PlatformInterface for NoopPlatform {
    fn open_tun(
        &self,
        _o: hammer_adapter::TunOptions,
    ) -> Result<i32, hammer_core::error::HammerError> {
        Ok(0)
    }

    fn use_platform_auto_detect_interface_control(&self) -> bool {
        false
    }

    fn auto_detect_interface_control(
        &self,
        _fd: i32,
    ) -> Result<(), hammer_core::error::HammerError> {
        Ok(())
    }

    fn start_default_interface_monitor(
        &self,
        _l: Arc<dyn hammer_adapter::DefaultInterfaceUpdateListener>,
    ) -> Result<(), hammer_core::error::HammerError> {
        Ok(())
    }

    fn close_default_interface_monitor(
        &self,
        _l: Arc<dyn hammer_adapter::DefaultInterfaceUpdateListener>,
    ) -> Result<(), hammer_core::error::HammerError> {
        Ok(())
    }

    fn get_interfaces(
        &self,
    ) -> Result<Vec<hammer_adapter::NetworkInterface>, hammer_core::error::HammerError> {
        Ok(Vec::new())
    }

    fn read_wifi_state(&self) -> Option<hammer_adapter::WifiState> {
        None
    }
}

// =====================================================================
// TCP / DoH coverage: adapter dial(Tcp) end-to-end via a smoltcp peer.
//
// Building a full DNS-over-TCP fake server inside smoltcp is plenty of
// code, and DoH adds rustls + h2 on top. To keep the integration test
// focused on the *adapter's TCP path* (smoltcp handshake + AsyncRead/Write
// wrap + per-flow demux), we use a smoltcp echo peer here. DoH/TCP-DNS
// protocol framing is covered by the existing dns_runtime.rs transport
// tests against a real OS TcpListener; the new piece is "the adapter
// dial returns a working stream", which echo verifies precisely.
// =====================================================================

use hammer_adapter::Network;
use hammer_adapter::SocksAddr;
use hammer_runtime::endpoints::EndpointOutboundAdapter as _EOA; // sanity import
use smoltcp::iface::{Config as SmolConfig, Interface as SmolIface, SocketSet as SmolSockets};
use smoltcp::phy::{Device as SmolDevice, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::tcp::{Socket as SmolTcp, SocketBuffer, State as SmolState};
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{HardwareAddress, IpCidr, Ipv4Address, Ipv4Cidr};
use std::collections::VecDeque;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep_until;

struct PeerDevice {
    rx_queue: VecDeque<Bytes>,
    tx_queue: VecDeque<Bytes>,
}

impl SmolDevice for PeerDevice {
    type RxToken<'a>
        = PeerRx
    where
        Self: 'a;
    type TxToken<'a>
        = PeerTx<'a>
    where
        Self: 'a;
    fn receive(&mut self, _: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let buf = self.rx_queue.pop_front()?;
        Some((
            PeerRx { buf },
            PeerTx {
                q: &mut self.tx_queue,
            },
        ))
    }
    fn transmit(&mut self, _: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(PeerTx {
            q: &mut self.tx_queue,
        })
    }
    fn capabilities(&self) -> DeviceCapabilities {
        let mut c = DeviceCapabilities::default();
        c.max_transmission_unit = 1280;
        c.medium = Medium::Ip;
        c
    }
}

struct PeerRx {
    buf: Bytes,
}
impl RxToken for PeerRx {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.buf)
    }
}
struct PeerTx<'a> {
    q: &'a mut VecDeque<Bytes>,
}
impl<'a> TxToken for PeerTx<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buf = vec![0u8; len];
        let r = f(&mut buf);
        self.q.push_back(Bytes::from(buf));
        r
    }
}

/// Drive a single-socket smoltcp echo peer that listens on `peer_ip:listen_port`,
/// accepts the adapter's connect, echoes every byte received until the
/// adapter closes the write side, then closes its own side.
async fn smoltcp_echo_peer(
    peer_ip: Ipv4Addr,
    listen_port: u16,
    mut ingress_rx: mpsc::Receiver<Bytes>,
    egress_tx: mpsc::Sender<Bytes>,
) {
    let mut device = PeerDevice {
        rx_queue: VecDeque::new(),
        tx_queue: VecDeque::new(),
    };
    let cfg = SmolConfig::new(HardwareAddress::Ip);
    let mut iface = SmolIface::new(cfg, &mut device, SmolInstant::now());
    iface.update_ip_addrs(|addrs| {
        let cidr = IpCidr::Ipv4(Ipv4Cidr::new(
            Ipv4Address::from_octets(peer_ip.octets()),
            32,
        ));
        let _ = addrs.push(cidr);
    });
    let mut sockets = SmolSockets::new(Vec::new());
    let rx = SocketBuffer::new(vec![0u8; 4096]);
    let tx = SocketBuffer::new(vec![0u8; 4096]);
    let handle = sockets.add(SmolTcp::new(rx, tx));
    sockets
        .get_mut::<SmolTcp>(handle)
        .listen(listen_port)
        .expect("smoltcp listen");

    let mut shutdown_after_drain = false;
    let mut pending_echo: VecDeque<Bytes> = VecDeque::new();
    let mut pending_echo_offset = 0usize;

    loop {
        let now = SmolInstant::now();
        let _ = iface.poll(now, &mut device, &mut sockets);

        while let Some(pkt) = device.tx_queue.pop_front() {
            if egress_tx.send(pkt).await.is_err() {
                return;
            }
        }

        // Echo back bytes if any.
        let (state, can_recv, can_send) = {
            let s = sockets.get::<SmolTcp>(handle);
            (s.state(), s.may_recv(), s.may_send())
        };
        if can_recv {
            let s = sockets.get_mut::<SmolTcp>(handle);
            let mut data: Vec<u8> = Vec::new();
            if s.recv_queue() > 0 {
                let _ = s.recv(|b| {
                    data.extend_from_slice(b);
                    (b.len(), ())
                });
            }
            if !data.is_empty() {
                pending_echo.push_back(Bytes::from(data));
            }
        }
        if can_send {
            let s = sockets.get_mut::<SmolTcp>(handle);
            while let Some(bytes) = pending_echo.front() {
                let remaining = &bytes[pending_echo_offset..];
                if remaining.is_empty() {
                    pending_echo.pop_front();
                    pending_echo_offset = 0;
                    continue;
                }
                match s.send_slice(remaining) {
                    Ok(0) => break,
                    Ok(n) => {
                        pending_echo_offset += n;
                        if pending_echo_offset >= bytes.len() {
                            pending_echo.pop_front();
                            pending_echo_offset = 0;
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        // If the peer FIN-ed us, close our side once recv drains.
        if !can_recv && can_send && sockets.get::<SmolTcp>(handle).recv_queue() == 0 {
            sockets.get_mut::<SmolTcp>(handle).close();
            shutdown_after_drain = true;
        }
        if shutdown_after_drain && matches!(state, SmolState::Closed | SmolState::TimeWait) {
            return;
        }

        let next = iface.poll_at(now, &sockets);
        let wait = match next {
            Some(at) if at > now => Duration::from_micros((at - now).total_micros() as u64)
                .max(Duration::from_millis(20)),
            _ => Duration::from_millis(20),
        };
        let deadline = tokio::time::Instant::now() + wait.min(Duration::from_secs(2));

        tokio::select! {
            biased;
            recv = ingress_rx.recv() => match recv {
                Some(pkt) => device.rx_queue.push_back(pkt),
                None => return,
            },
            _ = sleep_until(deadline) => {}
        }
    }
}

async fn smoltcp_close_peer(
    peer_ip: Ipv4Addr,
    listen_port: u16,
    mut ingress_rx: mpsc::Receiver<Bytes>,
    egress_tx: mpsc::Sender<Bytes>,
) {
    let mut device = PeerDevice {
        rx_queue: VecDeque::new(),
        tx_queue: VecDeque::new(),
    };
    let cfg = SmolConfig::new(HardwareAddress::Ip);
    let mut iface = SmolIface::new(cfg, &mut device, SmolInstant::now());
    iface.update_ip_addrs(|addrs| {
        let cidr = IpCidr::Ipv4(Ipv4Cidr::new(
            Ipv4Address::from_octets(peer_ip.octets()),
            32,
        ));
        let _ = addrs.push(cidr);
    });
    let mut sockets = SmolSockets::new(Vec::new());
    let rx = SocketBuffer::new(vec![0u8; 4096]);
    let tx = SocketBuffer::new(vec![0u8; 4096]);
    let handle = sockets.add(SmolTcp::new(rx, tx));
    sockets
        .get_mut::<SmolTcp>(handle)
        .listen(listen_port)
        .expect("smoltcp listen");

    let mut close_requested = false;
    loop {
        let now = SmolInstant::now();
        let _ = iface.poll(now, &mut device, &mut sockets);

        while let Some(pkt) = device.tx_queue.pop_front() {
            if egress_tx.send(pkt).await.is_err() {
                return;
            }
        }

        let state = sockets.get::<SmolTcp>(handle).state();
        if !close_requested && state == SmolState::Established {
            sockets.get_mut::<SmolTcp>(handle).close();
            close_requested = true;
        }
        if close_requested && matches!(state, SmolState::Closed | SmolState::TimeWait) {
            return;
        }

        let next = iface.poll_at(now, &sockets);
        let wait = match next {
            Some(at) if at > now => Duration::from_micros((at - now).total_micros() as u64)
                .max(Duration::from_millis(20)),
            _ => Duration::from_millis(20),
        };
        let deadline = tokio::time::Instant::now() + wait.min(Duration::from_secs(2));

        tokio::select! {
            biased;
            recv = ingress_rx.recv() => match recv {
                Some(pkt) => device.rx_queue.push_back(pkt),
                None => return,
            },
            _ = sleep_until(deadline) => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_via_endpoint_tcp_round_trip() {
    // 1. Set up channels:
    //    - adapter egress (encrypt_tx) -> we forward into peer.ingress
    //    - peer egress -> we forward into adapter.local_recv
    let (encrypt_tx, mut encrypt_rx) = mpsc::channel::<Bytes>(64);
    let (_default_tx, default_rx) = mpsc::channel::<Bytes>(1);
    let (local_tx, local_rx) = mpsc::channel::<Bytes>(64);
    let adapter_ip = Ipv4Addr::new(10, 66, 0, 2);
    let peer_ip = Ipv4Addr::new(1, 1, 1, 1);
    let peer_port: u16 = 53;
    let endpoint = Arc::new(FakeEndpoint {
        id: "wg-out".into(),
        encrypt_tx,
        default_rx: Mutex::new(Some(default_rx)),
        local_rx: Mutex::new(Some(local_rx)),
        interface: IpNet::V4(Ipv4Net::new(adapter_ip, 32).unwrap()),
    });

    // 2. Spawn smoltcp echo peer and the glue forwarders.
    let (peer_ingress_tx, peer_ingress_rx) = mpsc::channel::<Bytes>(64);
    let (peer_egress_tx, mut peer_egress_rx) = mpsc::channel::<Bytes>(64);
    tokio::spawn(smoltcp_echo_peer(
        peer_ip,
        peer_port,
        peer_ingress_rx,
        peer_egress_tx,
    ));
    tokio::spawn(async move {
        while let Some(pkt) = encrypt_rx.recv().await {
            if peer_ingress_tx.send(pkt).await.is_err() {
                return;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(pkt) = peer_egress_rx.recv().await {
            if local_tx.send(pkt).await.is_err() {
                return;
            }
        }
    });

    // 3. Adapter dial(Tcp) and echo bytes.
    let adapter = _EOA::arc(
        logger("endpoint-outbound/wg-out"),
        "wg-out".into(),
        endpoint.clone() as Arc<dyn Endpoint>,
    );
    let dst = SocksAddr::ip(IpAddr::V4(peer_ip), peer_port);
    let payload = b"hammer-endpoint-tcp-echo";
    let mut stream = adapter
        .dial(Network::Tcp, dst, payload)
        .await
        .expect("adapter TCP dial");

    // Read echo back.
    let mut got = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut got))
        .await
        .expect("read_exact timed out")
        .expect("read_exact failed");
    assert_eq!(&got[..], payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_via_endpoint_tcp_remote_close_returns_eof() {
    let (encrypt_tx, mut encrypt_rx) = mpsc::channel::<Bytes>(64);
    let (_default_tx, default_rx) = mpsc::channel::<Bytes>(1);
    let (local_tx, local_rx) = mpsc::channel::<Bytes>(64);
    let adapter_ip = Ipv4Addr::new(10, 66, 0, 2);
    let peer_ip = Ipv4Addr::new(1, 1, 1, 1);
    let peer_port: u16 = 53;
    let endpoint = Arc::new(FakeEndpoint {
        id: "wg-out".into(),
        encrypt_tx,
        default_rx: Mutex::new(Some(default_rx)),
        local_rx: Mutex::new(Some(local_rx)),
        interface: IpNet::V4(Ipv4Net::new(adapter_ip, 32).unwrap()),
    });

    let (peer_ingress_tx, peer_ingress_rx) = mpsc::channel::<Bytes>(64);
    let (peer_egress_tx, mut peer_egress_rx) = mpsc::channel::<Bytes>(64);
    tokio::spawn(smoltcp_close_peer(
        peer_ip,
        peer_port,
        peer_ingress_rx,
        peer_egress_tx,
    ));
    tokio::spawn(async move {
        while let Some(pkt) = encrypt_rx.recv().await {
            if peer_ingress_tx.send(pkt).await.is_err() {
                return;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(pkt) = peer_egress_rx.recv().await {
            if local_tx.send(pkt).await.is_err() {
                return;
            }
        }
    });

    let adapter = _EOA::arc(
        logger("endpoint-outbound/wg-out"),
        "wg-out".into(),
        endpoint.clone() as Arc<dyn Endpoint>,
    );
    let dst = SocksAddr::ip(IpAddr::V4(peer_ip), peer_port);
    let mut stream = adapter
        .dial(Network::Tcp, dst, &[])
        .await
        .expect("adapter TCP dial");

    let mut got = [0u8; 1];
    let err = tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut got))
        .await
        .expect("read_exact must not hang after remote close")
        .expect_err("remote close before data should report EOF");
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_via_endpoint_tcp_large_write_round_trip() {
    let (encrypt_tx, mut encrypt_rx) = mpsc::channel::<Bytes>(64);
    let (_default_tx, default_rx) = mpsc::channel::<Bytes>(1);
    let (local_tx, local_rx) = mpsc::channel::<Bytes>(64);
    let adapter_ip = Ipv4Addr::new(10, 66, 0, 2);
    let peer_ip = Ipv4Addr::new(1, 1, 1, 1);
    let peer_port: u16 = 53;
    let endpoint = Arc::new(FakeEndpoint {
        id: "wg-out".into(),
        encrypt_tx,
        default_rx: Mutex::new(Some(default_rx)),
        local_rx: Mutex::new(Some(local_rx)),
        interface: IpNet::V4(Ipv4Net::new(adapter_ip, 32).unwrap()),
    });

    let (peer_ingress_tx, peer_ingress_rx) = mpsc::channel::<Bytes>(64);
    let (peer_egress_tx, mut peer_egress_rx) = mpsc::channel::<Bytes>(64);
    tokio::spawn(smoltcp_echo_peer(
        peer_ip,
        peer_port,
        peer_ingress_rx,
        peer_egress_tx,
    ));
    tokio::spawn(async move {
        while let Some(pkt) = encrypt_rx.recv().await {
            if peer_ingress_tx.send(pkt).await.is_err() {
                return;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(pkt) = peer_egress_rx.recv().await {
            if local_tx.send(pkt).await.is_err() {
                return;
            }
        }
    });

    let adapter = _EOA::arc(
        logger("endpoint-outbound/wg-out"),
        "wg-out".into(),
        endpoint.clone() as Arc<dyn Endpoint>,
    );
    let dst = SocksAddr::ip(IpAddr::V4(peer_ip), peer_port);
    let mut stream = adapter
        .dial(Network::Tcp, dst, &[])
        .await
        .expect("adapter TCP dial");

    let payload = vec![0x5a; 32 * 1024];
    stream.write_all(&payload).await.expect("stream write");
    stream.flush().await.expect("stream flush");

    let mut got = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut got))
        .await
        .expect("read_exact timed out")
        .expect("read_exact failed");
    assert_eq!(got, payload);
}
