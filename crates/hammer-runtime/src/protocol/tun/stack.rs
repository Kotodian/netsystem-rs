use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener as StdTcpListener};
use std::ops::Range;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant as StdInstant;
use tracing::{debug, error, info, trace};

use async_trait::async_trait;
use bytes::Bytes;
#[cfg(feature = "endpoint")]
use etherparse::{
    Icmpv4Header, Icmpv4Type, Icmpv6Header, Icmpv6Type, IpNumber, Ipv4HeaderSlice, icmpv4,
};
#[cfg(feature = "endpoint")]
use hammer_adapter::Endpoint as EndpointTrait;
use hammer_adapter::{
    DnsQueryOptions, DnsRouter as DnsRouterTrait, Network, OutboundManager as OutboundManagerTrait,
    ProxyStream, RouteDecision, RouteMetadata, RouteTarget, Router as RouterTrait, SocksAddr,
};
use hammer_core::config::normalize_domain;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::log::Logger;
use hickory_proto::op::Message;
use ipnet::IpNet;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, copy_bidirectional_with_sizes,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant, timeout};

use hammer_core::metrics::{MetricsRegistry, MetricsScope, NetworkCounters, RegistryRecorder};
use hammer_core::protocol::dns::MessageExt;
use metrics::{Counter, Key, Metadata, Recorder};

const TUN_READ_HEADROOM: usize = 128;
const MAX_TUN_PACKET_SIZE: usize = 65_535;
pub(crate) const SYSTEM_TUN_RECV_BATCH_HINT: usize = 256;
const SYSTEM_UDP_FLOW_CAPACITY: usize = 256;
const SYSTEM_UDP_CHANNEL_CAPACITY: usize = 64;
const SYSTEM_DNS_HIJACK_QUEUE_CAPACITY: usize = 64;
const SYSTEM_DNS_HIJACK_WORKER_QUEUE_CAPACITY: usize = 1;
const SYSTEM_DNS_HIJACK_WORKERS: usize = 4;
const SYSTEM_ICMP_QUEUE_CAPACITY: usize = 32;
const SYSTEM_ICMP_WORKERS: usize = 2;
const SYSTEM_TUN_CONTROL_WRITE_QUEUE_CAPACITY: usize = 64;
const SYSTEM_TCP_PENDING_DIAL_CAPACITY: usize = 64;
const SYSTEM_TCP_BRIDGE_BUFFER_SIZE: usize = 64 * 1024;
const DEFAULT_SYSTEM_UDP_TIMEOUT: Duration = Duration::from_secs(30);

enum TunWriteItem {
    Packet(Vec<u8>),
    Batch(Vec<Vec<u8>>),
}

const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const TCP_HEADER_MIN_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;
const IPV4_TOTAL_LENGTH_OFFSET: usize = 2;
const IPV4_TTL_OFFSET: usize = 8;
const IPV4_PROTOCOL_OFFSET: usize = 9;
const IPV4_CHECKSUM_OFFSET: usize = 10;
#[cfg(feature = "endpoint")]
const IPV4_FLAGS_FRAGMENT_OFFSET: usize = 6;
const IPV4_SOURCE_OFFSET: usize = 12;
const IPV4_DESTINATION_OFFSET: usize = 16;
const IPV6_PAYLOAD_LEN_OFFSET: usize = 4;
const IPV6_PROTOCOL_OFFSET: usize = 6;
const IPV6_HOP_LIMIT_OFFSET: usize = 7;
const IPV6_SOURCE_OFFSET: usize = 8;
const IPV6_DESTINATION_OFFSET: usize = 24;
const TCP_SOURCE_PORT_OFFSET: usize = 0;
const TCP_DESTINATION_PORT_OFFSET: usize = 2;
const TCP_DATA_OFFSET_OFFSET: usize = 12;
const TCP_CHECKSUM_OFFSET: usize = 16;
const UDP_SOURCE_PORT_OFFSET: usize = 0;
const UDP_DESTINATION_PORT_OFFSET: usize = 2;
const UDP_LENGTH_OFFSET: usize = 4;
const UDP_CHECKSUM_OFFSET: usize = 6;
const DEFAULT_PACKET_TTL: u8 = 64;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpVersion {
    V4 = 4,
    V6 = 6,
}

impl IpVersion {
    fn wire_value(self) -> u8 {
        self as u8
    }

    fn from_packet(packet: &[u8]) -> HammerResult<Self> {
        let Some(first) = packet.first() else {
            return Err(HammerError::internal("empty IP packet"));
        };
        Self::try_from(first >> 4)
    }
}

impl TryFrom<u8> for IpVersion {
    type Error = HammerError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::V4.wire_value() => Ok(Self::V4),
            value if value == Self::V6.wire_value() => Ok(Self::V6),
            other => Err(HammerError::internal(format!(
                "unsupported IP version: {other}"
            ))),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpProtocol {
    Icmpv4 = 1,
    Tcp = 6,
    Udp = 17,
    Icmpv6 = 58,
}

impl IpProtocol {
    fn wire_value(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for IpProtocol {
    type Error = HammerError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::Icmpv4.wire_value() => Ok(Self::Icmpv4),
            value if value == Self::Tcp.wire_value() => Ok(Self::Tcp),
            value if value == Self::Udp.wire_value() => Ok(Self::Udp),
            value if value == Self::Icmpv6.wire_value() => Ok(Self::Icmpv6),
            other => Err(HammerError::internal(format!(
                "unsupported transport protocol: {other}"
            ))),
        }
    }
}

#[async_trait]
pub trait TunDevice: Send + Sync + 'static {
    async fn recv(&self) -> HammerResult<Vec<u8>>;
    async fn send(&self, packet: Vec<u8>) -> HammerResult<()>;
    /// Drain up to `max` packets from the device in one go. The default impl
    /// just calls `recv` once; Apple's batched driver overrides this to
    /// amortize the recvmsg_x syscall over many packets.
    async fn recv_batch(&self, max: usize) -> HammerResult<Vec<Vec<u8>>> {
        let _ = max;
        let packet = self.recv().await?;
        Ok(vec![packet])
    }
    /// Send a batch of packets back to the kernel. Apple's batched driver
    /// overrides this to coalesce them into one sendmsg_x syscall; everywhere
    /// else we fall back to a sequential `send` loop.
    async fn send_batch(&self, packets: &mut Vec<Vec<u8>>) -> HammerResult<()> {
        for packet in packets.drain(..) {
            self.send(packet).await?;
        }
        Ok(())
    }
    fn close(&self);
}

#[cfg(any(test, target_os = "macos", target_os = "ios", target_os = "tvos"))]
pub(crate) fn is_transient_tun_send_backpressure(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::ENOBUFS) | Some(libc::ENOSPC))
}

#[cfg(any(test, target_os = "macos", target_os = "ios", target_os = "tvos"))]
pub(crate) fn should_clear_tun_send_readiness(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::WouldBlock
}

pub struct MemoryTunDevice {
    input_tx: mpsc::Sender<Vec<u8>>,
    input_rx: Mutex<mpsc::Receiver<Vec<u8>>>,
    output_tx: mpsc::Sender<Vec<u8>>,
    output_rx: Mutex<mpsc::Receiver<Vec<u8>>>,
    closed: AtomicBool,
}

impl MemoryTunDevice {
    pub fn new() -> Arc<Self> {
        let (input_tx, input_rx) = mpsc::channel(32);
        let (output_tx, output_rx) = mpsc::channel(32);
        Arc::new(Self {
            input_tx,
            input_rx: Mutex::new(input_rx),
            output_tx,
            output_rx: Mutex::new(output_rx),
            closed: AtomicBool::new(false),
        })
    }

    pub async fn inject(&self, packet: Vec<u8>) -> HammerResult<()> {
        self.input_tx
            .send(packet)
            .await
            .map_err(|_| HammerError::internal("memory tun input closed"))
    }

    pub async fn take_output(&self) -> Option<Vec<u8>> {
        self.output_rx.lock().await.recv().await
    }

    pub async fn recv(&self) -> HammerResult<Vec<u8>> {
        <Self as TunDevice>::recv(self).await
    }

    pub async fn send(&self, packet: Vec<u8>) -> HammerResult<()> {
        <Self as TunDevice>::send(self, packet).await
    }

    pub fn close(&self) {
        <Self as TunDevice>::close(self);
    }
}

#[async_trait]
impl TunDevice for MemoryTunDevice {
    async fn recv(&self) -> HammerResult<Vec<u8>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(HammerError::internal("memory tun closed"));
        }
        self.input_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| HammerError::internal("memory tun input closed"))
    }

    async fn send(&self, packet: Vec<u8>) -> HammerResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(HammerError::internal("memory tun closed"));
        }
        self.output_tx
            .send(packet)
            .await
            .map_err(|_| HammerError::internal("memory tun output closed"))
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }
}

pub struct AsyncTunDevice {
    device: tun_rs::AsyncDevice,
    read_buffer_len: usize,
    closed: AtomicBool,
}

impl AsyncTunDevice {
    /// # Safety
    ///
    /// `fd` must be an owned TUN/utun file descriptor. The returned device closes
    /// that descriptor when dropped.
    pub unsafe fn from_fd(fd: RawFd, mtu: usize) -> HammerResult<Arc<Self>> {
        let read_buffer_len = tun_read_buffer_len(mtu)?;
        let device = unsafe { tun_rs::AsyncDevice::from_fd(fd) }
            .map_err(|err| HammerError::internal(format!("wrap TUN fd: {err}")))?;
        Ok(Arc::new(Self {
            device,
            read_buffer_len,
            closed: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl TunDevice for AsyncTunDevice {
    async fn recv(&self) -> HammerResult<Vec<u8>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(HammerError::internal("TUN device closed"));
        }
        let mut packet = vec![0_u8; self.read_buffer_len];
        let len = self
            .device
            .recv(&mut packet)
            .await
            .map_err(|err| HammerError::internal(format!("read TUN packet: {err}")))?;
        packet.truncate(len);
        Ok(packet)
    }

    async fn send(&self, packet: Vec<u8>) -> HammerResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(HammerError::internal("TUN device closed"));
        }
        self.device
            .send(&packet)
            .await
            .map_err(|err| HammerError::internal(format!("write TUN packet: {err}")))?;
        Ok(())
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }
}

fn tun_read_buffer_len(mtu: usize) -> HammerResult<usize> {
    if mtu == 0 {
        return Err(HammerError::internal("TUN MTU must be greater than zero"));
    }
    if mtu > MAX_TUN_PACKET_SIZE {
        return Err(HammerError::internal(format!(
            "TUN MTU {mtu} exceeds max IP packet size {MAX_TUN_PACKET_SIZE}"
        )));
    }
    mtu.checked_add(TUN_READ_HEADROOM)
        .ok_or_else(|| HammerError::internal("TUN read buffer size overflow"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedIpPacket {
    pub network: Network,
    pub source: SocksAddr,
    pub destination: SocksAddr,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedIpPacketView {
    network: Network,
    source: SocksAddr,
    destination: SocksAddr,
    payload_range: Range<usize>,
}

impl ParsedIpPacketView {
    fn payload<'a>(&self, packet: &'a [u8]) -> HammerResult<&'a [u8]> {
        packet
            .get(self.payload_range.clone())
            .ok_or_else(|| HammerError::internal("parsed packet payload range is out of bounds"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunPacket {
    pub metadata: RouteMetadata,
    pub payload: Vec<u8>,
}

impl TunPacket {
    pub fn for_test(network: Network, destination_port: u16, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            metadata: RouteMetadata {
                network,
                destination: Some(SocksAddr::ip(
                    IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
                    destination_port,
                )),
                ..Default::default()
            },
            payload: payload.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacketFlow {
    pub metadata: RouteMetadata,
    pub decision: RouteDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemTcpSession {
    pub source: SocksAddr,
    pub destination: SocksAddr,
    last_active: StdInstant,
    active_connections: usize,
}

#[derive(Debug)]
pub struct SystemTcpNat {
    next_port: u16,
    by_flow: HashMap<(IpAddr, u16, IpAddr, u16), u16>,
    by_port: HashMap<u16, SystemTcpSession>,
    timeout: Duration,
}

impl Default for SystemTcpNat {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemTcpNat {
    pub fn new() -> Self {
        Self::new_with_timeout(DEFAULT_SYSTEM_UDP_TIMEOUT)
    }

    pub fn new_with_timeout(timeout: Duration) -> Self {
        Self {
            next_port: 10_000,
            by_flow: HashMap::new(),
            by_port: HashMap::new(),
            timeout,
        }
    }

    pub fn lookup_back(&mut self, port: u16) -> Option<SystemTcpSession> {
        self.cleanup_expired();
        let session = self.by_port.get_mut(&port)?;
        session.last_active = StdInstant::now();
        Some(session.clone())
    }

    fn claim_active(&mut self, port: u16) -> Option<SystemTcpSession> {
        self.cleanup_expired();
        let session = self.by_port.get_mut(&port)?;
        session.last_active = StdInstant::now();
        session.active_connections += 1;
        Some(session.clone())
    }

    fn release_active(&mut self, port: u16) {
        if let Some(session) = self.by_port.get_mut(&port) {
            session.active_connections = session.active_connections.saturating_sub(1);
            session.last_active = StdInstant::now();
        }
        self.cleanup_expired();
    }

    fn lookup_or_insert(&mut self, source: SocksAddr, destination: SocksAddr) -> u16 {
        self.cleanup_expired();
        let key = (source.host, source.port, destination.host, destination.port);
        if let Some(port) = self.by_flow.get(&key) {
            if let Some(session) = self.by_port.get_mut(port) {
                session.last_active = StdInstant::now();
            }
            return *port;
        }
        let port = self.next_port;
        self.next_port = if self.next_port == u16::MAX {
            10_000
        } else {
            self.next_port + 1
        };
        self.by_flow.insert(key, port);
        self.by_port.insert(
            port,
            SystemTcpSession {
                source,
                destination,
                last_active: StdInstant::now(),
                active_connections: 0,
            },
        );
        port
    }

    fn cleanup_expired(&mut self) {
        let now = StdInstant::now();
        let timeout = self.timeout;
        self.by_port.retain(|_, session| {
            session.active_connections > 0 || now.duration_since(session.last_active) <= timeout
        });
        self.by_flow
            .retain(|_, port| self.by_port.contains_key(port));
    }
}

struct SystemTcpNatLease {
    nat: Arc<StdMutex<SystemTcpNat>>,
    port: u16,
}

impl SystemTcpNatLease {
    fn new(nat: Arc<StdMutex<SystemTcpNat>>, port: u16) -> Self {
        Self { nat, port }
    }
}

impl Drop for SystemTcpNatLease {
    fn drop(&mut self) {
        if let Ok(mut nat) = self.nat.lock() {
            nat.release_active(self.port);
        }
    }
}

struct SystemTcpInboundGuard {
    stream: TcpStream,
    close_on_drop: bool,
}

impl SystemTcpInboundGuard {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            close_on_drop: true,
        }
    }

    fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    fn disarm(&mut self) {
        self.close_on_drop = false;
    }
}

impl Drop for SystemTcpInboundGuard {
    fn drop(&mut self) {
        if self.close_on_drop {
            close_system_tcp_stream(&self.stream);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UdpFlowKey {
    source: (IpAddr, u16),
    destination: (IpAddr, u16),
}

impl UdpFlowKey {
    fn from_parsed(parsed: &ParsedIpPacketView) -> Self {
        Self {
            source: (parsed.source.host, parsed.source.port),
            destination: (parsed.destination.host, parsed.destination.port),
        }
    }
}

struct UdpFlowState {
    sender: mpsc::Sender<UdpFlowPayload>,
    last_active: Instant,
    outbound: String,
}

type UdpFlowMap = HashMap<UdpFlowKey, UdpFlowState>;

type UdpFlowPayload = Bytes;

#[cfg(feature = "endpoint")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TcpEndpointFlowKey {
    source: (IpAddr, u16),
    destination: (IpAddr, u16),
}

#[cfg(feature = "endpoint")]
impl TcpEndpointFlowKey {
    fn from_parsed(parsed: &ParsedIpPacketView) -> Self {
        Self {
            source: (parsed.source.host, parsed.source.port),
            destination: (parsed.destination.host, parsed.destination.port),
        }
    }
}

#[cfg(feature = "endpoint")]
struct TcpEndpointFlowState {
    endpoint: String,
    last_active: Instant,
}

#[cfg(feature = "endpoint")]
type TcpEndpointFlowMap = HashMap<TcpEndpointFlowKey, TcpEndpointFlowState>;

struct DnsHijackJob {
    packet: Vec<u8>,
    destination: SocksAddr,
    message: Message,
    options: DnsQueryOptions,
}

struct IcmpJob {
    packet: Vec<u8>,
    parsed: ParsedIpPacketView,
}

#[derive(Clone)]
struct TcpPendingDialLimiter {
    semaphore: Arc<Semaphore>,
}

impl TcpPendingDialLimiter {
    fn new(limit: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
        }
    }

    fn try_acquire(&self) -> HammerResult<OwnedSemaphorePermit> {
        self.semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| HammerError::internal("pending outbound TCP dial limit reached"))
    }
}

/// L3 fast path dispatcher.
///
/// Each registered `Endpoint` (today: WireGuard) exposes the IP prefixes it
/// owns via `Endpoint::allowed_destinations`. The TUN packet loop builds a
/// longest-prefix table from those at start-up and consults it before the
/// system NAT pass: when the packet's destination IP falls inside one of the
/// prefixes, the raw IP packet is shipped straight into that endpoint's
/// encrypt channel — no NAT rewrite, no listener round-trip.
///
/// The inbound IP packet streams (decapsulated by the endpoint) are taken at
/// start-up and fanned into the TUN writer by a dedicated task per stream.
#[cfg(feature = "endpoint")]
#[derive(Clone, Copy)]
struct L3AddressRewrite {
    tun: IpAddr,
    endpoint: IpAddr,
}

#[cfg(feature = "endpoint")]
#[derive(Clone, Copy, Default)]
struct L3AddressRewrites {
    v4: Option<L3AddressRewrite>,
    v6: Option<L3AddressRewrite>,
}

#[cfg(feature = "endpoint")]
struct L3EndpointRoute {
    id: String,
    allowed: Vec<IpNet>,
    tx: mpsc::Sender<Bytes>,
    batch_tx: Option<mpsc::Sender<Vec<Bytes>>>,
    rewrite: L3AddressRewrites,
    mtu: Option<usize>,
}

#[cfg(feature = "endpoint")]
struct L3InboundReceiver {
    rx: Option<mpsc::Receiver<Bytes>>,
    batch_rx: Option<mpsc::Receiver<Vec<Bytes>>>,
    rewrite: L3AddressRewrites,
}

#[cfg(feature = "endpoint")]
struct L3EndpointQueuedBatch {
    tx: mpsc::Sender<Bytes>,
    batch_tx: Option<mpsc::Sender<Vec<Bytes>>>,
    packets: Vec<Bytes>,
}

#[cfg(feature = "endpoint")]
type L3EndpointQueuedBatches = HashMap<String, L3EndpointQueuedBatch>;

#[cfg(feature = "endpoint")]
pub(crate) struct L3DispatchTable {
    endpoints: Vec<L3EndpointRoute>,
    inbound: StdMutex<Vec<L3InboundReceiver>>,
}

#[cfg(feature = "endpoint")]
impl L3DispatchTable {
    fn from_endpoints(eps: &[Arc<dyn EndpointTrait>], addresses: &StackAddresses) -> Self {
        let mut endpoints = Vec::new();
        let mut inbound = Vec::new();
        for ep in eps {
            let rewrite = L3AddressRewrites::from_endpoint(addresses, &ep.interface_addresses());
            let tx = ep.ip_send_clone();
            let batch_tx = ep.ip_send_batch_clone();
            let mtu = ep.ip_packet_mtu();
            let mut allowed = ep.allowed_destinations();
            allowed.sort_by(|a, b| b.prefix_len().cmp(&a.prefix_len()));
            allowed.dedup();
            if !allowed.is_empty() {
                endpoints.push(L3EndpointRoute {
                    id: ep.id().to_owned(),
                    allowed,
                    tx,
                    batch_tx,
                    rewrite,
                    mtu,
                });
            }
            let rx = ep.ip_recv_take();
            let batch_rx = ep.ip_recv_batch_take();
            if rx.is_some() || batch_rx.is_some() {
                inbound.push(L3InboundReceiver {
                    rx,
                    batch_rx,
                    rewrite,
                });
            }
        }
        Self {
            endpoints,
            inbound: StdMutex::new(inbound),
        }
    }

    #[inline]
    fn match_endpoint(&self, id: &str, dst: IpAddr) -> Option<&L3EndpointRoute> {
        self.endpoints.iter().find(|endpoint| {
            endpoint.id == id && endpoint.allowed.iter().any(|net| net.contains(&dst))
        })
    }

    fn take_inbound_receivers(&self) -> Vec<L3InboundReceiver> {
        std::mem::take(
            &mut *self
                .inbound
                .lock()
                .expect("L3DispatchTable inbound poisoned"),
        )
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
            && self
                .inbound
                .lock()
                .expect("L3DispatchTable inbound poisoned")
                .is_empty()
    }
}

#[cfg(feature = "endpoint")]
impl L3AddressRewrites {
    fn from_endpoint(addresses: &StackAddresses, endpoint_addresses: &[IpNet]) -> Self {
        let v4 = addresses.v4.as_ref().and_then(|tun| {
            first_endpoint_ip(endpoint_addresses, IpVersion::V4).map(|endpoint| L3AddressRewrite {
                tun: IpAddr::V4(tun.listener),
                endpoint,
            })
        });
        let v6 = addresses.v6.as_ref().and_then(|tun| {
            first_endpoint_ip(endpoint_addresses, IpVersion::V6).map(|endpoint| L3AddressRewrite {
                tun: IpAddr::V6(tun.listener),
                endpoint,
            })
        });
        Self { v4, v6 }
    }

    fn for_addr(&self, addr: IpAddr) -> Option<L3AddressRewrite> {
        match addr {
            IpAddr::V4(_) => self.v4,
            IpAddr::V6(_) => self.v6,
        }
    }

    fn rewrite_egress(&self, packet: &mut [u8], parsed: &ParsedIpPacketView) -> HammerResult<()> {
        let Some(rewrite) = self.for_addr(parsed.source.host) else {
            return Ok(());
        };
        rewrite_l3_packet_source(packet, rewrite.tun, rewrite.endpoint)
    }

    fn rewrite_ingress(&self, packet: &mut [u8]) -> HammerResult<()> {
        let destination = l3_packet_addr(packet, false)?;
        let Some(rewrite) = self.for_addr(destination) else {
            return Ok(());
        };
        rewrite_l3_packet_destination(packet, rewrite.endpoint, rewrite.tun)
    }
}

#[cfg(feature = "endpoint")]
impl L3EndpointRoute {
    fn prepare_packets(
        &self,
        packet: Vec<u8>,
        parsed: &ParsedIpPacketView,
        tun_write_tx: Option<&mpsc::Sender<TunWriteItem>>,
        metrics: &TunMetrics,
    ) -> HammerResult<Vec<Bytes>> {
        if let Some(mtu) = self.mtu {
            if packet.len() > mtu {
                return self.prepare_oversized(packet, parsed, mtu, tun_write_tx, metrics);
            }
        }
        let mut packet = packet;
        self.rewrite.rewrite_egress(&mut packet, parsed)?;
        Ok(vec![Bytes::from(packet)])
    }

    #[cfg(test)]
    fn dispatch(
        &self,
        packet: Vec<u8>,
        parsed: &ParsedIpPacketView,
        tun_write_tx: Option<&mpsc::Sender<TunWriteItem>>,
        metrics: &TunMetrics,
    ) -> HammerResult<()> {
        if let Some(mtu) = self.mtu {
            if packet.len() > mtu {
                return self.dispatch_oversized(packet, parsed, mtu, tun_write_tx, metrics);
            }
        }
        self.dispatch_rewritten(packet, parsed)
    }

    #[cfg(test)]
    fn dispatch_rewritten(
        &self,
        mut packet: Vec<u8>,
        parsed: &ParsedIpPacketView,
    ) -> HammerResult<()> {
        self.rewrite.rewrite_egress(&mut packet, parsed)?;
        self.send_packet(packet)
    }

    #[cfg(test)]
    fn dispatch_oversized(
        &self,
        packet: Vec<u8>,
        parsed: &ParsedIpPacketView,
        mtu: usize,
        tun_write_tx: Option<&mpsc::Sender<TunWriteItem>>,
        metrics: &TunMetrics,
    ) -> HammerResult<()> {
        for packet in self.prepare_oversized(packet, parsed, mtu, tun_write_tx, metrics)? {
            self.send_packet(packet.to_vec())?;
        }
        Ok(())
    }

    fn prepare_oversized(
        &self,
        packet: Vec<u8>,
        parsed: &ParsedIpPacketView,
        mtu: usize,
        tun_write_tx: Option<&mpsc::Sender<TunWriteItem>>,
        metrics: &TunMetrics,
    ) -> HammerResult<Vec<Bytes>> {
        match IpVersion::from_packet(&packet)? {
            IpVersion::V4 if ipv4_dont_fragment(&packet)? => {
                let response = ipv4_packet_too_big_packet(&packet, mtu)?;
                enqueue_endpoint_pmtu_response(tun_write_tx, response, metrics)?;
                Ok(Vec::new())
            }
            IpVersion::V4 => {
                let mut packet = packet;
                self.rewrite.rewrite_egress(&mut packet, parsed)?;
                Ok(fragment_ipv4_packet(&packet, mtu)?
                    .into_iter()
                    .map(Bytes::from)
                    .collect())
            }
            IpVersion::V6 => {
                let response = ipv6_packet_too_big_packet(&packet, mtu)?;
                enqueue_endpoint_pmtu_response(tun_write_tx, response, metrics)?;
                Ok(Vec::new())
            }
        }
    }

    #[cfg(test)]
    fn send_packet(&self, packet: Vec<u8>) -> HammerResult<()> {
        self.tx
            .try_send(Bytes::from(packet))
            .map_err(|err| HammerError::internal(format!("endpoint L3 dispatch failed: {err}")))
    }
}

#[cfg(feature = "endpoint")]
#[cfg(test)]
fn dispatch_endpoint_l3_packet(
    dispatch: &L3DispatchTable,
    endpoint_id: &str,
    packet: Vec<u8>,
    parsed: &ParsedIpPacketView,
    metrics: &TunMetrics,
    tun_write_tx: Option<&mpsc::Sender<TunWriteItem>>,
) -> bool {
    if let Some(endpoint) = dispatch.match_endpoint(endpoint_id, parsed.destination.host) {
        match endpoint.dispatch(packet, parsed, tun_write_tx, metrics) {
            Ok(()) => {
                metrics.counters.endpoint_dispatch_total.increment(1);
            }
            Err(err) => {
                metrics.counters.endpoint_dispatch_drop_total.increment(1);
                debug!("dispatch endpoint L3 packet: {err}");
            }
        }
        true
    } else {
        metrics.counters.endpoint_dispatch_drop_total.increment(1);
        debug!(
            "endpoint route {endpoint_id} does not accept {}",
            parsed.destination.host
        );
        false
    }
}

#[cfg(feature = "endpoint")]
fn queue_endpoint_l3_packet(
    dispatch: &L3DispatchTable,
    endpoint_id: &str,
    packet: Vec<u8>,
    parsed: &ParsedIpPacketView,
    metrics: &TunMetrics,
    tun_write_tx: Option<&mpsc::Sender<TunWriteItem>>,
    batches: &mut L3EndpointQueuedBatches,
) -> bool {
    if let Some(endpoint) = dispatch.match_endpoint(endpoint_id, parsed.destination.host) {
        match endpoint.prepare_packets(packet, parsed, tun_write_tx, metrics) {
            Ok(packets) => {
                metrics.counters.endpoint_dispatch_total.increment(1);
                if !packets.is_empty() {
                    let batch = batches.entry(endpoint.id.clone()).or_insert_with(|| {
                        L3EndpointQueuedBatch {
                            tx: endpoint.tx.clone(),
                            batch_tx: endpoint.batch_tx.clone(),
                            packets: Vec::new(),
                        }
                    });
                    batch.packets.extend(packets);
                }
            }
            Err(err) => {
                metrics.counters.endpoint_dispatch_drop_total.increment(1);
                debug!("dispatch endpoint L3 packet: {err}");
            }
        }
        true
    } else {
        metrics.counters.endpoint_dispatch_drop_total.increment(1);
        debug!(
            "endpoint route {endpoint_id} does not accept {}",
            parsed.destination.host
        );
        false
    }
}

#[cfg(feature = "endpoint")]
async fn flush_endpoint_l3_batches(batches: &mut L3EndpointQueuedBatches, metrics: &TunMetrics) {
    for (_, mut batch) in batches.drain() {
        if batch.packets.is_empty() {
            continue;
        }
        if let Some(batch_tx) = batch.batch_tx {
            if batch_tx.send(batch.packets).await.is_err() {
                metrics.counters.endpoint_dispatch_drop_total.increment(1);
                debug!("dispatch endpoint L3 batch: endpoint batch channel closed");
            }
            continue;
        }
        for packet in batch.packets.drain(..) {
            if batch.tx.send(packet).await.is_err() {
                metrics.counters.endpoint_dispatch_drop_total.increment(1);
                debug!("dispatch endpoint L3 packet: endpoint channel closed");
                break;
            }
        }
    }
}

#[cfg(feature = "endpoint")]
fn enqueue_endpoint_pmtu_response(
    tun_write_tx: Option<&mpsc::Sender<TunWriteItem>>,
    response: Vec<u8>,
    metrics: &TunMetrics,
) -> HammerResult<()> {
    let Some(tun_write_tx) = tun_write_tx else {
        return Err(HammerError::internal(
            "endpoint L3 packet needs PMTU response but TUN writer is unavailable",
        ));
    };
    enqueue_tun_packet_write(tun_write_tx, response, metrics);
    Ok(())
}

#[cfg(feature = "endpoint")]
fn rewrite_endpoint_inbound_packet(
    rewrite: &L3AddressRewrites,
    packet: &mut [u8],
    metrics: &TunMetrics,
) -> HammerResult<()> {
    if let Err(err) = rewrite.rewrite_ingress(packet) {
        metrics.counters.endpoint_dispatch_drop_total.increment(1);
        debug!("rewrite endpoint inbound packet: {err}");
        return Err(err);
    }
    Ok(())
}

#[cfg(feature = "endpoint")]
fn ipv4_header_slice(packet: &[u8]) -> HammerResult<Ipv4HeaderSlice<'_>> {
    let header = Ipv4HeaderSlice::from_slice(packet)
        .map_err(|err| HammerError::internal(format!("invalid IPv4 packet: {err}")))?;
    let total_len = header.total_len() as usize;
    if packet.len() < total_len {
        return Err(HammerError::internal("short IPv4 packet"));
    }
    header
        .payload_len()
        .map_err(|err| HammerError::internal(format!("invalid IPv4 packet length: {err}")))?;
    Ok(header)
}

#[cfg(feature = "endpoint")]
fn ipv4_dont_fragment(packet: &[u8]) -> HammerResult<bool> {
    Ok(ipv4_header_slice(packet)?.dont_fragment())
}

#[cfg(feature = "endpoint")]
fn fragment_ipv4_packet(packet: &[u8], mtu: usize) -> HammerResult<Vec<Vec<u8>>> {
    let header = ipv4_header_slice(packet)?;
    let ihl = header.slice().len();
    let total_len = header.total_len() as usize;
    if total_len <= mtu {
        return Ok(vec![packet[..total_len].to_vec()]);
    }
    if mtu <= ihl {
        return Err(HammerError::internal(format!(
            "endpoint MTU {mtu} cannot fit IPv4 header length {ihl}"
        )));
    }
    let max_payload = ((mtu - ihl) / 8) * 8;
    if max_payload == 0 {
        return Err(HammerError::internal(format!(
            "endpoint MTU {mtu} cannot fit an IPv4 fragment payload"
        )));
    }

    let payload = &packet[ihl..total_len];
    if payload.is_empty() {
        return Ok(vec![packet[..total_len].to_vec()]);
    }

    let original_flags = read_u16(packet, IPV4_FLAGS_FRAGMENT_OFFSET);
    let reserved_flag = original_flags & 0x8000;
    let original_more_fragments = original_flags & 0x2000 != 0;
    let base_fragment_offset = original_flags & 0x1fff;
    let mut fragments = Vec::with_capacity(payload.len().div_ceil(max_payload));
    let mut offset = 0;

    while offset < payload.len() {
        let remaining = payload.len() - offset;
        let take = remaining.min(max_payload);
        let last = offset + take == payload.len();
        let fragment_offset = base_fragment_offset
            .checked_add((offset / 8) as u16)
            .filter(|offset| *offset <= 0x1fff)
            .ok_or_else(|| HammerError::internal("IPv4 fragment offset overflow"))?;
        let flags = reserved_flag
            | if original_more_fragments || !last {
                0x2000
            } else {
                0
            }
            | fragment_offset;
        let fragment_len = ihl + take;
        let mut fragment = Vec::with_capacity(fragment_len);
        fragment.extend_from_slice(&packet[..ihl]);
        fragment.extend_from_slice(&payload[offset..offset + take]);
        write_u16(&mut fragment, IPV4_TOTAL_LENGTH_OFFSET, fragment_len as u16);
        write_u16(&mut fragment, IPV4_FLAGS_FRAGMENT_OFFSET, flags);
        update_ipv4_header_checksum(&mut fragment, ihl);
        fragments.push(fragment);
        offset += take;
    }

    Ok(fragments)
}

#[cfg(feature = "endpoint")]
fn first_endpoint_ip(addresses: &[IpNet], version: IpVersion) -> Option<IpAddr> {
    addresses.iter().find_map(|net| match (version, *net) {
        (IpVersion::V4, IpNet::V4(net)) => Some(IpAddr::V4(net.addr())),
        (IpVersion::V6, IpNet::V6(net)) => Some(IpAddr::V6(net.addr())),
        _ => None,
    })
}

pub struct SystemTunStack<D, R, Q, O>
where
    D: TunDevice,
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    logger: Logger,
    router: Arc<R>,
    dns_router: Arc<Q>,
    outbound: Arc<O>,
    inbound_id: String,
    options: hammer_core::config::TunInboundOptions,
    device: Arc<D>,
    tcp_nat: Arc<StdMutex<SystemTcpNat>>,
    tcp_pending_dials: TcpPendingDialLimiter,
    udp_flows: Arc<StdMutex<UdpFlowMap>>,
    /// L3 endpoints (WireGuard today) registered for the fast path. The
    /// service builder injects these via `set_endpoints` before `start`
    /// so the packet loop can build its dispatch table.
    #[cfg(feature = "endpoint")]
    endpoints: StdMutex<Vec<Arc<dyn EndpointTrait>>>,
    tun_interface_index: Option<u32>,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
    started: AtomicBool,
    metrics: TunMetrics,
}

impl<D, R, Q, O> SystemTunStack<D, R, Q, O>
where
    D: TunDevice,
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    pub fn new(
        logger: Logger,
        router: Arc<R>,
        dns_router: Arc<Q>,
        outbound: Arc<O>,
        inbound_id: String,
        options: hammer_core::config::TunInboundOptions,
        device: Arc<D>,
    ) -> Self {
        let metrics = MetricsRegistry::new().scope("inbound", "tun", inbound_id.clone());
        Self::new_with_interface_index(
            logger, router, dns_router, outbound, inbound_id, options, device, None, metrics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_interface_index(
        logger: Logger,
        router: Arc<R>,
        dns_router: Arc<Q>,
        outbound: Arc<O>,
        inbound_id: String,
        options: hammer_core::config::TunInboundOptions,
        device: Arc<D>,
        tun_interface_index: Option<u32>,
        metrics: MetricsScope,
    ) -> Self {
        let udp_timeout = options.udp_timeout.unwrap_or(DEFAULT_SYSTEM_UDP_TIMEOUT);
        Self {
            logger,
            router,
            dns_router,
            outbound,
            inbound_id,
            options,
            device,
            tcp_nat: Arc::new(StdMutex::new(SystemTcpNat::new_with_timeout(udp_timeout))),
            tcp_pending_dials: TcpPendingDialLimiter::new(SYSTEM_TCP_PENDING_DIAL_CAPACITY),
            udp_flows: Arc::new(StdMutex::new(HashMap::new())),
            #[cfg(feature = "endpoint")]
            endpoints: StdMutex::new(Vec::new()),
            tun_interface_index,
            tasks: StdMutex::new(Vec::new()),
            started: AtomicBool::new(false),
            metrics: TunMetrics::new(metrics),
        }
    }

    /// Register L3 endpoints whose `allowed_destinations` should bypass the
    /// system NAT/listener path. Must be called before `start`; calling after
    /// `start` has no effect (the dispatch table is built at start time).
    #[cfg(feature = "endpoint")]
    pub fn set_endpoints(&self, endpoints: Vec<Arc<dyn EndpointTrait>>) {
        *self
            .endpoints
            .lock()
            .expect("SystemTunStack endpoints poisoned") = endpoints;
    }

    pub fn start(&self) -> HammerResult<()> {
        if self.started.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        let addresses = StackAddresses::from_options(&self.options)?;
        let mut handles = Vec::new();
        let mut routes = SystemStackRoutes::default();
        if let Some(v4) = addresses.v4 {
            let listener = bind_system_listener(IpAddr::V4(v4.listener), self.tun_interface_index)
                .map_err(|err| {
                    HammerError::internal(format!("bind IPv4 system TCP listener: {err}"))
                })?;
            let port = listener
                .local_addr()
                .map_err(|err| HammerError::internal(format!("read IPv4 listener addr: {err}")))?
                .port();
            info!("system stack TCP listener {}:{port}", v4.listener);
            handles.push(crate::spawn::spawn(accept_tcp_loop(
                self.logger.clone(),
                Arc::clone(&self.router),
                Arc::clone(&self.dns_router),
                Arc::clone(&self.outbound),
                Arc::clone(&self.tcp_nat),
                self.tcp_pending_dials.clone(),
                self.inbound_id.clone(),
                listener,
                self.metrics.clone(),
            )));
            routes.v4 = Some(SystemStackRoute {
                listener_addr: IpAddr::V4(v4.listener),
                nat_addr: IpAddr::V4(v4.next),
                listener_port: port,
            });
        }
        if let Some(v6) = addresses.v6 {
            let listener = bind_system_listener(IpAddr::V6(v6.listener), self.tun_interface_index)
                .map_err(|err| {
                    HammerError::internal(format!("bind IPv6 system TCP listener: {err}"))
                })?;
            let port = listener
                .local_addr()
                .map_err(|err| HammerError::internal(format!("read IPv6 listener addr: {err}")))?
                .port();
            info!("system stack TCP listener [{}]:{port}", v6.listener);
            handles.push(crate::spawn::spawn(accept_tcp_loop(
                self.logger.clone(),
                Arc::clone(&self.router),
                Arc::clone(&self.dns_router),
                Arc::clone(&self.outbound),
                Arc::clone(&self.tcp_nat),
                self.tcp_pending_dials.clone(),
                self.inbound_id.clone(),
                listener,
                self.metrics.clone(),
            )));
            routes.v6 = Some(SystemStackRoute {
                listener_addr: IpAddr::V6(v6.listener),
                nat_addr: IpAddr::V6(v6.next),
                listener_port: port,
            });
        }
        #[cfg(feature = "endpoint")]
        let endpoint_dispatch = {
            let eps = self
                .endpoints
                .lock()
                .expect("SystemTunStack endpoints poisoned");
            if eps.is_empty() {
                None
            } else {
                let table = L3DispatchTable::from_endpoints(&eps, &addresses);
                if table.is_empty() {
                    None
                } else {
                    Some(Arc::new(table))
                }
            }
        };
        handles.push(crate::spawn::spawn(packet_loop(
            self.logger.clone(),
            Arc::clone(&self.router),
            Arc::clone(&self.dns_router),
            Arc::clone(&self.outbound),
            self.inbound_id.clone(),
            Arc::clone(&self.device),
            Arc::clone(&self.tcp_nat),
            Arc::clone(&self.udp_flows),
            routes,
            #[cfg(feature = "endpoint")]
            endpoint_dispatch,
            self.options
                .udp_timeout
                .unwrap_or(DEFAULT_SYSTEM_UDP_TIMEOUT),
            self.metrics.clone(),
        )));
        self.tasks
            .lock()
            .expect("SystemTunStack tasks poisoned")
            .extend(handles);
        info!("system stack started");
        Ok(())
    }

    pub fn close(&self) {
        self.device.close();
        for task in self
            .tasks
            .lock()
            .expect("SystemTunStack tasks poisoned")
            .drain(..)
        {
            task.abort();
        }
        if let Ok(mut flows) = self.udp_flows.try_lock() {
            flows.clear();
        }
    }
}

/// Plain counter set for the TUN inbound, registered directly against the
/// inbound's [`MetricsScope`]. This keeps every TUN stack instance bound to
/// its own `(module, component_type, component_id)` tuple instead of routing
/// through a process-global recorder.
#[derive(Clone)]
struct TunCounters {
    /// Errors reading a batch of packets from the TUN device.
    packet_recv_error_total: Counter,
    /// Empty packets dropped before parsing.
    packet_drop_empty_total: Counter,
    /// Packets that failed IP header parsing.
    packet_parse_error_total: Counter,
    /// Packets dropped because the destination is not a global unicast address.
    packet_drop_non_global_total: Counter,
    /// TUN write failures during the TCP rewrite batch.
    tcp_writeback_error_total: Counter,
    /// Errors accepting an inbound TCP connection from the system listener.
    tcp_accept_error_total: Counter,
    /// TCP accepts whose source port had no matching NAT entry.
    tcp_unknown_nat_total: Counter,
    /// Failures building the TCP outbound destination address.
    tcp_destination_error_total: Counter,
    /// TCP outbound dials dropped before issuing because of the pending-dial limit.
    tcp_dial_dropped_total: Counter,
    /// TCP outbound dial errors.
    tcp_dial_error_total: Counter,
    /// TCP bidirectional copy errors during the inbound/outbound bridge.
    tcp_copy_error_total: Counter,
    /// UDP route metadata preparation errors.
    udp_route_prepare_error_total: Counter,
    /// DNS hijack path errors (parse / exchange / serialize / write).
    udp_dns_error_total: Counter,
    /// DNS hijack packets dropped because the background task budget is full.
    udp_dns_drop_busy_total: Counter,
    /// Outbound `listen_packet()` errors when establishing a UDP flow.
    udp_listen_error_total: Counter,
    /// UDP flows evicted from the flow map because of capacity pressure.
    udp_flow_evict_total: Counter,
    /// UDP packets dropped because the chosen flow's send queue was full.
    udp_flow_drop_busy_total: Counter,
    /// UDP flow send errors.
    udp_flow_send_error_total: Counter,
    /// UDP flow recv errors.
    udp_flow_recv_error_total: Counter,
    /// UDP response write-back errors.
    udp_flow_response_write_error_total: Counter,
    /// UDP flows torn down by the idle timeout.
    udp_flow_timeout_total: Counter,
    /// ICMP packets dropped because the background task budget is full.
    icmp_drop_busy_total: Counter,
    /// Control packets dropped because the TUN write queue is full.
    control_write_drop_busy_total: Counter,
    /// Control packet writes that failed after leaving the main packet loop.
    control_write_error_total: Counter,
    /// IP packets dispatched to an L3 endpoint (WireGuard) via the fast path.
    #[cfg(feature = "endpoint")]
    endpoint_dispatch_total: Counter,
    /// IP packets dropped because the L3 endpoint's encrypt channel was full
    /// (back-pressure from boringtun) or already closed.
    #[cfg(feature = "endpoint")]
    endpoint_dispatch_drop_total: Counter,
    /// Decrypted IP packets received from an L3 endpoint and forwarded back
    /// to the TUN device.
    #[cfg(feature = "endpoint")]
    endpoint_inbound_total: Counter,
}

impl TunCounters {
    fn new(scope: &MetricsScope) -> Self {
        let recorder = scope.recorder();
        Self {
            packet_recv_error_total: recorder_counter(&recorder, "packet_recv_error_total"),
            packet_drop_empty_total: recorder_counter(&recorder, "packet_drop_empty_total"),
            packet_parse_error_total: recorder_counter(&recorder, "packet_parse_error_total"),
            packet_drop_non_global_total: recorder_counter(
                &recorder,
                "packet_drop_non_global_total",
            ),
            tcp_writeback_error_total: recorder_counter(&recorder, "tcp_writeback_error_total"),
            tcp_accept_error_total: recorder_counter(&recorder, "tcp_accept_error_total"),
            tcp_unknown_nat_total: recorder_counter(&recorder, "tcp_unknown_nat_total"),
            tcp_destination_error_total: recorder_counter(&recorder, "tcp_destination_error_total"),
            tcp_dial_dropped_total: recorder_counter(&recorder, "tcp_dial_dropped_total"),
            tcp_dial_error_total: recorder_counter(&recorder, "tcp_dial_error_total"),
            tcp_copy_error_total: recorder_counter(&recorder, "tcp_copy_error_total"),
            udp_route_prepare_error_total: recorder_counter(
                &recorder,
                "udp_route_prepare_error_total",
            ),
            udp_dns_error_total: recorder_counter(&recorder, "udp_dns_error_total"),
            udp_dns_drop_busy_total: recorder_counter(&recorder, "udp_dns_drop_busy_total"),
            udp_listen_error_total: recorder_counter(&recorder, "udp_listen_error_total"),
            udp_flow_evict_total: recorder_counter(&recorder, "udp_flow_evict_total"),
            udp_flow_drop_busy_total: recorder_counter(&recorder, "udp_flow_drop_busy_total"),
            udp_flow_send_error_total: recorder_counter(&recorder, "udp_flow_send_error_total"),
            udp_flow_recv_error_total: recorder_counter(&recorder, "udp_flow_recv_error_total"),
            udp_flow_response_write_error_total: recorder_counter(
                &recorder,
                "udp_flow_response_write_error_total",
            ),
            udp_flow_timeout_total: recorder_counter(&recorder, "udp_flow_timeout_total"),
            icmp_drop_busy_total: recorder_counter(&recorder, "icmp_drop_busy_total"),
            control_write_drop_busy_total: recorder_counter(
                &recorder,
                "control_write_drop_busy_total",
            ),
            control_write_error_total: recorder_counter(&recorder, "control_write_error_total"),
            #[cfg(feature = "endpoint")]
            endpoint_dispatch_total: recorder_counter(&recorder, "endpoint_dispatch_total"),
            #[cfg(feature = "endpoint")]
            endpoint_dispatch_drop_total: recorder_counter(
                &recorder,
                "endpoint_dispatch_drop_total",
            ),
            #[cfg(feature = "endpoint")]
            endpoint_inbound_total: recorder_counter(&recorder, "endpoint_inbound_total"),
        }
    }
}

#[derive(Clone)]
struct TunMetrics {
    counters: TunCounters,
    /// Route lookup failures, partitioned by network. Replaces the
    /// historical `tcp_route_error_total` / `udp_route_error_total`
    /// twins.
    route_error_total: NetworkCounters,
    /// Reject-rule hits, partitioned by network.
    reject_total: NetworkCounters,
    /// Outbound id lookups that failed (config drift / missing
    /// outbound), partitioned by network.
    outbound_missing_total: NetworkCounters,
}

impl TunMetrics {
    fn new(scope: MetricsScope) -> Self {
        Self {
            counters: TunCounters::new(&scope),
            route_error_total: NetworkCounters::new(&scope, "route_error_total"),
            reject_total: NetworkCounters::new(&scope, "reject_total"),
            outbound_missing_total: NetworkCounters::new(&scope, "outbound_missing_total"),
        }
    }
}

fn recorder_counter(recorder: &RegistryRecorder, name: &str) -> Counter {
    let key = Key::from_name(name.to_owned());
    let metadata = Metadata::new("tun", metrics::Level::INFO, Some(module_path!()));
    recorder.register_counter(&key, &metadata)
}

#[derive(Debug, Clone, Default)]
struct SystemStackRoutes {
    v4: Option<SystemStackRoute>,
    v6: Option<SystemStackRoute>,
}

#[derive(Debug, Clone)]
struct SystemStackRoute {
    listener_addr: IpAddr,
    nat_addr: IpAddr,
    listener_port: u16,
}

impl SystemStackRoutes {
    fn for_packet(&self, packet: &[u8]) -> Option<&SystemStackRoute> {
        match IpVersion::from_packet(packet).ok()? {
            IpVersion::V4 => self.v4.as_ref(),
            IpVersion::V6 => self.v6.as_ref(),
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn bind_system_listener(addr: IpAddr, interface_index: Option<u32>) -> HammerResult<TcpListener> {
    use std::os::fd::AsRawFd;

    use socket2::{Domain, Protocol, Socket, Type};

    let domain = match addr {
        IpAddr::V4(_) => Domain::IPV4,
        IpAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(|err| HammerError::internal(format!("create {addr} listener socket: {err}")))?;
    if matches!(addr, IpAddr::V6(_)) {
        socket.set_only_v6(true).map_err(|err| {
            HammerError::internal(format!("set {addr} listener IPv6-only: {err}"))
        })?;
    }
    if let Some(index) = interface_index {
        bind_listener_to_tun_interface_fd(socket.as_raw_fd(), addr, index)?;
    }
    socket
        .bind(&SocketAddr::new(addr, 0).into())
        .map_err(|err| HammerError::internal(format!("bind {addr}: {err}")))?;
    socket
        .listen(128)
        .map_err(|err| HammerError::internal(format!("listen {addr}: {err}")))?;
    let listener: StdTcpListener = socket.into();
    listener
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("set {addr} listener nonblocking: {err}")))?;
    TcpListener::from_std(listener)
        .map_err(|err| HammerError::internal(format!("register {addr} listener: {err}")))
}

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos")))]
fn bind_system_listener(addr: IpAddr, interface_index: Option<u32>) -> HammerResult<TcpListener> {
    use std::os::fd::AsRawFd;

    let listener = StdTcpListener::bind(SocketAddr::new(addr, 0))
        .map_err(|err| HammerError::internal(format!("bind {addr}: {err}")))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("set {addr} listener nonblocking: {err}")))?;
    if let Some(index) = interface_index {
        bind_listener_to_tun_interface_fd(listener.as_raw_fd(), addr, index)?;
    }
    TcpListener::from_std(listener)
        .map_err(|err| HammerError::internal(format!("register {addr} listener: {err}")))
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn bind_listener_to_tun_interface_fd(fd: RawFd, addr: IpAddr, index: u32) -> HammerResult<()> {
    let (level, name) = match addr {
        IpAddr::V4(_) => (libc::IPPROTO_IP, libc::IP_BOUND_IF),
        IpAddr::V6(_) => (libc::IPPROTO_IPV6, libc::IPV6_BOUND_IF),
    };
    let value = index as libc::c_int;
    let ret = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (&value as *const libc::c_int).cast::<libc::c_void>(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        return Err(HammerError::internal(format!(
            "bind {addr} listener to TUN ifindex {index}: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos")))]
fn bind_listener_to_tun_interface_fd(_fd: RawFd, _addr: IpAddr, _index: u32) -> HammerResult<()> {
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
pub fn tun_interface_index_from_fd(fd: RawFd) -> Option<u32> {
    use std::ffi::CStr;
    unsafe {
        let mut name = [0u8; libc::IFNAMSIZ];
        let mut len = name.len() as libc::socklen_t;
        let ret = libc::getsockopt(
            fd,
            libc::SYSPROTO_CONTROL,
            libc::UTUN_OPT_IFNAME,
            name.as_mut_ptr().cast::<libc::c_void>(),
            &mut len,
        );
        if ret != 0 || len == 0 {
            return None;
        }
        let cname = CStr::from_bytes_with_nul(&name[..len as usize]).ok()?;
        let idx = libc::if_nametoindex(cname.as_ptr());
        if idx == 0 { None } else { Some(idx) }
    }
}

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos")))]
pub fn tun_interface_index_from_fd(_fd: RawFd) -> Option<u32> {
    None
}

#[derive(Clone, Copy)]
struct StackAddresses {
    v4: Option<StackAddress<Ipv4Addr>>,
    v6: Option<StackAddress<Ipv6Addr>>,
}

#[derive(Clone, Copy)]
struct StackAddress<T> {
    listener: T,
    next: T,
}

impl StackAddresses {
    fn from_options(options: &hammer_core::config::TunInboundOptions) -> HammerResult<Self> {
        let mut v4 = None;
        let mut v6 = None;
        for net in &options.address {
            match net {
                IpNet::V4(net) if v4.is_none() => {
                    let listener = net.addr();
                    let next = next_ipv4(listener)
                        .filter(|addr| net.contains(addr))
                        .ok_or_else(|| {
                            HammerError::internal("need one more IPv4 address for system stack")
                        })?;
                    v4 = Some(StackAddress { listener, next });
                }
                IpNet::V6(net) if v6.is_none() => {
                    let listener = net.addr();
                    let next = next_ipv6(listener)
                        .filter(|addr| net.contains(addr))
                        .ok_or_else(|| {
                            HammerError::internal("need one more IPv6 address for system stack")
                        })?;
                    v6 = Some(StackAddress { listener, next });
                }
                _ => {}
            }
        }
        if v4.is_none() && v6.is_none() {
            return Err(HammerError::internal("missing TUN interface address"));
        }
        Ok(Self { v4, v6 })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TunDispatch {
    DnsResponse {
        metadata: RouteMetadata,
        payload: Bytes,
    },
    RoutedResponse {
        metadata: RouteMetadata,
        payload: Bytes,
    },
    Dropped {
        metadata: RouteMetadata,
        reason: String,
    },
}

#[inline]
pub fn parse_ip_packet(packet: &[u8]) -> HammerResult<ParsedIpPacket> {
    let parsed = parse_ip_packet_view(packet)?;
    let payload = parsed.payload(packet)?.to_vec();
    Ok(ParsedIpPacket {
        network: parsed.network,
        source: parsed.source,
        destination: parsed.destination,
        payload,
    })
}

#[inline]
fn parse_ip_packet_view(packet: &[u8]) -> HammerResult<ParsedIpPacketView> {
    match IpVersion::from_packet(packet)? {
        IpVersion::V4 => parse_ipv4_packet_view(packet),
        IpVersion::V6 => parse_ipv6_packet_view(packet),
    }
}

pub fn process_system_tcp_packet(
    packet: &mut [u8],
    nat: &mut SystemTcpNat,
    listener_addr: IpAddr,
    nat_addr: IpAddr,
    listener_port: u16,
) -> HammerResult<()> {
    match IpVersion::from_packet(packet)? {
        IpVersion::V4 => {
            process_system_tcp_ipv4(packet, nat, listener_addr, nat_addr, listener_port)
        }
        IpVersion::V6 => {
            process_system_tcp_ipv6(packet, nat, listener_addr, nat_addr, listener_port)
        }
    }
}

fn read_socks_addr(packet: &[u8], source: bool) -> HammerResult<SocksAddr> {
    match IpVersion::from_packet(packet)? {
        IpVersion::V4 => {
            if packet.len() < IPV4_HEADER_MIN_LEN + TCP_HEADER_MIN_LEN {
                return Err(HammerError::internal("short IPv4 TCP packet"));
            }
            let ihl = ((packet[0] & 0x0f) as usize) * 4;
            if ihl < IPV4_HEADER_MIN_LEN || packet.len() < ihl + TCP_HEADER_MIN_LEN {
                return Err(HammerError::internal("invalid IPv4 TCP packet"));
            }
            let host = if source {
                IpAddr::V4(Ipv4Addr::new(
                    packet[IPV4_SOURCE_OFFSET],
                    packet[IPV4_SOURCE_OFFSET + 1],
                    packet[IPV4_SOURCE_OFFSET + 2],
                    packet[IPV4_SOURCE_OFFSET + 3],
                ))
            } else {
                IpAddr::V4(Ipv4Addr::new(
                    packet[IPV4_DESTINATION_OFFSET],
                    packet[IPV4_DESTINATION_OFFSET + 1],
                    packet[IPV4_DESTINATION_OFFSET + 2],
                    packet[IPV4_DESTINATION_OFFSET + 3],
                ))
            };
            let port = if source {
                read_u16(packet, ihl)
            } else {
                read_u16(packet, ihl + 2)
            };
            Ok(SocksAddr::ip(host, port))
        }
        IpVersion::V6 => {
            if packet.len() < IPV6_HEADER_LEN + TCP_HEADER_MIN_LEN {
                return Err(HammerError::internal("short IPv6 TCP packet"));
            }
            let host = if source {
                IpAddr::V6(Ipv6Addr::from(
                    <[u8; 16]>::try_from(&packet[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET])
                        .map_err(|_| HammerError::internal("invalid IPv6 source"))?,
                ))
            } else {
                IpAddr::V6(Ipv6Addr::from(
                    <[u8; 16]>::try_from(&packet[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN])
                        .map_err(|_| HammerError::internal("invalid IPv6 destination"))?,
                ))
            };
            let port = if source {
                read_u16(packet, IPV6_HEADER_LEN)
            } else {
                read_u16(packet, IPV6_HEADER_LEN + 2)
            };
            Ok(SocksAddr::ip(host, port))
        }
    }
}

pub fn udp_response_packet(
    request: &[u8],
    source: SocksAddr,
    payload: &[u8],
) -> HammerResult<Vec<u8>> {
    UdpResponseTemplate::from_request(request)?.build(source, payload)
}

pub fn udp_unreachable_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    match IpVersion::from_packet(request)? {
        IpVersion::V4 => ipv4_udp_unreachable_packet(request),
        IpVersion::V6 => ipv6_udp_unreachable_packet(request),
    }
}

pub fn tcp_reset_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    match IpVersion::from_packet(request)? {
        IpVersion::V4 => ipv4_tcp_reset_packet(request),
        IpVersion::V6 => ipv6_tcp_reset_packet(request),
    }
}

#[derive(Clone)]
struct UdpResponseTemplate {
    header: Vec<u8>,
    version: IpVersion,
    udp_offset: usize,
}

impl UdpResponseTemplate {
    fn from_request(request: &[u8]) -> HammerResult<Self> {
        match IpVersion::from_packet(request)? {
            IpVersion::V4 => Self::from_ipv4_request(request),
            IpVersion::V6 => Self::from_ipv6_request(request),
        }
    }

    fn from_ipv4_request(request: &[u8]) -> HammerResult<Self> {
        if request.len() < IPV4_HEADER_MIN_LEN + UDP_HEADER_LEN {
            return Err(HammerError::internal("short IPv4 UDP packet"));
        }
        let ihl = ((request[0] & 0x0f) as usize) * 4;
        if ihl < IPV4_HEADER_MIN_LEN
            || request.len() < ihl + UDP_HEADER_LEN
            || request[IPV4_PROTOCOL_OFFSET] != IpProtocol::Udp.wire_value()
        {
            return Err(HammerError::internal("invalid IPv4 UDP packet"));
        }
        Ok(Self {
            header: request[..ihl + UDP_HEADER_LEN].to_vec(),
            version: IpVersion::V4,
            udp_offset: ihl,
        })
    }

    fn from_ipv6_request(request: &[u8]) -> HammerResult<Self> {
        if request.len() < IPV6_HEADER_LEN + UDP_HEADER_LEN
            || request[IPV6_PROTOCOL_OFFSET] != IpProtocol::Udp.wire_value()
        {
            return Err(HammerError::internal("invalid IPv6 UDP packet"));
        }
        Ok(Self {
            header: request[..IPV6_HEADER_LEN + UDP_HEADER_LEN].to_vec(),
            version: IpVersion::V6,
            udp_offset: IPV6_HEADER_LEN,
        })
    }

    fn build(&self, source: SocksAddr, payload: &[u8]) -> HammerResult<Vec<u8>> {
        match self.version {
            IpVersion::V4 => self.build_v4(source, payload),
            IpVersion::V6 => self.build_v6(source, payload),
        }
    }

    fn build_v4(&self, source: SocksAddr, payload: &[u8]) -> HammerResult<Vec<u8>> {
        let IpAddr::V4(source_ip) = source.host else {
            return Err(HammerError::internal("IPv4 UDP response needs IPv4 source"));
        };
        let total_len = self.header.len() + payload.len();
        if total_len > u16::MAX as usize {
            return Err(HammerError::internal("UDP response too large"));
        }
        let mut packet = Vec::with_capacity(total_len);
        packet.extend_from_slice(&self.header);
        packet.extend_from_slice(payload);
        let original_source = [packet[12], packet[13], packet[14], packet[15]];
        write_u16(&mut packet, 2, total_len as u16);
        packet[12..16].copy_from_slice(&source_ip.octets());
        packet[16..20].copy_from_slice(&original_source);
        let original_source_port = read_u16(&packet, self.udp_offset);
        write_u16(&mut packet, self.udp_offset, source.port);
        write_u16(&mut packet, self.udp_offset + 2, original_source_port);
        write_u16(
            &mut packet,
            self.udp_offset + UDP_LENGTH_OFFSET,
            (UDP_HEADER_LEN + payload.len()) as u16,
        );
        update_ipv4_udp_checksums(&mut packet, self.udp_offset)?;
        Ok(packet)
    }

    fn build_v6(&self, source: SocksAddr, payload: &[u8]) -> HammerResult<Vec<u8>> {
        let IpAddr::V6(source_ip) = source.host else {
            return Err(HammerError::internal("IPv6 UDP response needs IPv6 source"));
        };
        let payload_len = UDP_HEADER_LEN + payload.len();
        if payload_len > u16::MAX as usize {
            return Err(HammerError::internal("UDP response too large"));
        }
        let mut packet = Vec::with_capacity(self.header.len() + payload.len());
        packet.extend_from_slice(&self.header);
        packet.extend_from_slice(payload);
        let original_source = <[u8; 16]>::try_from(&packet[8..24]).unwrap();
        write_u16(&mut packet, 4, payload_len as u16);
        packet[8..24].copy_from_slice(&source_ip.octets());
        packet[24..40].copy_from_slice(&original_source);
        let original_source_port = read_u16(&packet, self.udp_offset);
        write_u16(&mut packet, self.udp_offset, source.port);
        write_u16(&mut packet, self.udp_offset + 2, original_source_port);
        write_u16(
            &mut packet,
            self.udp_offset + UDP_LENGTH_OFFSET,
            payload_len as u16,
        );
        update_ipv6_udp_checksum(&mut packet)?;
        Ok(packet)
    }
}

pub fn sniff_stream(packet: &mut TunPacket) {
    if packet.metadata.protocol.is_empty() {
        sniff_http(packet);
    }
    if packet.metadata.protocol.is_empty() {
        sniff_ssh(packet);
    }
    if packet.metadata.protocol.is_empty() {
        sniff_bittorrent_stream(packet);
    }
    if packet.metadata.protocol.is_empty() {
        sniff_tls_sni(packet);
    }
}

pub fn sniff_packet(packet: &mut TunPacket) {
    sniff_packet_metadata(&mut packet.metadata, &packet.payload);
}

fn sniff_packet_metadata(metadata: &mut RouteMetadata, payload: &[u8]) {
    if metadata.protocol.is_empty() {
        sniff_dns_payload(metadata, payload);
    }
    if metadata.protocol.is_empty() {
        sniff_quic_payload(metadata, payload);
    }
    if metadata.protocol.is_empty() {
        sniff_stun_payload(metadata, payload);
    }
}

pub struct PacketTunStack<R, Q, O>
where
    R: RouterTrait,
    Q: DnsRouterTrait,
    O: OutboundManagerTrait,
{
    router: Arc<R>,
    dns_router: Option<Arc<Q>>,
    outbound: Option<Arc<O>>,
    inbound_id: String,
}

impl<R, Q, O> PacketTunStack<R, Q, O>
where
    R: RouterTrait,
    Q: DnsRouterTrait,
    O: OutboundManagerTrait,
{
    pub fn new(_logger: Logger, router: Arc<R>, inbound_id: String) -> Self {
        Self {
            router,
            dns_router: None,
            outbound: None,
            inbound_id,
        }
    }

    pub fn new_with_runtime(
        _logger: Logger,
        router: Arc<R>,
        dns_router: Arc<Q>,
        outbound: Arc<O>,
        inbound_id: String,
    ) -> Self {
        Self {
            router,
            dns_router: Some(dns_router),
            outbound: Some(outbound),
            inbound_id,
        }
    }

    pub fn handle_packet(&self, packet: &[u8]) -> HammerResult<PacketFlow> {
        let mut tun_packet = self.packet_context(packet)?;
        prepare_route_metadata(
            self.router.as_ref(),
            &mut tun_packet.metadata,
            self.dns_router.as_deref(),
        )?;
        let decision = self.router.match_route(&mut tun_packet.metadata)?;
        debug!("handled TUN {:?} packet", tun_packet.metadata.network);
        Ok(PacketFlow {
            metadata: tun_packet.metadata,
            decision,
        })
    }

    pub async fn dispatch_packet(&self, packet: &[u8]) -> HammerResult<TunDispatch> {
        let mut tun_packet = self.packet_context(packet)?;
        prepare_route_metadata(
            self.router.as_ref(),
            &mut tun_packet.metadata,
            self.dns_router.as_deref(),
        )?;
        let decision = self.router.match_route(&mut tun_packet.metadata)?;
        match decision {
            RouteDecision::HijackDns => self.dispatch_dns(tun_packet).await,
            RouteDecision::Reject { method } => Ok(TunDispatch::Dropped {
                metadata: tun_packet.metadata,
                reason: format!("reject: {method}"),
            }),
            RouteDecision::Route {
                target: RouteTarget::Outbound(outbound),
            } => self.dispatch_route(tun_packet, &outbound).await,
            RouteDecision::Route {
                target: RouteTarget::Endpoint(endpoint),
            } => Ok(TunDispatch::Dropped {
                metadata: tun_packet.metadata,
                reason: format!("endpoint route requires L3 dispatch: {endpoint}"),
            }),
        }
    }

    fn packet_context(&self, packet: &[u8]) -> HammerResult<TunPacket> {
        let parsed = parse_ip_packet_view(packet)?;
        let payload = parsed.payload(packet)?.to_vec();
        let mut tun_packet = TunPacket {
            metadata: RouteMetadata {
                inbound: self.inbound_id.clone(),
                network: parsed.network,
                protocol: match parsed.network {
                    Network::Icmp => icmp_protocol(parsed.destination.host).to_owned(),
                    _ => String::new(),
                },
                source: Some(parsed.source),
                destination: Some(parsed.destination),
                ..Default::default()
            },
            payload,
        };
        if self.router.should_sniff(&tun_packet.metadata) {
            match tun_packet.metadata.network {
                Network::Tcp => sniff_stream(&mut tun_packet),
                Network::Udp => sniff_packet(&mut tun_packet),
                // ICMP carries no application-layer payload to sniff.
                Network::Icmp => {}
            }
        }
        Ok(tun_packet)
    }

    async fn dispatch_dns(&self, tun_packet: TunPacket) -> HammerResult<TunDispatch> {
        let dns_router = self
            .dns_router
            .as_ref()
            .ok_or_else(|| HammerError::internal("TUN DNS router is not configured"))?;
        let message = <Message as MessageExt>::from_bytes(&tun_packet.payload)?;
        let response = dns_router
            .exchange(message, dns_query_options(&tun_packet.metadata))
            .await?;
        Ok(TunDispatch::DnsResponse {
            metadata: tun_packet.metadata,
            payload: Bytes::from(MessageExt::to_bytes(&response)?),
        })
    }

    async fn dispatch_route(
        &self,
        tun_packet: TunPacket,
        outbound_id: &str,
    ) -> HammerResult<TunDispatch> {
        let outbound_manager = self
            .outbound
            .as_ref()
            .ok_or_else(|| HammerError::internal("TUN outbound manager is not configured"))?;
        let outbound = outbound_manager
            .get(outbound_id)
            .ok_or_else(|| HammerError::internal(format!("outbound not found: {outbound_id}")))?;
        let outbound = outbound.runtime();
        let destination = route_destination(&tun_packet.metadata, self.dns_router.as_deref())?;
        match tun_packet.metadata.network {
            Network::Tcp => {
                let mut stream = outbound
                    .dial(Network::Tcp, destination, &tun_packet.payload)
                    .await?;
                let mut payload = Vec::new();
                stream
                    .read_to_end(&mut payload)
                    .await
                    .map_err(|err| HammerError::internal(format!("TUN routed TCP read: {err}")))?;
                Ok(TunDispatch::RoutedResponse {
                    metadata: tun_packet.metadata,
                    payload: Bytes::from(payload),
                })
            }
            Network::Udp => {
                let mut packet = outbound.listen_packet().await?;
                packet
                    .send_to(destination, Bytes::from(tun_packet.payload))
                    .await?;
                let response = timeout(Duration::from_secs(2), packet.recv_from())
                    .await
                    .map_err(|_| HammerError::internal("TUN routed UDP response timed out"))??;
                Ok(TunDispatch::RoutedResponse {
                    metadata: tun_packet.metadata,
                    payload: response.payload,
                })
            }
            Network::Icmp => {
                let dest_ip = match tun_packet.metadata.destination.as_ref() {
                    Some(addr) => addr.host,
                    None => {
                        return Err(HammerError::internal(
                            "TUN ICMP dispatch missing destination",
                        ));
                    }
                };
                let mut conn = match outbound.listen_icmp().await {
                    Ok(conn) => conn,
                    Err(_) => {
                        // Outbound cannot carry ICMP — synthesize a Dest
                        // Unreachable response and surface it through the
                        // dispatch path. Caller is responsible for shipping
                        // `payload` back into the tun.
                        let unreachable = icmp_unreachable_response_for(
                            &tun_packet.metadata,
                            &tun_packet.payload,
                        )?;
                        return Ok(TunDispatch::RoutedResponse {
                            metadata: tun_packet.metadata,
                            payload: Bytes::from(unreachable),
                        });
                    }
                };
                conn.send_echo(dest_ip, &tun_packet.payload).await?;
                let reply = timeout(Duration::from_secs(2), conn.recv_reply())
                    .await
                    .map_err(|_| HammerError::internal("TUN routed ICMP response timed out"))??;
                Ok(TunDispatch::RoutedResponse {
                    metadata: tun_packet.metadata,
                    payload: reply.body,
                })
            }
        }
    }
}

fn parse_ipv4_packet_view(packet: &[u8]) -> HammerResult<ParsedIpPacketView> {
    if packet.len() < IPV4_HEADER_MIN_LEN {
        return Err(HammerError::internal("short IPv4 packet"));
    }
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    if ihl < IPV4_HEADER_MIN_LEN || packet.len() < ihl {
        return Err(HammerError::internal("invalid IPv4 header length"));
    }
    let source = IpAddr::V4(Ipv4Addr::new(
        packet[IPV4_SOURCE_OFFSET],
        packet[IPV4_SOURCE_OFFSET + 1],
        packet[IPV4_SOURCE_OFFSET + 2],
        packet[IPV4_SOURCE_OFFSET + 3],
    ));
    let destination = IpAddr::V4(Ipv4Addr::new(
        packet[IPV4_DESTINATION_OFFSET],
        packet[IPV4_DESTINATION_OFFSET + 1],
        packet[IPV4_DESTINATION_OFFSET + 2],
        packet[IPV4_DESTINATION_OFFSET + 3],
    ));
    parse_transport(
        IpProtocol::try_from(packet[IPV4_PROTOCOL_OFFSET])?,
        source,
        destination,
        packet,
        ihl,
    )
}

fn parse_ipv6_packet_view(packet: &[u8]) -> HammerResult<ParsedIpPacketView> {
    if packet.len() < IPV6_HEADER_LEN {
        return Err(HammerError::internal("short IPv6 packet"));
    }
    let source = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET]).unwrap(),
    ));
    let destination = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN]).unwrap(),
    ));
    parse_transport(
        IpProtocol::try_from(packet[IPV6_PROTOCOL_OFFSET])?,
        source,
        destination,
        packet,
        IPV6_HEADER_LEN,
    )
}

#[inline]
fn parse_transport(
    protocol: IpProtocol,
    source: IpAddr,
    destination: IpAddr,
    packet: &[u8],
    transport_offset: usize,
) -> HammerResult<ParsedIpPacketView> {
    let transport = packet
        .get(transport_offset..)
        .ok_or_else(|| HammerError::internal("transport offset out of bounds"))?;
    match protocol {
        IpProtocol::Tcp => parse_tcp(source, destination, transport, transport_offset),
        IpProtocol::Udp => parse_udp(source, destination, transport, transport_offset),
        IpProtocol::Icmpv4 => parse_icmpv4(source, destination, transport, transport_offset),
        IpProtocol::Icmpv6 => parse_icmpv6(source, destination, transport, transport_offset),
    }
}

fn parse_tcp(
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
    transport_offset: usize,
) -> HammerResult<ParsedIpPacketView> {
    if transport.len() < TCP_HEADER_MIN_LEN {
        return Err(HammerError::internal("short TCP segment"));
    }
    let source_port = read_u16(transport, TCP_SOURCE_PORT_OFFSET);
    let destination_port = read_u16(transport, TCP_DESTINATION_PORT_OFFSET);
    let data_offset = ((transport[TCP_DATA_OFFSET_OFFSET] >> 4) as usize) * 4;
    if data_offset < TCP_HEADER_MIN_LEN || transport.len() < data_offset {
        return Err(HammerError::internal("invalid TCP data offset"));
    }
    Ok(ParsedIpPacketView {
        network: Network::Tcp,
        source: SocksAddr::ip(source, source_port),
        destination: SocksAddr::ip(destination, destination_port),
        payload_range: transport_offset + data_offset..transport_offset + transport.len(),
    })
}

fn parse_udp(
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
    transport_offset: usize,
) -> HammerResult<ParsedIpPacketView> {
    if transport.len() < UDP_HEADER_LEN {
        return Err(HammerError::internal("short UDP datagram"));
    }
    let source_port = read_u16(transport, UDP_SOURCE_PORT_OFFSET);
    let destination_port = read_u16(transport, UDP_DESTINATION_PORT_OFFSET);
    let length = read_u16(transport, UDP_LENGTH_OFFSET) as usize;
    if length < UDP_HEADER_LEN || transport.len() < length {
        return Err(HammerError::internal("invalid UDP length"));
    }
    Ok(ParsedIpPacketView {
        network: Network::Udp,
        source: SocksAddr::ip(source, source_port),
        destination: SocksAddr::ip(destination, destination_port),
        payload_range: transport_offset + UDP_HEADER_LEN..transport_offset + length,
    })
}

fn process_system_tcp_ipv4(
    packet: &mut [u8],
    nat: &mut SystemTcpNat,
    listener_addr: IpAddr,
    nat_addr: IpAddr,
    listener_port: u16,
) -> HammerResult<()> {
    if packet.len() < IPV4_HEADER_MIN_LEN + TCP_HEADER_MIN_LEN {
        return Err(HammerError::internal("short IPv4 TCP packet"));
    }
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    if ihl < IPV4_HEADER_MIN_LEN
        || packet.len() < ihl + TCP_HEADER_MIN_LEN
        || packet[IPV4_PROTOCOL_OFFSET] != IpProtocol::Tcp.wire_value()
    {
        return Err(HammerError::internal("invalid IPv4 TCP packet"));
    }
    let source_addr = IpAddr::V4(Ipv4Addr::new(
        packet[IPV4_SOURCE_OFFSET],
        packet[IPV4_SOURCE_OFFSET + 1],
        packet[IPV4_SOURCE_OFFSET + 2],
        packet[IPV4_SOURCE_OFFSET + 3],
    ));
    let destination_addr = IpAddr::V4(Ipv4Addr::new(
        packet[IPV4_DESTINATION_OFFSET],
        packet[IPV4_DESTINATION_OFFSET + 1],
        packet[IPV4_DESTINATION_OFFSET + 2],
        packet[IPV4_DESTINATION_OFFSET + 3],
    ));
    let tcp = ihl;
    let source_port = read_u16(packet, tcp);
    let destination_port = read_u16(packet, tcp + 2);

    if source_addr == listener_addr && source_port == listener_port {
        let session = nat.lookup_back(destination_port).ok_or_else(|| {
            HammerError::internal(format!("tcp NAT session not found: {destination_port}"))
        })?;
        write_ip_addr(packet, IPV4_SOURCE_OFFSET, session.destination.host)?;
        write_u16(packet, tcp, session.destination.port);
        write_ip_addr(packet, IPV4_DESTINATION_OFFSET, session.source.host)?;
        write_u16(packet, tcp + 2, session.source.port);
    } else {
        let source = SocksAddr::ip(source_addr, source_port);
        let destination = SocksAddr::ip(destination_addr, destination_port);
        let nat_port = nat.lookup_or_insert(source, destination);
        write_ip_addr(packet, IPV4_SOURCE_OFFSET, nat_addr)?;
        write_u16(packet, tcp, nat_port);
        write_ip_addr(packet, IPV4_DESTINATION_OFFSET, listener_addr)?;
        write_u16(packet, tcp + 2, listener_port);
    }
    update_ipv4_tcp_checksums(packet, ihl)
}

fn process_system_tcp_ipv6(
    packet: &mut [u8],
    nat: &mut SystemTcpNat,
    listener_addr: IpAddr,
    nat_addr: IpAddr,
    listener_port: u16,
) -> HammerResult<()> {
    if packet.len() < IPV6_HEADER_LEN + TCP_HEADER_MIN_LEN
        || packet[IPV6_PROTOCOL_OFFSET] != IpProtocol::Tcp.wire_value()
    {
        return Err(HammerError::internal("invalid IPv6 TCP packet"));
    }
    let source_addr = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET]).unwrap(),
    ));
    let destination_addr = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN]).unwrap(),
    ));
    let tcp = IPV6_HEADER_LEN;
    let source_port = read_u16(packet, tcp);
    let destination_port = read_u16(packet, tcp + 2);

    if source_addr == listener_addr && source_port == listener_port {
        let session = nat.lookup_back(destination_port).ok_or_else(|| {
            HammerError::internal(format!("tcp NAT session not found: {destination_port}"))
        })?;
        write_ip_addr(packet, IPV6_SOURCE_OFFSET, session.destination.host)?;
        write_u16(packet, tcp, session.destination.port);
        write_ip_addr(packet, IPV6_DESTINATION_OFFSET, session.source.host)?;
        write_u16(packet, tcp + 2, session.source.port);
    } else {
        let source = SocksAddr::ip(source_addr, source_port);
        let destination = SocksAddr::ip(destination_addr, destination_port);
        let nat_port = nat.lookup_or_insert(source, destination);
        write_ip_addr(packet, IPV6_SOURCE_OFFSET, nat_addr)?;
        write_u16(packet, tcp, nat_port);
        write_ip_addr(packet, IPV6_DESTINATION_OFFSET, listener_addr)?;
        write_u16(packet, tcp + 2, listener_port);
    }
    update_ipv6_tcp_checksum(packet)
}

fn update_ipv4_tcp_checksums(packet: &mut [u8], ihl: usize) -> HammerResult<()> {
    write_u16(packet, IPV4_CHECKSUM_OFFSET, 0);
    let ip_checksum = checksum(&packet[..ihl]);
    write_u16(packet, IPV4_CHECKSUM_OFFSET, ip_checksum);
    let tcp_len = packet.len() - ihl;
    write_u16(packet, ihl + TCP_CHECKSUM_OFFSET, 0);
    let mut pseudo = Vec::with_capacity(12 + tcp_len);
    pseudo.extend_from_slice(&packet[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET + 4]);
    pseudo.push(0);
    pseudo.push(IpProtocol::Tcp.wire_value());
    pseudo.extend_from_slice(&(tcp_len as u16).to_be_bytes());
    pseudo.extend_from_slice(&packet[ihl..]);
    let tcp_checksum = checksum(&pseudo);
    write_u16(packet, ihl + TCP_CHECKSUM_OFFSET, tcp_checksum);
    Ok(())
}

fn update_ipv6_tcp_checksum(packet: &mut [u8]) -> HammerResult<()> {
    let tcp_len = packet.len() - IPV6_HEADER_LEN;
    write_u16(packet, IPV6_HEADER_LEN + TCP_CHECKSUM_OFFSET, 0);
    let mut pseudo = Vec::with_capacity(IPV6_HEADER_LEN + tcp_len);
    pseudo.extend_from_slice(&packet[IPV6_SOURCE_OFFSET..IPV6_HEADER_LEN]);
    pseudo.extend_from_slice(&(tcp_len as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, IpProtocol::Tcp.wire_value()]);
    pseudo.extend_from_slice(&packet[IPV6_HEADER_LEN..]);
    let tcp_checksum = checksum(&pseudo);
    write_u16(packet, IPV6_HEADER_LEN + TCP_CHECKSUM_OFFSET, tcp_checksum);
    Ok(())
}

fn update_ipv4_udp_checksums(packet: &mut [u8], udp_offset: usize) -> HammerResult<()> {
    write_u16(packet, IPV4_CHECKSUM_OFFSET, 0);
    let ip_checksum = checksum(&packet[..udp_offset]);
    write_u16(packet, IPV4_CHECKSUM_OFFSET, ip_checksum);
    write_u16(packet, udp_offset + UDP_CHECKSUM_OFFSET, 0);
    let udp_len = packet.len() - udp_offset;
    let mut pseudo = Vec::with_capacity(12 + udp_len);
    pseudo.extend_from_slice(&packet[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET + 4]);
    pseudo.push(0);
    pseudo.push(IpProtocol::Udp.wire_value());
    pseudo.extend_from_slice(&(udp_len as u16).to_be_bytes());
    pseudo.extend_from_slice(&packet[udp_offset..]);
    let udp_checksum = checksum(&pseudo);
    write_u16(
        packet,
        udp_offset + UDP_CHECKSUM_OFFSET,
        if udp_checksum == 0 {
            0xffff
        } else {
            udp_checksum
        },
    );
    Ok(())
}

fn update_ipv6_udp_checksum(packet: &mut [u8]) -> HammerResult<()> {
    write_u16(packet, IPV6_HEADER_LEN + UDP_CHECKSUM_OFFSET, 0);
    let udp_len = packet.len() - IPV6_HEADER_LEN;
    let mut pseudo = Vec::with_capacity(IPV6_HEADER_LEN + udp_len);
    pseudo.extend_from_slice(&packet[IPV6_SOURCE_OFFSET..IPV6_HEADER_LEN]);
    pseudo.extend_from_slice(&(udp_len as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, IpProtocol::Udp.wire_value()]);
    pseudo.extend_from_slice(&packet[IPV6_HEADER_LEN..]);
    let udp_checksum = checksum(&pseudo);
    write_u16(
        packet,
        IPV6_HEADER_LEN + UDP_CHECKSUM_OFFSET,
        if udp_checksum == 0 {
            0xffff
        } else {
            udp_checksum
        },
    );
    Ok(())
}

#[cfg(feature = "endpoint")]
fn rewrite_l3_packet_source(
    packet: &mut [u8],
    expected: IpAddr,
    replacement: IpAddr,
) -> HammerResult<()> {
    rewrite_l3_packet_addr(packet, expected, replacement, true)
}

#[cfg(feature = "endpoint")]
fn rewrite_l3_packet_destination(
    packet: &mut [u8],
    expected: IpAddr,
    replacement: IpAddr,
) -> HammerResult<()> {
    rewrite_l3_packet_addr(packet, expected, replacement, false)
}

#[cfg(feature = "endpoint")]
fn rewrite_l3_packet_addr(
    packet: &mut [u8],
    expected: IpAddr,
    replacement: IpAddr,
    source: bool,
) -> HammerResult<()> {
    if l3_packet_addr(packet, source)? != expected || expected == replacement {
        return Ok(());
    }
    match (IpVersion::from_packet(packet)?, replacement) {
        (IpVersion::V4, IpAddr::V4(new_addr)) => rewrite_ipv4_addr(packet, new_addr, source),
        (IpVersion::V6, IpAddr::V6(new_addr)) => rewrite_ipv6_addr(packet, new_addr, source),
        _ => Err(HammerError::internal(
            "endpoint L3 address rewrite family mismatch",
        )),
    }
}

// RFC 1624-style incremental checksum update for L3 NAT-style address
// rewrites. Real reassembly is out of scope on iOS NetExt (memory tight,
// fragments are uncommon on the WG-tunneled inner link); the incremental
// path lets the first fragment's transport checksum stay valid after we
// change the addresses, while non-initial fragments skip the transport
// step entirely since they carry no transport header.
#[cfg(feature = "endpoint")]
fn rewrite_ipv4_addr(packet: &mut [u8], new_addr: Ipv4Addr, source: bool) -> HammerResult<()> {
    if packet.len() < IPV4_HEADER_MIN_LEN {
        return Err(HammerError::internal("short IPv4 packet"));
    }
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    if ihl < IPV4_HEADER_MIN_LEN || packet.len() < ihl {
        return Err(HammerError::internal("invalid IPv4 header length"));
    }
    let fragment_offset = read_u16(packet, IPV4_FLAGS_FRAGMENT_OFFSET) & 0x1fff;
    let protocol = packet[IPV4_PROTOCOL_OFFSET];
    // Validate transport length *before* touching the packet so that a
    // malformed datagram is rejected without leaving half-rewritten bytes
    // behind (matches the previous validate-then-rewrite contract).
    if fragment_offset == 0 {
        ipv4_validate_transport_for_delta(packet, ihl, protocol)?;
    }

    let addr_offset = if source {
        IPV4_SOURCE_OFFSET
    } else {
        IPV4_DESTINATION_OFFSET
    };
    let mut old_bytes = [0_u8; 4];
    old_bytes.copy_from_slice(&packet[addr_offset..addr_offset + 4]);
    let new_bytes = new_addr.octets();
    let delta = checksum_pair_delta(&old_bytes, &new_bytes);

    packet[addr_offset..addr_offset + 4].copy_from_slice(&new_bytes);
    apply_checksum_delta_at(packet, IPV4_CHECKSUM_OFFSET, delta, false);

    if fragment_offset == 0 {
        ipv4_apply_transport_delta(packet, ihl, protocol, delta);
    }
    Ok(())
}

#[cfg(feature = "endpoint")]
fn rewrite_ipv6_addr(packet: &mut [u8], new_addr: Ipv6Addr, source: bool) -> HammerResult<()> {
    if packet.len() < IPV6_HEADER_LEN {
        return Err(HammerError::internal("short IPv6 packet"));
    }
    let next_header = packet[IPV6_PROTOCOL_OFFSET];
    let (transport_offset, transport_proto, later_fragment) =
        ipv6_transport_location(packet, next_header)?;
    if !later_fragment {
        ipv6_validate_transport_for_delta(packet, transport_offset, transport_proto)?;
    }

    let addr_offset = if source {
        IPV6_SOURCE_OFFSET
    } else {
        IPV6_DESTINATION_OFFSET
    };
    let mut old_bytes = [0_u8; 16];
    old_bytes.copy_from_slice(&packet[addr_offset..addr_offset + 16]);
    let new_bytes = new_addr.octets();
    let delta = checksum_pair_delta(&old_bytes, &new_bytes);

    packet[addr_offset..addr_offset + 16].copy_from_slice(&new_bytes);
    // IPv6 has no header checksum to update.

    if !later_fragment {
        ipv6_apply_transport_delta(packet, transport_offset, transport_proto, delta);
    }
    Ok(())
}

// IPv6 Fragment Header (next-header 44, RFC 8200 §4.5): if present, the
// fragment offset (top 13 bits of the second u16) tells us whether the
// transport header is in this packet (offset == 0, first fragment) or
// missing (offset != 0, later fragment).
#[cfg(feature = "endpoint")]
const IPV6_FRAGMENT_NEXT_HEADER: u8 = 44;
#[cfg(feature = "endpoint")]
const IPV6_FRAGMENT_HEADER_LEN: usize = 8;

#[cfg(feature = "endpoint")]
fn ipv6_transport_location(packet: &[u8], next_header: u8) -> HammerResult<(usize, u8, bool)> {
    if next_header == IPV6_FRAGMENT_NEXT_HEADER {
        if packet.len() < IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN {
            return Err(HammerError::internal("short IPv6 fragment header"));
        }
        let inner_proto = packet[IPV6_HEADER_LEN];
        let frag_word = read_u16(packet, IPV6_HEADER_LEN + 2);
        // Fragment Offset is the upper 13 bits scaled by 8 bytes; we only
        // care whether the first fragment carries the transport header, so
        // a non-zero fragment offset means "later fragment, skip transport".
        let later = (frag_word & 0xfff8) != 0;
        Ok((
            IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN,
            inner_proto,
            later,
        ))
    } else {
        Ok((IPV6_HEADER_LEN, next_header, false))
    }
}

#[cfg(feature = "endpoint")]
fn ipv4_validate_transport_for_delta(packet: &[u8], ihl: usize, protocol: u8) -> HammerResult<()> {
    if protocol == IpProtocol::Tcp.wire_value() {
        if packet.len() < ihl + TCP_CHECKSUM_OFFSET + 2 {
            return Err(HammerError::internal("short TCP segment"));
        }
    } else if protocol == IpProtocol::Udp.wire_value() {
        if packet.len() < ihl + UDP_HEADER_LEN {
            return Err(HammerError::internal("short UDP datagram"));
        }
    }
    Ok(())
}

#[cfg(feature = "endpoint")]
fn ipv6_validate_transport_for_delta(
    packet: &[u8],
    transport_offset: usize,
    protocol: u8,
) -> HammerResult<()> {
    if protocol == IpProtocol::Tcp.wire_value() {
        if packet.len() < transport_offset + TCP_CHECKSUM_OFFSET + 2 {
            return Err(HammerError::internal("short TCP segment"));
        }
    } else if protocol == IpProtocol::Udp.wire_value() {
        if packet.len() < transport_offset + UDP_HEADER_LEN {
            return Err(HammerError::internal("short UDP datagram"));
        }
    } else if protocol == IpProtocol::Icmpv6.wire_value() && packet.len() < transport_offset + 4 {
        return Err(HammerError::internal("short ICMPv6 packet"));
    }
    Ok(())
}

#[cfg(feature = "endpoint")]
fn ipv4_apply_transport_delta(packet: &mut [u8], ihl: usize, protocol: u8, delta: u32) {
    if protocol == IpProtocol::Tcp.wire_value() {
        apply_checksum_delta_at(packet, ihl + TCP_CHECKSUM_OFFSET, delta, false);
    } else if protocol == IpProtocol::Udp.wire_value() {
        let off = ihl + UDP_CHECKSUM_OFFSET;
        // RFC 768: a zero checksum in IPv4 UDP means "no checksum, do not
        // verify". Don't manufacture one out of nowhere just because we
        // changed addresses.
        if read_u16(packet, off) != 0 {
            apply_checksum_delta_at(packet, off, delta, true);
        }
    }
}

#[cfg(feature = "endpoint")]
fn ipv6_apply_transport_delta(
    packet: &mut [u8],
    transport_offset: usize,
    protocol: u8,
    delta: u32,
) {
    if protocol == IpProtocol::Tcp.wire_value() {
        apply_checksum_delta_at(packet, transport_offset + TCP_CHECKSUM_OFFSET, delta, false);
    } else if protocol == IpProtocol::Udp.wire_value() {
        // IPv6 UDP checksum is mandatory and a wire value of zero is
        // forbidden; if the incremental result lands on zero we rewrite it
        // to 0xffff (the canonical "all-ones" representation that hashes
        // the same under one's-complement arithmetic).
        apply_checksum_delta_at(packet, transport_offset + UDP_CHECKSUM_OFFSET, delta, true);
    } else if protocol == IpProtocol::Icmpv6.wire_value() {
        // ICMPv6 checksum offset is 2 bytes into the header (Type, Code,
        // then 2-byte checksum) and includes a pseudo-header derived from
        // the IPv6 source/destination, so address rewrites need the same
        // delta applied to it.
        apply_checksum_delta_at(packet, transport_offset + 2, delta, false);
    }
}

// Compute Σ(~m + m') over 16-bit words (RFC 1624 Eq 3 delta term). Caller
// guarantees old.len() == new.len() and an even length; one's-complement
// arithmetic does not care about alignment of the individual words to the
// packet, only that we sum the same bytes that contribute to the
// underlying internet checksum.
#[cfg(feature = "endpoint")]
fn checksum_pair_delta(old: &[u8], new: &[u8]) -> u32 {
    debug_assert_eq!(old.len(), new.len());
    debug_assert_eq!(old.len() % 2, 0);
    let mut delta: u32 = 0;
    let mut i = 0;
    while i + 1 < old.len() {
        let m = u16::from_be_bytes([old[i], old[i + 1]]) as u32;
        let m_prime = u16::from_be_bytes([new[i], new[i + 1]]) as u32;
        delta = delta.wrapping_add((!m) & 0xffff).wrapping_add(m_prime);
        i += 2;
    }
    delta
}

// Apply HC' = ~(~HC + delta) at offset, folding 32-bit overflow back into
// 16 bits. `udp_zero_to_ffff` keeps IPv6 / explicit-checksum UDP datagrams
// legal when the incremental update would otherwise produce a wire-zero
// value (forbidden by RFC 2460 / RFC 768).
#[cfg(feature = "endpoint")]
fn apply_checksum_delta_at(packet: &mut [u8], offset: usize, delta: u32, udp_zero_to_ffff: bool) {
    let hc = read_u16(packet, offset) as u32;
    let mut sum = ((!hc) & 0xffff).wrapping_add(delta);
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    let mut new_hc = ((!sum) & 0xffff) as u16;
    if udp_zero_to_ffff && new_hc == 0 {
        new_hc = 0xffff;
    }
    write_u16(packet, offset, new_hc);
}

#[cfg(feature = "endpoint")]
fn l3_packet_addr(packet: &[u8], source: bool) -> HammerResult<IpAddr> {
    match IpVersion::from_packet(packet)? {
        IpVersion::V4 => {
            if packet.len() < IPV4_HEADER_MIN_LEN {
                return Err(HammerError::internal("short IPv4 packet"));
            }
            let offset = if source {
                IPV4_SOURCE_OFFSET
            } else {
                IPV4_DESTINATION_OFFSET
            };
            Ok(IpAddr::V4(Ipv4Addr::new(
                packet[offset],
                packet[offset + 1],
                packet[offset + 2],
                packet[offset + 3],
            )))
        }
        IpVersion::V6 => {
            if packet.len() < IPV6_HEADER_LEN {
                return Err(HammerError::internal("short IPv6 packet"));
            }
            let offset = if source {
                IPV6_SOURCE_OFFSET
            } else {
                IPV6_DESTINATION_OFFSET
            };
            Ok(IpAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(&packet[offset..offset + 16]).unwrap(),
            )))
        }
    }
}

#[cfg(feature = "endpoint")]
fn update_ipv4_header_checksum(packet: &mut [u8], ihl: usize) {
    write_u16(packet, IPV4_CHECKSUM_OFFSET, 0);
    let ip_checksum = checksum(&packet[..ihl]);
    write_u16(packet, IPV4_CHECKSUM_OFFSET, ip_checksum);
}

fn close_system_tcp_stream(stream: &TcpStream) {
    let ret = unsafe { libc::shutdown(stream.as_raw_fd(), libc::SHUT_RDWR) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        if !matches!(
            err.kind(),
            std::io::ErrorKind::NotConnected | std::io::ErrorKind::InvalidInput
        ) {
            debug!("shutdown system TCP stream: {err}");
        }
    }
}

async fn bridge_system_tcp_streams(
    inbound: &mut TcpStream,
    outbound_stream: &mut dyn ProxyStream,
) -> std::io::Result<(u64, u64)> {
    let result = bridge_proxy_streams(inbound, outbound_stream).await;
    if result.is_err() {
        close_system_tcp_stream(inbound);
    } else {
        let _ = inbound.shutdown().await;
        let _ = outbound_stream.shutdown().await;
    }
    result
}

async fn bridge_proxy_streams<I>(
    inbound: &mut I,
    outbound_stream: &mut dyn ProxyStream,
) -> std::io::Result<(u64, u64)>
where
    I: AsyncRead + AsyncWrite + Unpin,
{
    copy_bidirectional_with_sizes(
        inbound,
        &mut *outbound_stream,
        SYSTEM_TCP_BRIDGE_BUFFER_SIZE,
        SYSTEM_TCP_BRIDGE_BUFFER_SIZE,
    )
    .await
}

fn ipv4_udp_unreachable_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    if request.len() < IPV4_HEADER_MIN_LEN + UDP_HEADER_LEN
        || request[IPV4_PROTOCOL_OFFSET] != IpProtocol::Udp.wire_value()
    {
        return Err(HammerError::internal("invalid IPv4 UDP packet"));
    }
    let ihl = ((request[0] & 0x0f) as usize) * 4;
    if ihl < IPV4_HEADER_MIN_LEN || request.len() < ihl + UDP_HEADER_LEN {
        return Err(HammerError::internal("invalid IPv4 UDP header"));
    }
    let quoted_len = request.len().min(ihl + UDP_HEADER_LEN);
    let total_len = IPV4_HEADER_MIN_LEN + 8 + quoted_len;
    let mut packet = vec![0_u8; total_len];
    packet[0] = 0x45;
    write_u16(&mut packet, IPV4_TOTAL_LENGTH_OFFSET, total_len as u16);
    packet[IPV4_TTL_OFFSET] = DEFAULT_PACKET_TTL;
    packet[IPV4_PROTOCOL_OFFSET] = IpProtocol::Icmpv4.wire_value();
    packet[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET]
        .copy_from_slice(&request[IPV4_DESTINATION_OFFSET..IPV4_HEADER_MIN_LEN]);
    packet[IPV4_DESTINATION_OFFSET..IPV4_HEADER_MIN_LEN]
        .copy_from_slice(&request[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET]);
    packet[IPV4_HEADER_MIN_LEN] = 3;
    packet[IPV4_HEADER_MIN_LEN + 1] = 3;
    packet[28..].copy_from_slice(&request[..quoted_len]);
    let ip_checksum = checksum(&packet[..IPV4_HEADER_MIN_LEN]);
    write_u16(&mut packet, IPV4_CHECKSUM_OFFSET, ip_checksum);
    let icmp_checksum = checksum(&packet[IPV4_HEADER_MIN_LEN..]);
    write_u16(&mut packet, IPV4_HEADER_MIN_LEN + 2, icmp_checksum);
    Ok(packet)
}

fn ipv6_udp_unreachable_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    if request.len() < IPV6_HEADER_LEN + UDP_HEADER_LEN
        || request[IPV6_PROTOCOL_OFFSET] != IpProtocol::Udp.wire_value()
    {
        return Err(HammerError::internal("invalid IPv6 UDP packet"));
    }
    let quoted_len = request.len().min(1232);
    let payload_len = UDP_HEADER_LEN + quoted_len;
    let mut packet = vec![0_u8; IPV6_HEADER_LEN + payload_len];
    packet[0] = 0x60;
    packet[IPV6_PAYLOAD_LEN_OFFSET..IPV6_PROTOCOL_OFFSET]
        .copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[IPV6_PROTOCOL_OFFSET] = IpProtocol::Icmpv6.wire_value();
    packet[IPV6_HOP_LIMIT_OFFSET] = DEFAULT_PACKET_TTL;
    packet[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET]
        .copy_from_slice(&request[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN]);
    packet[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN]
        .copy_from_slice(&request[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET]);
    packet[IPV6_HEADER_LEN] = 1;
    packet[IPV6_HEADER_LEN + 1] = 4;
    packet[48..].copy_from_slice(&request[..quoted_len]);
    update_ipv6_icmp_checksum(&mut packet)?;
    Ok(packet)
}

fn ipv4_tcp_reset_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    if request.len() < IPV4_HEADER_MIN_LEN + TCP_HEADER_MIN_LEN
        || request[IPV4_PROTOCOL_OFFSET] != IpProtocol::Tcp.wire_value()
    {
        return Err(HammerError::internal("invalid IPv4 TCP packet"));
    }
    let ihl = ((request[0] & 0x0f) as usize) * 4;
    if ihl < IPV4_HEADER_MIN_LEN || request.len() < ihl + TCP_HEADER_MIN_LEN {
        return Err(HammerError::internal("invalid IPv4 TCP header"));
    }
    let tcp_len = request.len() - ihl;
    let data_offset = ((request[ihl + 12] >> 4) as usize) * 4;
    if data_offset < TCP_HEADER_MIN_LEN || tcp_len < data_offset {
        return Err(HammerError::internal("invalid TCP data offset"));
    }
    let mut packet = vec![0_u8; IPV4_HEADER_MIN_LEN + TCP_HEADER_MIN_LEN];
    packet[0] = 0x45;
    write_u16(
        &mut packet,
        IPV4_TOTAL_LENGTH_OFFSET,
        (IPV4_HEADER_MIN_LEN + TCP_HEADER_MIN_LEN) as u16,
    );
    packet[IPV4_TTL_OFFSET] = DEFAULT_PACKET_TTL;
    packet[IPV4_PROTOCOL_OFFSET] = IpProtocol::Tcp.wire_value();
    packet[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET]
        .copy_from_slice(&request[IPV4_DESTINATION_OFFSET..IPV4_HEADER_MIN_LEN]);
    packet[IPV4_DESTINATION_OFFSET..IPV4_HEADER_MIN_LEN]
        .copy_from_slice(&request[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET]);
    write_u16(
        &mut packet,
        IPV4_HEADER_MIN_LEN,
        read_u16(request, ihl + TCP_DESTINATION_PORT_OFFSET),
    );
    write_u16(
        &mut packet,
        IPV4_HEADER_MIN_LEN + TCP_DESTINATION_PORT_OFFSET,
        read_u16(request, ihl),
    );
    let seq = read_u32(request, ihl + 4);
    let ack = read_u32(request, ihl + 8);
    let flags = request[ihl + 13];
    if flags & 0x10 != 0 {
        write_u32(&mut packet, 24, ack);
        packet[33] = 0x04;
    } else {
        let increment = (tcp_len - data_offset) as u32
            + u32::from(flags & 0x02 != 0)
            + u32::from(flags & 0x01 != 0);
        write_u32(&mut packet, 28, seq.wrapping_add(increment));
        packet[33] = 0x14;
    }
    packet[IPV4_HEADER_MIN_LEN + TCP_DATA_OFFSET_OFFSET] = 0x50;
    update_ipv4_tcp_checksums(&mut packet, IPV4_HEADER_MIN_LEN)?;
    Ok(packet)
}

fn ipv6_tcp_reset_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    if request.len() < IPV6_HEADER_LEN + TCP_HEADER_MIN_LEN
        || request[IPV6_PROTOCOL_OFFSET] != IpProtocol::Tcp.wire_value()
    {
        return Err(HammerError::internal("invalid IPv6 TCP packet"));
    }
    let tcp = IPV6_HEADER_LEN;
    let data_offset = ((request[tcp + 12] >> 4) as usize) * 4;
    if data_offset < TCP_HEADER_MIN_LEN || request.len() < tcp + data_offset {
        return Err(HammerError::internal("invalid TCP data offset"));
    }
    let tcp_len = request.len() - tcp;
    let mut packet = vec![0_u8; IPV6_HEADER_LEN + TCP_HEADER_MIN_LEN];
    packet[0] = 0x60;
    packet[IPV6_PAYLOAD_LEN_OFFSET..IPV6_PROTOCOL_OFFSET]
        .copy_from_slice(&(TCP_HEADER_MIN_LEN as u16).to_be_bytes());
    packet[IPV6_PROTOCOL_OFFSET] = IpProtocol::Tcp.wire_value();
    packet[IPV6_HOP_LIMIT_OFFSET] = DEFAULT_PACKET_TTL;
    packet[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET]
        .copy_from_slice(&request[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN]);
    packet[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN]
        .copy_from_slice(&request[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET]);
    write_u16(
        &mut packet,
        tcp,
        read_u16(request, tcp + TCP_DESTINATION_PORT_OFFSET),
    );
    write_u16(
        &mut packet,
        tcp + TCP_DESTINATION_PORT_OFFSET,
        read_u16(request, tcp),
    );
    let seq = read_u32(request, tcp + 4);
    let ack = read_u32(request, tcp + 8);
    let flags = request[tcp + 13];
    if flags & 0x10 != 0 {
        write_u32(&mut packet, 44, ack);
        packet[53] = 0x04;
    } else {
        let increment = (tcp_len - data_offset) as u32
            + u32::from(flags & 0x02 != 0)
            + u32::from(flags & 0x01 != 0);
        write_u32(&mut packet, 48, seq.wrapping_add(increment));
        packet[53] = 0x14;
    }
    packet[tcp + TCP_DATA_OFFSET_OFFSET] = 0x50;
    update_ipv6_tcp_checksum(&mut packet)?;
    Ok(packet)
}

fn update_ipv6_icmp_checksum(packet: &mut [u8]) -> HammerResult<()> {
    write_u16(packet, 42, 0);
    let icmp_len = packet.len() - 40;
    let mut pseudo = Vec::with_capacity(40 + icmp_len);
    pseudo.extend_from_slice(&packet[8..40]);
    pseudo.extend_from_slice(&(icmp_len as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, 58]);
    pseudo.extend_from_slice(&packet[40..]);
    write_u16(packet, 42, checksum(&pseudo));
    Ok(())
}

fn checksum(data: &[u8]) -> u16 {
    let mut sum = 0_u32;
    for chunk in data.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum = sum.wrapping_add(word);
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}

#[inline(always)]
fn read_u16(packet: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([packet[offset], packet[offset + 1]])
}

#[inline(always)]
fn read_u32(packet: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        packet[offset],
        packet[offset + 1],
        packet[offset + 2],
        packet[offset + 3],
    ])
}

#[inline(always)]
fn write_u16(packet: &mut [u8], offset: usize, value: u16) {
    packet[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

#[inline(always)]
fn write_u32(packet: &mut [u8], offset: usize, value: u32) {
    packet[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn write_ip_addr(packet: &mut [u8], offset: usize, addr: IpAddr) -> HammerResult<()> {
    match addr {
        IpAddr::V4(addr) => {
            let bytes = addr.octets();
            if packet.len() < offset + bytes.len() {
                return Err(HammerError::internal("short packet for IPv4 address"));
            }
            packet[offset..offset + bytes.len()].copy_from_slice(&bytes);
        }
        IpAddr::V6(addr) => {
            let bytes = addr.octets();
            if packet.len() < offset + bytes.len() {
                return Err(HammerError::internal("short packet for IPv6 address"));
            }
            packet[offset..offset + bytes.len()].copy_from_slice(&bytes);
        }
    }
    Ok(())
}

#[inline]
fn is_global_unicast(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(addr) => {
            !addr.is_unspecified()
                && !addr.is_loopback()
                && !addr.is_multicast()
                && !addr.is_broadcast()
                && !addr.is_link_local()
        }
        IpAddr::V6(addr) => {
            !addr.is_unspecified()
                && !addr.is_loopback()
                && !addr.is_multicast()
                && !addr.is_unicast_link_local()
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_tcp_loop<R, Q, O>(
    _logger: Logger,
    router: Arc<R>,
    dns_router: Arc<Q>,
    outbound: Arc<O>,
    tcp_nat: Arc<StdMutex<SystemTcpNat>>,
    tcp_pending_dials: TcpPendingDialLimiter,
    inbound_id: String,
    listener: TcpListener,
    metrics: TunMetrics,
) where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    loop {
        let (inbound, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                metrics.counters.tcp_accept_error_total.increment(1);
                debug!("system TCP listener closed: {err}");
                return;
            }
        };
        if let Ok(local) = inbound.local_addr() {
            debug!("system TCP listener accepted: local={local} peer={peer}");
        } else {
            debug!("system TCP listener accepted: peer={peer}");
        }
        let session = {
            let mut nat = tcp_nat.lock().expect("tcp_nat poisoned");
            nat.claim_active(peer.port())
        };
        let Some(session) = session else {
            metrics.counters.tcp_unknown_nat_total.increment(1);
            debug!("unknown system TCP NAT session: {}", peer.port());
            close_system_tcp_stream(&inbound);
            continue;
        };
        debug!(
            "system TCP NAT claimed: peer_port={} source={} destination={}",
            peer.port(),
            session.source,
            session.destination
        );
        let nat_lease = SystemTcpNatLease::new(Arc::clone(&tcp_nat), peer.port());
        let router = Arc::clone(&router);
        let dns_router = Arc::clone(&dns_router);
        let outbound = Arc::clone(&outbound);
        let tcp_pending_dials = tcp_pending_dials.clone();
        let inbound_id = inbound_id.clone();
        let metrics = metrics.clone();
        crate::spawn::spawn(async move {
            let _nat_lease = nat_lease;
            let mut inbound = SystemTcpInboundGuard::new(inbound);
            let mut metadata = RouteMetadata {
                inbound: inbound_id,
                network: Network::Tcp,
                source: Some(session.source.clone()),
                destination: Some(session.destination.clone()),
                ..Default::default()
            };
            let mut initial_payload = Vec::new();
            if let Some(sniff_timeout) = router.tcp_sniff_timeout(&metadata) {
                let mut buf = [0_u8; 4096];
                match timeout(sniff_timeout, inbound.stream_mut().read(&mut buf)).await {
                    Ok(Ok(0)) => {
                        inbound.disarm();
                        return;
                    }
                    Ok(Ok(n)) => {
                        initial_payload.extend_from_slice(&buf[..n]);
                        let mut packet = TunPacket {
                            metadata,
                            payload: initial_payload,
                        };
                        sniff_stream(&mut packet);
                        metadata = packet.metadata;
                        initial_payload = packet.payload;
                    }
                    Ok(Err(err)) => {
                        debug!("read TCP sniff payload: {err}");
                        return;
                    }
                    Err(_) => {}
                }
            } else {
                let mut buf = [0_u8; 4096];
                match inbound.stream_mut().try_read(&mut buf) {
                    Ok(0) => {
                        inbound.disarm();
                        return;
                    }
                    Ok(n) => {
                        initial_payload.extend_from_slice(&buf[..n]);
                    }
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                    Err(err) => {
                        debug!("read TCP initial payload: {err}");
                        return;
                    }
                }
            }
            let decision = match route_system_tcp_metadata(
                router.as_ref(),
                dns_router.as_ref(),
                &mut metadata,
            ) {
                Ok(decision) => decision,
                Err(err) => {
                    metrics.route_error_total.inc(Network::Tcp);
                    debug!("route TCP connection: {err}");
                    return;
                }
            };
            let outbound_id = match decision {
                RouteDecision::Route {
                    target: RouteTarget::Outbound(outbound_id),
                } => outbound_id,
                RouteDecision::Route {
                    target: RouteTarget::Endpoint(endpoint_id),
                } => {
                    metrics.reject_total.inc(Network::Tcp);
                    debug!("system TCP connection cannot use L3 endpoint route: {endpoint_id}");
                    return;
                }
                _ => {
                    metrics.reject_total.inc(Network::Tcp);
                    debug!("system TCP connection rejected");
                    return;
                }
            };
            let Some(outbound) = outbound.get(&outbound_id) else {
                metrics.outbound_missing_total.inc(Network::Tcp);
                error!("outbound not found: {outbound_id}");
                return;
            };
            let destination = match route_destination_without_dns(&metadata) {
                Ok(destination) => destination,
                Err(err) => {
                    metrics.counters.tcp_destination_error_total.increment(1);
                    debug!("build TCP destination: {err}");
                    return;
                }
            };
            debug!(
                "system TCP outbound dial: source={} destination={} domain={} protocol={} outbound={outbound_id} initial_payload={}B",
                metadata
                    .source
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_owned()),
                destination,
                metadata.domain.as_deref().unwrap_or("-"),
                metadata.protocol,
                initial_payload.len()
            );
            let dial_permit = match tcp_pending_dials.try_acquire() {
                Ok(permit) => permit,
                Err(err) => {
                    metrics.counters.tcp_dial_dropped_total.increment(1);
                    debug!("drop system TCP connection before outbound dial: {err}");
                    return;
                }
            };
            let mut outbound_stream = match outbound
                .runtime()
                .dial(Network::Tcp, destination, &initial_payload)
                .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    metrics.counters.tcp_dial_error_total.increment(1);
                    debug!("dial TCP outbound: {err}");
                    return;
                }
            };
            drop(dial_permit);
            match bridge_system_tcp_streams(inbound.stream_mut(), &mut *outbound_stream).await {
                Ok((from_inbound, from_outbound)) => {
                    inbound.disarm();
                    debug!("system TCP copied {from_inbound}/{from_outbound} bytes")
                }
                Err(err) => {
                    metrics.counters.tcp_copy_error_total.increment(1);
                    debug!("copy system TCP: {err}")
                }
            }
        });
    }
}

fn route_system_tcp_metadata<R, Q>(
    router: &R,
    dns_router: &Q,
    metadata: &mut RouteMetadata,
) -> HammerResult<RouteDecision>
where
    R: RouterTrait + ?Sized,
    Q: DnsRouterTrait + ?Sized,
{
    prepare_route_metadata(router, metadata, Some(dns_router))?;
    let decision = router.match_route(metadata)?;
    debug!(
        "system TCP route decision: source={} destination={} domain={} protocol={} decision={decision:?}",
        metadata
            .source
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_owned()),
        metadata
            .destination
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_owned()),
        metadata.domain.as_deref().unwrap_or("-"),
        metadata.protocol
    );
    Ok(decision)
}

enum TunPacketLoopMode<D>
where
    D: TunDevice,
{
    System { device: Arc<D> },
}

impl<D> TunPacketLoopMode<D>
where
    D: TunDevice,
{
    fn name(&self) -> &'static str {
        match self {
            Self::System { .. } => "system",
        }
    }

    async fn write_tcp_packets(&self, packets: &mut Vec<Vec<u8>>, metrics: &TunMetrics) {
        if packets.is_empty() {
            return;
        }
        let count = packets.len();
        match self {
            Self::System { device } => {
                if let Err(err) = device.send_batch(packets).await {
                    metrics.counters.tcp_writeback_error_total.increment(1);
                    debug!("write {} TCP packets: {err}", self.name());
                } else {
                    debug!("write {count} {} TCP packets back to TUN", self.name());
                }
            }
        }
    }
}

#[cfg(feature = "endpoint")]
fn endpoint_fast_path_decision<R, Q>(
    router: &R,
    dns_router: &Q,
    inbound_id: &str,
    packet: &[u8],
    parsed: &ParsedIpPacketView,
) -> HammerResult<RouteDecision>
where
    R: RouterTrait + ?Sized,
    Q: DnsRouterTrait + ?Sized,
{
    let mut metadata = RouteMetadata {
        inbound: inbound_id.to_owned(),
        network: parsed.network,
        protocol: match parsed.network {
            Network::Icmp => icmp_protocol(parsed.destination.host).to_owned(),
            _ => String::new(),
        },
        source: Some(parsed.source.clone()),
        destination: Some(parsed.destination.clone()),
        ..Default::default()
    };
    match parsed.network {
        Network::Tcp if router.tcp_sniff_timeout(&metadata).is_some() => {
            let payload = parsed.payload(packet)?;
            let mut tun_packet = TunPacket {
                metadata,
                payload: payload.to_vec(),
            };
            sniff_stream(&mut tun_packet);
            metadata = tun_packet.metadata;
        }
        _ if router.should_sniff(&metadata) => match parsed.network {
            Network::Tcp => {}
            Network::Udp => sniff_packet_metadata(&mut metadata, parsed.payload(packet)?),
            Network::Icmp => {}
        },
        _ => {}
    }
    prepare_route_metadata(router, &mut metadata, Some(dns_router))?;
    let decision = router.match_route(&mut metadata)?;
    if metadata.domain.is_some()
        || matches!(
            decision,
            RouteDecision::Route {
                target: RouteTarget::Endpoint(_)
            }
        )
    {
        debug!(
            "endpoint L3 route decision: source={} destination={} domain={} protocol={} decision={decision:?}",
            metadata
                .source
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            metadata
                .destination
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            metadata.domain.as_deref().unwrap_or("-"),
            metadata.protocol
        );
    }
    Ok(decision)
}

#[cfg(feature = "endpoint")]
fn cleanup_tcp_endpoint_flows(flows: &mut TcpEndpointFlowMap, timeout: Duration) {
    let now = Instant::now();
    flows.retain(|_, flow| now.duration_since(flow.last_active) <= timeout);
}

#[cfg(feature = "endpoint")]
fn pinned_tcp_endpoint(
    flows: &mut TcpEndpointFlowMap,
    key: &TcpEndpointFlowKey,
    timeout: Duration,
) -> Option<String> {
    cleanup_tcp_endpoint_flows(flows, timeout);
    let flow = flows.get_mut(key)?;
    flow.last_active = Instant::now();
    Some(flow.endpoint.clone())
}

#[cfg(feature = "endpoint")]
fn pin_tcp_endpoint_flow(
    flows: &mut TcpEndpointFlowMap,
    key: TcpEndpointFlowKey,
    endpoint: String,
) {
    flows.insert(
        key,
        TcpEndpointFlowState {
            endpoint,
            last_active: Instant::now(),
        },
    );
}

#[cfg(feature = "endpoint")]
fn tcp_packet_closes_flow(packet: &[u8]) -> bool {
    let Some(flags) = tcp_flags(packet) else {
        return false;
    };
    flags & 0x05 != 0
}

#[cfg(feature = "endpoint")]
fn tcp_flags(packet: &[u8]) -> Option<u8> {
    let transport_offset = match IpVersion::from_packet(packet).ok()? {
        IpVersion::V4 => {
            if packet.len() < IPV4_HEADER_MIN_LEN {
                return None;
            }
            let ihl = ((packet[0] & 0x0f) as usize) * 4;
            if ihl < IPV4_HEADER_MIN_LEN {
                return None;
            }
            ihl
        }
        IpVersion::V6 => IPV6_HEADER_LEN,
    };
    packet.get(transport_offset + 13).copied()
}

#[allow(clippy::too_many_arguments)]
async fn packet_loop<D, R, Q, O>(
    _logger: Logger,
    router: Arc<R>,
    dns_router: Arc<Q>,
    outbound: Arc<O>,
    inbound_id: String,
    device: Arc<D>,
    tcp_nat: Arc<StdMutex<SystemTcpNat>>,
    udp_flows: Arc<StdMutex<UdpFlowMap>>,
    routes: SystemStackRoutes,
    #[cfg(feature = "endpoint")] endpoint_dispatch: Option<Arc<L3DispatchTable>>,
    udp_timeout: Duration,
    metrics: TunMetrics,
) where
    D: TunDevice,
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    packet_loop_with_tcp_sink(
        router,
        dns_router,
        outbound,
        inbound_id,
        Arc::clone(&device),
        tcp_nat,
        udp_flows,
        routes,
        TunPacketLoopMode::System { device },
        #[cfg(feature = "endpoint")]
        endpoint_dispatch,
        udp_timeout,
        metrics,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn packet_loop_with_tcp_sink<D, R, Q, O>(
    router: Arc<R>,
    dns_router: Arc<Q>,
    outbound: Arc<O>,
    inbound_id: String,
    device: Arc<D>,
    tcp_nat: Arc<StdMutex<SystemTcpNat>>,
    udp_flows: Arc<StdMutex<UdpFlowMap>>,
    routes: SystemStackRoutes,
    mode: TunPacketLoopMode<D>,
    #[cfg(feature = "endpoint")] endpoint_dispatch: Option<Arc<L3DispatchTable>>,
    udp_timeout: Duration,
    metrics: TunMetrics,
) where
    D: TunDevice,
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    let stack_name = mode.name();
    info!("{stack_name} packet loop started");
    // Pull packets in batches when the underlying device supports it (Apple's
    // utun driver, currently). On platforms where recv_batch falls back to
    // single-packet recv() the batch is just `vec![pkt]` and the loop body
    // degrades to the previous behaviour.
    let mut tcp_writeback: Vec<Vec<u8>> = Vec::with_capacity(SYSTEM_TUN_RECV_BATCH_HINT);
    let mut udp_pending: Vec<(Vec<u8>, ParsedIpPacketView)> =
        Vec::with_capacity(SYSTEM_TUN_RECV_BATCH_HINT);
    let mut icmp_pending: Vec<(Vec<u8>, ParsedIpPacketView)> =
        Vec::with_capacity(SYSTEM_TUN_RECV_BATCH_HINT);
    #[cfg(feature = "endpoint")]
    let mut tcp_endpoint_flows: TcpEndpointFlowMap = HashMap::new();
    #[cfg(feature = "endpoint")]
    let mut endpoint_batches: L3EndpointQueuedBatches = HashMap::new();
    let (tun_write_tx, tun_write_rx) = mpsc::channel(SYSTEM_TUN_CONTROL_WRITE_QUEUE_CAPACITY);
    spawn_tun_packet_writer(Arc::clone(&device), metrics.clone(), tun_write_rx);
    let (dns_hijack_tx, dns_hijack_rx) = mpsc::channel(SYSTEM_DNS_HIJACK_QUEUE_CAPACITY);
    spawn_dns_hijack_workers(
        Arc::clone(&dns_router),
        tun_write_tx.clone(),
        metrics.clone(),
        dns_hijack_rx,
    );
    let (icmp_tx, icmp_rx) = mpsc::channel(SYSTEM_ICMP_QUEUE_CAPACITY);
    spawn_icmp_workers(
        Arc::clone(&router),
        Arc::clone(&dns_router),
        Arc::clone(&outbound),
        inbound_id.clone(),
        Arc::clone(&device),
        icmp_rx,
    );
    // L3 fast path fan-in: each registered Endpoint exposes a stream of
    // decapsulated inbound IP packets; pipe every packet straight to the
    // TUN writer so the kernel sees the same IP bytes the peer sent.
    #[cfg(feature = "endpoint")]
    if let Some(dispatch) = &endpoint_dispatch {
        for mut inbound in dispatch.take_inbound_receivers() {
            let tun_tx = tun_write_tx.clone();
            let metrics_clone = metrics.clone();
            crate::spawn::spawn(async move {
                loop {
                    tokio::select! {
                        packet = async {
                            match inbound.rx.as_mut() {
                                Some(rx) => rx.recv().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            let Some(packet) = packet else {
                                inbound.rx = None;
                                if inbound.batch_rx.is_none() {
                                    break;
                                }
                                continue;
                            };
                            let mut packet = packet.to_vec();
                            if rewrite_endpoint_inbound_packet(&inbound.rewrite, &mut packet, &metrics_clone).is_err() {
                                continue;
                            }
                            if tun_tx.send(TunWriteItem::Packet(packet)).await.is_err() {
                                metrics_clone
                                    .counters
                                    .tcp_writeback_error_total
                                    .increment(1);
                                break;
                            }
                            metrics_clone.counters.endpoint_inbound_total.increment(1);
                        }
                        batch = async {
                            match inbound.batch_rx.as_mut() {
                                Some(rx) => rx.recv().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            let Some(batch) = batch else {
                                inbound.batch_rx = None;
                                if inbound.rx.is_none() {
                                    break;
                                }
                                continue;
                            };
                            let mut packets = Vec::with_capacity(batch.len());
                            for packet in batch {
                                let mut packet = packet.to_vec();
                                if rewrite_endpoint_inbound_packet(&inbound.rewrite, &mut packet, &metrics_clone).is_ok() {
                                    packets.push(packet);
                                }
                            }
                            let count = packets.len() as u64;
                            if count == 0 {
                                continue;
                            }
                            if tun_tx.send(TunWriteItem::Batch(packets)).await.is_err() {
                                metrics_clone
                                    .counters
                                    .tcp_writeback_error_total
                                    .increment(1);
                                break;
                            }
                            metrics_clone.counters.endpoint_inbound_total.increment(count);
                        }
                    }
                }
            });
        }
    }
    loop {
        let packets = match device.recv_batch(SYSTEM_TUN_RECV_BATCH_HINT).await {
            Ok(packets) => packets,
            Err(err) => {
                metrics.counters.packet_recv_error_total.increment(1);
                debug!("read {stack_name} TUN packet loop stopped: {err}");
                return;
            }
        };
        if packets.is_empty() {
            tokio::task::yield_now().await;
            continue;
        }
        tcp_writeback.clear();
        udp_pending.clear();
        icmp_pending.clear();
        #[cfg(feature = "endpoint")]
        endpoint_batches.clear();
        // Pass 1: under one NAT mutex acquisition, rewrite every TCP packet
        // in the batch and stash UDP packets for sequential post-processing.
        // The guard is released at the end of this scope before any I/O
        // await — std::sync::MutexGuard is !Send so this matters for the
        // tokio multi-thread scheduler.
        {
            let mut nat = tcp_nat.lock().expect("tcp_nat poisoned");
            for mut packet in packets {
                if packet.is_empty() {
                    metrics.counters.packet_drop_empty_total.increment(1);
                    continue;
                }
                let parsed = match parse_ip_packet_view(&packet) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        metrics.counters.packet_parse_error_total.increment(1);
                        trace!("ignore unsupported TUN packet: {err}");
                        continue;
                    }
                };
                if !is_global_unicast(parsed.destination.host) {
                    metrics.counters.packet_drop_non_global_total.increment(1);
                    continue;
                }
                // L3 fast path must be selected by the router, not just by
                // allowed_ips. DNS hijack/reject/direct rules still get the
                // same policy decision before raw endpoint dispatch.
                #[cfg(feature = "endpoint")]
                if let Some(dispatch) = endpoint_dispatch.as_deref() {
                    if parsed.network == Network::Udp {
                        let key = UdpFlowKey::from_parsed(&parsed);
                        match try_enqueue_existing_udp_flow(
                            &udp_flows, &key, &packet, &parsed, &metrics,
                        ) {
                            Ok(true) => continue,
                            Ok(false) => {}
                            Err(err) => {
                                debug!(
                                    "enqueue existing system UDP flow before endpoint route: {err}"
                                );
                                continue;
                            }
                        }
                    }
                    let tcp_flow_key = if parsed.network == Network::Tcp {
                        Some(TcpEndpointFlowKey::from_parsed(&parsed))
                    } else {
                        None
                    };
                    if let Some(key) = tcp_flow_key.as_ref() {
                        if let Some(endpoint_id) =
                            pinned_tcp_endpoint(&mut tcp_endpoint_flows, key, udp_timeout)
                        {
                            let closes = tcp_packet_closes_flow(&packet);
                            let dispatched = queue_endpoint_l3_packet(
                                dispatch,
                                &endpoint_id,
                                packet,
                                &parsed,
                                &metrics,
                                Some(&tun_write_tx),
                                &mut endpoint_batches,
                            );
                            if closes || !dispatched {
                                tcp_endpoint_flows.remove(key);
                            }
                            continue;
                        }
                    }
                    match endpoint_fast_path_decision(
                        router.as_ref(),
                        dns_router.as_ref(),
                        &inbound_id,
                        &packet,
                        &parsed,
                    ) {
                        Ok(RouteDecision::Route {
                            target: RouteTarget::Endpoint(endpoint_id),
                        }) => {
                            if let Some(key) = tcp_flow_key {
                                let closes = tcp_packet_closes_flow(&packet);
                                let dispatched = queue_endpoint_l3_packet(
                                    dispatch,
                                    &endpoint_id,
                                    packet,
                                    &parsed,
                                    &metrics,
                                    Some(&tun_write_tx),
                                    &mut endpoint_batches,
                                );
                                if dispatched && !closes {
                                    pin_tcp_endpoint_flow(
                                        &mut tcp_endpoint_flows,
                                        key,
                                        endpoint_id.clone(),
                                    );
                                } else {
                                    tcp_endpoint_flows.remove(&key);
                                }
                            } else {
                                let _ = queue_endpoint_l3_packet(
                                    dispatch,
                                    &endpoint_id,
                                    packet,
                                    &parsed,
                                    &metrics,
                                    Some(&tun_write_tx),
                                    &mut endpoint_batches,
                                );
                            }
                            continue;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            metrics.route_error_total.inc(parsed.network);
                            debug!("route endpoint L3 packet: {err}");
                            continue;
                        }
                    }
                }
                match parsed.network {
                    Network::Tcp => {
                        let Some(route) = routes.for_packet(&packet) else {
                            debug!("missing {stack_name} TCP route for packet family");
                            continue;
                        };
                        let original_source = read_socks_addr(&packet, true).ok();
                        let original_destination = read_socks_addr(&packet, false).ok();
                        let rewrite_result = process_system_tcp_packet(
                            &mut packet,
                            &mut nat,
                            route.listener_addr,
                            route.nat_addr,
                            route.listener_port,
                        );
                        if let Err(err) = rewrite_result {
                            debug!("rewrite {stack_name} TCP packet: {err}");
                            continue;
                        }
                        let rewritten_source = read_socks_addr(&packet, true).ok();
                        let rewritten_destination = read_socks_addr(&packet, false).ok();
                        debug!(
                            "rewrite {stack_name} TCP packet: {} -> {} => {} -> {} (listener={} nat={})",
                            original_source
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            original_destination
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            rewritten_source
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            rewritten_destination
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            route.listener_addr,
                            route.nat_addr
                        );
                        tcp_writeback.push(packet);
                    }
                    Network::Udp => {
                        udp_pending.push((packet, parsed));
                    }
                    Network::Icmp => {
                        icmp_pending.push((packet, parsed));
                    }
                }
            }
        }
        #[cfg(feature = "endpoint")]
        flush_endpoint_l3_batches(&mut endpoint_batches, &metrics).await;
        // Pass 2: send the rewritten TCP batch either to the system TUN fd
        // or directly into an endpoint fast path, depending on the selected route.
        mode.write_tcp_packets(&mut tcp_writeback, &metrics).await;
        // Pass 3: UDP / DNS handling must not await in the TUN read loop.
        // Route lookup and flow queueing stay cheap and synchronous; DNS
        // hijack, outbound packet connection setup, and response writes run
        // in background tasks owned by their business path.
        for (packet, parsed) in udp_pending.drain(..) {
            if let Err(err) = handle_system_udp_packet(
                Arc::clone(&router),
                Arc::clone(&dns_router),
                Arc::clone(&outbound),
                inbound_id.clone(),
                Arc::clone(&udp_flows),
                &dns_hijack_tx,
                &tun_write_tx,
                udp_timeout,
                packet,
                parsed,
                metrics.clone(),
            ) {
                debug!("handle {stack_name} UDP packet: {err}");
            }
        }
        // Pass 4: ICMP echo handling also leaves the TUN read loop before
        // touching outbound I/O.
        for (packet, parsed) in icmp_pending.drain(..) {
            enqueue_system_icmp_packet(&icmp_tx, packet, parsed, &metrics);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_system_udp_packet<R, Q, O>(
    router: Arc<R>,
    dns_router: Arc<Q>,
    outbound: Arc<O>,
    inbound_id: String,
    udp_flows: Arc<StdMutex<UdpFlowMap>>,
    dns_hijack_tx: &mpsc::Sender<DnsHijackJob>,
    tun_write_tx: &mpsc::Sender<TunWriteItem>,
    udp_timeout: Duration,
    packet: Vec<u8>,
    parsed: ParsedIpPacketView,
    metrics: TunMetrics,
) -> HammerResult<()>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    let key = UdpFlowKey::from_parsed(&parsed);
    if try_enqueue_existing_udp_flow(&udp_flows, &key, &packet, &parsed, &metrics)? {
        return Ok(());
    }

    let mut metadata = RouteMetadata {
        inbound: inbound_id,
        network: Network::Udp,
        source: Some(parsed.source.clone()),
        destination: Some(parsed.destination.clone()),
        ..Default::default()
    };
    if router.should_sniff(&metadata) {
        sniff_packet_metadata(&mut metadata, parsed.payload(&packet)?);
    }
    if let Err(err) =
        prepare_route_metadata(router.as_ref(), &mut metadata, Some(dns_router.as_ref()))
    {
        metrics.counters.udp_route_prepare_error_total.increment(1);
        return Err(err);
    }
    let decision = match router.match_route(&mut metadata) {
        Ok(decision) => decision,
        Err(err) => {
            metrics.route_error_total.inc(Network::Udp);
            return Err(err);
        }
    };
    match decision {
        RouteDecision::HijackDns => {
            let message = match <Message as MessageExt>::from_bytes(parsed.payload(&packet)?) {
                Ok(message) => message,
                Err(err) => {
                    metrics.counters.udp_dns_error_total.increment(1);
                    return Err(err);
                }
            };
            let options = dns_query_options(&metadata);
            let destination = parsed.destination;
            match dns_router.try_exchange_fast(&message, options.clone())? {
                Some(response) => {
                    let response_bytes = MessageExt::to_bytes(&response)?;
                    let response_packet =
                        udp_response_packet(&packet, destination, &response_bytes)?;
                    enqueue_tun_packet_write(tun_write_tx, response_packet, &metrics);
                }
                None => {
                    enqueue_system_dns_hijack_packet(
                        dns_hijack_tx,
                        packet,
                        destination,
                        message,
                        options,
                        &metrics,
                    );
                }
            }
        }
        RouteDecision::Reject { method } => {
            metrics.reject_total.inc(Network::Udp);
            let message = format!(
                "drop UDP packet by reject rule: method={}, destination={}, protocol={}",
                method, parsed.destination, metadata.protocol
            );
            if metadata.protocol == "quic" {
                trace!("{}", message);
            } else {
                debug!("{}", message);
            }
            if let Ok(response) = udp_unreachable_packet(&packet) {
                enqueue_tun_packet_write(tun_write_tx, response, &metrics);
            }
        }
        RouteDecision::Route {
            target: RouteTarget::Outbound(outbound_id),
        } => {
            let Some(outbound_item) = outbound.get(&outbound_id) else {
                metrics.outbound_missing_total.inc(Network::Udp);
                return Err(HammerError::internal(format!(
                    "outbound not found: {outbound_id}"
                )));
            };
            let destination = route_destination(&metadata, Some(dns_router.as_ref()))?;
            if metadata.domain.is_some() || outbound_id == "direct" {
                debug!(
                    "system UDP route decision: source={} destination={} domain={} protocol={} outbound={outbound_id}",
                    metadata
                        .source
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "-".to_owned()),
                    destination,
                    metadata.domain.as_deref().unwrap_or("-"),
                    metadata.protocol
                );
            }
            let template = UdpResponseTemplate::from_request(&packet)?;
            let mut start_flow = None;
            let sender = {
                let mut flows = udp_flows.lock().expect("udp_flows poisoned");
                if let Some(flow) = flows.get_mut(&key) {
                    flow.last_active = Instant::now();
                    flow.sender.clone()
                } else {
                    evict_udp_flow_if_needed(&mut flows, &metrics);
                    let (tx, rx) = mpsc::channel(SYSTEM_UDP_CHANNEL_CAPACITY);
                    flows.insert(
                        key.clone(),
                        UdpFlowState {
                            sender: tx.clone(),
                            last_active: Instant::now(),
                            outbound: outbound_id.clone(),
                        },
                    );
                    start_flow = Some((
                        Arc::clone(&udp_flows),
                        key.clone(),
                        Arc::clone(outbound_item.runtime()),
                        destination,
                        template,
                        udp_timeout,
                        rx,
                        tun_write_tx.clone(),
                        metrics.clone(),
                    ));
                    tx
                }
            };
            match enqueue_udp_payload(&sender, &packet, &parsed, &metrics)? {
                UdpPayloadEnqueueResult::Enqueued => {}
                UdpPayloadEnqueueResult::Full => {}
                UdpPayloadEnqueueResult::Closed => {
                    udp_flows.lock().expect("udp_flows poisoned").remove(&key);
                    debug!("drop UDP packet for closed system flow: outbound={outbound_id}");
                }
            }
            if let Some((
                udp_flows,
                key,
                outbound_item,
                destination,
                template,
                udp_timeout,
                rx,
                tun_write_tx,
                metrics,
            )) = start_flow
            {
                crate::spawn::spawn(system_udp_flow_loop(
                    udp_flows,
                    key,
                    outbound_item,
                    destination,
                    template,
                    udp_timeout,
                    rx,
                    tun_write_tx,
                    metrics,
                ));
            }
        }
        RouteDecision::Route {
            target: RouteTarget::Endpoint(endpoint_id),
        } => {
            metrics.outbound_missing_total.inc(Network::Udp);
            return Err(HammerError::internal(format!(
                "endpoint route requires L3 dispatch: {endpoint_id}"
            )));
        }
    }
    Ok(())
}

enum UdpPayloadEnqueueResult {
    Enqueued,
    Full,
    Closed,
}

fn try_enqueue_existing_udp_flow(
    udp_flows: &StdMutex<UdpFlowMap>,
    key: &UdpFlowKey,
    packet: &[u8],
    parsed: &ParsedIpPacketView,
    metrics: &TunMetrics,
) -> HammerResult<bool> {
    let Some((sender, outbound)) = ({
        let mut flows = udp_flows.lock().expect("udp_flows poisoned");
        flows.get_mut(key).map(|flow| {
            flow.last_active = Instant::now();
            (flow.sender.clone(), flow.outbound.clone())
        })
    }) else {
        return Ok(false);
    };

    match enqueue_udp_payload(&sender, packet, parsed, metrics)? {
        UdpPayloadEnqueueResult::Enqueued => Ok(true),
        UdpPayloadEnqueueResult::Full => Ok(true),
        UdpPayloadEnqueueResult::Closed => {
            udp_flows.lock().expect("udp_flows poisoned").remove(key);
            debug!("remove closed system UDP flow before slow path: outbound={outbound}");
            Ok(false)
        }
    }
}

fn enqueue_udp_payload(
    sender: &mpsc::Sender<UdpFlowPayload>,
    packet: &[u8],
    parsed: &ParsedIpPacketView,
    metrics: &TunMetrics,
) -> HammerResult<UdpPayloadEnqueueResult> {
    match sender.try_reserve() {
        Ok(permit) => {
            let payload = Bytes::copy_from_slice(parsed.payload(packet)?);
            permit.send(payload);
            Ok(UdpPayloadEnqueueResult::Enqueued)
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics.counters.udp_flow_drop_busy_total.increment(1);
            debug!("drop UDP packet for busy system flow");
            Ok(UdpPayloadEnqueueResult::Full)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Ok(UdpPayloadEnqueueResult::Closed),
    }
}

fn enqueue_system_dns_hijack_packet(
    dns_hijack_tx: &mpsc::Sender<DnsHijackJob>,
    packet: Vec<u8>,
    destination: SocksAddr,
    message: Message,
    options: DnsQueryOptions,
    metrics: &TunMetrics,
) {
    let job = DnsHijackJob {
        packet,
        destination,
        message,
        options,
    };
    match dns_hijack_tx.try_send(job) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics.counters.udp_dns_drop_busy_total.increment(1);
            debug!("drop DNS hijack packet because DNS queue is full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            metrics.counters.udp_dns_drop_busy_total.increment(1);
            debug!("drop DNS hijack packet because DNS queue is closed");
        }
    }
}

fn spawn_dns_hijack_workers<Q>(
    dns_router: Arc<Q>,
    tun_write_tx: mpsc::Sender<TunWriteItem>,
    metrics: TunMetrics,
    rx: mpsc::Receiver<DnsHijackJob>,
) where
    Q: DnsRouterTrait + 'static,
{
    let mut worker_txs = Vec::with_capacity(SYSTEM_DNS_HIJACK_WORKERS);
    for _ in 0..SYSTEM_DNS_HIJACK_WORKERS {
        let (worker_tx, worker_rx) = mpsc::channel(SYSTEM_DNS_HIJACK_WORKER_QUEUE_CAPACITY);
        worker_txs.push(worker_tx);
        spawn_dns_hijack_worker(
            Arc::clone(&dns_router),
            tun_write_tx.clone(),
            metrics.clone(),
            worker_rx,
        );
    }
    spawn_dns_hijack_dispatcher(rx, worker_txs, metrics);
}

fn spawn_dns_hijack_dispatcher(
    mut rx: mpsc::Receiver<DnsHijackJob>,
    worker_txs: Vec<mpsc::Sender<DnsHijackJob>>,
    metrics: TunMetrics,
) -> JoinHandle<()> {
    crate::spawn::spawn(async move {
        let mut next_worker = 0usize;
        while let Some(job) = rx.recv().await {
            if let Some(job) = try_dispatch_dns_hijack_job(job, &worker_txs, &mut next_worker) {
                wait_dispatch_dns_hijack_job(job, &worker_txs, &mut next_worker, &metrics).await;
            }
        }
    })
}

fn try_dispatch_dns_hijack_job(
    job: DnsHijackJob,
    worker_txs: &[mpsc::Sender<DnsHijackJob>],
    next_worker: &mut usize,
) -> Option<DnsHijackJob> {
    let len = worker_txs.len();
    let mut pending = Some(job);
    for _ in 0..len {
        let index = *next_worker % len;
        *next_worker = (*next_worker + 1) % len;
        let job = pending.take().expect("pending DNS hijack job");
        match worker_txs[index].try_send(job) {
            Ok(()) => return None,
            Err(mpsc::error::TrySendError::Full(job))
            | Err(mpsc::error::TrySendError::Closed(job)) => {
                pending = Some(job);
            }
        }
    }
    pending
}

async fn wait_dispatch_dns_hijack_job(
    job: DnsHijackJob,
    worker_txs: &[mpsc::Sender<DnsHijackJob>],
    next_worker: &mut usize,
    metrics: &TunMetrics,
) {
    let len = worker_txs.len();
    let mut pending = Some(job);
    for _ in 0..len {
        let index = *next_worker % len;
        *next_worker = (*next_worker + 1) % len;
        let job = pending.take().expect("pending DNS hijack job");
        match worker_txs[index].send(job).await {
            Ok(()) => return,
            Err(err) => {
                pending = Some(err.0);
            }
        }
    }
    metrics.counters.udp_dns_drop_busy_total.increment(1);
    debug!("drop DNS hijack packet because all DNS workers are closed");
}

fn spawn_dns_hijack_worker<Q>(
    dns_router: Arc<Q>,
    tun_write_tx: mpsc::Sender<TunWriteItem>,
    metrics: TunMetrics,
    mut rx: mpsc::Receiver<DnsHijackJob>,
) -> JoinHandle<()>
where
    Q: DnsRouterTrait + 'static,
{
    crate::spawn::spawn(async move {
        while let Some(job) = rx.recv().await {
            match build_system_dns_hijack_response(Arc::clone(&dns_router), job).await {
                Ok(packet) => enqueue_tun_packet_write(&tun_write_tx, packet, &metrics),
                Err(err) => {
                    metrics.counters.udp_dns_error_total.increment(1);
                    debug!("handle system DNS hijack packet: {err}");
                }
            }
        }
    })
}

async fn build_system_dns_hijack_response<Q>(
    dns_router: Arc<Q>,
    job: DnsHijackJob,
) -> HammerResult<Vec<u8>>
where
    Q: DnsRouterTrait + 'static,
{
    let response = dns_router.exchange(job.message, job.options).await?;
    let response_bytes = MessageExt::to_bytes(&response)?;
    udp_response_packet(&job.packet, job.destination, &response_bytes)
}

fn enqueue_tun_packet_write(
    tun_write_tx: &mpsc::Sender<TunWriteItem>,
    packet: Vec<u8>,
    metrics: &TunMetrics,
) {
    match tun_write_tx.try_send(TunWriteItem::Packet(packet)) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics.counters.control_write_drop_busy_total.increment(1);
            debug!("drop TUN packet because write queue is full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            metrics.counters.control_write_drop_busy_total.increment(1);
            debug!("drop TUN packet because write queue is closed");
        }
    }
}

fn enqueue_tun_udp_response_write(
    tun_write_tx: &mpsc::Sender<TunWriteItem>,
    packet: Vec<u8>,
    metrics: &TunMetrics,
) {
    match tun_write_tx.try_send(TunWriteItem::Packet(packet)) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics
                .counters
                .udp_flow_response_write_error_total
                .increment(1);
            debug!("drop UDP response because TUN write queue is full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            metrics
                .counters
                .udp_flow_response_write_error_total
                .increment(1);
            debug!("drop UDP response because TUN write queue is closed");
        }
    }
}

fn spawn_tun_packet_writer<D>(
    device: Arc<D>,
    metrics: TunMetrics,
    mut rx: mpsc::Receiver<TunWriteItem>,
) -> JoinHandle<()>
where
    D: TunDevice,
{
    crate::spawn::spawn(async move {
        const WRITE_BATCH_HINT: usize = 64;
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(WRITE_BATCH_HINT);
        while let Some(item) = rx.recv().await {
            append_tun_write_item(item, &mut batch);
            while batch.len() < WRITE_BATCH_HINT {
                match rx.try_recv() {
                    Ok(item) => append_tun_write_item(item, &mut batch),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }
            if let Err(err) = device.send_batch(&mut batch).await {
                metrics.counters.control_write_error_total.increment(1);
                debug!("write TUN packet batch: {err}");
                batch.clear();
            }
        }
    })
}

fn append_tun_write_item(item: TunWriteItem, batch: &mut Vec<Vec<u8>>) {
    match item {
        TunWriteItem::Packet(packet) => batch.push(packet),
        TunWriteItem::Batch(mut packets) => batch.append(&mut packets),
    }
}

fn dns_query_options(metadata: &RouteMetadata) -> DnsQueryOptions {
    DnsQueryOptions {
        strategy: metadata.domain_strategy.unwrap_or_default(),
        ..DnsQueryOptions::default()
    }
}

fn prepare_route_metadata<R, Q>(
    router: &R,
    metadata: &mut RouteMetadata,
    dns_router: Option<&Q>,
) -> HammerResult<()>
where
    R: RouterTrait + ?Sized,
    Q: DnsRouterTrait + ?Sized,
{
    router.prepare_route_metadata(metadata)?;
    apply_reverse_dns_mapping(metadata, dns_router);
    Ok(())
}

fn apply_reverse_dns_mapping<Q>(metadata: &mut RouteMetadata, dns_router: Option<&Q>)
where
    Q: DnsRouterTrait + ?Sized,
{
    if !matches!(metadata.network, Network::Tcp | Network::Udp)
        || metadata.udp_disable_domain_unmapping
        || metadata.domain.is_some()
    {
        return;
    }
    let Some(destination) = metadata.destination.as_ref() else {
        return;
    };
    if destination.domain.is_some() {
        return;
    }
    let Some(router) = dns_router else {
        return;
    };
    let Some(domain) = router.lookup_reverse_mapping(destination.host) else {
        return;
    };
    metadata.domain = Some(normalize_destination_domain(&domain));
}

fn route_destination<Q>(metadata: &RouteMetadata, dns_router: Option<&Q>) -> HammerResult<SocksAddr>
where
    Q: DnsRouterTrait + ?Sized,
{
    let mut destination = route_destination_without_dns(metadata)?;
    if metadata.override_destination {
        return Ok(destination);
    }
    if metadata.network == Network::Udp
        && !metadata.udp_disable_domain_unmapping
        && destination.domain.is_none()
        && let Some(router) = dns_router
        && let Some(domain) = router.lookup_reverse_mapping(destination.host)
    {
        destination.domain = Some(normalize_destination_domain(&domain));
    }
    Ok(destination)
}

fn route_destination_without_dns(metadata: &RouteMetadata) -> HammerResult<SocksAddr> {
    let mut destination = metadata
        .destination
        .clone()
        .ok_or_else(|| HammerError::internal("TUN packet missing destination"))?;
    if metadata.override_destination
        && let Some(domain) = metadata.domain.as_deref()
    {
        destination.domain = Some(normalize_destination_domain(domain));
    }
    Ok(destination)
}

fn normalize_destination_domain(domain: &str) -> String {
    // First peel off a trailing `:port` (sniffers/SNI sometimes pack it in)
    // — `normalize_domain` would lowercase the digits but it can't tell
    // host from port. Then run the shared canonicaliser so the produced
    // string matches everything else metadata.domain compares against.
    let trimmed = domain.trim().trim_end_matches('.');
    let host = if let Some((host, port)) = trimmed.rsplit_once(':')
        && !host.contains(':')
        && port.chars().all(|c| c.is_ascii_digit())
    {
        host
    } else {
        trimmed
    };
    normalize_domain(host)
}

#[allow(clippy::too_many_arguments)]
async fn system_udp_flow_loop(
    udp_flows: Arc<StdMutex<UdpFlowMap>>,
    key: UdpFlowKey,
    outbound: Arc<dyn hammer_adapter::Outbound>,
    destination: SocksAddr,
    response_template: UdpResponseTemplate,
    udp_timeout: Duration,
    mut rx: mpsc::Receiver<UdpFlowPayload>,
    tun_write_tx: mpsc::Sender<TunWriteItem>,
    metrics: TunMetrics,
) {
    let mut packet_conn = match outbound.listen_packet().await {
        Ok(packet_conn) => packet_conn,
        Err(err) => {
            metrics.counters.udp_listen_error_total.increment(1);
            debug!("listen system UDP outbound: {err}");
            udp_flows.lock().expect("udp_flows poisoned").remove(&key);
            return;
        }
    };
    let idle_timer = time::sleep(udp_timeout);
    tokio::pin!(idle_timer);
    loop {
        tokio::select! {
            next = rx.recv() => {
                let Some(item) = next else {
                    break;
                };
                idle_timer.as_mut().reset(Instant::now() + udp_timeout);
                if let Err(err) = packet_conn.send_to(destination.clone(), item).await {
                    metrics.counters.udp_flow_send_error_total.increment(1);
                    debug!("send system UDP outbound: {err}");
                    break;
                }
            }
            response = packet_conn.recv_from() => {
                let response = match response {
                    Ok(response) => response,
                    Err(err) => {
                        metrics.counters.udp_flow_recv_error_total.increment(1);
                        debug!("receive system UDP outbound: {err}");
                        break;
                    }
                };
                idle_timer.as_mut().reset(Instant::now() + udp_timeout);
                match response_template.build(response.destination, &response.payload) {
                    Ok(packet) => {
                        enqueue_tun_udp_response_write(&tun_write_tx, packet, &metrics);
                    }
                    Err(err) => debug!("build system UDP response: {err}"),
                }
            }
            _ = &mut idle_timer => {
                metrics.counters.udp_flow_timeout_total.increment(1);
                break;
            }
        }
    }
    udp_flows.lock().expect("udp_flows poisoned").remove(&key);
}

fn evict_udp_flow_if_needed(flows: &mut UdpFlowMap, metrics: &TunMetrics) {
    if flows.len() < SYSTEM_UDP_FLOW_CAPACITY {
        return;
    }
    if let Some(oldest_key) = flows
        .iter()
        .min_by_key(|(_, flow)| flow.last_active)
        .map(|(key, _)| key.clone())
    {
        flows.remove(&oldest_key);
        metrics.counters.udp_flow_evict_total.increment(1);
    }
}

fn next_ipv4(addr: Ipv4Addr) -> Option<Ipv4Addr> {
    let value = u32::from(addr).checked_add(1)?;
    Some(Ipv4Addr::from(value))
}

fn next_ipv6(addr: Ipv6Addr) -> Option<Ipv6Addr> {
    let value = u128::from(addr).checked_add(1)?;
    Some(Ipv6Addr::from(value))
}

fn sniff_http(packet: &mut TunPacket) {
    let Ok(text) = std::str::from_utf8(&packet.payload) else {
        return;
    };
    let Some(first_line_end) = text.find("\r\n") else {
        return;
    };
    let first_line = &text[..first_line_end];
    if !first_line.contains(" HTTP/") {
        return;
    }
    for line in text[first_line_end + 2..].lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("host") {
            packet.metadata.protocol = "http".to_owned();
            // Canonical-form metadata.domain so DomainMatcher / DnsRule can
            // be a pure byte compare.
            let normalized = normalize_domain(value);
            if !normalized.is_empty() {
                packet.metadata.domain = Some(normalized);
            }
            return;
        }
    }
}

fn sniff_ssh(packet: &mut TunPacket) {
    const PREFIX: &[u8] = b"SSH-2.0-";
    if packet.payload.starts_with(PREFIX) {
        packet.metadata.protocol = "ssh".to_owned();
        let client = packet
            .payload
            .split(|b| *b == b'\n' || *b == b'\r')
            .next()
            .and_then(|line| std::str::from_utf8(line).ok())
            .map(|line| line.trim_start_matches("SSH-2.0-").to_owned());
        packet.metadata.client = client;
    }
}

fn sniff_bittorrent_stream(packet: &mut TunPacket) {
    if packet.payload.starts_with(b"\x13BitTorrent protocol") {
        packet.metadata.protocol = "bittorrent".to_owned();
    }
}

fn sniff_tls_sni(packet: &mut TunPacket) {
    if packet.payload.len() < 6 || packet.payload[0] != 22 {
        return;
    }
    if let Some(domain) = parse_tls_sni(&packet.payload) {
        packet.metadata.protocol = "tls".to_owned();
        let normalized = normalize_domain(&domain);
        if !normalized.is_empty() {
            packet.metadata.domain = Some(normalized);
        }
    }
}

fn sniff_dns_payload(metadata: &mut RouteMetadata, payload: &[u8]) {
    if payload.len() < 12 {
        return;
    }
    if payload[2] & 0x80 != 0 {
        return;
    }
    let questions = u16::from_be_bytes([payload[4], payload[5]]);
    let answers = u16::from_be_bytes([payload[6], payload[7]]);
    if questions == 0 || answers != 0 {
        return;
    }
    metadata.protocol = "dns".to_owned();
}

fn sniff_quic_payload(metadata: &mut RouteMetadata, payload: &[u8]) {
    if payload.len() < 7 || payload[0] & 0xc0 != 0xc0 {
        return;
    }
    let version = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
    if !matches!(version, 0x0000_0001 | 0x0000_0002 | 0xff00_001d) {
        return;
    }
    metadata.protocol = "quic".to_owned();
}

fn sniff_stun_payload(metadata: &mut RouteMetadata, payload: &[u8]) {
    if payload.len() >= 20 && payload[4..8] == [0x21, 0x12, 0xa4, 0x42] {
        metadata.protocol = "stun".to_owned();
    }
}

fn parse_tls_sni(payload: &[u8]) -> Option<String> {
    if payload.len() < 5 || payload[0] != 22 {
        return None;
    }
    let record_len = read_u16(payload, 3) as usize;
    let record = payload.get(5..5 + record_len)?;
    if record.len() < 4 || record[0] != 1 {
        return None;
    }
    let handshake_len =
        ((record[1] as usize) << 16) | ((record[2] as usize) << 8) | record[3] as usize;
    let body = record.get(4..4 + handshake_len)?;

    let mut pos = 0;
    read_tls_bytes(body, &mut pos, 2)?; // legacy_version
    read_tls_bytes(body, &mut pos, 32)?; // random
    let session_id_len = *body.get(pos)? as usize;
    pos += 1;
    read_tls_bytes(body, &mut pos, session_id_len)?;
    let cipher_suites_len = read_tls_u16(body, &mut pos)? as usize;
    read_tls_bytes(body, &mut pos, cipher_suites_len)?;
    let compression_methods_len = *body.get(pos)? as usize;
    pos += 1;
    read_tls_bytes(body, &mut pos, compression_methods_len)?;
    let extensions_len = read_tls_u16(body, &mut pos)? as usize;
    let extensions = read_tls_bytes(body, &mut pos, extensions_len)?;

    let mut ext_pos = 0;
    while ext_pos < extensions.len() {
        let extension_type = read_tls_u16(extensions, &mut ext_pos)?;
        let extension_len = read_tls_u16(extensions, &mut ext_pos)? as usize;
        let extension = read_tls_bytes(extensions, &mut ext_pos, extension_len)?;
        if extension_type == 0 {
            return parse_tls_server_name_extension(extension);
        }
    }
    None
}

fn parse_tls_server_name_extension(extension: &[u8]) -> Option<String> {
    let mut pos = 0;
    let list_len = read_tls_u16(extension, &mut pos)? as usize;
    let list = read_tls_bytes(extension, &mut pos, list_len)?;
    let mut list_pos = 0;
    while list_pos < list.len() {
        let name_type = *list.get(list_pos)?;
        list_pos += 1;
        let name_len = read_tls_u16(list, &mut list_pos)? as usize;
        let name = read_tls_bytes(list, &mut list_pos, name_len)?;
        if name_type == 0 && !name.is_empty() {
            return std::str::from_utf8(name)
                .ok()
                .map(|name| name.to_ascii_lowercase());
        }
    }
    None
}

fn read_tls_u16(data: &[u8], pos: &mut usize) -> Option<u16> {
    let bytes = read_tls_bytes(data, pos, 2)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_tls_bytes<'a>(data: &'a [u8], pos: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = pos.checked_add(len)?;
    let out = data.get(*pos..end)?;
    *pos = end;
    Some(out)
}

// ============================================================================
// ICMP echo handling
// ============================================================================
//
// `parse_icmpv4` / `parse_icmpv6` validate that the packet is an echo
// request (type 8 / 128) and feed `Network::Icmp` packets into the rest
// of the route engine. Anything else is dropped at parse time so the
// system stack loop does not waste a routing decision on a stray ICMP
// type that the kernel itself would normally generate.
//
// `handle_system_icmp_packet` mirrors `handle_system_udp_packet` for the
// echo subset: build a `TunPacket`, run the router, and either reply
// with an ICMP Destination Unreachable (reject / unsupported outbound
// fallback) or send the body through the chosen outbound's
// `listen_icmp()` conduit and re-encapsulate the reply back into a tun
// IP packet. The response stays per-flight; flow-level reuse is left
// for a future iteration once burst ping volume justifies it.

const ICMPV4_ECHO_REQUEST: u8 = 8;
const ICMPV4_ECHO_REPLY: u8 = 0;
const ICMPV6_ECHO_REQUEST: u8 = 128;
const ICMPV6_ECHO_REPLY: u8 = 129;

fn icmp_protocol(destination: IpAddr) -> &'static str {
    if destination.is_ipv6() {
        "icmpv6"
    } else {
        "icmp"
    }
}

fn parse_icmpv4(
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
    transport_offset: usize,
) -> HammerResult<ParsedIpPacketView> {
    if transport.len() < 8 {
        return Err(HammerError::internal("short ICMPv4 packet"));
    }
    if transport[0] != ICMPV4_ECHO_REQUEST {
        return Err(HammerError::internal(format!(
            "non-echo ICMPv4 dropped: type={}",
            transport[0]
        )));
    }
    Ok(ParsedIpPacketView {
        network: Network::Icmp,
        source: SocksAddr::ip(source, 0),
        destination: SocksAddr::ip(destination, 0),
        payload_range: transport_offset..transport_offset + transport.len(),
    })
}

fn parse_icmpv6(
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
    transport_offset: usize,
) -> HammerResult<ParsedIpPacketView> {
    if transport.len() < 8 {
        return Err(HammerError::internal("short ICMPv6 packet"));
    }
    if transport[0] != ICMPV6_ECHO_REQUEST {
        return Err(HammerError::internal(format!(
            "non-echo ICMPv6 dropped: type={}",
            transport[0]
        )));
    }
    Ok(ParsedIpPacketView {
        network: Network::Icmp,
        source: SocksAddr::ip(source, 0),
        destination: SocksAddr::ip(destination, 0),
        payload_range: transport_offset..transport_offset + transport.len(),
    })
}

async fn handle_system_icmp_packet<D, R, Q, O>(
    router: Arc<R>,
    dns_router: Arc<Q>,
    outbound: Arc<O>,
    inbound_id: String,
    device: Arc<D>,
    packet: Vec<u8>,
    parsed: ParsedIpPacketView,
) -> HammerResult<()>
where
    D: TunDevice,
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    let payload = parsed.payload(&packet)?;
    let mut metadata = RouteMetadata {
        inbound: inbound_id,
        network: Network::Icmp,
        source: Some(parsed.source.clone()),
        destination: Some(parsed.destination.clone()),
        protocol: icmp_protocol(parsed.destination.host).to_owned(),
        ..Default::default()
    };
    prepare_route_metadata(router.as_ref(), &mut metadata, Some(dns_router.as_ref()))?;
    let decision = router.match_route(&mut metadata)?;
    match decision {
        RouteDecision::HijackDns => {
            // ICMP is not routable to the DNS hijack; this should not be
            // reachable from any sane rule set but we must keep the match
            // exhaustive — drop the packet quietly.
            debug!(
                "drop ICMP packet hitting DNS hijack rule: destination={}",
                parsed.destination
            );
        }
        RouteDecision::Reject { method } => {
            debug!(
                "drop ICMP packet by reject rule: method={method}, destination={}",
                parsed.destination
            );
            if let Ok(response) = icmp_unreachable_packet(&packet) {
                device.send(response).await?;
            }
        }
        RouteDecision::Route {
            target: RouteTarget::Outbound(outbound_id),
        } => {
            let Some(outbound_item) = outbound.get(&outbound_id) else {
                return Err(HammerError::internal(format!(
                    "outbound not found: {outbound_id}"
                )));
            };
            let mut conn = match outbound_item.runtime().listen_icmp().await {
                Ok(conn) => conn,
                Err(err) => {
                    debug!(
                        "outbound {outbound_id} cannot carry ICMP ({err}); replying Destination Unreachable"
                    );
                    if let Ok(response) = icmp_unreachable_packet(&packet) {
                        device.send(response).await?;
                    }
                    return Ok(());
                }
            };
            let dest_ip = parsed.destination.host;
            conn.send_echo(dest_ip, payload).await?;
            let reply = match timeout(Duration::from_secs(2), conn.recv_reply()).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(err)) => {
                    debug!("ICMP recv_reply failed: {err}");
                    return Ok(());
                }
                Err(_) => {
                    debug!(
                        "ICMP recv_reply timed out: destination={}",
                        parsed.destination
                    );
                    return Ok(());
                }
            };
            let response_packet = icmp_echo_reply_packet(&packet, &reply.body)?;
            device.send(response_packet).await?;
        }
        RouteDecision::Route {
            target: RouteTarget::Endpoint(endpoint_id),
        } => {
            debug!("drop ICMP packet for L3 endpoint route outside fast path: {endpoint_id}");
            if let Ok(response) = icmp_unreachable_packet(&packet) {
                device.send(response).await?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_icmp_workers<D, R, Q, O>(
    router: Arc<R>,
    dns_router: Arc<Q>,
    outbound: Arc<O>,
    inbound_id: String,
    device: Arc<D>,
    rx: mpsc::Receiver<IcmpJob>,
) where
    D: TunDevice,
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    let rx = Arc::new(Mutex::new(rx));
    for _ in 0..SYSTEM_ICMP_WORKERS {
        let router = Arc::clone(&router);
        let dns_router = Arc::clone(&dns_router);
        let outbound = Arc::clone(&outbound);
        let inbound_id = inbound_id.clone();
        let device = Arc::clone(&device);
        let rx = Arc::clone(&rx);
        crate::spawn::spawn(async move {
            loop {
                let Some(job) = rx.lock().await.recv().await else {
                    break;
                };
                if let Err(err) = handle_system_icmp_packet(
                    Arc::clone(&router),
                    Arc::clone(&dns_router),
                    Arc::clone(&outbound),
                    inbound_id.clone(),
                    Arc::clone(&device),
                    job.packet,
                    job.parsed,
                )
                .await
                {
                    debug!("handle system ICMP packet: {err}");
                }
            }
        });
    }
}

fn enqueue_system_icmp_packet(
    icmp_tx: &mpsc::Sender<IcmpJob>,
    packet: Vec<u8>,
    parsed: ParsedIpPacketView,
    metrics: &TunMetrics,
) {
    match icmp_tx.try_send(IcmpJob { packet, parsed }) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics.counters.icmp_drop_busy_total.increment(1);
            debug!("drop ICMP packet because ICMP queue is full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            metrics.counters.icmp_drop_busy_total.increment(1);
            debug!("drop ICMP packet because ICMP queue is closed");
        }
    }
}

/// Helper used by `dispatch_route` (the synchronous test/legacy path)
/// to synthesize an ICMP Destination Unreachable when the chosen
/// outbound rejects ICMP. The system loop path uses
/// `icmp_unreachable_packet(&raw_packet)` directly because it has the
/// full IP packet bytes; here we have only metadata + ICMP body so we
/// reconstruct just enough of the IP header to feed the same builders.
fn icmp_unreachable_response_for(metadata: &RouteMetadata, body: &[u8]) -> HammerResult<Vec<u8>> {
    let source = metadata
        .source
        .as_ref()
        .ok_or_else(|| HammerError::internal("ICMP unreachable: missing source"))?;
    let destination = metadata
        .destination
        .as_ref()
        .ok_or_else(|| HammerError::internal("ICMP unreachable: missing destination"))?;
    let synthetic = synthesize_ip_packet(source.host, destination.host, body)?;
    icmp_unreachable_packet(&synthetic)
}

fn synthesize_ip_packet(source: IpAddr, destination: IpAddr, body: &[u8]) -> HammerResult<Vec<u8>> {
    match (source, destination) {
        (IpAddr::V4(src), IpAddr::V4(dst)) => {
            let total_len = 20 + body.len();
            let mut packet = vec![0_u8; total_len];
            packet[0] = 0x45;
            write_u16(&mut packet, 2, total_len as u16);
            packet[8] = 64;
            packet[9] = 1;
            packet[12..16].copy_from_slice(&src.octets());
            packet[16..20].copy_from_slice(&dst.octets());
            packet[20..].copy_from_slice(body);
            let ip_checksum = checksum(&packet[..20]);
            write_u16(&mut packet, 10, ip_checksum);
            Ok(packet)
        }
        (IpAddr::V6(src), IpAddr::V6(dst)) => {
            let payload_len = body.len();
            let mut packet = vec![0_u8; 40 + payload_len];
            packet[0] = 0x60;
            packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
            packet[6] = 58;
            packet[7] = 64;
            packet[8..24].copy_from_slice(&src.octets());
            packet[24..40].copy_from_slice(&dst.octets());
            packet[40..].copy_from_slice(body);
            Ok(packet)
        }
        _ => Err(HammerError::internal(
            "ICMP synthesize: mixed v4/v6 source/destination",
        )),
    }
}

pub fn icmp_unreachable_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    match IpVersion::from_packet(request)? {
        IpVersion::V4 => ipv4_icmp_unreachable_packet(request),
        IpVersion::V6 => ipv6_icmp_unreachable_packet(request),
    }
}

#[cfg(feature = "endpoint")]
fn ipv4_packet_too_big_packet(request: &[u8], mtu: usize) -> HammerResult<Vec<u8>> {
    let header = ipv4_header_slice(request)?;
    let ihl = header.slice().len();
    let total_len = header.total_len() as usize;
    let quoted_len = total_len.min(ihl + Icmpv4Header::MIN_LEN);
    let icmp_header = Icmpv4Header::with_checksum(
        Icmpv4Type::DestinationUnreachable(icmpv4::DestUnreachableHeader::FragmentationNeeded {
            next_hop_mtu: mtu.min(u16::MAX as usize) as u16,
        }),
        &request[..quoted_len],
    );
    let total_len = IPV4_HEADER_MIN_LEN + Icmpv4Header::MIN_LEN + quoted_len;
    let mut packet = vec![0_u8; total_len];
    packet[0] = 0x45;
    write_u16(&mut packet, IPV4_TOTAL_LENGTH_OFFSET, total_len as u16);
    packet[IPV4_TTL_OFFSET] = DEFAULT_PACKET_TTL;
    packet[IPV4_PROTOCOL_OFFSET] = u8::from(IpNumber::ICMP);
    packet[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET]
        .copy_from_slice(&request[IPV4_DESTINATION_OFFSET..IPV4_HEADER_MIN_LEN]);
    packet[IPV4_DESTINATION_OFFSET..IPV4_HEADER_MIN_LEN]
        .copy_from_slice(&request[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET]);
    packet[IPV4_HEADER_MIN_LEN..IPV4_HEADER_MIN_LEN + Icmpv4Header::MIN_LEN]
        .copy_from_slice(icmp_header.to_bytes().as_slice());
    packet[IPV4_HEADER_MIN_LEN + Icmpv4Header::MIN_LEN..].copy_from_slice(&request[..quoted_len]);
    update_ipv4_header_checksum(&mut packet, IPV4_HEADER_MIN_LEN);
    Ok(packet)
}

#[cfg(feature = "endpoint")]
fn ipv6_packet_too_big_packet(request: &[u8], mtu: usize) -> HammerResult<Vec<u8>> {
    if request.len() < IPV6_HEADER_LEN {
        return Err(HammerError::internal("invalid IPv6 packet"));
    }
    const IPV6_MIN_MTU: usize = 1280;
    let quoted_len = request
        .len()
        .min(IPV6_MIN_MTU - IPV6_HEADER_LEN - Icmpv6Header::MIN_LEN);
    let payload_len = Icmpv6Header::MIN_LEN + quoted_len;
    let source = <[u8; 16]>::try_from(&request[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN])
        .expect("IPv6 destination slice length");
    let destination = <[u8; 16]>::try_from(&request[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET])
        .expect("IPv6 source slice length");
    let icmp_header = Icmpv6Header::with_checksum(
        Icmpv6Type::PacketTooBig {
            mtu: mtu.min(u32::MAX as usize) as u32,
        },
        source,
        destination,
        &request[..quoted_len],
    )
    .map_err(|err| HammerError::internal(format!("ICMPv6 packet too big checksum: {err}")))?;
    let mut packet = vec![0_u8; IPV6_HEADER_LEN + payload_len];
    packet[0] = 0x60;
    write_u16(&mut packet, IPV6_PAYLOAD_LEN_OFFSET, payload_len as u16);
    packet[IPV6_PROTOCOL_OFFSET] = u8::from(IpNumber::IPV6_ICMP);
    packet[IPV6_HOP_LIMIT_OFFSET] = DEFAULT_PACKET_TTL;
    packet[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET].copy_from_slice(&source);
    packet[IPV6_DESTINATION_OFFSET..IPV6_HEADER_LEN].copy_from_slice(&destination);
    packet[IPV6_HEADER_LEN..IPV6_HEADER_LEN + Icmpv6Header::MIN_LEN]
        .copy_from_slice(icmp_header.to_bytes().as_slice());
    packet[IPV6_HEADER_LEN + Icmpv6Header::MIN_LEN..].copy_from_slice(&request[..quoted_len]);
    Ok(packet)
}

fn ipv4_icmp_unreachable_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    if request.len() < 20 {
        return Err(HammerError::internal("invalid IPv4 packet"));
    }
    let ihl = ((request[0] & 0x0f) as usize) * 4;
    if ihl < 20 || request.len() < ihl + 8 {
        return Err(HammerError::internal("invalid IPv4 header"));
    }
    let quoted_len = request.len().min(ihl + 8);
    let total_len = 20 + 8 + quoted_len;
    let mut packet = vec![0_u8; total_len];
    packet[0] = 0x45;
    write_u16(&mut packet, 2, total_len as u16);
    packet[8] = 64;
    packet[9] = 1;
    // Swap src / dst from the request so the unreachable comes back to
    // the originator.
    packet[12..16].copy_from_slice(&request[16..20]);
    packet[16..20].copy_from_slice(&request[12..16]);
    packet[20] = 3; // type 3 = Destination Unreachable
    packet[21] = 1; // code 1 = host unreachable (matches "outbound rejected ICMP")
    packet[28..].copy_from_slice(&request[..quoted_len]);
    let ip_checksum = checksum(&packet[..20]);
    write_u16(&mut packet, 10, ip_checksum);
    let icmp_checksum = checksum(&packet[20..]);
    write_u16(&mut packet, 22, icmp_checksum);
    Ok(packet)
}

fn ipv6_icmp_unreachable_packet(request: &[u8]) -> HammerResult<Vec<u8>> {
    if request.len() < 40 {
        return Err(HammerError::internal("invalid IPv6 packet"));
    }
    let quoted_len = request.len().min(1232);
    let payload_len = 8 + quoted_len;
    let mut packet = vec![0_u8; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = 58; // next header = ICMPv6
    packet[7] = 64;
    packet[8..24].copy_from_slice(&request[24..40]);
    packet[24..40].copy_from_slice(&request[8..24]);
    packet[40] = 1; // type 1 = Destination Unreachable
    packet[41] = 1; // code 1 = communication administratively prohibited
    packet[48..].copy_from_slice(&request[..quoted_len]);
    update_ipv6_icmp_checksum(&mut packet)?;
    Ok(packet)
}

/// Wrap the kernel-delivered ICMP reply body (starting at the ICMP
/// type byte) into a tun-deliverable IP packet. We swap src/dst from
/// the original echo request so the reply reaches the originating app.
pub fn icmp_echo_reply_packet(request: &[u8], reply_body: &[u8]) -> HammerResult<Vec<u8>> {
    match IpVersion::from_packet(request)? {
        IpVersion::V4 => ipv4_icmp_echo_reply_packet(request, reply_body),
        IpVersion::V6 => ipv6_icmp_echo_reply_packet(request, reply_body),
    }
}

fn ipv4_icmp_echo_reply_packet(request: &[u8], reply_body: &[u8]) -> HammerResult<Vec<u8>> {
    if request.len() < 20 {
        return Err(HammerError::internal("invalid IPv4 packet"));
    }
    let request_ihl = ((request[0] & 0x0f) as usize) * 4;
    if request_ihl < 20 || request.len() < request_ihl + 8 {
        return Err(HammerError::internal("invalid IPv4 ICMP packet"));
    }
    if reply_body.len() < 8 {
        return Err(HammerError::internal("ICMPv4 reply too short"));
    }
    let total_len = 20 + reply_body.len();
    let mut packet = vec![0_u8; total_len];
    packet[0] = 0x45;
    write_u16(&mut packet, 2, total_len as u16);
    packet[8] = 64;
    packet[9] = 1; // ICMP
    // src = original dst, dst = original src (the originating app's IP).
    packet[12..16].copy_from_slice(&request[16..20]);
    packet[16..20].copy_from_slice(&request[12..16]);
    let ip_checksum = checksum(&packet[..20]);
    write_u16(&mut packet, 10, ip_checksum);
    packet[20..].copy_from_slice(reply_body);
    // Force the ICMP type to echo reply — the kernel-delivered body
    // already carries type=0 in well-behaved cases, but we normalise
    // defensively in case a custom outbound passes back a literal echo.
    packet[20] = ICMPV4_ECHO_REPLY;
    packet[24..28].copy_from_slice(&request[request_ihl + 4..request_ihl + 8]);
    write_u16(&mut packet, 22, 0);
    let icmp_checksum = checksum(&packet[20..]);
    write_u16(&mut packet, 22, icmp_checksum);
    Ok(packet)
}

fn ipv6_icmp_echo_reply_packet(request: &[u8], reply_body: &[u8]) -> HammerResult<Vec<u8>> {
    if request.len() < 48 {
        return Err(HammerError::internal("invalid IPv6 ICMP packet"));
    }
    if reply_body.len() < 8 {
        return Err(HammerError::internal("ICMPv6 reply too short"));
    }
    let payload_len = reply_body.len();
    let mut packet = vec![0_u8; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = 58; // ICMPv6
    packet[7] = 64;
    packet[8..24].copy_from_slice(&request[24..40]);
    packet[24..40].copy_from_slice(&request[8..24]);
    packet[40..].copy_from_slice(reply_body);
    packet[40] = ICMPV6_ECHO_REPLY;
    packet[44..48].copy_from_slice(&request[44..48]);
    update_ipv6_icmp_checksum(&mut packet)?;
    Ok(packet)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dns::FixedResponseCode;
    use crate::{DnsRouter, DnsTransportManager, OutboundManager, Router};
    use async_trait::async_trait;
    use hammer_adapter::{
        ComponentMeta, DnsRouter as AdapterDnsRouter, DnsTransport, DnsTransportComponent,
        Lifecycle, OutboundManager as AdapterOutboundManager, Router as AdapterRouter,
        RuntimeComponent, StartStage,
    };
    use hammer_core::config;
    use hammer_core::log::{DiscardWriter, Factory};
    use hickory_proto::op::ResponseCode;
    use hickory_proto::rr::{RData, Record};
    use std::collections::VecDeque;
    use std::io;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
    use tokio::sync::Notify;

    fn test_logger(id: &str) -> Logger {
        Factory::new(StdInstant::now(), Arc::new(DiscardWriter)).new_logger(id)
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

    #[test]
    fn system_tun_stack_type_carries_concrete_device() {
        let _ = std::any::type_name::<
            SystemTunStack<MemoryTunDevice, Router, DnsRouter, OutboundManager>,
        >();
    }

    struct StubRouter;
    struct StubDnsRouter;
    struct StubOutboundManager;
    struct CountingRouter {
        should_sniff_calls: AtomicUsize,
        prepare_calls: AtomicUsize,
        match_calls: AtomicUsize,
        decision: RouteDecision,
    }
    #[cfg(feature = "endpoint")]
    struct EndpointThenRejectRouter {
        match_calls: AtomicUsize,
    }
    #[cfg(feature = "endpoint")]
    struct TcpSniffWaitRouter {
        match_calls: AtomicUsize,
    }
    #[cfg(feature = "endpoint")]
    struct DomainSplitRouter {
        match_calls: AtomicUsize,
    }
    #[cfg(feature = "endpoint")]
    struct ReverseMappingDnsRouter;
    #[cfg(feature = "endpoint")]
    struct RecordingEndpoint {
        tx: mpsc::Sender<Bytes>,
        batch_tx: Option<mpsc::Sender<Vec<Bytes>>>,
        mtu: Option<usize>,
    }

    impl CountingRouter {
        fn rejecting() -> Arc<Self> {
            Arc::new(Self {
                should_sniff_calls: AtomicUsize::new(0),
                prepare_calls: AtomicUsize::new(0),
                match_calls: AtomicUsize::new(0),
                decision: RouteDecision::Reject {
                    method: "default".to_owned(),
                },
            })
        }
    }

    impl Lifecycle for StubRouter {
        fn name(&self) -> &str {
            "stub-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    impl AdapterRouter for StubRouter {
        fn reset_network(&self) {}

        fn match_route(&self, _metadata: &mut RouteMetadata) -> HammerResult<RouteDecision> {
            Ok(RouteDecision::Reject {
                method: "default".to_owned(),
            })
        }

        fn prepare_route_metadata(&self, _metadata: &mut RouteMetadata) -> HammerResult<()> {
            Ok(())
        }

        fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<Duration> {
            None
        }

        fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
            false
        }
    }

    impl Lifecycle for CountingRouter {
        fn name(&self) -> &str {
            "counting-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    impl AdapterRouter for CountingRouter {
        fn reset_network(&self) {}

        fn match_route(&self, _metadata: &mut RouteMetadata) -> HammerResult<RouteDecision> {
            self.match_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.decision.clone())
        }

        fn prepare_route_metadata(&self, _metadata: &mut RouteMetadata) -> HammerResult<()> {
            self.prepare_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<Duration> {
            None
        }

        fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
            self.should_sniff_calls.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    #[cfg(feature = "endpoint")]
    impl Lifecycle for EndpointThenRejectRouter {
        fn name(&self) -> &str {
            "endpoint-then-reject-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[cfg(feature = "endpoint")]
    impl AdapterRouter for EndpointThenRejectRouter {
        fn reset_network(&self) {}

        fn match_route(&self, _metadata: &mut RouteMetadata) -> HammerResult<RouteDecision> {
            if self.match_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(RouteDecision::Route {
                    target: RouteTarget::Endpoint("wg-out".to_owned()),
                })
            } else {
                Ok(RouteDecision::Reject {
                    method: "default".to_owned(),
                })
            }
        }

        fn prepare_route_metadata(&self, _metadata: &mut RouteMetadata) -> HammerResult<()> {
            Ok(())
        }

        fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<Duration> {
            None
        }

        fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
            false
        }
    }

    #[cfg(feature = "endpoint")]
    impl Lifecycle for TcpSniffWaitRouter {
        fn name(&self) -> &str {
            "tcp-sniff-wait-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[cfg(feature = "endpoint")]
    impl AdapterRouter for TcpSniffWaitRouter {
        fn reset_network(&self) {}

        fn match_route(&self, _metadata: &mut RouteMetadata) -> HammerResult<RouteDecision> {
            self.match_calls.fetch_add(1, Ordering::Relaxed);
            Ok(RouteDecision::Route {
                target: RouteTarget::Endpoint("wg-out".to_owned()),
            })
        }

        fn prepare_route_metadata(&self, _metadata: &mut RouteMetadata) -> HammerResult<()> {
            Ok(())
        }

        fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<Duration> {
            Some(Duration::from_millis(1))
        }

        fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
            true
        }
    }

    #[cfg(feature = "endpoint")]
    impl Lifecycle for DomainSplitRouter {
        fn name(&self) -> &str {
            "domain-split-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[cfg(feature = "endpoint")]
    impl AdapterRouter for DomainSplitRouter {
        fn reset_network(&self) {}

        fn match_route(&self, metadata: &mut RouteMetadata) -> HammerResult<RouteDecision> {
            self.match_calls.fetch_add(1, Ordering::Relaxed);
            if metadata.domain.as_deref() == Some("ifconfig.so") {
                Ok(RouteDecision::Route {
                    target: RouteTarget::Outbound("direct".to_owned()),
                })
            } else {
                Ok(RouteDecision::Route {
                    target: RouteTarget::Endpoint("wg-out".to_owned()),
                })
            }
        }

        fn prepare_route_metadata(&self, _metadata: &mut RouteMetadata) -> HammerResult<()> {
            Ok(())
        }

        fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<Duration> {
            None
        }

        fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
            false
        }
    }

    #[cfg(feature = "endpoint")]
    impl Lifecycle for RecordingEndpoint {
        fn name(&self) -> &str {
            "recording-endpoint"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[cfg(feature = "endpoint")]
    impl EndpointTrait for RecordingEndpoint {
        fn id(&self) -> &str {
            "wg-out"
        }

        fn ip_send_clone(&self) -> mpsc::Sender<Bytes> {
            self.tx.clone()
        }

        fn ip_send_batch_clone(&self) -> Option<mpsc::Sender<Vec<Bytes>>> {
            self.batch_tx.clone()
        }

        fn ip_packet_mtu(&self) -> Option<usize> {
            self.mtu
        }

        fn ip_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
            None
        }

        fn allowed_destinations(&self) -> Vec<IpNet> {
            vec![
                "0.0.0.0/0".parse().expect("default IPv4 route"),
                "::/0".parse().expect("default IPv6 route"),
            ]
        }

        fn interface_addresses(&self) -> Vec<IpNet> {
            vec!["10.66.0.2/32".parse().expect("endpoint address")]
        }
    }

    impl Lifecycle for StubDnsRouter {
        fn name(&self) -> &str {
            "stub-dns-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl AdapterDnsRouter for StubDnsRouter {
        async fn exchange(
            &self,
            message: Message,
            _options: DnsQueryOptions,
        ) -> HammerResult<Message> {
            Ok(message)
        }

        async fn lookup(
            &self,
            _domain: &str,
            _options: DnsQueryOptions,
        ) -> HammerResult<Vec<IpAddr>> {
            Ok(Vec::new())
        }

        fn try_exchange_fast(
            &self,
            _message: &Message,
            _options: DnsQueryOptions,
        ) -> HammerResult<Option<Message>> {
            Ok(None)
        }

        fn clear_cache(&self) {}

        fn lookup_reverse_mapping(&self, _ip: IpAddr) -> Option<String> {
            None
        }

        fn reset_network(&self) {}
    }

    #[cfg(feature = "endpoint")]
    impl Lifecycle for ReverseMappingDnsRouter {
        fn name(&self) -> &str {
            "reverse-mapping-dns-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[cfg(feature = "endpoint")]
    #[async_trait]
    impl AdapterDnsRouter for ReverseMappingDnsRouter {
        async fn exchange(
            &self,
            message: Message,
            _options: DnsQueryOptions,
        ) -> HammerResult<Message> {
            Ok(message)
        }

        async fn lookup(
            &self,
            _domain: &str,
            _options: DnsQueryOptions,
        ) -> HammerResult<Vec<IpAddr>> {
            Ok(Vec::new())
        }

        fn try_exchange_fast(
            &self,
            _message: &Message,
            _options: DnsQueryOptions,
        ) -> HammerResult<Option<Message>> {
            Ok(None)
        }

        fn clear_cache(&self) {}

        fn lookup_reverse_mapping(&self, ip: IpAddr) -> Option<String> {
            (ip == IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))).then(|| "ifconfig.so".to_owned())
        }

        fn reset_network(&self) {}
    }

    impl Lifecycle for StubOutboundManager {
        fn name(&self) -> &str {
            "stub-outbound"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    impl AdapterOutboundManager for StubOutboundManager {
        fn list(&self) -> Vec<hammer_adapter::OutboundComponent> {
            Vec::new()
        }

        fn get(&self, _id: &str) -> Option<hammer_adapter::OutboundComponent> {
            None
        }

        fn default(&self) -> Option<hammer_adapter::OutboundComponent> {
            None
        }

        fn remove(&self, _id: &str) -> HammerResult<()> {
            Ok(())
        }
    }

    #[test]
    fn tun_stack_types_accept_concrete_service_dependencies() {
        let _ = std::any::type_name::<
            SystemTunStack<MemoryTunDevice, StubRouter, StubDnsRouter, StubOutboundManager>,
        >();
        let _ =
            std::any::type_name::<PacketTunStack<StubRouter, StubDnsRouter, StubOutboundManager>>();
    }

    #[test]
    fn tun_metrics_register_counters_under_scope_component_id() {
        let registry = MetricsRegistry::new();
        let metrics = TunMetrics::new(registry.scope("inbound", "tun", "vpn-main"));
        metrics.counters.packet_recv_error_total.increment(1);

        let samples = registry.snapshot();
        assert!(
            samples.iter().any(|sample| sample.module == "inbound"
                && sample.component_type == "tun"
                && sample.component_id == "vpn-main"
                && sample.name == "packet_recv_error_total"
                && sample.value == 1),
            "samples = {samples:?}"
        );
    }

    #[test]
    fn tun_metrics_do_not_leak_between_registries() {
        let registry_a = MetricsRegistry::new();
        let registry_b = MetricsRegistry::new();
        let metrics_a = TunMetrics::new(registry_a.scope("inbound", "tun", "tun-a"));
        let metrics_b = TunMetrics::new(registry_b.scope("inbound", "tun", "tun-b"));

        metrics_a.counters.packet_recv_error_total.increment(3);
        metrics_b.counters.packet_recv_error_total.increment(5);

        let samples_a = registry_a.snapshot();
        let samples_b = registry_b.snapshot();
        assert!(
            samples_a.iter().any(|sample| sample.component_id == "tun-a"
                && sample.name == "packet_recv_error_total"
                && sample.value == 3),
            "samples_a = {samples_a:?}"
        );
        assert!(
            samples_b.iter().any(|sample| sample.component_id == "tun-b"
                && sample.name == "packet_recv_error_total"
                && sample.value == 5),
            "samples_b = {samples_b:?}"
        );
        assert!(
            !samples_a
                .iter()
                .any(|sample| sample.component_id == "tun-b"),
            "samples_a = {samples_a:?}"
        );
        assert!(
            !samples_b
                .iter()
                .any(|sample| sample.component_id == "tun-a"),
            "samples_b = {samples_b:?}"
        );
    }

    #[test]
    fn tun_send_backpressure_does_not_clear_asyncfd_readiness() {
        let enospc = io::Error::from_raw_os_error(libc::ENOSPC);
        let enobufs = io::Error::from_raw_os_error(libc::ENOBUFS);

        assert!(is_transient_tun_send_backpressure(&enospc));
        assert!(is_transient_tun_send_backpressure(&enobufs));
        assert!(!should_clear_tun_send_readiness(&enospc));
        assert!(!should_clear_tun_send_readiness(&enobufs));
    }

    #[test]
    fn tun_send_would_block_clears_asyncfd_readiness() {
        let would_block = io::Error::from(io::ErrorKind::WouldBlock);

        assert!(!is_transient_tun_send_backpressure(&would_block));
        assert!(should_clear_tun_send_readiness(&would_block));
    }

    #[test]
    fn system_udp_defaults_stay_within_netext_memory_budget() {
        assert!(
            DEFAULT_SYSTEM_UDP_TIMEOUT <= Duration::from_secs(30),
            "idle video/QUIC UDP flows retain WireGuard UDP sockets until timeout"
        );
        assert!(
            SYSTEM_UDP_FLOW_CAPACITY <= 256,
            "each routed UDP flow can own a WireGuard ipstack UDP socket"
        );
    }

    #[test]
    fn tcp_pending_dial_limiter_rejects_when_full_and_releases_permit() {
        let limiter = TcpPendingDialLimiter::new(1);
        let first = limiter.try_acquire().expect("first dial permit");

        let err = match limiter.try_acquire() {
            Ok(_) => panic!("second permit should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("pending outbound TCP dial limit reached")
        );

        drop(first);
        let _second = limiter.try_acquire().expect("permit released after drop");
    }

    struct DelayedDnsTransport;

    impl Lifecycle for DelayedDnsTransport {
        fn name(&self) -> &str {
            "delayed-dns"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl DnsTransport for DelayedDnsTransport {
        fn reset(&self) {}

        async fn exchange(&self, message: Message) -> HammerResult<Message> {
            tokio::time::sleep(Duration::from_millis(200)).await;
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

    struct CountingDnsTransport {
        queries: Arc<AtomicUsize>,
    }

    struct BlockingDnsRouter {
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        started_tx: mpsc::UnboundedSender<()>,
        release: Notify,
    }

    impl Lifecycle for CountingDnsTransport {
        fn name(&self) -> &str {
            "counting-dns"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl DnsTransport for CountingDnsTransport {
        fn reset(&self) {}

        async fn exchange(&self, message: Message) -> HammerResult<Message> {
            self.queries.fetch_add(1, Ordering::Relaxed);
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

    impl BlockingDnsRouter {
        fn new(started_tx: mpsc::UnboundedSender<()>) -> Arc<Self> {
            Arc::new(Self {
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                started_tx,
                release: Notify::new(),
            })
        }
    }

    impl Lifecycle for BlockingDnsRouter {
        fn name(&self) -> &str {
            "blocking-dns-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl AdapterDnsRouter for BlockingDnsRouter {
        async fn exchange(
            &self,
            message: Message,
            _options: DnsQueryOptions,
        ) -> HammerResult<Message> {
            let current = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            self.max_in_flight.fetch_max(current, Ordering::Relaxed);
            let _ = self.started_tx.send(());
            self.release.notified().await;
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
            Ok(message.fixed_response(FixedResponseCode::NoError))
        }

        async fn lookup(
            &self,
            _domain: &str,
            _options: DnsQueryOptions,
        ) -> HammerResult<Vec<IpAddr>> {
            Ok(Vec::new())
        }

        fn try_exchange_fast(
            &self,
            _message: &Message,
            _options: DnsQueryOptions,
        ) -> HammerResult<Option<Message>> {
            Ok(None)
        }

        fn clear_cache(&self) {}

        fn lookup_reverse_mapping(&self, _ip: IpAddr) -> Option<String> {
            None
        }

        fn reset_network(&self) {}
    }

    struct ScriptedBatchTunDevice {
        batches: Mutex<VecDeque<Vec<Vec<u8>>>>,
        read_after_batches: Notify,
        output_tx: mpsc::Sender<Vec<u8>>,
        closed: AtomicBool,
    }

    impl ScriptedBatchTunDevice {
        fn new(batches: Vec<Vec<Vec<u8>>>) -> Arc<Self> {
            let (output_tx, _output_rx) = mpsc::channel(8);
            Arc::new(Self {
                batches: Mutex::new(VecDeque::from(batches)),
                read_after_batches: Notify::new(),
                output_tx,
                closed: AtomicBool::new(false),
            })
        }

        async fn wait_for_read_after_batches(&self) {
            self.read_after_batches.notified().await;
        }
    }

    #[async_trait]
    impl TunDevice for ScriptedBatchTunDevice {
        async fn recv(&self) -> HammerResult<Vec<u8>> {
            Err(HammerError::internal("scripted tun recv is not used"))
        }

        async fn send(&self, packet: Vec<u8>) -> HammerResult<()> {
            self.output_tx
                .send(packet)
                .await
                .map_err(|_| HammerError::internal("scripted tun output closed"))
        }

        async fn recv_batch(&self, _max: usize) -> HammerResult<Vec<Vec<u8>>> {
            if self.closed.load(Ordering::Relaxed) {
                return Err(HammerError::internal("scripted tun closed"));
            }
            let mut batches = self.batches.lock().await;
            if let Some(batch) = batches.pop_front() {
                return Ok(batch);
            }
            self.read_after_batches.notify_waiters();
            Err(HammerError::internal("scripted tun done"))
        }

        fn close(&self) {
            self.closed.store(true, Ordering::Relaxed);
        }
    }

    struct BatchRecordingTunDevice {
        batch_sizes: Mutex<Vec<usize>>,
        packets: Mutex<Vec<Vec<u8>>>,
    }

    impl BatchRecordingTunDevice {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                batch_sizes: Mutex::new(Vec::new()),
                packets: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl TunDevice for BatchRecordingTunDevice {
        async fn recv(&self) -> HammerResult<Vec<u8>> {
            Err(HammerError::internal(
                "batch recording tun recv is not used",
            ))
        }

        async fn send(&self, packet: Vec<u8>) -> HammerResult<()> {
            self.batch_sizes.lock().await.push(1);
            self.packets.lock().await.push(packet);
            Ok(())
        }

        async fn send_batch(&self, packets: &mut Vec<Vec<u8>>) -> HammerResult<()> {
            self.batch_sizes.lock().await.push(packets.len());
            self.packets.lock().await.extend(packets.drain(..));
            Ok(())
        }

        fn close(&self) {}
    }

    fn dns_query_packet(name: &str) -> Vec<u8> {
        let mut payload = vec![0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0];
        for label in name.split('.') {
            payload.push(label.len() as u8);
            payload.extend_from_slice(label.as_bytes());
        }
        payload.extend_from_slice(&[0, 0, 1, 0, 1]);

        let total_len = 20 + 8 + payload.len();
        let mut packet = vec![0u8; total_len];
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
        packet[8] = 64;
        packet[9] = 17;
        packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
        packet[16..20].copy_from_slice(&[1, 1, 1, 1]);
        packet[20..22].copy_from_slice(&5353_u16.to_be_bytes());
        packet[22..24].copy_from_slice(&53_u16.to_be_bytes());
        packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
        packet[28..].copy_from_slice(&payload);
        packet
    }

    fn udp_packet(source_port: u16, destination_port: u16, payload: &[u8]) -> Vec<u8> {
        let total_len = 20 + 8 + payload.len();
        let mut packet = vec![0u8; total_len];
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
        packet[8] = 64;
        packet[9] = 17;
        packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
        packet[16..20].copy_from_slice(&[1, 1, 1, 1]);
        packet[20..22].copy_from_slice(&source_port.to_be_bytes());
        packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
        packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
        packet[28..].copy_from_slice(payload);
        packet
    }

    #[cfg(feature = "endpoint")]
    fn udp_packet_with_source(
        source: [u8; 4],
        destination: [u8; 4],
        source_port: u16,
        destination_port: u16,
        payload: &[u8],
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
        packet[28..].copy_from_slice(payload);
        update_ipv4_header_checksum(&mut packet, IPV4_HEADER_MIN_LEN);
        update_ipv4_udp_checksums(&mut packet, IPV4_HEADER_MIN_LEN).expect("udp checksum");
        packet
    }

    #[cfg(feature = "endpoint")]
    fn ipv6_udp_packet(payload: &[u8]) -> Vec<u8> {
        let payload_len = UDP_HEADER_LEN + payload.len();
        let mut packet = vec![0u8; IPV6_HEADER_LEN + payload_len];
        packet[0] = 0x60;
        packet[IPV6_PAYLOAD_LEN_OFFSET..IPV6_PAYLOAD_LEN_OFFSET + 2]
            .copy_from_slice(&(payload_len as u16).to_be_bytes());
        packet[IPV6_PROTOCOL_OFFSET] = IpProtocol::Udp.wire_value();
        packet[IPV6_HOP_LIMIT_OFFSET] = DEFAULT_PACKET_TTL;
        packet[IPV6_SOURCE_OFFSET..IPV6_DESTINATION_OFFSET]
            .copy_from_slice(&Ipv6Addr::LOCALHOST.octets());
        packet[IPV6_DESTINATION_OFFSET..IPV6_DESTINATION_OFFSET + 16]
            .copy_from_slice(&Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111).octets());
        let udp = IPV6_HEADER_LEN;
        write_u16(&mut packet, udp + UDP_SOURCE_PORT_OFFSET, 5353);
        write_u16(&mut packet, udp + UDP_DESTINATION_PORT_OFFSET, 443);
        write_u16(&mut packet, udp + UDP_LENGTH_OFFSET, payload_len as u16);
        packet[udp + UDP_HEADER_LEN..].copy_from_slice(payload);
        update_ipv6_udp_checksum(&mut packet).expect("udp checksum");
        packet
    }

    #[cfg(feature = "endpoint")]
    fn tcp_packet(source_port: u16, destination_port: u16, flags: u8, payload: &[u8]) -> Vec<u8> {
        let total_len = IPV4_HEADER_MIN_LEN + TCP_HEADER_MIN_LEN + payload.len();
        let mut packet = vec![0u8; total_len];
        packet[0] = 0x45;
        packet[IPV4_TTL_OFFSET] = DEFAULT_PACKET_TTL;
        packet[IPV4_PROTOCOL_OFFSET] = IpProtocol::Tcp.wire_value();
        packet[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET].copy_from_slice(&[172, 19, 0, 1]);
        packet[IPV4_DESTINATION_OFFSET..IPV4_DESTINATION_OFFSET + 4].copy_from_slice(&[1, 1, 1, 1]);
        write_u16(&mut packet, IPV4_TOTAL_LENGTH_OFFSET, total_len as u16);
        let tcp = IPV4_HEADER_MIN_LEN;
        write_u16(&mut packet, tcp + TCP_SOURCE_PORT_OFFSET, source_port);
        write_u16(
            &mut packet,
            tcp + TCP_DESTINATION_PORT_OFFSET,
            destination_port,
        );
        packet[tcp + TCP_DATA_OFFSET_OFFSET] = 0x50;
        packet[tcp + 13] = flags;
        packet[tcp + TCP_HEADER_MIN_LEN..].copy_from_slice(payload);
        update_ipv4_header_checksum(&mut packet, IPV4_HEADER_MIN_LEN);
        update_ipv4_tcp_checksums(&mut packet, IPV4_HEADER_MIN_LEN).expect("tcp checksum");
        packet
    }

    fn dns_message_from_packet(packet: &[u8]) -> Message {
        let parsed = parse_ip_packet_view(packet).expect("parse DNS packet");
        <Message as MessageExt>::from_bytes(parsed.payload(packet).expect("DNS payload"))
            .expect("parse DNS message")
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn l3_endpoint_egress_rewrites_tun_source_to_endpoint_address() {
        let mut packet = udp_packet(5353, 53, b"hello");
        rewrite_l3_packet_source(
            &mut packet,
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)),
        )
        .expect("rewrite source");

        let parsed = parse_ip_packet_view(&packet).expect("parse rewritten packet");
        assert_eq!(parsed.source.host, IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)));
        assert_ne!(read_u16(&packet, IPV4_CHECKSUM_OFFSET), 0);
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn l3_endpoint_rewrite_rejects_truncated_udp_without_mutating_packet() {
        let mut packet = vec![0_u8; IPV4_HEADER_MIN_LEN];
        packet[0] = 0x45;
        packet[IPV4_TTL_OFFSET] = DEFAULT_PACKET_TTL;
        packet[IPV4_PROTOCOL_OFFSET] = IpProtocol::Udp.wire_value();
        packet[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET].copy_from_slice(&[10, 0, 0, 2]);
        packet[IPV4_DESTINATION_OFFSET..IPV4_DESTINATION_OFFSET + 4]
            .copy_from_slice(&[10, 66, 0, 2]);
        write_u16(
            &mut packet,
            IPV4_TOTAL_LENGTH_OFFSET,
            IPV4_HEADER_MIN_LEN as u16,
        );
        update_ipv4_header_checksum(&mut packet, IPV4_HEADER_MIN_LEN);
        let before = packet.clone();

        let err = rewrite_l3_packet_destination(
            &mut packet,
            IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        )
        .expect_err("truncated UDP must be rejected");

        assert!(
            err.to_string().contains("short UDP datagram"),
            "error = {err:?}"
        );
        assert_eq!(packet, before);
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn l3_endpoint_ingress_rewrites_initial_ipv4_fragment_checksum_delta() {
        let packet =
            udp_packet_with_source([8, 8, 8, 8], [10, 66, 0, 2], 5353, 443, &vec![0x42; 96]);
        let mut expected = packet.clone();
        rewrite_l3_packet_destination(
            &mut expected,
            IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        )
        .expect("full packet rewrite");
        let fragments = fragment_ipv4_packet(&packet, 68).expect("fragments");
        let mut first = fragments[0].clone();

        rewrite_l3_packet_destination(
            &mut first,
            IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        )
        .expect("initial fragment rewrite");

        assert_eq!(
            &first[IPV4_DESTINATION_OFFSET..IPV4_DESTINATION_OFFSET + 4],
            &[172, 19, 0, 1]
        );
        assert_eq!(checksum(&first[..IPV4_HEADER_MIN_LEN]), 0);
        assert_eq!(
            read_u16(&first, IPV4_HEADER_MIN_LEN + UDP_CHECKSUM_OFFSET),
            read_u16(&expected, IPV4_HEADER_MIN_LEN + UDP_CHECKSUM_OFFSET)
        );
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn l3_endpoint_ingress_rewrites_non_initial_ipv4_fragment_without_transport_header() {
        let packet =
            udp_packet_with_source([8, 8, 8, 8], [10, 66, 0, 2], 5353, 443, &vec![0x42; 96]);
        let fragments = fragment_ipv4_packet(&packet, 68).expect("fragments");
        let mut later = fragments[1].clone();

        rewrite_l3_packet_destination(
            &mut later,
            IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
        )
        .expect("non-initial fragment rewrite");

        assert_eq!(
            &later[IPV4_DESTINATION_OFFSET..IPV4_DESTINATION_OFFSET + 4],
            &[172, 19, 0, 1]
        );
        assert_eq!(checksum(&later[..IPV4_HEADER_MIN_LEN]), 0);
    }

    #[cfg(feature = "endpoint")]
    #[tokio::test]
    async fn l3_endpoint_dispatch_fragments_oversized_ipv4_packets() {
        let payload = vec![0x42; 96];
        let packet = udp_packet_with_source([172, 19, 0, 1], [1, 1, 1, 1], 5353, 443, &payload);
        let parsed = parse_ip_packet_view(&packet).expect("parse packet");
        let (endpoint_tx, mut endpoint_rx) = mpsc::channel(8);
        let endpoint: Arc<dyn EndpointTrait> = Arc::new(RecordingEndpoint {
            tx: endpoint_tx,
            batch_tx: None,
            mtu: Some(68),
        });
        let addresses = StackAddresses {
            v4: Some(StackAddress {
                listener: Ipv4Addr::new(172, 19, 0, 1),
                next: Ipv4Addr::new(172, 19, 0, 2),
            }),
            v6: None,
        };
        let dispatch = L3DispatchTable::from_endpoints(&[endpoint], &addresses);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));

        assert!(dispatch_endpoint_l3_packet(
            &dispatch, "wg-out", packet, &parsed, &metrics, None,
        ));

        let first = timeout(Duration::from_millis(50), endpoint_rx.recv())
            .await
            .expect("first fragment")
            .expect("first fragment");
        let second = timeout(Duration::from_millis(50), endpoint_rx.recv())
            .await
            .expect("second fragment")
            .expect("second fragment");
        assert!(first.len() <= 68);
        assert!(second.len() <= 68);
        assert_eq!(
            read_u16(&first, IPV4_TOTAL_LENGTH_OFFSET) as usize,
            first.len()
        );
        assert_eq!(
            &first[IPV4_SOURCE_OFFSET..IPV4_DESTINATION_OFFSET],
            &[10, 66, 0, 2]
        );
        assert_eq!(read_u16(&first, 6) & 0x2000, 0x2000);
        assert_eq!(read_u16(&second, 6) & 0x1fff, 6);
    }

    #[cfg(feature = "endpoint-wireguard")]
    #[tokio::test]
    async fn l3_endpoint_dispatch_accepts_one_tun_batch_without_capacity_drop() {
        let packet = udp_packet_with_source([172, 19, 0, 1], [1, 1, 1, 1], 5353, 443, &[0x42; 32]);
        let parsed = parse_ip_packet_view(&packet).expect("parse packet");
        let (endpoint_tx, mut endpoint_rx) = mpsc::channel(1);
        let (endpoint_batch_tx, mut endpoint_batch_rx) = mpsc::channel(1);
        let endpoint: Arc<dyn EndpointTrait> = Arc::new(RecordingEndpoint {
            tx: endpoint_tx,
            batch_tx: Some(endpoint_batch_tx),
            mtu: Some(1400),
        });
        let addresses = StackAddresses {
            v4: Some(StackAddress {
                listener: Ipv4Addr::new(172, 19, 0, 1),
                next: Ipv4Addr::new(172, 19, 0, 2),
            }),
            v6: None,
        };
        let dispatch = L3DispatchTable::from_endpoints(&[endpoint], &addresses);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));
        let mut batches = L3EndpointQueuedBatches::new();

        for _ in 0..SYSTEM_TUN_RECV_BATCH_HINT {
            assert!(queue_endpoint_l3_packet(
                &dispatch,
                "wg-out",
                packet.clone(),
                &parsed,
                &metrics,
                None,
                &mut batches,
            ));
        }
        flush_endpoint_l3_batches(&mut batches, &metrics).await;

        assert!(endpoint_rx.try_recv().is_err());
        let received = endpoint_batch_rx.try_recv().expect("batched packets");
        assert_eq!(received.len(), SYSTEM_TUN_RECV_BATCH_HINT);
    }

    #[cfg(feature = "endpoint")]
    #[tokio::test]
    async fn l3_endpoint_dispatch_returns_icmpv4_packet_too_big_for_df_packets() {
        let payload = vec![0x42; 96];
        let mut packet = udp_packet_with_source([172, 19, 0, 1], [1, 1, 1, 1], 5353, 443, &payload);
        write_u16(&mut packet, 6, 0x4000);
        update_ipv4_header_checksum(&mut packet, IPV4_HEADER_MIN_LEN);
        let parsed = parse_ip_packet_view(&packet).expect("parse packet");
        let (endpoint_tx, mut endpoint_rx) = mpsc::channel(8);
        let endpoint: Arc<dyn EndpointTrait> = Arc::new(RecordingEndpoint {
            tx: endpoint_tx,
            batch_tx: None,
            mtu: Some(68),
        });
        let addresses = StackAddresses {
            v4: Some(StackAddress {
                listener: Ipv4Addr::new(172, 19, 0, 1),
                next: Ipv4Addr::new(172, 19, 0, 2),
            }),
            v6: None,
        };
        let dispatch = L3DispatchTable::from_endpoints(&[endpoint], &addresses);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));
        let (tun_write_tx, mut tun_write_rx) = mpsc::channel::<TunWriteItem>(4);

        assert!(dispatch_endpoint_l3_packet(
            &dispatch,
            "wg-out",
            packet,
            &parsed,
            &metrics,
            Some(&tun_write_tx),
        ));

        assert!(endpoint_rx.try_recv().is_err());
        let response = timeout(Duration::from_millis(50), tun_write_rx.recv())
            .await
            .expect("icmp response")
            .expect("icmp response");
        let TunWriteItem::Packet(response) = response else {
            panic!("expected one ICMP response packet");
        };
        assert_eq!(
            response[IPV4_PROTOCOL_OFFSET],
            IpProtocol::Icmpv4.wire_value()
        );
        assert_eq!(response[20], 3);
        assert_eq!(response[21], 4);
        assert_eq!(read_u16(&response, 26), 68);
        drop(tun_write_tx);
    }

    #[cfg(feature = "endpoint")]
    #[tokio::test]
    async fn l3_endpoint_dispatch_returns_icmpv6_packet_too_big_for_oversized_ipv6() {
        let packet = ipv6_udp_packet(&vec![0x42; 96]);
        let parsed = parse_ip_packet_view(&packet).expect("parse packet");
        let (endpoint_tx, mut endpoint_rx) = mpsc::channel(8);
        let endpoint: Arc<dyn EndpointTrait> = Arc::new(RecordingEndpoint {
            tx: endpoint_tx,
            batch_tx: None,
            mtu: Some(68),
        });
        let addresses = StackAddresses {
            v4: None,
            v6: Some(StackAddress {
                listener: Ipv6Addr::LOCALHOST,
                next: Ipv6Addr::LOCALHOST,
            }),
        };
        let dispatch = L3DispatchTable::from_endpoints(&[endpoint], &addresses);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));
        let (tun_write_tx, mut tun_write_rx) = mpsc::channel::<TunWriteItem>(4);

        assert!(dispatch_endpoint_l3_packet(
            &dispatch,
            "wg-out",
            packet,
            &parsed,
            &metrics,
            Some(&tun_write_tx),
        ));

        assert!(endpoint_rx.try_recv().is_err());
        let response = timeout(Duration::from_millis(50), tun_write_rx.recv())
            .await
            .expect("icmp response")
            .expect("icmp response");
        let TunWriteItem::Packet(response) = response else {
            panic!("expected one ICMP response packet");
        };
        assert_eq!(
            response[IPV6_PROTOCOL_OFFSET],
            IpProtocol::Icmpv6.wire_value()
        );
        assert_eq!(response[IPV6_HEADER_LEN], 2);
        assert_eq!(read_u32(&response, IPV6_HEADER_LEN + 4), 68);
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn endpoint_fast_path_decision_uses_router_policy() {
        let router = CountingRouter::rejecting();
        let dns_router = StubDnsRouter;
        let packet = udp_packet(5353, 53, b"hello");
        let parsed = parse_ip_packet_view(&packet).expect("parse packet");

        let decision =
            endpoint_fast_path_decision(router.as_ref(), &dns_router, "tun-in", &packet, &parsed)
                .expect("route");

        assert!(matches!(decision, RouteDecision::Reject { .. }));
        assert_eq!(router.match_calls.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn endpoint_fast_path_routes_empty_tcp_when_sniff_wait_is_needed() {
        let router = TcpSniffWaitRouter {
            match_calls: AtomicUsize::new(0),
        };
        let dns_router = StubDnsRouter;
        let packet = tcp_packet(50000, 443, 0x02, b"");
        let parsed = parse_ip_packet_view(&packet).expect("parse tcp packet");

        let decision =
            endpoint_fast_path_decision(&router, &dns_router, "tun-in", &packet, &parsed)
                .expect("route");

        assert!(matches!(
            decision,
            RouteDecision::Route {
                target: RouteTarget::Endpoint(_)
            }
        ));
        assert_eq!(
            router.match_calls.load(Ordering::Relaxed),
            1,
            "empty SYN cannot fall through to the system TCP listener for endpoint routing"
        );
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn endpoint_fast_path_uses_reverse_dns_for_tcp_split_routes() {
        let router = DomainSplitRouter {
            match_calls: AtomicUsize::new(0),
        };
        let dns_router = ReverseMappingDnsRouter;
        let packet = tcp_packet(50000, 443, 0x02, b"");
        let parsed = parse_ip_packet_view(&packet).expect("parse tcp packet");

        let decision =
            endpoint_fast_path_decision(&router, &dns_router, "tun-in", &packet, &parsed)
                .expect("route");

        assert!(matches!(
            decision,
            RouteDecision::Route {
                target: RouteTarget::Outbound(ref outbound)
            } if outbound == "direct"
        ));
        assert_eq!(router.match_calls.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn system_tcp_route_uses_reverse_dns_for_split_routes() {
        let router = DomainSplitRouter {
            match_calls: AtomicUsize::new(0),
        };
        let dns_router = ReverseMappingDnsRouter;
        let mut metadata = RouteMetadata {
            inbound: "tun".to_owned(),
            network: Network::Tcp,
            source: Some(SocksAddr::ip(
                IpAddr::V4(Ipv4Addr::new(172, 19, 0, 1)),
                50000,
            )),
            destination: Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 443)),
            ..Default::default()
        };

        let decision =
            route_system_tcp_metadata(&router, &dns_router, &mut metadata).expect("route");

        assert!(matches!(
            decision,
            RouteDecision::Route {
                target: RouteTarget::Outbound(ref outbound)
            } if outbound == "direct"
        ));
        assert_eq!(metadata.domain.as_deref(), Some("ifconfig.so"));
        assert_eq!(router.match_calls.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "endpoint")]
    #[test]
    fn reverse_dns_mapping_keeps_tcp_dial_destination_as_ip() {
        let dns_router = ReverseMappingDnsRouter;
        let mut metadata = RouteMetadata {
            inbound: "tun".to_owned(),
            network: Network::Tcp,
            destination: Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 443)),
            ..Default::default()
        };

        apply_reverse_dns_mapping(&mut metadata, Some(&dns_router));
        let destination = route_destination_without_dns(&metadata).expect("destination");

        assert_eq!(metadata.domain.as_deref(), Some("ifconfig.so"));
        assert_eq!(destination.host, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
        assert_eq!(destination.port, 443);
        assert_eq!(
            destination.domain, None,
            "reverse DNS is for routing; direct TCP dial must keep the concrete IP",
        );
    }

    #[cfg(feature = "endpoint")]
    #[tokio::test]
    async fn tcp_endpoint_flow_pin_bypasses_later_sniff_route_changes() {
        let router = Arc::new(EndpointThenRejectRouter {
            match_calls: AtomicUsize::new(0),
        });
        let (endpoint_tx, mut endpoint_rx) = mpsc::channel(4);
        let endpoint: Arc<dyn EndpointTrait> = Arc::new(RecordingEndpoint {
            tx: endpoint_tx,
            batch_tx: None,
            mtu: None,
        });
        let addresses = StackAddresses {
            v4: Some(StackAddress {
                listener: Ipv4Addr::new(172, 19, 0, 1),
                next: Ipv4Addr::new(172, 19, 0, 2),
            }),
            v6: None,
        };
        let dispatch = Some(Arc::new(L3DispatchTable::from_endpoints(
            &[endpoint],
            &addresses,
        )));
        let device = ScriptedBatchTunDevice::new(vec![vec![
            tcp_packet(50000, 443, 0x02, b""),
            tcp_packet(50000, 443, 0x18, b"later-sniffed-payload"),
        ]]);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));

        let task = crate::spawn::spawn(packet_loop(
            test_logger("tun"),
            router.clone(),
            Arc::new(StubDnsRouter),
            Arc::new(StubOutboundManager),
            "tun".to_owned(),
            Arc::clone(&device),
            Arc::new(StdMutex::new(SystemTcpNat::new_with_timeout(
                DEFAULT_SYSTEM_UDP_TIMEOUT,
            ))),
            Arc::new(StdMutex::new(HashMap::new())),
            SystemStackRoutes::default(),
            dispatch,
            DEFAULT_SYSTEM_UDP_TIMEOUT,
            metrics,
        ));

        let first = timeout(Duration::from_millis(50), endpoint_rx.recv())
            .await
            .expect("first endpoint packet")
            .expect("first endpoint packet");
        let second = timeout(Duration::from_millis(50), endpoint_rx.recv())
            .await
            .expect("second endpoint packet")
            .expect("second endpoint packet");

        assert_eq!(
            parse_ip_packet_view(&first).expect("first").source.host,
            IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2))
        );
        assert_eq!(
            parse_ip_packet_view(&second).expect("second").source.host,
            IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2))
        );
        assert_eq!(
            router.match_calls.load(Ordering::Relaxed),
            1,
            "second packet in the same TCP flow must use the endpoint pin"
        );
        task.abort();
    }

    #[cfg(feature = "endpoint")]
    #[tokio::test]
    async fn existing_udp_flow_bypasses_endpoint_fast_path() {
        let router = Arc::new(CountingRouter {
            should_sniff_calls: AtomicUsize::new(0),
            prepare_calls: AtomicUsize::new(0),
            match_calls: AtomicUsize::new(0),
            decision: RouteDecision::Route {
                target: RouteTarget::Endpoint("wg-out".to_owned()),
            },
        });
        let packet = udp_packet(5353, 443, b"quic-late");
        let parsed = parse_ip_packet_view(&packet).expect("parse UDP packet");
        let key = UdpFlowKey::from_parsed(&parsed);
        let (flow_tx, mut flow_rx) = mpsc::channel(4);
        let mut flows = HashMap::new();
        flows.insert(
            key,
            UdpFlowState {
                sender: flow_tx,
                last_active: Instant::now(),
                outbound: "direct".to_owned(),
            },
        );
        let udp_flows = Arc::new(StdMutex::new(flows));
        let (endpoint_tx, mut endpoint_rx) = mpsc::channel(4);
        let endpoint: Arc<dyn EndpointTrait> = Arc::new(RecordingEndpoint {
            tx: endpoint_tx,
            batch_tx: None,
            mtu: None,
        });
        let addresses = StackAddresses {
            v4: Some(StackAddress {
                listener: Ipv4Addr::new(172, 19, 0, 1),
                next: Ipv4Addr::new(172, 19, 0, 2),
            }),
            v6: None,
        };
        let dispatch = Some(Arc::new(L3DispatchTable::from_endpoints(
            &[endpoint],
            &addresses,
        )));
        let device = ScriptedBatchTunDevice::new(vec![vec![packet]]);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));

        let task = crate::spawn::spawn(packet_loop(
            test_logger("tun"),
            router.clone(),
            Arc::new(StubDnsRouter),
            Arc::new(StubOutboundManager),
            "tun".to_owned(),
            Arc::clone(&device),
            Arc::new(StdMutex::new(SystemTcpNat::new_with_timeout(
                DEFAULT_SYSTEM_UDP_TIMEOUT,
            ))),
            udp_flows,
            SystemStackRoutes::default(),
            dispatch,
            DEFAULT_SYSTEM_UDP_TIMEOUT,
            metrics,
        ));

        assert_eq!(
            timeout(Duration::from_millis(50), flow_rx.recv())
                .await
                .expect("system flow payload")
                .expect("system flow payload"),
            Bytes::from_static(b"quic-late")
        );
        if let Ok(Some(packet)) = timeout(Duration::from_millis(20), endpoint_rx.recv()).await {
            panic!(
                "existing system UDP flow must not be diverted into endpoint dispatch: {} bytes",
                packet.len()
            );
        }
        assert_eq!(router.should_sniff_calls.load(Ordering::Relaxed), 0);
        assert_eq!(router.match_calls.load(Ordering::Relaxed), 0);
        task.abort();
    }

    fn dns_hijack_job(name: &str) -> DnsHijackJob {
        let packet = dns_query_packet(name);
        let parsed = parse_ip_packet_view(&packet).expect("parse DNS packet");
        let message = dns_message_from_packet(&packet);
        DnsHijackJob {
            packet,
            destination: parsed.destination,
            message,
            options: DnsQueryOptions::default(),
        }
    }

    #[tokio::test]
    async fn packet_loop_does_not_wait_for_dns_hijack_before_reading_next_batch() {
        let options = config::parse_config(
            r#"
[tun]
address = ["172.19.0.1/30"]
sniff = true
hijack_dns = true

[dns]
server = "hosts"

[hysteria2]
server = "example.com"
password = "secret"
sni = "example.com"

[route]
final = "direct"
"#,
        )
        .expect("parse config");
        let outbound = Arc::new(
            OutboundManager::from_options(
                test_logger("outbound"),
                options.route.final_.clone(),
                &options.outbounds,
            )
            .expect("outbound manager"),
        );
        let router = Arc::new(
            Router::from_options(test_logger("router"), options.route, Arc::clone(&outbound))
                .expect("router"),
        );
        let dns_transport = Arc::new(DnsTransportManager::new(
            test_logger("dns-transport"),
            "slow",
        ));
        dns_transport.insert(dns_transport_component(
            "slow",
            "mock",
            Arc::new(DelayedDnsTransport),
        ));
        let dns_router = Arc::new(DnsRouter::new_with_manager(
            test_logger("dns-router"),
            dns_transport,
            config::DomainStrategy::AsIs,
        ));
        let device = ScriptedBatchTunDevice::new(vec![vec![dns_query_packet("example.com")]]);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));

        let task = crate::spawn::spawn(packet_loop(
            test_logger("tun"),
            router,
            dns_router,
            outbound,
            "tun".to_owned(),
            Arc::clone(&device),
            Arc::new(StdMutex::new(SystemTcpNat::new_with_timeout(
                DEFAULT_SYSTEM_UDP_TIMEOUT,
            ))),
            Arc::new(StdMutex::new(HashMap::new())),
            SystemStackRoutes::default(),
            #[cfg(feature = "endpoint")]
            None,
            DEFAULT_SYSTEM_UDP_TIMEOUT,
            metrics,
        ));

        timeout(
            Duration::from_millis(50),
            device.wait_for_read_after_batches(),
        )
        .await
        .expect("packet loop should keep reading while DNS hijack is still pending");
        task.abort();
    }

    #[tokio::test]
    async fn cached_dns_hijack_uses_fastpath_when_slowpath_queue_is_full() {
        let options = config::parse_config(
            r#"
[tun]
address = ["172.19.0.1/30"]
sniff = true
hijack_dns = true

[dns]
server = "hosts"

[hysteria2]
server = "example.com"
password = "secret"
sni = "example.com"

[route]
final = "direct"
"#,
        )
        .expect("parse config");
        let outbound = Arc::new(
            OutboundManager::from_options(
                test_logger("outbound"),
                options.route.final_.clone(),
                &options.outbounds,
            )
            .expect("outbound manager"),
        );
        let router = Arc::new(
            Router::from_options(test_logger("router"), options.route, Arc::clone(&outbound))
                .expect("router"),
        );
        let dns_transport = Arc::new(DnsTransportManager::new(
            test_logger("dns-transport"),
            "counting",
        ));
        let queries = Arc::new(AtomicUsize::new(0));
        dns_transport.insert(dns_transport_component(
            "counting",
            "mock",
            Arc::new(CountingDnsTransport {
                queries: Arc::clone(&queries),
            }),
        ));
        let dns_router = Arc::new(DnsRouter::new_with_manager(
            test_logger("dns-router"),
            dns_transport,
            config::DomainStrategy::AsIs,
        ));

        let request = dns_query_packet("example.com");
        dns_router
            .exchange(
                dns_message_from_packet(&request),
                DnsQueryOptions::default(),
            )
            .await
            .expect("warm DNS cache");
        assert_eq!(queries.load(Ordering::Relaxed), 1);

        let (dns_hijack_tx, _dns_hijack_rx) = mpsc::channel(1);
        let queued = dns_query_packet("queued.example.com");
        let queued_parsed = parse_ip_packet_view(&queued).expect("queued DNS packet");
        let queued_message = dns_message_from_packet(&queued);
        dns_hijack_tx
            .try_send(DnsHijackJob {
                packet: queued,
                destination: queued_parsed.destination,
                message: queued_message,
                options: DnsQueryOptions::default(),
            })
            .expect("fill DNS slowpath queue");

        let (control_write_tx, mut control_write_rx) = mpsc::channel(1);
        let parsed = parse_ip_packet_view(&request).expect("DNS packet");
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));
        handle_system_udp_packet(
            router,
            Arc::clone(&dns_router),
            outbound,
            "tun".to_owned(),
            Arc::new(StdMutex::new(HashMap::new())),
            &dns_hijack_tx,
            &control_write_tx,
            DEFAULT_SYSTEM_UDP_TIMEOUT,
            request,
            parsed,
            metrics,
        )
        .expect("handle UDP DNS packet");

        let response_packet = timeout(Duration::from_millis(50), control_write_rx.recv())
            .await
            .expect("cached DNS response should bypass slowpath queue")
            .expect("cached DNS response packet");
        let TunWriteItem::Packet(response_packet) = response_packet else {
            panic!("expected cached DNS response packet");
        };
        let response = dns_message_from_packet(&response_packet);
        assert_eq!(response.metadata.id, 0x1234);
        assert_eq!(response.metadata.response_code, ResponseCode::NoError);
        assert_eq!(queries.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn tun_packet_writer_batches_ready_packets() {
        let device = BatchRecordingTunDevice::new();
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));
        let (tx, rx) = mpsc::channel(8);
        tx.try_send(TunWriteItem::Packet(vec![1]))
            .expect("queue packet 1");
        tx.try_send(TunWriteItem::Packet(vec![2]))
            .expect("queue packet 2");
        tx.try_send(TunWriteItem::Packet(vec![3]))
            .expect("queue packet 3");
        drop(tx);

        let task = spawn_tun_packet_writer(Arc::clone(&device), metrics, rx);
        timeout(Duration::from_millis(50), task)
            .await
            .expect("writer exits after sender closes")
            .expect("writer task");

        assert_eq!(*device.batch_sizes.lock().await, vec![3]);
        assert_eq!(
            *device.packets.lock().await,
            vec![vec![1], vec![2], vec![3]]
        );
    }

    #[tokio::test]
    async fn tun_packet_writer_preserves_batch_write_items() {
        let device = BatchRecordingTunDevice::new();
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));
        let (tx, rx) = mpsc::channel(8);
        tx.try_send(TunWriteItem::Batch(vec![vec![1], vec![2], vec![3]]))
            .expect("queue packet batch");
        drop(tx);

        let task = spawn_tun_packet_writer(Arc::clone(&device), metrics, rx);
        timeout(Duration::from_millis(50), task)
            .await
            .expect("writer exits after sender closes")
            .expect("writer task");

        assert_eq!(*device.batch_sizes.lock().await, vec![3]);
        assert_eq!(
            *device.packets.lock().await,
            vec![vec![1], vec![2], vec![3]]
        );
    }

    #[tokio::test]
    async fn dns_hijack_dispatcher_waits_for_worker_capacity_without_dropping_global_job() {
        let (global_tx, global_rx) = mpsc::channel(1);
        let (worker_tx, mut worker_rx) = mpsc::channel(SYSTEM_DNS_HIJACK_WORKER_QUEUE_CAPACITY);
        let queued = dns_hijack_job("queued.example.com");
        let blocked = dns_hijack_job("blocked.example.com");
        worker_tx.try_send(queued).expect("fill worker queue");
        global_tx.try_send(blocked).expect("queue global DNS job");
        drop(global_tx);

        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));
        let dispatcher = spawn_dns_hijack_dispatcher(global_rx, vec![worker_tx], metrics);
        let first = worker_rx.recv().await.expect("first worker job");
        assert_eq!(first.packet, dns_query_packet("queued.example.com"));

        let second = timeout(Duration::from_millis(50), worker_rx.recv())
            .await
            .expect("dispatcher should wait for worker capacity")
            .expect("second worker job");
        assert_eq!(second.packet, dns_query_packet("blocked.example.com"));
        timeout(Duration::from_millis(50), dispatcher)
            .await
            .expect("dispatcher exits after global queue closes")
            .expect("dispatcher task");
    }

    #[tokio::test]
    async fn dns_hijack_workers_process_slow_jobs_concurrently() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let dns_router = BlockingDnsRouter::new(started_tx);
        let (dns_hijack_tx, dns_hijack_rx) = mpsc::channel(SYSTEM_DNS_HIJACK_QUEUE_CAPACITY);
        let (tun_write_tx, _tun_write_rx) = mpsc::channel(16);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));

        spawn_dns_hijack_workers(
            Arc::clone(&dns_router),
            tun_write_tx,
            metrics,
            dns_hijack_rx,
        );
        for name in [
            "one.example.com",
            "two.example.com",
            "three.example.com",
            "four.example.com",
        ] {
            dns_hijack_tx
                .try_send(dns_hijack_job(name))
                .expect("queue DNS hijack job");
        }

        timeout(Duration::from_millis(50), started_rx.recv())
            .await
            .expect("first DNS job starts")
            .expect("first start event");
        timeout(Duration::from_millis(50), started_rx.recv())
            .await
            .expect("second DNS job starts")
            .expect("second start event");

        assert!(
            dns_router.max_in_flight.load(Ordering::Relaxed) >= 2,
            "DNS workers should process slow exchanges concurrently"
        );
        dns_router.release.notify_waiters();
    }

    #[tokio::test]
    async fn existing_udp_flow_fastpath_bypasses_router_and_enqueues_payload() {
        let packet = udp_packet(5353, 443, b"quic");
        let parsed = parse_ip_packet_view(&packet).expect("parse UDP packet");
        let key = UdpFlowKey {
            source: (parsed.source.host, parsed.source.port),
            destination: (parsed.destination.host, parsed.destination.port),
        };
        let (tx, mut rx) = mpsc::channel(4);
        let mut flows = HashMap::new();
        flows.insert(
            key,
            UdpFlowState {
                sender: tx,
                last_active: Instant::now(),
                outbound: "hysteria2".to_owned(),
            },
        );
        let udp_flows = Arc::new(StdMutex::new(flows));
        let router = CountingRouter::rejecting();
        let (dns_hijack_tx, _dns_hijack_rx) = mpsc::channel(1);
        let (control_write_tx, _control_write_rx) = mpsc::channel(1);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));

        handle_system_udp_packet(
            Arc::clone(&router),
            Arc::new(StubDnsRouter),
            Arc::new(StubOutboundManager),
            "tun".to_owned(),
            udp_flows,
            &dns_hijack_tx,
            &control_write_tx,
            DEFAULT_SYSTEM_UDP_TIMEOUT,
            packet,
            parsed,
            metrics,
        )
        .expect("existing flow packet");

        assert_eq!(router.should_sniff_calls.load(Ordering::Relaxed), 0);
        assert_eq!(router.prepare_calls.load(Ordering::Relaxed), 0);
        assert_eq!(router.match_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            timeout(Duration::from_millis(50), rx.recv())
                .await
                .expect("flow payload")
                .expect("flow payload"),
            b"quic".to_vec()
        );
    }

    #[tokio::test]
    async fn existing_udp_flow_full_queue_drops_without_fallback_route() {
        let packet = udp_packet(5353, 443, b"late");
        let parsed = parse_ip_packet_view(&packet).expect("parse UDP packet");
        let key = UdpFlowKey {
            source: (parsed.source.host, parsed.source.port),
            destination: (parsed.destination.host, parsed.destination.port),
        };
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(Bytes::from_static(b"queued"))
            .expect("fill flow queue");
        let mut flows = HashMap::new();
        flows.insert(
            key,
            UdpFlowState {
                sender: tx,
                last_active: Instant::now(),
                outbound: "hysteria2".to_owned(),
            },
        );
        let udp_flows = Arc::new(StdMutex::new(flows));
        let router = CountingRouter::rejecting();
        let (dns_hijack_tx, _dns_hijack_rx) = mpsc::channel(1);
        let (control_write_tx, _control_write_rx) = mpsc::channel(1);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));

        handle_system_udp_packet(
            Arc::clone(&router),
            Arc::new(StubDnsRouter),
            Arc::new(StubOutboundManager),
            "tun".to_owned(),
            udp_flows,
            &dns_hijack_tx,
            &control_write_tx,
            DEFAULT_SYSTEM_UDP_TIMEOUT,
            packet,
            parsed,
            metrics,
        )
        .expect("full flow queue packet is handled as drop");

        assert_eq!(router.match_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            rx.try_recv().expect("original queued payload").as_ref(),
            b"queued"
        );
        assert!(
            rx.try_recv().is_err(),
            "full existing flow must not route or enqueue the dropped payload"
        );
    }

    #[tokio::test]
    async fn closed_udp_flow_is_removed_before_slowpath_route() {
        let packet = udp_packet(5353, 443, b"retry");
        let parsed = parse_ip_packet_view(&packet).expect("parse UDP packet");
        let key = UdpFlowKey {
            source: (parsed.source.host, parsed.source.port),
            destination: (parsed.destination.host, parsed.destination.port),
        };
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let mut flows = HashMap::new();
        flows.insert(
            key.clone(),
            UdpFlowState {
                sender: tx,
                last_active: Instant::now(),
                outbound: "hysteria2".to_owned(),
            },
        );
        let udp_flows = Arc::new(StdMutex::new(flows));
        let router = CountingRouter::rejecting();
        let (dns_hijack_tx, _dns_hijack_rx) = mpsc::channel(1);
        let (control_write_tx, _control_write_rx) = mpsc::channel(1);
        let metrics = TunMetrics::new(MetricsRegistry::new().scope("inbound", "tun", "test"));

        handle_system_udp_packet(
            Arc::clone(&router),
            Arc::new(StubDnsRouter),
            Arc::new(StubOutboundManager),
            "tun".to_owned(),
            Arc::clone(&udp_flows),
            &dns_hijack_tx,
            &control_write_tx,
            DEFAULT_SYSTEM_UDP_TIMEOUT,
            packet,
            parsed,
            metrics,
        )
        .expect("closed flow falls through to slow path");

        assert_eq!(router.match_calls.load(Ordering::Relaxed), 1);
        assert!(
            !udp_flows.lock().expect("udp flows").contains_key(&key),
            "closed existing flow must be removed before slow path"
        );
    }

    struct ResetOnWriteStream;

    impl AsyncRead for ResetOnWriteStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for ResetOnWriteStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::ConnectionReset)))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn system_tcp_bridge_closes_inbound_on_copy_error_without_linger() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let client = crate::spawn::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr)
                .await
                .expect("connect client");
            client
                .write_all(b"GET / HTTP/1.1\r\n\r\n")
                .await
                .expect("write client payload");
            client
        });
        let (mut inbound, _) = listener.accept().await.expect("accept inbound");
        let mut outbound = ResetOnWriteStream;

        let err = bridge_system_tcp_streams(&mut inbound, &mut outbound)
            .await
            .expect_err("copy should surface outbound reset");

        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        assert_ne!(
            inbound.linger().expect("read inbound linger"),
            Some(Duration::ZERO),
            "system TUN close path should not rely on SO_LINGER=0; iOS rejects it with EINVAL"
        );
        let _ = client.await.expect("client task");
    }
}
