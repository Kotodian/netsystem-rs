use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener as StdTcpListener};
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant as StdInstant;

use async_trait::async_trait;
use hammer_adapter::{
    DnsQueryOptions, Network, OutboundManager as _, RouteDecision, RouteMetadata, SocksAddr,
};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use hickory_proto::op::Message;
use ipnet::IpNet;
use smoltcp::wire::IpProtocol;
use tokio::io::{AsyncReadExt, copy_bidirectional};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant, timeout};

pub use crate::TunInbound;
use crate::dns::MessageExt;
use crate::{DnsRouter, OutboundManager, Router};

const TUN_READ_HEADROOM: usize = 128;
const MAX_TUN_PACKET_SIZE: usize = 65_535;
const SYSTEM_UDP_FLOW_CAPACITY: usize = 1024;
const SYSTEM_UDP_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_SYSTEM_UDP_TIMEOUT: Duration = Duration::from_secs(300);

#[async_trait]
pub trait TunDevice: Send + Sync + 'static {
    async fn recv(&self) -> Result<Vec<u8>, HammerError>;
    async fn send(&self, packet: Vec<u8>) -> Result<(), HammerError>;
    fn close(&self);
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

    pub async fn inject(&self, packet: Vec<u8>) -> Result<(), HammerError> {
        self.input_tx
            .send(packet)
            .await
            .map_err(|_| HammerError::internal("memory tun input closed"))
    }

    pub async fn take_output(&self) -> Option<Vec<u8>> {
        self.output_rx.lock().await.recv().await
    }

    pub async fn recv(&self) -> Result<Vec<u8>, HammerError> {
        <Self as TunDevice>::recv(self).await
    }

    pub async fn send(&self, packet: Vec<u8>) -> Result<(), HammerError> {
        <Self as TunDevice>::send(self, packet).await
    }

    pub fn close(&self) {
        <Self as TunDevice>::close(self);
    }
}

#[async_trait]
impl TunDevice for MemoryTunDevice {
    async fn recv(&self) -> Result<Vec<u8>, HammerError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(HammerError::internal("memory tun closed"));
        }
        self.input_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| HammerError::internal("memory tun input closed"))
    }

    async fn send(&self, packet: Vec<u8>) -> Result<(), HammerError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(HammerError::internal("memory tun closed"));
        }
        self.output_tx
            .send(packet)
            .await
            .map_err(|_| HammerError::internal("memory tun output closed"))
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
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
    pub unsafe fn from_fd(fd: RawFd, mtu: usize) -> Result<Arc<Self>, HammerError> {
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
    async fn recv(&self) -> Result<Vec<u8>, HammerError> {
        if self.closed.load(Ordering::SeqCst) {
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

    async fn send(&self, packet: Vec<u8>) -> Result<(), HammerError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(HammerError::internal("TUN device closed"));
        }
        self.device
            .send(&packet)
            .await
            .map_err(|err| HammerError::internal(format!("write TUN packet: {err}")))?;
        Ok(())
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

fn tun_read_buffer_len(mtu: usize) -> Result<usize, HammerError> {
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
pub struct TunPacket {
    pub metadata: RouteMetadata,
    pub payload: Vec<u8>,
}

impl TunPacket {
    pub fn for_test(network: Network, destination_port: u16, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            metadata: RouteMetadata {
                network,
                destination: Some(SocksAddr {
                    host: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
                    port: destination_port,
                }),
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

    fn lookup_or_insert(&mut self, source: SocksAddr, destination: SocksAddr) -> u16 {
        self.cleanup_expired();
        let key = (source.host, source.port, destination.host, destination.port);
        if let Some(port) = self.by_flow.get(&key) {
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
            },
        );
        port
    }

    fn cleanup_expired(&mut self) {
        let now = StdInstant::now();
        let timeout = self.timeout;
        self.by_port
            .retain(|_, session| now.duration_since(session.last_active) <= timeout);
        self.by_flow
            .retain(|_, port| self.by_port.contains_key(port));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UdpFlowKey {
    outbound: String,
    source: (IpAddr, u16),
    destination: (IpAddr, u16),
}

struct UdpFlowState {
    sender: mpsc::Sender<Vec<u8>>,
    last_active: Instant,
}

type UdpFlowMap = HashMap<UdpFlowKey, UdpFlowState>;

pub struct SystemTunStack {
    logger: Logger,
    router: Arc<Router>,
    dns_router: Arc<DnsRouter>,
    outbound: Arc<OutboundManager>,
    inbound_tag: String,
    options: hammer_core::config::TunInboundOptions,
    device: Arc<dyn TunDevice>,
    tcp_nat: Arc<StdMutex<SystemTcpNat>>,
    udp_flows: Arc<Mutex<UdpFlowMap>>,
    tun_interface_index: Option<u32>,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
    started: AtomicBool,
}

impl SystemTunStack {
    pub fn new(
        logger: Logger,
        router: Arc<Router>,
        dns_router: Arc<DnsRouter>,
        outbound: Arc<OutboundManager>,
        inbound_tag: String,
        options: hammer_core::config::TunInboundOptions,
        device: Arc<dyn TunDevice>,
    ) -> Self {
        Self::new_with_interface_index(
            logger,
            router,
            dns_router,
            outbound,
            inbound_tag,
            options,
            device,
            None,
        )
    }

    pub fn new_with_interface_index(
        logger: Logger,
        router: Arc<Router>,
        dns_router: Arc<DnsRouter>,
        outbound: Arc<OutboundManager>,
        inbound_tag: String,
        options: hammer_core::config::TunInboundOptions,
        device: Arc<dyn TunDevice>,
        tun_interface_index: Option<u32>,
    ) -> Self {
        let udp_timeout = options.udp_timeout.unwrap_or(DEFAULT_SYSTEM_UDP_TIMEOUT);
        Self {
            logger,
            router,
            dns_router,
            outbound,
            inbound_tag,
            options,
            device,
            tcp_nat: Arc::new(StdMutex::new(SystemTcpNat::new_with_timeout(udp_timeout))),
            udp_flows: Arc::new(Mutex::new(HashMap::new())),
            tun_interface_index,
            tasks: StdMutex::new(Vec::new()),
            started: AtomicBool::new(false),
        }
    }

    pub fn start(&self) -> Result<(), HammerError> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let addresses = StackAddresses::from_options(&self.options)?;
        let mut handles = Vec::new();
        let mut routes = SystemStackRoutes::default();
        if let Some(v4) = addresses.v4 {
            let listener =
                bind_system_listener(IpAddr::V4(v4.listener), self.tun_interface_index).map_err(
                    |err| HammerError::internal(format!("bind IPv4 system TCP listener: {err}")),
                )?;
            let port = listener
                .local_addr()
                .map_err(|err| HammerError::internal(format!("read IPv4 listener addr: {err}")))?
                .port();
            self.logger
                .info(format!("system stack TCP listener {}:{port}", v4.listener));
            handles.push(tokio::spawn(accept_tcp_loop(
                self.logger.clone(),
                Arc::clone(&self.router),
                Arc::clone(&self.outbound),
                Arc::clone(&self.tcp_nat),
                self.inbound_tag.clone(),
                listener,
            )));
            routes.v4 = Some(SystemStackRoute {
                listener_addr: IpAddr::V4(v4.listener),
                nat_addr: IpAddr::V4(v4.next),
                listener_port: port,
            });
        }
        if let Some(v6) = addresses.v6 {
            let listener =
                bind_system_listener(IpAddr::V6(v6.listener), self.tun_interface_index).map_err(
                    |err| HammerError::internal(format!("bind IPv6 system TCP listener: {err}")),
                )?;
            let port = listener
                .local_addr()
                .map_err(|err| HammerError::internal(format!("read IPv6 listener addr: {err}")))?
                .port();
            self.logger.info(format!(
                "system stack TCP listener [{}]:{port}",
                v6.listener
            ));
            handles.push(tokio::spawn(accept_tcp_loop(
                self.logger.clone(),
                Arc::clone(&self.router),
                Arc::clone(&self.outbound),
                Arc::clone(&self.tcp_nat),
                self.inbound_tag.clone(),
                listener,
            )));
            routes.v6 = Some(SystemStackRoute {
                listener_addr: IpAddr::V6(v6.listener),
                nat_addr: IpAddr::V6(v6.next),
                listener_port: port,
            });
        }
        handles.push(tokio::spawn(packet_loop(
            self.logger.clone(),
            Arc::clone(&self.router),
            Arc::clone(&self.dns_router),
            Arc::clone(&self.outbound),
            self.inbound_tag.clone(),
            Arc::clone(&self.device),
            Arc::clone(&self.tcp_nat),
            Arc::clone(&self.udp_flows),
            routes,
            self.options
                .udp_timeout
                .unwrap_or(DEFAULT_SYSTEM_UDP_TIMEOUT),
        )));
        self.tasks
            .lock()
            .expect("SystemTunStack tasks poisoned")
            .extend(handles);
        self.logger.info("system stack started");
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
        match packet.first().map(|byte| byte >> 4) {
            Some(4) => self.v4.as_ref(),
            Some(6) => self.v6.as_ref(),
            _ => None,
        }
    }
}

fn bind_system_listener(
    addr: IpAddr,
    interface_index: Option<u32>,
) -> Result<TcpListener, HammerError> {
    let listener = StdTcpListener::bind(SocketAddr::new(addr, 0))
        .map_err(|err| HammerError::internal(format!("bind {addr}: {err}")))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("set {addr} listener nonblocking: {err}")))?;
    if let Some(index) = interface_index {
        bind_listener_to_tun_interface(&listener, addr, index)?;
    }
    TcpListener::from_std(listener)
        .map_err(|err| HammerError::internal(format!("register {addr} listener: {err}")))
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn bind_listener_to_tun_interface(
    listener: &StdTcpListener,
    addr: IpAddr,
    index: u32,
) -> Result<(), HammerError> {
    use std::os::fd::AsRawFd;
    let (level, name) = match addr {
        IpAddr::V4(_) => (libc::IPPROTO_IP, libc::IP_BOUND_IF),
        IpAddr::V6(_) => (libc::IPPROTO_IPV6, libc::IPV6_BOUND_IF),
    };
    let value = index as libc::c_int;
    let ret = unsafe {
        libc::setsockopt(
            listener.as_raw_fd(),
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
fn bind_listener_to_tun_interface(
    _listener: &StdTcpListener,
    _addr: IpAddr,
    _index: u32,
) -> Result<(), HammerError> {
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

struct StackAddresses {
    v4: Option<StackAddress<Ipv4Addr>>,
    v6: Option<StackAddress<Ipv6Addr>>,
}

struct StackAddress<T> {
    listener: T,
    next: T,
}

impl StackAddresses {
    fn from_options(options: &hammer_core::config::TunInboundOptions) -> Result<Self, HammerError> {
        let mut v4 = None;
        let mut v6 = None;
        for prefix in &options.address {
            let net: IpNet = prefix
                .0
                .parse()
                .map_err(|err| HammerError::internal(format!("parse TUN address: {err}")))?;
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
        payload: Vec<u8>,
    },
    RoutedResponse {
        metadata: RouteMetadata,
        payload: Vec<u8>,
    },
    Dropped {
        metadata: RouteMetadata,
        reason: String,
    },
}

pub fn parse_ip_packet(packet: &[u8]) -> Result<ParsedIpPacket, HammerError> {
    if packet.is_empty() {
        return Err(HammerError::internal("empty IP packet"));
    }
    match packet[0] >> 4 {
        4 => parse_ipv4_packet(packet),
        6 => parse_ipv6_packet(packet),
        version => Err(HammerError::internal(format!(
            "unsupported IP version: {version}"
        ))),
    }
}

pub fn process_system_tcp_packet(
    packet: &mut [u8],
    nat: &mut SystemTcpNat,
    listener_addr: IpAddr,
    nat_addr: IpAddr,
    listener_port: u16,
) -> Result<(), HammerError> {
    match packet.first().map(|byte| byte >> 4) {
        Some(4) => process_system_tcp_ipv4(packet, nat, listener_addr, nat_addr, listener_port),
        Some(6) => process_system_tcp_ipv6(packet, nat, listener_addr, nat_addr, listener_port),
        Some(version) => Err(HammerError::internal(format!(
            "unsupported IP version: {version}"
        ))),
        None => Err(HammerError::internal("empty IP packet")),
    }
}

pub fn udp_response_packet(
    request: &[u8],
    source: SocksAddr,
    payload: &[u8],
) -> Result<Vec<u8>, HammerError> {
    UdpResponseTemplate::from_request(request)?.build(source, payload)
}

pub fn udp_unreachable_packet(request: &[u8]) -> Result<Vec<u8>, HammerError> {
    match request.first().map(|byte| byte >> 4) {
        Some(4) => ipv4_udp_unreachable_packet(request),
        Some(6) => ipv6_udp_unreachable_packet(request),
        Some(version) => Err(HammerError::internal(format!(
            "unsupported IP version: {version}"
        ))),
        None => Err(HammerError::internal("empty IP packet")),
    }
}

pub fn tcp_reset_packet(request: &[u8]) -> Result<Vec<u8>, HammerError> {
    match request.first().map(|byte| byte >> 4) {
        Some(4) => ipv4_tcp_reset_packet(request),
        Some(6) => ipv6_tcp_reset_packet(request),
        Some(version) => Err(HammerError::internal(format!(
            "unsupported IP version: {version}"
        ))),
        None => Err(HammerError::internal("empty IP packet")),
    }
}

#[derive(Clone)]
struct UdpResponseTemplate {
    header: Vec<u8>,
    version: u8,
    udp_offset: usize,
}

impl UdpResponseTemplate {
    fn from_request(request: &[u8]) -> Result<Self, HammerError> {
        match request.first().map(|byte| byte >> 4) {
            Some(4) => Self::from_ipv4_request(request),
            Some(6) => Self::from_ipv6_request(request),
            Some(version) => Err(HammerError::internal(format!(
                "unsupported IP version: {version}"
            ))),
            None => Err(HammerError::internal("empty IP packet")),
        }
    }

    fn from_ipv4_request(request: &[u8]) -> Result<Self, HammerError> {
        if request.len() < 28 {
            return Err(HammerError::internal("short IPv4 UDP packet"));
        }
        let ihl = ((request[0] & 0x0f) as usize) * 4;
        if ihl < 20 || request.len() < ihl + 8 || request[9] != 17 {
            return Err(HammerError::internal("invalid IPv4 UDP packet"));
        }
        Ok(Self {
            header: request[..ihl + 8].to_vec(),
            version: 4,
            udp_offset: ihl,
        })
    }

    fn from_ipv6_request(request: &[u8]) -> Result<Self, HammerError> {
        if request.len() < 48 || request[6] != 17 {
            return Err(HammerError::internal("invalid IPv6 UDP packet"));
        }
        Ok(Self {
            header: request[..48].to_vec(),
            version: 6,
            udp_offset: 40,
        })
    }

    fn build(&self, source: SocksAddr, payload: &[u8]) -> Result<Vec<u8>, HammerError> {
        match self.version {
            4 => self.build_v4(source, payload),
            6 => self.build_v6(source, payload),
            version => Err(HammerError::internal(format!(
                "unsupported IP version: {version}"
            ))),
        }
    }

    fn build_v4(&self, source: SocksAddr, payload: &[u8]) -> Result<Vec<u8>, HammerError> {
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
        write_u16(&mut packet, self.udp_offset + 4, (8 + payload.len()) as u16);
        update_ipv4_udp_checksums(&mut packet, self.udp_offset)?;
        Ok(packet)
    }

    fn build_v6(&self, source: SocksAddr, payload: &[u8]) -> Result<Vec<u8>, HammerError> {
        let IpAddr::V6(source_ip) = source.host else {
            return Err(HammerError::internal("IPv6 UDP response needs IPv6 source"));
        };
        let payload_len = 8 + payload.len();
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
        write_u16(&mut packet, self.udp_offset + 4, payload_len as u16);
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
    if packet.metadata.protocol.is_empty() {
        sniff_dns(packet);
    }
    if packet.metadata.protocol.is_empty() {
        sniff_quic(packet);
    }
    if packet.metadata.protocol.is_empty() {
        sniff_stun(packet);
    }
}

pub struct SmoltcpTunStack {
    logger: Logger,
    router: Arc<Router>,
    dns_router: Option<Arc<DnsRouter>>,
    outbound: Option<Arc<OutboundManager>>,
    inbound_tag: String,
}

impl SmoltcpTunStack {
    pub fn new(logger: Logger, router: Arc<Router>, inbound_tag: String) -> Self {
        Self {
            logger,
            router,
            dns_router: None,
            outbound: None,
            inbound_tag,
        }
    }

    pub fn new_with_runtime(
        logger: Logger,
        router: Arc<Router>,
        dns_router: Arc<DnsRouter>,
        outbound: Arc<OutboundManager>,
        inbound_tag: String,
    ) -> Self {
        Self {
            logger,
            router,
            dns_router: Some(dns_router),
            outbound: Some(outbound),
            inbound_tag,
        }
    }

    pub fn handle_packet(&self, packet: &[u8]) -> Result<PacketFlow, HammerError> {
        let mut tun_packet = self.packet_context(packet)?;
        let decision = self.router.match_route(&mut tun_packet.metadata)?;
        self.logger.debug(format!(
            "handled TUN {:?} packet",
            tun_packet.metadata.network
        ));
        Ok(PacketFlow {
            metadata: tun_packet.metadata,
            decision,
        })
    }

    pub async fn dispatch_packet(&self, packet: &[u8]) -> Result<TunDispatch, HammerError> {
        let mut tun_packet = self.packet_context(packet)?;
        let decision = self.router.match_route(&mut tun_packet.metadata)?;
        match decision {
            RouteDecision::HijackDns => self.dispatch_dns(tun_packet).await,
            RouteDecision::Reject { method } => Ok(TunDispatch::Dropped {
                metadata: tun_packet.metadata,
                reason: format!("reject: {method}"),
            }),
            RouteDecision::Route { outbound } => self.dispatch_route(tun_packet, &outbound).await,
        }
    }

    fn packet_context(&self, packet: &[u8]) -> Result<TunPacket, HammerError> {
        let parsed = parse_ip_packet(packet)?;
        let mut tun_packet = TunPacket {
            metadata: RouteMetadata {
                inbound: self.inbound_tag.clone(),
                network: parsed.network,
                source: Some(parsed.source),
                destination: Some(parsed.destination),
                ..Default::default()
            },
            payload: parsed.payload,
        };
        match tun_packet.metadata.network {
            Network::Tcp => sniff_stream(&mut tun_packet),
            Network::Udp => sniff_packet(&mut tun_packet),
        }
        Ok(tun_packet)
    }

    async fn dispatch_dns(&self, tun_packet: TunPacket) -> Result<TunDispatch, HammerError> {
        let dns_router = self
            .dns_router
            .as_ref()
            .ok_or_else(|| HammerError::internal("TUN DNS router is not configured"))?;
        let message = <Message as MessageExt>::from_bytes(&tun_packet.payload)?;
        let response = dns_router
            .exchange(message, DnsQueryOptions::default())
            .await?;
        Ok(TunDispatch::DnsResponse {
            metadata: tun_packet.metadata,
            payload: MessageExt::to_bytes(&response)?,
        })
    }

    async fn dispatch_route(
        &self,
        tun_packet: TunPacket,
        outbound_tag: &str,
    ) -> Result<TunDispatch, HammerError> {
        let outbound_manager = self
            .outbound
            .as_ref()
            .ok_or_else(|| HammerError::internal("TUN outbound manager is not configured"))?;
        let outbound = outbound_manager
            .get(outbound_tag)
            .ok_or_else(|| HammerError::internal(format!("outbound not found: {outbound_tag}")))?;
        let destination = tun_packet
            .metadata
            .destination
            .clone()
            .ok_or_else(|| HammerError::internal("TUN packet missing destination"))?;
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
                    payload,
                })
            }
            Network::Udp => {
                let mut packet = outbound.listen_packet().await?;
                packet.send_to(destination, &tun_packet.payload).await?;
                let response = timeout(Duration::from_secs(2), packet.recv_from())
                    .await
                    .map_err(|_| HammerError::internal("TUN routed UDP response timed out"))??;
                Ok(TunDispatch::RoutedResponse {
                    metadata: tun_packet.metadata,
                    payload: response.payload,
                })
            }
        }
    }
}

fn parse_ipv4_packet(packet: &[u8]) -> Result<ParsedIpPacket, HammerError> {
    if packet.len() < 20 {
        return Err(HammerError::internal("short IPv4 packet"));
    }
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    if ihl < 20 || packet.len() < ihl {
        return Err(HammerError::internal("invalid IPv4 header length"));
    }
    let source = IpAddr::V4(Ipv4Addr::new(
        packet[12], packet[13], packet[14], packet[15],
    ));
    let destination = IpAddr::V4(Ipv4Addr::new(
        packet[16], packet[17], packet[18], packet[19],
    ));
    parse_transport(packet[9], source, destination, &packet[ihl..])
}

fn parse_ipv6_packet(packet: &[u8]) -> Result<ParsedIpPacket, HammerError> {
    if packet.len() < 40 {
        return Err(HammerError::internal("short IPv6 packet"));
    }
    let source = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[8..24]).unwrap(),
    ));
    let destination = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[24..40]).unwrap(),
    ));
    parse_transport(packet[6], source, destination, &packet[40..])
}

fn parse_transport(
    protocol: u8,
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
) -> Result<ParsedIpPacket, HammerError> {
    match protocol {
        6 => parse_tcp(source, destination, transport),
        17 => parse_udp(source, destination, transport),
        other => Err(HammerError::internal(format!(
            "unsupported transport protocol: {other}"
        ))),
    }
}

fn parse_tcp(
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
) -> Result<ParsedIpPacket, HammerError> {
    let _protocol = IpProtocol::Tcp;
    if transport.len() < 20 {
        return Err(HammerError::internal("short TCP segment"));
    }
    let source_port = u16::from_be_bytes([transport[0], transport[1]]);
    let destination_port = u16::from_be_bytes([transport[2], transport[3]]);
    let data_offset = ((transport[12] >> 4) as usize) * 4;
    if data_offset < 20 || transport.len() < data_offset {
        return Err(HammerError::internal("invalid TCP data offset"));
    }
    Ok(ParsedIpPacket {
        network: Network::Tcp,
        source: SocksAddr {
            host: source,
            port: source_port,
        },
        destination: SocksAddr {
            host: destination,
            port: destination_port,
        },
        payload: transport[data_offset..].to_vec(),
    })
}

fn parse_udp(
    source: IpAddr,
    destination: IpAddr,
    transport: &[u8],
) -> Result<ParsedIpPacket, HammerError> {
    let _protocol = IpProtocol::Udp;
    if transport.len() < 8 {
        return Err(HammerError::internal("short UDP datagram"));
    }
    let source_port = u16::from_be_bytes([transport[0], transport[1]]);
    let destination_port = u16::from_be_bytes([transport[2], transport[3]]);
    let length = u16::from_be_bytes([transport[4], transport[5]]) as usize;
    if length < 8 || transport.len() < length {
        return Err(HammerError::internal("invalid UDP length"));
    }
    Ok(ParsedIpPacket {
        network: Network::Udp,
        source: SocksAddr {
            host: source,
            port: source_port,
        },
        destination: SocksAddr {
            host: destination,
            port: destination_port,
        },
        payload: transport[8..length].to_vec(),
    })
}

fn process_system_tcp_ipv4(
    packet: &mut [u8],
    nat: &mut SystemTcpNat,
    listener_addr: IpAddr,
    nat_addr: IpAddr,
    listener_port: u16,
) -> Result<(), HammerError> {
    if packet.len() < 40 {
        return Err(HammerError::internal("short IPv4 TCP packet"));
    }
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    if ihl < 20 || packet.len() < ihl + 20 || packet[9] != 6 {
        return Err(HammerError::internal("invalid IPv4 TCP packet"));
    }
    let source_addr = IpAddr::V4(Ipv4Addr::new(
        packet[12], packet[13], packet[14], packet[15],
    ));
    let destination_addr = IpAddr::V4(Ipv4Addr::new(
        packet[16], packet[17], packet[18], packet[19],
    ));
    let tcp = ihl;
    let source_port = read_u16(packet, tcp);
    let destination_port = read_u16(packet, tcp + 2);

    if source_addr == listener_addr && source_port == listener_port {
        let session = nat.lookup_back(destination_port).ok_or_else(|| {
            HammerError::internal(format!("tcp NAT session not found: {destination_port}"))
        })?;
        write_ip_addr(packet, 12, session.destination.host)?;
        write_u16(packet, tcp, session.destination.port);
        write_ip_addr(packet, 16, session.source.host)?;
        write_u16(packet, tcp + 2, session.source.port);
    } else {
        let source = SocksAddr {
            host: source_addr,
            port: source_port,
        };
        let destination = SocksAddr {
            host: destination_addr,
            port: destination_port,
        };
        let nat_port = nat.lookup_or_insert(source, destination);
        write_ip_addr(packet, 12, nat_addr)?;
        write_u16(packet, tcp, nat_port);
        write_ip_addr(packet, 16, listener_addr)?;
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
) -> Result<(), HammerError> {
    if packet.len() < 60 || packet[6] != 6 {
        return Err(HammerError::internal("invalid IPv6 TCP packet"));
    }
    let source_addr = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[8..24]).unwrap(),
    ));
    let destination_addr = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[24..40]).unwrap(),
    ));
    let tcp = 40;
    let source_port = read_u16(packet, tcp);
    let destination_port = read_u16(packet, tcp + 2);

    if source_addr == listener_addr && source_port == listener_port {
        let session = nat.lookup_back(destination_port).ok_or_else(|| {
            HammerError::internal(format!("tcp NAT session not found: {destination_port}"))
        })?;
        write_ip_addr(packet, 8, session.destination.host)?;
        write_u16(packet, tcp, session.destination.port);
        write_ip_addr(packet, 24, session.source.host)?;
        write_u16(packet, tcp + 2, session.source.port);
    } else {
        let source = SocksAddr {
            host: source_addr,
            port: source_port,
        };
        let destination = SocksAddr {
            host: destination_addr,
            port: destination_port,
        };
        let nat_port = nat.lookup_or_insert(source, destination);
        write_ip_addr(packet, 8, nat_addr)?;
        write_u16(packet, tcp, nat_port);
        write_ip_addr(packet, 24, listener_addr)?;
        write_u16(packet, tcp + 2, listener_port);
    }
    update_ipv6_tcp_checksum(packet)
}

fn update_ipv4_tcp_checksums(packet: &mut [u8], ihl: usize) -> Result<(), HammerError> {
    write_u16(packet, 10, 0);
    let ip_checksum = checksum(&packet[..ihl]);
    write_u16(packet, 10, ip_checksum);
    let tcp_len = packet.len() - ihl;
    write_u16(packet, ihl + 16, 0);
    let mut pseudo = Vec::with_capacity(12 + tcp_len);
    pseudo.extend_from_slice(&packet[12..20]);
    pseudo.push(0);
    pseudo.push(6);
    pseudo.extend_from_slice(&(tcp_len as u16).to_be_bytes());
    pseudo.extend_from_slice(&packet[ihl..]);
    let tcp_checksum = checksum(&pseudo);
    write_u16(packet, ihl + 16, tcp_checksum);
    Ok(())
}

fn update_ipv6_tcp_checksum(packet: &mut [u8]) -> Result<(), HammerError> {
    let tcp_len = packet.len() - 40;
    write_u16(packet, 40 + 16, 0);
    let mut pseudo = Vec::with_capacity(40 + tcp_len);
    pseudo.extend_from_slice(&packet[8..40]);
    pseudo.extend_from_slice(&(tcp_len as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, 6]);
    pseudo.extend_from_slice(&packet[40..]);
    let tcp_checksum = checksum(&pseudo);
    write_u16(packet, 40 + 16, tcp_checksum);
    Ok(())
}

fn update_ipv4_udp_checksums(packet: &mut [u8], udp_offset: usize) -> Result<(), HammerError> {
    write_u16(packet, 10, 0);
    let ip_checksum = checksum(&packet[..udp_offset]);
    write_u16(packet, 10, ip_checksum);
    write_u16(packet, udp_offset + 6, 0);
    let udp_len = packet.len() - udp_offset;
    let mut pseudo = Vec::with_capacity(12 + udp_len);
    pseudo.extend_from_slice(&packet[12..20]);
    pseudo.push(0);
    pseudo.push(17);
    pseudo.extend_from_slice(&(udp_len as u16).to_be_bytes());
    pseudo.extend_from_slice(&packet[udp_offset..]);
    let udp_checksum = checksum(&pseudo);
    write_u16(
        packet,
        udp_offset + 6,
        if udp_checksum == 0 {
            0xffff
        } else {
            udp_checksum
        },
    );
    Ok(())
}

fn update_ipv6_udp_checksum(packet: &mut [u8]) -> Result<(), HammerError> {
    write_u16(packet, 46, 0);
    let udp_len = packet.len() - 40;
    let mut pseudo = Vec::with_capacity(40 + udp_len);
    pseudo.extend_from_slice(&packet[8..40]);
    pseudo.extend_from_slice(&(udp_len as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, 17]);
    pseudo.extend_from_slice(&packet[40..]);
    let udp_checksum = checksum(&pseudo);
    write_u16(
        packet,
        46,
        if udp_checksum == 0 {
            0xffff
        } else {
            udp_checksum
        },
    );
    Ok(())
}

fn ipv4_udp_unreachable_packet(request: &[u8]) -> Result<Vec<u8>, HammerError> {
    if request.len() < 28 || request[9] != 17 {
        return Err(HammerError::internal("invalid IPv4 UDP packet"));
    }
    let ihl = ((request[0] & 0x0f) as usize) * 4;
    if ihl < 20 || request.len() < ihl + 8 {
        return Err(HammerError::internal("invalid IPv4 UDP header"));
    }
    let quoted_len = request.len().min(ihl + 8);
    let total_len = 20 + 8 + quoted_len;
    let mut packet = vec![0_u8; total_len];
    packet[0] = 0x45;
    write_u16(&mut packet, 2, total_len as u16);
    packet[8] = 64;
    packet[9] = 1;
    packet[12..16].copy_from_slice(&request[16..20]);
    packet[16..20].copy_from_slice(&request[12..16]);
    packet[20] = 3;
    packet[21] = 3;
    packet[28..].copy_from_slice(&request[..quoted_len]);
    let ip_checksum = checksum(&packet[..20]);
    write_u16(&mut packet, 10, ip_checksum);
    let icmp_checksum = checksum(&packet[20..]);
    write_u16(&mut packet, 22, icmp_checksum);
    Ok(packet)
}

fn ipv6_udp_unreachable_packet(request: &[u8]) -> Result<Vec<u8>, HammerError> {
    if request.len() < 48 || request[6] != 17 {
        return Err(HammerError::internal("invalid IPv6 UDP packet"));
    }
    let quoted_len = request.len().min(1232);
    let payload_len = 8 + quoted_len;
    let mut packet = vec![0_u8; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = 58;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&request[24..40]);
    packet[24..40].copy_from_slice(&request[8..24]);
    packet[40] = 1;
    packet[41] = 4;
    packet[48..].copy_from_slice(&request[..quoted_len]);
    update_ipv6_icmp_checksum(&mut packet)?;
    Ok(packet)
}

fn ipv4_tcp_reset_packet(request: &[u8]) -> Result<Vec<u8>, HammerError> {
    if request.len() < 40 || request[9] != 6 {
        return Err(HammerError::internal("invalid IPv4 TCP packet"));
    }
    let ihl = ((request[0] & 0x0f) as usize) * 4;
    if ihl < 20 || request.len() < ihl + 20 {
        return Err(HammerError::internal("invalid IPv4 TCP header"));
    }
    let tcp_len = request.len() - ihl;
    let data_offset = ((request[ihl + 12] >> 4) as usize) * 4;
    if data_offset < 20 || tcp_len < data_offset {
        return Err(HammerError::internal("invalid TCP data offset"));
    }
    let mut packet = vec![0_u8; 40];
    packet[0] = 0x45;
    write_u16(&mut packet, 2, 40);
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&request[16..20]);
    packet[16..20].copy_from_slice(&request[12..16]);
    write_u16(&mut packet, 20, read_u16(request, ihl + 2));
    write_u16(&mut packet, 22, read_u16(request, ihl));
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
    packet[32] = 0x50;
    update_ipv4_tcp_checksums(&mut packet, 20)?;
    Ok(packet)
}

fn ipv6_tcp_reset_packet(request: &[u8]) -> Result<Vec<u8>, HammerError> {
    if request.len() < 60 || request[6] != 6 {
        return Err(HammerError::internal("invalid IPv6 TCP packet"));
    }
    let tcp = 40;
    let data_offset = ((request[tcp + 12] >> 4) as usize) * 4;
    if data_offset < 20 || request.len() < tcp + data_offset {
        return Err(HammerError::internal("invalid TCP data offset"));
    }
    let tcp_len = request.len() - tcp;
    let mut packet = vec![0_u8; 60];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&20_u16.to_be_bytes());
    packet[6] = 6;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&request[24..40]);
    packet[24..40].copy_from_slice(&request[8..24]);
    write_u16(&mut packet, 40, read_u16(request, tcp + 2));
    write_u16(&mut packet, 42, read_u16(request, tcp));
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
    packet[52] = 0x50;
    update_ipv6_tcp_checksum(&mut packet)?;
    Ok(packet)
}

fn update_ipv6_icmp_checksum(packet: &mut [u8]) -> Result<(), HammerError> {
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

fn read_u16(packet: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([packet[offset], packet[offset + 1]])
}

fn read_u32(packet: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        packet[offset],
        packet[offset + 1],
        packet[offset + 2],
        packet[offset + 3],
    ])
}

fn write_u16(packet: &mut [u8], offset: usize, value: u16) {
    packet[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn write_u32(packet: &mut [u8], offset: usize, value: u32) {
    packet[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn write_ip_addr(packet: &mut [u8], offset: usize, addr: IpAddr) -> Result<(), HammerError> {
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

async fn accept_tcp_loop(
    logger: Logger,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    tcp_nat: Arc<StdMutex<SystemTcpNat>>,
    inbound_tag: String,
    listener: TcpListener,
) {
    loop {
        let (mut inbound, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                logger.debug(format!("system TCP listener closed: {err}"));
                return;
            }
        };
        let session = {
            let mut nat = tcp_nat.lock().expect("tcp_nat poisoned");
            nat.lookup_back(peer.port())
        };
        let Some(session) = session else {
            logger.debug(format!("unknown system TCP NAT session: {}", peer.port()));
            continue;
        };
        let router = Arc::clone(&router);
        let outbound = Arc::clone(&outbound);
        let logger = logger.clone();
        let inbound_tag = inbound_tag.clone();
        tokio::spawn(async move {
            let mut metadata = RouteMetadata {
                inbound: inbound_tag,
                network: Network::Tcp,
                source: Some(session.source.clone()),
                destination: Some(session.destination.clone()),
                ..Default::default()
            };
            let decision = match router.match_route(&mut metadata) {
                Ok(decision) => decision,
                Err(err) => {
                    logger.debug(format!("route TCP connection: {err}"));
                    return;
                }
            };
            let RouteDecision::Route {
                outbound: outbound_tag,
            } = decision
            else {
                logger.debug("system TCP connection rejected");
                return;
            };
            let Some(outbound) = outbound.get(&outbound_tag) else {
                logger.error(format!("outbound not found: {outbound_tag}"));
                return;
            };
            let mut outbound_stream = match outbound
                .dial(Network::Tcp, session.destination.clone(), &[])
                .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    logger.debug(format!("dial TCP outbound: {err}"));
                    return;
                }
            };
            match copy_bidirectional(&mut inbound, &mut outbound_stream).await {
                Ok((from_inbound, from_outbound)) => logger.debug(format!(
                    "system TCP copied {from_inbound}/{from_outbound} bytes"
                )),
                Err(err) => logger.debug(format!("copy system TCP: {err}")),
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn packet_loop(
    logger: Logger,
    router: Arc<Router>,
    dns_router: Arc<DnsRouter>,
    outbound: Arc<OutboundManager>,
    inbound_tag: String,
    device: Arc<dyn TunDevice>,
    tcp_nat: Arc<StdMutex<SystemTcpNat>>,
    udp_flows: Arc<Mutex<UdpFlowMap>>,
    routes: SystemStackRoutes,
    udp_timeout: Duration,
) {
    logger.info("system packet loop started");
    loop {
        let mut packet = match device.recv().await {
            Ok(packet) => packet,
            Err(err) => {
                logger.debug(format!("read TUN packet loop stopped: {err}"));
                return;
            }
        };
        if packet.is_empty() {
            tokio::task::yield_now().await;
            continue;
        }
        let parsed = match parse_ip_packet(&packet) {
            Ok(parsed) => parsed,
            Err(err) => {
                logger.trace(format!("ignore unsupported TUN packet: {err}"));
                continue;
            }
        };
        if !is_global_unicast(parsed.destination.host) {
            continue;
        }
        match parsed.network {
            Network::Tcp => {
                let Some(route) = routes.for_packet(&packet) else {
                    logger.debug("missing system TCP route for packet family");
                    continue;
                };
                let rewrite_result = {
                    let mut nat = tcp_nat.lock().expect("tcp_nat poisoned");
                    process_system_tcp_packet(
                        &mut packet,
                        &mut *nat,
                        route.listener_addr,
                        route.nat_addr,
                        route.listener_port,
                    )
                };
                if let Err(err) = rewrite_result {
                    logger.debug(format!("rewrite system TCP packet: {err}"));
                    continue;
                }
                if let Err(err) = device.send(packet).await {
                    logger.debug(format!("write system TCP packet: {err}"));
                }
            }
            Network::Udp => {
                if let Err(err) = handle_system_udp_packet(
                    logger.clone(),
                    Arc::clone(&router),
                    Arc::clone(&dns_router),
                    Arc::clone(&outbound),
                    inbound_tag.clone(),
                    Arc::clone(&device),
                    Arc::clone(&udp_flows),
                    udp_timeout,
                    packet,
                    parsed,
                )
                .await
                {
                    logger.debug(format!("handle system UDP packet: {err}"));
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_system_udp_packet(
    logger: Logger,
    router: Arc<Router>,
    dns_router: Arc<DnsRouter>,
    outbound: Arc<OutboundManager>,
    inbound_tag: String,
    device: Arc<dyn TunDevice>,
    udp_flows: Arc<Mutex<UdpFlowMap>>,
    udp_timeout: Duration,
    packet: Vec<u8>,
    parsed: ParsedIpPacket,
) -> Result<(), HammerError> {
    let mut tun_packet = TunPacket {
        metadata: RouteMetadata {
            inbound: inbound_tag,
            network: Network::Udp,
            source: Some(parsed.source.clone()),
            destination: Some(parsed.destination.clone()),
            ..Default::default()
        },
        payload: parsed.payload.clone(),
    };
    sniff_packet(&mut tun_packet);
    match router.match_route(&mut tun_packet.metadata)? {
        RouteDecision::HijackDns => {
            let message = <Message as MessageExt>::from_bytes(&tun_packet.payload)?;
            let response = dns_router
                .exchange(message, DnsQueryOptions::default())
                .await?;
            let response_bytes = MessageExt::to_bytes(&response)?;
            let response_packet =
                udp_response_packet(&packet, parsed.destination, &response_bytes)?;
            device.send(response_packet).await?;
        }
        RouteDecision::Reject { method } => {
            let message = format!(
                "drop UDP packet by reject rule: method={}, destination={}, protocol={}",
                method, parsed.destination, tun_packet.metadata.protocol
            );
            if tun_packet.metadata.protocol == "quic" {
                logger.trace(message);
            } else {
                logger.debug(message);
            }
            if let Ok(response) = udp_unreachable_packet(&packet) {
                device.send(response).await?;
            }
        }
        RouteDecision::Route {
            outbound: outbound_tag,
        } => {
            let key = UdpFlowKey {
                outbound: outbound_tag.clone(),
                source: (parsed.source.host, parsed.source.port),
                destination: (parsed.destination.host, parsed.destination.port),
            };
            let sender = if let Some(sender) = {
                let mut flows = udp_flows.lock().await;
                if let Some(flow) = flows.get_mut(&key) {
                    flow.last_active = Instant::now();
                    Some(flow.sender.clone())
                } else {
                    None
                }
            } {
                sender
            } else {
                let Some(outbound_item) = outbound.get(&outbound_tag) else {
                    return Err(HammerError::internal(format!(
                        "outbound not found: {outbound_tag}"
                    )));
                };
                let packet_conn = outbound_item.listen_packet().await?;
                let template = UdpResponseTemplate::from_request(&packet)?;
                let (tx, rx) = mpsc::channel(SYSTEM_UDP_CHANNEL_CAPACITY);

                let mut flows = udp_flows.lock().await;
                if let Some(flow) = flows.get_mut(&key) {
                    flow.last_active = Instant::now();
                    flow.sender.clone()
                } else {
                    evict_udp_flow_if_needed(&mut flows);
                    flows.insert(
                        key.clone(),
                        UdpFlowState {
                            sender: tx.clone(),
                            last_active: Instant::now(),
                        },
                    );
                    tokio::spawn(system_udp_flow_loop(
                        logger.clone(),
                        Arc::clone(&device),
                        Arc::clone(&udp_flows),
                        key,
                        packet_conn,
                        parsed.destination.clone(),
                        template,
                        udp_timeout,
                        rx,
                    ));
                    tx
                }
            };
            if let Err(err) = sender.try_send(tun_packet.payload) {
                logger.debug(format!("drop UDP packet for busy system flow: {err}"));
            }
        }
    }
    Ok(())
}

async fn system_udp_flow_loop(
    logger: Logger,
    device: Arc<dyn TunDevice>,
    udp_flows: Arc<Mutex<UdpFlowMap>>,
    key: UdpFlowKey,
    mut packet_conn: Box<dyn hammer_adapter::ProxyPacketConn>,
    destination: SocksAddr,
    response_template: UdpResponseTemplate,
    udp_timeout: Duration,
    mut rx: mpsc::Receiver<Vec<u8>>,
) {
    let idle_timer = time::sleep(udp_timeout);
    tokio::pin!(idle_timer);
    loop {
        tokio::select! {
            next = rx.recv() => {
                let Some(payload) = next else {
                    break;
                };
                idle_timer.as_mut().reset(Instant::now() + udp_timeout);
                if let Err(err) = packet_conn.send_to(destination.clone(), &payload).await {
                    logger.debug(format!("send system UDP outbound: {err}"));
                    break;
                }
            }
            response = packet_conn.recv_from() => {
                let response = match response {
                    Ok(response) => response,
                    Err(err) => {
                        logger.debug(format!("receive system UDP outbound: {err}"));
                        break;
                    }
                };
                idle_timer.as_mut().reset(Instant::now() + udp_timeout);
                match response_template.build(response.destination, &response.payload) {
                    Ok(packet) => {
                        if let Err(err) = device.send(packet).await {
                            logger.debug(format!("write system UDP response: {err}"));
                            break;
                        }
                    }
                    Err(err) => logger.debug(format!("build system UDP response: {err}")),
                }
            }
            _ = &mut idle_timer => {
                break;
            }
        }
    }
    udp_flows.lock().await.remove(&key);
}

fn evict_udp_flow_if_needed(flows: &mut UdpFlowMap) {
    if flows.len() < SYSTEM_UDP_FLOW_CAPACITY {
        return;
    }
    if let Some(oldest_key) = flows
        .iter()
        .min_by_key(|(_, flow)| flow.last_active)
        .map(|(key, _)| key.clone())
    {
        flows.remove(&oldest_key);
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
            packet.metadata.domain = Some(value.trim().to_owned());
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
        packet.metadata.domain = Some(domain);
    }
}

fn sniff_dns(packet: &mut TunPacket) {
    if packet.payload.len() < 12 {
        return;
    }
    if packet.payload[2] & 0x80 != 0 {
        return;
    }
    let questions = u16::from_be_bytes([packet.payload[4], packet.payload[5]]);
    let answers = u16::from_be_bytes([packet.payload[6], packet.payload[7]]);
    if questions == 0 || answers != 0 {
        return;
    }
    packet.metadata.protocol = "dns".to_owned();
}

fn sniff_quic(packet: &mut TunPacket) {
    if packet.payload.len() < 7 || packet.payload[0] & 0xc0 != 0xc0 {
        return;
    }
    let version = u32::from_be_bytes([
        packet.payload[1],
        packet.payload[2],
        packet.payload[3],
        packet.payload[4],
    ]);
    if !matches!(version, 0x0000_0001 | 0x0000_0002 | 0xff00_001d) {
        return;
    }
    packet.metadata.protocol = "quic".to_owned();
}

fn sniff_stun(packet: &mut TunPacket) {
    if packet.payload.len() >= 20 && packet.payload[4..8] == [0x21, 0x12, 0xa4, 0x42] {
        packet.metadata.protocol = "stun".to_owned();
    }
}

fn parse_tls_sni(_payload: &[u8]) -> Option<String> {
    None
}
