use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use hammer_adapter::{
    DnsQueryOptions, Network, OutboundManager as _, RouteDecision, RouteMetadata, SocksAddr,
};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use hickory_proto::op::Message;
use smoltcp::wire::IpProtocol;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, timeout};

pub use crate::TunInbound;
use crate::dns::MessageExt;
use crate::{DnsRouter, OutboundManager, Router};

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
                let payload = stream.read_to_end().await?;
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
