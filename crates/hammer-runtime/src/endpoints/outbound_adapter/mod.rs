//! L4↔L3 outbound adapter wrapping an `Endpoint`.
//!
//! DNS transport (`UdpDnsTransport::exchange`, `tcp_exchange_via_or_direct`,
//! DoH) reaches the wire through `outbound.listen_packet()` /
//! `outbound.dial()`. After the wg endpoint refactor split `Endpoint` off
//! from `Outbound`, an endpoint id can no longer be resolved through that
//! API — DNS `via = "<endpoint-id>"` died with it. This adapter bridges
//! the gap: it wraps an `Arc<dyn Endpoint>`, implements `Outbound`, and
//! translates L4 sends into IPv4 packets pushed straight through
//! `Endpoint::ip_send_clone`. Inbound replies come back via
//! `Endpoint::ip_local_recv_take` and get demuxed into the per-flow
//! receivers handed out from `listen_packet`.
//!
//! Surface: UDP via `listen_packet`, TCP / DoH via `dial(Network::Tcp, …)`.
//! UDP packets are assembled in-place and pushed straight into the endpoint's
//! encrypt channel; TCP gets a per-dial smoltcp termination — see the `tcp`
//! submodule.

#![cfg(feature = "endpoint")]

mod tcp;

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};

use async_trait::async_trait;
use bytes::Bytes;
use hammer_adapter::{
    BufferFrame, ComponentMeta, ComponentMetadata, ComponentMetricsMeta, DataPlaneRuntime,
    Endpoint, EndpointLocalFlow, Network, Outbound, ProxyIcmpConn, ProxyPacketConn, ProxyStream,
    RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::log::Logger;
use tokio::sync::mpsc;

/// Per-flow inbound channel capacity. DNS queries are single-shot; 16
/// leaves plenty of headroom against scheduler stalls without bloating
/// resident memory.
const FLOW_QUEUE: usize = 16;
/// Cap on port-allocation retries before we declare exhaustion. Real DNS
/// QPS makes contention near-impossible, so this is purely a defensive
/// loop bound.
const PORT_ALLOC_RETRY_CAP: usize = 256;
const EPHEMERAL_MIN: u16 = 32_768;
const EPHEMERAL_MAX: u16 = u16::MAX;
const IPV4_MIN_HEADER: usize = 20;
const UDP_HEADER: usize = 8;
const IP_PROTOCOL_UDP: u8 = 17;
const IP_PROTOCOL_TCP: u8 = 6;
const DEFAULT_TTL: u8 = 64;
/// Backstop on the TCP smoltcp termination concurrency. DNS / DoH realistically
/// runs 1-3 sockets in flight; this is a misconfiguration guardrail, not a
/// performance knob.
const MAX_TCP_FLOWS: usize = 8;
/// MTU advertised to smoltcp inside the adapter. We default to a conservative
/// value that fits inside any reasonable wg payload after `WIREGUARD_OVERHEAD`.
const ADAPTER_TCP_MTU: usize = 1280;
/// TCP driver ingress channel capacity (IP packets per socket). DNS / DoH
/// roundtrips are short; 16 leaves headroom for handshake bursts.
const TCP_INGRESS_QUEUE: usize = 16;

pub struct EndpointOutboundAdapter {
    id: String,
    logger: Logger,
    /// Source address used as the IPv4 src for every adapter-originated
    /// packet. Inferred from `Endpoint::interface_addresses()`; `None`
    /// means the endpoint only owns IPv6 addresses, which we currently
    /// reject in `send_to` and `dial`.
    interface_v4: Option<Ipv4Addr>,
    udp_flows: Arc<StdMutex<UdpFlowMap>>,
    /// Mirrors `udp_flows.lock().by_port.len()` so the demux task can
    /// skip parsing entirely (a single relaxed atomic load) when no UDP
    /// flow is outstanding.
    udp_flow_count: Arc<AtomicUsize>,
    /// TCP per-flow ingress senders keyed by adapter source port. Demux
    /// task pushes raw IPv4-TCP packets here; the matching tcp::run_driver
    /// task on the other side feeds them into smoltcp.
    tcp_flows: Arc<StdMutex<TcpFlowMap>>,
    local_flows: Arc<StdMutex<HashSet<EndpointLocalFlow>>>,
    port_pool: StdMutex<()>,
    next_port: AtomicU16,
    /// One-shot demux startup guard. The first `listen_packet` or
    /// `dial(Tcp)` call takes the endpoint's local receiver and spawns
    /// the demux task; subsequent calls are no-ops.
    demux_started: OnceLock<bool>,
    /// Held strongly so the endpoint stays alive at least as long as the
    /// adapter (which OutboundManager keeps for the process lifetime).
    endpoint: Arc<dyn Endpoint>,
}

struct UdpFlowMap {
    by_port: HashMap<u16, mpsc::Sender<EndpointUdpDatagram>>,
}

struct TcpFlowMap {
    by_port: HashMap<u16, mpsc::Sender<Bytes>>,
}

impl EndpointOutboundAdapter {
    pub fn arc(logger: Logger, id: String, endpoint: Arc<dyn Endpoint>) -> Arc<Self> {
        let interface_v4 = endpoint
            .interface_addresses()
            .iter()
            .find_map(|net| match net.addr() {
                IpAddr::V4(v4) => Some(v4),
                IpAddr::V6(_) => None,
            });
        Arc::new(Self {
            id,
            logger,
            interface_v4,
            udp_flows: Arc::new(StdMutex::new(UdpFlowMap {
                by_port: HashMap::new(),
            })),
            udp_flow_count: Arc::new(AtomicUsize::new(0)),
            tcp_flows: Arc::new(StdMutex::new(TcpFlowMap {
                by_port: HashMap::new(),
            })),
            local_flows: Arc::new(StdMutex::new(HashSet::new())),
            port_pool: StdMutex::new(()),
            next_port: AtomicU16::new(EPHEMERAL_MIN),
            demux_started: OnceLock::new(),
            endpoint,
        })
    }

    /// Allocate an ephemeral source port that is currently *unused by both*
    /// the UDP and TCP flow maps. The two share a single port runtime so
    /// inner-IP responses can be unambiguously demuxed by (protocol,
    /// dst_port).
    fn allocate_port_locked(&self) -> CoreResult<u16> {
        for _ in 0..PORT_ALLOC_RETRY_CAP {
            let raw = self.next_port.load(Ordering::Relaxed);
            let port = if raw < EPHEMERAL_MIN {
                EPHEMERAL_MIN
            } else {
                raw
            };
            let next = if port == EPHEMERAL_MAX {
                EPHEMERAL_MIN
            } else {
                port + 1
            };
            self.next_port.store(next, Ordering::Relaxed);
            let udp = self.udp_flows.lock().expect("UdpFlowMap poisoned");
            let tcp = self.tcp_flows.lock().expect("TcpFlowMap poisoned");
            if !udp.by_port.contains_key(&port) && !tcp.by_port.contains_key(&port) {
                return Ok(port);
            }
        }
        Err(CoreError::internal(format!(
            "endpoint adapter '{}' exhausted ephemeral port runtime",
            self.id
        )))
    }

    /// Lazily kick off the demux task on first `listen_packet` / `dial(Tcp)`.
    /// We don't do it in the constructor because the endpoint is still
    /// `Idle` at that point; `ip_local_recv_take` only returns `Some` once
    /// the endpoint's lifecycle reaches `Start`.
    fn ensure_demux_started(&self) -> CoreResult<()> {
        let started = *self.demux_started.get_or_init(|| {
            let Some(local_rx) = self.endpoint.ip_local_recv_take() else {
                return false;
            };
            let id = self.id.clone();
            let logger = self.logger.clone();
            let udp_flows = Arc::clone(&self.udp_flows);
            let udp_flow_count = Arc::clone(&self.udp_flow_count);
            let tcp_flows = Arc::clone(&self.tcp_flows);
            crate::spawn::spawn(run_demux_task(
                id,
                logger,
                local_rx,
                udp_flows,
                udp_flow_count,
                tcp_flows,
            ));
            true
        });
        if !started {
            return Err(CoreError::internal(format!(
                "endpoint adapter '{}' could not acquire local-recv channel \
                 (endpoint may not be started or another consumer took it)",
                self.id
            )));
        }
        Ok(())
    }
}

async fn run_demux_task(
    id: String,
    logger: Logger,
    mut local_rx: mpsc::Receiver<Bytes>,
    udp_flows: Arc<StdMutex<UdpFlowMap>>,
    udp_flow_count: Arc<AtomicUsize>,
    tcp_flows: Arc<StdMutex<TcpFlowMap>>,
) {
    while let Some(pkt) = local_rx.recv().await {
        // Quick protocol probe before we commit to parsing anything.
        let Some(protocol) = ipv4_protocol(&pkt) else {
            continue;
        };
        match protocol {
            IP_PROTOCOL_UDP => {
                if udp_flow_count.load(Ordering::Relaxed) == 0 {
                    continue; // fast path: no outstanding UDP flows
                }
                let Some((dst_port, datagram)) = parse_inner_ipv4_udp(&pkt) else {
                    continue;
                };
                let map = udp_flows.lock().expect("UdpFlowMap poisoned");
                if let Some(slot) = map.by_port.get(&dst_port) {
                    // Backpressure: drop on full. DNS retries handle ephemeral loss.
                    let _ = slot.try_send(datagram);
                }
            }
            IP_PROTOCOL_TCP => {
                let Some(dst_port) = parse_inner_ipv4_tcp_dst_port(&pkt) else {
                    continue;
                };
                let map = tcp_flows.lock().expect("TcpFlowMap poisoned");
                if let Some(slot) = map.by_port.get(&dst_port) {
                    let _ = slot.try_send(pkt.clone());
                }
            }
            _ => {} // not interested
        }
    }
    logger.debug(format!("endpoint adapter '{id}' demux task exited"));
}

fn ipv4_protocol(pkt: &Bytes) -> Option<u8> {
    if pkt.len() < IPV4_MIN_HEADER {
        return None;
    }
    if (pkt[0] >> 4) != 4 {
        return None;
    }
    Some(pkt[9])
}

fn parse_inner_ipv4_tcp_dst_port(pkt: &Bytes) -> Option<u16> {
    if pkt.len() < IPV4_MIN_HEADER + 4 {
        return None;
    }
    let ihl = ((pkt[0] & 0x0f) as usize) * 4;
    if ihl < IPV4_MIN_HEADER || pkt.len() < ihl + 4 {
        return None;
    }
    Some(u16::from_be_bytes([pkt[ihl + 2], pkt[ihl + 3]]))
}

/// Inline IPv4 + UDP header parser tuned for the demux fast path.
///
/// Returns the destination port (i.e. the adapter source port that the
/// peer is replying to) and a datagram carrying the remote
/// `src_ip:src_port` so that DNS transport sees who answered.
fn parse_inner_ipv4_udp(pkt: &Bytes) -> Option<(u16, EndpointUdpDatagram)> {
    if pkt.len() < IPV4_MIN_HEADER + UDP_HEADER {
        return None;
    }
    if (pkt[0] >> 4) != 4 {
        return None;
    }
    let ihl = ((pkt[0] & 0x0f) as usize) * 4;
    if ihl < IPV4_MIN_HEADER || pkt.len() < ihl + UDP_HEADER {
        return None;
    }
    if pkt[9] != IP_PROTOCOL_UDP {
        return None;
    }
    let src_ip = Ipv4Addr::new(pkt[12], pkt[13], pkt[14], pkt[15]);
    let src_port = u16::from_be_bytes([pkt[ihl], pkt[ihl + 1]]);
    let dst_port = u16::from_be_bytes([pkt[ihl + 2], pkt[ihl + 3]]);
    let payload = pkt.slice(ihl + UDP_HEADER..);
    Some((
        dst_port,
        EndpointUdpDatagram {
            destination: SocksAddr::ip(IpAddr::V4(src_ip), src_port),
            payload,
        },
    ))
}

impl ComponentMetadata for EndpointOutboundAdapter {
    fn component_meta(&self) -> ComponentMeta {
        ComponentMeta::new(
            "outbound",
            "endpoint-adapter",
            self.id.clone(),
            vec![Network::Udp, Network::Tcp],
            Vec::new(),
            Some(ComponentMetricsMeta {
                module: "outbound",
                component_type: "outbound",
            }),
        )
    }
}

#[async_trait]
impl Outbound for EndpointOutboundAdapter {
    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> CoreResult<Box<dyn ProxyStream>> {
        match network {
            Network::Tcp => self.dial_tcp(destination, initial_payload).await,
            Network::Udp => Err(CoreError::internal(format!(
                "UDP via endpoint '{}' must use listen_packet, not dial",
                self.id
            ))),
            other => Err(CoreError::internal(format!(
                "{other:?} via endpoint '{}' is not supported",
                self.id
            ))),
        }
    }

    async fn listen_packet(&self) -> CoreResult<Box<dyn ProxyPacketConn>> {
        self.ensure_demux_started()?;
        let interface_v4 = self.interface_v4.ok_or_else(|| {
            CoreError::internal(format!(
                "endpoint '{}' has no IPv4 interface address",
                self.id
            ))
        })?;
        let (tx, rx) = mpsc::channel::<EndpointUdpDatagram>(FLOW_QUEUE);
        let _port_guard = self.port_pool.lock().expect("port pool poisoned");
        let local_port = self.allocate_port_locked()?;
        {
            let mut flows = self.udp_flows.lock().expect("UdpFlowMap poisoned");
            flows.by_port.insert(local_port, tx);
        }
        drop(_port_guard);
        self.udp_flow_count.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(EndpointUdpConn {
            local_port,
            endpoint: Arc::clone(&self.endpoint),
            interface_v4,
            flows: Arc::downgrade(&self.udp_flows),
            local_flows: Arc::clone(&self.local_flows),
            flow_count: Arc::downgrade(&self.udp_flow_count),
            rx,
            registered_routes: Vec::new(),
        }))
    }

    async fn listen_icmp(&self) -> CoreResult<Box<dyn ProxyIcmpConn>> {
        Err(CoreError::internal(format!(
            "ICMP via endpoint '{}' is not supported",
            self.id
        )))
    }

    fn reset(&self) {
        let _port_guard = self.port_pool.lock().expect("port pool poisoned");
        let mut udp = self.udp_flows.lock().expect("UdpFlowMap poisoned");
        udp.by_port.clear();
        self.udp_flow_count.store(0, Ordering::Relaxed);
        let mut tcp = self.tcp_flows.lock().expect("TcpFlowMap poisoned");
        tcp.by_port.clear();
        // Dropping the TCP flow senders closes the ingress channels; the
        // per-flow driver tasks see `None` on `ingress_rx.recv()` and exit
        // via their `on_close` drop guard.
        let mut local_flows = self.local_flows.lock().expect("local flows poisoned");
        for flow in local_flows.drain() {
            self.endpoint.unregister_local_flow(flow);
        }
    }
}

impl EndpointOutboundAdapter {
    async fn dial_tcp(
        &self,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> CoreResult<Box<dyn ProxyStream>> {
        self.ensure_demux_started()?;
        let interface_v4 = self.interface_v4.ok_or_else(|| {
            CoreError::internal(format!(
                "endpoint '{}' has no IPv4 interface address",
                self.id
            ))
        })?;
        let dst_v4 = match destination.host {
            IpAddr::V4(v4) => v4,
            IpAddr::V6(_) => {
                return Err(CoreError::internal(
                    "IPv6 destination via endpoint adapter is not yet supported",
                ));
            }
        };
        {
            let tcp = self.tcp_flows.lock().expect("TcpFlowMap poisoned");
            if tcp.by_port.len() >= MAX_TCP_FLOWS {
                return Err(CoreError::internal(format!(
                    "endpoint adapter '{}' TCP socket runtime exhausted ({} in flight)",
                    self.id, MAX_TCP_FLOWS,
                )));
            }
        }
        let (ingress_tx, ingress_rx) = mpsc::channel::<Bytes>(TCP_INGRESS_QUEUE);
        let _port_guard = self.port_pool.lock().expect("port pool poisoned");
        let local_port = self.allocate_port_locked()?;
        {
            let mut tcp = self.tcp_flows.lock().expect("TcpFlowMap poisoned");
            tcp.by_port.insert(local_port, ingress_tx);
        }
        drop(_port_guard);
        let local_flow = EndpointLocalFlow {
            network: Network::Tcp,
            local: SocketAddr::new(IpAddr::V4(interface_v4), local_port),
            remote: SocketAddr::new(IpAddr::V4(dst_v4), destination.port),
        };
        register_endpoint_local_flow(&self.endpoint, &self.local_flows, local_flow);
        let tcp_flows_weak = Arc::downgrade(&self.tcp_flows);
        let endpoint = Arc::clone(&self.endpoint);
        let local_flows = Arc::clone(&self.local_flows);
        let on_close: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if let Some(map) = tcp_flows_weak.upgrade() {
                let mut map = map.lock().expect("TcpFlowMap poisoned");
                map.by_port.remove(&local_port);
            }
            unregister_endpoint_local_flow(&endpoint, &local_flows, local_flow);
        });

        tcp::dial_tcp(tcp::TcpDialParams {
            logger: self.logger.clone(),
            interface_v4,
            local_port,
            dst_v4,
            dst_port: destination.port,
            mtu: ADAPTER_TCP_MTU,
            egress_tx: self.endpoint.ip_send_clone(),
            ingress_rx,
            initial_payload: Bytes::copy_from_slice(initial_payload),
            on_close,
        })
        .await
    }
}

fn register_endpoint_local_flow(
    endpoint: &Arc<dyn Endpoint>,
    local_flows: &Arc<StdMutex<HashSet<EndpointLocalFlow>>>,
    flow: EndpointLocalFlow,
) {
    let mut flows = local_flows.lock().expect("local flows poisoned");
    if flows.insert(flow) {
        endpoint.register_local_flow(flow);
    }
}

fn unregister_endpoint_local_flow(
    endpoint: &Arc<dyn Endpoint>,
    local_flows: &Arc<StdMutex<HashSet<EndpointLocalFlow>>>,
    flow: EndpointLocalFlow,
) {
    let mut flows = local_flows.lock().expect("local flows poisoned");
    if flows.remove(&flow) {
        endpoint.unregister_local_flow(flow);
    }
}

struct EndpointUdpConn {
    local_port: u16,
    endpoint: Arc<dyn Endpoint>,
    interface_v4: Ipv4Addr,
    flows: Weak<StdMutex<UdpFlowMap>>,
    local_flows: Arc<StdMutex<HashSet<EndpointLocalFlow>>>,
    flow_count: Weak<AtomicUsize>,
    rx: mpsc::Receiver<EndpointUdpDatagram>,
    registered_routes: Vec<EndpointLocalFlow>,
}

struct EndpointUdpDatagram {
    destination: SocksAddr,
    payload: Bytes,
}

impl Drop for EndpointUdpConn {
    fn drop(&mut self) {
        for flow in self.registered_routes.drain(..) {
            unregister_endpoint_local_flow(&self.endpoint, &self.local_flows, flow);
        }
        if let Some(flows) = self.flows.upgrade() {
            let mut map = flows.lock().expect("UdpFlowMap poisoned");
            if map.by_port.remove(&self.local_port).is_some() {
                if let Some(count) = self.flow_count.upgrade() {
                    count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[async_trait(?Send)]
impl ProxyPacketConn for EndpointUdpConn {
    async fn send(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<()> {
        let mut result = Ok(());
        for index in frame.drain_indices() {
            if result.is_ok() {
                result = async {
                    let metadata = runtime.metadata(index)?;
                    let destination = metadata.destination.ok_or_else(|| {
                        CoreError::internal("endpoint UDP frame missing destination")
                    })?;
                    let payload = runtime.copy_current_chain(index)?;
                    self.send_payload(destination, &payload).await
                }
                .await;
            }
            runtime.free_index(index);
        }
        result
    }

    async fn recv(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        max: usize,
    ) -> CoreResult<()> {
        if max == 0 {
            return Err(CoreError::internal("endpoint UDP recv max must be nonzero"));
        }
        runtime.free_frame(frame);
        let datagram = self
            .rx
            .recv()
            .await
            .ok_or_else(|| CoreError::internal("endpoint UDP flow channel closed"))?;
        let mut metadata = RouteMetadata::default();
        metadata.destination = Some(datagram.destination);
        let index = runtime.alloc_index_with_bytes(metadata, &datagram.payload)?;
        if let Err(err) = frame.push_index(index) {
            runtime.free_index(index);
            return Err(err);
        }
        Ok(())
    }
}

impl EndpointUdpConn {
    async fn send_payload(&mut self, destination: SocksAddr, payload: &[u8]) -> CoreResult<()> {
        let dst_v4 = match destination.host {
            IpAddr::V4(v4) => v4,
            IpAddr::V6(_) => {
                return Err(CoreError::internal(
                    "IPv6 destination via endpoint adapter is not yet supported",
                ));
            }
        };
        let pkt = build_ipv4_udp(
            self.interface_v4,
            self.local_port,
            dst_v4,
            destination.port,
            payload,
        );
        let flow = EndpointLocalFlow {
            network: Network::Udp,
            local: SocketAddr::new(IpAddr::V4(self.interface_v4), self.local_port),
            remote: SocketAddr::new(IpAddr::V4(dst_v4), destination.port),
        };
        if !self.registered_routes.contains(&flow) {
            register_endpoint_local_flow(&self.endpoint, &self.local_flows, flow);
            self.registered_routes.push(flow);
        }
        if self
            .endpoint
            .ip_send_clone()
            .send(Bytes::from(pkt))
            .await
            .is_err()
        {
            unregister_endpoint_local_flow(&self.endpoint, &self.local_flows, flow);
            self.registered_routes
                .retain(|registered| *registered != flow);
            return Err(CoreError::internal("endpoint encrypt channel closed"));
        }
        Ok(())
    }
}

/// Build a minimal IPv4 + UDP packet for the adapter's send path.
///
/// IHL is fixed at 5 (no options), DSCP/ECN zero, IP-id zero (the wg
/// receiver doesn't care; we're not generating fragments). Both
/// checksums are computed inline so we stay independent of the
/// stack.rs `update_ipv4_udp_checksums` family, which is module-private.
fn build_ipv4_udp(
    src_ip: Ipv4Addr,
    src_port: u16,
    dst_ip: Ipv4Addr,
    dst_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = IPV4_MIN_HEADER + UDP_HEADER + payload.len();
    let mut pkt = Vec::with_capacity(total_len);

    // --- IPv4 header (20 bytes, no options) ---
    pkt.push(0x45); // version=4, IHL=5
    pkt.push(0x00); // DSCP / ECN
    pkt.extend_from_slice(&(total_len as u16).to_be_bytes());
    pkt.extend_from_slice(&[0, 0]); // identification
    pkt.extend_from_slice(&[0, 0]); // flags + fragment offset
    pkt.push(DEFAULT_TTL);
    pkt.push(IP_PROTOCOL_UDP);
    pkt.extend_from_slice(&[0, 0]); // header checksum placeholder
    pkt.extend_from_slice(&src_ip.octets());
    pkt.extend_from_slice(&dst_ip.octets());
    let ip_checksum = internet_checksum(&pkt[..IPV4_MIN_HEADER]);
    pkt[10..12].copy_from_slice(&ip_checksum.to_be_bytes());

    // --- UDP header (8 bytes) + payload ---
    let udp_len = UDP_HEADER + payload.len();
    pkt.extend_from_slice(&src_port.to_be_bytes());
    pkt.extend_from_slice(&dst_port.to_be_bytes());
    pkt.extend_from_slice(&(udp_len as u16).to_be_bytes());
    pkt.extend_from_slice(&[0, 0]); // checksum placeholder
    pkt.extend_from_slice(payload);
    let udp_checksum = udp_checksum_ipv4(src_ip, dst_ip, &pkt[IPV4_MIN_HEADER..]);
    let udp_checksum = if udp_checksum == 0 {
        0xffff
    } else {
        udp_checksum
    };
    pkt[IPV4_MIN_HEADER + 6..IPV4_MIN_HEADER + 8].copy_from_slice(&udp_checksum.to_be_bytes());

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
    pseudo.push(IP_PROTOCOL_UDP);
    pseudo.extend_from_slice(&(udp_len as u16).to_be_bytes());
    pseudo.extend_from_slice(udp_and_payload);
    internet_checksum(&pseudo)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ipnet::{IpNet, Ipv4Net};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    /// Minimal `Endpoint` impl that lets the test push fake decapsulated
    /// IP packets into the adapter's demux path and observe what the
    /// adapter sends out the encrypt channel.
    struct FakeEndpoint {
        id: String,
        encrypt_tx: mpsc::Sender<Bytes>,
        ip_recv_rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
        ip_local_recv_rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
        interface: IpNet,
        started: AtomicBool,
    }

    impl hammer_core::lifecycle::Lifecycle for FakeEndpoint {
        fn name(&self) -> &str {
            "fake-endpoint"
        }
        fn start(&self, _stage: hammer_core::lifecycle::StartStage) -> CoreResult<()> {
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
            if self.started.load(Ordering::SeqCst) {
                self.encrypt_tx.clone()
            } else {
                mpsc::channel::<Bytes>(1).0
            }
        }
        fn ip_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
            self.ip_recv_rx.lock().expect("ip_recv_rx mutex").take()
        }
        fn ip_local_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
            self.ip_local_recv_rx
                .lock()
                .expect("ip_local_recv_rx mutex")
                .take()
        }
        fn interface_addresses(&self) -> Vec<IpNet> {
            vec![self.interface]
        }
    }

    fn fake_endpoint(
        id: &str,
        interface: Ipv4Addr,
    ) -> (
        Arc<FakeEndpoint>,
        mpsc::Receiver<Bytes>, // egress tap
        mpsc::Sender<Bytes>,   // ingress driver
    ) {
        let (encrypt_tx, encrypt_rx) = mpsc::channel::<Bytes>(16);
        let (_default_tx, default_rx) = mpsc::channel::<Bytes>(1);
        let (local_tx, local_rx) = mpsc::channel::<Bytes>(16);
        let ep = Arc::new(FakeEndpoint {
            id: id.to_owned(),
            encrypt_tx,
            ip_recv_rx: Mutex::new(Some(default_rx)),
            ip_local_recv_rx: Mutex::new(Some(local_rx)),
            interface: IpNet::V4(Ipv4Net::new(interface, 32).unwrap()),
            started: AtomicBool::new(true),
        });
        (ep, encrypt_rx, local_tx)
    }

    fn fake_endpoint_not_started(
        id: &str,
        interface: Ipv4Addr,
    ) -> (
        Arc<FakeEndpoint>,
        mpsc::Receiver<Bytes>,
        mpsc::Sender<Bytes>,
    ) {
        let (ep, encrypt_rx, local_tx) = fake_endpoint(id, interface);
        ep.started.store(false, Ordering::SeqCst);
        (ep, encrypt_rx, local_tx)
    }

    fn test_logger() -> Logger {
        use hammer_core::log::{DiscardWriter, Factory};
        use std::time::Instant;
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger("adapter-test")
    }

    fn test_runtime() -> DataPlaneRuntime {
        DataPlaneRuntime::with_buffer_capacity(2048, 64)
    }

    struct TestDatagram {
        destination: SocksAddr,
        payload: Vec<u8>,
    }

    async fn send_packet(
        conn: &mut dyn ProxyPacketConn,
        runtime: &DataPlaneRuntime,
        destination: SocksAddr,
        payload: &[u8],
    ) -> CoreResult<()> {
        let mut metadata = RouteMetadata::default();
        metadata.destination = Some(destination);
        let mut frame = runtime.alloc_pooled_frame()?;
        let index = runtime.alloc_index_with_bytes(metadata, payload)?;
        if let Err(err) = frame.push_index(index) {
            runtime.free_index(index);
            let _ = runtime.release_pooled_frame(frame);
            return Err(err);
        }
        let result = conn.send(runtime, &mut frame).await;
        runtime.release_pooled_frame(frame)?;
        result
    }

    async fn recv_packet(
        conn: &mut dyn ProxyPacketConn,
        runtime: &DataPlaneRuntime,
    ) -> CoreResult<TestDatagram> {
        let mut frame = runtime.alloc_pooled_frame()?;
        if let Err(err) = conn.recv(runtime, &mut frame, 1).await {
            let _ = runtime.release_pooled_frame(frame);
            return Err(err);
        }
        let Some(index) = frame.drain_indices().next() else {
            runtime.release_pooled_frame(frame)?;
            return Err(CoreError::internal("test packet recv returned empty frame"));
        };
        let metadata = match runtime.metadata(index) {
            Ok(metadata) => metadata,
            Err(err) => {
                runtime.free_index(index);
                let _ = runtime.release_pooled_frame(frame);
                return Err(err);
            }
        };
        let payload = match runtime.copy_current_chain(index) {
            Ok(payload) => payload,
            Err(err) => {
                runtime.free_index(index);
                let _ = runtime.release_pooled_frame(frame);
                return Err(err);
            }
        };
        runtime.free_index(index);
        runtime.release_pooled_frame(frame)?;
        let destination = metadata
            .destination
            .ok_or_else(|| CoreError::internal("test packet recv missing destination"))?;
        Ok(TestDatagram {
            destination,
            payload,
        })
    }

    #[test]
    fn parse_inner_ipv4_udp_extracts_dst_port_and_payload() {
        let pkt = build_ipv4_udp(
            Ipv4Addr::new(8, 8, 8, 8),
            53,
            Ipv4Addr::new(10, 0, 0, 2),
            40_000,
            b"\x00\x01response",
        );
        let (dst_port, datagram) = parse_inner_ipv4_udp(&Bytes::from(pkt)).expect("parse");
        assert_eq!(dst_port, 40_000);
        assert_eq!(datagram.destination.port, 53);
        assert_eq!(
            datagram.destination.host,
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))
        );
        assert_eq!(&datagram.payload[..], b"\x00\x01response");
    }

    #[test]
    fn parse_inner_ipv4_udp_rejects_non_udp() {
        // TCP protocol
        let mut pkt = build_ipv4_udp(
            Ipv4Addr::new(1, 2, 3, 4),
            1,
            Ipv4Addr::new(5, 6, 7, 8),
            2,
            b"x",
        );
        pkt[9] = 6; // TCP
        assert!(parse_inner_ipv4_udp(&Bytes::from(pkt)).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn listen_packet_send_round_trip() {
        let (ep, mut egress_rx, local_tx) = fake_endpoint("wg-test", Ipv4Addr::new(10, 0, 0, 2));
        let adapter =
            EndpointOutboundAdapter::arc(test_logger(), "wg-test".into(), ep as Arc<dyn Endpoint>);

        // Send a query and observe the packet on the egress side.
        let mut conn = adapter
            .clone()
            .listen_packet()
            .await
            .expect("listen_packet");
        let server = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53);
        let runtime = test_runtime();
        send_packet(&mut *conn, &runtime, server.clone(), b"\x00\x01query")
            .await
            .expect("send packet");

        let outbound = egress_rx.recv().await.expect("egress packet");
        // Parse our own packet to verify src/dst — the egress side carries
        // the adapter's ephemeral port in src_port (parse_inner_ipv4_udp
        // returns dst_port for the ingress case, so we read src_port out
        // directly here).
        let src_port = u16::from_be_bytes([outbound[20], outbound[21]]);
        let dst_port = u16::from_be_bytes([outbound[22], outbound[23]]);
        assert!(src_port >= EPHEMERAL_MIN, "src_port = {src_port}");
        assert_eq!(dst_port, 53, "dst_port should be DNS server port");
        let local_port = src_port;
        assert_eq!(&outbound[28..], b"\x00\x01query");

        // Craft a response packet (server → adapter source) and push it
        // through the local-recv channel.
        let response = build_ipv4_udp(
            Ipv4Addr::new(1, 1, 1, 1),
            53,
            Ipv4Addr::new(10, 0, 0, 2),
            local_port,
            b"\x00\x01response",
        );
        local_tx
            .send(Bytes::from(response))
            .await
            .expect("local_tx send");

        let datagram = recv_packet(&mut *conn, &runtime)
            .await
            .expect("recv packet");
        assert_eq!(datagram.payload.as_slice(), b"\x00\x01response");
        assert_eq!(datagram.destination.port, 53);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn listen_packet_fetches_encrypt_sender_after_endpoint_start() {
        let (ep, mut egress_rx, _local_tx) =
            fake_endpoint_not_started("wg-lazy", Ipv4Addr::new(10, 0, 0, 2));
        let adapter = EndpointOutboundAdapter::arc(
            test_logger(),
            "wg-lazy".into(),
            ep.clone() as Arc<dyn Endpoint>,
        );
        ep.started.store(true, Ordering::SeqCst);

        let mut conn = adapter.listen_packet().await.expect("listen_packet");
        let server = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53);
        let runtime = test_runtime();
        send_packet(&mut *conn, &runtime, server, b"\x00\x01query")
            .await
            .expect("send packet should use live endpoint sender");

        let outbound = egress_rx.recv().await.expect("egress packet");
        assert_eq!(&outbound[28..], b"\x00\x01query");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drop_conn_decrements_flow_count() {
        let (ep, _egress_rx, _local_tx) = fake_endpoint("wg-drop", Ipv4Addr::new(10, 0, 0, 2));
        let adapter =
            EndpointOutboundAdapter::arc(test_logger(), "wg-drop".into(), ep as Arc<dyn Endpoint>);

        let conn = adapter
            .clone()
            .listen_packet()
            .await
            .expect("listen_packet");
        assert_eq!(adapter.udp_flow_count.load(Ordering::Relaxed), 1);
        drop(conn);
        assert_eq!(adapter.udp_flow_count.load(Ordering::Relaxed), 0);
        assert!(adapter.udp_flows.lock().unwrap().by_port.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn listen_packet_reserves_unique_ports_across_wraparound() {
        let (ep, _egress_rx, _local_tx) = fake_endpoint("wg-wrap", Ipv4Addr::new(10, 0, 0, 2));
        let adapter =
            EndpointOutboundAdapter::arc(test_logger(), "wg-wrap".into(), ep as Arc<dyn Endpoint>);
        adapter.next_port.store(EPHEMERAL_MAX, Ordering::Relaxed);

        let _max = adapter
            .clone()
            .listen_packet()
            .await
            .expect("reserve max port");
        let _min = adapter
            .clone()
            .listen_packet()
            .await
            .expect("reserve wrapped min port");
        adapter.next_port.store(0, Ordering::Relaxed);
        let _next = adapter
            .clone()
            .listen_packet()
            .await
            .expect("reserve after below-min wrap");

        let flows = adapter.udp_flows.lock().expect("UdpFlowMap poisoned");
        assert_eq!(flows.by_port.len(), 3);
        assert!(flows.by_port.contains_key(&EPHEMERAL_MAX));
        assert!(flows.by_port.contains_key(&EPHEMERAL_MIN));
        assert!(flows.by_port.contains_key(&(EPHEMERAL_MIN + 1)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dial_udp_returns_descriptive_err() {
        // Dial(Udp) is explicitly not supported — DNS UDP uses
        // listen_packet — so we lock in the diagnostic.
        let (ep, _egress_rx, _local_tx) = fake_endpoint("wg-udp-dial", Ipv4Addr::new(10, 0, 0, 2));
        let adapter = EndpointOutboundAdapter::arc(
            test_logger(),
            "wg-udp-dial".into(),
            ep as Arc<dyn Endpoint>,
        );
        let err = match adapter
            .dial(
                Network::Udp,
                SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53),
                &[],
            )
            .await
        {
            Ok(_) => panic!("UDP dial should never succeed via endpoint adapter"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("listen_packet"), "got {err}");
    }
}
