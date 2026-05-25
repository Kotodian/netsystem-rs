//! WireGuard endpoint.
//!
//! Owns the boringtun + UDP transport actor and exposes a pure L3 surface
//! (`Endpoint::ip_send_clone` / `Endpoint::ip_recv_take`) so the TUN packet
//! loop can forward raw IP packets without any L4 reassembly. The old
//! double user-space-IP-stack data path is gone: WG sits next to outbounds,
//! not under them.
#![allow(dead_code)]

use arc_swap::ArcSwapOption;
use boringtun::x25519;
use bytes::Bytes;
use ipnet::IpNet;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

use hammer_adapter::{Endpoint, EndpointLocalFlow, Lifecycle, Network, PlatformInterface};
use hammer_core::config::{Endpoint as EndpointOptions, EndpointKind, WireguardEndpointOptions};
#[cfg(test)]
use hammer_core::error::HammerError;
use hammer_core::error::HammerResult;
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;
use hammer_core::protocol::wireguard::WIREGUARD_OVERHEAD;
#[cfg(feature = "endpoint-amneziawg")]
use hammer_core::protocol::wireguard::amnezia2::Amnezia2Options;
use hammer_core::protocol::wireguard::peer::{self, Peer};

#[cfg(feature = "endpoint-amneziawg")]
use super::amnezia2::to_boringtun_config;
use super::transport::{self, TransportHandles};
use crate::protocol::endpoint::EndpointRuntimeOptions;
use crate::socket_protector::SocketProtector;
use crate::ControlLogWriter;

/// L3 WireGuard endpoint. `Tunn` state machines and the UDP transport actor
/// are inert until lifecycle reaches `Start`; before that, `ip_send_clone`
/// hands out a closed sender and `ip_recv_take` returns `None`.
#[hammer_component_macros::hammer_component(
    endpoint,
    name = "wireguard",
    builder = build_endpoint_views,
    metrics = ("endpoint", "wireguard")
)]
pub struct WireguardEndpoint {
    logger: Logger,
    id: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    mtu: u32,
    listen_port: u16,
    addresses: Vec<IpNet>,
    #[cfg(feature = "endpoint-amneziawg")]
    amnezia: Option<Amnezia2Options>,
    peers: Arc<Vec<Peer>>,
    protector: SocketProtector,
    control_log: Option<Arc<ControlLogWriter>>,
    local_flows: Arc<LocalFlowTable>,
    inner: Mutex<WireguardState>,
    #[cfg(test)]
    fail_next_start: AtomicBool,
}

struct WireguardRuntime {
    transport: TransportHandles,
    /// Cloneable hot-path sender so `ip_send_clone` doesn't have to mutex-walk
    /// for every packet.
    encrypt_tx: mpsc::Sender<Bytes>,
    encrypt_batch_tx: mpsc::Sender<Vec<Bytes>>,
    inbound: InboundFanout,
}

/// Demuxes decapsulated IP packets from the boringtun transport into two
/// consumers: the TUN packet loop (default channel) and an opt-in local
/// consumer such as `EndpointOutboundAdapter` (local channel). Packets
/// matching a registered adapter-local flow are consumed by the local
/// channel instead of being mirrored into TUN.
///
/// Both receivers are take-once and live behind a `Mutex` because endpoint
/// startup runs on the control thread while consumers usually run on the
/// data plane. The forwarder task (spawned in `start_runtime`) checks
/// `local_tx_slot` per packet; until someone calls `take_local`, the slot
/// is `None` and the default channel pays zero local-flow lookup cost.
struct InboundFanout {
    default_rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
    default_batch_rx: Mutex<Option<mpsc::Receiver<Vec<Bytes>>>>,
    local_rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
    local_tx_slot: Arc<ArcSwapOption<mpsc::Sender<Bytes>>>,
    /// Held so `take_local` can publish a sender clone into `local_tx_slot`
    /// without re-allocating the channel.
    local_tx_template: mpsc::Sender<Bytes>,
}

#[derive(Default)]
struct LocalFlowTable {
    flows: Mutex<HashSet<EndpointLocalFlow>>,
}

impl LocalFlowTable {
    fn insert(&self, flow: EndpointLocalFlow) {
        self.flows
            .lock()
            .expect("local flow table poisoned")
            .insert(flow);
    }

    fn remove(&self, flow: EndpointLocalFlow) {
        self.flows
            .lock()
            .expect("local flow table poisoned")
            .remove(&flow);
    }

    fn matches_inbound(&self, pkt: &[u8]) -> bool {
        let Some(flow) = parse_local_inbound_flow(pkt) else {
            return false;
        };
        self.flows
            .lock()
            .expect("local flow table poisoned")
            .contains(&flow)
    }
}

impl InboundFanout {
    fn take_default(&self) -> Option<mpsc::Receiver<Bytes>> {
        self.default_rx
            .lock()
            .expect("default_rx mutex poisoned")
            .take()
    }

    fn take_default_batch(&self) -> Option<mpsc::Receiver<Vec<Bytes>>> {
        self.default_batch_rx
            .lock()
            .expect("default_batch_rx mutex poisoned")
            .take()
    }

    fn take_local(&self) -> Option<mpsc::Receiver<Bytes>> {
        let rx = self
            .local_rx
            .lock()
            .expect("local_rx mutex poisoned")
            .take()?;
        // Activating fan-out: the forwarder task will pick this up via
        // `ArcSwapOption::load` from the next packet onward.
        self.local_tx_slot
            .store(Some(Arc::new(self.local_tx_template.clone())));
        Some(rx)
    }
}

/// Default-channel buffer between the boringtun decapsulator and the TUN
/// packet loop. Mirrors `transport::INBOUND_QUEUE` so the forwarder task
/// doesn't impose a tighter bound than the upstream pipeline.
const DEFAULT_RECV_QUEUE: usize = 256;
/// Local-channel buffer for opt-in consumers (DNS roundtrip today). 64
/// leaves headroom against transient scheduler stalls without ballooning
/// resident memory on NetExt.
const LOCAL_RECV_QUEUE: usize = 64;

enum WireguardState {
    Idle,
    Running(WireguardRuntime),
    Closed,
}

impl WireguardEndpoint {
    fn new(
        logger: Logger,
        options: EndpointRuntimeOptions<WireguardEndpointOptions>,
        protector: SocketProtector,
        control_log: Option<Arc<ControlLogWriter>>,
    ) -> Self {
        let EndpointRuntimeOptions {
            id,
            interface,
            protocol: options,
        } = options;
        let private_key = x25519::StaticSecret::from(options.private_key);
        #[cfg(feature = "endpoint-amneziawg")]
        let amnezia = options.amnezia.as_ref().map(to_boringtun_config);
        #[cfg(feature = "endpoint-amneziawg")]
        let runtime_amnezia = options.amnezia.clone();
        let peers: Vec<Peer> = options
            .peers
            .into_iter()
            .enumerate()
            .map(|(idx, peer_opts)| {
                Peer::new(
                    peer_opts,
                    &private_key,
                    idx as u32,
                    #[cfg(feature = "endpoint-amneziawg")]
                    amnezia.clone(),
                )
            })
            .collect();
        Self {
            logger,
            id,
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
            mtu: interface.mtu,
            listen_port: options.listen_port,
            addresses: interface.address,
            #[cfg(feature = "endpoint-amneziawg")]
            amnezia: runtime_amnezia,
            peers: Arc::new(peers),
            protector,
            control_log,
            local_flows: Arc::new(LocalFlowTable::default()),
            inner: Mutex::new(WireguardState::Idle),
            #[cfg(test)]
            fail_next_start: AtomicBool::new(false),
        }
    }

    pub(crate) fn mtu(&self) -> u32 {
        self.mtu
    }

    pub(crate) fn route_outbound(&self, dst: IpAddr) -> Option<&Peer> {
        peer::route_outbound(&self.peers, dst).map(|idx| &self.peers[idx])
    }

    pub(crate) fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub(crate) fn encapsulate_buffer(&self) -> Vec<u8> {
        vec![0u8; self.mtu as usize + WIREGUARD_OVERHEAD]
    }

    pub(crate) fn addresses(&self) -> &[IpNet] {
        &self.addresses
    }

    fn boot(&self) -> HammerResult<()> {
        {
            let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
            if matches!(*inner, WireguardState::Running(_) | WireguardState::Closed) {
                return Ok(());
            }
        }

        let runtime = self.start_runtime()?;
        let mut inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        if matches!(*inner, WireguardState::Idle) {
            *inner = WireguardState::Running(runtime);
        } else {
            stop_runtime(runtime);
        }
        Ok(())
    }

    fn start_runtime(&self) -> HammerResult<WireguardRuntime> {
        #[cfg(test)]
        if self.fail_next_start.swap(false, Ordering::SeqCst) {
            return Err(HammerError::internal("injected wireguard start failure"));
        }

        let transport = transport::spawn_transport(
            self.logger.clone(),
            Arc::clone(&self.peers),
            self.listen_port,
            self.mtu,
            self.protector.clone(),
            self.control_log.as_ref().map(Arc::clone),
            #[cfg(feature = "endpoint-amneziawg")]
            self.amnezia.clone(),
        )?;

        let encrypt_tx_clone = transport.encrypt_tx.clone();
        let encrypt_batch_tx_clone = transport.encrypt_batch_tx.clone();
        let TransportHandles {
            encrypt_tx,
            encrypt_batch_tx,
            inbound_rx,
            inbound_batch_rx,
            local_addr,
            shutdown,
            reset_tx,
            keepalive_timer,
            join,
        } = transport;

        // Two-channel fan-out between the boringtun decapsulator and the
        // consumers. Default channel feeds the TUN packet loop; local
        // channel is opt-in (EndpointOutboundAdapter) and stays cold via
        // `ArcSwapOption::None` until someone calls `take_local`.
        let (default_tx, default_rx) = mpsc::channel::<Bytes>(DEFAULT_RECV_QUEUE);
        let (default_batch_tx, default_batch_rx) =
            mpsc::channel::<Vec<Bytes>>(transport::INBOUND_BATCH_QUEUE);
        let (local_tx_template, local_rx) = mpsc::channel::<Bytes>(LOCAL_RECV_QUEUE);
        let local_tx_slot: Arc<ArcSwapOption<mpsc::Sender<Bytes>>> =
            Arc::new(ArcSwapOption::empty());
        crate::spawn::spawn(run_inbound_forwarder(
            self.logger.clone(),
            inbound_rx,
            inbound_batch_rx,
            default_tx,
            default_batch_tx,
            Arc::clone(&local_tx_slot),
            Arc::clone(&self.local_flows),
        ));

        Ok(WireguardRuntime {
            transport: TransportHandles {
                encrypt_tx,
                encrypt_batch_tx,
                // Sentinel — the live receiver has moved into the forwarder.
                inbound_rx: mpsc::channel::<Bytes>(1).1,
                inbound_batch_rx: mpsc::channel::<Vec<Bytes>>(1).1,
                local_addr,
                shutdown,
                reset_tx,
                keepalive_timer,
                join,
            },
            encrypt_tx: encrypt_tx_clone,
            encrypt_batch_tx: encrypt_batch_tx_clone,
            inbound: InboundFanout {
                default_rx: Mutex::new(Some(default_rx)),
                default_batch_rx: Mutex::new(Some(default_batch_rx)),
                local_rx: Mutex::new(Some(local_rx)),
                local_tx_slot,
                local_tx_template,
            },
        })
    }

    fn shutdown(&self) {
        let runtime = {
            let mut inner = self.inner.lock().expect("WireguardEndpoint poisoned");
            match std::mem::replace(&mut *inner, WireguardState::Closed) {
                WireguardState::Running(rt) => Some(rt),
                _ => None,
            }
        };
        if let Some(rt) = runtime {
            stop_runtime(rt);
        }
    }

    fn restart(&self) {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        if let WireguardState::Running(rt) = &*inner {
            rt.transport.reset();
        }
    }

    #[cfg(test)]
    fn fail_next_start_for_test(&self) {
        self.fail_next_start.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn is_running(&self) -> bool {
        matches!(
            *self.inner.lock().expect("WireguardEndpoint poisoned"),
            WireguardState::Running(_)
        )
    }
}

fn stop_runtime(rt: WireguardRuntime) {
    rt.transport.cancel_keepalive_timer();
    let _ = rt.transport.shutdown.send(());
    rt.transport.join.abort();
    drop(rt.encrypt_tx);
    drop(rt.encrypt_batch_tx);
    // Drops both fan-out channels; the forwarder task exits when its
    // upstream `inbound_rx` (owned by the transport actor we just told to
    // shut down) closes.
    drop(rt.inbound);
}

/// Pulls decapsulated IP packets out of the boringtun transport and demuxes
/// them onto two consumer channels. The default channel is always wired
/// (TUN packet loop); the local channel is gated on `local_tx_slot`, which
/// stays `None` until an `EndpointOutboundAdapter` takes its receiver.
async fn run_inbound_forwarder(
    logger: Logger,
    mut inbound_rx: mpsc::Receiver<Bytes>,
    mut inbound_batch_rx: mpsc::Receiver<Vec<Bytes>>,
    default_tx: mpsc::Sender<Bytes>,
    default_batch_tx: mpsc::Sender<Vec<Bytes>>,
    local_tx_slot: Arc<ArcSwapOption<mpsc::Sender<Bytes>>>,
    local_flows: Arc<LocalFlowTable>,
) {
    loop {
        tokio::select! {
            packet = inbound_rx.recv() => {
                let Some(packet) = packet else {
                    break;
                };
                forward_inbound_packets(
                    &logger,
                    vec![packet],
                    &default_tx,
                    &default_batch_tx,
                    &local_tx_slot,
                    &local_flows,
                )
                .await;
            }
            batch = inbound_batch_rx.recv() => {
                let Some(batch) = batch else {
                    break;
                };
                forward_inbound_packets(
                    &logger,
                    batch,
                    &default_tx,
                    &default_batch_tx,
                    &local_tx_slot,
                    &local_flows,
                )
                .await;
            }
        }
    }
}

async fn forward_inbound_packets(
    logger: &Logger,
    packets: Vec<Bytes>,
    default_tx: &mpsc::Sender<Bytes>,
    default_batch_tx: &mpsc::Sender<Vec<Bytes>>,
    local_tx_slot: &Arc<ArcSwapOption<mpsc::Sender<Bytes>>>,
    local_flows: &LocalFlowTable,
) {
    let mut default_batch = Vec::with_capacity(packets.len());
    for pkt in packets {
        if let Some(local_tx) = local_tx_slot.load().as_ref()
            && local_flows.matches_inbound(&pkt)
        {
            let _ = local_tx.try_send(pkt);
        } else {
            default_batch.push(pkt);
        }
    }
    if default_batch.is_empty() {
        return;
    }
    if default_batch.len() == 1 {
        match default_tx.try_send(default_batch.pop().expect("one packet")) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                logger.debug("wg inbound forwarder: TUN receiver full; dropped packet");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                logger.debug("wg inbound forwarder: TUN receiver dropped");
            }
        }
        return;
    }
    match default_batch_tx.try_send(default_batch) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            logger.debug("wg inbound forwarder: TUN batch receiver full; dropped batch");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            logger.debug("wg inbound forwarder: TUN batch receiver dropped");
        }
    }
}

fn parse_local_inbound_flow(pkt: &[u8]) -> Option<EndpointLocalFlow> {
    if pkt.len() < 20 || (pkt[0] >> 4) != 4 {
        return None;
    }
    let ihl = ((pkt[0] & 0x0f) as usize) * 4;
    if ihl < 20 || pkt.len() < ihl + 4 {
        return None;
    }
    let fragment = u16::from_be_bytes([pkt[6], pkt[7]]);
    if fragment & 0x1fff != 0 {
        return None;
    }
    let network = match pkt[9] {
        6 => Network::Tcp,
        17 => Network::Udp,
        _ => return None,
    };
    let remote_ip = IpAddr::V4(Ipv4Addr::new(pkt[12], pkt[13], pkt[14], pkt[15]));
    let local_ip = IpAddr::V4(Ipv4Addr::new(pkt[16], pkt[17], pkt[18], pkt[19]));
    let remote_port = u16::from_be_bytes([pkt[ihl], pkt[ihl + 1]]);
    let local_port = u16::from_be_bytes([pkt[ihl + 2], pkt[ihl + 3]]);
    Some(EndpointLocalFlow {
        network,
        local: SocketAddr::new(local_ip, local_port),
        remote: SocketAddr::new(remote_ip, remote_port),
    })
}

impl Lifecycle for WireguardEndpoint {
    fn name(&self) -> &str {
        "wireguard-endpoint"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        if matches!(stage, StartStage::Start) {
            self.boot()?;
        }
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        self.shutdown();
        Ok(())
    }
}

impl Endpoint for WireguardEndpoint {
    fn id(&self) -> &str {
        &self.id
    }

    fn ip_send_clone(&self) -> mpsc::Sender<Bytes> {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        match &*inner {
            WireguardState::Running(rt) => rt.encrypt_tx.clone(),
            // Endpoint not started yet — hand out a sentinel sender whose
            // receiver is immediately dropped, so `try_send` reports the
            // channel as closed. The TUN packet loop only resolves a route
            // to this endpoint after lifecycle Start has completed, so this
            // branch is reachable only in tests / mid-restart races.
            WireguardState::Idle | WireguardState::Closed => mpsc::channel::<Bytes>(1).0,
        }
    }

    fn ip_send_batch_clone(&self) -> Option<mpsc::Sender<Vec<Bytes>>> {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        match &*inner {
            WireguardState::Running(rt) => Some(rt.encrypt_batch_tx.clone()),
            WireguardState::Idle | WireguardState::Closed => None,
        }
    }

    fn ip_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        match &*inner {
            WireguardState::Running(rt) => rt.inbound.take_default(),
            _ => None,
        }
    }

    fn ip_recv_batch_take(&self) -> Option<mpsc::Receiver<Vec<Bytes>>> {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        match &*inner {
            WireguardState::Running(rt) => rt.inbound.take_default_batch(),
            _ => None,
        }
    }

    fn ip_local_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        match &*inner {
            WireguardState::Running(rt) => rt.inbound.take_local(),
            _ => None,
        }
    }

    fn register_local_flow(&self, flow: EndpointLocalFlow) {
        self.local_flows.insert(flow);
    }

    fn unregister_local_flow(&self, flow: EndpointLocalFlow) {
        self.local_flows.remove(flow);
    }

    fn ip_packet_mtu(&self) -> Option<usize> {
        Some(self.mtu as usize)
    }

    fn allowed_destinations(&self) -> Vec<IpNet> {
        // Union of every peer's allowed_ips — the L3 fast path uses this to
        // build a longest-prefix table mapping packet dst IP to the WG
        // encrypt channel. Sorted longest-prefix-first so consumers doing a
        // linear walk see the most specific match first.
        let mut nets: Vec<IpNet> = self
            .peers
            .iter()
            .flat_map(|p| p.allowed_ips().iter().copied())
            .collect();
        nets.sort_by(|a, b| b.prefix_len().cmp(&a.prefix_len()));
        nets.dedup();
        nets
    }

    fn interface_addresses(&self) -> Vec<IpNet> {
        self.addresses.clone()
    }

    fn reset(&self) {
        self.restart();
    }
}

#[cfg(test)]
fn endpoint_for_test(
    logger: Logger,
    id: String,
    interface: hammer_core::config::EndpointInterfaceOptions,
    options: WireguardEndpointOptions,
) -> WireguardEndpoint {
    WireguardEndpoint::new(
        logger,
        EndpointRuntimeOptions {
            id,
            interface,
            protocol: options,
        },
        SocketProtector::default(),
        None,
    )
}

pub(crate) fn build_with_platform(
    logger: Logger,
    options: EndpointRuntimeOptions<WireguardEndpointOptions>,
    platform: Option<Arc<dyn PlatformInterface>>,
    control_log: Option<Arc<ControlLogWriter>>,
) -> WireguardEndpoint {
    let protector = SocketProtector::from(platform);
    WireguardEndpoint::new(logger, options, protector, control_log)
}

pub(crate) fn build_endpoint_views(
    logger: Logger,
    option: &EndpointOptions,
    platform: Option<Arc<dyn PlatformInterface>>,
    control_log: Option<Arc<ControlLogWriter>>,
) -> HammerResult<Arc<WireguardEndpoint>> {
    match &option.kind {
        EndpointKind::Wireguard(options) => Ok(Arc::new(build_with_platform(
            logger,
            EndpointRuntimeOptions::from_endpoint(option, options.clone()),
            platform,
            control_log,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Instant;

    use boringtun::noise::TunnResult;
    use hammer_core::config::{
        EndpointInterfaceOptions, WireguardEndpointOptions, WireguardPeerOptions,
    };
    use hammer_core::log::{DiscardWriter, Factory};

    fn logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    fn x25519_public(secret: [u8; 32]) -> [u8; 32] {
        x25519::PublicKey::from(&x25519::StaticSecret::from(secret)).to_bytes()
    }

    fn test_interface(address: &str) -> EndpointInterfaceOptions {
        EndpointInterfaceOptions {
            mtu: 1408,
            address: vec![address.parse().unwrap()],
        }
    }

    fn make_endpoint(
        name: &'static str,
        my_priv: [u8; 32],
        peer_pub: [u8; 32],
    ) -> WireguardEndpoint {
        let options = WireguardEndpointOptions {
            private_key: my_priv,
            listen_port: 0,
            #[cfg(feature = "endpoint-amneziawg")]
            amnezia: None,
            peers: vec![WireguardPeerOptions {
                public_key: peer_pub,
                pre_shared_key: None,
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 51820),
                allowed_ips: vec!["0.0.0.0/0".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };
        endpoint_for_test(
            logger(name),
            name.to_owned(),
            test_interface("10.66.0.2/32"),
            options,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_keeps_existing_runtime_and_sender() {
        let a_priv = [3u8; 32];
        let b_pub = x25519_public([4u8; 32]);
        let endpoint = make_endpoint("wg-restart", a_priv, b_pub);
        endpoint.boot().expect("boot endpoint");
        assert!(endpoint.is_running());

        let before = endpoint.ip_send_clone();
        endpoint.restart();

        assert!(endpoint.is_running(), "reset must keep the endpoint live");
        before
            .try_send(Bytes::from_static(b"stable"))
            .expect("pre-reset sender must remain attached");
        endpoint.shutdown();
    }

    /// Two `WireguardEndpoint`s configured as each other's peer must complete
    /// the noise handshake, after which an IP packet sent through one comes
    /// out byte-for-byte at the other. This is the smoke test that proves the
    /// boringtun + Peer wiring is correct independently of any data path.
    #[test]
    fn boringtun_round_trip_recovers_ip_packet() {
        let a_priv = [1u8; 32];
        let b_priv = [2u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let a = make_endpoint("a", a_priv, b_pub);
        let b = make_endpoint("b", b_priv, a_pub);
        let a_peer = &a.peers()[0];
        let b_peer = &b.peers()[0];

        let mut buf1 = vec![0u8; 2048];
        let init = match a_peer.lock_tunn().encapsulate(&[], &mut buf1) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("A: expected handshake_init, got {other:?}"),
        };

        let mut buf2 = vec![0u8; 2048];
        let response = match b_peer.lock_tunn().decapsulate(None, &init, &mut buf2) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("B: expected handshake_response, got {other:?}"),
        };

        let mut buf3 = vec![0u8; 2048];
        match a_peer.lock_tunn().decapsulate(None, &response, &mut buf3) {
            TunnResult::Done | TunnResult::WriteToNetwork(_) => {}
            other => panic!("A: handshake_response result {other:?}"),
        }

        let mut ip_packet = vec![0u8; 60];
        ip_packet[0] = 0x45;
        ip_packet[3] = 60;

        let mut enc_buf = vec![0u8; 2048];
        let encrypted = match a_peer.lock_tunn().encapsulate(&ip_packet, &mut enc_buf) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("A: encapsulate {other:?}"),
        };

        let mut dec_buf = vec![0u8; 2048];
        match b_peer
            .lock_tunn()
            .decapsulate(None, &encrypted, &mut dec_buf)
        {
            TunnResult::WriteToTunnelV4(out, _src) => assert_eq!(out, ip_packet),
            other => panic!("B: decapsulate {other:?}"),
        }
    }

    #[test]
    fn route_outbound_picks_longest_prefix_peer() {
        let local_priv = [3u8; 32];
        let peer1_pub = x25519_public([4u8; 32]);
        let peer2_pub = x25519_public([5u8; 32]);
        let opts = WireguardEndpointOptions {
            private_key: local_priv,
            listen_port: 0,
            #[cfg(feature = "endpoint-amneziawg")]
            amnezia: None,
            peers: vec![
                WireguardPeerOptions {
                    public_key: peer1_pub,
                    pre_shared_key: None,
                    endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 51820),
                    allowed_ips: vec!["10.0.0.0/8".parse().unwrap()],
                    persistent_keepalive: None,
                    reserved: [0; 3],
                },
                WireguardPeerOptions {
                    public_key: peer2_pub,
                    pre_shared_key: None,
                    endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)), 51820),
                    allowed_ips: vec!["10.66.0.0/16".parse().unwrap()],
                    persistent_keepalive: None,
                    reserved: [0; 3],
                },
            ],
        };
        let ep = endpoint_for_test(
            logger("multi"),
            "multi".to_owned(),
            test_interface("10.66.0.2/32"),
            opts,
        );

        let chosen = ep
            .route_outbound(IpAddr::V4(Ipv4Addr::new(10, 66, 0, 5)))
            .expect("must route to specific peer");
        assert_eq!(chosen.endpoint().to_string(), "2.2.2.2:51820");

        let chosen = ep
            .route_outbound(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)))
            .expect("must route to wider peer");
        assert_eq!(chosen.endpoint().to_string(), "1.1.1.1:51820");

        assert!(
            ep.route_outbound(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)))
                .is_none(),
            "no peer must match an out-of-range destination"
        );
    }

    #[test]
    fn encapsulate_buffer_sized_for_mtu_plus_overhead() {
        let priv_key = [9u8; 32];
        let peer_pub = x25519_public([10u8; 32]);
        let opts = WireguardEndpointOptions {
            private_key: priv_key,
            listen_port: 0,
            #[cfg(feature = "endpoint-amneziawg")]
            amnezia: None,
            peers: vec![WireguardPeerOptions {
                public_key: peer_pub,
                pre_shared_key: None,
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 51820),
                allowed_ips: vec!["0.0.0.0/0".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [1, 2, 3],
            }],
        };
        let ep = endpoint_for_test(
            logger("buf"),
            "buf".to_owned(),
            test_interface("10.66.0.2/32"),
            opts,
        );
        assert_eq!(
            ep.encapsulate_buffer().len(),
            ep.mtu() as usize + WIREGUARD_OVERHEAD
        );
        assert_eq!(ep.peers()[0].reserved(), [1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inbound_forwarder_lazy_enables_fanout_only_after_take_local() {
        use std::time::Duration;
        use tokio::time::timeout;

        // Wire the fan-out plumbing manually so the test exercises only the
        // forwarder + `InboundFanout` contract, independent of boringtun.
        let (transport_tx, transport_rx) = mpsc::channel::<Bytes>(8);
        let (_transport_batch_tx, transport_batch_rx) = mpsc::channel::<Vec<Bytes>>(8);
        let (default_tx, mut default_rx) = mpsc::channel::<Bytes>(8);
        let (default_batch_tx, _default_batch_rx) = mpsc::channel::<Vec<Bytes>>(8);
        let (local_tx_template, local_rx) = mpsc::channel::<Bytes>(8);
        let local_tx_slot: Arc<ArcSwapOption<mpsc::Sender<Bytes>>> =
            Arc::new(ArcSwapOption::empty());
        let local_flows = Arc::new(LocalFlowTable::default());

        let fanout = InboundFanout {
            default_rx: Mutex::new(Some(mpsc::channel::<Bytes>(1).1)), // unused for this test
            default_batch_rx: Mutex::new(Some(mpsc::channel::<Vec<Bytes>>(1).1)),
            local_rx: Mutex::new(Some(local_rx)),
            local_tx_slot: Arc::clone(&local_tx_slot),
            local_tx_template,
        };

        let logger = test_logger();
        let forwarder = tokio::spawn(run_inbound_forwarder(
            logger,
            transport_rx,
            transport_batch_rx,
            default_tx,
            default_batch_tx,
            Arc::clone(&local_tx_slot),
            Arc::clone(&local_flows),
        ));

        // Step 1: before anyone takes the local receiver, only the default
        // channel should observe a packet.
        transport_tx
            .send(Bytes::from_static(b"first"))
            .await
            .expect("send #1");
        let first = timeout(Duration::from_millis(200), default_rx.recv())
            .await
            .expect("default recv #1 timed out")
            .expect("default channel must receive first packet");
        assert_eq!(&first[..], b"first");

        // Step 2: activate the local channel via the same API the adapter
        // will use (`take_local`).
        let mut local_rx = fanout.take_local().expect("take_local returns receiver");
        assert!(fanout.take_local().is_none(), "take_local must be one-shot");

        // Step 3: merely taking local receive must not mirror unrelated
        // endpoint traffic into the adapter.
        transport_tx
            .send(Bytes::from_static(b"second"))
            .await
            .expect("send #2");
        let second_default = timeout(Duration::from_millis(200), default_rx.recv())
            .await
            .expect("default recv #2 timed out")
            .expect("default channel still wired");
        assert_eq!(&second_default[..], b"second");
        assert!(
            local_rx.try_recv().is_err(),
            "unmatched packet must not fan out locally"
        );

        // Step 4: a packet matching a registered adapter-local flow is
        // consumed by the local receiver and must not leak into the TUN path.
        let flow = EndpointLocalFlow {
            network: Network::Tcp,
            local: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)), 50000),
            remote: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 443),
        };
        local_flows.insert(flow);
        let local_packet = local_tcp_packet(flow);
        transport_tx
            .send(Bytes::from(local_packet.clone()))
            .await
            .expect("send local packet");
        let matched_local = timeout(Duration::from_millis(200), local_rx.recv())
            .await
            .expect("local recv timed out")
            .expect("local channel must receive matching flow packet");
        assert_eq!(&matched_local[..], &local_packet[..]);
        assert!(
            default_rx.try_recv().is_err(),
            "adapter-local packet must not be forwarded into TUN"
        );

        // Tear down so the forwarder task exits cleanly.
        drop(transport_tx);
        forwarder
            .await
            .expect("forwarder task should join after upstream close");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inbound_forwarder_does_not_block_local_flow_when_default_is_full() {
        use std::time::Duration;
        use tokio::time::timeout;

        let (transport_tx, transport_rx) = mpsc::channel::<Bytes>(8);
        let (_transport_batch_tx, transport_batch_rx) = mpsc::channel::<Vec<Bytes>>(8);
        let (default_tx, _default_rx) = mpsc::channel::<Bytes>(1);
        let (default_batch_tx, _default_batch_rx) = mpsc::channel::<Vec<Bytes>>(1);
        let (local_tx_template, local_rx) = mpsc::channel::<Bytes>(8);
        let local_tx_slot: Arc<ArcSwapOption<mpsc::Sender<Bytes>>> =
            Arc::new(ArcSwapOption::empty());
        let local_flows = Arc::new(LocalFlowTable::default());
        let fanout = InboundFanout {
            default_rx: Mutex::new(Some(mpsc::channel::<Bytes>(1).1)),
            default_batch_rx: Mutex::new(Some(mpsc::channel::<Vec<Bytes>>(1).1)),
            local_rx: Mutex::new(Some(local_rx)),
            local_tx_slot: Arc::clone(&local_tx_slot),
            local_tx_template,
        };
        let mut local_rx = fanout.take_local().expect("take_local");

        let forwarder = tokio::spawn(run_inbound_forwarder(
            test_logger(),
            transport_rx,
            transport_batch_rx,
            default_tx,
            default_batch_tx,
            Arc::clone(&local_tx_slot),
            Arc::clone(&local_flows),
        ));

        transport_tx
            .send(Bytes::from_static(b"fills-default"))
            .await
            .expect("send default filler");
        transport_tx
            .send(Bytes::from_static(b"default-overflow"))
            .await
            .expect("send default overflow");

        let flow = EndpointLocalFlow {
            network: Network::Tcp,
            local: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)), 50000),
            remote: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 443),
        };
        local_flows.insert(flow);
        let local_packet = local_tcp_packet(flow);
        transport_tx
            .send(Bytes::from(local_packet.clone()))
            .await
            .expect("send local packet");

        let matched_local = timeout(Duration::from_millis(200), local_rx.recv())
            .await
            .expect("local recv timed out")
            .expect("local channel must receive despite default backpressure");
        assert_eq!(&matched_local[..], &local_packet[..]);

        drop(transport_tx);
        forwarder
            .await
            .expect("forwarder task should join after upstream close");
    }

    fn test_logger() -> Logger {
        use hammer_core::log::{DiscardWriter, Factory};
        use std::sync::Arc;
        use std::time::Instant;
        let factory = Factory::new(Instant::now(), Arc::new(DiscardWriter));
        factory.new_logger("wg-fanout-test")
    }

    fn local_tcp_packet(flow: EndpointLocalFlow) -> Vec<u8> {
        let (IpAddr::V4(src), IpAddr::V4(dst)) = (flow.remote.ip(), flow.local.ip()) else {
            panic!("test helper only builds IPv4 packets");
        };
        let mut packet = vec![0u8; 40];
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&(40u16).to_be_bytes());
        packet[8] = 64;
        packet[9] = 6;
        packet[12..16].copy_from_slice(&src.octets());
        packet[16..20].copy_from_slice(&dst.octets());
        packet[20..22].copy_from_slice(&flow.remote.port().to_be_bytes());
        packet[22..24].copy_from_slice(&flow.local.port().to_be_bytes());
        packet[32] = 0x50;
        packet
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endpoint_reset_preserves_packet_channels() {
        let priv_key = [12u8; 32];
        let peer_pub = x25519_public([13u8; 32]);
        let ep = make_endpoint("wg-reset", priv_key, peer_pub);

        ep.boot().expect("boot endpoint");
        assert!(ep.is_running());

        let before = ep.ip_send_clone();
        let inbound = ep.ip_recv_take().expect("first inbound receiver");
        ep.restart();
        assert!(ep.is_running(), "reset must keep the endpoint live");

        before
            .try_send(Bytes::from_static(b"still-live"))
            .expect("pre-reset sender must remain usable");
        assert!(
            ep.ip_recv_take().is_none(),
            "reset must not swap in an unattached inbound receiver"
        );
        drop(inbound);

        let after = ep.ip_send_clone();
        after
            .try_send(Bytes::from_static(b"fresh"))
            .expect("post-reset sender must accept a packet");

        ep.shutdown();
    }
}
