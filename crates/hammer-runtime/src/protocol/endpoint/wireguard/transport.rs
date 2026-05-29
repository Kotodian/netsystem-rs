//! WireGuard UDP transport actor.
//!
//! Owns the outer UDP socket (the one boringtun's encrypted frames travel on)
//! and a peer-set. A single tokio task multiplexes:
//!
//!   * inbound UDP datagrams → look up the originating peer by source address
//!     → boringtun `decapsulate` → forward decrypted IP packets to the inner
//!     stack via `inbound_tx`. Boringtun-driven control packets (handshake
//!     responses, cookie replies) are sent straight back to the peer.
//!   * outbound IP packets from `encrypt_rx` → LPM-route to a peer →
//!     boringtun `encapsulate` → UDP `send_to` the peer's endpoint.
//!   * rekey/keepalive timer firings from the control thread → enqueue work
//!     onto this actor. The actor owns each mutable boringtun tunnel and sends
//!     the resulting UDP packets.
//!
//! This actor only moves encrypted UDP frames between peers and the
//! stack-facing queues.

use std::future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use boringtun::noise::{TunnControlResult, TunnResult, TunnTimerResult};
use bytes::{Bytes, BytesMut};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use hammer_core::error::{HammerError, HammerResult, WithContext};
use hammer_core::log::Logger;
use hammer_core::protocol::wireguard::WIREGUARD_OVERHEAD;
#[cfg(feature = "endpoint-amneziawg")]
use hammer_core::protocol::wireguard::amnezia2::Amnezia2Options;
use hammer_core::protocol::wireguard::peer::{
    self, Peer, PeerDataResult, PeerDataSessionUpdate, PeerDataTunnel, PeerTunnel,
};

#[cfg(feature = "endpoint-amneziawg")]
use super::amnezia2::{decode_inbound_packet, encode_outbound_packet, make_handshake_junk_packets};
use super::endpoint::{
    WireguardPeerHandshakeReceivedArgs, WireguardPeerKeepaliveReceivedArgs,
    WireguardPeerPacketReceivedArgs, WireguardPeerPacketSentArgs, WireguardPeerStartArgs,
    build_wireguard_inbound_control_subscriber, build_wireguard_start_handshake_subscriber,
};
use crate::socket_protector::SocketProtector;
use crate::{ControlThreadHandle, ControlTimerHandle};

/// 250 ms matches what boringtun's own examples and Cloudflare WARP use to
/// drive `Tunn::update_timers` — fine-grained enough for rekey/keepalive
/// decisions without burning a CPU.
const REKEY_KEEPALIVE_TIMER: Duration = Duration::from_millis(250);
const SESSION_REJECT_AFTER_TIME: Duration = Duration::from_secs(180);

/// Channel buffer for IP packets waiting to be encrypted. Apple utun can hand
/// the packet loop a 256-packet batch, and the L3 fast path uses `try_send` so
/// it never parks the TUN reader. Keep enough room for at least two full TUN
/// batches; otherwise a single speed-test burst can fill the channel and turn
/// the rest of that batch into drops.
pub(crate) const ENCRYPT_QUEUE: usize = 512;
pub(crate) const ENCRYPT_BATCH_QUEUE: usize = 32;
/// Same for the decrypted-inbound side. Now that each decrypted packet is
/// pushed as its own `Bytes` (the old `IpStackInput::Batch` enum is gone), the
/// queue holds one packet per slot.
/// 256 × MTU 1408 ≈ 360 KiB worst case, comfortably inside NetExt budget; the
/// queue almost never fills because the TUN writer drains it line-rate.
const INBOUND_QUEUE: usize = 256;
pub(crate) const INBOUND_BATCH_QUEUE: usize = 32;
/// After one UDP readiness wake, drain a small bounded burst so we keep the
/// boringtun decap loop hot before re-entering `select!`. Each packet is then
/// pushed individually to `inbound_tx`.
const INBOUND_DRAIN_BATCH: usize = 32;
/// Mirror on the egress side: after `encrypt_rx.recv()` returns the first
/// packet, opportunistically `try_recv` up to `OUTBOUND_DRAIN_BATCH-1` more so
/// boringtun's per-peer Tunn lock can be acquired once for the whole burst
/// instead of N times. Single-peer (the typical iOS client) batches collapse
/// to one lock acquisition; multi-peer split-tunnel still saves a lock per
/// run of consecutive same-peer packets.
const OUTBOUND_DRAIN_BATCH: usize = 64;
const TUNNEL_CONTROL_QUEUE: usize = 64;

#[derive(Clone)]
pub(crate) struct PeerStartControl {
    peer_index: usize,
    control: TunnelControl,
}

#[derive(Clone)]
pub(crate) struct PeerInboundControl {
    peer_index: usize,
    control: TunnelControl,
}

#[derive(Clone)]
pub(crate) struct PeerDataActivityControl {
    peer_index: usize,
    control: TunnelControl,
}

impl PeerStartControl {
    fn new(peer_index: usize, control: TunnelControl) -> Self {
        Self {
            peer_index,
            control,
        }
    }

    pub(crate) fn start_handshake(&self) -> HammerResult<()> {
        self.control.start_handshake(self.peer_index)
    }
}

impl PeerInboundControl {
    fn new(peer_index: usize, control: TunnelControl) -> Self {
        Self {
            peer_index,
            control,
        }
    }

    pub(crate) fn process_inbound_control(
        &self,
        src_ip: IpAddr,
        datagram: Bytes,
    ) -> HammerResult<()> {
        self.control
            .process_inbound_control(self.peer_index, src_ip, datagram)
    }
}

impl PeerDataActivityControl {
    fn new(peer_index: usize, control: TunnelControl) -> Self {
        Self {
            peer_index,
            control,
        }
    }

    pub(crate) fn record_packet_sent(&self, plaintext_len: usize) -> HammerResult<()> {
        self.control
            .record_packet_sent(self.peer_index, plaintext_len)
    }

    pub(crate) fn record_packet_received(&self, plaintext_len: usize) -> HammerResult<()> {
        self.control
            .record_packet_received(self.peer_index, plaintext_len)
    }
}

impl std::fmt::Debug for PeerStartControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerStartControl")
            .field("peer_index", &self.peer_index)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PeerInboundControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerInboundControl")
            .field("peer_index", &self.peer_index)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PeerDataActivityControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerDataActivityControl")
            .field("peer_index", &self.peer_index)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct TunnelControl {
    tx: mpsc::Sender<TunnelControlMessage>,
}

impl TunnelControl {
    fn new(tx: mpsc::Sender<TunnelControlMessage>) -> Self {
        Self { tx }
    }

    fn peer_start_control(&self, peer_index: usize) -> PeerStartControl {
        PeerStartControl::new(peer_index, self.clone())
    }

    fn peer_inbound_control(&self, peer_index: usize) -> PeerInboundControl {
        PeerInboundControl::new(peer_index, self.clone())
    }

    fn peer_data_activity_control(&self, peer_index: usize) -> PeerDataActivityControl {
        PeerDataActivityControl::new(peer_index, self.clone())
    }

    fn start_handshake(&self, peer_index: usize) -> HammerResult<()> {
        self.try_send(TunnelControlMessage::StartHandshake { peer_index })
    }

    fn process_inbound_control(
        &self,
        peer_index: usize,
        src_ip: IpAddr,
        datagram: Bytes,
    ) -> HammerResult<()> {
        self.try_send(TunnelControlMessage::InboundControlDatagram {
            peer_index,
            src_ip,
            datagram,
        })
    }

    fn run_rekey_keepalive_timer(&self) {
        if let Err(err) = self.try_send(TunnelControlMessage::RekeyKeepaliveTimer) {
            warn!("wireguard rekey/keepalive timer enqueue failed: {err}");
        }
    }

    fn record_packet_sent(&self, peer_index: usize, plaintext_len: usize) -> HammerResult<()> {
        self.try_send(TunnelControlMessage::DataPacketSent {
            peer_index,
            plaintext_len,
        })
    }

    fn record_packet_received(&self, peer_index: usize, plaintext_len: usize) -> HammerResult<()> {
        self.try_send(TunnelControlMessage::DataPacketReceived {
            peer_index,
            plaintext_len,
        })
    }

    fn try_send(&self, message: TunnelControlMessage) -> HammerResult<()> {
        self.tx.try_send(message).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => {
                HammerError::internal("wireguard tunnel control queue full")
            }
            mpsc::error::TrySendError::Closed(_) => {
                HammerError::internal("wireguard transport stopped")
            }
        })
    }
}

impl std::fmt::Debug for TunnelControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TunnelControl").finish_non_exhaustive()
    }
}

enum TunnelControlMessage {
    StartHandshake {
        peer_index: usize,
    },
    InboundControlDatagram {
        peer_index: usize,
        src_ip: IpAddr,
        datagram: Bytes,
    },
    RekeyKeepaliveTimer,
    DataPacketSent {
        peer_index: usize,
        plaintext_len: usize,
    },
    DataPacketReceived {
        peer_index: usize,
        plaintext_len: usize,
    },
}

enum DataPlaneCommand {
    SendNetwork {
        peer_index: usize,
        packet: Bytes,
        send_handshake_junk: bool,
    },
    InstallSession {
        peer_index: usize,
        generation: u64,
        session: boringtun::noise::DataSession,
        send_keepalive: bool,
    },
    SendKeepalive {
        peer_index: usize,
    },
    ExpireSession {
        peer_index: usize,
        local_index: usize,
        generation: u64,
    },
}

#[derive(Debug, Clone)]
struct ControlSessionMeta {
    local_index: usize,
    generation: u64,
    installed_at: Instant,
}

#[derive(Debug, Default)]
struct ControlPeerState {
    next_generation: u64,
    sessions: Vec<ControlSessionMeta>,
}

fn spawn_tunnel_control_actor(
    control_handle: &Arc<ControlThreadHandle>,
    peers: Arc<Vec<Peer>>,
    peer_tunnels: Vec<PeerTunnel>,
    data_tx: mpsc::Sender<DataPlaneCommand>,
    crypto_cap: usize,
) -> HammerResult<TunnelControl> {
    let (tx, rx) = mpsc::channel::<TunnelControlMessage>(TUNNEL_CONTROL_QUEUE);
    control_handle.call(move || {
        tokio::spawn(run_tunnel_control_actor(
            peers,
            peer_tunnels,
            data_tx,
            crypto_cap,
            rx,
        ));
    })?;
    Ok(TunnelControl::new(tx))
}

async fn run_tunnel_control_actor(
    peers: Arc<Vec<Peer>>,
    mut peer_tunnels: Vec<PeerTunnel>,
    data_tx: mpsc::Sender<DataPlaneCommand>,
    crypto_cap: usize,
    mut rx: mpsc::Receiver<TunnelControlMessage>,
) {
    let mut states = (0..peers.len())
        .map(|_| ControlPeerState::default())
        .collect::<Vec<_>>();
    while let Some(message) = rx.recv().await {
        handle_tunnel_control_actor_message(
            &peers,
            &mut peer_tunnels,
            &mut states,
            &data_tx,
            message,
            crypto_cap,
        );
    }
}

fn handle_tunnel_control_actor_message(
    peers: &[Peer],
    peer_tunnels: &mut [PeerTunnel],
    states: &mut [ControlPeerState],
    data_tx: &mpsc::Sender<DataPlaneCommand>,
    message: TunnelControlMessage,
    crypto_cap: usize,
) {
    match message {
        TunnelControlMessage::StartHandshake { peer_index } => {
            let Some((_, packet)) = run_control_action(
                peers,
                peer_tunnels,
                peer_index,
                crypto_cap,
                |tunnel, buf| tunnel.start_handshake(buf, false),
            ) else {
                return;
            };
            send_data_command(
                data_tx,
                DataPlaneCommand::SendNetwork {
                    peer_index,
                    packet: packet.freeze(),
                    send_handshake_junk: true,
                },
            );
        }
        TunnelControlMessage::InboundControlDatagram {
            peer_index,
            src_ip,
            datagram,
        } => {
            let Some(peer) = peers.get(peer_index) else {
                warn!("wireguard control packet: peer index {peer_index} out of range");
                return;
            };
            let Some(tunnel) = peer_tunnels.get_mut(peer_index) else {
                warn!("wireguard control packet: missing tunnel for peer index {peer_index}");
                return;
            };
            let mut packet_buf = alloc_crypto_buf(crypto_cap);
            match tunnel.decapsulate_control(Some(src_ip), &datagram, &mut packet_buf[..]) {
                TunnControlResult::Done => {}
                TunnControlResult::Err(err) => {
                    tracing::trace!(?err, "wireguard control decapsulate error");
                }
                TunnControlResult::WriteToNetwork(packet) => {
                    let packet = Bytes::copy_from_slice(packet);
                    send_data_command(
                        data_tx,
                        DataPlaneCommand::SendNetwork {
                            peer_index,
                            packet,
                            send_handshake_junk: false,
                        },
                    );
                }
                TunnControlResult::WriteToNetworkAndInstallSession { packet, session } => {
                    let generation =
                        install_control_session(states, data_tx, peer_index, session, false);
                    tracing::debug!(
                        endpoint_peer = %peer.endpoint(),
                        peer_index,
                        generation,
                        "wireguard control installed responder data session"
                    );
                    let packet = Bytes::copy_from_slice(packet);
                    send_data_command(
                        data_tx,
                        DataPlaneCommand::SendNetwork {
                            peer_index,
                            packet,
                            send_handshake_junk: true,
                        },
                    );
                }
                TunnControlResult::InstallSession {
                    session,
                    send_keepalive,
                } => {
                    let generation = install_control_session(
                        states,
                        data_tx,
                        peer_index,
                        session,
                        send_keepalive,
                    );
                    tracing::debug!(
                        endpoint_peer = %peer.endpoint(),
                        peer_index,
                        generation,
                        "wireguard control installed initiator data session"
                    );
                }
            }
        }
        TunnelControlMessage::RekeyKeepaliveTimer => {
            expire_control_sessions(states, data_tx);
            for peer_index in 0..peers.len() {
                let Some((_, result)) = run_control_timer_action(
                    peers,
                    peer_tunnels,
                    peer_index,
                    crypto_cap,
                    |tunnel, buf| tunnel.update_timers_control(buf),
                ) else {
                    continue;
                };
                match result {
                    ControlTimerAction::SendNetwork(packet) => {
                        send_data_command(
                            data_tx,
                            DataPlaneCommand::SendNetwork {
                                peer_index,
                                packet,
                                send_handshake_junk: true,
                            },
                        );
                    }
                    ControlTimerAction::SendKeepalive => {
                        send_data_command(data_tx, DataPlaneCommand::SendKeepalive { peer_index });
                    }
                }
            }
        }
        TunnelControlMessage::DataPacketSent {
            peer_index,
            plaintext_len,
        } => {
            if let Some(tunnel) = peer_tunnels.get_mut(peer_index) {
                tunnel.record_data_plane_packet_sent(plaintext_len);
            }
        }
        TunnelControlMessage::DataPacketReceived {
            peer_index,
            plaintext_len,
        } => {
            if let Some(tunnel) = peer_tunnels.get_mut(peer_index) {
                tunnel.record_data_plane_packet_received(plaintext_len);
            }
        }
    }
}

fn install_control_session(
    states: &mut [ControlPeerState],
    data_tx: &mpsc::Sender<DataPlaneCommand>,
    peer_index: usize,
    session: boringtun::noise::DataSession,
    send_keepalive: bool,
) -> u64 {
    let Some(state) = states.get_mut(peer_index) else {
        return 0;
    };
    state.next_generation = state.next_generation.saturating_add(1);
    let generation = state.next_generation;
    let local_index = session.local_index();
    state.sessions.push(ControlSessionMeta {
        local_index,
        generation,
        installed_at: Instant::now(),
    });
    send_data_command(
        data_tx,
        DataPlaneCommand::InstallSession {
            peer_index,
            generation,
            session,
            send_keepalive,
        },
    );
    generation
}

fn expire_control_sessions(
    states: &mut [ControlPeerState],
    data_tx: &mpsc::Sender<DataPlaneCommand>,
) {
    let now = Instant::now();
    for (peer_index, state) in states.iter_mut().enumerate() {
        let mut kept = Vec::with_capacity(state.sessions.len());
        for session in state.sessions.drain(..) {
            if now.duration_since(session.installed_at) >= SESSION_REJECT_AFTER_TIME {
                send_data_command(
                    data_tx,
                    DataPlaneCommand::ExpireSession {
                        peer_index,
                        local_index: session.local_index,
                        generation: session.generation,
                    },
                );
            } else {
                kept.push(session);
            }
        }
        state.sessions = kept;
    }
}

fn send_data_command(data_tx: &mpsc::Sender<DataPlaneCommand>, command: DataPlaneCommand) {
    if let Err(err) = data_tx.try_send(command) {
        warn!("wireguard data command enqueue failed: {err}");
    }
}

enum ControlTimerAction {
    SendNetwork(Bytes),
    SendKeepalive,
}

fn run_control_action<'a>(
    peers: &'a [Peer],
    peer_tunnels: &mut [PeerTunnel],
    peer_index: usize,
    crypto_cap: usize,
    action: impl for<'buf> FnOnce(&mut PeerTunnel, &'buf mut [u8]) -> TunnResult<'buf>,
) -> Option<(&'a Peer, BytesMut)> {
    let peer = peers.get(peer_index)?;
    let tunnel = peer_tunnels.get_mut(peer_index)?;
    let mut packet = alloc_crypto_buf(crypto_cap);
    let len = match action(tunnel, &mut packet[..]) {
        TunnResult::WriteToNetwork(out) => Some(out.len()),
        TunnResult::Done => None,
        TunnResult::Err(err) => {
            tracing::trace!(?err, "wireguard control tunnel action error");
            None
        }
        TunnResult::WriteToTunnelV4(_, _) | TunnResult::WriteToTunnelV6(_, _) => None,
    }?;
    packet.truncate(len);
    Some((peer, packet))
}

fn run_control_timer_action<'a>(
    peers: &'a [Peer],
    peer_tunnels: &mut [PeerTunnel],
    peer_index: usize,
    crypto_cap: usize,
    action: impl for<'buf> FnOnce(&mut PeerTunnel, &'buf mut [u8]) -> TunnTimerResult<'buf>,
) -> Option<(&'a Peer, ControlTimerAction)> {
    let peer = peers.get(peer_index)?;
    let tunnel = peer_tunnels.get_mut(peer_index)?;
    let mut packet = alloc_crypto_buf(crypto_cap);
    match action(tunnel, &mut packet[..]) {
        TunnTimerResult::WriteToNetwork(out) => {
            let len = out.len();
            packet.truncate(len);
            Some((peer, ControlTimerAction::SendNetwork(packet.freeze())))
        }
        TunnTimerResult::SendKeepalive => Some((peer, ControlTimerAction::SendKeepalive)),
        TunnTimerResult::Done => None,
        TunnTimerResult::Err(err) => {
            tracing::trace!(?err, "wireguard control timer error");
            None
        }
    }
}

/// Handles returned to the owner of a transport actor. Hold these for the
/// lifetime of the endpoint; dropping `shutdown` is enough to stop the actor
/// (the loop also exits when both `encrypt_tx` and `inbound_rx` close).
pub(crate) struct TransportHandles {
    /// Push IP packets into here to have them encapsulated and shipped to
    /// whichever peer's `allowed_ips` matches the destination.
    pub(crate) encrypt_tx: mpsc::Sender<Bytes>,
    pub(crate) encrypt_batch_tx: mpsc::Sender<Vec<Bytes>>,
    /// Receive decrypted IP packets coming back from any peer. The endpoint
    /// (now an `adapter::Endpoint`, not an `Outbound`) hands this stream
    /// straight to the TUN packet-loop fan-in.
    pub(crate) inbound_rx: mpsc::Receiver<Bytes>,
    pub(crate) inbound_batch_rx: mpsc::Receiver<Vec<Bytes>>,
    /// The address the OS bound the UDP socket to.
    #[cfg(test)]
    pub(crate) local_addr: SocketAddr,
    /// Send `()` (or drop) to ask the actor to exit promptly.
    pub(crate) shutdown: oneshot::Sender<()>,
    /// Ask the actor to rebind/reprotect its UDP sockets while keeping the
    /// packet channels stable for the TUN fast path.
    pub(crate) reset_tx: mpsc::Sender<()>,
    pub(crate) tunnel_control: TunnelControl,
    pub(crate) rekey_keepalive_timer: Option<ControlTimerHandle>,
    /// Join handle in case the caller wants to await actor termination.
    pub(crate) join: JoinHandle<()>,
}

struct TransportSockets {
    ipv4: Option<Arc<UdpSocket>>,
    ipv6: Option<Arc<UdpSocket>>,
}

enum TunnelState {
    Legacy(Vec<PeerTunnel>),
    Data(Vec<PeerDataTunnel>),
}

impl TransportSockets {
    fn local_addr(&self) -> HammerResult<SocketAddr> {
        let socket = self
            .ipv4
            .as_deref()
            .or(self.ipv6.as_deref())
            .ok_or_else(|| HammerError::internal("wireguard transport has no UDP socket"))?;
        socket
            .local_addr()
            .with_context(|| "wireguard udp local_addr")
    }

    fn send_socket(&self, destination: SocketAddr) -> Option<&UdpSocket> {
        if destination.is_ipv4() {
            self.ipv4.as_deref()
        } else {
            self.ipv6.as_deref()
        }
    }
}

impl TransportHandles {
    pub(crate) fn reset(&self) {
        match self.reset_tx.try_send(()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                debug!("wireguard transport reset already queued");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("wireguard transport reset requested after actor closed");
            }
        }
    }

    pub(crate) fn peer_start_control(&self, peer_index: usize) -> PeerStartControl {
        self.tunnel_control.peer_start_control(peer_index)
    }

    pub(crate) fn cancel_rekey_keepalive_timer(&self) {
        if let Some(timer) = &self.rekey_keepalive_timer {
            let _ = timer.cancel();
        }
    }

    #[cfg(test)]
    pub(crate) fn has_control_rekey_keepalive_timer_for_test(&self) -> bool {
        self.rekey_keepalive_timer.is_some()
    }
}

/// Spin up the UDP transport actor. Binds the socket synchronously (cheap —
/// it's just a `socket()` + `bind()` syscall) so this works inside the
/// blocking `Lifecycle::start` path; the resulting tokio `UdpSocket` is then
/// handed to a detached task.
pub(crate) fn spawn_transport(
    _logger: Logger,
    endpoint_id: String,
    peers: Arc<Vec<Peer>>,
    peer_tunnels: Vec<PeerTunnel>,
    listen_port: u16,
    mtu: u32,
    protector: SocketProtector,
    control_handle: Option<Arc<ControlThreadHandle>>,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<Amnezia2Options>,
) -> HammerResult<TransportHandles> {
    if peers.len() != peer_tunnels.len() {
        return Err(HammerError::internal(
            "wireguard transport peer/tunnel count mismatch",
        ));
    }
    let sockets = bind_transport_sockets(&peers, listen_port, &protector)?;
    let local_addr = sockets.local_addr()?;
    info!("wireguard transport listening on {local_addr}");

    let (encrypt_tx, encrypt_rx) = mpsc::channel::<Bytes>(ENCRYPT_QUEUE);
    let (encrypt_batch_tx, encrypt_batch_rx) = mpsc::channel::<Vec<Bytes>>(ENCRYPT_BATCH_QUEUE);
    let (inbound_tx, inbound_rx) = mpsc::channel::<Bytes>(INBOUND_QUEUE);
    let (inbound_batch_tx, inbound_batch_rx) = mpsc::channel::<Vec<Bytes>>(INBOUND_BATCH_QUEUE);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (reset_tx, reset_rx) = mpsc::channel::<()>(1);
    let crypto_cap = (mtu as usize) + WIREGUARD_OVERHEAD;
    let event_control_handle = control_handle.clone();
    let (tunnel_control, tunnel_state, tunnel_control_rx, data_control_rx) =
        if let Some(control_handle) = control_handle.clone() {
            let (data_control_tx, data_control_rx) =
                mpsc::channel::<DataPlaneCommand>(TUNNEL_CONTROL_QUEUE);
            let tunnel_control = spawn_tunnel_control_actor(
                &control_handle,
                Arc::clone(&peers),
                peer_tunnels,
                data_control_tx,
                crypto_cap,
            )?;
            let peer_data_tunnels = (0..peers.len())
                .map(|_| PeerDataTunnel::new())
                .collect::<Vec<_>>();
            (
                tunnel_control,
                TunnelState::Data(peer_data_tunnels),
                None,
                Some(data_control_rx),
            )
        } else {
            let (tunnel_control_tx, tunnel_control_rx) =
                mpsc::channel::<TunnelControlMessage>(TUNNEL_CONTROL_QUEUE);
            (
                TunnelControl::new(tunnel_control_tx),
                TunnelState::Legacy(peer_tunnels),
                Some(tunnel_control_rx),
                None,
            )
        };
    let rekey_keepalive_timer = if let Some(control_handle) = control_handle {
        let tunnel_control = tunnel_control.clone();
        Some(
            control_handle
                .schedule_interval(REKEY_KEEPALIVE_TIMER, REKEY_KEEPALIVE_TIMER, move || {
                    let tunnel_control = tunnel_control.clone();
                    async move {
                        tunnel_control.run_rekey_keepalive_timer();
                    }
                })
                .with_context(|| "wireguard control rekey/keepalive timer")?,
        )
    } else {
        None
    };

    let join = crate::spawn::spawn(run_actor(
        sockets,
        endpoint_id,
        peers,
        tunnel_state,
        tunnel_control.clone(),
        event_control_handle,
        listen_port,
        mtu,
        protector,
        #[cfg(feature = "endpoint-amneziawg")]
        amnezia,
        encrypt_rx,
        encrypt_batch_rx,
        inbound_tx,
        inbound_batch_tx,
        shutdown_rx,
        reset_rx,
        tunnel_control_rx,
        data_control_rx,
    ));

    Ok(TransportHandles {
        encrypt_tx,
        encrypt_batch_tx,
        inbound_rx,
        inbound_batch_rx,
        #[cfg(test)]
        local_addr,
        shutdown: shutdown_tx,
        reset_tx,
        tunnel_control,
        rekey_keepalive_timer,
        join,
    })
}

fn bind_transport_sockets(
    peers: &[Peer],
    listen_port: u16,
    protector: &SocketProtector,
) -> HammerResult<TransportSockets> {
    let needs_ipv4 = peers.iter().any(|peer| peer.endpoint().is_ipv4());
    let needs_ipv6 = peers.iter().any(|peer| peer.endpoint().is_ipv6());
    let needs_ipv4 = needs_ipv4 || !needs_ipv6;

    let mut ipv6 = if needs_ipv6 {
        Some(bind_udp_socket(
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), listen_port),
            true,
            protector,
        )?)
    } else {
        None
    };
    let ipv4_port = if listen_port == 0 && needs_ipv4 && needs_ipv6 {
        ipv6.as_ref()
            .expect("ipv6 socket just bound")
            .local_addr()
            .with_context(|| "wireguard udp local_addr")?
            .port()
    } else {
        listen_port
    };
    let ipv4 = if needs_ipv4 {
        Some(bind_udp_socket(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), ipv4_port),
            false,
            protector,
        )?)
    } else {
        None
    };

    // Prefer the IPv4 socket as the public handle when both exist; it proves
    // IPv4 peers are not accidentally routed through an IPv6-only socket.
    Ok(TransportSockets {
        ipv4,
        ipv6: ipv6.take(),
    })
}

fn reset_transport_sockets_with<F>(
    sockets: &mut TransportSockets,
    listen_port: u16,
    bind: F,
) -> HammerResult<SocketAddr>
where
    F: FnOnce() -> HammerResult<TransportSockets>,
{
    if listen_port == 0 {
        let new_sockets = bind()?;
        let local_addr = new_sockets.local_addr()?;
        *sockets = new_sockets;
        return Ok(local_addr);
    }

    let old_sockets = std::mem::replace(
        sockets,
        TransportSockets {
            ipv4: None,
            ipv6: None,
        },
    );
    drop(old_sockets);
    let new_sockets = bind()?;
    let local_addr = new_sockets.local_addr()?;
    *sockets = new_sockets;
    Ok(local_addr)
}

fn bind_udp_socket(
    bind: SocketAddr,
    ipv6_only: bool,
    protector: &SocketProtector,
) -> HammerResult<Arc<UdpSocket>> {
    if bind.is_ipv4() {
        return std_udp_socket(bind, protector);
    }
    let domain = if bind.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))
        .with_context(|| format!("wireguard udp socket {bind}"))?;
    if bind.is_ipv6() {
        socket
            .set_only_v6(ipv6_only)
            .with_context(|| "wireguard udp set_only_v6")?;
    }
    socket
        .bind(&bind.into())
        .with_context(|| format!("wireguard udp bind {bind}"))?;
    let std_socket: std::net::UdpSocket = socket.into();
    std_socket
        .set_nonblocking(true)
        .with_context(|| "wireguard udp set_nonblocking")?;
    let socket = UdpSocket::from_std(std_socket).with_context(|| "wireguard udp from_std")?;
    protector.protect(&socket)?;
    Ok(Arc::new(socket))
}

fn std_udp_socket(bind: SocketAddr, protector: &SocketProtector) -> HammerResult<Arc<UdpSocket>> {
    let socket =
        std::net::UdpSocket::bind(bind).with_context(|| format!("wireguard udp bind {bind}"))?;
    socket
        .set_nonblocking(true)
        .with_context(|| "wireguard udp set_nonblocking")?;
    let socket = UdpSocket::from_std(socket).with_context(|| "wireguard udp from_std")?;
    protector.protect(&socket)?;
    Ok(Arc::new(socket))
}

/// The actor body. Each branch is a single `select!` arm to keep the loop
/// fairly easy to reason about; backpressure on either mpsc is handled by
/// dropping packets after a warn-log because wg is best-effort at L3 anyway.
async fn run_actor(
    mut sockets: TransportSockets,
    endpoint_id: String,
    peers: Arc<Vec<Peer>>,
    mut tunnel_state: TunnelState,
    tunnel_control: TunnelControl,
    control_handle: Option<Arc<ControlThreadHandle>>,
    listen_port: u16,
    mtu: u32,
    protector: SocketProtector,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<Amnezia2Options>,
    mut encrypt_rx: mpsc::Receiver<Bytes>,
    mut encrypt_batch_rx: mpsc::Receiver<Vec<Bytes>>,
    inbound_tx: mpsc::Sender<Bytes>,
    inbound_batch_tx: mpsc::Sender<Vec<Bytes>>,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut reset_rx: mpsc::Receiver<()>,
    mut tunnel_control_rx: Option<mpsc::Receiver<TunnelControlMessage>>,
    mut data_control_rx: Option<mpsc::Receiver<DataPlaneCommand>>,
) {
    // Capacity for every encap/decap output buffer the actor will allocate.
    // Each step allocates its own BytesMut so the result can be `freeze()`d
    // straight into `inbound_tx` without a memcpy.
    let crypto_cap = (mtu as usize) + WIREGUARD_OVERHEAD;
    // boringtun WG data frames max out at `mtu + 32`; handshake / cookie
    // replies are < 200 B. Sizing the recv scratch to `crypto_cap` (with a
    // 2 KiB floor for tiny MTUs) keeps the actor's resident footprint tight
    // — the previous 65535 B per socket × 2 sockets wasted ~125 KiB of
    // NetExt RSS for nothing. A datagram larger than `crypto_cap` would be
    // truncated by `recv_from` and rejected by boringtun anyway.
    let udp_recv_cap = crypto_cap.max(2 * 1024);
    let mut udp_buf_v4 = vec![0u8; udp_recv_cap];
    let mut udp_buf_v6 = vec![0u8; udp_recv_cap];

    // Reusable scratch the actor holds across iterations:
    //   * `inbound_batch` — staging Vec for `handle_inbound_ready`. Cleared
    //     and refilled on every UDP readiness wake; capacity persists across
    //     wakes so we stop re-allocating a 32-slot Vec per recv.
    //   * `outbound_batch` / `outbound_routed` — staging Vecs for encrypt
    //     bursts. Keep them actor-owned so every select wake doesn't touch
    //     the allocator before it can send the first packet.
    let mut inbound_batch: Vec<Bytes> = Vec::with_capacity(INBOUND_DRAIN_BATCH);
    let mut outbound_batch: Vec<Bytes> = Vec::with_capacity(OUTBOUND_DRAIN_BATCH);
    let mut outbound_routed: Vec<(usize, Bytes)> = Vec::with_capacity(OUTBOUND_DRAIN_BATCH);
    let mut outbound_send_buf = BytesMut::with_capacity(crypto_cap);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                debug!("wireguard transport: shutdown");
                break;
            }
            reset = reset_rx.recv() => {
                if reset.is_none() {
                    continue;
                }
                match reset_transport_sockets_with(&mut sockets, listen_port, || {
                    bind_transport_sockets(&peers, listen_port, &protector)
                }) {
                    Ok(local_addr) => info!("wireguard transport rebound on {local_addr}"),
                    Err(err) => {
                        warn!("wireguard transport reset bind failed: {err}");
                    }
                }
            }
            message = recv_tunnel_control_optional(tunnel_control_rx.as_mut()) => {
                let Some(message) = message else {
                    tunnel_control_rx = None;
                    continue;
                };
                if let TunnelState::Legacy(peer_tunnels) = &mut tunnel_state {
                    handle_tunnel_control_message(
                        &sockets,
                        &peers,
                        peer_tunnels,
                        #[cfg(feature = "endpoint-amneziawg")]
                        amnezia.as_ref(),
                        message,
                        crypto_cap,
                    ).await;
                }
            }
            command = recv_data_control_optional(data_control_rx.as_mut()) => {
                let Some(command) = command else {
                    data_control_rx = None;
                    continue;
                };
                if let TunnelState::Data(peer_data_tunnels) = &mut tunnel_state {
                    handle_data_plane_command(
                    &sockets,
                    &endpoint_id,
                    &peers,
                    peer_data_tunnels,
                    control_handle.as_deref(),
                    &tunnel_control,
                    #[cfg(feature = "endpoint-amneziawg")]
                    amnezia.as_ref(),
                    command,
                    crypto_cap,
                ).await;
                }
            }
            res = recv_from_optional(sockets.ipv4.as_deref(), &mut udp_buf_v4), if sockets.ipv4.is_some() => {
                match res {
                    Ok((n, src)) => {
                        let socket = sockets
                            .ipv4
                            .as_deref()
                            .expect("ipv4 socket guarded by select arm");
                        handle_inbound_ready(
                            &sockets,
                            &endpoint_id,
                            &peers,
                            &mut tunnel_state,
                            control_handle.as_deref(),
                            &tunnel_control,
                            #[cfg(feature = "endpoint-amneziawg")]
                            amnezia.as_ref(),
                            socket,
                            src,
                            n,
                            &mut udp_buf_v4,
                            crypto_cap,
                            &inbound_tx,
                            &inbound_batch_tx,
                            &mut inbound_batch,
                        )
                        .await;
                    }
                    Err(err) => {
                        warn!("wireguard udp recv: {err}");
                    }
                }
            }
            res = recv_from_optional(sockets.ipv6.as_deref(), &mut udp_buf_v6), if sockets.ipv6.is_some() => {
                match res {
                    Ok((n, src)) => {
                        let socket = sockets
                            .ipv6
                            .as_deref()
                            .expect("ipv6 socket guarded by select arm");
                        handle_inbound_ready(
                            &sockets,
                            &endpoint_id,
                            &peers,
                            &mut tunnel_state,
                            control_handle.as_deref(),
                            &tunnel_control,
                            #[cfg(feature = "endpoint-amneziawg")]
                            amnezia.as_ref(),
                            socket,
                            src,
                            n,
                            &mut udp_buf_v6,
                            crypto_cap,
                            &inbound_tx,
                            &inbound_batch_tx,
                            &mut inbound_batch,
                        )
                        .await;
                    }
                    Err(err) => {
                        warn!("wireguard udp recv: {err}");
                    }
                }
            }
            ip = encrypt_rx.recv() => {
                let Some(first) = ip else {
                    debug!("wireguard transport: encrypt_rx closed");
                    break;
                };
                handle_outbound_batch(
                    &sockets,
                    &endpoint_id,
                    &peers,
                    &mut tunnel_state,
                    control_handle.as_deref(),
                    &tunnel_control,
                    #[cfg(feature = "endpoint-amneziawg")]
                    amnezia.as_ref(),
                    first,
                    &mut encrypt_rx,
                    crypto_cap,
                    &mut outbound_batch,
                    &mut outbound_routed,
                    &mut outbound_send_buf,
                )
                .await;
            }
            batch = encrypt_batch_rx.recv() => {
                let Some(mut batch) = batch else {
                    debug!("wireguard transport: encrypt_batch_rx closed");
                    break;
                };
                handle_outbound_packet_batch(
                    &sockets,
                    &endpoint_id,
                    &peers,
                    &mut tunnel_state,
                    control_handle.as_deref(),
                    &tunnel_control,
                    #[cfg(feature = "endpoint-amneziawg")]
                    amnezia.as_ref(),
                    &mut batch,
                    crypto_cap,
                    &mut outbound_routed,
                    &mut outbound_send_buf,
                )
                .await;
            }
        }
    }
}

async fn recv_from_optional(
    socket: Option<&UdpSocket>,
    buf: &mut [u8],
) -> io::Result<(usize, SocketAddr)> {
    match socket {
        Some(socket) => socket.recv_from(buf).await,
        None => future::pending().await,
    }
}

async fn recv_tunnel_control_optional(
    rx: Option<&mut mpsc::Receiver<TunnelControlMessage>>,
) -> Option<TunnelControlMessage> {
    match rx {
        Some(rx) => rx.recv().await,
        None => future::pending().await,
    }
}

async fn recv_data_control_optional(
    rx: Option<&mut mpsc::Receiver<DataPlaneCommand>>,
) -> Option<DataPlaneCommand> {
    match rx {
        Some(rx) => rx.recv().await,
        None => future::pending().await,
    }
}

/// Handle one UDP readiness wake. Always processes the datagram the select arm
/// already received, then opportunistically `try_recv_from`s up to
/// `INBOUND_DRAIN_BATCH-1` more datagrams off the same socket without
/// re-entering `select!`, so `flush_inbound_batch` can amortize one mpsc send
/// across the whole burst.
///
/// The multi-packet drain path is exercised by the round-trip e2e test in this
/// module under sustained load; we don't have a focused unit test for it
/// because reliably triggering "≥2 datagrams ready in one wake" in-process
/// requires racing two `send_to` calls before the first `recv_from` resolves —
/// flaky enough that an integration-style test is the better tool.
async fn handle_inbound_ready(
    sockets: &TransportSockets,
    endpoint_id: &str,
    peers: &[Peer],
    tunnel_state: &mut TunnelState,
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    udp_socket: &UdpSocket,
    first_src: SocketAddr,
    first_len: usize,
    recv_buf: &mut [u8],
    crypto_cap: usize,
    inbound_tx: &mpsc::Sender<Bytes>,
    inbound_batch_tx: &mpsc::Sender<Vec<Bytes>>,
    inbound_batch: &mut Vec<Bytes>,
) {
    // Re-use the actor-owned Vec; capacity (>= INBOUND_DRAIN_BATCH) survives
    // across readiness wakes.
    inbound_batch.clear();
    handle_inbound_datagram(
        sockets,
        endpoint_id,
        peers,
        tunnel_state,
        control_handle,
        tunnel_control,
        #[cfg(feature = "endpoint-amneziawg")]
        amnezia,
        first_src,
        &mut recv_buf[..first_len],
        crypto_cap,
        inbound_batch,
    )
    .await;

    for _ in 1..INBOUND_DRAIN_BATCH {
        match udp_socket.try_recv_from(recv_buf) {
            Ok((n, src)) => {
                handle_inbound_datagram(
                    sockets,
                    endpoint_id,
                    peers,
                    tunnel_state,
                    control_handle,
                    tunnel_control,
                    #[cfg(feature = "endpoint-amneziawg")]
                    amnezia,
                    src,
                    &mut recv_buf[..n],
                    crypto_cap,
                    inbound_batch,
                )
                .await;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => {
                warn!("wireguard udp recv: {err}");
                break;
            }
        }
    }

    flush_inbound_batch(inbound_tx, inbound_batch_tx, inbound_batch).await;
}

async fn handle_inbound_datagram(
    sockets: &TransportSockets,
    endpoint_id: &str,
    peers: &[Peer],
    tunnel_state: &mut TunnelState,
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    src: SocketAddr,
    datagram: &mut [u8],
    crypto_cap: usize,
    inbound_batch: &mut Vec<Bytes>,
) {
    let Some(peer_idx) = peers.iter().position(|peer| peer.endpoint() == src) else {
        warn!(
            "wireguard udp: dropped {} byte datagram from unknown source {src}",
            datagram.len()
        );
        return;
    };
    let peer = &peers[peer_idx];

    let mut datagram = datagram.to_vec();
    #[cfg(feature = "endpoint-amneziawg")]
    if let Some(options) = amnezia {
        match decode_inbound_packet(options, &mut datagram) {
            Ok(true) => {}
            Ok(false) => return,
            Err(err) => {
                tracing::trace!(?err, "wireguard amnezia inbound decode error");
                return;
            }
        }
    }

    // WARP-style 3-byte reserved header: sing-box clears it on the inbound
    // side before letting boringtun parse the frame (boringtun expects bytes
    // 1..4 to be zero).
    if peer.reserved() != [0u8; 3] && datagram.len() >= 4 {
        datagram[1] = 0;
        datagram[2] = 0;
        datagram[3] = 0;
    }
    let message_type = wireguard_message_type(&datagram);

    if let Some(message_type @ (1 | 2 | 3)) = message_type
        && let Some(control_handle) = control_handle
    {
        let result = control_handle.publish_event(WireguardPeerHandshakeReceivedArgs {
            endpoint_id: endpoint_id.to_owned(),
            peer_index: peer_idx,
            peer_endpoint: peer.endpoint(),
            peer_public_key: peer.public_key(),
            message_type,
            src_ip: src.ip(),
            datagram: Bytes::copy_from_slice(&datagram),
            inbound_control: tunnel_control.peer_inbound_control(peer_idx),
        });
        match result {
            Ok(()) => return,
            Err(err) => {
                warn!("wireguard inbound handshake event publish failed: {err}");
            }
        }
    }

    match tunnel_state {
        TunnelState::Legacy(peer_tunnels) => {
            let Some(peer_tunnel) = peer_tunnels.get_mut(peer_idx) else {
                warn!("wireguard udp: missing tunnel for peer index {peer_idx}");
                return;
            };

            // First call: pass the actual datagram + src ip (rate_limiter uses it).
            let action = step_decapsulate(peer, peer_tunnel, Some(src.ip()), &datagram, crypto_cap);
            if message_type == Some(4)
                && matches!(action, DecapAction::Idle)
                && let Some(control_handle) = control_handle
                && let Err(err) = control_handle.publish_event(WireguardPeerKeepaliveReceivedArgs {
                    endpoint_id: endpoint_id.to_owned(),
                    peer_index: peer_idx,
                    peer_endpoint: peer.endpoint(),
                    peer_public_key: peer.public_key(),
                })
            {
                warn!("wireguard keepalive event publish failed: {err}");
            }
            dispatch_action(sockets, peer, action, inbound_batch).await;

            // boringtun's contract: keep calling decapsulate(None, &[], buf) until
            // it returns Done so we drain any queued WriteToNetwork frames it staged
            // (e.g. the data packets that were waiting on a fresh handshake).
            loop {
                let action = step_decapsulate(peer, peer_tunnel, None, &[], crypto_cap);
                if matches!(action, DecapAction::Idle) {
                    break;
                }
                dispatch_action(sockets, peer, action, inbound_batch).await;
            }
        }
        TunnelState::Data(peer_data_tunnels) => {
            let Some(peer_data_tunnel) = peer_data_tunnels.get_mut(peer_idx) else {
                warn!("wireguard udp: missing data tunnel for peer index {peer_idx}");
                return;
            };
            let action = step_data_decapsulate(
                endpoint_id,
                peer_idx,
                peer,
                peer_data_tunnel,
                control_handle,
                tunnel_control,
                &datagram,
                crypto_cap,
            );
            if message_type == Some(4)
                && matches!(action, DecapAction::Idle)
                && let Some(control_handle) = control_handle
                && let Err(err) = control_handle.publish_event(WireguardPeerKeepaliveReceivedArgs {
                    endpoint_id: endpoint_id.to_owned(),
                    peer_index: peer_idx,
                    peer_endpoint: peer.endpoint(),
                    peer_public_key: peer.public_key(),
                })
            {
                warn!("wireguard keepalive event publish failed: {err}");
            }
            dispatch_action(sockets, peer, action, inbound_batch).await;
        }
    }
}

/// What `step_decapsulate` decided we should do with the bytes boringtun just
/// wrote into the per-call buffer. Each variant owns the buffer outright so
/// the dispatcher can hand it off (or `freeze()` it for the cross-actor mpsc)
/// without a memcpy.
#[derive(Debug)]
enum DecapAction {
    /// Nothing to send. Either boringtun consumed the datagram silently
    /// (`TunnResult::Done`).
    Idle,
    /// Boringtun rejected the datagram. Already trace-logged.
    Error,
    /// Send the encrypted frame in `buf` to `dst` over UDP. The buffer is
    /// already truncated to its real length; the WARP-reserved prefix gets
    /// stamped in place before the syscall.
    SendNetwork { buf: BytesMut, dst: SocketAddr },
    /// Stage a plaintext IP packet for the inner stack. Already frozen into a
    /// `Bytes`, so the eventual mpsc send is a zero-copy ownership move.
    SendInbound(Bytes),
}

async fn dispatch_action(
    sockets: &TransportSockets,
    peer: &Peer,
    action: DecapAction,
    inbound_batch: &mut Vec<Bytes>,
) {
    match action {
        DecapAction::Idle | DecapAction::Error => {}
        DecapAction::SendNetwork { mut buf, dst } => {
            stamp_reserved_in_place(&mut buf, peer.reserved());
            let Some(socket) = sockets.send_socket(dst) else {
                warn!("wireguard udp has no socket for {dst}");
                return;
            };
            if let Err(err) = socket.send_to(&buf, dst).await {
                warn!("wireguard udp send_to {dst}: {err}");
            }
        }
        DecapAction::SendInbound(bytes) => {
            inbound_batch.push(bytes);
        }
    }
}

/// Hand the staged batch off to the endpoint's ingress receiver. Each `Bytes`
/// goes in as its own mpsc message — the channel is sized (`INBOUND_QUEUE =
/// 256`) so a 32-packet readiness drain never has to block, and per-packet
/// granularity makes congestion drops localised instead of taking the whole
/// burst down. `batch` is reused across wakes; `drain(..)` keeps capacity.
async fn flush_inbound_batch(
    inbound_tx: &mpsc::Sender<Bytes>,
    inbound_batch_tx: &mpsc::Sender<Vec<Bytes>>,
    batch: &mut Vec<Bytes>,
) {
    if batch.is_empty() {
        return;
    }
    if batch.len() == 1 {
        if let Some(pkt) = batch.pop() {
            if inbound_tx.send(pkt).await.is_err() {
                warn!("wireguard inbound queue closed; dropped packet");
            }
        }
        return;
    }
    let outbound = std::mem::replace(batch, Vec::with_capacity(batch.capacity()));
    if inbound_batch_tx.send(outbound).await.is_err() {
        warn!("wireguard inbound batch queue closed; dropped packet batch");
    }
}

/// Allocate a packet-sized `BytesMut` without zeroing it.
///
/// boringtun's `encapsulate` / `decapsulate` only write into the destination
/// slice — `format_packet_data` (`session.rs`) builds the frame header,
/// `seal_in_place_separate_tag`s the payload, and copies the tag in; the
/// matching `handle_data` open-decrypts straight into `dst`. Neither path
/// reads the prior contents of `dst`, and every call site `truncate`s the
/// buffer to the actual written length before exposing it. So leaving the
/// uninit tail uninitialised is sound and saves one ~1432 B memset per packet
/// (~1.4 MB/s under sustained 1000 pkt/s video traffic — confirmed top
/// alloc/free site at 7.5% of NetExt CPU samples).
#[inline]
fn alloc_crypto_buf(cap: usize) -> BytesMut {
    let mut buf = BytesMut::with_capacity(cap);
    // SAFETY: see fn-level comment. boringtun is the only writer of these
    // bytes, and only `truncate`d prefix is ever observed. The uninit tail
    // is dropped before any reader sees it.
    unsafe { buf.set_len(cap) };
    buf
}

/// Run a single `Tunn::decapsulate` call. Allocates a
/// fresh `BytesMut` of `crypto_cap` bytes — boringtun's `decapsulate` API
/// requires a writable destination slice, and giving each call its own
/// buffer means `WriteToTunnelV4/V6` results can be `freeze()`d straight
/// into the inbound mpsc without copying.
fn step_decapsulate(
    peer: &Peer,
    tunnel: &mut PeerTunnel,
    src_ip: Option<IpAddr>,
    datagram: &[u8],
    crypto_cap: usize,
) -> DecapAction {
    let mut buf = alloc_crypto_buf(crypto_cap);
    let outcome = match tunnel.decapsulate(src_ip, datagram, &mut buf[..]) {
        TunnResult::Done => DecapOutcome::Idle,
        TunnResult::Err(err) => {
            // Includes routine cases (replays, handshake retries) so trace,
            // not warn.
            tracing::trace!(?err, "wireguard decapsulate error");
            DecapOutcome::Error
        }
        TunnResult::WriteToNetwork(out) => DecapOutcome::Network(out.len()),
        TunnResult::WriteToTunnelV4(out, _) | TunnResult::WriteToTunnelV6(out, _) => {
            DecapOutcome::Tunnel(out.len())
        }
    };
    match outcome {
        DecapOutcome::Idle => DecapAction::Idle,
        DecapOutcome::Error => DecapAction::Error,
        DecapOutcome::Network(len) => {
            buf.truncate(len);
            DecapAction::SendNetwork {
                buf,
                dst: peer.endpoint(),
            }
        }
        DecapOutcome::Tunnel(len) => {
            buf.truncate(len);
            DecapAction::SendInbound(buf.freeze())
        }
    }
}

fn step_data_decapsulate(
    endpoint_id: &str,
    peer_index: usize,
    peer: &Peer,
    tunnel: &mut PeerDataTunnel,
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    datagram: &[u8],
    crypto_cap: usize,
) -> DecapAction {
    let mut buf = alloc_crypto_buf(crypto_cap);
    let outcome = match tunnel.decapsulate(datagram, &mut buf[..]) {
        PeerDataResult::Done => {
            publish_packet_received(
                endpoint_id,
                peer_index,
                peer,
                control_handle,
                tunnel_control,
                0,
            );
            DecapOutcome::Idle
        }
        PeerDataResult::NeedHandshake => DecapOutcome::Idle,
        PeerDataResult::Err(err) => {
            tracing::trace!(?err, "wireguard data decapsulate error");
            DecapOutcome::Error
        }
        PeerDataResult::WriteToNetwork(out) => DecapOutcome::Network(out.len()),
        PeerDataResult::WriteToTunnelV4(out, _) | PeerDataResult::WriteToTunnelV6(out, _) => {
            let len = out.len();
            publish_packet_received(
                endpoint_id,
                peer_index,
                peer,
                control_handle,
                tunnel_control,
                len,
            );
            DecapOutcome::Tunnel(len)
        }
    };
    match outcome {
        DecapOutcome::Idle => DecapAction::Idle,
        DecapOutcome::Error => DecapAction::Error,
        DecapOutcome::Network(len) => {
            buf.truncate(len);
            DecapAction::SendNetwork {
                buf,
                dst: peer.endpoint(),
            }
        }
        DecapOutcome::Tunnel(len) => {
            buf.truncate(len);
            DecapAction::SendInbound(buf.freeze())
        }
    }
}

/// Local helper enum so we can stage `out.len()` out of `tunn.decapsulate`'s
/// borrow on `buf`, then mutate `buf` once the borrow ends.
enum DecapOutcome {
    Idle,
    Error,
    Network(usize),
    Tunnel(usize),
}

fn publish_packet_sent(
    endpoint_id: &str,
    peer_index: usize,
    peer: &Peer,
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    plaintext_len: usize,
) {
    let Some(control_handle) = control_handle else {
        return;
    };
    if let Err(err) = control_handle.publish_event(WireguardPeerPacketSentArgs {
        endpoint_id: endpoint_id.to_owned(),
        peer_index,
        peer_endpoint: peer.endpoint(),
        peer_public_key: peer.public_key(),
        plaintext_len,
        data_activity_control: tunnel_control.peer_data_activity_control(peer_index),
    }) {
        warn!("wireguard packet sent event publish failed: {err}");
    }
}

fn publish_start_handshake(
    endpoint_id: &str,
    peer_index: usize,
    peer: &Peer,
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
) {
    let Some(control_handle) = control_handle else {
        if let Err(err) = tunnel_control.start_handshake(peer_index) {
            warn!("wireguard direct start handshake failed: {err}");
        }
        return;
    };
    if let Err(err) = control_handle.publish_event(WireguardPeerStartArgs {
        endpoint_id: endpoint_id.to_owned(),
        peer_index,
        peer_endpoint: peer.endpoint(),
        peer_public_key: peer.public_key(),
        start_control: tunnel_control.peer_start_control(peer_index),
    }) {
        warn!("wireguard start event publish failed: {err}");
    }
}

fn publish_packet_received(
    endpoint_id: &str,
    peer_index: usize,
    peer: &Peer,
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    plaintext_len: usize,
) {
    let Some(control_handle) = control_handle else {
        return;
    };
    if let Err(err) = control_handle.publish_event(WireguardPeerPacketReceivedArgs {
        endpoint_id: endpoint_id.to_owned(),
        peer_index,
        peer_endpoint: peer.endpoint(),
        peer_public_key: peer.public_key(),
        plaintext_len,
        data_activity_control: tunnel_control.peer_data_activity_control(peer_index),
    }) {
        warn!("wireguard packet received event publish failed: {err}");
    }
}

/// Drain up to `OUTBOUND_DRAIN_BATCH` plaintext IP packets off the encrypt
/// channel after one `select!` wake. The actor owns the staging Vecs and
/// crypto output buffer so the common egress path does not allocate before it
/// can send the first packet.
///
/// `Tunn::encapsulate` mutates the actor-owned [`PeerTunnel`]. `socket.send_to`
/// is async, so the mutable tunnel borrow is kept inside a short block and is
/// released before awaiting UDP I/O.
async fn handle_outbound_batch(
    sockets: &TransportSockets,
    endpoint_id: &str,
    peers: &[Peer],
    tunnel_state: &mut TunnelState,
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    first: Bytes,
    encrypt_rx: &mut mpsc::Receiver<Bytes>,
    crypto_cap: usize,
    batch: &mut Vec<Bytes>,
    routed: &mut Vec<(usize, Bytes)>,
    send_buf: &mut BytesMut,
) {
    // Step 1: collect raw batch off the channel without re-entering select!.
    batch.clear();
    batch.push(first);
    for _ in 1..OUTBOUND_DRAIN_BATCH {
        match encrypt_rx.try_recv() {
            Ok(packet) => batch.push(packet),
            Err(_) => break, // empty or disconnected — surface on next select arm
        }
    }
    handle_outbound_packet_batch(
        sockets,
        endpoint_id,
        peers,
        tunnel_state,
        control_handle,
        tunnel_control,
        #[cfg(feature = "endpoint-amneziawg")]
        amnezia,
        batch,
        crypto_cap,
        routed,
        send_buf,
    )
    .await;
}

async fn handle_outbound_packet_batch(
    sockets: &TransportSockets,
    endpoint_id: &str,
    peers: &[Peer],
    tunnel_state: &mut TunnelState,
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    batch: &mut Vec<Bytes>,
    crypto_cap: usize,
    routed: &mut Vec<(usize, Bytes)>,
    send_buf: &mut BytesMut,
) {
    // Step 2: route each packet to its peer; drop malformed / unrouted ones now
    // so the per-peer encrypt loop doesn't have to re-check.
    routed.clear();
    for packet in batch.drain(..) {
        if packet.len() > crypto_cap.saturating_sub(WIREGUARD_OVERHEAD) {
            warn!(
                "wireguard outbound: IP packet length {} exceeds endpoint MTU {}, dropping",
                packet.len(),
                crypto_cap.saturating_sub(WIREGUARD_OVERHEAD)
            );
            continue;
        }
        let Some(dst_ip) = first_hop_destination(&packet) else {
            warn!("wireguard outbound: malformed IP packet, dropping");
            continue;
        };
        let Some(peer_idx) = route_outbound_peer(peers, dst_ip) else {
            warn!("wireguard outbound: no peer covers {dst_ip}, dropping");
            continue;
        };
        routed.push((peer_idx, packet));
    }

    // Step 3: walk routed in order, grouping consecutive same-peer packets
    // only to reuse endpoint/socket metadata. Packet order is preserved.
    let mut i = 0;
    while i < routed.len() {
        let peer_idx = routed[i].0;
        let peer = &peers[peer_idx];
        let dst = peer.endpoint();
        let reserved = peer.reserved();
        let Some(socket) = sockets.send_socket(dst) else {
            warn!("wireguard udp has no socket for {dst}");
            while i < routed.len() && routed[i].0 == peer_idx {
                i += 1;
            }
            continue;
        };

        while i < routed.len() && routed[i].0 == peer_idx {
            let plaintext_len = routed[i].1.len();
            let len = {
                unsafe { send_buf.set_len(crypto_cap) };
                match tunnel_state {
                    TunnelState::Legacy(peer_tunnels) => match peer_tunnels.get_mut(peer_idx) {
                        Some(tunnel) => match tunnel.encapsulate(&routed[i].1, &mut send_buf[..]) {
                            TunnResult::WriteToNetwork(out) => Some(out.len()),
                            TunnResult::Done => None,
                            TunnResult::Err(err) => {
                                tracing::trace!(?err, "wireguard encapsulate error");
                                None
                            }
                            // encapsulate never returns WriteToTunnel*, but match exhaustively.
                            TunnResult::WriteToTunnelV4(_, _)
                            | TunnResult::WriteToTunnelV6(_, _) => None,
                        },
                        None => {
                            warn!("wireguard outbound: missing tunnel for peer index {peer_idx}");
                            None
                        }
                    },
                    TunnelState::Data(peer_data_tunnels) => {
                        match peer_data_tunnels.get_mut(peer_idx) {
                            Some(tunnel) => {
                                match tunnel.encapsulate(&routed[i].1, &mut send_buf[..]) {
                                    PeerDataResult::WriteToNetwork(out) => Some(out.len()),
                                    PeerDataResult::NeedHandshake => {
                                        publish_start_handshake(
                                            endpoint_id,
                                            peer_idx,
                                            peer,
                                            control_handle,
                                            tunnel_control,
                                        );
                                        None
                                    }
                                    PeerDataResult::Done => None,
                                    PeerDataResult::Err(err) => {
                                        tracing::trace!(?err, "wireguard data encapsulate error");
                                        None
                                    }
                                    PeerDataResult::WriteToTunnelV4(_, _)
                                    | PeerDataResult::WriteToTunnelV6(_, _) => None,
                                }
                            }
                            None => {
                                warn!(
                                    "wireguard outbound: missing data tunnel for peer index {peer_idx}"
                                );
                                None
                            }
                        }
                    }
                }
            };
            i += 1;
            let Some(len) = len else {
                continue;
            };
            if matches!(tunnel_state, TunnelState::Data(_)) {
                publish_packet_sent(
                    endpoint_id,
                    peer_idx,
                    peer,
                    control_handle,
                    tunnel_control,
                    plaintext_len,
                );
            }
            send_buf.truncate(len);
            #[cfg(feature = "endpoint-amneziawg")]
            if let Some(options) = amnezia {
                let is_handshake = send_buf.first().is_some_and(|byte| matches!(*byte, 1 | 2));
                if is_handshake {
                    for junk in make_handshake_junk_packets(options) {
                        if let Err(err) = socket.send_to(&junk, dst).await {
                            warn!("wireguard amnezia junk send_to {dst}: {err}");
                        }
                    }
                }
                let mut encoded = send_buf.to_vec();
                encode_outbound_packet(options, &mut encoded);
                send_buf.clear();
                send_buf.extend_from_slice(&encoded);
            }
            stamp_reserved_in_place(send_buf, reserved);
            if let Err(err) = socket.send_to(&send_buf, dst).await {
                warn!("wireguard udp send_to {dst}: {err}");
            }
        }
    }
}

#[inline]
fn route_outbound_peer(peers: &[Peer], dst: IpAddr) -> Option<usize> {
    if peers.len() == 1 && peers[0].match_prefix(dst).is_some() {
        return Some(0);
    }
    peer::route_outbound(peers, dst)
}

async fn handle_tunnel_control_message(
    sockets: &TransportSockets,
    peers: &[Peer],
    peer_tunnels: &mut [PeerTunnel],
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    message: TunnelControlMessage,
    crypto_cap: usize,
) {
    match message {
        TunnelControlMessage::StartHandshake { peer_index } => {
            let Some((peer, mut packet)) = run_tunnel_control_action(
                peers,
                peer_tunnels,
                peer_index,
                crypto_cap,
                |tunnel, buf| tunnel.encapsulate(&[], buf),
            ) else {
                return;
            };
            send_tunnel_control_packet(
                sockets,
                peer,
                #[cfg(feature = "endpoint-amneziawg")]
                amnezia,
                &mut packet,
                #[cfg(feature = "endpoint-amneziawg")]
                true,
            )
            .await;
        }
        TunnelControlMessage::InboundControlDatagram {
            peer_index,
            src_ip,
            datagram,
        } => {
            let Some(peer) = peers.get(peer_index) else {
                warn!("wireguard control packet: peer index {peer_index} out of range");
                return;
            };
            let Some(peer_tunnel) = peer_tunnels.get_mut(peer_index) else {
                warn!("wireguard control packet: missing tunnel for peer index {peer_index}");
                return;
            };
            let mut inbound_batch = Vec::new();
            let action = step_decapsulate(peer, peer_tunnel, Some(src_ip), &datagram, crypto_cap);
            dispatch_action(sockets, peer, action, &mut inbound_batch).await;
            loop {
                let action = step_decapsulate(peer, peer_tunnel, None, &[], crypto_cap);
                if matches!(action, DecapAction::Idle) {
                    break;
                }
                dispatch_action(sockets, peer, action, &mut inbound_batch).await;
            }
            if !inbound_batch.is_empty() {
                warn!("wireguard inbound control unexpectedly produced tunnel packets");
            }
        }
        TunnelControlMessage::RekeyKeepaliveTimer => {
            for peer_index in 0..peers.len() {
                let Some((peer, mut packet)) = run_tunnel_control_action(
                    peers,
                    peer_tunnels,
                    peer_index,
                    crypto_cap,
                    |tunnel, buf| tunnel.update_timers(buf),
                ) else {
                    continue;
                };
                send_tunnel_control_packet(
                    sockets,
                    peer,
                    #[cfg(feature = "endpoint-amneziawg")]
                    amnezia,
                    &mut packet,
                    #[cfg(feature = "endpoint-amneziawg")]
                    false,
                )
                .await;
            }
        }
        TunnelControlMessage::DataPacketSent { .. }
        | TunnelControlMessage::DataPacketReceived { .. } => {}
    }
}

async fn handle_data_plane_command(
    sockets: &TransportSockets,
    endpoint_id: &str,
    peers: &[Peer],
    peer_data_tunnels: &mut [PeerDataTunnel],
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    command: DataPlaneCommand,
    crypto_cap: usize,
) {
    match command {
        DataPlaneCommand::SendNetwork {
            peer_index,
            packet,
            send_handshake_junk,
        } => {
            let Some(peer) = peers.get(peer_index) else {
                warn!("wireguard data command: peer index {peer_index} out of range");
                return;
            };
            #[cfg(not(feature = "endpoint-amneziawg"))]
            let _ = send_handshake_junk;
            let mut packet = BytesMut::from(&packet[..]);
            send_tunnel_control_packet(
                sockets,
                peer,
                #[cfg(feature = "endpoint-amneziawg")]
                amnezia,
                &mut packet,
                #[cfg(feature = "endpoint-amneziawg")]
                send_handshake_junk,
            )
            .await;
        }
        DataPlaneCommand::InstallSession {
            peer_index,
            generation,
            session,
            send_keepalive,
        } => {
            let Some(tunnel) = peer_data_tunnels.get_mut(peer_index) else {
                warn!("wireguard data command: missing data tunnel for peer index {peer_index}");
                return;
            };
            tunnel.install_session(PeerDataSessionUpdate {
                generation,
                session,
            });
            if send_keepalive {
                send_data_keepalive(
                    sockets,
                    endpoint_id,
                    peers,
                    peer_data_tunnels,
                    control_handle,
                    tunnel_control,
                    #[cfg(feature = "endpoint-amneziawg")]
                    amnezia,
                    peer_index,
                    crypto_cap,
                )
                .await;
            }
            drain_queued_data_packets(
                sockets,
                endpoint_id,
                peers,
                peer_data_tunnels,
                control_handle,
                tunnel_control,
                #[cfg(feature = "endpoint-amneziawg")]
                amnezia,
                peer_index,
                crypto_cap,
            )
            .await;
        }
        DataPlaneCommand::SendKeepalive { peer_index } => {
            send_data_keepalive(
                sockets,
                endpoint_id,
                peers,
                peer_data_tunnels,
                control_handle,
                tunnel_control,
                #[cfg(feature = "endpoint-amneziawg")]
                amnezia,
                peer_index,
                crypto_cap,
            )
            .await;
        }
        DataPlaneCommand::ExpireSession {
            peer_index,
            local_index,
            generation,
        } => {
            if let Some(tunnel) = peer_data_tunnels.get_mut(peer_index) {
                tunnel.expire_session(local_index, generation);
            }
        }
    }
}

async fn send_data_keepalive(
    sockets: &TransportSockets,
    endpoint_id: &str,
    peers: &[Peer],
    peer_data_tunnels: &mut [PeerDataTunnel],
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    peer_index: usize,
    crypto_cap: usize,
) {
    let Some(peer) = peers.get(peer_index) else {
        warn!("wireguard keepalive: peer index {peer_index} out of range");
        return;
    };
    let Some(tunnel) = peer_data_tunnels.get_mut(peer_index) else {
        warn!("wireguard keepalive: missing data tunnel for peer index {peer_index}");
        return;
    };
    let mut packet = alloc_crypto_buf(crypto_cap);
    let len = match tunnel.encapsulate_keepalive(&mut packet[..]) {
        PeerDataResult::WriteToNetwork(out) => out.len(),
        PeerDataResult::NeedHandshake => {
            publish_start_handshake(
                endpoint_id,
                peer_index,
                peer,
                control_handle,
                tunnel_control,
            );
            return;
        }
        PeerDataResult::Done => return,
        PeerDataResult::Err(err) => {
            tracing::trace!(?err, "wireguard keepalive encapsulate error");
            return;
        }
        PeerDataResult::WriteToTunnelV4(_, _) | PeerDataResult::WriteToTunnelV6(_, _) => return,
    };
    packet.truncate(len);
    publish_packet_sent(
        endpoint_id,
        peer_index,
        peer,
        control_handle,
        tunnel_control,
        0,
    );
    send_tunnel_control_packet(
        sockets,
        peer,
        #[cfg(feature = "endpoint-amneziawg")]
        amnezia,
        &mut packet,
        #[cfg(feature = "endpoint-amneziawg")]
        false,
    )
    .await;
}

async fn drain_queued_data_packets(
    sockets: &TransportSockets,
    endpoint_id: &str,
    peers: &[Peer],
    peer_data_tunnels: &mut [PeerDataTunnel],
    control_handle: Option<&ControlThreadHandle>,
    tunnel_control: &TunnelControl,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    peer_index: usize,
    crypto_cap: usize,
) {
    let Some(peer) = peers.get(peer_index) else {
        return;
    };
    for _ in 0..OUTBOUND_DRAIN_BATCH {
        let Some(tunnel) = peer_data_tunnels.get_mut(peer_index) else {
            return;
        };
        let mut packet = alloc_crypto_buf(crypto_cap);
        let (result, plaintext_len) = tunnel.encapsulate_next_queued_with_len(&mut packet[..]);
        let len = match result {
            PeerDataResult::WriteToNetwork(out) => out.len(),
            PeerDataResult::Done => return,
            PeerDataResult::NeedHandshake => {
                publish_start_handshake(
                    endpoint_id,
                    peer_index,
                    peer,
                    control_handle,
                    tunnel_control,
                );
                return;
            }
            PeerDataResult::Err(err) => {
                tracing::trace!(?err, "wireguard queued data encapsulate error");
                return;
            }
            PeerDataResult::WriteToTunnelV4(_, _) | PeerDataResult::WriteToTunnelV6(_, _) => {
                return;
            }
        };
        packet.truncate(len);
        publish_packet_sent(
            endpoint_id,
            peer_index,
            peer,
            control_handle,
            tunnel_control,
            plaintext_len,
        );
        send_tunnel_control_packet(
            sockets,
            peer,
            #[cfg(feature = "endpoint-amneziawg")]
            amnezia,
            &mut packet,
            #[cfg(feature = "endpoint-amneziawg")]
            false,
        )
        .await;
    }
}

fn run_tunnel_control_action<'a>(
    peers: &'a [Peer],
    peer_tunnels: &mut [PeerTunnel],
    peer_index: usize,
    crypto_cap: usize,
    action: impl for<'buf> FnOnce(&mut PeerTunnel, &'buf mut [u8]) -> TunnResult<'buf>,
) -> Option<(&'a Peer, BytesMut)> {
    let Some(peer) = peers.get(peer_index) else {
        warn!("wireguard control packet: peer index {peer_index} out of range");
        return None;
    };
    let Some(tunnel) = peer_tunnels.get_mut(peer_index) else {
        warn!("wireguard control packet: missing tunnel for peer index {peer_index}");
        return None;
    };
    let mut packet = alloc_crypto_buf(crypto_cap);
    let len = match action(tunnel, &mut packet[..]) {
        TunnResult::WriteToNetwork(out) => Some(out.len()),
        TunnResult::Done => None,
        TunnResult::Err(err) => {
            tracing::trace!(?err, "wireguard control tunnel action error");
            None
        }
        TunnResult::WriteToTunnelV4(_, _) | TunnResult::WriteToTunnelV6(_, _) => None,
    };
    if let Some(len) = len {
        packet.truncate(len);
        Some((peer, packet))
    } else {
        None
    }
}

async fn send_tunnel_control_packet(
    sockets: &TransportSockets,
    peer: &Peer,
    #[cfg(feature = "endpoint-amneziawg")] amnezia: Option<&Amnezia2Options>,
    packet: &mut BytesMut,
    #[cfg(feature = "endpoint-amneziawg")] send_handshake_junk: bool,
) {
    let dst = peer.endpoint();
    let Some(socket) = sockets.send_socket(dst) else {
        warn!("wireguard udp has no socket for {dst}");
        return;
    };
    #[cfg(feature = "endpoint-amneziawg")]
    if let Some(options) = amnezia {
        if send_handshake_junk {
            for junk in make_handshake_junk_packets(options) {
                if let Err(err) = socket.send_to(&junk, dst).await {
                    warn!("wireguard amnezia junk send_to {dst}: {err}");
                }
            }
        }
        let mut encoded = packet.to_vec();
        encode_outbound_packet(options, &mut encoded);
        packet.clear();
        packet.extend_from_slice(&encoded);
    }
    stamp_reserved_in_place(packet, peer.reserved());
    if let Err(err) = socket.send_to(packet, dst).await {
        warn!("wireguard control packet send {dst}: {err}");
    }
}

fn wireguard_message_type(packet: &[u8]) -> Option<u8> {
    if packet.len() < 4 || packet[1..4] != [0, 0, 0] {
        return None;
    }
    match packet[0] {
        message_type @ 1..=4 => Some(message_type),
        _ => None,
    }
}

/// Stamp the WARP-style 3-byte reserved prefix into `packet[1..4]`. boringtun
/// only writes the first byte (message type); the next three are ours.
#[inline]
fn stamp_reserved_in_place(packet: &mut [u8], reserved: [u8; 3]) {
    if reserved != [0u8; 3] && packet.len() >= 4 {
        packet[1..4].copy_from_slice(&reserved);
    }
}

/// Read the destination IP out of an IPv4 or IPv6 header. Returns `None` for
/// truncated or unrecognized packets.
fn first_hop_destination(packet: &[u8]) -> Option<IpAddr> {
    let version = *packet.first()? >> 4;
    match version {
        4 if packet.len() >= 20 => {
            let mut octets = [0u8; 4];
            octets.copy_from_slice(&packet[16..20]);
            Some(IpAddr::from(octets))
        }
        6 if packet.len() >= 40 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&packet[24..40]);
            Some(IpAddr::from(octets))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::time::Instant;

    use boringtun::x25519;
    use ipnet::IpNet;
    use tokio::time::timeout;

    use crate::ControlThread;
    use hammer_core::config::WireguardPeerOptions;
    use hammer_core::log::Level;
    use hammer_core::log::{DiscardWriter, Factory};
    use hammer_core::metrics::MetricsRegistry;

    fn logger(id: &'static str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    fn x25519_public(secret: [u8; 32]) -> [u8; 32] {
        x25519::PublicKey::from(&x25519::StaticSecret::from(secret)).to_bytes()
    }

    /// Reserve a localhost UDP port by binding-and-dropping a std socket. The
    /// port may technically be reused before our tokio socket grabs it, but on
    /// localhost ephemeral the window is tiny and good enough for this test.
    fn ephemeral_port() -> u16 {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral");
        socket.local_addr().expect("local_addr").port()
    }

    fn make_peer(
        peer_pub: [u8; 32],
        local_priv: [u8; 32],
        endpoint: SocketAddr,
        index: u32,
    ) -> (Peer, PeerTunnel) {
        let opts = WireguardPeerOptions {
            public_key: peer_pub,
            pre_shared_key: None,
            endpoint,
            allowed_ips: vec!["10.0.0.0/8".parse::<IpNet>().unwrap()],
            persistent_keepalive: None,
            reserved: [0; 3],
        };
        let peer = Peer::from_options(&opts);
        let tunnel = PeerTunnel::new(
            &opts,
            &x25519::StaticSecret::from(local_priv),
            index,
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        );
        (peer, tunnel)
    }

    /// Minimal 60-byte IPv4 datagram targeting 10.0.0.5 — boringtun doesn't
    /// validate L3 checksums or transport headers, it just shovels bytes.
    fn dummy_ipv4_packet() -> Vec<u8> {
        let mut pkt = vec![0u8; 60];
        pkt[0] = 0x45; // version 4, IHL 5
        pkt[2..4].copy_from_slice(&60u16.to_be_bytes());
        pkt[8] = 64; // TTL
        pkt[9] = 17; // UDP next-header (any non-zero will do)
        pkt[12..16].copy_from_slice(&[10, 0, 0, 1]);
        pkt[16..20].copy_from_slice(&[10, 0, 0, 5]);
        pkt
    }

    async fn recv_transport_packet(handles: &mut TransportHandles) -> Option<Bytes> {
        tokio::select! {
            packet = handles.inbound_rx.recv() => packet,
            batch = handles.inbound_batch_rx.recv() => {
                batch.and_then(|mut packets| {
                    if packets.is_empty() {
                        None
                    } else {
                        Some(packets.remove(0))
                    }
                })
            }
        }
    }

    #[tokio::test]
    async fn flush_inbound_batch_sends_one_packet() {
        let (tx, mut rx) = mpsc::channel(1);
        let (batch_tx, mut batch_rx) = mpsc::channel(1);
        let packet = Bytes::from_static(b"one");
        let mut batch = vec![packet.clone()];

        flush_inbound_batch(&tx, &batch_tx, &mut batch).await;

        let received = rx.try_recv().expect("one inbound message");
        assert_eq!(received, packet);
        assert!(rx.try_recv().is_err(), "only one packet pushed");
        assert!(batch_rx.try_recv().is_err(), "no batch for one packet");
        assert!(batch.is_empty(), "batch must be drained for the next wake");
    }

    #[tokio::test]
    async fn flush_inbound_batch_pushes_multiple_packets_as_one_batch() {
        let (tx, mut rx) = mpsc::channel(4);
        let (batch_tx, mut batch_rx) = mpsc::channel(4);
        let first = Bytes::from_static(b"one");
        let second = Bytes::from_static(b"two");
        let mut batch = vec![first.clone(), second.clone()];
        let original_capacity = batch.capacity();

        flush_inbound_batch(&tx, &batch_tx, &mut batch).await;

        assert!(rx.try_recv().is_err());
        let received = batch_rx.try_recv().expect("batched packets");
        assert_eq!(received, vec![first, second]);
        assert!(batch_rx.try_recv().is_err());
        assert!(batch.is_empty(), "batch must be drained for reuse");
        assert!(
            batch.capacity() >= original_capacity,
            "batch capacity must persist across wakes (got {}, was {original_capacity})",
            batch.capacity(),
        );
    }

    /// End-to-end smoke test: two transport actors configured as each other's
    /// peer must complete the noise handshake over real UDP and surface a
    /// queued IP packet to the other side's `inbound_rx`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn transport_round_trip_through_udp() {
        let a_priv = [11u8; 32];
        let b_priv = [22u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let port_a = ephemeral_port();
        let port_b = ephemeral_port();
        let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_a);
        let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_b);

        let (peer_a, tunnel_a) = make_peer(b_pub, a_priv, addr_b, 0);
        let (peer_b, tunnel_b) = make_peer(a_pub, b_priv, addr_a, 0);

        let handles_a = spawn_transport(
            logger("transport-a"),
            "transport-a".to_owned(),
            Arc::new(vec![peer_a]),
            vec![tunnel_a],
            port_a,
            1408,
            SocketProtector::default(),
            None,
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn A");
        let mut handles_b = spawn_transport(
            logger("transport-b"),
            "transport-b".to_owned(),
            Arc::new(vec![peer_b]),
            vec![tunnel_b],
            port_b,
            1408,
            SocketProtector::default(),
            None,
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn B");
        assert_eq!(handles_a.local_addr.port(), port_a);
        assert_eq!(handles_b.local_addr.port(), port_b);

        let payload = dummy_ipv4_packet();
        handles_a
            .encrypt_tx
            .send(Bytes::from(payload.clone()))
            .await
            .expect("encrypt_tx");

        // Five seconds is generous: the handshake completes in <50 ms over
        // localhost, even with the 250 ms timer driving retransmits.
        let recovered = timeout(
            Duration::from_secs(5),
            recv_transport_packet(&mut handles_b),
        )
        .await
        .expect("timed out waiting for inbound packet")
        .expect("inbound_rx closed before delivering");
        assert_eq!(&recovered[..], payload.as_slice());

        // Dropping the handles closes both mpsc channels and the oneshot;
        // the actor's select! loop notices and exits cleanly.
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_control_split_round_trip_through_udp() {
        let inner = Arc::new(DiscardWriter);
        let (control_thread_handle, control_thread) = ControlThread::new(
            Instant::now(),
            inner,
            MetricsRegistry::new(),
            Duration::from_secs(60),
            Level::Info,
        );
        let control_join = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("control thread runtime");
            runtime.block_on(control_thread.run());
        });
        let mut _subscriptions = build_wireguard_start_handshake_subscriber(
            logger("transport-control-split"),
            Arc::clone(&control_thread_handle),
        )
        .expect("build wireguard start subscriber");
        _subscriptions.extend(
            build_wireguard_inbound_control_subscriber(
                logger("transport-control-split"),
                Arc::clone(&control_thread_handle),
            )
            .expect("build wireguard inbound subscriber"),
        );

        let a_priv = [111u8; 32];
        let b_priv = [122u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let port_a = ephemeral_port();
        let port_b = ephemeral_port();
        let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_a);
        let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_b);

        let (peer_a, tunnel_a) = make_peer(b_pub, a_priv, addr_b, 0);
        let (peer_b, tunnel_b) = make_peer(a_pub, b_priv, addr_a, 0);

        let handles_a = spawn_transport(
            logger("transport-control-split-a"),
            "transport-control-split-a".to_owned(),
            Arc::new(vec![peer_a]),
            vec![tunnel_a],
            port_a,
            1408,
            SocketProtector::default(),
            Some(Arc::clone(&control_thread_handle)),
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn A");
        let mut handles_b = spawn_transport(
            logger("transport-control-split-b"),
            "transport-control-split-b".to_owned(),
            Arc::new(vec![peer_b]),
            vec![tunnel_b],
            port_b,
            1408,
            SocketProtector::default(),
            Some(Arc::clone(&control_thread_handle)),
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn B");

        let payload = dummy_ipv4_packet();
        handles_a
            .encrypt_tx
            .send(Bytes::from(payload.clone()))
            .await
            .expect("encrypt_tx");

        let recovered = timeout(
            Duration::from_secs(5),
            recv_transport_packet(&mut handles_b),
        )
        .await
        .expect("timed out waiting for split inbound packet")
        .expect("inbound_rx closed before delivering");
        assert_eq!(&recovered[..], payload.as_slice());

        let _ = handles_a.shutdown.send(());
        let _ = handles_b.shutdown.send(());
        assert!(control_thread_handle.shutdown_timeout(Duration::from_secs(1)));
        control_join.join().expect("control thread join");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_registers_rekey_keepalive_timer_on_control_thread() {
        let inner = Arc::new(DiscardWriter);
        let (control_thread_handle, control_thread) = ControlThread::new(
            Instant::now(),
            inner,
            MetricsRegistry::new(),
            Duration::from_secs(60),
            Level::Info,
        );
        let control_join = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("control thread runtime");
            runtime.block_on(control_thread.run());
        });

        let local_priv = [23u8; 32];
        let peer_pub = x25519_public([24u8; 32]);
        let (peer, tunnel) = make_peer(
            peer_pub,
            local_priv,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ephemeral_port()),
            0,
        );

        let handles = spawn_transport(
            logger("transport-control-rekey-keepalive-timer"),
            "transport-control-rekey-keepalive-timer".to_owned(),
            Arc::new(vec![peer]),
            vec![tunnel],
            0,
            1408,
            SocketProtector::default(),
            Some(Arc::clone(&control_thread_handle)),
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn transport with control rekey/keepalive timer");

        assert!(
            handles.has_control_rekey_keepalive_timer_for_test(),
            "wireguard endpoint rekey/keepalive timer must be registered on the control thread"
        );

        let _ = handles.shutdown.send(());
        assert!(control_thread_handle.shutdown_timeout(Duration::from_secs(1)));
        control_join.join().expect("control thread join");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inbound_handshake_event_updates_tunnel_state() {
        let inner = Arc::new(DiscardWriter);
        let (control_thread_handle, control_thread) = ControlThread::new(
            Instant::now(),
            inner,
            MetricsRegistry::new(),
            Duration::from_secs(60),
            Level::Info,
        );
        let control_join = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("control thread runtime");
            runtime.block_on(control_thread.run());
        });
        let _subscriptions = build_wireguard_inbound_control_subscriber(
            logger("transport-inbound-control"),
            Arc::clone(&control_thread_handle),
        )
        .expect("build wireguard inbound subscriber");

        let local_priv = [123u8; 32];
        let remote_priv = [124u8; 32];
        let local_pub = x25519_public(local_priv);
        let remote_pub = x25519_public(remote_priv);
        let remote_socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .expect("bind remote socket");
        let remote_addr = remote_socket.local_addr().expect("remote socket addr");

        let (peer, tunnel) = make_peer(remote_pub, local_priv, remote_addr, 0);
        let handles = spawn_transport(
            logger("transport-inbound-control"),
            "transport-inbound-control".to_owned(),
            Arc::new(vec![peer]),
            vec![tunnel],
            0,
            1408,
            SocketProtector::default(),
            Some(Arc::clone(&control_thread_handle)),
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn transport with inbound control event");
        let transport_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), handles.local_addr.port());

        let (_remote_peer, mut remote_tunnel) =
            make_peer(local_pub, remote_priv, transport_addr, 0);
        let mut init_buf = vec![0u8; 2048];
        let init = match remote_tunnel.encapsulate(&[], &mut init_buf) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("remote expected handshake init, got {other:?}"),
        };
        remote_socket
            .send_to(&init, transport_addr)
            .await
            .expect("send handshake init");

        let mut recv_buf = [0u8; 2048];
        let (n, _) = timeout(
            Duration::from_secs(2),
            remote_socket.recv_from(&mut recv_buf),
        )
        .await
        .expect("timed out waiting for handshake response")
        .expect("receive handshake response");
        assert!(n >= 4, "handshake response must include a header");
        assert_eq!(recv_buf[0], 2, "inbound event must update Tunn state");

        let _ = handles.shutdown.send(());
        assert!(control_thread_handle.shutdown_timeout(Duration::from_secs(1)));
        control_join.join().expect("control thread join");
    }

    /// Push a burst of IP packets through `encrypt_tx` in quick succession so
    /// the actor's `try_recv`-driven `handle_outbound_batch` path is exercised.
    /// All packets must reach the peer's `inbound_rx`. The point is to prove
    /// the batched encrypt path doesn't drop or reorder under concurrent
    /// submission, including across the noise handshake boundary.
    ///
    /// `worker_threads = 2` keeps the test cheap when cargo runs the wireguard
    /// suite in parallel — each tokio::test starts its own runtime, so an
    /// over-sized worker pool here would push the process near OS thread
    /// limits and starve more timing-sensitive tests like
    /// `tunnel_tcp_preserves_large_backpressured_stream`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_outbound_burst_delivers_every_packet() {
        const BURST: usize = 16;

        let a_priv = [33u8; 32];
        let b_priv = [44u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let port_a = ephemeral_port();
        let port_b = ephemeral_port();
        let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_a);
        let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_b);

        let (peer_a, tunnel_a) = make_peer(b_pub, a_priv, addr_b, 0);
        let (peer_b, tunnel_b) = make_peer(a_pub, b_priv, addr_a, 0);

        let handles_a = spawn_transport(
            logger("transport-a-burst"),
            "transport-a-burst".to_owned(),
            Arc::new(vec![peer_a]),
            vec![tunnel_a],
            port_a,
            1408,
            SocketProtector::default(),
            None,
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn A");
        let mut handles_b = spawn_transport(
            logger("transport-b-burst"),
            "transport-b-burst".to_owned(),
            Arc::new(vec![peer_b]),
            vec![tunnel_b],
            port_b,
            1408,
            SocketProtector::default(),
            None,
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn B");

        let payload = dummy_ipv4_packet();
        for _ in 0..BURST {
            handles_a
                .encrypt_tx
                .send(Bytes::from(payload.clone()))
                .await
                .expect("encrypt_tx");
        }

        let mut received = 0usize;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while received < BURST {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let delivered = timeout(remaining, async {
                tokio::select! {
                    packet = handles_b.inbound_rx.recv() => {
                        packet.map(|packet| vec![packet])
                    }
                    batch = handles_b.inbound_batch_rx.recv() => batch,
                }
            })
            .await
            .expect("timed out waiting for inbound burst packets")
            .expect("inbound channels closed before delivering all burst packets");
            for packet in delivered {
                assert_eq!(&packet[..], payload.as_slice());
                received += 1;
            }
        }
        assert_eq!(received, BURST);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_drops_plain_packet_larger_than_endpoint_mtu() {
        let a_priv = [55u8; 32];
        let b_pub = x25519_public([66u8; 32]);
        let (peer, tunnel) = make_peer(
            b_pub,
            a_priv,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ephemeral_port()),
            0,
        );
        let peers = vec![peer];
        let mut tunnel_state = TunnelState::Legacy(vec![tunnel]);
        let (control_tx, _control_rx) = mpsc::channel(TUNNEL_CONTROL_QUEUE);
        let tunnel_control = TunnelControl::new(control_tx);
        let sockets =
            bind_transport_sockets(&peers, 0, &SocketProtector::default()).expect("bind sockets");
        let (_tx, mut encrypt_rx) = mpsc::channel(1);
        let mut batch = Vec::new();
        let mut routed = Vec::new();
        let mut send_buf = BytesMut::with_capacity(64 + WIREGUARD_OVERHEAD);
        let mut packet = dummy_ipv4_packet();
        packet.resize(65, 0);

        handle_outbound_batch(
            &sockets,
            "transport-mtu",
            &peers,
            &mut tunnel_state,
            None,
            &tunnel_control,
            #[cfg(feature = "endpoint-amneziawg")]
            None,
            Bytes::from(packet),
            &mut encrypt_rx,
            64 + WIREGUARD_OVERHEAD,
            &mut batch,
            &mut routed,
            &mut send_buf,
        )
        .await;

        assert!(
            routed.is_empty(),
            "oversized packet must be dropped before encapsulation"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ephemeral_reset_bind_failure_keeps_existing_sockets() {
        let local_priv = [67u8; 32];
        let peer_pub = x25519_public([68u8; 32]);
        let (peer, _tunnel) = make_peer(
            peer_pub,
            local_priv,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ephemeral_port()),
            0,
        );
        let peers = vec![peer];
        let mut sockets =
            bind_transport_sockets(&peers, 0, &SocketProtector::default()).expect("bind sockets");
        let before = sockets.local_addr().expect("local addr before reset");

        let err = reset_transport_sockets_with(&mut sockets, 0, || {
            Err(HammerError::internal("injected bind failure"))
        })
        .expect_err("reset bind should fail");

        assert!(
            err.to_string().contains("injected bind failure"),
            "error = {err:?}"
        );
        assert_eq!(
            sockets.local_addr().expect("local addr after reset"),
            before,
            "failed ephemeral reset must keep the running socket"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_binds_ipv6_socket_for_ipv6_peer_endpoint() {
        let a_priv = [77u8; 32];
        let b_priv = [88u8; 32];
        let b_pub = x25519_public(b_priv);
        let (peer, tunnel) = make_peer(
            b_pub,
            a_priv,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 51820),
            0,
        );

        let handles = spawn_transport(
            logger("transport-v6"),
            "transport-v6".to_owned(),
            Arc::new(vec![peer]),
            vec![tunnel],
            0,
            1408,
            SocketProtector::default(),
            None,
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn v6 transport");

        assert!(
            handles.local_addr.is_ipv6(),
            "IPv6 peer endpoints need an IPv6 UDP socket, got {}",
            handles.local_addr
        );

        let _ = handles.shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_keeps_ipv4_socket_available_for_mixed_peer_endpoints() {
        let local_priv = [99u8; 32];
        let v4_pub = x25519_public([100u8; 32]);
        let v6_pub = x25519_public([101u8; 32]);
        let (v4_peer, v4_tunnel) = make_peer(
            v4_pub,
            local_priv,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51820),
            0,
        );
        let (v6_peer, v6_tunnel) = make_peer(
            v6_pub,
            local_priv,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 51821),
            1,
        );

        let handles = spawn_transport(
            logger("transport-mixed"),
            "transport-mixed".to_owned(),
            Arc::new(vec![v4_peer, v6_peer]),
            vec![v4_tunnel, v6_tunnel],
            0,
            1408,
            SocketProtector::default(),
            None,
            #[cfg(feature = "endpoint-amneziawg")]
            None,
        )
        .expect("spawn mixed transport");

        assert!(
            handles.local_addr.is_ipv4(),
            "mixed peers must keep an IPv4 UDP socket for IPv4 endpoints, got {}",
            handles.local_addr
        );

        let _ = handles.shutdown.send(());
    }
}
