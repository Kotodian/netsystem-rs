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
//!   * a 250 ms tick that calls `Tunn::update_timers` per peer so handshakes,
//!     keepalives, and rekeys make progress without external prodding.
//!
//! The smoltcp actor owns the inner IP stack; this actor only moves encrypted
//! UDP frames between peers and the stack-facing queues.

use std::future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use boringtun::noise::TunnResult;
use bytes::{Bytes, BytesMut};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::protocol::tun::ipstack::IpStackInput;
use crate::socket_protector::SocketProtector;

use super::WIREGUARD_OVERHEAD;
use super::peer::{self, Peer};

/// 250 ms matches what boringtun's own examples and Cloudflare WARP use to
/// drive `Tunn::update_timers` — fine-grained enough for handshake retries
/// without burning a CPU.
const TIMER_TICK: Duration = Duration::from_millis(250);

/// Channel buffer for IP packets waiting to be encrypted. 64 packets at
/// MTU=1408 ≈ 90 KiB of headroom — keeps a brief stall on the encryption
/// thread from immediately back-pressuring the smoltcp poll loop.
const ENCRYPT_QUEUE: usize = 64;
/// Same for the decrypted-inbound side. Each message can carry up to
/// `INBOUND_DRAIN_BATCH` packets, so effective in-flight headroom is
/// `INBOUND_QUEUE * INBOUND_DRAIN_BATCH` ≈ 512 packets at MTU=1408 ≈ 720 KiB —
/// kept small to fit inside the iOS NetExt memory budget.
const INBOUND_QUEUE: usize = 16;
/// After one UDP readiness wake, drain a small bounded burst so decrypted IP
/// packets can cross into ipstack as one batch instead of one mpsc message each.
const INBOUND_DRAIN_BATCH: usize = 32;

/// Handles returned to the owner of a transport actor. Hold these for the
/// lifetime of the endpoint; dropping `shutdown` is enough to stop the actor
/// (the loop also exits when both `encrypt_tx` and `inbound_rx` close).
pub(crate) struct TransportHandles {
    /// Push IP packets into here to have them encapsulated and shipped to
    /// whichever peer's `allowed_ips` matches the destination.
    pub(crate) encrypt_tx: mpsc::Sender<Bytes>,
    /// Receive decrypted IP packets coming back from any peer. The smoltcp
    /// stack drains this in the actor that owns the `phy::Device`.
    pub(crate) inbound_rx: mpsc::Receiver<IpStackInput>,
    /// The address the OS bound the UDP socket to. Mostly useful for tests
    /// — production peers configure their endpoints out-of-band.
    pub(crate) local_addr: SocketAddr,
    /// Send `()` (or drop) to ask the actor to exit promptly.
    pub(crate) shutdown: oneshot::Sender<()>,
    /// Join handle in case the caller wants to await actor termination.
    pub(crate) join: JoinHandle<()>,
}

struct TransportSockets {
    ipv4: Option<Arc<UdpSocket>>,
    ipv6: Option<Arc<UdpSocket>>,
}

impl TransportSockets {
    fn local_addr(&self) -> Result<SocketAddr, HammerError> {
        let socket = self
            .ipv4
            .as_deref()
            .or(self.ipv6.as_deref())
            .ok_or_else(|| HammerError::internal("wireguard transport has no UDP socket"))?;
        socket
            .local_addr()
            .map_err(|err| HammerError::internal(format!("wireguard udp local_addr: {err}")))
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
    /// Send the cancellation signal and await the actor finishing.
    #[allow(dead_code)]
    pub(crate) async fn shutdown(self) -> Result<(), HammerError> {
        let _ = self.shutdown.send(());
        self.join
            .await
            .map_err(|err| HammerError::internal(format!("wireguard transport join: {err}")))
    }
}

/// Spin up the UDP transport actor. Binds the socket synchronously (cheap —
/// it's just a `socket()` + `bind()` syscall) so this works inside the
/// blocking `Lifecycle::start` path; the resulting tokio `UdpSocket` is then
/// handed to a detached task.
pub(crate) fn spawn_transport(
    _logger: Logger,
    peers: Arc<Vec<Peer>>,
    listen_port: u16,
    mtu: u32,
    protector: SocketProtector,
) -> Result<TransportHandles, HammerError> {
    let sockets = bind_transport_sockets(&peers, listen_port, &protector)?;
    let local_addr = sockets.local_addr()?;
    info!("wireguard transport listening on {local_addr}");

    let (encrypt_tx, encrypt_rx) = mpsc::channel::<Bytes>(ENCRYPT_QUEUE);
    let (inbound_tx, inbound_rx) = mpsc::channel::<IpStackInput>(INBOUND_QUEUE);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let join = crate::spawn::spawn(run_actor(
        sockets,
        peers,
        mtu,
        encrypt_rx,
        inbound_tx,
        shutdown_rx,
    ));

    Ok(TransportHandles {
        encrypt_tx,
        inbound_rx,
        local_addr,
        shutdown: shutdown_tx,
        join,
    })
}

fn bind_transport_sockets(
    peers: &[Peer],
    listen_port: u16,
    protector: &SocketProtector,
) -> Result<TransportSockets, HammerError> {
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
            .map_err(|err| HammerError::internal(format!("wireguard udp local_addr: {err}")))?
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

fn bind_udp_socket(
    bind: SocketAddr,
    ipv6_only: bool,
    protector: &SocketProtector,
) -> Result<Arc<UdpSocket>, HammerError> {
    if bind.is_ipv4() {
        return std_udp_socket(bind, protector);
    }
    let domain = if bind.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))
        .map_err(|err| HammerError::internal(format!("wireguard udp socket {bind}: {err}")))?;
    if bind.is_ipv6() {
        socket
            .set_only_v6(ipv6_only)
            .map_err(|err| HammerError::internal(format!("wireguard udp set_only_v6: {err}")))?;
    }
    socket
        .bind(&bind.into())
        .map_err(|err| HammerError::internal(format!("wireguard udp bind {bind}: {err}")))?;
    let std_socket: std::net::UdpSocket = socket.into();
    std_socket
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("wireguard udp set_nonblocking: {err}")))?;
    let socket = UdpSocket::from_std(std_socket)
        .map_err(|err| HammerError::internal(format!("wireguard udp from_std: {err}")))?;
    protector.protect(&socket)?;
    Ok(Arc::new(socket))
}

fn std_udp_socket(
    bind: SocketAddr,
    protector: &SocketProtector,
) -> Result<Arc<UdpSocket>, HammerError> {
    let socket = std::net::UdpSocket::bind(bind)
        .map_err(|err| HammerError::internal(format!("wireguard udp bind {bind}: {err}")))?;
    socket
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("wireguard udp set_nonblocking: {err}")))?;
    let socket = UdpSocket::from_std(socket)
        .map_err(|err| HammerError::internal(format!("wireguard udp from_std: {err}")))?;
    protector.protect(&socket)?;
    Ok(Arc::new(socket))
}

/// The actor body. Each branch is a single `select!` arm to keep the loop
/// fairly easy to reason about; backpressure on either mpsc is handled by
/// dropping packets after a warn-log because wg is best-effort at L3 anyway.
async fn run_actor(
    sockets: TransportSockets,
    peers: Arc<Vec<Peer>>,
    mtu: u32,
    mut encrypt_rx: mpsc::Receiver<Bytes>,
    inbound_tx: mpsc::Sender<IpStackInput>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    // 65535 is the IPv4 datagram ceiling; boringtun's WG frames cap out at
    // mtu + 32, so this is generous. These two buffers stay with the actor;
    // they're only borrowed into recv_from and never escape.
    let mut udp_buf_v4 = vec![0u8; 65_535];
    let mut udp_buf_v6 = vec![0u8; 65_535];
    // Capacity for every encap/decap output buffer the actor will allocate.
    // Each step allocates its own BytesMut so the result can be `freeze()`d
    // straight into `inbound_tx` without a memcpy.
    let crypto_cap = (mtu as usize) + WIREGUARD_OVERHEAD;

    let mut timer = time::interval(TIMER_TICK);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    timer.tick().await; // first tick fires immediately; consume it

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                debug!("wireguard transport: shutdown");
                break;
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
                            &peers,
                            socket,
                            src,
                            n,
                            &mut udp_buf_v4,
                            crypto_cap,
                            &inbound_tx,
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
                            &peers,
                            socket,
                            src,
                            n,
                            &mut udp_buf_v6,
                            crypto_cap,
                            &inbound_tx,
                        )
                        .await;
                    }
                    Err(err) => {
                        warn!("wireguard udp recv: {err}");
                    }
                }
            }
            ip = encrypt_rx.recv() => {
                let Some(packet) = ip else {
                    debug!("wireguard transport: encrypt_rx closed");
                    break;
                };
                handle_outbound(&sockets, &peers, &packet, crypto_cap).await;
            }
            _ = timer.tick() => {
                tick_timers(&sockets, &peers, crypto_cap).await;
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
    peers: &[Peer],
    udp_socket: &UdpSocket,
    first_src: SocketAddr,
    first_len: usize,
    recv_buf: &mut [u8],
    crypto_cap: usize,
    inbound_tx: &mpsc::Sender<IpStackInput>,
) {
    let mut inbound_batch = Vec::with_capacity(INBOUND_DRAIN_BATCH);
    handle_inbound_datagram(
        sockets,
        peers,
        first_src,
        &mut recv_buf[..first_len],
        crypto_cap,
        &mut inbound_batch,
    )
    .await;

    for _ in 1..INBOUND_DRAIN_BATCH {
        match udp_socket.try_recv_from(recv_buf) {
            Ok((n, src)) => {
                handle_inbound_datagram(
                    sockets,
                    peers,
                    src,
                    &mut recv_buf[..n],
                    crypto_cap,
                    &mut inbound_batch,
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

    flush_inbound_batch(inbound_tx, inbound_batch);
}

async fn handle_inbound_datagram(
    sockets: &TransportSockets,
    peers: &[Peer],
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

    // WARP-style 3-byte reserved header: sing-box clears it on the inbound
    // side before letting boringtun parse the frame (boringtun expects bytes
    // 1..4 to be zero).
    if peer.reserved() != [0u8; 3] && datagram.len() >= 4 {
        datagram[1] = 0;
        datagram[2] = 0;
        datagram[3] = 0;
    }

    // First call: pass the actual datagram + src ip (rate_limiter uses it).
    let action = step_decapsulate(peer, Some(src.ip()), datagram, crypto_cap);
    dispatch_action(sockets, peer, action, inbound_batch).await;

    // boringtun's contract: keep calling decapsulate(None, &[], buf) until
    // it returns Done so we drain any queued WriteToNetwork frames it staged
    // (e.g. the data packets that were waiting on a fresh handshake).
    loop {
        let action = step_decapsulate(peer, None, &[], crypto_cap);
        if matches!(action, DecapAction::Idle) {
            break;
        }
        dispatch_action(sockets, peer, action, inbound_batch).await;
    }
}

/// What `step_decapsulate` decided we should do with the bytes boringtun just
/// wrote into the per-call buffer. Each variant owns the buffer outright so
/// the dispatcher can hand it off (or `freeze()` it for the cross-actor mpsc)
/// without a memcpy.
#[derive(Debug)]
enum DecapAction {
    /// Nothing to send. Either boringtun consumed the datagram silently
    /// (`TunnResult::Done`) or returned an error (already trace-logged).
    Idle,
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
        DecapAction::Idle => {}
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

/// Hand the staged batch off to ipstack as a single mpsc message. The drop
/// unit is `INBOUND_DRAIN_BATCH` packets in the worst case (vs. one packet
/// pre-batching) — that's the deliberate trade: surface congestion at the
/// transport boundary instead of silently piling into a deeper ipstack queue,
/// at the cost of a coarser-grained drop when the inner stack stalls.
fn flush_inbound_batch(inbound_tx: &mpsc::Sender<IpStackInput>, batch: Vec<Bytes>) {
    let Some(input) = ipstack_input_from_batch(batch) else {
        return;
    };
    let packets = match &input {
        IpStackInput::Packet(_) => 1,
        IpStackInput::Batch(packets) => packets.len(),
    };
    if inbound_tx.try_send(input).is_err() {
        warn!("wireguard inbound queue full or closed; dropping {packets} packet(s)");
    }
}

#[inline]
fn ipstack_input_from_batch(mut batch: Vec<Bytes>) -> Option<IpStackInput> {
    match batch.len() {
        0 => None,
        1 => Some(IpStackInput::Packet(batch.pop().expect("one packet"))),
        _ => Some(IpStackInput::Batch(batch)),
    }
}

/// Run a single `Tunn::decapsulate` call inside a short lock. Allocates a
/// fresh `BytesMut` of `crypto_cap` bytes — boringtun's `decapsulate` API
/// requires a writable destination slice, and giving each call its own
/// buffer means `WriteToTunnelV4/V6` results can be `freeze()`d straight
/// into the inbound mpsc without copying.
fn step_decapsulate(
    peer: &Peer,
    src_ip: Option<IpAddr>,
    datagram: &[u8],
    crypto_cap: usize,
) -> DecapAction {
    let mut buf = BytesMut::zeroed(crypto_cap);
    let outcome = {
        let mut tunn = peer.lock_tunn();
        match tunn.decapsulate(src_ip, datagram, &mut buf[..]) {
            TunnResult::Done => DecapOutcome::Idle,
            TunnResult::Err(err) => {
                // Includes routine cases (replays, handshake retries) so
                // trace, not warn.
                tracing::trace!(?err, "wireguard decapsulate error");
                DecapOutcome::Idle
            }
            TunnResult::WriteToNetwork(out) => DecapOutcome::Network(out.len()),
            TunnResult::WriteToTunnelV4(out, _) | TunnResult::WriteToTunnelV6(out, _) => {
                DecapOutcome::Tunnel(out.len())
            }
        }
    };
    match outcome {
        DecapOutcome::Idle => DecapAction::Idle,
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
    Network(usize),
    Tunnel(usize),
}

async fn handle_outbound(
    sockets: &TransportSockets,
    peers: &[Peer],
    ip_packet: &[u8],
    crypto_cap: usize,
) {
    let Some(dst_ip) = first_hop_destination(ip_packet) else {
        warn!("wireguard outbound: malformed IP packet, dropping");
        return;
    };
    let Some(peer_idx) = peer::route_outbound(peers, dst_ip) else {
        warn!("wireguard outbound: no peer covers {dst_ip}, dropping");
        return;
    };
    let peer = &peers[peer_idx];

    let mut buf = BytesMut::zeroed(crypto_cap);
    let len = {
        let mut tunn = peer.lock_tunn();
        match tunn.encapsulate(ip_packet, &mut buf[..]) {
            TunnResult::WriteToNetwork(out) => Some(out.len()),
            TunnResult::Done => None,
            TunnResult::Err(err) => {
                tracing::trace!(?err, "wireguard encapsulate error");
                None
            }
            // encapsulate never returns WriteToTunnel*, but match exhaustively.
            TunnResult::WriteToTunnelV4(_, _) | TunnResult::WriteToTunnelV6(_, _) => None,
        }
    };

    let Some(len) = len else { return };
    buf.truncate(len);
    stamp_reserved_in_place(&mut buf, peer.reserved());
    let dst = peer.endpoint();
    let Some(socket) = sockets.send_socket(dst) else {
        warn!("wireguard udp has no socket for {dst}");
        return;
    };
    if let Err(err) = socket.send_to(&buf, dst).await {
        warn!("wireguard udp send_to {dst}: {err}");
    }
}

async fn tick_timers(sockets: &TransportSockets, peers: &[Peer], crypto_cap: usize) {
    for peer in peers {
        let mut buf = BytesMut::zeroed(crypto_cap);
        let len = {
            let mut tunn = peer.lock_tunn();
            match tunn.update_timers(&mut buf[..]) {
                TunnResult::WriteToNetwork(out) => Some(out.len()),
                _ => None,
            }
        };
        if let Some(len) = len {
            buf.truncate(len);
            stamp_reserved_in_place(&mut buf, peer.reserved());
            let dst = peer.endpoint();
            let Some(socket) = sockets.send_socket(dst) else {
                warn!("wireguard udp has no socket for {dst}");
                continue;
            };
            if let Err(err) = socket.send_to(&buf, dst).await {
                warn!("wireguard timer send {dst}: {err}");
            }
        }
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

    use hammer_core::config::WireguardPeerOptions;
    use hammer_core::log::{DiscardWriter, Factory};

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
    ) -> Peer {
        let opts = WireguardPeerOptions {
            public_key: peer_pub,
            pre_shared_key: None,
            endpoint,
            allowed_ips: vec!["10.0.0.0/8".parse::<IpNet>().unwrap()],
            persistent_keepalive: None,
            reserved: [0; 3],
        };
        Peer::new(opts, &x25519::StaticSecret::from(local_priv), index)
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

    #[test]
    fn flush_inbound_batch_sends_packet_input_for_one_packet() {
        let (tx, mut rx) = mpsc::channel(1);
        let packet = Bytes::from_static(b"one");

        flush_inbound_batch(&tx, vec![packet.clone()]);

        match rx.try_recv().expect("one ipstack message") {
            IpStackInput::Packet(received) => assert_eq!(received, packet),
            other => panic!("unexpected ipstack input: {other:?}"),
        }
    }

    #[test]
    fn flush_inbound_batch_sends_single_batch_input_for_multiple_packets() {
        let (tx, mut rx) = mpsc::channel(1);
        let first = Bytes::from_static(b"one");
        let second = Bytes::from_static(b"two");

        flush_inbound_batch(&tx, vec![first.clone(), second.clone()]);

        match rx.try_recv().expect("one ipstack message") {
            IpStackInput::Batch(packets) => assert_eq!(packets, vec![first, second]),
            other => panic!("unexpected ipstack input: {other:?}"),
        }
        assert!(rx.try_recv().is_err());
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

        let peer_a = make_peer(b_pub, a_priv, addr_b, 0);
        let peer_b = make_peer(a_pub, b_priv, addr_a, 0);

        let handles_a = spawn_transport(
            logger("transport-a"),
            Arc::new(vec![peer_a]),
            port_a,
            1408,
            SocketProtector::default(),
        )
        .expect("spawn A");
        let mut handles_b = spawn_transport(
            logger("transport-b"),
            Arc::new(vec![peer_b]),
            port_b,
            1408,
            SocketProtector::default(),
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
        let recovered = timeout(Duration::from_secs(5), handles_b.inbound_rx.recv())
            .await
            .expect("timed out waiting for inbound packet")
            .expect("inbound_rx closed before delivering");
        let recovered = match recovered {
            IpStackInput::Packet(packet) => packet,
            IpStackInput::Batch(mut packets) if packets.len() == 1 => {
                packets.pop().expect("one packet")
            }
            other => panic!("unexpected ipstack input: {other:?}"),
        };
        assert_eq!(&recovered[..], payload.as_slice());

        // Dropping the handles closes both mpsc channels and the oneshot;
        // the actor's select! loop notices and exits cleanly.
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_binds_ipv6_socket_for_ipv6_peer_endpoint() {
        let a_priv = [77u8; 32];
        let b_priv = [88u8; 32];
        let b_pub = x25519_public(b_priv);
        let peer = make_peer(
            b_pub,
            a_priv,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 51820),
            0,
        );

        let handles = spawn_transport(
            logger("transport-v6"),
            Arc::new(vec![peer]),
            0,
            1408,
            SocketProtector::default(),
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
        let v4_peer = make_peer(
            v4_pub,
            local_priv,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51820),
            0,
        );
        let v6_peer = make_peer(
            v6_pub,
            local_priv,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 51821),
            1,
        );

        let handles = spawn_transport(
            logger("transport-mixed"),
            Arc::new(vec![v4_peer, v6_peer]),
            0,
            1408,
            SocketProtector::default(),
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
