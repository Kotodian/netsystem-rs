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

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use boringtun::noise::TunnResult;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

use hammer_core::error::HammerError;
use hammer_core::log::Logger;

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
/// Same for the decrypted-inbound side.
const INBOUND_QUEUE: usize = 64;

/// Handles returned to the owner of a transport actor. Hold these for the
/// lifetime of the endpoint; dropping `shutdown` is enough to stop the actor
/// (the loop also exits when both `encrypt_tx` and `inbound_rx` close).
pub(crate) struct TransportHandles {
    /// Push IP packets into here to have them encapsulated and shipped to
    /// whichever peer's `allowed_ips` matches the destination.
    pub(crate) encrypt_tx: mpsc::Sender<Vec<u8>>,
    /// Receive decrypted IP packets coming back from any peer. The smoltcp
    /// stack drains this in the actor that owns the `phy::Device`.
    pub(crate) inbound_rx: mpsc::Receiver<Vec<u8>>,
    /// The address the OS bound the UDP socket to. Mostly useful for tests
    /// — production peers configure their endpoints out-of-band.
    pub(crate) local_addr: SocketAddr,
    /// Send `()` (or drop) to ask the actor to exit promptly.
    pub(crate) shutdown: oneshot::Sender<()>,
    /// Join handle in case the caller wants to await actor termination.
    pub(crate) join: JoinHandle<()>,
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
    let bind = bind_addr_for_peers(&peers, listen_port);
    let std_socket = std::net::UdpSocket::bind(bind)
        .map_err(|err| HammerError::internal(format!("wireguard udp bind {bind}: {err}")))?;
    std_socket
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("wireguard udp set_nonblocking: {err}")))?;
    let socket = UdpSocket::from_std(std_socket)
        .map_err(|err| HammerError::internal(format!("wireguard udp from_std: {err}")))?;
    protector.protect(&socket)?;
    let local_addr = socket
        .local_addr()
        .map_err(|err| HammerError::internal(format!("wireguard udp local_addr: {err}")))?;
    info!("wireguard transport listening on {local_addr}");

    let (encrypt_tx, encrypt_rx) = mpsc::channel::<Vec<u8>>(ENCRYPT_QUEUE);
    let (inbound_tx, inbound_rx) = mpsc::channel::<Vec<u8>>(INBOUND_QUEUE);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let join = crate::spawn::spawn(run_actor(
        Arc::new(socket),
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

fn bind_addr_for_peers(peers: &[Peer], listen_port: u16) -> SocketAddr {
    let needs_ipv6 = peers.iter().any(|peer| peer.endpoint().is_ipv6());
    if needs_ipv6 {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), listen_port)
    } else {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), listen_port)
    }
}

/// The actor body. Each branch is a single `select!` arm to keep the loop
/// fairly easy to reason about; backpressure on either mpsc is handled by
/// dropping packets after a warn-log because wg is best-effort at L3 anyway.
async fn run_actor(
    socket: Arc<UdpSocket>,
    peers: Arc<Vec<Peer>>,
    mtu: u32,
    mut encrypt_rx: mpsc::Receiver<Vec<u8>>,
    inbound_tx: mpsc::Sender<Vec<u8>>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    // 65535 is the IPv4 datagram ceiling; boringtun's WG frames cap out at
    // mtu + 32, so this is generous.
    let mut udp_buf = vec![0u8; 65_535];

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
            res = socket.recv_from(&mut udp_buf) => {
                match res {
                    Ok((n, src)) => {
                        let datagram = udp_buf[..n].to_vec();
                        handle_inbound(&socket, &peers, mtu, src, datagram, &inbound_tx).await;
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
                handle_outbound(&socket, &peers, mtu, packet).await;
            }
            _ = timer.tick() => {
                tick_timers(&socket, &peers, mtu).await;
            }
        }
    }
}

async fn handle_inbound(
    socket: &UdpSocket,
    peers: &[Peer],
    mtu: u32,
    src: SocketAddr,
    datagram: Vec<u8>,
    inbound_tx: &mpsc::Sender<Vec<u8>>,
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
    let mut datagram = datagram;
    if peer.reserved() != [0u8; 3] && datagram.len() >= 4 {
        datagram[1] = 0;
        datagram[2] = 0;
        datagram[3] = 0;
    }

    let buf_size = (mtu as usize) + WIREGUARD_OVERHEAD;
    // First call: pass the actual datagram + src ip (rate_limiter uses it).
    let mut scratch = vec![0u8; buf_size];
    let outputs = step_decapsulate(peer, Some(src.ip()), &datagram, &mut scratch);
    drain_outputs(socket, peer, outputs, inbound_tx).await;

    // boringtun's contract: keep calling decapsulate(None, &[], buf) until
    // it returns Done so we drain any queued WriteToNetwork frames it staged
    // (e.g. the data packets that were waiting on a fresh handshake).
    loop {
        let mut scratch = vec![0u8; buf_size];
        let outputs = step_decapsulate(peer, None, &[], &mut scratch);
        if outputs.is_empty() {
            break;
        }
        drain_outputs(socket, peer, outputs, inbound_tx).await;
    }
}

#[derive(Debug)]
enum Output {
    /// Send these encapsulated bytes to a remote endpoint via UDP.
    SendNetwork(Vec<u8>, SocketAddr),
    /// Hand these decrypted IP bytes to the inner stack.
    SendInbound(Vec<u8>),
}

async fn drain_outputs(
    socket: &UdpSocket,
    peer: &Peer,
    outputs: Vec<Output>,
    inbound_tx: &mpsc::Sender<Vec<u8>>,
) {
    for out in outputs {
        match out {
            Output::SendNetwork(buf, dst) => {
                let buf = stamp_reserved(buf, peer.reserved());
                if let Err(err) = socket.send_to(&buf, dst).await {
                    warn!("wireguard udp send_to {dst}: {err}");
                }
            }
            Output::SendInbound(buf) => {
                if inbound_tx.try_send(buf).is_err() {
                    warn!("wireguard inbound queue full or closed; dropping packet");
                }
            }
        }
    }
}

/// Run a single `Tunn::decapsulate` call inside a short lock and translate the
/// borrowed `TunnResult` into owned `Output`s. We don't loop here — the caller
/// drains follow-up outputs with empty datagrams as boringtun specifies.
fn step_decapsulate(
    peer: &Peer,
    src_ip: Option<IpAddr>,
    datagram: &[u8],
    scratch: &mut [u8],
) -> Vec<Output> {
    let mut tunn = peer.lock_tunn();
    match tunn.decapsulate(src_ip, datagram, scratch) {
        TunnResult::Done => Vec::new(),
        TunnResult::Err(_) => Vec::new(),
        TunnResult::WriteToNetwork(out) => {
            vec![Output::SendNetwork(out.to_vec(), peer.endpoint())]
        }
        TunnResult::WriteToTunnelV4(out, _) | TunnResult::WriteToTunnelV6(out, _) => {
            vec![Output::SendInbound(out.to_vec())]
        }
    }
}

async fn handle_outbound(socket: &UdpSocket, peers: &[Peer], mtu: u32, ip_packet: Vec<u8>) {
    let Some(dst_ip) = first_hop_destination(&ip_packet) else {
        warn!("wireguard outbound: malformed IP packet, dropping");
        return;
    };
    let Some(peer_idx) = peer::route_outbound(peers, dst_ip) else {
        warn!("wireguard outbound: no peer covers {dst_ip}, dropping");
        return;
    };
    let peer = &peers[peer_idx];

    let buf_size = (mtu as usize) + WIREGUARD_OVERHEAD;
    let mut scratch = vec![0u8; buf_size];
    let to_send = {
        let mut tunn = peer.lock_tunn();
        match tunn.encapsulate(&ip_packet, &mut scratch) {
            TunnResult::WriteToNetwork(out) => Some(out.to_vec()),
            TunnResult::Done => None,
            TunnResult::Err(_) => None,
            // encapsulate never returns WriteToTunnel*, but match exhaustively.
            TunnResult::WriteToTunnelV4(_, _) | TunnResult::WriteToTunnelV6(_, _) => None,
        }
    };

    let Some(buf) = to_send else { return };
    let buf = stamp_reserved(buf, peer.reserved());
    let dst = peer.endpoint();
    if let Err(err) = socket.send_to(&buf, dst).await {
        warn!("wireguard udp send_to {dst}: {err}");
    }
}

async fn tick_timers(socket: &UdpSocket, peers: &[Peer], mtu: u32) {
    let buf_size = (mtu as usize) + WIREGUARD_OVERHEAD;
    for peer in peers {
        let mut scratch = vec![0u8; buf_size];
        let to_send = {
            let mut tunn = peer.lock_tunn();
            match tunn.update_timers(&mut scratch) {
                TunnResult::WriteToNetwork(out) => Some(out.to_vec()),
                _ => None,
            }
        };
        if let Some(buf) = to_send {
            let buf = stamp_reserved(buf, peer.reserved());
            let dst = peer.endpoint();
            if let Err(err) = socket.send_to(&buf, dst).await {
                warn!("wireguard timer send {dst}: {err}");
            }
        }
    }
}

/// Stamp the WARP-style 3-byte reserved prefix into `packet[1..4]`. boringtun
/// only writes the first byte (message type); the next three are ours.
fn stamp_reserved(mut packet: Vec<u8>, reserved: [u8; 3]) -> Vec<u8> {
    if reserved != [0u8; 3] && packet.len() >= 4 {
        packet[1..4].copy_from_slice(&reserved);
    }
    packet
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
            .send(payload.clone())
            .await
            .expect("encrypt_tx");

        // Five seconds is generous: the handshake completes in <50 ms over
        // localhost, even with the 250 ms timer driving retransmits.
        let recovered = timeout(Duration::from_secs(5), handles_b.inbound_rx.recv())
            .await
            .expect("timed out waiting for inbound packet")
            .expect("inbound_rx closed before delivering");
        assert_eq!(recovered, payload);

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
}
