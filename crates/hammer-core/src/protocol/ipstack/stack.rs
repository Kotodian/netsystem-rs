//! Generic smoltcp user-space TCP/IP stack actor. Carrier-agnostic: the
//! WireGuard outbound feeds it via boringtun today, and the TUN inbound's
//! smoltcp mode will share the same actor when it lands.
//!
//! Equivalent to sing-box's `transport/wireguard/device_stack.go::StackDevice`,
//! which wraps the gVisor `tcpip.Stack` around a wireguard-go `tun.Device`.
//! Hammer's version owns a `smoltcp::iface::Interface` plus a `SocketSet`,
//! drains inbound IP packets from a caller-supplied mpsc receiver, and emits
//! the IP packets smoltcp produces into a caller-supplied mpsc sender. On top
//! of that it services `dial(TCP)` / `bind(UDP)` / `listen(TCP)` requests via
//! `IpStackHandles`, returning user-side `tokio::io::DuplexStream` and a
//! plain `UdpHandle` keyed on `SocketAddr` — translating `SocksAddr` /
//! `ProxyPacketConn` is left to whichever outbound is wrapping us.

use std::collections::VecDeque;
// rustc-hash's `FxHashMap` is HashMap<K, V, BuildHasherDefault<FxHasher>> —
// non-cryptographic hashing for internal maps where the keys are our own
// `SocketHandle` indices, never user-controlled. The wg hot path lookups
// here showed up at ~1.6% on the NetExt CPU profile under sustained video;
// FxHasher is roughly 4× faster than the default SipHasher for small keys.
use rustc_hash::FxHashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tracing::{debug, warn};

use bytes::{Buf, Bytes, BytesMut};
use ipnet::IpNet;
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, IpEndpoint, IpListenEndpoint};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant as TokioInstant, sleep_until};

use crate::error::{HammerError, HammerResult};
use crate::log::Logger;

use super::device::ChannelDevice;

/// Per-TCP-socket smoltcp ring buffer size. 64 KiB matches a typical Linux
/// SO_RCVBUF default and is plenty for the bursty HTTP/QUIC payloads we expect
/// to multiplex over an IP carrier.
const TCP_BUF: usize = 64 * 1024;
/// Hard ceiling on how long a TCP bridge task may sit completely idle (no
/// user-side traffic and no inbound from smoltcp) before we tear it down.
/// Defensive backstop against an upstream that forgot to drop a stream:
/// without this each forgotten connection holds ~256 KiB (smoltcp rx/tx
/// rings + the bridge's `BytesMut` + the duplex tail), which is what causes
/// the NetExt to drift up under sustained traffic. Keep it near common HTTP
/// keepalive windows so idle browser connections are recycled quickly while
/// active streams stay alive.
const TCP_BRIDGE_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Tighter idle timeout that kicks in once the user side has signalled EOF
/// (`Ok(0)` from `read_buf`). After EOF the bridge is just draining the
/// remaining inbound from smoltcp and waiting for the actor to reap the
/// socket; on a slow peer FIN we'd otherwise sit on ~256 KiB per stream for
/// the full 60 s active-keepalive window. Aligns with sing-box's generic
/// `TCPTimeout = 15 s` (`constant/timeout.go`) so TLS shutdown / HTTP/2
/// GOAWAY / application graceful-close still have room to finish.
const TCP_BRIDGE_DRAIN_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
/// Buffer between the actor and a TCP bridge task — small because we drain
/// it immediately after every iface.poll cycle.
const TCP_DATA_QUEUE: usize = 16;
/// UDP payload buffer slots inside smoltcp.
const UDP_PACKET_SLOTS: usize = 32;
/// Largest UDP datagram smoltcp will surface to a socket. Keep this aligned
/// with the workspace smoltcp reassembly budget: large enough for the 4 KiB
/// fragmented regression case, small enough that long video/QUIC sessions do
/// not pin 128 KiB per WireGuard UDP flow.
const UDP_PAYLOAD_LIMIT: usize = 8 * 1024;
/// `tokio::io::duplex` capacity per direction for a TCP stream — 64 KiB so a
/// single send_slice can fit even under bursty writes.
const TCP_DUPLEX: usize = 64 * 1024;
/// Cap on outstanding UDP datagrams between bridge and user.
const UDP_QUEUE: usize = 32;

/// Floor for ephemeral source ports we hand out to TCP/UDP sockets.
const EPHEMERAL_FLOOR: u16 = 49_152;

/// Far-future poll wake-up when smoltcp has nothing pending. 1 hour is overkill
/// but cheap — it just gates how often we recompute poll_at when truly idle.
const IDLE_WAKEUP: Duration = Duration::from_secs(3600);
/// Upper bound for waiting on the in-stack TCP three-way handshake. A closed
/// peer normally fails much faster via RST; this keeps blackholed connects from
/// parking an outbound dial forever.
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct IpStackHandles {
    cmd_tx: mpsc::Sender<StackCommand>,
    /// Wrapped in a Mutex<Option<...>> so the consumer can call `&self`
    /// methods (clone an Arc<IpStackHandles> and `dial`) while still being
    /// able to take the sender once at shutdown.
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    join: JoinHandle<()>,
}

impl IpStackHandles {
    pub async fn dial_tcp(&self, dst: SocketAddr) -> HammerResult<DuplexStream> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(StackCommand::DialTcp {
                dst,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HammerError::internal("ipstack: actor closed"))?;
        reply_rx
            .await
            .map_err(|_| HammerError::internal("ipstack: dial response dropped"))?
    }

    pub async fn bind_udp(&self) -> HammerResult<UdpHandle> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(StackCommand::BindUdp { reply: reply_tx })
            .await
            .map_err(|_| HammerError::internal("ipstack: actor closed"))?;
        reply_rx
            .await
            .map_err(|_| HammerError::internal("ipstack: bind response dropped"))?
    }

    /// Build a `TcpListener` bound to `port` inside the stack. The listener
    /// is just a thin command sender — the actual smoltcp socket is created
    /// per-`accept()` call so the stack supports an unbounded series of
    /// inbound connections without juggling backlog state itself.
    pub fn listen_tcp(&self, port: u16) -> TcpListener {
        TcpListener {
            cmd_tx: self.cmd_tx.clone(),
            port,
        }
    }

    /// Tell the actor task to exit at its next select! poll. Idempotent —
    /// subsequent calls find the sender already taken.
    pub fn signal_shutdown(&self) {
        if let Some(tx) = self
            .shutdown
            .lock()
            .expect("IpStackHandles shutdown poisoned")
            .take()
        {
            let _ = tx.send(());
        }
    }

    /// Force the actor task to drop without waiting. Pairs with
    /// `signal_shutdown` for the lifecycle close path.
    pub fn abort(&self) {
        self.join.abort();
    }

    /// Count UDP sockets currently parked in the smoltcp `SocketSet`. Used by
    /// the leak regression test in this module to verify that closed UDP
    /// sockets are removed from the SocketSet, not just unbound.
    #[cfg(test)]
    pub(super) async fn debug_udp_socket_count(&self) -> usize {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(StackCommand::DebugUdpSocketCount { reply: reply_tx })
            .await
            .is_err()
        {
            return 0;
        }
        reply_rx.await.unwrap_or(0)
    }
}

/// Inbound-TCP listener bound to a port inside the stack. `accept()` runs
/// one acceptance cycle per call: it sends an `AcceptTcp` command to the
/// stack actor, which spins up a fresh smoltcp listening socket on that port,
/// and replies with a `DuplexStream` once the three-way handshake completes.
/// Concurrent accepts are fine — each gets its own socket.
pub struct TcpListener {
    cmd_tx: mpsc::Sender<StackCommand>,
    port: u16,
}

impl TcpListener {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn accept(&self) -> HammerResult<DuplexStream> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(StackCommand::AcceptTcp {
                port: self.port,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HammerError::internal("ipstack: actor closed"))?;
        reply_rx
            .await
            .map_err(|_| HammerError::internal("ipstack: accept response dropped"))?
    }
}

pub struct UdpHandle {
    send_tx: mpsc::Sender<(Bytes, SocketAddr)>,
    recv_rx: AsyncMutex<mpsc::Receiver<(Bytes, SocketAddr)>>,
    /// Port allocated by the actor inside the smoltcp Interface. Useful for
    /// integration tests that need to address one side from the other across
    /// the stack.
    local_port: u16,
}

impl UdpHandle {
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    /// Send a UDP datagram to `dst`. The caller is expected to have already
    /// resolved any domain destination to a concrete `SocketAddr` — translating
    /// outbound-layer addressing types (`SocksAddr`, etc.) into IP is not the
    /// stack's job.
    pub async fn send(&self, payload: Bytes, dst: SocketAddr) -> HammerResult<()> {
        self.send_tx
            .send((payload, dst))
            .await
            .map_err(|_| HammerError::internal("ipstack udp: stack closed"))
    }

    /// Receive the next UDP datagram emitted by the stack, paired with the
    /// remote IP/port that sent it.
    pub async fn recv(&self) -> HammerResult<(Bytes, SocketAddr)> {
        let mut rx = self.recv_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| HammerError::internal("ipstack udp: stack closed"))
    }
}

/// Carrier-to-stack input. Single packets keep the existing cheap path; batch
/// messages let UDP-backed carriers amortize channel wakeups and smoltcp polls
/// when several IP packets arrive in one readiness cycle.
#[derive(Debug)]
pub enum IpStackInput {
    Packet(Bytes),
    Batch(Vec<Bytes>),
}

/// Spin up the netstack actor. Synchronously builds the smoltcp interface
/// (no I/O involved) so the caller has a fully-wired `IpStackHandles` before the
/// task even runs its first iteration.
pub fn spawn_ipstack(
    logger: Logger,
    addresses: Vec<IpNet>,
    mtu: u32,
    inbound_rx: mpsc::Receiver<IpStackInput>,
    encrypt_tx: mpsc::Sender<Bytes>,
) -> HammerResult<IpStackHandles> {
    if addresses.is_empty() {
        return Err(HammerError::internal(
            "ipstack: at least one local IP address required",
        ));
    }

    let (egress_tx, egress_rx) = mpsc::unbounded_channel::<Bytes>();
    let mut device = ChannelDevice::new(mtu, egress_tx);

    let started = TokioInstant::now();
    let now = SmolInstant::from_micros(0);
    let mut iface = Interface::new(Config::new(HardwareAddress::Ip), &mut device, now);
    iface.update_ip_addrs(|addrs| {
        for net in &addresses {
            if let Some(cidr) = ipnet_to_cidr(net) {
                let _ = addrs.push(cidr);
            }
        }
    });
    // Accept packets addressed to anyone — the carrier is treated as point-to-point so
    // there is no real meaning to "this IP isn't ours". sing-box does the same.
    iface.set_any_ip(true);

    let (cmd_tx, cmd_rx) = mpsc::channel::<StackCommand>(16);
    let (event_tx, event_rx) = mpsc::channel::<SocketEvent>(64);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let inner = StackInner {
        logger,
        iface,
        sockets: SocketSet::new(Vec::new()),
        device,
        egress_rx,
        encrypt_tx,
        inbound_rx,
        cmd_rx,
        event_rx,
        event_tx,
        shutdown_rx,
        tcp_bridges: FxHashMap::default(),
        udp_bridges: FxHashMap::default(),
        pending_accepts: Vec::new(),
        pending_dials: Vec::new(),
        next_port: EPHEMERAL_FLOOR,
        started,
        handle_scratch: Vec::new(),
        // One alloc + bzero for the lifetime of the actor; large enough to
        // catch the worst case of either drain path.
        recv_scratch: vec![0u8; TCP_BUF.max(UDP_PAYLOAD_LIMIT)],
    };

    let join = tokio::spawn(inner.run());

    Ok(IpStackHandles {
        cmd_tx,
        shutdown: std::sync::Mutex::new(Some(shutdown_tx)),
        join,
    })
}

enum StackCommand {
    DialTcp {
        dst: SocketAddr,
        reply: oneshot::Sender<HammerResult<DuplexStream>>,
    },
    BindUdp {
        reply: oneshot::Sender<HammerResult<UdpHandle>>,
    },
    AcceptTcp {
        port: u16,
        reply: oneshot::Sender<HammerResult<DuplexStream>>,
    },
    /// Inspect how many UDP sockets are currently parked in the smoltcp
    /// `SocketSet`. Test-only — the production code never needs to ask.
    #[cfg(test)]
    DebugUdpSocketCount { reply: oneshot::Sender<usize> },
}

/// One in-flight inbound TCP accept. The listening smoltcp socket sits in
/// `SocketSet`; once the three-way handshake completes (`may_recv` &&
/// `may_send`) we hand the established socket off to a freshly-spawned
/// bridge task and reply to `oneshot` with the user-side `DuplexStream`.
struct PendingAccept {
    handle: SocketHandle,
    reply: oneshot::Sender<HammerResult<DuplexStream>>,
}

/// One outbound TCP connect waiting for smoltcp to reach ESTABLISHED (or fail).
struct PendingDial {
    handle: SocketHandle,
    dst: SocketAddr,
    reply: oneshot::Sender<HammerResult<DuplexStream>>,
    deadline: TokioInstant,
}

/// Messages that the per-socket bridge tasks send back to the actor. Each
/// carries the `SocketHandle` so the actor's single mpsc receiver can demux
/// without a fan-in struct per socket.
#[cfg_attr(test, derive(Debug))]
enum SocketEvent {
    TcpWrite {
        handle: SocketHandle,
        data: Bytes,
        ack: oneshot::Sender<HammerResult<()>>,
    },
    TcpClose {
        handle: SocketHandle,
        mode: TcpCloseMode,
    },
    UdpSend {
        handle: SocketHandle,
        data: Bytes,
        dst: SocketAddr,
    },
    UdpClose {
        handle: SocketHandle,
    },
}

#[cfg_attr(test, derive(Debug))]
enum TcpCloseMode {
    /// The user closed its write half. Preserve already-accepted bytes and
    /// send a FIN once our user->socket queue drains.
    GracefulWriteEof,
    /// The user read half disappeared or our defensive idle timer fired. The
    /// stream is no longer usable, so abort to release buffers promptly.
    Abort,
}

struct TcpBridge {
    /// Channel for the actor to push socket recv data into the bridge task.
    data_tx: mpsc::Sender<Bytes>,
    /// Bytes the actor is still trying to push into the smoltcp tx buffer
    /// (didn't fit in a single send_slice call).
    pending_tx: VecDeque<PendingTcpWrite>,
    /// User wrote EOF; call `socket.close()` once `pending_tx` is empty so
    /// already-accepted bytes are not discarded by an early RST.
    close_after_pending_tx: bool,
}

struct PendingTcpWrite {
    /// Remaining bytes to push into the smoltcp tx buffer. We `advance` it as
    /// each partial `send_slice` succeeds; an empty `Bytes` means the write
    /// is fully drained and the matching `ack` can fire.
    data: Bytes,
    ack: Option<oneshot::Sender<HammerResult<()>>>,
}

struct UdpBridge {
    /// Channel for the actor to push (payload, src) to the bridge task.
    data_tx: mpsc::Sender<(Bytes, SocketAddr)>,
}

struct StackInner {
    logger: Logger,
    iface: Interface,
    sockets: SocketSet<'static>,
    device: ChannelDevice,
    egress_rx: mpsc::UnboundedReceiver<Bytes>,
    encrypt_tx: mpsc::Sender<Bytes>,
    inbound_rx: mpsc::Receiver<IpStackInput>,
    cmd_rx: mpsc::Receiver<StackCommand>,
    event_rx: mpsc::Receiver<SocketEvent>,
    event_tx: mpsc::Sender<SocketEvent>,
    shutdown_rx: oneshot::Receiver<()>,
    tcp_bridges: FxHashMap<SocketHandle, TcpBridge>,
    udp_bridges: FxHashMap<SocketHandle, UdpBridge>,
    pending_accepts: Vec<PendingAccept>,
    pending_dials: Vec<PendingDial>,
    next_port: u16,
    started: TokioInstant,
    /// Reused by drain loops that need a stable handle snapshot before
    /// mutating `SocketSet`.
    handle_scratch: Vec<SocketHandle>,
    /// Reused across every `drain_tcp_recv` / `drain_udp_recv` call so we
    /// don't allocate-and-bzero a 64 KiB BytesMut per inbound packet just to
    /// truncate it down to a 1.5 KiB IP frame. `Bytes::copy_from_slice` then
    /// allocates only the bytes we actually keep.
    recv_scratch: Vec<u8>,
}

impl StackInner {
    async fn run(mut self) {
        loop {
            self.poll_and_drain().await;

            let now_smol = self.smol_now();
            let next_at = self
                .iface
                .poll_at(now_smol, &self.sockets)
                .map(|inst| self.tokio_from_smol(inst))
                .unwrap_or_else(|| TokioInstant::now() + IDLE_WAKEUP);
            let next_at = self
                .pending_dials
                .iter()
                .map(|dial| dial.deadline)
                .min()
                .map(|deadline| next_at.min(deadline))
                .unwrap_or(next_at);

            tokio::select! {
                biased;
                _ = &mut self.shutdown_rx => {
                    debug!("ipstack: shutdown");
                    return;
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_command(cmd).await;
                }
                Some(input) = self.inbound_rx.recv() => {
                    self.deliver_input(input);
                }
                Some(event) = self.event_rx.recv() => {
                    self.handle_event(event);
                }
                _ = sleep_until(next_at) => { /* time to poll */ }
            }
        }
    }

    fn deliver_input(&mut self, input: IpStackInput) {
        match input {
            IpStackInput::Packet(packet) => self.device.deliver(packet),
            IpStackInput::Batch(packets) => self.device.deliver_batch(packets),
        }
    }

    /// Run one full poll cycle then push everything that needs to leave the
    /// actor: device egress goes to the transport, socket receive buffers go
    /// to bridge tasks, and queued tx buffers (from a prior partial send) get
    /// retried.
    async fn poll_and_drain(&mut self) {
        let now = self.smol_now();
        let _ = self.iface.poll(now, &mut self.device, &mut self.sockets);

        // Flush smoltcp's outbound IP packets up to the transport.
        while let Ok(buf) = self.egress_rx.try_recv() {
            if self.encrypt_tx.send(buf).await.is_err() {
                warn!("ipstack: encrypt_tx closed, transport stopped");
                return;
            }
        }

        self.check_pending_accepts();
        self.check_pending_dials();
        self.drain_tcp_recv();
        self.retry_pending_tcp_writes();
        self.drain_udp_recv();
        self.reap_dead_sockets();
    }

    /// Walk pending accepts and resolve any whose listening socket has
    /// finished the SYN/SYN-ACK/ACK exchange. Each resolved accept transfers
    /// the now-established socket from "listen pool" to a regular bridge so
    /// the caller's `DuplexStream` can shovel bytes through it like a dial.
    fn check_pending_accepts(&mut self) {
        use smoltcp::socket::tcp::State;
        if self.pending_accepts.is_empty() {
            return;
        }
        let drained = std::mem::take(&mut self.pending_accepts);
        for accept in drained {
            let socket = self.sockets.get::<tcp::Socket>(accept.handle);
            match socket.state() {
                State::Listen | State::SynReceived | State::SynSent => {
                    // Still mid-handshake; check again next poll.
                    self.pending_accepts.push(accept);
                }
                State::Established
                | State::FinWait1
                | State::FinWait2
                | State::CloseWait
                | State::Closing
                | State::LastAck => {
                    // Connection is up (or already half-closing — let the
                    // bridge surface that to the caller).
                    let user_side = self.start_tcp_bridge(accept.handle);
                    let _ = accept.reply.send(Ok(user_side));
                }
                State::Closed | State::TimeWait => {
                    // Listening socket aborted (RST) or completed and closed
                    // before we surfaced it. Drop it and let the caller decide
                    // whether to re-listen.
                    self.sockets.remove(accept.handle);
                    let _ = accept.reply.send(Err(HammerError::internal(
                        "ipstack tcp listen aborted before connection established",
                    )));
                }
            }
        }
    }

    /// Resolve outbound dials only after smoltcp reports the TCP handshake is
    /// established. This keeps `Outbound::dial` from succeeding for a remote
    /// port that already rejected the SYN.
    fn check_pending_dials(&mut self) {
        use smoltcp::socket::tcp::State;
        if self.pending_dials.is_empty() {
            return;
        }

        let now = TokioInstant::now();
        let drained = std::mem::take(&mut self.pending_dials);
        for dial in drained {
            let state = self.sockets.get::<tcp::Socket>(dial.handle).state();
            match state {
                State::SynSent | State::SynReceived => {
                    if now >= dial.deadline {
                        self.sockets.remove(dial.handle);
                        let _ = dial.reply.send(Err(HammerError::internal(format!(
                            "ipstack tcp connect failed to {}: timeout",
                            dial.dst
                        ))));
                    } else {
                        self.pending_dials.push(dial);
                    }
                }
                State::Established
                | State::FinWait1
                | State::FinWait2
                | State::CloseWait
                | State::Closing
                | State::LastAck => {
                    let user_side = self.start_tcp_bridge(dial.handle);
                    let _ = dial.reply.send(Ok(user_side));
                }
                State::Closed | State::TimeWait | State::Listen => {
                    self.sockets.remove(dial.handle);
                    let _ = dial.reply.send(Err(HammerError::internal(format!(
                        "ipstack tcp connect failed to {}: {:?}",
                        dial.dst, state
                    ))));
                }
            }
        }
    }

    fn handle_event(&mut self, event: SocketEvent) {
        match event {
            SocketEvent::TcpWrite { handle, data, ack } => {
                self.handle_tcp_write(handle, data, ack);
            }
            SocketEvent::TcpClose { handle, mode } => {
                if self.tcp_bridges.contains_key(&handle) {
                    match mode {
                        TcpCloseMode::GracefulWriteEof => {
                            if let Some(bridge) = self.tcp_bridges.get_mut(&handle) {
                                bridge.close_after_pending_tx = true;
                            }
                            self.close_tcp_if_pending_tx_drained(handle);
                        }
                        TcpCloseMode::Abort => {
                            self.fail_pending_tcp_writes(handle, "ipstack tcp: user stream closed");
                            let socket = self.sockets.get_mut::<tcp::Socket>(handle);
                            socket.abort();
                        }
                    }
                }
            }
            SocketEvent::UdpSend { handle, data, dst } => {
                if !self.udp_bridges.contains_key(&handle) {
                    return;
                }
                let endpoint = match socket_endpoint_for(dst) {
                    Some(ep) => ep,
                    None => return,
                };
                let socket = self.sockets.get_mut::<udp::Socket>(handle);
                if let Err(err) = socket.send_slice(&data, endpoint) {
                    warn!("ipstack udp send_slice: {err:?}");
                }
            }
            SocketEvent::UdpClose { handle } => {
                if self.udp_bridges.remove(&handle).is_some() {
                    self.sockets.remove(handle);
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: StackCommand) {
        match cmd {
            StackCommand::DialTcp { dst, reply } => {
                self.queue_dial(dst, reply);
            }
            StackCommand::BindUdp { reply } => {
                let _ = reply.send(self.bind_udp());
            }
            StackCommand::AcceptTcp { port, reply } => {
                self.queue_accept(port, reply);
            }
            #[cfg(test)]
            StackCommand::DebugUdpSocketCount { reply } => {
                let count = self
                    .sockets
                    .iter()
                    .filter(|(_, sock)| matches!(sock, smoltcp::socket::Socket::Udp(_)))
                    .count();
                let _ = reply.send(count);
            }
        }
    }

    /// Park a fresh listening socket and remember the reply channel. The next
    /// poll cycle will notice when it transitions to ESTABLISHED and hand the
    /// caller a `DuplexStream`.
    fn queue_accept(&mut self, port: u16, reply: oneshot::Sender<HammerResult<DuplexStream>>) {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);
        if let Err(err) = socket.listen(port) {
            let _ = reply.send(Err(HammerError::internal(format!(
                "ipstack tcp listen on {port}: {err:?}"
            ))));
            return;
        }
        let handle = self.sockets.add(socket);
        self.pending_accepts.push(PendingAccept { handle, reply });
    }

    fn queue_dial(&mut self, dst: SocketAddr, reply: oneshot::Sender<HammerResult<DuplexStream>>) {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);

        let local_port = self.alloc_port();
        let remote = match socket_endpoint_for(dst) {
            Some(ep) => ep,
            None => {
                let _ = reply.send(Err(HammerError::internal(format!(
                    "ipstack dial: unsupported destination {dst}"
                ))));
                return;
            }
        };
        if let Err(err) = socket.connect(self.iface.context(), remote, local_port) {
            let _ = reply.send(Err(HammerError::internal(format!(
                "ipstack tcp connect: {err:?}"
            ))));
            return;
        }

        let handle = self.sockets.add(socket);
        self.pending_dials.push(PendingDial {
            handle,
            dst,
            reply,
            deadline: TokioInstant::now() + TCP_CONNECT_TIMEOUT,
        });
    }

    fn start_tcp_bridge(&mut self, handle: SocketHandle) -> DuplexStream {
        let (user_side, actor_side) = tokio::io::duplex(TCP_DUPLEX);
        let (data_tx, data_rx) = mpsc::channel::<Bytes>(TCP_DATA_QUEUE);
        let event_tx = self.event_tx.clone();
        tokio::spawn(tcp_bridge_task(handle, actor_side, data_rx, event_tx));
        self.tcp_bridges.insert(
            handle,
            TcpBridge {
                data_tx,
                pending_tx: VecDeque::new(),
                close_after_pending_tx: false,
            },
        );
        user_side
    }

    fn handle_tcp_write(
        &mut self,
        handle: SocketHandle,
        mut data: Bytes,
        ack: oneshot::Sender<HammerResult<()>>,
    ) {
        let Some(bridge) = self.tcp_bridges.get_mut(&handle) else {
            let _ = ack.send(Err(HammerError::internal("ipstack tcp: socket closed")));
            return;
        };

        if !bridge.pending_tx.is_empty() {
            bridge.pending_tx.push_back(PendingTcpWrite {
                data,
                ack: Some(ack),
            });
            return;
        }

        let written = self.write_tcp_slice(handle, &data);
        if written >= data.len() {
            let _ = ack.send(Ok(()));
            self.close_tcp_if_pending_tx_drained(handle);
            return;
        }

        // Partial write: record what's left for `retry_pending_tcp_writes` to
        // push out as the smoltcp tx buffer drains.
        data.advance(written);
        if let Some(bridge) = self.tcp_bridges.get_mut(&handle) {
            bridge.pending_tx.push_back(PendingTcpWrite {
                data,
                ack: Some(ack),
            });
        } else {
            let _ = ack.send(Err(HammerError::internal("ipstack tcp: socket closed")));
        }
    }

    fn write_tcp_slice(&mut self, handle: SocketHandle, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        if !socket.may_send() {
            return 0;
        }
        match socket.send_slice(data) {
            Ok(n) => n,
            Err(err) => {
                debug!("ipstack tcp send_slice deferred: {err:?}");
                0
            }
        }
    }

    fn fail_pending_tcp_writes(&mut self, handle: SocketHandle, reason: &'static str) {
        let Some(bridge) = self.tcp_bridges.get_mut(&handle) else {
            return;
        };
        while let Some(mut pending) = bridge.pending_tx.pop_front() {
            if let Some(ack) = pending.ack.take() {
                let _ = ack.send(Err(HammerError::internal(reason)));
            }
        }
    }

    fn close_tcp_if_pending_tx_drained(&mut self, handle: SocketHandle) {
        let should_close = self
            .tcp_bridges
            .get(&handle)
            .map(|bridge| bridge.close_after_pending_tx && bridge.pending_tx.is_empty())
            .unwrap_or(false);
        if should_close {
            let socket = self.sockets.get_mut::<tcp::Socket>(handle);
            socket.close();
        }
    }

    fn bind_udp(&mut self) -> HammerResult<UdpHandle> {
        let rx_buf = udp::PacketBuffer::new(
            vec![udp::PacketMetadata::EMPTY; UDP_PACKET_SLOTS],
            vec![0u8; UDP_PAYLOAD_LIMIT],
        );
        let tx_buf = udp::PacketBuffer::new(
            vec![udp::PacketMetadata::EMPTY; UDP_PACKET_SLOTS],
            vec![0u8; UDP_PAYLOAD_LIMIT],
        );
        let mut socket = udp::Socket::new(rx_buf, tx_buf);
        let local_port = self.alloc_port();
        socket
            .bind(IpListenEndpoint {
                addr: None,
                port: local_port,
            })
            .map_err(|err| HammerError::internal(format!("ipstack udp bind: {err:?}")))?;
        let handle = self.sockets.add(socket);

        let (recv_user_tx, recv_user_rx) = mpsc::channel::<(Bytes, SocketAddr)>(UDP_QUEUE);
        let (send_user_tx, send_user_rx) = mpsc::channel::<(Bytes, SocketAddr)>(UDP_QUEUE);
        let event_tx = self.event_tx.clone();
        tokio::spawn(udp_bridge_task(handle, send_user_rx, event_tx));

        self.udp_bridges.insert(
            handle,
            UdpBridge {
                data_tx: recv_user_tx,
            },
        );

        Ok(UdpHandle {
            send_tx: send_user_tx,
            recv_rx: AsyncMutex::new(recv_user_rx),
            local_port,
        })
    }

    fn drain_tcp_recv(&mut self) {
        self.handle_scratch.clear();
        self.handle_scratch.extend(self.tcp_bridges.keys().copied());
        for idx in 0..self.handle_scratch.len() {
            let handle = self.handle_scratch[idx];
            let socket = self.sockets.get_mut::<tcp::Socket>(handle);
            while socket.can_recv() {
                let data_tx = match self.tcp_bridges.get(&handle) {
                    Some(b) => b.data_tx.clone(),
                    None => break,
                };
                let permit = match data_tx.try_reserve_owned() {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                match socket.recv_slice(&mut self.recv_scratch[..TCP_BUF]) {
                    Ok(n) if n > 0 => {
                        permit.send(Bytes::copy_from_slice(&self.recv_scratch[..n]));
                    }
                    _ => break,
                }
            }
        }
    }

    fn retry_pending_tcp_writes(&mut self) {
        self.handle_scratch.clear();
        self.handle_scratch.extend(self.tcp_bridges.keys().copied());
        for idx in 0..self.handle_scratch.len() {
            let handle = self.handle_scratch[idx];
            let bridge = match self.tcp_bridges.get_mut(&handle) {
                Some(b) => b,
                None => continue,
            };
            if bridge.pending_tx.is_empty() {
                continue;
            }
            let socket = self.sockets.get_mut::<tcp::Socket>(handle);
            if !socket.may_send() {
                continue;
            }
            while let Some(pending) = bridge.pending_tx.front_mut() {
                if !socket.may_send() {
                    break;
                }
                match socket.send_slice(&pending.data) {
                    Ok(0) => break,
                    Ok(n) => {
                        pending.data.advance(n);
                        if !pending.data.is_empty() {
                            break;
                        }
                        let mut done = bridge.pending_tx.pop_front().expect("front exists");
                        if let Some(ack) = done.ack.take() {
                            let _ = ack.send(Ok(()));
                        }
                    }
                    Err(err) => {
                        debug!("ipstack tcp retry deferred: {err:?}");
                        break;
                    }
                }
            }
            self.close_tcp_if_pending_tx_drained(handle);
        }
    }

    fn drain_udp_recv(&mut self) {
        self.handle_scratch.clear();
        self.handle_scratch.extend(self.udp_bridges.keys().copied());
        for idx in 0..self.handle_scratch.len() {
            let handle = self.handle_scratch[idx];
            let socket = self.sockets.get_mut::<udp::Socket>(handle);
            while socket.can_recv() {
                match socket.recv_slice(&mut self.recv_scratch[..UDP_PAYLOAD_LIMIT]) {
                    Ok((n, meta)) => {
                        let chunk = Bytes::copy_from_slice(&self.recv_scratch[..n]);
                        let src = ip_endpoint_to_socket(meta.endpoint);
                        let bridge = match self.udp_bridges.get(&handle) {
                            Some(b) => b,
                            None => break,
                        };
                        if bridge.data_tx.try_send((chunk, src)).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    /// Drop sockets whose user-side has fully closed (the bridge task has
    /// exited and the smoltcp socket is no longer active).
    ///
    /// We treat `TimeWait` as already reapable: smoltcp keeps a TIME-WAIT
    /// socket around for 2×MSL (~4 min) before transitioning to `Closed`,
    /// and `is_active()` reports `true` the entire time. Without an explicit
    /// `TimeWait` opt-in the reap filter (`!is_active && !may_recv && !may_send`)
    /// skips them, so a steady stream of short-lived HTTP/QUIC connections
    /// piles up ~256 KiB each in the SocketSet for minutes — confirmed in the
    /// field as the dominant contributor to NetExt RSS drift. The 2×MSL guard
    /// exists to keep stray delayed segments from being misrouted to a new
    /// connection that reuses the same 5-tuple, but on a userspace stack
    /// where ephemeral ports are issued by `alloc_port()` and never collide
    /// with a still-occupied tuple, the cost dominates the benefit.
    fn reap_dead_sockets(&mut self) {
        use smoltcp::socket::tcp::State;
        let stale_tcp: Vec<SocketHandle> = self
            .tcp_bridges
            .keys()
            .copied()
            .filter(|handle| {
                let socket = self.sockets.get::<tcp::Socket>(*handle);
                socket.state() == State::TimeWait
                    || (!socket.is_active() && !socket.may_recv() && !socket.may_send())
            })
            .collect();
        let reaped = stale_tcp.len();
        for handle in stale_tcp {
            self.fail_pending_tcp_writes(handle, "ipstack tcp: socket closed");
            self.sockets.remove(handle);
            self.tcp_bridges.remove(&handle);
        }
        if reaped > 0 {
            tracing::debug!(
                reaped,
                tcp_bridges = self.tcp_bridges.len(),
                pending_dials = self.pending_dials.len(),
                pending_accepts = self.pending_accepts.len(),
                udp_bridges = self.udp_bridges.len(),
                "ipstack reap_dead_sockets"
            );
        }
    }

    fn alloc_port(&mut self) -> u16 {
        // Linear scan inside the ephemeral range. This shadowing scheme is
        // sufficient for a single-stack setup; if we ever reach 16k live
        // sockets on one stack instance, we have bigger problems.
        let port = self.next_port;
        self.next_port = self.next_port.wrapping_add(1);
        if self.next_port < EPHEMERAL_FLOOR {
            self.next_port = EPHEMERAL_FLOOR;
        }
        port
    }

    fn smol_now(&self) -> SmolInstant {
        let elapsed = TokioInstant::now().saturating_duration_since(self.started);
        SmolInstant::from_micros(elapsed.as_micros() as i64)
    }

    fn tokio_from_smol(&self, inst: SmolInstant) -> TokioInstant {
        let micros = inst.total_micros().max(0) as u64;
        self.started + Duration::from_micros(micros)
    }
}

async fn tcp_bridge_task(
    handle: SocketHandle,
    mut duplex: DuplexStream,
    mut data_rx: mpsc::Receiver<Bytes>,
    event_tx: mpsc::Sender<SocketEvent>,
) {
    // `read_buf` writes straight into the BytesMut without going through a
    // local stack buffer, and `split().freeze()` turns each chunk into an
    // owned `Bytes` we can ship across the actor boundary without memcpy.
    let mut buf = BytesMut::with_capacity(TCP_BUF);
    let mut user_write_open = true;
    loop {
        // Once the user has signalled write-EOF the bridge is purely draining
        // anything smoltcp still hands back (FIN_WAIT_2 tail data, peer-side
        // late ACK packets) until the actor reaps the socket. Drop to the
        // tighter drain timeout so a slow / unreachable peer can't hold us
        // for the full active-keepalive window.
        let idle_timeout = if user_write_open {
            TCP_BRIDGE_IDLE_TIMEOUT
        } else {
            TCP_BRIDGE_DRAIN_IDLE_TIMEOUT
        };
        tokio::select! {
            // User wrote -> we read from the actor stage and forward to socket.
            res = duplex.read_buf(&mut buf), if user_write_open => {
                match res {
                    Ok(0) => {
                        user_write_open = false;
                        if send_tcp_close(&event_tx, handle, TcpCloseMode::GracefulWriteEof).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        let _ = send_tcp_close(&event_tx, handle, TcpCloseMode::Abort).await;
                        break;
                    }
                    Ok(_) => {
                        let chunk = buf.split().freeze();
                        // `split` leaves the BytesMut empty; reserve so the
                        // next read has somewhere to land without growing.
                        buf.reserve(TCP_BUF);
                        let (ack_tx, ack_rx) = oneshot::channel();
                        if event_tx
                            .send(SocketEvent::TcpWrite {
                                handle,
                                data: chunk,
                                ack: ack_tx,
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                        match ack_rx.await {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) | Err(_) => break,
                        }
                    }
                }
            }
            // Actor recv'd from socket -> we write to user.
            data = data_rx.recv() => {
                let Some(data) = data else { break };
                if duplex.write_all(&data).await.is_err() {
                    let _ = send_tcp_close(&event_tx, handle, TcpCloseMode::Abort).await;
                    break;
                }
            }
            // Defensive idle backstop: if neither side has produced traffic
            // in the current idle window, assume the upstream forgot the
            // stream and let the bridge exit so the socket can be reaped.
            _ = tokio::time::sleep(idle_timeout) => {
                tracing::debug!(?idle_timeout, "ipstack tcp bridge idle timeout, closing");
                let _ = send_tcp_close(&event_tx, handle, TcpCloseMode::Abort).await;
                break;
            }
        }
    }
}

async fn send_tcp_close(
    event_tx: &mpsc::Sender<SocketEvent>,
    handle: SocketHandle,
    mode: TcpCloseMode,
) -> Result<(), mpsc::error::SendError<SocketEvent>> {
    event_tx.send(SocketEvent::TcpClose { handle, mode }).await
}

async fn udp_bridge_task(
    handle: SocketHandle,
    mut send_rx: mpsc::Receiver<(Bytes, SocketAddr)>,
    event_tx: mpsc::Sender<SocketEvent>,
) {
    while let Some((data, dst)) = send_rx.recv().await {
        if event_tx
            .send(SocketEvent::UdpSend { handle, data, dst })
            .await
            .is_err()
        {
            break;
        }
    }
    let _ = event_tx.send(SocketEvent::UdpClose { handle }).await;
}

fn ipnet_to_cidr(net: &IpNet) -> Option<IpCidr> {
    match net {
        IpNet::V4(v4) => {
            let octets = v4.addr().octets();
            Some(IpCidr::new(
                IpAddress::v4(octets[0], octets[1], octets[2], octets[3]),
                v4.prefix_len(),
            ))
        }
        IpNet::V6(v6) => {
            let segs = v6.addr().segments();
            Some(IpCidr::new(
                IpAddress::v6(
                    segs[0], segs[1], segs[2], segs[3], segs[4], segs[5], segs[6], segs[7],
                ),
                v6.prefix_len(),
            ))
        }
    }
}

fn socket_endpoint_for(addr: SocketAddr) -> Option<IpEndpoint> {
    match addr.ip() {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            Some(IpEndpoint::new(
                IpAddress::v4(o[0], o[1], o[2], o[3]),
                addr.port(),
            ))
        }
        IpAddr::V6(v6) => {
            let s = v6.segments();
            Some(IpEndpoint::new(
                IpAddress::v6(s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]),
                addr.port(),
            ))
        }
    }
}

fn ip_endpoint_to_socket(endpoint: IpEndpoint) -> SocketAddr {
    let ip: IpAddr = match endpoint.addr {
        IpAddress::Ipv4(v4) => IpAddr::V4(std::net::Ipv4Addr::from(v4.octets())),
        IpAddress::Ipv6(v6) => IpAddr::V6(std::net::Ipv6Addr::from(v6.octets())),
    };
    SocketAddr::new(ip, endpoint.port)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use crate::log::{DiscardWriter, Factory};

    use super::*;

    #[test]
    fn tcp_bridge_idle_timeout_stays_within_netext_memory_budget() {
        assert!(
            TCP_BRIDGE_IDLE_TIMEOUT <= Duration::from_secs(60),
            "idle TCP bridge timeout retains about 256 KiB per keepalive connection"
        );
    }

    #[test]
    fn udp_payload_buffer_limit_stays_within_mobile_memory_budget() {
        assert!(
            UDP_PAYLOAD_LIMIT <= 8 * 1024,
            "each WireGuard UDP flow allocates one rx and one tx payload buffer"
        );
    }

    fn test_logger() -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger("ipstack-test")
    }

    /// Each `bind_udp` allocates a smoltcp UDP socket carrying ~128 KiB of rx/tx
    /// payload buffers. When the user drops the `UdpHandle`, the bridge task
    /// fires `UdpClose` so the actor `socket.close()`s the smoltcp socket — but
    /// `close()` only unbinds, it does not free the SocketSet slot. Without an
    /// explicit `sockets.remove(handle)` the slot lingers forever, the buffers
    /// stay resident, and every `iface.poll` keeps walking the dead socket.
    /// DNS-over-UDP via the wireguard outbound binds one of these per query, so
    /// this leak shows up as steadily-growing NetExt RSS during normal browsing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bind_udp_then_drop_releases_smoltcp_socket_slot() {
        let logger = test_logger();
        let addrs = vec!["10.0.0.1/32".parse::<IpNet>().expect("local addr")];
        let (_inbound_tx, inbound_rx) = mpsc::channel(16);
        let (encrypt_tx, _encrypt_rx) = mpsc::channel(16);
        let stack = spawn_ipstack(logger, addrs, 1408, inbound_rx, encrypt_tx).expect("spawn");

        const ITERATIONS: usize = 32;
        for _ in 0..ITERATIONS {
            let handle = stack.bind_udp().await.expect("bind_udp");
            drop(handle);
        }

        // Bridge tasks signal `UdpClose` asynchronously; wait until the actor
        // has drained them. A handful of poll cycles is plenty.
        let mut final_count = ITERATIONS;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            final_count = stack.debug_udp_socket_count().await;
            if final_count == 0 {
                break;
            }
        }

        assert_eq!(
            final_count, 0,
            "smoltcp SocketSet retained closed UDP sockets — \
             {final_count} of {ITERATIONS} bind/drop cycles leaked"
        );

        stack.signal_shutdown();
    }

    /// After the user signals write-EOF, the bridge enters drain mode and
    /// must time out at `TCP_BRIDGE_DRAIN_IDLE_TIMEOUT` (15 s, sing-box
    /// `TCPTimeout`) rather than the 60 s active-keepalive window. Without
    /// this each graceful-close stream sits on ~256 KiB of bridge + smoltcp
    /// buffers for the full minute, which is the dominant NetExt RSS-drift
    /// source under sustained browsing (Bilibili infinite scroll etc.).
    #[tokio::test(start_paused = true)]
    async fn tcp_bridge_drain_timeout_fires_within_drain_window_after_user_eof() {
        let (mut user_side, actor_side) = tokio::io::duplex(64 * 1024);
        let (_data_tx, data_rx) = mpsc::channel::<Bytes>(16);
        let (event_tx, mut event_rx) = mpsc::channel::<SocketEvent>(8);

        let task = tokio::spawn(tcp_bridge_task(
            SocketHandle::default(),
            actor_side,
            data_rx,
            event_tx,
        ));

        // User-side EOF (write half closed) → bridge sends GracefulWriteEof
        // and flips into drain mode. We hold `_data_tx` so the bridge keeps
        // looping on `data_rx.recv()` (simulating an actor that has not yet
        // observed the FIN and reaped the socket).
        user_side.shutdown().await.expect("shutdown");

        let first = event_rx.recv().await.expect("graceful event");
        assert!(
            matches!(
                first,
                SocketEvent::TcpClose {
                    mode: TcpCloseMode::GracefulWriteEof,
                    ..
                }
            ),
            "expected GracefulWriteEof, got {first:?}"
        );

        // Just under the drain window: bridge must still be alive.
        tokio::time::advance(Duration::from_secs(14)).await;
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "bridge exited before drain timeout elapsed"
        );

        // Cross the threshold; drain timeout must fire and abort.
        tokio::time::advance(Duration::from_secs(2)).await;
        let abort = event_rx.recv().await.expect("abort event");
        assert!(
            matches!(
                abort,
                SocketEvent::TcpClose {
                    mode: TcpCloseMode::Abort,
                    ..
                }
            ),
            "expected Abort after drain timeout, got {abort:?}"
        );
        task.await.expect("bridge task joined");

        // And the drain timeout must be strictly tighter than the active
        // keepalive window — otherwise the whole point of the split is moot.
        assert!(TCP_BRIDGE_DRAIN_IDLE_TIMEOUT < TCP_BRIDGE_IDLE_TIMEOUT);
    }
}
