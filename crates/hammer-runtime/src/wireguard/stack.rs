//! User-space netstack actor for the WireGuard endpoint.
//!
//! Equivalent to sing-box's `transport/wireguard/device_stack.go::StackDevice`,
//! which wraps the gVisor `tcpip.Stack` around a wireguard-go `tun.Device`.
//! Hammer's version owns a `smoltcp::iface::Interface` plus a `SocketSet`,
//! drains decrypted inbound IP packets from the transport actor, and feeds
//! the IP packets that smoltcp emits back into the transport for boringtun
//! encryption. It also services `dial(TCP)` / `dial(UDP)` requests from
//! `WireguardEndpoint`, returning user-side `tokio::io::DuplexStream` /
//! `ProxyPacketConn` wrappers that bridge tokio AsyncRead/AsyncWrite into
//! smoltcp's synchronous socket API.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use ipnet::IpNet;
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, IpEndpoint, IpListenEndpoint};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant as TokioInstant, sleep_until};

use hammer_adapter::{ProxyDatagram, ProxyPacketConn, SocksAddr};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use super::device::WireguardDevice;

/// Per-TCP-socket smoltcp ring buffer size. 64 KiB matches a typical Linux
/// SO_RCVBUF default and is plenty for the bursty HTTP/QUIC payloads we expect
/// to multiplex over a wg tunnel.
const TCP_BUF: usize = 64 * 1024;
/// Buffer between the actor and a TCP bridge task — small because we drain
/// it immediately after every iface.poll cycle.
const TCP_DATA_QUEUE: usize = 16;
/// UDP payload buffer slots inside smoltcp.
const UDP_PACKET_SLOTS: usize = 32;
/// Largest IP datagram smoltcp will surface to a UDP socket. 64 KiB lets
/// jumbograms through but caps the worst-case allocation per packet.
const UDP_PAYLOAD_LIMIT: usize = 64 * 1024;
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
/// Upper bound for waiting on the in-tunnel TCP three-way handshake. A closed
/// peer normally fails much faster via RST; this keeps blackholed connects from
/// parking an outbound dial forever.
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct StackHandles {
    cmd_tx: mpsc::Sender<StackCommand>,
    /// Wrapped in a Mutex<Option<...>> so the consumer can call `&self`
    /// methods (clone an Arc<StackHandles> and `dial`) while still being
    /// able to take the sender once at shutdown.
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    join: JoinHandle<()>,
}

impl StackHandles {
    pub(crate) async fn dial_tcp(&self, dst: SocketAddr) -> Result<DuplexStream, HammerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(StackCommand::DialTcp {
                dst,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HammerError::internal("wireguard stack: actor closed"))?;
        reply_rx
            .await
            .map_err(|_| HammerError::internal("wireguard stack: dial response dropped"))?
    }

    pub(crate) async fn bind_udp(&self) -> Result<UdpHandle, HammerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(StackCommand::BindUdp { reply: reply_tx })
            .await
            .map_err(|_| HammerError::internal("wireguard stack: actor closed"))?;
        reply_rx
            .await
            .map_err(|_| HammerError::internal("wireguard stack: bind response dropped"))?
    }

    /// Build a `TcpListener` bound to `port` inside the tunnel. The listener
    /// is just a thin command sender — the actual smoltcp socket is created
    /// per-`accept()` call so the stack supports an unbounded series of
    /// inbound connections without juggling backlog state itself.
    pub(crate) fn listen_tcp(&self, port: u16) -> TcpListener {
        TcpListener {
            cmd_tx: self.cmd_tx.clone(),
            port,
        }
    }

    /// Tell the actor task to exit at its next select! poll. Idempotent —
    /// subsequent calls find the sender already taken.
    pub(crate) fn signal_shutdown(&self) {
        if let Some(tx) = self
            .shutdown
            .lock()
            .expect("StackHandles shutdown poisoned")
            .take()
        {
            let _ = tx.send(());
        }
    }

    /// Force the actor task to drop without waiting. Pairs with
    /// `signal_shutdown` for the lifecycle close path.
    pub(crate) fn abort(&self) {
        self.join.abort();
    }
}

/// Inbound-TCP listener bound to a port inside the tunnel. `accept()` runs
/// one acceptance cycle per call: it sends an `AcceptTcp` command to the
/// stack actor, which spins up a fresh smoltcp listening socket on that port,
/// and replies with a `DuplexStream` once the three-way handshake completes.
/// Concurrent accepts are fine — each gets its own socket.
pub(crate) struct TcpListener {
    cmd_tx: mpsc::Sender<StackCommand>,
    port: u16,
}

impl TcpListener {
    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    pub(crate) async fn accept(&self) -> Result<DuplexStream, HammerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(StackCommand::AcceptTcp {
                port: self.port,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HammerError::internal("wireguard stack: actor closed"))?;
        reply_rx
            .await
            .map_err(|_| HammerError::internal("wireguard stack: accept response dropped"))?
    }
}

pub(crate) struct UdpHandle {
    send_tx: mpsc::Sender<(Vec<u8>, SocketAddr)>,
    recv_rx: AsyncMutex<mpsc::Receiver<(Vec<u8>, SocketAddr)>>,
    /// Port allocated by the actor inside the smoltcp Interface. Useful for
    /// integration tests that need to address one side from the other through
    /// the tunnel.
    local_port: u16,
}

impl UdpHandle {
    pub(crate) fn local_port(&self) -> u16 {
        self.local_port
    }
}

#[async_trait]
impl ProxyPacketConn for UdpHandle {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> Result<(), HammerError> {
        let dst = SocketAddr::new(destination.host, destination.port);
        self.send_tx
            .send((payload.to_vec(), dst))
            .await
            .map_err(|_| HammerError::internal("wireguard udp: stack closed"))
    }

    async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        let mut rx = self.recv_rx.lock().await;
        let (payload, src) = rx
            .recv()
            .await
            .ok_or_else(|| HammerError::internal("wireguard udp: stack closed"))?;
        Ok(ProxyDatagram {
            destination: SocksAddr {
                host: src.ip(),
                port: src.port(),
            },
            payload,
        })
    }
}

/// Spin up the netstack actor. Synchronously builds the smoltcp interface
/// (no I/O involved) so the caller has a fully-wired `StackHandles` before the
/// task even runs its first iteration.
pub(crate) fn spawn_stack(
    logger: Logger,
    addresses: Vec<IpNet>,
    mtu: u32,
    inbound_rx: mpsc::Receiver<Vec<u8>>,
    encrypt_tx: mpsc::Sender<Vec<u8>>,
) -> Result<StackHandles, HammerError> {
    if addresses.is_empty() {
        return Err(HammerError::internal(
            "wireguard stack requires at least one local IP address",
        ));
    }

    let (egress_tx, egress_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let mut device = WireguardDevice::new(mtu, egress_tx);

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
    // Accept packets addressed to anyone — the wg tunnel is point-to-point so
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
        tcp_bridges: HashMap::new(),
        udp_bridges: HashMap::new(),
        pending_accepts: Vec::new(),
        pending_dials: Vec::new(),
        next_port: EPHEMERAL_FLOOR,
        started,
    };

    let join = tokio::spawn(inner.run());

    Ok(StackHandles {
        cmd_tx,
        shutdown: std::sync::Mutex::new(Some(shutdown_tx)),
        join,
    })
}

enum StackCommand {
    DialTcp {
        dst: SocketAddr,
        reply: oneshot::Sender<Result<DuplexStream, HammerError>>,
    },
    BindUdp {
        reply: oneshot::Sender<Result<UdpHandle, HammerError>>,
    },
    AcceptTcp {
        port: u16,
        reply: oneshot::Sender<Result<DuplexStream, HammerError>>,
    },
}

/// One in-flight inbound TCP accept. The listening smoltcp socket sits in
/// `SocketSet`; once the three-way handshake completes (`may_recv` &&
/// `may_send`) we hand the established socket off to a freshly-spawned
/// bridge task and reply to `oneshot` with the user-side `DuplexStream`.
struct PendingAccept {
    handle: SocketHandle,
    reply: oneshot::Sender<Result<DuplexStream, HammerError>>,
}

/// One outbound TCP connect waiting for smoltcp to reach ESTABLISHED (or fail).
struct PendingDial {
    handle: SocketHandle,
    dst: SocketAddr,
    reply: oneshot::Sender<Result<DuplexStream, HammerError>>,
    deadline: TokioInstant,
}

/// Messages that the per-socket bridge tasks send back to the actor. Each
/// carries the `SocketHandle` so the actor's single mpsc receiver can demux
/// without a fan-in struct per socket.
enum SocketEvent {
    TcpWrite {
        handle: SocketHandle,
        data: Vec<u8>,
        ack: oneshot::Sender<Result<(), HammerError>>,
    },
    TcpClose {
        handle: SocketHandle,
    },
    UdpSend {
        handle: SocketHandle,
        data: Vec<u8>,
        dst: SocketAddr,
    },
    UdpClose {
        handle: SocketHandle,
    },
}

struct TcpBridge {
    /// Channel for the actor to push socket recv data into the bridge task.
    data_tx: mpsc::Sender<Vec<u8>>,
    /// Bytes the actor is still trying to push into the smoltcp tx buffer
    /// (didn't fit in a single send_slice call).
    pending_tx: VecDeque<PendingTcpWrite>,
}

struct PendingTcpWrite {
    data: Vec<u8>,
    offset: usize,
    ack: Option<oneshot::Sender<Result<(), HammerError>>>,
}

struct UdpBridge {
    /// Channel for the actor to push (payload, src) to the bridge task.
    data_tx: mpsc::Sender<(Vec<u8>, SocketAddr)>,
}

struct StackInner {
    logger: Logger,
    iface: Interface,
    sockets: SocketSet<'static>,
    device: WireguardDevice,
    egress_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    encrypt_tx: mpsc::Sender<Vec<u8>>,
    inbound_rx: mpsc::Receiver<Vec<u8>>,
    cmd_rx: mpsc::Receiver<StackCommand>,
    event_rx: mpsc::Receiver<SocketEvent>,
    event_tx: mpsc::Sender<SocketEvent>,
    shutdown_rx: oneshot::Receiver<()>,
    tcp_bridges: HashMap<SocketHandle, TcpBridge>,
    udp_bridges: HashMap<SocketHandle, UdpBridge>,
    pending_accepts: Vec<PendingAccept>,
    pending_dials: Vec<PendingDial>,
    next_port: u16,
    started: TokioInstant,
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
                    self.logger.debug("wireguard stack: shutdown");
                    return;
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_command(cmd).await;
                }
                Some(packet) = self.inbound_rx.recv() => {
                    self.device.deliver(packet);
                }
                Some(event) = self.event_rx.recv() => {
                    self.handle_event(event);
                }
                _ = sleep_until(next_at) => { /* time to poll */ }
            }
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
                self.logger
                    .warn("wireguard stack: encrypt_tx closed, transport stopped");
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
                        "wireguard tcp listen aborted before connection established",
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
                            "wireguard tcp connect failed to {}: timeout",
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
                        "wireguard tcp connect failed to {}: {:?}",
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
            SocketEvent::TcpClose { handle } => {
                if self.tcp_bridges.contains_key(&handle) {
                    self.fail_pending_tcp_writes(handle, "wireguard tcp: user stream closed");
                    let socket = self.sockets.get_mut::<tcp::Socket>(handle);
                    socket.close();
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
                    self.logger
                        .warn(format!("wireguard udp send_slice: {err:?}"));
                }
            }
            SocketEvent::UdpClose { handle } => {
                if self.udp_bridges.remove(&handle).is_some() {
                    let socket = self.sockets.get_mut::<udp::Socket>(handle);
                    socket.close();
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
        }
    }

    /// Park a fresh listening socket and remember the reply channel. The next
    /// poll cycle will notice when it transitions to ESTABLISHED and hand the
    /// caller a `DuplexStream`.
    fn queue_accept(
        &mut self,
        port: u16,
        reply: oneshot::Sender<Result<DuplexStream, HammerError>>,
    ) {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);
        if let Err(err) = socket.listen(port) {
            let _ = reply.send(Err(HammerError::internal(format!(
                "wireguard tcp listen on {port}: {err:?}"
            ))));
            return;
        }
        let handle = self.sockets.add(socket);
        self.pending_accepts.push(PendingAccept { handle, reply });
    }

    fn queue_dial(
        &mut self,
        dst: SocketAddr,
        reply: oneshot::Sender<Result<DuplexStream, HammerError>>,
    ) {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);

        let local_port = self.alloc_port();
        let remote = match socket_endpoint_for(dst) {
            Some(ep) => ep,
            None => {
                let _ = reply.send(Err(HammerError::internal(format!(
                    "wireguard dial: unsupported destination {dst}"
                ))));
                return;
            }
        };
        if let Err(err) = socket.connect(self.iface.context(), remote, local_port) {
            let _ = reply.send(Err(HammerError::internal(format!(
                "wireguard tcp connect: {err:?}"
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
        let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(TCP_DATA_QUEUE);
        let event_tx = self.event_tx.clone();
        tokio::spawn(tcp_bridge_task(handle, actor_side, data_rx, event_tx));
        self.tcp_bridges.insert(
            handle,
            TcpBridge {
                data_tx,
                pending_tx: VecDeque::new(),
            },
        );
        user_side
    }

    fn handle_tcp_write(
        &mut self,
        handle: SocketHandle,
        data: Vec<u8>,
        ack: oneshot::Sender<Result<(), HammerError>>,
    ) {
        let Some(bridge) = self.tcp_bridges.get_mut(&handle) else {
            let _ = ack.send(Err(HammerError::internal("wireguard tcp: socket closed")));
            return;
        };

        if !bridge.pending_tx.is_empty() {
            bridge.pending_tx.push_back(PendingTcpWrite {
                data,
                offset: 0,
                ack: Some(ack),
            });
            return;
        }

        let written = self.write_tcp_slice(handle, &data);
        if written >= data.len() {
            let _ = ack.send(Ok(()));
            return;
        }

        if let Some(bridge) = self.tcp_bridges.get_mut(&handle) {
            bridge.pending_tx.push_back(PendingTcpWrite {
                data,
                offset: written,
                ack: Some(ack),
            });
        } else {
            let _ = ack.send(Err(HammerError::internal("wireguard tcp: socket closed")));
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
                self.logger
                    .debug(format!("wireguard tcp send_slice deferred: {err:?}"));
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

    fn bind_udp(&mut self) -> Result<UdpHandle, HammerError> {
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
            .map_err(|err| HammerError::internal(format!("wireguard udp bind: {err:?}")))?;
        let handle = self.sockets.add(socket);

        let (recv_user_tx, recv_user_rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(UDP_QUEUE);
        let (send_user_tx, send_user_rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(UDP_QUEUE);
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
        let handles: Vec<SocketHandle> = self.tcp_bridges.keys().copied().collect();
        for handle in handles {
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
                let mut buf = vec![0u8; TCP_BUF];
                match socket.recv_slice(&mut buf) {
                    Ok(n) if n > 0 => {
                        buf.truncate(n);
                        permit.send(buf);
                    }
                    _ => break,
                }
            }
        }
    }

    fn retry_pending_tcp_writes(&mut self) {
        let handles: Vec<SocketHandle> = self.tcp_bridges.keys().copied().collect();
        for handle in handles {
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
            loop {
                let Some(pending) = bridge.pending_tx.front_mut() else {
                    break;
                };
                if !socket.may_send() {
                    break;
                }
                match socket.send_slice(&pending.data[pending.offset..]) {
                    Ok(0) => break,
                    Ok(n) => {
                        pending.offset += n;
                        if pending.offset < pending.data.len() {
                            break;
                        }
                        let mut done = bridge.pending_tx.pop_front().expect("front exists");
                        if let Some(ack) = done.ack.take() {
                            let _ = ack.send(Ok(()));
                        }
                    }
                    Err(err) => {
                        self.logger
                            .debug(format!("wireguard tcp retry deferred: {err:?}"));
                        break;
                    }
                }
            }
        }
    }

    fn drain_udp_recv(&mut self) {
        let handles: Vec<SocketHandle> = self.udp_bridges.keys().copied().collect();
        for handle in handles {
            let socket = self.sockets.get_mut::<udp::Socket>(handle);
            while socket.can_recv() {
                let mut buf = vec![0u8; UDP_PAYLOAD_LIMIT];
                match socket.recv_slice(&mut buf) {
                    Ok((n, meta)) => {
                        buf.truncate(n);
                        let src = ip_endpoint_to_socket(meta.endpoint);
                        let bridge = match self.udp_bridges.get(&handle) {
                            Some(b) => b,
                            None => break,
                        };
                        if bridge.data_tx.try_send((buf, src)).is_err() {
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
    fn reap_dead_sockets(&mut self) {
        let stale_tcp: Vec<SocketHandle> = self
            .tcp_bridges
            .keys()
            .copied()
            .filter(|handle| {
                let socket = self.sockets.get::<tcp::Socket>(*handle);
                !socket.is_active() && !socket.may_recv() && !socket.may_send()
            })
            .collect();
        for handle in stale_tcp {
            self.fail_pending_tcp_writes(handle, "wireguard tcp: socket closed");
            self.sockets.remove(handle);
            self.tcp_bridges.remove(&handle);
        }
    }

    fn alloc_port(&mut self) -> u16 {
        // Linear scan inside the ephemeral range. This shadowing scheme is
        // sufficient for a single-tunnel setup; if we ever reach 16k live
        // sockets on one wg endpoint, we have bigger problems.
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
    mut data_rx: mpsc::Receiver<Vec<u8>>,
    event_tx: mpsc::Sender<SocketEvent>,
) {
    let mut buf = vec![0u8; TCP_BUF];
    loop {
        tokio::select! {
            // User wrote -> we read from the actor stage and forward to socket.
            res = duplex.read(&mut buf) => {
                match res {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let (ack_tx, ack_rx) = oneshot::channel();
                        if event_tx
                            .send(SocketEvent::TcpWrite {
                                handle,
                                data: buf[..n].to_vec(),
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
                    break;
                }
            }
        }
    }
    let _ = event_tx.send(SocketEvent::TcpClose { handle }).await;
}

async fn udp_bridge_task(
    handle: SocketHandle,
    mut send_rx: mpsc::Receiver<(Vec<u8>, SocketAddr)>,
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
