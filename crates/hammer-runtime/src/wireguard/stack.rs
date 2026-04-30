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

use std::collections::HashMap;
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
}

/// Messages that the per-socket bridge tasks send back to the actor. Each
/// carries the `SocketHandle` so the actor's single mpsc receiver can demux
/// without a fan-in struct per socket.
enum SocketEvent {
    TcpWrite {
        handle: SocketHandle,
        data: Vec<u8>,
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
    pending_tx: Vec<u8>,
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

        self.drain_tcp_recv();
        self.retry_pending_tcp_writes();
        self.drain_udp_recv();
        self.reap_dead_sockets();
    }

    fn handle_event(&mut self, event: SocketEvent) {
        match event {
            SocketEvent::TcpWrite { handle, data } => {
                let bridge = match self.tcp_bridges.get_mut(&handle) {
                    Some(b) => b,
                    None => return,
                };
                let socket = self.sockets.get_mut::<tcp::Socket>(handle);
                if socket.may_send() {
                    match socket.send_slice(&data) {
                        Ok(n) if n == data.len() => {}
                        Ok(n) => {
                            bridge.pending_tx.extend_from_slice(&data[n..]);
                        }
                        Err(err) => {
                            self.logger
                                .warn(format!("wireguard tcp send_slice: {err:?}"));
                        }
                    }
                } else {
                    bridge.pending_tx.extend_from_slice(&data);
                }
            }
            SocketEvent::TcpClose { handle } => {
                if self.tcp_bridges.contains_key(&handle) {
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
                let _ = reply.send(self.dial_tcp(dst));
            }
            StackCommand::BindUdp { reply } => {
                let _ = reply.send(self.bind_udp());
            }
        }
    }

    fn dial_tcp(&mut self, dst: SocketAddr) -> Result<DuplexStream, HammerError> {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; TCP_BUF]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);

        let local_port = self.alloc_port();
        let remote = match socket_endpoint_for(dst) {
            Some(ep) => ep,
            None => {
                return Err(HammerError::internal(format!(
                    "wireguard dial: unsupported destination {dst}"
                )));
            }
        };
        socket
            .connect(self.iface.context(), remote, local_port)
            .map_err(|err| HammerError::internal(format!("wireguard tcp connect: {err:?}")))?;

        let handle = self.sockets.add(socket);

        let (user_side, actor_side) = tokio::io::duplex(TCP_DUPLEX);
        let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(TCP_DATA_QUEUE);
        let event_tx = self.event_tx.clone();
        tokio::spawn(tcp_bridge_task(handle, actor_side, data_rx, event_tx));

        self.tcp_bridges.insert(
            handle,
            TcpBridge {
                data_tx,
                pending_tx: Vec::new(),
            },
        );

        Ok(user_side)
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
                let mut buf = vec![0u8; TCP_BUF];
                match socket.recv_slice(&mut buf) {
                    Ok(n) if n > 0 => {
                        buf.truncate(n);
                        let bridge = match self.tcp_bridges.get(&handle) {
                            Some(b) => b,
                            None => break,
                        };
                        if bridge.data_tx.try_send(buf).is_err() {
                            // bridge channel full or closed — wait for next poll
                            break;
                        }
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
            match socket.send_slice(&bridge.pending_tx) {
                Ok(n) if n > 0 => {
                    bridge.pending_tx.drain(..n);
                }
                _ => {}
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
            // User wrote -> we read from the actor side and forward to socket.
            res = duplex.read(&mut buf) => {
                match res {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if event_tx
                            .send(SocketEvent::TcpWrite {
                                handle,
                                data: buf[..n].to_vec(),
                            })
                            .await
                            .is_err()
                        {
                            break;
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
