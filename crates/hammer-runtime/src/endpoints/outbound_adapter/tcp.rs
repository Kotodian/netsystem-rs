//! TCP termination via smoltcp for the `EndpointOutboundAdapter`.
//!
//! `outbound.dial(Network::Tcp, …)` enters here. We spin up a single-socket
//! smoltcp `Interface` per dial: the driver task owns the smoltcp state,
//! feeds outbound IP packets into the endpoint's encrypt channel, and
//! consumes inbound IP packets the adapter demux routes our way. The
//! `EndpointTcpStream` returned to the DNS / DoH transport implements
//! `AsyncRead` + `AsyncWrite` and talks to the driver via two byte
//! channels.
//!
//! Why per-dial drivers instead of one shared `SocketSet`: Phase 1's
//! workload is 1-3 concurrent sockets (DNS UDP retries + DoH), so the
//! overhead of an extra task per dial is negligible and we sidestep the
//! locking/handle-bookkeeping shared-set would require.

#![cfg(feature = "endpoint")]

use std::collections::VecDeque;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hammer_adapter::ProxyStream;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::log::Logger;
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::tcp::{Socket as SmoltcpTcpSocket, SocketBuffer, State as TcpState};
use smoltcp::time::{Duration as SmoltcpDuration, Instant as SmoltcpInstant};
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, Ipv4Address, Ipv4Cidr};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep_until;

const TCP_RX_BUF: usize = 4 * 1024;
const TCP_TX_BUF: usize = 8 * 1024;
const USER_CHAN_QUEUE: usize = 8;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_TIMEOUT_FLOOR: Duration = Duration::from_millis(50);

/// Parameters for a single TCP dial. The adapter constructs this before
/// spawning the driver task.
pub(super) struct TcpDialParams {
    pub logger: Logger,
    pub interface_v4: Ipv4Addr,
    pub local_port: u16,
    pub dst_v4: Ipv4Addr,
    pub dst_port: u16,
    pub mtu: usize,
    /// Driver-side outbound packets. Routed straight into
    /// `Endpoint::ip_send_clone`.
    pub egress_tx: mpsc::Sender<Bytes>,
    /// Driver-side inbound packets. The adapter demux task feeds this
    /// channel with IPv4-TCP packets whose dst port matches `local_port`.
    pub ingress_rx: mpsc::Receiver<Bytes>,
    /// Bytes supplied to `Outbound::dial(..., initial_payload)` that must be
    /// written as soon as the smoltcp connection is established.
    pub initial_payload: Bytes,
    /// Hook the adapter wires up to deregister this flow when the driver
    /// shuts down (socket close, EOF, error).
    pub on_close: Arc<dyn Fn() + Send + Sync>,
}

pub(super) async fn dial_tcp(params: TcpDialParams) -> CoreResult<Box<dyn ProxyStream>> {
    let (user_send_tx, user_send_rx) = mpsc::channel::<Bytes>(USER_CHAN_QUEUE);
    let (user_recv_tx, user_recv_rx) = mpsc::channel::<Bytes>(USER_CHAN_QUEUE);
    let (connect_tx, connect_rx) = oneshot::channel::<CoreResult<()>>();

    let logger = params.logger.clone();
    let task = crate::spawn::spawn(run_driver(DriverContext {
        logger,
        interface_v4: params.interface_v4,
        local_port: params.local_port,
        dst_v4: params.dst_v4,
        dst_port: params.dst_port,
        mtu: params.mtu,
        egress_tx: params.egress_tx,
        ingress_rx: params.ingress_rx,
        user_send_rx,
        user_recv_tx,
        connect_tx: Some(connect_tx),
        on_close: params.on_close,
    }));

    match tokio::time::timeout(CONNECT_TIMEOUT, connect_rx).await {
        Err(_) => {
            task.abort();
            Err(CoreError::internal("endpoint TCP connect timed out"))
        }
        Ok(Err(_)) => Err(CoreError::internal(
            "endpoint TCP driver dropped before connect",
        )),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Ok(Ok(()))) => {
            if !params.initial_payload.is_empty() {
                user_send_tx
                    .send(params.initial_payload)
                    .await
                    .map_err(|_| CoreError::internal("endpoint TCP driver gone"))?;
            }
            Ok(Box::new(EndpointTcpStream {
                send_tx: user_send_tx,
                recv_rx: user_recv_rx,
                recv_buf: VecDeque::new(),
                write_eof: false,
            }))
        }
    }
}

struct DriverContext {
    logger: Logger,
    interface_v4: Ipv4Addr,
    local_port: u16,
    dst_v4: Ipv4Addr,
    dst_port: u16,
    mtu: usize,
    egress_tx: mpsc::Sender<Bytes>,
    ingress_rx: mpsc::Receiver<Bytes>,
    user_send_rx: mpsc::Receiver<Bytes>,
    user_recv_tx: mpsc::Sender<Bytes>,
    connect_tx: Option<oneshot::Sender<CoreResult<()>>>,
    on_close: Arc<dyn Fn() + Send + Sync>,
}

async fn run_driver(mut ctx: DriverContext) {
    struct OnCloseGuard(Arc<dyn Fn() + Send + Sync>);
    impl Drop for OnCloseGuard {
        fn drop(&mut self) {
            (self.0)();
        }
    }
    let _guard = OnCloseGuard(Arc::clone(&ctx.on_close));

    let mut device = AdapterDevice {
        rx_queue: VecDeque::new(),
        tx_queue: VecDeque::new(),
        mtu: ctx.mtu,
    };

    let iface_config = Config::new(HardwareAddress::Ip);
    let mut iface = Interface::new(iface_config, &mut device, SmoltcpInstant::now());
    iface.update_ip_addrs(|addrs| {
        let cidr = IpCidr::Ipv4(Ipv4Cidr::new(
            Ipv4Address::from_octets(ctx.interface_v4.octets()),
            32,
        ));
        let _ = addrs.push(cidr);
    });

    let mut sockets = SocketSet::new(Vec::new());
    let rx_buf = SocketBuffer::new(vec![0u8; TCP_RX_BUF]);
    let tx_buf = SocketBuffer::new(vec![0u8; TCP_TX_BUF]);
    let handle = sockets.add(SmoltcpTcpSocket::new(rx_buf, tx_buf));

    {
        let socket = sockets.get_mut::<SmoltcpTcpSocket>(handle);
        let dst = (
            IpAddress::Ipv4(Ipv4Address::from_octets(ctx.dst_v4.octets())),
            ctx.dst_port,
        );
        if let Err(e) = socket.connect(iface.context(), dst, ctx.local_port) {
            if let Some(tx) = ctx.connect_tx.take() {
                let _ = tx.send(Err(CoreError::internal(format!(
                    "smoltcp connect refused: {e}"
                ))));
            }
            return;
        }
    }

    let mut user_send_closed = false;
    let mut user_recv_eof = false;
    let mut socket_close_requested = false;
    let mut pending_send: VecDeque<Bytes> = VecDeque::new();
    let mut pending_send_offset = 0usize;

    loop {
        let now = SmoltcpInstant::now();
        let _ = iface.poll(now, &mut device, &mut sockets);

        // Drain device.tx_queue → encrypt channel. Use try_send for
        // backpressure; await only when the channel is full to give the
        // socket buffer a chance to fill up rather than spin.
        while let Some(pkt) = device.tx_queue.pop_front() {
            if ctx.egress_tx.send(pkt).await.is_err() {
                ctx.logger
                    .debug("endpoint TCP driver: encrypt channel closed");
                return;
            }
        }

        // Signal connect status the first time the socket transitions out
        // of SynSent. `may_send` / `may_recv` are true in Established.
        let (state, may_send, may_recv) = {
            let socket = sockets.get::<SmoltcpTcpSocket>(handle);
            (socket.state(), socket.may_send(), socket.may_recv())
        };
        if let Some(tx) = ctx.connect_tx.take() {
            match state {
                TcpState::Established => {
                    let _ = tx.send(Ok(()));
                }
                TcpState::Closed | TcpState::Closing | TcpState::TimeWait => {
                    let _ = tx.send(Err(CoreError::internal("endpoint TCP connection refused")));
                    return;
                }
                _ => {
                    // not yet — re-stash and try again next loop
                    ctx.connect_tx = Some(tx);
                }
            }
        }

        // Move readable bytes from socket → user_recv channel.
        if may_recv && !user_recv_eof {
            let mut chunk: Vec<u8> = Vec::new();
            let socket = sockets.get_mut::<SmoltcpTcpSocket>(handle);
            if socket.recv_queue() > 0 {
                let _ = socket.recv(|data| {
                    chunk.extend_from_slice(data);
                    (data.len(), ())
                });
            }
            if !chunk.is_empty() {
                if ctx.user_recv_tx.send(Bytes::from(chunk)).await.is_err() {
                    // Reader gone — close write side too.
                    user_recv_eof = true;
                }
            }
        }

        if may_send {
            let socket = sockets.get_mut::<SmoltcpTcpSocket>(handle);
            while let Some(bytes) = pending_send.front() {
                let remaining = &bytes[pending_send_offset..];
                if remaining.is_empty() {
                    pending_send.pop_front();
                    pending_send_offset = 0;
                    continue;
                }
                match socket.send_slice(remaining) {
                    Ok(0) => break,
                    Ok(n) => {
                        pending_send_offset += n;
                        if pending_send_offset >= bytes.len() {
                            pending_send.pop_front();
                            pending_send_offset = 0;
                        }
                    }
                    Err(_) => break,
                }
            }
            if user_send_closed && pending_send.is_empty() && !socket_close_requested {
                socket.close();
                socket_close_requested = true;
            }
        }

        if ctx.connect_tx.is_none() && !may_recv {
            let socket = sockets.get::<SmoltcpTcpSocket>(handle);
            if socket.recv_queue() == 0 {
                ctx.logger.debug(format!(
                    "endpoint TCP driver: remote receive half closed ({state})"
                ));
                return;
            }
        }

        if matches!(
            state,
            TcpState::Closed | TcpState::TimeWait | TcpState::Closing | TcpState::LastAck
        ) && !may_send
            && !may_recv
        {
            ctx.logger.debug(format!(
                "endpoint TCP driver: socket {state} reached terminal"
            ));
            return;
        }

        // Pick next event.
        let next_poll_at = iface.poll_at(now, &sockets);
        let mut next_wait = match next_poll_at {
            Some(at) if at > now => duration_from_smoltcp(at - now).max(POLL_TIMEOUT_FLOOR),
            _ => POLL_TIMEOUT_FLOOR,
        };
        if next_wait > Duration::from_secs(5) {
            next_wait = Duration::from_secs(5);
        }
        let deadline = tokio::time::Instant::now() + next_wait;

        tokio::select! {
            biased;
            recv = ctx.ingress_rx.recv() => match recv {
                Some(pkt) => device.rx_queue.push_back(pkt),
                None => {
                    ctx.logger.debug("endpoint TCP driver: ingress closed");
                    return;
                }
            },
            send = ctx.user_send_rx.recv(), if !user_send_closed => match send {
                Some(bytes) => {
                    pending_send.push_back(bytes);
                }
                None => {
                    user_send_closed = true;
                }
            },
            _ = sleep_until(deadline) => {}
        }
    }
}

fn duration_from_smoltcp(d: SmoltcpDuration) -> Duration {
    Duration::from_micros(d.total_micros() as u64)
}

/// One-shot `Device` impl backed by VecDeques. smoltcp polls it on each
/// loop iteration; `rx_queue` is fed by the adapter demux task and
/// `tx_queue` is drained into the endpoint's encrypt channel.
struct AdapterDevice {
    rx_queue: VecDeque<Bytes>,
    tx_queue: VecDeque<Bytes>,
    mtu: usize,
}

impl Device for AdapterDevice {
    type RxToken<'a>
        = AdapterRxToken
    where
        Self: 'a;
    type TxToken<'a>
        = AdapterTxToken<'a>
    where
        Self: 'a;

    fn receive(
        &mut self,
        _timestamp: SmoltcpInstant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let buf = self.rx_queue.pop_front()?;
        let tx = AdapterTxToken {
            tx_queue: &mut self.tx_queue,
            mtu: self.mtu,
        };
        Some((AdapterRxToken { buf }, tx))
    }

    fn transmit(&mut self, _timestamp: SmoltcpInstant) -> Option<Self::TxToken<'_>> {
        Some(AdapterTxToken {
            tx_queue: &mut self.tx_queue,
            mtu: self.mtu,
        })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.max_transmission_unit = self.mtu;
        caps.medium = Medium::Ip;
        caps
    }
}

struct AdapterRxToken {
    buf: Bytes,
}

impl RxToken for AdapterRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.buf)
    }
}

struct AdapterTxToken<'a> {
    tx_queue: &'a mut VecDeque<Bytes>,
    mtu: usize,
}

impl<'a> TxToken for AdapterTxToken<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        debug_assert!(len <= self.mtu);
        let mut buf = vec![0u8; len];
        let result = f(&mut buf);
        self.tx_queue.push_back(Bytes::from(buf));
        result
    }
}

/// Caller-visible `AsyncRead + AsyncWrite` wrapper. Owns the user side of
/// the two byte channels; the driver task owns the other end. Dropping
/// the stream closes the channels and the driver's `on_close` hook fires
/// from its drop guard, removing this flow from the adapter map.
struct EndpointTcpStream {
    send_tx: mpsc::Sender<Bytes>,
    recv_rx: mpsc::Receiver<Bytes>,
    recv_buf: VecDeque<u8>,
    write_eof: bool,
}

impl AsyncRead for EndpointTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.recv_buf.is_empty() {
            let n = self.recv_buf.len().min(buf.remaining());
            let (front, back) = self.recv_buf.as_slices();
            let take_front = front.len().min(n);
            buf.put_slice(&front[..take_front]);
            if take_front < n {
                buf.put_slice(&back[..n - take_front]);
            }
            self.recv_buf.drain(..n);
            return Poll::Ready(Ok(()));
        }
        match self.recv_rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Ok(())), // EOF
            Poll::Ready(Some(bytes)) => {
                self.recv_buf.extend(bytes.iter());
                // Re-poll to fill the read buffer.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl AsyncWrite for EndpointTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.write_eof {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "endpoint TCP stream write side closed",
            )));
        }
        // Phase 1 trade-off: tokio 1.52's `mpsc::Sender` doesn't expose
        // `poll_reserve` on stable, so we use `try_send` and re-poll on
        // Full. DNS / DoH traffic stays well under the channel capacity,
        // so the wake_by_ref spin path is hit rarely in practice; a
        // proper permit state machine is a Phase 2 polish if profiling
        // ever shows it on the hot path.
        match self.send_tx.try_send(Bytes::copy_from_slice(buf)) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Poll::Ready(Err(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "endpoint TCP driver gone"),
            )),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // Send is best-effort once it's in the channel; smoltcp pushes
        // data on every loop iteration. No app-visible flush primitive.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if !self.write_eof {
            self.write_eof = true;
            // Drop the sender so the driver sees None on user_send_rx and
            // calls socket.close().
            let dummy = mpsc::channel::<Bytes>(1).0;
            self.send_tx = dummy; // overwrite with a closed-on-drop placeholder
        }
        Poll::Ready(Ok(()))
    }
}
