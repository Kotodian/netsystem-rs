use std::collections::HashMap;
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant as StdInstant};
use tracing::{debug, error};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future;
use h3::client;
use hammer_adapter::{
    Network, Outbound, PlatformInterface, ProxyDatagram, ProxyPacketConn, ProxyStream, SocksAddr,
};
use hammer_core::config::{
    Hysteria2BbrProfile, Hysteria2Network, Hysteria2Obfs, Hysteria2ObfsType,
    Hysteria2OutboundOptions, OutboundKind,
};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use http::Request;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, mpsc};

pub mod bbr;
pub mod obfs;
pub mod protocol;

use bbr::{BbrProfile, CongestionControlHandle, apply_transport_config_with_handle};
use obfs::Salamander;

#[cfg(feature = "probe")]
use crate::protocol::icmp;
use crate::socket_protector::SocketProtector;

// Match sing-quic's hysteria2 client defaults
// (quic/hysteria/protocol.go DefaultStreamReceiveWindow / DefaultConnReceiveWindow):
//   stream window = 8 MiB, connection window = stream * 5 / 2.
// quinn's send_window default is also 8 MiB; bumping it slightly keeps the
// in-flight ceiling above one stream's BDP at typical BBR steady state.
const STREAM_RECEIVE_WINDOW: u32 = 8 * 1024 * 1024;
const CONNECTION_RECEIVE_WINDOW: u32 = STREAM_RECEIVE_WINDOW / 2 * 5;
const SEND_WINDOW: u64 = CONNECTION_RECEIVE_WINDOW as u64;
const HYSTERIA2_CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const HYSTERIA2_CONNECT_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const HYSTERIA2_CONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ClientOptions {
    pub server: String,
    pub server_port: u16,
    pub password: String,
    pub server_name: String,
    pub insecure: bool,
    pub udp_enabled: bool,
    pub bbr_profile: BbrProfile,
    pub disable_path_mtu_discovery: bool,
    pub initial_packet_size: u16,
    pub idle_timeout: Option<Duration>,
    pub keep_alive_period: Option<Duration>,
    pub send_bps: u64,
    pub receive_bps: u64,
    pub brutal_debug: bool,
    pub obfs: Option<Hysteria2Obfs>,
    pub platform: Option<Arc<dyn PlatformInterface>>,
}

pub struct Hysteria2Client {
    connection: quinn::Connection,
    _endpoint: quinn::Endpoint,
    _h3_sender: client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    sessions: Arc<Mutex<HashMap<u32, mpsc::Sender<ProxyDatagram>>>>,
    next_session: AtomicU32,
}

impl Hysteria2Client {
    pub async fn connect(options: ClientOptions) -> Result<Arc<Self>, HammerError> {
        connect_with_timeout(options, HYSTERIA2_CONNECT_TIMEOUT).await
    }

    pub async fn dial_tcp(
        &self,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> Result<Hysteria2Stream, HammerError> {
        let (mut send, mut recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|err| HammerError::internal(format!("open hysteria2 stream: {err}")))?;
        let request = protocol::encode_tcp_request(&destination.to_string(), initial_payload);
        send.write_all(&request)
            .await
            .map_err(|err| HammerError::internal(format!("write hysteria2 stream: {err}")))?;
        let response = protocol::read_tcp_response_header(&mut recv).await?;
        if !response.ok {
            return Err(HammerError::internal(format!(
                "remote error: {}",
                response.message
            )));
        }
        debug!("outbound connection to {destination}");
        Ok(Hysteria2Stream { send, recv })
    }

    pub async fn listen_udp(&self) -> Result<Hysteria2PacketConn, HammerError> {
        let session_id = self.next_session.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(64);
        self.sessions.lock().await.insert(session_id, tx);
        Ok(Hysteria2PacketConn {
            connection: self.connection.clone(),
            sessions: Arc::clone(&self.sessions),
            session_id,
            packet_id: AtomicU16::new(1),
            rx,
        })
    }

    pub fn close(&self, reason: &'static [u8]) {
        self.connection.close(0u32.into(), reason);
    }
}

async fn connect_with_timeout(
    options: ClientOptions,
    connect_timeout: Duration,
) -> Result<Arc<Hysteria2Client>, HammerError> {
    let server = format!("{}:{}", options.server, effective_port(options.server_port));
    debug!("hysteria2 resolving server {server}");
    let address = resolve_server(&options.server, options.server_port).await?;
    debug!("hysteria2 resolved server {server} -> {address}");
    let congestion = CongestionControlHandle::default();
    let endpoint = client_endpoint(&options, address, congestion.clone())?;
    let server_name = if options.server_name.is_empty() {
        options.server.as_str()
    } else {
        options.server_name.as_str()
    };
    let tls_mode = tls_mode(&options);
    debug!(
        "hysteria2 connecting {address} server_name={server_name} tls={tls_mode} udp={} timeout={}",
        options.udp_enabled,
        duration_label(connect_timeout)
    );
    let context = connect_context(&server, address, server_name, tls_mode, options.udp_enabled);
    let connecting = endpoint.connect(address, server_name).map_err(|err| {
        HammerError::internal(format!("start hysteria2 connect failed {context}: {err}"))
    })?;
    let connection = match tokio::time::timeout(connect_timeout, connecting).await {
        Ok(Ok(connection)) => connection,
        Ok(Err(err)) => {
            return Err(HammerError::internal(format!(
                "connect hysteria2 failed {context}: {err}"
            )));
        }
        Err(_) => {
            return Err(HammerError::internal(format!(
                "connect hysteria2 timed out after {} {context}; no QUIC handshake result",
                duration_label(connect_timeout)
            )));
        }
    };
    debug!("hysteria2 connected {address}");
    debug!("hysteria2 authenticating {context}");
    let (h3_sender, auth_response) = authenticate(&connection, &options)
        .await
        .map_err(|err| HammerError::internal(format!("hysteria2 auth failed {context}: {err}")))?;
    debug!("hysteria2 authenticated udp={}", options.udp_enabled);
    let actual_tx = actual_tx_bps(options.send_bps, auth_response.rx);
    if !auth_response.rx_auto && actual_tx > 0 {
        congestion.use_brutal(actual_tx);
    }
    let client = Arc::new(Hysteria2Client {
        connection,
        _endpoint: endpoint,
        _h3_sender: h3_sender,
        sessions: Arc::new(Mutex::new(HashMap::new())),
        next_session: AtomicU32::new(1),
    });
    if options.udp_enabled {
        spawn_datagram_loop(Arc::clone(&client));
    }
    Ok(client)
}

pub struct Hysteria2Stream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl Hysteria2Stream {
    pub async fn read_to_end(&mut self) -> Result<Vec<u8>, HammerError> {
        let mut bytes = Vec::new();
        AsyncReadExt::read_to_end(self, &mut bytes)
            .await
            .map_err(|err| HammerError::internal(format!("read hysteria2 stream: {err}")))?;
        Ok(bytes)
    }
}

impl AsyncRead for Hysteria2Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for Hysteria2Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        <quinn::SendStream as AsyncWrite>::poll_write(Pin::new(&mut self.send), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <quinn::SendStream as AsyncWrite>::poll_flush(Pin::new(&mut self.send), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <quinn::SendStream as AsyncWrite>::poll_shutdown(Pin::new(&mut self.send), cx)
    }
}

pub struct Hysteria2PacketConn {
    connection: quinn::Connection,
    sessions: Arc<Mutex<HashMap<u32, mpsc::Sender<ProxyDatagram>>>>,
    session_id: u32,
    packet_id: AtomicU16,
    rx: mpsc::Receiver<ProxyDatagram>,
}

impl Hysteria2PacketConn {
    pub async fn send_to(
        &mut self,
        destination: SocksAddr,
        payload: &[u8],
    ) -> Result<(), HammerError> {
        <Self as ProxyPacketConn>::send_to(self, destination, payload).await
    }

    pub async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        <Self as ProxyPacketConn>::recv_from(self).await
    }
}

#[async_trait]
impl ProxyPacketConn for Hysteria2PacketConn {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> Result<(), HammerError> {
        if payload.len() > protocol::MAX_UDP_SIZE {
            return Err(HammerError::internal("UDP packet too large"));
        }
        let packet_id = self.packet_id.fetch_add(1, Ordering::SeqCst);
        debug!(
            "hysteria2 UDP send session={} packet={} dest={} bytes={}",
            self.session_id,
            packet_id,
            destination,
            payload.len()
        );
        let message = protocol::UdpMessage {
            session_id: self.session_id,
            packet_id,
            fragment_id: 0,
            fragment_total: 1,
            destination: destination.to_string(),
            payload: Bytes::copy_from_slice(payload),
        };
        self.connection
            .send_datagram(message.encode())
            .map_err(|err| HammerError::internal(format!("send hysteria2 datagram: {err}")))
    }

    async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        match self.rx.recv().await {
            Some(datagram) => {
                debug!(
                    "hysteria2 UDP recv session={} addr={} bytes={}",
                    self.session_id,
                    datagram.destination,
                    datagram.payload.len()
                );
                Ok(datagram)
            }
            None => Err(HammerError::internal("hysteria2 UDP session closed")),
        }
    }
}

impl Drop for Hysteria2PacketConn {
    fn drop(&mut self) {
        let sessions = Arc::clone(&self.sessions);
        let session_id = self.session_id;
        crate::spawn::spawn(async move {
            sessions.lock().await.remove(&session_id);
        });
    }
}

pub struct Hysteria2Outbound {
    id: String,
    options: Hysteria2OutboundOptions,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    client_state: StdMutex<ClientState>,
    connect_backoff: StdMutex<ConnectBackoff>,
    client_init: Mutex<()>,
    protector: SocketProtector,
}

struct ClientState {
    epoch: u64,
    client: Option<Arc<Hysteria2Client>>,
}

#[derive(Debug, Default)]
struct ConnectBackoff {
    current_delay: Duration,
    next_allowed: Option<StdInstant>,
}

impl ConnectBackoff {
    fn remaining(&self, now: StdInstant) -> Option<Duration> {
        let next_allowed = self.next_allowed?;
        if now >= next_allowed {
            None
        } else {
            Some(next_allowed.duration_since(now))
        }
    }

    fn record_failure(&mut self, now: StdInstant) {
        self.current_delay = if self.current_delay.is_zero() {
            HYSTERIA2_CONNECT_BACKOFF_INITIAL
        } else {
            (self.current_delay * 2).min(HYSTERIA2_CONNECT_BACKOFF_MAX)
        };
        self.next_allowed = Some(now + self.current_delay);
    }

    fn reset(&mut self) {
        self.current_delay = Duration::ZERO;
        self.next_allowed = None;
    }

    #[cfg(test)]
    fn current_delay(&self) -> Duration {
        self.current_delay
    }
}

impl Hysteria2Outbound {
    pub fn new(logger: Logger, id: String, options: Hysteria2OutboundOptions) -> Self {
        Self::new_with_protector(logger, id, options, SocketProtector::default())
    }

    pub(crate) fn new_with_protector(
        _logger: Logger,
        id: String,
        options: Hysteria2OutboundOptions,
        protector: SocketProtector,
    ) -> Self {
        let networks = adapter_networks(&options.network);
        Self {
            id,
            options,
            networks,
            dependencies: Vec::new(),
            client_state: StdMutex::new(ClientState {
                epoch: 0,
                client: None,
            }),
            connect_backoff: StdMutex::new(ConnectBackoff::default()),
            client_init: Mutex::new(()),
            protector,
        }
    }

    async fn client(&self) -> Result<Arc<Hysteria2Client>, HammerError> {
        self.client_with_timeout(HYSTERIA2_CONNECT_TIMEOUT).await
    }

    async fn client_with_timeout(
        &self,
        connect_timeout: Duration,
    ) -> Result<Arc<Hysteria2Client>, HammerError> {
        if let Some(client) = self.cached_client() {
            return Ok(client);
        }
        loop {
            let _guard = self.client_init.lock().await;
            if let Some(client) = self.cached_client() {
                return Ok(client);
            }
            if let Some(remaining) = self.connect_backoff_remaining() {
                return Err(HammerError::internal(format!(
                    "hysteria2 outbound {} connect backing off for {} after recent failure",
                    self.id,
                    duration_label(remaining)
                )));
            }
            let epoch = self.client_epoch();
            let options = self.client_options()?;
            debug!("hysteria2 outbound {} initializing client", self.id);
            let client = match connect_with_timeout(options, connect_timeout).await {
                Ok(client) => client,
                Err(err) => {
                    if self.client_epoch() != epoch {
                        debug!(
                            "hysteria2 outbound {} stale connect failed after reset: {err}",
                            self.id
                        );
                        continue;
                    }
                    self.record_connect_failure();
                    error!("hysteria2 outbound {} connect failed: {err}", self.id);
                    return Err(err);
                }
            };
            let mut state = self
                .client_state
                .lock()
                .expect("Hysteria2Outbound client poisoned");
            if state.epoch != epoch {
                drop(state);
                client.close(b"network reset during connect");
                continue;
            }
            state.client = Some(Arc::clone(&client));
            drop(state);
            self.clear_connect_backoff();
            debug!("hysteria2 outbound {} client ready", self.id);
            return Ok(client);
        }
    }

    fn connect_backoff_remaining(&self) -> Option<Duration> {
        self.connect_backoff
            .lock()
            .expect("Hysteria2Outbound connect backoff poisoned")
            .remaining(StdInstant::now())
    }

    fn record_connect_failure(&self) {
        self.connect_backoff
            .lock()
            .expect("Hysteria2Outbound connect backoff poisoned")
            .record_failure(StdInstant::now());
    }

    fn clear_connect_backoff(&self) {
        self.connect_backoff
            .lock()
            .expect("Hysteria2Outbound connect backoff poisoned")
            .reset();
    }

    fn cached_client(&self) -> Option<Arc<Hysteria2Client>> {
        self.client_state
            .lock()
            .expect("Hysteria2Outbound client poisoned")
            .client
            .clone()
    }

    fn client_epoch(&self) -> u64 {
        self.client_state
            .lock()
            .expect("Hysteria2Outbound client poisoned")
            .epoch
    }

    fn client_options(&self) -> Result<ClientOptions, HammerError> {
        Ok(ClientOptions {
            server: self.options.server.clone(),
            server_port: self.options.server_port,
            password: self.options.password.clone(),
            server_name: self.options.tls.server_name.clone(),
            insecure: self.options.tls.insecure,
            udp_enabled: self.networks.contains(&Network::Udp),
            bbr_profile: runtime_bbr_profile(self.options.bbr_profile),
            disable_path_mtu_discovery: self.options.disable_path_mtu_discovery,
            initial_packet_size: self.options.initial_packet_size,
            idle_timeout: self.options.idle_timeout,
            keep_alive_period: self.options.keep_alive_period,
            send_bps: mbps_to_bps(self.options.up_mbps)?,
            receive_bps: mbps_to_bps(self.options.down_mbps)?,
            brutal_debug: self.options.brutal_debug,
            obfs: self.options.obfs.clone(),
            platform: self.protector.platform(),
        })
    }

    async fn resolve_probe_server(&self) -> Result<SocketAddr, HammerError> {
        resolve_server(&self.options.server, self.options.server_port).await
    }
}

pub(crate) fn build_outbound(
    logger: Logger,
    id: String,
    kind: &OutboundKind,
    protector: SocketProtector,
) -> Result<Arc<dyn Outbound>, HammerError> {
    match kind {
        OutboundKind::Hysteria2(options) => Ok(Arc::new(Hysteria2Outbound::new_with_protector(
            logger,
            id,
            options.clone(),
            protector,
        ))),
        _ => Err(HammerError::internal(
            "hysteria2 factory received wrong options",
        )),
    }
}

#[async_trait]
impl Outbound for Hysteria2Outbound {
    fn type_name(&self) -> &str {
        "hysteria2"
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn networks(&self) -> &[Network] {
        &self.networks
    }

    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }

    fn reset(&self) {
        let client = {
            let mut state = self
                .client_state
                .lock()
                .expect("Hysteria2Outbound client poisoned");
            state.epoch = state.epoch.wrapping_add(1);
            state.client.take()
        };
        if let Some(client) = client {
            client.close(b"network reset");
        }
        self.clear_connect_backoff();
    }

    #[cfg(feature = "probe")]
    async fn probe_latency(
        &self,
        protocol: &str,
        timeout_duration: Duration,
    ) -> Result<Duration, HammerError> {
        if protocol != "icmp" {
            return Err(HammerError::internal(format!(
                "{protocol} probe not supported by outbound: {}",
                self.id
            )));
        }

        let protector = self.protector.clone();
        let probe = async {
            let server = self.resolve_probe_server().await?;
            icmp::probe_echo(server.ip(), timeout_duration, protector).await
        };

        match tokio::time::timeout(timeout_duration, probe).await {
            Ok(result) => result,
            Err(_) => Err(HammerError::internal(format!(
                "icmp probe timed out after {}",
                duration_label(timeout_duration)
            ))),
        }
    }

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        if !self.networks.contains(&network) {
            return Err(HammerError::internal(format!(
                "{network} is not supported by outbound: {}",
                self.id
            )));
        }
        match network {
            Network::Tcp => Ok(Box::new(
                self.client()
                    .await?
                    .dial_tcp(destination, initial_payload)
                    .await?,
            )),
            Network::Udp => Err(HammerError::internal("use listen_packet for hysteria2 UDP")),
            // Unreachable in practice: `self.networks` only ever
            // contains Tcp/Udp (see `adapter_networks`), so the guard
            // above already filters Icmp out. Kept for exhaustiveness.
            Network::Icmp => Err(HammerError::internal(
                "icmp not supported by hysteria2 outbound",
            )),
        }
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        if !self.networks.contains(&Network::Udp) {
            return Err(HammerError::internal(format!(
                "udp is not supported by outbound: {}",
                self.id
            )));
        }
        Ok(Box::new(self.client().await?.listen_udp().await?))
    }
}

async fn authenticate(
    connection: &quinn::Connection,
    options: &ClientOptions,
) -> Result<
    (
        client::SendRequest<h3_quinn::OpenStreams, Bytes>,
        protocol::AuthResponse,
    ),
    HammerError,
> {
    let h3_conn = h3_quinn::Connection::new(connection.clone());
    let (mut driver, mut sender) = client::new(h3_conn)
        .await
        .map_err(|err| HammerError::internal(format!("h3 client init: {err}")))?;
    crate::spawn::spawn(async move {
        let _ = future::poll_fn(|cx| driver.poll_close(cx)).await;
    });
    let mut request = Request::post(format!(
        "https://{}{}",
        protocol::URL_HOST,
        protocol::URL_PATH
    ))
    .body(())
    .map_err(|err| HammerError::internal(format!("build auth request: {err}")))?;
    protocol::auth_request_to_headers(
        request.headers_mut(),
        &protocol::AuthRequest {
            auth: options.password.clone(),
            rx: options.receive_bps,
        },
    );
    let mut stream = sender
        .send_request(request)
        .await
        .map_err(|err| HammerError::internal(format!("send auth request: {err}")))?;
    stream
        .finish()
        .await
        .map_err(|err| HammerError::internal(format!("finish auth request: {err}")))?;
    let response = stream
        .recv_response()
        .await
        .map_err(|err| HammerError::internal(format!("recv auth response: {err}")))?;
    if response.status().as_u16() != protocol::STATUS_AUTH_OK {
        return Err(HammerError::internal(format!(
            "authentication failed, status code: {}",
            response.status()
        )));
    }
    let auth = protocol::auth_response_from_headers(response.headers());
    Ok((sender, auth))
}

fn spawn_datagram_loop(client: Arc<Hysteria2Client>) {
    crate::spawn::spawn(async move {
        loop {
            let datagram = match client.connection.read_datagram().await {
                Ok(datagram) => datagram,
                Err(err) => {
                    error!("receive datagram: {err}");
                    return;
                }
            };
            if let Err(err) = handle_datagram(&client, datagram).await {
                error!("handle datagram: {err}");
            }
        }
    });
}

async fn handle_datagram(client: &Hysteria2Client, data: Bytes) -> Result<(), HammerError> {
    let message = protocol::UdpMessage::decode(data)?;
    let destination = parse_destination(&message.destination);
    let sender = {
        client
            .sessions
            .lock()
            .await
            .get(&message.session_id)
            .cloned()
    };
    if let Some(sender) = sender
        && sender
            .try_send(ProxyDatagram {
                destination,
                payload: message.payload,
            })
            .is_err()
    {
        debug!("drop hysteria2 UDP datagram for busy session");
    }
    Ok(())
}

fn client_endpoint(
    options: &ClientOptions,
    server_addr: SocketAddr,
    congestion: CongestionControlHandle,
) -> Result<quinn::Endpoint, HammerError> {
    let bind_addr = if server_addr.is_ipv6() {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    };
    let socket = std::net::UdpSocket::bind(bind_addr)
        .map_err(|err| HammerError::internal(format!("bind QUIC UDP socket: {err}")))?;
    let local_addr = socket
        .local_addr()
        .map_err(|err| HammerError::internal(format!("read QUIC UDP socket addr: {err}")))?;
    debug!("hysteria2 UDP socket bound local={local_addr}");
    SocketProtector::from(options.platform.clone()).protect(&socket)?;
    debug!("hysteria2 UDP socket protected local={local_addr}");
    let runtime: Arc<dyn quinn::Runtime> = Arc::new(quinn::TokioRuntime);
    let socket = runtime
        .wrap_udp_socket(socket)
        .map_err(|err| HammerError::internal(format!("wrap QUIC UDP socket: {err}")))?;
    let socket = wrap_obfs_socket(socket, options.obfs.as_ref())?;
    let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        socket,
        runtime,
    )
    .map_err(|err| HammerError::internal(format!("create QUIC endpoint: {err}")))?;
    endpoint.set_default_client_config(client_config(options, congestion)?);
    Ok(endpoint)
}

fn client_config(
    options: &ClientOptions,
    congestion: CongestionControlHandle,
) -> Result<quinn::ClientConfig, HammerError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|err| HammerError::internal(format!("tls versions: {err}")))?;
    let mut crypto = if options.insecure {
        builder
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new(provider))
            .with_no_client_auth()
    } else {
        crate::tls_support::client_verifier_builder(builder, options.platform.clone())?
            .with_no_client_auth()
    };
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let mut config = quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(crypto)
            .map_err(|err| HammerError::internal(format!("quic tls config: {err}")))?,
    ));
    let mut transport = quinn::TransportConfig::default();
    // Match the sing-quic / hysteria2 official defaults — quinn's defaults are
    // far below the BDP we typically need for cross-region transfers.
    transport.stream_receive_window(quinn::VarInt::from_u32(STREAM_RECEIVE_WINDOW));
    transport.receive_window(quinn::VarInt::from_u32(CONNECTION_RECEIVE_WINDOW));
    transport.send_window(SEND_WINDOW);
    transport.keep_alive_interval(Some(
        options.keep_alive_period.unwrap_or(Duration::from_secs(10)),
    ));
    if let Some(timeout) = options.idle_timeout {
        let timeout = timeout
            .try_into()
            .map_err(|err| HammerError::internal(format!("invalid QUIC idle timeout: {err}")))?;
        transport.max_idle_timeout(Some(timeout));
    } else {
        transport.max_idle_timeout(Some(
            Duration::from_secs(30)
                .try_into()
                .expect("30s fits in QUIC idle timeout"),
        ));
    }
    apply_transport_config_with_handle(
        &mut config,
        transport,
        options.bbr_profile,
        options.initial_packet_size,
        options.disable_path_mtu_discovery,
        congestion,
        options.brutal_debug,
    );
    Ok(config)
}

fn connect_context(
    server: &str,
    address: SocketAddr,
    server_name: &str,
    tls_mode: &str,
    udp_enabled: bool,
) -> String {
    format!(
        "server={server} resolved={address} server_name={server_name} tls={tls_mode} udp={udp_enabled}"
    )
}

fn tls_mode(options: &ClientOptions) -> &'static str {
    if options.insecure {
        "insecure"
    } else if cfg!(target_vendor = "apple") {
        "platform"
    } else {
        "native-roots"
    }
}

fn duration_label(duration: Duration) -> String {
    if duration.as_secs() == 0 {
        return format!("{}ms", duration.as_millis());
    }
    if duration.subsec_millis() == 0 {
        return format!("{}s", duration.as_secs());
    }
    format!("{}.{:03}s", duration.as_secs(), duration.subsec_millis())
}

fn wrap_obfs_socket(
    socket: Arc<dyn quinn::AsyncUdpSocket>,
    obfs: Option<&Hysteria2Obfs>,
) -> Result<Arc<dyn quinn::AsyncUdpSocket>, HammerError> {
    let Some(obfs) = obfs else {
        return Ok(socket);
    };
    match obfs.type_ {
        Hysteria2ObfsType::Salamander => Ok(Arc::new(SalamanderUdpSocket {
            inner: socket,
            obfs: Salamander::new(obfs.password.as_bytes().to_vec()),
        })),
    }
}

#[derive(Debug)]
struct SalamanderUdpSocket {
    inner: Arc<dyn quinn::AsyncUdpSocket>,
    obfs: Salamander,
}

impl quinn::AsyncUdpSocket for SalamanderUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn quinn::UdpPoller>> {
        Arc::clone(&self.inner).create_io_poller()
    }

    fn try_send(&self, transmit: &quinn::udp::Transmit<'_>) -> io::Result<()> {
        let sealed = self.obfs.seal(transmit.contents);
        let transmit = quinn::udp::Transmit {
            destination: transmit.destination,
            ecn: transmit.ecn,
            contents: &sealed,
            segment_size: None,
            src_ip: transmit.src_ip,
        };
        self.inner.try_send(&transmit)
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let count = match self.inner.poll_recv(cx, bufs, meta) {
            Poll::Ready(Ok(count)) => count,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        };
        for index in 0..count {
            let len = meta[index].len;
            let plain = self
                .obfs
                .open(&bufs[index][..len])
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            bufs[index][..plain.len()].copy_from_slice(&plain);
            meta[index].len = plain.len();
            meta[index].stride = plain.len();
        }
        Poll::Ready(Ok(count))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        1
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

async fn resolve_server(server: &str, port: u16) -> Result<SocketAddr, HammerError> {
    let port = effective_port(port);
    if let Ok(ip) = server.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    tokio::net::lookup_host((server, port))
        .await
        .map_err(|err| HammerError::internal(format!("resolve hysteria2 server: {err}")))?
        .next()
        .ok_or_else(|| HammerError::internal("empty hysteria2 server resolution"))
}

fn effective_port(port: u16) -> u16 {
    if port == 0 { 443 } else { port }
}

fn mbps_to_bps(mbps: i64) -> Result<u64, HammerError> {
    if mbps < 0 {
        return Err(HammerError::internal(
            "hysteria2 bandwidth must be non-negative",
        ));
    }
    Ok((mbps as u64).saturating_mul(125_000))
}

fn actual_tx_bps(send_bps: u64, server_rx: u64) -> u64 {
    if server_rx == 0 || server_rx > send_bps {
        send_bps
    } else {
        server_rx
    }
}

fn adapter_networks(value: &[Hysteria2Network]) -> Vec<Network> {
    if value.is_empty() {
        return vec![Network::Tcp, Network::Udp];
    }
    value.iter().copied().map(adapter_network).collect()
}

fn runtime_bbr_profile(profile: Hysteria2BbrProfile) -> BbrProfile {
    match profile {
        Hysteria2BbrProfile::Standard => BbrProfile::Standard,
        Hysteria2BbrProfile::Conservative => BbrProfile::Conservative,
        Hysteria2BbrProfile::Aggressive => BbrProfile::Aggressive,
    }
}

fn adapter_network(value: Hysteria2Network) -> Network {
    match value {
        Hysteria2Network::Tcp => Network::Tcp,
        Hysteria2Network::Udp => Network::Udp,
    }
}

fn parse_destination(destination: &str) -> SocksAddr {
    if let Ok(addr) = destination.parse::<SocketAddr>() {
        return SocksAddr::ip(addr.ip(), addr.port());
    }
    if let Some((host, port)) = destination.rsplit_once(':')
        && let Ok(port) = port.parse::<u16>()
    {
        return SocksAddr::domain(
            host.trim_matches(['[', ']']),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port,
        );
    }
    SocksAddr::ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
}

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new(provider: Arc<rustls::crypto::CryptoProvider>) -> Arc<Self> {
        Arc::new(Self(provider))
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn _validate_obfs(obfs: &Option<Hysteria2Obfs>) -> Result<(), HammerError> {
    if let Some(obfs) = obfs {
        match obfs.type_ {
            Hysteria2ObfsType::Salamander => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::log::{DiscardWriter, Factory};

    fn logger(id: &str) -> Logger {
        Factory::new(std::time::Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    fn client_options_for_test(port: u16) -> ClientOptions {
        ClientOptions {
            server: "127.0.0.1".to_owned(),
            server_port: port,
            password: "secret".to_owned(),
            server_name: "localhost".to_owned(),
            insecure: true,
            udp_enabled: true,
            bbr_profile: BbrProfile::Standard,
            disable_path_mtu_discovery: false,
            initial_packet_size: 1200,
            idle_timeout: None,
            keep_alive_period: None,
            send_bps: 0,
            receive_bps: 0,
            brutal_debug: false,
            obfs: None,
            platform: None,
        }
    }

    fn outbound_options_for_test(port: u16) -> Hysteria2OutboundOptions {
        Hysteria2OutboundOptions {
            server: "127.0.0.1".to_owned(),
            server_port: port,
            password: "secret".to_owned(),
            up_mbps: 0,
            down_mbps: 0,
            network: vec![Hysteria2Network::Tcp, Hysteria2Network::Udp],
            idle_timeout: None,
            keep_alive_period: None,
            initial_packet_size: 1200,
            tls: hammer_core::config::OutboundTlsOptions {
                enabled: true,
                server_name: "localhost".to_owned(),
                insecure: true,
            },
            ..Default::default()
        }
    }

    #[test]
    fn connect_backoff_doubles_until_max_and_resets() {
        let now = std::time::Instant::now();
        let mut backoff = ConnectBackoff::default();

        assert_eq!(backoff.remaining(now), None);
        backoff.record_failure(now);
        assert_eq!(backoff.current_delay(), HYSTERIA2_CONNECT_BACKOFF_INITIAL);
        assert_eq!(
            backoff.remaining(now),
            Some(HYSTERIA2_CONNECT_BACKOFF_INITIAL)
        );

        backoff.record_failure(now + HYSTERIA2_CONNECT_BACKOFF_INITIAL);
        assert_eq!(
            backoff.current_delay(),
            HYSTERIA2_CONNECT_BACKOFF_INITIAL * 2
        );

        for _ in 0..10 {
            backoff.record_failure(now + HYSTERIA2_CONNECT_BACKOFF_MAX);
        }
        assert_eq!(backoff.current_delay(), HYSTERIA2_CONNECT_BACKOFF_MAX);

        backoff.reset();
        assert_eq!(backoff.remaining(now), None);
        assert_eq!(backoff.current_delay(), Duration::ZERO);
    }

    #[tokio::test]
    async fn outbound_connect_backoff_fails_fast_after_connect_error() {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind blackhole UDP socket");
        let port = socket.local_addr().expect("blackhole addr").port();
        let _blackhole = crate::spawn::spawn(async move {
            let mut buf = [0u8; 2048];
            while socket.recv_from(&mut buf).await.is_ok() {}
        });
        let outbound = Hysteria2Outbound::new(
            logger("hysteria2"),
            "hysteria2".to_owned(),
            outbound_options_for_test(port),
        );

        let first = match outbound
            .client_with_timeout(Duration::from_millis(50))
            .await
        {
            Ok(_) => panic!("blackhole should time out"),
            Err(err) => err,
        };
        assert!(first.to_string().contains("connect hysteria2 timed out"));

        let started = std::time::Instant::now();
        let second = match outbound
            .client_with_timeout(Duration::from_millis(50))
            .await
        {
            Ok(_) => panic!("second attempt should be blocked by backoff"),
            Err(err) => err,
        };
        assert!(
            started.elapsed() < Duration::from_millis(25),
            "backoff should fail fast instead of waiting for connect timeout"
        );
        assert!(second.to_string().contains("connect backing off"));
    }

    #[tokio::test]
    async fn outbound_reset_clears_connect_backoff() {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind blackhole UDP socket");
        let port = socket.local_addr().expect("blackhole addr").port();
        let _blackhole = crate::spawn::spawn(async move {
            let mut buf = [0u8; 2048];
            while socket.recv_from(&mut buf).await.is_ok() {}
        });
        let outbound = Hysteria2Outbound::new(
            logger("hysteria2"),
            "hysteria2".to_owned(),
            outbound_options_for_test(port),
        );

        let err = match outbound
            .client_with_timeout(Duration::from_millis(50))
            .await
        {
            Ok(_) => panic!("blackhole should time out"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("connect hysteria2 timed out"));
        assert!(outbound.connect_backoff_remaining().is_some());

        outbound.reset();

        assert_eq!(outbound.connect_backoff_remaining(), None);
    }

    #[tokio::test]
    async fn icmp_probe_resolves_configured_server_without_initializing_client() {
        let outbound = Hysteria2Outbound::new(
            logger("hysteria2"),
            "hysteria2".to_owned(),
            outbound_options_for_test(0),
        );

        let resolved = outbound
            .resolve_probe_server()
            .await
            .expect("resolve probe server");

        assert_eq!(
            resolved,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443)
        );
        assert!(
            outbound.cached_client().is_none(),
            "ICMP probe destination resolution must not initialize the QUIC client"
        );
        assert_eq!(outbound.connect_backoff_remaining(), None);
    }

    #[tokio::test]
    async fn connect_timeout_reports_hysteria2_context_before_outer_dns_timeout() {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind blackhole UDP socket");
        let port = socket.local_addr().expect("blackhole addr").port();
        let _blackhole = crate::spawn::spawn(async move {
            let mut buf = [0u8; 2048];
            while socket.recv_from(&mut buf).await.is_ok() {}
        });

        let result =
            connect_with_timeout(client_options_for_test(port), Duration::from_millis(50)).await;
        let err = match result {
            Ok(_) => panic!("blackhole should time out"),
            Err(err) => err,
        };

        let message = err.to_string();
        assert!(message.contains("connect hysteria2 timed out after 50ms"));
        assert!(message.contains("server=127.0.0.1"));
        assert!(message.contains(&format!("resolved=127.0.0.1:{port}")));
        assert!(message.contains("server_name=localhost"));
        assert!(message.contains("tls=insecure"));
        assert!(message.contains("udp=true"));
    }
}
