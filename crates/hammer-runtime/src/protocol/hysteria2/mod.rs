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
    Hysteria2Network, Hysteria2Obfs, Hysteria2ObfsType, Hysteria2OutboundOptions, OutboundKind,
    OutboundTlsOptions,
};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::log::Logger;
use hammer_core::protocol::congestion::BbrProfile;
use http::Request;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, mpsc};

use crate::tls::{
    EchRetryConfigStore, apply_ech_retry_config, ech_retry_config_store, take_ech_retry_configs,
};

mod outbound;
pub use hammer_core::protocol::hysteria2::{obfs, protocol};
#[cfg(test)]
use outbound::ConnectBackoff;
pub use outbound::Hysteria2Outbound;

use obfs::Salamander;

use crate::protocol::congestion::{CongestionControlHandle, apply_transport_config_with_handle};
#[cfg(feature = "probe")]
use crate::protocol::icmp;
use crate::socket_protector::SocketProtector;
use crate::tls::{OutboundClientTlsConfig, outbound_quic_client_config};
use crate::{ControlEventFilter, ControlEventSubscriptionHandle, ControlThreadHandle};

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

#[derive(Debug, Clone)]
pub struct Hysteria2AuthSuccessArgs {
    pub outbound_id: String,
    pub server: String,
    pub udp_enabled: bool,
    pub rx_auto: bool,
    pub server_rx_bps: u64,
    pub send_bps: u64,
    pub congestion: String,
}

#[derive(Debug, Clone)]
pub struct Hysteria2AuthFailureArgs {
    pub outbound_id: String,
    pub server: String,
    pub error: String,
}

#[derive(Clone)]
pub(super) struct Hysteria2AuthEventContext {
    outbound_id: String,
    control_handle: Arc<ControlThreadHandle>,
}

#[hammer_component_macros::hammer_component(
    event,
    name = "hysteria2-auth-log",
    builder = build_hysteria2_auth_log_subscriber
)]
pub struct Hysteria2AuthLogSubscriber;

pub(crate) fn build_hysteria2_auth_log_subscriber(
    logger: Logger,
    control_handle: Arc<ControlThreadHandle>,
) -> HammerResult<Vec<ControlEventSubscriptionHandle>> {
    let success_logger = logger.clone();
    let success = control_handle.subscribe_event(
        ControlEventFilter::event::<Hysteria2AuthSuccessArgs>(),
        move |event| {
            let logger = success_logger.clone();
            async move {
                if let Some(args) = event.args::<Hysteria2AuthSuccessArgs>() {
                    logger.info(format!(
                        "hysteria2 outbound {} auth success server={} udp={} rx_auto={} server_rx_bps={} send_bps={} congestion={}",
                        args.outbound_id,
                        args.server,
                        args.udp_enabled,
                        args.rx_auto,
                        args.server_rx_bps,
                        args.send_bps,
                        args.congestion
                    ));
                }
            }
        },
    )?;

    let failure_logger = logger;
    let failure = control_handle.subscribe_event(
        ControlEventFilter::event::<Hysteria2AuthFailureArgs>(),
        move |event| {
            let logger = failure_logger.clone();
            async move {
                if let Some(args) = event.args::<Hysteria2AuthFailureArgs>() {
                    logger.warn(format!(
                        "hysteria2 outbound {} auth failed server={}: {}",
                        args.outbound_id, args.server, args.error
                    ));
                }
            }
        },
    )?;

    Ok(vec![success, failure])
}

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
    pub tls: OutboundTlsOptions,
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
    pub async fn connect(options: ClientOptions) -> HammerResult<Arc<Self>> {
        connect_with_timeout(options, HYSTERIA2_CONNECT_TIMEOUT).await
    }

    pub async fn dial_tcp(
        &self,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> HammerResult<Hysteria2Stream> {
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

    pub async fn listen_udp(&self) -> HammerResult<Hysteria2PacketConn> {
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

    fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }
}

async fn connect_with_timeout(
    options: ClientOptions,
    connect_timeout: Duration,
) -> HammerResult<Arc<Hysteria2Client>> {
    connect_with_timeout_and_events(options, connect_timeout, None).await
}

async fn connect_with_timeout_and_events(
    mut options: ClientOptions,
    connect_timeout: Duration,
    auth_event_context: Option<Hysteria2AuthEventContext>,
) -> HammerResult<Arc<Hysteria2Client>> {
    resolve_hysteria2_ech_config(&mut options).await?;
    let server = format!("{}:{}", options.server, effective_port(options.server_port));
    debug!("hysteria2 resolving server {server}");
    let address = resolve_server(&options.server, options.server_port).await?;
    debug!("hysteria2 resolved server {server} -> {address}");
    let congestion = CongestionControlHandle::default();
    let ech_retry_configs = ech_retry_config_store(&options.tls.ech);
    let mut endpoint = client_endpoint(
        &options,
        address,
        congestion.clone(),
        ech_retry_configs.clone(),
    )?;
    let server_name = effective_server_name(&options).to_owned();
    let tls_mode = tls_mode(&options);
    debug!(
        "hysteria2 connecting {address} server_name={server_name} tls={tls_mode} udp={} timeout={}",
        options.udp_enabled,
        duration_label(connect_timeout)
    );
    let context = connect_context(
        &server,
        address,
        &server_name,
        tls_mode,
        options.udp_enabled,
    );
    let connection = match connect_quic_with_timeout(
        &endpoint,
        address,
        &server_name,
        connect_timeout,
        &context,
    )
    .await
    {
        Ok(connection) => connection,
        Err(err) => {
            let Some(retry_configs) = take_ech_retry_configs(&ech_retry_configs) else {
                return Err(err);
            };
            debug!("hysteria2 retrying with ECH retry config server_name={server_name}");
            apply_ech_retry_config(&mut options.tls.ech, retry_configs);
            endpoint = client_endpoint(&options, address, congestion.clone(), None)?;
            connect_quic_with_timeout(&endpoint, address, &server_name, connect_timeout, &context)
                .await?
        }
    };
    debug!("hysteria2 connected {address}");
    debug!("hysteria2 authenticating {context}");
    let (h3_sender, auth_response) = match authenticate(&connection, &options).await {
        Ok(auth) => auth,
        Err(err) => {
            publish_hysteria2_auth_failure(&auth_event_context, &server, err.to_string());
            return Err(HammerError::internal(format!(
                "hysteria2 auth failed {context}: {err}"
            )));
        }
    };
    let actual_tx = actual_tx_bps(options.send_bps, auth_response.rx);
    let congestion_mode = if !auth_response.rx_auto && actual_tx > 0 {
        congestion.use_brutal(actual_tx);
        "brutal"
    } else {
        "bbr"
    };
    debug!(
        "hysteria2 authenticated udp={} rx_auto={} server_rx={} send_bps={} congestion={}",
        options.udp_enabled,
        auth_response.rx_auto,
        auth_response.rx,
        options.send_bps,
        congestion_mode
    );
    if let Some(context) = &auth_event_context {
        publish_hysteria2_auth_success(
            context,
            Hysteria2AuthSuccessArgs {
                outbound_id: context.outbound_id.clone(),
                server: server.clone(),
                udp_enabled: options.udp_enabled,
                rx_auto: auth_response.rx_auto,
                server_rx_bps: auth_response.rx,
                send_bps: options.send_bps,
                congestion: congestion_mode.to_owned(),
            },
        );
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

fn publish_hysteria2_auth_success(
    context: &Hysteria2AuthEventContext,
    args: Hysteria2AuthSuccessArgs,
) {
    let _ = context.control_handle.publish_event(args);
}

fn publish_hysteria2_auth_failure(
    context: &Option<Hysteria2AuthEventContext>,
    server: &str,
    error: String,
) {
    let Some(context) = context else {
        return;
    };
    let _ = context
        .control_handle
        .publish_event(Hysteria2AuthFailureArgs {
            outbound_id: context.outbound_id.clone(),
            server: server.to_owned(),
            error,
        });
}

async fn connect_quic_with_timeout(
    endpoint: &quinn::Endpoint,
    address: SocketAddr,
    server_name: &str,
    connect_timeout: Duration,
    context: &str,
) -> HammerResult<quinn::Connection> {
    let connecting = endpoint.connect(address, server_name).map_err(|err| {
        HammerError::internal(format!("start hysteria2 connect failed {context}: {err}"))
    })?;
    match tokio::time::timeout(connect_timeout, connecting).await {
        Ok(Ok(connection)) => Ok(connection),
        Ok(Err(err)) => Err(HammerError::internal(format!(
            "connect hysteria2 failed {context}: {err}"
        ))),
        Err(_) => Err(HammerError::internal(format!(
            "connect hysteria2 timed out after {} {context}; no QUIC handshake result",
            duration_label(connect_timeout)
        ))),
    }
}

pub struct Hysteria2Stream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl Hysteria2Stream {
    pub async fn read_to_end(&mut self) -> HammerResult<Vec<u8>> {
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
    pub async fn send_to(&mut self, destination: SocksAddr, payload: Bytes) -> HammerResult<()> {
        <Self as ProxyPacketConn>::send_to(self, destination, payload).await
    }

    pub async fn recv_from(&mut self) -> HammerResult<ProxyDatagram> {
        <Self as ProxyPacketConn>::recv_from(self).await
    }
}

#[async_trait]
impl ProxyPacketConn for Hysteria2PacketConn {
    async fn send_to(&mut self, destination: SocksAddr, payload: Bytes) -> HammerResult<()> {
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
            payload,
        };
        self.connection
            .send_datagram(message.encode())
            .map_err(|err| HammerError::internal(format!("send hysteria2 datagram: {err}")))
    }

    async fn recv_from(&mut self) -> HammerResult<ProxyDatagram> {
        let connection = self.connection.clone();
        tokio::select! {
            datagram = self.rx.recv() => {
                match datagram {
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
            reason = connection.closed() => {
                self.sessions.lock().await.remove(&self.session_id);
                Err(HammerError::internal(format!(
                    "hysteria2 UDP connection closed: {reason}"
                )))
            }
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
                    client.sessions.lock().await.clear();
                    return;
                }
            };
            if let Err(err) = handle_datagram(&client, datagram).await {
                error!("handle datagram: {err}");
            }
        }
    });
}

async fn handle_datagram(client: &Hysteria2Client, data: Bytes) -> HammerResult<()> {
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
    ech_retry_configs: Option<EchRetryConfigStore>,
) -> HammerResult<quinn::Endpoint> {
    validate_hysteria2_tls_options(&options.tls)?;
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
    endpoint.set_default_client_config(client_config(options, congestion, ech_retry_configs)?);
    Ok(endpoint)
}

fn client_config(
    options: &ClientOptions,
    congestion: CongestionControlHandle,
    ech_retry_configs: Option<EchRetryConfigStore>,
) -> HammerResult<quinn::ClientConfig> {
    #[cfg(not(feature = "tls-utls-stream"))]
    let _ = &ech_retry_configs;
    let alpn_protocols = if options.tls.alpn.is_empty() {
        vec![b"h3".to_vec()]
    } else {
        options
            .tls
            .alpn
            .iter()
            .map(|alpn| alpn.as_bytes().to_vec())
            .collect()
    };
    let mut config = outbound_quic_client_config(OutboundClientTlsConfig {
        platform: options.platform.clone(),
        insecure: options.tls.insecure,
        alpn_protocols,
        server_fingerprints: options.tls.server_fingerprints.clone(),
        client_auth: options.tls.client_auth.clone(),
        ech: options.tls.ech.clone(),
        max_fragment_size: None,
        #[cfg(feature = "tls-outbound-stream")]
        fragment: None,
        #[cfg(feature = "tls-utls-stream")]
        ech_retry_configs,
        #[cfg(feature = "tls-utls-stream")]
        reality: None,
        utls: options.tls.utls.clone(),
    })?;
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

fn validate_hysteria2_tls_options(tls: &OutboundTlsOptions) -> HammerResult<()> {
    if let Some(ech) = &tls.ech {
        if ech.config_source.is_none() {
            return Err(HammerError::config_validation(
                "hysteria2.tls.ech requires inline config, config_path, or DNS HTTPS lookup",
            ));
        }
        if ech.pq_signature_schemes_enabled {
            return Err(HammerError::config_validation(
                "hysteria2.tls.ech.pq_signature_schemes_enabled is parsed but not supported by the current TLS backend",
            ));
        }
        if ech.dynamic_record_sizing_disabled {
            return Err(HammerError::config_validation(
                "hysteria2.tls.ech.dynamic_record_sizing_disabled is only valid for TCP TLS streams",
            ));
        }
    }
    if tls.reality.is_some() {
        return Err(HammerError::config_validation(
            "hysteria2.tls.reality requires a Reality-capable outbound such as VLESS",
        ));
    }
    if tls.fragment.is_some() || tls.record_fragment {
        return Err(HammerError::config_validation(
            "hysteria2.tls fragmentation is only valid for TCP TLS streams",
        ));
    }
    Ok(())
}

async fn resolve_hysteria2_ech_config(options: &mut ClientOptions) -> HammerResult<()> {
    let server_name = effective_server_name(options).to_owned();
    if crate::tls::resolve_dns_https_ech_config(&mut options.tls.ech, &server_name).await? {
        debug!("hysteria2 resolved ECH config from HTTPS record server_name={server_name}");
    }
    Ok(())
}

fn effective_server_name(options: &ClientOptions) -> &str {
    if options.server_name.is_empty() {
        options.server.as_str()
    } else {
        options.server_name.as_str()
    }
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
) -> HammerResult<Arc<dyn quinn::AsyncUdpSocket>> {
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

async fn resolve_server(server: &str, port: u16) -> HammerResult<SocketAddr> {
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

fn mbps_to_bps(mbps: i64) -> HammerResult<u64> {
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

fn _validate_obfs(obfs: &Option<Hysteria2Obfs>) -> HammerResult<()> {
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
    use h3::server;
    use hammer_core::log::{DiscardWriter, Factory, Level};
    use hammer_core::metrics::MetricsRegistry;
    use http::Response;
    use quinn::crypto::rustls::QuicServerConfig;
    use rustls::pki_types::PrivateKeyDer;
    use std::sync::atomic::AtomicUsize;

    fn logger(id: &str) -> Logger {
        Factory::new(std::time::Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    fn run_control_thread(thread: crate::ControlThread) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("control test runtime");
            runtime.block_on(thread.run());
        })
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
            tls: hammer_core::config::OutboundTlsOptions {
                enabled: true,
                server_name: "localhost".to_owned(),
                insecure: true,
                ..Default::default()
            },
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
                ..Default::default()
            },
            ..Default::default()
        }
    }

    struct AuthServer {
        _endpoint: quinn::Endpoint,
        port: u16,
        auth_count: Arc<AtomicUsize>,
    }

    impl AuthServer {
        async fn start(password: &str) -> HammerResult<Self> {
            let endpoint = quinn::Endpoint::server(
                auth_server_config()?,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            )
            .map_err(|err| HammerError::internal(format!("start auth server: {err}")))?;
            let port = endpoint
                .local_addr()
                .map_err(|err| HammerError::internal(format!("auth server addr: {err}")))?
                .port();
            let auth_count = Arc::new(AtomicUsize::new(0));
            spawn_auth_accept(
                endpoint.clone(),
                password.to_owned(),
                Arc::clone(&auth_count),
            );
            Ok(Self {
                _endpoint: endpoint,
                port,
                auth_count,
            })
        }

        fn port(&self) -> u16 {
            self.port
        }

        fn auth_count(&self) -> usize {
            self.auth_count.load(Ordering::SeqCst)
        }
    }

    fn spawn_auth_accept(
        endpoint: quinn::Endpoint,
        password: String,
        auth_count: Arc<AtomicUsize>,
    ) {
        crate::spawn::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let password = password.clone();
                let auth_count = Arc::clone(&auth_count);
                crate::spawn::spawn(async move {
                    if let Ok(connection) = incoming.await {
                        let _ = handle_auth_connection(connection, password, auth_count).await;
                    }
                });
            }
        });
    }

    async fn handle_auth_connection(
        connection: quinn::Connection,
        password: String,
        auth_count: Arc<AtomicUsize>,
    ) -> HammerResult<()> {
        let h3_conn = h3_quinn::Connection::new(connection);
        let mut incoming: server::Connection<_, Bytes> = server::Connection::new(h3_conn)
            .await
            .map_err(|err| HammerError::internal(format!("h3 auth server init: {err}")))?;
        let resolver = incoming
            .accept()
            .await
            .map_err(|err| HammerError::internal(format!("accept auth request: {err}")))?
            .ok_or_else(|| HammerError::internal("missing auth request"))?;
        let (request, mut stream) = resolver
            .resolve_request()
            .await
            .map_err(|err| HammerError::internal(format!("resolve auth request: {err}")))?;
        let auth = protocol::auth_request_from_headers(request.headers());
        auth_count.fetch_add(1, Ordering::SeqCst);
        let status = if auth.auth == password {
            protocol::STATUS_AUTH_OK
        } else {
            401
        };
        let mut response = Response::builder()
            .status(status)
            .body(())
            .map_err(|err| HammerError::internal(format!("build auth response: {err}")))?;
        protocol::auth_response_to_headers(
            response.headers_mut(),
            &protocol::AuthResponse {
                udp_enabled: true,
                rx: 0,
                rx_auto: false,
            },
        );
        stream
            .send_response(response)
            .await
            .map_err(|err| HammerError::internal(format!("send auth response: {err}")))?;
        stream
            .finish()
            .await
            .map_err(|err| HammerError::internal(format!("finish auth response: {err}")))?;
        crate::spawn::spawn(async move {
            let _incoming = incoming;
            std::future::pending::<()>().await;
        });
        Ok(())
    }

    fn auth_server_config() -> HammerResult<quinn::ServerConfig> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .map_err(|err| HammerError::internal(format!("generate certificate: {err}")))?;
        let mut crypto = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|err| HammerError::internal(format!("server tls versions: {err}")))?
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.into()],
            PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into()),
        )
        .map_err(|err| HammerError::internal(format!("server certificate: {err}")))?;
        crypto.alpn_protocols = vec![b"h3".to_vec()];
        Ok(quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(crypto)
                .map_err(|err| HammerError::internal(format!("server quic tls: {err}")))?,
        )))
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
    async fn outbound_reconnects_when_cached_client_is_closed() {
        let server = AuthServer::start("secret")
            .await
            .expect("start echo server");
        let outbound = Hysteria2Outbound::new(
            logger("hysteria2"),
            "hysteria2".to_owned(),
            outbound_options_for_test(server.port()),
        );

        let first = outbound
            .client_with_timeout(Duration::from_secs(2))
            .await
            .expect("connect first client");
        assert_eq!(server.auth_count(), 1);

        first.close(b"simulate stale cached connection");
        let _ = first.connection.closed().await;
        assert!(first.is_closed());

        let second = outbound
            .client_with_timeout(Duration::from_secs(2))
            .await
            .expect("reconnect client");

        assert!(
            !Arc::ptr_eq(&first, &second),
            "closed cached Hysteria2 client must not be reused"
        );
        assert_eq!(server.auth_count(), 2);
    }

    #[tokio::test]
    async fn outbound_ensure_connected_keeps_existing_client() {
        let server = AuthServer::start("secret")
            .await
            .expect("start echo server");
        let outbound = Hysteria2Outbound::new(
            logger("hysteria2"),
            "hysteria2".to_owned(),
            outbound_options_for_test(server.port()),
        );

        let first = outbound
            .client_with_timeout(Duration::from_secs(2))
            .await
            .expect("connect first client");
        assert_eq!(server.auth_count(), 1);

        outbound
            .ensure_connected()
            .await
            .expect("ensure outbound connected");

        let current = outbound
            .cached_client()
            .expect("ensure_connected should keep cached client");
        assert!(
            Arc::ptr_eq(&first, &current),
            "ensure_connected must not reconnect an already connected Hysteria2 client"
        );
        assert_eq!(server.auth_count(), 1);
    }

    #[tokio::test]
    async fn outbound_ensure_connected_warms_new_client_after_reset() {
        let server = AuthServer::start("secret")
            .await
            .expect("start echo server");
        let outbound = Hysteria2Outbound::new(
            logger("hysteria2"),
            "hysteria2".to_owned(),
            outbound_options_for_test(server.port()),
        );

        let first = outbound
            .client_with_timeout(Duration::from_secs(2))
            .await
            .expect("connect first client");
        assert_eq!(server.auth_count(), 1);

        outbound.reset();
        outbound
            .ensure_connected()
            .await
            .expect("ensure outbound connected");

        let second = outbound
            .cached_client()
            .expect("ensure_connected should cache the new client");
        assert!(
            !Arc::ptr_eq(&first, &second),
            "ensure_connected after reset must replace the old Hysteria2 client"
        );
        assert_eq!(server.auth_count(), 2);
    }

    #[tokio::test]
    async fn udp_recv_fails_fast_when_connection_closes() {
        let server = AuthServer::start("secret")
            .await
            .expect("start auth server");
        let client = connect_with_timeout(
            client_options_for_test(server.port()),
            Duration::from_secs(2),
        )
        .await
        .expect("connect client");
        let mut packet = client.listen_udp().await.expect("open UDP session");

        client.close(b"simulate suspended connection loss");
        let err = tokio::time::timeout(Duration::from_millis(200), packet.recv_from())
            .await
            .expect("closed QUIC connection should wake UDP recv")
            .expect_err("UDP recv should report connection closure");

        assert!(err.to_string().contains("hysteria2 UDP connection closed"));
    }

    #[tokio::test]
    async fn datagram_loop_exit_clears_udp_sessions() {
        let server = AuthServer::start("secret")
            .await
            .expect("start auth server");
        let client = connect_with_timeout(
            client_options_for_test(server.port()),
            Duration::from_secs(2),
        )
        .await
        .expect("connect client");
        let _packet = client.listen_udp().await.expect("open UDP session");
        assert_eq!(client.sessions.lock().await.len(), 1);

        client.close(b"simulate datagram loop exit");
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                if client.sessions.lock().await.is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("datagram loop exit should drop all UDP session senders");
    }

    #[tokio::test]
    async fn connect_publishes_auth_success_event() {
        let server = AuthServer::start("secret")
            .await
            .expect("start auth server");
        let metrics = MetricsRegistry::new();
        let (control_handle, control_thread) = crate::ControlThread::new(
            std::time::Instant::now(),
            Arc::new(DiscardWriter),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let control_join = run_control_thread(control_thread);
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let _subscription = control_handle
            .subscribe_event(
                ControlEventFilter::event::<Hysteria2AuthSuccessArgs>(),
                move |event| {
                    let seen_tx = seen_tx.clone();
                    async move {
                        if let Some(args) = event.args::<Hysteria2AuthSuccessArgs>() {
                            let _ = seen_tx.send(args.clone());
                        }
                    }
                },
            )
            .expect("subscribe auth success");

        let client = connect_with_timeout_and_events(
            client_options_for_test(server.port()),
            Duration::from_secs(2),
            Some(Hysteria2AuthEventContext {
                outbound_id: "hysteria2".to_owned(),
                control_handle: Arc::clone(&control_handle),
            }),
        )
        .await
        .expect("connect client");
        let args = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("auth success event");
        assert_eq!(args.outbound_id, "hysteria2");
        assert_eq!(args.server, format!("127.0.0.1:{}", server.port()));
        assert!(args.udp_enabled);
        assert_eq!(args.congestion, "bbr");

        client.close(b"test done");
        assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
        control_join.join().expect("control thread join");
    }

    #[tokio::test]
    async fn connect_publishes_auth_failure_event() {
        let server = AuthServer::start("secret")
            .await
            .expect("start auth server");
        let metrics = MetricsRegistry::new();
        let (control_handle, control_thread) = crate::ControlThread::new(
            std::time::Instant::now(),
            Arc::new(DiscardWriter),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let control_join = run_control_thread(control_thread);
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let _subscription = control_handle
            .subscribe_event(
                ControlEventFilter::event::<Hysteria2AuthFailureArgs>(),
                move |event| {
                    let seen_tx = seen_tx.clone();
                    async move {
                        if let Some(args) = event.args::<Hysteria2AuthFailureArgs>() {
                            let _ = seen_tx.send(args.clone());
                        }
                    }
                },
            )
            .expect("subscribe auth failure");

        let mut options = client_options_for_test(server.port());
        options.password = "wrong".to_owned();
        let err = match connect_with_timeout_and_events(
            options,
            Duration::from_secs(2),
            Some(Hysteria2AuthEventContext {
                outbound_id: "hysteria2".to_owned(),
                control_handle: Arc::clone(&control_handle),
            }),
        )
        .await
        {
            Ok(_) => panic!("auth should fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("hysteria2 auth failed"));
        let args = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("auth failure event");
        assert_eq!(args.outbound_id, "hysteria2");
        assert_eq!(args.server, format!("127.0.0.1:{}", server.port()));
        assert!(args.error.contains("authentication failed"));

        assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
        control_join.join().expect("control thread join");
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

    #[cfg(not(feature = "tls-utls"))]
    #[tokio::test]
    async fn unsupported_hysteria2_tls_options_fail_before_connecting() {
        let mut options = outbound_options_for_test(443);
        options.tls.utls = Some(hammer_core::config::UtlsOptions {
            fingerprint: hammer_core::config::UtlsFingerprint::Chrome,
        });
        let outbound = Hysteria2Outbound::new(logger("hysteria2"), "hysteria2".to_owned(), options);

        let err = outbound
            .ensure_connected()
            .await
            .expect_err("uTLS must not be silently ignored");
        let message = err.to_string();
        assert!(
            message.contains("tls.utls fingerprint chrome requires"),
            "error = {message:?}"
        );
        assert!(
            outbound.cached_client().is_none(),
            "unsupported TLS options must fail before a QUIC client is initialized"
        );
    }

    #[cfg(feature = "tls-utls")]
    #[test]
    fn hysteria2_utls_options_build_quic_config() {
        let mut options = client_options_for_test(443);
        options.tls.utls = Some(hammer_core::config::UtlsOptions {
            fingerprint: hammer_core::config::UtlsFingerprint::Chrome,
        });

        client_config(&options, CongestionControlHandle::default(), None)
            .expect("uTLS should build a QUIC client config when tls-utls is enabled");
    }

    #[test]
    fn hysteria2_tls_validation_allows_backend_supported_options() {
        let tls = hammer_core::config::OutboundTlsOptions {
            enabled: true,
            server_name: "localhost".to_owned(),
            insecure: true,
            server_fingerprints: vec![hammer_core::config::CertificateFingerprint {
                algorithm: hammer_core::config::CertificateFingerprintAlgorithm::Sha256,
                digest: vec![0x11; 32],
            }],
            client_auth: Some(hammer_core::config::ClientTlsAuth {
                certificates: vec![hammer_core::config::CertificateSource::Inline(
                    hammer_core::config::CertificateDerBytes(vec![1, 2, 3]),
                )],
                key: hammer_core::config::PrivateKeySource::Inline(
                    hammer_core::config::PrivateKeyDerBytes(vec![4, 5, 6]),
                ),
            }),
            ech: Some(hammer_core::config::EchOptions {
                config_source: Some(hammer_core::config::EchConfigSource::Inline(
                    hammer_core::config::EchConfigList(vec![0, 1, 2]),
                )),
                pq_signature_schemes_enabled: false,
                dynamic_record_sizing_disabled: false,
            }),
            ..Default::default()
        };

        validate_hysteria2_tls_options(&tls).expect("supported TLS options should validate");
    }

    #[test]
    fn hysteria2_tls_validation_allows_ech_dns_lookup() {
        let tls = hammer_core::config::OutboundTlsOptions {
            enabled: true,
            server_name: "localhost".to_owned(),
            insecure: true,
            ech: Some(hammer_core::config::EchOptions {
                config_source: Some(hammer_core::config::EchConfigSource::DnsHttpsRecord),
                pq_signature_schemes_enabled: false,
                dynamic_record_sizing_disabled: false,
            }),
            ..Default::default()
        };

        validate_hysteria2_tls_options(&tls).expect("DNS HTTPS ECH should validate");
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
