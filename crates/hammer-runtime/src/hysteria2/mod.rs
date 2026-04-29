use std::collections::HashMap;
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future;
use h3::client;
use hammer_adapter::{
    Network, Outbound, PlatformInterface, ProxyDatagram, ProxyPacketConn, ProxyStream, SocksAddr,
};
use hammer_core::config::{Hysteria2Obfs, Hysteria2OutboundOptions};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use http::Request;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, mpsc};

pub mod bbr;
pub mod obfs;
pub mod protocol;
pub mod testing;

use bbr::{BbrProfile, CongestionControlHandle, apply_transport_config_with_handle};
use obfs::Salamander;

use crate::socket_protector::SocketProtector;

// Match sing-quic's hysteria2 client defaults
// (quic/hysteria/protocol.go DefaultStreamReceiveWindow / DefaultConnReceiveWindow):
//   stream window = 8 MiB, connection window = stream * 5 / 2.
// quinn's send_window default is also 8 MiB; bumping it slightly keeps the
// in-flight ceiling above one stream's BDP at typical BBR steady state.
const STREAM_RECEIVE_WINDOW: u32 = 8 * 1024 * 1024;
const CONNECTION_RECEIVE_WINDOW: u32 = STREAM_RECEIVE_WINDOW / 2 * 5;
const SEND_WINDOW: u64 = CONNECTION_RECEIVE_WINDOW as u64;

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
    logger: Logger,
}

impl Hysteria2Client {
    pub async fn connect(options: ClientOptions, logger: Logger) -> Result<Arc<Self>, HammerError> {
        let address = resolve_server(&options.server, options.server_port)?;
        let congestion = CongestionControlHandle::default();
        let endpoint = client_endpoint(&options, address, congestion.clone())?;
        let server_name = if options.server_name.is_empty() {
            options.server.as_str()
        } else {
            options.server_name.as_str()
        };
        let connection = endpoint
            .connect(address, server_name)
            .map_err(|err| HammerError::internal(format!("connect hysteria2: {err}")))?
            .await
            .map_err(|err| HammerError::internal(format!("connect hysteria2: {err}")))?;
        let (h3_sender, auth_response) = authenticate(&connection, &options).await?;
        let actual_tx = actual_tx_bps(options.send_bps, auth_response.rx);
        if !auth_response.rx_auto && actual_tx > 0 {
            congestion.use_brutal(actual_tx);
        }
        let client = Arc::new(Self {
            connection,
            _endpoint: endpoint,
            _h3_sender: h3_sender,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session: AtomicU32::new(1),
            logger,
        });
        if options.udp_enabled {
            spawn_datagram_loop(Arc::clone(&client));
        }
        Ok(client)
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
        self.logger
            .debug(format!("outbound connection to {destination}"));
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
        let message = protocol::UdpMessage {
            session_id: self.session_id,
            packet_id,
            fragment_id: 0,
            fragment_total: 1,
            destination: destination.to_string(),
            payload: payload.to_vec(),
        };
        self.connection
            .send_datagram(Bytes::from(message.encode()))
            .map_err(|err| HammerError::internal(format!("send hysteria2 datagram: {err}")))
    }

    async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| HammerError::internal("hysteria2 UDP session closed"))
    }
}

impl Drop for Hysteria2PacketConn {
    fn drop(&mut self) {
        let sessions = Arc::clone(&self.sessions);
        let session_id = self.session_id;
        tokio::spawn(async move {
            sessions.lock().await.remove(&session_id);
        });
    }
}

pub struct Hysteria2Outbound {
    logger: Logger,
    tag: String,
    options: Hysteria2OutboundOptions,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    client: Mutex<Option<Arc<Hysteria2Client>>>,
    protector: SocketProtector,
}

impl Hysteria2Outbound {
    pub fn new(logger: Logger, tag: String, options: Hysteria2OutboundOptions) -> Self {
        Self::new_with_protector(logger, tag, options, SocketProtector::default())
    }

    pub(crate) fn new_with_protector(
        logger: Logger,
        tag: String,
        options: Hysteria2OutboundOptions,
        protector: SocketProtector,
    ) -> Self {
        let networks = parse_networks(&options.network);
        Self {
            logger,
            tag,
            options,
            networks,
            dependencies: Vec::new(),
            client: Mutex::new(None),
            protector,
        }
    }

    async fn client(&self) -> Result<Arc<Hysteria2Client>, HammerError> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(Arc::clone(client));
        }
        let options = ClientOptions {
            server: self.options.server.clone(),
            server_port: self.options.server_port,
            password: self.options.password.clone(),
            server_name: self.options.tls.server_name.clone(),
            insecure: self.options.tls.insecure,
            udp_enabled: self.networks.contains(&Network::Udp),
            bbr_profile: BbrProfile::parse(&self.options.bbr_profile)
                .map_err(HammerError::internal)?,
            disable_path_mtu_discovery: self.options.disable_path_mtu_discovery,
            initial_packet_size: self.options.initial_packet_size,
            idle_timeout: self.options.idle_timeout,
            keep_alive_period: self.options.keep_alive_period,
            send_bps: mbps_to_bps(self.options.up_mbps)?,
            receive_bps: mbps_to_bps(self.options.down_mbps)?,
            brutal_debug: self.options.brutal_debug,
            obfs: self.options.obfs.clone(),
            platform: self.protector.platform(),
        };
        let client = Hysteria2Client::connect(options, self.logger.clone()).await?;
        *guard = Some(Arc::clone(&client));
        Ok(client)
    }
}

#[async_trait]
impl Outbound for Hysteria2Outbound {
    fn type_name(&self) -> &str {
        "hysteria2"
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn networks(&self) -> &[Network] {
        &self.networks
    }

    fn dependencies(&self) -> &[String] {
        &self.dependencies
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
                self.tag
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
        }
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        if !self.networks.contains(&Network::Udp) {
            return Err(HammerError::internal(format!(
                "udp is not supported by outbound: {}",
                self.tag
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
    tokio::spawn(async move {
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
    tokio::spawn(async move {
        loop {
            let datagram = match client.connection.read_datagram().await {
                Ok(datagram) => datagram,
                Err(err) => {
                    client.logger.error(format!("receive datagram: {err}"));
                    return;
                }
            };
            if let Err(err) = handle_datagram(&client, &datagram).await {
                client.logger.error(format!("handle datagram: {err}"));
            }
        }
    });
}

async fn handle_datagram(client: &Hysteria2Client, data: &[u8]) -> Result<(), HammerError> {
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
    if let Some(sender) = sender {
        let _ = sender
            .send(ProxyDatagram {
                destination,
                payload: message.payload,
            })
            .await;
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
    SocketProtector::from(options.platform.clone()).protect(&socket)?;
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
        let roots = crate::tls_support::root_cert_store(options.platform.clone());
        builder.with_root_certificates(roots).with_no_client_auth()
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

fn wrap_obfs_socket(
    socket: Arc<dyn quinn::AsyncUdpSocket>,
    obfs: Option<&Hysteria2Obfs>,
) -> Result<Arc<dyn quinn::AsyncUdpSocket>, HammerError> {
    let Some(obfs) = obfs else {
        return Ok(socket);
    };
    if obfs.type_ != "salamander" {
        return Err(HammerError::internal(format!(
            "unknown obfs type: {}",
            obfs.type_
        )));
    }
    Ok(Arc::new(SalamanderUdpSocket {
        inner: socket,
        obfs: Salamander::new(obfs.password.as_bytes().to_vec()),
    }))
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

fn resolve_server(server: &str, port: u16) -> Result<SocketAddr, HammerError> {
    let port = if port == 0 { 443 } else { port };
    (server, port)
        .to_socket_addrs()
        .map_err(|err| HammerError::internal(format!("resolve hysteria2 server: {err}")))?
        .next()
        .ok_or_else(|| HammerError::internal("empty hysteria2 server resolution"))
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

fn parse_networks(value: &str) -> Vec<Network> {
    let mut networks = Vec::new();
    for item in value.lines() {
        match item {
            "tcp" => networks.push(Network::Tcp),
            "udp" => networks.push(Network::Udp),
            _ => {}
        }
    }
    if networks.is_empty() {
        vec![Network::Tcp, Network::Udp]
    } else {
        networks
    }
}

fn parse_destination(destination: &str) -> SocksAddr {
    if let Ok(addr) = destination.parse::<SocketAddr>() {
        return SocksAddr {
            host: addr.ip(),
            port: addr.port(),
        };
    }
    SocksAddr {
        host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        port: 0,
    }
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

fn server_config() -> Result<quinn::ServerConfig, HammerError> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .map_err(|err| HammerError::internal(format!("generate certificate: {err}")))?;
    let mut crypto = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
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
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(crypto)
            .map_err(|err| HammerError::internal(format!("server quic tls: {err}")))?,
    ));
    config.transport = Arc::new(quinn::TransportConfig::default());
    Ok(config)
}

fn _validate_obfs(obfs: &Option<Hysteria2Obfs>) -> Result<(), HammerError> {
    if let Some(obfs) = obfs {
        if obfs.type_ != "salamander" {
            return Err(HammerError::internal(format!(
                "unknown obfs type: {}",
                obfs.type_
            )));
        }
    }
    Ok(())
}
