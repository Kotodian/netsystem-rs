use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future;
use h3::client;
use hammer_adapter::{Network, Outbound, ProxyDatagram, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::{Hysteria2Obfs, Hysteria2OutboundOptions};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use http::Request;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio::sync::{Mutex, mpsc};

pub mod bbr;
pub mod obfs;
pub mod protocol;
pub mod testing;

use bbr::{BbrProfile, apply_transport_config};

#[derive(Debug, Clone)]
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
        let endpoint = client_endpoint(&options)?;
        let address = resolve_server(&options.server, options.server_port)?;
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
        let h3_sender = authenticate(&connection, &options).await?;
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
        let (mut send, recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|err| HammerError::internal(format!("open hysteria2 stream: {err}")))?;
        let request = protocol::encode_tcp_request(&destination.to_string(), initial_payload);
        send.write_all(&request)
            .await
            .map_err(|err| HammerError::internal(format!("write hysteria2 stream: {err}")))?;
        send.finish()
            .map_err(|err| HammerError::internal(format!("finish hysteria2 stream: {err}")))?;
        self.logger
            .debug(format!("outbound connection to {destination}"));
        Ok(Hysteria2Stream { recv })
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
    recv: quinn::RecvStream,
}

impl Hysteria2Stream {
    pub async fn read_to_end(&mut self) -> Result<Vec<u8>, HammerError> {
        <Self as ProxyStream>::read_to_end(self).await
    }
}

#[async_trait]
impl ProxyStream for Hysteria2Stream {
    async fn read_to_end(&mut self) -> Result<Vec<u8>, HammerError> {
        let bytes = self
            .recv
            .read_to_end(usize::MAX)
            .await
            .map_err(|err| HammerError::internal(format!("read hysteria2 stream: {err}")))?;
        let response = protocol::decode_tcp_response(&bytes)?;
        if response.ok {
            Ok(response.payload)
        } else {
            Err(HammerError::internal(format!(
                "remote error: {}",
                response.message
            )))
        }
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
}

impl Hysteria2Outbound {
    pub fn new(logger: Logger, tag: String, options: Hysteria2OutboundOptions) -> Self {
        let networks = parse_networks(&options.network);
        Self {
            logger,
            tag,
            options,
            networks,
            dependencies: Vec::new(),
            client: Mutex::new(None),
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
) -> Result<client::SendRequest<h3_quinn::OpenStreams, Bytes>, HammerError> {
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
            rx: 0,
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
    let _ = protocol::auth_response_from_headers(response.headers());
    Ok(sender)
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

fn client_endpoint(options: &ClientOptions) -> Result<quinn::Endpoint, HammerError> {
    let mut endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .map_err(|err| HammerError::internal(format!("create QUIC endpoint: {err}")))?;
    endpoint.set_default_client_config(client_config(options)?);
    Ok(endpoint)
}

fn client_config(options: &ClientOptions) -> Result<quinn::ClientConfig, HammerError> {
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
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    };
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let mut config = quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(crypto)
            .map_err(|err| HammerError::internal(format!("quic tls config: {err}")))?,
    ));
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(10)));
    apply_transport_config(
        &mut config,
        transport,
        options.bbr_profile,
        options.initial_packet_size,
        options.disable_path_mtu_discovery,
    );
    Ok(config)
}

fn resolve_server(server: &str, port: u16) -> Result<SocketAddr, HammerError> {
    let port = if port == 0 { 443 } else { port };
    (server, port)
        .to_socket_addrs()
        .map_err(|err| HammerError::internal(format!("resolve hysteria2 server: {err}")))?
        .next()
        .ok_or_else(|| HammerError::internal("empty hysteria2 server resolution"))
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
