use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use h3::server;
use hammer_core::error::HammerError;
use hammer_runtime::hysteria2::protocol;
use http::Response;
use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::PrivateKeyDer;
use tokio::io::AsyncReadExt;
use tokio::sync::Notify;

pub struct EchoServer {
    endpoint: quinn::Endpoint,
    port: u16,
    auth_count: Arc<AtomicUsize>,
    auth_notify: Arc<Notify>,
}

impl EchoServer {
    pub async fn start(password: &str) -> Result<Self, HammerError> {
        Self::start_with_auth_delay(password, Duration::ZERO).await
    }

    pub async fn start_with_auth_delay(
        password: &str,
        auth_delay: Duration,
    ) -> Result<Self, HammerError> {
        let endpoint = quinn::Endpoint::server(
            server_config()?,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        )
        .map_err(|err| HammerError::internal(format!("start echo server: {err}")))?;
        let port = endpoint
            .local_addr()
            .map_err(|err| HammerError::internal(format!("echo server addr: {err}")))?
            .port();
        let server = Self {
            endpoint,
            port,
            auth_count: Arc::new(AtomicUsize::new(0)),
            auth_notify: Arc::new(Notify::new()),
        };
        spawn_accept(
            server.endpoint.clone(),
            password.to_owned(),
            auth_delay,
            Arc::clone(&server.auth_count),
            Arc::clone(&server.auth_notify),
        );
        Ok(server)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn auth_count(&self) -> usize {
        self.auth_count.load(Ordering::SeqCst)
    }

    pub async fn wait_for_auth_count(&self, expected: usize) {
        while self.auth_count() < expected {
            self.auth_notify.notified().await;
        }
    }
}

fn spawn_accept(
    endpoint: quinn::Endpoint,
    password: String,
    auth_delay: Duration,
    auth_count: Arc<AtomicUsize>,
    auth_notify: Arc<Notify>,
) {
    hammer_runtime::spawn::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let password = password.clone();
            let auth_count = Arc::clone(&auth_count);
            let auth_notify = Arc::clone(&auth_notify);
            hammer_runtime::spawn::spawn(async move {
                if let Ok(connection) = incoming.await {
                    let _ = handle_connection(
                        connection,
                        password,
                        auth_delay,
                        auth_count,
                        auth_notify,
                    )
                    .await;
                }
            });
        }
    });
}

async fn handle_connection(
    connection: quinn::Connection,
    password: String,
    auth_delay: Duration,
    auth_count: Arc<AtomicUsize>,
    auth_notify: Arc<Notify>,
) -> Result<(), HammerError> {
    handle_auth(
        connection.clone(),
        password,
        auth_delay,
        auth_count,
        auth_notify,
    )
    .await?;
    spawn_tcp_echo(connection.clone());
    spawn_udp_echo(connection);
    Ok(())
}

async fn handle_auth(
    connection: quinn::Connection,
    password: String,
    auth_delay: Duration,
    auth_count: Arc<AtomicUsize>,
    auth_notify: Arc<Notify>,
) -> Result<(), HammerError> {
    let h3_conn = h3_quinn::Connection::new(connection);
    let mut incoming: server::Connection<_, Bytes> = server::Connection::new(h3_conn)
        .await
        .map_err(|err| HammerError::internal(format!("h3 server init: {err}")))?;
    let resolver = incoming
        .accept()
        .await
        .map_err(|err| HammerError::internal(format!("accept auth request: {err}")))?
        .ok_or_else(|| HammerError::internal("missing auth request"))?;
    let (request, mut stream) = resolver
        .resolve_request()
        .await
        .map_err(|err| HammerError::internal(format!("resolve auth request: {err}")))?;
    auth_count.fetch_add(1, Ordering::SeqCst);
    auth_notify.notify_waiters();
    if !auth_delay.is_zero() {
        tokio::time::sleep(auth_delay).await;
    }
    let auth = protocol::auth_request_from_headers(request.headers());
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
    hammer_runtime::spawn::spawn(async move {
        let _incoming = incoming;
        std::future::pending::<()>().await;
    });
    Ok(())
}

fn spawn_tcp_echo(connection: quinn::Connection) {
    hammer_runtime::spawn::spawn(async move {
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            hammer_runtime::spawn::spawn(async move {
                if protocol::read_tcp_request_header(&mut recv).await.is_err() {
                    return;
                }
                let response = protocol::encode_tcp_response(true, "", b"");
                if send.write_all(&response).await.is_err() {
                    return;
                }
                let mut prefixed = false;
                let mut buf = [0_u8; 4096];
                loop {
                    let len = match AsyncReadExt::read(&mut recv, &mut buf).await {
                        Ok(0) => break,
                        Ok(len) => len,
                        Err(_) => return,
                    };
                    if !prefixed {
                        prefixed = true;
                        if send.write_all(b"echo:").await.is_err() {
                            return;
                        }
                    }
                    if send.write_all(&buf[..len]).await.is_err() {
                        return;
                    }
                }
                let _ = send.finish();
            });
        }
    });
}

fn spawn_udp_echo(connection: quinn::Connection) {
    hammer_runtime::spawn::spawn(async move {
        while let Ok(datagram) = connection.read_datagram().await {
            if let Ok(request) = protocol::UdpMessage::decode(datagram) {
                let mut payload = bytes::BytesMut::from(&b"echo:"[..]);
                payload.extend_from_slice(&request.payload);
                let response = protocol::UdpMessage {
                    session_id: request.session_id,
                    packet_id: request.packet_id,
                    fragment_id: 0,
                    fragment_total: 1,
                    destination: request.destination,
                    payload: payload.freeze(),
                };
                let _ = connection.send_datagram(response.encode());
            }
        }
    });
}

fn server_config() -> Result<quinn::ServerConfig, HammerError> {
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
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(crypto)
            .map_err(|err| HammerError::internal(format!("server quic tls: {err}")))?,
    ));
    config.transport = Arc::new(quinn::TransportConfig::default());
    Ok(config)
}
