use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::Bytes;
use h3::server;
use hammer_core::error::HammerError;
use http::Response;

use super::{parse_destination, protocol, server_config};

#[doc(hidden)]
pub struct EchoServer {
    endpoint: quinn::Endpoint,
    port: u16,
}

impl EchoServer {
    pub async fn start(password: &str) -> Result<Self, HammerError> {
        let endpoint = quinn::Endpoint::server(
            server_config()?,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        )
        .map_err(|err| HammerError::internal(format!("start echo server: {err}")))?;
        let port = endpoint
            .local_addr()
            .map_err(|err| HammerError::internal(format!("echo server addr: {err}")))?
            .port();
        let server = Self { endpoint, port };
        spawn_accept(server.endpoint.clone(), password.to_owned());
        Ok(server)
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

fn spawn_accept(endpoint: quinn::Endpoint, password: String) {
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let password = password.clone();
            tokio::spawn(async move {
                if let Ok(connection) = incoming.await {
                    let _ = handle_connection(connection, password).await;
                }
            });
        }
    });
}

async fn handle_connection(
    connection: quinn::Connection,
    password: String,
) -> Result<(), HammerError> {
    handle_auth(connection.clone(), password).await?;
    spawn_tcp_echo(connection.clone());
    spawn_udp_echo(connection);
    Ok(())
}

async fn handle_auth(connection: quinn::Connection, password: String) -> Result<(), HammerError> {
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
    tokio::spawn(async move {
        let _incoming = incoming;
        std::future::pending::<()>().await;
    });
    Ok(())
}

fn spawn_tcp_echo(connection: quinn::Connection) {
    tokio::spawn(async move {
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            tokio::spawn(async move {
                let mut data = Vec::new();
                if recv
                    .read_to_end(usize::MAX)
                    .await
                    .map(|v| data = v)
                    .is_err()
                {
                    return;
                }
                let request = match protocol::decode_tcp_request(&data) {
                    Ok(request) => request,
                    Err(_) => return,
                };
                let mut payload = b"echo:".to_vec();
                payload.extend_from_slice(&request.payload);
                let response = protocol::encode_tcp_response(true, "", &payload);
                if send.write_all(&response).await.is_ok() {
                    let _ = send.finish();
                }
            });
        }
    });
}

fn spawn_udp_echo(connection: quinn::Connection) {
    tokio::spawn(async move {
        while let Ok(datagram) = connection.read_datagram().await {
            if let Ok(request) = protocol::UdpMessage::decode(&datagram) {
                let _ = parse_destination(&request.destination);
                let mut payload = b"echo:".to_vec();
                payload.extend_from_slice(&request.payload);
                let response = protocol::UdpMessage {
                    session_id: request.session_id,
                    packet_id: request.packet_id,
                    fragment_id: 0,
                    fragment_total: 1,
                    destination: request.destination,
                    payload,
                };
                let _ = connection.send_datagram(Bytes::from(response.encode()));
            }
        }
    });
}
