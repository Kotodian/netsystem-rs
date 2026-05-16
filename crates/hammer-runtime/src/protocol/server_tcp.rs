use std::net::SocketAddr;
use std::sync::Arc;

use hammer_adapter::PlatformInterface;
use hammer_core::error::{HammerError, HammerResult, WithContext};
use tokio::net::{TcpSocket, TcpStream};

use crate::socket_protector::SocketProtector;

#[derive(Clone)]
pub(crate) struct ServerTcpConnector {
    server: String,
    server_port: u16,
    protector: SocketProtector,
}

impl ServerTcpConnector {
    pub(crate) fn builder() -> ServerTcpConnectorBuilder {
        ServerTcpConnectorBuilder::default()
    }

    pub(crate) async fn connect(&self, context: &str) -> HammerResult<TcpStream> {
        let target = resolve_server(&self.server, self.server_port, context).await?;
        let socket = if target.is_ipv6() {
            TcpSocket::new_v6()
        } else {
            TcpSocket::new_v4()
        }
        .with_context(|| format!("{context} create tcp socket"))?;
        self.protector.protect(&socket)?;
        socket
            .connect(target)
            .await
            .with_context(|| format!("{context} tcp connect"))
    }

    pub(crate) fn platform(&self) -> Option<Arc<dyn PlatformInterface>> {
        self.protector.platform()
    }
}

#[derive(Default)]
pub(crate) struct ServerTcpConnectorBuilder {
    server: Option<String>,
    server_port: Option<u16>,
    protector: SocketProtector,
}

impl ServerTcpConnectorBuilder {
    pub(crate) fn server(mut self, server: impl Into<String>) -> Self {
        self.server = Some(server.into());
        self
    }

    pub(crate) fn server_port(mut self, server_port: u16) -> Self {
        self.server_port = Some(server_port);
        self
    }

    pub(crate) fn protector(mut self, protector: SocketProtector) -> Self {
        self.protector = protector;
        self
    }

    pub(crate) fn build(self) -> HammerResult<ServerTcpConnector> {
        let server = self
            .server
            .ok_or_else(|| HammerError::internal("server tcp connector missing server"))?;
        if server.is_empty() {
            return Err(HammerError::config_validation(
                "server tcp connector server is required",
            ));
        }
        let server_port = self
            .server_port
            .ok_or_else(|| HammerError::internal("server tcp connector missing server_port"))?;
        Ok(ServerTcpConnector {
            server,
            server_port,
            protector: self.protector,
        })
    }
}

async fn resolve_server(server: &str, port: u16, context: &str) -> HammerResult<SocketAddr> {
    if let Ok(ip) = server.parse() {
        return Ok(SocketAddr::new(ip, port));
    }
    let mut addrs = tokio::net::lookup_host((server, port))
        .await
        .with_context(|| format!("{context} resolve {server}"))?;
    addrs
        .next()
        .ok_or_else(|| HammerError::internal(format!("{context} resolve {server}: empty result")))
}
