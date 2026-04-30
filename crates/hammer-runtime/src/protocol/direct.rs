use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tracing::info;

use async_trait::async_trait;
use bytes::Bytes;
use hammer_adapter::{Network, Outbound, ProxyDatagram, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::OutboundKind;
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpSocket, UdpSocket};

use crate::socket_protector::SocketProtector;

pub struct DirectOutbound {
    id: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    protector: SocketProtector,
}

impl DirectOutbound {
    pub fn new(_logger: Logger, id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
            protector: SocketProtector::default(),
        }
    }

    pub(crate) fn new_with_protector(
        _logger: Logger,
        id: impl Into<String>,
        protector: SocketProtector,
    ) -> Self {
        Self {
            id: id.into(),
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
            protector,
        }
    }
}

pub(crate) fn build_outbound(
    logger: Logger,
    id: String,
    kind: &OutboundKind,
    protector: SocketProtector,
) -> Result<Arc<dyn Outbound>, HammerError> {
    match kind {
        OutboundKind::Direct(_) => Ok(Arc::new(DirectOutbound::new_with_protector(
            logger, id, protector,
        ))),
        _ => Err(HammerError::internal(
            "direct factory received wrong options",
        )),
    }
}

#[async_trait]
impl Outbound for DirectOutbound {
    fn type_name(&self) -> &str {
        "direct"
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

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        if network != Network::Tcp {
            return Err(HammerError::internal("direct dial only supports tcp"));
        }
        info!("outbound connection to {destination}");
        let socket = if destination.host.is_ipv6() {
            TcpSocket::new_v6()
        } else {
            TcpSocket::new_v4()
        }
        .map_err(|err| HammerError::internal(format!("create direct tcp socket: {err}")))?;
        self.protector.protect(&socket)?;
        let mut stream = socket
            .connect(socket_addr(&destination))
            .await
            .map_err(|err| HammerError::internal(format!("direct tcp connect: {err}")))?;
        if !initial_payload.is_empty() {
            stream
                .write_all(initial_payload)
                .await
                .map_err(|err| HammerError::internal(format!("direct tcp write: {err}")))?;
        }
        Ok(Box::new(stream))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        info!("outbound packet connection");
        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
            .await
            .map_err(|err| HammerError::internal(format!("direct udp bind: {err}")))?;
        self.protector.protect(&socket)?;
        Ok(Box::new(DirectPacketConn { socket }))
    }
}

struct DirectPacketConn {
    socket: UdpSocket,
}

#[async_trait]
impl ProxyPacketConn for DirectPacketConn {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> Result<(), HammerError> {
        self.socket
            .send_to(payload, socket_addr(&destination))
            .await
            .map_err(|err| HammerError::internal(format!("direct udp send: {err}")))?;
        Ok(())
    }

    async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        let mut buf = vec![0_u8; 64 * 1024];
        let (len, source) = self
            .socket
            .recv_from(&mut buf)
            .await
            .map_err(|err| HammerError::internal(format!("direct udp recv: {err}")))?;
        buf.truncate(len);
        Ok(ProxyDatagram {
            destination: SocksAddr {
                host: source.ip(),
                port: source.port(),
            },
            payload: Bytes::from(buf),
        })
    }
}

fn socket_addr(destination: &SocksAddr) -> SocketAddr {
    SocketAddr::new(destination.host, destination.port)
}
