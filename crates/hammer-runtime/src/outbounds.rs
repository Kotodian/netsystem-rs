use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use hammer_adapter::{Network, Outbound, ProxyDatagram, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

const TCP_READ_LIMIT: usize = 16 * 1024 * 1024;

pub struct DirectOutbound {
    logger: Logger,
    tag: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
}

impl DirectOutbound {
    pub fn new(logger: Logger, tag: impl Into<String>) -> Self {
        Self {
            logger,
            tag: tag.into(),
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
        }
    }
}

#[async_trait]
impl Outbound for DirectOutbound {
    fn type_name(&self) -> &str {
        "direct"
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
        if network != Network::Tcp {
            return Err(HammerError::internal("direct dial only supports tcp"));
        }
        self.logger
            .info(format!("outbound connection to {destination}"));
        let mut stream = TcpStream::connect(socket_addr(&destination))
            .await
            .map_err(|err| HammerError::internal(format!("direct tcp connect: {err}")))?;
        if !initial_payload.is_empty() {
            stream
                .write_all(initial_payload)
                .await
                .map_err(|err| HammerError::internal(format!("direct tcp write: {err}")))?;
        }
        stream
            .shutdown()
            .await
            .map_err(|err| HammerError::internal(format!("direct tcp shutdown: {err}")))?;
        Ok(Box::new(DirectStream { stream }))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        self.logger.info("outbound packet connection");
        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
            .await
            .map_err(|err| HammerError::internal(format!("direct udp bind: {err}")))?;
        Ok(Box::new(DirectPacketConn { socket }))
    }
}

struct DirectStream {
    stream: TcpStream,
}

#[async_trait]
impl ProxyStream for DirectStream {
    async fn read_to_end(&mut self) -> Result<Vec<u8>, HammerError> {
        let mut bytes = Vec::new();
        self.stream
            .read_to_end(&mut bytes)
            .await
            .map_err(|err| HammerError::internal(format!("direct tcp read: {err}")))?;
        if bytes.len() > TCP_READ_LIMIT {
            return Err(HammerError::internal("direct tcp response too large"));
        }
        Ok(bytes)
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
            payload: buf,
        })
    }
}

pub struct BlockOutbound {
    logger: Logger,
    tag: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
}

impl BlockOutbound {
    pub fn new(logger: Logger, tag: impl Into<String>) -> Self {
        Self {
            logger,
            tag: tag.into(),
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
        }
    }
}

#[async_trait]
impl Outbound for BlockOutbound {
    fn type_name(&self) -> &str {
        "block"
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
        _network: Network,
        destination: SocksAddr,
        _initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        self.logger
            .info(format!("blocked connection to {destination}"));
        Err(HammerError::internal(format!(
            "blocked connection to {destination}"
        )))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        self.logger.info("blocked packet connection");
        Err(HammerError::internal("blocked packet connection"))
    }
}

pub struct DnsOutbound {
    tag: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
}

impl DnsOutbound {
    pub fn new(tag: impl Into<String>) -> Self {
        Self {
            tag: tag.into(),
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
        }
    }
}

#[async_trait]
impl Outbound for DnsOutbound {
    fn type_name(&self) -> &str {
        "dns"
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
        _network: Network,
        _destination: SocksAddr,
        _initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        Err(HammerError::internal(
            "invalid operation: dns outbound cannot dial directly",
        ))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        Err(HammerError::internal(
            "invalid operation: dns outbound cannot listen directly",
        ))
    }
}

fn socket_addr(destination: &SocksAddr) -> SocketAddr {
    SocketAddr::new(destination.host, destination.port)
}
