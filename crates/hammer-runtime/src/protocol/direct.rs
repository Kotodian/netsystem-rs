use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use tracing::info;

use async_trait::async_trait;
use bytes::Bytes;
use hammer_adapter::{
    IcmpReply, Network, Outbound, ProxyDatagram, ProxyIcmpConn, ProxyPacketConn, ProxyStream,
    SocksAddr,
};
use hammer_core::config::OutboundKind;
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use socket2::{Domain, Protocol, Socket, Type};
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
            networks: vec![Network::Tcp, Network::Udp, Network::Icmp],
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
            networks: vec![Network::Tcp, Network::Udp, Network::Icmp],
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
        let target = resolve_destination(&destination).await?;
        let socket = if target.is_ipv6() {
            TcpSocket::new_v6()
        } else {
            TcpSocket::new_v4()
        }
        .map_err(|err| HammerError::internal(format!("create direct tcp socket: {err}")))?;
        self.protector.protect(&socket)?;
        let mut stream = socket
            .connect(target)
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
        Ok(Box::new(DirectPacketConn {
            protector: self.protector.clone(),
            ipv4: None,
            ipv6: None,
        }))
    }

    async fn listen_icmp(&self) -> Result<Box<dyn ProxyIcmpConn>, HammerError> {
        info!("outbound icmp connection");
        Ok(Box::new(DirectIcmpConn {
            protector: self.protector.clone(),
            ipv4: None,
            ipv6: None,
        }))
    }
}

struct DirectPacketConn {
    protector: SocketProtector,
    ipv4: Option<UdpSocket>,
    ipv6: Option<UdpSocket>,
}

impl DirectPacketConn {
    fn socket_for(&mut self, destination: IpAddr) -> Result<&UdpSocket, HammerError> {
        if destination.is_ipv6() {
            if self.ipv6.is_none() {
                self.ipv6 = Some(bind_udp_socket(
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                    &self.protector,
                )?);
            }
            return Ok(self.ipv6.as_ref().expect("IPv6 socket just initialized"));
        }

        if self.ipv4.is_none() {
            self.ipv4 = Some(bind_udp_socket(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                &self.protector,
            )?);
        }
        Ok(self.ipv4.as_ref().expect("IPv4 socket just initialized"))
    }
}

#[async_trait]
impl ProxyPacketConn for DirectPacketConn {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> Result<(), HammerError> {
        let target = resolve_destination(&destination).await?;
        self.socket_for(target.ip())?
            .send_to(payload, target)
            .await
            .map_err(|err| HammerError::internal(format!("direct udp send: {err}")))?;
        Ok(())
    }

    async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        match (self.ipv4.as_mut(), self.ipv6.as_mut()) {
            (Some(ipv4), Some(ipv6)) => {
                let mut v4 = vec![0_u8; 64 * 1024];
                let mut v6 = vec![0_u8; 64 * 1024];
                tokio::select! {
                    res = ipv4.recv_from(&mut v4) => datagram_from_recv(res, v4),
                    res = ipv6.recv_from(&mut v6) => datagram_from_recv(res, v6),
                }
            }
            (Some(ipv4), None) => {
                let mut buf = vec![0_u8; 64 * 1024];
                datagram_from_recv(ipv4.recv_from(&mut buf).await, buf)
            }
            (None, Some(ipv6)) => {
                let mut buf = vec![0_u8; 64 * 1024];
                datagram_from_recv(ipv6.recv_from(&mut buf).await, buf)
            }
            (None, None) => Err(HammerError::internal(
                "direct udp recv before any socket is opened",
            )),
        }
    }
}

fn bind_udp_socket(bind_ip: IpAddr, protector: &SocketProtector) -> Result<UdpSocket, HammerError> {
    let socket = std::net::UdpSocket::bind(SocketAddr::new(bind_ip, 0))
        .map_err(|err| HammerError::internal(format!("direct udp bind: {err}")))?;
    socket
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("direct udp set_nonblocking: {err}")))?;
    let socket = UdpSocket::from_std(socket)
        .map_err(|err| HammerError::internal(format!("direct udp from_std: {err}")))?;
    protector.protect(&socket)?;
    Ok(socket)
}

/// Per-flow ICMP echo conduit owned by `DirectOutbound`.
///
/// Holds a pair of unprivileged ICMP DGRAM sockets (`SOCK_DGRAM`,
/// `IPPROTO_ICMP{,V6}`). The kernel attaches an outer IP header on
/// send and strips it on receive, so the body bytes carried across
/// `send_echo` / `recv_reply` are the raw ICMP message starting at the
/// type byte — exactly what the tun stack needs to re-encapsulate.
struct DirectIcmpConn {
    protector: SocketProtector,
    ipv4: Option<UdpSocket>,
    ipv6: Option<UdpSocket>,
}

impl DirectIcmpConn {
    fn socket_for(&mut self, destination: IpAddr) -> Result<&UdpSocket, HammerError> {
        if destination.is_ipv6() {
            if self.ipv6.is_none() {
                self.ipv6 = Some(bind_icmp_socket(
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                    &self.protector,
                )?);
            }
            return Ok(self.ipv6.as_ref().expect("ICMP IPv6 socket just initialized"));
        }
        if self.ipv4.is_none() {
            self.ipv4 = Some(bind_icmp_socket(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                &self.protector,
            )?);
        }
        Ok(self.ipv4.as_ref().expect("ICMP IPv4 socket just initialized"))
    }
}

#[async_trait]
impl ProxyIcmpConn for DirectIcmpConn {
    async fn send_echo(
        &mut self,
        destination: IpAddr,
        body: &[u8],
    ) -> Result<(), HammerError> {
        // ICMP doesn't have a port concept; sendto requires SocketAddr,
        // so port=0 is conventional and the kernel ignores it.
        let target = SocketAddr::new(destination, 0);
        self.socket_for(destination)?
            .send_to(body, target)
            .await
            .map_err(|err| HammerError::internal(format!("direct icmp send: {err}")))?;
        Ok(())
    }

    async fn recv_reply(&mut self) -> Result<IcmpReply, HammerError> {
        match (self.ipv4.as_mut(), self.ipv6.as_mut()) {
            (Some(ipv4), Some(ipv6)) => {
                let mut v4 = vec![0_u8; 64 * 1024];
                let mut v6 = vec![0_u8; 64 * 1024];
                tokio::select! {
                    res = ipv4.recv_from(&mut v4) => icmp_reply_from_recv(res, v4),
                    res = ipv6.recv_from(&mut v6) => icmp_reply_from_recv(res, v6),
                }
            }
            (Some(ipv4), None) => {
                let mut buf = vec![0_u8; 64 * 1024];
                icmp_reply_from_recv(ipv4.recv_from(&mut buf).await, buf)
            }
            (None, Some(ipv6)) => {
                let mut buf = vec![0_u8; 64 * 1024];
                icmp_reply_from_recv(ipv6.recv_from(&mut buf).await, buf)
            }
            (None, None) => Err(HammerError::internal(
                "direct icmp recv before any socket is opened",
            )),
        }
    }
}

fn bind_icmp_socket(
    bind_ip: IpAddr,
    protector: &SocketProtector,
) -> Result<UdpSocket, HammerError> {
    let (domain, protocol) = match bind_ip {
        IpAddr::V4(_) => (Domain::IPV4, Protocol::ICMPV4),
        IpAddr::V6(_) => (Domain::IPV6, Protocol::ICMPV6),
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(protocol))
        .map_err(|err| HammerError::internal(format!("direct icmp socket {bind_ip}: {err}")))?;
    if matches!(bind_ip, IpAddr::V6(_)) {
        socket
            .set_only_v6(true)
            .map_err(|err| HammerError::internal(format!("direct icmp set_only_v6: {err}")))?;
    }
    socket
        .bind(&SocketAddr::new(bind_ip, 0).into())
        .map_err(|err| HammerError::internal(format!("direct icmp bind {bind_ip}: {err}")))?;
    let std_socket: std::net::UdpSocket = socket.into();
    std_socket
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("direct icmp set_nonblocking: {err}")))?;
    let socket = UdpSocket::from_std(std_socket)
        .map_err(|err| HammerError::internal(format!("direct icmp from_std: {err}")))?;
    protector.protect(&socket)?;
    Ok(socket)
}

fn icmp_reply_from_recv(
    result: std::io::Result<(usize, SocketAddr)>,
    mut buf: Vec<u8>,
) -> Result<IcmpReply, HammerError> {
    let (len, source) =
        result.map_err(|err| HammerError::internal(format!("direct icmp recv: {err}")))?;
    buf.truncate(len);
    Ok(IcmpReply {
        source: source.ip(),
        body: Bytes::from(buf),
    })
}

fn datagram_from_recv(
    result: std::io::Result<(usize, SocketAddr)>,
    mut buf: Vec<u8>,
) -> Result<ProxyDatagram, HammerError> {
    let (len, source) =
        result.map_err(|err| HammerError::internal(format!("direct udp recv: {err}")))?;
    buf.truncate(len);
    Ok(ProxyDatagram {
        destination: SocksAddr::ip(source.ip(), source.port()),
        payload: Bytes::from(buf),
    })
}

async fn resolve_destination(destination: &SocksAddr) -> Result<SocketAddr, HammerError> {
    let Some(domain) = destination.domain.as_deref() else {
        return Ok(SocketAddr::new(destination.host, destination.port));
    };
    let mut addrs = tokio::net::lookup_host((domain, destination.port))
        .await
        .map_err(|err| HammerError::internal(format!("direct resolve {domain}: {err}")))?;
    addrs
        .next()
        .ok_or_else(|| HammerError::internal(format!("direct resolve {domain}: empty result")))
}
