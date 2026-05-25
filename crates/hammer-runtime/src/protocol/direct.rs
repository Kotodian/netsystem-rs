use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use tracing::info;

use async_trait::async_trait;
use bytes::Bytes;
use hammer_adapter::{
    Network, Outbound, ProxyDatagram, ProxyIcmpConn, ProxyPacketConn, ProxyStream, SocksAddr,
};
use hammer_core::config::OutboundKind;
use hammer_core::error::{HammerError, HammerResult, WithContext};
use hammer_core::log::Logger;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpSocket, UdpSocket};

use crate::protocol::icmp::IcmpSocketConn;
use crate::socket_protector::SocketProtector;

#[hammer_component_macros::hammer_component(
    outbound,
    name = "direct",
    builder = build_outbound,
    metrics = ("outbound", "outbound")
)]
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
    _control_handle: Option<Arc<crate::ControlThreadHandle>>,
) -> HammerResult<Arc<DirectOutbound>> {
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
    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> HammerResult<Box<dyn ProxyStream>> {
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
        .with_context(|| "create direct tcp socket")?;
        self.protector.protect(&socket)?;
        let mut stream = socket
            .connect(target)
            .await
            .with_context(|| "direct tcp connect")?;
        if !initial_payload.is_empty() {
            stream
                .write_all(initial_payload)
                .await
                .with_context(|| "direct tcp write")?;
        }
        Ok(Box::new(stream))
    }

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
        info!("outbound packet connection");
        Ok(Box::new(DirectPacketConn {
            protector: self.protector.clone(),
            ipv4: None,
            ipv6: None,
        }))
    }

    async fn listen_icmp(&self) -> HammerResult<Box<dyn ProxyIcmpConn>> {
        info!("outbound icmp connection");
        Ok(Box::new(IcmpSocketConn::new(self.protector.clone())))
    }
}

struct DirectPacketConn {
    protector: SocketProtector,
    ipv4: Option<UdpSocket>,
    ipv6: Option<UdpSocket>,
}

impl DirectPacketConn {
    fn socket_for(&mut self, destination: IpAddr) -> HammerResult<&UdpSocket> {
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
    async fn send_to(&mut self, destination: SocksAddr, payload: Bytes) -> HammerResult<()> {
        let target = resolve_destination(&destination).await?;
        self.socket_for(target.ip())?
            .send_to(&payload, target)
            .await
            .with_context(|| "direct udp send")?;
        Ok(())
    }

    async fn recv_from(&mut self) -> HammerResult<ProxyDatagram> {
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

fn bind_udp_socket(bind_ip: IpAddr, protector: &SocketProtector) -> HammerResult<UdpSocket> {
    let socket = std::net::UdpSocket::bind(SocketAddr::new(bind_ip, 0))
        .with_context(|| "direct udp bind")?;
    socket
        .set_nonblocking(true)
        .with_context(|| "direct udp set_nonblocking")?;
    let socket = UdpSocket::from_std(socket).with_context(|| "direct udp from_std")?;
    protector.protect(&socket)?;
    Ok(socket)
}

fn datagram_from_recv(
    result: std::io::Result<(usize, SocketAddr)>,
    mut buf: Vec<u8>,
) -> HammerResult<ProxyDatagram> {
    let (len, source) = result.with_context(|| "direct udp recv")?;
    buf.truncate(len);
    Ok(ProxyDatagram {
        destination: SocksAddr::ip(source.ip(), source.port()),
        payload: Bytes::from(buf),
    })
}

async fn resolve_destination(destination: &SocksAddr) -> HammerResult<SocketAddr> {
    let Some(domain) = destination.domain.as_deref() else {
        return Ok(SocketAddr::new(destination.host, destination.port));
    };
    let mut addrs = tokio::net::lookup_host((domain, destination.port))
        .await
        .with_context(|| format!("direct resolve {domain}"))?;
    addrs
        .next()
        .ok_or_else(|| HammerError::internal(format!("direct resolve {domain}: empty result")))
}
