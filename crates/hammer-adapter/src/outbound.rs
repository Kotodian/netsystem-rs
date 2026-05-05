use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use hammer_core::error::CoreError;
use hammer_core::lifecycle::Lifecycle;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::dialer::Network;
use crate::rule::SocksAddr;

pub trait ProxyStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> ProxyStream for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyDatagram {
    pub destination: SocksAddr,
    pub payload: Bytes,
}

#[async_trait]
pub trait ProxyPacketConn: Send + Sync + 'static {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> Result<(), CoreError>;
    async fn recv_from(&mut self) -> Result<ProxyDatagram, CoreError>;
}

/// One inbound ICMP echo reply observed on a `ProxyIcmpConn`.
///
/// `body` carries the entire ICMP message starting at the type byte —
/// the kernel strips the IP header for `SOCK_DGRAM, IPPROTO_ICMP*`
/// sockets — so the consumer can re-encapsulate it directly into the
/// IP packet that goes back into the tun.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcmpReply {
    pub source: IpAddr,
    pub body: Bytes,
}

/// Per-flow ICMP echo conduit. Mirrors `ProxyPacketConn` but for the
/// echo subset (type 8/0 on v4, type 128/129 on v6) only.
///
/// `body` is the raw ICMP message starting at the type byte
/// (type+code+checksum+identifier+sequence+payload). Implementations
/// must own dual v4/v6 sockets and dispatch by `destination` family on
/// send.
#[async_trait]
pub trait ProxyIcmpConn: Send + Sync + 'static {
    async fn send_echo(
        &mut self,
        destination: IpAddr,
        body: &[u8],
    ) -> Result<(), CoreError>;
    async fn recv_reply(&mut self) -> Result<IcmpReply, CoreError>;
}

/// `adapter.Outbound` in Go — represents a single dialable egress
/// (hysteria2, direct, block, …). DNS queries use DnsRouter/DnsTransport
/// instead of pretending to be a normal outbound.
#[async_trait]
pub trait Outbound: Send + Sync + 'static {
    fn type_name(&self) -> &str;
    fn id(&self) -> &str;
    fn networks(&self) -> &[Network];
    fn dependencies(&self) -> &[String];
    fn reset(&self) {}

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, CoreError>;

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, CoreError>;

    /// Open an ICMP echo conduit on this outbound. The default impl
    /// reports unsupported, so only outbounds that genuinely carry ICMP
    /// (today: `direct`) override; the tun stack converts the resulting
    /// `Err` into an ICMP Destination Unreachable response written back
    /// to the client.
    async fn listen_icmp(&self) -> Result<Box<dyn ProxyIcmpConn>, CoreError> {
        Err(CoreError::internal(format!(
            "icmp not supported by outbound: {}",
            self.id()
        )))
    }
}

/// `adapter.OutboundManager` — owns the live set of outbounds and a default
/// fallback (used when a route rule has no explicit outbound match).
pub trait OutboundManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn Outbound>>;
    fn get(&self, id: &str) -> Option<Arc<dyn Outbound>>;
    fn default(&self) -> Option<Arc<dyn Outbound>>;
    fn remove(&self, id: &str) -> Result<(), CoreError>;
}
