use std::net::IpAddr;
use std::sync::Weak;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::lifecycle::Lifecycle;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::RuntimeComponent;
use crate::dialer::Network;
use crate::rule::SocksAddr;

pub type OutboundComponent = RuntimeComponent<dyn Outbound>;

pub trait ProxyStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> ProxyStream for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyDatagram {
    pub destination: SocksAddr,
    pub payload: Bytes,
}

#[async_trait]
pub trait ProxyPacketConn: Send + Sync + 'static {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> CoreResult<()>;
    async fn recv_from(&mut self) -> CoreResult<ProxyDatagram>;
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
    async fn send_echo(&mut self, destination: IpAddr, body: &[u8]) -> CoreResult<()>;
    async fn recv_reply(&mut self) -> CoreResult<IcmpReply>;
}

/// `adapter.Outbound` in Go — represents a single dialable egress
/// (hysteria2, direct, block, …). DNS queries use DnsRouter/DnsTransport
/// instead of pretending to be a normal outbound.
#[async_trait]
pub trait Outbound: Send + Sync + 'static {
    fn reset(&self) {}

    /// Ensure this outbound has a live cached connection if it needs one.
    /// Stateless outbounds keep the no-op; cached outbounds should return
    /// immediately when already connected.
    async fn ensure_connected(&self) -> CoreResult<()> {
        Ok(())
    }

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> CoreResult<Box<dyn ProxyStream>>;

    async fn listen_packet(&self) -> CoreResult<Box<dyn ProxyPacketConn>>;

    /// Open an ICMP echo conduit on this outbound. The default impl
    /// reports unsupported, so only outbounds that genuinely carry ICMP
    /// (today: `direct`) override; the tun stack converts the resulting
    /// `Err` into an ICMP Destination Unreachable response written back
    /// to the client.
    async fn listen_icmp(&self) -> CoreResult<Box<dyn ProxyIcmpConn>> {
        Err(CoreError::internal(format!(
            "icmp not supported by outbound"
        )))
    }

    /// Measure latency to this outbound's own probe endpoint for the
    /// requested protocol. The default reports unsupported; server-backed
    /// outbounds can override without forcing probe code to downcast.
    async fn probe_latency(&self, protocol: &str, _timeout: Duration) -> CoreResult<Duration> {
        Err(CoreError::internal(format!(
            "{protocol} probe not supported by outbound"
        )))
    }

    /// Hook invoked once after the runtime finishes the regular `Start`
    /// stage. Outbounds may override it for non-blocking startup work.
    /// Default is no-op so leaf outbounds need no opt-in.
    ///
    /// Returning `Err` is logged but does not abort service startup —
    /// callers treat the hook as fire-and-forget.
    async fn post_start(&self) -> CoreResult<()> {
        Ok(())
    }

    /// Aggregate outbounds (urltest, future selectors) report the id of
    /// the child currently selected. Returning `None` (the default) means
    /// the outbound is a leaf and no inner choice exists.
    fn now(&self) -> Option<String> {
        None
    }

    /// Resolve the effective per-probe timeout for an aggregate probe sweep.
    /// Leaf/default implementations simply echo the caller value; aggregate
    /// outbounds with configured defaults can override zero-duration calls.
    fn probe_group_timeout(&self, timeout: Duration) -> Duration {
        timeout
    }

    /// Inject the resolver an aggregate outbound uses to look up its
    /// children at run time. The runtime walks every registered outbound
    /// after the manager is `Arc`-wrapped and calls this once with a
    /// `Weak` so aggregates can fan out without owning the manager.
    /// Default is a no-op for leaves.
    fn bind_resolver(&self, _resolver: Weak<dyn OutboundManager>) {}

    /// Run an aggregate outbound's group-probe sweep on demand. Reports
    /// land in declaration order with `outbound_id` set to each child's
    /// id and `protocol` to a sweep-kind tag (e.g. `"urltest"`). Leaf
    /// outbounds reject the call by default since they have no children.
    async fn probe_group(&self, _timeout: Duration) -> CoreResult<Vec<crate::probe::ProbeReport>> {
        Err(CoreError::internal(format!("outbound has no group probe")))
    }
}

/// `adapter.OutboundManager` — owns the live set of outbounds and a default
/// fallback (used when a route rule has no explicit outbound match).
pub trait OutboundManager: Lifecycle {
    fn list(&self) -> Vec<OutboundComponent>;
    fn get(&self, id: &str) -> Option<OutboundComponent>;
    fn default(&self) -> Option<OutboundComponent>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}
