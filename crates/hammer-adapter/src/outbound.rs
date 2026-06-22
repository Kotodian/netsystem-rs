use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::lifecycle::Lifecycle;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::RuntimeComponent;
use crate::buffer::{BufferFrame, DataPlaneBuffers};
use crate::dialer::Network;
use hammer_core::SocksAddr;

pub type OutboundComponent = RuntimeComponent<dyn Outbound>;

pub trait ProxyStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> ProxyStream for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

#[async_trait(?Send)]
pub trait ProxyPacketConn: 'static {
    async fn send(&mut self, runtime: &DataPlaneBuffers, frame: &mut BufferFrame)
    -> CoreResult<()>;
    async fn recv(
        &mut self,
        runtime: &DataPlaneBuffers,
        frame: &mut BufferFrame,
        max: usize,
    ) -> CoreResult<()>;
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

/// `adapter.Outbound` in Go — represents a single dialable egress.
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
    /// override; the tun stack converts the resulting
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
}

/// `adapter.OutboundManager` — owns the live set of outbounds and a default
/// fallback (used when a route rule has no explicit outbound match).
pub trait OutboundManager: Lifecycle {
    fn list(&self) -> Vec<OutboundComponent>;
    fn get(&self, id: &str) -> Option<OutboundComponent>;
    fn default(&self) -> Option<OutboundComponent>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::DataPlaneRuntime;
    use hammer_infra::vec::Vec;

    struct CapturePacketConn {
        last: Option<Vec<u8>>,
    }

    #[async_trait(?Send)]
    impl ProxyPacketConn for CapturePacketConn {
        async fn send(
            &mut self,
            runtime: &DataPlaneBuffers,
            frame: &mut BufferFrame,
        ) -> CoreResult<()> {
            let index = frame
                .drain_indices()
                .next()
                .ok_or_else(|| CoreError::internal("empty test frame"))?;
            self.last = Some(runtime.copy_current_chain(index)?);
            runtime.free_index(index);
            Ok(())
        }

        async fn recv(
            &mut self,
            _runtime: &DataPlaneBuffers,
            _frame: &mut BufferFrame,
            _max: usize,
        ) -> CoreResult<()> {
            panic!("recv is not used in this test")
        }
    }

    #[tokio::test]
    async fn packet_conn_send_uses_borrowed_frame() {
        let runtime: DataPlaneRuntime = DataPlaneRuntime::with_buffer_capacity(128, 1);
        let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
        frame
            .push_index(
                runtime
                    .alloc_index_with_bytes(b"borrowed udp payload")
                    .expect("alloc UDP payload"),
            )
            .expect("push UDP payload");
        let mut conn = CapturePacketConn { last: None };

        conn.send(&runtime, &mut frame).await.expect("send frame");

        assert!(frame.is_empty());
        assert_eq!(
            conn.last,
            Some(b"borrowed udp payload".iter().copied().collect::<Vec<_>>())
        );
        assert_eq!(runtime.in_use_buffers(), 0);
        runtime
            .release_pooled_frame(frame)
            .expect("release pooled frame");
    }
}
