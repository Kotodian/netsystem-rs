use std::sync::Arc;

use async_trait::async_trait;
use hammer_core::error::CoreError;
use hammer_core::lifecycle::Lifecycle;

use crate::dialer::Network;
use crate::rule::SocksAddr;

#[async_trait]
pub trait ProxyStream: Send + Sync + 'static {
    async fn read_to_end(&mut self) -> Result<Vec<u8>, CoreError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyDatagram {
    pub destination: SocksAddr,
    pub payload: Vec<u8>,
}

#[async_trait]
pub trait ProxyPacketConn: Send + Sync + 'static {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> Result<(), CoreError>;
    async fn recv_from(&mut self) -> Result<ProxyDatagram, CoreError>;
}

/// `adapter.Outbound` in Go — represents a single egress (hysteria2, direct,
/// block, dns, …). Async dial/listen methods are deferred to M6 when the
/// Hysteria2 outbound brings real I/O. The shape stays stable so adding those
/// later is an extension, not a refactor.
#[async_trait]
pub trait Outbound: Send + Sync + 'static {
    fn type_name(&self) -> &str;
    fn tag(&self) -> &str;
    fn networks(&self) -> &[Network];
    fn dependencies(&self) -> &[String];

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, CoreError>;

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, CoreError>;
}

/// `adapter.OutboundManager` — owns the live set of outbounds and a default
/// fallback (used when a route rule has no explicit outbound match).
pub trait OutboundManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn Outbound>>;
    fn get(&self, tag: &str) -> Option<Arc<dyn Outbound>>;
    fn default(&self) -> Option<Arc<dyn Outbound>>;
    fn remove(&self, tag: &str) -> Result<(), CoreError>;
}
