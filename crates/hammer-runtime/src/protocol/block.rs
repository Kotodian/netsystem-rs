use tracing::info;

use async_trait::async_trait;
use hammer_adapter::{Network, Outbound, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::OutboundKind;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::log::Logger;
use std::sync::Arc;

#[hammer_component_macros::hammer_component(
    outbound,
    name = "block",
    builder = build_outbound,
    metrics = ("outbound", "outbound")
)]
pub struct BlockOutbound {
    id: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
}

impl BlockOutbound {
    pub fn new(_logger: Logger, id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
        }
    }
}

pub(crate) fn build_outbound(
    logger: Logger,
    id: String,
    kind: &OutboundKind,
    _protector: crate::socket_protector::SocketProtector,
) -> HammerResult<Arc<BlockOutbound>> {
    match kind {
        OutboundKind::Block => Ok(Arc::new(BlockOutbound::new(logger, id))),
        _ => Err(HammerError::internal(
            "block factory received wrong options",
        )),
    }
}

#[async_trait]
impl Outbound for BlockOutbound {
    async fn dial(
        &self,
        _network: Network,
        destination: SocksAddr,
        _initial_payload: &[u8],
    ) -> HammerResult<Box<dyn ProxyStream>> {
        info!("blocked connection to {destination}");
        Err(HammerError::internal(format!(
            "blocked connection to {destination}"
        )))
    }

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
        info!("blocked packet connection");
        Err(HammerError::internal("blocked packet connection"))
    }
}
