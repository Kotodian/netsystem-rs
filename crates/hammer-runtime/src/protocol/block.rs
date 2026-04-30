use tracing::info;

use async_trait::async_trait;
use hammer_adapter::{Network, Outbound, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::OutboundKind;
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use std::sync::Arc;

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
) -> Result<Arc<dyn Outbound>, HammerError> {
    match kind {
        OutboundKind::Block => Ok(Arc::new(BlockOutbound::new(logger, id))),
        _ => Err(HammerError::internal(
            "block factory received wrong options",
        )),
    }
}

#[async_trait]
impl Outbound for BlockOutbound {
    fn type_name(&self) -> &str {
        "block"
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
        _network: Network,
        destination: SocksAddr,
        _initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        info!("blocked connection to {destination}");
        Err(HammerError::internal(format!(
            "blocked connection to {destination}"
        )))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        info!("blocked packet connection");
        Err(HammerError::internal("blocked packet connection"))
    }
}
