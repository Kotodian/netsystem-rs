use std::sync::OnceLock;

use hammer_core::config::network::CongestionController;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::registry::RuntimeRegistry;

pub mod congestion;
pub mod tcp;
pub mod udp;

pub struct TransportMain {
    congestion: CongestionController,
}

impl TransportMain {
    pub fn new(congestion: CongestionController) -> Self {
        Self { congestion }
    }

    pub fn congestion(&self) -> CongestionController {
        self.congestion
    }
}

pub static TRANSPORT_MAIN: OnceLock<TransportMain> = OnceLock::new();

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    let config = reg.require::<hammer_core::config::Config>()?;
    TRANSPORT_MAIN
        .set(TransportMain::new(config.network.tcp.congestion))
        .map_err(|_| HammerError::internal("transport main already initialized"))?;
    Ok(())
}

#[linkme::distributed_slice(crate::packet_graph::CONTROL_INITS)]
fn init_transport(reg: &RuntimeRegistry) -> HammerResult<()> {
    init(reg)
}

/// Config → CC controller type. Single transport-layer dispatch point.
#[macro_export]
macro_rules! with_congestion {
    (|$cc:ident| $body:expr) => {{
        match crate::transport::TRANSPORT_MAIN
            .get()
            .ok_or_else(|| ::hammer_core::error::CoreError::internal("transport main not initialized"))?
            .congestion()
        {
            ::hammer_core::config::network::CongestionController::Bbr => {{
                type $cc = $crate::transport::congestion::BbrController;
                $body
            }}
        }
    }};
}
