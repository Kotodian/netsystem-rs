use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_core::config::network::CongestionController;
use hammer_core::error::HammerResult;
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

// VPP alignment: `transport_main_t transport_main;` is a file-scope global in
// VPP's `transport.c`; nodes read it via `&transport_main` (lock-free direct
// deref). `transport_init` fills it once and `vlib_test_cleanup` resets it
// between tests. The Rust mirror is a `pub static ArcSwapOption<TransportMain>`:
// `.load()` is lock-free on the hot path, and `store(None)` makes it resettable
// for test isolation — neither of which `OnceLock` provides.
pub static TRANSPORT_MAIN: ArcSwapOption<TransportMain> = ArcSwapOption::const_empty();

#[cfg(test)]
pub(crate) fn reset_for_test() {
    TRANSPORT_MAIN.store(None);
}

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    let config = reg.require::<hammer_core::config::Config>()?;
    TRANSPORT_MAIN.store(Some(Arc::new(TransportMain::new(config.network.tcp.congestion))));
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
            .load()
            .as_deref()
            .ok_or_else(|| {
                ::hammer_core::error::CoreError::internal("transport main not initialized")
            })?
            .congestion()
        {
            ::hammer_core::config::network::CongestionController::Bbr => {
                type $cc = $crate::transport::congestion::BbrController;
                $body
            }
        }
    }};
}
