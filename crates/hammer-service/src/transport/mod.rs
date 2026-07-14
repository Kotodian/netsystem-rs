use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_core::config::SessionBackend;
use hammer_core::error::HammerResult;
use hammer_core::registry::RuntimeRegistry;

pub mod congestion;

/// Transport-layer runtime snapshot.
///
/// Session schema lives in its owning plugin. This module keeps only the
/// protocol-neutral backend fact used by registered transports.
pub struct TransportMain {
    session_backend: SessionBackend,
}

impl TransportMain {
    pub fn new(session_backend: SessionBackend) -> Self {
        Self { session_backend }
    }

    pub fn session_backend(&self) -> SessionBackend {
        self.session_backend
    }
}

pub static TRANSPORT_MAIN: ArcSwapOption<TransportMain> = ArcSwapOption::const_empty();

pub fn session_backend() -> Option<SessionBackend> {
    TRANSPORT_MAIN
        .load()
        .as_deref()
        .map(|m| m.session_backend())
}

/// Session plugin publishes its chosen backend into the transport snapshot.
pub fn publish_session_backend(backend: SessionBackend) {
    TRANSPORT_MAIN.store(Some(Arc::new(TransportMain::new(backend))));
}

pub fn reset_for_test() {
    TRANSPORT_MAIN.store(None);
}

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    init_transport(reg.require::<hammer_core::config::Config>()?)
}

#[hammer_component_macros::init_function(name = "transport_init")]
fn init_transport(_config: Arc<hammer_core::config::Config>) -> HammerResult<()> {
    // Defaults only when no plugin has published yet (config phase may already have).
    if TRANSPORT_MAIN.load().is_none() {
        TRANSPORT_MAIN.store(Some(Arc::new(
            TransportMain::new(SessionBackend::default()),
        )));
    }
    Ok(())
}
