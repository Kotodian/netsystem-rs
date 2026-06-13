extern crate self as hammer_service;

pub mod app;
mod component_registry;
pub mod data_plane;
pub mod dns;
mod event_subscribers;
pub mod interface;
pub mod net;
#[cfg(feature = "probe")]
mod probe;
pub mod route;
mod service;
pub mod session;
mod trace;
pub mod transport;
pub mod tun;

pub mod adapter {
    pub use hammer_runtime::adapter::*;
}

pub use dns::{DnsClient, DnsRouter, DnsTransportManager};
pub use hammer_core::error::{HammerError, HammerResult};
pub use hammer_runtime::OutboundManager;
pub use hammer_runtime::{RuntimePlatform, install_default_crypto_provider};
#[cfg(feature = "probe")]
pub use probe::{IcmpOutboundProbe, ProbeManager, ProbeProtocolFactorySet};
pub use route::Router;
pub use service::RuntimeService;

mod socket_protector {
    pub(crate) use hammer_runtime::SocketProtector;
}
