extern crate self as hammer_service;

pub mod app;
pub mod data_plane;
mod event_subscribers;
pub mod interface;
pub mod net;
#[cfg(feature = "probe")]
mod probe;
mod service;
pub mod session;
mod trace;
pub mod transport;
pub mod tun;

pub mod adapter {
    pub use hammer_runtime::adapter::*;
}

pub use hammer_core::error::{HammerError, HammerResult};
pub use hammer_runtime::OutboundManager;
pub use hammer_runtime::RuntimePlatform;
#[cfg(feature = "probe")]
pub use probe::{IcmpOutboundProbe, ProbeManager, ProbeProtocolFactorySet};
pub use service::RuntimeService;

mod socket_protector {
    pub(crate) use hammer_runtime::SocketProtector;
}
