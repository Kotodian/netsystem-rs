// Re-export adapter types so the macros don't need fully-qualified paths and
// downstream call sites can `use hammer_runtime::adapter::Lifecycle` if they
// prefer the runtime crate's namespace.
pub mod adapter {
    pub use hammer_adapter::*;
}

pub use hammer_core::error::HammerError;

mod certificate;
mod connection;
mod dns;
mod endpoint;
mod http;
mod inbound;
mod macros;
mod network;
mod outbound;
pub mod pause;
mod router;
mod service_mgr;

pub use certificate::{CertificateProviderManager, CertificateStore};
pub use connection::ConnectionManager;
pub use dns::{DnsRouter, DnsTransportManager};
pub use endpoint::EndpointManager;
pub use http::HttpClientManager;
pub use inbound::InboundManager;
pub use network::NetworkManager;
pub use outbound::OutboundManager;
pub use pause::{PauseManager, StubManager};
pub use router::Router;
pub use service_mgr::ServiceManager;
