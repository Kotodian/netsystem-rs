// Re-export adapter types so the macros don't need fully-qualified paths and
// downstream call sites can `use hammer_runtime::adapter::Lifecycle` if they
// prefer the runtime crate's namespace.
pub mod adapter {
    pub use hammer_adapter::*;
}

pub use hammer_core::error::HammerError;

/// Install the ring CryptoProvider as the rustls process-wide default.
///
/// reqwest is configured with `rustls-no-provider`, so any TLS work performed
/// through `reqwest::Client::new()` (e.g. DoH) needs a default provider in
/// place before the first connection. Idempotent — subsequent calls are no-ops
/// when a provider is already installed.
pub fn install_default_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

mod certificate;
mod connection;
pub mod dns;
mod endpoint;
mod http;
pub mod hysteria2;
mod inbound;
mod macros;
mod network;
mod outbound;
pub mod outbounds;
pub mod pause;
mod router;
mod service_mgr;
pub mod tun;
mod tun_inbound;

pub use certificate::{CertificateProviderManager, CertificateStore};
pub use connection::ConnectionManager;
pub use dns::{DnsClient, DnsRouter, DnsTransportManager};
pub use endpoint::EndpointManager;
pub use http::HttpClientManager;
pub use inbound::InboundManager;
pub use network::NetworkManager;
pub use outbound::OutboundManager;
pub use pause::{PauseManager, StubManager};
pub use router::Router;
pub use service_mgr::ServiceManager;
pub use tun_inbound::TunInbound;
