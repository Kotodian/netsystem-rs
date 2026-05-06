// Re-export adapter types so the macros don't need fully-qualified paths and
// downstream call sites can `use hammer_runtime::adapter::Lifecycle` if they
// prefer the runtime crate's namespace.
pub mod adapter {
    pub use hammer_adapter::*;
}

pub use hammer_core::error::HammerError;

/// Install the ring CryptoProvider as the rustls process-wide default.
///
/// Some rustls code paths may use the process-wide provider instead of an
/// explicit builder provider. Idempotent — subsequent calls are no-ops when a
/// provider is already installed.
#[cfg(any(
    feature = "outbound-hysteria2",
    feature = "outbound-urltest",
    feature = "dns-https"
))]
pub fn install_default_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(not(any(
    feature = "outbound-hysteria2",
    feature = "outbound-urltest",
    feature = "dns-https"
)))]
pub fn install_default_crypto_provider() {}

mod certificate;
mod connection;
pub mod dns;
#[cfg(feature = "endpoint")]
pub mod endpoints;
pub mod inbounds;
mod macros;
mod network;
pub mod outbounds;
pub mod pause;
#[cfg(feature = "probe")]
pub mod probe;
pub mod protocol;
pub mod route;
mod runtime_service;
mod service_mgr;
mod socket_protector;
pub mod spawn;
#[cfg(any(
    feature = "outbound-hysteria2",
    feature = "outbound-urltest",
    feature = "dns-https"
))]
mod tls_support;

#[cfg(feature = "outbound-hysteria2")]
pub mod hysteria2 {
    pub use crate::protocol::hysteria2::*;
}

#[cfg(feature = "inbound-tun")]
pub mod tun {
    pub use crate::protocol::tun::*;
}

#[cfg(feature = "wireguard")]
pub mod wireguard {
    pub use crate::protocol::wireguard::*;
}

#[cfg(all(
    feature = "inbound-tun",
    any(target_os = "macos", target_os = "ios", target_os = "tvos")
))]
mod apple_utun;

pub use certificate::{CertificateProviderManager, CertificateStore};
pub use connection::ConnectionManager;
pub use dns::{DnsClient, DnsRouter, DnsTransportManager};
#[cfg(feature = "endpoint")]
pub use endpoints::EndpointManager;
pub use inbounds::InboundManager;
pub use network::NetworkManager;
pub use outbounds::OutboundManager;
pub use pause::PauseManager;
#[cfg(feature = "probe")]
pub use probe::{IcmpOutboundProbe, ProbeManager};
#[cfg(feature = "inbound-tun")]
pub use protocol::tun::TunInbound;
pub use route::Router;
pub use runtime_service::RuntimeService;
pub use service_mgr::ServiceManager;
