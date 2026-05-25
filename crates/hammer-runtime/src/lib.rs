// Re-export adapter types so the macros don't need fully-qualified paths and
// downstream call sites can `use hammer_runtime::adapter::Lifecycle` if they
// prefer the runtime crate's namespace.
pub mod adapter {
    pub use hammer_adapter::*;
}

pub use hammer_core::error::{HammerError, HammerResult};

/// Install the aws-lc-rs CryptoProvider as the rustls process-wide default.
///
/// Some rustls code paths may use the process-wide provider instead of an
/// explicit builder provider. Idempotent — subsequent calls are no-ops when a
/// provider is already installed.
#[cfg(feature = "tls")]
pub fn install_default_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[cfg(not(feature = "tls"))]
pub fn install_default_crypto_provider() {}

mod certificate;
mod component_registry;
mod connection;
mod control_thread;
pub mod dns;
pub mod endpoints;
mod event_subscribers;
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
    feature = "dns-https",
    feature = "outbound-urltest",
    feature = "tls-outbound",
    feature = "tls-quic"
))]
pub mod tls;

#[cfg(feature = "outbound-hysteria2")]
pub mod hysteria2 {
    pub use crate::protocol::hysteria2::*;
}

#[cfg(feature = "outbound-vless")]
pub mod vless {
    pub use crate::protocol::vless::*;
}

#[cfg(feature = "outbound-hysteria2")]
pub mod congestion {
    pub use crate::protocol::congestion::*;
}

#[cfg(feature = "inbound-tun")]
pub mod tun {
    pub use crate::protocol::tun::*;
}

#[cfg(feature = "endpoint-wireguard")]
pub mod wireguard {
    pub use crate::protocol::endpoint::wireguard::*;
}

#[cfg(all(
    feature = "inbound-tun",
    any(target_os = "macos", target_os = "ios", target_os = "tvos")
))]
mod apple_utun;

pub use certificate::{CertificateProviderManager, CertificateStore};
pub use component_registry::{
    EventSubscriberComponentDeclaration, register_event_subscriber_component,
};
pub use connection::{ConnectionManager, ConnectionRegistration};
pub(crate) use control_thread::ControlThread;
pub use control_thread::{
    ControlEvent, ControlEventArgs, ControlEventFilter, ControlEventSubscriptionHandle,
    ControlThreadHandle, ControlTimerHandle, LogEventArgs,
};
pub use dns::{DnsClient, DnsRouter, DnsTransportManager};
pub use endpoints::EndpointManager;
pub use event_subscribers::EventSubscriberBuilder;
pub use hammer_core::{
    MetricCounter, MetricGauge, MetricKind, MetricLabel, MetricSample, MetricsRegistry,
    MetricsScope,
};
pub use inbounds::InboundManager;
pub use network::NetworkManager;
pub use outbounds::OutboundManager;
pub use pause::PauseManager;
#[cfg(feature = "probe")]
pub use probe::{IcmpOutboundProbe, ProbeManager};
#[cfg(feature = "outbound-hysteria2")]
pub use protocol::hysteria2::{Hysteria2AuthFailureArgs, Hysteria2AuthSuccessArgs};
#[cfg(feature = "inbound-tun")]
pub use protocol::tun::TunInbound;
pub use route::Router;
pub use runtime_service::RuntimeService;
pub use service_mgr::ServiceManager;
pub use socket_protector::RuntimePlatform;
