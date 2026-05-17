mod backend;
#[cfg(feature = "tls-utls")]
mod btls_backend;
mod client;
#[cfg(feature = "tls-outbound")]
mod ech;
#[cfg(feature = "tls-outbound")]
mod material;
mod provider;
#[cfg(feature = "tls-utls")]
pub mod reality;
mod roots;
mod rustls_backend;
#[cfg(feature = "tls-outbound")]
mod utls;
#[cfg(feature = "tls-outbound")]
mod verifier;

#[cfg(any(feature = "dns-https", feature = "outbound-urltest"))]
pub(crate) use client::BasicClientTlsConfig;
#[cfg(feature = "tls-outbound")]
pub(crate) use client::OutboundClientTlsConfig;
#[cfg(feature = "tls-quic")]
pub(crate) use client::outbound_quic_client_config;
#[cfg(feature = "outbound-urltest")]
pub(crate) use client::safe_default_client_config;
#[cfg(feature = "dns-https")]
pub(crate) use client::tls13_client_config;
#[cfg(feature = "tls-outbound-stream")]
pub(crate) use client::{TlsClientStream, outbound_client_stream};
#[cfg(feature = "tls-quic")]
pub(crate) use ech::resolve_dns_https_ech_config_list;
