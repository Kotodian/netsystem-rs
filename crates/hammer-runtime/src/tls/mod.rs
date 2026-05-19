mod backend;
#[cfg(feature = "tls-utls-stream")]
mod btls_backend;
#[cfg(feature = "tls-utls-stream")]
mod btls_stream;
mod client;
#[cfg(feature = "tls-outbound")]
mod ech;
#[cfg(feature = "tls-outbound-stream")]
mod fragment;
#[cfg(feature = "tls-outbound")]
mod material;
mod provider;
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
#[cfg(all(feature = "tls-quic", feature = "outbound-hysteria2"))]
pub(crate) use client::outbound_quic_client_config;
#[cfg(feature = "outbound-urltest")]
pub(crate) use client::safe_default_client_config;
#[cfg(feature = "dns-https")]
pub(crate) use client::tls13_client_config;
#[cfg(feature = "tls-outbound-stream")]
pub(crate) use client::{TlsClientStream, outbound_client_stream};
#[cfg(all(feature = "tls-quic", feature = "outbound-hysteria2"))]
pub(crate) use ech::resolve_dns_https_ech_config_list;
