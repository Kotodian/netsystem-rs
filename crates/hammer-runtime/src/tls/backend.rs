use rustls::ClientConfig;

#[cfg(feature = "tls-client")]
use super::client::BasicClientTlsConfig;
#[cfg(feature = "tls-outbound")]
use super::client::OutboundClientTlsConfig;
use hammer_core::error::HammerResult;

pub(super) trait TlsBackend: Sync {
    #[cfg(feature = "dns-https")]
    fn tls13_client_config(&self, options: BasicClientTlsConfig) -> HammerResult<ClientConfig>;

    #[cfg(feature = "outbound-urltest")]
    fn safe_default_client_config(
        &self,
        options: BasicClientTlsConfig,
    ) -> HammerResult<ClientConfig>;

    #[cfg(feature = "tls-outbound")]
    fn outbound_client_config(
        &self,
        options: OutboundClientTlsConfig,
    ) -> HammerResult<ClientConfig>;

    #[cfg(feature = "tls-quic")]
    fn outbound_quic_client_config(
        &self,
        options: OutboundClientTlsConfig,
    ) -> HammerResult<quinn::ClientConfig>;
}

pub(super) fn default_backend() -> &'static dyn TlsBackend {
    &super::rustls_backend::RUSTLS_AWS_LC_BACKEND
}

#[cfg(feature = "tls-utls")]
pub(super) fn utls_backend() -> &'static dyn TlsBackend {
    &super::btls_backend::BTLS_UTLS_BACKEND
}
