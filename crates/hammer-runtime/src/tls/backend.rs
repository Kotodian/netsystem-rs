use async_trait::async_trait;
use rustls::ClientConfig;

#[cfg(feature = "tls-client")]
use super::client::BasicClientTlsConfig;
#[cfg(feature = "tls-outbound")]
use super::client::OutboundClientTlsConfig;
#[cfg(feature = "tls-outbound-stream")]
use super::client::TlsClientStream;
use hammer_core::error::HammerResult;
#[cfg(feature = "tls-outbound-stream")]
use rustls::pki_types::ServerName;
#[cfg(feature = "tls-outbound-stream")]
use tokio::net::TcpStream;

#[async_trait]
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

    #[cfg(feature = "tls-outbound-stream")]
    async fn outbound_client_stream(
        &self,
        options: OutboundClientTlsConfig,
        server_name: ServerName<'static>,
        stream: TcpStream,
    ) -> HammerResult<TlsClientStream>;

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
