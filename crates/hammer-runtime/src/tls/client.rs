#[cfg(feature = "tls-outbound-stream")]
use std::io;
#[cfg(feature = "tls-outbound-stream")]
use std::pin::Pin;
use std::sync::Arc;
#[cfg(feature = "tls-outbound-stream")]
use std::task::{Context, Poll};

use hammer_adapter::PlatformInterface;
#[cfg(all(feature = "tls-outbound", feature = "tls-utls-stream"))]
use hammer_core::config::RealityOptions;
#[cfg(feature = "tls-outbound-stream")]
use hammer_core::config::TlsFragmentOptions;
#[cfg(feature = "tls-outbound")]
use hammer_core::config::{CertificateFingerprint, ClientTlsAuth, EchOptions, UtlsOptions};
#[cfg(any(
    all(
        feature = "tls-quic",
        feature = "outbound-hysteria2",
        not(feature = "tls-utls")
    ),
    all(feature = "tls-outbound-stream", not(feature = "tls-utls-stream"))
))]
use hammer_core::error::HammerError;
use hammer_core::error::HammerResult;
#[cfg(any(feature = "dns-https", feature = "outbound-urltest"))]
use rustls::ClientConfig;
#[cfg(feature = "tls-outbound-stream")]
use rustls::pki_types::ServerName;
#[cfg(feature = "tls-outbound-stream")]
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[cfg(feature = "tls-outbound-stream")]
use tokio::net::TcpStream;

#[cfg(feature = "tls-outbound-stream")]
use super::fragment::FragmentedTcpStream;

use super::backend::default_backend;
#[cfg(feature = "tls-utls-stream")]
use super::backend::utls_backend;

#[cfg(any(feature = "dns-https", feature = "outbound-urltest"))]
pub(crate) struct BasicClientTlsConfig {
    pub platform: Option<Arc<dyn PlatformInterface>>,
    pub alpn_protocols: Vec<Vec<u8>>,
}

#[cfg(feature = "dns-https")]
pub(crate) fn tls13_client_config(options: BasicClientTlsConfig) -> HammerResult<ClientConfig> {
    default_backend().tls13_client_config(options)
}

#[cfg(feature = "outbound-urltest")]
pub(crate) fn safe_default_client_config(
    options: BasicClientTlsConfig,
) -> HammerResult<ClientConfig> {
    default_backend().safe_default_client_config(options)
}

#[cfg(feature = "tls-outbound")]
pub(crate) struct OutboundClientTlsConfig {
    pub platform: Option<Arc<dyn PlatformInterface>>,
    pub insecure: bool,
    pub alpn_protocols: Vec<Vec<u8>>,
    pub server_fingerprints: Vec<CertificateFingerprint>,
    pub client_auth: Option<ClientTlsAuth>,
    pub ech: Option<EchOptions>,
    pub max_fragment_size: Option<usize>,
    #[cfg(feature = "tls-outbound-stream")]
    pub fragment: Option<TlsFragmentOptions>,
    #[cfg(all(feature = "tls-utls", feature = "outbound-hysteria2"))]
    pub ech_retry_configs: Option<Arc<std::sync::Mutex<Option<Vec<u8>>>>>,
    #[cfg(feature = "tls-utls-stream")]
    pub reality: Option<RealityOptions>,
    pub utls: Option<UtlsOptions>,
}

#[cfg(feature = "tls-outbound-stream")]
pub(crate) enum TlsClientStream {
    Rustls(tokio_rustls::client::TlsStream<FragmentedTcpStream>),
    #[cfg(feature = "tls-utls-stream")]
    Btls(super::btls_stream::BtlsClientStream),
}

#[cfg(feature = "tls-outbound-stream")]
impl AsyncRead for TlsClientStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Rustls(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls-utls-stream")]
            Self::Btls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

#[cfg(feature = "tls-outbound-stream")]
impl AsyncWrite for TlsClientStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Rustls(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls-utls-stream")]
            Self::Btls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Rustls(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls-utls-stream")]
            Self::Btls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Rustls(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls-utls-stream")]
            Self::Btls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

#[cfg(feature = "tls-outbound-stream")]
pub(crate) async fn outbound_client_stream(
    options: OutboundClientTlsConfig,
    server_name: ServerName<'static>,
    stream: TcpStream,
) -> HammerResult<TlsClientStream> {
    #[cfg(feature = "tls-utls-stream")]
    let use_utls_backend = options.utls.is_some() || options.reality.is_some();
    #[cfg(not(feature = "tls-utls-stream"))]
    let use_utls_backend = options.utls.is_some();

    if use_utls_backend {
        #[cfg(feature = "tls-utls-stream")]
        {
            return utls_backend()
                .outbound_client_stream(options, server_name, stream)
                .await;
        }

        #[cfg(not(feature = "tls-utls-stream"))]
        {
            let utls = options.utls.as_ref().expect("checked above");
            return Err(HammerError::config_validation(format!(
                "tls.utls fingerprint {} requires the hammer-runtime tls-utls feature",
                super::utls::fingerprint_name(utls.fingerprint),
            )));
        }
    }
    default_backend()
        .outbound_client_stream(options, server_name, stream)
        .await
}

#[cfg(all(feature = "tls-quic", feature = "outbound-hysteria2"))]
pub(crate) fn outbound_quic_client_config(
    options: OutboundClientTlsConfig,
) -> HammerResult<quinn::ClientConfig> {
    if options.utls.is_some() {
        #[cfg(feature = "tls-utls")]
        {
            return utls_backend().outbound_quic_client_config(options);
        }

        #[cfg(not(feature = "tls-utls"))]
        {
            let utls = options.utls.as_ref().expect("checked above");
            return Err(HammerError::config_validation(format!(
                "tls.utls fingerprint {} requires the hammer-runtime tls-utls feature",
                super::utls::fingerprint_name(utls.fingerprint),
            )));
        }
    }
    default_backend().outbound_quic_client_config(options)
}
