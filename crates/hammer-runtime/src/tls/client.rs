use std::sync::Arc;

use hammer_adapter::PlatformInterface;
#[cfg(feature = "tls-outbound")]
use hammer_core::config::{CertificateFingerprint, ClientTlsAuth, EchOptions, UtlsOptions};
#[cfg(all(
    any(feature = "tls-quic", feature = "tls-outbound-stream"),
    not(feature = "tls-utls")
))]
use hammer_core::error::HammerError;
use hammer_core::error::HammerResult;
#[cfg(any(
    feature = "dns-https",
    feature = "outbound-urltest",
    feature = "tls-outbound-stream"
))]
use rustls::ClientConfig;

use super::backend::default_backend;
#[cfg(feature = "tls-utls")]
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
    #[cfg(feature = "tls-utls")]
    pub ech_retry_configs: Option<Arc<std::sync::Mutex<Option<Vec<u8>>>>>,
    pub utls: Option<UtlsOptions>,
}

#[cfg(feature = "tls-outbound-stream")]
pub(crate) fn outbound_client_config(
    options: OutboundClientTlsConfig,
) -> HammerResult<ClientConfig> {
    if options.utls.is_some() {
        #[cfg(feature = "tls-utls")]
        {
            return utls_backend().outbound_client_config(options);
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
    default_backend().outbound_client_config(options)
}

#[cfg(feature = "tls-quic")]
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
