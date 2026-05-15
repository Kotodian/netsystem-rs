#[cfg(feature = "tls-client")]
use super::client::BasicClientTlsConfig;
#[cfg(feature = "tls-outbound")]
use super::client::OutboundClientTlsConfig;
#[cfg(feature = "tls-outbound")]
use super::ech::ech_config;
#[cfg(feature = "tls-outbound")]
use super::material::load_client_auth;
use super::provider::default_provider;
#[cfg(feature = "tls-client")]
use super::roots::client_verifier_builder;
#[cfg(feature = "tls-outbound")]
use super::verifier::outbound_client_verifier_builder;
use crate::tls::backend::TlsBackend;
use hammer_core::error::{HammerError, HammerResult};
use rustls::ClientConfig;

#[derive(Debug)]
pub(super) struct RustlsAwsLcBackend;

pub(super) static RUSTLS_AWS_LC_BACKEND: RustlsAwsLcBackend = RustlsAwsLcBackend;

impl TlsBackend for RustlsAwsLcBackend {
    #[cfg(feature = "dns-https")]
    fn tls13_client_config(&self, options: BasicClientTlsConfig) -> HammerResult<ClientConfig> {
        let builder = ClientConfig::builder_with_provider(default_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|err| HammerError::internal(format!("tls versions: {err}")))?;
        let mut config = client_verifier_builder(builder, options.platform)?.with_no_client_auth();
        config.alpn_protocols = options.alpn_protocols;
        Ok(config)
    }

    #[cfg(feature = "outbound-urltest")]
    fn safe_default_client_config(
        &self,
        options: BasicClientTlsConfig,
    ) -> HammerResult<ClientConfig> {
        let builder = ClientConfig::builder_with_provider(default_provider())
            .with_safe_default_protocol_versions()
            .map_err(|err| HammerError::internal(format!("tls versions: {err}")))?;
        let mut config = client_verifier_builder(builder, options.platform)?.with_no_client_auth();
        config.alpn_protocols = options.alpn_protocols;
        Ok(config)
    }

    #[cfg(feature = "tls-outbound")]
    fn outbound_client_config(
        &self,
        options: OutboundClientTlsConfig,
    ) -> HammerResult<ClientConfig> {
        let provider = default_provider();
        let builder = ClientConfig::builder_with_provider(provider.clone());
        let builder = if let Some(ech) = &options.ech {
            builder
                .with_ech(rustls::client::EchMode::Enable(ech_config(ech)?))
                .map_err(|err| HammerError::internal(format!("tls ECH: {err}")))?
        } else {
            builder
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|err| HammerError::internal(format!("tls versions: {err}")))?
        };
        let builder = outbound_client_verifier_builder(
            builder,
            options.platform,
            provider.clone(),
            options.insecure,
            &options.server_fingerprints,
        )?;
        let mut config = if let Some(auth) = options.client_auth {
            let (certificates, key) = load_client_auth(auth)?;
            builder
                .with_client_auth_cert(certificates, key)
                .map_err(|err| {
                    HammerError::config_validation(format!("tls client certificate: {err}"))
                })?
        } else {
            builder.with_no_client_auth()
        };
        config.alpn_protocols = options.alpn_protocols;
        Ok(config)
    }

    #[cfg(feature = "tls-quic")]
    fn outbound_quic_client_config(
        &self,
        options: OutboundClientTlsConfig,
    ) -> HammerResult<quinn::ClientConfig> {
        let crypto = self.outbound_client_config(options)?;
        Ok(quinn::ClientConfig::new(std::sync::Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
                .map_err(|err| HammerError::internal(format!("quic tls config: {err}")))?,
        )))
    }
}
