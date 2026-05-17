#[cfg(feature = "tls-client")]
use super::client::BasicClientTlsConfig;
use super::client::OutboundClientTlsConfig;
#[cfg(feature = "tls-outbound-stream")]
use super::client::TlsClientStream;
#[cfg(feature = "tls-outbound")]
use super::material::load_client_auth;
use super::roots::platform_root_certificates;
use super::utls::{fingerprint_name, unsupported_for_rustls};
use crate::tls::backend::TlsBackend;
use async_trait::async_trait;
use btls::hash::MessageDigest;
use btls::pkey::PKey;
use btls::ssl::{
    CertificateCompressionAlgorithm, CertificateCompressor, KeyShare, Ssl, SslConnector,
    SslConnectorBuilder, SslMethod, SslVerifyMode,
};
use btls::x509::{X509, X509StoreContext, X509StoreContextRef};
use foreign_types_shared::ForeignType;
use hammer_adapter::PlatformInterface;
use hammer_core::config::{
    CertificateFingerprint, CertificateFingerprintAlgorithm, ClientTlsAuth, UtlsFingerprint,
    UtlsOptions,
};
use hammer_core::error::{HammerError, HammerResult};
use quinn_btls::QuicSslContext;
#[cfg(feature = "tls-outbound-stream")]
use rustls::pki_types::ServerName;
use std::ffi::CString;
use std::io::Cursor;
use std::sync::Arc;
#[cfg(feature = "tls-outbound-stream")]
use tokio::net::TcpStream;

#[derive(Debug)]
pub(super) struct BtlsUtlsBackend;

pub(super) static BTLS_UTLS_BACKEND: BtlsUtlsBackend = BtlsUtlsBackend;

#[async_trait]
impl TlsBackend for BtlsUtlsBackend {
    #[cfg(feature = "dns-https")]
    fn tls13_client_config(
        &self,
        options: BasicClientTlsConfig,
    ) -> HammerResult<rustls::ClientConfig> {
        super::rustls_backend::RUSTLS_AWS_LC_BACKEND.tls13_client_config(options)
    }

    #[cfg(feature = "outbound-urltest")]
    fn safe_default_client_config(
        &self,
        options: BasicClientTlsConfig,
    ) -> HammerResult<rustls::ClientConfig> {
        super::rustls_backend::RUSTLS_AWS_LC_BACKEND.safe_default_client_config(options)
    }

    #[cfg(feature = "tls-outbound")]
    fn outbound_client_config(
        &self,
        options: OutboundClientTlsConfig,
    ) -> HammerResult<rustls::ClientConfig> {
        if let Some(utls) = &options.utls {
            return Err(unsupported_for_rustls(utls));
        }
        super::rustls_backend::RUSTLS_AWS_LC_BACKEND.outbound_client_config(options)
    }

    #[cfg(feature = "tls-outbound-stream")]
    async fn outbound_client_stream(
        &self,
        options: OutboundClientTlsConfig,
        server_name: ServerName<'static>,
        stream: TcpStream,
    ) -> HammerResult<TlsClientStream> {
        let utls = options.utls.as_ref().ok_or_else(|| {
            HammerError::config_validation("tls.utls is required for the BoringSSL uTLS backend")
        })?;
        let ech_config_list = options
            .ech
            .as_ref()
            .map(btls_tcp_ech_config_list)
            .transpose()?;
        let connector = new_utls_tcp_connector(
            utls,
            options.insecure,
            &options.server_fingerprints,
            ech_config_list.is_some(),
            &options.alpn_protocols,
            options.platform,
            options.client_auth,
        )?;
        let mut config = connector
            .configure()
            .map_err(|err| HammerError::internal(format!("btls tls configure: {err}")))?;
        config.set_verify_hostname(!options.insecure);
        let server_name = server_name.to_str();
        let mut ssl = config
            .into_ssl(server_name.as_ref())
            .map_err(|err| HammerError::internal(format!("btls tls ssl: {err}")))?;
        let connection_profile =
            utls_connection_profile(utls, &options.alpn_protocols, ech_config_list);
        connection_profile
            .apply_to_ssl(&mut ssl)
            .map_err(|err| HammerError::config_validation(format!("tls.utls connection: {err}")))?;
        let stream = super::btls_stream::connect(ssl, stream).await?;
        Ok(TlsClientStream::Btls(stream))
    }

    #[cfg(feature = "tls-quic")]
    fn outbound_quic_client_config(
        &self,
        options: OutboundClientTlsConfig,
    ) -> HammerResult<quinn::ClientConfig> {
        let utls = options.utls.as_ref().ok_or_else(|| {
            HammerError::config_validation("tls.utls is required for the BoringSSL uTLS backend")
        })?;
        let ech_config_list = options.ech.as_ref().map(btls_ech_config_list).transpose()?;

        let mut crypto = new_utls_client_config(
            utls,
            options.insecure,
            &options.server_fingerprints,
            ech_config_list.is_some(),
        )?;
        if let Some(ech_retry_configs) = options.ech_retry_configs.clone() {
            crypto.set_ech_rejected_callback(move |retry_configs| {
                if let Ok(mut slot) = ech_retry_configs.lock() {
                    *slot = Some(retry_configs);
                }
            });
        }
        apply_utls_profile(&mut crypto, utls, &options.alpn_protocols, ech_config_list)?;
        add_platform_roots(&mut crypto, options.platform)?;
        configure_server_verification(&mut crypto, options.insecure, options.server_fingerprints)?;
        configure_client_auth(&mut crypto, options.client_auth)?;
        crypto
            .set_alpn(&options.alpn_protocols)
            .map_err(|err| HammerError::config_validation(format!("tls ALPN: {err}")))?;

        Ok(quinn::ClientConfig::new(Arc::new(crypto)))
    }
}

fn btls_ech_config_list(ech: &hammer_core::config::EchOptions) -> HammerResult<Vec<u8>> {
    if ech.pq_signature_schemes_enabled {
        return Err(HammerError::config_validation(
            "tls.ech.pq_signature_schemes_enabled is parsed but not supported by the BoringSSL uTLS backend",
        ));
    }
    if ech.dynamic_record_sizing_disabled {
        return Err(HammerError::config_validation(
            "tls.ech.dynamic_record_sizing_disabled is only valid for TCP TLS streams",
        ));
    }
    super::ech::ech_config_list_bytes(ech)
}

fn btls_tcp_ech_config_list(ech: &hammer_core::config::EchOptions) -> HammerResult<Vec<u8>> {
    if ech.pq_signature_schemes_enabled {
        return Err(HammerError::config_validation(
            "tls.ech.pq_signature_schemes_enabled is parsed but not supported by the BoringSSL uTLS backend",
        ));
    }
    super::ech::ech_config_list_bytes(ech)
}

fn new_utls_client_config(
    options: &UtlsOptions,
    insecure: bool,
    server_fingerprints: &[CertificateFingerprint],
    ech_enabled: bool,
) -> HammerResult<quinn_btls::ClientConfig> {
    let profile = UtlsProfile::for_fingerprint(options.fingerprint);
    let needs_cert_verify_callback = !server_fingerprints.is_empty() || (ech_enabled && !insecure);
    let result = if profile.certificate_compression || needs_cert_verify_callback {
        let server_fingerprints = server_fingerprints.to_vec();
        quinn_btls::ClientConfig::new_with_context_config(|builder| {
            if profile.certificate_compression {
                builder
                    .add_certificate_compression_algorithm(BrotliCertificateDecompressor)
                    .map_err(quinn_btls::Error::from)?;
            }
            if needs_cert_verify_callback {
                builder.set_cert_verify_callback(move |x509| {
                    verify_btls_server_certificate(x509, insecure, &server_fingerprints)
                });
            }
            Ok(())
        })
    } else {
        quinn_btls::ClientConfig::new()
    };
    result.map_err(|err| HammerError::internal(format!("btls quic client config: {err}")))
}

fn new_utls_tcp_connector(
    options: &UtlsOptions,
    insecure: bool,
    server_fingerprints: &[CertificateFingerprint],
    ech_enabled: bool,
    alpn_protocols: &[Vec<u8>],
    platform: Option<Arc<dyn PlatformInterface>>,
    client_auth: Option<ClientTlsAuth>,
) -> HammerResult<SslConnector> {
    let profile = UtlsProfile::for_fingerprint(options.fingerprint);
    let mut builder = SslConnector::builder(SslMethod::tls())
        .map_err(|err| HammerError::internal(format!("btls connector builder: {err}")))?;
    apply_utls_context_profile(builder.as_ptr(), &profile, options)?;
    if profile.certificate_compression {
        builder
            .add_certificate_compression_algorithm(BrotliCertificateDecompressor)
            .map_err(|err| {
                HammerError::config_validation(format!(
                    "tls.utls fingerprint {} certificate compression: {err}",
                    fingerprint_name(options.fingerprint),
                ))
            })?;
    }
    configure_tcp_server_verification(&mut builder, insecure, server_fingerprints, ech_enabled);
    add_platform_roots_to_builder(&mut builder, platform)?;
    configure_tcp_client_auth(&mut builder, client_auth)?;
    let alpn = encode_alpn_protocols(alpn_protocols)?;
    if !alpn.is_empty() {
        builder
            .set_alpn_protos(&alpn)
            .map_err(|err| HammerError::config_validation(format!("tls ALPN: {err}")))?;
    }
    Ok(builder.build())
}

fn verify_btls_server_certificate(
    x509: &mut X509StoreContextRef,
    insecure: bool,
    server_fingerprints: &[CertificateFingerprint],
) -> bool {
    if !insecure {
        if !apply_btls_ech_name_override(x509) {
            return false;
        }
        if !x509.verify_cert().unwrap_or(false) {
            return false;
        }
    }
    if server_fingerprints.is_empty() {
        return true;
    }
    verify_btls_server_fingerprint(x509, server_fingerprints)
}

fn apply_btls_ech_name_override(x509: &mut X509StoreContextRef) -> bool {
    let Ok(ssl_idx) = X509StoreContext::ssl_idx() else {
        return true;
    };
    let Some(name) = x509
        .ex_data(ssl_idx)
        .and_then(|ssl| ssl.get_ech_name_override().map(Vec::from))
    else {
        return true;
    };
    let Ok(name) = std::str::from_utf8(&name) else {
        return false;
    };
    x509.verify_param_mut().set_host(name).is_ok()
}

fn verify_btls_server_fingerprint(
    x509: &mut X509StoreContextRef,
    server_fingerprints: &[CertificateFingerprint],
) -> bool {
    let Some(certificate) = x509.cert() else {
        return false;
    };
    let Ok(digest) = certificate.digest(MessageDigest::sha256()) else {
        return false;
    };
    server_fingerprints
        .iter()
        .any(|fingerprint| match fingerprint.algorithm {
            CertificateFingerprintAlgorithm::Sha256 => {
                digest.as_ref() == fingerprint.digest.as_slice()
            }
        })
}

fn apply_utls_profile(
    crypto: &mut quinn_btls::ClientConfig,
    options: &UtlsOptions,
    alpn_protocols: &[Vec<u8>],
    ech_config_list: Option<Vec<u8>>,
) -> HammerResult<()> {
    let profile = UtlsProfile::for_fingerprint(options.fingerprint);
    apply_utls_context_profile(crypto.ctx_mut().as_ptr(), &profile, options)?;
    let connection_profile = utls_connection_profile(options, alpn_protocols, ech_config_list);
    crypto.set_ssl_config_callback(move |ssl| connection_profile.apply(ssl));
    Ok(())
}

fn apply_utls_context_profile(
    ctx: *mut btls_sys::SSL_CTX,
    profile: &UtlsProfile,
    options: &UtlsOptions,
) -> HammerResult<()> {
    unsafe {
        cvt_btls(
            btls_sys::SSL_CTX_set_min_proto_version(ctx, btls_sys::TLS1_3_VERSION as u16),
            options,
            "min TLS version",
        )?;
        cvt_btls(
            btls_sys::SSL_CTX_set_max_proto_version(ctx, btls_sys::TLS1_3_VERSION as u16),
            options,
            "max TLS version",
        )?;
        btls_sys::SSL_CTX_set_preserve_tls13_cipher_list(ctx, 1);
        set_ctx_string(
            ctx,
            profile.cipher_list,
            options,
            "cipher suites",
            |ctx, value| btls_sys::SSL_CTX_set_cipher_list(ctx, value),
        )?;
        btls_sys::SSL_CTX_set_grease_enabled(ctx, i32::from(profile.grease));
        btls_sys::SSL_CTX_set_permute_extensions(ctx, i32::from(profile.permute_extensions));
        if let Some(extension_order) = profile.extension_order {
            cvt_btls(
                btls_sys::SSL_CTX_set_extension_order(
                    ctx,
                    extension_order.as_ptr(),
                    extension_order.len() as std::ffi::c_int,
                ),
                options,
                "extension order",
            )?;
        }
        set_ctx_string(ctx, profile.curves, options, "curves", |ctx, value| {
            btls_sys::SSL_CTX_set1_curves_list(ctx, value)
        })?;
        set_ctx_string(
            ctx,
            profile.signature_algorithms,
            options,
            "signature algorithms",
            |ctx, value| btls_sys::SSL_CTX_set1_sigalgs_list(ctx, value),
        )?;
        if let Some(limit) = profile.record_size_limit {
            btls_sys::SSL_CTX_set_record_size_limit(ctx, limit);
        }
        if profile.signed_certificate_timestamps {
            btls_sys::SSL_CTX_enable_signed_cert_timestamps(ctx);
        }
        if profile.ocsp_stapling {
            btls_sys::SSL_CTX_enable_ocsp_stapling(ctx);
        }
    }
    Ok(())
}

fn utls_connection_profile(
    options: &UtlsOptions,
    alpn_protocols: &[Vec<u8>],
    ech_config_list: Option<Vec<u8>>,
) -> UtlsConnectionProfile {
    let profile = UtlsProfile::for_fingerprint(options.fingerprint);
    UtlsConnectionProfile {
        key_shares: profile.key_shares,
        ech_grease: profile.ech_grease,
        alps: profile.alps,
        alps_new_codepoint: profile.alps_new_codepoint,
        alpn_protocols: alpn_protocols.to_vec(),
        ech_config_list,
    }
}

struct UtlsConnectionProfile {
    key_shares: &'static [KeyShare],
    ech_grease: bool,
    alps: bool,
    alps_new_codepoint: bool,
    alpn_protocols: Vec<Vec<u8>>,
    ech_config_list: Option<Vec<u8>>,
}

impl UtlsConnectionProfile {
    fn apply(&self, ssl: &mut Ssl) -> quinn_btls::Result<()> {
        self.apply_to_ssl(ssl).map_err(quinn_btls::Error::from)
    }

    fn apply_to_ssl(&self, ssl: &mut Ssl) -> Result<(), btls::error::ErrorStack> {
        if !self.key_shares.is_empty() {
            ssl.set_client_key_shares(self.key_shares)?;
        }
        if let Some(ech_config_list) = &self.ech_config_list {
            ssl.set_ech_config_list(ech_config_list)?;
        } else if self.ech_grease {
            ssl.set_enable_ech_grease(true);
        }
        if self.alps {
            ssl.set_alps_use_new_codepoint(self.alps_new_codepoint);
            for protocol in self
                .alpn_protocols
                .iter()
                .filter(|protocol| !protocol.is_empty())
            {
                ssl.add_application_settings(protocol)?;
            }
        }
        Ok(())
    }
}

struct BrotliCertificateDecompressor;

impl CertificateCompressor for BrotliCertificateDecompressor {
    const ALGORITHM: CertificateCompressionAlgorithm = CertificateCompressionAlgorithm::BROTLI;
    const CAN_COMPRESS: bool = false;
    const CAN_DECOMPRESS: bool = true;

    fn decompress<W>(&self, input: &[u8], output: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        brotli::BrotliDecompress(&mut Cursor::new(input), output)?;
        Ok(())
    }
}

unsafe fn set_ctx_string(
    ctx: *mut btls_sys::SSL_CTX,
    value: &str,
    options: &UtlsOptions,
    setting: &str,
    setter: unsafe fn(*mut btls_sys::SSL_CTX, *const std::ffi::c_char) -> std::ffi::c_int,
) -> HammerResult<()> {
    let value = CString::new(value).map_err(|err| {
        HammerError::config_validation(format!(
            "tls.utls fingerprint {} {setting}: {err}",
            fingerprint_name(options.fingerprint),
        ))
    })?;
    cvt_btls(unsafe { setter(ctx, value.as_ptr()) }, options, setting)
}

fn cvt_btls(result: i32, options: &UtlsOptions, setting: &str) -> HammerResult<()> {
    if result == 1 {
        Ok(())
    } else {
        Err(HammerError::config_validation(format!(
            "tls.utls fingerprint {} {setting}: {}",
            fingerprint_name(options.fingerprint),
            btls::error::ErrorStack::get(),
        )))
    }
}

fn add_platform_roots(
    crypto: &mut quinn_btls::ClientConfig,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> HammerResult<()> {
    for certificate in platform_root_certificates(platform) {
        let cert = X509::from_der(certificate.as_ref()).map_err(|err| {
            HammerError::config_validation(format!("tls root certificate: {err}"))
        })?;
        let _ = crypto.ctx_mut().cert_store_mut().add_cert(&cert);
    }
    Ok(())
}

fn add_platform_roots_to_builder(
    builder: &mut SslConnectorBuilder,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> HammerResult<()> {
    for certificate in platform_root_certificates(platform) {
        let cert = X509::from_der(certificate.as_ref()).map_err(|err| {
            HammerError::config_validation(format!("tls root certificate: {err}"))
        })?;
        let _ = builder.cert_store_mut().add_cert(&cert);
    }
    Ok(())
}

fn configure_server_verification(
    crypto: &mut quinn_btls::ClientConfig,
    insecure: bool,
    server_fingerprints: Vec<CertificateFingerprint>,
) -> HammerResult<()> {
    crypto.verify_peer(!insecure || !server_fingerprints.is_empty());
    Ok(())
}

fn configure_tcp_server_verification(
    builder: &mut SslConnectorBuilder,
    insecure: bool,
    server_fingerprints: &[CertificateFingerprint],
    ech_enabled: bool,
) {
    let verify_peer = !insecure || !server_fingerprints.is_empty();
    if !server_fingerprints.is_empty() || (ech_enabled && !insecure) {
        let server_fingerprints = server_fingerprints.to_vec();
        builder.set_cert_verify_callback(move |x509| {
            verify_btls_server_certificate(x509, insecure, &server_fingerprints)
        });
    }
    builder.set_verify(if verify_peer {
        SslVerifyMode::PEER
    } else {
        SslVerifyMode::NONE
    });
}

fn configure_client_auth(
    crypto: &mut quinn_btls::ClientConfig,
    auth: Option<hammer_core::config::ClientTlsAuth>,
) -> HammerResult<()> {
    let Some(auth) = auth else {
        return Ok(());
    };
    let (certificates, key) = load_client_auth(auth)?;
    let mut certificates = certificates.into_iter();
    let first = certificates.next().ok_or_else(|| {
        HammerError::config_validation("tls client certificate chain must not be empty")
    })?;
    let first = X509::from_der(first.as_ref()).map_err(|err| {
        HammerError::config_validation(format!("parse tls client certificate: {err}"))
    })?;
    crypto
        .ctx_mut()
        .set_certificate(first)
        .map_err(|err| HammerError::config_validation(format!("tls client certificate: {err}")))?;
    for certificate in certificates {
        let certificate = X509::from_der(certificate.as_ref()).map_err(|err| {
            HammerError::config_validation(format!("parse tls client certificate chain: {err}"))
        })?;
        crypto
            .ctx_mut()
            .add_to_cert_chain(certificate)
            .map_err(|err| {
                HammerError::config_validation(format!("tls client certificate chain: {err}"))
            })?;
    }
    let key = PKey::private_key_from_der(key.secret_der())
        .or_else(|_| PKey::private_key_from_pkcs8(key.secret_der()))
        .map_err(|err| HammerError::config_validation(format!("parse tls client key: {err}")))?;
    crypto
        .ctx_mut()
        .set_private_key(key)
        .map_err(|err| HammerError::config_validation(format!("tls client key: {err}")))?;
    crypto
        .ctx()
        .check_private_key()
        .map_err(|err| HammerError::config_validation(format!("tls client key: {err}")))?;
    Ok(())
}

fn configure_tcp_client_auth(
    builder: &mut SslConnectorBuilder,
    auth: Option<ClientTlsAuth>,
) -> HammerResult<()> {
    let Some(auth) = auth else {
        return Ok(());
    };
    let (certificates, key) = load_client_auth(auth)?;
    let mut certificates = certificates.into_iter();
    let first = certificates.next().ok_or_else(|| {
        HammerError::config_validation("tls client certificate chain must not be empty")
    })?;
    let first = X509::from_der(first.as_ref()).map_err(|err| {
        HammerError::config_validation(format!("parse tls client certificate: {err}"))
    })?;
    builder
        .set_certificate(&first)
        .map_err(|err| HammerError::config_validation(format!("tls client certificate: {err}")))?;
    for certificate in certificates {
        let certificate = X509::from_der(certificate.as_ref()).map_err(|err| {
            HammerError::config_validation(format!("parse tls client certificate chain: {err}"))
        })?;
        builder.add_extra_chain_cert(certificate).map_err(|err| {
            HammerError::config_validation(format!("tls client certificate chain: {err}"))
        })?;
    }
    let key = PKey::private_key_from_der(key.secret_der())
        .or_else(|_| PKey::private_key_from_pkcs8(key.secret_der()))
        .map_err(|err| HammerError::config_validation(format!("parse tls client key: {err}")))?;
    builder
        .set_private_key(&key)
        .map_err(|err| HammerError::config_validation(format!("tls client key: {err}")))?;
    builder
        .check_private_key()
        .map_err(|err| HammerError::config_validation(format!("tls client key: {err}")))?;
    Ok(())
}

fn encode_alpn_protocols(protocols: &[Vec<u8>]) -> HammerResult<Vec<u8>> {
    let mut encoded = Vec::new();
    for protocol in protocols.iter().filter(|protocol| !protocol.is_empty()) {
        let len = u8::try_from(protocol.len()).map_err(|_| {
            HammerError::config_validation("tls ALPN protocol name must be at most 255 bytes")
        })?;
        encoded.push(len);
        encoded.extend_from_slice(protocol);
    }
    Ok(encoded)
}

struct UtlsProfile {
    cipher_list: &'static str,
    curves: &'static str,
    signature_algorithms: &'static str,
    key_shares: &'static [KeyShare],
    grease: bool,
    permute_extensions: bool,
    extension_order: Option<&'static [u16]>,
    signed_certificate_timestamps: bool,
    ocsp_stapling: bool,
    record_size_limit: Option<u16>,
    ech_grease: bool,
    alps: bool,
    alps_new_codepoint: bool,
    certificate_compression: bool,
}

impl UtlsProfile {
    fn for_fingerprint(fingerprint: UtlsFingerprint) -> Self {
        match fingerprint {
            UtlsFingerprint::Firefox => Self::firefox(),
            UtlsFingerprint::Edge => Self::edge(),
            UtlsFingerprint::Safari => Self::safari(),
            UtlsFingerprint::ThreeSixty => Self::three_sixty(),
            UtlsFingerprint::Qq => Self::qq(),
            UtlsFingerprint::Ios => Self::ios(),
            UtlsFingerprint::Android => Self::android(),
            UtlsFingerprint::Random | UtlsFingerprint::Randomized => Self {
                permute_extensions: true,
                extension_order: None,
                ..Self::chrome()
            },
            UtlsFingerprint::Chrome => Self::chrome(),
        }
    }

    fn chrome() -> Self {
        Self {
            cipher_list: CHROME_CIPHERS,
            curves: "X25519MLKEM768:X25519:P-256:P-384",
            signature_algorithms: CHROMIUM_SIGALGS,
            key_shares: CHROME_KEY_SHARES,
            grease: true,
            permute_extensions: true,
            extension_order: None,
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: None,
            ech_grease: true,
            alps: true,
            alps_new_codepoint: true,
            certificate_compression: true,
        }
    }

    fn firefox() -> Self {
        Self {
            cipher_list: FIREFOX_CIPHERS,
            curves: "X25519MLKEM768:X25519:P-256:P-384:P-521:ffdhe2048:ffdhe3072",
            signature_algorithms: FIREFOX_SIGALGS,
            key_shares: CHROME_KEY_SHARES,
            grease: false,
            permute_extensions: false,
            extension_order: Some(FIREFOX_EXTENSION_ORDER),
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: Some(0x4001),
            ech_grease: true,
            alps: false,
            alps_new_codepoint: false,
            certificate_compression: true,
        }
    }

    fn edge() -> Self {
        Self {
            cipher_list: CHROME_CIPHERS,
            curves: "X25519:P-256:P-384",
            signature_algorithms: CHROMIUM_SIGALGS,
            key_shares: CLASSIC_KEY_SHARES,
            grease: true,
            permute_extensions: false,
            extension_order: Some(CHROMIUM_LEGACY_EXTENSION_ORDER),
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: None,
            ech_grease: true,
            alps: true,
            alps_new_codepoint: true,
            certificate_compression: true,
        }
    }

    fn safari() -> Self {
        Self {
            cipher_list: SAFARI_CIPHERS,
            curves: "X25519MLKEM768:X25519:P-256:P-384:P-521",
            signature_algorithms: SAFARI_SIGALGS,
            key_shares: CHROME_KEY_SHARES,
            grease: true,
            permute_extensions: false,
            extension_order: Some(SAFARI_EXTENSION_ORDER),
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: None,
            ech_grease: true,
            alps: false,
            alps_new_codepoint: false,
            certificate_compression: true,
        }
    }

    fn three_sixty() -> Self {
        Self {
            cipher_list: CHROME_CIPHERS,
            curves: "X25519:P-256:P-384",
            signature_algorithms: CHROMIUM_LEGACY_SIGALGS,
            key_shares: CLASSIC_KEY_SHARES,
            grease: true,
            permute_extensions: false,
            extension_order: Some(CHROMIUM_LEGACY_EXTENSION_ORDER),
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: None,
            ech_grease: true,
            alps: true,
            alps_new_codepoint: true,
            certificate_compression: true,
        }
    }

    fn qq() -> Self {
        Self {
            cipher_list: CHROME_CIPHERS,
            curves: "X25519:P-256:P-384",
            signature_algorithms: CHROMIUM_SIGALGS,
            key_shares: CLASSIC_KEY_SHARES,
            grease: true,
            permute_extensions: false,
            extension_order: Some(QQ_EXTENSION_ORDER),
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: None,
            ech_grease: true,
            alps: true,
            alps_new_codepoint: true,
            certificate_compression: true,
        }
    }

    fn ios() -> Self {
        Self {
            cipher_list: IOS_CIPHERS,
            curves: "X25519:P-256:P-384:P-521",
            signature_algorithms: IOS_SIGALGS,
            key_shares: CLASSIC_KEY_SHARES,
            grease: true,
            permute_extensions: false,
            extension_order: Some(IOS_EXTENSION_ORDER),
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: None,
            ech_grease: true,
            alps: false,
            alps_new_codepoint: false,
            certificate_compression: false,
        }
    }

    fn android() -> Self {
        Self {
            cipher_list: CHROME_CIPHERS,
            curves: "X25519:P-256:P-384",
            signature_algorithms: CHROMIUM_LEGACY_SIGALGS,
            key_shares: CLASSIC_KEY_SHARES,
            grease: false,
            permute_extensions: false,
            extension_order: Some(ANDROID_EXTENSION_ORDER),
            signed_certificate_timestamps: false,
            ocsp_stapling: true,
            record_size_limit: None,
            ech_grease: false,
            alps: false,
            alps_new_codepoint: false,
            certificate_compression: false,
        }
    }
}

const CHROME_KEY_SHARES: &[KeyShare] = &[KeyShare::X25519_MLKEM768, KeyShare::X25519];
const CLASSIC_KEY_SHARES: &[KeyShare] = &[KeyShare::X25519];

const CHROME_CIPHERS: &str = concat!(
    "TLS_AES_128_GCM_SHA256:",
    "TLS_AES_256_GCM_SHA384:",
    "TLS_CHACHA20_POLY1305_SHA256:",
    "ECDHE-ECDSA-AES128-GCM-SHA256:",
    "ECDHE-RSA-AES128-GCM-SHA256:",
    "ECDHE-ECDSA-AES256-GCM-SHA384:",
    "ECDHE-RSA-AES256-GCM-SHA384:",
    "ECDHE-ECDSA-CHACHA20-POLY1305:",
    "ECDHE-RSA-CHACHA20-POLY1305:",
    "ECDHE-RSA-AES128-SHA:",
    "ECDHE-RSA-AES256-SHA:",
    "AES128-GCM-SHA256:",
    "AES256-GCM-SHA384:",
    "AES128-SHA:",
    "AES256-SHA"
);
const FIREFOX_CIPHERS: &str = concat!(
    "TLS_AES_128_GCM_SHA256:",
    "TLS_CHACHA20_POLY1305_SHA256:",
    "TLS_AES_256_GCM_SHA384:",
    "ECDHE-ECDSA-AES128-GCM-SHA256:",
    "ECDHE-RSA-AES128-GCM-SHA256:",
    "ECDHE-ECDSA-CHACHA20-POLY1305:",
    "ECDHE-RSA-CHACHA20-POLY1305:",
    "ECDHE-ECDSA-AES256-GCM-SHA384:",
    "ECDHE-RSA-AES256-GCM-SHA384:",
    "ECDHE-ECDSA-AES256-SHA:",
    "ECDHE-ECDSA-AES128-SHA:",
    "ECDHE-RSA-AES128-SHA:",
    "ECDHE-RSA-AES256-SHA:",
    "AES128-GCM-SHA256:",
    "AES256-GCM-SHA384:",
    "AES128-SHA:",
    "AES256-SHA"
);
const SAFARI_CIPHERS: &str = concat!(
    "TLS_AES_256_GCM_SHA384:",
    "TLS_CHACHA20_POLY1305_SHA256:",
    "TLS_AES_128_GCM_SHA256:",
    "ECDHE-ECDSA-AES256-GCM-SHA384:",
    "ECDHE-ECDSA-AES128-GCM-SHA256:",
    "ECDHE-ECDSA-CHACHA20-POLY1305:",
    "ECDHE-RSA-AES256-GCM-SHA384:",
    "ECDHE-RSA-AES128-GCM-SHA256:",
    "ECDHE-RSA-CHACHA20-POLY1305:",
    "ECDHE-ECDSA-AES256-SHA:",
    "ECDHE-ECDSA-AES128-SHA:",
    "ECDHE-RSA-AES256-SHA:",
    "ECDHE-RSA-AES128-SHA:",
    "AES256-GCM-SHA384:",
    "AES128-GCM-SHA256:",
    "AES256-SHA:",
    "AES128-SHA"
);
const IOS_CIPHERS: &str = concat!(
    "TLS_AES_128_GCM_SHA256:",
    "TLS_AES_256_GCM_SHA384:",
    "TLS_CHACHA20_POLY1305_SHA256:",
    "ECDHE-ECDSA-AES256-GCM-SHA384:",
    "ECDHE-ECDSA-AES128-GCM-SHA256:",
    "ECDHE-ECDSA-CHACHA20-POLY1305:",
    "ECDHE-RSA-AES256-GCM-SHA384:",
    "ECDHE-RSA-AES128-GCM-SHA256:",
    "ECDHE-RSA-CHACHA20-POLY1305:",
    "ECDHE-ECDSA-AES256-SHA384:",
    "ECDHE-ECDSA-AES128-SHA256:",
    "ECDHE-ECDSA-AES256-SHA:",
    "ECDHE-ECDSA-AES128-SHA:",
    "ECDHE-RSA-AES256-SHA384:",
    "ECDHE-RSA-AES128-SHA256:",
    "ECDHE-RSA-AES256-SHA:",
    "ECDHE-RSA-AES128-SHA:",
    "AES256-GCM-SHA384:",
    "AES128-GCM-SHA256:",
    "AES256-SHA256:",
    "AES128-SHA256:",
    "AES256-SHA:",
    "AES128-SHA"
);

const CHROMIUM_SIGALGS: &str = concat!(
    "ecdsa_secp256r1_sha256:",
    "rsa_pss_rsae_sha256:",
    "rsa_pkcs1_sha256:",
    "ecdsa_secp384r1_sha384:",
    "rsa_pss_rsae_sha384:",
    "rsa_pkcs1_sha384:",
    "rsa_pss_rsae_sha512:",
    "rsa_pkcs1_sha512"
);
const CHROMIUM_LEGACY_SIGALGS: &str = concat!(
    "ecdsa_secp256r1_sha256:",
    "rsa_pss_rsae_sha256:",
    "rsa_pkcs1_sha256:",
    "ecdsa_secp384r1_sha384:",
    "rsa_pss_rsae_sha384:",
    "rsa_pkcs1_sha384:",
    "rsa_pss_rsae_sha512:",
    "rsa_pkcs1_sha512:",
    "rsa_pkcs1_sha1"
);
const FIREFOX_SIGALGS: &str = concat!(
    "ecdsa_secp256r1_sha256:",
    "ecdsa_secp384r1_sha384:",
    "ecdsa_secp521r1_sha512:",
    "rsa_pss_rsae_sha256:",
    "rsa_pss_rsae_sha384:",
    "rsa_pss_rsae_sha512:",
    "rsa_pkcs1_sha256:",
    "rsa_pkcs1_sha384:",
    "rsa_pkcs1_sha512:",
    "ecdsa_sha1:",
    "rsa_pkcs1_sha1"
);
const SAFARI_SIGALGS: &str = concat!(
    "ecdsa_secp256r1_sha256:",
    "rsa_pss_rsae_sha256:",
    "rsa_pkcs1_sha256:",
    "ecdsa_secp384r1_sha384:",
    "rsa_pss_rsae_sha384:",
    "rsa_pss_rsae_sha384:",
    "rsa_pkcs1_sha384:",
    "rsa_pss_rsae_sha512:",
    "rsa_pkcs1_sha512:",
    "rsa_pkcs1_sha1"
);
const IOS_SIGALGS: &str = concat!(
    "ecdsa_secp256r1_sha256:",
    "rsa_pss_rsae_sha256:",
    "rsa_pkcs1_sha256:",
    "ecdsa_secp384r1_sha384:",
    "ecdsa_sha1:",
    "rsa_pss_rsae_sha384:",
    "rsa_pss_rsae_sha384:",
    "rsa_pkcs1_sha384:",
    "rsa_pss_rsae_sha512:",
    "rsa_pkcs1_sha512:",
    "rsa_pkcs1_sha1"
);

const CHROMIUM_LEGACY_EXTENSION_ORDER: &[u16] = &[
    btls_sys::TLSEXT_TYPE_server_name as u16,
    btls_sys::TLSEXT_TYPE_extended_master_secret as u16,
    btls_sys::TLSEXT_TYPE_renegotiate as u16,
    btls_sys::TLSEXT_TYPE_supported_groups as u16,
    btls_sys::TLSEXT_TYPE_ec_point_formats as u16,
    btls_sys::TLSEXT_TYPE_session_ticket as u16,
    btls_sys::TLSEXT_TYPE_application_layer_protocol_negotiation as u16,
    btls_sys::TLSEXT_TYPE_status_request as u16,
    btls_sys::TLSEXT_TYPE_signature_algorithms as u16,
    btls_sys::TLSEXT_TYPE_certificate_timestamp as u16,
    btls_sys::TLSEXT_TYPE_key_share as u16,
    btls_sys::TLSEXT_TYPE_psk_key_exchange_modes as u16,
    btls_sys::TLSEXT_TYPE_supported_versions as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters as u16,
    btls_sys::TLSEXT_TYPE_cert_compression as u16,
    btls_sys::TLSEXT_TYPE_application_settings as u16,
    btls_sys::TLSEXT_TYPE_encrypted_client_hello as u16,
    btls_sys::TLSEXT_TYPE_early_data as u16,
    btls_sys::TLSEXT_TYPE_cookie as u16,
    btls_sys::TLSEXT_TYPE_delegated_credential as u16,
    btls_sys::TLSEXT_TYPE_application_settings_old as u16,
    btls_sys::TLSEXT_TYPE_certificate_authorities as u16,
    btls_sys::TLSEXT_TYPE_pake as u16,
    btls_sys::TLSEXT_TYPE_trust_anchors as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters_legacy as u16,
];
const QQ_EXTENSION_ORDER: &[u16] = &[
    btls_sys::TLSEXT_TYPE_server_name as u16,
    btls_sys::TLSEXT_TYPE_extended_master_secret as u16,
    btls_sys::TLSEXT_TYPE_renegotiate as u16,
    btls_sys::TLSEXT_TYPE_supported_groups as u16,
    btls_sys::TLSEXT_TYPE_ec_point_formats as u16,
    btls_sys::TLSEXT_TYPE_session_ticket as u16,
    btls_sys::TLSEXT_TYPE_application_layer_protocol_negotiation as u16,
    btls_sys::TLSEXT_TYPE_status_request as u16,
    btls_sys::TLSEXT_TYPE_signature_algorithms as u16,
    btls_sys::TLSEXT_TYPE_certificate_timestamp as u16,
    btls_sys::TLSEXT_TYPE_key_share as u16,
    btls_sys::TLSEXT_TYPE_psk_key_exchange_modes as u16,
    btls_sys::TLSEXT_TYPE_supported_versions as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters as u16,
    btls_sys::TLSEXT_TYPE_cert_compression as u16,
    btls_sys::TLSEXT_TYPE_application_settings as u16,
    btls_sys::TLSEXT_TYPE_early_data as u16,
    btls_sys::TLSEXT_TYPE_cookie as u16,
    btls_sys::TLSEXT_TYPE_delegated_credential as u16,
    btls_sys::TLSEXT_TYPE_application_settings_old as u16,
    btls_sys::TLSEXT_TYPE_certificate_authorities as u16,
    btls_sys::TLSEXT_TYPE_pake as u16,
    btls_sys::TLSEXT_TYPE_trust_anchors as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters_legacy as u16,
];
const FIREFOX_EXTENSION_ORDER: &[u16] = &[
    btls_sys::TLSEXT_TYPE_server_name as u16,
    btls_sys::TLSEXT_TYPE_extended_master_secret as u16,
    btls_sys::TLSEXT_TYPE_renegotiate as u16,
    btls_sys::TLSEXT_TYPE_supported_groups as u16,
    btls_sys::TLSEXT_TYPE_ec_point_formats as u16,
    btls_sys::TLSEXT_TYPE_application_layer_protocol_negotiation as u16,
    btls_sys::TLSEXT_TYPE_status_request as u16,
    btls_sys::TLSEXT_TYPE_delegated_credential as u16,
    btls_sys::TLSEXT_TYPE_certificate_timestamp as u16,
    btls_sys::TLSEXT_TYPE_key_share as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters as u16,
    btls_sys::TLSEXT_TYPE_supported_versions as u16,
    btls_sys::TLSEXT_TYPE_signature_algorithms as u16,
    btls_sys::TLSEXT_TYPE_psk_key_exchange_modes as u16,
    btls_sys::TLSEXT_TYPE_record_size_limit as u16,
    btls_sys::TLSEXT_TYPE_cert_compression as u16,
    btls_sys::TLSEXT_TYPE_encrypted_client_hello as u16,
    btls_sys::TLSEXT_TYPE_session_ticket as u16,
    btls_sys::TLSEXT_TYPE_early_data as u16,
    btls_sys::TLSEXT_TYPE_cookie as u16,
    btls_sys::TLSEXT_TYPE_application_settings as u16,
    btls_sys::TLSEXT_TYPE_application_settings_old as u16,
    btls_sys::TLSEXT_TYPE_certificate_authorities as u16,
    btls_sys::TLSEXT_TYPE_pake as u16,
    btls_sys::TLSEXT_TYPE_trust_anchors as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters_legacy as u16,
];
const SAFARI_EXTENSION_ORDER: &[u16] = &[
    btls_sys::TLSEXT_TYPE_server_name as u16,
    btls_sys::TLSEXT_TYPE_extended_master_secret as u16,
    btls_sys::TLSEXT_TYPE_renegotiate as u16,
    btls_sys::TLSEXT_TYPE_supported_groups as u16,
    btls_sys::TLSEXT_TYPE_ec_point_formats as u16,
    btls_sys::TLSEXT_TYPE_application_layer_protocol_negotiation as u16,
    btls_sys::TLSEXT_TYPE_status_request as u16,
    btls_sys::TLSEXT_TYPE_signature_algorithms as u16,
    btls_sys::TLSEXT_TYPE_certificate_timestamp as u16,
    btls_sys::TLSEXT_TYPE_key_share as u16,
    btls_sys::TLSEXT_TYPE_psk_key_exchange_modes as u16,
    btls_sys::TLSEXT_TYPE_supported_versions as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters as u16,
    btls_sys::TLSEXT_TYPE_cert_compression as u16,
    btls_sys::TLSEXT_TYPE_early_data as u16,
    btls_sys::TLSEXT_TYPE_cookie as u16,
    btls_sys::TLSEXT_TYPE_delegated_credential as u16,
    btls_sys::TLSEXT_TYPE_application_settings as u16,
    btls_sys::TLSEXT_TYPE_application_settings_old as u16,
    btls_sys::TLSEXT_TYPE_certificate_authorities as u16,
    btls_sys::TLSEXT_TYPE_pake as u16,
    btls_sys::TLSEXT_TYPE_trust_anchors as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters_legacy as u16,
];
const IOS_EXTENSION_ORDER: &[u16] = &[
    btls_sys::TLSEXT_TYPE_server_name as u16,
    btls_sys::TLSEXT_TYPE_extended_master_secret as u16,
    btls_sys::TLSEXT_TYPE_renegotiate as u16,
    btls_sys::TLSEXT_TYPE_supported_groups as u16,
    btls_sys::TLSEXT_TYPE_ec_point_formats as u16,
    btls_sys::TLSEXT_TYPE_application_layer_protocol_negotiation as u16,
    btls_sys::TLSEXT_TYPE_status_request as u16,
    btls_sys::TLSEXT_TYPE_signature_algorithms as u16,
    btls_sys::TLSEXT_TYPE_certificate_timestamp as u16,
    btls_sys::TLSEXT_TYPE_key_share as u16,
    btls_sys::TLSEXT_TYPE_psk_key_exchange_modes as u16,
    btls_sys::TLSEXT_TYPE_supported_versions as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters as u16,
    btls_sys::TLSEXT_TYPE_early_data as u16,
    btls_sys::TLSEXT_TYPE_cookie as u16,
    btls_sys::TLSEXT_TYPE_cert_compression as u16,
    btls_sys::TLSEXT_TYPE_delegated_credential as u16,
    btls_sys::TLSEXT_TYPE_application_settings as u16,
    btls_sys::TLSEXT_TYPE_application_settings_old as u16,
    btls_sys::TLSEXT_TYPE_certificate_authorities as u16,
    btls_sys::TLSEXT_TYPE_pake as u16,
    btls_sys::TLSEXT_TYPE_trust_anchors as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters_legacy as u16,
];
const ANDROID_EXTENSION_ORDER: &[u16] = &[
    btls_sys::TLSEXT_TYPE_server_name as u16,
    btls_sys::TLSEXT_TYPE_extended_master_secret as u16,
    btls_sys::TLSEXT_TYPE_renegotiate as u16,
    btls_sys::TLSEXT_TYPE_supported_groups as u16,
    btls_sys::TLSEXT_TYPE_ec_point_formats as u16,
    btls_sys::TLSEXT_TYPE_status_request as u16,
    btls_sys::TLSEXT_TYPE_signature_algorithms as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters as u16,
    btls_sys::TLSEXT_TYPE_key_share as u16,
    btls_sys::TLSEXT_TYPE_psk_key_exchange_modes as u16,
    btls_sys::TLSEXT_TYPE_supported_versions as u16,
    btls_sys::TLSEXT_TYPE_session_ticket as u16,
    btls_sys::TLSEXT_TYPE_application_layer_protocol_negotiation as u16,
    btls_sys::TLSEXT_TYPE_certificate_timestamp as u16,
    btls_sys::TLSEXT_TYPE_cert_compression as u16,
    btls_sys::TLSEXT_TYPE_early_data as u16,
    btls_sys::TLSEXT_TYPE_cookie as u16,
    btls_sys::TLSEXT_TYPE_delegated_credential as u16,
    btls_sys::TLSEXT_TYPE_application_settings as u16,
    btls_sys::TLSEXT_TYPE_application_settings_old as u16,
    btls_sys::TLSEXT_TYPE_certificate_authorities as u16,
    btls_sys::TLSEXT_TYPE_pake as u16,
    btls_sys::TLSEXT_TYPE_trust_anchors as u16,
    btls_sys::TLSEXT_TYPE_quic_transport_parameters_legacy as u16,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utls_profiles_apply_to_btls_config() {
        for fingerprint in [
            UtlsFingerprint::Chrome,
            UtlsFingerprint::Firefox,
            UtlsFingerprint::Edge,
            UtlsFingerprint::Safari,
            UtlsFingerprint::ThreeSixty,
            UtlsFingerprint::Qq,
            UtlsFingerprint::Ios,
            UtlsFingerprint::Android,
            UtlsFingerprint::Random,
            UtlsFingerprint::Randomized,
        ] {
            let options = UtlsOptions { fingerprint };
            let mut crypto =
                new_utls_client_config(&options, false, &[], false).expect("btls client config");
            apply_utls_profile(&mut crypto, &options, &[b"h3".to_vec()], None)
                .expect("uTLS profile should be accepted by BoringSSL");
        }
    }

    #[test]
    fn utls_server_fingerprints_build_btls_config() {
        let options = UtlsOptions {
            fingerprint: UtlsFingerprint::Chrome,
        };
        let fingerprints = [CertificateFingerprint {
            algorithm: CertificateFingerprintAlgorithm::Sha256,
            digest: vec![0; 32],
        }];
        let mut crypto = new_utls_client_config(&options, false, &fingerprints, false)
            .expect("btls client config");
        apply_utls_profile(&mut crypto, &options, &[b"h3".to_vec()], None)
            .expect("uTLS profile should be accepted by BoringSSL");
        configure_server_verification(&mut crypto, false, fingerprints.to_vec())
            .expect("uTLS server fingerprint verification should be accepted");
    }

    #[test]
    fn utls_ech_builds_btls_config_with_cert_verify_callback() {
        let options = UtlsOptions {
            fingerprint: UtlsFingerprint::Chrome,
        };
        let mut crypto =
            new_utls_client_config(&options, false, &[], true).expect("btls client config");
        apply_utls_profile(
            &mut crypto,
            &options,
            &[b"h3".to_vec()],
            Some(vec![0, 1, 2]),
        )
        .expect("uTLS ECH profile should be accepted by BoringSSL");
    }
}
