#[cfg(feature = "tls-client")]
use super::client::BasicClientTlsConfig;
use super::client::OutboundClientTlsConfig;
#[cfg(feature = "tls-outbound")]
use super::material::load_client_auth;
use super::roots::platform_root_certificates;
use super::utls::{fingerprint_name, unsupported_for_rustls};
use crate::tls::backend::TlsBackend;
use btls::pkey::PKey;
use btls::x509::X509;
use foreign_types_shared::ForeignType;
use hammer_adapter::PlatformInterface;
use hammer_core::config::{CertificateFingerprint, UtlsFingerprint, UtlsOptions};
use hammer_core::error::{HammerError, HammerResult};
use quinn_btls::QuicSslContext;
use std::ffi::CString;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct BtlsUtlsBackend;

pub(super) static BTLS_UTLS_BACKEND: BtlsUtlsBackend = BtlsUtlsBackend;

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

    #[cfg(feature = "tls-quic")]
    fn outbound_quic_client_config(
        &self,
        options: OutboundClientTlsConfig,
    ) -> HammerResult<quinn::ClientConfig> {
        let utls = options.utls.as_ref().ok_or_else(|| {
            HammerError::config_validation("tls.utls is required for the BoringSSL uTLS backend")
        })?;
        if options.ech.is_some() {
            return Err(HammerError::config_validation(
                "tls.ech with tls.utls is not supported by the current QUIC uTLS backend",
            ));
        }

        let mut crypto = quinn_btls::ClientConfig::new()
            .map_err(|err| HammerError::internal(format!("btls quic client config: {err}")))?;
        apply_utls_profile(&mut crypto, utls)?;
        add_platform_roots(&mut crypto, options.platform)?;
        configure_server_verification(&mut crypto, options.insecure, options.server_fingerprints)?;
        configure_client_auth(&mut crypto, options.client_auth)?;
        crypto
            .set_alpn(&options.alpn_protocols)
            .map_err(|err| HammerError::config_validation(format!("tls ALPN: {err}")))?;

        Ok(quinn::ClientConfig::new(Arc::new(crypto)))
    }
}

fn apply_utls_profile(
    crypto: &mut quinn_btls::ClientConfig,
    options: &UtlsOptions,
) -> HammerResult<()> {
    let profile = UtlsProfile::for_fingerprint(options.fingerprint);
    let ctx = crypto.ctx_mut();
    let ctx = ctx.as_ptr();
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
        btls_sys::SSL_CTX_set_grease_enabled(ctx, i32::from(profile.grease));
        btls_sys::SSL_CTX_set_permute_extensions(ctx, i32::from(profile.permute_extensions));
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

fn configure_server_verification(
    crypto: &mut quinn_btls::ClientConfig,
    insecure: bool,
    server_fingerprints: Vec<CertificateFingerprint>,
) -> HammerResult<()> {
    if !server_fingerprints.is_empty() {
        return Err(HammerError::config_validation(
            "tls.server_fingerprint with tls.utls is not supported by the current QUIC uTLS backend",
        ));
    }
    crypto.verify_peer(!insecure);
    Ok(())
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

struct UtlsProfile {
    curves: &'static str,
    signature_algorithms: &'static str,
    grease: bool,
    permute_extensions: bool,
    signed_certificate_timestamps: bool,
    ocsp_stapling: bool,
    record_size_limit: Option<u16>,
}

impl UtlsProfile {
    fn for_fingerprint(fingerprint: UtlsFingerprint) -> Self {
        match fingerprint {
            UtlsFingerprint::Firefox => Self::firefox(),
            UtlsFingerprint::Safari | UtlsFingerprint::Ios => Self::safari(),
            UtlsFingerprint::Random | UtlsFingerprint::Randomized => Self {
                permute_extensions: true,
                ..Self::chrome()
            },
            UtlsFingerprint::Chrome
            | UtlsFingerprint::Edge
            | UtlsFingerprint::ThreeSixty
            | UtlsFingerprint::Qq
            | UtlsFingerprint::Android => Self::chrome(),
        }
    }

    fn chrome() -> Self {
        Self {
            curves: "X25519MLKEM768:X25519:P-256:P-384",
            signature_algorithms: COMMON_SIGALGS,
            grease: true,
            permute_extensions: false,
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: None,
        }
    }

    fn firefox() -> Self {
        Self {
            curves: "X25519:P-256:P-384:P-521",
            signature_algorithms: FIREFOX_SIGALGS,
            grease: true,
            permute_extensions: false,
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: Some(0x4001),
        }
    }

    fn safari() -> Self {
        Self {
            curves: "X25519:P-256:P-384:P-521",
            signature_algorithms: COMMON_SIGALGS,
            grease: true,
            permute_extensions: false,
            signed_certificate_timestamps: true,
            ocsp_stapling: true,
            record_size_limit: None,
        }
    }
}

const COMMON_SIGALGS: &str = concat!(
    "ecdsa_secp256r1_sha256:",
    "rsa_pss_rsae_sha256:",
    "rsa_pkcs1_sha256:",
    "ecdsa_secp384r1_sha384:",
    "rsa_pss_rsae_sha384:",
    "rsa_pkcs1_sha384:",
    "rsa_pss_rsae_sha512:",
    "rsa_pkcs1_sha512"
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
    "rsa_pkcs1_sha512"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utls_profiles_apply_to_btls_config() {
        for fingerprint in [
            UtlsFingerprint::Chrome,
            UtlsFingerprint::Firefox,
            UtlsFingerprint::Safari,
            UtlsFingerprint::Randomized,
        ] {
            let mut crypto = quinn_btls::ClientConfig::new().expect("btls client config");
            apply_utls_profile(&mut crypto, &UtlsOptions { fingerprint })
                .expect("uTLS profile should be accepted by BoringSSL");
        }
    }
}
