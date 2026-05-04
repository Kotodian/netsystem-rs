use std::sync::Arc;

use base64::Engine as _;
use hammer_adapter::PlatformInterface;
use hammer_core::error::HammerError;
use rustls::client::WantsClientCert;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, ConfigBuilder, RootCertStore, WantsVerifier};

pub(crate) fn client_verifier_builder(
    builder: ConfigBuilder<ClientConfig, WantsVerifier>,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, HammerError> {
    client_verifier_builder_for_platform(builder, platform)
}

#[cfg(target_vendor = "apple")]
fn client_verifier_builder_for_platform(
    builder: ConfigBuilder<ClientConfig, WantsVerifier>,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, HammerError> {
    let extra_roots = platform_root_certificates(platform);
    if extra_roots.is_empty() {
        use rustls_platform_verifier::BuilderVerifierExt;

        return builder
            .with_platform_verifier()
            .map_err(|err| HammerError::internal(format!("platform TLS verifier: {err}")));
    }

    let provider = Arc::clone(builder.crypto_provider());
    let verifier = rustls_platform_verifier::Verifier::new_with_extra_roots(extra_roots, provider)
        .map_err(|err| HammerError::internal(format!("platform TLS verifier: {err}")))?;
    Ok(builder
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier)))
}

#[cfg(not(target_vendor = "apple"))]
fn client_verifier_builder_for_platform(
    builder: ConfigBuilder<ClientConfig, WantsVerifier>,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, HammerError> {
    Ok(builder.with_root_certificates(root_cert_store(platform_root_certificates(platform))))
}

#[cfg(not(target_vendor = "apple"))]
fn root_cert_store(extra_roots: Vec<CertificateDer<'static>>) -> RootCertStore {
    let mut roots = RootCertStore::empty();
    for cert in extra_roots {
        let _ = roots.add(cert);
    }
    if roots.roots.is_empty() {
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
    }
    roots
}

fn platform_root_certificates(
    platform: Option<Arc<dyn PlatformInterface>>,
) -> Vec<CertificateDer<'static>> {
    platform
        .map(|platform| platform_root_certificates_from_entries(platform.system_certificates()))
        .unwrap_or_default()
}

fn platform_root_certificates_from_entries(
    entries: impl IntoIterator<Item = String>,
) -> Vec<CertificateDer<'static>> {
    let mut roots = RootCertStore::empty();
    let mut certificates = Vec::new();
    for entry in entries {
        for der in parse_certificate(&entry) {
            let cert = CertificateDer::from(der);
            if roots.add(cert.clone()).is_ok() {
                certificates.push(cert);
            }
        }
    }
    certificates
}

fn parse_certificate(input: &str) -> Vec<Vec<u8>> {
    if input.contains("BEGIN CERTIFICATE") {
        return parse_pem_certificates(input);
    }
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .map(|der| vec![der])
        .unwrap_or_default()
}

fn parse_pem_certificates(input: &str) -> Vec<Vec<u8>> {
    let mut certs = Vec::new();
    let mut rest = input;
    while let Some(start) = rest.find("-----BEGIN CERTIFICATE-----") {
        rest = &rest[start + "-----BEGIN CERTIFICATE-----".len()..];
        let Some(end) = rest.find("-----END CERTIFICATE-----") else {
            break;
        };
        let body = rest[..end]
            .lines()
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("");
        if let Ok(der) = base64::engine::general_purpose::STANDARD.decode(body) {
            certs.push(der);
        }
        rest = &rest[end + "-----END CERTIFICATE-----".len()..];
    }
    certs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_root_certificates_accepts_pem_and_base64_der() {
        let pem_cert =
            rcgen::generate_simple_self_signed(vec!["pem.example.com".to_owned()]).unwrap();
        let der_cert =
            rcgen::generate_simple_self_signed(vec!["der.example.com".to_owned()]).unwrap();
        let pem_der = pem_cert.cert.der().to_vec();
        let der = der_cert.cert.der().to_vec();
        let pem = pem_cert.cert.pem();
        let base64_der = base64::engine::general_purpose::STANDARD.encode(&der);

        let certs = platform_root_certificates_from_entries([pem, base64_der, String::new()]);

        assert_eq!(certs.len(), 2);
        assert_eq!(certs[0].as_ref(), pem_der.as_slice());
        assert_eq!(certs[1].as_ref(), der.as_slice());
    }
}
