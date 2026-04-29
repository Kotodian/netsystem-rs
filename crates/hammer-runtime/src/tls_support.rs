use std::sync::Arc;

use base64::Engine as _;
use hammer_adapter::PlatformInterface;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;

pub(crate) fn root_cert_store(platform: Option<Arc<dyn PlatformInterface>>) -> RootCertStore {
    let mut roots = RootCertStore::empty();
    if let Some(platform) = platform {
        for cert in platform.system_certificates() {
            for der in parse_certificate(&cert) {
                let _ = roots.add(CertificateDer::from(der));
            }
        }
    }
    if roots.roots.is_empty() {
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
    }
    roots
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
