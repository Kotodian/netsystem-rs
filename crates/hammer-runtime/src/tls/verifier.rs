use std::fmt;
use std::sync::Arc;

use hammer_adapter::PlatformInterface;
use hammer_core::config::{CertificateFingerprint, CertificateFingerprintAlgorithm};
use hammer_core::error::{HammerError, HammerResult};
use rustls::client::WantsClientCert;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::hash::{Hash, HashAlgorithm};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ConfigBuilder, WantsVerifier};

#[cfg(not(target_vendor = "apple"))]
use super::roots::root_cert_store;
use super::roots::{client_verifier_builder, platform_root_certificates};

pub(super) fn outbound_client_verifier_builder(
    builder: ConfigBuilder<ClientConfig, WantsVerifier>,
    platform: Option<Arc<dyn PlatformInterface>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
    insecure: bool,
    server_fingerprints: &[CertificateFingerprint],
) -> HammerResult<ConfigBuilder<ClientConfig, WantsClientCert>> {
    if server_fingerprints.is_empty() {
        return if insecure {
            Ok(builder
                .dangerous()
                .with_custom_certificate_verifier(SkipServerVerification::new(provider)))
        } else {
            client_verifier_builder(builder, platform)
        };
    }

    let inner: Arc<dyn ServerCertVerifier> = if insecure {
        SkipServerVerification::new(provider.clone())
    } else {
        platform_server_verifier(platform, provider.clone())?
    };
    let verifier = ServerFingerprintVerifier::new(inner, server_fingerprints.to_vec(), &provider)?;
    Ok(builder
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier)))
}

#[cfg(target_vendor = "apple")]
fn platform_server_verifier(
    platform: Option<Arc<dyn PlatformInterface>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> HammerResult<Arc<dyn ServerCertVerifier>> {
    let verifier = rustls_platform_verifier::Verifier::new_with_extra_roots(
        platform_root_certificates(platform),
        provider,
    )
    .map_err(|err| HammerError::internal(format!("platform TLS verifier: {err}")))?;
    Ok(Arc::new(verifier))
}

#[cfg(not(target_vendor = "apple"))]
fn platform_server_verifier(
    platform: Option<Arc<dyn PlatformInterface>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> HammerResult<Arc<dyn ServerCertVerifier>> {
    let roots = Arc::new(root_cert_store(platform_root_certificates(platform)));
    rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider)
        .build()
        .map(|verifier| -> Arc<dyn ServerCertVerifier> { verifier })
        .map_err(|err| HammerError::internal(format!("TLS verifier: {err}")))
}

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new(provider: Arc<rustls::crypto::CryptoProvider>) -> Arc<Self> {
        Arc::new(Self(provider))
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

struct ServerFingerprintVerifier {
    inner: Arc<dyn ServerCertVerifier>,
    fingerprints: Vec<CertificateFingerprint>,
    sha256: &'static dyn Hash,
}

impl ServerFingerprintVerifier {
    fn new(
        inner: Arc<dyn ServerCertVerifier>,
        fingerprints: Vec<CertificateFingerprint>,
        provider: &rustls::crypto::CryptoProvider,
    ) -> HammerResult<Self> {
        Ok(Self {
            inner,
            fingerprints,
            sha256: sha256_hash(provider)?,
        })
    }

    fn matches(&self, certificate: &CertificateDer<'_>) -> bool {
        self.fingerprints
            .iter()
            .any(|fingerprint| match fingerprint.algorithm {
                CertificateFingerprintAlgorithm::Sha256 => {
                    self.sha256.hash(certificate.as_ref()).as_ref() == fingerprint.digest.as_slice()
                }
            })
    }
}

impl fmt::Debug for ServerFingerprintVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerFingerprintVerifier")
            .field("fingerprint_count", &self.fingerprints.len())
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for ServerFingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let verified =
            self.inner
                .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)?;
        if self.matches(end_entity) {
            Ok(verified)
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.inner.requires_raw_public_keys()
    }

    fn root_hint_subjects(&self) -> Option<&[rustls::DistinguishedName]> {
        self.inner.root_hint_subjects()
    }
}

fn sha256_hash(provider: &rustls::crypto::CryptoProvider) -> HammerResult<&'static dyn Hash> {
    provider
        .cipher_suites
        .iter()
        .filter_map(|suite| suite.tls13())
        .map(|suite| suite.common.hash_provider)
        .find(|hash| hash.algorithm() == HashAlgorithm::SHA256)
        .ok_or_else(|| HammerError::internal("TLS provider does not expose SHA-256"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::provider::default_provider;

    #[test]
    fn server_fingerprint_verifier_pins_leaf_sha256() {
        let provider = default_provider();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let cert_der = CertificateDer::from(cert.cert.der().to_vec());
        let digest = sha256_hash(&provider)
            .unwrap()
            .hash(cert_der.as_ref())
            .as_ref()
            .to_vec();
        let verifier = ServerFingerprintVerifier::new(
            SkipServerVerification::new(provider.clone()),
            vec![CertificateFingerprint {
                algorithm: CertificateFingerprintAlgorithm::Sha256,
                digest,
            }],
            &provider,
        )
        .unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();

        verifier
            .verify_server_cert(&cert_der, &[], &server_name, &[], UnixTime::now())
            .expect("matching fingerprint should be accepted");

        let mismatch = ServerFingerprintVerifier::new(
            SkipServerVerification::new(provider.clone()),
            vec![CertificateFingerprint {
                algorithm: CertificateFingerprintAlgorithm::Sha256,
                digest: vec![0; 32],
            }],
            &provider,
        )
        .unwrap();
        let err = mismatch
            .verify_server_cert(&cert_der, &[], &server_name, &[], UnixTime::now())
            .expect_err("mismatched fingerprint should be rejected");
        assert!(matches!(
            err,
            rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure
            )
        ));
    }
}
