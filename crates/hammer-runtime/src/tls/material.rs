use std::fs;
use std::path::Path;

use hammer_core::config::{CertificateSource, ClientTlsAuth, PrivateKeySource};
use hammer_core::error::{HammerError, HammerResult};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub(super) fn load_client_auth(
    auth: ClientTlsAuth,
) -> HammerResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let mut certificates = Vec::new();
    for source in auth.certificates {
        certificates.extend(load_certificate_source(source)?);
    }
    if certificates.is_empty() {
        return Err(HammerError::config_validation(
            "tls client certificate chain must not be empty",
        ));
    }
    let key = load_private_key_source(auth.key)?;
    Ok((certificates, key))
}

fn load_certificate_source(
    source: CertificateSource,
) -> HammerResult<Vec<CertificateDer<'static>>> {
    match source {
        CertificateSource::Inline(certificate) => Ok(vec![CertificateDer::from(certificate.0)]),
        CertificateSource::Path(path) => {
            let bytes = fs::read(&path).map_err(|err| {
                HammerError::config_validation(format!(
                    "read tls client certificate {}: {err}",
                    path.display()
                ))
            })?;
            parse_certificate_bytes(&bytes, &path)
        }
    }
}

fn parse_certificate_bytes(
    bytes: &[u8],
    path: &Path,
) -> HammerResult<Vec<CertificateDer<'static>>> {
    if looks_like_pem(bytes) {
        let certificates = CertificateDer::pem_slice_iter(bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                HammerError::config_validation(format!(
                    "parse tls client certificate {}: {err}",
                    path.display()
                ))
            })?;
        if certificates.is_empty() {
            return Err(HammerError::config_validation(format!(
                "tls client certificate {} contains no certificates",
                path.display()
            )));
        }
        return Ok(certificates);
    }
    Ok(vec![CertificateDer::from(bytes.to_vec())])
}

fn load_private_key_source(source: PrivateKeySource) -> HammerResult<PrivateKeyDer<'static>> {
    match source {
        PrivateKeySource::Inline(key) => private_key_from_bytes(key.0, "tls client key"),
        PrivateKeySource::Path(path) => {
            let bytes = fs::read(&path).map_err(|err| {
                HammerError::config_validation(format!(
                    "read tls client key {}: {err}",
                    path.display()
                ))
            })?;
            private_key_from_bytes(bytes, &format!("tls client key {}", path.display()))
        }
    }
}

fn private_key_from_bytes(bytes: Vec<u8>, field: &str) -> HammerResult<PrivateKeyDer<'static>> {
    if looks_like_pem(&bytes) {
        return PrivateKeyDer::from_pem_slice(&bytes)
            .map_err(|err| HammerError::config_validation(format!("parse {field}: {err}")));
    }
    let key: PrivateKeyDer<'static> = PrivateKeyDer::try_from(bytes)
        .map_err(|err| HammerError::config_validation(format!("parse {field}: {err}")))?;
    Ok(key)
}

fn looks_like_pem(bytes: &[u8]) -> bool {
    bytes
        .windows(b"-----BEGIN ".len())
        .any(|window| window == b"-----BEGIN ")
}
