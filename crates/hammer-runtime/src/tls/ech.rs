use std::fs;
use std::path::Path;

use base64::Engine as _;
use hammer_core::config::{EchConfigSource, EchOptions};
use hammer_core::error::{HammerError, HammerResult};
use rustls::pki_types::EchConfigListBytes;

pub(super) fn ech_config(ech: &EchOptions) -> HammerResult<rustls::client::EchConfig> {
    if ech.pq_signature_schemes_enabled {
        return Err(HammerError::config_validation(
            "tls.ech.pq_signature_schemes_enabled is parsed but not supported by rustls 0.23",
        ));
    }
    if ech.dynamic_record_sizing_disabled {
        return Err(HammerError::config_validation(
            "tls.ech.dynamic_record_sizing_disabled is only valid for TCP TLS streams",
        ));
    }
    let config = ech_config_list_bytes(ech)?;
    rustls::client::EchConfig::new(
        EchConfigListBytes::from(config),
        rustls::crypto::aws_lc_rs::hpke::ALL_SUPPORTED_SUITES,
    )
    .map_err(|err| HammerError::config_validation(format!("tls.ech config: {err}")))
}

pub(super) fn ech_config_list_bytes(ech: &EchOptions) -> HammerResult<Vec<u8>> {
    match ech.config_source.as_ref() {
        Some(EchConfigSource::Inline(config)) => Ok(config.0.clone()),
        Some(EchConfigSource::Path(path)) => load_ech_config_path(path),
        Some(EchConfigSource::DnsHttpsRecord) => {
            return Err(HammerError::config_validation(
                "tls.ech DNS HTTPS record lookup is not implemented yet",
            ));
        }
        None => {
            return Err(HammerError::config_validation(
                "tls.ech.config or config_path is required",
            ));
        }
    }
}

fn load_ech_config_path(path: &Path) -> HammerResult<Vec<u8>> {
    let bytes = fs::read(path).map_err(|err| {
        HammerError::config_validation(format!("read tls ECH config {}: {err}", path.display()))
    })?;
    if let Ok(text) = std::str::from_utf8(&bytes) {
        let compact = text.split_whitespace().collect::<String>();
        if !compact.is_empty()
            && let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(compact)
        {
            return Ok(decoded);
        }
    }
    Ok(bytes)
}
