use std::fs;
#[cfg(any(test, all(feature = "tls-quic", feature = "outbound-hysteria2")))]
use std::net::IpAddr;
use std::path::Path;

use base64::Engine as _;
use hammer_core::config::{EchConfigSource, EchOptions};
use hammer_core::error::{HammerError, HammerResult};
#[cfg(any(test, all(feature = "tls-quic", feature = "outbound-hysteria2")))]
use hickory_resolver::Resolver;
#[cfg(any(test, all(feature = "tls-quic", feature = "outbound-hysteria2")))]
use hickory_resolver::proto::rr::rdata::svcb::{SvcParamKey, SvcParamValue};
#[cfg(any(test, all(feature = "tls-quic", feature = "outbound-hysteria2")))]
use hickory_resolver::proto::rr::{RData, Record, RecordType};
use rustls::pki_types::EchConfigListBytes;

#[cfg(any(test, all(feature = "tls-quic", feature = "outbound-hysteria2")))]
const MAX_HTTPS_ALIAS_DEPTH: usize = 4;

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
                "tls.ech DNS HTTPS record lookup must be resolved before building TLS config",
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

#[cfg(all(feature = "tls-quic", feature = "outbound-hysteria2"))]
pub(crate) async fn resolve_dns_https_ech_config_list(server_name: &str) -> HammerResult<Vec<u8>> {
    let resolver = Resolver::builder_tokio()
        .map_err(|err| HammerError::internal(format!("read system DNS config: {err}")))?
        .build()
        .map_err(|err| HammerError::internal(format!("create DNS resolver: {err}")))?;
    let mut name = normalize_dns_name(server_name)?;

    for depth in 0..=MAX_HTTPS_ALIAS_DEPTH {
        let lookup = resolver
            .lookup(name.as_str(), RecordType::HTTPS)
            .await
            .map_err(|err| {
                HammerError::config_validation(format!(
                    "lookup tls.ech HTTPS record for {name}: {err}"
                ))
            })?;
        match ech_config_list_from_https_answers(lookup.answers()) {
            HttpsEchLookupResult::Config(config) => return Ok(config),
            HttpsEchLookupResult::Alias(alias) if depth < MAX_HTTPS_ALIAS_DEPTH => {
                name = alias;
            }
            HttpsEchLookupResult::Alias(_) => {
                return Err(HammerError::config_validation(format!(
                    "tls.ech HTTPS alias chain for {server_name} is too deep"
                )));
            }
            HttpsEchLookupResult::NotFound => {
                return Err(HammerError::config_validation(format!(
                    "tls.ech HTTPS record for {name} does not include ech"
                )));
            }
        }
    }

    unreachable!("HTTPS alias loop must return before exceeding max depth")
}

#[cfg(any(test, all(feature = "tls-quic", feature = "outbound-hysteria2")))]
fn normalize_dns_name(server_name: &str) -> HammerResult<String> {
    let server_name = server_name.trim();
    if server_name.is_empty() {
        return Err(HammerError::config_validation(
            "tls.ech DNS HTTPS lookup requires a server_name",
        ));
    }
    if server_name.parse::<IpAddr>().is_ok() {
        return Err(HammerError::config_validation(
            "tls.ech DNS HTTPS lookup requires a DNS server_name, not an IP address",
        ));
    }
    if server_name.ends_with('.') {
        Ok(server_name.to_owned())
    } else {
        Ok(format!("{server_name}."))
    }
}

#[cfg(any(test, all(feature = "tls-quic", feature = "outbound-hysteria2")))]
#[derive(Debug, PartialEq, Eq)]
enum HttpsEchLookupResult {
    Config(Vec<u8>),
    Alias(String),
    NotFound,
}

#[cfg(any(test, all(feature = "tls-quic", feature = "outbound-hysteria2")))]
fn ech_config_list_from_https_answers(answers: &[Record]) -> HttpsEchLookupResult {
    let mut https_records = answers
        .iter()
        .filter_map(|record| match &record.data {
            RData::HTTPS(https) => Some(&https.0),
            _ => None,
        })
        .collect::<Vec<_>>();
    https_records.sort_by_key(|record| record.svc_priority);

    if let Some(alias) = https_records
        .iter()
        .find(|record| record.svc_priority == 0)
        .map(|record| record.target_name.to_utf8())
    {
        if alias == "." {
            return HttpsEchLookupResult::NotFound;
        }
        return HttpsEchLookupResult::Alias(alias);
    }

    https_records
        .iter()
        .filter(|record| record.svc_priority > 0)
        .find_map(|record| {
            record
                .svc_params
                .iter()
                .find_map(|(key, value)| match (key, value) {
                    (SvcParamKey::EchConfigList, SvcParamValue::EchConfigList(config)) => {
                        Some(config.0.clone())
                    }
                    _ => None,
                })
        })
        .map(HttpsEchLookupResult::Config)
        .unwrap_or(HttpsEchLookupResult::NotFound)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use hickory_resolver::proto::rr::rdata::svcb::{
        EchConfigList as DnsEchConfigList, SvcParamKey, SvcParamValue,
    };
    use hickory_resolver::proto::rr::rdata::{HTTPS, SVCB};
    use hickory_resolver::proto::rr::{Name, RData, Record};

    use super::*;

    #[test]
    fn dns_name_normalization_requires_hostname() {
        assert_eq!(
            normalize_dns_name("example.com").expect("dns name"),
            "example.com."
        );
        assert!(normalize_dns_name("").is_err());
        assert!(normalize_dns_name("127.0.0.1").is_err());
    }

    #[test]
    fn extracts_lowest_priority_ech_config_from_https_records() {
        let high = https_record(3, "high.example.", Some(vec![3]));
        let low = https_record(1, "low.example.", Some(vec![1]));

        assert_eq!(
            ech_config_list_from_https_answers(&[high, low]),
            HttpsEchLookupResult::Config(vec![1])
        );
    }

    #[test]
    fn alias_mode_takes_precedence_over_service_mode() {
        let service = https_record(1, ".", Some(vec![1]));
        let alias = https_record(0, "svc.example.", None);

        assert_eq!(
            ech_config_list_from_https_answers(&[service, alias]),
            HttpsEchLookupResult::Alias("svc.example.".to_owned())
        );
    }

    fn https_record(priority: u16, target: &str, ech: Option<Vec<u8>>) -> Record {
        let params = ech
            .map(|config| {
                vec![(
                    SvcParamKey::EchConfigList,
                    SvcParamValue::EchConfigList(DnsEchConfigList(config)),
                )]
            })
            .unwrap_or_default();
        Record::from_rdata(
            Name::from_str("example.com.").expect("owner name"),
            60,
            RData::HTTPS(HTTPS(SVCB::new(
                priority,
                Name::from_str(target).expect("target name"),
                params,
            ))),
        )
    }
}
