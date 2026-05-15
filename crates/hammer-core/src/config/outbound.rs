//! `[hysteria2]` and direct/block outbound config sections.
//!
//! Hysteria2 is the only outbound protocol with its own TOML section
//! (top-level `[hysteria2]`); direct is synthesized by `build_outbounds`.
//! DNS is handled by `DnsRouter`/`DnsTransport`, not as a dialable outbound.
//! `Outbound` / `OutboundKind` sit at this layer so adding a new outbound
//! protocol means dropping a new variant here.

use std::time::Duration;

#[cfg(feature = "tls")]
use std::path::PathBuf;

#[cfg(feature = "hysteria2")]
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::HammerError;
#[cfg(feature = "hysteria2")]
use crate::protocol::congestion::BbrProfile;

use super::constants as C;
use super::raw_struct;
#[cfg(any(feature = "hysteria2", feature = "tls"))]
use super::raw_struct_with_default_check;

#[cfg(feature = "hysteria2")]
raw_struct_with_default_check! {
    pub struct RawHysteria2Config {
        /// Outbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Hysteria2 server host or IP.
        pub server: String => "String::is_empty",
        /// Single Hysteria2 server port.
        pub server_port: Option<u16> => "Option::is_none",
        /// Port-hopping range strings from the raw config.
        pub server_ports: Vec<String> => "Vec::is_empty",
        /// Hysteria2 password.
        pub password: String => "String::is_empty",
        /// Upload bandwidth hint in Mbps.
        pub up_mbps: Option<i64> => "Option::is_none",
        /// Download bandwidth hint in Mbps.
        pub down_mbps: Option<i64> => "Option::is_none",
        /// TLS SNI override.
        pub sni: String => "String::is_empty",
        /// Whether invalid TLS certificates are accepted.
        pub insecure: Option<bool> => "Option::is_none",
        /// Optional sing-box-style TLS section.
        pub tls: RawOutboundTlsConfig => "RawOutboundTlsConfig::is_default",
        /// Enabled network list from the raw config.
        pub network: Vec<Hysteria2Network> => "Vec::is_empty",
        /// Port-hopping interval.
        #[serde(with = "humantime_serde::option")]
        pub hop_interval: Option<Duration> => "Option::is_none",
        /// Maximum port-hopping interval.
        #[serde(with = "humantime_serde::option")]
        pub hop_interval_max: Option<Duration> => "Option::is_none",
        /// QUIC idle timeout.
        #[serde(with = "humantime_serde::option")]
        pub idle_timeout: Option<Duration> => "Option::is_none",
        /// QUIC keep-alive period.
        #[serde(with = "humantime_serde::option")]
        pub keep_alive_period: Option<Duration> => "Option::is_none",
        /// Hysteria2 BBR profile.
        pub bbr_profile: Option<BbrProfile> => "Option::is_none",
        /// Whether Brutal congestion-control debug output is enabled.
        pub brutal_debug: Option<bool> => "Option::is_none",
        /// Whether QUIC path MTU discovery is disabled.
        #[serde(rename = "disable_path_mtu_discovery")]
        pub disable_path_mtu: Option<bool> => "Option::is_none",
        /// Initial QUIC datagram size.
        pub initial_packet_size: Option<u16> => "Option::is_none",
        /// Optional Hysteria2 obfuscation section.
        pub obfs: RawHysteria2Obfs => "RawHysteria2Obfs::is_default",
    }
}

#[cfg(feature = "tls")]
raw_struct! {
    pub struct RawOutboundTlsConfig {
        /// Whether TLS is enabled. Hysteria2 always requires TLS.
        pub enabled: Option<bool> => "Option::is_none",
        /// TLS SNI/server-name override.
        pub server_name: String => "String::is_empty",
        /// Whether invalid TLS certificates are accepted.
        pub insecure: Option<bool> => "Option::is_none",
        /// ALPN protocol list.
        pub alpn: Vec<String> => "Vec::is_empty",
        /// Minimum TLS version.
        pub min_version: Option<TlsVersion> => "Option::is_none",
        /// Maximum TLS version.
        pub max_version: Option<TlsVersion> => "Option::is_none",
        /// Accepted server certificate fingerprints.
        pub server_fingerprint: Vec<RawCertificateFingerprint> => "Vec::is_empty",
        /// Inline client certificate chain, PEM or base64 DER.
        pub client_certificate: Vec<RawCertificate> => "Vec::is_empty",
        /// Client certificate file paths.
        pub client_certificate_path: Vec<PathBuf> => "Vec::is_empty",
        /// Inline client private key, PEM or base64 DER.
        pub client_key: Option<RawPrivateKey> => "Option::is_none",
        /// Client private key path.
        pub client_key_path: Option<PathBuf> => "Option::is_none",
        /// uTLS ClientHello fingerprint options.
        pub utls: RawUtlsConfig => "RawUtlsConfig::is_default",
        /// Encrypted ClientHello options.
        pub ech: RawEchConfig => "RawEchConfig::is_default",
        /// Reality client options.
        pub reality: RawRealityConfig => "RawRealityConfig::is_default",
        /// TLS handshake fragmentation options.
        pub fragment: RawTlsFragmentConfig => "RawTlsFragmentConfig::is_default",
        /// TLS record fragmentation options.
        pub record_fragment: RawTlsRecordFragmentConfig => "RawTlsRecordFragmentConfig::is_default",
    }
}

#[cfg(feature = "hysteria2")]
impl RawOutboundTlsConfig {
    pub(super) fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(feature = "tls")]
raw_struct_with_default_check! {
    pub struct RawUtlsConfig {
        /// Whether uTLS fingerprinting is enabled.
        pub enabled: Option<bool> => "Option::is_none",
        /// Browser fingerprint profile.
        pub fingerprint: Option<UtlsFingerprint> => "Option::is_none",
    }
}

#[cfg(feature = "tls")]
raw_struct_with_default_check! {
    pub struct RawEchConfig {
        /// Whether ECH is enabled.
        pub enabled: Option<bool> => "Option::is_none",
        /// Inline base64 ECHConfigList.
        pub config: Option<RawEchConfigList> => "Option::is_none",
        /// File path containing a base64 ECHConfigList.
        pub config_path: Option<PathBuf> => "Option::is_none",
        /// Whether post-quantum signature schemes are enabled.
        pub pq_signature_schemes_enabled: Option<bool> => "Option::is_none",
        /// Whether dynamic record sizing is disabled.
        pub dynamic_record_sizing_disabled: Option<bool> => "Option::is_none",
    }
}

#[cfg(feature = "tls")]
raw_struct_with_default_check! {
    pub struct RawRealityConfig {
        /// Whether Reality is enabled.
        pub enabled: Option<bool> => "Option::is_none",
        /// Reality server X25519 public key.
        pub public_key: Option<RawRealityPublicKey> => "Option::is_none",
        /// Reality short id in hex form.
        pub short_id: Option<RawRealityShortId> => "Option::is_none",
    }
}

#[cfg(feature = "tls")]
raw_struct_with_default_check! {
    pub struct RawTlsFragmentConfig {
        /// Whether TLS handshake fragmentation is enabled.
        pub enabled: Option<bool> => "Option::is_none",
        /// Fragment size expression.
        pub size: String => "String::is_empty",
        /// Delay between fragments.
        #[serde(with = "humantime_serde::option")]
        pub sleep: Option<Duration> => "Option::is_none",
    }
}

#[cfg(feature = "tls")]
raw_struct_with_default_check! {
    pub struct RawTlsRecordFragmentConfig {
        /// Whether TLS record fragmentation is enabled.
        pub enabled: Option<bool> => "Option::is_none",
    }
}

#[cfg(feature = "hysteria2")]
raw_struct_with_default_check! {
    pub struct RawHysteria2Obfs {
        /// Obfuscation type.
        #[serde(rename = "type")]
        pub type_: Option<Hysteria2ObfsType> => "Option::is_none",
        /// Obfuscation password.
        pub password: String => "String::is_empty",
    }
}

raw_struct! {
    pub struct RawDirectOutboundConfig {
        /// Outbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Network strategy placeholder for sing-box compatibility.
        pub network_strategy: String => "String::is_empty",
    }
}

raw_struct! {
    pub struct RawBlockOutboundConfig {
        /// Outbound id used by route rules.
        pub id: String => "String::is_empty",
    }
}

raw_struct! {
    pub struct RawUrltestConfig {
        /// Outbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Child outbound ids — at least one is required. Each id must match
        /// a declared outbound or an endpoint's outbound view.
        pub outbounds: Vec<String> => "Vec::is_empty",
        /// HTTP(S) URL probed via each child outbound. Defaults to
        /// `https://www.gstatic.com/generate_204` when absent.
        pub url: Option<Url> => "Option::is_none",
        /// Tolerance in milliseconds. A new candidate must beat the
        /// current pick by at least this much to trigger a switch.
        pub tolerance_ms: Option<u64> => "Option::is_none",
        /// Per-probe timeout. Applied as a wall-clock cap around the
        /// dial + TLS handshake + HTTP HEAD round-trip.
        #[serde(with = "humantime_serde::option")]
        pub timeout: Option<Duration> => "Option::is_none",
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawOutbound {
    #[cfg(feature = "hysteria2")]
    Hysteria2(RawHysteria2Config),
    Direct(RawDirectOutboundConfig),
    Block(RawBlockOutboundConfig),
    Urltest(RawUrltestConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    pub id: String,
    pub kind: OutboundKind,
}

impl Outbound {
    pub fn type_name(&self) -> &'static str {
        match &self.kind {
            #[cfg(feature = "hysteria2")]
            OutboundKind::Hysteria2(_) => C::TYPE_HYSTERIA2,
            OutboundKind::Direct(_) => C::TYPE_DIRECT,
            OutboundKind::Block => C::TYPE_BLOCK,
            OutboundKind::Urltest(_) => C::TYPE_URLTEST,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum OutboundKind {
    #[cfg(feature = "hysteria2")]
    Hysteria2(Hysteria2OutboundOptions),
    Direct(DirectOutboundOptions),
    Block,
    Urltest(UrltestOutboundOptions),
}

/// Resolved urltest outbound config — child ids stay as strings; the runtime
/// `OutboundManager` resolves them to live `Outbound` Arcs at PostStart so
/// declaration order does not matter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrltestOutboundOptions {
    pub outbounds: Vec<String>,
    pub url: Url,
    pub tolerance: Duration,
    pub timeout: Duration,
}

#[cfg(feature = "hysteria2")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hysteria2OutboundOptions {
    pub server: String,
    pub server_port: u16,
    pub server_ports: Vec<String>,
    pub password: String,
    pub up_mbps: i64,
    pub down_mbps: i64,
    pub network: Vec<Hysteria2Network>,
    pub hop_interval: Option<Duration>,
    pub hop_interval_max: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub keep_alive_period: Option<Duration>,
    pub bbr_profile: BbrProfile,
    pub brutal_debug: bool,
    pub disable_path_mtu_discovery: bool,
    pub initial_packet_size: u16,
    pub tls: OutboundTlsOptions,
    pub obfs: Option<Hysteria2Obfs>,
}

#[cfg(feature = "hysteria2")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Hysteria2Network {
    Tcp,
    Udp,
}

#[cfg(feature = "hysteria2")]
impl Hysteria2Network {
    pub fn name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundTlsOptions {
    pub enabled: bool,
    pub server_name: String,
    pub insecure: bool,
    pub alpn: Vec<String>,
    pub min_version: Option<TlsVersion>,
    pub max_version: Option<TlsVersion>,
    pub server_fingerprints: Vec<CertificateFingerprint>,
    pub client_auth: Option<ClientTlsAuth>,
    pub utls: Option<UtlsOptions>,
    pub ech: Option<EchOptions>,
    pub reality: Option<RealityOptions>,
    pub fragment: Option<TlsFragmentOptions>,
    pub record_fragment: bool,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RawCertificate(pub String);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RawPrivateKey(pub String);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RawCertificateFingerprint(pub String);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertificateSource {
    Inline(CertificateDerBytes),
    Path(PathBuf),
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivateKeySource {
    Inline(PrivateKeyDerBytes),
    Path(PathBuf),
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateDerBytes(pub Vec<u8>);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateKeyDerBytes(pub Vec<u8>);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTlsAuth {
    pub certificates: Vec<CertificateSource>,
    pub key: PrivateKeySource,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateFingerprint {
    pub algorithm: CertificateFingerprintAlgorithm,
    pub digest: Vec<u8>,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertificateFingerprintAlgorithm {
    Sha256,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum TlsVersion {
    #[serde(rename = "1.2")]
    Tls12,
    #[serde(rename = "1.3")]
    Tls13,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum UtlsFingerprint {
    #[default]
    Chrome,
    Firefox,
    Edge,
    Safari,
    #[serde(rename = "360")]
    ThreeSixty,
    Qq,
    Ios,
    Android,
    Random,
    Randomized,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtlsOptions {
    pub fingerprint: UtlsFingerprint,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchOptions {
    pub config_source: Option<EchConfigSource>,
    pub pq_signature_schemes_enabled: bool,
    pub dynamic_record_sizing_disabled: bool,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EchConfigSource {
    Inline(EchConfigList),
    Path(PathBuf),
    DnsHttpsRecord,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RawEchConfigList(pub String);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchConfigList(pub Vec<u8>);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealityOptions {
    pub public_key: RealityPublicKey,
    pub short_id: RealityShortId,
}

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RawRealityPublicKey(pub String);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RawRealityShortId(pub String);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealityPublicKey(pub [u8; 32]);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealityShortId(pub Vec<u8>);

#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFragmentOptions {
    pub size: String,
    pub sleep: Duration,
}

#[cfg(feature = "hysteria2")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hysteria2Obfs {
    pub type_: Hysteria2ObfsType,
    pub password: String,
}

#[cfg(feature = "hysteria2")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Hysteria2ObfsType {
    #[default]
    Salamander,
}

#[cfg(feature = "hysteria2")]
impl Hysteria2ObfsType {
    pub fn name(self) -> &'static str {
        match self {
            Self::Salamander => "salamander",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirectOutboundOptions {
    pub network_strategy: String,
}

/// Build the runtime outbound list from the parsed Hysteria2 section. Direct
/// is always synthesized so the DNS subsystem (and any future "fall back to
/// direct" rule) has a stable id to dial through.
#[cfg(feature = "hysteria2")]
pub(super) fn build_outbounds(
    hysteria: Hysteria2OutboundOptions,
    hysteria_id: String,
) -> Vec<Outbound> {
    vec![
        Outbound {
            id: hysteria_id,
            kind: OutboundKind::Hysteria2(hysteria),
        },
        Outbound {
            id: C::DEFAULT_DIRECT_ID.to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions {
                network_strategy: C::NETWORK_STRATEGY_DEFAULT.to_owned(),
            }),
        },
    ]
}

pub(super) fn build_default_outbounds() -> (Vec<Outbound>, String) {
    let mut outbounds = Vec::new();
    ensure_direct_outbound(&mut outbounds);
    (outbounds, C::DEFAULT_DIRECT_ID.to_owned())
}

pub(super) fn build_declared_outbounds(
    raw: Vec<RawOutbound>,
) -> Result<Vec<Outbound>, HammerError> {
    let mut outbounds = Vec::new();
    for (idx, raw) in raw.into_iter().enumerate() {
        let outbound = match raw {
            #[cfg(feature = "hysteria2")]
            RawOutbound::Hysteria2(raw) => {
                let (options, id) = build_hysteria_options(raw)?;
                Outbound {
                    id,
                    kind: OutboundKind::Hysteria2(options),
                }
            }
            RawOutbound::Direct(raw) => {
                let id = if raw.id.is_empty() {
                    C::DEFAULT_DIRECT_ID.to_owned()
                } else {
                    raw.id
                };
                Outbound {
                    id,
                    kind: OutboundKind::Direct(DirectOutboundOptions {
                        network_strategy: if raw.network_strategy.is_empty() {
                            C::NETWORK_STRATEGY_DEFAULT.to_owned()
                        } else {
                            raw.network_strategy
                        },
                    }),
                }
            }
            RawOutbound::Block(raw) => {
                if raw.id.is_empty() {
                    return Err(HammerError::config_validation(format!(
                        "outbounds[{idx}].id is required"
                    )));
                }
                Outbound {
                    id: raw.id,
                    kind: OutboundKind::Block,
                }
            }
            RawOutbound::Urltest(raw) => {
                let (options, id) = build_urltest_options(idx, raw)?;
                Outbound {
                    id,
                    kind: OutboundKind::Urltest(options),
                }
            }
        };
        outbounds.push(outbound);
    }
    ensure_direct_outbound(&mut outbounds);
    Ok(outbounds)
}

fn build_urltest_options(
    idx: usize,
    raw: RawUrltestConfig,
) -> Result<(UrltestOutboundOptions, String), HammerError> {
    let RawUrltestConfig {
        id,
        outbounds,
        url,
        tolerance_ms,
        timeout,
    } = raw;
    if id.is_empty() {
        return Err(HammerError::config_validation(format!(
            "outbounds[{idx}].id is required"
        )));
    }
    if outbounds.is_empty() {
        return Err(HammerError::config_validation(format!(
            "outbounds[{idx}] (urltest '{id}') requires at least one child in `outbounds`"
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for child in &outbounds {
        if child.is_empty() {
            return Err(HammerError::config_validation(format!(
                "outbounds[{idx}] (urltest '{id}') has an empty child id"
            )));
        }
        if !seen.insert(child.as_str()) {
            return Err(HammerError::config_validation(format!(
                "outbounds[{idx}] (urltest '{id}') lists duplicate child id: {child}"
            )));
        }
        if child == &id {
            return Err(HammerError::config_validation(format!(
                "outbounds[{idx}] (urltest '{id}') cannot reference itself"
            )));
        }
    }
    let url = match url {
        Some(url) => url,
        None => Url::parse(C::DEFAULT_URLTEST_URL).expect("default urltest URL is valid"),
    };
    match url.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(HammerError::config_validation(format!(
                "outbounds[{idx}] (urltest '{id}') url scheme must be http or https, got: {scheme}"
            )));
        }
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(HammerError::config_validation(format!(
            "outbounds[{idx}] (urltest '{id}') url is missing a host"
        )));
    }
    let tolerance = Duration::from_millis(tolerance_ms.unwrap_or(C::DEFAULT_URLTEST_TOLERANCE_MS));
    let timeout = timeout.unwrap_or_else(|| Duration::from_millis(C::DEFAULT_URLTEST_TIMEOUT_MS));
    if timeout.is_zero() {
        return Err(HammerError::config_validation(format!(
            "outbounds[{idx}] (urltest '{id}') timeout must be > 0"
        )));
    }
    Ok((
        UrltestOutboundOptions {
            outbounds,
            url,
            tolerance,
            timeout,
        },
        id,
    ))
}

/// Validate that every urltest references real, non-urltest outbound ids.
/// Nesting urltest inside urltest is rejected in V1 — sing-box flattens via
/// `RealTag` indirection, but we want the simpler invariant of "leaves only".
pub(super) fn validate_urltest_dependencies<'a>(
    outbounds: &[Outbound],
    valid_child_ids: impl IntoIterator<Item = &'a str>,
) -> Result<(), HammerError> {
    use std::collections::{HashMap, HashSet};

    let by_id: HashMap<&str, &Outbound> = outbounds.iter().map(|o| (o.id.as_str(), o)).collect();
    let valid_child_ids: HashSet<&str> = valid_child_ids.into_iter().collect();
    for outbound in outbounds {
        let OutboundKind::Urltest(options) = &outbound.kind else {
            continue;
        };
        for child_id in &options.outbounds {
            if !valid_child_ids.contains(child_id.as_str()) {
                return Err(HammerError::config_validation(format!(
                    "urltest '{}' references unknown outbound id: {child_id}",
                    outbound.id
                )));
            }
            if by_id
                .get(child_id.as_str())
                .is_some_and(|child| matches!(child.kind, OutboundKind::Urltest(_)))
            {
                return Err(HammerError::config_validation(format!(
                    "urltest '{}' cannot nest another urltest: {child_id}",
                    outbound.id
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn ensure_direct_outbound(outbounds: &mut Vec<Outbound>) {
    if outbounds
        .iter()
        .any(|outbound| outbound.id == C::DEFAULT_DIRECT_ID)
    {
        return;
    }
    outbounds.push(Outbound {
        id: C::DEFAULT_DIRECT_ID.to_owned(),
        kind: OutboundKind::Direct(DirectOutboundOptions {
            network_strategy: C::NETWORK_STRATEGY_DEFAULT.to_owned(),
        }),
    });
}

#[cfg(feature = "hysteria2")]
fn build_hysteria_tls_options(
    raw: RawOutboundTlsConfig,
    legacy_sni: String,
    legacy_insecure: Option<bool>,
    server: &str,
) -> Result<OutboundTlsOptions, HammerError> {
    let RawOutboundTlsConfig {
        enabled,
        server_name: tls_server_name,
        insecure: tls_insecure,
        alpn,
        min_version,
        max_version,
        server_fingerprint,
        client_certificate,
        client_certificate_path,
        client_key,
        client_key_path,
        utls,
        ech,
        reality,
        fragment,
        record_fragment,
    } = raw;

    if enabled == Some(false) {
        return Err(HammerError::config_validation(
            "hysteria2.tls.enabled=false is not supported",
        ));
    }
    let mut server_name = merge_tls_server_name("hysteria2", legacy_sni, tls_server_name)?;
    let insecure = merge_tls_insecure("hysteria2", legacy_insecure, tls_insecure)?;
    if !insecure && server_name.is_empty() && server.parse::<std::net::IpAddr>().is_err() {
        server_name = server.to_owned();
    }
    if !insecure && server_name.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.tls.server_name is required unless insecure=true",
        ));
    }
    validate_hysteria_tls_versions(min_version, max_version)?;
    validate_hysteria_alpn(&alpn)?;

    let server_fingerprints = server_fingerprint
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<CertificateFingerprint>, _>>()?;
    let client_auth: Optional<ClientTlsAuth> = RawClientTlsAuth {
        certificates: client_certificate,
        certificate_paths: client_certificate_path,
        key: client_key,
        key_path: client_key_path,
    }
    .try_into()?;
    let utls: Optional<UtlsOptions> = utls.try_into()?;
    let ech: Optional<EchOptions> = ech.try_into()?;
    let reality: Optional<RealityOptions> = reality.try_into()?;
    let fragment: Optional<TlsFragmentOptions> = fragment.try_into()?;

    Ok(OutboundTlsOptions {
        enabled: true,
        server_name,
        insecure,
        alpn,
        min_version,
        max_version,
        server_fingerprints,
        client_auth: client_auth.into_option(),
        utls: utls.into_option(),
        ech: ech.into_option(),
        reality: reality.into_option(),
        fragment: fragment.into_option(),
        record_fragment: record_fragment.enabled.unwrap_or(false),
    })
}

#[cfg(feature = "hysteria2")]
fn merge_tls_server_name(
    prefix: &str,
    legacy_sni: String,
    tls_server_name: String,
) -> Result<String, HammerError> {
    if !legacy_sni.is_empty() && !tls_server_name.is_empty() && legacy_sni != tls_server_name {
        return Err(HammerError::config_validation(format!(
            "{prefix}.sni conflicts with {prefix}.tls.server_name"
        )));
    }
    if tls_server_name.is_empty() {
        Ok(legacy_sni)
    } else {
        Ok(tls_server_name)
    }
}

#[cfg(feature = "hysteria2")]
fn merge_tls_insecure(
    prefix: &str,
    legacy_insecure: Option<bool>,
    tls_insecure: Option<bool>,
) -> Result<bool, HammerError> {
    if let (Some(legacy), Some(nested)) = (legacy_insecure, tls_insecure)
        && legacy != nested
    {
        return Err(HammerError::config_validation(format!(
            "{prefix}.insecure conflicts with {prefix}.tls.insecure"
        )));
    }
    Ok(tls_insecure.or(legacy_insecure).unwrap_or(false))
}

#[cfg(feature = "hysteria2")]
fn validate_hysteria_tls_versions(
    min_version: Option<TlsVersion>,
    max_version: Option<TlsVersion>,
) -> Result<(), HammerError> {
    if min_version == Some(TlsVersion::Tls12) || max_version == Some(TlsVersion::Tls12) {
        return Err(HammerError::config_validation(
            "hysteria2.tls only supports TLS 1.3",
        ));
    }
    Ok(())
}

#[cfg(feature = "hysteria2")]
fn validate_hysteria_alpn(alpn: &[String]) -> Result<(), HammerError> {
    if alpn.iter().any(String::is_empty) {
        return Err(HammerError::config_validation(
            "hysteria2.tls.alpn must not contain empty protocols",
        ));
    }
    if !alpn.is_empty() && (alpn.len() != 1 || alpn[0] != "h3") {
        return Err(HammerError::config_validation(
            "hysteria2.tls.alpn must be [\"h3\"]",
        ));
    }
    Ok(())
}

#[cfg(feature = "hysteria2")]
struct Optional<T>(Option<T>);

#[cfg(feature = "hysteria2")]
impl<T> Optional<T> {
    fn into_option(self) -> Option<T> {
        self.0
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawCertificateFingerprint> for CertificateFingerprint {
    type Error = HammerError;

    fn try_from(raw: RawCertificateFingerprint) -> Result<Self, Self::Error> {
        parse_certificate_fingerprint(&raw.0)
    }
}

#[cfg(feature = "hysteria2")]
fn parse_certificate_fingerprint(raw: &str) -> Result<CertificateFingerprint, HammerError> {
    let Some((algorithm, digest)) = raw.split_once('/') else {
        return Err(HammerError::config_validation(
            "hysteria2.tls.server_fingerprint entries must use sha256/<digest>",
        ));
    };
    if !algorithm.eq_ignore_ascii_case("sha256") {
        return Err(HammerError::config_validation(
            "hysteria2.tls.server_fingerprint only supports sha256",
        ));
    }
    let digest = decode_hex_or_base64("hysteria2.tls.server_fingerprint", digest)?;
    if digest.len() != 32 {
        return Err(HammerError::config_validation(
            "hysteria2.tls.server_fingerprint sha256 digest must be 32 bytes",
        ));
    }
    Ok(CertificateFingerprint {
        algorithm: CertificateFingerprintAlgorithm::Sha256,
        digest,
    })
}

#[cfg(feature = "hysteria2")]
struct RawClientTlsAuth {
    certificates: Vec<RawCertificate>,
    certificate_paths: Vec<PathBuf>,
    key: Option<RawPrivateKey>,
    key_path: Option<PathBuf>,
}

#[cfg(feature = "hysteria2")]
struct CertificateSources(Vec<CertificateSource>);

#[cfg(feature = "hysteria2")]
impl TryFrom<RawCertificate> for CertificateSources {
    type Error = HammerError;

    fn try_from(raw: RawCertificate) -> Result<Self, Self::Error> {
        parse_certificate_chain(&raw.0, "hysteria2.tls.client_certificate").map(|certificates| {
            CertificateSources(
                certificates
                    .into_iter()
                    .map(|der| CertificateSource::Inline(CertificateDerBytes(der)))
                    .collect(),
            )
        })
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawPrivateKey> for PrivateKeySource {
    type Error = HammerError;

    fn try_from(raw: RawPrivateKey) -> Result<Self, Self::Error> {
        parse_private_key(&raw.0, "hysteria2.tls.client_key")
            .map(PrivateKeyDerBytes)
            .map(PrivateKeySource::Inline)
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawClientTlsAuth> for Optional<ClientTlsAuth> {
    type Error = HammerError;

    fn try_from(raw: RawClientTlsAuth) -> Result<Self, Self::Error> {
        let RawClientTlsAuth {
            certificates,
            certificate_paths,
            key,
            key_path,
        } = raw;
        let mut certificate_sources = Vec::new();
        for certificate in certificates {
            let CertificateSources(mut sources) = certificate.try_into()?;
            certificate_sources.append(&mut sources);
        }
        for path in certificate_paths {
            if path.as_os_str().is_empty() {
                return Err(HammerError::config_validation(
                    "hysteria2.tls.client_certificate_path must not contain empty paths",
                ));
            }
            certificate_sources.push(CertificateSource::Path(path));
        }

        let key_source = match (key, key_path) {
            (Some(_), Some(_)) => {
                return Err(HammerError::config_validation(
                    "hysteria2.tls.client_key and client_key_path cannot both be set",
                ));
            }
            (Some(key), None) => Some(key.try_into()?),
            (None, Some(path)) => {
                if path.as_os_str().is_empty() {
                    return Err(HammerError::config_validation(
                        "hysteria2.tls.client_key_path must not be empty",
                    ));
                }
                Some(PrivateKeySource::Path(path))
            }
            (None, None) => None,
        };

        let auth = match (certificate_sources.is_empty(), key_source) {
            (true, None) => None,
            (true, Some(_)) => Err(HammerError::config_validation(
                "hysteria2.tls.client_certificate is required when client_key is set",
            ))?,
            (false, None) => Err(HammerError::config_validation(
                "hysteria2.tls.client_key is required when client_certificate is set",
            ))?,
            (false, Some(key)) => Some(ClientTlsAuth {
                certificates: certificate_sources,
                key,
            }),
        };
        Ok(Self(auth))
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawUtlsConfig> for Optional<UtlsOptions> {
    type Error = HammerError;

    fn try_from(raw: RawUtlsConfig) -> Result<Self, Self::Error> {
        let enabled = raw.enabled.unwrap_or(false);
        if !enabled {
            if raw.fingerprint.is_some() {
                return Err(HammerError::config_validation(
                    "hysteria2.tls.utls.enabled must be true when fingerprint is set",
                ));
            }
            return Ok(Self(None));
        }
        Ok(Self(Some(UtlsOptions {
            fingerprint: raw.fingerprint.unwrap_or_default(),
        })))
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawEchConfigList> for EchConfigList {
    type Error = HammerError;

    fn try_from(raw: RawEchConfigList) -> Result<Self, Self::Error> {
        decode_hex_or_base64("hysteria2.tls.ech.config", &raw.0).map(Self)
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawEchConfig> for Optional<EchOptions> {
    type Error = HammerError;

    fn try_from(raw: RawEchConfig) -> Result<Self, Self::Error> {
        let RawEchConfig {
            enabled,
            config,
            config_path,
            pq_signature_schemes_enabled,
            dynamic_record_sizing_disabled,
        } = raw;
        let enabled = enabled.unwrap_or(false);
        if !enabled {
            if config.is_some()
                || config_path.is_some()
                || pq_signature_schemes_enabled.is_some()
                || dynamic_record_sizing_disabled.is_some()
            {
                return Err(HammerError::config_validation(
                    "hysteria2.tls.ech.enabled must be true when ECH fields are set",
                ));
            }
            return Ok(Self(None));
        }
        if config.is_some() && config_path.is_some() {
            return Err(HammerError::config_validation(
                "hysteria2.tls.ech.config and config_path cannot both be set",
            ));
        }
        let config_source = match (config, config_path) {
            (Some(config), None) => Some(EchConfigSource::Inline(config.try_into()?)),
            (None, Some(path)) => Some(EchConfigSource::Path(path)),
            (None, None) => Some(EchConfigSource::DnsHttpsRecord),
            (Some(_), Some(_)) => unreachable!("checked above"),
        };
        Ok(Self(Some(EchOptions {
            config_source,
            pq_signature_schemes_enabled: pq_signature_schemes_enabled.unwrap_or(false),
            dynamic_record_sizing_disabled: dynamic_record_sizing_disabled.unwrap_or(false),
        })))
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawRealityPublicKey> for RealityPublicKey {
    type Error = HammerError;

    fn try_from(raw: RawRealityPublicKey) -> Result<Self, Self::Error> {
        let public_key = decode_hex_or_base64("hysteria2.tls.reality.public_key", &raw.0)?;
        let public_key = public_key.try_into().map_err(|_| {
            HammerError::config_validation("hysteria2.tls.reality.public_key must be 32 bytes")
        })?;
        Ok(Self(public_key))
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawRealityShortId> for RealityShortId {
    type Error = HammerError;

    fn try_from(raw: RawRealityShortId) -> Result<Self, Self::Error> {
        let short_id = decode_hex("hysteria2.tls.reality.short_id", &raw.0)?;
        if short_id.len() > 8 {
            return Err(HammerError::config_validation(
                "hysteria2.tls.reality.short_id must be at most 8 bytes",
            ));
        }
        Ok(Self(short_id))
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawRealityConfig> for Optional<RealityOptions> {
    type Error = HammerError;

    fn try_from(raw: RawRealityConfig) -> Result<Self, Self::Error> {
        let enabled = raw.enabled.unwrap_or(false);
        if !enabled {
            if raw.public_key.is_some() || raw.short_id.is_some() {
                return Err(HammerError::config_validation(
                    "hysteria2.tls.reality.enabled must be true when Reality fields are set",
                ));
            }
            return Ok(Self(None));
        }
        let public_key = raw.public_key.ok_or_else(|| {
            HammerError::config_validation("hysteria2.tls.reality.public_key is required")
        })?;
        let short_id = raw.short_id.ok_or_else(|| {
            HammerError::config_validation("hysteria2.tls.reality.short_id is required")
        })?;
        Ok(Self(Some(RealityOptions {
            public_key: public_key.try_into()?,
            short_id: short_id.try_into()?,
        })))
    }
}

#[cfg(feature = "hysteria2")]
impl TryFrom<RawTlsFragmentConfig> for Optional<TlsFragmentOptions> {
    type Error = HammerError;

    fn try_from(raw: RawTlsFragmentConfig) -> Result<Self, Self::Error> {
        let enabled = raw.enabled.unwrap_or(false);
        if !enabled {
            if !raw.size.is_empty() || raw.sleep.is_some() {
                return Err(HammerError::config_validation(
                    "hysteria2.tls.fragment.enabled must be true when fragment fields are set",
                ));
            }
            return Ok(Self(None));
        }
        Ok(Self(Some(TlsFragmentOptions {
            size: if raw.size.is_empty() {
                "tlshello".to_owned()
            } else {
                raw.size
            },
            sleep: raw.sleep.unwrap_or(Duration::ZERO),
        })))
    }
}

#[cfg(feature = "hysteria2")]
fn parse_certificate_chain(raw: &str, field: &str) -> Result<Vec<Vec<u8>>, HammerError> {
    if raw.contains("-----BEGIN ") {
        return parse_pem_blocks(raw, &["CERTIFICATE"], field);
    }
    Ok(vec![decode_hex_or_base64(field, raw)?])
}

#[cfg(feature = "hysteria2")]
fn parse_private_key(raw: &str, field: &str) -> Result<Vec<u8>, HammerError> {
    let keys = if raw.contains("-----BEGIN ") {
        parse_pem_blocks(
            raw,
            &["PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"],
            field,
        )?
    } else {
        vec![decode_hex_or_base64(field, raw)?]
    };
    if keys.len() != 1 {
        return Err(HammerError::config_validation(format!(
            "{field} must contain exactly one private key"
        )));
    }
    Ok(keys.into_iter().next().expect("one key checked"))
}

#[cfg(feature = "hysteria2")]
fn parse_pem_blocks(
    input: &str,
    accepted_labels: &[&str],
    field: &str,
) -> Result<Vec<Vec<u8>>, HammerError> {
    let mut blocks = Vec::new();
    let mut offset = 0;
    while let Some(begin_rel) = input[offset..].find("-----BEGIN ") {
        let begin = offset + begin_rel;
        let label_start = begin + "-----BEGIN ".len();
        let Some(label_end_rel) = input[label_start..].find("-----") else {
            return Err(HammerError::config_validation(format!(
                "{field} contains malformed PEM"
            )));
        };
        let label_end = label_start + label_end_rel;
        let label = &input[label_start..label_end];
        let body_start = label_end + "-----".len();
        let end_marker = format!("-----END {label}-----");
        let Some(end_rel) = input[body_start..].find(&end_marker) else {
            return Err(HammerError::config_validation(format!(
                "{field} contains an unterminated PEM block"
            )));
        };
        let end = body_start + end_rel;
        if accepted_labels.contains(&label) {
            let body = input[body_start..end]
                .lines()
                .map(str::trim)
                .collect::<String>();
            blocks.push(
                base64::engine::general_purpose::STANDARD
                    .decode(body)
                    .map_err(|_| {
                        HammerError::config_validation(format!(
                            "{field} contains invalid PEM base64"
                        ))
                    })?,
            );
        }
        offset = end + end_marker.len();
    }
    if blocks.is_empty() {
        return Err(HammerError::config_validation(format!(
            "{field} contains no supported PEM block"
        )));
    }
    Ok(blocks)
}

#[cfg(feature = "hysteria2")]
fn decode_hex_or_base64(field: &str, value: &str) -> Result<Vec<u8>, HammerError> {
    let compact: String = value
        .chars()
        .filter(|c| !matches!(c, ':' | '-' | ' ' | '\n' | '\r' | '\t'))
        .collect();
    if compact.is_empty() {
        return Err(HammerError::config_validation(format!(
            "{field} must not be empty"
        )));
    }
    if compact.len() % 2 == 0 && compact.chars().all(|c| c.is_ascii_hexdigit()) {
        return decode_hex(field, &compact);
    }
    base64::engine::general_purpose::STANDARD
        .decode(compact)
        .map_err(|_| HammerError::config_validation(format!("{field} must be hex or base64")))
}

#[cfg(feature = "hysteria2")]
fn decode_hex(field: &str, value: &str) -> Result<Vec<u8>, HammerError> {
    let compact: String = value
        .chars()
        .filter(|c| !matches!(c, ':' | '-' | ' ' | '\n' | '\r' | '\t'))
        .collect();
    if compact.len() % 2 != 0 || !compact.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(HammerError::config_validation(format!(
            "{field} must be even-length hex"
        )));
    }
    let mut out = Vec::with_capacity(compact.len() / 2);
    for chunk in compact.as_bytes().chunks_exact(2) {
        let hex = std::str::from_utf8(chunk).expect("hex bytes are valid utf8");
        out.push(u8::from_str_radix(hex, 16).map_err(|_| {
            HammerError::config_validation(format!("{field} contains invalid hex"))
        })?);
    }
    Ok(out)
}

#[cfg(feature = "hysteria2")]
pub(super) fn build_hysteria_options(
    mut raw: RawHysteria2Config,
) -> Result<(Hysteria2OutboundOptions, String), HammerError> {
    let id = if raw.id.is_empty() {
        C::DEFAULT_HYSTERIA_ID.to_owned()
    } else {
        std::mem::take(&mut raw.id)
    };
    if raw.server.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.server is required",
        ));
    }
    let server_port = raw.server_port.unwrap_or(C::DEFAULT_HYSTERIA_PORT);
    if raw.password.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.password is required",
        ));
    }
    if !raw.server_ports.is_empty() || raw.hop_interval.is_some() || raw.hop_interval_max.is_some()
    {
        return Err(HammerError::config_validation(
            "hysteria2 port hopping is not supported yet",
        ));
    }
    let up_mbps = raw.up_mbps.unwrap_or(0);
    let down_mbps = raw.down_mbps.unwrap_or(0);
    if up_mbps < 0 || down_mbps < 0 {
        return Err(HammerError::config_validation(
            "hysteria2.up_mbps and hysteria2.down_mbps must be non-negative",
        ));
    }
    let tls = build_hysteria_tls_options(raw.tls, raw.sni, raw.insecure, &raw.server)?;
    let network = if raw.network.is_empty() {
        vec![Hysteria2Network::Tcp, Hysteria2Network::Udp]
    } else {
        raw.network
    };
    let bbr_profile = raw.bbr_profile.unwrap_or_default();
    let obfs = build_obfs(raw.obfs)?;
    Ok((
        Hysteria2OutboundOptions {
            server: raw.server,
            server_port,
            server_ports: raw.server_ports,
            password: raw.password,
            up_mbps,
            down_mbps,
            network,
            hop_interval: raw.hop_interval,
            hop_interval_max: raw.hop_interval_max,
            idle_timeout: raw.idle_timeout,
            keep_alive_period: raw.keep_alive_period,
            bbr_profile,
            brutal_debug: raw.brutal_debug.unwrap_or(false),
            disable_path_mtu_discovery: raw.disable_path_mtu.unwrap_or(false),
            initial_packet_size: raw.initial_packet_size.unwrap_or(0),
            tls,
            obfs,
        },
        id,
    ))
}

#[cfg(feature = "hysteria2")]
fn build_obfs(raw: RawHysteria2Obfs) -> Result<Option<Hysteria2Obfs>, HammerError> {
    if raw.type_.is_none() && raw.password.is_empty() {
        return Ok(None);
    }
    let Some(type_) = raw.type_ else {
        return Err(HammerError::config_validation(
            "hysteria2.obfs.type and hysteria2.obfs.password must be set together",
        ));
    };
    if raw.password.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.obfs.type and hysteria2.obfs.password must be set together",
        ));
    }
    Ok(Some(Hysteria2Obfs {
        type_,
        password: raw.password,
    }))
}
