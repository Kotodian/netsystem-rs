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

#[cfg(any(feature = "hysteria2", feature = "vless"))]
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

#[cfg(feature = "vless")]
raw_struct! {
    pub struct RawVlessConfig {
        /// Outbound id used by route rules.
        pub id: String => "String::is_empty",
        /// VLESS server host or IP.
        pub server: String => "String::is_empty",
        /// VLESS server port.
        pub server_port: Option<u16> => "Option::is_none",
        /// VLESS user UUID.
        pub uuid: String => "String::is_empty",
        /// Optional VLESS flow, currently empty or xtls-rprx-vision.
        pub flow: String => "String::is_empty",
        /// Enabled network list. Defaults to tcp+udp.
        pub network: Vec<crate::Network> => "Vec::is_empty",
        /// Optional sing-box-style TLS section.
        pub tls: RawOutboundTlsConfig => "RawOutboundTlsConfig::is_default",
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

#[cfg(any(feature = "hysteria2", feature = "vless"))]
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
    #[cfg(feature = "vless")]
    Vless(RawVlessConfig),
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
            #[cfg(feature = "vless")]
            OutboundKind::Vless(_) => C::TYPE_VLESS,
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
    #[cfg(feature = "vless")]
    Vless(VlessOutboundOptions),
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

#[cfg(feature = "vless")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessOutboundOptions {
    pub server: String,
    pub server_port: u16,
    pub uuid: [u8; 16],
    pub flow: Option<String>,
    pub network: Vec<crate::Network>,
    pub tls: OutboundTlsOptions,
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
            #[cfg(feature = "vless")]
            RawOutbound::Vless(raw) => {
                let (options, id) = build_vless_options(idx, raw)?;
                Outbound {
                    id,
                    kind: OutboundKind::Vless(options),
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

#[cfg(feature = "vless")]
fn build_vless_options(
    idx: usize,
    raw: RawVlessConfig,
) -> Result<(VlessOutboundOptions, String), HammerError> {
    let RawVlessConfig {
        id,
        server,
        server_port,
        uuid,
        flow,
        network,
        tls,
    } = raw;
    if id.is_empty() {
        return Err(HammerError::config_validation(format!(
            "outbounds[{idx}].id is required"
        )));
    }
    let prefix = format!("outbounds[{idx}] (vless '{id}')");
    if server.is_empty() {
        return Err(HammerError::config_validation(format!(
            "{prefix}.server is required"
        )));
    }
    let uuid = parse_vless_uuid(&format!("outbounds[{idx}].uuid"), &uuid)?;
    let flow = match flow.as_str() {
        "" => None,
        "xtls-rprx-vision" => Some(flow),
        unsupported => {
            return Err(HammerError::config_validation(format!(
                "{prefix} unsupported flow: {unsupported}"
            )));
        }
    };
    let tls = OutboundTlsOptionsBuilder::new(&prefix, tls)
        .server(&server)
        .policy(OutboundTlsBuildPolicy::tcp_stream())
        .build()?;
    if flow.as_deref() == Some("xtls-rprx-vision") && !tls.enabled {
        return Err(HammerError::config_validation(format!(
            "{prefix} flow xtls-rprx-vision requires tls.enabled=true"
        )));
    }
    let network = build_tcp_udp_networks(&format!("{prefix}.network"), network)?;
    if flow.as_deref() == Some("xtls-rprx-vision")
        && !matches!(network.as_slice(), [crate::Network::Tcp])
    {
        return Err(HammerError::config_validation(format!(
            "{prefix} flow xtls-rprx-vision supports only tcp network"
        )));
    }
    Ok((
        VlessOutboundOptions {
            server,
            server_port: server_port.unwrap_or(C::DEFAULT_VLESS_PORT),
            uuid,
            flow,
            network,
            tls,
        },
        id,
    ))
}

#[cfg(feature = "vless")]
fn build_tcp_udp_networks(
    field: &str,
    networks: Vec<crate::Network>,
) -> Result<Vec<crate::Network>, HammerError> {
    let networks = if networks.is_empty() {
        vec![crate::Network::Tcp, crate::Network::Udp]
    } else {
        networks
    };
    let mut has_tcp = false;
    let mut has_udp = false;
    for network in &networks {
        match network {
            crate::Network::Tcp if has_tcp => {
                return Err(HammerError::config_validation(format!(
                    "{field} lists duplicate network: tcp"
                )));
            }
            crate::Network::Tcp => has_tcp = true,
            crate::Network::Udp if has_udp => {
                return Err(HammerError::config_validation(format!(
                    "{field} lists duplicate network: udp"
                )));
            }
            crate::Network::Udp => has_udp = true,
            crate::Network::Icmp => {
                return Err(HammerError::config_validation(format!(
                    "{field} supports only tcp and udp"
                )));
            }
        }
    }
    Ok(networks)
}

#[cfg(feature = "vless")]
fn parse_vless_uuid(field: &str, raw: &str) -> Result<[u8; 16], HammerError> {
    decode_hex(field, raw)?
        .try_into()
        .map_err(|_| HammerError::config_validation(format!("{field} must be 16 bytes")))
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
    OutboundTlsOptionsBuilder::new("hysteria2", raw)
        .server(server)
        .legacy(TlsLegacyOptions {
            server_name: legacy_sni,
            insecure: legacy_insecure,
        })
        .policy(OutboundTlsBuildPolicy::hysteria2())
        .build()
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
struct OutboundTlsOptionsBuilder<'a> {
    prefix: &'a str,
    raw: RawOutboundTlsConfig,
    server: &'a str,
    legacy: TlsLegacyOptions,
    policy: OutboundTlsBuildPolicy,
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
impl<'a> OutboundTlsOptionsBuilder<'a> {
    fn new(prefix: &'a str, raw: RawOutboundTlsConfig) -> Self {
        Self {
            prefix,
            raw,
            server: "",
            legacy: TlsLegacyOptions::default(),
            policy: OutboundTlsBuildPolicy::default(),
        }
    }

    fn server(mut self, server: &'a str) -> Self {
        self.server = server;
        self
    }

    #[cfg(feature = "hysteria2")]
    fn legacy(mut self, legacy: TlsLegacyOptions) -> Self {
        self.legacy = legacy;
        self
    }

    fn policy(mut self, policy: OutboundTlsBuildPolicy) -> Self {
        self.policy = policy;
        self
    }

    fn build(self) -> Result<OutboundTlsOptions, HammerError> {
        let Self {
            prefix,
            raw,
            server,
            legacy,
            policy,
        } = self;
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

        let tls_fields_set = tls_fields_set(TlsFieldState {
            server_name: &tls_server_name,
            insecure: tls_insecure,
            alpn: &alpn,
            min_version,
            max_version,
            server_fingerprint: &server_fingerprint,
            client_certificate: &client_certificate,
            client_certificate_path: &client_certificate_path,
            client_key: client_key.as_ref(),
            client_key_path: client_key_path.as_ref(),
            utls: &utls,
            ech: &ech,
            reality: &reality,
            fragment: &fragment,
            record_fragment: &record_fragment,
        });
        let enabled = match (policy.required, enabled) {
            (true, Some(false)) => {
                return Err(HammerError::config_validation(format!(
                    "{prefix}.tls.enabled=false is not supported"
                )));
            }
            (true, _) => true,
            (false, Some(enabled)) => enabled,
            (false, None) => false,
        };
        if !enabled {
            if tls_fields_set {
                return Err(HammerError::config_validation(format!(
                    "{prefix}.tls.enabled must be true when TLS fields are set"
                )));
            }
            return Ok(OutboundTlsOptions::default());
        }

        let mut server_name = merge_tls_server_name(prefix, legacy.server_name, tls_server_name)?;
        let insecure = merge_tls_insecure(prefix, legacy.insecure, tls_insecure)?;
        if policy.default_server_name_from_server
            && !insecure
            && server_name.is_empty()
            && server.parse::<std::net::IpAddr>().is_err()
        {
            server_name = server.to_owned();
        }
        if !insecure && server_name.is_empty() {
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls.server_name is required unless insecure=true"
            )));
        }
        if policy.tls13_only {
            validate_tls13_only(prefix, min_version, max_version)?;
        }
        match policy.alpn {
            AlpnPolicy::Any => validate_tls_alpn(prefix, &alpn)?,
            #[cfg(feature = "hysteria2")]
            AlpnPolicy::H3Only => validate_h3_alpn(prefix, &alpn)?,
        }

        let server_fingerprints = server_fingerprint
            .into_iter()
            .map(|fingerprint| {
                parse_certificate_fingerprint_for_field(
                    &format!("{prefix}.tls.server_fingerprint"),
                    &fingerprint.0,
                )
            })
            .collect::<Result<Vec<CertificateFingerprint>, _>>()?;
        let client_auth = build_client_tls_auth_for_prefix(
            prefix,
            client_certificate,
            client_certificate_path,
            client_key,
            client_key_path,
        )?;
        let utls = build_utls_options_for_prefix(prefix, utls)?;
        let ech = build_ech_options_for_prefix(prefix, ech)?;
        let reality = build_reality_options_for_prefix(prefix, reality)?;
        let fragment = build_tls_fragment_options_for_prefix(prefix, fragment)?;
        if !policy.allow_reality && reality.is_some() {
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls.reality requires a Reality-capable outbound such as VLESS"
            )));
        }
        let record_fragment = record_fragment.enabled.unwrap_or(false);
        if !policy.allow_fragment && (fragment.is_some() || record_fragment) {
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls fragmentation is only valid for TCP TLS streams"
            )));
        }

        Ok(OutboundTlsOptions {
            enabled: true,
            server_name,
            insecure,
            alpn,
            min_version,
            max_version,
            server_fingerprints,
            client_auth,
            utls,
            ech,
            reality,
            fragment,
            record_fragment,
        })
    }
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
#[derive(Default)]
struct TlsLegacyOptions {
    server_name: String,
    insecure: Option<bool>,
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
#[derive(Debug, Clone, Copy)]
struct OutboundTlsBuildPolicy {
    required: bool,
    default_server_name_from_server: bool,
    tls13_only: bool,
    alpn: AlpnPolicy,
    allow_reality: bool,
    allow_fragment: bool,
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
impl Default for OutboundTlsBuildPolicy {
    fn default() -> Self {
        Self {
            required: false,
            default_server_name_from_server: false,
            tls13_only: false,
            alpn: AlpnPolicy::Any,
            allow_reality: false,
            allow_fragment: false,
        }
    }
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
impl OutboundTlsBuildPolicy {
    #[cfg(feature = "hysteria2")]
    fn hysteria2() -> Self {
        Self {
            required: true,
            default_server_name_from_server: true,
            tls13_only: true,
            alpn: AlpnPolicy::H3Only,
            allow_reality: true,
            allow_fragment: true,
        }
    }

    #[cfg(feature = "vless")]
    fn tcp_stream() -> Self {
        Self {
            required: false,
            default_server_name_from_server: true,
            tls13_only: false,
            alpn: AlpnPolicy::Any,
            allow_reality: true,
            allow_fragment: true,
        }
    }
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
#[derive(Debug, Clone, Copy)]
enum AlpnPolicy {
    Any,
    #[cfg(feature = "hysteria2")]
    H3Only,
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
struct TlsFieldState<'a> {
    server_name: &'a str,
    insecure: Option<bool>,
    alpn: &'a [String],
    min_version: Option<TlsVersion>,
    max_version: Option<TlsVersion>,
    server_fingerprint: &'a [RawCertificateFingerprint],
    client_certificate: &'a [RawCertificate],
    client_certificate_path: &'a [PathBuf],
    client_key: Option<&'a RawPrivateKey>,
    client_key_path: Option<&'a PathBuf>,
    utls: &'a RawUtlsConfig,
    ech: &'a RawEchConfig,
    reality: &'a RawRealityConfig,
    fragment: &'a RawTlsFragmentConfig,
    record_fragment: &'a RawTlsRecordFragmentConfig,
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn tls_fields_set(state: TlsFieldState<'_>) -> bool {
    !state.server_name.is_empty()
        || state.insecure.is_some()
        || !state.alpn.is_empty()
        || state.min_version.is_some()
        || state.max_version.is_some()
        || !state.server_fingerprint.is_empty()
        || !state.client_certificate.is_empty()
        || !state.client_certificate_path.is_empty()
        || state.client_key.is_some()
        || state.client_key_path.is_some()
        || !state.utls.is_default()
        || !state.ech.is_default()
        || !state.reality.is_default()
        || !state.fragment.is_default()
        || !state.record_fragment.is_default()
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
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

#[cfg(any(feature = "hysteria2", feature = "vless"))]
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

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn validate_tls13_only(
    prefix: &str,
    min_version: Option<TlsVersion>,
    max_version: Option<TlsVersion>,
) -> Result<(), HammerError> {
    if min_version == Some(TlsVersion::Tls12) || max_version == Some(TlsVersion::Tls12) {
        return Err(HammerError::config_validation(format!(
            "{prefix}.tls only supports TLS 1.3"
        )));
    }
    Ok(())
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn validate_tls_alpn(prefix: &str, alpn: &[String]) -> Result<(), HammerError> {
    if alpn.iter().any(String::is_empty) {
        return Err(HammerError::config_validation(format!(
            "{prefix}.tls.alpn must not contain empty protocols"
        )));
    }
    Ok(())
}

#[cfg(feature = "hysteria2")]
fn validate_h3_alpn(prefix: &str, alpn: &[String]) -> Result<(), HammerError> {
    validate_tls_alpn(prefix, alpn)?;
    if !alpn.is_empty() && (alpn.len() != 1 || alpn[0] != "h3") {
        return Err(HammerError::config_validation(format!(
            "{prefix}.tls.alpn must be [\"h3\"]"
        )));
    }
    Ok(())
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn build_client_tls_auth_for_prefix(
    prefix: &str,
    certificates: Vec<RawCertificate>,
    certificate_paths: Vec<PathBuf>,
    key: Option<RawPrivateKey>,
    key_path: Option<PathBuf>,
) -> Result<Option<ClientTlsAuth>, HammerError> {
    let mut certificate_sources = Vec::new();
    for certificate in certificates {
        let certificates =
            parse_certificate_chain(&certificate.0, &format!("{prefix}.tls.client_certificate"))?;
        certificate_sources.extend(
            certificates
                .into_iter()
                .map(|der| CertificateSource::Inline(CertificateDerBytes(der))),
        );
    }
    for path in certificate_paths {
        if path.as_os_str().is_empty() {
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls.client_certificate_path must not contain empty paths"
            )));
        }
        certificate_sources.push(CertificateSource::Path(path));
    }

    let key_source = match (key, key_path) {
        (Some(_), Some(_)) => {
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls.client_key and client_key_path cannot both be set"
            )));
        }
        (Some(key), None) => Some(PrivateKeySource::Inline(PrivateKeyDerBytes(
            parse_private_key(&key.0, &format!("{prefix}.tls.client_key"))?,
        ))),
        (None, Some(path)) => {
            if path.as_os_str().is_empty() {
                return Err(HammerError::config_validation(format!(
                    "{prefix}.tls.client_key_path must not be empty"
                )));
            }
            Some(PrivateKeySource::Path(path))
        }
        (None, None) => None,
    };

    match (certificate_sources.is_empty(), key_source) {
        (true, None) => Ok(None),
        (true, Some(_)) => Err(HammerError::config_validation(format!(
            "{prefix}.tls.client_certificate is required when client_key is set"
        ))),
        (false, None) => Err(HammerError::config_validation(format!(
            "{prefix}.tls.client_key is required when client_certificate is set"
        ))),
        (false, Some(key)) => Ok(Some(ClientTlsAuth {
            certificates: certificate_sources,
            key,
        })),
    }
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn build_utls_options_for_prefix(
    prefix: &str,
    raw: RawUtlsConfig,
) -> Result<Option<UtlsOptions>, HammerError> {
    let enabled = raw.enabled.unwrap_or(false);
    if !enabled {
        if raw.fingerprint.is_some() {
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls.utls.enabled must be true when fingerprint is set"
            )));
        }
        return Ok(None);
    }
    Ok(Some(UtlsOptions {
        fingerprint: raw.fingerprint.unwrap_or_default(),
    }))
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn build_ech_options_for_prefix(
    prefix: &str,
    raw: RawEchConfig,
) -> Result<Option<EchOptions>, HammerError> {
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
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls.ech.enabled must be true when ECH fields are set"
            )));
        }
        return Ok(None);
    }
    if config.is_some() && config_path.is_some() {
        return Err(HammerError::config_validation(format!(
            "{prefix}.tls.ech.config and config_path cannot both be set"
        )));
    }
    let config_source = match (config, config_path) {
        (Some(config), None) => Some(EchConfigSource::Inline(EchConfigList(
            decode_hex_or_base64(&format!("{prefix}.tls.ech.config"), &config.0)?,
        ))),
        (None, Some(path)) => Some(EchConfigSource::Path(path)),
        (None, None) => Some(EchConfigSource::DnsHttpsRecord),
        (Some(_), Some(_)) => unreachable!("checked above"),
    };
    Ok(Some(EchOptions {
        config_source,
        pq_signature_schemes_enabled: pq_signature_schemes_enabled.unwrap_or(false),
        dynamic_record_sizing_disabled: dynamic_record_sizing_disabled.unwrap_or(false),
    }))
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn build_reality_options_for_prefix(
    prefix: &str,
    raw: RawRealityConfig,
) -> Result<Option<RealityOptions>, HammerError> {
    let enabled = raw.enabled.unwrap_or(false);
    if !enabled {
        if raw.public_key.is_some() || raw.short_id.is_some() {
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls.reality.enabled must be true when Reality fields are set"
            )));
        }
        return Ok(None);
    }
    let public_key = raw.public_key.ok_or_else(|| {
        HammerError::config_validation(format!("{prefix}.tls.reality.public_key is required"))
    })?;
    let public_key =
        decode_hex_or_base64(&format!("{prefix}.tls.reality.public_key"), &public_key.0)?;
    let public_key = public_key.try_into().map_err(|_| {
        HammerError::config_validation(format!("{prefix}.tls.reality.public_key must be 32 bytes"))
    })?;
    let short_id = raw.short_id.ok_or_else(|| {
        HammerError::config_validation(format!("{prefix}.tls.reality.short_id is required"))
    })?;
    let short_id = decode_hex(&format!("{prefix}.tls.reality.short_id"), &short_id.0)?;
    if short_id.len() > 8 {
        return Err(HammerError::config_validation(format!(
            "{prefix}.tls.reality.short_id must be at most 8 bytes"
        )));
    }
    Ok(Some(RealityOptions {
        public_key: RealityPublicKey(public_key),
        short_id: RealityShortId(short_id),
    }))
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn build_tls_fragment_options_for_prefix(
    prefix: &str,
    raw: RawTlsFragmentConfig,
) -> Result<Option<TlsFragmentOptions>, HammerError> {
    let enabled = raw.enabled.unwrap_or(false);
    if !enabled {
        if !raw.size.is_empty() || raw.sleep.is_some() {
            return Err(HammerError::config_validation(format!(
                "{prefix}.tls.fragment.enabled must be true when fragment fields are set"
            )));
        }
        return Ok(None);
    }
    Ok(Some(TlsFragmentOptions {
        size: if raw.size.is_empty() {
            "tlshello".to_owned()
        } else {
            raw.size
        },
        sleep: raw.sleep.unwrap_or(Duration::ZERO),
    }))
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn parse_certificate_fingerprint_for_field(
    field: &str,
    raw: &str,
) -> Result<CertificateFingerprint, HammerError> {
    let Some((algorithm, digest)) = raw.split_once('/') else {
        return Err(HammerError::config_validation(format!(
            "{field} entries must use sha256/<digest>"
        )));
    };
    if !algorithm.eq_ignore_ascii_case("sha256") {
        return Err(HammerError::config_validation(format!(
            "{field} only supports sha256"
        )));
    }
    let digest = decode_hex_or_base64(field, digest)?;
    if digest.len() != 32 {
        return Err(HammerError::config_validation(format!(
            "{field} sha256 digest must be 32 bytes"
        )));
    }
    Ok(CertificateFingerprint {
        algorithm: CertificateFingerprintAlgorithm::Sha256,
        digest,
    })
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
fn parse_certificate_chain(raw: &str, field: &str) -> Result<Vec<Vec<u8>>, HammerError> {
    if raw.contains("-----BEGIN ") {
        return parse_pem_blocks(raw, &["CERTIFICATE"], field);
    }
    Ok(vec![decode_hex_or_base64(field, raw)?])
}

#[cfg(any(feature = "hysteria2", feature = "vless"))]
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

#[cfg(any(feature = "hysteria2", feature = "vless"))]
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

#[cfg(any(feature = "hysteria2", feature = "vless"))]
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

#[cfg(any(feature = "hysteria2", feature = "vless"))]
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
