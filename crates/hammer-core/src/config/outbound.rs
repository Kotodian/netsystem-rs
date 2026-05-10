//! `[hysteria2]` and direct/block outbound config sections.
//!
//! Hysteria2 is the only outbound protocol with its own TOML section
//! (top-level `[hysteria2]`); direct is synthesized by `build_outbounds`.
//! DNS is handled by `DnsRouter`/`DnsTransport`, not as a dialable outbound.
//! `Outbound` / `OutboundKind` sit at this layer so adding a new outbound
//! protocol means dropping a new variant here.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::HammerError;

use super::constants as C;
use super::raw_struct;
#[cfg(feature = "outbound-hysteria2")]
use super::raw_struct_with_default_check;

#[cfg(feature = "outbound-hysteria2")]
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
        pub bbr_profile: Option<Hysteria2BbrProfile> => "Option::is_none",
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

#[cfg(feature = "outbound-hysteria2")]
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
    #[cfg(feature = "outbound-hysteria2")]
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
            #[cfg(feature = "outbound-hysteria2")]
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
    #[cfg(feature = "outbound-hysteria2")]
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

#[cfg(feature = "outbound-hysteria2")]
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
    pub bbr_profile: Hysteria2BbrProfile,
    pub brutal_debug: bool,
    pub disable_path_mtu_discovery: bool,
    pub initial_packet_size: u16,
    pub tls: OutboundTlsOptions,
    pub obfs: Option<Hysteria2Obfs>,
}

#[cfg(feature = "outbound-hysteria2")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Hysteria2Network {
    Tcp,
    Udp,
}

#[cfg(feature = "outbound-hysteria2")]
impl Hysteria2Network {
    pub fn name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[cfg(feature = "outbound-hysteria2")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Hysteria2BbrProfile {
    #[default]
    Standard,
    Conservative,
    Aggressive,
}

#[cfg(feature = "outbound-hysteria2")]
impl Hysteria2BbrProfile {
    pub fn name(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Conservative => "conservative",
            Self::Aggressive => "aggressive",
        }
    }
}

#[cfg(feature = "outbound-hysteria2")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundTlsOptions {
    pub enabled: bool,
    pub server_name: String,
    pub insecure: bool,
}

#[cfg(feature = "outbound-hysteria2")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hysteria2Obfs {
    pub type_: Hysteria2ObfsType,
    pub password: String,
}

#[cfg(feature = "outbound-hysteria2")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Hysteria2ObfsType {
    #[default]
    Salamander,
}

#[cfg(feature = "outbound-hysteria2")]
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
#[cfg(feature = "outbound-hysteria2")]
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
            #[cfg(feature = "outbound-hysteria2")]
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

#[cfg(feature = "outbound-hysteria2")]
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
    let insecure = raw.insecure.unwrap_or(false);
    if !insecure && raw.sni.is_empty() && raw.server.parse::<std::net::IpAddr>().is_err() {
        raw.sni = raw.server.clone();
    }
    if !insecure && raw.sni.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.sni is required unless insecure=true",
        ));
    }
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
            tls: OutboundTlsOptions {
                enabled: true,
                server_name: raw.sni,
                insecure,
            },
            obfs,
        },
        id,
    ))
}

#[cfg(feature = "outbound-hysteria2")]
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
