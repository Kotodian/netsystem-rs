//! `[hysteria2]` and direct/block outbound config sections.
//!
//! Hysteria2 is the only outbound protocol with its own TOML section
//! (top-level `[hysteria2]`); direct/block/dns are synthesized at runtime
//! by `build_outbounds`. `Outbound` / `OutboundKind` sit at this layer so
//! adding a new outbound protocol means dropping a new variant here.

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::error::HammerError;

use super::constants as C;
use super::parse::{self, deserialize_duration_option, is_false, is_zero_i64, is_zero_u16};
use super::raw_struct_with_default_check;

raw_struct_with_default_check! {
    pub struct RawHysteria2Config {
        /// Outbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Hysteria2 server host or IP.
        pub server: String => "String::is_empty",
        /// Single Hysteria2 server port.
        pub server_port: u16 => "is_zero_u16",
        /// Port-hopping range strings from the raw config.
        pub server_ports: Vec<String> => "Vec::is_empty",
        /// Hysteria2 password.
        pub password: String => "String::is_empty",
        /// Upload bandwidth hint in Mbps.
        pub up_mbps: i64 => "is_zero_i64",
        /// Download bandwidth hint in Mbps.
        pub down_mbps: i64 => "is_zero_i64",
        /// TLS SNI override.
        pub sni: String => "String::is_empty",
        /// Whether invalid TLS certificates are accepted.
        pub insecure: bool => "is_false",
        /// Enabled network list from the raw config.
        #[serde(deserialize_with = "deserialize_hysteria2_networks")]
        pub network: Vec<Hysteria2Network> => "Vec::is_empty",
        /// Port-hopping interval.
        #[serde(
            deserialize_with = "deserialize_hysteria2_hop_interval",
            serialize_with = "serialize_duration_option"
        )]
        pub hop_interval: Option<Duration> => "Option::is_none",
        /// Maximum port-hopping interval.
        #[serde(
            deserialize_with = "deserialize_hysteria2_hop_interval_max",
            serialize_with = "serialize_duration_option"
        )]
        pub hop_interval_max: Option<Duration> => "Option::is_none",
        /// QUIC idle timeout.
        #[serde(
            deserialize_with = "deserialize_hysteria2_idle_timeout",
            serialize_with = "serialize_duration_option"
        )]
        pub idle_timeout: Option<Duration> => "Option::is_none",
        /// QUIC keep-alive period.
        #[serde(
            deserialize_with = "deserialize_hysteria2_keep_alive_period",
            serialize_with = "serialize_duration_option"
        )]
        pub keep_alive_period: Option<Duration> => "Option::is_none",
        /// Hysteria2 BBR profile.
        #[serde(deserialize_with = "deserialize_hysteria2_bbr_profile")]
        pub bbr_profile: Option<Hysteria2BbrProfile> => "Option::is_none",
        /// Whether Brutal congestion-control debug output is enabled.
        pub brutal_debug: bool => "is_false",
        /// Whether QUIC path MTU discovery is disabled.
        #[serde(rename = "disable_path_mtu_discovery")]
        pub disable_path_mtu: bool => "is_false",
        /// Initial QUIC datagram size.
        pub initial_packet_size: u16 => "is_zero_u16",
        /// Optional Hysteria2 obfuscation section.
        pub obfs: RawHysteria2Obfs => "RawHysteria2Obfs::is_default",
    }
}

raw_struct_with_default_check! {
    pub struct RawHysteria2Obfs {
        /// Obfuscation type.
        #[serde(rename = "type")]
        #[serde(deserialize_with = "deserialize_hysteria2_obfs_type")]
        pub type_: Option<Hysteria2ObfsType> => "Option::is_none",
        /// Obfuscation password.
        pub password: String => "String::is_empty",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    pub id: String,
    pub kind: OutboundKind,
}

impl Outbound {
    pub fn type_name(&self) -> &'static str {
        match &self.kind {
            OutboundKind::Hysteria2(_) => C::TYPE_HYSTERIA2,
            OutboundKind::Direct(_) => C::TYPE_DIRECT,
            OutboundKind::Block => C::TYPE_BLOCK,
            OutboundKind::Dns => "dns",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundKind {
    Hysteria2(Hysteria2OutboundOptions),
    Direct(DirectOutboundOptions),
    Block,
    Dns,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Hysteria2Network {
    Tcp,
    Udp,
}

impl Hysteria2Network {
    pub fn name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Hysteria2BbrProfile {
    #[default]
    Standard,
    Conservative,
    Aggressive,
}

impl Hysteria2BbrProfile {
    pub fn name(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Conservative => "conservative",
            Self::Aggressive => "aggressive",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundTlsOptions {
    pub enabled: bool,
    pub server_name: String,
    pub insecure: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hysteria2Obfs {
    pub type_: Hysteria2ObfsType,
    pub password: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Hysteria2ObfsType {
    #[default]
    Salamander,
}

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
    let mut server_port = raw.server_port;
    if server_port == 0 && raw.server_ports.is_empty() {
        server_port = C::DEFAULT_HYSTERIA_PORT;
    }
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
    if raw.up_mbps < 0 || raw.down_mbps < 0 {
        return Err(HammerError::config_validation(
            "hysteria2.up_mbps and hysteria2.down_mbps must be non-negative",
        ));
    }
    if !raw.insecure && raw.sni.is_empty() && raw.server.parse::<std::net::IpAddr>().is_err() {
        raw.sni = raw.server.clone();
    }
    if !raw.insecure && raw.sni.is_empty() {
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
            up_mbps: raw.up_mbps,
            down_mbps: raw.down_mbps,
            network,
            hop_interval: raw.hop_interval,
            hop_interval_max: raw.hop_interval_max,
            idle_timeout: raw.idle_timeout,
            keep_alive_period: raw.keep_alive_period,
            bbr_profile,
            brutal_debug: raw.brutal_debug,
            disable_path_mtu_discovery: raw.disable_path_mtu,
            initial_packet_size: raw.initial_packet_size,
            tls: OutboundTlsOptions {
                enabled: true,
                server_name: raw.sni,
                insecure: raw.insecure,
            },
            obfs,
        },
        id,
    ))
}

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

fn deserialize_hysteria2_networks<'de, D>(
    deserializer: D,
) -> Result<Vec<Hysteria2Network>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(|value| match value.as_str() {
            "tcp" => Ok(Hysteria2Network::Tcp),
            "udp" => Ok(Hysteria2Network::Udp),
            other => Err(de::Error::custom(format!(
                "hysteria2.network: unknown network {other:?}"
            ))),
        })
        .collect()
}

fn deserialize_hysteria2_hop_interval<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("hysteria2.hop_interval", deserializer)
}

fn deserialize_hysteria2_hop_interval_max<'de, D>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("hysteria2.hop_interval_max", deserializer)
}

fn deserialize_hysteria2_idle_timeout<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("hysteria2.idle_timeout", deserializer)
}

fn deserialize_hysteria2_keep_alive_period<'de, D>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("hysteria2.keep_alive_period", deserializer)
}

fn deserialize_hysteria2_bbr_profile<'de, D>(
    deserializer: D,
) -> Result<Option<Hysteria2BbrProfile>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "" => Ok(None),
        "standard" => Ok(Some(Hysteria2BbrProfile::Standard)),
        "conservative" => Ok(Some(Hysteria2BbrProfile::Conservative)),
        "aggressive" => Ok(Some(Hysteria2BbrProfile::Aggressive)),
        other => Err(de::Error::custom(format!(
            "hysteria2.bbr_profile unsupported BBR profile: {other}"
        ))),
    }
}

fn deserialize_hysteria2_obfs_type<'de, D>(
    deserializer: D,
) -> Result<Option<Hysteria2ObfsType>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "" => Ok(None),
        "salamander" => Ok(Some(Hysteria2ObfsType::Salamander)),
        other => Err(de::Error::custom(format!(
            "hysteria2.obfs.type: unknown obfs type {other:?}"
        ))),
    }
}

fn serialize_duration_option<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    parse::serialize_duration_option(value, serializer)
}
