use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    #[serde(default, skip_serializing_if = "RawLogConfig::is_default")]
    pub log: RawLogConfig,
    #[serde(default, skip_serializing_if = "RawTunConfig::is_default")]
    pub tun: RawTunConfig,
    #[serde(default, skip_serializing_if = "RawHysteria2Config::is_default")]
    pub hysteria2: RawHysteria2Config,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<RawEndpoint>,
    #[serde(default, skip_serializing_if = "RawDnsConfig::is_default")]
    pub dns: RawDnsConfig,
    #[serde(default, skip_serializing_if = "RawRouteConfig::is_default")]
    pub route: RawRouteConfig,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawLogConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub level: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub timestamp: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
}

impl RawLogConfig {
    fn is_default(&self) -> bool {
        *self == RawLogConfig::default()
    }
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawTunConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub interface_name: String,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub mtu: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stack: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub route_address: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub route_exclude_address: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_route: Option<bool>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub strict_route: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub udp_timeout: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub sniff: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub hijack_dns: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub sniff_override_destination: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sniff_timeout: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub domain_strategy: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub udp_disable_domain_unmapping: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub block_quic: bool,
}

impl RawTunConfig {
    fn is_default(&self) -> bool {
        *self == RawTunConfig::default()
    }
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawHysteria2Config {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub server: String,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub server_port: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub server_ports: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub up_mbps: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub down_mbps: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sni: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub insecure: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub network: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hop_interval: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hop_interval_max: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub idle_timeout: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub keep_alive_period: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bbr_profile: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub brutal_debug: bool,
    #[serde(
        rename = "disable_path_mtu_discovery",
        default,
        skip_serializing_if = "is_false"
    )]
    pub disable_path_mtu: bool,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub initial_packet_size: u16,
    #[serde(default, skip_serializing_if = "RawHysteria2Obfs::is_default")]
    pub obfs: RawHysteria2Obfs,
}

impl RawHysteria2Config {
    fn is_default(&self) -> bool {
        *self == RawHysteria2Config::default()
    }
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawHysteria2Obfs {
    #[serde(default, skip_serializing_if = "String::is_empty", rename = "type")]
    pub type_: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password: String,
}

impl RawHysteria2Obfs {
    fn is_default(&self) -> bool {
        *self == RawHysteria2Obfs::default()
    }
}

/// Outer endpoint variant — sing-box style `[[endpoints]]` entries with a
/// `type` discriminator. Adding a new endpoint protocol (e.g. tailscale) means
/// adding a new variant here without breaking existing TOML files.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawEndpoint {
    Wireguard(RawWireguardEndpoint),
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawWireguardEndpoint {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub private_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtu: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<RawWireguardPeer>,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawWireguardPeer {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub public_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_shared_key: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub address: String,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub port: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_ips: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistent_keepalive_interval: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserved: Option<[u8; 3]>,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawDnsConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub server: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub strategy: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub via: String,
}

impl RawDnsConfig {
    fn is_default(&self) -> bool {
        *self == RawDnsConfig::default()
    }
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawRouteConfig {
    #[serde(rename = "final", default, skip_serializing_if = "String::is_empty")]
    pub final_: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_detect_interface: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<RawRouteRule>,
}

impl RawRouteConfig {
    fn is_default(&self) -> bool {
        *self == RawRouteConfig::default()
    }
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawRouteRule {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inbound: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub protocol: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain_suffix: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain_keyword: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ip_cidr: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub outbound: String,
}

fn is_false(v: &bool) -> bool {
    !*v
}
fn is_zero_u16(v: &u16) -> bool {
    *v == 0
}
fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}
