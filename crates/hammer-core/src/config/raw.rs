use serde::{Deserialize, Serialize};

// Raw config structs are mostly serde field declarations with the same
// `default + skip_serializing_if` pattern. Keep the actual field list explicit,
// but let the macro own the repetitive attributes and derives.
macro_rules! raw_struct {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field_vis:vis $field:ident : $ty:ty => $skip:literal
            ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        $vis struct $name {
            $(
                $(#[$field_meta])*
                #[serde(default, skip_serializing_if = $skip)]
                $field_vis $field: $ty,
            )*
        }
    };
}

// Top-level sections are skipped when they are exactly default. This wraps
// `raw_struct!` and adds the small helper serde calls from `RawConfig`.
macro_rules! raw_struct_with_default_check {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field_vis:vis $field:ident : $ty:ty => $skip:literal
            ),* $(,)?
        }
    ) => {
        raw_struct! {
            $(#[$meta])*
            $vis struct $name {
                $(
                    $(#[$field_meta])*
                    $field_vis $field: $ty => $skip,
                )*
            }
        }

        impl $name {
            fn is_default(&self) -> bool {
                *self == $name::default()
            }
        }
    };
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    /// Optional logging section.
    #[serde(default, skip_serializing_if = "RawLogConfig::is_default")]
    pub log: RawLogConfig,
    /// Optional TUN inbound section.
    #[serde(default, skip_serializing_if = "RawTunConfig::is_default")]
    pub tun: RawTunConfig,
    /// Optional top-level Hysteria2 outbound section.
    #[serde(default, skip_serializing_if = "RawHysteria2Config::is_default")]
    pub hysteria2: RawHysteria2Config,
    /// Optional sing-box style endpoint list.
    #[cfg(feature = "wireguard")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<RawEndpoint>,
    /// Optional DNS transport section.
    #[serde(default, skip_serializing_if = "RawDnsConfig::is_default")]
    pub dns: RawDnsConfig,
    /// Optional route section.
    #[serde(default, skip_serializing_if = "RawRouteConfig::is_default")]
    pub route: RawRouteConfig,
}

raw_struct_with_default_check! {
    pub struct RawLogConfig {
        /// Minimum log level, for example `debug`, `info`, or `warn`.
        pub level: String => "String::is_empty",
        /// Log output target from the user config.
        pub output: String => "String::is_empty",
        /// Whether log lines should include timestamps.
        pub timestamp: bool => "is_false",
        /// Whether logging is disabled entirely.
        pub disabled: bool => "is_false",
    }
}

raw_struct_with_default_check! {
    pub struct RawTunConfig {
        /// Inbound tag used by route rules.
        pub id: String => "String::is_empty",
        /// Requested TUN interface name.
        pub interface_name: String => "String::is_empty",
        /// TUN MTU.
        pub mtu: u32 => "is_zero_u32",
        /// Packet stack mode, for example `system` or `disabled`.
        pub stack: String => "String::is_empty",
        /// Local interface addresses in CIDR form.
        pub address: Vec<String> => "Vec::is_empty",
        /// Routes included through the tunnel.
        pub route_address: Vec<String> => "Vec::is_empty",
        /// Routes excluded from the tunnel.
        pub route_exclude_address: Vec<String> => "Vec::is_empty",
        /// Whether the platform should install routes automatically.
        pub auto_route: Option<bool> => "Option::is_none",
        /// Whether route installation should use strict routing semantics.
        pub strict_route: bool => "is_false",
        /// UDP idle timeout string.
        pub udp_timeout: String => "String::is_empty",
        /// Whether protocol/domain sniffing is enabled.
        pub sniff: bool => "is_false",
        /// Whether DNS packets should be intercepted by the DNS router.
        pub hijack_dns: bool => "is_false",
        /// Whether sniffed destinations replace the original destination.
        pub sniff_override_destination: bool => "is_false",
        /// Sniffing timeout string.
        pub sniff_timeout: String => "String::is_empty",
        /// Domain resolution strategy for sniffed/routed traffic.
        pub domain_strategy: String => "String::is_empty",
        /// Whether UDP domain unmapping is disabled for this inbound.
        pub udp_disable_domain_unmapping: bool => "is_false",
        /// Whether detected QUIC traffic should be rejected.
        pub block_quic: bool => "is_false",
    }
}

raw_struct_with_default_check! {
    pub struct RawHysteria2Config {
        /// Outbound tag used by route rules.
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
        pub network: Vec<String> => "Vec::is_empty",
        /// Port-hopping interval string.
        pub hop_interval: String => "String::is_empty",
        /// Maximum port-hopping interval string.
        pub hop_interval_max: String => "String::is_empty",
        /// QUIC idle timeout string.
        pub idle_timeout: String => "String::is_empty",
        /// QUIC keep-alive period string.
        pub keep_alive_period: String => "String::is_empty",
        /// Hysteria2 BBR profile name.
        pub bbr_profile: String => "String::is_empty",
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
        pub type_: String => "String::is_empty",
        /// Obfuscation password.
        pub password: String => "String::is_empty",
    }
}

/// Outer endpoint variant — sing-box style `[[endpoints]]` entries with a
/// `type` discriminator. Adding a new endpoint protocol (e.g. tailscale) means
/// adding a new variant here without breaking existing TOML files.
#[cfg(feature = "wireguard")]
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawEndpoint {
    /// WireGuard endpoint entry.
    Wireguard(RawWireguardEndpoint),
}

raw_struct! {
    #[cfg(feature = "wireguard")]
    pub struct RawWireguardEndpoint {
        /// Endpoint tag used by route rules and lifecycle managers.
        pub id: String => "String::is_empty",
        /// Base64-encoded WireGuard private key.
        pub private_key: String => "String::is_empty",
        /// Optional UDP listen port.
        pub listen_port: Option<u16> => "Option::is_none",
        /// Optional WireGuard interface MTU.
        pub mtu: Option<u32> => "Option::is_none",
        /// Local WireGuard interface addresses in CIDR form.
        pub address: Vec<String> => "Vec::is_empty",
        /// WireGuard peer list.
        pub peers: Vec<RawWireguardPeer> => "Vec::is_empty",
    }
}

raw_struct! {
    #[cfg(feature = "wireguard")]
    pub struct RawWireguardPeer {
        /// Base64-encoded peer public key.
        pub public_key: String => "String::is_empty",
        /// Optional base64-encoded pre-shared key.
        pub pre_shared_key: Option<String> => "Option::is_none",
        /// Peer endpoint address; currently must be an IP literal.
        pub address: String => "String::is_empty",
        /// Peer endpoint UDP port.
        pub port: u16 => "is_zero_u16",
        /// Allowed IP prefixes routed to this peer.
        pub allowed_ips: Vec<String> => "Vec::is_empty",
        /// Optional persistent keepalive interval in seconds.
        pub persistent_keepalive_interval: Option<u32> => "Option::is_none",
        /// Optional reserved WARP-style header bytes.
        pub reserved: Option<[u8; 3]> => "Option::is_none",
    }
}

raw_struct_with_default_check! {
    pub struct RawDnsConfig {
        /// DNS transport tag.
        pub id: String => "String::is_empty",
        /// Upstream DNS server URL or address.
        pub server: String => "String::is_empty",
        /// DNS answer selection strategy.
        pub strategy: String => "String::is_empty",
        /// Outbound tag used to reach the upstream DNS server.
        pub via: String => "String::is_empty",
    }
}

raw_struct_with_default_check! {
    pub struct RawRouteConfig {
        /// Final outbound tag.
        #[serde(rename = "final")]
        pub final_: String => "String::is_empty",
        /// Whether to let the runtime detect the platform default interface.
        pub auto_detect_interface: Option<bool> => "Option::is_none",
        /// Ordered user route rules.
        pub rules: Vec<RawRouteRule> => "Vec::is_empty",
    }
}

raw_struct! {
    pub struct RawRouteRule {
        /// Inbound tag matchers.
        pub inbound: Vec<String> => "Vec::is_empty",
        /// Protocol matchers, for example `dns`, `quic`, or `http`.
        pub protocol: Vec<String> => "Vec::is_empty",
        /// Exact domain matchers.
        pub domain: Vec<String> => "Vec::is_empty",
        /// Domain suffix matchers.
        pub domain_suffix: Vec<String> => "Vec::is_empty",
        /// Domain keyword matchers.
        pub domain_keyword: Vec<String> => "Vec::is_empty",
        /// IP CIDR matchers.
        pub ip_cidr: Vec<String> => "Vec::is_empty",
        /// Route target outbound tag.
        pub outbound: String => "String::is_empty",
    }
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
