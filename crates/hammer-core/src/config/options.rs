#[cfg(feature = "wireguard")]
use std::net::SocketAddr;
use std::time::Duration;

use ipnet::IpNet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub log: LogOptions,
    pub dns: DnsOptions,
    pub inbounds: Vec<Inbound>,
    pub outbounds: Vec<Outbound>,
    #[cfg(feature = "wireguard")]
    pub endpoints: Vec<Endpoint>,
    pub route: RouteOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogOptions {
    pub disabled: bool,
    pub level: String,
    pub output: String,
    pub timestamp: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inbound {
    pub tag: String,
    pub kind: InboundKind,
}

impl Inbound {
    pub fn type_name(&self) -> &'static str {
        match self.kind {
            InboundKind::Tun(_) => "tun",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundKind {
    Tun(TunInboundOptions),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TunInboundOptions {
    pub interface_name: String,
    pub mtu: u32,
    pub address: Vec<Prefix>,
    pub route_address: Vec<Prefix>,
    pub route_exclude_address: Vec<Prefix>,
    pub auto_route: bool,
    pub strict_route: bool,
    pub stack: String,
    pub udp_timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    pub tag: String,
    pub kind: OutboundKind,
}

impl Outbound {
    pub fn type_name(&self) -> &'static str {
        match &self.kind {
            OutboundKind::Hysteria2(_) => "hysteria2",
            OutboundKind::Direct(_) => "direct",
            OutboundKind::Block => "block",
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

/// `[[endpoints]]` element — protocols that maintain long-lived state and
/// participate in the lifecycle alongside outbounds. Mirrors the sing-box
/// 1.11+ endpoint concept: `Endpoint = Outbound + Lifecycle` (see
/// `crates/hammer-adapter/src/endpoint.rs`).
#[cfg(feature = "wireguard")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub tag: String,
    pub kind: EndpointKind,
}

#[cfg(feature = "wireguard")]
impl Endpoint {
    pub fn type_name(&self) -> &'static str {
        match &self.kind {
            EndpointKind::Wireguard(_) => constants::TYPE_WIREGUARD,
        }
    }
}

#[cfg(feature = "wireguard")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointKind {
    Wireguard(WireguardEndpointOptions),
}

#[cfg(feature = "wireguard")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireguardEndpointOptions {
    /// Static private key (Curve25519, 32 bytes).
    pub private_key: [u8; 32],
    /// Local UDP listen port; `0` lets the OS pick.
    pub listen_port: u16,
    /// Tunnel MTU advertised to the inner stack. sing-box default is 1408.
    pub mtu: u32,
    /// Local addresses inside the tunnel (CIDR form).
    pub address: Vec<IpNet>,
    pub peers: Vec<WireguardPeerOptions>,
}

#[cfg(feature = "wireguard")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireguardPeerOptions {
    /// Peer static public key (Curve25519, 32 bytes).
    pub public_key: [u8; 32],
    /// Optional pre-shared key (32 bytes) for additional symmetric mixing.
    pub pre_shared_key: Option<[u8; 32]>,
    /// Resolved peer endpoint. Hostname-only entries are resolved during
    /// endpoint lifecycle Start, not at config parse time.
    pub endpoint: SocketAddr,
    pub allowed_ips: Vec<IpNet>,
    /// `None` disables persistent keepalive.
    pub persistent_keepalive: Option<Duration>,
    /// First three reserved bytes of every WireGuard packet — non-zero values
    /// are how Cloudflare WARP demuxes traffic per-connection.
    pub reserved: [u8; 3],
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hysteria2OutboundOptions {
    pub server: String,
    pub server_port: u16,
    pub server_ports: Vec<String>,
    pub password: String,
    pub up_mbps: i64,
    pub down_mbps: i64,
    pub network: String,
    pub hop_interval: Option<Duration>,
    pub hop_interval_max: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub keep_alive_period: Option<Duration>,
    pub bbr_profile: String,
    pub brutal_debug: bool,
    pub disable_path_mtu_discovery: bool,
    pub initial_packet_size: u16,
    pub tls: OutboundTlsOptions,
    pub obfs: Option<Hysteria2Obfs>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundTlsOptions {
    pub enabled: bool,
    pub server_name: String,
    pub insecure: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hysteria2Obfs {
    pub type_: String,
    pub password: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirectOutboundOptions {
    pub network_strategy: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsOptions {
    pub servers: Vec<DnsServer>,
    pub final_: String,
    pub strategy: DomainStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsServer {
    pub tag: String,
    pub kind: DnsServerKind,
}

impl DnsServer {
    pub fn via(&self) -> &str {
        match &self.kind {
            DnsServerKind::Udp(o) => &o.via,
            DnsServerKind::Tcp(o) => &o.via,
            DnsServerKind::Https(o) => &o.via,
            DnsServerKind::Hosts | DnsServerKind::Local => "",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsServerKind {
    Udp(RemoteDnsServer),
    Tcp(RemoteDnsServer),
    Https(RemoteHttpsDnsServer),
    Hosts,
    Local,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteDnsServer {
    pub server: String,
    pub server_port: u16,
    pub via: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteHttpsDnsServer {
    pub server: String,
    pub server_port: u16,
    pub via: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteOptions {
    pub final_: String,
    pub auto_detect_interface: bool,
    pub rules: Vec<Rule>,
    pub default_domain_resolver: Option<DomainResolveOptions>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DomainResolveOptions {
    pub server: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub default_options: DefaultRule,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefaultRule {
    pub inbound: Vec<String>,
    pub protocol: Vec<String>,
    pub domain: Vec<String>,
    pub domain_suffix: Vec<String>,
    pub domain_keyword: Vec<String>,
    pub ip_cidr: Vec<IpNet>,
    pub action: RuleActionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleActionKind {
    Sniff(SniffActionOptions),
    HijackDns,
    Reject(RejectActionOptions),
    Resolve(ResolveActionOptions),
    RouteOptions(RouteOptionsActionOptions),
    Route(RouteActionOptions),
}

impl Default for RuleActionKind {
    fn default() -> Self {
        RuleActionKind::HijackDns
    }
}

impl RuleActionKind {
    pub fn name(&self) -> &'static str {
        match self {
            RuleActionKind::Sniff(_) => "sniff",
            RuleActionKind::HijackDns => "hijack-dns",
            RuleActionKind::Reject(_) => "reject",
            RuleActionKind::Resolve(_) => "resolve",
            RuleActionKind::RouteOptions(_) => "route-options",
            RuleActionKind::Route(_) => "route",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteActionOptions {
    pub outbound: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SniffActionOptions {
    pub timeout: Option<Duration>,
    pub override_destination: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RejectActionOptions {
    pub method: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolveActionOptions {
    pub strategy: DomainStrategy,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteOptionsActionOptions {
    pub udp_disable_domain_unmapping: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DomainStrategy {
    #[default]
    AsIs,
    PreferIpv4,
    PreferIpv6,
    Ipv4Only,
    Ipv6Only,
}

impl DomainStrategy {
    pub fn name(self) -> &'static str {
        match self {
            DomainStrategy::AsIs => "as_is",
            DomainStrategy::PreferIpv4 => "prefer_ipv4",
            DomainStrategy::PreferIpv6 => "prefer_ipv6",
            DomainStrategy::Ipv4Only => "ipv4_only",
            DomainStrategy::Ipv6Only => "ipv6_only",
        }
    }
}

/// Thin wrapper preserving the original textual prefix until M5 promotes it to `ipnet::IpNet`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prefix(pub String);

pub mod constants {
    pub const TYPE_TUN: &str = "tun";
    pub const TYPE_HYSTERIA2: &str = "hysteria2";
    pub const TYPE_DIRECT: &str = "direct";
    pub const TYPE_BLOCK: &str = "block";
    #[cfg(feature = "wireguard")]
    pub const TYPE_WIREGUARD: &str = "wireguard";

    pub const PROTOCOL_DNS: &str = "dns";
    pub const PROTOCOL_QUIC: &str = "quic";

    pub const REJECT_METHOD_DEFAULT: &str = "default";

    pub const NETWORK_STRATEGY_DEFAULT: &str = "default";

    pub const DEFAULT_TUN_ID: &str = "tun";
    pub const DEFAULT_HYSTERIA_ID: &str = "hysteria2";
    pub const DEFAULT_DIRECT_ID: &str = "direct";
    pub const DEFAULT_DNS_ID: &str = "default";
    pub const DEFAULT_TUN_STACK: &str = "system";
    pub const DEFAULT_TUN_MTU: u32 = 9000;
    pub const DEFAULT_DNS_PATH: &str = "/dns-query";
    pub const DEFAULT_HYSTERIA_PORT: u16 = 443;
    /// sing-box's default WireGuard tunnel MTU (1500 - 20 IPv4 - 8 UDP - 32 wg overhead - margin).
    #[cfg(feature = "wireguard")]
    pub const DEFAULT_WIREGUARD_MTU: u32 = 1408;
    pub const DNS_TYPE_HOSTS: &str = "hosts";
    pub const DNS_TYPE_LOCAL: &str = "local";
}
