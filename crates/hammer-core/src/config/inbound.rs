//! Inbound config sections.
//!
//! Currently the only inbound kind is `tun`, but `Inbound` / `InboundKind`
//! sit at this layer so adding a new inbound (e.g. `socks` for desktop
//! debugging) means dropping a new variant here without touching the rest
//! of the config tree.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use crate::error::HammerError;

use super::constants as C;
use super::dns::DomainStrategy;
use super::{raw_struct, raw_struct_with_default_check};

raw_struct_with_default_check! {
    pub struct RawTunConfig {
        /// Inbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Requested TUN interface name.
        pub interface_name: String => "String::is_empty",
        /// TUN MTU.
        pub mtu: Option<u32> => "Option::is_none",
        /// Packet stack mode, for example `system` or `disabled`.
        pub stack: Option<TunStack> => "Option::is_none",
        /// Whether this interface is TAP (Ethernet frames) instead of L3 TUN.
        pub tap: Option<bool> => "Option::is_none",
        /// Local interface addresses in CIDR form.
        pub address: Vec<IpNet> => "Vec::is_empty",
        /// Routes included through the tunnel.
        pub route_address: Vec<IpNet> => "Vec::is_empty",
        /// Routes excluded from the tunnel.
        pub route_exclude_address: Vec<IpNet> => "Vec::is_empty",
        /// Whether the platform should install routes automatically.
        pub auto_route: Option<bool> => "Option::is_none",
        /// Whether route installation should use strict routing semantics.
        pub strict_route: Option<bool> => "Option::is_none",
        /// UDP idle timeout.
        #[serde(with = "humantime_serde::option")]
        pub udp_timeout: Option<Duration> => "Option::is_none",
        /// Whether protocol/domain sniffing is enabled.
        pub sniff: Option<bool> => "Option::is_none",
        /// Whether DNS packets should be intercepted by the DNS router.
        pub hijack_dns: Option<bool> => "Option::is_none",
        /// Whether sniffed destinations replace the original destination.
        pub sniff_override_destination: Option<bool> => "Option::is_none",
        /// Sniffing timeout.
        #[serde(with = "humantime_serde::option")]
        pub sniff_timeout: Option<Duration> => "Option::is_none",
        /// Domain resolution strategy for sniffed/routed traffic.
        pub domain_strategy: DomainStrategy => "DomainStrategy::is_default",
        /// Whether UDP domain unmapping is disabled for this inbound.
        pub udp_disable_domain_unmapping: Option<bool> => "Option::is_none",
        /// Whether detected QUIC traffic should be rejected.
        pub block_quic: Option<bool> => "Option::is_none",
    }
}

raw_struct! {
    pub struct RawInboundUser {
        /// Proxy authentication username.
        pub username: String => "String::is_empty",
        /// Proxy authentication password.
        pub password: String => "String::is_empty",
    }
}

raw_struct! {
    pub struct RawSocksInboundConfig {
        /// Inbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Local address to listen on.
        pub listen: Option<IpAddr> => "Option::is_none",
        /// Local TCP port to listen on.
        pub listen_port: Option<u16> => "Option::is_none",
        /// UDP association idle timeout.
        #[serde(with = "humantime_serde::option")]
        pub udp_timeout: Option<Duration> => "Option::is_none",
        /// Authentication users.
        pub users: Vec<RawInboundUser> => "Vec::is_empty",
    }
}

raw_struct! {
    pub struct RawHttpInboundConfig {
        /// Inbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Local address to listen on.
        pub listen: Option<IpAddr> => "Option::is_none",
        /// Local TCP port to listen on.
        pub listen_port: Option<u16> => "Option::is_none",
        /// UDP idle timeout from shared listen fields. HTTP inbound does not
        /// use UDP today, but keeping the field accepted matches sing-box's
        /// shared listen schema.
        #[serde(with = "humantime_serde::option")]
        pub udp_timeout: Option<Duration> => "Option::is_none",
        /// Authentication users.
        pub users: Vec<RawInboundUser> => "Vec::is_empty",
    }
}

raw_struct! {
    pub struct RawMixedInboundConfig {
        /// Inbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Local address to listen on.
        pub listen: Option<IpAddr> => "Option::is_none",
        /// Local TCP port to listen on.
        pub listen_port: Option<u16> => "Option::is_none",
        /// UDP association idle timeout.
        #[serde(with = "humantime_serde::option")]
        pub udp_timeout: Option<Duration> => "Option::is_none",
        /// Authentication users.
        pub users: Vec<RawInboundUser> => "Vec::is_empty",
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawInbound {
    Tun(RawTunConfig),
    Socks(RawSocksInboundConfig),
    Http(RawHttpInboundConfig),
    Mixed(RawMixedInboundConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inbound {
    pub id: String,
    pub kind: InboundKind,
}

impl RawInbound {
    pub(super) fn tun(&self) -> Option<&RawTunConfig> {
        match self {
            RawInbound::Tun(raw) => Some(raw),
            RawInbound::Socks(_) | RawInbound::Http(_) | RawInbound::Mixed(_) => None,
        }
    }
}

impl Inbound {
    pub fn type_name(&self) -> &'static str {
        match self.kind {
            InboundKind::Tun(_) => "tun",
            InboundKind::Socks(_) => "socks",
            InboundKind::Http(_) => "http",
            InboundKind::Mixed(_) => "mixed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundKind {
    Tun(TunInboundOptions),
    Socks(SocksInboundOptions),
    Http(HttpInboundOptions),
    Mixed(MixedInboundOptions),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TunInboundOptions {
    pub interface_name: String,
    pub mtu: u32,
    pub address: Vec<IpNet>,
    pub route_address: Vec<IpNet>,
    pub route_exclude_address: Vec<IpNet>,
    pub auto_route: bool,
    pub strict_route: bool,
    pub stack: TunStack,
    pub tap: bool,
    pub udp_timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenInboundOptions {
    pub listen: IpAddr,
    pub listen_port: u16,
    pub udp_timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundUser {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocksInboundOptions {
    pub listen: ListenInboundOptions,
    pub users: Vec<InboundUser>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpInboundOptions {
    pub listen: ListenInboundOptions,
    pub users: Vec<InboundUser>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MixedInboundOptions {
    pub listen: ListenInboundOptions,
    pub users: Vec<InboundUser>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TunStack {
    #[default]
    System,
    Disabled,
}

impl TunStack {
    pub fn name(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Disabled => "disabled",
        }
    }
}

/// Build the lone `Inbound` (currently always `tun`) and return the resolved
/// tun id alongside it so the caller can hand it to route-rule derivation.
pub(super) fn build_tun_inbound(raw: &RawTunConfig) -> Result<(Inbound, String), HammerError> {
    let id = if raw.id.is_empty() {
        C::DEFAULT_TUN_ID.to_owned()
    } else {
        raw.id.clone()
    };
    let options = build_tun_options(raw)?;
    Ok((
        Inbound {
            id: id.clone(),
            kind: InboundKind::Tun(options),
        },
        id,
    ))
}

pub(super) fn build_inbound(raw: &RawInbound) -> Result<(Inbound, String), HammerError> {
    match raw {
        RawInbound::Tun(tun) => build_tun_inbound(tun),
        RawInbound::Socks(socks) => build_socks_inbound(socks),
        RawInbound::Http(http) => build_http_inbound(http),
        RawInbound::Mixed(mixed) => build_mixed_inbound(mixed),
    }
}

fn build_socks_inbound(raw: &RawSocksInboundConfig) -> Result<(Inbound, String), HammerError> {
    let id = if raw.id.is_empty() {
        "socks".to_owned()
    } else {
        raw.id.clone()
    };
    Ok((
        Inbound {
            id: id.clone(),
            kind: InboundKind::Socks(SocksInboundOptions {
                listen: build_listen_options(raw.listen, raw.listen_port, raw.udp_timeout)?,
                users: build_users(&raw.users)?,
            }),
        },
        id,
    ))
}

fn build_http_inbound(raw: &RawHttpInboundConfig) -> Result<(Inbound, String), HammerError> {
    let id = if raw.id.is_empty() {
        "http".to_owned()
    } else {
        raw.id.clone()
    };
    Ok((
        Inbound {
            id: id.clone(),
            kind: InboundKind::Http(HttpInboundOptions {
                listen: build_listen_options(raw.listen, raw.listen_port, raw.udp_timeout)?,
                users: build_users(&raw.users)?,
            }),
        },
        id,
    ))
}

fn build_mixed_inbound(raw: &RawMixedInboundConfig) -> Result<(Inbound, String), HammerError> {
    let id = if raw.id.is_empty() {
        "mixed".to_owned()
    } else {
        raw.id.clone()
    };
    Ok((
        Inbound {
            id: id.clone(),
            kind: InboundKind::Mixed(MixedInboundOptions {
                listen: build_listen_options(raw.listen, raw.listen_port, raw.udp_timeout)?,
                users: build_users(&raw.users)?,
            }),
        },
        id,
    ))
}

fn build_listen_options(
    listen: Option<IpAddr>,
    listen_port: Option<u16>,
    udp_timeout: Option<Duration>,
) -> Result<ListenInboundOptions, HammerError> {
    let listen_port = listen_port
        .ok_or_else(|| HammerError::config_validation("inbound.listen_port is required"))?;
    if listen_port == 0 {
        return Err(HammerError::config_validation(
            "inbound.listen_port must be non-zero",
        ));
    }
    Ok(ListenInboundOptions {
        listen: listen.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        listen_port,
        udp_timeout,
    })
}

fn build_users(raw: &[RawInboundUser]) -> Result<Vec<InboundUser>, HammerError> {
    raw.iter()
        .map(|user| {
            if user.username.is_empty() || user.password.is_empty() {
                return Err(HammerError::config_validation(
                    "inbound users require username and password",
                ));
            }
            Ok(InboundUser {
                username: user.username.clone(),
                password: user.password.clone(),
            })
        })
        .collect()
}

fn build_tun_options(raw: &RawTunConfig) -> Result<TunInboundOptions, HammerError> {
    let mtu = raw.mtu.unwrap_or(C::DEFAULT_TUN_MTU);
    let stack = raw.stack.unwrap_or_default();
    let address = raw.address.clone();
    if address.is_empty() {
        return Err(HammerError::config_validation("tun.address is required"));
    }
    let route_address = raw.route_address.clone();
    let route_exclude_address = raw.route_exclude_address.clone();
    let auto_route = raw.auto_route.unwrap_or(true);
    Ok(TunInboundOptions {
        interface_name: raw.interface_name.clone(),
        mtu,
        address,
        route_address,
        route_exclude_address,
        auto_route,
        strict_route: raw.strict_route.unwrap_or(false),
        stack,
        tap: raw.tap.unwrap_or(false),
        udp_timeout: raw.udp_timeout,
    })
}
