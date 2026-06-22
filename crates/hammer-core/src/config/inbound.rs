//! Inbound config sections.
//!
//! Currently the only inbound kind is `tun`.

use std::time::Duration;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use crate::error::HammerError;

use super::constants as C;
use super::raw_struct_with_default_check;
use super::route::DomainStrategy;

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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawInbound {
    Tun(RawTunConfig),
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
        }
    }
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
    pub address: Vec<IpNet>,
    pub route_address: Vec<IpNet>,
    pub route_exclude_address: Vec<IpNet>,
    pub auto_route: bool,
    pub strict_route: bool,
    pub stack: TunStack,
    pub tap: bool,
    pub udp_timeout: Option<Duration>,
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
    }
}

fn build_tun_options(raw: &RawTunConfig) -> Result<TunInboundOptions, HammerError> {
    let mtu = raw.mtu.unwrap_or(C::DEFAULT_TUN_MTU);
    let stack = raw.stack.unwrap_or_default();
    let tap = raw.tap.unwrap_or(false);
    if tap && matches!(stack, TunStack::System) {
        return Err(HammerError::config_validation(
            "tun.tap requires stack = \"disabled\" because the legacy system stack only accepts L3 IP packets",
        ));
    }
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
        tap,
        udp_timeout: raw.udp_timeout,
    })
}
