// Per-domain config submodules: each owns its own Raw* / Options* / build_*.
// The previous `raw.rs` / `options.rs` / `build.rs` umbrellas are gone — see
// /home/lqk/.claude/plans/cosmic-popping-wall.md for the migration plan.
mod dns;
#[cfg(feature = "endpoint")]
mod endpoint;
mod inbound;
mod log;
mod outbound;
mod route;

pub use dns::*;
#[cfg(feature = "endpoint")]
pub use endpoint::*;
pub use inbound::*;
pub use log::*;
pub use outbound::*;
pub use route::*;

use std::collections::HashSet;

use crate::error::HammerError;

/// Per-domain submodules share the same `default + skip_serializing_if`
/// pattern. The macros own the repetitive attributes/derives; submodules
/// `use super::raw_struct;` (or `raw_struct_with_default_check`) to reach
/// them. Top-level sections also get an `is_default` helper so `RawConfig`'s
/// own serde attributes can elide them when they're untouched.
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
        #[derive(Debug, Default, ::serde::Deserialize, ::serde::Serialize, PartialEq, Eq)]
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
        $crate::config::raw_struct! {
            $(#[$meta])*
            $vis struct $name {
                $(
                    $(#[$field_meta])*
                    $field_vis $field: $ty => $skip,
                )*
            }
        }

        impl $name {
            pub(super) fn is_default(&self) -> bool {
                *self == $name::default()
            }
        }
    };
}

pub(crate) use raw_struct;
pub(crate) use raw_struct_with_default_check;

/// String constants and integer defaults shared across the config layer
/// and the runtime. Submodules `use super::constants as C;` to access these.
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

/// The full TOML schema. Each section delegates serde mechanics and
/// `Raw* → Options*` conversion to its own per-domain submodule; this
/// struct just glues the sections together.
#[derive(Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    /// Optional logging section.
    #[serde(default, skip_serializing_if = "log::RawLogConfig::is_default")]
    pub log: log::RawLogConfig,
    /// Optional TUN inbound section.
    #[serde(default, skip_serializing_if = "inbound::RawTunConfig::is_default")]
    pub tun: inbound::RawTunConfig,
    /// Explicit inbound list. When present, it supersedes the legacy `[tun]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inbounds: Vec<inbound::RawInbound>,
    /// Optional top-level Hysteria2 outbound section.
    #[serde(
        default,
        skip_serializing_if = "outbound::RawHysteria2Config::is_default"
    )]
    pub hysteria2: outbound::RawHysteria2Config,
    /// Explicit outbound list. When present, it supersedes legacy `[hysteria2]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outbounds: Vec<outbound::RawOutbound>,
    /// Optional sing-box style endpoint list.
    #[cfg(feature = "endpoint")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<endpoint::RawEndpoint>,
    /// Optional DNS transport section.
    #[serde(default, skip_serializing_if = "dns::RawDnsConfig::is_default")]
    pub dns: dns::RawDnsConfig,
    /// Optional route section.
    #[serde(default, skip_serializing_if = "route::RawRouteConfig::is_default")]
    pub route: route::RawRouteConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub log: log::LogOptions,
    pub dns: dns::DnsOptions,
    pub inbounds: Vec<inbound::Inbound>,
    pub outbounds: Vec<outbound::Outbound>,
    #[cfg(feature = "endpoint")]
    pub endpoints: Vec<endpoint::Endpoint>,
    pub route: route::RouteOptions,
}

pub fn check_config(content: &str) -> Result<(), HammerError> {
    parse_config(content).map(|_| ())
}

pub fn format_config(content: &str) -> Result<String, HammerError> {
    let raw = decode_raw(content)?;
    toml::to_string(&raw).map_err(|e| HammerError::internal(format!("encode TOML: {e}")))
}

pub fn parse_config(content: &str) -> Result<Options, HammerError> {
    let raw = decode_raw(content)?;
    build_options(raw)
}

fn decode_raw(content: &str) -> Result<RawConfig, HammerError> {
    let deserializer = toml::Deserializer::parse(content).map_err(translate_toml_error)?;
    serde_path_to_error::deserialize(deserializer).map_err(translate_toml_path_error)
}

fn translate_toml_path_error(err: serde_path_to_error::Error<toml::de::Error>) -> HammerError {
    let path = err.path().to_string();
    let inner = err.into_inner();
    if let Some(field) = extract_unknown_field(inner.message()) {
        return HammerError::config_validation(format!("unsupported config key: {field}"));
    }
    if path.is_empty() || path == "." {
        translate_toml_error(inner)
    } else {
        HammerError::config_parse(format!(
            "parse TOML: {path}: {}",
            toml_error_message(&inner)
        ))
    }
}

fn translate_toml_error(err: toml::de::Error) -> HammerError {
    let msg = toml_error_message(&err);
    if let Some(field) = extract_unknown_field(msg) {
        return HammerError::config_validation(format!("unsupported config key: {field}"));
    }
    HammerError::config_parse(format!("parse TOML: {msg}"))
}

fn toml_error_message(err: &toml::de::Error) -> &str {
    err.message()
        .strip_prefix("parse TOML: ")
        .unwrap_or_else(|| err.message())
}

fn extract_unknown_field(msg: &str) -> Option<String> {
    let needle = "unknown field ";
    let i = msg.find(needle)?;
    let rest = &msg[i + needle.len()..];
    let mut chars = rest.chars();
    let opener = chars.next()?;
    if opener != '`' && opener != '\'' && opener != '"' {
        return None;
    }
    let inner = &rest[opener.len_utf8()..];
    let close = inner.find(opener)?;
    Some(inner[..close].to_owned())
}

/// Top-level orchestrator: glue each domain's `build_*` together and
/// resolve the cross-domain pieces (route final defaulting to the
/// hysteria id, default_domain_resolver pointing at the dns id, etc.).
fn build_options(raw: RawConfig) -> Result<Options, HammerError> {
    #[cfg(feature = "endpoint")]
    let RawConfig {
        log: raw_log,
        tun: raw_tun,
        inbounds: raw_inbounds,
        hysteria2: raw_hysteria,
        outbounds: raw_outbounds,
        endpoints: raw_endpoints,
        dns: raw_dns,
        route: raw_route,
    } = raw;
    #[cfg(not(feature = "endpoint"))]
    let RawConfig {
        log: raw_log,
        tun: raw_tun,
        inbounds: raw_inbounds,
        hysteria2: raw_hysteria,
        outbounds: raw_outbounds,
        dns: raw_dns,
        route: raw_route,
    } = raw;

    let (inbounds, mut rules) = if raw_inbounds.is_empty() {
        let (tun_inbound, tun_id) = inbound::build_tun_inbound(&raw_tun)?;
        (
            vec![tun_inbound],
            route::derive_tun_route_rules(&raw_tun, &tun_id)?,
        )
    } else {
        let mut inbounds = Vec::new();
        let mut rules = Vec::new();
        for raw_inbound in &raw_inbounds {
            let (inbound, id) = inbound::build_inbound(raw_inbound)?;
            rules.extend(route::derive_tun_route_rules(raw_inbound.tun(), &id)?);
            inbounds.push(inbound);
        }
        (inbounds, rules)
    };
    validate_unique_ids("inbounds", inbounds.iter().map(|item| item.id.as_str()))?;
    rules.extend(route::build_user_rules(&raw_route.rules)?);

    let (outbounds, default_route_final) = if raw_outbounds.is_empty() {
        let (hysteria_options, hysteria_id) = outbound::build_hysteria_options(raw_hysteria)?;
        (
            outbound::build_outbounds(hysteria_options, hysteria_id.clone()),
            hysteria_id,
        )
    } else {
        let outbounds = outbound::build_declared_outbounds(raw_outbounds)?;
        let default = default_outbound_id(&outbounds)?;
        (outbounds, default)
    };
    validate_unique_ids("outbounds", outbounds.iter().map(|item| item.id.as_str()))?;

    #[cfg(feature = "endpoint")]
    let endpoints = endpoint::build_endpoints(raw_endpoints)?;
    #[cfg(feature = "endpoint")]
    validate_unique_ids("endpoints", endpoints.iter().map(|item| item.id.as_str()))?;

    let route_final = if raw_route.final_.is_empty() {
        default_route_final
    } else {
        raw_route.final_.clone()
    };
    let auto_detect = raw_route.auto_detect_interface.unwrap_or(true);

    let dns_options = dns::build_dns_options(&raw_dns, constants::DEFAULT_DIRECT_ID)?;
    validate_unique_ids(
        "dns.servers",
        dns_options.servers.iter().map(|item| item.id.as_str()),
    )?;
    validate_known_id(
        "dns.final",
        &dns_options.final_,
        dns_options.servers.iter().map(|item| item.id.as_str()),
    )?;
    let route_options = route::RouteOptions {
        final_: route_final,
        auto_detect_interface: auto_detect,
        rules,
        default_domain_resolver: Some(dns::DomainResolveOptions {
            server: dns_options.final_.clone(),
        }),
    };

    Ok(Options {
        log: log::build_log_options(raw_log),
        dns: dns_options,
        inbounds,
        outbounds,
        #[cfg(feature = "endpoint")]
        endpoints,
        route: route_options,
    })
}

fn default_outbound_id(outbounds: &[outbound::Outbound]) -> Result<String, HammerError> {
    outbounds
        .iter()
        .find(|outbound| outbound.id != constants::DEFAULT_DIRECT_ID)
        .or_else(|| outbounds.first())
        .map(|outbound| outbound.id.clone())
        .ok_or_else(|| HammerError::config_validation("at least one outbound is required"))
}

fn validate_unique_ids<'a>(
    scope: &str,
    ids: impl IntoIterator<Item = &'a str>,
) -> Result<(), HammerError> {
    let mut seen = HashSet::new();
    for id in ids {
        if !seen.insert(id) {
            return Err(HammerError::config_validation(format!(
                "duplicate {scope} id: {id}"
            )));
        }
    }
    Ok(())
}

fn validate_known_id<'a>(
    field: &str,
    id: &str,
    known: impl IntoIterator<Item = &'a str>,
) -> Result<(), HammerError> {
    if known.into_iter().any(|candidate| candidate == id) {
        return Ok(());
    }
    Err(HammerError::config_validation(format!(
        "{field} references unknown server id: {id}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_unknown_field_with_backticks() {
        assert_eq!(
            extract_unknown_field("unknown field `profile`, expected one of `log`"),
            Some("profile".to_owned())
        );
    }

    #[test]
    fn extracts_unknown_field_with_single_quotes() {
        assert_eq!(
            extract_unknown_field("unknown field 'profile' at line 5"),
            Some("profile".to_owned())
        );
    }
}
