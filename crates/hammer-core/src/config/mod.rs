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

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

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
    #[cfg(feature = "outbound-hysteria2")]
    pub const TYPE_HYSTERIA2: &str = "hysteria2";
    pub const TYPE_DIRECT: &str = "direct";
    pub const TYPE_BLOCK: &str = "block";
    pub const TYPE_URLTEST: &str = "urltest";
    #[cfg(feature = "wireguard")]
    pub const TYPE_WIREGUARD: &str = "wireguard";

    /// Default URL probed by the urltest outbound when the user does not
    /// configure one. Mirrors sing-box.
    pub const DEFAULT_URLTEST_URL: &str = "https://www.gstatic.com/generate_204";
    /// Default tolerance window for urltest selection in milliseconds. A new
    /// candidate must be at least this much faster than the current pick to
    /// trigger a switch.
    pub const DEFAULT_URLTEST_TOLERANCE_MS: u64 = 50;
    /// Default per-probe timeout for urltest in milliseconds.
    pub const DEFAULT_URLTEST_TIMEOUT_MS: u64 = 5_000;

    pub const PROTOCOL_DNS: &str = "dns";
    pub const PROTOCOL_QUIC: &str = "quic";

    pub const REJECT_METHOD_DEFAULT: &str = "default";

    pub const NETWORK_STRATEGY_DEFAULT: &str = "default";

    pub const DEFAULT_TUN_ID: &str = "tun";
    #[cfg(feature = "outbound-hysteria2")]
    pub const DEFAULT_HYSTERIA_ID: &str = "hysteria2";
    pub const DEFAULT_DIRECT_ID: &str = "direct";
    pub const DEFAULT_DNS_ID: &str = "default";
    pub const DEFAULT_TUN_STACK: &str = "system";
    pub const DEFAULT_TUN_MTU: u32 = 9000;
    pub const DEFAULT_DNS_PATH: &str = "/dns-query";
    #[cfg(feature = "outbound-hysteria2")]
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
    #[cfg(feature = "outbound-hysteria2")]
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
        #[cfg(feature = "outbound-hysteria2")]
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
        #[cfg(feature = "outbound-hysteria2")]
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
        #[cfg(feature = "outbound-hysteria2")]
        {
            let (hysteria_options, hysteria_id) = outbound::build_hysteria_options(raw_hysteria)?;
            (
                outbound::build_outbounds(hysteria_options, hysteria_id.clone()),
                hysteria_id,
            )
        }
        #[cfg(not(feature = "outbound-hysteria2"))]
        {
            outbound::build_default_outbounds()
        }
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
    #[cfg(feature = "endpoint")]
    validate_unique_ids(
        "outbound/endpoint",
        outbounds
            .iter()
            .map(|item| item.id.as_str())
            .chain(endpoints.iter().map(|item| item.id.as_str())),
    )?;

    #[cfg(feature = "endpoint")]
    outbound::validate_urltest_dependencies(
        &outbounds,
        outbounds
            .iter()
            .map(|item| item.id.as_str())
            .chain(endpoints.iter().map(|item| item.id.as_str())),
    )?;
    #[cfg(not(feature = "endpoint"))]
    outbound::validate_urltest_dependencies(
        &outbounds,
        outbounds.iter().map(|item| item.id.as_str()),
    )?;

    #[cfg(feature = "endpoint")]
    validate_route_rule_outbounds(
        &rules,
        outbounds
            .iter()
            .map(|item| item.id.as_str())
            .chain(endpoints.iter().map(|item| item.id.as_str())),
    )?;
    #[cfg(not(feature = "endpoint"))]
    validate_route_rule_outbounds(&rules, outbounds.iter().map(|item| item.id.as_str()))?;

    let route_final = if raw_route.final_.is_empty() {
        default_route_final
    } else {
        raw_route.final_.clone()
    };
    #[cfg(feature = "endpoint")]
    validate_known_id_kind(
        "route.final",
        &route_final,
        "outbound id",
        outbounds
            .iter()
            .map(|item| item.id.as_str())
            .chain(endpoints.iter().map(|item| item.id.as_str())),
    )?;
    #[cfg(not(feature = "endpoint"))]
    validate_known_id_kind(
        "route.final",
        &route_final,
        "outbound id",
        outbounds.iter().map(|item| item.id.as_str()),
    )?;
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
    #[cfg(feature = "endpoint")]
    validate_dns_server_via(
        &dns_options.servers,
        outbounds
            .iter()
            .map(|item| item.id.as_str())
            .chain(endpoints.iter().map(|item| item.id.as_str())),
    )?;
    #[cfg(not(feature = "endpoint"))]
    validate_dns_server_via(
        &dns_options.servers,
        outbounds.iter().map(|item| item.id.as_str()),
    )?;
    let default_domain_resolver = if raw_route.default_domain_resolver.is_empty() {
        None
    } else {
        Some(dns::DomainResolveOptions {
            server: raw_route.default_domain_resolver.clone(),
        })
    };
    validate_dns_server_domain_resolver(
        &dns_options.servers,
        default_domain_resolver.as_ref().map(|d| d.server.as_str()),
    )?;
    let route_options = route::RouteOptions {
        final_: route_final,
        auto_detect_interface: auto_detect,
        rules,
        default_domain_resolver,
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

fn validate_route_rule_outbounds<'a>(
    rules: &[route::Rule],
    known: impl IntoIterator<Item = &'a str>,
) -> Result<(), HammerError> {
    let known = known.into_iter().collect::<HashSet<_>>();
    for rule in rules {
        let route::RuleActionKind::Route(action) = &rule.default_options.action else {
            continue;
        };
        if known.contains(action.outbound.as_str()) {
            continue;
        }
        return Err(HammerError::config_validation(format!(
            "route.rules outbound references unknown outbound id: {}",
            action.outbound
        )));
    }
    Ok(())
}

fn validate_dns_server_via<'a>(
    servers: &[dns::DnsServer],
    known: impl IntoIterator<Item = &'a str>,
) -> Result<(), HammerError> {
    let known = known.into_iter().collect::<HashSet<_>>();
    for server in servers {
        let via = server.via();
        if via.is_empty() || known.contains(via) {
            continue;
        }
        return Err(HammerError::config_validation(format!(
            "dns.server via references unknown outbound id: {via}"
        )));
    }
    Ok(())
}

/// True iff the server will perform a bootstrap lookup at runtime: it has a
/// domain (not IP-literal) `server` AND a non-empty `via` outbound. IP-literal
/// servers and direct (no-via) servers never trigger bootstrap.
fn server_needs_bootstrap(server: &dns::DnsServer) -> bool {
    let is_domain = server
        .server_string()
        .map(|s| s.parse::<IpAddr>().is_err())
        .unwrap_or(false);
    is_domain && !server.via().is_empty()
}

/// The bootstrap DNS server tag actually used to resolve `server`'s domain.
/// Returns empty for servers that don't need bootstrap (IP-literal or
/// direct). Per-server `domain_resolver` wins; otherwise the global default.
fn effective_bootstrap<'a>(server: &'a dns::DnsServer, default: Option<&'a str>) -> &'a str {
    if !server_needs_bootstrap(server) {
        return "";
    }
    if !server.domain_resolver().is_empty() {
        server.domain_resolver()
    } else {
        default.unwrap_or("")
    }
}

fn validate_dns_server_domain_resolver(
    servers: &[dns::DnsServer],
    default: Option<&str>,
) -> Result<(), HammerError> {
    let tags: HashSet<&str> = servers.iter().map(|s| s.id.as_str()).collect();

    if let Some(default_tag) = default
        && !tags.contains(default_tag)
    {
        return Err(HammerError::config_validation(format!(
            "route.default_domain_resolver references unknown dns.server id: {default_tag}"
        )));
    }

    for server in servers {
        // Per-server domain_resolver field, if explicitly set, must
        // reference a real DNS server id even when this server itself
        // will not perform bootstrap lookups — catches typos early.
        let raw_resolver = server.domain_resolver();
        if !raw_resolver.is_empty() && !tags.contains(raw_resolver) {
            return Err(HammerError::config_validation(format!(
                "dns.server '{}': domain_resolver references unknown dns.server id: {raw_resolver}",
                server.id
            )));
        }

        // Strict: a server that will actually run a bootstrap lookup
        // must have an effective bootstrap source.
        if server_needs_bootstrap(server) && effective_bootstrap(server, default).is_empty() {
            return Err(HammerError::config_validation(format!(
                "dns.server '{}': domain server with via requires \
                 domain_resolver or route.default_domain_resolver",
                server.id
            )));
        }
    }

    detect_dns_bootstrap_cycle(servers, default)
}

fn detect_dns_bootstrap_cycle(
    servers: &[dns::DnsServer],
    default: Option<&str>,
) -> Result<(), HammerError> {
    let by_id: HashMap<&str, &dns::DnsServer> =
        servers.iter().map(|s| (s.id.as_str(), s)).collect();
    let mut color: HashMap<&str, u8> = HashMap::new();
    let mut path: Vec<&str> = Vec::new();
    for start in servers.iter().map(|s| s.id.as_str()) {
        visit_for_cycle(start, &by_id, default, &mut color, &mut path)?;
    }
    Ok(())
}

fn visit_for_cycle<'a>(
    node: &'a str,
    by_id: &HashMap<&'a str, &'a dns::DnsServer>,
    default: Option<&'a str>,
    color: &mut HashMap<&'a str, u8>,
    path: &mut Vec<&'a str>,
) -> Result<(), HammerError> {
    match color.get(node).copied().unwrap_or(0) {
        2 => return Ok(()),
        1 => {
            let cycle_start = path.iter().position(|&n| n == node).unwrap_or(0);
            let mut cycle: Vec<&str> = path[cycle_start..].to_vec();
            cycle.push(node);
            return Err(HammerError::config_validation(format!(
                "dns.server bootstrap cycle: {}",
                cycle.join(" -> ")
            )));
        }
        _ => {}
    }
    color.insert(node, 1);
    path.push(node);
    if let Some(server) = by_id.get(node) {
        let next = effective_bootstrap(server, default);
        if !next.is_empty() && by_id.contains_key(next) {
            visit_for_cycle(next, by_id, default, color, path)?;
        }
    }
    path.pop();
    color.insert(node, 2);
    Ok(())
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
    validate_known_id_kind(field, id, "server id", known)
}

fn validate_known_id_kind<'a>(
    field: &str,
    id: &str,
    kind: &str,
    known: impl IntoIterator<Item = &'a str>,
) -> Result<(), HammerError> {
    if known.into_iter().any(|candidate| candidate == id) {
        return Ok(());
    }
    Err(HammerError::config_validation(format!(
        "{field} references unknown {kind}: {id}"
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
