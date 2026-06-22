// Per-domain config submodules: each owns its own Raw* / Options* / build_*.
// The previous `raw.rs` / `options.rs` / `build.rs` umbrellas are gone — see
// /home/lqk/.claude/plans/cosmic-popping-wall.md for the migration plan.
#[cfg(feature = "wireguard")]
mod endpoint;
mod inbound;
mod log;
mod outbound;
mod route;
mod runtime;
mod trace;
mod util;

#[cfg(feature = "wireguard")]
pub use endpoint::*;
pub use inbound::*;
pub use log::*;
pub use outbound::*;
pub use route::*;
pub use runtime::*;
pub use trace::*;
pub use util::*;

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
    pub const TYPE_BLOCK: &str = "block";
    #[cfg(feature = "wireguard")]
    pub const TYPE_WIREGUARD: &str = "wireguard";

    pub const PROTOCOL_QUIC: &str = "quic";

    pub const REJECT_METHOD_DEFAULT: &str = "default";

    pub const DEFAULT_TUN_ID: &str = "tun";
    pub const DEFAULT_TUN_STACK: &str = "system";
    pub const DEFAULT_TUN_MTU: u32 = 9000;
    /// sing-box's default WireGuard tunnel MTU (1500 - 20 IPv4 - 8 UDP - 32 wg overhead - margin).
    #[cfg(feature = "wireguard")]
    pub const DEFAULT_WIREGUARD_MTU: u32 = 1408;
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
    /// Explicit outbound list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outbounds: Vec<outbound::RawOutbound>,
    /// Optional sing-box style endpoint list.
    #[cfg(feature = "wireguard")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<endpoint::RawEndpoint>,
    /// Optional route section.
    #[serde(default, skip_serializing_if = "route::RawRouteConfig::is_default")]
    pub route: route::RawRouteConfig,
    /// Optional runtime dump section.
    #[serde(default, skip_serializing_if = "runtime::RawRuntimeConfig::is_default")]
    pub runtime: runtime::RawRuntimeConfig,
    /// Optional node packet trace section.
    #[serde(default, skip_serializing_if = "trace::RawTraceConfig::is_default")]
    pub trace: trace::RawTraceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub log: log::LogOptions,
    pub inbounds: Vec<inbound::Inbound>,
    pub outbounds: Vec<outbound::Outbound>,
    #[cfg(feature = "wireguard")]
    pub endpoints: Vec<endpoint::Endpoint>,
    pub route: route::RouteOptions,
    pub runtime: runtime::RuntimeOptions,
    pub trace: trace::TraceOptions,
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
/// resolve the cross-domain pieces, such as route final defaulting to the
/// selected outbound id.
fn build_options(raw: RawConfig) -> Result<Options, HammerError> {
    #[cfg(feature = "wireguard")]
    let RawConfig {
        log: raw_log,
        tun: raw_tun,
        inbounds: raw_inbounds,
        outbounds: raw_outbounds,
        endpoints: raw_endpoints,
        route: raw_route,
        runtime: raw_runtime,
        trace: raw_trace,
    } = raw;
    #[cfg(not(feature = "wireguard"))]
    let RawConfig {
        log: raw_log,
        tun: raw_tun,
        inbounds: raw_inbounds,
        outbounds: raw_outbounds,
        route: raw_route,
        runtime: raw_runtime,
        trace: raw_trace,
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
            if let Some(tun) = raw_inbound.tun() {
                rules.extend(route::derive_tun_route_rules(tun, &id)?);
            }
            inbounds.push(inbound);
        }
        (inbounds, rules)
    };
    validate_unique_ids("inbounds", inbounds.iter().map(|item| item.id.as_str()))?;
    rules.extend(route::build_user_rules(&raw_route.rules)?);

    let outbounds = outbound::build_declared_outbounds(raw_outbounds)?;
    validate_unique_ids("outbounds", outbounds.iter().map(|item| item.id.as_str()))?;

    #[cfg(feature = "wireguard")]
    let endpoints = endpoint::build_endpoints(raw_endpoints)?;
    #[cfg(feature = "wireguard")]
    validate_unique_ids("endpoints", endpoints.iter().map(|item| item.id.as_str()))?;
    #[cfg(feature = "wireguard")]
    validate_unique_ids(
        "outbound/endpoint",
        outbounds
            .iter()
            .map(|item| item.id.as_str())
            .chain(endpoints.iter().map(|item| item.id.as_str())),
    )?;

    #[cfg(feature = "wireguard")]
    validate_route_rule_outbounds(
        &rules,
        outbounds
            .iter()
            .map(|item| item.id.as_str())
            .chain(endpoints.iter().map(|item| item.id.as_str())),
    )?;
    #[cfg(not(feature = "wireguard"))]
    validate_route_rule_outbounds(&rules, outbounds.iter().map(|item| item.id.as_str()))?;

    #[cfg(feature = "wireguard")]
    let default_route_final = default_outbound_id(
        outbounds.iter().map(|item| item.id.as_str()),
        endpoints.iter().map(|item| item.id.as_str()),
    )?;
    #[cfg(not(feature = "wireguard"))]
    let default_route_final = default_outbound_id(
        outbounds.iter().map(|item| item.id.as_str()),
        std::iter::empty(),
    )?;

    let route_final = if raw_route.final_.is_empty() {
        default_route_final
    } else {
        raw_route.final_.clone()
    };
    #[cfg(feature = "wireguard")]
    validate_known_id_kind(
        "route.final",
        &route_final,
        "outbound id",
        outbounds
            .iter()
            .map(|item| item.id.as_str())
            .chain(endpoints.iter().map(|item| item.id.as_str())),
    )?;
    #[cfg(not(feature = "wireguard"))]
    validate_known_id_kind(
        "route.final",
        &route_final,
        "outbound id",
        outbounds.iter().map(|item| item.id.as_str()),
    )?;
    let auto_detect = raw_route.auto_detect_interface.unwrap_or(true);
    let route_options = route::RouteOptions {
        final_: route_final,
        auto_detect_interface: auto_detect,
        rules,
    };

    Ok(Options {
        log: log::build_log_options(raw_log),
        inbounds,
        outbounds,
        #[cfg(feature = "wireguard")]
        endpoints,
        route: route_options,
        runtime: runtime::build_runtime_options(raw_runtime)?,
        trace: trace::build_trace_options(raw_trace)?,
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

fn default_outbound_id<'a>(
    outbounds: impl IntoIterator<Item = &'a str>,
    endpoints: impl IntoIterator<Item = &'a str>,
) -> Result<String, HammerError> {
    outbounds
        .into_iter()
        .chain(endpoints)
        .next()
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            HammerError::config_validation("at least one outbound or endpoint is required")
        })
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
