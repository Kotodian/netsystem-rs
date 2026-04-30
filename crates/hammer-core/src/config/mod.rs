mod build;
mod options;
mod parse;
mod raw;

// Per-domain submodules (each owns its own Raw*, Options*, build_*).
// Created incrementally as the legacy raw/options/build umbrellas are
// drained — see /home/lqk/.claude/plans/cosmic-popping-wall.md.
#[cfg(feature = "wireguard")]
mod endpoint;
mod inbound;
mod log;
mod outbound;

#[cfg(feature = "wireguard")]
pub use endpoint::*;
pub use inbound::*;
pub use log::*;
pub use options::*;
pub use outbound::*;

use crate::error::HammerError;
use raw::RawConfig;

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
/// and the runtime. Hoisted out of `options::` so submodules can `use
/// super::constants as C;` without going through the legacy umbrella.
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

pub fn check_config(content: &str) -> Result<(), HammerError> {
    parse_config(content).map(|_| ())
}

pub fn format_config(content: &str) -> Result<String, HammerError> {
    let raw = decode_raw(content)?;
    toml::to_string(&raw).map_err(|e| HammerError::internal(format!("encode TOML: {e}")))
}

pub fn parse_config(content: &str) -> Result<Options, HammerError> {
    let raw = decode_raw(content)?;
    build::build_options(raw)
}

fn decode_raw(content: &str) -> Result<RawConfig, HammerError> {
    toml::from_str::<RawConfig>(content).map_err(translate_toml_error)
}

fn translate_toml_error(err: toml::de::Error) -> HammerError {
    let msg = err.message();
    if let Some(field) = extract_unknown_field(msg) {
        return HammerError::config_validation(format!("unsupported config key: {field}"));
    }
    HammerError::config_parse(format!("parse TOML: {msg}"))
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
