//! Hammer configuration: single-layer, single-name TOML schema.
//!
//! Each section (`[log]`, `[trace]`, `[network]`, `[worker]`) is a struct that
//! directly deserializes from TOML. Container `#[serde(default)]` fills missing
//! fields from each struct's `Default` impl (which carries production
//! constants); `#[serde(with = "humantime_serde")]` handles `Duration` parsing.
//! A `validate()` pass enforces invariants serde cannot express. There is no
//! separate `Raw*` serde layer or `*Options` parsed layer — one type, one name.
//!
//! Configuration may be split across files via the top-level `include` key
//! (see `loader::load_config`): directories are loaded as sorted `*.toml`
//! fragments and merged into one `Config`.

pub mod loader;
pub mod log;
pub mod network;
pub mod route;
pub mod trace;
pub mod worker;

pub use loader::{load_config, parse_config};
pub use log::Log;
pub use network::{Network, SessionBackend};
pub use route::{Route, RouteAction, Via};
pub use trace::{Trace, TraceInput};
#[cfg(target_os = "macos")]
pub use worker::QosClass;
pub use worker::Worker;
pub use worker::WorkerScheduler;
#[cfg(target_os = "linux")]
pub use worker::{SchedulerPolicy, WorkerCpu, WorkerNuma};

/// String constants shared across the config layer and the runtime.
pub mod constants {
    pub const TYPE_TUN: &str = "tun";
    pub const DEFAULT_TUN_MTU: u32 = 9000;
}

use crate::error::{HammerError, HammerResult};
use hammer_infra::vec::Vec;

/// The full TOML schema. Top-level `include` drives multi-file loading
/// (see `loader`); it is consumed by `load_config` and absent from a
/// single-file `parse_config` result.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Files/directories to merge in before this config's own sections apply.
    /// Only meaningful when loading from a path via `load_config`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    plugins: Vec<String>,
    #[serde(
        default,
        rename = "plugin",
        skip_serializing_if = "toml::Table::is_empty"
    )]
    plugin_sections: toml::Table,
    pub log: Log,
    pub trace: Trace,
    pub network: Network,
    pub worker: Worker,
}

impl Config {
    #[inline]
    pub fn requested_plugins(&self) -> &[String] {
        &self.plugins
    }

    pub fn plugin_config<T>(&self, name: &str) -> HammerResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        if !self.plugins.iter().any(|plugin| plugin == name) {
            return Err(HammerError::config_validation(format!(
                "plugin `{name}` is not requested"
            )));
        }
        let value = self
            .plugin_sections
            .get(name)
            .cloned()
            .unwrap_or_else(|| toml::Value::Table(toml::Table::new()));
        value
            .try_into()
            .map_err(|error| HammerError::config_parse(format!("parse plugin.{name}: {error}")))
    }

    /// Validate every section's invariants. Called by `parse_config` and
    /// `load_config` after assembly.
    pub fn validate(&self) -> HammerResult<()> {
        self.trace.validate()?;
        self.network.validate()?;
        self.worker.validate()?;
        Ok(())
    }
}

pub fn check_config(content: &str) -> HammerResult<()> {
    parse_config(content).map(|_| ())
}

pub fn format_config(content: &str) -> HammerResult<String> {
    let cfg = parse_config(content)?;
    toml::to_string(&cfg).map_err(|e| HammerError::internal(format!("encode TOML: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_validates() {
        Config::default()
            .validate()
            .expect("default config is valid");
    }

    #[test]
    fn parse_minimal_config_uses_defaults() {
        let cfg = parse_config("").expect("parse empty");
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn parse_config_rejects_unknown_top_key() {
        let err = parse_config("bogus = 1\n").expect_err("reject unknown");
        assert!(err.to_string().contains("unsupported config key: bogus"));
    }
}
