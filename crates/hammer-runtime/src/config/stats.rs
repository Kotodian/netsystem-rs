//! `[stats]` configuration for the VPP-shaped statistics collector.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::error::RuntimeResult;
use crate::file::FILE_MAIN;
use crate::{Engine, File};
use hammer_stats::{StatsMain, stats_segment_socket};

pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(10);
pub(crate) const STATS_SEGMENT_SIZE: usize = 32 << 20;

fn default_update_interval() -> Duration {
    DEFAULT_UPDATE_INTERVAL
}

fn deserialize_socket_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let path = <PathBuf as serde::Deserialize>::deserialize(deserializer)?;
    if path.as_os_str().is_empty() {
        return Err(serde::de::Error::custom("stats socket_path is required"));
    }
    Ok(path)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatsConfig {
    #[serde(deserialize_with = "deserialize_socket_path")]
    pub socket_path: PathBuf,
    #[serde(default = "default_update_interval", with = "humantime_serde")]
    pub update_interval: Duration,
}

#[hammer_component_macros::config_function(
    name = "runtime_stats_config",
    section = "stats",
    early = true,
    required = true
)]
fn configure_stats(config: StatsConfig, engine: &mut Engine) -> RuntimeResult<()> {
    engine.registry.set(Arc::new(config));
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "stats_main_init",
    runs_after = ["file_main_init"]
)]
fn init_stats_main(config: Arc<StatsConfig>) -> RuntimeResult<()> {
    if config.socket_path.as_os_str().is_empty() {
        return Err(crate::RuntimeError::ConfigValidation {
            message: "stats socket_path is required".to_owned(),
        });
    }
    if StatsMain::global().is_ok() {
        return Ok(());
    }
    let listener = StatsMain::init("stat segment", STATS_SEGMENT_SIZE, &config.socket_path)?;
    FILE_MAIN
        .get()
        .expect("FileMain is initialized before stats startup")
        .add(File::new(
            listener,
            "stats segment socket".to_owned(),
            0,
            stats_segment_socket::file_functions::<crate::NodeRuntime, crate::RuntimeError>(),
        ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_config_requires_socket_path() {
        let result = toml::from_str::<StatsConfig>("update_interval = \"10s\"");
        assert!(result.is_err());
    }

    #[test]
    fn stats_config_parses_socket_path_and_duration() {
        let config: StatsConfig =
            toml::from_str("socket_path = \"/tmp/hammer-stats.sock\"\nupdate_interval = \"250ms\"")
                .expect("stats config");
        assert_eq!(
            config.socket_path,
            std::path::PathBuf::from("/tmp/hammer-stats.sock")
        );
        assert_eq!(config.update_interval, Duration::from_millis(250));
    }

    #[test]
    fn stats_config_rejects_empty_socket_path() {
        let result = toml::from_str::<StatsConfig>("socket_path = \"\"\nupdate_interval = \"10s\"");
        assert!(result.is_err());
    }

    #[test]
    fn stats_config_defaults_interval_without_defaulting_socket_path() {
        let config: StatsConfig = toml::from_str("socket_path = \"/tmp/hammer-stats.sock\"")
            .expect("stats config with default interval");
        assert_eq!(config.update_interval, DEFAULT_UPDATE_INTERVAL);
    }

    #[test]
    fn stats_config_accepts_zero_interval() {
        let config: StatsConfig =
            toml::from_str("socket_path = \"/tmp/hammer-stats.sock\"\nupdate_interval = \"0s\"")
                .expect("zero stats interval");
        assert_eq!(config.update_interval, Duration::ZERO);
    }
}
