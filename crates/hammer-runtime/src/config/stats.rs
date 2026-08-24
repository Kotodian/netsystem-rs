//! `[stats]` configuration for the VPP-shaped statistics collector.

use std::sync::Arc;
use std::time::Duration;

use crate::error::RuntimeResult;
use crate::Engine;

pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct StatsConfig {
    #[serde(with = "humantime_serde")]
    pub update_interval: Duration,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            update_interval: DEFAULT_UPDATE_INTERVAL,
        }
    }
}

#[hammer_component_macros::config_function(name = "runtime_stats_config", section = "stats")]
fn configure_stats(config: StatsConfig, engine: &mut Engine) -> RuntimeResult<()> {
    engine.registry.set(Arc::new(config));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_config_defaults_to_vpp_interval() {
        let config: StatsConfig = toml::from_str("").expect("default stats config");
        assert_eq!(config.update_interval, Duration::from_secs(10));
    }

    #[test]
    fn stats_config_parses_duration() {
        let config: StatsConfig = toml::from_str("update_interval = \"250ms\"")
            .expect("stats interval");
        assert_eq!(config.update_interval, Duration::from_millis(250));
    }

    #[test]
    fn stats_config_accepts_zero_interval() {
        let config: StatsConfig = toml::from_str("update_interval = \"0s\"")
            .expect("zero stats interval");
        assert_eq!(config.update_interval, Duration::ZERO);
    }
}
