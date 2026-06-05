//! `[runtime]` config section: control-plane runtime dump policy.

use std::time::Duration;

use crate::error::{HammerError, HammerResult};

use super::raw_struct_with_default_check;

pub const DEFAULT_RUNTIME_INTERVAL: Duration = Duration::from_secs(30);

raw_struct_with_default_check! {
    pub struct RawRuntimeConfig {
        /// Whether runtime dumping is enabled.
        pub enabled: Option<bool> => "Option::is_none",
        /// Runtime dump interval.
        #[serde(with = "humantime_serde::option")]
        pub interval: Option<Duration> => "Option::is_none",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub enabled: bool,
    pub interval: Duration,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: DEFAULT_RUNTIME_INTERVAL,
        }
    }
}

pub(super) fn build_runtime_options(raw: RawRuntimeConfig) -> HammerResult<RuntimeOptions> {
    let enabled = raw.enabled.unwrap_or(false);
    let interval = raw.interval.unwrap_or(DEFAULT_RUNTIME_INTERVAL);
    if interval.is_zero() {
        return Err(HammerError::config_validation(
            "runtime.interval must be non-zero",
        ));
    }

    Ok(RuntimeOptions { enabled, interval })
}
