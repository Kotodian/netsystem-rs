//! `[log]` config section: logging policy. Single-layer schema.

use crate::log::Level;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Log {
    /// Minimum log level, for example `debug`, `info`, or `warn`.
    pub level: Level,
    /// Log output target from the user config.
    pub output: String,
    /// Whether log lines should include timestamps.
    pub timestamp: bool,
    /// Whether logging is disabled entirely.
    pub disabled: bool,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            level: Level::default(),
            output: "stderr".to_owned(),
            timestamp: false,
            disabled: false,
        }
    }
}

impl Log {
    pub fn is_default(&self) -> bool {
        *self == Log::default()
    }
}
