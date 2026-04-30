//! `[log]` config section: parsed Raw form, runtime Options, and the
//! conversion from one to the other.

use crate::log::Level;

use super::raw_struct_with_default_check;

raw_struct_with_default_check! {
    pub struct RawLogConfig {
        /// Minimum log level, for example `debug`, `info`, or `warn`.
        pub level: Option<Level> => "Option::is_none",
        /// Log output target from the user config.
        pub output: String => "String::is_empty",
        /// Whether log lines should include timestamps.
        pub timestamp: Option<bool> => "Option::is_none",
        /// Whether logging is disabled entirely.
        pub disabled: Option<bool> => "Option::is_none",
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogOptions {
    pub disabled: bool,
    pub level: Level,
    pub output: String,
    pub timestamp: bool,
}

pub(super) fn build_log_options(raw: RawLogConfig) -> LogOptions {
    LogOptions {
        disabled: raw.disabled.unwrap_or(false),
        level: raw.level.unwrap_or_default(),
        output: raw.output,
        timestamp: raw.timestamp.unwrap_or(false),
    }
}
