//! `[log]` config section: parsed Raw form, runtime Options, and the
//! conversion from one to the other.

use serde::{Deserialize, Deserializer, de};

use crate::log::Level;

use super::parse::is_false;
use super::raw_struct_with_default_check;

raw_struct_with_default_check! {
    pub struct RawLogConfig {
        /// Minimum log level, for example `debug`, `info`, or `warn`.
        #[serde(deserialize_with = "deserialize_log_level")]
        pub level: Option<Level> => "Option::is_none",
        /// Log output target from the user config.
        pub output: String => "String::is_empty",
        /// Whether log lines should include timestamps.
        pub timestamp: bool => "is_false",
        /// Whether logging is disabled entirely.
        pub disabled: bool => "is_false",
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
        disabled: raw.disabled,
        level: raw.level.unwrap_or_default(),
        output: raw.output,
        timestamp: raw.timestamp,
    }
}

fn deserialize_log_level<'de, D>(deserializer: D) -> Result<Option<Level>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Ok(None);
    }
    Level::from_name(value.to_ascii_lowercase().as_str())
        .map(Some)
        .ok_or_else(|| de::Error::custom(format!("log.level: unknown log level {value:?}")))
}

