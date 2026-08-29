use serde::{Deserialize, Serialize};

// Numeric values intentionally match `sing/log` Level so platform log codes stay
// comparable with the Go reference implementation.
#[repr(i32)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Panic = 0,
    Fatal = 1,
    Error = 2,
    Warn = 3,
    #[default]
    Info = 4,
    Debug = 5,
    Trace = 6,
}

impl Level {
    pub fn platform_code(self) -> &'static str {
        match self {
            Level::Trace => "T",
            Level::Debug => "D",
            Level::Info => "I",
            Level::Warn => "W",
            Level::Error => "E",
            Level::Fatal => "F",
            Level::Panic => "P",
        }
    }

    pub fn from_i32(value: i32) -> Option<Level> {
        match value {
            0 => Some(Level::Panic),
            1 => Some(Level::Fatal),
            2 => Some(Level::Error),
            3 => Some(Level::Warn),
            4 => Some(Level::Info),
            5 => Some(Level::Debug),
            6 => Some(Level::Trace),
            _ => None,
        }
    }

    pub fn from_name(value: &str) -> Option<Level> {
        match value {
            "panic" => Some(Level::Panic),
            "fatal" => Some(Level::Fatal),
            "error" => Some(Level::Error),
            "warn" | "warning" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" => Some(Level::Debug),
            "trace" => Some(Level::Trace),
            _ => None,
        }
    }
}

impl From<tracing::Level> for Level {
    fn from(level: tracing::Level) -> Self {
        match level {
            tracing::Level::ERROR => Level::Error,
            tracing::Level::WARN => Level::Warn,
            tracing::Level::INFO => Level::Info,
            tracing::Level::DEBUG => Level::Debug,
            tracing::Level::TRACE => Level::Trace,
        }
    }
}

impl From<Level> for tracing::Level {
    fn from(level: Level) -> Self {
        match level {
            Level::Panic | Level::Fatal | Level::Error => tracing::Level::ERROR,
            Level::Warn => tracing::Level::WARN,
            Level::Info => tracing::Level::INFO,
            Level::Debug => tracing::Level::DEBUG,
            Level::Trace => tracing::Level::TRACE,
        }
    }
}
