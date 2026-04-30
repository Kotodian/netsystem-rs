use serde::{Deserialize, Serialize};

// Numeric values intentionally match `sing/log` Level so that the i32 transmitted
// to Swift over uniffi stays compatible with the Go reference implementation.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_codes_match_go() {
        assert_eq!(Level::Panic.platform_code(), "P");
        assert_eq!(Level::Fatal.platform_code(), "F");
        assert_eq!(Level::Error.platform_code(), "E");
        assert_eq!(Level::Warn.platform_code(), "W");
        assert_eq!(Level::Info.platform_code(), "I");
        assert_eq!(Level::Debug.platform_code(), "D");
        assert_eq!(Level::Trace.platform_code(), "T");
    }

    #[test]
    fn discriminants_match_sing_log() {
        assert_eq!(Level::Panic as i32, 0);
        assert_eq!(Level::Fatal as i32, 1);
        assert_eq!(Level::Error as i32, 2);
        assert_eq!(Level::Warn as i32, 3);
        assert_eq!(Level::Info as i32, 4);
        assert_eq!(Level::Debug as i32, 5);
        assert_eq!(Level::Trace as i32, 6);
    }
}
