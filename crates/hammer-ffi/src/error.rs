use hammer_core::error::CoreError;
use thiserror::Error;

/// User-facing error surfaced through uniffi to Swift. Mirrors the `[Error]`
/// interface in `src/hammer.udl`. Lives in `hammer-ffi` (not `hammer-core`)
/// so that `uniffi::udl_derive(Error)` can implement uniffi's `Lower`/`Lift`
/// traits without violating Rust's orphan rule.
#[derive(Debug, Error)]
pub enum HammerError {
    #[error("parse TOML: {message}")]
    ConfigParse { message: String },

    #[error("{message}")]
    ConfigValidation { message: String },

    #[error("HammerPlatform is required")]
    PlatformMissing,

    #[error("service closed")]
    ServiceClosed,

    #[error("service already started")]
    ServiceAlreadyStarted,

    #[error("{stage}: {message}")]
    Lifecycle { stage: String, message: String },

    #[error("platform: {message}")]
    Platform { message: String },

    #[error("{message}")]
    Internal { message: String },
}

impl HammerError {
    pub fn lifecycle(stage: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Lifecycle {
            stage: stage.into(),
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn platform(message: impl Into<String>) -> Self {
        Self::Platform {
            message: message.into(),
        }
    }
}

impl From<CoreError> for HammerError {
    fn from(err: CoreError) -> Self {
        match err {
            CoreError::ConfigParse { message } => HammerError::ConfigParse { message },
            CoreError::ConfigValidation { message } => HammerError::ConfigValidation { message },
            CoreError::Lifecycle { stage, message } => HammerError::Lifecycle { stage, message },
            CoreError::Internal { message } => HammerError::Internal { message },
        }
    }
}
