use thiserror::Error;

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
    pub fn config_parse(message: impl Into<String>) -> Self {
        Self::ConfigParse {
            message: message.into(),
        }
    }

    pub fn config_validation(message: impl Into<String>) -> Self {
        Self::ConfigValidation {
            message: message.into(),
        }
    }

    pub fn lifecycle(stage: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Lifecycle {
            stage: stage.into(),
            message: message.into(),
        }
    }

    pub fn platform(message: impl Into<String>) -> Self {
        Self::Platform {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

pub type HammerResult<T> = Result<T, HammerError>;
