use thiserror::Error;

/// Internal error type used by foundational layers (config, log, lifecycle,
/// runtime registry). The user-facing `HammerError` lives in `hammer-ffi` and
/// converts from this through `From<CoreError>` — that split is what lets the
/// uniffi `udl_derive(Error)` macro stay inside `hammer-ffi` and avoid
/// orphan-rule violations.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("parse TOML: {message}")]
    ConfigParse { message: String },

    #[error("{message}")]
    ConfigValidation { message: String },

    #[error("{stage}: {message}")]
    Lifecycle { stage: String, message: String },

    #[error("service closed")]
    ServiceClosed,

    #[error("{message}")]
    Internal { message: String },
}

impl CoreError {
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

    pub fn service_closed() -> Self {
        Self::ServiceClosed
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

pub type CoreResult<T> = Result<T, CoreError>;

// Backwards-compatible aliases so M1 source layouts keep building during the
// migration window. New code should use `CoreError` / `CoreResult` directly.
pub type HammerError = CoreError;
pub type HammerResult<T> = CoreResult<T>;
