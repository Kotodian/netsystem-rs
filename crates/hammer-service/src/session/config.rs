//! Session-owned configuration under `[network.session]`.

use hammer_runtime::{RuntimeError, RuntimeResult};

const SESSION_POOL_CAPACITY: usize = 1_024;
const APP_SESSION_CAPACITY: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Session {
    #[serde(default)]
    pub attach_socket_path: Option<String>,
    #[serde(alias = "preallocated_sessions")]
    pub pool_capacity: usize,
    pub app_session_capacity: usize,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            attach_socket_path: None,
            pool_capacity: SESSION_POOL_CAPACITY,
            app_session_capacity: APP_SESSION_CAPACITY,
        }
    }
}

impl Session {
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.pool_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "network.session.pool_capacity must be non-zero",
            ));
        }
        if self.app_session_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "network.session.app_session_capacity must be non-zero",
            ));
        }
        Ok(())
    }
}

/// The session owner receives the `[network]` table and ignores sections it
/// does not own. `None` preserves the distinction between an absent session
/// section and a session section populated with defaults.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
pub struct NetworkSessionConfig {
    pub session: Option<Session>,
}
