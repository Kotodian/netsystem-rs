//! Session-owned configuration under `[network.session]`.

use std::time::Duration;

use hammer_runtime::{RuntimeError, RuntimeResult};

const SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
const SESSION_POOL_CAPACITY: usize = 1_024;
const READY_QUEUE_CAPACITY: usize = 1_024;
const APP_SESSION_CAPACITY: usize = 1_024;
const OOO_CAPACITY: usize = 8;
const SESSION_BUFFER_SLOT_BYTES: usize = 2_048;
const SESSION_BUFFER_SLOTS: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Session {
    #[serde(default)]
    pub attach_socket_path: Option<String>,
    #[serde(with = "humantime_serde")]
    pub timer_tick: Duration,
    #[serde(alias = "preallocated_sessions")]
    pub pool_capacity: usize,
    #[serde(alias = "event_queue_length")]
    pub ready_queue_capacity: usize,
    pub app_session_capacity: usize,
    pub ooo_capacity: usize,
    pub buffer: SessionBuffer,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            attach_socket_path: None,
            timer_tick: SESSION_TIMER_TICK,
            pool_capacity: SESSION_POOL_CAPACITY,
            ready_queue_capacity: READY_QUEUE_CAPACITY,
            app_session_capacity: APP_SESSION_CAPACITY,
            ooo_capacity: OOO_CAPACITY,
            buffer: SessionBuffer::default(),
        }
    }
}

impl Session {
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.timer_tick.is_zero() {
            return Err(RuntimeError::config_validation(
                "network.session.timer_tick must be non-zero",
            ));
        }
        if self.pool_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "network.session.pool_capacity must be non-zero",
            ));
        }
        if self.ready_queue_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "network.session.ready_queue_capacity must be non-zero",
            ));
        }
        if self.app_session_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "network.session.app_session_capacity must be non-zero",
            ));
        }
        self.buffer.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SessionBuffer {
    pub slot_bytes: usize,
    pub slots: usize,
}

impl Default for SessionBuffer {
    fn default() -> Self {
        Self {
            slot_bytes: SESSION_BUFFER_SLOT_BYTES,
            slots: SESSION_BUFFER_SLOTS,
        }
    }
}

impl SessionBuffer {
    fn validate(&self) -> RuntimeResult<()> {
        if self.slot_bytes == 0 {
            return Err(RuntimeError::config_validation(
                "network.session.buffer.slot_bytes must be non-zero",
            ));
        }
        if self.slots == 0 {
            return Err(RuntimeError::config_validation(
                "network.session.buffer.slots must be non-zero",
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
