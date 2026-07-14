//! IPC protocol message definitions.

use hammer_infra::vec::Vec;
use serde::{Deserialize, Serialize};

/// Current protocol version sent on connect.
pub const PROTOCOL_VERSION: u32 = 1;

/// Request messages from client to daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum IpcRequest {
    Pause,
    Wake,
    ResetNetwork,
    Shutdown,
    Metrics { format: MetricsFormat },
    ConfigReload { toml: String },
    Status,
    ListListeners,
    ListSessions,
}

/// Reply messages from daemon to client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum IpcReply {
    Ok,
    Error(String),
    Metrics(Vec<u8>),
    Status(RuntimeStatus),
    Listeners(Vec<ListenerInfo>),
    Sessions(Vec<SessionInfo>),
}

/// Output format for metrics.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricsFormat {
    Table,
    Json,
    Prometheus,
}

/// Runtime status snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStatus {
    pub running: bool,
    pub n_workers: usize,
    pub n_sessions: usize,
    pub uptime_secs: u64,
}

/// Listener info for `ListListeners` reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerInfo {
    pub id: u64,
    pub protocol: String,
    pub address: String,
    pub port: u16,
}

/// Session info for `ListSessions` reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub id: u64,
    pub protocol: String,
    pub state: String,
    pub local_addr: String,
    pub remote_addr: String,
}
