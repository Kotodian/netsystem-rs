//! IPC layer for hammer control plane (vlibmemory + vlibapi equivalent).
//!
//! Provides Unix socket transport, message framing, and request/response
//! dispatch for the `hammer` daemon and `hammerctl` client.

pub mod client;
pub mod frame;
pub mod protocol;
pub mod server;

pub use client::IpcClient;
pub use frame::{IpcError, read_frame, write_frame};
pub use protocol::{
    IpcReply, IpcRequest, ListenerInfo, MetricsFormat, PROTOCOL_VERSION, RuntimeStatus, SessionInfo,
};
pub use server::IpcServer;
