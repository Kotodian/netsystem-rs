//! Server-owned Binary API protocol and shared-memory transport definitions.
//! The daemon-side server (`hammer-service`) re-exports the socket envelope
//! while keeping handler registration, Main Thread dispatch, and lifecycle
//! ownership on the server side.

extern crate self as hammer_ipc;

pub mod binary_api;
