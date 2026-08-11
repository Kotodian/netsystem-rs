//! hammer-vcl: an explicit native VCL-shaped Session API for Hammer.
//!
//! Mirrors VPP's `vcl_worker_t` / `vcl_session_t` model over Hammer's
//! attach and shared-memory Application Session path (VPP `vcl_private.h`,
//! `vppcom.c`):
//!
//! - a client-local [`VclWorker`] owning a fixed-capacity,
//!   generation-safe local Session pool ([`VclSessionHandle`]);
//! - VPP-shaped local states with a precise nonblocking connecting interim
//!   ([`VclSessionState::Connecting`]);
//! - a two-step API: [`VclWorker::session_create`] then blocking or
//!   nonblocking [`VclWorker::session_stream_connect`];
//! - peer-open children created from the ACCEPTED event with an explicit
//!   ACCEPTED_REPLY;
//! - typed `thiserror` errors preserving the owning layer's source
//!   (`AppClientError` -> `SessionConnectError` / `SessionControlError` /
//!   `AppSessionError`); no numeric status and no panic-based error
//!   replacement.
//!
//! Security and API boundary: this is an explicit native Rust library only.
//! There is no `LD_PRELOAD`, no LDP, no libc syscall interception, no socket
//! interposition, and no environment compatibility shim. Unix sockets are
//! limited to attach/bootstrap and descriptor passing.
//!
//! This crate is additive: `hammer-vcl` depends on `hammer-app`, which
//! depends on `hammer-runtime`. There is no reverse dependency.

mod error;
mod pool;
mod session;
mod worker;

pub use error::VclError;
pub use pool::VclSessionHandle;
pub use session::{VclDirection, VclInitiator, VclSession, VclSessionAttributes, VclSessionState};
pub use worker::{VclEvent, VclWorker};
