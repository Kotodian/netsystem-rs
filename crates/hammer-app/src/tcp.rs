//! TCP transport operations are owned by the session/transport layer.
//!
//! The app crate exposes session FIFO/message-queue handles only; TCP-specific
//! listener and stream facades stay in the session/transport layer.
