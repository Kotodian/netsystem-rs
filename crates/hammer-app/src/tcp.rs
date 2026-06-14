//! TCP transport operations are owned by the session/transport layer.
//!
//! The app crate exposes op-owned ring helpers only; TCP-specific listener and
//! stream facades were removed with the app-ring data-area refactor.
