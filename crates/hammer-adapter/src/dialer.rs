pub use hammer_core::Network;

/// Marker for dial-capable runtime components.
pub trait Dialer: Send + Sync + 'static {}
