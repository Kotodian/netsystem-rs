//! WireGuard runtime components.

#[cfg(feature = "endpoint-amneziawg")]
mod amnezia2;
mod endpoint;
mod transport;

pub use endpoint::{WireguardEndpoint, WireguardPeerStartArgs, WireguardStartHandshakeSubscriber};
