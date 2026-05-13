//! WireGuard runtime components.

mod endpoint;
mod transport;

pub use endpoint::WireguardEndpoint;
#[cfg(test)]
pub(crate) use transport::ENCRYPT_QUEUE as TEST_ENCRYPT_QUEUE;
