// Handler traits are intentionally marker-only until a protocol needs to share
// concrete connection callbacks across crates.

pub trait ConnectionHandler: Send + Sync + 'static {}

pub trait PacketConnectionHandler: Send + Sync + 'static {}
