//! TUN inbound. `TunInbound` owns the kernel TUN device adapters and the
//! system-socket packet-routing stack consumed by the iOS NetExt; L3 endpoint
//! protocols (WireGuard etc.) are dispatched through `Endpoint::ip_send` via
//! the packet loop's `L3DispatchTable`.

#[cfg(feature = "inbound-tun")]
pub mod stack;

#[cfg(feature = "inbound-tun")]
pub use stack::*;

#[cfg(feature = "inbound-tun")]
mod fd_io;

#[cfg(feature = "inbound-tun")]
pub use fd_io::*;

#[cfg(feature = "inbound-tun")]
mod inbound;

#[cfg(feature = "inbound-tun")]
pub use inbound::*;
