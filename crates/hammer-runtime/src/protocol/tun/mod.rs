//! TUN module. Two largely independent pieces live here:
//!
//! * `inbound` (gated on `inbound-tun`) — the `TunInbound` lifecycle, the
//!   kernel TUN device adapters, and the system-socket-based packet-routing
//!   stack. This is what the iOS NetExt actually consumes.
//! * `ipstack` (gated on `ipstack`) — a generic smoltcp user-space TCP/IP
//!   stack, fed by an mpsc channel. Currently used by the WireGuard outbound;
//!   the TUN inbound's smoltcp mode will share it once it lands.

#[cfg(feature = "ipstack")]
pub(crate) mod ipstack;

#[cfg(feature = "inbound-tun")]
pub mod stack;

#[cfg(feature = "inbound-tun")]
pub use stack::*;

#[cfg(feature = "inbound-tun")]
mod inbound;

#[cfg(feature = "inbound-tun")]
pub use inbound::*;
