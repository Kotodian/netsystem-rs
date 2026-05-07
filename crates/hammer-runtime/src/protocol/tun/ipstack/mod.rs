//! Generic smoltcp user-space IP stack. Carrier-agnostic — the WireGuard
//! outbound feeds it via boringtun today, and the TUN inbound's smoltcp mode
//! will share this code when it lands.

pub(crate) mod device;
pub(crate) mod stack;

pub(crate) use device::ChannelDevice;
pub(crate) use stack::{IpStackHandles, TcpListener, UdpHandle, spawn_ipstack};
