#![allow(dead_code)]

pub mod device;
pub mod stack;
#[cfg(feature = "ipstack-wireguard")]
pub mod wireguard;

pub use stack::{IpStackHandles, IpStackInput, TcpListener, UdpHandle, spawn_ipstack};
