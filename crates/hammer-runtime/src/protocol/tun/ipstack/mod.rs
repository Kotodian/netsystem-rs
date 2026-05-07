//! Generic smoltcp user-space IP stack. Carrier-agnostic — the WireGuard
//! outbound feeds it via boringtun today, and the TUN inbound's smoltcp mode
//! will share this code when it lands.
//!
//! Until the second consumer arrives, parts of the surface (e.g. TcpListener,
//! AcceptTcp) are reachable only through the wg outbound's narrow `dial` path
//! plus its tests. Mirror the `wireguard/mod.rs:14` convention and silence
//! dead-code lints crate-wide rather than littering individual `#[allow]`s.

#![allow(dead_code)]

pub(crate) mod device;
pub(crate) mod stack;

pub(crate) use stack::{IpStackHandles, UdpHandle, spawn_ipstack};
