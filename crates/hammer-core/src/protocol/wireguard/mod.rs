#[cfg(feature = "amneziawg")]
pub mod amnezia2;
pub mod peer;

/// Tunnel-side overhead added to every IP packet by WireGuard's data frame
/// (16 byte poly1305 tag + 16 byte header).
pub const WIREGUARD_OVERHEAD: usize = 32;
