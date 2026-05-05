#[cfg(feature = "outbound-block")]
pub mod block;
#[cfg(feature = "outbound-direct")]
pub mod direct;
#[cfg(feature = "outbound-hysteria2")]
pub mod hysteria2;
#[cfg(any(feature = "outbound-direct", feature = "probe"))]
pub(crate) mod icmp;
#[cfg(feature = "inbound-tun")]
pub mod tun;

#[cfg(feature = "wireguard")]
pub mod wireguard;
