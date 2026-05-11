#[cfg(feature = "outbound-block")]
pub mod block;
#[cfg(feature = "outbound-direct")]
pub mod direct;
#[cfg(feature = "outbound-hysteria2")]
pub mod hysteria2;
#[cfg(any(feature = "outbound-direct", feature = "probe"))]
pub(crate) mod icmp;
#[cfg(feature = "ipstack")]
pub mod endpoint;
#[cfg(any(feature = "inbound-tun", feature = "ipstack"))]
pub mod tun;
#[cfg(feature = "outbound-urltest")]
pub mod urltest;
