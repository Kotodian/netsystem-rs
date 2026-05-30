pub mod input;
pub mod parse;

pub use input::{IpInputNext, IpInputNode};
pub use parse::{IpInputTarget, IpProtocol, ParsedIpPacket, parse_ip_packet};
