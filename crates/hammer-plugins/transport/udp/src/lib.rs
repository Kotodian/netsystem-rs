//! Dynamic `udp` plugin (`libhammer_plugin_udp`).

pub mod input;

hammer_component_macros::declare_plugin!(name = "udp", load_after = ["ip"]);

pub use input::{UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace};
