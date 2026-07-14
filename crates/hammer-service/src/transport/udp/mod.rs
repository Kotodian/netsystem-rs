pub mod input;

#[cfg(feature = "plugin-udp")]
hammer_component_macros::declare_plugin!(name = "udp", load_after = ["transport"]);

pub use input::{UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace};
