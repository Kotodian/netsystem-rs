//! Dynamic `udp` plugin (`libhammer_plugin_udp`).

use hammer_core::plugin::PluginRegistration;

static LOAD_AFTER: &[&str] = &[];

static REGISTRATION: PluginRegistration = PluginRegistration {
    name: "udp",
    version: env!("CARGO_PKG_VERSION"),
    version_required: env!("CARGO_PKG_VERSION"),
    load_after: LOAD_AFTER,
};

#[unsafe(no_mangle)]
pub extern "C" fn hammer_plugin_registration() -> *const PluginRegistration {
    &REGISTRATION
}

pub fn registration() -> &'static PluginRegistration {
    &REGISTRATION
}

pub mod input;

hammer_component_macros::declare_plugin!(name = "udp", load_after = []);

pub use input::{UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace};
