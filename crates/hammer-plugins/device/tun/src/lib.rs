//! Dynamic `tun` device-driver plugin (`libhammer_plugin_tun`).

mod tun;

pub use tun::*;

use hammer_core::plugin::PluginRegistration;

static LOAD_AFTER: &[&str] = &[];

static REGISTRATION: PluginRegistration = PluginRegistration {
    name: "tun",
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
