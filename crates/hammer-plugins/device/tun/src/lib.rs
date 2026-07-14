//! Dynamic `tun` device-driver plugin (`libhammer_plugin_tun`).

mod tun;

pub use tun::*;

hammer_component_macros::declare_plugin!(name = "tun", load_after = []);
