extern crate self as hammer_service;

pub mod app;
pub mod data_plane;
/// Device-class abstraction. Concrete drivers live under `hammer-plugins/device/`.
pub mod device;
pub mod feature_arc;
/// Interface / adjacency control plane — shared infrastructure, not a plugin.
pub mod interface;
pub mod net;
pub mod opaque;
/// Session layer — shared infrastructure, not a plugin.
pub mod session;
pub mod trace;
/// Transport-neutral helpers. Protocol plugins live under `hammer-plugins/transport/`.
pub mod transport;

pub use hammer_core::error::{HammerError, HammerResult};

#[cfg(test)]
pub fn reset_subsystem_mains_for_test() {
    reset_subsystem_mains_for_plugin_test();
}

/// Test helper for plugin crates that cannot see `#[cfg(test)]` items on this crate.
pub fn reset_subsystem_mains_for_plugin_test() {
    crate::transport::reset_for_test();
    crate::net::reset_ip_main_for_test();
    crate::interface::reset_interface_main_for_test();
}
