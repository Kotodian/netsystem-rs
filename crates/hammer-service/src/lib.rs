extern crate self as hammer_service;

pub mod app;
pub mod data_plane;
pub mod device;
mod event_subscribers;
pub mod interface;
pub mod net;
mod packet_graph;
mod service;
pub mod session;
mod trace;
pub mod transport;
pub mod tun;

pub mod adapter {
    pub use hammer_runtime::adapter::*;
}

pub use hammer_core::error::{HammerError, HammerResult};
pub use hammer_runtime::RuntimePlatform;
pub use service::RuntimeService;

#[cfg(test)]
pub(crate) fn reset_subsystem_mains_for_test() {
    crate::transport::reset_for_test();
    crate::transport::tcp::reset_for_test();
    crate::net::reset_ip_main_for_test();
}
