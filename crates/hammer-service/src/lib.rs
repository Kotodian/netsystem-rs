extern crate self as hammer_service;

pub mod app;
pub mod data_plane;
pub mod feature_arc;
pub mod opaque;
mod packet_graph;
mod trace;

#[cfg(feature = "plugin-device")]
pub mod device;
#[cfg(feature = "plugin-interface")]
pub mod interface;
#[cfg(feature = "plugin-ip")]
pub mod net;
#[cfg(feature = "plugin-session")]
pub mod session;
#[cfg(feature = "plugin-transport")]
pub mod transport;
#[cfg(feature = "plugin-tun")]
pub mod tun;

pub use hammer_core::error::{HammerError, HammerResult};

#[cfg(test)]
pub(crate) fn reset_subsystem_mains_for_test() {
    #[cfg(feature = "plugin-transport")]
    {
        crate::transport::reset_for_test();
        #[cfg(feature = "plugin-tcp")]
        crate::transport::tcp::reset_for_test();
    }
    #[cfg(feature = "plugin-ip")]
    crate::net::reset_ip_main_for_test();
    #[cfg(feature = "plugin-interface")]
    crate::interface::reset_interface_main_for_test();
}
