mod error;
mod ffi;
mod platform;
mod service;

pub use error::HammerError;
pub use ffi::{
    hammer_check_config, hammer_format_config, hammer_get_tunnel_file_descriptor,
    hammer_new_service, hammer_setup,
};
pub use hammer_core::config::parse_config;
pub use platform::{
    HammerDefaultInterfaceUpdateListener, HammerNetworkInterface,
    HammerNetworkInterfaceIterator, HammerPlatform, HammerSetupOptions, HammerStringIterator,
    HammerTunOptions, HammerWIFIState,
};
pub use service::HammerService;

uniffi::include_scaffolding!("hammer");
