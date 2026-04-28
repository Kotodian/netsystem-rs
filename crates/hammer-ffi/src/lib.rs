mod error;
mod ffi;
mod platform;
mod service;

pub use error::HammerError;
pub use ffi::{check_config, format_config, new_service};
pub use hammer_core::config::parse_config;
pub use platform::{NetworkInterface, Platform, TunOptions, WifiState};
pub use service::Service;

uniffi::include_scaffolding!("hammer");
