pub mod config;
pub mod error;
mod ffi;
pub mod log;
mod platform;
pub mod service;

pub use config::parse_config;
pub use error::HammerError;
// FFI entrypoints take owned `String` (uniffi requirement) and shadow any
// like-named helpers from `config` for crate-root callers.
pub use ffi::{check_config, format_config, new_service};
pub use platform::{NetworkInterface, Platform, TunOptions, WifiState};
pub use service::Service;

uniffi::include_scaffolding!("hammer");
