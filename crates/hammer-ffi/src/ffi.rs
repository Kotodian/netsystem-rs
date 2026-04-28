use std::sync::Arc;

use hammer_core::config;

use crate::Platform;
use crate::error::HammerError;
use crate::service::Service;

/// `namespace.check_config` — uniffi 0.31 hands us the TOML body as an owned String.
pub fn check_config(content: String) -> Result<(), HammerError> {
    config::check_config(&content).map_err(HammerError::from)
}

/// `namespace.format_config` — returns the canonical TOML form. Equivalent to Go's
/// `FormatConfig` but returns a plain `String` instead of the gomobile-friendly
/// `StringValue` wrapper.
pub fn format_config(content: String) -> Result<String, HammerError> {
    config::format_config(&content).map_err(HammerError::from)
}

/// `namespace.new_service` — uniffi delivers callback interfaces as `Box<dyn Trait>`.
/// We immediately promote it to `Arc<dyn Platform>` so the Service can clone the
/// handle into the log writer and (later milestones) the network manager.
pub fn new_service(
    config_content: String,
    platform: Box<dyn Platform>,
) -> Result<Arc<Service>, HammerError> {
    let platform: Arc<dyn Platform> = Arc::from(platform);
    Service::new(&config_content, platform)
}
