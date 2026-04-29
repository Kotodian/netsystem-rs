use std::sync::Arc;

use hammer_core::config;

use crate::error::HammerError;
use crate::platform::{HammerPlatform, HammerSetupOptions};
use crate::service::HammerService;

/// Mirrors `HammerSetup(opts, &err)` from the gomobile SDK. Rust runtime needs
/// no basePath/tempPath/debug — the call exists so existing hosts can keep
/// invoking it during startup.
pub fn hammer_setup(_options: HammerSetupOptions) -> Result<(), HammerError> {
    Ok(())
}

pub fn hammer_check_config(content: String) -> Result<(), HammerError> {
    config::check_config(&content).map_err(HammerError::from)
}

pub fn hammer_format_config(content: String) -> Result<String, HammerError> {
    config::format_config(&content).map_err(HammerError::from)
}

pub fn hammer_new_service(
    config_content: String,
    platform: Box<dyn HammerPlatform>,
) -> Result<Arc<HammerService>, HammerError> {
    let platform: Arc<dyn HammerPlatform> = Arc::from(platform);
    HammerService::new(&config_content, platform)
}

/// gomobile shipped a global utun-fd lookup; the Rust SDK takes the fd directly
/// from `HammerPlatform.openTun`, so this returns -1 and the host should fall
/// back to its own discovery path.
pub fn hammer_get_tunnel_file_descriptor() -> i32 {
    -1
}
