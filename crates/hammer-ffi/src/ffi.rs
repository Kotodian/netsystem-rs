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

/// gomobile shipped a global utun-fd lookup. Keep the same fallback for iOS
/// hosts where `NEPacketTunnelFlow` no longer exposes a stable KVC fd path.
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
pub fn hammer_get_tunnel_file_descriptor() -> i32 {
    find_tunnel_file_descriptor(1024, is_apple_tunnel_file_descriptor).unwrap_or(-1)
}

/// Non-Apple platforms do not have NetworkExtension utun sockets.
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos")))]
pub fn hammer_get_tunnel_file_descriptor() -> i32 {
    -1
}

#[cfg(any(test, target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn find_tunnel_file_descriptor(
    max_fd: i32,
    mut is_tunnel_file_descriptor: impl FnMut(i32) -> bool,
) -> Option<i32> {
    (0..max_fd).find(|fd| is_tunnel_file_descriptor(*fd))
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn is_apple_tunnel_file_descriptor(fd: i32) -> bool {
    use std::ffi::CStr;

    let mut name = [0u8; libc::IFNAMSIZ];
    let mut len = name.len() as libc::socklen_t;
    let ret = unsafe {
        libc::getsockopt(
            fd,
            libc::SYSPROTO_CONTROL,
            libc::UTUN_OPT_IFNAME,
            name.as_mut_ptr().cast::<libc::c_void>(),
            &mut len,
        )
    };
    if ret != 0 || len == 0 {
        return false;
    }
    let name = unsafe { CStr::from_ptr(name.as_ptr().cast::<libc::c_char>()) };
    name.to_bytes().starts_with(b"utun")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tunnel_fd_scan_returns_first_matching_candidate() {
        let got = find_tunnel_file_descriptor(8, |fd| matches!(fd, 3 | 5));

        assert_eq!(got, Some(3));
    }
}
