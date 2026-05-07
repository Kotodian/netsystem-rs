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

/// gomobile shipped a global utun-fd lookup. Keep the scan as a last-resort
/// fallback for hosts where `NEPacketTunnelFlow` no longer exposes a stable
/// KVC fd path.
///
/// A NetworkExtension process may hold multiple `utun*` sockets at once
/// (NWPathMonitor, NEDNSProxy, stale tunnels from previous runs). Picking the
/// first match is unsafe — on iOS 16 the OS frequently leaves earlier utun
/// fds open while `NEPacketTunnelProvider.packetFlow` owns a different one,
/// and reading from the wrong device makes the real utun queue overflow with
/// `ctl_enqueuembuf failed: 55` (ENOBUFS).
///
/// We therefore refuse to choose when the scan is ambiguous, forcing the host
/// to use a precise SPI such as
/// `tunnel.packetFlow.value(forKeyPath: "socket.fileDescriptor")`.
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
pub fn hammer_get_tunnel_file_descriptor() -> i32 {
    find_unique_tunnel_file_descriptor(1024, is_apple_tunnel_file_descriptor).unwrap_or(-1)
}

/// Non-Apple platforms do not have NetworkExtension utun sockets.
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos")))]
pub fn hammer_get_tunnel_file_descriptor() -> i32 {
    -1
}

/// Reference implementation of the legacy "first-match" scan, kept for tests
/// that exercise the old semantics. Production code uses
/// [`find_unique_tunnel_file_descriptor`].
#[cfg(test)]
fn find_tunnel_file_descriptor(
    max_fd: i32,
    mut is_tunnel_file_descriptor: impl FnMut(i32) -> bool,
) -> Option<i32> {
    (0..max_fd).find(|fd| is_tunnel_file_descriptor(*fd))
}

/// Like [`find_tunnel_file_descriptor`], but returns `None` when more than one
/// fd matches. Used by [`hammer_get_tunnel_file_descriptor`] to refuse
/// ambiguous scans instead of silently picking a wrong tunnel device.
#[cfg(any(test, target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn find_unique_tunnel_file_descriptor(
    max_fd: i32,
    mut is_tunnel_file_descriptor: impl FnMut(i32) -> bool,
) -> Option<i32> {
    let mut found: Option<i32> = None;
    for fd in 0..max_fd {
        if is_tunnel_file_descriptor(fd) {
            if found.is_some() {
                return None;
            }
            found = Some(fd);
        }
    }
    found
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

    #[test]
    fn unique_scan_returns_sole_candidate() {
        let got = find_unique_tunnel_file_descriptor(8, |fd| fd == 5);

        assert_eq!(got, Some(5));
    }

    #[test]
    fn unique_scan_refuses_when_ambiguous() {
        let got = find_unique_tunnel_file_descriptor(8, |fd| matches!(fd, 3 | 5));

        assert_eq!(got, None);
    }

    #[test]
    fn unique_scan_returns_none_when_no_match() {
        let got = find_unique_tunnel_file_descriptor(8, |_| false);

        assert_eq!(got, None);
    }
}
