use std::error::Error;
use std::fs;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::PathBuf;

use hammer_stats::{StatsError, StatsMain};

fn test_socket_path() -> PathBuf {
    PathBuf::from(format!("/tmp/hs-{}.sock", std::process::id()))
}

#[test]
fn stats_listener_matches_vpp_socket_configuration() -> Result<(), Box<dyn Error>> {
    let socket_path = test_socket_path();
    let _ = fs::remove_file(&socket_path);
    let _listener = StatsMain::init("stats-listener-test", 2 * 1024 * 1024, &socket_path)?;
    let stats = StatsMain::global()?;

    assert!(socket_path.exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = fs::metadata(&socket_path)?.permissions().mode();
        assert_ne!(mode & 0o020, 0, "socket must be group-writable");
        assert_eq!(mode & 0o002, 0, "socket must not be other-writable");
    }

    #[cfg(target_os = "linux")]
    {
        let mut enabled: libc::c_int = 0;
        let mut length = std::mem::size_of_val(&enabled) as libc::socklen_t;
        // SAFETY: `listener` owns a live Unix socket and both output pointers
        // refer to valid writable storage for the duration of this call.
        let result = unsafe {
            libc::getsockopt(
                _listener.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PASSCRED,
                std::ptr::from_mut(&mut enabled).cast(),
                &mut length,
            )
        };
        assert_eq!(result, 0);
        assert_eq!(enabled, 1, "VPP-compatible listener must enable passcred");
    }

    let repeat_path = PathBuf::from(format!("/tmp/hs-{}-repeat.sock", std::process::id()));
    let _ = fs::remove_file(&repeat_path);
    assert!(matches!(
        StatsMain::init("stats-listener-repeat", 2 * 1024 * 1024, &socket_path),
        Err(StatsError::AlreadyInitialized)
    ));
    assert!(matches!(
        StatsMain::init("stats-listener-repeat", 2 * 1024 * 1024, &repeat_path),
        Err(StatsError::AlreadyInitialized)
    ));
    assert!(!repeat_path.exists());

    stats.unlink_socket_path()?;
    Ok(())
}
