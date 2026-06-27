use hammer_core::error::{HammerError, HammerResult};

/// Server side of the Unix-domain-socket attach protocol.
/// The dataplane binds a listener, accepts app connections, and sends
/// shared-memory segment fds + offset layout to the app process.
pub struct AttachServer {
    listener: std::os::unix::net::UnixListener,
}

impl AttachServer {
    /// Bind to a Unix domain socket at `path`.
    pub fn bind(path: &str) -> HammerResult<Self> {
        let _ = std::fs::remove_file(path);
        let listener = std::os::unix::net::UnixListener::bind(path)
            .map_err(|e| {
                HammerError::internal(format!("failed to bind attach server at {path}: {e}"))
            })?;
        Ok(Self { listener })
    }
}
