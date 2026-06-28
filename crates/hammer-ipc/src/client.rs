//! IPC client for hammerctl.

use std::os::unix::net::UnixStream;
use std::time::Duration;

use crate::{IpcError, IpcReply, IpcRequest, frame};

/// IPC client for connecting to the hammer daemon.
pub struct IpcClient {
    stream: UnixStream,
}

impl IpcClient {
    /// Connect to the hammer daemon at the given socket path.
    pub fn connect(path: &str) -> Result<Self, IpcError> {
        let stream = UnixStream::connect(path)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Ok(Self { stream })
    }

    /// Send a request and wait for the reply.
    pub fn request(&mut self, req: IpcRequest) -> Result<IpcReply, IpcError> {
        let payload = bincode::serialize(&req)?;
        frame::write_frame(&mut self.stream, &payload)?;
        let payload = frame::read_frame(&mut self.stream)?;
        let reply = bincode::deserialize(&payload)?;
        Ok(reply)
    }
}
