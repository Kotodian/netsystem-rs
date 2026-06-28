//! IPC server for the hammer daemon.

use std::io::Read;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};

use crate::{IpcError, IpcReply, IpcRequest, frame};

/// IPC server that accepts connections and dispatches requests.
pub struct IpcServer {
    listener: UnixListener,
    registrations: Vec<Registration>,
}

impl IpcServer {
    /// Bind to the given Unix socket path.
    pub fn bind(path: &str) -> Result<Self, IpcError> {
        let _ = std::fs::remove_file(path);
        let listener = UnixListener::bind(path)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            registrations: Vec::new(),
        })
    }

    /// Get the listener's raw file descriptor for epoll/kqueue registration.
    pub fn raw_fd(&self) -> i32 {
        self.listener.as_raw_fd()
    }

    /// Accept new connections and register them.
    pub fn accept(&mut self) -> Result<Option<u32>, IpcError> {
        match self.listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(true)?;
                let reg = Registration::new(stream);
                let idx = self.registrations.len() as u32;
                self.registrations.push(reg);
                Ok(Some(idx))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Process readable data for a registration.
    /// Returns the parsed request if a full frame was received.
    pub fn read_ready(&mut self, idx: u32) -> Result<Option<IpcRequest>, IpcError> {
        let reg = &mut self.registrations[idx as usize];
        reg.fill_read_buffer()?;
        if let Some(payload) = reg.try_parse_frame()? {
            let req = bincode::deserialize(&payload)?;
            Ok(Some(req))
        } else {
            Ok(None)
        }
    }

    /// Write a reply to a registration.
    pub fn write_reply(&mut self, idx: u32, reply: &IpcReply) -> Result<(), IpcError> {
        let reg = &mut self.registrations[idx as usize];
        let payload = bincode::serialize(reply)?;
        frame::write_frame(&mut reg.stream, &payload)?;
        Ok(())
    }

    /// Remove a registration (client disconnected).
    pub fn remove(&mut self, idx: u32) {
        if (idx as usize) < self.registrations.len() {
            self.registrations.remove(idx as usize);
        }
    }
}

/// Per-connection registration state.
pub struct Registration {
    stream: UnixStream,
    read_buffer: Vec<u8>,
}

impl Registration {
    fn new(stream: UnixStream) -> Self {
        Self {
            stream,
            read_buffer: Vec::with_capacity(4096),
        }
    }

    fn fill_read_buffer(&mut self) -> Result<(), IpcError> {
        let mut buf = [0u8; 4096];
        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => return Err(IpcError::ConnectionClosed),
                Ok(n) => self.read_buffer.extend_from_slice(&buf[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn try_parse_frame(&mut self) -> Result<Option<Vec<u8>>, IpcError> {
        if self.read_buffer.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([
            self.read_buffer[0],
            self.read_buffer[1],
            self.read_buffer[2],
            self.read_buffer[3],
        ]) as usize;
        if self.read_buffer.len() < 4 + len {
            return Ok(None);
        }
        let payload = self.read_buffer[4..4 + len].to_vec();
        self.read_buffer.drain(0..4 + len);
        Ok(Some(payload))
    }
}
