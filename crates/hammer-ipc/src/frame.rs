//! Message framing: [u32 BE length][payload]
//! Error types for the IPC layer.

use hammer_infra::vec::Vec;
use std::io::{Read, Write};

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024; // 16 MB
const MAX_FRAME_SIZE_STR: &str = "16 MB";

/// IPC protocol error.
#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame too large: {0} bytes (max {MAX_FRAME_SIZE_STR})")]
    FrameTooLarge(usize),
    #[error("bincode error: {0}")]
    Bincode(String),
    #[error("connection closed")]
    ConnectionClosed,
}

impl From<bincode::Error> for IpcError {
    fn from(e: bincode::Error) -> Self {
        IpcError::Bincode(e.to_string())
    }
}

/// Read a length-prefixed frame from a sync stream.
/// Frame format: [4 bytes BE u32 length][payload]
pub fn read_frame<R: Read>(stream: &mut R) -> Result<Vec<u8>, IpcError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(IpcError::FrameTooLarge(len));
    }
    let mut payload = hammer_infra::vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

/// Read a length-prefixed frame from an async stream.
pub async fn async_read_frame(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
    buf: &mut [u8],
) -> Result<Option<Vec<u8>>, IpcError> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Ok(None);
    }
    if len > MAX_FRAME_SIZE {
        return Err(IpcError::FrameTooLarge(len));
    }
    if buf.len() < len {
        return Err(IpcError::FrameTooLarge(len));
    }
    stream.read_exact(&mut buf[..len]).await?;
    Ok(Some(Vec::from(&buf[..len])))
}

/// Write a length-prefixed frame to an async stream.
pub async fn async_write_frame(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    payload: &[u8],
) -> Result<(), IpcError> {
    use tokio::io::AsyncWriteExt;
    let len = payload.len();
    if len > MAX_FRAME_SIZE {
        return Err(IpcError::FrameTooLarge(len));
    }
    stream.write_all(&(len as u32).to_be_bytes()).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

/// Write a length-prefixed frame to the stream.
/// Frame format: [4 bytes BE u32 length][payload]
pub fn write_frame<W: Write>(stream: &mut W, payload: &[u8]) -> Result<(), IpcError> {
    let len = payload.len();
    if len > MAX_FRAME_SIZE {
        return Err(IpcError::FrameTooLarge(len));
    }
    stream.write_all(&(len as u32).to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}
