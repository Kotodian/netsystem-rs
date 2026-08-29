//! Shared Protobuf Binary API envelope, typed client, and client-facing
//! errors. The daemon-side server (`hammer-service`) re-exports these and
//! keeps server ownership and Main Thread dispatch; external client
//! processes such as `hammerctl` use this module directly over a Tokio
//! Unix socket, mirroring VPP's separate vat2 client process.

use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use prost::Message;

/// Client-side frame limit for requests and replies. The server may
/// configure its own frame limit; this constant bounds what a client
/// process accepts or emits.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// One protobuf request frame: context correlates a reply to its request,
/// `method` names the registered Binary API method, and `payload` carries
/// the method's typed protobuf request.
#[derive(Clone, PartialEq, Message)]
pub struct BinaryApiRequest {
    #[prost(uint64, tag = "1")]
    pub context: u64,
    #[prost(string, tag = "2")]
    pub method: String,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// One protobuf reply frame carrying the transport-level status and the
/// method's typed protobuf reply payload.
#[derive(Clone, PartialEq, Message)]
pub struct BinaryApiReply {
    #[prost(uint64, tag = "1")]
    pub context: u64,
    #[prost(enumeration = "BinaryApiStatus", tag = "2")]
    pub status: i32,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum BinaryApiStatus {
    Ok = 0,
    InvalidRequest = 1,
    MethodMissing = 2,
    MethodDuplicate = 3,
    MethodPanicked = 4,
    MainThreadUnavailable = 5,
    Internal = 6,
}

/// Client-facing errors raised while calling the Binary API. Server-side
/// errors (bind, accept, frame I/O) stay with the server in `hammer-service`.
#[derive(Debug, thiserror::Error)]
pub enum BinaryApiError {
    #[error("Binary API frame length {bytes} exceeds maximum {maximum}")]
    FrameTooLarge { bytes: usize, maximum: usize },
    #[error("connect to Binary API Unix socket at `{path}`")]
    ClientConnect {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("write Binary API request `{method}`")]
    ClientWrite {
        method: String,
        #[source]
        source: io::Error,
    },
    #[error("read Binary API reply for `{method}`")]
    ClientRead {
        method: String,
        #[source]
        source: io::Error,
    },
    #[error("decode Binary API reply for `{method}`")]
    ClientReplyDecode {
        method: String,
        #[source]
        source: prost::DecodeError,
    },
    #[error("Binary API reply context mismatch for `{method}`: expected {expected}, got {actual}")]
    ClientReplyContext {
        method: String,
        expected: u64,
        actual: u64,
    },
    #[error("Binary API reply for `{method}` returned unknown status {status}")]
    ClientReplyStatusInvalid { method: String, status: i32 },
    #[error("Binary API request `{method}` was rejected with {status:?}")]
    ClientRejected {
        method: String,
        status: BinaryApiStatus,
    },
}

/// Blocking external client for the Binary API protobuf envelope. Mirrors
/// VPP's `vac_connect`/`vac_write` client: a separate process resolves the
/// method name and correlates replies by client context.
pub struct BinaryApiClient {
    stream: UnixStream,
    next_context: u64,
}

impl BinaryApiClient {
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, BinaryApiError> {
        let path = path.as_ref();
        let stream = UnixStream::connect(path).map_err(|source| BinaryApiError::ClientConnect {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self {
            stream,
            next_context: 1,
        })
    }

    pub fn call(&mut self, method: &str, payload: &[u8]) -> Result<Vec<u8>, BinaryApiError> {
        let context = self.next_context;
        self.next_context = self.next_context.wrapping_add(1);
        let frame = BinaryApiRequest {
            context,
            method: method.to_owned(),
            payload: payload.to_vec(),
        }
        .encode_to_vec();
        if frame.len() > DEFAULT_MAX_FRAME_BYTES {
            return Err(BinaryApiError::FrameTooLarge {
                bytes: frame.len(),
                maximum: DEFAULT_MAX_FRAME_BYTES,
            });
        }
        let frame_bytes = u32::try_from(frame.len()).expect("validated Binary API frame fits u32");
        self.stream
            .write_all(&frame_bytes.to_be_bytes())
            .and_then(|()| self.stream.write_all(&frame))
            .and_then(|()| self.stream.flush())
            .map_err(|source| BinaryApiError::ClientWrite {
                method: method.to_owned(),
                source,
            })?;

        let mut reply_length = [0_u8; size_of::<u32>()];
        self.stream
            .read_exact(&mut reply_length)
            .map_err(|source| BinaryApiError::ClientRead {
                method: method.to_owned(),
                source,
            })?;
        let reply_bytes = u32::from_be_bytes(reply_length) as usize;
        if reply_bytes > DEFAULT_MAX_FRAME_BYTES {
            return Err(BinaryApiError::FrameTooLarge {
                bytes: reply_bytes,
                maximum: DEFAULT_MAX_FRAME_BYTES,
            });
        }
        let mut frame = vec![0; reply_bytes];
        self.stream
            .read_exact(&mut frame)
            .map_err(|source| BinaryApiError::ClientRead {
                method: method.to_owned(),
                source,
            })?;
        let reply = BinaryApiReply::decode(frame.as_slice()).map_err(|source| {
            BinaryApiError::ClientReplyDecode {
                method: method.to_owned(),
                source,
            }
        })?;
        if reply.context != context {
            return Err(BinaryApiError::ClientReplyContext {
                method: method.to_owned(),
                expected: context,
                actual: reply.context,
            });
        }
        let status = BinaryApiStatus::try_from(reply.status).map_err(|_| {
            BinaryApiError::ClientReplyStatusInvalid {
                method: method.to_owned(),
                status: reply.status,
            }
        })?;
        if status != BinaryApiStatus::Ok {
            return Err(BinaryApiError::ClientRejected {
                method: method.to_owned(),
                status,
            });
        }
        Ok(reply.payload)
    }
}
