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

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::mem::size_of;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use prost::Message;

    use crate::binary_api::{
        BinaryApiClient, BinaryApiError, BinaryApiReply, BinaryApiRequest, BinaryApiStatus,
    };

    static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn socket_path() -> PathBuf {
        let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "hammer-ipc-binary-api-{}-{sequence}.sock",
            std::process::id()
        ))
    }

    /// Serves one request from a background thread and replies through the
    /// given responder, so the client framing is exercised without a daemon.
    /// The listener is bound in the calling thread first, so the client can
    /// never race a not-yet-bound socket.
    fn spawn_server(
        path: &PathBuf,
        respond: impl Fn(BinaryApiRequest) -> BinaryApiReply + Send + 'static,
    ) -> thread::JoinHandle<()> {
        let listener = UnixListener::bind(path).expect("bind Binary API test server");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept Binary API test client");
            let mut length = [0_u8; size_of::<u32>()];
            stream
                .read_exact(&mut length)
                .expect("read Binary API request length");
            let mut frame = vec![0; u32::from_be_bytes(length) as usize];
            stream
                .read_exact(&mut frame)
                .expect("read Binary API request frame");
            let request =
                BinaryApiRequest::decode(frame.as_slice()).expect("decode Binary API request");
            let reply = respond(request);
            let frame = reply.encode_to_vec();
            stream
                .write_all(&(frame.len() as u32).to_be_bytes())
                .expect("write Binary API reply length");
            stream
                .write_all(&frame)
                .expect("write Binary API reply frame");
        })
    }

    fn ok_reply(context: u64, payload: Vec<u8>) -> BinaryApiReply {
        BinaryApiReply {
            context,
            status: BinaryApiStatus::Ok as i32,
            payload,
        }
    }

    #[test]
    fn envelope_round_trips_context_method_and_payload() {
        let request = BinaryApiRequest {
            context: 7,
            method: "pause".to_owned(),
            payload: vec![1, 2, 3],
        };
        let decoded =
            BinaryApiRequest::decode(request.encode_to_vec().as_slice()).expect("decode request");
        assert_eq!(decoded, request);

        let reply = BinaryApiReply {
            context: 7,
            status: BinaryApiStatus::InvalidRequest as i32,
            payload: vec![4, 5],
        };
        let decoded =
            BinaryApiReply::decode(reply.encode_to_vec().as_slice()).expect("decode reply");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn status_maps_to_and_from_protobuf_ints() {
        assert_eq!(BinaryApiStatus::Ok as i32, 0);
        assert_eq!(BinaryApiStatus::InvalidRequest as i32, 1);
        assert_eq!(BinaryApiStatus::MethodMissing as i32, 2);
        assert_eq!(BinaryApiStatus::MethodDuplicate as i32, 3);
        assert_eq!(BinaryApiStatus::MethodPanicked as i32, 4);
        assert_eq!(BinaryApiStatus::MainThreadUnavailable as i32, 5);
        assert_eq!(BinaryApiStatus::Internal as i32, 6);
        for status in [
            BinaryApiStatus::Ok,
            BinaryApiStatus::InvalidRequest,
            BinaryApiStatus::MethodMissing,
            BinaryApiStatus::MethodDuplicate,
            BinaryApiStatus::MethodPanicked,
            BinaryApiStatus::MainThreadUnavailable,
            BinaryApiStatus::Internal,
        ] {
            assert_eq!(BinaryApiStatus::try_from(status as i32), Ok(status));
        }
        assert!(BinaryApiStatus::try_from(99).is_err());
    }

    #[test]
    fn client_call_round_trips_payload_and_context() {
        let path = socket_path();
        let server = spawn_server(&path, |request| {
            assert_eq!(request.context, 1);
            assert_eq!(request.method, "test.method");
            assert_eq!(request.payload, b"payload");
            ok_reply(request.context, Vec::from(b"reply".as_slice()))
        });

        let mut client = BinaryApiClient::connect(&path).expect("connect Binary API client");
        let payload = client
            .call("test.method", b"payload")
            .expect("call succeeds");
        assert_eq!(payload, b"reply");
        server.join().expect("join Binary API test server");
    }

    #[test]
    fn client_rejects_reply_with_mismatched_context() {
        let path = socket_path();
        let server = spawn_server(&path, |request| {
            ok_reply(request.context.wrapping_add(1), Vec::new())
        });

        let mut client = BinaryApiClient::connect(&path).expect("connect Binary API client");
        let error = client
            .call("test.method", &[])
            .expect_err("mismatched context must fail");
        assert!(matches!(
            error,
            BinaryApiError::ClientReplyContext {
                method,
                expected,
                actual,
            } if method == "test.method" && expected == 1 && actual == 2
        ));
        server.join().expect("join Binary API test server");
    }

    #[test]
    fn client_rejects_reply_with_unknown_status() {
        let path = socket_path();
        let server = spawn_server(&path, |request| BinaryApiReply {
            context: request.context,
            status: 99,
            payload: Vec::new(),
        });

        let mut client = BinaryApiClient::connect(&path).expect("connect Binary API client");
        let error = client
            .call("test.method", &[])
            .expect_err("unknown status must fail");
        assert!(matches!(
            error,
            BinaryApiError::ClientReplyStatusInvalid { method, status }
                if method == "test.method" && status == 99
        ));
        server.join().expect("join Binary API test server");
    }

    #[test]
    fn client_rejects_reply_with_non_ok_status() {
        let path = socket_path();
        let server = spawn_server(&path, |request| BinaryApiReply {
            context: request.context,
            status: BinaryApiStatus::MethodMissing as i32,
            payload: Vec::new(),
        });

        let mut client = BinaryApiClient::connect(&path).expect("connect Binary API client");
        let error = client
            .call("test.method", &[])
            .expect_err("non-ok status must fail");
        assert!(matches!(
            error,
            BinaryApiError::ClientRejected { method, status }
                if method == "test.method" && status == BinaryApiStatus::MethodMissing
        ));
        server.join().expect("join Binary API test server");
    }

    #[test]
    fn client_error_display_names_method_and_reason() {
        let error = BinaryApiError::ClientRejected {
            method: "shutdown".to_owned(),
            status: BinaryApiStatus::MethodMissing,
        };
        let message = error.to_string();
        assert!(message.contains("shutdown"), "unexpected error: {message}");
        assert!(
            message.contains("MethodMissing"),
            "unexpected error: {message}"
        );
    }
}
