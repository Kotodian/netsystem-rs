//! Protobuf Binary API served on a Main Thread Tokio Unix socket.

use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, ThreadId};

use hammer_runtime::binary_api::BinaryApiMethodStatus;
use hammer_runtime::{Engine, PluginError, RuntimeError, RuntimeResult, WorkerBarrier};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, PartialEq, Message)]
pub struct BinaryApiRequest {
    #[prost(uint64, tag = "1")]
    pub context: u64,
    #[prost(string, tag = "2")]
    pub method: String,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

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

#[hammer_component_macros::runtime_error(subsystem = "binary api")]
#[derive(Debug, thiserror::Error)]
pub enum BinaryApiError {
    #[error("Binary API socket path is empty")]
    SocketPathEmpty,
    #[error("Binary API maximum frame size {bytes} is invalid")]
    FrameSizeInvalid { bytes: usize },
    #[error("bind Binary API Unix socket at `{path}`")]
    SocketBind {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("configure Binary API Unix socket as nonblocking")]
    SocketNonblocking {
        #[source]
        source: io::Error,
    },
    #[error("clone Binary API Unix listener")]
    ListenerClone {
        #[source]
        source: io::Error,
    },
    #[error("register Binary API Unix listener with Tokio")]
    ListenerRegistration {
        #[source]
        source: io::Error,
    },
    #[error("accept Binary API connection")]
    Accept {
        #[source]
        source: io::Error,
    },
    #[error("Binary API must run on its owning Main Thread")]
    WrongThread,
    #[error("read Binary API frame")]
    FrameRead {
        #[source]
        source: io::Error,
    },
    #[error("write Binary API frame")]
    FrameWrite {
        #[source]
        source: io::Error,
    },
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    socket_path: Option<String>,
    max_frame_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_path: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

impl Config {
    fn validate(&self) -> Result<(), BinaryApiError> {
        if self
            .socket_path
            .as_ref()
            .is_some_and(|path| path.trim().is_empty())
        {
            return Err(BinaryApiError::SocketPathEmpty);
        }
        if self.max_frame_bytes == 0 || self.max_frame_bytes > u32::MAX as usize {
            return Err(BinaryApiError::FrameSizeInvalid {
                bytes: self.max_frame_bytes,
            });
        }
        Ok(())
    }
}

/// Main Thread owner of the Binary API Unix listener and frame policy.
pub struct BinaryApiMain {
    listener: StdUnixListener,
    socket_path: PathBuf,
    socket_device: u64,
    socket_inode: u64,
    connection: Arc<BinaryApiConnection>,
}

/// Blocking external client for the Binary API protobuf envelope.
pub struct BinaryApiClient {
    stream: StdUnixStream,
    next_context: u64,
}

impl BinaryApiClient {
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, BinaryApiError> {
        let path = path.as_ref();
        let stream =
            StdUnixStream::connect(path).map_err(|source| BinaryApiError::ClientConnect {
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

struct BinaryApiConnection {
    max_frame_bytes: usize,
    owner: ThreadId,
    barrier: Option<WorkerBarrier>,
}

impl BinaryApiMain {
    pub fn bind(path: impl AsRef<Path>, max_frame_bytes: usize) -> Result<Self, BinaryApiError> {
        if max_frame_bytes == 0 || max_frame_bytes > u32::MAX as usize {
            return Err(BinaryApiError::FrameSizeInvalid {
                bytes: max_frame_bytes,
            });
        }
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Err(BinaryApiError::SocketPathEmpty);
        }
        let listener = bind_listener(path).map_err(|source| BinaryApiError::SocketBind {
            path: path.to_path_buf(),
            source,
        })?;
        let metadata = std::fs::metadata(path).map_err(|source| BinaryApiError::SocketBind {
            path: path.to_path_buf(),
            source,
        })?;
        listener
            .set_nonblocking(true)
            .map_err(|source| BinaryApiError::SocketNonblocking { source })?;
        Ok(Self {
            listener,
            socket_path: path.to_path_buf(),
            socket_device: metadata.dev(),
            socket_inode: metadata.ino(),
            connection: Arc::new(BinaryApiConnection {
                max_frame_bytes,
                owner: thread::current().id(),
                barrier: Engine::with_current(|engine| engine.worker_barrier()),
            }),
        })
    }

    pub async fn serve(self: Arc<Self>) -> Result<(), BinaryApiError> {
        self.connection.ensure_main_thread()?;
        let listener = self
            .listener
            .try_clone()
            .map_err(|source| BinaryApiError::ListenerClone { source })?;
        let listener = tokio::net::UnixListener::from_std(listener)
            .map_err(|source| BinaryApiError::ListenerRegistration { source })?;
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .map_err(|source| BinaryApiError::Accept { source })?;
            let connection = Arc::clone(&self.connection);
            tokio::spawn(async move {
                if let Err(error) = connection.serve(stream).await {
                    tracing::warn!(%error, "Binary API connection closed after failure");
                }
            });
        }
    }
}

impl BinaryApiConnection {
    async fn serve(&self, stream: tokio::net::UnixStream) -> Result<(), BinaryApiError> {
        self.ensure_main_thread()?;
        let (mut reader, mut writer) = stream.into_split();
        loop {
            let Some(frame) = read_frame(&mut reader, self.max_frame_bytes).await? else {
                return Ok(());
            };
            let reply = match BinaryApiRequest::decode(frame.as_slice()) {
                Ok(request) => self.dispatch(request),
                Err(_) => reply(0, BinaryApiStatus::InvalidRequest, Vec::new()),
            };
            write_frame(&mut writer, &reply.encode_to_vec(), self.max_frame_bytes).await?;
        }
    }

    fn ensure_main_thread(&self) -> Result<(), BinaryApiError> {
        if thread::current().id() == self.owner {
            Ok(())
        } else {
            Err(BinaryApiError::WrongThread)
        }
    }

    fn dispatch(&self, request: BinaryApiRequest) -> BinaryApiReply {
        let Some(barrier) = &self.barrier else {
            return dispatch_method(request);
        };
        if barrier.is_pending() {
            return dispatch_method(request);
        }
        let mut control = ();
        barrier.sync(&mut control, |_| dispatch_method(request))
    }
}

impl Drop for BinaryApiMain {
    fn drop(&mut self) {
        let metadata = match std::fs::metadata(&self.socket_path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return,
            Err(source) => {
                tracing::warn!(
                    path = %self.socket_path.display(),
                    %source,
                    "failed to inspect Binary API socket during cleanup"
                );
                return;
            }
        };
        if metadata.dev() != self.socket_device || metadata.ino() != self.socket_inode {
            return;
        }
        if let Err(source) = std::fs::remove_file(&self.socket_path) {
            tracing::warn!(
                path = %self.socket_path.display(),
                %source,
                "failed to remove Binary API socket during cleanup"
            );
        }
    }
}

fn bind_listener(path: &Path) -> io::Result<StdUnixListener> {
    match StdUnixListener::bind(path) {
        Ok(listener) => Ok(listener),
        Err(bind_error) if bind_error.kind() == io::ErrorKind::AddrInUse => {
            match StdUnixStream::connect(path) {
                Ok(_) => Err(bind_error),
                Err(source) if source.kind() == io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path)?;
                    StdUnixListener::bind(path)
                }
                Err(source) if source.kind() == io::ErrorKind::NotFound => {
                    StdUnixListener::bind(path)
                }
                Err(_) => Err(bind_error),
            }
        }
        Err(source) => Err(source),
    }
}

fn dispatch_method(request: BinaryApiRequest) -> BinaryApiReply {
    let context = request.context;
    let resolved =
        Engine::with_current(|engine| engine.plugin_main().binary_api_method(&request.method));
    let method = match resolved {
        None => {
            return reply(context, BinaryApiStatus::MainThreadUnavailable, Vec::new());
        }
        Some(Err(PluginError::BinaryApiMethodMissing { .. })) => {
            return reply(context, BinaryApiStatus::MethodMissing, Vec::new());
        }
        Some(Err(PluginError::BinaryApiMethodDuplicate { .. })) => {
            return reply(context, BinaryApiStatus::MethodDuplicate, Vec::new());
        }
        Some(Err(_)) => return reply(context, BinaryApiStatus::Internal, Vec::new()),
        Some(Ok(method)) => method,
    };
    let method_reply = method.call(&request.payload);
    let status = match method_reply.status() {
        BinaryApiMethodStatus::Ok => BinaryApiStatus::Ok,
        BinaryApiMethodStatus::InvalidRequest => BinaryApiStatus::InvalidRequest,
        BinaryApiMethodStatus::Panicked => BinaryApiStatus::MethodPanicked,
    };
    reply(context, status, method_reply.payload().to_vec())
}

fn reply(context: u64, status: BinaryApiStatus, payload: Vec<u8>) -> BinaryApiReply {
    BinaryApiReply {
        context,
        status: status as i32,
        payload,
    }
}

async fn read_frame(
    reader: &mut (impl AsyncRead + Unpin),
    maximum: usize,
) -> Result<Option<Vec<u8>>, BinaryApiError> {
    let mut length = [0_u8; size_of::<u32>()];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(source) => return Err(BinaryApiError::FrameRead { source }),
    }
    let bytes = u32::from_be_bytes(length) as usize;
    if bytes > maximum {
        return Err(BinaryApiError::FrameTooLarge { bytes, maximum });
    }
    let mut frame = vec![0; bytes];
    reader
        .read_exact(&mut frame)
        .await
        .map_err(|source| BinaryApiError::FrameRead { source })?;
    Ok(Some(frame))
}

async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    frame: &[u8],
    maximum: usize,
) -> Result<(), BinaryApiError> {
    if frame.len() > maximum {
        return Err(BinaryApiError::FrameTooLarge {
            bytes: frame.len(),
            maximum,
        });
    }
    writer
        .write_all(&(frame.len() as u32).to_be_bytes())
        .await
        .map_err(|source| BinaryApiError::FrameWrite { source })?;
    writer
        .write_all(frame)
        .await
        .map_err(|source| BinaryApiError::FrameWrite { source })?;
    writer
        .flush()
        .await
        .map_err(|source| BinaryApiError::FrameWrite { source })
}

#[hammer_component_macros::config_function(
    name = "binary_api_config",
    section = "binary_api",
    early = true
)]
fn configure(config: Config) -> RuntimeResult<Arc<Config>> {
    config.validate().map_err(RuntimeError::from)?;
    Ok(Arc::new(config))
}

#[hammer_component_macros::init_function(name = "binary_api_init")]
fn init(config: Arc<Config>) -> RuntimeResult<Option<Arc<BinaryApiMain>>> {
    let Some(path) = config.socket_path.as_deref() else {
        return Ok(None);
    };
    BinaryApiMain::bind(path, config.max_frame_bytes)
        .map(Arc::new)
        .map(Some)
        .map_err(RuntimeError::from)
}
