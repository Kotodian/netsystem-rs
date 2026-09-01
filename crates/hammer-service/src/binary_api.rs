//! Protobuf Binary API served by the `binary-api` Process Node.
//!
//! Mirrors VPP's `vl_api_clnt_node` (`third_party/vpp/src/vlibmemory/socket_api.c`):
//! FileMain readiness callbacks only signal this node; the node consumes the
//! event batches to accept connections, read length-prefixed frames, dispatch
//! them under the worker barrier, and flush replies with TCP backpressure.
//! Per-event budgets keep one chatty client from monopolizing the main thread.

use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use hammer_infra::pool::Pool;
use hammer_runtime::FILE_MAIN;
use hammer_runtime::binary_api::{BinaryApiMethodEntry, BinaryApiMethodStatus};
use hammer_runtime::file::{FileIoStatus, FileMain};
use hammer_runtime::{
    GlobalMain, NodeRuntime, PluginError, ProcessContext, ProcessWake, RuntimeError, RuntimeResult,
};
use prost::Message;

/// Shared envelope, blocking client, and client-facing errors owned by
/// `hammer-ipc` and re-exported here so existing callers can reach them
/// through the server crate.
pub use hammer_ipc::binary_api::{
    BinaryApiClient, BinaryApiError, BinaryApiReply, BinaryApiRequest, BinaryApiStatus,
    DEFAULT_MAX_FRAME_BYTES,
};

/// Server-side Binary API errors. Client-facing errors are `BinaryApiError`
/// from `hammer-ipc`.
#[hammer_component_macros::runtime_error(subsystem = "binary api")]
#[derive(Debug, thiserror::Error)]
pub enum BinaryApiServerError {
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
    #[error("create Binary API FileMain")]
    FileMainCreate {
        #[source]
        source: RuntimeError,
    },
    #[error("register Binary API Unix listener with FileMain")]
    ListenerRegistration {
        #[source]
        source: RuntimeError,
    },
    #[error("Binary API FileMain is not ready for the Process Node")]
    FileMainNotReady,
}

// VPP `VL_API_CLNT_NODE` budgets: bounded work per readiness event so one
// chatty client or a burst of connects cannot monopolize the main thread.
const PROCESS_NODE_NAME: &str = "binary-api";
const MAX_CLIENTS: u32 = 1024;
const MAX_ACCEPTS_PER_EVENT: usize = 16;
const MAX_FRAMES_PER_READ_EVENT: usize = 16;
const READ_CHUNK_BYTES: usize = 4096;
const LISTENER_TOKEN: u64 = 0;
const EVENT_ACCEPT_READY: u64 = 1;
const EVENT_CLIENT_READ_READY: u64 = 2;
const EVENT_CLIENT_WRITE_READY: u64 = 3;

/// One accepted Binary API connection stored directly in the existing index
/// pool. File private data is the pool index; FileMain deletion precedes pool
/// removal so a pending readiness event cannot target a recycled value.
struct BinaryApiConnection {
    file_index: Option<u32>,
    read_buf: Vec<u8>,
    output: Vec<u8>,
}

impl BinaryApiConnection {
    fn new() -> Self {
        Self {
            file_index: None,
            read_buf: Vec::new(),
            output: Vec::new(),
        }
    }

    fn bind_file(&mut self, index: u32) {
        self.file_index = Some(index);
    }

    fn clear(&mut self) {
        self.file_index = None;
        self.read_buf.clear();
        self.output.clear();
    }
}

/// Main-thread connection pool for Binary API Process Node state.
struct BinaryApiConnections {
    listener: u32,
    clients: Pool<BinaryApiConnection>,
}

impl BinaryApiConnections {
    fn new(listener: u32) -> Self {
        Self {
            listener,
            clients: Pool::with_fixed_capacity(MAX_CLIENTS),
        }
    }

    fn reserve(&mut self) -> Option<u32> {
        (self.clients.len() < MAX_CLIENTS as usize)
            .then(|| self.clients.insert(BinaryApiConnection::new()))
    }

    fn release(&mut self, index: u32) {
        if let Some(mut connection) = self.clients.remove(index) {
            connection.clear();
        }
    }
}

impl Default for BinaryApiConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl BinaryApiConnections {
    /// Consumes one event batch signalled by FileMain readiness callbacks.
    fn process_event(
        &mut self,
        file_main: &FileMain,
        event_type: u64,
        data: &[u64],
        max_frame_bytes: usize,
    ) -> RuntimeResult<()> {
        match event_type {
            EVENT_ACCEPT_READY => {
                for _ in 0..MAX_ACCEPTS_PER_EVENT {
                    if !self.accept_ready(file_main)? {
                        break;
                    }
                }
            }
            EVENT_CLIENT_READ_READY => {
                for raw in data {
                    if let Ok(index) = u32::try_from(*raw) {
                        self.client_read(file_main, index, max_frame_bytes);
                    }
                }
            }
            EVENT_CLIENT_WRITE_READY => {
                for raw in data {
                    if let Ok(index) = u32::try_from(*raw) {
                        self.client_flush(file_main, index, max_frame_bytes);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// VPP `vl_api_socket_accept`: pulls one pending connection per call; the
    /// per-event loop bounds the count.
    fn accept_ready(&mut self, file_main: &FileMain) -> RuntimeResult<bool> {
        let Some(connection_index) = self.reserve() else {
            return Ok(false); // table full: the kernel backlog holds connects
        };
        match file_main.accept(
            self.listener,
            "binary-api client",
            connection_index as u64,
            client_file::file_functions::<NodeRuntime, RuntimeError>(),
        ) {
            Ok(Some(index)) => {
                match self.clients.get_mut(connection_index) {
                    Some(connection) => {
                        connection.bind_file(index);
                    }
                    None => {
                        // Unreachable: the token was just reserved. Drop the
                        // registered File rather than leak it.
                        let _ = file_main.delete(index);
                        self.release(connection_index);
                    }
                }
                Ok(true)
            }
            Ok(None) => {
                self.release(connection_index);
                Ok(false)
            }
            Err(error) => {
                // A per-connection accept failure must not kill the node: VPP
                // logs and keeps polling. The dropped socket closes with the
                // error path; the next listener readiness pulls a new one.
                tracing::warn!(%error, "Binary API accept failed; dropping connection");
                self.release(connection_index);
                Ok(false)
            }
        }
    }

    /// VPP `vl_api_socket_read`: one bounded chunk per readiness event into
    /// `unprocessed_input`, then every complete frame in it.
    fn client_read(&mut self, file_main: &FileMain, connection_index: u32, max_frame_bytes: usize) {
        let Some(connection) = self.clients.get(connection_index) else {
            return; // stale event for a closed or recycled slot
        };
        let Some(index) = connection.file_index else {
            return;
        };
        let mut chunk = [0_u8; READ_CHUNK_BYTES];
        match file_main.read_some(index, &mut chunk) {
            Ok(FileIoStatus::Progress(n)) => {
                if let Some(connection) = self.clients.get_mut(connection_index) {
                    connection.read_buf.extend_from_slice(&chunk[..n]);
                }
                self.parse_frames(file_main, connection_index, max_frame_bytes);
            }
            Ok(FileIoStatus::WouldBlock) => {}
            Ok(FileIoStatus::Closed) => self.close(file_main, connection_index),
            Err(error) => {
                tracing::warn!(%error, "Binary API read failed; closing client");
                self.close(file_main, connection_index);
            }
        }
    }

    /// VPP `vl_api_socket_write`: flush the reply buffer; EAGAIN (WouldBlock)
    /// keeps write interest armed so the next write readiness drains it, and
    /// a complete drain clears it.
    fn client_flush(
        &mut self,
        file_main: &FileMain,
        connection_index: u32,
        max_frame_bytes: usize,
    ) {
        loop {
            // Resume frames stalled by a saturated output buffer first.
            self.parse_frames(file_main, connection_index, max_frame_bytes);
            let Some(connection) = self.clients.get(connection_index) else {
                return;
            };
            if connection.output.is_empty() {
                if let Some(index) = connection.file_index {
                    let _ = file_main.set_data_available_to_write(index, false);
                }
                return;
            }
            let Some(index) = connection.file_index else {
                return;
            };
            match file_main.write_some(index, &connection.output) {
                Ok(FileIoStatus::Progress(n)) => {
                    if let Some(connection) = self.clients.get_mut(connection_index) {
                        connection.output.drain(..n);
                    }
                }
                Ok(FileIoStatus::WouldBlock) => return,
                Ok(FileIoStatus::Closed) => {
                    self.close(file_main, connection_index);
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, "Binary API write failed; closing client");
                    self.close(file_main, connection_index);
                    return;
                }
            }
        }
    }

    /// Parses every complete length-prefixed frame in `read_buf`, dispatching
    /// each under the worker barrier and enqueueing the reply. Stops when the
    /// frame budget for this event is spent, the output buffer saturates, or
    /// the buffer holds only a partial frame.
    fn parse_frames(
        &mut self,
        file_main: &FileMain,
        connection_index: u32,
        max_frame_bytes: usize,
    ) {
        for _ in 0..MAX_FRAMES_PER_READ_EVENT {
            let Some(connection) = self.clients.get(connection_index) else {
                return;
            };
            if connection.read_buf.len() < size_of::<u32>() {
                return;
            }
            let declared = u32::from_be_bytes(
                connection.read_buf[..size_of::<u32>()]
                    .try_into()
                    .expect("four-byte length prefix"),
            ) as usize;
            if declared > max_frame_bytes {
                tracing::warn!(declared, "Binary API frame exceeds maximum; closing client");
                self.close(file_main, connection_index);
                return;
            }
            let frame_len = size_of::<u32>() + declared;
            if connection.read_buf.len() < frame_len {
                return; // partial frame: VPP keeps it in unprocessed_input
            }
            let request =
                BinaryApiRequest::decode(&connection.read_buf[size_of::<u32>()..frame_len]);
            let reply = match request {
                Ok(request) => dispatch(request),
                Err(_) => reply(0, BinaryApiStatus::InvalidRequest, Vec::new()),
            };
            let encoded = reply.encode_to_vec(); // single allocation for the reply frame
            if encoded.len() > max_frame_bytes {
                tracing::warn!(
                    bytes = encoded.len(),
                    "Binary API reply exceeds maximum; closing client"
                );
                self.close(file_main, connection_index);
                return;
            }
            let Some(connection) = self.clients.get_mut(connection_index) else {
                return;
            };
            // Saturate rather than grow: parsing stalls until a flush drains
            // below the budget, and TCP backpressure limits the client.
            if connection.output.len() + size_of::<u32>() + encoded.len()
                > output_budget(max_frame_bytes)
            {
                return;
            }
            let arm_write = connection.output.is_empty();
            connection
                .output
                .extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            connection.output.extend_from_slice(&encoded);
            connection.read_buf.drain(..frame_len);
            if arm_write {
                if let Some(index) = connection.file_index {
                    let _ = file_main.set_data_available_to_write(index, true);
                }
            }
        }
    }

    /// Closes one client: removes backend interest and the File record, then
    /// frees the slot for reuse.
    fn close(&mut self, file_main: &FileMain, connection_index: u32) {
        if let Some(index) = self
            .clients
            .get(connection_index)
            .and_then(|connection| connection.file_index)
        {
            let _ = file_main.delete(index);
        }
        self.release(connection_index);
    }
}

impl Drop for BinaryApiConnections {
    fn drop(&mut self) {
        let Some(file_main) = FILE_MAIN.get() else {
            return;
        };
        for connection_index in self
            .clients
            .iter()
            .map(|(index, _)| index)
            .collect::<Vec<_>>()
        {
            if let Some(connection) = self.clients.get(connection_index) {
                if let Some(index) = connection.file_index {
                    let _ = file_main.delete(index);
                }
            }
            if let Some(mut connection) = self.clients.remove(connection_index) {
                connection.clear();
            }
        }
    }
}

/// Main-thread owner of the Binary API Unix listener and frame policy. The
/// listener is registered in the process-global `FILE_MAIN`; `GlobalMain` owns
/// the control loop and the `binary-api` Process Node consumes its events.
pub struct BinaryApiMain {
    listener: u32,
    socket_path: PathBuf,
    socket_device: u64,
    socket_inode: u64,
    max_frame_bytes: usize,
}

impl BinaryApiMain {
    pub fn bind(
        path: impl AsRef<Path>,
        max_frame_bytes: usize,
    ) -> Result<Self, BinaryApiServerError> {
        if max_frame_bytes == 0 || max_frame_bytes > u32::MAX as usize {
            return Err(BinaryApiServerError::FrameSizeInvalid {
                bytes: max_frame_bytes,
            });
        }
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Err(BinaryApiServerError::SocketPathEmpty);
        }
        let listener = bind_listener(path).map_err(|source| BinaryApiServerError::SocketBind {
            path: path.to_path_buf(),
            source,
        })?;
        let metadata =
            std::fs::metadata(path).map_err(|source| BinaryApiServerError::SocketBind {
                path: path.to_path_buf(),
                source,
            })?;
        let file_main = FILE_MAIN
            .get()
            .ok_or(BinaryApiServerError::FileMainNotReady)?;
        let listener_index = file_main
            .add_listener(
                listener,
                "binary-api listener",
                LISTENER_TOKEN,
                listener_file::file_functions::<NodeRuntime, RuntimeError>(),
            )
            .map_err(|source| BinaryApiServerError::ListenerRegistration { source })?;
        Ok(Self {
            listener: listener_index,
            socket_path: path.to_path_buf(),
            socket_device: metadata.dev(),
            socket_inode: metadata.ino(),
            max_frame_bytes,
        })
    }
}

impl Drop for BinaryApiMain {
    fn drop(&mut self) {
        if let Some(file_main) = FILE_MAIN.get() {
            let _ = file_main.delete(self.listener);
        }
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

/// VPP's `vl_api_clnt_node` signal path: the callback never touches the
/// socket registration pool; it only hands the File's token to the node.
/// Without a live node (main process shutdown) the readiness is dropped.
fn signal_ready(event_type: u64, token: u64) -> RuntimeResult<()> {
    match GlobalMain::with_current(|engine| engine.process_handle(PROCESS_NODE_NAME)) {
        Some(Some(handle)) => handle.signal(event_type, token),
        _ => Ok(()),
    }
}

#[hammer_component_macros::file]
mod listener_file {
    fn read<Context, Error>(
        _graph: &Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<super::RuntimeError>,
    {
        super::signal_ready(super::EVENT_ACCEPT_READY, file.private_data()).map_err(Into::into)
    }

    fn error<Context, Error>(
        _graph: &Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<super::RuntimeError>,
    {
        super::signal_ready(super::EVENT_ACCEPT_READY, file.private_data()).map_err(Into::into)
    }
}

#[hammer_component_macros::file]
mod client_file {
    fn read<Context, Error>(
        _graph: &Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<super::RuntimeError>,
    {
        super::signal_ready(super::EVENT_CLIENT_READ_READY, file.private_data()).map_err(Into::into)
    }

    fn write<Context, Error>(
        _graph: &Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<super::RuntimeError>,
    {
        super::signal_ready(super::EVENT_CLIENT_WRITE_READY, file.private_data())
            .map_err(Into::into)
    }

    fn error<Context, Error>(
        _graph: &Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<super::RuntimeError>,
    {
        super::signal_ready(super::EVENT_CLIENT_READ_READY, file.private_data()).map_err(Into::into)
    }
}

#[inline]
fn output_budget(max_frame_bytes: usize) -> usize {
    2 * max_frame_bytes
}

/// Resolves the method exactly once and routes it by `is_mp_safe`. Only a
/// successfully resolved mp-safe entry can bypass the barrier; resolution
/// failures keep the legacy reply and barriered dispatch path.
fn dispatch(request: BinaryApiRequest) -> BinaryApiReply {
    let context = request.context;
    let resolved: Result<BinaryApiMethodEntry, BinaryApiReply> =
        match GlobalMain::with_current(|engine| {
            engine.plugin_main().binary_api_method(&request.method)
        }) {
            None => Err(reply(
                context,
                BinaryApiStatus::MainThreadUnavailable,
                Vec::new(),
            )),
            Some(Err(PluginError::BinaryApiMethodMissing { .. })) => {
                Err(reply(context, BinaryApiStatus::MethodMissing, Vec::new()))
            }
            Some(Err(PluginError::BinaryApiMethodDuplicate { .. })) => {
                Err(reply(context, BinaryApiStatus::MethodDuplicate, Vec::new()))
            }
            Some(Err(_)) => Err(reply(context, BinaryApiStatus::Internal, Vec::new())),
            Some(Ok(method)) => Ok(method),
        };
    match resolved {
        // VPP's `msg_handler_internal` takes the worker barrier only when
        // `!m->is_mp_safe` (api_shared.c:545, 564): an mp-safe method runs
        // directly on the serial Main Thread and never fetches the barrier
        // and does not acquire the worker barrier.
        Ok(method) if method.is_mp_safe() => invoke_method(request, method),
        _ => dispatch_barriered(request, resolved),
    }
}

/// Calls an already-resolved handler exactly once and maps its status to the
/// reply. Resolution happens once in `dispatch` before the mp-safe branch, so
/// this helper never resolves and never touches the worker barrier.
fn invoke_method(request: BinaryApiRequest, entry: BinaryApiMethodEntry) -> BinaryApiReply {
    let method_reply = entry.call(&request.payload);
    let status = match method_reply.status() {
        BinaryApiMethodStatus::Ok => BinaryApiStatus::Ok,
        BinaryApiMethodStatus::InvalidRequest => BinaryApiStatus::InvalidRequest,
        BinaryApiMethodStatus::Panicked => BinaryApiStatus::MethodPanicked,
    };
    reply(request.context, status, method_reply.payload().to_vec())
}

/// Dispatches one request under the worker barrier exactly once per request:
/// a pending barrier dispatches unlocked (VPP `msg_handler_internal` skips
/// the barrier while one is already pending), otherwise the handler runs
/// inside `barrier.sync` with no await while held. Resolution failures arrive
/// as the `Err` reply and run the same branches as successful handlers.
fn dispatch_barriered(
    request: BinaryApiRequest,
    resolved: Result<BinaryApiMethodEntry, BinaryApiReply>,
) -> BinaryApiReply {
    let entry = match resolved {
        Ok(entry) => entry,
        Err(reply) => return reply,
    };
    let mut request = Some(request);
    let Some(reply) = GlobalMain::with_current(|engine| {
        let main = engine.data_plane_main_mut();
        let request = request.take().expect("binary request is invoked once");
        if hammer_runtime::barrier::__is_pending() {
            invoke_method(request, entry)
        } else {
            hammer_runtime::worker_thread_barrier_sync!(main, { invoke_method(request, entry) })
        }
    }) else {
        return invoke_method(
            request.take().expect("binary request is invoked once"),
            entry,
        );
    };
    reply
}

fn reply(context: u64, status: BinaryApiStatus, payload: Vec<u8>) -> BinaryApiReply {
    BinaryApiReply {
        context,
        status: status as i32,
        payload,
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
            max_frame_bytes: hammer_ipc::binary_api::DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

impl Config {
    fn validate(&self) -> Result<(), BinaryApiServerError> {
        if self
            .socket_path
            .as_ref()
            .is_some_and(|path| path.trim().is_empty())
        {
            return Err(BinaryApiServerError::SocketPathEmpty);
        }
        if self.max_frame_bytes == 0 || self.max_frame_bytes > u32::MAX as usize {
            return Err(BinaryApiServerError::FrameSizeInvalid {
                bytes: self.max_frame_bytes,
            });
        }
        Ok(())
    }
}

#[hammer_component_macros::process_node(name = "binary-api")]
async fn binary_api_clnt(mut context: ProcessContext) -> RuntimeResult<()> {
    // VPP `vl_api_clnt_node`: FileMain callbacks signal this node; the main
    // FileMain poll loop owns readiness and this node consumes its event batch.
    let capability = context.require::<BinaryApiMain>()?;
    let file_main = FILE_MAIN
        .get()
        .expect("FileMain is initialized before Binary API startup");
    let mut table = BinaryApiConnections::new(capability.listener);
    let max_frame_bytes = capability.max_frame_bytes;
    loop {
        match context.wait_for_event().await {
            ProcessWake::Clock => return Ok(()),
            ProcessWake::Event(batch) => {
                table.process_event(
                    file_main,
                    batch.event_type(),
                    batch.data(),
                    max_frame_bytes,
                )?;
            }
        }
    }
}
