//! Binary API Process and the independent socket I/O owner.
//! File callbacks accept, read complete frames, and flush output. The Process
//! receives complete messages, matching socket_api.c's process_args boundary.

use std::cell::UnsafeCell;
use std::ffi::CString;
use std::fs::{OpenOptions, remove_file};
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use hammer_infra::pool::Pool;
use hammer_infra::svm::queue::{SvmQueueError, SvmQueueOperation};
use hammer_infra::svm::region::SvmRegion;
use hammer_ipc::binary_api::memory_shared::MapError;
use hammer_ipc::binary_api::{ApiMain, control, memclnt};
use hammer_runtime::FILE_MAIN;
use hammer_runtime::binary_api::{BinaryApiMethodEntry, BinaryApiMethodStatus};
use hammer_runtime::{DataPlaneMain, NodeMain, PluginError, RuntimeError, RuntimeResult};
use prost::Message;

// Existing socket protocol compatibility; SHM messages never use this envelope.
pub use hammer_ipc::binary_api::{
    BinaryApiReply, BinaryApiRequest, BinaryApiStatus, DEFAULT_MAX_FRAME_BYTES,
};

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

const MAX_CLIENTS: u32 = 1024;
const MAX_ACCEPTS_PER_EVENT: usize = 16;
const READ_CHUNK_BYTES: usize = 4096;
const EVENT_SOCKET_MESSAGE: u64 = 1;
const EVENT_SOCKET_REMOVE: u64 = 2;

struct BinaryApiConnection {
    file_index: Option<u32>,
    read_buf: Vec<u8>,
    output: Vec<u8>,
    is_being_removed: bool,
}

/// Socket registrations and complete messages awaiting the API Process.
/// This owner is independent of ApiMain's memory registrations and SDK clients.
pub struct SocketMain {
    listener: u32,
    socket_path: PathBuf,
    socket_device: u64,
    socket_inode: u64,
    max_frame_bytes: usize,
    clients: UnsafeCell<Pool<BinaryApiConnection>>,
    process_messages: UnsafeCell<Pool<(u32, Vec<u8>)>>,
}

// SAFETY: mutable pools are private; every operation requires the runtime main
// thread. No pool borrow crosses dispatch, a File callback, or an await.
unsafe impl Sync for SocketMain {}

impl SocketMain {
    fn global() -> &'static Self {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("socket API callbacks execute on runtime main thread");
        SOCKET_MAIN
            .get()
            .expect("socket owner installed before File polling")
    }

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
        let listener = file_main
            .add_listener(
                listener,
                "binary-api listener",
                0,
                listener_file::file_functions::<NodeMain, RuntimeError>(),
            )
            .map_err(|source| BinaryApiServerError::ListenerRegistration { source })?;
        Ok(Self {
            listener,
            socket_path: path.to_path_buf(),
            socket_device: metadata.dev(),
            socket_inode: metadata.ino(),
            max_frame_bytes,
            clients: UnsafeCell::new(Pool::with_fixed_capacity(MAX_CLIENTS)),
            process_messages: UnsafeCell::new(Pool::new()),
        })
    }

    fn accept_ready(&self, fd: RawFd) -> RuntimeResult<()> {
        // Use the File borrowed by the callback, without looking it up again
        // through FileMain while that &mut File is live.
        let listener = unsafe { BorrowedFd::borrow_raw(fd) }
            .try_clone_to_owned()
            .map(StdUnixListener::from)
            .map_err(|source| RuntimeError::FileAccept { source })?;
        for _ in 0..MAX_ACCEPTS_PER_EVENT {
            if unsafe { &*self.clients.get() }.len() >= MAX_CLIENTS as usize {
                break;
            }
            let stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => break,
                Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                Err(source) => return Err(RuntimeError::FileAccept { source }),
            };
            stream
                .set_nonblocking(true)
                .map_err(|source| RuntimeError::FileAccept { source })?;
            #[cfg(target_os = "macos")]
            {
                use std::os::fd::AsRawFd;
                let enabled: libc::c_int = 1;
                if unsafe {
                    libc::setsockopt(
                        stream.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_NOSIGPIPE,
                        std::ptr::from_ref(&enabled).cast(),
                        size_of::<libc::c_int>() as libc::socklen_t,
                    )
                } != 0
                {
                    return Err(RuntimeError::FileAccept {
                        source: io::Error::last_os_error(),
                    });
                }
            }
            let index = unsafe { &mut *self.clients.get() }.insert(BinaryApiConnection {
                file_index: None,
                read_buf: Vec::new(),
                output: Vec::new(),
                is_being_removed: false,
            });
            let file = hammer_runtime::File::new(
                OwnedFd::from(stream),
                "binary-api client".to_owned(),
                u64::from(index),
                client_file::file_functions::<NodeMain, RuntimeError>(),
            );
            match FILE_MAIN
                .get()
                .expect("socket FileMain installed")
                .add(file)
            {
                Ok(file_index) => {
                    unsafe { &mut *self.clients.get() }
                        .get_mut(index)
                        .expect("new socket slot is present")
                        .file_index = Some(file_index)
                }
                Err(error) => {
                    unsafe { &mut *self.clients.get() }
                        .remove(index)
                        .expect("new socket slot is present");
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn request_remove(&self, graph: &mut NodeMain, index: u32) -> RuntimeResult<()> {
        let Some(connection) = (unsafe { &mut *self.clients.get() }).get_mut(index) else {
            return Ok(());
        };
        if connection.is_being_removed {
            return Ok(());
        }
        // FIFO event ordering keeps the slot alive until preceding messages
        // have been retired. This is a cleanup request, never I/O readiness.
        signal_process(graph, EVENT_SOCKET_REMOVE, index)?;
        connection.is_being_removed = true;
        Ok(())
    }

    fn read_ready(&self, graph: &mut NodeMain, index: u32, fd: RawFd) -> RuntimeResult<()> {
        if unsafe { &*self.clients.get() }
            .get(index)
            .is_none_or(|connection| connection.is_being_removed)
        {
            return Ok(());
        }
        let mut bytes = [0_u8; READ_CHUNK_BYTES];
        let length = loop {
            let read = unsafe { libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len()) };
            if read > 0 {
                break read as usize;
            }
            if read == 0 {
                return self.request_remove(graph, index);
            }
            let source = io::Error::last_os_error();
            match source.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => {
                    tracing::warn!(?source, index, "socket read failed");
                    return self.request_remove(graph, index);
                }
            }
        };
        unsafe { &mut *self.clients.get() }
            .get_mut(index)
            .expect("socket still present")
            .read_buf
            .extend_from_slice(&bytes[..length]);
        loop {
            let frame = {
                let connection = unsafe { &mut *self.clients.get() }
                    .get_mut(index)
                    .expect("socket still present");
                if connection.read_buf.len() < size_of::<u32>() {
                    break;
                }
                let declared = u32::from_be_bytes(
                    connection.read_buf[..4]
                        .try_into()
                        .expect("four-byte length"),
                ) as usize;
                if declared > self.max_frame_bytes {
                    return self.request_remove(graph, index);
                }
                let frame_len = size_of::<u32>() + declared;
                if connection.read_buf.len() < frame_len {
                    break;
                }
                let frame = connection.read_buf[4..frame_len].to_vec();
                connection.read_buf.drain(..frame_len);
                frame
            };
            let pending = unsafe { &mut *self.process_messages.get() }.insert((index, frame));
            if let Err(error) = signal_process(graph, EVENT_SOCKET_MESSAGE, pending) {
                unsafe { &mut *self.process_messages.get() }
                    .remove(pending)
                    .expect("unsignalled message remains owned");
                return Err(error);
            }
        }
        Ok(())
    }

    fn write_ready(&self, graph: &mut NodeMain, index: u32, fd: RawFd) -> RuntimeResult<bool> {
        let Some(connection) = (unsafe { &mut *self.clients.get() }).get_mut(index) else {
            return Ok(false);
        };
        if connection.is_being_removed {
            return Ok(false);
        }
        if connection.output.is_empty() {
            return Ok(false);
        }
        #[cfg(target_os = "linux")]
        let flags = libc::MSG_NOSIGNAL;
        #[cfg(not(target_os = "linux"))]
        let flags = 0;
        let written = unsafe {
            libc::send(
                fd,
                connection.output.as_ptr().cast(),
                connection.output.len().min(READ_CHUNK_BYTES),
                flags,
            )
        };
        if written > 0 {
            connection.output.drain(..written as usize);
            return Ok(!connection.output.is_empty());
        }
        let source = io::Error::last_os_error();
        if written < 0
            && matches!(
                source.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            )
        {
            return Ok(true);
        }
        tracing::warn!(?source, index, "socket write failed");
        self.request_remove(graph, index)?;
        Ok(false)
    }

    fn process_message(&self, pending: u32) -> RuntimeResult<()> {
        hammer_runtime::thread_main::ensure_main_thread()?;
        let Some((index, frame)) = (unsafe { &mut *self.process_messages.get() }).remove(pending)
        else {
            return Ok(());
        };
        if unsafe { &*self.clients.get() }
            .get(index)
            .is_none_or(|connection| connection.is_being_removed)
        {
            return Ok(());
        }
        // This endpoint still has its existing protobuf protocol. The memory
        // endpoint independently uses numeric ApiMsgData and safe fn(T).
        let response = match BinaryApiRequest::decode(frame.as_slice()) {
            Ok(request) => dispatch(request),
            Err(source) => {
                tracing::warn!(?source, index, "socket request rejected");
                reply(0, BinaryApiStatus::InvalidRequest, Vec::new())
            }
        }
        .encode_to_vec();
        if response.len() > self.max_frame_bytes {
            return self.close(index);
        }
        let file_index = {
            let connection = unsafe { &mut *self.clients.get() }
                .get_mut(index)
                .expect("socket remains present after dispatch");
            if connection.output.len() + 4 + response.len() > 2 * self.max_frame_bytes {
                // Never dispatch the same side-effecting request again merely
                // because its reply did not fit the output budget.
                return self.close(index);
            }
            connection
                .output
                .extend_from_slice(&(response.len() as u32).to_be_bytes());
            connection.output.extend_from_slice(&response);
            connection
                .file_index
                .expect("accepted socket has File index")
        };
        FILE_MAIN
            .get()
            .expect("socket FileMain installed")
            .set_data_available_to_write(file_index, true)?;
        Ok(())
    }

    fn close(&self, index: u32) -> RuntimeResult<()> {
        hammer_runtime::thread_main::ensure_main_thread()?;
        let file_index = unsafe { &*self.clients.get() }
            .get(index)
            .and_then(|connection| connection.file_index);
        if let Some(file_index) = file_index {
            FILE_MAIN
                .get()
                .expect("socket FileMain installed")
                .delete(file_index)?;
        }
        // Purge queued work before this numeric connection index can be reused.
        let pending: Vec<_> = unsafe { &*self.process_messages.get() }
            .iter()
            .filter_map(|(pending, (client, _))| (*client == index).then_some(pending))
            .collect();
        for pending in pending {
            // Keep the pending slot until its event is consumed, so an old
            // token can never address a new message that reused the slot.
            if let Some((client, bytes)) =
                unsafe { &mut *self.process_messages.get() }.get_mut(pending)
            {
                *client = u32::MAX;
                bytes.clear();
            }
        }
        unsafe { &mut *self.clients.get() }.remove(index);
        Ok(())
    }

    fn shutdown(&self) -> RuntimeResult<()> {
        hammer_runtime::thread_main::ensure_main_thread()?;
        let indices: Vec<_> = unsafe { &*self.clients.get() }
            .iter()
            .map(|(index, _)| index)
            .collect();
        let mut primary_error = None;
        for index in indices {
            if let Err(source) = self.close(index) {
                if primary_error.is_none() {
                    primary_error = Some(source);
                } else {
                    tracing::error!(?source, "additional socket close error");
                }
            }
        }
        if let Err(source) = FILE_MAIN
            .get()
            .expect("socket FileMain installed")
            .delete(self.listener)
        {
            if primary_error.is_none() {
                primary_error = Some(source);
            } else {
                tracing::error!(?source, "socket listener cleanup error");
            }
        }
        unsafe { *self.process_messages.get() = Pool::new() };
        match std::fs::metadata(&self.socket_path) {
            Ok(metadata)
                if metadata.dev() == self.socket_device && metadata.ino() == self.socket_inode =>
            {
                if let Err(source) = std::fs::remove_file(&self.socket_path) {
                    tracing::error!(?source, path=%self.socket_path.display(), "socket path cleanup failed");
                }
            }
            Ok(_) => (),
            Err(source) if source.kind() == io::ErrorKind::NotFound => (),
            Err(source) => {
                tracing::error!(?source, path=%self.socket_path.display(), "socket path inspection failed")
            }
        }
        match primary_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn signal_process(graph: &mut NodeMain, event: u64, index: u32) -> RuntimeResult<()> {
    let node = __PROCESS_NODE_BINARY_API
        .process_node_index()
        .ok_or(RuntimeError::ProcessNodeIdentityUnavailable { name: "binary-api" })?;
    graph.signal_process(node, event, u64::from(index))
}

#[hammer_component_macros::file]
mod listener_file {
    fn read<Context, Error>(
        _: &mut Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Context: std::borrow::BorrowMut<super::NodeMain>,
        Error: From<super::RuntimeError>,
    {
        super::SocketMain::global()
            .accept_ready(file.fd())
            .map_err(Into::into)
    }
    fn error<Context, Error>(
        _: &mut Context,
        _: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Context: std::borrow::BorrowMut<super::NodeMain>,
        Error: From<super::RuntimeError>,
    {
        Err(super::RuntimeError::FileAccept {
            source: std::io::Error::from(std::io::ErrorKind::ConnectionAborted),
        }
        .into())
    }
}

#[hammer_component_macros::file]
mod client_file {
    fn read<Context, Error>(
        graph: &mut Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Context: std::borrow::BorrowMut<super::NodeMain>,
        Error: From<super::RuntimeError>,
    {
        let index = u32::try_from(file.private_data()).expect("socket File has a connection index");
        super::SocketMain::global()
            .read_ready(graph.borrow_mut(), index, file.fd())
            .map_err(Into::into)
    }
    fn write<Context, Error>(
        graph: &mut Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Context: std::borrow::BorrowMut<super::NodeMain>,
        Error: From<super::RuntimeError>,
    {
        let index = u32::try_from(file.private_data()).expect("socket File has a connection index");
        let pending = super::SocketMain::global()
            .write_ready(graph.borrow_mut(), index, file.fd())
            .map_err(Error::from)?;
        file.set_write_enabled(pending);
        Ok(())
    }
    fn error<Context, Error>(
        graph: &mut Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Context: std::borrow::BorrowMut<super::NodeMain>,
        Error: From<super::RuntimeError>,
    {
        let index = u32::try_from(file.private_data()).expect("socket File has a connection index");
        super::SocketMain::global()
            .request_remove(graph.borrow_mut(), index)
            .map_err(Into::into)
    }
}

#[hammer_component_macros::main_loop_exit_function]
fn exit_binary_api() -> RuntimeResult<()> {
    let socket_result = match SOCKET_MAIN.get() {
        Some(socket) => socket.shutdown(),
        None => Ok(()),
    };
    let memory_result = if ApiMain::current().is_mapped() {
        unsafe { ApiMain::current().unmap_shared_regions() }
            .map_err(|source| RuntimeError::from(MapError::Region { source }))
    } else {
        Ok(())
    };
    if let Err(error) = socket_result {
        if let Err(source) = memory_result {
            tracing::error!(%source, "API memory cleanup also failed");
        }
        return Err(error);
    }
    memory_result
}

/// Resolves the method exactly once and routes it by `is_mp_safe`. Only a
/// successfully resolved mp-safe entry can bypass the barrier; resolution
/// failures keep the legacy reply and barriered dispatch path.
fn dispatch(request: BinaryApiRequest) -> BinaryApiReply {
    let context = request.context;
    let resolved: Result<BinaryApiMethodEntry, BinaryApiReply> =
        match hammer_runtime::PluginMain::global()
            .and_then(|plugins| plugins.binary_api_method(&request.method))
        {
            Err(PluginError::BinaryApiMethodMissing { .. }) => {
                Err(reply(context, BinaryApiStatus::MethodMissing, Vec::new()))
            }
            Err(PluginError::BinaryApiMethodDuplicate { .. }) => {
                Err(reply(context, BinaryApiStatus::MethodDuplicate, Vec::new()))
            }
            Err(_) => Err(reply(context, BinaryApiStatus::Internal, Vec::new())),
            Ok(method) => Ok(method),
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
    match hammer_runtime::barrier::global() {
        Some(barrier) if barrier.is_pending() => invoke_method(request, entry),
        Some(barrier) => barrier.sync(|| invoke_method(request, entry)),
        None => invoke_method(request, entry),
    }
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
fn configure(config: Config) -> RuntimeResult<()> {
    config.validate().map_err(RuntimeError::from)?;
    assert!(
        BINARY_API_CONFIG.set(config).is_ok(),
        "Binary API configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::config_function(
    name = "api_segment_config",
    section = "api-segment",
    early = true
)]
fn configure_api_segment(config: ApiSegmentConfig) -> RuntimeResult<()> {
    config.validate()?;
    // All clients must agree on bootstrap IDs, including unsupported messages.
    // Reserve memclnt.api's 1..=28 before any plugin API-init allocates a range.
    let mut api = ApiMain::new(hammer_ipc::binary_api::MEMCLNT_LAST + 1);
    api.set_api_region_name(config.region_name.clone());
    api.set_api_uid(config.uid);
    api.set_api_gid(config.gid);
    api.set_global_base_va(config.global_base_va as u64);
    api.set_global_size(config.global_size as u64);
    api.set_global_pvt_heap_size(config.global_private_heap_size as u64);
    api.set_api_pvt_heap_size(config.api_private_heap_size as u64);
    api.set_api_size(config.api_size as u64);
    api.install();
    assert!(
        API_SEGMENT_CONFIG.set(config).is_ok(),
        "API segment configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "binary_api_init",
    runs_after = ["vpe_api_init"]
)]
fn init() -> RuntimeResult<()> {
    let segment = API_SEGMENT_CONFIG
        .get()
        .expect("API segment configuration is installed before initialization");
    let region_name = segment.region_name.trim_start_matches('/');
    let (root_path, api_path) = if segment.root_path.as_os_str().is_empty() {
        (
            PathBuf::from("/dev/shm/global_vm"),
            PathBuf::from("/dev/shm").join(region_name),
        )
    } else {
        let prefix = if segment.root_path.is_absolute() {
            segment.root_path.clone()
        } else {
            PathBuf::from("/dev/shm").join(&segment.root_path)
        };
        let mut root_name = prefix.as_os_str().to_os_string();
        root_name.push("-global_vm");
        let mut api_name = prefix.as_os_str().to_os_string();
        api_name.push("-");
        api_name.push(region_name);
        (PathBuf::from(root_name), PathBuf::from(api_name))
    };
    for path in [&root_path, &api_path] {
        match remove_file(path) {
            Ok(()) => (),
            Err(source) if source.kind() == io::ErrorKind::NotFound => (),
            Err(source) => {
                return Err(MapError::Open {
                    path: path.clone(),
                    source,
                }
                .into());
            }
        }
    }
    let root_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&root_path)
        .map_err(|source| MapError::Open {
            path: root_path.clone(),
            source,
        })?;
    if unsafe {
        libc::fchown(
            root_file.as_raw_fd(),
            segment.uid as libc::uid_t,
            segment.gid as libc::gid_t,
        )
    } != 0
    {
        let source = io::Error::last_os_error();
        tracing::warn!(path = %root_path.display(), ?source, "API root backing ownership unchanged");
    }
    let api = ApiMain::current();
    let root = SvmRegion::create(
        api.global_base_va(),
        &api.root_region_config(),
        root_file.into(),
    )
    .map_err(|source| MapError::Region { source })?;
    unsafe { api.map_shared_region(root, &api_path, true) }?;

    let config = BINARY_API_CONFIG
        .get()
        .expect("Binary API configuration is installed before initialization");
    let Some(path) = config.socket_path.as_deref() else {
        return Ok(());
    };
    let main = SocketMain::bind(path, config.max_frame_bytes)?;
    assert!(
        SOCKET_MAIN.set(main).is_ok(),
        "binary API initialization callback executes once"
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    socket_path: Option<String>,
    max_frame_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct ApiSegmentConfig {
    pub region_name: String,
    #[serde(rename = "prefix")]
    pub root_path: PathBuf,
    #[serde(deserialize_with = "deserialize_uid")]
    pub uid: u32,
    #[serde(deserialize_with = "deserialize_gid")]
    pub gid: u32,
    #[serde(rename = "baseva")]
    pub global_base_va: usize,
    #[serde(deserialize_with = "deserialize_size")]
    pub global_size: usize,
    #[serde(rename = "global-pvt-heap-size", deserialize_with = "deserialize_size")]
    pub global_private_heap_size: usize,
    #[serde(rename = "api-pvt-heap-size", deserialize_with = "deserialize_size")]
    pub api_private_heap_size: usize,
    #[serde(deserialize_with = "deserialize_size")]
    pub api_size: usize,
}

impl Default for ApiSegmentConfig {
    fn default() -> Self {
        Self {
            region_name: "/vpe-api".to_owned(),
            root_path: PathBuf::new(),
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            global_base_va: 0x1_3000_0000,
            global_size: 64 << 20,
            global_private_heap_size: 128 << 10,
            api_private_heap_size: 128 << 10,
            api_size: 16 << 20,
        }
    }
}

static BINARY_API_CONFIG: OnceLock<Config> = OnceLock::new();
static API_SEGMENT_CONFIG: OnceLock<ApiSegmentConfig> = OnceLock::new();
static SOCKET_MAIN: OnceLock<SocketMain> = OnceLock::new();

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_path: None,
            max_frame_bytes: hammer_ipc::binary_api::DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

fn deserialize_uid<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Uid;
    impl<'de> serde::de::Visitor<'de> for Uid {
        type Value = u32;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a numeric uid or account name")
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<u32, E> {
            u32::try_from(value).map_err(E::custom)
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<u32, E> {
            u32::try_from(value).map_err(E::custom)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u32, E> {
            let name = CString::new(value).map_err(E::custom)?;
            let entry = unsafe { libc::getpwnam(name.as_ptr()) };
            if entry.is_null() {
                return Err(E::custom(format_args!("account `{value}` does not exist")));
            }
            Ok(unsafe { (*entry).pw_uid })
        }
    }
    deserializer.deserialize_any(Uid)
}

fn deserialize_gid<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Gid;
    impl<'de> serde::de::Visitor<'de> for Gid {
        type Value = u32;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a numeric gid or group name")
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<u32, E> {
            u32::try_from(value).map_err(E::custom)
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<u32, E> {
            u32::try_from(value).map_err(E::custom)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u32, E> {
            let name = CString::new(value).map_err(E::custom)?;
            let entry = unsafe { libc::getgrnam(name.as_ptr()) };
            if entry.is_null() {
                return Err(E::custom(format_args!("group `{value}` does not exist")));
            }
            Ok(unsafe { (*entry).gr_gid })
        }
    }
    deserializer.deserialize_any(Gid)
}

fn deserialize_size<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Size;
    impl<'de> serde::de::Visitor<'de> for Size {
        type Value = usize;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a byte count or a number followed by M or G")
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<usize, E> {
            usize::try_from(value).map_err(E::custom)
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<usize, E> {
            usize::try_from(value).map_err(E::custom)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<usize, E> {
            let value = value.trim();
            let (number, multiplier) = match value.as_bytes().last() {
                Some(b'M') => (&value[..value.len() - 1], 1_u64 << 20),
                Some(b'G') => (&value[..value.len() - 1], 1_u64 << 30),
                _ => (value, 1),
            };
            let bytes = number.trim().parse::<u64>().map_err(E::custom)?;
            let bytes = bytes
                .checked_mul(multiplier)
                .ok_or_else(|| E::custom("API segment size overflows u64"))?;
            usize::try_from(bytes).map_err(E::custom)
        }
    }
    deserializer.deserialize_any(Size)
}

impl ApiSegmentConfig {
    fn validate(&self) -> RuntimeResult<()> {
        if self.region_name.is_empty() {
            return Err(RuntimeError::config_validation(
                "api-segment.region-name must not be empty",
            ));
        }
        if self.global_base_va == 0 {
            return Err(RuntimeError::config_validation(
                "api-segment.baseva must be non-zero",
            ));
        }
        for (name, bytes) in [
            ("global-size", self.global_size),
            ("global-pvt-heap-size", self.global_private_heap_size),
            ("api-pvt-heap-size", self.api_private_heap_size),
            ("api-size", self.api_size),
        ] {
            if bytes == 0 {
                return Err(RuntimeError::config_validation(format!(
                    "api-segment.{name} must be non-zero"
                )));
            }
        }
        Ok(())
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
fn binary_api_clnt(
    main: &mut DataPlaneMain,
) -> impl std::future::Future<Output = RuntimeResult<()>> + Send + 'static {
    // VPP `vl_api_clnt_node`: FileMain callbacks signal this node; the main
    // FileMain poll loop owns readiness and this node consumes its event batch.
    let api = ApiMain::current();
    let mapped = api.is_mapped();
    let events = (|| {
        // Control messages are bootstrap declarations, independent of whether
        // this process has a SHM region. Install them before the API-init list.
        control::setup_message_id_table(api);
        hammer_runtime::init::run_api_init(main)?;
        main.process_events()
    })();
    async move {
        let mut events = events?;
        let mut memory_enabled = mapped;
        let socket = SOCKET_MAIN.get();
        let scan_interval = Duration::from_secs(10);
        let mut next_scan = Instant::now() + scan_interval;
        while memory_enabled || socket.is_some() {
            if memory_enabled {
                let drain_started = Instant::now();
                loop {
                    match memclnt::receive() {
                        Ok(false) => break,
                        Ok(true) => (),
                        Err(
                            source @ SvmQueueError::SignalAfterCommit {
                                operation: SvmQueueOperation::Sub,
                                ..
                            },
                        )
                        | Err(
                            source @ SvmQueueError::EventSignalAfterCommit {
                                operation: SvmQueueOperation::Sub,
                                ..
                            },
                        ) => tracing::warn!(?source, "memory API dequeue notification failed"),
                        Err(source) => {
                            tracing::error!(?source, "memory API input disabled after queue error");
                            memory_enabled = false;
                            break;
                        }
                    }
                    if drain_started.elapsed() >= Duration::from_micros(10) {
                        break;
                    }
                }
                let now = Instant::now();
                if memory_enabled && now >= next_scan {
                    hammer_runtime::worker_thread_barrier_sync!({
                        api.dead_client_scan(now);
                    });
                    next_scan = Instant::now() + scan_interval;
                }
            }
            if !memory_enabled && socket.is_none() {
                break;
            }
            let wake_at = (Instant::now() + Duration::from_micros(400)).min(next_scan);
            tokio::task::yield_now().await;
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Some((event_type, token)) => {
                            if let Some(socket) = socket {
                                let index = u32::try_from(token)
                                    .expect("socket Process event carries a pool index");
                                match event_type {
                                    EVENT_SOCKET_MESSAGE => socket.process_message(index)?,
                                    EVENT_SOCKET_REMOVE => socket.close(index)?,
                                    _ => tracing::warn!(event_type, "unknown Binary API event"),
                                }
                            }
                        }
                        None => break,
                    }
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)),
                    if memory_enabled => (),
            }
        }
        Ok(())
    }
}
