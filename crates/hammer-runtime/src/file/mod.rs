//! Worker-local file readiness, corresponding to VPP's `clib_file_t` and
//! `clib_file_main_t`.
//!
//! This module owns descriptors, readiness dispatch, and indexed synchronous
//! descriptor I/O. Device queues own packet and queue semantics.

use std::cell::UnsafeCell;
use std::fmt;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::OnceLock;
use std::time::Duration;

use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::runtime::Handle;

use hammer_infra::pool::Pool;
use hammer_infra::sync::{SpinLock, SpinLockGuard};

use crate::NodeRuntime;
use crate::error::{RuntimeError, RuntimeResult};
use crate::global_main::GlobalMain;
use hammer_component_macros::init_function;
use hammer_core::file::{
    File as CoreFile, FileFunction as CoreFileFunction, FileFunctions as CoreFileFunctions,
};

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
use macos::Poller;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux::Poller;

/// The runtime's concrete specialization of the shared File ABI.
pub type File = CoreFile<NodeRuntime, RuntimeError>;
pub type FileFunction = CoreFileFunction<NodeRuntime, RuntimeError>;
pub type FileFunctions = CoreFileFunctions<NodeRuntime, RuntimeError>;

fn duplicate_file_descriptor(file: &File) -> io::Result<OwnedFd> {
    // SAFETY: `F_DUPFD_CLOEXEC` returns a fresh descriptor referencing the
    // same socket; the registered descriptor stays FileMain-owned.
    let duplicated = unsafe { libc::fcntl(file.fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `duplicated` is a valid owned descriptor from fcntl.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

/// Outcome of one safe nonblocking socket operation on a FileMain-owned
/// descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileIoStatus {
    /// The operation consumed or produced the reported byte count.
    Progress(usize),
    /// The descriptor would block; retry when readiness fires again.
    WouldBlock,
    /// The peer closed the connection (or it is otherwise unusable).
    Closed,
}

/// Worker-local callback invoked when a registered deadline expires.
pub type DeadlineFunction = fn(&NodeRuntime, &mut Deadline) -> RuntimeResult<()>;

/// A worker-local deadline registration owned by [`FileMain`].
pub struct Deadline {
    description: String,
    private_data: u64,
    function: DeadlineFunction,
    polling_thread_index: u32,
    duration: Option<Duration>,
    expiry_events: u64,
}

impl Deadline {
    /// Creates a disarmed deadline registration.
    pub fn new(
        description: impl Into<String>,
        private_data: u64,
        function: DeadlineFunction,
    ) -> Self {
        Self {
            description: description.into(),
            private_data,
            function,
            polling_thread_index: 0,
            duration: None,
            expiry_events: 0,
        }
    }

    #[inline]
    pub fn set_polling_thread_index(&mut self, thread_index: u32) {
        self.polling_thread_index = thread_index;
    }

    #[inline]
    pub fn polling_thread_index(&self) -> u32 {
        self.polling_thread_index
    }

    #[inline]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[inline]
    pub fn private_data(&self) -> u64 {
        self.private_data
    }

    #[inline]
    pub fn set_private_data(&mut self, private_data: u64) {
        self.private_data = private_data;
    }

    #[inline]
    pub fn expiry_events(&self) -> u64 {
        self.expiry_events
    }
}

impl fmt::Debug for Deadline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Deadline")
            .field("description", &self.description)
            .field("private_data", &self.private_data)
            .field("duration", &self.duration)
            .field("expiry_events", &self.expiry_events)
            .finish()
    }
}

/// Generation-safe global File registry and readiness dispatcher.
///
/// The registry follows VPP's single `clib_file_main_t` lock and one
/// `pending_free` owner queue. Pollers remain owned by their polling thread.
pub struct FileMain {
    pollers: Vec<UnsafeCell<Poller>>,
    state: SpinLock<(
        Pool<Box<File>>,
        Pool<Box<Deadline>>,
        Vec<Box<File>>,
        Vec<Box<Deadline>>,
    )>,
}

// SAFETY: the state lock protects all pools and pending-free ownership. Each
// poller is accessed only by the thread selected by polling_thread_index.
unsafe impl Sync for FileMain {}

pub static FILE_MAIN: OnceLock<FileMain> = OnceLock::new();

#[init_function(name = "file_main_init")]
pub fn init_file_main(engine: &mut GlobalMain) -> RuntimeResult<()> {
    if FILE_MAIN.get().is_none() {
        let poller_count = engine.configured_worker_count().saturating_add(1);
        let file_main = FileMain::with_worker_count(poller_count)?;
        let _ = FILE_MAIN.set(file_main);
    }
    Ok(())
}

fn dispatch_file(
    file: &mut File,
    graph: &NodeRuntime,
    readiness: Readiness,
) -> RuntimeResult<usize> {
    let functions = file.functions();
    if readiness.contains(Readiness::ERROR)
        && let Some(function) = functions.error
    {
        file.record_error_event();
        function(graph, file)?;
        return Ok(1);
    }

    let mut dispatched = 0;
    if readiness.contains(Readiness::READ)
        && let Some(function) = functions.read
    {
        file.record_read_event();
        function(graph, file)?;
        dispatched += 1;
    }
    if readiness.contains(Readiness::WRITE)
        && file.write_enabled()
        && let Some(function) = functions.write
    {
        file.record_write_event();
        function(graph, file)?;
        dispatched += 1;
    }
    Ok(dispatched)
}

impl FileMain {
    /// Creates a File registry with one main poller for standalone callers.
    pub fn new() -> RuntimeResult<Self> {
        Self::with_worker_count(1)
    }

    pub(crate) fn with_worker_count(worker_count: usize) -> RuntimeResult<Self> {
        let pollers = (0..worker_count)
            .map(|_| Poller::new().map(UnsafeCell::new))
            .collect::<RuntimeResult<Vec<_>>>()?;
        Ok(Self {
            pollers,
            state: SpinLock::new((
                Pool::with_capacity(FILE_POOL_CAPACITY),
                Pool::with_capacity(FILE_POOL_CAPACITY),
                Vec::new(),
                Vec::new(),
            )),
        })
    }

    #[allow(clippy::mut_from_ref)]
    fn poller_mut(&self, thread_index: u32) -> RuntimeResult<&mut Poller> {
        let poller =
            self.pollers
                .get(thread_index as usize)
                .ok_or_else(|| RuntimeError::Lifecycle {
                    stage: "FileMain".to_owned(),
                    message: format!("polling thread {thread_index} is not configured"),
                })?;
        // SAFETY: the caller owns the poller selected by the File's
        // polling_thread_index and the lifecycle barrier excludes teardown.
        Ok(unsafe { &mut *poller.get() })
    }

    fn state(
        &self,
    ) -> SpinLockGuard<
        '_,
        (
            Pool<Box<File>>,
            Pool<Box<Deadline>>,
            Vec<Box<File>>,
            Vec<Box<Deadline>>,
        ),
    > {
        self.state.lock()
    }

    fn file_ptr(&self, index: u32) -> Option<*mut File> {
        let state = self.state();
        let file = state.0.get(index)?;
        file.is_active()
            .then(|| std::ptr::from_ref(file.as_ref()).cast_mut())
    }

    fn deadline_ptr(&self, index: u32) -> Option<*mut Deadline> {
        let state = self.state();
        let deadline = state.1.get(index)?;
        Some(std::ptr::from_ref(deadline.as_ref()).cast_mut())
    }

    fn release_pending(&self, thread_index: u32) {
        let mut state = self.state();
        state
            .2
            .retain(|file| file.polling_thread_index() != thread_index);
        state
            .3
            .retain(|deadline| deadline.polling_thread_index() != thread_index);
    }

    pub(crate) fn io_wake_fd_for_worker(&self, thread_index: u32) -> RuntimeResult<OwnedFd> {
        self.poller_mut(thread_index)?
            .try_clone_wake()
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "duplicate worker File wake descriptor",
                source,
            })
    }

    pub(crate) fn clear_io_wake_for_worker(&self, thread_index: u32) -> RuntimeResult<()> {
        self.poller_mut(thread_index)?.clear_wake();
        Ok(())
    }

    /// Registers a File and returns its existing `hammer-infra` Pool Index.
    pub fn add(&self, file: File) -> RuntimeResult<u32> {
        let thread_index = file.polling_thread_index();
        let index = {
            let mut state = self.state();
            state.0.insert(Box::new(file))
        };
        let spec = self
            .poll_spec(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        if let Err(error) = self.poller_mut(thread_index)?.add(spec) {
            let _ = self.state().0.remove(index);
            return Err(error);
        }
        Ok(index)
    }

    /// Registers a disarmed worker deadline and returns its generation-safe
    /// `hammer-infra` Pool Index.
    pub fn add_deadline(&self, deadline: Deadline) -> RuntimeResult<u32> {
        let thread_index = deadline.polling_thread_index();
        let index = {
            let mut state = self.state();
            state.1.insert(Box::new(deadline))
        };
        if let Err(error) = self.poller_mut(thread_index)?.add_deadline(index) {
            let _ = self.state().1.remove(index);
            return Err(error);
        }
        Ok(index)
    }

    /// Returns the currently armed duration, if the deadline is registered.
    pub fn deadline(&self, index: u32) -> RuntimeResult<Option<Duration>> {
        let deadline = self
            .deadline_ptr(index)
            .ok_or(RuntimeError::DeadlineIndexInvalid { index })?;
        Ok(unsafe { (*deadline).duration })
    }

    /// Arms or disarms a registered deadline.
    pub fn set_deadline(&self, index: u32, duration: Option<Duration>) -> RuntimeResult<()> {
        let deadline = self
            .deadline_ptr(index)
            .ok_or(RuntimeError::DeadlineIndexInvalid { index })?;
        let (thread_index, previous_duration) =
            unsafe { ((*deadline).polling_thread_index(), (*deadline).duration) };
        let poller = self.poller_mut(thread_index)?;
        if let Err(error) = poller.set_deadline(index, duration) {
            if poller.set_deadline(index, previous_duration).is_err() {
                tracing::error!(
                    ?index,
                    "failed to restore File deadline after update failed"
                );
            }
            return Err(error);
        }
        let mut state = self.state();
        let deadline = state
            .1
            .get_mut(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        deadline.duration = duration;
        Ok(())
    }

    /// Removes a deadline after canceling its platform registration.
    pub fn delete_deadline(&self, index: u32) -> RuntimeResult<bool> {
        let Some(deadline) = self.deadline_ptr(index) else {
            return Ok(false);
        };
        let thread_index = unsafe { (*deadline).polling_thread_index() };
        self.poller_mut(thread_index)?.delete_deadline(index)?;
        let mut state = self.state();
        let Some(deadline) = state.1.remove(index) else {
            return Ok(false);
        };
        state.3.push(deadline);
        Ok(true)
    }

    /// Returns whether the generation-safe File index is currently registered.
    pub fn is_registered(&self, index: u32) -> bool {
        self.file_ptr(index).is_some()
    }

    /// Returns a registered File's description without exposing its record.
    pub fn file_description(&self, index: u32) -> Option<String> {
        let file = self.file_ptr(index)?;
        Some(unsafe { (*file).description().to_owned() })
    }

    /// Returns a registered File's callback-owned private data.
    pub fn file_private_data(&self, index: u32) -> Option<u64> {
        let file = self.file_ptr(index)?;
        Some(unsafe { (*file).private_data() })
    }

    /// Returns the read, write, and error callback counts for a registered File.
    pub fn file_event_counts(&self, index: u32) -> Option<(u64, u64, u64)> {
        let file = self.file_ptr(index)?;
        Some(unsafe {
            (
                (*file).read_events(),
                (*file).write_events(),
                (*file).error_events(),
            )
        })
    }

    fn poll_spec(&self, index: u32) -> Option<PollSpec> {
        let file = self.file_ptr(index)?;
        Some(unsafe { PollSpec::new(index, &*file) })
    }

    /// Reads into vectors through the live File selected by its Index.
    ///
    /// # Safety
    /// Every iovec must reference writable memory for its declared length and
    /// remain valid until this synchronous call returns.
    pub unsafe fn readv(
        &self,
        index: u32,
        vectors: &mut [libc::iovec],
    ) -> RuntimeResult<Option<usize>> {
        let file = self
            .file_ptr(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let fd = unsafe { (*file).fd() };
        let count = libc::c_int::try_from(vectors.len())
            .expect("File iovec count is bounded by data-plane capacity");
        loop {
            // SAFETY: the caller upholds the iovec validity contract and File
            // owns the descriptor for the duration of this synchronous call.
            let read = unsafe { libc::readv(fd, vectors.as_ptr(), count) };
            if read >= 0 {
                return Ok(Some(read as usize));
            }
            let source = io::Error::last_os_error();
            match source.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(None),
                _ => return Err(RuntimeError::FileRead { source }.into()),
            }
        }
    }

    /// Writes vectors through the live File selected by its Index.
    ///
    /// # Safety
    /// Every iovec must reference readable memory for its declared length and
    /// remain valid and unmodified until this synchronous call returns.
    pub unsafe fn writev(
        &self,
        index: u32,
        vectors: &[libc::iovec],
    ) -> RuntimeResult<Option<usize>> {
        let file = self
            .file_ptr(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let fd = unsafe { (*file).fd() };
        let count = libc::c_int::try_from(vectors.len())
            .expect("File iovec count is bounded by data-plane capacity");
        loop {
            // SAFETY: the caller upholds the iovec validity contract and File
            // owns the descriptor for the duration of this synchronous call.
            let written = unsafe { libc::writev(fd, vectors.as_ptr(), count) };
            if written >= 0 {
                return Ok(Some(written as usize));
            }
            let source = io::Error::last_os_error();
            match source.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(None),
                _ => return Err(RuntimeError::FileWrite { source }.into()),
            }
        }
    }

    /// Removes backend interest, closes the descriptor, and defers record free.
    pub fn delete(&self, index: u32) -> RuntimeResult<bool> {
        let file = {
            let state = self.state();
            if !state.0.contains_key(index) {
                return Ok(false);
            }
            let Some(file) = state.0.get(index) else {
                return Ok(false);
            };
            if !file.is_active() {
                return Ok(false);
            }
            std::ptr::from_ref(file.as_ref()).cast_mut()
        };
        let (thread_index, spec) =
            unsafe { ((*file).polling_thread_index(), PollSpec::new(index, &*file)) };
        self.poller_mut(thread_index)?.delete(spec)?;
        let mut state = self.state();
        let Some(mut file) = state.0.remove(index) else {
            return Ok(false);
        };
        file.set_active(false);
        file.close();
        state.2.push(file);
        Ok(true)
    }

    /// Changes write interest without replacing the File Index.
    pub fn set_data_available_to_write(&self, index: u32, available: bool) -> RuntimeResult<bool> {
        let file = self
            .file_ptr(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let (thread_index, previous, before) = unsafe {
            (
                (*file).polling_thread_index(),
                (*file).write_enabled(),
                PollSpec::new(index, &*file),
            )
        };
        if previous == available {
            return Ok(previous);
        }
        let mut after = before;
        after.write = available;
        self.poller_mut(thread_index)?.modify(before, after)?;
        let mut state = self.state();
        let file = state
            .0
            .get_mut(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        file.set_write_enabled(available);
        Ok(previous)
    }

    /// Registers a bound Unix listener with read interest, mirroring VPP's
    /// `clib_file_main_add_socket`. Pending connections are pulled through
    /// [`Self::accept`].
    pub fn add_listener(
        &self,
        listener: UnixListener,
        description: impl Into<String>,
        private_data: u64,
        functions: FileFunctions,
    ) -> RuntimeResult<u32> {
        listener
            .set_nonblocking(true)
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "set Binary API listener nonblocking",
                source,
            })?;
        let index = self.add(File::new(
            OwnedFd::from(listener),
            description.into(),
            private_data,
            functions,
        ))?;
        Ok(index)
    }

    /// Accepts one pending connection from a registered listener, or returns
    /// `None` when no connection is pending. The accepted socket is
    /// registered with read interest. The listener registration is untouched:
    /// accept runs on a duplicated descriptor.
    pub fn accept(
        &self,
        listener: u32,
        description: impl Into<String>,
        private_data: u64,
        functions: FileFunctions,
    ) -> RuntimeResult<Option<u32>> {
        let file = self
            .file_ptr(listener)
            .ok_or(RuntimeError::FileIndexInvalid { index: listener })?;
        let duplicated = unsafe { duplicate_file_descriptor(&*file) }.map_err(|source| {
            RuntimeError::FilePollerIo {
                operation: "duplicate listener for accept",
                source,
            }
        })?;
        let stream = match UnixListener::from(duplicated).accept() {
            Ok((stream, _)) => stream,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(source) => return Err(RuntimeError::FileAccept { source }.into()),
        };
        stream
            .set_nonblocking(true)
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "set accepted Binary API socket nonblocking",
                source,
            })?;
        // macOS: writes to a vanished peer must surface as `Closed`, not kill
        // the daemon with SIGPIPE. The option is per-socket, so set it once
        // while the connection is fresh; per-write setsockopt fails after the
        // peer's EOF is drained.
        #[cfg(target_os = "macos")]
        {
            let one: libc::c_int = 1;
            // SAFETY: `setsockopt` on the accepted socket descriptor.
            let rc = unsafe {
                libc::setsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_NOSIGPIPE,
                    &one as *const libc::c_int as *const libc::c_void,
                    size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            if rc != 0 {
                // macOS rejects setsockopt with EINVAL on a connection whose
                // peer closed before accept (EOF is already pending on the
                // accepted socket). Such a connection cannot be served and
                // cannot take SO_NOSIGPIPE, so drop it and report no
                // connection; the daemon keeps polling.
                drop(stream);
                return Ok(None);
            }
        }
        let index = self.add(File::new(
            OwnedFd::from(stream),
            description.into(),
            private_data,
            functions,
        ))?;
        Ok(Some(index))
    }

    /// Performs one synchronous nonblocking read through a duplicated
    /// descriptor, so the poll registration never consumes data or
    /// readiness.
    pub fn read_some(&self, index: u32, buffer: &mut [u8]) -> RuntimeResult<FileIoStatus> {
        let file = self
            .file_ptr(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let duplicated = unsafe { duplicate_file_descriptor(&*file) }.map_err(|source| {
            RuntimeError::FilePollerIo {
                operation: "duplicate descriptor for read",
                source,
            }
        })?;
        let mut stream = UnixStream::from(duplicated);
        match stream.read(buffer) {
            Ok(0) => Ok(FileIoStatus::Closed),
            Ok(n) => Ok(FileIoStatus::Progress(n)),
            Err(source) if peer_closed(&source) => Ok(FileIoStatus::Closed),
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                Ok(FileIoStatus::WouldBlock)
            }
            Err(source) => Err(RuntimeError::FileRead { source }.into()),
        }
    }

    /// Performs one synchronous nonblocking write through a duplicated
    /// descriptor, so the poll registration is never consumed by a partial
    /// flush.
    pub fn write_some(&self, index: u32, buffer: &[u8]) -> RuntimeResult<FileIoStatus> {
        let file = self
            .file_ptr(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let duplicated = unsafe { duplicate_file_descriptor(&*file) }.map_err(|source| {
            RuntimeError::FilePollerIo {
                operation: "duplicate descriptor for write",
                source,
            }
        })?;
        // A vanished client must surface as `Closed`, not kill the daemon
        // with SIGPIPE. Linux honors MSG_NOSIGNAL per send; macOS sets
        // SO_NOSIGPIPE once on the socket in [`Self::accept`] (per-write
        // setsockopt fails once the peer has closed and the EOF is drained).
        #[cfg(target_os = "linux")]
        let flags = libc::MSG_NOSIGNAL;
        #[cfg(not(target_os = "linux"))]
        let flags = 0;
        // SAFETY: `duplicated` is a live socket descriptor for the call
        // duration.
        let written = unsafe {
            libc::send(
                duplicated.as_raw_fd(),
                buffer.as_ptr() as *const libc::c_void,
                buffer.len(),
                flags,
            )
        };
        if written >= 0 {
            if written == 0 {
                return Ok(FileIoStatus::Closed);
            }
            return Ok(FileIoStatus::Progress(written as usize));
        }
        let source = io::Error::last_os_error();
        match source.kind() {
            _ if peer_closed(&source) => Ok(FileIoStatus::Closed),
            io::ErrorKind::WouldBlock => Ok(FileIoStatus::WouldBlock),
            _ => Err(RuntimeError::FileWrite { source }.into()),
        }
    }

    /// Performs one nonblocking readiness poll and dispatches main-thread callbacks.
    pub fn poll(&self, graph: &NodeRuntime) -> RuntimeResult<usize> {
        self.poll_for_worker(0, graph)
    }

    /// Performs one nonblocking readiness poll for the selected owner thread.
    pub(crate) fn poll_for_worker(
        &self,
        thread_index: u32,
        graph: &NodeRuntime,
    ) -> RuntimeResult<usize> {
        self.release_pending(thread_index);
        let mut events = [PollEvent::default(); POLL_BATCH_SIZE];
        let count = self.poller_mut(thread_index)?.poll(&mut events)?;
        let mut dispatched = 0;
        for event in &events[..count] {
            match event.target {
                Some(PollTarget::File(index)) => {
                    let Some(file) = self.file_ptr(index) else {
                        continue;
                    };
                    let remove = unsafe {
                        if event.readiness.contains(Readiness::ERROR)
                            && (*file).functions().error.is_none()
                        {
                            true
                        } else {
                            dispatched += dispatch_file(&mut *file, graph, event.readiness)?;
                            false
                        }
                    };
                    if remove {
                        self.delete(index)?;
                        continue;
                    }
                    if event.rearm {
                        let Some(spec) = self.poll_spec(index) else {
                            continue;
                        };
                        self.poller_mut(thread_index)?.add(spec)?;
                    }
                }
                Some(PollTarget::Deadline(index)) => {
                    let Some(deadline) = self.deadline_ptr(index) else {
                        continue;
                    };
                    self.poller_mut(thread_index)?.consume_deadline(index)?;
                    let deadline = unsafe { &mut *deadline };
                    deadline.expiry_events += 1;
                    let function = deadline.function;
                    function(graph, deadline)?;
                    dispatched += 1;
                    if event.rearm {
                        if self.deadline_ptr(index).is_some() {
                            self.poller_mut(thread_index)?.rearm_deadline(index)?;
                        }
                    }
                }
                None => {}
            }
        }
        Ok(dispatched)
    }
}

/// Control-plane readiness adapter for the main shard in [`FILE_MAIN`].
///
/// The adapter owns only a duplicated wake descriptor. The global `FileMain`
/// remains the sole owner of files, pollers, worker delivery, and reclamation.
pub struct AsyncFileMain {
    wake: AsyncFd<OwnedFd>,
}

impl AsyncFileMain {
    /// Creates the Tokio adapter for the main-thread shard.
    pub fn new() -> RuntimeResult<Self> {
        let duplicate = FILE_MAIN
            .get()
            .expect("FileMain is initialized before runtime services start")
            .io_wake_fd_for_worker(0)?;
        let handle = Handle::try_current().map_err(|source| RuntimeError::FilePollerIo {
            operation: "enter Tokio reactor for AsyncFileMain",
            source: io::Error::other(source),
        })?;
        let _enter = handle.enter();
        let wake = AsyncFd::with_interest(duplicate, Interest::READABLE).map_err(|source| {
            RuntimeError::FilePollerIo {
                operation: "register AsyncFileMain wake descriptor with Tokio",
                source,
            }
        })?;
        Ok(Self { wake })
    }

    /// Awaits main-shard readiness and performs one nonblocking poll.
    pub async fn next_ready(&mut self, graph: &NodeRuntime) -> RuntimeResult<usize> {
        let mut guard =
            self.wake
                .readable()
                .await
                .map_err(|source| RuntimeError::FilePollerIo {
                    operation: "await AsyncFileMain wake readiness",
                    source,
                })?;
        let file_main = FILE_MAIN
            .get()
            .expect("FileMain is initialized before runtime services start");
        file_main.clear_io_wake_for_worker(0)?;
        guard.clear_ready();
        file_main.poll_for_worker(0, graph)
    }

    /// Returns the direct global FileMain registry.
    pub fn file_main(&self) -> &'static FileMain {
        FILE_MAIN
            .get()
            .expect("FileMain is initialized before runtime services start")
    }
}

pub(super) const POLL_BATCH_SIZE: usize = 16;
pub(super) const FILE_POOL_CAPACITY: usize = 1024;

#[derive(Clone, Copy, Debug)]
pub(super) struct PollSpec {
    pub(super) index: u32,
    pub(super) fd: RawFd,
    pub(super) read: bool,
    pub(super) write: bool,
}

impl PollSpec {
    #[inline]
    pub(super) fn new(index: u32, file: &File) -> Self {
        let functions = file.functions();
        Self {
            index,
            fd: file.fd(),
            read: functions.read.is_some() || functions.error.is_some(),
            write: file.write_enabled(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum PollTarget {
    File(u32),
    Deadline(u32),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PollEvent {
    pub(super) target: Option<PollTarget>,
    pub(super) readiness: Readiness,
    pub(super) rearm: bool,
}

impl Default for PollEvent {
    fn default() -> Self {
        Self {
            target: None,
            readiness: Readiness::default(),
            rearm: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Readiness(u8);

impl Readiness {
    pub(super) const READ: Self = Self(1 << 0);
    pub(super) const WRITE: Self = Self(1 << 1);
    pub(super) const ERROR: Self = Self(1 << 2);

    #[inline]
    pub(super) const fn contains(self, readiness: Self) -> bool {
        self.0 & readiness.0 != 0
    }

    #[inline]
    pub(super) fn insert(&mut self, readiness: Self) {
        self.0 |= readiness.0;
    }
}

/// Maps peer-disappearance errors to the `Closed` outcome instead of a
/// daemon-fatal error.
fn peer_closed(source: &io::Error) -> bool {
    matches!(
        source.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
    )
}
