//! Worker-local file readiness, corresponding to VPP's `clib_file_t` and
//! `clib_file_main_t`.
//!
//! This module owns descriptors, readiness dispatch, and indexed synchronous
//! descriptor I/O. Device queues own packet and queue semantics.

use std::fmt;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::Duration;

use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::runtime::Handle;

use crate::error::{RuntimeError, RuntimeResult};
use hammer_infra::pool::{Index, Pool};

use crate::NodeRuntime;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
use macos::Poller;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux::Poller;

/// Worker-local callback invoked for one ready descriptor event.
pub type FileFunction = fn(&NodeRuntime, &mut File) -> RuntimeResult<()>;

/// Worker-local callback invoked when a registered deadline expires.
pub type DeadlineFunction = fn(&NodeRuntime, &mut Deadline) -> RuntimeResult<()>;

/// Functions dispatched for one File's read, write, and error readiness.
#[derive(Clone, Copy, Debug, Default)]
pub struct FileFunctions {
    pub read: Option<FileFunction>,
    pub write: Option<FileFunction>,
    pub error: Option<FileFunction>,
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

/// Readiness metadata, callback state, and ownership for one descriptor.
pub struct File {
    fd: OwnedFd,
    description: String,
    private_data: u64,
    functions: FileFunctions,
    write_enabled: bool,
    read_events: u64,
    write_events: u64,
    error_events: u64,
}

impl File {
    /// Creates one inactive-write File record.
    ///
    /// Read interest is derived from the read and error functions.
    /// Write interest is enabled later through
    /// [`FileMain::set_data_available_to_write`].
    pub fn new(
        fd: OwnedFd,
        description: String,
        private_data: u64,
        functions: FileFunctions,
    ) -> Self {
        Self {
            fd,
            description,
            private_data,
            functions,
            write_enabled: false,
            read_events: 0,
            write_events: 0,
            error_events: 0,
        }
    }

    #[inline]
    /// Returns the registered descriptor without transferring ownership.
    pub fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Duplicates the owned descriptor so a synchronous socket call never
    /// races with the platform backend's poll registration or consumes its
    /// readiness.
    fn try_clone(&self) -> io::Result<OwnedFd> {
        // SAFETY: `F_DUPFD_CLOEXEC` returns a fresh descriptor referencing
        // the same socket; the registered descriptor stays FileMain-owned.
        let duplicated = unsafe { libc::fcntl(self.fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if duplicated < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `duplicated` is a valid owned descriptor from fcntl.
        Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
    }

    #[inline]
    /// Returns the operator-facing File description.
    pub fn description(&self) -> &str {
        &self.description
    }

    #[inline]
    /// Returns callback-owned opaque data.
    pub fn private_data(&self) -> u64 {
        self.private_data
    }

    #[inline]
    /// Replaces callback-owned opaque data.
    pub fn set_private_data(&mut self, private_data: u64) {
        self.private_data = private_data;
    }

    #[inline]
    /// Returns the number of dispatched read callbacks.
    pub fn read_events(&self) -> u64 {
        self.read_events
    }

    #[inline]
    /// Returns the number of dispatched write callbacks.
    pub fn write_events(&self) -> u64 {
        self.write_events
    }

    #[inline]
    /// Returns the number of dispatched error callbacks.
    pub fn error_events(&self) -> u64 {
        self.error_events
    }

    #[inline]
    fn poll_spec(&self, index: Index) -> PollSpec {
        PollSpec {
            index,
            fd: self.fd(),
            read: self.functions.read.is_some() || self.functions.error.is_some(),
            write: self.write_enabled,
        }
    }

    fn dispatch(&mut self, graph: &NodeRuntime, readiness: Readiness) -> RuntimeResult<usize> {
        if readiness.contains(Readiness::ERROR)
            && let Some(function) = self.functions.error
        {
            self.error_events += 1;
            function(graph, self)?;
            return Ok(1);
        }

        let mut dispatched = 0;
        if readiness.contains(Readiness::READ)
            && let Some(function) = self.functions.read
        {
            self.read_events += 1;
            function(graph, self)?;
            dispatched += 1;
        }
        if readiness.contains(Readiness::WRITE)
            && self.write_enabled
            && let Some(function) = self.functions.write
        {
            self.write_events += 1;
            function(graph, self)?;
            dispatched += 1;
        }
        Ok(dispatched)
    }
}

/// A worker-local deadline registration owned by [`FileMain`].
pub struct Deadline {
    description: String,
    private_data: u64,
    function: DeadlineFunction,
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
            duration: None,
            expiry_events: 0,
        }
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

impl fmt::Debug for File {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("File")
            .field("fd", &self.fd())
            .field("description", &self.description)
            .field("private_data", &self.private_data)
            .field("write_enabled", &self.write_enabled)
            .field("read_events", &self.read_events)
            .field("write_events", &self.write_events)
            .field("error_events", &self.error_events)
            .finish()
    }
}

/// Generation-safe File registry and readiness dispatcher for one Data Worker.
pub struct FileMain {
    poller: Poller,
    files: Pool<File>,
    deadlines: Pool<Deadline>,
}

impl FileMain {
    /// Creates the platform poller and empty File registry for one Data Worker.
    pub fn new() -> RuntimeResult<Self> {
        Ok(Self {
            poller: Poller::new()?,
            files: Pool::with_capacity(FILE_POOL_CAPACITY),
            deadlines: Pool::with_capacity(FILE_POOL_CAPACITY),
        })
    }

    /// Descriptor that becomes readable when File readiness is pending, so an
    /// idle main loop can sleep in the tokio reactor yet wake for I/O.
    pub(crate) fn io_wake_fd(&self) -> RuntimeResult<OwnedFd> {
        self.poller
            .try_clone_wake()
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "duplicate worker File wake descriptor",
                source,
            })
    }

    /// Consumes the wake signal after an idle wake-up; the next [`Self::poll`]
    /// collects the readiness that raised it.
    pub(crate) fn clear_io_wake(&self) {
        self.poller.clear_wake();
    }

    /// Registers a File and returns its existing `hammer-infra` Pool Index.
    pub fn add(&mut self, file: File) -> RuntimeResult<Index> {
        let index = self.files.insert(file).ok_or(RuntimeError::FilePoolFull)?;
        let spec = self
            .files
            .get(index)
            .map(|file| file.poll_spec(index))
            .expect("newly inserted File must resolve");
        if let Err(error) = self.poller.add(spec) {
            self.files.remove(index);
            return Err(error);
        }
        Ok(index)
    }

    /// Registers a disarmed worker deadline and returns its generation-safe
    /// `hammer-infra` Pool Index.
    pub fn add_deadline(&mut self, deadline: Deadline) -> RuntimeResult<Index> {
        let index = self
            .deadlines
            .insert(deadline)
            .ok_or(RuntimeError::DeadlinePoolFull)?;
        if let Err(error) = self.poller.add_deadline(index) {
            self.deadlines.remove(index);
            return Err(error);
        }
        Ok(index)
    }

    /// Returns the currently armed duration, if the deadline is registered.
    pub fn deadline(&self, index: Index) -> RuntimeResult<Option<Duration>> {
        self.deadlines
            .get(index)
            .map(|deadline| deadline.duration)
            .ok_or(RuntimeError::DeadlineIndexInvalid { index })
    }

    /// Arms or disarms a registered deadline.
    pub fn set_deadline(&mut self, index: Index, duration: Option<Duration>) -> RuntimeResult<()> {
        if self.deadlines.get(index).is_none() {
            return Err(RuntimeError::DeadlineIndexInvalid { index });
        }
        let previous_duration = self
            .deadlines
            .get(index)
            .map(|deadline| deadline.duration)
            .expect("validated deadline remains registered");
        if let Err(error) = self.poller.set_deadline(index, duration) {
            if let Err(cleanup_error) = self.poller.set_deadline(index, previous_duration) {
                tracing::error!(
                    %cleanup_error,
                    ?index,
                    "failed to restore File deadline after update failed"
                );
            }
            return Err(error);
        }
        let deadline = self
            .deadlines
            .get_mut(index)
            .expect("validated deadline remains registered");
        deadline.duration = duration;
        Ok(())
    }

    /// Removes a deadline after canceling its platform registration.
    pub fn delete_deadline(&mut self, index: Index) -> RuntimeResult<bool> {
        if self.deadlines.get(index).is_none() {
            return Ok(false);
        }
        self.poller.delete_deadline(index)?;
        self.deadlines.remove(index);
        Ok(true)
    }

    #[inline]
    /// Looks up a live File, rejecting stale generations.
    pub fn get(&self, index: Index) -> Option<&File> {
        self.files.get(index)
    }

    /// Reads into vectors through the live File selected by its Index.
    ///
    /// # Safety
    /// Every iovec must reference writable memory for its declared length and
    /// remain valid until this synchronous call returns.
    pub unsafe fn readv(
        &self,
        index: Index,
        vectors: &mut [libc::iovec],
    ) -> RuntimeResult<Option<usize>> {
        let file = self
            .files
            .get(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let count = libc::c_int::try_from(vectors.len())
            .expect("File iovec count is bounded by data-plane capacity");
        loop {
            // SAFETY: the caller upholds the iovec validity contract and File
            // owns the descriptor for the duration of this synchronous call.
            let read = unsafe { libc::readv(file.fd(), vectors.as_ptr(), count) };
            if read >= 0 {
                return Ok(Some(read as usize));
            }
            let source = io::Error::last_os_error();
            match source.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(None),
                _ => return Err(RuntimeError::FileRead { source }),
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
        index: Index,
        vectors: &[libc::iovec],
    ) -> RuntimeResult<Option<usize>> {
        let file = self
            .files
            .get(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let count = libc::c_int::try_from(vectors.len())
            .expect("File iovec count is bounded by data-plane capacity");
        loop {
            // SAFETY: the caller upholds the iovec validity contract and File
            // owns the descriptor for the duration of this synchronous call.
            let written = unsafe { libc::writev(file.fd(), vectors.as_ptr(), count) };
            if written >= 0 {
                return Ok(Some(written as usize));
            }
            let source = io::Error::last_os_error();
            match source.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(None),
                _ => return Err(RuntimeError::FileWrite { source }),
            }
        }
    }

    /// Removes backend interest before releasing the File record.
    pub fn delete(&mut self, index: Index) -> RuntimeResult<bool> {
        let Some(spec) = self.files.get(index).map(|file| file.poll_spec(index)) else {
            return Ok(false);
        };
        self.poller.delete(spec)?;
        self.files.remove(index);
        Ok(true)
    }

    /// Changes write interest without replacing the File Index.
    pub fn set_data_available_to_write(
        &mut self,
        index: Index,
        available: bool,
    ) -> RuntimeResult<bool> {
        let Some(file) = self.files.get(index) else {
            return Err(RuntimeError::FileIndexInvalid { index });
        };
        let previous = file.write_enabled;
        if previous == available {
            return Ok(previous);
        }
        let before = file.poll_spec(index);
        let mut after = before;
        after.write = available;
        self.poller.modify(before, after)?;
        let Some(file) = self.files.get_mut(index) else {
            return Err(RuntimeError::FileIndexInvalid { index });
        };
        file.write_enabled = available;
        Ok(previous)
    }

    /// Registers a bound Unix listener with read interest, mirroring VPP's
    /// `clib_file_main_add_socket`. Pending connections are pulled through
    /// [`Self::accept`].
    pub fn add_listener(
        &mut self,
        listener: UnixListener,
        description: impl Into<String>,
        private_data: u64,
        functions: FileFunctions,
    ) -> RuntimeResult<Index> {
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
        &mut self,
        listener: Index,
        description: impl Into<String>,
        private_data: u64,
        functions: FileFunctions,
    ) -> RuntimeResult<Option<Index>> {
        let file = self
            .files
            .get(listener)
            .ok_or(RuntimeError::FileIndexInvalid { index: listener })?;
        let duplicated = file
            .try_clone()
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "duplicate listener for accept",
                source,
            })?;
        let stream = match UnixListener::from(duplicated).accept() {
            Ok((stream, _)) => stream,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(source) => return Err(RuntimeError::FileAccept { source }),
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
    pub fn read_some(&self, index: Index, buffer: &mut [u8]) -> RuntimeResult<FileIoStatus> {
        let file = self
            .files
            .get(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let duplicated = file
            .try_clone()
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "duplicate descriptor for read",
                source,
            })?;
        let mut stream = UnixStream::from(duplicated);
        match stream.read(buffer) {
            Ok(0) => Ok(FileIoStatus::Closed),
            Ok(n) => Ok(FileIoStatus::Progress(n)),
            Err(source) if peer_closed(&source) => Ok(FileIoStatus::Closed),
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                Ok(FileIoStatus::WouldBlock)
            }
            Err(source) => Err(RuntimeError::FileRead { source }),
        }
    }

    /// Performs one synchronous nonblocking write through a duplicated
    /// descriptor, so the poll registration is never consumed by a partial
    /// flush.
    pub fn write_some(&self, index: Index, buffer: &[u8]) -> RuntimeResult<FileIoStatus> {
        let file = self
            .files
            .get(index)
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let duplicated = file
            .try_clone()
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "duplicate descriptor for write",
                source,
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
            _ => Err(RuntimeError::FileWrite { source }),
        }
    }

    /// Performs one nonblocking readiness poll and dispatches live callbacks.
    pub fn poll(&mut self, graph: &NodeRuntime) -> RuntimeResult<usize> {
        let mut events = [PollEvent::default(); POLL_BATCH_SIZE];
        let count = self.poller.poll(&mut events)?;
        let mut dispatched = 0;
        for event in &events[..count] {
            match event.target {
                Some(PollTarget::File(index)) => {
                    let Some(file) = self.files.get_mut(index) else {
                        continue;
                    };
                    if event.readiness.contains(Readiness::ERROR) && file.functions.error.is_none()
                    {
                        self.delete(index)?;
                        continue;
                    }
                    dispatched += file.dispatch(graph, event.readiness)?;
                    if event.rearm {
                        let spec = self
                            .files
                            .get(index)
                            .map(|file| file.poll_spec(index))
                            .expect("callback dispatch cannot remove its File");
                        self.poller.add(spec)?;
                    }
                }
                Some(PollTarget::Deadline(index)) => {
                    if self.deadlines.get(index).is_none() {
                        continue;
                    }
                    self.poller.consume_deadline(index)?;
                    let deadline = self
                        .deadlines
                        .get_mut(index)
                        .expect("validated deadline remains registered");
                    deadline.expiry_events += 1;
                    (deadline.function)(graph, deadline)?;
                    dispatched += 1;
                    if event.rearm {
                        self.poller.rearm_deadline(index)?;
                    }
                }
                None => {}
            }
        }
        Ok(dispatched)
    }

    /// Converts this FileMain into an owned [`AsyncFileMain`], the
    /// control-plane counterpart of the data-worker FileMain path.
    ///
    /// Only the platform poller's duplicated wake descriptor is wrapped in a
    /// Tokio `AsyncFd`; managed sockets remain FileMain-owned descriptors
    /// registered exclusively with the platform backend. A current Tokio
    /// runtime context is required; its absence fails rather than panics.
    pub fn into_async(self) -> RuntimeResult<AsyncFileMain> {
        let duplicate = self.io_wake_fd()?;
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
        Ok(AsyncFileMain {
            file_main: self,
            wake,
        })
    }
}

/// Control-plane FileMain owned by one Tokio task.
///
/// `AsyncFileMain` is the single-owner control-plane counterpart to the
/// data-worker FileMain path: it owns a plain [`FileMain`] and the Tokio
/// `AsyncFd` created from only that FileMain's duplicated backend wake
/// descriptor. Managed sockets remain FileMain-owned descriptors registered
/// exclusively with the platform backend and are never dual-registered with
/// Tokio. It is `!Sync` and therefore never registry-shared; one control
/// ProcessNode owns it for its lifetime.
pub struct AsyncFileMain {
    file_main: FileMain,
    wake: AsyncFd<OwnedFd>,
}

impl AsyncFileMain {
    /// Awaits FileMain readiness and performs one nonblocking poll.
    ///
    /// Mirrors the data-worker wake order: the backend wake is cleared
    /// before the reactor readiness is acknowledged, then
    /// [`FileMain::poll`] dispatches live callbacks. Callback and poller
    /// errors propagate to the awaiting control ProcessNode.
    pub async fn next_ready(&mut self, graph: &NodeRuntime) -> RuntimeResult<usize> {
        let mut guard =
            self.wake
                .readable()
                .await
                .map_err(|source| RuntimeError::FilePollerIo {
                    operation: "await AsyncFileMain wake readiness",
                    source,
                })?;
        self.file_main.clear_io_wake();
        guard.clear_ready();
        self.file_main.poll(graph)
    }

    /// Returns the owned FileMain for descriptor registration and lookup.
    pub fn file_main(&self) -> &FileMain {
        &self.file_main
    }

    /// Returns the owned FileMain mutably for descriptor registration.
    pub fn file_main_mut(&mut self) -> &mut FileMain {
        &mut self.file_main
    }
}

pub(super) const POLL_BATCH_SIZE: usize = 16;
pub(super) const FILE_POOL_CAPACITY: usize = 1024;

#[derive(Clone, Copy, Debug)]
pub(super) struct PollSpec {
    pub(super) index: Index,
    pub(super) fd: RawFd,
    pub(super) read: bool,
    pub(super) write: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum PollTarget {
    File(Index),
    Deadline(Index),
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

#[inline]
pub(super) fn encode_index(index: Index) -> u64 {
    (u64::from(index.generation()) << 32) | u64::from(index.slot())
}

#[inline]
pub(super) fn decode_index(token: u64) -> Option<Index> {
    let generation = (token >> 32) as u32;
    (generation != 0).then(|| Index::new(token as u32, generation))
}
