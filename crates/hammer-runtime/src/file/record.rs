//! Generic File record owned by the runtime's I/O subsystem.

use std::cell::Cell;
use std::fmt;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

/// Readiness contract used by a File's polling owner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FileReadinessMode {
    #[default]
    Level,
    Drain,
}

/// Callback invoked for one ready file descriptor.
pub type FileFunction<Context, Error, Owner = ()> =
    fn(&mut Context, &File<Context, Error, Owner>) -> Result<(), Error>;

/// Read, write, and error callbacks associated with one [`File`].
pub struct FileFunctions<Context, Error, Owner = ()> {
    pub read: Option<FileFunction<Context, Error, Owner>>,
    pub write: Option<FileFunction<Context, Error, Owner>>,
    pub error: Option<FileFunction<Context, Error, Owner>>,
}

impl<Context, Error, Owner> Copy for FileFunctions<Context, Error, Owner> {}

impl<Context, Error, Owner> Clone for FileFunctions<Context, Error, Owner> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Context, Error, Owner> Default for FileFunctions<Context, Error, Owner> {
    fn default() -> Self {
        Self {
            read: None,
            write: None,
            error: None,
        }
    }
}

impl<Context, Error, Owner> fmt::Debug for FileFunctions<Context, Error, Owner> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileFunctions")
            .field("read", &self.read.is_some())
            .field("write", &self.write.is_some())
            .field("error", &self.error.is_some())
            .finish()
    }
}

/// Descriptor ownership and callback state for one registered file.
pub struct File<Context, Error, Owner = ()> {
    fd: Option<OwnedFd>,
    owner: Owner,
    description: String,
    private_data: u64,
    functions: FileFunctions<Context, Error, Owner>,
    write_enabled: bool,
    readiness_mode: FileReadinessMode,
    polling_thread_index: u32,
    read_events: Cell<u64>,
    write_events: u64,
    error_events: Cell<u64>,
    active: bool,
}

impl<Context, Error, Owner: Default> File<Context, Error, Owner> {
    /// Creates a file record with write interest disabled.
    pub fn new(
        fd: OwnedFd,
        description: String,
        private_data: u64,
        functions: FileFunctions<Context, Error, Owner>,
    ) -> Self {
        Self::with_owner(fd, description, private_data, functions, Owner::default())
    }
}

impl<Context, Error, Owner> File<Context, Error, Owner> {
    pub(crate) fn with_owner(
        fd: OwnedFd,
        description: String,
        private_data: u64,
        functions: FileFunctions<Context, Error, Owner>,
        owner: Owner,
    ) -> Self {
        Self {
            fd: Some(fd),
            owner,
            description,
            private_data,
            functions,
            write_enabled: false,
            readiness_mode: FileReadinessMode::Level,
            polling_thread_index: 0,
            read_events: Cell::new(0),
            write_events: 0,
            error_events: Cell::new(0),
            active: true,
        }
    }

    #[inline]
    pub(crate) fn owner(&self) -> &Owner {
        &self.owner
    }

    /// Returns the registered descriptor without transferring ownership.
    #[inline]
    pub fn fd(&self) -> RawFd {
        self.fd.as_ref().map_or(-1, |fd| fd.as_raw_fd())
    }

    /// Closes the descriptor while retaining this File record for deferred free.
    #[inline]
    pub fn close(&mut self) {
        self.fd.take();
    }

    /// Returns the operator-facing file description.
    #[inline]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns callback-owned opaque data.
    #[inline]
    pub fn private_data(&self) -> u64 {
        self.private_data
    }

    /// Replaces callback-owned opaque data.
    #[inline]
    pub fn set_private_data(&mut self, private_data: u64) {
        self.private_data = private_data;
    }

    /// Returns the callbacks associated with this file.
    #[inline]
    pub fn functions(&self) -> FileFunctions<Context, Error, Owner> {
        self.functions
    }

    /// Returns whether the File remains registered with its owner poller.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Marks the File active or pending deletion.
    #[inline]
    pub fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    /// Returns whether write readiness is enabled.
    #[inline]
    pub fn write_enabled(&self) -> bool {
        self.write_enabled
    }

    /// Enables or disables write readiness for the runtime poller.
    #[inline]
    pub fn set_write_enabled(&mut self, enabled: bool) {
        self.write_enabled = enabled;
    }

    /// Selects multishot readiness only when the owner drains or reschedules
    /// until the descriptor would block.
    #[inline]
    pub fn set_readiness_mode(&mut self, mode: FileReadinessMode) {
        self.readiness_mode = mode;
    }

    #[inline]
    pub fn readiness_mode(&self) -> FileReadinessMode {
        self.readiness_mode
    }

    /// Returns the worker that owns readiness polling for this file.
    #[inline]
    pub fn polling_thread_index(&self) -> u32 {
        self.polling_thread_index
    }

    /// Assigns the worker that owns readiness polling for this file.
    #[inline]
    pub fn set_polling_thread_index(&mut self, thread_index: u32) {
        self.polling_thread_index = thread_index;
    }

    /// Records one dispatched read callback.
    #[inline]
    pub fn record_read_event(&self) {
        self.read_events.set(self.read_events.get() + 1);
    }

    /// Records one dispatched write callback.
    #[inline]
    pub fn record_write_event(&mut self) {
        self.write_events += 1;
    }

    /// Records one dispatched error callback.
    #[inline]
    pub fn record_error_event(&self) {
        self.error_events.set(self.error_events.get() + 1);
    }

    /// Returns the number of dispatched read callbacks.
    #[inline]
    pub fn read_events(&self) -> u64 {
        self.read_events.get()
    }

    /// Returns the number of dispatched write callbacks.
    #[inline]
    pub fn write_events(&self) -> u64 {
        self.write_events
    }

    /// Returns the number of dispatched error callbacks.
    #[inline]
    pub fn error_events(&self) -> u64 {
        self.error_events.get()
    }
}

impl<Context, Error, Owner> fmt::Debug for File<Context, Error, Owner> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("File")
            .field("fd", &self.fd())
            .field("description", &self.description)
            .field("private_data", &self.private_data)
            .field("write_enabled", &self.write_enabled)
            .field("polling_thread_index", &self.polling_thread_index)
            .field("read_events", &self.read_events)
            .field("write_events", &self.write_events)
            .field("error_events", &self.error_events)
            .finish()
    }
}
