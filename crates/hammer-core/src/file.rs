//! Shared generic file callback ABI.

use std::fmt;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

/// Callback invoked for one ready file descriptor.
pub type FileFunction<Context, Error> =
    fn(&Context, &mut File<Context, Error>) -> Result<(), Error>;

/// Read, write, and error callbacks associated with one [`File`].
pub struct FileFunctions<Context, Error> {
    pub read: Option<FileFunction<Context, Error>>,
    pub write: Option<FileFunction<Context, Error>>,
    pub error: Option<FileFunction<Context, Error>>,
}

impl<Context, Error> Copy for FileFunctions<Context, Error> {}

impl<Context, Error> Clone for FileFunctions<Context, Error> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Context, Error> Default for FileFunctions<Context, Error> {
    fn default() -> Self {
        Self {
            read: None,
            write: None,
            error: None,
        }
    }
}

impl<Context, Error> fmt::Debug for FileFunctions<Context, Error> {
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
pub struct File<Context, Error> {
    fd: Option<OwnedFd>,
    description: String,
    private_data: u64,
    functions: FileFunctions<Context, Error>,
    write_enabled: bool,
    polling_thread_index: u32,
    read_events: u64,
    write_events: u64,
    error_events: u64,
    active: bool,
}

impl<Context, Error> File<Context, Error> {
    /// Creates a file record with write interest disabled.
    pub fn new(
        fd: OwnedFd,
        description: String,
        private_data: u64,
        functions: FileFunctions<Context, Error>,
    ) -> Self {
        Self {
            fd: Some(fd),
            description,
            private_data,
            functions,
            write_enabled: false,
            polling_thread_index: 0,
            read_events: 0,
            write_events: 0,
            error_events: 0,
            active: true,
        }
    }

    /// Returns the registered descriptor without transferring ownership.
    #[inline]
    pub fn fd(&self) -> RawFd {
        self.fd.as_ref().map_or(-1, AsRawFd::as_raw_fd)
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
    pub fn functions(&self) -> FileFunctions<Context, Error> {
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
    pub fn record_read_event(&mut self) {
        self.read_events += 1;
    }

    /// Records one dispatched write callback.
    #[inline]
    pub fn record_write_event(&mut self) {
        self.write_events += 1;
    }

    /// Records one dispatched error callback.
    #[inline]
    pub fn record_error_event(&mut self) {
        self.error_events += 1;
    }

    /// Returns the number of dispatched read callbacks.
    #[inline]
    pub fn read_events(&self) -> u64 {
        self.read_events
    }

    /// Returns the number of dispatched write callbacks.
    #[inline]
    pub fn write_events(&self) -> u64 {
        self.write_events
    }

    /// Returns the number of dispatched error callbacks.
    #[inline]
    pub fn error_events(&self) -> u64 {
        self.error_events
    }
}

impl<Context, Error> fmt::Debug for File<Context, Error> {
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
