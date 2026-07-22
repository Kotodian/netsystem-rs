//! Worker-local file readiness, corresponding to VPP's `clib_file_t` and
//! `clib_file_main_t`.
//!
//! This module owns descriptor readiness and callback dispatch. Device and
//! Graph Node code remains responsible for reading and writing packet data.

use std::fmt;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

use crate::error::{RuntimeError, RuntimeResult};
use hammer_infra::pool::{Index, Pool};

use crate::DataWorkerId;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
use macos::Poller;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux::Poller;

/// Worker-local callback invoked for one ready descriptor event.
pub type FileFunction = fn(&mut File) -> RuntimeResult<()>;

/// Functions dispatched for one File's read, write, and error readiness.
#[derive(Clone, Copy, Debug, Default)]
pub struct FileFunctions {
    pub read: Option<FileFunction>,
    pub write: Option<FileFunction>,
    pub error: Option<FileFunction>,
}

/// Readiness metadata, callback state, and ownership for one descriptor.
pub struct File {
    fd: OwnedFd,
    polling_worker: DataWorkerId,
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
        polling_worker: DataWorkerId,
        description: String,
        private_data: u64,
        functions: FileFunctions,
    ) -> Self {
        Self {
            fd,
            polling_worker,
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

    #[inline]
    /// Returns the only Data Worker allowed to poll this File.
    pub fn polling_worker(&self) -> DataWorkerId {
        self.polling_worker
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

    fn dispatch(&mut self, readiness: Readiness) -> RuntimeResult<usize> {
        if readiness.contains(Readiness::ERROR)
            && let Some(function) = self.functions.error
        {
            self.error_events += 1;
            function(self)?;
            return Ok(1);
        }

        let mut dispatched = 0;
        if readiness.contains(Readiness::READ)
            && let Some(function) = self.functions.read
        {
            self.read_events += 1;
            function(self)?;
            dispatched += 1;
        }
        if readiness.contains(Readiness::WRITE)
            && self.write_enabled
            && let Some(function) = self.functions.write
        {
            self.write_events += 1;
            function(self)?;
            dispatched += 1;
        }
        Ok(dispatched)
    }
}

impl fmt::Debug for File {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("File")
            .field("fd", &self.fd())
            .field("polling_worker", &self.polling_worker)
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
    polling_worker: DataWorkerId,
    poller: Poller,
    files: Pool<File>,
}

impl FileMain {
    /// Creates the platform poller and empty File registry for one Data Worker.
    pub fn new(polling_worker: DataWorkerId) -> RuntimeResult<Self> {
        Ok(Self {
            polling_worker,
            poller: Poller::new()?,
            files: Pool::with_capacity(FILE_POOL_CAPACITY),
        })
    }

    /// Registers a File and returns its existing `hammer-infra` Pool Index.
    pub fn add(&mut self, file: File) -> RuntimeResult<Index> {
        if file.polling_worker != self.polling_worker {
            return Err(RuntimeError::invariant(format!(
                "file polling worker {:?} does not match FileMain worker {:?}",
                file.polling_worker, self.polling_worker
            )));
        }

        let index = self
            .files
            .insert(file)
            .ok_or_else(|| RuntimeError::invariant("FileMain pool is full"))?;
        let spec = self
            .files
            .get(index)
            .map(|file| file.poll_spec(index))
            .ok_or_else(|| RuntimeError::invariant("new File index did not resolve"))?;
        if let Err(error) = self.poller.add(spec) {
            self.files.remove(index);
            return Err(error);
        }
        Ok(index)
    }

    #[inline]
    /// Looks up a live File, rejecting stale generations.
    pub fn get(&self, index: Index) -> Option<&File> {
        self.files.get(index)
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
            return Err(RuntimeError::invariant(
                "File index is stale or not registered",
            ));
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
            return Err(RuntimeError::invariant(
                "File disappeared while updating write readiness",
            ));
        };
        file.write_enabled = available;
        Ok(previous)
    }

    /// Performs one nonblocking readiness poll and dispatches live callbacks.
    pub fn poll(&mut self) -> RuntimeResult<usize> {
        let mut events = [PollEvent::default(); POLL_BATCH_SIZE];
        let count = self.poller.poll(&mut events)?;
        let mut dispatched = 0;
        for event in &events[..count] {
            let Some(index) = event.index else {
                continue;
            };
            let Some(file) = self.files.get_mut(index) else {
                continue;
            };
            if event.readiness.contains(Readiness::ERROR) && file.functions.error.is_none() {
                self.delete(index)?;
                continue;
            }
            dispatched += file.dispatch(event.readiness)?;
            if event.rearm {
                let spec = self
                    .files
                    .get(index)
                    .map(|file| file.poll_spec(index))
                    .ok_or_else(|| {
                        RuntimeError::invariant("File disappeared while rearming readiness")
                    })?;
                self.poller.add(spec)?;
            }
        }
        Ok(dispatched)
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

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PollEvent {
    pub(super) index: Option<Index>,
    pub(super) readiness: Readiness,
    pub(super) rearm: bool,
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

#[inline]
pub(super) fn encode_index(index: Index) -> u64 {
    (u64::from(index.generation()) << 32) | u64::from(index.slot())
}

#[inline]
pub(super) fn decode_index(token: u64) -> Option<Index> {
    let generation = (token >> 32) as u32;
    (generation != 0).then(|| Index::new(token as u32, generation))
}
