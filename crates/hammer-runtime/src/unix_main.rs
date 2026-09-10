use std::collections::VecDeque;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::log::Level;

const ERROR_HISTORY_CAPACITY: usize = 128;

/// Unix host-process state and lifecycle policy.
///
/// The daemon owns this value on thread zero and passes ordinary borrows into
/// startup and shutdown operations. It does not retain a DataPlaneMain lookup.
pub struct UnixMain {
    flags: u32,
    error_history: VecDeque<(Instant, RuntimeError)>,
    total_errors: u64,
    startup_path: PathBuf,
    runtime_path: Option<PathBuf>,
    pid_path: Option<PathBuf>,
    log_path: Option<PathBuf>,
    log_fd: Option<OwnedFd>,
    log_level: Level,
    poll_sleep: Duration,
    exit_status: Option<i32>,
}

impl UnixMain {
    pub fn new(startup_path: PathBuf, log_level: Level) -> Self {
        Self {
            flags: 0,
            error_history: VecDeque::with_capacity(ERROR_HISTORY_CAPACITY),
            total_errors: 0,
            startup_path,
            runtime_path: None,
            pid_path: None,
            log_path: None,
            log_fd: None,
            log_level,
            poll_sleep: Duration::ZERO,
            exit_status: None,
        }
    }

    #[inline]
    pub fn startup_path(&self) -> &Path {
        &self.startup_path
    }

    #[inline]
    pub fn log_level(&self) -> Level {
        self.log_level
    }

    pub fn startup_document(&self) -> RuntimeResult<String> {
        std::fs::read_to_string(&self.startup_path).map_err(|source| {
            RuntimeError::StartupConfigRead {
                path: self.startup_path.clone(),
                source,
            }
        })
    }

    pub(crate) fn record_error(&mut self, error: RuntimeError) {
        if self.error_history.len() == ERROR_HISTORY_CAPACITY {
            self.error_history.pop_front();
        }
        self.error_history.push_back((Instant::now(), error));
        self.total_errors = self.total_errors.saturating_add(1);
    }

    #[inline]
    pub(crate) fn request_exit(&mut self, status: i32) {
        self.exit_status = Some(status);
    }

    #[inline]
    pub fn exit_status(&self) -> Option<i32> {
        self.exit_status
    }

    pub async fn wait_for_exit_signal(&mut self) -> RuntimeResult<i32> {
        let mut terminate = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        )
        .map_err(|source| RuntimeError::UnixSignal {
            signal: "SIGTERM",
            source,
        })?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|source| RuntimeError::UnixSignal {
                    signal: "SIGINT",
                    source,
                })?;
            }
            _ = terminate.recv() => {}
        }
        self.request_exit(0);
        Ok(0)
    }

    pub fn shutdown(&mut self, status: i32) -> RuntimeResult<()> {
        self.request_exit(status);
        self.log_fd = None;
        Ok(())
    }
}
