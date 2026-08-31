use std::future::Future;
use std::thread::{self, ThreadId};
use std::time::Instant;

use crate::error::{RuntimeError, RuntimeResult};
use crate::log::Level;

/// Main OS-thread Tokio scheduler owner.
pub struct ControlThread {
    owner: ThreadId,
    runtime: tokio::runtime::Runtime,
}

impl ControlThread {
    pub fn new(_base_time: Instant, _min_level: Level) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build main ControlThread runtime");
        Self {
            owner: thread::current().id(),
            runtime,
        }
    }

    #[inline]
    pub fn runtime(&self) -> &tokio::runtime::Runtime {
        &self.runtime
    }

    pub fn run<F>(&self, future: F) -> RuntimeResult<F::Output>
    where
        F: Future,
    {
        if thread::current().id() != self.owner {
            return Err(RuntimeError::ProcessControlWrongThread);
        }
        Ok(self.runtime.block_on(future))
    }
}
