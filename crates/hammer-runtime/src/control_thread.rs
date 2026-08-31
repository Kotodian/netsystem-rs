use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::thread::{self, ThreadId};
use std::time::Instant;

use crate::RuntimeRegistry;
use crate::error::{RuntimeError, RuntimeResult};
use crate::log::Level;
use crate::process::{ProcessContext, ProcessEntry, ProcessFuture, ProcessHandle};
use tokio::task::{JoinHandle, LocalSet};

struct RunningProcess {
    handle: ProcessHandle,
    task: JoinHandle<RuntimeResult<()>>,
}

/// Main OS-thread Tokio scheduler owner.
pub struct ControlThread {
    owner: ThreadId,
    runtime: tokio::runtime::Runtime,
    local: LocalSet,
    running: Vec<RunningProcess>,
    started: bool,
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
            local: LocalSet::new(),
            running: Vec::new(),
            started: false,
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

    pub(crate) fn is_started(&self) -> bool {
        self.started
    }

    fn ensure_owner(&self) -> RuntimeResult<()> {
        (thread::current().id() == self.owner)
            .then_some(())
            .ok_or(RuntimeError::ProcessControlWrongThread)
    }

    pub(crate) fn start_processes(
        &mut self,
        registry: Arc<RuntimeRegistry>,
        entries: Vec<ProcessEntry>,
    ) -> RuntimeResult<()> {
        self.ensure_owner()?;
        let mut names = Vec::with_capacity(entries.len());
        for entry in &entries {
            if names.contains(&entry.name) {
                return Err(RuntimeError::DuplicateProcessNode { name: entry.name });
            }
            names.push(entry.name);
        }
        let mut prepared: Vec<(ProcessEntry, _, ProcessFuture)> = Vec::with_capacity(entries.len());
        for entry in entries {
            if self
                .running
                .iter()
                .any(|running| running.handle.name() == entry.name)
            {
                continue;
            }
            let (events, receiver) = tokio::sync::mpsc::unbounded_channel();
            let context = ProcessContext::new(entry.name, Arc::clone(&registry), receiver);
            let future = match catch_unwind(AssertUnwindSafe(|| (entry.start)(context))) {
                Ok(future) => future,
                Err(payload) => std::panic::resume_unwind(payload),
            };
            prepared.push((entry, events, future));
        }
        for (entry, events, future) in prepared {
            let task = self.local.spawn_local(future);
            self.running.push(RunningProcess {
                handle: ProcessHandle::new(entry.name, events),
                task,
            });
        }
        self.started = true;
        Ok(())
    }

    pub(crate) fn process_handle(&self, name: &str) -> Option<ProcessHandle> {
        self.running
            .iter()
            .find(|running| running.handle.name() == name)
            .map(|running| running.handle.clone())
    }

    pub(crate) fn run_processes_until<'a, F>(
        &'a self,
        future: F,
    ) -> impl Future<Output = F::Output> + 'a
    where
        F: Future + 'a,
    {
        self.local.run_until(future)
    }

    pub(crate) fn shutdown_processes(&mut self) -> RuntimeResult<()> {
        self.ensure_owner()?;
        let mut running = core::mem::take(&mut self.running);
        for process in &running {
            process.task.abort();
        }
        self.local.block_on(&self.runtime, async move {
            for process in running.drain(..) {
                match process.task.await {
                    Ok(Ok(())) => {}
                    Err(error) if error.is_cancelled() => {}
                    Ok(Err(error)) => tracing::error!(process = process.handle.name(), %error, "Process Node failed"),
                    Err(error) => tracing::error!(process = process.handle.name(), %error, "Process Node panicked"),
                }
            }
        });
        self.started = false;
        Ok(())
    }
}
