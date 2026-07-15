//! VPP-style cooperative Process Nodes owned by the main thread.
//!
//! Process Nodes do not consume packet frames and never run in a Data Worker
//! graph. A main-thread [`tokio::task::LocalSet`] polls their futures; waiting
//! for a clock or event suspends only that process. Shutdown aborts and joins
//! every future before plugin code may be unloaded.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::thread::{self, ThreadId};
use std::time::Duration;

use hammer_core::error::{HammerError, HammerResult};
use hammer_core::registry::RuntimeRegistry;
use hammer_infra::vec::Vec;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, LocalSet};

use crate::DataPlaneRuntime;

pub type ProcessFuture = Pin<Box<dyn Future<Output = HammerResult<()>> + 'static>>;

#[derive(Clone, Copy)]
pub struct ProcessEntry {
    pub name: &'static str,
    pub start: fn(ProcessContext) -> ProcessFuture,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessEventBatch {
    event_type: u64,
    data: Vec<u64>,
}

impl ProcessEventBatch {
    #[inline]
    pub fn event_type(&self) -> u64 {
        self.event_type
    }

    #[inline]
    pub fn data(&self) -> &[u64] {
        &self.data
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessWake {
    Clock,
    Event(ProcessEventBatch),
}

impl ProcessWake {
    #[inline]
    pub fn event_type(&self) -> Option<u64> {
        match self {
            Self::Clock => None,
            Self::Event(event) => Some(event.event_type()),
        }
    }

    #[inline]
    pub fn event_data(&self) -> &[u64] {
        match self {
            Self::Clock => &[],
            Self::Event(event) => event.data(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProcessSignal {
    event_type: u64,
    data: u64,
}

pub struct ProcessContext {
    name: &'static str,
    registry: Arc<RuntimeRegistry>,
    runtime: DataPlaneRuntime,
    events: mpsc::UnboundedReceiver<ProcessSignal>,
    pending: Vec<ProcessEventBatch>,
}

impl ProcessContext {
    fn new(
        name: &'static str,
        registry: Arc<RuntimeRegistry>,
        runtime: DataPlaneRuntime,
        events: mpsc::UnboundedReceiver<ProcessSignal>,
    ) -> Self {
        Self {
            name,
            registry,
            runtime,
            events,
            pending: Vec::new(),
        }
    }

    #[inline]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[inline]
    pub fn require<T>(&self) -> HammerResult<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.registry.require::<T>()
    }

    /// Main-thread data-plane view, equivalent to the process node's
    /// `vlib_main_t`. It is never a Data Worker runtime.
    #[inline]
    pub fn data_plane_runtime(&self) -> &DataPlaneRuntime {
        &self.runtime
    }

    /// Suspend until a signal arrives or `duration` elapses.
    ///
    /// Signals of the same type are returned as one batch, matching VPP's
    /// per-event-type pending vectors. Other types remain pending for the next
    /// call.
    pub async fn wait_for_event_or_clock(&mut self, duration: Duration) -> ProcessWake {
        if !self.pending.is_empty() {
            return ProcessWake::Event(self.pending.remove(0));
        }

        let first = tokio::select! {
            biased;
            signal = self.events.recv() => signal,
            () = tokio::time::sleep(duration) => return ProcessWake::Clock,
        };
        let Some(first) = first else {
            return ProcessWake::Clock;
        };
        self.push_signal(first);
        while let Ok(signal) = self.events.try_recv() {
            self.push_signal(signal);
        }
        ProcessWake::Event(self.pending.remove(0))
    }

    fn push_signal(&mut self, signal: ProcessSignal) {
        if let Some(batch) = self
            .pending
            .iter_mut()
            .find(|batch| batch.event_type == signal.event_type)
        {
            batch.data.push(signal.data);
            return;
        }
        self.pending.push(ProcessEventBatch {
            event_type: signal.event_type,
            data: hammer_infra::vec![signal.data],
        });
    }
}

#[derive(Clone)]
pub struct ProcessHandle {
    name: &'static str,
    events: mpsc::UnboundedSender<ProcessSignal>,
}

impl ProcessHandle {
    #[inline]
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn signal(&self, event_type: u64, data: u64) -> HammerResult<()> {
        self.events
            .send(ProcessSignal { event_type, data })
            .map_err(|_| HammerError::service_closed())
    }
}

struct RunningProcess {
    handle: ProcessHandle,
    task: JoinHandle<HammerResult<()>>,
}

pub(crate) struct ProcessMain {
    owner: ThreadId,
    local: LocalSet,
    running: Vec<RunningProcess>,
}

impl ProcessMain {
    pub(crate) fn new() -> Self {
        Self {
            owner: thread::current().id(),
            local: LocalSet::new(),
            running: Vec::new(),
        }
    }

    fn ensure_owner(&self) -> HammerResult<()> {
        if thread::current().id() == self.owner {
            Ok(())
        } else {
            Err(HammerError::internal(
                "Process Nodes must be controlled by their main thread",
            ))
        }
    }

    pub(crate) fn start(
        &mut self,
        registry: Arc<RuntimeRegistry>,
        runtime: DataPlaneRuntime,
    ) -> HammerResult<()> {
        self.ensure_owner()?;
        if !self.running.is_empty() {
            return Err(HammerError::internal("Process Nodes are already started"));
        }
        let entries = crate::registration::process_nodes();
        let mut names = Vec::with_capacity(entries.len());
        for entry in &entries {
            if names.contains(&entry.name) {
                return Err(HammerError::internal(format!(
                    "duplicate Process Node `{}`",
                    entry.name
                )));
            }
            names.push(entry.name);
        }
        let mut prepared = Vec::with_capacity(entries.len());
        for entry in entries {
            let (events, receiver) = mpsc::unbounded_channel();
            let context =
                ProcessContext::new(entry.name, Arc::clone(&registry), runtime.clone(), receiver);
            let future =
                catch_unwind(AssertUnwindSafe(|| (entry.start)(context))).map_err(|_| {
                    HammerError::internal(format!(
                        "Process Node `{}` panicked during start",
                        entry.name
                    ))
                })?;
            prepared.push((entry, events, future));
        }
        for (entry, events, future) in prepared {
            let task = self.local.spawn_local(future);
            self.running.push(RunningProcess {
                handle: ProcessHandle {
                    name: entry.name,
                    events,
                },
                task,
            });
        }
        Ok(())
    }

    pub(crate) fn handle(&self, name: &str) -> Option<ProcessHandle> {
        self.running
            .iter()
            .find(|running| running.handle.name == name)
            .map(|running| running.handle.clone())
    }

    pub(crate) fn run_until<F>(&self, runtime: &tokio::runtime::Runtime, future: F) -> F::Output
    where
        F: Future,
    {
        self.local.block_on(runtime, future)
    }

    pub(crate) fn shutdown(&mut self, runtime: &tokio::runtime::Runtime) -> HammerResult<()> {
        self.ensure_owner()?;
        let mut running = core::mem::take(&mut self.running);
        for process in &running {
            process.task.abort();
        }
        self.local.block_on(runtime, async move {
            for process in running.drain(..) {
                match process.task.await {
                    Ok(Ok(())) => {}
                    Err(error) if error.is_cancelled() => {}
                    Ok(Err(error)) => {
                        tracing::error!(process = process.handle.name, %error, "Process Node failed");
                    }
                    Err(error) => {
                        tracing::error!(process = process.handle.name, %error, "Process Node panicked");
                    }
                }
            }
        });
        Ok(())
    }
}
