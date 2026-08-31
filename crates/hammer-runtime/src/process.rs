//! VPP-style cooperative Process Nodes owned by the main thread.
//!
//! Process Nodes do not consume packet frames and never run in a Data Worker
//! graph. A main-thread [`tokio::task::LocalSet`] polls their futures; waiting
//! for a clock or event suspends only that process. Shutdown aborts and joins
//! every future before plugin authority teardown; active plugin images remain
//! mapped for the process lifetime.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::RuntimeRegistry;
use crate::error::{RuntimeError, RuntimeResult};
use tokio::sync::mpsc;

pub type ProcessFuture = Pin<Box<dyn Future<Output = RuntimeResult<()>> + 'static>>;

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
pub(crate) struct ProcessSignal {
    event_type: u64,
    data: u64,
}

pub struct ProcessContext {
    name: &'static str,
    registry: Arc<RuntimeRegistry>,
    events: mpsc::UnboundedReceiver<ProcessSignal>,
    pending: Vec<ProcessEventBatch>,
}

impl ProcessContext {
    pub(crate) fn new(
        name: &'static str,
        registry: Arc<RuntimeRegistry>,
        events: mpsc::UnboundedReceiver<ProcessSignal>,
    ) -> Self {
        Self {
            name,
            registry,
            events,
            pending: Vec::new(),
        }
    }

    #[inline]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[inline]
    pub fn require<T>(&self) -> RuntimeResult<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.registry.require::<T>()
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
        self.collect_signals(first)
    }

    /// Suspend until the next FileMain readiness signal, matching VPP's
    /// event-driven Process Node loop without a periodic clock wake.
    pub async fn wait_for_event(&mut self) -> ProcessWake {
        if !self.pending.is_empty() {
            return ProcessWake::Event(self.pending.remove(0));
        }
        let Some(first) = self.events.recv().await else {
            return ProcessWake::Clock;
        };
        self.collect_signals(first)
    }

    fn collect_signals(&mut self, first: ProcessSignal) -> ProcessWake {
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
            data: vec![signal.data],
        });
    }
}

#[derive(Clone)]
pub struct ProcessHandle {
    name: &'static str,
    events: mpsc::UnboundedSender<ProcessSignal>,
}

impl ProcessHandle {
    pub(crate) fn new(name: &'static str, events: mpsc::UnboundedSender<ProcessSignal>) -> Self {
        Self { name, events }
    }

    #[inline]
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn signal(&self, event_type: u64, data: u64) -> RuntimeResult<()> {
        self.events
            .send(ProcessSignal { event_type, data })
            .map_err(|_| RuntimeError::service_closed())
    }
}
