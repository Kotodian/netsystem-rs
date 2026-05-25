use std::any::{Any, TypeId};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use hammer_core::log::Level;
use hammer_core::metrics::MetricCounter;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use super::ControlCommand;

pub(crate) type EventFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(crate) type EventCallback = Box<dyn FnMut(ControlEvent) -> EventFuture + Send + 'static>;

pub trait ControlEventArgs: fmt::Debug + Send + Sync + 'static {}

impl<T> ControlEventArgs for T where T: fmt::Debug + Send + Sync + 'static {}

#[derive(Clone)]
pub struct ControlEvent {
    args: Arc<dyn Any + Send + Sync>,
    type_id: TypeId,
    type_name: &'static str,
}

impl ControlEvent {
    pub fn new<T>(args: T) -> Self
    where
        T: ControlEventArgs,
    {
        Self {
            args: Arc::new(args),
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
        }
    }

    pub fn args<T>(&self) -> Option<&T>
    where
        T: ControlEventArgs,
    {
        self.args.downcast_ref::<T>()
    }
}

impl fmt::Debug for ControlEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlEvent")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct LogEventArgs {
    pub level: Level,
    pub message: Arc<str>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct SyntheticEventArgs {
    pub id: Arc<str>,
    pub value: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum ControlEventFilter {
    All,
    EventArgs(TypeId),
    Predicate(fn(&ControlEvent) -> bool),
}

impl ControlEventFilter {
    pub fn event<T>() -> Self
    where
        T: ControlEventArgs,
    {
        Self::EventArgs(TypeId::of::<T>())
    }

    fn matches(self, event: &ControlEvent) -> bool {
        match self {
            Self::All => true,
            Self::EventArgs(type_id) => event.type_id == type_id,
            Self::Predicate(predicate) => predicate(event),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ControlEventSubscriptionId(pub(crate) u64);

pub struct ControlEventSubscriptionHandle {
    id: ControlEventSubscriptionId,
    command_tx: UnboundedSender<ControlCommand>,
}

impl ControlEventSubscriptionHandle {
    pub(super) fn new(
        id: ControlEventSubscriptionId,
        command_tx: UnboundedSender<ControlCommand>,
    ) -> Self {
        Self { id, command_tx }
    }

    pub fn cancel(&self) -> bool {
        let (done_tx, _done_rx) = mpsc::channel();
        self.command_tx
            .send(ControlCommand::CancelEventSubscription(self.id, done_tx))
            .is_ok()
    }

    pub fn cancel_timeout(&self, timeout: Duration) -> bool {
        let (done_tx, done_rx) = mpsc::channel();
        if self
            .command_tx
            .send(ControlCommand::CancelEventSubscription(self.id, done_tx))
            .is_err()
        {
            return false;
        }
        done_rx.recv_timeout(timeout).unwrap_or(false)
    }
}

impl Drop for ControlEventSubscriptionHandle {
    fn drop(&mut self) {
        let (done_tx, _done_rx) = mpsc::channel();
        let _ = self
            .command_tx
            .send(ControlCommand::CancelEventSubscription(self.id, done_tx));
    }
}

pub(crate) struct EventSubscriberRegistration {
    id: ControlEventSubscriptionId,
    filter: ControlEventFilter,
    callback: EventCallback,
}

impl EventSubscriberRegistration {
    pub(crate) fn new(
        id: ControlEventSubscriptionId,
        filter: ControlEventFilter,
        callback: EventCallback,
    ) -> Self {
        Self {
            id,
            filter,
            callback,
        }
    }
}

pub(crate) struct EventRegistry {
    entries: Vec<EventSubscriberEntry>,
    dropped_busy_total: MetricCounter,
    callback_panic_total: MetricCounter,
}

impl EventRegistry {
    pub(crate) fn new(
        dropped_busy_total: MetricCounter,
        callback_panic_total: MetricCounter,
    ) -> Self {
        Self {
            entries: Vec::new(),
            dropped_busy_total,
            callback_panic_total,
        }
    }

    pub(crate) fn register(&mut self, registration: EventSubscriberRegistration) {
        self.entries.push(EventSubscriberEntry {
            id: registration.id,
            filter: registration.filter,
            callback: Some(registration.callback),
            running: None,
        });
    }

    pub(crate) fn cancel(&mut self, id: ControlEventSubscriptionId) -> bool {
        let Some(index) = self.entries.iter().position(|entry| entry.id == id) else {
            return false;
        };
        if let Some(handle) = self.entries[index].running.take() {
            handle.abort();
        }
        self.entries.swap_remove(index);
        true
    }

    pub(crate) fn shutdown(&mut self) {
        for entry in self.entries.drain(..) {
            if let Some(handle) = entry.running {
                handle.abort();
            }
        }
    }

    pub(crate) fn reap_finished(&mut self) {
        for entry in &mut self.entries {
            if entry.running.as_ref().is_some_and(JoinHandle::is_finished) {
                let handle = entry.running.take().expect("checked running handle");
                let callback_panic_total = self.callback_panic_total.clone();
                tokio::spawn(async move {
                    if handle.await.is_err() {
                        callback_panic_total.inc();
                    }
                });
            }
        }
        self.entries
            .retain(|entry| entry.callback.is_some() || entry.running.is_some());
    }

    pub(crate) fn dispatch(&mut self, event: ControlEvent) {
        self.reap_finished();
        for entry in &mut self.entries {
            if !entry.filter.matches(&event) {
                continue;
            }
            if entry.running.is_some() {
                self.dropped_busy_total.inc();
                continue;
            }
            let Some(callback) = entry.callback.as_mut() else {
                continue;
            };
            let event = event.clone();
            let future = match std::panic::catch_unwind(AssertUnwindSafe(|| callback(event))) {
                Ok(future) => future,
                Err(_) => {
                    self.callback_panic_total.inc();
                    entry.callback = None;
                    continue;
                }
            };
            entry.running = Some(tokio::spawn(future));
        }
        self.reap_finished();
    }
}

struct EventSubscriberEntry {
    id: ControlEventSubscriptionId,
    filter: ControlEventFilter,
    callback: Option<EventCallback>,
    running: Option<JoinHandle<()>>,
}

pub(crate) fn boxed_event_callback<F, Fut>(mut callback: F) -> EventCallback
where
    F: FnMut(ControlEvent) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move |event| Box::pin(callback(event)))
}
