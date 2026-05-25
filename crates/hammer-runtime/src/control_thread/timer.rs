use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::mpsc;
use std::time::Duration;

use hammer_core::log::{Level, LogWriter};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;

use super::ControlCommand;

pub(crate) type TimerFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(crate) type TimerCallback = Box<dyn FnMut() -> TimerFuture + Send + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ControlTimerId(pub(crate) u64);

pub(crate) struct ControlTimerHandle {
    id: ControlTimerId,
    command_tx: UnboundedSender<ControlCommand>,
}

impl ControlTimerHandle {
    pub(super) fn new(id: ControlTimerId, command_tx: UnboundedSender<ControlCommand>) -> Self {
        Self { id, command_tx }
    }

    pub(crate) fn cancel_timeout(&self, timeout: Duration) -> bool {
        let (done_tx, done_rx) = mpsc::channel();
        if self
            .command_tx
            .send(ControlCommand::CancelTimer(self.id, done_tx))
            .is_err()
        {
            return false;
        }
        done_rx.recv_timeout(timeout).unwrap_or(false)
    }
}

pub(crate) struct ControlTimerRegistration {
    id: ControlTimerId,
    initial_delay: Duration,
    schedule: TimerSchedule,
    callback: TimerCallback,
}

impl ControlTimerRegistration {
    pub(crate) fn once(
        id: ControlTimerId,
        delay: Duration,
        callback: TimerCallback,
    ) -> Self {
        Self {
            id,
            initial_delay: delay,
            schedule: TimerSchedule::Once,
            callback,
        }
    }

    pub(crate) fn interval(
        id: ControlTimerId,
        initial_delay: Duration,
        interval: Duration,
        callback: TimerCallback,
    ) -> Self {
        Self {
            id,
            initial_delay,
            schedule: TimerSchedule::Interval { interval },
            callback,
        }
    }

    pub(crate) fn id(&self) -> ControlTimerId {
        self.id
    }
}

pub(crate) struct TimerRegistry {
    entries: Vec<TimerEntry>,
}

impl TimerRegistry {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub(crate) fn register(&mut self, registration: ControlTimerRegistration) {
        let next_deadline = TokioInstant::now() + registration.initial_delay;
        self.entries.push(TimerEntry {
            id: registration.id,
            next_deadline: Some(next_deadline),
            schedule: registration.schedule,
            callback: Some(registration.callback),
            running: None,
        });
    }

    pub(crate) fn cancel(&mut self, id: ControlTimerId) -> bool {
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

    pub(crate) fn next_deadline(&self) -> Option<TokioInstant> {
        self.entries
            .iter()
            .filter_map(|entry| entry.next_deadline)
            .min()
    }

    pub(crate) fn reap_finished(&mut self) {
        for entry in &mut self.entries {
            if entry.running.as_ref().is_some_and(JoinHandle::is_finished) {
                entry.running = None;
            }
        }
        self.entries.retain(|entry| !entry.is_complete());
    }

    pub(crate) fn fire_due(&mut self, log: &dyn LogWriter) {
        self.reap_finished();
        let now = TokioInstant::now();
        for entry in &mut self.entries {
            let Some(deadline) = entry.next_deadline else {
                continue;
            };
            if deadline > now {
                continue;
            }
            entry.advance_deadline(now);
            if entry.running.is_some() {
                continue;
            }
            let Some(callback) = entry.callback.as_mut() else {
                continue;
            };
            let future = match std::panic::catch_unwind(AssertUnwindSafe(callback)) {
                Ok(future) => future,
                Err(_) => {
                    log.write_message(
                        Level::Error,
                        format!("control timer {:?} callback panicked", entry.id),
                    );
                    entry.callback = None;
                    entry.next_deadline = None;
                    continue;
                }
            };
            entry.running = Some(tokio::spawn(future));
            if matches!(entry.schedule, TimerSchedule::Once) {
                entry.callback = None;
            }
        }
        self.reap_finished();
    }
}

struct TimerEntry {
    id: ControlTimerId,
    next_deadline: Option<TokioInstant>,
    schedule: TimerSchedule,
    callback: Option<TimerCallback>,
    running: Option<JoinHandle<()>>,
}

impl TimerEntry {
    fn advance_deadline(&mut self, now: TokioInstant) {
        match self.schedule {
            TimerSchedule::Once => {
                self.next_deadline = None;
            }
            TimerSchedule::Interval { interval } => {
                self.next_deadline = Some(now + interval);
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.next_deadline.is_none() && self.callback.is_none() && self.running.is_none()
    }
}

#[derive(Clone, Copy)]
enum TimerSchedule {
    Once,
    Interval { interval: Duration },
}
