use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};

use super::ControlCommand;

pub(crate) type TimerFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(crate) type TimerCallback = Box<dyn FnMut() -> TimerFuture + Send + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ControlTimerId(pub(crate) u64);

pub struct ControlTimerHandle {
    id: ControlTimerId,
    command_tx: UnboundedSender<ControlCommand>,
}

impl ControlTimerHandle {
    pub(super) fn new(id: ControlTimerId, command_tx: UnboundedSender<ControlCommand>) -> Self {
        Self { id, command_tx }
    }

    pub fn cancel(&self) -> bool {
        let (done_tx, _done_rx) = mpsc::channel();
        self.command_tx
            .send(ControlCommand::CancelTimer(self.id, done_tx))
            .is_ok()
    }

    pub fn cancel_timeout(&self, timeout: Duration) -> bool {
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
    pub(crate) fn once(id: ControlTimerId, delay: Duration, callback: TimerCallback) -> Self {
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
    entries: HashMap<ControlTimerId, TimerEntry>,
}

impl TimerRegistry {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub(crate) fn register(
        &mut self,
        registration: ControlTimerRegistration,
        command_tx: UnboundedSender<ControlCommand>,
    ) {
        let id = registration.id;
        if let Some(previous) = self.entries.remove(&id) {
            previous.task.abort();
        }
        let task = spawn_timer_task(registration, command_tx);
        self.entries.insert(id, TimerEntry { task });
    }

    pub(crate) fn cancel(&mut self, id: ControlTimerId) -> bool {
        let Some(entry) = self.entries.remove(&id) else {
            return false;
        };
        entry.task.abort();
        true
    }

    pub(crate) fn finish(&mut self, id: ControlTimerId) {
        self.entries.remove(&id);
    }

    pub(crate) fn shutdown(&mut self) {
        for (_, entry) in self.entries.drain() {
            entry.task.abort();
        }
    }
}

struct TimerEntry {
    task: JoinHandle<()>,
}

#[derive(Clone, Copy)]
enum TimerSchedule {
    Once,
    Interval { interval: Duration },
}

fn spawn_timer_task(
    registration: ControlTimerRegistration,
    command_tx: UnboundedSender<ControlCommand>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let ControlTimerRegistration {
            id,
            initial_delay,
            schedule,
            mut callback,
        } = registration;
        match schedule {
            TimerSchedule::Once => {
                if !initial_delay.is_zero() {
                    tokio::time::sleep(initial_delay).await;
                }
                let _ = run_timer_callback(id, &mut callback).await;
                let _ = command_tx.send(ControlCommand::TimerFinished(id));
            }
            TimerSchedule::Interval { interval } => {
                let start = TokioInstant::now() + initial_delay;
                let mut ticker = tokio::time::interval_at(start, interval);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    if !run_timer_callback(id, &mut callback).await {
                        break;
                    }
                }
                let _ = command_tx.send(ControlCommand::TimerFinished(id));
            }
        }
    })
}

async fn run_timer_callback(id: ControlTimerId, callback: &mut TimerCallback) -> bool {
    let future = match std::panic::catch_unwind(AssertUnwindSafe(|| callback())) {
        Ok(future) => future,
        Err(_) => {
            tracing::error!("control timer {id:?} callback panicked (catch_unwind)");
            return false;
        }
    };
    PanicLoggedFuture { id, future }.await
}

struct PanicLoggedFuture {
    id: ControlTimerId,
    future: TimerFuture,
}

impl Future for PanicLoggedFuture {
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match std::panic::catch_unwind(AssertUnwindSafe(|| self.future.as_mut().poll(cx))) {
            Ok(Poll::Ready(())) => Poll::Ready(true),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => {
                tracing::error!("control timer {id:?} callback panicked", id = self.id);
                Poll::Ready(false)
            }
        }
    }
}
