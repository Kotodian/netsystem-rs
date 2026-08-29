mod timer;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use crate::error::{RuntimeError, RuntimeResult};
use crate::log::Level;

pub use self::timer::ControlTimerHandle;
use self::timer::{ControlTimerId, ControlTimerRegistration, TimerRegistry};

pub const DEFAULT_CONTROL_CALL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ControlThreadHandle {
    command_tx: tokio::sync::mpsc::UnboundedSender<ControlCommand>,
    closed: AtomicBool,
    next_timer_id: std::sync::atomic::AtomicU64,
}

impl ControlThreadHandle {
    fn new(command_tx: tokio::sync::mpsc::UnboundedSender<ControlCommand>) -> Arc<Self> {
        Arc::new(Self {
            command_tx,
            closed: AtomicBool::new(false),
            next_timer_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    pub fn shutdown_timeout(&self, timeout: Duration) -> bool {
        if self.closed.swap(true, Ordering::Relaxed) {
            return true;
        }
        let (done_tx, done_rx) = mpsc::channel();
        if self
            .command_tx
            .send(ControlCommand::Shutdown(done_tx))
            .is_err()
        {
            return false;
        }
        done_rx.recv_timeout(timeout).is_ok()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn schedule_once<F, Fut>(
        &self,
        delay: Duration,
        callback: F,
    ) -> RuntimeResult<ControlTimerHandle>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule_timer(ControlTimerRegistration::once(
            self.next_timer_id(),
            delay,
            boxed_timer_callback(callback),
        ))
    }

    pub fn schedule_interval<F, Fut>(
        &self,
        initial_delay: Duration,
        interval: Duration,
        callback: F,
    ) -> RuntimeResult<ControlTimerHandle>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if interval.is_zero() {
            return Err(RuntimeError::ControlTimerIntervalZero);
        }
        self.schedule_timer(ControlTimerRegistration::interval(
            self.next_timer_id(),
            initial_delay,
            interval,
            boxed_timer_callback(callback),
        ))
    }

    fn next_timer_id(&self) -> ControlTimerId {
        ControlTimerId(
            self.next_timer_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    fn schedule_timer(
        &self,
        registration: ControlTimerRegistration,
    ) -> RuntimeResult<ControlTimerHandle> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(RuntimeError::service_closed());
        }
        let id = registration.id();
        self.command_tx
            .send(ControlCommand::RegisterTimer(registration))
            .map_err(|_| RuntimeError::ControlThreadStopped)?;
        Ok(ControlTimerHandle::new(id, self.command_tx.clone()))
    }

    pub fn call<R>(&self, f: impl FnOnce() -> R + Send + 'static) -> RuntimeResult<R>
    where
        R: Send + 'static,
    {
        self.call_with_timeout(DEFAULT_CONTROL_CALL_TIMEOUT, f)
    }

    pub fn call_blocking<R>(&self, f: impl FnOnce() -> R + Send + 'static) -> RuntimeResult<R>
    where
        R: Send + 'static,
    {
        self.call_inner(f, false)
    }

    fn call_inner<R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
        _with_timeout: bool,
    ) -> RuntimeResult<R>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<RuntimeResult<R>>();
        self.command_tx
            .send(ControlCommand::Call(Box::new(move || {
                let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                    Ok(value) => Ok(value),
                    Err(_) => Err(RuntimeError::ControlCommandPanicked),
                };
                let _ = done_tx.send(outcome);
            })))
            .map_err(|_| RuntimeError::ControlThreadStopped)?;
        done_rx
            .recv()
            .map_err(|_| RuntimeError::ControlCommandCanceled)?
    }

    pub fn call_with_timeout<R>(
        &self,
        timeout: Duration,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> RuntimeResult<R>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<RuntimeResult<R>>();
        self.command_tx
            .send(ControlCommand::Call(Box::new(move || {
                let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                    Ok(value) => Ok(value),
                    Err(_) => Err(RuntimeError::ControlCommandPanicked),
                };
                let _ = done_tx.send(outcome);
            })))
            .map_err(|_| RuntimeError::ControlThreadStopped)?;
        match done_rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(RuntimeError::ControlCommandTimedOut),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RuntimeError::ControlCommandCanceled),
        }
    }

    pub fn call_async<R>(
        &self,
        timeout: Duration,
        f: impl FnOnce(mpsc::Sender<RuntimeResult<R>>) -> RuntimeResult<()> + Send + 'static,
    ) -> RuntimeResult<R>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<RuntimeResult<R>>();
        let dispatch_done_tx = done_tx.clone();
        self.command_tx
            .send(ControlCommand::AsyncCall(Box::new(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    f(dispatch_done_tx)
                }));
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        let _ = done_tx.send(Err(err));
                    }
                    Err(_) => {
                        let _ = done_tx.send(Err(RuntimeError::ControlCommandPanicked));
                    }
                }
            })))
            .map_err(|_| RuntimeError::ControlThreadStopped)?;
        match done_rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(RuntimeError::ControlCommandTimedOut),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RuntimeError::ControlCommandCanceled),
        }
    }
}

pub struct ControlThread {
    command_rx: tokio::sync::mpsc::UnboundedReceiver<ControlCommand>,
    command_tx: tokio::sync::mpsc::UnboundedSender<ControlCommand>,
    timers: TimerRegistry,
}

impl ControlThread {
    pub fn new(
        base_time: std::time::Instant,
        min_level: Level,
    ) -> (Arc<ControlThreadHandle>, Self) {
        let _ = (base_time, min_level);
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = ControlThreadHandle::new(command_tx.clone());
        let thread = Self {
            command_rx,
            command_tx,
            timers: TimerRegistry::new(),
        };
        (handle, thread)
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        self.timers.shutdown();
                        break;
                    };
                    if self.handle_command(command) {
                        break;
                    }
                }
            }
        }
    }

    fn handle_command(&mut self, command: ControlCommand) -> bool {
        match command {
            ControlCommand::Shutdown(done) => {
                self.timers.shutdown();
                let _ = done.send(());
                true
            }
            ControlCommand::Call(call) => {
                call();
                false
            }
            ControlCommand::AsyncCall(call) => {
                call();
                false
            }
            ControlCommand::RegisterTimer(registration) => {
                self.timers.register(registration, self.command_tx.clone());
                false
            }
            ControlCommand::CancelTimer(id, done) => {
                let canceled = self.timers.cancel(id);
                let _ = done.send(canceled);
                false
            }
            ControlCommand::TimerFinished(id) => {
                self.timers.finish(id);
                false
            }
        }
    }
}

enum ControlCommand {
    Shutdown(mpsc::Sender<()>),
    Call(Box<dyn FnOnce() + Send>),
    AsyncCall(Box<dyn FnOnce() + Send>),
    RegisterTimer(ControlTimerRegistration),
    CancelTimer(ControlTimerId, mpsc::Sender<bool>),
    TimerFinished(ControlTimerId),
}

fn boxed_timer_callback<F, Fut>(mut callback: F) -> timer::TimerCallback
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move || Box::pin(callback()))
}
