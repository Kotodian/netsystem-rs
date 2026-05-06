use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hammer_core::error::HammerError;
use hammer_core::log::{Formatter, Level, LogWriter};
use hammer_core::metrics::{MetricCounter, MetricKind, MetricsRegistry};
use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, error::TryRecvError, error::TrySendError,
};

const LOG_QUEUE_CAPACITY: usize = 4096;
const MIN_METRICS_INTERVAL: Duration = Duration::from_secs(1);

/// Default ceiling for regular synchronous `call` round-trips. Lifecycle
/// start/close is deliberately dispatched through `call_blocking` instead:
/// those paths are synchronous and non-cancelable, so timing out the caller
/// while the control thread keeps mutating service state would be worse than
/// waiting for the lifecycle operation to finish.
pub(crate) const DEFAULT_CONTROL_CALL_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct ControlLogWriter {
    log_tx: Sender<LogRecord>,
    command_tx: UnboundedSender<ControlCommand>,
    closed: AtomicBool,
    dropped_logs: MetricCounter,
}

impl ControlLogWriter {
    fn new(
        log_tx: Sender<LogRecord>,
        command_tx: UnboundedSender<ControlCommand>,
        dropped_logs: MetricCounter,
    ) -> Arc<Self> {
        Arc::new(Self {
            log_tx,
            command_tx,
            closed: AtomicBool::new(false),
            dropped_logs,
        })
    }

    /// Acknowledge after the control loop drains log records that are queued
    /// when the flush command is handled. This is a log-drain point, not a
    /// global command barrier for records produced concurrently with flush.
    pub(crate) fn flush_timeout(&self, timeout: Duration) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        let (done_tx, done_rx) = mpsc::channel();
        if self
            .command_tx
            .send(ControlCommand::Flush(done_tx))
            .is_err()
        {
            return false;
        }
        done_rx.recv_timeout(timeout).is_ok()
    }

    pub(crate) fn shutdown_timeout(&self, timeout: Duration) -> bool {
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

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Dispatch `f` onto the control thread and block until it completes,
    /// using `DEFAULT_CONTROL_CALL_TIMEOUT` as the upper bound.
    pub(crate) fn call<R>(&self, f: impl FnOnce() -> R + Send + 'static) -> Result<R, HammerError>
    where
        R: Send + 'static,
    {
        self.call_with_timeout(DEFAULT_CONTROL_CALL_TIMEOUT, f)
    }

    /// Dispatch `f` onto the control thread and wait until the closure
    /// completes. Use this only for synchronous, non-cancelable state
    /// transitions where returning a timeout would leave background work
    /// continuing after the public API reported failure.
    pub(crate) fn call_blocking<R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, HammerError>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<Result<R, HammerError>>();
        self.command_tx
            .send(ControlCommand::Call(Box::new(move || {
                let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                    Ok(value) => Ok(value),
                    Err(_) => Err(HammerError::internal("control closure panicked")),
                };
                let _ = done_tx.send(outcome);
            })))
            .map_err(|_| HammerError::internal("control thread stopped"))?;
        done_rx
            .recv()
            .map_err(|_| HammerError::internal("control command canceled"))?
    }

    /// Like [`Self::call`] but with an explicit timeout. Returns
    /// `internal("control command timed out")` if the closure does not
    /// complete in time, and `internal("control closure panicked")` if it
    /// unwinds — the control thread itself stays alive in either case so
    /// subsequent calls can still proceed.
    pub(crate) fn call_with_timeout<R>(
        &self,
        timeout: Duration,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, HammerError>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<Result<R, HammerError>>();
        self.command_tx
            .send(ControlCommand::Call(Box::new(move || {
                let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                    Ok(value) => Ok(value),
                    Err(_) => Err(HammerError::internal("control closure panicked")),
                };
                let _ = done_tx.send(outcome);
            })))
            .map_err(|_| HammerError::internal("control thread stopped"))?;
        match done_rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err(HammerError::internal("control command timed out"))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(HammerError::internal("control command canceled"))
            }
        }
    }

    /// Dispatch an asynchronous closure onto the control thread. `f` is run
    /// synchronously on the control thread and is expected to either send
    /// the final result through the supplied `Sender` (typically from a
    /// spawned future on the worker runtime) or return `Err` to surface an
    /// early failure.
    ///
    /// `timeout` bounds the total wait on the caller side; it must cover
    /// both the synchronous portion and the time the spawned future needs
    /// to produce a value. Callers should pass `inner_timeout + buffer` so
    /// the inner async work has a chance to time out cleanly first.
    ///
    /// A panic in the synchronous portion is caught and reported as
    /// `internal("control async closure panicked")`; the control thread
    /// stays alive. A panic inside a spawned future is observed as a
    /// `Disconnected` channel and reported as `canceled`.
    pub(crate) fn call_async<R>(
        &self,
        timeout: Duration,
        f: impl FnOnce(mpsc::Sender<Result<R, HammerError>>) -> Result<(), HammerError> + Send + 'static,
    ) -> Result<R, HammerError>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<Result<R, HammerError>>();
        let dispatch_done_tx = done_tx.clone();
        self.command_tx
            .send(ControlCommand::AsyncCall(Box::new(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    f(dispatch_done_tx)
                }));
                match result {
                    Ok(Ok(())) => {
                        // f spawned the future; the spawned task owns its
                        // sender clone and will deliver the result.
                    }
                    Ok(Err(err)) => {
                        let _ = done_tx.send(Err(err));
                    }
                    Err(_) => {
                        let _ = done_tx
                            .send(Err(HammerError::internal("control async closure panicked")));
                    }
                }
            })))
            .map_err(|_| HammerError::internal("control thread stopped"))?;
        match done_rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err(HammerError::internal("control async command timed out"))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(HammerError::internal("control async command canceled"))
            }
        }
    }
}

impl LogWriter for ControlLogWriter {
    fn write_message(&self, level: Level, message: String) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        match self.log_tx.try_send(LogRecord { level, message }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => self.dropped_logs.inc(),
            Err(TrySendError::Closed(_)) => self.dropped_logs.inc(),
        }
    }
}

pub(crate) struct ControlThread {
    log_rx: Receiver<LogRecord>,
    command_rx: UnboundedReceiver<ControlCommand>,
    inner: Arc<dyn LogWriter>,
    metrics: Arc<MetricsRegistry>,
    formatter: Formatter,
    min_level: Level,
    metrics_interval: Duration,
}

impl ControlThread {
    pub(crate) fn new(
        base_time: Instant,
        inner: Arc<dyn LogWriter>,
        metrics: Arc<MetricsRegistry>,
        metrics_interval: Duration,
        min_level: Level,
    ) -> (Arc<ControlLogWriter>, Self) {
        let (log_tx, log_rx) = tokio::sync::mpsc::channel(LOG_QUEUE_CAPACITY);
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let scope = metrics.scope("runtime", "control_thread", "hammer-main");
        let writer = ControlLogWriter::new(log_tx, command_tx, scope.counter("log_dropped_total"));
        let thread = Self {
            log_rx,
            command_rx,
            inner,
            metrics,
            formatter: Formatter::new(base_time),
            min_level,
            metrics_interval: metrics_interval.max(MIN_METRICS_INTERVAL),
        };
        (writer, thread)
    }

    pub(crate) async fn run(mut self) {
        let mut metrics_tick = tokio::time::interval(self.metrics_interval);
        metrics_tick.tick().await;
        loop {
            tokio::select! {
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    if self.handle_command(command) {
                        break;
                    }
                }
                log = self.log_rx.recv() => {
                    let Some(log) = log else {
                        continue;
                    };
                    self.inner.write_message(log.level, log.message);
                    self.drain_logs();
                }
                _ = metrics_tick.tick() => {
                    dump_metrics(
                        self.inner.as_ref(),
                        &self.metrics,
                        &self.formatter,
                        self.min_level,
                    );
                }
            }
        }
    }

    fn handle_command(&mut self, command: ControlCommand) -> bool {
        match command {
            ControlCommand::Flush(done) => {
                self.drain_logs();
                let _ = done.send(());
                false
            }
            ControlCommand::Shutdown(done) => {
                self.drain_logs();
                dump_metrics(
                    self.inner.as_ref(),
                    &self.metrics,
                    &self.formatter,
                    self.min_level,
                );
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
        }
    }

    fn drain_logs(&mut self) {
        loop {
            match self.log_rx.try_recv() {
                Ok(LogRecord { level, message }) => self.inner.write_message(level, message),
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return,
            }
        }
    }
}

struct LogRecord {
    level: Level,
    message: String,
}

enum ControlCommand {
    Flush(mpsc::Sender<()>),
    Shutdown(mpsc::Sender<()>),
    Call(Box<dyn FnOnce() + Send>),
    AsyncCall(Box<dyn FnOnce() + Send>),
}

fn dump_metrics(
    inner: &dyn LogWriter,
    metrics: &MetricsRegistry,
    formatter: &Formatter,
    min_level: Level,
) {
    let samples: Vec<_> = metrics
        .snapshot()
        .into_iter()
        .filter(|sample| sample.value > 0 || sample.kind == MetricKind::Gauge)
        .collect();
    if !level_enabled(Level::Info, min_level) {
        return;
    }
    for sample in samples {
        let mut message = format!(
            "metrics module={} type={} id={} name={} kind={} value={}",
            sample.module,
            sample.component_type,
            sample.component_id,
            sample.name,
            sample.kind.as_str(),
            sample.value
        );
        for label in sample.labels {
            message.push(' ');
            message.push_str(&label.key);
            message.push('=');
            message.push_str(&label.value);
        }
        let line = formatter.format(None, Level::Info, "metrics", &message, Instant::now());
        inner.write_message(Level::Info, line);
    }
}

fn level_enabled(level: Level, min_level: Level) -> bool {
    level as i32 <= min_level as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::log::Level;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CaptureWriter {
        lines: Mutex<Vec<String>>,
    }

    impl LogWriter for CaptureWriter {
        fn write_message(&self, _level: Level, message: String) {
            self.lines.lock().unwrap().push(message);
        }
    }

    fn run_control_thread(thread: ControlThread) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("control test runtime");
            runtime.block_on(thread.run());
        })
    }

    #[test]
    fn control_writer_flushes_queued_logs() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        let (writer, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let handle = run_control_thread(thread);

        writer.write_message(Level::Info, "line\n".to_owned());
        assert!(writer.flush_timeout(Duration::from_secs(1)));
        assert!(writer.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");

        let lines = inner.lines.lock().unwrap();
        assert!(lines.iter().any(|line| line == "line\n"));
    }

    #[test]
    fn control_writer_dumps_registered_metrics() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        metrics
            .scope("outbound", "outbound", "direct")
            .counter_with_labels("dial_error_total", [("network", "tcp")])
            .inc();
        let (writer, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_millis(10),
            Level::Info,
        );
        let handle = run_control_thread(thread);

        std::thread::sleep(Duration::from_millis(40));
        assert!(writer.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");

        let lines = inner.lines.lock().unwrap();
        assert!(
            lines
                .iter()
                .any(|line| line.contains("metrics module=outbound type=outbound id=direct")),
            "lines = {lines:?}"
        );
    }

    #[test]
    fn control_writer_respects_log_level_for_metrics() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        metrics
            .scope("outbound", "outbound", "direct")
            .counter_with_labels("dial_error_total", [("network", "tcp")])
            .inc();
        let (writer, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_millis(10),
            Level::Warn,
        );
        let handle = run_control_thread(thread);

        std::thread::sleep(Duration::from_millis(40));
        assert!(writer.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");

        let lines = inner.lines.lock().unwrap();
        assert!(
            !lines
                .iter()
                .any(|line| line.contains("metrics module=outbound type=outbound id=direct")),
            "lines = {lines:?}"
        );
    }

    #[test]
    fn control_thread_survives_panicking_call() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        let (writer, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let handle = run_control_thread(thread);

        // Silence the panic backtrace produced by catch_unwind so the test
        // output stays readable; restore the previous hook on drop.
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        let panicked: Result<(), HammerError> =
            writer.call_with_timeout(Duration::from_secs(1), || panic!("boom"));
        assert!(panicked.is_err(), "panicking closure should surface error");

        // The control thread must still be alive and able to service further
        // calls — that is the whole point of catching the unwind.
        let value = writer
            .call_with_timeout(Duration::from_secs(1), || 42_u32)
            .expect("post-panic call should succeed");
        assert_eq!(value, 42);

        std::panic::set_hook(prev_hook);

        assert!(writer.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");
    }

    #[test]
    fn control_call_times_out_when_thread_blocked() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        let (writer, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let handle = run_control_thread(thread);

        // Occupy the control thread for ~300ms with a synchronous sleep, then
        // attempt a second call with a much shorter timeout. The second call
        // must return Err quickly instead of blocking until the first one
        // completes.
        let blocker_writer = Arc::clone(&writer);
        let blocker = std::thread::spawn(move || {
            let _ = blocker_writer.call_with_timeout(Duration::from_secs(2), || {
                std::thread::sleep(Duration::from_millis(300));
            });
        });
        std::thread::sleep(Duration::from_millis(50));

        let start = Instant::now();
        let result: Result<(), HammerError> =
            writer.call_with_timeout(Duration::from_millis(100), || ());
        let elapsed = start.elapsed();
        assert!(result.is_err(), "expected timeout, got Ok");
        assert!(
            elapsed < Duration::from_millis(250),
            "should time out promptly, took {elapsed:?}"
        );

        blocker.join().expect("blocker thread join");
        assert!(writer.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");
    }

    #[test]
    fn control_blocking_call_waits_for_slow_closure() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        let (writer, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let handle = run_control_thread(thread);

        let value = writer
            .call_blocking(|| {
                std::thread::sleep(Duration::from_millis(120));
                7_u32
            })
            .expect("blocking control call");
        assert_eq!(value, 7);

        assert!(writer.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");
    }
}
