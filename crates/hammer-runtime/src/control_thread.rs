use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hammer_core::error::{HammerError, HammerResult};
use hammer_core::log::{Level, LogWriter};
use hammer_core::metrics::{MetricCounter, MetricsRegistry};
#[cfg(test)]
use hammer_core::metrics::{MetricKind, MetricSample};
use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, error::TryRecvError, error::TrySendError,
};

const LOG_QUEUE_CAPACITY: usize = 4096;
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
    pub(crate) fn call<R>(&self, f: impl FnOnce() -> R + Send + 'static) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        self.call_with_timeout(DEFAULT_CONTROL_CALL_TIMEOUT, f)
    }

    /// Dispatch `f` onto the control thread and wait until the closure
    /// completes. Use this only for synchronous, non-cancelable state
    /// transitions where returning a timeout would leave background work
    /// continuing after the public API reported failure.
    pub(crate) fn call_blocking<R>(&self, f: impl FnOnce() -> R + Send + 'static) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<HammerResult<R>>();
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
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<HammerResult<R>>();
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
        f: impl FnOnce(mpsc::Sender<HammerResult<R>>) -> HammerResult<()> + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel::<HammerResult<R>>();
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
}

impl ControlThread {
    pub(crate) fn new(
        _base_time: Instant,
        inner: Arc<dyn LogWriter>,
        metrics: Arc<MetricsRegistry>,
        _metrics_interval: Duration,
        _min_level: Level,
    ) -> (Arc<ControlLogWriter>, Self) {
        let (log_tx, log_rx) = tokio::sync::mpsc::channel(LOG_QUEUE_CAPACITY);
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let scope = metrics.scope("runtime", "control_thread", "hammer-main");
        let writer = ControlLogWriter::new(log_tx, command_tx, scope.counter("log_dropped_total"));
        let thread = Self {
            log_rx,
            command_rx,
            inner,
        };
        (writer, thread)
    }

    pub(crate) async fn run(mut self) {
        // Once every `ControlLogWriter` is dropped, `log_rx.recv()` returns
        // `None` permanently. Disable that branch instead of `continue`-ing,
        // which would re-poll a ready-`None` future on every loop iteration
        // and burn CPU. Command and metrics dispatch keep running until the
        // command channel itself closes.
        let mut log_open = true;
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
                log = self.log_rx.recv(), if log_open => {
                    let Some(log) = log else {
                        log_open = false;
                        continue;
                    };
                    self.inner.write_message(log.level, log.message);
                    self.drain_logs();
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

/// Prefix shared by single-line and chunked metric snapshot dumps.
#[cfg(test)]
const SNAPSHOT_LINE_PREFIX: &str = "metrics_snapshot";

/// Conservative ceiling for a single snapshot log message. iOS unified
/// logging truncates ~1024 bytes per private record; staying under 900 bytes
/// leaves headroom for the formatter's timestamp/level/target prefix.
#[cfg(test)]
const SNAPSHOT_CHUNK_THRESHOLD: usize = 900;

/// File-private rendering contract: how a metric sample becomes one entry in
/// the snapshot line. Lives in this file (not on `MetricSample`) so the JSON
/// wire format stays a control-thread presentation concern, not part of the
/// metrics data model.
#[cfg(test)]
trait MetricLineRender {
    /// Append the fully-qualified flat key
    /// `{module}.{component_type}.{component_id}.{name}.{kind}[.{label_key}.{label_value}...]`
    /// to `out`. All five `MetricKey` identity fields are encoded so two
    /// registry entries differing only in `component_type`, `kind`, or label
    /// names produce distinct snapshot keys — `MetricsRegistry` treats those
    /// as separate metrics, the snapshot must too. Each segment passes through
    /// [`escape_segment`] first so segment-internal `.`, `"`, and other unsafe
    /// characters do not collide with the separator or break the surrounding
    /// JSON. Labels are emitted as key/value pairs so both the label name and
    /// value remain part of the snapshot identity.
    fn write_flat_key(&self, out: &mut String);

    /// Append `{module}.{component_type}.{component_id}` to `out`.
    fn write_component_key(&self, out: &mut String);

    /// Approximate `,"flat_key":value` byte size. Slight over-count keeps the
    /// chunk-fill decision conservative.
    fn approx_render_size(&self) -> usize;
}

#[cfg(test)]
impl MetricLineRender for MetricSample {
    fn write_flat_key(&self, out: &mut String) {
        escape_segment(out, &self.module);
        out.push('.');
        escape_segment(out, &self.component_type);
        out.push('.');
        escape_segment(out, &self.component_id);
        out.push('.');
        escape_segment(out, &self.name);
        out.push('.');
        escape_segment(out, self.kind.as_str());
        for label in &self.labels {
            out.push('.');
            escape_segment(out, &label.key);
            out.push('.');
            escape_segment(out, &label.value);
        }
    }

    fn write_component_key(&self, out: &mut String) {
        escape_segment(out, &self.module);
        out.push('.');
        escape_segment(out, &self.component_type);
        out.push('.');
        escape_segment(out, &self.component_id);
    }

    fn approx_render_size(&self) -> usize {
        // ,"<flat_key>":<u64>  — u64 worst case is 20 digits, so reserve the
        // full width so chunk-fill decisions never under-count and let the
        // unified log truncate. Segment lengths are post-escape (each `.`,
        // `~`, `"`, `\` doubles), counted exactly to avoid over-chunking on
        // the common ASCII-identifier path.
        let mut size = 4 + 20;
        size += escaped_segment_len(&self.module) + 1;
        size += escaped_segment_len(&self.component_type) + 1;
        size += escaped_segment_len(&self.component_id) + 1;
        size += escaped_segment_len(&self.name) + 1;
        size += escaped_segment_len(self.kind.as_str());
        for label in &self.labels {
            size += 1 + escaped_segment_len(&label.key) + 1 + escaped_segment_len(&label.value);
        }
        size
    }
}

#[cfg(test)]
fn same_component(a: &MetricSample, b: &MetricSample) -> bool {
    a.module == b.module && a.component_type == b.component_type && a.component_id == b.component_id
}

/// Append `segment` to `out` using a reversible encoding that protects both
/// the `.`-separated flat-key structure and the surrounding JSON string:
/// - ASCII letters/digits plus `_` and `-` stay literal
/// - every other UTF-8 byte becomes `~XX` (hex escape, so `.` never collides
///   with the separator and spaces/control chars remain distinct instead of
///   collapsing to `_`)
/// This is reversible at the byte level and keeps the surrounding JSON string
/// valid because the emitted form is ASCII-only.
#[cfg(test)]
fn escape_segment(out: &mut String, segment: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &b in segment.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(char::from(b));
        } else {
            out.push('~');
            out.push(char::from(HEX[(b >> 4) as usize]));
            out.push(char::from(HEX[(b & 0x0F) as usize]));
        }
    }
}

#[cfg(test)]
fn escaped_segment_len(s: &str) -> usize {
    // Safe ASCII bytes stay 1:1; everything else becomes `~XX` (3 bytes).
    s.as_bytes()
        .iter()
        .map(|b| {
            if b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-' {
                1
            } else {
                3
            }
        })
        .sum()
}

#[cfg(test)]
fn escape_json_string_fragment(out: &mut String, fragment: &str) {
    use std::fmt::Write as _;

    for c in fragment.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => write!(out, "\\u{:04x}", c as u32).expect("string write"),
            c => out.push(c),
        }
    }
}

/// Build one or more JSON snapshot lines, grouped by component. Each
/// `(module, component_type, component_id)` gets its own line or chunk sequence.
/// Caller must ensure `samples` is non-empty.
#[cfg(test)]
fn build_snapshot_lines(samples: &[MetricSample], ts: u64) -> Vec<String> {
    debug_assert!(
        !samples.is_empty(),
        "build_snapshot_lines called with empty samples"
    );

    let mut lines = Vec::new();
    let mut start = 0;
    for idx in 1..=samples.len() {
        if idx == samples.len() || !same_component(&samples[start], &samples[idx]) {
            lines.extend(build_component_snapshot_lines(&samples[start..idx], ts));
            start = idx;
        }
    }
    lines
}

#[cfg(test)]
fn build_component_snapshot_lines(samples: &[MetricSample], ts: u64) -> Vec<String> {
    debug_assert!(
        !samples.is_empty(),
        "build_component_snapshot_lines called with empty samples"
    );

    let mut component = String::new();
    samples[0].write_component_key(&mut component);

    let mut single = String::with_capacity(64 + component.len() + samples.len() * 48);
    write_snapshot_prefix(&mut single, ts, &component, None);
    write_snapshot_body(&mut single, samples);
    single.push('}');
    if single.len() <= SNAPSHOT_CHUNK_THRESHOLD {
        return vec![single];
    }

    // Oversized. Repartition samples into chunks. Estimate is approximate; we
    // gate each push with the threshold so a pathologically long single
    // sample still ends up in its own diagnostic line instead of being folded
    // into the numbered chunk sequence.
    let mut chunks: Vec<Vec<&MetricSample>> = Vec::new();
    let mut diagnostics: Vec<String> = Vec::new();
    let mut current: Vec<&MetricSample> = Vec::new();
    let chunk_header_size = component_chunk_header_size(ts, &component, samples.len());
    let mut current_size = chunk_header_size;
    for sample in samples {
        let sample_size = sample.approx_render_size();
        if sample_size + chunk_header_size > SNAPSHOT_CHUNK_THRESHOLD {
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
                current_size = chunk_header_size;
            }
            diagnostics.push(build_oversized_line(sample, ts, &component));
            continue;
        }
        if !current.is_empty() && current_size + sample_size > SNAPSHOT_CHUNK_THRESHOLD {
            chunks.push(std::mem::take(&mut current));
            current_size = chunk_header_size;
        }
        current.push(sample);
        current_size += sample_size;
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    let total = chunks.len();
    let mut lines: Vec<String> = chunks
        .into_iter()
        .enumerate()
        .map(|(idx, chunk)| {
            let mut s = String::with_capacity(64 + chunk.len() * 48);
            write_snapshot_prefix(&mut s, ts, &component, Some((idx, total)));
            write_snapshot_body(&mut s, chunk);
            s.push('}');
            s
        })
        .collect();
    lines.extend(diagnostics);
    lines
}

/// Maximum bytes of the flat key kept verbatim in an oversized warning line.
/// 200 keeps the warning well under any practical oslog cap while preserving
/// enough prefix to identify which metric blew up.
#[cfg(test)]
const OVERSIZED_KEY_HEAD: usize = 200;

#[cfg(test)]
fn build_oversized_line(sample: &MetricSample, ts: u64, component: &str) -> String {
    use std::fmt::Write as _;

    let component_total_len = component.len();
    let component_head = &component[..component.len().min(OVERSIZED_KEY_HEAD)];
    let component_truncated = component_head.len() < component_total_len;
    let mut escaped_component_head = String::with_capacity(component_head.len());
    escape_json_string_fragment(&mut escaped_component_head, component_head);

    let mut full_key = String::new();
    sample.write_flat_key(&mut full_key);
    let total_len = full_key.len();
    let head = &full_key[..full_key.len().min(OVERSIZED_KEY_HEAD)];
    let truncated = head.len() < total_len;

    let mut escaped_head = String::with_capacity(head.len());
    escape_json_string_fragment(&mut escaped_head, head);

    let mut s = String::with_capacity(96 + escaped_head.len());
    write!(
        s,
        "{}.oversized {{\"ts\":{},\"component\":\"{}\",\"component_truncated\":{},\"key_head\":\"{}\",\"truncated\":{},\"size\":{},\"value\":{}}}",
        SNAPSHOT_LINE_PREFIX,
        ts,
        escaped_component_head,
        component_truncated,
        escaped_head,
        truncated,
        total_len,
        sample.value,
    )
    .expect("string write");
    s
}

#[cfg(test)]
fn component_chunk_header_size(ts: u64, component: &str, max_chunks: usize) -> usize {
    // Each chunk carries at least one sample, so the caller's sample count is
    // a tight upper bound for both `idx` (range 0..total) and `total`. Padding
    // with `usize::MAX` here would over-count by ~57 bytes on 64-bit builds
    // and push borderline samples into the .oversized fallback even when they
    // would have fit in a numbered chunk.
    let mut s = String::new();
    write_snapshot_prefix(&mut s, ts, component, Some((max_chunks, max_chunks)));
    s.len() + 1
}

#[cfg(test)]
fn write_snapshot_prefix(
    out: &mut String,
    ts: u64,
    component: &str,
    chunk: Option<(usize, usize)>,
) {
    use std::fmt::Write as _;
    match chunk {
        None => write!(
            out,
            "{} {{\"ts\":{},\"component\":\"{}\"",
            SNAPSHOT_LINE_PREFIX, ts, component,
        )
        .expect("string write"),
        Some((idx, total)) => write!(
            out,
            "{}.{} {{\"ts\":{},\"component\":\"{}\",\"part\":{},\"of\":{}",
            SNAPSHOT_LINE_PREFIX, idx, ts, component, idx, total,
        )
        .expect("string write"),
    }
}

#[cfg(test)]
fn write_snapshot_body<'a, I>(out: &mut String, samples: I)
where
    I: IntoIterator<Item = &'a MetricSample>,
{
    use std::fmt::Write as _;
    for sample in samples {
        out.push_str(",\"");
        sample.write_flat_key(out);
        out.push_str("\":");
        write!(out, "{}", sample.value).expect("string write");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::log::Level;
    use hammer_core::metrics::MetricLabel;
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
    fn control_writer_does_not_dump_registered_metrics() {
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
            !lines.iter().any(|line| line.contains("metrics_snapshot")),
            "metrics are exposed through RuntimeService::metrics_snapshot, not control logs: {lines:?}"
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
            !lines.iter().any(|line| line.contains("metrics_snapshot")),
            "snapshot should be suppressed at Warn level: lines = {lines:?}"
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

        let panicked: HammerResult<()> =
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
        let result: HammerResult<()> = writer.call_with_timeout(Duration::from_millis(100), || ());
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

    #[test]
    fn control_writer_shutdown_does_not_dump_idle_metrics() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
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
            !lines.iter().any(|line| line.contains("metrics_snapshot")),
            "control thread should not dump metrics on shutdown: {lines:?}"
        );
    }

    #[test]
    fn metrics_snapshot_emits_single_line_with_multiple_metrics() {
        let metrics = MetricsRegistry::new();
        let scope = metrics.scope("outbound", "outbound", "direct");
        scope.counter("dial_error_total").inc();
        scope.counter("stream_read_error_total").inc();
        scope.counter("idle_total");
        scope
            .counter_with_labels(
                "packet_send_error_total",
                [("network", "tcp"), ("family", "v4")],
            )
            .inc();
        let lines = build_snapshot_lines(&metrics.snapshot(), 1_715_059_200);
        let snapshot_line = lines
            .iter()
            .find(|line| line.starts_with("metrics_snapshot "))
            .expect("expected single-line snapshot");
        for key in [
            "\"outbound.outbound.direct.dial_error_total.counter\":1",
            "\"outbound.outbound.direct.idle_total.counter\":0",
            "\"outbound.outbound.direct.stream_read_error_total.counter\":1",
            // Labels sort by (key, value): ("family","v4") < ("network","tcp"),
            // so the flat key suffix is `.family.v4.network.tcp`.
            "\"outbound.outbound.direct.packet_send_error_total.counter.family.v4.network.tcp\":1",
        ] {
            assert!(
                snapshot_line.contains(key),
                "snapshot missing {key}: {snapshot_line}"
            );
        }
    }

    #[test]
    fn metrics_snapshot_chunks_when_oversized() {
        let metrics = MetricsRegistry::new();
        let scope = metrics.scope("outbound", "outbound", "very-long-component-id-for-padding");
        // 50 counters with padded names easily exceed SNAPSHOT_CHUNK_THRESHOLD (900).
        for i in 0..50_u32 {
            scope
                .counter(&format!("metric_with_padding_name_index_{i:02}_total"))
                .inc();
        }

        let lines = build_snapshot_lines(&metrics.snapshot(), 1_715_059_200);
        let chunks: Vec<&String> = lines
            .iter()
            .filter(|line| line.contains("metrics_snapshot."))
            .collect();
        assert!(
            chunks.len() >= 2,
            "expected at least 2 chunks, got {} (lines = {:?})",
            chunks.len(),
            lines
        );
        for chunk in &chunks {
            assert!(chunk.contains("\"part\":"), "chunk missing part: {chunk}");
            assert!(chunk.contains("\"of\":"), "chunk missing of: {chunk}");
            assert!(chunk.contains("\"ts\":"), "chunk missing ts: {chunk}");
        }
    }

    #[test]
    fn metrics_snapshot_not_dumped_on_shutdown() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        metrics
            .scope("outbound", "outbound", "direct")
            .counter("dial_error_total")
            .inc();
        // Long interval → only the shutdown path can fire dump_metrics.
        let (writer, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let handle = run_control_thread(thread);

        assert!(writer.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");

        let lines = inner.lines.lock().unwrap();
        assert!(
            !lines.iter().any(|line| line.contains("metrics_snapshot")),
            "shutdown should not emit metrics; Swift callers pull service.metrics(): {lines:?}"
        );
    }

    #[test]
    fn metrics_snapshot_keeps_zero_valued_counters() {
        let metrics = MetricsRegistry::new();
        metrics
            .scope("outbound", "outbound", "direct")
            .counter("idle_total");

        let snapshot_lines = build_snapshot_lines(&metrics.snapshot(), 1_715_059_200);
        assert!(
            snapshot_lines
                .iter()
                .any(|line| line.contains("\"outbound.outbound.direct.idle_total.counter\":0")),
            "zero-valued counter should remain visible: {snapshot_lines:?}"
        );
    }

    fn sample(
        module: &str,
        component_id: &str,
        name: &str,
        value: u64,
        labels: &[(&str, &str)],
    ) -> MetricSample {
        MetricSample {
            module: module.into(),
            component_type: module.into(),
            component_id: component_id.into(),
            name: name.into(),
            kind: MetricKind::Counter,
            value,
            labels: labels
                .iter()
                .map(|(k, v)| MetricLabel {
                    key: (*k).into(),
                    value: (*v).into(),
                })
                .collect(),
        }
    }

    #[test]
    fn flat_key_encodes_full_metric_identity_with_sorted_label_values() {
        // Stored sorted by (key, value) per MetricKey constructor invariant —
        // ("family","v4") < ("network","tcp"), so the flat suffix is ".v4.tcp".
        let s = sample(
            "outbound",
            "direct",
            "packet_send_error_total",
            7,
            &[("family", "v4"), ("network", "tcp")],
        );
        let mut out = String::new();
        s.write_flat_key(&mut out);
        assert_eq!(
            out,
            "outbound.outbound.direct.packet_send_error_total.counter.family.v4.network.tcp"
        );
    }

    #[test]
    fn flat_key_escapes_unsafe_characters_to_keep_snapshot_injective_and_json_safe() {
        // Segment with a literal `.`: must NOT split the separator. `~1` is
        // the JSON-Pointer-style placeholder; the inverse `~0` preserves any
        // raw `~` already in the input.
        // The `sample(...)` fixture sets component_type = module, so an escaped
        // module string appears twice — that's expected behaviour, not a bug.
        let dotty = sample("foo.bar", "id~tilde", "name has space", 1, &[]);
        let mut out = String::new();
        dotty.write_flat_key(&mut out);
        assert_eq!(
            out,
            "foo~2Ebar.foo~2Ebar.id~7Etilde.name~20has~20space.counter"
        );

        // Spaces and control characters must remain distinct from underscores
        // and must not collapse into the same snapshot key.
        let spaced = sample("foo bar", "i", "n", 1, &[]);
        let underscored = sample("foo_bar", "i", "n", 1, &[]);
        let mut spaced_key = String::new();
        let mut underscored_key = String::new();
        spaced.write_flat_key(&mut spaced_key);
        underscored.write_flat_key(&mut underscored_key);
        assert_ne!(
            spaced_key, underscored_key,
            "space must not collapse to underscore: {spaced_key} vs {underscored_key}"
        );

        let control = sample("line\nbreak", "i", "n\tm", 1, &[]);
        let mut control_key = String::new();
        control.write_flat_key(&mut control_key);
        assert!(
            control_key.contains("line~0Abreak"),
            "newline should be preserved as an escaped byte, got {control_key}"
        );
        assert!(
            control_key.contains("n~09m"),
            "tab should be preserved as an escaped byte, got {control_key}"
        );

        // Two distinct label sets that would collapse without escaping —
        // ("k","a.b") vs ("k","a") + ("k2","b") — must produce distinct keys.
        let a = sample("m", "i", "n", 1, &[("k", "a.b")]);
        let b = sample("m", "i", "n", 1, &[("k", "a"), ("k2", "b")]);
        let (mut ka, mut kb) = (String::new(), String::new());
        a.write_flat_key(&mut ka);
        b.write_flat_key(&mut kb);
        assert_ne!(ka, kb, "escape must keep snapshot injective: {ka} vs {kb}");

        let same_values_a = sample("m", "i", "n", 1, &[("left", "v"), ("right", "w")]);
        let same_values_b = sample("m", "i", "n", 1, &[("alpha", "v"), ("beta", "w")]);
        let (mut sva, mut svb) = (String::new(), String::new());
        same_values_a.write_flat_key(&mut sva);
        same_values_b.write_flat_key(&mut svb);
        assert_ne!(
            sva, svb,
            "label names must remain part of the snapshot identity: {sva} vs {svb}"
        );

        // Quote and backslash are hex-escaped like every other unsafe byte, so
        // they stay distinct and remain valid inside the JSON key string.
        let quoted = sample("m", "i", "weird\"name", 1, &[]);
        let mut q = String::new();
        quoted.write_flat_key(&mut q);
        assert!(q.contains("weird~22name"), "expected hex escape, got {q}");
        let backslashed = sample("m", "i", "back\\slash", 1, &[]);
        let mut bs = String::new();
        backslashed.write_flat_key(&mut bs);
        assert!(
            bs.contains("back~5Cslash"),
            "expected backslash escape, got {bs}"
        );
    }

    #[test]
    fn build_snapshot_lines_emits_single_line_under_threshold() {
        let samples = vec![sample("outbound", "direct", "dial_error_total", 1, &[])];
        let lines = build_snapshot_lines(&samples, 1715059200);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].starts_with("metrics_snapshot {"));
        assert!(lines[0].contains("\"ts\":1715059200"));
        assert!(lines[0].contains("\"component\":\"outbound.outbound.direct\""));
        assert!(lines[0].contains("\"outbound.outbound.direct.dial_error_total.counter\":1"));
        assert!(lines[0].ends_with('}'));
    }

    #[test]
    fn build_snapshot_lines_groups_output_by_component() {
        let samples = vec![
            sample("outbound", "direct", "dial_error_total", 1, &[]),
            sample("runtime", "hammer-main", "log_dropped_total", 0, &[]),
        ];
        let lines = build_snapshot_lines(&samples, 1715059200);
        assert_eq!(lines.len(), 2, "one line per component: {lines:?}");

        let outbound = lines
            .iter()
            .find(|line| line.contains("\"component\":\"outbound.outbound.direct\""))
            .expect("outbound component line");
        assert!(outbound.contains("\"outbound.outbound.direct.dial_error_total.counter\":1"));
        assert!(
            !outbound.contains("runtime.runtime.hammer-main"),
            "component lines must not mix samples: {outbound}"
        );

        let runtime = lines
            .iter()
            .find(|line| line.contains("\"component\":\"runtime.runtime.hammer-main\""))
            .expect("runtime component line");
        assert!(runtime.contains("\"runtime.runtime.hammer-main.log_dropped_total.counter\":0"));
        assert!(
            !runtime.contains("outbound.outbound.direct"),
            "component lines must not mix samples: {runtime}"
        );
    }

    #[test]
    fn build_snapshot_lines_emits_oversized_diagnostic_for_giant_single_sample() {
        // Pathological: a single sample whose component_id starts with quote
        // and backslash escapes and is long enough to exceed the chunk
        // threshold. The head must be JSON-escaped after truncation so the log
        // line stays parseable.
        let huge_id = format!("😀{}", "a".repeat(SNAPSHOT_CHUNK_THRESHOLD + 100));
        let samples = vec![sample("outbound", &huge_id, "dial_error_total", 42, &[])];
        let lines = build_snapshot_lines(&samples, 1715059200);
        assert_eq!(lines.len(), 1, "expected exactly one diagnostic line");
        let line = &lines[0];
        assert!(
            line.starts_with("metrics_snapshot.oversized {"),
            "unexpected prefix: {line}"
        );
        assert!(line.contains("\"truncated\":true"), "{line}");
        assert!(line.contains("\"value\":42"), "{line}");
        let mut full_key = String::new();
        samples[0].write_flat_key(&mut full_key);
        let head: String = full_key.chars().take(OVERSIZED_KEY_HEAD).collect();
        let mut escaped_head = String::new();
        escape_json_string_fragment(&mut escaped_head, &head);
        assert!(
            line.contains(&format!("\"key_head\":\"{}\"", escaped_head)),
            "key_head should be escaped and truncated cleanly: {line}"
        );
        assert!(
            line.len() < SNAPSHOT_CHUNK_THRESHOLD,
            "diagnostic line itself must fit budget, got {} bytes",
            line.len()
        );
    }

    #[test]
    fn build_snapshot_lines_keeps_oversized_diagnostics_out_of_chunk_count() {
        let mut samples = Vec::new();
        for i in 0..40_u32 {
            samples.push(sample(
                "outbound",
                "very-long-component-id-for-padding-bytes",
                &format!("metric_with_padding_name_index_{i:02}_total"),
                1,
                &[],
            ));
            if i == 19 {
                samples.push(sample(
                    "outbound",
                    &format!("😀{}", "z".repeat(SNAPSHOT_CHUNK_THRESHOLD + 100)),
                    "giant_total",
                    1,
                    &[],
                ));
            }
        }

        let lines = build_snapshot_lines(&samples, 0);
        let regular: Vec<&String> = lines
            .iter()
            .filter(|line| line.contains("\"part\":"))
            .collect();
        let oversized: Vec<&String> = lines
            .iter()
            .filter(|line| line.starts_with("metrics_snapshot.oversized "))
            .collect();

        assert_eq!(
            oversized.len(),
            1,
            "expected one oversized diagnostic: {lines:?}"
        );
        assert!(
            regular.len() >= 2,
            "expected at least two numbered chunks, got {} (lines = {:?})",
            regular.len(),
            lines
        );

        let mut seen_parts = Vec::new();
        for line in &regular {
            let part = line
                .split("\"part\":")
                .nth(1)
                .and_then(|rest| rest.split(',').next())
                .and_then(|s| s.parse::<usize>().ok())
                .expect("chunk part index");
            seen_parts.push(part);
            assert!(line.contains("\"of\":"), "chunk missing of: {line}");
        }
        seen_parts.sort_unstable();
        seen_parts.dedup();
        assert!(
            !seen_parts.is_empty() && seen_parts[0] == 0,
            "chunk parts should start at 0: {seen_parts:?}"
        );
        assert_eq!(
            seen_parts,
            (0..seen_parts.len()).collect::<Vec<_>>(),
            "chunk parts should be contiguous: {seen_parts:?}"
        );
        assert!(
            !oversized[0].contains("\"part\":"),
            "oversized diagnostics must stay out of numbered chunk metadata: {}",
            oversized[0]
        );
    }

    #[test]
    fn build_snapshot_lines_chunks_when_payload_exceeds_threshold() {
        let mut samples = Vec::new();
        for i in 0..40_u32 {
            samples.push(sample(
                "outbound",
                "very-long-component-id-for-padding-bytes",
                &format!("metric_with_padding_name_index_{i:02}_total"),
                1,
                &[],
            ));
        }
        let lines = build_snapshot_lines(&samples, 0);
        assert!(
            lines.len() >= 2,
            "expected at least 2 chunks, got {}",
            lines.len()
        );
        let total = lines.len();
        for (idx, line) in lines.iter().enumerate() {
            assert!(
                line.starts_with(&format!("metrics_snapshot.{idx} {{")),
                "chunk {idx} bad prefix: {line}"
            );
            assert!(line.contains(&format!("\"part\":{idx}")), "{line}");
            assert!(line.contains(&format!("\"of\":{total}")), "{line}");
            assert!(line.ends_with('}'), "{line}");
        }
    }

    #[test]
    fn component_chunk_header_size_is_a_tight_upper_bound() {
        // chunk_header_size feeds the partition gate in
        // build_component_snapshot_lines. If the estimate is much larger than
        // the real worst-case header, samples that would fit in a numbered
        // chunk get spuriously routed to .oversized — changing the output
        // schema for valid snapshots.
        let component = "outbound.outbound.direct";
        let ts: u64 = 1_715_059_200;
        for max_chunks in [1usize, 5, 10, 100, 999] {
            let worst_idx = max_chunks.saturating_sub(1);
            let mut real = String::new();
            write_snapshot_prefix(&mut real, ts, component, Some((worst_idx, max_chunks)));
            let real_size = real.len() + 1;

            let estimate = component_chunk_header_size(ts, component, max_chunks);
            assert!(
                estimate >= real_size,
                "estimate {estimate} must be >= real {real_size} for max_chunks={max_chunks}",
            );
            let slack = estimate - real_size;
            assert!(
                slack <= 3,
                "estimate slack {slack} too loose for max_chunks={max_chunks} \
                 (estimate={estimate}, real={real_size})",
            );
        }
    }
}
