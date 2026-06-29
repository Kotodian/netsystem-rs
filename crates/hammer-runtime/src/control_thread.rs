mod timer;

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use hammer_core::error::{HammerError, HammerResult};
#[cfg(test)]
use hammer_core::metrics::{MetricKind, MetricSample};

pub use self::timer::ControlTimerHandle;
use self::timer::{ControlTimerId, ControlTimerRegistration, TimerRegistry};

pub const DEFAULT_CONTROL_CALL_TIMEOUT: Duration = Duration::from_secs(30);

struct BarrierArcs {
    wait: Arc<AtomicU32>,
    workers: Arc<AtomicU32>,
    n_workers: u32,
}

pub struct ControlThreadHandle {
    command_tx: tokio::sync::mpsc::UnboundedSender<ControlCommand>,
    closed: AtomicBool,
    next_timer_id: std::sync::atomic::AtomicU64,
    barrier_state: Mutex<Option<BarrierArcs>>,
}

impl ControlThreadHandle {
    fn new(command_tx: tokio::sync::mpsc::UnboundedSender<ControlCommand>) -> Arc<Self> {
        Arc::new(Self {
            command_tx,
            closed: AtomicBool::new(false),
            next_timer_id: std::sync::atomic::AtomicU64::new(1),
            barrier_state: Mutex::new(None),
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
    ) -> HammerResult<ControlTimerHandle>
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
    ) -> HammerResult<ControlTimerHandle>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if interval.is_zero() {
            return Err(HammerError::internal(
                "control timer interval must be non-zero",
            ));
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
    ) -> HammerResult<ControlTimerHandle> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(HammerError::service_closed());
        }
        let id = registration.id();
        self.command_tx
            .send(ControlCommand::RegisterTimer(registration))
            .map_err(|_| HammerError::internal("control thread stopped"))?;
        Ok(ControlTimerHandle::new(id, self.command_tx.clone()))
    }

    pub fn call<R>(&self, f: impl FnOnce() -> R + Send + 'static) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        self.call_with_timeout(DEFAULT_CONTROL_CALL_TIMEOUT, f)
    }

    pub fn call_blocking<R>(&self, f: impl FnOnce() -> R + Send + 'static) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        self.call_inner(f, false)
    }

    fn call_inner<R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
        _with_timeout: bool,
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
        done_rx
            .recv()
            .map_err(|_| HammerError::internal("control command canceled"))?
    }

    pub fn call_with_timeout<R>(
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

    pub fn call_async<R>(
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
                    Ok(Ok(())) => {}
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

    pub fn set_barrier_arcs(&self, wait: Arc<AtomicU32>, workers: Arc<AtomicU32>, n_workers: u32) {
        *self.barrier_state.lock().expect("barrier_state lock") = Some(BarrierArcs {
            wait,
            workers,
            n_workers,
        });
    }

    pub fn control_call_with_barrier<R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let (wait, workers, n_workers) = {
            let guard = self.barrier_state.lock().expect("barrier_state lock");
            let s = guard.as_ref().ok_or_else(|| {
                HammerError::internal("control_call_with_barrier: barrier not configured")
            })?;
            (Arc::clone(&s.wait), Arc::clone(&s.workers), s.n_workers)
        };

        self.call_blocking(move || {
            let _guard = crate::barrier::barrier_sync(&wait, &workers, n_workers);
            f()
        })
    }
}

pub struct ControlThread {
    command_rx: tokio::sync::mpsc::UnboundedReceiver<ControlCommand>,
    command_tx: tokio::sync::mpsc::UnboundedSender<ControlCommand>,
    timers: TimerRegistry,
}

impl ControlThread {
    pub fn new(
        _base_time: std::time::Instant,
        _min_level: hammer_core::log::Level,
    ) -> (Arc<ControlThreadHandle>, Self) {
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

/// Prefix shared by single-line and chunked metric snapshot dumps.
#[cfg(test)]
const SNAPSHOT_LINE_PREFIX: &str = "metrics_snapshot";

/// Conservative ceiling for a single snapshot log message.
#[cfg(test)]
const SNAPSHOT_CHUNK_THRESHOLD: usize = 900;

/// File-private rendering contract: how a metric sample becomes one entry in
/// the snapshot line.
#[cfg(test)]
trait MetricLineRender {
    fn write_flat_key(&self, out: &mut String);
    fn write_component_key(&self, out: &mut String);
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
        SNAPSHOT_LINE_PREFIX, ts, escaped_component_head, component_truncated, escaped_head, truncated, total_len, sample.value,
    ).expect("string write");
    s
}

#[cfg(test)]
fn component_chunk_header_size(ts: u64, component: &str, max_chunks: usize) -> usize {
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
            SNAPSHOT_LINE_PREFIX, ts, component
        )
        .expect("string write"),
        Some((idx, total)) => write!(
            out,
            "{}.{} {{\"ts\":{},\"component\":\"{}\",\"part\":{},\"of\":{}",
            SNAPSHOT_LINE_PREFIX, idx, ts, component, idx, total
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
    use hammer_core::metrics::MetricLabel;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

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
    fn control_handle_shutdown_completes() {
        let (control_handle, thread) =
            ControlThread::new(std::time::Instant::now(), hammer_core::log::Level::Info);
        let handle = run_control_thread(thread);
        assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");
    }

    #[test]
    fn control_thread_survives_panicking_call() {
        let (control_handle, thread) =
            ControlThread::new(std::time::Instant::now(), hammer_core::log::Level::Info);
        let handle = run_control_thread(thread);

        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        let panicked: HammerResult<()> =
            control_handle.call_with_timeout(Duration::from_secs(1), || panic!("boom"));
        assert!(panicked.is_err(), "panicking closure should surface error");
        assert!(
            !control_handle.is_closed(),
            "control thread must survive panic"
        );

        let ok: HammerResult<()> = control_handle.call_with_timeout(Duration::from_secs(1), || {});
        assert!(
            ok.is_ok(),
            "control thread must still service calls after panic"
        );

        std::panic::set_hook(prev_hook);
        assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");
    }

    #[test]
    fn control_timer_fires_once() {
        use std::sync::atomic::AtomicBool;
        let (control_handle, thread) =
            ControlThread::new(std::time::Instant::now(), hammer_core::log::Level::Info);
        let handle = run_control_thread(thread);

        let fired = Arc::new(AtomicBool::new(false));
        let fired_clone = Arc::clone(&fired);
        let timer_handle = control_handle
            .schedule_once(Duration::from_millis(10), move || {
                let fired = Arc::clone(&fired_clone);
                async move {
                    fired.store(true, Ordering::SeqCst);
                }
            })
            .expect("schedule once");

        std::thread::sleep(Duration::from_millis(50));
        assert!(fired.load(Ordering::SeqCst), "timer should have fired");
        assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");
    }
}
