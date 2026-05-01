use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use super::format::Formatter;
use super::id::{self, ConnId};
use super::level::Level;
use tracing::Dispatch;
use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// Sink for fully-formatted log lines (newline included).
pub trait LogWriter: Send + Sync {
    fn write_message(&self, level: Level, message: String);
}

pub struct DiscardWriter;

impl LogWriter for DiscardWriter {
    fn write_message(&self, _level: Level, _message: String) {}
}

pub struct Factory {
    state: Arc<LogState>,
    dispatch: Dispatch,
}

struct LogState {
    formatter: Formatter,
    writer: Arc<dyn LogWriter>,
    min_level: Level,
}

impl Factory {
    pub fn new(base_time: Instant, writer: Arc<dyn LogWriter>) -> Arc<Self> {
        Self::new_with_min_level(base_time, writer, Level::Trace)
    }

    pub fn new_with_min_level(
        base_time: Instant,
        writer: Arc<dyn LogWriter>,
        min_level: Level,
    ) -> Arc<Self> {
        let state = Arc::new(LogState {
            formatter: Formatter::new(base_time),
            writer,
            min_level,
        });
        // Each Factory owns its own Dispatch so multiple Factories can coexist
        // without racing on a global subscriber. Callers attach the dispatch to
        // their async work via `tracing::dispatcher::set_default(factory.dispatch())`
        // (sync) or `future.with_subscriber(factory.dispatch().clone())` (async),
        // and propagate it to spawned tasks via `crate::spawn::spawn` (which
        // wraps `tokio::spawn(future.with_current_subscriber())`).
        let layer = HammerLogLayer {
            state: Arc::clone(&state),
        };
        let subscriber = tracing_subscriber::registry().with(layer);
        let dispatch = Dispatch::new(subscriber);
        Arc::new(Self { state, dispatch })
    }

    pub fn dispatch(&self) -> &Dispatch {
        &self.dispatch
    }

    pub fn new_logger(self: &Arc<Self>, id: impl Into<String>) -> Logger {
        Logger {
            id: Arc::from(id.into()),
            factory: Arc::clone(self),
        }
    }
}

#[derive(Clone)]
pub struct Logger {
    id: Arc<str>,
    factory: Arc<Factory>,
}

impl Logger {
    pub fn log(&self, level: Level, message: impl Into<String>) {
        self.log_at(level, message, Instant::now())
    }

    pub fn log_at(&self, level: Level, message: impl Into<String>, ts: Instant) {
        self.factory
            .state
            .emit(level, &self.id, &message.into(), ts);
    }

    pub fn trace(&self, msg: impl Into<String>) {
        self.log(Level::Trace, msg)
    }
    pub fn debug(&self, msg: impl Into<String>) {
        self.log(Level::Debug, msg)
    }
    pub fn info(&self, msg: impl Into<String>) {
        self.log(Level::Info, msg)
    }
    pub fn warn(&self, msg: impl Into<String>) {
        self.log(Level::Warn, msg)
    }
    pub fn error(&self, msg: impl Into<String>) {
        self.log(Level::Error, msg)
    }

    pub fn with_conn(&self, _id: ConnId) -> Self {
        // M1: ConnId is propagated via task_local::with_conn_id; this convenience helper
        // exists so callers reading the Go reference port can keep the same shape.
        self.clone()
    }
}

impl LogState {
    fn emit(&self, level: Level, id: &str, message: &str, ts: Instant) {
        if !level_enabled(level, self.min_level) {
            return;
        }
        let ctx = id::current();
        let line = self.formatter.format(ctx, level, id, message, ts);
        self.writer.write_message(level, line);
    }
}

struct HammerLogLayer {
    state: Arc<LogState>,
}

impl<S> Layer<S> for HammerLogLayer
where
    S: Subscriber,
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        let level: Level = (*metadata.level()).into();
        level_enabled(level, self.state.min_level)
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let level: Level = (*event.metadata().level()).into();
        if !level_enabled(level, self.state.min_level) {
            return;
        }
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);
        let message = visitor.finish();
        self.state
            .emit(level, event.metadata().target(), &message, Instant::now());
    }
}

#[derive(Default)]
struct EventVisitor {
    message: Option<String>,
    fields: Vec<String>,
}

impl EventVisitor {
    fn finish(self) -> String {
        let mut message = self.message.unwrap_or_default();
        for field in self.fields {
            if !message.is_empty() {
                message.push(' ');
            }
            message.push_str(&field);
        }
        message
    }

    fn record_value(&mut self, field: &Field, value: impl fmt::Display) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.fields.push(format!("{}={value}", field.name()));
        }
    }
}

impl Visit for EventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(field, format_args!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, value);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, value);
    }
}

fn level_enabled(level: Level, min_level: Level) -> bool {
    level as i32 <= min_level as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tracing::{debug, info};

    struct CaptureWriter {
        lines: Mutex<Vec<(Level, String)>>,
    }

    impl LogWriter for CaptureWriter {
        fn write_message(&self, level: Level, message: String) {
            self.lines.lock().unwrap().push((level, message));
        }
    }

    #[test]
    fn logger_routes_lines_to_writer() {
        let writer = Arc::new(CaptureWriter {
            lines: Mutex::new(Vec::new()),
        });
        let factory = Factory::new(Instant::now(), writer.clone());
        let logger = factory.new_logger("router");
        logger.info("started");
        let captured = writer.lines.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0, Level::Info);
        assert!(
            captured[0].1.starts_with("H[I] "),
            "got = {}",
            captured[0].1
        );
        assert!(
            captured[0].1.contains(" path: started"),
            "got = {}",
            captured[0].1
        );
        assert!(captured[0].1.ends_with('\n'));
    }

    #[test]
    fn logger_filters_messages_below_min_level() {
        let writer = Arc::new(CaptureWriter {
            lines: Mutex::new(Vec::new()),
        });
        let factory = Factory::new_with_min_level(Instant::now(), writer.clone(), Level::Info);
        let logger = factory.new_logger("router");

        logger.debug("hidden");
        logger.info("visible");
        logger.error("also visible");

        let captured = writer.lines.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].0, Level::Info);
        assert_eq!(captured[1].0, Level::Error);
    }

    #[test]
    fn macro_does_not_format_below_min_level() {
        use std::fmt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Probe<'a>(&'a AtomicUsize);

        impl fmt::Display for Probe<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fetch_add(1, Ordering::SeqCst);
                f.write_str("probe")
            }
        }

        let writer = Arc::new(CaptureWriter {
            lines: Mutex::new(Vec::new()),
        });
        let factory = Factory::new_with_min_level(Instant::now(), writer.clone(), Level::Info);
        // Pin the Factory's Dispatch onto this test thread so the tracing
        // macros below route through the Factory's HammerLogLayer and its
        // level filter — that's what suppresses the format! call for
        // `debug!` while letting `info!` through.
        let _dispatch_guard = tracing::dispatcher::set_default(factory.dispatch());
        let calls = AtomicUsize::new(0);

        debug!("hidden {}", Probe(&calls));
        info!("visible {}", Probe(&calls));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let captured = writer.lines.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0, Level::Info);
    }
}
