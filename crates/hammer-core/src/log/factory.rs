use std::sync::Arc;
use std::time::Instant;

use super::format::Formatter;
use super::id::{self, ConnId};
use super::level::Level;

/// Sink for fully-formatted log lines (newline included).
pub trait LogWriter: Send + Sync {
    fn write_message(&self, level: Level, message: String);
}

pub struct DiscardWriter;

impl LogWriter for DiscardWriter {
    fn write_message(&self, _level: Level, _message: String) {}
}

pub struct Factory {
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
        Arc::new(Self {
            formatter: Formatter::new(base_time),
            writer,
            min_level,
        })
    }

    pub fn new_logger(self: &Arc<Self>, tag: impl Into<String>) -> Logger {
        Logger {
            tag: Arc::from(tag.into()),
            factory: Arc::clone(self),
        }
    }

    pub fn close(&self) {}
}

#[derive(Clone)]
pub struct Logger {
    tag: Arc<str>,
    factory: Arc<Factory>,
}

impl Logger {
    pub fn log(&self, level: Level, message: impl Into<String>) {
        self.log_at(level, message, Instant::now())
    }

    pub fn log_at(&self, level: Level, message: impl Into<String>, ts: Instant) {
        if level as i32 > self.factory.min_level as i32 {
            return;
        }
        let ctx = id::current();
        let line = self
            .factory
            .formatter
            .format(ctx, level, &self.tag, &message.into(), ts);
        self.factory.writer.write_message(level, line);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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
}
