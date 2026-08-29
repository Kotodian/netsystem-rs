use std::time::{Duration, Instant};

use super::id::ConnId;
use super::level::Level;

#[derive(Debug, Clone, Copy)]
pub struct Formatter {
    pub base_time: Instant,
    pub disable_line_break: bool,
}

impl Formatter {
    pub fn new(base_time: Instant) -> Self {
        Self {
            base_time,
            disable_line_break: false,
        }
    }

    /// Equivalent to Go's `Format(ctx, level, id, message, timestamp)`.
    /// Adds a trailing `\n` unless `disable_line_break` is set or one is already present.
    pub fn format(
        &self,
        ctx: Option<ConnId>,
        level: Level,
        id: &str,
        message: &str,
        timestamp: Instant,
    ) -> String {
        let elapsed = timestamp.saturating_duration_since(self.base_time);
        let line = build_hammer_line(ctx, level, id, message, elapsed);
        apply_line_break(line, self.disable_line_break)
    }

    /// Mirror of Go's `FormatPlatform`. Identical to `format`.
    pub fn format_platform(
        &self,
        ctx: Option<ConnId>,
        level: Level,
        id: &str,
        message: &str,
        timestamp: Instant,
    ) -> String {
        self.format(ctx, level, id, message, timestamp)
    }
}

fn build_hammer_line(
    ctx: Option<ConnId>,
    level: Level,
    id: &str,
    message: &str,
    elapsed: Duration,
) -> String {
    let mut s = String::with_capacity(message.len() + 64);
    s.push_str("H[");
    s.push_str(level.platform_code());
    s.push_str("] +");
    push_elapsed(&mut s, elapsed);
    if let Some(id) = ctx {
        s.push_str(" c#");
        push_hex(&mut s, id.short(), 4);
    }
    if !id.is_empty() {
        s.push(' ');
        s.push_str(&display_id(id));
        s.push(':');
    }
    s.push(' ');
    s.push_str(message);
    s
}

fn apply_line_break(message: String, strip: bool) -> String {
    if strip {
        if message.ends_with('\n') {
            let mut m = message;
            m.pop();
            m
        } else {
            message
        }
    } else if message.ends_with('\n') {
        message
    } else {
        let mut m = message;
        m.push('\n');
        m
    }
}

pub fn display_id(id: &str) -> String {
    let id = id
        .strip_prefix("hammer_runtime::")
        .or_else(|| id.strip_prefix("hammer_core::"))
        .unwrap_or(id);
    match id {
        "router" => return "path".into(),
        _ => {}
    }
    if let Some(rest) = id.strip_prefix("outbound/") {
        return match rest {
            other => format!("egress.{other}"),
        };
    }
    if let Some(rest) = id.strip_prefix("inbound/") {
        return if rest == "tun" {
            "tun.in".into()
        } else {
            format!("ingress.{rest}")
        };
    }
    id.to_owned()
}

fn push_elapsed(out: &mut String, duration: Duration) {
    use std::fmt::Write;
    let _ = write!(
        out,
        "{}.{:03}s",
        duration.as_secs(),
        duration.subsec_millis()
    );
}

fn push_hex(out: &mut String, value: u16, width: usize) {
    use std::fmt::Write;
    let _ = write!(out, "{value:0width$x}");
}
