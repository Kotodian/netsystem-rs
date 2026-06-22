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
        .or_else(|| id.strip_prefix("hammer_ffi::"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn fixture(extra: Duration) -> (Formatter, Instant) {
        let base = Instant::now();
        (
            Formatter {
                base_time: base,
                disable_line_break: true,
            },
            base + extra,
        )
    }

    #[test]
    fn format_platform_uses_hammer_style() {
        let (formatter, ts) = fixture(Duration::from_millis(37));
        let id = ConnId {
            id: 0x123e,
            created_at: formatter.base_time,
        };
        let got =
            formatter.format_platform(Some(id), Level::Info, "inbound/tun", "started at utun4", ts);
        assert_eq!(got, "H[I] +0.037s c#123e tun.in: started at utun4");
    }

    #[test]
    fn format_platform_elapsed_seconds() {
        let (formatter, ts) = fixture(Duration::from_millis(12_345));
        let got =
            formatter.format_platform(None, Level::Debug, "router", "final outbound is hy2", ts);
        assert_eq!(got, "H[D] +12.345s path: final outbound is hy2");
    }

    #[test]
    fn format_platform_without_id() {
        let (formatter, ts) = fixture(Duration::from_millis(1));
        let got = formatter.format_platform(None, Level::Warn, "", "started", ts);
        assert_eq!(got, "H[W] +0.001s started");
    }

    #[test]
    fn format_platform_strips_sing_box_residue() {
        let (formatter, ts) = fixture(Duration::from_secs(2));
        let id = ConnId {
            id: 0xdead_0042,
            created_at: formatter.base_time,
        };
        let got = formatter.format_platform(Some(id), Level::Error, "handshake failed", ts);
        assert!(!got.contains("\x1b["), "got = {got}");
        assert!(
            !got.contains("INFO[") && !got.contains("ERROR["),
            "got = {got}"
        );
        assert!(!got.contains("outbound/selector"), "got = {got}");
        assert!(got.contains("egress.selector:"), "got = {got}");
        assert!(got.contains("c#0042"), "got = {got}");
    }

    #[test]
    fn display_id_table() {
        let cases = [
            ("router", "path"),
            ("outbound/block", "egress.block"),
            ("outbound/selector", "egress.selector"),
            ("inbound/tun", "tun.in"),
            ("inbound/mixed", "ingress.mixed"),
            ("certificate-provider", "certificate-provider"),
        ];
        for (input, want) in cases {
            assert_eq!(display_id(input), want, "input = {input}");
        }
    }

    #[test]
    fn format_adds_line_break() {
        let base = Instant::now();
        let formatter = Formatter::new(base);
        let got = formatter.format(None, Level::Info, "router", "started", base);
        assert!(got.ends_with('\n'), "got = {got:?}");
    }

    #[test]
    fn format_does_not_double_line_break() {
        let base = Instant::now();
        let formatter = Formatter::new(base);
        let got = formatter.format(None, Level::Info, "router", "already\n", base);
        assert!(got.ends_with('\n'));
        assert!(!got.ends_with("\n\n"));
    }

    #[test]
    fn elapsed_negative_treated_as_zero() {
        let base = Instant::now();
        let formatter = Formatter {
            base_time: base + Duration::from_secs(10),
            disable_line_break: true,
        };
        let got = formatter.format(None, Level::Info, "router", "msg", base);
        assert!(got.starts_with("H[I] +0.000s "), "got = {got}");
    }
}
