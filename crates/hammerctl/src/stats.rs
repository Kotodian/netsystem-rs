//! `hammerctl stats` — list and dump live stats entries through the checked
//! [`StatsClient`] (hammer-ipc), one connection per CLI invocation.
//!
//! Output is a stable, tab-separated, headerless record stream (one line per
//! entry), so an empty result prints zero bytes. No snapshot, cache, retry,
//! or worker-barrier interaction: `list` runs the server's transient list
//! once; `dump` lists once and, only when entries exist, dumps exactly once.

use std::io::{self, Write};
use std::path::Path;

use clap::Subcommand;
use hammer_ipc::stats::{
    ConstLabel, DirectoryEntry, DirectoryType, DumpEntry, DumpValue, EntryId, PrometheusType,
    StatsClient, StatsClientError,
};

/// `hammerctl stats` subcommands.
#[derive(Subcommand, Debug)]
pub(crate) enum StatsCommand {
    /// List the directory entries matching any pattern (empty selects all)
    List {
        /// Regex patterns; entries matching any pattern are listed
        #[arg(value_name = "PATTERN")]
        patterns: Vec<String>,
    },
    /// Dump the live values of entries matching any pattern
    Dump {
        /// Regex patterns; entries matching any pattern are dumped
        #[arg(value_name = "PATTERN")]
        patterns: Vec<String>,
    },
}

/// Connects once and executes `command`, writing one output line per record
/// to `output`. A broken pipe ends the run quietly with success, matching
/// the Unix filter convention for a consumer that went away early (as with
/// `hammerctl stats list | head`); any other output failure is a typed
/// [`RunError::Output`].
pub(crate) fn run(
    socket: &Path,
    command: &StatsCommand,
    output: &mut dyn io::Write,
) -> Result<(), RunError> {
    let mut client = StatsClient::connect(socket)?;
    let lines = match command {
        StatsCommand::List { patterns } => list(&mut client, patterns)?,
        StatsCommand::Dump { patterns } => dump(&mut client, patterns)?,
    };
    write_lines(output, &lines)
}

/// Errors from running a stats command: the client call, or writing the
/// result lines. A broken pipe is deliberately not an error.
#[derive(Debug)]
pub(crate) enum RunError {
    Client(StatsClientError),
    Output(io::Error),
}

impl From<StatsClientError> for RunError {
    fn from(error: StatsClientError) -> RunError {
        RunError::Client(error)
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Client(error) => std::fmt::Display::fmt(error, f),
            RunError::Output(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RunError::Client(error) => Some(error),
            RunError::Output(error) => Some(error),
        }
    }
}

/// Writes each line followed by a newline, byte-for-byte the previous
/// `println!` output. A broken pipe is a quiet success; any other write
/// failure is [`RunError::Output`].
fn write_lines(output: &mut dyn io::Write, lines: &[String]) -> Result<(), RunError> {
    for line in lines {
        match writeln!(output, "{line}") {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => return Ok(()),
            Err(error) => return Err(RunError::Output(error)),
        }
    }
    Ok(())
}

/// Lists entries exactly once and formats one line per entry.
fn list(client: &mut StatsClient, patterns: &[String]) -> Result<Vec<String>, StatsClientError> {
    let entries = client.list(patterns)?;
    Ok(entries.iter().map(format_list_entry).collect())
}

/// Lists exactly once, then dumps exactly once only when entries exist: an
/// empty list short-circuits to no output without a dump call.
fn dump(client: &mut StatsClient, patterns: &[String]) -> Result<Vec<String>, StatsClientError> {
    let entries = client.list(patterns)?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<EntryId> = entries.iter().map(|entry| entry.id).collect();
    let values = client.dump(&ids)?;
    Ok(values.iter().map(format_dump_entry).collect())
}

fn format_list_entry(entry: &DirectoryEntry) -> String {
    format!(
        "{}:{}\t{}\t{}\t{}\t{}\t{}\t{}",
        entry.id.index(),
        entry.id.generation(),
        escape_free_text(&entry.path),
        directory_spelling(entry.directory_type),
        prometheus_spelling(entry.prometheus_type),
        escape_free_text(&entry.fq_name),
        escape_free_text(&entry.help),
        format_labels(&entry.const_labels),
    )
}

fn format_dump_entry(entry: &DumpEntry) -> String {
    let value = match entry.value {
        DumpValue::Counter(value) => format!("counter:{value}"),
        DumpValue::Gauge(value) => format!("gauge:{value}"),
    };
    format!(
        "{}:{}\t{}\t{}\t{}\t{}",
        entry.id.index(),
        entry.id.generation(),
        escape_free_text(&entry.path),
        directory_spelling(entry.directory_type),
        prometheus_spelling(entry.prometheus_type),
        value,
    )
}

/// Stable lowercase spellings of the VPP directory types (shared.h).
fn directory_spelling(kind: DirectoryType) -> &'static str {
    match kind {
        DirectoryType::Illegal => "illegal",
        DirectoryType::ScalarIndex => "scalar",
        DirectoryType::CounterVectorSimple => "counter-vector-simple",
        DirectoryType::CounterVectorCombined => "counter-vector-combined",
        DirectoryType::NameVector => "name-vector",
        DirectoryType::Empty => "empty",
        DirectoryType::Symlink => "symlink",
        DirectoryType::HistogramLog2 => "histogram-log2",
        DirectoryType::RingBuffer => "ring-buffer",
        DirectoryType::Gauge => "gauge",
    }
}

fn prometheus_spelling(kind: PrometheusType) -> &'static str {
    match kind {
        PrometheusType::Counter => "counter",
        PrometheusType::Gauge => "gauge",
    }
}

/// Escapes free text so one record stays on one line: backslash, tab,
/// newline, carriage return, in that order.
fn escape_free_text(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// Escapes a label name or value: free-text escaping plus comma and equals.
fn escape_label(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            ',' => escaped.push_str("\\,"),
            '=' => escaped.push_str("\\="),
            other => escaped.push(other),
        }
    }
    escaped
}

/// Labels retain DTO order and render `escaped_name=escaped_value` joined by
/// comma; no labels render an empty column.
fn format_labels(labels: &[ConstLabel]) -> String {
    let mut rendered = String::new();
    for (position, label) in labels.iter().enumerate() {
        if position > 0 {
            rendered.push(',');
        }
        rendered.push_str(&escape_label(&label.name));
        rendered.push('=');
        rendered.push_str(&escape_label(&label.value));
    }
    rendered
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;

    use hammer_ipc::binary_api::{BinaryApiReply, BinaryApiRequest, BinaryApiStatus};
    use hammer_ipc::stats::StatsServerError;
    use hammer_ipc::stats::wire;
    use prost::Message;

    use super::*;

    static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    /// A scripted Binary API server that answers `stats.list` and
    /// `stats.dump` with the configured entries and records the methods it
    /// saw, in call order.
    struct FakeStatsServer {
        path: PathBuf,
        calls: Arc<Mutex<Vec<String>>>,
        list_entries: Vec<wire::ListEntry>,
        dump_entries: Vec<wire::DumpEntry>,
        list_error: Option<wire::ErrorReply>,
    }

    impl FakeStatsServer {
        fn new(list_entries: Vec<wire::ListEntry>, dump_entries: Vec<wire::DumpEntry>) -> Self {
            Self {
                path: fake_socket_path(),
                calls: Arc::new(Mutex::new(Vec::new())),
                list_entries,
                dump_entries,
                list_error: None,
            }
        }

        fn start(&self) {
            let _ = std::fs::remove_file(&self.path);
            let listener = UnixListener::bind(&self.path).expect("bind fake stats server");
            let calls = self.calls.clone();
            let list_entries = self.list_entries.clone();
            let dump_entries = self.dump_entries.clone();
            let list_error = self.list_error.clone();
            thread::spawn(move || {
                let (mut stream, _) = listener.accept().expect("accept fake client");
                while let Some(request) = read_request(&mut stream) {
                    calls
                        .lock()
                        .expect("fake server call log")
                        .push(request.method.clone());
                    let payload = match request.method.as_str() {
                        "stats.list" => match &list_error {
                            Some(error) => wire::ListReply {
                                result: Some(wire::list_reply::Result::Error(error.clone())),
                            },
                            None => wire::ListReply {
                                result: Some(wire::list_reply::Result::Entries(
                                    wire::ListEntries {
                                        entries: list_entries.clone(),
                                    },
                                )),
                            },
                        }
                        .encode_to_vec(),
                        "stats.dump" => wire::DumpReply {
                            result: Some(wire::dump_reply::Result::Entries(wire::DumpEntries {
                                entries: dump_entries.clone(),
                            })),
                        }
                        .encode_to_vec(),
                        _ => continue,
                    };
                    write_reply(
                        &mut stream,
                        &BinaryApiReply {
                            context: request.context,
                            status: BinaryApiStatus::Ok as i32,
                            payload,
                        },
                    );
                }
            });
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("fake server call log").clone()
        }
    }

    fn fake_socket_path() -> PathBuf {
        let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "hammerctl-fake-{}-{sequence}.sock",
            std::process::id()
        ))
    }

    fn read_request(stream: &mut UnixStream) -> Option<BinaryApiRequest> {
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).ok()?;
        let mut frame = vec![0_u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut frame).ok()?;
        BinaryApiRequest::decode(frame.as_slice()).ok()
    }

    fn write_reply(stream: &mut UnixStream, reply: &BinaryApiReply) {
        let frame = reply.encode_to_vec();
        stream
            .write_all(&(frame.len() as u32).to_be_bytes())
            .expect("write reply length");
        stream.write_all(&frame).expect("write reply");
    }

    fn wire_entry(
        index: u32,
        generation: u64,
        path: &str,
        fq_name: &str,
        directory_type: wire::DirectoryType,
        prometheus_type: wire::PrometheusType,
    ) -> wire::ListEntry {
        wire::ListEntry {
            id: Some(wire::EntryId { index, generation }),
            path: path.to_owned(),
            directory_type: directory_type as i32,
            prometheus_type: prometheus_type as i32,
            fq_name: fq_name.to_owned(),
            help: String::new(),
            const_labels: Vec::new(),
        }
    }

    fn wire_dump(
        index: u32,
        generation: u64,
        path: &str,
        directory_type: wire::DirectoryType,
        prometheus_type: wire::PrometheusType,
        value: wire::value::Value,
    ) -> wire::DumpEntry {
        wire::DumpEntry {
            id: Some(wire::EntryId { index, generation }),
            path: path.to_owned(),
            directory_type: directory_type as i32,
            prometheus_type: prometheus_type as i32,
            value: Some(wire::Value { value: Some(value) }),
        }
    }

    #[test]
    fn directory_and_prometheus_spellings_are_stable() {
        assert_eq!(directory_spelling(DirectoryType::Illegal), "illegal");
        assert_eq!(directory_spelling(DirectoryType::ScalarIndex), "scalar");
        assert_eq!(
            directory_spelling(DirectoryType::CounterVectorSimple),
            "counter-vector-simple"
        );
        assert_eq!(
            directory_spelling(DirectoryType::CounterVectorCombined),
            "counter-vector-combined"
        );
        assert_eq!(directory_spelling(DirectoryType::NameVector), "name-vector");
        assert_eq!(directory_spelling(DirectoryType::Empty), "empty");
        assert_eq!(directory_spelling(DirectoryType::Symlink), "symlink");
        assert_eq!(
            directory_spelling(DirectoryType::HistogramLog2),
            "histogram-log2"
        );
        assert_eq!(directory_spelling(DirectoryType::RingBuffer), "ring-buffer");
        assert_eq!(directory_spelling(DirectoryType::Gauge), "gauge");
        assert_eq!(prometheus_spelling(PrometheusType::Counter), "counter");
        assert_eq!(prometheus_spelling(PrometheusType::Gauge), "gauge");
    }

    #[test]
    fn list_lines_escape_free_text_exactly() {
        let entry = DirectoryEntry {
            id: EntryId::new(3, 7).expect("id"),
            path: "/sys/a\\b\tc\nd\re".to_owned(),
            directory_type: DirectoryType::Gauge,
            prometheus_type: PrometheusType::Gauge,
            fq_name: "fq\\x\ty\nz\rw".to_owned(),
            help: "h".to_owned(),
            const_labels: Vec::new(),
        };
        assert_eq!(
            format_list_entry(&entry),
            "3:7\t/sys/a\\\\b\\tc\\nd\\re\tgauge\tgauge\tfq\\\\x\\ty\\nz\\rw\th\t"
        );
    }

    #[test]
    fn labels_escape_comma_equals_and_free_text_in_order() {
        let entry = DirectoryEntry {
            id: EntryId::new(1, 1).expect("id"),
            path: "/a".to_owned(),
            directory_type: DirectoryType::ScalarIndex,
            prometheus_type: PrometheusType::Counter,
            fq_name: "a_total".to_owned(),
            help: String::new(),
            const_labels: vec![
                ConstLabel {
                    name: "k,ey=1".to_owned(),
                    value: "v\\a,b=c".to_owned(),
                },
                ConstLabel {
                    name: "plain".to_owned(),
                    value: "value".to_owned(),
                },
            ],
        };
        assert_eq!(
            format_list_entry(&entry),
            "1:1\t/a\tscalar\tcounter\ta_total\t\tk\\,ey\\=1=v\\\\a\\,b\\=c,plain=value"
        );
    }

    #[test]
    fn dump_lines_render_typed_values() {
        let counter = DumpEntry {
            id: EntryId::new(0, 1).expect("id"),
            path: "/sys/heartbeat".to_owned(),
            directory_type: DirectoryType::ScalarIndex,
            prometheus_type: PrometheusType::Counter,
            value: DumpValue::Counter(42),
        };
        assert_eq!(
            format_dump_entry(&counter),
            "0:1\t/sys/heartbeat\tscalar\tcounter\tcounter:42"
        );

        let gauge = DumpEntry {
            id: EntryId::new(1, 1).expect("id"),
            path: "/sys/boottime".to_owned(),
            directory_type: DirectoryType::ScalarIndex,
            prometheus_type: PrometheusType::Gauge,
            value: DumpValue::Gauge(3.5),
        };
        assert_eq!(
            format_dump_entry(&gauge),
            "1:1\t/sys/boottime\tscalar\tgauge\tgauge:3.5"
        );
    }

    #[test]
    fn list_calls_the_server_exactly_once() {
        let server = FakeStatsServer::new(
            vec![wire_entry(
                0,
                1,
                "/sys/heartbeat",
                "hammer_sys_heartbeat_total",
                wire::DirectoryType::ScalarIndex,
                wire::PrometheusType::Counter,
            )],
            vec![],
        );
        server.start();
        let mut client = StatsClient::connect(&server.path).expect("connect fake server");
        let lines = list(&mut client, &["/.*".to_owned()]).expect("list");

        assert_eq!(
            lines,
            vec!["0:1\t/sys/heartbeat\tscalar\tcounter\thammer_sys_heartbeat_total\t\t"]
        );
        assert_eq!(server.calls(), vec!["stats.list"]);
    }

    #[test]
    fn dump_short_circuits_when_the_list_is_empty() {
        let server = FakeStatsServer::new(vec![], vec![]);
        server.start();
        let mut client = StatsClient::connect(&server.path).expect("connect fake server");
        let lines = dump(&mut client, &[]).expect("dump");

        assert!(lines.is_empty());
        assert_eq!(server.calls(), vec!["stats.list"]);
    }

    #[test]
    fn dump_lists_once_then_dumps_once_preserving_order_and_duplicates() {
        let server = FakeStatsServer::new(
            vec![
                wire_entry(
                    1,
                    2,
                    "/b",
                    "b_total",
                    wire::DirectoryType::CounterVectorSimple,
                    wire::PrometheusType::Counter,
                ),
                wire_entry(
                    0,
                    1,
                    "/a",
                    "a_total",
                    wire::DirectoryType::ScalarIndex,
                    wire::PrometheusType::Gauge,
                ),
            ],
            vec![
                wire_dump(
                    1,
                    2,
                    "/b",
                    wire::DirectoryType::CounterVectorSimple,
                    wire::PrometheusType::Counter,
                    wire::value::Value::Counter(7),
                ),
                wire_dump(
                    1,
                    2,
                    "/b",
                    wire::DirectoryType::CounterVectorSimple,
                    wire::PrometheusType::Counter,
                    wire::value::Value::Counter(7),
                ),
                wire_dump(
                    0,
                    1,
                    "/a",
                    wire::DirectoryType::ScalarIndex,
                    wire::PrometheusType::Gauge,
                    wire::value::Value::Gauge(1.5),
                ),
            ],
        );
        server.start();
        let mut client = StatsClient::connect(&server.path).expect("connect fake server");
        let lines = dump(&mut client, &["/.*".to_owned()]).expect("dump");

        assert_eq!(
            lines,
            vec![
                "1:2\t/b\tcounter-vector-simple\tcounter\tcounter:7",
                "1:2\t/b\tcounter-vector-simple\tcounter\tcounter:7",
                "0:1\t/a\tscalar\tgauge\tgauge:1.5",
            ]
        );
        assert_eq!(server.calls(), vec!["stats.list", "stats.dump"]);
    }

    #[test]
    fn server_errors_surface_as_typed_client_errors() {
        let mut server = FakeStatsServer::new(vec![], vec![]);
        server.list_error = Some(wire::ErrorReply {
            error: Some(wire::error_oneof::Error::InvalidPattern(
                wire::InvalidPatternError {
                    pattern: "(".to_owned(),
                },
            )),
        });
        server.start();
        let mut client = StatsClient::connect(&server.path).expect("connect fake server");
        let error = list(&mut client, &["(".to_owned()]).expect_err("invalid pattern");

        match error {
            StatsClientError::Server { method, source } => {
                assert_eq!(method, "stats.list");
                assert!(matches!(
                    source,
                    StatsServerError::InvalidPattern { pattern } if pattern == "("
                ));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    /// Test writer that accepts writes until the first complete line, then
    /// fails every later write with `BrokenPipe`, modelling
    /// `hammerctl stats list | head` closing the pipe early.
    struct FirstLineThenBrokenPipe {
        written: Vec<u8>,
        closed: bool,
    }

    impl FirstLineThenBrokenPipe {
        fn new() -> FirstLineThenBrokenPipe {
            FirstLineThenBrokenPipe {
                written: Vec::new(),
                closed: false,
            }
        }

        fn written(&self) -> &[u8] {
            &self.written
        }
    }

    impl io::Write for FirstLineThenBrokenPipe {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            if self.closed {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"));
            }
            self.written.extend_from_slice(data);
            if self.written.ends_with(b"\n") {
                self.closed = true;
            }
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// `run` writes the formatted lines byte-for-byte through the injected
    /// writer — the same record bytes the old `println!` path produced.
    #[test]
    fn run_preserves_exact_output_lines() {
        let server = FakeStatsServer::new(
            vec![
                wire_entry(
                    0,
                    1,
                    "/a",
                    "a_total",
                    wire::DirectoryType::ScalarIndex,
                    wire::PrometheusType::Counter,
                ),
                wire_entry(
                    1,
                    1,
                    "/b",
                    "b_total",
                    wire::DirectoryType::ScalarIndex,
                    wire::PrometheusType::Gauge,
                ),
            ],
            vec![],
        );
        server.start();
        let command = StatsCommand::List {
            patterns: vec!["/.*".to_owned()],
        };

        let mut output: Vec<u8> = Vec::new();
        run(&server.path, &command, &mut output).expect("full run succeeds");
        assert_eq!(
            output,
            b"0:1\t/a\tscalar\tcounter\ta_total\t\t\n1:1\t/b\tscalar\tgauge\tb_total\t\t\n"
        );
    }

    /// A consumer that goes away after the first line ends the run quietly
    /// with success and the first line preserved — never a panic or a
    /// failure status.
    #[test]
    fn run_treats_broken_pipe_as_quiet_success_after_the_first_line() {
        let server = FakeStatsServer::new(
            vec![
                wire_entry(
                    0,
                    1,
                    "/a",
                    "a_total",
                    wire::DirectoryType::ScalarIndex,
                    wire::PrometheusType::Counter,
                ),
                wire_entry(
                    1,
                    1,
                    "/b",
                    "b_total",
                    wire::DirectoryType::ScalarIndex,
                    wire::PrometheusType::Gauge,
                ),
            ],
            vec![],
        );
        server.start();
        let command = StatsCommand::List {
            patterns: vec!["/.*".to_owned()],
        };

        let mut output = FirstLineThenBrokenPipe::new();
        run(&server.path, &command, &mut output).expect("broken pipe is a quiet success");
        assert_eq!(output.written(), b"0:1\t/a\tscalar\tcounter\ta_total\t\t\n");
    }
}
