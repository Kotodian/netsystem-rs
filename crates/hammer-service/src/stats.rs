//! Stats capability: the `[stats]` configuration, the engine's
//! [`StatsCapability`] (system metrics and the stats segment), the
//! `stats-collector` Process Node, and the `stats.list` / `stats.dump`
//! Binary API handlers.
//!
//! The design mirrors VPP: the three `/sys` scalars of `stats.h:22-24`
//! (heartbeat, last-stats-clear, boottime), one collector pass per interval
//! (`do_stat_segment_updates`, collector.c:132-151), and boottime set once
//! before the suspend loop (`stat_segment_collector_process`,
//! collector.c:153-180).

use std::sync::Arc;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

use hammer_ipc::stats::wire;
use hammer_runtime::{Engine, ProcessContext, ProcessWake, RuntimeError, RuntimeResult};
use hammer_stats::{
    DEFAULT_CAPACITY, DirectoryType, EntryId, PrometheusType, StatsError, StatsMain,
};

/// Default collector cadence, matching VPP's default `update_interval`
/// (collector.c:177).
const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

/// VPP's three system scalars (`stats.h:22-24`) are published under `/sys`
/// (stats.c:281): the heartbeat bumps once per collector pass, boottime is
/// set once at Process Node start, and last-stats-clear keeps its initial
/// zero because #246 has no clear command.
const SYS_HEARTBEAT_PATH: &str = "/sys/heartbeat";
const SYS_BOOTTIME_PATH: &str = "/sys/boottime";
const SYS_LAST_STATS_CLEAR_PATH: &str = "/sys/last_stats_clear";

/// `[stats]` configuration.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StatsConfig {
    /// Shared-segment capacity in bytes; the minimum is enforced by
    /// [`StatsMain::with_capacity`].
    segment_capacity: usize,
    /// Collector cadence; must be non-zero.
    #[serde(with = "humantime_serde")]
    update_interval: Duration,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            segment_capacity: DEFAULT_CAPACITY,
            update_interval: DEFAULT_UPDATE_INTERVAL,
        }
    }
}

impl StatsConfig {
    fn validate(&self) -> RuntimeResult<()> {
        if self.update_interval.is_zero() {
            return Err(RuntimeError::config_validation(
                "stats.update_interval must be non-zero",
            ));
        }
        Ok(())
    }
}

/// Typed stats subsystem errors, converted to `RuntimeError::Subsystem` with
/// subsystem `"stats"` by the `runtime_error` macro.
///
/// This is the service-local error type, distinct from `hammer_ipc`'s checked
/// remote `StatsServerError` domain.
#[hammer_component_macros::runtime_error(subsystem = "stats")]
#[derive(Debug, thiserror::Error)]
enum StatsServiceError {
    /// A stats segment operation failed.
    #[error(transparent)]
    Stats(#[from] StatsError),
    /// The main Engine does not have its installed stats segment.
    #[error("the Main Thread stats segment is unavailable")]
    StatsMainUnavailable,
    /// The Main Thread stats capability could not be installed. Zero-data: a
    /// failed claim on a fresh slot carries no deeper reason.
    #[error("the Main Thread stats capability could not be installed")]
    InstallFailed,
    /// The system clock could not be read for the boottime.
    #[error("failed to read the system clock for the stats boottime")]
    SystemTime(#[source] SystemTimeError),
}

/// Service-side stats configuration and scalar handles.
///
/// The structural `StatsMain` owner lives on the main [`Engine`]. This
/// capability remains registry-safe because it retains only the cadence and
/// direct shared-segment scalar handles used by the collector.
struct StatsCapability {
    /// Collector cadence from `[stats]`.
    update_interval: Duration,
    /// `/sys/boottime` handle, set once at Process Node start.
    boottime: hammer_stats::Timestamp,
}

impl StatsCapability {
    /// Builds the segment, registers the VPP system scalars and the heartbeat
    /// collector, installs the segment on the main Engine, and returns the
    /// service capability to be registered.
    fn install(engine: &mut Engine, config: &StatsConfig) -> Result<Arc<Self>, StatsServiceError> {
        let mut stats =
            StatsMain::with_capacity(config.segment_capacity).map_err(StatsServiceError::Stats)?;
        // VPP heartbeat: a scalar bumped once per collector pass
        // (collector.c:149-150). The registered collector owns the handle
        // and updates it only via `Counter::increment`.
        let (_, heartbeat) = stats
            .add_counter(
                SYS_HEARTBEAT_PATH,
                prometheus::Opts::new(
                    "hammer_sys_heartbeat_total",
                    "collector passes since the engine started",
                ),
            )
            .map_err(StatsServiceError::Stats)?;
        stats.register_collector(move || heartbeat.increment().map(|_| ()));
        let (_, boottime) = stats
            .add_timestamp(
                SYS_BOOTTIME_PATH,
                prometheus::Opts::new(
                    "hammer_sys_boottime_seconds",
                    "Unix epoch seconds when the stats collector started",
                ),
            )
            .map_err(StatsServiceError::Stats)?;
        // No stats-clear command exists in #246: the scalar keeps VPP's
        // initial zero (stats.h:23, stats.c:281). The handle is dropped; the
        // entry stays live in the segment.
        let (_, last_stats_clear) = stats
            .add_timestamp(
                SYS_LAST_STATS_CLEAR_PATH,
                prometheus::Opts::new(
                    "hammer_sys_last_stats_clear_seconds",
                    "Unix epoch seconds of the last stats clear; zero until a clear exists",
                ),
            )
            .map_err(StatsServiceError::Stats)?;
        drop(last_stats_clear);

        engine
            .install_stats_main(stats)
            .map_err(|_| StatsServiceError::InstallFailed)?;
        Ok(Arc::new(Self {
            update_interval: config.update_interval,
            boottime,
        }))
    }

    /// Collector cadence for the Process Node loop.
    fn update_interval(&self) -> Duration {
        self.update_interval
    }

    /// One-shot boottime publication at Process Node start (VPP
    /// collector.c:172).
    fn set_boottime(&self, seconds: u64) -> Result<(), StatsError> {
        self.boottime.set(seconds)
    }
}

/// `[stats]` configuration function, registered early so the init function
/// can require it.
#[hammer_component_macros::config_function(name = "stats_config", section = "stats", early = true)]
fn configure(config: StatsConfig) -> RuntimeResult<Arc<StatsConfig>> {
    config.validate()?;
    Ok(Arc::new(config))
}

/// Builds and registers the engine's [`StatsCapability`].
#[hammer_component_macros::init_function(name = "stats_init")]
fn init(
    engine: &mut Engine,
    config: Arc<StatsConfig>,
) -> RuntimeResult<Option<Arc<StatsCapability>>> {
    Ok(Some(StatsCapability::install(engine, &config)?))
}

/// Collector Process Node, mirroring VPP's `stat_segment_collector_process`
/// (collector.c:153-180): boottime is set once, the first pass runs
/// immediately, then one pass per `update_interval`; the heartbeat scalar is
/// bumped by the registered collector on every pass.
#[hammer_component_macros::process_node(name = "stats-collector")]
async fn stats_collector(mut context: ProcessContext) -> RuntimeResult<()> {
    let capability = context.require::<StatsCapability>()?;
    // Boottime publication is a startup invariant (VPP collector.c:172): a
    // broken clock read or segment write terminates the node instead of
    // running passes against an unpublished boottime.
    let boottime = unix_seconds_now().map_err(StatsServiceError::SystemTime)?;
    capability
        .set_boottime(boottime)
        .map_err(StatsServiceError::Stats)?;
    run_collect_pass()?;
    loop {
        match context
            .wait_for_event_or_clock(capability.update_interval())
            .await
        {
            ProcessWake::Clock => {}
            ProcessWake::Event(batch) => tracing::debug!(
                event_type = batch.event_type(),
                "stats-collector woke on an unexpected event; collecting anyway"
            ),
        }
        run_collect_pass()?;
    }
}

/// Runs one operation against the main Engine's structural stats owner.
///
/// The current-thread Engine pointer already identifies the owner; this path
/// deliberately does not synchronize through the Worker Barrier.
fn with_current_stats<R>(
    operation: impl FnOnce(&mut StatsMain) -> Result<R, StatsError>,
) -> Result<R, StatsServiceError> {
    Engine::with_current(|engine| engine.with_stats_main(operation))
        .flatten()
        .ok_or(StatsServiceError::StatsMainUnavailable)?
        .map_err(StatsServiceError::Stats)
}

/// One collector pass (VPP `do_stat_segment_updates`, collector.c:132-151):
/// a failing collector is logged at error level and the pass continues; only
/// an access failure (owner gone) is fatal for the node.
fn run_collect_pass() -> RuntimeResult<()> {
    match with_current_stats(|stats| stats.collect()) {
        Ok(()) => Ok(()),
        Err(StatsServiceError::Stats(error)) => {
            tracing::error!(%error, "stats collector pass reported an error; continuing");
            Ok(())
        }
        Err(error) => Err(RuntimeError::from(error)),
    }
}

/// Current Unix time in whole seconds (VPP `unix_time_now`, collector.c:172).
fn unix_seconds_now() -> Result<u64, SystemTimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
}

/// `stats.list`: returns the directory entries matching any pattern (empty
/// selects all), in ascending directory-index order.
#[hammer_component_macros::binary_api(name = "stats.list", mp_safe)]
fn stats_list(request: wire::ListRequest) -> wire::ListReply {
    match with_current_stats(|stats| stats.list(&request.patterns)) {
        Ok(entries) => wire::ListReply {
            result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                entries: entries.into_iter().map(to_wire_list_entry).collect(),
            })),
        },
        Err(StatsServiceError::Stats(error)) => list_error(error),
        Err(_) => list_internal_error(),
    }
}

/// `stats.dump`: a point-in-time copy of the requested entries, preserving
/// input order and duplicates (VPP `stat_segment_dump`).
#[hammer_component_macros::binary_api(name = "stats.dump", mp_safe)]
fn stats_dump(request: wire::DumpRequest) -> wire::DumpReply {
    // Checked id conversion runs before any segment access: generation 0
    // never names a published entry.
    let ids = match request
        .ids
        .iter()
        .map(checked_wire_id)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(ids) => ids,
        Err(error) => return dump_error(error),
    };
    match with_current_stats(|stats| stats.dump(&ids)) {
        Ok(entries) => wire::DumpReply {
            result: Some(wire::dump_reply::Result::Entries(wire::DumpEntries {
                entries: entries.into_iter().map(to_wire_dump_entry).collect(),
            })),
        },
        Err(StatsServiceError::Stats(error)) => dump_error(error),
        Err(_) => dump_internal_error(),
    }
}

fn list_error(error: StatsError) -> wire::ListReply {
    wire::ListReply {
        result: Some(wire::list_reply::Result::Error(wire::ErrorReply {
            error: Some(wire_error(error)),
        })),
    }
}

fn list_internal_error() -> wire::ListReply {
    wire::ListReply {
        result: Some(wire::list_reply::Result::Error(wire::ErrorReply {
            error: Some(wire::error_oneof::Error::Internal(wire::Empty {})),
        })),
    }
}

fn dump_error(error: StatsError) -> wire::DumpReply {
    wire::DumpReply {
        result: Some(wire::dump_reply::Result::Error(wire::ErrorReply {
            error: Some(wire_error(error)),
        })),
    }
}

fn dump_internal_error() -> wire::DumpReply {
    wire::DumpReply {
        result: Some(wire::dump_reply::Result::Error(wire::ErrorReply {
            error: Some(wire::error_oneof::Error::Internal(wire::Empty {})),
        })),
    }
}

/// Checked wire-to-domain id conversion: generation 0 is rejected because
/// it never names a published entry.
fn checked_wire_id(id: &wire::EntryId) -> Result<EntryId, StatsError> {
    EntryId::try_from((id.index, id.generation))
}

/// Maps a stats domain error to the in-band wire error: the exact pattern
/// (never the regex source), the full entry id for not-found/stale/
/// incompatible, `ReadBusy` as-is, and every segment/init/corruption/access
/// error as `Internal`.
fn wire_error(error: StatsError) -> wire::error_oneof::Error {
    use wire::error_oneof::Error as WireError;
    match error {
        StatsError::InvalidPattern { pattern, .. } => {
            WireError::InvalidPattern(wire::InvalidPatternError { pattern })
        }
        StatsError::NotFound { id } => WireError::NotFound(wire::EntryError {
            id: Some(to_wire_id(id)),
        }),
        StatsError::StaleEntry { id } => WireError::StaleEntry(wire::EntryError {
            id: Some(to_wire_id(id)),
        }),
        StatsError::ReadBusy => WireError::ReadBusy(wire::Empty {}),
        StatsError::IncompatibleType {
            id,
            prometheus_type,
            directory_type,
        } => WireError::IncompatibleType(wire::IncompatibleTypeError {
            id: Some(to_wire_id(id)),
            directory_type: wire_directory_type(directory_type),
            prometheus_type: wire_prometheus_type(prometheus_type),
        }),
        _ => WireError::Internal(wire::Empty {}),
    }
}

/// The wire discriminant of a directory type. Both enums share VPP's stable
/// `stat_directory_type_t` values (shared.h:8-20), but the mapping is
/// explicit and exhaustive so a variant added or renumbered on either
/// domain fails to compile here instead of silently crossing the wire.
fn wire_directory_type(kind: DirectoryType) -> i32 {
    match kind {
        DirectoryType::Illegal => wire::DirectoryType::Illegal as i32,
        DirectoryType::ScalarIndex => wire::DirectoryType::ScalarIndex as i32,
        DirectoryType::CounterVectorSimple => wire::DirectoryType::CounterVectorSimple as i32,
        DirectoryType::CounterVectorCombined => wire::DirectoryType::CounterVectorCombined as i32,
        DirectoryType::NameVector => wire::DirectoryType::NameVector as i32,
        DirectoryType::Empty => wire::DirectoryType::Empty as i32,
        DirectoryType::Symlink => wire::DirectoryType::Symlink as i32,
        DirectoryType::HistogramLog2 => wire::DirectoryType::HistogramLog2 as i32,
        DirectoryType::RingBuffer => wire::DirectoryType::RingBuffer as i32,
        DirectoryType::Gauge => wire::DirectoryType::Gauge as i32,
    }
}

/// The wire discriminant of a Prometheus kind. The Rust enum discriminants
/// (0, 1) differ from the mapped bytes (1, 2), so the wire enum's own
/// discriminant is used instead of a raw cast.
fn wire_prometheus_type(kind: PrometheusType) -> i32 {
    match kind {
        PrometheusType::Counter => wire::PrometheusType::Counter as i32,
        PrometheusType::Gauge => wire::PrometheusType::Gauge as i32,
    }
}

fn to_wire_id(id: EntryId) -> wire::EntryId {
    wire::EntryId {
        index: id.index(),
        generation: id.generation(),
    }
}

fn to_wire_list_entry(entry: hammer_stats::DirectoryEntry) -> wire::ListEntry {
    wire::ListEntry {
        id: Some(to_wire_id(entry.id)),
        path: entry.path,
        directory_type: wire_directory_type(entry.directory_type),
        prometheus_type: wire_prometheus_type(entry.prometheus_type),
        fq_name: entry.fq_name,
        help: entry.help,
        const_labels: entry
            .const_labels
            .into_iter()
            .map(|label| wire::ConstLabel {
                name: label.name,
                value: label.value,
            })
            .collect(),
    }
}

fn to_wire_dump_entry(entry: hammer_stats::DumpEntry) -> wire::DumpEntry {
    let hammer_stats::DumpEntry {
        id,
        path,
        directory_type,
        prometheus_type,
        value,
    } = entry;
    let value = match value {
        hammer_stats::DumpValue::Counter(value) => wire::value::Value::Counter(value),
        hammer_stats::DumpValue::Gauge(value) => wire::value::Value::Gauge(value),
        hammer_stats::DumpValue::CounterVectorSimple(rows) => {
            wire::value::Value::CounterVectorSimple(wire::CounterVectorSimple {
                rows: rows
                    .into_iter()
                    .map(|values| wire::CounterVectorSimpleRow { values })
                    .collect(),
            })
        }
        hammer_stats::DumpValue::CounterVectorCombined(rows) => {
            wire::value::Value::CounterVectorCombined(wire::CounterVectorCombined {
                rows: rows
                    .into_iter()
                    .map(|values| wire::CounterVectorCombinedRow {
                        values: values
                            .into_iter()
                            .map(|(packets, bytes)| wire::CounterVectorCombinedValue {
                                packets,
                                bytes,
                            })
                            .collect(),
                    })
                    .collect(),
            })
        }
        hammer_stats::DumpValue::NameVector(slots) => {
            wire::value::Value::NameVector(wire::NameVector {
                slots: slots
                    .into_iter()
                    .map(|name| wire::NameVectorSlot { name })
                    .collect(),
            })
        }
        hammer_stats::DumpValue::HistogramLog2(rows) => {
            wire::value::Value::HistogramLog2(wire::HistogramLog2 {
                rows: rows
                    .into_iter()
                    .map(|bins| wire::HistogramLog2Row { bins })
                    .collect(),
            })
        }
        hammer_stats::DumpValue::RingBuffer(snapshots) => {
            wire::value::Value::RingBuffer(wire::RingBuffer {
                snapshots: snapshots
                    .into_iter()
                    .map(|snapshot| wire::RingBufferSnapshot {
                        sequence: snapshot.sequence,
                        entries: snapshot.entries,
                    })
                    .collect(),
            })
        }
    };
    wire::DumpEntry {
        id: Some(to_wire_id(id)),
        path,
        directory_type: wire_directory_type(directory_type),
        prometheus_type: wire_prometheus_type(prometheus_type),
        value: Some(wire::Value { value: Some(value) }),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, RuntimeRegistry};
    use hammer_stats::{DirectoryType, DumpValue};

    /// A small but valid capacity for capability tests: well above the
    /// minimum, without mapping 32 MiB per test.
    const TEST_CAPACITY: usize = 1 << 20;

    fn test_config() -> StatsConfig {
        StatsConfig {
            segment_capacity: TEST_CAPACITY,
            ..StatsConfig::default()
        }
    }

    fn test_engine() -> Engine {
        Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        )
    }

    fn install_for_test() -> (Engine, Arc<StatsCapability>) {
        let mut engine = test_engine();
        let capability =
            StatsCapability::install(&mut engine, &test_config()).expect("install capability");
        (engine, capability)
    }

    fn with_stats<R>(
        engine: &mut Engine,
        operation: impl FnOnce(&mut StatsMain) -> Result<R, StatsError>,
    ) -> Result<R, StatsError> {
        engine
            .with_stats_main(operation)
            .expect("stats main must be installed")
    }

    /// Parses a document like the one the `config_function` macro produces
    /// for `section = "stats"`: the section is wrapped, so an absent section
    /// falls back to `StatsConfig::default()`.
    #[derive(Debug, serde::Deserialize, Default)]
    #[serde(default)]
    struct StatsSection {
        #[serde(default, rename = "stats")]
        stats: StatsConfig,
    }

    #[test]
    fn config_defaults_and_parses_human_intervals() {
        let default = StatsConfig::default();
        assert_eq!(default.segment_capacity, hammer_stats::DEFAULT_CAPACITY);
        assert_eq!(default.update_interval, Duration::from_secs(10));

        let parsed: StatsConfig = toml::from_str::<StatsSection>("")
            .expect("parse empty document")
            .stats;
        assert_eq!(parsed.segment_capacity, hammer_stats::DEFAULT_CAPACITY);
        assert_eq!(parsed.update_interval, Duration::from_secs(10));

        let parsed: StatsConfig =
            toml::from_str::<StatsSection>("[stats]\nupdate_interval = \"250ms\"\n")
                .expect("parse interval")
                .stats;
        assert_eq!(parsed.update_interval, Duration::from_millis(250));
        assert_eq!(parsed.segment_capacity, hammer_stats::DEFAULT_CAPACITY);

        let parsed: StatsConfig =
            toml::from_str::<StatsSection>("[stats]\nsegment_capacity = 1048576\n")
                .expect("parse capacity")
                .stats;
        assert_eq!(parsed.segment_capacity, 1 << 20);
        assert_eq!(parsed.update_interval, Duration::from_secs(10));
    }

    #[test]
    fn config_rejects_zero_interval_and_unknown_fields() {
        let error = StatsConfig {
            update_interval: Duration::ZERO,
            ..test_config()
        }
        .validate()
        .expect_err("zero interval must be rejected");
        match error {
            RuntimeError::ConfigValidation { message } => {
                assert!(
                    message.contains("stats.update_interval"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }

        assert!(
            toml::from_str::<StatsSection>("[stats]\nnot_a_field = 1\n")
                .expect_err("unknown field must be rejected")
                .to_string()
                .contains("not_a_field")
        );
    }

    #[test]
    fn too_small_capacity_is_a_typed_install_error() {
        let config = StatsConfig {
            segment_capacity: 0,
            ..test_config()
        };
        let mut engine = test_engine();
        let error = match StatsCapability::install(&mut engine, &config) {
            Err(error) => error,
            Ok(_) => panic!("zero capacity must be rejected"),
        };
        match error {
            StatsServiceError::Stats(StatsError::CapacityTooSmall { .. }) => {}
            other => panic!("unexpected error: {other}"),
        }
        // The init boundary converts the typed error into the runtime
        // subsystem error, keeping the `"stats"` subsystem and the source
        // chain down to the capacity rejection.
        let runtime_error = RuntimeError::from(error);
        assert!(matches!(
            runtime_error,
            RuntimeError::Subsystem {
                subsystem: "stats",
                ..
            }
        ));
        let source = runtime_error
            .source()
            .expect("subsystem error exposes its source");
        assert!(
            source.to_string().contains("below the minimum"),
            "unexpected source: {source}"
        );
    }

    /// The three system scalars are published before the collector node
    /// runs: heartbeat as a counter scalar, boottime and last-stats-clear as
    /// gauge scalars, all with initial zero values (VPP stats.h:22-24,
    /// stats.c:281).
    #[test]
    fn system_metrics_are_published_before_the_node_starts() {
        let (mut engine, _capability) = install_for_test();
        let entries = with_stats(&mut engine, |stats| stats.list(&[])).expect("list");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].path, SYS_HEARTBEAT_PATH);
        assert_eq!(entries[0].fq_name, "hammer_sys_heartbeat_total");
        assert_eq!(entries[0].directory_type, DirectoryType::ScalarIndex);
        assert_eq!(entries[0].prometheus_type, PrometheusType::Counter);
        assert_eq!(entries[1].path, SYS_BOOTTIME_PATH);
        assert_eq!(entries[1].fq_name, "hammer_sys_boottime_seconds");
        assert_eq!(entries[1].directory_type, DirectoryType::ScalarIndex);
        assert_eq!(entries[1].prometheus_type, PrometheusType::Gauge);
        assert_eq!(entries[2].path, SYS_LAST_STATS_CLEAR_PATH);
        assert_eq!(entries[2].fq_name, "hammer_sys_last_stats_clear_seconds");
        assert_eq!(entries[2].directory_type, DirectoryType::ScalarIndex);
        assert_eq!(entries[2].prometheus_type, PrometheusType::Gauge);

        let ids: Vec<EntryId> = entries.iter().map(|entry| entry.id).collect();
        let dump = with_stats(&mut engine, |stats| stats.dump(&ids)).expect("dump");
        assert_eq!(dump[0].value, DumpValue::Counter(0));
        assert_eq!(dump[1].value, DumpValue::Gauge(0.0));
        assert_eq!(dump[2].value, DumpValue::Gauge(0.0));
    }

    /// The Process Node's start sequence — boottime set once, then one
    /// immediate pass — publishes boottime and bumps the heartbeat to 1
    /// while last-stats-clear stays 0 (VPP collector.c:149-150, 172).
    #[test]
    fn boottime_set_and_one_collect_publishes_system_values() {
        let (mut engine, capability) = install_for_test();
        capability
            .set_boottime(1_700_000_000)
            .expect("set boottime");
        with_stats(&mut engine, |stats| stats.collect()).expect("collect");

        let ids: Vec<EntryId> = with_stats(&mut engine, |stats| stats.list(&[]))
            .expect("list")
            .into_iter()
            .map(|entry| entry.id)
            .collect();
        let dump = with_stats(&mut engine, |stats| stats.dump(&ids)).expect("dump");
        assert_eq!(dump[0].value, DumpValue::Counter(1));
        assert!(
            matches!(dump[1].value, DumpValue::Gauge(value) if value > 0.0),
            "boottime must be published: {:?}",
            dump[1].value
        );
        assert_eq!(dump[2].value, DumpValue::Gauge(0.0));
    }

    /// A failing collector must not stop the system heartbeat collector or
    /// the main Engine's stats owner (VPP runs every collector even when one
    /// fails; collector.c:137-147).
    #[test]
    fn failing_collector_still_runs_the_system_collector() {
        let (mut engine, _capability) = install_for_test();
        with_stats(&mut engine, |stats| {
            stats.register_collector(|| Err(StatsError::InvalidPath("boom".to_owned())));
            Ok(())
        })
        .expect("register");

        // The first error in registration order surfaces from the pass.
        let pass = with_stats(&mut engine, |stats| stats.collect());
        assert!(
            matches!(pass, Err(StatsError::InvalidPath(_))),
            "unexpected pass result: {pass:?}"
        );

        // The heartbeat collector still ran before the failing one.
        let heartbeat = with_stats(&mut engine, |stats| {
            stats.list(&[SYS_HEARTBEAT_PATH.to_owned()])
        })
        .expect("list");
        let dump = with_stats(&mut engine, |stats| stats.dump(&[heartbeat[0].id])).expect("dump");
        assert_eq!(dump[0].value, DumpValue::Counter(1));

        // The owner still serves the next operation.
        with_stats(&mut engine, |stats| stats.list(&[])).expect("list");
    }

    #[test]
    fn wire_entries_preserve_order_duplicates_and_labels() {
        use hammer_stats::{ConstLabel, DirectoryEntry, DumpEntry};

        let id0 = EntryId::try_from((0, 1)).expect("id");
        let id1 = EntryId::try_from((1, 2)).expect("id");
        let entries = vec![
            DirectoryEntry {
                id: id0,
                path: "/a".to_owned(),
                directory_type: DirectoryType::ScalarIndex,
                prometheus_type: PrometheusType::Counter,
                fq_name: "a_total".to_owned(),
                help: "a".to_owned(),
                const_labels: vec![
                    ConstLabel {
                        name: "iface".to_owned(),
                        value: "eth0".to_owned(),
                    },
                    ConstLabel {
                        name: "dir".to_owned(),
                        value: "rx".to_owned(),
                    },
                ],
            },
            DirectoryEntry {
                id: id1,
                path: "/g".to_owned(),
                directory_type: DirectoryType::Gauge,
                prometheus_type: PrometheusType::Gauge,
                fq_name: "g".to_owned(),
                help: "g".to_owned(),
                const_labels: Vec::new(),
            },
        ];
        let wire_entries: Vec<wire::ListEntry> =
            entries.into_iter().map(to_wire_list_entry).collect();
        assert_eq!(wire_entries.len(), 2);
        assert_eq!(wire_entries[0].id.as_ref().unwrap().index, 0);
        assert_eq!(wire_entries[1].id.as_ref().unwrap().generation, 2);
        assert_eq!(wire_entries[0].path, "/a");
        assert_eq!(wire_entries[0].fq_name, "a_total");
        assert_eq!(wire_entries[0].help, "a");
        // Const labels keep their order.
        assert_eq!(wire_entries[0].const_labels.len(), 2);
        assert_eq!(wire_entries[0].const_labels[0].name, "iface");
        assert_eq!(wire_entries[0].const_labels[0].value, "eth0");
        assert_eq!(wire_entries[0].const_labels[1].name, "dir");
        // Kinds map to the exact wire discriminants.
        assert_eq!(
            wire_entries[0].directory_type,
            wire::DirectoryType::ScalarIndex as i32
        );
        assert_eq!(
            wire_entries[0].prometheus_type,
            wire::PrometheusType::Counter as i32
        );
        assert_eq!(
            wire_entries[1].directory_type,
            wire::DirectoryType::Gauge as i32
        );
        assert_eq!(
            wire_entries[1].prometheus_type,
            wire::PrometheusType::Gauge as i32
        );

        // Dump entries preserve input order and duplicates with exact values.
        let dumps = vec![
            DumpEntry {
                id: id1,
                path: "/g".to_owned(),
                directory_type: DirectoryType::Gauge,
                prometheus_type: PrometheusType::Gauge,
                value: DumpValue::Gauge(1.5),
            },
            DumpEntry {
                id: id0,
                path: "/a".to_owned(),
                directory_type: DirectoryType::ScalarIndex,
                prometheus_type: PrometheusType::Counter,
                value: DumpValue::Counter(7),
            },
            DumpEntry {
                id: id1,
                path: "/g".to_owned(),
                directory_type: DirectoryType::Gauge,
                prometheus_type: PrometheusType::Gauge,
                value: DumpValue::Gauge(1.5),
            },
        ];
        let wire_dumps: Vec<wire::DumpEntry> = dumps.into_iter().map(to_wire_dump_entry).collect();
        assert_eq!(wire_dumps.len(), 3);
        assert_eq!(wire_dumps[0].id.as_ref().unwrap().generation, 2);
        assert_eq!(
            wire_dumps[0].value.as_ref().unwrap().value,
            Some(wire::value::Value::Gauge(1.5))
        );
        assert_eq!(wire_dumps[1].id.as_ref().unwrap().index, 0);
        assert_eq!(
            wire_dumps[1].value.as_ref().unwrap().value,
            Some(wire::value::Value::Counter(7))
        );
        assert_eq!(wire_dumps[2], wire_dumps[0]);
    }

    #[test]
    fn wire_dump_converts_owned_values_and_preserves_symlink_identity() {
        use hammer_stats::{DumpEntry, RingBufferSnapshot};

        let entries = vec![
            DumpEntry {
                id: EntryId::try_from((0, 1)).expect("id"),
                path: "/counter".to_owned(),
                directory_type: DirectoryType::ScalarIndex,
                prometheus_type: PrometheusType::Counter,
                value: DumpValue::Counter(7),
            },
            DumpEntry {
                id: EntryId::try_from((1, 2)).expect("id"),
                path: "/gauge".to_owned(),
                directory_type: DirectoryType::Gauge,
                prometheus_type: PrometheusType::Gauge,
                value: DumpValue::Gauge(1.5),
            },
            DumpEntry {
                id: EntryId::try_from((2, 3)).expect("id"),
                path: "/simple".to_owned(),
                directory_type: DirectoryType::CounterVectorSimple,
                prometheus_type: PrometheusType::Counter,
                value: DumpValue::CounterVectorSimple(vec![vec![1, 2], vec![3]]),
            },
            DumpEntry {
                id: EntryId::try_from((3, 4)).expect("id"),
                path: "/combined".to_owned(),
                directory_type: DirectoryType::CounterVectorCombined,
                prometheus_type: PrometheusType::Counter,
                value: DumpValue::CounterVectorCombined(vec![vec![(1, 2)], vec![(3, 4)]]),
            },
            DumpEntry {
                id: EntryId::try_from((4, 5)).expect("id"),
                path: "/names".to_owned(),
                directory_type: DirectoryType::NameVector,
                prometheus_type: PrometheusType::Gauge,
                value: DumpValue::NameVector(vec![Some("eth0".to_owned()), None]),
            },
            DumpEntry {
                id: EntryId::try_from((5, 6)).expect("id"),
                path: "/histogram".to_owned(),
                directory_type: DirectoryType::HistogramLog2,
                prometheus_type: PrometheusType::Counter,
                value: DumpValue::HistogramLog2(vec![vec![2, 4], vec![8]]),
            },
            DumpEntry {
                id: EntryId::try_from((6, 7)).expect("id"),
                path: "/ring".to_owned(),
                directory_type: DirectoryType::RingBuffer,
                prometheus_type: PrometheusType::Counter,
                value: DumpValue::RingBuffer(vec![RingBufferSnapshot {
                    sequence: 9,
                    entries: vec![b"old".to_vec(), b"new".to_vec()],
                }]),
            },
            DumpEntry {
                id: EntryId::try_from((7, 8)).expect("id"),
                path: "/symlink".to_owned(),
                directory_type: DirectoryType::Symlink,
                prometheus_type: PrometheusType::Counter,
                value: DumpValue::CounterVectorSimple(vec![vec![11]]),
            },
        ];

        let wire_dumps: Vec<wire::DumpEntry> =
            entries.into_iter().map(to_wire_dump_entry).collect();

        assert_eq!(
            wire_dumps[0].value.as_ref().unwrap().value,
            Some(wire::value::Value::Counter(7))
        );
        assert_eq!(
            wire_dumps[1].value.as_ref().unwrap().value,
            Some(wire::value::Value::Gauge(1.5))
        );
        assert_eq!(
            wire_dumps[2].value.as_ref().unwrap().value,
            Some(wire::value::Value::CounterVectorSimple(
                wire::CounterVectorSimple {
                    rows: vec![
                        wire::CounterVectorSimpleRow { values: vec![1, 2] },
                        wire::CounterVectorSimpleRow { values: vec![3] },
                    ],
                }
            ))
        );
        assert_eq!(
            wire_dumps[3].value.as_ref().unwrap().value,
            Some(wire::value::Value::CounterVectorCombined(
                wire::CounterVectorCombined {
                    rows: vec![
                        wire::CounterVectorCombinedRow {
                            values: vec![wire::CounterVectorCombinedValue {
                                packets: 1,
                                bytes: 2,
                            }],
                        },
                        wire::CounterVectorCombinedRow {
                            values: vec![wire::CounterVectorCombinedValue {
                                packets: 3,
                                bytes: 4,
                            }],
                        },
                    ],
                }
            ))
        );
        assert_eq!(
            wire_dumps[4].value.as_ref().unwrap().value,
            Some(wire::value::Value::NameVector(wire::NameVector {
                slots: vec![
                    wire::NameVectorSlot {
                        name: Some("eth0".to_owned()),
                    },
                    wire::NameVectorSlot { name: None },
                ],
            }))
        );
        assert_eq!(
            wire_dumps[5].value.as_ref().unwrap().value,
            Some(wire::value::Value::HistogramLog2(wire::HistogramLog2 {
                rows: vec![
                    wire::HistogramLog2Row { bins: vec![2, 4] },
                    wire::HistogramLog2Row { bins: vec![8] },
                ],
            }))
        );
        assert_eq!(
            wire_dumps[6].value.as_ref().unwrap().value,
            Some(wire::value::Value::RingBuffer(wire::RingBuffer {
                snapshots: vec![wire::RingBufferSnapshot {
                    sequence: 9,
                    entries: vec![b"old".to_vec(), b"new".to_vec()],
                }],
            }))
        );
        assert_eq!(
            wire_dumps[7].id,
            Some(wire::EntryId {
                index: 7,
                generation: 8,
            })
        );
        assert_eq!(wire_dumps[7].path, "/symlink");
        assert_eq!(
            wire_dumps[7].directory_type,
            wire::DirectoryType::Symlink as i32
        );
        assert_eq!(
            wire_dumps[7].value.as_ref().unwrap().value,
            Some(wire::value::Value::CounterVectorSimple(
                wire::CounterVectorSimple {
                    rows: vec![wire::CounterVectorSimpleRow { values: vec![11] }],
                }
            ))
        );
    }

    #[test]
    fn wire_error_mapping_is_exact() {
        use wire::error_oneof::Error as WireError;

        let (mut engine, _capability) = install_for_test();
        let id = EntryId::try_from((4, 9)).expect("id");

        // InvalidPattern carries exactly the pattern, never the regex source.
        let pattern_error = with_stats(&mut engine, |stats| stats.list(&["(".to_owned()]))
            .expect_err("invalid pattern");
        match wire_error(pattern_error) {
            WireError::InvalidPattern(inner) => assert_eq!(inner.pattern, "("),
            other => panic!("expected InvalidPattern, got: {other:?}"),
        }

        // NotFound and StaleEntry carry the full entry id.
        match wire_error(StatsError::NotFound { id }) {
            WireError::NotFound(inner) => {
                let inner = inner.id.expect("id present");
                assert_eq!((inner.index, inner.generation), (4, 9));
            }
            other => panic!("expected NotFound, got: {other:?}"),
        }
        match wire_error(StatsError::StaleEntry { id }) {
            WireError::StaleEntry(inner) => {
                let inner = inner.id.expect("id present");
                assert_eq!((inner.index, inner.generation), (4, 9));
            }
            other => panic!("expected StaleEntry, got: {other:?}"),
        }
        assert!(matches!(
            wire_error(StatsError::ReadBusy),
            WireError::ReadBusy(_)
        ));

        // IncompatibleType carries the id and the exact wire discriminants.
        match wire_error(StatsError::IncompatibleType {
            id,
            prometheus_type: PrometheusType::Counter,
            directory_type: DirectoryType::Gauge,
        }) {
            WireError::IncompatibleType(inner) => {
                let inner_id = inner.id.expect("id present");
                assert_eq!((inner_id.index, inner_id.generation), (4, 9));
                assert_eq!(inner.directory_type, wire::DirectoryType::Gauge as i32);
                assert_eq!(inner.prometheus_type, wire::PrometheusType::Counter as i32);
            }
            other => panic!("expected IncompatibleType, got: {other:?}"),
        }

        // Every segment/init/corruption/access error falls back to Internal,
        // including ids that can never name an entry.
        for error in [
            StatsError::InvalidEntryId {
                index: 0,
                generation: 0,
            },
            StatsError::CapacityTooSmall {
                minimum: 1,
                requested: 0,
            },
            StatsError::SegmentFull,
            StatsError::InvalidPath("nope".to_owned()),
            StatsError::DuplicateName("nope".to_owned()),
            StatsError::OutOfBounds,
        ] {
            let description = error.to_string();
            assert!(
                matches!(wire_error(error), WireError::Internal(_)),
                "expected Internal for: {description}"
            );
        }
    }

    /// Every directory and Prometheus kind maps to its exact wire
    /// discriminant. The domain matches are exhaustive on both sides, so a
    /// variant added or renumbered in either enum stops compiling here.
    #[test]
    fn kind_mappings_are_exhaustive_and_exact() {
        use wire::DirectoryType as WireDirectoryType;

        let pairs = [
            (DirectoryType::Illegal, WireDirectoryType::Illegal),
            (DirectoryType::ScalarIndex, WireDirectoryType::ScalarIndex),
            (
                DirectoryType::CounterVectorSimple,
                WireDirectoryType::CounterVectorSimple,
            ),
            (
                DirectoryType::CounterVectorCombined,
                WireDirectoryType::CounterVectorCombined,
            ),
            (DirectoryType::NameVector, WireDirectoryType::NameVector),
            (DirectoryType::Empty, WireDirectoryType::Empty),
            (DirectoryType::Symlink, WireDirectoryType::Symlink),
            (
                DirectoryType::HistogramLog2,
                WireDirectoryType::HistogramLog2,
            ),
            (DirectoryType::RingBuffer, WireDirectoryType::RingBuffer),
            (DirectoryType::Gauge, WireDirectoryType::Gauge),
        ];
        for (domain, wire_kind) in pairs {
            assert_eq!(
                wire_directory_type(domain),
                wire_kind as i32,
                "mismatched mapping for {domain:?}"
            );
        }

        assert_eq!(
            wire_prometheus_type(PrometheusType::Counter),
            wire::PrometheusType::Counter as i32
        );
        assert_eq!(
            wire_prometheus_type(PrometheusType::Gauge),
            wire::PrometheusType::Gauge as i32
        );
    }

    #[test]
    fn checked_wire_id_rejects_generation_zero() {
        assert!(matches!(
            checked_wire_id(&wire::EntryId {
                index: 0,
                generation: 0
            }),
            Err(StatsError::InvalidEntryId { .. })
        ));
        let id = checked_wire_id(&wire::EntryId {
            index: 7,
            generation: 3,
        })
        .expect("valid id");
        assert_eq!(id.index(), 7);
        assert_eq!(id.generation(), 3);
    }
}
