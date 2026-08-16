//! Public-seam tests for the reader batch: timestamp metrics, collector
//! registry, `list` and `dump`.
//!
//! Each test is one vertical slice through `StatsMain`'s public API,
//! mirroring VPP's `stat_segment_ls`/`stat_segment_dump` client protocol
//! (stat_client.c:349-466).

use std::error::Error as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use hammer_stats::{
    ConstLabel, DirectoryType, DumpValue, EntryId, PrometheusType, StatsError, StatsMain,
};

/// Counters and timestamps are scalar directory entries; timestamps are
/// Prometheus gauges whose value is a plain integer.
#[test]
fn counters_and_timestamps_report_scalar_directory_type() {
    let mut stats = StatsMain::new().expect("default construction");

    let (id, counter) = stats
        .add_counter(
            "/if/rx",
            prometheus::Opts::new("rx_bytes", "bytes received"),
        )
        .expect("counter added");
    counter.increment_by(42).expect("increment");

    let entries = stats.list(&[]).expect("list");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, id);
    assert_eq!(entries[0].path, "/if/rx");
    assert_eq!(entries[0].directory_type, DirectoryType::ScalarIndex);
    assert_eq!(entries[0].prometheus_type, PrometheusType::Counter);

    let (tid, timestamp) = stats
        .add_timestamp(
            "/sys/boottime",
            prometheus::Opts::new("boottime", "boot time"),
        )
        .expect("timestamp added");
    timestamp.set(1_700_000_000).expect("set");
    assert_eq!(timestamp.get().expect("get"), 1_700_000_000);

    let clone = timestamp.try_clone().expect("clone");
    clone.set(1_800_000_000).expect("clone sets");
    assert_eq!(timestamp.get().expect("read after clone"), 1_800_000_000);

    let entries = stats.list(&[]).expect("list");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[1].id, tid);
    assert_eq!(entries[1].path, "/sys/boottime");
    assert_eq!(entries[1].directory_type, DirectoryType::ScalarIndex);
    assert_eq!(entries[1].prometheus_type, PrometheusType::Gauge);
}

/// The empty pattern selects every active entry in ascending directory-index
/// order, with fully decoded descriptor fields; removed entries are absent.
#[test]
fn list_returns_active_entries_in_index_order_with_descriptor_fields() {
    let mut stats = StatsMain::new().expect("default construction");

    let (id0, _) = stats
        .add_counter(
            "/if/rx",
            prometheus::Opts::new("rx_bytes", "bytes received")
                .const_label("iface", "eth0")
                .const_label("dir", "rx")
                .const_label("empty", ""),
        )
        .expect("counter");
    let (id1, gauge) = stats
        .add_gauge(
            "/sys/temp",
            prometheus::Opts::new("temp_c", "core temperature").const_label("sensor", "cpu"),
        )
        .expect("gauge");

    let entries = stats.list(&[]).expect("list");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id, id0);
    assert_eq!(entries[1].id, id1);
    assert_eq!(entries[0].fq_name, "rx_bytes");
    assert_eq!(entries[0].help, "bytes received");
    // prometheus 0.14 stores const labels in a `HashMap` and sorts
    // `Desc::const_label_pairs` (desc.rs:176-183), so the stored order is
    // alphabetical by label name; `list` preserves it, including empty
    // label values.
    assert_eq!(
        entries[0].const_labels,
        vec![
            ConstLabel {
                name: "dir".to_owned(),
                value: "rx".to_owned(),
            },
            ConstLabel {
                name: "empty".to_owned(),
                value: String::new(),
            },
            ConstLabel {
                name: "iface".to_owned(),
                value: "eth0".to_owned(),
            },
        ]
    );
    assert_eq!(entries[1].fq_name, "temp_c");
    assert_eq!(entries[1].help, "core temperature");

    // Removed entries are absent from the listing.
    stats.remove_entry(id0).expect("remove");
    let entries = stats.list(&[]).expect("list after remove");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, id1);
    gauge.get().expect("gauge still usable");
}

/// Multiple patterns are OR-ed; no match is an empty result; an invalid
/// pattern is a typed error carrying the pattern.
#[test]
fn list_filters_with_or_regex_semantics() {
    let mut stats = StatsMain::new().expect("default construction");
    stats
        .add_counter("/if/rx", prometheus::Opts::new("rx_bytes", "rx"))
        .expect("rx");
    stats
        .add_counter("/if/tx", prometheus::Opts::new("tx_bytes", "tx"))
        .expect("tx");
    stats
        .add_gauge("/sys/temp", prometheus::Opts::new("temp_c", "temp"))
        .expect("temp");

    let entries = stats.list(&["^/if/".to_owned()]).expect("prefix pattern");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].path, "/if/rx");
    assert_eq!(entries[1].path, "/if/tx");

    let entries = stats
        .list(&["rx$".to_owned(), "temp".to_owned()])
        .expect("or patterns");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].path, "/if/rx");
    assert_eq!(entries[1].path, "/sys/temp");

    let entries = stats
        .list(&["^/nope$".to_owned()])
        .expect("no match is not an error");
    assert!(entries.is_empty());

    let err = stats
        .list(&["(".to_owned()])
        .err()
        .expect("invalid regex rejected");
    assert!(
        matches!(
            &err,
            StatsError::InvalidPattern { pattern, source }
                if pattern == "(" && source.to_string().contains('(')
        ),
        "unexpected error: {err}"
    );
    assert!(
        err.source().is_some(),
        "pattern error must expose the regex source"
    );
    assert!(
        err.to_string().contains('('),
        "error must carry the pattern: {err}"
    );
}

/// `dump` is a point-in-time copy that preserves input order and duplicates,
/// never triggers a collector, and reflects later updates only in a fresh
/// dump.
#[test]
fn dump_is_point_in_time_and_preserves_order_and_duplicates() {
    let mut stats = StatsMain::new().expect("default construction");

    let calls = Arc::new(AtomicU32::new(0));
    let probe = calls.clone();
    stats.register_collector(move || {
        probe.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });

    let (id0, counter) = stats
        .add_counter("/c", prometheus::Opts::new("c", "c"))
        .expect("counter");
    let (id1, gauge) = stats
        .add_gauge("/g", prometheus::Opts::new("g", "g"))
        .expect("gauge");
    let (id2, timestamp) = stats
        .add_timestamp("/t", prometheus::Opts::new("t", "t"))
        .expect("timestamp");

    counter.increment_by(5).expect("increment");
    gauge.set(1.5).expect("set gauge");
    timestamp.set(99).expect("set timestamp");

    // Preserves input order and duplicates; values are owned copies.
    let dump = stats
        .dump(&[id0, id0, id2, id1, id0])
        .expect("dump with duplicates");
    assert_eq!(dump.len(), 5);
    assert_eq!(dump[0].id, id0);
    assert_eq!(dump[1].id, id0);
    assert_eq!(dump[2].id, id2);
    assert_eq!(dump[3].id, id1);
    assert_eq!(dump[4].id, id0);
    assert_eq!(dump[0].value, DumpValue::Counter(5));
    assert_eq!(dump[1].value, DumpValue::Counter(5));
    assert_eq!(dump[2].value, DumpValue::Gauge(99.0));
    assert_eq!(dump[3].value, DumpValue::Gauge(1.5));
    assert_eq!(dump[4].value, DumpValue::Counter(5));

    // Later updates only change a fresh dump, not the earlier copy.
    counter.increment_by(3).expect("increment again");
    let fresh = stats.dump(&[id0]).expect("fresh dump");
    assert_eq!(fresh[0].value, DumpValue::Counter(8));
    assert_eq!(dump[0].value, DumpValue::Counter(5));

    // Dump never triggers collectors; collect runs them exactly once.
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    stats.collect().expect("collect");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// Missing indices and stale generations are typed errors, whether the slot
/// is out of range, was reused under a newer generation, or was removed.
#[test]
fn dump_rejects_missing_and_stale_ids() {
    let mut stats = StatsMain::new().expect("default construction");
    let (id0, _) = stats
        .add_counter("/a", prometheus::Opts::new("a", "a"))
        .expect("counter");

    let missing = EntryId::try_from((99, 1)).expect("valid id");
    let err = stats
        .dump(&[missing])
        .err()
        .expect("missing index rejected");
    assert!(matches!(err, StatsError::NotFound { id } if id == missing));

    let stale = EntryId::try_from((id0.index(), id0.generation() + 1)).expect("stale id");
    let err = stats.dump(&[stale]).err().expect("stale id rejected");
    assert!(
        matches!(err, StatsError::StaleEntry { id } if id == stale),
        "unexpected error: {err}"
    );

    stats.remove_entry(id0).expect("remove");
    let err = stats.dump(&[id0]).err().expect("removed id rejected");
    assert!(
        matches!(err, StatsError::NotFound { id } if id == id0),
        "unexpected error: {err}"
    );
}

/// Collectors run in registration order, every collector runs even when one
/// fails, and the returned error is the first in registration order.
#[test]
fn collect_runs_every_collector_and_returns_the_first_error() {
    let mut stats = StatsMain::new().expect("default construction");
    let (_, counter) = stats
        .add_counter("/c", prometheus::Opts::new("c", "c"))
        .expect("counter");

    // First registered: fails with a distinct error.
    stats.register_collector(|| Err(StatsError::InvalidPath("first".to_owned())));
    // Second: healthy, updates the metric through a cloned handle.
    let update = counter.try_clone().expect("clone");
    stats.register_collector(move || update.increment_by(10));
    // Third: fails with a different error; must not mask the first.
    stats.register_collector(|| Err(StatsError::DuplicateName("third".to_owned())));

    let err = stats.collect().err().expect("first error returned");
    assert!(
        matches!(err, StatsError::InvalidPath(_)),
        "unexpected error: {err}"
    );

    // Every collector ran once despite the errors.
    assert_eq!(counter.get().expect("value"), 10);
}

/// Removed metric blocks are reused by the arena, and a reused block must
/// decode exactly the new occupant's strings: the shorter strings of the
/// second metric sit in a block whose tail still holds the longer strings
/// of the first, so every string needs its own freshly written NUL
/// terminator rather than relying on stale bytes.
#[test]
fn reused_block_after_removal_decodes_exact_strings() {
    let mut stats = StatsMain::new().expect("default construction");

    // Both blocks are exactly 128 bytes: header (32) plus name+help with
    // their terminators rounds to 64, plus the 64-byte value record. Equal
    // sizes force the allocator to hand the same block back on reuse.
    let (id_a, counter) = stats
        .add_counter("/reuse/long", prometheus::Opts::new("a", "x".repeat(27)))
        .expect("long-string counter");

    let entries = stats.list(&[]).expect("list before removal");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "/reuse/long");
    assert_eq!(entries[0].fq_name, "a");
    assert_eq!(entries[0].help, "x".repeat(27));

    // Removing while the handle lives puts the slot on the removed list;
    // dropping the handle lets the next structural pass release the block.
    stats.remove_entry(id_a).expect("remove");
    drop(counter);

    let (id_b, counter_b) = stats
        .add_counter("/reuse/short", prometheus::Opts::new("b", "y"))
        .expect("short-string counter reusing the block");

    let entries = stats.list(&[]).expect("list after reuse");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, id_b);
    assert_eq!(entries[0].path, "/reuse/short");
    assert_eq!(entries[0].fq_name, "b");
    assert_eq!(entries[0].help, "y");

    let dump = stats.dump(&[id_b]).expect("dump after reuse");
    assert_eq!(dump.len(), 1);
    assert_eq!(dump[0].path, "/reuse/short");
    counter_b.increment().expect("reused handle still live");
}
