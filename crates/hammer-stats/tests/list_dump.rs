//! Public-seam tests for the reader batch: timestamp metrics, collector
//! registry, `list` and `dump`.
//!
//! Each test is one vertical slice through `StatsMain`'s public API,
//! mirroring VPP's `stat_segment_ls`/`stat_segment_dump` client protocol
//! (stat_client.c:349-466).

use std::error::Error as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use hammer_infra::page_size;
use hammer_stats::{
    ConstLabel, DirectoryType, DumpValue, EntryId, PrometheusType, StatsDescriptor, StatsEntry,
    StatsError, StatsMain, StatsRegistration, StatsValueLayout,
};

/// A protocol-neutral scalar registration returns a direct handle that can be
/// updated without another `StatsMain` lookup, and the reader sees that value.
#[test]
fn register_counter_returns_direct_handle_for_list_and_dump() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [StatsRegistration {
        path: "/if/rx",
        descriptor: StatsDescriptor {
            fq_name: "rx_bytes",
            help: "bytes received",
            const_labels: &[("iface", "eth0")],
        },
        value: StatsValueLayout::Counter,
    }];

    let mut entries = stats.register(&registrations).expect("counter registered");
    assert_eq!(entries.len(), 1);
    let StatsEntry::Counter { id, handle } = entries.pop().expect("entry") else {
        panic!("counter registration returned a different value kind");
    };
    handle.increment_by(42).expect("direct increment");

    let listed = stats.list(&[]).expect("list");
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].path, "/if/rx");
    assert_eq!(listed[0].fq_name, "rx_bytes");
    assert_eq!(listed[0].help, "bytes received");

    let dumped = stats.dump(&[id]).expect("dump");
    assert_eq!(dumped[0].value, DumpValue::Counter(42));
}

#[test]
fn register_counter_vector_simple_returns_direct_handle_for_list_and_dump() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [StatsRegistration {
        path: "/node/errors",
        descriptor: StatsDescriptor {
            fq_name: "node_errors",
            help: "per-worker node errors",
            const_labels: &[],
        },
        value: StatsValueLayout::CounterVectorSimple {
            rows: 2,
            columns: 3,
        },
    }];

    let mut entries = stats.register(&registrations).expect("vector registered");
    let StatsEntry::CounterVectorSimple { id, handle } = entries.pop().expect("entry") else {
        panic!("simple counter vector registration returned a different value kind");
    };
    handle
        .increment_by(0, 1, 7)
        .expect("first direct increment");
    handle
        .increment_by(1, 2, 4)
        .expect("second direct increment");

    let listed = stats.list(&[]).expect("list");
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].path, "/node/errors");
    assert_eq!(listed[0].directory_type, DirectoryType::CounterVectorSimple);
    assert_eq!(listed[0].prometheus_type, PrometheusType::Counter);

    let dumped = stats.dump(&[id]).expect("dump");
    assert_eq!(
        dumped[0].value,
        DumpValue::CounterVectorSimple(vec![vec![0, 7, 0], vec![0, 0, 4]])
    );
}

#[test]
fn register_symlinks_keeps_generic_registration_link_free_and_resolves_replacement() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [StatsRegistration {
        path: "/sys/node/calls",
        descriptor: StatsDescriptor {
            fq_name: "node_calls_total",
            help: "node calls",
            const_labels: &[],
        },
        value: StatsValueLayout::CounterVectorSimple {
            rows: 2,
            columns: 2,
        },
    }];
    let mut entries = stats
        .register(&registrations)
        .expect("generic vector registration");
    let StatsEntry::CounterVectorSimple {
        id: vector_id,
        handle: vector,
    } = entries.pop().expect("vector entry")
    else {
        panic!("generic registration returned a different value kind");
    };

    let symlink_id = stats
        .register_symlinks(&[(
            "/nodes/example/calls",
            StatsDescriptor {
                fq_name: "node_calls_total",
                help: "node calls",
                const_labels: &[("node", "example")],
            },
            "/sys/node/calls",
            1,
        )])
        .expect("link registration")
        .into_iter()
        .next()
        .expect("link id");

    vector.increment_by(0, 0, 7).expect("first value");
    vector.increment_by(1, 1, 11).expect("second value");
    let before = stats.dump(&[symlink_id]).expect("link before replacement");
    assert_eq!(
        before[0].value,
        DumpValue::CounterVectorSimple(vec![vec![0], vec![11]])
    );

    let (replacement_id, replacement) = stats
        .replace_counter_vector_simple("/sys/node/calls", 3, 4)
        .expect("replace root vector");
    assert_eq!(replacement_id.index(), vector_id.index());
    assert_eq!(replacement_id.generation(), vector_id.generation() + 1);
    assert_eq!(replacement.rows(), 3);
    assert_eq!(replacement.columns(), 4);
    replacement.increment_by(2, 3, 5).expect("new vector cell");
    let after = stats.dump(&[symlink_id]).expect("link follows replacement");
    assert_eq!(
        after[0].value,
        DumpValue::CounterVectorSimple(vec![vec![0], vec![11], vec![0]])
    );

    assert!(matches!(
        vector.get(0, 0),
        Err(StatsError::StaleEntry { id }) if id == vector_id
    ));

    let dumped = stats
        .dump(&[replacement_id, symlink_id])
        .expect("dump replacement and link");
    assert_eq!(
        dumped[0].value,
        DumpValue::CounterVectorSimple(vec![vec![7, 0, 0, 0], vec![0, 11, 0, 0], vec![0, 0, 0, 5]])
    );
    assert_eq!(
        dumped[1].value,
        DumpValue::CounterVectorSimple(vec![vec![0], vec![11], vec![0]])
    );
}

#[test]
fn register_counter_vector_combined_returns_direct_handle_for_list_and_dump() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [StatsRegistration {
        path: "/if/counters",
        descriptor: StatsDescriptor {
            fq_name: "interface_counters",
            help: "per-worker packet and byte counters",
            const_labels: &[],
        },
        value: StatsValueLayout::CounterVectorCombined {
            rows: 2,
            columns: 2,
        },
    }];

    let mut entries = stats.register(&registrations).expect("vector registered");
    let StatsEntry::CounterVectorCombined { id, handle } = entries.pop().expect("entry") else {
        panic!("combined counter vector registration returned a different value kind");
    };
    handle
        .increment_by(0, 1, 3, 42)
        .expect("direct combined increment");
    assert_eq!(handle.get(0, 1).expect("combined counter read"), (3, 42));

    let listed = stats.list(&[]).expect("list");
    assert_eq!(listed[0].id, id);
    assert_eq!(
        listed[0].directory_type,
        DirectoryType::CounterVectorCombined
    );

    let dumped = stats.dump(&[id]).expect("dump");
    assert_eq!(
        dumped[0].value,
        DumpValue::CounterVectorCombined(vec![vec![(0, 0), (3, 42)], vec![(0, 0), (0, 0)]])
    );
}

#[test]
fn register_name_vector_returns_fixed_slot_handle_and_snapshot() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [StatsRegistration {
        path: "/names/workers",
        descriptor: StatsDescriptor {
            fq_name: "worker_names",
            help: "bounded worker names",
            const_labels: &[],
        },
        value: StatsValueLayout::NameVector { length: 3 },
    }];

    let mut entries = stats
        .register(&registrations)
        .expect("name vector registered");
    let StatsEntry::NameVector { id, handle } = entries.pop().expect("entry") else {
        panic!("name vector registration returned a different value kind");
    };
    handle.set(0, "worker-a").expect("first name");
    handle.set(2, "worker-c").expect("third name");
    assert_eq!(
        handle.get(0).expect("first name read").as_deref(),
        Some("worker-a")
    );
    assert_eq!(handle.get(1).expect("empty name read"), None);

    let too_long = "x".repeat(256);
    assert!(handle.set(1, &too_long).is_err());
    assert_eq!(handle.get(1).expect("unchanged empty slot"), None);

    let listed = stats.list(&[]).expect("list");
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].directory_type, DirectoryType::NameVector);
    let dumped = stats.dump(&[id]).expect("dump");
    assert_eq!(
        dumped[0].value,
        DumpValue::NameVector(vec![
            Some("worker-a".to_owned()),
            None,
            Some("worker-c".to_owned()),
        ])
    );
}

#[test]
fn register_histogram_log2_returns_direct_handle_and_bins_snapshot() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [StatsRegistration {
        path: "/latency/histogram",
        descriptor: StatsDescriptor {
            fq_name: "latency_histogram",
            help: "bounded log2 latency bins",
            const_labels: &[],
        },
        value: StatsValueLayout::HistogramLog2 { rows: 2 },
    }];

    let mut entries = stats
        .register(&registrations)
        .expect("histogram registered");
    let StatsEntry::HistogramLog2 { id, handle } = entries.pop().expect("entry") else {
        panic!("histogram registration returned a different value kind");
    };
    handle.increment_bin(0, 3, 2).expect("bin increment");
    handle.increment_value(1, 8, 5).expect("value increment");
    assert_eq!(handle.get(0, 3).expect("bin read"), 2);
    assert_eq!(handle.get(1, 3).expect("value bin read"), 5);

    let listed = stats.list(&[]).expect("list");
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].directory_type, DirectoryType::HistogramLog2);
    let dumped = stats.dump(&[id]).expect("dump");
    let DumpValue::HistogramLog2(rows) = &dumped[0].value else {
        panic!("histogram dump returned a different value kind");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][3], 2);
    assert_eq!(rows[1][3], 5);
    assert_eq!(rows[0].len(), 64);
}

#[test]
fn register_ring_buffer_returns_fixed_row_snapshot_and_schema() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [StatsRegistration {
        path: "/events/ring",
        descriptor: StatsDescriptor {
            fq_name: "event_ring",
            help: "bounded event ring",
            const_labels: &[],
        },
        value: StatsValueLayout::RingBuffer {
            rows: 1,
            capacity: 2,
            entry_size: 3,
            schema: &[1, 2, 3],
        },
    }];

    let mut entries = stats.register(&registrations).expect("ring registered");
    let StatsEntry::RingBuffer { id, handle } = entries.pop().expect("entry") else {
        panic!("ring registration returned a different value kind");
    };
    assert_eq!(handle.schema().expect("schema read"), Some(vec![1, 2, 3]));
    assert_eq!(handle.produce(0, b"abc").expect("first event"), 1);
    assert_eq!(handle.produce(0, b"xyz").expect("second event"), 2);
    assert_eq!(
        handle.latest(0).expect("latest read"),
        Some(b"xyz".to_vec())
    );

    let listed = stats.list(&[]).expect("list");
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].directory_type, DirectoryType::RingBuffer);
    let dumped = stats.dump(&[id]).expect("dump");
    let DumpValue::RingBuffer(rows) = &dumped[0].value else {
        panic!("ring dump returned a different value kind");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].sequence, 2);
    assert_eq!(rows[0].entries, vec![b"abc".to_vec(), b"xyz".to_vec()]);
}

#[test]
fn dump_rejects_uninitialized_directory_tail_after_growth() {
    let mut stats = StatsMain::new().expect("default construction");
    for index in 0..9 {
        let path = format!("/growth/{index}");
        let name = format!("growth_{index}");
        stats
            .add_counter(&path, prometheus::Opts::new(name, "growth"))
            .expect("counter registered");
    }

    let tail = EntryId::try_from((15, 1)).expect("valid uninitialized slot id");
    let error = stats
        .dump(&[tail])
        .expect_err("uninitialized tail rejected");
    assert!(matches!(error, StatsError::NotFound { id } if id == tail));
}

/// Invalid input is rejected during the preparation pass before any
/// directory entry is published.
#[test]
fn register_batch_failure_publishes_nothing() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [
        StatsRegistration {
            path: "/if/rx",
            descriptor: StatsDescriptor {
                fq_name: "rx_bytes",
                help: "bytes received",
                const_labels: &[],
            },
            value: StatsValueLayout::Counter,
        },
        StatsRegistration {
            path: "",
            descriptor: StatsDescriptor {
                fq_name: "invalid_path_metric",
                help: "must not publish",
                const_labels: &[],
            },
            value: StatsValueLayout::Counter,
        },
    ];

    let Err(error) = stats.register(&registrations) else {
        panic!("an invalid registration unexpectedly published");
    };
    assert!(matches!(error, StatsError::InvalidPath(path) if path.is_empty()));
    assert!(stats.list(&[]).expect("list after failed batch").is_empty());
}

/// A late allocator failure preserves the entries published before it.
#[test]
fn register_capacity_failure_preserves_published_prefix() {
    let page = page_size().expect("page size must be queryable");
    let mut stats = StatsMain::with_capacity(2 * page).expect("two pages fit");
    let help = "x".repeat(1_000);
    let count = page / 256 + 32;
    let paths: Vec<String> = (0..count).map(|index| format!("/large/{index}")).collect();
    let names: Vec<String> = (0..count).map(|index| format!("large_{index}")).collect();
    let registrations: Vec<StatsRegistration<'_>> = paths
        .iter()
        .zip(&names)
        .map(|(path, name)| StatsRegistration {
            path,
            descriptor: StatsDescriptor {
                fq_name: name,
                help: &help,
                const_labels: &[],
            },
            value: StatsValueLayout::Counter,
        })
        .collect();

    let Err(error) = stats.register(&registrations) else {
        panic!("capacity-constrained batch unexpectedly succeeded");
    };
    assert!(matches!(error, StatsError::SegmentFull));
    let listed = stats.list(&[]).expect("list after capacity failure");
    assert!(!listed.is_empty(), "successful entries remain published");
    assert!(
        listed
            .iter()
            .enumerate()
            .all(|(index, entry)| entry.path == format!("/large/{index}")),
        "published entries must retain registration order"
    );
}

/// Gauge and timestamp registrations return their concrete direct handles and
/// preserve their distinct VPP directory representations.
#[test]
fn register_gauge_and_timestamp_returns_direct_handles() {
    let mut stats = StatsMain::new().expect("default construction");
    let registrations = [
        StatsRegistration {
            path: "/sys/temp",
            descriptor: StatsDescriptor {
                fq_name: "temperature_celsius",
                help: "current temperature",
                const_labels: &[],
            },
            value: StatsValueLayout::Gauge,
        },
        StatsRegistration {
            path: "/sys/boottime",
            descriptor: StatsDescriptor {
                fq_name: "boottime_seconds",
                help: "process boot time",
                const_labels: &[],
            },
            value: StatsValueLayout::Timestamp,
        },
    ];

    let mut entries = stats
        .register(&registrations)
        .expect("scalar registrations");
    let StatsEntry::Gauge {
        id: gauge_id,
        handle: gauge,
    } = entries.remove(0)
    else {
        panic!("gauge registration returned a different value kind");
    };
    let StatsEntry::Timestamp {
        id: timestamp_id,
        handle: timestamp,
    } = entries.remove(0)
    else {
        panic!("timestamp registration returned a different value kind");
    };
    gauge.set(37.5).expect("set gauge");
    timestamp.set(1_700_000_000).expect("set timestamp");

    let listed = stats.list(&[]).expect("list");
    assert_eq!(listed[0].id, gauge_id);
    assert_eq!(listed[0].directory_type, DirectoryType::Gauge);
    assert_eq!(listed[1].id, timestamp_id);
    assert_eq!(listed[1].directory_type, DirectoryType::ScalarIndex);

    let dumped = stats.dump(&[gauge_id, timestamp_id]).expect("dump");
    assert_eq!(dumped[0].value, DumpValue::Gauge(37.5));
    assert_eq!(dumped[1].value, DumpValue::Gauge(1_700_000_000.0));
}

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

    let clone = timestamp.clone();
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
    let update = counter.clone();
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

/// A new metric after removal decodes exactly its own strings. Descriptor
/// writers always write each NUL terminator explicitly, independent of the
/// retired block storage kept by StatsMain.
#[test]
fn new_block_after_removal_decodes_exact_strings() {
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

    // Removing returns the slot to the free list while StatsMain retains the
    // detached block in its retired allocation set.
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
