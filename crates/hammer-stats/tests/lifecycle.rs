//! Lifecycle tests for the stats segment public seam.

use hammer_infra::page_size;
use hammer_stats::{EntryId, StatsError, StatsMain};

#[test]
fn creation_validates_capacity_and_places_header_first_page() {
    // Zero and sub-page capacities are rejected before any segment is created.
    let zero = StatsMain::with_capacity(0)
        .err()
        .expect("zero capacity must be rejected");
    assert!(
        matches!(zero, StatsError::CapacityTooSmall { minimum, requested } if minimum > 0 && requested == 0),
        "unexpected error: {zero}"
    );

    let page = page_size().expect("page size must be queryable");
    let tiny = StatsMain::with_capacity(page)
        .err()
        .expect("a single page cannot hold header, directory, and a metric");
    assert!(
        matches!(tiny, StatsError::CapacityTooSmall { minimum, requested } if minimum > page && requested == page),
        "unexpected error: {tiny}"
    );

    // The smallest accepted capacity still constructs and drops cleanly.
    let stats = StatsMain::with_capacity(2 * page).expect("two pages fit the minimum layout");
    drop(stats);

    let stats = StatsMain::new().expect("default capacity constructs");
    drop(stats);
}

#[test]
fn add_counter_publishes_and_counts() {
    let mut stats = StatsMain::new().expect("default construction");

    let (id, counter) = stats
        .add_counter(
            "/if/rx",
            prometheus::Opts::new("rx_bytes", "bytes received"),
        )
        .expect("counter added");
    assert_eq!(id.index(), 0);
    assert_eq!(id.generation(), 1);

    counter.increment().expect("increment");
    counter.increment_by(41).expect("increment by");
    assert_eq!(counter.get().expect("read"), 42);

    let clone = counter.try_clone().expect("clone");
    clone.increment_by(8).expect("clone increments");
    assert_eq!(counter.get().expect("read after clone"), 50);
    assert_eq!(clone.get().expect("clone read"), 50);
    drop(clone);
    counter.increment().expect("still live after clone drop");
    assert_eq!(counter.get().expect("final read"), 51);

    // Duplicate paths are rejected before any structure change.
    let dup = stats
        .add_counter("/if/rx", prometheus::Opts::new("rx_bytes", "again"))
        .err()
        .expect("duplicate name must be rejected");
    assert!(
        matches!(dup, StatsError::DuplicateName(_)),
        "unexpected: {dup}"
    );

    // Empty paths are rejected.
    let bad = stats
        .add_counter("", prometheus::Opts::new("empty", "empty path"))
        .err()
        .expect("empty path must be rejected");
    assert!(
        matches!(bad, StatsError::InvalidPath(_)),
        "unexpected: {bad}"
    );

    // Variable labels cannot be represented for a single-value metric.
    let bad = stats
        .add_counter(
            "/if/tx",
            prometheus::Opts::new("tx_bytes", "bytes sent").variable_label("if"),
        )
        .err()
        .expect("variable labels must be rejected");
    assert!(
        matches!(bad, StatsError::InvalidDescriptor(_)),
        "unexpected: {bad}"
    );

    // The next metric lands in the next slot with its own value.
    let (id2, other) = stats
        .add_counter("/sys/uptime", prometheus::Opts::new("uptime", "seconds"))
        .expect("second counter added");
    assert_eq!(id2.index(), 1);
    assert_eq!(id2.generation(), 1);
    other.increment_by(7).expect("other increments");
    assert_eq!(counter.get().expect("first unaffected"), 51);
    assert_eq!(other.get().expect("second value"), 7);
}

#[test]
fn add_gauge_sets_and_reads_floats() {
    let mut stats = StatsMain::new().expect("default construction");

    let (id, gauge) = stats
        .add_gauge(
            "/sys/temp",
            prometheus::Opts::new("temp_c", "core temperature"),
        )
        .expect("gauge added");
    assert_eq!(id.index(), 0);
    assert_eq!(id.generation(), 1);

    gauge.set(36.5).expect("set");
    assert_eq!(gauge.get().expect("read"), 36.5);
    gauge.set(-2.25).expect("set negative");
    assert_eq!(gauge.get().expect("read negative"), -2.25);

    let clone = gauge.try_clone().expect("clone");
    clone.set(99.0).expect("clone sets");
    assert_eq!(gauge.get().expect("read after clone"), 99.0);
    drop(clone);
    gauge.set(0.0).expect("still live after clone drop");
    assert_eq!(gauge.get().expect("final read"), 0.0);

    // A gauge shares the slot sequence with counters.
    let (id2, counter) = stats
        .add_counter("/sys/ticks", prometheus::Opts::new("ticks", "monotonic"))
        .expect("counter after gauge");
    assert_eq!(id2.index(), 1);
    counter.increment().expect("counter increments");
    assert_eq!(gauge.get().expect("gauge unaffected"), 0.0);
}

#[test]
fn directory_grows_past_the_initial_slots() {
    let mut stats = StatsMain::new().expect("default construction");

    // The first 8 metrics fit the initial directory; the 9th forces a
    // replacement (8 -> 16) and the 17th a second one (16 -> 32).
    let mut counters = Vec::new();
    for i in 0..24 {
        let name = format!("/stats/m{i}");
        let (id, counter) = stats
            .add_counter(&name, prometheus::Opts::new(format!("m{i}"), "growth"))
            .expect("metric added");
        assert_eq!(id.index(), i as u32);
        assert_eq!(id.generation(), 1);
        counter.increment_by(i as u64).expect("increment");
        counters.push(counter);
    }

    // Entries survived relocation: every handle still sees its value.
    for (i, counter) in counters.iter().enumerate() {
        assert_eq!(counter.get().expect("value after growth"), i as u64);
    }

    // The name index still rejects duplicates after relocation.
    let dup = stats
        .add_counter("/stats/m5", prometheus::Opts::new("m5", "again"))
        .err()
        .expect("duplicate after growth");
    assert!(
        matches!(dup, StatsError::DuplicateName(_)),
        "unexpected: {dup}"
    );

    // Adds keep working in the grown directory.
    let (id25, counter) = stats
        .add_counter("/stats/m24", prometheus::Opts::new("m24", "after growth"))
        .expect("metric after growth");
    assert_eq!(id25.index(), 24);
    counter.increment().expect("increment after growth");
    assert_eq!(counter.get().expect("value"), 1);
}

#[test]
fn remove_entry_hides_metric_and_reuses_slot_with_new_generation() {
    let mut stats = StatsMain::new().expect("default construction");

    let (id0, c0) = stats
        .add_counter("/a", prometheus::Opts::new("a", "first"))
        .expect("first counter");
    let (_, c1) = stats
        .add_counter("/b", prometheus::Opts::new("b", "second"))
        .expect("second counter");
    c0.increment_by(5).expect("increment");
    c1.increment_by(7).expect("increment");

    // Stale generations and out-of-range indices are rejected typed.
    let stale = EntryId::try_from((id0.index(), id0.generation() + 1)).expect("stale id");
    let err = stats.remove_entry(stale).err().expect("stale id rejected");
    assert!(
        matches!(err, StatsError::StaleEntry { id } if id == stale),
        "unexpected: {err}"
    );
    let missing = EntryId::try_from((99, 1)).expect("valid id");
    let err = stats
        .remove_entry(missing)
        .err()
        .expect("missing index rejected");
    assert!(
        matches!(err, StatsError::NotFound { id } if id == missing),
        "unexpected: {err}"
    );

    // Removal hides the entry; live handles go stale, others keep working.
    stats.remove_entry(id0).expect("removed");
    let err = c0.increment().err().expect("stale handle rejected");
    assert!(
        matches!(err, StatsError::StaleEntry { id } if id == id0),
        "unexpected: {err}"
    );
    c1.increment().expect("other counter unaffected");
    assert_eq!(c1.get().expect("value"), 8);

    // A second removal of the same id is NotFound.
    let err = stats
        .remove_entry(id0)
        .err()
        .expect("second removal rejected");
    assert!(
        matches!(err, StatsError::NotFound { id } if id == id0),
        "unexpected: {err}"
    );

    // While a handle is live the removed slot is not reused; the name is
    // free again, so a re-add lands in a fresh slot.
    let (id2, c2) = stats
        .add_counter("/a", prometheus::Opts::new("a", "reused"))
        .expect("re-add with live handle");
    assert_eq!(id2.index(), 2);
    assert_eq!(id2.generation(), 1);
    c2.increment_by(3).expect("increment");
    assert_eq!(c2.get().expect("value"), 3);
    assert!(matches!(
        c0.increment().err().expect("old handle still stale"),
        StatsError::StaleEntry { .. }
    ));

    // Dropping the last live handle lets the next structural pass reclaim
    // the slot, which the next add reuses with a bumped generation.
    drop(c0);
    let (id3, c3) = stats
        .add_counter("/c", prometheus::Opts::new("c", "reclaimed"))
        .expect("add after reclaim");
    assert_eq!(id3.index(), id0.index());
    assert_eq!(id3.generation(), 2);
    c3.increment_by(4).expect("increment");
    assert_eq!(c3.get().expect("value"), 4);
}

/// Generation 0 is never published, so the boundary conversion rejects it
/// typed instead of building an id that can never match an entry.
#[test]
fn entry_id_conversion_rejects_generation_zero() {
    let error = EntryId::try_from((5, 0))
        .err()
        .expect("generation zero must be rejected");
    assert!(
        matches!(
            error,
            StatsError::InvalidEntryId {
                index: 5,
                generation: 0
            }
        ),
        "unexpected: {error}"
    );
    assert!(
        matches!(
            EntryId::try_from((0, 0)),
            Err(StatsError::InvalidEntryId { .. })
        ),
        "index zero with generation zero must also be rejected"
    );

    // A non-zero generation is accepted and the accessors return the pair.
    let id = EntryId::try_from((5, 7)).expect("valid id");
    assert_eq!(id.index(), 5);
    assert_eq!(id.generation(), 7);
}

#[test]
fn tiny_segment_exhausts_with_segment_full() {
    let page = page_size().expect("page size must be queryable");
    // Two pages is the smallest accepted capacity: one page is reserved for
    // the header, and the arena holds only the initial directory plus a
    // handful of metric blocks.
    let mut stats = StatsMain::with_capacity(2 * page).expect("tiny segment");

    let mut first = None;
    let mut count = 0u64;
    loop {
        let name = format!("/tiny/m{count}");
        match stats.add_counter(&name, prometheus::Opts::new(format!("m{count}"), "tiny")) {
            Ok((id, counter)) => {
                if first.is_none() {
                    first = Some((id, counter));
                }
                count += 1;
            }
            Err(error) => {
                assert!(
                    matches!(error, StatsError::SegmentFull),
                    "expected SegmentFull, got: {error}"
                );
                break;
            }
        }
        assert!(count < 1_000_000, "segment never exhausted");
    }
    assert!(
        count >= 4,
        "the arena must hold a few metrics before exhausting"
    );

    // Freeing one metric's block (handle dropped, then removed) lets the
    // next add reuse its slot without growing anything.
    let (id0, counter0) = first.take().expect("first metric");
    drop(counter0);
    stats.remove_entry(id0).expect("remove releases the block");
    let (id, counter) = stats
        .add_counter("/tiny/reuse", prometheus::Opts::new("reuse", "tiny"))
        .expect("add after remove");
    assert_eq!(id.index(), id0.index());
    assert_eq!(id.generation(), 2);
    counter.increment().expect("increment");
    assert_eq!(counter.get().expect("value"), 1);
}
