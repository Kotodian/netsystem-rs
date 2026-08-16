//! The public stats segment API.

use std::alloc::Layout;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use regex::Regex;

use hammer_infra::segment::{Segment, SegmentAllocation};

use crate::directory::{
    DirectorySlot, DirectoryType, EntryState, NULL_INDEX, PrometheusType, SLOT_SIZE, encode_name,
};
use crate::error::StatsError;
use crate::header::StatsHeader;
use crate::mapping::Mapping;
use crate::metric_value::MetricValue;
use crate::offset::Offset;
use crate::read::{DirectoryEntry, DumpEntry, DumpValue};

/// Default stats segment size, mirroring VPP's 32 MiB default
/// (`STAT_SEGMENT_DEFAULT_SIZE`).
pub const DEFAULT_CAPACITY: usize = 32 << 20;

/// Initial directory slot count: one 2 KiB block.
const INITIAL_DIRECTORY_SLOTS: u64 = 8;

/// Maximum stable-read attempts before a reader gives up on a segment that
/// is being continuously republished.
const MAX_READ_ATTEMPTS: usize = 4;

/// Bytes required beyond the reserved first page: the header record, the
/// initial directory block, and the smallest possible metric block.
const MIN_TAIL_BYTES: usize = std::mem::size_of::<StatsHeader>()
    + (INITIAL_DIRECTORY_SLOTS as usize) * SLOT_SIZE
    + crate::descriptor::MIN_BLOCK_BYTES as usize;

/// Identifies one directory entry across slot reuse.
///
/// Captured at add time; `remove_entry` accepts exactly this pair, so a
/// stale `EntryId` (whose slot has since been reused) is rejected instead
/// of acting on a different metric.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EntryId {
    /// Slot index in the current directory block.
    index: u32,
    /// Slot generation captured at add time; mismatch means stale.
    generation: u64,
}

impl EntryId {
    /// Slot index in the current directory block.
    pub const fn index(self) -> u32 {
        self.index
    }

    /// Slot generation captured at add time; mismatch means stale.
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

/// Builds an [`EntryId`] from a raw `(index, generation)` pair. Generation 0
/// is never published by the segment, so it is rejected typed.
impl TryFrom<(u32, u64)> for EntryId {
    type Error = StatsError;

    fn try_from((index, generation): (u32, u64)) -> Result<Self, Self::Error> {
        if generation == 0 {
            return Err(StatsError::InvalidEntryId { index, generation });
        }
        Ok(Self { index, generation })
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "index {} generation {}", self.index, self.generation)
    }
}

/// A live handle to one counter value record.
///
/// Owns one reference on the value record; the record's generation is
/// checked on every operation so a handle outliving `remove_entry` fails
/// with [`StatsError::StaleEntry`] instead of writing into a reused block.
pub struct Counter {
    segment: Segment,
    value_offset: Offset,
    /// The entry id captured at add time; a generation mismatch on any
    /// operation means the entry was removed and its slot possibly reused.
    id: EntryId,
}

impl Counter {
    /// Duplicates the handle; both handles update the same value record.
    pub fn try_clone(&self) -> Result<Counter, StatsError> {
        self.with_value(MetricValue::try_add_ref)?;
        Ok(Counter {
            segment: self.segment.clone(),
            value_offset: self.value_offset,
            id: self.id,
        })
    }

    /// Increments the value by one.
    pub fn increment(&self) -> Result<(), StatsError> {
        self.with_value(|value| {
            value.add_value(1);
            Ok(())
        })
    }

    /// Increments the value by `delta`.
    pub fn increment_by(&self, delta: u64) -> Result<(), StatsError> {
        self.with_value(|value| {
            value.add_value(delta);
            Ok(())
        })
    }

    /// Reads the current value.
    pub fn get(&self) -> Result<u64, StatsError> {
        self.with_value(|value| Ok(value.load_value()))
    }

    /// Runs `op` on the value record after a generation check that rejects
    /// stale handles. The record outlives directory relocation, so handles
    /// are never invalidated by directory growth. `op` is inlined; there is
    /// no dynamic dispatch.
    fn with_value<T>(
        &self,
        op: impl FnOnce(&MetricValue) -> Result<T, StatsError>,
    ) -> Result<T, StatsError> {
        let mapping = Mapping::new(&self.segment);
        let value = mapping.metric_value(self.value_offset)?;
        if value.generation() != self.id.generation {
            return Err(StatsError::StaleEntry { id: self.id });
        }
        op(value)
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        if let Ok(value) = Mapping::new(&self.segment).metric_value(self.value_offset) {
            value.release_ref();
        }
    }
}

/// A live handle to one gauge value record.
///
/// Same lifetime and staleness rules as [`Counter`]; the value is stored as
/// its IEEE-754 bit pattern.
pub struct Gauge {
    segment: Segment,
    value_offset: Offset,
    /// The entry id captured at add time; see [`Counter`].
    id: EntryId,
}

impl Gauge {
    /// Duplicates the handle; both handles update the same value record.
    pub fn try_clone(&self) -> Result<Gauge, StatsError> {
        self.with_value(MetricValue::try_add_ref)?;
        Ok(Gauge {
            segment: self.segment.clone(),
            value_offset: self.value_offset,
            id: self.id,
        })
    }

    /// Sets the value.
    pub fn set(&self, value: f64) -> Result<(), StatsError> {
        self.with_value(|record| {
            record.store_value(value.to_bits());
            Ok(())
        })
    }

    /// Reads the current value.
    pub fn get(&self) -> Result<f64, StatsError> {
        self.with_value(|record| Ok(f64::from_bits(record.load_value())))
    }

    /// Runs `op` on the value record after a generation check; see
    /// [`Counter::with_value`].
    fn with_value<T>(
        &self,
        op: impl FnOnce(&MetricValue) -> Result<T, StatsError>,
    ) -> Result<T, StatsError> {
        let mapping = Mapping::new(&self.segment);
        let value = mapping.metric_value(self.value_offset)?;
        if value.generation() != self.id.generation {
            return Err(StatsError::StaleEntry { id: self.id });
        }
        op(value)
    }
}

impl Drop for Gauge {
    fn drop(&mut self) {
        if let Ok(value) = Mapping::new(&self.segment).metric_value(self.value_offset) {
            value.release_ref();
        }
    }
}

/// A live handle to one timestamp value record.
///
/// VPP exposes its `/sys` boot time, heartbeat, and last-stats-clear as
/// scalar entries (stats.h:29-31); Hammer models such a scalar metric as a
/// Prometheus gauge whose value is a plain integer, recorded as
/// [`PrometheusType::Gauge`] with [`DirectoryType::ScalarIndex`].
pub struct Timestamp {
    segment: Segment,
    value_offset: Offset,
    /// The entry id captured at add time; see [`Counter`].
    id: EntryId,
}

impl Timestamp {
    /// Duplicates the handle; both handles update the same value record.
    pub fn try_clone(&self) -> Result<Timestamp, StatsError> {
        self.with_value(MetricValue::try_add_ref)?;
        Ok(Timestamp {
            segment: self.segment.clone(),
            value_offset: self.value_offset,
            id: self.id,
        })
    }

    /// Sets the value (e.g. a `SystemTime` epoch second).
    pub fn set(&self, value: u64) -> Result<(), StatsError> {
        self.with_value(|record| {
            record.store_value(value);
            Ok(())
        })
    }

    /// Reads the current value.
    pub fn get(&self) -> Result<u64, StatsError> {
        self.with_value(|record| Ok(record.load_value()))
    }

    /// Runs `op` on the value record after a generation check; see
    /// [`Counter::with_value`].
    fn with_value<T>(
        &self,
        op: impl FnOnce(&MetricValue) -> Result<T, StatsError>,
    ) -> Result<T, StatsError> {
        let mapping = Mapping::new(&self.segment);
        let value = mapping.metric_value(self.value_offset)?;
        if value.generation() != self.id.generation {
            return Err(StatsError::StaleEntry { id: self.id });
        }
        op(value)
    }
}

impl Drop for Timestamp {
    fn drop(&mut self) {
        if let Ok(value) = Mapping::new(&self.segment).metric_value(self.value_offset) {
            value.release_ref();
        }
    }
}

/// The stats segment: header, directory, and metric values in shared memory.
///
/// The segment is backed by one shared-memory mapping whose first page is
/// reserved for the versioned [`crate::header::StatsHeader`]. The directory
/// and every metric block are owned `SegmentAllocation`s; `&mut StatsMain`
/// is the sole structural writer, publishing each change atomically under
/// the header's `in_progress` sequence marker.
pub struct StatsMain {
    segment: Segment,
    /// Process-local name index mirroring VPP's `directory_vector_by_name`
    /// (stats.c:78-123,196): active name -> (index, generation).
    ///
    /// VPP keeps that hash in the process-local stats segment structure, not
    /// in shared memory; the authoritative name always lives in the
    /// directory entry inside the segment. This map is a rebuildable
    /// acceleration index giving O(1)-expected duplicate detection on add,
    /// kept in step on successful add and remove. It can be rebuilt from
    /// the segment by scanning active entries and is never read by readers
    /// of the shared segment.
    names: HashMap<Box<str>, EntryId>,
    /// Registered collector closures, run by [`StatsMain::collect`] in
    /// registration order.
    ///
    /// The `FnMut` bound makes `StatsMain` `!Sync`, matching its role: this
    /// handle is the sole structural writer of its segment, so any reader
    /// is an alias within one thread and needs no cross-thread locking.
    collectors: Vec<Box<dyn FnMut() -> Result<(), StatsError> + Send + 'static>>,
}

impl StatsMain {
    /// Creates a stats segment of [`DEFAULT_CAPACITY`] bytes.
    pub fn new() -> Result<StatsMain, StatsError> {
        StatsMain::with_capacity(DEFAULT_CAPACITY)
    }

    /// Creates a stats segment of at least `capacity` bytes, page-rounded.
    ///
    /// The capacity must hold the reserved first page, the shared header,
    /// the initial directory, and at least one metric; smaller requests are
    /// rejected with [`StatsError::CapacityTooSmall`].
    pub fn with_capacity(capacity: usize) -> Result<StatsMain, StatsError> {
        let page = hammer_infra::page_size()?;
        let minimum = page + MIN_TAIL_BYTES;
        if capacity < minimum {
            return Err(StatsError::CapacityTooSmall {
                minimum,
                requested: capacity,
            });
        }
        let total = align_up(capacity, page).ok_or(StatsError::OutOfBounds)?;
        let segment = Segment::shared_with_reserved_prefix(&unique_segment_name(), total, page)?;

        // The initial directory is the first arena allocation, so it lands
        // directly after the reserved first page, 64-byte aligned.
        let directory_layout =
            Layout::from_size_align((INITIAL_DIRECTORY_SLOTS as usize) * SLOT_SIZE, 64)
                .map_err(|_| StatsError::InvalidLayout)?;
        let directory = segment.allocate(directory_layout)?;
        let directory_offset = Offset::new(directory.into_raw_offset());

        let mapping = Mapping::new(&segment);
        mapping.write_header(StatsHeader::new(
            total as u64,
            directory_offset.get(),
            INITIAL_DIRECTORY_SLOTS,
        ));

        Ok(StatsMain {
            segment,
            names: HashMap::new(),
            collectors: Vec::new(),
        })
    }

    /// Adds a counter metric, mirroring VPP's `vlib_stats_add_counter_vector`.
    ///
    /// Publishes a directory entry and returns an [`EntryId`] plus a
    /// [`Counter`] handle owning the value record. The `Opts` must carry a
    /// valid fq name and help; variable labels are rejected. The entry is a
    /// scalar (`DirectoryType::ScalarIndex`), as are VPP's `/sys` heartbeat,
    /// boottime, and last-stats-clear metrics (stats.h:29-31, stats.c:281).
    pub fn add_counter(
        &mut self,
        path: &str,
        opts: prometheus::Opts,
    ) -> Result<(EntryId, Counter), StatsError> {
        let (id, value_offset) = self.add_metric(
            path,
            &opts,
            PrometheusType::Counter,
            DirectoryType::ScalarIndex,
        )?;
        Ok((
            id,
            Counter {
                segment: self.segment.clone(),
                value_offset,
                id,
            },
        ))
    }

    /// Adds a gauge metric, mirroring VPP's `vlib_stats_add_gauge`.
    ///
    /// Same contract as [`StatsMain::add_counter`]; the returned [`Gauge`]
    /// stores an `f64` value.
    pub fn add_gauge(
        &mut self,
        path: &str,
        opts: prometheus::Opts,
    ) -> Result<(EntryId, Gauge), StatsError> {
        let (id, value_offset) =
            self.add_metric(path, &opts, PrometheusType::Gauge, DirectoryType::Gauge)?;
        Ok((
            id,
            Gauge {
                segment: self.segment.clone(),
                value_offset,
                id,
            },
        ))
    }

    /// Adds a timestamp scalar, mirroring VPP's `/sys` boottime, heartbeat,
    /// and last-stats-clear metrics (stats.h:29-31, stats.c:281).
    ///
    /// The metric is a Prometheus gauge whose value is a plain integer
    /// (`PrometheusType::Gauge` with `DirectoryType::ScalarIndex`).
    pub fn add_timestamp(
        &mut self,
        path: &str,
        opts: prometheus::Opts,
    ) -> Result<(EntryId, Timestamp), StatsError> {
        let (id, value_offset) = self.add_metric(
            path,
            &opts,
            PrometheusType::Gauge,
            DirectoryType::ScalarIndex,
        )?;
        Ok((
            id,
            Timestamp {
                segment: self.segment.clone(),
                value_offset,
                id,
            },
        ))
    }

    /// Registers a collector closure, run by [`StatsMain::collect`].
    ///
    /// Collectors capture the metric handles they update (`Counter`,
    /// `Gauge`, `Timestamp`) — the update capability VPP gives a collector
    /// through its entry index (stats.c:590-604).
    ///
    /// Complexity: O(1) amortized plus one box.
    pub fn register_collector(
        &mut self,
        collector: impl FnMut() -> Result<(), StatsError> + Send + 'static,
    ) {
        self.collectors.push(Box::new(collector));
    }

    /// Runs every registered collector once, in registration order.
    ///
    /// No directory, epoch, or allocation work: collectors update their
    /// captured handles directly, as in VPP's `do_stat_segment_updates`
    /// pass (collector.c:135-158). Every collector runs even when an
    /// earlier one failed; the first error in registration order is
    /// returned once the pass completes.
    ///
    /// Complexity: O(collectors), no allocation.
    pub fn collect(&mut self) -> Result<(), StatsError> {
        let mut first_error = None;
        for collector in &mut self.collectors {
            if let Err(error) = collector() {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Returns owned copies of the active directory entries whose paths
    /// match any of `patterns` (empty patterns select all; OR semantics),
    /// in ascending directory-index order.
    ///
    /// The patterns are compiled once before any segment read; an invalid
    /// pattern is a typed [`StatsError::InvalidPattern`] carrying the exact
    /// pattern and the regex [`std::error::Error`] as its source. Each
    /// entry's descriptor block is bounds-checked and decoded into owned
    /// strings, so no borrowed data escapes the call.
    ///
    /// The stability loop mirrors VPP's client protocol
    /// (stat_client.c:370-404, 429): `in_progress` and the epoch bracket
    /// the owned-copy build, and a bounded retry replaces VPP's "Epoch
    /// changed while reading, invalid results" failure with
    /// [`StatsError::ReadBusy`]. `StatsMain` is `!Sync`, so a reader is
    /// always an alias within the writing thread and cannot race a
    /// structural write; the epoch check guards against republish windows
    /// that would make the copy internally inconsistent.
    ///
    /// Complexity: O(patterns compiled) + O(initialized slots x patterns) +
    /// O(copied descriptor bytes); the result is fully owned.
    pub fn list(&self, patterns: &[String]) -> Result<Vec<DirectoryEntry>, StatsError> {
        let regexes: Vec<Regex> = patterns
            .iter()
            .map(|pattern| {
                Regex::new(pattern).map_err(|error| StatsError::InvalidPattern {
                    pattern: pattern.clone(),
                    source: error,
                })
            })
            .collect::<Result<_, _>>()?;

        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let mut result: Vec<DirectoryEntry> = Vec::new();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let epoch = header.epoch();
            if header.in_progress() != 0 {
                if attempts >= MAX_READ_ATTEMPTS {
                    return Err(StatsError::ReadBusy);
                }
                std::hint::spin_loop();
                continue;
            }

            let directory_offset = header.directory_offset();
            let initialized = header.initialized_len().min(u64::from(u32::MAX)) as u32;
            result.clear();
            for index in 0..initialized {
                let slot = mapping.entry(directory_offset, index)?;
                if slot.state()? != EntryState::Active {
                    continue;
                }
                let path = std::str::from_utf8(slot.name())
                    .map_err(|_| StatsError::InvalidState(slot.state_byte()))?
                    .to_owned();
                let matched = regexes.is_empty() || regexes.iter().any(|re| re.is_match(&path));
                if !matched {
                    continue;
                }
                let decoded = crate::descriptor::decode_descriptor(
                    mapping.descriptor_block(slot.descriptor_offset())?,
                )?;
                result.push(DirectoryEntry {
                    id: EntryId {
                        index,
                        generation: slot.generation(),
                    },
                    path,
                    directory_type: slot.directory_type()?,
                    prometheus_type: slot.prometheus_type()?,
                    fq_name: decoded.fq_name,
                    help: decoded.help,
                    const_labels: decoded.const_labels,
                });
            }

            // The acquire fence orders the copied reads before the epoch
            // re-check. The snapshot at the loop top is an acquire read, so
            // the copy stays between the two checks; the re-check is the
            // read side of the writer's release clear
            // (`clear_in_progress`, the analogue of VPP's
            // `__atomic_store_n (&in_progress, 0, __ATOMIC_RELEASE)`,
            // stats.c:49), so a publication observed by the re-check is
            // fully visible and any overlap discards the copy instead of
            // returning it. The writer's `mark_in_progress` is a seq_cst
            // store — the begin boundary VPP's structural spinlock
            // supplies — so no structural write can become visible before
            // the marker.
            std::sync::atomic::fence(Ordering::Acquire);
            if header.in_progress() == 0 && header.epoch() == epoch {
                return Ok(result);
            }
            if attempts >= MAX_READ_ATTEMPTS {
                return Err(StatsError::ReadBusy);
            }
            std::hint::spin_loop();
        }
    }

    /// Returns owned point-in-time copies of the entries named by `ids`,
    /// preserving input order and duplicates.
    ///
    /// Each id is validated against the directory (index bounds, slot
    /// state, entry generation) and the value record (generation), so
    /// missing and stale ids stay typed
    /// [`StatsError::NotFound`]/[`StatsError::StaleEntry`]. The type pair
    /// is decoded into the [`DumpValue`] for the metric:
    /// Counter+ScalarIndex -> `u64`, Gauge+ScalarIndex (a timestamp) ->
    /// `u64 as f64`, Gauge+Gauge -> `f64::from_bits`; any other combination
    /// is a typed [`StatsError::IncompatibleType`].
    ///
    /// Same stable-epoch protocol as [`StatsMain::list`]; no descriptor
    /// parse and no collector work.
    ///
    /// Complexity: O(ids).
    pub fn dump(&self, ids: &[EntryId]) -> Result<Vec<DumpEntry>, StatsError> {
        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let mut result: Vec<DumpEntry> = Vec::new();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let epoch = header.epoch();
            if header.in_progress() != 0 {
                if attempts >= MAX_READ_ATTEMPTS {
                    return Err(StatsError::ReadBusy);
                }
                std::hint::spin_loop();
                continue;
            }

            let directory_offset = header.directory_offset();
            let capacity = header.directory_capacity();
            result.clear();
            for &id in ids {
                if u64::from(id.index) >= capacity {
                    return Err(StatsError::NotFound { id });
                }
                let slot = mapping.entry(directory_offset, id.index)?;
                if slot.state()? != EntryState::Active {
                    return Err(StatsError::NotFound { id });
                }
                if slot.generation() != id.generation {
                    return Err(StatsError::StaleEntry { id });
                }
                // Active-generation invariant: the slot, the id, and the
                // value record all carry exactly the same generation
                // (removal advances slot and record together; reuse keeps
                // them equal), so a mismatch means the entry changed under
                // the read.
                let record = mapping.metric_value(slot.value_offset())?;
                if record.generation() != id.generation {
                    return Err(StatsError::StaleEntry { id });
                }
                let prometheus_type = slot.prometheus_type()?;
                let directory_type = slot.directory_type()?;
                let dump_value = match (prometheus_type, directory_type) {
                    (PrometheusType::Counter, DirectoryType::ScalarIndex) => {
                        DumpValue::Counter(record.load_value())
                    }
                    (PrometheusType::Gauge, DirectoryType::ScalarIndex) => {
                        DumpValue::Gauge(record.load_value() as f64)
                    }
                    (PrometheusType::Gauge, DirectoryType::Gauge) => {
                        DumpValue::Gauge(f64::from_bits(record.load_value()))
                    }
                    _ => {
                        return Err(StatsError::IncompatibleType {
                            id,
                            prometheus_type,
                            directory_type,
                        });
                    }
                };
                result.push(DumpEntry {
                    id,
                    path: std::str::from_utf8(slot.name())
                        .map_err(|_| StatsError::InvalidState(slot.state_byte()))?
                        .to_owned(),
                    directory_type,
                    prometheus_type,
                    value: dump_value,
                });
            }

            // Same acquire fence as `list`: the copy stays between the
            // acquire snapshot and this re-check, whose zero read
            // synchronizes with the writer's release clear (the seq_cst
            // mark supplies the begin boundary on the writer side).
            std::sync::atomic::fence(Ordering::Acquire);
            if header.in_progress() == 0 && header.epoch() == epoch {
                return Ok(result);
            }
            if attempts >= MAX_READ_ATTEMPTS {
                return Err(StatsError::ReadBusy);
            }
            std::hint::spin_loop();
        }
    }

    /// Removes the entry identified by `id`, mirroring VPP's
    /// `vlib_stats_remove_entry`.
    ///
    /// The entry is hidden (it no longer participates in duplicate-name
    /// checks or slot reuse) and every live handle to its value record is
    /// invalidated via a generation bump. If no handle survives, the metric
    /// block is released and the slot joins the free list immediately;
    /// otherwise the slot joins the removed list and is reclaimed by the
    /// next structural pass once its handles are gone.
    pub fn remove_entry(&mut self, id: EntryId) -> Result<(), StatsError> {
        self.release_removed_entries()?;

        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();
        let capacity = header.directory_capacity();
        if u64::from(id.index) >= capacity {
            return Err(StatsError::NotFound { id });
        }
        let entry = mapping.entry(directory_offset, id.index)?;
        match entry.state()? {
            EntryState::Active => {}
            EntryState::Free | EntryState::Removed => {
                return Err(StatsError::NotFound { id });
            }
        }
        if entry.generation() != id.generation {
            return Err(StatsError::StaleEntry { id });
        }

        // Preparation: the one checked generation increment, applied to the
        // value record (which staleness-invalidates direct handles) and to
        // the slot metadata, so a later free-list reuse publishes the
        // already-advanced generation without a second increment. Also
        // decide whether the block can be released in this pass and prepare
        // the checked write target and the name key. All checked work
        // happens before the publication tail.
        let value = mapping.metric_value(entry.value_offset())?;
        let next_generation = value.next_generation()?;
        let release_now = value.refs() == 0;
        let target = mapping.entry_write_target(directory_offset, id.index)?;
        // The segment name is always NUL-terminated UTF-8 written from a
        // `&str`; a decode failure can only mean a corrupt slot.
        let removed_name: Box<str> = std::str::from_utf8(entry.name())
            .map_err(|_| StatsError::InvalidState(entry.state_byte()))?
            .into();

        let mut removed = entry;
        removed.set_generation(next_generation);
        if release_now {
            removed.set_state(EntryState::Free);
            removed.set_link(header.free_list_head());
        } else {
            removed.set_state(EntryState::Removed);
            removed.set_link(header.removed_list_head());
        }

        // Publication: VPP header sequence stores (stats.c:27,48-49) — set
        // `in_progress`, prevalidated infallible writes, bump `epoch`, clear
        // `in_progress`.
        header.mark_in_progress();
        // SAFETY: `target` was computed by `entry_write_target` for this
        // exact slot during this preparation phase.
        unsafe { mapping.write_entry(target, removed) };
        value.store_generation(next_generation);
        if release_now {
            header.store_free_list_head(u64::from(id.index));
        } else {
            header.store_removed_list_head(u64::from(id.index));
        }
        header.bump_epoch();
        header.clear_in_progress();

        // The name is free the moment the entry is hidden; the index must
        // not outlive the entry it names.
        self.names.remove(&removed_name);

        if release_now {
            self.release_metric_storage(&mapping, &entry)?;
        }
        Ok(())
    }

    /// The shared add path: normalize, duplicate-check, select a slot,
    /// allocate and write the metric block, then publish.
    ///
    /// Slot selection mirrors VPP's `vlib_stats_create_counter`: reuse the
    /// free-list head first, else append at the vector high-water mark.
    /// All fallible and checked work (duplicate check, layout, allocation,
    /// block write, slot validation) happens before `in_progress` is set;
    /// the publication tail performs no arithmetic and no fallible work.
    fn add_metric(
        &mut self,
        path: &str,
        opts: &prometheus::Opts,
        kind: PrometheusType,
        directory_type: DirectoryType,
    ) -> Result<(EntryId, Offset), StatsError> {
        // Release blocks whose handles are gone before adding.
        self.release_removed_entries()?;
        let descriptor = crate::descriptor::normalize(opts, kind)?;
        let name_key: Box<str> = path.into();
        let name = encode_name(&name_key)?;

        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();
        let capacity = header.directory_capacity();
        let initialized = header.initialized_len();

        // Duplicate-name rejection: O(1)-expected via the process-local name
        // index, mirroring VPP's `directory_vector_by_name` lookup in
        // `vlib_stats_find_entry_index` (stats.c:78-81), with the generation
        // carried so a rebuilt index can never shadow a fresh name.
        if self.names.contains_key(path) {
            return Err(StatsError::DuplicateName(path.to_owned()));
        }

        // Slot selection: free-list reuse first (VPP's
        // `dir_vector_first_free_elt`), else append at the high-water mark,
        // growing the directory if full. The effective directory offset is
        // part of the result because replacement relocates the block.
        let free_head = header.free_list_head();
        let (directory_offset, index, generation, appended, next_free_head) =
            if free_head != NULL_INDEX {
                if free_head >= capacity {
                    return Err(StatsError::InvalidState(0xFF));
                }
                let free_entry = mapping.entry(directory_offset, free_head as u32)?;
                if free_entry.state()? != EntryState::Free {
                    return Err(StatsError::InvalidState(free_entry.state_byte()));
                }
                // Reuse publishes the slot's already-advanced generation
                // (advanced exactly once at removal); never-used appended
                // slots start at 1.
                let generation = free_entry.generation();
                (
                    directory_offset,
                    free_head as u32,
                    generation,
                    false,
                    Some(free_entry.link()),
                )
            } else {
                let (directory_offset, capacity, initialized) = if initialized < capacity {
                    (directory_offset, capacity, initialized)
                } else {
                    self.replace_directory(&mapping, &header)?;
                    (
                        header.directory_offset(),
                        header.directory_capacity(),
                        header.initialized_len(),
                    )
                };
                if initialized >= capacity {
                    return Err(StatsError::SegmentFull);
                }
                (directory_offset, initialized as u32, 1, true, None)
            };

        // Allocate and write the metric block.
        let layout = crate::descriptor::block_layout(&descriptor)?;
        let mut block = self.segment.allocate(layout)?;
        let value_offset =
            crate::descriptor::write_block(&mut block.bytes_mut(), &descriptor, generation)?;
        let block_offset = Offset::new(block.into_raw_offset());
        // The entry and every handle use the mapping-relative value offset.
        let value_offset = block_offset
            .checked_add(value_offset)
            .ok_or(StatsError::OutOfBounds)?;

        // Preparation: the entry value, the checked write target, and the
        // checked successor length. Everything checked happens before
        // `in_progress` is set.
        let entry = DirectorySlot::new_active(
            name,
            generation,
            directory_type,
            kind,
            block_offset,
            value_offset,
        );
        let target = mapping.entry_write_target(directory_offset, index)?;
        let next_initialized = initialized.checked_add(1).ok_or(StatsError::OutOfBounds)?;

        // Publication: VPP header sequence stores (stats.c:27,48-49) — set
        // `in_progress`, prevalidated infallible writes, bump `epoch`, clear
        // `in_progress`.
        header.mark_in_progress();
        // SAFETY: `target` was computed by `entry_write_target` for this
        // exact slot during this preparation phase.
        unsafe { mapping.write_entry(target, entry) };
        if let Some(head) = next_free_head {
            header.store_free_list_head(head);
        }
        if appended {
            header.store_initialized_len(next_initialized);
        }
        header.bump_epoch();
        header.clear_in_progress();

        // The name is active only once the entry is published; keeping the
        // index in step here keeps it a pure cache of the segment state.
        let id = EntryId { index, generation };
        self.names.insert(name_key, id);

        // The handle expectation is the entry generation, so slot, id,
        // handle, and value record stay exactly equal while active.
        Ok((id, value_offset))
    }

    /// Grows the directory to twice its slot count, copying every
    /// initialized slot (active, free, and removed alike) by value into the
    /// new block. Mirrors VPP's vector growth: slot indices and list links
    /// are index-based, so relocation is invisible to entries.
    fn replace_directory(
        &mut self,
        mapping: &Mapping,
        header: &StatsHeader,
    ) -> Result<(), StatsError> {
        let old_offset = header.directory_offset();
        let old_capacity = header.directory_capacity();
        let initialized = header.initialized_len();
        let new_capacity = old_capacity.checked_mul(2).ok_or(StatsError::OutOfBounds)?;
        let old_bytes = (old_capacity as usize)
            .checked_mul(SLOT_SIZE)
            .ok_or(StatsError::OutOfBounds)?;
        let new_bytes = (new_capacity as usize)
            .checked_mul(SLOT_SIZE)
            .ok_or(StatsError::OutOfBounds)?;

        // Copy initialized slots by value (validated reads).
        let mut entries = Vec::with_capacity(initialized as usize);
        let count = initialized.min(u64::from(u32::MAX)) as u32;
        for index in 0..count {
            entries.push(mapping.entry(old_offset, index)?);
        }

        let new_layout =
            Layout::from_size_align(new_bytes, 64).map_err(|_| StatsError::InvalidLayout)?;
        let new_allocation = self.segment.allocate(new_layout)?;
        mapping.write_directory_entries(Offset::new(new_allocation.offset()), &entries)?;

        // Reconstruct the old block's ownership before publication so the
        // fallible work is complete; the drop itself is infallible.
        let old_layout =
            Layout::from_size_align(old_bytes, 64).map_err(|_| StatsError::InvalidLayout)?;
        let old_allocation = unsafe {
            SegmentAllocation::from_raw_offset(self.segment.clone(), old_offset.get(), old_layout)?
        };
        let new_offset = Offset::new(new_allocation.into_raw_offset());

        // Publication: VPP header sequence stores (stats.c:27,48-49) around
        // the directory switch — set `in_progress`, swap offset and
        // capacity, bump `epoch`, clear `in_progress` — then release the
        // old block.
        header.mark_in_progress();
        header.store_directory_offset(new_offset);
        header.store_directory_capacity(new_capacity);
        header.bump_epoch();
        header.clear_in_progress();
        drop(old_allocation);
        Ok(())
    }

    /// Cold structural pass: walks the removed list and releases every
    /// metric block whose live-handle count reached zero, moving those
    /// slots onto the free list and re-chaining the surviving removed
    /// entries. One publication wraps the whole pass.
    fn release_removed_entries(&mut self) -> Result<(), StatsError> {
        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();
        let capacity = header.directory_capacity();

        // Walk the removed chain; every read bounds-validates its slot.
        let mut head = header.removed_list_head();
        let mut chain: Vec<(u32, DirectorySlot)> = Vec::new();
        let mut visited: u64 = 0;
        while head != NULL_INDEX {
            visited += 1;
            if head >= capacity || visited > capacity {
                return Err(StatsError::InvalidState(0xFF));
            }
            let entry = mapping.entry(directory_offset, head as u32)?;
            if entry.state()? != EntryState::Removed {
                return Err(StatsError::InvalidState(entry.state_byte()));
            }
            chain.push((head as u32, entry));
            head = entry.link();
        }
        if chain.is_empty() {
            return Ok(());
        }

        // Partition on live-handle count and precompute every write target;
        // all checked work happens before the publication tail.
        let mut released: Vec<(u32, DirectorySlot)> = Vec::new();
        let mut release_now: Vec<bool> = Vec::with_capacity(chain.len());
        let mut targets: Vec<*mut DirectorySlot> = Vec::with_capacity(chain.len());
        for (index, entry) in &chain {
            let value = mapping.metric_value(entry.value_offset())?;
            let frees = value.refs() == 0;
            if frees {
                released.push((*index, *entry));
            }
            release_now.push(frees);
            targets.push(mapping.entry_write_target(directory_offset, *index)?);
        }
        if released.is_empty() {
            return Ok(());
        }

        // Publication: VPP header sequence stores (stats.c:27,48-49)
        // wrapping the chain rewrite — set `in_progress`, write each chain
        // slot exactly once (released slots join the free list, surviving
        // ones are re-chained), update both list heads, bump `epoch`, clear
        // `in_progress`. Writes go through the prepared targets; no
        // arithmetic and no fallible work.
        let free_head = header.free_list_head();
        header.mark_in_progress();
        let mut next_kept = NULL_INDEX;
        let mut next_free = free_head;
        for (((index, entry), target), frees) in
            chain.iter().zip(&targets).zip(release_now.iter()).rev()
        {
            let mut changed = *entry;
            if *frees {
                changed.set_state(EntryState::Free);
                changed.set_link(next_free);
                next_free = u64::from(*index);
            } else {
                changed.set_link(next_kept);
                next_kept = u64::from(*index);
            }
            // SAFETY: `target` was computed by `entry_write_target` for
            // this exact slot during this preparation phase.
            unsafe { mapping.write_entry(*target, changed) };
        }
        header.store_removed_list_head(next_kept);
        header.store_free_list_head(next_free);
        header.bump_epoch();
        header.clear_in_progress();

        // Release the reclaimed blocks after publication; an error here
        // surfaces corruption (the slots are already free, so the caller's
        // structural change still holds).
        for (_, entry) in released {
            self.release_metric_storage(&mapping, &entry)?;
        }
        Ok(())
    }

    /// Releases a metric block back to the segment arena, reconstructing
    /// its exact layout from the versioned descriptor header.
    fn release_metric_storage(
        &self,
        mapping: &Mapping,
        entry: &DirectorySlot,
    ) -> Result<(), StatsError> {
        let descriptor = mapping.descriptor(entry.descriptor_offset())?;
        if descriptor.version() != crate::descriptor::DESCRIPTOR_VERSION {
            return Err(StatsError::InvalidDescriptor(
                "corrupt metric block version".to_owned(),
            ));
        }
        let total_size = descriptor.total_size();
        if total_size < crate::descriptor::MIN_BLOCK_BYTES
            || total_size > crate::descriptor::MAX_BLOCK_BYTES as u64
            || total_size % 64 != 0
        {
            return Err(StatsError::InvalidDescriptor(
                "corrupt metric block size".to_owned(),
            ));
        }
        let layout = Layout::from_size_align(total_size as usize, 64)
            .map_err(|_| StatsError::InvalidLayout)?;
        let allocation = unsafe {
            SegmentAllocation::from_raw_offset(
                self.segment.clone(),
                entry.descriptor_offset().get(),
                layout,
            )?
        };
        drop(allocation);
        Ok(())
    }
}

/// Rounds `value` up to a multiple of `align` (a power of two), or `None`
/// if the rounding would overflow `usize`.
fn align_up(value: usize, align: usize) -> Option<usize> {
    value.checked_add(align - 1).map(|v| v & !(align - 1))
}

/// A per-instance unique shared-memory name, so concurrent `StatsMain`
/// instances never collide on the same OS object.
fn unique_segment_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let serial = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("hammer-stats-{}-{}", std::process::id(), serial)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_occupies_the_reserved_first_page() {
        let page = hammer_infra::page_size().expect("page size must be queryable");
        let stats = StatsMain::with_capacity(2 * page).expect("two pages construct");
        let mapping = Mapping::new(&stats.segment);
        let header = mapping.header();
        assert_eq!(header.magic(), crate::header::STATS_MAGIC);
        assert_eq!(header.version(), crate::header::STATS_VERSION);
        assert_eq!(header.capacity(), (2 * page) as u64);
        assert_eq!(header.epoch(), 0);
        assert_eq!(header.in_progress(), 0);
        assert_eq!(header.directory_capacity(), INITIAL_DIRECTORY_SLOTS);
        assert_eq!(header.initialized_len(), 0);
        assert_eq!(header.free_list_head(), NULL_INDEX);
        assert_eq!(header.removed_list_head(), NULL_INDEX);
        let directory_offset = header.directory_offset();
        assert_eq!(
            directory_offset.get() % 64,
            0,
            "directory must be 64-byte aligned"
        );
        assert!(
            directory_offset.get() >= page as u64,
            "directory must start after the reserved first page"
        );
    }

    /// Internal-corruption probe: a slot whose raw type bytes are each
    /// valid but combine incompatibly (a Prometheus counter on a gauge
    /// directory entry) must surface as a typed error, not a misread.
    #[test]
    fn dump_rejects_incompatible_raw_type_combination() {
        let mut stats = StatsMain::new().expect("default construction");
        let (id, _) = stats
            .add_counter("/x", prometheus::Opts::new("x", "x"))
            .expect("counter");

        let mapping = Mapping::new(&stats.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();
        let slot = mapping
            .entry(directory_offset, id.index)
            .expect("live slot read");
        // Test-only corruption of a slot no other reader observes: a gauge
        // directory entry carrying a Prometheus counter kind.
        let corrupted = DirectorySlot::new_active(
            encode_name("/x").expect("name"),
            slot.generation(),
            DirectoryType::Gauge,
            PrometheusType::Counter,
            slot.descriptor_offset(),
            slot.value_offset(),
        );
        let target = mapping
            .entry_write_target(directory_offset, id.index)
            .expect("write target");
        // SAFETY: single-threaded test; the corruption is the point.
        unsafe { mapping.write_entry(target, corrupted) };

        let err = stats
            .dump(&[id])
            .err()
            .expect("incompatible types rejected");
        assert!(
            matches!(
                err,
                StatsError::IncompatibleType {
                    id: got,
                    prometheus_type: PrometheusType::Counter,
                    directory_type: DirectoryType::Gauge,
                } if got == id
            ),
            "unexpected error: {err}"
        );
    }

    /// The mapping boundary rejects null or non-64-byte-aligned directory
    /// offsets before any pointer arithmetic, mirroring the same check on
    /// descriptor and value offsets. Without it, `entry`'s slot read and
    /// `entry_write_target`'s slot write could address an unaligned
    /// `DirectorySlot`.
    #[test]
    fn entry_and_write_target_reject_misaligned_directory_offsets() {
        let stats = StatsMain::new().expect("default construction");
        let mapping = Mapping::new(&stats.segment);
        for bad in [Offset::new(0), Offset::new(1), Offset::new(64 + 8)] {
            assert!(matches!(mapping.entry(bad, 0), Err(StatsError::Misaligned)));
            assert!(matches!(
                mapping.entry_write_target(bad, 0),
                Err(StatsError::Misaligned)
            ));
        }
        // A valid offset still resolves: the directory is 64-byte aligned.
        let directory_offset = mapping.header().directory_offset();
        assert!(mapping.entry(directory_offset, 0).is_ok());
        assert!(mapping.entry_write_target(directory_offset, 0).is_ok());
    }

    /// Active-generation invariant: for every active entry, the slot, the
    /// `EntryId`, the handle expectation, and the value record all carry
    /// exactly one generation; removal advances slot and record together by
    /// exactly one, and free-list reuse publishes that advanced generation
    /// without a second increment.
    #[test]
    fn active_entry_generations_are_equal() {
        let mut stats = StatsMain::new().expect("default construction");
        let (id0, counter) = stats
            .add_counter("/a", prometheus::Opts::new("a", "a"))
            .expect("counter");
        let mapping = Mapping::new(&stats.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();

        let slot = mapping
            .entry(directory_offset, id0.index())
            .expect("live slot");
        let record = mapping
            .metric_value(slot.value_offset())
            .expect("live record");
        assert_eq!(slot.generation(), id0.generation);
        assert_eq!(record.generation(), id0.generation);
        assert_eq!(counter.get().expect("live value"), 0);

        // Remove while the handle is live: the slot joins the removed list
        // and the record keeps its storage, so both stay safe to read. Both
        // advanced by exactly one.
        stats.remove_entry(id0).expect("remove");
        let removed = mapping
            .entry(directory_offset, id0.index)
            .expect("removed slot");
        assert_eq!(removed.generation(), id0.generation + 1);
        let removed_record = mapping
            .metric_value(removed.value_offset())
            .expect("removed record");
        assert_eq!(removed_record.generation(), id0.generation + 1);
        assert!(
            counter.get().is_err(),
            "the advanced record must stale direct handles"
        );

        // Reuse publishes the slot's advanced generation as-is: no second
        // increment, and the fresh value record matches it.
        drop(counter);
        let (id1, counter1) = stats
            .add_counter("/a", prometheus::Opts::new("a", "a"))
            .expect("re-add");
        assert_eq!(id1.index, id0.index);
        assert_eq!(id1.generation, id0.generation + 1);
        let slot1 = mapping
            .entry(directory_offset, id1.index)
            .expect("reused slot");
        let record1 = mapping
            .metric_value(slot1.value_offset())
            .expect("reused record");
        assert_eq!(slot1.generation(), id1.generation);
        assert_eq!(record1.generation(), id1.generation);
        counter1.increment().expect("increment reused handle");
    }
}
