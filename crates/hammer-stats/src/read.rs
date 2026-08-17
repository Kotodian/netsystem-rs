//! Public owned copies produced by [`crate::StatsMain::list`] and
//! [`crate::StatsMain::dump`].
//!
//! These types are decoded from the shared segment inside the reader calls
//! and never borrow from it: no value escapes a read as a reference into
//! the mapping.

use crate::directory::{DirectoryType, PrometheusType};
use crate::stats::EntryId;

/// One const label of a metric's Prometheus descriptor.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConstLabel {
    /// Label name, as in `prometheus::Opts::const_label`.
    pub name: String,
    /// Label value.
    pub value: String,
}

/// One active directory entry, fully decoded into owned strings.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DirectoryEntry {
    /// The entry's identity, also returned by the add call.
    pub id: EntryId,
    /// The directory path, e.g. `/if/rx`.
    pub path: String,
    /// The directory type (`stat_directory_type_t`).
    pub directory_type: DirectoryType,
    /// The Prometheus metric kind.
    pub prometheus_type: PrometheusType,
    /// The metric's fully qualified name from its descriptor.
    pub fq_name: String,
    /// The metric's help text from its descriptor.
    pub help: String,
    /// The descriptor's const labels, in registration order.
    pub const_labels: Vec<ConstLabel>,
}

/// One dumped value, captured point-in-time.
#[derive(Clone, PartialEq, Debug)]
pub struct DumpEntry {
    /// The requested entry's identity.
    pub id: EntryId,
    /// The directory path.
    pub path: String,
    /// The directory type (`stat_directory_type_t`).
    pub directory_type: DirectoryType,
    /// The Prometheus metric kind.
    pub prometheus_type: PrometheusType,
    /// The value at the time of the dump.
    pub value: DumpValue,
}

/// A dumped value, mirroring VPP's scalar `copy_data` result for
/// `STAT_DIR_TYPE_SCALAR_INDEX` and `STAT_DIR_TYPE_GAUGE`
/// (stat_client.c:230-235).
#[derive(Clone, PartialEq, Debug)]
pub enum DumpValue {
    /// Integer value.
    Counter(u64),
    /// Floating-point value.
    Gauge(f64),
    /// Row-major simple counter-vector values.
    CounterVectorSimple(Vec<Vec<u64>>),
    /// Row-major packet/byte counter-vector values.
    CounterVectorCombined(Vec<Vec<(u64, u64)>>),
    /// Fixed-slot names, with `None` for an unset slot.
    NameVector(Vec<Option<String>>),
    /// Row-major histogram bins.
    HistogramLog2(Vec<Vec<u64>>),
    /// One owned physical-ring snapshot per row.
    RingBuffer(Vec<RingBufferSnapshot>),
}

/// One owned row snapshot from a fixed ring buffer.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RingBufferSnapshot {
    /// Release-published producer sequence observed for this row.
    pub sequence: u64,
    /// Physical slots in ring order, oldest-to-newest by slot index.
    pub entries: Vec<Vec<u8>>,
}
