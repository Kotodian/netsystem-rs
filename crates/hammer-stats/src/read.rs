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
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum DumpValue {
    /// Integer value.
    Counter(u64),
    /// Floating-point value.
    Gauge(f64),
}
