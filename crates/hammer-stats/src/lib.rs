//! A shared-memory stats segment modeled on VPP's vlib stats segment.
//!
//! [`StatsMain`] owns a page-reserved shared segment whose first page holds
//! a versioned header (mirroring `vlib_stats_shared_header_t`), followed by
//! a directory of 256-byte entries (mirroring `vlib_stats_entry_t`) and
//! per-metric value blocks. Metrics are added as counters or gauges; each
//! carries a Prometheus descriptor and a cache-line-aligned value record
//! that `Counter`/`Gauge` handles update in place.

mod descriptor;
mod directory;
mod error;
mod header;
mod mapping;
mod metric_value;
mod offset;
mod read;
mod stats;

pub use crate::directory::{DirectoryType, PrometheusType};
pub use crate::error::StatsError;
pub use crate::read::{ConstLabel, DirectoryEntry, DumpEntry, DumpValue};
pub use crate::stats::{Counter, DEFAULT_CAPACITY, EntryId, Gauge, StatsMain, Timestamp};
