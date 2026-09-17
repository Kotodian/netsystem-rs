//! The `/buffer-pools` family contract: the three gauges of one buffer pool.
//!
//! This module is the Hammer counterpart of VPP's `buffer_gauges_collect_*_fn`
//! (`third_party/vpp/src/vlib/buffer.c:838-872`): the identity of the three
//! published facts and the only place that turns one pool reading into a gauge
//! value. Each pool owner declares its own entries through `#[derive(Stats)]`
//! and registers its own collector; that collector hands its reading to
//! [`update_pool_gauge`].

use hammer_core::buffer::BufferPoolUsage;

use crate::protocol::DirectoryEntry;

/// Which of the three pool facts one collector publishes.
///
/// VPP encodes the same distinction in three separate collection functions
/// (`buffer_gauges_collect_{cached,used,available}_fn`, `buffer.c:838-872`);
/// Hammer carries it as one field so a single collector type serves all three,
/// like VPP's one `vlib_stats_collector_reg_t` shape with a chosen `collect_fn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferPoolGauge {
    /// VPP `buffer_gauges_collect_cached_fn`: `Σ bpt->n_cached`.
    Cached,
    /// VPP `buffer_gauges_collect_used_fn`: `n_buffers - n_avail - Σ n_cached`.
    Used,
    /// VPP `buffer_gauges_collect_available_fn`: `n_avail`.
    Available,
}

/// Writes one pool reading into the gauge's own entry, like VPP's
/// `d->entry->value = …` (`buffer.c:846,858,870`).
///
/// The value is one reading of the pool, not the pool itself: this module does
/// not know pool names, how many pools exist, or who collects them. The entry
/// belongs to the calling collector, so the write path takes no segment lock
/// and has no error to return.
pub fn update_pool_gauge(entry: &DirectoryEntry, gauge: BufferPoolGauge, usage: BufferPoolUsage) {
    entry.set_scalar(match gauge {
        BufferPoolGauge::Cached => usage.cached,
        BufferPoolGauge::Used => usage.used(),
        BufferPoolGauge::Available => usage.available,
    });
}
