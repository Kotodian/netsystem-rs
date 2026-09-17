//! `/buffer-pools` owner glue: the parameterized declaration, the collector and
//! the registration item for the buffer pools this runtime established.
//!
//! VPP registers these in `vlib_buffer_main_init` right after the pools exist
//! (`third_party/vpp/src/vlib/buffer.c:937-956`); Hammer runs the registration
//! item inside `run_stats_registrations()`, which is after `new_main` created
//! the pools and before any worker or stats round exists.

use hammer_core::buffer::BufferMain;
use hammer_stats::buffer_pools::{BufferPoolGauge, update_pool_gauge};
use hammer_stats::{Collector, DirectoryEntry, DirectoryIndex, StatsMain};

use crate::error::RuntimeResult;

/// The three gauges of one buffer pool, named after the pool exactly like VPP's
/// `"/buffer-pools/%v/{cached,used,available}"` registration loop
/// (`buffer.c:944,948,953`).
///
/// This is a parameterized declaration: `{pool_name}` is an `install` argument
/// filled by the pool's own name (VPP's `bp->name`, `buffer.c:529`). The macro
/// does not know `/buffer-pools`; it only binds the template and the fields.
#[derive(hammer_component_macros::Stats)]
pub(crate) struct BufferPoolGauges {
    #[stats(path = "/buffer-pools/{pool_name}/cached")]
    cached: hammer_stats::Gauge,
    #[stats(path = "/buffer-pools/{pool_name}/used")]
    used: hammer_stats::Gauge,
    #[stats(path = "/buffer-pools/{pool_name}/available")]
    available: hammer_stats::Gauge,
}

/// Publishes one pool gauge per round: VPP's `vlib_stats_collector_t` row for
/// one `buffer_gauges_collect_*_fn` (`buffer.c:838-872,937-956`).
///
/// One type serves all three gauges: `gauge` is the only difference between
/// VPP's three functions, `pool_index` is VPP's `private_data` and
/// `entry_index` is the entry this instance owns.
struct BufferPoolGaugeCollector {
    entry_index: DirectoryIndex,
    pool_index: u8,
    gauge: BufferPoolGauge,
}

impl Collector for BufferPoolGaugeCollector {
    fn entry_index(&self) -> DirectoryIndex {
        self.entry_index
    }

    fn collect(&self, entry: &DirectoryEntry) {
        // VPP: `bp = buffer_get_by_index (vm->buffer_main, d->private_data)`
        // then `d->entry->value = …` (`buffer.c:838-872`). The pool set is fixed
        // when it is created, so a missing pool is a bug, not a state.
        let usage = BufferMain::global().pool_usage(self.pool_index);
        update_pool_gauge(entry, self.gauge, usage);
    }
}

/// Registers one entry and three collectors per pool, like VPP's
/// `vec_foreach (bp, bm->buffer_pools)` loop (`buffer.c:937-956`).
#[hammer_component_macros::stats_collect_registration]
fn register_buffer_pools(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    let buffer_main = BufferMain::global();
    for pool_index in 0..buffer_main.pool_count() {
        let pool_index = u8::try_from(pool_index).expect("a buffer pool index fits u8");
        let gauges =
            BufferPoolGauges::install(&stats_main.segment, buffer_main.pool_name(pool_index))?;
        // VPP registration order: cached, used, available (`buffer.c:943-955`).
        for (gauge, entry_index) in [
            (BufferPoolGauge::Cached, gauges.cached.index),
            (BufferPoolGauge::Used, gauges.used.index),
            (BufferPoolGauge::Available, gauges.available.index),
        ] {
            stats_main.register_collector(BufferPoolGaugeCollector {
                entry_index,
                pool_index,
                gauge,
            });
        }
    }
    Ok(())
}
