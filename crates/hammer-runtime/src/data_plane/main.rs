use rand::{SeedableRng, rngs::SmallRng};
use std::cell::Cell;
use std::fmt;
use std::time::Instant;

use crate::error::{RuntimeError, RuntimeResult};
use crate::file::{FILE_MAIN, FileMode};
use hammer_core::data_plane::{
    BUFFER_CACHE_LINE_SIZE, DEFAULT_BUFFER_FRAME_POOL_SIZE, Frame, FrameBatchWidth, NodeErrorIndex,
    NodeId, NodeKind, NodeRegistration,
};
use hammer_core::error::DataPlaneError;
use hammer_infra::PageSize;
use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};
use hammer_infra::bitmap::Bitmap;
use hammer_stats::DirectoryIndex;

use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HANDOFF_SLOT_CAPACITY, HandoffSlot};
use crate::node::{
    NodeEntry, NodeErrorCode, NodeErrorDescriptor, NodeFunctionRegistration, NodeMain, NodeRuntime,
};
use crate::runtime_simd::{native_simd_bytes, preferred_frame_batch_width};
use crate::trace::{DataPlaneTrace, PacketTrace, TraceControlHandle};

mod buffer_pool;
mod config;
mod dispatch;
mod frame_queue;
mod handoff;
mod trace;
mod worker;

pub use config::DataPlaneBufferConfig;

#[repr(C)]
pub struct DataPlaneMain {
    cacheline0: CacheLineAlignMark,
    random: SmallRng,
    thread_index: u32,
    pub(crate) nodes: NodeMain,
    current_node: Cell<Option<NodeId>>,
    /// The `/node/errors` directory entry this thread records into (VPP's
    /// `error_main.stats_err_entry_index`). The row is this thread's
    /// `thread_index`, so the entry is the only per-thread fact; `None` means
    /// this process published no error columns and the record path counts
    /// nothing. Installed once at the freeze point, like VPP's per-thread
    /// `error_main.counters` refresh (`third_party/vpp/src/vlib/threads.c:766-778`).
    pub(crate) node_error_stats_entry_index: Cell<Option<DirectoryIndex>>,
    handoff: Option<DataPlaneHandoffWorker>,
    active_numa_node: u32,
    trace: DataPlaneTrace,
    simd_bytes: usize,
    file_main: FileMode,
    /// Main loops completed in the current reporting interval
    /// (`loops_this_reporting_interval` in VPP's `vlib_main_t`).
    loops_this_reporting_interval: u64,
    /// Start of the current reporting interval; `None` until one window is
    /// complete, which VPP expresses with a zero sentinel.
    loop_interval_start: Option<Instant>,
    /// End of the current reporting interval (`loop_interval_end`).
    loop_interval_end: Instant,
    /// Damped loops per second of the latest window (`loops_per_second`).
    loops_per_second: f64,
    /// `exp(-1.0 / 20.0)`, computed once like VPP's `damping_constant`.
    damping_constant: f64,
    main_loop_exit_now: bool,
    main_loop_exit_status: i32,
    /// Start of the next Node dispatch's `clocks` measurement: VPP
    /// `dispatch_node`'s `last_time_stamp`, refreshed once per main-loop
    /// iteration and otherwise advanced by the dispatch point itself.
    pub(crate) last_time_stamp: u64,
    pub(crate) worker_init_functions_called: Bitmap,
}

const _: () = {
    assert!(core::mem::align_of::<DataPlaneMain>() == CACHE_LINE);
    assert!(core::mem::offset_of!(DataPlaneMain, cacheline0) == 0);
    assert!(core::mem::offset_of!(DataPlaneMain, random) == 0);
};

impl DataPlaneMain {
    /// Worker-local, non-cryptographic randomness seeded at worker construction.
    #[inline]
    pub fn random(&mut self) -> &mut SmallRng {
        &mut self.random
    }

    /// Installs this thread's `/node/errors` entry.
    ///
    /// Called once at the freeze point, before any Worker runs: thread zero's
    /// runtime and each Worker runtime get the entry the registration path
    /// published, or `None` when no node declared errors.
    pub(crate) fn install_node_error_stats_entry(&self, entry: Option<DirectoryIndex>) {
        self.node_error_stats_entry_index.set(entry);
    }
}

impl fmt::Debug for DataPlaneMain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneMain")
            .field("thread_index", &self.thread_index)
            .field("nodes", &self.nodes)
            .field("current_node", &self.current_node.get())
            .field("handoff", &self.handoff)
            .field("active_numa_node", &self.active_numa_node)
            .field("trace", &self.trace)
            .field("simd_bytes", &self.simd_bytes)
            .field("loops_per_second", &self.loops_per_second)
            .field("main_loop_exit_now", &self.main_loop_exit_now)
            .field("main_loop_exit_status", &self.main_loop_exit_status)
            .finish()
    }
}
