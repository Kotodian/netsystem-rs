use rand::{SeedableRng, rngs::SmallRng};
use std::cell::Cell;
use std::fmt;
use std::time::{Duration, Instant};

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
use hammer_infra::timer_wheel::{TimerHandle, TimerStartError, TimerWheel1t3w1024slOv};
use hammer_stats::DirectoryIndex;

use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HANDOFF_SLOT_CAPACITY, HandoffSlot};
use crate::node::{
    NodeEntry, NodeErrorCode, NodeErrorDescriptor, NodeFunctionRegistration, NodeMain, NodeRuntime,
};
use crate::runtime_simd::{native_simd_bytes, preferred_frame_batch_width};
use crate::trace::TraceMain;

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
    pub(crate) trace_main: TraceMain,
    pub(crate) handoff_trace_node: NodeId,
    pub(crate) main_loop_start_ticks: u64,
    pub(crate) seconds_per_cpu_tick: f64,
    pub(crate) cpu_reference_ticks: u64,
    pub(crate) unix_reference_seconds: f64,
    simd_bytes: usize,
    cpu_pinned: bool,
    pub(crate) enqueue_next: crate::graph::fanout::EnqueueNextFn,
    file_main: FileMode,
    /// VPP `vlib_main_t::timing_wheel`, owned by this dispatch thread.
    timing_wheel: TimerWheel1t3w1024slOv<NodeId>,
    timing_wheel_last_advance: Instant,
    expired_timers: Vec<NodeId>,
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
    /// Largest internal pending frame dispatched in this worker main-loop
    /// iteration. VPP: vlib/main.h:426-439, vlib/main.c:1064-1066.
    pub(crate) max_internal_frame_vectors: usize,
    pub(crate) queue_signal_callback: Option<fn(&mut DataPlaneMain) -> RuntimeResult<()>>,
    pub(crate) worker_init_functions_called: Bitmap,
}

const _: () = {
    assert!(core::mem::align_of::<DataPlaneMain>() == CACHE_LINE);
    assert!(core::mem::offset_of!(DataPlaneMain, cacheline0) == 0);
    assert!(core::mem::offset_of!(DataPlaneMain, random) == 0);
};

impl DataPlaneMain {
    pub(crate) const TIMER_TICK: Duration = Duration::from_micros(10);

    /// VPP `vlib_tw_timer_start` for a scheduled Node. Timer payloads are
    /// graph identities, never File or transport-specific state.
    pub fn start_node_timer(
        &mut self,
        node: NodeId,
        ticks: u64,
    ) -> Result<TimerHandle, TimerStartError> {
        self.nodes
            .node_kind(node)
            .expect("scheduled timer names a registered Node");
        self.advance_node_timers();
        self.timing_wheel.start(node, ticks)
    }

    #[inline]
    pub fn stop_node_timer(&mut self, handle: TimerHandle) -> bool {
        self.timing_wheel.stop(handle)
    }

    /// VPP `vlib_tw_timer_first_expires_in_ticks`: File poll reads this from
    /// its DataPlane Main before choosing a sleep timeout.
    pub(crate) fn timer_first_expires_in_ticks(&self) -> Option<u32> {
        if self.timing_wheel.is_empty() {
            return None;
        }
        let ticks = self
            .timing_wheel
            .first_expires_in_ticks()
            .expect("the DataPlane timing wheel has a fast-slot bitmap");
        let elapsed_ticks = self.timing_wheel_last_advance.elapsed().as_micros()
            / Self::TIMER_TICK.as_micros();
        Some(ticks.saturating_sub(elapsed_ticks.min(u128::from(u32::MAX)) as u32))
    }

    /// VPP `vlib_tw_timer_expire_timers` and `process_expired_timers`'s
    /// scheduled-Node arm. Only this thread advances its wheel.
    pub(crate) fn advance_node_timers(&mut self) {
        let now = Instant::now();
        if self.timing_wheel.is_empty() {
            self.timing_wheel_last_advance = now;
            return;
        }
        let ticks = (now - self.timing_wheel_last_advance).as_micros()
            / Self::TIMER_TICK.as_micros();
        let ticks = ticks.min(u128::from(u32::MAX)) as u32;
        if ticks == 0 {
            return;
        }
        self.timing_wheel.expire(ticks, &mut self.expired_timers);
        self.timing_wheel_last_advance += Self::TIMER_TICK * ticks;
        for node in self.expired_timers.drain(..) {
            self.nodes
                .schedule_node(node)
                .expect("timer expiration names a registered Node");
        }
        if self.timing_wheel.is_empty() {
            self.timing_wheel_last_advance = now;
        }
    }

    /// Installs the main-loop queue check before Process Nodes start.
    /// VPP: `vlib_set_queue_signal_callback`, vlib/main.h:462-466.
    pub fn set_queue_signal_callback(
        &mut self,
        callback: fn(&mut DataPlaneMain) -> RuntimeResult<()>,
    ) {
        assert_eq!(self.thread_index, 0, "queue signals belong to main thread");
        assert!(
            self.queue_signal_callback.replace(callback).is_none(),
            "main loop has one queue signal callback"
        );
    }

    /// VPP: `vlib_last_vectors_per_main_loop`, vlib/main.h:435-439.
    #[inline(always)]
    pub fn max_internal_frame_vectors(&self) -> usize {
        self.max_internal_frame_vectors
    }

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
            .field("trace_main", &self.trace_main)
            .field("simd_bytes", &self.simd_bytes)
            .field("loops_per_second", &self.loops_per_second)
            .field("main_loop_exit_now", &self.main_loop_exit_now)
            .field("main_loop_exit_status", &self.main_loop_exit_status)
            .finish()
    }
}
