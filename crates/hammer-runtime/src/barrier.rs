//! VPP-style main/control-thread barrier.
//!
//! Main/control code may synchronize and release; workers only acknowledge
//! from their runtime loop. A missed deadline is a process-fatal worker
//! deadlock, not a recoverable runtime error.

use core::hint::spin_loop;
use hammer_infra::align::CacheLineAlignMark;
use std::cell::UnsafeCell;
use std::fmt;
use std::panic::Location;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

#[cfg(debug_assertions)]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_millis(600_100);
#[cfg(not(debug_assertions))]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_secs(1);

#[repr(C)]
#[derive(Debug)]
struct BarrierCounter {
    cacheline0: CacheLineAlignMark,
    value: AtomicU32,
}

impl BarrierCounter {
    #[inline]
    const fn new(value: u32) -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            value: AtomicU32::new(value),
        }
    }

    #[inline]
    fn fetch_add(&self, value: u32, ordering: Ordering) -> u32 {
        self.value.fetch_add(value, ordering)
    }

    #[inline]
    fn fetch_sub(&self, value: u32, ordering: Ordering) -> u32 {
        self.value.fetch_sub(value, ordering)
    }

    #[inline]
    fn load(&self, ordering: Ordering) -> u32 {
        self.value.load(ordering)
    }
}

#[repr(C)]
struct State {
    // VPP allocates these counters on separate cache lines. The aligned
    // wrappers preserve that layout while keeping the state in one Arc.
    wait: BarrierCounter,
    workers: BarrierCounter,
    reforks: BarrierCounter,
    recursion: AtomicU32,
    startup_cancelled: AtomicBool,
    node_runtime: UnsafeCell<Option<crate::node::NodeRuntimeInner>>,
}

// SAFETY: `node_runtime` has one main-thread writer. It is replaced only while
// every barrier participant is stopped, published by the release store to
// `wait`, read by Data Workers after the matching acquire load, and cleared
// only after `reforks` reaches zero. No access overlaps a write or drop.
unsafe impl Sync for State {}

/// VPP-style synchronization shared by the main thread and Data Workers.
#[derive(Clone)]
pub struct WorkerBarrier {
    state: Arc<State>,
    worker_count: u32,
    participant_count: u32,
    main_thread: ThreadId,
}

static PROCESS_BARRIER: OnceLock<WorkerBarrier> = OnceLock::new();

pub(crate) fn install(worker_count: u32, participant_count: u32) -> WorkerBarrier {
    let barrier = WorkerBarrier::new(worker_count, participant_count);
    assert!(
        PROCESS_BARRIER.set(barrier.clone()).is_ok(),
        "worker barrier installed more than once"
    );
    barrier
}

#[doc(hidden)]
pub fn global() -> Option<WorkerBarrier> {
    PROCESS_BARRIER.get().cloned()
}

#[doc(hidden)]
pub fn __sync_guard(main: &mut crate::DataPlaneMain) -> impl Drop + use<> {
    assert_eq!(
        main.thread_index(),
        0,
        "worker barrier sync requires main thread"
    );
    let barrier = PROCESS_BARRIER
        .get()
        .expect("worker barrier is not installed");
    barrier.pause(Location::caller());
    ScopeGuard {
        barrier: barrier.clone(),
        caller: Location::caller(),
        _not_send: std::marker::PhantomData,
    }
}

#[doc(hidden)]
pub fn __assert_held() {
    let Some(barrier) = PROCESS_BARRIER.get() else {
        return;
    };
    assert!(barrier.is_pending(), "worker barrier scope is required");
}

#[doc(hidden)]
pub fn __is_pending() -> bool {
    PROCESS_BARRIER.get().is_some_and(WorkerBarrier::is_pending)
}

impl WorkerBarrier {
    #[inline]
    pub(crate) fn new(worker_count: u32, participant_count: u32) -> Self {
        assert!(worker_count <= participant_count);
        Self {
            state: Arc::new(State {
                wait: BarrierCounter::new(0),
                workers: BarrierCounter::new(0),
                reforks: BarrierCounter::new(0),
                recursion: AtomicU32::new(0),
                startup_cancelled: AtomicBool::new(false),
                node_runtime: UnsafeCell::new(None),
            }),
            worker_count,
            participant_count,
            main_thread: std::thread::current().id(),
        }
    }

    /// Pauses every worker permanently while process-exit work runs.
    ///
    /// This matches VPP's final main-thread barrier: callers must invoke it
    /// only when the process will terminate after `operation` returns.
    #[track_caller]
    pub(crate) fn final_sync<R>(&self, operation: impl FnOnce() -> R) -> R {
        self.pause(Location::caller());
        operation()
    }

    /// Pauses every worker while `operation` runs on the main/control thread.
    #[track_caller]
    pub fn sync<R>(&self, operation: impl FnOnce() -> R) -> R {
        let caller = Location::caller();
        self.pause(caller);
        let release = Release {
            barrier: self.clone(),
            caller,
        };
        let result = operation();
        drop(release);
        result
    }

    /// Number of Data Workers this barrier coordinates.
    #[inline]
    pub const fn worker_count(&self) -> u32 {
        self.worker_count
    }

    #[inline]
    pub(crate) const fn participant_count(&self) -> u32 {
        self.participant_count
    }

    #[inline]
    /// Returns true while a barrier sync or startup arm is active.
    ///
    /// Control code uses this to prove that workers are being held before
    /// mutating worker-visible state. The main-thread caller must also verify
    /// that it is running on the main/control engine; `is_pending` alone does
    /// not identify the calling thread.
    pub fn is_pending(&self) -> bool {
        self.state.wait.load(Ordering::Acquire) != 0
    }

    /// Number of nested syncs currently held on the main thread (VPP
    /// `vlib_worker_thread_barrier_sync` recursion count). Workers are parked
    /// while non-zero, so nested code must not wait for worker progress.
    #[inline]
    pub(crate) fn recursion_level(&self) -> u32 {
        self.state.recursion.load(Ordering::Acquire)
    }

    /// Acknowledges an armed barrier and waits for release.
    ///
    /// Registered no-data-structure-clone thread functions call this from
    /// their loop, matching VPP's `vlib_worker_thread_barrier_check` contract.
    pub fn check(&self) {
        self.check_for_refork();
    }

    pub(crate) fn check_for_refork(&self) -> bool {
        if !self.is_pending() {
            return false;
        }
        self.state.workers.fetch_add(1, Ordering::Release);
        while self.state.wait.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
        self.state.workers.fetch_sub(1, Ordering::Release);
        self.state.reforks.load(Ordering::Acquire) != 0
    }

    pub(crate) fn request_node_refork(&self, graph: crate::node::NodeRuntimeInner) {
        assert_eq!(
            std::thread::current().id(),
            self.main_thread,
            "graph refork publication requires the installed main thread"
        );
        assert!(
            self.is_pending(),
            "graph refork requires a held worker barrier"
        );
        assert_eq!(
            self.state.reforks.load(Ordering::Acquire),
            0,
            "previous graph refork must complete before another publication"
        );
        // SAFETY: all participants acknowledged the held barrier before the
        // topology owner could reach this method. A nested mutation may replace
        // the unpublished refork input; workers cannot read it until release.
        unsafe {
            *self.state.node_runtime.get() = Some(graph);
        }
    }

    pub(crate) fn refork(&self, nodes: &mut crate::NodeMain) {
        assert_ne!(
            self.state.reforks.load(Ordering::Acquire),
            0,
            "worker refork requires a published graph"
        );
        // SAFETY: the acquire observation of `reforks` follows the release that
        // publishes `node_runtime`. Main retains it until every Data Worker has
        // cloned and installed its own graph.
        let graph = unsafe {
            (&*self.state.node_runtime.get())
                .as_ref()
                .expect("refork counter and graph publication stay paired")
                .clone()
        };
        nodes.refork(graph);
        let previous = self.state.reforks.fetch_sub(1, Ordering::Release);
        assert_ne!(previous, 0, "graph refork completion underflow");
        while self.state.reforks.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
    }

    #[inline]
    pub(crate) fn cancel_startup(&self) {
        self.state.startup_cancelled.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn startup_cancelled(&self) -> bool {
        self.state.startup_cancelled.load(Ordering::Acquire)
    }

    #[track_caller]
    pub(crate) fn arm(&self) {
        assert_eq!(
            self.state.recursion.load(Ordering::Acquire),
            0,
            "startup barrier is armed outside a barrier scope"
        );
        let previous = self.state.wait.fetch_add(1, Ordering::Release);
        if previous != 0 {
            barrier_deadlock_at("arm startup barrier", 0, previous, Location::caller());
        }
    }

    #[inline]
    pub(crate) fn paused_workers(&self) -> u32 {
        self.state.workers.load(Ordering::Acquire)
    }

    #[track_caller]
    pub(crate) fn release_startup(&self) {
        assert_eq!(
            self.state.recursion.load(Ordering::Acquire),
            0,
            "cancelled startup has no active barrier scope"
        );
        let previous = self.state.wait.fetch_sub(1, Ordering::Release);
        assert_eq!(previous, 1, "startup barrier releases once");
        wait_for_worker_count(
            &self.state.workers,
            0,
            Instant::now() + BARRIER_SYNC_TIMEOUT,
            "startup barrier release",
            Location::caller(),
        );
    }

    fn pause(&self, caller: &'static Location<'static>) {
        assert_eq!(
            std::thread::current().id(),
            self.main_thread,
            "worker barrier sync requires the installed main thread"
        );
        let recursion_level = self.state.recursion.fetch_add(1, Ordering::Relaxed);
        if recursion_level == 0 {
            if self.state.wait.load(Ordering::Acquire) == 0 {
                let previous = self.state.wait.fetch_add(1, Ordering::Release);
                assert_eq!(previous, 0, "outer barrier sync closes once");
            }
            wait_for_worker_count(
                &self.state.workers,
                self.participant_count,
                Instant::now() + BARRIER_SYNC_TIMEOUT,
                "barrier sync",
                caller,
            );
        }
    }

    fn release_from(&self, caller: &'static Location<'static>) {
        let recursion_level = self.state.recursion.load(Ordering::Relaxed);
        if recursion_level == 0 {
            barrier_deadlock_at("barrier release without matching sync", 1, 0, caller);
        }
        let previous_recursion = self.state.recursion.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(previous_recursion, recursion_level);
        if previous_recursion > 1 {
            return;
        }
        // SAFETY: only the main thread writes this slot and workers remain
        // parked while `wait` is non-zero.
        let refork_requested = unsafe { (&*self.state.node_runtime.get()).is_some() };
        if refork_requested {
            let previous = self
                .state
                .reforks
                .fetch_add(self.worker_count, Ordering::Release);
            assert_eq!(previous, 0, "graph refork counter must start at zero");
        }
        let previous = self.state.wait.fetch_sub(1, Ordering::Release);
        assert_eq!(previous, 1, "outer worker barrier release is unique");
        wait_for_worker_count(
            &self.state.workers,
            0,
            Instant::now() + BARRIER_SYNC_TIMEOUT,
            "barrier release",
            caller,
        );
        if refork_requested {
            wait_for_worker_count(
                &self.state.reforks,
                0,
                Instant::now() + BARRIER_SYNC_TIMEOUT,
                "graph refork",
                caller,
            );
            // SAFETY: every worker completed its clone before the counter
            // reached zero, and the next publication cannot begin until this
            // outer release returns.
            unsafe {
                drop(self.state.node_runtime.get().replace(None));
            }
        }
    }
}

impl fmt::Debug for WorkerBarrier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerBarrier")
            .field("worker_count", &self.worker_count)
            .field("participant_count", &self.participant_count)
            .field("wait", &self.state.wait.load(Ordering::Relaxed))
            .field("workers", &self.state.workers.load(Ordering::Relaxed))
            .field("reforks", &self.state.reforks.load(Ordering::Relaxed))
            .field("recursion", &self.state.recursion.load(Ordering::Relaxed))
            .finish()
    }
}

struct Release {
    barrier: WorkerBarrier,
    caller: &'static Location<'static>,
}

struct ScopeGuard {
    barrier: WorkerBarrier,
    caller: &'static Location<'static>,
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Drop for ScopeGuard {
    fn drop(&mut self) {
        self.barrier.release_scope(self.caller);
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        self.barrier.release_scope(self.caller);
    }
}

impl WorkerBarrier {
    fn release_scope(&self, caller: &'static Location<'static>) {
        self.release_from(caller);
    }
}

fn wait_for_worker_count(
    workers: &BarrierCounter,
    expected: u32,
    deadline: Instant,
    phase: &'static str,
    caller: &'static Location<'static>,
) {
    loop {
        let observed = workers.load(Ordering::Acquire);
        if observed == expected {
            return;
        }
        if Instant::now() > deadline {
            barrier_deadlock_at(phase, expected, observed, caller);
        }
        spin_loop();
    }
}

#[track_caller]
pub(crate) fn barrier_deadlock(phase: &'static str, expected: u32, observed: u32) -> ! {
    barrier_deadlock_at(phase, expected, observed, Location::caller())
}

#[cold]
#[inline(never)]
fn barrier_deadlock_at(
    phase: &'static str,
    expected: u32,
    observed: u32,
    caller: &'static Location<'static>,
) -> ! {
    eprintln!(
        "{phase}: worker thread deadlock; caller={caller}; workers_at_barrier={observed}; expected={expected}"
    );
    std::process::abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    // vlib/main.c::vlib_main keeps the final barrier held after exit hooks.
    // Zero Workers lets this unit test verify the permanent scope directly.
    #[test]
    fn final_barrier_remains_held_after_exit_hooks() {
        let barrier = WorkerBarrier::new(0, 0);
        let status = barrier.final_sync(|| {
            assert!(barrier.is_pending());
            assert_eq!(barrier.recursion_level(), 1);
            7
        });
        assert_eq!(status, 7);
        assert!(barrier.is_pending());
        assert_eq!(barrier.recursion_level(), 1);
    }

    #[test]
    fn barrier_scope_matches_sync_check_release() {
        let barrier = WorkerBarrier::new(1, 1);
        PROCESS_BARRIER
            .set(barrier.clone())
            .expect("test barrier installs once");
        let stop = Arc::new(AtomicBool::new(false));
        let worker_barrier = barrier.clone();
        let worker_stop = Arc::clone(&stop);
        let worker = std::thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                worker_barrier.check();
                std::thread::yield_now();
            }
        });

        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_infra::main_heap::init_default().unwrap();
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                3,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut main = crate::DataPlaneMain::new(crate::DataPlaneBufferConfig::default());
        crate::worker_thread_barrier_sync!(&mut main, {
            assert_eq!(barrier.recursion_level(), 1);
            crate::worker_thread_barrier_sync!(&mut main, {
                assert_eq!(barrier.recursion_level(), 2)
            });
            assert_eq!(barrier.recursion_level(), 1);
        });
        stop.store(true, Ordering::Release);
        worker.join().expect("worker exits after barrier scope");
    }
}
