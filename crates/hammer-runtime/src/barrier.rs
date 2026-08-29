//! VPP-style main/control-thread barrier.
//!
//! Main/control code may synchronize and release; workers only acknowledge
//! from their runtime loop. A missed deadline is a process-fatal worker
//! deadlock, not a recoverable runtime error.

use core::hint::spin_loop;
use hammer_infra::align::CacheLineAlignMark;
use std::panic::Location;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

#[cfg(debug_assertions)]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_millis(600_100);
#[cfg(not(debug_assertions))]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_secs(1);

#[repr(C)]
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
}

/// VPP-style synchronization shared by the main thread and Data Workers.
#[derive(Clone)]
pub struct WorkerBarrier {
    state: Arc<State>,
    worker_count: u32,
}

impl WorkerBarrier {
    #[inline]
    pub(crate) fn new(worker_count: u32) -> Self {
        Self {
            state: Arc::new(State {
                wait: BarrierCounter::new(0),
                workers: BarrierCounter::new(0),
            }),
            worker_count,
        }
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
        self.state.wait.load(Ordering::Acquire)
    }

    /// Acknowledges an armed barrier and waits for release.
    pub(crate) fn check(&self) {
        if !self.is_pending() {
            return;
        }
        self.state.workers.fetch_add(1, Ordering::Release);
        while self.state.wait.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
        self.state.workers.fetch_sub(1, Ordering::Release);
    }

    #[track_caller]
    pub(crate) fn arm(&self) {
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
    pub(crate) fn release(&self) {
        self.release_from(Location::caller());
    }

    fn pause(&self, caller: &'static Location<'static>) {
        let recursion_level = self.state.wait.fetch_add(1, Ordering::Release);
        if recursion_level == 0 {
            wait_for_worker_count(
                &self.state.workers,
                self.worker_count,
                Instant::now() + BARRIER_SYNC_TIMEOUT,
                "barrier sync",
                caller,
            );
        }
    }

    fn release_from(&self, caller: &'static Location<'static>) {
        let recursion_level = self.state.wait.fetch_sub(1, Ordering::Release);
        if recursion_level == 0 {
            barrier_deadlock_at("barrier release without matching sync", 1, 0, caller);
        }
        if recursion_level > 1 {
            return;
        }
        wait_for_worker_count(
            &self.state.workers,
            0,
            Instant::now() + BARRIER_SYNC_TIMEOUT,
            "barrier release",
            caller,
        );
    }
}

struct Release {
    barrier: WorkerBarrier,
    caller: &'static Location<'static>,
}

impl Drop for Release {
    fn drop(&mut self) {
        self.barrier.release_from(self.caller);
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
