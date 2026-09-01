//! VPP-style main/control-thread barrier.
//!
//! Main/control code may synchronize and release; workers only acknowledge
//! from their runtime loop. A missed deadline is a process-fatal worker
//! deadlock, not a recoverable runtime error.

use core::hint::spin_loop;
use hammer_infra::align::CacheLineAlignMark;
use std::panic::Location;
use std::sync::atomic::{AtomicU32, Ordering};
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
#[derive(Debug)]
struct State {
    // VPP allocates these counters on separate cache lines. The aligned
    // wrappers preserve that layout while keeping the state in one Arc.
    wait: BarrierCounter,
    workers: BarrierCounter,
}

/// VPP-style synchronization shared by the main thread and Data Workers.
#[derive(Clone, Debug)]
pub struct WorkerBarrier {
    state: Arc<State>,
    worker_count: u32,
    main_thread: ThreadId,
}

static PROCESS_BARRIER: OnceLock<WorkerBarrier> = OnceLock::new();

pub(crate) fn install(worker_count: u32) -> WorkerBarrier {
    let barrier = WorkerBarrier::new(worker_count);
    PROCESS_BARRIER
        .set(barrier.clone())
        .expect("worker barrier installed more than once");
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
    pub(crate) fn new(worker_count: u32) -> Self {
        Self {
            state: Arc::new(State {
                wait: BarrierCounter::new(0),
                workers: BarrierCounter::new(0),
            }),
            worker_count,
            main_thread: std::thread::current().id(),
        }
    }

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
        assert_eq!(
            std::thread::current().id(),
            self.main_thread,
            "worker barrier sync requires the installed main thread"
        );
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
        let outermost = self.recursion_level() == 1;
        if outermost {
            if let Some(worker_count) = PROCESS_BARRIER.get().map(WorkerBarrier::worker_count) {
                let _ = crate::global_main::GlobalMain::with_current(|engine| {
                    if worker_count != 0 {
                        engine.publish_worker_graph_refork(worker_count);
                    }
                });
            }
        }
        self.release_from(caller);
        if outermost {
            let _ = crate::global_main::GlobalMain::with_current(|engine| {
                engine.wait_for_worker_graph_refork();
            });
        }
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

    #[test]
    fn barrier_scope_matches_sync_check_release() {
        let barrier = WorkerBarrier::new(1);
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
