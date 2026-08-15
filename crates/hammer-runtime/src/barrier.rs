//! VPP-style main/control-thread barrier.
//!
//! Main/control code may synchronize and release; workers only acknowledge
//! from their runtime loop. A missed deadline is a process-fatal worker
//! deadlock, not a recoverable runtime error.

use core::hint::spin_loop;
use std::panic::Location;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

#[cfg(debug_assertions)]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_millis(600_100);
#[cfg(not(debug_assertions))]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_secs(1);

#[repr(align(64))]
struct BarrierCounter(AtomicU32);

impl BarrierCounter {
    #[inline]
    const fn new(value: u32) -> Self {
        Self(AtomicU32::new(value))
    }

    #[inline]
    fn fetch_add(&self, value: u32, ordering: Ordering) -> u32 {
        self.0.fetch_add(value, ordering)
    }

    #[inline]
    fn fetch_sub(&self, value: u32, ordering: Ordering) -> u32 {
        self.0.fetch_sub(value, ordering)
    }

    #[inline]
    fn load(&self, ordering: Ordering) -> u32 {
        self.0.load(ordering)
    }

    #[cfg(test)]
    #[inline]
    fn store(&self, value: u32, ordering: Ordering) {
        self.0.store(value, ordering);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    const CACHE_LINE_BYTES: usize = 64;

    #[test]
    fn barrier_counters_use_separate_cache_lines() {
        assert_eq!(CACHE_LINE_BYTES, 64);
        assert_eq!(std::mem::align_of::<BarrierCounter>(), CACHE_LINE_BYTES);
        assert_eq!(std::mem::size_of::<BarrierCounter>(), CACHE_LINE_BYTES);
        assert_eq!(std::mem::offset_of!(State, wait), 0);
        assert_eq!(std::mem::offset_of!(State, workers), CACHE_LINE_BYTES);
    }

    #[test]
    fn barrier_check_arms_worker() {
        let barrier = WorkerBarrier::new(1);
        let flag = Arc::new(AtomicBool::new(false));

        let worker_barrier = barrier.clone();
        let flag_c = Arc::clone(&flag);

        let (tx, rx) = mpsc::channel();

        let worker = thread::spawn(move || {
            rx.recv().unwrap();
            worker_barrier.check();
            flag_c.store(true, Ordering::Release);
        });

        barrier.arm();
        tx.send(()).unwrap();

        while barrier.paused_workers() != 1 {
            spin_loop();
        }

        assert!(!flag.load(Ordering::Acquire));

        barrier.release();

        worker.join().unwrap();
        assert!(flag.load(Ordering::Acquire));
    }

    #[test]
    fn barrier_sync_guards_workers() {
        let barrier = WorkerBarrier::new(1);
        let worker_barrier = barrier.clone();

        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            worker_barrier.check();
        });

        barrier.sync(|| {
            assert_eq!(barrier.paused_workers(), 1);
        });

        worker.join().unwrap();
    }

    #[test]
    fn barrier_releases_after_protected_mutation() {
        let barrier = WorkerBarrier::new(1);
        let published = Arc::new(AtomicU32::new(1));

        let worker_barrier = barrier.clone();
        let worker_published = Arc::clone(&published);
        let worker = thread::spawn(move || {
            while !worker_barrier.is_pending() {
                spin_loop();
            }
            worker_barrier.check();
            worker_published.load(Ordering::Acquire)
        });

        let mut next = 1_u32;
        barrier.sync(|| {
            next = 2;
            published.store(next, Ordering::Release);
        });

        assert_eq!(worker.join().expect("worker"), 2);
        assert_eq!(next, 2);
    }

    #[test]
    fn barrier_concurrent_workers() {
        let n = 4;
        let barrier = WorkerBarrier::new(n);
        let counter = Arc::new(AtomicU32::new(0));
        let barrier_armed = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        for _ in 0..n {
            let worker_barrier = barrier.clone();
            let c = Arc::clone(&counter);
            let armed = Arc::clone(&barrier_armed);
            handles.push(thread::spawn(move || {
                while !armed.load(Ordering::Acquire) {
                    spin_loop();
                }
                worker_barrier.check();
                c.fetch_add(1, Ordering::Relaxed);
            }));
        }

        barrier.arm();
        barrier_armed.store(true, Ordering::Release);

        while barrier.paused_workers() != n {
            spin_loop();
        }

        assert_eq!(counter.load(Ordering::Acquire), 0);

        barrier.release();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Acquire), n);
    }

    #[test]
    fn barrier_release_waits_for_worker_owned_count_to_reach_zero() {
        let barrier = WorkerBarrier::new(1);
        barrier.state.wait.store(1, Ordering::Release);
        barrier.state.workers.store(1, Ordering::Release);
        let (released_tx, released_rx) = mpsc::channel();
        let (exit_tx, exit_rx) = mpsc::channel();

        let worker_barrier = barrier.clone();
        let worker = thread::spawn(move || {
            while worker_barrier.is_pending() {
                spin_loop();
            }
            released_tx.send(()).expect("report barrier release");
            exit_rx.recv().expect("leave barrier");
            worker_barrier.state.workers.fetch_sub(1, Ordering::Release);
        });

        let release_barrier = barrier.clone();
        let releaser = thread::spawn(move || release_barrier.release());

        released_rx.recv().expect("worker observed release");
        exit_tx.send(()).expect("allow worker exit");
        worker.join().expect("worker");
        releaser.join().expect("releaser");
        assert_eq!(barrier.paused_workers(), 0);
    }

    #[test]
    fn recursive_barrier_releases_workers_only_at_outer_drop() {
        let barrier = WorkerBarrier::new(1);
        let resumed = Arc::new(AtomicBool::new(false));

        let worker_barrier = barrier.clone();
        let worker_resumed = Arc::clone(&resumed);
        let worker = thread::spawn(move || {
            while !worker_barrier.is_pending() {
                spin_loop();
            }
            worker_barrier.check();
            worker_resumed.store(true, Ordering::Release);
        });

        barrier.sync(|| {
            barrier.sync(|| {});
            assert!(!resumed.load(Ordering::Acquire));
            assert!(barrier.is_pending());
        });
        worker.join().expect("worker");
        assert!(resumed.load(Ordering::Acquire));
    }

    #[test]
    fn barrier_deadlock_is_fail_stop() {
        const CHILD_ENV: &str = "HAMMER_BARRIER_DEADLOCK_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            wait_for_worker_count(
                &BarrierCounter::new(0),
                1,
                Instant::now(),
                "barrier sync",
                Location::caller(),
            );
        }

        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "barrier::tests::barrier_deadlock_is_fail_stop",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .expect("run barrier deadlock child");

        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("barrier sync: worker thread deadlock")
        );
    }
}
