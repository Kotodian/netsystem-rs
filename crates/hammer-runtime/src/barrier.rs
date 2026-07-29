//! VPP-style main/control-thread barrier.
//!
//! Main/control code may synchronize and release; workers only acknowledge
//! from their runtime loop. A missed deadline is a process-fatal worker
//! deadlock, not a recoverable runtime error.

use core::hint::spin_loop;
use std::cell::UnsafeCell;
use std::panic::Location;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

#[cfg(debug_assertions)]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_millis(600_100);
#[cfg(not(debug_assertions))]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_secs(1);

struct State {
    wait: AtomicU32,
    workers: AtomicU32,
}

/// Value whose access is ordered by an associated worker barrier.
pub struct Barrier<T> {
    value: UnsafeCell<T>,
}

impl<T> Barrier<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
        }
    }

    /// # Safety
    /// The caller must hold the worker barrier or otherwise prove that no
    /// writer can access this value for the returned reference's lifetime.
    #[inline]
    pub(crate) unsafe fn get_unchecked(&self) -> &T {
        // SAFETY: upheld by the caller's barrier-phase contract.
        unsafe { &*self.value.get() }
    }

    /// # Safety
    /// The caller must have exclusive barrier-phase ownership of this value
    /// for the duration of `operation`.
    #[inline]
    pub unsafe fn with_mut_unchecked<R>(&self, operation: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: upheld by the caller's barrier-phase contract.
        operation(unsafe { &mut *self.value.get() })
    }
}

// SAFETY: `Barrier<T>` exposes its `UnsafeCell` only through unsafe operations
// whose callers must establish the worker-barrier or completion phase. Moving
// or dropping the shared value remains valid when `T: Send`.
unsafe impl<T: Send> Sync for Barrier<T> {}

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
                wait: AtomicU32::new(0),
                workers: AtomicU32::new(0),
            }),
            worker_count,
        }
    }

    /// Pauses every worker while `operation` has mutable access to `value`.
    #[track_caller]
    pub fn sync<T: ?Sized, R>(&self, value: &mut T, operation: impl FnOnce(&mut T) -> R) -> R {
        let caller = Location::caller();
        self.pause(caller);
        let release = Release {
            barrier: self.clone(),
            caller,
        };
        let result = operation(value);
        drop(release);
        result
    }

    #[inline]
    pub(crate) const fn worker_count(&self) -> u32 {
        self.worker_count
    }

    #[inline]
    pub(crate) fn is_pending(&self) -> bool {
        self.state.wait.load(Ordering::Acquire) != 0
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
    workers: &AtomicU32,
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

        let mut value = ();
        barrier.sync(&mut value, |_| {
            assert_eq!(barrier.paused_workers(), 1);
        });

        worker.join().unwrap();
    }

    #[test]
    fn typed_barrier_releases_after_protected_mutation() {
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
        barrier.sync(&mut next, |next| {
            *next = 2;
            published.store(*next, Ordering::Release);
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
        assert_eq!(counter.load(Ordering::Acquire), n as u32);
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

        let mut outer_value = ();
        let mut inner_value = ();
        barrier.sync(&mut outer_value, |_| {
            barrier.sync(&mut inner_value, |_| {});
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
                &AtomicU32::new(0),
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
