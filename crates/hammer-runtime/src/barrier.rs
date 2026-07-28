//! VPP-style main/control-thread barrier.
//!
//! Main/control code may synchronize and release; workers only acknowledge
//! from their runtime loop. A missed deadline is a process-fatal worker
//! deadlock, not a recoverable runtime error.

use core::hint::spin_loop;
use core::ops::{Deref, DerefMut};
use std::panic::Location;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

#[cfg(debug_assertions)]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_millis(600_100);
#[cfg(not(debug_assertions))]
pub(crate) const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_secs(1);

/// RAII guard that releases the barrier when dropped.
/// The control thread holds this while mutating shared state.
#[must_use]
#[derive(Debug)]
pub struct BarrierGuard {
    wait: Arc<AtomicU32>,
    workers: Arc<AtomicU32>,
    caller: &'static Location<'static>,
}

impl BarrierGuard {
    fn new(
        wait: &Arc<AtomicU32>,
        workers: &Arc<AtomicU32>,
        caller: &'static Location<'static>,
    ) -> Self {
        Self {
            wait: Arc::clone(wait),
            workers: Arc::clone(workers),
            caller,
        }
    }
}

/// Release workers from the barrier.
#[track_caller]
pub fn barrier_release(wait: &AtomicU32, workers: &AtomicU32) {
    barrier_release_from(wait, workers, Location::caller());
}

impl Drop for BarrierGuard {
    fn drop(&mut self) {
        barrier_release_from(&self.wait, &self.workers, self.caller);
    }
}

/// Mutable access to a value while all Data Workers are synchronized.
///
/// The type makes the barrier requirement part of the protected mutation:
/// access ends before the contained guard releases the workers.
#[must_use]
pub struct Barrier<'a, T: ?Sized> {
    value: &'a mut T,
    guard: BarrierGuard,
}

impl<'a, T: ?Sized> Barrier<'a, T> {
    /// Synchronizes every configured Data Worker before granting mutable access.
    #[track_caller]
    pub fn sync(value: &'a mut T, engine: &crate::engine::Engine) -> Self {
        let worker_count = u32::try_from(engine.configured_worker_count())
            .expect("configured Data Worker count must fit in u32");
        Self::new(
            value,
            &engine.wait_at_barrier,
            &engine.workers_at_barrier,
            worker_count,
        )
    }

    #[track_caller]
    pub(crate) fn new(
        value: &'a mut T,
        wait: &Arc<AtomicU32>,
        workers: &Arc<AtomicU32>,
        worker_count: u32,
    ) -> Self {
        let guard = barrier_sync(wait, workers, worker_count);
        Self { value, guard }
    }
}

impl<T: ?Sized> Deref for Barrier<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        debug_assert_eq!(self.guard.wait.load(Ordering::Acquire), 1);
        self.value
    }
}

impl<T: ?Sized> DerefMut for Barrier<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}

/// Synchronize all workers: set the wait flag, then spin until all workers
/// have acknowledged. Returns a guard that releases the barrier on drop.
///
/// Memory ordering mirrors VPP threads.c:296 barrier_check:
/// - wait_at_barrier: release-store (main), acquire-load (workers)
/// - workers_at_barrier: fetch_add Release (workers), load Acquire (main)
#[track_caller]
pub fn barrier_sync(
    wait: &Arc<AtomicU32>,
    workers: &Arc<AtomicU32>,
    n_workers: u32,
) -> BarrierGuard {
    let caller = Location::caller();
    let recursion_level = wait.fetch_add(1, Ordering::Release);
    if recursion_level == 0 {
        wait_for_worker_count(
            workers,
            n_workers,
            Instant::now() + BARRIER_SYNC_TIMEOUT,
            "barrier sync",
            caller,
        );
    }
    BarrierGuard::new(wait, workers, caller)
}

fn barrier_release_from(wait: &AtomicU32, workers: &AtomicU32, caller: &'static Location<'static>) {
    let recursion_level = wait.fetch_sub(1, Ordering::Release);
    if recursion_level == 0 {
        barrier_deadlock_at("barrier release without matching sync", 1, 0, caller);
    }
    if recursion_level > 1 {
        return;
    }
    wait_for_worker_count(
        workers,
        0,
        Instant::now() + BARRIER_SYNC_TIMEOUT,
        "barrier release",
        caller,
    );
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

/// Called by workers in their main loop. If the wait flag is set,
/// acknowledge and spin until released.
pub fn barrier_check(wait: &AtomicU32, workers: &AtomicU32) {
    _ = barrier_check_and_report(wait, workers, || {});
}

/// Runtime-internal barrier check that reports whether this worker crossed a
/// barrier release and must inspect a published graph update before dispatch
/// resumes.
pub(crate) fn barrier_check_and_report(
    wait: &AtomicU32,
    workers: &AtomicU32,
    before_wait: impl FnOnce(),
) -> bool {
    if wait.load(Ordering::Acquire) > 0 {
        before_wait();
        workers.fetch_add(1, Ordering::Release);
        while wait.load(Ordering::Acquire) > 0 {
            spin_loop();
        }
        workers.fetch_sub(1, Ordering::Release);
        true
    } else {
        false
    }
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
        let wait = Arc::new(AtomicU32::new(0));
        let workers = Arc::new(AtomicU32::new(0));
        let flag = Arc::new(AtomicBool::new(false));

        let wait_c = Arc::clone(&wait);
        let workers_c = Arc::clone(&workers);
        let flag_c = Arc::clone(&flag);

        let (tx, rx) = mpsc::channel();

        let worker = thread::spawn(move || {
            rx.recv().unwrap();
            barrier_check(&wait_c, &workers_c);
            flag_c.store(true, Ordering::Release);
        });

        wait.store(1, Ordering::Release);
        tx.send(()).unwrap();

        while workers.load(Ordering::Acquire) != 1 {
            spin_loop();
        }

        assert!(!flag.load(Ordering::Acquire));

        barrier_release(&wait, &workers);

        worker.join().unwrap();
        assert!(flag.load(Ordering::Acquire));
    }

    #[test]
    fn barrier_sync_guards_workers() {
        let wait = Arc::new(AtomicU32::new(0));
        let workers = Arc::new(AtomicU32::new(0));

        let wait_c = Arc::clone(&wait);
        let workers_c = Arc::clone(&workers);

        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            barrier_check(&wait_c, &workers_c);
        });

        let guard = barrier_sync(&wait, &workers, 1);
        assert!(workers.load(Ordering::Acquire) >= 1);
        drop(guard);

        worker.join().unwrap();
    }

    #[test]
    fn typed_barrier_releases_after_protected_mutation() {
        let wait = Arc::new(AtomicU32::new(0));
        let workers = Arc::new(AtomicU32::new(0));
        let published = Arc::new(AtomicU32::new(1));

        let worker_wait = Arc::clone(&wait);
        let worker_count = Arc::clone(&workers);
        let worker_published = Arc::clone(&published);
        let worker = thread::spawn(move || {
            while worker_wait.load(Ordering::Acquire) == 0 {
                spin_loop();
            }
            barrier_check(&worker_wait, &worker_count);
            worker_published.load(Ordering::Acquire)
        });

        let mut next = 1_u32;
        {
            let mut update = Barrier::new(&mut next, &wait, &workers, 1);
            *update = 2;
            published.store(*update, Ordering::Release);
        }

        assert_eq!(worker.join().expect("worker"), 2);
        assert_eq!(next, 2);
    }

    #[test]
    fn barrier_concurrent_workers() {
        let n = 4;
        let wait = Arc::new(AtomicU32::new(0));
        let workers_at_barrier = Arc::new(AtomicU32::new(0));
        let counter = Arc::new(AtomicU32::new(0));
        let barrier_armed = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        for _ in 0..n {
            let w = Arc::clone(&wait);
            let wk = Arc::clone(&workers_at_barrier);
            let c = Arc::clone(&counter);
            let armed = Arc::clone(&barrier_armed);
            handles.push(thread::spawn(move || {
                while !armed.load(Ordering::Acquire) {
                    spin_loop();
                }
                barrier_check(&w, &wk);
                c.fetch_add(1, Ordering::Relaxed);
            }));
        }

        wait.store(1, Ordering::Release);
        barrier_armed.store(true, Ordering::Release);

        while workers_at_barrier.load(Ordering::Acquire) != n as u32 {
            spin_loop();
        }

        assert_eq!(counter.load(Ordering::Acquire), 0);

        barrier_release(&wait, &workers_at_barrier);

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Acquire), n as u32);
    }

    #[test]
    fn barrier_release_waits_for_worker_owned_count_to_reach_zero() {
        let wait = Arc::new(AtomicU32::new(1));
        let workers = Arc::new(AtomicU32::new(1));
        let (released_tx, released_rx) = mpsc::channel();
        let (exit_tx, exit_rx) = mpsc::channel();

        let worker_wait = Arc::clone(&wait);
        let worker_count = Arc::clone(&workers);
        let worker = thread::spawn(move || {
            while worker_wait.load(Ordering::Acquire) > 0 {
                spin_loop();
            }
            released_tx.send(()).expect("report barrier release");
            exit_rx.recv().expect("leave barrier");
            worker_count.fetch_sub(1, Ordering::Release);
        });

        let release_wait = Arc::clone(&wait);
        let release_count = Arc::clone(&workers);
        let releaser = thread::spawn(move || barrier_release(&release_wait, &release_count));

        released_rx.recv().expect("worker observed release");
        exit_tx.send(()).expect("allow worker exit");
        worker.join().expect("worker");
        releaser.join().expect("releaser");
        assert_eq!(workers.load(Ordering::Acquire), 0);
    }

    #[test]
    fn recursive_barrier_releases_workers_only_at_outer_drop() {
        let wait = Arc::new(AtomicU32::new(0));
        let workers = Arc::new(AtomicU32::new(0));
        let resumed = Arc::new(AtomicBool::new(false));

        let worker_wait = Arc::clone(&wait);
        let worker_count = Arc::clone(&workers);
        let worker_resumed = Arc::clone(&resumed);
        let worker = thread::spawn(move || {
            while worker_wait.load(Ordering::Acquire) == 0 {
                spin_loop();
            }
            barrier_check(&worker_wait, &worker_count);
            worker_resumed.store(true, Ordering::Release);
        });

        let outer = barrier_sync(&wait, &workers, 1);
        let inner = barrier_sync(&wait, &workers, 1);
        drop(inner);
        assert!(!resumed.load(Ordering::Acquire));
        assert_eq!(wait.load(Ordering::Acquire), 1);

        drop(outer);
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
