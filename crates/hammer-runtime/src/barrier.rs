use core::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

/// RAII guard that releases the barrier when dropped.
/// The control thread holds this while mutating shared state.
#[must_use]
pub struct BarrierGuard(Arc<AtomicU32>, Arc<AtomicU32>);

impl BarrierGuard {
    fn new(wait: &Arc<AtomicU32>, workers: &Arc<AtomicU32>) -> Self {
        BarrierGuard(Arc::clone(wait), Arc::clone(workers))
    }
}

/// Release workers from the barrier. Called by BarrierGuard::drop and
/// DataPlaneBarrierGuard::drop.
pub fn barrier_release(wait: &AtomicU32, workers: &AtomicU32) {
    wait.store(0, Ordering::Release);
    while workers.load(Ordering::Acquire) > 0 {
        spin_loop();
    }
}

impl Drop for BarrierGuard {
    fn drop(&mut self) {
        barrier_release(&self.0, &self.1);
    }
}

/// Synchronize all workers: set the wait flag, then spin until all workers
/// have acknowledged. Returns a guard that releases the barrier on drop.
///
/// Memory ordering mirrors VPP threads.c:296 barrier_check:
/// - wait_at_barrier: release-store (main), acquire-load (workers)
/// - workers_at_barrier: fetch_add Release (workers), load Acquire (main)
pub fn barrier_sync(
    wait: &Arc<AtomicU32>,
    workers: &Arc<AtomicU32>,
    n_workers: u32,
) -> BarrierGuard {
    wait.store(1, Ordering::SeqCst);
    std::sync::atomic::compiler_fence(Ordering::SeqCst);
    while workers.load(Ordering::Acquire) != n_workers {
        spin_loop();
    }
    BarrierGuard::new(wait, workers)
}

/// Called by workers in their main loop. If the wait flag is set,
/// acknowledge and spin until released.
pub fn barrier_check(wait: &AtomicU32, workers: &AtomicU32) {
    _ = barrier_check_and_report(wait, workers);
}

/// Runtime-internal barrier check that reports whether this worker crossed a
/// barrier release and must inspect a published graph update before dispatch
/// resumes.
pub(crate) fn barrier_check_and_report(wait: &AtomicU32, workers: &AtomicU32) -> bool {
    if wait.load(Ordering::Acquire) > 0 {
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

/// Startup-only barrier check whose release advances to another non-zero
/// phase. The regular control-plane barrier always releases to zero.
pub(crate) fn barrier_check_phase(wait: &AtomicU32, workers: &AtomicU32, phase: u32) {
    debug_assert_ne!(phase, 0);
    workers.fetch_add(1, Ordering::Release);
    while wait.load(Ordering::Acquire) == phase {
        spin_loop();
    }
    workers.fetch_sub(1, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;
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

        wait.store(1, Ordering::SeqCst);
        std::sync::atomic::compiler_fence(Ordering::SeqCst);
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

        wait.store(1, Ordering::SeqCst);
        std::sync::atomic::compiler_fence(Ordering::SeqCst);
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
}
