use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Waker};
use std::thread;
use std::time::Instant;

use crate::barrier;
use crate::engine::Engine;
use crate::spawn;
use crate::spawn::{DATA_LOCAL_DRIVER_WAKER, DATA_WORKER_IDLE_SLICE, with_data_plane_runtime};

/// VPP-style fixed-schedule engine main loop.
///
/// Step order mirrors VPP `main.c:1442-1693`:
/// 1. Barrier check (workers_at_barrier / wait_at_barrier)
/// 2. Drain handoff + run ready nodes, poll remote-local queue, poll DataLocalTask futures
/// 3. Main-loop callbacks (currently no-op)
/// 4. Tokio reactor tick — drive transport/session futures
/// 5. Schedule polling-state driver nodes (periodically)
/// 6. Run ready nodes (handles interrupt frames + newly-scheduled polling frames)
/// 7. Dispatch timer nodes (no timer wheel in data-plane yet)
/// 8. Advance timers, increment main_loop_count
/// 9. Exit if main_loop_exit_now
pub(crate) fn engine_main_loop(
    engine: &Engine,
    runtime: &tokio::runtime::Runtime,
    remote_local: &spawn::DataRemoteLocalQueue,
) -> i32 {
    let wait = &engine.wait_at_barrier;
    let workers = &engine.workers_at_barrier;
    let idle_slice = DATA_WORKER_IDLE_SLICE.with(|s| s.get());

    let worker_waker: Waker = Arc::new(spawn::DataWorkerThreadWake {
        thread: thread::current(),
    })
    .into();
    let waker: Waker = worker_waker.clone();
    let mut cx = Context::from_waker(&waker);
    DATA_LOCAL_DRIVER_WAKER.with(|slot| {
        *slot.borrow_mut() = Some(worker_waker);
    });

    let mut last_poll_drivers_at = Instant::now();

    loop {
        let mut progress = false;

        // Step 1: Barrier check — VPP threads.c:296
        barrier::barrier_check(wait, workers);

        // Step 2: Drain handoff queues, run ready nodes, poll remote/local tasks
        with_data_plane_runtime(|rt| {
            let _ = rt.run_ready_nodes();
        });
        progress |= spawn::poll_remote_local_tasks(remote_local);
        progress |= spawn::poll_data_local_tasks(&mut cx);
        with_data_plane_runtime(|rt| {
            let _ = rt.run_ready_nodes();
        });

        // Step 3: Main-loop callbacks (reserved for future hooks)

        // Step 4: Tokio reactor tick
        if progress {
            runtime.block_on(async {
                tokio::task::yield_now().await;
            });
        } else {
            runtime.block_on(async {
                tokio::time::sleep(idle_slice).await;
            });
        }

        // Step 5: Schedule polling driver nodes periodically
        let now = Instant::now();
        if now >= last_poll_drivers_at {
            last_poll_drivers_at = now + idle_slice;
            with_data_plane_runtime(|rt| {
                let _ = rt.schedule_polling_driver_nodes();
            });
        }

        // Step 6: Run any newly-scheduled frames (interrupt + polling)
        with_data_plane_runtime(|rt| {
            let _ = rt.run_ready_nodes();
        });

        // Step 7: Dispatch timer nodes (no data-plane timer wheel yet)

        // Step 8: Advance timers, increment loop count
        engine.main_loop_count.fetch_add(1, Ordering::Relaxed);

        // Step 9: Exit check
        if engine.main_loop_exit_now.load(Ordering::Relaxed) {
            let status = *engine.main_loop_exit_status.lock().unwrap();
            return status;
        }
    }
}
