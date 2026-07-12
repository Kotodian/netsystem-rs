use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Context;
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
pub fn engine_main_loop(
    engine: &Engine,
    runtime: &tokio::runtime::Runtime,
    remote_local: &spawn::DataRemoteLocalQueue,
) -> i32 {
    let wait = &engine.wait_at_barrier;
    let workers = &engine.workers_at_barrier;
    let idle_slice = DATA_WORKER_IDLE_SLICE.with(|s| s.get());

    let worker_waker = Arc::new(spawn::DataWorkerThreadWake {
        thread: thread::current(),
    })
    .into();
    let mut cx = Context::from_waker(&worker_waker);
    DATA_LOCAL_DRIVER_WAKER.with(|slot| {
        *slot.borrow_mut() = Some(worker_waker.clone());
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
        dispatch_main_loop_callbacks();

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

        // Step 8: Advance timers — deferred (no data-plane timer wheel yet).
        // VPP dispatches timer-wheel-expired sched nodes here.
        // Increment loop count.
        engine.main_loop_count.fetch_add(1, Ordering::Relaxed);

        // Step 9: Exit check
        if engine.main_loop_exit_now.load(Ordering::Relaxed) {
            let status = *engine
                .main_loop_exit_status
                .lock()
                .expect("engine_main_loop: poisoned exit status mutex");
            return status;
        }
    }
}

/// Dispatch main-loop callbacks registered via `MAIN_LOOP_CALLBACKS`.
pub(crate) fn dispatch_main_loop_callbacks() {
    for callback in crate::init::MAIN_LOOP_CALLBACKS.iter() {
        callback();
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::Engine;
    use crate::spawn::DataRemoteLocalQueue;
    use hammer_core::data_plane::DataPlaneBufferConfig;
    use hammer_core::registry::RuntimeRegistry;
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};

    fn test_runtime() -> DataPlaneRuntime {
        let buffers = DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots: 4,
            frame_slots: 4,
            ..DataPlaneBufferConfig::default()
        };
        DataPlaneRuntime::new(DataPlaneRuntimeConfig { buffers })
    }

    #[test]
    fn engine_main_loop_exits_on_flag() {
        let rt = test_runtime();
        crate::spawn::set_data_plane_runtime(rt.clone());

        let engine = Engine::new(rt, RuntimeRegistry::new());
        engine
            .main_loop_exit_now
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime for test");

        let remote_local = DataRemoteLocalQueue::default();
        remote_local.attach_current_thread();

        let start = std::time::Instant::now();
        let status = super::engine_main_loop(&engine, &tokio_rt, &remote_local);
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "engine_main_loop should exit quickly when main_loop_exit_now is set, took {elapsed:?}"
        );
        assert_eq!(status, 0);
    }
}
