use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::thread;
use std::time::Instant;

use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

use crate::barrier;
use crate::engine::Engine;
use crate::spawn;
use crate::spawn::{DATA_LOCAL_DRIVER_WAKER, DATA_WORKER_IDLE_SLICE, with_data_plane_runtime};

/// VPP-style fixed-schedule engine main loop.
///
/// Step order mirrors VPP `main.c:1442-1693`:
/// 1. Barrier check (workers_at_barrier / wait_at_barrier)
/// 2. Poll worker-local File readiness
/// 3. Drain handoff + run ready nodes, poll remote-local queue, poll DataLocalTask futures
/// 4. Tokio reactor tick — drive transport/session futures
/// 5. Schedule polling-state driver nodes (periodically)
/// 6. Run ready nodes (handles interrupt frames + newly-scheduled polling frames)
/// 7. Dispatch timer nodes (no timer wheel in data-plane yet)
/// 8. Advance timers, increment main_loop_count
/// 9. Exit if main_loop_exit_now
pub fn engine_main_loop(
    engine: &mut Engine,
    runtime: &tokio::runtime::Runtime,
    remote_local: &spawn::DataRemoteLocalQueue,
) -> i32 {
    let wait = Arc::clone(&engine.wait_at_barrier);
    let workers = Arc::clone(&engine.workers_at_barrier);
    let idle_slice = DATA_WORKER_IDLE_SLICE.with(|s| s.get());

    let worker_waker = Arc::new(spawn::DataWorkerThreadWake {
        thread: thread::current(),
    })
    .into();
    let mut cx = Context::from_waker(&worker_waker);
    DATA_LOCAL_DRIVER_WAKER.with(|slot| {
        *slot.borrow_mut() = Some(worker_waker.clone());
    });

    let io_wake = {
        let _reactor = runtime.enter();
        match engine.file_main().io_wake_fd() {
            Ok(wake_fd) => match AsyncFd::with_interest(wake_fd, Interest::READABLE) {
                Ok(wake) => Some(wake),
                Err(error) => {
                    tracing::warn!(
                        worker = engine.thread_index,
                        %error,
                        "File wake fd registration failed; idle sleep is fixed-slice"
                    );
                    None
                }
            },
            Err(error) => {
                tracing::warn!(
                    worker = engine.thread_index,
                    %error,
                    "File wake fd duplication failed; idle sleep is fixed-slice"
                );
                None
            }
        }
    };

    let mut last_poll_drivers_at = Instant::now();

    loop {
        let mut progress = false;

        // Step 1: Barrier check — VPP threads.c:296
        if barrier::barrier_check_and_report(&wait, &workers, || {
            engine.publish_worker_runtime_stats();
        }) && !engine.apply_worker_graph_update_after_barrier()
        {
            return 1;
        }

        // Step 2: Poll worker-local File readiness before graph dispatch.
        match engine.poll_file_readiness() {
            Ok(dispatched) => progress |= dispatched != 0,
            Err(error) => {
                tracing::error!(worker = engine.thread_index, %error, "File poll failed");
                return 1;
            }
        }
        with_data_plane_runtime(|rt| {
            if let Ok(scheduled) = rt.schedule_interrupt_driver_nodes() {
                progress |= scheduled != 0;
            }
        });

        // Step 3: Drain handoff queues, run ready nodes, poll remote/local tasks
        with_data_plane_runtime(|rt| {
            let _ = rt.run_ready_nodes();
        });
        progress |= spawn::poll_remote_local_tasks(remote_local);
        progress |= spawn::poll_data_local_tasks(&mut cx);
        with_data_plane_runtime(|rt| {
            let _ = rt.run_ready_nodes();
        });

        // Step 4: Tokio reactor tick. VPP sleeps inside `epoll_wait`
        // (`vlib_file_poll`) so device readiness ends the idle wait
        // immediately; select the File wake fd against the idle slice for the
        // same behavior.
        if progress {
            runtime.block_on(async {
                tokio::task::yield_now().await;
            });
        } else {
            runtime.block_on(async {
                match &io_wake {
                    Some(wake) => tokio::select! {
                        guard = wake.readable() => match guard {
                            Ok(mut guard) => {
                                engine.file_main().clear_io_wake();
                                guard.clear_ready();
                            }
                            Err(_) => tokio::time::sleep(idle_slice).await,
                        },
                        _ = tokio::time::sleep(idle_slice) => {}
                    },
                    None => tokio::time::sleep(idle_slice).await,
                }
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
        if let Some(status) = requested_exit_status(engine) {
            return status;
        }
    }
}

fn requested_exit_status(engine: &Engine) -> Option<i32> {
    if !engine.main_loop_exit_now.load(Ordering::Acquire) {
        return None;
    }
    Some(
        *engine
            .main_loop_exit_status
            .lock()
            .expect("engine_main_loop: poisoned exit status mutex"),
    )
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;

    use crate::DataPlaneBufferConfig;
    use crate::engine::Engine;
    use crate::spawn::DataRemoteLocalQueue;
    use hammer_runtime::RuntimeRegistry;
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, File, FileFunctions};

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

        let main = Engine::new(rt, RuntimeRegistry::new());
        let mut engine = main.spawn(1).expect("spawn data worker");
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
        let status = super::engine_main_loop(&mut engine, &tokio_rt, &remote_local);
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "engine_main_loop should exit quickly when main_loop_exit_now is set, took {elapsed:?}"
        );
        assert_eq!(status, 0);
    }

    #[test]
    fn engine_main_loop_polls_worker_file_readiness_before_exit() {
        let rt = test_runtime();
        crate::spawn::set_data_plane_runtime(rt.clone());

        let main = Engine::new(rt, RuntimeRegistry::new());
        let mut engine = main.spawn(1).expect("spawn data worker");
        let (registered, mut peer) = UnixStream::pair().expect("create socket pair");
        let index = engine
            .file_main_mut()
            .add(File::new(
                OwnedFd::from(registered),
                "main-loop readiness".to_owned(),
                0,
                FileFunctions {
                    read: Some(|_, file| {
                        file.set_private_data(1);
                        Ok(())
                    }),
                    ..FileFunctions::default()
                },
            ))
            .expect("add file");
        peer.write_all(&[1]).expect("make socket readable");
        engine
            .main_loop_exit_now
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime for test");
        let remote_local = DataRemoteLocalQueue::default();
        remote_local.attach_current_thread();

        assert_eq!(
            super::engine_main_loop(&mut engine, &tokio_rt, &remote_local),
            0
        );
        assert_eq!(
            engine
                .file_main()
                .get(index)
                .expect("registered file")
                .private_data(),
            1
        );
    }
}
