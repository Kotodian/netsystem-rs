use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

use crate::DataPlaneMain;
use crate::FILE_MAIN;
use crate::spawn;
use std::time::Duration;

/// VPP-style fixed-schedule data-plane main loop.
///
/// Step order mirrors VPP `main.c:1442-1693`:
/// 1. Barrier check (workers_at_barrier / wait_at_barrier)
/// 2. Poll worker-local File readiness
/// 3. Drain handoff, run ready nodes, and poll the worker control queue
/// 4. Tokio reactor tick — drive transport/session futures
/// 5. Schedule polling-state driver nodes (periodically)
/// 6. Run ready nodes (handles interrupt frames + newly-scheduled polling frames)
/// 7. Dispatch timer nodes (no timer wheel in data-plane yet)
/// 8. Advance timers, increment main_loop_count and check exit
pub fn data_plane_main_loop(
    main: &mut DataPlaneMain,
    runtime: &tokio::runtime::Runtime,
    remote_local: &spawn::DataRemoteLocalQueue,
    idle_slice: Duration,
) -> i32 {
    main.attach_worker_interrupt_thread();

    let io_wake = {
        let _reactor = runtime.enter();
        match FILE_MAIN
            .get()
            .expect("FileMain is initialized before Data Worker startup")
            .io_wake_fd_for_worker(main.thread_index())
        {
            Ok(wake_fd) => match AsyncFd::with_interest(wake_fd, Interest::READABLE) {
                Ok(wake) => Some(wake),
                Err(error) => {
                    tracing::warn!(
                        worker = main.thread_index(),
                        %error,
                        "File wake fd registration failed; idle sleep is fixed-slice"
                    );
                    None
                }
            },
            Err(error) => {
                tracing::warn!(
                        worker = main.thread_index(),
                    %error,
                    "File wake fd duplication failed; idle sleep is fixed-slice"
                );
                None
            }
        }
    };

    loop {
        let mut progress = false;

        // Step 1: Barrier check — VPP threads.c:296
        if let Some(barrier) = crate::barrier::global()
            && barrier.is_pending()
            && barrier.check_for_refork()
        {
            barrier.refork(&mut main.nodes);
        }

        // Step 2: Poll worker-local File readiness before graph dispatch.
        match main.poll_file_readiness() {
            Ok(dispatched) => progress |= dispatched != 0,
            Err(error) => {
                tracing::error!(worker = main.thread_index(), %error, "File poll failed");
                return 1;
            }
        }
        if let Ok(scheduled) = main.schedule_remote_interrupts() {
            progress |= scheduled != 0;
        }
        if let Ok(scheduled) = main.schedule_polling_pre_input_nodes() {
            progress |= scheduled != 0;
        }
        if let Ok(scheduled) = main.schedule_interrupt_pre_input_nodes() {
            progress |= scheduled != 0;
        }
        if let Ok(scheduled) = main.schedule_polling_driver_nodes() {
            progress |= scheduled != 0;
        }
        if let Ok(scheduled) = main.schedule_interrupt_driver_nodes() {
            progress |= scheduled != 0;
        }

        // Step 3: Drain handoff queues, run ready nodes, poll remote/local tasks
        let _ = main.run_ready_nodes();
        progress |= spawn::poll_remote_local_tasks(remote_local, main);
        let _ = main.run_ready_nodes();

        // Step 4: Tokio reactor tick. VPP sleeps inside `epoll_wait`
        // (`vlib_file_poll`) so device readiness ends the idle wait
        // immediately; select the File wake fd against the idle slice for the
        // same behavior.
        if progress {
            runtime.block_on(async {
                tokio::task::yield_now().await;
            });
        } else {
            let wake_error = runtime.block_on(async {
                match &io_wake {
                    Some(wake) => tokio::select! {
                        guard = wake.readable() => match guard {
                            Ok(mut guard) => {
                                let result = FILE_MAIN
                                    .get()
                                    .expect("FileMain is initialized before Data Worker startup")
                                    .clear_io_wake_for_worker(main.thread_index());
                                guard.clear_ready();
                                result.err()
                            }
                            Err(_) => {
                                tokio::time::sleep(idle_slice).await;
                                None
                            },
                        },
                        _ = tokio::time::sleep(idle_slice) => None
                    },
                    None => {
                        tokio::time::sleep(idle_slice).await;
                        None
                    }
                }
            });
            if let Some(error) = wake_error {
                tracing::error!(worker = main.thread_index(), %error, "File wake clear failed");
                return 1;
            }
        }

        // Step 5: Run any newly-scheduled frames (pre-input + input)
        let _ = main.run_ready_nodes();

        // Step 6: Dispatch timer nodes (no data-plane timer wheel yet)

        // Step 7: Advance timers — deferred (no data-plane timer wheel yet).
        // VPP dispatches timer-wheel-expired sched nodes here.
        // Increment loop count.
        main.increment_main_loop_count();

        // Step 8: Exit check
        if let Some(status) = requested_exit_status(main) {
            return status;
        }
    }
}

fn requested_exit_status(main: &DataPlaneMain) -> Option<i32> {
    if !main.main_loop_exit_requested() {
        return None;
    }
    Some(main.main_loop_exit_status())
}
