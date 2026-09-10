use crate::DataPlaneMain;
use std::future::Future;
use std::time::Duration;

/// Runs the thread-zero graph lifecycle and Tokio executor.
///
/// VPP correspondence: `vlib_main` initializes the graph, dispatches normal
/// init/config and main-loop-enter callbacks, performs the later initial worker
/// barrier, starts Process Nodes, and keeps the final worker barrier held while
/// exit callbacks run.
pub fn run<F, T>(
    global: crate::GlobalMain,
    threads: crate::ThreadMain,
    plugins: crate::PluginMain,
    main: &mut DataPlaneMain,
    future: F,
) -> crate::RuntimeResult<T>
where
    F: Future<Output = crate::RuntimeResult<T>>,
{
    crate::ensure_main_thread()?;
    assert_eq!(
        main.thread_index(),
        0,
        "thread-zero lifecycle requires main index 0"
    );

    assert!(
        crate::global_main::GLOBAL_MAIN.set(global).is_ok(),
        "GlobalMain initializes once"
    );
    assert!(
        crate::thread_main::THREAD_MAIN.set(threads).is_ok(),
        "ThreadMain initializes once"
    );
    assert!(
        crate::plugin::PLUGIN_MAIN.set(plugins).is_ok(),
        "PluginMain initializes once"
    );
    let global = crate::GlobalMain::global();

    let run_result = (|| -> crate::RuntimeResult<T> {
        main.init_graph_from_declarations(
            global.node_registrations.iter().copied(),
            global.node_function_registrations.iter().copied(),
        )?;
        crate::init::run_init_functions(global, main)?;
        crate::init::run_config_functions(global, Some(main), false, global.startup_config())?;
        crate::init::run_main_loop_enter(global, main)?;

        crate::worker_thread_barrier_sync!(main, {});
        main.start_processes(global.node_registrations.iter().copied())?;
        main.run_main_until(future)?
    })();

    let finish = || {
        let process_result = main.stop_processes();
        let exit_result = crate::init::run_main_loop_exit(global, main);

        let output = run_result?;
        process_result?;
        exit_result?;
        Ok(output)
    };
    match crate::barrier::global() {
        Some(barrier) if !barrier.startup_cancelled() => barrier.final_sync(finish),
        _ => finish(),
    }
}

/// VPP-style fixed-schedule data-plane main loop.
///
/// Step order mirrors VPP `main.c:1442-1693`:
/// 1. Barrier check (workers_at_barrier / wait_at_barrier)
/// 2. Poll worker-local File readiness
/// 3. Drain handoff and run ready nodes
/// 4. Schedule polling-state driver nodes (periodically)
/// 5. Run ready nodes (handles interrupt frames + newly-scheduled polling frames)
/// 6. Dispatch timer nodes (no timer wheel in data-plane yet)
/// 7. Advance timers, increment main_loop_count and check exit
pub fn data_plane_main_loop(main: &mut DataPlaneMain, idle_slice: Duration) -> i32 {
    main.attach_worker_interrupt_thread();

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

        // Step 3: Drain handoff queues and run ready nodes.
        let _ = main.run_ready_nodes();

        if !progress {
            std::thread::sleep(idle_slice);
        }

        // Step 4: Run any newly-scheduled frames (pre-input + input)
        let _ = main.run_ready_nodes();

        // Step 5: Dispatch timer nodes (no data-plane timer wheel yet)

        // Step 6: Advance timers — deferred (no data-plane timer wheel yet).
        // VPP dispatches timer-wheel-expired sched nodes here.
        // Increment loop count.
        main.increment_main_loop_count();

        // Step 7: Exit check
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
