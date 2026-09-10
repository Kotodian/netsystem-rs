use core::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::error::{RuntimeError, RuntimeResult};
use crate::{
    DataPlaneHandoff, DataPlaneMain, DataWorkerId, GlobalMain, ThreadMain, barrier, spawn,
};

pub fn start_workers(
    threads: &mut ThreadMain,
    main: &mut DataPlaneMain,
    global: &mut GlobalMain,
) -> RuntimeResult<()> {
    let worker_count = threads.worker_count();
    let barrier = barrier::install(worker_count);
    barrier.arm();
    let startup_cancelled = Arc::new(AtomicBool::new(false));
    let handoff = DataPlaneHandoff::with_node_capacity(
        worker_count as usize,
        crate::config::worker::handoff().queue_capacity,
        main.nodes().node_count(),
    );
    let queues: Arc<[spawn::DataRemoteLocalQueue]> = (0..worker_count)
        .map(|_| spawn::DataRemoteLocalQueue::new(crate::config::worker::control().queue_capacity))
        .collect::<Vec<_>>()
        .into();
    spawn::install_worker_control_queues(Arc::clone(&queues));

    let mut worker_mains = Vec::with_capacity(worker_count as usize);
    for worker_slot in 0..worker_count {
        let thread_index = worker_slot + 1;
        let descriptor = threads
            .thread_by_index(thread_index)
            .expect("configured worker descriptor exists");
        let (nodes, simd_bytes, _, trace_control) = main.worker_parts();
        worker_mains.push(DataPlaneMain::new_worker(
            nodes,
            simd_bytes,
            Some(handoff.worker(DataWorkerId::new(worker_slot))),
            trace_control,
            thread_index,
            descriptor.numa_node().unwrap_or(0),
        )?);
    }

    for (worker_slot, worker_main) in worker_mains.into_iter().enumerate() {
        let thread_index = worker_slot as u32 + 1;
        let descriptor = threads
            .thread_by_index_mut(thread_index)
            .expect("configured worker descriptor exists");
        descriptor.install_barrier(barrier.clone());
        if let Err(error) = descriptor.launch_data_worker(
            worker_main,
            queues[worker_slot].clone(),
            global.worker_init_function_registrations.clone(),
            barrier.clone(),
            Arc::clone(&startup_cancelled),
        ) {
            return Err(abort_workers(threads, &barrier, &startup_cancelled, error));
        }
    }

    if let Err(error) = wait_for_workers_at_barrier(threads, &barrier) {
        return Err(abort_workers(threads, &barrier, &startup_cancelled, error));
    }
    if let Err(error) = crate::init::run_num_workers_change(global, main) {
        return Err(abort_workers(threads, &barrier, &startup_cancelled, error));
    }
    for thread_index in 1..=worker_count {
        threads
            .thread_by_index_mut(thread_index)
            .expect("configured worker descriptor exists")
            .acknowledge_startup();
    }
    barrier.release();
    Ok(())
}

pub fn stop_workers(threads: &mut ThreadMain, status: i32) -> RuntimeResult<()> {
    let worker_count = threads.worker_count();
    for worker_slot in 0..worker_count {
        let worker = DataWorkerId::new(worker_slot);
        loop {
            match spawn::schedule_on_worker(worker, move |main| main.request_exit(status)) {
                Ok(()) => break,
                Err(RuntimeError::WorkerControlQueueFull { .. }) => std::thread::yield_now(),
                Err(error) => return Err(error),
            }
        }
    }
    for thread_index in 1..=worker_count {
        threads
            .thread_by_index_mut(thread_index)
            .expect("configured worker descriptor exists")
            .join()?;
    }
    Ok(())
}

fn wait_for_workers_at_barrier(
    threads: &ThreadMain,
    barrier: &barrier::WorkerBarrier,
) -> RuntimeResult<()> {
    let deadline = Instant::now() + barrier::BARRIER_SYNC_TIMEOUT;
    loop {
        let observed = barrier.paused_workers();
        if observed == barrier.worker_count() {
            return Ok(());
        }
        if (1..=threads.worker_count()).any(|thread_index| {
            threads
                .thread_by_index(thread_index)
                .is_some_and(crate::WorkerThread::is_finished)
        }) {
            return Err(RuntimeError::WorkerExitedBeforeStartupBarrier { phase: "launch" });
        }
        if Instant::now() > deadline {
            barrier::barrier_deadlock("worker launch barrier", barrier.worker_count(), observed);
        }
        spin_loop();
    }
}

fn abort_workers(
    threads: &mut ThreadMain,
    barrier: &barrier::WorkerBarrier,
    startup_cancelled: &AtomicBool,
    startup_error: RuntimeError,
) -> RuntimeError {
    startup_cancelled.store(true, Ordering::Release);
    barrier.release();
    for thread_index in 1..=threads.worker_count() {
        if let Some(thread) = threads.thread_by_index_mut(thread_index)
            && let Err(error) = thread.join()
        {
            tracing::error!(worker = thread_index, %error, "data worker failed while startup aborted");
        }
    }
    startup_error
}
