use core::hint::spin_loop;
use std::sync::Arc;
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
    let participant_count = threads
        .thread_count()
        .checked_sub(1)
        .expect("configured ThreadMain includes thread zero");
    let init_order = crate::init::topological_order(&global.worker_init_function_registrations)?;
    let init_functions: Arc<[&'static crate::init::InitFunction]> = init_order
        .into_iter()
        .map(|index| global.worker_init_function_registrations[index])
        .collect();
    let handoff = DataPlaneHandoff::with_node_capacity(
        worker_count as usize,
        crate::config::worker::handoff().queue_capacity,
        main.nodes().node_count(),
    );
    let queues: Arc<[spawn::DataRemoteLocalQueue]> = (0..worker_count)
        .map(|_| spawn::DataRemoteLocalQueue::new(crate::config::worker::control().queue_capacity))
        .collect::<Vec<_>>()
        .into();

    let mut worker_mains = Vec::with_capacity(worker_count as usize);
    for worker_slot in 0..worker_count {
        let thread_index = worker_slot + 1;
        let descriptor = threads
            .thread_by_index(thread_index)
            .expect("configured worker descriptor exists");
        let (nodes, simd_bytes, _, trace_control) = main.worker_parts();
        worker_mains.push(Box::new(DataPlaneMain::new_worker(
            nodes,
            simd_bytes,
            Some(handoff.worker(DataWorkerId::new(worker_slot))),
            trace_control,
            thread_index,
            descriptor.numa_node().unwrap_or(0),
        )?));
    }

    spawn::install_worker_control_queues(Arc::clone(&queues));
    let barrier = barrier::install(worker_count, participant_count);
    barrier.arm();
    for (worker_slot, worker_main) in worker_mains.into_iter().enumerate() {
        let thread_index = worker_slot as u32 + 1;
        let descriptor = threads
            .thread_by_index_mut(thread_index)
            .expect("configured worker descriptor exists");
        descriptor.install_barrier(barrier.clone());
        if let Err(error) = descriptor.launch(
            Some((worker_main, queues[worker_slot].clone())),
            Arc::clone(&init_functions),
        ) {
            return Err(abort_workers(threads, &barrier, error));
        }
    }
    for thread_index in worker_count + 1..threads.thread_count() {
        let descriptor = threads
            .thread_by_index_mut(thread_index)
            .expect("registered runtime thread descriptor exists");
        descriptor.install_barrier(barrier.clone());
        if let Err(error) = descriptor.launch(None, Arc::clone(&init_functions)) {
            return Err(abort_workers(threads, &barrier, error));
        }
    }

    if let Err(error) = wait_for_workers_at_barrier(threads, &barrier) {
        return Err(abort_workers(threads, &barrier, error));
    }
    if let Err(error) = crate::init::run_num_workers_change(global, main) {
        return Err(abort_workers(threads, &barrier, error));
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
    for thread_index in 1..threads.thread_count() {
        threads
            .thread_by_index_mut(thread_index)
            .expect("configured worker descriptor exists")
            .join()?;
    }
    Ok(())
}

fn wait_for_workers_at_barrier(
    threads: &mut ThreadMain,
    barrier: &barrier::WorkerBarrier,
) -> RuntimeResult<()> {
    let deadline = Instant::now() + barrier::BARRIER_SYNC_TIMEOUT;
    loop {
        let observed = barrier.paused_workers();
        if observed == barrier.participant_count() {
            return Ok(());
        }
        if let Some(thread_index) = (1..threads.thread_count()).find(|&thread_index| {
            threads
                .thread_by_index(thread_index)
                .is_some_and(crate::WorkerThread::is_finished)
        }) {
            return match threads
                .thread_by_index_mut(thread_index)
                .expect("finished runtime thread descriptor exists")
                .join()
            {
                Err(error) => Err(error),
                Ok(()) => Err(RuntimeError::ThreadExitedBeforeStartupBarrier { thread_index }),
            };
        }
        if Instant::now() > deadline {
            barrier::barrier_deadlock(
                "worker launch barrier",
                barrier.participant_count(),
                observed,
            );
        }
        spin_loop();
    }
}

fn abort_workers(
    threads: &mut ThreadMain,
    barrier: &barrier::WorkerBarrier,
    startup_error: RuntimeError,
) -> RuntimeError {
    barrier.cancel_startup();
    barrier.release();
    for thread_index in 1..threads.thread_count() {
        if let Some(thread) = threads.thread_by_index_mut(thread_index)
            && let Err(error) = thread.join()
        {
            tracing::error!(worker = thread_index, %error, "data worker failed while startup aborted");
        }
    }
    startup_error
}
