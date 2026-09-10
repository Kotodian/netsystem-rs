use std::sync::Arc;

use crate::error::{RuntimeError, RuntimeResult};
use crate::{
    DataPlaneHandoff, DataPlaneMain, DataWorkerId, GlobalMain, ThreadMain, barrier, spawn,
};

#[hammer_component_macros::main_loop_enter_function]
fn start_workers(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let threads = ThreadMain::global();
    let global = GlobalMain::global();
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
    for thread_index in 1..threads.thread_count() {
        threads
            .thread_by_index(thread_index)
            .expect("configured runtime thread descriptor exists")
            .install_barrier(barrier.clone());
    }
    barrier.arm();
    for (worker_slot, worker_main) in worker_mains.into_iter().enumerate() {
        let thread_index = worker_slot as u32 + 1;
        let descriptor = threads
            .thread_by_index(thread_index)
            .expect("configured worker descriptor exists");
        if let Err(error) = descriptor.launch(
            Some((worker_main, queues[worker_slot].clone())),
            Arc::clone(&init_functions),
        ) {
            return Err(cancel_startup(&barrier, error));
        }
    }
    for thread_index in worker_count + 1..threads.thread_count() {
        let descriptor = threads
            .thread_by_index(thread_index)
            .expect("registered runtime thread descriptor exists");
        if let Err(error) = descriptor.launch(None, Arc::clone(&init_functions)) {
            return Err(cancel_startup(&barrier, error));
        }
    }

    crate::worker_thread_barrier_sync!(main, { crate::init::run_num_workers_change(global, main) })
}

fn cancel_startup(barrier: &barrier::WorkerBarrier, startup_error: RuntimeError) -> RuntimeError {
    barrier.cancel_startup();
    barrier.release_startup();
    startup_error
}
