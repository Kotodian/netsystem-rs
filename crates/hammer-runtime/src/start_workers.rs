use std::sync::Arc;

use crate::error::{RuntimeError, RuntimeResult};
use hammer_stats::StatsMain;

use crate::thread_main::WorkerThreadCount;
use crate::{DataPlaneHandoff, DataPlaneMain, DataWorkerId, GlobalMain, ThreadMain, barrier};

#[hammer_component_macros::main_loop_enter_function]
fn start_workers(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let threads = ThreadMain::global();
    let global = GlobalMain::global();
    let worker_count = threads.worker_count();
    let participant_count = threads
        .thread_count()
        .checked_sub(1)
        .expect("configured ThreadMain includes thread zero");
    // The worker-count gauge belongs to this domain and is written once, like
    // VPP's thread startup publishing `n_vlib_mains - 1`.
    StatsMain::global()?.segment.set_gauge(
        WorkerThreadCount::global().worker_threads.index,
        u64::from(worker_count),
    );
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
    // One counter row per thread, at the same frozen node capacity the handoff
    // uses; thread zero's row goes into this graph and each Worker's row into
    // that Worker's graph clone before launch.
    let node_counter_rows =
        crate::node_stats::NodeCounterRows::install(worker_count + 1, main.nodes().node_count());
    main.nodes.install_node_counters(
        node_counter_rows
            .row(0)
            .expect("thread zero owns a counter row"),
    );
    // Every Worker runtime receives its per-thread error fact at this freeze
    // point, like VPP's worker clone refresh
    // (`third_party/vpp/src/vlib/threads.c:766-778`): the entry thread zero's
    // registration path published, or `None` when no node declared errors. The
    // record row is the thread index, so the entry is the whole per-thread fact.
    let node_error_stats_entry = main.node_error_stats_entry_index.get();
    let mut worker_mains = Vec::with_capacity(worker_count as usize);
    for worker_slot in 0..worker_count {
        let thread_index = worker_slot + 1;
        let descriptor = threads
            .thread_by_index(thread_index)
            .expect("configured worker descriptor exists");
        let (mut nodes, simd_bytes, _, trace_control) = main.worker_parts();
        nodes.install_node_counters(
            node_counter_rows
                .row(thread_index)
                .expect("every Data Worker owns a counter row"),
        );
        let worker_main = DataPlaneMain::new_worker(
            nodes,
            simd_bytes,
            Some(handoff.worker(DataWorkerId::new(worker_slot))),
            trace_control,
            thread_index,
            descriptor.numa_node().unwrap_or(0),
        )?;
        worker_main.install_node_error_stats_entry(node_error_stats_entry);
        worker_mains.push(Box::new(worker_main));
    }

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
        if let Err(error) = descriptor.launch(Some(worker_main), Arc::clone(&init_functions)) {
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
