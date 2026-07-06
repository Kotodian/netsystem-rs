use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

use crate::engine::Engine;
use crate::spawn;
use hammer_core::error::HammerResult;

pub const WORKER_COUNT: u32 = 2;

pub static WORKER_BARRIER_ARCS: OnceLock<(Arc<AtomicU32>, Arc<AtomicU32>, u32)> = OnceLock::new();

pub fn start_workers(engine: &mut Engine) -> HammerResult<()> {
    let wait = Arc::new(AtomicU32::new(0));
    let workers = Arc::new(AtomicU32::new(0));
    let n_workers = WORKER_COUNT;
    let registry = Arc::clone(&engine.registry);

    let _ = WORKER_BARRIER_ARCS.set((Arc::clone(&wait), Arc::clone(&workers), n_workers));

    engine.wait_at_barrier = Arc::clone(&wait);
    engine.workers_at_barrier = Arc::clone(&workers);

    for idx in 1..=WORKER_COUNT {
        let wait = Arc::clone(&wait);
        let workers = Arc::clone(&workers);
        let registry = Arc::clone(&registry);
        let worker_numa_node = engine.numa_node;
        let runtime_seed = engine.runtime.worker_seed();

        thread::Builder::new()
            .name(format!("hammer-worker-{idx}"))
            .spawn(move || {
                let rt = runtime_seed(idx, worker_numa_node);
                spawn::set_data_plane_runtime(rt.clone());

                let engine = Engine {
                    thread_index: idx,
                    numa_node: worker_numa_node,
                    main_loop_count: AtomicU32::new(0),
                    runtime: rt,
                    registry,
                    wait_at_barrier: wait,
                    workers_at_barrier: workers,
                    main_loop_exit_now: AtomicBool::new(false),
                    main_loop_exit_status: Mutex::new(0),
                };

                worker_main(idx, engine);
            })
            .map_err(|e| {
                hammer_core::error::HammerError::internal(format!(
                    "failed to spawn worker {idx}: {e}"
                ))
            })?;
    }

    Ok(())
}

fn worker_main(idx: u32, mut engine: Engine) {
    let _ = crate::init::run_worker_init_functions(&mut engine);

    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build per-worker tokio runtime");

    let remote_local = spawn::DataRemoteLocalQueue::default();
    remote_local.attach_current_thread();

    let exit_status = crate::main_loop::engine_main_loop(&engine, &tokio_rt, &remote_local);

    spawn::cleanup_thread_local();

    tracing::debug!("worker {idx} exited with status {exit_status}");
}

#[::linkme::distributed_slice(crate::init::INIT_FUNCTIONS)]
static __INIT_FN_START_WORKERS: crate::init::InitFunction = crate::init::InitFunction {
    name: "start_workers",
    runs_before: &[],
    runs_after: &[],
    func: start_workers,
};
