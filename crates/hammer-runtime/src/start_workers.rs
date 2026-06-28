use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::data_plane::new_worker_runtime;
use crate::engine::Engine;
use crate::spawn;
use hammer_core::error::HammerResult;

pub const WORKER_COUNT: u32 = 2;

pub fn start_workers(engine: &mut Engine) -> HammerResult<()> {
    let wait = Arc::new(AtomicU32::new(0));
    let workers = Arc::new(AtomicU32::new(0));
    let registry = Arc::clone(&engine.registry);

    engine.wait_at_barrier = Arc::clone(&wait);
    engine.workers_at_barrier = Arc::clone(&workers);

    for idx in 1..=WORKER_COUNT {
        let wait = Arc::clone(&wait);
        let workers = Arc::clone(&workers);
        let registry = Arc::clone(&registry);

        thread::Builder::new()
            .name(format!("hammer-worker-{idx}"))
            .spawn(move || {
                let rt = new_worker_runtime(2048, 256);
                spawn::set_data_plane_runtime(rt.clone());

                let engine = Engine {
                    thread_index: idx,
                    numa_node: 0,
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

fn worker_main(idx: u32, engine: Engine) {
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
