use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::barrier;
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

fn worker_main(_: u32, engine: Engine) {
    // TODO(C4): replace stub loop with engine_main_loop(&engine)
    // engine_init_graph(&engine.runtime, idx) must also be called
    // before entering the loop.
    let wait = engine.wait_at_barrier;
    let workers = engine.workers_at_barrier;
    loop {
        barrier::barrier_check(&wait, &workers);
        std::thread::sleep(std::time::Duration::from_millis(10));
        if engine
            .main_loop_exit_now
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            break;
        }
    }
}

#[::linkme::distributed_slice(crate::init::INIT_FUNCTIONS)]
static __INIT_FN_START_WORKERS: crate::init::InitFunction = crate::init::InitFunction {
    name: "start_workers",
    runs_before: &[],
    runs_after: &[],
    func: start_workers,
};
