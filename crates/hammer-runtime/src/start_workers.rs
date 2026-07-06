use std::sync::atomic::AtomicU32;
use std::sync::{Arc, OnceLock};
use std::thread;

use crate::engine::Engine;
use crate::spawn;
use hammer_core::config::Worker;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::registry::RuntimeRegistry;

pub static WORKER_BARRIER_ARCS: OnceLock<(Arc<AtomicU32>, Arc<AtomicU32>, u32)> = OnceLock::new();

pub fn start_workers(engine: &mut Engine) -> HammerResult<()> {
    let (worker_config, n_workers) = resolve_worker_startup(&engine.registry)?;
    let wait = Arc::new(AtomicU32::new(0));
    let workers = Arc::new(AtomicU32::new(0));

    let _ = WORKER_BARRIER_ARCS.set((Arc::clone(&wait), Arc::clone(&workers), n_workers));

    engine.wait_at_barrier = Arc::clone(&wait);
    engine.workers_at_barrier = Arc::clone(&workers);

    for idx in 1..=n_workers {
        let worker_seed = engine.worker_seed();
        let worker_config = worker_config.clone();

        thread::Builder::new()
            .name(format!("hammer-worker-{idx}"))
            .spawn(move || {
                let worker_index = usize::try_from(idx - 1).expect("worker index fits usize");
                crate::worker_thread::apply_worker_thread_setup(&worker_config, worker_index);
                let worker_numa_node = crate::numa::current_numa_node().unwrap_or(0);
                let engine = worker_seed.spawn_on_numa(idx, worker_numa_node);
                spawn::set_data_plane_runtime(engine.runtime.clone());

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

fn resolve_worker_startup(registry: &Arc<RuntimeRegistry>) -> HammerResult<(Worker, u32)> {
    let worker_config = registry
        .get::<hammer_core::config::Config>()
        .map(|config| config.worker.clone())
        .unwrap_or_else(hammer_core::config::Worker::default);
    let worker_count = u32::try_from(worker_config.count).map_err(|_| {
        HammerError::internal(format!(
            "worker.count does not fit u32: {}",
            worker_config.count
        ))
    })?;
    Ok((worker_config, worker_count))
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

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::config::Config;
    use hammer_core::registry::RuntimeRegistry;
    use std::sync::Arc;

    #[test]
    fn resolve_worker_startup_uses_registry_config_and_defaults() {
        let registry = Arc::new(RuntimeRegistry::new());

        let (default_worker, default_count) =
            resolve_worker_startup(&registry).expect("default worker startup");
        assert_eq!(default_worker, Worker::default());
        assert_eq!(default_count, Worker::default().count as u32);

        let mut config = Config::default();
        config.worker.count = 7;
        registry.set(Arc::new(config));

        let (configured_worker, configured_count) =
            resolve_worker_startup(&registry).expect("configured worker startup");
        assert_eq!(configured_worker.count, 7);
        assert_eq!(configured_count, 7);
    }
}
