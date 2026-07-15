use core::hint::spin_loop;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::thread::JoinHandle;

use hammer_core::config::Worker;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::registry::RuntimeRegistry;

use crate::engine::Engine;
use crate::spawn;
use crate::{DataPlaneHandoff, DataWorkerId, barrier};

#[hammer_component_macros::main_loop_enter_function]
pub fn start_workers(engine: &mut Engine) -> HammerResult<()> {
    let (worker_config, worker_count) = resolve_worker_startup(&engine.registry)?;
    let wait = Arc::new(AtomicU32::new(1));
    let workers = Arc::new(AtomicU32::new(0));
    let ready = Arc::new(AtomicU32::new(0));
    engine.wait_at_barrier = Arc::clone(&wait);
    engine.workers_at_barrier = Arc::clone(&workers);
    engine.main_loop_exit_now.store(false, Ordering::Release);

    let handoff = DataPlaneHandoff::new(worker_config.count, worker_config.handoff.queue_capacity);
    let mut startup = Vec::with_capacity(worker_config.count);
    for _ in 0..worker_config.count {
        startup.push(OnceLock::new());
    }
    let startup = Arc::new(startup);
    let mut threads = Vec::with_capacity(worker_config.count);

    for thread_index in 1..=worker_count {
        let worker_seed = engine.worker_seed();
        let worker_config = worker_config.clone();
        let worker = DataWorkerId::new(thread_index - 1);
        let handoff = handoff.worker(worker);
        let worker_startup = Arc::clone(&startup);
        let worker_ready = Arc::clone(&ready);
        let worker_wait = Arc::clone(&wait);
        let workers_at_barrier = Arc::clone(&workers);
        let worker_exit = Arc::clone(&engine.main_loop_exit_now);
        let launched = thread::Builder::new()
            .name(format!("hammer-worker-{thread_index}"))
            .spawn(move || {
                let initialized: Result<_, String> = (|| {
                    let worker_slot = worker.slot();
                    crate::worker_thread::apply_worker_thread_setup(&worker_config, worker_slot);
                    let numa_node = crate::numa::current_numa_node().unwrap_or(0);
                    let mut engine = worker_seed
                        .spawn_on_numa(thread_index, numa_node, handoff)
                        .map_err(|error| error.to_string())?;
                    spawn::set_data_plane_runtime(engine.runtime.clone());
                    spawn::apply_worker_idle_slice(worker_config.idle_slice);
                    crate::init::run_worker_init_functions(&mut engine)
                        .map_err(|error| error.to_string())?;
                    let tokio = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| format!("build per-worker tokio runtime: {error}"))?;
                    Ok((engine, tokio))
                })();

                let running = match initialized {
                    Ok((engine, tokio)) => {
                        let published = worker_startup[worker.slot()].set(Ok(()));
                        debug_assert!(published.is_ok());
                        Some((engine, tokio))
                    }
                    Err(error) => {
                        let published = worker_startup[worker.slot()].set(Err(error));
                        debug_assert!(published.is_ok());
                        None
                    }
                };

                worker_ready.fetch_add(1, Ordering::Release);
                barrier::barrier_check(&worker_wait, &workers_at_barrier);

                let Some((mut engine, tokio)) = running else {
                    spawn::cleanup_thread_local();
                    return;
                };
                if worker_exit.load(Ordering::Acquire) {
                    spawn::cleanup_thread_local();
                    return;
                }

                let remote_local = spawn::DataRemoteLocalQueue::default();
                remote_local.attach_current_thread();
                let exit_status =
                    crate::main_loop::engine_main_loop(&mut engine, &tokio, &remote_local);
                spawn::cleanup_thread_local();
                tracing::debug!(worker = thread_index, exit_status, "worker exited");
            });

        match launched {
            Ok(thread) => threads.push(thread),
            Err(error) => {
                abort_workers(&wait, &workers, &engine.main_loop_exit_now, threads);
                return Err(HammerError::internal(format!(
                    "failed to spawn worker {thread_index}: {error}"
                )));
            }
        }
    }

    while ready.load(Ordering::Acquire) != worker_count {
        if threads.iter().any(JoinHandle::is_finished) {
            abort_workers(&wait, &workers, &engine.main_loop_exit_now, threads);
            return Err(HammerError::internal(
                "worker exited before publishing startup state",
            ));
        }
        spin_loop();
    }
    while workers.load(Ordering::Acquire) != worker_count {
        if threads.iter().any(JoinHandle::is_finished) {
            abort_workers(&wait, &workers, &engine.main_loop_exit_now, threads);
            return Err(HammerError::internal(
                "worker exited before reaching the initial barrier",
            ));
        }
        spin_loop();
    }

    if let Err(error) = validate_worker_topologies(&startup) {
        abort_workers(&wait, &workers, &engine.main_loop_exit_now, threads);
        return Err(error);
    }
    if let Err(error) = engine.retain_worker_threads(&mut threads) {
        abort_workers(&wait, &workers, &engine.main_loop_exit_now, threads);
        return Err(error);
    }
    barrier::barrier_release(&wait, &workers);
    Ok(())
}

fn resolve_worker_startup(registry: &Arc<RuntimeRegistry>) -> HammerResult<(Worker, u32)> {
    let worker = registry
        .get::<hammer_core::config::Config>()
        .map(|config| config.worker.clone())
        .unwrap_or_default();
    worker.validate()?;
    let count = u32::try_from(worker.count).map_err(|_| {
        HammerError::internal(format!("worker.count does not fit u32: {}", worker.count))
    })?;
    Ok((worker, count))
}

fn validate_worker_topologies<T>(startup: &[OnceLock<Result<T, String>>]) -> HammerResult<()>
where
    T: PartialEq,
{
    let expected = match startup.first().and_then(OnceLock::get) {
        Some(Ok(topology)) => topology,
        Some(Err(error)) => {
            return Err(HammerError::internal(format!(
                "worker 0 initialization failed: {error}"
            )));
        }
        None => return Err(HammerError::internal("worker 0 startup state is missing")),
    };

    for (worker, state) in startup.iter().enumerate().skip(1) {
        match state.get() {
            Some(Ok(topology)) if topology == expected => {}
            Some(Ok(_)) => {
                return Err(HammerError::internal(format!(
                    "worker graph topology mismatch between worker 0 and worker {worker}"
                )));
            }
            Some(Err(error)) => {
                return Err(HammerError::internal(format!(
                    "worker {worker} initialization failed: {error}"
                )));
            }
            None => {
                return Err(HammerError::internal(format!(
                    "worker {worker} startup state is missing"
                )));
            }
        }
    }
    Ok(())
}

fn abort_workers(
    wait: &AtomicU32,
    workers: &AtomicU32,
    exit: &AtomicBool,
    threads: Vec<JoinHandle<()>>,
) {
    exit.store(true, Ordering::Release);
    barrier::barrier_release(wait, workers);
    for thread in threads {
        let _ = thread.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::config::Config;
    use hammer_core::registry::RuntimeRegistry;

    #[test]
    fn resolve_worker_startup_uses_registry_config_and_defaults() {
        let registry = RuntimeRegistry::new();

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

    #[test]
    fn resolve_worker_startup_carries_poll_sleep_idle_slice() {
        let registry = RuntimeRegistry::new();
        let mut config = Config::default();
        config.worker.idle_slice = std::time::Duration::from_millis(50);
        config.worker.count = 1;
        registry.set(Arc::new(config));

        let (worker, count) = resolve_worker_startup(&registry).expect("worker startup");
        assert_eq!(count, 1);
        assert_eq!(worker.idle_slice, std::time::Duration::from_millis(50));

        spawn::apply_worker_idle_slice(worker.idle_slice);
        assert_eq!(
            spawn::current_worker_idle_slice(),
            std::time::Duration::from_millis(50)
        );
    }
}
