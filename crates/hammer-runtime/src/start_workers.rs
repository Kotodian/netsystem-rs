use core::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

use crate::error::{RuntimeError, RuntimeResult};

use crate::config::Worker;
use crate::engine::Engine;
use crate::spawn;
use crate::{DataPlaneHandoff, DataWorkerId, barrier};

#[hammer_component_macros::main_loop_enter_function]
pub fn start_workers(engine: &mut Engine) -> RuntimeResult<()> {
    let (worker_config, worker_count) = resolve_worker_startup(engine)?;
    let wait = Arc::new(AtomicU32::new(1));
    let workers = Arc::new(AtomicU32::new(0));
    engine.wait_at_barrier = Arc::clone(&wait);
    engine.workers_at_barrier = Arc::clone(&workers);
    engine.main_loop_exit_now.store(false, Ordering::Release);
    engine.prepare_worker_runtime_stats(worker_config.count);

    let handoff = DataPlaneHandoff::new(worker_config.count, worker_config.handoff.queue_capacity);
    let mut threads = Vec::with_capacity(worker_config.count);

    for worker_slot in 0..worker_count {
        let worker = DataWorkerId::new(worker_slot);
        let thread_index = worker_slot + 1;
        let worker_seed = engine.worker_seed();
        let worker_config = worker_config.clone();
        let handoff = handoff.worker(worker);
        let worker_wait = Arc::clone(&wait);
        let workers_at_barrier = Arc::clone(&workers);
        let worker_exit = Arc::clone(&engine.main_loop_exit_now);
        let launched = thread::Builder::new()
            .name(format!("hammer-worker-{thread_index}"))
            .stack_size(worker_config.stack_size)
            .spawn(move || -> RuntimeResult<()> {
                // VPP workers stop at the launch barrier before constructing
                // any thread-local runtime state.
                barrier::barrier_check(&worker_wait, &workers_at_barrier);
                if worker_exit.load(Ordering::Acquire) {
                    return Ok(());
                }

                let numa_node = worker_config.apply_current_thread_setup(worker.slot())?;
                let mut engine = worker_seed.spawn_on_numa(thread_index, numa_node, handoff)?;
                spawn::set_data_plane_runtime(engine.runtime.clone());
                spawn::apply_worker_idle_slice(worker_config.idle_slice);
                let result = (|| {
                    crate::init::run_worker_init_functions(&mut engine)?;
                    if worker_exit.load(Ordering::Acquire) {
                        return Ok(());
                    }

                    let tokio = tokio::runtime::Builder::new_current_thread()
                        .max_blocking_threads(worker_config.max_blocking_threads)
                        .enable_all()
                        .build()
                        .map_err(|error| {
                            RuntimeError::lifecycle(
                                format!("build data worker {thread_index} runtime"),
                                error.to_string(),
                            )
                        })?;
                    let remote_local = spawn::DataRemoteLocalQueue::default();
                    remote_local.attach_current_thread();
                    let exit_status =
                        crate::main_loop::engine_main_loop(&mut engine, &tokio, &remote_local);
                    tracing::debug!(worker = thread_index, exit_status, "worker exited");
                    if exit_status == 0 {
                        Ok(())
                    } else {
                        Err(RuntimeError::lifecycle(
                            format!("data worker {thread_index} main loop"),
                            format!("exited with status {exit_status}"),
                        ))
                    }
                })();
                spawn::cleanup_thread_local();
                result
            });

        match launched {
            Ok(thread) => threads.push(thread),
            Err(error) => {
                let fallback = RuntimeError::lifecycle(
                    format!("spawn data worker {thread_index}"),
                    error.to_string(),
                );
                return Err(abort_workers(
                    &wait,
                    &workers,
                    &engine.main_loop_exit_now,
                    threads,
                    fallback,
                ));
            }
        }
    }

    if !wait_for_workers_at_barrier(
        &workers,
        worker_count,
        &threads,
        "worker launch barrier sync",
    ) {
        return Err(abort_workers(
            &wait,
            &workers,
            &engine.main_loop_exit_now,
            threads,
            RuntimeError::invariant("worker exited before reaching the launch barrier"),
        ));
    }

    // Release the launch barrier, then immediately arm VPP's second initial
    // barrier. A worker can acknowledge this one only from its main-loop entry,
    // after worker-local initialization has completed.
    barrier::barrier_release(&wait, &workers);
    arm_barrier(&wait);
    if !wait_for_workers_at_barrier(
        &workers,
        worker_count,
        &threads,
        "worker main-loop barrier sync",
    ) {
        return Err(abort_workers(
            &wait,
            &workers,
            &engine.main_loop_exit_now,
            threads,
            RuntimeError::invariant("worker exited before reaching the main-loop barrier"),
        ));
    }
    if engine.main_loop_exit_now.load(Ordering::Acquire) {
        return Err(abort_workers(
            &wait,
            &workers,
            &engine.main_loop_exit_now,
            threads,
            RuntimeError::invariant("worker requested exit during initialization"),
        ));
    }
    if let Err(error) = engine.retain_worker_threads(&mut threads) {
        return Err(abort_workers(
            &wait,
            &workers,
            &engine.main_loop_exit_now,
            threads,
            error,
        ));
    }
    barrier::barrier_release(&wait, &workers);
    Ok(())
}

fn resolve_worker_startup(engine: &Engine) -> RuntimeResult<(Worker, u32)> {
    let worker = engine.worker_config().clone();
    worker.validate()?;
    let count = u32::try_from(worker.count).map_err(|_| RuntimeError::WorkerCountOverflow {
        count: worker.count,
    })?;
    Ok((worker, count))
}

fn arm_barrier(wait: &AtomicU32) {
    wait.store(1, Ordering::Release);
}

#[track_caller]
fn wait_for_workers_at_barrier(
    workers: &AtomicU32,
    worker_count: u32,
    threads: &[JoinHandle<RuntimeResult<()>>],
    phase: &'static str,
) -> bool {
    let deadline = Instant::now() + barrier::BARRIER_SYNC_TIMEOUT;
    loop {
        let observed = workers.load(Ordering::Acquire);
        if observed == worker_count {
            return true;
        }
        if threads.iter().any(JoinHandle::is_finished) {
            return false;
        }
        if Instant::now() > deadline {
            barrier::barrier_deadlock(phase, worker_count, observed);
        }
        spin_loop();
    }
}

fn abort_workers(
    wait: &AtomicU32,
    workers: &AtomicU32,
    exit: &AtomicBool,
    threads: Vec<JoinHandle<RuntimeResult<()>>>,
    fallback: RuntimeError,
) -> RuntimeError {
    exit.store(true, Ordering::Release);
    barrier::barrier_release(wait, workers);
    let mut first_failure = None;
    for (worker, thread) in threads.into_iter().enumerate() {
        match thread.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) if first_failure.is_none() => first_failure = Some(error),
            Ok(Err(error)) => {
                tracing::error!(worker, %error, "data worker failed while startup aborted");
            }
            Err(payload) => {
                let panic = crate::engine::thread_panic_message(payload);
                if first_failure.is_none() {
                    first_failure = Some(RuntimeError::invariant(format!(
                        "data worker {worker} panicked: {panic}"
                    )));
                } else {
                    tracing::error!(worker, %panic, "data worker panicked while startup aborted");
                }
            }
        }
    }
    first_failure.unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataPlaneRuntime, DataPlaneRuntimeConfig, RuntimeRegistry};

    fn test_engine() -> Engine {
        Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        )
    }

    #[test]
    fn resolve_worker_startup_uses_engine_worker_config_and_defaults() {
        let mut engine = test_engine();

        let (default_worker, default_count) =
            resolve_worker_startup(&engine).expect("default worker startup");
        assert_eq!(default_worker, Worker::default());
        assert_eq!(default_count, Worker::default().count as u32);

        let mut worker = Worker::default();
        worker.count = 7;
        engine
            .apply_worker_config(worker)
            .expect("apply worker config");

        let (configured_worker, configured_count) =
            resolve_worker_startup(&engine).expect("configured worker startup");
        assert_eq!(configured_worker.count, 7);
        assert_eq!(configured_count, 7);
    }

    #[test]
    fn resolve_worker_startup_carries_poll_sleep_idle_slice() {
        let mut engine = test_engine();
        let mut worker = Worker::default();
        worker.idle_slice = std::time::Duration::from_millis(50);
        worker.count = 1;
        engine
            .apply_worker_config(worker)
            .expect("apply worker config");

        let (worker, count) = resolve_worker_startup(&engine).expect("worker startup");
        assert_eq!(count, 1);
        assert_eq!(worker.idle_slice, std::time::Duration::from_millis(50));

        spawn::apply_worker_idle_slice(worker.idle_slice);
        assert_eq!(
            spawn::current_worker_idle_slice(),
            std::time::Duration::from_millis(50)
        );
    }
}
