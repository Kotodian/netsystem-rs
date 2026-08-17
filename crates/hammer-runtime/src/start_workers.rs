use core::hint::spin_loop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
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
    let barrier = barrier::WorkerBarrier::new(worker_count);
    barrier.arm();
    engine.barrier = barrier.clone();
    engine.main_loop_exit_now.store(false, Ordering::Release);
    engine.prepare_worker_publication(worker_config.count);

    let handoff = DataPlaneHandoff::with_node_capacity(
        worker_config.count,
        worker_config.handoff.queue_capacity,
        engine.runtime.nodes().node_count(),
    );
    let worker_control_queues: std::sync::Arc<[spawn::DataRemoteLocalQueue]> = (0..worker_count)
        .map(|_| spawn::DataRemoteLocalQueue::new(worker_config.control.queue_capacity))
        .collect::<Vec<_>>()
        .into();
    engine.install_worker_control_queues(std::sync::Arc::clone(&worker_control_queues));
    let mut threads = Vec::with_capacity(worker_config.count);

    for worker_slot in 0..worker_count {
        let worker = DataWorkerId::new(worker_slot);
        let thread_index = worker_slot + 1;
        let worker_seed = engine.worker_seed();
        let worker_config = worker_config.clone();
        let handoff = handoff.worker(worker);
        let remote_local = worker_control_queues[worker.slot()].clone();
        let worker_barrier = barrier.clone();
        let worker_exit = std::sync::Arc::clone(&engine.main_loop_exit_now);
        let launched = thread::Builder::new()
            .name(format!("hammer-worker-{thread_index}"))
            .stack_size(worker_config.stack_size)
            .spawn(move || -> RuntimeResult<()> {
                let result = catch_unwind(AssertUnwindSafe(|| -> RuntimeResult<()> {
                    // VPP workers stop at the launch barrier before constructing
                    // any thread-local runtime state.
                    worker_barrier.check();
                    if worker_exit.load(Ordering::Acquire) {
                        return Ok(());
                    }

                    let numa_node = worker_config.apply_current_thread_setup(worker.slot())?;
                    let mut engine = worker_seed.spawn_on_numa(thread_index, numa_node, handoff)?;
                    engine.install_current();
                    spawn::set_data_plane_runtime(engine.runtime.clone());
                    spawn::apply_worker_idle_slice(worker_config.idle_slice);
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
                    remote_local.attach_current_thread();
                    let exit_status =
                        crate::main_loop::engine_main_loop(&mut engine, &tokio, &remote_local);
                    tracing::debug!(worker = thread_index, exit_status, "worker exited");
                    let loop_result = if exit_status == 0 {
                        Ok(())
                    } else {
                        Err(RuntimeError::lifecycle(
                            format!("data worker {thread_index} main loop"),
                            format!("exited with status {exit_status}"),
                        ))
                    };
                    let exit_result = crate::init::run_worker_exit_functions(&mut engine);
                    match (loop_result, exit_result) {
                        (Ok(()), result) => result,
                        (Err(loop_error), Ok(())) => Err(loop_error),
                        (Err(loop_error), Err(exit_error)) => {
                            tracing::error!(
                                worker = thread_index,
                                %exit_error,
                                "data worker exit callback failed"
                            );
                            Err(loop_error)
                        }
                    }
                }));
                remote_local.close();
                spawn::cleanup_thread_local();
                match result {
                    Ok(result) => result,
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            });

        match launched {
            Ok(thread) => threads.push(thread),
            Err(error) => {
                let startup_error = RuntimeError::lifecycle(
                    format!("spawn data worker {thread_index}"),
                    error.to_string(),
                );
                return Err(abort_workers(
                    &barrier,
                    &engine.main_loop_exit_now,
                    threads,
                    startup_error,
                ));
            }
        }
    }

    if !wait_for_workers_at_barrier(&barrier, &threads, "worker launch barrier sync") {
        return Err(abort_workers(
            &barrier,
            &engine.main_loop_exit_now,
            threads,
            RuntimeError::WorkerExitedBeforeStartupBarrier { phase: "launch" },
        ));
    }

    // Release the launch barrier, then immediately arm VPP's second initial
    // barrier. A worker can acknowledge this one only from its main-loop entry,
    // after worker-local initialization has completed.
    barrier.release();
    barrier.arm();
    if !wait_for_workers_at_barrier(&barrier, &threads, "worker main-loop barrier sync") {
        return Err(abort_workers(
            &barrier,
            &engine.main_loop_exit_now,
            threads,
            RuntimeError::WorkerExitedBeforeStartupBarrier { phase: "main-loop" },
        ));
    }
    if engine.main_loop_exit_now.load(Ordering::Acquire) {
        return Err(abort_workers(
            &barrier,
            &engine.main_loop_exit_now,
            threads,
            RuntimeError::WorkerRequestedExitDuringInitialization,
        ));
    }
    if let Err(error) = engine.retain_worker_threads(&mut threads) {
        return Err(abort_workers(
            &barrier,
            &engine.main_loop_exit_now,
            threads,
            error,
        ));
    }
    barrier.release();
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

#[track_caller]
fn wait_for_workers_at_barrier(
    barrier: &barrier::WorkerBarrier,
    threads: &[JoinHandle<RuntimeResult<()>>],
    phase: &'static str,
) -> bool {
    let deadline = Instant::now() + barrier::BARRIER_SYNC_TIMEOUT;
    loop {
        let observed = barrier.paused_workers();
        if observed == barrier.worker_count() {
            return true;
        }
        if threads.iter().any(JoinHandle::is_finished) {
            return false;
        }
        if Instant::now() > deadline {
            barrier::barrier_deadlock(phase, barrier.worker_count(), observed);
        }
        spin_loop();
    }
}

fn abort_workers(
    barrier: &barrier::WorkerBarrier,
    exit: &AtomicBool,
    threads: Vec<JoinHandle<RuntimeResult<()>>>,
    startup_error: RuntimeError,
) -> RuntimeError {
    exit.store(true, Ordering::Release);
    barrier.release();
    let mut unwind_payload = None;
    for (worker, thread) in threads.into_iter().enumerate() {
        match thread.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(worker, %error, "data worker failed while startup aborted");
            }
            Err(payload) if unwind_payload.is_none() => unwind_payload = Some(payload),
            Err(payload) => tracing::error!(
                worker,
                panic = %crate::engine::thread_panic_message(payload),
                "data worker panicked while startup aborted"
            ),
        }
    }
    if let Some(payload) = unwind_payload {
        std::panic::resume_unwind(payload);
    }
    startup_error
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
