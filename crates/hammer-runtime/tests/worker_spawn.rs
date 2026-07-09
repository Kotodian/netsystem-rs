use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

use hammer_adapter::{DataPlaneRuntime, DataPlaneRuntimeConfig};
use hammer_core::data_plane::DataPlaneBufferConfig;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::barrier;
use hammer_runtime::engine::Engine;
use hammer_runtime::spawn::DataRemoteLocalQueue;

fn test_runtime(thread_index: u32) -> DataPlaneRuntime {
    let buffers = DataPlaneBufferConfig {
        buffer_slot_capacity: 64,
        buffer_slots: 4,
        frame_capacity: 16,
        frame_slots: 4,
        thread_index,
        ..DataPlaneBufferConfig::default()
    };
    DataPlaneRuntime::new(DataPlaneRuntimeConfig { buffers })
}

#[test]
fn worker_spawn_engine_main_loop_exits() {
    let n_workers = 2u32;
    let mut handles = Vec::new();

    for idx in 0..n_workers {
        let handle = thread::spawn(move || {
            let rt = test_runtime(idx);
            hammer_runtime::spawn::set_data_plane_runtime(rt.clone());
            let mut engine = Engine::new(rt, RuntimeRegistry::new());
            engine.thread_index = idx;

            let remote_local = DataRemoteLocalQueue::default();
            remote_local.attach_current_thread();

            let tokio_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");

            engine.main_loop_exit_now.store(true, Ordering::Relaxed);
            let status =
                hammer_runtime::main_loop::engine_main_loop(&engine, &tokio_rt, &remote_local);
            assert_eq!(0, status, "worker {idx} exit status");
        });
        handles.push(handle);
    }

    for (idx, h) in handles.into_iter().enumerate() {
        h.join().unwrap_or_else(|_| panic!("worker {idx} panicked"));
    }
}

#[test]
fn worker_spawn_barrier_sync() {
    let n_workers = 2u32;
    let wait = Arc::new(AtomicU32::new(0));
    let workers_at_barrier = Arc::new(AtomicU32::new(0));
    let exit = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..n_workers)
        .map(|_| {
            let w = Arc::clone(&wait);
            let wk = Arc::clone(&workers_at_barrier);
            let e = Arc::clone(&exit);
            thread::spawn(move || {
                while !e.load(Ordering::Acquire) {
                    barrier::barrier_check(&w, &wk);
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(50));

    {
        let _guard = barrier::barrier_sync(&wait, &workers_at_barrier, n_workers);
        // All workers are parked at the barrier while the guard is held.
    }

    exit.store(true, Ordering::Release);

    for h in handles {
        h.join().unwrap();
    }
}
