use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use hammer_runtime::DataPlaneBufferConfig;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::engine::Engine;
use hammer_runtime::spawn::{DataRemoteLocalQueue, DataRuntime};
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};

fn test_runtime(thread_index: u32) -> DataPlaneRuntime {
    let buffers = DataPlaneBufferConfig {
        buffer_slot_capacity: 64,
        buffer_slots: 4,
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
            let main = Engine::new(rt, RuntimeRegistry::new());
            let mut engine = main.spawn(idx + 1).expect("spawn data worker");
            hammer_runtime::spawn::set_data_plane_runtime(engine.runtime.clone());

            let remote_local = DataRemoteLocalQueue::default();
            remote_local.attach_current_thread();

            let tokio_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");

            engine.main_loop_exit_now.store(true, Ordering::Relaxed);
            let status =
                hammer_runtime::main_loop::engine_main_loop(&mut engine, &tokio_rt, &remote_local);
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
    let runtime =
        DataRuntime::new(2, "barrier-worker", 2 * 1024 * 1024, 1).expect("spawn data runtime");
    let barrier = runtime.barrier();
    let mut value = 1;
    barrier.sync(&mut value, |value| {
        // All workers are parked at the barrier while the guard is held.
        *value = 2;
    });
    assert_eq!(value, 2);
    runtime.shutdown_timeout(Duration::from_secs(1));
}
