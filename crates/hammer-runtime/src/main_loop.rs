use crate::DataPlaneMain;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use hammer_infra::sync::SpinLock;
use tokio::sync::Notify;

type MainThreadFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

static PENDING_MAIN_THREAD_FUTURES: SpinLock<Vec<MainThreadFuture>> = SpinLock::new(Vec::new());
static MAIN_THREAD_FUTURES_READY: Notify = Notify::const_new();
static MAIN_THREAD_FUTURE_WAKER: OnceLock<Waker> = OnceLock::new();

struct MainThreadFutureWake;

impl Wake for MainThreadFutureWake {
    fn wake(self: Arc<Self>) {
        MAIN_THREAD_FUTURES_READY.notify_one();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        MAIN_THREAD_FUTURES_READY.notify_one();
    }
}

/// Queues control-plane work for the next thread-zero main-loop dispatch.
/// VPP: `vlib_rpc_call_main_thread_inline`, threads.c:1663-1693.
#[inline]
pub fn enqueue_main_thread_future<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    PENDING_MAIN_THREAD_FUTURES
        .lock()
        .push(Box::pin(future));
    MAIN_THREAD_FUTURES_READY.notify_one();
}

/// VPP: `vlib_rpc_call_main_thread_process`, threads.c:1709-1737. Swap the
/// pending batch under the lock, then poll it once under one WorkerBarrier.
/// A suspended future returns to pending; its waker requests a later pass.
fn poll_main_thread_futures(processing: &mut Vec<MainThreadFuture>) {
    {
        let mut pending = PENDING_MAIN_THREAD_FUTURES.lock();
        std::mem::swap(&mut *pending, processing);
    }
    if processing.is_empty() {
        return;
    }
    let waker = MAIN_THREAD_FUTURE_WAKER
        .get_or_init(|| Waker::from(Arc::new(MainThreadFutureWake)));
    let mut context = Context::from_waker(waker);
    crate::worker_thread_barrier_sync!({
        processing.retain_mut(|future| {
            matches!(future.as_mut().poll(&mut context), Poll::Pending)
        });
    });
    if !processing.is_empty() {
        PENDING_MAIN_THREAD_FUTURES.lock().append(processing);
    }
}

impl DataPlaneMain {
    pub fn run_main_until<F>(&mut self, future: F) -> crate::RuntimeResult<F::Output>
    where
        F: Future,
    {
        crate::ensure_main_thread()?;
        if self.thread_index() != 0 || self.nodes.process_runtime.is_none() {
            return Err(crate::RuntimeError::MainProcessRuntimeUnavailable);
        }
        let runtime = self
            .nodes
            .process_runtime
            .take()
            .expect("validated thread-zero runtime remains installed");
        let output = runtime.block_on(async {
            tokio::pin!(future);
            let mut processing = Vec::new();
            loop {
                // VPP main.c:1497-1512: main-thread RPCs run once before File.
                poll_main_thread_futures(&mut processing);
                self.nodes.restore_processes(Instant::now())?;
                tokio::select! {
                    _ = MAIN_THREAD_FUTURES_READY.notified() => {}
                    output = &mut future => break Ok(output),
                    readiness = self.next_file_readiness() => {
                        readiness?;
                    }
                }
            }
        });
        assert!(
            self.nodes.process_runtime.replace(runtime).is_none(),
            "thread-zero runtime has one NodeMain owner"
        );
        output
    }
}

/// Runs the thread-zero graph lifecycle and Tokio executor.
///
/// VPP correspondence: `vlib_main` dispatches normal init/config, materializes
/// the graph after owner state is published, enters the main-loop callbacks,
/// performs the later initial worker barrier, starts Process Nodes, and keeps
/// the final worker barrier held while exit callbacks run.
pub fn run<F, T>(
    global: crate::GlobalMain,
    threads: crate::ThreadMain,
    plugins: crate::PluginMain,
    main: &mut DataPlaneMain,
    future: F,
) -> crate::RuntimeResult<T>
where
    F: Future<Output = crate::RuntimeResult<T>>,
{
    crate::ensure_main_thread()?;
    assert_eq!(
        main.thread_index(),
        0,
        "thread-zero lifecycle requires main index 0"
    );

    assert!(
        crate::global_main::GLOBAL_MAIN.set(global).is_ok(),
        "GlobalMain initializes once"
    );
    assert!(
        crate::thread_main::THREAD_MAIN.set(threads).is_ok(),
        "ThreadMain initializes once"
    );
    assert!(
        crate::plugin::PLUGIN_MAIN.set(plugins).is_ok(),
        "PluginMain initializes once"
    );
    let global = crate::GlobalMain::global();

    let run_result = (|| -> crate::RuntimeResult<T> {
        crate::init::run_init_functions(global, main)?;
        crate::init::run_config_functions(global, Some(main), false, global.startup_config())?;
        // Service node initializers may publish registrations through their
        // owner Mains (for example NetMain's DPO roots). Materialize the
        // graph only after those owners have completed normal init/config.
        main.init_graph_from_declarations(
            global.node_registrations.iter().copied(),
            global.node_function_registrations.iter().copied(),
        )?;
        crate::init::run_main_loop_enter(global, main)?;

        crate::worker_thread_barrier_sync!(main, {});
        main.start_processes(global.node_registrations.iter().copied())?;
        main.run_main_until(future)?
    })();

    let finish = || {
        let process_result = main.stop_processes();
        let exit_result = crate::init::run_main_loop_exit(global, main);

        let output = run_result?;
        process_result?;
        exit_result?;
        Ok(output)
    };
    match crate::barrier::global() {
        Some(barrier) if !barrier.startup_cancelled() => barrier.final_sync(finish),
        _ => finish(),
    }
}

/// VPP-style fixed-schedule data-plane main loop.
///
/// Step order mirrors VPP `main.c:1442-1693`:
/// 1. Barrier check (workers_at_barrier / wait_at_barrier)
/// 2. Poll worker-local File readiness
/// 3. Drain handoff and run ready nodes
/// 4. Schedule polling-state driver nodes (periodically)
/// 5. Run ready nodes (handles interrupt frames + newly-scheduled polling frames)
/// 6. Dispatch timer nodes (no timer wheel in data-plane yet)
/// 7. Advance timers, increment the thread's main-loop counter and check exit
pub fn data_plane_main_loop(main: &mut DataPlaneMain, idle_slice: Duration) -> i32 {
    main.attach_worker_interrupt_thread();

    loop {
        let mut progress = false;

        // Step 1: Barrier check — VPP threads.c:296
        if let Some(barrier) = crate::barrier::global()
            && barrier.is_pending()
            && barrier.check_for_refork()
        {
            barrier.refork(&mut main.nodes);
        }

        // VPP `main.c:1519`: one timestamp per iteration; every dispatch in
        // this iteration measures from it or from the previous dispatch's end.
        main.last_time_stamp = hammer_infra::time::cpu_time_now();

        // Step 2: Poll worker-local File readiness before graph dispatch.
        match main.poll_file_readiness() {
            Ok(dispatched) => progress |= dispatched != 0,
            Err(error) => {
                tracing::error!(worker = main.thread_index(), %error, "File poll failed");
                return 1;
            }
        }
        if let Ok(scheduled) = main.schedule_remote_interrupts() {
            progress |= scheduled != 0;
        }
        if let Ok(scheduled) = main.schedule_polling_pre_input_nodes() {
            progress |= scheduled != 0;
        }
        if let Ok(scheduled) = main.schedule_interrupt_pre_input_nodes() {
            progress |= scheduled != 0;
        }
        if let Ok(scheduled) = main.schedule_polling_driver_nodes() {
            progress |= scheduled != 0;
        }
        if let Ok(scheduled) = main.schedule_interrupt_driver_nodes() {
            progress |= scheduled != 0;
        }

        // Step 3: Drain handoff queues and run ready nodes.
        let _ = main.run_ready_nodes();

        if !progress {
            std::thread::sleep(idle_slice);
        }

        // Step 4: Run any newly-scheduled frames (pre-input + input)
        let _ = main.run_ready_nodes();

        // Step 5: Dispatch timer nodes (no data-plane timer wheel yet)

        // Step 6: Advance timers — deferred (no data-plane timer wheel yet).
        // VPP dispatches timer-wheel-expired sched nodes here.
        // Increment loop count.
        main.increment_main_loop_count();

        // Step 7: Exit check
        if let Some(status) = requested_exit_status(main) {
            return status;
        }
    }
}

fn requested_exit_status(main: &DataPlaneMain) -> Option<i32> {
    if !main.main_loop_exit_requested() {
        return None;
    }
    Some(main.main_loop_exit_status())
}
