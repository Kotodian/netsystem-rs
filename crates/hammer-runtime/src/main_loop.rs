use crate::DataPlaneMain;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::Ordering;
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
    PENDING_MAIN_THREAD_FUTURES.lock().push(Box::pin(future));
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
    let waker =
        MAIN_THREAD_FUTURE_WAKER.get_or_init(|| Waker::from(Arc::new(MainThreadFutureWake)));
    let mut context = Context::from_waker(waker);
    crate::worker_thread_barrier_sync!({
        processing.retain_mut(|future| matches!(future.as_mut().poll(&mut context), Poll::Pending));
    });
    if !processing.is_empty() {
        PENDING_MAIN_THREAD_FUTURES.lock().append(processing);
    }
}

impl DataPlaneMain {
    #[cfg(target_os = "linux")]
    pub fn run_main_until<F>(main: &Rc<RefCell<Self>>, future: F) -> crate::RuntimeResult<F::Output>
    where
        F: Future,
    {
        crate::ensure_main_thread()?;
        let runtime = {
            let mut owner = main.borrow_mut();
            if owner.thread_index() != 0 || owner.nodes.process_runtime.is_none() {
                return Err(crate::RuntimeError::MainProcessRuntimeUnavailable);
            }
            let runtime = owner
                .nodes
                .process_runtime
                .take()
                .expect("validated thread-zero runtime remains installed");
            runtime
        };
        let files = crate::file::AsyncFileMain::global();
        let local = tokio::task::LocalSet::new();
        let output = runtime.block_on(local.run_until(async {
            tokio::pin!(future);
            let mut processing = Vec::new();
            let mut queue_signal_interval = tokio::time::interval(Duration::from_micros(400));
            queue_signal_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            queue_signal_interval.tick().await;
            loop {
                // VPP main.c:1497-1512: main-thread RPCs run once before File.
                poll_main_thread_futures(&mut processing);
                // VPP main.c:1585-1586 checks the API queues from the main
                // loop, then signals the API Process if work is pending.
                {
                    let mut owner = main.borrow_mut();
                    if let Some(callback) = owner.queue_signal_callback {
                        callback(&mut owner)?;
                    }
                    owner.nodes.restore_processes(Instant::now())?;
                }
                let queue_signal_enabled = main.borrow().queue_signal_callback.is_some();
                let selected: crate::RuntimeResult<Option<F::Output>> = tokio::select! {
                    biased;
                    _ = MAIN_THREAD_FUTURES_READY.notified() => Ok(None),
                    _ = queue_signal_interval.tick(), if queue_signal_enabled => Ok(None),
                    output = &mut future => Ok(Some(output)),
                    readiness = files.next_ready() => {
                        readiness?;
                        let mut owner = main.borrow_mut();
                        files.dispatch_ready(&mut owner.nodes);
                        Ok(None)
                    },
                };
                if let Some(output) = selected? {
                    break Ok(output);
                }
            }
        }));
        assert!(
            main.borrow_mut()
                .nodes
                .process_runtime
                .replace(runtime)
                .is_none(),
            "thread-zero runtime has one NodeMain owner"
        );
        output
    }

    #[cfg(not(target_os = "linux"))]
    pub fn run_main_until<F>(_: &Rc<RefCell<Self>>, _: F) -> crate::RuntimeResult<F::Output>
    where
        F: Future,
    {
        Err(crate::RuntimeError::FilePollerOperationUnsupported {
            operation: "thread-zero async File backend",
        })
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
    main: &Rc<RefCell<DataPlaneMain>>,
    future: F,
) -> crate::RuntimeResult<T>
where
    F: Future<Output = crate::RuntimeResult<T>>,
{
    crate::ensure_main_thread()?;
    assert_eq!(
        main.borrow().thread_index(),
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
        crate::init::run_init_functions(global, &mut main.borrow_mut())?;
        // Service node initializers may publish registrations through their
        // owner Mains (for example NetMain's DPO roots). Materialize the
        // graph only after those owners have completed normal init/config.
        main.borrow_mut().init_graph_from_declarations(
            global.node_registrations.iter().copied(),
            global.node_function_registrations.iter().copied(),
        )?;
        main.borrow_mut().start_main_loop_trace_clock();
        crate::init::run_main_loop_enter(global, &mut main.borrow_mut())?;

        // VPP applies post-worker configuration while the worker barrier is
        // held. Interface/FIB/DPO publication and builtin application setup
        // must observe the same control-plane ownership window. The enter
        // phase installs the barrier before this non-early configuration runs.
        crate::worker_thread_barrier_sync!({
            crate::init::run_config_functions(
                global,
                Some(&mut main.borrow_mut()),
                false,
                global.startup_config(),
            )
        })?;

        crate::WorkerThread::main()
            .expect("workers are installed before Process startup")
            .initial_barrier_sync_and_release();

        main.borrow_mut()
            .start_processes(global.node_registrations.iter().copied())?;
        DataPlaneMain::run_main_until(main, future)?
    })();

    let finish = || {
        let process_result = main.borrow_mut().stop_processes();
        let exit_result = crate::init::run_main_loop_exit(global, &mut main.borrow_mut());

        let output = match run_result {
            Ok(output) => output,
            Err(primary) => {
                if let Err(cleanup) = process_result {
                    tracing::error!(%cleanup, "Process cleanup failed after main-loop error");
                }
                if let Err(cleanup) = exit_result {
                    tracing::error!(%cleanup, "main-loop exit failed after main-loop error");
                }
                return Err(primary);
            }
        };
        match (process_result, exit_result) {
            (Err(primary), Err(cleanup)) => {
                tracing::error!(%cleanup, "main-loop exit failed after Process cleanup error");
                Err(primary)
            }
            (Err(primary), _) => Err(primary),
            (_, Err(cleanup)) => Err(cleanup),
            (Ok(()), Ok(())) => Ok(output),
        }
    };
    if let Some(thread) = crate::WorkerThread::main()
        && !thread.startup_cancelled()
    {
        thread.sync(std::panic::Location::caller());
    }
    finish()
}

/// VPP-style fixed-schedule data-plane main loop.
///
/// Step order mirrors VPP `main.c:1442-1693`:
/// 1. Barrier check (workers_at_barrier / wait_at_barrier)
/// 2. Drain handoff, then poll worker-local File readiness when due
/// 3. Run ready nodes
/// 4. Schedule polling-state driver nodes (periodically)
/// 5. Run ready nodes (handles interrupt frames + newly-scheduled polling frames)
/// 6. Expire the DataPlane Main timing wheel
/// 7. Increment the thread's main-loop counter and check exit
pub fn data_plane_main_loop(worker: &crate::WorkerThread) -> i32 {
    let file_poll = crate::config::worker::file_poll();
    let barrier = crate::WorkerThread::main().expect("worker barrier installs before dispatch");
    let files = crate::file::FILE_MAIN
        .get()
        .expect("FileMain initializes before Data Worker dispatch");
    let mut file_poll_skip_loops = 0u32;
    let mut last_barrier_release = Instant::now();
    let handoff_queues_dequeue = crate::handoff::select_handoff_queues_dequeue();

    loop {
        // The preceding iteration's mutable main borrow has ended before
        // this VPP worker-barrier acknowledgement.
        let barrier_waited = barrier.is_pending();
        let refork_required = barrier.check();
        if barrier_waited {
            last_barrier_release = Instant::now();
        }
        // SAFETY: the barrier check has returned and only this Worker borrows
        // its main until the end of this loop iteration.
        let main = unsafe { crate::ThreadMain::global().worker_main_on_worker(worker) };
        if refork_required {
            if barrier.refork(main).is_err() {
                return 1;
            }
        }

        let mut progress = false;
        // VPP vlib/main.h:426-431 resets this worker's maximum each round.
        main.max_internal_frame_vectors = 0;

        // VPP `main.c:1519`: one timestamp per iteration; every dispatch in
        // this iteration measures from it or from the previous dispatch's end.
        main.last_time_stamp = hammer_infra::time::cpu_time_now();

        if main.handoff_queue_pending_bmp.load(Ordering::Relaxed) != 0 {
            unsafe { handoff_queues_dequeue(main) };
            progress = true;
        }
        match main.schedule_worker_node_interrupts() {
            Ok(scheduled) => progress |= scheduled != 0,
            Err(_) => return 1,
        }

        // VPP `vlib_file_poll`: busy rounds skip sleep but still poll File
        // nonblocking; only an idle adaptive worker may wait.
        let skip_sleep = if file_poll_skip_loops != 0 {
            file_poll_skip_loops -= 1;
            true
        } else {
            let busy = worker.last_vectors_per_main_loop().load(Ordering::Relaxed) >= 2
                || main.nodes.has_polling_input_nodes()
                || last_barrier_release.elapsed() < Duration::from_millis(500);
            if busy {
                file_poll_skip_loops = 1024;
            }
            busy
        };
        if skip_sleep {
            if main.poll_file_readiness().is_err() {
                return 1;
            }
        } else {
            let no_sleep = main.file_poll_no_sleep_epolls != 0;
            if no_sleep {
                main.file_poll_no_sleep_epolls -= 1;
            }
            let may_wait = matches!(file_poll, crate::file::WorkerFilePollMode::Adaptive)
                && !progress
                && !no_sleep
                && !barrier.is_pending()
                && !worker.has_node_interrupts()
                && !main.nodes.has_pending_work();
            match main.poll_file_readiness() {
                Ok(dispatched) => progress |= dispatched != 0,
                Err(_) => return 1,
            }
            if may_wait && !progress {
                worker.advertise_runtime_sleep();
            }
            if may_wait
                && !progress
                && main.handoff_queue_pending_bmp.load(Ordering::SeqCst) == 0
                && !barrier.is_pending()
                && !worker.has_node_interrupts()
                && !main.nodes.has_pending_work()
            {
                let pending = files.has_pending_for_worker(main.thread_index());
                match pending {
                    Ok(false) => {
                        let waited = files.wait_for_worker(main);
                        worker.clear_runtime_sleep();
                        if waited.is_err() {
                            return 1;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => {
                        worker.clear_runtime_sleep();
                        return 1;
                    }
                }
            }
            if may_wait {
                worker.clear_runtime_sleep();
            }
        }
        if main.schedule_polling_pre_input_nodes().is_err() {
            return 1;
        }
        if main.schedule_interrupt_pre_input_nodes().is_err() {
            return 1;
        }
        if main.schedule_polling_driver_nodes().is_err() {
            return 1;
        }
        if main.schedule_interrupt_driver_nodes().is_err() {
            return 1;
        }

        // Step 3: Run destination Frames enqueued before File polling.
        if main.run_ready_nodes().is_err() {
            return 1;
        }

        // Step 4: Run any newly-scheduled frames (pre-input + input)
        if main.run_ready_nodes().is_err() {
            return 1;
        }

        // Step 5: Expire per-thread Node timers. Their scheduled frames run
        // at the next graph dispatch point, as in VPP's timing-wheel pass.
        main.advance_node_timers();
        // Increment loop count.
        worker
            .last_vectors_per_main_loop()
            .store(main.max_internal_frame_vectors(), Ordering::Relaxed);
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
