//! Tracing-aware spawn wrapper that targets the data-plane runtime.
//!
//! Every Service owns its own `tracing::Dispatch` (see `Factory::dispatch`),
//! attached on the calling thread before lifecycle work begins. The bare
//! `tokio::spawn` does not propagate the calling thread's dispatcher to the
//! spawned task, so the task would lose its routing once the worker thread
//! polls it. `tracing::instrument::WithSubscriber::with_current_subscriber`
//! captures the dispatcher at spawn time and attaches it to the future, so
//! subsequent polls re-enter that dispatcher regardless of which worker
//! thread runs them.
//!
//! On top of tracing propagation, this module also routes spawns to the
//! current service's **data-plane runtime** so the control-plane runtime
//! stays isolated from business work. The service crate enters a
//! [`DataRuntimeContext`] while it runs lifecycle/control closures, and
//! spawned data tasks carry that context through Tokio task-local storage.
//! Calls made outside a service context — for example from `#[tokio::test]`
//! integration tests that never construct a service — fall back to the
//! ambient runtime via `tokio::spawn`, preserving prior behaviour for tests.
//!
//! Use `crate::spawn::spawn(future)` everywhere we'd otherwise call
//! `tokio::spawn`. Forgetting it does not corrupt routing — it only causes
//! the task's events to be dropped (no global default subscriber is
//! installed) — but it should still be considered a bug.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use hammer_core::config::Worker;
use hammer_core::data_plane::DataPlaneBuffers;
use hammer_runtime::node::NodeRuntimeStatsRow;
use hammer_runtime::{TraceControlHandle, TraceRecordSink};

use crate::data_plane::{RuntimeDataPlaneRuntime, new_worker_runtime};
use crate::worker_thread::apply_worker_thread_setup;
use hammer_core::error::{HammerError, HammerResult};
use hammer_runtime::DataPlaneRuntime;
use tokio::runtime::Handle;
use tokio::task::JoinHandle as TokioJoinHandle;
use tracing::instrument::WithSubscriber;

#[derive(Clone)]
pub struct DataRuntimeContext {
    inner: Arc<DataRuntimeContextInner>,
}

#[derive(Clone)]
pub struct DataPlaneExecutor {
    context: DataRuntimeContext,
}

struct DataRuntimeContextInner {
    id: usize,
    workers: Vec<DataRuntimeContextWorker>,
    next: AtomicUsize,
}

#[derive(Clone)]
struct DataRuntimeContextWorker {
    handle: Handle,
    remote_local: DataRemoteLocalQueue,
}

pub struct DataRuntime {
    context: DataRuntimeContext,
    workers: Vec<DataRuntimeWorker>,
    wait_at_barrier: Arc<AtomicU32>,
    workers_at_barrier: Arc<AtomicU32>,
}

struct DataRuntimeWorker {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    done_rx: std::sync::mpsc::Receiver<()>,
    join: Option<thread::JoinHandle<()>>,
}

type DataLocalTaskFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;
type RemoteDataLocalTask = Box<dyn FnOnce() + Send + 'static>;

static NEXT_DATA_RUNTIME_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);

tokio::task_local! {
    static TASK_DATA_CONTEXT: DataRuntimeContext;
}

thread_local! {
    static THREAD_DATA_CONTEXT: RefCell<Vec<DataRuntimeContext>> = const { RefCell::new(Vec::new()) };
    static CURRENT_DATA_WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
    static DATA_PLANE_RUNTIME: RefCell<Option<RuntimeDataPlaneRuntime>> =
        const { RefCell::new(None) };
    pub(crate) static DATA_WORKER_IDLE_SLICE: Cell<Duration> =
        const { Cell::new(Duration::from_millis(1)) };
    static DATA_LOCAL_TASKS: RefCell<VecDeque<Rc<DataLocalTask>>> =
        const { RefCell::new(VecDeque::new()) };
    pub(crate) static DATA_LOCAL_DRIVER_WAKER: RefCell<Option<Waker>> = const { RefCell::new(None) };
}

pub(crate) fn apply_worker_idle_slice(idle_slice: Duration) {
    DATA_WORKER_IDLE_SLICE.with(|slot| slot.set(idle_slice));
}

pub(crate) fn current_worker_idle_slice() -> Duration {
    DATA_WORKER_IDLE_SLICE.with(|slot| slot.get())
}

fn init_data_plane_runtime(config: &hammer_core::config::Config) {
    DATA_PLANE_RUNTIME.with(|runtime| {
        if runtime.borrow().is_none() {
            *runtime.borrow_mut() = Some(new_worker_runtime(config));
        }
    });
}

#[derive(Clone, Default)]
pub struct DataRemoteLocalQueue {
    tasks: Arc<Mutex<VecDeque<RemoteDataLocalTask>>>,
    thread: Arc<Mutex<Option<thread::Thread>>>,
}

impl DataRuntime {
    pub fn new(
        worker_threads: usize,
        thread_name: &str,
        thread_stack_size: usize,
        max_blocking_threads: usize,
    ) -> HammerResult<Self> {
        let mut worker = Worker::default();
        worker.count = worker_threads;
        worker.stack_size = thread_stack_size;
        worker.max_blocking_threads = max_blocking_threads;
        Self::from_config(&worker, thread_name)
    }

    pub fn from_config(worker: &Worker, thread_name: &str) -> HammerResult<Self> {
        worker.validate().map_err(HammerError::from)?;
        Self::spawn_workers(worker, thread_name)
    }

    fn spawn_workers(worker: &Worker, thread_name: &str) -> HammerResult<Self> {
        if worker.count == 0 {
            return Err(HammerError::internal(
                "data runtime must have at least one worker thread",
            ));
        }

        let worker_threads = worker.count;
        let thread_stack_size = worker.stack_size;
        let max_blocking_threads = worker.max_blocking_threads;
        let idle_slice = worker.idle_slice;
        let worker_config = worker.clone();

        let context_id = next_data_runtime_context_id();
        let wait_at_barrier = Arc::new(AtomicU32::new(0));
        let workers_at_barrier = Arc::new(AtomicU32::new(0));
        let mut context_workers = Vec::with_capacity(worker_threads);
        let mut workers = Vec::with_capacity(worker_threads);
        for index in 0..worker_threads {
            let worker_name = format!("{thread_name}-{index}");
            let remote_local = DataRemoteLocalQueue::default();
            let worker_remote_local = remote_local.clone();
            let worker_config = worker_config.clone();
            let worker_wait_at_barrier = Arc::clone(&wait_at_barrier);
            let worker_workers_at_barrier = Arc::clone(&workers_at_barrier);
            let (handle_tx, handle_rx) = std::sync::mpsc::channel::<Result<Handle, String>>();
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let builder = thread::Builder::new()
                .name(worker_name.clone())
                .stack_size(thread_stack_size);
            let join = builder
                .spawn(move || {
                    apply_worker_thread_setup(&worker_config, index);
                    DATA_WORKER_IDLE_SLICE.with(|slot| slot.set(idle_slice));
                    let config = hammer_core::config::Config {
                        worker: worker_config.clone(),
                        ..hammer_core::config::Config::default()
                    };
                    init_data_plane_runtime(&config);
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .max_blocking_threads(max_blocking_threads)
                        .enable_all()
                        .build();
                    match runtime {
                        Ok(runtime) => {
                            CURRENT_DATA_WORKER.with(|slot| slot.set(Some((context_id, index))));
                            worker_remote_local.attach_current_thread();
                            let _ = handle_tx.send(Ok(runtime.handle().clone()));
                            run_data_worker_loop(
                                &worker_remote_local,
                                &runtime,
                                shutdown_rx,
                                &worker_wait_at_barrier,
                                &worker_workers_at_barrier,
                            );
                            DATA_LOCAL_TASKS.with(|tasks| tasks.borrow_mut().clear());
                            DATA_LOCAL_DRIVER_WAKER.with(|waker| waker.borrow_mut().take());
                            CURRENT_DATA_WORKER.with(|slot| slot.set(None));
                            DATA_PLANE_RUNTIME.with(|slot| *slot.borrow_mut() = None);
                        }
                        Err(err) => {
                            let _ = handle_tx.send(Err(format!(
                                "init data runtime worker {worker_name}: {err}"
                            )));
                        }
                    }
                    let _ = done_tx.send(());
                })
                .map_err(|err| {
                    HammerError::internal(format!("spawn data runtime worker: {err}"))
                })?;

            let handle = handle_rx.recv().map_err(|err| {
                HammerError::internal(format!("receive data runtime handle: {err}"))
            })?;
            let handle = handle.map_err(HammerError::internal)?;
            context_workers.push(DataRuntimeContextWorker {
                handle,
                remote_local,
            });
            workers.push(DataRuntimeWorker {
                shutdown_tx: Some(shutdown_tx),
                done_rx,
                join: Some(join),
            });
        }

        Ok(Self {
            context: DataRuntimeContext::from_workers_with_barrier(context_id, context_workers),
            workers,
            wait_at_barrier,
            workers_at_barrier,
        })
    }

    pub fn context(&self) -> DataRuntimeContext {
        self.context.clone()
    }

    pub fn executor(&self) -> DataPlaneExecutor {
        self.context.executor()
    }

    pub fn data_plane_barrier(&self) -> DataPlaneBarrierHandle {
        DataPlaneBarrierHandle {
            wait: Arc::clone(&self.wait_at_barrier),
            workers: Arc::clone(&self.workers_at_barrier),
            n_workers: self.context.inner.workers.len() as u32,
        }
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) {
        for worker in &mut self.workers {
            if let Some(tx) = worker.shutdown_tx.take() {
                let _ = tx.send(());
            }
        }

        let deadline = Instant::now() + timeout;
        for worker in &mut self.workers {
            let now = Instant::now();
            if now >= deadline {
                continue;
            }
            let remaining = deadline.saturating_duration_since(now);
            if worker.done_rx.recv_timeout(remaining).is_ok()
                && let Some(join) = worker.join.take()
            {
                let _ = join.join();
            }
        }
    }
}

impl DataRuntimeContext {
    pub fn new(handle: Handle) -> Self {
        Self::new_many(vec![handle])
    }

    pub fn new_many(handles: Vec<Handle>) -> Self {
        assert!(
            !handles.is_empty(),
            "data runtime context requires at least one handle"
        );
        let workers = handles
            .into_iter()
            .map(|handle| DataRuntimeContextWorker {
                handle,
                remote_local: DataRemoteLocalQueue::default(),
            })
            .collect();
        Self::from_workers(next_data_runtime_context_id(), workers)
    }

    fn from_workers(id: usize, workers: Vec<DataRuntimeContextWorker>) -> Self {
        Self::from_workers_with_barrier(id, workers)
    }

    fn from_workers_with_barrier(id: usize, workers: Vec<DataRuntimeContextWorker>) -> Self {
        assert!(
            !workers.is_empty(),
            "data runtime context requires at least one worker"
        );
        Self {
            inner: Arc::new(DataRuntimeContextInner {
                id,
                workers,
                next: AtomicUsize::new(0),
            }),
        }
    }

    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        THREAD_DATA_CONTEXT.with(|slot| slot.borrow_mut().push(self.clone()));
        let _guard = ThreadDataContextGuard;
        f()
    }

    pub fn executor(&self) -> DataPlaneExecutor {
        DataPlaneExecutor {
            context: self.clone(),
        }
    }

    pub fn for_each_worker<F, R>(&self, f: F) -> HammerResult<Vec<R>>
    where
        F: Fn(usize) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        let f = Arc::new(f);
        let (tx, rx) = std::sync::mpsc::channel();
        for (index, worker) in self.inner.workers.iter().cloned().enumerate() {
            let tx = tx.clone();
            let f = Arc::clone(&f);
            let context = self.clone();
            drop(
                worker
                    .handle
                    .spawn(TASK_DATA_CONTEXT.scope(context, async move {
                        let _ = tx.send((index, f(index)));
                    })),
            );
        }
        drop(tx);

        let mut results = (0..self.inner.workers.len())
            .map(|_| None)
            .collect::<Vec<_>>();
        for _ in 0..results.len() {
            let (index, result) = rx.recv().map_err(|err| {
                HammerError::internal(format!("receive data worker initializer result: {err}"))
            })?;
            let slot = results.get_mut(index).ok_or_else(|| {
                HammerError::internal(format!(
                    "data worker initializer index out of range: {index}"
                ))
            })?;
            if slot.is_some() {
                return Err(HammerError::internal(format!(
                    "duplicate data worker initializer result: {index}"
                )));
            }
            *slot = Some(result);
        }

        results
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.ok_or_else(|| {
                    HammerError::internal(format!(
                        "missing data worker initializer result: {index}"
                    ))
                })
            })
            .collect()
    }

    pub fn install_on_workers<F, R>(&self, f: F) -> HammerResult<Vec<R>>
    where
        F: Fn(usize, &DataPlaneRuntime) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        self.for_each_worker(move |worker| with_data_plane_runtime(|runtime| f(worker, runtime)))
    }

    pub fn set_trace_control_on_workers(
        &self,
        control: Option<TraceControlHandle>,
        packet_capacity: usize,
    ) -> HammerResult<()> {
        let _ = packet_capacity;
        self.for_each_worker(move |_| {
            with_data_plane_runtime(|runtime| {
                runtime.set_trace_control(control.clone(), 0);
            });
        })
        .map(|_| ())
    }

    pub fn drain_trace_records_on_workers(&self, sink: TraceRecordSink) -> HammerResult<usize> {
        self.for_each_worker(move |_| sink.drain_completed())
            .map(|counts| counts.into_iter().sum())
    }

    pub fn runtime_stats_on_workers(&self) -> HammerResult<Vec<(usize, Vec<NodeRuntimeStatsRow>)>> {
        self.for_each_worker(move |worker| {
            with_data_plane_runtime(|runtime| {
                (worker, runtime.nodes().node_runtime_stats_snapshot())
            })
        })
    }

    pub async fn runtime_stats_on_workers_async(
        &self,
    ) -> HammerResult<Vec<(usize, Vec<NodeRuntimeStatsRow>)>> {
        let mut receivers = Vec::with_capacity(self.inner.workers.len());
        for (index, worker) in self.inner.workers.iter().cloned().enumerate() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let context = self.clone();
            drop(
                worker
                    .handle
                    .spawn(TASK_DATA_CONTEXT.scope(context, async move {
                        let rows = with_data_plane_runtime(|runtime| {
                            runtime.nodes().node_runtime_stats_snapshot()
                        });
                        let _ = tx.send((index, rows));
                    })),
            );
            receivers.push(rx);
        }

        let mut results = (0..self.inner.workers.len())
            .map(|_| None)
            .collect::<Vec<_>>();
        for rx in receivers {
            let (index, rows) = rx.await.map_err(|err| {
                HammerError::internal(format!("receive data worker runtime stats: {err}"))
            })?;
            let slot = results.get_mut(index).ok_or_else(|| {
                HammerError::internal(format!(
                    "data worker runtime stats index out of range: {index}"
                ))
            })?;
            if slot.is_some() {
                return Err(HammerError::internal(format!(
                    "duplicate data worker runtime stats: {index}"
                )));
            }
            *slot = Some((index, rows));
        }

        results
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.ok_or_else(|| {
                    HammerError::internal(format!("missing data worker runtime stats: {index}"))
                })
            })
            .collect()
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.inner.workers.len()
    }

    pub fn spawn_local_on_worker<F, Fut>(&self, worker: usize, factory: F) -> HammerResult<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let context = self.clone();
        self.schedule_local_on_worker(
            worker,
            Box::new(move || {
                THREAD_DATA_CONTEXT.with(|slot| slot.borrow_mut().push(context.clone()));
                let _guard = ThreadDataContextGuard;
                let _ = spawn_local(factory);
            }),
        )
    }

    pub(crate) async fn call_local_on_worker<F, Fut, T>(
        &self,
        worker: usize,
        factory: F,
    ) -> HammerResult<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let context = self.clone();
        let state = Arc::new(DataRemoteJoinState::new());
        let complete_state = Arc::clone(&state);
        self.schedule_local_on_worker(
            worker,
            Box::new(move || {
                THREAD_DATA_CONTEXT.with(|slot| slot.borrow_mut().push(context.clone()));
                let _guard = ThreadDataContextGuard;
                let join = spawn_local(factory);
                let complete_state = Arc::clone(&complete_state);
                drop(spawn_local(move || async move {
                    let result = join.await.map_err(|err| {
                        HammerError::internal(format!("join worker-local task: {err}"))
                    });
                    complete_state.complete(result);
                }));
            }),
        )?;
        DataRemoteJoinHandle { state }.await
    }

    pub(crate) fn call_blocking_on_worker<R>(
        &self,
        worker: usize,
        f: impl FnOnce() -> HammerResult<R> + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        self.schedule_local_on_worker(
            worker,
            Box::new(move || {
                let result = match catch_unwind(AssertUnwindSafe(f)) {
                    Ok(result) => result,
                    Err(_) => Err(HammerError::internal("data worker closure panicked")),
                };
                let _ = done_tx.send(result);
            }),
        )?;
        done_rx
            .recv()
            .map_err(|_| HammerError::internal("data worker closure canceled"))?
    }

    fn spawn_send_on_worker<F>(&self, worker: usize, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let context = self.clone();
        let dispatcher = tracing::dispatcher::get_default(Clone::clone);
        let control = Arc::new(DataLocalTaskControl::new());
        let state = Arc::new(DataLocalJoinState::new(Arc::clone(&control)));
        let join = DataLocalJoinHandle {
            state: Arc::clone(&state),
        };
        let panic_state = Arc::clone(&state);
        self.schedule_local_on_worker(
            worker,
            Box::new(move || {
                THREAD_DATA_CONTEXT.with(|slot| slot.borrow_mut().push(context.clone()));
                let _guard = ThreadDataContextGuard;
                let task_state = Arc::clone(&state);
                let future = TASK_DATA_CONTEXT
                    .scope(context, async move {
                        let output = future.await;
                        task_state.complete(output);
                    })
                    .with_subscriber(dispatcher);
                DATA_LOCAL_TASKS.with(|queue| {
                    queue.borrow_mut().push_back(Rc::new(DataLocalTask {
                        control,
                        future: RefCell::new(Some(Box::pin(future))),
                        on_panic: Box::new(move || panic_state.panic()),
                    }));
                });
                wake_data_local_driver();
            }),
        )
        .expect("target worker index is valid");
        JoinHandle::from_local(join)
    }

    fn schedule_local_on_worker(
        &self,
        worker: usize,
        task: RemoteDataLocalTask,
    ) -> HammerResult<()> {
        if worker >= self.inner.workers.len() {
            return Err(HammerError::internal(format!(
                "invalid app worker {worker}; worker_count={}",
                self.inner.workers.len()
            )));
        }
        self.inner.workers[worker].remote_local.push(task);
        Ok(())
    }

    fn spawn_worker_index(&self) -> usize {
        if let Some(index) = self.current_worker_index() {
            return index;
        }
        self.next_worker_index()
    }

    fn next_worker_index(&self) -> usize {
        let index = self.inner.next.fetch_add(1, Ordering::Relaxed);
        index % self.inner.workers.len()
    }

    pub(crate) fn current_worker_index(&self) -> Option<usize> {
        CURRENT_DATA_WORKER.with(|slot| match slot.get() {
            Some((id, index)) if id == self.inner.id && index < self.inner.workers.len() => {
                Some(index)
            }
            _ => None,
        })
    }
}

/// Barrier handle for control-plane thread synchronization.
/// Wraps the VPP-style atomic barrier.
#[derive(Clone)]
pub struct DataPlaneBarrierHandle {
    wait: Arc<AtomicU32>,
    workers: Arc<AtomicU32>,
    n_workers: u32,
}

impl DataPlaneBarrierHandle {
    pub fn sync(&self) -> HammerResult<DataPlaneBarrierGuard> {
        self.wait.store(1, Ordering::SeqCst);
        std::sync::atomic::compiler_fence(Ordering::SeqCst);
        while self.workers.load(Ordering::Acquire) != self.n_workers {
            core::hint::spin_loop();
        }
        Ok(DataPlaneBarrierGuard {
            wait: Arc::clone(&self.wait),
            workers: Arc::clone(&self.workers),
            n_workers: self.n_workers,
        })
    }

    pub fn synchronize<R>(&self, operation: impl FnOnce() -> HammerResult<R>) -> HammerResult<R> {
        let _guard = self.sync()?;
        operation()
    }
}

#[derive(Debug)]
pub struct DataPlaneBarrierGuard {
    wait: Arc<AtomicU32>,
    workers: Arc<AtomicU32>,
    n_workers: u32,
}

impl DataPlaneBarrierGuard {
    pub fn paused_workers(&self) -> usize {
        self.n_workers as usize
    }
}

impl Drop for DataPlaneBarrierGuard {
    fn drop(&mut self) {
        crate::barrier::barrier_release(&self.wait, &self.workers);
    }
}

impl DataRemoteLocalQueue {
    pub fn attach_current_thread(&self) {
        *self
            .thread
            .lock()
            .expect("remote local thread handle poisoned") = Some(thread::current());
    }

    fn push(&self, task: RemoteDataLocalTask) {
        self.tasks
            .lock()
            .expect("remote local queue poisoned")
            .push_back(task);
        if let Some(thread) = self
            .thread
            .lock()
            .expect("remote local thread handle poisoned")
            .as_ref()
            .cloned()
        {
            thread.unpark();
        }
    }

    pub(crate) fn drain(&self) -> VecDeque<RemoteDataLocalTask> {
        let mut tasks = self.tasks.lock().expect("remote local queue poisoned");
        std::mem::take(&mut *tasks)
    }
}

impl DataPlaneExecutor {
    pub fn execute<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let worker = self.context.spawn_worker_index();
        self.context.spawn_send_on_worker(worker, future)
    }
}

#[derive(Debug)]
pub(crate) struct DataWorkerThreadWake {
    pub(crate) thread: thread::Thread,
}

impl Wake for DataWorkerThreadWake {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

struct DataLocalTask {
    control: Arc<DataLocalTaskControl>,
    future: RefCell<Option<DataLocalTaskFuture>>,
    on_panic: Box<dyn Fn() + 'static>,
}

impl DataLocalTask {
    fn poll(&self, cx: &mut Context<'_>) -> bool {
        self.control.set_driver_waker(cx.waker().clone());
        if self.control.is_cancelled() {
            self.future.borrow_mut().take();
            return false;
        }

        let mut future_slot = self.future.borrow_mut();
        let Some(future) = future_slot.as_mut() else {
            return false;
        };
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(())) => {
                future_slot.take();
                false
            }
            Ok(Poll::Pending) => true,
            Err(_) => {
                future_slot.take();
                (self.on_panic)();
                false
            }
        }
    }
}

struct DataLocalTaskControl {
    cancelled: AtomicBool,
    driver_waker: Mutex<Option<Waker>>,
}

impl DataLocalTaskControl {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            driver_waker: Mutex::new(None),
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.wake_driver();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn set_driver_waker(&self, waker: Waker) {
        *self.driver_waker.lock().expect("data local waker poisoned") = Some(waker);
    }

    fn wake_driver(&self) {
        if let Some(waker) = self
            .driver_waker
            .lock()
            .expect("data local waker poisoned")
            .as_ref()
            .cloned()
        {
            waker.wake();
        }
    }
}

struct DataLocalJoinState<T> {
    control: Arc<DataLocalTaskControl>,
    result: Mutex<Option<Result<T, DataLocalJoinError>>>,
    join_waker: Mutex<Option<Waker>>,
}

impl<T> DataLocalJoinState<T> {
    fn new(control: Arc<DataLocalTaskControl>) -> Self {
        Self {
            control,
            result: Mutex::new(None),
            join_waker: Mutex::new(None),
        }
    }

    fn complete(&self, output: T) {
        let mut result = self.result.lock().expect("data local result poisoned");
        if result.is_none() && !self.control.is_cancelled() {
            *result = Some(Ok(output));
            drop(result);
            self.wake_join();
        }
    }

    fn cancel(&self) {
        self.control.cancel();
        let mut result = self.result.lock().expect("data local result poisoned");
        if result.is_none() {
            *result = Some(Err(DataLocalJoinError::cancelled()));
            drop(result);
            self.wake_join();
        }
    }

    fn panic(&self) {
        let mut result = self.result.lock().expect("data local result poisoned");
        if result.is_none() {
            *result = Some(Err(DataLocalJoinError::panic()));
            drop(result);
            self.wake_join();
        }
    }

    fn wake_join(&self) {
        if let Some(waker) = self
            .join_waker
            .lock()
            .expect("data local join waker poisoned")
            .take()
        {
            waker.wake();
        }
    }
}

struct DataRemoteJoinState<T> {
    result: Mutex<Option<HammerResult<T>>>,
    waker: Mutex<Option<Waker>>,
}

impl<T> DataRemoteJoinState<T> {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            waker: Mutex::new(None),
        }
    }

    fn complete(&self, result: HammerResult<T>) {
        let mut slot = self.result.lock().expect("remote local result poisoned");
        if slot.is_none() {
            *slot = Some(result);
            drop(slot);
            if let Some(waker) = self
                .waker
                .lock()
                .expect("remote local waker poisoned")
                .take()
            {
                waker.wake();
            }
        }
    }
}

struct DataRemoteJoinHandle<T> {
    state: Arc<DataRemoteJoinState<T>>,
}

impl<T> Future for DataRemoteJoinHandle<T> {
    type Output = HammerResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut slot = self
            .state
            .result
            .lock()
            .expect("remote local result poisoned");
        if let Some(result) = slot.take() {
            return Poll::Ready(result);
        }
        *self
            .state
            .waker
            .lock()
            .expect("remote local waker poisoned") = Some(cx.waker().clone());
        Poll::Pending
    }
}

enum JoinHandleKind<T> {
    Tokio(TokioJoinHandle<T>),
    Local(DataLocalJoinHandle<T>),
}

pub struct JoinHandle<T> {
    inner: JoinHandleKind<T>,
}

impl<T> JoinHandle<T> {
    fn from_tokio(inner: TokioJoinHandle<T>) -> Self {
        Self {
            inner: JoinHandleKind::Tokio(inner),
        }
    }

    fn from_local(inner: DataLocalJoinHandle<T>) -> Self {
        Self {
            inner: JoinHandleKind::Local(inner),
        }
    }

    pub fn abort(&mut self) {
        match &mut self.inner {
            JoinHandleKind::Tokio(inner) => inner.abort(),
            JoinHandleKind::Local(inner) => inner.abort(),
        }
    }
}

impl<T> Unpin for JoinHandle<T> {}

#[derive(Debug)]
pub struct JoinError {
    kind: JoinErrorKind,
}

#[derive(Debug)]
enum JoinErrorKind {
    Tokio(tokio::task::JoinError),
    Local(DataLocalJoinError),
}

impl JoinError {
    fn from_tokio(err: tokio::task::JoinError) -> Self {
        Self {
            kind: JoinErrorKind::Tokio(err),
        }
    }

    fn from_local(err: DataLocalJoinError) -> Self {
        Self {
            kind: JoinErrorKind::Local(err),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        match &self.kind {
            JoinErrorKind::Tokio(err) => err.is_cancelled(),
            JoinErrorKind::Local(err) => err.is_cancelled(),
        }
    }

    pub fn is_panic(&self) -> bool {
        match &self.kind {
            JoinErrorKind::Tokio(err) => err.is_panic(),
            JoinErrorKind::Local(err) => err.is_panic(),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            JoinErrorKind::Tokio(err) => fmt::Display::fmt(err, f),
            JoinErrorKind::Local(err) => fmt::Display::fmt(err, f),
        }
    }
}

impl Error for JoinError {}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            JoinHandleKind::Tokio(inner) => Pin::new(inner).poll(cx).map_err(JoinError::from_tokio),
            JoinHandleKind::Local(inner) => Pin::new(inner).poll(cx).map_err(JoinError::from_local),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataLocalJoinError {
    kind: DataLocalJoinErrorKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DataLocalJoinErrorKind {
    Cancelled,
    Panic,
}

impl DataLocalJoinError {
    fn cancelled() -> Self {
        Self {
            kind: DataLocalJoinErrorKind::Cancelled,
        }
    }

    fn panic() -> Self {
        Self {
            kind: DataLocalJoinErrorKind::Panic,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self.kind, DataLocalJoinErrorKind::Cancelled)
    }

    pub fn is_panic(&self) -> bool {
        matches!(self.kind, DataLocalJoinErrorKind::Panic)
    }
}

impl fmt::Display for DataLocalJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            DataLocalJoinErrorKind::Cancelled => f.write_str("data local task cancelled"),
            DataLocalJoinErrorKind::Panic => f.write_str("data local task panicked"),
        }
    }
}

impl Error for DataLocalJoinError {}

pub(crate) fn cleanup_thread_local() {
    DATA_LOCAL_TASKS.with(|tasks| tasks.borrow_mut().clear());
    DATA_LOCAL_DRIVER_WAKER.with(|waker| waker.borrow_mut().take());
    CURRENT_DATA_WORKER.with(|slot| slot.set(None));
    DATA_PLANE_RUNTIME.with(|slot| *slot.borrow_mut() = None);
}

pub(crate) fn poll_data_local_tasks(cx: &mut Context<'_>) -> bool {
    let initial_len = DATA_LOCAL_TASKS.with(|queue| queue.borrow().len());
    let mut progressed = false;
    for _ in 0..initial_len {
        let Some(task) = DATA_LOCAL_TASKS.with(|queue| queue.borrow_mut().pop_front()) else {
            break;
        };
        progressed = true;
        if task.poll(cx) {
            DATA_LOCAL_TASKS.with(|queue| queue.borrow_mut().push_back(task));
        }
    }
    progressed
}

pub(crate) fn poll_remote_local_tasks(queue: &DataRemoteLocalQueue) -> bool {
    let mut progressed = false;
    for task in queue.drain() {
        progressed = true;
        task();
    }
    progressed
}

fn poll_data_plane_nodes(cx: &mut Context<'_>) -> bool {
    let mut progressed = false;
    loop {
        let ready = with_data_plane_runtime(|runtime| {
            let mut ready = runtime.nodes().ready();
            Pin::new(&mut ready).poll(cx)
        });
        if !matches!(ready, Poll::Ready(())) {
            break;
        }
        progressed = true;
        let result = with_data_plane_runtime(|runtime| runtime.run_ready_nodes());
        match result {
            Ok(0) => continue,
            Ok(_) => {
                progressed = true;
            }
            Err(err) => {
                tracing::debug!("data plane node scheduler failed: {err}");
                break;
            }
        }
    }
    progressed
}

fn wake_data_local_driver() {
    DATA_LOCAL_DRIVER_WAKER.with(|waker| {
        if let Some(waker) = waker.borrow().as_ref().cloned() {
            waker.wake();
        }
    });
}

fn run_data_worker_loop(
    remote_local: &DataRemoteLocalQueue,
    runtime: &tokio::runtime::Runtime,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    wait_at_barrier: &AtomicU32,
    workers_at_barrier: &AtomicU32,
) {
    let worker_waker = Waker::from(Arc::new(DataWorkerThreadWake {
        thread: thread::current(),
    }));
    DATA_LOCAL_DRIVER_WAKER.with(|slot| {
        *slot.borrow_mut() = Some(worker_waker.clone());
    });
    let mut next_polling_driver_at = Instant::now();

    loop {
        match shutdown_rx.try_recv() {
            Ok(_) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
        }

        let local_progress = poll_data_worker_once(
            remote_local,
            &worker_waker,
            &mut next_polling_driver_at,
            wait_at_barrier,
            workers_at_barrier,
        );
        drive_tokio_worker_once(runtime, local_progress);

        if !local_progress {
            let idle_slice = DATA_WORKER_IDLE_SLICE.with(|slot| slot.get());
            thread::park_timeout(idle_slice);
        }
    }
}

fn poll_data_worker_once(
    remote_local: &DataRemoteLocalQueue,
    worker_waker: &Waker,
    next_polling_driver_at: &mut Instant,
    wait_at_barrier: &AtomicU32,
    workers_at_barrier: &AtomicU32,
) -> bool {
    let mut cx = Context::from_waker(worker_waker);
    DATA_LOCAL_DRIVER_WAKER.with(|slot| {
        *slot.borrow_mut() = Some(worker_waker.clone());
    });

    crate::barrier::barrier_check(wait_at_barrier, workers_at_barrier);

    let now = Instant::now();
    let mut progressed = false;
    if now >= *next_polling_driver_at {
        let poll_interval = DATA_WORKER_IDLE_SLICE.with(|slot| slot.get());
        *next_polling_driver_at = now + poll_interval;
        progressed =
            match with_data_plane_runtime(|runtime| runtime.schedule_polling_driver_nodes()) {
                Ok(scheduled) => scheduled > 0,
                Err(err) => {
                    tracing::debug!("data plane polling driver scheduler failed: {err}");
                    false
                }
            };
    }
    progressed |= poll_remote_local_tasks(remote_local);
    progressed |= poll_data_plane_nodes(&mut cx);
    progressed |= poll_data_local_tasks(&mut cx);
    progressed |= poll_data_plane_nodes(&mut cx);
    progressed
}

fn drive_tokio_worker_once(runtime: &tokio::runtime::Runtime, local_progress: bool) {
    if local_progress {
        runtime.block_on(async {
            tokio::task::yield_now().await;
        });
        return;
    }

    let idle_slice = DATA_WORKER_IDLE_SLICE.with(|slot| slot.get());
    runtime.block_on(async {
        tokio::time::sleep(idle_slice).await;
    });
}

struct ThreadDataContextGuard;

impl Drop for ThreadDataContextGuard {
    fn drop(&mut self) {
        THREAD_DATA_CONTEXT.with(|slot| {
            slot.borrow_mut().pop();
        });
    }
}

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match current_task_data_context() {
        Some(context) => context.executor().execute(future),
        None => JoinHandle::from_tokio(tokio::spawn(future.with_current_subscriber())),
    }
}

pub struct DataLocalJoinHandle<T> {
    state: Arc<DataLocalJoinState<T>>,
}

impl<T> DataLocalJoinHandle<T> {
    pub fn abort(&mut self) {
        self.state.cancel();
    }
}

impl<T> Unpin for DataLocalJoinHandle<T> {}

impl<T> Future for DataLocalJoinHandle<T> {
    type Output = Result<T, DataLocalJoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut result = self
            .state
            .result
            .lock()
            .expect("data local result poisoned");
        if let Some(result) = result.take() {
            return Poll::Ready(result);
        }
        *self
            .state
            .join_waker
            .lock()
            .expect("data local join waker poisoned") = Some(cx.waker().clone());
        Poll::Pending
    }
}

pub fn spawn_local<F, Fut, T>(factory: F) -> DataLocalJoinHandle<T>
where
    F: FnOnce() -> Fut + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let context = current_data_context().expect("data local spawn requires a data runtime context");
    assert!(
        context.current_worker_index().is_some(),
        "data local spawn requires the current data worker thread"
    );
    let dispatcher = tracing::dispatcher::get_default(Clone::clone);
    let control = Arc::new(DataLocalTaskControl::new());
    DATA_LOCAL_DRIVER_WAKER.with(|waker| {
        if let Some(waker) = waker.borrow().as_ref().cloned() {
            control.set_driver_waker(waker);
        }
    });
    let state = Arc::new(DataLocalJoinState::new(Arc::clone(&control)));
    let task_state = Arc::clone(&state);
    let future = TASK_DATA_CONTEXT
        .scope(context, async move {
            let output = factory().await;
            task_state.complete(output);
        })
        .with_subscriber(dispatcher);
    DATA_LOCAL_TASKS.with(|queue| {
        let panic_state = Arc::clone(&state);
        queue.borrow_mut().push_back(Rc::new(DataLocalTask {
            control,
            future: RefCell::new(Some(Box::pin(future))),
            on_panic: Box::new(move || panic_state.panic()),
        }));
    });
    wake_data_local_driver();
    DataLocalJoinHandle { state }
}

pub fn spawn_current_local<F>(future: F) -> DataLocalJoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: Send + 'static,
{
    let context =
        current_data_context().expect("current local spawn requires a data runtime context");
    assert!(
        context.current_worker_index().is_some(),
        "current local spawn requires the current data worker thread"
    );
    spawn_local(move || future)
}

pub async fn yield_local_now() {
    struct YieldLocal {
        yielded: bool,
    }

    impl Future for YieldLocal {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yielded {
                return Poll::Ready(());
            }
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    YieldLocal { yielded: false }.await
}

pub fn set_data_plane_runtime(runtime: RuntimeDataPlaneRuntime) {
    DATA_PLANE_RUNTIME.with(|slot| {
        *slot.borrow_mut() = Some(runtime);
    });
}

pub fn with_data_plane_runtime<R>(f: impl FnOnce(&RuntimeDataPlaneRuntime) -> R) -> R {
    DATA_PLANE_RUNTIME.with(|runtime| {
        f(runtime
            .borrow()
            .as_ref()
            .expect("data plane runtime not initialized on worker thread"))
    })
}

pub fn with_data_plane_buffers<R>(f: impl FnOnce(&DataPlaneBuffers) -> R) -> R {
    with_data_plane_runtime(|runtime| f(runtime.buffers()))
}

fn current_data_context() -> Option<DataRuntimeContext> {
    TASK_DATA_CONTEXT
        .try_with(Clone::clone)
        .ok()
        .or_else(|| THREAD_DATA_CONTEXT.with(|slot| slot.borrow().last().cloned()))
}

fn current_task_data_context() -> Option<DataRuntimeContext> {
    TASK_DATA_CONTEXT.try_with(Clone::clone).ok()
}

fn next_data_runtime_context_id() -> usize {
    NEXT_DATA_RUNTIME_CONTEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::data_plane::{BufferFrame, NodeId, NodeRegistration, NodeState};
    use hammer_runtime::{
        DataPlaneRuntime, DriverNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
        TraceControlPlane, TraceEntry, TraceInputPolicy, TracePolicy, TraceRecord,
    };
    use std::sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    };
    use std::thread;
    use std::time::Duration;
    use tokio::sync::oneshot;

    static TEST_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    static POLLING_DRIVER_CALLS: AtomicU64 = AtomicU64::new(0);

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("spawn test lock poisoned")
    }

    struct PollingDriverNode;

    impl Node for PollingDriverNode {
        fn process(&mut self, _runtime: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }

        fn node_process(&self) -> NodeProcessFn {
            polling_driver_process
        }
    }

    impl DriverNode for PollingDriverNode {
        fn node_registration(&self) -> NodeRegistration {
            NodeRegistration::next("spawn-test-polling-driver", 0)
        }
    }

    fn polling_driver_process(
        _runtime: &DataPlaneRuntime,
        _data: NodeRuntimeData,
        _: &mut BufferFrame,
    ) -> NodeResult {
        POLLING_DRIVER_CALLS.fetch_add(1, Ordering::SeqCst);
        NodeResult::drop()
    }

    #[test]
    fn data_runtime_spawns_across_fixed_data_threads() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(2, "spawn-test-data", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let names = driver.block_on(async {
            let mut handles = Vec::new();
            for _ in 0..8 {
                handles.push(context.executor().execute(async {
                    thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default()
                }));
            }
            let mut names = Vec::new();
            for handle in handles {
                names.push(handle.await.expect("data task finished"));
            }
            names
        });

        assert!(
            names.iter().any(|name| name == "spawn-test-data-0"),
            "{names:?}"
        );
        assert!(
            names.iter().any(|name| name == "spawn-test-data-1"),
            "{names:?}"
        );

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_context_runs_initializer_on_each_worker() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(2, "spawn-test-init", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();

        let mut names = context
            .for_each_worker(|index| {
                let name = thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                (index, name)
            })
            .expect("run worker initializer");
        names.sort();

        assert_eq!(
            names,
            vec![
                (0, "spawn-test-init-0".to_owned()),
                (1, "spawn-test-init-1".to_owned()),
            ]
        );

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_thread_exposes_thread_local_data_plane_runtime() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-buffer", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let (before, during, after) = run_on_context(&context, async {
            spawn(async move {
                let stats = with_data_plane_runtime(|runtime| {
                    let before = runtime.in_use_buffers();
                    let index = runtime
                        .alloc_index_with_bytes(b"packet")
                        .expect("alloc data buffer");
                    let during = runtime.in_use_buffers();
                    let mut owner = runtime
                        .buffers()
                        .get_next_frame(hammer_core::data_plane::NodeId::new(0))
                        .expect("cleanup frame");
                    owner.push_index(index).expect("cleanup push");
                    (before, during)
                });
                let after = with_data_plane_runtime(|runtime| runtime.in_use_buffers());
                (stats.0, stats.1, after)
            })
            .await
            .expect("buffer task finished")
        });

        assert_eq!(before, 0);
        assert_eq!(during, 1);
        assert_eq!(after, 0);

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_context_sets_trace_control_on_each_worker_runtime() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(2, "spawn-test-trace", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let control = TraceControlPlane::new(8);
        control.publish(TracePolicy {
            enabled: false,
            record_capacity: 8,
            packet_capacity: 2,
            inputs: vec![TraceInputPolicy {
                node: NodeId::new(0),
                count: 2,
            }]
            .into(),
        });

        context
            .set_trace_control_on_workers(Some(control.handle()), 2)
            .expect("set trace control");
        let marks = context
            .for_each_worker(|_| {
                with_data_plane_runtime(|runtime| {
                    let index = runtime
                        .alloc_index_with_bytes(b"packet")
                        .expect("alloc packet");
                    runtime
                        .try_mark_trace(NodeId::new(0), index)
                        .expect("disabled trace mark is no-op");
                    let marked = runtime
                        .get_buffer(index)
                        .expect("buffer")
                        .trace_handle()
                        .is_some();
                    let mut owner = runtime
                        .buffers()
                        .get_next_frame(hammer_core::data_plane::NodeId::new(0))
                        .expect("cleanup frame");
                    owner.push_index(index).expect("cleanup push");
                    marked
                })
            })
            .expect("inspect workers");

        assert_eq!(marks, vec![false, false]);

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_context_collects_runtime_stats_on_workers() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(2, "spawn-test-runtime-stats", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();

        let stats = context
            .runtime_stats_on_workers()
            .expect("collect runtime stats");

        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].0, 0);
        assert_eq!(stats[1].0, 1);
        assert!(stats[0].1.is_empty());
        assert!(stats[1].1.is_empty());

        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build driver runtime");
        let async_stats = driver.block_on(async {
            context
                .runtime_stats_on_workers_async()
                .await
                .expect("collect async runtime stats")
        });

        assert_eq!(async_stats.len(), 2);
        assert_eq!(async_stats[0].0, 0);
        assert_eq!(async_stats[1].0, 1);
        assert!(async_stats[0].1.is_empty());
        assert!(async_stats[1].1.is_empty());

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_worker_polls_driver_from_worker_loop_at_interval() {
        let _guard = test_lock();
        POLLING_DRIVER_CALLS.store(0, Ordering::SeqCst);
        let worker = Worker {
            count: 1,
            stack_size: 512 * 1024,
            max_blocking_threads: 2,
            idle_slice: Duration::from_millis(25),
            ..Worker::default()
        };
        let data_runtime =
            DataRuntime::from_config(&worker, "spawn-test-poll-driver").expect("data runtime");
        let context = data_runtime.context();

        context
            .for_each_worker(|_| {
                with_data_plane_runtime(|runtime| {
                    let node = runtime.nodes().register_driver(PollingDriverNode);
                    runtime
                        .nodes()
                        .set_node_state(node, NodeState::Polling)
                        .expect("set polling driver state");
                });
            })
            .expect("register polling driver on worker");

        let deadline = Instant::now() + Duration::from_millis(300);
        while Instant::now() < deadline {
            if POLLING_DRIVER_CALLS.load(Ordering::SeqCst) >= 3 {
                data_runtime.shutdown_timeout(Duration::from_secs(1));
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }

        let calls = POLLING_DRIVER_CALLS.load(Ordering::SeqCst);
        data_runtime.shutdown_timeout(Duration::from_secs(1));
        assert!(
            calls >= 3,
            "polling driver should keep running at the worker poll interval; calls={calls}"
        );
    }

    #[test]
    fn data_runtime_local_spawn_keeps_non_send_buffer_on_worker_thread() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let (thread_before, thread_after, before, during, after, payload) =
            run_on_context(&context, async {
                spawn(async {
                    spawn_local(|| async {
                        let thread_before = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let buffer = with_data_plane_runtime(|runtime| {
                            let before = runtime.in_use_buffers();
                            let buffer = runtime
                                .alloc_index_with_bytes(b"packet")
                                .expect("alloc local data buffer");
                            let during = runtime.in_use_buffers();
                            let mut owner = runtime
                                .buffers()
                                .get_next_frame(hammer_core::data_plane::NodeId::new(0))
                                .expect("local owner");
                            owner.push_index(buffer).expect("local push");
                            (before, during, owner)
                        });
                        yield_local_now().await;
                        let thread_after = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let payload = with_data_plane_runtime(|runtime| {
                            let payload = runtime
                                .get_buffer(buffer.2.pending_indices()[0])
                                .expect("local data buffer")
                                .current()
                                .to_vec();
                            payload
                        });
                        drop(buffer.2);
                        let after = with_data_plane_runtime(|runtime| runtime.in_use_buffers());
                        (
                            thread_before,
                            thread_after,
                            buffer.0,
                            buffer.1,
                            after,
                            payload,
                        )
                    })
                    .await
                    .expect("local data task finished")
                })
                .await
                .expect("data task finished")
            });

        assert_eq!(thread_before, "spawn-test-local-0");
        assert_eq!(thread_after, "spawn-test-local-0");
        assert_eq!(before, 0);
        assert_eq!(during, 1);
        assert_eq!(after, 0);
        assert_eq!(payload, b"packet");

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn current_local_spawn_keeps_non_send_buffer_on_current_worker() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-current-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let (thread_before, thread_after, payload) = run_on_context(&context, async {
            spawn(async {
                spawn_local(|| async {
                    let join = spawn_current_local(async {
                        let thread_before = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let buffer = with_data_plane_runtime(|runtime| {
                            let index = runtime
                                .alloc_index_with_bytes(b"packet")
                                .expect("alloc local data buffer");
                            let mut owner = runtime
                                .buffers()
                                .get_next_frame(hammer_core::data_plane::NodeId::new(0))
                                .expect("local owner");
                            owner.push_index(index).expect("local push");
                            owner
                        });
                        yield_local_now().await;
                        let thread_after = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let payload = with_data_plane_runtime(|runtime| {
                            let payload = runtime
                                .get_buffer(buffer.pending_indices()[0])
                                .expect("current local data buffer")
                                .current()
                                .to_vec();
                            payload
                        });
                        drop(buffer);
                        (thread_before, thread_after, payload)
                    });
                    join.await.expect("current local task joined")
                })
                .await
                .expect("outer local data task finished")
            })
            .await
            .expect("data task finished")
        });

        assert_eq!(thread_before, "spawn-test-current-local-0");
        assert_eq!(thread_after, "spawn-test-current-local-0");
        assert_eq!(payload, b"packet");

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_runtime_local_spawn_is_not_polled_under_tokio_worker_context() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-no-tokio-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let has_tokio_handle = run_on_context(&context, async {
            spawn(async {
                spawn_local(|| async { tokio::runtime::Handle::try_current().is_ok() })
                    .await
                    .expect("local task finished")
            })
            .await
            .expect("data task finished")
        });

        assert!(
            !has_tokio_handle,
            "app/dataplane local task should not inherit a Tokio worker runtime"
        );

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_runtime_spawn_is_not_polled_under_tokio_worker_context() {
        let _guard = test_lock();
        let data_runtime = DataRuntime::new(1, "spawn-test-no-tokio-generic", 512 * 1024, 2)
            .expect("data runtime");
        let context = data_runtime.context();
        let has_tokio_handle = run_on_context(&context, async {
            spawn(async { tokio::runtime::Handle::try_current().is_ok() })
                .await
                .expect("data task finished")
        });

        assert!(
            !has_tokio_handle,
            "dataplane spawned task should not inherit a Tokio worker runtime"
        );

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn frame_pending_future_resumes_on_current_data_worker() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-frame", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let (before_thread, after_thread, payload) = run_on_context(&context, async {
            spawn(async {
                spawn_local(|| async {
                    let runtime = with_data_plane_buffers(Clone::clone);
                    let frame = std::rc::Rc::new(RefCell::new(
                        runtime
                            .get_next_frame(hammer_core::data_plane::NodeId::new(0))
                            .expect("alloc frame"),
                    ));
                    let index = runtime
                        .alloc_index_with_bytes(b"packet")
                        .expect("alloc data buffer");
                    let consumer_frame = std::rc::Rc::clone(&frame);
                    let consumer_runtime = runtime.clone();
                    let consumer = spawn_current_local(async move {
                        let before_thread = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let pending = consumer_frame.borrow().pending();
                        pending.await;
                        let after_thread = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let buffer = consumer_frame.borrow().pending_indices()[0];
                        let payload = consumer_runtime
                            .get_buffer(buffer)
                            .expect("pending buffer")
                            .current()
                            .to_vec();
                        (before_thread, after_thread, payload)
                    });
                    let producer_frame = std::rc::Rc::clone(&frame);
                    let producer = spawn_current_local(async move {
                        yield_local_now().await;
                        producer_frame
                            .borrow_mut()
                            .push_index(index)
                            .expect("push pending buffer");
                    });
                    producer.await.expect("producer joined");
                    let result = consumer.await.expect("consumer joined");
                    let frame = match std::rc::Rc::try_unwrap(frame) {
                        Ok(frame) => frame.into_inner(),
                        Err(_) => panic!("frame has remaining references"),
                    };
                    drop(frame);
                    result
                })
                .await
                .expect("local frame task finished")
            })
            .await
            .expect("data task finished")
        });

        assert_eq!(before_thread, "spawn-test-frame-0");
        assert_eq!(after_thread, "spawn-test-frame-0");
        assert_eq!(payload, b"packet");

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_runtime_local_child_stays_on_parent_worker_with_multiple_workers() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(2, "spawn-test-pipeline", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let (parent_thread, child_thread) = run_on_context(&context, async {
            spawn(async {
                let parent_thread = thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let child_thread = spawn_local(|| async {
                    thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default()
                })
                .await
                .expect("local child task finished");
                (parent_thread, child_thread)
            })
            .await
            .expect("parent task finished")
        });

        assert_eq!(parent_thread, child_thread);
        assert!(parent_thread.starts_with("spawn-test-pipeline-"));

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn nested_data_spawn_stays_on_current_worker_with_multiple_workers() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(2, "spawn-test-nested", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let (parent_thread, child_thread) = run_on_context(&context, async {
            spawn(async {
                let parent_thread = thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let child_thread = spawn(async {
                    thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default()
                })
                .await
                .expect("nested task finished");
                (parent_thread, child_thread)
            })
            .await
            .expect("parent task finished")
        });

        assert_eq!(parent_thread, child_thread);
        assert!(parent_thread.starts_with("spawn-test-nested-"));

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_plane_executor_bootstrap_starts_local_child_on_parent_worker() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(2, "spawn-test-executor", 512 * 1024, 2).expect("data runtime");
        let executor = data_runtime.executor();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let (parent_thread, child_thread) = driver.block_on(async {
            executor
                .execute(async move {
                    let parent_thread = thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default();
                    let child_thread = spawn_local(|| async {
                        thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default()
                    })
                    .await
                    .expect("local child task finished");
                    (parent_thread, child_thread)
                })
                .await
                .expect("executor parent task finished")
        });

        assert_eq!(parent_thread, child_thread);
        assert!(parent_thread.starts_with("spawn-test-executor-"));

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_local_spawn_abort_stops_pending_task() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-abort-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        run_on_context(&context, async {
            spawn(async {
                let mut task = spawn_local(|| async {
                    std::future::pending::<()>().await;
                    1usize
                });
                task.abort();
                let err = task
                    .await
                    .expect_err("aborted local task must not produce output");
                assert!(err.is_cancelled(), "{err}");
            })
            .await
            .expect("data task finished")
        });

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn dropping_data_local_handle_does_not_abort_task() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-drop-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        run_on_context(&context, async {
            spawn(async {
                let (tx, rx) = oneshot::channel();
                let task = spawn_local(move || async move {
                    yield_local_now().await;
                    let _ = tx.send(());
                });
                drop(task);
                rx.await.expect("local task should send completion");
            })
            .await
            .expect("data task finished")
        });

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_local_spawn_panic_returns_error_without_stopping_driver() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-panic-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let output = run_on_context(&context, async {
            spawn(async {
                let panicking = spawn_local(|| async {
                    panic!("local task panic");
                });
                let err = panicking
                    .await
                    .expect_err("panic should be reported as join error");
                assert!(err.is_panic(), "{err}");

                spawn_local(|| async { 42usize })
                    .await
                    .expect("driver should continue after panic")
            })
            .await
            .expect("data task finished")
        });

        assert_eq!(output, 42);

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn spawn_targets_current_data_context() {
        let _guard = test_lock();
        let first =
            DataRuntime::new(1, "spawn-test-first", 512 * 1024, 2).expect("first data runtime");
        let first_context = first.context();
        let second =
            DataRuntime::new(1, "spawn-test-second", 512 * 1024, 2).expect("second data runtime");
        let second_context = second.context();

        let first_name = spawn_thread_name(&first_context);
        let second_name = spawn_thread_name(&second_context);

        assert_eq!(first_name, "spawn-test-first-0");
        assert_eq!(second_name, "spawn-test-second-0");

        second.shutdown_timeout(Duration::from_secs(1));
        first.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_context_enter_does_not_install_tokio_runtime_handle() {
        let _guard = test_lock();
        let data_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("spawn-test-entered")
            .enable_all()
            .build()
            .expect("data runtime");
        let data_context = DataRuntimeContext::new(data_runtime.handle().clone());
        let control_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("control runtime");

        control_runtime.block_on(async {
            assert!(
                matches!(
                    data_context.enter(|| Handle::current().runtime_flavor()),
                    tokio::runtime::RuntimeFlavor::CurrentThread
                ),
                "enter should preserve the ambient control runtime handle instead of installing a data-worker Tokio handle"
            );
        });

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn nested_data_context_restores_outer_context() {
        let _guard = test_lock();
        let first =
            DataRuntime::new(1, "spawn-test-outer", 512 * 1024, 2).expect("first data runtime");
        let first_context = first.context();
        let second =
            DataRuntime::new(1, "spawn-test-inner", 512 * 1024, 2).expect("second data runtime");
        let second_context = second.context();

        let ids = first_context.enter(|| {
            let outer_before = current_data_context()
                .map(|context| context.inner.id)
                .expect("outer context present");
            let inner = second_context.enter(|| {
                current_data_context()
                    .map(|context| context.inner.id)
                    .expect("inner context present")
            });
            let outer_after = current_data_context()
                .map(|context| context.inner.id)
                .expect("outer context restored");
            (outer_before, inner, outer_after)
        });

        assert_eq!(ids.0, first_context.inner.id);
        assert_eq!(ids.1, second_context.inner.id);
        assert_eq!(ids.2, first_context.inner.id);

        second.shutdown_timeout(Duration::from_secs(1));
        first.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn spawned_task_inherits_data_context_for_nested_spawn() {
        let _guard = test_lock();
        let runtime = DataRuntime::new(1, "spawn-test-inherited", 512 * 1024, 2).expect("runtime");
        let context = runtime.context();

        let name = run_on_context(&context, async move {
            spawn(async move {
                spawn(async move {
                    thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default()
                })
                .await
                .expect("inner task finished")
            })
            .await
            .expect("outer task finished")
        });

        assert_eq!(name, "spawn-test-inherited-0");
        runtime.shutdown_timeout(Duration::from_secs(1));
    }

    fn run_on_executor<T>(
        executor: DataPlaneExecutor,
        future: impl Future<Output = T> + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");
        driver.block_on(async { executor.execute(future).await.expect("data task finished") })
    }

    fn run_on_context<T>(
        context: &DataRuntimeContext,
        future: impl Future<Output = T> + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        run_on_executor(context.executor(), future)
    }

    fn spawn_thread_name(context: &DataRuntimeContext) -> String {
        run_on_context(context, async {
            thread::current()
                .name()
                .map(ToOwned::to_owned)
                .unwrap_or_default()
        })
    }

    #[test]
    fn spawn_falls_back_to_ambient_runtime_without_context() {
        let _guard = test_lock();
        let data_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("spawn-test-unused")
            .enable_all()
            .build()
            .expect("data runtime");

        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");
        let name = driver.block_on(async move {
            spawn(async move {
                thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default()
            })
            .await
            .expect("spawned task finished")
        });
        assert_ne!(name, "spawn-test-unused");

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }
}
