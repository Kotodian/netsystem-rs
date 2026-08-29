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
//! ambient runtime via `tokio::spawn` and use the process default subscriber.
//!
//! Use `crate::spawn::spawn(future)` everywhere we'd otherwise call
//! `tokio::spawn`. Forgetting it does not corrupt routing — it only causes
//! the task to lose its service-specific dispatch — but it should still be
//! considered a bug.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use hammer_core::data_plane::DataPlaneBuffers;
use hammer_runtime::{TraceControlHandle, TraceRecordSink};

use crate::config::Worker;
use crate::data_plane::RuntimeDataPlaneRuntime;
use crate::error::{RuntimeError, RuntimeResult};
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
    barrier: crate::barrier::WorkerBarrier,
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

fn init_data_plane_runtime(config: &Worker) -> RuntimeResult<()> {
    DATA_PLANE_RUNTIME.with(|runtime| {
        if runtime.borrow().is_none() {
            *runtime.borrow_mut() = Some(config.create_runtime()?);
        }
        Ok(())
    })
}

#[derive(Clone)]
pub struct DataRemoteLocalQueue {
    tasks: Arc<Mutex<DataRemoteLocalQueueState>>,
    thread: Arc<Mutex<Option<thread::Thread>>>,
}

struct DataRemoteLocalQueueState {
    accepting: bool,
    capacity: usize,
    tasks: VecDeque<RemoteDataLocalTask>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DataRemoteLocalQueueError {
    Closed,
    Full { capacity: usize },
}

impl Default for DataRemoteLocalQueue {
    fn default() -> Self {
        Self::new(1_024)
    }
}

impl DataRemoteLocalQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        assert_ne!(capacity, 0, "remote-local queue capacity must be non-zero");
        Self {
            tasks: Arc::new(Mutex::new(DataRemoteLocalQueueState {
                accepting: false,
                capacity,
                tasks: VecDeque::with_capacity(capacity),
            })),
            thread: Arc::new(Mutex::new(None)),
        }
    }
}

impl DataRuntime {
    pub fn new(
        worker_threads: usize,
        thread_name: &str,
        thread_stack_size: usize,
        max_blocking_threads: usize,
    ) -> RuntimeResult<Self> {
        let mut worker = Worker::default();
        worker.count = worker_threads;
        worker.stack_size = thread_stack_size;
        worker.max_blocking_threads = max_blocking_threads;
        Self::from_config(&worker, thread_name)
    }

    pub fn from_config(worker: &Worker, thread_name: &str) -> RuntimeResult<Self> {
        worker.validate().map_err(RuntimeError::from)?;
        Self::spawn_workers(worker, thread_name)
    }

    fn spawn_workers(worker: &Worker, thread_name: &str) -> RuntimeResult<Self> {
        let worker_threads = worker.count;
        let thread_stack_size = worker.stack_size;
        let max_blocking_threads = worker.max_blocking_threads;
        let idle_slice = worker.idle_slice;
        let worker_config = worker.clone();

        let context_id = next_data_runtime_context_id();
        let worker_count =
            u32::try_from(worker_threads).map_err(|_| RuntimeError::WorkerCountOverflow {
                count: worker_threads,
            })?;
        let barrier = crate::barrier::WorkerBarrier::new(worker_count);
        let mut context_workers = Vec::with_capacity(worker_threads);
        let mut workers = Vec::with_capacity(worker_threads);
        for index in 0..worker_threads {
            let worker_name = format!("{thread_name}-{index}");
            let remote_local = DataRemoteLocalQueue::new(worker.control.queue_capacity);
            let worker_remote_local = remote_local.clone();
            let worker_config = worker_config.clone();
            let worker_barrier = barrier.clone();
            let (handle_tx, handle_rx) = std::sync::mpsc::channel::<RuntimeResult<Handle>>();
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let builder = thread::Builder::new()
                .name(worker_name.clone())
                .stack_size(thread_stack_size);
            let join = builder
                .spawn(move || {
                    if let Err(error) = worker_config.apply_current_thread_setup(index) {
                        let _ = handle_tx.send(Err(RuntimeError::DataWorkerThreadSetup {
                            worker: index,
                            source: Box::new(error),
                        }));
                        let _ = done_tx.send(());
                        return;
                    }
                    DATA_WORKER_IDLE_SLICE.with(|slot| slot.set(idle_slice));
                    if let Err(error) = init_data_plane_runtime(&worker_config) {
                        let _ =
                            handle_tx.send(Err(RuntimeError::DataWorkerRuntimeInitialization {
                                worker: index,
                                source: Box::new(error),
                            }));
                        let _ = done_tx.send(());
                        return;
                    }
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
                                &worker_barrier,
                            );
                            worker_remote_local.close();
                            DATA_LOCAL_TASKS.with(|tasks| tasks.borrow_mut().clear());
                            DATA_LOCAL_DRIVER_WAKER.with(|waker| waker.borrow_mut().take());
                            CURRENT_DATA_WORKER.with(|slot| slot.set(None));
                            DATA_PLANE_RUNTIME.with(|slot| *slot.borrow_mut() = None);
                        }
                        Err(err) => {
                            let _ = handle_tx.send(Err(RuntimeError::DataWorkerRuntimeBuild {
                                worker: index,
                                source: err,
                            }));
                        }
                    }
                    let _ = done_tx.send(());
                })
                .map_err(|source| RuntimeError::DataWorkerThreadSpawn {
                    worker: index,
                    source,
                })?;

            let handle = handle_rx
                .recv()
                .map_err(|_| RuntimeError::DataWorkerStartupCanceled { worker: index })??;
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
            context: DataRuntimeContext::from_workers(context_id, context_workers),
            workers,
            barrier,
        })
    }

    pub fn context(&self) -> DataRuntimeContext {
        self.context.clone()
    }

    pub fn executor(&self) -> DataPlaneExecutor {
        self.context.executor()
    }

    pub fn barrier(&self) -> crate::barrier::WorkerBarrier {
        self.barrier.clone()
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

    pub fn for_each_worker<F, R>(&self, f: F) -> RuntimeResult<Vec<R>>
    where
        F: Fn(usize) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        let f = Arc::new(f);
        let mut receivers = Vec::with_capacity(self.inner.workers.len());
        for (index, worker) in self.inner.workers.iter().cloned().enumerate() {
            let (tx, rx) = std::sync::mpsc::channel();
            let f = Arc::clone(&f);
            let context = self.clone();
            drop(
                worker
                    .handle
                    .spawn(TASK_DATA_CONTEXT.scope(context, async move {
                        let _ = tx.send(f(index));
                    })),
            );
            receivers.push((index, rx));
        }

        let mut results = Vec::with_capacity(receivers.len());
        for (worker, rx) in receivers {
            results.push(
                rx.recv()
                    .map_err(|_| RuntimeError::DataWorkerResultCanceled { worker })?,
            );
        }
        Ok(results)
    }

    pub fn install_on_workers<F, R>(&self, f: F) -> RuntimeResult<Vec<R>>
    where
        F: Fn(usize, &DataPlaneRuntime) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        self.for_each_worker(move |worker| with_data_plane_runtime(|runtime| f(worker, runtime)))
    }

    pub fn set_trace_control_on_workers(
        &self,
        control: Option<TraceControlHandle>,
    ) -> RuntimeResult<()> {
        self.for_each_worker(move |_| {
            with_data_plane_runtime(|runtime| {
                runtime.set_trace_control(control.clone());
            });
        })
        .map(|_| ())
    }

    pub fn drain_trace_records_on_workers(&self, sink: TraceRecordSink) -> RuntimeResult<usize> {
        self.for_each_worker(move |_| sink.drain_completed())
            .map(|counts| counts.into_iter().sum())
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.inner.workers.len()
    }

    pub fn spawn_local_on_worker<F, Fut>(&self, worker: usize, factory: F) -> RuntimeResult<()>
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
    ) -> RuntimeResult<T>
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
                    let result = join
                        .await
                        .map_err(|source| RuntimeError::DataWorkerLocalTask { worker, source });
                    complete_state.complete(result);
                }));
            }),
        )?;
        DataRemoteJoinHandle { state }.await
    }

    pub(crate) fn call_blocking_on_worker<R>(
        &self,
        worker: usize,
        f: impl FnOnce() -> RuntimeResult<R> + Send + 'static,
    ) -> RuntimeResult<R>
    where
        R: Send + 'static,
    {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        self.schedule_local_on_worker(
            worker,
            Box::new(move || {
                let result = match catch_unwind(AssertUnwindSafe(f)) {
                    Ok(result) => result,
                    Err(_) => Err(RuntimeError::DataWorkerCallPanicked { worker }),
                };
                let _ = done_tx.send(result);
            }),
        )?;
        done_rx
            .recv()
            .map_err(|_| RuntimeError::DataWorkerCallCanceled { worker })?
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
    ) -> RuntimeResult<()> {
        if worker >= self.inner.workers.len() {
            return Err(RuntimeError::DataWorkerIndexOutOfRange {
                worker,
                worker_count: self.inner.workers.len(),
            });
        }
        self.inner.workers[worker]
            .remote_local
            .push(task)
            .map_err(|error| match error {
                DataRemoteLocalQueueError::Closed => RuntimeError::WorkerControlClosed {
                    worker: crate::DataWorkerId::new(
                        u32::try_from(worker).expect("worker index fits u32"),
                    ),
                },
                DataRemoteLocalQueueError::Full { capacity } => {
                    RuntimeError::WorkerControlQueueFull {
                        worker: crate::DataWorkerId::new(
                            u32::try_from(worker).expect("worker index fits u32"),
                        ),
                        capacity,
                    }
                }
            })
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

impl DataRemoteLocalQueue {
    pub fn attach_current_thread(&self) {
        self.tasks
            .lock()
            .expect("remote local queue poisoned")
            .accepting = true;
        *self
            .thread
            .lock()
            .expect("remote local thread handle poisoned") = Some(thread::current());
    }

    pub(crate) fn push(
        &self,
        task: impl FnOnce() + Send + 'static,
    ) -> Result<(), DataRemoteLocalQueueError> {
        let mut state = self.tasks.lock().expect("remote local queue poisoned");
        if !state.accepting {
            return Err(DataRemoteLocalQueueError::Closed);
        }
        if state.tasks.len() == state.capacity {
            return Err(DataRemoteLocalQueueError::Full {
                capacity: state.capacity,
            });
        }
        state.tasks.push_back(Box::new(task));
        drop(state);
        if let Some(thread) = self
            .thread
            .lock()
            .expect("remote local thread handle poisoned")
            .as_ref()
            .cloned()
        {
            thread.unpark();
        }
        Ok(())
    }

    pub(crate) fn close(&self) {
        let mut state = self.tasks.lock().expect("remote local queue poisoned");
        state.accepting = false;
        let tasks = std::mem::take(&mut state.tasks);
        drop(state);
        *self
            .thread
            .lock()
            .expect("remote local thread handle poisoned") = None;
        drop(tasks);
    }

    pub(crate) fn drain(&self) -> VecDeque<RemoteDataLocalTask> {
        let mut state = self.tasks.lock().expect("remote local queue poisoned");
        std::mem::take(&mut state.tasks)
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
    result: Mutex<Option<RuntimeResult<T>>>,
    waker: Mutex<Option<Waker>>,
}

impl<T> DataRemoteJoinState<T> {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            waker: Mutex::new(None),
        }
    }

    fn complete(&self, result: RuntimeResult<T>) {
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
    type Output = RuntimeResult<T>;

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
    barrier: &crate::barrier::WorkerBarrier,
) {
    let worker_waker = Waker::from(Arc::new(DataWorkerThreadWake {
        thread: thread::current(),
    }));
    DATA_LOCAL_DRIVER_WAKER.with(|slot| {
        *slot.borrow_mut() = Some(worker_waker.clone());
    });

    loop {
        match shutdown_rx.try_recv() {
            Ok(_) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
        }

        let local_progress = poll_data_worker_once(remote_local, &worker_waker, barrier);
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
    barrier: &crate::barrier::WorkerBarrier,
) -> bool {
    let mut cx = Context::from_waker(worker_waker);
    DATA_LOCAL_DRIVER_WAKER.with(|slot| {
        *slot.borrow_mut() = Some(worker_waker.clone());
    });

    barrier.check();

    let mut progressed = false;
    progressed |= match with_data_plane_runtime(|runtime| -> RuntimeResult<usize> {
        let pre_input = runtime.schedule_polling_pre_input_nodes()?;
        let drivers = runtime.schedule_polling_driver_nodes()?;
        Ok(pre_input + drivers)
    }) {
        Ok(scheduled) => scheduled > 0,
        Err(err) => {
            tracing::debug!("data plane polling node scheduler failed: {err}");
            false
        }
    };
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
