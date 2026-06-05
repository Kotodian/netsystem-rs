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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use hammer_adapter::{DataPlaneBuffers, TraceControlHandle, TraceRecordSink};
use hammer_core::log::Logger;

use crate::data_plane::{RuntimeDataPlaneRuntime, new_worker_runtime};
use hammer_core::error::{HammerError, HammerResult};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
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
    barrier: Arc<DataPlaneBarrierState>,
}

#[derive(Debug)]
struct DataPlaneBarrierState {
    worker_count: usize,
    epoch: AtomicU64,
    released_epoch: AtomicU64,
    sync_count: AtomicUsize,
    lock: Mutex<DataPlaneBarrierControl>,
    condvar: Condvar,
}

#[derive(Debug, Default)]
struct DataPlaneBarrierControl {
    target_epoch: u64,
    paused_workers: usize,
    worker_epochs: Vec<u64>,
    wakers: Vec<Option<Waker>>,
}

#[derive(Clone)]
pub struct DataPlaneBarrierHandle {
    state: Arc<DataPlaneBarrierState>,
    workers: Vec<Handle>,
}

#[derive(Debug)]
pub struct DataPlaneBarrierGuard {
    state: Arc<DataPlaneBarrierState>,
    epoch: u64,
    paused_workers: usize,
}

#[derive(Clone)]
struct DataRuntimeContextWorker {
    handle: Handle,
}

pub struct DataRuntime {
    context: DataRuntimeContext,
    workers: Vec<DataRuntimeWorker>,
}

struct DataRuntimeWorker {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    done_rx: std::sync::mpsc::Receiver<()>,
    join: Option<thread::JoinHandle<()>>,
}

type DataLocalTaskFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;

static NEXT_DATA_RUNTIME_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);

tokio::task_local! {
    static TASK_DATA_CONTEXT: DataRuntimeContext;
}

thread_local! {
    static THREAD_DATA_CONTEXT: RefCell<Vec<DataRuntimeContext>> = const { RefCell::new(Vec::new()) };
    static CURRENT_DATA_WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
    static DATA_PLANE_RUNTIME: RuntimeDataPlaneRuntime =
        new_worker_runtime(DATA_BUFFER_SLOT_CAPACITY, DATA_BUFFER_SLOTS);
    static DATA_LOCAL_TASKS: RefCell<VecDeque<Rc<DataLocalTask>>> =
        const { RefCell::new(VecDeque::new()) };
    static DATA_LOCAL_DRIVER_WAKER: RefCell<Option<Waker>> = const { RefCell::new(None) };
}

const DATA_BUFFER_SLOT_CAPACITY: usize = 2048;
const DATA_BUFFER_SLOTS: usize = 4096;

impl DataRuntime {
    pub fn new(
        worker_threads: usize,
        thread_name: &str,
        thread_stack_size: usize,
        max_blocking_threads: usize,
    ) -> HammerResult<Self> {
        if worker_threads == 0 {
            return Err(HammerError::internal(
                "data runtime must have at least one worker thread",
            ));
        }

        let context_id = next_data_runtime_context_id();
        let barrier = Arc::new(DataPlaneBarrierState::new(worker_threads));
        let mut context_workers = Vec::with_capacity(worker_threads);
        let mut workers = Vec::with_capacity(worker_threads);
        for index in 0..worker_threads {
            let worker_name = format!("{thread_name}-{index}");
            let worker_barrier = Arc::clone(&barrier);
            let (handle_tx, handle_rx) = std::sync::mpsc::channel::<Result<Handle, String>>();
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let builder = thread::Builder::new()
                .name(worker_name.clone())
                .stack_size(thread_stack_size);
            let join = builder
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .max_blocking_threads(max_blocking_threads)
                        .enable_all()
                        .build();
                    match runtime {
                        Ok(runtime) => {
                            CURRENT_DATA_WORKER.with(|slot| slot.set(Some((context_id, index))));
                            drop(runtime.spawn(DataLocalDriver {
                                worker: index,
                                barrier: worker_barrier,
                            }));
                            let _ = handle_tx.send(Ok(runtime.handle().clone()));
                            runtime.block_on(async move {
                                let _ = shutdown_rx.await;
                            });
                            DATA_LOCAL_TASKS.with(|tasks| tasks.borrow_mut().clear());
                            DATA_LOCAL_DRIVER_WAKER.with(|waker| waker.borrow_mut().take());
                            CURRENT_DATA_WORKER.with(|slot| slot.set(None));
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
            context_workers.push(DataRuntimeContextWorker { handle });
            workers.push(DataRuntimeWorker {
                shutdown_tx: Some(shutdown_tx),
                done_rx,
                join: Some(join),
            });
        }

        Ok(Self {
            context: DataRuntimeContext::from_workers_with_barrier(
                context_id,
                context_workers,
                barrier,
            ),
            workers,
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
            state: Arc::clone(&self.context.inner.barrier),
            workers: self
                .context
                .inner
                .workers
                .iter()
                .map(|worker| worker.handle.clone())
                .collect(),
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
            .map(|handle| DataRuntimeContextWorker { handle })
            .collect();
        Self::from_workers(next_data_runtime_context_id(), workers)
    }

    fn from_workers(id: usize, workers: Vec<DataRuntimeContextWorker>) -> Self {
        let worker_count = workers.len();
        Self::from_workers_with_barrier(
            id,
            workers,
            Arc::new(DataPlaneBarrierState::new(worker_count)),
        )
    }

    fn from_workers_with_barrier(
        id: usize,
        workers: Vec<DataRuntimeContextWorker>,
        barrier: Arc<DataPlaneBarrierState>,
    ) -> Self {
        assert!(
            !workers.is_empty(),
            "data runtime context requires at least one worker"
        );
        Self {
            inner: Arc::new(DataRuntimeContextInner {
                id,
                workers,
                next: AtomicUsize::new(0),
                barrier,
            }),
        }
    }

    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        THREAD_DATA_CONTEXT.with(|slot| slot.borrow_mut().push(self.clone()));
        let _guard = ThreadDataContextGuard;
        let _runtime_guard = self.first_handle().enter();
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

    pub fn set_trace_control_on_workers(
        &self,
        control: Option<TraceControlHandle>,
        packet_capacity: usize,
    ) -> HammerResult<()> {
        let _ = packet_capacity;
        self.for_each_worker(move |_| {
            DATA_PLANE_RUNTIME.with(|runtime| {
                runtime.set_trace_control(control.clone(), 0);
            });
        })
        .map(|_| ())
    }

    pub fn drain_trace_records_on_workers(&self, sink: TraceRecordSink) -> HammerResult<usize> {
        self.for_each_worker(move |_| sink.drain_completed())
            .map(|counts| counts.into_iter().sum())
    }

    pub fn drain_trace_records_on_workers_with_logger(
        &self,
        sink: TraceRecordSink,
        logger: Logger,
    ) -> HammerResult<usize> {
        self.for_each_worker(move |_| sink.drain_completed_with_logger(&logger))
            .map(|counts| counts.into_iter().sum())
    }

    fn first_handle(&self) -> &Handle {
        &self.inner.workers[0].handle
    }

    fn spawn_worker(&self) -> DataRuntimeContextWorker {
        if let Some(index) = self.current_worker_index() {
            return self.inner.workers[index].clone();
        }
        self.next_worker()
    }

    fn next_worker(&self) -> DataRuntimeContextWorker {
        let index = self.inner.next.fetch_add(1, Ordering::Relaxed);
        self.inner.workers[index % self.inner.workers.len()].clone()
    }

    fn current_worker_index(&self) -> Option<usize> {
        CURRENT_DATA_WORKER.with(|slot| match slot.get() {
            Some((id, index)) if id == self.inner.id && index < self.inner.workers.len() => {
                Some(index)
            }
            _ => None,
        })
    }
}

impl DataPlaneBarrierState {
    fn new(worker_count: usize) -> Self {
        Self {
            worker_count,
            epoch: AtomicU64::new(0),
            released_epoch: AtomicU64::new(0),
            sync_count: AtomicUsize::new(0),
            lock: Mutex::new(DataPlaneBarrierControl {
                target_epoch: 0,
                paused_workers: 0,
                worker_epochs: vec![0; worker_count],
                wakers: vec![None; worker_count],
            }),
            condvar: Condvar::new(),
        }
    }

    fn worker_poll(&self, worker: usize, cx: &mut Context<'_>) -> Poll<()> {
        let mut control = self.lock.lock().expect("data plane barrier lock poisoned");
        control.wakers[worker] = Some(cx.waker().clone());
        let target = self.epoch.load(Ordering::Acquire);
        if target == self.released_epoch.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        if control.target_epoch != target {
            return Poll::Ready(());
        }
        if control.worker_epochs[worker] != target {
            control.worker_epochs[worker] = target;
            control.paused_workers += 1;
            self.condvar.notify_all();
        }
        if target == self.released_epoch.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl DataPlaneBarrierHandle {
    pub fn sync(&self) -> HammerResult<DataPlaneBarrierGuard> {
        let epoch = self.state.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.state.sync_count.fetch_add(1, Ordering::SeqCst);
        let wakers = {
            let mut control = self
                .state
                .lock
                .lock()
                .map_err(|_| HammerError::internal("data plane barrier lock poisoned"))?;
            control.target_epoch = epoch;
            control.paused_workers = 0;
            for worker_epoch in &mut control.worker_epochs {
                *worker_epoch = 0;
            }
            control
                .wakers
                .iter()
                .filter_map(|waker| waker.as_ref().cloned())
                .collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
        for worker in &self.workers {
            drop(worker.spawn(async {}));
        }
        let mut control = self
            .state
            .lock
            .lock()
            .map_err(|_| HammerError::internal("data plane barrier lock poisoned"))?;
        while control.paused_workers < self.state.worker_count {
            control = self
                .state
                .condvar
                .wait(control)
                .map_err(|_| HammerError::internal("data plane barrier lock poisoned"))?;
        }
        Ok(DataPlaneBarrierGuard {
            state: Arc::clone(&self.state),
            epoch,
            paused_workers: control.paused_workers,
        })
    }

    #[inline]
    pub fn synchronize<R>(&self, operation: impl FnOnce() -> HammerResult<R>) -> HammerResult<R> {
        let _guard = self.sync()?;
        operation()
    }

    #[inline]
    pub fn sync_count(&self) -> usize {
        self.state.sync_count.load(Ordering::SeqCst)
    }
}

impl DataPlaneBarrierGuard {
    #[inline]
    pub fn paused_workers(&self) -> usize {
        self.paused_workers
    }
}

impl Drop for DataPlaneBarrierGuard {
    fn drop(&mut self) {
        self.state
            .released_epoch
            .store(self.epoch, Ordering::Release);
        let mut control = self
            .state
            .lock
            .lock()
            .expect("data plane barrier lock poisoned");
        for waker in &mut control.wakers {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }
        self.state.condvar.notify_all();
    }
}

impl DataPlaneExecutor {
    pub fn execute<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let future = future.with_current_subscriber();
        let worker = self.context.spawn_worker();
        let scoped = TASK_DATA_CONTEXT.scope(self.context.clone(), future);
        worker.handle.spawn(scoped)
    }
}

struct DataLocalDriver {
    worker: usize,
    barrier: Arc<DataPlaneBarrierState>,
}

impl Future for DataLocalDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        DATA_LOCAL_DRIVER_WAKER.with(|waker| {
            *waker.borrow_mut() = Some(cx.waker().clone());
        });
        if self.barrier.worker_poll(self.worker, cx).is_pending() {
            return Poll::Pending;
        }
        poll_data_plane_nodes(cx);
        poll_data_local_tasks(cx);
        poll_data_plane_nodes(cx);
        Poll::Pending
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

fn poll_data_local_tasks(cx: &mut Context<'_>) {
    let initial_len = DATA_LOCAL_TASKS.with(|queue| queue.borrow().len());
    for _ in 0..initial_len {
        let Some(task) = DATA_LOCAL_TASKS.with(|queue| queue.borrow_mut().pop_front()) else {
            break;
        };
        if task.poll(cx) {
            DATA_LOCAL_TASKS.with(|queue| queue.borrow_mut().push_back(task));
        }
    }
}

fn poll_data_plane_nodes(cx: &mut Context<'_>) {
    loop {
        let ready = with_data_plane_runtime(|runtime| {
            let mut ready = runtime.nodes().ready();
            Pin::new(&mut ready).poll(cx)
        });
        if !matches!(ready, Poll::Ready(())) {
            break;
        }
        let result = with_data_plane_runtime(|runtime| runtime.run_ready_nodes());
        match result {
            Ok(0) => continue,
            Ok(_) => {}
            Err(err) => {
                tracing::debug!("data plane node scheduler failed: {err}");
                break;
            }
        }
    }
}

fn wake_data_local_driver() {
    DATA_LOCAL_DRIVER_WAKER.with(|waker| {
        if let Some(waker) = waker.borrow().as_ref().cloned() {
            waker.wake();
        }
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
    match current_data_context() {
        Some(context) => context.executor().execute(future),
        None => tokio::spawn(future.with_current_subscriber()),
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

pub(crate) fn with_data_plane_runtime<R>(f: impl FnOnce(&RuntimeDataPlaneRuntime) -> R) -> R {
    DATA_PLANE_RUNTIME.with(f)
}

pub fn with_data_plane_buffers<R>(f: impl FnOnce(&DataPlaneBuffers) -> R) -> R {
    DATA_PLANE_RUNTIME.with(|runtime| f(runtime.packet_buffers()))
}

fn current_data_context() -> Option<DataRuntimeContext> {
    TASK_DATA_CONTEXT
        .try_with(Clone::clone)
        .ok()
        .or_else(|| THREAD_DATA_CONTEXT.with(|slot| slot.borrow().last().cloned()))
}

fn next_data_runtime_context_id() -> usize {
    NEXT_DATA_RUNTIME_CONTEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_adapter::{
        NodeId, TraceControlPlane, TraceEntry, TraceInputPolicy, TracePolicy, TraceRecord,
    };
    use hammer_core::log::{Factory, Level, LogWriter};
    use std::sync::{Arc, Mutex as StdMutex, OnceLock};
    use std::thread;
    use std::time::Duration;
    use std::time::Instant as StdInstant;
    use tokio::sync::oneshot;

    static TEST_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("spawn test lock poisoned")
    }

    struct CaptureWriter {
        lines: StdMutex<std::vec::Vec<(Level, String)>>,
    }

    impl LogWriter for CaptureWriter {
        fn write_message(&self, level: Level, message: String) {
            self.lines
                .lock()
                .expect("capture writer poisoned")
                .push((level, message));
        }
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

        let names = context.enter(|| {
            driver.block_on(async {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(8);
                for _ in 0..8 {
                    let tx = tx.clone();
                    drop(spawn(async move {
                        let name = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let _ = tx.send(name).await;
                    }));
                }
                drop(tx);
                let mut names = Vec::new();
                while let Some(name) = rx.recv().await {
                    names.push(name);
                }
                names
            })
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
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let (before, during, after) = context.enter(|| {
            driver.block_on(async {
                let (tx, rx) = oneshot::channel();
                drop(spawn(async move {
                    let stats = with_data_plane_runtime(|runtime| {
                        let before = runtime.in_use_buffers();
                        {
                            let index = runtime
                                .alloc_index_with_bytes(Default::default(), b"packet")
                                .expect("alloc data buffer");
                            let during = runtime.in_use_buffers();
                            runtime.free_index(index);
                            (before, during)
                        }
                    });
                    let after = with_data_plane_runtime(|runtime| runtime.in_use_buffers());
                    let _ = tx.send((stats.0, stats.1, after));
                }));
                tokio::time::timeout(Duration::from_secs(2), rx)
                    .await
                    .expect("buffer task timed out")
                    .expect("buffer task dropped sender")
            })
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
            }],
        });

        context
            .set_trace_control_on_workers(Some(control.handle()), 2)
            .expect("set trace control");
        let marks = context
            .for_each_worker(|_| {
                DATA_PLANE_RUNTIME.with(|runtime| {
                    let index = runtime
                        .alloc_index_with_bytes(Default::default(), b"packet")
                        .expect("alloc packet");
                    runtime
                        .try_mark_trace(NodeId::new(0), index)
                        .expect("disabled trace mark is no-op");
                    let marked = runtime
                        .get_buffer(index)
                        .expect("buffer")
                        .trace_mark()
                        .is_some();
                    runtime.free_index(index);
                    marked
                })
            })
            .expect("inspect workers");

        assert_eq!(marks, vec![false, false]);

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_context_drains_trace_records_through_logger() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-trace-drain", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let control = TraceControlPlane::new(4);
        control.handle().push_completed_record(TraceRecord {
            epoch: 1,
            input_node: NodeId::new(0),
            input_node_name: Some("trace-input"),
            entries: vec![TraceEntry {
                node: NodeId::new(1),
                node_name: Some("trace-node"),
                payload_bytes: b"test".to_vec(),
                formatter: None,
            }],
        });
        let writer = Arc::new(CaptureWriter {
            lines: StdMutex::new(std::vec::Vec::new()),
        });
        let logger = Factory::new(StdInstant::now(), writer.clone()).new_logger("trace-control");

        let drained = context
            .drain_trace_records_on_workers_with_logger(control.sink(), logger)
            .expect("drain trace records");

        assert_eq!(drained, 1);
        let lines = writer.lines.lock().expect("capture writer poisoned");
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].0, Level::Debug);
        assert!(
            lines[0]
                .1
                .contains("packet trace epoch=1 input=trace-input")
        );
        assert!(lines[0].1.contains("trace-node: 0x74657374"));

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_runtime_local_spawn_keeps_non_send_buffer_on_worker_thread() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let (thread_before, thread_after, before, during, after, payload) = context.enter(|| {
            driver.block_on(async {
                spawn(async {
                    spawn_local(|| async {
                        let thread_before = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let buffer = with_data_plane_runtime(|runtime| {
                            let before = runtime.in_use_buffers();
                            let buffer = runtime
                                .alloc_index_with_bytes(Default::default(), b"packet")
                                .expect("alloc local data buffer");
                            let during = runtime.in_use_buffers();
                            (before, during, buffer)
                        });
                        tokio::task::yield_now().await;
                        let thread_after = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let payload = with_data_plane_runtime(|runtime| {
                            let payload = runtime
                                .copy_current(buffer.2)
                                .expect("copy local data buffer");
                            runtime.free_index(buffer.2);
                            payload
                        });
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
            })
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
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let (thread_before, thread_after, payload) = context.enter(|| {
            driver.block_on(async {
                spawn(async {
                    spawn_local(|| async {
                        let join = spawn_current_local(async {
                            let thread_before = thread::current()
                                .name()
                                .map(ToOwned::to_owned)
                                .unwrap_or_default();
                            let buffer = with_data_plane_runtime(|runtime| {
                                runtime
                                    .alloc_index_with_bytes(Default::default(), b"packet")
                                    .expect("alloc local data buffer")
                            });
                            tokio::task::yield_now().await;
                            let thread_after = thread::current()
                                .name()
                                .map(ToOwned::to_owned)
                                .unwrap_or_default();
                            let payload = with_data_plane_runtime(|runtime| {
                                let payload = runtime
                                    .copy_current(buffer)
                                    .expect("copy current local data buffer");
                                runtime.free_index(buffer);
                                payload
                            });
                            (thread_before, thread_after, payload)
                        });
                        join.await.expect("current local task joined")
                    })
                    .await
                    .expect("outer local data task finished")
                })
                .await
                .expect("data task finished")
            })
        });

        assert_eq!(thread_before, "spawn-test-current-local-0");
        assert_eq!(thread_after, "spawn-test-current-local-0");
        assert_eq!(payload, b"packet");

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn frame_pending_future_resumes_on_current_data_worker() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-frame", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let (before_thread, after_thread, payload) = context.enter(|| {
            driver.block_on(async {
                spawn(async {
                    spawn_local(|| async {
                        let runtime = with_data_plane_buffers(Clone::clone);
                        let frame = std::rc::Rc::new(RefCell::new(
                            runtime.alloc_pooled_frame().expect("alloc pooled frame"),
                        ));
                        let index = runtime
                            .alloc_index_with_bytes(Default::default(), b"packet")
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
                            let buffer = consumer_frame
                                .borrow_mut()
                                .drain_pending()
                                .next()
                                .expect("pending buffer");
                            let payload = consumer_runtime
                                .copy_current(buffer)
                                .expect("copy pending buffer");
                            consumer_runtime.free_index(buffer);
                            (before_thread, after_thread, payload)
                        });
                        let producer_frame = std::rc::Rc::clone(&frame);
                        let producer = spawn_current_local(async move {
                            tokio::task::yield_now().await;
                            producer_frame
                                .borrow_mut()
                                .push_index(index)
                                .expect("push pending buffer");
                        });
                        producer.await.expect("producer joined");
                        let result = consumer.await.expect("consumer joined");
                        let frame = std::rc::Rc::try_unwrap(frame)
                            .expect("frame has no remaining references")
                            .into_inner();
                        runtime
                            .release_pooled_frame(frame)
                            .expect("release pooled frame");
                        result
                    })
                    .await
                    .expect("local frame task finished")
                })
                .await
                .expect("data task finished")
            })
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
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let (parent_thread, child_thread) = context.enter(|| {
            driver.block_on(async {
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
            })
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
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let (parent_thread, child_thread) = context.enter(|| {
            driver.block_on(async {
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
            })
        });

        assert_eq!(parent_thread, child_thread);
        assert!(parent_thread.starts_with("spawn-test-nested-"));

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_runtime_barrier_pauses_all_workers_until_guard_is_released() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(2, "spawn-test-barrier", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let barrier = data_runtime.data_plane_barrier();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, _) = tokio::sync::broadcast::channel::<()>(2);
        context
            .for_each_worker({
                let entered_tx = entered_tx.clone();
                let release_tx = release_tx.clone();
                move |index| {
                    let entered_tx = entered_tx.clone();
                    let mut release_rx = release_tx.subscribe();
                    drop(spawn_current_local(async move {
                        entered_tx.send(index).expect("send entered worker");
                        tokio::task::yield_now().await;
                        release_rx.recv().await.expect("release worker");
                    }));
                }
            })
            .expect("spawn pinned worker tasks");
        for _ in 0..2 {
            entered_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("worker task entered");
        }

        let guard = driver.block_on(async {
            tokio::task::spawn_blocking(move || barrier.sync())
                .await
                .expect("barrier task joined")
                .expect("barrier sync")
        });

        assert_eq!(guard.paused_workers(), 2);
        drop(guard);
        release_tx.send(()).expect("release workers");

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
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        context.enter(|| {
            driver.block_on(async {
                spawn(async {
                    let mut task = spawn_local(|| async {
                        std::future::pending::<()>().await;
                        1usize
                    });
                    task.abort();
                    let result = tokio::time::timeout(Duration::from_secs(1), task)
                        .await
                        .expect("aborted local task should finish");
                    let err = result.expect_err("aborted local task must not produce output");
                    assert!(err.is_cancelled(), "{err}");
                })
                .await
                .expect("data task finished")
            })
        });

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn dropping_data_local_handle_does_not_abort_task() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-drop-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        context.enter(|| {
            driver.block_on(async {
                spawn(async {
                    let (tx, rx) = oneshot::channel();
                    let task = spawn_local(move || async move {
                        tokio::task::yield_now().await;
                        let _ = tx.send(());
                    });
                    drop(task);
                    tokio::time::timeout(Duration::from_secs(1), rx)
                        .await
                        .expect("dropped local handle should not abort task")
                        .expect("local task should send completion");
                })
                .await
                .expect("data task finished")
            })
        });

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_local_spawn_panic_returns_error_without_stopping_driver() {
        let _guard = test_lock();
        let data_runtime =
            DataRuntime::new(1, "spawn-test-panic-local", 512 * 1024, 2).expect("data runtime");
        let context = data_runtime.context();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let output = context.enter(|| {
            driver.block_on(async {
                spawn(async {
                    let panicking = spawn_local(|| async {
                        panic!("local task panic");
                    });
                    let result = tokio::time::timeout(Duration::from_secs(1), panicking)
                        .await
                        .expect("panicking local task should finish");
                    let err = result.expect_err("panic should be reported as join error");
                    assert!(err.is_panic(), "{err}");

                    spawn_local(|| async { 42usize })
                        .await
                        .expect("driver should continue after panic")
                })
                .await
                .expect("data task finished")
            })
        });

        assert_eq!(output, 42);

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn spawn_targets_current_data_context() {
        let _guard = test_lock();
        let first = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("spawn-test-first")
            .enable_all()
            .build()
            .expect("first runtime");
        let first_context = DataRuntimeContext::new(first.handle().clone());
        let second = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("spawn-test-second")
            .enable_all()
            .build()
            .expect("second runtime");
        let second_context = DataRuntimeContext::new(second.handle().clone());

        let first_name = spawn_thread_name(&first_context);
        let second_name = spawn_thread_name(&second_context);

        assert_eq!(first_name, "spawn-test-first");
        assert_eq!(second_name, "spawn-test-second");

        second.shutdown_timeout(Duration::from_secs(1));
        first.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn data_context_enters_tokio_runtime_handle() {
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
            let current = data_context.enter(Handle::current);
            assert_eq!(
                current.runtime_flavor(),
                data_runtime.handle().runtime_flavor()
            );
        });

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn nested_data_context_restores_outer_context() {
        let _guard = test_lock();
        let first = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("spawn-test-outer")
            .enable_all()
            .build()
            .expect("first runtime");
        let first_context = DataRuntimeContext::new(first.handle().clone());
        let second = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("spawn-test-inner")
            .enable_all()
            .build()
            .expect("second runtime");
        let second_context = DataRuntimeContext::new(second.handle().clone());

        let names = first_context.enter(|| {
            let outer_before = spawn_thread_name_without_enter();
            let inner = second_context.enter(spawn_thread_name_without_enter);
            let outer_after = spawn_thread_name_without_enter();
            (outer_before, inner, outer_after)
        });

        assert_eq!(names.0, "spawn-test-outer");
        assert_eq!(names.1, "spawn-test-inner");
        assert_eq!(names.2, "spawn-test-outer");

        second.shutdown_timeout(Duration::from_secs(1));
        first.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn spawned_task_inherits_data_context_for_nested_spawn() {
        let _guard = test_lock();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("spawn-test-inherited")
            .enable_all()
            .build()
            .expect("runtime");
        let context = DataRuntimeContext::new(runtime.handle().clone());

        let (tx, rx) = oneshot::channel::<String>();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");
        context.enter(|| {
            driver.block_on(async move {
                drop(spawn(async move {
                    let (inner_tx, inner_rx) = oneshot::channel::<String>();
                    drop(spawn(async move {
                        let name = thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default();
                        let _ = inner_tx.send(name);
                    }));
                    let name = inner_rx.await.expect("inner sender dropped");
                    let _ = tx.send(name);
                }));
                tokio::time::timeout(Duration::from_secs(2), rx)
                    .await
                    .expect("spawned task timed out")
                    .expect("spawned task dropped sender")
            })
        });

        runtime.shutdown_timeout(Duration::from_secs(1));
    }

    fn spawn_thread_name(context: &DataRuntimeContext) -> String {
        context.enter(spawn_thread_name_without_enter)
    }

    fn spawn_thread_name_without_enter() -> String {
        let observed: Arc<StdMutex<Option<String>>> = Arc::new(StdMutex::new(None));
        let observed_clone = Arc::clone(&observed);
        let (tx, rx) = oneshot::channel::<()>();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");
        driver.block_on(async move {
            drop(spawn(async move {
                let name = thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                *observed_clone.lock().expect("observed lock") = Some(name);
                let _ = tx.send(());
            }));
            tokio::time::timeout(Duration::from_secs(2), rx)
                .await
                .expect("spawned task timed out")
                .expect("spawned task dropped sender");
        });

        let name = observed.lock().expect("observed lock").clone();
        name.expect("thread name missing")
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

        let (tx, rx) = oneshot::channel::<String>();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");
        let name = driver.block_on(async move {
            drop(spawn(async move {
                let name = thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let _ = tx.send(name);
            }));
            tokio::time::timeout(Duration::from_secs(2), rx)
                .await
                .expect("spawned task timed out")
                .expect("spawned task dropped sender")
        });
        assert_ne!(name, "spawn-test-unused");

        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }
}
