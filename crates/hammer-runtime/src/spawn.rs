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
//! stays isolated from business work. `RuntimeService` enters a
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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use hammer_adapter::DataPlaneRuntime;
use hammer_core::error::{HammerError, HammerResult};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::instrument::WithSubscriber;

#[derive(Clone)]
pub struct DataRuntimeContext {
    inner: Arc<DataRuntimeContextInner>,
}

struct DataRuntimeContextInner {
    id: usize,
    workers: Vec<DataRuntimeContextWorker>,
    next: AtomicUsize,
}

#[derive(Clone)]
struct DataRuntimeContextWorker {
    handle: Handle,
    local_tx: Option<tokio::sync::mpsc::UnboundedSender<LocalTaskFactory>>,
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

type LocalTask = Pin<Box<dyn Future<Output = ()> + 'static>>;
type LocalTaskFactory = Box<dyn FnOnce() -> LocalTask + Send + 'static>;

static NEXT_DATA_RUNTIME_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);

tokio::task_local! {
    static TASK_DATA_CONTEXT: DataRuntimeContext;
}

thread_local! {
    static THREAD_DATA_CONTEXT: RefCell<Vec<DataRuntimeContext>> = const { RefCell::new(Vec::new()) };
    static CURRENT_DATA_WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
    static DATA_PLANE_RUNTIME: DataPlaneRuntime =
        DataPlaneRuntime::with_buffer_capacity(DATA_BUFFER_SLOT_CAPACITY, DATA_BUFFER_SLOTS);
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
        let mut context_workers = Vec::with_capacity(worker_threads);
        let mut workers = Vec::with_capacity(worker_threads);
        for index in 0..worker_threads {
            let worker_name = format!("{thread_name}-{index}");
            let (handle_tx, handle_rx) = std::sync::mpsc::channel::<Result<Handle, String>>();
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let (local_tx, mut local_rx) =
                tokio::sync::mpsc::unbounded_channel::<LocalTaskFactory>();
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
                            let _ = handle_tx.send(Ok(runtime.handle().clone()));
                            let local_set = tokio::task::LocalSet::new();
                            let mut shutdown_rx = shutdown_rx;
                            runtime.block_on(local_set.run_until(async move {
                                loop {
                                    tokio::select! {
                                        task = local_rx.recv() => match task {
                                            Some(task) => {
                                                tokio::task::spawn_local(task());
                                            }
                                            None => break,
                                        },
                                        _ = &mut shutdown_rx => break,
                                    }
                                }
                            }));
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
            context_workers.push(DataRuntimeContextWorker {
                handle,
                local_tx: Some(local_tx),
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
        })
    }

    pub fn context(&self) -> DataRuntimeContext {
        self.context.clone()
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
                local_tx: None,
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
        let _runtime_guard = self.first_handle().enter();
        f()
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

    fn local_worker(&self) -> DataRuntimeContextWorker {
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
    let future = future.with_current_subscriber();
    match current_data_context() {
        Some(context) => {
            let handle = context.spawn_worker().handle;
            let scoped = TASK_DATA_CONTEXT.scope(context, future);
            handle.spawn(scoped)
        }
        None => tokio::spawn(future),
    }
}

pub struct DataLocalJoinHandle<T> {
    rx: tokio::sync::oneshot::Receiver<T>,
    cancel: Arc<DataLocalCancel>,
}

struct DataLocalCancel {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}

impl<T> DataLocalJoinHandle<T> {
    pub fn abort(&mut self) {
        self.cancel.cancelled.store(true, Ordering::Release);
        self.cancel.notify.notify_waiters();
    }
}

impl<T> Unpin for DataLocalJoinHandle<T> {}

impl<T> Future for DataLocalJoinHandle<T> {
    type Output = Result<T, tokio::sync::oneshot::error::RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx)
    }
}

pub fn spawn_local<F, Fut, T>(factory: F) -> DataLocalJoinHandle<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let context = current_data_context().expect("data local spawn requires a data runtime context");
    let worker = context.local_worker();
    let local_tx = worker
        .local_tx
        .expect("data local spawn requires a DataRuntime worker");
    let (tx, rx) = tokio::sync::oneshot::channel();
    let cancel = Arc::new(DataLocalCancel {
        cancelled: AtomicBool::new(false),
        notify: tokio::sync::Notify::new(),
    });
    let task_context = context.clone();
    let dispatcher = tracing::dispatcher::get_default(Clone::clone);
    let task_cancel = Arc::clone(&cancel);
    let task: LocalTaskFactory = Box::new(move || {
        Box::pin(
            TASK_DATA_CONTEXT
                .scope(task_context, async move {
                    tokio::select! {
                        output = factory() => {
                            let _ = tx.send(output);
                        }
                        _ = wait_data_local_cancel(task_cancel) => {}
                    }
                })
                .with_subscriber(dispatcher),
        )
    });
    local_tx
        .send(task)
        .expect("data local worker stopped before task was queued");
    DataLocalJoinHandle { rx, cancel }
}

async fn wait_data_local_cancel(cancel: Arc<DataLocalCancel>) {
    loop {
        if cancel.cancelled.load(Ordering::Acquire) {
            return;
        }
        cancel.notify.notified().await;
    }
}

pub fn spawn_current_local<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let context =
        current_data_context().expect("current local spawn requires a data runtime context");
    let future = future.with_current_subscriber();
    tokio::task::spawn_local(TASK_DATA_CONTEXT.scope(context, future))
}

pub fn with_data_plane_runtime<R>(f: impl FnOnce(&DataPlaneRuntime) -> R) -> R {
    DATA_PLANE_RUNTIME.with(f)
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
    use std::sync::{Arc, Mutex as StdMutex, OnceLock};
    use std::thread;
    use std::time::Duration;
    use tokio::sync::oneshot;

    static TEST_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("spawn test lock poisoned")
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
        });

        assert_eq!(thread_before, "spawn-test-current-local-0");
        assert_eq!(thread_after, "spawn-test-current-local-0");
        assert_eq!(payload, b"packet");

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
                let mut task = spawn_local(|| async {
                    std::future::pending::<()>().await;
                    1usize
                });
                task.abort();
                let result = tokio::time::timeout(Duration::from_secs(1), task)
                    .await
                    .expect("aborted local task should finish");
                assert!(
                    result.is_err(),
                    "aborted local task must not produce output"
                );
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
        });

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
