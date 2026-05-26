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

use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use hammer_adapter::BufferPool;
use hammer_core::error::{HammerError, HammerResult};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::instrument::WithSubscriber;

#[derive(Clone)]
pub struct DataRuntimeContext {
    inner: Arc<DataRuntimeContextInner>,
}

struct DataRuntimeContextInner {
    handles: Vec<Handle>,
    next: AtomicUsize,
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

tokio::task_local! {
    static TASK_DATA_CONTEXT: DataRuntimeContext;
}

thread_local! {
    static THREAD_DATA_CONTEXT: RefCell<Vec<DataRuntimeContext>> = const { RefCell::new(Vec::new()) };
    static DATA_BUFFER_POOL: BufferPool = BufferPool::with_capacity(DATA_BUFFER_SLOT_CAPACITY, DATA_BUFFER_SLOTS);
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

        let mut handles = Vec::with_capacity(worker_threads);
        let mut workers = Vec::with_capacity(worker_threads);
        for index in 0..worker_threads {
            let worker_name = format!("{thread_name}-{index}");
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
                            let _ = handle_tx.send(Ok(runtime.handle().clone()));
                            runtime.block_on(async {
                                let _ = shutdown_rx.await;
                            });
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
            handles.push(handle);
            workers.push(DataRuntimeWorker {
                shutdown_tx: Some(shutdown_tx),
                done_rx,
                join: Some(join),
            });
        }

        Ok(Self {
            context: DataRuntimeContext::new_many(handles),
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
        Self {
            inner: Arc::new(DataRuntimeContextInner {
                handles,
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
        &self.inner.handles[0]
    }

    fn next_handle(&self) -> Handle {
        let index = self.inner.next.fetch_add(1, Ordering::Relaxed);
        self.inner.handles[index % self.inner.handles.len()].clone()
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
            let handle = context.next_handle();
            let scoped = TASK_DATA_CONTEXT.scope(context, future);
            handle.spawn(scoped)
        }
        None => tokio::spawn(future),
    }
}

pub fn with_data_buffer_pool<R>(f: impl FnOnce(&BufferPool) -> R) -> R {
    DATA_BUFFER_POOL.with(f)
}

fn current_data_context() -> Option<DataRuntimeContext> {
    TASK_DATA_CONTEXT
        .try_with(Clone::clone)
        .ok()
        .or_else(|| THREAD_DATA_CONTEXT.with(|slot| slot.borrow().last().cloned()))
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
    fn data_thread_exposes_thread_local_buffer_pool() {
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
                    let stats = with_data_buffer_pool(|pool| {
                        let before = pool.in_use();
                        {
                            let _buffer = pool
                                .alloc_with_bytes(Default::default(), b"packet")
                                .expect("alloc data buffer");
                            let during = pool.in_use();
                            (before, during)
                        }
                    });
                    let after = with_data_buffer_pool(|pool| pool.in_use());
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
