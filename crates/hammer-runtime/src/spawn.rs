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

use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::instrument::WithSubscriber;

#[derive(Clone)]
pub struct DataRuntimeContext {
    handle: Handle,
}

tokio::task_local! {
    static TASK_DATA_HANDLE: Handle;
}

thread_local! {
    static THREAD_DATA_HANDLE: RefCell<Vec<Handle>> = const { RefCell::new(Vec::new()) };
}

impl DataRuntimeContext {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }

    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        THREAD_DATA_HANDLE.with(|slot| slot.borrow_mut().push(self.handle.clone()));
        let _guard = ThreadDataHandleGuard;
        let _runtime_guard = self.handle.enter();
        f()
    }
}

struct ThreadDataHandleGuard;

impl Drop for ThreadDataHandleGuard {
    fn drop(&mut self) {
        THREAD_DATA_HANDLE.with(|slot| {
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
    match current_data_handle() {
        Some(handle) => {
            let scoped = TASK_DATA_HANDLE.scope(handle.clone(), future);
            handle.spawn(scoped)
        }
        None => tokio::spawn(future),
    }
}

fn current_data_handle() -> Option<Handle> {
    TASK_DATA_HANDLE
        .try_with(Clone::clone)
        .ok()
        .or_else(|| THREAD_DATA_HANDLE.with(|slot| slot.borrow().last().cloned()))
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
