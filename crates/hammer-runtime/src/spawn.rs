//! Worker control queues and thread-owned `DataPlaneMain` access.
//!
//! The data-plane loop owns its `DataPlaneMain` directly on the worker thread.
//! Control code submits closures through `DataRemoteLocalQueue`; it does not
//! create another runtime context or execution aggregate.

use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use hammer_core::data_plane::DataPlaneBuffers;

use crate::DataPlaneMain;
use tracing::instrument::WithSubscriber;

thread_local! {
    static DATA_PLANE_MAIN: Cell<Option<*mut DataPlaneMain>> = const { Cell::new(None) };
    pub(crate) static DATA_WORKER_IDLE_SLICE: Cell<Duration> =
        const { Cell::new(Duration::from_millis(1)) };
}

pub(crate) fn apply_worker_idle_slice(idle_slice: Duration) {
    DATA_WORKER_IDLE_SLICE.with(|slot| slot.set(idle_slice));
}

#[derive(Clone)]
pub struct DataRemoteLocalQueue {
    tasks: Arc<Mutex<DataRemoteLocalQueueState>>,
    thread: Arc<Mutex<Option<thread::Thread>>>,
}

struct DataRemoteLocalQueueState {
    accepting: bool,
    capacity: usize,
    tasks: VecDeque<Box<dyn FnOnce() + Send + 'static>>,
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

    pub(crate) fn drain(&self) -> VecDeque<Box<dyn FnOnce() + Send + 'static>> {
        let mut state = self.tasks.lock().expect("remote local queue poisoned");
        std::mem::take(&mut state.tasks)
    }
}

pub(crate) fn poll_remote_local_tasks(queue: &DataRemoteLocalQueue) -> bool {
    let mut progressed = false;
    for task in queue.drain() {
        progressed = true;
        task();
    }
    progressed
}

pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future.with_current_subscriber())
}

pub(crate) fn cleanup_thread_local() {
    DATA_PLANE_MAIN.with(|slot| slot.set(None));
}

pub fn set_data_plane_main(main: &mut DataPlaneMain) {
    DATA_PLANE_MAIN.with(|slot| slot.set(Some(main as *mut DataPlaneMain)));
}

pub fn with_data_plane_main<R>(f: impl FnOnce(&DataPlaneMain) -> R) -> R {
    DATA_PLANE_MAIN.with(|slot| {
        let pointer = slot
            .get()
            .expect("data plane main not initialized on worker thread");
        // SAFETY: the pointer is installed only for the owning worker thread
        // and cleared before the worker's DataPlaneMain is dropped.
        unsafe { f(&*pointer) }
    })
}

pub fn with_data_plane_main_mut<R>(f: impl FnOnce(&mut DataPlaneMain) -> R) -> R {
    DATA_PLANE_MAIN.with(|slot| {
        let pointer = slot
            .get()
            .expect("data plane main not initialized on worker thread");
        // SAFETY: the pointer is installed only for the owning worker thread,
        // and worker control tasks execute serially on that thread.
        unsafe { f(&mut *pointer) }
    })
}

pub fn with_data_plane_buffers<R>(f: impl FnOnce(&DataPlaneBuffers) -> R) -> R {
    with_data_plane_main(|main| f(main.buffers()))
}
