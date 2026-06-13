use std::cell::RefCell;
use std::thread::LocalKey;

use hammer_adapter::NodeRuntimeData;
use hammer_core::error::{CoreError, CoreResult};

use crate::session::worker::SessionQueueRuntime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionQueueHandle {
    runtime_data: NodeRuntimeData,
}

impl SessionQueueHandle {
    #[inline]
    pub(in crate::session) const fn new(runtime_data: NodeRuntimeData) -> Self {
        Self { runtime_data }
    }

    #[inline]
    pub(in crate::session) const fn runtime_data(self) -> NodeRuntimeData {
        self.runtime_data
    }
}

pub(crate) fn register_session_queue_runtime<P>(
    store: &'static LocalKey<RefCell<hammer_infra::vec::Vec<SessionQueueRuntime<P>>>>,
    runtime: SessionQueueRuntime<P>,
) -> CoreResult<SessionQueueHandle> {
    store.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        let runtime_data = NodeRuntimeData::from_usize(slot)?;
        runtimes.push(runtime);
        Ok(SessionQueueHandle::new(runtime_data))
    })
}

pub(crate) fn with_session_queue_runtime<P, R>(
    store: &'static LocalKey<RefCell<hammer_infra::vec::Vec<SessionQueueRuntime<P>>>>,
    handle: SessionQueueHandle,
    f: impl FnOnce(&mut SessionQueueRuntime<P>) -> CoreResult<R>,
) -> CoreResult<R> {
    let slot = handle.runtime_data().usize_word(0)?;
    store.with(|runtimes| {
        let mut runtimes = runtimes
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("session queue runtimes borrowed"))?;
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("session queue runtime slot is invalid"))?;
        f(runtime)
    })
}
