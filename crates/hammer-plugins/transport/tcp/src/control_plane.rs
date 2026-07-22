use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::{TcpCapabilities, TcpControlPlaneAction, TcpListenerId, TcpListenerKey};
use hammer_runtime::RuntimeResult;

use hammer_runtime::ControlThreadHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpManagedListener {
    listener: TcpListenerKey,
    capabilities: TcpCapabilities,
}

struct TcpControlPlaneState {
    listeners: HashMap<TcpListenerId, TcpManagedListener>,
}

struct TcpControlPlaneCell {
    inner: UnsafeCell<TcpControlPlaneState>,
}

impl TcpControlPlaneCell {
    #[inline]
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(TcpControlPlaneState {
                listeners: HashMap::new(),
            }),
        }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut(&self) -> &mut TcpControlPlaneState {
        unsafe { &mut *self.inner.get() }
    }

    #[inline]
    unsafe fn get(&self) -> &TcpControlPlaneState {
        unsafe { &*self.inner.get() }
    }
}

// SAFETY: TcpControlPlaneState is owned by the control thread and all access is
// serialized through control-thread dispatch.
unsafe impl Send for TcpControlPlaneCell {}
// SAFETY: shared references may cross threads, but dereferences stay within
// the single control-thread ownership model above.
unsafe impl Sync for TcpControlPlaneCell {}

#[derive(Clone)]
pub struct TcpControlPlane {
    control_handle: Arc<ControlThreadHandle>,
    state: Arc<TcpControlPlaneCell>,
}

impl TcpControlPlane {
    #[inline]
    pub fn new(control_handle: Arc<ControlThreadHandle>) -> Self {
        Self {
            control_handle,
            state: Arc::new(TcpControlPlaneCell::new()),
        }
    }

    pub fn apply(&self, action: TcpControlPlaneAction) -> RuntimeResult<()> {
        match action {
            TcpControlPlaneAction::InstallListener {
                listener_id,
                listener,
                capabilities,
            } => self.with_state_mut(move |state| {
                state.listeners.insert(
                    listener_id,
                    TcpManagedListener {
                        listener,
                        capabilities,
                    },
                );
                Ok(())
            }),
            TcpControlPlaneAction::RemoveListener {
                listener_id,
                reason: _,
            } => self.with_state_mut(move |state| {
                state.listeners.remove(&listener_id);
                Ok(())
            }),
        }
    }

    fn with_state_mut<R>(
        &self,
        f: impl FnOnce(&mut TcpControlPlaneState) -> RuntimeResult<R> + Send + 'static,
    ) -> RuntimeResult<R>
    where
        R: Send + 'static,
    {
        let state = Arc::clone(&self.state);
        self.control_handle.call_blocking(move || {
            // SAFETY: TcpControlPlaneState is owned by the control thread.
            let state = unsafe { state.get_mut() };
            f(state)
        })?
    }

    fn with_state<R>(
        &self,
        f: impl FnOnce(&TcpControlPlaneState) -> RuntimeResult<R> + Send + 'static,
    ) -> RuntimeResult<R>
    where
        R: Send + 'static,
    {
        let state = Arc::clone(&self.state);
        self.control_handle.call_blocking(move || {
            // SAFETY: TcpControlPlaneState reads also route through the
            // control thread to keep ownership single-threaded.
            let state = unsafe { state.get() };
            f(state)
        })?
    }

    #[doc(hidden)]
    #[inline]
    pub fn has_listener(&self, listener_id: TcpListenerId) -> bool {
        self.with_state(move |state| Ok(state.listeners.contains_key(&listener_id)))
            .unwrap_or(false)
    }

    #[doc(hidden)]
    #[inline]
    pub fn listener_for_test(
        &self,
        listener_id: TcpListenerId,
    ) -> Option<(TcpListenerKey, TcpCapabilities)> {
        self.with_state(move |state| {
            Ok(state
                .listeners
                .get(&listener_id)
                .map(|entry| (entry.listener, entry.capabilities)))
        })
        .ok()
        .flatten()
    }
}
