use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hammer_core::error::{HammerError, HammerResult};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpConnectionKey, TcpControlPlaneAction, TcpListenerId,
    TcpListenerKey, TcpNegotiatedOptions, TcpShutdownDirection, TcpState, TcpWorkerEvent,
};
pub use hammer_core::protocol::tcp::{TcpConnectionId, TcpTimerId, TcpTimerKind};

use crate::{ControlThreadHandle, ControlTimerHandle};

const ALL_TIMER_KINDS: [TcpTimerKind; 6] = [
    TcpTimerKind::Connect,
    TcpTimerKind::Retransmit,
    TcpTimerKind::DelayedAck,
    TcpTimerKind::Persist,
    TcpTimerKind::KeepAlive,
    TcpTimerKind::TimeWait,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TcpTimerKey {
    connection: TcpConnectionId,
    kind: TcpTimerKind,
}

impl TcpTimerKey {
    #[inline]
    const fn new(connection: TcpConnectionId, kind: TcpTimerKind) -> Self {
        Self { connection, kind }
    }
}

struct TimerEntry {
    timer_id: TcpTimerId,
    handle: ControlTimerHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpManagedListener {
    listener: TcpListenerKey,
    capabilities: TcpCapabilities,
    close_reason: Option<TcpCloseReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpManagedConnection {
    key: TcpConnectionKey,
    state: TcpState,
    capabilities: TcpCapabilities,
    negotiated: TcpNegotiatedOptions,
    shutdown: Option<(TcpShutdownDirection, TcpCloseReason)>,
    close_reason: Option<TcpCloseReason>,
}

#[derive(Default)]
struct TcpTimerRegistryState {
    next_timer_id: u64,
    entries: HashMap<TcpTimerKey, TimerEntry>,
}

struct TcpTimerRegistryCell {
    inner: UnsafeCell<TcpTimerRegistryState>,
}

impl TcpTimerRegistryCell {
    #[inline]
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(TcpTimerRegistryState {
                next_timer_id: 1,
                entries: HashMap::new(),
            }),
        }
    }

    #[inline]
    unsafe fn get_mut(&self) -> &mut TcpTimerRegistryState {
        unsafe { &mut *self.inner.get() }
    }

    #[allow(dead_code)]
    #[inline]
    unsafe fn get(&self) -> &TcpTimerRegistryState {
        unsafe { &*self.inner.get() }
    }
}

// SAFETY: TcpTimerRegistryState is only accessed on the single control thread,
// either via call_blocking or from timer callbacks scheduled onto that thread.
unsafe impl Send for TcpTimerRegistryCell {}
// SAFETY: shared references may cross threads, but dereferences still obey the
// single control-thread ownership contract above.
unsafe impl Sync for TcpTimerRegistryCell {}

struct TcpControlPlaneState {
    listeners: HashMap<TcpListenerId, TcpManagedListener>,
    connections: HashMap<TcpConnectionId, TcpManagedConnection>,
    closed_connections: HashMap<TcpConnectionId, TcpCloseReason>,
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
                connections: HashMap::new(),
                closed_connections: HashMap::new(),
            }),
        }
    }

    #[inline]
    unsafe fn get_mut(&self) -> &mut TcpControlPlaneState {
        unsafe { &mut *self.inner.get() }
    }

    #[inline]
    unsafe fn get(&self) -> &TcpControlPlaneState {
        unsafe { &*self.inner.get() }
    }
}

// SAFETY: TcpControlPlaneState is owned by the control thread and all access is
// serialized through control-thread dispatch or timer callbacks on that thread.
unsafe impl Send for TcpControlPlaneCell {}
// SAFETY: shared references may cross threads, but dereferences stay within
// the single control-thread ownership model above.
unsafe impl Sync for TcpControlPlaneCell {}

#[derive(Clone)]
pub struct TcpControlTimerSet {
    control_handle: Arc<ControlThreadHandle>,
    state: Arc<TcpTimerRegistryCell>,
}

impl TcpControlTimerSet {
    #[inline]
    pub fn new(control_handle: Arc<ControlThreadHandle>) -> Self {
        Self {
            control_handle,
            state: Arc::new(TcpTimerRegistryCell::new()),
        }
    }

    pub fn arm_once<F, Fut>(
        &self,
        connection: TcpConnectionId,
        kind: TcpTimerKind,
        delay: Duration,
        callback: F,
    ) -> HammerResult<()>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let control_handle = Arc::clone(&self.control_handle);
        let state_cell = Arc::clone(&self.state);
        self.control_handle.call_blocking(move || {
            // SAFETY: TcpTimerRegistryState is owned by the control thread.
            let state = unsafe { state_cell.get_mut() };
            let timer_id = TcpTimerId::new(state.next_timer_id);
            state.next_timer_id = state.next_timer_id.wrapping_add(1).max(1);
            Self::arm_once_with_id_on_control(
                &control_handle,
                &state_cell,
                state,
                connection,
                timer_id,
                kind,
                delay,
                callback,
            )
        })?
    }

    #[inline]
    pub fn cancel(&self, connection: TcpConnectionId, kind: TcpTimerKind) -> bool {
        let state = Arc::clone(&self.state);
        self.control_handle
            .call_blocking(move || {
                // SAFETY: TcpTimerRegistryState is owned by the control thread.
                let state = unsafe { state.get_mut() };
                Self::cancel_on_control(state, connection, kind)
            })
            .unwrap_or(false)
    }

    fn arm_once_with_id_on_control<F, Fut>(
        control_handle: &Arc<ControlThreadHandle>,
        state_cell: &Arc<TcpTimerRegistryCell>,
        state: &mut TcpTimerRegistryState,
        connection: TcpConnectionId,
        timer_id: TcpTimerId,
        kind: TcpTimerKind,
        delay: Duration,
        mut callback: F,
    ) -> HammerResult<()>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let key = TcpTimerKey::new(connection, kind);
        let state_cell = Arc::clone(state_cell);
        let handle = control_handle.schedule_once(delay, move || {
            let state_cell = Arc::clone(&state_cell);
            let future = callback();
            async move {
                future.await;
                // SAFETY: timer callbacks run on the control thread runtime and
                // mutate the registry only after the user callback finishes.
                let state = unsafe { state_cell.get_mut() };
                if state
                    .entries
                    .get(&key)
                    .is_some_and(|entry| entry.timer_id == timer_id)
                {
                    state.entries.remove(&key);
                }
            }
        })?;
        let previous = state.entries.insert(key, TimerEntry { timer_id, handle });
        if let Some(previous) = previous {
            previous.handle.cancel();
        }
        Ok(())
    }

    fn cancel_on_control(
        state: &mut TcpTimerRegistryState,
        connection: TcpConnectionId,
        kind: TcpTimerKind,
    ) -> bool {
        state
            .entries
            .remove(&TcpTimerKey::new(connection, kind))
            .is_some_and(|entry| entry.handle.cancel())
    }

    fn cancel_all_on_control(state: &mut TcpTimerRegistryState, connection: TcpConnectionId) {
        for kind in ALL_TIMER_KINDS {
            Self::cancel_on_control(state, connection, kind);
        }
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub(crate) fn has_timer_for_test(
        &self,
        connection: TcpConnectionId,
        kind: TcpTimerKind,
    ) -> bool {
        let state = Arc::clone(&self.state);
        self.control_handle
            .call_blocking(move || {
                // SAFETY: TcpTimerRegistryState is owned by the control thread.
                let state = unsafe { state.get() };
                state
                    .entries
                    .contains_key(&TcpTimerKey::new(connection, kind))
            })
            .unwrap_or(false)
    }
}

#[derive(Clone)]
pub struct TcpControlPlane {
    control_handle: Arc<ControlThreadHandle>,
    timers: TcpControlTimerSet,
    events: TcpWorkerEventSink,
    state: Arc<TcpControlPlaneCell>,
}

impl TcpControlPlane {
    #[inline]
    pub fn new<F>(control_handle: Arc<ControlThreadHandle>, on_event: F) -> Self
    where
        F: Fn(TcpWorkerEvent) + Send + Sync + 'static,
    {
        Self {
            control_handle: Arc::clone(&control_handle),
            timers: TcpControlTimerSet::new(control_handle),
            events: TcpWorkerEventSink::new(on_event),
            state: Arc::new(TcpControlPlaneCell::new()),
        }
    }

    pub fn apply(&self, action: TcpControlPlaneAction) -> HammerResult<()> {
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
                        close_reason: None,
                    },
                );
                Ok(())
            }),
            TcpControlPlaneAction::RemoveListener {
                listener_id,
                reason,
            } => self.with_state_mut(move |state| {
                if let Some(listener) = state.listeners.get_mut(&listener_id) {
                    listener.close_reason = Some(reason);
                }
                state.listeners.remove(&listener_id);
                Ok(())
            }),
            TcpControlPlaneAction::InstallConnection {
                connection_id,
                key,
                state: tcp_state,
                capabilities,
                negotiated,
            } => {
                self.with_state_mut(move |state| {
                    state.closed_connections.remove(&connection_id);
                    state.connections.insert(
                        connection_id,
                        TcpManagedConnection {
                            key,
                            state: tcp_state,
                            capabilities,
                            negotiated,
                            shutdown: None,
                            close_reason: None,
                        },
                    );
                    Ok(())
                })?;
                self.events.emit(TcpWorkerEvent::StateChanged {
                    connection_id,
                    key,
                    state: tcp_state,
                });
                Ok(())
            }
            TcpControlPlaneAction::UpsertConnectionState {
                connection_id,
                key,
                state: tcp_state,
                capabilities,
                negotiated,
            } => {
                let should_emit = self.with_state_mut(move |state| {
                    if state.closed_connections.contains_key(&connection_id) {
                        return Ok(false);
                    }
                    match state.connections.get_mut(&connection_id) {
                        Some(connection) => {
                            connection.key = key;
                            connection.state = tcp_state;
                            connection.capabilities = capabilities;
                            connection.negotiated = negotiated;
                        }
                        None => {
                            state.connections.insert(
                                connection_id,
                                TcpManagedConnection {
                                    key,
                                    state: tcp_state,
                                    capabilities,
                                    negotiated,
                                    shutdown: None,
                                    close_reason: None,
                                },
                            );
                        }
                    }
                    Ok(true)
                })?;
                if should_emit {
                    self.events.emit(TcpWorkerEvent::StateChanged {
                        connection_id,
                        key,
                        state: tcp_state,
                    });
                }
                Ok(())
            }
            TcpControlPlaneAction::TransitionConnection {
                connection_id,
                state: tcp_state,
            } => {
                let key = self.with_state_mut(move |state| {
                    if state.closed_connections.contains_key(&connection_id) {
                        return Ok(None);
                    }
                    let connection =
                        state.connections.get_mut(&connection_id).ok_or_else(|| {
                            HammerError::internal(format!(
                                "tcp connection {} is not installed",
                                connection_id.get()
                            ))
                        })?;
                    connection.state = tcp_state;
                    Ok(Some(connection.key))
                })?;
                if let Some(key) = key {
                    self.events.emit(TcpWorkerEvent::StateChanged {
                        connection_id,
                        key,
                        state: tcp_state,
                    });
                }
                Ok(())
            }
            TcpControlPlaneAction::ShutdownConnection {
                connection_id,
                direction,
                reason,
            } => {
                let should_emit = self.with_state_mut(move |state| {
                    if state.closed_connections.contains_key(&connection_id) {
                        return Ok(false);
                    }
                    let Some(connection) = state.connections.get_mut(&connection_id) else {
                        return Ok(false);
                    };
                    connection.shutdown = Some((direction, reason));
                    Ok(true)
                })?;
                if should_emit {
                    self.events.emit(TcpWorkerEvent::ShutdownObserved {
                        connection_id,
                        direction,
                        reason,
                    });
                }
                Ok(())
            }
            TcpControlPlaneAction::CloseConnection {
                connection_id,
                reason,
            } => {
                self.close_connection(connection_id, reason)?;
                self.events.emit(TcpWorkerEvent::Closed {
                    connection_id,
                    reason,
                });
                Ok(())
            }
            TcpControlPlaneAction::ArmTimer {
                connection_id,
                timer_id,
                kind,
                timeout,
            } => self.arm_timer(connection_id, timer_id, kind, timeout),
            TcpControlPlaneAction::CancelTimer {
                connection_id,
                kind,
            } => self.cancel_timer(connection_id, kind),
        }
    }

    fn with_state_mut<R>(
        &self,
        f: impl FnOnce(&mut TcpControlPlaneState) -> HammerResult<R> + Send + 'static,
    ) -> HammerResult<R>
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
        f: impl FnOnce(&TcpControlPlaneState) -> HammerResult<R> + Send + 'static,
    ) -> HammerResult<R>
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

    fn close_connection(
        &self,
        connection_id: TcpConnectionId,
        reason: TcpCloseReason,
    ) -> HammerResult<()> {
        let state = Arc::clone(&self.state);
        let timer_state = Arc::clone(&self.timers.state);
        self.control_handle.call_blocking(move || {
            // SAFETY: both cells are owned by the single control thread.
            let state = unsafe { state.get_mut() };
            state.closed_connections.insert(connection_id, reason);
            if let Some(connection) = state.connections.get_mut(&connection_id) {
                connection.close_reason = Some(reason);
            }
            state.connections.remove(&connection_id);
            // SAFETY: the timer registry shares the same control-thread owner.
            let timer_state = unsafe { timer_state.get_mut() };
            TcpControlTimerSet::cancel_all_on_control(timer_state, connection_id);
            Ok(())
        })?
    }

    fn arm_timer(
        &self,
        connection_id: TcpConnectionId,
        timer_id: TcpTimerId,
        kind: TcpTimerKind,
        timeout: Duration,
    ) -> HammerResult<()> {
        self.with_state(move |state| {
            if state.connections.contains_key(&connection_id) {
                Ok(())
            } else {
                Err(HammerError::internal(format!(
                    "tcp connection {} is not installed",
                    connection_id.get()
                )))
            }
        })?;
        let events = self.events.clone();
        let timer_state_cell = Arc::clone(&self.timers.state);
        let control_handle = Arc::clone(&self.control_handle);
        self.control_handle.call_blocking(move || {
            // SAFETY: the timer registry is owned by the control thread.
            let timer_state = unsafe { timer_state_cell.get_mut() };
            TcpControlTimerSet::arm_once_with_id_on_control(
                &control_handle,
                &timer_state_cell,
                timer_state,
                connection_id,
                timer_id,
                kind,
                timeout,
                move || {
                    let events = events.clone();
                    async move {
                        events.emit(TcpWorkerEvent::TimerExpired {
                            connection_id,
                            timer_id,
                            kind,
                        });
                    }
                },
            )
        })?
    }

    fn cancel_timer(&self, connection_id: TcpConnectionId, kind: TcpTimerKind) -> HammerResult<()> {
        let timer_state = Arc::clone(&self.timers.state);
        self.control_handle.call_blocking(move || {
            // SAFETY: the timer registry is owned by the control thread.
            let timer_state = unsafe { timer_state.get_mut() };
            TcpControlTimerSet::cancel_on_control(timer_state, connection_id, kind);
            Ok(())
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
    pub fn has_connection(&self, connection_id: TcpConnectionId) -> bool {
        self.with_state(move |state| Ok(state.connections.contains_key(&connection_id)))
            .unwrap_or(false)
    }

    #[doc(hidden)]
    #[inline]
    pub fn connection_state_for_test(&self, connection_id: TcpConnectionId) -> Option<TcpState> {
        self.with_state(move |state| {
            Ok(state
                .connections
                .get(&connection_id)
                .map(|entry| entry.state))
        })
        .ok()
        .flatten()
    }
}

struct TcpWorkerEventSink {
    raw: *const (),
    clone_raw: fn(*const ()) -> *const (),
    drop_raw: fn(*const ()),
    emit: fn(*const (), TcpWorkerEvent),
}

unsafe impl Send for TcpWorkerEventSink {}
unsafe impl Sync for TcpWorkerEventSink {}

impl Clone for TcpWorkerEventSink {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            raw: (self.clone_raw)(self.raw),
            clone_raw: self.clone_raw,
            drop_raw: self.drop_raw,
            emit: self.emit,
        }
    }
}

impl Drop for TcpWorkerEventSink {
    #[inline]
    fn drop(&mut self) {
        (self.drop_raw)(self.raw);
    }
}

impl TcpWorkerEventSink {
    #[inline]
    fn new<F>(sink: F) -> Self
    where
        F: Fn(TcpWorkerEvent) + Send + Sync + 'static,
    {
        let sink = Arc::new(sink);
        Self {
            raw: Arc::into_raw(sink) as *const (),
            clone_raw: clone_event_sink_arc_handle::<F>,
            drop_raw: drop_event_sink_arc_handle::<F>,
            emit: emit_event_with::<F>,
        }
    }

    #[inline]
    fn emit(&self, event: TcpWorkerEvent) {
        (self.emit)(self.raw, event);
    }
}

#[inline]
fn clone_event_sink_arc_handle<F>(raw: *const ()) -> *const ()
where
    F: Fn(TcpWorkerEvent) + Send + Sync + 'static,
{
    let raw = raw.cast::<F>();
    unsafe {
        Arc::increment_strong_count(raw);
    }
    raw.cast()
}

#[inline]
fn drop_event_sink_arc_handle<F>(raw: *const ())
where
    F: Fn(TcpWorkerEvent) + Send + Sync + 'static,
{
    unsafe {
        drop(Arc::from_raw(raw.cast::<F>()));
    }
}

#[inline]
fn emit_event_with<F>(raw: *const (), event: TcpWorkerEvent)
where
    F: Fn(TcpWorkerEvent) + Send + Sync + 'static,
{
    unsafe {
        (&*raw.cast::<F>())(event);
    }
}
