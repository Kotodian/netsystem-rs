//! TCP listener registration owned by [`super::TcpMain`].
//!
//! Listener records are published by the Session lookup under WorkerBarrier.

use std::cell::UnsafeCell;
use std::net::SocketAddr;

use crate::TcpCapabilities;
use hammer_runtime::{DataWorkerId, RuntimeResult};
use hammer_service::session::SessionHandle;

use super::lookup::TcpLookupId;

#[hammer_component_macros::runtime_error(subsystem = "tcp")]
#[derive(Debug, thiserror::Error)]
pub enum TcpListenerControlError {
    #[error("tcp listener {bind} is already registered")]
    AlreadyRegistered { bind: SocketAddr },
    #[error("tcp listener {lookup_id} is not registered")]
    NotRegistered { lookup_id: TcpLookupId },
    #[error("tcp lookup id space is exhausted")]
    LookupIdExhausted,
}

#[derive(Clone, Copy)]
pub(super) struct TcpListenerRegistration {
    pub(super) lookup_id: TcpLookupId,
    pub(super) session_listener: SessionHandle,
    pub(super) owner_worker: DataWorkerId,
    bind: SocketAddr,
    pub(super) capabilities: TcpCapabilities,
}

struct TcpListenerControlState {
    next_tcp_lookup_id: TcpLookupId,
    tcp_listeners: Vec<TcpListenerRegistration>,
}

pub(super) struct TcpListenerControl {
    state: UnsafeCell<TcpListenerControlState>,
}

// SAFETY: Main Thread mutates only while WorkerBarrier stops Data Workers;
// packet workers read the listener vector without taking a second lock.
unsafe impl Sync for TcpListenerControl {}

impl TcpListenerControlState {
    fn new() -> Self {
        Self {
            next_tcp_lookup_id: 1,
            tcp_listeners: Vec::new(),
        }
    }

    fn bind_tcp_listener(
        &mut self,
        bind: SocketAddr,
        owner_worker: DataWorkerId,
        capabilities: TcpCapabilities,
        session_listener: SessionHandle,
    ) -> RuntimeResult<TcpLookupId> {
        if self
            .tcp_listeners
            .iter()
            .any(|registration| registration.bind == bind)
        {
            return Err(TcpListenerControlError::AlreadyRegistered { bind }.into());
        }

        let lookup_id = self.alloc_tcp_lookup_id()?;
        self.tcp_listeners.push(TcpListenerRegistration {
            lookup_id,
            session_listener,
            owner_worker,
            bind,
            capabilities,
        });
        Ok(lookup_id)
    }

    fn close_tcp_listener(&mut self, lookup_id: TcpLookupId) -> RuntimeResult<()> {
        let slot = self
            .tcp_listeners
            .iter()
            .position(|listener| listener.lookup_id == lookup_id)
            .ok_or(TcpListenerControlError::NotRegistered { lookup_id })?;
        self.tcp_listeners
            .drain(slot..slot + 1)
            .next()
            .expect("tcp listener exists at computed slot");
        Ok(())
    }

    fn close_connection_index(&mut self, connection_index: TcpLookupId) -> RuntimeResult<()> {
        self.close_tcp_listener(connection_index)
    }

    fn alloc_tcp_lookup_id(&mut self) -> RuntimeResult<TcpLookupId> {
        let id = self.next_tcp_lookup_id;
        self.next_tcp_lookup_id = self
            .next_tcp_lookup_id
            .checked_add(1)
            .ok_or(TcpListenerControlError::LookupIdExhausted)?;
        Ok(id)
    }
}

impl TcpListenerControl {
    pub(super) fn new() -> Self {
        Self {
            state: UnsafeCell::new(TcpListenerControlState::new()),
        }
    }

    #[inline]
    pub(super) fn listener_for_session(
        &self,
        session: SessionHandle,
    ) -> Option<TcpListenerRegistration> {
        // Main Thread changes this vector only while WorkerBarrier stops Data Workers.
        let state = unsafe { &*self.state.get() };
        state
            .tcp_listeners
            .iter()
            .find(|listener| listener.session_listener == session)
            .copied()
    }

    pub(super) fn bind(
        &self,
        bind: SocketAddr,
        owner_worker: DataWorkerId,
        capabilities: TcpCapabilities,
        session_listener: SessionHandle,
    ) -> RuntimeResult<TcpLookupId> {
        let state = unsafe { &mut *self.state.get() };
        state.bind_tcp_listener(bind, owner_worker, capabilities, session_listener)
    }

    pub(super) fn close_connection_index(
        &self,
        connection_index: TcpLookupId,
    ) -> RuntimeResult<()> {
        let state = unsafe { &mut *self.state.get() };
        state.close_connection_index(connection_index)
    }
}
