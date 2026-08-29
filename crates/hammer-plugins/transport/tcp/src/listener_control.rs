//! TCP listener registration owned by [`super::TcpMain`].
//!
//! Formerly `crate::service` — same UnsafeCell + ArcSwap publish path.
//! Control-plane fills happen only from `tcp::init`.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use crate::TcpCapabilities;
use hammer_runtime::app::SessionHandle;
use hammer_runtime::{DataWorkerId, RuntimeResult};

use super::TcpInputControlPlane;
use super::lookup::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpListenerAddress, TcpListenerLookupAccess,
    TcpLookupId, TcpLookupSnapshot, TcpLookupValue, TcpV4ListenerKey, TcpV6ListenerKey,
};

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

#[derive(Clone)]
struct TcpListenerRegistration {
    lookup_id: TcpLookupId,
    session_listener: SessionHandle,
    owner_worker: DataWorkerId,
    bind: SocketAddr,
    capabilities: TcpCapabilities,
}

struct TcpListenerControlState {
    next_tcp_lookup_id: TcpLookupId,
    tcp_control: TcpInputControlPlane,
    tcp_lookup: TcpLookupSnapshot,
    tcp_listeners: Vec<TcpListenerRegistration>,
    tcp_listener_slots: HashMap<u64, usize>,
}

struct TcpListenerControlCell {
    inner: UnsafeCell<TcpListenerControlState>,
}

impl TcpListenerControlCell {
    fn new(state: TcpListenerControlState) -> Self {
        Self {
            inner: UnsafeCell::new(state),
        }
    }

    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut(&self) -> &mut TcpListenerControlState {
        unsafe { &mut *self.inner.get() }
    }
}

// SAFETY: access is serialized by TcpListenerControlHandle through the single
// control thread. The cell is never mutated concurrently from multiple threads.
unsafe impl Send for TcpListenerControlCell {}
// SAFETY: shared references may cross threads, but all dereferences route
// through the control-thread serialization contract above.
unsafe impl Sync for TcpListenerControlCell {}

#[derive(Clone)]
pub(super) struct TcpListenerControlHandle {
    state: Arc<TcpListenerControlCell>,
}

impl TcpListenerControlState {
    fn new(tcp_control: TcpInputControlPlane) -> Self {
        Self {
            next_tcp_lookup_id: 1,
            tcp_control,
            tcp_lookup: TcpLookupSnapshot::empty(),
            tcp_listeners: Vec::new(),
            tcp_listener_slots: HashMap::new(),
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
        self.rebuild_tcp_listener_slots();
        self.publish_tcp_lookup()?;

        Ok(lookup_id)
    }

    fn close_tcp_listener(&mut self, lookup_id: TcpLookupId) -> RuntimeResult<()> {
        let slot = self
            .tcp_listener_slots
            .get(&u64::from(lookup_id))
            .copied()
            .ok_or(TcpListenerControlError::NotRegistered { lookup_id })?;
        self.tcp_listeners
            .drain(slot..slot + 1)
            .next()
            .expect("tcp listener exists at computed slot");
        self.rebuild_tcp_listener_slots();
        self.publish_tcp_lookup()
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

    fn publish_tcp_lookup(&mut self) -> RuntimeResult<()> {
        let mut snapshot = TcpLookupSnapshot::empty();
        for registration in self.tcp_listeners.iter().cloned() {
            let value = TcpLookupValue {
                id: registration.lookup_id,
                session_listener: registration.session_listener,
                owner_worker: registration.owner_worker,
                capabilities: registration.capabilities,
            };
            insert_tcp_listener_key(&mut snapshot, registration.bind, value);
        }
        self.tcp_control.publish_lookup(snapshot.clone())?;
        self.tcp_lookup = snapshot;
        Ok(())
    }

    fn rebuild_tcp_listener_slots(&mut self) {
        let mut slots = HashMap::new();
        for (index, registration) in self.tcp_listeners.iter().cloned().enumerate() {
            slots.insert(u64::from(registration.lookup_id), index);
        }
        self.tcp_listener_slots = slots;
    }
}

fn insert_tcp_listener_key(
    snapshot: &mut TcpLookupSnapshot,
    bind: SocketAddr,
    value: TcpLookupValue,
) {
    match bind.ip() {
        IpAddr::V4(addr) => insert_typed_tcp_listener::<TcpIpv4ListenerAddress>(
            snapshot,
            TcpV4ListenerKey::new(0, addr, bind.port()),
            value,
        ),
        IpAddr::V6(addr) => insert_typed_tcp_listener::<TcpIpv6ListenerAddress>(
            snapshot,
            TcpV6ListenerKey::new(0, addr, bind.port()),
            value,
        ),
    }
}

fn insert_typed_tcp_listener<A>(
    snapshot: &mut TcpLookupSnapshot,
    key: A::Key,
    value: TcpLookupValue,
) where
    A: TcpListenerAddress,
    TcpLookupSnapshot: TcpListenerLookupAccess<A>,
{
    snapshot.insert_listener::<A>(key, value);
}

impl TcpListenerControlHandle {
    pub(super) fn new(tcp_control: TcpInputControlPlane) -> Self {
        Self {
            state: Arc::new(TcpListenerControlCell::new(TcpListenerControlState::new(
                tcp_control,
            ))),
        }
    }

    pub(super) fn bind(
        &self,
        bind: SocketAddr,
        owner_worker: DataWorkerId,
        capabilities: TcpCapabilities,
        session_listener: SessionHandle,
    ) -> RuntimeResult<TcpLookupId> {
        let state = unsafe { self.state.get_mut() };
        state.bind_tcp_listener(bind, owner_worker, capabilities, session_listener)
    }

    pub(super) fn close_connection_index(
        &self,
        connection_index: TcpLookupId,
    ) -> RuntimeResult<()> {
        let state = unsafe { self.state.get_mut() };
        state.close_connection_index(connection_index)
    }
}
