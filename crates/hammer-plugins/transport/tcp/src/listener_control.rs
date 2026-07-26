//! TCP listener registration owned by [`super::TcpMain`].
//!
//! Formerly `crate::service` — same UnsafeCell + ArcSwap publish path.
//! Control-plane fills happen only from `tcp::init`.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use crate::TcpCapabilities;
use hammer_runtime::DataWorkerId;
use hammer_runtime::{RuntimeError, RuntimeResult};

use super::TcpInputControlPlane;
use super::lookup::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpListenerAddress, TcpListenerLookupAccess,
    TcpLookupId, TcpLookupSnapshot, TcpLookupValue, TcpV4ListenerKey, TcpV6ListenerKey,
};

#[derive(Debug, thiserror::Error)]
pub enum TcpListenerControlError {
    #[error("tcp listener {bind} is already registered")]
    AlreadyRegistered { bind: SocketAddr },
    #[error("tcp listener {lookup_id} is not registered")]
    NotRegistered { lookup_id: TcpLookupId },
    #[error("tcp lookup id space is exhausted")]
    LookupIdExhausted,
}

impl From<TcpListenerControlError> for RuntimeError {
    fn from(error: TcpListenerControlError) -> Self {
        Self::subsystem("tcp", error)
    }
}

#[derive(Clone)]
struct TcpListenerRegistration {
    lookup_id: TcpLookupId,
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

#[cfg(test)]
#[derive(Clone)]
struct TcpListenerControlSnapshot {
    tcp_listeners: Vec<TcpLookupId>,
    tcp_lookup: TcpLookupSnapshot,
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
    ) -> RuntimeResult<TcpLookupId> {
        let state = unsafe { self.state.get_mut() };
        state.bind_tcp_listener(bind, owner_worker, capabilities)
    }

    #[cfg(test)]
    pub(super) fn close(&self, lookup_id: TcpLookupId) -> RuntimeResult<()> {
        let state = unsafe { self.state.get_mut() };
        state.close_tcp_listener(lookup_id)
    }

    #[cfg(test)]
    fn snapshot_for_test(&self) -> TcpListenerControlSnapshot {
        let state = unsafe { &*self.state.inner.get() };
        let mut tcp_listeners = Vec::new();
        for registration in state.tcp_listeners.iter() {
            tcp_listeners.push(registration.lookup_id);
        }
        TcpListenerControlSnapshot {
            tcp_listeners,
            tcp_lookup: state.tcp_lookup.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lookup::TcpV4ListenerKey;
    use std::net::Ipv4Addr;

    #[test]
    fn tcp_listener_lifecycle() {
        let control = TcpInputControlPlane::new();
        let handle = TcpListenerControlHandle::new(control);

        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7300);
        let lookup_id = handle
            .bind(bind, DataWorkerId::new(0), TcpCapabilities::default())
            .expect("bind tcp listener");

        let snapshot = handle.snapshot_for_test();
        assert_eq!(snapshot.tcp_listeners.len(), 1);
        assert_eq!(snapshot.tcp_listeners[0], lookup_id);

        let lookup_entry = snapshot
            .tcp_lookup
            .lookup_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                0,
                Ipv4Addr::new(127, 0, 0, 1),
                7300,
            ));
        assert!(lookup_entry.is_some(), "listener must appear in lookup");
        assert_eq!(lookup_entry.unwrap().id, lookup_id);

        handle.close(lookup_id).expect("close tcp listener");
        let snapshot = handle.snapshot_for_test();
        assert!(
            snapshot
                .tcp_lookup
                .lookup_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                    0,
                    Ipv4Addr::new(127, 0, 0, 1),
                    7300,
                ))
                .is_none(),
            "closed listener lookup must be removed"
        );
        assert!(
            snapshot.tcp_listeners.iter().all(|id| *id != lookup_id),
            "closed listener registration must be removed"
        );
    }
}
