use std::cell::UnsafeCell;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use hammer_adapter::DataWorkerId;
use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;

use crate::transport::tcp::TcpInputControlPlane;
use crate::transport::tcp::lookup::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpListenerAddress, TcpListenerLookupAccess,
    TcpLookupId, TcpLookupSnapshot, TcpLookupValue, TcpV4ListenerKey, TcpV6ListenerKey,
};
use hammer_core::protocol::tcp::TcpCapabilities;

#[derive(Clone)]
struct TcpListenerRegistration {
    lookup_id: TcpLookupId,
    owner_worker: DataWorkerId,
    bind: SocketAddr,
    capabilities: TcpCapabilities,
}

pub(crate) struct RuntimeTcpListenerControlState {
    next_tcp_lookup_id: TcpLookupId,
    tcp_control: TcpInputControlPlane,
    tcp_lookup: TcpLookupSnapshot,
    tcp_listeners: hammer_infra::vec::Vec<TcpListenerRegistration>,
    tcp_listener_slots: FlatHashTable<u64, usize>,
}

struct RuntimeTcpListenerControlCell {
    inner: UnsafeCell<RuntimeTcpListenerControlState>,
}

impl RuntimeTcpListenerControlCell {
    fn new(state: RuntimeTcpListenerControlState) -> Self {
        Self {
            inner: UnsafeCell::new(state),
        }
    }

    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut(&self) -> &mut RuntimeTcpListenerControlState {
        unsafe { &mut *self.inner.get() }
    }
}

// SAFETY: access is serialized by RuntimeTcpListenerControlHandle through the
// single control thread. The cell is never mutated concurrently from multiple
// threads.
unsafe impl Send for RuntimeTcpListenerControlCell {}
// SAFETY: shared references may cross threads, but all dereferences route
// through the control-thread serialization contract above.
unsafe impl Sync for RuntimeTcpListenerControlCell {}

#[derive(Clone)]
pub(crate) struct RuntimeTcpListenerControlHandle {
    state: Arc<RuntimeTcpListenerControlCell>,
}

#[cfg(test)]
#[derive(Clone)]
struct RuntimeTcpListenerControlSnapshot {
    tcp_listeners: hammer_infra::vec::Vec<RuntimeTcpListenerSnapshot>,
    tcp_lookup: TcpLookupSnapshot,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct RuntimeTcpListenerSnapshot {
    lookup_id: TcpLookupId,
}

impl RuntimeTcpListenerControlState {
    fn new() -> HammerResult<Self> {
        let tcp_control = crate::transport::tcp::TCP_MAIN
            .load()
            .as_deref()
            .ok_or_else(|| HammerError::internal("tcp main not initialized"))?
            .control()
            .clone();

        Ok(Self {
            next_tcp_lookup_id: 1,
            tcp_control,
            tcp_lookup: TcpLookupSnapshot::empty(),
            tcp_listeners: hammer_infra::vec::Vec::new(),
            tcp_listener_slots: FlatHashTable::new(),
        })
    }

    fn bind_tcp_listener(
        &mut self,
        bind: SocketAddr,
        owner_worker: usize,
        capabilities: TcpCapabilities,
    ) -> HammerResult<TcpLookupId> {
        if self
            .tcp_listeners
            .iter()
            .any(|registration| registration.bind == bind)
        {
            return Err(HammerError::internal(format!(
                "tcp listener {bind} is already registered"
            )));
        }

        let lookup_id = self.alloc_tcp_lookup_id()?;
        self.tcp_listeners.push(TcpListenerRegistration {
            lookup_id,
            owner_worker: worker_id(owner_worker)?,
            bind,
            capabilities,
        });
        self.rebuild_tcp_listener_slots();
        self.publish_tcp_lookup()?;

        Ok(lookup_id)
    }

    fn close_tcp_listener(&mut self, lookup_id: TcpLookupId) -> HammerResult<()> {
        let slot = self
            .tcp_listener_slots
            .lookup(&u64::from(lookup_id))
            .ok_or_else(|| {
                HammerError::internal(format!(
                    "tcp listener {lookup_id} is not registered in runtime service"
                ))
            })?;
        self.tcp_listeners
            .drain(slot..slot + 1)
            .next()
            .expect("tcp listener exists at computed slot");
        self.rebuild_tcp_listener_slots();
        self.publish_tcp_lookup()
    }

    fn alloc_tcp_lookup_id(&mut self) -> HammerResult<TcpLookupId> {
        let id = self.next_tcp_lookup_id;
        self.next_tcp_lookup_id = self
            .next_tcp_lookup_id
            .checked_add(1)
            .ok_or_else(|| HammerError::internal("tcp lookup id overflow"))?;
        Ok(id)
    }

    fn publish_tcp_lookup(&mut self) -> HammerResult<()> {
        let mut snapshot = TcpLookupSnapshot::empty();
        for registration in self.tcp_listeners.iter().cloned() {
            let value = TcpLookupValue {
                id: registration.lookup_id,
                owner_worker: registration.owner_worker,
                capabilities: registration.capabilities,
            };
            insert_tcp_listener_key(&mut snapshot, registration.bind, value);
        }
        self.tcp_control
            .publish_lookup(snapshot.clone())
            .map_err(|err| HammerError::internal(format!("publish tcp lookup snapshot: {err}")))?;
        self.tcp_lookup = snapshot;
        Ok(())
    }

    fn rebuild_tcp_listener_slots(&mut self) {
        let mut slots = FlatHashTable::new();
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

impl RuntimeTcpListenerControlHandle {
    fn new(state: RuntimeTcpListenerControlState) -> Self {
        Self {
            state: Arc::new(RuntimeTcpListenerControlCell::new(state)),
        }
    }

    fn bind_tcp_listener_on_control(
        &self,
        bind: SocketAddr,
        owner_worker: usize,
        capabilities: TcpCapabilities,
    ) -> HammerResult<TcpLookupId> {
        let state = unsafe { self.state.get_mut() };
        state.bind_tcp_listener(bind, owner_worker, capabilities)
    }

    fn close_tcp_listener_on_control(&self, lookup_id: TcpLookupId) -> HammerResult<()> {
        let state = unsafe { self.state.get_mut() };
        state.close_tcp_listener(lookup_id)
    }

    #[cfg(test)]
    fn snapshot_for_test_on_control(&self) -> RuntimeTcpListenerControlSnapshot {
        let state = unsafe { &*self.state.inner.get() };
        let mut tcp_listeners = hammer_infra::vec::Vec::new();
        for registration in state.tcp_listeners.iter() {
            tcp_listeners.push(RuntimeTcpListenerSnapshot {
                lookup_id: registration.lookup_id,
            });
        }
        RuntimeTcpListenerControlSnapshot {
            tcp_listeners,
            tcp_lookup: state.tcp_lookup.clone(),
        }
    }
}

fn worker_id(worker: usize) -> HammerResult<DataWorkerId> {
    u32::try_from(worker)
        .map(DataWorkerId::new)
        .map_err(|_| HammerError::internal(format!("worker index {worker} does not fit into u32")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tcp::lookup::TcpV4ListenerKey;
    use hammer_core::registry::RuntimeRegistry;
    use std::net::Ipv4Addr;

    fn tcp_main_for_test() {
        crate::reset_subsystem_mains_for_test();
        crate::transport::tcp::init(&RuntimeRegistry::new()).expect("init tcp main");
    }

    #[test]
    fn tcp_listener_lifecycle() {
        tcp_main_for_test();
        let mut state = RuntimeTcpListenerControlState::new().expect("new state");
        let handle = RuntimeTcpListenerControlHandle::new(state);

        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7300);
        let capabilities = TcpCapabilities::default();

        let lookup_id = handle
            .bind_tcp_listener_on_control(bind, 0, capabilities)
            .expect("bind tcp listener");

        let snapshot = handle.snapshot_for_test_on_control();
        assert_eq!(snapshot.tcp_listeners.len(), 1);
        assert_eq!(snapshot.tcp_listeners[0].lookup_id, lookup_id);

        let lookup_entry = snapshot
            .tcp_lookup
            .lookup_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                0,
                Ipv4Addr::new(127, 0, 0, 1),
                7300,
            ));
        assert!(lookup_entry.is_some(), "listener must appear in lookup");
        assert_eq!(lookup_entry.unwrap().id, lookup_id);

        handle
            .close_tcp_listener_on_control(lookup_id)
            .expect("close tcp listener");
        let snapshot = handle.snapshot_for_test_on_control();
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
            snapshot
                .tcp_listeners
                .iter()
                .all(|registration| registration.lookup_id != lookup_id),
            "closed listener registration must be removed"
        );
    }
}
