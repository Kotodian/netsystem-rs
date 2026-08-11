use std::net::SocketAddr;

use hammer_infra::bihash::{Bihash, BihashKey};
use hammer_runtime::app::SessionHandle;

use super::runtime::SessionTransportId;

const SESSION_ENDPOINT_CAPACITY: u32 = 1024;
const ENDPOINT_STATE_OPENED: u64 = 1;
const ENDPOINT_STATE_READY: u64 = 2;
const ENDPOINT_STATE_MIGRATING: u64 = 3;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct SessionEndpointKey([u64; 6]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionEndpointState {
    Opened,
    Ready,
    Migrating,
}

impl SessionEndpointState {
    #[inline]
    const fn raw(self) -> u64 {
        match self {
            Self::Opened => ENDPOINT_STATE_OPENED,
            Self::Ready => ENDPOINT_STATE_READY,
            Self::Migrating => ENDPOINT_STATE_MIGRATING,
        }
    }

    #[inline]
    const fn from_raw(value: u64) -> Option<Self> {
        match value {
            ENDPOINT_STATE_OPENED => Some(Self::Opened),
            ENDPOINT_STATE_READY => Some(Self::Ready),
            ENDPOINT_STATE_MIGRATING => Some(Self::Migrating),
            _ => None,
        }
    }
}

impl SessionEndpointKey {
    #[inline]
    fn new(local: SocketAddr, remote: SocketAddr, transport: SessionTransportId) -> Option<Self> {
        if local.is_ipv4() != remote.is_ipv4() {
            return None;
        }
        let (local_hi, local_lo) = ip_words(local);
        let (remote_hi, remote_lo) = ip_words(remote);
        Some(Self([
            (u64::from(transport.raw()) << 8) | u64::from(if local.is_ipv4() { 4u8 } else { 6u8 }),
            local_hi,
            local_lo,
            remote_hi,
            remote_lo,
            (u64::from(local.port()) << 16) | u64::from(remote.port()),
        ]))
    }
}

impl BihashKey for SessionEndpointKey {
    #[inline(always)]
    fn hash(self) -> u64 {
        hammer_infra::bihash::hash_words(&self.0)
    }
}

pub(crate) struct SessionEndpointLookup {
    table: Bihash<SessionEndpointKey, 8>,
    states: Bihash<SessionEndpointKey, 8>,
}

impl SessionEndpointLookup {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            table: Bihash::new(SESSION_ENDPOINT_CAPACITY),
            states: Bihash::new(SESSION_ENDPOINT_CAPACITY),
        }
    }

    #[inline]
    pub(crate) fn insert(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
        handle: SessionHandle,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        if self.table.insert_if_absent(key, handle.raw()).is_err() {
            return false;
        }
        if self
            .states
            .insert_if_absent(key, SessionEndpointState::Opened.raw())
            .is_err()
        {
            self.table.remove_if_current(&key, handle.raw());
            return false;
        }
        true
    }

    #[inline]
    pub(crate) fn remove_if_current(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
        handle: SessionHandle,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        let removed = self.table.remove_if_current(&key, handle.raw());
        if removed && let Some(state) = self.states.lookup(&key) {
            self.states.remove_if_current(&key, state);
        }
        removed
    }

    #[inline]
    pub(crate) fn replace_if_current(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
        old_handle: SessionHandle,
        new_handle: SessionHandle,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        if !self
            .table
            .replace_if_current(&key, old_handle.raw(), new_handle.raw())
        {
            return false;
        }
        self.states.insert(key, SessionEndpointState::Ready.raw());
        true
    }

    #[inline]
    pub(crate) fn claim_migration(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
        handle: SessionHandle,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        if self.table.lookup(&key) != Some(handle.raw()) {
            return false;
        }
        self.states.replace_if_current(
            &key,
            SessionEndpointState::Opened.raw(),
            SessionEndpointState::Migrating.raw(),
        )
    }

    #[inline]
    pub(crate) fn cancel_migration(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
        handle: SessionHandle,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        if self.table.lookup(&key) != Some(handle.raw()) {
            return false;
        }
        self.states.replace_if_current(
            &key,
            SessionEndpointState::Migrating.raw(),
            SessionEndpointState::Opened.raw(),
        )
    }

    #[inline]
    pub(crate) fn publish_migration(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
        old_handle: SessionHandle,
        new_handle: SessionHandle,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        if !self
            .table
            .replace_if_current(&key, old_handle.raw(), new_handle.raw())
        {
            return false;
        }
        if self.states.replace_if_current(
            &key,
            SessionEndpointState::Migrating.raw(),
            SessionEndpointState::Ready.raw(),
        ) {
            return true;
        }
        self.table
            .replace_if_current(&key, new_handle.raw(), old_handle.raw());
        false
    }

    #[inline]
    pub(crate) fn mark_ready(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
        handle: SessionHandle,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        if self.table.lookup(&key) != Some(handle.raw()) {
            return false;
        }
        self.states.replace_if_current(
            &key,
            SessionEndpointState::Opened.raw(),
            SessionEndpointState::Ready.raw(),
        )
    }

    #[inline]
    pub(crate) fn lookup(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
    ) -> Option<SessionHandle> {
        let key = SessionEndpointKey::new(local, remote, transport)?;
        self.table.lookup(&key).map(SessionHandle::from)
    }

    #[inline]
    pub(crate) fn state(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
    ) -> Option<SessionEndpointState> {
        let key = SessionEndpointKey::new(local, remote, transport)?;
        self.states
            .lookup(&key)
            .and_then(SessionEndpointState::from_raw)
    }
}

#[inline]
fn ip_words(endpoint: SocketAddr) -> (u64, u64) {
    match endpoint {
        SocketAddr::V4(address) => (u64::from(u32::from(*address.ip())), 0),
        SocketAddr::V6(address) => {
            let octets = address.ip().octets();
            let mut first = [0; 8];
            let mut second = [0; 8];
            first.copy_from_slice(&octets[..8]);
            second.copy_from_slice(&octets[8..]);
            (u64::from_be_bytes(first), u64::from_be_bytes(second))
        }
    }
}
