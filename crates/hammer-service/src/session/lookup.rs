use std::net::SocketAddr;

use hammer_infra::bihash::{Bihash, BihashKey};
use hammer_runtime::app::SessionHandle;

const SESSION_ENDPOINT_CAPACITY: u32 = 1024;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct SessionEndpointKey([u64; 6]);

impl SessionEndpointKey {
    #[inline]
    fn new(local: SocketAddr, remote: SocketAddr, transport: u8) -> Option<Self> {
        if local.is_ipv4() != remote.is_ipv4() {
            return None;
        }
        let (local_hi, local_lo) = ip_words(local);
        let (remote_hi, remote_lo) = ip_words(remote);
        Some(Self([
            (u64::from(transport) << 8) | u64::from(if local.is_ipv4() { 4u8 } else { 6u8 }),
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
    session_indices: Bihash<SessionEndpointKey, 8>,
    thread_indices: Bihash<SessionEndpointKey, 8>,
}

impl SessionEndpointLookup {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            session_indices: Bihash::new(SESSION_ENDPOINT_CAPACITY),
            thread_indices: Bihash::new(SESSION_ENDPOINT_CAPACITY),
        }
    }

    #[inline]
    pub(crate) fn add_connection(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: u8,
        session: SessionHandle,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        if self
            .session_indices
            .insert_if_absent(key, session.session_index.into())
            .is_err()
        {
            return false;
        }
        if self
            .thread_indices
            .insert_if_absent(key, session.thread_index.into())
            .is_err()
        {
            self.session_indices.remove(&key);
            return false;
        }
        true
    }

    #[inline]
    pub(crate) fn del_connection(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: u8,
    ) -> bool {
        let Some(key) = SessionEndpointKey::new(local, remote, transport) else {
            return false;
        };
        let removed = self.session_indices.remove(&key);
        let thread_removed = self.thread_indices.remove(&key);
        debug_assert_eq!(removed, thread_removed);
        removed
    }

    #[inline]
    pub(crate) fn lookup_connection(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: u8,
    ) -> Option<SessionHandle> {
        let key = SessionEndpointKey::new(local, remote, transport)?;
        let session_index = self.session_indices.lookup(&key)?;
        let thread_index = self.thread_indices.lookup(&key)?;
        Some(SessionHandle::new(
            u32::try_from(session_index).expect("Session index fits u32"),
            u32::try_from(thread_index).expect("Session thread index fits u32"),
        ))
    }
}

#[inline]
fn ip_words(endpoint: SocketAddr) -> (u64, u64) {
    match endpoint {
        SocketAddr::V4(address) => (u32::from(*address.ip()).into(), 0),
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
