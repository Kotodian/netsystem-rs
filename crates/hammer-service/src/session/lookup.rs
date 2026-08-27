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
    connections: Bihash<SessionEndpointKey, 8>,
}

impl SessionEndpointLookup {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            connections: Bihash::new(SESSION_ENDPOINT_CAPACITY),
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
        self.connections
            .insert_if_absent(key, session.into())
            .is_ok()
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
        self.connections.remove(&key)
    }

    #[inline]
    pub(crate) fn lookup_connection(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: u8,
    ) -> Option<SessionHandle> {
        let key = SessionEndpointKey::new(local, remote, transport)?;
        self.connections.lookup(&key).map(SessionHandle::from)
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
