use std::net::SocketAddr;

use hammer_infra::bihash::{Bihash, BihashKey};
use hammer_infra::pool::{Index, Pool};

use crate::connection::UdpConnection;

const UDP_TUPLE_CAPACITY: u32 = 1024;

#[inline(always)]
fn pool_index_value(index: Index) -> u64 {
    (u64::from(index.generation()) << 32) | u64::from(index.slot())
}

#[inline(always)]
fn pool_index_from_value(value: u64) -> Index {
    Index::new(value as u32, (value >> 32) as u32)
}

#[inline(always)]
fn ip_words(ip: SocketAddr) -> (u64, u64) {
    match ip {
        SocketAddr::V4(address) => (u64::from(u32::from(*address.ip())), 0),
        SocketAddr::V6(address) => {
            let octets = address.ip().octets();
            (
                u64::from_be_bytes(octets[..8].try_into().expect("IPv6 half has eight bytes")),
                u64::from_be_bytes(octets[8..].try_into().expect("IPv6 half has eight bytes")),
            )
        }
    }
}

/// Exact UDP 4-tuple key. The version and both port values are included in
/// the key, so IPv4, IPv6, and adjacent port pairs cannot alias.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct UdpTupleKey([u64; 6]);

impl UdpTupleKey {
    #[inline]
    fn new(local: SocketAddr, remote: SocketAddr) -> Option<Self> {
        if local.is_ipv4() != remote.is_ipv4() {
            return None;
        }
        let (local_hi, local_lo) = ip_words(local);
        let (remote_hi, remote_lo) = ip_words(remote);
        Some(Self([
            if local.is_ipv4() { 4 } else { 6 },
            local_hi,
            local_lo,
            remote_hi,
            remote_lo,
            (u64::from(local.port()) << 16) | u64::from(remote.port()),
        ]))
    }
}

/// VPP `session_lookup_safe4/6` shared table for connected UDP tuples.
///
/// The value is a `SessionHandle` (worker index in the high word and session
/// slot in the low word). It is used only to classify a packet that reaches a
/// non-owning Data Worker; it is not a handoff route or port dispatch table.
pub(crate) struct UdpSessionLookup {
    table: Bihash<UdpTupleKey, 8>,
}

impl UdpSessionLookup {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            table: Bihash::new(UDP_TUPLE_CAPACITY),
        }
    }

    #[inline]
    pub(crate) fn insert(&self, local: SocketAddr, remote: SocketAddr, handle: u64) {
        if let Some(key) = UdpTupleKey::new(local, remote) {
            self.table.insert(key, handle);
        }
    }

    #[inline]
    pub(crate) fn remove(&self, local: SocketAddr, remote: SocketAddr) {
        if let Some(key) = UdpTupleKey::new(local, remote) {
            self.table.remove(&key);
        }
    }

    #[inline]
    pub(crate) fn lookup(&self, local: SocketAddr, remote: SocketAddr) -> Option<u64> {
        let key = UdpTupleKey::new(local, remote)?;
        self.table.lookup(&key)
    }
}

impl BihashKey for UdpTupleKey {
    #[inline(always)]
    fn hash(self) -> u64 {
        hammer_infra::bihash::hash_words(&self.0)
    }
}

/// Worker-local UDP tuple and listener indexes. `Bihash` is used for the
/// connected tuple hot path; listener matching remains a small worker-owned
/// list because wildcard local addresses require exact-before-wildcard search.
pub(crate) struct UdpLookup {
    tuples: Bihash<UdpTupleKey, 8>,
}

impl UdpLookup {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            tuples: Bihash::new(UDP_TUPLE_CAPACITY),
        }
    }

    #[inline]
    pub(crate) fn insert_tuple(&self, index: Index, local: SocketAddr, remote: SocketAddr) -> bool {
        let Some(key) = UdpTupleKey::new(local, remote) else {
            return false;
        };
        self.tuples.insert(key, pool_index_value(index));
        true
    }

    #[inline]
    pub(crate) fn remove_tuple(&self, local: SocketAddr, remote: SocketAddr) {
        if let Some(key) = UdpTupleKey::new(local, remote) {
            self.tuples.remove(&key);
        }
    }

    #[inline]
    pub(crate) fn find_tuple(
        &self,
        connections: &Pool<UdpConnection>,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<Index> {
        let key = UdpTupleKey::new(local, remote)?;
        let index = pool_index_from_value(self.tuples.lookup(&key)?);
        connections.contains_key(index).then_some(index)
    }
}
