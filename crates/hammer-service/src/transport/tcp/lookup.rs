use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_adapter::DataWorkerId;
use hammer_infra::map::{FlatHashKey, FlatHashTable};

pub type TcpLookupId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TcpLookupKind {
    Listener = 0,
    EstablishedConnection = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpLookupValue {
    pub kind: TcpLookupKind,
    pub id: TcpLookupId,
    pub owner_worker: DataWorkerId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpV4ListenerKey(u128);

impl TcpV4ListenerKey {
    #[inline]
    pub fn new(scope_id: u32, local_addr: Ipv4Addr, local_port: u16) -> Self {
        Self(
            (u128::from(scope_id) << 48)
                | (u128::from(u32::from(local_addr)) << 16)
                | u128::from(local_port),
        )
    }
}

impl FlatHashKey for TcpV4ListenerKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        self.0.hash_key()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpV4ConnectionKey(u128);

impl TcpV4ConnectionKey {
    #[inline]
    pub fn new(
        scope_id: u32,
        local_addr: Ipv4Addr,
        local_port: u16,
        remote_addr: Ipv4Addr,
        remote_port: u16,
    ) -> Self {
        Self(
            (u128::from(scope_id) << 96)
                | (u128::from(u32::from(local_addr)) << 64)
                | (u128::from(u32::from(remote_addr)) << 32)
                | (u128::from(local_port) << 16)
                | u128::from(remote_port),
        )
    }
}

impl FlatHashKey for TcpV4ConnectionKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        self.0.hash_key()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpV6ListenerKey {
    local_addr: u128,
    scope_port: u64,
}

impl TcpV6ListenerKey {
    #[inline]
    pub fn new(scope_id: u32, local_addr: Ipv6Addr, local_port: u16) -> Self {
        Self {
            local_addr: u128::from(local_addr),
            scope_port: (u64::from(scope_id) << 16) | u64::from(local_port),
        }
    }
}

impl FlatHashKey for TcpV6ListenerKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        hash_words(&[fold_u128(self.local_addr), self.scope_port])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpV6ConnectionKey {
    local_addr: u128,
    remote_addr: u128,
    scope_ports: u64,
}

impl TcpV6ConnectionKey {
    #[inline]
    pub fn new(
        scope_id: u32,
        local_addr: Ipv6Addr,
        local_port: u16,
        remote_addr: Ipv6Addr,
        remote_port: u16,
    ) -> Self {
        Self {
            local_addr: u128::from(local_addr),
            remote_addr: u128::from(remote_addr),
            scope_ports: (u64::from(scope_id) << 32)
                | (u64::from(local_port) << 16)
                | u64::from(remote_port),
        }
    }
}

impl FlatHashKey for TcpV6ConnectionKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        hash_words(&[
            fold_u128(self.local_addr),
            fold_u128(self.remote_addr),
            self.scope_ports,
        ])
    }
}

#[derive(Debug, Clone)]
pub struct TcpLookupSnapshot {
    listeners_v4: FlatHashTable<TcpV4ListenerKey, TcpLookupValue>,
    listeners_v6: FlatHashTable<TcpV6ListenerKey, TcpLookupValue>,
    connections_v4: FlatHashTable<TcpV4ConnectionKey, TcpLookupValue>,
    connections_v6: FlatHashTable<TcpV6ConnectionKey, TcpLookupValue>,
}

impl TcpLookupSnapshot {
    #[inline]
    pub fn empty() -> Self {
        Self {
            listeners_v4: FlatHashTable::new(),
            listeners_v6: FlatHashTable::new(),
            connections_v4: FlatHashTable::new(),
            connections_v6: FlatHashTable::new(),
        }
    }

    #[inline]
    pub fn lookup_v4(
        &self,
        connection: TcpV4ConnectionKey,
        listener: TcpV4ListenerKey,
    ) -> Option<TcpLookupValue> {
        self.lookup_connection_v4(connection)
            .or_else(|| self.lookup_listener_v4(listener))
    }

    #[inline]
    pub fn lookup_v6(
        &self,
        connection: TcpV6ConnectionKey,
        listener: TcpV6ListenerKey,
    ) -> Option<TcpLookupValue> {
        self.lookup_connection_v6(connection)
            .or_else(|| self.lookup_listener_v6(listener))
    }

    #[inline]
    pub fn lookup_listener_v4(&self, key: TcpV4ListenerKey) -> Option<TcpLookupValue> {
        self.listeners_v4.lookup(&key)
    }

    #[inline]
    pub fn lookup_listener_v6(&self, key: TcpV6ListenerKey) -> Option<TcpLookupValue> {
        self.listeners_v6.lookup(&key)
    }

    #[inline]
    pub fn lookup_connection_v4(&self, key: TcpV4ConnectionKey) -> Option<TcpLookupValue> {
        self.connections_v4.lookup(&key)
    }

    #[inline]
    pub fn lookup_connection_v6(&self, key: TcpV6ConnectionKey) -> Option<TcpLookupValue> {
        self.connections_v6.lookup(&key)
    }

    #[inline]
    pub(crate) fn insert_listener_v4(&mut self, key: TcpV4ListenerKey, value: TcpLookupValue) {
        self.listeners_v4.insert(key, value);
    }

    #[inline]
    pub(crate) fn insert_listener_v6(&mut self, key: TcpV6ListenerKey, value: TcpLookupValue) {
        self.listeners_v6.insert(key, value);
    }

    #[inline]
    pub(crate) fn insert_connection_v4(&mut self, key: TcpV4ConnectionKey, value: TcpLookupValue) {
        self.connections_v4.insert(key, value);
    }

    #[inline]
    pub(crate) fn insert_connection_v6(&mut self, key: TcpV6ConnectionKey, value: TcpLookupValue) {
        self.connections_v6.insert(key, value);
    }
}

impl Default for TcpLookupSnapshot {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone)]
pub struct TcpWorkerOwnedState {
    owner_worker: DataWorkerId,
    listeners_v4: FlatHashTable<TcpV4ListenerKey, TcpLookupValue>,
    listeners_v6: FlatHashTable<TcpV6ListenerKey, TcpLookupValue>,
    connections_v4: FlatHashTable<TcpV4ConnectionKey, TcpLookupValue>,
    connections_v6: FlatHashTable<TcpV6ConnectionKey, TcpLookupValue>,
}

impl TcpWorkerOwnedState {
    #[inline]
    pub fn new(owner_worker: DataWorkerId) -> Self {
        Self {
            owner_worker,
            listeners_v4: FlatHashTable::new(),
            listeners_v6: FlatHashTable::new(),
            connections_v4: FlatHashTable::new(),
            connections_v6: FlatHashTable::new(),
        }
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub fn insert_listener_v4(&mut self, key: TcpV4ListenerKey, id: TcpLookupId) {
        self.listeners_v4
            .insert(key, self.value(TcpLookupKind::Listener, id));
    }

    #[inline]
    pub fn insert_listener_v6(&mut self, key: TcpV6ListenerKey, id: TcpLookupId) {
        self.listeners_v6
            .insert(key, self.value(TcpLookupKind::Listener, id));
    }

    #[inline]
    pub fn insert_connection_v4(&mut self, key: TcpV4ConnectionKey, id: TcpLookupId) {
        self.connections_v4
            .insert(key, self.value(TcpLookupKind::EstablishedConnection, id));
    }

    #[inline]
    pub fn insert_connection_v6(&mut self, key: TcpV6ConnectionKey, id: TcpLookupId) {
        self.connections_v6
            .insert(key, self.value(TcpLookupKind::EstablishedConnection, id));
    }

    #[inline]
    pub fn publish_snapshot(&self) -> TcpLookupSnapshot {
        TcpLookupSnapshot {
            listeners_v4: self.listeners_v4.clone(),
            listeners_v6: self.listeners_v6.clone(),
            connections_v4: self.connections_v4.clone(),
            connections_v6: self.connections_v6.clone(),
        }
    }

    #[inline]
    fn value(&self, kind: TcpLookupKind, id: TcpLookupId) -> TcpLookupValue {
        TcpLookupValue {
            kind,
            id,
            owner_worker: self.owner_worker,
        }
    }
}

#[inline(always)]
fn fold_u128(value: u128) -> u64 {
    value as u64 ^ (value >> 64) as u64
}

#[inline(always)]
fn hash_words(words: &[u64]) -> usize {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for word in words {
        state ^= splitmix64(*word ^ state);
        state = state.rotate_left(13);
    }
    splitmix64(state) as usize
}

#[inline(always)]
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
