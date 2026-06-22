use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::{TcpCapabilities, TcpConnectionId};
use hammer_core::protocol::transport::TransportConnectionKey;
use hammer_infra::map::{FlatHashKey, FlatHashTable};
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::vec::Vec;

use crate::session::SessionId;
use crate::transport::tcp::TcpInputNext;

pub type TcpLookupId = u32;

#[derive(Debug)]
pub struct TcpConnectionRouteIndex {
    entries: Pool<TcpConnectionRouteEntry>,
    session_slots: FlatHashTable<u64, PoolIndex>,
    connection_slots: FlatHashTable<u64, PoolIndex>,
    tuple_slots: FlatHashTable<TransportConnectionKey, PoolIndex>,
}

#[derive(Debug)]
pub struct TcpPendingRouteIndex {
    entries: Pool<TcpPendingRouteEntry>,
    session_slots: FlatHashTable<u64, PoolIndex>,
    tuple_slots: FlatHashTable<TransportConnectionKey, PoolIndex>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPendingRouteEntry {
    session_id: SessionId,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    owner: DataWorkerId,
    next: TcpInputNext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpConnectionRouteEntry {
    session_id: SessionId,
    connection_id: Option<TcpConnectionId>,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    owner: DataWorkerId,
    next: TcpInputNext,
}

impl TcpConnectionRouteEntry {
    #[inline]
    fn new(
        session_id: SessionId,
        connection_id: Option<TcpConnectionId>,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            local,
            remote,
            owner,
            next,
        }
    }

    #[inline]
    fn tuple_key(self) -> Option<TransportConnectionKey> {
        self.local
            .and_then(|local| TransportConnectionKey::from_socket_addrs(0, local, self.remote))
    }
}

impl TcpPendingRouteEntry {
    #[inline]
    fn new(
        session_id: SessionId,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) -> Self {
        Self {
            session_id,
            local,
            remote,
            owner,
            next,
        }
    }

    #[inline]
    fn tuple_key(self) -> Option<TransportConnectionKey> {
        self.local
            .and_then(|local| TransportConnectionKey::from_socket_addrs(0, local, self.remote))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpLookupValue {
    pub id: TcpLookupId,
    pub owner_worker: DataWorkerId,
    pub capabilities: TcpCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TcpFastOpenCacheEntry {
    local: SocketAddr,
    remote: SocketAddr,
    cookie: [u8; 16],
    cookie_len: u8,
    max_segment_size: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpFastOpenSecret {
    listener_id: TcpLookupId,
    secret: [u8; 16],
}

pub trait TcpListenerAddress: Copy + Eq {
    type Ip;
    type Key: FlatHashKey;

    fn key(scope_id: u32, local_addr: Self::Ip, local_port: u16) -> Self::Key;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpIpv4ListenerAddress;

impl TcpListenerAddress for TcpIpv4ListenerAddress {
    type Ip = Ipv4Addr;
    type Key = TcpListenerKey<Self>;

    #[inline]
    fn key(scope_id: u32, local_addr: Ipv4Addr, local_port: u16) -> Self::Key {
        TcpListenerKey::from_words(
            (u128::from(scope_id) << 48)
                | (u128::from(u32::from(local_addr)) << 16)
                | u128::from(local_port),
            0,
            Self,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpIpv6ListenerAddress;

impl TcpListenerAddress for TcpIpv6ListenerAddress {
    type Ip = Ipv6Addr;
    type Key = TcpListenerKey<Self>;

    #[inline]
    fn key(scope_id: u32, local_addr: Ipv6Addr, local_port: u16) -> Self::Key {
        TcpListenerKey::from_words(
            u128::from(local_addr),
            (u64::from(scope_id) << 16) | u64::from(local_port),
            Self,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpListenerKey<A: TcpListenerAddress> {
    words: [u128; 2],
    address: A,
}

pub type TcpV4ListenerKey = TcpListenerKey<TcpIpv4ListenerAddress>;
pub type TcpV6ListenerKey = TcpListenerKey<TcpIpv6ListenerAddress>;

impl<A: TcpListenerAddress> TcpListenerKey<A> {
    #[inline]
    fn from_words(first: u128, second: u64, address: A) -> Self {
        Self {
            words: [first, u128::from(second)],
            address,
        }
    }
}

impl TcpV4ListenerKey {
    #[inline]
    pub fn new(scope_id: u32, local_addr: Ipv4Addr, local_port: u16) -> Self {
        TcpIpv4ListenerAddress::key(scope_id, local_addr, local_port)
    }
}

impl TcpV6ListenerKey {
    #[inline]
    pub fn new(scope_id: u32, local_addr: Ipv6Addr, local_port: u16) -> Self {
        TcpIpv6ListenerAddress::key(scope_id, local_addr, local_port)
    }
}

impl<A: TcpListenerAddress> FlatHashKey for TcpListenerKey<A> {
    #[inline(always)]
    fn hash_key(self) -> usize {
        hash_words(&[fold_u128(self.words[0]), fold_u128(self.words[1])])
    }
}

#[derive(Debug, Clone)]
pub struct TcpListenerTable<A: TcpListenerAddress> {
    entries: FlatHashTable<A::Key, TcpLookupValue>,
}

impl<A: TcpListenerAddress> TcpListenerTable<A> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            entries: FlatHashTable::new(),
        }
    }

    #[inline]
    pub fn lookup(&self, key: A::Key) -> Option<TcpLookupValue> {
        self.entries.lookup(&key)
    }

    #[inline]
    pub fn prefetch(&self, key: A::Key) {
        self.entries.prefetch_key(&key);
    }

    #[inline]
    pub fn insert(&mut self, key: A::Key, value: TcpLookupValue) {
        self.entries.insert(key, value);
    }
}

impl<A: TcpListenerAddress> Default for TcpListenerTable<A> {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone)]
pub struct TcpListenerLookup {
    v4: TcpListenerTable<TcpIpv4ListenerAddress>,
    v6: TcpListenerTable<TcpIpv6ListenerAddress>,
}

impl TcpListenerLookup {
    #[inline]
    pub fn empty() -> Self {
        Self {
            v4: TcpListenerTable::empty(),
            v6: TcpListenerTable::empty(),
        }
    }

    #[inline]
    pub fn v4(&self) -> &TcpListenerTable<TcpIpv4ListenerAddress> {
        &self.v4
    }

    #[inline]
    pub fn v6(&self) -> &TcpListenerTable<TcpIpv6ListenerAddress> {
        &self.v6
    }

    #[inline]
    pub fn v4_mut(&mut self) -> &mut TcpListenerTable<TcpIpv4ListenerAddress> {
        &mut self.v4
    }

    #[inline]
    pub fn v6_mut(&mut self) -> &mut TcpListenerTable<TcpIpv6ListenerAddress> {
        &mut self.v6
    }
}

impl Default for TcpListenerLookup {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone)]
pub struct TcpLookupSnapshot {
    listeners: TcpListenerLookup,
}

impl TcpLookupSnapshot {
    #[inline]
    pub fn empty() -> Self {
        Self {
            listeners: TcpListenerLookup::empty(),
        }
    }

    #[inline]
    pub fn lookup_listener<A: TcpListenerAddress>(&self, key: A::Key) -> Option<TcpLookupValue>
    where
        Self: TcpListenerLookupAccess<A>,
    {
        self.listener_table().lookup(key)
    }

    #[inline]
    pub fn prefetch_listener<A: TcpListenerAddress>(&self, key: A::Key)
    where
        Self: TcpListenerLookupAccess<A>,
    {
        self.listener_table().prefetch(key);
    }

    #[inline]
    pub(crate) fn insert_listener<A: TcpListenerAddress>(
        &mut self,
        key: A::Key,
        value: TcpLookupValue,
    ) where
        Self: TcpListenerLookupAccess<A>,
    {
        self.listener_table_mut().insert(key, value);
    }
}

pub trait TcpListenerLookupAccess<A: TcpListenerAddress> {
    fn listener_table(&self) -> &TcpListenerTable<A>;
    fn listener_table_mut(&mut self) -> &mut TcpListenerTable<A>;
}

impl TcpListenerLookupAccess<TcpIpv4ListenerAddress> for TcpLookupSnapshot {
    #[inline]
    fn listener_table(&self) -> &TcpListenerTable<TcpIpv4ListenerAddress> {
        self.listeners.v4()
    }

    #[inline]
    fn listener_table_mut(&mut self) -> &mut TcpListenerTable<TcpIpv4ListenerAddress> {
        self.listeners.v4_mut()
    }
}

impl TcpListenerLookupAccess<TcpIpv6ListenerAddress> for TcpLookupSnapshot {
    #[inline]
    fn listener_table(&self) -> &TcpListenerTable<TcpIpv6ListenerAddress> {
        self.listeners.v6()
    }

    #[inline]
    fn listener_table_mut(&mut self) -> &mut TcpListenerTable<TcpIpv6ListenerAddress> {
        self.listeners.v6_mut()
    }
}

impl Default for TcpLookupSnapshot {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

impl TcpConnectionRouteIndex {
    #[inline]
    pub fn empty() -> Self {
        Self {
            entries: Pool::with_capacity(1024),
            session_slots: FlatHashTable::with_capacity(1024),
            connection_slots: FlatHashTable::with_capacity(1024),
            tuple_slots: FlatHashTable::with_capacity(1024),
        }
    }

    #[inline]
    pub fn upsert(
        &mut self,
        session_id: SessionId,
        connection_id: Option<TcpConnectionId>,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        let entry =
            TcpConnectionRouteEntry::new(session_id, connection_id, local, remote, owner, next);
        let key = session_id.get();
        if let Some(entry_index) = self.session_slots.lookup(&key) {
            let old_entry = *self
                .entries
                .get(entry_index)
                .expect("tcp connection route entry index is valid");
            self.unindex_entry(entry_index, old_entry);
            *self
                .entries
                .get_mut(entry_index)
                .expect("tcp connection route entry index is valid") = entry;
            self.index_entry(entry_index, entry);
            return;
        }
        let entry_index = self
            .entries
            .insert(entry)
            .expect("tcp connection route entry pool exhausted");
        self.index_entry(entry_index, entry);
    }

    #[inline]
    fn index_entry(&mut self, entry_index: PoolIndex, entry: TcpConnectionRouteEntry) {
        self.session_slots
            .insert(entry.session_id.get(), entry_index);
        if let Some(connection_id) = entry.connection_id {
            self.connection_slots
                .insert(connection_id.get(), entry_index);
        }
        if let Some(key) = entry.tuple_key() {
            self.tuple_slots.insert(key, entry_index);
        }
    }

    #[inline]
    fn unindex_entry(&mut self, entry_index: PoolIndex, entry: TcpConnectionRouteEntry) {
        if self.session_slots.lookup(&entry.session_id.get()) == Some(entry_index) {
            self.session_slots.remove(&entry.session_id.get());
        }
        if let Some(connection_id) = entry.connection_id {
            let key = connection_id.get();
            if self.connection_slots.lookup(&key) == Some(entry_index) {
                self.connection_slots.remove(&key);
            }
        }
        if let Some(key) = entry.tuple_key()
            && self.tuple_slots.lookup(&key) == Some(entry_index)
        {
            self.tuple_slots.remove(&key);
        }
    }

    #[inline]
    pub fn lookup_by_connection_id(&self, connection_id: TcpConnectionId) -> Option<SessionId> {
        let entry_index = self.connection_slots.lookup(&connection_id.get())?;
        Some(
            self.entries
                .get(entry_index)
                .expect("tcp connection route entry index is valid")
                .session_id,
        )
    }

    #[inline]
    pub fn prefetch_tuple(&self, local: SocketAddr, remote: SocketAddr) {
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return;
        };
        self.tuple_slots.prefetch_key(&key);
    }

    #[inline]
    pub fn lookup_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        let entry_index = self
            .tuple_slots
            .lookup(&TransportConnectionKey::from_socket_addrs(
                0, local, remote,
            )?)?;
        let entry = self
            .entries
            .get(entry_index)
            .expect("tcp connection route entry index is valid");
        Some((entry.session_id, entry.owner, entry.next))
    }

    pub fn forget_session(&mut self, session_id: SessionId) {
        let key = session_id.get();
        let Some(entry_index) = self.session_slots.lookup(&key) else {
            return;
        };
        let removed = *self
            .entries
            .get(entry_index)
            .expect("tcp connection route entry index is valid");
        self.unindex_entry(entry_index, removed);
        let _ = self
            .entries
            .remove(entry_index)
            .expect("tcp connection route entry index is valid");
    }
}

impl Default for TcpConnectionRouteIndex {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

impl Clone for TcpConnectionRouteIndex {
    fn clone(&self) -> Self {
        let mut cloned = Self::empty();
        for (_, entry) in self.entries.iter() {
            cloned.upsert(
                entry.session_id,
                entry.connection_id,
                entry.local,
                entry.remote,
                entry.owner,
                entry.next,
            );
        }
        cloned
    }
}

impl TcpPendingRouteIndex {
    #[inline]
    pub fn empty() -> Self {
        Self {
            entries: Pool::with_capacity(1024),
            session_slots: FlatHashTable::with_capacity(1024),
            tuple_slots: FlatHashTable::with_capacity(1024),
        }
    }

    #[inline]
    pub fn upsert(
        &mut self,
        session_id: SessionId,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        let entry = TcpPendingRouteEntry::new(session_id, local, remote, owner, next);
        let key = session_id.get();
        if let Some(entry_index) = self.session_slots.lookup(&key) {
            let old_entry = *self
                .entries
                .get(entry_index)
                .expect("tcp pending route entry index is valid");
            self.unindex_entry(entry_index, old_entry);
            *self
                .entries
                .get_mut(entry_index)
                .expect("tcp pending route entry index is valid") = entry;
            self.index_entry(entry_index, entry);
            return;
        }
        let entry_index = self
            .entries
            .insert(entry)
            .expect("tcp pending route entry pool exhausted");
        self.index_entry(entry_index, entry);
    }

    #[inline]
    fn index_entry(&mut self, entry_index: PoolIndex, entry: TcpPendingRouteEntry) {
        self.session_slots
            .insert(entry.session_id.get(), entry_index);
        if let Some(key) = entry.tuple_key() {
            self.tuple_slots.insert(key, entry_index);
        }
    }

    #[inline]
    fn unindex_entry(&mut self, entry_index: PoolIndex, entry: TcpPendingRouteEntry) {
        if self.session_slots.lookup(&entry.session_id.get()) == Some(entry_index) {
            self.session_slots.remove(&entry.session_id.get());
        }
        if let Some(key) = entry.tuple_key()
            && self.tuple_slots.lookup(&key) == Some(entry_index)
        {
            self.tuple_slots.remove(&key);
        }
    }

    pub fn lookup_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        let entry_index = self
            .tuple_slots
            .lookup(&TransportConnectionKey::from_socket_addrs(
                0, local, remote,
            )?)?;
        let entry = self
            .entries
            .get(entry_index)
            .expect("tcp pending route entry index is valid");
        Some((entry.session_id, entry.owner, entry.next))
    }

    #[inline]
    pub fn prefetch_tuple(&self, local: SocketAddr, remote: SocketAddr) {
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return;
        };
        self.tuple_slots.prefetch_key(&key);
    }

    pub fn forget_session(&mut self, session_id: SessionId) {
        let key = session_id.get();
        let Some(entry_index) = self.session_slots.lookup(&key) else {
            return;
        };
        let removed = *self
            .entries
            .get(entry_index)
            .expect("tcp pending route entry index is valid");
        self.unindex_entry(entry_index, removed);
        let _ = self
            .entries
            .remove(entry_index)
            .expect("tcp pending route entry index is valid");
    }
}

impl Default for TcpPendingRouteIndex {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

impl Clone for TcpPendingRouteIndex {
    fn clone(&self) -> Self {
        let mut cloned = Self::empty();
        for (_, entry) in self.entries.iter() {
            cloned.upsert(
                entry.session_id,
                entry.local,
                entry.remote,
                entry.owner,
                entry.next,
            );
        }
        cloned
    }
}

#[derive(Debug)]
pub struct TcpWorkerOwnedState {
    owner_worker: DataWorkerId,
    listeners: TcpLookupSnapshot,
    connections: TcpConnectionRouteIndex,
    pending: TcpPendingRouteIndex,
    fast_open_cache: Vec<TcpFastOpenCacheEntry>,
    fast_open_secrets: Vec<TcpFastOpenSecret>,
    next_iss: u32,
}

impl TcpWorkerOwnedState {
    #[inline]
    pub fn new(owner_worker: DataWorkerId) -> Self {
        Self {
            owner_worker,
            listeners: TcpLookupSnapshot::empty(),
            connections: TcpConnectionRouteIndex::empty(),
            pending: TcpPendingRouteIndex::empty(),
            fast_open_cache: Vec::new(),
            fast_open_secrets: Vec::new(),
            next_iss: 81_000,
        }
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub fn insert_listener<A: TcpListenerAddress>(
        &mut self,
        key: A::Key,
        id: TcpLookupId,
        capabilities: TcpCapabilities,
    ) where
        TcpLookupSnapshot: TcpListenerLookupAccess<A>,
    {
        self.listeners
            .insert_listener::<A>(key, self.value(id, capabilities));
    }

    #[inline]
    pub fn publish_snapshot(&self) -> TcpLookupSnapshot {
        self.listeners.clone()
    }

    #[inline]
    pub fn remember_session(
        &mut self,
        session_id: SessionId,
        connection_id: Option<TcpConnectionId>,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        self.connections
            .upsert(session_id, connection_id, local, remote, owner, next);
    }

    #[inline]
    pub fn remember_pending_open(
        &mut self,
        session_id: SessionId,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        self.pending.upsert(session_id, local, remote, owner, next);
    }

    #[inline]
    pub fn session_route_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.connections.lookup_by_tuple(local, remote)
    }

    #[inline]
    pub fn pending_route_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.pending.lookup_by_tuple(local, remote)
    }

    #[inline]
    pub fn prefetch_tuple(&self, local: SocketAddr, remote: SocketAddr) {
        self.connections.prefetch_tuple(local, remote);
        self.pending.prefetch_tuple(local, remote);
    }

    #[inline]
    pub fn session_id_by_connection_id(&self, connection_id: TcpConnectionId) -> Option<SessionId> {
        self.connections.lookup_by_connection_id(connection_id)
    }

    #[inline]
    pub fn forget_session(&mut self, session_id: SessionId) {
        self.connections.forget_session(session_id);
    }

    #[inline]
    pub fn forget_pending_open(&mut self, session_id: SessionId) {
        self.pending.forget_session(session_id);
    }

    #[inline]
    pub fn next_initial_sequence(&mut self, local: SocketAddr, remote: SocketAddr) -> u32 {
        let mut value = self.next_iss;
        value ^= u32::from(local.port()) << 16 | u32::from(remote.port());
        value ^= match (local.ip(), remote.ip()) {
            (std::net::IpAddr::V4(local), std::net::IpAddr::V4(remote)) => {
                u32::from(local) ^ u32::from(remote).rotate_left(13)
            }
            (std::net::IpAddr::V6(local), std::net::IpAddr::V6(remote)) => {
                let local = u128::from(local);
                let remote = u128::from(remote);
                (local as u32) ^ ((local >> 64) as u32) ^ (remote as u32).rotate_left(7)
            }
            _ => 0x9e37_79b9,
        };
        self.next_iss = self.next_iss.wrapping_add(64_099);
        value.max(1)
    }

    pub fn fast_open_cookie(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(&[u8], Option<u16>)> {
        self.fast_open_cache
            .iter()
            .find(|entry| entry.local == local && entry.remote == remote)
            .map(|entry| {
                (
                    &entry.cookie[..usize::from(entry.cookie_len)],
                    entry.max_segment_size,
                )
            })
    }

    pub fn remember_fast_open_cookie(
        &mut self,
        local: SocketAddr,
        remote: SocketAddr,
        cookie: &[u8],
        max_segment_size: Option<u16>,
    ) {
        let mut copied = [0u8; 16];
        let len = cookie.len().min(copied.len());
        copied[..len].copy_from_slice(&cookie[..len]);
        if let Some(entry) = self
            .fast_open_cache
            .iter_mut()
            .find(|entry| entry.local == local && entry.remote == remote)
        {
            entry.cookie = copied;
            entry.cookie_len = len as u8;
            entry.max_segment_size = max_segment_size;
            return;
        }
        self.fast_open_cache.push(TcpFastOpenCacheEntry {
            local,
            remote,
            cookie: copied,
            cookie_len: len as u8,
            max_segment_size,
        });
    }

    pub fn fast_open_cookie_for_listener(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> [u8; 16] {
        let secret = if let Some(secret) = self
            .fast_open_secrets
            .iter()
            .find(|secret| secret.listener_id == listener_id)
        {
            secret.secret
        } else {
            let mut secret = [0u8; 16];
            secret[..4].copy_from_slice(&listener_id.to_be_bytes());
            secret[4..8].copy_from_slice(&self.next_iss.to_be_bytes());
            match (local.ip(), remote.ip()) {
                (std::net::IpAddr::V4(local_ip), std::net::IpAddr::V4(remote_ip)) => {
                    secret[8..12].copy_from_slice(&u32::from(local_ip).to_be_bytes());
                    secret[12..16].copy_from_slice(&u32::from(remote_ip).to_be_bytes());
                }
                _ => {
                    secret[8..10].copy_from_slice(&local.port().to_be_bytes());
                    secret[10..12].copy_from_slice(&remote.port().to_be_bytes());
                    secret[12..16].copy_from_slice(&self.next_iss.rotate_left(11).to_be_bytes());
                }
            }
            self.fast_open_secrets.push(TcpFastOpenSecret {
                listener_id,
                secret,
            });
            secret
        };
        let mut cookie = secret;
        cookie[..2].copy_from_slice(&local.port().to_be_bytes());
        cookie[2..4].copy_from_slice(&remote.port().to_be_bytes());
        cookie
    }

    pub fn validate_fast_open_cookie(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        cookie: &[u8],
    ) -> bool {
        cookie == self.fast_open_cookie_for_listener(listener_id, local, remote)
    }

    #[inline]
    fn value(&self, id: TcpLookupId, capabilities: TcpCapabilities) -> TcpLookupValue {
        TcpLookupValue {
            id,
            owner_worker: self.owner_worker,
            capabilities,
        }
    }

    pub fn register_queue<C>(
        worker: DataWorkerId,
        buffers: hammer_adapter::DataPlaneBuffers,
    ) -> hammer_core::error::CoreResult<crate::transport::tcp::TcpQueueHandle<C>>
    where
        C: crate::transport::congestion::CongestionController + 'static,
    {
        crate::transport::tcp::register_tcp_session_queue::<C>(worker, buffers)
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
