use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_core::protocol::transport::TransportConnectionKey;
use hammer_infra::map::{FlatHashKey, FlatHashTable};

use crate::session::SessionId;
use crate::transport::tcp::TcpInputNext;

pub type TcpLookupId = u32;

#[derive(Debug, Clone)]
pub struct TcpConnectionRouteIndex {
    entries: hammer_infra::vec::Vec<TcpConnectionRouteEntry>,
    connection_slots: FlatHashTable<u64, SessionId>,
    tuple_slots: FlatHashTable<TransportConnectionKey, SessionId>,
}

#[derive(Debug, Clone)]
pub struct TcpPendingRouteIndex {
    entries: hammer_infra::vec::Vec<TcpPendingRouteEntry>,
    tuple_slots: FlatHashTable<TransportConnectionKey, SessionId>,
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
            entries: hammer_infra::vec::Vec::new(),
            connection_slots: FlatHashTable::new(),
            tuple_slots: FlatHashTable::new(),
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
        if let Some(position) = self
            .entries
            .iter()
            .position(|existing| existing.session_id == session_id)
        {
            let old_entry = self.entries[position];
            self.unindex_entry(old_entry);
            self.entries[position] = entry;
        } else {
            self.entries.push(entry);
        }
        self.index_entry(entry);
    }

    #[inline]
    fn index_entry(&mut self, entry: TcpConnectionRouteEntry) {
        if let Some(connection_id) = entry.connection_id {
            self.connection_slots
                .insert(connection_id.get(), entry.session_id);
        }
        if let Some(key) = entry.tuple_key() {
            self.tuple_slots.insert(key, entry.session_id);
        }
    }

    #[inline]
    fn unindex_entry(&mut self, entry: TcpConnectionRouteEntry) {
        if let Some(connection_id) = entry.connection_id {
            let key = connection_id.get();
            if self.connection_slots.lookup(&key) == Some(entry.session_id) {
                self.connection_slots.remove(&key);
            }
        }
        if let Some(key) = entry.tuple_key()
            && self.tuple_slots.lookup(&key) == Some(entry.session_id)
        {
            self.tuple_slots.remove(&key);
        }
    }

    #[inline]
    pub fn lookup_by_connection_id(&self, connection_id: TcpConnectionId) -> Option<SessionId> {
        self.connection_slots.lookup(&connection_id.get())
    }

    #[inline]
    pub fn lookup_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        let session_id = self
            .tuple_slots
            .lookup(&TransportConnectionKey::from_socket_addrs(
                0, local, remote,
            )?)?;
        self.entries
            .iter()
            .find(|entry| entry.session_id == session_id)
            .map(|entry| (entry.session_id, entry.owner, entry.next))
    }

    pub fn forget_session(&mut self, session_id: SessionId) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.session_id == session_id)
        {
            let removed = self.entries[index];
            self.unindex_entry(removed);
            let last = self
                .entries
                .pop()
                .expect("connection route entry exists at computed index");
            if index != self.entries.len() {
                self.entries[index] = last;
            }
        }
    }
}

impl Default for TcpConnectionRouteIndex {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

impl TcpPendingRouteIndex {
    #[inline]
    pub fn empty() -> Self {
        Self {
            entries: hammer_infra::vec::Vec::new(),
            tuple_slots: FlatHashTable::new(),
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
        if let Some(position) = self
            .entries
            .iter()
            .position(|existing| existing.session_id == session_id)
        {
            let old_entry = self.entries[position];
            self.unindex_entry(old_entry);
            self.entries[position] = entry;
        } else {
            self.entries.push(entry);
        }
        self.index_entry(entry);
    }

    #[inline]
    fn index_entry(&mut self, entry: TcpPendingRouteEntry) {
        if let Some(key) = entry.tuple_key() {
            self.tuple_slots.insert(key, entry.session_id);
        }
    }

    #[inline]
    fn unindex_entry(&mut self, entry: TcpPendingRouteEntry) {
        if let Some(key) = entry.tuple_key()
            && self.tuple_slots.lookup(&key) == Some(entry.session_id)
        {
            self.tuple_slots.remove(&key);
        }
    }

    pub fn lookup_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        let session_id = self
            .tuple_slots
            .lookup(&TransportConnectionKey::from_socket_addrs(
                0, local, remote,
            )?)?;
        self.entries
            .iter()
            .find(|entry| entry.session_id == session_id)
            .map(|entry| (entry.session_id, entry.owner, entry.next))
    }

    pub fn forget_session(&mut self, session_id: SessionId) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.session_id == session_id)
        {
            let removed = self.entries[index];
            self.unindex_entry(removed);
            let last = self
                .entries
                .pop()
                .expect("pending route entry exists at computed index");
            if index != self.entries.len() {
                self.entries[index] = last;
            }
        }
    }
}

impl Default for TcpPendingRouteIndex {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug)]
pub struct TcpWorkerOwnedState {
    owner_worker: DataWorkerId,
    listeners: TcpLookupSnapshot,
    connections: TcpConnectionRouteIndex,
    pending: TcpPendingRouteIndex,
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
    pub fn insert_listener<A: TcpListenerAddress>(&mut self, key: A::Key, id: TcpLookupId)
    where
        TcpLookupSnapshot: TcpListenerLookupAccess<A>,
    {
        self.listeners.insert_listener::<A>(key, self.value(id));
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

    #[inline]
    fn value(&self, id: TcpLookupId) -> TcpLookupValue {
        TcpLookupValue {
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
