use std::cell::{Cell, RefCell};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_utils::CachePadded;
use hammer_adapter::{DataWorkerId, NodeRuntimeData};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpConnectionId, TcpFastOpenCookie, TcpTimestampOption,
};
use hammer_core::protocol::transport::TransportConnectionKey;
use hammer_infra::map::{FlatHashKey, FlatHashTable};
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::vec::Vec;

use crate::session::SessionId;
use crate::transport::congestion::CongestionController;
use crate::transport::tcp::{TcpConnection, TcpInputNext, TcpState};

pub type TcpLookupId = u32;

thread_local! {
    static TCP_WORKER_STATE: Cell<*mut TcpWorkerOwnedState> = const { Cell::new(std::ptr::null_mut()) };
    static TCP_WORKER_STATE_OWNER: RefCell<Option<Box<TcpWorkerOwnedState>>> = const { RefCell::new(None) };
}

pub(crate) fn set_tcp_worker_state(state: &mut TcpWorkerOwnedState) {
    TCP_WORKER_STATE.with(|cell| cell.set(state as *mut _));
}

// VPP alignment: `vlib_worker_threads` set up `vlib_per_thread_main` at worker
// thread entry before the worker's first frame dispatch. `install_tcp_worker_state`
// is the Rust mirror: it moves a `TcpWorkerOwnedState` into per-thread ownership
// (so the box is dropped when the worker thread exits) and points
// `TCP_WORKER_STATE` at it. Must be called on the worker thread before any
// graph-wiring reads `tcp_worker_state()` (e.g. `init_graph` / `wire_worker_graph`).
pub(crate) fn install_tcp_worker_state(state: TcpWorkerOwnedState) {
    let mut boxed = Box::new(state);
    let ptr: *mut TcpWorkerOwnedState = &mut *boxed;
    TCP_WORKER_STATE_OWNER.with(|slot| {
        *slot.borrow_mut() = Some(boxed);
    });
    TCP_WORKER_STATE.with(|cell| cell.set(ptr));
}

pub(crate) fn tcp_worker_state() -> &'static TcpWorkerOwnedState {
    let ptr = TCP_WORKER_STATE.with(|cell| cell.get());
    assert!(!ptr.is_null(), "TCP worker state not initialized");
    unsafe { &*ptr }
}

pub(crate) fn tcp_worker_state_mut() -> &'static mut TcpWorkerOwnedState {
    let ptr = TCP_WORKER_STATE.with(|cell| cell.get());
    assert!(!ptr.is_null(), "TCP worker state not initialized");
    unsafe { &mut *ptr }
}

#[derive(Debug)]
struct TcpConnectionRouteIndex {
    entries: Pool<TcpConnectionRouteEntry>,
    session_slots: FlatHashTable<u64, PoolIndex>,
    connection_slots: FlatHashTable<u64, PoolIndex>,
    tuple_slots: FlatHashTable<TransportConnectionKey, PoolIndex>,
}

#[derive(Debug)]
struct TcpPendingRouteIndex {
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
    capabilities: TcpCapabilities,
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
        capabilities: TcpCapabilities,
    ) -> Self {
        Self {
            session_id,
            local,
            remote,
            owner,
            next,
            capabilities,
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
    cookie: TcpFastOpenCookie,
    max_segment_size: Option<u16>,
}

#[derive(Debug, Clone)]
struct TcpFastOpenSecret {
    listener_id: TcpLookupId,
    epoch: u32,
    secret: RandomState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpListenerPendingEntry {
    listener_id: TcpLookupId,
    local: SocketAddr,
    remote: SocketAddr,
    client_sequence: u32,
    advertised_window: u16,
    capabilities: TcpCapabilities,
    timestamp: Option<TcpTimestampOption>,
    created_epoch: u32,
}

const TCP_FAST_OPEN_COOKIE_LEN: usize = 16;
const TCP_FAST_OPEN_COOKIE_VERSION: u8 = 1;
const TCP_FAST_OPEN_COOKIE_ROTATION_SECS: u64 = 60 * 60;
const TCP_FAST_OPEN_COOKIE_EPOCH_LEN: usize = 4;
const TCP_FAST_OPEN_COOKIE_TAG_OFFSET: usize = 1 + TCP_FAST_OPEN_COOKIE_EPOCH_LEN;
const TCP_FAST_OPEN_COOKIE_TAG_LEN: usize =
    TCP_FAST_OPEN_COOKIE_LEN - TCP_FAST_OPEN_COOKIE_TAG_OFFSET;
const TCP_FAST_OPEN_COOKIE_PREVIOUS_EPOCH_WINDOW: u32 = 1;
const TCP_LISTENER_COOKIE_ROTATION_SECS: u64 = 60 * 60;
const TCP_LISTENER_COOKIE_PREVIOUS_EPOCH_WINDOW: u32 = 1;
const TCP_LISTENER_PENDING_ROTATION_SECS: u64 = 15;
const TCP_LISTENER_PENDING_PREVIOUS_EPOCH_WINDOW: u32 = 1;
const TCP_LISTENER_BACKLOG_LIMIT: usize = 128;
const TCP_LISTENER_PENDING_BUCKET_COUNT: usize =
    TCP_LISTENER_PENDING_PREVIOUS_EPOCH_WINDOW as usize + 2;
const TCP_LISTENER_PENDING_CAPACITY: usize = 1024;

#[derive(Debug)]
struct TcpListenerPendingTable {
    entries: Pool<TcpListenerPendingEntry>,
    tuple_index: FlatHashTable<TransportConnectionKey, PoolIndex>,
    listener_counts: FlatHashTable<TcpLookupId, usize>,
    epoch_buckets: [Vec<PoolIndex>; TCP_LISTENER_PENDING_BUCKET_COUNT],
    bucket_epochs: [u32; TCP_LISTENER_PENDING_BUCKET_COUNT],
    pruned_epoch: Option<u32>,
}

impl Default for TcpListenerPendingTable {
    #[inline]
    fn default() -> Self {
        Self {
            entries: Pool::with_capacity(TCP_LISTENER_PENDING_CAPACITY),
            tuple_index: FlatHashTable::with_capacity(TCP_LISTENER_PENDING_CAPACITY),
            listener_counts: FlatHashTable::new(),
            epoch_buckets: std::array::from_fn(|_| Vec::new()),
            bucket_epochs: [0; TCP_LISTENER_PENDING_BUCKET_COUNT],
            pruned_epoch: None,
        }
    }
}

impl TcpListenerPendingTable {
    #[inline]
    fn begin(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        client_sequence: u32,
        advertised_window: u16,
        capabilities: TcpCapabilities,
        timestamp: Option<TcpTimestampOption>,
        backlog: usize,
        current_epoch: u32,
    ) -> bool {
        self.prune(current_epoch);
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return false;
        };
        if let Some(index) = self.tuple_index.lookup(&key) {
            let Some(existing) = self.entries.get_mut(index) else {
                return false;
            };
            if existing.listener_id != listener_id {
                return false;
            }
            existing.client_sequence = client_sequence;
            existing.advertised_window = advertised_window;
            existing.capabilities = capabilities;
            existing.timestamp = timestamp;
            if existing.created_epoch != current_epoch {
                existing.created_epoch = current_epoch;
                self.push_epoch_bucket(current_epoch, index);
            }
            return true;
        }
        let limit = backlog.max(1).min(TCP_LISTENER_BACKLOG_LIMIT);
        let used = self.listener_counts.lookup(&listener_id).unwrap_or(0);
        if used >= limit {
            return false;
        }
        let Some(index) = self.entries.insert(TcpListenerPendingEntry {
            listener_id,
            local,
            remote,
            client_sequence,
            advertised_window,
            capabilities,
            timestamp,
            created_epoch: current_epoch,
        }) else {
            return false;
        };
        self.tuple_index.insert(key, index);
        self.listener_counts.insert(listener_id, used + 1);
        self.push_epoch_bucket(current_epoch, index);
        true
    }

    #[inline]
    fn get(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        current_epoch: u32,
    ) -> Option<(u32, u16, TcpCapabilities, Option<TcpTimestampOption>)> {
        self.prune(current_epoch);
        let key = TransportConnectionKey::from_socket_addrs(0, local, remote)?;
        let index = self.tuple_index.lookup(&key)?;
        let entry = self.entries.get(index)?;
        if entry.listener_id != listener_id {
            return None;
        }
        Some((
            entry.client_sequence,
            entry.advertised_window,
            entry.capabilities,
            entry.timestamp,
        ))
    }

    #[inline]
    fn contains(&mut self, local: SocketAddr, remote: SocketAddr, current_epoch: u32) -> bool {
        self.prune(current_epoch);
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return false;
        };
        self.tuple_index.lookup(&key).is_some()
    }

    #[inline]
    fn finish(&mut self, listener_id: TcpLookupId, local: SocketAddr, remote: SocketAddr) {
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return;
        };
        let Some(index) = self.tuple_index.lookup(&key) else {
            return;
        };
        let Some(entry) = self.entries.get(index) else {
            return;
        };
        if entry.listener_id != listener_id {
            return;
        }
        self.remove_index(index, key, listener_id);
    }

    #[inline]
    fn prune(&mut self, current_epoch: u32) {
        if self.pruned_epoch == Some(current_epoch) {
            return;
        }
        for bucket in 0..TCP_LISTENER_PENDING_BUCKET_COUNT {
            if self.epoch_buckets[bucket].is_empty() {
                continue;
            }
            let bucket_epoch = self.bucket_epochs[bucket];
            if current_epoch >= bucket_epoch
                && current_epoch - bucket_epoch <= TCP_LISTENER_PENDING_PREVIOUS_EPOCH_WINDOW
            {
                continue;
            }
            while let Some(index) = self.epoch_buckets[bucket].pop() {
                let Some(entry) = self.entries.get(index).copied() else {
                    continue;
                };
                if entry.created_epoch != bucket_epoch {
                    continue;
                }
                let Some(key) =
                    TransportConnectionKey::from_socket_addrs(0, entry.local, entry.remote)
                else {
                    continue;
                };
                if self.tuple_index.lookup(&key) != Some(index) {
                    continue;
                }
                self.remove_index(index, key, entry.listener_id);
            }
        }
        self.pruned_epoch = Some(current_epoch);
    }

    #[cfg(test)]
    #[inline]
    fn entry_count(&self, listener_id: TcpLookupId) -> usize {
        self.listener_counts.lookup(&listener_id).unwrap_or(0)
    }

    #[cfg(test)]
    #[inline]
    fn has_entry(&self, listener_id: TcpLookupId, local: SocketAddr, remote: SocketAddr) -> bool {
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return false;
        };
        let Some(index) = self.tuple_index.lookup(&key) else {
            return false;
        };
        let Some(entry) = self.entries.get(index) else {
            return false;
        };
        entry.listener_id == listener_id
    }

    #[cfg(test)]
    #[inline]
    fn update_created_epoch(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        epoch: u32,
    ) -> bool {
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return false;
        };
        let Some(index) = self.tuple_index.lookup(&key) else {
            return false;
        };
        let Some(entry) = self.entries.get_mut(index) else {
            return false;
        };
        if entry.listener_id != listener_id {
            return false;
        }
        entry.created_epoch = epoch;
        self.push_epoch_bucket(epoch, index);
        true
    }

    #[inline]
    fn push_epoch_bucket(&mut self, epoch: u32, index: PoolIndex) {
        let bucket = epoch as usize % TCP_LISTENER_PENDING_BUCKET_COUNT;
        if self.bucket_epochs[bucket] != epoch {
            self.prune_bucket(bucket);
            self.bucket_epochs[bucket] = epoch;
        }
        self.epoch_buckets[bucket].push(index);
        self.pruned_epoch = None;
    }

    #[inline]
    fn remove_index(
        &mut self,
        index: PoolIndex,
        key: TransportConnectionKey,
        listener_id: TcpLookupId,
    ) {
        self.tuple_index.remove(&key);
        let _ = self.entries.remove(index);
        if let Some(count) = self.listener_counts.lookup(&listener_id) {
            if count <= 1 {
                self.listener_counts.remove(&listener_id);
            } else {
                self.listener_counts.insert(listener_id, count - 1);
            }
        }
        self.pruned_epoch = None;
    }

    #[inline]
    fn prune_bucket(&mut self, bucket: usize) {
        let bucket_epoch = self.bucket_epochs[bucket];
        while let Some(index) = self.epoch_buckets[bucket].pop() {
            let Some(entry) = self.entries.get(index).copied() else {
                continue;
            };
            if entry.created_epoch != bucket_epoch {
                continue;
            }
            let Some(key) = TransportConnectionKey::from_socket_addrs(0, entry.local, entry.remote)
            else {
                continue;
            };
            if self.tuple_index.lookup(&key) != Some(index) {
                continue;
            }
            self.remove_index(index, key, entry.listener_id);
        }
    }
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
    fn empty() -> Self {
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
    fn upsert(
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

    #[cfg(test)]
    #[inline]
    fn lookup_by_connection_id(&self, connection_id: TcpConnectionId) -> Option<SessionId> {
        let entry_index = self.connection_slots.lookup(&connection_id.get())?;
        Some(
            self.entries
                .get(entry_index)
                .expect("tcp connection route entry index is valid")
                .session_id,
        )
    }

    #[inline]
    fn prefetch_tuple(&self, local: SocketAddr, remote: SocketAddr) {
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return;
        };
        self.tuple_slots.prefetch_key(&key);
    }

    #[inline]
    fn lookup_by_tuple(
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

    fn forget_session(&mut self, session_id: SessionId) {
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
    fn empty() -> Self {
        Self {
            entries: Pool::with_capacity(1024),
            session_slots: FlatHashTable::with_capacity(1024),
            tuple_slots: FlatHashTable::with_capacity(1024),
        }
    }

    #[inline]
    fn upsert(
        &mut self,
        session_id: SessionId,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
        capabilities: TcpCapabilities,
    ) {
        let entry = TcpPendingRouteEntry::new(session_id, local, remote, owner, next, capabilities);
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

    fn lookup_by_tuple(
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
    fn capabilities_by_session(&self, session_id: SessionId) -> Option<TcpCapabilities> {
        let entry_index = self.session_slots.lookup(&session_id.get())?;
        Some(
            self.entries
                .get(entry_index)
                .expect("tcp pending route entry index is valid")
                .capabilities,
        )
    }

    #[inline]
    fn prefetch_tuple(&self, local: SocketAddr, remote: SocketAddr) {
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return;
        };
        self.tuple_slots.prefetch_key(&key);
    }

    fn forget_session(&mut self, session_id: SessionId) {
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
                entry.capabilities,
            );
        }
        cloned
    }
}

#[derive(Debug)]
pub struct TcpWorkerOwnedStateCacheline0 {
    #[cfg(test)]
    owner_worker: DataWorkerId,
    #[cfg(test)]
    listeners: TcpLookupSnapshot,
    connections: TcpConnectionRouteIndex,
    pending: TcpPendingRouteIndex,
    #[cfg(test)]
    next_iss: u32,
    /// Per-worker session-queue runtime data for the TCP input node's
    /// `TcpQueue<C>`. Owned by the worker thread (this TLS); never shared
    /// across workers, so no synchronization is needed.
    queue_runtime_data: Option<NodeRuntimeData>,
}

#[derive(Debug)]
struct TcpWorkerOwnedStateCacheline1 {
    fast_open_cache: Vec<TcpFastOpenCacheEntry>,
    fast_open_cache_index: FlatHashTable<TransportConnectionKey, usize>,
    fast_open_secrets: Vec<TcpFastOpenSecret>,
    listener_pending: TcpListenerPendingTable,
    listener_cookie_secrets: Vec<TcpFastOpenSecret>,
}

#[derive(Debug)]
pub struct TcpWorkerOwnedState {
    cacheline0: CachePadded<TcpWorkerOwnedStateCacheline0>,
    cacheline1: CachePadded<TcpWorkerOwnedStateCacheline1>,
}

impl Deref for TcpWorkerOwnedState {
    type Target = TcpWorkerOwnedStateCacheline0;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cacheline0
    }
}

impl DerefMut for TcpWorkerOwnedState {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cacheline0
    }
}

impl TcpWorkerOwnedState {
    #[inline]
    pub(crate) fn new(owner_worker: DataWorkerId) -> Self {
        #[cfg(not(test))]
        let _ = owner_worker;
        Self {
            cacheline0: CachePadded::new(TcpWorkerOwnedStateCacheline0 {
                #[cfg(test)]
                owner_worker,
                #[cfg(test)]
                listeners: TcpLookupSnapshot::empty(),
                connections: TcpConnectionRouteIndex::empty(),
                pending: TcpPendingRouteIndex::empty(),
                #[cfg(test)]
                next_iss: 81_000,
                queue_runtime_data: None,
            }),
            cacheline1: CachePadded::new(TcpWorkerOwnedStateCacheline1 {
                fast_open_cache: Vec::new(),
                fast_open_cache_index: FlatHashTable::new(),
                fast_open_secrets: Vec::new(),
                listener_pending: TcpListenerPendingTable::default(),
                listener_cookie_secrets: Vec::new(),
            }),
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn owner_worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    /// Per-worker TCP session-queue runtime data, set once by
    /// `TcpMain::register_tcp_input` when it first builds the worker's
    /// `TcpQueue<C>`. Read back on subsequent registrations of the same
    /// worker's TCP input node. Worker-local, no locking.
    #[inline]
    pub(crate) fn queue_runtime_data(&self) -> Option<NodeRuntimeData> {
        self.queue_runtime_data
    }

    #[inline]
    pub(crate) fn set_queue_runtime_data(&mut self, data: NodeRuntimeData) {
        self.queue_runtime_data = Some(data);
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn insert_listener<A: TcpListenerAddress>(
        &mut self,
        key: A::Key,
        id: TcpLookupId,
        capabilities: TcpCapabilities,
    ) where
        TcpLookupSnapshot: TcpListenerLookupAccess<A>,
    {
        let value = self.value(id, capabilities);
        self.listeners.insert_listener::<A>(key, value);
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn publish_snapshot(&self) -> TcpLookupSnapshot {
        self.listeners.clone()
    }

    #[inline]
    pub(crate) fn remember_session(
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
    pub(crate) fn remember_pending_open(
        &mut self,
        session_id: SessionId,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
        capabilities: TcpCapabilities,
    ) {
        self.pending
            .upsert(session_id, local, remote, owner, next, capabilities);
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn session_route_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.connections.lookup_by_tuple(local, remote)
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn pending_route_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.pending.lookup_by_tuple(local, remote)
    }

    #[inline]
    pub(crate) fn input_route(
        &mut self,
        local: SocketAddr,
        remote: SocketAddr,
        check_listener_pending: bool,
    ) -> (Option<(SessionId, DataWorkerId, TcpInputNext)>, bool) {
        let route = self
            .connections
            .lookup_by_tuple(local, remote)
            .or_else(|| self.pending.lookup_by_tuple(local, remote));
        if route.is_some() || !check_listener_pending {
            return (route, false);
        }
        (
            None,
            self.cacheline1
                .listener_pending
                .contains(local, remote, listener_pending_epoch()),
        )
    }

    #[inline]
    pub(crate) fn prefetch_tuple(&self, local: SocketAddr, remote: SocketAddr) {
        self.connections.prefetch_tuple(local, remote);
        self.pending.prefetch_tuple(local, remote);
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn session_id_by_connection_id(
        &self,
        connection_id: TcpConnectionId,
    ) -> Option<SessionId> {
        self.connections.lookup_by_connection_id(connection_id)
    }

    #[inline]
    pub(crate) fn forget_session(&mut self, session_id: SessionId) {
        self.connections.forget_session(session_id);
    }

    #[inline]
    pub(crate) fn forget_pending_open(&mut self, session_id: SessionId) {
        self.pending.forget_session(session_id);
    }

    #[inline]
    pub(crate) fn pending_open_capabilities(
        &self,
        session_id: SessionId,
    ) -> Option<TcpCapabilities> {
        self.pending.capabilities_by_session(session_id)
    }

    #[inline]
    pub(crate) fn publish_connection<C>(
        &mut self,
        session_id: SessionId,
        connection: &TcpConnection<C>,
    ) -> bool
    where
        C: CongestionController + 'static,
    {
        let pending_capabilities = self.pending.capabilities_by_session(session_id);
        self.forget_session(session_id);
        self.forget_pending_open(session_id);
        match connection.state() {
            TcpState::Closed => true,
            TcpState::SynSent => {
                self.remember_pending_open(
                    session_id,
                    connection.local(),
                    connection.remote(),
                    connection.owner_worker(),
                    connection.next_node(),
                    pending_capabilities.unwrap_or_default(),
                );
                false
            }
            _ => {
                self.remember_session(
                    session_id,
                    connection.connection_id(),
                    connection.local(),
                    connection.remote(),
                    connection.owner_worker(),
                    connection.next_node(),
                );
                false
            }
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn next_initial_sequence(&mut self, local: SocketAddr, remote: SocketAddr) -> u32 {
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

    #[cfg(test)]
    pub(crate) fn fast_open_cookie(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(TcpFastOpenCookie, Option<u16>)> {
        let key = TransportConnectionKey::from_socket_addrs(0, local, remote)?;
        let index = self.cacheline1.fast_open_cache_index.lookup(&key)?;
        let entry = self.cacheline1.fast_open_cache.get(index)?;
        Some((entry.cookie, entry.max_segment_size))
    }

    pub(crate) fn remember_fast_open_cookie(
        &mut self,
        local: SocketAddr,
        remote: SocketAddr,
        cookie: TcpFastOpenCookie,
        max_segment_size: Option<u16>,
    ) {
        let Some(key) = TransportConnectionKey::from_socket_addrs(0, local, remote) else {
            return;
        };
        if let Some(index) = self.cacheline1.fast_open_cache_index.lookup(&key) {
            if let Some(entry) = self.cacheline1.fast_open_cache.get_mut(index) {
                entry.cookie = cookie;
                entry.max_segment_size = max_segment_size;
            }
            return;
        }
        self.cacheline1.fast_open_cache.push(TcpFastOpenCacheEntry {
            local,
            remote,
            cookie,
            max_segment_size,
        });
        let index = self.cacheline1.fast_open_cache.len() - 1;
        self.cacheline1.fast_open_cache_index.insert(key, index);
    }

    pub(crate) fn fast_open_cookie_for_listener(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> TcpFastOpenCookie {
        let epoch = fast_open_cookie_epoch();
        self.fast_open_cookie_for_listener_in_epoch(listener_id, local, remote, epoch)
    }

    pub(crate) fn validate_fast_open_cookie(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        cookie: &[u8],
    ) -> bool {
        let epoch = fast_open_cookie_epoch();
        self.validate_fast_open_cookie_in_epoch(listener_id, local, remote, cookie, epoch)
    }

    pub(crate) fn listener_cookie_for_syn(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        client_sequence: u32,
    ) -> u32 {
        let epoch = listener_cookie_epoch();
        self.listener_cookie_for_syn_in_epoch(listener_id, local, remote, client_sequence, epoch)
    }

    pub(crate) fn validate_listener_cookie(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        client_sequence: u32,
        cookie: u32,
    ) -> bool {
        let epoch = listener_cookie_epoch();
        self.validate_listener_cookie_in_epoch(
            listener_id,
            local,
            remote,
            client_sequence,
            cookie,
            epoch,
        )
    }

    pub(crate) fn begin_listener_pending(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        client_sequence: u32,
        advertised_window: u16,
        capabilities: TcpCapabilities,
        timestamp: Option<TcpTimestampOption>,
        backlog: usize,
    ) -> bool {
        self.cacheline1.listener_pending.begin(
            listener_id,
            local,
            remote,
            client_sequence,
            advertised_window,
            capabilities,
            timestamp,
            backlog,
            listener_pending_epoch(),
        )
    }

    pub(crate) fn listener_pending(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(u32, u16, TcpCapabilities, Option<TcpTimestampOption>)> {
        self.cacheline1
            .listener_pending
            .get(listener_id, local, remote, listener_pending_epoch())
    }

    #[cfg(test)]
    pub(crate) fn has_listener_pending(&mut self, local: SocketAddr, remote: SocketAddr) -> bool {
        self.cacheline1
            .listener_pending
            .contains(local, remote, listener_pending_epoch())
    }

    pub(crate) fn finish_listener_pending(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
    ) {
        self.cacheline1
            .listener_pending
            .finish(listener_id, local, remote);
    }

    #[cfg(test)]
    pub(crate) fn listener_pending_contains(
        &self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> bool {
        self.cacheline1
            .listener_pending
            .has_entry(listener_id, local, remote)
    }

    #[cfg(test)]
    pub(crate) fn listener_pending_len(&self, listener_id: TcpLookupId) -> usize {
        self.cacheline1.listener_pending.entry_count(listener_id)
    }

    #[cfg(test)]
    fn set_listener_pending_epoch(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        epoch: u32,
    ) -> bool {
        self.cacheline1
            .listener_pending
            .update_created_epoch(listener_id, local, remote, epoch)
    }

    fn fast_open_cookie_for_listener_in_epoch(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        epoch: u32,
    ) -> TcpFastOpenCookie {
        let secret = self.fast_open_secret(listener_id, epoch);
        build_fast_open_cookie(&secret, listener_id, local, remote, epoch)
    }

    fn validate_fast_open_cookie_in_epoch(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        cookie: &[u8],
        epoch: u32,
    ) -> bool {
        let Ok(cookie): Result<TcpFastOpenCookie, _> = cookie.try_into() else {
            return false;
        };
        let Some(cookie_epoch) = cookie.epoch() else {
            return false;
        };
        if epoch < cookie_epoch || epoch - cookie_epoch > TCP_FAST_OPEN_COOKIE_PREVIOUS_EPOCH_WINDOW
        {
            return false;
        }
        let secret = self.fast_open_secret(listener_id, cookie_epoch);
        let expected = build_fast_open_cookie(&secret, listener_id, local, remote, cookie_epoch);
        cookie.constant_time_eq(&expected)
    }

    fn listener_cookie_for_syn_in_epoch(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        client_sequence: u32,
        epoch: u32,
    ) -> u32 {
        listener_cookie_word(
            self.listener_cookie_secret(listener_id, epoch),
            listener_id,
            local,
            remote,
            client_sequence,
            epoch,
        )
    }

    fn validate_listener_cookie_in_epoch(
        &mut self,
        listener_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        client_sequence: u32,
        cookie: u32,
        epoch: u32,
    ) -> bool {
        if self.listener_cookie_for_syn_in_epoch(listener_id, local, remote, client_sequence, epoch)
            == cookie
        {
            return true;
        }
        if epoch == 0 {
            return false;
        }
        if TCP_LISTENER_COOKIE_PREVIOUS_EPOCH_WINDOW == 0 {
            return false;
        }
        let previous_epoch = epoch - 1;
        self.listener_cookie_for_syn_in_epoch(
            listener_id,
            local,
            remote,
            client_sequence,
            previous_epoch,
        ) == cookie
    }

    fn fast_open_secret(&mut self, listener_id: TcpLookupId, epoch: u32) -> &RandomState {
        prune_cookie_secrets(
            &mut self.cacheline1.fast_open_secrets,
            listener_id,
            epoch,
            TCP_FAST_OPEN_COOKIE_PREVIOUS_EPOCH_WINDOW,
        );
        if let Some(index) = self
            .cacheline1
            .fast_open_secrets
            .iter()
            .position(|secret| secret.listener_id == listener_id && secret.epoch == epoch)
        {
            return &self.cacheline1.fast_open_secrets[index].secret;
        }
        self.cacheline1.fast_open_secrets.push(TcpFastOpenSecret {
            listener_id,
            epoch,
            secret: RandomState::new(),
        });
        &self.cacheline1.fast_open_secrets[self.cacheline1.fast_open_secrets.len() - 1].secret
    }

    fn listener_cookie_secret(&mut self, listener_id: TcpLookupId, epoch: u32) -> &RandomState {
        prune_cookie_secrets(
            &mut self.cacheline1.listener_cookie_secrets,
            listener_id,
            epoch,
            TCP_LISTENER_COOKIE_PREVIOUS_EPOCH_WINDOW,
        );
        if let Some(index) = self
            .cacheline1
            .listener_cookie_secrets
            .iter()
            .position(|secret| secret.listener_id == listener_id && secret.epoch == epoch)
        {
            return &self.cacheline1.listener_cookie_secrets[index].secret;
        }
        self.cacheline1
            .listener_cookie_secrets
            .push(TcpFastOpenSecret {
                listener_id,
                epoch,
                secret: RandomState::new(),
            });
        &self.cacheline1.listener_cookie_secrets[self.cacheline1.listener_cookie_secrets.len() - 1]
            .secret
    }

    #[cfg(test)]
    #[inline]
    fn value(&self, id: TcpLookupId, capabilities: TcpCapabilities) -> TcpLookupValue {
        TcpLookupValue {
            id,
            owner_worker: self.owner_worker,
            capabilities,
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

fn fast_open_cookie_epoch() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (now / TCP_FAST_OPEN_COOKIE_ROTATION_SECS) as u32
}

fn listener_cookie_epoch() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (now / TCP_LISTENER_COOKIE_ROTATION_SECS) as u32
}

fn listener_pending_epoch() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (now / TCP_LISTENER_PENDING_ROTATION_SECS) as u32
}

fn prune_cookie_secrets(
    secrets: &mut Vec<TcpFastOpenSecret>,
    listener_id: TcpLookupId,
    current_epoch: u32,
    previous_epoch_window: u32,
) {
    let mut index = 0usize;
    while index < secrets.len() {
        let secret = &secrets[index];
        let expired = secret.listener_id == listener_id
            && (current_epoch < secret.epoch
                || current_epoch - secret.epoch > previous_epoch_window);
        if expired {
            let Some(last) = secrets.pop() else {
                break;
            };
            if index < secrets.len() {
                secrets[index] = last;
                continue;
            }
            continue;
        }
        index += 1;
    }
}

fn build_fast_open_cookie(
    secret: &RandomState,
    listener_id: TcpLookupId,
    local: SocketAddr,
    remote: SocketAddr,
    epoch: u32,
) -> TcpFastOpenCookie {
    let mut bytes = [0u8; TCP_FAST_OPEN_COOKIE_LEN];
    bytes[0] = TCP_FAST_OPEN_COOKIE_VERSION;
    write_be_u32(&mut bytes, 1, epoch);
    let tag = fast_open_cookie_tag(secret, listener_id, local, remote, epoch);
    write_bytes(
        &mut bytes,
        TCP_FAST_OPEN_COOKIE_TAG_OFFSET,
        &tag[..TCP_FAST_OPEN_COOKIE_TAG_LEN],
    );
    bytes.into()
}

fn fast_open_cookie_tag(
    secret: &RandomState,
    listener_id: TcpLookupId,
    local: SocketAddr,
    remote: SocketAddr,
    epoch: u32,
) -> [u8; 16] {
    let first = fast_open_cookie_tag_word(secret, listener_id, local, remote, epoch, 0);
    let second = fast_open_cookie_tag_word(secret, listener_id, local, remote, epoch, 1);
    let mut tag = [0u8; 16];
    write_be_u64(&mut tag, 0, first);
    write_be_u64(&mut tag, 8, second);
    tag
}

#[inline(always)]
fn write_bytes(output: &mut [u8], offset: usize, bytes: &[u8]) {
    let mut index = 0usize;
    while index < bytes.len() {
        output[offset + index] = bytes[index];
        index += 1;
    }
}

#[inline(always)]
fn write_be_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset] = (value >> 24) as u8;
    output[offset + 1] = (value >> 16) as u8;
    output[offset + 2] = (value >> 8) as u8;
    output[offset + 3] = value as u8;
}

#[inline(always)]
fn write_be_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset] = (value >> 56) as u8;
    output[offset + 1] = (value >> 48) as u8;
    output[offset + 2] = (value >> 40) as u8;
    output[offset + 3] = (value >> 32) as u8;
    output[offset + 4] = (value >> 24) as u8;
    output[offset + 5] = (value >> 16) as u8;
    output[offset + 6] = (value >> 8) as u8;
    output[offset + 7] = value as u8;
}

fn fast_open_cookie_tag_word(
    secret: &RandomState,
    listener_id: TcpLookupId,
    local: SocketAddr,
    remote: SocketAddr,
    epoch: u32,
    discriminator: u64,
) -> u64 {
    let mut hasher = secret.build_hasher();
    discriminator.hash(&mut hasher);
    listener_id.hash(&mut hasher);
    epoch.hash(&mut hasher);
    hash_socket_addr(&mut hasher, local);
    hash_socket_addr(&mut hasher, remote);
    hasher.finish()
}

fn listener_cookie_word(
    secret: &RandomState,
    listener_id: TcpLookupId,
    local: SocketAddr,
    remote: SocketAddr,
    client_sequence: u32,
    epoch: u32,
) -> u32 {
    let mut hasher = secret.build_hasher();
    listener_id.hash(&mut hasher);
    epoch.hash(&mut hasher);
    client_sequence.hash(&mut hasher);
    hash_socket_addr(&mut hasher, local);
    hash_socket_addr(&mut hasher, remote);
    let raw = hasher.finish() as u32;
    raw.max(1)
}

fn hash_socket_addr(hasher: &mut impl Hasher, addr: SocketAddr) {
    match addr {
        SocketAddr::V4(addr) => {
            4u8.hash(hasher);
            addr.ip().octets().hash(hasher);
            addr.port().hash(hasher);
        }
        SocketAddr::V6(addr) => {
            6u8.hash(hasher);
            addr.ip().octets().hash(hasher);
            addr.port().hash(hasher);
            addr.flowinfo().hash(hasher);
            addr.scope_id().hash(hasher);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TcpWorkerOwnedState, write_bytes};
    use hammer_adapter::DataWorkerId;
    use hammer_core::protocol::tcp::{TcpCapabilities, TcpFastOpenCookie};
    use std::net::{Ipv4Addr, SocketAddr};

    fn worker_state() -> TcpWorkerOwnedState {
        TcpWorkerOwnedState::new(DataWorkerId::new(0))
    }

    fn socket_pair() -> (SocketAddr, SocketAddr) {
        (
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 443)),
            SocketAddr::from((Ipv4Addr::new(198, 51, 100, 20), 50_000)),
        )
    }

    #[test]
    fn fast_open_cookie_validation_accepts_cookie_for_current_epoch() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();

        let cookie = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 42);

        assert!(state.validate_fast_open_cookie_in_epoch(7, local, remote, &cookie, 42));
    }

    #[test]
    fn fast_open_cookie_validation_rejects_modified_cookie() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();
        let cookie = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 42);
        let mut bytes = [0u8; TcpFastOpenCookie::MAX_LEN];
        write_bytes(&mut bytes, 0, cookie.as_slice());
        bytes[15] ^= 0x5a;
        let cookie: TcpFastOpenCookie = bytes.into();

        assert!(!state.validate_fast_open_cookie_in_epoch(7, local, remote, &cookie, 42));
    }

    #[test]
    fn fast_open_cookie_validation_rejects_cookie_for_different_tuple() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();
        let cookie = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 42);
        let other_remote = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 21), 50_000));

        assert!(!state.validate_fast_open_cookie_in_epoch(7, local, other_remote, &cookie, 42));
    }

    #[test]
    fn fast_open_cookie_validation_rejects_cookie_after_rotation_window() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();

        let cookie = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 42);

        assert!(state.validate_fast_open_cookie_in_epoch(7, local, remote, &cookie, 43));
        assert!(!state.validate_fast_open_cookie_in_epoch(7, local, remote, &cookie, 44));
    }

    #[test]
    fn fast_open_cookie_secret_rotates_with_epoch() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();

        let cookie_a = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 42);
        let cookie_b = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 43);

        assert_ne!(cookie_a, cookie_b);
        assert_eq!(state.cacheline1.fast_open_secrets.len(), 2);

        let _ = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 44);

        assert_eq!(state.cacheline1.fast_open_secrets.len(), 2);
        assert!(
            state
                .cacheline1
                .fast_open_secrets
                .iter()
                .all(|secret| secret.epoch >= 43)
        );
    }

    #[test]
    fn fast_open_cookie_cache_updates_and_uses_tuple_index() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();
        let cookie_a = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 42);
        let cookie_b = state.fast_open_cookie_for_listener_in_epoch(7, local, remote, 43);

        state.remember_fast_open_cookie(local, remote, cookie_a, Some(1_440));
        assert_eq!(state.cacheline1.fast_open_cache.len(), 1);
        assert_eq!(state.cacheline1.fast_open_cache_index.len(), 1);
        assert_eq!(
            state.fast_open_cookie(local, remote),
            Some((cookie_a, Some(1_440)))
        );

        state.remember_fast_open_cookie(local, remote, cookie_b, Some(1_460));
        assert_eq!(state.cacheline1.fast_open_cache.len(), 1);
        assert_eq!(state.cacheline1.fast_open_cache_index.len(), 1);
        assert_eq!(
            state.fast_open_cookie(local, remote),
            Some((cookie_b, Some(1_460)))
        );
    }

    #[test]
    fn listener_cookie_validation_accepts_current_epoch_cookie() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();

        let cookie = state.listener_cookie_for_syn_in_epoch(7, local, remote, 100, 42);

        assert!(state.validate_listener_cookie_in_epoch(7, local, remote, 100, cookie, 42));
    }

    #[test]
    fn listener_cookie_validation_rejects_wrong_client_sequence() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();

        let cookie = state.listener_cookie_for_syn_in_epoch(7, local, remote, 100, 42);

        assert!(!state.validate_listener_cookie_in_epoch(7, local, remote, 101, cookie, 42));
    }

    #[test]
    fn listener_cookie_validation_rejects_wrong_tuple() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();
        let cookie = state.listener_cookie_for_syn_in_epoch(7, local, remote, 100, 42);
        let other_remote = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 21), 50_000));

        assert!(!state.validate_listener_cookie_in_epoch(7, local, other_remote, 100, cookie, 42));
    }

    #[test]
    fn listener_cookie_secret_rotates_with_epoch() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();

        let cookie_a = state.listener_cookie_for_syn_in_epoch(7, local, remote, 100, 42);
        let cookie_b = state.listener_cookie_for_syn_in_epoch(7, local, remote, 100, 43);

        assert_ne!(cookie_a, cookie_b);
        assert_eq!(state.cacheline1.listener_cookie_secrets.len(), 2);

        let _ = state.listener_cookie_for_syn_in_epoch(7, local, remote, 100, 44);

        assert_eq!(state.cacheline1.listener_cookie_secrets.len(), 2);
        assert!(
            state
                .cacheline1
                .listener_cookie_secrets
                .iter()
                .all(|secret| secret.epoch >= 43)
        );
    }

    #[test]
    fn listener_pending_backlog_tracks_tuple_lifecycle() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();

        assert!(state.begin_listener_pending(
            7,
            local,
            remote,
            100,
            4_096,
            TcpCapabilities::default(),
            None,
            10
        ));
        assert_eq!(state.listener_pending_len(7), 1);
        assert!(state.listener_pending_contains(7, local, remote));
        assert_eq!(
            state.listener_pending(7, local, remote),
            Some((100, 4_096, TcpCapabilities::default(), None))
        );

        state.finish_listener_pending(7, local, remote);

        assert_eq!(state.listener_pending_len(7), 0);
        assert!(!state.listener_pending_contains(7, local, remote));
    }

    #[test]
    fn listener_pending_backlog_rejects_new_tuple_when_full() {
        let mut state = worker_state();
        let local = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 443));
        let remote_a = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 20), 50_000));
        let remote_b = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 21), 50_001));

        assert!(state.begin_listener_pending(
            7,
            local,
            remote_a,
            100,
            4_096,
            TcpCapabilities::default(),
            None,
            1
        ));
        assert!(!state.begin_listener_pending(
            7,
            local,
            remote_b,
            101,
            4_096,
            TcpCapabilities::default(),
            None,
            1
        ));
        assert_eq!(state.listener_pending_len(7), 1);
    }

    #[test]
    fn listener_pending_refreshes_existing_tuple_without_consuming_backlog_slot() {
        let mut state = worker_state();
        let (local, remote) = socket_pair();

        assert!(state.begin_listener_pending(
            7,
            local,
            remote,
            100,
            4_096,
            TcpCapabilities::default(),
            None,
            1
        ));
        assert!(state.begin_listener_pending(
            7,
            local,
            remote,
            101,
            8_192,
            TcpCapabilities::default(),
            None,
            1
        ));

        assert_eq!(state.listener_pending_len(7), 1);
        assert_eq!(
            state.listener_pending(7, local, remote),
            Some((101, 8_192, TcpCapabilities::default(), None))
        );
    }

    #[test]
    fn listener_pending_prunes_expired_entries_before_backlog_check() {
        let mut state = worker_state();
        let local = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 443));
        let remote_a = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 20), 50_000));
        let remote_b = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 21), 50_001));

        assert!(state.begin_listener_pending(
            7,
            local,
            remote_a,
            100,
            4_096,
            TcpCapabilities::default(),
            None,
            1
        ));

        let expired_epoch = 10;
        assert!(state.set_listener_pending_epoch(7, local, remote_a, expired_epoch));

        assert!(state.begin_listener_pending(
            7,
            local,
            remote_b,
            101,
            4_096,
            TcpCapabilities::default(),
            None,
            1
        ));
        assert_eq!(state.listener_pending_len(7), 1);
        assert!(state.listener_pending_contains(7, local, remote_b));
        assert!(!state.listener_pending_contains(7, local, remote_a));
    }

    #[test]
    fn listener_pending_index_stays_consistent_after_remove_and_prune() {
        let mut state = worker_state();
        let local = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 443));
        let remote_a = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 20), 50_000));
        let remote_b = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 21), 50_001));

        assert!(state.begin_listener_pending(
            7,
            local,
            remote_a,
            100,
            4_096,
            TcpCapabilities::default(),
            None,
            2
        ));
        assert!(state.begin_listener_pending(
            7,
            local,
            remote_b,
            101,
            4_096,
            TcpCapabilities::default(),
            None,
            2
        ));

        state.finish_listener_pending(7, local, remote_a);
        assert_eq!(state.listener_pending_len(7), 1);
        assert_eq!(
            state.listener_pending(7, local, remote_b),
            Some((101, 4_096, TcpCapabilities::default(), None))
        );

        assert!(state.set_listener_pending_epoch(7, local, remote_b, 10));
        assert!(!state.has_listener_pending(local, remote_b));
        assert_eq!(state.listener_pending_len(7), 0);
    }

    #[test]
    fn listener_pending_remove_updates_moved_tuple_index() {
        let mut state = worker_state();
        let local = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 443));
        let remote_a = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 20), 50_000));
        let remote_b = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 21), 50_001));
        let remote_c = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 22), 50_002));

        assert!(state.begin_listener_pending(
            7,
            local,
            remote_a,
            100,
            4_096,
            TcpCapabilities::default(),
            None,
            3
        ));
        assert!(state.begin_listener_pending(
            7,
            local,
            remote_b,
            101,
            4_096,
            TcpCapabilities::default(),
            None,
            3
        ));
        assert!(state.begin_listener_pending(
            7,
            local,
            remote_c,
            102,
            4_096,
            TcpCapabilities::default(),
            None,
            3
        ));

        state.finish_listener_pending(7, local, remote_b);

        assert_eq!(state.listener_pending_len(7), 2);
        assert_eq!(
            state.listener_pending(7, local, remote_a),
            Some((100, 4_096, TcpCapabilities::default(), None))
        );
        assert_eq!(
            state.listener_pending(7, local, remote_c),
            Some((102, 4_096, TcpCapabilities::default(), None))
        );
        assert!(!state.has_listener_pending(local, remote_b));
    }
}
