use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_adapter::DataWorkerId;
use hammer_infra::map::{FlatHashKey, FlatHashTable};

pub type TcpLookupId = u32;

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

#[derive(Debug, Clone)]
pub struct TcpWorkerOwnedState {
    owner_worker: DataWorkerId,
    listeners: TcpLookupSnapshot,
}

impl TcpWorkerOwnedState {
    #[inline]
    pub fn new(owner_worker: DataWorkerId) -> Self {
        Self {
            owner_worker,
            listeners: TcpLookupSnapshot::empty(),
        }
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
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
