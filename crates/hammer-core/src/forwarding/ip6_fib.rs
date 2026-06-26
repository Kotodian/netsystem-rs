use std::net::Ipv6Addr;

use ipnet::Ipv6Net;

use crate::ds::{FlatHashKey, FlatHashTable, PrefixLengthSearchOrder};

pub type Ip6Fib<V> = Ip6PrefixHashTable<V>;

#[derive(Debug, Clone)]
pub struct Ip6PrefixHashTable<V: Copy> {
    routes: FlatHashTable<Ip6PrefixKey, V>,
    prefix_lengths: PrefixLengthSearchOrder,
}

impl<V: Copy> Ip6PrefixHashTable<V> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            routes: FlatHashTable::empty(),
            prefix_lengths: PrefixLengthSearchOrder::empty(),
        }
    }

    #[inline]
    pub fn from_routes(routes: impl IntoIterator<Item = (Ipv6Net, V)>) -> Self {
        let mut table = Self::empty();
        for (prefix, value) in routes {
            table.insert(prefix, value);
        }
        table
    }

    #[inline]
    pub fn insert(&mut self, prefix: Ipv6Net, value: V) {
        let prefix_len = prefix.prefix_len();
        self.routes
            .insert(Ip6PrefixKey::new(prefix.addr(), prefix_len), value);
        self.prefix_lengths.insert(prefix_len);
    }

    #[inline(always)]
    pub fn lookup(&self, destination: Ipv6Addr) -> Option<V> {
        for prefix_len in self.prefix_lengths.as_slice().iter().copied() {
            if let Some(value) = self.lookup_key(Ip6PrefixKey::new(destination, prefix_len)) {
                return Some(value);
            }
        }
        None
    }

    #[inline(always)]
    pub fn lookup_key(&self, key: Ip6PrefixKey) -> Option<V> {
        self.routes.lookup(&key)
    }

    #[inline(always)]
    pub fn prefetch_key(&self, key: Ip6PrefixKey) {
        self.routes.prefetch_key(&key);
    }

    #[inline(always)]
    pub fn prefetch_destination(&self, destination: Ipv6Addr) {
        for prefix_len in self.prefix_lengths.as_slice().iter().take(2).copied() {
            self.prefetch_key(Ip6PrefixKey::new(destination, prefix_len));
        }
    }

    #[inline(always)]
    pub fn prefix_lengths(&self) -> &[u8] {
        self.prefix_lengths.as_slice()
    }

    #[inline(always)]
    pub fn bucket_count(&self) -> usize {
        self.routes.bucket_count()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Ip6PrefixKey {
    masked: u128,
    prefix_len: u8,
}

impl Ip6PrefixKey {
    #[inline(always)]
    pub fn new(addr: Ipv6Addr, prefix_len: u8) -> Self {
        assert!(prefix_len <= 128, "invalid IPv6 prefix length");
        Self {
            masked: mask_ipv6(addr, prefix_len),
            prefix_len,
        }
    }

    #[inline(always)]
    pub const fn masked(self) -> u128 {
        self.masked
    }

    #[inline(always)]
    pub const fn prefix_len(self) -> u8 {
        self.prefix_len
    }
}

impl FlatHashKey for Ip6PrefixKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        let mixed = self.masked ^ (self.masked >> 64) ^ u128::from(self.prefix_len);
        mixed.hash_key()
    }
}

#[inline(always)]
pub fn mask_ipv6(addr: Ipv6Addr, prefix_len: u8) -> u128 {
    assert!(prefix_len <= 128, "invalid IPv6 prefix length");
    if prefix_len == 0 {
        return 0;
    }
    let value = u128::from(addr);
    value & (!0u128 << (128 - prefix_len))
}
