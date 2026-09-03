use std::fmt;
use std::net::Ipv6Addr;

use hammer_infra::bihash::{Bihash, BihashKey, splitmix64};
use ipnet::Ipv6Net;

use hammer_infra::map::PrefixLengthSearchOrder;

const IP6_PREFIX_KV_PER_PAGE: usize = 2;

pub type Ip6Fib<V> = Ip6PrefixHashTable<V>;

pub struct Ip6PrefixHashTable<V: Copy> {
    routes: Bihash<Ip6PrefixKey, IP6_PREFIX_KV_PER_PAGE>,
    values: Vec<V>,
    prefix_lengths: PrefixLengthSearchOrder,
}

impl<V: Copy> Ip6PrefixHashTable<V> {
    #[inline]
    pub fn empty() -> Self {
        Self::with_capacity(1)
    }

    #[inline]
    pub fn from_routes(routes: impl IntoIterator<Item = (Ipv6Net, V)>) -> Self {
        let routes = routes.into_iter().collect::<Vec<_>>();
        let mut table = Self::with_capacity(routes.len());
        for (prefix, value) in routes {
            table.insert(prefix, value);
        }
        table
    }

    #[inline]
    pub fn insert(&mut self, prefix: Ipv6Net, value: V) {
        let prefix_len = prefix.prefix_len();
        let key = Ip6PrefixKey::new(prefix.addr(), prefix_len);
        if let Some(index) = self.routes.lookup(&key) {
            let slot = index as usize;
            if let Some(existing) = self.values.get_mut(slot) {
                *existing = value;
                return;
            }
            unreachable!("IPv6 FIB bihash value index must reference owned storage");
        }

        let index = self.values.len() as u64;
        self.values.push(value);
        self.routes.insert(key, index);
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
        let index = self.routes.lookup(&key)? as usize;
        debug_assert!(index < self.values.len());
        // SAFETY: Bihash values are created only from indexes returned by
        // `self.values.len()` immediately before the matching value is pushed.
        // Existing keys update that slot in place, and clone preserves both
        // collections without changing their index relationship.
        Some(unsafe { *self.values.get_unchecked(index) })
    }

    #[inline(always)]
    pub fn prefetch_key(&self, key: Ip6PrefixKey) {
        self.routes.prefetch(&key);
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
        self.routes.nbuckets() as usize
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    fn with_capacity(route_count: usize) -> Self {
        let max_buckets = 1usize << 31;
        let bucket_count = route_count
            .max(1)
            .checked_next_power_of_two()
            .unwrap_or(max_buckets)
            .min(max_buckets) as u32;
        Self {
            routes: Bihash::new(bucket_count),
            values: Vec::with_capacity(route_count),
            prefix_lengths: PrefixLengthSearchOrder::empty(),
        }
    }
}

impl<V: Copy> Clone for Ip6PrefixHashTable<V> {
    fn clone(&self) -> Self {
        let routes = Bihash::new(self.routes.nbuckets());
        for (key, value) in self.routes.iter() {
            routes.insert(key, value);
        }
        Self {
            routes,
            values: self.values.clone(),
            prefix_lengths: self.prefix_lengths.clone(),
        }
    }
}

impl<V: Copy + fmt::Debug> fmt::Debug for Ip6PrefixHashTable<V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Ip6PrefixHashTable")
            .field("routes", &self.routes.len())
            .field("values", &self.values)
            .field("prefix_lengths", &self.prefix_lengths)
            .finish()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Ip6PrefixKey {
    words: [u64; 3],
}

impl Ip6PrefixKey {
    #[inline(always)]
    pub fn new(addr: Ipv6Addr, prefix_len: u8) -> Self {
        assert!(prefix_len <= 128, "invalid IPv6 prefix length");
        let masked = mask_ipv6(addr, prefix_len);
        Self {
            words: [(masked >> 64) as u64, masked as u64, u64::from(prefix_len)],
        }
    }

    #[inline(always)]
    pub const fn masked(self) -> u128 {
        (self.words[0] as u128) << 64 | self.words[1] as u128
    }

    #[inline(always)]
    pub const fn prefix_len(self) -> u8 {
        self.words[2] as u8
    }
}

impl BihashKey for Ip6PrefixKey {
    #[inline(always)]
    fn hash(self) -> u64 {
        splitmix64(self.words[0] ^ self.words[1] ^ self.words[2])
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
