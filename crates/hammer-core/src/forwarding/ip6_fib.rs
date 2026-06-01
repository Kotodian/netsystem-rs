use std::net::Ipv6Addr;

use ipnet::Ipv6Net;

use super::prefetch::prefetch_read_l1;

pub type Ip6Fib<V> = Ip6PrefixHashTable<V>;

#[derive(Debug, Clone)]
pub struct Ip6PrefixHashTable<V: Copy> {
    buckets: Box<[Ip6PrefixBucket<V>]>,
    prefix_lengths: Box<[u8]>,
    len: usize,
}

impl<V: Copy> Ip6PrefixHashTable<V> {
    #[inline]
    pub fn empty() -> Self {
        Self::with_capacity(1)
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
        let key = Ip6PrefixKey::new(prefix.addr(), prefix_len);
        if (self.len + 1) * 2 >= self.buckets.len() {
            self.grow();
        }
        if self.insert_key_value(key, value) {
            self.insert_prefix_len(prefix_len);
        }
    }

    #[inline(always)]
    pub fn lookup(&self, destination: Ipv6Addr) -> Option<V> {
        for prefix_len in self.prefix_lengths.iter().copied() {
            if let Some(value) = self.lookup_key(Ip6PrefixKey::new(destination, prefix_len)) {
                return Some(value);
            }
        }
        None
    }

    #[inline(always)]
    pub fn lookup_key(&self, key: Ip6PrefixKey) -> Option<V> {
        let mut slot = self.slot(key);
        loop {
            let bucket = &self.buckets[slot];
            match bucket.entry {
                Some(entry) if entry.key == key => return Some(entry.value),
                Some(_) => slot = self.next_slot(slot),
                None => return None,
            }
        }
    }

    #[inline(always)]
    pub fn prefetch_key(&self, key: Ip6PrefixKey) {
        prefetch_read_l1(&self.buckets[self.slot(key)]);
    }

    #[inline(always)]
    pub fn prefetch_destination(&self, destination: Ipv6Addr) {
        for prefix_len in self.prefix_lengths.iter().take(2).copied() {
            self.prefetch_key(Ip6PrefixKey::new(destination, prefix_len));
        }
    }

    #[inline(always)]
    pub fn prefix_lengths(&self) -> &[u8] {
        &self.prefix_lengths
    }

    #[inline(always)]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two().max(1);
        Self {
            buckets: vec![Ip6PrefixBucket::empty(); capacity].into_boxed_slice(),
            prefix_lengths: Box::new([]),
            len: 0,
        }
    }

    #[inline]
    fn grow(&mut self) {
        let next_capacity = self.buckets.len() * 2;
        let old_buckets = std::mem::replace(
            &mut self.buckets,
            vec![Ip6PrefixBucket::empty(); next_capacity].into_boxed_slice(),
        );
        self.len = 0;
        for bucket in old_buckets.iter().copied() {
            if let Some(entry) = bucket.entry {
                self.insert_key_value(entry.key, entry.value);
            }
        }
    }

    #[inline]
    fn insert_key_value(&mut self, key: Ip6PrefixKey, value: V) -> bool {
        let mut slot = self.slot(key);
        loop {
            let bucket = &mut self.buckets[slot];
            match bucket.entry {
                Some(entry) if entry.key == key => {
                    bucket.entry = Some(Ip6PrefixEntry { key, value });
                    return false;
                }
                Some(_) => slot = self.next_slot(slot),
                None => {
                    bucket.entry = Some(Ip6PrefixEntry { key, value });
                    self.len += 1;
                    return true;
                }
            }
        }
    }

    #[inline]
    fn insert_prefix_len(&mut self, prefix_len: u8) {
        if self.prefix_lengths.contains(&prefix_len) {
            return;
        }
        let mut prefix_lengths = self.prefix_lengths.to_vec();
        prefix_lengths.push(prefix_len);
        prefix_lengths.sort_by(|a, b| b.cmp(a));
        self.prefix_lengths = prefix_lengths.into_boxed_slice();
    }

    #[inline(always)]
    fn slot(&self, key: Ip6PrefixKey) -> usize {
        hash_key(key) & (self.buckets.len() - 1)
    }

    #[inline(always)]
    fn next_slot(&self, slot: usize) -> usize {
        (slot + 1) & (self.buckets.len() - 1)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Ip6PrefixBucket<V: Copy> {
    entry: Option<Ip6PrefixEntry<V>>,
}

impl<V: Copy> Ip6PrefixBucket<V> {
    #[inline(always)]
    const fn empty() -> Self {
        Self { entry: None }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Ip6PrefixEntry<V: Copy> {
    key: Ip6PrefixKey,
    value: V,
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

#[inline(always)]
pub fn mask_ipv6(addr: Ipv6Addr, prefix_len: u8) -> u128 {
    assert!(prefix_len <= 128, "invalid IPv6 prefix length");
    if prefix_len == 0 {
        return 0;
    }
    let value = u128::from(addr);
    value & (!0u128 << (128 - prefix_len))
}

#[inline(always)]
fn hash_key(key: Ip6PrefixKey) -> usize {
    let mixed = key.masked ^ (key.masked >> 64) ^ u128::from(key.prefix_len);
    splitmix64(mixed as u64) as usize
}

#[inline(always)]
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
