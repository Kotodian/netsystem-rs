use crate::boxed::Slice;
use crate::prefetch::prefetch_read_l1;
use crate::vec::Vec;

pub trait FlatHashKey: Copy + Eq {
    fn hash_key(self) -> usize;
}

impl FlatHashKey for u128 {
    #[inline(always)]
    fn hash_key(self) -> usize {
        splitmix64((self ^ (self >> 64)) as u64) as usize
    }
}

impl FlatHashKey for u64 {
    #[inline(always)]
    fn hash_key(self) -> usize {
        splitmix64(self) as usize
    }
}

impl FlatHashKey for u32 {
    #[inline(always)]
    fn hash_key(self) -> usize {
        splitmix64(u64::from(self)) as usize
    }
}

impl FlatHashKey for u16 {
    #[inline(always)]
    fn hash_key(self) -> usize {
        splitmix64(u64::from(self)) as usize
    }
}

#[derive(Debug, Clone)]
pub struct FlatHashTable<K: FlatHashKey, V: Copy> {
    buckets: Slice<FlatHashBucket<K, V>>,
    len: usize,
}

impl<K: FlatHashKey, V: Copy> FlatHashTable<K, V> {
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(1)
    }

    #[inline]
    pub fn empty() -> Self {
        Self::new()
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two().max(1);
        Self {
            buckets: Slice::from_elem(capacity, FlatHashBucket::empty()),
            len: 0,
        }
    }

    #[inline]
    pub fn from_entries(entries: impl IntoIterator<Item = (K, V)>) -> Self {
        let mut table = Self::new();
        for (key, value) in entries {
            table.insert(key, value);
        }
        table
    }

    #[inline]
    pub fn insert(&mut self, key: K, value: V) {
        if (self.len + 1) * 2 >= self.buckets.len() {
            self.grow();
        }
        self.insert_key_value(key, value);
    }

    #[inline]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let mut slot = self.slot(*key);
        loop {
            match self.buckets[slot].entry {
                Some(entry) if entry.key == *key => {
                    let value = entry.value;
                    self.buckets[slot].entry = None;
                    self.len -= 1;
                    self.reinsert_cluster_after_removed_slot(slot);
                    return Some(value);
                }
                Some(_) => slot = self.next_slot(slot),
                None => return None,
            }
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<&V> {
        let mut slot = self.slot(*key);
        loop {
            let bucket = &self.buckets[slot];
            match bucket.entry.as_ref() {
                Some(entry) if entry.key == *key => return Some(&entry.value),
                Some(_) => slot = self.next_slot(slot),
                None => return None,
            }
        }
    }

    #[inline(always)]
    pub fn lookup(&self, key: &K) -> Option<V> {
        self.get(key).copied()
    }

    #[inline(always)]
    pub fn prefetch_key(&self, key: &K) {
        prefetch_read_l1(&self.buckets[self.slot(*key)]);
    }

    #[inline(always)]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    #[inline(always)]
    pub fn bucket_ptr(&self) -> *const u8 {
        self.buckets.as_ptr().cast::<u8>()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn grow(&mut self) {
        let next_capacity = self.buckets.len() * 2;
        let old_buckets = std::mem::replace(
            &mut self.buckets,
            Slice::from_elem(next_capacity, FlatHashBucket::empty()),
        );
        self.len = 0;
        for bucket in old_buckets.iter().copied() {
            if let Some(entry) = bucket.entry {
                self.insert_key_value(entry.key, entry.value);
            }
        }
    }

    #[inline]
    fn insert_key_value(&mut self, key: K, value: V) -> bool {
        let mut slot = self.slot(key);
        loop {
            let bucket = &mut self.buckets[slot];
            match bucket.entry {
                Some(entry) if entry.key == key => {
                    bucket.entry = Some(FlatHashEntry { key, value });
                    return false;
                }
                Some(_) => slot = self.next_slot(slot),
                None => {
                    bucket.entry = Some(FlatHashEntry { key, value });
                    self.len += 1;
                    return true;
                }
            }
        }
    }

    #[inline]
    fn reinsert_cluster_after_removed_slot(&mut self, removed_slot: usize) {
        let mut slot = self.next_slot(removed_slot);
        while let Some(entry) = self.buckets[slot].entry {
            self.buckets[slot].entry = None;
            self.len -= 1;
            self.insert_key_value(entry.key, entry.value);
            slot = self.next_slot(slot);
        }
    }

    #[inline(always)]
    fn slot(&self, key: K) -> usize {
        key.hash_key() & (self.buckets.len() - 1)
    }

    #[inline(always)]
    fn next_slot(&self, slot: usize) -> usize {
        (slot + 1) & (self.buckets.len() - 1)
    }
}

impl<K: FlatHashKey, V: Copy> Default for FlatHashTable<K, V> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct FlatHashBucket<K: FlatHashKey, V: Copy> {
    entry: Option<FlatHashEntry<K, V>>,
}

impl<K: FlatHashKey, V: Copy> FlatHashBucket<K, V> {
    #[inline(always)]
    const fn empty() -> Self {
        Self { entry: None }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct FlatHashEntry<K: FlatHashKey, V: Copy> {
    key: K,
    value: V,
}

#[derive(Debug, Clone)]
pub struct PrefixLengthSearchOrder {
    prefix_lengths: Slice<u8>,
}

impl PrefixLengthSearchOrder {
    #[inline]
    pub fn empty() -> Self {
        Self {
            prefix_lengths: Slice::new(),
        }
    }

    #[inline]
    pub fn insert(&mut self, prefix_len: u8) {
        if self.prefix_lengths.contains(&prefix_len) {
            return;
        }
        let mut prefix_lengths = self.prefix_lengths.iter().copied().collect::<Vec<_>>();
        prefix_lengths.push(prefix_len);
        prefix_lengths.sort_by(|a, b| b.cmp(a));
        self.prefix_lengths = prefix_lengths.into_boxed_slice();
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.prefix_lengths
    }
}

impl Default for PrefixLengthSearchOrder {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[inline(always)]
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
