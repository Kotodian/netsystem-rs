//! Value-page types for VPP-style bihash.
//!
//! A value page is a fixed-size array of `Kv` (key-value) slots with a fixed
//! `u64` value payload. The common case is `(u64, u64)` with `KVP = 7`
//! (roughly one cache line) or `KVP = 4` (exactly one 64-byte cache line).

/// 8-byte free sentinel — VPP's `0xFEEDFACE8BADF00D`.
pub const FREE_U64: u64 = 0xFEEDFACE_8BADF00D;

/// A single (key, value) pair.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Kv<K> {
    pub key: K,
    pub value: u64,
}

impl<K: Default> Default for Kv<K> {
    #[inline]
    fn default() -> Self {
        Self {
            key: K::default(),
            value: FREE_U64,
        }
    }
}

impl<K> Kv<K> {
    #[inline(always)]
    pub const fn is_free(&self) -> bool {
        self.value == FREE_U64
    }

    #[inline(always)]
    pub fn mark_free(&mut self) {
        self.value = FREE_U64;
    }
}

/// A value page — `KVP` contiguous `Kv` slots.
#[derive(Clone)]
pub struct ValuePage<K, const KVP: usize> {
    slots: [Kv<K>; KVP],
}

impl<K: Copy + Default, const KVP: usize> ValuePage<K, KVP> {
    pub fn new() -> Self {
        Self {
            slots: core::array::from_fn(|_| Kv::default()),
        }
    }

    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        KVP
    }

    #[inline(always)]
    pub const fn slots(&self) -> &[Kv<K>; KVP] {
        &self.slots
    }

    #[inline(always)]
    pub fn slots_mut(&mut self) -> &mut [Kv<K>; KVP] {
        &mut self.slots
    }
}

// Specialized free-count for (u64, u64, 7) — KVP=7 fits roughly 1 cache line.
impl ValuePage<u64, 7> {
    pub fn free_count(&self) -> usize {
        self.slots.iter().filter(|kv| kv.is_free()).count()
    }

    pub fn is_all_free(&self) -> bool {
        self.free_count() == 7
    }
}

// Specialized for (u64, u64, 4) — KVP=4 is exactly 1 cache line (64 bytes).
impl ValuePage<u64, 4> {
    pub fn free_count(&self) -> usize {
        self.slots.iter().filter(|kv| kv.is_free()).count()
    }

    pub fn is_all_free(&self) -> bool {
        self.free_count() == 4
    }
}

impl<K: Copy + Default, const KVP: usize> Default for ValuePage<K, KVP> {
    fn default() -> Self {
        Self::new()
    }
}
