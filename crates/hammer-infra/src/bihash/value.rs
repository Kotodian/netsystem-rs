//! Value-page types for VPP-style bihash.
//!
//! A value page is a fixed-size array of `Kv` (key-value) slots.  The most
//! common case is `(u64, u64)` with `KVP = 7` (roughly one cache line) or
//! `KVP = 4` (exactly one 64-byte cache line).

/// 8-byte free sentinel — VPP's `0xFEEDFACE8BADF00D`.
pub const FREE_U64: u64 = 0xFEEDFACE_8BADF00D;

/// Trait for value types that have a distinct "free" sentinel.
///
/// `Bihash` stores `V` inline in each slot and needs a way to mark a slot
/// as free without overwriting the caller's value. Phase 1 only requires
/// this for `u64`; the sentinel is `FREE_U64`. Phase 2 can add more impls
/// (e.g. for `u32` or composite types).
pub trait BihashFree: Copy + Eq {
    /// Returns the canonical free-sentinel value for this type.
    fn free_sentinel() -> Self;
    /// Returns `true` when `self` equals the free sentinel.
    fn is_free_value(&self) -> bool {
        *self == Self::free_sentinel()
    }
}

impl BihashFree for u64 {
    fn free_sentinel() -> u64 {
        FREE_U64
    }
}

/// A single (key, value) pair.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct Kv<K, V> {
    pub key: K,
    pub value: V,
}

// Specialized impl for (u64, u64) — the most common case.
impl Kv<u64, u64> {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            key: 0,
            value: FREE_U64,
        }
    }

    #[inline(always)]
    pub const fn is_free(&self) -> bool {
        self.value == FREE_U64
    }

    #[inline(always)]
    pub fn mark_free(&mut self) {
        self.value = FREE_U64;
    }

    #[inline(always)]
    pub const fn key_eq(a: u64, b: u64) -> bool {
        a == b
    }
}

/// A value page — `KVP` contiguous `Kv` slots.
#[derive(Clone)]
pub struct ValuePage<K, V, const KVP: usize> {
    slots: [Kv<K, V>; KVP],
}

impl<K: Copy + Default, V: Copy + Default + BihashFree, const KVP: usize> ValuePage<K, V, KVP> {
    pub fn new() -> Self {
        Self {
            slots: core::array::from_fn(|_| Kv {
                key: K::default(),
                value: V::free_sentinel(),
            }),
        }
    }

    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        KVP
    }

    #[inline(always)]
    pub const fn slots(&self) -> &[Kv<K, V>; KVP] {
        &self.slots
    }

    #[inline(always)]
    pub fn slots_mut(&mut self) -> &mut [Kv<K, V>; KVP] {
        &mut self.slots
    }
}

// Specialized free-count for (u64, u64, 7) — KVP=7 fits roughly 1 cache line.
impl ValuePage<u64, u64, 7> {
    pub fn free_count(&self) -> usize {
        self.slots.iter().filter(|kv| kv.is_free()).count()
    }

    pub fn is_all_free(&self) -> bool {
        self.free_count() == 7
    }
}

// Specialized for (u64, u64, 4) — KVP=4 is exactly 1 cache line (64 bytes).
impl ValuePage<u64, u64, 4> {
    pub fn free_count(&self) -> usize {
        self.slots.iter().filter(|kv| kv.is_free()).count()
    }

    pub fn is_all_free(&self) -> bool {
        self.free_count() == 4
    }
}

impl<K: Copy + Default, V: Copy + Default + BihashFree, const KVP: usize> Default
    for ValuePage<K, V, KVP>
{
    fn default() -> Self {
        Self::new()
    }
}
