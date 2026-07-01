/// Trait implemented by every key type accepted by `Bihash`.
///
/// Mirrors VPP's per-template function set (`hash`, `key_compare`) but
/// returns a `u64` hash so behavior is identical on 32- and 64-bit targets.
pub trait BihashKey: Copy + Eq {
    /// Platform-independent 64-bit hash of the key.
    fn hash(self) -> u64;
    /// Equality check. Separate from `PartialEq` so `#[inline(always)]`
    /// is not overridden by trait dispatch in generic contexts.
    fn key_eq(self, other: Self) -> bool;
}

impl BihashKey for u64 {
    #[inline(always)]
    fn hash(self) -> u64 {
        splitmix64(self)
    }
    #[inline(always)]
    fn key_eq(self, other: Self) -> bool {
        self == other
    }
}

impl BihashKey for u32 {
    #[inline(always)]
    fn hash(self) -> u64 {
        splitmix64(u64::from(self))
    }
    #[inline(always)]
    fn key_eq(self, other: Self) -> bool {
        self == other
    }
}

impl BihashKey for u16 {
    #[inline(always)]
    fn hash(self) -> u64 {
        splitmix64(u64::from(self))
    }
    #[inline(always)]
    fn key_eq(self, other: Self) -> bool {
        self == other
    }
}

impl BihashKey for usize {
    #[inline(always)]
    fn hash(self) -> u64 {
        splitmix64(self as u64)
    }
    #[inline(always)]
    fn key_eq(self, other: Self) -> bool {
        self == other
    }
}

impl BihashKey for u128 {
    #[inline(always)]
    fn hash(self) -> u64 {
        let folded = (self ^ (self >> 64)) as u64;
        splitmix64(folded)
    }
    #[inline(always)]
    fn key_eq(self, other: Self) -> bool {
        self == other
    }
}

impl BihashKey for [u64; 3] {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&self)
    }
    #[inline(always)]
    fn key_eq(self, other: Self) -> bool {
        self[0] == other[0] && self[1] == other[1] && self[2] == other[2]
    }
}

impl BihashKey for [u64; 6] {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&self)
    }
    #[inline(always)]
    fn key_eq(self, other: Self) -> bool {
        self[0] == other[0]
            && self[1] == other[1]
            && self[2] == other[2]
            && self[3] == other[3]
            && self[4] == other[4]
            && self[5] == other[5]
    }
}

/// XOR-fold helper used by composite keys.
#[inline(always)]
pub fn hash_words(words: &[u64]) -> u64 {
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    for w in words {
        state ^= splitmix64(*w ^ state);
        state = state.rotate_left(13);
    }
    splitmix64(state)
}

/// The same splitmix64 used by `FlatHashTable` in `crates/hammer-infra/src/map.rs`.
#[inline(always)]
pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
