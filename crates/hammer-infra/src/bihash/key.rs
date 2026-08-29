/// Trait implemented by every key type accepted by `Bihash`.
///
/// Mirrors VPP's per-template hash function but uses Rust `Eq` for key
/// equality and returns a `u64` hash so behavior is identical on 32- and
/// 64-bit targets.
pub trait BihashKey: Copy + Eq {
    /// Platform-independent 64-bit hash of the key.
    fn hash(self) -> u64;
}

impl BihashKey for u64 {
    #[inline(always)]
    fn hash(self) -> u64 {
        xxhash64_word(self)
    }
}

impl BihashKey for u32 {
    #[inline(always)]
    fn hash(self) -> u64 {
        xxhash64_word(u64::from(self))
    }
}

impl BihashKey for u16 {
    #[inline(always)]
    fn hash(self) -> u64 {
        xxhash64_word(u64::from(self))
    }
}

impl BihashKey for usize {
    #[inline(always)]
    fn hash(self) -> u64 {
        xxhash64_word(self as u64)
    }
}

impl BihashKey for u128 {
    #[inline(always)]
    fn hash(self) -> u64 {
        let folded = (self ^ (self >> 64)) as u64;
        xxhash64_word(folded)
    }
}

impl BihashKey for [u64; 3] {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&self)
    }
}

impl BihashKey for [u64; 6] {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&self)
    }
}

/// XOR-fold helper used by composite keys.
#[inline(always)]
pub fn hash_words(words: &[u64]) -> u64 {
    let folded = words.iter().copied().fold(0, |acc, word| acc ^ word);
    xxhash64_word(folded)
}

#[inline(always)]
fn xxhash64_word(value: u64) -> u64 {
    const P1: u64 = 11_400_714_746_178_003_727;
    const P2: u64 = 14_029_467_366_897_019_727;
    const P4: u64 = 9_650_029_242_287_828_579;
    const P5: u64 = 2_870_177_450_012_600_261;
    let mut hash = P5.wrapping_add(8);
    let lane = value.wrapping_mul(P2).rotate_left(31).wrapping_mul(P1);
    hash ^= lane;
    hash = hash.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(P2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(1_609_587_929_392_839_161);
    hash ^ (hash >> 32)
}

/// SplitMix64 finalizer used by scalar and composite bihash keys.
#[inline(always)]
pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
