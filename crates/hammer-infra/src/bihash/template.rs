//! Type aliases for common key-value sizes.
//!
//! Each alias pre-selects `KVP` so that one value page fits roughly 1 cache line.

use crate::bihash::Bihash;

/// VPP `clib_bihash_8_8`: 7 KV pairs per value page.
pub type Bihash8x8 = Bihash<u64, 7>;

/// VPP `clib_bihash_16_8`: 4 KV pairs per value page.
pub type Bihash16x8 = Bihash<u128, 4>;

/// VPP `clib_bihash_24_8`: 4 KV pairs per value page.
pub type Bihash24x8 = Bihash<[u64; 3], 4>;

/// VPP `clib_bihash_48_8`: 4 KV pairs per value page.
pub type Bihash48x8 = Bihash<[u64; 6], 4>;
