//! Type aliases for common key-value sizes.
//!
//! Each alias pre-selects `KVP` so that one value page fits roughly 1 cache line.

use crate::bihash::Bihash;

/// 8-byte key + 8-byte value -> 16 bytes per KV pair.
/// 7 per page -> 112 bytes (1.75 cache lines; 4 -> exactly 64 bytes).
pub type Bihash8x8<V> = Bihash<u64, V, 7>;

/// 16-byte key + 8-byte value -> 24 bytes per KV pair.
/// 3 per page -> 72 bytes (1.125 cache lines).
pub type Bihash16x8<V> = Bihash<u128, V, 3>;

/// 24-byte key + 8-byte value -> 32 bytes per KV pair.
/// 2 per page -> 64 bytes (exactly 1 cache line).
pub type Bihash24x8<V> = Bihash<[u64; 3], V, 2>;

/// 48-byte key + 8-byte value -> 56 bytes per KV pair.
/// 1 per page -> 56 bytes (fits 1 cache line).
pub type Bihash48x8<V> = Bihash<[u64; 6], V, 1>;
