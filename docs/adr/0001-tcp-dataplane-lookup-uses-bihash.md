# TCP dataplane lookup uses bihash

Hammer TCP dataplane exact-match lookup uses VPP-style bihash instead of `FlatHashTable` or `std` hash maps. Bihash keys remain the existing TCP/session domain key types with `BihashKey` hashing implementations and Rust `Eq` equality, while bihash values are opaque `u64` handles such as packed pool indices or Hammer `Vec` indices; business records stay owned by pools, sessions, or listener/cache state. This matches VPP session lookup semantics, keeps packet-path lookup predictable, and avoids expanding the TCP hot path with wrapper key/value types.
