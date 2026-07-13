# Hammer io-uring fork

This source is vendored from `io-uring` v0.7.13 at upstream commit
`6789e176157cdc88a64b8f2ca9c56ef48756c1a7`.

Hammer keeps the fork in-tree so the kernel ring mapping and resource lifetime
contract can be audited with the data-plane runtime. SQ, CQ, and SQE mappings
remain fd-backed `MAP_SHARED` mappings because they are shared with the Linux
kernel; they must not be redirected into Hammer heap or SVM segments.

Worker-owned rings are built with `Builder::dontfork()`. The upstream API
already applies `MADV_DONTFORK` to every ring mapping, so Hammer does not carry
a duplicate mapping implementation.
