# VPP Pool Alignment Review

## Feature and Changed Surface

Issue #270 changes `hammer-infra::Pool` to use numeric VPP indexes, VPP free-index reuse, `Option` lookups/removal, and panic-on-infra allocation exhaustion. The affected ownership surfaces are `hammer-infra::pool`, `hammer-infra::fifo`, `hammer-infra::rbtree`, and their Session/File callers.

## VPP Analog and Evidence

- `third_party/vpp/src/vppinfra/pool.h` defines `free_bitmap`, `free_indices`, `max_elts`, and `opaque`; `pool_get` reuses the last free index and fixed-pool exhaustion exits through the infra allocator boundary.
- `third_party/vpp/src/vppinfra/rbtree.h` defines `rb_tree_t.nodes` as a Pool, reserves index zero as `T.nil`, and stores the root in Pool `opaque`.
- `third_party/vpp/src/vppinfra/rbtree.c` allocates nodes with `pool_get_zero` and releases them with `pool_put`.

## Verdict

Needs changes until the final workspace test gate is run. The implementation is otherwise aligned on Pool ownership, numeric indexes, free-index LIFO reuse, RB-tree sentinel/root storage, and caller-visible `Option` handling.

## Findings

### Non-blocking

- `Pool` currently retains Hammer's existing cache-line storage policy. The issue records explicit alignment as an optional type-level policy; no caller is required to provide an alignment on the default path.
- `cargo check -p hammer-infra --all-targets` and `cargo check -p hammer-service --lib` pass. Workspace all-target checking is blocked by an unrelated pre-existing type-inference error in `hammer-stats/src/protocol.rs:1344`.

## Commands Run

- `cargo check -p hammer-infra --all-targets`
- `cargo check -p hammer-service --lib`
- `cargo check --workspace --all-targets` (blocked by unrelated `hammer-stats` test inference)
- `cargo fmt --all`
- `git diff --check`
