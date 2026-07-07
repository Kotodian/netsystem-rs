# Task 3 Report: Migrate all TCP lookup indices from `FlatHashTable` to bihash

## What I implemented

- Migrated TCP dataplane lookup ownership in `crates/hammer-service/src/transport/tcp/lookup.rs` from `FlatHashTable` to bihash-backed indices:
  - connection session/connection/tuple routes
  - pending-open session/tuple routes
  - listener snapshot storage
  - listener pending tuple/count indices
  - Fast Open cache tuple indices
- Kept business ownership in existing `Pool` / `Vec` storage and stored only `u64` handles in bihash values via `pool_index_to_bihash_value` / `pool_index_from_bihash_value`.
- Split tuple lookup/indexing by address family with existing domain keys instead of introducing wrapper key types.
- Removed `FlatHashTable` / `FlatHashKey` usage from `crates/hammer-service/src/transport/tcp/lookup.rs`.
- Added/updated targeted regression coverage in:
  - `crates/hammer-service/src/transport/tcp/lookup.rs`
  - `crates/hammer-service/src/transport/tcp/input.rs`
- Fixed the input-side test harness to wire `TcpInputNext::nodes(...)` in actual enum order, then verified the follow-on node sees the stamped session route.

## TDD evidence

### Slice 1: input follow-on session route propagation

- RED:
  - Command: `cargo test -p hammer-service tcp_input_preserves_session_route_in_opaque_for_follow_on_nodes -- --nocapture`
  - Result: failed with `left: None` / `right: Some(SessionId(...))`
  - Root cause investigation showed the test helper built `TcpInputNext::nodes(...)` in the wrong enum-slot order, so the probe was attached to the wrong next edge.
- GREEN:
  - Command: `cargo test -p hammer-service tcp_input_preserves_session_route_in_opaque_for_follow_on_nodes -- --nocapture`
  - Result: `1 passed; 0 failed`

### Slice 2: handoff/input next-slot routing

- RED:
  - Same miswired `TcpInputNext::nodes(...)` helper caused the handoff test to assert against the wrong next node slot.
- GREEN:
  - Command: `cargo test -p hammer-service tcp_input_handoffs_existing_session_to_owner_worker -- --nocapture`
  - Result: `1 passed; 0 failed`

### Slice 3: lookup bihash migration seams

- Added targeted regression tests for:
  - route lookup preserving both IPv4 and IPv6 entries
  - listener snapshot lookup returning the stored value
  - Fast Open cache updating an existing tuple in place
- GREEN verification:
  - Command: `cargo test -p hammer-service transport::tcp::lookup -- --nocapture`
  - Result: `21 passed; 0 failed`

## Tests run and results

- `cargo test -p hammer-service tcp_input_preserves_session_route_in_opaque_for_follow_on_nodes -- --nocapture`
  - passed
- `cargo test -p hammer-service tcp_input_handoffs_existing_session_to_owner_worker -- --nocapture`
  - passed
- `cargo test -p hammer-service transport::tcp::lookup -- --nocapture`
  - passed (`21 passed; 0 failed`)
- `cargo test -p hammer-service transport::tcp::input -- --nocapture`
  - passed (`7 passed; 0 failed`)
- `cargo check -p hammer-service`
  - passed
- `cargo fmt --all`
  - passed
- `cargo fmt --all -- --check`
  - passed
- `git diff --check -- crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-service/src/transport/tcp/input.rs`
  - passed
- `rg -n "FlatHashTable|FlatHashKey" crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-core/src/protocol/transport.rs`
  - only expected compatibility hit remains in `crates/hammer-core/src/protocol/transport.rs`
- `rg -n "TcpBihashKey|TcpV4RouteKey|TcpV6RouteKey|std::vec::Vec|pub trait .*Free" crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-core/src/protocol/transport.rs crates/hammer-infra/src/bihash`
  - no matches

## Files changed

- `crates/hammer-infra/src/bihash/mod.rs`
- `crates/hammer-infra/tests/bihash.rs`
- `crates/hammer-core/src/protocol/transport.rs`
- `crates/hammer-service/src/transport/tcp/lookup.rs`
- `crates/hammer-service/src/transport/tcp/input.rs`
- `.superpowers/sdd/tcp-bihash-task-3-report.md`

## Self-review findings

- No `FlatHashTable` / `FlatHashKey` usage remains in `lookup.rs`.
- No forbidden TCP lookup wrapper key types were introduced.
- No TCP lookup call sites were converted to raw `[u64; N]` / `u128` plumbing.
- Bihash values remain `u64` handles while pools/vectors retain business ownership.
- Input regression turned out to be a test-helper next-slot bug, not a dataplane behavior bug.

## Concerns

- None for this task. The remaining `TransportConnectionKey<IpAddr>` flat-hash compatibility in `crates/hammer-core/src/protocol/transport.rs` is the expected temporary compatibility noted in the task brief.
