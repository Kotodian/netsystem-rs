# Task 2 Report: Add existing TCP lookup key support for bihash

## What I implemented

- Added `BihashKey` support for existing `TransportConnectionKey<Ipv4Addr>` and `TransportConnectionKey<Ipv6Addr>` in `crates/hammer-core/src/protocol/transport.rs`.
- Added explicit `Default` impls for `TransportConnectionKey<Ipv4Addr>`, `TransportConnectionKey<Ipv6Addr>`, and `TransportConnectionKey<IpAddr>` so bihash tables can instantiate empty slots.
- Added `Bihash` round-trip tests for IPv4 and IPv6 transport connection keys in `crates/hammer-core/tests/protocol_tcp.rs`.
- Added `BihashKey` and `Default` support for existing `TcpListenerKey<A>` in `crates/hammer-service/src/transport/tcp/lookup.rs`.
- Added private `PoolIndex <-> u64` bihash value encoding helpers in `crates/hammer-service/src/transport/tcp/lookup.rs`.
- Added TCP lookup tests for listener-key bihash use and pool-index round-trip encoding inside `crates/hammer-service/src/transport/tcp/lookup.rs`.
- Kept existing legacy `FlatHashKey` compatibility where current `FlatHashTable` call sites still depend on it, because Task 3 has not migrated those tables yet.

## TDD evidence

### RED

1. `cargo test -p hammer-core --test protocol_tcp transport_connection_key_works_as_bihash_key -- --nocapture`
   - Failed as expected.
   - Key errors:
     - `TransportConnectionKey<Ipv4Addr>: BihashKey` not implemented
     - `TransportConnectionKey<Ipv6Addr>: BihashKey` not implemented
     - `TransportConnectionKey<...>: Default` not implemented

2. `cargo test -p hammer-service tcp_listener_key_works_as_bihash_key -- --nocapture`
   - Failed as expected for the new seam.
   - Key task-related errors:
     - `TcpListenerKey<TcpIpv4ListenerAddress>: BihashKey` not implemented
     - `TcpListenerKey<TcpIpv4ListenerAddress>: Default` not implemented
     - `pool_index_to_bihash_value` / `pool_index_from_bihash_value` missing
   - The same build also exposed a pre-existing unrelated crate failure:
     - `crates/hammer-service/src/transport/tcp/input.rs:884` `E0425 cannot find value 'node' in this scope`

### GREEN

1. `cargo test -p hammer-core --test protocol_tcp transport_connection_key_v -- --nocapture`
   - Passed.
   - Result: `2 passed; 0 failed`

2. `cargo test -p hammer-core --test protocol_tcp -- --nocapture`
   - Passed.
   - Result: `5 passed; 0 failed`

3. `cargo test -p hammer-service --lib tcp_listener_key_works_as_bihash_key -- --nocapture`
   - The task-specific bihash/encoding errors were resolved.
   - Build is still blocked by the unrelated existing error:
     - `crates/hammer-service/src/transport/tcp/input.rs:884` `E0425 cannot find value 'node' in this scope`

4. `cargo test -p hammer-service --lib pool_index_bihash_value_round_trip -- --nocapture`
   - Same result as above.
   - The task-specific compile failures are gone, but the crate still stops on the same unrelated `input.rs` error.

## Tests run and results

- `cargo test -p hammer-core --test protocol_tcp transport_connection_key_works_as_bihash_key -- --nocapture`
  - RED, failed for missing bihash/default support.
- `cargo test -p hammer-service tcp_listener_key_works_as_bihash_key -- --nocapture`
  - RED, failed for missing listener-key bihash support, missing pool-index helpers, and an unrelated existing crate error.
- `cargo test -p hammer-core --test protocol_tcp transport_connection_key_v -- --nocapture`
  - PASS, 2 tests passed.
- `cargo test -p hammer-core --test protocol_tcp -- --nocapture`
  - PASS, 5 tests passed.
- `cargo test -p hammer-service --lib tcp_listener_key_works_as_bihash_key -- --nocapture`
  - BLOCKED by pre-existing `crates/hammer-service/src/transport/tcp/input.rs:884` compile error.
- `cargo test -p hammer-service --lib pool_index_bihash_value_round_trip -- --nocapture`
  - BLOCKED by the same pre-existing compile error.

## Files changed

- `crates/hammer-core/src/protocol/transport.rs`
- `crates/hammer-core/tests/protocol_tcp.rs`
- `crates/hammer-service/src/transport/tcp/lookup.rs`
- `.superpowers/sdd/tcp-bihash-task-2-report.md`

## Self-review findings

- The task stays within the required seam: no wrapper key types, no raw `[u64; N]` or `u128` call-site plumbing, and no test-only public APIs were added.
- The new pool-index codec is private and uses `FREE_U64` protection with `debug_assert_ne!`.
- The service lookup file still needs legacy `FlatHashKey` compatibility because Task 3 has not migrated those `FlatHashTable` users yet.
- Importing `BihashKey` into `lookup.rs` shadowed existing standard `Hash::hash(...)` calls; I fixed those sites by disambiguating them with `Hash::hash(...)`.

## Concerns

- `hammer-service` unit-test verification is currently blocked by an unrelated pre-existing compile failure in `crates/hammer-service/src/transport/tcp/input.rs:884`:
  - `error[E0425]: cannot find value 'node' in this scope`
- Because of that blocker, I could not produce a green `hammer-service` test run for the two new lookup tests from this task, even though the task-specific missing-trait and missing-helper errors are resolved.

## Follow-up: compile blocker fix

- Reordered the `tcp_input_handoffs_existing_session_to_owner_worker` test setup in `crates/hammer-service/src/transport/tcp/input.rs` so `node` is registered before `runtime.buffers().get_next_frame(node)` is called.
- This matches the nearby working test pattern and removes the compile blocker without changing task logic.

### Verification

- `cargo test -p hammer-service --lib tcp_listener_key_works_as_bihash_key -- --nocapture`
  - Passed.
  - Result: `1 passed; 0 failed`
- `cargo test -p hammer-service --lib pool_index_bihash_value_round_trip -- --nocapture`
  - Passed.
  - Result: `1 passed; 0 failed`

### Current status

- The earlier `node`-before-declaration compile error no longer appears in focused `hammer-service` verification.
- No additional code paths were changed for Task 2.
