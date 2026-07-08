# VPP Session Scheduling Task 4 Report

## What I implemented

- Replaced the old `SessionRxEnqueue` field bag with `RxDelivery` in `crates/hammer-service/src/session/runtime.rs`.
- Added `OooSpan { start, len: NonZeroU32 }` and a hot-path size guard for `RxDelivery`.
- Kept enqueue errors on the existing `CoreResult` boundary.
- Modeled legal RX outcomes directly:
  - `RxDelivery::NotAccepted { available }`
  - `RxDelivery::InOrder { accepted }`
  - `RxDelivery::OutOfOrder { accepted, delivered, span }`
- Updated session runtime RX enqueue logic to produce `RxDelivery` and keep RX capacity facts in session runtime.
- Updated TCP receive consumers to match on `RxDelivery` instead of reading `accepted_len`, `delivered_len`, `newest_ooo_start`, and `newest_ooo_len`.
- Added OOO accepted-byte accounting in `hammer-infra::fifo::enqueue_ooo` so exact duplicate OOO enqueue can report zero newly accepted bytes.
- Added/updated tests for:
  - zero accepted -> `NotAccepted`
  - non-zero `OooSpan`
  - in-order delivery carrying no OOO facts
  - TCP receive behavior for in-order, OOO, and not-accepted outcomes
  - size guard / architecture surface checks

## Tests and results

- `cargo test -p hammer-service rx_delivery` -> passed
- `cargo test -p hammer-service enqueue_rx` -> passed
- `cargo test -p hammer-infra duplicate_ooo_enqueue_reports_zero_newly_accepted_bytes` -> passed
- `cargo test -p hammer-service` -> passed
- `cargo fmt --all` -> passed
- `git diff --check` -> passed

## TDD Evidence

### RED

Command:

```bash
cargo test -p hammer-service session_rx_delivery_models_legal_outcomes
```

Output excerpt:

```text
error[E0432]: unresolved imports `crate::session::runtime::OooSpan`, `crate::session::runtime::RxDelivery`
error[E0433]: cannot find type `OooSpan` in this scope
error[E0061]: this method takes 5 arguments but 3 arguments were supplied
error[E0433]: cannot find type `RxDelivery` in this scope
```

This was the intended RED state: the new tests referenced the new RX result model and TCP receive API before production code existed.

### GREEN

Commands:

```bash
cargo test -p hammer-service rx_delivery
cargo test -p hammer-service enqueue_rx
cargo test -p hammer-infra duplicate_ooo_enqueue_reports_zero_newly_accepted_bytes
cargo test -p hammer-service
cargo fmt --all
git diff --check
```

Output excerpt:

```text
test transport::tcp::connection::tests::tcp_receive_payload_not_accepted_rx_delivery_leaves_receive_state_unchanged ... ok
test transport::tcp::connection::tests::tcp_receive_payload_ooo_rx_delivery_keeps_rcv_nxt_and_stages_sack ... ok
test transport::tcp::connection::tests::tcp_receive_payload_in_order_rx_delivery_advances_rcv_nxt ... ok
test session::runtime::tests::enqueue_rx_ooo_delivery_reports_non_zero_span ... ok
test session::runtime::tests::enqueue_rx_in_order_delivery_cannot_carry_ooo_facts ... ok
test session::runtime::tests::enqueue_rx_zero_accepted_bytes_returns_not_accepted ... ok
test duplicate_ooo_enqueue_reports_zero_newly_accepted_bytes ... ok
test result: ok. 146 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Files changed

- `crates/hammer-infra/src/fifo.rs`
- `crates/hammer-infra/tests/fifo_ooo.rs`
- `crates/hammer-service/src/session/app.rs`
- `crates/hammer-service/src/session/runtime.rs`
- `crates/hammer-service/src/transport/tcp/connection.rs`
- `crates/hammer-service/src/transport/tcp/established.rs`
- `crates/hammer-service/src/transport/tcp/listen.rs`
- `crates/hammer-service/src/transport/tcp/mod.rs`
- `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- `crates/hammer-service/tests/tcp_session_app_boundary.rs`

## Self-review findings

- The RX result surface is now transport-neutral and encodes illegal zero-accepted accepted-variants out of the public shape.
- The OOO duplicate case needed a narrow `hammer-infra` fix so the runtime could distinguish exact duplicates from newly accepted bytes.
- TCP receive call sites now make ACK/SACK/window decisions by matching on `RxDelivery`, without restoring FIFO/app notification details to TCP.

## Concerns

- `RxDelivery`’s size guard landed at `<= 24` bytes rather than `<= 16`; that still keeps an explicit hot-path bound, but the enum layout is a little larger than the tightest optimistic target.
- The OOO span start remains the first retained span start after predecessor trimming; accepted-byte accounting is now correct for exact duplicates, but more complex overlap shapes still rely on the existing single-span modeling rather than a richer overlap description.
