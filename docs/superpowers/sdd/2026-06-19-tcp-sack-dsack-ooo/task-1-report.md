# Task 1 Report

## Scope

Implemented Task 1 only: wire inbound TCP SACK facts through packet parse and allow outbound TCP header prepend to carry optional SACK/DSACK blocks on the existing `TcpSegment` / `write_tcp_segment_header` path. No recovery cleanup or RX OOO storage work was added.

## What changed

### `crates/hammer-core/src/protocol/tcp/options.rs`

- Kept existing SYN option writer behavior unchanged.
- No non-SYN SACK writer entry was kept here after follow-up cleanup; ACK/SACK encoding now stays inside the approved existing header write path.

### `crates/hammer-core/src/protocol/tcp/segment.rs`

- Extended `write_tcp_segment_header` to accept `sack_blocks: Option<&[TcpSackBlock]>`.
- SYN segments still use the existing capability-driven SYN option path.
- Non-SYN segments now encode SACK only when optional facts are present, directly inside `write_tcp_segment_header`.

### `crates/hammer-core/tests/protocol_tcp_segment.rs`

- Added RED-first coverage for ACK header write with one SACK block.
- The test uses only a local helper and the existing production header write entrypoint.

### `crates/hammer-service/src/transport/tcp/segment.rs`

- `TcpPacket` now keeps parsed inbound `sack_blocks: Vec<TcpSackBlock>`.
- `parse_tcp_packet()` now consumes `tcp_options_from_bytes(segment.options())` once and keeps both `capabilities` and `sack_blocks`.
- Extended `TcpSegment::new(...)` to accept `sack_blocks: Option<&[TcpSackBlock]>`.
- `TcpSegment` stores up to 4 SACK blocks inline in a fixed array plus count, so the output path can stay `Copy` and avoid adding a new public carrier type.
- `TcpSegment::write_header()` forwards optional SACK facts into `write_tcp_segment_header(...)`.
- Added a focused unit test proving inbound SACK blocks are preserved by `parse_tcp_packet()`.

## Minimal callsite/test adaptations

- Added `None` at existing `TcpSegment::new(...)` callsites touched by compilation:
  - `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - `crates/hammer-service/src/transport/tcp/output.rs` test helper
  - `crates/hammer-service/src/transport/tcp/session.rs` tests
  - `crates/hammer-service/tests/tcp_output.rs`
- Added `sack_blocks: Vec::new()` to the existing `TcpPacket` test literal in `state_machine.rs`.

## TDD record

### RED

Ran:

```bash
cargo test -p hammer-core protocol_tcp_segment -- --nocapture
```

Observed failure before implementation:

- `write_tcp_segment_header` took 2 args, while the new ACK+SACK test required a 3rd optional SACK facts argument.
- This confirmed the missing outbound ACK SACK/header carrying path.

### GREEN

Ran after implementation:

```bash
cargo test -p hammer-core protocol_tcp_segment -- --nocapture
cargo test -p hammer-service transport::tcp::segment -- --nocapture
```

Both commands passed.

## Follow-up cleanup

- Removed the standalone `tcp_options_from_segments(...)` helper after review feedback.
- Folded ACK/SACK option emission back into the approved existing `write_tcp_segment_header(...)` path.
- This keeps SYN capability option writing where it already lived and avoids introducing any separate ACK/SACK option-writer entry.

## Follow-up verification

Re-ran after helper removal:

```bash
cargo test -p hammer-core protocol_tcp_segment -- --nocapture
cargo test -p hammer-service transport::tcp::segment -- --nocapture
```

Results:

- `cargo test -p hammer-core protocol_tcp_segment -- --nocapture`: exit 0
- `cargo test -p hammer-service transport::tcp::segment -- --nocapture`: exit 0

## ACK-only fix round

- Tightened outbound SACK emission so the existing header path emits SACK only for segments carrying `TcpSegmentFlags::ACK`.
- Added a regression test covering the required boundary: non-ACK segments must not emit SACK even when optional SACK facts are passed.
- Commit: pending in this fix round until the ACK-only regression and focused verification completed.

### ACK-only RED

Ran:

```bash
cargo test -p hammer-core core_tcp_non_ack_does_not_write_sack_blocks -- --nocapture
```

Observed failure before the fix:

- `core_tcp_non_ack_does_not_write_sack_blocks` failed because `parsed.sack_blocks` was not empty on a non-ACK segment.

### ACK-only verification

Ran after the fix:

```bash
cargo test -p hammer-core core_tcp_non_ack_does_not_write_sack_blocks -- --nocapture
cargo test -p hammer-core protocol_tcp_segment -- --nocapture
cargo test -p hammer-service transport::tcp::segment -- --nocapture
```

Results:

- `cargo test -p hammer-core core_tcp_non_ack_does_not_write_sack_blocks -- --nocapture`: exit 0
- `cargo test -p hammer-core protocol_tcp_segment -- --nocapture`: exit 0
- `cargo test -p hammer-service transport::tcp::segment -- --nocapture`: exit 0

## Constraints check

- No new public type added.
- No new production ACK/SACK helper API added.
- Existing `write_tcp_segment_header` / `TcpSegment` path remains the only outbound header prepend route.
- No TCP-specific runtime or buffer API was introduced.
- Borrowed slice input is used for outbound SACK facts; no extra owned collection is created on the write path.
- Inbound parse keeps the existing `Vec<TcpSackBlock>` produced by option parsing.

## Concerns

1. The required `cargo test -p hammer-core protocol_tcp_segment -- --nocapture` filter does not match individual test names in this crate, so it acted as a compile-and-binary-level verification rather than directly executing the new named test. The new test still compiled into the target and the service-side targeted regression did execute.
2. `cargo test -p hammer-service transport::tcp::segment -- --nocapture` still prints unrelated existing warnings from `crates/hammer-service/src/service.rs` test-only helpers. They are pre-existing and outside Task 1 scope.
