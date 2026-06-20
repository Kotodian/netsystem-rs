# Task 2 Report

## Status

BLOCKED

## What I read and followed

- `/Users/linqiankai/.cc-switch/skills/test-driven-development/SKILL.md`
- `/Users/linqiankai/.codex/skills/rust-coding-skill/SKILL.md`
- `/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/docs/superpowers/sdd/2026-06-18-tcp-session-rx-tx-storage-ooo/task-2-brief.md`

## Scope reviewed

- `crates/hammer-runtime/src/app/data.rs`
- `crates/hammer-runtime/src/app/ring.rs`
- `crates/hammer-service/src/session/app.rs`
- compile-fallout context only:
  - `crates/hammer-service/src/session/runtime.rs`
  - `crates/hammer-adapter/src/buffer.rs`

## Current state I confirmed

- `SessionAppRuntime::drain_submissions()` currently converts `AppSqeData::Send` into `AppSendData` and stores it in `pending_sends`.
- The queue item is `SessionAppTxProgress { send: AppSendData, sent_len }`.
- The hot path later calls:
  - `pending_send_len(session_id)`
  - `copy_pending_send_bytes(session_id, payload_len) -> Vec<u8>`
  - `commit_pending_send_bytes(session_id, payload_len)`
- The actual data-plane buffer chain is still created later in `flush_one_session_tx()` inside `crates/hammer-service/src/session/runtime.rs`.

This matches the brief's description of the bad path.

## Why I did not implement yet

Task 2 requires both of these to become true at the same time:

1. `drain_submissions()` must immediately copy app-owned send bytes into session-owned TX buffer-chain storage.
2. The copy must still happen exactly once at the app/session boundary.

Given the current Task 3-owned TX path, those two requirements create a real blocker:

- If Task 2 copies app data into a session-owned buffer chain at `drain_submissions()`, then the later TX path must send from that chain.
- But the current TX path in `session/runtime.rs` does not send from an existing session-owned chain. It asks `SessionAppRuntime` for a `Vec<u8>` slice copy, allocates a fresh packet buffer, appends those bytes into that fresh buffer, and then hands that fresh buffer to transport output.
- Replacing only `session/app.rs` with a queue of owned buffer chains is not enough, because partial sends still need a way to expose only the next `[sent_len .. sent_len + payload_len]` window without copying again.

## The concrete missing capability

To satisfy Task 2 without violating the "copy only once" rule, the existing generic buffer foundation still needs one non-trivial generic capability:

- a way to create a sendable view/clone of an existing buffer chain range, or otherwise hand a sub-range of session-owned TX buffer-chain storage to the TX path without re-copying payload bytes.

I checked the currently available generic buffer APIs:

- `attach_clone(head, tail)` clones existing buffers by refcount and chain linkage.
- `with_current_chain_range(index, offset, len, f)` temporarily mutates current-range metadata and then restores it.
- `truncate_chain(index, len)` mutates owned chain length.

Those are not enough for this task by themselves:

- `with_current_chain_range(...)` is temporary and restored after the closure, so it cannot produce a persistent queued/sendable chain view.
- `attach_clone(...)` clones whole chains/buffers, not an arbitrary `[offset, len]` range view.
- Using `with_current_chain_range(...)` plus `attach_clone(...)` would share the same underlying buffer metadata, and the restore step would revert the cloned view as well, because the clone shares buffers instead of snapshotting buffer-header state.

## Why this exceeds the allowed boundary

The user explicitly said:

- do not touch TCP/session runtime hot path files yet except unavoidable compile fallout
- Task 3 owns the TX dispatch rewrite
- if a new public API is needed and it is non-trivial beyond the brief's allowance, stop and ask

At this point, finishing Task 2 correctly requires one of these non-trivial moves:

1. Add a new generic buffer-chain sub-range clone/view API in `hammer-adapter`, then consume it from the later TX path.
2. Pull part of Task 3 forward and rewrite `flush_one_session_tx()` to transmit directly from session-owned TX buffer-chain storage.

Either option crosses the current approved boundary.

## TDD note

I attempted to derive a failing test from `session/app.rs`, but the old behavior can still satisfy a naive `drain_submissions()` test because:

- the app chunk can be released and reallocated independently of whether session-owned TX storage exists, and
- the existing queue still reports pending payload length/bytes successfully through deferred `AppSendData`.

The meaningful failing test needs the yet-missing generic buffer-chain range/send capability or the Task 3 TX rewrite.

## Recommendation

Please approve one of these before implementation continues:

1. Preferred: allow one generic `hammer-adapter` API for persistent buffer-chain sub-range cloning/view construction, specifically for handing session-owned TX byte ranges to later output without another payload copy.
2. Alternatively: allow Task 3 TX dispatch changes now, so Task 2 and Task 3 can be landed together around the new ownership boundary.

## Commands run

- `cargo test -p hammer-service drain_submissions_copies_send_into_session_owned_tx_storage`

That test path did not expose the real blocker described above, so no code changes were kept.

## Files changed

- No functional source changes kept.

## Commits

- None
