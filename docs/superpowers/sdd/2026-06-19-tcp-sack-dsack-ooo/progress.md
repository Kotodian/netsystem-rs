# TCP SACK/DSACK/OOO SDD Progress

- Date: 2026-06-19
- Branch: `codex/hammer-app-ring-zero-copy`
- Plan: `docs/superpowers/plans/2026-06-19-tcp-sack-dsack-ooo-plan.md`
- Base commit: `9663be4be857095bec0c2d4979ee9939906024c3`
- Ledger location for this run: `docs/superpowers/sdd/2026-06-19-tcp-sack-dsack-ooo/`
- Note: current sandbox does not allow writing `.git/sdd`, so this run records SDD artifacts in docs.

## Pre-flight

- Merged the former receive/SACK/app-recv split into the current integrated Task 3.
- Cleaned stale plan wording for Task 2 test migration and Task 4 ownership.
- Re-checked current code before execution:
  - `write_tcp_segment_header()` still only writes SYN options.
  - `parse_tcp_packet()` still drops inbound `sack_blocks`.
  - `TcpSentSegment` is still public and re-exported.
  - `crates/hammer-service/tests/tcp_rack_tlp.rs` still constructs `TcpSentSegment` directly.

## Task Status

- Task 1: complete (commits `9663be4b..349ee24f`, review clean after fix round)
- Task 2: ready (same approval gate for the final recovery-facing surface)
- Task 3: pending
- Task 4: pending
