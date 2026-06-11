# Hammer TCP Congestion Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to execute this plan task-by-task.

**Goal:** Implement Hammer-owned TCP congestion control in the TCP node/output path. Congestion control must affect send eligibility through congestion window and pacing. Reno and Cubic remain rejected until their Hammer TCP node controllers exist.

**Architecture:** The public TCP surface exposes a generic `TcpCongestionControl` trait and a per-connection `TcpCongestionState` wrapper. Concrete algorithm state remains private to the congestion module. Each TCP connection owns its own mutable congestion-control state; the output path and congestion-control node update that state directly from send, ACK, RTT, and loss observations.

**Tech Stack:** Rust 2024, `hammer-service`, existing VPP-style TCP nodes, existing service app-control/retransmit queue tests.

## File Structure

- `crates/hammer-service/src/transport/tcp/congestion.rs`: public congestion trait/sample/state wrapper, private concrete controller, and module-local white-box tests.
- `crates/hammer-service/src/transport/tcp/congestion_control.rs`: data-plane congestion-control node facade for ACK/send/loss observations.
- `crates/hammer-service/src/transport/tcp/connection.rs`: per-connection data-plane TCP state, including retransmit queue, send state, congestion state, and pacing deadline.
- `crates/hammer-core/src/protocol/tcp/mod.rs`: handshake observations carry peer TCP capabilities, including MSS.
- `crates/hammer-service/src/transport/tcp/options.rs`: TCP option parsing for peer capabilities such as MSS.
- `crates/hammer-service/src/transport/tcp/output.rs`: ACK delivery samples, retransmit timestamps, and send-window helpers using `min(peer_window, congestion_window)`.
- `crates/hammer-service/src/transport/tcp/state.rs`: congestion algorithm selection; rejects unsupported algorithms and does not expose smoltcp fallback.
- `crates/hammer-service/src/service.rs`: real runtime TCP output path wiring for congestion window, pacing, ACK delivery, RTT sampling, retransmit loss, and timer wakeups.
- `crates/hammer-service/tests/tcp_congestion.rs`: config/registry tests for Hammer-owned controller selection.
- `crates/hammer-service/tests/tcp_connection_state.rs`: per-connection ownership and trait-backed state tests.
- `crates/hammer-service/tests/tcp_congestion_node.rs`: ACK/send/loss data-plane node facade tests.
- `crates/hammer-service/tests/tcp_output.rs`: output-window and retransmit delivery-sample tests.
- `crates/hammer-service/tests/tcp_syn_sent_adapter.rs`: SYN-ACK TCP option MSS parsing tests.

## Tasks

- [x] Remove smoltcp congestion fallback methods and imports from Hammer TCP congestion selection.
- [x] Reject Reno/Cubic with Hammer config-validation errors until Hammer TCP node implementations exist.
- [x] Add the congestion module with a public trait and generic per-connection state wrapper.
- [x] Keep concrete algorithm state private to the congestion module; cover it with module-local white-box tests.
- [x] Extend retransmit tracking with `sent_at` and ACK delivery samples.
- [x] Add per-connection data-plane TCP state and lookup table support.
- [x] Add the congestion-control node facade for ACK/send/loss observations.
- [x] Gate output using a per-connection send view and congestion window.
- [x] Wire the real service output path to update congestion state on send, ACK, RTT sample, pacing, and retransmit loss.
- [x] Initialize congestion state from a connection-provided max segment size instead of a hard-coded algorithm MSS.
- [x] Parse peer MSS from SYN/SYN-ACK TCP options into handshake observations.
- [x] Refresh each active/passive connection's owned congestion state from peer MSS when handshake capabilities arrive.
- [x] Add pacing timers and cancel them during service shutdown.
- [x] Add service regression tests for congestion-window gating, handshake MSS ownership, ACK-driven delivery updates, pacing enforcement, and retransmit-loss feedback.

## Verification

- [x] `cargo fmt --all`
- [x] `cargo test -p hammer-service bbr_`
- [x] `cargo test -p hammer-service --test tcp_connection_state`
- [x] `cargo test -p hammer-service --test tcp_congestion_node`
- [x] `cargo test -p hammer-service --test tcp_congestion`
- [x] `cargo test -p hammer-service runtime_service_retransmits_unacked_payload_after_timeout`
- [x] `cargo test -p hammer-service --test tcp_connection_state tcp_congestion_state_uses_connection_max_segment_size_for_initial_windows`
- [x] `cargo test -p hammer-service tcp_syn_sent_observation_parses_peer_mss_option`
- [x] `cargo test -p hammer-service runtime_service_active_handshake_mss_sets_owned_congestion_window`
- [x] `cargo test -p hammer-service runtime_service_passive_handshake_mss_sets_owned_congestion_window`
- [x] `cargo test -p hammer-service transport::tcp::options::tests`
- [x] `cargo test -p hammer-service`
- [x] `cargo test --workspace` after max-segment-size fix
- [x] `rg -n "smoltcp_congestion|smoltcp_fallback|SmolTcpCongestion|only Bbr|pub .*TcpBbr|pub .*TCP_BBR|TCP_BBR_MSS|TCP_BBR_INITIAL_WINDOW:|TCP_BBR_MIN_WINDOW:|TCP_BBR_PROBE_RTT_WINDOW|TcpCongestionState::new\\(\\)" crates/hammer-core/src crates/hammer-service/src crates/hammer-service/tests`
- [x] `git status --short`

## Acceptance Criteria

- Each connection owns an independent `TcpCongestionState`.
- `TcpCongestionState` derives initial/min/probe windows from the connection-provided max segment size.
- Active and passive handshakes parse peer MSS from TCP options and use it to initialize the owned connection congestion state.
- TCP node/output public API is named around congestion control, not the concrete first algorithm.
- `TcpConnectionSnapshot` remains a control/observation view, not the main congestion-control data path.
- `TcpCongestionControlNode` updates per-connection delivery, RTT, bandwidth estimate, cwnd, pacing, and loss state.
- `tcp-output` gates payload with `min(peer_advertised_window, congestion_window) - bytes_in_flight`.
- Output pacing deadline is stored per connection and enforced by the real output pump.
- Retransmit timeout feeds loss into the per-connection congestion controller.
- Reno and Cubic return config validation errors and have no smoltcp fallback path.
- `cargo test --workspace` passes before commit.

## Commit

- [ ] Commit with:

```bash
git add docs/superpowers/plans/2026-06-11-hammer-tcp-congestion-control.md crates/hammer-core/src/protocol/tcp/mod.rs crates/hammer-service/src/transport/tcp crates/hammer-service/src/service.rs crates/hammer-service/tests
git commit -m "hammer-service(Feat): implement tcp congestion control"
```
