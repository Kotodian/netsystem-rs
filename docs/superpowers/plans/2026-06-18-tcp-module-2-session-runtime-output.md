# TCP Module 2 Session Runtime Output Plan

> Execute with `superpowers:executing-plans` and `rust-coding-skill`.

## Goal

Complete TCP Module 2 TX integration:

- Session runtime schedules TX and materializes app payload bytes into existing dataplane buffers.
- TCP connection decides send budget, updates sequence/recovery/congestion accounting, and arms TCP timers.
- TCP session protocol records one `TcpSegment` intent per TX buffer.
- TCP output consumes `TcpSegment`, prepends the TCP header, writes route metadata, and forwards to lookup.

## Hard Constraints

- No unapproved domain types.
- Approved new domain type: `TcpSegment`.
- Approved new infra type: `hammer_infra::fifo::FifoQueue<T>`.
- `TcpSegment` must be created through its constructor. Do not hand-build its fields outside `segment.rs`.
- Session runtime must not know TCP segment/header concepts.
- TCP output owns TCP header prepend.
- TCP connection/session code must not copy or rebuild packet chains for header insertion.
- Do not add buffer allocator APIs. Use the existing preallocated buffer pool and `DataPlaneBuffers::alloc_index`.
- Do not add `alloc_tcp_segment`, `insert_output_segment`, or “output segment” terminology.
- Do not add congestion-control graph nodes for Module 2.
- Do not scan all timer kinds on expiry. Runtime gives the exact timer token; TCP handles only that kind.
- Do not add builder-style `with_session_queue` for `TcpOutputNode`; its constructor takes the required queue handle.
- If another `struct`, `enum`, or newtype is needed, stop for user approval first.

## Layer Contract

| Layer | Owns | May Call | Must Not Do |
| --- | --- | --- | --- |
| `session/app.rs` | app send queues and app-copy progress | `AppSendData`, `FifoQueue`, `Pool`, `FlatHashTable` | TCP state, CC, packet graph |
| `session/runtime.rs` | polling, timer expiry dispatch, ready sessions, app-to-buffer copy, enqueue | generic `SessionQueueProtocol` TX hooks, existing buffer APIs | TCP header/segment fields |
| `transport/tcp/session.rs` | TCP session lookup and `BufferIndex -> TcpSegment` intent map | TCP connection methods, session timer context | app memory internals, node scheduling policy |
| `transport/tcp/state_machine.rs` | TCP send budget, sequence, recovery, congestion accounting | `CongestionController`, `TcpRecoveryState` | buffer allocation, app ring APIs |
| `transport/tcp/segment.rs` | `TcpSegment`, TCP header bytes, route metadata | `write_tcp_segment_header` | scheduling, app queues, CC |
| `transport/tcp/output.rs` | consume `TcpSegment`, prepend header, route/drop | existing buffer `prepend`, TCP session queue handle | send budget, app commit, CC |

## Final TX Flow

1. `SessionQueueNode` is scheduled by the runtime.
2. `SessionDriverRuntime::poll_app()` drains app submissions.
3. `SessionAppRuntime` records pending sends and marks affected sessions ready.
4. For each ready session, session runtime asks the protocol for `tx_payload_len(session_id, pending_len, now)`.
5. If the answer is `0`, no dataplane buffer is allocated.
6. If the answer is `N > 0`, session runtime allocates one existing dataplane buffer slot with `DataPlaneBuffers::alloc_index(RouteMetadata::default())`.
7. Session runtime copies exactly `N` bytes from app memory into that buffer using existing append/write APIs.
8. Session runtime calls `prepare_tx(session_id, index, N, now)`.
9. TCP session asks the established TCP connection for a `TcpSegment` using the connection method that constructs it through `TcpSegment::new`.
10. TCP session records `index -> TcpSegment`.
11. Session runtime enqueues `index` to `TcpOutputNode`.
12. If enqueue fails, session runtime calls `cancel_tx(index)` and frees the buffer.
13. If enqueue succeeds, session runtime calls `commit_tx(session_id, index, N, now)`.
14. TCP commit advances `snd_nxt`, records the sent range in recovery, calls `CongestionController::on_packet_sent`, and arms any required TCP timers.
15. Session runtime commits app progress for exactly `N` bytes.
16. `TcpOutputNode` later takes `TcpSegment` for `index`, writes the TCP header, prepends it to the same buffer, writes route metadata, and forwards to IP lookup.

## Required Session Protocol Shape

```rust
fn tx_payload_len(
    &mut self,
    context: &mut SessionQueueControlContext<'_, S>,
    session_id: SessionId,
    pending_len: usize,
    now: Instant,
) -> CoreResult<usize>;

fn prepare_tx(
    &mut self,
    context: &mut SessionQueueControlContext<'_, S>,
    session_id: SessionId,
    index: BufferIndex,
    payload_len: usize,
    now: Instant,
) -> CoreResult<()>;

fn cancel_tx(&mut self, index: BufferIndex);

fn commit_tx(
    &mut self,
    context: &mut SessionQueueControlContext<'_, S>,
    session_id: SessionId,
    index: BufferIndex,
    payload_len: usize,
    now: Instant,
) -> CoreResult<()>;
```

Ordering rules:

- `prepare_tx` may register `TcpSegment`; it must not advance `snd_nxt`.
- `cancel_tx` removes any registered `TcpSegment` for `index`.
- `commit_tx` is the only TX hook that mutates TCP send accounting.
- App progress commit happens only after TCP commit succeeds.

## TcpSegment Contract

`TcpSegment` is the only TCP output intent type.

```rust
pub struct TcpSegment {
    local: SocketAddr,
    remote: SocketAddr,
    sequence: u32,
    acknowledgment: u32,
    advertised_window: u16,
    flags: TcpSegmentFlags,
    capabilities: TcpCapabilities,
    payload_len: usize,
}
```

Allowed methods:

```rust
impl TcpSegment {
    pub fn new(
        local: SocketAddr,
        remote: SocketAddr,
        sequence: u32,
        acknowledgment: u32,
        advertised_window: u16,
        flags: TcpSegmentFlags,
        capabilities: TcpCapabilities,
        payload_len: usize,
    ) -> Self;

    pub fn write_header(&self, output: &mut [u8]) -> CoreResult<usize>;
    pub fn route_metadata(&self) -> RouteMetadata;
    pub const fn payload_len(&self) -> usize;
}
```

Do not add field-by-field getters unless a real caller requires them.

## Implementation Tasks

- [x] Add `hammer_infra::fifo::FifoQueue<T>` with FIFO-only API and tests.
- [x] Change session app pending sends to avoid session lookup by linear scan.
- [x] Replace session TX header-writing hooks with `tx_payload_len`, `prepare_tx`, `cancel_tx`, and `commit_tx`.
- [x] Add `transport/tcp/segment.rs::TcpSegment`.
- [x] Make established TCP TX budget use peer window, congestion window, in-flight bytes, pacing delay, and MSS.
- [x] Make established TCP commit update `snd_nxt`, recovery sent ranges, CC `on_packet_sent`, and TCP timers.
- [x] Clean ACK handling so acknowledged segments are removed through recovery ACK processing.
- [x] Wire `TcpOutputNode::new(next, session_queue)` and remove output-node builder-style queue injection.
- [x] Make TCP output take `TcpSegment`, prepend the TCP header, set route metadata, and forward lookup.
- [x] Remove congestion-control packet graph nodes from Module 2 wiring.
- [x] Remove `alloc_tcp_segment` and old output-segment naming.
- [x] Remove timer-kind scans on timer expiry.
- [x] Fix segment registration cleanup: if registering or enqueueing fails, remove intent and free buffer.
- [x] Ensure `TcpSegment` construction goes through `TcpSegment::new`.

## Verification Checklist

Run:

```bash
cargo fmt --all
cargo check -p hammer-service
cargo test -p hammer-infra --test fifo
cargo test -p hammer-service --test tcp_output
cargo test -p hammer-service --test transport_congestion_bbr
cargo test -p hammer-service --test transport_congestion_graph
cargo test -p hammer-service tcp_session --lib
```

Architecture scans:

```bash
rg -n "alloc_tcp_segment|insert_output_segment|output_segment|OutputSegment|control_header|TcpConnectionTimerKind::all" crates/hammer-service/src/transport/tcp crates/hammer-service/src/session
rg -n "copy_current_chain|truncate_chain\\(|append\\(index, header\\)" crates/hammer-service/src/transport/tcp/output.rs crates/hammer-service/tests/tcp_output.rs
rg -n "CongestionControlNode|BbrCongestionNode|CongestionControlNext" crates/hammer-service/src/transport/congestion
rg -n "TcpSegment \\{" crates/hammer-service/src crates/hammer-service/tests
```

Expected:

- The first three scans return no matches.
- The final scan only finds the `TcpSegment` definition in `segment.rs`; all construction call sites use `TcpSegment::new`.
