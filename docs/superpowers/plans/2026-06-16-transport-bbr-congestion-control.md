# Transport BBR + TCP RACK-TLP Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add BBR as Hammer's first typed transport congestion controller, usable by TCP and future QUIC callers, and wire TCP RACK-TLP recovery into the existing typed TCP connection/session model.

**Architecture:** `transport/congestion` is transport-agnostic: TCP and QUIC feed the same send, ACK, loss, RTT, bytes-in-flight, and app-limited samples into a concrete `C: CongestionController`. `TcpConnection<S, C>` owns that concrete controller and never branches on which algorithm is stored. TCP RACK-TLP stays under `transport/tcp`, stores TCP sent-record state with `hammer_infra::vec::Vec`, maps TCP delivery events into shared congestion samples, and registers every TCP wakeup timer through the session tick timer wheel.

**Tech Stack:** Rust 2024, `hammer-service`, current typed `TcpConnection<S>` design upgraded to `TcpConnection<S, C>`, `hammer_infra::vec::Vec`, existing session timer wheel, Quinn BBR reference code in `third_party/quinn/quinn-proto/src/congestion/bbr/`, RFC 8985 RACK-TLP, and Linux TCP recovery/output sources (`net/ipv4/tcp_recovery.c`, `net/ipv4/tcp_output.c`).

## Execution Note

Current implementation status after review:

- Implemented `transport/congestion` shared sample/controller types, `BbrController`, and `BbrCongestionNode`.
- Implemented `TcpConnection<S, C>` with no default `C`; service composition selects `BbrController` only at the TCP session boundary.
- Implemented TCP timer bitflags and session timer-token mapping for retransmit, RACK, TLP, pacing, delayed ACK, persist, keepalive, and TIME-WAIT.
- Implemented `transport/tcp/recovery.rs` with `hammer_infra::vec::Vec`, including infra `Vec::remove(index)` support so recovery does not rebuild vectors to delete ACKed/lost records.
- Wired TCP state-node segment emission to the transport congestion next.
- Removed the old TCP congestion registry/config algorithm carrier (`TcpCongestionAlgorithm`, `TcpCongestionRegistry`, `TcpConnectionConfigState`) because congestion algorithms are selected by graph sibling/controller type, not by a TCP enum.
- Removed temporary TCP recovery/session methods that were only exercised by tests (`record_sent_segment`, `apply_ack_recovery`, and their connection helpers). TCP currently has no production app-send-to-segment path, so leaving those methods would falsely imply RACK/TLP recovery is wired into real TCP output.

Task 5 Steps 1-4 are therefore intentionally not claimed as complete until a real TCP send/output production path exists to call recovery record/ACK/TLP events.

---

## Current Code Context

- `crates/hammer-service/src/transport/congestion/mod.rs` currently stores one paced controller behind an internal carrier and exposes `CongestionAlgorithm::Hammer`.
- `crates/hammer-service/src/transport/congestion/` must be designed as shared transport code. TCP is the first caller in this plan; QUIC must be able to use the same `CongestionController` trait and `BbrController` without importing TCP recovery or TCP session code.
- `crates/hammer-service/src/transport/tcp/state_machine.rs` currently defines `TcpConnection<S>` and stores `congestion: TcpCongestionState`; this plan replaces that with explicit `TcpConnection<S, C>`.
- `crates/hammer-service/src/transport/tcp/congestion_control.rs` currently adapts TCP ACK/send/loss observations into connection congestion methods.
- `crates/hammer-service/src/transport/tcp/session.rs` already registers retransmit timers through `SessionTimerToken` and `SessionDriverRuntime::arm_timer_ticks`.
- `crates/hammer-service/src/transport/tcp/connection.rs` currently has only retransmit timer support.
- TCP RACK-TLP needs TCP sent-record state and session-registered timers. It must not be modeled as a congestion algorithm.
- Current `TcpConnection::next_output_at` is a connection-private pacing timer and must be removed.

Reference points:

- RFC 8985, "The RACK-TLP Loss Detection Algorithm for TCP": `https://www.rfc-editor.org/rfc/rfc8985.html`
- Linux RACK recovery source: `https://github.com/torvalds/linux/blob/master/net/ipv4/tcp_recovery.c`
- Linux TLP/output source: `https://github.com/torvalds/linux/blob/master/net/ipv4/tcp_output.c`

## Non-Negotiable Constraints

- Do not create a new worktree.
- Do not add algorithm dispatch inside `TcpConnection`.
- Do not add a carrier enum for congestion algorithms.
- Do not add a default congestion type parameter such as `TcpConnection<S, C = BbrController>`.
- Do not add `TcpCongestionState = BbrController` or any TCP alias that makes BBR the hidden TCP default.
- Do not add TCP, QUIC runtime, session, app, or node imports under `crates/hammer-service/src/transport/congestion/`.
- Do not make shared congestion APIs depend on TCP sequence numbers, TCP SACK blocks, TCP timers, QUIC packet structs, QUIC stream IDs, or any transport-specific session queue.
- Do not use standard-library vector storage in TCP recovery or congestion hot-path sample buffers; use `hammer_infra::vec::Vec`.
- Do not keep TCP wakeup work as a private connection deadline. Retransmit, RACK, TLP, pacing, delayed ACK, persist, keepalive, and TIME-WAIT timers must all have `TcpConnectionTimerKind` plus `SessionTimerToken` entries and must be armed through the session timer wheel.
- Do not add node-side branching on TCP protocol state for congestion or recovery.
- Do not add queue APIs that let packet nodes drive storage policy.
- Do not add `queue.put_connection(...)`; typed connection event methods update queue storage, indexes, app completions, closes, and timer state from inside `session.rs`.
- Do not let packet nodes use turbofish state extraction such as `queue.take_connection::<Established>(...)`; the node's local left-hand type selects the TCP state and the graph-selected congestion controller, for example `let connection: TcpConnection<Established, C> = ...`.
- Do not duplicate TCP packet node files per congestion algorithm. A graph instantiation may carry one `C: CongestionController`, but TCP nodes must not branch on `C`, ask for algorithm identity, or create BBR/Cubic/Reno-specific TCP node implementations.
- Do not let any node ask `TcpConnection` which congestion algorithm it carries.
- Do not add mutable escape hatches from `TcpConnection` that allow an adapter to mutate controller internals directly.
- Do not put BBR mode/cwnd/pacing policy in `transport/tcp/recovery.rs`.
- Do not put TCP sequence ranges, SACK blocks, retransmit state, or TLP state in `transport/congestion`.

## Target File Structure

- Create `crates/hammer-service/src/transport/congestion/types.rs`
  - Protocol-agnostic sample and metrics types: `PacketNumber`, `AckedPacket`, `LostPacket`, `RttSample`, `CongestionMetrics`.
  - No TCP, QUIC runtime, session, app, or node imports.

- Create `crates/hammer-service/src/transport/congestion/controller.rs`
  - Defines `CongestionController`.
  - The trait is the only interface `TcpConnection<S, C>` needs.

- Create `crates/hammer-service/src/transport/congestion/node.rs`
  - Defines the transport-level congestion graph node family root: `CongestionControlNode` and `CongestionControlNext`.
  - Does not import TCP, QUIC runtime, session, app, or transport-specific packet types.
  - Does not define TCP packet nodes.
  - Algorithm nodes are concrete siblings under `transport/congestion`, not aliases or variants inside TCP nodes.

- Create `crates/hammer-service/src/transport/congestion/bbr.rs`
  - Defines `BbrController`.
  - Defines concrete sibling node `BbrCongestionNode`.
  - Implements `CongestionController`.
  - Implements Hammer `Node` for `BbrCongestionNode` as `sibling_of = CongestionControlNode`.
  - Owns BBR mode, bandwidth sampling, min RTT, cwnd, pacing rate, ProbeRTT, and loss response.

- Future Reno/Cubic work must add sibling controller and congestion-node files beside BBR under `transport/congestion`, for example `CubicCongestionNode` and `RenoCongestionNode`. Do not add variants inside `TcpConnection`, do not add a controller carrier enum, and do not duplicate TCP packet nodes per algorithm. This plan does not create fake Reno/Cubic modules.

- Modify `crates/hammer-service/src/transport/congestion/mod.rs`
  - Re-export shared types, `CongestionController`, `CongestionControlNode`, `CongestionControlNext`, `BbrController`, and `BbrCongestionNode`.

- Modify `crates/hammer-service/src/transport/tcp/congestion.rs`
  - Re-export shared congestion sample/controller types for TCP callers only when existing TCP imports need a compatibility path.
  - Do not define `TcpCongestionState = BbrController`.
  - Do not make BBR the default TCP controller type.
  - Do not move BBR logic into TCP; this file is only the TCP-facing import boundary for shared congestion.

- Modify `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - Change `TcpConnection<S>` to `TcpConnection<S, C>`.
  - Do not give `C` a default type parameter.
  - Store `congestion: C`.
  - Add TCP-owned `recovery: TcpRecoveryState`.
  - Remove `next_output_at`; pacing is a session timer, not a connection-private deadline.
  - Keep state transitions typed; connection methods remain generic over `C: CongestionController`.

- Modify `crates/hammer-service/src/transport/tcp/connection.rs`
  - Extend `TcpConnectionTimerKind` with all TCP wakeup timers: `Retransmit`, `Rack`, `Tlp`, `Pacing`, `DelayedAck`, `Persist`, `KeepAlive`, and `TimeWait`.
  - Keep timer bit state on the connection, but actual scheduling must go through `TcpSessionProtocol` and `SessionDriverRuntime`.

- Create `crates/hammer-service/src/transport/tcp/recovery.rs`
  - Owns TCP sent records, cumulative ACK/SACK delivery, RACK loss marking, and TLP probe selection.
  - Uses `hammer_infra::vec::Vec`.
  - Feeds `AckedPacket`, `LostPacket`, and `RttSample` into a generic `C: CongestionController`.
  - Does not import session queue, app rings, packet nodes, or BBR internals.

- Modify `crates/hammer-service/src/transport/tcp/session.rs`
  - Add session timer tokens for every `TcpConnectionTimerKind`.
  - Add typed session timer registration helpers used by typed connection event methods.
  - Expired TCP timers must enter the existing session ready path through session timer expiry, not private connection deadlines.

- Modify `crates/hammer-service/src/transport/tcp/congestion_control.rs`
  - Stop treating this file as the congestion-control node owner.
  - Keep only TCP-specific observation conversion if existing callers still need it, or remove it after callers use `transport/congestion` observations directly.
  - It must not define BBR/Cubic/Reno nodes.

- Create `crates/hammer-service/tests/transport_congestion_bbr.rs`
  - Shared BBR controller tests.

- Create `crates/hammer-service/tests/tcp_rack_tlp.rs`
  - TCP recovery tests with a recording test controller.

- Modify `crates/hammer-service/tests/transport_congestion_bbr.rs`
  - Verify the BBR transport congestion node is a graph sibling and does not expose direct observation/controller APIs.

- Modify `crates/hammer-service/tests/tcp_connection_state.rs`
  - Verify `TcpConnection<S, C>` construction is selected by the left-hand type and typed state behavior remains intact.

## Target Shared Congestion API

`crates/hammer-service/src/transport/congestion/types.rs`:

```rust
use std::time::{Duration, Instant};

pub type PacketNumber = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AckedPacket {
    pub packet_number: PacketNumber,
    pub bytes: u32,
    pub sent_at: Instant,
    pub app_limited: bool,
    pub ecn_ce: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LostPacket {
    pub packet_number: PacketNumber,
    pub bytes: u32,
    pub sent_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RttSample {
    pub latest: Duration,
    pub min: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CongestionMetrics {
    pub congestion_window: u32,
    pub pacing_rate_bytes_per_second: Option<u64>,
    pub delivered: u64,
    pub max_bandwidth_bytes_per_second: u64,
    pub min_rtt: Option<Duration>,
}
```

`crates/hammer-service/src/transport/congestion/controller.rs`:

```rust
use std::time::{Duration, Instant};

use super::types::{AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample};

pub trait CongestionController: Clone + core::fmt::Debug {
    fn new(max_datagram_size: u32) -> Self
    where
        Self: Sized;

    fn metrics(&self) -> CongestionMetrics;
    fn max_datagram_size(&self) -> u32;
    fn congestion_window(&self) -> u32;
    fn pacing_rate_bytes_per_second(&self) -> Option<u64>;
    fn delivered(&self) -> u64;
    fn min_rtt(&self) -> Option<Duration>;
    fn max_bandwidth_bytes_per_second(&self) -> u64;

    fn on_packet_sent(
        &mut self,
        packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: Instant,
    );

    fn on_ack(
        &mut self,
        now: Instant,
        acked: AckedPacket,
        rtt: RttSample,
        bytes_in_flight: u32,
    );

    fn on_end_acks(
        &mut self,
        now: Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        largest_acked_packet: PacketNumber,
    );

    fn on_loss(&mut self, now: Instant, lost: LostPacket, persistent_congestion: bool);
    fn on_mtu_update(&mut self, max_datagram_size: u32);
    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration>;
}
```

`BbrController` may expose BBR-specific read-only methods:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BbrMode {
    Startup,
    Drain,
    ProbeBw,
    ProbeRtt,
}

impl BbrController {
    pub fn mode(&self) -> BbrMode {
        self.mode
    }
}
```

No generic `CongestionController` method should expose BBR-specific state.

TCP and QUIC must both fit this API:

- TCP RACK-TLP maps sequence-space delivery into `PacketNumber`, `AckedPacket`, `LostPacket`, and `RttSample` before calling `C`.
- QUIC maps packet-number-space ACK ranges, ECN validation, and loss detection into the same shared sample types before calling `C`.
- Shared BBR code must not know whether the caller is TCP or QUIC.
- TCP-specific RACK/TLP and QUIC-specific loss recovery stay in their own transport modules.

## Target TCP Generic Connection Shape

`crates/hammer-service/src/transport/tcp/state_machine.rs`:

```rust
use crate::transport::congestion::CongestionController;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Listen;

#[derive(Debug, Clone)]
pub struct TcpConnection<S, C>
where
    C: CongestionController,
{
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    close_reason: Option<TcpCloseReason>,
    iss: u32,
    irs: u32,
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    rcv_nxt: u32,
    rcv_wnd: u32,
    options: TcpConnectionOptionState,
    retransmit_timeout: TcpRetransmitTimeoutState,
    congestion: C,
    recovery: TcpRecoveryState,
    active_timers: TcpConnectionTimerKind,
    pending_timers: TcpConnectionTimerKind,
    state: S,
}
```

Construction is state-specific and always uses the same method name, `new`. The left-hand type chooses both the TCP state and congestion controller:

```rust
impl<C> TcpConnection<Closed, C>
where
    C: CongestionController,
{
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            connection_id,
            owner_worker,
            local_port,
            local,
            remote,
            close_reason: None,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: DEFAULT_TCP_WINDOW,
            rcv_nxt: 0,
            rcv_wnd: DEFAULT_TCP_WINDOW,
            options: TcpConnectionOptionState::default(),
            retransmit_timeout: TcpRetransmitTimeoutState::new(),
            congestion: C::new(DEFAULT_TCP_MAX_SEGMENT_SIZE),
            recovery: TcpRecoveryState::new(),
            active_timers: TcpConnectionTimerKind::empty(),
            pending_timers: TcpConnectionTimerKind::empty(),
            state: Closed,
        }
    }
}
```

Listen has the same constructor name on its own concrete state:

```rust
impl<C> TcpConnection<Listen, C>
where
    C: CongestionController,
{
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            connection_id,
            owner_worker,
            local_port,
            local,
            remote,
            close_reason: None,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: DEFAULT_TCP_WINDOW,
            rcv_nxt: 0,
            rcv_wnd: DEFAULT_TCP_WINDOW,
            options: TcpConnectionOptionState::default(),
            retransmit_timeout: TcpRetransmitTimeoutState::new(),
            congestion: C::new(DEFAULT_TCP_MAX_SEGMENT_SIZE),
            recovery: TcpRecoveryState::new(),
            active_timers: TcpConnectionTimerKind::empty(),
            pending_timers: TcpConnectionTimerKind::empty(),
            state: Listen,
        }
    }
}
```

BBR construction must name the controller in the left-hand type:

```rust
let connection: TcpConnection<Closed, BbrController> = TcpConnection::new(
    None,
    owner_worker,
    local_port,
    Some(local),
    remote,
);
```

Sibling algorithms use the same constructor with a different left-hand type. There is no `with_controller`, no constructor parameter for the algorithm, and no default `C = BbrController`:

```rust
let connection: TcpConnection<Closed, TestController> = TcpConnection::new(
    None,
    owner_worker,
    local_port,
    Some(local),
    remote,
);
```

TCP packet nodes keep the existing state-node shape and use left-hand typed connections for both TCP state and the graph-selected congestion controller:

```rust
let connection: TcpConnection<Established, BbrController> = queue.take_connection(session_id)?;
let header = connection.receive_established_packet(runtime, index, queue, session_id, &packet)?;
```

Transport congestion sibling nodes are Hammer graph nodes. They are selected by graph registration and executed only through `Node::process` / `node_process`, not by constructing a node and calling an observation method by hand:

```rust
#[hammer_component_macros::node(role = internal, sibling_of = CongestionControlNode)]
pub struct BbrCongestionNode {}
```

Do not add TCP node code that asks the connection which congestion algorithm it carries.

## Target Node Design

This feature keeps the existing TCP packet graph shape. TCP packet nodes are TCP state nodes only: `TcpListenNode`, `TcpSynSentNode`, `TcpEstablishedNode`, and the close-state nodes are not duplicated for BBR, Cubic, Reno, or any other congestion algorithm. The node does packet-graph work only; the typed connection event owns TCP protocol decisions, queue storage updates, index updates, app completions, session close/removal, and timer refresh.

The CC node family is a transport-layer family under `transport/congestion`. Every congestion-control algorithm is a sibling in that family: `BbrCongestionNode`, future `CubicCongestionNode`, future `RenoCongestionNode`, and any later algorithm node share the same graph-node family root, but each algorithm has its own concrete node type. Do not hide those siblings behind a TCP node, a `TcpConnection` branch, or a public generic node carrier.

### Node Files

TCP packet node files are state-owned. The file name fixes the TCP state; congestion algorithm selection is not part of these node names or their graph registration. Every TCP state node that emits a TCP segment sends it to its `Congestion` next, not directly to `TcpOutputNode`.

```text
crates/hammer-service/src/transport/tcp/listen.rs
  node: TcpListenNode
  next: TcpListenNext::{Congestion, Drop}
  connection binding: TcpConnection<Listen, C>
  event: receive_syn(queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/syn_sent.rs
  node: TcpSynSentNode
  next: TcpSynSentNext::{Congestion, Drop}
  connection binding: TcpConnection<SynSent, C>
  event: receive_open_reply(queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/syn_rcvd.rs
  node: TcpSynRcvdNode
  next: TcpSynRcvdNext::{Congestion, Drop}
  connection binding: TcpConnection<SynRcvd, C>
  event: receive_final_ack(queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/established.rs
  node: TcpEstablishedNode
  next: TcpEstablishedNext::{Congestion, Drop}
  connection binding: TcpConnection<Established, C>
  event: receive_established_packet(runtime, index, queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/close_wait.rs
  node: TcpCloseWaitNode
  next: TcpCloseWaitNext::{Congestion, Drop}
  connection binding: TcpConnection<CloseWait, C>
  event: receive_close_wait(queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/fin_wait1.rs
  node: TcpFinWait1Node
  next: TcpFinWait1Next::{Congestion, Drop}
  connection binding: TcpConnection<FinWait1, C>
  event: receive_fin_wait1(queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/fin_wait2.rs
  node: TcpFinWait2Node
  next: TcpFinWait2Next::{Congestion, Drop}
  connection binding: TcpConnection<FinWait2, C>
  event: receive_fin_wait2(queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/closing.rs
  node: TcpClosingNode
  next: TcpClosingNext::{Congestion, Drop}
  connection binding: TcpConnection<Closing, C>
  event: receive_closing(queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/last_ack.rs
  node: TcpLastAckNode
  next: TcpLastAckNext::{Congestion, Drop}
  connection binding: TcpConnection<LastAck, C>
  event: receive_last_ack(queue, session_id, &packet)

crates/hammer-service/src/transport/tcp/time_wait.rs
  node: TcpTimeWaitNode
  next: TcpTimeWaitNext::{Congestion, Drop}
  connection binding: TcpConnection<TimeWait, C>
  event: receive_time_wait(queue, session_id, &packet)
```

Do not add a generic receive node that handles several TCP states. The node file name already fixes the state it owns. The `C` in these bindings is the service graph's selected controller type for that graph instance; it is not inspected at runtime and it is not encoded in the TCP node name.

### Packet Node Process Template

Each packet node keeps the existing `Node` and `InternalNode` registration shape. The node stores only graph next ids and an optional `SessionQueueHandle`:

```rust
pub struct TcpEstablishedNode {
    next: [NodeId; TcpEstablishedNext::COUNT],
    session_queue: Option<SessionQueueHandle>,
}
```

`process` obtains the configured queue, parses the packet, resolves the session id from the existing tuple index, takes the exact typed connection through the left-hand binding, calls the state-specific event, then allocates only the returned control header:

```rust
fn tcp_established_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: SessionQueueHandle,
    congestion: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: TcpCongestionController,
{
    let packet = parse_tcp_packet(runtime, index)?;
    let mut output_index = None;
    let input_consumed = packet.payload_len != 0;

    let result = TcpSessionProtocol::<C>::with_queue(
        session_queue,
        |queue: &mut TcpSessionQueue<C>| {
            let (session_id, _, _) = queue
                .session_route_by_tuple(packet.local, packet.remote)
                .ok_or_else(|| CoreError::internal("tcp established session is missing"))?;
            let connection: TcpConnection<Established, C> = queue.take_connection(session_id)?;
            let control = connection.receive_established_packet(
                runtime,
                index,
                queue,
                session_id,
                &packet,
            )?;
            if let Some(header) = control {
                let allocated = alloc_tcp_segment(
                    runtime.packet_buffers(),
                    tcp_segment_metadata(packet.local, packet.remote),
                    header,
                )?;
                output_index = Some(allocated);
            }
            Ok(())
        },
    );

    if let Err(error) = result {
        if let Some(output_index) = output_index.take() {
            runtime.free_index(output_index);
        }
        return Err(error);
    }
    if let Some(output_index) = output_index.take()
        && let Err(error) = next_frames.enqueue(runtime, congestion, output_index)
    {
        runtime.free_index(output_index);
        return Err(error);
    }
    if !input_consumed {
        runtime.free_index(index);
    }
    Ok(())
}
```

TCP graph wiring must connect those state-node `Congestion` next slots to the selected transport congestion sibling node:

```text
TcpListenNext::Congestion      -> BbrCongestionNode
TcpSynSentNext::Congestion     -> BbrCongestionNode
TcpSynRcvdNext::Congestion     -> BbrCongestionNode
TcpEstablishedNext::Congestion -> BbrCongestionNode
TcpCloseWaitNext::Congestion   -> BbrCongestionNode
TcpFinWait1Next::Congestion    -> BbrCongestionNode
TcpFinWait2Next::Congestion    -> BbrCongestionNode
TcpClosingNext::Congestion     -> BbrCongestionNode
TcpLastAckNext::Congestion     -> BbrCongestionNode
TcpTimeWaitNext::Congestion    -> BbrCongestionNode

CongestionControlNext::Transmit -> graph-selected transmit continuation
CongestionControlNext::Defer    -> transport/session ready path, when the caller provides a hold/requeue path
CongestionControlNext::Drop     -> drop
```

The CC node family does not name TCP output nodes. A TCP service graph may connect the selected CC sibling's `Transmit` slot to TCP's output continuation, while a future QUIC graph may connect the same `Transmit` slot to its own output continuation. That binding belongs to graph assembly, not to `transport/congestion`.

The node never branches on TCP flags to choose a protocol transition. `receive_established_packet` consumes `TcpConnection<Established, C>`, applies ACK/RACK/TLP/congestion updates, applies TCP state transitions, updates `queue.driver` and `queue.protocol`, refreshes timers, and returns only `Option<TcpSegmentHeader>`.

Handshake nodes use the same template with their own route lookup and left-hand type:

```rust
let (session_id, _, _) = queue
    .pending_route_by_tuple(packet.local, packet.remote)
    .ok_or_else(|| CoreError::internal("tcp syn-sent session is missing"))?;
let connection: TcpConnection<SynSent, C> = queue.take_connection(session_id)?;
let control = connection.receive_open_reply(queue, session_id, &packet)?;
```

Passive-open nodes use `session_route_by_tuple` for listener/session lookup and call the listener event:

```rust
let connection: TcpConnection<Listen, C> = queue.take_connection(session_id)?;
let control = connection.receive_syn(queue, session_id, &packet)?;
```

Close-state nodes use the established-session lookup and their own fixed state:

```rust
let connection: TcpConnection<FinWait1, C> = queue.take_connection(session_id)?;
let control = connection.receive_fin_wait1(queue, session_id, &packet)?;
```

### Input Routing Node

`TcpInputNode` does not read `TcpConnectionState<C>` and does not map `TcpState` to graph nodes. It uses `TcpSessionConnectionIndex` metadata:

```rust
let route = queue
    .session_route_by_tuple(local, remote)
    .or_else(|| queue.pending_route_by_tuple(local, remote));
```

The index entry already carries `TcpInputNext`, originally produced from `connection.next_node()` inside the typed connection event. `TcpInputNode` routes the packet to that stored graph next. It does not recompute next from TCP flags, TCP state, or congestion algorithm.

### Transport Congestion Sibling Node

The CC node family lives under `crates/hammer-service/src/transport/congestion/`, not under TCP. A CC node is a Hammer packet graph node: it is registered in the graph and executed only through `Node::process` / `node_process`. Do not expose `observe_ack`, `observe_send`, `controller`, or `controller_mut` methods on a node; those are controller methods, not graph-node APIs.

`crates/hammer-service/src/transport/congestion/node.rs` defines the transport-level graph node family root. The root owns the next-node layout used by all sibling algorithms:

```rust
use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult, NodeRuntimeData,
    NodeVectorDispatch,
};
use hammer_core::error::CoreResult;

#[hammer_component_macros::node_next]
pub enum CongestionControlNext {
    Transmit,
    Defer,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = CongestionControlNext)]
pub struct CongestionControlNode {}

impl Node for CongestionControlNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        congestion_control_process(runtime, NodeRuntimeData::default(), frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        congestion_control_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn congestion_control_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = CongestionControlNode::runtime_nexts(runtime)?;
    congestion_control_frame(runtime, frame, next)
}

pub(crate) fn congestion_control_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    next: [NodeId; CongestionControlNext::COUNT],
) -> CoreResult<NodeResult> {
    let transmit = next[CongestionControlNext::Transmit as usize];
    let drop = next[CongestionControlNext::Drop as usize];
    NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        let metadata = runtime.metadata(index)?;
        if metadata.source.is_none() || metadata.destination.is_none() {
            return Ok(Some(drop));
        }
        Ok(Some(transmit))
    })
}
```

The root node is a family anchor and conservative pass-through for frames that already have routeable metadata. Algorithm siblings are the real selectable CC nodes.

Next-node meaning:

```text
CongestionControlNext::Transmit -> packet is eligible for the graph-selected transmit continuation.
CongestionControlNext::Defer    -> packet is held by the owning transport/session until a pacing or recovery timer makes it ready again.
CongestionControlNext::Drop     -> packet is malformed for the congestion gate or has no owning transport context.
```

`Defer` is a real graph edge, but this 3.4 plan does not add a generic transport-private packet holding queue. TCP uses session-registered pacing/RACK/TLP timers to re-enter ready dispatch. If a later QUIC/TCP output graph wants to put packets through a CC sibling node, that graph must provide the transport-owned hold/requeue mechanism before selecting `Defer`.

`crates/hammer-service/src/transport/congestion/bbr.rs` defines BBR as a graph sibling of the transport CC family root. It is not an alias around a public generic node and it is not called directly by TCP packet nodes. Because `sibling_of = CongestionControlNode` shares the root's next layout, `BbrCongestionNode::runtime_nexts(runtime)?` returns the same `Transmit`, `Defer`, and `Drop` slots:

```rust
use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult, NodeRuntimeData,
    NodeVectorDispatch,
};
use hammer_core::error::CoreResult;

use super::node::{CongestionControlNext, CongestionControlNode};

#[hammer_component_macros::node(role = internal, sibling_of = CongestionControlNode)]
pub struct BbrCongestionNode {}

impl Node for BbrCongestionNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        bbr_congestion_frame(runtime, frame, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        bbr_congestion_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn bbr_congestion_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = BbrCongestionNode::runtime_nexts(runtime)?;
    bbr_congestion_frame(runtime, frame, next)
}

fn bbr_congestion_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    next: [NodeId; CongestionControlNext::COUNT],
) -> CoreResult<NodeResult> {
    let transmit = next[CongestionControlNext::Transmit as usize];
    let drop = next[CongestionControlNext::Drop as usize];
    NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        let metadata = runtime.metadata(index)?;
        if metadata.source.is_none() || metadata.destination.is_none() {
            return Ok(Some(drop));
        }
        Ok(Some(transmit))
    })
}
```

The BBR skeleton above preserves the shared next layout and routes only valid metadata to `Transmit`. This plan does not fake a BBR graph-input context. Real BBR ACK/send/loss updates happen through `BbrController` owned by typed TCP/QUIC connection state; a later graph insertion can select `Defer` only after it has a transport-owned hold/requeue path.

Future sibling algorithms add their own graph-node siblings beside BBR: `CubicCongestionNode`, `RenoCongestionNode`, and so on. Shared ACK/send/loss behavior stays on `CongestionController` implementations. TCP-specific RACK-TLP remains in `transport/tcp/recovery.rs` and calls `self.congestion.on_ack(...)`, `on_packet_sent(...)`, and `on_loss(...)` directly through `C: CongestionController` while it owns the typed connection. The congestion node is never a TCP node and never imports `TcpConnection`, `TcpConnectionState`, `TcpSessionQueue`, `SessionId`, TCP sequence types, or TCP SACK blocks.

### Timer Node Path

TCP wakeup timers enter through the existing session queue timer path, not packet nodes. Expiry does two things only:

1. Convert the `SessionTimerToken` to `TcpConnectionTimerKind`.
2. Mark that TCP timer pending on the stored connection and mark the session ready.

The ready dispatch then calls a TCP-owned event on the erased storage boundary:

```rust
impl<C> TcpConnectionState<C>
where
    C: TcpCongestionController,
{
    pub(crate) fn on_tcp_timer_expiry(
        self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
        now: Instant,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        // TCP-owned erased boundary dispatches to typed events.
        // Packet nodes do not match this enum.
    }
}
```

The erased TCP boundary passes the timer event to the typed connection selected by the stored variant. The timer-ready path calls this boundary method directly and does not use packet-node state casts:

```rust
state.on_tcp_timer_expiry(queue, session_id, kind, now)
```

`on_tcp_timer` is a typed TCP event method. Packet nodes and congestion sibling nodes do not match protocol state, timer kind, or congestion algorithm.

### Node Coverage

Packet node to typed connection method:

```text
TcpListenNode      -> TcpConnection<Listen, C>::receive_syn
TcpSynSentNode     -> TcpConnection<SynSent, C>::receive_open_reply
TcpSynRcvdNode     -> TcpConnection<SynRcvd, C>::receive_final_ack
TcpEstablishedNode -> TcpConnection<Established, C>::receive_established_packet
TcpCloseWaitNode   -> TcpConnection<CloseWait, C>::receive_close_wait
TcpFinWait1Node    -> TcpConnection<FinWait1, C>::receive_fin_wait1
TcpFinWait2Node    -> TcpConnection<FinWait2, C>::receive_fin_wait2
TcpClosingNode     -> TcpConnection<Closing, C>::receive_closing
TcpLastAckNode     -> TcpConnection<LastAck, C>::receive_last_ack
TcpTimeWaitNode    -> TcpConnection<TimeWait, C>::receive_time_wait
BbrCongestionNode  -> CongestionControlNext::{Transmit, Defer, Drop}
Future CC sibling  -> CongestionControlNext::{Transmit, Defer, Drop}
Session timer ready -> TcpConnectionState<C>::on_tcp_timer_expiry -> typed TCP timer event
```

## Target TCP Timer Registration

### Timer Audit

TCP has two kinds of time-related state:

- Estimators: `TcpRetransmitTimeoutState::{srtt,rttvar,rto}` and congestion-controller RTT filters are data used to compute future timeouts. They are not runnable timers and stay in their owning state structs.
- Wakeup timers: any state that means "run TCP logic later" must go through the session tick timer wheel. This includes retransmit, RACK, TLP, pacing, delayed ACK, persist, keepalive, and TIME-WAIT.

This plan removes `TcpConnection::next_output_at` and forbids adding any replacement connection-private `Instant` deadline for TCP wakeups.

`crates/hammer-service/src/transport/tcp/connection.rs`:

```rust
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TcpConnectionTimerKind: u16 {
        const RETRANSMIT = 1 << 0;
        const RACK = 1 << 1;
        const TLP = 1 << 2;
        const PACING = 1 << 3;
        const DELAYED_ACK = 1 << 4;
        const PERSIST = 1 << 5;
        const KEEP_ALIVE = 1 << 6;
        const TIME_WAIT = 1 << 7;
    }
}
```

`TcpConnectionTimerKind` is both the single timer identifier and the active/pending bit set. Callers pass one constant at a time; the connection stores combinations.

```rust
impl<S, C> TcpConnection<S, C>
where
    C: CongestionController,
{
    pub fn tcp_timer_is_active(&self, kind: TcpConnectionTimerKind) -> bool {
        self.active_timers.contains(kind)
    }

    pub fn tcp_timer_is_pending(&self, kind: TcpConnectionTimerKind) -> bool {
        self.pending_timers.contains(kind)
    }

    pub fn tcp_timer_set(&mut self, kind: TcpConnectionTimerKind) {
        self.active_timers.insert(kind);
    }

    pub fn tcp_timer_reset(&mut self, kind: TcpConnectionTimerKind) {
        self.active_timers.remove(kind);
        self.pending_timers.remove(kind);
    }

    pub fn tcp_timer_expire(&mut self, kind: TcpConnectionTimerKind) {
        self.active_timers.remove(kind);
        self.pending_timers.insert(kind);
    }
}
```

`crates/hammer-service/src/transport/tcp/session.rs`:

```rust
impl<C> TcpSessionProtocol<C>
where
    C: TcpCongestionController,
{
    #[inline]
    pub fn timer_token(kind: TcpConnectionTimerKind) -> Option<SessionTimerToken> {
        let bits = kind.bits();
        if bits == 0 || bits.count_ones() != 1 {
            return None;
        }
        Some(SessionTimerToken::new(bits.trailing_zeros() + 1))
    }

    #[inline]
    pub fn timer_kind(token: SessionTimerToken) -> Option<TcpConnectionTimerKind> {
        let ordinal = token.get();
        if ordinal == 0 || ordinal > u16::BITS {
            return None;
        }
        TcpConnectionTimerKind::from_bits(1u16 << (ordinal - 1))
    }

    #[inline]
    pub fn arm_tcp_timer_ticks(
        context: &mut SessionProtocolContext<'_, TcpConnectionState<C>>,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
        ticks: u64,
    ) -> CoreResult<()> {
        let Some(token) = Self::timer_token(kind) else {
            return Ok(());
        };
        context.arm_timer_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_tcp_timer(
        context: &mut SessionProtocolContext<'_, TcpConnectionState<C>>,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
    ) -> bool {
        let Some(token) = Self::timer_token(kind) else {
            return false;
        };
        context.cancel_timer(session_id, token)
    }
}
```

`TcpSessionQueue` keeps convenience methods that register against the session timer wheel:

```rust
impl<C> TcpSessionQueue<C>
where
    C: TcpCongestionController,
{
    pub(crate) fn arm_tcp_timer_ticks(
        &mut self,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
        ticks: u64,
    ) -> CoreResult<()> {
        let mut context = SessionProtocolContext::new(&mut self.driver);
        TcpSessionProtocol::<C>::arm_tcp_timer_ticks(&mut context, session_id, kind, ticks)
    }

    pub(crate) fn cancel_tcp_timer(
        &mut self,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
    ) -> bool {
        let mut context = SessionProtocolContext::new(&mut self.driver);
        TcpSessionProtocol::<C>::cancel_tcp_timer(&mut context, session_id, kind)
    }
}
```

Typed connection methods call these helpers immediately after send, ACK, loss, RACK timeout, TLP timeout, delayed ACK decisions, persist decisions, keepalive decisions, TIME-WAIT entry, or pacing updates. A connection may store active/pending timer bits as protocol state, but the runnable timer task lives in the session wheel.

Expired TCP timers must set the connection's pending timer bit before the session is marked ready:

```rust
fn handle_timer_expiry(
    &mut self,
    driver: &mut SessionDriverRuntime<TcpConnectionState<C>>,
    expiry: SessionTimerExpiry,
) -> CoreResult<()> {
    let Some(kind) = Self::timer_kind(expiry.token()) else {
        return Ok(());
    };
    if let Some(connection) = driver.session_state_mut(expiry.session_id()) {
        connection.tcp_timer_expire(kind);
    }
    driver.mark_ready(expiry.session_id());
    Ok(())
}
```

## Target TCP RACK-TLP API

`crates/hammer-service/src/transport/tcp/recovery.rs`:

```rust
use std::time::{Duration, Instant};

use hammer_core::protocol::tcp::{TcpSackBlock, TcpSeq};
use hammer_infra::vec::Vec;

use crate::transport::congestion::{
    AckedPacket, CongestionController, LostPacket, RttSample,
};

const RACK_REO_WND_FLOOR: Duration = Duration::from_millis(1);
const TLP_TIMEOUT_FLOOR: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpSentSegment {
    pub packet_number: u64,
    pub sequence: u32,
    pub end_sequence: u32,
    pub bytes: u32,
    pub sent_at: Instant,
    pub retransmitted: bool,
    pub is_probe: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpRecoveryAck {
    pub acknowledgment: u32,
    pub now: Instant,
    pub latest_rtt: Duration,
    pub min_rtt: Duration,
    pub app_limited: bool,
    pub ecn_ce: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TcpRecoveryRecord {
    segment: TcpSentSegment,
    delivered: bool,
    lost: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcpRecoveryState {
    next_packet_number: u64,
    records: Vec<TcpRecoveryRecord>,
    min_rtt: Option<Duration>,
    srtt: Option<Duration>,
    rack_delivered_sent_at: Option<Instant>,
    rack_timeout_ticks: Option<u64>,
    tlp_timeout_ticks: Option<u64>,
    tlp_probe_out: bool,
}
```

Recovery feeds a generic controller directly. There is no ACK/loss vector output wrapper:

```rust
impl TcpRecoveryState {
    pub fn on_ack<C>(
        &mut self,
        ack: TcpRecoveryAck,
        congestion: &mut C,
    )
    where
        C: CongestionController,
    {
        // Mark delivered records, call congestion.on_ack for each newly ACKed segment,
        // call congestion.on_end_acks once, mark RACK losses, then call congestion.on_loss.
    }

    pub fn on_sack_blocks<C>(
        &mut self,
        ack: TcpRecoveryAck,
        blocks: &[TcpSackBlock],
        congestion: &mut C,
    )
    where
        C: CongestionController,
    {
        // Mark SACK-delivered records and feed generic ACK/loss samples into congestion.
    }

    pub fn on_rack_timeout<C>(&mut self, now: Instant, congestion: &mut C)
    where
        C: CongestionController,
    {
        // Mark RACK losses and feed LostPacket samples into congestion.
    }

    pub fn next_tlp_probe(&mut self) -> Option<TcpSentSegment> {
        // Return the newest outstanding segment as a probe after the session TLP timer expires.
    }

    pub fn rack_timeout_ticks(&self) -> Option<u64> {
        self.rack_timeout_ticks
    }

    pub fn tlp_timeout_ticks(&self) -> Option<u64> {
        self.tlp_timeout_ticks
    }
}
```

Connection methods register recovery timeout ticks directly through the session timer wheel:

```rust
impl<S, C> TcpConnection<S, C>
where
    C: CongestionController,
{
    fn refresh_recovery_timers(
        &mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
    ) -> CoreResult<()> {
        self.register_or_cancel_timer(
            queue,
            session_id,
            TcpConnectionTimerKind::RACK,
            self.recovery.rack_timeout_ticks(),
        )?;
        self.register_or_cancel_timer(
            queue,
            session_id,
            TcpConnectionTimerKind::TLP,
            self.recovery.tlp_timeout_ticks(),
        )?;
        Ok(())
    }

    fn register_or_cancel_timer(
        &mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
        timeout_ticks: Option<u64>,
    ) -> CoreResult<()> {
        if let Some(timeout_ticks) = timeout_ticks {
            self.tcp_timer_set(kind);
            queue.arm_tcp_timer_ticks(session_id, kind, timeout_ticks.max(1))?;
        } else {
            self.tcp_timer_reset(kind);
            queue.cancel_tcp_timer(session_id, kind);
        }
        Ok(())
    }
}
```

Do not add a session API that converts `Duration` to ticks. TCP recovery and typed connection methods decide TCP timer tick counts and call the existing session tick timer path.

## Task 1: Specify Generic Congestion Controller And BBR

**Files:**
- Create: `crates/hammer-service/tests/transport_congestion_bbr.rs`
- Create: `crates/hammer-service/src/transport/congestion/types.rs`
- Create: `crates/hammer-service/src/transport/congestion/controller.rs`
- Create: `crates/hammer-service/src/transport/congestion/bbr.rs`
- Modify: `crates/hammer-service/src/transport/congestion/mod.rs`

- [ ] **Step 1: Add shared BBR tests**

Create `crates/hammer-service/tests/transport_congestion_bbr.rs`:

```rust
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use hammer_service::transport::congestion::{
    AckedPacket, BbrController, BbrMode, CongestionController, LostPacket, RttSample,
};

const MSS: u32 = 1_440;

fn rtt(ms: u64) -> RttSample {
    let value = Duration::from_millis(ms);
    RttSample {
        latest: value,
        min: value,
    }
}

#[test]
fn bbr_starts_in_startup_with_initial_window() {
    let controller = BbrController::new(MSS);

    assert_eq!(controller.mode(), BbrMode::Startup);
    assert_eq!(controller.max_datagram_size(), MSS);
    assert_eq!(controller.congestion_window(), 10 * MSS);
    assert_eq!(controller.pacing_rate_bytes_per_second(), None);
    assert_eq!(controller.delivered(), 0);
    assert_eq!(controller.min_rtt(), None);
}

#[test]
fn bbr_ack_updates_delivery_rtt_cwnd_and_pacing() {
    let sent_at = Instant::now();
    let now = sent_at + Duration::from_millis(20);
    let mut controller = BbrController::new(MSS);

    controller.on_packet_sent(1, MSS, 10 * MSS, sent_at);
    controller.on_ack(
        now,
        AckedPacket {
            packet_number: 1,
            bytes: MSS,
            sent_at,
            app_limited: false,
            ecn_ce: false,
        },
        rtt(20),
        9 * MSS,
    );
    controller.on_end_acks(now, 9 * MSS, false, 1);

    assert_eq!(controller.delivered(), u64::from(MSS));
    assert_eq!(controller.min_rtt(), Some(Duration::from_millis(20)));
    assert!(controller.congestion_window() > 10 * MSS);
    assert!(controller.pacing_rate_bytes_per_second().is_some());
}

#[test]
fn bbr_accepts_quic_style_packet_number_samples_without_tcp_types() {
    let sent_at = Instant::now();
    let now = sent_at + Duration::from_millis(12);
    let mut controller = BbrController::new(MSS);

    controller.on_packet_sent(42, MSS, 2 * MSS, sent_at);
    controller.on_ack(
        now,
        AckedPacket {
            packet_number: 42,
            bytes: MSS,
            sent_at,
            app_limited: false,
            ecn_ce: true,
        },
        rtt(12),
        MSS,
    );
    controller.on_end_acks(now, MSS, false, 42);

    assert_eq!(controller.delivered(), u64::from(MSS));
    assert!(controller.pacing_rate_bytes_per_second().is_some());
}

#[test]
fn bbr_loss_reduces_window_without_falling_below_minimum() {
    let sent_at = Instant::now();
    let now = sent_at + Duration::from_millis(20);
    let mut controller = BbrController::new(MSS);
    let before = controller.congestion_window();

    controller.on_loss(
        now,
        LostPacket {
            packet_number: 1,
            bytes: MSS,
            sent_at,
        },
        false,
    );

    assert!(controller.congestion_window() < before);
    assert!(controller.congestion_window() >= 4 * MSS);
}

#[test]
fn shared_congestion_has_no_tcp_session_app_or_node_types() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/congestion");
    let mut combined = String::new();
    for entry in fs::read_dir(root).expect("read congestion dir") {
        let entry = entry.expect("dir entry");
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("rs") {
            combined.push_str(&fs::read_to_string(entry.path()).expect("read source"));
        }
    }

    for forbidden in [
        "TcpSeq",
        "TcpConnection",
        "TcpConnectionState",
        "TcpSession",
        "SessionId",
        "SessionQueue",
        "AppRing",
        "AppOp",
        "TcpSegment",
        "TcpPacket",
        "QuicConnection",
        "QuicSession",
        "QuicPacket",
        "QuicStream",
    ] {
        assert!(
            !combined.contains(forbidden),
            "shared congestion leaked transport/session/app/node type: {forbidden}"
        );
    }
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cargo test -p hammer-service --test transport_congestion_bbr
```

Expected: FAIL because `BbrController`, `CongestionController`, `AckedPacket`, `LostPacket`, and `RttSample` are not exported yet.

- [ ] **Step 3: Add shared API files**

Add `types.rs` and `controller.rs` exactly as shown in the target API sections.

- [ ] **Step 4: Move the current paced controller into `bbr.rs`**

Move the existing paced-controller fields and logic from `mod.rs` into `BbrController`. Rename paced constants to BBR constants and implement `CongestionController` for `BbrController`. Keep `BbrController::mode()` as the only BBR-specific public read method.

- [ ] **Step 5: Re-export modules**

Replace `crates/hammer-service/src/transport/congestion/mod.rs` with:

```rust
mod bbr;
mod controller;
mod types;

pub use bbr::{BbrController, BbrMode};
pub use controller::CongestionController;
pub use types::{AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample};
```

- [ ] **Step 6: Run BBR tests**

Run:

```bash
cargo test -p hammer-service --test transport_congestion_bbr
```

Expected: PASS.

## Task 2: Make TCP Connections Generic Over Congestion Controller

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/congestion.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/tests/tcp_connection_state.rs`

- [ ] **Step 1: Update TCP congestion re-exports**

Change `crates/hammer-service/src/transport/tcp/congestion.rs` to:

```rust
pub use crate::transport::congestion::{
    AckedPacket as TcpAckedPacket, CongestionController as TcpCongestionController,
    LostPacket as TcpLostPacket, RttSample as TcpRttSample,
};
```

Do not add `TcpCongestionState`. Do not re-export `BbrController` through the TCP module. TCP code that wants the shared BBR controller for a concrete graph/test imports it from `crate::transport::congestion::BbrController`.

- [ ] **Step 2: Change `TcpConnection` to carry `C`**

Update the struct shape as shown in the target TCP generic connection section. Every impl that does not need BBR-specific methods becomes:

```rust
impl<S, C> TcpConnection<S, C>
where
    C: TcpCongestionController,
{
    pub fn congestion(&self) -> &C {
        &self.congestion
    }
}
```

Every typed transition method must preserve the same concrete controller:

```rust
impl<C> TcpConnection<SynSent, C>
where
    C: TcpCongestionController,
{
    pub fn accept_syn_ack(
        mut self,
        packet: &TcpPacket,
        acknowledgment: u32,
    ) -> (TcpConnection<Established, C>, TcpSegmentHeader) {
        // Existing sequence/window updates stay here.
        // The returned connection keeps the same `congestion: C`.
    }
}
```

- [ ] **Step 3: Keep construction unified as `new`**

Add `TcpConnection<Closed, C>::new(...)` and `TcpConnection<Listen, C>::new(...)` exactly as shown in the target section. Do not add an algorithm parameter, do not add `with_controller`, and do not add a root-state trait.

- [ ] **Step 4: Update storage conversions**

Keep `TcpConnectionState<C>` as the erased TCP-state storage boundary for the current codebase. It is generic over the selected congestion controller and does not default to BBR:

```rust
pub enum TcpConnectionState<C>
where
    C: TcpCongestionController,
{
    Closed(TcpConnection<Closed, C>),
    Listen(TcpConnection<Listen, C>),
    SynSent(TcpConnection<SynSent, C>),
    SynRcvd(TcpConnection<SynRcvd, C>),
    Established(TcpConnection<Established, C>),
    CloseWait(TcpConnection<CloseWait, C>),
    LastAck(TcpConnection<LastAck, C>),
    FinWait1(TcpConnection<FinWait1, C>),
    FinWait2(TcpConnection<FinWait2, C>),
    Closing(TcpConnection<Closing, C>),
    TimeWait(TcpConnection<TimeWait, C>),
}
```

The concrete service graph may define a local alias such as `type TcpBbrConnectionState = TcpConnectionState<BbrController>` at the graph/session composition boundary. Do not put that alias in `state_machine.rs`, do not use it as a `TcpConnection` default, and do not hide the selected controller inside `TcpConnection`.

- [ ] **Step 5: Add a generic construction test**

Append to `crates/hammer-service/tests/tcp_connection_state.rs`:

```rust
use hammer_service::transport::congestion::{BbrController, CongestionController};
use hammer_service::transport::congestion::{
    AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample,
};
use hammer_service::transport::tcp::{Closed, TcpConnection, DEFAULT_TCP_OUTPUT_PAYLOAD_LEN};
use hammer_adapter::DataWorkerId;

#[derive(Clone, Debug)]
struct TestController(BbrController);

impl CongestionController for TestController {
    fn new(max_datagram_size: u32) -> Self {
        Self(BbrController::new(max_datagram_size))
    }

    fn metrics(&self) -> hammer_service::transport::congestion::CongestionMetrics {
        self.0.metrics()
    }

    fn max_datagram_size(&self) -> u32 {
        self.0.max_datagram_size()
    }

    fn congestion_window(&self) -> u32 {
        self.0.congestion_window()
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        self.0.pacing_rate_bytes_per_second()
    }

    fn delivered(&self) -> u64 {
        self.0.delivered()
    }

    fn min_rtt(&self) -> Option<std::time::Duration> {
        self.0.min_rtt()
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        self.0.max_bandwidth_bytes_per_second()
    }

    fn on_packet_sent(
        &mut self,
        packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: std::time::Instant,
    ) {
        self.0
            .on_packet_sent(packet_number, bytes_sent, bytes_in_flight, now);
    }

    fn on_ack(
        &mut self,
        now: std::time::Instant,
        acked: AckedPacket,
        rtt: RttSample,
        bytes_in_flight: u32,
    ) {
        self.0.on_ack(now, acked, rtt, bytes_in_flight);
    }

    fn on_end_acks(
        &mut self,
        now: std::time::Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        largest_acked_packet: PacketNumber,
    ) {
        self.0
            .on_end_acks(now, bytes_in_flight, app_limited, largest_acked_packet);
    }

    fn on_loss(
        &mut self,
        now: std::time::Instant,
        lost: LostPacket,
        persistent_congestion: bool,
    ) {
        self.0.on_loss(now, lost, persistent_congestion);
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        self.0.on_mtu_update(max_datagram_size);
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<std::time::Duration> {
        self.0.next_send_delay(pending_bytes)
    }
}

#[test]
fn tcp_connection_can_carry_a_sibling_congestion_controller_type() {
    let remote = "127.0.0.1:443".parse().expect("remote");
    let connection: TcpConnection<Closed, TestController> = TcpConnection::new(
        None,
        DataWorkerId::new(0),
        10_000,
        None,
        remote,
    );

    assert_eq!(connection.congestion().max_datagram_size(), DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32);
}
```

- [ ] **Step 6: Run TCP state tests**

Run:

```bash
cargo test -p hammer-service --test tcp_connection_state
```

Expected: PASS.

## Task 3: Register TCP Timers Through Session

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/tests/tcp_connection_state.rs`
- Modify: `crates/hammer-service/tests/transport_congestion_bbr.rs`

- [ ] **Step 1: Extend timer kinds**

Update `TcpConnectionTimerKind` exactly as shown in the target TCP timer section.

- [ ] **Step 2: Add session timer token conversion helpers**

Add `timer_token`, `timer_kind`, `arm_tcp_timer_ticks`, and `cancel_tcp_timer` exactly as shown in the target TCP timer section. Timer tokens are derived from the one-hot bit position of `TcpConnectionTimerKind`; do not add a hand-written token constant per timer.

- [ ] **Step 3: Replace retransmit-specific queue wrappers**

Change existing retransmit code from:

```rust
queue.arm_retransmit_timer(session_id, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
queue.cancel_retransmit_timer(session_id);
```

to:

```rust
queue.arm_tcp_timer_ticks(
    session_id,
    TcpConnectionTimerKind::RETRANSMIT,
    TCP_ACTIVE_OPEN_TIMER_TICKS,
)?;
queue.cancel_tcp_timer(session_id, TcpConnectionTimerKind::RETRANSMIT);
```

- [ ] **Step 4: Ensure timer expiry marks the session ready**

In `SessionQueueProtocol<TcpConnectionState<C>> for TcpSessionProtocol<C>`, map known timer tokens to `TcpConnectionTimerKind`, then mark the session ready:

```rust
fn handle_timer_expiry(
    &mut self,
    driver: &mut SessionDriverRuntime<TcpConnectionState<C>>,
    expiry: SessionTimerExpiry,
) -> CoreResult<()> {
    let Some(kind) = Self::timer_kind(expiry.token()) else {
        return Ok(());
    };
    if let Some(connection) = driver.session_state_mut(expiry.session_id()) {
        connection.tcp_timer_expire(kind);
    }
    driver.mark_ready(expiry.session_id());
    Ok(())
}
```

Use the complete `timer_kind` implementation from the target TCP timer section. It includes all eight TCP timer tokens.

- [ ] **Step 5: Add session timer registration tests**

Extend TCP session tests with:

```rust
#[test]
fn tcp_session_queue_can_register_rack_tlp_and_pacing_timers() {
    let worker = DataWorkerId::new(0);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
    let mut queue =
        TcpSessionQueue::<BbrController>::new(worker, runtime.packet_buffers().clone());
    let session_id = queue.insert_session(tcp_connection());

    queue
        .arm_tcp_timer_ticks(session_id, TcpConnectionTimerKind::RACK, 1)
        .expect("arm rack timer");
    queue
        .arm_tcp_timer_ticks(session_id, TcpConnectionTimerKind::TLP, 2)
        .expect("arm tlp timer");
    queue
        .arm_tcp_timer_ticks(session_id, TcpConnectionTimerKind::PACING, 3)
        .expect("arm pacing timer");

    assert_eq!(queue.expire_timers_for_test(1).expect("expire rack"), 1);
    assert_eq!(queue.expire_timers_for_test(1).expect("expire tlp"), 1);
    assert_eq!(queue.expire_timers_for_test(1).expect("expire pacing"), 1);
}
```

- [ ] **Step 6: Run session timer tests**

Run:

```bash
cargo test -p hammer-service transport::tcp::session::tests::tcp_session_queue_can_register_rack_tlp_and_pacing_timers
cargo test -p hammer-service transport::tcp::session::tests::tcp_session_queue_retransmit_timer_can_be_armed_and_cancelled
```

Expected: PASS.

## Task 4: Add TCP RACK-TLP Recovery With Infra Vec

**Files:**
- Create: `crates/hammer-service/tests/tcp_rack_tlp.rs`
- Create: `crates/hammer-service/src/transport/tcp/recovery.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`

- [ ] **Step 1: Add recovery tests with a recording controller**

Create `crates/hammer-service/tests/tcp_rack_tlp.rs`:

```rust
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use hammer_core::protocol::tcp::TcpSackBlock;
use hammer_infra::vec::Vec;
use hammer_service::transport::congestion::{
    AckedPacket, CongestionController, CongestionMetrics, LostPacket, PacketNumber, RttSample,
};
use hammer_service::transport::tcp::recovery::{
    TcpRecoveryAck, TcpRecoveryState, TcpSentSegment,
};

#[derive(Clone, Debug, Default)]
struct RecordingController {
    acked: Vec<AckedPacket>,
    lost: Vec<LostPacket>,
    sent: Vec<PacketNumber>,
    end_acks: u32,
    last_rtt: Option<RttSample>,
    last_event_at: Option<Instant>,
    last_sent_bytes: u32,
    last_bytes_in_flight: u32,
    app_limited_end: bool,
    largest_acked_packet: PacketNumber,
    mtu: u32,
    pending_send_delay: Option<Duration>,
}

impl CongestionController for RecordingController {
    fn new(max_datagram_size: u32) -> Self {
        Self {
            mtu: max_datagram_size,
            ..Self::default()
        }
    }

    fn metrics(&self) -> CongestionMetrics {
        CongestionMetrics {
            congestion_window: 0,
            pacing_rate_bytes_per_second: None,
            delivered: self.acked.len() as u64,
            max_bandwidth_bytes_per_second: 0,
            min_rtt: None,
        }
    }

    fn max_datagram_size(&self) -> u32 {
        self.mtu
    }

    fn congestion_window(&self) -> u32 {
        0
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        None
    }

    fn delivered(&self) -> u64 {
        self.acked.len() as u64
    }

    fn min_rtt(&self) -> Option<Duration> {
        None
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        0
    }

    fn on_packet_sent(
        &mut self,
        packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: Instant,
    ) {
        self.sent.push(packet_number);
        self.last_event_at = Some(now);
        self.last_sent_bytes = bytes_sent;
        self.last_bytes_in_flight = bytes_in_flight.saturating_add(bytes_sent);
        self.pending_send_delay = Some(Duration::ZERO);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        acked: AckedPacket,
        rtt: RttSample,
        bytes_in_flight: u32,
    ) {
        self.pending_send_delay = Some(now.saturating_duration_since(acked.sent_at));
        self.last_event_at = Some(now);
        self.last_rtt = Some(rtt);
        self.last_bytes_in_flight = bytes_in_flight;
        self.acked.push(acked);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        largest_acked_packet: PacketNumber,
    ) {
        self.last_event_at = Some(now);
        self.pending_send_delay = Some(Duration::ZERO);
        self.last_bytes_in_flight = bytes_in_flight;
        self.app_limited_end = app_limited;
        self.largest_acked_packet = largest_acked_packet;
        self.end_acks += 1;
    }

    fn on_loss(&mut self, now: Instant, lost: LostPacket, persistent_congestion: bool) {
        self.last_event_at = Some(now);
        self.pending_send_delay = Some(now.saturating_duration_since(lost.sent_at));
        if persistent_congestion {
            self.last_bytes_in_flight = 0;
        }
        self.lost.push(lost);
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        self.mtu = max_datagram_size;
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        if pending_bytes == 0 {
            None
        } else {
            self.pending_send_delay
        }
    }
}

fn segment(packet_number: u64, sequence: u32, len: u32, sent_at: Instant) -> TcpSentSegment {
    TcpSentSegment {
        packet_number,
        sequence,
        end_sequence: sequence.wrapping_add(len),
        bytes: len,
        sent_at,
        retransmitted: false,
        is_probe: false,
    }
}

fn ack(acknowledgment: u32, now: Instant, rtt_ms: u64) -> TcpRecoveryAck {
    TcpRecoveryAck {
        acknowledgment,
        now,
        latest_rtt: Duration::from_millis(rtt_ms),
        min_rtt: Duration::from_millis(rtt_ms),
        app_limited: false,
        ecn_ce: false,
    }
}

#[test]
fn rack_cumulative_ack_feeds_controller_ack_sample() {
    let now = Instant::now();
    let mut recovery = TcpRecoveryState::new();
    let mut controller = RecordingController::new(1_460);
    recovery.record_sent(segment(1, 1_000, 1_000, now));
    recovery.record_sent(segment(2, 2_000, 1_000, now + Duration::from_millis(1)));

    recovery.on_ack(ack(2_000, now + Duration::from_millis(40), 40), &mut controller);

    assert_eq!(controller.acked.len(), 1);
    assert_eq!(controller.acked[0].packet_number, 1);
    assert_eq!(controller.acked[0].bytes, 1_000);
    assert_eq!(controller.end_acks, 1);
    assert_eq!(recovery.bytes_in_flight(), 1_000);
}

#[test]
fn rack_marks_older_unacked_segment_lost_after_later_sack() {
    let now = Instant::now();
    let mut recovery = TcpRecoveryState::new();
    let mut controller = RecordingController::new(1_460);
    recovery.record_sent(segment(1, 1_000, 1_000, now));
    recovery.record_sent(segment(2, 2_000, 1_000, now + Duration::from_millis(1)));

    recovery.on_sack_blocks(
        ack(1_000, now + Duration::from_millis(30), 40),
        &[TcpSackBlock {
            left_edge: 2_000,
            right_edge: 3_000,
        }],
        &mut controller,
    );
    recovery.on_rack_timeout(now + Duration::from_millis(56), &mut controller);

    assert_eq!(controller.lost.len(), 1);
    assert_eq!(controller.lost[0].packet_number, 1);
}

#[test]
fn tlp_selects_newest_outstanding_segment_as_probe() {
    let now = Instant::now();
    let mut recovery = TcpRecoveryState::new();
    let mut controller = RecordingController::new(1_460);
    recovery.record_sent(segment(1, 1_000, 1_000, now));
    recovery.record_sent(segment(2, 2_000, 1_000, now + Duration::from_millis(1)));
    recovery.on_ack(ack(2_000, now + Duration::from_millis(40), 40), &mut controller);

    let probe = recovery.next_tlp_probe().expect("tlp probe");

    assert_eq!(probe.packet_number, 2);
    assert_eq!(probe.sequence, 2_000);
    assert!(probe.is_probe);
}

#[test]
fn tcp_recovery_uses_infra_vec_and_has_no_session_app_or_bbr_types() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/tcp/recovery.rs"),
    )
    .expect("read recovery source");

    assert!(source.contains("hammer_infra::vec::Vec"));
    for forbidden in [
        "TcpConnectionState",
        "TcpSession",
        "SessionId",
        "SessionQueue",
        "AppRing",
        "AppOp",
        "BbrController",
        "BbrMode",
    ] {
        assert!(
            !source.contains(forbidden),
            "tcp recovery leaked unrelated type: {forbidden}"
        );
    }
}
```

- [ ] **Step 2: Run the failing recovery test**

Run:

```bash
cargo test -p hammer-service --test tcp_rack_tlp
```

Expected: FAIL because the recovery module does not exist.

- [ ] **Step 3: Add `recovery.rs` with infra Vec storage**

Create `crates/hammer-service/src/transport/tcp/recovery.rs` using the target TCP RACK-TLP API. Implement:

```rust
impl TcpRecoveryState {
    pub fn new() -> Self;
    pub fn next_packet_number(&mut self) -> u64;
    pub fn record_sent(&mut self, segment: TcpSentSegment);
    pub fn bytes_in_flight(&self) -> u32;
    pub fn has_unacked_data(&self) -> bool;
    pub fn rack_timeout_ticks(&self) -> Option<u64>;
    pub fn tlp_timeout_ticks(&self) -> Option<u64>;
    pub fn on_ack<C: CongestionController>(&mut self, ack: TcpRecoveryAck, congestion: &mut C);
    pub fn on_sack_blocks<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        blocks: &[TcpSackBlock],
        congestion: &mut C,
    );
    pub fn on_rack_timeout<C: CongestionController>(&mut self, now: Instant, congestion: &mut C);
    pub fn next_tlp_probe(&mut self) -> Option<TcpSentSegment>;
}
```

- [ ] **Step 4: Export the recovery module**

In `crates/hammer-service/src/transport/tcp/mod.rs`, add:

```rust
pub mod recovery;
pub use recovery::{TcpRecoveryAck, TcpRecoveryState, TcpSentSegment};
```

- [ ] **Step 5: Run recovery tests**

Run:

```bash
cargo test -p hammer-service --test tcp_rack_tlp
```

Expected: PASS.

## Task 5: Wire Recovery, BBR, And Session Timers Through Typed Connection Methods

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_rcvd.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/close_wait.rs`
- Modify: `crates/hammer-service/src/transport/tcp/fin_wait1.rs`
- Modify: `crates/hammer-service/src/transport/tcp/fin_wait2.rs`
- Modify: `crates/hammer-service/src/transport/tcp/closing.rs`
- Modify: `crates/hammer-service/src/transport/tcp/last_ack.rs`
- Modify: `crates/hammer-service/src/transport/tcp/time_wait.rs`
- Create: `crates/hammer-service/src/transport/congestion/node.rs`
- Modify: `crates/hammer-service/src/transport/congestion/bbr.rs`
- Modify: `crates/hammer-service/src/transport/congestion/mod.rs`
- Modify: `crates/hammer-service/tests/transport_congestion_bbr.rs`
- Create: `crates/hammer-service/tests/transport_congestion_graph.rs`
- Modify: `crates/hammer-service/tests/tcp_established_receive.rs`

- [ ] **Step 1: Record sent TCP segments through the connection**

Add to the established connection impl generic over the concrete congestion controller:

```rust
impl<C> TcpConnection<Established, C>
where
    C: TcpCongestionController,
    TcpConnection<Established, C>: Into<TcpConnectionState<C>>,
{
    pub(crate) fn record_tcp_segment_sent(
        mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        segment: TcpSentSegment,
    ) -> CoreResult<()> {
        self.recovery.record_sent(segment);
        self.congestion.on_packet_sent(
            segment.packet_number,
            segment.bytes,
            self.recovery.bytes_in_flight(),
            segment.sent_at,
        );
        self.refresh_recovery_timers(queue, session_id)?;
        self.refresh_pacing_timer(queue, session_id, segment.bytes)?;
        queue
            .driver
            .replace_session_state(session_id, self.into());
        Ok(())
    }
}
```

- [ ] **Step 2: Apply ACK/SACK recovery through the connection**

Add to the same generic impl:

```rust
impl<C> TcpConnection<Established, C>
where
    C: TcpCongestionController,
    TcpConnection<Established, C>: Into<TcpConnectionState<C>>,
{
    pub(crate) fn apply_tcp_ack_recovery(
        mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        ack: TcpRecoveryAck,
    ) -> CoreResult<()> {
        let min_rtt = self
            .congestion
            .min_rtt()
            .map_or(ack.latest_rtt, |current| current.min(ack.latest_rtt));
        self.recovery.on_ack(
            TcpRecoveryAck {
                acknowledgment: ack.acknowledgment,
                now: ack.now,
                latest_rtt: ack.latest_rtt,
                min_rtt,
                app_limited: ack.app_limited,
                ecn_ce: ack.ecn_ce,
            },
            &mut self.congestion,
        );
        self.refresh_recovery_timers(queue, session_id)?;
        queue
            .driver
            .replace_session_state(session_id, self.into());
        Ok(())
    }
}
```

- [ ] **Step 3: Register pacing through the session timer**

Add:

```rust
fn refresh_pacing_timer(
    &mut self,
    queue: &mut TcpSessionQueue<C>,
    session_id: SessionId,
    pending_bytes: u32,
) -> CoreResult<()> {
    let ticks = self
        .congestion
        .next_send_delay(pending_bytes)
        .map(tcp_delay_to_timer_ticks);
    self.register_or_cancel_timer(
        queue,
        session_id,
        TcpConnectionTimerKind::PACING,
        ticks,
    )
}
```

`tcp_delay_to_timer_ticks` is TCP-local policy. It converts TCP-owned delay calculations into at least one session timer tick without adding a session API.

Add it beside TCP connection/session timer wiring:

```rust
const TCP_TIMER_TICK_DURATION: Duration = Duration::from_millis(10);

fn tcp_delay_to_timer_ticks(delay: Duration) -> u64 {
    if delay.is_zero() {
        return 1;
    }
    let ticks = delay.as_nanos().div_ceil(TCP_TIMER_TICK_DURATION.as_nanos());
    ticks.min(u128::from(u64::MAX)).max(1) as u64
}
```

Use `tcp_delay_to_timer_ticks` for pacing, RACK, TLP, delayed ACK, persist, keepalive, and TIME-WAIT conversions when those computations start from a `Duration`.

- [ ] **Step 4: Handle RACK and TLP session timer expiry**

Timer expiry must enter typed connection methods:

```rust
impl<C> TcpConnection<Established, C>
where
    C: TcpCongestionController,
    TcpConnection<Established, C>: Into<TcpConnectionState<C>>,
{
    pub(crate) fn on_rack_timer(
        mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        now: Instant,
    ) -> CoreResult<()> {
        self.recovery.on_rack_timeout(now, &mut self.congestion);
        self.refresh_recovery_timers(queue, session_id)?;
        queue
            .driver
            .replace_session_state(session_id, self.into());
        Ok(())
    }

    pub(crate) fn on_tlp_timer(
        mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
    ) -> CoreResult<Option<TcpSentSegment>> {
        let probe = self.recovery.next_tlp_probe();
        self.refresh_recovery_timers(queue, session_id)?;
        queue
            .driver
            .replace_session_state(session_id, self.into());
        Ok(probe)
    }
}
```

The session code may allocate and send the probe segment, but it must not decide RACK/TLP protocol state itself.

- [ ] **Step 5: Add the transport congestion graph node family and BBR sibling**

Create `crates/hammer-service/src/transport/congestion/node.rs` exactly as shown in the transport congestion sibling node section. TCP types are forbidden in that file. Add the concrete BBR graph sibling node in `bbr.rs`:

```rust
#[hammer_component_macros::node(role = internal, sibling_of = CongestionControlNode)]
pub struct BbrCongestionNode {}
```

Implement `Node` for `BbrCongestionNode` exactly as shown in the transport congestion sibling node section. Future siblings live beside BBR as concrete graph nodes, for example `CubicCongestionNode` and `RenoCongestionNode`. Do not add BBR/Cubic/Reno nodes in `transport/tcp`. Do not add `observe_ack`, `observe_send`, `controller`, or `controller_mut` methods to a congestion graph node.

Update `crates/hammer-service/src/transport/congestion/mod.rs`:

```rust
mod bbr;
mod controller;
mod node;
mod types;

pub use bbr::{BbrCongestionNode, BbrController, BbrMode};
pub use controller::CongestionController;
pub use node::{CongestionControlNext, CongestionControlNode};
pub use types::{AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample};
```

- [ ] **Step 6: Wire TCP state-node output to the congestion sibling**

In every TCP state node listed in the files section, rename the output next variant from `Output` to `Congestion`:

```rust
#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    Congestion,
    Drop,
}
```

Update each node process to enqueue allocated TCP control/output segments to the `Congestion` next slot:

```rust
let congestion = next[TcpEstablishedNext::Congestion as usize];
// ...
if let Some(output_index) = output_index.take()
    && let Err(error) = next_frames.enqueue(runtime, congestion, output_index)
{
    runtime.free_index(output_index);
    return Err(error);
}
```

Graph registration must connect all TCP state-node `Congestion` next slots to the selected congestion sibling node. The selected sibling's `CongestionControlNext::Transmit` slot connects to the graph-selected transmit continuation. In the TCP service graph that continuation can be TCP output; `transport/congestion` must not name `TcpOutputNode` or any other protocol-specific output node. `CongestionControlNext::Defer` connects to the transport/session ready path once a transport-owned hold/requeue path exists, and `CongestionControlNext::Drop` connects to drop.

Add `crates/hammer-service/tests/transport_congestion_graph.rs`:

```rust
use std::fs;
use std::path::Path;

#[test]
fn tcp_state_nodes_emit_segments_to_congestion_node_not_tcp_output_directly() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/tcp");
    for file in [
        "listen.rs",
        "syn_sent.rs",
        "syn_rcvd.rs",
        "established.rs",
        "close_wait.rs",
        "fin_wait1.rs",
        "fin_wait2.rs",
        "closing.rs",
        "last_ack.rs",
        "time_wait.rs",
    ] {
        let source = fs::read_to_string(root.join(file)).expect("read tcp node");
        assert!(
            source.contains("Congestion"),
            "{file} must expose a Congestion next"
        );
        assert!(
            !source.contains("Next::Output"),
            "{file} must not keep Output as the TCP segment emission next"
        );
    }
}

#[test]
fn congestion_node_exposes_transmit_defer_drop_nexts() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/congestion");
    let node = fs::read_to_string(root.join("node.rs")).expect("read congestion node");
    let bbr = fs::read_to_string(root.join("bbr.rs")).expect("read bbr node");

    assert!(node.contains("pub enum CongestionControlNext"));
    assert!(node.contains("Transmit"));
    assert!(node.contains("Defer"));
    assert!(node.contains("Drop"));
    assert!(bbr.contains("sibling_of = CongestionControlNode"));
    assert!(bbr.contains("BbrCongestionNode::runtime_nexts(runtime)?"));
}
```

- [ ] **Step 7: Add transport sibling-node structure test**

In `crates/hammer-service/tests/transport_congestion_bbr.rs`, add:

```rust
#[test]
fn bbr_congestion_node_is_transport_graph_sibling_not_direct_api() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/congestion");
    let node = fs::read_to_string(root.join("node.rs")).expect("read congestion node");
    let bbr = fs::read_to_string(root.join("bbr.rs")).expect("read bbr");

    assert!(node.contains("pub struct CongestionControlNode"));
    assert!(bbr.contains("sibling_of = CongestionControlNode"));
    assert!(bbr.contains("pub struct BbrCongestionNode"));

    for forbidden in [
        "fn observe_ack",
        "fn observe_send",
        "fn controller(",
        "fn controller_mut(",
        "BbrCongestionNode::new",
    ] {
        assert!(
            !bbr.contains(forbidden),
            "congestion graph node exposed direct API: {forbidden}"
        );
    }
}
```

- [ ] **Step 8: Run focused TCP integration tests**

Run:

```bash
cargo test -p hammer-service --test tcp_rack_tlp
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service --test tcp_connection_state
cargo test -p hammer-service --test transport_congestion_graph
```

Expected: PASS.

## Task 6: Structural Scans And Final Verification

**Files:**
- Modify: `docs/superpowers/plans/2026-06-14-tcp-session-completion.md`
- Modify: `docs/superpowers/plans/2026-06-16-transport-bbr-congestion-control.md`

- [ ] **Step 1: Update the umbrella plan**

In `docs/superpowers/plans/2026-06-14-tcp-session-completion.md`, under Feature 3.4, add:

```markdown
Detailed execution plan moved to `docs/superpowers/plans/2026-06-16-transport-bbr-congestion-control.md`. The implementation target is typed `TcpConnection<S, C>` plus transport-layer sibling congestion nodes, TCP RACK-TLP recovery backed by `hammer_infra::vec::Vec`, and session-registered RACK/TLP/pacing timers.
```

- [ ] **Step 2: Run formatting**

Run:

```bash
cargo fmt --all
```

Expected: PASS.

- [ ] **Step 3: Run focused tests**

Run:

```bash
cargo test -p hammer-service --test transport_congestion_bbr
cargo test -p hammer-service --test tcp_rack_tlp
cargo test -p hammer-service --test tcp_connection_state
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service transport::tcp::session::tests
```

Expected: PASS.

- [ ] **Step 4: Run structural scans**

Run:

```bash
rg -n "TcpSeq|TcpConnection|TcpConnectionState|TcpSession|SessionId|SessionQueue|AppRing|AppOp|TcpSegment|TcpPacket" crates/hammer-service/src/transport/congestion
rg -n "TcpConnectionState|TcpSession|SessionId|SessionQueue|AppRing|AppOp|BbrController|BbrMode" crates/hammer-service/src/transport/tcp/recovery.rs
rg -n "BbrCongestionNode|CubicCongestionNode|RenoCongestionNode|CongestionControlNode" crates/hammer-service/src/transport/tcp
rg -n "fn observe_ack|fn observe_send|fn controller\\(|fn controller_mut\\(|BbrCongestionNode::new" crates/hammer-service/src/transport/congestion
rg -n "std::vec|next_output_at|congestion_mut_for_tcp_adapter|queue\\.take_connection::<|match .*TcpConnectionState|TcpState::Closed|TcpState::Established|next\\.state\\(\\)" crates/hammer-service/src/transport/tcp crates/hammer-service/tests
```

Expected:
- The first scan has no output.
- The second scan has no output.
- The third scan has no output.
- The fourth scan has no production matches. Test string literals that intentionally guard forbidden names are acceptable.
- The fifth scan has no production matches. Test string literals that intentionally guard forbidden names are acceptable.

- [ ] **Step 5: Run broader crate tests**

Run:

```bash
cargo test -p hammer-service
```

Expected: PASS.

## Acceptance Criteria

- `TcpConnection<S, C>` owns a concrete `C: CongestionController` and never branches on algorithm identity.
- BBR is implemented as `BbrController`; Reno and Cubic remain sibling controller/node types, not hidden variants.
- TCP recovery uses `hammer_infra::vec::Vec` for sent records and test recording buffers.
- RACK, TLP, retransmit, and pacing work are registered through the session timer wheel.
- TCP recovery feeds generic ACK/loss/RTT samples into `C: CongestionController`.
- Shared congestion has no TCP/session/app/node imports.
- TCP recovery has no session/app/BBR imports.
- TCP packet nodes use left-hand typed connections and typed connection methods.
- Existing TCP behavior tests remain green.

## Self-Review Checklist

- Spec coverage:
  - Generic sibling congestion controllers: Tasks 1-2.
  - BBR as first controller: Task 1.
  - TCP RACK-TLP recovery: Task 4.
  - Session-registered timers: Tasks 3 and 5.
  - No shared congestion TCP leakage: Task 6.
  - No recovery session/app/BBR leakage: Task 6.

- Type consistency:
  - `CongestionController` is defined before `TcpConnection<S, C>` uses it.
  - `TcpRecoveryState` uses `hammer_infra::vec::Vec`.
  - Timer registration uses `TcpConnectionTimerKind` plus `SessionTimerToken`.
  - BBR-specific state is visible only through `BbrController`.
