# Feature 3.2 TCP-Owned Typestate State Machine Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor Hammer TCP so mutable TCP protocol state is owned and advanced only by a Fuchsia-style typestate machine.

**Architecture:** There is one state-machine carrier, `TcpStateMachine<S>`, and concrete state structs such as `Closed`, `Listen`, `SynSent`, `SynRcvd`, `Established`, and close-state structs. The carrier owns shared TCP protocol fields. Packet nodes interact with typed outer `TcpConnection<S>` values that carry connection identity, worker ownership, socket addresses, and one `TcpStateMachine<S>`; `TcpConnection<S>` is not a second protocol-state carrier. Transition methods consume one typed connection/machine and either return a concrete next typed value or return a generic runtime value constrained by `From<Next>` for each legal next state.

**Tech Stack:** Rust 2024, existing Hammer TCP packet graph nodes, existing `TcpPacket`, existing `TcpInputNext`, existing `hammer_core::protocol::tcp` protocol types, and Fuchsia netstack3's typestate transition shape from `/private/tmp/fuchsia-tcp/state.rs`.

---

## Hard Rules

- Do not create a new worktree.
- Do not implement code until this plan is accepted.
- Preserve `docs/superpowers/plans/2026-06-14-tcp-session-completion.md`.
- Do not add output, effect, disposition, transition, active-open, event, packet, segment, or next-node wrapper structs/enums for this refactor.
- Do not pass `emit_*`, send, or allocation callables into the state machine.
- Do not expose any caller-visible constructor for non-root TCP phases.
- Do not expose any API that re-borrows a different typed machine out of `TcpConnectionState`.
- Do not add any connection-side mutation hook that writes TCP phase, close reason, sequence, window, handshake option, congestion, or timer fields.
- Do not expose mutable references to the private TCP context or its protocol sub-objects through `TcpConnectionState`.
- Do not model connection identity operations as state-machine events. `connection_id`, worker ownership, and socket addresses are outer connection metadata.
- Do not let nodes set TCP phase or protocol fields. Nodes advance the machine by consuming the runtime enum variant they own and calling the typed transition method for that node.
- Do not let nodes classify TCP control flags into protocol transition cases. The state machine decides the protocol branch.
- `state_machine.rs` may use the existing parsed `TcpPacket`; it must not define a replacement packet/event/header type.
- `state_machine.rs` must not import app ring types, session queue types, packet buffer types, `TcpConnectionState`, or TCP segment allocation functions.
- Packet allocation remains in packet graph nodes. When a transition must produce a control response, the state machine returns the existing core `TcpSegmentHeader` value next to the typed next state; it does not call outside code and does not allocate a packet buffer.
- Packet-driven state methods must not name or import `TcpConnectionState` or the erased runtime phase enum. They must expose every possible next phase through direct type bounds such as `Runtime: From<TcpStateMachine<Established>>`.

## Fuchsia Shape To Preserve

Use Fuchsia for the transition structure, not as a source of Hammer helper types:

- root states expose user operations such as connect/listen;
- concrete state structs own the data for their phase;
- transition methods consume a state and return the next state;
- the runtime enum exists only because the outside runtime stores heterogeneous phases;
- packet-driven branching is still decided inside the state machine, but each branch constructs a concrete next typed machine and converts it through the method's `Runtime: From<TcpStateMachine<Next>>` bounds;
- output segment construction is not represented as a Hammer-specific enum. Fuchsia returns payload-free `Segment<()>` values from some state methods; Hammer's equivalent is the existing `TcpSegmentHeader`, with buffer allocation still done by the node.

Do not copy Fuchsia's private disposition enums into Hammer.

## Node-Facing Typed Connection Shape

Packet nodes must interact with the typed connection on the left-hand side. The node knows which state it owns from the node name and asks the queue for that exact typed state. The state-specific method returns a typed next value that exposes `next_node()` and can be stored back through the queue.

```rust
let connection: TcpConnection<SynSent> = queue.take_connection(session_id)?;
let (next, control) = connection.receive_open_reply(&packet)?;
let next_node = next.next_node();
queue.put_connection(session_id, next);
```

This shape is mandatory for `listen.rs`, `syn_sent.rs`, `rcv_process.rs`, and `established.rs`. Nodes must not manually match TCP flags, must not set TCP state, and must not directly manipulate the runtime storage enum. The runtime storage enum exists only behind the queue boundary so heterogeneous sessions can be stored; it is not the node's protocol API.

`TcpConnection<S>` preserves connection metadata across transitions and delegates all TCP protocol mutation to its inner `TcpStateMachine<S>`. State-specific node methods on `TcpConnection<S>` may use generic `Out: From<TcpConnection<Next>>` bounds for multi-branch packet transitions, but they must not return `TcpConnectionState` directly and must not introduce disposition/effect/output wrapper types.

## Target File Responsibilities

- `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - Owns `TcpStateMachine<S>`, all concrete state structs, all TCP protocol mutations, all transition methods, and typed next-node projection.
  - Contains private constructors for every concrete state.
  - Contains read-only accessors used by the runtime enum and nodes.
- `crates/hammer-service/src/transport/tcp/connection.rs`
  - Keeps TCP option, retransmit-timeout, and connection-view helper types if they are still needed.
  - Defines typed outer `TcpConnection<S>` values and the erased `TcpConnectionState` runtime storage enum used by the queue.
  - Keeps connection identity, worker ownership, local/remote socket metadata, and typed-machine storage on `TcpConnection<S>`.
  - Provides root constructors, read-only projection methods, and standard storage conversions.
  - Contains no protocol field setters and no mutable protocol object accessors.
- `crates/hammer-service/src/transport/tcp/listen.rs`
  - Owns listen-node graph work and SYN-ACK packet allocation.
  - Calls only the listen typed transition for listen packets.
- `crates/hammer-service/src/transport/tcp/syn_sent.rs`
  - Owns syn-sent-node graph work and ACK/RST/SYN-ACK packet allocation from the optional `TcpSegmentHeader` returned by the state machine.
  - Calls only the syn-sent typed transition for syn-sent sessions.
- `crates/hammer-service/src/transport/tcp/rcv_process.rs`
  - Owns receive-process graph work for synchronized close/handshake phases.
  - Calls only typed transition methods for variants assigned to this node.
- `crates/hammer-service/src/transport/tcp/established.rs`
  - Owns established-node graph work, payload delivery, FIN notification, and ACK/RST packet allocation from the optional `TcpSegmentHeader` returned by the state machine.
  - Calls only the established typed transition for established sessions.
- `crates/hammer-service/src/transport/tcp/session.rs`
  - Uses root closed construction plus the typed connect transition for active open.
  - Wraps the typed active-open result as the matching runtime enum variant, inserts it, indexes it, arms timers, and marks readiness.
  - Provides typed queue boundary methods `take_connection<S>()` and `put_connection<S>()` so packet nodes use the left-hand typed connection shape.
- `crates/hammer-service/src/transport/tcp/output.rs`
  - Keeps plain send-window functions.
  - Removes the old send-view abstraction.
- `crates/hammer-service/tests/tcp_state_machine.rs`
  - Adds typestate contract tests and source-shape tests.

## Target State Ownership

`TcpConnection<S>` owns connection identity and runtime ownership metadata. The typestate machine owns only TCP protocol state.

```rust
#[derive(Debug, Clone)]
pub struct TcpConnection<S> {
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    machine: TcpStateMachine<S>,
}
```

`state_machine.rs` owns the mutable TCP protocol context. Its fields are private to the module.

```rust
#[derive(Debug, Clone)]
struct TcpProtocolState {
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
    congestion: TcpCongestionState,
    active_timers: u8,
    pending_timers: u8,
    next_output_at: Option<Instant>,
}
```

`TcpProtocolState` has private constructors and private mutating helpers inside `state_machine.rs` only. External modules do not receive `&mut TcpProtocolState`, `&mut TcpConnectionOptionState`, `&mut TcpCongestionState`, or `&mut TcpRetransmitTimeoutState`.

`TcpStateMachine<S>` is the only state-machine carrier. It carries the shared private protocol state plus the concrete phase type:

```rust
#[derive(Debug, Clone)]
pub struct TcpStateMachine<S> {
    protocol: TcpProtocolState,
    state: S,
}
```

Concrete state structs contain phase-local data only. They do not contain connection identity, socket addresses, or the shared protocol fields. If a phase has no phase-local fields, it can be a private-field zero-sized struct to prevent external construction:

```rust
#[derive(Debug, Clone)]
pub struct Closed {
    _private: (),
}

#[derive(Debug, Clone)]
pub struct Listen {
    _private: (),
}

#[derive(Debug, Clone)]
pub struct SynSent {
    _private: (),
}
```

Use the same private-construction shape for `SynRcvd`, `Established`, `CloseWait`, `LastAck`, `FinWait1`, `FinWait2`, `Closing`, and `TimeWait`, adding phase-local fields only when the protocol actually needs them.

Each state has a private constructor:

```rust
impl SynSent {
    fn new() -> Self {
        Self { _private: () }
    }
}
```

Only transition methods and root constructors call concrete state constructors.

## Target Runtime Boundary

`TcpConnectionState` remains the queue storage enum. It is the erased runtime boundary and must not be returned from typed transition methods:

```rust
#[derive(Debug, Clone)]
pub enum TcpConnectionState {
    Closed(TcpConnection<Closed>),
    Listen(TcpConnection<Listen>),
    SynSent(TcpConnection<SynSent>),
    SynRcvd(TcpConnection<SynRcvd>),
    Established(TcpConnection<Established>),
    CloseWait(TcpConnection<CloseWait>),
    LastAck(TcpConnection<LastAck>),
    FinWait1(TcpConnection<FinWait1>),
    FinWait2(TcpConnection<FinWait2>),
    Closing(TcpConnection<Closing>),
    TimeWait(TcpConnection<TimeWait>),
}
```

Allowed root construction:

```rust
impl TcpConnection<Closed> {
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self;
}

impl TcpConnection<Listen> {
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self;
}
```

Call root constructors with the state on the left-hand side only:

```rust
let connection: TcpConnection<Closed> =
    TcpConnection::new(connection_id, owner_worker, local_port, local, remote);

let connection: TcpConnection<Listen> =
    TcpConnection::new(connection_id, owner_worker, local_port, local, remote);
```

Do not write `TcpConnection::<Closed>::new(...)` or `TcpConnection::<Listen>::new(...)` at call sites. The left-hand type is the state assertion. Do not expose `new` for non-root states. Do not add `with_state`, `closed`, `syn_sent`, `established`, or any constructor that lets callers manually pick a non-root TCP phase.

Phase movement is exposed only at the queue boundary through typed take/put:

```rust
let connection: TcpConnection<SynSent> = queue.take_connection(session_id)?;
let (next, control) = connection.receive_open_reply(&packet)?;
let next_node = next.next_node();
queue.put_connection(session_id, next);
```

Do not add `take_syn_sent_phase`, `as_established_machine`, `expect_syn_sent`, `map_phase_once`, or similar per-state extraction APIs. Do not make nodes match `TcpConnectionState` just to obtain their current state; the left-hand `TcpConnection<S>` type is the node's current-state assertion.

Production code must not keep the old constructor that accepts an arbitrary `TcpState`. Test fixtures that need a direct established connection use a `#[cfg(test)]` helper in `TcpConnectionState` or build the state through the handshake transitions.

`connection.rs` implements `From<TcpConnection<...>> for TcpConnectionState` and `TryFrom<TcpConnectionState> for TcpConnection<...>` for every runtime state variant. `state_machine.rs` relies only on typed machines and never imports `TcpConnectionState`.

```rust
impl From<TcpConnection<SynSent>> for TcpConnectionState {
    fn from(connection: TcpConnection<SynSent>) -> Self {
        Self::SynSent(connection)
    }
}
```

## Transition API

Root active open:

```rust
impl TcpStateMachine<Closed> {
    pub(super) fn connect(self, iss: u32) -> TcpStateMachine<SynSent>;
}
```

Passive open deterministic core:

```rust
impl TcpStateMachine<Listen> {
    fn on_syn(
        self,
        sequence: u32,
        advertised_window: u16,
        capabilities: TcpCapabilities,
    ) -> TcpStateMachine<SynRcvd>;
}
```

Handshake completion:

```rust
impl TcpStateMachine<SynRcvd> {
    fn on_final_ack(
        self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<Established>;
}
```

Established FIN:

```rust
impl TcpStateMachine<Established> {
    fn on_fin(
        self,
        sequence: u32,
        payload_len: usize,
        acknowledgment: Option<u32>,
        advertised_window: u16,
    ) -> TcpStateMachine<CloseWait>;
}
```

Local close and close-state transitions are part of this state machine:

```rust
impl TcpStateMachine<Established> {
    fn close(self) -> TcpStateMachine<FinWait1>;
}

impl TcpStateMachine<CloseWait> {
    fn close(self) -> TcpStateMachine<LastAck>;
}

impl TcpStateMachine<FinWait1> {
    fn on_fin_ack(self, sequence: u32, acknowledgment: u32, advertised_window: u16)
        -> TcpStateMachine<TimeWait>;
    fn on_ack(self, acknowledgment: u32, advertised_window: u16)
        -> TcpStateMachine<FinWait2>;
    fn on_fin(self, sequence: u32, advertised_window: u16)
        -> TcpStateMachine<Closing>;
}

impl TcpStateMachine<FinWait2> {
    fn on_fin(self, sequence: u32, advertised_window: u16)
        -> TcpStateMachine<TimeWait>;
}

impl TcpStateMachine<Closing> {
    fn on_ack(self, acknowledgment: u32, advertised_window: u16)
        -> TcpStateMachine<TimeWait>;
}

impl TcpStateMachine<LastAck> {
    fn on_ack(self, acknowledgment: u32, advertised_window: u16)
        -> TcpStateMachine<Closed>;
}

impl TcpStateMachine<TimeWait> {
    fn on_timeout(self) -> TcpStateMachine<Closed>;
}
```

These typed transition methods are for protocol events that the state machine has already accepted. They do not use `Result<next, current>` or `Option<next>` to encode "no transition".

Packet-driven node entry points use the existing `TcpPacket` and return a generic output constrained by every legal concrete next typestate. They do not take callables and do not return `CoreResult`; the method consumes one typed connection and returns `(Out, Option<TcpSegmentHeader>)`.

Each packet-driven method must name every possible next typestate in its `Out` bounds. `Out` is not a new transition/disposition type; in production callers it is the typed connection value selected by Rust's left-hand type or the queue storage conversion at the boundary. The optional `TcpSegmentHeader` is the existing Hammer core TCP header for one payload-free control segment, not a state-machine output wrapper. `None` means no control packet. `Some(header)` means the node should allocate exactly that header and enqueue it to the existing output edge.

```rust
impl TcpConnection<SynSent> {
    pub(super) fn receive_open_reply<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<SynSent>>
            + From<TcpConnection<SynRcvd>>
            + From<TcpConnection<Established>>
            + From<TcpConnection<Closed>>;
}
```

The method decides whether the packet keeps the connection in `SynSent`, advances to `SynRcvd`, advances to `Established`, or closes. It then returns `Out::from(next_connection)` for exactly one concrete typed next connection. The state machine decides the protocol branch, the next state type, and any control header. The node only chooses the left-hand state type, stores the returned next value, and turns `Some(TcpSegmentHeader)` into a packet buffer.

All packet-driven entry points must use this concrete next-state list:

```rust
impl TcpConnection<Listen> {
    pub(super) fn receive_syn<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<Listen>>
            + From<TcpConnection<SynRcvd>>;
}

impl TcpConnection<SynRcvd> {
    pub(super) fn receive_final_ack<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<SynRcvd>>
            + From<TcpConnection<Established>>
            + From<TcpConnection<Closed>>;
}

impl TcpConnection<Established> {
    pub(super) fn receive_data<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<Established>>
            + From<TcpConnection<CloseWait>>
            + From<TcpConnection<Closed>>;
}

impl TcpConnection<CloseWait> {
    pub(super) fn receive_close_wait<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<CloseWait>>
            + From<TcpConnection<Closed>>;
}

impl TcpConnection<FinWait1> {
    pub(super) fn receive_fin_wait1<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<FinWait1>>
            + From<TcpConnection<FinWait2>>
            + From<TcpConnection<Closing>>
            + From<TcpConnection<TimeWait>>
            + From<TcpConnection<Closed>>;
}

impl TcpConnection<FinWait2> {
    pub(super) fn receive_fin_wait2<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<FinWait2>>
            + From<TcpConnection<TimeWait>>
            + From<TcpConnection<Closed>>;
}

impl TcpConnection<Closing> {
    pub(super) fn receive_closing<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<Closing>>
            + From<TcpConnection<TimeWait>>
            + From<TcpConnection<Closed>>;
}

impl TcpConnection<LastAck> {
    pub(super) fn receive_last_ack<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<LastAck>>
            + From<TcpConnection<Closed>>;
}

impl TcpConnection<TimeWait> {
    pub(super) fn receive_time_wait<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<TimeWait>>
            + From<TcpConnection<Closed>>;
}
```

## Read-Only Projection And Events

`TcpConnectionState` keeps identity projection itself and delegates TCP protocol projection to its inner phase:

```rust
impl TcpConnectionState {
    pub fn state(&self) -> TcpState;
    pub fn next_node(&self) -> TcpInputNext;
    pub fn view(&self) -> TcpConnectionView;
    pub fn connection_id(&self) -> Option<TcpConnectionId>;
    pub fn owner_worker(&self) -> DataWorkerId;
    pub fn local_port(&self) -> u16;
    pub fn local(&self) -> Option<SocketAddr>;
    pub fn remote(&self) -> SocketAddr;
    pub fn iss(&self) -> u32;
    pub fn irs(&self) -> u32;
    pub fn snd_una(&self) -> u32;
    pub fn snd_nxt(&self) -> u32;
    pub fn snd_wnd(&self) -> u32;
    pub fn rcv_nxt(&self) -> u32;
    pub fn rcv_wnd(&self) -> u32;
    pub fn local_capabilities(&self) -> TcpCapabilities;
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities>;
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions;
}
```

Mutable TCP protocol operations are modeled as state-machine events, not connection field setters. Retransmit timeout, congestion observation, local capability configuration, and output scheduling are forwarded to methods implemented in `state_machine.rs`, where the private `TcpProtocolState` is mutated. Connection identity and ownership operations, such as assigning `connection_id`, remain direct outer `TcpConnectionState` methods because they are not TCP protocol transitions.

## Next Node

Typed machines expose their node target:

```rust
impl TcpStateMachine<SynSent> {
    pub const fn next_node(&self) -> TcpInputNext {
        TcpInputNext::SynSent
    }
}
```

The outer connection delegates next-node projection to the inner phase:

```rust
impl TcpConnectionState {
    pub fn next_node(&self) -> TcpInputNext {
        match self {
            TcpConnectionState::Closed(connection) => connection.next_node(),
            TcpConnectionState::Listen(connection) => connection.next_node(),
            TcpConnectionState::SynSent(connection) => connection.next_node(),
            TcpConnectionState::SynRcvd(connection) => connection.next_node(),
            TcpConnectionState::Established(connection) => connection.next_node(),
            TcpConnectionState::CloseWait(connection) => connection.next_node(),
            TcpConnectionState::LastAck(connection) => connection.next_node(),
            TcpConnectionState::FinWait1(connection) => connection.next_node(),
            TcpConnectionState::FinWait2(connection) => connection.next_node(),
            TcpConnectionState::Closing(connection) => connection.next_node(),
            TcpConnectionState::TimeWait(connection) => connection.next_node(),
        }
    }
}
```

Do not create a separate next-node wrapper type.

## Node Process Contract

Each TCP packet node keeps the existing frame-loop shape:

1. `parse_tcp_packet(runtime, index)` parses the existing `TcpPacket`.
2. The node resolves the session id through the existing listener, pending, or established tuple indexes.
3. The node obtains the exact typed connection it owns from the queue, for example `let connection: TcpConnection<SynSent> = queue.take_connection(session_id)?;`.
4. The node calls the fixed state-specific method for that node, for example `connection.receive_open_reply(&packet)`.
5. The typed method returns `(next, control_header)`, where `next` is a typed connection value, not `TcpConnectionState` and not a wrapper/disposition type.
6. The node reads `next.next_node()` if it needs the resulting graph state, then stores `next` back through `queue.put_connection(session_id, next)`.
7. If `control_header` is `Some(header)`, the node allocates a packet with `alloc_tcp_segment(runtime.packet_buffers(), tcp_segment_metadata(packet.local, connection.remote()), header)` and enqueues it to the node's `Output` next edge.
8. The node performs only runtime side effects: tuple indexing, pending-index removal, retransmit timer arming/canceling, ready marking, app completion, payload delivery, and session close/removal.
9. The input packet buffer is freed unless ownership is transferred to app RX delivery.

Node protocol dispatch is not allowed: `listen.rs`, `syn_sent.rs`, `rcv_process.rs`, and `established.rs` must not inspect `packet.flags` to choose TCP behavior, must not set TCP state manually, must not match protocol states for a node that already has one fixed state, and must not mutate sequence/window/option/congestion/timer fields directly. `rcv_process.rs` may use separate typed helper entry points for each state assigned to receive-process, but each helper still has a left-hand typed connection such as `TcpConnection<SynRcvd>` or `TcpConnection<CloseWait>`.

The `TcpInputNext` mapping is owned by typed machines. `TcpConnectionState::next_node()` delegates to the current typed phase and the input node uses that result for graph routing/indexing. Do not add a second next-node enum or manually map `TcpState` to graph nodes in packet-processing code.

---

### Task 1: Replace The Contract Tests First

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Replace: `crates/hammer-service/tests/tcp_state_machine.rs`

- [ ] **Step 1: Add typestate construction tests in `state_machine.rs`**

Add these tests to a `#[cfg(test)] mod tests` in `crates/hammer-service/src/transport/tcp/state_machine.rs`. They live there because transition constructors and deterministic transition methods are not public API.

```rust
use hammer_core::protocol::tcp::{TcpCapabilities, TcpCloseReason, TcpState};

use super::{
    CloseWait, Closed, Established, Listen, SynRcvd, SynSent, TcpStateMachine,
};
use crate::transport::tcp::TcpInputNext;

fn assert_machine<S>(machine: TcpStateMachine<S>) -> TcpStateMachine<S> {
    machine
}

#[test]
fn closed_connect_returns_syn_sent_typestate() {
    let closed: TcpStateMachine<Closed> = TcpStateMachine::new();

    let syn_sent = assert_machine::<SynSent>(closed.connect(7_000));

    assert_eq!(syn_sent.tcp_state(), TcpState::SynSent);
    assert_eq!(syn_sent.next_node(), TcpInputNext::SynSent);
    assert_eq!(syn_sent.iss(), 7_000);
    assert_eq!(syn_sent.snd_una(), 7_000);
    assert_eq!(syn_sent.snd_nxt(), 7_001);
}
```

- [ ] **Step 2: Add deterministic transition tests in `state_machine.rs`**

Add these tests in the same file. Each state is obtained from a root constructor or a previous transition result.

```rust
#[test]
fn listen_syn_returns_syn_rcvd_typestate() {
    let listen: TcpStateMachine<Listen> = TcpStateMachine::new();

    let syn_rcvd = assert_machine::<SynRcvd>(
        listen.on_syn(7_000, u16::MAX, TcpCapabilities::default()),
    );

    assert_eq!(syn_rcvd.tcp_state(), TcpState::SynRcvd);
    assert_eq!(syn_rcvd.next_node(), TcpInputNext::RcvProcess);
    assert_eq!(syn_rcvd.irs(), 7_000);
    assert_eq!(syn_rcvd.rcv_nxt(), 7_001);
}

#[test]
fn syn_rcvd_final_ack_returns_established_typestate() {
    let listen: TcpStateMachine<Listen> = TcpStateMachine::new();
    let syn_rcvd = listen.on_syn(7_000, u16::MAX, TcpCapabilities::default());
    let final_ack = syn_rcvd.snd_nxt();

    let established = assert_machine::<Established>(
        syn_rcvd.on_final_ack(final_ack, u16::MAX),
    );

    assert_eq!(established.tcp_state(), TcpState::Established);
    assert_eq!(established.next_node(), TcpInputNext::Established);
}

#[test]
fn established_fin_returns_close_wait_typestate() {
    let listen: TcpStateMachine<Listen> = TcpStateMachine::new();
    let syn_rcvd = listen.on_syn(7_000, u16::MAX, TcpCapabilities::default());
    let final_ack = syn_rcvd.snd_nxt();
    let established = syn_rcvd.on_final_ack(final_ack, u16::MAX);
    let fin_ack = established.snd_nxt();

    let close_wait = assert_machine::<CloseWait>(
        established.on_fin(7_001, 0, Some(fin_ack), u16::MAX),
    );

    assert_eq!(close_wait.tcp_state(), TcpState::CloseWait);
    assert_eq!(close_wait.next_node(), TcpInputNext::RcvProcess);
}
```

- [ ] **Step 3: Add source-shape tests in `tests/tcp_state_machine.rs`**

The forbidden strings are assembled with `concat!` so the tests can look for exact source symbols without reintroducing them as contiguous source text in the test itself.

```rust
#[test]
fn tcp_sources_do_not_expose_connection_mutation_hooks() {
    let sources = [
        include_str!("../src/transport/tcp/connection.rs"),
        include_str!("../src/transport/tcp/state_machine.rs"),
    ];
    let forbidden = [
        concat!("machine", "_"),
        concat!("set", "_state"),
        concat!("set", "_sequence", "_state"),
        concat!("set", "_send", "_state"),
        concat!("set", "_receive", "_state"),
        concat!("connection", "_mut"),
        concat!("option", "_state", "_mut"),
        concat!("congestion", "_mut"),
        concat!("retransmit", "_timeout", "_mut"),
        concat!("accept", "_in", "_order", "_payload"),
    ];

    for source in sources {
        for symbol in forbidden {
            assert!(!source.contains(symbol), "{symbol}");
        }
    }
}

#[test]
fn tcp_sources_do_not_expose_extra_transition_shapes() {
    let sources = [
        include_str!("../src/transport/tcp/state_machine.rs"),
        include_str!("../src/transport/tcp/connection.rs"),
        include_str!("../src/transport/tcp/listen.rs"),
        include_str!("../src/transport/tcp/syn_sent.rs"),
        include_str!("../src/transport/tcp/rcv_process.rs"),
        include_str!("../src/transport/tcp/established.rs"),
    ];
    let forbidden = [
        concat!("Tcp", "Active", "Open"),
        concat!("Tcp", "Output", "Send", "View"),
        concat!("Tcp", "State", "Segment"),
        concat!("Tcp", "State", "Transition"),
        concat!("Tcp", "State", "Machine", "Output"),
        concat!("Disposition"),
        concat!("enter", "_"),
    ];

    for source in sources {
        for symbol in forbidden {
            assert!(!source.contains(symbol), "{symbol}");
        }
    }
}
```

- [ ] **Step 4: Verify RED**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine
```

Expected before implementation: compile failures for missing root constructors, enum variants, and transition methods, plus source-shape failures from the current dirty attempt.

### Task 2: Build The Owned State Machine Core

**Files:**
- Replace: `crates/hammer-service/src/transport/tcp/state_machine.rs`

- [ ] **Step 1: Define private protocol context and state carrier**

Implement `TcpProtocolState`, `TcpStateMachine<S>`, and concrete state structs from **Target State Ownership**. Keep all fields private. Do not define `TcpConnection<S>` in `state_machine.rs`; the typed outer connection belongs in `connection.rs` and owns identity/worker/address metadata plus one `TcpStateMachine<S>`.

- [ ] **Step 2: Implement constructors**

Implement a private `TcpProtocolState::new(...)` and private concrete-state constructors. Expose only root machine construction through left-hand typed `new()`:

```rust
impl TcpStateMachine<Closed> {
    pub(super) fn new() -> Self;
}

impl TcpStateMachine<Listen> {
    pub(super) fn new() -> Self;
}
```

Call root machine constructors with the state on the left-hand side only:

```rust
let machine: TcpStateMachine<Closed> = TcpStateMachine::new();
let machine: TcpStateMachine<Listen> = TcpStateMachine::new();
```

Do not write `TcpStateMachine::<Closed>::new()`, `TcpStateMachine::<Listen>::new()`, `TcpStateMachine::<Closed>::closed()`, or `TcpStateMachine::<Listen>::listen()`.

- [ ] **Step 3: Implement read-only accessors**

Implement read-only accessors on `TcpStateMachine<S>` by reading through the private protocol state:

```rust
impl<S> TcpStateMachine<S> {
    pub fn iss(&self) -> u32;
    pub fn irs(&self) -> u32;
    pub fn snd_una(&self) -> u32;
    pub fn snd_nxt(&self) -> u32;
    pub fn snd_wnd(&self) -> u32;
    pub fn rcv_nxt(&self) -> u32;
    pub fn rcv_wnd(&self) -> u32;
    pub fn local_capabilities(&self) -> TcpCapabilities;
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities>;
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions;
}
```

Do not add mutable accessors for the private protocol state or its protocol sub-objects. Identity, worker ownership, and socket-address accessors are implemented on `TcpConnection<S>`/`TcpConnectionState`, not on `TcpStateMachine<S>`.

- [ ] **Step 4: Implement protocol transitions**

Implement:

```rust
impl TcpStateMachine<Closed> {
    pub(super) fn connect(self, iss: u32) -> TcpStateMachine<SynSent>;
}

impl TcpStateMachine<Listen> {
    pub(super) fn on_syn(
        self,
        sequence: u32,
        advertised_window: u16,
        capabilities: TcpCapabilities,
    ) -> TcpStateMachine<SynRcvd>;
}

impl TcpStateMachine<SynRcvd> {
    pub(super) fn on_final_ack(
        self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<Established>;
}

impl TcpStateMachine<Established> {
    pub(super) fn on_fin(
        self,
        sequence: u32,
        payload_len: usize,
        acknowledgment: Option<u32>,
        advertised_window: u16,
    ) -> TcpStateMachine<CloseWait>;
}
```

All mutations of phase data, close reason, sequence numbers, windows, and negotiated handshake options happen in these methods or helper methods private to `state_machine.rs`.

- [ ] **Step 5: Implement packet-driven transitions with direct output bounds**

Implement `on_segment` for node-owned phases. Use the existing `TcpPacket`; do not introduce a packet wrapper.

```rust
impl TcpStateMachine<SynSent> {
    pub(super) fn on_segment<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpStateMachine<SynSent>>
            + From<TcpStateMachine<SynRcvd>>
            + From<TcpStateMachine<Established>>
            + From<TcpStateMachine<Closed>>;
}
```

The method handles valid `SYN|ACK`, unacceptable ACK reset header without transition, acceptable `RST|ACK` close, simultaneous open, and ignored packets internally. It must return `Out::from(next_machine)` for exactly one concrete typed next machine, including `Out::from(self)` for the ignored/no-phase-change path. When protocol rules require an ACK, RST, SYN-ACK, or FIN, return it as `Some(TcpSegmentHeader)`.

Implement the full packet-driven method set listed in **Transition API** for `Listen`, `SynRcvd`, `Established`, `CloseWait`, `FinWait1`, `FinWait2`, `Closing`, `LastAck`, and `TimeWait`. Every possible next state must appear as a direct `Out: From<TcpStateMachine<Next>>` bound.

- [ ] **Step 6: Implement close-state transitions**

Implement local close and remote close paths in `state_machine.rs`:

```rust
impl TcpStateMachine<Established> {
    pub(super) fn close(self) -> TcpStateMachine<FinWait1>;
    pub(super) fn on_fin(
        self,
        sequence: u32,
        payload_len: usize,
        acknowledgment: Option<u32>,
        advertised_window: u16,
    ) -> TcpStateMachine<CloseWait>;
}

impl TcpStateMachine<CloseWait> {
    pub(super) fn close(self) -> TcpStateMachine<LastAck>;
}

impl TcpStateMachine<FinWait1> {
    pub(super) fn on_ack(
        self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<FinWait2>;

    pub(super) fn on_fin(
        self,
        sequence: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<Closing>;

    pub(super) fn on_fin_ack(
        self,
        sequence: u32,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<TimeWait>;
}

impl TcpStateMachine<FinWait2> {
    pub(super) fn on_fin(
        self,
        sequence: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<TimeWait>;
}

impl TcpStateMachine<Closing> {
    pub(super) fn on_ack(
        self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<TimeWait>;
}

impl TcpStateMachine<LastAck> {
    pub(super) fn on_ack(
        self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<Closed>;
}

impl TcpStateMachine<TimeWait> {
    pub(super) fn on_timeout(self) -> TcpStateMachine<Closed>;
}
```

RST from any synchronized or close state transitions to `Closed` through a typed method on that state, with `TcpCloseReason::RemoteReset` stored in the private protocol context.

- [ ] **Step 7: Implement event methods for non-transition mutation**

Add event-named methods in `state_machine.rs` for existing runtime needs:

```rust
impl<S> TcpStateMachine<S> {
    pub(super) fn configure_local_capabilities(&mut self, capabilities: TcpCapabilities) -> TcpNegotiatedOptions;
    pub(super) fn observe_congestion_ack(&mut self, sample: TcpCongestionAckSample);
    pub(super) fn observe_congestion_loss(&mut self, bytes_lost: u32);
    pub(super) fn schedule_next_output(&mut self, deadline: Option<Instant>);
    pub(super) fn arm_tcp_timer(&mut self, timer: TcpConnectionTimerKind);
    pub(super) fn reset_tcp_timer(&mut self, timer: TcpConnectionTimerKind);
    pub(super) fn expire_tcp_timer(&mut self, timer: TcpConnectionTimerKind);
    pub(super) fn observe_retransmit_timeout(&mut self) -> Duration;
}
```

The exact method list may be reduced while editing if an event is unused, but any remaining TCP protocol mutation must mutate the private protocol context only inside `state_machine.rs`.

### Task 3: Refactor `TcpConnection<S>` Into The Outer Typed Connection

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`

- [ ] **Step 1: Keep identity outside the protocol machine**

Replace the flat protocol fields with `machine: TcpStateMachine<S>`, but keep `connection_id`, `owner_worker`, `local_port`, `local`, and `remote` as direct fields on `TcpConnection<S>`. `TcpConnectionState` is the erased storage enum with variants such as `SynSent(TcpConnection<SynSent>)`.

- [ ] **Step 2: Implement root constructors only**

Root `TcpConnection<Closed>::new(...)` constructs `let machine: TcpStateMachine<Closed> = TcpStateMachine::new();`.

Root `TcpConnection<Listen>::new(...)` constructs `let machine: TcpStateMachine<Listen> = TcpStateMachine::new();`.

Remove the production constructor that accepts arbitrary `TcpState`.

- [ ] **Step 3: Implement read-only projection**

Implement `state()`, `next_node()`, `view()`, identity accessors, address accessors, sequence/window accessors, option read accessors, timer read accessors, and output sizing by matching the enum and delegating to the inner typed machine.

- [ ] **Step 4: Keep identity mutation outside the state machine**

If passive open needs a `TcpConnectionId`, allocate the `SessionId` first and construct `TcpConnection<Listen>` with the final id. Do not add a post-insert connection-id mutation path for this refactor.

Timers, congestion observations, retransmit-timeout observations, local capability configuration, and output scheduling are TCP protocol operations. Forward those to the inner typed machine through event-named methods; do not expose mutable protocol sub-objects and do not write TCP protocol fields directly in `connection.rs`.

### Task 4: Refactor Session Active Open

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`

- [ ] **Step 1: Use root closed state plus connect transition**

```rust
let connection: TcpConnection<Closed> = TcpConnection::new(
    None,
    self.worker(),
    local.port(),
    Some(local),
    remote,
);
let connection = connection.connect(iss);
let session_id = self.insert_session(connection.into());
```

- [ ] **Step 2: Preserve runtime work outside the machine**

Keep session insertion, pending tuple indexing, retransmit timer arming, and ready marking in `session.rs`.

- [ ] **Step 3: Remove arbitrary phase construction from tests**

Update session tests to use left-hand root construction such as `let connection: TcpConnection<Closed> = TcpConnection::new(...);`, test-only fixtures, or real transitions instead of constructing arbitrary phases through the old constructor.

### Task 5: Refactor Packet Nodes To Consume Typed Variants

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`

- [ ] **Step 1: Listen node**

Create a typed listen connection with left-hand root construction, call `connection.receive_syn(&packet)`, and insert the returned typed `TcpConnection<SynRcvd>` through the queue storage boundary. Insert the session with its final `TcpConnectionId` already present in outer connection metadata; do not assign connection identity through a state-machine event.

```rust
let connection: TcpConnection<Listen> =
    TcpConnection::new(Some(connection_id), queue.worker(), packet.local.port(), Some(packet.local), packet.remote);
let (next, control) = connection.receive_syn(&packet);
let next_node = next.next_node();
let session_id = queue.insert_session(next.into());
```

The node allocates a SYN-ACK only when the returned optional `TcpSegmentHeader` is `Some(header)`. It does not inspect flags to decide the protocol transition.

- [ ] **Step 2: Syn-sent node**

Use the left-hand typed connection shape exactly:

```rust
let connection: TcpConnection<SynSent> = queue.take_connection(session_id)?;
let (next, control) = connection.receive_open_reply(&packet)?;
let next_node = next.next_node();
queue.put_connection(session_id, next);
```

The node does not inspect flags to choose TCP behavior. It only allocates the returned optional `TcpSegmentHeader` and performs pending-index/session completion/close work based on the resulting typed connection view.

- [ ] **Step 3: Receive-process node**

Use typed helper entry points for receive-process states. Each helper takes the left-hand type it owns:

```rust
let connection: TcpConnection<SynRcvd> = queue.take_connection(session_id)?;
let (next, control) = connection.receive_final_ack(&packet)?;
let next_node = next.next_node();
queue.put_connection(session_id, next);
```

```rust
let connection: TcpConnection<CloseWait> = queue.take_connection(session_id)?;
let (next, control) = connection.receive_close_wait(&packet)?;
let next_node = next.next_node();
queue.put_connection(session_id, next);
```

The node must not inspect TCP flags to choose protocol behavior. `SynRcvd`, `CloseWait`, `LastAck`, `FinWait1`, `FinWait2`, `Closing`, and `TimeWait` must all be advanced through typed close/handshake methods that delegate protocol mutation to `state_machine.rs`.

- [ ] **Step 4: Established node**

Use the left-hand typed connection shape:

```rust
let connection: TcpConnection<Established> = queue.take_connection(session_id)?;
let (next, control, accepted_payload_len, fin) = connection.receive_data(&packet)?;
let next_node = next.next_node();
queue.put_connection(session_id, next);
```

Payload delivery and FIN app completion stay in the node, based on the packet payload and resulting typed phase view on `TcpConnectionState`. Sequence advancement and FIN state transition stay in the state machine.

- [ ] **Step 5: App close path**

When the app close submission is drained in `session.rs`, do not close the session directly if the TCP state is synchronized. Take the runtime enum variant, call the typed local close transition, reinsert `FinWait1` for `Established` or `LastAck` for `CloseWait`, and let packet output/timer behavior remain in the existing session graph. If the current state is already a close state, dispatch through the typed close-state method that is valid for that phase or reject the duplicate close as a session-layer condition.

### Task 6: Remove Old Output View And Arbitrary Construction

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/tests/tcp_output.rs`
- Modify: tests that still use old construction or mutable protocol accessors

- [ ] **Step 1: Delete the old send-view type**

Keep:

```rust
pub const DEFAULT_TCP_OUTPUT_PAYLOAD_LEN: usize = 1_440;
pub const fn tcp_effective_output_payload_len(peer_max_segment_size: Option<u16>) -> usize;
pub fn tcp_available_send_window(...);
pub fn tcp_payload_len_in_send_window(...);
```

Do not replace the old send-view type with another send-view wrapper.

- [ ] **Step 2: Update tests**

Rewrite tests to use read-only projection, root constructors, real transitions, and event methods.

Tests must not mutate sequence/window/phase/option/congestion/timer fields through connection-side field accessors.

### Task 7: Verification

**Files:**
- Verify all touched TCP files and tests

- [ ] **Step 1: Source scan for forbidden shapes**

Run:

```bash
rg -n 'as_.*_machine|TcpStateMachine::<(SynSent|SynRcvd|Established|CloseWait)::new|TcpActiveOpen|TcpOutputSendView|TcpStateSegment|TcpStateTransition|TcpStateMachineOutput|Disposition|enter_|emit_control|emit_|FnOnce|FnMut' crates/hammer-service/src/transport/tcp crates/hammer-service/tests
```

Expected:

```text
no matches
```

- [ ] **Step 2: Source scan for connection-side mutation hooks**

Run:

```bash
rg -n 'machine_|fn set_state|fn set_sequence_state|fn set_send_state|fn set_receive_state|fn connection_mut|fn option_state_mut|fn congestion_mut|fn retransmit_timeout_mut|fn accept_in_order_payload' crates/hammer-service/src/transport/tcp
```

Expected:

```text
no matches
```

- [ ] **Step 3: Source scan for public protocol fields**

Run:

```bash
rg -n 'pub .*iss:|pub .*irs:|pub .*snd_una:|pub .*snd_nxt:|pub .*snd_wnd:|pub .*rcv_nxt:|pub .*rcv_wnd:' crates/hammer-service/src/transport/tcp
```

Expected:

```text
no matches
```

- [ ] **Step 4: Source scan for node flag classification**

Run:

```bash
rg -n 'packet\\.flags.*SYN|packet\\.flags.*ACK|packet\\.flags.*RST|packet\\.flags.*FIN' crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/src/transport/tcp/syn_sent.rs crates/hammer-service/src/transport/tcp/rcv_process.rs crates/hammer-service/src/transport/tcp/established.rs
```

Expected:

```text
no matches
```

- [ ] **Step 5: Boundary scan**

Run:

```bash
rg -n 'AppOpId|AppRingHandle|SessionId|SessionQueue|BufferIndex|BufferFrame|alloc_tcp_segment' crates/hammer-service/src/transport/tcp/state_machine.rs
```

Expected:

```text
no matches
```

- [ ] **Step 6: Focused tests**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine
cargo test -p hammer-service --test tcp_connection_state
cargo test -p hammer-service --test tcp_passive_open
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service --test tcp_output
cargo test -p hammer-service --test tcp_congestion_node
cargo test -p hammer-service transport::tcp::session::tests
cargo test -p hammer-service transport::tcp::syn_sent::tests
```

Expected: every command exits 0.

---

## Self-Review Checklist

- The only state-machine carrier is `TcpStateMachine<S>`.
- `TcpStateMachine<S>` owns the private `TcpProtocolState`; concrete state structs own only phase-local data.
- `TcpConnectionState` is an erased runtime enum and read-only projection boundary.
- Non-root states are produced only by transition methods.
- No connection-side mutation hooks write TCP protocol fields.
- No mutable protocol sub-object is exposed through `TcpConnectionState`.
- Nodes advance typed machines; nodes do not manually set state.
- Nodes do not classify TCP flags into protocol transitions.
- Packet allocation stays outside `state_machine.rs`.
- No extra output/effect/disposition/transition/event/header wrapper type exists.
