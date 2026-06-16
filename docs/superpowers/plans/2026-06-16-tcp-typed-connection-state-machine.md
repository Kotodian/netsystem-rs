# TCP Typed Connection State Machine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor Hammer TCP so `TcpConnection<S>` is the only typed TCP state-machine carrier, packet nodes are fixed to one TCP state, and all protocol/session/index/timer decisions are driven by typed connection event methods.

**Architecture:** `TcpConnection<S>` owns connection identity, protocol fields, timers/options/congestion, and exactly one state marker `S`. `TcpConnectionState` remains only erased runtime storage for `SessionDriverRuntime`, with conversion in/out and no protocol-facing accessor surface. Node code extracts the exact typed connection by left-hand type, calls one typed event method, and handles only existing external work values such as `Option<TcpSegmentHeader>`.

**Tech Stack:** Rust 2024, existing Hammer packet graph nodes, existing `TcpPacket`, existing `TcpSegmentHeader`, existing session queue/runtime, and Fuchsia-inspired typestate transitions: consume current typed state, construct the concrete next typed state inside the owning event method, and let that event method install the next session state.

---

## Core Shape

The state machine is the generic connection:

```rust
pub struct TcpConnection<S> {
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
    congestion: TcpCongestionState,
    active_timers: u8,
    pending_timers: u8,
    next_output_at: Option<Instant>,
    state: S,
}
```

The only state types are marker structs:

```rust
Closed
Listen
SynSent
SynRcvd
Established
CloseWait
FinWait1
FinWait2
Closing
LastAck
TimeWait
```

There is no separate `TcpStateMachine<S>`, no phase enum, no transition output enum, no route/commit/store carrier, and no state-specific convenience wrapper.

## Non-Negotiable Rules

- Do not create a new worktree.
- Do not modify production code before this plan is accepted.
- Do not add `TcpActiveOpen`.
- Do not add `TcpStateMachine`.
- Do not add `TcpOutputSendView`.
- Do not add `TcpConnectionView` or `TcpConnectionState::view()`.
- Do not add `TcpStateSegment`.
- Do not add `TcpStateTransition`.
- Do not add `TcpStateMachineOutput`.
- Do not add disposition/effect/event/output wrapper enums.
- Do not add named route/key/commit/store carrier types for queue convenience.
- Do not add a `phase` enum/storage concept.
- Do not add `with_state`, `enter_*`, `expect_*`, `as_*`, `map_*`, or `process_*_packet`.
- Do not add `queue.put_connection(...)`.
- Do not add queue APIs named by storage mechanics such as `replace_erased`, `close_erased`, `index_*_fields`, `persist_*`, `store_*`, or `commit_*`.
- Do not let packet nodes match `TcpConnectionState`.
- Do not let packet nodes use `queue.take_connection::<State>(...)`; use the left-hand type.
- Do not let packet nodes branch on `TcpState`.
- Do not let packet nodes inspect TCP flags to choose protocol transitions.
- Do not expose constructors for non-root state markers.
- Do not add a generic state-changing helper such as `transition<T>(...)`.
- Do not add connection-side setters for TCP state, close reason, sequence numbers, windows, options, congestion, or timers.
- Do not add common delegate methods on `TcpConnectionState`, including `owner_worker()`, `next_node()`, `local()`, `remote()`, `snd_nxt()`, timer methods, congestion methods, option methods, or `state()`.
- Do not import app ring types or generic session runtime internals into `state_machine.rs`.
- Do not add a special output helper named `retransmit_syn_header_if_ready` or equivalent. Timer expiry is a TCP event, not a header getter.

Root construction uses left-hand type inference:

```rust
let connection: TcpConnection<Closed> =
    TcpConnection::new(None, worker, local.port(), Some(local), remote);

let listener: TcpConnection<Listen> =
    TcpConnection::new(None, worker, packet.local.port(), Some(packet.local), packet.remote);
```

Do not write:

```rust
TcpConnection::<Closed>::new(...)
TcpConnection::<Listen>::new(...)
TcpConnection::<SynSent>::new(...)
```

## File Responsibilities

- `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - Defines `TcpConnection<S>`.
  - Defines marker states.
  - Owns all TCP protocol fields.
  - Implements root construction for `Closed` and `Listen`.
  - Implements pure state-specific protocol transition methods that return concrete `TcpConnection<NextState>`.
  - Provides read-only getters on `TcpConnection<S>`, not on `TcpConnectionState`.
  - Provides per-state `state()` and `next_node()` on `TcpConnection<State>`.
  - Does not import `TcpSessionQueue`, app ring types, generic session runtime internals, or define runtime storage wrapper types.

- `crates/hammer-service/src/transport/tcp/connection.rs`
  - Keeps support structs already owned there: `TcpConnectionOptionState`, `TcpRetransmitTimeoutState`, and `TcpConnectionTimerKind`.
  - Defines `TcpConnectionState` as erased runtime storage only.
  - Implements `From<TcpConnection<State>> for TcpConnectionState`.
  - Implements `TryFrom<TcpConnectionState> for TcpConnection<State>`.
  - Implements only erased event dispatch methods that are explicitly TCP-owned, such as `on_tcp_timer_expiry(...)`; callers do not match storage variants.
  - Does not provide shared getters, mutable protocol delegates, or `view()`.

- `crates/hammer-service/src/transport/tcp/session.rs`
  - Keeps the existing session driver/runtime integration.
  - Provides typed `take_connection<S>()` for storage extraction.
  - Implements node-facing typed event methods on `TcpConnection<State>`: `connect`, `receive_syn`, `receive_open_reply`, `receive_final_ack`, `receive_data`, and close-state receive methods.
  - Those event methods take `&mut TcpSessionQueue` and `SessionId` when needed, call pure transitions from `state_machine.rs`, and directly update `TcpSessionQueue` private `driver`/`protocol` fields from inside `session.rs`.
  - `TcpConnection<Established>::receive_data` also performs accepted-payload delivery by advancing/truncating the current input buffer and calling `queue.enqueue_rx(...)` from inside the typed event method.
  - Does not add queue primitive APIs named by storage mechanics. Since event impls live in `session.rs`, they may use `queue.driver` and `queue.protocol` directly.

- `crates/hammer-service/src/transport/tcp/session_index.rs`
  - Keeps existing internal entry structs.
  - Adds `owner` and `next` metadata to existing entries.
  - Provides index APIs named by lookup behavior, not storage mechanics:
    - `remember_session(...)`
    - `remember_pending_open(...)`
    - `forget_session(...)`
    - `forget_pending_open(...)`
    - `lookup_by_tuple(...) -> Option<(SessionId, DataWorkerId, TcpInputNext)>`
    - `lookup_pending_by_tuple(...) -> Option<(SessionId, DataWorkerId, TcpInputNext)>`
  - Does not define new route/key/commit/store carrier types.

- `crates/hammer-service/src/transport/tcp/input.rs`
  - Routes existing sessions from index metadata.
  - Does not read `TcpConnectionState`.
  - Does not map `TcpState` to nodes.

- Packet nodes:
  - `listen.rs`: handles only `TcpConnection<Listen>`.
  - `syn_sent.rs`: handles only `TcpConnection<SynSent>`.
  - `syn_rcvd.rs`: handles only `TcpConnection<SynRcvd>`.
  - `established.rs`: handles only `TcpConnection<Established>`.
  - `close_wait.rs`: handles only `TcpConnection<CloseWait>`.
  - `fin_wait1.rs`: handles only `TcpConnection<FinWait1>`.
  - `fin_wait2.rs`: handles only `TcpConnection<FinWait2>`.
  - `closing.rs`: handles only `TcpConnection<Closing>`.
  - `last_ack.rs`: handles only `TcpConnection<LastAck>`.
  - `time_wait.rs`: handles only `TcpConnection<TimeWait>`.
  - Delete `rcv_process.rs` after the split.

## Node Contract

Allowed node shape:

```rust
let connection: TcpConnection<SynSent> = queue.take_connection(session_id)?;
let control = connection.receive_open_reply(queue, session_id, &packet)?;
```

Allowed node responsibilities after the event call:

```rust
if let Some(header) = control {
    let allocated = alloc_tcp_segment(
        runtime.packet_buffers(),
        tcp_segment_metadata(packet.local, packet.remote),
        header,
    )?;
    output_index = Some(allocated);
}
```

Allowed established receive shape:

```rust
let connection: TcpConnection<Established> = queue.take_connection(session_id)?;
let control = connection.receive_data(runtime, index, queue, session_id, &packet)?;
```

The typed event method delivers accepted payload itself. The node may allocate `control`; it must not decide whether the next state is closed, established, pending, live, indexed, app-completed, or whether a payload is accepted.

## Event Contract

Multi-branch packet events do not return a polymorphic next-state value to the node. The typed event method consumes `TcpConnection<CurrentState>`, chooses the TCP protocol branch internally, constructs the concrete next `TcpConnection<NextState>`, installs that next connection into `TcpSessionQueue`, and returns only existing external work data.

Example shape:

```rust
impl TcpConnection<SynSent> {
    pub(crate) fn receive_open_reply(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        // Branch on TCP flags and ACK validity here.
        // Construct TcpConnection<SynSent>, TcpConnection<SynRcvd>,
        // TcpConnection<Established>, or TcpConnection<Closed> here.
        // Update queue.driver and queue.protocol here.
        // Return only the control header the packet node may allocate.
    }
}
```

Timer expiry follows the same rule:

```rust
impl TcpConnectionState {
    pub(crate) fn on_tcp_timer_expiry(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<TcpSegmentHeader> {
        // Dispatch is encapsulated here.
        // Runtime does not match TcpConnectionState.
    }
}
```

Runtime timer code may call:

```rust
if let Some(connection) = driver.session_state_mut(expiry.session_id()) {
    let control = connection.on_tcp_timer_expiry(TcpConnectionTimerKind::Retransmit);
    // Runtime allocates returned TcpSegmentHeader if present.
}
```

Runtime timer code must not do:

```rust
let state = driver.take_session_state(session_id)?;
match state {
    TcpConnectionState::SynSent(connection) => {
        connection.on_retransmit_timeout(driver, protocol, session_id)?;
    }
    other => {
        driver.replace_session_state(session_id, other);
    }
}
```

## Node Coverage

`TcpInputNext` becomes:

```rust
#[hammer_component_macros::node_next]
pub enum TcpInputNext {
    Drop,
    Punt,
    Listen,
    SynSent,
    SynRcvd,
    Established,
    CloseWait,
    FinWait1,
    FinWait2,
    Closing,
    LastAck,
    TimeWait,
    Reset,
}
```

Per-state `next_node()` methods live on concrete typed connections:

```rust
TcpConnection<Closed>::next_node      -> TcpInputNext::Drop
TcpConnection<Listen>::next_node      -> TcpInputNext::Listen
TcpConnection<SynSent>::next_node     -> TcpInputNext::SynSent
TcpConnection<SynRcvd>::next_node     -> TcpInputNext::SynRcvd
TcpConnection<Established>::next_node -> TcpInputNext::Established
TcpConnection<CloseWait>::next_node   -> TcpInputNext::CloseWait
TcpConnection<FinWait1>::next_node    -> TcpInputNext::FinWait1
TcpConnection<FinWait2>::next_node    -> TcpInputNext::FinWait2
TcpConnection<Closing>::next_node     -> TcpInputNext::Closing
TcpConnection<LastAck>::next_node     -> TcpInputNext::LastAck
TcpConnection<TimeWait>::next_node    -> TcpInputNext::TimeWait
```

Packet node to typed method:

```rust
TcpListenNode      -> TcpConnection<Listen>::receive_syn
TcpSynSentNode     -> TcpConnection<SynSent>::receive_open_reply
TcpSynRcvdNode     -> TcpConnection<SynRcvd>::receive_final_ack
TcpEstablishedNode -> TcpConnection<Established>::receive_data
TcpCloseWaitNode   -> TcpConnection<CloseWait>::receive_close_wait
TcpFinWait1Node    -> TcpConnection<FinWait1>::receive_fin_wait1
TcpFinWait2Node    -> TcpConnection<FinWait2>::receive_fin_wait2
TcpClosingNode     -> TcpConnection<Closing>::receive_closing
TcpLastAckNode     -> TcpConnection<LastAck>::receive_last_ack
TcpTimeWaitNode    -> TcpConnection<TimeWait>::receive_time_wait
```

## Task 1: Reset Contract Tests To The Final Shape

**Files:**
- Modify: `crates/hammer-service/tests/tcp_state_machine.rs`

- [ ] **Step 1: Keep structural tests that forbid middle types**

Use source scans that check these names do not appear in TCP production source:

```rust
#[test]
fn tcp_state_machine_public_api_has_no_forbidden_middle_types() {
    let sources = [
        read_tcp_source("src/transport/tcp/connection.rs"),
        read_tcp_source("src/transport/tcp/state_machine.rs"),
        read_tcp_source("src/transport/tcp/session.rs"),
        read_tcp_source("src/transport/tcp/listen.rs"),
        read_tcp_source("src/transport/tcp/syn_sent.rs"),
        read_tcp_source("src/transport/tcp/established.rs"),
        read_tcp_source("src/transport/tcp/mod.rs"),
    ]
    .join("\n");

    let forbidden = [
        concat!("Tcp", "State", "Machine"),
        concat!("Tcp", "Connection", "View"),
        concat!("Tcp", "Output", "Send", "View"),
        concat!("Tcp", "State", "Segment"),
        concat!("Tcp", "State", "Transition"),
        concat!("Tcp", "State", "Machine", "Output"),
        concat!("Tcp", "Active", "Open"),
        concat!("Tcp", "Connection", "Route"),
        concat!("Tcp", "Connection", "Index", "Key"),
        concat!("Tcp", "Connection", "Queue", "Commit"),
        concat!("Tcp", "Connection", "Store"),
        concat!("Disposition"),
        concat!("Effect"),
    ];

    for pattern in forbidden {
        assert!(
            !sources.contains(pattern),
            "forbidden TCP state-machine helper remains: {pattern}"
        );
    }
}
```

- [ ] **Step 2: Add node responsibility tests**

Scan packet node source for forbidden state-driving patterns:

```rust
#[test]
fn packet_nodes_do_not_drive_tcp_queue_state() {
    let sources = [
        read_tcp_source("src/transport/tcp/listen.rs"),
        read_tcp_source("src/transport/tcp/syn_sent.rs"),
        read_tcp_source("src/transport/tcp/syn_rcvd.rs"),
        read_tcp_source("src/transport/tcp/established.rs"),
        read_tcp_source("src/transport/tcp/close_wait.rs"),
        read_tcp_source("src/transport/tcp/fin_wait1.rs"),
        read_tcp_source("src/transport/tcp/fin_wait2.rs"),
        read_tcp_source("src/transport/tcp/closing.rs"),
        read_tcp_source("src/transport/tcp/last_ack.rs"),
        read_tcp_source("src/transport/tcp/time_wait.rs"),
    ]
    .join("\n");

    let forbidden = [
        concat!("put", "_connection"),
        concat!("next", ".state()"),
        concat!("indexed", ".state()"),
        concat!("take_connection", "::"),
        concat!("TcpState::Closed"),
        concat!("TcpState::Established"),
        concat!("match next"),
        concat!("match connection"),
        concat!("TcpConnectionState::"),
    ];

    for pattern in forbidden {
        assert!(
            !sources.contains(pattern),
            "packet node still drives TCP state or queue policy: {pattern}"
        );
    }
}
```

- [ ] **Step 3: Add timer responsibility test**

Scan session runtime for the forbidden timer dispatch shape:

```rust
#[test]
fn tcp_timer_dispatch_is_owned_by_tcp_state() {
    let source = read_tcp_source("src/transport/tcp/session.rs");
    assert!(!source.contains("match state"));
    assert!(!source.contains("TcpConnectionState::SynSent"));
    assert!(!source.contains("on_retransmit_timeout"));
    assert!(!source.contains("retransmit_syn_header_if_ready"));
    assert!(source.contains("on_tcp_timer_expiry"));
}
```

- [ ] **Step 4: Run the failing target**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine
```

Expected before implementation: FAIL because the final API and split nodes are not yet fully implemented.

## Task 2: Put The Typed Carrier In `state_machine.rs`

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`

- [ ] **Step 1: Move `TcpConnection<S>` into `state_machine.rs`**

Define the marker states and carrier in `state_machine.rs`. Marker fields stay private:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    _private: (),
}
```

Repeat the same private marker shape for `Listen`, `SynSent`, `SynRcvd`, `Established`, `CloseWait`, `FinWait1`, `FinWait2`, `Closing`, `LastAck`, and `TimeWait`.

- [ ] **Step 2: Keep only root `new`**

Only `Closed` and `Listen` implement root construction through `TcpConnection::new(...)` and left-hand type inference:

```rust
impl<S: TcpRootState> TcpConnection<S> {
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        // Initializes protocol fields and S::new_root().
    }
}
```

`TcpRootState` is private to `state_machine.rs`. Do not expose non-root constructors.

- [ ] **Step 3: Use state-specific constructors for transitions**

Inside `state_machine.rs`, transition methods construct the next concrete type through state-specific private constructors. Do not use a generic `transition<T>`.

Allowed pattern:

```rust
impl TcpConnection<Closed> {
    pub(crate) fn connect_state(self, initial_sequence: u32) -> TcpConnection<SynSent> {
        TcpConnection::syn_sent_from_closed(self, initial_sequence)
    }
}

impl TcpConnection<SynSent> {
    fn syn_sent_from_closed(
        current: TcpConnection<Closed>,
        initial_sequence: u32,
    ) -> TcpConnection<SynSent> {
        // Move fields, set ISS/SND.UNA/SND.NXT, construct SynSent.
    }
}
```

Forbidden pattern:

```rust
impl<S> TcpConnection<S> {
    fn transition<T>(self, state: T) -> TcpConnection<T> {
        // Do not add this helper.
    }
}
```

- [ ] **Step 4: Add read-only typed getters only**

Keep immutable getters required by output, indexing, tests, and timers on `TcpConnection<S>`:

```rust
impl<S> TcpConnection<S> {
    pub const fn connection_id(&self) -> Option<TcpConnectionId> { self.connection_id }
    pub const fn owner_worker(&self) -> DataWorkerId { self.owner_worker }
    pub const fn local(&self) -> Option<SocketAddr> { self.local }
    pub const fn remote(&self) -> SocketAddr { self.remote }
    pub const fn iss(&self) -> u32 { self.iss }
    pub const fn snd_una(&self) -> u32 { self.snd_una }
    pub const fn snd_nxt(&self) -> u32 { self.snd_nxt }
    pub const fn rcv_nxt(&self) -> u32 { self.rcv_nxt }
}
```

Do not add setters or mutable field accessors.

- [ ] **Step 5: Add concrete `state()` and `next_node()` methods**

Use concrete impl blocks, not a runtime enum:

```rust
impl TcpConnection<SynSent> {
    pub const fn state(&self) -> TcpState { TcpState::SynSent }
    pub const fn next_node(&self) -> TcpInputNext { TcpInputNext::SynSent }
}
```

Repeat for each marker state. Do not add these methods to `TcpConnectionState`.

- [ ] **Step 6: Delete old carrier/view helpers**

Remove from `connection.rs`:

```rust
pub struct TcpConnection<S> { ... }
pub struct TcpConnectionView { ... }
impl TcpConnectionView { ... }
```

Re-export the carrier from `state_machine.rs` if existing imports expect `connection::TcpConnection`:

```rust
pub use super::state_machine::TcpConnection;
```

- [ ] **Step 7: Verify carrier shape**

Run:

```bash
rg -n "TcpStateMachine|TcpConnectionView|TcpOutputSendView|\\.view\\(|pub .*iss:|pub .*snd_una:|pub .*rcv_nxt:|transition<|self\\.transition" crates/hammer-service/src/transport/tcp
```

Expected: no matches.

## Task 3: Make `TcpConnectionState` Erased Storage Only

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`

- [ ] **Step 1: Define only the storage enum and conversions**

Keep this shape:

```rust
#[derive(Debug, Clone)]
pub enum TcpConnectionState {
    Closed(TcpConnection<Closed>),
    Listen(TcpConnection<Listen>),
    SynSent(TcpConnection<SynSent>),
    SynRcvd(TcpConnection<SynRcvd>),
    Established(TcpConnection<Established>),
    CloseWait(TcpConnection<CloseWait>),
    FinWait1(TcpConnection<FinWait1>),
    FinWait2(TcpConnection<FinWait2>),
    Closing(TcpConnection<Closing>),
    LastAck(TcpConnection<LastAck>),
    TimeWait(TcpConnection<TimeWait>),
}
```

Implement `From<TcpConnection<State>> for TcpConnectionState` and `TryFrom<TcpConnectionState> for TcpConnection<State>` for every marker state.

- [ ] **Step 2: Keep mismatch errors independent of `state()`**

The `TryFrom` error must not call `other.state()`. Use the enum variant name from the match arm:

```rust
Err(CoreError::internal("tcp connection state mismatch: expected SynSent"))
```

- [ ] **Step 3: Add erased TCP-owned timer dispatch only**

`TcpConnectionState` may encapsulate erased dispatch for timer events because that keeps runtime from matching storage variants:

```rust
impl TcpConnectionState {
    pub(crate) fn on_tcp_timer_expiry(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<TcpSegmentHeader> {
        match self {
            Self::SynSent(connection) => connection.on_tcp_timer_expiry(timer),
            _ => None,
        }
    }
}
```

This match is allowed only inside TCP-owned erased dispatch. Do not add generic getters or mutation delegates.

- [ ] **Step 4: Remove storage enum accessors**

Delete all `TcpConnectionState` methods that expose:

```text
state
owner_worker
next_node
local
remote
iss
snd_una
snd_nxt
rcv_nxt
timer/congestion/options mutation delegates
```

- [ ] **Step 5: Verify storage boundary**

Run:

```bash
rg -n "TcpConnectionView|\\.view\\(|pub fn owner_worker\\(&self\\)|pub fn next_node\\(&self\\)|pub fn local\\(&self\\)|pub fn remote\\(&self\\)|pub fn state\\(&self\\)|pub fn snd_nxt\\(&self\\)|pub fn tcp_timer_set\\(&mut self\\)" crates/hammer-service/src/transport/tcp/connection.rs
```

Expected: no protocol accessor/delegate surface on `TcpConnectionState`.

## Task 4: Move Queue Decisions Into Typed Connection Events

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`

- [ ] **Step 1: Keep typed extraction only**

`TcpSessionQueue` keeps typed extraction:

```rust
pub(crate) fn take_connection<S>(
    &mut self,
    session_id: SessionId,
) -> CoreResult<TcpConnection<S>>
where
    TcpConnection<S>: TryFrom<TcpConnectionState, Error = CoreError>;
```

Call sites must use:

```rust
let connection: TcpConnection<SynSent> = queue.take_connection(session_id)?;
```

Call sites must not use:

```rust
queue.take_connection::<SynSent>(session_id)?
```

- [ ] **Step 2: Delete generic queue put**

Remove:

```rust
pub(crate) fn put_connection<C>(&mut self, session_id: SessionId, connection: C)
where
    C: Into<TcpConnectionState>;
```

Do not replace it with a generic queue API.

- [ ] **Step 3: Implement active open as a connection event**

`TcpSessionQueue::connect` computes ISS and delegates:

```rust
pub(crate) fn connect(
    &mut self,
    local: SocketAddr,
    remote: SocketAddr,
) -> CoreResult<SessionId> {
    let iss = self.protocol.next_initial_sequence(local, remote);
    let connection: TcpConnection<Closed> =
        TcpConnection::new(None, self.worker(), local.port(), Some(local), remote);
    connection.connect(self, iss)
}
```

`TcpConnection<Closed>::connect` is implemented in `session.rs` and directly updates `queue.driver` and `queue.protocol`:

```rust
impl TcpConnection<Closed> {
    pub(crate) fn connect(
        self,
        queue: &mut TcpSessionQueue,
        initial_sequence: u32,
    ) -> CoreResult<SessionId> {
        let connection = self.connect_state(initial_sequence);
        let session_id = queue.driver.insert_session(connection.clone().into());
        queue.protocol.remember_pending_open(
            session_id,
            connection.local(),
            connection.remote(),
            connection.owner_worker(),
            connection.next_node(),
        );
        TcpSessionProtocol::arm_retransmit_timer_on_queue(queue, session_id, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
        queue.driver.mark_ready(session_id);
        Ok(session_id)
    }
}
```

Use a helper such as `arm_retransmit_timer_on_queue` only if it is a timer operation, not a storage/commit abstraction.

- [ ] **Step 4: Implement passive open as a connection event**

Node constructs `TcpConnection<Listen>` by left-hand type. `TcpConnection<Listen>::receive_syn` allocates the child session inside `insert_session_with_id`, installs `TcpConnection<SynRcvd>`, indexes it, arms retransmit, marks ready, and returns `Option<TcpSegmentHeader>`.

The node does not inspect the returned state because no state is returned.

- [ ] **Step 5: Implement `SynSent` branches as a connection event**

`TcpConnection<SynSent>::receive_open_reply(queue, session_id, packet)` handles:

- valid `SYN|ACK` -> build `TcpConnection<Established>`, cancel retransmit timer, remove pending index, remember live session, complete app connect, return ACK header;
- unacceptable ACK without RST -> keep or close according to current behavior and return RST header;
- acceptable `RST|ACK` -> close session, remove pending index, return no header;
- simultaneous open SYN -> build `TcpConnection<SynRcvd>`, remember live/pending state according to current behavior, return SYN|ACK header;
- ignored segment -> reinstall `TcpConnection<SynSent>` and return no header.

This method updates `queue.driver` and `queue.protocol` directly in `session.rs`. It does not return `TcpConnectionState`, a generic `Out`, or an enum.

- [ ] **Step 6: Implement `SynRcvd` final ACK as a connection event**

`TcpConnection<SynRcvd>::receive_final_ack(queue, session_id, packet)` handles:

- acceptable final ACK -> build `TcpConnection<Established>`, cancel retransmit timer, remember live session, return no header or current ACK behavior;
- RST -> close session and forget indexes;
- other segment -> reinstall `TcpConnection<SynRcvd>` or return the existing ACK header required by current behavior.

- [ ] **Step 7: Implement established and close-state events**

`TcpConnection<Established>::receive_data(runtime, index, queue, session_id, packet)` returns:

```rust
CoreResult<Option<TcpSegmentHeader>>
```

Use this signature:

```rust
impl TcpConnection<Established> {
    pub(crate) fn receive_data(
        self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        // TCP protocol decisions, state install/close, and payload delivery happen here.
    }
}
```

When payload is accepted, this method advances the current input buffer to `packet.payload_offset`, truncates it to `packet.payload_len`, and calls `queue.enqueue_rx(session_id, index, fin)`. It returns only the optional ACK/control header that the node may allocate. The node must treat successful payload delivery as consuming the input buffer and must not free `index` again.

Close-state events return:

```rust
CoreResult<Option<TcpSegmentHeader>>
```

Implement these methods:

```rust
TcpConnection<CloseWait>::receive_close_wait
TcpConnection<FinWait1>::receive_fin_wait1
TcpConnection<FinWait2>::receive_fin_wait2
TcpConnection<Closing>::receive_closing
TcpConnection<LastAck>::receive_last_ack
TcpConnection<TimeWait>::receive_time_wait
```

Each method installs the concrete next typed connection or closes the session from inside the method.

- [ ] **Step 8: Verify queue responsibility**

Run:

```bash
rg -n "put_connection|replace_erased|close_erased|index_.*_fields|persist_|store_|commit_|TcpConnectionRoute|TcpConnectionIndexKey|TcpConnectionQueueCommit|TcpConnectionStore|next\\.state\\(\\)|indexed\\.state\\(\\)|TcpState::Closed|TcpState::Established" crates/hammer-service/src/transport/tcp
```

Expected: no matches except legitimate `TcpState` constants inside typed state methods/tests.

## Task 5: Update Index Metadata Without New Carrier Types

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session_index.rs`
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`

- [ ] **Step 1: Extend existing entries**

Add owner/next metadata to existing entries:

```rust
struct TcpConnectionIndexEntry {
    session_id: SessionId,
    connection_id: Option<TcpConnectionId>,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    owner: DataWorkerId,
    next: TcpInputNext,
}

struct TcpPendingIndexEntry {
    id: SessionId,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    owner: DataWorkerId,
    next: TcpInputNext,
}
```

- [ ] **Step 2: Rename index APIs by domain behavior**

Use these index method names:

```rust
pub fn remember_session(
    &mut self,
    session_id: SessionId,
    connection_id: Option<TcpConnectionId>,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    owner: DataWorkerId,
    next: TcpInputNext,
);

pub fn remember_pending_open(
    &mut self,
    id: SessionId,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    owner: DataWorkerId,
    next: TcpInputNext,
);

pub fn forget_session(&mut self, session_id: SessionId);

pub fn forget_pending_open(&mut self, id: SessionId);
```

Do not add route/key carrier structs.

- [ ] **Step 3: Return routing metadata from lookup**

`lookup_by_tuple` returns routing metadata:

```rust
pub fn lookup_by_tuple(
    &self,
    local: SocketAddr,
    remote: SocketAddr,
) -> Option<(SessionId, DataWorkerId, TcpInputNext)>;
```

Pending lookup uses the same return shape:

```rust
pub fn lookup_pending_by_tuple(
    &self,
    local: SocketAddr,
    remote: SocketAddr,
) -> Option<(SessionId, DataWorkerId, TcpInputNext)>;
```

- [ ] **Step 4: Route input from index metadata**

`input.rs` resolves session routing from the index only:

```rust
let route = runtime
    .session_route_by_tuple(local, remote)
    .or_else(|| runtime.pending_route_by_tuple(local, remote));

let Some((_session_id, owner, next)) = route else {
    return Ok(None);
};

Ok(Some(TcpSessionInputEntry { owner, next }))
```

It must not read `TcpConnectionState` for owner or next.

- [ ] **Step 5: Verify routing source**

Run:

```bash
rg -n "session_state\\(session_id\\)|connection\\.owner_worker\\(\\)|connection\\.next_node\\(\\)|match .*TcpState|TcpInputNext::RcvProcess" crates/hammer-service/src/transport/tcp/input.rs crates/hammer-service/src/transport/tcp/session_index.rs
```

Expected: no input routing from erased storage and no `RcvProcess`.

## Task 6: Split `RcvProcess` Into Typed Nodes

**Files:**
- Delete: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Create: `crates/hammer-service/src/transport/tcp/syn_rcvd.rs`
- Create: `crates/hammer-service/src/transport/tcp/close_wait.rs`
- Create: `crates/hammer-service/src/transport/tcp/fin_wait1.rs`
- Create: `crates/hammer-service/src/transport/tcp/fin_wait2.rs`
- Create: `crates/hammer-service/src/transport/tcp/closing.rs`
- Create: `crates/hammer-service/src/transport/tcp/last_ack.rs`
- Create: `crates/hammer-service/src/transport/tcp/time_wait.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-service/tests/tcp_passive_open.rs`
- Modify: `crates/hammer-service/tests/tcp_established_receive.rs`

- [ ] **Step 1: Replace `TcpInputNext::RcvProcess`**

Remove `RcvProcess` and add:

```rust
SynRcvd,
CloseWait,
FinWait1,
FinWait2,
Closing,
LastAck,
TimeWait,
```

- [ ] **Step 2: Add `TcpSynRcvdNode`**

The packet body uses this shape:

```rust
let connection: TcpConnection<SynRcvd> = queue.take_connection(session_id)?;
let control = connection.receive_final_ack(queue, session_id, &packet)?;
```

Then allocate `control` if present. No state branch, no `TcpConnectionState`, no `TcpState`.

- [ ] **Step 3: Add close-state nodes**

Each close-state node uses the same pattern:

```rust
let connection: TcpConnection<CloseWait> = queue.take_connection(session_id)?;
let control = connection.receive_close_wait(queue, session_id, &packet)?;
```

Repeat for `FinWait1`, `FinWait2`, `Closing`, `LastAck`, and `TimeWait`.

- [ ] **Step 4: Update graph exports and registration**

Export the new node types from `mod.rs`, remove the old `rcv_process` module/export, and update all `TcpInputNext::nodes(...)` call sites.

- [ ] **Step 5: Verify split**

Run:

```bash
rg -n "RcvProcess|TcpRcvProcess" crates/hammer-service/src crates/hammer-service/tests
```

Expected: no matches.

## Task 7: Update Existing Packet Nodes

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`

- [ ] **Step 1: Update listen node**

Use:

```rust
let connection: TcpConnection<Listen> =
    TcpConnection::new(None, worker, packet.local.port(), Some(packet.local), packet.remote);
let control = connection.receive_syn(queue, &packet)?;
```

The typed method creates and installs `TcpConnection<SynRcvd>` and returns the SYN|ACK header.

- [ ] **Step 2: Update syn-sent node**

Use:

```rust
let connection: TcpConnection<SynSent> = queue.take_connection(session_id)?;
let control = connection.receive_open_reply(queue, session_id, &packet)?;
```

The node only allocates `control`.

- [ ] **Step 3: Update established node**

Use:

```rust
let connection: TcpConnection<Established> = queue.take_connection(session_id)?;
let control = connection.receive_data(runtime, index, queue, session_id, &packet)?;
```

The node allocates `control` if present. It does not branch on protocol state and does not perform payload acceptance decisions.

- [ ] **Step 4: Verify packet nodes**

Run:

```bash
rg -n "TcpState|next\\.state\\(\\)|indexed\\.state\\(\\)|take_connection::<|match next|match connection|put_connection|TcpConnectionState::" crates/hammer-service/src/transport/tcp/{listen.rs,syn_sent.rs,syn_rcvd.rs,established.rs,close_wait.rs,fin_wait1.rs,fin_wait2.rs,closing.rs,last_ack.rs,time_wait.rs}
```

Expected: no matches.

## Task 8: Move Timer Output Into TCP-Owned Events

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/connection.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`

- [ ] **Step 1: Add typed retransmit timer event**

Implement on `TcpConnection<SynSent>`:

```rust
impl TcpConnection<SynSent> {
    pub(crate) fn on_tcp_timer_expiry(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<TcpSegmentHeader> {
        if timer != TcpConnectionTimerKind::Retransmit {
            return None;
        }
        // Consume pending timer state, update RTO, keep retransmit active,
        // and return the SYN TcpSegmentHeader using existing header construction.
    }
}
```

This is an event method, not a special header getter.

- [ ] **Step 2: Encapsulate erased timer dispatch**

Implement on `TcpConnectionState`:

```rust
impl TcpConnectionState {
    pub(crate) fn on_tcp_timer_expiry(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<TcpSegmentHeader> {
        match self {
            Self::SynSent(connection) => connection.on_tcp_timer_expiry(timer),
            _ => None,
        }
    }
}
```

This is the only allowed erased timer match. It is TCP-owned and prevents runtime from matching state.

- [ ] **Step 3: Update session runtime timer path**

In `SessionQueueProtocol<TcpConnectionState> for TcpSessionProtocol`, timer expiry marks the session ready and records pending timer through the TCP event object. Ready-session handling calls `on_tcp_timer_expiry` and allocates the returned header.

Forbidden:

```rust
match state {
    TcpConnectionState::SynSent(connection) => ...
}
```

Allowed:

```rust
if let Some(connection) = driver.session_state_mut(session_id) {
    if let Some(header) = connection.on_tcp_timer_expiry(TcpConnectionTimerKind::Retransmit) {
        // allocate existing TcpSegmentHeader
    }
}
```

- [ ] **Step 4: Verify timer ownership**

Run:

```bash
rg -n "match state|TcpConnectionState::SynSent|on_retransmit_timeout|retransmit_syn_header_if_ready|connection\\.state\\(\\) != TcpState::SynSent|TcpState::SynSent" crates/hammer-service/src/transport/tcp/session.rs
```

Expected: no runtime state matching or state probes.

## Task 9: Preserve Existing Behavior

**Files:**
- Modify tests only as required by renamed/split nodes and typed API changes.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt --all
```

Expected: success.

- [ ] **Step 2: Run focused contract tests**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine
```

Expected: pass.

- [ ] **Step 3: Run existing behavior tests**

Run:

```bash
cargo test -p hammer-service --test tcp_connection_state
cargo test -p hammer-service --test tcp_passive_open
cargo test -p hammer-service --test tcp_established_receive
cargo test -p hammer-service transport::tcp::session::tests
cargo test -p hammer-service transport::tcp::syn_sent::tests
```

Expected: pass.

- [ ] **Step 4: Run structural scans**

Run:

```bash
rg -n "TcpActiveOpen|TcpOutputSendView|TcpStateSegment|TcpStateTransition|TcpStateMachineOutput|Disposition|Effect|Event|TcpConnectionView|\\.view\\(|with_state|enter_|expect_|map_.*_once|take_connection::<|put_connection|replace_erased|close_erased|index_.*_fields|persist_|store_|commit_|next\\.state\\(\\)|indexed\\.state\\(\\)|TcpInputNext::RcvProcess|TcpRcvProcess|transition<|self\\.transition|retransmit_syn_header_if_ready|on_retransmit_timeout" crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_state_machine.rs
```

Expected: no matches except forbidden-string tests that intentionally use `concat!`.

- [ ] **Step 5: Run app/runtime isolation scan**

Run:

```bash
rg -n "TcpConnection<|TcpConnectionState|TcpInputNext|SynSent|Established|CloseWait" crates/hammer-service/src/session crates/hammer-runtime/src/app
```

Expected: no TCP state-machine types leak into generic session or app runtime code.

## Self-Review Checklist

- [ ] `TcpConnection<S>` is the only generic TCP state-machine carrier.
- [ ] State markers are the only state types.
- [ ] `TcpConnectionState` is erased runtime storage plus conversions and TCP-owned erased event dispatch only.
- [ ] `TcpConnectionState` has no view and no common protocol accessors.
- [ ] Root construction uses left-hand type inference.
- [ ] Non-root states are constructed only inside typed transition methods.
- [ ] No generic state-changing helper exists.
- [ ] Every packet-receiving state has a dedicated `TcpInputNext` variant and node.
- [ ] `RcvProcess` is removed.
- [ ] Packet nodes do not match, branch on `TcpState`, inspect flags, or commit queue state.
- [ ] Queue/index/app/timer effects are driven by concrete `TcpConnection<State>` methods.
- [ ] No queue primitive API is named by erased storage mechanics.
- [ ] No new wrapper/middle result type is added for transition output.
- [ ] Timer expiry is a TCP-owned event, not a runtime state match and not a special SYN-header getter.
- [ ] Existing active open, passive open, established receive, close-state behavior, timers, and app completions remain green.
