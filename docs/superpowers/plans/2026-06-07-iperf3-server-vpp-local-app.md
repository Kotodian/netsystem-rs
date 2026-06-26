# Hammer Local TCP App + iperf3 Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Hammer-internal local TCP application surface and implement an `iperf3` TCP server on top of it, without using VCL or any external socket compatibility layer.

**Architecture:** Keep the current VPP-style path `ip-local -> tcp-input -> tcp-listen / tcp-established -> tcp-rcv-process`, but stop treating `tcp-rcv-process` as a sink. Instead, make TCP local delivery a first-class Hammer packet-graph feature: listener lookup remains data-plane-facing and worker-owned, `tcp-rcv-process` advances connection state and reassembles payload, then dispatches “connection event + readable bytes” to a registered app node. App nodes never touch raw TCP packets directly; they emit write/close intents that the TCP local stack converts into outbound TCP/IP packets and forwards through the existing `ip-lookup -> adjacency-rewrite -> interface-output` graph.

**Tech Stack:** Rust 2024, `hammer-service` internal nodes, `hammer-adapter` packet buffers and packet opaque metadata, `hammer-infra::map::FlatHashTable`, `arc-swap`, `ipnet`, `smoltcp 0.12`, local `iperf3 3.18` interoperability target, VPP host-stack-inspired local delivery shape

---

## Scope Guardrails

- This work is **not** a new inbound protocol and **not** a proxy/inbound feature.
- Do **not** add VCL, LD_PRELOAD, external socket emulation, or a userland `socket()` API.
- The first app is `iperf3` TCP server only. Do **not** implement UDP, `-R`, bidirectional, multi-stream, or JSON result output in v1.
- The app interface is internal to Hammer packet graph code. Do **not** extend TOML config in this plan.
- Listener/connection hot paths must stay data-plane-friendly: use `FlatHashTable`, worker ownership, and packet opaque metadata instead of `Mutex`/`RwLock`/`HashMap` in per-packet lookup/mutation paths.
- TCP local delivery must reuse the runtime-owned barrier model already used for interface/FIB publication. Do not invent a service-local synchronization model.

## File Map

### Create
- `crates/hammer-service/src/transport/tcp/app.rs`
- `crates/hammer-service/src/transport/tcp/local.rs`
- `crates/hammer-service/src/transport/tcp/segment.rs`
- `crates/hammer-service/src/transport/tcp/output.rs`
- `crates/hammer-service/src/transport/tcp/iperf3.rs`
- `crates/hammer-service/tests/tcp_local_app_nodes.rs`
- `crates/hammer-service/tests/iperf3_server.rs`

### Modify
- `crates/hammer-service/src/transport/tcp/mod.rs`
- `crates/hammer-service/src/transport/tcp/input.rs`
- `crates/hammer-service/src/transport/tcp/lookup.rs`
- `crates/hammer-service/src/transport/tcp/listen.rs`
- `crates/hammer-service/src/transport/tcp/established.rs`
- `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- `crates/hammer-service/src/service.rs`
- `crates/hammer-service/src/lib.rs`
- `crates/hammer-service/Cargo.toml`
- `crates/hammer-adapter/src/rule.rs`
- `crates/hammer-adapter/tests/packet_buffer.rs`

### Responsibility Notes
- `app.rs`: public internal surface for local TCP apps: ids, events, write intents, registration contract.
- `local.rs`: worker-owned listener/connection registries, connection ids, control-plane publication, app lookup snapshots.
- `segment.rs`: packet opaque payloads and TCP local-delivery per-buffer metadata; no protocol parsing beyond what the local stack needs.
- `output.rs`: packet emission helpers for SYN-ACK/ACK/data/FIN/RST and routing of emitted packets through existing egress nodes.
- `iperf3.rs`: `iperf3` control-state parser, server session state, control/data stream coordination, and app node implementation.
- `tcp_local_app_nodes.rs`: unit/integration tests for listener registration, connection event dispatch, writeback, and app-node routing.
- `iperf3_server.rs`: end-to-end Hammer-local `iperf3` server tests, including launching the real `/opt/homebrew/bin/iperf3` client in a subprocess against the Hammer data path.

## Public/Internal Interface Additions

- `hammer_service::transport::tcp::TcpLocalAppControlPlane`
- `hammer_service::transport::tcp::TcpLocalAppNode`
- `hammer_service::transport::tcp::TcpAppEvent`
- `hammer_service::transport::tcp::TcpAppWriteRequest`
- `hammer_service::transport::tcp::TcpConnectionId`
- `hammer_service::transport::tcp::TcpListenerId`
- `hammer_service::transport::tcp::Iperf3ServerControlPlane`
- `hammer_service::RuntimeService::register_local_tcp_app(...)`
- `hammer_service::RuntimeService::register_iperf3_server(...)`

The app-facing API is internal crate/service API, not a generic OS socket API:
- `Open` event when a listener SYN completes into an accepted connection
- `Readable` event carrying reassembled bytes for one connection
- `Writable` event when queued bytes may be accepted again
- `Closed` event with peer/local/error reason
- `TcpAppWriteRequest` for app node to queue bytes or emit a local close

## Data Model Decisions

- `RouteMetadata` stays for route/FIB/domain context; do not overload it with transient TCP connection bookkeeping.
- Add a `PrimaryOpaquePayload` or `SecondaryOpaquePayload` for TCP local-delivery metadata, at minimum:
  - `listener_id`
  - `connection_id`
  - `app_id`
  - `owner_worker`
  - `event_kind`
  - `readable_len`
  - `flags` for FIN/RST/control/data classification
- Continue using `TcpLookupSnapshot` / `TcpWorkerOwnedState` for numeric listener/connection lookup.
- Add a separate local-app registry snapshot keyed by listener id and/or app id, published via `ArcSwap`.

## Task 1: Add packet-opaque metadata and app/event surface for local TCP delivery

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/app.rs`
- Create: `crates/hammer-service/src/transport/tcp/segment.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-adapter/tests/packet_buffer.rs`
- Test: `crates/hammer-service/tests/tcp_local_app_nodes.rs`

- [ ] **Step 1: Write the failing opaque/event tests**

```rust
#[test]
fn tcp_local_delivery_opaque_round_trips_connection_and_app_fields() {
    let value = TcpLocalDeliveryOpaque {
        listener_id: 7,
        connection_id: 41,
        app_id: 3,
        owner_worker: 1,
        readable_len: 1448,
        flags: TcpLocalDeliveryFlags::READABLE.bits(),
        event: TcpAppEventKind::Readable as u8,
    };

    let mut opaque = hammer_adapter::PrimaryOpaque::default();
    opaque.write(&value);
    let decoded = opaque.read::<TcpLocalDeliveryOpaque>();

    assert_eq!(decoded, value);
}

#[test]
fn tcp_app_event_reports_open_and_readable_payload_shape() {
    let event = TcpAppEvent::Readable {
        connection: TcpConnectionId::new(41),
        listener: TcpListenerId::new(7),
        bytes: b"hello".to_vec(),
        fin: false,
    };

    match event {
        TcpAppEvent::Readable { connection, listener, bytes, fin } => {
            assert_eq!(connection, TcpConnectionId::new(41));
            assert_eq!(listener, TcpListenerId::new(7));
            assert_eq!(bytes, b"hello");
            assert!(!fin);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}
```

- [ ] **Step 2: Run the first focused test to verify it fails**

Run: `cargo test -p hammer-service tcp_local_delivery_opaque_round_trips_connection_and_app_fields -- --exact`

Expected: FAIL because `app.rs` / `segment.rs` and the opaque payload type do not exist.

- [ ] **Step 3: Add the module wiring**

```rust
// crates/hammer-service/src/transport/tcp/mod.rs
pub mod app;
pub mod established;
pub mod input;
pub mod iperf3;
pub mod listen;
pub mod local;
pub mod lookup;
pub mod output;
pub mod rcv_process;
pub mod reset;
pub mod segment;
pub mod state;
pub mod syn_sent;

pub use app::{
    TcpAppCloseReason, TcpAppEvent, TcpAppEventKind, TcpAppWriteRequest, TcpConnectionId,
    TcpListenerId, TcpLocalAppControlPlane, TcpLocalAppNode,
};
pub use segment::{TcpLocalDeliveryFlags, TcpLocalDeliveryOpaque};
```

- [ ] **Step 4: Define the id and event surface**

```rust
// crates/hammer-service/src/transport/tcp/app.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TcpListenerId(u32);

impl TcpListenerId {
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TcpConnectionId(u32);

impl TcpConnectionId {
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TcpAppEventKind {
    Open = 1,
    Readable = 2,
    Writable = 3,
    Closed = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpAppCloseReason {
    PeerFin,
    PeerRst,
    LocalClose,
    ProtocolError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpAppEvent {
    Open {
        connection: TcpConnectionId,
        listener: TcpListenerId,
    },
    Readable {
        connection: TcpConnectionId,
        listener: TcpListenerId,
        bytes: std::vec::Vec<u8>,
        fin: bool,
    },
    Writable {
        connection: TcpConnectionId,
    },
    Closed {
        connection: TcpConnectionId,
        reason: TcpAppCloseReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpAppWriteRequest {
    Write {
        connection: TcpConnectionId,
        bytes: std::vec::Vec<u8>,
    },
    Close {
        connection: TcpConnectionId,
    },
}
```

- [ ] **Step 5: Define the packet opaque payload**

```rust
// crates/hammer-service/src/transport/tcp/segment.rs
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TcpLocalDeliveryFlags: u16 {
        const OPEN = 0x0001;
        const READABLE = 0x0002;
        const WRITABLE = 0x0004;
        const CLOSED = 0x0008;
        const FIN = 0x0010;
        const RST = 0x0020;
        const CONTROL = 0x0040;
        const DATA = 0x0080;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TcpLocalDeliveryOpaque {
    pub listener_id: u32,
    pub connection_id: u32,
    pub app_id: u32,
    pub owner_worker: u16,
    pub readable_len: u16,
    pub flags: u16,
    pub event: u8,
}

impl hammer_adapter::PrimaryOpaquePayload for TcpLocalDeliveryOpaque {
    fn encode_primary(&self) -> [u64; 5] {
        [
            u64::from(self.listener_id),
            u64::from(self.connection_id),
            u64::from(self.app_id),
            (u64::from(self.owner_worker) << 32)
                | (u64::from(self.readable_len) << 16)
                | u64::from(self.flags),
            u64::from(self.event),
        ]
    }

    fn decode_primary(words: [u64; 5]) -> Self {
        Self {
            listener_id: words[0] as u32,
            connection_id: words[1] as u32,
            app_id: words[2] as u32,
            owner_worker: (words[3] >> 32) as u16,
            readable_len: (words[3] >> 16) as u16,
            flags: words[3] as u16,
            event: words[4] as u8,
        }
    }
}
```

- [ ] **Step 6: Add adapter-level opaque round-trip tests**

```rust
#[test]
fn tcp_local_delivery_opaque_round_trips_words() {
    let payload = TcpLocalDeliveryOpaque {
        listener_id: 9,
        connection_id: 77,
        app_id: 4,
        owner_worker: 1,
        readable_len: 1024,
        flags: TcpLocalDeliveryFlags::READABLE.bits(),
        event: TcpAppEventKind::Readable as u8,
    };

    let mut opaque = PrimaryOpaque::default();
    opaque.write(&payload);
    assert_eq!(opaque.read::<TcpLocalDeliveryOpaque>(), payload);
}
```

- [ ] **Step 7: Run the focused tests**

Run: `cargo test -p hammer-service tcp_local_delivery_opaque_round_trips_connection_and_app_fields -- --exact`

Expected: PASS

Run: `cargo test -p hammer-adapter tcp_local_delivery_opaque_round_trips_words -- --exact`

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/transport/tcp/app.rs crates/hammer-service/src/transport/tcp/segment.rs crates/hammer-adapter/tests/packet_buffer.rs crates/hammer-service/tests/tcp_local_app_nodes.rs
git commit -m "hammer-service(Feat): add tcp local app event surface"
```

---

## Task 2: Add listener/app registry control plane and worker-owned connection records

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/local.rs`
- Modify: `crates/hammer-service/src/transport/tcp/lookup.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Test: `crates/hammer-service/tests/tcp_local_app_nodes.rs`

- [ ] **Step 1: Write the failing listener/app registry tests**

```rust
#[test]
fn local_tcp_control_plane_registers_listener_and_resolves_app() {
    let control = TcpLocalAppControlPlane::new();
    let app = hammer_adapter::NodeId::new(19);

    let listener = control
        .register_listener_v4(
            std::net::Ipv4Addr::new(192, 0, 2, 10),
            5201,
            app,
        )
        .expect("register listener");

    let resolved = control
        .snapshot()
        .lookup_listener_v4(std::net::Ipv4Addr::new(192, 0, 2, 10), 5201)
        .expect("listener hit");

    assert_eq!(listener, TcpListenerId::new(0));
    assert_eq!(resolved.listener, listener);
    assert_eq!(resolved.app_node, app);
}

#[test]
fn worker_owned_tcp_state_prefers_connection_before_listener_lookup() {
    let mut state = TcpWorkerOwnedState::new(hammer_adapter::DataWorkerId::new(1));
    state.insert_listener_v4(
        TcpV4ListenerKey::new(0, std::net::Ipv4Addr::new(192, 0, 2, 10), 5201),
        11,
    );
    state.insert_connection_v4(
        TcpV4ConnectionKey::new(
            0,
            std::net::Ipv4Addr::new(192, 0, 2, 10),
            5201,
            std::net::Ipv4Addr::new(198, 51, 100, 10),
            40000,
        ),
        41,
    );

    let snapshot = state.publish_snapshot();
    let hit = snapshot.lookup_v4(
        TcpV4ConnectionKey::new(
            0,
            std::net::Ipv4Addr::new(192, 0, 2, 10),
            5201,
            std::net::Ipv4Addr::new(198, 51, 100, 10),
            40000,
        ),
        TcpV4ListenerKey::new(0, std::net::Ipv4Addr::new(192, 0, 2, 10), 5201),
    );

    assert!(matches!(hit, Some(value) if value.kind == TcpLookupKind::EstablishedConnection));
}
```

- [ ] **Step 2: Run one focused test to verify it fails**

Run: `cargo test -p hammer-service local_tcp_control_plane_registers_listener_and_resolves_app -- --exact`

Expected: FAIL because `TcpLocalAppControlPlane` does not exist.

- [ ] **Step 3: Define the local app registry snapshot**

```rust
// crates/hammer-service/src/transport/tcp/local.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpLocalAppBinding {
    pub listener: TcpListenerId,
    pub app_node: hammer_adapter::NodeId,
}

#[derive(Debug, Clone)]
pub struct TcpLocalAppSnapshot {
    listeners_v4: hammer_infra::map::FlatHashTable<TcpV4ListenerKey, TcpLocalAppBinding>,
    listeners_v6: hammer_infra::map::FlatHashTable<TcpV6ListenerKey, TcpLocalAppBinding>,
}

impl TcpLocalAppSnapshot {
    pub fn empty() -> Self {
        Self {
            listeners_v4: hammer_infra::map::FlatHashTable::new(),
            listeners_v6: hammer_infra::map::FlatHashTable::new(),
        }
    }

    pub fn lookup_listener_v4(
        &self,
        local_addr: std::net::Ipv4Addr,
        local_port: u16,
    ) -> Option<TcpLocalAppBinding> {
        self.listeners_v4.lookup(&TcpV4ListenerKey::new(0, local_addr, local_port))
    }
}
```

- [ ] **Step 4: Define the control plane and id allocation**

```rust
pub struct TcpLocalAppControlPlane {
    next_listener_id: std::sync::atomic::AtomicU32,
    inner: std::sync::Arc<arc_swap::ArcSwap<TcpLocalAppSnapshot>>,
}

impl TcpLocalAppControlPlane {
    pub fn new() -> Self {
        Self {
            next_listener_id: std::sync::atomic::AtomicU32::new(0),
            inner: std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(
                TcpLocalAppSnapshot::empty(),
            )),
        }
    }

    pub fn register_listener_v4(
        &self,
        local_addr: std::net::Ipv4Addr,
        local_port: u16,
        app_node: hammer_adapter::NodeId,
    ) -> hammer_core::error::CoreResult<TcpListenerId> {
        let id = TcpListenerId::new(
            self.next_listener_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );
        self.inner.rcu(|current| {
            let mut next = TcpLocalAppSnapshot::clone(current);
            next.listeners_v4.insert(
                TcpV4ListenerKey::new(0, local_addr, local_port),
                TcpLocalAppBinding {
                    listener: id,
                    app_node,
                },
            );
            next
        });
        Ok(id)
    }

    pub fn snapshot(&self) -> std::sync::Arc<TcpLocalAppSnapshot> {
        self.inner.load_full()
    }
}
```

- [ ] **Step 5: Extend worker-owned connection state with local-delivery records**

```rust
// crates/hammer-service/src/transport/tcp/local.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpLocalConnectionRecord {
    pub connection: TcpConnectionId,
    pub listener: TcpListenerId,
    pub app_node: hammer_adapter::NodeId,
    pub owner_worker: hammer_adapter::DataWorkerId,
}
```

Add worker-owned helpers:

```rust
impl TcpWorkerOwnedState {
    pub fn insert_local_connection_v4(
        &mut self,
        key: TcpV4ConnectionKey,
        connection_id: TcpConnectionId,
    ) {
        self.insert_connection_v4(key, connection_id.get());
    }
}
```

Do not replace `TcpLookupSnapshot`; keep it as the numeric hot-path lookup surface used by `tcp-input`.

- [ ] **Step 6: Run the focused tests**

Run: `cargo test -p hammer-service local_tcp_control_plane_registers_listener_and_resolves_app -- --exact`

Expected: PASS

Run: `cargo test -p hammer-service worker_owned_tcp_state_prefers_connection_before_listener_lookup -- --exact`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/transport/tcp/local.rs crates/hammer-service/src/transport/tcp/lookup.rs crates/hammer-service/tests/tcp_local_app_nodes.rs
git commit -m "hammer-service(Feat): add tcp local app registry"
```

---

## Task 3: Teach `tcp-listen` to accept listeners into local connections and stamp packet opaque metadata

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/segment.rs`
- Modify: `crates/hammer-adapter/src/rule.rs`
- Test: `crates/hammer-service/tests/tcp_local_app_nodes.rs`

- [ ] **Step 1: Write the failing accept-path tests**

```rust
#[test]
fn tcp_listen_accepts_registered_listener_and_stamps_open_event() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = TcpLocalAppGraph::new(&runtime);

    graph
        .local_apps
        .register_listener_v4(std::net::Ipv4Addr::new(192, 0, 2, 10), 5201, graph.app_node)
        .expect("register app listener");

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_tcp_packet(
        std::net::Ipv4Addr::new(198, 51, 100, 10),
        40000,
        std::net::Ipv4Addr::new(192, 0, 2, 10),
        5201,
        tcp_flags(false, true, false, false),
        b"",
    );
    push_tcp_packet(
        &runtime,
        frame,
        &packet,
        std::net::Ipv4Addr::new(198, 51, 100, 10).into(),
        40000,
        std::net::Ipv4Addr::new(192, 0, 2, 10).into(),
        5201,
    );

    assert!(runtime.schedule_frame(graph.tcp_input, frame).expect("schedule"));
    assert!(runtime.run_ready_nodes().expect("run nodes") >= 3);

    let open = graph.app_state.lock().unwrap().events[0].clone();
    assert!(matches!(open, TcpAppEvent::Open { .. }));
}

#[test]
fn tcp_listen_inserts_established_flow_for_subsequent_ack_lookup() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = TcpLocalAppGraph::new(&runtime);
    let listener = graph
        .local_apps
        .register_listener_v4(std::net::Ipv4Addr::new(192, 0, 2, 10), 5201, graph.app_node)
        .expect("listener");

    graph.synthetic_accept(listener, std::net::Ipv4Addr::new(198, 51, 100, 10), 40000);

    let hit = graph.lookup_snapshot().lookup_v4(
        TcpV4ConnectionKey::new(
            0,
            std::net::Ipv4Addr::new(192, 0, 2, 10),
            5201,
            std::net::Ipv4Addr::new(198, 51, 100, 10),
            40000,
        ),
        TcpV4ListenerKey::new(0, std::net::Ipv4Addr::new(192, 0, 2, 10), 5201),
    );

    assert!(matches!(hit, Some(value) if value.kind == TcpLookupKind::EstablishedConnection));
}
```

- [ ] **Step 2: Run one focused test to verify it fails**

Run: `cargo test -p hammer-service tcp_listen_accepts_registered_listener_and_stamps_open_event -- --exact`

Expected: FAIL because the app registry is not connected to `tcp-listen`.

- [ ] **Step 3: Add local-app-aware control data to `TcpInputControlPlane`**

```rust
pub struct TcpInputControlPlane {
    inner: Arc<ArcSwap<TcpInputSnapshot>>,
    local_apps: Option<TcpLocalAppControlPlane>,
    next: [NodeId; TcpInputNext::COUNT],
}

impl TcpInputControlPlane {
    pub fn with_local_apps(mut self, local_apps: TcpLocalAppControlPlane) -> Self {
        self.local_apps = Some(local_apps);
        self
    }
}
```

The snapshot stays focused on dispatch/lookup; app registry access may remain a separate handle so listener publication is independent.

- [ ] **Step 4: Stamp accepted-listener metadata in `tcp-listen`**

Inside `tcp-listen`, change “blind pass-through to accept next” into:
- read the listener binding from the local app registry
- allocate a `TcpConnectionId`
- insert the connection into worker-owned lookup state
- write `TcpLocalDeliveryOpaque { listener_id, connection_id, app_id, owner_worker, event = Open, flags = OPEN }`
- route accepted packets toward `tcp-rcv-process` instead of directly to the app node

Minimal skeleton:

```rust
fn mark_listener_accept(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    accepted: AcceptedTcpLocalFlow,
) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer
        .packet_buffer_header_mut()
        .opaque
        .write(&TcpLocalDeliveryOpaque {
            listener_id: accepted.listener.get(),
            connection_id: accepted.connection.get(),
            app_id: accepted.app_node.slot(),
            owner_worker: accepted.owner_worker.slot() as u16,
            readable_len: 0,
            flags: TcpLocalDeliveryFlags::OPEN.bits(),
            event: TcpAppEventKind::Open as u8,
        });
    Ok(())
}
```

If the current buffer accessors do not expose opaque mutation through `Buffer`, add the smallest direct `Buffer` method required for opaque writes and cover it with a packet-buffer test.

- [ ] **Step 5: Add/adjust packet-buffer helper tests if needed**

```rust
#[test]
fn buffer_can_write_primary_opaque_payload() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_capacities(128, 4, 4, 1);
    let index = runtime
        .alloc_index_with_bytes(hammer_adapter::RouteMetadata::default(), b"opaque")
        .expect("alloc buffer");

    {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
        buffer.write_primary_opaque(&TcpLocalDeliveryOpaque {
            listener_id: 1,
            connection_id: 2,
            app_id: 3,
            owner_worker: 0,
            readable_len: 4,
            flags: TcpLocalDeliveryFlags::READABLE.bits(),
            event: TcpAppEventKind::Readable as u8,
        });
    }

    let buffer = runtime.get_buffer(index).expect("buffer");
    assert_eq!(
        buffer.read_primary_opaque::<TcpLocalDeliveryOpaque>().connection_id,
        2
    );
}
```

- [ ] **Step 6: Run the focused tests**

Run: `cargo test -p hammer-service tcp_listen_accepts_registered_listener_and_stamps_open_event -- --exact`

Expected: PASS

Run: `cargo test -p hammer-service tcp_listen_inserts_established_flow_for_subsequent_ack_lookup -- --exact`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/input.rs crates/hammer-service/src/transport/tcp/listen.rs crates/hammer-service/src/transport/tcp/segment.rs crates/hammer-adapter/src/rule.rs crates/hammer-adapter/tests/packet_buffer.rs crates/hammer-service/tests/tcp_local_app_nodes.rs
git commit -m "hammer-service(Feat): accept tcp listeners into local app flows"
```

---

## Task 4: Replace `tcp-rcv-process` drop behavior with readable-event delivery into app nodes

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/segment.rs`
- Test: `crates/hammer-service/tests/tcp_local_app_nodes.rs`

- [ ] **Step 1: Write the failing readable-event tests**

```rust
#[test]
fn tcp_rcv_process_dispatches_reassembled_bytes_to_app_node() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = TcpLocalAppGraph::new(&runtime);
    let flow = graph.accepted_flow_for(std::net::Ipv4Addr::new(192, 0, 2, 10), 5201);

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_tcp_packet(
        std::net::Ipv4Addr::new(198, 51, 100, 10),
        40000,
        std::net::Ipv4Addr::new(192, 0, 2, 10),
        5201,
        tcp_flags(false, false, false, true),
        b"hello-iperf",
    );
    push_established_packet(&runtime, frame, &packet, flow);

    assert!(runtime.schedule_frame(graph.rcv_process, frame).expect("schedule"));
    assert!(runtime.run_ready_nodes().expect("run") >= 2);

    let events = &graph.app_state.lock().unwrap().events;
    assert!(matches!(
        &events[0],
        TcpAppEvent::Readable { bytes, .. } if bytes == b"hello-iperf"
    ));
}

#[test]
fn tcp_rcv_process_marks_fin_as_readable_then_closed() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = TcpLocalAppGraph::new(&runtime);
    let flow = graph.accepted_flow_for(std::net::Ipv4Addr::new(192, 0, 2, 10), 5201);

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_tcp_packet(
        std::net::Ipv4Addr::new(198, 51, 100, 10),
        40000,
        std::net::Ipv4Addr::new(192, 0, 2, 10),
        5201,
        tcp_flags(true, false, false, true),
        b"tail",
    );
    push_established_packet(&runtime, frame, &packet, flow);

    assert!(runtime.schedule_frame(graph.rcv_process, frame).expect("schedule"));
    assert!(runtime.run_ready_nodes().expect("run") >= 2);

    let events = graph.app_state.lock().unwrap().events.clone();
    assert!(events.iter().any(|event| matches!(event, TcpAppEvent::Readable { fin: true, .. })));
    assert!(events.iter().any(|event| matches!(event, TcpAppEvent::Closed { reason: TcpAppCloseReason::PeerFin, .. })));
}
```

- [ ] **Step 2: Run one focused test to verify it fails**

Run: `cargo test -p hammer-service tcp_rcv_process_dispatches_reassembled_bytes_to_app_node -- --exact`

Expected: FAIL because `tcp-rcv-process` still drops.

- [ ] **Step 3: Convert `tcp-rcv-process` from sink to dispatcher**

Replace the current implementation:

```rust
let drop_next = next[TcpRcvProcessNext::Drop as usize];
```

with a local-delivery dispatcher that:
- reads `TcpLocalDeliveryOpaque`
- resolves the destination app node from `app_id`
- extracts current payload bytes from the current packet chain
- rewrites opaque flags/event to `Readable`
- routes the packet index to the app node

Minimal shape:

```rust
#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    Drop,
    AppDispatch,
}
```

```rust
fn next_node_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    registry: &TcpLocalAppRuntime,
    next: &[NodeId; TcpRcvProcessNext::COUNT],
) -> CoreResult<Option<NodeId>> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let opaque = buffer.read_primary_opaque::<TcpLocalDeliveryOpaque>();
    if opaque.app_id == 0 && opaque.listener_id == 0 && opaque.connection_id == 0 {
        runtime.free_index(index);
        return Ok(Some(next[TcpRcvProcessNext::Drop.slot()]));
    }
    let app = registry
        .resolve_app_node(opaque.app_id)
        .ok_or_else(|| hammer_core::error::CoreError::internal("missing tcp local app node"))?;
    let current_len = u16::try_from(buffer.current().len())
        .map_err(|_| hammer_core::error::CoreError::internal("tcp readable length overflow"))?;
    buffer.write_primary_opaque(&TcpLocalDeliveryOpaque {
        readable_len: current_len,
        flags: TcpLocalDeliveryFlags::READABLE.bits(),
        event: TcpAppEventKind::Readable as u8,
        ..opaque
    });
    Ok(Some(app))
}
```

- [ ] **Step 4: Add an app capture node test harness**

Create `TcpAppCaptureNode` in `tcp_local_app_nodes.rs` that:
- reads the primary opaque payload
- copies current bytes
- records `TcpAppEvent`
- frees the buffer unless it needs to simulate writeback

Use the same “descriptor-only capture node” pattern already used in `tcp_input_nodes.rs` and `net_lookup_node.rs`.

- [ ] **Step 5: Run the focused tests**

Run: `cargo test -p hammer-service tcp_rcv_process_dispatches_reassembled_bytes_to_app_node -- --exact`

Expected: PASS

Run: `cargo test -p hammer-service tcp_rcv_process_marks_fin_as_readable_then_closed -- --exact`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/rcv_process.rs crates/hammer-service/src/transport/tcp/established.rs crates/hammer-service/tests/tcp_local_app_nodes.rs
git commit -m "hammer-service(Feat): dispatch tcp readable events to local apps"
```

---

## Task 5: Add TCP output helpers so app nodes can write bytes and close connections through existing egress graph

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/src/transport/tcp/app.rs`
- Modify: `crates/hammer-service/src/transport/tcp/local.rs`
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Test: `crates/hammer-service/tests/tcp_local_app_nodes.rs`

- [ ] **Step 1: Write the failing writeback tests**

```rust
#[test]
fn app_write_request_emits_tcp_payload_through_interface_output() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_capacities(4096, 16, 8, 8);
    let graph = TcpEgressGraph::new(&runtime);
    let flow = graph.install_established_flow();

    graph
        .local_stack
        .queue_write(TcpAppWriteRequest::Write {
            connection: flow.connection,
            bytes: b"server-bytes".to_vec(),
        })
        .expect("queue write");

    graph.local_stack.flush_writes(&runtime).expect("flush writes");

    let output = graph.output_device.take_packets();
    assert_eq!(output.len(), 1);
    assert!(packet_contains_payload(&output[0], b"server-bytes"));
}

#[test]
fn app_close_request_emits_fin_on_established_connection() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_capacities(4096, 16, 8, 8);
    let graph = TcpEgressGraph::new(&runtime);
    let flow = graph.install_established_flow();

    graph
        .local_stack
        .queue_write(TcpAppWriteRequest::Close {
            connection: flow.connection,
        })
        .expect("queue close");

    graph.local_stack.flush_writes(&runtime).expect("flush writes");

    let output = graph.output_device.take_packets();
    assert_eq!(output.len(), 1);
    assert!(tcp_flags_from_packet(&output[0]).fin);
}
```

- [ ] **Step 2: Run one focused test to verify it fails**

Run: `cargo test -p hammer-service app_write_request_emits_tcp_payload_through_interface_output -- --exact`

Expected: FAIL because no TCP local output path exists.

- [ ] **Step 3: Add a minimal connection record for output synthesis**

```rust
// crates/hammer-service/src/transport/tcp/local.rs
#[derive(Debug, Clone)]
pub struct TcpEstablishedFlow {
    pub connection: TcpConnectionId,
    pub listener: TcpListenerId,
    pub app_node: NodeId,
    pub owner_worker: DataWorkerId,
    pub local_addr: std::net::IpAddr,
    pub local_port: u16,
    pub remote_addr: std::net::IpAddr,
    pub remote_port: u16,
    pub ingress_interface: u32,
    pub egress_interface: u32,
    pub send_next_seq: u32,
    pub recv_next_seq: u32,
}
```

- [ ] **Step 4: Add the write queue and flush API**

```rust
pub struct TcpLocalOutputControlPlane {
    pending: std::sync::Arc<std::sync::Mutex<std::vec::Vec<TcpAppWriteRequest>>>,
}

impl TcpLocalOutputControlPlane {
    pub fn queue_write(&self, request: TcpAppWriteRequest) -> CoreResult<()> {
        self.pending
            .lock()
            .map_err(|_| CoreError::internal("tcp local output queue poisoned"))?
            .push(request);
        Ok(())
    }

    pub fn drain_writes(&self) -> CoreResult<std::vec::Vec<TcpAppWriteRequest>> {
        let mut guard = self
            .pending
            .lock()
            .map_err(|_| CoreError::internal("tcp local output queue poisoned"))?;
        Ok(std::mem::take(&mut *guard))
    }
}
```

This queue is control-path, not packet hot path, so `Mutex<Vec<_>>` is acceptable here.

- [ ] **Step 5: Build TCP/IP packets and route them through existing egress nodes**

Use `etherparse` as the serializer dependency for v1 packet emission. Add it to `crates/hammer-service/Cargo.toml`:

```toml
etherparse = { workspace = true }
```

Then implement:

```rust
pub fn emit_tcp_write(
    runtime: &DataPlaneRuntime,
    lookup: NodeId,
    flow: &TcpEstablishedFlow,
    payload: &[u8],
    fin: bool,
) -> CoreResult<()> {
    let bytes = build_tcp_ipv4_packet(flow, payload, fin)?;
    let mut metadata = hammer_adapter::RouteMetadata::default();
    metadata.network = hammer_adapter::Network::Tcp;
    metadata.source = Some(hammer_core::SocksAddr::ip(flow.local_addr, flow.local_port));
    metadata.destination = Some(hammer_core::SocksAddr::ip(flow.remote_addr, flow.remote_port));
    metadata.ingress_interface = Some(flow.ingress_interface);
    metadata.egress_interface = Some(flow.egress_interface);

    let frame = runtime.alloc_frame_index()?;
    let index = runtime.alloc_index_with_bytes(metadata, &bytes)?;
    runtime.get_frame_mut(frame)?.push_index(index)?;
    runtime.schedule_frame(lookup, frame)?;
    Ok(())
}
```

Use the existing `ip-lookup -> adjacency-rewrite -> interface-output` graph in tests, not a side-channel output path.

- [ ] **Step 6: Run the focused tests**

Run: `cargo test -p hammer-service app_write_request_emits_tcp_payload_through_interface_output -- --exact`

Expected: PASS

Run: `cargo test -p hammer-service app_close_request_emits_fin_on_established_connection -- --exact`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/output.rs crates/hammer-service/src/transport/tcp/app.rs crates/hammer-service/src/transport/tcp/local.rs crates/hammer-service/Cargo.toml crates/hammer-service/tests/tcp_local_app_nodes.rs
git commit -m "hammer-service(Feat): add tcp local app writeback path"
```

---

## Task 6: Implement the `iperf3` TCP server app node on top of the local app surface

**Files:**
- Create: `crates/hammer-service/src/transport/tcp/iperf3.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/lib.rs`
- Test: `crates/hammer-service/tests/iperf3_server.rs`

- [ ] **Step 1: Write the failing protocol-shape tests**

```rust
#[test]
fn iperf3_server_recognizes_control_cookie_and_state_byte() {
    let mut server = Iperf3ServerState::new(5201);
    let control = server
        .on_event(TcpAppEvent::Readable {
            connection: TcpConnectionId::new(1),
            listener: TcpListenerId::new(0),
            bytes: iperf3_control_bytes(b"1234567890123456789012345678901234567", Iperf3State::ParamExchange),
            fin: false,
        })
        .expect("process control bytes");

    assert!(control.actions.iter().any(|action| matches!(action, TcpAppWriteRequest::Write { .. })));
}

#[test]
fn iperf3_server_classifies_second_connection_as_data_stream_for_same_cookie() {
    let mut server = Iperf3ServerState::new(5201);
    server.observe_control_cookie(TcpConnectionId::new(1), iperf3_cookie());

    let classification = server
        .classify_new_connection(TcpConnectionId::new(2), iperf3_cookie())
        .expect("classify data stream");

    assert_eq!(classification.kind, Iperf3StreamKind::Data);
}
```

- [ ] **Step 2: Run one focused test to verify it fails**

Run: `cargo test -p hammer-service iperf3_server_recognizes_control_cookie_and_state_byte -- --exact`

Expected: FAIL because `iperf3.rs` does not exist.

- [ ] **Step 3: Define the minimal `iperf3` server state**

Implement only the subset needed for a real `iperf3 -c <addr> -p 5201` default TCP test:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Iperf3StreamKind {
    Control,
    Data,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Iperf3State {
    ParamExchange = 9,
    CreateStreams = 10,
    TestStart = 11,
    TestRunning = 12,
    TestEnd = 13,
    ExchangeResults = 14,
    DisplayResults = 15,
    IperfDone = 16,
}

#[derive(Debug, Clone)]
pub struct Iperf3Connection {
    pub kind: Iperf3StreamKind,
    pub cookie: [u8; 37],
}

#[derive(Debug, Default)]
pub struct Iperf3ServerState {
    listen_port: u16,
    control: std::collections::HashMap<TcpConnectionId, Iperf3Connection>,
    data: std::collections::HashMap<TcpConnectionId, Iperf3Connection>,
    bytes_received: u64,
}
```

The `HashMap` usage is acceptable here because it is app-level control state, not packet hot-path lookup.

- [ ] **Step 4: Implement the minimal protocol parser**

Implement only:
- 37-byte cookie extraction
- control-state byte decoding
- minimal response emission for:
  - `ParamExchange`
  - `CreateStreams`
  - `TestStart`
  - `TestEnd`
  - `ExchangeResults`
  - `IperfDone`

Use explicit helper functions:

```rust
fn parse_cookie_prefix(bytes: &[u8]) -> Option<[u8; 37]>;
fn parse_state_byte(bytes: &[u8]) -> Option<Iperf3State>;
fn build_state_reply(state: Iperf3State) -> std::vec::Vec<u8>;
fn build_minimal_server_params() -> std::vec::Vec<u8>;
```

For v1, respond with the smallest byte sequences that allow the real client to continue the standard TCP test. Do not attempt final JSON report parity.

- [ ] **Step 5: Implement the app node**

```rust
#[hammer_component_macros::node]
pub struct Iperf3ServerNode {
    #[node(default = register_iperf3_runtime(state.clone(), output.clone()))]
    runtime_data: hammer_adapter::NodeRuntimeData,
    state: std::sync::Arc<std::sync::Mutex<Iperf3ServerState>>,
    output: TcpLocalOutputControlPlane,
}
```

Processing rules:
- `Open`: record the connection as unknown until cookie/state bytes arrive
- first connection with a new cookie becomes control
- second connection with same cookie becomes data
- `Readable` on control connection drives state transitions and emits replies
- `Readable` on data connection just accumulates bytes and may emit ACK/data responses via the TCP local output path if needed
- `Closed`: remove the connection from state

- [ ] **Step 6: Add protocol-shape tests**

```rust
#[test]
fn iperf3_server_accumulates_data_bytes_on_data_connection() {
    let mut server = Iperf3ServerState::new(5201);
    server.observe_control_cookie(TcpConnectionId::new(1), iperf3_cookie());
    server.observe_data_cookie(TcpConnectionId::new(2), iperf3_cookie());

    server
        .on_event(TcpAppEvent::Readable {
            connection: TcpConnectionId::new(2),
            listener: TcpListenerId::new(0),
            bytes: b"payload".to_vec(),
            fin: false,
        })
        .expect("consume data");

    assert_eq!(server.bytes_received(), 7);
}
```

- [ ] **Step 7: Run the focused tests**

Run: `cargo test -p hammer-service iperf3_server_recognizes_control_cookie_and_state_byte -- --exact`

Expected: PASS

Run: `cargo test -p hammer-service iperf3_server_accumulates_data_bytes_on_data_connection -- --exact`

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/iperf3.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/lib.rs crates/hammer-service/tests/iperf3_server.rs
git commit -m "hammer-service(Feat): add iperf3 local tcp server node"
```

---

## Task 7: Expose formal service registration APIs and wire a deterministic test graph

**Files:**
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-service/src/lib.rs`
- Modify: `crates/hammer-service/src/transport/tcp/local.rs`
- Test: `crates/hammer-service/tests/tcp_local_app_nodes.rs`
- Test: `crates/hammer-service/tests/iperf3_server.rs`

- [ ] **Step 1: Write the failing service-registration tests**

```rust
#[test]
fn runtime_service_exposes_local_tcp_app_registration_api() {
    let service = test_runtime_service().expect("runtime service");
    let registration = service
        .register_iperf3_server(std::net::Ipv4Addr::new(192, 0, 2, 10), 5201)
        .expect("register iperf3");

    assert_eq!(registration.port, 5201);
}

#[test]
fn service_packet_graph_keeps_tcp_nodes_resolvable_after_local_app_addition() {
    let graph = super::ServicePacketGraphDeclarations::default();

    assert!(graph.resolve("tcp-input-node").is_some());
    assert!(graph.resolve("tcp-listen-node").is_some());
    assert!(graph.resolve("tcp-rcv-process-node").is_some());
    assert!(graph.resolve("interface-output-node").is_some());
}
```

- [ ] **Step 2: Run one focused test to verify it fails**

Run: `cargo test -p hammer-service runtime_service_exposes_local_tcp_app_registration_api -- --exact`

Expected: FAIL because `RuntimeService` has no local TCP app registration surface.

- [ ] **Step 3: Add service-facing registration methods**

```rust
pub struct TcpLocalRegistration {
    pub listener: TcpListenerId,
    pub port: u16,
    pub node: NodeId,
}

impl RuntimeService {
    pub fn register_local_tcp_app(
        &self,
        listen: std::net::IpAddr,
        port: u16,
        node: NodeId,
    ) -> HammerResult<TcpLocalRegistration> {
        self.control_async_call(Duration::from_secs(5), move |inner, _data, done| {
            let registration = inner.register_local_tcp_app(listen, port, node)?;
            let _ = done.send(Ok(registration));
            Ok(())
        })
    }

    pub fn register_iperf3_server(
        &self,
        listen: std::net::Ipv4Addr,
        port: u16,
    ) -> HammerResult<TcpLocalRegistration> {
        self.control_async_call(Duration::from_secs(5), move |inner, _data, done| {
            let registration = inner.register_iperf3_server(listen, port)?;
            let _ = done.send(Ok(registration));
            Ok(())
        })
    }
}
```

Keep this registration purely programmatic in v1. Do not touch `hammer-core` config parsing.

- [ ] **Step 4: Add the smallest `ServiceInner` plumbing required**

Inside `ServiceInner`, add fields for:
- `tcp_local_apps: TcpLocalAppControlPlane`
- `tcp_local_output: TcpLocalOutputControlPlane`
- optionally `iperf3_apps: Vec<NodeId>` if needed for ownership

Do not add generic component-registry machinery yet unless the implementation genuinely needs it.

- [ ] **Step 5: Add a deterministic local-app test graph builder**

Create a helper used by `tcp_local_app_nodes.rs` and `iperf3_server.rs`:

```rust
struct TcpLocalAppGraph {
    drop: NodeId,
    ip_lookup: NodeId,
    adjacency_rewrite: NodeId,
    interface_output: NodeId,
    tcp_input: NodeId,
    tcp_listen: NodeId,
    tcp_rcv_process: NodeId,
    app_node: NodeId,
    local_apps: TcpLocalAppControlPlane,
    local_stack: TcpLocalOutputControlPlane,
    app_state: Arc<Mutex<TcpAppCaptureState>>,
}
```

Model it after the existing `TcpGraph` in `tcp_input_nodes.rs` and the lookup/output graph helpers in `interface_control.rs` / `net_lookup_node.rs`.

- [ ] **Step 6: Run the focused tests**

Run: `cargo test -p hammer-service runtime_service_exposes_local_tcp_app_registration_api -- --exact`

Expected: PASS

Run: `cargo test -p hammer-service service_packet_graph_keeps_tcp_nodes_resolvable_after_local_app_addition -- --exact`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/hammer-service/src/service.rs crates/hammer-service/src/lib.rs crates/hammer-service/src/transport/tcp/local.rs crates/hammer-service/tests/tcp_local_app_nodes.rs crates/hammer-service/tests/iperf3_server.rs
git commit -m "hammer-service(Feat): expose local tcp app service registration"
```

---

## Task 8: Add real-client `iperf3` interoperability tests and final verification

**Files:**
- Modify: `crates/hammer-service/tests/iperf3_server.rs`
- Test: `crates/hammer-service/tests/iperf3_server.rs`

- [ ] **Step 1: Write the failing real-client integration test**

```rust
#[test]
fn hammer_iperf3_server_accepts_real_iperf3_tcp_client() {
    let harness = Iperf3Harness::spawn().expect("spawn harness");

    let output = std::process::Command::new("/opt/homebrew/bin/iperf3")
        .args([
            "-c",
            "192.0.2.10",
            "-p",
            "5201",
            "-t",
            "1",
        ])
        .output()
        .expect("run iperf3 client");

    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("sender"));
    assert!(String::from_utf8_lossy(&output.stdout).contains("receiver"));

    harness.shutdown();
}
```

- [ ] **Step 2: Run the real-client test to verify it fails**

Run: `cargo test -p hammer-service hammer_iperf3_server_accepts_real_iperf3_tcp_client -- --exact --nocapture`

Expected: FAIL because the `iperf3` control/data handshake is still incomplete.

- [ ] **Step 3: Build the harness around the existing packet graph**

The harness should:
- create a data-plane runtime
- register one interface with connected route + interface output
- register the local `iperf3` app node on `192.0.2.10:5201`
- run a deterministic worker pump loop
- expose a TUN/memory-device endpoint that the test harness can feed from/to

Prefer an in-process harness over spinning up a full `RuntimeService` config when the latter adds unrelated lifecycle noise.

- [ ] **Step 4: Complete the minimal missing `iperf3` protocol steps until the real client succeeds**

Tighten only what the test proves is necessary:
- cookie handling
- control/data stream association
- required state byte replies
- minimal parameter exchange payload
- orderly end-of-test close

Do not expand to UDP, reverse, or JSON unless the real default TCP client requires it.

- [ ] **Step 5: Run focused interoperability tests**

Run: `cargo test -p hammer-service iperf3_server_recognizes_control_cookie_and_state_byte -- --exact`

Expected: PASS

Run: `cargo test -p hammer-service hammer_iperf3_server_accepts_real_iperf3_tcp_client -- --exact --nocapture`

Expected: PASS

- [ ] **Step 6: Run the broader TCP/local-app suites**

Run: `cargo test -p hammer-service tcp_local_app_nodes -- --nocapture`

Expected: PASS

Run: `cargo test -p hammer-service iperf3_server -- --nocapture`

Expected: PASS

- [ ] **Step 7: Run the regression suites affected by local delivery**

Run: `cargo test -p hammer-service tcp_input_nodes -- --nocapture`

Expected: PASS

Run: `cargo test -p hammer-service interface_control -- --nocapture`

Expected: PASS

Run: `cargo test -p hammer-service net_lookup_node -- --nocapture`

Expected: PASS

- [ ] **Step 8: Run formatting and the crate test suite**

Run: `cargo fmt --all`

Expected: exits 0 with no diff left behind.

Run: `cargo test -p hammer-service`

Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add crates/hammer-service/tests/iperf3_server.rs
git commit -m "hammer-service(Feat): verify local iperf3 tcp server interoperability"
```

---

## Test Matrix

- Packet opaque round-trip:
  - `tcp_local_delivery_opaque_round_trips_connection_and_app_fields`
  - `tcp_local_delivery_opaque_round_trips_words`
- Listener/app registration:
  - `local_tcp_control_plane_registers_listener_and_resolves_app`
  - `worker_owned_tcp_state_prefers_connection_before_listener_lookup`
- Accept path:
  - `tcp_listen_accepts_registered_listener_and_stamps_open_event`
  - `tcp_listen_inserts_established_flow_for_subsequent_ack_lookup`
- Readable dispatch:
  - `tcp_rcv_process_dispatches_reassembled_bytes_to_app_node`
  - `tcp_rcv_process_marks_fin_as_readable_then_closed`
- Output path:
  - `app_write_request_emits_tcp_payload_through_interface_output`
  - `app_close_request_emits_fin_on_established_connection`
- `iperf3` protocol shape:
  - `iperf3_server_recognizes_control_cookie_and_state_byte`
  - `iperf3_server_classifies_second_connection_as_data_stream_for_same_cookie`
  - `iperf3_server_accumulates_data_bytes_on_data_connection`
- Real-client interoperability:
  - `hammer_iperf3_server_accepts_real_iperf3_tcp_client`
- Regression:
  - `tcp_input_nodes`
  - `interface_control`
  - `net_lookup_node`

## Assumptions and Defaults Chosen

- The “formal service capability” requirement is satisfied by programmatic `RuntimeService` registration APIs, not by TOML config syntax in v1.
- `iperf3` compatibility target is the locally installed `iperf3 3.18` client and the standard `iperf3 -c <addr> -p <port> -t 1` TCP test path.
- Connection-local transient metadata belongs in packet opaque payloads and worker-owned TCP state, not in `RouteMetadata`.
- App nodes consume reassembled bytes plus connection events; they do not parse raw TCP packets.
- The app output path is intent-based (`TcpAppWriteRequest`) and must reuse existing IP lookup / adjacency rewrite / interface output nodes for actual emission.
- `HashMap` is acceptable inside `iperf3` app state because it is app control state, not packet hot-path lookup.
- If `iperf3` interoperability reveals one additional mandatory control-state reply in the default TCP path, add only that state and corresponding test; do not broaden scope to UDP or reverse.

## Self-Review

- Spec coverage:
  - local TCP app surface: covered by Tasks 1, 2, 4, 5, 7
  - `tcp-rcv-process` app-node registration model: covered by Tasks 3 and 4
  - writeback through existing egress graph: covered by Task 5
  - `iperf3` server on top of local apps: covered by Tasks 6 and 8
  - no VCL / no inbound expansion / no config expansion: enforced by scope guardrails and assumptions
- Placeholder scan:
  - no `TODO` / `TBD` / “appropriate handling” placeholders remain
  - every task includes explicit files, tests, commands, and minimum code shapes
- Type consistency:
  - `TcpListenerId`, `TcpConnectionId`, `TcpAppEvent`, `TcpAppWriteRequest`, `TcpLocalDeliveryOpaque`, and `Iperf3State` are introduced once and reused consistently
