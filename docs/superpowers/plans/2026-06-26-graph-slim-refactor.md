# Graph Slim Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `Graph<D>` + `Boot` TLS + `wire` + `assemble_*` free-function layer with a VPP-shaped design: `DataPlaneRuntime::init_graph` method, `impl GraphNode::init` per-node methods, per-subsystem `*_MAIN: OnceLock` globals filled by linkme-collected control-plane init functions that inject `Config` via `RuntimeRegistry`.

**Architecture:** Three separated phases mirroring VPP — (1) linkme collects node metadata (`NodeEntry`) at compile time and control-plane init functions (`CONTROL_INITS`) at compile time; (2) at startup control-plane inits run once, each `require::<Config>()` from `RuntimeRegistry` and fill its own `*_MAIN` global; (3) `DataPlaneRuntime::init_graph` walks `NodeEntry` calling `GraphNode::init`, then `resolve_named_next_nodes`. No free functions assemble the graph, no outer layer sets globals, no config bundle is passed.

**Tech Stack:** Rust 2024, `linkme` (static slice collection), `hammer-adapter` (`DataPlaneRuntime`, `NodeRuntime`), `hammer-core` (`RuntimeRegistry`, `Config`), `hammer-component-macros` (`#[graph_node]`, `#[node_next]`), `hammer-service` (TCP/IP/session nodes, control planes).

## Global Constraints

- Dependency direction: `hammer-adapter → hammer-core`, `hammer-service → {hammer-adapter, hammer-core, hammer-runtime}`. `DataPlaneRuntime` lives in `hammer-adapter` and must not depend on `hammer-service` types.
- `NodeEntry` and `trait GraphNode` live in `hammer-adapter` (or `hammer-runtime::graph` re-export); `init` takes `&DataPlaneRuntime`.
- `RuntimeRegistry` (`hammer-core::registry`) is the only DI container. Only `Config` is injected; per-subsystem state stays in `*_MAIN` globals, never in the registry.
- Per VPP rules in `AGENTS.md`: congestion control is per-connection via `TcpConnection<S, C>`, not a graph node. CC **type dispatch** (config enum → `BbrController` etc.) is **transport-layer** via one `with_congestion!` macro reading `TRANSPORT_MAIN`, not `TCP_MAIN`. TCP owns only TCP-specific graph construction (control plane, handoff, session queue) via `TcpMain::register_tcp_input::<C>`. Feature arcs stay on `FeatureArcControl`, not in the node graph.
- Node graph names drop the trailing `-node` suffix globally (`drop-node` → `drop`, `tcp-input-node` → `tcp-input`, etc.). `#[node]` `NODE_NAME` generation and `#[node_next]` fallback naming both strip `-node`; manual `NODE_NAME` constants, `name = "..."`, `#[next("...")]`, `node_by_name`/`graph_node` literals, trace config, and tests update in lockstep (Task 4).
- No `_underscore` bindings; unused locals are deleted. `snake_case` functions, `PascalCase` types.
- No commits unless requested. Run `cargo build -p hammer-service` and `cargo test -p hammer-runtime --test graph_registry` / `cargo test -p hammer-service --lib packet_graph` / `cargo test -p hammer-service --test transport_congestion_graph` while iterating.

## Target surface (final, minimal)

| Responsibility | Owner | Form | Trigger |
|---|---|---|---|
| Graph init | `DataPlaneRuntime` | method `init_graph(&self, worker, &[NodeEntry])` | service.rs one line |
| Node inits itself | node type | trait method `impl GraphNode::init` | `init_graph` walks |
| Subsystem state | each subsystem module | global `TCP_MAIN: OnceLock<TcpMain>` etc | — |
| Control-plane init (requires Config, builds main) | each subsystem `init(&RuntimeRegistry)` + linkme `CONTROL_INITS` | linkme-collected fn | `init_control_planes(&registry)` one line |
| Config injection | service.rs | `registry.set::<Config>(Arc::new(config))` | one place |
| Owner builds its node | main | `TcpMain::register_tcp_input::<C>` (TCP only); other congestion nodes use standard register in macro | node init |
| Node metadata | linkme `NodeEntry` | static slice | compile time |
| Connect graph by name | `NodeRuntime::resolve_named_next_nodes` | method | end of `init_graph` |

service.rs实质三行: `registry.set::<Config>(...)`, `init_control_planes(&registry)?`, `ctx.install_on_workers(|w, rt| rt.init_graph(w, &SERVICE_GRAPH_NODES))?`.

## File Structure

- Modify `crates/hammer-adapter/src/buffer.rs` — add `DataPlaneRuntime::init_graph`.
- Modify `crates/hammer-runtime/src/graph/mod.rs` — delete `Graph<D>`; define `NodeEntry`, `trait GraphNode`, `init_graph` re-export (or move to adapter).
- Modify `crates/hammer-component-macros/src/lib.rs` — `#[graph_node]` emits `impl GraphNode` + `NodeEntry` in linkme; delete `wires`/`wire`/`assemble`/`default_next_node_name`/boot wrapping.
- Rewrite `crates/hammer-service/src/packet_graph.rs` — delete `Boot`/`WorkerState`/`BOOT` TLS/`with_boot`/`wire_session_queue`/`resolve_graph_node`/`install_service_graph` free fn; add `CONTROL_INITS` slice + `init_control_planes` + `SERVICE_GRAPH_NODES` slice.
- Modify `crates/hammer-service/src/transport/mod.rs` — `TRANSPORT_MAIN: OnceLock<TransportMain>` + `with_congestion!` + `init(&RuntimeRegistry)` in `CONTROL_INITS`.
- Modify `crates/hammer-service/src/transport/tcp/mod.rs` — `TCP_MAIN: OnceLock<TcpMain>` + `init(&RuntimeRegistry)` + `#[distributed_slice(CONTROL_INITS)]` + `register_tcp_input_graph_node::<C>` / `TcpMain::register_tcp_input::<C>` (no congestion field on `TcpMain`).
- Modify `crates/hammer-service/src/net/lookup/mod.rs` — `IP_MAIN: OnceLock<IpMain>` + `init(&RuntimeRegistry)` + `IpMain::register_node`.
- Modify `crates/hammer-service/src/session/node.rs` — `impl GraphNode for SessionQueueNode` (attach queue inside init, no wire).
- Modify all `#[graph_node]` call sites — drop `wires = {...}`, `assemble = ...`, `wire = ...`.
- Modify `crates/hammer-service/src/service.rs` — replace `install_service_graph(...)` 6-arg call with `registry.set::<Config>` + `init_control_planes` + `install_on_workers(init_graph)`.
- Rewrite `crates/hammer-runtime/tests/graph_registry.rs` — for `NodeEntry` + `impl GraphNode`.
- Modify `crates/hammer-service/tests/transport_congestion_graph.rs` — keep guard (no Bbr hardcode in nodes; single `with_congestion!` at transport layer).

---

### Task 1: Define `NodeEntry` and `GraphNode` trait, delete `Graph<D>`

**Files:**
- Modify: `crates/hammer-runtime/src/graph/mod.rs`
- Modify: `crates/hammer-runtime/src/lib.rs` (re-exports)
- Test: `crates/hammer-runtime/tests/graph_registry.rs` (rewrite in Task 9)

**Interfaces:**
- Produces: `pub struct NodeEntry { pub registration: NodeRegistration, pub kind: NodeKind, pub init: fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId> }`; `pub trait GraphNode { fn init(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<NodeId>; }`; re-export from `hammer-runtime::graph`.

- [ ] **Step 1: Replace `graph/mod.rs` contents**

```rust
//! VPP `vlib_node_main` node registration entry + graph-node trait.
//!
//! Node metadata is collected by linkme into `NodeEntry` slices (VPP
//! `VLIB_REGISTER_NODE`). `DataPlaneRuntime::init_graph` walks them and calls
//! `GraphNode::init`; `NodeRuntime::resolve_named_next_nodes` links by name
//! (VPP `vlib_node_main_init`).

use hammer_adapter::{
    DataPlaneRuntime, NodeId, NodeKind, NodeRegistration,
};
use hammer_core::error::CoreResult;

/// A statically-registered graph node (VPP `vlib_node_registration_t`).
pub struct NodeEntry {
    pub registration: NodeRegistration,
    pub kind: NodeKind,
    pub init: fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId>,
}

/// A graph node knows how to initialize itself into a `DataPlaneRuntime`.
///
/// Implementations read their own subsystem's `*_MAIN` global (set by the
/// control-plane init phase) and call the main's node-construction method.
/// The graph layer never passes dependencies.
pub trait GraphNode {
    fn init(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<NodeId>;
}
```

- [ ] **Step 2: Update `hammer-runtime/src/lib.rs` re-exports**

Ensure `pub mod graph;` remains and `NodeEntry`/`GraphNode` are reachable. Remove any `Graph` re-export that existed.

- [ ] **Step 3: Build the runtime crate**

Run: `cargo build -p hammer-runtime`
Expected: errors only from downstream callers still using `Graph<D>` (packet_graph, graph_registry test) — those are fixed in later tasks. The `graph` module itself must compile.

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-runtime/src/graph/mod.rs crates/hammer-runtime/src/lib.rs
git commit -m "hammer-runtime(Refactor): replace Graph<D> with NodeEntry + GraphNode trait"
```

---

### Task 2: Add `DataPlaneRuntime::init_graph` method

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs` (impl `DataPlaneRuntime`, near `run_ready_nodes` ~line 1365)
- Modify: `crates/hammer-runtime/src/graph/mod.rs` (re-export `init_graph` if desired, or callers use `rt.init_graph`)

**Interfaces:**
- Consumes: `NodeEntry` from Task 1.
- Produces: `DataPlaneRuntime::init_graph(&self, worker: usize, entries: &[NodeEntry]) -> CoreResult<()>`.

- [ ] **Step 1: Add the method to `impl DataPlaneRuntime`**

```rust
/// Initialize a packet graph: walk `entries`, call each `init`, then resolve
/// named next-node edges. VPP `vlib_register_all_static_nodes` +
/// `vlib_node_main_init`. Per-worker `worker` index is forwarded to each node.
pub fn init_graph(&self, worker: usize, entries: &[crate::node::graph::NodeEntry]) -> CoreResult<()> {
    for entry in entries {
        (entry.init)(self, worker).map_err(|err| {
            CoreError::internal(format!(
                "init graph node `{}`: {err}",
                node_entry_name(entry.registration).unwrap_or("?")
            ))
        })?;
    }
    self.nodes.resolve_named_next_nodes()
}

fn node_entry_name(reg: NodeRegistration) -> Option<&'static str> {
    match reg {
        NodeRegistration::Plain => None,
        NodeRegistration::Next { name, .. } | NodeRegistration::Sibling { name, .. } => Some(name),
    }
}
```

Note: `NodeEntry` must be importable from `hammer-adapter`. If `NodeEntry` lives in `hammer-runtime`, this creates a cycle. **Resolution:** move `NodeEntry` + `GraphNode` trait definition into `hammer-adapter/src/node.rs` (adapter owns them, since `init: fn(&DataPlaneRuntime, usize)` references adapter's `DataPlaneRuntime`). `hammer-runtime::graph` re-exports them. Update Task 1 accordingly: define in `hammer-adapter/src/node.rs`, re-export via `hammer-runtime::graph`.

- [ ] **Step 2: Move `NodeEntry`/`GraphNode` to `hammer-adapter/src/node.rs`**

Add to `crates/hammer-adapter/src/node.rs`:
```rust
pub struct NodeEntry {
    pub registration: NodeRegistration,
    pub kind: NodeKind,
    pub init: fn(&crate::buffer::DataPlaneRuntime, usize) -> CoreResult<NodeId>,
}

pub trait GraphNode {
    fn init(runtime: &crate::buffer::DataPlaneRuntime, worker: usize) -> CoreResult<NodeId>;
}
```
`hammer-runtime/src/graph/mod.rs` becomes `pub use hammer_adapter::{NodeEntry, GraphNode};` plus the doc comment.

- [ ] **Step 3: Build adapter**

Run: `cargo build -p hammer-adapter`
Expected: PASS (method compiles, `resolve_named_next_nodes` already exists on `NodeRuntime`).

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-adapter/src/node.rs crates/hammer-adapter/src/buffer.rs crates/hammer-runtime/src/graph/mod.rs
git commit -m "hammer-adapter(Feat): add DataPlaneRuntime::init_graph, NodeEntry, GraphNode"
```

---

### Task 3: Rewrite `#[graph_node]` macro to emit `impl GraphNode` + linkme `NodeEntry`

> **⚠️ Committed with wrong CC binding (commit `c3b9edd7`).** `graph_register_congestion_body` at `lib.rs:1272` reaches into `crate::transport::tcp::TCP_MAIN` — CC dispatch must be transport-layer. Fix in **Task 3b** before continuing Task 4+.

**Files:**
- Modify: `crates/hammer-component-macros/src/lib.rs`

**Interfaces:**
- Consumes: `NodeEntry`, `GraphNode` (adapter), `NodeRegistration::next(name, count)` (const, already made const in prior work).
- Produces: for each `#[graph_node(graph = service, next = TcpXNext)]`, an `impl GraphNode for TcpXNode` whose `init` constructs the node (reading its `*_MAIN` global) and a `#[linkme::distributed_slice(SERVICE_GRAPH_NODES)] static` tuple `(NodeRegistration, NodeKind, init_fn)`.

- [x] **Step 1: Simplify `GraphNodeArgs`** (committed)

Remove fields: `assemble: Option<Path>`, `wire: Option<Path>`, `wires: Vec<(Ident, LitStr)>`. Remove `parse_wire_map`. Remove `graph_install_ty`/`graph_install_path`. Remove `default_next_node_name` table (Task 4 handles next-name via `node_next` `#[next("name")]` or keep current `NEXT_NAMES` default naming — see Task 4). Keep: `graph`, `name`, `register` (internal/driver/handoff/tcp_input), `next`.

- [x] **Step 2: Emit `impl GraphNode` instead of assemble free fn** (committed — **CC body wrong, see Task 3b**)

For a node with `next` and no congestion type param:
```rust
impl ::hammer_adapter::GraphNode for #ident {
    fn init(runtime: &::hammer_adapter::DataPlaneRuntime, worker: usize) -> ::hammer_core::error::CoreResult<::hammer_adapter::NodeId> {
        runtime.nodes().try_register_internal_with_next_names(
            #ident::new([::hammer_adapter::NodeId::new(0); #next::COUNT]),
            &#next::NEXT_NAMES,
        )
    }
}
```
For `register = handoff`:
```rust
impl ::hammer_adapter::GraphNode for #ident {
    fn init(runtime: &::hammer_adapter::DataPlaneRuntime, _worker: usize) -> ::hammer_core::error::CoreResult<::hammer_adapter::NodeId> {
        runtime.register_internal_with_handle_for_graph::<#ident>()
    }
}
```

**Controller resolution: CC is transport-layer, not TCP.** Congestion-typed nodes (`<C: CongestionController>`) must **not** bind CC dispatch to `transport::tcp::TCP_MAIN`. Two registration paths (implemented in Task 3b):

1. **Default congestion path** (TcpListenNode, TcpEstablishedNode, TcpSynSentNode, TcpRcvProcessNode): `with_congestion!` picks `C`, then standard `try_register_internal_with_next_names` — no `*_MAIN` involvement.
2. **TCP control-plane input path** (`register = tcp_input` on TcpInputNode only): `with_congestion!` picks `C`, then calls `crate::transport::tcp::register_tcp_input_graph_node::<C>(runtime, worker)` which delegates to `TcpMain::register_tcp_input::<C>` for queue/handoff/control-plane construction.

```rust
// Task 3b target — NOT TCP_MAIN in macro
crate::with_congestion!(|C| {
    runtime.nodes().try_register_internal_with_next_names(
        #ident::<C>::new([::hammer_adapter::NodeId::new(0); #next::COUNT]),
        &#next::NEXT_NAMES,
    )
})
// TcpInputNode only:
crate::with_congestion!(|C| {
    crate::transport::tcp::register_tcp_input_graph_node::<C>(runtime, worker)
})
```

- [x] **Step 3: Emit linkme `NodeEntry`** (committed)

```rust
#[::linkme::distributed_slice(#graph_slice)]
static #static_ident: ::hammer_adapter::NodeEntry = ::hammer_adapter::NodeEntry {
    registration: #node_registration,
    kind: #node_kind,
    init: #init_ident,
};
```

- [x] **Step 4: Build macros** (committed)

- [x] **Step 5: Commit** (committed as `c3b9edd7`)

---

### Task 3b: Fix CC dispatch — transport layer, not TCP

**Files:**
- Modify: `crates/hammer-component-macros/src/lib.rs` (`graph_register_congestion_body`, `GraphRegisterKind`, `Parse` for `register = tcp_input`)
- Modify: `crates/hammer-service/src/transport/mod.rs` (new `TransportMain`, `TRANSPORT_MAIN`, `with_congestion!`)
- Modify: `crates/hammer-service/src/transport/tcp/input.rs` (`register = tcp_input` on `#[graph_node]`)
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs` (`register_tcp_input_graph_node`, `TcpMain::register_tcp_input` — stub OK until Task 6)

**Interfaces:**
- Produces:
  - `pub struct TransportMain { congestion: CongestionController }`
  - `pub static TRANSPORT_MAIN: OnceLock<TransportMain>`
  - `pub fn init(reg: &RuntimeRegistry) -> HammerResult<()>` (registered in `CONTROL_INITS` in Task 5; until then set alongside Boot in `install_service_graph`)
  - `#[macro_export] macro_rules! with_congestion` in `transport/mod.rs` — reads `TRANSPORT_MAIN.congestion()`, maps `CongestionController::Bbr` → `type C = BbrController`
  - `pub fn register_tcp_input_graph_node<C: congestion::CongestionController + 'static>(rt, worker) -> CoreResult<NodeId>` in `tcp/mod.rs` — calls `TCP_MAIN.get()?.register_tcp_input::<C>` (TCP owns TCP session/runtime state; macro does **not** reference `TCP_MAIN`)
  - Macro `graph_register_congestion_body`: default path = `with_congestion!` + `try_register_internal_with_next_names`; `register = tcp_input` path = `with_congestion!` + `register_tcp_input_graph_node::<C>`

- [ ] **Step 1: Add transport-layer owner in `transport/mod.rs`**

```rust
use std::sync::OnceLock;
use hammer_core::config::network::CongestionController;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::registry::RuntimeRegistry;

pub struct TransportMain {
    congestion: CongestionController,
}

impl TransportMain {
    pub fn new(congestion: CongestionController) -> Self {
        Self { congestion }
    }
    pub fn congestion(&self) -> CongestionController {
        self.congestion
    }
}

pub static TRANSPORT_MAIN: OnceLock<TransportMain> = OnceLock::new();

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    let config = reg.require::<hammer_core::config::Config>()?;
    TRANSPORT_MAIN
        .set(TransportMain::new(config.network.tcp.congestion))
        .map_err(|_| HammerError::internal("transport main already initialized"))?;
    Ok(())
}

/// Config → CC controller type. Single transport-layer dispatch point.
#[macro_export]
macro_rules! with_congestion {
    (|$cc:ident| $body:expr) => {{
        match crate::transport::TRANSPORT_MAIN
            .get()
            .ok_or_else(|| ::hammer_core::error::CoreError::internal("transport main not initialized"))?
            .congestion()
        {
            ::hammer_core::config::network::CongestionController::Bbr => {{
                type $cc = $crate::transport::congestion::BbrController;
                $body
            }}
        }
    }};
}
```

Config field remains `config.network.tcp.congestion` today; transport layer **reads** it without delegating ownership to `TcpMain`.

- [ ] **Step 2: Fix `graph_register_congestion_body` in macro crate**

Replace `lib.rs:1272-1277` (`crate::with_tcp_cc!` + `TCP_MAIN.register_node`) with:

```rust
fn graph_register_congestion_body(
    ident: &Ident,
    next: Option<&Path>,
    register: GraphRegisterKind,
) -> Result<TokenStream2> {
    let next = next.ok_or_else(|| ...)?;
    if matches!(register, GraphRegisterKind::TcpInput) {
        Ok(quote! {
            crate::with_congestion!(|C| {
                crate::transport::tcp::register_tcp_input_graph_node::<C>(runtime, worker)
            })
        })
    } else {
        Ok(quote! {
            crate::with_congestion!(|C| {
                runtime.nodes().try_register_internal_with_next_names(
                    #ident::<C>::new([::hammer_adapter::NodeId::new(0); #next::COUNT]),
                    &#next::NEXT_NAMES,
                )
            })
        })
    }
}
```

Add `GraphRegisterKind::TcpInput` and `register = tcp_input` to `GraphNodeArgs` parse. Pass `ident` and `register` into `graph_register_congestion_body` from `expand_graph_node`.

- [ ] **Step 3: TcpInputNode attr + stub in tcp/mod.rs**

```rust
#[hammer_component_macros::graph_node(
    graph = service,
    name = "tcp-input",  // after Task 4 rename
    next = TcpInputNext,
    register = tcp_input,
)]
```

```rust
pub fn register_tcp_input_graph_node<C: congestion::CongestionController + 'static>(
    runtime: &DataPlaneRuntime,
    worker: usize,
) -> CoreResult<NodeId> {
    TCP_MAIN
        .get()
        .ok_or_else(|| CoreError::internal("tcp main not initialized"))?
        .register_tcp_input::<C>(runtime, worker)
}
```

`TcpMain::register_tcp_input` stub returns `CoreError::internal("not implemented")` until Task 6.

- [ ] **Step 4: Remove `with_tcp_cc!` from `packet_graph.rs`**

Replace remaining `with_tcp_cc!` call sites with `with_congestion!` (or `crate::transport::with_congestion!`). Delete the old `with_tcp_cc!` macro definition from `packet_graph.rs`.

- [ ] **Step 5: Build**

Run: `cargo build -p hammer-component-macros && cargo build -p hammer-service`
Expected: PASS (TcpMain stub may be minimal).

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-component-macros/src/lib.rs crates/hammer-service/src/transport/
git commit -m "hammer-service(Fix): CC dispatch via TRANSPORT_MAIN, not TCP_MAIN"
```

---

### Task 4: Drop `-node` suffix globally + `node_next` `#[next("...")]` annotations

**Files:**
- Modify: `crates/hammer-component-macros/src/lib.rs` (`expand_node` NODE_NAME ~600, `expand_node_next` fallback ~1015, unit tests ~1437-1451)
- Modify: `crates/hammer-service/src/data_plane.rs` (manual `NODE_NAME` 26, 42)
- Modify: `crates/hammer-service/src/transport/tcp/output.rs` (19, 34)
- Modify: `crates/hammer-service/src/transport/tcp/{input,listen,established,syn_sent,rcv_process,reset}.rs` (`name =`, `#[next]`)
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs` (`TcpInputNext` `#[next]` 501-511)
- Modify: `crates/hammer-service/src/packet_graph.rs` (165, 228-232)
- Modify: `crates/hammer-service/src/net/lookup/mod.rs` (285)
- Modify: `crates/hammer-service/src/service.rs` (1072, 1083, 1096, 1115-1121)
- Modify: `crates/hammer-service/src/tun/mod.rs` (1167, 1340)
- Modify: `crates/hammer-core/tests/config_trace.rs` (22, 26, 34, 36, 49)
- Modify: `crates/hammer-service/tests/net_lookup_node.rs` (265, 689)
- Modify: `crates/hammer-service/tests/interface_control.rs` (298, 312)
- Defer to Task 11: `crates/hammer-runtime/tests/graph_registry.rs` (`drop-node` → `drop`, `test-output-node` → `test-output`)

**Interfaces:**
- Produces: `NODE_NAME` without trailing `-node`; `NEXT_NAMES` fallback `"{snake_variant}"` not `"{snake_variant}-node"`; all graph/trace/test string literals updated.

**Name mapping (old → new):**

| Old | New | Source |
|---|---|---|
| `drop-node` | `drop` | DropNode |
| `handoff-node` | `handoff` | HandoffNode |
| `ip-lookup-node` | `ip-lookup` | IpLookupNode |
| `adjacency-rewrite-node` | `adjacency-rewrite` | AdjacencyRewriteNode |
| `tcp-input-node` | `tcp-input` | TcpInputNode |
| `tcp-output-node` | `tcp-output` | TcpOutputNode |
| `tcp-listen-node` | `tcp-listen` | TcpListenNode |
| `tcp-rcv-process-node` | `tcp-rcv-process` | TcpRcvProcessNode |
| `tcp-syn-sent-node` | `tcp-syn-sent` | TcpSynSentNode |
| `tcp-established-node` | `tcp-established` | TcpEstablishedNode |
| `tcp-reset-node` | `tcp-reset` | TcpResetNode |
| `session-queue` | `session-queue` | explicit override — unchanged |
| `interface-output-node` | `interface-output` | InterfaceOutputNode |
| `ip-input-node` | `ip-input` | IpInputNode |
| `icmp-input-node` | `icmp-input` | IcmpInputNode |
| `udp-input-node` | `udp-input` | UdpInputNode |
| `tun-input-driver-node` | `tun-input-driver` | manual tun/mod.rs |
| `tun-output-driver-node` | `tun-output-driver` | manual tun/mod.rs |

`#[node_next]` explicit `#[next("...")]` updates (file:line):

| File | Line | Old | New |
|---|---|---|---|
| `transport/tcp/mod.rs` | 501 | `drop-node` | `drop` |
| `transport/tcp/mod.rs` | 503 | `tcp-listen-node` | `tcp-listen` |
| `transport/tcp/mod.rs` | 505 | `tcp-rcv-process-node` | `tcp-rcv-process` |
| `transport/tcp/mod.rs` | 507 | `tcp-syn-sent-node` | `tcp-syn-sent` |
| `transport/tcp/mod.rs` | 509 | `tcp-established-node` | `tcp-established` |
| `transport/tcp/mod.rs` | 511 | `tcp-reset-node` | `tcp-reset` |
| `transport/tcp/output.rs` | 19 | `ip-lookup-node` | `ip-lookup` |
| `transport/tcp/reset.rs` | 14 | `ip-lookup-node` | `ip-lookup` |
| `transport/tcp/listen.rs` | 26 | `tcp-output-node` | `tcp-output` |
| `transport/tcp/listen.rs` | 28 | `tcp-established-node` | `tcp-established` |
| `transport/tcp/established.rs` | 19 | `tcp-output-node` | `tcp-output` |
| `transport/tcp/rcv_process.rs` | 18 | `tcp-output-node` | `tcp-output` |
| `transport/tcp/syn_sent.rs` | 15 | `tcp-output-node` | `tcp-output` |

`name = "..."` on `#[graph_node]` updates:

| File | Line | Old | New |
|---|---|---|---|
| `transport/tcp/input.rs` | 142 | `tcp-input-node` | `tcp-input` |
| `transport/tcp/listen.rs` | 35 | `tcp-listen-node` | `tcp-listen` |
| `transport/tcp/rcv_process.rs` | 25 | `tcp-rcv-process-node` | `tcp-rcv-process` |
| `transport/tcp/syn_sent.rs` | 22 | `tcp-syn-sent-node` | `tcp-syn-sent` |
| `transport/tcp/established.rs` | 26 | `tcp-established-node` | `tcp-established` |

(`TcpResetNode` / `TcpOutputNode` / `IpLookupNode` rely on `NODE_NAME` from `#[node]` or manual const — no `name =` attr.)

- [ ] **Step 1: Change `#[node]` `NODE_NAME` generation (`expand_node` ~600)**

```rust
fn graph_node_name_from_ident(ident: &Ident) -> String {
    let snake = to_snake_case(&ident.to_string()).replace('_', "-");
    snake.strip_suffix("-node").map(|s| s.to_string()).unwrap_or(snake)
}
let node_name = LitStr::new(&graph_node_name_from_ident(&ident), ident.span());
```

- [ ] **Step 2: Change `#[node_next]` fallback (`expand_node_next` ~1015)**

```rust
None => to_snake_case(&variant_ident.to_string()),
```

- [ ] **Step 3: Update macro unit tests (~1437-1451)**

`#[next("custom-node")]` explicit annotation stays `custom-node` (tests explicit override). `Fallback` variant fallback: `fallback-node` → `fallback`.

- [ ] **Step 4: Update manual `NODE_NAME` + string literals**

Apply file:line list above plus:
- `data_plane.rs:26` → `"drop"`, `:42` → `"handoff"`
- `output.rs:34` → `"tcp-output"`
- `packet_graph.rs:165` → `"tcp-output"`; `:228-232` test list
- `net/lookup/mod.rs:285` → `"drop"`
- `service.rs:1072,1083,1096` → `"tun-input-driver"`; `:1115-1121` resolve_graph_node assertions
- `tun/mod.rs:1167` → `"tun-input-driver"`; `:1340` → `"tun-output-driver"`
- `config_trace.rs:22,26,34,36,49` → `tun-input-driver`, `ip-input`
- `net_lookup_node.rs:265` → `ip-lookup`; `:689` → `adjacency-rewrite`
- `interface_control.rs:298` → `interface-output`; `:312` → `tun-output-driver`

**Not changed:** `hammer-adapter/src/node.rs` `function-stats-node` (adapter unit-test fixture, not `#[node]`-generated). `spawn.rs:1690` `trace-node` (trace drain test string, not a graph node). `graph_registry.rs` deferred to Task 11.

- [ ] **Step 5: Build service**

Run: `cargo build -p hammer-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-component-macros/src/lib.rs crates/hammer-service/ crates/hammer-core/tests/config_trace.rs
git commit -m "hammer-service(Refactor): drop -node suffix from graph node names globally"
```

---

### Task 5: Rewrite `packet_graph.rs` — delete Boot, add CONTROL_INITS

**Files:**
- Rewrite: `crates/hammer-service/src/packet_graph.rs`

**Interfaces:**
- Consumes: `NodeEntry`, `DataPlaneRuntime::init_graph`, `RuntimeRegistry`, `Config`.
- Produces: `SERVICE_GRAPH_NODES: [NodeEntry]` (linkme), `CONTROL_INITS: [fn(&RuntimeRegistry) -> HammerResult<()>]` (linkme), `init_control_planes(&RuntimeRegistry) -> HammerResult<()>`. `with_congestion!` lives in `transport/mod.rs` (Task 3b); **do not** reintroduce `with_tcp_cc!` here.

- [ ] **Step 1: New `packet_graph.rs` skeleton**

```rust
//! Service packet graph: linkme `SERVICE_GRAPH_NODES` + control-plane init
//! registry. No Boot, no TLS, no free assemble functions.

use hammer_adapter::NodeEntry;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::registry::RuntimeRegistry;

#[linkme::distributed_slice]
pub static SERVICE_GRAPH_NODES: [NodeEntry] = [..];

#[linkme::distributed_slice]
pub static CONTROL_INITS: [fn(&RuntimeRegistry) -> HammerResult<()>] = [..];

pub fn init_control_planes(reg: &RuntimeRegistry) -> HammerResult<()> {
    for init in CONTROL_INITS {
        init(reg)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_graph_contains_tcp_nodes() {
        let names: Vec<&'static str> = SERVICE_GRAPH_NODES
            .iter()
            .filter_map(|e| match e.registration {
                hammer_adapter::NodeRegistration::Next { name, .. }
                | hammer_adapter::NodeRegistration::Sibling { name, .. } => Some(name),
                _ => None,
            })
            .collect();
        for want in [
            "drop", "handoff", "ip-lookup", "tcp-input",
            "tcp-listen", "session-queue",
        ] {
            assert!(names.iter().any(|n| *n == want), "missing {want}");
        }
    }
}
```

- [ ] **Step 2: Delete old code**

Remove `Boot`, `WorkerState`, `BOOT` thread_local, `with_boot`, `ensure_tcp_session`, `set_session_queue_node`, `register_tcp_session`, `graph_node`, `wire_session_queue`, `resolve_graph_node`, `install_service_graph`, `service_graph`, `graph_node_slot`, `node_registration_name`, and the old `with_tcp_cc!` macro. Register `crate::transport::init` in `CONTROL_INITS` (transport layer sets `TRANSPORT_MAIN` before TCP/IP inits run).

- [ ] **Step 3: Build (expect downstream breakage)**

Run: `cargo build -p hammer-service`
Expected: errors in `service.rs`, `session/node.rs`, `net/lookup/mod.rs` — fixed in Tasks 6–8.

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-service/src/packet_graph.rs
git commit -m "hammer-service(Refactor): replace Boot/TLS with linkme CONTROL_INITS + SERVICE_GRAPH_NODES"
```

---

### Task 6: TCP subsystem `TCP_MAIN` + `init` + `TcpMain::register_tcp_input`

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/input.rs` (`TcpInputNode` GraphNode via macro + `register = tcp_input`)

**Interfaces:**
- Consumes: `RuntimeRegistry`, `CONTROL_INITS`, `HandoffHandle` (not congestion — that is `TRANSPORT_MAIN`).
- Produces: `pub static TCP_MAIN: OnceLock<TcpMain>`; `pub fn init(&RuntimeRegistry)` registered in `CONTROL_INITS`; `impl TcpMain { fn register_tcp_input::<C>(&self, rt, worker) -> CoreResult<NodeId> }`; `register_tcp_input_graph_node::<C>` (entry from macro, defined in Task 3b).

- [ ] **Step 1: Define `TcpMain` + global + init**

In `crates/hammer-service/src/transport/tcp/mod.rs`:
```rust
pub struct TcpMain {
    control: TcpInputControlPlane,
    handoff_handle: hammer_adapter::NodeHandle,
    // per-worker state set during register_tcp_input
}

impl TcpMain {
    pub fn new(handoff_handle: hammer_adapter::NodeHandle) -> Self {
        Self {
            control: TcpInputControlPlane::new(),
            handoff_handle,
        }
    }
    pub fn handoff_handle(&self) -> hammer_adapter::NodeHandle { self.handoff_handle.clone() }

    pub fn register_tcp_input<C: crate::transport::congestion::CongestionController + 'static>(
        &self,
        rt: &hammer_adapter::DataPlaneRuntime,
        worker: usize,
    ) -> hammer_core::error::CoreResult<hammer_adapter::NodeId> {
        crate::transport::tcp::lookup::ensure_worker_state(worker);
        let session_queue = /* TcpQueue<C> from session-queue runtime_data */;
        let next = [hammer_adapter::NodeId::new(0); TcpInputNext::COUNT];
        let node = self.control.node::<C>(
            next,
            Some(session_queue),
            Some((self.handoff_handle, hammer_adapter::DataWorkerId::new(worker as u32))),
        );
        rt.nodes().try_register_internal_with_next_names(node, &TcpInputNext::NEXT_NAMES)
    }
}
```

No `congestion` field on `TcpMain`. CC type `C` is chosen by `with_congestion!` before this call.

- [ ] **Step 2: Handoff handle availability**

Inject `HandoffHandle` via `RuntimeRegistry` (see prior Step 2 in this task). `init` does `reg.require::<HandoffHandle>()`.

- [ ] **Step 3: `TcpInputNode` via `#[graph_node]`**

```rust
#[hammer_component_macros::graph_node(
    graph = service,
    name = "tcp-input",
    next = TcpInputNext,
    register = tcp_input,
)]
```

Macro emits init calling `with_congestion!(|C| register_tcp_input_graph_node::<C>(...))` — **not** `TCP_MAIN` directly in macro crate.

- [ ] **Step 4: Build service**

Run: `cargo build -p hammer-service`
Expected: remaining errors in `session/node.rs`, `net/lookup/mod.rs`, `service.rs` — Tasks 7, 8, 9.

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/ crates/hammer-service/src/packet_graph.rs
git commit -m "hammer-service(Feat): TcpMain global + register_tcp_input, no CC ownership"
```

---

### Task 7: IP lookup `IP_MAIN` + `init` + `IpMain::register_node`

**Files:**
- Modify: `crates/hammer-service/src/net/lookup/mod.rs`

**Interfaces:**
- Produces: `pub static IP_MAIN: OnceLock<IpMain>`; `init(&RuntimeRegistry)` in `CONTROL_INITS`; `IpMain::register_node(&self, rt: &DataPlaneRuntime) -> CoreResult<NodeId>`.

- [ ] **Step 1: Define `IpMain` + global + init + register**

```rust
use std::sync::OnceLock;
use hammer_core::config::Config;
use hammer_core::error::{CoreError, HammerResult};
use hammer_core::registry::RuntimeRegistry;

pub struct IpMain {
    table: FibTable,
}

impl IpMain {
    pub fn new(routes: &[hammer_core::config::Route]) -> HammerResult<Self> {
        let drop = hammer_adapter::NodeId::new(0); // placeholder; resolved by name later
        let mut builder = FibTableBuilder::new(drop);
        for route in routes {
            if let RouteAction::Drop = route.action().map_err(|e| HammerError::internal(e.to_string()))? {
                builder.add_drop_route(route.prefix);
            }
        }
        Ok(Self { table: builder.build() })
    }
    pub fn register_node(&self, rt: &hammer_adapter::DataPlaneRuntime) -> CoreResult<hammer_adapter::NodeId> {
        let drop = rt.node_by_name("drop").ok_or_else(|| CoreError::internal("drop missing"))?;
        // rebuild table with real drop id, or store drop-id lazily; simplest: rebuild here
        let mut builder = FibTableBuilder::new(drop);
        // re-add drop routes from self.routes (store routes in IpMain)
        rt.nodes().try_register_internal(IpLookupControlPlane::new(builder.build()).node())
    }
}

pub static IP_MAIN: OnceLock<IpMain> = OnceLock::new>;

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    let config = reg.require::<Config>()?;
    let main = IpMain::new(&config.network.route)?;
    IP_MAIN.set(main).map_err(|_| HammerError::internal("ip main already initialized"))?;
    Ok(())
}

#[linkme::distributed_slice(crate::packet_graph::CONTROL_INITS)]
fn init_ip(reg: &RuntimeRegistry) -> HammerResult<()> { init(reg) }
```
Store `routes: Arc<[Route]>` in `IpMain` so `register_node` can rebuild with the real drop id (drop id only known after `drop` registers). The `IpLookupNode` `#[graph_node]` calls `IP_MAIN.get()?.register_node(runtime)`.

- [ ] **Step 2: Convert `IpLookupNode` to `#[graph_node]`**

```rust
#[hammer_component_macros::graph_node(graph = service)]
#[hammer_component_macros::node]
pub struct IpLookupNode { /* unchanged */ }
```
Macro emits `impl GraphNode for IpLookupNode { fn init(rt, _w) { IP_MAIN.get()?.register_node(rt) } }`. `NODE_NAME` is `ip-lookup` from `#[node]` macro (Task 4).

- [ ] **Step 3: Build**

Run: `cargo build -p hammer-service`
Expected: errors only in `session/node.rs` and `service.rs`.

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-service/src/net/lookup/mod.rs
git commit -m "hammer-service(Feat): IpMain global + linkme control init + GraphNode for ip-lookup"
```

---

### Task 8: Session queue `impl GraphNode` (attach inside init, no wire)

**Files:**
- Modify: `crates/hammer-service/src/session/node.rs`

**Interfaces:**
- Produces: `impl GraphNode for SessionQueueNode` that registers the driver node and attaches the TCP queue + dispatcher using `node_by_name("tcp-output")` (guaranteed registered before session-queue by linkme order or explicit ordering).

- [ ] **Step 1: Order session-queue after tcp-output**

linkme order is not guaranteed. **Resolution:** add `SERVICE_GRAPH_LATE` slice (see prior Task 8 text). Mark `session-queue` `#[graph_node(graph = service, late)]`.

- [ ] **Step 2: `impl GraphNode for SessionQueueNode`**

```rust
impl hammer_adapter::GraphNode for SessionQueueNode {
    fn init(rt: &hammer_adapter::DataPlaneRuntime, worker: usize) -> hammer_core::error::CoreResult<hammer_adapter::NodeId> {
        crate::with_congestion!(|C| {
            crate::transport::tcp::lookup::ensure_worker_state(worker);
            let queue = /* TcpQueue<C> from session queue runtime_data */;
            let node = SessionQueueNode::new()?;
            let id = rt.nodes().try_register_driver(node.clone())?;
            let tcp_output = rt.node_by_name("tcp-output").ok_or_else(|| hammer_core::error::CoreError::internal("tcp-output missing"))?;
            node.attach_queue(queue, tcp_output.into(), dispatch_registered_session_queue_once_at::<crate::transport::tcp::TcpConnection<C>>)?;
            rt.nodes().set_node_state(id, hammer_adapter::NodeState::Polling)?;
            Ok(id)
        })
    }
}
```
Remove `assemble_session_queue_node` free fn and `wire = ...` from the `#[graph_node]` attr.

- [ ] **Step 3: Build**

Run: `cargo build -p hammer-service`
Expected: errors only in `service.rs` (Task 9).

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-service/src/session/node.rs crates/hammer-runtime/src/graph/mod.rs
git commit -m "hammer-service(Refactor): session-queue GraphNode with in-init attach, drop wire"
```

---

### Task 9: Rewire `service.rs` — registry.set Config + init_control_planes + init_graph

**Files:**
- Modify: `crates/hammer-service/src/service.rs`

**Interfaces:**
- Consumes: `RuntimeRegistry`, `Config`, `init_control_planes`, `DataPlaneRuntime::init_graph`, `SERVICE_GRAPH_NODES`, `HandoffHandle`.

- [ ] **Step 1: Replace the `install_service_graph(...)` 6-arg call (around line 370)**

```rust
let registry = RuntimeRegistry::new();   // already created at line 414; move earlier
registry.set::<hammer_core::config::Config>(Arc::new(config.clone()));
registry.set::<crate::packet_graph::HandoffHandle>(Arc::new(crate::packet_graph::HandoffHandle(
    hammer_adapter::NodeHandle::new(worker.handoff.node_handle),
)));
crate::packet_graph::init_control_planes(&registry)?;
data_context.install_on_workers(|worker, rt| {
    rt.init_graph(worker, &crate::packet_graph::SERVICE_GRAPH_NODES)
        .map_err(hammer_core::error::HammerError::from)
})?;
```
Move `let registry = RuntimeRegistry::new();` above this block (currently line 414). Keep the existing `registry.set::<CertificateStore>` etc. calls after.

- [ ] **Step 2: Remove `packet_graph::resolve_graph_node` usages in trace publish + tests**

`trace.publish_options(&config.trace, packet_graph::resolve_graph_node)?` (line 404) — `resolve_graph_node` is deleted. Replace with lookup over `SERVICE_GRAPH_NODES` by name. Update `service.rs` tests (lines 1115-1121) to use new names (`handoff`, `tcp-input`, etc.).

- [ ] **Step 3: Build service**

Run: `cargo build -p hammer-service`
Expected: PASS (full crate compiles).

- [ ] **Step 4: Commit**

```bash
git add crates/hammer-service/src/service.rs crates/hammer-service/src/packet_graph.rs
git commit -m "hammer-service(Refactor): wire service.rs to registry Config + init_control_planes + init_graph"
```

---

### Task 10: Update `#[graph_node]` call sites — drop wires/assemble/wire

**Files:**
- Modify: `crates/hammer-service/src/data_plane.rs` (DropNode, HandoffNode)
- Modify: `crates/hammer-service/src/transport/tcp/{output,listen,established,syn_sent,rcv_process,reset,input}.rs`

- [ ] **Step 1: Strip attrs**

Each `#[graph_node(...)]` keeps only `graph = service`, `name = "..."` (where needed), `next = XNext`, `register = handoff/driver` (where needed). Remove all `wires = {...}`, `assemble = ...`, `wire = ...`. Example final:
```rust
#[hammer_component_macros::graph_node(graph = service, next = TcpOutputNext)]
```

- [ ] **Step 2: Build**

Run: `cargo build -p hammer-service`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/hammer-service/src/
git commit -m "hammer-service(Refactor): strip wires/assemble/wire from all graph_node attrs"
```

---

### Task 11: Rewrite `graph_registry.rs` test

**Files:**
- Rewrite: `crates/hammer-runtime/tests/graph_registry.rs`

- [ ] **Step 1: Rewrite for `NodeEntry` + `impl GraphNode`**

```rust
use hammer_adapter::{DataPlaneRuntime, GraphNode, InternalNode, Node, NodeEntry, NodeId, NodeKind, NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData};
use hammer_core::error::{CoreError, CoreResult};

#[hammer_component_macros::node_next]
enum TestNext {
    #[next("drop")]
    Drop,
}

#[derive(Clone, Copy)]
struct DropNode;
impl DropNode { fn new() -> Self { Self } }
impl InternalNode for DropNode {
    fn node_registration(&self) -> NodeRegistration { NodeRegistration::next("drop", 0) }
}
// ... Node impl unchanged ...

impl GraphNode for DropNode {
    fn init(rt: &DataPlaneRuntime, _w: usize) -> CoreResult<NodeId> {
        rt.nodes().try_register_internal(DropNode::new())
    }
}

#[hammer_component_macros::node(role = internal, next = TestNext)]
struct TestOutputNode;
impl GraphNode for TestOutputNode {
    fn init(rt: &DataPlaneRuntime, _w: usize) -> CoreResult<NodeId> {
        rt.nodes().try_register_internal_with_next_names(
            TestOutputNode::new([NodeId::new(0); TestNext::COUNT]),
            &TestNext::NEXT_NAMES,
        )
    }
}

static TEST_NODES: [NodeEntry; 2] = [
    NodeEntry { registration: NodeRegistration::next("drop", 0), kind: NodeKind::Internal, init: DropNode::init },
    NodeEntry { registration: NodeRegistration::next("test-output", TestNext::COUNT), kind: NodeKind::Internal, init: TestOutputNode::init },
];

#[test]
fn init_graph_resolves_named_next_edges() {
    let rt = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    rt.init_graph(0, &TEST_NODES).expect("init");
    let drop_id = rt.node_by_name("drop").expect("drop");
    let out_id = rt.node_by_name("test-output").expect("output");
    assert_eq!(rt.nodes().node_next_slot(out_id, 0).unwrap(), drop_id);
}
```
Add tests for reverse order + dynamic `NodeEntry` push (if `init_graph` supports a dynamic vec; if not, drop the dynamic test).

- [ ] **Step 2: Run test**

Run: `cargo test -p hammer-runtime --test graph_registry`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/hammer-runtime/tests/graph_registry.rs
git commit -m "hammer-runtime(Refactor): rewrite graph_registry test for NodeEntry + GraphNode"
```

---

### Task 12: Verify full workspace

- [ ] **Step 1: Build workspace**

Run: `cargo build --workspace`
Expected: PASS.

- [ ] **Step 2: Run target tests**

Run: `cargo test -p hammer-runtime --test graph_registry && cargo test -p hammer-service --lib packet_graph && cargo test -p hammer-service --test transport_congestion_graph`
Expected: all PASS.

- [ ] **Step 3: Clippy**

Run: `cargo clippy -p hammer-service --all-targets`
Expected: no new warnings from refactor.

- [ ] **Step 4: Final commit if any fixes**

```bash
git add -A
git commit -m "hammer-service(Refactor): graph slim refactor verification"
```

## Self-Review

**Spec coverage:**
- Delete `Graph<D>` → Task 1. ✅
- Delete `Boot`/TLS/`with_boot`/`wire`/`resolve_graph_node`/`install_service_graph` free fn → Task 5. ✅
- `DataPlaneRuntime::init_graph` method → Task 2. ✅
- `impl GraphNode::init` per-node → Tasks 3, 3b, 6, 7, 8. ✅
- `*_MAIN: OnceLock` globals → Tasks 3b (`TRANSPORT_MAIN`), 6 (`TCP_MAIN`), 7 (`IP_MAIN`). ✅
- CC dispatch transport-layer (`with_congestion!`, not `TCP_MAIN`) → Task 3b. ✅
- Global `-node` suffix drop → Task 4. ✅
- linkme `CONTROL_INITS` + `init_control_planes` → Task 5. ✅
- DI: `registry.set::<Config>` only → Task 9. ✅
- Macro: drop `wires`/`wire`/`assemble`/`default_next_node_name` → Tasks 3, 4. ✅
- Call sites stripped → Task 10. ✅
- Tests → Tasks 11, 12. ✅

**Placeholder scan:** No TBD/TODO. Code blocks present in every step. `/* ... */` placeholders in Task 6/8 mark spots where existing helpers plug in.

**Type consistency:** `NodeEntry` (adapter) used consistently. `GraphNode::init(&DataPlaneRuntime, usize) -> CoreResult<NodeId>` consistent. `TRANSPORT_MAIN`/`TCP_MAIN`/`IP_MAIN` naming consistent. `CONTROL_INITS`/`SERVICE_GRAPH_NODES` consistent. `HandoffHandle` defined in Task 6, used in Task 9. `with_congestion!` in `transport/mod.rs`; `with_tcp_cc!` removed.

**Risk notes:**
- `NodeEntry` must live in `hammer-adapter` (not `hammer-runtime`) to avoid a cycle with `DataPlaneRuntime::init_graph`. Task 2 Step 2 handles this.
- linkme order for session-queue attach solved by `SERVICE_GRAPH_LATE` (Task 8).
- `with_congestion!` reads `TRANSPORT_MAIN.congestion()`; must run after `transport::init` in `init_control_planes`. Only `TcpInputNode` uses `register = tcp_input`; other `<C>` nodes use generic macro path without `TCP_MAIN`.
- Task 3 commit `c3b9edd7` routes **all** `<C>` nodes to `TCP_MAIN.register_node` — Task 3b fixes this split before Task 4 lands.
