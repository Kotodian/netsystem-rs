# VPP-shaped TCP worker state and congestion selection

## Status

Rejected and superseded for worker-state ownership. Do not implement the typed
Graph Node State or shared graph-state capability proposed below. Vendored VPP
keeps graph dispatch runtime in `vlib_node_runtime_t`, Session worker state in
`session_main.wrk[thread]`, and TCP worker state in `tcp_main.wrk[thread]`; it
does not provide a generic graph-owned registry for Session or transport
business state.

The congestion-selection analysis remains historical design context. The
Graph Node State, shared TCP state capability, and transport-registration
interfaces in this document are not approved implementation guidance.

This proposal intentionally changes the current `AGENTS.md` rule requiring
`TcpConnection<S, C>`. That rule cannot coexist with runtime congestion
selection while enum dispatch, trait objects, type lookup, and duplicated
algorithm-specific workers are forbidden. The rule must be amended as part of
approval; implementation must not silently violate it.

## Result

Hammer has one Session worker and one TCP worker per Data Worker. They are
separate worker-local owners. Local/SVM storage changes only the Session worker
adapter. The configured congestion algorithm changes only the congestion state
created for a TCP connection. Neither choice changes graph topology, graph node
types, TCP worker types, or the number of worker state slots.

The final data path is:

```text
Session Queue node state
  owns SessionWorker<selected session storage>
  selects scheduled Session + Transport Index
  calls registered TCP transport operations
    -> TCP node state owns one TcpWorker
       -> TcpWorker owns Pool<TcpConnection>
          -> TcpConnection owns congestion::State
             -> State contains one selected ops reference and inline private state
  prepares payload buffers from Session TX FIFO
  calls push_header once for the prepared batch
  flushes the committed batch through Graph Fanout
```

There is no TCP `thread_local!`, no `TcpWorkerState<C, Seg>`, no algorithm or
storage-backend multiplication, and no algorithm branch in TCP graph nodes.

## VPP evidence

This design uses vendored VPP as the semantic and ownership reference:

- `third_party/vpp/src/vnet/session/session.h`: `session_main.wrk[thread]`
  owns Session worker state.
- `third_party/vpp/src/vnet/tcp/tcp.h`: `tcp_main.wrk[thread]` owns TCP
  connections, timers, cleanup queues, and TCP worker statistics separately.
- `third_party/vpp/src/vnet/session/transport.h`: the registered transport
  interface exposes `send_params`, `push_header`, `custom_tx`, and
  `update_time`; TX selection is metadata in `transport_options.tx_type`.
- `third_party/vpp/src/vnet/session/session_node.c`: Session fills pending
  buffers from its FIFO, calls `push_header` for the batch, and only then
  flushes pending buffers to the graph.
- `third_party/vpp/src/vnet/tcp/tcp.c`: TCP registers one transport VFT with
  `TRANSPORT_TX_PEEK`; it does not register a Session/TCP strategy type.
- `third_party/vpp/src/vnet/tcp/tcp_types.h`: each TCP connection stores one
  selected `cc_algo` and fixed-size `cc_data`; the TCP worker is not generic
  over congestion control.
- `third_party/vpp/src/vnet/tcp/tcp_cubic.c`: Cubic registers one ops table and
  verifies its private state fits the connection's congestion storage.

Hammer does not copy the C API. It copies these ownership and dispatch facts.

## Current defects to remove

The current implementation has four coupled dimensions:

```text
TcpWorkerState<C, Seg>
  C   = congestion algorithm
  Seg = Local or SVM Session storage
```

That coupling causes the rejected surfaces:

- `Controller` or another selection enum implementing
  `CongestionController`;
- one TLS slot for every algorithm/backend combination;
- `TcpWorkerStore<C>` and repeated Local/SVM forwarding implementations;
- `TcpMain::new::<Seg>` selecting monomorphized TCP node functions;
- `SessionTxStrategy`, `SessionPacketizedTransport`, and
  `TransportInternalTransport` encoding VPP TX metadata as trait families;
- `NodeRuntimeData` indexes into process-global TLS vectors rather than Graph
  Runtime-owned typed node state.

Renaming any of these surfaces does not fix their ownership.

## Ownership

| State | Owner | Created/replaced | May access | Must not access |
| --- | --- | --- | --- | --- |
| Graph topology | main Graph Runtime | before worker start or under graph transaction | node identity and Next Arc wiring | worker business state |
| Graph Node State | worker Graph Runtime | cloned from graph declaration, then replaced during worker init | its node's packet work and narrow registered capabilities | process-global state slots |
| Session worker | Session Queue Graph Node State | once per Data Worker from Session config | Session FIFOs, Message Queue, readiness, work batch | TCP connections, TCP timers, congestion state |
| TCP worker | TCP Graph Node State | once per Data Worker | TCP connections, lookup, timers, exact timer dispatch | Local/SVM selection, app readiness ownership |
| TCP connection | TCP worker pool | connect/listen accept | sequence, ACK, recovery, selected congestion state | Session FIFO ownership |
| Congestion state | TCP connection | connection creation or listener inheritance | typed congestion events and metrics | graph scheduling, Session state, TCP worker selection |

## Typed Graph Node State

### Public contract

`Node` declares an associated `State` and a typed process function. A node
registration contains its initial state, not four opaque words. Graph Runtime
clones that state when it creates a worker graph. Worker initialization may
replace the state through the typed registration capability for that node.

Conceptual interface:

```rust
pub trait Node: 'static {
    type State: Clone + 'static;

    fn process(
        state: &mut Self::State,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> NodeResult;
}
```

The exact registration value stays macro-generated. Callers do not receive an
`Any`, `TypeId`, `NodeRuntimeData`, raw pointer, state slot number, or downcast
method.

### Runtime implementation

Graph Runtime necessarily stores heterogeneous node states. That erasure is a
private runtime implementation detail behind a generated per-node function
table for clone, drop, replace, and dispatch. The typed trampoline is generated
where the concrete state type is known and invokes `Node::process` directly.

The erased carrier is not exported from `hammer-runtime`, is never passed to a
plugin node, and is never downcast during dispatch. The plugin registration
image remains loaded for at least as long as its state and function table.
Graph cloning calls the registered clone operation. State replacement validates
the registration capability rather than accepting an untyped NodeId plus data.

This is the minimum private erasure required for heterogeneous dynamic-plugin
node states. It does not leak erasure into the node interface or packet path.

### State placement

- `SessionQueueNode` state owns one selected Session worker and its transport
  registrations.
- TCP input/listen/established/receive/syn-sent dispatch uses one shared TCP
  node state owned by Graph Runtime. These graph nodes must not each own a copy
  of `TcpWorker`.
- A typed shared-state capability created by TCP graph registration refers to
  that one TCP node state. It is valid only inside the current worker Graph
  Runtime and cannot be cloned across workers.
- Worker init replaces Session and TCP state. It does not add nodes, Next Arcs,
  or process functions.

The shared TCP state capability is necessary because several TCP state graph
nodes operate on the same connection pool. Existing surfaces cannot express
this: `NodeRuntimeData` only carries integers, while one independent state per
TCP node would duplicate connection ownership.

## Session and transport interface

Session owns Local/SVM construction. The backend choice is made once while
constructing the Session Queue node state. `SessionWorker<Seg>` remains a
Session implementation detail. TCP worker state, TCP graph nodes, TCP
connections, and congestion state are not generic over `Seg`.

One transport registration replaces the current strategy trait family. Its TX
mode is data, not a Rust implementation family. `hammer-service` exposes a
worker-local `register_transport<T>()` operation. TCP calls it from worker init
after installing its typed TCP state. Registration mutates only the current
worker's Session Queue node state; it does not add graph topology, extend the
plugin root ABI, or add a Runtime special case.

`T` is a real transport adapter, not the owner of transport state. The TCP
adapter contains the typed capability for the one TCP Graph Node State and
implements operations generically for Session's private `Seg`. Session
registration monomorphizes those operations for its already-selected backend
and stores the resulting function table in Session Queue state. Consequently,
backend specialization exists only inside Session; it does not produce a
Local/SVM TCP worker, node, connection, or congestion type.

```rust
pub enum TransportTx {
    Peek,
    Dequeue,
    Internal,
    Datagram,
}

pub trait SessionTransport: Clone + 'static {
    const ID: SessionTransportId;
    const TX: TransportTx;

    fn update_time<Seg: SessionSegment>(...);
    fn send_params<Seg: SessionSegment>(...);
    fn push_header<Seg: SessionSegment>(...);
    fn custom_tx<Seg: SessionSegment>(...);
    fn disconnect<Seg: SessionSegment>(...);
}

pub fn register_transport<T: SessionTransport>(
    engine: &mut Engine,
    transport: T,
) -> RuntimeResult<()>;
```

The registered table is private to `SessionQueueState<Seg>`. There is no
`dyn SessionTransport`, public erased transport carrier, process-global
transport registry, or Local/SVM forwarding trait. Adding another transport
adds another adapter and registration, which makes this a real seam; adding a
congestion algorithm does not touch it.

`TransportTx` is not a congestion-controller enum and does not choose a worker
implementation. It is VPP transport metadata consumed by Session Queue to pick
the one packetization rule. It replaces marker types and strategy traits.

TCP registers `TX = Peek`. Normal TCP send order is fixed:

1. TCP `send_params` returns send space, TX offset, Send Goal Size, and
   scheduling flags.
2. Session peeks retained bytes from the selected Session FIFO and prepares a
   batch of data-plane buffers.
3. TCP `push_header` writes output intent and commits sequence, recovery, and
   timer state for the entire batch.
4. Session flushes the committed batch to the TCP output Next Arc.
5. ACK cleanup asks Session to drop acknowledged FIFO bytes.

`custom_tx` handles retransmit, ACK-only, persist, delayed ACK, and other
transport-owned output. It does not become another strategy trait.

The transport registration may call only the narrow Session operations needed
for FIFO length/copy/drop, scheduling, RX delivery, and lifecycle notification.
Session does not expose its concrete Local/SVM type, queues, or entries to TCP.
TCP does not expose connections or timers to Session.

## Congestion control

### Why `TcpConnection<C>` is removed

The configured algorithm is known at runtime. A Rust generic parameter is known
at compile time. Runtime selection among arbitrary concrete `C` values requires
one of: a closed enum, dynamic dispatch, erased storage, or separate
monomorphized owners. The first, second, and fourth choices were explicitly
rejected and the current code demonstrates the resulting type multiplication.

The VPP-shaped answer is one concrete `TcpConnection` with a selected ops table
and connection-local private state. This is deliberate dynamic algorithm
dispatch, isolated to congestion events; it is not dynamic dispatch of TCP
workers or graph nodes.

### Connection representation

`congestion::State` is a concrete connection-owned domain value:

```rust
pub(crate) struct State {
    algorithm: &'static Algorithm,
    private: Private,
}
```

`Algorithm` is an immutable registered ops table. `Private` is aligned fixed
storage with a compile-time capacity check for each implementation. Both types
are private to the TCP congestion module. There is no public controller enum,
`dyn CongestionController`, `Any`, `TypeId`, heap allocation, or algorithm
field in `TcpWorker`.

The congestion module owns the only unsafe code required to view its aligned
private storage as an algorithm's private state. Each registration supplies
typed generated trampolines; algorithm implementations receive their concrete
state, not bytes or raw pointers. Initialization records whether private state
is live, and drop uses the selected algorithm's typed trampoline. The compile-
time size/alignment check prevents registration of a state that does not fit.

This mirrors `cc_algo` plus `cc_data` while containing Rust's layout and drop
requirements in one audited module.

### Selection lifecycle

- Parsed config uses `config::CongestionAlgorithm` with `Bbr` and `Cubic`.
- TCP initialization resolves the config value to `&'static Algorithm` once and
  stores it in `TcpPolicy`.
- New active connections copy that reference into `congestion::State` and
  initialize private state with the current MSS.
- Listeners retain the selected algorithm; accepted connections inherit it,
  matching VPP listener behavior.
- Existing connections do not change algorithm when a new config snapshot is
  published. A future live-change feature requires a separate state migration
  design and is out of scope.

Adding Cubic adds only Cubic private state, Cubic event functions, one
registration, and one config parse variant. It does not add worker slots,
Session backend combinations, TCP node functions, or Session transport types.

## Error handling

This work does not continue the repository-wide `RuntimeError::Invariant`
cleanup tracked by issue #124, but it must not add new catch-all errors.

- Unknown congestion config is a deserialization/config validation error before
  TCP initialization. It leaves `TCP_MAIN` unpublished.
- Duplicate algorithm name or invalid private-state layout is a programmer bug
  in static registration and fails at TCP initialization; it is not a packet
  error.
- Missing required graph registration during worker init is a typed Graph
  Runtime startup error carrying the node name. Worker state is not partially
  installed.
- Session/TCP state already installed is a typed worker initialization error.
  Replacement removes readiness from the old Session owner only after the new
  state is fully constructed; cleanup errors do not replace the primary error.
- Missing/stale TCP connection on an input packet is a typed TCP node outcome
  and counter/next decision. It is not flattened into `TcpError::Dispatch` or a
  control-plane `RuntimeError`.
- Session FIFO/resource failures before `push_header` abort the batch while it
  is still invisible. `push_header` has a commit contract: after entry it must
  complete without a recoverable half-commit error. Programming defects panic
  inside the worker execution boundary; they are not converted to lifecycle
  errors and continued.

Tests match typed variants and fields. They do not match error strings.

## Naming and removals

Remove these names and concepts:

- `Controller`
- `ConfiguredCongestionController`
- `TcpWorkerState<C, Seg>`
- `TcpWorkerStore<C>`
- `with_tcp_worker_mut`
- `SessionTxStrategy`
- `SessionPacketizedTx`
- `TransportInternalTx`
- `SessionPacketizedTransport`
- `TransportInternalTransport`
- `NodeRuntimeData`

Use module context rather than repeated prefixes:

- `congestion::State`: mutable connection-owned congestion state;
- `congestion::Algorithm`: immutable registered algorithm operations;
- `config::CongestionAlgorithm`: parsed selection because `Algorithm` is
  already the precise domain noun for the selected config field;
- `TransportTx`: transport TX metadata;
- `TransportSendParams` and `TxBatchBuffer`: existing transport-neutral facts.

No type is prefixed with `Configured`, `Dynamic`, `InternalTransport`, or an
algorithm/backend combination.

## Migration order

Each stage must leave one coherent interface; compatibility wrappers are not
allowed.

1. Add typed Graph Node State to Runtime and migrate existing
   `NodeRuntimeData` users. Delete the word carrier and untyped worker setter.
2. Move Session Queue attachments and Session worker ownership into its typed
   node state. Session configuration selects Local/SVM construction there.
3. Move one non-generic TCP worker into shared TCP Graph Node State. Delete TCP
   TLS, `TcpWorkerState<C, Seg>`, and `TcpWorkerStore<C>`.
4. Replace the Session TX strategy family with one transport registration and
   VPP-shaped TX operations. Keep TCP registered as `TransportTx::Peek`.
5. Replace `TcpConnection<C>` with concrete `TcpConnection` and private
   `congestion::State`; add BBR and Cubic registrations.
6. Fix the two obsolete error-string assertions already exposed by CI and run
   rustfmt.
7. Commit, push, and verify only through GitHub Actions. The macOS TCP Lab
   workflow owns host-side utun configuration and diagnostics.

No local Cargo build, test, TUN creation, route change, or host-network probe is
part of verification.

## Verification

Compile/behavior tests must prove:

- one Session state and one TCP state exist per Data Worker;
- Local/SVM changes Session construction without changing TCP node/process
  types;
- BBR/Cubic changes connection congestion behavior without changing worker or
  graph types;
- Graph worker cloning clones typed node state and worker replacement cannot
  mutate topology;
- Session Queue calls `push_header` once before a batch becomes graph-visible;
- ACK cleanup retains VPP `PEEK` semantics;
- exact timer tokens remain owned and dispatched by TCP;
- failures before commit leave Session/TCP/graph state unchanged;
- tests match typed errors, not display strings.

GitHub Actions is the only execution authority. Required green jobs are fmt,
workspace clippy, workspace tests on Linux/macOS, dataplane performance, and
TCP Lab reachability on macOS.

## Proposed interface approval

The following new interfaces require approval before implementation under
`AGENTS.md`:

1. `Node::State` and the typed process contract. Existing interfaces cannot own
   worker-local business state; `NodeRuntimeData` only carries opaque words.
2. A private Graph Runtime node-state storage/function table plus a typed
   worker replacement capability. Heterogeneous dynamic-plugin state cannot be
   stored or safely replaced by the current runtime without private erasure.
3. One shared TCP Graph Node State capability. Multiple TCP graph nodes need
   one connection/timer owner; independent node state would duplicate it.
4. `TransportTx` metadata, the single VPP-shaped `SessionTransport` interface,
   and worker-local `register_transport<T>()`. Existing strategy traits turn
   metadata into type multiplication; existing code has no Session-owned way
   for a transport plugin to install operations without owning Session state.
5. Private `congestion::State`, `congestion::Algorithm`, and fixed aligned
   private storage. Runtime algorithm selection cannot be represented by
   `TcpConnection<C>` without one of the rejected enum/dyn/duplication designs.
6. Amend the `AGENTS.md` TCP generic rule from `TcpConnection<S, C>` to one
   concrete `TcpConnection` owning private `congestion::State`. Leaving the old
   rule in place would make the approved implementation self-contradictory.

Approval means implementing this replacement design and deleting the listed
old interfaces, not layering adapters over them.

## Decision-impact review

### Point 1: TCP Lab is already behaviorally green

- Source: current project and GitHub Actions run `30090511488`.
- Evidence: `TCP Lab reachability [macOS]`, Linux/macOS clippy, allocation
  contract, HugeTLB, and performance jobs passed. The run failed only fmt and
  two obsolete error-string assertions in workspace tests.
- Impact: the architecture refactor is not required to prove current issue #79
  reachability. Mixing a runtime-wide typed-state migration into the issue #79
  closing commit substantially increases regression risk.
- Action: preserve the green TCP Lab workflow and make architecture changes in
  reviewable stages; do not rewrite CI or host utun setup while changing state
  ownership.

### Point 2: the repository context already requires typed Graph Node State

- Source: current project (`CONTEXT.md`).
- Evidence: it explicitly forbids `NodeRuntimeData`, global state vectors,
  dispatch-time downcasts, and public erased state.
- Impact: replacing only TCP TLS would create another temporary global owner
  and contradict an already-recorded architecture decision.
- Action: make Runtime typed node state the prerequisite; do not invent a TCP-
  local storage workaround.

### Point 3: all four runtime-polymorphism bans cannot coexist with arbitrary
runtime algorithm selection

- Source: Rust language mechanics plus VPP evidence; this is a reasoned
  inference, not a current-project behavior.
- Evidence: a runtime-selected value of unrelated concrete types needs a sum
  representation, dynamic dispatch, erased storage, or separately selected
  monomorphized owner. VPP uses ops plus fixed private storage.
- Impact: retaining `TcpConnection<C>` will inevitably reintroduce an enum,
  `dyn`, type erasure at a higher layer, or algorithm-specific workers.
- Action: explicitly approve the VPP-shaped private ops/storage exception and
  remove the generic from the connection and worker.

## Skill candidates from repeated work

### `vpp-design-audit`

- Trigger: any Hammer graph/session/transport/TCP/device refactor.
- Input: requested behavior, affected Hammer modules, relevant VPP subsystem.
- Output: vendored VPP evidence, ownership comparison, allowed call directions,
  rejected local abstractions, and executable verification seams.
- SKILL.md fit: yes. The process is repeated, repository-specific, and can
  enforce “vendored VPP first” before edits.

### `hammer-error-owner`

- Trigger: adding or changing `Result`, error variants, packet drops, startup
  cleanup, or error translation.
- Input: failing operation, owner, caller, recoverability, rollback state.
- Output: classification as packet outcome/config-resource failure/programmer
  bug, owning error type, source-preserving translation point, and typed tests.
- SKILL.md fit: yes. The same classification failures recur across Runtime,
  Service, and plugins and are already documented in `AGENTS.md`.

### `ci-only-network-lab`

- Trigger: macOS utun, Linux privileged networking, TCP Lab, or any request that
  forbids local execution.
- Input: workflow, target job, required host setup, expected observations.
- Output: workflow changes, diagnostic capture, push/run/watch sequence, and a
  concise failure report mapped back to code.
- SKILL.md fit: yes. It encodes an execution policy and repeatable GitHub
  Actions workflow rather than project architecture.

The claim that these three patterns are recurrent is supported by the current
thread and project files. Whether they recur across older unrelated repositories
requires more history than is available in this workspace.
