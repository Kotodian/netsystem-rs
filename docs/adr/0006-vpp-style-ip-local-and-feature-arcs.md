# VPP-Style IP Local Delivery and Feature Arcs

Status: accepted

Date: 2026-09-03

This ADR replaces the IP local-delivery and Feature Arc portions of ADR-0005
and the Feature Arc publication rule in ADR-0002. Feature Arc is
protocol-neutral interface infrastructure: declarations describe graph nodes,
enabled chains are selected by `sw_if_index`, and packet progress is stored in
the generic buffer header. IPv4 and IPv6 retain separate local nodes and local
protocol tables. This is a design record only; it changes no Rust
implementation.

## Context

The current `hammer-service::feature_arc` implementation is a generic object
graph built around `FeatureArc<A>`, `FeatureArcControl<A>`,
`FeatureArcStartHandle`, `FeatureArcStartSlot<A>`, and an `ArcSwap` snapshot.
Starting an arc performs shared snapshot loads and `HashMap` lookups keyed by
the current start node and `sw_if_index`. Advancing a feature loads the
snapshot again. It stores progress in IP-owned `NetworkOpaque` even though the
same cursor is used by device, interface, and protocol-neutral feature nodes.
`FeatureArcControl::publish` also discovers a barrier through global state.
These choices put pointer chasing, map lookup, and atomic reference traffic on
the packet path and attach generic Feature Arc state to the wrong owner.

The current IP local implementation has one `IpLocalNode`, one
`IpReceiveNode`, one `IpLocalArc`, and one `IpLocalNext` for both IPv4 and
IPv6. It publishes a separate `IpLocalState` snapshot, stores packet-path
runtime state in a process-wide `Mutex<Vec<_>>`, sends both drop and punt to
the generic `drop` node, and implements one source-check mode for two protocols
whose VPP validation rules differ substantially. IPv4 and IPv6 protocol
registration eventually update the same table.

Vendored VPP has two independent local paths. `ip4-local`, `ip4-receive`, and
`ip4-local-end-of-arc` are siblings with one IPv4 local-next table;
`ip6-local`, `ip6-receive`, and `ip6-local-end-of-arc` are a different sibling
set with a different table. Both sets start a concrete local Feature Arc only
after local validation succeeds. The end-of-arc node performs the final wire
protocol dispatch and does not repeat the head checks.

VPP Feature Arc registration assigns a compact `u8` arc index, topologically
orders each arc's features, and assigns feature indices. Per-interface enable
state compiles to a dense `config_index_by_sw_if_index` table and a shared
`u32` configuration heap. `vnet_feature_enable_disable` changes one interface's
compiled chain; it is called by interface lifecycle, feature owners, CLI, or
Binary API code and is not itself a Binary API abstraction.
`vnet_feature_arc_start` is different: an arc head calls it for a packet after
the head's own validation, passing the selected `sw_if_index`. It performs a
bitmap test, loads that interface's configuration index, reads the first next
slot, and advances `vlib_buffer_t::current_config_index`. Each feature node then
reads its own configuration words and the following next slot from that same
heap. The packet does not carry a Feature Arc object, per-arc handle, or IP
opaque cursor.

## Decision

### Ownership and module boundary

Feature Arc moves to `hammer-service::interface::feature` and is owned by
`InterfaceMain`. `NetMain` continues to own `InterfaceMain`; it does not expose
a second Feature Arc interface. There is no independent Feature Arc global,
`FeatureMain`, attached start handle, generic arc object, or plugin-specific
state stored by `hammer-service`.

Concrete plugins own declarations:

- the IP plugin declares `ip4-local` and `ip6-local`, their concrete start and
  end nodes, and any other IP arcs it implements;
- a feature plugin registers its concrete graph node, name, and ordering
  constraints against an existing arc;
- the service resolves declarations, sorts them, installs graph next slots,
  compiles per-interface chains, and owns only protocol-neutral indices and
  configuration words;
- the feature plugin owns the meaning and explicit encoding/decoding of its
  configuration words. Service never stores the plugin's Rust type or a
  callback table.

The existing private `InterfaceState` adds exactly one field,
`feature: FeatureState`. The added Feature Arc types have the following full
shape:

```rust
struct FeatureState {
    arc_registrations: Vec<FeatureArcRegistration>,
    feature_registrations: Vec<FeatureRegistration>,
    arcs: Vec<FeatureArc>,
    arc_index_by_name: BTreeMap<&'static str, u8>,
    shared_config_heap: Vec<u32>,
    free_config_ranges: BTreeMap<u32, u32>,
    installed: bool,
}

struct FeatureArcRegistration {
    name: &'static str,
    start_nodes: Box<[NodeId]>,
    last_in_arc: Option<&'static str>,
}

struct FeatureRegistration {
    arc_name: &'static str,
    name: &'static str,
    node: NodeId,
    runs_before: Box<[&'static str]>,
    runs_after: Box<[&'static str]>,
}

struct FeatureArc {
    name: &'static str,
    start_nodes: Box<[NodeId]>,
    default_end_node: NodeId,
    feature_nodes: Box<[NodeId]>,
    feature_names: Box<[&'static str]>,
    feature_index_by_name: BTreeMap<&'static str, u32>,
    config_index_by_sw_if_index: Vec<u32>,
    feature_count_by_sw_if_index: Vec<i16>,
    sw_if_index_has_features: Bitmap,
    chains: Pool<FeatureChain>,
    chain_by_words: HashMap<Box<[u32]>, u32>,
}

struct FeatureChain {
    occurrences: Vec<FeatureOccurrence>,
    end_node: NodeId,
    heap_index: u32,
    heap_len: u32,
    reference_count: u32,
}

struct FeatureOccurrence {
    feature_index: u32,
    words: Box<[u32]>,
}
```

`FeatureChain::occurrences` is sorted by `feature_index` and may contain
repeated occurrences with different or identical words, matching VPP add/del
semantics. `heap_index` names the first packet-visible word; `heap_len` counts
the whole allocated extent including the preceding pool back-pointer and final
end-node key word. `chain_by_words` owns the same key material used for lookup;
it does not point into the movable heap.

`FeatureState` is private implementation state of the existing
`InterfaceMain`; it is not another Main or an access wrapper. `UnsafeCell`
continues to serve `InterfaceMain`'s existing publication model. Startup
registration is legal only before workers start. A live enable, disable, or end
node change receives the main `DataPlaneMain` context and performs the actual
mutation inside `worker_thread_barrier_sync!`; workers only read between
barrier releases.
There is no lock, atomic pointer, reference-counted snapshot, or second
publication protocol around `FeatureState`.

The maps, registration records, configuration pool, and free-range map are
control-plane structures. None is read while processing a packet. Ordinary
collections allocate through Hammer's process Main Heap. `Bitmap` and `Pool`
reuse the existing `hammer-infra` implementations. `NodeRuntime` is not a
field of `FeatureState` or `InterfaceMain`: its `Rc<RefCell<_>>` state is
main-thread-local. Startup installation borrows it only for the call, and live
topology changes use the main-thread `DataPlaneMain::nodes()` borrow already
present in the mutation context.

### Registration macros and installation

The complete registration interface is:

```rust
impl InterfaceMain {
    pub fn register_feature_arc(
        &self,
        name: &'static str,
        start_nodes: &[NodeId],
        last_in_arc: Option<&'static str>,
    ) -> Result<u8, FeatureError>;

    pub fn register_feature(
        &self,
        arc_name: &'static str,
        name: &'static str,
        node: NodeId,
        runs_before: &[&'static str],
        runs_after: &[&'static str],
    ) -> Result<(), FeatureError>;

    pub fn install_feature_arcs(&self, nodes: &NodeRuntime) -> Result<(), FeatureError>;

    pub fn feature_arc_index(&self, name: &str) -> Option<u8>;

    pub fn feature_index(&self, arc_index: u8, name: &str) -> Option<u32>;
}
```

`register_feature_arc` assigns the next compact `u8` index immediately and
rejects the 256th arc, duplicate names, an empty start-node set, or duplicate
start nodes. `register_feature` records a declaration only. It rejects
duplicate `(arc_name, name)` and conflicting reuse of one graph node inside an
arc. It does not allocate a stable feature index before topology is known.

`install_feature_arcs` runs once after all graph nodes and Feature Arc
declarations are
available and before Data Workers start. For each arc it:

1. resolves every feature's before/after names inside that same arc;
2. when `last_in_arc` is present, requires that registered feature and adds a
   `feature -> last_in_arc` constraint for every other feature, matching VPP;
3. rejects missing constraint targets and cycles;
4. stores the topological order in `feature_nodes` and uses its zero-based
   position as the `u32` feature index;
5. uses the last ordered feature node as the default end node;
6. validates every start and feature node against the borrowed `NodeRuntime`;
   the borrow ends with installation and is never stored in `InterfaceMain`.

Registration closes after `install_feature_arcs`. Runtime-loaded code cannot
add or reorder an arc or feature after workers start; loading a new feature
declaration requires a process restart. Runtime enable/disable remains
supported. This keeps indices and all existing per-interface configuration
meanings stable and avoids a second graph-rewrite lifecycle during additive
DSO loading.

The existing `#[feature_arc]` and `#[feature]` proc-macro names are retained but
their marker-type implementation is replaced. Both attributes apply to the
real graph-node struct and generate only a direct owner registration method:

```rust
#[feature_arc(
    name = "ip4-local",
    start_nodes = [Ip4LocalNode, Ip4ReceiveNode],
    last_in_arc = Ip4LocalEndOfArcNode
)]
#[graph_node(graph = ip, name = "ip4-local", role = internal, next = Ip4LocalNext)]
pub struct Ip4LocalNode;

#[graph_node(
    graph = ip,
    name = "ip4-receive",
    role = internal,
    sibling_of = Ip4LocalNode,
)]
pub struct Ip4ReceiveNode;

// Generated on the real arc-head node.
impl Ip4LocalNode {
    pub const FEATURE_ARC_NAME: &'static str = "ip4-local";

    pub fn register_feature_arc(
        interfaces: &InterfaceMain,
        nodes: &NodeRuntime,
    ) -> Result<u8, FeatureError> {
        let ip4_local_node = nodes
            .node_by_name(Ip4LocalNode::NODE_NAME)
            .ok_or(FeatureError::NodeNotFound {
                name: Ip4LocalNode::NODE_NAME,
            })?;
        let ip4_receive_node = nodes
            .node_by_name(Ip4ReceiveNode::NODE_NAME)
            .ok_or(FeatureError::NodeNotFound {
                name: Ip4ReceiveNode::NODE_NAME,
            })?;
        interfaces.register_feature_arc(
            Self::FEATURE_ARC_NAME,
            &[ip4_local_node, ip4_receive_node],
            Some(Ip4LocalEndOfArcNode::NODE_NAME),
        )
    }
}

#[feature(arc = Ip4LocalNode)]
#[graph_node(
    graph = ip,
    name = "ip4-local-end-of-arc",
    role = internal,
    sibling_of = Ip4LocalNode,
)]
pub struct Ip4LocalEndOfArcNode;

// Generated on the real feature node.
impl Ip4LocalEndOfArcNode {
    pub fn register_feature(
        interfaces: &InterfaceMain,
        nodes: &NodeRuntime,
    ) -> Result<(), FeatureError> {
        let node = nodes
            .node_by_name(Self::NODE_NAME)
            .ok_or(FeatureError::NodeNotFound {
                name: Self::NODE_NAME,
            })?;
        interfaces.register_feature(
            Ip4LocalNode::FEATURE_ARC_NAME,
            Self::NODE_NAME,
            node,
            &[],
            &[],
        )
    }
}
```

`#[feature]` also accepts Graph Node type paths in `runs_before = [...]` and
`runs_after = [...]`; the generated method passes their `NODE_NAME` constants
directly. An independent feature plugin therefore has an explicit Rust
dependency on the plugin that owns the arc and on any node named in its order
constraints. The macros do not
create a marker enum, trait implementation, handle, registration image,
snapshot, ABI carrier, erased value, or barrier call. The owning plugin's
existing `init_function` invokes the generated methods with the startup
`NodeRuntime` borrow and stores a returned arc index in its own Main when
needed.

The vendored `VNET_FEATURE_ARC_ORDER` bulk-constraint declaration has no caller
outside its definition. Hammer does not add a third macro or registration type:
per-feature `runs_before`/`runs_after` plus `last_in_arc` express every ordering
used by the vendored tree. Hammer also does not copy
`enable_disable_cb` or the process-wide `vnet_feature_register` observer list.
A feature owner's control method validates and updates its own state, then
calls `InterfaceMain::{enable_feature,disable_feature}` explicitly. The current
Hammer graph has no adjacency, device, or tunnel feature-config cache that
requires observer invalidation, and `hammer-service` must not retain
plugin-owned callbacks. A future cache must be updated by its concrete owner in
the same control transaction; it does not reopen a generic callback registry.

### Configuration compilation and lifetime

The interface control methods use numeric indices after startup name
resolution:

```rust
impl InterfaceMain {
    pub fn enable_feature(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        feature_index: u32,
        sw_if_index: u32,
        config: &[u32],
    ) -> Result<(), FeatureError>;

    pub fn disable_feature(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        feature_index: u32,
        sw_if_index: u32,
        config: &[u32],
    ) -> Result<(), FeatureError>;

    pub fn is_feature_enabled(
        &self,
        arc_index: u8,
        feature_index: u32,
        sw_if_index: u32,
    ) -> Result<bool, FeatureError>;

    pub fn modify_feature_arc_end(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        sw_if_index: u32,
        end_node: NodeId,
    ) -> Result<(), FeatureError>;

    pub fn reset_feature_arc_end(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        sw_if_index: u32,
    ) -> Result<(), FeatureError>;
}
```

A zero-word feature uses `&[]`. A plugin needing structured configuration
explicitly encodes its own fields into `u32` words and reads the known number of
words in its node. As in VPP, configuration length belongs to each enabled
occurrence rather than to Feature Arc registration. No Rust object
representation, padding, address, trait object, or representation attribute
crosses the service seam.

Enable adds one `(feature_index, config)` occurrence and disable removes one
exact occurrence, matching VPP's reference-counted configuration behavior.
Disabling an occurrence that is absent is a successful no-op. The ordered
feature list is sorted by the installed feature index. The selected end node is
the interface override when present and the arc default otherwise.

For a chain `A -> B -> end`, the shared heap contains this extent:

```text
[chain pool index]       <- control-only back-pointer; not exposed to packets
[start-to-A next]
[A config words...]
[A-to-B next]
[B config words...]
[B-to-end next]
[end node id]            <- deduplication key suffix; not read by packets
```

All next values are validated Hammer-local `u16` graph slots stored in `u32`
words so configuration data remains naturally four-byte aligned. When an arc
has multiple start nodes, adding `start -> first feature` on every start node
must return the same local next slot. A mismatch is an installation/configuration
error; IP local start nodes satisfy this through graph sibling semantics.

`config_index_by_sw_if_index` and `FeatureChain::heap_index` point at the first
packet-visible next word, one word after the pool back-pointer. The compiled
packet words plus the final `end node id` form the `chain_by_words` key, matching
VPP's inclusion of the end node in `config_string_hash`. The pool back-pointer
locates the owning `FeatureChain` during control-plane replacement, so no
parallel heap-index-to-pool-index vector exists.

`FeatureChain` values are deduplicated per arc by that key.
`reference_count` counts interfaces using the exact chain. New chains allocate
one contiguous extent in `shared_config_heap`; released zero-reference chains return their
extent to `free_config_ranges`, which coalesces adjacent ranges. The next
allocation uses the lowest fitting range before extending the heap. Heap
growth or compaction happens only while workers are stopped; published indices
remain unchanged until the corresponding interface table entry is replaced.

Feature chain compilation can require several graph edges. The runtime adds one
generic failure-atomic operation rather than exposing a Feature-specific graph
interface:

```rust
impl NodeRuntime {
    pub fn add_node_next_slots(
        &self,
        edges: &[(NodeId, NodeId)],
    ) -> RuntimeResult<Vec<u16>>;
}
```

The return has one node-local slot per input edge. The method validates every
node, sibling group, existing edge and final `u16` count, and reserves every
affected vector before inserting the first edge. A recoverable error therefore
leaves the graph unchanged. Existing `add_node_next_slot` delegates the
one-edge case to the same implementation. An actual main-graph insertion sets
ADR-0003's coalesced worker Graph Refork flag; an all-existing edge set does
not. The outermost barrier release publishes and reforks the worker graph once.

One Feature Arc mutation is ordered as follows:

1. validate arc, feature, live software interface, end node, configuration
   words, and the complete candidate occurrence list;
2. build the candidate sequence and required graph edges in control-local
   temporary storage without mutating the published heap;
3. enter `worker_thread_barrier_sync!` and allocate an unreachable candidate
   heap extent and pool slot large enough for the final chain;
4. call `add_node_next_slots` and patch its `u16` slots into the candidate
   sequence; on error, reclaim the unreachable candidate and return with the
   graph and published interface chain unchanged;
5. deduplicate or install the fully written candidate `FeatureChain` and retain
   it for the interface;
6. replace `config_index_by_sw_if_index`, feature count, and presence bitmap;
7. release the previous chain and reclaim it only at zero references.

Before step 3, a failure leaves all state unchanged. The candidate allocation
is unreachable from workers and has an owner-scoped rollback path. After the
batch graph operation succeeds, the remaining writes, table replacement and
old-chain release are infallible. Heap growth and every shared `Vec`
reallocation occur only while workers are stopped. Startup calls perform the
same mutation before Data Workers exist and therefore do not enter a barrier.
There is no snapshot clone, whole-table rebuild, or deferred release list.

### Packet-path API and layout

The packet-path API is:

```rust
impl InterfaceMain {
    #[inline(always)]
    pub fn start_feature_arc(
        &self,
        arc_index: u8,
        sw_if_index: u32,
        buffer: &mut Buffer,
        default_next: u16,
    ) -> u16;

    #[inline(always)]
    pub fn next_feature(&self, buffer: &mut Buffer) -> u16;

    #[inline(always)]
    pub fn next_feature_with_config<R>(
        &self,
        buffer: &mut Buffer,
        config_words: usize,
        read: impl for<'a> FnOnce(&'a [u32], u16) -> R,
    ) -> R;
}
```

`start_feature_arc` performs exactly one bounds-safe bitmap test. If the
interface has no features it returns `default_next` without touching packet
configuration state. Otherwise it loads the dense
`config_index_by_sw_if_index` entry, reads the first heap word as the next slot,
writes `heap_index + 1` as the buffer's current configuration index, and
returns the slot.

`next_feature` reads the heap word at the current index, advances by one, and
returns the next slot. `next_feature_with_config` lends exactly `config_words`
preceding words and the following next slot to a non-escaping closure, then
advances by `config_words + 1`. The closure form prevents a plugin from
retaining a heap slice across the next worker barrier while remaining
monomorphized and allocation-free.

The existing generic buffer header already reserves
`current_config_or_punt: u32`, but its Rust accessors currently reinterpret the
value as `NodeId`. The field remains one generic `u32`; only its accessors are
corrected:

```rust
impl Buffer {
    pub fn current_config_index(&self) -> u32;
    pub fn set_current_config_index(&mut self, index: u32);
}
```

The same rename and `u32` contract propagates through `DataPlaneBuffers` and
the buffer-pool forwarding methods. Existing handoff code stores
`resolved_node.slot()` and reconstructs `NodeId::new(current_config_index)` at
its own seam; the buffer header does not claim that every use of the cursor is
a node identity. `NetworkOpaque` gains no Feature Arc field or accessor.

An arc index is needed only by the start node selecting an arc. After
`start_feature_arc`, the globally shared heap index is sufficient, exactly as
in VPP. The fixed buffer-header size and cacheline assertions remain mandatory.

The hot path therefore contains no map lookup, node-name lookup, allocation,
lock, atomic snapshot load, or reference-count operation. Its memory reads are
one bitmap word, one dense interface index, and sequential four-byte-aligned
heap words. Packet batching may borrow `InterfaceMain` for one node invocation,
but no heap slice may be stored in node state.

### Enable and arc-start timing

Registration, enablement, and packet traversal are three separate operations:

1. During startup, plugin init functions call the methods generated by
   `#[feature_arc]` and `#[feature]`. `InterfaceMain::install_feature_arcs`
   closes registration and compiles the topology before Data Workers start.
2. A feature owner or interface lifecycle operation calls
   `InterfaceMain::enable_feature` or `disable_feature` when that feature must
   become active or inactive on one existing `sw_if_index`. For example, an IP
   interface add/delete path may enable or disable its `not-enabled` feature;
   a plugin control method may change its own feature. A packet node never
   enables a feature.
3. A concrete arc-head node calls `start_feature_arc` for each eligible packet,
   after that node has completed its own validation and selected the effective
   RX or TX `sw_if_index`. A feature node calls `next_feature` or
   `next_feature_with_config` exactly once after applying its behavior.

`enable_feature` first proves that `sw_if_index` names a live `SwInterface`.
When `InterfaceMain` deletes a software interface, the same owner transaction
clears that index from every arc, releases its referenced configurations,
zeros its feature counts, and clears its bitmap bits before the interface pool
slot can be reused. A reused index therefore cannot inherit the previous
interface's Feature Arc chain.

Feature Arc defines no Binary API messages, method names, payloads, statuses,
or dispatcher dependency. A Binary API, CLI, interface callback, or another
owner-defined control path may call the same `InterfaceMain` methods, but that
control ingress is outside this module's interface.

Startup registration and initial interface construction happen before workers
and require no barrier. A live enable, disable, or end-node change is an
`InterfaceMain` publication operation. It accepts the main
`&mut DataPlaneMain`, validates and prepares the replacement first, and enters
`worker_thread_barrier_sync!` only for the final worker-visible replacement and
old-config release. Calls nested inside another main-thread barrier use the
barrier's existing recursion semantics. Read-only `is_feature_enabled` and all
packet start/next operations never enter or inspect the barrier.

### IP local owner state and node inventory

ADR-0005's two concrete IP Main owners remain. ADR-0006 replaces only their
local-delivery fields with the following exact fields:

| Owner | Field | Type | Invariant |
| --- | --- | --- | --- |
| `Ip4Main` | `local_next_by_ip_protocol` | `[u16; 256]` | initialized entirely to the IPv4 punt slot |
| `Ip4Main` | `local_feature_arc_index` | `u8` | returned by registering `ip4-local` |
| `Ip6Main` | `local_next_by_ip_protocol` | `[u16; 256]` | initialized entirely to the IPv6 punt slot |
| `Ip6Main` | `local_feature_arc_index` | `u8` | returned by registering `ip6-local` |

The IP plugin adds six zero-sized graph-node owners and two independent static
next enums:

```rust
pub struct Ip4LocalNode;
pub struct Ip4ReceiveNode;
pub struct Ip4LocalEndOfArcNode;

#[repr(u16)]
pub enum Ip4LocalNext {
    Drop = 0,
    Punt = 1,
    FullReassembly = 2,
}

pub struct Ip6LocalNode;
pub struct Ip6ReceiveNode;
pub struct Ip6LocalEndOfArcNode;

#[repr(u16)]
pub enum Ip6LocalNext {
    Drop = 0,
    Punt = 1,
    FullReassembly = 2,
}
```

`Ip4ReceiveNode` and `Ip4LocalEndOfArcNode` are graph siblings of
`Ip4LocalNode`; the IPv6 equivalents are siblings of `Ip6LocalNode`. Siblings
share their concrete next-slot table. IPv4 and IPv6 are not siblings and never
share slots or local protocol state.

The three static next slots resolve to concrete IPv4/IPv6 drop, punt, and local
full-reassembly nodes. Drop and punt are distinct paths. Protocol consumers are
added dynamically after these static slots. No ICMP node name is embedded in
the IP node declaration.

The concrete arc registrations are:

```rust
let ip4_local_arc_index = Ip4LocalNode::register_feature_arc(
    interfaces,
    nodes,
)?;
Ip4LocalEndOfArcNode::register_feature(interfaces, nodes)?;

let ip6_local_arc_index = Ip6LocalNode::register_feature_arc(
    interfaces,
    nodes,
)?;
Ip6LocalEndOfArcNode::register_feature(interfaces, nodes)?;
```

These calls return numeric indices stored in the concrete Main owners. They do
not create marker structs, protocol enums, attached handles, a registration
image, or a family-level facade. The generated arc registrations name each
end-of-arc feature through `last_in_arc`, so installation orders it last and
uses its node as the default end.

### Local protocol registration

The IP owner exports four explicit initialization APIs:

```rust
pub fn register_ip4_protocol(
    nodes: &NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()>;
pub fn unregister_ip4_protocol(protocol: u8) -> RuntimeResult<()>;
pub fn register_ip6_protocol(
    nodes: &NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()>;
pub fn unregister_ip6_protocol(protocol: u8) -> RuntimeResult<()>;
```

Registration adds `node` as a next of the concrete local root, obtains that
root's local `u16` slot, and stores it in only the matching 256-entry table.
Sibling next-table sharing makes the same slot valid from receive and
end-of-arc nodes. Unregister restores the matching punt slot; the monotonic
graph edge remains installed. These APIs are initialization/lifecycle APIs,
not packet dispatch or DPO registration APIs.

Startup registration runs before workers. A live plugin load or retirement
invokes the same owner APIs only inside the plugin loader's existing outer
worker-barrier transaction, because that transaction also owns node insertion,
Graph Refork, and DSO lifetime. Protocol registration does not discover or
enter another barrier; `NodeRuntime::add_node_next_slot` marks the graph change
for the outer release. The table slot is published before load release and is
restored to punt before the retiring node or DSO can become unreachable.

The ICMP plugin loads after IP, registers its concrete ICMPv4 and ICMPv6 input
nodes through the two matching registration functions, and unregisters them
before plugin retirement. TCP and UDP do the same for their concrete wire
protocols. Until a consumer registers, the protocol punts. The IP plugin still
knows the ICMP wire protocol numbers required for IPv4/IPv6 local checksum and
source-validation rules; it does not own ICMP message parsing, type dispatch,
echo handling, or error generation.

### Common local graph sequence

The local path is:

```text
local adjacency -> ip4-local
receive DPO     -> ip4-receive
ip4-local or ip4-receive
  -> IPv4 head validation
  -> start ip4-local Feature Arc when validation passed
  -> zero or more feature nodes
  -> ip4-local-end-of-arc when at least one feature ran
  -> IPv4 local-next table indexed by wire protocol

local adjacency -> ip6-local
receive DPO     -> ip6-receive
ip6-local or ip6-receive
  -> IPv6 head validation
  -> start ip6-local Feature Arc when validation passed
  -> zero or more feature nodes
  -> ip6-local-end-of-arc when at least one feature ran
  -> IPv6 local-next table indexed by wire protocol
```

At the head, the node computes `protocol_next` from its concrete local table
before calling `InterfaceMain::start_feature_arc`; it passes that slot as
`default_next`.
With no enabled feature, the packet goes directly to its protocol consumer.
With features, the compiled chain ends at the concrete end-of-arc node, which
reloads `protocol_next` and dispatches it. The end node never restarts the arc,
repeats source lookup, or repeats transport validation.

Any local validation error records the concrete node error and selects the
concrete drop slot without starting the Feature Arc. An unregistered wire
protocol is not a malformed packet: its table entry selects the concrete punt
slot and may still traverse enabled local features first.

### IPv4 local behavior

`Ip4LocalNode` and `Ip4ReceiveNode` share one private IPv4 frame function with
a constant `is_receive_dpo` argument. `Ip4LocalEndOfArcNode` calls only the
final-dispatch portion. The head performs this order for each packet:

1. read the IPv4 header and wire protocol from the validated packet cursor;
2. classify fragments first. A fragment selects `FullReassembly` and skips
   transport checksum and source checks;
3. classify packets already marked as translated. They use their protocol
   next and skip the same checks;
4. for ordinary TCP/UDP, respect valid hardware-offload/computed checksum
   facts; otherwise validate the IPv4 pseudo-header checksum. UDP checksum zero
   remains valid for IPv4, and UDP length must not exceed the IPv4 payload;
5. choose the source-lookup FIB from the concrete packet `fib_index`, applying
   only the separate IP-owned `fib_index_override` fact when present. RX/TX
   `sw_if_index` is never reinterpreted as a FIB index;
6. choose the effective local interface from `sw_if_index[RX]`. On the receive
   path, a `ReceiveDpo` with an actual interface overrides this value; a receive
   object without one leaves the packet RX interface unchanged;
7. perform the concrete IPv4 source FIB lookup and store the resulting
   load-balance/DPO metadata required by later processing;
8. reject a source whose selected child is receive as a spoofed local source;
9. reject a source with an empty baked uRPF list, except destination
   `255.255.255.255`, which remains valid for broadcast bootstrap traffic;
10. on success, start `ip4-local` with the effective interface and the selected
    protocol/reassembly next.

The per-frame source/FIB/result cache is a stack local matching VPP's
`ip4_local_last_check_t`. It is not process-global or node-shared, and a packet
that already has an error does not seed it.

The IPv4 node-local errors are:

```rust
pub enum Ip4LocalError {
    TcpChecksum,
    UdpChecksum,
    UdpLength,
    SourceLookupMiss,
    SpoofedLocalSource,
}
```

### IPv6 local behavior

`Ip6LocalNode` and `Ip6ReceiveNode` share one private IPv6 frame function with
a constant `is_receive_dpo` argument. `Ip6LocalEndOfArcNode` calls only final
dispatch. The head performs this order for each packet:

1. read the IPv6 header and walk the already bounds-checked extension-header
   chain to locate the effective upper-layer protocol and transport offset;
2. apply the IPv6 node's concrete UDP/ICMP validation branches while preserving
   the base wire protocol as the local-next table key;
3. respect valid hardware-offload/computed checksum facts; otherwise let the
   concrete UDP and ICMPv6 branches validate their IPv6 pseudo-header checksum;
4. validate UDP length and reject malformed extension/transport length. An
   explicit zero UDP checksum follows the vendored VPP local behavior rather
   than being silently reclassified by a shared IPv4 helper;
5. use the concrete packet `fib_index` and separate override fact for source
   lookup, never an interface index;
6. derive the effective local interface from RX and apply a receive object's
   actual interface exactly as on IPv4;
7. perform loose uRPF source validation except for ICMPv6 and link-local
   unicast sources, preserving neighbor-discovery behavior;
8. on success, start `ip6-local` with the effective interface and the selected
   protocol next.

IPv6 extension processing, checksum rules, and source-check exemptions are not
implemented by calling the IPv4 function with a protocol parameter. Sharing
is limited to protocol-neutral Feature Arc and frame/next helpers.

The IPv6 node-local errors are:

```rust
pub enum Ip6LocalError {
    BadLength,
    UdpChecksum,
    IcmpChecksum,
    UdpLength,
    SourceLookupMiss,
}
```

### Synchronization and failure handling

Feature declaration errors are startup errors and abort installation before
workers start. Live interface-feature validation, capacity/allocation, or graph
errors return the owner-local `FeatureError` without modifying the selected
interface chain. A control ingress that exposes the operation translates that
error at its own real seam.

```rust
pub enum FeatureError {
    ArcLimit { requested: usize },
    ArcNotFound { name: &'static str },
    DuplicateArc { name: &'static str },
    EmptyStartNodes { arc: &'static str },
    EmptyArc { arc: &'static str },
    NodeNotFound { name: &'static str },
    GraphNodeNotFound { node: NodeId },
    DuplicateStartNode { arc: &'static str, node: NodeId },
    DuplicateFeature { arc: &'static str, feature: &'static str },
    FeatureNodeConflict { arc: &'static str, node: NodeId },
    ConstraintTargetMissing {
        arc: &'static str,
        feature: &'static str,
        target: &'static str,
    },
    OrderCycle { arc: &'static str },
    RegistrationClosed,
    NotInstalled,
    ArcIndexInvalid { arc_index: u8 },
    FeatureIndexInvalid { arc_index: u8, feature_index: u32 },
    InterfaceNotFound { sw_if_index: u32 },
    FeatureCountOverflow { arc_index: u8, sw_if_index: u32 },
    StartNextMismatch {
        arc_index: u8,
        node: NodeId,
        expected: u16,
        actual: u16,
    },
    NextSlotOverflow { node: NodeId, requested: usize },
    StorageExhausted { requested_words: usize },
}
```

Packet errors never construct `FeatureError`. An invalid published config
index, truncated heap sequence, or next value exceeding `u16` is an impossible
owner invariant and asserts with the arc/interface/index facts; continuing
would route a packet through corrupted graph state. Attempting graph mutation
from a Data Worker is likewise a programmer invariant, not a recoverable
Feature error. `NodeRuntime` node absence and next-count overflow are
translated once into the two concrete graph variants above; there is no
catch-all runtime wrapper.

There is no public manual publication method. `InterfaceMain` performs each
live worker-visible replacement inside the exported barrier macro, and workers
observe the final state only after release.

## Consequences

Feature Arc becomes a deep `InterfaceMain` abstraction rather than a handle
graph: plugins declare ordering and configuration, interfaces select enabled
chains, and the packet path sees only numeric slots and sequential words. The
same implementation supports IP, device, and protocol-neutral arcs without
teaching service about any protocol.

IPv4 and IPv6 local delivery have separate nodes, validation, errors, tables,
and arc indices. This intentionally retains semantic duplication where the two
wire protocols differ, while removing duplicated Feature Arc plumbing. ICMP
remains an independent plugin with an explicit dependency on IP.

The migration is source-breaking. Existing in-tree callers move at once;
there is no compatibility alias, deprecated handle API, dual publication
path, or migration flag. No persisted data, external database format, or wire
protocol changes.

## 变更清单

### 新增

| 类型/API | 位置或标识 | 字段/签名与行为 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 私有类型 | `hammer-service::interface::feature::FeatureState` | registrations、installed arcs、name index、shared `u32` heap、free ranges和 installed flag；完整字段见 Decision | `InterfaceMain` 私有实现；不保存 `NodeRuntime`，无独立 Main/global/handle/ABI | lifecycle and heap reuse tests |
| 私有类型 | `FeatureArcRegistration`, `FeatureRegistration` | 完整字段见 Decision；arc 使用 `last_in_arc`，feature 使用 concrete node 与顺序约束 | 只存在于 startup registration phase；不增加 DSO image | registration/order tests |
| 私有类型 | concrete `FeatureArc`, `FeatureChain`, `FeatureOccurrence` | 完整字段见 Decision；dense per-interface tables、chain pool、dedupe/refcount、heap back-pointer 和连续 extent | 替代 generic arc/chain/step types；无冗余 heap-index map | configuration sharing/lifetime tests |
| error enum | `FeatureError` | 完整 variants 见 Decision；startup declaration、interface、graph、storage failure | 替代 `FeatureArcError`；packet path invariant 不返回 error | concrete-variant and failure-atomic tests |
| API | `InterfaceMain::{register_feature_arc,register_feature,install_feature_arcs,feature_arc_index,feature_index}` | 完整签名见 Decision；startup-only register/install | plugins migrate from marker traits/control object | plugin declaration and topology tests |
| API | `InterfaceMain::{enable_feature,disable_feature,is_feature_enabled,modify_feature_arc_end,reset_feature_arc_end}` | numeric per-interface control；live mutation在 owner 内使用 barrier 宏 | existing callers migrate；不依赖 Binary API | startup/no-op/live mutation tests |
| API | `InterfaceMain::{start_feature_arc,next_feature,next_feature_with_config}` | `Buffer` cursor + bitmap + dense index + shared heap；config borrow不能逃逸 closure | graph nodes migrate from attached start handles | packet graph tests and benchmark |
| API | `NodeRuntime::add_node_next_slots` | 对一组 `(node, next)` 先完整验证和 reserve，再 failure-atomic 地返回逐边 `u16` slot；实际新增主图边时设置一次 coalesced Graph Refork 请求 | generic runtime graph primitive；不引入 Feature-specific graph wrapper | recoverable-error atomicity, existing-edge no-op and one-refork tests |
| graph nodes | `Ip4LocalNode`, `Ip4ReceiveNode`, `Ip4LocalEndOfArcNode`, `Ip6LocalNode`, `Ip6ReceiveNode`, `Ip6LocalEndOfArcNode` | six zero-sized concrete nodes; separate sibling sets | replace shared local/receive node | local graph registration and execution tests |
| next enums | `Ip4LocalNext`, `Ip6LocalNext` | `Drop`, `Punt`, `FullReassembly` at fixed `u16` slots | dynamic protocol slots follow static slots | next-table and sibling tests |
| error enums | `Ip4LocalError`, `Ip6LocalError` | exact node-local variants defined above | replace mixed `IpLocalError` | per-protocol packet error tests |
| API | `unregister_ip4_protocol`, `unregister_ip6_protocol` | restore only the matching table entry to concrete punt | complements explicit registration APIs | registration lifecycle test |

### 修改

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| type/module | `hammer-service::feature_arc` -> private `hammer-service::interface::feature` | generic handle module becomes `InterfaceMain` implementation | source-breaking module move; no re-export alias | workspace compile and ownership test |
| type | `InterfaceMain`/private `InterfaceState` | adds private `feature: FeatureState`; owns registration and all `sw_if_index` keyed configuration | no `NetMain::feature_main()` or separate Feature Main | initialization, interface lifecycle and access tests |
| type | `Ip4Main`, `Ip6Main` | local-next becomes separate `[u16; 256]`; each adds only its concrete local arc index | existing other ADR-0005 fields unchanged | initialization and independent-table tests |
| type/API | `Buffer`, `DataPlaneBuffers`, buffer-pool forwarding methods | preserve existing `current_config_or_punt: u32`; replace NodeId-typed `current_config`/`set_current_config` accessors with `current_config_index() -> u32` and `set_current_config_index(u32)` | handoff explicitly converts at its own seam; no buffer layout change | compile, header layout, handoff and feature-chain tests |
| proc macros | `#[feature_arc]`, `#[feature]` | attributes move from marker enum/trait generation to direct registration methods on real graph-node structs；arc/start/end/order arguments are Graph Node type paths，name 来自 `NODE_NAME`；arc head gains `FEATURE_ARC_NAME`；完整 expansion 见 Decision | invocation syntax breaking；no generated state/image/barrier | macro expansion compile and plugin init tests |
| API | `NodeRuntime::add_node_next_slot` | one-edge call uses the batch implementation；actual main-graph insertion requests ADR-0003 refork，existing edge is a no-op | behavior change only for worker visibility | single-edge and live worker graph tests |
| API | `register_ip4_protocol`, `register_ip6_protocol` | 接受 startup `&NodeRuntime` borrow，add next on concrete local root and update only matching table | signatures remain explicit; shared implementation removed | ICMP/TCP/UDP registration tests |

### 删除

| 类型/API | 位置或标识 | 删除内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| types | `FeatureArc<A>`, `FeatureArcControl<A>`, `FeatureArcStartHandle`, `FeatureArcStartSlot<A>`, `FeatureArcStart`, `FeatureArcInner<A>` | handle/snapshot/control hierarchy | direct `InterfaceMain` methods; no aliases | workspace compile and API inventory |
| traits | `FeatureArcSpec`, `Feature<A>`, `FeatureArcStartNode<A>` | generic marker identities and allocation-returning ordering methods | macros generate direct registration methods on real nodes | trait removal and plugin compile checks |
| private types | `FeatureArcState<A>`, `FeatureArcStartState`, `FeatureArcConfigEntry`, `FeatureArcChain`, `FeatureArcEnabled<A>`, `FeatureArcStep` | map-backed snapshot compiler records | concrete arc/config pool and shared heap | source/API inventory and behavior tests |
| APIs | `start_handle`, `attach_start`, `detach_start`, `attach_start_at`, `add_start_node`, `remove_start_node`, `set_default_end_node`, `start_for_interface_or*`, `next_feature_slot*`, `next_feature_frame` | attached handle and runtime start-node mutation surface | startup arc registration plus direct start/next | compile and packet graph tests |
| API | `FeatureArcControl::publish` and internal global barrier lookup | snapshot publication and hidden synchronization discovery | `InterfaceMain` live mutation uses explicit main context and barrier macro | barrier ownership test |
| types | `IpLocalNode`, `IpReceiveNode`, `IpLocalArc`, `IpLocalNext`, `IpLocalError`, `IpLocalControlPlane`, `IpLocalState`, `IpLocalStateHandle`, `IpLocalRuntime`, `IpLocalSourceCheck` | shared protocol node/state/snapshot/registry model | concrete IPv4/IPv6 nodes and Main fields | workspace compile and local behavior tests |
| state | process-wide IP-local `Mutex<Vec<_>>` and all local `ArcSwap` snapshots | packet-path shared lookup and atomic publication | Main-owned tables and service feature heap | concurrency audit and packet benchmark |
| fields/APIs | `NetworkOpaque::feature_arc_index`, `NetworkOpaque::{feature_config_index,set_feature_config_index}` | redundant arc identity and IP-private feature cursor | start node keeps arc index; generic `Buffer` keeps current config index | opaque layout and graph-chain tests |
| APIs | NodeId-typed `Buffer`/pool/runtime `current_config` and `set_current_config` | buffer-wide claim that the generic cursor is always a node identity | generic `u32` accessors; handoff performs explicit conversion | workspace compile and handoff test |

Every inventory row is a source-level change. There is no Binary API, database,
file-format, buffer-layout, or cross-process shared-memory schema addition in
this ADR.

## Verification and acceptance

Implementation is accepted only when the following focused checks pass. Tests
must exercise public behavior or compile-time layout and must not search source
text.

1. Arc installation assigns compact `u8` indices, topologically assigns
   feature indices, rejects missing constraints/cycles, and validates equal
   start-node next slots.
2. Enable/disable uses dense per-interface indices, preserves exact occurrence
   counts, shares identical chains, resolves the hidden heap back-pointer to the
   owning pool entry, reuses/coalesces freed heap ranges, and leaves the old
   chain published on every recoverable failure.
3. A packet with no enabled feature keeps its caller-provided next; a packet
   with features visits them in order, observes exact config words, advances
   once per feature, and reaches the selected end node.
4. The packet-path test and release benchmark show no allocation, lock,
   snapshot load, map/name lookup, or shared reference-count operation. Layout
   assertions prove four-byte heap alignment, unchanged buffer-header layout,
   and no Feature Arc state in `NetworkOpaque`.
5. Startup registration/enablement performs no barrier; a no-op live change
   performs no barrier; an actual live `InterfaceMain` change enters the macro
   once and nested control ingress uses recursion. A multi-edge graph update is
   failure-atomic and causes exactly one worker Graph Refork at outer release.
   Read-only and packet-path methods never synchronize.
6. IPv4 and IPv6 local tables initialize to their own punt slots. Registering
   or unregistering one protocol in one table cannot change the other table;
   no local-protocol classification enum or second 256-entry classifier exists.
7. Separate sibling tests prove local/receive/end-of-arc next-slot sharing
   inside one protocol and no sharing across protocols.
8. IPv4 tests cover fragment and translated-packet bypass, TCP/UDP checksum,
   UDP length, concrete FIB/override selection, receive actual interface,
   spoofed-local rejection, uRPF miss, and broadcast exemption.
9. IPv6 tests cover extension-header traversal, UDP/ICMP checksum and length,
   concrete FIB/override selection, receive actual interface, uRPF miss, and
   ICMPv6/link-local exemptions.
10. Local validation errors go directly to the concrete drop next; an
    unregistered protocol punts; enabled features run before final protocol
    dispatch; end-of-arc never repeats head validation.
11. A real IP/ICMP DSO test proves ICMP loads after IP and registers distinct
    ICMPv4/ICMPv6 nodes without an IP-to-ICMP node-name dependency.
12. Macro expansion tests prove `#[feature_arc]` and `#[feature]` resolve their
    Graph Node type-path arguments and register the real node structs. Workspace
    compile checks prove removal of marker traits, handles, control plane,
    snapshots, shared local nodes, and compatibility re-exports.

These tests are grouped at the highest owning boundary: Feature Arc ordering,
heap, interface association, and publication in `hammer-service`; local packet
semantics in the IP plugin; macro expansion in `hammer-component-macros`; and
cross-DSO registration in one integration test. Constructor-only and duplicate
per-protocol tests are excluded.

## Decision closure

All architecture choices in this ADR are closed:

- Feature Arc is private implementation of `InterfaceMain` and is keyed by
  `sw_if_index`; `NetMain` has no Feature Arc accessor.
- `#[feature_arc]` and `#[feature]` accept real Graph Node type paths and
  generate direct registration methods on those structs; no registration
  image, marker trait, or metadata-only type is introduced.
- Declarations close before workers; runtime loading cannot reorder arcs.
- Runtime feature mutation is an `InterfaceMain` operation and does not depend
  on Binary API. The owner uses the barrier macro for an actual live change.
- Per-interface configuration is a dense config index into one shared aligned
  heap with a VPP-shaped hidden pool back-pointer. Packet progress uses
  `Buffer::current_config_index`; no arc identity or Feature Arc cursor is
  carried in `NetworkOpaque`, and `InterfaceMain` stores no `NodeRuntime`.
- Multi-edge Feature topology updates use one failure-atomic generic
  `NodeRuntime` operation and request one ADR-0003 worker Graph Refork only when
  the main graph actually changes.
- IPv4 and IPv6 local nodes, validation, errors, next tables, and arc indices
  are concrete and independent; no local-protocol classifier enum or table is
  introduced.
- ICMP is an independent plugin that registers with IP; DPO does not
  participate in local wire-protocol dispatch.

Implementation may optimize control-plane allocation only if these ownership,
wire, graph, failure-atomicity, and packet-path contracts remain unchanged.

## Evidence

### Current project facts

- `crates/hammer-service/src/feature_arc.rs` defines the current generic
  handle/control/snapshot hierarchy, packet-path maps, and owner-internal
  `publish` barrier lookup.
- `crates/hammer-service/src/opaque.rs` contains both the unused
  `feature_arc_index` field and feature configuration accessors in the IP
  opaque reserve.
- `crates/hammer-core/src/buffer/header.rs` already contains the generic
  `current_config_or_punt: u32` field, but its current accessors reinterpret it
  as `NodeId`; handoff is the only current consumer of that interpretation.
- `crates/hammer-runtime/src/node.rs` keeps `NodeRuntime` in
  `Rc<RefCell<_>>`, so it cannot be stored in shared `InterfaceMain` state. Its
  current one-edge mutation validates before insertion but does not provide an
  all-edges atomic operation or request ADR-0003 Graph Refork itself.
- `crates/hammer-plugins/net/ip/src/local.rs` defines one shared local node,
  receive node, arc, next enum, state snapshot, source-check mode, and
  process-wide runtime mutex for both wire protocols.
- Issue #291 requires protocol-neutral service ownership, concrete IPv4/IPv6
  implementations, independent ICMP lifecycle, and explicit protocol
  registration. It does not require a generic Feature Arc Binary API.

### Vendored VPP facts

- `third_party/vpp/src/vnet/feature/feature.h` defines arc/feature
  registrations, compact arc indices, per-arc config mains, dense
  `config_index_by_sw_if_index`, interface feature bitmap/count, and the shared
  feature configuration heap.
- `third_party/vpp/src/vnet/feature/registration.c` topologically orders
  features, assigns feature indices, initializes start/end nodes, and requires
  equal local next indices across an arc's start nodes.
- The vendored tree has no `VNET_FEATURE_ARC_ORDER` use outside the macro
  definition. Its actual feature declarations use per-feature
  `runs_before`/`runs_after`; Hammer therefore adds no unused bulk-registration
  surface.
- `third_party/vpp/src/vnet/feature/feature.c` assigns arc indices and performs
  per-interface enable/disable by replacing a config index and updating count
  and bitmap state.
- `third_party/vpp/src/vnet/config.h` and `config.c` define the sequential
  config-data/next-slot layout, config sharing/reference counts, heap indices,
  and end-node replacement.
- `third_party/vpp/src/vnet/feature/feature_api.c`, IP interface lifecycle,
  interface statistics, ARP, reassembly, SPAN, and TCP SYN filter code all call
  the same `vnet_feature_enable_disable`; Binary API is one caller, not the
  Feature Arc ownership or registration mechanism.
- VPP's feature update observer list exists to refresh adjacency/device/tunnel
  caches. Current Hammer has no equivalent Feature Arc cache, and repository
  ownership rules prohibit storing plugin-owned callbacks in
  `hammer-service`; the observer registry is therefore not copied.
- `third_party/vpp/src/vnet/ip/lookup.h` and `lookup.c` define the two
  256-entry local protocol tables, default punt behavior, and protocol-specific
  ICMP facts. Hammer keeps the concrete IPv4/IPv6 validation branches and adds
  no protocol-classification enum or mapping table.
- `third_party/vpp/src/vnet/ip/ip4_forward.c` defines the `ip4-local` arc,
  local/receive/end sibling nodes, fragment/translated bypass, transport
  validation, source lookup, spoofed-local/uRPF/broadcast rules, final protocol
  dispatch, and register/unregister behavior.
- `third_party/vpp/src/vnet/ip/ip6_forward.c` defines the independent
  `ip6-local` arc and siblings, extension-aware validation, UDP/ICMP checksum
  classification, ICMPv6/link-local source exemptions, final dispatch, and
  register/unregister behavior.
- `third_party/vpp/src/vnet/dpo/receive_dpo.c` binds receive objects to the
  concrete receive nodes and supplies the optional actual interface used by
  local processing.
