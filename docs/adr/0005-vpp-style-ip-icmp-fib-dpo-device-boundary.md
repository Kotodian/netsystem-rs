# VPP-Style IP, ICMP, FIB, DPO, and Device Data-Path Ownership

Status: accepted

Date: 2026-09-02

This ADR defines the breaking source-level migration for the IP forwarding
stack. It is a design record, not an implementation claim. The implementation
issue derived from it owns the Rust migration and focused behavior tests. This
ADR change contains no Rust implementation or test code.

## Context

The current checkout has one `hammer-plugins/ip` DSO that registers IP and
ICMP graph nodes together (`crates/hammer-plugins/ip/src/lib.rs`). Its
`forwarding` module also contains FIB source merging, IPv4/IPv6 lookup storage,
DPO values, load-balance state, adjacency rewrite state, and publication
handles. IP local protocol registration is currently backed by `thread_local!`
state in `ip/local.rs`.

The current IP route list is read from `[network].route` during plugin
initialization (`crates/hammer-plugins/ip/src/config.rs`). That is a startup
configuration path, not a VPP-style runtime control-plane API. The repository
already has the required Binary API transport and method registration: the
Binary API Process Node resolves a plugin method and, for a non-`mp_safe`
method, enters `worker_thread_barrier_sync!` before invoking it.

The service layer already owns network/interface/device coordination, but its
`DeviceInputNext` still contains fixed `ip-input` nexts. The actual data-plane
contract is already visible in the code: device input records RX
`sw_if_index`, IP input consumes it, and adjacency rewrite writes TX
`sw_if_index` into `NetworkOpaque`.

`NetMain::init` creates `local0` through the generic hardware-interface path.
The current path does not by itself enforce the VPP rule that `local0` is a
reserved sentinel and cannot receive ordinary IP addressing.

Vendored VPP provides the semantic reference:

- `vnet_main_t` embeds `vnet_interface_main_t`;
- `ip4_register_protocol` and `ip6_register_protocol` are separate local
  protocol registration faces;
- FIB source precedence and DPO classes are distinct from protocol local-next
  tables;
- `vnet_hw_if_set_input_node` installs a device input node, while
  `vnet_hw_interface_rx_redirect_to_node` delegates per-interface redirection
  to the selected device class;
- receive DPO uses an actual software-interface index when one exists and
  `~0` when no concrete interface is part of the route;
- `sw_if_index` and `fib_index` are separate index spaces.

## Decision

### Ownership and dependency direction

The dependency direction is:

```text
hammer-plugins/icmp -> hammer-plugins/ip -> hammer-service::net
hammer-plugins/ip -----------------------> hammer-service::net
hammer-plugins/transport/tcp,udp ---------> hammer-plugins/ip
hammer-plugins/device/* ------------------> hammer-service
hammer-service ----------------------------> hammer-runtime
```

`hammer-service` never depends on the IP or ICMP plugin. The service-owned DPO
class contract contains only graph metadata and the two lifecycle operations
needed to retain an arbitrary class instance. It never carries plugin object
bytes, pool references, or plugin-specific types into `hammer-runtime` or
another plugin.

| Owner | Owns | Does not own |
| --- | --- | --- |
| `hammer-service::net` | `NetMain`, interface/device coordination, protocol-neutral FIB source/entry/path graph, the DPO type/class registry, class/node/edge metadata, class-indexed lock/release dispatch, canonical `DpoId`/`DpoRef` contracts, service-owned lookup/receive/interface DPO pools, source precedence, back-walk, and the generic forwarding-update seam | plugin-owned DPO object bytes/pools and non-lifecycle operations, ICMP parsing, device queue polling, Binary API method ownership |
| `hammer-plugins/ip` | `IP_MAIN` protocol/port registry, concrete `IP4_MAIN`/`IP6_MAIN` owners for packet input/local lookup, FIB backends and per-interface table mappings, adjacency and load-balance DPO pools, and IP-owned Binary API handlers | ICMP nodes/Main, device queues, worker barrier, generic DPO class registry/stack implementation |
| `hammer-plugins/icmp` | `ICMP_MAIN`, ICMP graph nodes, ICMP type tables, ICMP packet parsing/generation, ICMP registration with IP | IP FIB tables, interface queues, device polling |
| `hammer-plugins/transport/tcp` / `udp` | transport nodes and connections; registration with IP local-next/port tables; typed IP PMTU reads where enabled | IP FIB/DPO pools, ICMP parsing, interface/device queues |
| Device plugin | its RX node, buffer acquisition/normalization, device headers, device-specific next-edge installation, TX implementation | IP protocol parsing and FIB lookup |
| `hammer-service::binary_api` | Binary API framing, method lookup, reply mapping, and the single dispatcher-owned barrier scope | FIB/IP/device domain state and plugin method logic |
| `GlobalMain` | plugin lifetime/loading, graph installation/refork, main-thread scheduling, the single `WorkerBarrier` | packet processing and plugin-owned protocol state |

IPv4 and IPv6 remain two concrete implementations inside one IP plugin. The
service layer does not expose an IPv4/IPv6 family trait, enum, or registration
face.

### Complete VPP ownership correspondence

The split is aligned to VPP ownership, not to the current Hammer module names:

| VPP authority | Responsibility | Hammer authority |
| --- | --- | --- |
| `vlib_global_main_t` | process-wide worker lifecycle, graph publication, plugin lifetime | `GlobalMain` |
| `vnet_main_t` | network-wide authority embedding interface main | `NetMain` embedding `InterfaceMain` |
| `vnet_interface_main_t` | hardware/software interface pools, RX/TX queues, interface callbacks and output-node relationships | `InterfaceMain` |
| `vnet_device_main_t` | device-input worker scheduling and aggregate RX accounting | `DeviceMain` |
| `ip_main_t` | IP protocol/port registration and IP control state | `IpMain` |
| `ip_lookup_main_t` | per-protocol local-next table, interface-address records, feature-arc indices | IP plugin local/lookup owner |
| `ip4_main_t` / `ip6_main_t` | concrete unicast FIB pools, MFIB pools, and per-protocol `fib_index_by_sw_if_index` / `mfib_index_by_sw_if_index` mappings | IP plugin's IPv4 and IPv6 owners; this ADR moves only the unicast FIB graph/backend contract |
| `fib_main_t` and `fib_table_t` | source registry, entry/path-list graph, table-id/index mapping, back-walk and forwarding projection | `hammer-service::net::fib`; the concrete IP backend supplies forwarding projection and owns its DPO objects |
| DPO registry (`dpo_vfts`, `dpo_nodes`, `dpo_edges`) | class keys, per-protocol node bindings, stack edges, compact identity metadata, and cross-class reference acquisition/release | `hammer-service::net::dpo`; the service record holds only lifecycle callbacks and graph metadata, while every concrete pool remains owner-local |
| concrete DPO pools | adjacency, load-balance, lookup, receive, interface RX, and other object bytes | owning service or protocol plugin pool |

The packet path follows the same separation:

```text
RX queue scheduling
  -> device-owned input node
  -> RX sw_if_index in NetworkOpaque
  -> IP input selects per-protocol fib_index_by_sw_if_index entry
  -> IP local-next dispatch or IP lookup DPO
  -> concrete unicast FIB LPM
  -> per-entry load-balance object and post-LPM bucket selection
  -> child DpoId.next / adjacency rewrite / receive / drop / punt node
  -> TX sw_if_index or interface-TX DPO
  -> InterfaceMain output-node/queue selection
  -> device-owned TX implementation
```

The IP input node never indexes a FIB directly with `sw_if_index`; it performs
the explicit per-protocol mapping first. A configured TX interface may
override the selected FIB only through the packet's TX interface fact, matching
VPP's `ip_lookup_set_buffer_fib_index` ordering.

### Main globals and lifecycle

Each plugin Main is an owner-local process global stored by value:

```rust
pub static IP_MAIN: OnceLock<IpMain> = OnceLock::new();
pub static IP4_MAIN: OnceLock<Ip4Main> = OnceLock::new();
pub static IP6_MAIN: OnceLock<Ip6Main> = OnceLock::new();
pub static ICMP_MAIN: OnceLock<IcmpMain> = OnceLock::new();
```

The concrete owners expose `init(...)` and `global()`. Initialization fully
validates and constructs the value before calling `OnceLock::set`. There is no
`OnceLock<Arc<Main>>`, `ArcSwapOption<Main>`, runtime type registry lookup, or
compatibility facade for these Main values.

`IpMain` owns only the generic IP protocol and TCP/UDP port registries, matching
VPP's `ip_main_t`; it does not own FIB pools, DPO object pools, or graph nodes.
`Ip4Main` and `Ip6Main` each own their concrete `lookup_main`, unicast and
MFIB table authorities, table-id maps, `fib_index_by_sw_if_index` and
`mfib_index_by_sw_if_index` mappings, interface-address/table-bind callbacks,
flow-hash configuration, and host configuration. This ADR defines only the
unicast `FibTable<P, B>` path; MFIB remains a separate IP-owned implementation.
The IP plugin's forwarding implementation owns the shared
adjacency pool and the concrete load-balance objects used by those two tables;
that implementation is not a service type and is not duplicated per address
implementation. Their graph nodes are registered by the graph image and their
init functions only initialize these owner values.

The startup dependencies are:

```text
net_main_init (constructs embedded InterfaceMain and consumes interface/net images)
    -> device_main_init
net_main_init -> ip_main_init -> ip4_main_init / ip6_main_init
ip_main_init -> icmp_init (ICMP DSO declares load_after = ["ip"])
all declarations -> install_packet_graph -> Binary API route publication
```

This is a dependency graph, not a claim that device drivers or IP are one
linear plugin. Device class images and IP class images may be consumed in
either order after `NetMain` exists. Both IP and ICMP DSOs expose their own
graph nodes. `ip4_main_init` and `ip6_main_init` create their local-next tables
before the ICMP graph-node initialization registers ICMP nodes into those
tables. Node registration, not a pre-graph init callback, is the point at
which a concrete `NodeId` is available.

### Service-owned network registration image

`NetRegistrationImage` is a service-owned static declaration image with two
network registration faces: FIB sources and DPO classes. It crosses a plugin
DSO seam, so its representation is an `abi_stable` contract, not merely a
C-layout hint:

```rust
#[repr(C)]
#[derive(StableAbi)]
pub struct NetRegistrationImage {
    pub fib_sources: RSlice<'static, FibSourceRegistration>,
    pub dpo_classes: RSlice<'static, DpoClassRegistration>,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct FibSourceRegistration {
    pub name: RStr<'static>,
    pub priority_class: u8,
    pub behavior: FibSourceBehavior,
}

#[repr(u8)]
#[derive(StableAbi)]
pub enum FibSourceBehavior {
    Drop,
    Api,
    Simple,
    RecursiveResolution,
    Mpls,
    Interface,
    Interpose,
    Lisp,
    Adjacency,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct DpoClassRegistration {
    pub name: RStr<'static>,
    pub nodes: RSlice<'static, DpoNodeRegistration>,
    lock: extern "C" fn(u32) -> DpoLockStatus,
    release: extern "C" fn(u32),
}

#[repr(u8)]
#[derive(StableAbi)]
pub enum DpoLockStatus {
    Acquired,
    ObjectMissing,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct DpoNodeRegistration {
    pub proto: DpoProto,
    pub node_name: RStr<'static>,
}
```

`FibSourceRegistration`, `FibSourceBehavior`, `DpoProto`, `DpoLockStatus`, and
every other type nested in this image also derive `StableAbi` and contain only
ABI-stable fields. The two lifecycle callbacks use the C ABI and accept only a
pool index; they obtain their concrete owner through that owner's process-global
Main. Native Rust slices, references to unsized values, Rust `str`, plugin object
pointers, and Rust ABI callbacks are not fields of this image. `#[repr(C)]`
only fixes field order and alignment; it does not make an otherwise unstable
field safe across a DSO boundary.

`DpoClassRegistration::name` names one DPO class key, not a Rust object type
and not a graph node. `nodes` is the complete per-data-path list passed to the
class registration. A class with no fixed node list (for example an
instance-dependent interface TX DPO) has an empty list and supplies its next
node from its concrete owner operation. `lock` validates and acquires one
reference to the class-local `index`; `release` is an infallible internal
counterpart invoked only by `DpoRef::drop`. Neither callback exposes the pool or
the concrete object to service code.

It is separate from `InterfaceRegistrationImage`. The two images are not
merged into an enum, a generic registration hook, or the runtime lifecycle
`RegistrationImage`. The existing runtime image remains the only registration
carrier owned by `hammer-runtime`; the net image is consumed by `NetMain` from
the fixed symbol exported by a plugin that explicitly contributes net classes.

The declarations are generated by concrete proc macros owned by the service
component API:

```rust
#[derive(FibSource)]
#[fib_source(name = "interface", priority = 0x03, behavior = Interface)]
pub struct InterfaceFibSource;

#[derive(DpoClass)]
#[dpo_class(
    name = "adjacency",
    nodes = [(IP4, "ip4-rewrite"), (IP6, "ip6-rewrite")]
)]
pub struct AdjacencyDpo {
    // concrete IP adjacency fields are owned by this plugin type
}
```

Like the existing `DeviceClass` and `HwClass` derives, these derives generate
only declaration metadata and a `registration(lock, release)` accessor. The
concrete owner supplies its private C-ABI functions to that accessor when
placing the registration in the static image. The derive does not generate
pool access, reference-count logic, class-slot storage, or object state. The
generated arrays are adapted to `RSlice` at the static image.

One concrete object type may contribute several class registrations.
`LookupDpo` is the example: its single pool/object layout is paired with the
source, destination, multicast, and interface-table lookup class keys, each
with its own per-protocol node list. The owner lists those declarations in its
`NetRegistrationImage`; the derive does not infer the relationship between
class keys and an object pool. The class key count is not the object type
count, and a graph node name is never used as an object identity.

```rust
pub static IP_ADJACENCY_CLASS: DpoClassRegistration =
    AdjacencyDpo::registration(adjacency_lock, adjacency_release);
```

The generated image contains no `AdjacencyDpo` object, pool, or reference to
either. `NetMain` consumes the image and assigns a `DpoType` class slot. That
slot and the two lifecycle callbacks are recorded with the service-side
class/node record; IP initialization stores the assigned slot beside its
`Pool<AdjacencyDpo>` and remaining concrete operations. A `DpoId` then carries
only `{ type, proto, index, next }` and is the compact identity, not the object
itself.

`AdjacencyDpo` is declared in `hammer-plugins/ip` because its fields, pool,
node behavior, and class operations are IP-owned. The service sees only the
static name/protocol declaration, its lifecycle callbacks, and the assigned
class slot. It never stores the `AdjacencyDpo` type, its pool, or the concrete
object state. The callbacks can only acquire or release an index through the
IP owner; service cannot resolve or mutate the object.

The class-slot handoff is explicit. `net_main_init` installs every image item,
reserving the VPP built-in class slots first and allocating plugin slots from a
monotonic dynamic range. It keeps the resulting `DpoType`, lifecycle callbacks,
and per-protocol node slots in `DpoClassRecord`. The derived graph
edges live separately in `DpoStackRegistry`; they are keyed by child/parent
class and data-path protocol, exactly like VPP's `dpo_edges`, and are not part
of the class record. During `ip_init`, explicit owner code asks `NetMain` for
the slot of each static `DpoClassRegistration` and records the returned class
slot next to the IP pool and its concrete static operations. This is a lookup
of a startup declaration, not a runtime object registry or an erased object
capability. A class cannot allocate or publish an object until its slot has
been bound. A slot is never reused during the process lifetime of the owning
DSO.

The generic DPO layer and the concrete class binding are therefore separate:

```text
hammer-service::net::dpo
  DpoProto, DpoType, DpoId, DpoRef, DpoMain, DpoStackRegistry
  DpoClassRegistration { name, nodes, lock, release }
  DpoClassRecord { DpoType, node slots, lock, release }
  DpoStackRegistry { derived child/parent class-proto edges }
  DropDpo/PuntDpo singletons; LookupDpo/ReceiveDpo pools

hammer-plugins/ip
  AdjacencyDpo and LoadBalanceDpo   concrete IP-owned DPO objects/pools/ops
  ip4-rewrite / ip6-rewrite        concrete graph-node implementations
```

`dpo_type_t` is a class key, not the object bytes. In VPP the key selects the
type registration record (`dpo_vfts[type]` and `dpo_nodes[type][proto]`) and
the concrete pool from which `dpoi_index` is decoded. Hammer keeps that same
relationship: `DpoClassRecord` is the service-side class/node/lifecycle
registration, while the owner stores the returned class slot together with its
concrete pool and non-lifecycle operations. A DPO node checks the class slot in
`DpoId` before it indexes the owner's pool. There is no erased object pointer or
service-owned object operation table.

This is the minimum Rust equivalent of the part of VPP's `dpo_vft_t` required
for arbitrary cross-class ownership. The service dispatches only lock/release;
formatting, memory accounting, MTU/uRPF, interpose, and object mutation remain
owner-local static functions. The service registry derives graph node slots and
stack edges, but only the concrete owner accesses object bytes. Runtime route
entries, lookup/receive objects, adjacency records, load-balance buckets, and
other concrete DPO instances are never stored in the static ABI image.

### DPO class and object inventory

The migration follows VPP's distinction between a DPO type graph and a DPO
instance graph. The first graph is keyed by `(DpoType, DpoProto)` and contains
node/edge selection. The second graph is made of `DpoId` values whose
`(type, index)` pair identifies one concrete object. The following inventory is
the ownership map for the classes relevant to this design; classes marked
future retain the same seam when their owning plugin is added.

`DpoType` reserves the invalid `DPO_FIRST` value and keeps the VPP built-in
class order (`DROP`, `IP_NULL`, `PUNT`, `LOAD_BALANCE`, `REPLICATE`, the
adjacency subtypes, `RECEIVE`, `LOOKUP`, and the remaining built-ins) stable.
Plugin classes are allocated from the dynamic range after `DPO_LAST`; a
process-lifetime slot is never recycled. `DpoProto` keeps VPP's packed
data-path protocol values and adds an explicit `NONE` value for an
uninitialised ID. The target `DpoId` is one non-generic `#[repr(C)]` value with
the VPP widths and order: `DpoType(u8)`, `DpoProto(u8)`, `next(u16)`, and
`index(u32)`. Its size is an implementation invariant (`size_of::<DpoId>() ==
8`). The invalid value is `(DPO_FIRST, DPO_PROTO_NONE, 0, u32::MAX)`.

`DpoId` is a scalar identity value whose four fields are `Copy`, so the type
derives `Copy` and `Clone`. This is intentionally shallow: a value copy
duplicates `{ type, proto, index, next }` without resolving an object or
changing its lifetime. No copy of a `DpoId` is itself an owning reference.

`DpoRef` is the service-owned, non-`Copy`, non-`Clone` representation of one
owning reference. `DpoMain::lock(DpoId)` validates the class/protocol binding,
dispatches the registered class `lock(index)`, and returns `DpoRef` only after
the concrete owner has acquired the reference. `DpoRef::id()` returns the
compact identity for publication. Its private release callback is invoked by
`Drop`; Hammer exposes no manual `unlock` operation. Moving a `DpoRef` transfers
exactly one reference and copying its `DpoId` changes no ownership.

VPP's `dpo_lock` is a reference-count acquisition, not a thread or pool mutex.
`dpo_id_t` is a copyable scalar, so an atomic identity replacement can publish
a new value only if the new class object is retained before the old identity's
reference is discarded. `load_balance_t`, `lookup_dpo_t`, and `receive_dpo_t`
increment their object count on this acquisition and reclaim the pool value
only when no owning slot remains. Hammer keeps that lifetime rule through the
class-indexed lifecycle dispatch: every owning FIB, bucket, path, or parent slot
stores one `DpoRef` before its `DpoId` becomes reachable. When the slot is
replaced or removed after the barrier, dropping that `DpoRef` invokes the
matching owner release and any zero-count pool destruction.

The lock is therefore a required ownership transition, not optional metadata.
The reason is that one `DpoId` identity can be copied into several independently
published locations: a FIB entry, a load-balance bucket, a path or parent DPO,
and the worker-visible forwarding projection. Without one acquisition for each
owning location, the concrete pool object could reach zero references, be
destroyed, and have its index reused while another copied identity is still
reachable. VPP's lock exists precisely to prevent that pool/index use-after-
reclaim during replacement and back-walk updates.

The owner callback validates the class-local pool index and increments the
concrete object's reference/dependency count. Service never receives the
object, its address, or its pool. The returned `DpoRef` binds that acquisition
to the destination slot. Moving it out after a barrier-safe replacement invokes
Rust `Drop`, which calls the same class owner's release callback and performs
any owner-local child cleanup. `DpoId` itself never gains `Drop` and never
performs this accounting when copied.

`DpoMain::stack(child_type, child_proto, parent)` is the owning form of VPP's
`dpo_stack`: it validates the parent, derives the child-to-parent graph edge,
writes that edge into a copied parent identity, acquires the parent through the
same class lifecycle dispatch, and returns a `DpoRef`. It never returns an
unretained stacked identity. Direct publication uses `DpoMain::lock`; stacked
publication uses `DpoMain::stack`.

The VPP classes fall into two lock categories:

| Class | Why VPP acquires the lock | Hammer form |
| --- | --- | --- |
| Adjacency (including incomplete/midchain/glean/mcast subtypes) | Increment the adjacency/FIB-node reference so the shared adjacency pool entry and its back-walk dependencies cannot be reclaimed while a DPO identity is published | Registered IP-owner `lock` acquires the shared adjacency index; `DpoRef::drop` dispatches its release only after replacement/removal |
| Load-balance | Increment `lb_locks`; when the count reaches zero VPP destroys the object and releases every child bucket DPO | Registered IP-owner `lock` acquires the object; its zero-count release drops the `DpoRef` held for each child bucket |
| Lookup | Increment `lkd_locks`; a configured-table lookup also keeps its referenced FIB/MFIB table source alive | Registered service-owner `lock` acquires the lookup object and its table dependency; release ends both dependencies at zero count |
| Receive | Increment `rd_locks` so the receive pool entry remains valid for every published identity | Registered service-owner `lock` acquires the receive object; release removes the pool value only after the final `DpoRef` drops |
| Interface-RX and other pool-backed classes | Keep the class-specific pool object and its child/interface dependency alive | The owning class supplies the same lifecycle callbacks and each destination stores a `DpoRef` |
| Drop, Punt, IP-null, interface-TX, and permanent link-local DPOs | The object is a process-lifetime singleton or a stateless wrapper, so VPP's lock operation is intentionally a no-op | The registered callbacks validate the class/index and otherwise perform no lifetime work; `DpoRef` keeps one uniform owning interface |

For lookup DPOs whose table is selected from configuration, VPP also holds the
referenced table source while the lookup object is live. Hammer represents that
dependency through owner-held Rust ownership of the concrete lookup value and
its table relationship; no separate table-lock API is added.

| VPP class | Object storage | Hammer owner in this design | `DpoId.index` |
| --- | --- | --- | --- |
| `DPO_DROP` | Per-protocol static singleton IDs; no object lifetime | `hammer-service::net` | Fixed selector; never dereferenced as a pool index |
| `DPO_PUNT` | Per-protocol static singleton IDs; no object lifetime | `hammer-service::net` | Fixed selector; never dereferenced as a pool index |
| `DPO_IP_NULL` | Constant records for protocol/action combinations | `hammer-plugins/ip` | Index into the IP plugin's constant action table |
| `DPO_LOAD_BALANCE` | `load_balance_t` pool; buckets contain child `dpo_id_t` values | `hammer-plugins/ip` for the IP FIB projection | Index into the load-balance pool |
| `DPO_REPLICATE` | `replicate_t` pool; buckets contain child `dpo_id_t` values | Future multicast/replication plugin | Index into the replicate pool |
| `DPO_ADJACENCY` | Shared adjacency pool; class-specific subtype registrations | `hammer-plugins/ip` | Index into the adjacency pool |
| `DPO_ADJACENCY_INCOMPLETE` | Same adjacency pool, ARP/ND subtype | `hammer-plugins/ip` | Index into the adjacency pool |
| `DPO_ADJACENCY_MIDCHAIN` | Same adjacency pool, midchain subtype | `hammer-plugins/ip` | Index into the adjacency pool |
| `DPO_ADJACENCY_GLEAN` | Same adjacency pool, glean subtype | `hammer-plugins/ip` | Index into the adjacency pool |
| `DPO_ADJACENCY_MCAST` / `DPO_ADJACENCY_MCAST_MIDCHAIN` | Same adjacency pool, multicast subtypes | `hammer-plugins/ip` | Index into the adjacency pool |
| `DPO_RECEIVE` | `receive_dpo_t` pool | `hammer-service::net` | Index into the receive-DPO pool |
| `DPO_LOOKUP` plus lookup subtypes | One `lookup_dpo_t` pool; source/destination/multicast/interface-table subtypes share the object layout but receive separate class keys and fast-path node lists | `hammer-service::net` | Index into the shared lookup-DPO pool; `DpoId.type` selects the subtype node class |
| `DPO_CLASSIFY` | `classify_dpo_t` pool | Future classifier plugin | Index into the classify pool |
| `DPO_INTERFACE_RX` | Per-interface/protocol DPO pool and database | `hammer-service::net::interface` | Index into the interface-RX pool |
| `DPO_INTERFACE_TX` | No pool; wraps an existing `sw_if_index` | `hammer-service::net::interface` | The wrapped `sw_if_index` |
| `DPO_DVR`, `DPO_L3_PROXY` | Concrete pools | Owning bridge/IP plugin when introduced | Index into the owning pool |
| `DPO_LISP_CP` | Concrete control-plane DPO object | LISP plugin when introduced | Index into the LISP pool |
| `DPO_MFIB_ENTRY` | Concrete multicast-FIB entry object | multicast IP owner when introduced | Index into the MFIB-entry pool |
| `DPO_MPLS_DISPOSITION_PIPE` / `DPO_MPLS_DISPOSITION_UNIFORM` | Concrete MPLS disposition objects | MPLS plugin when introduced | Index into the MPLS pool |
| `DPO_BIER_TABLE`, `DPO_BIER_FMASK`, `DPO_BIER_IMP`, `DPO_BIER_DISP_TABLE`, `DPO_BIER_DISP_ENTRY` | Concrete BIER tables, masks, and entries | BIER plugin when introduced | Index into the owning BIER pool |
| `DPO_IP6_LL` | One permanent link-local singleton DPO ID | IPv6 owner when introduced | Fixed singleton selector; never dereferenced as a pool index |
| `DPO_PW_CW` | Concrete pseudowire control-word object | pseudowire plugin when introduced | Index into the pseudowire pool |
| `DPO_FIRST` | Invalid/uninitialised sentinel; no object | service DPO core | `u32::MAX` invalid index |

VPP's Path MTU DPO is normally a dynamically registered class rather than one
of the built-in `dpo_type_t` values. The IP plugin registers that class after
the service built-ins, stores `IpPmtuDpo` objects in an IP-owned pool, and
stacks each object on its parent DPO. Its class operations provide MTU, uRPF,
interpose and owner-local lifetime behavior. The FIB-linked `IpPmtu` tracker
owns the configured, parent and operational MTU values and updates attached
adjacency rewrite limits; it is not a `PathMtuCache` in service net.

Adjacency subtype selection is part of object construction, not a generic enum
conversion. In VPP `dpo_set(DPO_ADJACENCY, ...)` reads the adjacency object's
`lookup_next_index` and changes the class key to incomplete, midchain, glean,
or multicast variants. Hammer's adjacency owner performs the same typed
classification before it publishes the `(DpoType, index)` pair. Therefore a
midchain is not a field on a generic adjacency ID and is not a service-owned
`DpoKind` variant.

Lookup subtype selection follows the same rule but does not create a second
Rust object type. VPP allocates one `lookup_dpo_t`, chooses a subtype class key
from `(input, table source, cast)`, and publishes that key with the pool index.
Hammer's `LookupDpo` owner keeps one pool and several class registrations; the
selected subtype key determines the lookup node while the index still decodes
the same `LookupDpo` object.

The lookup object retains the four VPP control facts: address input
(`src-address` or `dst-address`), table selection (configured table or the
input interface's table), cast (unicast or multicast), and the selected table
identity. A source lookup, destination lookup, destination-from-interface
lookup, and destination-multicast lookup are distinct class keys over that
same object layout. The lookup node reads the object, selects the table from
the packet's RX interface when requested, performs LPM, then follows the
resulting DPO; it does not reinterpret a table id as a DPO index.

Load-balance registration also follows VPP's sibling-node contract. The
`load_balance_t` class registers the per-protocol load-balance node; the
protocol lookup node is installed as its graph sibling and is not represented
as a second load-balance object class. `DpoClassRegistration` therefore records
the class's originating node set; the existing graph-node registration owns any
`sibling_of` relationship. It does not fabricate an object per graph node.

`dpo_register()` installs one class key with its per-protocol node list.
`dpo_register_new_type()` allocates a new key for a plugin class or for a
fast-path subtype that shares an existing object pool. The proc macro emits
only the stable declaration. `DpoClassRecord` stores only the service-side
key/node/lifecycle data. Explicit owner initialization retains the
class-key-to-pool binding and all object access needed by its operations.

The DPO value operations use Rust value semantics. `DpoId` derives `Copy` and
`Clone`; assigning or passing it by value copies the compact
`{ type, proto, index, next }` identity and does not resolve an index, touch a
pool, or invoke a class operation. The service-owned `DpoMain` validates
class/protocol/node metadata, derives stack edges, and dispatches only the
registered lifecycle operations. A pool-backed callback resolves the index in
its own owner. An invalid class/protocol/index combination is a control-plane
error, not an unchecked cast.

Cross-owner retention uses one service-owned interface. For example, an
IP-owned load-balance bucket can retain a service-owned lookup or receive DPO
without importing either concrete type: it passes the child's `DpoId` to
`DpoMain::lock` and stores the returned `DpoRef` beside the packet-visible
bucket identity. A stacked child uses `DpoMain::stack`, which returns the same
owning value after installing the derived edge. The matching release is private
to `DpoRef::drop`; no caller can manually unlock, and service never stores the
concrete object.

This is the accepted Rust mapping of VPP's `dpo_vfts[type]` lifecycle dispatch.
Compile-time knowledge of every foreign class was rejected because it couples
each DPO owner to all potential parents. Process-lifetime, never-reclaimed pool
objects were rejected because they discard VPP's reference-counted reclamation.

`dpo_get_mtu`, `dpo_get_urpf`, `dpo_mk_interpose`, and `dpo_is_adj` remain
semantic DPO operations. The owner of the concrete class implements them as
typed static functions; interpose creates an owner-class object stacked on the
parent DPO, and adjacency classification is based on the registered adjacency
class slots. They are not fields on `DpoId`, are not service dispatch methods,
and are not implemented by a generic protocol enum.

`dpo_get_next_node()` may be class-default or instance-dependent. The default
path resolves `dpo_nodes[type][proto]`; `DPO_INTERFACE_TX` is the important
instance-dependent case because its next node comes from the wrapped
`sw_if_index`. `DpoStackRegistry` therefore caches only type/protocol edges;
it never caches an object pointer or an instance-specific node result. A stack
operation receives both child `(DpoType, DpoProto)` and parent `(DpoType,
DpoProto)`, obtains the parent node slot, adds the graph edge, and writes that
edge into the child `DpoId.next`. The parent object identity is retained
by the returned `DpoRef`, separately from the graph edge.

Pool growth/removal follows the VPP `dpo_pool_barrier_sync` rule. A pool
operation that can expand or recycle worker-visible storage is performed only
inside the already-held Binary API dispatcher barrier scope (or during startup
before workers run); the concrete owner does not enter a second barrier. The
owner publishes the object/ID while workers are stopped and releases the scope
after the mutation. `DpoId` remains a compact identity value; its
worker-visible replacement is ordered by the existing barrier publication, not
by a second pointer or completion protocol.

The current single `AdjacencyRewriteNode` is split into two concrete IP graph
nodes to match VPP's per-data-path node table: `Ip4RewriteNode` owns IPv4
checksum/TTL and MTU handling, while `Ip6RewriteNode` owns IPv6 header/MTU
handling. Both resolve an IP-plugin-owned adjacency object from the matching
class slot and pool index, then write the service-owned TX `sw_if_index`;
neither changes the DPO class registry.

The split is a graph registration seam, not duplicated packet-processing
implementations. Inside the IP plugin, a private static-dispatch core owns the
shared adjacency read, TX metadata write, MTU decision plumbing, error
classification, trace recording, and next-arc selection. The protocol-specific
policy is selected at compile time:

```rust
trait RewriteProtocol {
    const DPO_PROTO: DpoProto;

    fn mtu_action(packet: &mut Buffer, adjacency: &AdjacencyDpo) -> MtuAction;
    fn apply_rewrite(packet: &mut Buffer, adjacency: &AdjacencyDpo) -> RewriteResult;
}

fn process_rewrite_frame<P: RewriteProtocol>(frame: &mut BufferFrame) -> NodeNext {
    // shared orchestration: read adjacency, classify errors, update TX opaque,
    // record trace, and select the next arc around P's protocol policy
}

struct Ip4Rewrite;
struct Ip6Rewrite;

impl Node for Ip4RewriteNode {
    fn process(&mut self, frame: &mut BufferFrame) -> NodeNext {
        process_rewrite_frame::<Ip4Rewrite>(frame)
    }
}

impl Node for Ip6RewriteNode {
    fn process(&mut self, frame: &mut BufferFrame) -> NodeNext {
        process_rewrite_frame::<Ip6Rewrite>(frame)
    }
}
```

`RewriteProtocol`, `Ip4Rewrite`, and `Ip6Rewrite` are private IP-plugin
implementation details; they are not service interfaces, DPO registrations, or
dynamic dispatch objects. `Ip4RewriteNode` and `Ip6RewriteNode` remain the two
concrete graph adapters because VPP's DPO node table selects a per-protocol
graph target and the wire policies differ. The shared generic core keeps the
behavioral contract in one place without introducing a unified family
abstraction or a runtime protocol branch.

### Concrete DPO ownership

`AdjacencyDpo` and `LoadBalanceDpo` are deliberately not service/net
abstractions. They are concrete DPO classes owned by the IP plugin in this
change. Each class stores the `DpoType` returned by the service registry next
to its concrete object pool and class-specific static operations:

```text
IP plugin adjacency owner:
    class = DpoType("adjacency")
    objects = Pool<AdjacencyDpo>
    ops = IP-plugin adjacency operations

IP4_MAIN / IP6_MAIN load-balance owner:
    class = DpoType("load-balance")
    objects = Pool<LoadBalanceDpo>
    ops = IP-plugin load-balance operations
```

The pool index is meaningful only with the matching `DpoType`. A rewrite or
lookup node must not cast an arbitrary `DpoId.index`; it checks the class slot
and then obtains the concrete object through the owner-local pool API.

- `AdjacencyDpo` owns the IP neighbour/peer facts needed by rewrite: next-hop,
  egress `sw_if_index`, rewrite bytes, link MTU/path MTU, and the IP-specific
  next action. Its pool and rewrite updates remain in `hammer-plugins/ip`.
- `LoadBalanceDpo` owns the IP forwarding bucket storage and hash selection used
  by the two IP lookup backends. Its packet-visible buckets contain service
  `DpoId` values and its control state retains one `DpoRef` per bucket; the
  pool, bucket validation, and selection implementation remain in
  `hammer-plugins/ip`.

The service owner keeps the same object-bearing shape for the generic network
DPO classes that own network state:

- `LookupDpo` stores lookup input/table/cast facts in a service-owned pool;
- `ReceiveDpo` stores the concrete `sw_if_index` and source-address facts in a
  service-owned pool;
- `InterfaceRxDpo` stores the one-per-interface/per-protocol receive object
  in an interface-owned pool; the interface record owns it until removal;
- `InterfaceTxDpo` is the stateless wrapper around a `sw_if_index`; its next
  node is resolved by the interface owner and it has no pool lifetime;
- `DropDpo` and `PuntDpo` are stateless per-protocol singletons, matching VPP's
  `drop_dpos[DPO_PROTO_NUM]` and `punt_dpos[DPO_PROTO_NUM]` arrays.

This follows VPP's ownership split: `lookup_dpo_t` and `receive_dpo_t` are
actual pool objects, while `dpo_id_t` carries their pool index. Likewise,
`ip_adjacency_t`/`adj_nbr_*` and `load_balance_t` are concrete object stores;
the generic DPO registry does not own their bytes.

`hammer-service::net` exposes only the values and contracts needed to connect
these implementations: `DpoProto`, `DpoType`, `DpoId`, `DpoRef`, DPO class/stack
registration, FIB source/entry/path relationships, and the forwarding
projection input. A normal route path is first retained as a `FibPath` with
its next-hop, interface, table, weight, preference, and path flags. The IP
owner resolves that path when the source becomes contributing, creates or
updates the IP-owned adjacency/load-balance objects, acquires the result through
`DpoMain`, and records the resulting service `DpoRef` in the generic FIB graph.
The worker-visible projection uses `DpoRef::id()`. The generic FIB never
reaches into an adjacency or load-balance pool and never imports an IP type.

This follows VPP's ownership split: `ip_adjacency_t` and `adj_nbr_*` live under
`vnet/adj`, while `load_balance_t` is a concrete DPO implementation under
`vnet/dpo` and is consumed by IP and MPLS nodes. Hammer keeps the DPO identity
and FIB graph reusable without prematurely moving either concrete object into
the service crate. A future non-IP plugin can provide its own DPO instance
pool or reuse the generic bucket algorithm through a separate, explicitly
owned implementation.

### Concrete DPO construction and projection

`DpoId` is the final runtime projection, not the object construction interface.
A control-plane path first creates or reuses an object in the concrete class
pool (or selects a service-owned singleton for a stateless class), then
projects that object identity into the compact service value stored by FIB and
graph state:

```text
route path semantics
    -> owner class pool object / singleton
    -> class slot + pool index
    -> DpoId { type, proto, index, next }
    -> FIB entry / DPO stack / graph next
```

The concrete classes in this migration are split by ownership:

```text
hammer-service::net::dpo
  DropDpo, PuntDpo                 singleton/stateless DPO classes
  LookupDpo, ReceiveDpo             service-owned pools because they reference
                                    FIB/interface state

hammer-plugins/ip
  AdjacencyDpo, LoadBalanceDpo
  (AdjacencyDpo also supplies the midchain/glean/mcast subtype class keys;
   there is no separate MidchainDpo pool)
```

The exact constructors remain class-specific. A normal Binary API route
handler does not match `path_type` to construct a DPO and does not write a raw
class/index pair. It decodes the request into a service `FibPath`; the IP
owner's path resolver later selects the concrete adjacency subtype, resolves
recursive/interface state, and projects the resulting object to `DpoId`.
`Local`, `Drop`, `SourceLookup`, and `InterfaceRx` are VPP path semantics in
that same input, not a generic DPO constructor API. An owner-internal special
operation may call `special_dpo_add/update` with an already constructed
concrete `DpoId` (for example the IPv6 link-local lookup DPO), but that direct
DPO form is not exposed by the ordinary route message. A normal adjacency may
classify itself as incomplete, midchain, glean, or multicast while it is being
resolved; no route request constructs a `MidchainDpo` object or writes a raw
class/index pair.

`DpoType` is the service-assigned class key, not a stand-alone object. Its
meaning comes from the installed `DpoClassRecord` and the concrete pool and
operations bound to that key by the owning class module. `DpoProto` selects the
protocol-specific graph node/edge. `DpoId.index` is never globally
dereferenced; only the matching class owner may turn `(DpoType, index)` back
into a concrete object. This keeps path semantics visible at the plugin seam
while retaining VPP's compact `dpo_id_t` shape without dynamic dispatch.

The ownership mapping to VPP is explicit:

| VPP object | Hammer owner and object | Meaning |
| --- | --- | --- |
| `dpo_proto_t` | `hammer-service::net::DpoProto` | data-path protocol used to choose a graph arc |
| `dpo_type_t` | `hammer-service::net::DpoType` + `DpoClassRecord` | class key and registered per-protocol nodes; the matching concrete owner stores the object pool and operations for that key |
| `dpo_id_t` | `hammer-service::net::DpoId` | `{ type, proto, index, next }` identity produced from an owner pool object or singleton |
| `dpo_nodes[type][proto]` | `DpoClassRegistration::nodes` consumed by `NetMain` | static `(DpoProto, node-name)` bindings |
| `dpo_edges[child][proto][parent][proto]` | `hammer-service::net::DpoStackRegistry` | derived graph edge for stacking |
| `dpo_vft_t` | service class lifecycle callbacks plus owner-local concrete operations | service stores only class-indexed lock/release; format, MTU/uRPF, interpose, and object mutation remain with the concrete owner |
| `dpoi_index` + `dpoi_type` | owner-local `(class slot, Pool<T>)` lookup | index is valid only in the pool belonging to the matching class slot |

Thus the concrete `AdjacencyDpo` class is in the IP plugin because its node
bindings, object pool, and class operations are IP-owned. The service owns the
assigned class slot, node/edge derivation, and `DpoId` layout; it does not own
the adjacency objects. A different plugin can contribute another class binding
and its own pool without changing `DpoId` or importing the IP plugin.

Every DSO that contributes this image exposes one fixed service-owned C ABI
symbol. The service consumer resolves that symbol through the existing
`PluginMain::get_plugin_symbol` seam using the concrete signature
`unsafe extern "C" fn() -> RRef<'static, NetRegistrationImage>`; no public Rust
function-pointer alias is introduced. The runtime loader keeps the DSO alive
and performs symbol lookup; it does not decode, merge, or own the image. The
service owner validates the `StableAbi` image and consumes it before
`net_main_init` completes. Static network declarations must be available before
that point; loading a new declaration image after consumption is rejected.
Built-in service images use the same `StableAbi` data types without crossing a
DSO symbol seam.
Runtime-owned route updates are exposed through the owner plugin's Binary API
methods, not a direct FIB publication handle.

### FIB and DPO split

The following protocol-neutral forwarding types move to
`hammer-service::net`:

- `DpoProto`, `DpoType`, `DpoId`, `DpoRef`, `DpoMain`, and the internal
  `DpoStackRegistry`;
- `FibSource`, `FibEntry`, `FibPathList`, `FibPath`, source precedence/merge
  state, and the FIB mutation/publication contract;
- `ForwardingMetadata`, because it carries only DPO/FIB facts in the packet
  opaque area;
- the protocol-neutral DPO/FIB facts consumed by a flow-hash implementation;
  packet parsing and the concrete hash configuration remain with the IP4/IP6
  owner.

`FibLookupResult` is not a service type in the target design. VPP has two
different results: control-plane lookup returns a `fib_entry` index, while the
packet lookup returns the concrete forwarding load-balance index and then
follows its selected child DPO. Those are separate operations and are not
collapsed into one result object.

`DpoProto` is retained only as the service-owned data-path link discriminator
described by VPP's `dpo_proto_t`. In VPP this answers “which packet graph/link
format does this DPO emit?”; it is not an IP protocol number, an ICMP/TCP/UDP
protocol selector, or a protocol-family registration API. DPO stacking needs
the parent and child data-path protocols to select the correct graph arc; the
service layer owns those constants and their node/edge table. The IP plugin
selects the concrete `DpoProto` at its node/class call site and there is no
conversion from a wire protocol or a unified address-family value.

The FIB is an incremental control-plane graph, not a whole-table snapshot
builder. `hammer-service::net::fib` owns the graph, source precedence,
source/path lifecycle, cover relationships, and back-walk scheduling. The IP
plugin instantiates it twice with one concrete backend per implementation:

```text
FibTable<Ipv4Net, Ip4FibBackend>
FibTable<Ipv6Net, Ip6FibBackend>
```

`FibTableBackend` is the only backend seam. It deliberately mirrors VPP's
non-forwarding and forwarding databases. It receives a complete `DpoId` only
for owner projection/teardown and never receives an external `table_id`, an
arbitrary `fib_index`, or a plugin pool from a caller. A backend instance is
bound to the concrete table context when the table is created;
the IPv6 instance may still write the process-wide forwarding hash, but it
injects its bound `fib_index` internally. Every fallible mutation returns an
owner-local typed error so the FIB can prepare all participants before the
first store mutation:

```rust
pub trait FibTableBackend {
    type Prefix: Copy;
    type Address: Copy;
    type Error;

    // Non-forwarding database: prefix -> fib_entry index.
    fn lookup(&self, prefix: Self::Prefix) -> Option<u32>;
    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32>;
    fn less_specific(&self, prefix: Self::Prefix) -> Option<u32>;
    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error>;
    fn remove_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error>;

    // Derived forwarding database: packet address -> entry load-balance index.
    // The backend is already bound to one concrete table context.
    fn forwarding_lookup(&self, address: Self::Address) -> Option<u32>;
    fn forwarding_update(
        &mut self,
        prefix: Self::Prefix,
        dpo: DpoId,
    ) -> Result<(), Self::Error>;
    fn forwarding_remove(
        &mut self,
        prefix: Self::Prefix,
        old: DpoId,
        cover: Option<(Self::Prefix, DpoId)>,
    ) -> Result<(), Self::Error>;

    // Owner-specific path resolution and DPO projection.
    // None means the entry is not installed in the forwarding database.
    fn project_forwarding(
        &mut self,
        entry: &FibEntry<Self::Prefix>,
        source: FibSource,
    ) -> Result<Option<DpoRef>, Self::Error>;
}
```

`DpoId` in this contract is the concrete, non-generic eight-byte identity
described above; `DpoId<N>` is not a target API. `project_forwarding` returns a
`DpoRef`, not a raw identity, so the concrete backend must acquire the new
object through `DpoMain::lock` or `DpoMain::stack` before service can publish
it. `special_dpo_add/update` and concrete bucket construction accept the same
owning value. This keeps the VPP reference-acquisition point without leaking
plugin object types or pools into `hammer-service::net`.

The service FIB stores the current `DpoRef` with the entry projection and passes
`DpoRef::id()` to `forwarding_update/remove`. Replacement moves the old
`DpoRef` out only after the forwarding store no longer names it; `Drop` then
dispatches the concrete release. There is no projection wrapper, object store,
manual unlock, or second lifecycle interface.

`FibTable<P, B>` is statically dispatched. It owns `FibEntry<P>`, source
records, shared `FibPathList`, configured `FibPath` values, graph-node
references, source ownership records, source route counts, and the current
forwarding projection reference. `B` is the only implementation seam: it owns
the concrete prefix stores, resolves owner-dependent paths, creates or updates
concrete `LoadBalanceDpo`/special DPO objects, and returns the complete `DpoRef`
whose identity VPP passes to the protocol backend. The backend writes only
`dpoi_index` into
its forwarding table. There is no separate forwarding adapter, callback
registry, or erased owner state.

`project_forwarding` is the Rust seam for VPP's
`fib_entry_src_mk_lb`. It receives the service-owned entry and source facts;
the backend may use its concrete IP owner state, but the service FIB never
dereferences a plugin pool. For a fallible Rust mutation, `FibTable` builds the
candidate source/path state in detached `FibEntry`/`FibPathList` values, asks
the backend to project that candidate, and commits the candidate only after
projection and store validation succeed. An error therefore leaves the
published source graph and the owner's current projection unchanged; no
candidate wrapper type is introduced. If a projection already exists and its
class/chain remains valid, the backend updates that owner object in place. A
class, chain, exclusive, or interpose change prepares a replacement before the
old projection is removed. The backend acquires the new projection through
`DpoMain` before `forwarding_update` publishes its `dpoi_index`; the old
`DpoRef` remains reachable until the replacement has completed.

The service FIB calls `project_forwarding` during source activation,
reactivation, and back-walk. If it returns a DPO, the FIB calls
`forwarding_update`; if it returns `None`, `forwarding_remove` removes the old
forwarding identity and the service FIB drops the now-unreferenced `DpoRef`
after worker quiescence. This is the Hammer
equivalent of
VPP's `fib_entry_src_mk_lb` followed by `fib_table_fwding_dpo_update`, while
keeping source behavior and concrete DPO objects in the owner that can resolve
them.

Lookup therefore has two explicit control/data-plane forms. A control-plane
lookup accepts a route `Prefix` and returns a `fib_entry` index for inspecting
source/path state. The packet path accepts a concrete destination `Address`
(not a prefix with its host bits discarded), performs the IP implementation's
LPM in the forwarding store, recovers the entry's load-balance index, selects
its bucket, and only then obtains the bucket's complete child `DpoId`. The
forwarding store's `u32` is the entry load-balance `dpoi_index`; it is not a
standalone DPO identity. The complete `DpoId` remains in the FIB entry for
class/protocol dispatch, while its control-plane `DpoRef` owns the lifetime.
There is no erased table, `dyn` backend,
`FibTableHandle`, or unified protocol-family
facade. `Ip4Main` owns
the IPv4 table instances and table-id map; `Ip6Main` owns the IPv6 instances
and map. Both use the same service graph contract without sharing a protocol
enum or index wrapper.

### FIB table and source lifecycle

The VPP-shaped table operations are separate interfaces with separate
failure and ownership semantics:

| Operation | FIB effect |
| --- | --- |
| `special_add` | Add a source-owned contribution whose behavior supplies the default drop DPO or another source-defined special result. Repeated adds update the source contribution count. |
| `special_dpo_add` / `special_dpo_update` | Add or replace a source-owned concrete `DpoRef`; these are owner-internal operations, and the source owns the projection until that source contribution is removed. |
| `special_remove` | Remove one special source reference; the entry remains while another source contributes. |
| `path_add` | Add the listed paths incrementally to this source's path set. |
| `path_remove` | Remove only the listed paths; if the source has no paths left, remove or inherit the source. |
| `update` / `update_one_path` | Replace this source's complete path set, or replace it with one path. |
| `delete` / `delete_index` | Delete this source contribution by prefix or entry index; only the last source removes the entry from the backend. Deleting an absent prefix or an absent source is an idempotent no-op, matching VPP. |
| `flush` | Delete every entry contributed by one source. |
| `mark` then `sweep` | Mark that source's existing entries stale, accept a resync, then delete only entries still stale. This is not `flush`. |
| `find_or_create` | Resolve table-id to `fib_index` and return owner-borrowed table access. Table destruction is allowed only after the mutable borrow and all source-owned values have ended. |

`special_dpo_add` and `special_dpo_update` remain `FibTable` operations for
service-owned built-ins and owner-internal plugin code. They accept a complete
`DpoRef` already acquired through the class lifecycle dispatch. The source
entry keeps that projection reachable until the contribution is removed, then
moves the reference out as part of the same failure-atomic forwarding mutation.
Rust `Drop` performs the final release and owner-local child cleanup.
They are not a generic DPO constructor and are not exposed as raw `DpoId` fields
in the ordinary route Binary API.

Creating a table installs the protocol implementation's mandatory default and
special entries. The table stores protocol-independent facts equivalent to
VPP `fib_table_t`: external `table_id`, internal `fib_index`, table flags
(including IPv6 link-local and resync), source ownership records, active source
count, per-source route counts, total route count, flow-hash configuration,
epoch and description. The table-id map is removed only after entry teardown
and forwarding projection removal. `table_id` is never used as a `fib_index`.

FIB source registration retains VPP's two-part identity: priority class and
behavior. `NetMain` assigns a unique slot within each priority class, so
ordering is `(priority_class, priority_slot)` and equal-class sources cannot
accidentally form an ECMP choice. The image contains metadata only. The
service stores the behavior tag and runs generic source precedence, reference
counts, cover tracking, and back-walk. Behavior-dependent actions are owned by
the concrete backend that can resolve the source (for example interface,
adjacency, recursive, MPLS, LISP, or interpose behavior); no plugin callback
table or source-specific state is serialized into the image.

For each entry, the source lifecycle is ordered as follows: create the source
record and configured path list; apply the source behavior's add/path rules;
activate it when it becomes the best source; link its parent path list and
detect recursive loops; ask the concrete backend whether the entry can be
installed; reactivate and recalculate when the winning source
changes; deactivate and unlink it when another source wins; notify every
source on forwarding changes; and deinitialize the source only after its
reference count reaches zero. Cover-change and cover-update decisions are
returned by the generic source lifecycle and carry a back-walk reason. Losing
sources remain attached to the entry and are eligible to become active after
the winner is removed. This preserves VPP's `fib_entry_src_vft_t` seam without
putting a runtime VFT, function pointer, or plugin state in the service FIB.

For unicast forwarding, each `FibEntry` has one main forwarding projection per
forwarding chain, matching VPP's `fib_entry_t::fe_lb`. The first contributing
source creates the owner's concrete load-balance object and inserts its
`dpoi_index: u32` into the backend. Later path, adjacency,
recursive-resolution, or source-winner changes update that existing object in
place when its class and chain remain valid; exclusive, interpose, chain-type,
or owner policy changes prepare a replacement before the old projection is
removed. When the entry loses its last contributing source, the FIB removes the
forwarding projection, waits for the worker quiescence required by the runtime
publication contract, and the concrete owner removes the old load-balance value
from its pool after the final `DpoRef` is released. Its child references are
then dropped by ordinary Rust field destruction. Exclusive and interpose entries still follow VPP's source-selection
rules before a unicast load-balance is built.

### Path lists, recursion and back-walk

`FibPathList` is a graph node and a shared set, not a mutable `Vec` exposed to
callers. Its canonical key is the complete sorted **configured** path set plus
the path-list key flags (currently the uRPF suppression flag). Shared lists are
deduplicated in a table and remain owned while source records and child links
refer to them. An incremental path add/remove on a shared list creates or
reuses a different path list before the source switches to it; once no source
or child link refers to the old list, its concrete owner removes it and Rust
`Drop` handles its fields. Each configured `FibPath` is also a graph node and
contains only input facts: next-hop protocol/address union, interface/table
identity, weight, preference, path flags, path type and object IDs required by
special path types. Its derived operational state (resolved DPO, resolving
entry, up/down/looped status and owner pool identity) is kept by the concrete
backend projection state and is never part of the path-list key. Resolution may
add a dependency on another FIB entry/path list.

Path-list back-walk rebuilds its uRPF contribution, detects recursive cycles,
and propagates to children. A small child set is walked synchronously; a
popular list may enqueue a low-priority walk, matching VPP's convergence/speed
trade-off. Back-walk contexts carry a reason and depth; supported reasons are
`Resolve`, `Evaluate`, `InterfaceUp`, `InterfaceDown`, `InterfaceBind`,
`InterfaceDelete`, `AdjacencyUpdate`, `AdjacencyMtu`, and `AdjacencyDown`.
Node references are `(node_type, u32 index)` values, never pointers. Depth is
bounded at the VPP graph limit (32); exceeding it is a recursive-cycle error.
Queued walks are owned and drained by the FIB control context. Data workers
only consume the published forwarding projection.

Interface address and link-state changes therefore submit interface-source
contributions through the same entry/path machinery. The IP owner creates
connected/attached, local receive, glean, broadcast/drop and uRPF results as
concrete path or special-DPO contributions. They do not write a forwarding
trie directly.

This ADR covers unicast FIB only. VPP's MFIB has distinct `mfib_table_t`,
`mfib_entry_t`, multicast path semantics and replicate forwarding objects.
`Ip4Main`/`Ip6Main` may retain concrete MFIB ownership and
`mfib_index_by_sw_if_index` mappings, but MFIB is not implemented by
`FibTable<P, B>` and requires a separate design before it moves into service
net. The `multicast` flag on a path or entry is therefore rejected by the
unicast route methods until that separate MFIB design exists; it is not a
request to store multicast state in the unicast backend.

Flow hashing follows VPP's order: the selected forwarding backend is found by
LPM first; only then does the IP4/IP6 owner read that load-balance object's
hash configuration and compute or reuse the packet hash. The concrete inputs
remain in the IP owner (addresses, valid transport ports, protocol, IPv6 flow
label, tunnel identifiers, and router-id mixing where configured). Nested
load-balance traversal consumes a shifted hash so recursive levels do not
polarise. The service FIB does not parse transport headers and does not add a
packet-family hash branch.

For a route mutation, the IP owner and generic service FIB perform the VPP
sequence:

1. resolve `table_id` to the concrete table and validate the prefix, source,
   paths, interfaces and next-table facts;
2. select the VPP operation: special add/remove, incremental `path_add` or
   `path_remove`, whole-set `update`, one-path update, or source `delete`;
3. normalize and sort paths, find or create the canonical path list, and build
   the candidate source state while keeping losing source contributions
   attached to the entry;
4. run candidate source activation/deactivation/reactivation and cover/back-
   walk logic;
5. for a contributing unicast source, pass the service-owned entry/source
   facts to the concrete backend. `project_forwarding` resolves retained
   paths, validates source-specific behavior, and prepares a new or in-place
   `LoadBalanceDpo` projection as a complete `DpoRef`; the backend acquires it
   through `DpoMain` before its identity is published. The service FIB does not
   construct or discover a DPO;
6. commit the non-forwarding entry store and derived forwarding store through
   the backend. On removal, the FIB supplies the less-specific cover's
   `(prefix, DpoId)` when the backend needs it: IPv4 restores the cover through
   the MTRIE, while IPv6 deletes the exact
   `(bound fib_index, masked prefix, prefix length)` key and updates shared
   prefix-length accounting. Every backend operation is failure-atomic; an
   error leaves the source graph and both stores unchanged;
7. after a successful backend removal and worker quiescence, the service FIB
   moves the old `DpoRef` out of its slot. Rust `Drop` dispatches the matching
   release; the concrete owner destroys a zero-count value and its children. A
   plain `DpoId` copy never owns an object.

This corresponds to VPP `fib_table_entry_special_*`,
`fib_table_entry_path_add/remove`, `fib_table_entry_update`,
`fib_table_entry_delete`, `fib_entry_src_action_*`,
`fib_path_list_*_add/remove`, `fib_table_fwding_dpo_update/remove`, and
`fib_walk_sync/async`. The non-forwarding graph remains the source of truth;
the forwarding trie/hash is only its derived data-plane projection.

The forwarding projection is mutated only while the Binary API dispatcher holds
the worker barrier. A worker keeps using the installed concrete table values
after the dispatcher releases the macro scope; the owner must not reclaim or
recycle an entry, path list, load-balance object or DPO until the same
quiescence rule used by the existing graph/interface publication path has
completed. There is no `FibTableHandle`, `Arc<UnsafeCell<_>>`,
`ArcSwapOption`, immutable-table replacement, or second pointer-publication
protocol.

### Binary API control-plane publication

Runtime network publication has one external ingress: the existing Binary API.
The plugin that owns the domain defines its protobuf request/reply and registers
the synchronous handler with the existing component macro:

```rust
#[hammer_component_macros::binary_api(name = "ip4.route.add_del")]
fn ip4_route_add_del(request: Ip4RouteAddDelRequest) -> Ip4RouteAddDelReply;

#[hammer_component_macros::binary_api(name = "ip6.route.add_del")]
fn ip6_route_add_del(request: Ip6RouteAddDelRequest) -> Ip6RouteAddDelReply;

#[hammer_component_macros::binary_api(name = "ip4.route.lookup")]
fn ip4_route_lookup(request: Ip4RouteLookupRequest) -> Ip4RouteLookupReply;

#[hammer_component_macros::binary_api(name = "ip6.route.lookup")]
fn ip6_route_lookup(request: Ip6RouteLookupRequest) -> Ip6RouteLookupReply;
```

The two concrete method sets avoid a Rust family enum while retaining VPP's
separate IPv4/IPv6 lookup backends. Their request fields follow VPP's
`ip_route_add_del` and `ip_route_lookup` semantics:

| 方法 | 请求事实 | 所属 handler 的动作 |
| --- | --- | --- |
| `ip4.route.add_del` | `is_add`, `is_multipath`, external `table_id`, concrete IPv4 prefix, and one or more VPP-shaped path descriptions | decode paths to service `FibPath` values, use the fixed `FIB_SOURCE_API` source, validate `sw_if_index` through `InterfaceMain`, and invoke the IPv4 owner's FIB mutation. The handler does not construct a DPO; the IP path resolver creates or updates concrete objects when the source contributes. |
| `ip6.route.add_del` | same operation with a concrete IPv6 prefix | decode paths to service `FibPath` values, use the fixed `FIB_SOURCE_API` source, and invoke the IPv6 owner's FIB mutation. Direct `DpoId` input is reserved for owner-internal special-DPO operations. |
| `ip4.route.lookup` | external `table_id`, `exact`, concrete IPv4 address/prefix | perform non-forwarding FIB LPM/exact lookup and return the matched route's prefix, paths and stats index |
| `ip6.route.lookup` | external `table_id`, `exact`, concrete IPv6 address/prefix | perform non-forwarding FIB LPM/exact lookup and return the matched route's prefix, paths and stats index |

The payload contract is concrete and wire-visible:

| 消息 | 字段 |
| --- | --- |
| `Ip4RouteAddDelRequest` | `is_add: bool`, `is_multipath: bool`, `route: { table_id: u32, prefix: IPv4 prefix, paths: repeated FibRoutePath }` |
| `Ip6RouteAddDelRequest` | `is_add: bool`, `is_multipath: bool`, `route: { table_id: u32, prefix: IPv6 prefix, paths: repeated FibRoutePath }` |
| `FibRoutePath` | `sw_if_index: u32`, `table_id: u32`, `rpf_id: u32`, `weight: u8`, `preference: u8`, `path_type: FibPathType`, `flags: FibPathFlags`, `proto: FibPathNhProto`, `nh: FibPathNh`, `n_labels: u8`, and `label_stack: FibMplsLabel[16]` |
| `Ip4RouteLookupRequest` | `table_id: u32`, `exact: bool`, `prefix: { address: bytes(4), length: u32 }` |
| `Ip6RouteLookupRequest` | `table_id: u32`, `exact: bool`, `prefix: { address: bytes(16), length: u32 }` |
| `*RouteAddDelReply` | `status: IpRouteStatus`, `stats_index: u32` |
| `*RouteLookupReply` | `status: IpRouteStatus`, `route: Option<{ table_id: u32, stats_index: u32, prefix, paths: repeated FibRoutePath }>` |

`FibPathType` is the complete VPP wire path behavior (`normal`, `local`, `drop`,
`udp_encap`, `bier_imp`, `icmp_unreach`, `icmp_prohibit`, `source_lookup`,
`dvr`, `interface_rx`, and `classify`); it is not an ICMP/TCP/UDP protocol
number. `FibPathNhProto` is a separate wire enum (`ip4`, `ip6`, `mpls`,
`ethernet`, `bier`) and is decoded to the internal data-path discriminator only
at the concrete owner seam. `FibPathNh` is the VPP next-hop union containing
the address, MPLS label, object id and classify table index fields. The decoder
retains all path fields and converts them to service `FibPath` facts; the IP
owner resolves them. Ordinary route messages never carry a concrete `DpoId`;
only owner-internal `special_dpo_add/update` calls do. For add/delete,
`is_add && !is_multipath` replaces this source's path set,
`is_add && is_multipath` adds to it, `!is_add && is_multipath` removes the
listed paths, and `!is_add && !is_multipath` removes the source contribution.
Empty or incorrectly sized address bytes, invalid prefix lengths, and an empty
path set where a path is required are typed `IpRouteStatus` errors. An absent
route on delete follows VPP's idempotent no-op semantics; `RouteMissing` is
reserved for a lookup that requires an existing entry. The owner-local enum
has the actionable variants `Ok`, `TableMissing`, `SourceMissing`,
`PrefixInvalid`, `PathInvalid`, `InterfaceMissing`, `DpoClassMissing`,
`CapacityExhausted`, and `RouteMissing`; it has no catch-all message variant.

The route methods use the registered API source internally; a client cannot
submit an arbitrary source id. `table_id` is the external table name and is
never treated as a `fib_index`. Paths carry raw `sw_if_index`, next-hop facts,
weight, preference, next-hop protocol and VPP path flags. The IP handler
validates those facts and gives the generic FIB service-owned `FibPath` values;
the IP resolver later produces concrete `DpoId` identities. No DPO message
field contains an IP, ICMP, TCP, or UDP plugin type.

Route add/delete and lookup methods are non-`mp_safe` because they touch the
worker-visible forwarding projection. The Binary API dispatcher, not the
handler, performs the only synchronization:

```text
Binary API Process Node
  -> resolve registered method
  -> worker_thread_barrier_sync!(main, {
       validate and mutate owner state;
       return typed protobuf reply;
     })
```

The handler must not call `WorkerBarrier::sync`, `barrier::__sync_guard`, or
the barrier macro itself. A nested Binary API call observes the already-held
scope and runs without reacquiring it. Validation happens before the first
graph mutation; a failed request returns an owner-defined status and leaves
all FIB source, entry, DPO, interface, and lookup state unchanged.

The current `[network].route` list is removed as a runtime publication path.
If startup defaults are needed, a control client submits the same Binary API
messages after plugin initialization; no second config-only route mutation API
is introduced. Binary API's existing envelope, context correlation, and
handler ABI remain unchanged; these are new plugin-owned methods and typed
payloads.

### DPO dispatch and IP local protocol dispatch

These are separate contracts:

```text
DPO class + DpoProto  -> per-proto forwarding node (ip4-rewrite/ip6-rewrite)
IP protocol number     -> IP local-next slot -> plugin graph node
```

The DPO class determines the forwarding next edge for a DPO instance. It does
not decide which node handles an IP protocol number. The IP plugin owns the
local-next tables and exposes two VPP-shaped concrete APIs:

The service DPO registry imports no `hammer-plugins::*` crate and stores no
plugin-owned type, object pool, general object-operation table, or capability.
It stores only the service-owned class metadata and lifecycle callback pair. A
plugin contributes a DPO class through that static declaration face, then binds
the returned runtime class key to its concrete pool and remaining operations.
`NetMain` resolves class node names and derives type/protocol graph edges, but
never dereferences a plugin object's `dpoi_index`. IP/ICMP dependency exists
only in IP local-next and ICMP error registration, never in the generic DPO
registry.

```rust
pub fn register_ip4_protocol(protocol: u8, node: NodeId) -> RuntimeResult<()>;
pub fn register_ip6_protocol(protocol: u8, node: NodeId) -> RuntimeResult<()>;
```

The ICMP, TCP and UDP plugins call the appropriate concrete function from their
node-registration path. The existing single `register_protocol(protocol, node)`
surface is removed so a caller cannot silently register a protocol in the
wrong table.

The ICMP error output edge is installed through explicit IP-owned concrete
registration functions (`register_ip4_icmp_error_node` and
`register_ip6_icmp_error_node`). If no ICMP error consumer is
installed, the IP rewrite node uses its drop next. This is an explicit
IP-to-ICMP dependency, not a generic callback carrier.

### Complete VPP subsystem alignment

The following table is the complete ownership map for the IP-facing network
stack. A row is an ownership decision, not a promise that Hammer will copy the
VPP C type or field layout literally.

| VPP authority or path | Hammer owner | State and interface that remain at that seam |
| --- | --- | --- |
| `vlib_global_main_t` | `GlobalMain` | worker lifecycle, plugin lifetime, graph publication/refork, and control scheduling |
| `vnet_main_t` | `NetMain` | network authority, reserved `local0`, and embedded `InterfaceMain` |
| `vnet_interface_main_t` | `InterfaceMain` | `HwInterface`, `SwInterface`, RX/TX queues, interface callbacks, MTU and output-node relationships |
| `vnet_device_main_t` | `DeviceMain` | device worker range, RX accounting and scheduling; it does not own interface instances |
| `vnet_device_class_t` | device plugin plus `InterfaceMain` class record | device-owned TX/RX/admin/MAC/RSS/redirect callbacks; service stores the declaration and selected class slot |
| `ip_main_t` | `IpMain` | IP protocol and TCP/UDP port registries only |
| `ip_lookup_main_t` | `Ip4Main` and `Ip6Main` | interface-address pool, local-next table, feature-arc indices, and packet lookup metadata for the concrete implementation |
| `ip4_main_t` / `ip6_main_t` | `Ip4Main` / `Ip6Main` | concrete unicast FIB table contexts, MFIB owners, external table-id maps, per-interface table binding, callbacks and flow-hash/host policy; only unicast FIB is specified here |
| `fib_main_t`, `fib_table_t`, `fib_entry_t`, `fib_path_list_t` | `hammer-service::net::fib` | source contributions, entry attributes, configured path-list sharing, recursive dependencies, back-walk and the current forwarding `DpoRef`; the concrete backend stores only projected identity facts |
| `dpo_vfts`, `dpo_nodes`, `dpo_edges` | `hammer-service::net::dpo` plus owner-local object operations | class slots, declared per-link nodes, derived class/proto edges, compact identity, and lock/release lifecycle dispatch; no plugin object bytes |
| `ip_adjacency_t`, `adj_nbr_*` | IP plugin forwarding owner | one shared adjacency pool with normal/incomplete/midchain/glean/multicast class slots, rewrite header and egress interface facts |
| `load_balance_t` | IP plugin forwarding owner | concrete bucket pool and hash selection; packet buckets are child `DpoId`s and control state owns their `DpoRef`s |
| `lookup_dpo_t` | service net DPO implementation | one pool object with source/destination, configured/interface table, unicast/multicast and table identity facts |
| `receive_dpo_t` | service net DPO implementation | one pool object with concrete receive interface and local address facts |
| `interface_rx_dpo_t` / interface-TX DPO | `InterfaceMain` | RX object pool and stateless TX wrapper around `sw_if_index`; TX next is instance-dependent |
| `ip_pmtu_t` / `ip_pmtu_dpo_t` | IP plugin forwarding owner | FIB-linked PMTU tracker and interposed PMTU DPO; ICMP and TCP use typed IP operations |
| `ip4-input` / `ip6-input` | IP plugin | concrete header validation and metadata preparation |
| `ip4-local` / `ip6-local` | IP plugin | local feature arc, checksum/source checks and IP-protocol local-next dispatch |
| `ip4-lookup` / `ip6-lookup` | IP plugin | concrete LPM lookup and post-LPM load-balance selection |
| `ip4-rewrite` / `ip6-rewrite` and adjacency siblings | IP plugin | concrete header policy, MTU check, rewrite and TX interface publication |
| `icmp4`, `icmp6`, echo/error/PMTU nodes | ICMP plugin | ICMP parsing, type tables, generated packets and explicit registration into IP local-next/error seams |
| `vl_api_ip_*` handlers | owning IP/ICMP/device plugin | request validation and owner calls; publication is performed by the Binary API dispatcher under the worker barrier |

The packet graph is therefore concrete at both ends and generic only at the
service seams:

```text
device-owned RX queue/node
  -> selected parser/next edge
  -> NetworkOpaque.sw_if_index[RX] + NetworkIpOpaque.fib_index
  -> ip4-input or ip6-input
  -> concrete local feature arc and local-next table
  -> ip4-lookup or ip6-lookup
  -> concrete unicast LPM -> per-entry load-balance object -> child DpoId
  -> DPO class/proto edge
  -> receive/drop/punt/lookup/adjacency/PMTU node
  -> ip4-rewrite or ip6-rewrite
  -> NetworkOpaque.sw_if_index[TX]
  -> InterfaceMain output-node/queue selection
  -> device-owned TX function
```

`DpoProto` is used only at the DPO graph seam in this path. The wire IP
protocol number is consumed by `ip4-local`/`ip6-local` and indexes that
implementation's `local_next_by_ip_protocol[256]`; it never indexes a DPO
class. Conversely, a DPO class never decides whether a packet is ICMP, TCP or
UDP. This is the separation that permits ICMP to be an independent DSO while
still registering with IP.

#### IP input, local delivery, lookup and feature arcs

Each concrete IP implementation owns the VPP-equivalent sequence below; the
service feature-arc module supplies only the generic ordering and per-interface
configuration machinery:

1. The selected input node validates the concrete header and writes the packet
   cursor, IP protocol number, ECN and `fib_index` metadata.
2. The input node resolves the FIB through the implementation's
   `fib_index_by_sw_if_index[NetworkOpaque.sw_if_index[RX]]` mapping. An
   optional packet-level `fib_index_override` is a separate IP metadata fact;
   it never reuses or overloads `NetworkOpaque.sw_if_index[TX]`, which remains
   the concrete output interface.
3. The input feature arc runs before lookup. The local arc classifies a local
   destination, validates the local packet, advances its feature chain, and
   finally selects a local-next slot by the wire IP protocol number.
4. The local-next default is punt or drop. ICMP registers its concrete node in
   both local tables through `register_ip4_protocol` and
   `register_ip6_protocol`; the registration call is the only IP/ICMP local
   dispatch seam.
5. The concrete unicast lookup node performs LPM, obtains the entry's
   load-balance object index, computes the configured flow hash after LPM, and
   follows the selected child DPO. MFIB lookup is a separate IP-owned path and
   is not implemented by this ADR. The service FIB does not parse headers or
   choose a hash input.
6. Output features run at the IP-owned rewrite/output seam. Interface output
   consumes the concrete TX `sw_if_index` and delegates queue/device work to
   `InterfaceMain` and the selected device implementation.

The concrete feature-arc declarations are the VPP-shaped `ip4-unicast`,
`ip4-multicast`, `ip4-local`, `ip4-output`, `ip4-punt`, `ip4-drop` and their
IPv6 counterparts. They are not moved into `hammer-service` and are not
collapsed into one family parameter. The existing generic `FeatureArc` module
remains a graph-ordering primitive; the IP plugin owns the concrete arc names,
feature nodes and per-protocol configuration.

#### Tables, addresses, interface binding and neighbors

The route API is only one part of the VPP control-plane surface. The IP plugin
must expose concrete Binary API operations for the following operations, each
using its owning `Ip4Main` or `Ip6Main` state:

| VPP operation | Concrete IP operation | Resulting state |
| --- | --- | --- |
| `ip_table_add_del`, `ip_table_allocate`, `ip_table_flush` | `ip4.table.*` / `ip6.table.*` | external `table_id` to internal `fib_index` map and an initialized concrete FIB |
| `ip_route_add_del`, `ip_route_dump`, `ip_route_lookup` | `ip4.route.*` / `ip6.route.*` | source contributions and their forwarding projection; dump remains a bounded multipart decision for the Binary API envelope |
| interface address add/del | `ip4.interface.address.add_del` / `ip6.interface.address.add_del` | address pool record plus connected/attached/local/glean/broadcast and uRPF contributions |
| `sw_interface_set_table` | `ip4.interface.table.bind` / `ip6.interface.table.bind` | per-interface lookup-table mapping and table-bind callbacks |
| IP flow-hash configuration | `ip4.table.flow_hash.set` / `ip6.table.flow_hash.set` | concrete table hash policy used after LPM |
| neighbor/ARP/ND add/del | `ip4.neighbor.add_del` / `ip6.neighbor.add_del` | IP-owned adjacency update; unresolved state publishes the incomplete adjacency subtype |
| path MTU update/get/replace | `ip4.path_mtu.*` / `ip6.path_mtu.*` where applicable | IP-owned FIB-linked PMTU tracker and PMTU DPO interposition |
| IP feature enable/disable | `ip4.feature.*` / `ip6.feature.*` | concrete feature-arc configuration for one `sw_if_index` |

The external table identifier is the API key. The internal `fib_index` is an
owner-local pool index. An interface address is not inserted by directly
mutating a lookup trie: its source contribution creates the connected and
local facts, and the FIB graph then selects and projects the winner. Neighbor
resolution changes the adjacency object and causes the same FIB back-walk to
replace the forwarding projection. This keeps interface, address, neighbor,
route, and recursive resolution updates on one source/entry/path-list graph.

The route methods in this ADR represent the VPP `FIB_SOURCE_API` source and do
not accept an arbitrary raw source id from a client. A plugin source id is an
internal result of `FibSource` registration. Interface, neighbor, PMTU and
other owners submit their own source contributions through owner-local control
operations; clients cannot impersonate those sources by putting a number in a
route message.

#### DPO class/object lifecycle and stacking

The class and instance graphs have these exact responsibilities:

```text
DpoType + DpoProto
  -> DpoClassRecord (class slot + declared per-link nodes)
  -> DpoStackRegistry (derived child/parent class-proto edge)

DpoId { type, proto, index, next }
  -> matching concrete owner pool or permanent singleton
  -> class-specific node/object operation
```

`DpoType` is the key used to select the class operations and graph-node table;
it does not contain an object. `DpoId.index` is the index of the real object
in the pool belonging to that class. `LookupDpo`, `ReceiveDpo`,
`AdjacencyDpo`, `LoadBalanceDpo` and `IpPmtuDpo` therefore remain object-bearing
types. `DropDpo` and `PuntDpo` are the permanent singleton exception, while
`InterfaceTxDpo` wraps an existing `sw_if_index` and has no pool.

The service class record mirrors only the VPP DPO VFT operations required for
cross-owner lifetime: `lock(index)` and its private release callback. Formatting,
memory accounting, instance-dependent next-node lookup, uRPF, MTU, interpose,
and object mutation remain owner-local. Adjacency subtype selection and
projection replacement are ordinary `&mut` operations on the concrete owner's
pool. Plain `DpoId` copies are independent identity values; every owning slot
holds a `DpoRef` acquired through `DpoMain`. The enclosing transaction provides
worker publication ordering, and `DpoRef::drop` dispatches release only after
the published identity has been removed.

`DpoMain::stack` derives or reuses the edge from the child and parent
class/proto keys, writes only the edge slot into a copied `DpoId`, acquires that
parent object, and returns `DpoRef`. It never stores an object pointer in the
edge registry or returns an unretained stacked identity.

Pool growth and index recycling follow VPP's `dpo_pool_barrier_sync` rule:
only the control owner may grow or recycle a worker-visible pool while the
worker barrier is held. A class owner removes the old concrete value before
recycling the pool slot, and no index is interpreted under a different class
key. The service FIB stores complete `DpoId` values but does not inspect object
bytes. The concrete backend performs projection replacement, path-list
replacement, forwarding replacement, and teardown through ordinary owner
mutation; each owning insertion first obtains `DpoRef` through `DpoMain`, and
removal drops that reference only after worker-visible state no longer contains
the identity. A plain `DpoId` copy in a path list or bucket is only an identity
copy, never an implicit owner. `DpoRef` is the single generic lifecycle
interface; it is not an object store and cannot resolve object bytes.

#### Interface/device contract

`InterfaceMain` is the owner of interface identity and queue topology, not the
owner of a protocol parser. A device plugin supplies a concrete device class
and its RX/TX implementations. The control-plane sequence is:

```text
device class registration
  -> InterfaceMain creates HwInterface/SwInterface and queues
  -> device plugin calls set_input_node(hw_if_index, device_rx_node)
  -> optional rx_redirect_to_node(hw_if_index, selected_node)
  -> DeviceMain schedules queues to the device RX node
```

`rx_redirect_to_node` resolves the selected device class and invokes its
concrete VFT callback, matching `vnet_hw_interface_rx_redirect_to_node`; the
service does not add a protocol-specific enum or callback carrier. The device
node decides whether the next node is Ethernet, IP, a tunnel parser, or a
device-owned consumer. If it chooses IP, the next edge is installed by the
device plugin and the buffer reaches the concrete IP input node with RX
`sw_if_index` already written. On egress, an IP DPO publishes TX
`sw_if_index`; `InterfaceMain` resolves the output node/queue and invokes the
device class TX operation.

#### Control-plane and publication contract

All table, route, address, bind, neighbor, feature, PMTU and interface/device
mutations use Binary API methods. The dispatcher owns the sequence:

```text
decode envelope -> resolve owner method ->
worker_thread_barrier_sync!({ validate all participants; mutate; reply })
```

The method's `mp_safe` declaration is the synchronization contract. Methods
that only inspect stable control state may be `mp_safe`; methods that mutate
worker-visible graph, interface, DPO or FIB state are not. A handler never
calls the barrier macro or its helper directly. Validation is complete before
the first mutation, and a failed request leaves source contributions, pools,
class bindings, interface mappings and forwarding projections unchanged.

Route lookup replies expose the matched control-plane route, not the packet
path's selected DPO. The reply contains the normalised prefix, external
`table_id`, stats index and the configured `FibRoutePath` list, matching VPP's
`ip_route_lookup` reply. Packet lookup is a separate IP data-plane operation:
the concrete IP node performs LPM in its forwarding backend, obtains the
load-balance value, selects a bucket and then follows the resulting child
`DpoId`; none of those data-plane identities are serialized in the route
lookup reply.

#### DSO and proc-macro contract

The runtime `PluginModule` ABI remains unchanged. A plugin that contributes
network declarations additionally exports the fixed `extern "C"
fn() -> RRef<'static, NetRegistrationImage>` symbol already described above;
plugins without network declarations simply do not export it. The existing
init function receives `&mut GlobalMain`; it asks `engine.plugin_main()` to
resolve the symbol through the generic `get_plugin_symbol` API, validates the
stable image, and passes the borrowed image to `InterfaceMain`/`NetMain` before
worker-visible network state is published. `NetMain` does not own a loader or
reach into `PluginMain`; the runtime keeps the DSO alive. No
`NetRegistrationImageExport` type, object pointer, pool, or callback surface
beyond the DPO class lifecycle pair is introduced.

`FibSource` and `DpoClass` are concrete proc-macro declaration faces, not
runtime dispatch. A `FibSource` derive emits only stable source metadata and an
owner-local source binding. A `DpoClass` derive emits stable class/node metadata
and a `registration()` accessor; explicit owner code attaches the lock/release
callbacks. One object owner may contribute several class keys over one pool
(for example lookup or adjacency subtypes), but the macro never emits a pool,
reference counter, generic object enum, family parameter, or `dyn` object.

The ABI image is initialization input. Runtime route entries, paths, table
maps, DPO instances, interface instances, queues, and feature-chain state are
all created by their owning Main after the image is consumed and are never
serialized into the image.

### ICMP ownership and Path MTU

The ICMP plugin owns ICMP wire parsing, type dispatch, echo/error generation,
and ICMP graph nodes. The ICMP-specific parser currently living in
`hammer-service::net::pmtu` moves into the ICMP plugin. The PMTU authority also
moves to the IP plugin to match VPP's `ip_pmtu_t`/`ip_pmtu_dpo_t`: it is a
FIB-linked forwarding policy, not a generic service cache. ICMP parses a
Fragmentation Needed message and calls the IP plugin's typed
`ip_path_mtu_update` operation; TCP consumes the typed IP PMTU lookup through
its existing explicit IP dependency. `hammer-service::net` contains neither
ICMP bytes nor PMTU policy.

The existing PMTU worker-publication behavior must be audited separately. This
ADR does not add a lock or a new cross-worker publication protocol.

### Device to IP data path

The service layer no longer owns a fixed device-to-IP next enum. A device
plugin owns its RX node and chooses its next target. `InterfaceMain` owns the
hardware-interface relationship and exposes two VPP-shaped operations:

```rust
pub fn set_input_node(hw_if_index: u32, node: NodeId) -> InterfaceResult<()>;
pub fn rx_redirect_to_node(hw_if_index: u32, node: NodeId) -> InterfaceResult<()>;
```

`set_input_node` installs the device plugin's own input node for queue
scheduling. `rx_redirect_to_node` validates the hardware interface, finds its
selected `DeviceClass`, and invokes that class's concrete
`rx_redirect_to_node` function pointer. The callback installs the next edge in
the device input node. There is no service-owned `DeviceRx` trait, `dyn`
dispatcher, or erased callback state.

The data path is:

```text
device RX node
    -> device-owned normalization and next-edge selection
    -> NetworkOpaque.sw_if_index[RX]
    -> IP input/local/lookup (when selected by the device)
    -> DPO lookup and adjacency rewrite
    -> NetworkOpaque.sw_if_index[TX]
    -> InterfaceMain TX queue lookup
    -> selected device TX implementation
```

The device may select Ethernet, IP, tunnel, or another parser. IP never polls
device queues, and a device never performs IP/FIB parsing merely because it is
registered with an interface.

### `local0` and index invariants

`local0` is created by `NetMain` as an always-present reserved interface. It:

- has a stable hardware and software interface index;
- rejects ordinary address assignment and is not a normal route egress;
- may be referenced by FIB/uRPF logic only for sentinel or no-specific-interface
  semantics;
- is not substituted for a concrete receive DPO interface.

A receive DPO with a concrete interface stores that actual `sw_if_index`. A
receive DPO without one stores the invalid-interface value `u32::MAX`. A
`fib_index` is never inferred from, stored in place of, or numerically equated
with a `sw_if_index`.

Interface deletion first quiesces worker access, removes RX/TX queues, removes
the interface's FIB source contributions through the owning control operation,
and only then releases the interface pool entries. The deletion operation is
requested through the device/interface Binary API; it is not a direct packet
path call.

### Synchronization and failure handling

Binary API handlers validate all participants before mutation. A failed request
leaves sibling registries and worker-visible tables unchanged. During the
dispatcher-owned `worker_thread_barrier_sync!` scope, the service FIB mutates
its long-lived source/entry/path graph and updates the concrete forwarding
backend incrementally. There is no complete-table build step and no handler-
owned barrier release.

Expected packet failures remain node-local errors and next-arc decisions.
Configuration, missing plugin APIs, invalid source/DPO relationships, stale
interface indices, and resource exhaustion return owner-local typed errors.
No packet path allocates an error or formats a message.

## ADR Review (2026-09-02)

Verdict: accepted. The ownership split, compact identity, cross-owner lifetime,
DSO loading, publication, and phase-one test seams now have enforceable owners
and interfaces.

| Finding | Evidence | Impact | Resolution |
| --- | --- | --- | --- |
| DPO identity width was underspecified | VPP `dpo_id_t` is `{type, proto, next, index}` and is asserted to fit in one `u64`; the current Hammer `Dpo<N>` is generic and is not that ABI shape | A generic next type can silently make atomic identity publication impossible and lets callers confuse an identity with an object | The target is one non-generic `DpoId` with `u8/u8/u16/u32` fields and an 8-byte size assertion; current `Dpo<N>` is a breaking migration item |
| Projection ownership was hidden behind a broad backend sentence | VPP FIB passes a complete DPO identity to the protocol backend, while concrete DPO lock/unlock and pool destruction remain class-local | A service FIB implementation could accidentally own or erase plugin objects, recreating the rejected object store abstraction | `project_forwarding` returns `DpoRef`; service stores that reference and passes only its `DpoId` to the concrete forwarding backend |
| DSO image loading was assigned to the wrong layer | Current symbol lookup is `GlobalMain -> PluginMain::get_plugin_symbol`; `hammer-runtime` cannot import service-owned types | `NetMain` cannot independently load a DSO without violating dependency direction | The init function resolves and validates the stable image through the existing `GlobalMain`/`PluginMain` path, then passes a borrow to `NetMain`/`InterfaceMain`; the runtime only keeps the DSO alive |
| The acceptance list mixed the whole target with one migration | The current checkout has no service FIB, split ICMP DSO, or Net proc-macro implementation, while the old `FibTableBuilder` and fixed device path remain | One issue would be unreviewable and would encourage speculative scaffolding | Phase 1 now gates only the split seams, concrete FIB/DPO identity behavior, Binary API ownership, and focused tests; later VPP surfaces are explicitly deferred |
| Cross-owner DPO retention had no callable path | VPP uses `dpo_vfts[type]` so a load-balance or stack can retain any child class; owner-local functions alone cannot retain an arbitrary foreign `DpoId` | A copied identity can outlive its pool object, or each plugin must know every foreign class | `DpoClassRegistration` supplies C-ABI lock/release callbacks; `DpoMain::lock/stack` return non-`Copy` `DpoRef`, whose `Drop` performs the only release path |

The DPO identity, DSO loading, and cross-owner lifetime findings are
current-project/VPP-backed conclusions. The phased delivery decision is a
project-management recommendation based on the current diff, not a VPP
requirement.

## Non-goals

- Do not split IPv4 and IPv6 into separate DSOs.
- Do not add a unified protocol-family registration abstraction, a generic
  registration hook, an erased network object, or dynamic object dispatch
  beyond the accepted class-indexed DPO lifecycle callbacks.
- Do not merge `NetRegistrationImage` with `InterfaceRegistrationImage` or the
  runtime lifecycle registration image.
- Do not make service choose a fixed IP next for every device.
- Do not publish routes by direct config mutation or an owner-specific publish
  handle; route and forwarding changes use the Binary API methods defined here.
- Do not change the existing Binary API envelope, context correlation, or
  handler function-pointer ABI. Adding the IP route method payloads is in scope.
- Do not put PMTU policy or a PMTU DPO in generic service net. The IP-owned
  PMTU move is in scope; its existing cross-worker publication behavior is
  audited without adding a new synchronization protocol.

## Consequences

The IP plugin becomes smaller in ownership scope while retaining concrete
IPv4/IPv6 implementations. ICMP has an independent DSO, Main, graph image,
and lifecycle, but its dependency on IP is explicit and VPP-shaped. FIB/DPO
logic becomes reusable by non-IP forwarding plugins without moving IP-specific
prefix data into service net. Runtime route publication now has one observable
control-plane contract: a Binary API request and typed reply. The dispatcher
owns the barrier scope, so plugin handlers remain focused on decode, domain
validation, and owner calls.

The migration is intentionally breaking. All in-tree callers move in one
change; no compatibility facade preserves the old IP DSO exports, fixed device
next enum, `thread_local!` local registration, atomic FIB handle, or
`[network].route` route-publication path.

## 变更清单

### 新增

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型 | `hammer-plugins/icmp::IcmpMain` | ICMP owner-local Main，保存 ICMP 控制状态 | 新 DSO；无旧 ABI 兼容层 | DSO lifecycle and global access integration test |
| 类型 | `hammer-plugins/ip::{Ip4Main,Ip6Main}` | 两个 concrete implementation Main；分别保存 lookup-main、unicast FIB、MFIB owner、table map、per-interface bind 和本地 feature/local-next 状态；本 ADR 只规定 unicast FIB | 从当前混合 `IpMain` 控制面拆出；不拆为两个 DSO | owner lifecycle and per-proto lookup tests |
| 类型 | `hammer-service::net::NetRegistrationImage` | `#[repr(C)]` + `StableAbi`；使用 `RSlice` 指向稳定注册项；仅含 `fib_sources` 和 `dpo_classes` 两个静态声明面 | 与 `InterfaceRegistrationImage` 分离；DSO 只传稳定值；启动期消费 | ABI compile check, DSO symbol lookup, startup-order test |
| 类型 | `hammer-service::net::{FibSourceRegistration,DpoClassRegistration,DpoNodeRegistration,DpoLockStatus}` | FIB source precedence；DPO class name、稳定的 `(DpoProto, node_name)` 绑定和 C-ABI `lock(u32)`/`release(u32)` lifecycle callbacks；不含 plugin type、对象或 pool reference | 运行时 source/class id 由 owner 分配；注册项 layout 变更为 stable types | macro expansion, lifecycle callback, and image installation test |
| 类型 | `hammer-service::net::dpo::DpoClassRecord` | 运行时保存 `DpoType` class slot、per-proto node slots 与 lock/release callbacks；不保存对象、pool 或派生 edges | 新增 service registry record；仅运行时状态，无序列化迁移 | registration, cross-owner lock, and owner-pool binding test |
| 类型/API | `hammer-service::net::dpo::DpoRef`、`DpoMain::lock`、`DpoMain::stack` | `DpoRef` 是 non-`Copy`/non-`Clone` 的单一 owning reference；`lock` 按 class 分派引用获取，`stack` 同时派生 edge 并获取 parent；`DpoRef::id()` 返回 8-byte identity，`Drop` 调用 private release callback | 新增 service-owned lifecycle interface；不提供 manual unlock、object store、lease hierarchy 或 plugin object access | cross-owner retain/release, stack, invalid class/index, and final-drop reclamation tests |
| 类型 | `hammer-service::net::dpo::{DropDpo,PuntDpo,LookupDpo,ReceiveDpo}` | 具体 service-owned DPO classes；Drop/Punt 是 per-protocol singleton，Lookup/Receive 是真实 pool objects，并各自实现 class operations | 新增 concrete class/pool surfaces；不通过统一 `DpoKind` 兼容 | object allocation, owner-drop, and graph-next tests |
| 类型 | `hammer-plugins/ip::forwarding::{AdjacencyDpo,LoadBalanceDpo}` | IP-owned concrete DPO objects/pools/operations；`AdjacencyDpo` 同时提供 normal/incomplete/midchain/glean/mcast class keys；通过 owner-local class conversion 投影到 service FIB `DpoId` | 替代通用 `Adjacency`/`LoadBalance` 构造面；不新增 `MidchainDpo` pool；无 service ownership | IP DPO pool, subtype, and conversion integration test |
| 类型 | `hammer-service::net::FibTableBackend` | 唯一的静态 dispatch seam：控制面 prefix 到 `fib_entry` index、packet address 到 entry `dpoi_index: u32`、cover replacement，以及 owner-specific projection；`project_forwarding` 返回已由 `DpoMain` 获取的 `DpoRef`，trait 不接收 plugin pool 或 erased object operation | `Ip4FibBackend`/`Ip6FibBackend` 各自实现；无 `dyn`、额外 forwarding adapter 或 family facade | compile-time implementation and projection failure-atomicity checks |
| 类型 | `hammer-plugins/ip::{Ip4RewriteNode,Ip6RewriteNode}` | 具体 IP4/IP6 adjacency rewrite graph node；消费 IP-owned `AdjacencyDpo`，分别处理 checksum/TTL/MTU | 替代单一 `AdjacencyRewriteNode`；同一 IP DSO | graph execution and per-proto rewrite tests |
| 类型 | `hammer-plugins/ip` 私有 `RewriteProtocol`/`process_rewrite_frame` 核心 | 静态泛型共享 adjacency 读取、MTU、TX metadata、错误分类、trace、next 编排；`Ip4Rewrite`/`Ip6Rewrite` 提供具体策略 | 不进入 service 或 DPO ABI；两个 node adapter 共享一套实现 | compile-time generic dispatch and behavior parity tests |
| 类型 | `hammer-plugins/ip::{IpPmtu,IpPmtuDpo}` | VPP `ip_pmtu_t` FIB tracker 与 `ip_pmtu_dpo_t` 实际对象池；DPO interpose 保留 parent identity，具体值由 owner 的 Rust 生命周期管理并提供 MTU/uRPF 操作 | 从 `hammer-service::net::pmtu::PathMtuCache` 迁移到 IP owner；ICMP/TCP 改用 typed IP seam | PMTU DPO, adjacency MTU and FIB back-walk tests |
| 类型 | `hammer-plugins/ip::{Ip4RouteAddDelRequest,Ip6RouteAddDelRequest,Ip4RouteLookupRequest,Ip6RouteLookupRequest}` | Binary API 的具体 IPv4/IPv6 请求；包含 `is_add`/`is_multipath`、外部 `table_id`、具体 prefix/address 和 paths/exact；route add/delete 的 source 由 handler 固定为 `FIB_SOURCE_API`，不在 wire payload 暴露 | 新 Binary API payload；不引入统一 family 枚举 | prost encode/decode and invalid-field tests |
| 类型 | `hammer-plugins/ip::{Ip4RouteAddDelReply,Ip6RouteAddDelReply,Ip4RouteLookupReply,Ip6RouteLookupReply}` | 返回 owner-defined status、stats 和匹配 route/path facts；不序列化 selected DPO；状态不复用 display string | 新 payload；envelope status 仍由 `hammer-ipc` 定义 | reply status and context tests |
| 类型 | `hammer-plugins/ip::{Ip4RoutePath,Ip6RoutePath,IpRoutePathType,IpRouteStatus}` | 具体地址宽度、VPP path 行为和可行动的 route 状态码 | 新 Binary API nested messages/enums；无旧 wire 兼容层 | prost schema and status mapping tests |
| API | `#[derive(FibSource)]` / `#[derive(DpoClass)]` | 生成具体静态 registration item | 替代手写 forwarding registration arrays | proc-macro compile and DSO inventory test |
| API | concrete DPO class lifecycle callbacks | 对齐 VPP 的引用获取/释放点；owner 的 C-ABI `lock(index)` 校验并增加对象/依赖引用，`release(index)` 仅由 `DpoRef::drop` 分派；singleton 走 typed no-op；两者都不是 mutex | 每个 concrete DPO class 注册同一 service-owned lifecycle interface；不提供 manual unlock、object store 或 plugin-to-plugin dependency | owner refcount, cross-owner retention, pool-reclamation, and Drop tests |
| API | fixed `net_registration_image` C ABI symbol | service consumer uses the concrete `unsafe extern "C" fn() -> RRef<'static, NetRegistrationImage>` signature directly；不新增函数指针 alias | 取代 Rust ABI/裸 `&'static` 返回；仅在 `net_main_init` 前接受静态声明 | real `dlopen`/symbol lookup and ABI type check |
| API | `ip4.route.add_del` / `ip6.route.add_del` | Binary API handler；按 VPP add/delete + multipath 语义增量修改 FIB graph 和 forwarding backend | 新方法名；客户端迁移到具体方法 | end-to-end Binary API route mutation test |
| API | `ip4.route.lookup` / `ip6.route.lookup` | Binary API handler；按 `table_id` 和 `exact` 读取 non-forwarding FIB entry，返回匹配 route/path/stats facts；不返回 selected DPO | 新方法名；客户端迁移到具体方法 | LPM/exact and route-facts reply test |
| API | `ip4.table.*` / `ip6.table.*` | table add/del/allocate/flush；维护 external `table_id` 到 concrete `fib_index` 的 owner-local map | 新 Binary API methods；不改变 Binary API envelope | table lifecycle and index-separation test |
| API | `ip4.interface.address.add_del` / `ip6.interface.address.add_del` | 通过 interface source 写入地址 pool，并生成 connected/attached/local/glean/broadcast/uRPF contributions | 新 Binary API methods；不允许直接写 lookup backend | address contribution and back-walk test |
| API | `ip4.interface.table.bind` / `ip6.interface.table.bind` | 绑定 `sw_if_index` 到 concrete table；触发 table-bind callbacks | 新 Binary API methods；`sw_if_index` 与 `fib_index` 保持分离 | bind callback and lookup-table test |
| API | `ip4.neighbor.add_del` / `ip6.neighbor.add_del` | 由 IP-owned ARP/ND adapter 更新 adjacency pool 和 `FIB_SOURCE_ADJ` contribution | 新 Binary API methods；未解析邻居使用 incomplete adjacency class | neighbor-to-adjacency projection test |
| API | `ip4.table.flow_hash.set` / `ip6.table.flow_hash.set` | 设置具体表的 flow-hash policy；只在 LPM 选定 load-balance 后执行 | 新 Binary API methods；hash fields 不进入 service FIB | post-LPM hash-selection test |
| API | `ip4.path_mtu.*` / `ip6.path_mtu.*` | IP-owned PMTU update/get/replace operations；必要时 interpose `IpPmtuDpo` | 从 service PMTU cache 迁移；ICMP/TCP 使用 typed IP seam | PMTU control/data path test |
| API | `ip4.feature.*` / `ip6.feature.*` | per-interface feature arc enable/disable；只操作具体 IP arc | 新 Binary API methods；service 只提供通用 arc ordering | feature-arc ordering test |
| API | `InterfaceMain::set_input_node` | 安装 device-owned RX input node | 替代 service 固定输入节点 | interface/device integration test |
| API | `InterfaceMain::rx_redirect_to_node` | 通过选中 `DeviceClass` callback 安装 per-interface next | 删除固定 Device-to-IP redirect | callback dispatch test |
| API | `register_ip4_protocol` / `register_ip6_protocol` | 分离 IP4/IP6 local protocol 注册 | 替代单一 `register_protocol` | ICMP local dispatch test |
| API | `register_ip4_icmp_error_node` / `register_ip6_icmp_error_node` | 安装 ICMP error output next | 未安装时使用 drop next | rewrite/error-next integration test |

### 修改

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型 | `hammer-service::net::DpoProto`, `DpoType`, `DpoId`, `DpoStackRegistry` | 从 IP plugin 移入 service net；`DpoType/DpoProto` 使用 packed `u8` 值，`DpoId` 是非泛型 `#[repr(C)]` `{ type: u8, proto: u8, next: u16, index: u32 }`、固定 8 bytes、`Copy`/`Clone` 的 non-owning identity；`DpoStackRegistry` 成为 `DpoMain` 内部 edge state；移除对 `IpVersion` 的转换依赖 | 所有 IP callers 更新 import；源码级 breaking change | workspace compile, size assertion, and DPO behavior tests |
| 类型 | `hammer-plugins/ip::{Adjacency,AdjacencyRewrite,LoadBalance}` | 改名为 `AdjacencyDpo`/`LoadBalanceDpo` concrete object pools；Adjacency 的 midchain/glean/mcast 是共享 pool 上的 class subtypes，不是新对象类型；generic FIB/DPO service 只接收 `DpoId`/forwarding facts | 删除旧通用构造面；IP lookup/rewrite 改用 concrete owner pool | IP DPO pool, subtype, and generic FIB projection integration test |
| 类型/API | `hammer-plugins/ip::forwarding::DpoKind` 与通用 `Dpo` 构造入口 | 删除泛化 kind-to-index/通用构造路径；具体 DPO 通过 `From` 生成 `DpoId` | breaking source migration；不保留 generic alias | compile-time API removal and concrete conversion tests |
| 类型 | `ForwardingMetadata` | 移入 service net，仅保留 DPO/FIB facts 与 TX interface contract；不携带 adjacency/load-balance对象 | IP lookup/rewrite 改用 service metadata | lookup/rewrite integration test |
| 类型 | `hammer-plugins/ip::AdjacencyRewriteNode` | 拆为 `Ip4RewriteNode`/`Ip6RewriteNode`；DPO class 只保存 service metadata，节点通过私有泛型核心共享编排并保留 IP-specific policy | 类型迁移为删除旧项 + 新增两项；不保留旧 node alias | per-proto graph inventory and behavior test |
| 类型 | `FibSource`, `FibEntry`, `FibPathList`, `FibPath`, `FibTable` | 从 IP plugin 的快照 builder 改为 service-owned 增量 source/entry/path graph；`FibTable` 静态组合唯一的 `FibTableBackend` seam，entry 保存当前 forwarding-chain `DpoRef`，backend 使用其 `DpoId` 更新 concrete forwarding store | IP4/IP6 backend 分别实现；无 family facade 或独立 forwarding adapter；现有 route snapshot callers 迁移为 source/path operations | LPM, source precedence, path-list sharing, cover back-walk, DPO projection tests |
| 类型 | `IpMain` | 改为 `OnceLock<IpMain>` owner；移除 FIB contributions、table handle 和 graph publication；只持有 generic IP protocol 与 TCP/UDP port registries | 删除 `ArcSwapOption<IpMain>`；调用方迁移到 `init/global`，并按 `Ip4Main`/`Ip6Main` 访问 concrete table | initialization and owner-separation test |
| 类型 | `hammer-service::binary_api::BinaryApiMethodEntry` dispatch contract | FIB-touching methods 保持 `mp_safe = false`；由 dispatcher 统一进入宏，handler 不再承担同步职责 | 现有 envelope/ABI 不变；方法注册迁移 | mp-safe/non-mp-safe dispatch test |
| 类型 | `NetMain` / `DeviceMain` | 改为按值 `OnceLock<Main>`；NetMain 负责 Net registration image 和 local0 | 删除 `OnceLock<Arc<_>>` | duplicate-init and ownership compile checks |
| 类型 | `hammer-service::interface::InterfaceMain` | 由 `NetMain` 按值嵌入并通过借用暴露；不再作为独立 `Arc<InterfaceMain>` ownership root | `NetMain::init`/调用方迁移为 `&InterfaceMain`；接口 pool 和 callback 语义不变 | net/interface lifecycle and borrow-ownership checks |
| 类型/API | IP graph registration image | IP image 删除 ICMP nodes；ICMP image 单独声明自己的 nodes/Main | `load_after = ["ip"]`；所有 DSO 一次迁移 | DSO graph inventory and load-order test |
| API | `IpMain` FIB publication | 删除直接 `publish(table)`；改由 `ip4.route.add_del`/`ip6.route.add_del` Binary API handler 调用 service FIB 增量操作 | 运行时客户端改用 Binary API；dispatcher 宏负责 barrier | Binary API mutation and worker visibility test |
| API | `hammer-service::net::pmtu` | 删除 service-owned PMTU cache；ICMP byte parser和 PMTU policy 均移至 IP/ICMP owners | TCP caller 改为显式依赖 IP 的 typed PMTU read/update | PMTU owner and cross-plugin contract test |
| API | `NetworkOpaque.sw_if_index` | 明确 `[0]` 为 RX、`[1]` 为 TX；不承载 `fib_index` | device/IP/interface callers audited | RX/TX metadata path test |
| 类型/API | `hammer-service::opaque::NetworkIpOpaque` | 增加独立 `fib_index: u32` 与可选 `fib_index_override` publication facts；不得复用 `sw_if_index[TX]` | packet opaque layout changes inside the existing fixed opaque budget; no wire migration | input lookup metadata and index-separation test |
| API | Device class callback set | 增加 concrete `rx_redirect_to_node` callback，保留 owner validation | device plugins provide callback; no generic trait | callback order and redirect test |

### 删除

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型/API | `hammer-plugins/ip::ip::icmp::*` 的 IP-owned ICMP module surface | ICMP nodes、control state、ICMP exports 移至 `hammer-plugins/icmp` | 无 compatibility re-export；ICMP callers 改依赖新 DSO | workspace compile and DSO export test |
| 类型/API | `hammer-service::device::DeviceInputNext::{Ip4Input,Ip6Input}` | 删除 service 固定 IP next | device callback 选择具体 parser/next | graph inventory and RX integration test |
| 类型/API | service-owned `DeviceInputNode` fixed protocol path | 删除固定 service input implementation；由 device plugin 提供 RX node | TUN/device plugins migrate to own node | device DSO graph test |
| 类型/API | `hammer-plugins/ip::{AdjacencyRewriteNext,register_adjacency_rewrite}` | 删除单一 adjacency rewrite 节点及其注册入口 | 迁移为 IP4/IP6 concrete node registration；DPO metadata 由 `NetMain` 消费 | graph registration compile/inventory test |
| 类型/API | `hammer-service::net::Dpo` generic struct/constructors | 删除把所有 DPO classes 统一塞进一个通用构造器的表面；保留 concrete owner 的 object creation、subtype selection 和 `(DpoType, index)` 到 `DpoId` projection | callers migrate to concrete DPO class pools and singleton APIs; no `MidchainDpo` pool | compile-time owner projection and pool-lifetime test |
| API | `register_protocol(protocol, node)` | 删除含糊的单表注册入口 | callers use explicit IP4/IP6 functions | compile-time API migration check |
| 全局/API | `IP_MAIN: ArcSwapOption<IpMain>` | 删除 atomic pointer IP Main global | 通过 `IpMain::init/global` 访问 | global lifecycle integration test |
| 类型/API | `NetworkIpConfig::route`、`Route`、`Via` | 删除通过 TOML 直接创建/替换 FIB 的配置模型 | 迁移为 Binary API route messages；保留 reassembly 配置 | config rejection and Binary API bootstrap test |
| API | `IpLookupControlPlane::publish` | 删除 handler 外的直接表发布入口及其 `barrier::global()/WorkerBarrier::sync` 调用 | 所有调用方迁移到 Binary API dispatcher + service FIB mutation | compile-time removal and end-to-end publication test |
| 类型/API | `FibTableHandle` 的 `Arc<UnsafeCell<FibTable>>` publication | 删除第二套 pointer/lock publication model | node runtime uses owner-published table under barrier | barrier publication and race audit |
| 类型/API | `hammer-service::net::pmtu::{PathMtuCache,PATH_MTU_CACHE,publish_path_mtu_cache,path_mtu_cache}` | 删除 service-owned PMTU cache/global；PMTU tracker/DPO 改由 IP forwarding owner 持有 | TCP/ICMP callers 迁移到 IP-owned typed operations | PMTU lifecycle and cross-plugin API test |
| 类型/API | `hammer-service::net::FibLookupResult` | 删除把控制面 `fib_entry` 查找、数据面 load-balance 查找、bucket 选择和 child DPO 混成一个返回值的类型 | callers 分别使用 `fib_entry` lookup、forwarding lookup 和 IP-owned bucket selection | compile-time API removal and control/data-plane lookup tests |
| 类型/API | `hammer-plugins::ip::forwarding::FibTableBuilder` | 删除整表快照 builder；路由通过 source/path 增量操作发布 | startup route callers 迁移为 Binary API source/path mutations | incremental update and failure-atomicity test |
| API | `hammer-service::net::pmtu::apply_ipv4_frag_needed_icmp` | 删除 service 内 ICMP byte parser | parser moves to ICMP plugin; ICMP calls typed IP PMTU operation | ownership and behavior tests |
| API | `hammer-service::net::pmtu::process_ipv4_icmp_path_mtu_packet` | 删除 service 内完整 ICMP packet processing | ICMP node performs parse and calls typed IP PMTU update | ICMP DSO integration test |
| 类型/API | 任何统一协议族抽象、泛化 registration hook、erased/dynamic registration carrier | 明确不支持这些抽象 | 无迁移层；设计和实现 review 直接拒绝 | compile/API review, not source-text behavior test |

Binary API envelope、context correlation、socket framing 和 handler
function-pointer ABI 不变；本 ADR 新增 IP plugin-owned protobuf payloads
和方法名。`[network].route` 配置模型删除，现有启动脚本/客户端迁移为
`ip4.route.add_del` 或 `ip6.route.add_del` 请求。这仍是一次源码级
breaking change，所有 in-tree plugins 和 callers 在同一变更中迁移。

## Verification and acceptance

The design is accepted when the ownership and identity invariants below are
unambiguous. The implementation issue derived from this ADR is intentionally
phased; its first phase is the only completion gate for the current migration.

### Phase 1 implementation gate

1. IP and ICMP are separate DSOs, with ICMP loading after IP and registering
   IPv4/IPv6 local-next and ICMP-error consumers through explicit IP APIs.
2. Device RX remains device-owned: a concrete device callback selects the next
   node, while RX/TX `sw_if_index` and the independent `fib_index` survive the
   IP path.
3. The service FIB owns source/path/entry precedence and back-walk state;
   concrete IPv4/IPv6 owners provide LPM storage and DPO projection through
   static dispatch. No `IpFamily`, erased state, or generic DPO object store is
   introduced.
4. `DpoId` is a non-generic eight-byte `Copy + Clone` identity. Concrete
   pool-backed DPO owners register class lock/release callbacks;
   `DpoMain::lock/stack` return non-`Copy` `DpoRef`, and only its `Drop`
   releases ownership after barrier-safe replacement/removal. Service code
   never dereferences `DpoId.index`.
5. Route mutation is exposed through the existing Binary API dispatcher; the
   dispatcher owns the sole `worker_thread_barrier_sync!` scope and handlers do
   not acquire or release a barrier.
6. Focused tests cover VPP-equivalent source precedence, IPv4 cover
   restoration, IPv4/IPv6 LPM, post-LPM bucket selection, DPO
   stacking/identity/lifetime, concrete class/index validation, final-release
   reclamation, and failure-atomic mutation. Each behavior is proved once at
   the highest owning interface; tests do not repeat constructor coverage or
   use source-text assertions.
7. One real plugin-load test proves that the IP and ICMP DSOs load in dependency
   order and that ICMP registration reaches the concrete IP local/error tables.

### Later gates

Table/address/neighbor/feature/PMTU APIs, MFIB, and additional DPO classes
remain part of the target architecture and are tracked as follow-up phases.
They are not silently required to land in the first implementation slice.

## 未决问题

These are implementation verification items, not permission to reintroduce a
different architecture:

- The DPO lifecycle shape is settled as C-ABI `lock(u32) -> DpoLockStatus` and
  `release(u32)` callbacks, with release callable only from `DpoRef::drop`.
  Implementation must prove the exact `StableAbi` function-pointer expansion.
  The device redirect signature is settled as `(InterfaceMain, hw_if_index,
  NodeId)` by the current interface owner and VPP's
  `(vnet_main_t, hw_if_index, node_index)` callback.
- The fixed export remains `net_registration_image`; the proc-macro attributes
  remain `fib_source` and `dpo_class`. Implementation must verify their
  `StableAbi` expansion against the existing loader convention.
- PMTU cache publication from an ICMP data worker must be reconciled with the
  existing synchronization contract before moving the parser; no new lock is
  allowed as a shortcut.
- The concrete `FibTable<P, B>` and `FibTableBackend` methods must be proven
  against the existing `Ip4Mtrie` and `Ip6Fib` APIs; the concrete DPO owners
  must also prove that every owning insertion receives `DpoRef`, that pool
  removal cannot occur while any such reference remains, and that the Rust
  `DpoId: Copy + Clone` contract is preserved. This does not authorize a
  unified protocol-family abstraction or object dispatch beyond lifecycle.
- The final prost tag numbers and whether the existing client needs a bounded
  multi-reply extension for route dumps must be fixed before implementation.
  The four method names, field meanings, status categories, and barrier rule
  are settled here; wire polish is not a second publication mechanism.
- Whether startup defaults are sent by `hammerctl` or another existing control
  client is an operational choice; whichever client is selected must submit
  the same Binary API requests after initialization.
- The exact `StableAbi` derivation details for all nested registration metadata
  (including the final `DpoProto` representation) must be checked by the proc
  macro compile tests. This is an implementation check, not permission to
  replace the stable image with native Rust slices.

## 依据与假设

### 当前项目事实

- `crates/hammer-plugins/ip/src/lib.rs` currently registers IP and ICMP graph
  nodes in one DSO.
- `crates/hammer-plugins/ip/src/forwarding/` currently owns DPO, FIB,
  load-balance, adjacency, metadata, and table publication types.
- `crates/hammer-plugins/ip/src/forwarding/dpo.rs` currently defines
  `DpoType(u16)`, the generic `Dpo<N>`/`DpoId` envelope with `Copy`/`Clone`,
  `DpoKind`, and direct constructors that do not bind a class key to an object
  pool.
- `crates/hammer-plugins/ip/src/ip/local.rs` currently uses `thread_local!` for
  local protocol registration state.
- `crates/hammer-service/src/device/mod.rs` currently contains fixed
  `DeviceInputNext` IP nexts.
- `crates/hammer-service/src/interface_model.rs` stores input/output node
  identities and interface-owned queues.
- `crates/hammer-plugins/ip/src/ip/input.rs` reads RX `sw_if_index`, and
  `crates/hammer-plugins/ip/src/lookup/mod.rs` writes TX `sw_if_index`.
- `crates/hammer-service/src/net/mod.rs` creates `local0` via the generic
  interface registration path.
- `crates/hammer-service/src/binary_api.rs` already resolves a registered
  method and enters `worker_thread_barrier_sync!` for non-`mp_safe` methods;
  `crates/hammer-runtime/src/lib.rs` exports that macro.
- `crates/hammer-service/src/net/pmtu.rs` currently owns the `PathMtuCache`
  and also parses IPv4 ICMP Fragmentation Needed bytes; this is the current
  mixed ownership that the decision removes.
- `crates/hammer-plugins/ip/src/lookup/mod.rs::IpLookupControlPlane::publish`
  still calls `barrier::global()/WorkerBarrier::sync` directly and uses an
  `Arc<UnsafeCell<_>>` handle; both are the publication surfaces being removed.
- The current branch has no Rust implementation or test changes for this ADR;
  target-interface tests are part of the implementation issue.

### 已确认设计

- ICMP is an independent plugin but may explicitly depend on IP.
- IPv4 and IPv6 remain concrete implementations in one IP plugin.
- DPO class/protocol selects forwarding dispatch; IP local protocol mapping is
  a separate owner API.
- Device RX is not fixed by service to IP; device plugins own RX nodes and
  callbacks.
- `local0` is a reserved sentinel; `sw_if_index` and `fib_index` are distinct.
- Main values use owner-local `OnceLock<Main>` with `init` and `global`.
- Runtime route publication is through IP plugin-owned Binary API methods;
  the dispatcher owns the only barrier scope and plugin handlers never call a
  barrier entrypoint.
- `Ip4RewriteNode` and `Ip6RewriteNode` are both accepted graph nodes; their
  shared packet orchestration is a private, statically dispatched generic core
  inside the IP plugin, with concrete IPv4/IPv6 policies.
- Ordinary route paths are decoded to service `FibPath` facts first; the IP
  owner resolves contributing paths into concrete DPO objects and only then
  projects their `(DpoType, index)` to service `DpoId`. Direct `DpoId` input is
  limited to owner-internal special-DPO operations. The generic
  `DpoKind`/`Dpo` construction surface is removed.
- DPO lifetime uses the service-owned class dispatch selected by `DpoType`:
  concrete owners register lock/release callbacks, owning slots store
  non-`Copy` `DpoRef`, and only `DpoRef::drop` releases the reference. `DpoId`
  remains a copyable, non-owning value and service never stores object bytes.
- Generic registration hooks, erased object dispatch, dynamic production
  state, and unified protocol-family abstractions are out of scope. The only
  cross-owner function dispatch is the accepted DPO lifecycle pair.

### 设计假设

- `NetRegistrationImage` remains separate from the existing
  `InterfaceRegistrationImage`; both are consumed by their respective service
  owners during startup.
- `NetRegistrationImage` is `StableAbi` and uses `RSlice`/`RStr`-style stable
  fields; a service-owned `extern "C"` export returns it through `RRef`. The
  existing runtime `PluginModule` is not expanded with service types because
  `hammer-runtime` must not depend on `hammer-service`.
- Protocol-neutral FIB source/entry/path/back-walk machinery can be made
  generic over the existing concrete IPv4/IPv6 lookup backends without moving
  their prefix storage into service net. The single backend seam returns
  `DpoRef`; the service FIB owns that reference while the backend stores only
  the projected `DpoId` facts. No separate projector or erased state is
  required.
- TCP continues to consume a typed IP-owned Path MTU operation after the ICMP
  parser moves to the ICMP plugin; TCP already has an explicit IP plugin
  dependency in the current Cargo graph.
- The existing one-reply Binary API envelope is sufficient for route mutation
  and lookup. Multipart route dump replies are deferred until a separate
  envelope decision; this ADR does not invent a second streaming transport.

### Vendored VPP 依据

- FIB20 control-plane model: <https://fd.io/docs/vpp/v2101/gettingstarted/developers/fib20/controlplane>
- FIB20 data model, routes, graph walks, and dataplane pages linked from that
  control-plane document.
- `third_party/vpp/src/vnet/ip/ip.api` (`ip_route_add_del`,
  `ip_route_lookup`, and route dump contracts).
- `third_party/vpp/src/vnet/fib/fib_api.h` (`fib_api_route_add_del` and path
  decoding boundary).
- `third_party/vpp/src/vnet/fib/fib_entry_src.c` (`fib_entry_src_action_install`
  and source winner replacement).
- `third_party/vpp/src/vnet/fib/fib_table.c`
  (`fib_table_fwding_dpo_update/remove` pass the complete DPO identity to the
  protocol backend; IPv4 removal also passes the less-specific cover).
- `third_party/vpp/src/vnet/fib/ip4_fib_8.c` (the IPv4 MTRIE stores
  `dpoi_index` and restores the cover's index on removal) and
  `third_party/vpp/src/vnet/fib/ip6_fib.c` (the shared forwarding hash keys on
  masked address, bound `fib_index`, and prefix length, with shared
  prefix-length refcounts/bitmap).
- `third_party/vpp/src/vnet/fib/fib_walk.c` (`fib_walk_sync/async` back-walk).
- `third_party/vpp/src/vnet/dpo/dpo.h` and `dpo.c` (`dpo_proto_t`,
  `dpo_type_t`, `dpo_id_t`, `dpo_vft_t`, `dpo_register`, `dpo_register_new_type`,
  `dpo_vfts`, `dpo_nodes`, `dpo_edges`, `dpo_lock`, atomic identity replacement,
  and derived edge selection; `dpo_lock` is reference-count acquisition for a
  copyable identity, not a mutex; the protocol is a data-path graph
  discriminator, distinct from local IP protocol numbers).
- `third_party/vpp/src/vnet/dpo/dpo.c:dpo_set` and `dpo_copy` (publish or
  atomically copy the new identity, acquire its class reference, then account
  for the previous identity; this is why the new identity must be locked before
  the old one is discarded).
- `third_party/vpp/src/vnet/dpo/load_balance.c:887-941`,
  `lookup_dpo.c:222-258`, and `receive_dpo.c:77-96` (pool-backed DPO lock
  counts and zero-count destruction; configured lookup DPOs also hold a FIB or
  MFIB table source).
- `third_party/vpp/src/vnet/adj/adj.c:334-363` and the adjacency DPO VFTs
  (adjacency lock is a FIB-node reference; all adjacency class subtypes share
  the same pool and reference lifecycle).
- `third_party/vpp/src/vnet/adj/adj_nbr.c` (`adj_nbr_dpo_vft` and per-protocol
  `ip4-rewrite`/`ip6-rewrite` node bindings).
- `third_party/vpp/src/vnet/dpo/lookup_dpo.h` and `lookup_dpo.c` (one object
  pool with multiple dynamically registered subtype class keys).
- `third_party/vpp/src/vnet/dpo/receive_dpo.h` and `receive_dpo.c` (real
  receive object pool and class operations).
- `third_party/vpp/src/vnet/dpo/drop_dpo.c`, `punt_dpo.c`, and
  `ip_null_dpo.c` (stateless singleton/fixed-record classes).
- `third_party/vpp/src/vnet/dpo/load_balance.h` and `load_balance.c`
  (object pool, bucket DPO IDs, sibling lookup node, and pool barrier growth).
- `third_party/vpp/src/vnet/ip/ip4.h`, `ip6.h`, and `lookup.h`
- `third_party/vpp/src/vnet/interface.h` and `interface_funcs.h`
- `third_party/vpp/src/vnet/interface/rx_queue.c`
- `third_party/vpp/src/vnet/devices/virtio/virtio.c` and `device.c`
- `third_party/vpp/src/vnet/misc.c` and `interface.c`

### 仍需历史记录确认

- The intended owner and synchronization contract for the existing PMTU cache
  when an ICMP data node reports an update.
