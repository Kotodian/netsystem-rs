# VPP-Style IP, ICMP, FIB, DPO, and Device Data-Path Ownership

Status: accepted

Date: 2026-09-02

This ADR defines the breaking source-level migration for the IP forwarding
stack. It is a design record, not an implementation claim. The implementation
issue derived from it owns the Rust migration and focused behavior tests. This
ADR change contains no Rust implementation or test code.

## Context

The current checkout has one `crates/hammer-plugins/ip` DSO that registers IP and
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

The target plugin subtree is `hammer-plugins/net/{ip,icmp}`:

```text
hammer-plugins/
  net/
    ip/
    icmp/
```

`hammer-plugins/net/ip` and `hammer-plugins/net/icmp` are separate DSOs. The
IP DSO contains the concrete IPv4/IPv6 implementations; the ICMP DSO owns ICMP
nodes and explicitly loads after IP. The existing `crates/hammer-plugins/ip`
tree is the source being migrated, not the target layout.

### Ownership and dependency direction

The dependency direction is:

```text
hammer-plugins/net/icmp -> hammer-plugins/net/ip -> hammer-service::net
hammer-plugins/net/ip -----------------------> hammer-service::net
hammer-plugins/transport/tcp,udp ---------> hammer-plugins/net/ip
hammer-plugins/device/* ------------------> hammer-service
hammer-service ----------------------------> hammer-runtime
```

`hammer-service` never depends on the IP or ICMP plugin. The service-owned DPO
surface contains class keys, graph metadata, compact identities, generic
layouts, and only the objects whose state is service-owned. Address-bearing objects are generic over
the producer's concrete address type; they never carry an IP plugin type,
pool reference, or IP policy into `hammer-runtime` or another plugin.

| Owner | Owns | Does not own |
| --- | --- | --- |
| `hammer-service::net` | `NetMain`, interface/device coordination, protocol-neutral FIB source/entry/path graph, DPO class/node/edge metadata, generic DPO layouts and built-in class rules, source precedence, back-walk, and the generic forwarding-update seam | concrete producer payloads and their pools, IP-specific parsing and policy, ICMP parsing, device queue polling, Binary API method ownership |
| `hammer-plugins/net/ip` | `IP_MAIN` protocol/port registry, concrete `IP4_MAIN`/`IP6_MAIN` owners for packet input/local lookup, FIB backends and per-interface table mappings, IP-null/special-route/PMTU policy, their concrete DPO pools, concrete next-hop decoding, and IP-owned Binary API handlers | ICMP nodes/Main, service-owned DPO pools, device queues, worker barrier, generic DPO class registry/stack implementation |
| `hammer-plugins/net/icmp` | `ICMP_MAIN`, ICMP graph nodes, ICMP type tables, ICMP packet parsing/generation, ICMP registration with IP | IP FIB tables, interface queues, device polling |
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
| `fib_main_t` and `fib_table_t` | source registry, entry/path-list graph, table-id/index mapping, back-walk and forwarding projection | `hammer-service::net::fib`; the concrete IP backend supplies prefix storage and path facts, while service DPO owns the forwarding objects |
| DPO registry (`dpo_vfts`, `dpo_nodes`, `dpo_edges`) | class keys, per-protocol node bindings, stack edges, and compact identity metadata | `hammer-service::net::dpo`; `DpoMain` owns the metadata directly, while every concrete object pool and dependency remains owner-local |
| concrete DPO pools | adjacency, load-balance, lookup, receive, interface RX, and other object bytes | the module that instantiates the concrete payload; service owns only pools whose payload is service-owned |

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
The service net DPO module owns the shared adjacency and load-balance layouts,
class rules, and built-in implementations used by those two tables. A concrete
`A`/`R` instantiation is stored by the class owner that supplies those types;
the IP plugin supplies only concrete prefix storage, next-hop interpretation,
and the IPv4/IPv6 graph nodes that consume the DPO. IP does not own generic
adjacency/load-balance policy or a second per-family pool, and no service DPO
object contains IP address semantics or IP policy.

The new owner fields are limited to the state each VPP main actually owns:

```rust
pub struct Ip4Main {
    local_next_by_ip_protocol: [Option<NodeId>; 256],
    unicast_tables: Vec<FibTable<Ipv4Net, Ip4FibBackend>>,
    table_id_to_fib_index: BTreeMap<u32, u32>,
    fib_index_by_sw_if_index: Vec<u32>,
    mfib_index_by_sw_if_index: Vec<u32>,
    feature_arc_indices: Vec<u16>,
    flow_hash_seed: u32,
    host_config: Ip4HostConfig,
}

pub struct Ip6Main {
    local_next_by_ip_protocol: [Option<NodeId>; 256],
    unicast_tables: Vec<FibTable<Ipv6Net, Ip6FibBackend>>,
    table_id_to_fib_index: BTreeMap<u32, u32>,
    fib_index_by_sw_if_index: Vec<u32>,
    mfib_index_by_sw_if_index: Vec<u32>,
    interface_route_adj_index_by_sw_if_index: BTreeMap<u32, u32>,
    feature_arc_indices: Vec<u16>,
    flow_hash_seed: u32,
    host_config: Ip6HostConfig,
    hbh_enabled: bool,
}

pub struct Ip4HostConfig {
    ttl: u8,
    tos: u8,
}

pub struct Ip6HostConfig {
    ttl: u8,
}

pub struct IcmpMain {
    icmp4_type_nodes: BTreeMap<u8, NodeId>,
    icmp6_type_nodes: BTreeMap<u8, NodeId>,
    ip4_error_node: Option<NodeId>,
    ip6_error_node: Option<NodeId>,
}
```

`Ip4Main` and `Ip6Main` retain the per-interface MFIB index mappings because
those mappings are part of the VPP input contract. Their concrete address and
prefix pools remain inside each protocol's lookup owner; this ADR does not
pretend that one Rust address layout serves both implementations. MFIB
table/object fields remain IP-owner state and are not smuggled into the service
unicast table.

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

### DPO class and object inventory

The migration follows VPP's distinction between a DPO type graph and a DPO
instance graph. The first graph is keyed by `(DpoType, DpoProto)` and contains
node/edge selection. The second graph is made of `DpoId` values whose
`(type, index)` pair identifies one concrete object. The following inventory is
the ownership map for the classes relevant to this design. Classes outside this
migration use the same seam when an owning plugin is introduced; no such class
is added by this ADR.

`DpoType` has one invalid value and otherwise contains a class key allocated by
`DpoMain`. It does not expose VPP's closed built-in enum as a Rust API. Core
classes are registered first and plugin classes are allocated afterwards from
the same monotonic range; a key is never reused during the process lifetime.
`DpoProto` is the compact data-path protocol key used to select a graph
node/edge. Its values belong to the DPO graph registration, not to an IP
family abstraction, next-hop selector, or IP local protocol number. The key
does not make the DPO registry depend on the IP plugin or embed a protocol
payload.

`DpoId` is an ordinary Rust `Copy + Clone` identity value wrapping one private
`u64`. Its methods expose the packed `type`, `proto`, `next`, and `index` facts;
it is not an ABI record and does not use `repr(C)`. Copies never inspect a pool
and never imply ownership. The invalid value remains
`(DpoType::INVALID, DpoProto::NONE, 0, u32::MAX)`.

The target representations are intentionally compact Rust values, not C
records or a closed protocol enum:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DpoType(u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DpoProto(u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DpoId(u64);

impl DpoType {
    pub const INVALID: Self = Self(u8::MAX);
}

impl DpoProto {
    pub const IP4: Self = Self(0);
    pub const IP6: Self = Self(1);
    pub const NONE: Self = Self(u8::MAX);
}
```

`DpoMain` owns the allocated class keys and graph metadata. The packed `DpoId`
accessors expose the four VPP identity fields without exposing a constructor
that can bypass class-owner validation. No MPLS, LISP, BIER or other protocol
payload is embedded in these identities.

The fields behind the class metadata and the object-bearing classes are:

```rust
pub struct DpoMain {
    nodes: BTreeMap<(DpoType, DpoProto), NodeId>,
    edges: BTreeMap<(DpoType, DpoProto, DpoType, DpoProto), u16>,
    next_type: u8,
}

pub struct DropDpo {
    ids: Box<[DpoId]>,
}

pub struct PuntDpo {
    ids: Box<[DpoId]>,
}

pub struct LookupDpo {
    fib_index: u32,
    proto: DpoProto,
    input: LookupInput,
    table: LookupTable,
    cast: LookupCast,
}

pub enum LookupInput {
    SourceAddress,
    DestinationAddress,
}

pub enum LookupTable {
    FromInputInterface,
    Configured,
}

pub enum LookupCast {
    Unicast,
    Multicast,
}

pub struct ReceiveDpo<A> {
    sw_if_index: u32,
    address: A,
}

pub struct InterfaceRxDpo {
    sw_if_index: u32,
    next_node: NodeId,
    proto: DpoProto,
}

pub struct InterfaceTxDpo {
    sw_if_index: u32,
}

#[derive(DpoClass)]
#[dpo_class]
pub struct AdjacencyDpo<A, R> {
    egress_sw_if_index: Option<u32>,
    next_hop: Option<A>,
    rewrite: R,
    child: Option<DpoId>,
}

pub struct LoadBalanceDpo {
    bucket_count: u16,
    bucket_mask: u16,
    proto: DpoProto,
    flags: LoadBalanceFlags,
    fib_entry_flags: FibEntryFlags,
    lock_count: u32,
    map_index: u32,
    urpf_index: u32,
    hash_config: u16,
    overflow_bucket_index: u32,
    inline_buckets: [DpoId; 4],
}
```

The address slot is deliberately a type parameter, not a service trait. A
producer passes its own concrete value `A` (for example a protocol address,
link-layer address, or tunnel endpoint) and owns canonicalisation, equality
and wire interpretation before calling the generic DPO constructor. Net stores,
borrows, and moves `A`; it never parses it, converts it to bytes, tags it with
a family, or calls a fake `canonical` method. Bounds such as `Eq` or `Hash`
are placed only on the concrete pool operation that needs them, not on a
marker trait in the net interface.

Thus `ReceiveDpo<A>` and `AdjacencyDpo<A, R>` are statically distinct for every
producer's `A`/`R` pair. The class owner registers the monomorphized pool under
one `DpoType`; a copied `DpoId` remains only the class/index identity and never
erases which concrete pool owns its index. The `A` parameter is not the FIB
prefix or route API address type; those remain concrete associated types of an
IP backend and concrete fields of an IP-owned Binary API message.

Vendored VPP's `receive_dpo_t` and adjacency objects retain address-shaped
facts (`rd_addr` and the adjacency next-hop union). Their C representation uses
`ip46_address_t` for the IP implementations VPP ships; Hammer keeps the same
object ownership and class/index identity while making the address slot a
monomorphized Rust type instead of copying that C union into service net.

`LookupDpo` and `ReceiveDpo` are real pool objects, while `DropDpo` and
`PuntDpo` are per-registered-protocol singleton holders. Their owner allocates
one contiguous `Box<[DpoId]>` during graph initialization and indexes it only
with a validated `DpoProto`; there is no closed `DpoProto::NUM` array or
protocol-specific payload in service net. The adjacency subtype class keys
share the `AdjacencyDpo` pool; `DpoType` selects normal, incomplete, midchain,
glean, or multicast behavior. `DpoMain` stores only class/node/edge metadata;
the matching owner stores the pool and its typed operations.

The `DPO_*` labels in the following table are VPP class names used for the
correspondence only. They are not public Rust associated constants and are not
an exhaustive closed set of Hammer classes.

VPP's C implementation uses `dpo_lock`/`dpo_unlock` because a copyable
`dpo_id_t` can be published into several independent slots and the pool owner
must know when the last slot releases an object. Hammer keeps the copyable
identity but moves that accounting to each concrete class owner. A class owner
counts only long-lived published roots (FIB forwarding slots, DPO child slots,
interface slots and worker-visible projections); local temporary copies are
not roots. Replacing a root retains the new identity, publishes it, then drops
the old root in the same barrier transaction. When the owner count reaches zero
after worker quiescence, the concrete pool value and its child fields are
retired by ordinary Rust ownership.

This preserves the reason for VPP's lock without exposing a generic lock or
release API. `DpoMain` has no lifetime callback, `DpoId` has no hidden owner
pointer, and there is no `DpoRef`, `DpoObjectStore`, class record, or manual
`unlock` operation. Every root mutation is an owner-local operation that can
see the matching concrete pool and its dependencies.

`DpoMain::stack` mirrors VPP's `dpo_stack`: it receives the child class/proto
and a parent `DpoId`, derives the graph edge, and returns a copy of the parent
identity with that edge in `next`. It does not retain a parent object and does
not return a second ownership type. A concrete owner retains any parent or
child object it needs through its own typed state.

| VPP class | Object storage | Hammer owner in this design | `DpoId.index` |
| --- | --- | --- | --- |
| `DPO_DROP` | Per-protocol static singleton IDs; no object lifetime | `hammer-service::net` | Fixed selector; never dereferenced as a pool index |
| `DPO_PUNT` | Per-protocol static singleton IDs; no object lifetime | `hammer-service::net` | Fixed selector; never dereferenced as a pool index |
| `DPO_IP_NULL` | Fixed IP action records; no per-route object allocation | `hammer-plugins/net/ip` | Index into the IP owner's immutable action table |
| `DPO_LOAD_BALANCE` | `load_balance_t` pool; buckets contain child `dpo_id_t` values | `hammer-service::net::dpo` | Index into the service load-balance pool |
| `DPO_REPLICATE` | `replicate_t` pool; buckets contain child `dpo_id_t` values | Future multicast/replication plugin | Index into the replicate pool |
| `DPO_ADJACENCY` | Shared service adjacency pool; class-specific subtype registrations | `hammer-service::net::dpo` | Index into the service adjacency pool |
| `DPO_ADJACENCY_INCOMPLETE` | Same service adjacency pool; IP neighbor producer supplies unresolved facts | `hammer-service::net::dpo` | Index into the service adjacency pool |
| `DPO_ADJACENCY_MIDCHAIN` | Same service adjacency pool; tunnel/recursive producer supplies child facts | `hammer-service::net::dpo` | Index into the service adjacency pool |
| `DPO_ADJACENCY_GLEAN` | Same service adjacency pool; IP lookup producer supplies opaque prefix facts | `hammer-service::net::dpo` | Index into the service adjacency pool |
| `DPO_ADJACENCY_MCAST` / `DPO_ADJACENCY_MCAST_MIDCHAIN` | Same service adjacency pool; multicast producer supplies subtype facts | `hammer-service::net::dpo` | Index into the service adjacency pool |
| `DPO_RECEIVE` | `receive_dpo_t` pool | `hammer-service::net::dpo` | Index into the service DPO receive pool |
| `DPO_LOOKUP` plus lookup subtypes | One `lookup_dpo_t` pool; source/destination/multicast/interface-table subtypes share the object layout but receive separate class keys and fast-path node lists | `hammer-service::net::dpo` | Index into the service DPO lookup pool; `DpoId.type` selects the subtype node class |
| `DPO_CLASSIFY` | `classify_dpo_t` pool | Future classifier plugin | Index into the classify pool |
| `DPO_INTERFACE_RX` | Per-interface/protocol DPO pool and database | `hammer-service::net::interface` | Index into the interface-RX pool |
| `DPO_INTERFACE_TX` | No pool; wraps an existing `sw_if_index` | `hammer-service::net::interface` | The wrapped `sw_if_index` |
| `DPO_DVR`, `DPO_L3_PROXY` | Concrete pools | Owning bridge/IP plugin when introduced | Index into the owning pool |
| `DPO_MFIB_ENTRY` | Concrete multicast-FIB entry object | multicast IP owner when introduced | Index into the MFIB-entry pool |
| `DPO_IP6_LL` | One permanent link-local singleton DPO ID | IPv6 owner when introduced | Fixed singleton selector; never dereferenced as a pool index |
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
or multicast variants. Hammer's service adjacency owner validates the
producer-selected class key before publishing the `(DpoType, index)` pair;
the producer, not a generic enum, supplies the subtype-specific facts.
Therefore a midchain is not a field on a generic adjacency ID and is not an
IP-owned variant.

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
as a second load-balance object class. `DpoMain` stores the class's originating
node set directly; the existing graph-node registration owns any `sibling_of`
relationship. It does not fabricate an object per graph node.

`dpo_register()` installs one class key with its per-protocol node list.
`dpo_register_new_type()` allocates a new key for a plugin class or for a
fast-path subtype that shares an existing object pool. Hammer's proc macro
calls `DpoMain::register_new_type` with the already resolved
`&[(DpoProto, NodeId)]` list. The class key/node data are fields of `DpoMain`;
there is no declaration record or second runtime record. Explicit owner
initialization retains the class-key-to-pool binding and all object access
needed by its operations.

The DPO value operations use Rust value semantics. `DpoId` derives `Copy` and
`Clone`; assigning or passing it by value copies the compact
`{ type, proto, index, next }` identity and does not resolve an index, touch a
pool, or invoke an object operation. The service-owned `DpoMain` validates
class/protocol/node metadata and derives stack edges. The concrete class owner
validates the `index` against its own pool before dereferencing it. An invalid
class/protocol/index combination is a control-plane error, not an unchecked
cast.

Cross-owner dependencies are explicit at the concrete owner seam. For example,
a load-balance owner retains each child root while its bucket contains that
identity, and releases the child before retiring the parent. No erased
foreign-object operation table, generic retain token, or service-wide lifetime
dispatcher is introduced.

This is the Rust mapping of VPP's `dpo_vfts[type]` class/node lookup without
copying its C callback table. Compile-time knowledge of every foreign class is
kept at the concrete owner seam, and object reclamation is ordered by the owner
that can see the actual pool and dependencies.

`dpo_get_mtu`, `dpo_get_urpf`, `dpo_mk_interpose`, and `dpo_is_adj` remain
semantic DPO operations. The owner of the concrete class implements them as
typed static functions; interpose creates an owner-class object stacked on the
parent DPO, and adjacency classification is based on the registered adjacency
class slots. They are not fields on `DpoId`, are not service dispatch methods,
and are not implemented by a generic protocol enum.

`dpo_get_next_node()` may be class-default or instance-dependent. The default
path resolves `dpo_nodes[type][proto]`; `DPO_INTERFACE_TX` is the important
instance-dependent case because its next node comes from the wrapped
`sw_if_index`. `DpoMain` therefore caches only type/protocol edges; it never
caches an object pointer or an instance-specific node result. A stack operation
receives the child `(DpoType, DpoProto)` and parent `DpoId`, obtains the parent
node slot, adds the graph edge, and returns a copy of the parent identity with
that edge in `next`. Parent object lifetime remains the concrete owner's
responsibility.

Pool growth/removal follows the VPP `dpo_pool_barrier_sync` rule. A pool
operation that can expand or recycle worker-visible storage is performed only
inside the already-held Binary API dispatcher barrier scope (or during startup
before workers run); the concrete owner does not enter a second barrier. The
owner publishes the object/ID while workers are stopped and exits the scope
after the mutation. `DpoId` remains a compact identity value; its
worker-visible replacement is ordered by the existing barrier publication, not
by a second pointer or completion protocol.

The current single `AdjacencyRewriteNode` is split into two concrete IP graph
nodes to match VPP's per-data-path node table: `Ip4RewriteNode` owns IPv4
checksum/TTL and IP MTU handling, while `Ip6RewriteNode` owns IPv6 header and
IP MTU handling. Both resolve a service-owned adjacency object from the
matching class slot and pool index, interpret its concrete rewrite state, then
write the service-owned TX `sw_if_index`; neither changes the DPO class
registry.

The split is a graph registration seam, not duplicated packet-processing
implementations. Inside the IP plugin, a private static-dispatch core owns the
shared adjacency read, TX metadata write, MTU decision plumbing, error
classification, trace recording, and next-arc selection. The protocol-specific
policy is selected at compile time:

```rust
pub struct Ip4RewriteNode {
    runtime_data: NodeRuntimeData,
}

pub struct Ip6RewriteNode {
    runtime_data: NodeRuntimeData,
}

trait RewriteProtocol {
    type Address;
    type Rewrite;
    const DPO_PROTO: DpoProto;

    fn mtu_action(
        packet: &mut Buffer,
        adjacency: &AdjacencyDpo<Self::Address, Self::Rewrite>,
    ) -> MtuAction;
    fn apply_rewrite(
        packet: &mut Buffer,
        adjacency: &AdjacencyDpo<Self::Address, Self::Rewrite>,
    ) -> RewriteResult;
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

`hammer-service::net::dpo` owns the layout and class rules. `DpoMain` owns only
the class key, node bindings and derived graph edges. The concrete pool owner
is the module that chooses the type parameters and can interpret the bytes:
the service owns service-valued pools, while an IP, tunnel or multicast owner
owns a pool instantiated with its concrete address/rewrite values. No pool
owner is hidden behind `DpoMain`.

```text
concrete adjacency owner:
    class = DpoType("adjacency")
    objects = Pool<AdjacencyDpo<A, R>>
    ops = owner-local typed adjacency operations

service load-balance owner:
    class = DpoType("load-balance")
    objects = Pool<LoadBalanceDpo>
    ops = service DPO load-balance operations
```

The pool index is meaningful only with the matching `DpoType`. A rewrite or
lookup node must not cast an arbitrary `DpoId.index`; it checks the class slot
and then obtains the concrete object through the class owner's typed pool API.
The generic layout lives in service net, while the concrete owner of an
`A`/`R` instantiation owns that pool, its root-retention count, and the node
that interprets it. `DpoMain` cannot validate or dereference the pool index.

- `AdjacencyDpo<A, R>` owns only protocol-neutral forwarding facts: egress
  `sw_if_index`, an optional producer-supplied `A` next-hop, owner-supplied
  rewrite state `R`, and an optional child `DpoId` for a stacked class. Its
  layout and class-subtype rules remain in service net; IP, tunnel, and
  multicast producers choose and validate the concrete `A`/`R`, then provide
  the node that interprets them.
- `LoadBalanceDpo` owns forwarding bucket storage and hash selection. Its
  packet-visible buckets contain service `DpoId` values; the service owner
  validates buckets and retains each child root while it is present.

Midchain is a subtype of the shared adjacency pool, not a second object pool.
Its owner-local state includes the recursive target entry, the stacked child
`DpoId`, tracking sibling, looped flag, midchain flags, rewrite bytes, and the
owner's post-rewrite fixup state. The owner operations are update-rewrite,
reset/update-next-node, stack on the child DPO, stack on the target FIB entry,
recursive-loop detection, restack, and unstack-to-drop. A target entry change
restacks the adjacency; admin-down, an invalid target, or a detected loop
unstacks it to drop. The tunnel or other producer owns the fixup function and
its data; service net stores neither callback nor tunnel state. FIB tracking is
cancelled before an adjacency pool slot is removed.

The object-bearing DPO classes stay with the owner of the state they inspect:

- `LookupDpo` stores lookup input/table/cast facts in the service-owned pool;
- `ReceiveDpo<A>` stores the concrete `sw_if_index` and producer-owned address
  value in the class owner's pool for that `A`;
- `InterfaceRxDpo` stores the one-per-interface/per-protocol receive object
  in an interface-owned pool; the interface record owns it until removal;
- `InterfaceTxDpo` is the stateless wrapper around a `sw_if_index`; its next
  node is resolved by the interface owner and it has no pool lifetime;
- `DropDpo` and `PuntDpo` are stateless per-protocol singletons, matching VPP's
  `drop_dpos[DPO_PROTO_NUM]` and `punt_dpos[DPO_PROTO_NUM]` arrays.

This follows VPP's object split without making the service layer depend on the
IP plugin: `lookup_dpo_t`, `receive_dpo_t`, `load_balance_t`, and adjacency
objects remain actual pool objects, while `dpo_id_t` carries their pool index.
The generic DPO registry and class layouts do not import IP route, next-hop,
ARP/ND, PMTU, tunnel, or multicast types; address-bearing slots are generic
over the producer's concrete address type.

`hammer-service::net` exposes only the values and contracts needed to connect
these implementations: `DpoProto`, `DpoType`, `DpoId`, direct
`DpoMain::register_new_type`/stack operations, FIB source/entry/path relationships, and the forwarding
projection input. A normal route path is first retained as a `FibPath` with
its next-hop, interface, table, weight, preference, and owner-defined path
flags. The IP
backend resolves that path when the source becomes contributing, asks the
concrete DPO owner to create or update the adjacency/load-balance objects, and
records the resulting `DpoId` in the generic FIB graph. The generic FIB never
reaches into a DPO pool and never imports an IP type.

This follows VPP's ownership split: `ip_adjacency_t` and `adj_nbr_*` live under
`vnet/adj`, while `load_balance_t` is a concrete DPO implementation under
`vnet/dpo` and is consumed by protocol nodes. Hammer places the shared
layouts and class/node rules in service net; concrete class owners instantiate
pools and typed operations for their payload types and let IP backends provide only concrete
next-hop facts and graph-node bindings. A non-IP plugin can instantiate the same
layouts without importing the IP plugin; no non-IP implementation is required
by this ADR.

### Data layout and packet-path performance

The layout contract follows VPP's cacheline decisions without turning them into
a second abstraction layer. `hammer-infra::align::CACHE_LINE` remains 64 bytes,
`VEC_MIN_ALIGN` remains the 8-byte minimum allocation alignment, and
`CacheLineAlignMark` is the existing marker for a cache-aligned record or field
group. No new alignment helper or DPO wrapper is introduced.

`DpoId` is the compact exception to cacheline-sized pool objects. Its private
`u64` representation must satisfy both `size_of::<DpoId>() == 8` and
`align_of::<DpoId>() == 8`; its accessors, validity check and stack operation are
small candidates for `#[inline(always)]`. It is not marked `repr(C)` and it is
never padded to a cacheline.

Concrete pool elements for `LookupDpo`, `ReceiveDpo<A>`, `LoadBalanceDpo` and
each `AdjacencyDpo<A, R>` instantiation are allocated at a cacheline-aligned
base. The fields read by a packet node come first; control-only fields are kept
after the hot prefix or in owner-local side storage. A concrete instantiation
adds compile-time `size_of`/`align_of` assertions and, where it uses explicit
cacheline groups, offset assertions. `repr(C, align(64))` is allowed only for a
record whose field offsets or cacheline boundaries are an actual FFI/layout
contract. These DPO objects are Rust-owned; their concrete owners use the
existing alignment marker and compile-time assertions instead of adding a C ABI
merely to make a pool fast. Ordinary Rust structs and control-plane records keep
their normal Rust representation.

`LoadBalanceDpo` follows `load_balance_t`'s one-cacheline fast form. Its
`lock_count` is the concrete owner's published-root count corresponding to
VPP's `lb_locks`; it is a lifetime reference count, not a mutex, and has no
manual lock/unlock API. The bucket
count is a power of two, `bucket_mask` is `bucket_count - 1`, and the first four
child `DpoId` values are inline. The object contains only the compact hot
fields and a `u32::MAX`-sentinel owner-local overflow index; when more than four
buckets are needed, the class owner keeps a contiguous overflow slice keyed by
the existing load-balance pool index. Bucket selection is therefore a mask and
an array access in the common case, with no division, map lookup, allocation or
bucket reconstruction on the packet path. The owner validates the maximum
bucket count before publication.

`LookupDpo` keeps its FIB/table/input/cast facts in the first cacheline, and
`ReceiveDpo<A>` keeps `sw_if_index` and the producer-owned address there. An
`AdjacencyDpo<A, R>` puts the FIB-node/config/subtype facts in its hot prefix,
rewrite state in its following cacheline group, and delegates, tracking,
fixup/control state in a separate control group. The generic adjacency layout does not
promise one size for every `A`/`R`; each concrete owner proves its own layout.
Delegates, path extensions, back-walk records and source lists are control-plane
state and never participate in the packet hot prefix.

The packet forwarding sequence is deliberately bounded and allocation-free:

```text
dense sw_if_index[RX] -> fib_index_by_sw_if_index
    -> concrete LPM -> entry load-balance index
    -> post-LPM bucket mask/lookup -> child DpoId.next
    -> concrete DPO node -> sw_if_index[TX]
```

The packet path does not traverse `FibEntrySrc` lists, delegates, path
extensions or back-walk links; it does not allocate, acquire a control-plane
lock, format an error, or consult a `BTreeMap`. `fib_index_by_sw_if_index` is a
dense per-implementation mapping, while FIB graph ownership and class/index
validation stay in the control plane. `DpoMain` resolves class/node/edge facts
when a class or graph edge is published, so a worker follows the compact
identity and cached `next` rather than resolving a class map for every packet.

`NetworkOpaque` remains inside the existing `PrimaryOpaque` byte/alignment
budget. RX/TX interface indices and the independently stored FIB index use
compact scalar fields and explicit sentinels; no pointer, address object or
control-plane collection is copied into packet metadata. A layout change is
accepted only with compile-time size/alignment assertions and a packet-path
behavior test.

No throughput or latency number is asserted here because the repository has no
approved forwarding SLA or release benchmark baseline. A release benchmark is
required before claiming a measured performance improvement; until then these
are structural invariants that prevent avoidable cache misses, indirection and
packet-path allocation.

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
  LookupDpo, ReceiveDpo             generic FIB/local-delivery layouts
  AdjacencyDpo, LoadBalanceDpo      generic layouts and class rules
  InterfaceRxDpo, InterfaceTxDpo    interface-owned DPO layouts/objects

hammer-plugins/net/ip
  IpNullDpo, IpPmtuDpo              IP-specific DPO classes/policy
  ip4-rewrite / ip6-rewrite         concrete IP graph-node implementations
```

The exact constructors remain class-specific. A normal Binary API route
handler does not match `IpRoutePathBehavior` to construct a DPO and does not
write a raw class/index pair. It decodes the request into a service `FibPath`; the
contributing owner selects the concrete adjacency subtype, resolves
recursive/interface state, and asks the concrete DPO owner to project the
resulting object to `DpoId`. For `Drop`, `IcmpUnreachable`, and
`IcmpProhibit`, the IP owner selects an immutable `IpNullDpo` action record;
ordinary route messages still carry no `DpoId`.
`Local`, `Drop`, `SourceLookup`, and `InterfaceRx` are VPP path semantics in
that same input, not a generic DPO constructor API. An owner-internal special
operation may call `special_dpo_add/update` with an already constructed
concrete `DpoId` (for example the IPv6 link-local lookup DPO), but that direct
DPO form is not exposed by the ordinary route message. A normal adjacency
producer may classify itself as incomplete, midchain, glean, or multicast while
it is being resolved; no route request constructs a `MidchainDpo` object or
writes a raw class/index pair.

`DpoType` is the registry-assigned class key, not a stand-alone object. Its
meaning comes from the class/node tables held directly by `DpoMain` and the
concrete pool and operations bound to that key by the owning class module.
`DpoProto` selects the protocol-specific graph node/edge. `DpoId.index` is never globally
dereferenced; only the matching class owner may turn `(DpoType, index)` back
into a concrete object. This keeps path semantics visible at the plugin seam
while retaining VPP's compact `dpo_id_t` shape without dynamic dispatch.

The ownership mapping to VPP is explicit:

| VPP object | Hammer owner and object | Meaning |
| --- | --- | --- |
| `dpo_proto_t` | `hammer-service::net::DpoProto` | data-path protocol used to choose a graph arc |
| `dpo_type_t` | `hammer-service::net::DpoType` | class key; `DpoMain` stores its per-protocol nodes, while the matching concrete owner stores the object pool and operations |
| `dpo_id_t` | `hammer-service::net::DpoId` | `{ type, proto, index, next }` identity produced from an owner pool object or singleton |
| `dpo_nodes[type][proto]` | `DpoMain` class/node tables | registered `(DpoProto, NodeId)` bindings |
| `dpo_edges[child][proto][parent][proto]` | `DpoMain` edge table | derived graph edge for stacking |
| `dpo_vft_t` | owner-local typed DPO operations; `DpoMain` stores only the per-protocol node metadata | formatting, object mutation, dependency policy, MTU, uRPF and interpose remain with the concrete class owner; no erased callback table crosses the service seam |
| `dpoi_index` + `dpoi_type` | owner-local `(class slot, Pool<T>)` lookup | index is valid only in the pool belonging to the matching class slot |

Thus the generic `AdjacencyDpo<A, R>` object and its subtype class keys are in
`hammer-service::net::dpo`. The IP plugin owns only the IPv4/IPv6 graph nodes,
neighbor/address interpretation, and IP policy that produce or consume those
objects. A tunnel or multicast plugin can contribute another subtype producer
without changing `DpoId` or importing the IP plugin.

There is no network-specific DSO image or export. A plugin's existing
`RegistrationImage` loads its `InitFunction` inventory. The generated
owner-local DPO registration function calls `DpoMain` directly after `NetMain`
exists; the loader does not decode or own network declarations. Runtime route
updates are exposed through the owner plugin's Binary API methods, not a direct
FIB publication handle.

### FIB and DPO split

The following protocol-neutral forwarding types move to
`hammer-service::net`:

- `DpoProto`, `DpoType`, `DpoId`, and `DpoMain`;
- producer-supplied concrete address values retained by generic DPO layouts;
- `FibSource`, `FibEntry`, `FibEntrySrc`, `FibPathList`, `FibPath`, source
  precedence/merge state, and the FIB mutation/publication contract. `FibPath`
  is generic over the owner-supplied path flags value;
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

The service graph fields and the two concrete backend records are:

```rust
pub struct FibSource {
    id: u8,
    priority: u8,
    behavior: FibSourceBehavior,
}

pub enum FibSourceBehavior {
    Drop,
    Api,
    Simple,
    RecursiveResolution,
    Interface,
    Interpose,
    Adjacency,
}

pub struct FibEntrySrc<N, F, SourceData, PathExt> {
    path_exts: FibPathExtList<N, F, PathExt>,
    path_list: Option<u32>,
    entry_flags: FibEntryFlags,
    source: FibSource,
    flags: FibEntrySrcFlags,
    ref_count: u8,
    cover: Option<(u32, u32)>,
    interpose_dpo: Option<DpoId>,
    source_data: SourceData,
}

pub struct FibPath<N, F> {
    sw_if_index: u32,
    table_id: u32,
    rpf_id: u32,
    weight: u8,
    preference: u8,
    flags: F,
    next_hop: N,
}

pub struct FibPathExtList<N, F, E> {
    entries: Vec<FibPathExt<N, F, E>>,
}

pub struct FibPathExt<N, F, E> {
    path: FibPath<N, F>,
    path_index: u32,
    data: E,
}

pub struct FibPathList<N, F> {
    paths: Box<[FibPath<N, F>]>,
    key_flags: FibPathListFlags,
    flags: FibPathListFlags,
    source_count: u32,
    child_count: u32,
    children: Vec<(u16, u32)>,
    urpf_index: Option<u32>,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct LoadBalanceFlags: u8 {
        const USES_MAP = 1 << 0;
        const STICKY = 1 << 1;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct FibEntryFlags: u16 {
        const CONNECTED = 1 << 0;
        const ATTACHED = 1 << 1;
        const DROP = 1 << 2;
        const EXCLUSIVE = 1 << 3;
        const IMPORT = 1 << 4;
        const LOCAL = 1 << 5;
        const MULTICAST = 1 << 6;
        const LOOSE_URPF_EXEMPT = 1 << 7;
        const NO_ATTACHED_EXPORT = 1 << 8;
        const COVERED_INHERIT = 1 << 9;
        const INTERPOSE = 1 << 10;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct FibEntrySrcFlags: u16 {
        const ADDED = 1 << 0;
        const CONTRIBUTING = 1 << 1;
        const ACTIVE = 1 << 2;
        const STALE = 1 << 3;
        const INHERITED = 1 << 4;
        const PROVIDES_GLEAN = 1 << 5;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct FibPathListFlags: u8 {
        const SHARED = 1 << 0;
        const DROP = 1 << 1;
        const LOCAL = 1 << 2;
        const EXCLUSIVE = 1 << 3;
        const RESOLVED = 1 << 4;
        const LOOPED = 1 << 5;
        const POPULAR = 1 << 6;
        const NO_URPF = 1 << 7;
    }
}

pub struct BackWalkContext {
    reason: BackWalkReason,
    depth: u8,
}

pub enum BackWalkReason {
    Resolve,
    Evaluate,
    InterfaceUp,
    InterfaceDown,
    InterfaceBind,
    InterfaceDelete,
    AdjacencyUpdate,
    AdjacencyMtu,
    AdjacencyDown,
}

pub struct Ip4FibBackend {
    fib_index: u32,
    prefixes: Ip4Mtrie<u32>,
}

pub struct Ip6FibBackend {
    fib_index: u32,
    prefixes: Ip6Fib<u32>,
}
```

This is the complete Rust projection of VPP's common `fib_entry_src_t` fields;
it is not a secondary map. `FibEntrySrc` is the concrete record for one source
owner and has no independent FIB-node identity. `N` is the concrete path value,
`SourceData` is that owner's source payload, and `PathExt` is that owner's
path-extension payload. The generic record is instantiated per concrete source
store; a table does not choose one `SourceData` or `PathExt` for every source.

The cover/sibling pair and optional interpose DPO are direct fields on the
record. `cover: None` means that the source is not tracking a cover; when it is
`Some`, the tuple is `(cover_entry, sibling_slot)`. `interpose_dpo` is an
independent optional contribution and does not select source behavior. This
keeps topology facts separate from the `FibSource.behavior` selector without
introducing a wrapper type or a protocol-specific union.

VPP stores heterogeneous source payloads in a C union because every source
record has the same physical type. Rust cannot make one `Vec` hold different
`FibEntrySrc<N, F, SourceData, PathExt>` instantiations without an enum or dynamic
erasure. Hammer therefore stores each concrete record in its behavior owner's
typed pool and keeps only `(FibSource, u32)` source slots in the entry. The
`u32` is an owner-local pool position, not a castable service index. The source
behavior match invokes the corresponding monomorphized implementation; no
`dyn` value, byte-erased union, callback table, or table-wide generic payload is
introduced. A plugin source may select an existing behavior through the proc
macro; adding a new behavior is an explicit service change.

`path_exts` is per source, exactly like VPP `fes_path_exts`, and is not part of
the shared `FibPathList` key. Each extension retains the complete matching
path value, its global path index, and its concrete payload. Owner-specific
extensions therefore use the same list seam without forcing protocol fields,
address layouts, or tunnel state into `FibPath<N, F>`.

The VPP `fib_entry_src_vft_t` actions are implemented by a static match on the
`FibSource.behavior` tag; the function-table shape is not copied into Rust. Each
match arm receives the concrete owner record from its typed pool:

| VPP action | Rust source operation | Required state transition |
| --- | --- | --- |
| `init` | create source record | empty path-ext list, no path list, zero flags/count, and owner-initialized `SourceData` |
| `deinit` | remove source record | only after `ref_count == 0`; flush path extensions, detach cover/sibling and release the path-list link |
| `activate` | activate best source | resolve/track cover and sibling facts, select interpose contribution, or install the behaviour's special/path result |
| `deactivate` | deactivate losing source | clear active/contributing state, remove cover tracking, and detach the active path-list link |
| `reactivate` | re-evaluate updated winner | run deactivate/activate semantics and return whether forwarding must be rebuilt |
| `add` | add source contribution | create the special/path list required by the behaviour; generic code increments `ref_count` and marks `ADDED` |
| `remove` | remove one contribution | decrement only the repeated-add count; at zero, remove/inherit/deactivate and then deinit |
| `path_swap` | replace complete path set | create/find the replacement canonical path list, remap extensions by path value/index, then replace the source link |
| `path_add` | add paths | copy-on-write the shared path list and insert matching path extensions |
| `path_remove` | remove paths | copy-on-write the shared path list and remove matching extensions; an empty result follows source remove/inherit rules |
| `cover_change` | cover changed | recompute the less-specific cover and return the required back-walk reason |
| `cover_update` | cover forwarding changed | re-resolve inherited/glean/adjacency state and return whether to reinstall |
| `installed` | source installed notification | notify the source after the entry's forwarding projection becomes visible |
| `fwd_update` | forwarding update notification | notify every added source, not only the active source, with the winning source |
| `set_data` / `get_data` | typed source-data access | replace, mutate or borrow the concrete `SourceData`; no `void*`, protocol union, or erased payload |
| `contribute_interpose` | interpose contribution | return `interpose_dpo` when it is contributing |
| `flags_change` | source flag change | apply entry-flag changes and re-evaluate source/path-list flags before publication |
| `copy` | inherited source copy | clone source data and path extensions, link the same path list, set `INHERITED`, and clear `ACTIVE`/`CONTRIBUTING` until activation |

The phase-one behavior set is the VPP set that remains useful without
protocol-specific source branches: `Drop`, `Api`, `Simple`,
`RecursiveResolution`, `Interface`, `Adjacency`, and `Interpose`. A source
registration stores one of these behavior tags; it does not allocate a new
behavior at runtime. `SourceData` and `PathExt` are associated with the typed
owner pool that stores a record, so two pools using the same behavior may still
use different concrete payload types. `FibTableBackend` therefore has no
table-wide `SourceData` or `PathExt` associated type: it owns prefix storage and
forwarding projection, while the source behavior arm owns its typed record.

`ref_count` is specifically VPP's `fes_ref_count`: repeated adds of one source
to one entry. It is not a path-list user count, a DPO reference count, or a
FIB-node lock count. The path-list and DPO dependencies have their own owner
lifetimes and are replaced only after the source record no longer points at
them.

`FibPath<N, F>` is the protocol-neutral core path. `N` is the concrete next-hop
value and `F` is an owner-defined path-flags value; the service FIB stores both
without interpreting either business type. For this migration the IP plugin
supplies `IpPathFlags`; another business plugin supplies its own `F`. Owner-specific facts use the
separate `PathExt` record and never become fields of this path list. The
concrete IP next-hop values are:

```rust
pub enum Ip4NextHop {
    Address(Ipv4Addr),
    Connected { prefix: Ipv4Net },
    UdpEncap { id: u32 },
    Classify { table_id: u32 },
}

pub enum Ip6NextHop {
    Address(Ipv6Addr),
    Connected { prefix: Ipv6Net },
    UdpEncap { id: u32 },
    Classify { table_id: u32 },
}
```

`FibTableBackend` is the only backend seam. It deliberately mirrors VPP's
non-forwarding and forwarding databases. It receives a complete `DpoId` for
owner projection and removal and never receives an external `table_id`, an
arbitrary `fib_index`, or a plugin pool from a caller. A backend instance is
bound to the concrete table context when the table is created;
the IPv6 instance may still write the process-wide forwarding hash, but it
injects its bound `fib_index` internally. Every fallible mutation returns an
owner-local typed error so the FIB can prepare all participants before the
first store mutation:

```rust
pub trait FibTableBackend {
    type Prefix: Copy;
    type PacketAddress: Copy;
    type NextHop: Clone;
    type PathFlags: Copy;
    type Error;

    // Non-forwarding database: prefix -> fib_entry index.
    fn lookup(&self, prefix: Self::Prefix) -> Option<u32>;
    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32>;
    fn less_specific(&self, prefix: Self::Prefix) -> Option<u32>;
    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error>;
    fn remove_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error>;

    // Derived forwarding database: packet address -> entry load-balance index.
    // The backend is already bound to one concrete table context.
    fn forwarding_lookup(&self, address: Self::PacketAddress) -> Option<u32>;
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
        entry: &FibEntry<Self::Prefix, Self::NextHop>,
        source: FibSource,
    ) -> Result<Option<DpoId>, Self::Error>;
}
```

`DpoId` in this contract is the concrete, non-generic eight-byte identity
described above; `DpoId<N>` is not a target API. `project_forwarding` returns
the identity produced by the concrete DPO owner after the contributing protocol
owner has interpreted its path facts. The matching concrete DPO owner owns the
object and its dependency ordering; the service FIB stores only the identity and passes it to
`forwarding_update/remove`. There is no projection wrapper, object store,
manual lifetime operation, or second lifecycle interface.

`FibTable<P, B>` is statically dispatched. This means its backend call is
monomorphized; it does not make `DpoType` static. It owns
`FibEntry<P, B::NextHop>`, source slots, shared
`FibPathList<B::NextHop, B::PathFlags>`, configured
`FibPath<B::NextHop, B::PathFlags>` values, graph-node
references, source ownership records, source route counts, and the current
forwarding `DpoId`. `B` is the only implementation seam: it owns the concrete
prefix stores, resolves owner-dependent paths, asks the matching concrete DPO
owner to create or update `LoadBalanceDpo` objects, and returns the complete
`DpoId` whose identity VPP passes to the protocol backend. The backend writes only
`dpoi_index` into
its forwarding table. There is no separate forwarding adapter, callback
registry, or erased owner state.

`project_forwarding` is the Rust seam for VPP's
`fib_entry_src_mk_lb`. It receives the service-owned entry and source facts;
the backend may use its concrete protocol owner state, but the service FIB never
dereferences a plugin pool. For a fallible Rust mutation, `FibTable` builds the
candidate source/path state in detached `FibEntry`/`FibPathList` values, asks
the backend to project that candidate, and commits the candidate only after
projection and store validation succeed. An error therefore leaves the
published source graph and the owner's current projection unchanged; no
candidate wrapper type is introduced. If a projection already exists and its
class/chain remains valid, the backend updates that owner object in place. A
class, chain, exclusive, or interpose change prepares a replacement before the
old projection is removed. The backend publishes the new `DpoId` only after the
concrete object and its dependencies are ready.

The service FIB calls `project_forwarding` during source activation,
reactivation, and back-walk. If it returns a DPO, the FIB calls
`forwarding_update`; if it returns `None`, `forwarding_remove` removes the old
forwarding identity and the matching concrete DPO owner releases that root and
retires the corresponding object when its published-root count reaches zero
after worker quiescence. This is the Hammer
equivalent of
VPP's `fib_entry_src_mk_lb` followed by `fib_table_fwding_dpo_update`, while
keeping source behavior and concrete DPO objects in the owner that can resolve
them.

Lookup therefore has two explicit control/data-plane forms. A control-plane
lookup accepts a route `Prefix` and returns a `fib_entry` index for inspecting
source/path state. The packet path accepts a concrete destination address value
(not a prefix with its host bits discarded), performs the IP implementation's
LPM in the forwarding store, recovers the entry's load-balance index, selects
its bucket, and only then obtains the bucket's complete child `DpoId`. The
forwarding store's `u32` is the entry load-balance `dpoi_index`; it is not a
standalone DPO identity. The complete `DpoId` remains in the FIB entry for
class/protocol dispatch, while the matching concrete DPO owner owns the
corresponding object.
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
| `special_dpo_add` / `special_dpo_update` | Add or replace a source-owned concrete `DpoId`; these are owner-internal operations, and the concrete owner keeps the object until that source contribution is removed. |
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
`DpoId` produced by the concrete owner. The source entry keeps that identity
reachable until the contribution is removed; the owner retires the concrete
object as part of the same failure-atomic forwarding mutation. They are not a
generic DPO constructor and are not exposed as raw `DpoId` fields in the
ordinary route Binary API.

Creating a table installs the protocol implementation's mandatory default and
special entries. The table stores protocol-independent facts equivalent to
VPP `fib_table_t`: external `table_id`, internal `fib_index`, table flags
(including IPv6 link-local and resync), source ownership records, active source
count, per-source route counts, total route count, flow-hash configuration,
epoch and description. The table-id map is removed only after entry teardown
and forwarding projection removal. `table_id` is never used as a `fib_index`.

FIB source registration retains VPP's three-part source record: source id,
single `u8` priority, and behavior. Lower priority values win; equal priority
does not create an ECMP choice because source precedence is resolved before path
selection. The image contains metadata only. The entry stores the behavior tag
and source slot; generic FIB code owns precedence, cover tracking and back-walk,
while the matching typed source pool owns behavior-specific state and lifecycle.
No plugin callback table or source-specific state is serialized into the image.

Each `FibEntry` owns one source slot for every source that has contributed to
that prefix. A slot is the pair `(FibSource, u32)`, where the `u32` addresses the
matching concrete source record in its behavior owner's typed pool. The record
is still the Rust form of VPP's embedded `fib_entry_src_t`: it keeps
`path_exts`, the source path-list link, entry/source flags, `source`, the
repeated-add `ref_count`, the direct cover tuple and optional `interpose_dpo`, and the
owner's concrete `SourceData`. The physical split is what permits heterogeneous
source payloads without a central enum or dynamic erasure. `ref_count` is
repeated adds from one source, not the number of users of a path list or DPO
object. `path_exts` remains associated with the `(entry, source)` record and is
not part of a shared `FibPathList` key.

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

`FibSource` carries the VPP source id, single priority and behavior. The
entry stores that metadata independently of the concrete source payload. The
source owner supplies `SourceData`; service net does not enumerate protocol
feature names. The source state tracks
`ADDED`, `CONTRIBUTING`, `ACTIVE`, `STALE`,
`INHERITED`, and `PROVIDES_GLEAN`; source priority never doubles as a behavior
selector. Entry state separately records connected, attached, drop, exclusive,
import, local, multicast, uRPF-exempt, no-attached-export, covered-inherit,
and interpose flags.

The entry's optional delegates are created only when their corresponding graph
link exists:

| Delegate | Stored facts | Lifecycle |
| --- | --- | --- |
| forwarding chain | one owner-defined forwarding chain and its `DpoId` | create on first chain, restack on forwarding change, remove with the entry |
| covered entries | child entry list for a cover | add/remove and walk on cover change |
| BFD | BFD state associated with the entry | update from the owning BFD source and clear with the entry |
| entry tracking | tracker node `(node_type, index)` and sibling | attach/detach as the tracker target changes |
| attached import/export | cross-table attached cover relationship | export/import more-specific entries, update on cover change, purge on teardown |

These delegates keep optional chains out of every entry's fixed storage. The
covered list and tracker are indirect FIB graph links: a tracker becomes one
child of the target entry instead of making every client a direct child, which
avoids quadratic notification fan-out. Cross-table attached import/export is
committed as one FIB mutation; a failed cover update leaves both tables and
their delegates unchanged.

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
from its pool after no published identity names it. Its child dependencies are
then retired by the owning concrete state. Exclusive and interpose entries still follow VPP's source-selection
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
contains only input facts: the owner-supplied next-hop value, interface/table
identity, weight, preference and path flags. IP-owned route path behavior is
decoded before this seam; special actions become owner-created DPO identities,
not a service path enum or object field. Its derived operational state (resolved DPO, resolving
entry, up/down/looped status and owner pool identity) is kept by the concrete
backend projection state and is never part of the path-list key. Resolution may
add a dependency on another FIB entry/path list.

Path extensions are associated with the source's path, not with the shared
canonical path-list key. The association is the tuple `(entry, source,
path_index)`; the extension payload is the concrete `PathExt` parameter of the
owner pool's `FibEntrySrc`. The lifecycle is add/insert, remove/find (including
lookup by path index), resolve, stack, and flush when the source or path list
changes. Protocol-specific extensions use this owner-defined seam and never
become fixed fields of `FibPath<N, F>`.

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

The uRPF list is a baked, read-only projection of the resolved interface and
adjacency facts. It is shared by load-balance objects and is rebuilt when a
path state changes; packet lookup does not re-walk the forwarding DPO chain to
reconstruct it. The load-balance map is a supporting weighted-bucket remapping
object, not a DPO class: normalized weights are translated to buckets, maps
may be shared by recursive load-balance objects, and a path state change
refills every dependent map. Its owner-managed reference count follows Rust
drop/replacement ordering and exposes no manual release operation.

Every FIB graph node is identified by `(node_type, u32 index)` and keeps an
heterogeneous child list with a sibling slot; no graph relation stores a raw
pointer. Back-walk uses synchronous or queued `fib_walk_sync`/
`fib_walk_async` according to high/low priority. A force-sync mark overrides
the queue for operations that must complete before publication. Walk depth is
bounded at 32 and a repeated node is reported as a recursive-loop error.

Interface address and link-state changes therefore submit interface-source
contributions through the same entry/path machinery. The IP owner creates
connected/attached, local receive, glean, broadcast/drop and uRPF results as
concrete path or special-DPO contributions. They do not write a forwarding
trie directly.

This ADR covers the unicast `FibTable<P, B>` implementation, while fixing the
MFIB seam explicitly. `Ip4Main` and `Ip6Main` own distinct `mfib_table` and
`mfib_entry` pools, multicast path/RPF/interface flags, and the
`mfib_index_by_sw_if_index` mappings. Multicast entries project to the generic
replicate DPO and may use multicast adjacency subtypes; those objects are not
fields of the unicast table or its path-list key. The unicast route methods
reject multicast path requests, and MFIB mutations follow the same Binary API
barrier and failure-atomic publication contract in the owning IP main.

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
   `LoadBalanceDpo` projection as a complete `DpoId`; the backend prepares the
   concrete object before its identity is published. The service FIB does not
   construct or discover a DPO;
6. commit the non-forwarding entry store and derived forwarding store through
   the backend. On removal, the FIB supplies the less-specific cover's
   `(prefix, DpoId)` when the backend needs it: IPv4 restores the cover through
   the MTRIE, while IPv6 deletes the exact
   `(bound fib_index, masked prefix, prefix length)` key and updates shared
   prefix-length accounting. Every backend operation is failure-atomic; an
   error leaves the source graph and both stores unchanged;
7. after a successful backend removal and worker quiescence, the concrete owner
   retires the old DPO object and its concrete child dependencies. A plain
   `DpoId` copy never owns an object.

This corresponds to VPP `fib_table_entry_special_*`,
`fib_table_entry_path_add/remove`, `fib_table_entry_update`,
`fib_table_entry_delete`, `fib_entry_src_action_*`,
`fib_path_list_*_add/remove`, `fib_table_fwding_dpo_update/remove`, and
`fib_walk_sync/async`. The non-forwarding graph remains the source of truth;
the forwarding trie/hash is only its derived data-plane projection.

The forwarding projection is mutated only while the Binary API dispatcher holds
the worker barrier. A worker keeps using the installed concrete table values
after the dispatcher exits the macro scope; the owner must not reclaim or
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

The concrete request, path, reply, and status fields are:

```rust
pub enum IpRoutePathBehavior {
    Normal,
    Local,
    Drop,
    UdpEncap,
    IcmpUnreachable,
    IcmpProhibit,
    SourceLookup,
    Dvr,
    InterfaceRx,
    Classify,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct IpPathFlags: u32 {
        const RESOLVE_VIA_HOST = 1 << 0;
        const RESOLVE_VIA_ATTACHED = 1 << 1;
        const LOCAL = 1 << 2;
        const ATTACHED = 1 << 3;
        const DROP = 1 << 4;
        const EXCLUSIVE = 1 << 5;
        const INTF_RX = 1 << 6;
        const RPF_ID = 1 << 7;
        const SOURCE_LOOKUP = 1 << 8;
        const UDP_ENCAP = 1 << 9;
        const DEAG = 1 << 13;
        const DVR = 1 << 14;
        const ICMP_UNREACH = 1 << 15;
        const ICMP_PROHIBIT = 1 << 16;
        const CLASSIFY = 1 << 17;
        const GLEAN = 1 << 19;
    }
}

pub struct Ip4RoutePath {
    sw_if_index: u32,
    table_id: u32,
    rpf_id: u32,
    weight: u8,
    preference: u8,
    behavior: IpRoutePathBehavior,
    flags: IpPathFlags,
    next_hop: Ip4NextHop,
}

pub struct Ip6RoutePath {
    sw_if_index: u32,
    table_id: u32,
    rpf_id: u32,
    weight: u8,
    preference: u8,
    behavior: IpRoutePathBehavior,
    flags: IpPathFlags,
    next_hop: Ip6NextHop,
}

pub struct Ip4RouteAddDelRequest {
    is_add: bool,
    is_multipath: bool,
    table_id: u32,
    prefix: Ipv4Net,
    paths: Vec<Ip4RoutePath>,
}

pub struct Ip6RouteAddDelRequest {
    is_add: bool,
    is_multipath: bool,
    table_id: u32,
    prefix: Ipv6Net,
    paths: Vec<Ip6RoutePath>,
}

pub struct Ip4RouteLookupRequest {
    table_id: u32,
    exact: bool,
    address: Ipv4Addr,
    prefix_length: u32,
}

pub struct Ip6RouteLookupRequest {
    table_id: u32,
    exact: bool,
    address: Ipv6Addr,
    prefix_length: u32,
}

pub struct Ip4RouteAddDelReply {
    status: IpRouteStatus,
    stats_index: u32,
}

pub struct Ip6RouteAddDelReply {
    status: IpRouteStatus,
    stats_index: u32,
}

pub struct Ip4RouteLookupReply {
    status: IpRouteStatus,
    route: Option<Ip4RouteRecord>,
}

pub struct Ip6RouteLookupReply {
    status: IpRouteStatus,
    route: Option<Ip6RouteRecord>,
}

pub struct Ip4RouteRecord {
    table_id: u32,
    stats_index: u32,
    prefix: Ipv4Net,
    paths: Vec<Ip4RoutePath>,
}

pub struct Ip6RouteRecord {
    table_id: u32,
    stats_index: u32,
    prefix: Ipv6Net,
    paths: Vec<Ip6RoutePath>,
}

pub enum IpRouteStatus {
    Ok,
    TableMissing,
    SourceMissing,
    PrefixInvalid,
    PathInvalid,
    InterfaceMissing,
    DpoClassMissing,
    CapacityExhausted,
    RouteMissing,
}
```

The two concrete method sets avoid a Rust family enum while retaining VPP's
separate IPv4/IPv6 lookup backends. Their request fields follow VPP's
`ip_route_add_del` and `ip_route_lookup` semantics:

| 方法 | 请求事实 | 所属 handler 的动作 |
| --- | --- | --- |
| `ip4.route.add_del` | `is_add`, `is_multipath`, external `table_id`, concrete IPv4 prefix, and one or more phase-one VPP path descriptions | decode core path facts to service `FibPath` values, use the fixed `FIB_SOURCE_API` source, validate `sw_if_index` through `InterfaceMain`, and invoke the IPv4 owner's FIB mutation. The handler does not construct a DPO; the IP path resolver creates or updates concrete objects when the source contributes. |
| `ip6.route.add_del` | same operation with a concrete IPv6 prefix | decode core path facts to service `FibPath` values, use the fixed `FIB_SOURCE_API` source, and invoke the IPv6 owner's FIB mutation. Direct `DpoId` input is reserved for owner-internal special-DPO operations. |
| `ip4.route.lookup` | external `table_id`, `exact`, concrete IPv4 address/prefix | perform non-forwarding FIB LPM/exact lookup and return the matched route's prefix, paths and stats index |
| `ip6.route.lookup` | external `table_id`, `exact`, concrete IPv6 address/prefix | perform non-forwarding FIB LPM/exact lookup and return the matched route's prefix, paths and stats index |

The payload contract is concrete and wire-visible, but it is a Hammer contract
rather than a claim of byte-for-byte compatibility with VPP's imported
`fib_path` message. VPP wire-only extension fields are rejected by these four
methods; they are not silently retained, and they cannot change
`FibPath<N, F>`. No route-dump method or multipart reply is part of this
migration. The existing one-request/one-reply envelope remains the complete
Binary API contract for these methods.

| 消息 | 字段 |
| --- | --- |
| `Ip4RouteAddDelRequest` | `is_add: bool`, `is_multipath: bool`, `route: { table_id: u32, prefix: IPv4 prefix, paths: repeated Ip4RoutePath }` |
| `Ip6RouteAddDelRequest` | `is_add: bool`, `is_multipath: bool`, `route: { table_id: u32, prefix: IPv6 prefix, paths: repeated Ip6RoutePath }` |
| `Ip4RoutePath` / `Ip6RoutePath` | `sw_if_index: u32`, `table_id: u32`, `rpf_id: u32`, `weight: u8`, `preference: u8`, `behavior: IpRoutePathBehavior`, `flags: IpPathFlags`, and concrete `next_hop: Ip4NextHop` or `Ip6NextHop`; no owner-specific extension fields |
| `Ip4RouteLookupRequest` | `table_id: u32`, `exact: bool`, `prefix: { address: bytes(4), length: u32 }` |
| `Ip6RouteLookupRequest` | `table_id: u32`, `exact: bool`, `prefix: { address: bytes(16), length: u32 }` |
| `*RouteAddDelReply` | `status: IpRouteStatus`, `stats_index: u32` |
| `*RouteLookupReply` | `status: IpRouteStatus`, `route: Option<{ table_id: u32, stats_index: u32, prefix, paths: repeated Ip4RoutePath or Ip6RoutePath }>` |

`IpRoutePathBehavior` is an IP Binary API input enum for phase-one path actions
(`normal`, `local`, `drop`, `udp_encap`, `icmp_unreach`, `icmp_prohibit`,
`source_lookup`, `dvr`, `interface_rx`, and `classify`). It is owned by the IP
plugin and is decoded before the service FIB seam; it is not a service FIB type,
an ICMP/TCP/UDP protocol number, or a next-hop protocol enum. There is no
service-level address union.
Each concrete route method decodes its own next-hop fields into its IP-owned
`Ip4NextHop` or `Ip6NextHop`, then supplies the core `FibPath<N, F>` to the
generic FIB. Owner-specific extensions are outside this phase-one payload; an
unsupported extension is rejected as `PathInvalid` rather than silently
retained in the core path. The IP owner resolves the supported core facts. Ordinary route
messages never carry a concrete `DpoId`;
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

The two VPP fields that are easy to conflate are kept at different seams:
`vl_api_fib_path_t.proto` (when a wire-compatible client needs it) describes
how the IP API encoded its next-hop payload and is consumed only by the IP
decoder; `dpo_proto_t`/`DpoProto` describes the packet graph protocol needed to
choose a DPO node and edge. The decoder never casts a wire next-hop protocol to
`DpoProto`, and the generic FIB never sees either wire encoding. The IP owner
chooses the graph protocol while projecting its resolved concrete DPO.

The route methods use the registered API source internally; a client cannot
submit an arbitrary source id. `table_id` is the external table name and is
never treated as a `fib_index`. Paths carry raw `sw_if_index`, concrete
owner-supplied next-hop facts, weight, preference and owner-defined path flags.
The IP
handler validates those facts and gives the generic FIB service-owned
`FibPath<N, F>` values;
the IP resolver in the projection step produces concrete `DpoId` identities. No DPO message
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
After plugin initialization, the daemon remains empty and the in-tree
`hammerctl` bootstrap command submits the same Binary API messages in order;
no second config-only route mutation API is introduced. Binary API's existing
envelope, context correlation, and handler ABI remain unchanged; these are new
plugin-owned methods and typed payloads.

### DPO dispatch and IP local protocol dispatch

These are separate contracts:

```text
DPO class + DpoProto  -> per-proto forwarding node (ip4-rewrite/ip6-rewrite)
IP protocol number     -> IP local-next slot -> plugin graph node
```

The DPO class determines the forwarding next edge for a DPO instance. It does
not decide which node handles an IP protocol number. The IP plugin owns the
local-next tables and exposes two VPP-shaped concrete APIs:

The service DPO core imports no `hammer-plugins::*` crate and stores no
plugin-owned type, object pool, general object-operation table, or capability.
It stores only class/node/edge metadata and compact identities. A plugin
contributes a DPO class through its owner init function, which invokes the
generated registration method and binds the returned runtime class key to its
concrete pool and operations. `DpoMain` records the supplied concrete `NodeId`
values and derives type/protocol graph edges, but never dereferences a plugin
object's `dpoi_index`. IP/ICMP dependency exists only in IP local-next and ICMP
error registration, never in the generic DPO registry.

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
| `mfib_table_t`, `mfib_entry_t` | `Ip4Main` / `Ip6Main` | separate multicast table/entry/path state, RPF/interface flags and replicate-DPO projection; never fields of the unicast `FibTable` |
| `fib_main_t`, `fib_table_t`, `fib_entry_t`, `fib_entry_src_t`, `fib_path_list_t` | `hammer-service::net::fib` | source contributions, per-entry source records, entry attributes, configured path-list sharing, recursive dependencies, back-walk and the current forwarding `DpoId`; service DPO owns generic objects and the concrete protocol owner supplies path interpretation |
| `fib_entry_delegate_t`, cover/track, attached export/import | `hammer-service::net::fib` | lazily created optional forwarding chains, covered-entry lists, tracker siblings, BFD state and cross-table cover propagation; updates are failure-atomic |
| `fib_path_ext_t` | source/feature owner of the extension | `(entry, source, path_index)` association and owner-specific resolve/stack/flush; extension data does not enter `FibPath<N, F>` |
| `fib_urpf_list_t` | `hammer-service::net::fib` | baked read-only interface/adjacency facts shared by forwarding objects |
| `dpo_vfts`, `dpo_nodes`, `dpo_edges` | `hammer-service::net::dpo` plus owner-local object operations | class slots, declared per-link nodes, derived class/proto edges, and compact identity; no plugin object bytes or generic lifecycle callbacks |
| `ip_adjacency_t`, `adj_nbr_*` | `hammer-service::net::dpo` plus protocol-specific producers | one shared generic adjacency layout per concrete address/rewrite instantiation with normal/incomplete/midchain/glean/multicast class slots; IP/tunnel/multicast owners supply the concrete address and rewrite types, child, and policy facts |
| `adj_midchain` / midchain delegate | tunnel or recursive producer plus shared adjacency owner | recursive target entry, child DPO, sibling, looped/admin state, rewrite and owner fixup; restack on target change and unstack to drop on invalid/looped state |
| `load_balance_t` | `hammer-service::net::dpo` | concrete bucket pool and hash selection; packet buckets are child `DpoId`s and service owner orders their dependencies |
| `load_balance_map_t` | `hammer-service::net::dpo` | supporting normalized weighted-bucket remapping; shared by load-balance objects and refilled on path-state back-walk; not a DPO class |
| `lookup_dpo_t` | `hammer-service::net::dpo` | one class-owner pool object with source/destination, configured/interface table, unicast/multicast and table identity facts |
| `receive_dpo_t` | `hammer-service::net::dpo` | one class-owner pool per concrete address type, with receive interface and producer-owned address semantics |
| `interface_rx_dpo_t` / interface-TX DPO | `InterfaceMain` | RX object pool and stateless TX wrapper around `sw_if_index`; TX next is instance-dependent |
| `ip_pmtu_t` / `ip_pmtu_dpo_t` | IP plugin forwarding owner | FIB-linked PMTU tracker and interposed PMTU DPO; ICMP and TCP use typed IP operations |
| `ip4-input` / `ip6-input` | IP plugin | concrete header validation and metadata preparation |
| `ip4-local` / `ip6-local` | IP plugin | local feature arc, checksum/source checks and IP-protocol local-next dispatch |
| `ip4-lookup` / `ip6-lookup` | IP plugin | concrete LPM lookup and post-LPM load-balance selection |
| `ip4-rewrite` / `ip6-rewrite` and adjacency siblings | IP plugin | concrete IP header policy, IP MTU check, address interpretation and TX interface publication over service-owned adjacency objects |
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
| `ip_route_add_del`, `ip_route_lookup` | `ip4.route.*` / `ip6.route.*` | source contributions and their forwarding projection; route dump is outside this migration |
| interface address add/del | `ip4.interface.address.add_del` / `ip6.interface.address.add_del` | address pool record plus connected/attached/local/glean/broadcast and uRPF contributions |
| `sw_interface_set_table` | `ip4.interface.table.bind` / `ip6.interface.table.bind` | per-interface lookup-table mapping and table-bind callbacks |
| IP flow-hash configuration | `ip4.table.flow_hash.set` / `ip6.table.flow_hash.set` | concrete table hash policy used after LPM |
| neighbor/ARP/ND add/del | `ip4.neighbor.add_del` / `ip6.neighbor.add_del` | IP-owned address/neighbor interpretation requests a service adjacency update; unresolved state publishes the incomplete adjacency subtype |
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
  -> DpoMain class/node tables
  -> DpoMain edge table (derived child/parent class-proto edge)

DpoId { type, proto, index, next }
  -> matching concrete owner pool or permanent singleton
  -> class-specific node/object operation
```

`DpoType` is the key used by `DpoMain` to select the graph-node table; the
concrete class owner uses the same key to select its typed operations. It does
not contain an object. `DpoId.index` is the index of the real object in the
pool belonging to that class. `LookupDpo`, `ReceiveDpo`,
`AdjacencyDpo`, and `LoadBalanceDpo` remain service-net object-bearing types.
`IpNullDpo` and `IpPmtuDpo` are IP-owner object types whose concrete state never
enters service net. `IpNullDpo` is backed by a fixed immutable action table;
`DpoId.proto` selects the concrete IP graph protocol and is not duplicated in
the object. `DropDpo` and `PuntDpo` remain the permanent singleton exception,
while `InterfaceTxDpo` wraps an existing `sw_if_index` and has no pool.

There is no service class record. `DpoMain` keeps only the class key, per-
protocol node slots, and derived edges. Formatting, memory accounting,
instance-dependent next-node lookup, uRPF, MTU, interpose, object mutation, and
dependency ordering remain owner-local. Adjacency subtype selection and
projection replacement are ordinary `&mut` operations on the concrete owner's
pool. Plain `DpoId` copies are independent identity values; a concrete owner
keeps each pool value alive while its published identities remain reachable.
The enclosing Binary API transaction provides worker publication ordering.

`DpoMain::stack` derives or reuses the edge from the child class/proto and the
parent `DpoId`, writes only the edge slot into a copy of the parent identity,
and returns that value. It never stores an object pointer in the edge table. The
concrete owner keeps the parent object alive for as long as the stacked child
can be reached.

Pool growth and index recycling follow VPP's `dpo_pool_barrier_sync` rule:
only the control owner may grow or recycle a worker-visible pool while the
worker barrier is held. A class owner removes the old concrete value before
recycling the pool slot, and no index is interpreted under a different class
key. The service FIB stores complete `DpoId` values but does not inspect object
bytes. The concrete backend performs projection replacement, path-list
replacement, forwarding replacement, and teardown through ordinary owner
mutation. It removes or retires the concrete value only after worker-visible
state no longer contains the identity. A plain `DpoId` copy in a path list or
bucket is only an identity copy, never an implicit owner. There is no generic
lifecycle interface and no service object store.

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
`table_id`, stats index and the configured concrete route-path list, matching VPP's
`ip_route_lookup` reply. Packet lookup is a separate IP data-plane operation:
the concrete IP node performs LPM in its forwarding backend, obtains the
load-balance value, selects a bucket and then follows the resulting child
`DpoId`; none of those data-plane identities are serialized in the route
lookup reply.

#### DSO and proc-macro contract

The runtime `PluginModule` ABI remains unchanged. A plugin contributes network
classes through the existing `RegistrationImage` and its `InitFunction`
inventory. The owner init function receives the already initialized `NetMain`,
invokes the generated `register_dpo_class` method, and binds the returned class
key to the plugin's concrete pool. `NetMain` does not own a loader or reach into
`PluginMain`; no network image, export type, object pointer, pool, or callback
surface is introduced.

`FibSource` and `DpoClass` are concrete proc-macro declaration faces, not
runtime dispatch. A `FibSource` derive emits only owner-local `NAME`, `PRIORITY`
and `BEHAVIOUR` constants. `DpoClass` may be derived only on the real DPO
object layout (including a generic layout such as `AdjacencyDpo<A, R>`); the
old name-only marker type is forbidden. The derive emits one owner-local
`register_dpo_class` function which calls `DpoMain::register_new_type` and
returns its runtime `DpoType`. It never emits a class name, static class key,
registration record, pool, operation table, lifetime token, generic object
enum, family parameter, or `dyn` object.

The optional `nodes` attribute declares the per-protocol arguments needed by
that function. Each right-hand side is an identifier bound by the owner to an
already registered concrete `NodeId`; it is not a node name, node lookup, or a
stringly-typed registry entry:

```rust
#[derive(DpoClass)]
#[dpo_class(nodes = [
    (DpoProto::IP4, ip4_null_node),
    (DpoProto::IP6, ip6_null_node),
])]
pub struct IpNullDpo {
    action: IpNullAction,
}

#[derive(DpoClass)]
#[dpo_class(nodes = [
    (DpoProto::IP4, ip4_pmtu_node),
    (DpoProto::IP6, ip6_pmtu_node),
])]
pub struct IpPmtuDpo {
    proto: DpoProto,
    pmtu: u16,
    published_roots: u16,
    stacked: DpoId,
}
```

Conceptually, the first declaration expands to the following direct owner
method (the exact generated identifier hygiene is an implementation detail):

```rust
impl IpNullDpo {
    pub fn register_dpo_class(
        dpo_main: &mut DpoMain,
        ip4_null_node: NodeId,
        ip6_null_node: NodeId,
    ) -> RuntimeResult<DpoType> {
        dpo_main.register_new_type(&[
            (DpoProto::IP4, ip4_null_node),
            (DpoProto::IP6, ip6_null_node),
        ])
    }
}
```

`DpoMain::register_new_type` validates the node list, allocates one monotonic
class key, stores its `(DpoProto, NodeId)` bindings, and returns that key. It
does not inspect the object fields or install a pool. The IP init function
obtains the node IDs from the graph owner, calls
`IpNullDpo::register_dpo_class` and `IpPmtuDpo::register_dpo_class`, then stores
the two returned keys beside the IP-owned pools. `IpNullDpo` therefore uses the
same macro even though VPP names its equivalent built-in `DPO_IP_NULL`; Hammer
keeps the key opaque and does not expose a static built-in enum.

Its only public class-allocation operation has the VPP-shaped contract below;
the returned key is the only value that crosses back to the concrete owner:

```rust
impl DpoMain {
    pub fn register_new_type(
        &mut self,
        nodes: &[(DpoProto, NodeId)],
    ) -> RuntimeResult<DpoType>;
}
```

An empty node list is valid only for a class with no originating graph node, but
duplicate `DpoProto` entries and invalid `NodeId` values are rejected before the
key is allocated. There is no separate `register_node` operation:
VPP installs the complete per-protocol node set as one class registration, and
the macro preserves that atomic registration boundary.

The owner call is explicit and occurs after graph node materialization:

```rust
let ip_null_type = IpNullDpo::register_dpo_class(
    &mut net.dpo_main,
    ip4_null_node,
    ip6_null_node,
)?;
let ip_pmtu_type = IpPmtuDpo::register_dpo_class(
    &mut net.dpo_main,
    ip4_pmtu_node,
    ip6_pmtu_node,
)?;
ip_main.ip_null_type = ip_null_type;
ip_main.ip_pmtu_type = ip_pmtu_type;
```

The two node pairs are distinct class registrations; a DPO class key never
selects a node by string or by an IP-family enum. The owner may use the same
`DpoProto` in different class registrations because the class key remains the
first dispatch component.

For a shared object layout with several VPP subtype keys, such as lookup or
adjacency, `#[dpo_class]` without `nodes` emits the same function with one
`&[(DpoProto, NodeId)]` argument. The owner invokes it once per subtype and
keeps each returned `DpoType` in its own subtype binding. This is the VPP
`dpo_register_new_type` pattern without a marker type or a second registry
record. The object owner still implements all typed operations and owns the
pool; the macro only supplies the direct class-allocation call.

The existing plugin image is initialization input. Runtime route entries, paths,
table maps, DPO instances, interface instances, queues, and feature-chain state
are all created by their owning Main during init and are never serialized into
an additional network image.

### ICMP ownership and Path MTU

The ICMP plugin owns ICMP wire parsing, type dispatch, echo/error generation,
and ICMP graph nodes. The ICMP-specific parser currently living in
`hammer-service::net::pmtu` moves into the ICMP plugin. The PMTU authority also
moves to the IP plugin to match VPP's `ip_pmtu_t`/`ip_pmtu_dpo_t`: it is a
FIB-linked forwarding policy, not a generic service cache. ICMP parses a
Fragmentation Needed message and submits a typed PMTU update event to the
existing control-plane Binary API dispatcher. The dispatcher invokes the IP
owner's `ip_path_mtu_update` operation inside the sole
`worker_thread_barrier_sync!` scope; TCP consumes the typed IP PMTU lookup after
that publication. `hammer-service::net` contains neither ICMP bytes nor PMTU
policy, and no second cross-worker synchronization protocol is introduced.

The IP owner keeps the FIB-linked tracker, the interposed DPO and the
IP-null-action table as concrete state. `IpRoutePathBehavior` remains an
IP-owned input fact until the source owner projects it to an `IpNullDpo`:
`Drop` selects the discard action, while `IcmpUnreachable` and
`IcmpProhibit` select their corresponding actions. `IpNullDpo` is an IP-plugin
DPO class, not a service-net type; it stores only the action because the
`DpoId` already carries `DpoProto`. Only the resulting `DpoId` enters the
service FIB/DPO graph.

```rust
pub struct IpNullDpo {
    action: IpNullAction,
}

pub enum IpNullAction {
    Drop,
    SendIcmpUnreachable,
    SendIcmpProhibit,
}

pub struct Ip4NullNode;
pub struct Ip6NullNode;

pub struct IpPmtu {
    fib_entry: u32,
    sibling: u32,
    flags: IpPmtuFlags,
    configured_mtu: u16,
    parent_mtu: u16,
    operational_mtu: u16,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct IpPmtuFlags: u8 {
        const ATTACHED = 1 << 0;
        const REMOTE = 1 << 1;
        const STALE = 1 << 2;
    }
}

pub struct IpPmtuDpo {
    proto: DpoProto,
    pmtu: u16,
    published_roots: u16,
    stacked: DpoId,
}

pub struct Ip4PmtuNode {
    runtime_data: NodeRuntimeData,
}

pub struct Ip6PmtuNode {
    runtime_data: NodeRuntimeData,
}
```

`IpNullDpo` values are immutable entries in the IP owner's fixed action table;
there is no per-route allocation and no `proto` field in the object. The
`DpoId` published for an IPv4 or IPv6 route carries the matching `DpoProto` and
the action-table index. `Ip4NullNode` and `Ip6NullNode` are separate concrete
graph nodes, matching VPP's `ip4-null` and `ip6-null` nodes; they share only a
private generic packet-loop helper inside the IP plugin.

Each null node executes this sequence for every buffer:

1. Read the TX DPO index from the packet metadata and obtain the immutable
   `IpNullDpo` action from the matching IP owner's table. The class owner has
   already validated the class/index pair before publication; an invalid pair
   is a control-plane invariant failure, not a packet fallback.
2. Start with the `drop` next edge. For `SendIcmpUnreachable` or
   `SendIcmpProhibit`, consult worker-owned, time-windowed hash/bitmap state so
   repeated null-route packets cannot amplify an ICMP response. A suppressed
   response takes the drop edge and performs no allocation or map lookup.
3. On an allowed response, write the concrete ICMP type/code into the existing
   packet error metadata and enqueue the buffer to the registered IP4/IP6 ICMP
   error next. IPv4 uses destination-unreachable host-unreachable or
   administratively-prohibited codes; IPv6 uses no-route-to-destination or
   administratively-prohibited codes. If the ICMP error next is not installed,
   the node keeps the drop edge.
4. Preserve the node's trace identity as the DPO index and enqueue exactly one
   next edge. The node does not touch FIB source records, path extensions,
   delegates, DPO class tables or device queues.

The two nodes therefore have identical control flow but concrete IP header and
ICMP metadata policies. This is the only shared implementation seam; no
`IpFamily`, protocol selector field, dynamic node or generic DPO object is
introduced.

`IpPmtuDpo` is registered as a concrete IP-owned DPO class and retains its
stacked child identity while the tracker updates attached adjacency limits.
`published_roots` is the owner-local lifetime count corresponding to VPP's
`ipm_locks`; it counts published identity roots, is not a mutex, and has no
public lock/unlock operation. The owner increments it before publishing a new
root and decrements the old root only after the barrier publication has made it
unreachable. Zero retires the object and its stacked child after worker
quiescence. The DPO is not a service cache or a generic DPO record.

`Ip4PmtuNode` and `Ip6PmtuNode` are separate concrete graph nodes with one
private, statically dispatched IP-plugin orchestration. For each buffer they:

1. read the PMTU DPO index from the TX DPO metadata and resolve it through the
   IP owner's typed pool;
2. replace the TX index with `stacked`'s index and use its cached next edge;
3. record the PMTU and the buffer-chain packet length in the node trace;
4. call the concrete IPv4 or IPv6 fragment operation with the PMTU. Fragment
   buffers use the existing buffer allocator/frame contract; this is a
   PMTU-node operation and not an allocation in ordinary LPM lookup;
5. when IPv4 reports `DONT_FRAGMENT_SET`, write fragmentation-needed ICMP
   metadata and select the registered IPv4 ICMP-error next; otherwise a
   fragmentation error selects the concrete IP drop next;
6. on successful fragmentation, free the original buffer chain and enqueue all
   generated fragments to the stacked next; on failure, retain/enqueue the
   original buffer on drop and record the typed fragment error;
7. enqueue each generated or retained buffer exactly once. The node never walks
   FIB source records, path extensions, delegates or device queues.

The two nodes share only this private generic control flow. IPv4 and IPv6 keep
their concrete fragment policy and ICMP-error metadata, matching VPP's
`ip4-pmtu-dpo` and `ip6-pmtu-dpo` registrations.

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
and only then removes the interface pool entries. The deletion operation is
requested through the device/interface Binary API; it is not a direct packet
path call.

### Synchronization and failure handling

Binary API handlers validate all participants before mutation. A failed request
leaves sibling registries and worker-visible tables unchanged. During the
dispatcher-owned `worker_thread_barrier_sync!` scope, the service FIB mutates
its long-lived source/entry/path graph and updates the concrete forwarding
backend incrementally. There is no complete-table build step and no handler-
owned barrier entry/exit.

Expected packet failures remain node-local errors and next-arc decisions.
Configuration, missing plugin APIs, invalid source/DPO relationships, stale
interface indices, and resource exhaustion return owner-local typed errors.
No packet path allocates an error or formats a message.

## ADR Review (2026-09-03)

Verdict: accepted after the source-record and DPO-retention corrections below.
The acceptance list verifies these contracts during implementation; it does not
introduce another architecture choice.

| Finding | Evidence | Impact | Resolution |
| --- | --- | --- | --- |
| DPO identity width was underspecified | VPP `dpo_id_t` is `{type, proto, next, index}` and is asserted to fit in one `u64`; the current Hammer `Dpo<N>` is generic and is not that ABI shape | A generic next type can silently make atomic identity publication impossible and lets callers confuse an identity with an object | The target is one non-generic `DpoId` with `u8/u8/u16/u32` fields and an 8-byte size assertion; current `Dpo<N>` is a breaking migration item |
| Projection ownership was hidden behind a broad backend sentence | VPP FIB passes a complete DPO identity to the protocol backend, while concrete DPO pools remain class-local | A service FIB implementation could accidentally own or erase class objects, recreating the rejected object store abstraction | `project_forwarding` returns `DpoId`; layouts/class rules live in service net, the concrete instantiating owner owns each pool and its retention roots, and service FIB stores only the identity |
| The class key was described as a closed static type | VPP reserves built-in `dpo_type_t` values but allocates plugin classes with `dpo_register_new_type()`; the key selects `dpo_vfts`/`dpo_nodes` and does not contain object bytes | A public static IP/DPO class table would prevent independent plugin DPO classes and make service net own IP knowledge | `DpoType(u8)` is opaque; `DpoMain` allocates keys monotonically for core and plugin registrations, while concrete owners retain the returned key |
| Next-hop and path flags were mixed into the service/FIB seam | VPP's API `fib_path.proto` exists only to decode a wire address union; it is converted before the FIB path is installed, while `dpo_proto_t` selects graph nodes | A service `FibPathNhProto`/`FibPathNh` or fixed `FibPathFlags` would couple generic FIB and DPO to IP address and route-policy representation | Wire path payloads are concrete `Ip4RoutePath`/`Ip6RoutePath`; the service stores only `FibPath<N, F>` with owner-supplied next-hop and flags values. `DpoProto` remains solely the graph protocol discriminator |
| Address storage was reduced to an erased byte value | VPP's `receive_dpo_t` and adjacency carry address-shaped facts, but their C union is an implementation choice; a Rust service seam must preserve the producer's concrete value and interpretation | `Box<[u8]>` hides the address contract, forces runtime validation outside the type, and makes a copied DPO identity unable to state which concrete pool owns its index | Remove the empty `Address` trait. `ReceiveDpo<A>` and `AdjacencyDpo<A, R>` store producer-owned concrete values in monomorphized pools selected by `DpoType`; local pool operations add only the bounds they need. No `dyn`, byte erasure, address-family enum, or `DpoProto` conversion is added |
| Protocol-specific path data leaked into the generic path shape | VPP keeps the core `fib_path_t` separate from `fib_path_ext_t`; protocol-specific facts are decoded or retained by their owner | A concrete extension or flag field makes the service FIB depend on one protocol and falsely turns one extension or policy set into a universal path fact | `FibPath<N, F>` and the phase-one IP route payloads contain no service-owned path-policy bits or owner-specific extension data; each owner defines its own flags and extension lifecycle |
| DSO image loading was assigned to the wrong layer | Current symbol lookup is `GlobalMain -> PluginMain::get_plugin_symbol`; `hammer-runtime` cannot import service-owned types | `NetMain` cannot independently load a DSO without violating dependency direction | The existing `RegistrationImage` is loaded through `GlobalMain`/`PluginMain`; its ordered init functions call `NetMain` directly, and the runtime only keeps the DSO alive |
| The acceptance list mixed the whole target with one migration | The current checkout has no service FIB, split ICMP DSO, or Net proc-macro implementation, while the old `FibTableBuilder` and fixed device path remain | One issue would be unreviewable and would encourage speculative scaffolding | This ADR fixes the split seams, concrete FIB/DPO identity behavior, Binary API ownership, and focused tests. MFIB extensions, route dumps, and additional protocol classes are explicitly outside this change and require their own design record |
| A duplicate service-side DPO class/reference layer was added without a real owner | VPP keeps class/node arrays, while object pools and operations stay with each concrete class | The layer duplicates `DpoMain` state and invites a generic C-style lifetime API | Remove the duplicate layer and callbacks; `DpoMain` stores class/node/edge metadata directly and each concrete owner orders pool retirement |
| Per-entry source contribution state was implicit | VPP `fib_entry_t` stores a vector of `fib_entry_src_t`; that record carries `fes_path_exts`, the source path-list link, entry/source flags, `fes_ref_count`, and source-specific union data | Without an explicit record, source precedence, duplicate add/remove, cover tracking, owner payload association, and extension replacement have no owner | Add owner-instantiated `FibEntrySrc<N, F, SourceData, PathExt>` records with the complete common fields and direct cover tuple plus optional interpose DPO; entries retain `(FibSource, u32)` slots, and the behavior tag dispatches to the matching typed pool without a central union or `dyn` |
| Cover/interpose facts were modeled as source behavior | VPP selects a behavior from `fib_source_get_behaviour` and separately overrides it for `FIB_ENTRY_FLAG_INTERPOSE`; cover/sibling fields are facts used by several behaviors | An enum variant made `Cover`/`Interpose` look like mutually exclusive behaviors and could discard RR/interface/adjacency-specific lifecycle rules | Delete `FibEntrySrcRelation`; `FibEntrySrc` stores the direct `cover` tuple and optional `interpose_dpo`, while `FibSource.behavior` remains the only behavior selector |
| Copyable DPO identity had no last-reference mechanism | VPP locks the new `dpo_id_t` before atomically replacing the old one and unlocks the previous identity; copies can live in FIB, buckets and child objects | `Copy` plus a worker barrier cannot discover the last copied identity, so “Drop handles it” would permit premature retirement | Every concrete pool owner counts published roots, retains the new root before replacement, releases the old root after publication, and retires only at zero after worker quiescence; no generic reference type or manual unlock API |
| DPO pool ownership was ambiguous | VPP class/node metadata is separate from class-local object pools and operations | A service-owned pool for a generic layout would either own foreign payloads or require an erased object store | `DpoMain` owns metadata; service owns only service-valued pools such as lookup/load-balance; the module instantiating `A`/`R` owns that concrete pool, root counts and object operations |
| FIB priority and flags were not VPP-shaped | VPP `fib_source_t` carries one `fib_source_priority_t` byte, while graph-state flags and protocol path flags are separate bit masks | Separate priority class/slot fields add a non-VPP ordering model; tuple/u8 flags hide width and bit-value contracts; a fixed service `FibPathFlags` would couple IP policy to the FIB | `FibSource` now has one `priority: u8`; service graph flags use `bitflags!` with explicit widths, while each business owner defines its own path flags (`IpPathFlags` for IP); excluded MPLS/BIER/PW path bits remain out of scope |
| IP route path behavior leaked into service FIB | Path behavior is a wire/API decode concern; service FIB stores configured path facts and owner-supplied next-hop and flags values | `FibPathType` or a fixed `FibPathFlags` made service net own IP actions and mixed them with generic path data | Replace them with IP-owned `IpRoutePathBehavior` and `IpPathFlags`; decode both before constructing `FibPath<N, F>`, retain action data in the IP source owner, and project it through the IP-owned `IpNullDpo` class |
| IP-null ownership and node behavior were underspecified | VPP's `ip_null_dpo.c` combines a fixed action record with per-protocol `ip4-null`/`ip6-null` nodes, rate limiting and ICMP error nexts | Omitting the object or its node contract either loses explicit-drop semantics or leaves the ICMP/error path undefined | Keep `IpNullDpo` and its immutable action table in `hammer-plugins/net/ip`; define separate `Ip4NullNode`/`Ip6NullNode` adapters, worker-local rate limiting, exact ICMP mappings and drop fallback |
| PMTU DPO/node shape was incomplete | VPP `ip_pmtu_dpo_t` contains protocol, PMTU, lock count and stacked DPO; `ip_pmtu_dpo_inline` rewrites TX state, traces packet size, fragments, handles IPv4 DF and enqueues generated/original buffers | A parent-only DPO sketch could lose stacked dispatch, lifecycle accounting or fragment/error ownership | `IpPmtuDpo` now names `stacked` and owner-local `published_roots` (the `ipm_locks` reason, not a mutex); `Ip4PmtuNode`/`Ip6PmtuNode` specify the complete VPP fragment, DF/ICMP, drop, trace and buffer enqueue flow |
| DPO/FIB layout and hot-path cost were unspecified | VPP aligns pool elements and places switch-path fields first; `load_balance_t` keeps four inline buckets and fits in one cacheline, while `ip_adjacency_t` separates hot, rewrite and control groups; Hammer already has 64-byte alignment and opaque-budget assertions | Without an explicit contract, a Rust port can add fat-pointer indirection, map lookups, packet-path allocation, or oversized metadata while still appearing ownership-correct | Reuse `CacheLineAlignMark`, keep `DpoId` at 8 bytes, inline four load-balance buckets, keep overflow storage owner-local, forbid control-plane traversal/allocation in packet lookup, and require concrete size/alignment assertions; no performance number is claimed without a release benchmark |

The DPO identity, address abstraction, DSO loading, concrete-owner lifetime and
layout/packet-path findings are current-project/VPP-backed conclusions. The
phased delivery decision and the absence of a numeric performance target are
project decisions based on the current repository, not VPP requirements.

## Non-goals

- Do not split IPv4 and IPv6 into separate DSOs.
- Do not add a unified protocol-family registration abstraction, a generic
  registration hook, an erased network object, or dynamic object dispatch.
- Do not make service choose a fixed IP next for every device.
- Do not publish routes by direct config mutation or an owner-specific publish
  handle; route and forwarding changes use the Binary API methods defined here.
- Do not change the existing Binary API envelope, context correlation, or
  handler function-pointer ABI. Adding the IP route method payloads is in scope.
- Do not put PMTU policy or a PMTU DPO in generic service net. The IP-owned
  PMTU move and the dispatcher-owned publication contract are in scope; no
  second cross-worker synchronization protocol is introduced.

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
| 类型 | `hammer-plugins/net/icmp::IcmpMain` | ICMP owner-local Main，保存 ICMP 控制状态 | 新 DSO；无旧 ABI 兼容层 | DSO lifecycle and global access integration test |
| 类型 | `hammer-plugins/net/ip::{Ip4Main,Ip6Main}` | 两个 concrete implementation Main；分别保存 lookup-main、unicast FIB、MFIB owner、table map、per-interface bind 和本地 feature/local-next 状态；本 ADR 只规定 unicast FIB | 从当前混合 `IpMain` 控制面拆出；不拆为两个 DSO | owner lifecycle and per-proto lookup tests |
| 类型 | `hammer-plugins/net/ip::IpPathFlags` | IP-owned `bitflags!` path-policy bits (`u32`), including resolve, local/drop, interface-RX, source-lookup, encapsulation, ICMP, classify and glean actions | 从 service `FibPathFlags` 移出；Binary API 与 `FibPath<N, F>` 使用具体 `IpPathFlags` | bit-width/value and IP route decoding tests |
| 类型 | `hammer-service::net::dpo::{DropDpo,PuntDpo}` | service-owned stateless DPO classes；per-protocol singleton IDs | 新增 concrete class surfaces；不通过统一 `DpoKind` 兼容 | singleton identity and graph-next tests |
| 类型 | `hammer-service::net::dpo::{ReceiveDpo<A>,AdjacencyDpo<A,R>}` | 通过 producer-owned concrete `A`/`R` 形成静态 DPO layout；net 只保存并借用值，不定义 `Address` trait、canonical 方法、family tag 或 wire encoding | 新增 generic net seam；每个 class owner 以具体 `A`/`R` 实例化自己的 pool，旧 IP-owned fixed byte/address fields 删除 | compile-time generic pool/type checks and concrete producer validation tests |
| 类型 | `hammer-service::net::fib::FibEntrySrc<N,F,SourceData,PathExt>` | owner-instantiated per-source record；保存 `path_exts`、path-list link、entry/source flags、`source`、重复添加 `ref_count`、直接 `cover` 元组与可选 `interpose_dpo`、owner-supplied `SourceData` | 新增 service FIB record；`FibEntry` 只保留 `(FibSource, u32)` source slots 以容纳异构具体记录；cover/sibling facts 不再由额外类型承载，也不使用中心 union 或 `dyn` | source precedence, duplicate add/remove, source-data/cover/interpose validation, extension replacement, cover/interpose, and failure-atomic lifecycle tests |
| 类型 | `hammer-service::net::fib::{FibEntryFlags,FibEntrySrcFlags,FibPathListFlags}` 与 `hammer-service::net::dpo::LoadBalanceFlags` | 使用 `bitflags!` 的透明整数 flags；分别为 entry `u16`、source `u16`、path-list `u8`、load-balance `u8`，位值对齐 VPP；路径策略位不在 service net 定义 | 替代手写 tuple/裸 flags；无 wire 兼容层，service 只处理自身图状态 | compile-time width/bit-value assertions and FIB/DPO state tests |
| 类型 | `hammer-service::net::dpo::{LookupDpo,ReceiveDpo}` | service-owned generic layouts and class rules；`ReceiveDpo<A>` 直接保存 producer-owned `A`，具体 class owner 绑定其 monomorphized pool | 新增 service DPO class layouts；class owner/local node consume typed pool | object allocation, owner retirement, concrete type checks, and graph-next tests |
| 类型 | `hammer-service::net::dpo::{AdjacencyDpo,LoadBalanceDpo}` | generic layouts and class rules；`AdjacencyDpo<A, R>` 的 normal/incomplete/midchain/glean/mcast 使用同一 layout 的 class keys，并保存 producer-owned `A` next-hop 与 `R` rewrite；具体 `A`/`R` pool owner 负责 midchain restack/unstack、child ordering 和 published-root retention；load-balance 热字段前置、四个 child `DpoId` inline、power-of-two mask，超过四个 bucket 使用 contiguous owner-local overflow storage，并保留 baked uRPF facts | 替代 IP-owned `Adjacency`/`LoadBalance` 构造面；不新增 subtype pool 或 load-balance-map DPO class；具体布局需通过 size/alignment/offset assertions | DPO pool, subtype producer, concrete address/rewrite type checks, midchain restack, uRPF/map update, layout and projection integration tests |
| 类型 | `hammer-service::net::FibTableBackend` | 唯一的静态 dispatch seam：控制面 prefix 到 `fib_entry` index、packet address 到 entry `dpoi_index: u32`、cover replacement，以及 owner-specific projection；`project_forwarding` 返回 `DpoId`，trait 不接收 plugin pool 或 erased object operation | `Ip4FibBackend`/`Ip6FibBackend` 各自实现；无 `dyn`、额外 forwarding adapter 或 family facade | compile-time implementation and projection failure-atomicity checks |
| 类型 | `hammer-plugins/net/ip::{Ip4RewriteNode,Ip6RewriteNode}` | 具体 IP4/IP6 adjacency rewrite graph node；消费 service-owned `AdjacencyDpo`，分别处理 checksum/TTL/IP MTU | 替代单一 `AdjacencyRewriteNode`；同一 IP DSO | graph execution and per-proto rewrite tests |
| 类型 | `hammer-plugins/net/ip` 私有 `RewriteProtocol`/`process_rewrite_frame` 核心 | 静态泛型共享 adjacency 读取、MTU、TX metadata、错误分类、trace、next 编排；`Ip4Rewrite`/`Ip6Rewrite` 提供具体策略 | 不进入 service 或 DPO ABI；两个 node adapter 共享一套实现 | compile-time generic dispatch and behavior parity tests |
| 类型 | `hammer-plugins/net/ip::{IpNullDpo,IpNullAction,Ip4NullNode,Ip6NullNode}` | IP-owned fixed action records and concrete null graph nodes；`IpNullDpo` only stores `action`, while `DpoId.proto` carries IP4/IP6；nodes perform VPP-equivalent rate limiting, ICMP error metadata setup and drop/ICMP next selection | 新增 IP DPO/node surface；不向 `hammer-service::net::dpo` 搬运 IP action 或 node state | null-action projection, rate-limit, ICMP mapping, fallback-drop and graph-next tests |
| 类型 | `hammer-plugins/net/ip::{IpPmtu,IpPmtuFlags,IpPmtuDpo,Ip4PmtuNode,Ip6PmtuNode}` | VPP `ip_pmtu_t` FIB tracker 与 `ip_pmtu_dpo_t` 实际对象池；`IpPmtuFlags` 使用 `bitflags!` 的透明 `u8`（`ATTACHED`、`REMOTE`、`STALE`）；DPO 保存 `proto`、`pmtu`、`published_roots`（对应 VPP `ipm_locks` 的生命周期计数）和 `stacked: DpoId`；两个 concrete PMTU nodes 共享私有编排，分别执行 IPv4/IPv6 fragment、DF/ICMP-error/drop 分支和 trace | 从 `hammer-service::net::pmtu::PathMtuCache` 迁移到 IP owner；ICMP/TCP 改用 typed IP seam；无手工 `unlock` 或 mutex API | PMTU DPO fields, flag width/values, root-retirement, fragment success/failure, DF ICMP and adjacency MTU/FIB back-walk tests |
| 类型 | `hammer-plugins/net/ip::{Ip4RouteAddDelRequest,Ip6RouteAddDelRequest,Ip4RouteLookupRequest,Ip6RouteLookupRequest}` | Binary API 的具体 IPv4/IPv6 请求；包含 `is_add`/`is_multipath`、外部 `table_id`、具体 prefix/address 和 paths/exact；route add/delete 的 source 由 handler 固定为 `FIB_SOURCE_API`，不在 wire payload 暴露 | 新 Binary API payload；不引入统一 family 枚举 | prost encode/decode and invalid-field tests |
| 类型 | `hammer-plugins/net/ip::{Ip4RouteAddDelReply,Ip6RouteAddDelReply,Ip4RouteLookupReply,Ip6RouteLookupReply}` | 返回 owner-defined status、stats 和匹配 route/path facts；不序列化 selected DPO；状态不复用 display string | 新 payload；envelope status 仍由 `hammer-ipc` 定义 | reply status and context tests |
| 类型 | `hammer-plugins/net/ip::{Ip4RoutePath,Ip6RoutePath,IpRoutePathBehavior,IpRouteStatus}` | concrete IP-owned Binary API path payloads and phase-one path behavior; decoded into service `FibPath<Ip4NextHop, IpPathFlags>`/`FibPath<Ip6NextHop, IpPathFlags>`; no service-level path-behavior enum, next-hop protocol, address union, or owner-specific extension | 新 Binary API nested messages/enums；无旧 wire 兼容层；本迁移不提供 owner-specific extension 字段 | prost schema and status mapping tests |
| API | `#[derive(FibSource)]` / `#[derive(DpoClass)]` | `FibSource` 生成 owner-local source constants；`DpoClass` 只能派生在真实 DPO object layout 上，并生成 `register_dpo_class(...)`，直接把 owner 提供的 `(DpoProto, NodeId)` 参数交给 `DpoMain::register_new_type`；不生成 class name、static key、registration record、pool 或 callback table | 替代手写 forwarding registration arrays；`IpNullDpo` 和 `IpPmtuDpo` 均通过同一 derive 注册，shared layout 通过 owner 提供的 node slice 为每个 subtype 注册 | proc-macro expansion, DPO class allocation, and DSO inventory test |
| API | `ip4.route.add_del` / `ip6.route.add_del` | Binary API handler；按 VPP add/delete + multipath 语义增量修改 FIB graph 和 forwarding backend | 新方法名；客户端迁移到具体方法 | end-to-end Binary API route mutation test |
| API | `ip4.route.lookup` / `ip6.route.lookup` | Binary API handler；按 `table_id` 和 `exact` 读取 non-forwarding FIB entry，返回匹配 route/path/stats facts；不返回 selected DPO | 新方法名；客户端迁移到具体方法 | LPM/exact and route-facts reply test |
| API | `ip4.table.*` / `ip6.table.*` | table add/del/allocate/flush；维护 external `table_id` 到 concrete `fib_index` 的 owner-local map | 新 Binary API methods；不改变 Binary API envelope | table lifecycle and index-separation test |
| API | `ip4.interface.address.add_del` / `ip6.interface.address.add_del` | 通过 interface source 写入地址 pool，并生成 connected/attached/local/glean/broadcast/uRPF contributions | 新 Binary API methods；不允许直接写 lookup backend | address contribution and back-walk test |
| API | `ip4.interface.table.bind` / `ip6.interface.table.bind` | 绑定 `sw_if_index` 到 concrete table；触发 table-bind callbacks | 新 Binary API methods；`sw_if_index` 与 `fib_index` 保持分离 | bind callback and lookup-table test |
| API | `ip4.neighbor.add_del` / `ip6.neighbor.add_del` | 由 IP-owned ARP/ND adapter 解析地址并请求 service DPO owner 更新 adjacency pool 和 `FIB_SOURCE_ADJ` contribution | 新 Binary API methods；未解析邻居使用 incomplete adjacency class | neighbor-to-adjacency projection test |
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
| 类型 | `hammer-service::net::DpoProto`, `DpoType`, `DpoId` | 从 IP plugin 移入 service net；`DpoProto` 是 VPP graph protocol key，`DpoType` 是 `DpoMain` 运行时分配的 opaque class key，不是封闭 Rust enum；`DpoId` 是非泛型 Rust `Copy` identity，使用私有 `u64` 保持 VPP `{ type, proto, next, index }` 的 8-byte size/alignment 形状，不使用 `repr(C)`；`DpoMain` 直接持有 class/node/edge state，具体 class owner 负责 index 校验、published-root retention 和 pool retirement；移除对 `IpVersion` 的转换依赖 | 所有 callers 改为显式 class registration；源码级 breaking change | workspace compile, dynamic class allocation, identity-size/alignment assertion, and DPO behavior tests |
| 类型 | `hammer-plugins/net/ip::{Adjacency,AdjacencyRewrite,LoadBalance}` | 删除 IP-owned objects；以 service-owned `AdjacencyDpo`/`LoadBalanceDpo` pool 替代；Adjacency subtype 通过 class key 选择，不新增 subtype pool；IP 只保留地址验证/编码、邻居策略和 rewrite policy | 删除旧 IP forwarding 构造面；IP lookup/rewrite 改用 service DPO owner API；固定地址/重写数组不迁移 | service DPO pool, subtype producer, address/rewrite validation, and generic FIB projection integration test |
| 类型/API | `hammer-plugins/net/ip::forwarding::DpoKind` 与通用 `Dpo` 构造入口 | 删除泛化 kind-to-index/通用构造路径；具体 DPO 通过 `From` 生成 `DpoId` | breaking source migration；不保留 generic alias | compile-time API removal and concrete conversion tests |
| 类型 | `ForwardingMetadata` | 移入 service net，仅保留 DPO/FIB facts 与 TX interface contract；不携带 adjacency/load-balance对象 | IP lookup/rewrite 改用 service metadata | lookup/rewrite integration test |
| 类型 | `hammer-plugins/net/ip::AdjacencyRewriteNode` | 拆为 `Ip4RewriteNode`/`Ip6RewriteNode`；DPO class 只保存 service metadata，节点通过私有泛型核心共享编排并保留 IP-specific policy | 类型迁移为删除旧项 + 新增两项；不保留旧 node alias | per-proto graph inventory and behavior test |
| 类型 | `FibSource`, `FibEntry`, `FibPathList`, `FibPath`, `FibTable` | 从 IP plugin 的快照 builder 改为 service-owned 增量 source/entry/path graph；`FibPath<N, F>` 的 `N` 与 `F` 由具体 backend/业务 owner 提供，service 不定义 next-hop protocol/union/path-policy flags，也不把扩展塞进共享 path；`FibEntry` 通过 `(FibSource, u32)` slots 连接 owner-instantiated `FibEntrySrc<N,F,SourceData,PathExt>` records；entry delegates cover optional chains, trackers, BFD and attached import/export；`FibTable` 静态组合唯一的 `FibTableBackend` seam，entry 保存当前 forwarding-chain `DpoId`，backend 使用其 identity 更新 concrete forwarding store | IP4/IP6 backend 分别实现并提供 `IpPathFlags`；无 family facade 或独立 forwarding adapter；现有 route snapshot callers 迁移为 source/path operations；异构 source payloads remain in typed owner pools, not a service union or erased map | LPM, source precedence, behavior dispatch, source-data/cover/interpose validation, path-extension replacement, delegate/cover tracking, back-walk, and DPO projection tests |
| 类型 | `IpMain` | 改为 `OnceLock<IpMain>` owner；移除 FIB contributions、table handle 和 graph publication；只持有 generic IP protocol 与 TCP/UDP port registries | 删除 `ArcSwapOption<IpMain>`；调用方迁移到 `init/global`，并按 `Ip4Main`/`Ip6Main` 访问 concrete table | initialization and owner-separation test |
| 类型 | `hammer-service::binary_api::BinaryApiMethodEntry` dispatch contract | FIB-touching methods 保持 `mp_safe = false`；由 dispatcher 统一进入宏，handler 不再承担同步职责 | 现有 envelope/ABI 不变；方法注册迁移 | mp-safe/non-mp-safe dispatch test |
| 类型 | `NetMain` / `DeviceMain` | 改为按值 `OnceLock<Main>`；NetMain 直接承载 DPO/FIB class metadata 和 local0 | 删除 `OnceLock<Arc<_>>` | duplicate-init and ownership compile checks |
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
| 类型/API | `hammer-plugins/net/ip::ip::icmp::*` 的 IP-owned ICMP module surface | ICMP nodes、control state、ICMP exports 移至 `hammer-plugins/net/icmp` | 无 compatibility re-export；ICMP callers 改依赖新 DSO | workspace compile and DSO export test |
| 类型 | `hammer-service::net::fib::FibEntrySrcRelation` | 删除独立的 cover/sibling/interpose 关系枚举；cover/sibling 事实直接存放在 `FibEntrySrc.cover` 元组，interpose 事实存放在可选的 `FibEntrySrc.interpose_dpo` | 无兼容 alias；实现直接迁移到字段访问，文档和源码不得保留该类型 | 文档 inventory 检查、源码编译和 source-lifecycle 行为测试 |
| 类型 | `hammer-service::net::fib::FibPathFlags` | 删除 service-owned 的固定路径策略位集合；路径 flags 改为 `FibPath<N, F>` 的 owner-supplied `F` | IP callers 迁移到 `hammer-plugins/net/ip::IpPathFlags`；无兼容 alias | generic FIB compile check and IP route flag tests |
| 类型/API | `hammer-service::device::DeviceInputNext::{Ip4Input,Ip6Input}` | 删除 service 固定 IP next | device callback 选择具体 parser/next | graph inventory and RX integration test |
| 类型/API | service-owned `DeviceInputNode` fixed protocol path | 删除固定 service input implementation；由 device plugin 提供 RX node | TUN/device plugins migrate to own node | device DSO graph test |
| 类型/API | `hammer-plugins/net/ip::{AdjacencyRewriteNext,register_adjacency_rewrite}` | 删除单一 adjacency rewrite 节点及其注册入口 | 迁移为 IP4/IP6 concrete node registration；DPO metadata 由 `NetMain` 消费 | graph registration compile/inventory test |
| 类型/API | `hammer-service::net::Dpo` generic struct/constructors | 删除把所有 DPO classes 统一塞进一个通用构造器的表面；保留 concrete owner 的 object creation、subtype selection 和 `(DpoType, index)` 到 `DpoId` projection | callers migrate to concrete DPO class pools and singleton APIs; no `MidchainDpo` pool | compile-time owner projection and pool-lifetime test |
| 类型/API | 本 ADR 草案曾拟引入的跨 DSO network image、class record、lifecycle token、stack registry 和 erased registration surfaces | 实现前全部取消；当前仓库没有这些运行时类型或 ABI 导出，`DpoMain` 直接保存 class/node/edge 元数据，FIB 和 backend 只传 `DpoId` | 无代码迁移、无兼容 alias、无 ABI 版本影响 | 文档/API inventory 检查和 proc-macro compile test |
| API | `register_protocol(protocol, node)` | 删除含糊的单表注册入口 | callers use explicit IP4/IP6 functions | compile-time API migration check |
| 全局/API | `IP_MAIN: ArcSwapOption<IpMain>` | 删除 atomic pointer IP Main global | 通过 `IpMain::init/global` 访问 | global lifecycle integration test |
| 类型/API | `NetworkIpConfig::route`、`Route`、`Via` | 删除通过 TOML 直接创建/替换 FIB 的配置模型 | 迁移为 Binary API route messages；保留 reassembly 配置 | config rejection and Binary API bootstrap test |
| API | `IpLookupControlPlane::publish` | 删除 handler 外的直接表发布入口及其 `barrier::global()/WorkerBarrier::sync` 调用 | 所有调用方迁移到 Binary API dispatcher + service FIB mutation | compile-time removal and end-to-end publication test |
| 类型/API | `FibTableHandle` 的 `Arc<UnsafeCell<FibTable>>` publication | 删除第二套 pointer/lock publication model | node runtime uses owner-published table under barrier | barrier publication and race audit |
| 类型/API | `hammer-service::net::pmtu::{PathMtuCache,PATH_MTU_CACHE,publish_path_mtu_cache,path_mtu_cache}` | 删除 service-owned PMTU cache/global；PMTU tracker/DPO 改由 IP forwarding owner 持有 | TCP/ICMP callers 迁移到 IP-owned typed operations | PMTU lifecycle and cross-plugin API test |
| 类型/API | `hammer-service::net::FibLookupResult` | 删除把控制面 `fib_entry` 查找、数据面 load-balance 查找、bucket 选择和 child DPO 混成一个返回值的类型 | callers 分别使用 `fib_entry` lookup、forwarding lookup 和 service DPO bucket selection | compile-time API removal and control/data-plane lookup tests |
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
3. The service FIB owns source/path/entry precedence, per-entry source slots
   backed by owner-instantiated `FibEntrySrc<N, F, SourceData, PathExt>` records
   with the complete VPP common fields, the direct cover tuple and optional interpose DPO, and owner-supplied
   source data, and back-walk state;
   concrete IPv4/IPv6 owners provide LPM storage and DPO projection through
   static dispatch. DPO class keys are allocated at registration time, but
   object access remains concrete and monomorphized. No unified address-family
   state, erased state, or generic DPO object store is introduced. Address-
   bearing service DPOs are monomorphized over the producer's concrete address
   type; no fixed IPv4/IPv6 byte array, address trait, or erased address crosses
   that seam. Source path extensions are per-record and support add/remove/find,
   resolve, stack, and flush without a fixed label field in `FibPath<N, F>`.
4. `DpoId` is a non-generic eight-byte `Copy + Clone` identity. `DpoMain`
   stores class/node/edge metadata directly; concrete pool-backed DPO owners
   retain published roots, create, publish, and retire their actual objects
   under the dispatcher-owned barrier. Service code never dereferences
   `DpoId.index`.
5. Route mutation is exposed through the existing Binary API dispatcher; the
   dispatcher owns the sole `worker_thread_barrier_sync!` scope and handlers do
   not enter the barrier themselves.
6. Focused tests cover VPP-equivalent source precedence and owner-supplied
   source-data/cover/interpose validation, bitflag widths and transitions,
   per-source path-extension replacement and lifecycle, IPv4 cover restoration,
   IPv4/IPv6 LPM, post-LPM bucket selection, DPO stacking/identity/lifetime,
   concrete class/index validation, owner-pool retirement, PMTU fragment success
   and failure including IPv4 DF ICMP metadata, and failure-atomic mutation.
   Each behavior is proved once at the highest owning interface; tests do not
   repeat constructor coverage or use source-text assertions.
7. One real plugin-load test proves that the IP and ICMP DSOs load in dependency
   order and that ICMP registration reaches the concrete IP local/error tables.

### 明确排除项

Table/address/neighbor/feature/PMTU APIs, MFIB, and additional DPO classes are
outside the first implementation slice. They are not silently required by this
ADR and cannot change the interfaces or ownership decisions recorded here.

## 决策闭合

本 ADR 的架构、owner、生命周期、wire 和启动行为决策已经闭合。下面的
条目是实现必须遵守的最终约束，不是待用户确认的问题：

1. `DpoMain` 不提供通用生命周期回调。每个具体对象池 owner 私有维护
   `published_roots`：替换前保留新 identity，barrier 内发布，发布后释放旧
   identity；计数归零且 worker quiescence 完成后才回收对象和子依赖。设备
   redirect 的唯一接口是
   `InterfaceMain::rx_redirect_to_node(hw_if_index: u32, node: NodeId)`，由
   选中的 `DeviceClass` 直接执行具体回调。
2. `FibSource` 和 `DpoClass` 是唯一新增的声明宏。`DpoClass` 只能作用于
   实际 DPO object layout，其输入和输出固定为：

   ```rust
   #[derive(FibSource)]
   #[fib_source(name = "api", priority = 255, behavior = Api)]
   struct ApiSource;

   #[derive(DpoClass)]
   #[dpo_class(nodes = [
       (DpoProto::IP4, ip4_null_node),
       (DpoProto::IP6, ip6_null_node),
   ])]
   struct IpNullDpo {
       action: IpNullAction,
   }

   #[derive(DpoClass)]
   #[dpo_class(nodes = [
       (DpoProto::IP4, ip4_pmtu_node),
       (DpoProto::IP6, ip6_pmtu_node),
   ])]
   struct IpPmtuDpo {
       proto: DpoProto,
       pmtu: u16,
       published_roots: u16,
       stacked: DpoId,
   }
   ```

   `FibSource` 只生成 owner-local `NAME`、`PRIORITY`、`BEHAVIOR` 常量；
   `DpoClass` 只生成 owner-local `register_dpo_class(...)`，该函数调用
   `DpoMain::register_new_type` 并返回运行时 `DpoType`。`nodes` 右侧是
   owner 传入的已注册 `NodeId` 参数名，不是字符串或隐式查找。宏不生成
   `NAME`、静态 `DpoType`、`RegistrationImage`、ABI image、class record、
   pool、callback table、lifetime token、enum、family 参数或 `dyn` 值；
   只有显式 owner init function 通过现有 `RegistrationImage` 收集。
   `IpNullDpo` 与 `IpPmtuDpo` 都必须通过这个 derive 注册。没有固定 node
   列表的 shared layout（例如 lookup/adjacency）使用 `#[dpo_class]`，生成
   接收 `&[(DpoProto, NodeId)]` 的同一函数，由 owner 为每个 subtype 调用。
3. ICMP data worker 只产生两个 concrete typed events：`Ip4PmtuUpdate` 和
   `Ip6PmtuUpdate`。Binary API Process Node 将事件转换为
   `Ip4Main::update_path_mtu` 或 `Ip6Main::update_path_mtu`，并在唯一的
   `worker_thread_barrier_sync!` 范围内调用；data worker 不发布 PMTU，系统不
   增加第二套同步协议。
4. `FibTable<Ipv4Net, Ip4FibBackend>` 唯一绑定 `Ip4Mtrie<u32>`，
   `FibTable<Ipv6Net, Ip6FibBackend>` 唯一绑定 `Ip6Fib<u32>`。后端接口只接收
   concrete prefix/address 和完整 `DpoId`，IPv4 删除接收 less-specific cover，
   IPv6 删除 `(fib_index, masked_prefix, prefix_length)`；`project_forwarding`
   由对应 IP owner 静态实现。没有第三种 backend、forwarding adapter、family
   facade 或 erased dispatch。
5. `FibPathExt` 始终是 source owner 的 `(entry, source, path_index)` 关联和
   concrete `PathExt` payload。IP route 的四个方法拒绝 owner-specific extension；
   非 IP owner 若需要扩展，必须在自己的 Binary API 和 owner state 中实现，不能
   修改 service `FibPath<N, F>`、IP4/IP6 route payload 或引入通用 extension enum。
6. 四个 route message 使用固定 prost tags，后续不得重排：

   | 消息 | tag 分配 |
   | --- | --- |
   | `Ip{4,6}RouteAddDelRequest` | `is_add=1`, `is_multipath=2`, `table_id=3`, `prefix=4`, `paths=5` |
   | `Ip{4,6}RoutePath` | `sw_if_index=1`, `table_id=2`, `rpf_id=3`, `weight=4`, `preference=5`, `behavior=6`, `flags=7`, `next_hop=8` |
   | `Ip{4,6}RouteLookupRequest` | `table_id=1`, `exact=2`, `address=3`, `prefix_length=4` |
   | `Ip{4,6}RouteAddDelReply` | `status=1`, `stats_index=2` |
   | `Ip{4,6}RouteLookupReply` | `status=1`, `route=2` |
   | `Ip{4,6}RouteRecord` | `table_id=1`, `stats_index=2`, `prefix=3`, `paths=4` |

   IPv4/IPv6 address fields use exactly 4/16 bytes. 本迁移不定义 route dump、
   multipart reply 或 stream envelope；现有 one-request/one-reply envelope 是
   完整接口。
7. 启动时不再读取 `[network].route`。daemon 初始化后保持空 route state；仓库
   内 `hammerctl` 是 canonical bootstrap client，按顺序提交相同的
   `ip4.route.*`/`ip6.route.*` Binary API 请求。任何外部 client 只能复用这些
   方法，不能直接写 FIB。

## 未决问题

无。实现阶段只验证上述约束（编译、DSO load-order、Binary API、FIB/DPO 行为和
布局断言），不再产生新的架构选择。

## 依据

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
- `DpoType` is an opaque class key allocated by `DpoMain` at registration time;
  it is not a closed static Rust enum. `DpoProto` remains only the VPP graph
  protocol discriminator.
- DPO pool elements use the existing 64-byte `CacheLineAlignMark` contract;
  hot fields precede control-only state, and each concrete instantiation proves
  its size, alignment and cacheline offsets at compile time. `LoadBalanceDpo`
  keeps four inline buckets and a power-of-two mask; larger bucket storage is a
  contiguous owner-local overflow slice.
- The packet path is allocation-free and map-free after publication:
  dense `sw_if_index[RX]` to `fib_index`, concrete LPM, post-LPM bucket
  selection, cached `DpoId.next`, and `sw_if_index[TX]`. It never walks source
  records, path extensions, delegates or back-walk state. `NetworkOpaque` must
  stay within the existing primary opaque byte/alignment budget.
- Address abstraction is a producer-owned concrete type parameter at the DPO
  seam. `ReceiveDpo<A>` and `AdjacencyDpo<A, R>` store `A` directly in
  monomorphized pools; net does not require an address trait, family tag, byte
  representation, or canonicalisation method.
- Next-hop wire decoding and path-flag decoding are owned by the IP route API.
  The service FIB accepts only `FibPath<N, F>` with owner-supplied next-hop and
  flags values; it has no `nh_proto`/address union or path-flag bit set.
- `LookupDpo`, `ReceiveDpo`, `AdjacencyDpo`, and `LoadBalanceDpo` are
  service-net DPO layouts and class rules. The concrete class owner stores each
  monomorphized pool and its producer payload; IP, tunnel, and multicast owners
  provide protocol-specific producers and graph nodes, but IP does not own a
  generic adjacency/load-balance pool.
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
- Ordinary route paths are decoded to service `FibPath` facts first; the
  contributing owner resolves paths into service DPO objects and only then
  projects their `(DpoType, index)` to service `DpoId`. Direct `DpoId` input is
  limited to owner-internal special-DPO operations. The generic
  `DpoKind`/`Dpo` construction surface is removed.
- DPO lifetime is class-owner state selected by `DpoType`: `DpoMain` stores
  class/node/edge metadata, service DPO owners store the generic pool values,
  and protocol owners store only their producer state. Retirement is ordered
  under the worker barrier. `DpoId` remains a copyable, non-owning value.
- Generic registration hooks, erased object dispatch, dynamic object state, and
  unified protocol-family abstractions are out of scope. DPO class-key
  allocation is the one intentional runtime registry operation. Cross-owner
  dependencies use explicit owner APIs and concrete state.

### 已决实现约束

- Existing runtime `RegistrationImage` is the only plugin declaration image.
  DPO/FIB init functions call owner APIs after `NetMain` exists; `PluginModule`
  is not expanded with service types because `hammer-runtime` must not depend on
  `hammer-service`.
- Protocol-neutral FIB source/entry/path/back-walk machinery is generic over the
  two concrete lookup backends without moving prefix storage into service net.
  `project_forwarding` returns `DpoId`; the matching concrete DPO owner owns the
  projected object and the IP owner supplies path/address interpretation. The
  service FIB stores only the identity, with no projector object or erased state.
- TCP consumes the typed IP-owned `Ip4Main::path_mtu` or
  `Ip6Main::path_mtu` operation after the ICMP parser moves to the ICMP plugin.
  No protocol-family event or shared PMTU cache is introduced.
- The existing one-reply Binary API envelope is the complete envelope for this
  migration. Route mutation and lookup use the fixed prost tags in
  `决策闭合`; route dump and multipart replies are explicitly excluded.

### Vendored VPP 依据

- FIB20 control-plane model: <https://fd.io/docs/vpp/v2101/gettingstarted/developers/fib20/controlplane>
- FIB20 data model, routes, graph walks, and dataplane pages linked from that
  control-plane document.
- `third_party/vpp/src/vnet/ip/ip.api` (`ip_route_add_del`,
  `ip_route_lookup`, and route dump contracts).
- `third_party/vpp/src/vnet/fib/fib_api.h` (`fib_api_route_add_del` and path
  decoding boundary).
- `third_party/vpp/src/vnet/fib/fib_node.h` and `fib_node_list.h` (node
  identity, heterogeneous children, and sibling links).
- `third_party/vpp/src/vnet/fib/fib_source.h`, `fib_entry.h`,
  `fib_entry_src.h`, and `fib_types.h` (source identities, priorities,
  behavior, source/entry flags, and source lifecycle).
- `third_party/vpp/src/vnet/fib/fib_entry_src.c` (`fib_entry_src_action_install`
  and source winner replacement).
- `third_party/vpp/src/vnet/fib/fib_path.c` and
  `third_party/vpp/src/vnet/fib/fib_path_ext.h` (core `fib_path_t` versus
  owner-specific path extensions).
- `third_party/vpp/src/vnet/fib/fib_entry_delegate.h`,
  `fib_entry_delegate.c`, `fib_entry_cover.h`, `fib_entry_track.h`, and
  `fib_attached_export.h` (optional forwarding-chain, cover, tracker, BFD,
  and attached import/export relations).
- `third_party/vpp/src/vnet/fib/fib_urpf_list.h` and `fib_urpf_list.c`
  (baked uRPF facts and reference lifecycle).
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
- `third_party/vpp/src/vnet/dpo/load_balance.h` (`load_balance_t` hot-field
  ordering, power-of-two bucket mask, four inline buckets and one-cacheline
  size assertion), `lookup_dpo.h` and `receive_dpo.h` (cache-aligned pool
  elements with lookup/receive facts in the switch-path prefix), and
  `third_party/vpp/src/vnet/adj/adj.h` (adjacency cacheline groups and
  hot-prefix/rewrite/delegate offsets).
- `crates/hammer-infra/src/align.rs` and
  `crates/hammer-core/src/buffer/header.rs` (Hammer's existing 64-byte marker,
  allocation alignment and compile-time layout assertions), plus
  `crates/hammer-service/src/opaque.rs` (the fixed `NetworkOpaque` primary
  opaque budget).
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
- `third_party/vpp/src/vnet/adj/adj_types.h`, `adj_glean.h`, `adj_mcast.h`,
  `adj_delegate.h`, `adj_midchain.h`, `adj_midchain_delegate.c`, and
  `adj_dp.h` (shared adjacency subtypes, midchain state, fixup ownership, and
  restack/unstack behavior).
- `third_party/vpp/src/vnet/dpo/lookup_dpo.h` and `lookup_dpo.c` (one object
  pool with multiple dynamically registered subtype class keys).
- `third_party/vpp/src/vnet/dpo/receive_dpo.h` and `receive_dpo.c` (real
  receive object pool and class operations).
- `third_party/vpp/src/vnet/ip/ip4.h` and `ip6.h` (concrete address/FIB
  authorities remain with each IP implementation).
- `third_party/vpp/src/vnet/adj/adj.h` (address-shaped neighbor and midchain
  state in the shared adjacency object).
- `third_party/vpp/src/vnet/dpo/drop_dpo.c`, `punt_dpo.c`, and
  `ip_null_dpo.c` (stateless singleton/fixed-record classes).
- `third_party/vpp/src/vnet/dpo/load_balance.h` and `load_balance.c`
  (object pool, bucket DPO IDs, sibling lookup node, and pool barrier growth).
- `third_party/vpp/src/vnet/dpo/load_balance_map.h` and
  `load_balance_map.c` (normalized weighted-bucket remapping, sharing, and
  path-state back-walk updates).
- `third_party/vpp/src/vnet/ip/ip4.h`, `ip6.h`, and `lookup.h`
- `third_party/vpp/src/vnet/interface.h` and `interface_funcs.h`
- `third_party/vpp/src/vnet/interface/rx_queue.c`
- `third_party/vpp/src/vnet/devices/virtio/virtio.c` and `device.c`
- `third_party/vpp/src/vnet/misc.c` and `interface.c`

### 历史记录核对

无额外历史记录依赖。当前 checkout、Issue #291 和 vendored VPP 源码已经
确定本文的 ownership、wire、生命周期与 publication contract；实现只能验证
这些约束，不能重新打开架构选择。
