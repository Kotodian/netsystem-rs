# Hammer Runtime

Hammer's runtime separates process-wide authorities, operating-system thread
descriptions, and per-thread execution state. GlobalMain and ThreadMain are
distinct process-wide authorities; Data Workers own packet graph execution,
and the main thread drives control processes.

## Runtime Language

**GlobalMain**:
The process-wide authority corresponding to VPP's `vlib_global_main_t`, owning
process metadata, global registrations, and their global call progress. Its
seven hook registration lists belong directly to GlobalMain; thread
administration, per-thread main ownership, graph-refork coordination, and
plugin image lifetime have separate owners.
_Avoid_: MainThread, ThreadMain, DataPlaneMain, engine

**UnixMain**:
The process host-runtime authority corresponding to VPP's `unix_main_t` and
`vlib_unix_main`, owning Unix startup, host process lifecycle, and Unix-facing
control concerns independently from GlobalMain's graph and registration state.
_Avoid_: GlobalMain, DataPlaneMain, ControlThread

**ThreadMain**:
The process-wide thread administration authority corresponding to VPP's
`vlib_thread_main_t`, responsible for thread registrations, configured thread
counts, and CPU, NUMA, and scheduling policy. It is distinct from a thread's
execution state and from GlobalMain's seven hook registration lists.
Its available CPU and NUMA-node sets use the existing
`hammer_infra::bitmap::Bitmap`; configured placement does not replace them.
_Avoid_: GlobalMain, WorkerThread, ControlThread, runtime registry

**WorkerThread**:
The description of one runtime operating-system thread, corresponding to
VPP's `vlib_worker_thread_t`, including its identity, placement, launch facts,
and role in worker synchronization. The main thread also has a thread-zero
description; thread identity is distinct from DataPlaneMain execution state.
_Avoid_: ThreadMain, DataPlaneMain, worker configuration

**DataPlaneMain**:
The execution state owned by one runtime thread, corresponding to VPP's
`vlib_main_t`, including its graph, trace, random stream, main-loop facts, and
worker-init progress. A single `FileMode` enum selects file scheduling: index
zero owns the existing `AsyncFileMain` through `Async`, and workers select
`Sync` against the existing global FileMain. Subsequent Data Worker mains
belong to their workers for the process lifetime.
_Avoid_: GlobalMain, ThreadMain, WorkerThread, shared control registry

**PluginMain**:
The independent process authority for plugin images, plugin metadata, load
order, and the lifetime of loaded plugin code. It supplies direct references
to image declarations to their registration owners without becoming a second
owner of those inventories or their dispatch progress.
_Avoid_: GlobalMain plugin field, plugin registry, generic module manager

**Thread Registration**:
A declaration of a runtime OS-thread role administered by ThreadMain,
including its entry function, configured count, placement policy, and whether
the thread receives a DataPlaneMain clone. It is distinct from a lifecycle hook
executed against an existing DataPlaneMain.
_Avoid_: worker-init hook, GlobalMain hook list, WorkerThread instance

**Auxiliary Thread**:
A registered runtime OS thread whose role does not receive a DataPlaneMain
clone. It remains a WorkerThread description administered by ThreadMain, but it
does not enter the Data Worker graph execution path.
_Avoid_: Data Worker, worker-init target, cloned main

**Runtime Registration Inventory**:
GlobalMain's seven distinct hook registration lists: init, main-loop-enter,
main-loop-exit, worker-init, worker-count-change, API-init, and config, with
early config belonging to the config list. GlobalMain owns global call
progress, each DataPlaneMain owns its worker-init progress, and ThreadMain's
thread registrations describe threads rather than lifecycle hooks.
The proposed Rust progress storage reuses `Bitmap` with stable callback
indices. Assigning those indices while preserving callback identity across
lists and plugin images is registration work still to verify; an erased raw
pointer, callback name, or temporary sorting position is not that contract.
_Avoid_: one universal registration list, per-category called set, plugin-owned
dispatch state

**Registration Image**:
An image-boundary carrier exported by a process image. It carries references
to declarations across the image boundary so the
corresponding owner can install them into the process-global inventories. It is
not a second registration owner and does not own dispatch progress.
_Avoid_: copied registration Vec, plugin-local registration owner, universal
registration carrier

**Graph Refork Request**:
The worker-thread-owned fact that a published graph change requires each Data
Worker to rebuild its worker-local graph state. Worker barrier state, worker
thread coordination, the request, and refork completion accounting belong to
the worker-thread authority.
_Avoid_: GlobalMain-owned barrier, worker completion counter, graph worker state

**Worker Barrier**:
A main-thread synchronization interval that pauses every Data Worker while
control code mutates worker-visible state. It is distinct from process-wide
worker lifecycle, graph publication, and refork authority.
_Avoid_: GlobalMain barrier, worker lock

**Graph Refork**:
Rebuilding each Data Worker's node/runtime clone from the published main graph
while retaining that worker's existing runtime state.
_Avoid_: graph replacement, worker reinitialization

**Process Restore**:
A main-thread scheduling record that says why a suspended Process Node may be
resumed, such as an event, clock expiration, timed event, or yield. It is
consumed by thread-zero DataPlaneMain/NodeMain scheduling and is distinct from
a Data Worker graph frame.
_Avoid_: packet frame, task completion, generic wakeup

**Main-Thread RPC**:
A queued control-plane operation whose callback is executed by the thread-zero
DataPlaneMain/NodeMain scheduler, with a worker barrier when the operation
publishes worker-visible state.
_Avoid_: Data Worker task, Tokio request, packet dispatch

**Per-Thread State**:
Module-owned state partitioned by runtime thread. An executing thread may
borrow only its own entry, and every entry exists before worker launch.
_Avoid_: shared worker state, remotely borrowed state

**Session Worker Event**:
A concrete Session operation delivered through the target Session worker's
event queue and executed by the Session queue Node. Enqueue acceptance is not
operation completion and never grants access to another worker's state.
_Avoid_: arbitrary worker task, synchronous worker call

**Data Worker**:
A worker operating-system thread that owns one `DataPlaneMain` and executes
packet graph nodes, frames, buffers, handoff work, and worker-local readiness.
`DataPlaneMain` also owns the worker's non-cryptographic random stream,
corresponding to `vlib_main_t.random_buffer`. Protocol nodes consume that
stream; they do not install a separate per-protocol RNG lifecycle.
_Avoid_: main thread, control thread

**Data-Plane Buffer**:
The packet-storage object transferred through Graph Nodes. Its header carries
pool provenance and a signed current-data position over inline pre-data and
packet data.
_Avoid_: generated buffer identity, pool handle

**Buffer Index**:
The process-wide compact `u32` identity that locates one Data-Plane Buffer at
64-byte granularity. It contains neither pool identity nor an allocation
generation, and copying it does not change Buffer ownership or reference count.
_Avoid_: generic Index, buffer handle, pool identity

**Physmem Mapping**:
The shared page-based backing region from which a Buffer Pool obtains packet
storage. It owns mapping and placement, while the Buffer Pool owns slots.
_Avoid_: Buffer allocator, slot pool, Buffer owner

**Buffer Pool**:
The allocation authority for Data-Plane Buffer slots of one data capacity,
backed by one Physmem Mapping. It owns slot lifecycle, its central free set,
per-worker caches, and the Buffer initialization template.
_Avoid_: capacity flags, per-index pool identity

**Buffer Main**:
The independent process-global Buffer authority, initialized once before any
Data Worker starts and retained until process exit. It owns the process address
base, Buffer Pool registry, and default NUMA Pool selection.
_Avoid_: GlobalMain field, worker-owned arena, mutable buffer address base

**Frame**:
The contiguous calling record delivered to one Graph Node, carrying that
Node's scalar, vector, and auxiliary arguments. Recycling Frame memory is
separate from releasing the Buffer obligations named by its vector elements.
_Avoid_: BufferFrame, growable index list, per-element owner

**Next Frame**:
Worker-local append state for one source-Node next arc. It tracks a Frame and
the exclusive right to enqueue that Frame to its destination Node.
_Avoid_: checked-out frame, output vector

**Pending Frame**:
A Frame scheduled for destination-Node dispatch together with its Node Runtime
and associated Next Frame identity.
_Avoid_: scheduled buffer owner, frame pool token

**Node Main**:
The worker-local graph execution authority that owns Node Runtimes, Next
Frames, Pending Frames, and reusable Frame size classes.
_Avoid_: graph manager, aggregate NodeRuntime

**Node Runtime**:
The worker-local execution state for one Graph Node, passed mutably to that
Node while it processes a Frame.
_Avoid_: copied runtime data, current-node side channel

**Drop Node**:
The terminal Graph Node for ordinary packet discard. Packet-processing Nodes
route rejected Buffer Indices to it with a typed error classification; it
records disposition and ends the retained Buffer lifecycle.
_Avoid_: per-node Buffer cleanup, public `free_buffer`, silent discard

**Buffer Ownership**:
The exclusive responsibility to mutate and eventually release an allocated
Data-Plane Buffer. A Frame carries that responsibility through the graph, and
a Worker Handoff or long-lived domain owner may retain it without changing the
reference count. Shared chain segments are immutable until their reference
count again permits exclusive access.
_Avoid_: arena-wide buffer lock, shared mutable packet, per-Buffer free helper

**IP Reassembly Context**:
Worker-owned state for the fragments of one original IP packet. It retains raw
Buffer Indices and either transfers the completed chain or ends the lifecycle
of fragments it still owns.
_Avoid_: borrowed retained fragment, manual fragment release path

**Worker Handoff**:
Transfer of packet ownership to another Data Worker at an explicit graph
destination, independent of the packet's current Feature Arc position.
_Avoid_: feature continuation, intermediary handoff node

**Process Node**:
A cooperative control-plane execution context scheduled on the main operating-
system thread. In Hammer it is represented by one Tokio task and may suspend
until an event, clock, timed event, or yield makes it runnable; it is not an OS
thread and does not execute packet graph work.
_Avoid_: process thread, Data Worker, background thread

**InterfaceRegistrationImage**:
The service-owned static declaration image for device classes, hardware-
interface classes, and interface callbacks. It is consumed by `InterfaceMain`
at startup to build active interface state and is independent of `PluginMain`
ownership or plugin lifecycle.
_Avoid_: generic registration image, interface record

**NetMain**:
The service-owned network authority corresponding to VPP's `vnet_main_t`. It
is the single entry point for network-wide interface and device coordination
and owns the `InterfaceMain` authority.
_Avoid_: network manager, network context

**InterfaceMain**:
The interface authority embedded in `NetMain`, corresponding to VPP's
`vnet_interface_main_t`. It owns interface identity,
address, MTU, hardware-interface, queue, and interface callback state that must
be coordinated across network services and device drivers. It is initialized
before runtime interface configuration is applied.
_Avoid_: InterfaceControlPlane, interface registry

**DeviceMain**:
The service-owned device authority corresponding to VPP's
`vnet_device_main_t`. It is process-global and owns device-input worker scope,
aggregate receive statistics, and device scheduling state, while device
instances, hardware interfaces, and RX/TX queues belong to `InterfaceMain`.
_Avoid_: device registry, interface registry

**DeviceClass**:
The driver behavior declaration that describes how a device class sends and
integrates with the network interface authority.
_Avoid_: device kind, device type

**HwClass**:
The hardware-interface behavior declaration associated with a device class.
_Avoid_: hardware interface type

**HwInterface**:
The hardware-facing interface instance owned by `InterfaceMain`, identified by
its hardware-interface index and linked to a software interface.
_Avoid_: hardware interface record

**SwInterface**:
The software-facing interface instance owned by `InterfaceMain`, identified by
its software-interface index and linked to its hardware interface when one
exists.
_Avoid_: software interface record

**Interface Component**:
A compile-time Hammer component declaration for a network-device class,
hardware-interface class, or interface callback. It registers the driver's
network behavior with the owning network authority and is distinct from a
runtime software-interface or hardware-interface instance.
_Avoid_: interface record, interface helper

## Network/IP Language

**Independent Plugin**:
A plugin that owns its lifecycle and network behavior. Independence does not
prohibit an explicit dependency on another plugin's owner-defined interface.
_Avoid_: zero-dependency plugin, isolated plugin

**ICMP Plugin**:
The independent plugin that owns locally delivered ICMP messages, type dispatch
and echo replies. IP owns ICMP error generation for rejected IP packets.
_Avoid_: error-generation plugin, generic network plugin

**IPv4/IPv6 Implementation**:
The concrete IPv4 or IPv6 behavior inside the single IP plugin. They share the
network forwarding model but are not one selectable protocol-family object.
_Avoid_: ip4 plugin, ip6 plugin, family DSO

**FIB Source**:
A concrete authority that contributes route semantics for a prefix under a
defined precedence and merge contract.
_Avoid_: route table, source registry entry

**FIB Graph Node**:
A source, entry, path list, path, or tracker identified by `(node_type, index)`
and connected through child/sibling links for recursive resolution and
back-walk.
_Avoid_: raw pointer node, route snapshot

**FIB Entry Source**:
The per-entry, embedded contribution record for one `FIB Source`, not an
independent graph node. It retains the source's path-extension list, path-list
link, entry/source flags, source identity, repeated-add count, common
cover/interpose relation facts, and one concrete owner-supplied source-data
payload. Service net does not enumerate protocol-specific source branches.
_Avoid_: independent graph index, source registration metadata, callback table,
erased source payload

**Path Extension**:
Per-source path state associated with `(entry, source, path_index)` that carries
facts outside the shared FIB path. Its concrete payload and lifecycle are
selected by the source or table owner.
_Avoid_: universal path field, label stack in `FibPath`

**Entry Delegate**:
An optional FIB entry relation created only when needed for an additional
forwarding chain, covered-entry list, tracker, BFD state, or attached
import/export relationship.
_Avoid_: fixed chain array, generic delegate object

**Midchain Adjacency**:
An adjacency subtype that stacks a child DPO on a recursive target entry and
restacks or un-stacks to drop as target state changes.
_Avoid_: separate midchain pool, tunnel callback in service net

**Load-Balance Path**:
A resolved FIB path supplied to the load-balance owner, carrying its path
identity, forwarding DPO and requested weight before bucket normalization.
_Avoid_: pre-expanded bucket, DPO class registration

**Load-Balance Map**:
A supporting weighted-bucket remapping object shared by load-balance instances;
it is not a DPO class and is rebuilt when path state changes.
_Avoid_: load-balance DPO type, forwarding-chain walk for uRPF

**uRPF List**:
The immutable, unique set of accepting interfaces contributed by FIB paths
according to their reverse-path semantics, which need not require forwarding
resolution. It is distinct from a DPO operation that reports one interface and
from the configured next-hop set.
_Avoid_: DPO-chain traversal, DPO class, next-hop address list

**MFIB**:
The multicast FIB authority with its own table/entry/path state and replicate
DPO projection, separate from the unicast `FibTable` implementation.
_Avoid_: multicast fields in unicast FIB, shared family table

**DPO Class**:
A forwarding behavior key with per-data-path node metadata and owner operations
for references, instance node resolution, MTU/uRPF, interpose and diagnostics.
The owning module binds the key to its concrete objects; stateless classes need
no pool, and several classes may describe different behaviors of one object form.
_Avoid_: DPO instance, forwarding object

**DPO Instance**:
A concrete forwarding object owned by the module that understands its state.
Its compact 8-byte `DpoId` identity is a dispatch fact, not the object itself;
copies do not retain or inspect the pool value. Owning fields acquire/release
references through class lock/unlock. Control-plane pool queries retain standard
Rust borrow guards; worker selection returns copied identities within the
barrier-protected read scope.
_Avoid_: DPO class, forwarding object

**Network Address**:
A producer-owned concrete value stored directly in a generic DPO layout. Net
borrows or moves that value but does not define an address trait, canonicalisation
method, byte representation, family enum, or wire interpretation.
_Avoid_: `Box<[u8]>` address erasure, `dyn` address, `IpFamily`, IP address type
in service net

**DPO Data-Path Protocol**:
The discriminator that selects a DPO's packet-graph/link behavior. It is not an
IP wire protocol number and does not select ICMP, TCP, or UDP local dispatch.
_Avoid_: DPO protocol number, IP protocol selector

**DPO Hot Layout**:
The concrete DPO object's cacheline contract: switch-path fields are placed in
the first cacheline, control-only state is separated, and the concrete owner
proves size, alignment and required offsets. Load-balance uses a precomputed
power-of-two mask and stores up to four child identities inline. Above that
threshold the entire bucket array is contiguous out of line, not only its tail.
_Avoid_: cacheline padding on every type, packet-path map lookup, bucket rebuild

**Packet-Path Forwarding Contract**:
The bounded worker sequence from dense RX interface to concrete FIB LPM,
post-LPM DPO bucket selection, cached next edge and TX interface. It performs
no allocation, control-plane lock, source/delegate/path-extension walk or
control-plane map lookup after publication.
_Avoid_: packet-path FIB graph traversal, dynamic DPO dispatch, hot-path
allocation

**IP Feature Arc**:
A concrete IP packet-processing chain for one protocol and location, such as
IPv4 local input or IPv6 output.
_Avoid_: generic IP family arc, DPO protocol dispatch

**Path MTU DPO**:
An IP-owned forwarding object that applies a path MTU constraint while
preserving the underlying forwarding decision.
_Avoid_: service PMTU cache, ICMP parser in net

**Local0 Interface**:
The always-present network interface used as a reserved sentinel rather than
an addressable endpoint.
_Avoid_: loopback interface, ordinary local route interface

**Device RX Node**:
A graph node owned by a device plugin that receives ingress packets and chooses
their next graph target.
_Avoid_: fixed device input path, service-owned protocol input

**IP/Device Data-Path Seam**:
The packet-graph seam where device and IP plugins exchange node identity and
RX/TX interface facts without a fixed protocol path.
_Avoid_: device-to-IP hardwire, generic protocol dispatcher

**Interface RX Redirect**:
An interface-owner request for a device class to redirect one hardware
interface's receive stream to a concrete graph node.
_Avoid_: global input next, fixed IP input redirect

**Binary API Route Publication**:
The control-plane command surface used to request runtime route and forwarding
changes from the plugin that owns them.
_Avoid_: direct route publish handle, config-only route mutation

**Stats Metric**:
An owner-defined, externally observable runtime value published through the
Hammer stats segment with a stable VPP-style directory path. It has one owning
subsystem and one defined update authority.
_Avoid_: ad-hoc metric, log value, duplicated monitoring counter

**Show Diagnostic**:
A VPP-style CLI projection of owner-defined stats and runtime facts. It selects
and formats existing published data; it does not create a second metric store.
_Avoid_: command-local counter, CLI-only metric

**Stats Owner**:
The subsystem that defines, registers, updates, and explains a Stats Metric.
Runtime, interface, session, transport, plugin, and infrastructure metrics
remain with their concrete owners.
_Avoid_: central metrics manager, generic metric registry owner

**Runtime Statistic**:
A VPP-style cumulative fact such as node calls, vectors, clocks, suspends, or
classified input/output/drop/punt vectors. Rates and summaries are derived
when a diagnostic snapshot is formatted.
_Avoid_: stored rate, query-time counter, CLI-local measurement

**Module-owned ctl**:
The owner module defines its ctl arguments, Binary API request/reply binding,
reply decoding, and VPP-style formatter. `hammerctl` supplies the Binary API
and stats-segment transport plus static ctl-module composition; it does not
own plugin-specific command enums or formatting. Runtime owns core runtime,
error, and memory ctl commands; interface, session, and transport modules own
their respective ctl commands.
_Avoid_: central plugin command enum, CLI-owned plugin formatter

**Statistic Clear Baseline**:
The owner-published cumulative values against which a VPP-style `show` command
computes post-clear deltas. Clearing updates the baseline at the control-plane
synchronization boundary.
_Avoid_: resetting hot-path counters, client-side subtraction state

## Binary API Language

**API Main**:
The process-wide Binary API authority corresponding to VPP's `api_main_t`. It
owns Message Data, runtime Message ID ranges, name/CRC lookup, API versions,
shared-memory regions, the main input queue, and shared-memory client
registrations; socket listener and socket connection state have a separate
owner.
_Avoid_: BinaryApiMain, socket listener owner, method-name table, plugin API
state

**Socket Main**:
The Binary API socket-transport authority corresponding to VPP's
`socket_main_t`. It owns the listener, socket registrations, incomplete stream
input, pending output, and socket readiness state, but not Message Data or
shared-memory client registrations.
_Avoid_: API Main, UnixMain, generic client registry

**Binary API Message**:
The protobuf transport value containing only a runtime Message ID and the
encoded bytes of one `.api`-defined message. Client Index, Context, `retval`,
and operation fields remain in that generated message exactly when declared by
its `.api` schema.
_Avoid_: method invocation, request envelope, reply envelope, event envelope,
generic reply status

**API Message Config**:
The generated registration input corresponding to VPP's
`vl_msg_api_msg_config_t`; it binds one runtime Message ID and message name to
its generated protobuf validator, handler, trace policy, replay policy, and
MP-safe bit during API initialization.
_Avoid_: method declaration, message descriptor, handler invocation

**API Message Data**:
The API Main record indexed by runtime Message ID, corresponding to VPP's
`vl_api_msg_data_t`. It contains the installed message name, generated
protobuf validator, handler, trace policy, replay policy, and MP-safe bit for
the current process; CRC lookup remains in the separate name-and-CRC table.
_Avoid_: API Message Config, payload object, plugin state

**Message Name and CRC**:
The canonical `<message-name>_<crc>` key generated from `.api` and used to map
client schema messages to the current runtime Message IDs.
_Avoid_: API Message Identity object, method string, persisted Message ID

**Message ID**:
The runtime-assigned numeric identity used by the current API process to find a
Binary API Message Data record and handler. It has VPP's `u16` semantic range,
is valid for one running API instance, and is not a persisted or cross-version
identity.
_Avoid_: stable API version, method string, global sequence number

**API Message Range**:
A contiguous range of runtime Message IDs assigned to one API owner during
API startup. The range is an allocation fact, not part of the stable message
identity.
_Avoid_: plugin version, fixed public ID block, request sequence

**API Message Table**:
The client-visible association between Message Name and CRC keys and the
current runtime Message IDs, serialized when a client registers.
_Avoid_: message registry, protobuf `oneof`, client-side method list

**Client Index**:
The API registration handle assigned to one connected client. Shared-memory
handles include the restart epoch and socket handles identify the socket
registration space; stale handles never resolve to a current registration.
_Avoid_: process ID, file index, request ID

**API Registration**:
The server-side record for one shared-memory API client, corresponding to the
shared-memory fields of VPP's `vl_api_registration_t`. It identifies the
client's reply queue and liveness state; socket registrations belong to Socket
Main.
_Avoid_: API Message Config, event subscription, transport-neutral connection
object

**API Shared Memory Region**:
The shared Segment containing the Binary API header, main input queue,
per-client reply queues, message rings, and serialized API Message Table. Values
crossing processes are mapping-relative offsets rather than process-local raw
pointers.
_Avoid_: App Session Segment, Main Heap, fixed virtual-address contract

**API Shared Memory Header**:
The published root of an API Shared Memory Region, corresponding to VPP's
`vl_shmem_hdr_t`. It names the main input queue and records protocol version,
server identity, restart epoch, and API Message Table location.
_Avoid_: transport frame header, protobuf message header, socket bootstrap

**Socket Registration**:
The Socket Main record for a listener, accepted server connection, or client
connection. It owns the File index, incomplete input, pending output, removal
state, and any descriptors retained for shared-memory bootstrap.
_Avoid_: API Registration, transport-neutral client, API Message Config

**API Event Subscription**:
A subscription record owned by the network subsystem or plugin that emits the
event. It names a Client Index but is not owned by API Main; Hammer does not
create a process-wide `VpeApiMain` aggregate for unrelated event families.
_Avoid_: API Main event pool, generic plugin subscription registry

**Request Context**:
The client-supplied value echoed by a reply to associate that reply with its
request. It is scoped to a client and does not establish ordering between
clients.
_Avoid_: Message ID, Client Index, global sequence number

**Unary Message**:
A Binary API request whose operation completes with one typed reply carrying
the operation's result code and response fields.
_Avoid_: dump request, event message, transport status

**Dump Stream**:
A Binary API request sequence that yields zero or more ordered Details
Messages and ends at the corresponding control-ping reply.
_Avoid_: paginated RPC, vector response, repeated unary call

**Details Message**:
An owner-defined reply item emitted in order while a Dump Stream is being
enumerated. It is not the completion marker for the stream.
_Avoid_: final reply, event message, diagnostic log

**Control Ping**:
The API message used to delimit completion of a Dump Stream after all Details
Messages have been emitted.
_Avoid_: transport heartbeat, timeout probe, dump item

**Event Message**:
An owner-defined Binary API message sent to a subscribed Client Index without
being the direct reply to a single request.
_Avoid_: unsolicited reply, log event, polling result

**MP-Safe Message**:
A Binary API message whose handler may run on the serial Main Thread without
entering the Worker Barrier. A non-MP-safe handler enters the existing Worker
Barrier before it may publish or mutate worker-visible state.
_Avoid_: thread-safe payload, worker-owned message, lock-free message

**API Schema**:
The owner-maintained `.api` description of Binary API message fields, Message
Name and CRC values, request/reply/details/event relationships, and service
metadata. Runtime MP-safe selection is installed during API initialization,
matching VPP rather than becoming a second schema annotation.
_Avoid_: generated transport frame, runtime registry snapshot, CLI syntax


## SVM Language

These are target domain terms. The SVM owner modules live in one subtree,
`crates/hammer-infra/src/svm.rs` plus `crates/hammer-infra/src/svm/`
(`segment`, `region`, `queue`, `msg_queue`, `fifo`, `fifo_segment`); the legacy
`Segment` and `MultiRingMsgQueue` remain at the crate root until they are
deleted. The offset-based region owner is implemented in `svm/region.rs`,
`svm/region_heap.rs`, and `svm/hash_map.rs`; the remaining SVM users still run
on the legacy `Segment`, `Fifo`, and `MultiRingMsgQueue` until they are
migrated to it.
The proposed ownership, Rust fields/method signatures, deletion inventory,
and approval status are recorded in
[ADR-0011](docs/adr/0011-vpp-style-svm-ownership-and-multiarch.md); the region
redesign (offset heap, name table, independent subregions) is specified in
section 12 of that ADR.
Terminology here is not a claim that the migration has been implemented.

**SvmSegment**:
The SVM mapping and backing-resource owner used by shared regions, FIFO
segments, and API memory bootstrap. A segment is distinct from the allocator
or protocol state placed in its storage.
_Avoid_: Segment, shared-memory wrapper, application segment owner

**SvmRegion**:
A general SVM region with its own metadata, data allocation authority, client
membership, and published root. Its segment payload is a fixed header followed
by a metadata heap and, when present, a data portion; every shared location is
an offset, never a process-local pointer. A region does not own the mapping,
descriptor, or backend, and it is closed rather than repaired when its lock
owner dies. It can occupy an existing SvmSegment and is not limited to App
Sessions.
_Avoid_: mmap wrapper, FIFO segment, API registration, RegionName, root_path, backing_file

**SvmRegionMain**:
The named-root authority of a subdivided region. It lives in the root region's
metadata heap and holds the region name table and the monotonic subregion
identity counter. It does not reserve or carve a virtual-address range and does
not own subregion mappings; each subregion is its own SvmSegment whose
descriptor is exchanged by the daemon outside the shared region.
_Avoid_: API Main, process-global memory allocator, subregion pool, name hash

**SvmRegionHeap**:
The offset-based block allocator that owns a region's metadata heap. Blocks
carry adjacent-block size and use flags in their headers, free blocks are
linked by offsets into fixed bins, and the bytes preceding a user area point
back at its block header, so no process-private pointer is stored in shared
memory. Exhaustion, double free, and block corruption terminate the process
with structured facts instead of returning a recoverable error. The heap has no
lock of its own; the region lock serializes access.

A heap is a field of the region header, never inside the arena it manages, so
it is owned by the region rather than by any process or Rust value: there is no
destructor, no destroy operation, and no way to move a descriptor out of shared
memory. Only region creation initializes it, once, before the region is
published; attach validates and never repairs it. Its range is fixed at
creation and only its contents grow. It disappears with the segment mapping
after the name is removed, and a region whose lock owner died is closed instead
of having its heap reused.
_Avoid_: local heap, Main Heap, talc allocator, bump allocator

**SvmHashMap**:
The byte-string-keyed hash table used for the region name table. Its semantics
match the standard library hash map, but slots, key bytes, and links are
offsets in the owning SvmRegionHeap, the table owns its key bytes in that heap,
and hashing uses a fixed-seed hasher so another process reaches the same
bucket. It has no destructor and no internal lock.
_Avoid_: std HashMap, Bihash, name hash, RegionName

**Subregion**:
An independently mapped region registered by name under a subdivided root
region. It is located by name lookup in the root region's name table and
identified by a monotonic, never-reused subregion identity, so a stale identity
never silently addresses another subregion. Its mapping descriptor is exchanged
by the daemon, not derived from a virtual-address bitmap.
_Avoid_: VA slice, child region, subregion pool

**Region Membership**:
One process's registration in a region's client list, written in that region's
metadata heap and reclaimed by pid liveness probing. Registration is an RAII
handle; recovery records no process start time and never reuses a registration
for a different process.
_Avoid_: client vec, connection handle, worker registration

**Region Owner Death**:
Robust-mutex owner death marks the region failed and later access reports the
dead owner instead of pretending the lock was acquired; the mutex is never
reinitialized in place. A fresh region identity replaces the failed one only
after participants stop using the old region.
_Avoid_: force unlock, mutex rebuild, consistent-but-unknown

**SvmFifoSegment**:
The SVM owner of FIFO storage allocation, slices, and reusable FIFO headers
and chunks. Session policy belongs to the Session owner using that storage.
_Avoid_: SvmRegion, Session segment manager, generic heap

**SvmFifo**:
The SVM byte FIFO whose producer publishes bytes and whose consumer releases
bytes, including out-of-order delivery and FIFO notification state.
_Avoid_: Fifo, message queue, packet Buffer

**SvmQueue**:
The SVM bounded queue of fixed-size elements, with interprocess production,
consumption, and waiting semantics. It is distinct from API message storage
allocation rings. The proposed Rust element parameter describes a validated
shared representation; size and alignment are derived from that element type.
Only stored contents are generic; synchronization and storage backends are not
type parameters. In VPP's
fixed-element queue, eventfd changes notification, not the shared mutex.
_Avoid_: SharedQueue, SvmFifo, API message allocator

**SvmMsgQueue**:
The SVM message queue exchanging descriptors for slots in its data rings.
The storage protocol is independent of Session event contents and Binary API
message allocation policy. Descriptors are consumed in queue order; each
data ring allocates at its tail and reclaims at its head. Dequeuing a
descriptor and releasing its payload slot are distinct operations. The queue
owner is not generic: reserve<T> and dequeue<T> select the stored content type
for a slot, allowing different rings to hold different contents. Reservations own
publication; consumed messages own ordered slot reclamation. Ring count
and capacity remain runtime configuration.
_Avoid_: MultiRingMsgQueue, SvmQueue, API message allocation ring

**Message Reservation**:
An unpublished data-ring slot whose transaction owns the producer exclusion
until commit or cancellation. MsgReservation<T> lends the stored value as
&mut T. Commit transfers the message to the consumer;
cancellation restores the unpublished allocation.
_Avoid_: arbitrary free slot, payload copy, detached pointer wrapper

**FIFO Segment Slice**:
The allocation partition containing reusable FIFO headers and chunk size
classes. Shared allocation state is distinct from the executing worker's
private FIFO and out-of-order state.
_Avoid_: per-thread wrapper, message ring, Session policy

**Queue Notification**:
The Linux design uses posix-sync's process-shared robust mutex and shared
condition variable directly. One mutex protects queue state, publication,
consumption, and ordered slot reclamation; there is no extra consumer mutex.
A consumed message retains the crate guard until it releases its slot, so
handlers run after decoding and releasing the message. Only stored contents
are generic. Eventfd changes notification only in this Rust design; VPP's
eventfd MQ instead uses a private producer spinlock.
Owner death from lock or condition-wait reacquisition closes the damaged
instance without declaring unknown data consistent. Later operations observe
Closed/NotRecoverable, and the lifecycle owner retires the old identity and
creates a fresh queue after participants stop using the old instance. Waiters
periodically reacquire to detect failure even without notification. This
recovery design must be behaviorally tested; it is not implemented yet.
MacOS/iOS remain outside the current Linux scope.
_Avoid_: Worker Barrier, payload publication, shared numeric fd identity

**API Message Allocation Ring**:
Binary API-owned storage that selects a message size class and reclaims each
message after its receiver finishes. It is separate from SvmQueue transport
and from SvmMsgQueue's ordered payload rings. API queue elements identify
shared message storage using validated offsets/identities; they do not carry
process-local pointers. Shared region allocation, peer lifecycle, and recovery
remain prerequisites for the future vlibapi/vlibmemory refactor. ADR-0011
records the caller audit and required behavioral validation.
_Avoid_: Session CTRL ring, SvmMsgQueue, fixed-element transport queue

**Machine Architecture Function**:
A concrete ordinary function with a baseline implementation and supported
instruction-set implementations selected by CPU capability and priority.
The SVM uses correspond to VPP's two FIFO chunk copy functions; selection is
independent of Graph Node registration and never stored in shared memory.
Rust compiles concrete instruction-set variants and selects a supported
machine-code entry once; this does not introduce algorithm type parameters.
_Avoid_: Graph Node, protocol backend, shared function pointer
