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
