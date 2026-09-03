# Hammer Runtime

Hammer's runtime separates process-wide authority from the control scheduler
that owns the main operating-system thread and keeps packet execution on Data
Workers.

## Runtime Language

**GlobalMain**:
The process-wide runtime authority corresponding to VPP's
`vlib_global_main_t`. It owns worker lifecycle, graph publication and refork,
registrations, plugin lifetime, and process-wide lifecycle state; it does not
own Worker Barrier synchronization or execute packet graph work.
_Avoid_: MainThread, DataPlaneMain, engine

**Worker Barrier**:
A main-thread synchronization interval that pauses every Data Worker while
control code mutates worker-visible state. It is distinct from process-wide
worker lifecycle, graph publication, and refork authority.
_Avoid_: GlobalMain barrier, worker lock

**Graph Refork**:
Rebuilding each Data Worker's node/runtime clone from the published main graph
while retaining that worker's existing runtime state.
_Avoid_: graph replacement, worker reinitialization

**ControlThread**:
The scheduler running on the main operating-system thread. It uses a
single-thread Tokio runtime to dispatch Process Nodes, process restores, timer
expirations, main-thread RPCs, control I/O readiness, and lifecycle decisions;
it does not execute Data Worker packet graph work.
_Avoid_: GlobalMain, Data Worker, control loop

**Process Restore**:
A main-thread scheduling record that says why a suspended Process Node may be
resumed, such as an event, clock expiration, timed event, or yield. It is
consumed by `ControlThread` and is distinct from a Data Worker graph frame.
_Avoid_: packet frame, task completion, generic wakeup

**Main-Thread RPC**:
A queued control-plane operation whose callback is executed by `ControlThread`
on the main operating-system thread, with a worker barrier when the operation
publishes worker-visible state.
_Avoid_: Data Worker task, Tokio request, packet dispatch

**Data Worker**:
A worker operating-system thread that owns one `DataPlaneMain` and executes
packet graph nodes, frames, buffers, handoff work, and worker-local readiness.
_Avoid_: main thread, control thread

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
The independent plugin that owns ICMP behavior while consuming the IP plugin's
explicit IPv4 and IPv6 interfaces.
_Avoid_: IP plugin ICMP module, generic network plugin

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

**Load-Balance Map**:
A supporting weighted-bucket remapping object shared by load-balance instances;
it is not a DPO class and is rebuilt when path state changes.
_Avoid_: load-balance DPO type, forwarding-chain walk for uRPF

**MFIB**:
The multicast FIB authority with its own table/entry/path state and replicate
DPO projection, separate from the unicast `FibTable` implementation.
_Avoid_: multicast fields in unicast FIB, shared family table

**DPO Class**:
A forwarding behavior key with per-data-path node metadata, bound to a concrete
object pool by its owning module. Several classes may describe different
behaviors of one object form.
_Avoid_: DPO instance, forwarding object

**DPO Instance**:
A concrete forwarding object owned by the module that understands its state.
Its compact 8-byte `DpoId` identity is a dispatch fact, not the object itself;
copies do not retain or inspect the pool value.
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
proves size, alignment and required offsets. Load-balance keeps four inline
child identities and uses a precomputed power-of-two mask; larger buckets stay
in contiguous owner-local storage.
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
